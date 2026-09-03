"""Crash-safe primitives for YTArchiver's line-based sidecar files.

The archive has several JSONL and text files that are updated independently
from SQLite or another sidecar.  This module keeps the filesystem mechanics in
one place: reads distinguish a genuinely missing file from an unreadable one,
writes are staged and validated in the destination directory, and multi-store
operations can leave a durable reconciliation marker until every commit lands.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal


class SidecarError(OSError):
    """Base class for sidecar persistence failures."""


class SidecarReadError(SidecarError):
    """The target exists (or may exist) but could not be read safely."""


class SidecarWriteError(SidecarError):
    """A staged sidecar could not be made durable or installed."""


class SidecarValidationError(SidecarError, ValueError):
    """A JSON/JSONL payload does not match the required object shape."""


@dataclass(frozen=True)
class BytesRead:
    path: Path
    exists: bool
    data: bytes


@dataclass(frozen=True)
class TextRead:
    path: Path
    exists: bool
    text: str


@dataclass(frozen=True)
class JsonlRead:
    path: Path
    exists: bool
    records: tuple[dict[str, Any], ...]
    invalid_lines: tuple[int, ...] = ()


_LOCKS: dict[str, threading.RLock] = {}
_LOCKS_GUARD = threading.Lock()


def sidecar_lock(path: str | os.PathLike[str]) -> threading.RLock:
    """Return the process-wide re-entrant lock for *path*."""
    key = os.path.normcase(os.path.abspath(os.fspath(path)))
    with _LOCKS_GUARD:
        lock = _LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _LOCKS[key] = lock
        return lock


def read_bytes(path: str | os.PathLike[str]) -> BytesRead:
    """Read bytes, returning ``exists=False`` only for FileNotFoundError.

    Permission, sharing, device, and other I/O failures are deliberately not
    converted to an empty document.  Callers that intend to rewrite a file can
    therefore fail closed instead of erasing old data they could not inspect.
    """
    target = Path(path)
    try:
        with open(target, "rb") as handle:
            return BytesRead(target, True, handle.read())
    except FileNotFoundError:
        return BytesRead(target, False, b"")
    except OSError as exc:
        raise SidecarReadError(f"could not read sidecar {target}: {exc}") from exc


def read_text(path: str | os.PathLike[str], *,
              encoding: str = "utf-8-sig") -> TextRead:
    snapshot = read_bytes(path)
    if not snapshot.exists:
        return TextRead(snapshot.path, False, "")
    try:
        return TextRead(
            snapshot.path,
            True,
            snapshot.data.decode(encoding),
        )
    except UnicodeDecodeError as exc:
        raise SidecarReadError(
            f"sidecar is not valid {encoding}: {snapshot.path}") from exc


def _parse_jsonl_bytes(
        payload: bytes, *,
        invalid: Literal["raise", "skip"] = "raise",
        require_trailing_newline: bool = False,
        path: str | os.PathLike[str] = "<memory>",
        ) -> tuple[tuple[dict[str, Any], ...], tuple[int, ...]]:
    if require_trailing_newline and payload and not payload.endswith(b"\n"):
        raise SidecarValidationError(
            f"JSONL sidecar has an incomplete final line: {path}")
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SidecarValidationError(
            f"JSONL sidecar is not valid UTF-8: {path}") from exc

    records: list[dict[str, Any]] = []
    invalid_lines: list[int] = []
    for line_no, raw_line in enumerate(text.splitlines(), 1):
        if not raw_line.strip():
            continue
        try:
            value = json.loads(raw_line)
            if not isinstance(value, dict):
                raise TypeError("record is not an object")
        except (json.JSONDecodeError, TypeError) as exc:
            if invalid == "skip":
                invalid_lines.append(line_no)
                continue
            raise SidecarValidationError(
                f"invalid JSONL object at {path}:{line_no}: {exc}") from exc
        records.append(value)
    return tuple(records), tuple(invalid_lines)


def validate_jsonl_bytes(payload: bytes, *,
                         require_trailing_newline: bool = True) -> None:
    """Raise unless every nonblank JSONL record is an object."""
    _parse_jsonl_bytes(
        payload,
        invalid="raise",
        require_trailing_newline=require_trailing_newline,
    )


def read_jsonl(
        path: str | os.PathLike[str], *,
        invalid: Literal["raise", "skip"] = "raise",
        ) -> JsonlRead:
    snapshot = read_bytes(path)
    if not snapshot.exists:
        return JsonlRead(snapshot.path, False, ())
    records, invalid_lines = _parse_jsonl_bytes(
        snapshot.data,
        invalid=invalid,
        path=snapshot.path,
    )
    return JsonlRead(snapshot.path, True, records, invalid_lines)


def read_json_object(path: str | os.PathLike[str]) -> tuple[bool, dict[str, Any]]:
    snapshot = read_text(path)
    if not snapshot.exists:
        return False, {}
    try:
        value = json.loads(snapshot.text)
    except json.JSONDecodeError as exc:
        raise SidecarValidationError(
            f"invalid JSON sidecar {snapshot.path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SidecarValidationError(
            f"JSON sidecar is not an object: {snapshot.path}")
    return True, value


def encode_jsonl(records: Iterable[Mapping[str, Any]]) -> bytes:
    lines: list[str] = []
    for index, record in enumerate(records, 1):
        if not isinstance(record, Mapping):
            raise SidecarValidationError(
                f"JSONL record {index} is not an object")
        lines.append(json.dumps(dict(record), ensure_ascii=False) + "\n")
    payload = "".join(lines).encode("utf-8")
    validate_jsonl_bytes(payload)
    return payload


def fsync_directory(path: str | os.PathLike[str]) -> None:
    """Durably record a directory entry change where the platform permits."""
    if os.name == "nt":
        return
    directory = Path(path)
    flags = getattr(os, "O_RDONLY", 0) | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(directory, flags)
    except OSError:
        # Some filesystems/platforms do not expose directory fsync.  The file
        # itself has still passed its mandatory fsync barrier.
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _stage_path(target: Path) -> Path:
    return target.parent / f".{target.name}.{uuid.uuid4().hex}.stage"


def atomic_write_bytes(
        path: str | os.PathLike[str],
        payload: bytes | bytearray | memoryview,
        *,
        validator: Callable[[bytes], None] | None = None,
        before_replace: Callable[[str], None] | None = None,
        after_replace: Callable[[str], None] | None = None,
        stage_path: str | os.PathLike[str] | None = None,
        preserve_stage_on_replace_error: bool = False,
        ) -> None:
    """Stage, flush, verify, and atomically replace one sidecar.

    The temporary file always lives beside the target, so ``os.replace`` is a
    same-filesystem operation.  The old target remains authoritative on every
    failure before the replace.
    """
    target = Path(path)
    data = bytes(payload)
    target.parent.mkdir(parents=True, exist_ok=True)
    with sidecar_lock(target):
        stage = Path(stage_path) if stage_path is not None else _stage_path(target)
        if os.path.abspath(stage.parent) != os.path.abspath(target.parent):
            raise SidecarValidationError(
                f"sidecar stage must share target directory: {target}")
        installed = False
        replace_started = False
        try:
            # A caller-supplied stage names an established recovery file that
            # the caller has already reconciled. Unique generic stages remain
            # exclusive-create so unrelated writers cannot collide.
            mode = "wb" if stage_path is not None else "xb"
            with open(stage, mode) as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())

            staged = read_bytes(stage)
            if not staged.exists or staged.data != data:
                raise SidecarValidationError(
                    f"staged sidecar bytes did not verify: {target}")
            if validator is not None:
                validator(staged.data)
            if before_replace is not None:
                before_replace(os.fspath(stage))
            replace_started = True
            os.replace(stage, target)
            installed = True
            fsync_directory(target.parent)
            if after_replace is not None:
                after_replace(os.fspath(target))
        except (SidecarError, SidecarValidationError):
            raise
        except OSError as exc:
            raise SidecarWriteError(
                f"could not commit sidecar {target}: {exc}") from exc
        finally:
            keep_stage = (
                preserve_stage_on_replace_error
                and replace_started
                and stage.exists()
            )
            if not installed and not keep_stage:
                try:
                    stage.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    # A failed cleanup must never hide the original failure.
                    pass


def atomic_write_text(
        path: str | os.PathLike[str], text: str, *,
        encoding: str = "utf-8",
        validator: Callable[[bytes], None] | None = None,
        before_replace: Callable[[str], None] | None = None,
        after_replace: Callable[[str], None] | None = None,
        ) -> None:
    atomic_write_bytes(
        path,
        text.encode(encoding),
        validator=validator,
        before_replace=before_replace,
        after_replace=after_replace,
    )


def atomic_write_jsonl(
        path: str | os.PathLike[str],
        records: Iterable[Mapping[str, Any]], *,
        before_replace: Callable[[str], None] | None = None,
        after_replace: Callable[[str], None] | None = None,
        ) -> None:
    atomic_write_bytes(
        path,
        encode_jsonl(records),
        validator=validate_jsonl_bytes,
        before_replace=before_replace,
        after_replace=after_replace,
    )


def atomic_write_json_object(
        path: str | os.PathLike[str], value: Mapping[str, Any], *,
        before_replace: Callable[[str], None] | None = None,
        after_replace: Callable[[str], None] | None = None,
        ) -> None:
    if not isinstance(value, Mapping):
        raise SidecarValidationError("JSON sidecar root is not an object")
    payload = json.dumps(dict(value), ensure_ascii=False, indent=2).encode("utf-8")

    def _validate(staged: bytes) -> None:
        try:
            parsed = json.loads(staged.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SidecarValidationError("staged JSON sidecar is invalid") from exc
        if not isinstance(parsed, dict):
            raise SidecarValidationError("staged JSON sidecar is not an object")

    atomic_write_bytes(
        path,
        payload,
        validator=_validate,
        before_replace=before_replace,
        after_replace=after_replace,
    )


def append_jsonl_object(
        path: str | os.PathLike[str], record: Mapping[str, Any], *,
        before_replace: Callable[[str], None] | None = None,
        after_replace: Callable[[str], None] | None = None,
        ) -> None:
    """Atomically append one object without ever trusting unreadable old data."""
    if not isinstance(record, Mapping):
        raise SidecarValidationError("appended JSONL record is not an object")
    with sidecar_lock(path):
        current = read_jsonl(path, invalid="raise")
        atomic_write_jsonl(
            path,
            (*current.records, dict(record)),
            before_replace=before_replace,
            after_replace=after_replace,
        )


def atomic_update_text(
        path: str | os.PathLike[str],
        transform: Callable[[str, bool], str], *,
        encoding: str = "utf-8",
        before_replace: Callable[[str], None] | None = None,
        after_replace: Callable[[str], None] | None = None,
        ) -> None:
    """Read/modify/write text while holding the shared sidecar lock."""
    with sidecar_lock(path):
        current = read_text(path)
        updated = transform(current.text, current.exists)
        if not isinstance(updated, str):
            raise SidecarValidationError("text transform did not return text")
        atomic_write_text(
            path,
            updated,
            encoding=encoding,
            before_replace=before_replace,
            after_replace=after_replace,
        )


_SAFE_MARKER_TOKEN = re.compile(r"[^A-Za-z0-9_.-]+")


def reconciliation_marker_path(
        directory: str | os.PathLike[str], *,
        operation: str,
        key: str,
        ) -> Path:
    token = _SAFE_MARKER_TOKEN.sub("-", operation).strip("-.") or "operation"
    digest = hashlib.sha256(key.encode("utf-8", errors="replace")).hexdigest()[:16]
    return Path(directory) / f".{token}.{digest}.reconcile.json"


def _validate_marker(value: Mapping[str, Any]) -> None:
    if value.get("schema") != 1:
        raise SidecarValidationError("unsupported reconciliation marker schema")
    if not isinstance(value.get("operation_id"), str):
        raise SidecarValidationError("reconciliation marker lacks operation_id")
    stores = value.get("stores")
    if not isinstance(stores, list) or not stores:
        raise SidecarValidationError("reconciliation marker has no stores")
    for store in stores:
        if (not isinstance(store, dict)
                or not isinstance(store.get("name"), str)
                or store.get("state") not in {"pending", "committed"}):
            raise SidecarValidationError("invalid reconciliation store record")
    if not isinstance(value.get("details", {}), dict):
        raise SidecarValidationError("reconciliation marker details are not an object")


def load_reconciliation_marker(
        path: str | os.PathLike[str]) -> dict[str, Any] | None:
    exists, value = read_json_object(path)
    if not exists:
        return None
    _validate_marker(value)
    return value


@dataclass(frozen=True)
class ReconciliationMarker:
    path: Path
    operation_id: str
    hide: Callable[[str], None] | None = None

    def _write(self, value: Mapping[str, Any]) -> None:
        _validate_marker(value)
        atomic_write_json_object(
            self.path,
            value,
            before_replace=self.hide,
            after_replace=self.hide,
        )

    def _mutate(self, mutator: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        with sidecar_lock(self.path):
            value = load_reconciliation_marker(self.path)
            if value is None:
                raise SidecarReadError(
                    f"reconciliation marker disappeared: {self.path}")
            if value.get("operation_id") != self.operation_id:
                raise SidecarValidationError(
                    f"reconciliation marker ownership changed: {self.path}")
            mutator(value)
            value["updated_at"] = time.time()
            self._write(value)
            return value

    def mark_committed(self, store_name: str) -> None:
        def _mark(value: dict[str, Any]) -> None:
            for store in value["stores"]:
                if store["name"] == store_name:
                    store["state"] = "committed"
                    store["committed_at"] = time.time()
                    return
            raise SidecarValidationError(
                f"unknown reconciliation store: {store_name}")
        self._mutate(_mark)

    def record_failure(self, error: object) -> None:
        def _record(value: dict[str, Any]) -> None:
            value["last_error"] = str(error)[:2000]
        self._mutate(_record)

    def finish(self) -> bool:
        """Remove a fully committed marker. Return False if cleanup failed."""
        with sidecar_lock(self.path):
            value = load_reconciliation_marker(self.path)
            if value is None:
                return True
            if value.get("operation_id") != self.operation_id:
                raise SidecarValidationError(
                    f"reconciliation marker ownership changed: {self.path}")
            if any(store["state"] != "committed" for store in value["stores"]):
                raise SidecarValidationError(
                    "cannot clear marker while a store is still pending")
            try:
                self.path.unlink()
                fsync_directory(self.path.parent)
                return True
            except FileNotFoundError:
                return True
            except OSError:
                # All-committed is itself durable, so a later reconciliation
                # pass can safely remove this harmless stale marker.
                return False


def begin_reconciliation(
        path: str | os.PathLike[str], *,
        operation: str,
        stores: Sequence[str],
        details: Mapping[str, Any] | None = None,
        hide: Callable[[str], None] | None = None,
        ) -> ReconciliationMarker:
    names = [str(name) for name in stores if str(name)]
    if not names or len(set(names)) != len(names):
        raise SidecarValidationError(
            "reconciliation stores must be unique non-empty names")
    marker = ReconciliationMarker(Path(path), uuid.uuid4().hex, hide)
    now = time.time()
    value: dict[str, Any] = {
        "schema": 1,
        "operation_id": marker.operation_id,
        "operation": operation,
        "created_at": now,
        "updated_at": now,
        "stores": [{"name": name, "state": "pending"} for name in names],
        "details": dict(details or {}),
    }
    marker._write(value)
    return marker
