"""Validated, all-or-nothing restore of YTArchiver application state.

The restore ZIP is untrusted input.  It is fully staged and validated before
any live file is touched.  A small transaction journal plus same-volume
rollback directory then make the multi-file replacement recoverable across a
crash or power loss.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
import time
import uuid
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from backend.activity_history import ACTIVITY_HISTORY_FILE
from backend.auto_backup import (
    BACKUP_MANIFEST_NAME,
    BOOKMARK_BACKUP_NAME,
    backup_file_entries,
    read_bookmark_backup,
    validate_bookmark_backup,
    write_bookmark_database,
)
from backend.services.channel_leases import (
    LeaseOwner,
    channel_leases,
    global_archive_aliases,
)
from backend.ytarchiver_config import APP_DATA_DIR, CONFIG_FILE, TRANSCRIPTION_DB

RESTORE_JOURNAL = APP_DATA_DIR / "restore_transaction.json"
_STAGE_PREFIX = ".ytarchiver-restore-stage-"
_BACKUP_SNAPSHOT_RE = re.compile(r"^backups/config_[^/\\]+\.json$")
_COPY_CHUNK = 1024 * 1024


@dataclass(frozen=True, slots=True)
class RestoreLimits:
    """Hard resource limits applied before extraction."""

    max_files: int = 512
    max_entry_bytes: int = 3 * 1024 * 1024 * 1024
    max_total_bytes: int = 5 * 1024 * 1024 * 1024
    max_compression_ratio: float = 500.0
    min_free_bytes: int = 64 * 1024 * 1024


@dataclass(slots=True)
class _StagedRestore:
    root: Path
    live_dir: Path
    included: dict[str, Path]
    manifest: dict[str, Any]
    digests: dict[str, str]
    total_bytes: int
    bookmarks_source: str = "none"
    bookmark_count: int = 0


class RestoreError(RuntimeError):
    """A backup is invalid or could not be committed safely."""


def _resource_targets() -> dict[str, Path]:
    targets = {str(name): Path(path) for name, path in backup_file_entries()}
    targets[TRANSCRIPTION_DB.name] = TRANSCRIPTION_DB
    targets[BOOKMARK_BACKUP_NAME] = APP_DATA_DIR / BOOKMARK_BACKUP_NAME
    return targets


def _resolved(path: Path | str) -> Path:
    return Path(path).resolve(strict=False)


def _is_within(path: Path | str, parent: Path | str) -> bool:
    try:
        _resolved(path).relative_to(_resolved(parent))
        return True
    except (OSError, ValueError):
        return False


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            json.dump(dict(value), handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
        tmp_name = ""
    finally:
        if fd >= 0:
            os.close(fd)
        if tmp_name:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass


def _safe_member_name(raw_name: str) -> str:
    if not raw_name or "\x00" in raw_name or "\\" in raw_name:
        raise RestoreError(f"Unsafe ZIP member name: {raw_name!r}")
    if raw_name.startswith("/") or (len(raw_name) > 1 and raw_name[1] == ":"):
        raise RestoreError(f"Absolute ZIP member path rejected: {raw_name!r}")
    path = PurePosixPath(raw_name)
    if any(part in {"", ".", ".."} for part in path.parts):
        raise RestoreError(f"Traversal ZIP member rejected: {raw_name!r}")
    return path.as_posix()


def _target_for_member(name: str, targets: Mapping[str, Path]) -> Path | None:
    if name == BACKUP_MANIFEST_NAME:
        return None
    if _BACKUP_SNAPSHOT_RE.fullmatch(name):
        return APP_DATA_DIR / name
    if "/" in name or name not in targets:
        raise RestoreError(f"Backup contains an unknown resource: {name}")
    return targets[name]


def _manifest_hashes(manifest: Mapping[str, Any]) -> dict[str, tuple[str, int | None]]:
    result: dict[str, tuple[str, int | None]] = {}
    resources = manifest.get("resources")
    if isinstance(resources, dict):
        for name, details in resources.items():
            if not isinstance(details, dict):
                continue
            digest = str(details.get("sha256") or "").strip().lower()
            size = details.get("size")
            if digest:
                result[str(name)] = (
                    digest,
                    int(size) if isinstance(size, int) and size >= 0 else None,
                )
    files = manifest.get("files")
    if isinstance(files, list):
        for details in files:
            if not isinstance(details, dict):
                continue
            name = str(details.get("name") or "")
            digest = str(details.get("sha256") or "").strip().lower()
            size = details.get("size")
            if name and digest:
                result[name] = (
                    digest,
                    int(size) if isinstance(size, int) and size >= 0 else None,
                )
    return result


def _validate_json_resource(name: str, path: Path) -> None:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, UnicodeError, ValueError) as exc:
        raise RestoreError(f"{name} is not valid JSON: {exc}") from exc
    if not isinstance(value, (dict, list)):
        raise RestoreError(f"{name} must contain a JSON object or list")
    if name == CONFIG_FILE.name:
        if not isinstance(value, dict) or not isinstance(value.get("channels", []), list):
            raise RestoreError("Restored config has an invalid channels list")
    if name == BOOKMARK_BACKUP_NAME:
        validate_bookmark_backup(value)
    if name.endswith("_queue.json"):
        if not isinstance(value, dict):
            raise RestoreError("Restored queue must be a JSON object")
        for lane in ("sync", "gpu"):
            if lane in value and not isinstance(value[lane], list):
                raise RestoreError(f"Restored queue lane {lane!r} is invalid")
        if "resuming" in value and not isinstance(value["resuming"], dict):
            raise RestoreError("Restored queue resuming state is invalid")
    if name.endswith("_queue_resuming.json"):
        if not isinstance(value, dict) or not isinstance(value.get("resuming", {}), dict):
            raise RestoreError("Restored queue sidecar is invalid")


def _validate_activity_history(path: Path) -> None:
    """Validate every canonical activity-history JSONL record."""
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                value = json.loads(line)
                if isinstance(value, str) and value:
                    continue
                if (
                    isinstance(value, dict)
                    and isinstance(value.get("entry"), str)
                    and value["entry"]
                ):
                    continue
                raise ValueError(f"invalid record on line {line_number}")
    except (OSError, UnicodeError, ValueError) as exc:
        raise RestoreError(
            f"Restored activity history is invalid: {exc}") from exc


def _validate_sqlite(path: Path) -> None:
    try:
        uri = f"file:{path.as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        try:
            row = connection.execute("PRAGMA quick_check").fetchone()
        finally:
            connection.close()
    except sqlite3.Error as exc:
        raise RestoreError(f"Restored search database is invalid: {exc}") from exc
    if not row or str(row[0]).lower() != "ok":
        raise RestoreError(f"Restored search database failed quick_check: {row}")


def stage_backup(
    zip_path: str | os.PathLike[str],
    *,
    limits: RestoreLimits | None = None,
) -> _StagedRestore:
    """Extract and validate an entire backup without changing live state."""
    limits = limits or RestoreLimits()
    archive = Path(zip_path)
    if not archive.is_file():
        raise RestoreError("Backup ZIP does not exist")

    parent = APP_DATA_DIR.parent
    parent.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix=_STAGE_PREFIX, dir=str(parent)))
    live_dir = root / "live"
    live_dir.mkdir()
    targets = _resource_targets()
    included: dict[str, Path] = {}
    digests: dict[str, str] = {}
    manifest: dict[str, Any] = {}
    total_bytes = 0
    try:
        with zipfile.ZipFile(archive, "r") as zipped:
            infos = [info for info in zipped.infolist() if not info.is_dir()]
            if not infos:
                raise RestoreError("Backup is empty")
            if len(infos) > limits.max_files:
                raise RestoreError(
                    f"Backup has too many files ({len(infos)} > {limits.max_files})"
                )

            planned: list[tuple[zipfile.ZipInfo, str, Path | None]] = []
            seen_names: set[str] = set()
            seen_targets: set[str] = set()
            for info in infos:
                name = _safe_member_name(info.filename)
                if name in seen_names:
                    raise RestoreError(f"Backup contains duplicate member {name!r}")
                seen_names.add(name)
                if info.flag_bits & 0x1:
                    raise RestoreError(f"Encrypted ZIP member rejected: {name}")
                if info.file_size < 0 or info.file_size > limits.max_entry_bytes:
                    raise RestoreError(f"Backup member is too large: {name}")
                total_bytes += int(info.file_size)
                if total_bytes > limits.max_total_bytes:
                    raise RestoreError("Backup expands beyond the total-size limit")
                if info.file_size and info.compress_size == 0:
                    raise RestoreError(f"Backup member has an invalid compressed size: {name}")
                ratio = info.file_size / max(1, info.compress_size)
                if ratio > limits.max_compression_ratio:
                    raise RestoreError(
                        f"Backup member compression ratio is unsafe: {name}"
                    )
                target = _target_for_member(name, targets)
                if target is not None:
                    if not _is_within(target, APP_DATA_DIR):
                        raise RestoreError(f"Restore target escapes app data: {name}")
                    target_key = os.path.normcase(str(_resolved(target)))
                    if target_key in seen_targets:
                        raise RestoreError(f"Two ZIP members target the same file: {name}")
                    seen_targets.add(target_key)
                planned.append((info, name, target))

            if CONFIG_FILE.name not in seen_names:
                raise RestoreError("Backup does not contain the application config")
            free = shutil.disk_usage(parent).free
            required = total_bytes + max(limits.min_free_bytes, total_bytes // 10)
            if free < required:
                raise RestoreError(
                    "Not enough free space to stage this backup safely "
                    f"({required} bytes required, {free} available)"
                )

            for info, name, target in planned:
                relative = Path(name) if target is None else _resolved(target).relative_to(
                    _resolved(APP_DATA_DIR)
                )
                staged = live_dir / relative
                staged.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                written = 0
                with zipped.open(info, "r") as source, staged.open("wb") as destination:
                    while True:
                        chunk = source.read(_COPY_CHUNK)
                        if not chunk:
                            break
                        destination.write(chunk)
                        digest.update(chunk)
                        written += len(chunk)
                    destination.flush()
                    os.fsync(destination.fileno())
                if written != info.file_size:
                    raise RestoreError(f"Backup member was truncated while reading: {name}")
                digests[name] = digest.hexdigest()
                if target is not None:
                    included[name] = staged
                elif name == BACKUP_MANIFEST_NAME:
                    try:
                        loaded = json.loads(staged.read_text(encoding="utf-8"))
                    except (OSError, UnicodeError, ValueError) as exc:
                        raise RestoreError(f"Backup manifest is invalid: {exc}") from exc
                    if not isinstance(loaded, dict):
                        raise RestoreError("Backup manifest must be a JSON object")
                    manifest = loaded

        expected_hashes = _manifest_hashes(manifest)
        try:
            manifest_version = int(manifest.get("manifest_version") or 0)
        except (TypeError, ValueError):
            manifest_version = 0
        if manifest_version >= 2:
            # A v2 manifest is an allow-list for every live state resource in
            # the archive.  Merely checking the hashes it happens to mention
            # lets an attacker omit (for example) config.json from
            # ``resources`` and then alter it without detection.  Historical
            # config snapshots are additive convenience files and were not
            # included in early v2 manifests, so they remain outside this
            # exact-coverage rule.
            covered_members = {
                name
                for _info, name, target in planned
                if target is not None and not _BACKUP_SNAPSHOT_RE.fullmatch(name)
            }
            missing_hashes = sorted(covered_members - set(expected_hashes))
            if missing_hashes:
                raise RestoreError(
                    "Backup manifest omits restored resource hash(es): "
                    + ", ".join(missing_hashes)
                )
        for name, (expected_digest, expected_size) in expected_hashes.items():
            if name not in digests:
                raise RestoreError(f"Manifest references missing resource: {name}")
            if digests[name].lower() != expected_digest:
                raise RestoreError(f"Checksum mismatch for restored resource: {name}")
            if expected_size is not None:
                actual_size = (live_dir / Path(name)).stat().st_size
                if actual_size != expected_size:
                    raise RestoreError(f"Size mismatch for restored resource: {name}")

        for name, staged in included.items():
            if name.endswith(".json"):
                _validate_json_resource(name, staged)
            elif name == ACTIVITY_HISTORY_FILE.name:
                _validate_activity_history(staged)
            elif name == TRANSCRIPTION_DB.name:
                _validate_sqlite(staged)

        bookmarks_source = "none"
        bookmark_count = 0
        if BOOKMARK_BACKUP_NAME in included:
            payload = json.loads(included[BOOKMARK_BACKUP_NAME].read_text(encoding="utf-8"))
            bookmarks_source = "backup"
            bookmark_count = len(payload["bookmarks"])
            if TRANSCRIPTION_DB.name not in included:
                seeded = live_dir / TRANSCRIPTION_DB.name
                write_bookmark_database(seeded, payload)
                included[TRANSCRIPTION_DB.name] = seeded
        elif TRANSCRIPTION_DB.name in included:
            payload = read_bookmark_backup(included[TRANSCRIPTION_DB.name])
            bookmarks_source = "backup"
            bookmark_count = len(payload["bookmarks"])
        return _StagedRestore(
            root=root,
            live_dir=live_dir,
            included=included,
            manifest=manifest,
            digests=digests,
            total_bytes=total_bytes,
            bookmarks_source=bookmarks_source,
            bookmark_count=bookmark_count,
        )
    except Exception:
        shutil.rmtree(root, ignore_errors=True)
        raise


def _snapshot_current_config() -> str | None:
    if not CONFIG_FILE.is_file():
        return None
    backup_dir = APP_DATA_DIR / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y-%m-%d_%H%M%S", time.localtime())
    target = backup_dir / f"config_pre_restore_{stamp}_{uuid.uuid4().hex[:8]}.json"
    shutil.copy2(CONFIG_FILE, target)
    return str(target)


def _write_journal(record: dict[str, Any]) -> None:
    record["updated_at"] = time.time()
    _atomic_json(RESTORE_JOURNAL, record)


def _remove_or_park(path: Path, park_dir: Path) -> None:
    if not path.exists():
        return
    park_dir.mkdir(parents=True, exist_ok=True)
    destination = park_dir / f"{uuid.uuid4().hex}-{path.name}"
    os.replace(path, destination)


def _rollback_record(record: dict[str, Any]) -> tuple[bool, str]:
    entries = record.get("entries")
    if not isinstance(entries, list):
        return False, "restore journal has no valid entry list"
    root = Path(str(record.get("stage_root") or ""))
    if not _is_within(root, APP_DATA_DIR.parent) or not root.name.startswith(_STAGE_PREFIX):
        return False, "restore journal has an unsafe staging path"
    failed_new = root / "failed-new"
    errors: list[str] = []
    record["state"] = "rolling_back"
    try:
        _write_journal(record)
    except OSError:
        # The existing journal still describes enough filesystem state for
        # the idempotent inference below.  Continue restoring user data.
        pass
    for entry in reversed(entries):
        if not isinstance(entry, dict):
            errors.append("malformed restore entry")
            continue
        if entry.get("rollback_done"):
            continue
        target = Path(str(entry.get("target") or ""))
        old = Path(str(entry.get("old_backup") or ""))
        had_old = bool(entry.get("had_old"))
        if not _is_within(target, APP_DATA_DIR) or not _is_within(old, root):
            errors.append(f"unsafe restore entry for {target}")
            continue
        try:
            if old.exists():
                _remove_or_park(target, failed_new)
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(old, target)
            elif not had_old:
                # The old snapshot was absence.  This remains safe on a
                # repeated rollback: parking a missing path is a no-op.
                _remove_or_park(target, failed_new)
            elif not target.exists():
                # With an old generation recorded, ``old`` can be missing
                # only before it was moved or after it was moved back.  In
                # both normal cases the target exists.  Absence of both is a
                # genuine manual-recovery condition.
                errors.append(f"missing rollback copy for {target}")
                continue
            # had_old + old missing + target present means either this entry
            # had not begun committing or a prior rollback already restored
            # it.  Treat both as the same idempotent completed state.
            entry["rollback_done"] = True
            try:
                _write_journal(record)
            except OSError:
                pass
        except OSError as exc:
            errors.append(f"{target}: {exc}")
    return not errors, "; ".join(errors)


def _cleanup_transaction(record: Mapping[str, Any]) -> None:
    root = Path(str(record.get("stage_root") or ""))
    if _is_within(root, APP_DATA_DIR.parent) and root.name.startswith(_STAGE_PREFIX):
        shutil.rmtree(root, ignore_errors=True)
    try:
        RESTORE_JOURNAL.unlink(missing_ok=True)
    except OSError:
        pass


def recover_interrupted_restore() -> dict[str, Any]:
    """Resolve a crash-time restore before any state owner starts."""
    try:
        with RESTORE_JOURNAL.open("r", encoding="utf-8") as handle:
            record = json.load(handle)
    except FileNotFoundError:
        return {"ok": True, "recovered": False}
    except (OSError, UnicodeError, ValueError) as exc:
        return {
            "ok": False,
            "recovery_required": True,
            "error": f"Restore recovery journal is unreadable: {exc}",
        }
    if not isinstance(record, dict):
        return {
            "ok": False,
            "recovery_required": True,
            "error": "Restore recovery journal is malformed",
        }
    if record.get("state") == "committed":
        _cleanup_transaction(record)
        return {"ok": True, "recovered": True, "action": "commit-kept"}
    rolled_back, error = _rollback_record(record)
    if not rolled_back:
        return {
            "ok": False,
            "recovery_required": True,
            "error": error or "Restore rollback could not be completed",
        }
    _cleanup_transaction(record)
    return {"ok": True, "recovered": True, "action": "rolled-back"}


def _commit_staged(stage: _StagedRestore) -> dict[str, Any]:
    targets = _resource_targets()
    rollback_dir = stage.root / "rollback"
    rollback_dir.mkdir()
    entries: list[dict[str, Any]] = []
    included_by_target = {
        os.path.normcase(str(_resolved(targets[name]))): staged
        for name, staged in stage.included.items()
        if name in targets
    }
    # Every exported top-level resource has snapshot semantics.  If it is not
    # in the backup, its restored state is absent.  SQLite WAL/SHM files are
    # always absent because the backed-up DB is a self-contained snapshot.
    live_targets = list(dict.fromkeys(targets.values()))
    live_targets.extend(
        [Path(f"{TRANSCRIPTION_DB}-wal"), Path(f"{TRANSCRIPTION_DB}-shm")]
    )
    for index, target in enumerate(live_targets):
        target = Path(target)
        staged = included_by_target.get(os.path.normcase(str(_resolved(target))))
        old_backup = rollback_dir / f"{index:04d}-{target.name}"
        entries.append(
            {
                "target": str(target),
                "staged": str(staged) if staged is not None else "",
                "old_backup": str(old_backup),
                "had_old": target.exists(),
                "commit_state": "prepared",
            }
        )
    # Historical config snapshots in a backup are additive; restoring one
    # must not erase newer local safety snapshots.
    for name, staged in stage.included.items():
        if name in targets:
            continue
        target = APP_DATA_DIR / name
        old_backup = rollback_dir / f"extra-{uuid.uuid4().hex}-{target.name}"
        entries.append(
            {
                "target": str(target),
                "staged": str(staged),
                "old_backup": str(old_backup),
                "had_old": target.exists(),
                "commit_state": "prepared",
            }
        )

    record: dict[str, Any] = {
        "version": 1,
        "state": "prepared",
        "stage_root": str(stage.root),
        "entries": entries,
    }
    _write_journal(record)
    try:
        record["state"] = "committing"
        _write_journal(record)
        for entry in entries:
            target = Path(entry["target"])
            old = Path(entry["old_backup"])
            staged_raw = str(entry.get("staged") or "")
            staged = Path(staged_raw) if staged_raw else None
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                old.parent.mkdir(parents=True, exist_ok=True)
                os.replace(target, old)
                entry["commit_state"] = "old_saved"
                _write_journal(record)
            if staged is not None:
                os.replace(staged, target)
                entry["commit_state"] = "new_installed"
            else:
                entry["commit_state"] = "new_absent"
            _write_journal(record)
        record["state"] = "committed"
        _write_journal(record)
    except Exception as exc:
        rolled_back, rollback_error = _rollback_record(record)
        if rolled_back:
            _cleanup_transaction(record)
            raise RestoreError(f"Restore commit failed and was rolled back: {exc}") from exc
        record["state"] = "rollback_failed"
        record["error"] = str(exc)
        record["rollback_error"] = rollback_error
        try:
            _write_journal(record)
        except OSError:
            pass
        raise RestoreError(
            "Restore failed and automatic rollback also failed; restart is "
            f"blocked until recovery completes: {rollback_error or exc}"
        ) from exc

    restored = [name for name in stage.included if name in targets]
    removed = [
        Path(entry["target"]).name
        for entry in entries
        if not entry.get("staged") and entry.get("had_old")
    ]
    _cleanup_transaction(record)
    return {"restored": restored, "removed": removed}


def restore_backup(
    zip_path: str | os.PathLike[str],
    *,
    before_commit: Callable[[], bool | Mapping[str, Any] | None] | None = None,
    limits: RestoreLimits | None = None,
    lease_timeout: float = 10.0,
) -> dict[str, Any]:
    """Stage, quiesce, and atomically replace the complete app snapshot."""
    stage: _StagedRestore | None = None
    lease = None
    # Keep the quiesce result available if the coordinator has to report a
    # failure.  Some quiesce failures cross the process's one-way freeze
    # boundary, in which case the UI must require a restart before any more
    # work is attempted.
    quiesce_result: Mapping[str, Any] | None = None
    try:
        stage = stage_backup(zip_path, limits=limits)
        if before_commit is not None:
            quiesced = before_commit()
            if isinstance(quiesced, Mapping):
                quiesce_result = quiesced
            if quiesced is False or (
                isinstance(quiesced, Mapping) and not quiesced.get("ok", False)
            ):
                detail = (
                    str(quiesced.get("error") or "")
                    if isinstance(quiesced, Mapping)
                    else ""
                )
                raise RestoreError(
                    "Active work could not be stopped for restore"
                    + (f": {detail}" if detail else "")
                )
        owner = LeaseOwner(
            "restore", uuid.uuid4().hex, label="Restore application backup"
        )
        acquired = channel_leases.acquire(
            global_archive_aliases(), owner, timeout=lease_timeout
        )
        if not acquired.ok or acquired.lease is None:
            raise RestoreError(acquired.explanation)
        lease = acquired.lease
        # Older backups sometimes omitted the whole index, including notes.
        # Extract only authored state after quiescing; do not keep a stale
        # catalog against the newly restored configuration.
        if TRANSCRIPTION_DB.name not in stage.included:
            try:
                payload = read_bookmark_backup(TRANSCRIPTION_DB)
                if payload["bookmarks"]:
                    seeded = stage.live_dir / TRANSCRIPTION_DB.name
                    write_bookmark_database(seeded, payload)
                    stage.included[TRANSCRIPTION_DB.name] = seeded
                    stage.bookmarks_source = "current_installation"
                    stage.bookmark_count = len(payload["bookmarks"])
            except (OSError, ValueError, sqlite3.Error) as exc:
                raise RestoreError(
                    "This older backup contains no bookmarks and current bookmarks "
                    "could not be preserved; no application state was changed: "
                    + str(exc)) from exc
        snapshot = _snapshot_current_config()
        bookmarks_source = stage.bookmarks_source
        bookmark_count = stage.bookmark_count
        committed = _commit_staged(stage)
        stage = None
        return {
            "ok": True,
            "files_restored": len(committed["restored"]),
            "restored": committed["restored"],
            "removed": committed["removed"],
            "pre_restore_snapshot": snapshot,
            "needs_restart": True,
            "state_already_committed": True,
            "bookmarks_source": bookmarks_source,
            "bookmark_count": bookmark_count,
        }
    except (OSError, ValueError, sqlite3.Error, zipfile.BadZipFile, RestoreError) as exc:
        result = {
            "ok": False,
            "error": str(exc),
            "recovery_required": RESTORE_JOURNAL.exists(),
        }
        if quiesce_result and quiesce_result.get("needs_restart"):
            result["needs_restart"] = True
        return result
    finally:
        if lease is not None:
            lease.release()
        if stage is not None and not RESTORE_JOURNAL.exists():
            shutil.rmtree(stage.root, ignore_errors=True)
