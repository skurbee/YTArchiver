"""Durable, stable-ID activity history.

Activity rows used to be split between ``config.json`` and a JSONL file.
That made a whole-config save part of normal logging and allowed the two
histories to disagree.  This repository is the only writer for activity
history.  It accepts the old JSON-string JSONL format on read and rewrites
records as ``{"id": ..., "entry": ...}`` objects on the next mutation.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from .ytarchiver_config import APP_DATA_DIR

_log = logging.getLogger(__name__)
ACTIVITY_HISTORY_FILE = APP_DATA_DIR / "autorun_history.jsonl"
ACTIVITY_HISTORY_MAX = 10_000
_STORE_LOCKS_GUARD = threading.Lock()
_STORE_LOCKS: dict[str, threading.RLock] = {}


def _shared_store_lock(path: Path) -> threading.RLock:
    """Return the process-wide lock for one physical history file.

    Sync and autorun construct short-lived repository objects.  A lock owned
    by each object does not protect their shared read/merge/replace sequence,
    so two writers can otherwise both read generation N and one can erase the
    other's generation N+1 row.
    """
    key = os.path.normcase(os.path.abspath(os.fspath(path)))
    with _STORE_LOCKS_GUARD:
        lock = _STORE_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _STORE_LOCKS[key] = lock
        return lock


def _legacy_id(source: str, index: int, entry: str) -> str:
    digest = hashlib.sha256(
        f"{source}\0{index}\0{entry}".encode("utf-8", "replace")
    ).hexdigest()[:24]
    return f"legacy_{digest}"


class ActivityHistoryStore:
    """Thread-safe JSONL repository with atomic stable-ID upserts."""

    def __init__(self, path: str | os.PathLike[str] | Path,
                 max_entries: int = ACTIVITY_HISTORY_MAX):
        self.path = Path(path)
        self.max_entries = max(1, int(max_entries))
        self._lock = _shared_store_lock(self.path)

    @staticmethod
    def _coerce_record(value: Any, *, index: int,
                       source: str = "jsonl") -> dict[str, Any] | None:
        if isinstance(value, str):
            entry = value
            row_id = _legacy_id(source, index, entry)
            updated_at = 0.0
        elif isinstance(value, dict) and isinstance(value.get("entry"), str):
            entry = value["entry"]
            row_id = str(value.get("id") or "").strip()
            if not row_id:
                row_id = _legacy_id(source, index, entry)
            try:
                updated_at = float(value.get("updated_at") or 0.0)
            except (TypeError, ValueError):
                updated_at = 0.0
        else:
            return None
        if not entry:
            return None
        return {"id": row_id, "entry": entry, "updated_at": updated_at}

    def _read_locked(self) -> list[dict[str, Any]]:
        if not self.path.is_file():
            return []
        records: list[dict[str, Any]] = []
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                for index, line in enumerate(handle):
                    try:
                        value = json.loads(line)
                    except (TypeError, ValueError):
                        continue
                    record = self._coerce_record(value, index=index)
                    if record is not None:
                        records.append(record)
        except OSError as exc:
            _log.warning("activity history read failed: %s", exc)
            return []
        return records[-self.max_entries:]

    def _write_locked(self, records: list[dict[str, Any]]) -> bool:
        tmp = ""
        fd = -1
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(
                prefix=f".{self.path.name}.", suffix=".tmp",
                dir=str(self.path.parent),
            )
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                fd = -1
                for record in records[-self.max_entries:]:
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)
            tmp = ""
            return True
        except OSError as exc:
            _log.warning("activity history save failed: %s", exc)
            return False
        finally:
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
            if tmp:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

    def entries(self) -> list[str]:
        with self._lock:
            return [record["entry"] for record in self._read_locked()]

    def records(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(record) for record in self._read_locked()]

    def append(self, entry: str, *, entry_id: str = "") -> str:
        row_id = str(entry_id or "").strip() or f"activity_{uuid.uuid4().hex}"
        if not isinstance(entry, str) or not entry:
            return ""
        return row_id if self.upsert(row_id, entry) else ""

    def upsert(self, entry_id: str, entry: str) -> bool:
        row_id = str(entry_id or "").strip()
        if not row_id or not isinstance(entry, str) or not entry:
            return False
        with self._lock:
            records = self._read_locked()
            replacement = {
                "id": row_id,
                "entry": entry,
                "updated_at": time.time(),
            }
            for index, record in enumerate(records):
                if record.get("id") == row_id:
                    records[index] = replacement
                    break
            else:
                records.append(replacement)
            return self._write_locked(records[-self.max_entries:])

    def migrate_legacy(self, entries: Iterable[str]) -> bool:
        """Merge legacy config entries once without duplicating retries."""
        legacy = [entry for entry in entries if isinstance(entry, str) and entry]
        if not legacy:
            return True
        with self._lock:
            records = self._read_locked()
            known_ids = {str(record.get("id") or "") for record in records}
            for index, entry in enumerate(legacy):
                row_id = _legacy_id("config", index, entry)
                if row_id in known_ids:
                    continue
                records.append({
                    "id": row_id,
                    "entry": entry,
                    "updated_at": 0.0,
                })
                known_ids.add(row_id)
            return self._write_locked(records[-self.max_entries:])

    def clear(self, retire_legacy: Callable[[], int] | None = None) -> int:
        """Clear JSONL and, optionally, its legacy source under one lock.

        ``retire_legacy`` runs after the JSONL unlink while the process-wide
        lock for this physical history file is still held.  Appends and
        migrations therefore happen wholly before or wholly after a clear;
        they cannot land in the gap and be mistaken for pre-clear history.
        If retiring the legacy source fails, the JSONL generation is restored
        before the error is re-raised.
        """
        with self._lock:
            records = self._read_locked()
            existed = self.path.is_file()
            try:
                self.path.unlink(missing_ok=True)
            except OSError as exc:
                _log.warning("activity history clear failed: %s", exc)
                return -1
            try:
                legacy_removed = (
                    max(0, int(retire_legacy() or 0))
                    if retire_legacy is not None else 0
                )
            except Exception:
                if existed and not self._write_locked(records):
                    _log.error(
                        "activity history rollback failed after legacy "
                        "retirement error"
                    )
                raise
            return len(records) + legacy_removed


ACTIVITY_HISTORY = ActivityHistoryStore(ACTIVITY_HISTORY_FILE)
