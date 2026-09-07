"""Append/recover storage for per-media provenance checkpoints.

Read history once per pass, append one durable line per success, and compact
superseded identities at the next writable open. An incomplete final line is
recoverable; malformed committed lines are never discarded silently.
"""

import hashlib
import json
import os
import uuid
from pathlib import Path

from .sidecar_store import (
    SidecarReadError,
    SidecarValidationError,
    SidecarWriteError,
    atomic_write_bytes,
    atomic_write_jsonl,
    read_bytes,
    sidecar_lock,
)


def _payload(record):
    return json.dumps(record, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _validate(record):
    if (not isinstance(record, dict) or not isinstance(record.get("path"), str)
            or not record["path"] or not isinstance(record.get("size"), int)
            or record["size"] < 0 or not isinstance(record.get("mtime"), int)):
        raise SidecarValidationError("Invalid provenance checkpoint record")
    checksum = record.get("_checksum")
    if checksum is not None:
        data = {key: value for key, value in record.items() if key != "_checksum"}
        if checksum != hashlib.sha256(_payload(data)).hexdigest():
            raise SidecarValidationError("Provenance checkpoint checksum mismatch")


class ProvenanceLedger:
    def __init__(self, path):
        self.path = Path(path)
        self.records = {}
        self._signature = None
        self._loaded = False

    def _stat_signature(self):
        try:
            value = self.path.stat()
            return value.st_ino, value.st_size, value.st_mtime_ns
        except FileNotFoundError:
            return (0, 0, 0)
        except OSError as exc:
            raise SidecarReadError(str(exc)) from exc

    def load(self, *, recover=False):
        with sidecar_lock(self.path):
            snapshot = read_bytes(self.path)
            records = {}
            count = 0
            valid_end = 0
            tail = b""
            lines = snapshot.data.splitlines(keepends=True)
            for number, line in enumerate(lines):
                try:
                    if line.strip():
                        record = json.loads(line.decode("utf-8-sig"))
                        _validate(record)
                        records[os.path.normcase(record["path"])] = record
                        count += 1
                except (UnicodeError, ValueError, SidecarValidationError) as exc:
                    if number == len(lines) - 1 and not line.endswith(b"\n"):
                        tail = line
                        break
                    raise SidecarValidationError(
                        f"Unreadable provenance history at line {number + 1}: {exc}") from exc
                valid_end += len(line)
            if tail and recover:
                # Preserve the exact interrupted bytes before replacing the
                # ledger with its verified prefix. Never edit during preview.
                evidence = self.path.with_name(
                    f".{self.path.name}.{uuid.uuid4().hex}.interrupted")
                atomic_write_bytes(evidence, snapshot.data)
                atomic_write_bytes(self.path, snapshot.data[:valid_end])
            elif tail:
                # Read-only preview may use the valid prefix, but this owner
                # cannot append until an explicit writable recovery.
                self._loaded = False
                self.records = records
                return self._completed()
            if recover and count > max(1024, len(records) * 2):
                atomic_write_jsonl(self.path, records.values())
            elif recover and snapshot.data and not tail and not snapshot.data.endswith(b"\n"):
                # A valid legacy final record without a newline is preserved.
                atomic_write_bytes(self.path, snapshot.data + b"\n")
            self.records = records
            self._signature = self._stat_signature()
            self._loaded = bool(recover or not snapshot.data or snapshot.data.endswith(b"\n"))
            return self._completed()

    def _completed(self):
        return {key: (row["size"], row["mtime"]) for key, row in self.records.items()}

    def append(self, record):
        record = dict(record)
        _validate(record)
        record.pop("_checksum", None)
        record["_checksum"] = hashlib.sha256(_payload(record)).hexdigest()
        line = _payload(record) + b"\n"
        with sidecar_lock(self.path):
            if not self._loaded or self._stat_signature() != self._signature:
                self.load(recover=True)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(self.path, "ab") as stream:
                    start = stream.tell()
                    try:
                        if stream.write(line) != len(line):
                            raise OSError("Short provenance checkpoint write")
                        stream.flush()
                        os.fsync(stream.fileno())
                    except BaseException:
                        # Best effort undo of this exact append only. If this
                        # cannot complete, the next owner verifies/replays the
                        # prefix and preserves any torn tail as evidence.
                        try:
                            stream.seek(start)
                            stream.truncate()
                            stream.flush()
                            os.fsync(stream.fileno())
                        except OSError:
                            pass
                        raise
            except OSError as exc:
                self._loaded = False
                raise SidecarWriteError(str(exc)) from exc
            self.records[os.path.normcase(record["path"])] = record
            self._signature = self._stat_signature()
