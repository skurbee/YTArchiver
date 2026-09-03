"""Durable file repository for queue and crash-resume state."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

ReadState = Literal["ok", "missing", "sidelined", "blocked"]


@dataclass(frozen=True, slots=True)
class QueueReadResult:
    state: ReadState
    data: dict[str, Any]
    error: str = ""

    @property
    def exists(self) -> bool:
        return self.state == "ok"


@dataclass(frozen=True, slots=True)
class QueueCommitResult:
    ok: bool
    path: str
    error: str = ""


class QueueRepository:
    """Own queue-file parsing, corruption quarantine, and atomic commits."""

    def __init__(self, main_path: str | Path):
        self.main_path = Path(main_path)

    @property
    def resuming_path(self) -> Path:
        suffix = self.main_path.suffix or ".json"
        return self.main_path.with_name(
            f"{self.main_path.stem}_resuming{suffix}"
        )

    @staticmethod
    def _sidelined_path(path: Path) -> Path:
        return Path(f"{path}.bak")

    def _read_object(self, path: Path) -> QueueReadResult:
        if not path.exists():
            return QueueReadResult("missing", {})
        try:
            raw = path.read_text(encoding="utf-8")
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError("queue root must be an object")
            return QueueReadResult("ok", value)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            try:
                os.replace(path, self._sidelined_path(path))
            except OSError as backup_exc:
                return QueueReadResult(
                    "blocked", {},
                    f"{exc}; corrupt file could not be preserved: {backup_exc}",
                )
            return QueueReadResult("sidelined", {}, str(exc))

    def load_main(self) -> QueueReadResult:
        return self._read_object(self.main_path)

    def load_resuming(self) -> QueueReadResult:
        result = self._read_object(self.resuming_path)
        if result.state != "ok":
            return result
        resuming = result.data.get("resuming")
        if isinstance(resuming, dict):
            return QueueReadResult("ok", dict(resuming))
        try:
            os.replace(
                self.resuming_path,
                self._sidelined_path(self.resuming_path),
            )
        except OSError as exc:
            return QueueReadResult(
                "blocked", {},
                f"resuming state is malformed and could not be preserved: {exc}",
            )
        return QueueReadResult(
            "sidelined", {}, "resuming state must be an object",
        )

    @staticmethod
    def _commit(path: Path, payload: dict[str, Any]) -> QueueCommitResult:
        temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(temp, "w", encoding="utf-8", newline="\n") as stream:
                json.dump(payload, stream, indent=2)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, path)
            return QueueCommitResult(True, str(path))
        except (OSError, TypeError, ValueError) as exc:
            try:
                temp.unlink(missing_ok=True)
            except OSError:
                pass
            return QueueCommitResult(False, str(path), str(exc))

    def commit_main(self, payload: dict[str, Any]) -> QueueCommitResult:
        return self._commit(self.main_path, payload)

    def commit_resuming(self, payload: dict[str, Any]) -> QueueCommitResult:
        return self._commit(self.resuming_path, payload)


__all__ = [
    "QueueCommitResult",
    "QueueReadResult",
    "QueueRepository",
]
