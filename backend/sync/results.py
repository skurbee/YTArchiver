"""Sync outcome interpretation shared by producers and queue completion."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class SyncStatus(StrEnum):
    SUCCESS = "success"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class SyncOutcome:
    status: SyncStatus
    errors: int = 0

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "SyncOutcome":
        cancelled = bool(payload.get("cancelled"))
        errors = max(0, int(payload.get("errors", 0) or 0))
        if cancelled:
            return cls(SyncStatus.CANCELLED, errors)
        if not bool(payload.get("ok", True)) or errors:
            return cls(SyncStatus.FAILED, max(1, errors))
        return cls(SyncStatus.SUCCESS)


class SyncResult(dict[str, Any]):
    """JSON-compatible result with an explicit constructor and outcome policy.

    Supplemental counters remain available to the bridge. The typed outcome
    normalizes failure accounting without changing historical count fields.
    """

    def __init__(self, *, ok: bool, downloaded: int = 0, errors: int = 0,
                 reason: str = "", **details: Any) -> None:
        super().__init__(ok=bool(ok), downloaded=max(0, int(downloaded)),
                         errors=max(0, int(errors)), **details)
        if reason:
            self["reason"] = reason

    @property
    def outcome(self) -> SyncOutcome:
        return SyncOutcome.from_payload(self)
