"""Admission result for a processing task, distinct from its later outcome."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class EnqueueStatus(StrEnum):
    ACCEPTED = "accepted"
    SHUTDOWN = "shutdown"
    MISSING_FILE = "missing_file"
    DUPLICATE = "duplicate"
    SAVE_FAILED = "save_failed"


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    status: EnqueueStatus
    task_id: str = ""
    error: str = ""

    @property
    def accepted(self) -> bool:
        return self.status is EnqueueStatus.ACCEPTED

    def api_payload(self) -> dict[str, Any]:
        result: dict[str, Any] = {"ok": self.accepted, "code": self.status.value}
        if self.accepted:
            result["task_id"] = self.task_id
        else:
            result.update(error=self.error, retryable=self.status is EnqueueStatus.SAVE_FAILED)
        return result
