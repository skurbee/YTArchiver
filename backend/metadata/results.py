"""Metadata outcomes, separate from the persisted metadata payload."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class FetchStatus(StrEnum):
    SUCCESS = "success"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    COOKIE = "cookie"
    RATE_LIMIT = "rate_limit"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class MetadataFetchResult:
    status: FetchStatus
    metadata: dict[str, Any] | None = None
    detail: str = ""

    def __post_init__(self) -> None:
        if (self.status is FetchStatus.SUCCESS) != (self.metadata is not None):
            raise ValueError("Only successful metadata fetches carry a payload")

    @classmethod
    def success(cls, metadata: dict[str, Any]) -> "MetadataFetchResult":
        return cls(FetchStatus.SUCCESS, metadata=metadata)

    @property
    def session_failure(self) -> bool:
        return self.status in {FetchStatus.COOKIE, FetchStatus.RATE_LIMIT}

    @property
    def retryable(self) -> bool:
        return self.status in {FetchStatus.TIMEOUT, FetchStatus.RATE_LIMIT}

    def api_failure(self) -> dict[str, Any]:
        """Translate once at the JSON bridge boundary; preserve existing flags."""
        if self.status is FetchStatus.SUCCESS:
            raise ValueError("Successful fetch has no failure response")
        result: dict[str, Any] = {
            "ok": False, "code": self.status.value,
            "error": self.detail or self.status.value,
            "retryable": self.retryable,
        }
        if self.status is FetchStatus.CANCELLED:
            result["cancelled"] = True
        elif self.status is FetchStatus.TIMEOUT:
            result["transient"] = True
        elif self.session_failure:
            result.update(cookie_auth_required=self.status is FetchStatus.COOKIE,
                          rate_limited=self.status is FetchStatus.RATE_LIMIT)
        return result

    def legacy_value(self) -> dict[str, Any] | None:
        """Compatibility for callers of the historical private fetch helper."""
        if self.status is FetchStatus.SUCCESS:
            return self.metadata
        if self.status is FetchStatus.TIMEOUT:
            return {"_timeout": True}
        if self.session_failure:
            return {"_youtube_failure": self.status.value}
        return None
