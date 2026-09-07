"""Bounded, repeatable results for short-lived background UI operations."""
from __future__ import annotations

import copy
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal


class OperationLimitError(RuntimeError):
    pass


@dataclass
class OperationRecord:
    namespace: str
    state: Literal["pending", "completed", "failed"] = "pending"
    completed_at: float | None = None
    result: dict[str, Any] | None = None
    started: bool = False


class OperationResults:
    """Own completion time, bounded retention and detached polling snapshots.

    Pending work is never expired merely for being slow. Admission limits its
    count; the managed launcher explicitly acknowledges cancelled reservations.
    Completed results are available repeatedly until TTL/cap eviction.
    """
    def __init__(self, *, ttl=600.0, max_completed=128, max_pending=64,
                 clock: Callable[[], float] = time.monotonic):
        self._lock = threading.RLock()
        self._records: dict[str, OperationRecord] = {}
        self._clock = clock
        self._ttl = max(0.0, float(ttl))
        self._max_completed = max(1, int(max_completed))
        self._max_pending = max(1, int(max_pending))

    def _prune_locked(self):
        now = self._clock()
        completed = sorted(
            ((record.completed_at, token) for token, record in self._records.items()
             if record.completed_at is not None))
        expired = {token for completed_at, token in completed
                   if now - completed_at >= self._ttl}
        kept = [token for _at, token in completed if token not in expired]
        expired.update(kept[:max(0, len(kept) - self._max_completed)])
        for token in expired:
            self._records.pop(token, None)

    def begin(self, namespace: str, token: str):
        with self._lock:
            self._prune_locked()
            if token in self._records:
                raise ValueError("Operation ID is already registered")
            if sum(r.state == "pending" for r in self._records.values()) >= self._max_pending:
                raise OperationLimitError("Too many background operations; wait for one to finish.")
            self._records[token] = OperationRecord(namespace)

    def cancelled_before_start(self, token: str):
        """Acknowledge the launcher's decision to skip this reserved worker."""
        with self._lock:
            record = self._records.get(token)
            if record is not None and not record.started:
                self.complete(token, {"ok": False, "cancelled": True,
                                      "error": "Operation cancelled"})

    def complete(self, token: str, result: dict[str, Any]):
        with self._lock:
            record = self._records.get(token)
            if record is None or record.state != "pending":
                return
            record.result = copy.deepcopy(result)
            record.completed_at = self._clock()
            record.state = "completed" if result.get("ok") else "failed"
            self._prune_locked()

    def run(self, token: str, worker: Callable[[], dict[str, Any]]):
        with self._lock:
            record = self._records.get(token)
            if record is None or record.state != "pending":
                return
            record.started = True
        try:
            result = worker()
            if not isinstance(result, dict):
                raise TypeError("Operation did not return a result")
        except Exception as exc:
            result = {"ok": False, "error": str(exc)}
        self.complete(token, result)

    def discard(self, token: str):
        """Remove a launch that failed before any worker acquired ownership."""
        with self._lock:
            self._records.pop(token, None)

    def poll(self, namespace: str, token: str) -> dict[str, Any]:
        with self._lock:
            self._prune_locked()
            record = self._records.get(token)
            if record is None or record.namespace != namespace:
                return {"ok": False, "error": "unknown token"}
            if record.state == "pending":
                return {"ok": True, "pending": True}
            return copy.deepcopy(record.result)


_INIT_LOCK = threading.Lock()


def operation_results(api) -> OperationResults:
    """One registry per application owner, initialized safely by bridge threads."""
    with _INIT_LOCK:
        registry = getattr(api, "_operation_results", None)
        if registry is None:
            registry = OperationResults()
            api._operation_results = registry
        return registry
