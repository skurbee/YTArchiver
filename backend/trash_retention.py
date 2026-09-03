"""Wakeable scheduler for safe, policy-driven Trash cleanup.

The destructive implementation lives in :mod:`backend.trash_manager`.  This
module owns only policy timing and lifecycle: startup readiness, foreground
busy deferral, configuration validation, cancellation, and status snapshots.
Keeping the cleanup callable injectable makes those rules deterministic to
test without touching real archive files.
"""

from __future__ import annotations

import copy
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

from backend.log import get_logger
from backend.ytarchiver_config import (
    TRASH_RETENTION_DEFAULT_DAYS,
    TRASH_RETENTION_MAX_DAYS,
    load_config,
)

_log = get_logger(__name__)


def _default_cleanup(**kwargs):
    """Import the permanent-delete implementation only when a run is due."""
    from backend.trash_manager import purge_expired

    return purge_expired(**kwargs)


def _policy_days(value) -> int:
    """Strictly validate persisted policy; corrupt values fail closed."""
    if isinstance(value, bool):
        raise ValueError("Trash retention cannot be a boolean")
    if isinstance(value, int):
        days = value
    elif isinstance(value, float) and value.is_integer():
        days = int(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped or not stripped.lstrip("+-").isdigit():
            raise ValueError("Trash retention is not a whole number")
        days = int(stripped)
    else:
        raise ValueError("Trash retention is not a whole number")
    if days != 0 and not 1 <= days <= TRASH_RETENTION_MAX_DAYS:
        raise ValueError(
            f"Trash retention must be 0 or 1-{TRASH_RETENTION_MAX_DAYS}")
    return days


def _grace_timestamp(value) -> float:
    try:
        grace = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if grace != grace or grace in (float("inf"), float("-inf")):
        return 0.0
    return max(0.0, grace)


class TrashRetentionScheduler:
    """Run Trash retention after startup, then daily or when explicitly woken."""

    STARTUP_GRACE_SECONDS = 120.0
    INTERVAL_SECONDS = 24 * 60 * 60.0
    BUSY_RETRY_SECONDS = 5 * 60.0
    ERROR_RETRY_SECONDS = 15 * 60.0

    def __init__(
        self,
        *,
        cleanup_fn: Callable[..., Any] | None = None,
        config_loader: Callable[[], Mapping[str, Any]] | None = None,
        busy_fn: Callable[[], Any] | None = None,
        startup_grace_seconds: float = STARTUP_GRACE_SECONDS,
        interval_seconds: float = INTERVAL_SECONDS,
        busy_retry_seconds: float = BUSY_RETRY_SECONDS,
        error_retry_seconds: float = ERROR_RETRY_SECONDS,
        startup_required_signals: tuple[str, ...] = ("checks", "indexing"),
        wall_clock: Callable[[], float] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
        thread_factory: Callable[..., threading.Thread] = threading.Thread,
    ) -> None:
        self._cleanup_fn = cleanup_fn or _default_cleanup
        self._config_loader = config_loader or load_config
        self._busy_fn = busy_fn or (lambda: False)
        self._startup_grace_seconds = max(
            0.0, float(startup_grace_seconds))
        self._interval_seconds = max(0.01, float(interval_seconds))
        self._busy_retry_seconds = max(0.01, float(busy_retry_seconds))
        self._error_retry_seconds = max(0.01, float(error_retry_seconds))
        self._wall_clock = wall_clock or time.time
        self._monotonic_clock = monotonic_clock or time.monotonic
        self._thread_factory = thread_factory

        self._stop = threading.Event()
        self._wake = threading.Event()
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._startup_pending_signals = {
            str(signal).strip().lower()
            for signal in startup_required_signals
            if str(signal).strip()
        }
        self._startup_ready_at: float | None = None
        if not self._startup_pending_signals:
            self._startup_ready_at = (
                self._monotonic_clock() + self._startup_grace_seconds)

        self._last_attempt_ts = 0.0
        self._last_success_ts = 0.0
        self._last_result: dict[str, Any] = {}
        self._busy_reason = ""

    def start(self) -> bool:
        """Start once. Lifecycle registration should happen before this call."""
        with self._lock:
            if self._stop.is_set():
                return False
            if self._thread is not None:
                return self._thread.is_alive()
            thread = self._thread_factory(
                target=self._loop,
                name="trash-retention",
                daemon=True,
            )
            self._thread = thread
        try:
            thread.start()
        except BaseException:
            with self._lock:
                if self._thread is thread:
                    self._thread = None
            raise
        return True

    def wake(self) -> bool:
        """Apply a committed policy change without waiting for the daily tick."""
        self._wake.set()
        return True

    def notify_startup_ready(self, signal: str = "") -> dict[str, Any]:
        """Release one startup gate; no signal releases all remaining gates."""
        normalized = str(signal or "").strip().lower()
        with self._lock:
            if normalized:
                self._startup_pending_signals.discard(normalized)
            else:
                self._startup_pending_signals.clear()
            if (not self._startup_pending_signals
                    and self._startup_ready_at is None):
                self._startup_ready_at = (
                    self._monotonic_clock() + self._startup_grace_seconds)
            waiting_for = sorted(self._startup_pending_signals)
        self._wake.set()
        startup_wait = self._startup_wait_seconds()
        return {
            "ok": True,
            "startup_ready": (
                not waiting_for
                and startup_wait is not None
                and startup_wait <= 0),
            "waiting_for": waiting_for,
            "startup_grace_remaining_seconds": startup_wait,
        }

    def request_stop(self) -> bool:
        self._stop.set()
        self._wake.set()
        return True

    def join(self, timeout: float = 5.0) -> bool:
        with self._lock:
            thread = self._thread
        if (thread is not None and thread.is_alive()
                and thread is not threading.current_thread()):
            thread.join(timeout=max(0.0, float(timeout)))
        return thread is None or not thread.is_alive()

    def stop(self, timeout: float = 5.0) -> bool:
        self.request_stop()
        return self.join(timeout)

    def is_alive(self) -> bool:
        with self._lock:
            thread = self._thread
        return bool(thread is not None and thread.is_alive())

    def _startup_wait_seconds(self) -> float | None:
        with self._lock:
            if self._startup_pending_signals:
                return None
            ready_at = self._startup_ready_at
        if ready_at is None:
            return 0.0
        return max(0.0, ready_at - self._monotonic_clock())

    def _record_result(
        self,
        result: Mapping[str, Any],
        *,
        attempted_at: float | None = None,
        success: bool = False,
        busy_reason: str = "",
    ) -> dict[str, Any]:
        row = dict(result)
        with self._lock:
            if attempted_at is not None:
                self._last_attempt_ts = float(attempted_at)
            if success and attempted_at is not None:
                self._last_success_ts = float(attempted_at)
            self._last_result = copy.deepcopy(row)
            self._busy_reason = str(busy_reason or "")
        return row

    def tick(self, now: float | None = None) -> dict[str, Any]:
        """Run one deterministic policy check and, when eligible, cleanup."""
        if self._stop.is_set():
            return self._record_result({"ok": True, "cancelled": True})

        startup_wait = self._startup_wait_seconds()
        if startup_wait is None:
            with self._lock:
                waiting_for = sorted(self._startup_pending_signals)
            return self._record_result({
                "ok": True,
                "deferred": True,
                "reason": "startup",
                "waiting_for": waiting_for,
            })
        if startup_wait > 0:
            return self._record_result({
                "ok": True,
                "deferred": True,
                "reason": "startup_grace",
                "retry_in_seconds": startup_wait,
            })

        attempted_at = self._wall_clock() if now is None else float(now)
        try:
            busy = self._busy_fn()
        except Exception as exc:
            result = {
                "ok": False,
                "deferred": True,
                "reason": "busy_check_failed",
                "error": str(exc),
            }
            _log.warning("Trash retention busy check failed: %s", exc)
            return self._record_result(
                result, attempted_at=attempted_at, busy_reason=str(exc))
        if busy:
            reason = str(busy) if not isinstance(busy, bool) else "busy"
            return self._record_result({
                "ok": True,
                "deferred": True,
                "reason": reason,
            }, attempted_at=attempted_at, busy_reason=reason)

        try:
            cfg = dict(self._config_loader() or {})
            days = _policy_days(cfg.get(
                "trash_retention_days", TRASH_RETENTION_DEFAULT_DAYS))
            grace_until = _grace_timestamp(
                cfg.get("trash_retention_grace_until_ts", 0.0))
        except Exception as exc:
            result = {
                "ok": False,
                "error": f"Invalid Trash retention policy: {exc}",
            }
            _log.warning("%s", result["error"])
            return self._record_result(result, attempted_at=attempted_at)

        if days == 0:
            return self._record_result({
                "ok": True,
                "disabled": True,
                "retention_days": 0,
            }, attempted_at=attempted_at, success=True)
        if self._stop.is_set():
            return self._record_result(
                {"ok": True, "cancelled": True},
                attempted_at=attempted_at,
            )

        try:
            raw_result = self._cleanup_fn(
                config=cfg,
                retention_days=days,
                grace_until_ts=grace_until,
                cancel_event=self._stop,
                now=attempted_at,
            )
            if raw_result is None:
                result = {"ok": True}
            elif isinstance(raw_result, Mapping):
                result = dict(raw_result)
                result.setdefault("ok", True)
            else:
                result = {"ok": bool(raw_result)}
            result.setdefault("retention_days", days)
            result.setdefault("grace_until_ts", grace_until)
        except Exception as exc:
            result = {"ok": False, "error": str(exc)}
            _log.warning("Trash retention cleanup failed: %s", exc)

        succeeded = bool(result.get("ok")) and not result.get("cancelled")
        return self._record_result(
            result, attempted_at=attempted_at, success=succeeded)

    # Existing scheduler tests and integrations commonly use a private
    # ``_tick`` name. Keep the alias while making ``tick`` the public API.
    _tick = tick

    def _loop(self) -> None:
        while not self._stop.is_set():
            # Clear before the work. A wake that arrives during tick remains
            # set and makes the subsequent wait return immediately.
            self._wake.clear()
            try:
                result = self.tick()
            except Exception as exc:  # last-resort thread survival guard
                _log.error("Trash retention scheduler tick failed: %s", exc)
                result = {"ok": False, "error": str(exc)}
                self._record_result(result, attempted_at=self._wall_clock())
            if self._stop.is_set():
                return

            if result.get("reason") == "startup":
                delay = self._interval_seconds
            elif result.get("reason") == "startup_grace":
                delay = max(0.01, float(
                    result.get("retry_in_seconds") or 0.01))
            elif result.get("deferred"):
                delay = self._busy_retry_seconds
            elif not result.get("ok"):
                delay = self._error_retry_seconds
            else:
                delay = self._interval_seconds
            self._wake.wait(timeout=delay)

    def snapshot(self) -> dict[str, Any]:
        startup_wait = self._startup_wait_seconds()
        with self._lock:
            thread = self._thread
            waiting_for = sorted(self._startup_pending_signals)
            return {
                "running": bool(thread is not None and thread.is_alive()),
                "stop_requested": self._stop.is_set(),
                "startup_ready": (
                    not waiting_for
                    and startup_wait is not None
                    and startup_wait <= 0),
                "waiting_for": waiting_for,
                "startup_grace_remaining_seconds": (
                    startup_wait if startup_wait is not None else None),
                "last_attempt_ts": self._last_attempt_ts,
                "last_success_ts": self._last_success_ts,
                "busy_reason": self._busy_reason,
                "last_result": copy.deepcopy(self._last_result),
            }


__all__ = ["TrashRetentionScheduler"]
