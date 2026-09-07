"""Ordering and cancellation contract for the slow startup stages."""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable

from backend.log import get_logger

_log = get_logger(__name__)


def run_startup_stages(
    *,
    cancel_event: threading.Event,
    finished: threading.Event,
    disk_scan: Callable[[], bool | None],
    sweep: Callable[[], None],
    backfill: Callable[[], None],
    clear_loading: Callable[[], None],
    clear_indicator: Callable[[], None],
    ready_callbacks: Iterable[Callable[[str], None]],
) -> None:
    """Retry a deferred scan once, then publish readiness after local work."""
    try:
        try:
            deferred = False
            if not cancel_event.is_set():
                deferred = disk_scan() is False
            if not cancel_event.is_set():
                sweep()
            if deferred and not cancel_event.is_set():
                disk_scan()
        finally:
            finished.set()
            cancel_event.wait(0.5)
            clear_loading()
            try:
                clear_indicator()
            except Exception as exc:
                _log.debug("Startup indicator cleanup failed: %s", exc)
        if not cancel_event.is_set():
            backfill()
    finally:
        finished.set()
        if not cancel_event.is_set():
            for notify in ready_callbacks:
                try:
                    notify("indexing")
                except Exception as exc:
                    _log.debug("Startup-ready notification failed: %s", exc)
