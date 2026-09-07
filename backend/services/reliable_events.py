"""Acknowledged UI events, independent of disposable display-log batches."""
from __future__ import annotations

import copy
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from backend.log import get_logger

_log = get_logger(__name__)


class ReliableEventChannel:
    """Retain the latest event per key until the frontend acknowledges it.

    Revisions order state even after a bridge retry. A prompt and its close
    event share a key, so expiry/closure can supersede an undelivered prompt.
    Only this channel's single delivery callback may be in flight at a time.
    """

    def __init__(self, deliver: Callable[[list[dict[str, Any]]], Any], *,
                 schedule: bool = True, clock: Callable[[], float] = time.time):
        self._deliver = deliver
        self._schedule = schedule
        self._clock = clock
        self._channel = uuid.uuid4().hex
        self._lock = threading.Lock()
        self._pending: dict[str, dict[str, Any]] = {}
        self._revision = 0
        self._timer: threading.Timer | None = None
        self._busy = False
        self._closed = False
        self._retry_delay = 0.25
        self._last_attempt = 0

    def publish(self, topic: str, key: str, payload: dict[str, Any], *,
                expires_at: float | None = None) -> int:
        if not topic or not key:
            raise ValueError("An event topic and identity are required")
        value = copy.deepcopy(payload)
        with self._lock:
            if self._closed:
                return 0
            self._revision += 1
            event = {"channel": self._channel, "key": key, "topic": topic,
                     "revision": self._revision, "payload": value,
                     "expires_at": expires_at}
            self._pending[key] = event
            timer = self._prepare_timer_locked(0.06)
            revision = self._revision
        self._start_timer(timer)
        return revision

    def _prune_locked(self) -> None:
        now = self._clock()
        expired = [key for key, event in self._pending.items()
                   if event["expires_at"] is not None and event["expires_at"] <= now]
        for key in expired:
            del self._pending[key]

    def _prepare_timer_locked(self, delay: float) -> threading.Timer | None:
        if not self._schedule or self._closed or self._busy or self._timer:
            return None
        self._prune_locked()
        if not self._pending:
            return None
        timer = threading.Timer(delay, self.deliver_once)
        timer.daemon = True
        timer.name = "ui-event-delivery"
        self._timer = timer
        return timer

    def _start_timer(self, timer: threading.Timer | None) -> None:
        if timer is None:
            return
        # Starting outside the state lock permits immediate callbacks. Failure
        # must leave the retained event eligible for a later publish/wake retry.
        try:
            timer.start()
        except Exception as exc:
            with self._lock:
                if self._timer is timer:
                    self._timer = None
            _log.warning("Could not schedule application event delivery: %s", exc)

    def wake(self) -> None:
        with self._lock:
            timer = self._prepare_timer_locked(0.0)
        self._start_timer(timer)

    def deliver_once(self) -> None:
        """Attempt one acknowledged batch; callbacks never hold the state lock."""
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = None
            if self._closed or self._busy:
                return
            self._prune_locked()
            ordered = sorted(self._pending.values(), key=lambda event: event["revision"])
            # Rotate retries so an unavailable listener cannot starve other topics.
            batch = ([event for event in ordered if event["revision"] > self._last_attempt]
                     + [event for event in ordered if event["revision"] <= self._last_attempt])[:64]
            if not batch:
                return
            self._last_attempt = batch[-1]["revision"]
            self._busy = True
        acknowledgements: set[int] = set()
        try:
            response = self._deliver(copy.deepcopy(batch))
            if isinstance(response, list):
                acknowledgements = {value for value in response if type(value) is int}
        except Exception as exc:
            _log.debug("UI event delivery will retry: %s", exc)
        finally:
            with self._lock:
                for event in batch:
                    current = self._pending.get(event["key"])
                    if (event["revision"] in acknowledgements and current is not None
                            and current["revision"] == event["revision"]):
                        del self._pending[event["key"]]
                self._busy = False
                self._retry_delay = 0.25 if acknowledgements else min(5.0, self._retry_delay * 2)
                timer = self._prepare_timer_locked(self._retry_delay)
            self._start_timer(timer)

    def pending(self) -> list[dict[str, Any]]:
        with self._lock:
            self._prune_locked()
            return copy.deepcopy(list(self._pending.values()))

    def close(self) -> None:
        with self._lock:
            self._closed = True
            if self._timer:
                self._timer.cancel()
                self._timer = None
