"""Lifecycle deadlines include status probes and blocked scheduler storage."""
from __future__ import annotations

import threading
import time
from contextlib import contextmanager

import pytest

from backend import autorun
from backend.services.job_supervisor import JobSupervisor, OwnerAdapter


def test_scheduler_lifecycle_does_not_wait_for_configuration_io(monkeypatch):
    entered, release = threading.Event(), threading.Event()
    committed = threading.Event()
    monkeypatch.setattr(autorun, "load_config", lambda: {"autorun_mode": "timer"})
    monkeypatch.setattr(autorun, "config_is_writable", lambda: True)

    @contextmanager
    def blocked_commit():
        yield {}
        entered.set()
        assert release.wait(2)
        committed.set()

    monkeypatch.setattr(autorun, "config_transaction", blocked_commit)
    scheduler = autorun.AutorunScheduler(lambda: pytest.fail("timer must not fire"))
    scheduler._interval_mins = 60
    worker = threading.Thread(target=scheduler.notify_sync_done)
    worker.start()
    try:
        assert entered.wait(1)
        started = time.monotonic()
        assert scheduler.is_alive()
        assert not scheduler.join(0.03)
        assert time.monotonic() - started < 0.2
        supervisor = JobSupervisor()
        supervisor.register_owner(OwnerAdapter(
            "autorun", "Schedule", scheduler.is_alive,
            scheduler.cancel, scheduler.join, scheduler.cancel))
        started = time.monotonic()
        report = supervisor.quiesce(reason="restore", timeout=0.08)
        assert time.monotonic() - started < 0.3
        assert not report["ok"]
        # The short budget may expire before prepare starts, leaving force
        # blocked instead. Either way the actual scheduler must remain owned.
        assert "autorun" in report["remaining"]
        assert worker.is_alive()
        assert not committed.is_set()
        assert scheduler.is_alive()

        release.set()
        worker.join(2)
        assert not worker.is_alive()
        assert committed.is_set()
        recovered = supervisor.quiesce(reason="restore", timeout=1)
        assert recovered["ok"], recovered
        assert not recovered["remaining"]
        assert not scheduler.is_alive()
    finally:
        release.set()
        worker.join(2)
        scheduler.cancel()
        assert scheduler.join(1)


@pytest.mark.parametrize("blocked_phase", ["active", "details", "join"])
def test_slow_lifecycle_callbacks_remain_owned_and_are_not_duplicated(blocked_phase):
    supervisor = JobSupervisor()
    release, entered = threading.Event(), threading.Event()
    calls = []
    prepared = []

    def block():
        calls.append(threading.current_thread().name)
        entered.set()
        assert release.wait(2)
        return True

    supervisor.register_owner(OwnerAdapter(
        "worker", "Worker", block if blocked_phase == "active" else lambda: True,
        lambda: prepared.append(True),
        (lambda _timeout: block()) if blocked_phase == "join" else lambda _timeout: False,
        lambda: None,
        details=(lambda: {"value": block()}) if blocked_phase == "details" else None))
    try:
        for _ in range(2):
            started = time.monotonic()
            report = supervisor.quiesce(reason="restore", timeout=0.2)
            assert time.monotonic() - started < 0.4
            assert not report["ok"]
            assert any(name.startswith("lifecycle:") for name in report["remaining"])
        assert entered.is_set()
        assert prepared
        # Each retained phase may inspect the owner, but retrying quiescence
        # never starts another concurrent copy of the blocked phase.
        assert len(calls) == len(set(calls))
    finally:
        release.set()
        for call in supervisor._lifecycle_calls.values():
            call.thread.join(1)
