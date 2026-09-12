"""Window geometry debounce and shutdown without touching a real window or config."""

from __future__ import annotations

import atexit
import os
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-window-debounce-")
atexit.register(_TEST_APPDATA.cleanup)
for _key, _leaf in (("APPDATA", "Roaming"), ("LOCALAPPDATA", "Local")):
    _directory = Path(_TEST_APPDATA.name, _leaf)
    _directory.mkdir()
    os.environ[_key] = str(_directory)

from backend import window_state  # noqa: E402
from backend.services.job_supervisor import JobSupervisor, OwnerAdapter  # noqa: E402


@pytest.fixture
def debouncer_factory():
    workers = []

    def make(save, delay=0.0):
        worker = window_state.WindowStateDebouncer(save, delay_seconds=delay)
        workers.append(worker)
        return worker

    yield make
    for worker in workers:
        worker.stop()
        assert worker.join(3), "window-state worker did not stop"


def test_latest_geometry_is_merged_and_waits_for_quiet_period(
        debouncer_factory, monkeypatch):
    clock = SimpleNamespace(now=100.0)
    # Replace the module reference, never the process-wide time module used
    # by pytest or threading's bounded waits.
    monkeypatch.setattr(
        window_state, "time", SimpleNamespace(monotonic=lambda: clock.now))
    saved = []
    completed = threading.Event()

    def save(partial):
        saved.append(dict(partial))
        completed.set()

    worker = debouncer_factory(save, delay=10.0)
    waiting = threading.Event()
    original_wait = worker._condition.wait

    def observed_wait(timeout=None):
        waiting.set()
        return original_wait(timeout)

    monkeypatch.setattr(worker._condition, "wait", observed_wait)
    worker.schedule({"x": 1, "y": 2, "width": 1000, "height": 700,
                     "maximized": False})
    assert waiting.wait(2)

    with worker._condition:
        clock.now = 109.0
    worker.schedule({"x": 30, "y": 40})
    worker.schedule({"width": 1400, "height": 900})
    worker.schedule({"x": 50, "maximized": True})

    # The original deadline has passed, but the later geometry events must
    # have restarted the quiet period. Observe the worker waiting again.
    with worker._condition:
        waiting.clear()
        clock.now = 110.0
        worker._condition.notify_all()
    assert waiting.wait(2)
    assert saved == []

    with worker._condition:
        clock.now = 119.0
        worker._condition.notify_all()
    assert completed.wait(2)
    assert saved == [{"x": 50, "y": 40, "width": 1400, "height": 900,
                      "maximized": True}]


def test_one_lazy_daemon_worker_is_reused_across_bursts(debouncer_factory):
    saved = []
    identities = []
    completed = threading.Semaphore(0)

    def save(partial):
        saved.append(dict(partial))
        identities.append(threading.current_thread())
        completed.release()

    worker = debouncer_factory(save)
    assert not worker.is_active()
    for x in (10, 20, 30):
        worker.schedule({"x": x})
        assert completed.acquire(timeout=2)

    assert saved == [{"x": 10}, {"x": 20}, {"x": 30}]
    assert all(identity is identities[0] for identity in identities)
    assert identities[0].daemon


def test_stop_discards_pending_geometry_and_rejects_later_events(debouncer_factory):
    saved = []
    worker = debouncer_factory(saved.append, delay=60.0)
    worker.schedule({"x": 10, "y": 20})
    worker.stop()
    assert worker.join(2)
    assert not worker.is_active()

    worker.schedule({"width": 1400, "height": 900})
    assert worker.join(0)
    assert not worker.is_active()
    assert saved == []


def test_stop_before_first_event_never_starts_a_worker(debouncer_factory):
    saved = []
    worker = debouncer_factory(saved.append)
    worker.stop()
    worker.schedule({"maximized": True})
    assert worker.join(0)
    assert not worker.is_active()
    assert saved == []


def test_inflight_save_allows_scheduling_and_remains_owned_until_join(
        debouncer_factory):
    entered = threading.Event()
    release = threading.Event()
    scheduled = threading.Event()
    saved = []

    def save(partial):
        saved.append(dict(partial))
        entered.set()
        release.wait(5)

    worker = debouncer_factory(save)
    scheduler = None
    try:
        worker.schedule({"x": 10})
        assert entered.wait(2)

        def schedule_while_saving():
            worker.schedule({"x": 20})
            scheduled.set()

        scheduler = threading.Thread(target=schedule_while_saving, daemon=True)
        scheduler.start()
        # A blocked persistence callback must not hold the scheduling lock.
        assert scheduled.wait(2)
        scheduler.join(2)
        worker.stop()
        assert worker.is_active()
        assert not worker.join(0.01)

        release.set()
        assert worker.join(2)
        assert not worker.is_active()
        # Stop cancelled the later pending state, while the first save was
        # allowed to finish and remained visible to the shutdown supervisor.
        assert saved == [{"x": 10}]
    finally:
        release.set()
        if scheduler is not None:
            scheduler.join(2)


def test_callback_failure_does_not_kill_the_worker(debouncer_factory):
    failed = threading.Event()
    recovered = threading.Event()
    saved = []
    identities = []

    def save(partial):
        identities.append(threading.current_thread())
        if partial["x"] == 1:
            failed.set()
            raise OSError("simulated geometry persistence failure")
        saved.append(dict(partial))
        recovered.set()

    worker = debouncer_factory(save)
    worker.schedule({"x": 1})
    assert failed.wait(2)
    worker.schedule({"x": 2})
    assert recovered.wait(2)
    assert saved == [{"x": 2}]
    assert len(identities) == 2
    assert identities[0] is identities[1]


def test_updates_received_during_save_are_persisted_on_the_next_pass(
        debouncer_factory):
    entered = threading.Event()
    release = threading.Event()
    completed = threading.Event()
    saved = []

    def save(partial):
        saved.append(dict(partial))
        if len(saved) == 1:
            entered.set()
            release.wait(5)
        else:
            completed.set()

    worker = debouncer_factory(save)
    try:
        worker.schedule({"x": 10, "y": 15})
        assert entered.wait(2)
        worker.schedule({"x": 20, "width": 1400})
        release.set()
        assert completed.wait(2)
        assert saved == [{"x": 10, "y": 15}, {"x": 20, "width": 1400}]
    finally:
        release.set()


def test_supervisor_quiesce_waits_for_a_blocked_geometry_save(debouncer_factory):
    entered = threading.Event()
    release = threading.Event()
    persisted = threading.Event()

    def save(_partial):
        entered.set()
        release.wait(5)
        persisted.set()

    worker = debouncer_factory(save)
    supervisor = JobSupervisor()
    supervisor.register_owner(OwnerAdapter(
        "window-state", "Window state", worker.is_active,
        worker.stop, worker.join, worker.stop))
    try:
        worker.schedule({"x": 10})
        assert entered.wait(2)
        report = supervisor.quiesce(reason="restore", timeout=0.1)
        assert not report["ok"], report
        assert any(name == "window-state" or name.endswith(":window-state")
                   for name in report["remaining"]), report
        assert worker.is_active()
        assert not persisted.is_set()

        release.set()
        recovered = supervisor.quiesce(reason="restore", timeout=2)
        assert recovered["ok"], recovered
        assert not recovered["remaining"]
        assert persisted.is_set()
        assert not worker.is_active()
    finally:
        release.set()
