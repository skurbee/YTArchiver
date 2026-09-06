"""Schedule presentation must not imply future work is already blocked."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-schedule-state-")
os.environ["APPDATA"] = _PROFILE.name
os.environ["LOCALAPPDATA"] = _PROFILE.name
Path(_PROFILE.name, "YTArchiver").mkdir()

from backend import autorun  # noqa: E402


@pytest.mark.parametrize("clock", [False, True])
def test_background_work_does_not_hide_future_deadline(monkeypatch, clock):
    monkeypatch.setattr(autorun, "load_config", dict)
    monkeypatch.setattr(autorun.time, "time", lambda: 1000)
    scheduler = autorun.AutorunScheduler(lambda: None,
        sync_busy_fn=lambda: "an archive rescan")
    scheduler._interval_mins = 60
    scheduler._clock_mode = clock
    scheduler._next_fire_ts = 4600
    state = scheduler.get_state()
    assert state["seconds_remaining"] == 3600
    assert state["next_fire_ts"] == 4600
    assert not state["waiting_for_sync"]
    assert not state["busy_reason"]


@pytest.mark.parametrize("busy", [True, "an archive rescan", "database maintenance"])
def test_due_schedule_reports_known_blocker_without_claiming_queue(monkeypatch, busy):
    monkeypatch.setattr(autorun, "load_config", dict)
    monkeypatch.setattr(autorun.time, "time", lambda: 1000)
    scheduler = autorun.AutorunScheduler(lambda: None, sync_busy_fn=lambda: busy)
    scheduler._interval_mins = 60
    scheduler._next_fire_ts = 900
    state = scheduler.get_state()
    assert state["waiting_for_sync"]
    assert state["seconds_remaining"] == 0
    assert state["overdue_seconds"] == 100
    assert state["busy_reason"] == (busy if isinstance(busy, str) else "")
    assert not state["scheduled_sync_running"]


def test_own_scheduled_run_is_distinct_from_unrelated_blocker(monkeypatch):
    monkeypatch.setattr(autorun, "load_config", dict)
    scheduler = autorun.AutorunScheduler(lambda: None, sync_busy_fn=lambda: True)
    scheduler._interval_mins = 60
    scheduler._waiting_for_sync_done = True
    state = scheduler.get_state()
    assert state["scheduled_sync_running"]
    assert state["waiting_for_sync"]
