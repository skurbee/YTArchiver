from __future__ import annotations

import os
import tempfile
import threading
import time
from pathlib import Path

import pytest

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch4-maint-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend.services.channel_leases import (  # noqa: E402
    LeaseOwner,
    channel_aliases,
    channel_leases,
)
from backend.sync import core  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh_maintenance_owner():
    core._reset_post_sync_maintenance_for_tests()
    yield
    core._reset_post_sync_maintenance_for_tests()
    assert channel_leases.active_snapshot() == ()


def _wait_until(predicate, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


def test_deferred_maintenance_waits_for_channel_lease_and_owns_its_write(
    tmp_path,
):
    channel_dir = tmp_path / "Archive" / "Shared Channel"
    channel_dir.mkdir(parents=True)
    held = channel_leases.try_acquire(
        channel_aliases(paths=[channel_dir]),
        LeaseOwner("sync", "held-sync", label="Active sync"),
    )
    assert held.ok and held.lease is not None

    entered = threading.Event()
    observed = {}

    def maintenance(target, *, cancel_event=None):
        observed["target"] = target
        observed["cancelled"] = bool(cancel_event and cancel_event.is_set())
        observed["leases"] = channel_leases.active_snapshot()
        entered.set()

    try:
        assert core._bg_channel_maintenance(
            "sidecars",
            maintenance,
            str(channel_dir),
            cancel_event=threading.Event(),
        )
        assert _wait_until(
            lambda: core.post_sync_maintenance_snapshot()["worker_alive"]
        )
        assert not entered.wait(0.2)
        snapshot = core.post_sync_maintenance_snapshot()
        assert snapshot["pending"] + bool(snapshot["active_label"]) >= 1
    finally:
        held.lease.release()

    assert entered.wait(2.0)
    assert observed["target"] == str(channel_dir)
    assert observed["cancelled"] is False
    maintenance_leases = [
        row for row in observed["leases"] if row.owner == "sync-maintenance"
    ]
    assert len(maintenance_leases) == 1
    assert _wait_until(lambda: not core.post_sync_maintenance_snapshot()["active_label"])


def test_quiesce_cancels_active_maintenance_before_it_can_commit(tmp_path):
    channel_dir = tmp_path / "Archive" / "Cancelable Channel"
    channel_dir.mkdir(parents=True)
    entered = threading.Event()
    committed = []

    def maintenance(_target, *, cancel_event):
        entered.set()
        cancel_event.wait(2.0)
        if not cancel_event.is_set():
            committed.append("late write")

    assert core._bg_channel_maintenance(
        "cancelable",
        maintenance,
        str(channel_dir),
        cancel_event=threading.Event(),
    )
    assert entered.wait(2.0)
    assert core.post_sync_maintenance_active()

    assert core.post_sync_maintenance_cancel()
    assert core.post_sync_maintenance_join(2.0)
    assert committed == []
    assert core.post_sync_maintenance_snapshot() == {
        "worker_alive": False,
        "pending": 0,
        "pending_labels": [],
        "active_label": "",
        "stopping": True,
    }
    assert not core._bg_channel_maintenance(
        "too-late", lambda _target: committed.append("unexpected"), str(channel_dir)
    )
