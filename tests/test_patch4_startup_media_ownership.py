from __future__ import annotations

import os
import tempfile
import threading
from pathlib import Path
from unittest import mock

import pytest

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch4-startup-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import drift_scan  # noqa: E402
from backend.api_mixins import media_ops_mixin  # noqa: E402
from backend.api_mixins.media_ops_mixin import MediaOpsMixin  # noqa: E402
from backend.api_mixins.startup_mixin import StartupMixin  # noqa: E402
from backend.metadata import core as metadata_core  # noqa: E402
from backend.services.channel_leases import (  # noqa: E402
    LeaseOwner,
    channel_aliases,
    channel_leases,
)
from backend.services.job_supervisor import JobSupervisor  # noqa: E402


@pytest.fixture(autouse=True)
def _no_leaked_archive_lease():
    assert channel_leases.active_snapshot() == ()
    yield
    assert channel_leases.active_snapshot() == ()


class _StartupApi(StartupMixin):
    def __init__(self):
        self._job_supervisor = JobSupervisor()
        self.entered = threading.Event()
        self.exited = threading.Event()
        self.received_cancel = None

    def _run_startup_sequence(self, cancel_event):
        self.received_cancel = cancel_event
        self.entered.set()
        cancel_event.wait(2.0)
        self.exited.set()


def test_startup_writer_is_registered_and_quiesced_before_restore():
    api = _StartupApi()
    result = api.startup_ready()

    assert result == {"ok": True}
    assert api.entered.wait(1.0)
    dynamic = [
        row for row in api._job_supervisor.snapshot()["owners"]
        if row.get("dynamic")
    ]
    assert len(dynamic) == 1
    assert dynamic[0]["owner"] == "startup-indexing"

    report = api._job_supervisor.quiesce(reason="backup restore", timeout=1.0)
    assert report["ok"]
    assert api.received_cancel.is_set()
    assert api.exited.wait(1.0)
    assert api._job_supervisor.snapshot()["owners"] == []


def test_closed_admission_prevents_startup_writer_from_entering():
    api = _StartupApi()
    api._job_supervisor.close_admission("backup restore")

    result = api.startup_ready()

    assert result["ok"] is False
    assert result["started"] is False
    assert "backup restore" in result["error"]
    assert not api.entered.is_set()
    assert api._startup_fired is False
    assert api._job_supervisor.snapshot()["owners"] == []


class _MediaApi(MediaOpsMixin):
    def __init__(self):
        self._job_supervisor = JobSupervisor()
        self._config = {}
        self._log_stream = mock.Mock()
        self._window = None


def test_held_sync_lease_blocks_drift_repair_before_any_mutation(
    tmp_path,
    monkeypatch,
):
    archive = tmp_path / "Archive"
    channel_dir = archive / "Channel"
    channel_dir.mkdir(parents=True)
    channel = {
        "name": "Channel",
        "url": "https://youtube.com/@leaseblock",
    }
    holder = channel_leases.try_acquire(
        channel_aliases(channel, paths=[channel_dir]),
        LeaseOwner("sync", "sync-holds-channel", label="Active sync"),
    )
    assert holder.ok and holder.lease is not None
    api = _MediaApi()
    api._config = {"output_dir": str(archive)}
    monkeypatch.setattr(media_ops_mixin.subs_backend, "get_channel", lambda _i: channel)

    try:
        with mock.patch.object(drift_scan, "apply_channel") as mutate:
            result = api.drift_apply_channel({"url": channel["url"]})
        assert result["ok"] is False
        assert result["busy"] is True
        assert result["started"] is False
        assert result["error"] == (
            "Archive work is busy with Active sync. "
            "Try again after the active work finishes."
        )
        assert "sync-holds-channel" not in result["error"]
        mutate.assert_not_called()
        assert api._job_supervisor.snapshot()["owners"] == []
    finally:
        holder.lease.release()


def test_duration_backfill_is_visible_and_cancelled_by_quiesce(monkeypatch):
    entered = threading.Event()
    exited = threading.Event()

    def backfill(_stream, cancel_event):
        entered.set()
        cancel_event.wait(2.0)
        exited.set()
        return {"resolved": 0}

    monkeypatch.setattr(metadata_core, "backfill_missing_durations", backfill)
    api = _MediaApi()

    result = api.video_lengths_backfill_start()

    assert result == {"ok": True, "started": True}
    assert entered.wait(1.0)
    dynamic = [
        row for row in api._job_supervisor.snapshot()["owners"]
        if row.get("dynamic")
    ]
    assert [row["owner"] for row in dynamic] == ["index-maintenance"]

    report = api._job_supervisor.quiesce(reason="application shutdown", timeout=1.0)
    assert report["ok"]
    assert exited.wait(1.0)
    assert api._dur_backfill_running is False
