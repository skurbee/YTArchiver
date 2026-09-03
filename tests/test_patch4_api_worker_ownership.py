from __future__ import annotations

import threading
from unittest import mock

from backend.api_mixins import subs_mixin
from backend.api_mixins.onboarding_mixin import OnboardingMixin
from backend.api_mixins.subs_mixin import SubsMixin
from backend.api_mixins.sync_mixin import SyncMixin
from backend.api_mixins.thumbnail_mixin import ThumbnailMixin
from backend.services.channel_leases import channel_leases
from backend.services.job_supervisor import JobSupervisor


def test_closed_admission_blocks_dependency_installer_before_start() -> None:
    class Api(OnboardingMixin):
        pass

    api = Api()
    api._job_supervisor = JobSupervisor()
    api._job_supervisor.close_admission("test restore")
    api._run_install = mock.Mock()

    result = api.onboarding_install_core()

    assert result["ok"] is False
    assert result["started"] is False
    assert "test restore" in result["error"]
    api._run_install.assert_not_called()
    assert not any(
        row.get("dynamic")
        for row in api._job_supervisor.snapshot()["owners"]
    )


def test_closed_admission_blocks_sync_lane_before_target_runs() -> None:
    class Api(SyncMixin):
        def __init__(self) -> None:
            self._job_supervisor = JobSupervisor()
            self._sync_thread = None
            self._sync_cancel = threading.Event()

    api = Api()
    called = threading.Event()
    api._job_supervisor.close_admission("test shutdown")

    started = api._start_sync_thread_locked(called.set)

    assert started is False
    assert called.is_set() is False
    assert "test shutdown" in api._sync_start_error


def test_lifecycle_force_never_calls_user_queue_clear() -> None:
    entered = threading.Event()
    release = threading.Event()

    class Api(SyncMixin):
        def __init__(self) -> None:
            self._job_supervisor = JobSupervisor()
            self._sync_thread = None
            self._sync_cancel = threading.Event()
            self.sync_force_stop = mock.Mock()

    api = Api()

    def stubborn_sync() -> None:
        entered.set()
        release.wait(timeout=1.0)

    assert api._start_sync_thread_locked(stubborn_sync) is True
    assert entered.wait(timeout=1.0)

    report = api._job_supervisor.quiesce(
        reason="test shutdown", timeout=0.08)
    release.set()
    api._sync_thread.join(timeout=1.0)

    assert report["ok"] is False
    api.sync_force_stop.assert_not_called()


def test_thumbnail_writer_is_visible_and_quiesce_cancels_it() -> None:
    entered = threading.Event()
    observed_cancel = threading.Event()

    def blocking_sweep(_channel, stream=None, cancel_event=None):
        del stream
        entered.set()
        assert cancel_event is not None
        cancel_event.wait(timeout=2.0)
        if cancel_event.is_set():
            observed_cancel.set()
        return {"fetched": 0, "missing": 0, "checked": 0}

    class Api(ThumbnailMixin):
        def __init__(self) -> None:
            self._job_supervisor = JobSupervisor()
            self._config = None
            self._log_stream = mock.Mock()
            self.services = None

        @staticmethod
        def sync_is_running() -> bool:
            return False

    api = Api()
    channel = {
        "name": "Test Channel",
        "url": "https://www.youtube.com/@patch4ownershiptest",
    }
    with mock.patch(
        "backend.api_mixins.thumbnail_mixin.subs_backend.get_channel",
        return_value=channel,
    ), mock.patch(
        "backend.metadata.sweep_missing_thumbnails",
        side_effect=blocking_sweep,
    ):
        result = api.refetch_thumbnails(channel)
        assert result == {"ok": True, "started": True}
        assert entered.wait(timeout=1.0)

        active = [
            row
            for row in api._job_supervisor.snapshot()["owners"]
            if row.get("dynamic")
        ]
        assert any(
            row["owner"] == "thumbnail-maintenance"
            and row["label"] == "Refetch thumbnails for Test Channel"
            for row in active
        )

        report = api._job_supervisor.quiesce(
            reason="test restore", timeout=1.0)

    assert report["ok"] is True
    assert report["remaining"] == []
    assert observed_cancel.is_set()
    assert not any(
        row.owner == "thumbnail-maintenance"
        for row in channel_leases.active_snapshot()
    )


def test_reset_sync_state_commits_against_fresh_config() -> None:
    class Api(SubsMixin):
        def __init__(self) -> None:
            self._reload_config = mock.Mock()

    live = {
        "unrelated": "keep me",
        "channels": [{
            "name": "Fresh Name",
            "url": "https://www.youtube.com/@atomicreset",
            "initialized": True,
            "sync_complete": True,
            "filter": "unchanged",
        }],
    }

    def atomic_update(mutator):
        result = mutator(live)
        return result, dict(live)

    api = Api()
    with mock.patch.object(
        subs_mixin.subs_backend,
        "get_channel",
        return_value={
            "name": "Stale Name",
            "url": "https://www.youtube.com/@atomicreset",
        },
    ), mock.patch.object(
        subs_mixin, "update_config", side_effect=atomic_update
    ) as update:
        result = api.subs_reset_sync_state({"name": "Stale Name"})

    assert result["ok"] is True
    assert result["cleared_flags"] == 2
    assert live["unrelated"] == "keep me"
    assert live["channels"][0]["filter"] == "unchanged"
    assert "initialized" not in live["channels"][0]
    assert "sync_complete" not in live["channels"][0]
    update.assert_called_once()
    api._reload_config.assert_called_once()
