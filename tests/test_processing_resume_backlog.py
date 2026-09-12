"""A resumed download waits for its existing exclusive Processing backlog."""

import copy
import threading

import pytest

from backend.services.channel_leases import channel_aliases
from backend.transcribe.core import TranscribeManager


@pytest.fixture
def manager(tmp_path):
    # These queries need only runtime ownership; never initialize a worker or
    # open an application journal in this fixture.
    value = object.__new__(TranscribeManager)
    value._journal_lock = threading.RLock()
    value._jobs_lock = threading.Lock()
    value._jobs = []
    value._inline_caption_jobs = []
    value._current_job = None
    channels = [{"name": name, "url": f"https://youtube.com/@fixture{name}"}
                for name in ("Alpha", "Beta")]

    def config():
        assert value._jobs_lock.acquire(blocking=False), "Alias lookup held the jobs lock"
        value._jobs_lock.release()
        return {"output_dir": str(tmp_path), "channels": channels}

    value._cfg_loader = config
    value.fixture_aliases = channel_aliases(channels[0], paths=[tmp_path / "Alpha"])
    value.fixture_root = tmp_path
    return value


def job(manager, task_id, channel="Alpha", **extra):
    return {"task_id": task_id, "channel": channel,
            "path": str(manager.fixture_root / channel / "2020" / f"{task_id}.mp4"),
            "kind": "transcribe", "from_download": True, **extra}


def test_capture_includes_current_pending_and_inline_work_for_same_aliases(manager):
    manager._current_job = job(manager, "active")
    manager._jobs = [job(manager, "next"), job(manager, "other", "Beta")]
    manager._inline_caption_jobs = [job(manager, "inline")]
    before = copy.deepcopy(manager._jobs)

    assert manager.pending_channel_job_ids(manager.fixture_aliases) == {
        "active", "next", "inline"}
    assert manager._jobs == before


@pytest.mark.parametrize("extra,excluded", [
    ({"_download_sync_job_id": "sync-alpha"}, True),
    ({"_download_sync_job_id": "old-sync"}, False),
    ({"_download_sync_job_id": "sync-alpha", "retranscribe": True}, False),
    ({"_download_sync_job_id": "sync-alpha", "kind": "compress"}, False),
    ({"_download_sync_job_id": "sync-alpha", "from_download": False}, False),
    ({}, False),  # Restart recovery has no runtime parent capability.
])
def test_only_exact_runtime_download_child_is_compatible(manager, extra, excluded):
    manager._jobs = [job(manager, "work", **extra)]
    ids = manager.pending_channel_job_ids(manager.fixture_aliases, sync_job_id="sync-alpha")
    assert ids == (set() if excluded else {"work"})


def test_finite_backlog_survives_promotion_and_does_not_include_later_arrivals(manager):
    first = job(manager, "first")
    second = job(manager, "second")
    manager._jobs = [first, second]
    captured = manager.pending_channel_job_ids(manager.fixture_aliases)
    manager._current_job = manager._jobs.pop(0)
    manager._jobs.append(job(manager, "arrived-later"))
    assert manager.pending_job_ids(captured) == {"first", "second"}
    manager._current_job = manager._jobs.pop(0)
    assert manager.pending_job_ids(captured) == {"second"}
    manager._current_job = None
    assert not manager.pending_job_ids(captured)
    assert manager._jobs[0]["task_id"] == "arrived-later"


def test_cancelled_job_remains_pending_until_its_cleanup_finishes(manager):
    cancel = threading.Event()
    cancel.set()
    manager._current_job = job(manager, "cleanup", cancel=cancel)
    assert manager.pending_job_ids({"cleanup"}) == {"cleanup"}
    manager._current_job = None
    assert not manager.pending_job_ids({"cleanup"})


@pytest.mark.parametrize("query", ["capture", "membership"])
def test_backlog_query_waits_for_failed_completion_to_restore_job(manager, query):
    retry = job(manager, "retry")
    manager._current_job = retry
    started = threading.Event()
    finished = threading.Event()
    observed = []

    def read():
        started.set()
        observed.append(manager.pending_channel_job_ids(manager.fixture_aliases)
                        if query == "capture" else manager.pending_job_ids({"retry"}))
        finished.set()

    reader = threading.Thread(target=read, daemon=True)
    try:
        # Finalization removes the current runtime slot before saving; a
        # failed save restores it while the same journal boundary is held.
        with manager._journal_lock:
            manager._current_job = None
            reader.start()
            assert started.wait(2)
            assert not finished.wait(0.05)
            manager._jobs.append(retry)
        assert finished.wait(2)
        assert observed == [{"retry"}]
    finally:
        reader.join(2)
    assert not reader.is_alive()
