"""Resume keeps exclusive Processing recovery ahead of the same Sync task."""

import contextlib
import copy
import importlib
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


def _until(predicate, timeout=3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate()


@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    from backend.queues import QueueState
    from backend.services.channel_leases import LeaseOwner, channel_leases
    from backend.services.queue_repository import QueueRepository
    from backend.transcribe.core import TranscribeManager

    sync = importlib.import_module("backend.sync.sync_all")
    task = {"task_id": "sync-restored", "name": "Fixture", "kind": "download",
            "url": "https://www.youtube.com/@fixture", "mode": "full"}
    config = {"output_dir": str(tmp_path / "archive"), "channels": [task]}
    queues = QueueState(QueueRepository(tmp_path / "queue.json"))
    assert queues.sync_reserve_task(task) == task["task_id"]
    monkeypatch.setattr(sync, "load_config", lambda: copy.deepcopy(config))
    monkeypatch.setattr(sync, "ARCHIVE_FILE", str(tmp_path / "missing-archive.txt"))
    monkeypatch.setattr(sync, "clear_sync_progress", lambda: None)
    monkeypatch.setattr(sync, "_check_batch_cooldown", lambda _ch: (True, ""))
    monkeypatch.setattr(sync, "config_transaction", lambda: contextlib.nullcontext(config))
    monkeypatch.setattr(sync, "_channel_folder_media_confirmed", lambda *_: False)
    monkeypatch.setattr(sync.channel_identity, "preflight_channel_identity",
                        lambda *_a, **_k: {"ok": True})
    run = Mock(return_value={"ok": True, "downloaded": 1, "errors": 0})
    monkeypatch.setattr(sync, "sync_channel", run)
    _cfg, _resolved, aliases = sync._resolve_sync_task_target(task)
    stream = Mock()
    waiting = threading.Event()
    lines = []

    def emit(segments):
        text = "".join(str(segment[0]) for segment in segments)
        lines.append(text)
        if "waiting for Processing" in text:
            waiting.set()

    stream.emit.side_effect = emit
    manager = TranscribeManager(stream, model="small")
    monkeypatch.setattr(manager, "_channel_aliases_for_job", lambda _job: aliases)
    cancel, pause, skip, clear = (threading.Event() for _ in range(4))
    holders, threads, results, errors = [], [], [], []

    def hold(job_id="gpu-first", owner="processing", kind="transcribe"):
        acquired = channel_leases.try_acquire(
            aliases, LeaseOwner(owner, job_id, kind=kind, label="Fixture owner"))
        assert acquired.ok
        holders.append(acquired.lease)
        return acquired.lease

    def start(processing=manager):
        def worker():
            try:
                results.append(sync.sync_all(
                    stream, cancel, queues=queues, transcribe_mgr=processing,
                    pause_event=pause, skip_event=skip, clear_event=clear,
                    add_downloads_from_config=False))
            except BaseException as exc:
                errors.append(exc)
        thread = threading.Thread(target=worker, daemon=True)
        threads.append(thread)
        thread.start()
        return thread

    def finished(thread):
        thread.join(3)
        assert not thread.is_alive()
        assert not errors
        return results[-1]

    yield SimpleNamespace(**locals())
    cancel.set()
    pause.clear()
    for holder in holders:
        holder.release()
    for thread in threads:
        thread.join(3)
        assert not thread.is_alive()
    assert channel_leases.active_snapshot() == ()


@pytest.mark.parametrize("with_helper", [True, False])
def test_processing_owner_waits_then_runs_same_durable_sync_task(pipeline, with_helper):
    p = pipeline
    held = p.hold()
    thread = p.start(p.manager if with_helper else object())
    assert p.waiting.wait(2)
    assert p.queues.current_sync["task_id"] == "sync-restored"
    assert p.queues.sync_snapshot() == []
    assert [row.owner for row in p.channel_leases.active_snapshot()] == ["processing"]
    p.run.assert_not_called()
    held.release()
    result = p.finished(thread)
    assert result["ok"] and result["downloaded"] == 1
    assert p.run.call_args.args[0]["task_id"] == "sync-restored"
    assert p.queues.current_sync is None
    p.stream.emit_error.assert_not_called()


def test_captured_two_job_backlog_runs_before_sync_even_between_leases(pipeline):
    p = pipeline
    first = {"task_id": "gpu-first", "kind": "transcribe", "from_download": True}
    second = {"task_id": "gpu-second", "kind": "transcribe", "from_download": True}
    later = {"task_id": "gpu-later", "kind": "transcribe", "from_download": True}
    p.manager._current_job = first
    p.manager._jobs = [second]
    first_lease = p.hold()
    thread = p.start()
    assert p.waiting.wait(2)
    with p.manager._jobs_lock:
        p.manager._current_job = None
        p.manager._jobs.append(later)
    first_lease.release()
    # No current lease exists here; the second restored task still has priority.
    time.sleep(0.35)
    assert p.channel_leases.active_snapshot() == ()
    p.run.assert_not_called()
    second_lease = p.hold("gpu-second")
    with p.manager._jobs_lock:
        p.manager._jobs.remove(second)
        p.manager._current_job = second
    time.sleep(0.3)
    p.run.assert_not_called()
    with p.manager._jobs_lock:
        p.manager._current_job = None
    second_lease.release()
    assert p.finished(thread)["ok"]
    p.run.assert_called_once()
    assert p.manager._jobs == [later]  # New arrivals never extend the captured set.


@pytest.mark.parametrize("owner,kind", [("reorganize", "download"),
                                       ("sync", "download"),
                                       ("processing", "metadata")])
def test_unrelated_owners_and_non_download_tasks_keep_busy_recovery(pipeline, owner, kind):
    p = pipeline
    with p.queues._lock:
        p.queues.sync[0]["kind"] = kind
    p.hold(owner=owner)
    result = p.finished(p.start())
    assert result["reason"] == "channel_busy"
    assert result["busy"]["requeued"]
    assert p.queues.current_sync is None
    assert p.queues.sync_snapshot()[0]["task_id"] == "sync-restored"
    assert "Pass incomplete:" in "".join(p.lines)
    assert "Pass complete:" not in "".join(p.lines)
    assert not p.waiting.is_set()
    p.run.assert_not_called()


def test_cancel_wait_keeps_unfinished_current_recovery_record(pipeline):
    p = pipeline
    p.hold()
    thread = p.start()
    assert p.waiting.wait(2)
    p.cancel.set()
    p.finished(thread)
    assert p.queues.current_sync["task_id"] == "sync-restored"
    p.run.assert_not_called()


def test_clear_wait_does_not_recreate_cleared_task(pipeline):
    p = pipeline
    p.hold()
    thread = p.start()
    assert p.waiting.wait(2)
    p.clear.set()
    p.cancel.set()
    assert p.queues.replace_current_task_durable(
        "sync", None, expected_task_id="sync-restored")
    p.finished(thread)
    assert p.queues.current_sync is None
    assert p.queues.sync_snapshot() == []
    p.run.assert_not_called()


def test_skip_wait_finishes_exact_task_without_entering_download(pipeline):
    p = pipeline
    p.hold()
    thread = p.start()
    assert p.waiting.wait(2)
    p.skip.set()
    result = p.finished(thread)
    assert result["skipped"] == 1
    assert p.queues.current_sync is None
    assert p.queues.sync_snapshot() == []
    p.run.assert_not_called()


@pytest.mark.parametrize("stop_while_paused", [False, True])
def test_wait_pause_acknowledges_and_cancel_remains_responsive(pipeline, stop_while_paused):
    p = pipeline
    held = p.hold()
    thread = p.start()
    assert p.waiting.wait(2)
    p.pause.set()
    _until(lambda: p.queues.sync_paused_active)
    held.release()
    time.sleep(0.3)
    p.run.assert_not_called()
    if stop_while_paused:
        p.cancel.set()
    else:
        p.pause.clear()
    p.finished(thread)
    assert not p.queues.sync_paused_active
    if stop_while_paused:
        p.run.assert_not_called()
        assert p.queues.current_sync["task_id"] == "sync-restored"
    else:
        p.run.assert_called_once()


def test_promotion_failure_leaves_task_pending_without_wait_or_download(pipeline, monkeypatch):
    p = pipeline
    p.hold()
    monkeypatch.setattr(p.queues, "sync_promote_task_to_current", lambda _id: False)
    result = p.finished(p.start())
    assert result["reason"] == "queue_persistence"
    assert p.queues.sync_snapshot()[0]["task_id"] == "sync-restored"
    assert p.queues.current_sync is None
    assert not p.waiting.is_set()
    p.run.assert_not_called()


def test_skip_while_paused_acknowledges_task_before_resume(pipeline):
    p = pipeline
    p.hold()
    thread = p.start()
    assert p.waiting.wait(2)
    p.pause.set()
    _until(lambda: p.queues.sync_paused_active)
    p.skip.set()
    _until(lambda: p.queues.current_sync is None)
    p.run.assert_not_called()
    assert p.pause.is_set()
    p.pause.clear()
    assert p.finished(thread)["skipped"] == 1


def test_new_unrelated_owner_stops_processing_wait_with_durable_busy_result(pipeline):
    p = pipeline
    first = {"task_id": "gpu-first", "kind": "transcribe", "from_download": True}
    p.manager._jobs = [first]
    held = p.hold()
    thread = p.start()
    assert p.waiting.wait(2)
    # The captured job remains pending after releasing its temporary lease.
    held.release()
    p.hold("reorganize-first", owner="reorganize", kind="reorganize")
    result = p.finished(thread)
    assert result["reason"] == "channel_busy"
    assert result["busy"]["requeued"]
    assert p.queues.sync_snapshot()[0]["task_id"] == "sync-restored"
    p.run.assert_not_called()


def test_skip_acknowledgement_failure_preserves_exact_recovery_slot(pipeline, monkeypatch):
    p = pipeline
    p.hold()
    thread = p.start()
    assert p.waiting.wait(2)
    monkeypatch.setattr(p.queues, "sync_finish_task_durable", lambda *_a: False)
    p.skip.set()
    result = p.finished(thread)
    assert result["reason"] == "queue_persistence"
    assert p.queues.current_sync["task_id"] == "sync-restored"
    assert p.queues.sync_snapshot() == []
    p.run.assert_not_called()


def test_channel_rename_during_wait_refreshes_target_before_admission(pipeline):
    p = pipeline
    held = p.hold()
    thread = p.start()
    assert p.waiting.wait(2)
    p.pause.set()
    _until(lambda: p.queues.sync_paused_active)
    p.config["channels"][0] = {**p.task, "name": "Renamed Fixture",
                               "folder_override": "Renamed Folder"}
    observed = []

    def download(channel, *_args, **_kwargs):
        observed.append((channel, p.channel_leases.active_snapshot()))
        return {"ok": True, "downloaded": 1, "errors": 0}

    p.run.side_effect = download
    held.release()
    p.pause.clear()
    assert p.finished(thread)["ok"]
    channel, leases = observed[0]
    assert channel["name"] == "Renamed Fixture"
    assert channel["folder_override"] == "Renamed Folder"
    assert channel["task_id"] == "sync-restored"
    assert len(leases) == 1
    assert any("renamed folder" in alias for alias in leases[0].aliases)
    assert p.aliases.issubset(leases[0].aliases)
