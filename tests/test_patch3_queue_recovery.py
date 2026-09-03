from __future__ import annotations

import contextlib
import json
import os
import tempfile
import threading
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(
    prefix="ytarchiver-queue-recovery-tests-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import queues
from backend.api_mixins.redownload_mixin import RedownloadMixin
from backend.api_mixins.sync_mixin import SyncMixin
from backend.sync import core as sync_core
from backend.transcribe import core as transcribe_core
from backend.transcribe.core import TranscribeManager


def _queue_paths(tmp_path: Path) -> tuple[Path, Path]:
    main = tmp_path / "queue.json"
    sidecar = tmp_path / "queue_resuming.json"
    return main, sidecar


def _resuming_payload(task: dict) -> dict:
    return {
        "_schema_version": 2,
        "_seq": 1,
        "resuming": {"sync": task},
    }


def test_sidecar_recovers_when_main_is_absent_and_rebuilds_main(tmp_path):
    main, sidecar = _queue_paths(tmp_path)
    task = {
        "task_id": "sync-current", "kind": "redownload",
        "name": "Current", "url": "u", "redownload_res": "720",
    }
    sidecar.write_text(json.dumps(_resuming_payload(task)), encoding="utf-8")

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.load()
        assert state.get_loaded_resuming()["sync"]["task_id"] == "sync-current"
        assert main.exists()
        saved = json.loads(main.read_text(encoding="utf-8"))
        assert saved["resuming"]["sync"]["task_id"] == "sync-current"
        assert state.to_ui_payload()["identity_ids_durable"]
        state.mark_orphan()


def test_malformed_schema_marker_migrates_without_dropping_tasks(tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    main.write_text(json.dumps({
        "_schema_version": "not-an-integer",
        "sync": [{"name": "Legacy", "url": "u"}],
        "gpu": [],
    }), encoding="utf-8")

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.load()
        assert state.sync_snapshot()[0]["task_id"]
        saved = json.loads(main.read_text(encoding="utf-8"))
        assert saved["_schema_version"] == 3
        state.mark_orphan()


def test_malformed_sidecar_cannot_override_valid_main_recovery(tmp_path):
    main, sidecar = _queue_paths(tmp_path)
    task = {"task_id": "gpu-current", "kind": "transcribe",
            "path": "video.mp4", "title": "Video"}
    main.write_text(json.dumps({
        "_schema_version": 3, "sync": [], "gpu": [],
        "resuming": {"gpu": task},
    }), encoding="utf-8")
    sidecar.write_text("{}", encoding="utf-8")

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=False):
        state = queues.QueueState()
        assert state.load()
        assert state.get_loaded_resuming()["gpu"]["task_id"] == "gpu-current"
        assert sidecar.with_name(sidecar.name + ".bak").exists()
        state.mark_orphan()


def test_sync_peek_and_failed_promotion_keep_pending_task_durable(tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    task = {"task_id": "sync-paused", "kind": "download",
            "name": "Paused", "url": "u"}
    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.sync_enqueue_with_id(task, durable=True)
        peeked = state.sync_peek_next(exclude_kinds={"redownload"})
        assert peeked["task_id"] == "sync-paused"
        assert json.loads(main.read_text(encoding="utf-8"))["sync"][0][
            "task_id"] == "sync-paused"

        with mock.patch.object(state, "_write_resuming_payload",
                               return_value=False):
            assert not state.sync_promote_task_to_current("sync-paused")

        assert state.current_sync is None
        assert state.sync_snapshot()[0]["task_id"] == "sync-paused"
        assert json.loads(main.read_text(encoding="utf-8"))["sync"][0][
            "task_id"] == "sync-paused"
        state.mark_orphan()


def test_paused_sync_requeues_pending_before_clearing_current(tmp_path):
    main, sidecar = _queue_paths(tmp_path)
    task = {"task_id": "sync-pause", "kind": "download",
            "name": "Paused", "url": "u"}
    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.sync_enqueue_with_id(task, durable=True)
        assert state.sync_promote_task_to_current("sync-pause")

        assert state.sync_requeue_current_front(task)

        assert state.current_sync is None
        assert [item["task_id"] for item in state.sync_snapshot()] == [
            "sync-pause"]
        saved = json.loads(main.read_text(encoding="utf-8"))
        assert [item["task_id"] for item in saved["sync"]] == ["sync-pause"]
        assert "sync" not in saved.get("resuming", {})
        sidecar_saved = json.loads(sidecar.read_text(encoding="utf-8"))
        assert "sync" not in sidecar_saved["resuming"]
        state.mark_orphan()


def test_paused_sync_clear_failure_keeps_pending_and_current(tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    task = {"task_id": "sync-pause", "kind": "download",
            "name": "Paused", "url": "u"}
    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.sync_enqueue_with_id(task, durable=True)
        assert state.sync_promote_task_to_current("sync-pause")

        with mock.patch.object(
                state, "_write_resuming_payload", return_value=False):
            assert not state.sync_requeue_current_front(task)

        assert state.current_sync["task_id"] == "sync-pause"
        assert [item["task_id"] for item in state.sync_snapshot()] == [
            "sync-pause"]
        saved = json.loads(main.read_text(encoding="utf-8"))
        assert saved["resuming"]["sync"]["task_id"] == "sync-pause"
        assert saved["sync"][0]["task_id"] == "sync-pause"
        state.mark_orphan()


def test_sidecar_recovers_when_main_is_corrupt_and_backs_it_up(tmp_path):
    main, sidecar = _queue_paths(tmp_path)
    main.write_text("{broken", encoding="utf-8")
    task = {"task_id": "gpu-current", "kind": "transcribe",
            "path": "video.mp4", "title": "Video"}
    sidecar.write_text(json.dumps({
        "_schema_version": 2, "_seq": 1, "resuming": {"gpu": task},
    }), encoding="utf-8")

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.load()
        assert Path(str(main) + ".bak").read_text(encoding="utf-8") == "{broken"
        assert state.get_loaded_resuming()["gpu"]["task_id"] == "gpu-current"
        assert json.loads(main.read_text(encoding="utf-8"))[
            "resuming"]["gpu"]["task_id"] == "gpu-current"
        state.mark_orphan()


def test_journal_collision_and_defer_restart_follow_queue_ids_and_order(
        tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    journal = tmp_path / "pending.json"
    deferred = tmp_path / "deferred.mp4"
    following = tmp_path / "following.mp4"
    deferred.write_bytes(b"video")
    following.write_bytes(b"video")
    main.write_text(json.dumps({
        "_schema_version": 3,
        "sync": [],
        "gpu": [
            {"task_id": "collision", "kind": "transcribe",
             "path": str(following), "title": "Following"},
            {"task_id": "collision", "kind": "transcribe",
             "path": str(deferred), "title": "Deferred"},
        ],
    }), encoding="utf-8")
    journal.write_text(json.dumps([
        {"task_id": "collision", "kind": "transcribe",
         "path": str(deferred), "title": "Deferred",
         "defer_requested": True, "retranscribe": True},
        {"task_id": "collision", "kind": "transcribe",
         "path": str(following), "title": "Following",
         "retranscribe": True},
    ]), encoding="utf-8")

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True), \
            mock.patch.object(transcribe_core, "_pending_journal_path",
                              return_value=journal), \
            mock.patch.object(transcribe_core, "_resolve_transcript_paths",
                              return_value=None):
        state = queues.QueueState()
        assert state.load()
        manager = TranscribeManager(mock.Mock())
        manager.attach_queues(state)
        assert manager.load_pending() == 2

        visible_ids = [item["task_id"] for item in state.gpu_snapshot()]
        runtime_ids = [item["task_id"] for item in manager._jobs]
        saved = json.loads(journal.read_text(encoding="utf-8"))
        assert len(set(visible_ids)) == 2
        assert runtime_ids == visible_ids
        assert [item["task_id"] for item in saved] == visible_ids
        assert [Path(item["path"]).name for item in saved] == [
            "following.mp4", "deferred.mp4"]
        assert not any(item.get("defer_requested") for item in saved)
        state.mark_orphan()


def test_journal_migration_failure_restores_queue_and_exposes_no_runtime_job(
        tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    journal = tmp_path / "pending.json"
    video = tmp_path / "video.mp4"
    video.write_bytes(b"video")
    visible = {"task_id": "gpu-visible", "kind": "transcribe",
               "path": str(video), "title": "Video"}
    main.write_text(json.dumps({
        "_schema_version": 3, "sync": [], "gpu": [visible],
    }), encoding="utf-8")
    journal.write_text(json.dumps([{
        "task_id": "legacy-collision", "kind": "transcribe",
        "path": str(video), "title": "Video", "retranscribe": True,
    }]), encoding="utf-8")

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True), \
            mock.patch.object(transcribe_core, "_pending_journal_path",
                              return_value=journal):
        state = queues.QueueState()
        assert state.load()
        before = state.gpu_snapshot()
        manager = TranscribeManager(mock.Mock())
        manager.attach_queues(state)
        with mock.patch.object(manager, "_write_pending_snapshot",
                               return_value=False):
            assert manager.load_pending() == 0
        assert manager._jobs == []
        assert state.gpu_snapshot() == before
        assert json.loads(main.read_text(encoding="utf-8"))["gpu"] == before
        state.mark_orphan()


def test_partial_runtime_restore_reorders_to_authoritative_queue_tail(tmp_path):
    following = tmp_path / "following.mp4"
    deferred = tmp_path / "deferred.mp4"
    following.write_bytes(b"video")
    deferred.write_bytes(b"video")
    state = queues.QueueState()
    with mock.patch.object(state, "save_debounced"):
        state.gpu_enqueue({"task_id": "gpu-following", "kind": "transcribe",
                           "path": str(following), "title": "Following"})
        state.gpu_enqueue({"task_id": "gpu-deferred", "kind": "transcribe",
                           "path": str(deferred), "title": "Deferred"})
    manager = TranscribeManager(mock.Mock())
    manager.attach_queues(state)
    manager._jobs = [{
        "task_id": "gpu-deferred", "kind": "transcribe",
        "path": str(deferred), "title": "Deferred",
        "cancel": threading.Event(),
    }]

    with mock.patch.object(manager, "_persist_pending", return_value=True):
        assert manager._restore_runtime_jobs_from_queue() == 1
    assert [job["task_id"] for job in manager._jobs] == [
        "gpu-following", "gpu-deferred"]


def test_gpu_running_cancel_fsync_failure_does_not_signal_or_clear(tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    journal = tmp_path / "pending.json"
    job = {
        "task_id": "gpu-current", "kind": "transcribe",
        "path": str(tmp_path / "video.mp4"), "title": "Video",
        "cancel": threading.Event(),
    }
    Path(job["path"]).write_bytes(b"video")

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True), \
            mock.patch.object(transcribe_core, "_pending_journal_path",
                              return_value=journal):
        state = queues.QueueState()
        assert state.replace_current_task_durable(
            "gpu", TranscribeManager._queue_payload_for_job(job),
            expected_task_id="")
        manager = TranscribeManager(mock.Mock())
        manager.attach_queues(state)
        manager._current_job = job
        assert manager._write_pending_snapshot([
            manager._snapshot_pending_job(job)])

        class Api(SyncMixin):
            pass

        api = Api()
        api._queues = state
        api._transcribe = manager
        api._log_stream = mock.Mock()
        with mock.patch.object(transcribe_core.os, "fsync",
                               side_effect=PermissionError("denied")):
            result = api.gpu_skip_current("gpu-current")

        assert not result["ok"]
        assert not job["cancel"].is_set()
        assert state.current_gpu["task_id"] == "gpu-current"
        assert json.loads(journal.read_text(encoding="utf-8"))[0][
            "task_id"] == "gpu-current"
        state.mark_orphan()


def test_gpu_running_cancel_signals_only_after_both_durable_commits():
    order = []
    cancel = mock.Mock()
    cancel.set.side_effect = lambda: order.append("signal")
    manager = TranscribeManager(mock.Mock())
    manager._current_job = {
        "task_id": "gpu-current", "kind": "transcribe",
        "path": "video.mp4", "cancel": cancel,
    }

    def save_journal(_snapshot):
        order.append("journal")
        return True

    def save_visible():
        order.append("queue")
        return True

    with mock.patch.object(manager, "_write_pending_snapshot",
                           side_effect=save_journal), \
            mock.patch.object(manager, "_send_cancel_command",
                              side_effect=lambda: order.append("worker")):
        assert manager.cancel_current_durable("gpu-current", save_visible)

    assert order == ["journal", "queue", "signal", "worker"]


def test_completed_compress_survives_current_clear_failure_as_cleanup_only(
        tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    journal = tmp_path / "pending.json"
    video = tmp_path / "video.mp4"
    video.write_bytes(b"video")
    job = {
        "task_id": "gpu-compress", "kind": "compress",
        "path": str(video), "title": "Video", "channel": "Channel",
        "quality": "Average", "output_res": "720",
        "cancel": threading.Event(),
    }

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True), \
            mock.patch.object(transcribe_core, "_pending_journal_path",
                              return_value=journal):
        state = queues.QueueState()
        assert state.gpu_enqueue_with_id(
            TranscribeManager._queue_payload_for_job(job), durable=True)
        manager = TranscribeManager(mock.Mock())
        manager.attach_queues(state)
        manager._jobs = [job]
        manager._paused.clear()
        manager._manual_drain.set()

        with mock.patch.object(manager, "_compress_one",
                               return_value=transcribe_core._WorkerOutcome.SUCCESS) as run, \
                mock.patch.object(manager, "_flush_batch_stats"), \
                mock.patch.object(state, "replace_current_task_durable",
                                  return_value=False):
            manager._worker_loop()

        assert run.call_count == 1
        assert manager._jobs == [job]
        assert job["_cleanup_only"]
        saved = json.loads(journal.read_text(encoding="utf-8"))
        assert saved[0]["task_id"] == "gpu-compress"
        assert saved[0]["cleanup_only"] is True
        assert state.current_gpu["task_id"] == "gpu-compress"

        manager._paused.clear()
        manager._manual_drain.set()
        with mock.patch.object(manager, "_compress_one") as rerun, \
                mock.patch.object(manager, "_flush_batch_stats"):
            manager._worker_loop()
        rerun.assert_not_called()
        assert manager._jobs == []
        assert state.current_gpu is None
        state.mark_orphan()


def test_cancelled_transcribe_checkpoint_failure_keeps_current_recovery(
        tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    journal = tmp_path / "pending.json"
    video = tmp_path / "video.mp4"
    video.write_bytes(b"video")
    job = {
        "task_id": "gpu-cancelled", "kind": "transcribe",
        "path": str(video), "title": "Video", "channel": "Channel",
        "cancel": threading.Event(), "_pending_decremented": True,
    }

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True), \
            mock.patch.object(transcribe_core, "_pending_journal_path",
                              return_value=journal):
        state = queues.QueueState()
        assert state.gpu_enqueue_with_id(
            TranscribeManager._queue_payload_for_job(job), durable=True)
        manager = TranscribeManager(mock.Mock())
        manager.attach_queues(state)
        manager._jobs = [job]
        manager._paused.clear()
        manager._manual_drain.set()
        real_persist = manager._persist_pending
        failed_checkpoint = False

        def fail_terminal_checkpoint_once():
            nonlocal failed_checkpoint
            if job.get("_cleanup_only") and not failed_checkpoint:
                failed_checkpoint = True
                return False
            return real_persist()

        with mock.patch.object(
                manager, "_transcribe_one",
                return_value=transcribe_core._WorkerOutcome.CANCELLED), \
                mock.patch.object(manager, "_prepare_job_model",
                                  return_value=True), \
                mock.patch.object(manager, "_persist_pending",
                                  side_effect=fail_terminal_checkpoint_once), \
                mock.patch.object(manager, "_flush_batch_stats"), \
                mock.patch.object(
                    state, "replace_current_task_durable",
                    wraps=state.replace_current_task_durable) as clear_current:
            manager._worker_loop()

        assert failed_checkpoint
        clear_current.assert_not_called()
        assert state.current_gpu["task_id"] == "gpu-cancelled"
        assert manager._jobs == [job]
        assert job["_cleanup_only"]
        saved = json.loads(journal.read_text(encoding="utf-8"))
        assert saved[0]["task_id"] == "gpu-cancelled"
        assert saved[0]["cleanup_only"] is True
        state.mark_orphan()


def test_sync_running_cancel_fsync_failure_does_not_signal_or_clear(tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    task = {"task_id": "sync-current", "kind": "download",
            "name": "Current", "url": "u"}
    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.replace_current_task_durable(
            "sync", task, expected_task_id="")

        class Api(SyncMixin):
            pass

        api = Api()
        api._queues = state
        api._sync_skip = threading.Event()
        api._log_stream = mock.Mock()
        with mock.patch.object(queues.os, "fsync",
                               side_effect=PermissionError("denied")):
            result = api.sync_skip_current("sync-current")

        assert not result["ok"]
        assert not api._sync_skip.is_set()
        assert state.current_sync["task_id"] == "sync-current"
        state.mark_orphan()


def test_processing_clear_rolls_back_queue_and_current_on_journal_failure(
        tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    pending = {"task_id": "gpu-pending", "kind": "transcribe",
               "path": "pending.mp4", "title": "Pending"}
    current = {"task_id": "gpu-current", "kind": "transcribe",
               "path": "current.mp4", "title": "Current"}
    current_job = dict(current, cancel=threading.Event())

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.gpu_enqueue_with_id(pending, durable=True)
        assert state.replace_current_task_durable(
            "gpu", current, expected_task_id="")
        manager = TranscribeManager(mock.Mock())
        manager.attach_queues(state)
        manager._jobs = [dict(pending, cancel=threading.Event())]
        manager._current_job = current_job

        class Api(SyncMixin):
            pass

        api = Api()
        api._queues = state
        api._transcribe = manager
        api._on_queue_changed = mock.Mock()
        with mock.patch.object(manager, "clear_pending_journal",
                               return_value=False):
            result = api.gpu_clear_queue()

        assert not result["ok"]
        assert [item["task_id"] for item in state.gpu_snapshot()] == [
            "gpu-pending"]
        assert state.current_gpu["task_id"] == "gpu-current"
        assert [item["task_id"] for item in manager._jobs] == ["gpu-pending"]
        assert not current_job["cancel"].is_set()
        state.mark_orphan()


def test_sync_clear_fsync_failure_rolls_back_and_never_signals(tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    pending = {"task_id": "sync-pending", "kind": "download",
               "name": "Pending", "url": "p"}
    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.sync_enqueue_with_id(pending, durable=True)

        class Api(SyncMixin):
            pass

        api = Api()
        api._queues = state
        api._sync_cancel = threading.Event()
        api._redwnl_lock = threading.Lock()
        api._redwnl_pending = []
        api._redwnl_cancel = threading.Event()
        api._on_queue_changed = mock.Mock()
        with mock.patch.object(queues.os, "fsync",
                               side_effect=PermissionError("denied")):
            result = api.sync_clear_queue()

        assert not result["ok"]
        assert not api._sync_cancel.is_set()
        assert [item["task_id"] for item in state.sync_snapshot()] == [
            "sync-pending"]
        state.mark_orphan()


def test_gpu_clear_fsync_failure_restores_exact_in_memory_queue(tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    tasks = [
        {"task_id": "gpu-a", "kind": "transcribe", "path": "a.mp4"},
        {"task_id": "gpu-b", "kind": "compress", "path": "b.mp4"},
    ]
    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        for task in tasks:
            assert state.gpu_enqueue_with_id(task, durable=True)
        before = state.gpu_snapshot()
        with mock.patch.object(queues.os, "fsync",
                               side_effect=PermissionError("denied")):
            assert state.gpu_clear() == -1
        assert state.gpu_snapshot() == before
        assert json.loads(main.read_text(encoding="utf-8"))["gpu"] == before
        state.mark_orphan()


def test_sync_clear_does_not_requeue_task_popped_in_cancel_race():
    sync_all = __import__("backend.sync.sync_all", fromlist=["sync_all"])
    state = queues.QueueState()
    channel = {"task_id": "sync-race", "kind": "download",
               "name": "Race", "url": "race"}
    cancel = threading.Event()
    cleared = threading.Event()
    with mock.patch.object(state, "save_debounced"):
        state.sync_enqueue(channel)
    original_peek = state.sync_peek_next

    def peek_then_clear(**kwargs):
        item = original_peek(**kwargs)
        state.sync_clear()
        cleared.set()
        cancel.set()
        return item

    try:
        with mock.patch.object(state, "sync_peek_next",
                               side_effect=peek_then_clear), \
                mock.patch.object(state, "save_now", return_value=True), \
                mock.patch.object(sync_all, "load_config",
                                  return_value={"channels": [channel]}), \
                mock.patch.object(sync_all, "ARCHIVE_FILE",
                                  "__missing_archive__.txt"), \
                mock.patch.object(sync_all, "clear_sync_progress"), \
                mock.patch.object(
                    sync_all, "config_transaction",
                    side_effect=lambda: contextlib.nullcontext({})):
            sync_all.sync_all(
                mock.Mock(), cancel_event=cancel, queues=state,
                add_downloads_from_config=False, clear_event=cleared)
        assert state.sync_snapshot() == []
    finally:
        state.mark_orphan()


def test_sync_peek_failure_preserves_queue_and_does_not_stamp_success():
    sync_all = __import__("backend.sync.sync_all", fromlist=["sync_all"])
    state = queues.QueueState()
    channel = {"task_id": "sync-unreadable", "kind": "download",
               "name": "Unreadable", "url": "unreadable"}
    config = {"channels": [channel]}
    with mock.patch.object(state, "save_debounced"):
        state.sync_enqueue(channel)

    try:
        with mock.patch.object(
                state, "sync_peek_next", side_effect=OSError("read failed")), \
                mock.patch.object(sync_all, "load_config",
                                  return_value=config), \
                mock.patch.object(sync_all, "ARCHIVE_FILE",
                                  "__missing_archive__.txt"), \
                mock.patch.object(sync_all, "clear_sync_progress"), \
                mock.patch.object(
                    sync_all, "config_transaction",
                    side_effect=lambda: contextlib.nullcontext(config)):
            result = sync_all.sync_all(
                mock.Mock(), queues=state,
                add_downloads_from_config=False)

        assert result["ok"] is False
        assert result["reason"] == "queue_persistence"
        assert [item["task_id"] for item in state.sync_snapshot()] == [
            "sync-unreadable"]
        assert "last_sync" not in config
    finally:
        state.mark_orphan()


def test_natural_sync_completion_preserves_skipped_redownload_row(tmp_path):
    sync_all = __import__("backend.sync.sync_all", fromlist=["sync_all"])
    main, _sidecar = _queue_paths(tmp_path)
    redownload = {
        "task_id": "sync-rd", "kind": "redownload",
        "name": "Redo", "url": "redo", "redownload_res": "720",
    }
    config = {"channels": [redownload]}

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.sync_enqueue_with_id(redownload, durable=True)
        try:
            with mock.patch.object(sync_all, "load_config",
                                   return_value=config), \
                    mock.patch.object(sync_all, "ARCHIVE_FILE",
                                      "__missing_archive__.txt"), \
                    mock.patch.object(sync_all, "clear_sync_progress"), \
                    mock.patch.object(
                        sync_all, "config_transaction",
                        side_effect=lambda: contextlib.nullcontext(config)):
                result = sync_all.sync_all(
                    mock.Mock(), queues=state,
                    add_downloads_from_config=False)

            assert result["ok"] is True
            assert [item["task_id"] for item in state.sync_snapshot()] == [
                "sync-rd"]
            saved = json.loads(main.read_text(encoding="utf-8"))
            assert [item["task_id"] for item in saved["sync"]] == ["sync-rd"]
        finally:
            state.mark_orphan()


def test_completed_sync_clear_failure_keeps_exact_recovery_identity(tmp_path):
    sync_all = __import__("backend.sync.sync_all", fromlist=["sync_all"])
    main, _sidecar = _queue_paths(tmp_path)
    channel = {
        "task_id": "sync-complete", "kind": "download",
        "name": "Channel", "url": "channel",
    }
    config = {"channels": [channel]}

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.sync_enqueue_with_id(channel, durable=True)
        real_replace = state.replace_current_task_durable

        def fail_only_completion(lane, replacement, *, expected_task_id=None):
            if replacement is None:
                with mock.patch.object(
                        queues.os, "fsync",
                        side_effect=PermissionError("denied")):
                    return real_replace(
                        lane, replacement, expected_task_id=expected_task_id)
            return real_replace(
                lane, replacement, expected_task_id=expected_task_id)

        try:
            with mock.patch.object(
                    state, "replace_current_task_durable",
                    side_effect=fail_only_completion), \
                    mock.patch.object(sync_all, "load_config",
                                      return_value=config), \
                    mock.patch.object(sync_all, "ARCHIVE_FILE",
                                      "__missing_archive__.txt"), \
                    mock.patch.object(sync_all, "clear_sync_progress"), \
                    mock.patch.object(
                        sync_all, "config_transaction",
                        side_effect=lambda: contextlib.nullcontext(config)), \
                    mock.patch.object(
                        sync_all, "sync_channel",
                        return_value=sync_core.SyncResult(
                            ok=True, downloaded=1, errors=0)):
                result = sync_all.sync_all(
                    mock.Mock(), queues=state,
                    add_downloads_from_config=False)

            assert result["ok"] is False
            assert result["reason"] == "queue_persistence"
            assert state.current_sync["task_id"] == "sync-complete"
            assert "last_sync" not in config
            saved = json.loads(main.read_text(encoding="utf-8"))
            assert saved["resuming"]["sync"]["task_id"] == "sync-complete"
        finally:
            state.mark_orphan()


def test_next_sync_requeues_stale_current_instead_of_erasing_it(tmp_path):
    sync_all = __import__("backend.sync.sync_all", fromlist=["sync_all"])
    main, _sidecar = _queue_paths(tmp_path)
    channel = {
        "task_id": "sync-stale", "kind": "download",
        "name": "Stale", "url": "stale",
    }
    config = {"channels": [channel]}

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.replace_current_task_durable(
            "sync", channel, expected_task_id="")
        try:
            with mock.patch.object(sync_all, "load_config",
                                   return_value=config), \
                    mock.patch.object(sync_all, "ARCHIVE_FILE",
                                      "__missing_archive__.txt"), \
                    mock.patch.object(sync_all, "clear_sync_progress"), \
                    mock.patch.object(
                        sync_all, "config_transaction",
                        side_effect=lambda: contextlib.nullcontext(config)), \
                    mock.patch.object(
                        sync_all, "sync_channel",
                        return_value=sync_core.SyncResult(
                            ok=True, downloaded=0, errors=0)) as run:
                result = sync_all.sync_all(
                    mock.Mock(), queues=state,
                    add_downloads_from_config=False)

            assert result["ok"] is True
            run.assert_called_once()
            assert run.call_args.args[0]["task_id"] == "sync-stale"
            assert state.current_sync is None
            assert state.sync_snapshot() == []
        finally:
            state.mark_orphan()


def test_regular_sync_requeues_stale_redownload_for_its_owner(tmp_path):
    sync_all = __import__("backend.sync.sync_all", fromlist=["sync_all"])
    main, _sidecar = _queue_paths(tmp_path)
    redownload = {
        "task_id": "sync-stale-rd", "kind": "redownload",
        "name": "Redo", "url": "redo", "redownload_res": "720",
    }
    config = {"channels": [redownload]}

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.replace_current_task_durable(
            "sync", redownload, expected_task_id="")
        try:
            with mock.patch.object(sync_all, "load_config",
                                   return_value=config), \
                    mock.patch.object(sync_all, "ARCHIVE_FILE",
                                      "__missing_archive__.txt"), \
                    mock.patch.object(sync_all, "clear_sync_progress"), \
                    mock.patch.object(
                        sync_all, "config_transaction",
                        side_effect=lambda: contextlib.nullcontext(config)), \
                    mock.patch.object(sync_all, "sync_channel") as run:
                result = sync_all.sync_all(
                    mock.Mock(), queues=state,
                    add_downloads_from_config=False)

            assert result["ok"] is True
            run.assert_not_called()
            assert state.current_sync is None
            assert [item["task_id"] for item in state.sync_snapshot()] == [
                "sync-stale-rd"]
        finally:
            state.mark_orphan()


def test_filtered_sync_clear_refuses_uncommitted_identity_migration(tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    tasks = [
        {"task_id": "sync-ordinary", "kind": "download",
         "name": "Ordinary", "url": "ordinary"},
        {"task_id": "sync-rd", "kind": "redownload",
         "name": "Redo", "url": "redo", "redownload_res": "720"},
    ]
    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        for task in tasks:
            assert state.sync_enqueue_with_id(task, durable=True)
        before_memory = state.sync_snapshot()
        before_disk = main.read_bytes()
        state._identity_ids_durable = False

        assert state.sync_clear_except_kinds({"redownload"}) == -1
        assert state.sync_snapshot() == before_memory
        assert main.read_bytes() == before_disk
        state.mark_orphan()


def test_metadata_worker_does_not_resurrect_task_cleared_after_promotion(
        tmp_path):
    sync_all = __import__("backend.sync.sync_all", fromlist=["sync_all"])
    main, _sidecar = _queue_paths(tmp_path)
    task = {
        "task_id": "sync-meta-cleared", "kind": "metadata",
        "name": "Metadata", "url": "metadata", "refresh": True,
    }
    config = {"channels": [task]}

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.sync_enqueue_with_id(task, durable=True)
        real_promote = state.sync_promote_task_to_current

        def promote_then_clear(task_id):
            assert real_promote(task_id)
            assert state.replace_current_task_durable(
                "sync", None, expected_task_id=task_id)
            return True

        def fetch_after_clear(*_args, **_kwargs):
            assert state.current_sync is None
            return {"ok": False, "cancelled": True, "errors": 0}

        try:
            with mock.patch.object(
                    state, "sync_promote_task_to_current",
                    side_effect=promote_then_clear), \
                    mock.patch.object(sync_all, "load_config",
                                      return_value=config), \
                    mock.patch.object(sync_all, "ARCHIVE_FILE",
                                      "__missing_archive__.txt"), \
                    mock.patch.object(sync_all, "clear_sync_progress"), \
                    mock.patch.object(
                        sync_all, "config_transaction",
                        side_effect=lambda: contextlib.nullcontext(config)), \
                    mock.patch(
                        "backend.metadata.fetch_channel_metadata",
                        side_effect=fetch_after_clear) as fetch:
                result = sync_all.sync_all(
                    mock.Mock(), queues=state,
                    add_downloads_from_config=False)

            fetch.assert_called_once()
            assert result["ok"] is False
            assert result["reason"] == "queue_persistence"
            assert state.current_sync is None
        finally:
            state.mark_orphan()


def test_redownload_worker_does_not_resurrect_cleared_promoted_task(tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    task = {
        "task_id": "sync-rd-cleared", "kind": "redownload",
        "name": "Redo", "url": "redo", "redownload_res": "720",
    }

    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.replace_current_task_durable(
            "sync", task, expected_task_id="")
        assert state.replace_current_task_durable(
            "sync", None, expected_task_id="sync-rd-cleared")

        class Api(RedownloadMixin):
            def __init__(self):
                self._queues = state
                self._log_stream = mock.Mock()
                self._redwnl_cancel = threading.Event()
                self._sync_pause = threading.Event()
                self._window = None
                self._on_queue_changed = mock.Mock()
                self.services = None

        def run_after_clear(*_args, **_kwargs):
            assert state.current_sync is None
            return {"ok": False, "cancelled": True}

        api = Api()
        with mock.patch(
                "backend.redownload.redownload_channel",
                side_effect=run_after_clear) as run, \
                mock.patch("backend.archive_scan.invalidate_channel"):
            api._run_redownload_one(
                {"name": "Redo", "url": "redo"},
                str(tmp_path), "720", None, rd_task=task)

        run.assert_called_once()
        assert state.current_sync is None
        state.mark_orphan()


def test_redownload_resume_keeps_durable_rows_and_stages_mixed_queue():
    state = queues.QueueState()
    with mock.patch.object(state, "save_debounced"):
        state.sync_enqueue({"task_id": "sync-regular", "kind": "download",
                            "name": "Regular", "url": "regular"})
        state.sync_enqueue({"task_id": "sync-redownload",
                            "kind": "redownload", "name": "Redo",
                            "url": "redo", "redownload_res": "720"})
    calls = []

    class Api(RedownloadMixin):
        def __init__(self):
            self._queues = state
            self.services = None

        def chan_redownload(self, identity, resolution, **kwargs):
            calls.append((identity, resolution, kwargs))
            return {"ok": True, "queued": True}

    result = Api().resume_pending_redownloads()
    assert result == {"ok": True, "resumed": 1, "skipped": 0,
                      "regular_pending": 1}
    assert [item["task_id"] for item in state.sync_snapshot()] == [
        "sync-regular", "sync-redownload"]
    assert calls[0][2]["task_id"] == "sync-redownload"
    assert calls[0][2]["_queue_only"] is True


def test_failed_redownload_resume_keeps_its_only_durable_row():
    state = queues.QueueState()
    with mock.patch.object(state, "save_debounced"):
        state.sync_enqueue({"task_id": "sync-redownload",
                            "kind": "redownload", "name": "Redo",
                            "url": "redo", "redownload_res": "720"})

    class Api(RedownloadMixin):
        def __init__(self):
            self._queues = state
            self.services = None

        @staticmethod
        def chan_redownload(*_args, **_kwargs):
            return {"ok": False, "error": "not available"}

    result = Api().resume_pending_redownloads()
    assert not result["ok"]
    assert result["regular_pending"] == 0
    assert state.sync_snapshot()[0]["task_id"] == "sync-redownload"


def test_redownload_promotion_never_removes_last_durable_copy(tmp_path):
    main, _sidecar = _queue_paths(tmp_path)
    task = {"task_id": "sync-redownload", "kind": "redownload",
            "name": "Redo", "url": "redo", "redownload_res": "720"}
    with mock.patch.object(queues, "QUEUE_FILE", main), \
            mock.patch.object(queues, "config_is_writable", return_value=True):
        state = queues.QueueState()
        assert state.sync_enqueue_with_id(task, durable=True)
        with mock.patch.object(state, "sync_remove_task", return_value=False):
            assert not state.sync_promote_task_to_current("sync-redownload")
        assert state.sync_snapshot()[0]["task_id"] == "sync-redownload"
        assert state.current_sync is None
        state.mark_orphan()
