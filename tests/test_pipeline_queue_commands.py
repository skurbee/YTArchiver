"""Cross-store failures must restore work and never signal a successor."""

import threading
from unittest import mock

from backend.queues import QueueState
from backend.transcribe.core import TranscribeManager


def test_sync_defer_second_commit_failure_restores_complete_pending_snapshot():
    state = QueueState()
    current = {"task_id": "exact", "kind": "download", "name": "A", "url": "a"}
    sibling = {"task_id": "next", "kind": "download", "name": "B", "url": "b"}
    state.current_sync = current.copy()
    state.sync = [sibling.copy()]
    signal = mock.Mock()
    with mock.patch.object(state, "save_now", return_value=True), mock.patch.object(
            state, "replace_current_task_durable", return_value=False):
        result = state.command_sync_current("exact", defer=True, signal=signal)
    assert not result["ok"]
    assert state.sync_snapshot() == [sibling]
    assert state.current_sync == current
    signal.assert_not_called()
    state.mark_orphan()


def test_sync_command_signals_only_committed_exact_identity():
    state = QueueState()
    state.current_sync = {"task_id": "new", "kind": "redownload", "name": "N", "url": "n"}
    signal = mock.Mock()
    with mock.patch.object(state, "save_now", return_value=True), mock.patch.object(
            state, "_write_resuming_payload", return_value=True):
        assert not state.command_sync_current("old", signal=signal)["ok"]
        assert state.command_sync_current("new", signal=signal)["ok"]
    assert signal.call_count == 1
    assert signal.call_args.args[0]["cancel_requested"]
    assert state.current_sync["task_id"] == "new"
    state.mark_orphan()


def test_processing_service_compensates_queue_when_journal_rejects_remove():
    manager = TranscribeManager(mock.Mock())
    queues = QueueState()
    manager.attach_queues(queues)
    job = {"task_id": "gpu-exact", "path": "fixture.mp4", "kind": "transcribe",
           "cancel": threading.Event(), "retranscribe": True}
    manager._jobs = [job]
    queues.gpu = [manager._queue_payload_for_job(job)]
    with mock.patch.object(manager, "_persist_pending", return_value=False), mock.patch.object(
            queues, "save_now", return_value=True), mock.patch.object(
            manager, "_notify_jobs_cancelled") as notify:
        assert not manager.queue_commands().remove({"gpu-exact"})
    assert manager._jobs == [job]
    assert queues.gpu_snapshot()[0]["task_id"] == "gpu-exact"
    notify.assert_not_called()
    assert not job["cancel"].is_set()
    queues.mark_orphan()
