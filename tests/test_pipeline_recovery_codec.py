"""Exercise both durable representations through the production codec."""

import json
import threading
from dataclasses import fields
from unittest import mock

import pytest

from backend.transcribe.core import TranscribeManager
from backend.transcribe.job_execution import WorkerOutcome
from backend.transcribe.recovery import ProcessingRecord, RecoveryState


@pytest.mark.parametrize("kind", ["transcribe", "compress"])
def test_every_recovery_field_survives_both_stores(kind):
    state = RecoveryState(**{f.name: ("success" if f.name == "completed_outcome" else True)
                             for f in fields(RecoveryState)})
    record = ProcessingRecord(task_id="gpu-exact", path="fixture.mp4", title="Title",
                              kind=kind, requested_model="small", recovery=state)
    runtime = record.runtime_payload(cancel_event=threading.Event(), consume_defer=False)
    runtime["cb"] = lambda: None
    runtime["_completion_result"] = object()
    journal = TranscribeManager._snapshot_pending_job(runtime)
    queue = TranscribeManager._queue_payload_for_job(runtime)
    json.dumps(journal)
    json.dumps(queue)
    assert ProcessingRecord.decode(journal).recovery == state
    for f in fields(RecoveryState):
        if f.name != "defer_requested":
            assert queue[f.name] == getattr(state, f.name)
    assert journal["task_id"] == queue["task_id"] == "gpu-exact"
    assert "cancel" not in journal and "cb" not in queue


def test_old_interrupted_write_restores_replacement_but_caption_write_does_not():
    assert ProcessingRecord.decode({"write_intent": True}, interrupted=True).recovery.retry_as_replace
    native = ProcessingRecord.decode({"write_intent": True, "caption_recovery": True}, interrupted=True)
    assert native.recovery.retry_required
    assert not native.recovery.retry_as_replace
    assert not ProcessingRecord.decode({}).recovery.output_complete


def test_failed_followup_keeps_completed_output_and_retry_does_not_repeat_callback():
    manager = object.__new__(TranscribeManager)
    manager._jobs_lock = threading.Lock()
    manager._jobs = []
    manager._inline_caption_jobs = []
    manager._stream = mock.Mock()
    manager._persist_pending = mock.Mock(return_value=True)
    manager.compress_enqueue = mock.Mock(side_effect=[False, True])
    callback = mock.Mock()
    job = {"task_id": "gpu-parent", "path": "fixture.mp4", "cb": callback,
           "compress_after": {"quality": "Average"}}
    manager._current_job = job
    assert not manager._finish_successful_job(job, {"ok": True}, terminal_outcome=WorkerOutcome.SUCCESS)
    saved = manager._snapshot_pending_job(job)
    assert saved["output_complete"] and saved["followup_pending"]
    assert not saved["followup_enqueued"]
    assert manager._retry_completed_followup(job) is WorkerOutcome.SUCCESS
    callback.assert_called_once_with({"ok": True})
    assert manager._snapshot_pending_job(job)["followup_enqueued"]


def test_reserving_followup_before_output_is_invalid():
    with pytest.raises(ValueError):
        RecoveryState().followup_reserved()


def test_completion_transitions_do_not_recreate_cleared_runtime_markers():
    job = {"_retry_as_replace": False, "_no_speech_pending": True, "_cleanup_only": True}
    # The two durable completion helpers have finished their pending work.
    job.pop("_no_speech_pending")
    job.pop("_cleanup_only")
    RecoveryState.decode(job, runtime=True).output_finished(
        "no_speech", has_followup=True).apply(job)
    RecoveryState.decode(job, runtime=True).followup_reserved().apply(job)
    assert "_no_speech_pending" not in job
    assert "_cleanup_only" not in job
    assert "_followup_pending" not in job
    assert job["_retry_as_replace"] is False
    RecoveryState.decode(job, runtime=True).followup_failed().apply(job)
    assert "_followup_enqueued" not in job
    assert job["_followup_pending"] is True
    # The persisted schema remains complete despite sparse runtime markers.
    persisted = TranscribeManager._snapshot_pending_job(job)
    assert persisted["no_speech_pending"] is False
    assert persisted["cleanup_only"] is False
