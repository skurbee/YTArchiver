"""Sample confirmations are exact, expire safely, and unblock cancellation."""
from __future__ import annotations

import json
import os
import queue
import tempfile
import threading
from unittest import mock

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-controls-samples-")
os.environ["APPDATA"] = _PROFILE.name
os.environ["LOCALAPPDATA"] = _PROFILE.name

from backend.api_mixins import redownload_mixin  # noqa: E402


class Stream:
    def __init__(self):
        self.events = queue.Queue()

    def emit(self, segments):
        self.events.put(json.loads(segments[0][0]))

    def flush(self):
        pass

    def emit_dim(self, _message):
        pass


def start_sample(api, stream, cancel=None):
    results = []
    cancel = cancel or threading.Event()
    worker = threading.Thread(target=lambda: results.append(
        api._wait_redownload_sample(20, "savings", "720p", 10, cancel, stream)))
    worker.start()
    event = stream.events.get(timeout=2)
    assert event["kind"] == "redownload_sample"
    return worker, results, event, cancel


def test_timeout_cancels_and_late_answer_does_not_affect_next_sample(monkeypatch):
    monkeypatch.setattr(redownload_mixin, "_SAMPLE_CONFIRM_TIMEOUT_SEC", 0.05)
    api, stream = redownload_mixin.RedownloadMixin(), Stream()
    worker, result, opened, _cancel = start_sample(api, stream)
    worker.join(2)
    assert not worker.is_alive()
    assert result == ["cancel"]
    assert stream.events.get(timeout=2) == {"kind": "redownload_sample_closed", "sample_id": opened["sample_id"], "reason": "timeout"}
    monkeypatch.setattr(redownload_mixin, "_SAMPLE_CONFIRM_TIMEOUT_SEC", 2)
    next_worker, next_result, next_opened, _ = start_sample(api, stream)
    assert not api.redownload_sample_confirm("continue", opened["sample_id"])["ok"]
    assert next_worker.is_alive()
    assert api.redownload_sample_confirm("cancel", next_opened["sample_id"])["ok"]
    next_worker.join(2)
    assert next_result == ["cancel"]


def test_answer_requires_exact_sample_and_waits_for_backend_ack():
    api, stream = redownload_mixin.RedownloadMixin(), Stream()
    worker, results, opened, _ = start_sample(api, stream)
    assert not api.redownload_sample_confirm("continue", "wrong-id")["ok"]
    assert worker.is_alive()
    response = api.redownload_sample_confirm("480", opened["sample_id"])
    assert response["ok"] and response["sample_id"] == opened["sample_id"]
    worker.join(2)
    assert results == ["480"]
    assert stream.events.get(timeout=2)["reason"] == "answered"
    assert not api.redownload_sample_confirm("continue", opened["sample_id"])["ok"]


def test_cancelled_worker_closes_visible_confirmation_promptly():
    api, stream = redownload_mixin.RedownloadMixin(), Stream()
    worker, result, _opened, cancel = start_sample(api, stream)
    cancel.set()
    worker.join(2)
    assert not worker.is_alive()
    assert result == ["cancel"]
    assert stream.events.get(timeout=2)["reason"] == "cancelled"


def test_ambiguous_legacy_ack_cannot_answer_two_samples():
    api = redownload_mixin.RedownloadMixin()
    first_stream, second_stream = Stream(), Stream()
    first, results_a, opened_a, cancel_a = start_sample(api, first_stream)
    second, results_b, opened_b, cancel_b = start_sample(api, second_stream)
    try:
        assert not api.redownload_sample_confirm("continue")["ok"]
        assert api.redownload_sample_confirm("continue", opened_a["sample_id"])["ok"]
        first.join(2)
        assert results_a == ["continue"]
        assert second.is_alive()
        assert api.redownload_sample_confirm("cancel", opened_b["sample_id"])["ok"]
        second.join(2)
        assert results_b == ["cancel"]
    finally:
        cancel_a.set()
        cancel_b.set()
        first.join(2)
        second.join(2)


def test_failed_sample_publish_cancels_instead_of_continuing():
    api = redownload_mixin.RedownloadMixin()
    stream = mock.Mock()
    stream.emit.side_effect = RuntimeError("event bridge unavailable")
    assert api._wait_redownload_sample(20, "smaller", "720p", 10, threading.Event(), stream) == "cancel"
    assert api._redwnl_samples == {}


def test_cancel_before_worker_entry_does_not_start_download_and_clears_only_at_exit(monkeypatch):
    from backend import archive_scan, redownload

    class Queue:
        def __init__(self):
            self.current_sync = {"task_id": "cancelled", "cancel_requested": True}
            self.replacements = []

        def replace_current_task_durable(self, lane, task, *, expected_task_id):
            assert lane == "sync" and expected_task_id == "cancelled"
            self.replacements.append(task)
            self.current_sync = task
            return True

    api, queued = redownload_mixin.RedownloadMixin(), Queue()
    api._redownload_queues = lambda: queued
    api._redownload_log_stream = mock.Mock(return_value=mock.Mock())
    api._redwnl_cancel = threading.Event()
    api._redwnl_cancel.set()
    api._window = None
    api._on_queue_changed = mock.Mock()
    run = mock.Mock()
    monkeypatch.setattr(redownload, "redownload_channel", run)
    monkeypatch.setattr(archive_scan, "invalidate_channel", mock.Mock())
    api._run_redownload_one({"name": "Channel"}, "unused", "720", "", rd_task={"task_id": "cancelled"})
    run.assert_not_called()
    assert queued.replacements == [None]
    assert queued.current_sync is None
