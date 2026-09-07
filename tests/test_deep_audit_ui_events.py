"""Delivery and state contracts independent of disposable display output."""
import threading
from unittest import mock

from backend.log_stream import LogStreamer
from backend.services.reliable_events import ReliableEventChannel


def test_lost_ack_retries_same_identity_until_acknowledged():
    delivered = []

    def deliver(batch):
        delivered.append(batch)
        if len(delivered) == 1:
            raise OSError("bridge response lost")
        return [event["revision"] for event in batch]

    channel = ReliableEventChannel(deliver, schedule=False)
    channel.publish("control", "sample:1", {"kind": "redownload_sample"})
    channel.deliver_once()
    assert len(channel.pending()) == 1
    channel.deliver_once()
    assert delivered[0] == delivered[1]
    assert channel.pending() == []


def test_ack_for_inflight_state_cannot_remove_newer_state():
    def deliver(batch):
        channel.publish("processing", "job:1", {"state": "paused"})
        return [batch[0]["revision"]]

    channel = ReliableEventChannel(deliver, schedule=False)
    channel.publish("processing", "job:1", {"state": "transcribing", "pct": 40})
    channel.deliver_once()
    assert channel.pending()[0]["payload"] == {"state": "paused"}


def test_unavailable_consumer_does_not_starve_later_topics():
    seen = []
    channel = ReliableEventChannel(lambda batch: seen.extend(batch), schedule=False)
    for number in range(100):
        channel.publish("control", str(number), {"number": number})
    channel.deliver_once()
    channel.deliver_once()
    assert {event["payload"]["number"] for event in seen} == set(range(100))
    assert len(channel.pending()) == 100


def test_expired_prompt_is_retired_but_close_supersedes_undelivered_open():
    channel = ReliableEventChannel(lambda _batch: [], schedule=False, clock=lambda: 100)
    channel.publish("control", "old", {"kind": "open"}, expires_at=99)
    channel.publish("control", "current", {"kind": "open"}, expires_at=101)
    channel.publish("control", "current", {"kind": "closed"})
    assert [event["payload"] for event in channel.pending()] == [{"kind": "closed"}]


def make_stream():
    stream = LogStreamer()
    stream.events.close()
    stream.events = ReliableEventChannel(stream._deliver_events, schedule=False)
    return stream


def test_control_retention_survives_exhausted_display_retry_budget():
    stream = make_stream()
    stream.emit_control({"kind": "cookie_alert", "message": "Sign in again"})
    with stream._lock:
        for _ in range(stream.MAX_RETRY_BATCHES * 4):
            stream._enqueue_retry_locked([[['display only', None]]], [], 1)
    assert stream._retry_dropped > 0
    assert stream.events.pending()[0]["payload"]["kind"] == "cookie_alert"
    assert stream._buffer == []


def test_scanners_receive_untruncated_text_before_simple_mode_filter():
    stream = make_stream()
    stream.simple_mode = True
    received = []
    stream.add_line_scanner(received.append)
    text = "x" * (stream.MAX_SEGMENT_TEXT_CHARS + 1) + " disk full"
    stream.emit([[text, "dim"]])
    assert received == [text]
    assert stream._buffer == []


def test_late_percentage_does_not_resume_paused_or_finalizing_job():
    stream = make_stream()
    for state in ("paused", "finalizing", "needs_attention"):
        stream.emit_processing({"request_id": "job", "state": state, "message": state})
        stream.emit_processing({"request_id": "job", "state": "transcribing", "pct": 81})
        payload = stream.events.pending()[0]["payload"]
        assert (payload["state"], payload["message"], payload["pct"]) == (state, state, 81)
    stream.emit_processing({"request_id": "job", "state": "resuming"})
    stream.emit_processing({"request_id": "job", "state": "transcribing", "pct": 82})
    assert stream.events.pending()[0]["payload"]["state"] == "transcribing"
    stream.emit_processing({"request_id": "job", "kind": "complete"})
    assert stream.emit_processing({"request_id": "job", "state": "paused"}) == 0
    assert stream.events.pending()[0]["payload"]["kind"] == "complete"


def test_delivery_waits_for_ready_and_returns_frontend_acknowledgements():
    window = mock.Mock()
    stream = make_stream()
    stream.set_window(window)
    window.evaluate_js.return_value = [1]
    stream.emit_control({"kind": "cookie_alert"})
    stream.events.deliver_once()
    window.evaluate_js.assert_not_called()
    stream._ready = True
    stream.events.deliver_once()
    assert stream.events.pending() == []
    assert "_appEventBatch" in window.evaluate_js.call_args.args[0]


def test_coalesced_resume_preserves_lifecycle_epoch_in_latest_progress():
    stream = make_stream()
    stream.emit_processing({"request_id": "job", "state": "paused"})
    paused = stream.events.pending()[0]["payload"]
    stream.emit_processing({"request_id": "job", "state": "transcribing", "pct": 20})
    held = stream.events.pending()[0]["payload"]
    assert (held["state"], held["phase_revision"]) == ("paused", paused["phase_revision"])
    stream.emit_processing({"request_id": "job", "state": "resuming"})
    stream.emit_processing({"request_id": "job", "state": "transcribing", "pct": 21})
    resumed = stream.events.pending()[0]["payload"]
    assert resumed["state"] == "transcribing"
    assert resumed["phase_revision"] > paused["phase_revision"]
    assert len(stream.events.pending()) == 1


def test_failed_timer_start_keeps_event_and_allows_next_wake(monkeypatch):
    timers = []
    class Timer:
        def __init__(self, delay, callback):
            self.callback = callback
            self.number = len(timers)
            timers.append(self)
        def start(self):
            if self.number == 0:
                raise RuntimeError("no thread available")
            self.callback()  # also detects starting with the nonreentrant lock held
        def cancel(self):
            pass
    monkeypatch.setattr("backend.services.reliable_events.threading.Timer", Timer)
    delivered = []
    def deliver(batch):
        delivered.extend(batch)
        return [item["revision"] for item in batch]
    channel = ReliableEventChannel(deliver)
    channel.publish("control", "fixture", {"kind": "fixture"})
    assert len(channel.pending()) == 1 and not delivered
    worker = threading.Thread(target=channel.wake, daemon=True)
    worker.start()
    worker.join(timeout=1)
    assert not worker.is_alive(), "immediate timer delivery must not hold the state lock"
    assert len(delivered) == 1 and not channel.pending()
    channel.close()
