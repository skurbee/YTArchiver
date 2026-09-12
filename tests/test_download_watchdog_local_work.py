"""Exercise the real output adapter while its caller performs local work."""

import io
import threading
import time
from types import SimpleNamespace
from unittest import mock

import pytest

from backend.sync import ytdlp_session


@pytest.fixture
def watch(monkeypatch):
    handles = []
    monkeypatch.setattr(ytdlp_session, "budget_wait_seconds", lambda _: 0)

    def start(*, signal=None, data=b"ready\n", kill_sec=0.12):
        child = SimpleNamespace(stdout=io.BytesIO(data), stderr=None, returncode=None)
        child.poll = lambda: child.returncode
        killed = threading.Event()

        def stop(proc, **_):
            proc.returncode = -9
            killed.set()

        monkeypatch.setattr(ytdlp_session, "stop_owned_process", stop)
        stream = mock.Mock()
        kwargs = {signal[0]: signal[1]} if signal else {}
        watchdog = ytdlp_session.start_download_watchdog(
            child, stream, kill_sec=kill_sec, poll_interval=0.005, **kwargs)
        handles.append(watchdog)
        output = ytdlp_session.iter_download_output(child, watchdog)
        return SimpleNamespace(child=child, killed=killed, stream=stream,
                               watchdog=watchdog, output=output)

    yield start
    for watchdog in handles:
        watchdog.stop(timeout=1)


@pytest.mark.parametrize("data", [b"ready\n", b"ready\n" * 1000])
def test_local_processing_and_full_output_queue_do_not_look_like_a_stalled_child(watch, data):
    run = watch(data=data)
    try:
        assert next(run.output) == b"ready\n"
        assert run.watchdog.parser_busy.is_set()
        assert not run.killed.wait(0.3)
        assert not run.watchdog.stalled["hit"]
    finally:
        run.output.close()


@pytest.mark.parametrize("signal", ["cancel_event", "pause_event"])
def test_cancel_and_pause_still_stop_child_during_local_processing(watch, signal):
    event = threading.Event()
    run = watch(signal=(signal, event))
    try:
        next(run.output)
        event.set()
        assert run.killed.wait(1)
        assert not run.watchdog.stalled["hit"]
    finally:
        run.output.close()


def test_genuine_silent_child_is_stopped_and_message_describes_channel_scope(watch):
    run = watch(data=b"")
    assert run.killed.wait(1)
    assert run.watchdog.stalled["hit"]
    text = "".join(segment[0] for call in run.stream.emit.call_args_list for segment in call.args[0])
    assert "stopping this channel check" in text
    assert "skipping this download" not in text


def test_parser_break_without_retaining_iterator_clears_local_work_state(watch):
    run = watch(data=b"ready\nunread\n")
    del run.output
    # Match the production for-loop shape: no external iterator reference and
    # no explicit close when a cancel/pause/skip branch breaks the parser loop.
    for line in ytdlp_session.iter_download_output(run.child, run.watchdog):
        assert line == b"ready\n"
        assert run.watchdog.parser_busy.is_set()
        break
    assert not run.watchdog.parser_busy.is_set()
    assert not run.watchdog.output_complete


def test_silence_deadline_restarts_when_local_processing_finishes(watch):
    run = watch(kill_sec=0.5)
    next(run.output)
    assert not run.killed.wait(0.65)
    finished_at = time.monotonic()
    # Resuming the adapter clears busy and observes EOF; the child is still
    # alive, so the watchdog must protect subsequent waiting normally.
    assert list(run.output) == []
    assert not run.watchdog.parser_busy.is_set()
    assert not run.killed.wait(0.04)
    assert run.killed.wait(1)
    assert time.monotonic() - finished_at >= 0.45
