"""Punctuation worker idle lifetime without launching a model or subprocess."""

from __future__ import annotations

import atexit
import io
import json
import os
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-punct-idle-")
atexit.register(_TEST_APPDATA.cleanup)
for _key, _leaf in (("APPDATA", "Roaming"), ("LOCALAPPDATA", "Local")):
    _directory = Path(_TEST_APPDATA.name, _leaf)
    _directory.mkdir()
    os.environ[_key] = str(_directory)

from backend.process_runner import PROCESS_REGISTRY  # noqa: E402
from backend.transcribe.punct_manager import PunctuationManager  # noqa: E402


class ControlledTimer:
    """Keep cancelled callbacks callable to reproduce a real timer race."""

    def __init__(self, interval, function, args=None, kwargs=None):
        self.interval = interval
        self.function = function
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.daemon = False
        self.started = False
        self.cancelled = False

    def start(self):
        self.started = True

    def cancel(self):
        self.cancelled = True

    def fire(self):
        self.function(*self.args, **self.kwargs)


@pytest.fixture
def worker(monkeypatch):
    timers = []
    processes = []
    manager = PunctuationManager(mock.Mock())

    def timer_factory(*args, **kwargs):
        timer = ControlledTimer(*args, **kwargs)
        timers.append(timer)
        return timer

    def start_process():
        process = mock.Mock()
        process.poll.return_value = None
        process.stdin = io.StringIO()
        process.stdout.readline.return_value = json.dumps({
            "status": "ok", "text": "Some punctuated words.",
        }) + "\n"
        manager._proc = process
        processes.append(process)
        return True

    def terminate_process(process, **_kwargs):
        process.poll.return_value = 0

    starter = mock.Mock(side_effect=start_process)
    terminate = mock.Mock(side_effect=terminate_process)
    monkeypatch.setattr(threading, "Timer", timer_factory)
    monkeypatch.setattr(manager, "_start", starter)
    monkeypatch.setattr(PROCESS_REGISTRY, "terminate_process", terminate)
    monkeypatch.setattr(
        "backend.transcribe.punct_manager.subprocess.Popen",
        mock.Mock(side_effect=AssertionError("No real subprocess in this test")),
    )
    yield SimpleNamespace(
        manager=manager, timers=timers, processes=processes,
        starter=starter, terminate=terminate,
    )
    manager._stop()


def test_sequential_requests_reuse_model_and_restart_idle_countdown(worker):
    manager = worker.manager
    assert manager.punctuate("some unpunctuated words") == "Some punctuated words."
    first = worker.timers[-1]
    assert first.interval == manager.IDLE_UNLOAD_SECONDS == 30.0
    assert first.started and first.daemon

    assert manager.punctuate("another segment of words") == "Some punctuated words."
    assert worker.starter.call_count == 1
    assert first.cancelled
    assert manager._idle_timer is worker.timers[-1]
    assert manager._idle_timer is not first
    worker.terminate.assert_not_called()


def test_idle_expiry_unloads_and_next_request_lazily_restarts(worker):
    manager = worker.manager
    manager.punctuate("some unpunctuated words")
    first_process = manager._proc
    worker.timers[-1].fire()

    worker.terminate.assert_called_once_with(first_process, timeout=2.0)
    assert manager._proc is None
    assert manager._idle_timer is None
    assert manager.punctuate("more unpunctuated words") == "Some punctuated words."
    assert worker.starter.call_count == 2
    assert manager._proc is not first_process


def test_cancelled_timer_cannot_unload_a_newer_request(worker):
    manager = worker.manager
    manager.punctuate("some unpunctuated words")
    obsolete = worker.timers[-1]
    manager.punctuate("the following caption segment")
    current = worker.timers[-1]

    obsolete.fire()

    worker.terminate.assert_not_called()
    assert manager._idle_timer is current
    assert manager._proc is worker.processes[0]


def test_idle_callback_cannot_interrupt_inflight_inference(worker):
    manager = worker.manager
    manager.punctuate("some unpunctuated words")
    obsolete = worker.timers[-1]
    inference_started = threading.Event()
    finish_inference = threading.Event()
    callback_started = threading.Event()
    callback_finished = threading.Event()
    result = []

    def read_response():
        inference_started.set()
        assert finish_inference.wait(2.0)
        return json.dumps({"status": "ok", "text": "Finished long inference."})

    def fire_obsolete_callback():
        callback_started.set()
        try:
            obsolete.fire()
        finally:
            callback_finished.set()

    manager._proc.stdout.readline.side_effect = read_response
    request = threading.Thread(target=lambda: result.append(
        manager.punctuate("a longer caption inference", timeout_sec=3.0)))
    callback = threading.Thread(target=fire_obsolete_callback)
    request.start()
    try:
        assert inference_started.wait(1.0)
        callback.start()
        assert callback_started.wait(1.0)
        worker.terminate.assert_not_called()
        assert manager._proc is worker.processes[0]
    finally:
        finish_inference.set()
        request.join(2.0)
        if callback.ident is not None:
            callback.join(2.0)

    assert not request.is_alive()
    assert callback_finished.is_set()
    assert result == ["Finished long inference."]
    worker.terminate.assert_not_called()
    assert manager._idle_timer is worker.timers[-1]
    assert manager._idle_timer is not obsolete


def test_failed_start_does_not_schedule_idle_unload(worker):
    worker.starter.side_effect = None
    worker.starter.return_value = False

    assert worker.manager.punctuate("some unpunctuated words") == "some unpunctuated words"
    assert worker.manager.last_error
    assert worker.manager._idle_timer is None
    assert worker.timers == []
    worker.terminate.assert_not_called()


def test_worker_exit_during_request_does_not_schedule_idle_unload(worker):
    manager = worker.manager
    manager.punctuate("some unpunctuated words")
    process = manager._proc
    previous_timer = worker.timers[-1]

    def exited_response():
        process.poll.return_value = 1
        return ""

    process.stdout.readline.side_effect = exited_response
    assert manager.punctuate("the next caption segment") == "the next caption segment"
    assert manager.last_error == "Punctuation worker exited without a result."
    assert previous_timer.cancelled
    assert manager._idle_timer is None
    assert len(worker.timers) == 1


def test_explicit_stop_cancels_idle_callback(worker):
    manager = worker.manager
    manager.punctuate("some unpunctuated words")
    obsolete = worker.timers[-1]
    manager._stop()
    assert obsolete.cancelled
    assert manager._idle_timer is None

    obsolete.fire()

    assert worker.terminate.call_count == 1
    assert manager._proc is None
