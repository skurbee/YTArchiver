"""Shared inference, extraction ownership and commit-boundary regressions."""
import json
import queue
import subprocess
import sys
import threading
from unittest import mock

import pytest

from backend.process_runner import PROCESS_REGISTRY
from backend.transcribe import audio_extract, core, punct_manager
from backend.transcribe.inference import apply_punctuation
from backend.transcribe.job_execution import WorkerOutcome


def manager_with_response(response):
    manager = core.TranscribeManager(mock.Mock(), model="small")
    process = mock.Mock()
    process.poll.return_value = None
    messages = queue.Queue()
    for message in response:
        messages.put(json.dumps(message) if message is not None else None)
    manager._proc, manager._line_queue = process, messages
    manager._loaded_model = "small"
    manager._stop_subprocess = mock.Mock()
    manager._graceful_cancel_current = mock.Mock(return_value=True)
    job = {"path": "synthetic.mp4", "title": "Fixture", "requested_model": "small",
           "cancel": threading.Event(), "task_id": "fixture-inference", "state_cb": mock.Mock()}
    return manager, process, messages, job


@pytest.mark.parametrize("chunk", [False, True])
def test_gpu_failure_uses_the_same_single_cpu_retry_policy(chunk):
    manager, _, _, job = manager_with_response([{"status": "error", "text": "CUDA out of memory"}])
    outcome, result = manager._transcribe_single_file(
        job["path"], job, _log_prefix="Section 1/2" if chunk else "", duration_fallback=1)
    assert outcome is WorkerOutcome.RETRY and result is None
    assert job["_retried_cpu"] and manager._cpu_fallback_active
    manager._line_queue.put(json.dumps({"status": "error", "text": "CUBLAS failed"}))
    again, _ = manager._transcribe_single_file(job["path"], job, duration_fallback=1)
    assert again is WorkerOutcome.FAILED


@pytest.mark.parametrize("response", [[None], [[1, 2]], [{"status": "progress", "pct": "bad"}],
                                     [{"status": "ok", "model": "medium", "text": "wrong model"}]])
def test_invalid_or_incomplete_response_cannot_become_success(response):
    manager, _, _, job = manager_with_response(response)
    outcome, result = manager._transcribe_single_file(job["path"], job, duration_fallback=1)
    assert outcome is WorkerOutcome.FAILED and result is None


def test_cancellation_during_inference_uses_graceful_worker_cancel():
    manager, process, messages, job = manager_with_response([])
    process.stdin.flush.side_effect = job["cancel"].set
    outcome, _ = manager._transcribe_single_file(job["path"], job, duration_fallback=1)
    assert outcome is WorkerOutcome.CANCELLED
    manager._graceful_cancel_current.assert_called_once()
    manager._stop_subprocess.assert_not_called()


def test_progress_is_published_as_structured_state_with_task_identity():
    manager, _, _, job = manager_with_response([
        {"status": "progress", "pct": 32},
        {"status": "ok", "model": "small", "text": "fixture", "segments": []}])
    outcome, _ = manager._transcribe_single_file(job["path"], job, "Section 1/2", duration_fallback=1)
    assert outcome is WorkerOutcome.SUCCESS
    payload = job["state_cb"].call_args.args[0]
    assert payload["state"] == "transcribing" and payload["pct"] == 32
    assert payload["task_id"] == job["task_id"]


def test_punctuation_timeout_attribution_is_shared_even_on_exception():
    for result in ({"text": "raw", "segments": []}, {"text": "merged raw", "segments": []}):
        apply_punctuation(result, enabled=True, punctuate=mock.Mock(side_effect=TimeoutError()),
                          timed_out=lambda: True, align=mock.Mock(), report_error=mock.Mock())
        assert result["_punct_attempted"] and result["_punct_timeout"] and not result["_punct_success"]


def test_chunk_coordinator_preserves_retry_and_cleans_only_its_scratch(monkeypatch, tmp_path):
    manager, _, _, job = manager_with_response([])
    scratch = tmp_path / "owned-chunks"
    scratch.mkdir()
    monkeypatch.setattr("tempfile.mkdtemp", lambda **kwargs: str(scratch))
    monkeypatch.setattr(core, "extract_audio_chunk", lambda *a, **k: audio_extract.AudioExtraction(WorkerOutcome.SUCCESS))
    monkeypatch.setattr(manager, "_transcribe_single_file", lambda *a, **k: (WorkerOutcome.RETRY, None))
    assert manager._transcribe_chunked(job, 7500) is WorkerOutcome.RETRY
    assert not scratch.exists()


@pytest.mark.parametrize("stop_mode", ["cancel", "force-owner"])
def test_silent_audio_extractor_is_registered_and_cancelled_promptly(monkeypatch, tmp_path, stop_mode):
    cancel = threading.Event()
    launched, process_holder, result = threading.Event(), [], []
    original_popen = subprocess.Popen
    def launch(command, **kwargs):
        child = original_popen([sys.executable, "-B", "-c", "import time; time.sleep(30)"], **kwargs)
        process_holder.append(child)
        launched.set()
        return child
    monkeypatch.setattr(audio_extract, "find_ffmpeg", lambda: "fixture-ffmpeg")
    monkeypatch.setattr(audio_extract.subprocess, "Popen", launch)
    worker = threading.Thread(target=lambda: result.append(audio_extract.extract_audio_chunk(
        "fixture.mp4", str(tmp_path / "chunk.wav"), start=0, duration=60,
        cancel_event=cancel, task_id="fixture-extractor")))
    worker.start()
    assert launched.wait(2)
    try:
        # The supervisor registers immediately, before waiting on silent pipes.
        import time
        deadline = time.monotonic() + 2
        while not any(row.task_id == "fixture-extractor" for row in PROCESS_REGISTRY.snapshot()):
            assert time.monotonic() < deadline
            time.sleep(0.01)
        if stop_mode == "cancel":
            cancel.set()
        else:
            PROCESS_REGISTRY.terminate_owner("processing", timeout=1.0)
        worker.join(3)
        expected = WorkerOutcome.CANCELLED if stop_mode == "cancel" else WorkerOutcome.FAILED
        assert not worker.is_alive() and result[0].outcome is expected
        assert process_holder[0].poll() is not None
        assert not any(row.task_id == "fixture-extractor" for row in PROCESS_REGISTRY.snapshot())
    finally:
        cancel.set()
        if process_holder[0].poll() is None:
            process_holder[0].kill()
        process_holder[0].wait(timeout=2)
        worker.join(3)


def test_cancelled_audio_preparation_never_spawns(monkeypatch, tmp_path):
    cancel = threading.Event()
    cancel.set()
    spawn = mock.Mock()
    monkeypatch.setattr(audio_extract.subprocess, "Popen", spawn)
    result = audio_extract.extract_audio_chunk("fixture.mp4", str(tmp_path / "chunk.wav"),
                                               start=0, duration=60, cancel_event=cancel, task_id="fixture")
    assert result.outcome is WorkerOutcome.CANCELLED
    spawn.assert_not_called()


def test_extractor_supervisor_bootstrap_failure_stops_exact_child(monkeypatch, tmp_path):
    real_popen = subprocess.Popen
    children = []
    def launch(command, **kwargs):
        child = real_popen([sys.executable, "-B", "-c", "import time; time.sleep(30)"], **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(audio_extract, "find_ffmpeg", lambda: "fixture-ffmpeg")
    monkeypatch.setattr(audio_extract.subprocess, "Popen", launch)
    monkeypatch.setattr(audio_extract, "supervise_streaming_process",
                        mock.Mock(side_effect=RuntimeError("reader thread unavailable")))
    try:
        result = audio_extract.extract_audio_chunk(
            "fixture.mp4", str(tmp_path / "chunk.wav"), start=0, duration=60,
            cancel_event=threading.Event(), task_id="fixture-bootstrap")
        assert result.outcome is WorkerOutcome.FAILED
        assert "reader thread unavailable" in result.error
        assert children[0].poll() is not None
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=2)
            if child.stderr:
                child.stderr.close()


def test_invalid_cancel_ack_requires_worker_stop_instead_of_raising():
    manager, _, _, _ = manager_with_response([[1, 2]])
    assert not manager._wait_for_cancel_ack(timeout=0.01)


@pytest.mark.parametrize("response", ['[]', '{"status":"ok","text":{}}'])
def test_punctuation_parent_rejects_invalid_response_and_restarts(response):
    manager = punct_manager.PunctuationManager(mock.Mock())
    manager._proc = mock.Mock()
    manager._proc.poll.return_value = None
    manager._proc.stdout.readline.return_value = response
    manager._stop = mock.Mock()
    assert manager.punctuate("three fixture words") == "three fixture words"
    assert manager.last_error
    manager._stop.assert_called_once()


def test_short_punctuation_request_does_not_inherit_previous_timeout():
    manager = punct_manager.PunctuationManager(mock.Mock())
    manager.last_was_timeout = True
    assert manager.punctuate("short") == "short"
    assert not manager.last_was_timeout
