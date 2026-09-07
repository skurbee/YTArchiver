"""One request/response transaction for normal and chunked Whisper input."""
from __future__ import annotations

import json
import queue
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..worker_protocol import ProtocolError, decode_request, decode_response
from .job_execution import WorkerOutcome


@dataclass(frozen=True)
class InferenceResult:
    outcome: WorkerOutcome
    response: dict[str, Any] | None = None
    error: str = ""
    transport_failed: bool = False


def run_inference(*, path: str, duration: float, snapshot_io: Callable,
                  is_cancelled: Callable[[], bool], cancel: Callable[[], None],
                  progress: Callable[[int], None], accept_model: Callable[[dict], bool]) -> InferenceResult:
    """Own request validation, pipe failure, cancellation and response decoding.

    Pause is intentionally a chunk/job-boundary operation: persistent Whisper
    inference keeps draining its pipe while the caller waits to become idle.
    """
    if is_cancelled():
        return InferenceResult(WorkerOutcome.CANCELLED)
    proc, responses = snapshot_io()
    if proc is None or responses is None:
        return InferenceResult(WorkerOutcome.FAILED, error="Transcription stopped unexpectedly. Try again.",
                               transport_failed=True)
    try:
        request = {"path": path, "duration": 0, "duration_fallback": duration}
        line = json.dumps(request, allow_nan=False)
        decode_request(line, "whisper")
        proc.stdin.write(line + "\n")
        proc.stdin.flush()
    except Exception as exc:
        return InferenceResult(WorkerOutcome.CANCELLED if is_cancelled() else WorkerOutcome.FAILED,
                               error=f"Write to whisper failed: {exc}", transport_failed=True)
    last_pct = -1
    while True:
        if is_cancelled():
            cancel()
            return InferenceResult(WorkerOutcome.CANCELLED)
        current_proc, current_queue = snapshot_io()
        if current_proc is not proc or current_queue is not responses:
            return InferenceResult(WorkerOutcome.FAILED,
                                   error="Transcription stopped unexpectedly. Try again.", transport_failed=True)
        try:
            line = responses.get(timeout=0.5)
        except queue.Empty:
            if proc.poll() is None:
                continue
            line = None
        if line is None:
            return InferenceResult(WorkerOutcome.FAILED,
                                   error="Transcription stopped unexpectedly. Try again.", transport_failed=True)
        try:
            message = decode_response(line)
        except ProtocolError as exc:
            return InferenceResult(WorkerOutcome.FAILED, error=f"Invalid Whisper response: {exc}",
                                   transport_failed=True)
        status = message["status"]
        if status == "progress":
            pct = int(message["pct"])
            if pct != last_pct:
                last_pct = pct
                progress(pct)
        elif status == "cancelled":
            return InferenceResult(WorkerOutcome.CANCELLED)
        elif status == "ok":
            if not accept_model(message):
                return InferenceResult(WorkerOutcome.FAILED)
            return InferenceResult(WorkerOutcome.SUCCESS, message)
        elif status == "error":
            return InferenceResult(WorkerOutcome.FAILED, message,
                                   error=message.get("text") or "unknown worker error")


def is_gpu_memory_error(message: str) -> bool:
    low = message.lower()
    return ("cuda" in low and ("out of memory" in low or "oom" in low)) or "cublas" in low


def apply_punctuation(result: dict, *, enabled: bool, punctuate: Callable[[str], str],
                      timed_out: Callable[[], bool], align: Callable,
                      report_error: Callable[[str], None]) -> None:
    """Share success/attempt/timeout attribution without changing text rules."""
    result["_punct_attempted"] = False
    result["_punct_success"] = False
    result["_punct_timeout"] = False
    raw = result.get("text") or ""
    if not enabled or not raw:
        return
    result["_punct_attempted"] = True
    try:
        punctuated = punctuate(raw)
        if punctuated and punctuated != raw:
            result["text"] = punctuated
            align(punctuated, result.get("segments", []))
            result["_punct_success"] = True
    except Exception as exc:
        report_error(str(exc))
    finally:
        result["_punct_timeout"] = bool(timed_out())
