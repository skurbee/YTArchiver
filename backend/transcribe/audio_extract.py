"""Owned, cancellable audio preparation for a long-video transcription."""
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from ..compress import find_ffmpeg
from ..process_runner import PROCESS_REGISTRY, stop_owned_process, supervise_streaming_process
from ..subprocess_util import make_startupinfo
from .job_execution import WorkerOutcome


@dataclass(frozen=True)
class AudioExtraction:
    outcome: WorkerOutcome
    error: str = ""
    timed_out: bool = False


def extract_audio_chunk(source: str, destination: str, *, start: float,
                        duration: float, cancel_event, task_id: str) -> AudioExtraction:
    if cancel_event.is_set():
        return AudioExtraction(WorkerOutcome.CANCELLED)
    executable = find_ffmpeg()
    if not executable:
        return AudioExtraction(WorkerOutcome.FAILED, "ffmpeg was not found")
    command = [executable, "-y", "-hide_banner", "-loglevel", "error",
               "-ss", str(start), "-t", str(duration), "-i", source,
               "-vn", "-ac", "1", "-ar", "16000", "-acodec", "pcm_s16le", destination]
    process = None
    try:
        process = subprocess.Popen(
            command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", errors="replace", startupinfo=make_startupinfo(),
            creationflags=(getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0))
        result = supervise_streaming_process(
            process, cancel_event=cancel_event, timeout=max(1200.0, duration * 3.0),
            owner="processing", task_id=task_id, role="audio-extraction")
        if result.cancelled or cancel_event.is_set():
            return AudioExtraction(WorkerOutcome.CANCELLED)
        if result.timed_out:
            return AudioExtraction(WorkerOutcome.FAILED, "audio extraction timed out", timed_out=True)
        if result.returncode != 0 or not result.output_complete:
            return AudioExtraction(WorkerOutcome.FAILED,
                                   "\n".join(result.stderr_tail[-8:]) or "audio extraction did not finish")
        if not Path(destination).is_file() or Path(destination).stat().st_size == 0:
            return AudioExtraction(WorkerOutcome.FAILED, "audio extraction produced no audio file")
        return AudioExtraction(WorkerOutcome.SUCCESS)
    except Exception as exc:
        # A supervisor bootstrap failure (for example, no reader thread can
        # start) must not leave this phase running outside the job's lifetime.
        if process is not None:
            stop_owned_process(process)
            if process.poll() is not None:
                PROCESS_REGISTRY.unregister(process)
        return AudioExtraction(WorkerOutcome.CANCELLED if cancel_event.is_set() else WorkerOutcome.FAILED,
                               str(exc))
