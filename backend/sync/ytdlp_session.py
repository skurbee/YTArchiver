"""yt-dlp subprocess session helpers."""

from __future__ import annotations

import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Any

from .. import utils as _utils
from ..log import get_logger, swallow
from ..process_runner import PROCESS_REGISTRY

_log = get_logger(__name__)


@dataclass(slots=True)
class LaunchResult:
    """Outcome of a yt-dlp launch attempt before stdout parsing begins."""

    proc: subprocess.Popen | None = None
    cancelled: bool = False
    failed: bool = False


@dataclass(slots=True)
class DownloadWatchdog:
    """State shared with the watchdog thread that can kill stalled yt-dlp."""

    stop_event: threading.Event
    last_output: list[float]
    stalled: dict[str, bool]
    thread: threading.Thread

    def stop(self, timeout: float | None = None) -> None:
        self.stop_event.set()
        if self.thread.is_alive():
            self.thread.join(timeout=timeout)


def popen_ytdlp_process(cmd: list[str], *, startupinfo: Any = None
                        ) -> subprocess.Popen:
    """Start one yt-dlp process in binary stdout mode and register it."""
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,
        startupinfo=startupinfo,
        env=_utils.utf8_subprocess_env(),
    )
    try:
        PROCESS_REGISTRY.register(proc)
    except Exception as exc:
        swallow("process-registry register", exc)
    return proc


def start_download_watchdog(
        proc: subprocess.Popen,
        stream,
        *,
        cancel_event=None,
        pause_event=None,
        kill_sec: int = 120,
        poll_interval: float = 3.0,
) -> DownloadWatchdog:
    """Kill a silent yt-dlp process when it stalls or pause/cancel fires."""
    stop_event = threading.Event()
    last_output = [time.time()]
    stalled = {"hit": False}

    def _run() -> None:
        while not stop_event.wait(poll_interval):
            if proc.poll() is not None:
                return
            if ((cancel_event is not None and cancel_event.is_set())
                    or (pause_event is not None and pause_event.is_set())):
                try:
                    proc.kill()
                except Exception as exc:
                    swallow("cancel kill", exc)
                return
            if time.time() - last_output[0] > kill_sec:
                stalled["hit"] = True
                try:
                    stream.emit([[f" ⚠ No response for "
                                  f"{kill_sec}s — skipping this "
                                  f"download and moving on.\n", "red"]])
                    stream.flush()
                except Exception as exc:
                    swallow("stall-warn stream flush", exc)
                try:
                    proc.kill()
                except Exception as exc:
                    swallow("stall kill", exc)
                return

    thread = threading.Thread(target=_run, name="dl-watchdog", daemon=True)
    thread.start()
    return DownloadWatchdog(
        stop_event=stop_event,
        last_output=last_output,
        stalled=stalled,
        thread=thread,
    )


def finish_ytdlp_process(
        proc: subprocess.Popen,
        *,
        wait_timeout: float = 10.0,
        terminate_timeout: float = 5.0,
        kill_timeout: float = 2.0,
) -> int | None:
    """Wait for yt-dlp, escalate if hung, close stdout, and unregister."""
    try:
        proc.wait(timeout=wait_timeout)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=terminate_timeout)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
                proc.wait(timeout=kill_timeout)
            except Exception as exc:
                swallow("force-kill wait", exc)
    try:
        if proc.stdout is not None:
            proc.stdout.close()
    except Exception as exc:
        swallow("stdout close", exc)
    try:
        PROCESS_REGISTRY.unregister(proc)
    except Exception as exc:
        swallow("process-registry unregister", exc)
    return proc.returncode


def launch_ytdlp_process(
        cmd: list[str],
        stream,
        *,
        startupinfo: Any = None,
        cancel_event=None,
        attempts: int = 3,
        retry_sleep: float = 2.0,
) -> LaunchResult:
    """Launch yt-dlp with bounded retries and ProcessRegistry registration."""
    attempts = max(1, int(attempts))
    for attempt in range(attempts):
        if cancel_event is not None and cancel_event.is_set():
            return LaunchResult(cancelled=True)
        try:
            return LaunchResult(
                proc=popen_ytdlp_process(cmd, startupinfo=startupinfo))
        except OSError as exc:
            if attempt == attempts - 1:
                stream.emit([
                    ["ERROR: ", "red"],
                    [f"Couldn't start the download tool after 3 tries: {exc}\n",
                     "red"],
                ])
                return LaunchResult(failed=True)
            stream.emit_dim(
                f" Launch attempt {attempt + 1} failed ({exc}); "
                "retrying in 2s...")
            time.sleep(retry_sleep)
    return LaunchResult(failed=True)
