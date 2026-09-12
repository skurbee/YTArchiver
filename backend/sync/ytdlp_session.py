"""yt-dlp subprocess session helpers."""

from __future__ import annotations

import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from .. import utils as _utils
from .. import youtube_traffic
from ..log import get_logger, swallow
from ..process_runner import (
    PROCESS_REGISTRY,
    ProcessOutputReader,
    finish_owned_process,
    popen_ytdlp,
    stop_owned_process,
)
from ..youtube_request_process import budget_wait_seconds

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
    parser_busy: threading.Event = field(default_factory=threading.Event)
    output_reader: ProcessOutputReader | None = None
    output_complete: bool = False

    def stop(self, timeout: float | None = None) -> None:
        self.stop_event.set()
        if self.thread.is_alive():
            self.thread.join(timeout=timeout)
        if self.output_reader is not None:
            self.output_reader.close()


def popen_ytdlp_process(cmd: list[str], *, startupinfo: Any = None,
                        cancel_event=None, pause_event=None, stream=None,
                        text: bool = False,
                        ) -> subprocess.Popen:
    """Start a registered process, optionally decoding its output for probes."""
    permission = youtube_traffic.acquire(
        "channel_sync", cancel_event=cancel_event,
        pause_event=pause_event, stream=stream)
    if not permission.get("ok"):
        raise OSError(
            permission.get("error") or "YouTube traffic governor cancelled")
    output_options = {"text": True, "encoding": "utf-8", "errors": "replace"} if text else {}
    proc = popen_ytdlp(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,
        startupinfo=startupinfo,
        env=_utils.utf8_subprocess_env(),
        request_cancel_event=cancel_event,
        request_pause_event=pause_event,
        **output_options,
    )
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
    last_output = [time.monotonic()]
    stalled = {"hit": False}
    parser_busy = threading.Event()

    def _run() -> None:
        accounted_wait = budget_wait_seconds(proc)
        while not stop_event.wait(poll_interval):
            waited = budget_wait_seconds(proc)
            last_output[0] += max(0.0, waited - accounted_wait)
            accounted_wait = waited
            if proc.poll() is not None:
                return
            if ((cancel_event is not None and cancel_event.is_set())
                    or (pause_event is not None and pause_event.is_set())):
                try:
                    stop_owned_process(proc, registry=PROCESS_REGISTRY, timeout=2.0)
                except Exception as exc:
                    swallow("cancel kill", exc)
                return
            # The parser performs local caption/index work between reads.
            # During that work a full output queue can also block the child;
            # neither condition is evidence of a stalled YouTube request.
            # Cancellation above remains active throughout local processing.
            if parser_busy.is_set():
                continue
            if time.monotonic() - last_output[0] > kill_sec:
                stalled["hit"] = True
                try:
                    stream.emit([[f" ⚠ No response for "
                                  f"{kill_sec}s — stopping this channel "
                                  f"check. Sync again to check the "
                                  f"remaining videos.\n", "red"]])
                    stream.flush()
                except Exception as exc:
                    swallow("stall-warn stream flush", exc)
                try:
                    stop_owned_process(proc, registry=PROCESS_REGISTRY, timeout=2.0)
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
        parser_busy=parser_busy,
    )


def iter_download_output(proc: subprocess.Popen, watchdog: DownloadWatchdog):
    """Yield raw stdout for the sync parser with bounded post-exit draining.

    Sync retains its immediate pause/cancel watchdog and binary decoder.
    Pipe mechanics are shared with all other streaming subprocesses, so an
    inherited stdout handle cannot keep the parser blocked after child exit.
    """
    reader = ProcessOutputReader(proc).start()
    watchdog.output_reader = reader
    post_exit_deadline = None
    try:
        while not watchdog.stop_event.is_set():
            item = reader.read(timeout=0.1)
            if item is not None:
                post_exit_deadline = None
                channel, line = item
                if channel == "stdout":
                    watchdog.parser_busy.set()
                    try:
                        yield line
                    finally:
                        # Start a fresh silence interval only once the caller
                        # is ready to consume output again. Set the timestamp
                        # before clearing busy so the watchdog cannot see the
                        # old deadline between those two operations.
                        watchdog.last_output[0] = time.monotonic()
                        watchdog.parser_busy.clear()
                continue
            if reader.finished:
                break
            if proc.poll() is not None:
                if post_exit_deadline is None:
                    post_exit_deadline = time.monotonic() + 1.0
                elif time.monotonic() >= post_exit_deadline:
                    break
    finally:
        watchdog.output_complete = (reader.finished and not reader.failed.is_set()
                                    and not watchdog.stop_event.is_set())
        reader.close()


def finish_ytdlp_process(
        proc: subprocess.Popen,
        *,
        wait_timeout: float = 10.0,
        terminate_timeout: float = 5.0,
        kill_timeout: float = 2.0,
        watchdog: DownloadWatchdog | None = None,
) -> int | None:
    """Finish the owned child without closing a pipe held by a live reader."""
    return finish_owned_process(
        proc, registry=PROCESS_REGISTRY, wait_timeout=wait_timeout,
        stop_timeout=terminate_timeout + kill_timeout,
        output_reader=getattr(watchdog, "output_reader", None),
    )


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
