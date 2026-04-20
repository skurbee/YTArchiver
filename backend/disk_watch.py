"""
Disk-error monitor — YTArchiver.py:2361-2528 port.

When a yt-dlp / ffmpeg write fails because the output drive is full or
disconnected, pause all tasks and start a retry timer. When the drive
becomes writable again, resume.

This module exposes:
  DiskErrorMonitor(pause_event, log_stream)
    .scan_line(text) # inspect one log line; triggers handler if matched
    .is_active() # True while a disk error is being handled
    .force_check() # immediately check if disk is writable again
"""

from __future__ import annotations

import os
import re
import threading
import time
from typing import Callable, Optional

from .log_stream import LogStreamer


# Patterns that indicate a write-failure (mirrors YTArchiver's _DISK_ERROR_RE)
_DISK_ERROR_PATTERNS = [
    r"No space left on device",
    r"disk (?:is )?full",
    r"\[Errno 28\]", # ENOSPC
    r"\[Errno 30\]", # EROFS (read-only filesystem)
    r"Input/output error",
    r"\[Errno 5\]", # EIO
    r"The system cannot find the path", # Windows: drive unmapped
    r"The device is not ready", # Windows: disk disconnected
    r"\bOSError\b.*(?:writ|permission|access)",
    r"Unable to open .* for writing",
    r"Permission denied",
    r"HTTP Error 5\d\d", # not a disk error but signals upstream trouble
]
_DISK_ERROR_RE = re.compile("|".join(_DISK_ERROR_PATTERNS), re.IGNORECASE)

DISK_RETRY_MINUTES = 5 # mirrors YTArchiver._DISK_RETRY_MINUTES


def _check_directory_writable(path: str) -> bool:
    """Return True if we can open and delete a probe file in `path`."""
    if not path:
        return False
    try:
        if not os.path.isdir(path):
            return False
        probe = os.path.join(path, f".ytarchiver_probe_{os.getpid()}")
        with open(probe, "w", encoding="utf-8") as f:
            f.write("ok")
        try: os.remove(probe)
        except OSError: pass
        return True
    except OSError:
        return False


class DiskErrorMonitor:
    """Scans log output for write-failure signals and pauses tasks on match.

    Callers:
      - main.py wires scan_line() into the log_stream so every emitted line
        passes through
      - pause_events (sync + GPU) are set by the monitor when disk fails
    """

    def __init__(self, log_stream: LogStreamer,
                 on_pause: Callable[[], None],
                 on_resume: Callable[[], None],
                 get_output_dir: Callable[[], str]):
        self._stream = log_stream
        self._lock = threading.Lock()
        self._active = False
        self._start_ts = 0.0
        self._path: Optional[str] = None
        self._retry_thread: Optional[threading.Thread] = None
        self._on_pause = on_pause
        self._on_resume = on_resume
        self._get_output_dir = get_output_dir

    def is_active(self) -> bool:
        return self._active

    def scan_line(self, text: str) -> None:
        """Call on every log line. If a disk-error pattern matches and we're
        not already handling one, kick off the pause + retry loop."""
        if self._active or not text or len(text) < 8:
            return
        if not _DISK_ERROR_RE.search(text):
            return
        with self._lock:
            if self._active:
                return
            self._active = True
            self._start_ts = time.time()
            self._path = self._get_output_dir() or ""
        self._enter_error_state()

    def force_check(self) -> None:
        """Immediately probe the output dir; resume if writable."""
        threading.Thread(target=self._retry_tick, daemon=True).start()

    # ── Private ─────────────────────────────────────────────────────────

    def _enter_error_state(self) -> None:
        border = "\u2588" * 65
        self._stream.emit([
            ["\n" + border + "\n", "red"],
            ["\u2588 DISK ERROR DETECTED \u2014 All tasks paused.\n", "red"],
            ["\u2588 The output drive may be disconnected or full.\n", "red"],
            [f"\u2588 Will retry in {DISK_RETRY_MINUTES} minutes\u2026\n", "red"],
            [border + "\n\n", "red"],
        ])
        self._stream.flush()
        try: self._on_pause()
        except Exception: pass
        # Start retry thread
        self._retry_thread = threading.Thread(
            target=self._retry_loop, daemon=True)
        self._retry_thread.start()

    def _retry_loop(self) -> None:
        while self._active:
            time.sleep(DISK_RETRY_MINUTES * 60)
            if not self._active:
                return
            self._retry_tick()

    def _retry_tick(self) -> None:
        if not self._active:
            return
        path = self._path or self._get_output_dir() or ""
        writable = _check_directory_writable(path)
        if writable:
            border = "\u2588" * 65
            with self._lock:
                self._active = False
            self._stream.emit([
                ["\n" + border + "\n", "simpleline_green"],
                ["\u2588 \u2713 Disk is writable again \u2014 resuming all tasks.\n",
                 "simpleline_green"],
                [border + "\n\n", "simpleline_green"],
            ])
            self._stream.flush()
            try: self._on_resume()
            except Exception: pass
        else:
            self._stream.emit([
                [f" \u26a0 Disk still unwritable \u2014 retrying in "
                 f"{DISK_RETRY_MINUTES} minutes\u2026\n", "red"],
            ])
            self._stream.flush()
