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
    # "Permission denied" used to be bare here, but yt-dlp also prints
    # that string for non-disk reasons (age-gate, member-only, expired
    # cookies, API auth rejection) — tripping the watchdog and pausing
    # ALL workers for 5 minutes on a benign YouTube restriction.
    # Require a filesystem-specific context keyword alongside so only
    # real write failures trigger the pause.
    # audit F-53: stricter Permission-denied pattern. Require the
    # associated path to end in a media/partial extension so yt-dlp's
    # age-gate / cookie-expired errors don't trip the watchdog (they
    # say "Permission denied" with no file path).
    r"Permission denied:.*\.(part|temp|ytdl|mp4|mkv|webm|m4a)",
    r"(?:writ|output|file|disk|save).*Permission denied",
    # Windows variant of the same error.
    r"Access is denied.*\.(part|temp|ytdl|mp4|mkv|webm|m4a)",
    # audit D-12: HTTP 5xx REMOVED — it was flagging YouTube's
    # upstream errors as "DISK ERROR" and pausing every worker for
    # 5 minutes on transient 502s from YouTube. Disk and upstream
    # service outages are unrelated; mixing them in the watchdog
    # pattern was pure noise.
]
_DISK_ERROR_RE = re.compile("|".join(_DISK_ERROR_PATTERNS), re.IGNORECASE)

DISK_RETRY_MINUTES = 5 # mirrors YTArchiver._DISK_RETRY_MINUTES


def _check_directory_writable(path: str) -> bool:
    """Return True if we can open and delete a probe file in `path`
    AND at least 2 GB of free space is available.

    audit D-13: the bare writability probe passed at 1 MB free, which
    let the monitor prematurely "recover" after an ENOSPC pause — the
    next multi-GB download immediately failed, tripping the pause
    again. Require meaningful free space before declaring the drive
    healthy so recovery doesn't oscillate.
    """
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
        # Minimum 2 GB free before calling the drive "writable" from
        # a watchdog-recovery perspective. Tunable via env.
        try:
            import shutil as _sh
            _min_free = int(os.environ.get("YTARCHIVER_DISK_MIN_FREE_GB", "2"))
            _free_bytes = _sh.disk_usage(path).free
            if _free_bytes < _min_free * 1024 * 1024 * 1024:
                return False
        except Exception:
            pass
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
