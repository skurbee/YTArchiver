"""
Log streaming — Python → JS batched message pipe.

Workers (sync, transcribe, compress, reorg, scanners) call `stream.emit(segments)`
with segment arrays in the same shape the UI renderer expects:
    [["text", "tag"], ["more text", None], ...]

Calls are batched so high-frequency output (hundreds of lines/sec during a
whisper run) doesn't saturate the evaluate_js bridge. The batch flushes on
a 60-ms timer — plenty fast for UX, but ~17x fewer JS calls than naive
per-line dispatch.

JS side (in logs.js) must expose `window._logBatch(list)` which iterates
and calls appendMainLog for each entry.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, List, Optional


Segment = List[Any] # [text, tag?]
SegmentList = List[Segment] # one log line


# Tags we want to filter out in Simple mode. These are the "noisy"
# diagnostic lines (dim hints, yt-dlp debug chatter, per-step details).
# Matches YTArchiver's Simple mode filter at ~line 17776.
VERBOSE_ONLY_TAGS = frozenset({
    "dim",
    "filterskip_dim",
    "dlprogress",
    "dlprogress_pct",
    "metadata_using",
    # Simple mode only shows the three green startup milestones
    # ("Disk scan complete", "Browse tab preload complete", "Startup
    # checks complete, ready to download"). The per-channel "Loading\u2026
    # (18/103)" tick line and the yt-dlp download-percent lines are all
    # verbose-only telemetry.
    "startup_loading",
})


def _line_is_verbose_only(segments: SegmentList) -> bool:
    """Return True if every content segment's PRIMARY tag is verbose-only.

    A segment's tag can be a string, a list/tuple of strings (e.g. a
    visual tag plus an identity marker like "sync_row_5" used for in-
    place replacement), or None. Only the FIRST tag in the list drives
    visual styling — the rest are DOM markers that shouldn't influence
    simple-vs-verbose filtering. None-tagged content always shows in
    both modes.
    """
    if not segments:
        return False
    saw_content = False
    for seg in segments:
        if not isinstance(seg, (list, tuple)) or len(seg) < 2:
            return False
        text, tag = seg[0], seg[1]
        # Lines with only "\n" or empty text don't contribute
        if text in (None, "", "\n"):
            continue
        saw_content = True
        if tag is None:
            return False
        primary = tag[0] if isinstance(tag, (list, tuple)) and tag else tag
        if primary not in VERBOSE_ONLY_TAGS:
            return False
    return saw_content


class LogStreamer:
    """Batching log pipe from worker threads to the webview."""

    BATCH_INTERVAL_SEC = 0.06
    MAX_BATCH_SIZE = 200

    def __init__(self, window=None):
        self._window = window
        self._buffer: List[SegmentList] = []
        self._buffer_activity: List[dict] = []
        self._lock = threading.Lock()
        self._flush_timer: Optional[threading.Timer] = None
        self._last_flush = 0.0
        # Latest in-place line for whisper/encode progress replacement.
        # Key = tag that identifies the line type; value = the target line
        # ID we send to JS to replace.
        self._inplace_line_ids: dict = {}
        # When True (Simple mode), dim/verbose-only lines are filtered out.
        # Tags listed in VERBOSE_ONLY_TAGS are skipped in simple mode.
        self.simple_mode: bool = True
        # Optional line-by-line scanners (e.g. disk-error watchdog). Each
        # callable receives the concatenated text of one emitted line.
        self._line_scanners: list = []

    def set_window(self, window):
        self._window = window

    def add_line_scanner(self, fn):
        """Register a callback(text: str) invoked once per emitted line.
        Used by the disk-error watchdog; keep scanners fast and non-blocking.
        """
        if callable(fn):
            self._line_scanners.append(fn)

    def _run_line_scanners(self, segments: SegmentList):
        if not self._line_scanners:
            return
        try:
            text = "".join(str(seg[0] or "") for seg in segments if seg)
        except Exception:
            return
        for fn in list(self._line_scanners):
            try: fn(text)
            except Exception: pass

    # ── main log ──

    def emit(self, segments: SegmentList):
        """Append one line of segments to the main log."""
        if not segments:
            return
        # Simple-mode filter — drop pure-verbose lines
        if self.simple_mode and _line_is_verbose_only(segments):
            return
        # Feed the disk-error watchdog (and any other scanners) before we
        # buffer — scanners may need to react before the line renders.
        self._run_line_scanners(segments)
        with self._lock:
            self._buffer.append(segments)
            if len(self._buffer) >= self.MAX_BATCH_SIZE:
                self._flush_now_locked()
                return
        self._schedule_flush()

    def emit_text(self, text: str, tag: Optional[str] = None):
        """Convenience: emit one plain-text line with optional tag."""
        line = text if text.endswith("\n") else text + "\n"
        self.emit([[line, tag]])

    def emit_simple(self, text: str):
        self.emit_text(text, "simpleline")

    def emit_dim(self, text: str):
        self.emit_text(text, "dim")

    def emit_error(self, text: str):
        self.emit_text(text, "red")

    def emit_header(self, text: str):
        self.emit_text(text, "header")

    # ── activity log ──

    def emit_activity(self, cells: dict, alt: bool = False):
        """Append one structured activity-log entry (9-column grid row)."""
        with self._lock:
            self._buffer_activity.append({"cells": cells, "alt": alt})
        self._schedule_flush()

    # ── batching ──

    def _schedule_flush(self):
        if self._flush_timer is not None:
            return
        t = threading.Timer(self.BATCH_INTERVAL_SEC, self._flush)
        t.daemon = True
        t.start()
        self._flush_timer = t

    def _flush(self):
        with self._lock:
            self._flush_timer = None
            self._flush_now_locked()

    def _flush_now_locked(self):
        """Must be called with self._lock held."""
        main_batch = self._buffer
        act_batch = self._buffer_activity
        self._buffer = []
        self._buffer_activity = []
        self._last_flush = time.time()
        if not main_batch and not act_batch:
            return
        if self._window is None:
            return
        try:
            payload = {"main": main_batch, "activity": act_batch}
            # Escape closing </script> in JSON before injection
            js_payload = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
            self._window.evaluate_js(f"window._logBatch({js_payload})")
        except Exception:
            # Window may be gone; drop silently so we don't deadlock workers
            pass

    # ── helpers ──

    def flush(self):
        """Force an immediate flush. Call before shutting down the window."""
        with self._lock:
            if self._flush_timer is not None:
                self._flush_timer.cancel()
                self._flush_timer = None
            self._flush_now_locked()
