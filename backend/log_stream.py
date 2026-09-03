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
from collections import deque
from typing import Any

from .log import get_logger

_log = get_logger(__name__)


Segment = list[Any] # [text, tag?]
SegmentList = list[Segment] # one log line


# Tags we want to filter out in Simple mode. These are the "noisy"
# diagnostic lines (dim hints, yt-dlp debug chatter, per-step details).
# Matches YTArchiver's Simple mode filter at ~line 17776.
VERBOSE_ONLY_TAGS = frozenset({
    "dim",
    # Python logger ERROR/CRITICAL records are diagnostic console text. Keep
    # them available (and red) in Verbose mode without exposing module names,
    # SQL constraints, or absolute archive paths in Simple mode.
    "internal_error",
    "internal_warning",
    # audit LG-7: `filterskip` lines (e.g. "[Skip] — short video
    # filtered (< 60s): ...") were leaking into Simple mode because
    # only the secondary `filterskip_dim` tag was in this set.
    # Primary `filterskip` needs to be here too so `_line_is_verbose_only`
    # classifies the whole line as verbose-only.
    "filterskip",
    "filterskip_dim",
    "dlprogress",
    "dlprogress_pct",
    "metadata_using",
    # "Transcribing — Loading Whisper model (small) on GPU..." uses
    # this tag. It's a one-time subprocess-spawn diagnostic that would
    # otherwise land at whatever scroll position the log happens to
    # be at when Whisper starts up — typically a later channel's "no
    # new videos" row, way below the video that actually queued the
    # transcribe job. Hide in Simple mode so the user only sees the
    # `tx_done_<vid>` placeholder and its eventual ✓ replacement.
    "transcribe_using",
    # Simple mode only shows the three green startup milestones
    # ("Disk scan complete", "Startup
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
        # whitespace-only segments (leading " ", padding
        # " \u2014 ") shouldn't flip the whole line to verbose-only
        # purely because their tag is None or non-verbose. These
        # are layout glue, not content. Skip them in the decision
        # the same way empty text is skipped.
        if text.strip() == "":
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
    MAX_SEGMENT_TEXT_CHARS = 8192
    MAX_RETRY_BATCHES = 4
    MAX_RETRY_ITEMS = MAX_BATCH_SIZE * MAX_RETRY_BATCHES
    MAX_RETRY_ATTEMPTS = 3
    RETRY_BASE_SEC = 0.12
    _TRUNC_SUFFIX = "... [truncated]"

    def __init__(self, window=None):
        self._window = window
        self._ready = False
        self._buffer: list[SegmentList] = []
        self._buffer_activity: list[dict] = []
        # staging buffers populated by _flush_now_locked
        # under self._lock; _do_flush reads them from here AFTER the
        # lock is released so evaluate_js doesn't stall workers.
        self._pending_main: list[SegmentList] = []
        self._pending_act: list[dict] = []
        self._lock = threading.Lock()
        self._flush_timer: threading.Timer | None = None
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
        # Semantic-tag scanners avoid brittle text matching for exact events
        # such as a confirmed completed download. Mapping: tag -> callbacks.
        self._tag_scanners: dict[str, list] = {}
        # Consecutive evaluate_js drop counter — declared here so it isn't
        # lazily materialized on the error path (see _do_flush).
        self._drop_count = 0
        # A bridge call can fail transiently while the webview is busy.  Keep
        # a strictly bounded set of swapped batches so those messages are not
        # lost merely because they were already removed from _buffer.
        self._retry_batches = deque()
        self._retry_items = 0
        self._retry_timer: threading.Timer | None = None
        self._retry_dropped = 0
        # Exactly one evaluate_js delivery may be in flight. Fresh batches
        # queue behind it so a failed older batch can never render after a
        # newer success and regress an in-place progress/control row.
        self._bridge_busy = False

    def set_window(self, window):
        self._window = window
        # Do not flush here. pywebview.create_window() returns before
        # webview.start() has a live JS bridge; evaluate_js in that gap
        # can block startup for ~20s. mark_ready() flushes after the
        # frontend is running.

    def mark_ready(self):
        """Mark the JS bridge ready and flush buffered startup messages."""
        self._ready = True
        try:
            self.flush()
        except Exception as e:
            _log.debug("swallowed: %s", e)

    def add_line_scanner(self, fn):
        """Register a callback(text: str) invoked once per emitted line.
        Used by the disk-error watchdog; keep scanners fast and non-blocking.
        """
        if callable(fn):
            self._line_scanners.append(fn)

    def add_tag_scanner(self, tag: str, fn) -> None:
        """Invoke ``fn(text)`` when an emitted line carries ``tag``."""
        marker = str(tag or "").strip()
        if marker and callable(fn):
            self._tag_scanners.setdefault(marker, []).append(fn)

    def _run_line_scanners(self, segments: SegmentList):
        if not self._line_scanners and not self._tag_scanners:
            return
        try:
            text = "".join(str(seg[0] or "") for seg in segments if seg)
        except Exception:
            return
        for fn in list(self._line_scanners):
            try: fn(text)
            except Exception as e: _log.debug("swallowed: %s", e)
        if not self._tag_scanners:
            return
        markers: set[str] = set()
        for seg in segments:
            if not isinstance(seg, (list, tuple)) or len(seg) < 2:
                continue
            tag = seg[1]
            if isinstance(tag, (list, tuple, set)):
                markers.update(str(item) for item in tag if item)
            elif tag:
                markers.add(str(tag))
        for marker in markers:
            for fn in list(self._tag_scanners.get(marker, ())):
                try: fn(text)
                except Exception as e: _log.debug("swallowed: %s", e)

    # ── main log ──

    def _clamp_segments(self, segments: SegmentList) -> SegmentList:
        out = []
        for seg in segments:
            if not isinstance(seg, (list, tuple)) or not seg:
                out.append(seg)
                continue
            text = str(seg[0] or "")
            if len(text) > self.MAX_SEGMENT_TEXT_CHARS:
                had_newline = text.endswith("\n")
                keep = max(
                    0,
                    self.MAX_SEGMENT_TEXT_CHARS
                    - len(self._TRUNC_SUFFIX)
                    - (1 if had_newline else 0),
                )
                text = text[:keep] + self._TRUNC_SUFFIX
                if had_newline:
                    text += "\n"
            clipped = list(seg)
            clipped[0] = text
            out.append(clipped)
        return out

    def emit(self, segments: SegmentList):
        """Append one line of segments to the main log."""
        if not segments:
            return
        segments = self._clamp_segments(segments)
        # Simple-mode filter — drop pure-verbose lines
        if self.simple_mode and _line_is_verbose_only(segments):
            return
        # Feed the disk-error watchdog (and any other scanners) before we
        # buffer — scanners may need to react before the line renders.
        self._run_line_scanners(segments)
        _fire_now = False
        with self._lock:
            self._buffer.append(segments)
            if (self._ready and self._window is not None
                    and len(self._buffer) >= self.MAX_BATCH_SIZE):
                # swap buffers under lock, fire JS bridge
                # call outside the lock. Also cancel any scheduled
                # timer so we don't double-fire.
                if self._flush_timer is not None:
                    try: self._flush_timer.cancel()
                    except Exception as e: _log.debug("swallowed: %s", e)
                    self._flush_timer = None
                self._flush_now_locked()
                _main = self._pending_main
                _act = self._pending_act
                self._pending_main = []
                self._pending_act = []
                _fire_now = True
        if _fire_now:
            self._do_flush(_main, _act)
            return
        self._schedule_flush()

    def emit_text(self, text: str, tag: str | None = None):
        """Convenience: emit one plain-text line with optional tag."""
        line = text if text.endswith("\n") else text + "\n"
        self.emit([[line, tag]])

    def emit_simple(self, text: str):
        self.emit_text(text, "simpleline")

    def emit_dim(self, text: str):
        self.emit_text(text, "dim")

    def emit_error(self, text: str, *, detail: str | None = None):
        # `error_detail` is a semantic marker consumed by the session-error
        # popover. A red segment alone is not enough: pass summaries color
        # their numeric error count red too, which previously made the popup
        # collect several vague summary rows for one real failure.
        line = text if text.endswith("\n") else text + "\n"
        segments = [[line, ["red", "error_detail"]]]
        if detail:
            # Hidden in the main log, but retained in the DOM so the
            # session-error popover can expose raw diagnostics on hover.
            segments.append([
                str(detail).strip()[:self.MAX_SEGMENT_TEXT_CHARS],
                "error_raw",
            ])
        self.emit(segments)

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
        # check + set _flush_timer inside the lock so two
        # concurrent emit() calls can't each create a Timer, leaving
        # orphan timers that fire into a closed window. Previously the
        # `if _flush_timer is not None: return` check happened outside
        # any lock, racing with the timer's own self-clear.
        timer = None
        with self._lock:
            if not self._ready or self._window is None:
                return
            if self._flush_timer is not None:
                return
            timer = threading.Timer(self.BATCH_INTERVAL_SEC, self._flush)
            timer.daemon = True
            # Publish before start: a zero-delay timer can run immediately
            # and clear this field. Starting while holding _lock also
            # deadlocks Timer doubles that invoke the callback synchronously.
            self._flush_timer = timer
        try:
            timer.start()
        except Exception as exc:
            main_batch = []
            act_batch = []
            with self._lock:
                if self._flush_timer is timer:
                    self._flush_timer = None
                    # Timer creation failed, so no callback exists to consume
                    # the retained buffers. Swap them atomically here and use
                    # the normal bridge/retry path outside the lock. This also
                    # avoids recursive scheduling and synchronous-Timer
                    # deadlocks.
                    self._flush_now_locked()
                    main_batch = self._pending_main
                    act_batch = self._pending_act
                    self._pending_main = []
                    self._pending_act = []
            _log.warning("could not start log flush timer: %s", exc)
            if main_batch or act_batch:
                self._do_flush(main_batch, act_batch)

    def _flush(self):
        # DO NOT hold self._lock while evaluate_js is
        # running — pywebview's JS bridge can block for seconds under
        # load (devtools attached, huge payload, GC pause), and every
        # worker emit() stalls behind it while the lock is held. Swap
        # the buffers inside the lock, release, then evaluate_js.
        with self._lock:
            self._flush_timer = None
            main_batch = self._buffer
            act_batch = self._buffer_activity
            self._buffer = []
            self._buffer_activity = []
            self._last_flush = time.time()
        self._do_flush(main_batch, act_batch)

    def _flush_now_locked(self):
        """Must be called with self._lock held. Swaps buffers out under
        the caller's lock, but the evaluate_js call itself runs after
        this method returns (see _flush + flush) so the lock doesn't
        block on the JS bridge. Kept named *_locked because callers
        expect it to be lock-safe to invoke."""
        main_batch = self._buffer
        act_batch = self._buffer_activity
        self._buffer = []
        self._buffer_activity = []
        self._last_flush = time.time()
        # Stash for the caller to flush outside the lock. Writing these
        # two attrs is safe under the lock; the JS bridge call is not.
        self._pending_main = main_batch
        self._pending_act = act_batch

    def _enqueue_retry_locked(self, main_batch, act_batch, attempt: int,
                              *, front: bool = False) -> bool:
        """Queue one delivery while ``self._lock`` is held.

        ``front`` is used when the current oldest delivery fails. Fresh output
        may have queued while its bridge call was in flight, so the failed
        batch must go back ahead of that newer output to preserve FIFO.
        """
        main = list(main_batch or [])
        activity = list(act_batch or [])
        item_count = len(main) + len(activity)
        if not item_count:
            return False
        if attempt > self.MAX_RETRY_ATTEMPTS:
            self._retry_dropped += item_count
            return False

        # One pathological activity batch must not defeat the queue bound.
        if item_count > self.MAX_RETRY_ITEMS:
            overflow = item_count - self.MAX_RETRY_ITEMS
            if overflow < len(main):
                main = main[overflow:]
            else:
                activity = activity[overflow - len(main):]
                main = []
            self._retry_dropped += overflow
            item_count = len(main) + len(activity)

        # Preserve the newest useful queued output under sustained failure
        # while retaining a hard memory ceiling. A failed in-flight batch is
        # inserted at the front after making room, so it remains older than
        # every surviving fresh batch.
        while (self._retry_batches
               and (len(self._retry_batches) >= self.MAX_RETRY_BATCHES
                    or self._retry_items + item_count
                    > self.MAX_RETRY_ITEMS)):
            old_main, old_act, _old_attempt = self._retry_batches.popleft()
            removed = len(old_main) + len(old_act)
            self._retry_items -= removed
            self._retry_dropped += removed
        entry = (main, activity, attempt)
        if front:
            self._retry_batches.appendleft(entry)
        else:
            self._retry_batches.append((main, activity, attempt))
        self._retry_items += item_count
        return True

    def _schedule_retry_locked(self) -> None:
        if (self._retry_timer is not None or not self._retry_batches
                or self._bridge_busy or not self._ready
                or self._window is None):
            return
        # The head owns delivery order and therefore its own attempt budget.
        head_attempt = self._retry_batches[0][2]
        delay = (0.0 if head_attempt <= 0 else min(
            self.RETRY_BASE_SEC * (2 ** (head_attempt - 1)), 1.0))
        timer = threading.Timer(delay, self._retry_flush)
        timer.daemon = True
        # Publish before start: a zero-delay timer can run immediately and
        # clear this field. Assigning afterward can strand a completed Timer
        # here forever and suppress every future retry.
        self._retry_timer = timer
        try:
            timer.start()
        except Exception:
            if self._retry_timer is timer:
                self._retry_timer = None
            # Called only while ``self._lock`` is held. Logging through the
            # installed LogStreamerHandler here would call this same stream's
            # emit(), try to reacquire the non-reentrant lock, and deadlock.
            # Keep the payload queued; a later ordinary flush can retry it.

    def _queue_retry(self, main_batch, act_batch, attempt: int,
                     *, front: bool = False) -> None:
        with self._lock:
            queued = self._enqueue_retry_locked(
                main_batch, act_batch, attempt, front=front)
            if queued:
                self._schedule_retry_locked()

    def _take_retry_locked(self):
        if self._bridge_busy or not self._retry_batches:
            return None
        main_batch, act_batch, attempt = self._retry_batches.popleft()
        self._retry_items -= len(main_batch) + len(act_batch)
        self._bridge_busy = True
        return main_batch, act_batch, attempt

    def _retry_flush(self) -> None:
        with self._lock:
            self._retry_timer = None
            delivery = self._take_retry_locked()
        if delivery is not None:
            self._deliver_batch(*delivery)

    def _do_flush(self, main_batch, act_batch, retry_attempt: int = 0):
        if not main_batch and not act_batch:
            return
        with self._lock:
            if self._bridge_busy or self._retry_batches:
                queued = self._enqueue_retry_locked(
                    main_batch, act_batch, retry_attempt)
                if queued:
                    self._schedule_retry_locked()
                return
            self._bridge_busy = True
        self._deliver_batch(main_batch, act_batch, retry_attempt)

    def _deliver_batch(self, main_batch, act_batch,
                       retry_attempt: int = 0) -> None:
        # Snapshot the window reference once: it's set/cleared from other
        # threads (set_window / shutdown), so re-reading self._window after
        # the None-check could see it flip mid-flush.
        win = self._window
        if win is None or not self._ready:
            with self._lock:
                self._bridge_busy = False
                self._enqueue_retry_locked(
                    main_batch, act_batch, retry_attempt, front=True)
            return
        warning_count = 0
        try:
            payload = {"main": main_batch, "activity": act_batch}
            # Escape closing </script> in JSON before injection
            js_payload = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
            win.evaluate_js(f"window._logBatch({js_payload})")
        except Exception:
            # Window may be temporarily busy or closing. Retry this swapped
            # batch without blocking workers; the queue and attempt count are
            # both bounded so a dead bridge cannot grow memory indefinitely.
            with self._lock:
                self._bridge_busy = False
                self._enqueue_retry_locked(
                    main_batch, act_batch, retry_attempt + 1, front=True)
                # Track consecutive failures so a real problem
                # (oversized payload, devtools paused, evaluate_js exception
                # storm) surfaces instead of disappearing entirely.
                self._drop_count += 1
                warning_count = self._drop_count
                self._schedule_retry_locked()
        else:
            with self._lock:
                self._bridge_busy = False
                self._drop_count = 0
                self._schedule_retry_locked()
        if warning_count in (10, 100, 1000):
            try:
                import logging as _lg
                _lg.getLogger(__name__).warning(
                    "log_stream: %d consecutive evaluate_js drops "
                    "(window may be unresponsive)", warning_count)
            except Exception:
                pass

    # ── helpers ──

    def flush(self):
        """Force an immediate flush. Call before shutting down the window.

        performs the JS-bridge call OUTSIDE the lock so a
        slow evaluate_js doesn't stall other emit() callers queueing
        up behind it.

        Patch C: bounded lock-acquire to defend against a window-close
        deadlock. If a worker thread holds _lock inside emit() while
        the main thread calls flush() during shutdown, the main thread
        used to wait forever. Now: 5s timeout, then bail. The flush is
        best-effort — losing the very last batch on close is acceptable;
        hanging the app on close is not.
        """
        if not self._lock.acquire(timeout=5.0):
            return
        delivery = None
        try:
            if self._flush_timer is not None:
                self._flush_timer.cancel()
                self._flush_timer = None
            if self._retry_timer is not None:
                self._retry_timer.cancel()
                self._retry_timer = None
            self._flush_now_locked()
            _main = getattr(self, "_pending_main", [])
            _act = getattr(self, "_pending_act", [])
            self._pending_main = []
            self._pending_act = []
            if _main or _act:
                self._enqueue_retry_locked(_main, _act, 0)
            if self._ready and self._window is not None:
                delivery = self._take_retry_locked()
                if delivery is None:
                    self._schedule_retry_locked()
        finally:
            self._lock.release()
        # Deliver only the oldest batch. Its completion schedules the next
        # queued batch, so a concurrent fresh flush cannot overtake a retry.
        if delivery is not None:
            self._deliver_batch(*delivery)
