"""
sync.sync_all — multi-channel sync orchestrator.

Extracted from sync/core.py (Patch 14, v71.6).

`sync_all` is the top-level entry point the worker thread invokes when
the user hits Sync Subbed (or when an autorun pass fires, or when a
queue-only worker is started just to drain queued metadata tasks). It
walks the persistent sync queue, calls `sync_channel` for each item,
and emits the start-of-pass / end-of-pass log decoration.

Public surface (re-exported via sync/__init__.py for back-compat):
    sync_all(stream, cancel_event=None, only_with_new=True,
             queues=None, transcribe_mgr=None,
             pause_event=None, skip_event=None,
             add_downloads_from_config=True) -> dict
"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime
from typing import Any

from ..log import get_logger, swallow
from ..log_stream import LogStreamer
from ..ytarchiver_config import ARCHIVE_FILE, config_transaction, load_config

# sync_channel + the module-level cookie-alert flag live in core.py.
# The flag is reset at pass start; mutate it via the imported module
# object so the change is visible inside sync_channel.
from . import core as _core

# Helpers used by sync_all
from .active_state import fire_channel_synced_hook, fire_metadata_changed_hook
from .core import sync_channel
from .display_push import clear_sync_progress
from .log_rows import (
    _ROW_EMIT_PASS_ID,
    _bracket_segments,
    _new_pass_id,
    _short_summary,
    _sync_row_emit,
    emit_metadata_activity_row,
)
from .quickcheck import (
    _check_batch_cooldown,
    _should_batch_limit,
    quick_check_new_uploads,
    set_batch_cooldown,
)
from .sync_helpers import _fmt_duration

_log = get_logger(__name__)


def sync_all(stream: LogStreamer, cancel_event: threading.Event | None = None,
             only_with_new: bool = True, queues=None, transcribe_mgr=None,
             pause_event: threading.Event | None = None,
             skip_event: threading.Event | None = None,
             add_downloads_from_config: bool = True) -> dict[str, Any]:
    """
    Sync every channel in config["channels"] sequentially.

    `pause_event`: while set, the loop blocks between channels (~0.5s poll).
    `skip_event`: if set mid-channel, the cancel_event is fired to kill the
                   current yt-dlp subprocess; the outer loop then clears both
                   events and advances to the next channel. Total cancellation
                   still works via cancel_event directly.
    """
    cfg = load_config()
    channels = cfg.get("channels", [])
    if not channels:
        stream.emit(_bracket_segments("Sync") +
                    [["No channels subscribed.\n", "simpleline"]])
        # Clear any leftover sync_progress file from a prior pass
        # — the finalizer at end-of-function won't run when we
        # early-return here, leaving the companion display showing
        # stale "in progress" state (audit: sync_all L25).
        try: clear_sync_progress()
        except Exception as e: swallow("progress reset on early exit", e)
        return {"ok": False, "reason": "no channels", "total": 0}

    # ENQUEUE DECISION:
    # - `add_downloads_from_config=True` (Sync Subbed) AND no
    # download tasks queued yet → add every subscribed channel as
    # a download task. `sync_enqueue` dedupes on (kind, url) so
    # pre-existing metadata tasks stay intact; downloads append
    # alongside.
    # - `add_downloads_from_config=False` (worker started just to
    # drain the queue, e.g. from `metadata_queue_all`) → never
    # touch the queue. Process whatever is there and stop.
    # - `add_downloads_from_config=True` BUT download tasks already
    # exist (paused-then-resumed Sync Subbed) → resume mode, keep
    # the existing queue as-is.
    # a user hit bug where queuing 103 metadata tasks auto-fired the
    # worker via sync_start_all, which in turn added 103 downloads —
    # "Sync pass starting (206 channels)" when he only asked for 103
    # metadata checks.
    _resume_mode = False
    if queues is not None:
        try:
            _sync_snapshot = queues.sync_snapshot()
            existing_dl = any(
                (c.get("kind") or "download").lower() == "download"
                for c in _sync_snapshot
            )
            if existing_dl:
                _resume_mode = True
            elif add_downloads_from_config:
                for _ch in channels:
                    queues.sync_enqueue(_ch)
            # else: worker was started just to drain the queue — do
            # not touch it.
        except Exception as e:
            swallow("queue snapshot / enqueue", e)

    # Per-pass unique id — stashed on a thread-local that
    # `_sync_row_emit` reads by default, so every call site inside the
    # sync loop picks it up without having to pass it explicitly.
    # Autorun-fired second passes were silently replacing the first
    # pass's rows in-place (far above the current scroll) because
    # `sync_row_1` collided across passes; a fresh id per pass fixes
    # it cleanly. Cleared in the `finally` at the bottom of this func.
    _ROW_EMIT_PASS_ID.id = _new_pass_id()

    # Reset the cookie-sign-out alert flag so this pass can emit the
    # red banner once if yt-dlp surfaces a sign-in / cookie-extract
    # error. Without resetting, a fix-then-resync wouldn't show the
    # all-clear path — the flag stays True from the prior pass.
    # The flag lives in core.py (sync_channel reads it). Mutate via the
    # imported module so the change is visible across modules — a plain
    # `global` here wouldn't reach core.py's module namespace.
    # Hold the same _cookie_alert_lock that sync_channel uses for
    # mutating the flag, so an autorun-triggered second pass overlapping
    # the tail of the prior pass can't read a torn flag value (audit:
    # sync/core.py:919).
    with _core._cookie_alert_lock:
        _core._COOKIE_ALERT_FIRED = False

    # Start-of-pass header — show total remaining work, not len(config).
    # In resume mode that's the restored queue size; fresh mode it's the
    # whole channel list we just enqueued. Exclude kind=redownload
    # entries — those are handled by Api._redwnl_worker and appear in
    # the queue only for popover visibility; counting them in "Sync
    # pass starting (N channels)" would over-report.
    try:
        _queue_snapshot = [
            c for c in (queues.sync_snapshot() if queues is not None else [])
            if (c.get("kind") or "download").lower() != "redownload"
        ]
        _starting_total = len(_queue_snapshot) if queues is not None else len(channels)
    except Exception:
        _queue_snapshot = []
        _starting_total = len(channels)

    # Label the banner with the actual action the queue represents
    # instead of the generic "Sync pass". A queue of views-refresh items
    # now says "Views/likes refresh starting" so the user isn't
    # second-guessing what the pass is doing. Mixed kinds fall back to
    # "Sync pass".
    def _pass_label(items):
        if not items:
            return "Sync pass"
        kinds = {(c.get("kind") or "download").lower() for c in items}
        if kinds == {"download"}:
            return "Sync pass"
        if kinds == {"metadata_comments"}:
            return "Comments refresh"
        if kinds == {"videoid_backfill"}:
            return "Video ID backfill"
        if kinds == {"repair_yt_captions"}:
            return "Repair YT auto-captions"
        if kinds == {"punct_restore"}:
            return "Restore transcript punctuation"
        if kinds == {"metadata"}:
            # All refresh=True → views/likes refresh; all refresh=False →
            # metadata download; mixed → generic metadata pass.
            refreshes = {bool(c.get("refresh")) for c in items}
            if refreshes == {True}:
                return "Views/likes refresh"
            if refreshes == {False}:
                return "Metadata download"
            return "Metadata pass"
        return "Sync pass"  # mixed kinds

    _label = _pass_label(_queue_snapshot if queues is not None else channels)
    # Suffix word follows the work unit. Most kinds run per-channel, but
    # the archive-wide repair tools are a single task — "1 channels" is
    # both ungrammatical and misleading. Match plural form to count too.
    _kinds_in_queue = {(c.get("kind") or "download").lower()
                       for c in (_queue_snapshot
                                 if queues is not None else channels)}
    _archive_wide_kinds = {"repair_yt_captions", "punct_restore"}
    if _kinds_in_queue and _kinds_in_queue.issubset(_archive_wide_kinds):
        _unit = "task" if _starting_total == 1 else "tasks"
    else:
        _unit = "channel" if _starting_total == 1 else "channels"
    if _resume_mode:
        stream.emit([[f"=== Resuming {_label.lower()} ", "header"],
                     [f"({_starting_total} {_unit} remaining) ===\n",
                      "header"]])
    else:
        stream.emit([[f"=== {_label} starting ", "header"],
                     [f"({_starting_total} {_unit}) ===\n", "header"]])

    sum_dl = 0
    sum_err = 0
    skipped = 0
    # Per-kind accumulators so the Pass-complete line can say the
    # verb that actually happened — "X refreshed" for a views/likes
    # pass, "X comments refreshed" for a comments pass, "X IDs
    # backfilled" for a video_id backfill, etc. Before, only sum_dl
    # / sum_err tracked anything, so a refresh pass always read as
    # "0 downloaded" even when 900 videos had their counts updated.
    sum_meta_refreshed = 0    # bulk_refresh_views_likes `refreshed`
    sum_meta_fetched = 0      # new metadata entries (first-time fetch)
    sum_comments_refreshed = 0
    sum_ids_backfilled = 0
    t_start = time.time()

    # Load the global download-archive ONCE into a set for O(1) membership
    # tests. This backs the per-channel "quick check" fast path below, which
    # probes the first 5 videos of each channel and short-circuits the full
    # yt-dlp walk when everything is already archived. Mirrors the OLD
    # YTArchiver _load_archived_ids + _quick_check_new_uploads pairing.
    _known_ids: set = set()
    _archive_malformed = 0
    # YouTube video IDs are exactly 11 chars from [A-Za-z0-9_-]. We
    # `errors="replace"` to survive a torn line, but a U+FFFD inside
    # the id field would land an unmatchable string in `_known_ids`,
    # defeating quick_check_new_uploads. Validate after split so both
    # torn-line AND encoding-corruption cases get counted as malformed
    # (audit: sync_all.py H34).
    import re as _re_id
    _VID_RE = _re_id.compile(r'^[A-Za-z0-9_-]{11}$')
    try:
        if os.path.isfile(ARCHIVE_FILE):
            with open(ARCHIVE_FILE, "r", encoding="utf-8", errors="replace") as _af:
                for _line in _af:
                    # Format: "youtube VIDEOID\n" — split and keep the id
                    _stripped = _line.strip()
                    if not _stripped:
                        continue
                    _parts = _stripped.split(None, 1)
                    if len(_parts) == 2 and _VID_RE.match(_parts[1]):
                        _known_ids.add(_parts[1])
                    else:
                        # Non-empty line that didn't parse — count and
                        # log a single warn at the end. Without this,
                        # a corrupted archive line silently shrinks
                        # _known_ids, defeating quick_check_new_uploads
                        # and forcing full channel walks (audit:
                        # sync/sync_all.py:218). Counting individual
                        # lines is cheap; we cap the per-line log to
                        # a single summary so a deeply-corrupt archive
                        # doesn't flood the main log.
                        _archive_malformed += 1
    except OSError:
        pass
    if _archive_malformed:
        try:
            stream.emit_dim(
                f"[Sync] archive: {_archive_malformed} malformed line(s) "
                f"skipped (consider regenerating via diagnostics).")
        except Exception:
            pass

    def _now_clock() -> str:
        # "1:03am" style, matching OLD's log format.
        now = datetime.now()
        return now.strftime("%-I:%M%p") if os.name != "nt" \
               else now.strftime("%I:%M%p").lstrip("0")

    # Mid-pass pause state. `_last_live` tracks the row we most recently
    # painted as live so we can re-paint it as PAUSED: Name when the user
    # pauses between channels, then back to live when they Resume.
    # `total` is dynamic (processed + remaining) and gets updated on each
    # pop — matches YTArchiver.py:19138 `current_total = processed + 1 +
    # len(_sync_queue)`. Initial value is the starting queue size.
    _last_live = {"i": 0, "total": _starting_total, "name": ""}

    def _wait_if_paused():
        """If pause_event is set, log pause + wait until resumed.
        Re-paints the last live row as PAUSED and back. Idempotent."""
        if pause_event is None or not pause_event.is_set():
            return
        # Re-paint the last live row (if any) in paused style.
        if _last_live["name"]:
            _sync_row_emit(stream,
                           _last_live["i"], _last_live["total"],
                           f"PAUSED: {_last_live['name']}",
                           name_tag="simpleline", summary_tag="dim")
        stream.emit([
            ["\u23F8 Sync paused at ", "simpleline"],
            [_now_clock().lower(), "simpleline"],
            [" \u2014 click Resume.\n", "dim"],
        ])
        # Tell the UI the pause is now ACTUALLY in effect (vs just
        # requested). Frontend stops blinking the Resume button.
        # Also ensure sync_paused=True is visible to the frontend so
        # the Resume button appears when an auto-pause (e.g. cookie
        # alert) set pause_event without going through queue_pause().
        if queues is not None:
            try:
                if not queues.sync_paused:
                    queues.set_sync_paused(True)
            except Exception as e: swallow("queue pause flag", e)
            try: queues.set_sync_paused_active(True)
            except Exception as e: swallow("queue pause-active flag", e)
        from ..pause_helpers import wait_for_resume
        # queue-flag flipping + wait loop delegated to
        # pause_helpers.wait_for_resume; row-repaint stays here.
        # Loop the wait so a re-pause that lands in the gap between
        # wait_for_resume returning and us clearing the flag re-enters
        # the wait instead of falling through (audit: sync_all H30).
        cancelled = False
        while True:
            cancelled = wait_for_resume(pause_event, cancel_event, tick=0.25)
            if cancelled:
                break
            # Re-check pause_event under no lock — best-effort. If it
            # got set again immediately after wait_for_resume returned,
            # loop and wait again. If it's clear, fall through to
            # clearing paused_active and emitting "Resumed".
            if pause_event.is_set():
                continue
            break
        # Always clear the active flag (resumed OR cancelled).
        if queues is not None:
            try: queues.set_sync_paused_active(False)
            except Exception as e: swallow("queue pause-active clear", e)
        if cancelled:
            return
        stream.emit([
            ["\u25B6 Sync resumed at ", "simpleline_green"],
            [_now_clock().lower(), "simpleline_green"],
            [".\n", "dim"],
        ])
        # Re-paint the last row back to live (without PAUSED prefix).
        if _last_live["name"]:
            _sync_row_emit(stream,
                           _last_live["i"], _last_live["total"],
                           _last_live["name"])

    # QUEUE-DRIVEN LOOP (ports YTArchiver.py:19130-19144 exactly).
    # Pop from queues.sync until it's empty. No config iteration —
    # that's the root-cause bug hit where a resumed half-pass
    # would restart from A because the loop walked config instead of
    # the queue. `_processed` counts what we've done in THIS invocation;
    # `total` stays stable at the INITIAL queue size to keep the row
    # denominator steady across pauses (bug L-2: old code recomputed
    # per iteration, so pausing+resuming made [3/7] drift to [3/4] as
    # remaining items drained — confusing.)
    _processed = 0
    # Snapshot the queue size through QueueState.sync_snapshot() so the
    # count we display in [N/total] matches what sync_pop will actually
    # deliver. Without a locked snapshot, an enqueue happening between
    # the count + the first pop made the denominator drift by
    # the number of newly-added items (audit: sync_all.py:294).
    try:
        if queues is not None:
            _initial_total = sum(
                1 for c in queues.sync_snapshot()
                if (c.get("kind") or "download").lower() != "redownload"
            )
        else:
            _initial_total = 0
    except Exception:
        _initial_total = 0
    # `total` must be bound before the loop: if the first pop returns
    # None (queue cleared/cancelled before we start) or the queue holds
    # only redownload items (which `continue` before assignment), the
    # return at the bottom would otherwise raise NameError and skip the
    # whole end-of-pass cleanup.
    total = _initial_total

    def _requeue_paused_task(ch: dict[str, Any], ch_name: str,
                             i: int, total: int, *,
                             reason: str,
                             bracket_tag=None) -> bool:
        """Handle a task that stopped because the sync queue was paused."""
        nonlocal _processed
        if (pause_event is None or not pause_event.is_set()
                or queues is None):
            return False
        try:
            queues.sync_requeue_front(ch)
        except Exception as e:
            swallow(f"{reason} pause requeue", e)
        _sync_row_emit(stream, i, total, ch_name,
                       summary="paused",
                       name_tag="simpleline",
                       summary_tag="simpleline",
                       bracket_tag=bracket_tag)
        _last_live["name"] = ""
        _processed -= 1
        return True

    def _emit_failed_metadata_task(i: int, total: int, ch_name: str,
                                   task_t0: float, *,
                                   bracket_tag=None) -> None:
        """Shared failed-row scaffold for the metadata-family tasks
        (metadata / comments / video-id backfill). Emits a red 'failed'
        sync row plus a failed metadata activity-log row. Callers still
        emit their own kind-specific stream.emit_error message first
        (T204 — collapses the verbatim-duplicated failed block)."""
        _sync_row_emit(stream, i, total, ch_name,
                       summary="failed",
                       name_tag="dim", summary_tag="red",
                       bracket_tag=bracket_tag)
        emit_metadata_activity_row(
            stream, ch_name,
            primary="failed", secondary="",
            errors=1, elapsed=time.time() - task_t0,
            green=False)

    while True:
        # Clear a stale skip flag left over from the previous channel.
        # sync_skip_current now only sets _sync_skip (no longer overloads
        # _sync_cancel), so we just need to drain the skip event here so
        # it doesn't mis-fire on the NEXT channel.
        if skip_event is not None and skip_event.is_set():
            skip_event.clear()
        # A mid-download pause requeues the in-flight channel at the front.
        # Wait before popping again so the paused row stays visible in Tasks.
        _wait_if_paused()
        if cancel_event is not None and cancel_event.is_set():
            stream.emit([["\n\u26d4 Pass cancelled.\n", "red"]])
            break
        # Pop next channel off the queue. When the queue is empty, we're
        # done — this is how the loop terminates, naturally supporting
        # both fresh passes (queue was fully enqueued above) and resume
        # (queue was restored from disk with a subset).
        ch = None
        if queues is not None:
            try:
                ch = queues.sync_pop()
            except Exception:
                ch = None
        if ch is None:
            break
        # Skip redownload tasks — they live in queues.sync only for
        # Sync Tasks popover visibility; Api._redwnl_worker drains a
        # separate `_redwnl_pending` list to actually run them (with
        # the right resolution, sample-confirm bridge, etc). Falling
        # through here would mis-process them as regular sync
        # downloads. reported symptom: "=== Sync pass starting (N
        # channels) === [1/N] ChannelName — no new videos" appearing
        # while a redownload of that channel was correctly running
        # in the popover — because both workers popped the same task.
        if (ch.get("kind") or "").lower() == "redownload":
            # Don't count this pop against `_processed` — the user
            # didn't ask sync_all to do anything with it, so the
            # "1/total" display should stay accurate to real syncs.
            continue
        _processed += 1
        i = _processed
        # use the INITIAL total captured above rather than
        # recomputing from `remaining + processed` each pass. The
        # denominator stays stable across pauses/resumes. Fall back to
        # the dynamic calc when initial was 0 (rare) so we still
        # display something sensible.
        try:
            _remaining = sum(
                1 for c in queues.sync_snapshot()
                if (c.get("kind") or "download").lower() != "redownload"
            ) if queues is not None else 0
        except Exception:
            _remaining = 0
        total = max(_initial_total, _processed + _remaining) \
                if _initial_total else (_processed + _remaining)
        # Honor pause request before we start this channel — if the user
        # paused mid-pass, we park here and re-paint the last-live row
        # as PAUSED. Matches OLD's pause-at-top-of-channel behavior.
        _wait_if_paused()
        if cancel_event is not None and cancel_event.is_set():
            # skip_event no longer overloads cancel_event, so this is
            # always a genuine user cancel \u2014 break unconditionally.
            stream.emit([["\n\u26d4 Pass cancelled.\n", "red"]])
            # Requeue the just-popped (never-run) item \u2014 sync_pop
            # removed it BEFORE this cancel check, and the end-of-pass
            # block deliberately preserves the rest of the queue for
            # Resume; without this, every mid-pass cancel silently
            # lost exactly one pending task.
            try:
                if queues is not None and ch is not None:
                    queues.sync_requeue_front(ch)
            except Exception:
                pass
            break
        # Batch cooldown check — skip channels still cooling down from a
        # bootstrap batch (>100k videos, not yet init_complete).
        can_proceed, cooldown_label = _check_batch_cooldown(ch)
        if not can_proceed:
            skipped += 1
            _sync_row_emit(stream, i, total, ch.get("name", "?"),
                           summary=f"cooldown until {cooldown_label}",
                           name_tag="dim", summary_tag="dim")
            continue
        # Kind dispatch. Download items (the default / no `kind` key)
        # take the full sync_channel path; metadata items take the
        # fetch_channel_metadata path. This is how metadata recheck
        # tasks become first-class queue citizens — visible in the
        # Sync Tasks popover, pausable, and cancellable via the same
        # controls as downloads. rule: "every channel's
        # metadata check should show as its own sync task."
        # Kind is computed BEFORE the live-row emit so the row's
        # bracket / name color matches the task type — green for
        # downloads, pink for metadata-family work.
        _ch_kind = (ch.get("kind") or "download").lower()
        _is_meta_kind = _ch_kind in ("metadata", "metadata_comments",
                                     "videoid_backfill",
                                     "repair_yt_captions",
                                     "punct_restore")
        _row_bracket = "meta_bracket" if _is_meta_kind else "sync_bracket"
        _row_name_tag = ("simpleline_pink" if _is_meta_kind
                         else "simpleline_green")

        # Emit the "live" row for this channel (header only, no summary).
        # sync_channel does its work; afterwards we emit the "done" row
        # with the same sync_row_<i> marker so it replaces the header in
        # place, giving the user a single consolidated line per channel.
        ch_name = ch.get("name", "?")
        _last_live.update({"i": i, "total": total, "name": ch_name})
        _sync_row_emit(stream, i, total, ch_name,
                       name_tag=_row_name_tag,
                       bracket_tag=_row_bracket)
        # If user hit Pause between the cooldown check and now, honor it
        # before kicking off yt-dlp.
        _wait_if_paused()
        _task_t0 = time.time()  # per-task timer for [Mtadta] activity row
        # Set current_sync for non-download kinds too. Without this,
        # the popover head row is empty during metadata / comments /
        # backfill passes, which breaks the "Pause is taking effect"
        # blink (paintBlinkState requires a running head row to compute
        # `sync_running=true`). Cleared at the end of the iteration
        # below so the next channel doesn't inherit a stale row.
        if _ch_kind in ("metadata", "metadata_comments", "videoid_backfill",
                        "repair_yt_captions", "punct_restore"):
            if queues is not None:
                try: queues.set_current_sync(ch)
                except Exception as e: swallow("current-sync set", e)
        if _ch_kind == "metadata":
            try:
                from .. import metadata as _meta
                # feature H-14: year-scoped metadata tasks carry a
                # `scope: {"year": N}` set by
                # Api.metadata_queue_channel_year so the backend filters
                # on-disk videos to that year before processing. None
                # for whole-channel tasks (existing behavior unchanged).
                _res = _meta.fetch_channel_metadata(
                    ch, stream, cancel_event,
                    refresh=bool(ch.get("refresh")),
                    pause_event=pause_event,
                    scope=ch.get("scope"),
                    queues=queues)
                # Detect pause-interrupted metadata walk — same
                # re-enqueue-at-front treatment as downloads.
                if _requeue_paused_task(
                        ch, ch_name, i, total,
                        reason="metadata",
                        bracket_tag=_row_bracket):
                    continue
                _fetched = int(_res.get("fetched", 0) or 0)
                _refreshed = int(_res.get("refreshed", 0) or 0)
                _errors_meta = int(_res.get("errors", 0) or 0)
                # Unchanged = videos that were checked but whose view
                # counts hadn't moved since last refresh. Shown in the
                # activity log so users can see the work the pass did
                # even when nothing actually needed updating. Without
                # this, a row reading "85 refreshed" hid the fact that
                # the pass had to walk ~460 entries to find 85 that
                # changed.
                _skipped = int(_res.get("skipped", 0) or 0)
                # Roll into pass-wide accumulators so Pass complete
                # reports accurate numbers per kind.
                sum_meta_fetched += _fetched
                sum_meta_refreshed += _refreshed
                sum_err += _errors_meta
                # issue #136 + when the user runs "Refresh
                # views/likes" (refresh=True), every on-disk video is
                # re-hit and counts roll into `refreshed`, not
                # `fetched`. The old summary ignored `refreshed` and
                # errors entirely, so the task row said "up to date"
                # even on a refresh pass with partial failures — hid
                # real problems.
                _parts: list[str] = []
                if _fetched:
                    _parts.append(f"{_fetched} new")
                if _refreshed:
                    _parts.append(f"{_refreshed} refreshed")
                if _errors_meta:
                    _parts.append(f"{_errors_meta} errors")
                if not _parts:
                    _parts.append("up to date")
                _summary = " \u00b7 ".join(_parts)
                _summary_tag = ("red" if _errors_meta else
                                "simpleline_pink" if (_fetched or _refreshed)
                                else "dim")
                _sync_row_emit(stream, i, total, ch_name,
                               summary=_summary,
                               name_tag="simpleline",
                               summary_tag=_summary_tag,
                               bracket_tag=_row_bracket)
                # Activity-log row — mirrors the [Dwnld] row pattern.
                # Column layout is FIXED so values always line up
                # vertically across rows:
                #   primary   = the main action verb ("N refreshed",
                #               "N fetched", or "up to date")
                #   secondary = "N new" when a refresh pass also picked
                #               up first-time fetches, else empty
                #   tertiary  = "N unchanged" (when refresh pass touched
                #               videos whose counts hadn't moved), else
                #               empty
                # Design rule: keep "unchanged" always in tertiary
                # so it doesn't jump columns when "new" appears or
                # disappears. Previously a row with only "refreshed +
                # unchanged" put unchanged in secondary, while a row
                # with "refreshed + new + unchanged" pushed it to
                # tertiary — visually misaligned.
                _a_secondary = ""
                _a_tertiary = ""
                if _refreshed > 0 and _fetched > 0:
                    _a_primary = f"{_refreshed} refreshed"
                    _a_secondary = f"{_fetched} new"
                    if _skipped > 0:
                        _a_tertiary = f"{_skipped} unchanged"
                elif _refreshed > 0:
                    _a_primary = f"{_refreshed} refreshed"
                    if _skipped > 0:
                        _a_tertiary = f"{_skipped} unchanged"
                elif _fetched > 0:
                    _a_primary = f"{_fetched} fetched"
                else:
                    _a_primary = "up to date"
                emit_metadata_activity_row(
                    stream, ch_name,
                    primary=_a_primary, secondary=_a_secondary,
                    tertiary=_a_tertiary,
                    errors=_errors_meta,
                    elapsed=time.time() - _task_t0,
                    green=(_errors_meta == 0))
            except Exception as _me:
                stream.emit_error(f"Metadata failed for {ch_name}: {_me}")
                _emit_failed_metadata_task(i, total, ch_name, _task_t0,
                                           bracket_tag=_row_bracket)
            _last_live["name"] = ""
            # Push Settings > Metadata tab refresh — last_views_refresh_ts
            # may have just been stamped on the channel config.
            fire_metadata_changed_hook()
            continue
        # Comments-only refresh task. Separate from `metadata` because
        # comments can only be fetched per-video (no bulk mode) and
        # users wanted them as a distinct user-triggered action — NOT
        # bundled into "Refresh views/likes". Task dict may carry
        # `only_recent_days` to scope to the most-recent uploads.
        if _ch_kind == "metadata_comments":
            try:
                from .. import metadata as _meta
                _res = _meta.refresh_channel_comments(
                    ch, stream, cancel_event=cancel_event,
                    pause_event=pause_event,
                    only_recent_days=ch.get("only_recent_days"),
                    queues=queues)
                # Honor pause the same way the metadata branch does.
                if _requeue_paused_task(
                        ch, ch_name, i, total,
                        reason="metadata-comments",
                        bracket_tag=_row_bracket):
                    continue
                _fetched = int(_res.get("fetched", 0) or 0)
                _errors_c = int(_res.get("errors", 0) or 0)
                _unchanged = int(_res.get("unchanged", 0) or 0)
                sum_comments_refreshed += _fetched
                sum_err += _errors_c
                # Rich row: pink brackets/dashes/parens/dots, white body
                # text, red errors. Scope shown inline next to the
                # channel name when the task was scoped (last Nd).
                _scope_d = ch.get("only_recent_days")
                if _scope_d:
                    _name_segs = [
                        [ch_name, "simpleline"],
                        [" (", "meta_bracket"],
                        [f"last {_scope_d}d", "simpleline"],
                        [")", "meta_bracket"],
                    ]
                else:
                    _name_segs = [[ch_name, "simpleline"]]
                # Fixed-width column layout so rows stack vertically:
                # the count is right-aligned to 4 chars, "OK" is always
                # shown (even when 0) so the column never disappears,
                # and the no-scope/failure messages pad to the same
                # combined width as the count+OK pair so the trailing
                # "(took …)" cell lands at the same column across all
                # row variants.
                _COUNT_W = 4   # right-aligned count width — fits 9999
                _OK_W    = 4
                # Width of "{count:>W} comments refreshed · {ok:>W} OK"
                _NORMAL_PREFIX_W = (_COUNT_W + len(" comments refreshed")
                                    + len(" · ")
                                    + _OK_W + len(" OK"))
                _chunks = []
                if _fetched > 0:
                    _chunks.append([f"{_fetched:>{_COUNT_W}} comments refreshed",
                                    "simpleline"])
                    _chunks.append([f"{_unchanged:>{_OK_W}} OK", "simpleline"])
                elif _errors_c:
                    _msg = "comments refresh failed"
                    _chunks.append([_msg + " " * max(0, _NORMAL_PREFIX_W - len(_msg)),
                                    "red"])
                else:
                    _msg = "no videos in scope"
                    _chunks.append([_msg + " " * max(0, _NORMAL_PREFIX_W - len(_msg)),
                                    "dim"])
                # Errors alongside successes: append after the OK cell.
                # Pushes (took …) right for this one row only — acceptable
                # since errors are rare and worth standing out.
                if _errors_c and _fetched > 0:
                    _chunks.append([f"{_errors_c} Errors", "red"])
                _sum_segs = [[" \u2014 ", "meta_bracket"]]
                for _j, _c in enumerate(_chunks):
                    if _j > 0:
                        _sum_segs.append([" \u00b7 ", "meta_bracket"])
                    _sum_segs.append(_c)
                _sum_segs.append([" \u00b7 ", "meta_bracket"])
                _sum_segs.append(["(", "meta_bracket"])
                _sum_segs.append([f"took {_fmt_duration(time.time() - _task_t0)}",
                                  "simpleline"])
                _sum_segs.append([")\n", "meta_bracket"])
                _sync_row_emit(stream, i, total, _name_segs,
                               summary=_sum_segs,
                               bracket_tag=_row_bracket)
                if _fetched:
                    _a_primary = f"{_fetched} comments refreshed"
                elif _errors_c:
                    _a_primary = "comments refresh failed"
                else:
                    _a_primary = "no videos in scope"
                # Comments rows: put "X OK" in SECONDARY (not tertiary)
                # so the row reads "comments refreshed - OK - errors -
                # took" with no empty column gap. Wording matches the
                # log row above ("OK") rather than the older
                # "unchanged" wording. Different from views/likes
                # refresh, which keeps unchanged in tertiary because
                # secondary there can carry "N new" on a mixed-result
                # refresh pass. Comments has no "new" concept so
                # secondary was always empty — that was the awkward
                # gap. Show "0 OK" too when fetched > 0 so the column
                # stays populated row-to-row.
                _a_secondary = (f"{_unchanged} OK"
                                if _fetched > 0 else "")
                emit_metadata_activity_row(
                    stream, ch_name,
                    primary=_a_primary, secondary=_a_secondary,
                    tertiary="",
                    errors=_errors_c, elapsed=time.time() - _task_t0,
                    green=(_errors_c == 0))
            except Exception as _ce:
                stream.emit_error(
                    f"Comments refresh failed for {ch_name}: {_ce}")
                _emit_failed_metadata_task(i, total, ch_name, _task_t0,
                                           bracket_tag=_row_bracket)
            _last_live["name"] = ""
            # Push Metadata-tab refresh (last_comments_refresh_ts may
            # have just been stamped).
            fire_metadata_changed_hook()
            continue
        # Video-id backfill task. One-shot resolution + DB write for
        # archives migrated from the tkinter-era YTArchiver that have
        # no [id] bracket in filenames and no .info.json sidecars —
        # without this, the bulk views/likes refresh path can't match
        # ANY on-disk file to its YouTube row. Separate from the
        # views/likes kind because it's deliberately a fast, cheap
        # prerequisite pass, not an actual metadata refresh.
        if _ch_kind == "videoid_backfill":
            try:
                from .. import metadata as _meta
                _mode = ch.get("mode") or "fast"
                _res = _meta.backfill_video_ids(
                    ch, stream, cancel_event=cancel_event,
                    pause_event=pause_event,
                    queues=queues,
                    mode=_mode)
                if _requeue_paused_task(
                        ch, ch_name, i, total,
                        reason="backfill",
                        bracket_tag=_row_bracket):
                    continue
                _resolved = int(_res.get("resolved", 0) or 0)
                _unresolved = int(_res.get("unresolved", 0) or 0)
                _already = int(_res.get("already_set", 0) or 0)
                sum_ids_backfilled += _resolved
                _parts = []
                if _resolved:
                    _parts.append(f"{_resolved} backfilled")
                if _already:
                    _parts.append(f"{_already} already had ID")
                if _unresolved:
                    _parts.append(f"{_unresolved} unresolved")
                if not _parts:
                    _parts.append(_res.get("error") or "nothing to do")
                _summary = " \u00b7 ".join(_parts)
                _summary_tag = ("simpleline_pink" if _resolved
                                else "dim" if not _res.get("error")
                                else "red")
                _sync_row_emit(stream, i, total, ch_name,
                               summary=_summary,
                               name_tag="simpleline",
                               summary_tag=_summary_tag,
                               bracket_tag=_row_bracket)
                _a_primary = (f"{_resolved} IDs backfilled" if _resolved
                              else "no IDs to backfill")
                _a_secondary = (f"{_unresolved} unresolved"
                                if _unresolved else "")
                _a_err = 1 if _res.get("error") else 0
                emit_metadata_activity_row(
                    stream, ch_name,
                    primary=_a_primary, secondary=_a_secondary,
                    errors=_a_err, elapsed=time.time() - _task_t0,
                    green=(_a_err == 0))
            except Exception as _be:
                stream.emit_error(
                    f"ID backfill failed for {ch_name}: {_be}")
                _emit_failed_metadata_task(i, total, ch_name, _task_t0,
                                           bracket_tag=_row_bracket)
            _last_live["name"] = ""
            # Push Metadata-tab refresh — the Video IDs column status
            # just changed (resolved count went up, missing went down).
            fire_metadata_changed_hook()
            continue
        # Repair YT auto-captions task. Re-fetches each archived video's
        # VTT, runs the fixed _parse_vtt, and rewrites the per-word array
        # in the JSONL + segments DB. Lives on the sync queue so it
        # serializes with downloads/metadata — multiple yt-dlp processes
        # hitting YT in parallel would invite rate-limiting.
        if _ch_kind == "repair_yt_captions":
            try:
                from .. import repair_captions as _rc
                _output_dir = (cfg.get("output_dir") or "").strip()
                if not _output_dir:
                    raise RuntimeError("output_dir not configured")
                _res = _rc.repair_archive(
                    output_dir=_output_dir,
                    channel_folder=ch.get("channel_folder") or None,
                    video_id=ch.get("video_id") or None,
                    dry_run=bool(ch.get("dry_run")),
                    log_stream=stream,
                    cancel_event=cancel_event,
                    pause_event=pause_event,
                    queues=queues,
                    scope_url=(ch.get("url") or "").strip() or None,
                )
                # Clear the per-video "(N/total)" decoration so the next
                # popover render shows the clean task name again.
                if queues is not None:
                    try: queues.set_sync_pass_progress(0, 0)
                    except Exception as e: swallow("repair pass-progress reset", e)
                _ok_n = int(_res.get("succeeded", 0) or 0)
                _skip_n = int(_res.get("skipped", 0) or 0)
                _fail_n = int(_res.get("failed", 0) or 0)
                _was_cancelled = bool(_res.get("cancelled"))
                _summary = (f"{_ok_n} repaired · "
                            f"{_skip_n} skipped · "
                            f"{_fail_n} failed")
                if _was_cancelled:
                    _summary = "cancelled — " + _summary
                _sum_tag = ("red" if _fail_n > 0 and not _was_cancelled
                            else "simpleline_pink" if _ok_n > 0
                            else "dim")
                _sync_row_emit(stream, i, total, ch_name,
                               summary=_summary,
                               name_tag="simpleline",
                               summary_tag=_sum_tag,
                               bracket_tag=_row_bracket)
            except Exception as _rpe:
                stream.emit_error(f"Repair failed for {ch_name}: {_rpe}\n")
                _sync_row_emit(stream, i, total, ch_name,
                               summary="failed",
                               name_tag="dim", summary_tag="red",
                               bracket_tag=_row_bracket)
            _last_live["name"] = ""
            continue
        # Restore transcript punctuation task. Runs each video's per-
        # segment text through the PunctuationManager (the same punct
        # restoration worker the Whisper path uses at ingest) and writes
        # the punctuated form back to segments.text + the JSONL. Pure
        # local CPU/GPU work — no YT calls — but still serializes through
        # the sync queue so it doesn't fight an in-flight download or
        # repair pass for the punct model's GPU slot.
        if _ch_kind == "punct_restore":
            try:
                from .. import punct_restore as _pr
                _output_dir = (cfg.get("output_dir") or "").strip()
                if not _output_dir:
                    raise RuntimeError("output_dir not configured")
                _res = _pr.restore_punctuation_archive(
                    output_dir=_output_dir,
                    channel_folder=ch.get("channel_folder") or None,
                    video_id=ch.get("video_id") or None,
                    dry_run=bool(ch.get("dry_run")),
                    log_stream=stream,
                    cancel_event=cancel_event,
                    pause_event=pause_event,
                    queues=queues,
                    scope_url=(ch.get("url") or "").strip() or None,
                )
                if queues is not None:
                    try: queues.set_sync_pass_progress(0, 0)
                    except Exception as e: swallow("punct pass-progress reset", e)
                _ok_n = int(_res.get("succeeded", 0) or 0)
                _skip_n = int(_res.get("skipped", 0) or 0)
                _fail_n = int(_res.get("failed", 0) or 0)
                _was_cancelled = bool(_res.get("cancelled"))
                _summary = (f"{_ok_n} punctuated · "
                            f"{_skip_n} skipped · "
                            f"{_fail_n} failed")
                if _was_cancelled:
                    _summary = "cancelled — " + _summary
                _sum_tag = ("red" if _fail_n > 0 and not _was_cancelled
                            else "simpleline_pink" if _ok_n > 0
                            else "dim")
                _sync_row_emit(stream, i, total, ch_name,
                               summary=_summary,
                               name_tag="simpleline",
                               summary_tag=_sum_tag,
                               bracket_tag=_row_bracket)
            except Exception as _pre:
                stream.emit_error(
                    f"Punctuation restore failed for {ch_name}: {_pre}\n")
                _sync_row_emit(stream, i, total, ch_name,
                               summary="failed",
                               name_tag="dim", summary_tag="red",
                               bracket_tag=_row_bracket)
            _last_live["name"] = ""
            continue
        # ── Quick-check fast path ────────────────────────────────────
        # Extra speedup on top of `--break-on-existing`: probe the first
        # 5 video IDs via `--flat-playlist --lazy-playlist --playlist-end
        # 5` and check them against the download archive. If all 5 are
        # already archived, skip the full yt-dlp run entirely. For a
        # 1000+ video channel, this saves the API pagination cost
        # (~45 pages × 0.25s sleep = ~11s) on top of what break-on-
        # existing already saves.
        # Gating mirrors YTArchiver.py:22984 exactly:
        # init_complete AND sync_complete AND mode == "full"
        # The fast-path exists BECAUSE full-mode channels can't rely on
        # `--break-on-existing` doing the work alone after a bootstrap
        # (in case a mid-channel video is missing and needs backfill).
        # For sub/date modes, the main break-on-existing path is already
        # fast enough — no need for the probe.
        _ch_url = (ch.get("url") or "").strip()
        _ch_mode = (ch.get("mode") or "full").lower()
        _ch_is_init = bool(ch.get("initialized", False))
        _ch_sync_ok = bool(ch.get("sync_complete", True))
        if ch.get("init_complete", False):
            _ch_sync_ok = True
        _fast_path_eligible = (
            ch.get("init_complete", False) and
            _ch_sync_ok and
            _ch_mode == "full" and
            # Channels with failed videos pending retry must run the
            # full sync_channel pass — retries are prepended only
            # there, so quick-checking "no new videos" starved them
            # forever (silent permanent archive gaps that never even
            # reached the 3-strike give-up).
            not (ch.get("failed_video_ids") or {})
        )
        if _known_ids and _ch_url and _fast_path_eligible:
            _qc = quick_check_new_uploads(
                _ch_url, _known_ids, check_count=5, timeout_sec=30)
            if _qc.get("ok") and not _qc.get("has_new"):
                _sync_row_emit(stream, i, total, ch_name,
                               summary="no new videos",
                               name_tag="simpleline", summary_tag="dim")
                _last_live["name"] = ""
                continue
            if _qc.get("quickcheck_skipped"):
                _sync_row_emit(stream, i, total, ch_name,
                               summary="quick check cooling down; full sync",
                               name_tag="simpleline", summary_tag="dim")
            elif _qc.get("timed_out"):
                _sync_row_emit(stream, i, total, ch_name,
                               summary="quick check timed out; full sync",
                               name_tag="simpleline", summary_tag="dim")
            elif _qc.get("empty_probe"):
                _sync_row_emit(stream, i, total, ch_name,
                               summary="quick check returned empty; full sync",
                               name_tag="simpleline", summary_tag="dim")
        # Wrap sync_channel in try/finally so the sync-active flag is
        # ALWAYS cleared on this channel — even when sync_channel takes
        # one of its many early-return paths (no URL, yt-dlp missing,
        # write blocked, disk_low, launch failed, cancelled-during-
        # launch, etc.) which historically skipped the in-function
        # clear_sync_active() at the bottom. Stuck-active channels make
        # is_any_sync_active() lie and hold transcribe [Trnscr] rows
        # forever.
        try:
            res = sync_channel(ch, stream, cancel_event,
                               queues=queues, transcribe_mgr=transcribe_mgr,
                               pause_event=pause_event,
                               kill_current=skip_event,
                               pass_idx=i, pass_total=total)
        finally:
            try:
                from .active_state import clear_sync_active as _clear_active
                _clear_active(ch_name)
            except Exception as _ce:
                swallow("clear-sync-active", _ce)
        _dl = int(res.get("downloaded", 0) or 0)
        _err = int(res.get("errors", 0) or 0)
        sum_dl += _dl
        sum_err += _err
        # Detect "paused mid-download": pause_event set and the
        # readline loop bailed out. Put this channel back at the
        # FRONT of the queue so Resume continues it instead of
        # silently skipping. yt-dlp's `--continue` + download-archive
        # picks up where it left off, so no data is lost.
        if _requeue_paused_task(ch, ch_name, i, total, reason="download"):
            # Loop will hit _wait_if_paused() on next iter and block.
            continue
        # Replace the live row with a compact summary.
        _sync_row_emit(stream, i, total, ch_name,
                       summary=_short_summary(_dl, _err),
                       name_tag="simpleline_green" if _dl > 0 else "simpleline",
                       summary_tag="simpleline_green" if _dl > 0 else "dim")
        # Clear the "live" marker so a pause between channels doesn't
        # re-paint this row (which is now DONE with a summary).
        _last_live["name"] = ""
        # Notify any registered listener that this channel's per-channel
        # state just changed. Main.py wires this to a JS push so the
        # Subs tab's "Last Sync" column updates live as the pass
        # advances — without it the column stays frozen at its boot-time
        # values until the user clicks away and back.
        try: fire_channel_synced_hook()
        except Exception as e: swallow("channel-synced hook", e)
        # If this was a batch-limited bootstrap run, apply the next cooldown.
        # We only set cooldown when the channel hadn't finished initializing
        # and this pass hit the BATCH_LIMIT threshold.
        if _should_batch_limit(ch, res.get("total", 0)):
            set_batch_cooldown(ch.get("url", ""))

    elapsed = time.time() - t_start
    # Per-kind summary: the action verb on the Pass complete line
    # now reflects what the pass actually did. Previously it always
    # said "N downloaded" even for a views/likes refresh where no
    # download happened — confusing ("why does it say 0 downloaded
    # when I just refreshed 912 videos?").
    # Pass complete line styling:
    #   === brackets                 → green+bold (simplestatus_green)
    #   "Pass complete:"             → white+bold (simplestatus_white)
    #   action verb (N refreshed…)   → green+bold for non-zero,
    #                                  white+bold for zero (still
    #                                  readable, just less celebratory)
    #   separators, errors when 0,
    #   skipped, "took Ns"           → bright white (simpleline)
    #   errors when > 0              → red
    # Prior build used `header` (muted #a0aabb) for body parts, which
    # users reported as "near unreadable" on their displays. Switched
    # the body to `simpleline` (bright --c-text) + `simplestatus_white`
    # for the primary label.
    emit_parts: list[list[str]] = [
        ["=== ", "simplestatus_green"],
        ["Pass complete: ", "simplestatus_white"],
    ]
    _verb_chunks: list[tuple[str, str]] = []  # (text, tag) pairs for action verbs
    if sum_dl > 0:
        _verb_chunks.append((f"{sum_dl} downloaded", "simplestatus_green"))
    if sum_meta_refreshed > 0:
        _verb_chunks.append((f"{sum_meta_refreshed} refreshed", "simplestatus_green"))
    if sum_meta_fetched > 0:
        _verb_chunks.append((f"{sum_meta_fetched} metadata fetched", "simplestatus_green"))
    if sum_comments_refreshed > 0:
        _verb_chunks.append((f"{sum_comments_refreshed} comments refreshed", "simplestatus_green"))
    if sum_ids_backfilled > 0:
        _verb_chunks.append((f"{sum_ids_backfilled} IDs backfilled", "simplestatus_green"))
    if not _verb_chunks:
        # All counters zero. Pick a sensible 0-verb matching the
        # pass label so the user sees "something ran" rather than a
        # blank summary. White+bold for zero-count (still fully
        # readable, just not the celebratory green).
        if _label == "Views/likes refresh":
            _verb_chunks.append(("0 refreshed", "simplestatus_white"))
        elif _label == "Comments refresh":
            _verb_chunks.append(("0 comments refreshed", "simplestatus_white"))
        elif _label == "Video ID backfill":
            _verb_chunks.append(("0 IDs backfilled", "simplestatus_white"))
        elif _label == "Metadata download":
            _verb_chunks.append(("0 metadata fetched", "simplestatus_white"))
        else:
            _verb_chunks.append(("0 downloaded", "simplestatus_white"))
    # Interleave with separators.
    for _i, (_txt, _tag) in enumerate(_verb_chunks):
        if _i > 0:
            emit_parts.append([" \u00b7 ", "simpleline"])
        emit_parts.append([_txt, _tag])
    # Errors: red if non-zero, white if 0.
    emit_parts.append([" \u00b7 ", "simpleline"])
    emit_parts.append([f"{sum_err} errors",
                       "red" if sum_err > 0 else "simpleline"])
    if skipped:
        emit_parts.append([" \u00b7 ", "simpleline"])
        emit_parts.append([f"{skipped} skipped", "simpleline"])
    emit_parts.append([" \u00b7 ", "simpleline"])
    emit_parts.append([f"took {_fmt_duration(elapsed)} ", "simpleline"])
    emit_parts.append(["===\n", "simplestatus_green"])
    stream.emit(emit_parts)
    # Global "Last Full Sync" timestamp \u2014 written here at pass
    # completion (and only for a download-kind pass, and only when not
    # cancelled) so the UI's "Last Full Sync: \u2026" label genuinely means
    # "a full Sync Subbed pass finished at this time". Previously this
    # was written inside sync_channel whenever any channel downloaded,
    # so the label advanced mid-pass after the first channel with new
    # videos \u2014 making it look like a sync "completed" at the moment the
    # FIRST channel finished a download, not at the actual end of pass.
    _cancelled_for_ts = (cancel_event is not None and cancel_event.is_set())
    if _label == "Sync pass" and not _cancelled_for_ts:
        try:
            with config_transaction() as _cfg_end:
                _cfg_end["last_sync"] = datetime.now().strftime(
                    "%Y-%m-%d %H:%M")
        except Exception as _ce:
            _log.debug("end-of-pass last_sync write failed: %s", _ce)
    # Clean up: clear the running-slot and pass-progress decoration.
    # Only flush remaining queued items when the loop drained
    # NATURALLY (no cancel). On cancel, leave the queue alone so
    # items re-inserted by the pause/cancel path (sync_all line 923
    # re-insert + metadata pause line 447 re-queue) survive for the
    # user to resume (audit: sync/sync_all.py:1018).
    _cancelled = (cancel_event is not None and cancel_event.is_set())
    if queues is not None:
        if not _cancelled:
            try: queues.sync_clear()
            except Exception as e: swallow("queue clear on pass end", e)
        # Don't drop the in-flight item from `current_sync` on cancel
        # — that's what `save_now` writes into the `resuming` dict
        # so the next app launch can pick it back up. Clearing it
        # here meant a cancelled-mid-channel run lost the in-flight
        # channel from the resume entry-point (audit: sync_all H27).
        if not _cancelled:
            try: queues.set_current_sync(None)
            except Exception as e: swallow("current-sync clear", e)
        try: queues.set_sync_pass_progress(0, 0)
        except Exception as e: swallow("pass-progress reset", e)
    # Clear the sync-progress file so any companion display goes idle.
    try: clear_sync_progress()
    except Exception as e: swallow("sync-progress cleanup", e)
    # Clear the thread-local pass_id so stray `_sync_row_emit` calls
    # after this function returns don't tag rows with a dead pass.
    try: _ROW_EMIT_PASS_ID.id = ""
    except Exception as e: swallow("pass-id clear", e)
    return {"ok": True, "downloaded": sum_dl, "errors": sum_err,
            "skipped": skipped,
            "took": _fmt_duration(elapsed), "total": total}
