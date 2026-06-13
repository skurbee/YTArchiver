"""
sync.log_rows — activity-log row emitters and pass-id state.

Patch 18 phase 4 (v69.4): extracted from backend/sync/legacy.py.

Public surface (re-exported via the sync package):
    _bracket_segments(label, ...)
        Build `[label]` log segments with the bracket/label tag split.
    _new_pass_id()
        Hand out a unique pass token for in-place-replace markers.
    _sync_row_emit(stream, idx, total, name, summary=None, ...)
        One `[N/total] Name — summary` line, replaces in-place per pass.
    _short_summary(downloaded, errors)
        "N new" / "no new videos" / etc.
    _count_cell(n, label)
        "✓ label" when n==1, else "N label".
    emit_metadata_activity_row(stream, channel_name, ...)
        [Metdta] activity-log row.
    emit_consolidated_auto_row(stream, channel_name, ...)
        [Dwnld] activity-log row (4 cells + errors + took).
    _persist_row_history(row_id, line)
        Append/replace in config['autorun_history'].
    register_pending_dwnld_row / pop_pending_dwnld_row
        Cross-thread registry the transcribe-complete hook uses to
        patch a [Dwnld] row's transcribed-count in place.

Module-level state owned here:
    _PASS_ID_COUNTER + _PASS_ID_LOCK + _ROW_EMIT_PASS_ID
    _HIST_INDEX_BY_ROW_ID + _HIST_INDEX_LOCK
    _RECENT_DWNLD_ROWS + _RECENT_DWNLD_LOCK

`_fmt_duration` (used by the [Metdta]/[Dwnld] emitters) lives in
sync/core.py — accessed via the proxy below.
"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime
from typing import Any

from ..log import get_logger
from ..log_stream import LogStreamer

_log = get_logger(__name__)


def _fmt_duration(*args, **kwargs):
    """Lazy proxy — the real impl lives in sync/core.py."""
    from .core import _fmt_duration as _impl
    return _impl(*args, **kwargs)


def _bracket_segments(label: str, bracket_tag: str = "sync_bracket",
                      label_tag: str = "simpleline",
                      trailing_space: bool = True,
                      extra_tag: str | None = None) -> list:
    """Build log segments for a bracketed header, e.g. [Sync] or [1/103].

    Returns a list shaped for `stream.emit(segments)`. ONLY the `[`, `]`
    and inner `/` (for "N/M") get the bracket color; the word / numbers
    stay in the neutral label tag. Matches the OLD visual: green
    punctuation, white numbers / words.

    `extra_tag` (optional): an additional tag appended to every segment's
    tag list. Used for in-place replacement markers (e.g. "sync_row_12"
    so channel 12's live + done emits replace each other in place).
    """
    def _merge(primary: str) -> Any:
        return [primary, extra_tag] if extra_tag else primary

    segs = [["[", _merge(bracket_tag)]]
    parts = label.split("/")
    for i, part in enumerate(parts):
        if i > 0:
            segs.append(["/", _merge(bracket_tag)])
        segs.append([part, _merge(label_tag)])
    segs.append(["]" + (" " if trailing_space else ""), _merge(bracket_tag)])
    return segs


# Module-level pass-id counter + lock. Each invocation of `sync_all()`
# (or `sync_one_channel`) calls `_new_pass_id()` once to get a unique
# token and stashes it on `_ROW_EMIT_PASS_ID.id`; `_sync_row_emit`
# reads that thread-local by default and appends the token to its
# in-place-replace marker so passes never collide.
#
# Without this, the autorun-triggered second pass's `sync_row_1` emit
# would find the first pass's `sync_row_1` DOM element (still in
# scrollback) and silently replace its content — leaving the user
# staring at a log that seemed to skip most of the channel iteration.

# Module-level pass-id counter + lock. Each invocation of `sync_all()`
# (or `sync_one_channel`) calls `_new_pass_id()` once to get a unique
# token and stashes it on `_ROW_EMIT_PASS_ID.id`; `_sync_row_emit`
# reads that thread-local by default and appends the token to its
# in-place-replace marker so passes never collide.
#
# Without this, the autorun-triggered second pass's `sync_row_1` emit
# would find the first pass's `sync_row_1` DOM element (still in
# scrollback) and silently replace its content — leaving the user
# staring at a log that seemed to skip most of the channel iteration.
_PASS_ID_COUNTER = 0
_PASS_ID_LOCK = threading.Lock()
_ROW_EMIT_PASS_ID = threading.local()


def _new_pass_id() -> str:
    global _PASS_ID_COUNTER
    with _PASS_ID_LOCK:
        _PASS_ID_COUNTER += 1
        return f"p{_PASS_ID_COUNTER}"


def _sync_row_emit(stream: LogStreamer, idx: int, total: int,
                   name, summary=None,
                   name_tag: str = "simpleline_green",
                   summary_tag: str = "simpleline",
                   pass_id: str = "",
                   bracket_tag: str = "sync_bracket") -> None:
    """Emit a single `[N/total] Name — summary` line that replaces in-
    place across re-emissions for the same channel index WITHIN ONE
    sync pass.

    `pass_id` disambiguates markers across passes: without it, a second
    sync pass's channel-1 row would replace the first pass's channel-1
    row WAY up in the log (at its DOM position from 8 minutes ago) and
    the user sees nothing new at the current scroll position. Callers
    should pass a unique id (`_new_pass_id()`) when starting a pass.

    summary=None → "live" row (just `[N/total] Name`).
    summary=str → "done" row (appends ` — summary`). Pad with spaces
                   so the em-dash column aligns roughly at col 34.

    Rich-segment form: `name` and/or `summary` may be a list of
    `[text, tag]` pairs for multi-color output. When `summary` is a list,
    the " — " prefix is NOT auto-added — include the separator and the
    trailing newline yourself in the segments.
    """
    # Fall back to the thread-local stashed by sync_all / sync_one_channel
    # if the caller didn't pass one explicitly.
    if not pass_id:
        pass_id = getattr(_ROW_EMIT_PASS_ID, "id", "") or ""
    marker = (f"sync_row_{pass_id}_{idx}" if pass_id
              else f"sync_row_{idx}")
    segs = _bracket_segments(f"{idx}/{total}", extra_tag=marker,
                             bracket_tag=bracket_tag)
    if isinstance(name, str):
        _name_disp_len = len(name)
        _name_in = [[name, name_tag]]
    else:
        _name_in = list(name)
        _name_disp_len = sum(len(s[0]) for s in _name_in)

    def _mk_tag(t):
        return [t, marker] if isinstance(t, str) else list(t) + [marker]

    if summary is None:
        for s_txt, s_tag in _name_in:
            segs.append([s_txt, _mk_tag(s_tag)])
        segs.append(["\n", [name_tag, marker]])
    else:
        # Pad the channel name to align the em-dash at a consistent column
        name_col = 34
        pad = " " * max(0, name_col - _name_disp_len)
        for s_txt, s_tag in _name_in:
            segs.append([s_txt, _mk_tag(s_tag)])
        if pad:
            segs.append([pad, [name_tag, marker]])
        if isinstance(summary, str):
            segs.append([f" \u2014 {summary}\n", [summary_tag, marker]])
        else:
            for s_txt, s_tag in summary:
                segs.append([s_txt, _mk_tag(s_tag)])
    stream.emit(segs)


def _short_summary(downloaded: int, errors: int) -> str:
    """Compact one-phrase summary for a channel row done-emit.
    Matches OLD's per-channel summary style ('no new videos' / '3 new')."""
    if downloaded <= 0 and errors <= 0:
        return "no new videos"
    if errors <= 0:
        return f"{downloaded} new video{'s' if downloaded != 1 else ''}"
    if downloaded <= 0:
        return f"{errors} error{'s' if errors != 1 else ''}"
    return (f"{downloaded} new \u00b7 "
            f"{errors} error{'s' if errors != 1 else ''}")

def _count_cell(n: int, label: str) -> str:
    """Render a count cell. If n == 1, return "\u2713 {label}" instead of
    "1 {label}" — single-video polish for transcribed + metadata.
    For 0 we still show the numeric form so the user can see
    "0 transcribed" when a channel has auto_transcribe off. For >= 2
    we show the numeric count.

    NOTE: `downloaded` is ALWAYS rendered numerically (never \u2713) per
     follow-up: "leave the downloaded part as a number. ... 1
    downloaded (check) transcribed (check) metadata". Callers emit
    downloaded via f"{n} downloaded" directly.
    """
    if n == 1:
        return f"\u2713 {label}"
    return f"{n} {label}"


def emit_metadata_activity_row(stream: LogStreamer,
                                channel_name: str,
                                primary: str,
                                secondary: str,
                                errors: int,
                                elapsed: float,
                                green: bool = True,
                                tertiary: str = "") -> str:
    """Emit a [Metdta] activity-log row for metadata / comments / ID
    backfill tasks. Parallel to the [Dwnld] row `sync_channel` emits
    at end of a download pass. Users flagged that metadata refreshes
    were landing only in the main log, not the activity log above —
    this fills that gap so all background work has a matching history
    entry.

    Uses 3 data cells (matches [Dwnld]'s primary/secondary/tertiary)
    so the row reads:
        [Mtadta] [time,date] — [channel] —
        [primary] [secondary] [tertiary] [N errors] [took X]

    `primary` example: "61 refreshed", "12 comments refreshed",
                       "40 IDs backfilled".
    `secondary` example: "375 unchanged" (videos whose counts didn't
                         move on a refresh pass), "5 new".
    `tertiary` example: "375 unchanged" — used when secondary already
                         carries "N new" on a mixed-result refresh pass.
                         Empty string when nothing to put there.
    """
    now = datetime.now()
    time_str = (now.strftime("%-I:%M%p") if os.name != "nt"
                else now.strftime("%I:%M%p").lstrip("0")).lower()
    date_str = now.strftime("%b %d").replace(" 0", " ")
    took = _fmt_duration(elapsed)
    # Use nanosecond-precision so two metadata tasks completing in
    # the same second don't collide on row_id and overwrite each
    # other's activity row (audit: log_rows L24).
    row_id = f"metdta_{channel_name}_{time.time_ns()}"
    # Kind label is "Metdta" (6 chars, matches the existing classic
    # rows emitted by fetch_channel_metadata's legacy path). Row tag
    # is hist_pink — metadata-kind rows have always rendered pink in
    # the activity log; `green` was only used here to signal "nothing
    # happened" vs "something happened", NOT to force green tinting.
    # When no work happened we leave the tag blank (default color).
    # User: had_work used to flip false the moment errors > 0, so a
    # channel that refreshed 80 comments but failed 1 video painted
    # the "80 comments refreshed" cell white instead of pink.
    # Decouple from `errors` and `green` (the errors cell renders
    # in red separately). Pink fires whenever the primary string
    # indicates real work, regardless of partial failures.
    had_work = primary not in (
        "up to date", "no videos in scope", "no IDs to backfill",
        "failed", "comments refresh failed")
    payload = {
        "kind": "Metdta",
        "time_date": f"{time_str}, {date_str}",
        "channel": channel_name,
        "primary": primary,
        "secondary": secondary,
        "tertiary": tertiary,
        "errors": f"{errors} error" if errors == 1 else f"{errors} errors",
        "took": f"took {took}",
        "row_tag": "hist_pink" if had_work else "",
        "row_id": row_id,
    }
    stream.emit_activity(payload)
    try:
        kind_tag = f"[{'Metdta'.center(6)}]"
        ts_date = f"{time_str}, {date_str}".ljust(16)
        ch_part = f" {channel_name} \u2014" if channel_name else " " * 7
        # Mirror the [Dwnld] persistence format with 3 cells (primary
        # / secondary / tertiary) where downloaded / transcribed /
        # metadata would be.
        body = (f"{primary:<14} \u00b7 "
                f"{(secondary or '-'):<15} \u00b7 "
                f"{(tertiary or '-'):<15} \u00b7 "
                f"{int(errors or 0)} errors \u00b7 "
                f"took {took}")
        line = f"{kind_tag} {ts_date} \u2014{ch_part} {body}"
        _persist_row_history(row_id, line)
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return row_id

def emit_consolidated_auto_row(stream: LogStreamer,
                                channel_name: str,
                                downloaded: int,
                                transcribed: int,
                                metadata: int,
                                errors: int,
                                elapsed: float,
                                kind: str = "Dwnld",
                                row_id: str | None = None) -> str:
    """Emit ONE combined activity-log row replacing the historical trio
    of [Auto] + [Trnscr] + [Metdta]. UI receives four count cells:
        [kind] [time,date] \u2014 [channel] \u2014
        [primary=N downloaded] [secondary=N transcribed]
        [tertiary=N metadata] [errors=N errors] [took=took X]
    Per `downloaded` is ALWAYS numeric. `transcribed` and
    `metadata` use a \u2713 check when their count is exactly 1.
    Persisted string body is 5 bullets:
        N downloaded \u00b7 <N|\u2713> transcribed \u00b7 <N|\u2713> metadata \u00b7 N errors \u00b7 took X

    If `row_id` is provided (or generated), the UI tags the row with
    `data-row-id=<row_id>` so a later call with the same id replaces
    that row in place — used by the transcribe-complete hook to
    retroactively update a row that fired while Whisper was still
    running. Returns the row_id (existing or newly generated) so the
    caller can stash it for later updates.
    """
    now = datetime.now()
    time_str = (now.strftime("%-I:%M%p") if os.name != "nt"
                else now.strftime("%I:%M%p").lstrip("0")).lower()
    date_str = now.strftime("%b %d").replace(" 0", " ")
    took = _fmt_duration(elapsed)
    primary_s = f"{int(downloaded or 0)} downloaded"
    secondary_s = _count_cell(int(transcribed or 0), "transcribed")
    tertiary_s = _count_cell(int(metadata or 0), "metadata")
    if not row_id:
        # Channel + start-of-pass timestamp — two calls within the same
        # sync pass for the same channel share an id, but a fresh pass
        # minutes later gets a new one.
        row_id = f"dwnld_{channel_name}_{int(time.time())}"
    stream.emit_activity({
        "kind": kind,
        "time_date": f"{time_str}, {date_str}",
        "channel": channel_name,
        "primary": primary_s,
        "secondary": secondary_s,
        "tertiary": tertiary_s,
        "errors": f"{errors} error" if errors == 1 else f"{errors} errors",
        "took": f"took {took}",
        "row_tag": "hist_green" if downloaded > 0 else "",
        "row_id": row_id,
    })
    # Persist directly (bypassing format_history_entry so the checkmark
    # forms round-trip cleanly instead of being truncated to "0" by
    # the count-extraction logic). Use ljust/rjust padding so rendered
    # rows in activity-log history view line up visually.
    try:
        kind_tag = f"[{kind.center(6)}]" if len(kind) < 6 else f"[{kind}]"
        ts_date = f"{time_str}, {date_str}".ljust(16)
        ch_part = f" {channel_name} \u2014" if channel_name else " " * 7
        body = (f"{primary_s:<14} \u00b7 "
                    f"{secondary_s:<15} \u00b7 "
                    f"{tertiary_s:<13} \u00b7 "
                    f"{int(errors or 0)} errors \u00b7 "
                    f"took {took}")
        line = f"{kind_tag} {ts_date} \u2014{ch_part} {body}"
        # Row-ID-aware persist: if this row_id has already been
        # written to config (previous emit for same row), REPLACE
        # the persisted line instead of appending. Prevents
        # duplicate [Dwnld] rows when the transcribe-complete hook
        # re-emits with updated counts.
        _persist_row_history(row_id, line)
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return row_id


# Row-ID-aware history persistence. `emit_consolidated_auto_row` can
# be called twice for the SAME row_id — first when sync_channel
# finishes (transcribe count may still be 0), then retroactively when
# the transcribe worker drains. Without tracking which config index
# each row_id owns, the retroactive call appends a SECOND line to
# `autorun_history`, producing duplicate [Dwnld] rows on next load
# (and sometimes immediately if renderActivityLog re-runs). This
# dict maps row_id -> index in config["autorun_history"] so the
# retroactive path overwrites the correct slot.
_HIST_INDEX_BY_ROW_ID: dict[str, int] = {}
_HIST_INDEX_LOCK = threading.Lock()


def _persist_row_history(row_id: str, line: str) -> None:
    """Append `line` to config['autorun_history'], or replace the
    previously-persisted entry if this row_id has already been
    written. Deduplicates retroactive updates of the same row.
    """
    try:
        from .. import autorun as _ar
        from .. import ytarchiver_config as _cfg
    except Exception:
        return
    if not _cfg.config_is_writable():
        return
    try:
        cfg = _cfg.load_config()
        hist = cfg.setdefault("autorun_history", [])
        with _HIST_INDEX_LOCK:
            existing_idx = _HIST_INDEX_BY_ROW_ID.get(row_id) if row_id else None
            if (existing_idx is not None
                    and 0 <= existing_idx < len(hist)):
                # Retroactive update — replace the previous line.
                hist[existing_idx] = line
            else:
                hist.append(line)
                # Trim + shift any tracked indices if we exceeded cap.
                if len(hist) > _ar.AUTORUN_HISTORY_MAX:
                    trim_n = len(hist) - _ar.AUTORUN_HISTORY_MAX
                    hist = hist[-_ar.AUTORUN_HISTORY_MAX:]
                    cfg["autorun_history"] = hist
                    for _k, _v in list(_HIST_INDEX_BY_ROW_ID.items()):
                        if _v < trim_n:
                            _HIST_INDEX_BY_ROW_ID.pop(_k, None)
                        else:
                            _HIST_INDEX_BY_ROW_ID[_k] = _v - trim_n
                if row_id:
                    _HIST_INDEX_BY_ROW_ID[row_id] = len(hist) - 1
        _cfg.save_config(cfg)
    except Exception as e:
        _log.debug("swallowed: %s", e)


# Registry: channel_name -> (row_id, downloaded, metadata, errors,
# start_time). Populated by sync_channel after it emits a [Dwnld] row
# whose transcribed-count may be incomplete (Whisper still running).
# `_flush_batch_stats` in transcribe.py checks this and re-emits the
# [Dwnld] row with the updated transcribed count using the same
# row_id, so the UI updates the existing row in place instead of
# appending a separate [Trnscr].
_RECENT_DWNLD_ROWS: dict[str, dict[str, Any]] = {}
_RECENT_DWNLD_LOCK = threading.Lock()


def register_pending_dwnld_row(channel_name: str, row_id: str,
                                 downloaded: int, metadata: int,
                                 errors: int, elapsed_start: float) -> None:
    """Called by sync_channel right after emit_consolidated_auto_row so
    a subsequent transcribe-complete update can find the row and patch
    its transcribed cell instead of emitting a separate [Trnscr]."""
    with _RECENT_DWNLD_LOCK:
        _RECENT_DWNLD_ROWS[channel_name] = {
            "row_id": row_id,
            "downloaded": int(downloaded or 0),
            "metadata": int(metadata or 0),
            "errors": int(errors or 0),
            "elapsed_start": float(elapsed_start or time.time()),
            "registered_at": time.time(),
        }


def pop_pending_dwnld_row(channel_name: str,
                          max_age_sec: float = 1800.0
                          ) -> dict[str, Any] | None:
    """Fetch + clear this channel's pending [Dwnld] registry entry if
    it exists and is fresher than `max_age_sec` (30 min default).
    Returns None when there's no recent row to update (transcribe
    completion should fall back to emitting a standalone [Trnscr])."""
    with _RECENT_DWNLD_LOCK:
        entry = _RECENT_DWNLD_ROWS.get(channel_name)
        if entry is None:
            return None
        if time.time() - entry.get("registered_at", 0) > max_age_sec:
            _RECENT_DWNLD_ROWS.pop(channel_name, None)
            return None
        _RECENT_DWNLD_ROWS.pop(channel_name, None)
        return entry

