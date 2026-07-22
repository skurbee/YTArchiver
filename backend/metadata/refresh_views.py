"""
metadata.refresh_views â€” bulk views/likes refresh.

Extracted from metadata/refresh.py (Patch 22, v72.4).

Fast view-count refresh via a single flat-playlist call. Compares
against existing JSONL, only full-fetches videos whose counts changed.
The "Refresh views" button on Settings â†’ Metadata drives this.
"""
from __future__ import annotations

import os
import random
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from ..log import get_logger
from ..log_stream import LogStreamer
from ..ytarchiver_config import ConfigUnchanged
from .io import (
    _folder_for_channel,
    _lock_for,
    _read_metadata_jsonl,
    _write_metadata_jsonl,
)
from ..sync import find_yt_dlp
from ..text_utils import normalize_title as _canon_norm_title
from ..utils import sqlite_like_escape as _like_esc
from ._refresh_proxies import (
    _enter_pause_wait,
    _exit_pause_wait,
    _flat_playlist_bulk_stats,
)
from .fetcher import (
    fetch_single_video_metadata,
)
from .scan import _scan_channel_videos

_log = get_logger(__name__)


def _utc_fetched_at_now() -> str:
    return datetime.now(UTC).isoformat()


def _build_refresh_summary_segments(
    *,
    name: str,
    full_fetched: int,
    updated_in_place: int,
    skipped_same: int,
    full_errors: int,
    no_meta_entry: int,
    disk_count: int,
    bulk_count: int,
    took: float,
) -> list[list[str]]:
    """Build the final tagged user-facing refresh summary line."""
    err_color = "red" if full_errors else "simpleline_pink"
    tagged: list[list[str]] = [
        [" \u2014 ", "meta_bracket"],
        [f"{name}: ", "simpleline"],
    ]
    first = True
    emitted_something = False

    def sep() -> None:
        nonlocal first
        if not first:
            tagged.append([" \u00b7 ", "simpleline"])

    def add_count(value: int, label: str, color: str = "simpleline_pink",
                  label_color: str = "simpleline") -> None:
        nonlocal first, emitted_something
        sep()
        tagged.append([f"{value}", color])
        tagged.append([label, label_color])
        first = False
        emitted_something = True

    if full_fetched:
        add_count(full_fetched, " with updated counts")
    if updated_in_place:
        add_count(updated_in_place, " counts updated in place")
    if skipped_same:
        add_count(skipped_same, " unchanged")
    if full_errors:
        add_count(full_errors, " errors", err_color, err_color)
    if no_meta_entry and not full_fetched:
        add_count(no_meta_entry, " need first fetch")
    if not emitted_something:
        if disk_count == 0:
            tagged.append(["no videos on disk for this channel",
                           "simpleline"])
        elif bulk_count == 0:
            tagged.append(["channel returned no videos", "simpleline"])
        else:
            tagged.append([
                f"no matches ({disk_count} on disk vs {bulk_count} from "
                f"YouTube \u2014 titles too divergent to match)",
                "simpleline",
            ])
    tagged.append([f" (took {took:.1f}s)\n", "simpleline"])
    return tagged


def _classify_video_counts(stats: dict[str, Any], old: dict[str, Any],
                           full_fetch_on_change: bool) -> dict[str, Any]:
    """Pure per-video count-diff decision (T328 extraction).

    Given the bulk flat-playlist `stats` and the existing `old` metadata
    entry, decide whether anything moved enough to warrant work. Does NOT
    mutate `old`. Returns the new/old count values plus:
      - changed: a count we trust actually differs
      - no_flat_data: flat mode returned no view_count but we have one
        stored, so we can't tell — force a full fetch to learn the truth
      - decision: the verbose-mode human label
      - action: "full_fetch" | "in_place" | "skip"

    This is the function with the longest historical bug trail; isolating
    it makes the changed / no-flat-data / skip logic unit-testable without
    the full yt-dlp + JSONL + DB I/O stack.
    """
    view_new = stats.get("view_count")
    like_new = stats.get("like_count")
    comment_new = stats.get("comment_count")
    view_old = old.get("view_count")
    like_old = old.get("like_count")
    comment_old = old.get("comment_count")
    changed = False
    if view_new is not None and view_new != view_old:
        changed = True
    # like_count often missing in flat mode; only flag if it's explicitly
    # different (not when the old had a real value and the new is None —
    # that's a bulk-mode gap, not a real drop).
    if (like_new is not None and like_old is not None
            and like_new != like_old):
        changed = True
    if (comment_new is not None and comment_old is not None
            and comment_new != comment_old):
        changed = True
    # "No flat data": flat-playlist returned None for view_count BUT we
    # have a stored value, so we can't tell whether it changed. Route it
    # through the per-video fetch path to get a current count.
    no_flat_data = (view_new is None and view_old is not None)
    if no_flat_data:
        decision = "no flat data → full fetch"
        action = "full_fetch"
    elif changed and full_fetch_on_change:
        decision = "changed → full fetch"
        action = "full_fetch"
    elif changed:
        decision = "changed → in-place update"
        action = "in_place"
    else:
        decision = "unchanged → skip"
        action = "skip"
    return {
        "view_new": view_new, "like_new": like_new,
        "comment_new": comment_new,
        "view_old": view_old, "like_old": like_old,
        "changed": changed, "no_flat_data": no_flat_data,
        "decision": decision, "action": action,
    }


def bulk_refresh_views_likes(channel: dict[str, Any],
                              stream: LogStreamer,
                              cancel_event: threading.Event | None = None,
                              pause_event: threading.Event | None = None,
                              scope: dict[str, Any] | None = None,
                              full_fetch_on_change: bool = False,
                              queues=None,
                              ) -> dict[str, Any]:
    """Fast view-count refresh path. Uses one flat-playlist call to
    get per-video view/like/comment counts, compares against the
    existing metadata.jsonl, and only does a full --dump-json fetch
    for videos whose counts actually changed (to also pick up updated
    top-comments, descriptions, etc.).

    `full_fetch_on_change=False` skips even that second pass and just
    updates the count fields in-place â€” useful for "i only care about
    the view count, don't waste any more yt-dlp calls" flows.

    `scope={"year": N}` honors the year-scoped refresh introduced for
    the Browse grid year-head right-click. `scope={"days": N}` limits
    the actual refresh work to videos uploaded in the last N days.

    Returns the same shape as fetch_channel_metadata so the sync
    worker's summary-parser keeps working: `{ok, fetched, refreshed,
    errors, skipped, bulk_fetched}`. `bulk_fetched` is new and lets
    callers know how many videos were resolved via the fast path
    (= "considered" rather than "re-fetched").
    """
    folder = _folder_for_channel(channel)
    if folder is None:
        stream.emit_error("Archive folder isn't configured. Set it in Settings â†’ General.")
        return {"ok": False, "error": "no output_dir"}
    folder.mkdir(parents=True, exist_ok=True)

    yt = find_yt_dlp()
    if not yt:
        stream.emit_error("Can't refresh video info â€” the download tool (yt-dlp) isn't installed.")
        return {"ok": False, "error": "yt-dlp missing"}

    name = channel.get("name") or channel.get("folder") or "?"
    ch_url = (channel.get("url") or "").strip()
    if not ch_url:
        stream.emit_error(
            f"Metadata: {name} has no URL â€” can't refresh.")
        return {"ok": False, "error": "no url"}

    _scope_year: int | None = None
    if scope and isinstance(scope.get("year"), int):
        _scope_year = int(scope["year"])
    _scope_days: int | None = None
    if scope and scope.get("days") is not None:
        try:
            _d = int(scope.get("days"))
            if _d > 0:
                _scope_days = _d
        except (TypeError, ValueError):
            _scope_days = None
    _scope_bits: list[str] = []
    if _scope_year is not None:
        _scope_bits.append(f"{_scope_year} only")
    if _scope_days is not None:
        _scope_bits.append(f"last {_scope_days}d")
    _banner = f" ({', '.join(_scope_bits)})" if _scope_bits else ""
    # Log kept user-friendly â€” previously said "(flat-playlist)" which
    # is an implementation detail (yt-dlp mode) the user doesn't need
    # to see in Simple mode.
    # Tag the per-channel transitional emits with `views_refresh_progress`
    # so each replaces the previous in-place. Cleared via clear_line just
    # before the final summary so Simple mode ends with the channel
    # header + the catalog-walk counter + the summary, no transitional
    # noise. (User asked: "should look like [3 lines], just delete those
    # lines when all is finished".)
    stream.emit([["  \u2014 ", ["meta_bracket", "views_refresh_progress"]],
                 [f"Refreshing {name}{_banner}...\n",
                  ["simpleline", "views_refresh_progress"]]])

    t0 = time.time()
    # Heartbeat thread â€” re-emits the in-place "Refreshing X..." line
    # every 3s with elapsed time + current sub-phase so the user always
    # sees motion. Without this the line sits silent while yt-dlp spins
    # up + walks the catalog (many seconds on cold-cookie firefox; can
    # be 30s+ before any output streams).
    _hb_phase = ["fetching catalog from YouTube"]  # mutable holder
    _hb_catalog_count = [0]  # running count from _flat_playlist_bulk_stats
    _hb_alive = [True]
    def _heartbeat():
        from ..utils import format_elapsed as _fmt_el
        while _hb_alive[0]:
            time.sleep(3)
            if not _hb_alive[0]:
                break
            try:
                _el = int(time.time() - t0)
                # Fold the catalog count into the phase string when known
                # so the user sees ONE active line per channel, not two.
                _phase = _hb_phase[0]
                if _hb_catalog_count[0] > 0:
                    _phase = (f"{_phase} \u00b7 "
                              f"{_hb_catalog_count[0]:,} videos in catalog")
                stream.emit([
                    ["  \u2014 ", ["meta_bracket", "views_refresh_progress"]],
                    [f"Refreshing {name}{_banner} \u2014 {_phase} ({_fmt_el(_el)})\n",
                     ["simpleline", "views_refresh_progress"]],
                ])
            except Exception as e:
                _log.debug("swallowed: %s", e)
    _hb_thread = threading.Thread(target=_heartbeat, daemon=True)
    _hb_thread.start()
    class _HeartbeatGuard:
        def __init__(self, alive):
            self._alive = alive

        def stop(self):
            self._alive[0] = False

        def __del__(self):
            self.stop()

    _hb_guard = _HeartbeatGuard(_hb_alive)
    # Cap the heartbeat at a hard deadline (30 min) and watch the
    # current thread's liveness so it can't leak forever if the
    # function body raises before reaching the normal `_hb_alive[0]
    # = False` teardown (audit: refresh_views H76). The
    # parent-thread watch is cheap and bounded.
    _parent_thread = threading.current_thread()
    _hb_start_ts = time.time()
    def _hb_self_kill():
        while _hb_alive[0]:
            time.sleep(5)
            if (not _parent_thread.is_alive()
                    or (time.time() - _hb_start_ts) > 1800):
                _hb_alive[0] = False
                break
    threading.Thread(target=_hb_self_kill, daemon=True,
                     name="hb-watchdog").start()

    def _catalog_progress(n):
        _hb_catalog_count[0] = int(n)

    bulk = _flat_playlist_bulk_stats(yt, ch_url, stream,
                                     cancel_event, pause_event,
                                     queues=queues,
                                     progress_cb=_catalog_progress)
    bulk_all = bulk
    # Lock in the final catalog count once the walk completes.
    _hb_catalog_count[0] = max(_hb_catalog_count[0], len(bulk))
    _hb_phase[0] = "matching local files"
    if not bulk:
        _hb_guard.stop()
        # Replace the in-place "Refreshing X..." line with this warning
        # so the user sees the failure cleanly instead of two side-by-
        # side lines (the warning + the orphaned Refreshing... line).
        stream.emit([
            [" \u26A0 ", ["meta_bracket", "views_refresh_progress"]],
            [f"Initial check unsuccessful for {name} â€” "
             f"trying per-video lookup...\n",
             ["simpleline", "views_refresh_progress"]],
        ])
        # Verbose-only diagnostic. `dim` tag is in VERBOSE_ONLY_TAGS, so
        # Simple mode hides this line entirely while Verbose users still
        # see the technical context. Design rule: Simple mode should
        # be easily readable for someone with no technical context;
        # Verbose mode is where the noisy diagnostics belong.
        stream.emit([
            ["   â€” ", ["dim", "views_refresh_progress"]],
            [f"Bulk-stats returned no data for {name} â€” "
             f"channel may be empty / private / geo-locked, or yt-dlp "
             f"hit a transient YouTube block.\n",
             ["dim", "views_refresh_progress"]],
        ])
        return {"ok": False, "error": "bulk_empty",
                "fetched": 0, "refreshed": 0, "errors": 0, "skipped": 0,
                "bulk_fetched": 0}
    if _scope_days is not None:
        cutoff = datetime.now(UTC) - timedelta(days=_scope_days)

        def _is_recent_upload(stats: dict[str, Any]) -> bool:
            raw = str((stats or {}).get("upload_date") or "").strip()
            if len(raw) != 8 or not raw.isdigit():
                return False
            try:
                return datetime.strptime(raw, "%Y%m%d").replace(tzinfo=UTC) >= cutoff
            except ValueError:
                return False

        bulk = {vid: stats for vid, stats in bulk.items()
                if _is_recent_upload(stats)}
        if not bulk:
            _hb_guard.stop()
            stream.emit([
                [" \u2014 ", ["meta_bracket", "views_refresh_progress"]],
                [f"{name}: no videos in last {_scope_days}d.\n",
                 ["simpleline", "views_refresh_progress"]],
            ])
            return {"ok": True, "fetched": 0, "refreshed": 0,
                    "errors": 0, "skipped": 0, "bulk_fetched": 0}

    # Verbose-only: announce the bulk-stats walk landed and how many
    # videos it found. Helps the user follow the multi-phase flow.
    stream.emit([
        ["   â€” ", ["dim"]],
        [f"bulk-stats: {len(bulk):,} videos retrieved from YouTube "
         f"catalog (took {time.time() - t0:.1f}s)\n", ["dim"]],
    ])

    # Enumerate on-disk videos so we only refresh ones we actually
    # have files for (mirrors fetch_channel_metadata's disk-driven
    # philosophy â€” never pay yt-dlp time for playlist entries with
    # no archive file).
    on_disk = _scan_channel_videos(folder)
    if _scope_year is not None:
        on_disk = [v for v in on_disk if v[2] == _scope_year]

    # Title-fallback resolution. The default archive layout has NO
    # `[video_id]` bracket in filenames AND many legacy-tkinter-era
    # registrations landed in the videos-table with video_id=NULL.
    # Without a second matching strategy every file shows as "missing"
    # and the whole bulk pass reports "no matches". Fix: build a
    # normalized-title â†’ video_id map from the bulk data and resolve
    # empty vid_ids via title lookup. The normalization aggressively
    # folds whitespace and punctuation so minor filesystem sanitization
    # differences (en-dash â†’ hyphen, colons dropped, etc.) still match.
    # closure delegates to text_utils.normalize_title with
    # the alnum-only + strip-id-bracket modes set. Trailing-punct strip
    # is OFF because the matcher distinguishes "title?" from "title".
    def _norm_title(s: str) -> str:
        return _canon_norm_title(
            s,
            strip_trailing_punct=False,
            strip_id_bracket=True,
            alnum_only=True,
        )

    _title_to_vid: dict[str, str] = {}
    _ambiguous_titles: set = set()
    for _vid, _stats in bulk.items():
        _nt = _norm_title(_stats.get("title") or "")
        if not _nt:
            continue
        if _nt in _title_to_vid and _title_to_vid[_nt] != _vid:
            _ambiguous_titles.add(_nt)
        else:
            _title_to_vid[_nt] = _vid

    # Resolve vid_ids for on-disk tuples that came back empty. Track
    # which (filepath, video_id) pairs we backfilled so we can persist
    # them to the index DB after the scan â€” next run skips the title
    # match entirely because the DB lookup at _scan_channel_videos
    # fills fp_to_id.
    _title_resolved: list[tuple[str, str, str]] = []  # (fp, vid, title)
    _resolved_on_disk = []
    for (_v, _t, _y, _m, _fp) in on_disk:
        if _v:
            _resolved_on_disk.append((_v, _t, _y, _m, _fp))
            continue
        _nt = _norm_title(_t)
        if not _nt or _nt in _ambiguous_titles:
            _resolved_on_disk.append((_v, _t, _y, _m, _fp))
            continue
        _guess = _title_to_vid.get(_nt, "")
        if _guess:
            _resolved_on_disk.append((_guess, _t, _y, _m, _fp))
            _title_resolved.append((_fp, _guess, _t))
        else:
            _resolved_on_disk.append((_v, _t, _y, _m, _fp))
    on_disk = _resolved_on_disk

    # Backfill resolved video_ids into the videos-table so future
    # bulk refreshes skip the title-match dance.
    if _title_resolved:
        try:
            from .. import index as _idx
            _conn = _idx._open()
            if _conn is not None:
                with _idx._db_lock:
                    try:
                        for _fp, _vid, _ttl in _title_resolved:
                            _vurl = f"https://www.youtube.com/watch?v={_vid}"
                            try:
                                _conn.execute(
                                    "UPDATE videos SET video_id=?, video_url=? "
                                    "WHERE filepath=? COLLATE NOCASE "
                                    "AND (video_id IS NULL OR video_id='')",
                                    (_vid, _vurl, _fp))
                            except Exception as e:
                                _log.debug("swallowed: %s", e)
                        _conn.commit()
                    except Exception as _ce:
                        # Roll back the whole title-resolve batch if
                        # commit fails (e.g. disk full mid-flush).
                        # Without this, some files end up with video_id
                        # set and others don't, and the in-memory
                        # on_disk list disagrees with the DB (audit:
                        # metadata/refresh_views.py:264-273).
                        try: _conn.rollback()
                        except Exception: pass
                        _log.error("title-resolve commit failed: %s", _ce)
        except Exception as e:
            _log.warning("views refresh title-resolution DB update failed "
                         "for %r: %s", name, e)
        # User-friendly wording: dropped "(no [id] in filename)"
        # technicality â€” users shouldn't have to know about the
        # internal DB state to understand what happened.
        stream.emit([
            [" \u2014 ", ["meta_bracket", "views_refresh_progress"]],
            [f"Matched {len(_title_resolved)} video(s) by title "
             f"\u2014 saved their YouTube IDs.\n",
             ["simpleline", "views_refresh_progress"]],
        ])

    # \u2500\u2500 Removed-from-YT detection (cheap, runs every bulk refresh) \u2500\u2500
    # Walk the resolved on-disk list: any local file whose video_id
    # is NOT in the flat-playlist response was deleted / privated /
    # unlisted by the channel since download. Stamp the row so the UI
    # can show a red \u2717 on the per-video tile + a channel-level "N
    # gone from YT" counter. Inverse: any file currently marked
    # removed whose vid HAS returned to the catalog gets the
    # timestamp cleared (uploader restored / unprivated the video).
    # Only run detection on a COMPLETE catalog. A partial walk (cancel
    # mid-walk, 60s stall, yt-dlp dying after streaming some rows)
    # would make "not in bulk" flag thousands of perfectly-live videos
    # as removed from YouTube.
    _catalog_complete = bool(getattr(bulk_all, "complete", False))
    if not _catalog_complete:
        try:
            stream.emit_dim(
                " (catalog walk incomplete â€” skipping removed-from-YT "
                "detection this pass)")
        except Exception:
            pass
    try:
        _now_rm = time.time()
        _newly_removed: list[str] = []
        _newly_restored: list[str] = []
        from .. import index as _idx
        # Read-only state lookup â€” reader path avoids queueing behind sweep.
        _conn_rm = _idx._reader_open()
        if _catalog_complete and _conn_rm is not None:
            # Normalize the folder path to the SAME canonical form that
            # videos.filepath rows were inserted with (os.path.normpath
            # in register_video). Old _like_esc(str(folder)) used the
            # Path's str form which can have forward-slash separators
            # on Windows when the Path was constructed from a forward-
            # slash source â€” leaving the LIKE pattern mismatched
            # against backslash-stored DB rows (audit: refresh_views.
            # py:294-352).
            _folder_norm = os.path.normpath(str(folder))
            _pat = _like_esc(_folder_norm) + "%"
            _db_state: dict[str, tuple[str | None, float | None]] = {}
            with _idx._reader_lock:
                for _row in _conn_rm.execute(
                        "SELECT filepath, video_id, removed_from_yt_ts "
                        "FROM videos WHERE filepath LIKE ? ESCAPE '\\'", (_pat,)):
                    _db_state[os.path.normpath(_row[0])] = (
                        _row[1], _row[2])
            for (_v, _t, _y, _m, _fp) in on_disk:
                if not _v:
                    continue
                _key = os.path.normpath(_fp)
                _db_vid, _db_removed_ts = _db_state.get(_key, (None, None))
                _is_in_catalog = (_v in bulk_all)
                if not _is_in_catalog and _db_removed_ts is None:
                    _newly_removed.append(_fp)
                elif _is_in_catalog and _db_removed_ts is not None:
                    _newly_restored.append(_fp)
            if _newly_removed or _newly_restored:
                # Writes must go through the writer connection. The
                # previous code re-used the reader connection (_conn_rm)
                # for these UPDATEs, which on WAL with query_only=ON
                # either fails outright or bypasses the writer's
                # busy-timeout discipline. Use _open() for writes and
                # serialize via _db_lock.
                _conn_wr = _idx._open()
                if _conn_wr is not None:
                    with _idx._db_lock:
                        try:
                            # Scope by channel too â€” filepath alone is
                            # not a unique identity in a multi-channel
                            # archive on case-insensitive NTFS / pooled
                            # drives. A path collision across channels
                            # without the channel scope would mis-flag
                            # the wrong channel's video as removed
                            # (audit: refresh_views.py C14).
                            for _fp in _newly_removed:
                                try:
                                    _conn_wr.execute(
                                        "UPDATE videos SET removed_from_yt_ts=? "
                                        "WHERE filepath=? COLLATE NOCASE "
                                        "AND channel=?",
                                        (_now_rm, _fp, name))
                                except Exception as e:
                                    _log.debug("swallowed: %s", e)
                            for _fp in _newly_restored:
                                try:
                                    _conn_wr.execute(
                                        "UPDATE videos SET removed_from_yt_ts=NULL "
                                        "WHERE filepath=? COLLATE NOCASE "
                                        "AND channel=?",
                                        (_fp, name))
                                except Exception as e:
                                    _log.debug("swallowed: %s", e)
                            _conn_wr.commit()
                        except Exception as _we:
                            try: _conn_wr.rollback()
                            except Exception as e: _log.debug("swallowed: %s", e)
                            _log.error("removed_from_yt UPDATE failed: %s", _we)
                if _newly_removed:
                    stream.emit([
                        [" \u26a0 ", ["meta_bracket", "views_refresh_progress"]],
                        [f"{len(_newly_removed)} video(s) no longer on "
                         f"YouTube (removed / privated since last sync).\n",
                         ["simpleline", "views_refresh_progress"]],
                    ])
                if _newly_restored:
                    stream.emit([
                        [" \u2713 ", ["meta_bracket", "views_refresh_progress"]],
                        [f"{len(_newly_restored)} previously-removed video(s) "
                         f"are back on YouTube.\n",
                         ["simpleline", "views_refresh_progress"]],
                    ])
    except Exception as _rm_e:
        try:
            stream.emit_error(
                f"removed-from-YT detection failed for {name}: {_rm_e}")
        except Exception as e:
            _log.warning("views refresh removed-video DB stamp failed "
                         "for %r: %s", name, e)

    on_disk_ids = {v[0] for v in on_disk if v[0]}
    # Count duplicate video_ids (same id on disk in two folders â€” usually
    # a manual file copy / channel-merge leftover). Set comprehension
    # silently drops duplicates; count them explicitly so the user can
    # see the inconsistency (audit: refresh_views H77).
    _on_disk_vid_count = sum(1 for v in on_disk if v[0])
    if _on_disk_vid_count > len(on_disk_ids):
        try:
            stream.emit_dim(
                f" ({_on_disk_vid_count - len(on_disk_ids)} duplicate "
                f"video_id(s) on disk â€” same video in multiple folders)")
        except Exception:
            pass

    # Load existing metadata across all on-disk JSONLs, keyed by id.
    # Track which JSONL each entry came from so we can write it back.
    existing_by_id: dict[str, dict[str, Any]] = {}
    jsonl_by_id: dict[str, str] = {}
    # Scope by channel-name prefix so a sibling channel's JSONL that
    # accidentally lives under this tree doesn't pollute existing_by_id
    # (audit: refresh_views H100 â€” same fix as C16/refresh_fetch).
    _expected_prefix = f".{name} "
    _expected_exact = f".{name} Metadata.jsonl"
    for dp, _dns, fns in os.walk(str(folder)):
        for fn in fns:
            if not fn.endswith("Metadata.jsonl"):
                continue
            if fn != _expected_exact and not fn.startswith(_expected_prefix):
                continue
            jp = os.path.join(dp, fn)
            try:
                entries = _read_metadata_jsonl(jp)
            except Exception as _re:
                try:
                    stream.emit_dim(f" (jsonl read failed: {fn} â€” {_re})")
                except Exception:
                    pass
                continue
            for vid, entry in entries.items():
                existing_by_id[vid] = entry
                jsonl_by_id[vid] = jp

    # Walk the bulk data, update or flag-for-fetch per video.
    changed_ids: list[str] = []       # to re-fetch via --dump-json
    updated_in_place = 0              # just bumped counts in existing entry
    skipped_same = 0                  # counts unchanged, nothing to do
    missing_on_disk = 0               # in bulk but no on-disk file
    no_meta_entry = 0                 # on disk but no existing metadata

    for vid, stats in bulk.items():
        if vid not in on_disk_ids:
            missing_on_disk += 1
            continue
        old = existing_by_id.get(vid)
        if old is None:
            # Haven't fetched this video's full metadata yet â€” always
            # full-fetch it regardless of full_fetch_on_change. We
            # literally have no record for this video, so there's
            # nothing to "update in place" â€” we have to do the full
            # --dump-json to create the entry. full_fetch_on_change
            # only governs whether CHANGED-COUNT entries also get
            # re-fetched (which re-pulls comments too, so the
            # views/likes refresh path now sets it False to keep
            # comments out of scope).
            no_meta_entry += 1
            changed_ids.append(vid)
            continue
        # Decide whether anything moved enough to warrant a full fetch.
        # Pure classifier (T328 — see _classify_video_counts); the loop
        # below only applies the result + emits the verbose trace.
        _dec = _classify_video_counts(stats, old, full_fetch_on_change)
        _view_new = _dec["view_new"]
        _like_new = _dec["like_new"]
        _comment_new = _dec["comment_new"]
        _view_old = _dec["view_old"]
        _like_old = _dec["like_old"]
        _changed = _dec["changed"]
        _no_flat_data = _dec["no_flat_data"]
        # Only bump in-place counters + fetched_at when something
        # actually changed. The previous "always bump fetched_at"
        # path meant every "no-op" refresh rewrote EVERY metadata
        # jsonl on disk (10k-video archive = thousands of MB of churn
        # + DrivePool I/O + mtime bumps that defeated downstream
        # fingerprint caches). Now unchanged vids stay untouched.
        if _view_new is not None:
            old["view_count"] = _view_new
        if _like_new is not None:
            old["like_count"] = _like_new
        if _comment_new is not None:
            old["comment_count"] = _comment_new
        if _changed or _no_flat_data:
            old["fetched_at"] = _utc_fetched_at_now()
            old["_dirty"] = True

        # VERBOSE-ONLY per-video diff trace. Compact one-liner showing
        # the oldâ†’new counts and the decision. With 1000s of videos
        # per channel this floods the log â€” that's intentional in
        # Verbose mode â€” Verbose is intentionally noisy. Simple mode
        # hides via `dim` tag.
        def _fmt_cnt(n):
            return "â€”" if n is None else f"{n:,}"
        _decision = _dec["decision"]
        stream.emit([
            ["    â€” ", ["dim"]],
            [f"{vid} Â· views {_fmt_cnt(_view_old)}â†’{_fmt_cnt(_view_new)} Â· "
             f"likes {_fmt_cnt(_like_old)}â†’{_fmt_cnt(_like_new)} Â· "
             f"{_decision}\n", ["dim"]],
        ])

        # Route by the classifier's action (T328). "full_fetch" covers both
        # no-flat-data (only path to a current view count) and changed-with-
        # full_fetch_on_change; "in_place" bumped counts already; "skip" is
        # an unchanged no-op.
        if _dec["action"] == "full_fetch":
            changed_ids.append(vid)
        elif _dec["action"] == "in_place":
            updated_in_place += 1
        else:
            skipped_same += 1

    # Persist the in-place-updated entries. Group by jsonl path so we
    # only rewrite each file once. Skip entries that aren't actually
    # dirty â€” see `_dirty` flag set above. The previous code added
    # every video in existing_by_id to dirty_paths, churning every
    # jsonl on disk even for a no-op refresh.
    _hb_phase[0] = "writing updated counts"
    dirty_paths: dict[str, dict[str, dict[str, Any]]] = {}
    for vid, entry in existing_by_id.items():
        if not entry.get("_dirty"):
            continue
        jp = jsonl_by_id.get(vid)
        if jp is None:
            continue
        # Strip the transient _dirty marker before persisting.
        _entry_clean = {k: v for k, v in entry.items() if k != "_dirty"}
        dirty_paths.setdefault(jp, {})[vid] = _entry_clean
    for jp, entries in dirty_paths.items():
        # Load current contents, merge our updates on top, rewrite.
        # Hold the per-path write lock across read+merge+write so a
        # concurrent writer's just-landed entry can't be clobbered by
        # our stale read. Without this, _write_metadata_jsonl's
        # internal lock only serialized the WRITE half â€” two threads
        # could read, both miss the other's changes, then both write
        # (audit: refresh_views.py C15).
        with _lock_for(jp):
            try:
                full = _read_metadata_jsonl(jp, strict=True)
            except Exception as e:
                stream.emit_dim(
                    f" (metadata read failed for {os.path.basename(jp)}; "
                    f"skipping rewrite: {e})")
                continue
            full.update(entries)
            try:
                _write_metadata_jsonl(jp, full)
                # Mirror the refreshed view/like counts into the index DB so
                # the global Videos view can sort the whole archive by
                # views/likes off an indexed column (no sidecar walk at
                # query time). Best-effort â€” never block the refresh.
                try:
                    from .. import index as _idx_db
                    _idx_db.update_video_stats(
                        [(vid, e.get("view_count"), e.get("like_count"),
                          e.get("upload_date"))
                         for vid, e in entries.items() if vid])
                except Exception as _se:
                    _log.debug("index stats mirror failed: %s", _se)
            except OSError as e:
                stream.emit_dim(f" (metadata write failed for {os.path.basename(jp)}: {e})")

    # Secondary pass: full --dump-json fetch for videos whose counts
    # changed (picks up new comments, updated descriptions, etc.).
    # Reuses the existing per-video fetch_single_video_metadata path
    # so the logging + error handling stays consistent.
    full_fetched = 0
    full_errors = 0
    if changed_ids:
        # With full_fetch_on_change=False (the views/likes-refresh
        # default), this list only contains videos that had NO
        # existing metadata entry â€” we're filling in first-time
        # metadata, not comment refresh. Wording updated so a user
        # who clicked "Refresh views/likes" doesn't see a line
        # claiming we're pulling comments.
        _n = len(changed_ids)
        _what = ("new entries \u2014 fetching full metadata..."
                 if not full_fetch_on_change
                 else "have updated counts \u2014 re-fetching details...")
        _hb_phase[0] = (f"fetching full metadata for {_n} new entries"
                        if not full_fetch_on_change
                        else f"re-fetching details for {_n} updated videos")
        stream.emit([
            [" \u2014 ", ["meta_bracket", "views_refresh_progress"]],
            [f"{_n} video(s) {_what}\n",
             ["simpleline", "views_refresh_progress"]],
        ])
        # Build a video_id â†’ (filepath, title) map from on_disk so we
        # can pass filepath + title_hint to fetch_single_video_metadata
        # (signature: channel, video_id, file_path, title_hint, stream).
        # The prior call had `stream` in the title_hint slot AND passed
        # a nonexistent `cancel_event` kwarg â€” every per-video fetch
        # raised TypeError and got caught as an error (users reported
        # 40/40 errors on a test channel). Fixed by passing args in
        # the right order; cancel/pause are still honored by the
        # wrapping loop.
        fp_by_id: dict[str, tuple[str, str]] = {}
        for (_v, _t, _y, _m, _fp) in on_disk:
            if _v and _fp:
                fp_by_id[_v] = (_fp, _t or "")
        # Progress tick: emit a dim "[N/total] processed" line every
        # _PROGRESS_TICK_EVERY videos OR every _PROGRESS_TICK_SECS
        # so a 600-video channel doesn't look stuck for an hour
        # between the initial "N video(s) have updated counts..."
        # line and the final summary. User flagged this as "refresh
        # views got stuck" on Bernie Sanders (610 videos).
        _PROGRESS_TICK_EVERY = 25
        _PROGRESS_TICK_SECS = 20.0
        _last_tick_ts = time.time()
        _processed = 0
        _total = len(changed_ids)
        for _j, vid in enumerate(changed_ids, 1):
            if cancel_event is not None and cancel_event.is_set():
                break
            if _j > 1:
                time.sleep(random.uniform(0.3, 0.8))
                if cancel_event is not None and cancel_event.is_set():
                    break
            # Pause-wait between videos. The user might have clicked
            # Pause minutes ago â€” they're waiting on this exact loop
            # to land here. Emit a Paused log line + signal active.
            if pause_event is not None and pause_event.is_set():
                _enter_pause_wait(stream, f"{name} (metadata refresh)", queues)
                while (pause_event.is_set()
                       and not (cancel_event is not None and cancel_event.is_set())):
                    time.sleep(0.25)
                _exit_pause_wait(stream, f"{name} (metadata refresh)", queues)
            _pair = fp_by_id.get(vid)
            if not _pair:
                _processed += 1
                continue
            fp, title_hint = _pair
            try:
                # refresh=True is REQUIRED here: changed-count and
                # _no_flat_data videos by definition already have a
                # jsonl entry, and without the flag the fetcher's
                # existing-entry guard returned {ok, skipped} without
                # fetching â€” so "full fetch on change" never actually
                # updated comments/descriptions and _no_flat_data
                # videos kept stale counts forever, while the summary
                # claimed success.
                res = fetch_single_video_metadata(
                    channel, vid, fp, title_hint, stream,
                    emit_inline_log=False, refresh=True)
                if res.get("ok") and not res.get("skipped"):
                    full_fetched += 1
                elif not res.get("ok") and not res.get("transient"):
                    full_errors += 1
            except Exception as _e:
                stream.emit_dim(f" (full fetch failed for {vid}: {_e})")
                full_errors += 1
            _processed += 1
            # Update the heartbeat phase with [N/total] so the
            # 3-second heartbeat tick shows live progress.
            _hb_phase[0] = f"refreshing metadata [{_processed}/{_total}]"
            _last_tick_ts = time.time()

    # Stamp last-refresh timestamp on the channel config. Separate
    # from per-video fetched_at so the Subs UI can say "refreshed
    # N minutes ago" for the whole channel.
    try:
        from .. import ytarchiver_config as _cfg
        with _cfg.config_transaction() as cfg:
            ch_url_norm = ch_url.rstrip("/")
            now_ts = time.time()
            matched = False
            for ch in cfg.get("channels", []):
                if (ch.get("url") or "").rstrip("/") == ch_url_norm:
                    ch["last_views_refresh_ts"] = now_ts
                    matched = True
                    break
            if not matched:
                raise _cfg.ConfigUnchanged()
    except ConfigUnchanged:
        pass
    except Exception as e:
        _log.warning("views refresh timestamp stamp failed for %r: %s",
                     name, e)

    # Stop the heartbeat thread BEFORE the clear_line + summary so
    # the in-place line doesn't get re-painted on top of the summary.
    _hb_guard.stop()
    took = time.time() - t0
    # Drop all the per-channel transitional lines tagged with
    # `views_refresh_progress` ("Refreshing X...", "N video(s) have
    # updated counts...", "[N/M] fetching metadata..."). The summary
    # line below stays as the only post-completion artifact alongside
    # the catalog-walk counter (which uses `backfill_progress` and
    # is preserved). Mirrors the same clear_line pattern used by
    # backfill_video_ids' final summary.
    try:
        import json as _json
        stream.emit([[_json.dumps({
            "kind": "clear_line", "marker": "views_refresh_progress"}),
            "__control__"]])
    except Exception as e:
        _log.debug("swallowed: %s", e)
    # Tagged emit: channel name + labels render white, counts render
    # pink, errors red. Previously the whole line was one pink blob
    # which users called out as visual noise ("channel name should be
    # white, labels should be white, only the numbers highlight").
    # "via bulk path" dropped â€” user-facing log doesn't need to
    # surface the internal code path.
    stream.emit(_build_refresh_summary_segments(
        name=name,
        full_fetched=full_fetched,
        updated_in_place=updated_in_place,
        skipped_same=skipped_same,
        full_errors=full_errors,
        no_meta_entry=no_meta_entry,
        disk_count=len(on_disk),
        bulk_count=len(bulk),
        took=took,
    ))
    return {
        "ok": True,
        "fetched": no_meta_entry,
        "refreshed": full_fetched + updated_in_place,
        "errors": full_errors,
        "skipped": skipped_same,
        "bulk_fetched": len(bulk),
        "took": took,
    }



# Patch 19 phase M5 (v69.2): thumbnail/video-id status ops moved out.

