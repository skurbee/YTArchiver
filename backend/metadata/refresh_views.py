"""
metadata.refresh_views — bulk views/likes refresh.

Extracted from metadata/refresh.py (Patch 22, v72.4).

Fast view-count refresh via a single flat-playlist call. Compares
against existing JSONL, only full-fetches videos whose counts changed.
The "Refresh views" button on Settings → Metadata drives this.
"""
from __future__ import annotations

import os
import re
import threading
import time
from datetime import datetime
from typing import Any

from ..log import get_logger
from ..log_stream import LogStreamer
from ..metadata_io import (
    _folder_for_channel,
    _lock_for,
    _read_metadata_jsonl,
    _write_metadata_jsonl,
)
from ..sync import find_yt_dlp
from ..text_utils import normalize_title as _canon_norm_title
from ..utils import sqlite_like_escape as _like_esc
from .fetcher import (
    fetch_metadata_for_videos,
    fetch_single_video_metadata,
)
from .scan import _scan_channel_videos
from ._refresh_proxies import (
    _ID_RE,
    _ID_RE_11,
    _enter_pause_wait,
    _exit_pause_wait,
    _fetch_per_video_upload_dates,
    _flat_playlist_bulk_stats,
    _probe_durations_bulk,
    _probe_file_duration,
    _resolve_channel_id_url,
    _resolve_ids_by_title,
    backfill_video_ids,
    existing_info_ids,
)

_log = get_logger(__name__)


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
    updates the count fields in-place — useful for "i only care about
    the view count, don't waste any more yt-dlp calls" flows.

    `scope={"year": N}` honors the year-scoped refresh introduced for
    the Browse grid year-head right-click.

    Returns the same shape as fetch_channel_metadata so the sync
    worker's summary-parser keeps working: `{ok, fetched, refreshed,
    errors, skipped, bulk_fetched}`. `bulk_fetched` is new and lets
    callers know how many videos were resolved via the fast path
    (= "considered" rather than "re-fetched").
    """
    folder = _folder_for_channel(channel)
    if folder is None:
        stream.emit_error("Archive folder isn't configured. Set it in Settings → General.")
        return {"ok": False, "error": "no output_dir"}
    folder.mkdir(parents=True, exist_ok=True)

    yt = find_yt_dlp()
    if not yt:
        stream.emit_error("Can't refresh video info — the download tool (yt-dlp) isn't installed.")
        return {"ok": False, "error": "yt-dlp missing"}

    name = channel.get("name") or channel.get("folder") or "?"
    ch_url = (channel.get("url") or "").strip()
    if not ch_url:
        stream.emit_error(
            f"Metadata: {name} has no URL — can't refresh.")
        return {"ok": False, "error": "no url"}

    _scope_year: int | None = None
    if scope and isinstance(scope.get("year"), int):
        _scope_year = int(scope["year"])
    _banner = f" ({_scope_year} only)" if _scope_year is not None else ""
    # Log kept user-friendly — previously said "(flat-playlist)" which
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
    # Heartbeat thread — re-emits the in-place "Refreshing X..." line
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
    # Lock in the final catalog count once the walk completes.
    _hb_catalog_count[0] = max(_hb_catalog_count[0], len(bulk))
    _hb_phase[0] = "matching local files"
    if not bulk:
        _hb_alive[0] = False  # stop heartbeat
        # Replace the in-place "Refreshing X..." line with this warning
        # so the user sees the failure cleanly instead of two side-by-
        # side lines (the warning + the orphaned Refreshing... line).
        stream.emit([
            [" \u26A0 ", ["meta_bracket", "views_refresh_progress"]],
            [f"Initial check unsuccessful for {name} — "
             f"trying per-video lookup...\n",
             ["simpleline", "views_refresh_progress"]],
        ])
        # Verbose-only diagnostic. `dim` tag is in VERBOSE_ONLY_TAGS, so
        # Simple mode hides this line entirely while Verbose users still
        # see the technical context. Design rule: Simple mode should
        # be easily readable for someone with no technical context;
        # Verbose mode is where the noisy diagnostics belong.
        stream.emit([
            ["   — ", ["dim", "views_refresh_progress"]],
            [f"Bulk-stats returned no data for {name} — "
             f"channel may be empty / private / geo-locked, or yt-dlp "
             f"hit a transient YouTube block.\n",
             ["dim", "views_refresh_progress"]],
        ])
        return {"ok": False, "error": "bulk_empty",
                "fetched": 0, "refreshed": 0, "errors": 0, "skipped": 0,
                "bulk_fetched": 0}

    # Verbose-only: announce the bulk-stats walk landed and how many
    # videos it found. Helps the user follow the multi-phase flow.
    stream.emit([
        ["   — ", ["dim"]],
        [f"bulk-stats: {len(bulk):,} videos retrieved from YouTube "
         f"catalog (took {time.time() - t0:.1f}s)\n", ["dim"]],
    ])

    # Enumerate on-disk videos so we only refresh ones we actually
    # have files for (mirrors fetch_channel_metadata's disk-driven
    # philosophy — never pay yt-dlp time for playlist entries with
    # no archive file).
    on_disk = _scan_channel_videos(folder)
    if _scope_year is not None:
        on_disk = [v for v in on_disk if v[2] == _scope_year]

    # Title-fallback resolution. The default archive layout has NO
    # `[video_id]` bracket in filenames AND many legacy-tkinter-era
    # registrations landed in the videos-table with video_id=NULL.
    # Without a second matching strategy every file shows as "missing"
    # and the whole bulk pass reports "no matches". Fix: build a
    # normalized-title → video_id map from the bulk data and resolve
    # empty vid_ids via title lookup. The normalization aggressively
    # folds whitespace and punctuation so minor filesystem sanitization
    # differences (en-dash → hyphen, colons dropped, etc.) still match.
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
    # them to the index DB after the scan — next run skips the title
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
            _log.debug("swallowed: %s", e)
        # User-friendly wording: dropped "(no [id] in filename)"
        # technicality — users shouldn't have to know about the
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
    try:
        _now_rm = time.time()
        _newly_removed: list[str] = []
        _newly_restored: list[str] = []
        from .. import index as _idx
        # Read-only state lookup — reader path avoids queueing behind sweep.
        _conn_rm = _idx._reader_open()
        if _conn_rm is not None:
            # Normalize the folder path to the SAME canonical form that
            # videos.filepath rows were inserted with (os.path.normpath
            # in register_video). Old _like_esc(str(folder)) used the
            # Path's str form which can have forward-slash separators
            # on Windows when the Path was constructed from a forward-
            # slash source — leaving the LIKE pattern mismatched
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
                _is_in_catalog = (_v in bulk)
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
                            # Scope by channel too — filepath alone is
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
            _log.debug("swallowed: %s", e)

    on_disk_ids = {v[0] for v in on_disk if v[0]}
    # Count duplicate video_ids (same id on disk in two folders — usually
    # a manual file copy / channel-merge leftover). Set comprehension
    # silently drops duplicates; count them explicitly so the user can
    # see the inconsistency (audit: refresh_views H77).
    _on_disk_vid_count = sum(1 for v in on_disk if v[0])
    if _on_disk_vid_count > len(on_disk_ids):
        try:
            stream.emit_dim(
                f" ({_on_disk_vid_count - len(on_disk_ids)} duplicate "
                f"video_id(s) on disk — same video in multiple folders)")
        except Exception:
            pass

    # Load existing metadata across all on-disk JSONLs, keyed by id.
    # Track which JSONL each entry came from so we can write it back.
    existing_by_id: dict[str, dict[str, Any]] = {}
    jsonl_by_id: dict[str, str] = {}
    # Scope by channel-name prefix so a sibling channel's JSONL that
    # accidentally lives under this tree doesn't pollute existing_by_id
    # (audit: refresh_views H100 — same fix as C16/refresh_fetch).
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
                    stream.emit_dim(f" (jsonl read failed: {fn} — {_re})")
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
            # Haven't fetched this video's full metadata yet — always
            # full-fetch it regardless of full_fetch_on_change. We
            # literally have no record for this video, so there's
            # nothing to "update in place" — we have to do the full
            # --dump-json to create the entry. full_fetch_on_change
            # only governs whether CHANGED-COUNT entries also get
            # re-fetched (which re-pulls comments too, so the
            # views/likes refresh path now sets it False to keep
            # comments out of scope).
            no_meta_entry += 1
            changed_ids.append(vid)
            continue
        # Decide whether anything moved enough to warrant a full fetch.
        _view_new = stats.get("view_count")
        _like_new = stats.get("like_count")
        _comment_new = stats.get("comment_count")
        _view_old = old.get("view_count")
        _like_old = old.get("like_count")
        _comment_old = old.get("comment_count")
        _changed = False
        if _view_new is not None and _view_new != _view_old:
            _changed = True
        # like_count often missing in flat mode; only flag if it's
        # explicitly different (not when the old had a real value and
        # the new is None — that's a bulk-mode gap, not a real drop).
        if (_like_new is not None and _like_old is not None
                and _like_new != _like_old):
            _changed = True
        if (_comment_new is not None and _comment_old is not None
                and _comment_new != _comment_old):
            _changed = True

        # "No flat data" detection (2026-05-14 fix): if flat-playlist
        # returned None for view_count BUT we have a stored value, we
        # can't tell whether it changed. The old code silently treated
        # this as "same" and skipped — meaning bulk refresh was a no-op
        # for any video yt-dlp's flat-playlist didn't return counts for.
        # Now we route it through the per-video fetch path so we
        # actually get current counts. ~17% of videos still need this
        # even with the `youtubetab:skip=webpage` extractor arg
        # (members-only, very recent uploads, etc.).
        _no_flat_data = (_view_new is None and _view_old is not None)

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
            old["fetched_at"] = datetime.now().isoformat()
            old["_dirty"] = True

        # VERBOSE-ONLY per-video diff trace. Compact one-liner showing
        # the old→new counts and the decision. With 1000s of videos
        # per channel this floods the log — that's intentional in
        # Verbose mode — Verbose is intentionally noisy. Simple mode
        # hides via `dim` tag.
        def _fmt_cnt(n):
            return "—" if n is None else f"{n:,}"
        if _no_flat_data:
            _decision = "no flat data → full fetch"
        elif _changed and full_fetch_on_change:
            _decision = "changed → full fetch"
        elif _changed:
            _decision = "changed → in-place update"
        else:
            _decision = "unchanged → skip"
        stream.emit([
            ["    — ", ["dim"]],
            [f"{vid} · views {_fmt_cnt(_view_old)}→{_fmt_cnt(_view_new)} · "
             f"likes {_fmt_cnt(_like_old)}→{_fmt_cnt(_like_new)} · "
             f"{_decision}\n", ["dim"]],
        ])

        if _no_flat_data:
            # Force a full per-video fetch — only path that can give
            # us a current view count for this vid.
            changed_ids.append(vid)
        elif _changed and full_fetch_on_change:
            changed_ids.append(vid)
        elif _changed:
            updated_in_place += 1
        else:
            skipped_same += 1

    # Persist the in-place-updated entries. Group by jsonl path so we
    # only rewrite each file once. Skip entries that aren't actually
    # dirty — see `_dirty` flag set above. The previous code added
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
        # internal lock only serialized the WRITE half — two threads
        # could read, both miss the other's changes, then both write
        # (audit: refresh_views.py C15).
        with _lock_for(jp):
            full = _read_metadata_jsonl(jp)
            full.update(entries)
            try:
                _write_metadata_jsonl(jp, full)
                # Mirror the refreshed view/like counts into the index DB so
                # the global Videos view can sort the whole archive by
                # views/likes off an indexed column (no sidecar walk at
                # query time). Best-effort — never block the refresh.
                try:
                    from .. import index as _idx_db
                    _idx_db.update_video_stats(
                        [(vid, e.get("view_count"), e.get("like_count"))
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
        # existing metadata entry — we're filling in first-time
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
        # Build a video_id → (filepath, title) map from on_disk so we
        # can pass filepath + title_hint to fetch_single_video_metadata
        # (signature: channel, video_id, file_path, title_hint, stream).
        # The prior call had `stream` in the title_hint slot AND passed
        # a nonexistent `cancel_event` kwarg — every per-video fetch
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
        for vid in changed_ids:
            if cancel_event is not None and cancel_event.is_set():
                break
            # Pause-wait between videos. The user might have clicked
            # Pause minutes ago — they're waiting on this exact loop
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
                res = fetch_single_video_metadata(
                    channel, vid, fp, title_hint, stream,
                    emit_inline_log=False)
                if res.get("ok"):
                    full_fetched += 1
                elif not res.get("transient"):
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
        # Hold the sync-side config write lock so a concurrent
        # settings_save / channel update doesn't read same cfg and
        # lose this timestamp update on race (audit: refresh_views.
        # py:582-589). _config_write_lock lives in sync/core.py.
        try:
            from ..sync.core import _config_write_lock as _cwl
        except Exception as _ilex:
            # Fail loud rather than silently dropping serialization —
            # a concurrent settings_save could land between our load
            # and save and lose the timestamp update (audit: H85, H94).
            _log.error("refresh_views can't import _config_write_lock: %s "
                       "(skipping timestamp stamp to avoid lost-update)", _ilex)
            _cwl = None
        def _do_stamp():
            cfg = _cfg.load_config()
            ch_url_norm = ch_url.rstrip("/")
            now_ts = time.time()
            for ch in cfg.get("channels", []):
                if (ch.get("url") or "").rstrip("/") == ch_url_norm:
                    ch["last_views_refresh_ts"] = now_ts
                    break
            _cfg.save_config(cfg)
        if _cwl is not None:
            with _cwl:
                _do_stamp()
        # If _cwl is None (import failed), we intentionally SKIP the
        # stamp rather than fall through to an unserialized write that
        # could clobber a concurrent writer.
    except Exception as e:
        _log.debug("swallowed: %s", e)

    # Stop the heartbeat thread BEFORE the clear_line + summary so
    # the in-place line doesn't get re-painted on top of the summary.
    _hb_alive[0] = False
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
    # "via bulk path" dropped — user-facing log doesn't need to
    # surface the internal code path.
    _err_color = "red" if full_errors else "simpleline_pink"
    tagged: list[list[str]] = [
        [" \u2014 ", "meta_bracket"],
        [f"{name}: ", "simpleline"],
    ]
    _first = True
    def _sep():
        if not _first:
            tagged.append([" \u00b7 ", "simpleline"])
    _emitted_something = False
    if full_fetched:
        _sep()
        tagged.append([f"{full_fetched}", "simpleline_pink"])
        tagged.append([" with updated counts", "simpleline"])
        _first = False
        _emitted_something = True
    if updated_in_place:
        _sep()
        tagged.append([f"{updated_in_place}", "simpleline_pink"])
        tagged.append([" counts updated in place", "simpleline"])
        _first = False
        _emitted_something = True
    if skipped_same:
        _sep()
        tagged.append([f"{skipped_same}", "simpleline_pink"])
        tagged.append([" unchanged", "simpleline"])
        _first = False
        _emitted_something = True
    if full_errors:
        _sep()
        tagged.append([f"{full_errors}", _err_color])
        tagged.append([" errors", _err_color])
        _first = False
        _emitted_something = True
    if no_meta_entry and not full_fetched:
        _sep()
        tagged.append([f"{no_meta_entry}", "simpleline_pink"])
        tagged.append([" need first fetch", "simpleline"])
        _first = False
        _emitted_something = True
    if not _emitted_something:
        # Zero matches across all counters. Normally the title-fallback
        # loop above resolves legacy-tkinter archive files — so hitting
        # this branch means even title-matching failed. Usually:
        # (a) empty channel folder, (b) ambiguous titles (duplicates
        # skipped for safety), or (c) filesystem-sanitized titles too
        # divergent to match.
        _n_disk = len(on_disk)
        _n_bulk = len(bulk)
        if _n_disk == 0:
            tagged.append(["no videos on disk for this channel",
                           "simpleline"])
        elif _n_bulk == 0:
            tagged.append(["channel returned no videos", "simpleline"])
        else:
            tagged.append([
                f"no matches ({_n_disk} on disk vs {_n_bulk} from YouTube "
                f"\u2014 titles too divergent to match)", "simpleline"])
    tagged.append([f" (took {took:.1f}s)\n", "simpleline"])
    stream.emit(tagged)
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

