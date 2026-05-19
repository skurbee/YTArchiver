"""
metadata.refresh_fetch — fetch missing metadata for a channel's videos.

Extracted from metadata/refresh.py (Patch 22, v72.4).

`fetch_channel_metadata(channel, stream, refresh=False, ...)` fills in
missing metadata for a channel's on-disk videos. When `refresh=True`
this delegates to `bulk_refresh_views_likes`; when False it walks the
channel folder, identifies which videos lack metadata, and fetches them
via the per-video metadata fetcher.
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
    _read_metadata_jsonl,
)
from ..sync import find_yt_dlp
from ..text_utils import normalize_title as _canon_norm_title
from ..utils import sqlite_like_escape as _like_esc
from .fetcher import fetch_metadata_for_videos
from .scan import _scan_channel_videos
from ._refresh_proxies import (
    _ID_RE,
    _enter_pause_wait,
    _exit_pause_wait,
    _flat_playlist_bulk_stats,
    _probe_durations_bulk,
    _resolve_channel_id_url,
    _resolve_ids_by_title,
    backfill_video_ids,
    existing_info_ids,
)
from .refresh_views import bulk_refresh_views_likes

_log = get_logger(__name__)


def fetch_channel_metadata(channel: dict[str, Any],
                           stream: LogStreamer,
                           cancel_event: threading.Event | None = None,
                           refresh: bool = False,
                           pause_event: threading.Event | None = None,
                           scope: dict[str, Any] | None = None,
                           queues=None,
                           ) -> dict[str, Any]:
    """Fill in missing metadata for this channel's on-disk videos.

    Two modes:
      - refresh=False (DEFAULT): DISK-DRIVEN. Enumerate videos on disk
        via `_scan_channel_videos` (filename `[id]` bracket first, then
        index-DB filepath lookup). Compare against existing JSONL IDs.
        Fetch only the missing handful. NO playlist walk — because the
        playlist would include ~hundreds of channel-videos that aren't
        downloaded, all of which are irrelevant for this job.
      - refresh=True: "Refresh views/likes" — delegates to
        bulk_refresh_views_likes() which does one flat-playlist call
        for all videos, then only full-fetches ones whose counts
        changed. Users reported the old every-video-full-fetch path
        taking 1h17m for a 404-video channel; the bulk path typically
        finishes in well under a minute.

    `scope` restricts which on-disk videos are considered:
      - `{"year": 2024}` — only videos whose upload year (from mtime)
        matches. Used by the Browse video-grid year-head context menu
        to offer year-scoped metadata refresh (feature H-14). Videos
        whose year can't be determined (mtime lookup failed) are
        excluded from scoped passes.
    """
    # Smart-refresh short-circuit: when the caller wants refresh=True,
    # go straight to the bulk path. That function knows to only
    # full-fetch videos with changed counts; for a channel where 99%
    # of view-counts haven't moved, it becomes ~1 API call instead
    # of ~N. Error cases (bulk returns empty) fall back below.
    if refresh:
        _res = bulk_refresh_views_likes(channel, stream,
                                        cancel_event=cancel_event,
                                        pause_event=pause_event,
                                        scope=scope,
                                        full_fetch_on_change=True,
                                        queues=queues)
        # Fall through to the old path ONLY if the bulk path couldn't
        # get any data at all (e.g. channel URL stripped, yt-dlp
        # returned empty, private channel). That path is still
        # useful as a safety net — it at least does the
        # disk-driven fetch for newly-added missing metadata.
        if _res.get("ok") or _res.get("bulk_fetched", 0) > 0:
            return _res
        stream.emit_dim(
            " (fast refresh returned nothing — falling back to "
            "per-video refresh)")
        # Continue into the legacy path below.

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
    # feature H-14: when scope has a year, banner shows the scope
    # ("Rechecking Foo (2024 only)..."); otherwise unchanged.
    _scope_year: int | None = None
    if scope and isinstance(scope.get("year"), int):
        _scope_year = int(scope["year"])
    _scope_banner = f" ({_scope_year} only)" if _scope_year is not None else ""
    stream.emit([["  \u2014 ", "meta_bracket"],
                 [f"Rechecking {name}{_scope_banner}...\n", "simpleline"]])

    # 1. Enumerate videos ON DISK. `_scan_channel_videos` returns
    # (video_id, title, year, month, filepath) — video_id is
    # filled in either from filename `[id]` bracket or from the
    # index DB (filepath → video_id lookup).
    on_disk = _scan_channel_videos(folder)
    # feature H-14: year-scoped filter. Entries where year is None
    # (mtime couldn't be resolved) are excluded from scoped passes —
    # if we can't place them in a year, we can't honor the year scope.
    if _scope_year is not None:
        on_disk = [v for v in on_disk if v[2] == _scope_year]
    on_disk_ids = [v[0] for v in on_disk if v[0]]
    # Previously-failed fetches + previously-failed id-resolves —
    # OLD-YTArchiver compatible skip logic. Videos marked in the DB
    # as "already tried and it didn't work" are NOT retried this run.
    # On refresh=True we clear the flags and try again.
    _failed_fetch: set = set()
    _failed_id_files: set = set()
    try:
        from .. import index as _idx
        conn = _idx._open()
        if conn is not None:
            ch_name = channel.get("name") or channel.get("folder") or ""
            with _idx._db_lock:
                if refresh:
                    conn.execute(
                        "UPDATE videos SET metadata_fetch_failed_ts=NULL, "
                        "id_resolve_failed_ts=NULL WHERE channel=?",
                        (ch_name,))
                    conn.commit()
                else:
                    for (_vid,) in conn.execute(
                            "SELECT video_id FROM videos WHERE channel=? "
                            "AND metadata_fetch_failed_ts IS NOT NULL "
                            "AND video_id IS NOT NULL AND video_id != ''",
                            (ch_name,)).fetchall():
                        _failed_fetch.add(_vid)
                    for (_fp,) in conn.execute(
                            "SELECT filepath FROM videos WHERE channel=? "
                            "AND id_resolve_failed_ts IS NOT NULL "
                            "AND (video_id IS NULL OR video_id='')",
                            (ch_name,)).fetchall():
                        if _fp:
                            _failed_id_files.add(os.path.normpath(_fp).lower())
    except Exception as e:
        _log.debug("swallowed: %s", e)
    # Files we couldn't resolve to a video_id. Skip ones we already
    # gave up on in a previous run.
    unmatched_files = [v[4] for v in on_disk if not v[0]
                        and os.path.normpath(v[4]).lower() not in _failed_id_files]
    n_without_id = len(unmatched_files)
    n_perm_no_id = sum(1 for v in on_disk if not v[0]
                        and os.path.normpath(v[4]).lower() in _failed_id_files)

    # 2. Read existing metadata JSONLs.
    have_meta: set = set()
    jsonl_count = 0
    for dp, _dns, fns in os.walk(str(folder)):
        for fn in fns:
            if fn.endswith("Metadata.jsonl"):
                jsonl_count += 1
                have_meta.update(_read_metadata_jsonl(os.path.join(dp, fn)).keys())

    # 3. Enumerate existing THUMBNAILS. a case: 2 videos had
    # metadata JSONL entries but no thumbnail file on disk — the
    # earlier logic only checked metadata so those 2 showed as
    # "complete" and weren't re-fetched. Thumbnails are stored as
    # `<safe_title> [<video_id>].<ext>` inside `.Thumbnails/`
    # subfolders (one per year/month split). We extract the
    # bracketed video_id from each filename.
    have_thumb: set = set()
    thumb_file_count = 0
    _thumb_id_re = re.compile(r"\[([A-Za-z0-9_-]{11})\]")
    for dp, _dns, fns in os.walk(str(folder)):
        # Only look in .Thumbnails/ folders — avoids picking up a
        # bracketed id from an unrelated file elsewhere in the tree.
        if os.path.basename(dp).lower() != ".thumbnails":
            continue
        for fn in fns:
            low = fn.lower()
            if not low.endswith((".jpg", ".jpeg", ".png", ".webp")):
                continue
            thumb_file_count += 1
            m = _thumb_id_re.search(fn)
            if m:
                have_thumb.add(m.group(1))

    # 4. Compute targets: missing metadata OR missing thumbnail.
    # Dedupe via dict (preserves insertion order) so a video whose
    # id accidentally appears multiple times in `on_disk_ids`
    # (historical bug: 13 a user's channel files all got assigned the
    # fake id "a-user-channel" from filename-suffix parsing)
    # doesn't produce 13 identical fetch attempts.
    # Skip videos whose metadata fetch previously failed (deleted /
    # private / region-locked) — `_failed_fetch` set comes from
    # the DB and gets cleared on refresh=True.
    seen: set = set()
    deduped_ids: list = []
    for _vid in on_disk_ids:
        if _vid not in seen:
            seen.add(_vid)
            deduped_ids.append(_vid)
    if refresh:
        targets = list(deduped_ids)
    else:
        targets = [vid for vid in deduped_ids
                   if (vid not in have_meta or vid not in have_thumb)
                   and vid not in _failed_fetch]

    # Breakdown so the user can see exactly what the scan found
    # (metadata coverage vs thumbnail coverage are now reported
    # separately). Only the em-dash prefix is pink — title/number
    # content stays default (simpleline) so it reads clearly against
    # the dark background. rule: "colored em dash indicating
    # what task it is from. Any brackets or anything should be pink.
    # The actual metadata tag should be pink but no actual title or
    # numbers or anything should be pink."
    missing_meta = sum(1 for vid in on_disk_ids if vid not in have_meta)
    missing_thumb = sum(1 for vid in on_disk_ids if vid not in have_thumb)
    _perm_failed = sum(1 for vid in on_disk_ids if vid in _failed_fetch)
    # FIX (2026-05-14): the ratio used to read `len(have_meta)/len(on_disk_ids)`
    # which was misleading because `have_meta` includes orphan files for
    # videos no longer on disk. Example seen with ColdFusion:
    #   `metadata: 513/512 (0 missing)` \u2014 513 metadata files but only
    #    512 unique video IDs on disk; 1 orphan metadata file.
    #   `thumbnails: 497/512 (20 missing)` \u2014 497 thumbs but only 492
    #    match current videos (5 orphan); display claimed 20 missing
    #    but 512-497=15 didn't math.
    # Now X = covered (videos WITH the asset), Y = total on-disk videos,
    # so X/Y is a real coverage ratio and X + missing = Y always holds.
    # Orphan files get their own callout when present.
    n_videos = len(on_disk_ids)
    covered_meta = n_videos - missing_meta
    covered_thumb = n_videos - missing_thumb
    orphan_meta = max(0, len(have_meta) - covered_meta)
    orphan_thumb = max(0, len(have_thumb) - covered_thumb)
    def _coverage_str(label: str, covered: int, total: int,
                       missing: int, orphan: int) -> str:
        s = f"{label}: {covered:,}/{total:,}"
        if missing and orphan:
            s += f" ({missing:,} missing, {orphan:,} stale)"
        elif missing:
            s += f" ({missing:,} missing)"
        elif orphan:
            s += f" \u2713 ({orphan:,} stale)"
        else:
            s += " \u2713"
        return s
    _meta_str = _coverage_str("metadata", covered_meta, n_videos,
                               missing_meta, orphan_meta)
    _thumb_str = _coverage_str("thumbnails", covered_thumb, n_videos,
                                missing_thumb, orphan_thumb)
    _parts = [
        f"{len(on_disk):,} on disk \u00b7 "
        f"{_meta_str} \u00b7 "
        f"{_thumb_str}"
    ]
    if _perm_failed:
        _parts.append(f"{_perm_failed:,} previously failed (skipped)")
    _parts.append(f"{len(targets):,} need fetching")
    stream.emit([
        [" \u2014 ", "meta_bracket"],
        [" \u00b7 ".join(_parts) + "\n", "simpleline"],
    ])
    if n_without_id:
        # Some on-disk files couldn't be matched to a video_id via the
        # filename-bracket path or the index DB. a case: 2 recent
        # videos were manually dropped into the 2026/ folder with
        # bracket-less filenames AND no corresponding DB row got
        # populated with an id. They show up with no thumbnail in the
        # grid, and without an id we can't fetch metadata either.
        # FALLBACK: walk the channel's YouTube playlist ONCE (one
        # yt-dlp --flat-playlist call) to grab every (id, title) pair
        # for this channel, then match unmatched files to playlist
        # entries by normalized title. Any matches get backfilled into
        # the index DB so this fallback doesn't need to run on next
        # recheck.
        stream.emit([
            [" \u26A0 ", "meta_bracket"],
            [f"{n_without_id:,} on-disk video(s) couldn't be matched "
             f"to a YouTube ID \u2014 checking channel for title "
             f"match...\n", "simpleline"],
        ])
        url = channel.get("url", "").strip()
        resolved = _resolve_ids_by_title(
            yt, url, unmatched_files, stream, cancel_event, pause_event)
        if resolved:
            # Backfill the index DB. For each resolved (filepath → id)
            # pair: check if any OTHER DB row already claims this id.
            # If yes → this is a duplicate download of the same
            # YouTube video (YouTuber renamed the video, both
            # downloads sit on disk). Mark the smaller / newer copy
            # as `is_duplicate_of=<primary filepath>` so the Browse
            # grid hides it. If no → normal backfill, just populate
            # the id on the existing row.
            n_duplicates_flagged = 0
            try:
                from .. import index as _idx
                conn = _idx._open()
                if conn is not None:
                    with _idx._db_lock:
                        for _fp, _vid in resolved.items():
                            existing = conn.execute(
                                "SELECT filepath, size_bytes FROM videos "
                                "WHERE video_id=? AND filepath != ? "
                                "COLLATE NOCASE",
                                (_vid, _fp)).fetchone()
                            if existing:
                                # Figure out which file (existing vs
                                # new) is primary. Keep the larger
                                # one as primary; flag the smaller
                                # as duplicate-of the primary.
                                existing_fp, existing_size = existing
                                try:
                                    new_size = os.path.getsize(_fp)
                                except OSError:
                                    new_size = 0
                                if new_size > (existing_size or 0):
                                    # New file wins — flip existing
                                    # row to duplicate, assign id to
                                    # new row.
                                    conn.execute(
                                        "UPDATE videos SET video_id=?, "
                                        "video_url=?, is_duplicate_of=NULL "
                                        "WHERE filepath=? COLLATE NOCASE",
                                        (_vid,
                                         f"https://www.youtube.com/watch?v={_vid}",
                                         _fp))
                                    conn.execute(
                                        "UPDATE videos SET is_duplicate_of=? "
                                        "WHERE filepath=? COLLATE NOCASE",
                                        (_fp, existing_fp))
                                else:
                                    # Existing wins — new row is the
                                    # duplicate.
                                    conn.execute(
                                        "UPDATE videos SET video_id=?, "
                                        "video_url=?, is_duplicate_of=? "
                                        "WHERE filepath=? COLLATE NOCASE",
                                        (_vid,
                                         f"https://www.youtube.com/watch?v={_vid}",
                                         existing_fp, _fp))
                                n_duplicates_flagged += 1
                            else:
                                conn.execute(
                                    "UPDATE videos SET video_id=?, video_url=? "
                                    "WHERE filepath=? COLLATE NOCASE",
                                    (_vid,
                                     f"https://www.youtube.com/watch?v={_vid}",
                                     _fp))
                        conn.commit()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            if n_duplicates_flagged:
                stream.emit([
                    [" \u26A0 ", "meta_bracket"],
                    [f"{n_duplicates_flagged:,} duplicate download(s) "
                     f"detected \u2014 hidden from grid (files still on "
                     f"disk).\n", "simpleline"],
                ])
                # Drop the Browse grid cache for this channel so the
                # next click on it queries fresh and reflects the
                # duplicate filtering.
                try:
                    from .. import index as _idx
                    _idx.invalidate_channel_videos(
                        channel.get("name") or channel.get("folder", ""))
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            stream.emit([
                [" \u2713 ", "simpleline_green"],
                [f"Matched {len(resolved):,} of {n_without_id:,} "
                 f"by title \u2014 backfilled into index.\n",
                 "simpleline"],
            ])
            # Add the newly-resolved ids into our working sets so the
            # target calculation below picks them up for fetching.
            for _fp, _vid in resolved.items():
                on_disk_ids.append(_vid)
            # Re-enter the target computation with the new ids.
        still_unmatched = [fp for fp in unmatched_files
                            if os.path.normpath(fp) not in (resolved or {})]
        if still_unmatched:
            # Mark these files in the DB as "id-resolve-failed" so
            # future rechecks skip them instead of re-running the
            # playlist walk. Matches OLD YTArchiver's pattern where
            # unresolvable files stop wasting API calls after the
            # first attempt. `refresh=True` clears the flag above, so
            # the user can force a retry via "Refresh Counts".
            try:
                from .. import index as _idx
                conn = _idx._open()
                if conn is not None:
                    _now = time.time()
                    with _idx._db_lock:
                        for _fp in still_unmatched:
                            conn.execute(
                                "UPDATE videos SET id_resolve_failed_ts=? "
                                "WHERE filepath=? COLLATE NOCASE",
                                (_now, os.path.normpath(_fp)))
                        conn.commit()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            stream.emit([
                [" \u2014 ", "meta_bracket"],
                [f"{len(still_unmatched):,} file(s) couldn't be matched "
                 f"even by title (deleted? re-uploaded with new ID? "
                 f"filename edited?). Marked as permanent skip.\n",
                 "simpleline"],
            ])
            for _fp in still_unmatched[:5]:
                stream.emit([
                    [" \u2022 ", "meta_bracket"],
                    [f"{os.path.basename(_fp)}\n", "simpleline"],
                ])
            if len(still_unmatched) > 5:
                stream.emit([
                    [" ", "dim"],
                    [f"\u2026 and {len(still_unmatched) - 5:,} more\n", "dim"],
                ])
    if n_perm_no_id:
        stream.emit([
            [" \u2014 ", "meta_bracket"],
            [f"{n_perm_no_id:,} file(s) previously marked as "
             f"unresolvable \u2014 skipping (use Refresh to retry).\n",
             "simpleline"],
        ])

        # Recompute targets with the newly-resolved ids in scope.
        if refresh:
            targets = list(on_disk_ids)
        else:
            targets = [vid for vid in on_disk_ids
                       if vid not in have_meta or vid not in have_thumb]

    if not targets:
        stream.emit([[" \u2713 ", "simpleline_green"],
                     ["All metadata + thumbnails up to date.\n", "simpleline"]])
        return {"ok": True, "fetched": 0, "skipped": len(on_disk_ids),
                "errors": 0}

    return fetch_metadata_for_videos(channel, targets, stream,
                                     cancel_event, refresh=refresh,
                                     pause_event=pause_event,
                                     queues=queues)
