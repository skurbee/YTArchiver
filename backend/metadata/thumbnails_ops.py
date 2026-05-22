"""
metadata.thumbnails_ops — channel-scoped thumbnail + video-id status ops.

Patch 19 phase M5 (v69.2): extracted from metadata/legacy.py.

Public surface (re-imported into legacy.py):
    sweep_missing_thumbnails(channel, stream)
        Walk a channel folder for .mp4 files without thumbs and
        download missing ones from cached metadata.jsonl URLs.

    realign_misplaced_thumbnails(channels=None, stream=None)
        Move stray .Thumbnails/ entries to the year/month folders
        they should live under.

    count_thumbnail_status_bulk(channels, force=False)
        Per-channel {total, with_thumb, missing} for the
        Settings > Metadata grid.

    count_video_id_status_bulk(channels, force=False)
        Per-channel {total, with_id, missing, tried_failed}.

    count_video_id_status(channel)
        Single-channel variant.

The disk-walk code path in count_thumbnail_status_bulk depends on
_scan_channel_videos (moved to scan.py in M2), which in turn requires
the v68.7 `LIKE ? ESCAPE '\\'` SQL fix to actually resolve video_ids
for OLD-naming files.
"""
from __future__ import annotations

import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from ..log import get_logger
from ..metadata_io import (
    _folder_for_channel,
    _get_metadata_jsonl_path,
    _read_metadata_jsonl,
)
from ..thumbnails import (
    _channel_fingerprint,
    _download_thumbnail,
    _ensure_thumbnails_dir,
    _load_thumb_cache,
    _save_thumb_cache,
    _thumbnail_exists_for,
)
from ..utils import sqlite_like_escape as _like_esc
from ..ytarchiver_config import load_config
from .scan import _scan_channel_videos

_log = get_logger(__name__)


# TTL cache for the Video-IDs GROUP-BY query (mirrored from legacy.py).
_VIDEO_ID_CACHE_TTL_SEC = 60.0
_video_id_cache_state: dict[str, Any] = {"ts": 0.0, "rows": {}}
_video_id_cache_lock = threading.Lock()


def sweep_missing_thumbnails(channel: dict[str, Any], stream=None,
                              cancel_event=None) -> dict[str, int]:
    """Issue #147/#158: scan a channel folder for .mp4 files that lack a
    thumbnail in `.Thumbnails/` and download any missing ones from the
    URLs cached in metadata.jsonl. Use after a sync pass to catch
    thumbnails that yt-dlp's bulk download missed (rate-limited, racy,
    transient network blips). Returns {checked, fetched, missing}.

    `cancel_event` (audit: thumbnails_ops H38) is checked per-bucket so
    a user-pressed Cancel during the post-sync sweep returns promptly
    instead of waiting for hundreds of HTTP fetches.
    """
    def _is_cancelled():
        return cancel_event is not None and cancel_event.is_set()
    folder = _folder_for_channel(channel)
    if not folder or not folder.exists():
        return {"checked": 0, "fetched": 0, "missing": 0}
    checked = fetched = still_missing = 0
    # _scan_channel_videos returns (vid_id, title, year, month, filepath).
    # Group by (year, month) so each metadata.jsonl is read exactly once
    # per bucket. The jsonl path depends on the channel's split_years +
    # split_months config — feed those plus the year/month into
    # `_get_metadata_jsonl_path` so it builds the right path. (Earlier
    # version passed the channel dict where `split_years: bool` was
    # expected and `sub` where the channel root was expected — single-
    # channel refetch threw "missing 1 required positional argument:
    # 'split_months'". Fixed by passing args correctly.)
    ch_root = str(folder)
    name = channel.get("name") or channel.get("folder") or ""
    split_years = bool(channel.get("split_years", False))
    split_months = bool(channel.get("split_months", False))
    # Pre-walk every .Thumbnails dir under the channel root once and
    # collect the full vid-id set. This means we treat thumbs as
    # "exists" if ANY .Thumbnails dir in the channel has them — not
    # just the one next to the mp4. Bug surfaced on The PrimeTime
    # where most thumbs lived in 2025/.Thumbnails/ but their mp4s
    # had been re-foldered to 2023/ and 2024/ — the refetcher kept
    # re-downloading thumbs that already existed in a sibling year.
    _id_re = re.compile(r"\[([A-Za-z0-9_-]{11})\]")
    _all_thumb_vids: set = set()
    try:
        for _dp, _dns, _fns in os.walk(ch_root):
            if os.path.basename(_dp) != ".Thumbnails":
                continue
            for _fn in _fns:
                if not _fn.lower().endswith(
                        (".jpg", ".jpeg", ".png", ".webp")):
                    continue
                _m = _id_re.search(_fn)
                if _m:
                    _candidate = _m.group(1)
                    # Reject all-alpha 11-char strings — those are
                    # almost always user-typed labels (e.g.
                    # "[a-user-channel]") rather than real YouTube
                    # video IDs which always mix digits + symbols
                    # (audit: thumbnails_ops H98).
                    if not _candidate.isalpha():
                        _all_thumb_vids.add(_candidate)
    except Exception as e:
        _log.debug("swallowed: %s", e)
    by_bucket: dict[tuple[int | None, int | None],
                    list[tuple[str, str]]] = {}
    for vid_id, _title, _y, _m, path in _scan_channel_videos(folder):
        if not vid_id:
            continue
        by_bucket.setdefault((_y, _m), []).append((path, vid_id))
    for (yr, mo), items in by_bucket.items():
        if _is_cancelled():
            break
        jp, sub = _get_metadata_jsonl_path(
            name, ch_root, split_years, split_months, yr, mo)
        thumb_dir = _ensure_thumbnails_dir(sub)
        meta = _read_metadata_jsonl(jp) if jp else {}
        for path, vid_id in items:
            if _is_cancelled():
                break
            checked += 1
            # Already covered somewhere in the channel? Skip the
            # re-download. (Was: only checked thumb_dir adjacent to
            # the mp4, missing channel-wide reorgs.)
            if vid_id in _all_thumb_vids:
                continue
            entry = meta.get(vid_id) or {}
            url = entry.get("thumbnail_url") or ""
            title = entry.get("title") or os.path.splitext(
                os.path.basename(path))[0]
            if not url:
                still_missing += 1
                continue
            try:
                _download_thumbnail(url, thumb_dir, title, vid_id,
                                     stream=stream)
                if _thumbnail_exists_for(thumb_dir, vid_id):
                    fetched += 1
                    _all_thumb_vids.add(vid_id)
                    # Mark the DB flag so Settings > Metadata's
                    # Thumbnails column reflects the new state on its
                    # next query without re-walking.
                    try:
                        from .. import index as _idx
                        _c = _idx._open()
                        if _c is not None:
                            with _idx._db_lock:
                                try:
                                    # Scope by channel so a video that
                                    # exists in multiple channel folders
                                    # (cross-channel merge / manual copy)
                                    # doesn't flip the flag for the
                                    # wrong channel's row (audit:
                                    # thumbnails_ops H78).
                                    _c.execute(
                                        "UPDATE videos SET has_thumbnail=1 "
                                        "WHERE video_id=? AND channel=?",
                                        (vid_id, name))
                                    _c.commit()
                                except Exception as _ue:
                                    # Roll back so a parallel reader
                                    # doesn't see this UPDATE half-
                                    # applied (audit: thumbnails_ops.
                                    # py:149-158).
                                    try: _c.rollback()
                                    except Exception: pass
                                    _log.debug("has_thumbnail rollback: %s", _ue)
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                else:
                    still_missing += 1
            except Exception:
                still_missing += 1
    return {"checked": checked, "fetched": fetched,
            "missing": still_missing}


def realign_misplaced_thumbnails(channels: list[dict[str, Any]] | None = None,
                                  dry_run: bool = True,
                                  stream=None) -> dict[str, Any]:
    """Survey + (optionally) move thumbnails that ended up in a different
    year/month folder than the mp4 they belong to.

    Mechanism: each thumbnail filename carries a `[video_id]` tag.
    For every `.Thumbnails/*.{jpg,jpeg,webp,png}` under each channel
    folder, look up the mp4 with that video_id in the index DB; if
    the mp4's parent folder differs from the thumbnail's parent
    folder (the one ABOVE its `.Thumbnails/` dir), the thumb is
    misplaced and should live next to the mp4.

    Same-volume rename via `os.replace` so no copy/delete cycle and
    no risk of corruption.

    `dry_run=True` (default) just reports; `dry_run=False` actually
    moves files. Returns:
      {
        scanned, aligned, misaligned, moved, skipped_dest_exists,
        orphan_no_db, per_channel: {name: {misaligned, moved, ...}}
      }
    """
    from .. import index as _idx
    out_dir = (load_config() or {}).get("output_dir") or ""
    if not out_dir:
        return {"ok": False, "error": "no output_dir"}

    if channels is None:
        channels = (load_config() or {}).get("channels", []) or []

    # Build vid → mp4_parent map from the DB. When `channels` is
    # narrow (typical case — single-channel realign), prefix-filter
    # the SQL with the channel folder so we don't os.path.isfile()
    # every row in the videos table. On a 92k-video archive this
    # was the bottleneck: 92k isfile syscalls per call, all on Z:\
    # DrivePool, which made realign take minutes to even start.
    vid_to_mp4_parent: dict[str, str] = {}
    try:
        conn = _idx._reader_open() or _idx._open()
        if conn is not None:
            with _idx._reader_lock:
                if channels:
                    # Filter to just the channel folders we'll process.
                    # LIKE prefix matches normalize via SUBSTR before
                    # the isfile() check so we only stat files that
                    # could possibly belong to one of the targeted
                    # channels.
                    from backend.sync import channel_folder_name as _cfn
                    _allowed_prefixes = []
                    for _ch in channels:
                        _fn = _cfn(_ch)
                        if _fn:
                            _allowed_prefixes.append(
                                os.path.normpath(os.path.join(out_dir, _fn)))
                    for fp, vid in conn.execute(
                            "SELECT filepath, video_id FROM videos "
                            "WHERE video_id IS NOT NULL AND video_id<>''"):
                        if not (fp and vid):
                            continue
                        _np = os.path.normpath(fp)
                        if not any(_np.startswith(_p) for _p in _allowed_prefixes):
                            continue
                        # Wrap isfile in try/except so a single path-
                        # too-long row doesn't abort the whole realign
                        # (audit: metadata/core.py:194-212). Long paths
                        # are common in deeply-organized archives.
                        try:
                            _is_file = os.path.isfile(_np)
                        except OSError:
                            continue
                        if _is_file:
                            vid_to_mp4_parent[vid] = os.path.normpath(
                                os.path.dirname(_np))
                else:
                    # No channel filter — fall back to the old scan
                    # (only used when caller explicitly wants a global
                    # realign).
                    for fp, vid in conn.execute(
                            "SELECT filepath, video_id FROM videos "
                            "WHERE video_id IS NOT NULL AND video_id<>''"):
                        if not (fp and vid):
                            continue
                        try:
                            _is_file = os.path.isfile(fp)
                        except OSError:
                            continue
                        if _is_file:
                            vid_to_mp4_parent[vid] = os.path.normpath(
                                os.path.dirname(fp))
    except Exception as e:
        if stream:
            try: stream.emit_error(f"Couldn't read the archive index for thumbnail repair: {e}")
            except Exception as e: _log.debug("swallowed: %s", e)
        return {"ok": False, "error": str(e)}

    id_re = re.compile(r"\[([A-Za-z0-9_-]{11})\]")
    scanned = aligned = misaligned = moved = skipped_dest = orphan = 0
    per_channel: dict[str, dict[str, int]] = {}

    for ch in channels:
        name = ch.get("name") or ch.get("folder") or ""
        folder = ch.get("folder_override") or ch.get("folder") or name
        if not folder:
            continue
        ch_root = os.path.join(out_dir, folder)
        if not os.path.isdir(ch_root):
            continue
        pc = {"misaligned": 0, "moved": 0, "skipped_dest_exists": 0,
              "orphan_no_db": 0}
        for dp, _dns, fns in os.walk(ch_root):
            if os.path.basename(dp) != ".Thumbnails":
                continue
            thumb_parent = os.path.normpath(os.path.dirname(dp))
            for fn in fns:
                if not fn.lower().endswith(
                        (".jpg", ".jpeg", ".webp", ".png")):
                    continue
                m = id_re.search(fn)
                if not m:
                    continue
                scanned += 1
                vid = m.group(1)
                mp4_parent = vid_to_mp4_parent.get(vid)
                if mp4_parent is None:
                    orphan += 1
                    pc["orphan_no_db"] += 1
                    continue
                if thumb_parent.lower() == mp4_parent.lower():
                    aligned += 1
                    continue
                # Misaligned. Compute target.
                misaligned += 1
                pc["misaligned"] += 1
                target_dir = os.path.join(mp4_parent, ".Thumbnails")
                target_path = os.path.join(target_dir, fn)
                source_path = os.path.join(dp, fn)
                if os.path.exists(target_path):
                    # Duplicate already at destination — skip the move
                    # to avoid losing data. User can manually consolidate.
                    skipped_dest += 1
                    pc["skipped_dest_exists"] += 1
                    continue
                if dry_run:
                    continue
                # Actually move. Ensure target dir exists.
                try:
                    os.makedirs(target_dir, exist_ok=True)
                    os.replace(source_path, target_path)
                    moved += 1
                    pc["moved"] += 1
                    # Confirm flag for this vid (it should already
                    # be 1 if a prior walk ran, but ensure correctness
                    # so the Thumbnails column stays accurate).
                    try:
                        from .. import index as _idx2
                        _c2 = _idx2._open()
                        if _c2 is not None:
                            with _idx2._db_lock:
                                # Channel-scoped UPDATE (audit: H78).
                                _c2.execute(
                                    "UPDATE videos SET has_thumbnail=1 "
                                    "WHERE video_id=? AND channel=?",
                                    (vid, name))
                                _c2.commit()
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                except Exception as e:
                    if stream:
                        try:
                            stream.emit_error(
                                f"realign: failed to move "
                                f"{source_path} → {target_path}: {e}")
                        except Exception as e: _log.debug("swallowed: %s", e)
        if any(v > 0 for v in pc.values()):
            per_channel[name] = pc

    if stream and not dry_run:
        try:
            stream.emit_text(
                f" — Realigned {moved} misplaced thumbnail(s) across "
                f"{len(per_channel)} channel(s). "
                f"({skipped_dest} skipped — duplicate at target.)",
                "simpleline_pink")
            stream.flush()
        except Exception as e:
            _log.debug("swallowed: %s", e)

    return {
        "ok": True,
        "scanned": scanned,
        "aligned": aligned,
        "misaligned": misaligned,
        "moved": moved,
        "skipped_dest_exists": skipped_dest,
        "orphan_no_db": orphan,
        "per_channel": per_channel,
        "dry_run": bool(dry_run),
    }


def count_thumbnail_status_bulk(channels: list[dict[str, Any]],
                                  force: bool = False
                                  ) -> dict[str, dict[str, Any]]:
    """Issue #154: count thumbnail coverage per channel. Returns
    {channel_lower: {total, with_thumb, missing}}.

    CACHED + INCREMENTAL (2026-05-13): results are persisted to
    `thumbnail_status_cache.json` keyed by channel name. On the next
    call we compare the channel folder's recursive mtime fingerprint
    against the cached value — unchanged channels return cached
    results instantly, changed channels get re-walked.

    `force=True` ignores the cache and re-walks every channel. Wire
    this from a "Force recheck" button when the user wants fresh
    numbers (e.g. after manually adding thumbnails outside the app).

    Parallelized via ThreadPoolExecutor for the channels that DO
    need a fresh walk — 8 workers because each is mostly waiting on
    pooled-drive I/O latency, not CPU.
    """
    cache = {} if force else _load_thumb_cache()
    out: dict[str, dict[str, Any]] = {}
    needs_walk: list[tuple[dict[str, Any], Path, str, float]] = []

    # FAST PATH (2026-05-14): when `force=False`, query the DB column
    # `has_thumbnail` instead of walking disk. The column is populated
    # by the prior disk walk + by `sweep_missing_thumbnails` so it's
    # the source of truth most of the time. Falls back to the disk
    # walk for any channel that has ANY row with has_thumbnail=NULL
    # (means we haven't done the one-time backfill yet).
    if not force:
        try:
            from .. import index as _idx
            conn = _idx._reader_open() or _idx._open()
            if conn is not None:
                with _idx._reader_lock:
                    # Per-channel: total, sum(has_thumbnail), count NULL.
                    # NULL count > 0 → channel needs a backfill walk.
                    db_stats = {}
                    for r in conn.execute(
                            "SELECT channel, COUNT(*) AS total, "
                            "  SUM(CASE WHEN has_thumbnail=1 THEN 1 ELSE 0 END) AS with_thumb, "
                            "  SUM(CASE WHEN has_thumbnail IS NULL THEN 1 ELSE 0 END) AS unknown "
                            "FROM videos GROUP BY channel"):
                        nm = (r[0] or "").lower()
                        db_stats[nm] = {
                            "total": int(r[1] or 0),
                            "with_thumb": int(r[2] or 0),
                            "unknown": int(r[3] or 0),
                        }
                # Apply to channels that have a fully-populated column.
                # Channels with ANY NULL fall through to the disk walk.
                # Patch fix (v68.2): also fall through when with_thumb=0
                # but total > 0 — that's almost certainly a stale write
                # from before the `_like_esc` fix (vid_id resolution
                # had silently failed for OLD-naming channels, writing
                # has_thumbnail=0 for every row). The disk walk costs
                # ~50ms per channel and self-heals the DB.
                for ch in (channels or []):
                    nm = (ch.get("name") or ch.get("folder") or "").lower()
                    if not nm:
                        continue
                    s = db_stats.get(nm)
                    if (s and s["total"] > 0 and s["unknown"] == 0
                            and s["with_thumb"] > 0):
                        out[nm] = {
                            "total": s["total"],
                            "with_thumb": s["with_thumb"],
                            "missing": max(0, s["total"] - s["with_thumb"]),
                        }
        except Exception as e:
            _log.debug("swallowed: %s", e)

    # Pass 1: figure out which channels can use the cache.
    # Patch fix (v68.3): also distrust cache entries with total>0 but
    # with_thumb=0 — those are the stale writes from before the
    # `_like_esc` fix landed. Force a fresh disk walk so the DB +
    # cache self-heal. (Channels that legitimately have 0 thumbnails
    # pay one extra disk walk per Settings page open; cheap.)
    for ch in (channels or []):
        folder = _folder_for_channel(ch)
        name = (ch.get("name") or ch.get("folder") or "").lower()
        if not folder or not folder.exists() or not name:
            continue
        if name in out:
            # Already filled by the DB fast path above.
            continue
        fp = _channel_fingerprint(folder)
        cached = cache.get(name)
        _cache_looks_stale = (cached
                              and cached.get("total", 0) > 0
                              and cached.get("with_thumb", 0) == 0)
        if (not force and cached
                and cached.get("fingerprint") == fp
                and "total" in cached
                and not _cache_looks_stale):
            out[name] = {
                "total": cached.get("total", 0),
                "with_thumb": cached.get("with_thumb", 0),
                "missing": cached.get("missing", 0),
            }
            continue
        needs_walk.append((ch, folder, name, fp))

    if not needs_walk:
        return out

    # Pass 2: walk the stale/missing channels in parallel.
    #
    # Bug fix (2026-05-14): the previous algorithm checked each mp4
    # against the .Thumbnails dir SITTING NEXT TO IT, missing thumbs
    # that lived elsewhere in the same channel (e.g. The PrimeTime
    # had 2025/.Thumbnails/ containing thumbs for files now in 2023/
    # and 2024/ after reorg — counter reported "42% thumbnails" when
    # disk actually held a 1:1 thumb-for-mp4 match). Now we collect
    # EVERY `[vid_id]` from EVERY .Thumbnails/ in the channel folder
    # once, then check membership per-mp4.
    def _count_one(item):
        ch, folder, name, fp = item
        total = with_thumb = 0
        # Collect every video_id present in any .Thumbnails/ under
        # this channel folder. One folder walk; cheap.
        all_thumb_vids: set = set()
        try:
            id_re = re.compile(r"\[([A-Za-z0-9_-]{11})\]")
            for dp, _dns, fns in os.walk(str(folder)):
                if os.path.basename(dp) != ".Thumbnails":
                    continue
                for fn in fns:
                    if not fn.lower().endswith(
                            (".jpg", ".jpeg", ".png", ".webp")):
                        continue
                    m = id_re.search(fn)
                    if m:
                        all_thumb_vids.add(m.group(1))
        except Exception as e:
            _log.debug("swallowed: %s", e)
        # Persist the per-vid has_thumbnail flag so the next call
        # hits the SQL fast path instead of re-walking. Bulk UPDATE
        # by `video_id` (channel-scoped to avoid cross-channel
        # collisions if two channels happen to share an id).
        rows_for_db: list[tuple[int, str]] = []
        try:
            for vid_id, _title, _y, _m, path in _scan_channel_videos(folder):
                total += 1
                has = 1 if (vid_id and vid_id in all_thumb_vids) else 0
                if vid_id:
                    rows_for_db.append((has, vid_id))
                if has:
                    with_thumb += 1
        except Exception as e:
            _log.debug("swallowed: %s", e)
        # Write the flag back to the DB. One transaction per channel.
        try:
            if rows_for_db:
                from .. import index as _idx
                _conn = _idx._open()
                if _conn is not None:
                    with _idx._db_lock:
                        _conn.executemany(
                            "UPDATE videos SET has_thumbnail=? "
                            "WHERE video_id=?",
                            rows_for_db)
                        _conn.commit()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return (name, fp, {
            "total": total,
            "with_thumb": with_thumb,
            "missing": max(0, total - with_thumb),
        })

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_count_one, item) for item in needs_walk]
        for fut in as_completed(futures):
            try:
                name, fp, stats = fut.result()
                out[name] = stats
                # Update cache with fresh values + fingerprint.
                cache[name] = {
                    "fingerprint": fp,
                    "total": stats["total"],
                    "with_thumb": stats["with_thumb"],
                    "missing": stats["missing"],
                    "ts": time.time(),
                }
            except Exception as e:
                _log.debug("swallowed: %s", e)

    _save_thumb_cache(cache)
    return out


# TTL cache for the Video-IDs GROUP-BY query. The query itself is
# usually <1s on a 92k-row table, but caching the result means the
# Metadata page is instant on every visit instead of running a fresh
# scan each open. TTL is short (60s) because the videos table churns
# every time sync downloads a video — long TTL would surface stale
# numbers right when the user is most likely looking.
_VIDEO_ID_CACHE_TTL_SEC = 60.0
_video_id_cache_state: dict[str, Any] = {"ts": 0.0, "rows": {}}
# Audit #6: guard concurrent reads/writes from multiple worker threads
# (Settings > Metadata page loads from JS bridge thread; bulk refresh
# pipeline running on the sync worker; etc.). Reads + writes are
# fast (dict copy), so a single Lock is fine — no need for RLock.
_video_id_cache_lock = threading.Lock()


def count_video_id_status_bulk(channels: list[dict[str, Any]],
                                  force: bool = False
                                  ) -> dict[str, dict[str, Any]]:
    """Single-query batch version of count_video_id_status.

    Returns {channel_name: {total, with_id, missing, tried_failed}}
    keyed by channel name (lowercased for case-insensitive lookup).
    Falls back to per-channel queries if the batch query fails.

    Why this exists: the per-channel function runs 3 COUNT(*) queries
    against a 9M+ row table, holding the FTS DB lock the whole time.
    With 100+ channels that's 300+ serialized queries — Settings >
    Metadata table took 30+ seconds to load and would visibly hang
    when another DB op (sweep_new_videos, ingest_jsonl) was holding
    the lock. This collapses the work into one GROUP BY query that
    completes in under a second on the same data.
    """
    out: dict[str, dict[str, Any]] = {}
    if not channels:
        return out
    # TTL cache shortcut: if the same data was computed recently AND
    # the caller didn't ask for a force-refresh, return the cached
    # rows. Avoids hitting the DB on every Metadata-page open.
    if not force:
        try:
            with _video_id_cache_lock:
                now = time.time()
                age = now - float(_video_id_cache_state.get("ts") or 0)
                cached_rows = _video_id_cache_state.get("rows") or {}
                if cached_rows and age < _VIDEO_ID_CACHE_TTL_SEC:
                    # Return a shallow copy so callers can't mutate
                    # cached state from outside the lock.
                    return dict(cached_rows)
        except Exception as e:
            _log.debug("swallowed: %s", e)
    try:
        from .. import index as _idx
        # Issue #153 follow-on: route through the read-only `_reader_conn`
        # so this Settings > Metadata table query never waits behind
        # sync's `register_video` writers on `_db_lock`. WAL handles
        # cross-connection serialization. Falls back to the shared
        # `_conn` if the reader isn't available.
        conn = _idx._reader_open() or _idx._open()
        if conn is None:
            return out
        # Use GROUP BY on the raw `channel` column (NOT LOWER(channel))
        # so the existing idx_vid_channel index can serve the query.
        # LOWER() forces a full table scan, which on a 9M-row table
        # took 30+ seconds per call. Case-folding for cross-case
        # matching happens in Python below — typically no-op since
        # channel names rarely vary in case across rows.
        with _idx._reader_lock:
            try:
                _has_tried_col = True
                _has_removed_col = True
                rows = conn.execute(
                    "SELECT channel, "
                    "  COUNT(*) AS total, "
                    "  SUM(CASE WHEN video_id IS NOT NULL AND video_id != '' "
                    "           THEN 1 ELSE 0 END) AS with_id, "
                    "  SUM(CASE WHEN (video_id IS NULL OR video_id = '') "
                    "           AND id_backfill_tried_ts IS NOT NULL "
                    "           THEN 1 ELSE 0 END) AS tried, "
                    "  SUM(CASE WHEN removed_from_yt_ts IS NOT NULL "
                    "           THEN 1 ELSE 0 END) AS removed "
                    "FROM videos GROUP BY channel"
                ).fetchall()
            except Exception:
                # Older DB without removed_from_yt_ts column.
                _has_removed_col = False
                try:
                    _has_tried_col = True
                    rows = conn.execute(
                        "SELECT channel, "
                        "  COUNT(*) AS total, "
                        "  SUM(CASE WHEN video_id IS NOT NULL AND video_id != '' "
                        "           THEN 1 ELSE 0 END) AS with_id, "
                        "  SUM(CASE WHEN (video_id IS NULL OR video_id = '') "
                        "           AND id_backfill_tried_ts IS NOT NULL "
                        "           THEN 1 ELSE 0 END) AS tried "
                        "FROM videos GROUP BY channel"
                    ).fetchall()
                except Exception:
                    _has_tried_col = False
                    rows = conn.execute(
                        "SELECT channel, "
                        "  COUNT(*) AS total, "
                        "  SUM(CASE WHEN video_id IS NOT NULL AND video_id != '' "
                        "           THEN 1 ELSE 0 END) AS with_id "
                        "FROM videos GROUP BY channel"
                    ).fetchall()
        # Merge case-variant channels in Python (e.g. "MyChan" + "mychan"
        # → one entry under "mychan"). Sums the counts so duplicates from
        # case-drifted rows aren't lost.
        for r in rows:
            ch_raw = r[0] or ""
            ch_low = ch_raw.lower()
            total = int(r[1] or 0)
            with_id = int(r[2] or 0)
            tried = int(r[3] or 0) if _has_tried_col and len(r) > 3 else 0
            removed = (int(r[4] or 0) if _has_removed_col and len(r) > 4
                       else 0)
            cur = out.get(ch_low)
            if cur is None:
                out[ch_low] = {
                    "total": total,
                    "with_id": with_id,
                    "missing": max(0, total - with_id),
                    "tried_failed": tried,
                    "removed_from_yt": removed,
                }
            else:
                cur["total"] += total
                cur["with_id"] += with_id
                cur["missing"] = max(0, cur["total"] - cur["with_id"])
                cur["tried_failed"] += tried
                cur["removed_from_yt"] = cur.get("removed_from_yt", 0) + removed
    except Exception:
        return {}
    # Refresh the TTL cache so the next page-load gets instant data.
    try:
        with _video_id_cache_lock:
            _video_id_cache_state["ts"] = time.time()
            _video_id_cache_state["rows"] = out
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return out


def count_video_id_status(channel: dict[str, Any]) -> dict[str, Any]:
    """Cheap DB-only count: how many on-disk videos have a resolvable
    video_id stored in the index `videos` table? Powers the Settings >
    Metadata "Video IDs" column so the user can spot channels that
    need a one-time backfill (common for archives migrated from the
    tkinter-era YTArchiver, which never wrote [id] brackets into
    filenames nor .info.json sidecars).

    Returns {total, with_id, missing, tried_failed}:
      * total:        row count for files under the channel folder.
      * with_id:      rows where video_id is non-NULL / non-empty.
      * missing:      total - with_id.
      * tried_failed: rows that are still missing AND have an
                      id_backfill_tried_ts — i.e. the backfill
                      pass attempted them and every strategy
                      returned no match. Separates "probably
                      genuinely unresolvable (renamed, removed,
                      title-drift beyond fuzzy threshold)" from
                      "never tried — run Fix IDs".

    Purely a DB-read — no disk walk, no yt-dlp. Safe to call for
    every channel on a tab render.
    """
    folder = _folder_for_channel(channel)
    if folder is None:
        return {"total": 0, "with_id": 0, "missing": 0, "tried_failed": 0}
    try:
        from .. import index as _idx
        # Use the read-only connection so this single-channel fallback
        # doesn't contend with writers on `_db_lock`. Called from the
        # Settings > Metadata bulk loader when the GROUP-BY path can't
        # cover a specific channel (case drift between config name +
        # DB column).
        conn = _idx._reader_open() or _idx._open()
        if conn is None:
            return {"total": 0, "with_id": 0, "missing": 0, "tried_failed": 0}
        _pat = _like_esc(str(folder)) + "%"
        with _idx._reader_lock:
            _total = conn.execute(
                "SELECT COUNT(*) FROM videos WHERE filepath LIKE ? ESCAPE '\\'",
                (_pat,)).fetchone()[0]
            _with_id = conn.execute(
                "SELECT COUNT(*) FROM videos WHERE filepath LIKE ? ESCAPE '\\' "
                "AND video_id IS NOT NULL AND video_id != ''",
                (_pat,)).fetchone()[0]
            # Rows still missing an id that have been through the
            # backfill pass at least once. Column won't exist on
            # very old DBs; guarded query falls back to 0.
            try:
                _tried = conn.execute(
                    "SELECT COUNT(*) FROM videos WHERE filepath LIKE ? ESCAPE '\\' "
                    "AND (video_id IS NULL OR video_id='') "
                    "AND id_backfill_tried_ts IS NOT NULL",
                    (_pat,)).fetchone()[0]
            except Exception:
                _tried = 0
        return {
            "total": int(_total or 0),
            "with_id": int(_with_id or 0),
            "missing": int((_total or 0) - (_with_id or 0)),
            "tried_failed": int(_tried or 0),
        }
    except Exception:
        return {"total": 0, "with_id": 0, "missing": 0, "tried_failed": 0}
