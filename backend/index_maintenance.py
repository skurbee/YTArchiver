"""
index_maintenance — archive sweep + prune + FTS rebuild.

Extracted from backend/index.py (Patch 20, v72.2). Three top-level
maintenance entry points:

    sweep_new_videos(output_dir, channels, progress_cb=None,
                     gpu_busy_fn=None) -> dict
        — walk each channel folder, register any video file not already
          in `videos`, ingest paired `.jsonl` sidecars into the FTS
          segments table. Honors a busy-GPU gate so it yields rather
          than competes with an active retranscribe for the SQLite
          single-writer slot.

    prune_missing_videos() -> dict
        — drop rows from `videos` / `segments` whose file no longer
          exists on disk. Used by Settings → Rescan.

    rebuild_fts_index() -> dict
        — wipe the FTS5 table and rebuild it from scratch by re-ingesting
          every `.jsonl` on disk. Settings → Rebuild button drives this.

Connection + lock primitives come from index.py via `_idx`.
"""
from __future__ import annotations

import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from . import index as _idx
from .log import get_logger

_log = get_logger(__name__)


def sweep_new_videos(output_dir: str, channels: list,
                     progress_cb=None,
                     gpu_busy_fn=None) -> dict:
    """Walk each channel folder under `output_dir`, register any video
    file not already in the videos table, and ingest any paired .jsonl
    that isn't in segments yet.

    Matches YTArchiver's disk-scan behavior at :3012 _scan_channel_disk_info —
    picks up files added manually or while the app was closed.

    Optional `progress_cb(idx, total, channel_name)` is invoked as each
    channel starts so the caller can update a "Loading… N/M (channel)"
    status line. Called on the same thread as the walk.

    Returns {registered, ingested} counts.

    The sweep uses its OWN sqlite3 connection (via _idx._open_independent)
    so its many per-file writes don't go through the shared `_idx._db_lock`.
    Without this, sync's DLTRACK register_video calls + transcribe's
    FTS-ingest calls all serialized behind the sweep's lock acquisition,
    causing visible "Downloading 100%" hangs of many minutes during
    boot. WAL mode handles cross-connection serialization at the
    SQLite layer instead.
    """
    import os as _os
    from pathlib import Path as _Path

    if not output_dir:
        return {"registered": 0, "ingested": 0}
    import time as _t
    # Yield-loop: defer sweep while user-initiated GPU work (retranscribe,
    # manual transcribe) is running. Sweep's many small writes to its own
    # connection still compete with the active job for SQLite's single-
    # writer slot at the file level — observed: retranscribes stuck at
    # 99% for 6-8 minutes while sweep ran. User-initiated work always
    # wins; sweep waits its turn (capped at 10 minutes so a bug in
    # gpu_busy_fn doesn't deadlock the boot sequence).
    if callable(gpu_busy_fn):
        _waited = 0.0
        try:
            while gpu_busy_fn() and _waited < 600.0:
                _t.sleep(0.5)
                _waited += 0.5
        except Exception:
            pass
    # Make sure the shared connection's schema-init has run at least
    # once (creates tables, sets PRAGMAs at the file level).
    _ = _idx._open()
    sweep_conn = _idx._open_independent()
    if sweep_conn is None:
        return {"registered": 0, "ingested": 0}

    _VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v",
                   ".wav", ".mp3", ".m4a", ".flac")
    registered = 0
    ingested = 0

    # Cache existing filepaths to avoid hitting the DB per file. Use
    # the sweep's private connection — readers in WAL mode never block
    # writers, so this doesn't compete with anything.
    existing = {r[0].lower() for r in sweep_conn.execute("SELECT filepath FROM videos").fetchall()
                if r[0]}
    indexed_jsonls = {r[0].lower() for r in sweep_conn.execute("SELECT path FROM indexed_files").fetchall()
                      if r[0]}

    # Per-channel folder fingerprint — lets us skip channels whose
    # folder tree hasn't been touched since the last successful sweep.
    # Matters because the enumeration itself (scandir of 100k entries
    # across Z:\ DrivePool) is the slow part; even the stat-free walk
    # takes minutes on archive. Fingerprint = recursive mtime
    # max across the channel root + all subdirectories (year, month).
    # Windows updates a folder's mtime when its entries change, so if
    # a new download landed anywhere in the tree, at least one
    # directory's mtime will be later than the last saved fingerprint.
    # Videos getting MODIFIED in place (without adding/removing
    # entries) wouldn't bump the mtime — fine, since sweep's job is
    # only to catch newly-added files.
    from .archive_scan import load_disk_cache as _load_dc
    from .archive_scan import save_disk_cache as _save_dc
    _fp_cache = _load_dc()
    # Map channel URL → folder_fingerprint stored in the disk cache.
    def _folder_fingerprint(ch_folder: _Path) -> float:
        """Return max mtime across the channel folder + immediate
        subdirs (one level deep is enough because yt-dlp always
        writes into yyyy/... or yyyy/MM.../ and those intermediate
        dirs always get bumped when a new file is written under them).
        A handful of stat calls per channel — cheap."""
        try:
            mx = ch_folder.stat().st_mtime
        except OSError:
            return 0.0
        try:
            for entry in _os.scandir(ch_folder):
                try:
                    if entry.is_dir(follow_symlinks=False):
                        try:
                            m = entry.stat(follow_symlinks=False).st_mtime
                            if m > mx:
                                mx = m
                            # One extra level for year/month splits.
                            for sub in _os.scandir(entry.path):
                                try:
                                    if sub.is_dir(follow_symlinks=False):
                                        sm = sub.stat(follow_symlinks=False).st_mtime
                                        if sm > mx:
                                            mx = sm
                                except OSError:
                                    pass
                        except OSError:
                            pass
                except OSError:
                    pass
        except OSError:
            pass
        return mx

    total_ch = len(channels)
    skipped_unchanged = 0
    for i_ch, ch in enumerate(channels):
        ch_name = ch.get("name") or ch.get("folder", "")
        if not ch_name:
            continue
        # Mid-sweep yield: if a retranscribe / manual transcribe kicked
        # off after sweep started, pause here too. Same rationale as the
        # pre-sweep wait above. Cap at 10 minutes per channel boundary.
        if callable(gpu_busy_fn):
            _yielded = 0.0
            try:
                while gpu_busy_fn() and _yielded < 600.0:
                    _t.sleep(0.5)
                    _yielded += 0.5
            except Exception:
                pass
        if progress_cb is not None:
            try: progress_cb(i_ch + 1, total_ch, ch_name)
            except Exception as e: _log.debug("swallowed: %s", e)
        folder = _Path(output_dir) / ch_name
        if not folder.is_dir():
            continue
        # Fingerprint-skip: if this channel's folder tree hasn't been
        # touched (by file add/remove) since the last successful
        # sweep, skip the walk entirely. Drops a 4-minute full sweep
        # to seconds on a steady-state archive.
        ch_url = (ch.get("url") or "").strip()
        current_fp = _folder_fingerprint(folder)
        last_fp_cache_entry = _fp_cache.get(ch_url, {}) if ch_url else {}
        last_fp = float(last_fp_cache_entry.get("sweep_fingerprint", 0) or 0)
        if current_fp > 0 and last_fp > 0 and current_fp <= last_fp:
            skipped_unchanged += 1
            continue
        # Either never swept before or the folder changed — walk it.
        # Use scandir directly so we get DirEntry objects with cached
        # stat info — avoids a separate `os.path.getsize` disk round
        # trip per file. Walk recursively by yielding directories
        # from the parent scan. On a 100k-file archive across Z:\
        # (DrivePool, network-ish latency per stat), this is the
        # difference between a ~30s sweep and a multi-minute one.
        import re as _re
        _strip_id = _re.compile(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$")
        stack = [str(folder)]
        while stack:
            dp = stack.pop()
            try:
                it = _os.scandir(dp)
            except OSError:
                continue
            with it:
                for entry in it:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
                            continue
                    except OSError:
                        continue
                    fn = entry.name
                    low = fn.lower()
                    if not low.endswith(_VIDEO_EXTS):
                        continue
                    if "_temp_compress" in low or low.endswith(".part"):
                        continue
                    # yt-dlp intermediate track suffix check (`.f140-7.m4a`)
                    _stem = _os.path.splitext(fn)[0]
                    _dot = _stem.rfind(".")
                    if _dot >= 0:
                        _tail = _stem[_dot + 1:]
                        if (_tail and _tail[0].lower() == "f"
                                and len(_tail) >= 2
                                and _tail[1:].replace("-", "").isdigit()):
                            continue
                    # Check EXISTING-IN-DB first — most files in a
                    # normal launch are already registered. No stat
                    # call needed for them. Previously the sweep
                    # called getsize() on every file before checking
                    # `in existing`, wasting 99% of stat budget on a
                    # steady-state archive.
                    fp = _os.path.normpath(entry.path)
                    fp_lower = fp.lower()
                    if fp_lower in existing:
                        # Already registered; check if a .jsonl
                        # sidecar is present and not yet ingested.
                        # `indexed_jsonls` check first (pure set
                        # lookup) so we only hit the disk with
                        # isfile() when we actually care.
                        base = _os.path.splitext(fp)[0]
                        jp = base + ".jsonl"
                        jp_lower = _os.path.normpath(jp).lower()
                        if (jp_lower not in indexed_jsonls
                                and _os.path.isfile(jp)):
                            title = _strip_id.sub("", _os.path.basename(base)) or _os.path.basename(base)
                            # Pass sweep_conn so this call doesn't compete
                            # for _idx._db_lock — see _idx._open_independent docstring.
                            if _idx.ingest_jsonl(fp, jp, title, ch_name,
                                            _conn_override=sweep_conn):
                                ingested += 1
                        continue
                    # New file — need size now (both for 0-byte skip
                    # and for register_video's size_bytes column).
                    try:
                        size = entry.stat(follow_symlinks=False).st_size
                    except OSError:
                        continue
                    if size == 0:
                        continue
                    _idx.register_video(fp, ch_name, _conn_override=sweep_conn)
                    registered += 1
                    # Ingest .jsonl sidecar if present.
                    base = _os.path.splitext(fp)[0]
                    jp = base + ".jsonl"
                    if _os.path.isfile(jp):
                        title = _strip_id.sub("", _os.path.basename(base)) or _os.path.basename(base)
                        if _idx.ingest_jsonl(fp, jp, title, ch_name,
                                        _conn_override=sweep_conn):
                            ingested += 1
        # Channel walk completed — stamp the fingerprint so next
        # sweep can skip if unchanged. Stamp AFTER the walk so a
        # crash mid-walk doesn't leave a stale "skip me" flag.
        # issue #134: only stamp onto an already-populated entry.
        # If the row is missing (e.g. just invalidated by a redownload
        # before its background rescan finished), creating a fingerprint-
        # only entry here would leave num_vids/size_bytes = 0 in the
        # Subs table and survive restart (staleness check skips the next
        # walk). Let `update_disk_cache_for_channel` own the initial
        # populate; next sweep will walk this channel again, which is
        # cheap compared to the bug.
        if ch_url:
            existing_row = _fp_cache.get(ch_url)
            # tightened to `and` — update_disk_cache_for_channel
            # always writes BOTH fields together, so a row with only one
            # is itself a corruption case we don't want to cement by
            # adding a fingerprint on top.
            if isinstance(existing_row, dict) and (
                    "num_vids" in existing_row
                    and "size_bytes" in existing_row):
                existing_row["sweep_fingerprint"] = current_fp

    # Persist the updated fingerprint cache.
    if skipped_unchanged < total_ch:
        try:
            _save_dc(_fp_cache)
        except Exception as e:
            _log.debug("swallowed: %s", e)

    # Close the sweep's private connection — best-effort, don't fail the
    # whole sweep if close raises (DB file is fine either way).
    try:
        sweep_conn.close()
    except Exception as e:
        _log.debug("swallowed: %s", e)

    return {"registered": registered, "ingested": ingested,
            "skipped_unchanged": skipped_unchanged,
            "walked": total_ch - skipped_unchanged}


def prune_missing_videos() -> dict[str, int]:
    """Delete stale/phantom video rows from the DB. Cleanup categories:

      1. `missing` — filepath no longer exists on disk. Dead
                      `(1)` duplicates, deleted files, etc.
      2. `zero_byte` — file exists but is 0 bytes. Phantom
                       placeholders from failed downloads (
                       a user's channel "Intel just did an AMD" 0-byte
                       file that my title-matcher then mis-assigned
                       the real video's id to, producing duplicate
                       grid rows with shared thumbnails).
      3. `duplicate_id` — multiple rows share the same video_id.
                          Keep the row with the largest `size_bytes`
                          (presumed real file), drop the rest.

    Segments + FTS entries tied to removed video_ids also get dropped
    so ghost search hits don't linger. Returns per-category counts.
    """
    import os as _os
    conn = _idx._open()
    if conn is None:
        return {"videos_removed": 0, "segments_removed": 0,
                "missing": 0, "zero_byte": 0, "duplicate_id": 0}
    videos_removed = 0
    segs_removed = 0
    n_missing = n_zero = n_dup = n_fake_id = 0
    affected_channels: set = set()
    try:
        with _idx._db_lock:
            # Category 0: null out all-alphabetic video_ids. These are
            # filename-suffix parse errors (a user's channel files ending in
            # `[a-user-channel]` that matched `[A-Za-z0-9_-]{11}` but
            # aren't real YT ids). The row stays — it's a real file
            # on disk — but its video_id field gets cleared so the
            # next metadata recheck will title-resolve it properly
            # instead of treating 13 different files as duplicates of
            # one fake id.
            fake_rows = conn.execute(
                "SELECT id, channel, video_id FROM videos "
                "WHERE video_id IS NOT NULL AND video_id != '' "
                "AND length(video_id) = 11").fetchall()
            fake_ids_to_null = [
                rid for rid, _ch, _v in fake_rows if _v and _v.isalpha()
            ]
            if fake_ids_to_null:
                for rid, _ch, _v in fake_rows:
                    if _v and _v.isalpha():
                        conn.execute(
                            "UPDATE videos SET video_id=NULL, "
                            "video_url=NULL WHERE id=?", (rid,))
                        n_fake_id += 1
                        if _ch:
                            affected_channels.add(_ch)
            # Category 1 + 2: missing files and 0-byte files.
            rows = conn.execute(
                "SELECT filepath FROM videos").fetchall()
            to_delete_fps = []
            for r in rows:
                fp = (r[0] or "").strip()
                if not fp:
                    continue
                if not _os.path.isfile(fp):
                    to_delete_fps.append((fp, "missing"))
                    continue
                try:
                    if _os.path.getsize(fp) == 0:
                        to_delete_fps.append((fp, "zero_byte"))
                except OSError:
                    to_delete_fps.append((fp, "missing"))

            for fp, cat in to_delete_fps:
                vid_row = conn.execute(
                    "SELECT video_id, channel FROM videos WHERE filepath=? "
                    "COLLATE NOCASE LIMIT 1", (fp,)).fetchone()
                vid = (vid_row[0] if vid_row else "") or ""
                _ch = (vid_row[1] if vid_row and len(vid_row) > 1 else "") or ""
                if _ch:
                    affected_channels.add(_ch)
                # Only drop segments if this is the LAST row holding
                # that video_id — otherwise we'd orphan search hits
                # from the surviving real-file row.
                if vid:
                    other = conn.execute(
                        "SELECT COUNT(*) FROM videos WHERE video_id=? "
                        "AND filepath != ? COLLATE NOCASE",
                        (vid, fp)).fetchone()
                    if not other or other[0] == 0:
                        # cascade the segment delete into
                        # the FTS external-content table so the
                        # rowids we just orphaned can't keep
                        # producing phantom search hits. Using
                        # segments_fts's special 'delete' command
                        # would require per-row text, so just drop
                        # every fts row whose rowid is no longer in
                        # segments. Simpler + bulletproof.
                        _seg_ids = [r[0] for r in conn.execute(
                            "SELECT id FROM segments WHERE video_id=?",
                            (vid,)).fetchall()]
                        c1 = conn.execute(
                            "DELETE FROM segments WHERE video_id=?",
                            (vid,))
                        segs_removed += c1.rowcount or 0
                        # Best-effort FTS delete. Skip silently if
                        # the segments_fts table doesn't exist (very
                        # old DB).
                        if _seg_ids:
                            try:
                                # Chunk to stay under SQLite's bound
                                # parameter limit (999 default).
                                for _start in range(0, len(_seg_ids), 500):
                                    _chunk = _seg_ids[_start:_start + 500]
                                    _ph = ",".join("?" * len(_chunk))
                                    conn.execute(
                                        f"DELETE FROM segments_fts "
                                        f"WHERE rowid IN ({_ph})",
                                        _chunk)
                            except Exception as e:
                                _log.debug("swallowed: %s", e)
                c2 = conn.execute(
                    "DELETE FROM videos WHERE filepath=? COLLATE NOCASE",
                    (fp,))
                deleted_here = c2.rowcount or 0
                videos_removed += deleted_here
                if cat == "missing":
                    n_missing += deleted_here
                else:
                    n_zero += deleted_here

            # Category 3: multiple rows share the same video_id —
            # redundant downloads of the same YouTube video. Rather
            # than delete rows or files (files are on Z:\ which is
            # read-only per project rule), mark the non-primary ones
            # as duplicates via `is_duplicate_of=<primary filepath>`.
            # The Browse grid filter hides these so it matches what
            # YouTube shows (one entry per video), while the files
            # stay on disk for the user to manage manually.
            dup_vids = [r[0] for r in conn.execute(
                "SELECT video_id FROM videos "
                "WHERE video_id IS NOT NULL AND video_id != '' "
                "AND is_duplicate_of IS NULL "
                "GROUP BY video_id HAVING COUNT(*) > 1").fetchall()]
            for vid in dup_vids:
                rows = conn.execute(
                    "SELECT id, filepath, size_bytes, channel FROM videos "
                    "WHERE video_id=? AND is_duplicate_of IS NULL "
                    "ORDER BY COALESCE(size_bytes, 0) DESC, id ASC",
                    (vid,)).fetchall()
                keep_fp = rows[0][1]
                for rid, _fp, _sz, _ch in rows[1:]:
                    c = conn.execute(
                        "UPDATE videos SET is_duplicate_of=? WHERE id=?",
                        (keep_fp, rid))
                    flagged = c.rowcount or 0
                    n_dup += flagged
                    if _ch:
                        affected_channels.add(_ch)
            conn.commit()
        # Drop the Browse grid cache for every channel that had a
        # row removed or flagged — the cache is keyed by
        # (channel, sort, limit, include_thumbs) and lives inside
        # _browse_videos_cache. Without this, the grid keeps
        # showing the pre-prune list for up to
        # BROWSE_CACHE_TTL_SEC after the click.
        for _ch in affected_channels:
            try:
                _idx.invalidate_channel_videos(_ch)
            except Exception as e:
                _log.debug("swallowed: %s", e)
    except Exception as e:
        print(f"[index] prune_missing_videos failed: {e}")
    return {"videos_removed": videos_removed,
            "segments_removed": segs_removed,
            "missing": n_missing, "zero_byte": n_zero,
            "duplicate_id": n_dup,
            "fake_id_cleared": n_fake_id}


# ── Rebuild FTS index from scratch (rebuild button on Index tab) ────────

def rebuild_fts_index() -> dict[str, Any]:
    """Drop segments_fts virtual table and rebuild it by reinserting every
    row from segments. Safe to run — preserves the segments table itself.
    Returns {ok, rows_indexed} or {ok: False, error}.
    Use when FTS seems broken (search returns nothing despite visible segments)
    or after a DB schema migration.
    """
    conn = _idx._open()
    if conn is None:
        return {"ok": False, "error": "DB unavailable"}
    try:
        with _idx._db_lock:
            conn.execute("DROP TABLE IF EXISTS segments_fts")
            conn.execute("""CREATE VIRTUAL TABLE segments_fts USING fts5(
                text,
                content=segments,
                content_rowid=id
            )""")
            conn.execute(
                "INSERT INTO segments_fts (rowid, text) "
                "SELECT id, text FROM segments"
            )
            rows = conn.execute("SELECT COUNT(*) FROM segments_fts").fetchone()[0]
            # `indexed_files` (the table used to compute the
            # "unindexed transcripts" warning banner) is only populated
            # by _idx.ingest_jsonl. A pure FTS rebuild would leave the banner
            # claiming "N unindexed" even though every segment just got
            # re-indexed. Refresh indexed_files from the segments table
            # so the banner reflects reality.
            conn.execute("DELETE FROM indexed_files")
            conn.execute(
                "INSERT OR REPLACE INTO indexed_files(path, mtime, segment_count) "
                "SELECT jsonl_path, 0, COUNT(*) "
                "FROM segments WHERE jsonl_path IS NOT NULL "
                "GROUP BY jsonl_path"
            )
            conn.commit()
        return {"ok": True, "rows_indexed": int(rows)}
    except sqlite3.Error as e:
        return {"ok": False, "error": str(e)}
