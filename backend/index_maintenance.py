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
import sqlite3
import threading
from typing import Any

from . import index as _idx
from .fs_search import is_partial_artifact
from .log import get_logger

_log = get_logger(__name__)


# Every sweep covers the same archive and performs the same reconciliation.
# Startup, the post-download auto-index threshold, and the manual Rescan button
# can all request one at nearly the same time.  Let exactly one caller do the
# work; followers wait for it instead of opening a second SQLite writer and
# fighting the first sweep for several minutes.
_sweep_singleflight = threading.Condition()
_sweep_running = False


def _coalesced_sweep_result() -> dict[str, int | bool]:
    return {
        "registered": 0,
        "ingested": 0,
        "agg_ingested": 0,
        "id_backfilled": 0,
        "availability_missing": 0,
        "availability_restored": 0,
        "tx_reconciled": 0,
        "tx_reconciled_by_id": 0,
        "tx_reconciled_by_title": 0,
        "skipped_unchanged": 0,
        "walked": 0,
        "coalesced": True,
    }


def sweep_new_videos(output_dir: str, channels: list,
                     progress_cb=None, gpu_busy_fn=None) -> dict:
    """Run one archive sweep process-wide and coalesce concurrent callers."""
    global _sweep_running
    with _sweep_singleflight:
        if _sweep_running:
            while _sweep_running:
                _sweep_singleflight.wait()
            return _coalesced_sweep_result()
        _sweep_running = True
    try:
        return _sweep_new_videos_impl(
            output_dir, channels, progress_cb=progress_cb,
            gpu_busy_fn=gpu_busy_fn)
    finally:
        with _sweep_singleflight:
            _sweep_running = False
            _sweep_singleflight.notify_all()


def _jsonl_needs_ingest(conn: sqlite3.Connection, jsonl_path: str) -> bool:
    """True when a sidecar exists and indexed_files has no matching mtime."""
    jp = os.path.normpath(jsonl_path)
    if not os.path.isfile(jp):
        return False
    try:
        mtime = os.path.getmtime(jp)
        row = conn.execute(
            "SELECT mtime FROM indexed_files WHERE path=? LIMIT 1",
            (jp,)).fetchone()
        if row is None:
            return True
        return float(row[0] or 0) != mtime
    except (OSError, sqlite3.Error, TypeError, ValueError):
        return True


def _reconcile_tx_status_from_transcript_titles(
        conn: sqlite3.Connection,
        output_dir: str,
        channels: list,
        wait_fn=None) -> int:
    """Flip stale pending rows when aggregate transcript text proves done.

    Older aggregate Transcript.txt files may not have video IDs in their
    headers/jsonl. Transcribe All still treats those entries as complete by
    normalized title; the Health tab must use the same evidence or it reports
    stale pending counts forever.
    """
    if conn is None or not output_dir or not channels:
        return 0

    import re as _re
    from .transcribe.helpers import (
        _norm_title,
        _scan_existing_transcript_titles,
    )

    strip_id = _re.compile(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$")

    def _maybe_wait() -> None:
        if callable(wait_fn):
            try:
                wait_fn()
            except Exception:
                pass

    def _folder_name(ch: dict[str, Any]) -> str:
        try:
            from .sync import channel_folder_name as _cfn
            return _cfn(ch)
        except Exception:
            return ((ch.get("folder_override") or "").strip()
                    or (ch.get("folder") or "").strip()
                    or (ch.get("name") or "").strip())

    def _names_for(ch: dict[str, Any], folder_name: str) -> list[str]:
        names: list[str] = []
        for val in (ch.get("name"), ch.get("folder"), ch.get("folder_override"),
                    folder_name):
            val = (val or "").strip()
            if val and val.lower() not in {n.lower() for n in names}:
                names.append(val)
        return names

    total_changed = 0
    for ch in channels:
        if not isinstance(ch, dict):
            continue
        ch_name = (ch.get("name") or "").strip()
        folder_name = _folder_name(ch)
        if not folder_name:
            continue
        folder = os.path.join(output_dir, folder_name)
        if not os.path.isdir(folder):
            continue

        names = _names_for(ch, folder_name)
        if not names:
            continue
        where = " OR ".join(["channel=? COLLATE NOCASE"] * len(names))
        try:
            rows = conn.execute(
                "SELECT id, title, filepath, video_id FROM videos "
                f"WHERE ({where}) "
                "AND COALESCE(tx_status, 'pending') != 'transcribed'",
                names,
            ).fetchall()
        except sqlite3.Error as e:
            _log.debug("tx_status title reconcile query failed (%s): %s",
                       ch_name or folder_name, e)
            continue
        if not rows:
            continue

        _maybe_wait()
        already = _scan_existing_transcript_titles(
            folder, ch_name or folder_name)
        if not already:
            continue
        done_vids = {
            vid for (_raw, vid) in already.values()
            if (vid or "").strip()
        }

        ids: list[int] = []
        for n, row in enumerate(rows, start=1):
            if n % 100 == 0:
                _maybe_wait()
            row_id, title, filepath, video_id = row
            vid = (video_id or "").strip()
            if vid and vid in done_vids:
                ids.append(int(row_id))
                continue

            candidates = []
            title_s = (title or "").strip()
            if title_s:
                candidates.append(title_s)
                plain = strip_id.sub("", title_s).strip()
                if plain and plain != title_s:
                    candidates.append(plain)
            fp_s = (filepath or "").strip()
            if fp_s:
                stem = os.path.splitext(os.path.basename(fp_s))[0].strip()
                if stem:
                    candidates.append(stem)
                    plain = strip_id.sub("", stem).strip()
                    if plain and plain != stem:
                        candidates.append(plain)

            if any(_norm_title(c) in already for c in candidates if c):
                ids.append(int(row_id))

        if not ids:
            continue
        for start in range(0, len(ids), 500):
            chunk = ids[start:start + 500]
            placeholders = ",".join(["?"] * len(chunk))
            cur = conn.execute(
                "UPDATE videos SET tx_status='transcribed' "
                f"WHERE id IN ({placeholders})",
                chunk,
            )
            total_changed += cur.rowcount or 0
        conn.commit()

    return total_changed


def _sweep_new_videos_impl(output_dir: str, channels: list,
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
    # max_wait is a safety cap against a wedged busy signal, NOT a normal
    # exit path. The caller's gate now keys off live thread/job state
    # (sync worker alive, single-download alive, GPU job, per-channel
    # active) which can't get stuck True, so the cap can be generous: a
    # full 105-channel pass with downloads + transcription routinely
    # exceeds the old 600s, and barging ahead there made the sweep
    # compete with the user's active download for the Z: pool — the exact
    # thing this gate exists to prevent. One hour comfortably outlasts any
    # pass while still bounding a genuine wedge.
    def _wait_while_busy(max_wait: float = 3600.0) -> bool:
        """Pause low-priority sweep work while user-visible work is active."""
        if not callable(gpu_busy_fn):
            return False
        _waited = 0.0
        try:
            while gpu_busy_fn() and _waited < max_wait:
                _t.sleep(0.5)
                _waited += 0.5
        except Exception:
            return False
        return _waited > 0

    # Yield-loop: defer sweep while active sync or user-initiated GPU work is
    # running. User-visible work wins over startup maintenance.
    _wait_while_busy()
    # Make sure the shared connection's schema-init has run at least
    # once (creates tables, sets PRAGMAs at the file level).
    _ = _idx._open()
    sweep_conn = _idx._open_independent()
    if sweep_conn is None:
        return {"registered": 0, "ingested": 0}

    from .fs_search import MEDIA_EXTS_TUPLE as _VIDEO_EXTS  # unified media set
    registered = 0
    ingested = 0
    id_backfilled = 0
    availability_missing = 0
    availability_restored = 0

    # `existing` is built per-channel inside the loop below — was
    # previously a single SELECT-fetchall across the entire videos
    # table at sweep start, which on a 200k-row archive pinned a
    # multi-MB set in memory for the entire sweep duration. Per-channel
    # scoping bounds memory to one channel's filepaths at a time, and
    # uses the idx_vid_channel index so each query is fast.
    # indexed_files is checked per sidecar via its PRIMARY KEY instead
    # of loading the entire table into a sweep-long set.

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
        # Use scandir as a context manager so the underlying directory
        # handle is released promptly. Without `with`, the generator
        # holds the handle until GC, which on Z: DrivePool + antivirus
        # can produce transient access failures (audit:
        # index_maintenance.py:122).
        try:
            with _os.scandir(ch_folder) as _it:
                for entry in _it:
                    _wait_while_busy()
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            try:
                                m = entry.stat(follow_symlinks=False).st_mtime
                                if m > mx:
                                    mx = m
                                # One extra level for year/month splits.
                                with _os.scandir(entry.path) as _it2:
                                    for sub in _it2:
                                        _wait_while_busy()
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
        # Mid-sweep yield: if sync/GPU work kicked off after sweep started,
        # pause here too. Same rationale as the pre-sweep wait above.
        _wait_while_busy()
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
        # Load `existing` scoped to JUST this channel so the membership
        # check below is fast without holding every filepath in memory
        # across the entire sweep. Uses idx_vid_channel.
        existing = set()
        noid = set()
        existing_paths: dict[str, str] = {}
        known_missing = set()
        for _er in sweep_conn.execute(
                "SELECT filepath, video_id, availability FROM videos "
                "WHERE channel=? COLLATE NOCASE", (ch_name,)).fetchall():
            if not _er[0]:
                continue
            _efpl = _er[0].lower()
            existing.add(_efpl)
            existing_paths[_efpl] = _er[0]
            if not (_er[1] or "").strip():
                noid.add(_efpl)
            if (_er[2] or "available") == "missing":
                known_missing.add(_efpl)
        # Reconciliation only applies to rows that existed before this walk.
        # Newly registered files are already available by definition and do
        # not need a second UPDATE (this also keeps empty/new channels cheap).
        _catalog_existing = set(existing)
        # Use scandir directly so we get DirEntry objects with cached
        # stat info — avoids a separate `os.path.getsize` disk round
        # trip per file. Walk recursively by yielding directories
        # from the parent scan. On a 100k-file archive across Z:\
        # (DrivePool, network-ish latency per stat), this is the
        # difference between a ~30s sweep and a multi-minute one.
        import re as _re
        _strip_id = _re.compile(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$")
        stack = [str(folder)]
        _walk_complete = True
        _seen_existing: set[str] = set()
        while stack:
            _wait_while_busy()
            dp = stack.pop()
            try:
                it = _os.scandir(dp)
            except OSError:
                # Never infer "missing" from an incomplete DrivePool/network
                # walk. Registration can continue in readable directories,
                # but reconciliation for this entire channel is skipped.
                _walk_complete = False
                continue
            with it:
                _entry_count = 0
                for entry in it:
                    _entry_count += 1
                    if _entry_count % 25 == 0:
                        _wait_while_busy()
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
                            continue
                    except OSError:
                        _walk_complete = False
                        continue
                    fn = entry.name
                    low = fn.lower()
                    if not low.endswith(_VIDEO_EXTS):
                        continue
                    # One canonical final-file classifier for every archive
                    # walker. The old local copy missed `.temp.mp4`, which is
                    # how a 488 MB yt-dlp intermediate became a permanent
                    # Browse card even after the file disappeared.
                    if is_partial_artifact(fn, dp):
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
                        _seen_existing.add(fp_lower)
                        # Already registered but with NO video_id — re-register
                        # so register_video's direct .info.json read backfills
                        # the id. The sweep would otherwise skip this row
                        # forever, leaving the id (and thus metadata)
                        # permanently missing. Scoped to NULL-id rows only, so
                        # it's near-free on a healthy archive.
                        if fp_lower in noid:
                            _wait_while_busy()
                            try:
                                if _idx.register_video(
                                        fp, ch_name,
                                        _conn_override=sweep_conn):
                                    id_backfilled += 1
                                    noid.discard(fp_lower)
                            except Exception as _bfe:
                                _log.debug("sweep id-backfill failed (%s): %s",
                                           fp, _bfe)
                        # Already registered; check if a .jsonl
                        # sidecar is present and either missing from
                        # indexed_files or newer than the indexed mtime.
                        base = _os.path.splitext(fp)[0]
                        jp = base + ".jsonl"
                        if _jsonl_needs_ingest(sweep_conn, jp):
                            title = _strip_id.sub("", _os.path.basename(base)) or _os.path.basename(base)
                            # Pass sweep_conn so this call doesn't compete
                            # for _idx._db_lock — see _idx._open_independent docstring.
                            _wait_while_busy()
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
                    _wait_while_busy()
                    if not _idx.register_video(
                            fp, ch_name, _conn_override=sweep_conn):
                        continue
                    registered += 1
                    existing.add(fp_lower)
                    existing_paths[fp_lower] = fp
                    # Ingest .jsonl sidecar if present.
                    base = _os.path.splitext(fp)[0]
                    jp = base + ".jsonl"
                    if _os.path.isfile(jp):
                        title = _strip_id.sub("", _os.path.basename(base)) or _os.path.basename(base)
                        _wait_while_busy()
                        if _idx.ingest_jsonl(fp, jp, title, ch_name,
                                            _conn_override=sweep_conn):
                            ingested += 1
        # The complete channel walk is the cheapest reliable source of file
        # availability: no extra 100k-file stat pass is needed. Mark catalog
        # rows seen in this folder available, and rows formerly under this
        # folder but not seen missing. If *any* scandir failed above, make no
        # missing judgments so transient Z:\\/DrivePool errors cannot hide a
        # channel. Partial rows retain their quarantined state.
        if _walk_complete:
            _folder_abs = _os.path.normcase(_os.path.abspath(str(folder)))

            def _under_folder(_path: str) -> bool:
                try:
                    _p = _os.path.normcase(_os.path.abspath(_path))
                    return _os.path.commonpath([_folder_abs, _p]) == _folder_abs
                except (OSError, ValueError):
                    return False

            _scoped_existing = {
                low for low, original in existing_paths.items()
                if low in _catalog_existing
                if _under_folder(original)
            }
            _missing = _scoped_existing - _seen_existing
            _restored_here = 0
            _missing_here = 0
            # Only rows that were previously marked missing need a restore
            # write.  The old code issued one UPDATE for every seen file in a
            # channel (usually thousands of no-ops), holding SQLite's sole
            # writer slot for minutes on the 36 GB production index.
            _restore = _seen_existing & known_missing
            if _restore:
                _restored_cur = sweep_conn.executemany(
                    "UPDATE videos SET availability='available' "
                    "WHERE filepath=? COLLATE NOCASE AND "
                    "availability='missing'",
                    [(existing_paths[p],) for p in _restore
                     if p in existing_paths])
                _restored_here = max(0, _restored_cur.rowcount or 0)
                availability_restored += _restored_here
            if _missing:
                _missing_cur = sweep_conn.executemany(
                    "UPDATE videos SET availability='missing' "
                    "WHERE filepath=? COLLATE NOCASE AND "
                    "COALESCE(availability, 'available')='available'",
                    [(existing_paths[p],) for p in _missing])
                _missing_here = max(0, _missing_cur.rowcount or 0)
                availability_missing += _missing_here
            if _restore or _missing:
                sweep_conn.commit()
            if _restored_here or _missing_here:
                _idx.invalidate_channel_videos(ch_name)
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
        post_walk_fp = _folder_fingerprint(folder)
        if ch_url:
            existing_row = _fp_cache.get(ch_url)
            # tightened to `and` — update_disk_cache_for_channel
            # always writes BOTH fields together, so a row with only one
            # is itself a corruption case we don't want to cement by
            # adding a fingerprint on top.
            if isinstance(existing_row, dict) and (
                    "num_vids" in existing_row
                    and "size_bytes" in existing_row):
                existing_row["sweep_fingerprint"] = post_walk_fp or current_fp

    # ── Aggregated transcript sidecars ──────────────────────────────
    # The per-channel walk above only ingests `{video-base}.jsonl`
    # sidecars, but the transcribe pipeline writes aggregated hidden
    # `.{name} ... Transcript.jsonl` files — and the Search/Graph
    # "unindexed" banner (index_unindexed_count) counts exactly those.
    # Any aggregated jsonl created or touched outside the live
    # transcribe path (caption repair, punct restore, folder reorg,
    # files added while the app was closed) was invisible to this
    # sweep, so the banner stayed stuck at "N transcript files aren't
    # yet in the search index" no matter how many times the user hit
    # Rescan. Walk them here with the same filename filter the banner
    # uses, ignoring the fingerprint skip above (these files may
    # predate the stamped fingerprints).
    agg_ingested = 0
    try:
        for dp, _dns, fns in _os.walk(output_dir):
            for fn in fns:
                if not (fn.startswith(".")
                        and fn.endswith("Transcript.jsonl")):
                    continue
                _wait_while_busy()
                jp = _os.path.normpath(_os.path.join(dp, fn))
                if not _jsonl_needs_ingest(sweep_conn, jp):
                    continue
                rel = _os.path.relpath(dp, output_dir)
                agg_ch = rel.split(_os.sep)[0] if rel != "." else ""
                # `.Foo Transcript.jsonl` -> visible `Foo Transcript.txt`
                root_name = fn[1:-len(".jsonl")]
                txt_fp = _os.path.join(dp, root_name + ".txt")
                try:
                    if _idx.ingest_jsonl(txt_fp, jp, root_name, agg_ch,
                                         _conn_override=sweep_conn):
                        agg_ingested += 1
                        ingested += 1
                except Exception as e:
                    _log.debug("aggregated jsonl ingest failed (%s): %s",
                               jp, e)
    except Exception as e:
        _log.warning("aggregated transcript sweep failed: %s", e)

    # ── Self-heal tx_status against ground truth ─────────────────────
    # tx_status is a denormalized flag; the aggregated-transcript ingest
    # could not reliably flip it (it can only match an individual video by
    # filepath, but aggregated ingest is keyed to the channel-level .txt),
    # so whole channels drifted to a stale 'pending' even though their
    # segments were fully indexed. Reconcile here against the real signal —
    # a video whose video_id has >=1 segment IS transcribed — so the flag
    # can never silently drift out of sync on any future rescan. Cheap:
    # only non-transcribed rows are probed, each via the idx_seg_video_id
    # index (EXISTS), so this is a handful of seconds even on a large DB.
    reconciled = 0
    try:
        _rc = sweep_conn.execute(
            "UPDATE videos SET tx_status='transcribed' "
            "WHERE tx_status != 'transcribed' "
            "AND video_id IS NOT NULL AND video_id != '' "
            "AND EXISTS (SELECT 1 FROM segments s "
            "            WHERE s.video_id = videos.video_id)")
        reconciled = _rc.rowcount or 0
        sweep_conn.commit()
        if reconciled:
            _log.info("tx_status reconcile: flipped %d video(s) to "
                      "'transcribed' (had segments but stale status)",
                      reconciled)
    except Exception as e:
        _log.debug("tx_status reconcile failed: %s", e)

    title_reconciled = 0
    try:
        title_reconciled = _reconcile_tx_status_from_transcript_titles(
            sweep_conn, output_dir, channels, _wait_while_busy)
        if title_reconciled:
            _log.info("tx_status title reconcile: flipped %d video(s) to "
                      "'transcribed' (matched existing Transcript.txt)",
                      title_reconciled)
    except Exception as e:
        _log.debug("tx_status title reconcile failed: %s", e)

    # Persist the updated fingerprints by MERGING into a FRESH load —
    # never by saving our start-of-sweep snapshot. The sweep walks for
    # minutes while sync's update_disk_cache_for_channel and
    # invalidate-rescans write per-channel stats; saving the stale
    # snapshot clobbered every one of those updates (the recurring
    # issue-#134 stale-stats class).
    if skipped_unchanged < total_ch:
        try:
            from .archive_scan import _CACHE_LOCK as _dc_lock
            with _dc_lock:
                _fresh = _load_dc()
                for _url, _row in _fp_cache.items():
                    if not isinstance(_row, dict) \
                            or "sweep_fingerprint" not in _row:
                        continue
                    _fr = _fresh.get(_url)
                    if isinstance(_fr, dict):
                        _fr["sweep_fingerprint"] = _row["sweep_fingerprint"]
                    else:
                        _fresh[_url] = _row
                _save_dc(_fresh)
        except Exception as e:
            _log.debug("swallowed: %s", e)

    # Close the sweep's private connection — best-effort, don't fail the
    # whole sweep if close raises (DB file is fine either way).
    try:
        sweep_conn.close()
    except Exception as e:
        _log.debug("swallowed: %s", e)

    return {"registered": registered, "ingested": ingested,
            "agg_ingested": agg_ingested,
            "id_backfilled": id_backfilled,
            "availability_missing": availability_missing,
            "availability_restored": availability_restored,
            "tx_reconciled": reconciled + title_reconciled,
            "tx_reconciled_by_id": reconciled,
            "tx_reconciled_by_title": title_reconciled,
            "skipped_unchanged": skipped_unchanged,
            "walked": total_ch - skipped_unchanged}


def prune_missing_videos() -> dict[str, int]:
    """Delete stale/phantom video rows from the DB. Cleanup categories:

      1. `missing` — filepath no longer exists on disk. Dead
                      `(1)` duplicates, deleted files, etc.
      2. `zero_byte` — file exists but is 0 bytes. Phantom
                       placeholders from failed downloads can be
                       mis-assigned to another video's id, producing
                       duplicate grid rows with shared thumbnails.
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
        # Category 1 + 2: collect missing / zero-byte files without
        # holding the writer lock. On large Z: archives these stats can
        # take minutes; keeping _db_lock free lets sync/register/transcribe
        # writers continue to make progress while the disk walk runs.
        reader = _idx._reader_open() or conn
        reader_lock = (_idx._reader_lock if reader is not conn
                       else _idx._db_lock)
        with reader_lock:
            rows = reader.execute("SELECT filepath FROM videos").fetchall()
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

        with _idx._db_lock:
            # Category 0: null out all-alphabetic video_ids. These are
            # filename-suffix parse errors (channel files ending in a
            # bracketed non-YouTube token that matched `[A-Za-z0-9_-]{11}` but
            # aren't real YT ids). The row stays — it's a real file
            # on disk — but its video_id field gets cleared so the
            # next metadata recheck will title-resolve it properly
            # instead of treating 13 different files as duplicates of
            # one fake id.
            # REWRITTEN (audit DATA-high): the old isalpha() heuristic
            # nulled EVERY all-alphabetic 11-char id — but ~10% of
            # genuine YouTube ids are purely alphabetic ((52/64)^11),
            # so each Rescan destroyed the ids of tens of thousands of
            # correctly-identified videos, the next metadata pass
            # slowly re-resolved them, and the next Rescan nulled them
            # again — a permanent churn loop degrading search joins,
            # dup detection, and thumbnail association. Worse, the
            # heuristic missed its own motivating case ([a-user-channel]
            # contains hyphens, which isalpha() rejects). Now we null
            # only on POSITIVE evidence of the parse error: the "id"
            # equals the row's channel name (modulo spaces/-/_). No
            # evidence → leave the id alone.
            fake_rows = conn.execute(
                "SELECT id, channel, video_id FROM videos "
                "WHERE video_id IS NOT NULL AND video_id != '' "
                "AND length(video_id) = 11").fetchall()
            for rid, _ch, _v, in fake_rows:
                if not _v:
                    continue
                _vl = _v.lower()
                _chl = (_ch or "").strip().lower()
                if _chl and _vl in (
                        _chl,
                        _chl.replace(" ", ""),
                        _chl.replace(" ", "-"),
                        _chl.replace(" ", "_")):
                    conn.execute(
                        "UPDATE videos SET video_id=NULL, "
                        "video_url=NULL WHERE id=?", (rid,))
                    n_fake_id += 1
                    if _ch:
                        affected_channels.add(_ch)

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
                        # Cascade the segment delete into the FTS
                        # external-content table using the proper
                        # 'delete' command (which requires rowid + text).
                        # The previous DELETE FROM segments_fts WHERE
                        # rowid IN (...) pattern only removed the entry
                        # mapping; the tokens stayed indexed and could
                        # still match search queries via phantom hits.
                        # Worse, SQLite recycles rowids after DELETE, so
                        # a later INSERT could land on a recycled id and
                        # inherit the stale FTS tokens — making a brand
                        # new video's text alias under an old text's
                        # search hits. Mirrors index.py:564 pattern.
                        # Skip the segments DELETE if the FTS detach
                        # failed — otherwise we leave orphan FTS rows
                        # whose rowids will be recycled by a later
                        # INSERT, aliasing a new video's text under
                        # the deleted one's search hits (audit:
                        # index_maintenance H113).
                        _fts_ok = False
                        try:
                            conn.execute(
                                "INSERT INTO segments_fts(segments_fts, rowid, text) "
                                "SELECT 'delete', id, text FROM segments "
                                "WHERE video_id=?",
                                (vid,))
                            _fts_ok = True
                        except Exception as e:
                            _log.warning("FTS detach failed for %s: %s — "
                                         "skipping segments DELETE to avoid "
                                         "orphan FTS rows", vid, e)
                        if _fts_ok:
                            c1 = conn.execute(
                                "DELETE FROM segments WHERE video_id=?",
                                (vid,))
                            segs_removed += c1.rowcount or 0
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
                _log.warning("Browse cache invalidation failed after prune "
                             "for %r: %s", _ch, e)
    except Exception as e:
        _log.warning("prune_missing_videos failed: %s", e)
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
            #
            # Stamp the on-disk mtime of each jsonl_path. The old
            # mtime=0 placeholder caused the next sweep to treat every
            # jsonl as needing re-ingest (every "current mtime" is
            # greater than 0), doing huge redundant work after every
            # FTS rebuild.
            conn.execute("DELETE FROM indexed_files")
            jsonl_paths = [r[0] for r in conn.execute(
                "SELECT DISTINCT jsonl_path FROM segments "
                "WHERE jsonl_path IS NOT NULL").fetchall()]
            for _jp in jsonl_paths:
                if not _jp:
                    continue
                try:
                    _mt = os.path.getmtime(_jp)
                except OSError:
                    _mt = 0.0
                try:
                    n = conn.execute(
                        "SELECT COUNT(*) FROM segments WHERE jsonl_path=?",
                        (_jp,)).fetchone()[0]
                except sqlite3.Error:
                    n = 0
                try:
                    conn.execute(
                        "INSERT OR REPLACE INTO indexed_files"
                        "(path, mtime, segment_count) VALUES(?, ?, ?)",
                        (_jp, float(_mt), int(n)))
                except sqlite3.Error as e:
                    _log.debug("swallowed: %s", e)
            conn.commit()
        return {"ok": True, "rows_indexed": int(rows)}
    except sqlite3.Error as e:
        return {"ok": False, "error": str(e)}
