"""
Transcription / archive index — SQLite FTS5 backend.

Uses the SAME database path + schema as the legacy YTArchiver
(%APPDATA%\\YTArchiver\\transcription_index.db, defined at YTArchiver.py:23444)
so existing search / browse state is picked up with zero migration.

Operations:
  - Register a new video immediately after download (so Browse tab shows it
    without requiring manual Update Index — fixes tkinter bug #4/#5).
  - Read transcript segments for a video (Watch view karaoke rendering).
  - FTS5 search with optional channel filter (Browse > Search).
  - Bookmark CRUD (Browse > Bookmarks).
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .ytarchiver_config import TRANSCRIPTION_DB


_db_lock = threading.RLock()
_conn: Optional[sqlite3.Connection] = None


def _open_independent() -> Optional[sqlite3.Connection]:
    """Open a FRESH SQLite connection (separate from the shared `_conn`).

    Long-running background work (the startup sweep especially) used to
    funnel every per-file write through `_conn` + the Python-level
    `_db_lock`, which serialized ALL DB activity across the app. While
    the sweep was running, sync's DLTRACK `register_video` calls and
    transcribe's FTS-ingest calls had to wait — observed as a video
    stuck at "Downloading 100%" for many minutes during boot.

    With a separate connection, the sweep no longer competes for
    `_db_lock`. SQLite's WAL mode (already enabled at the file level
    by `_open()`'s init) handles cross-connection serialization at the
    DB layer — multiple readers run concurrently, and brief writes
    from sync interleave with the sweep's writes via SQLite's much
    finer-grained per-write lock.

    Caller is responsible for closing the connection when done.
    """
    try:
        TRANSCRIPTION_DB.parent.mkdir(parents=True, exist_ok=True)
        c = sqlite3.connect(str(TRANSCRIPTION_DB),
                            check_same_thread=False, timeout=30.0)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        return c
    except Exception:
        return None


# ── DB open / schema ────────────────────────────────────────────────────

def _open() -> Optional[sqlite3.Connection]:
    """Open or return the cached connection. Returns None if DB can't be opened."""
    global _conn
    with _db_lock:
        if _conn is not None:
            return _conn
        try:
            TRANSCRIPTION_DB.parent.mkdir(parents=True, exist_ok=True)
            _conn = sqlite3.connect(str(TRANSCRIPTION_DB), check_same_thread=False, timeout=10.0)
            _conn.execute("PRAGMA journal_mode=WAL")
            _conn.execute("PRAGMA synchronous=NORMAL")
            # Schema matches YTArchiver.py:23448 verbatim
            _conn.execute("""CREATE TABLE IF NOT EXISTS segments (
                id INTEGER PRIMARY KEY,
                video_id TEXT NOT NULL,
                title TEXT NOT NULL,
                channel TEXT NOT NULL,
                year INTEGER,
                month INTEGER,
                start_time REAL,
                end_time REAL,
                text TEXT NOT NULL,
                jsonl_path TEXT
            )""")
            _conn.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS segments_fts USING fts5(
                text,
                content=segments,
                content_rowid=id
            )""")
            _conn.execute("""CREATE TABLE IF NOT EXISTS indexed_files (
                path TEXT PRIMARY KEY,
                mtime REAL,
                segment_count INTEGER
            )""")
            _conn.execute("""CREATE TABLE IF NOT EXISTS bookmarks (
                id INTEGER PRIMARY KEY,
                segment_id INTEGER,
                video_id TEXT,
                title TEXT,
                channel TEXT,
                start_time REAL,
                text TEXT,
                note TEXT DEFAULT '',
                created REAL DEFAULT (strftime('%s','now')),
                FOREIGN KEY (segment_id) REFERENCES segments(id) ON DELETE SET NULL
            )""")
            _conn.execute("""CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                channel TEXT NOT NULL,
                year INTEGER,
                month INTEGER,
                filepath TEXT UNIQUE COLLATE NOCASE,
                video_id TEXT,
                video_url TEXT,
                duration_s REAL,
                size_bytes INTEGER,
                tx_status TEXT DEFAULT 'pending',
                added_ts REAL
            )""")
            # Migrations — idempotent ALTER statements for columns
            # added after initial schema. OLD-YTArchiver compatible:
            # search_failed_ts — set when title-search fails
            # id_resolve_failed_ts — set when playlist-walk
            # fallback can't match this
            # file to any channel video
            # metadata_fetch_failed_ts — set when yt-dlp --dump-json
            # returns nothing (deleted /
            # private / region-locked)
            # These columns let us skip previously-failed videos on
            # subsequent metadata rechecks instead of retrying every
            # run — "OLD would skip videos that didn't have the
            # information we needed or that failed every single fetch
            # we tried. It was working great."
            for stmt in (
                "ALTER TABLE segments ADD COLUMN words TEXT DEFAULT ''",
                "ALTER TABLE videos ADD COLUMN search_failed_ts REAL",
                "ALTER TABLE videos ADD COLUMN id_resolve_failed_ts REAL",
                "ALTER TABLE videos ADD COLUMN metadata_fetch_failed_ts REAL",
                # Duplicate-download bookkeeping. When a channel-video
                # gets downloaded twice (YouTuber renamed the video,
                # YTArchiver re-downloaded under the new title),
                # both files sit on disk and both get indexed as
                # separate rows. Rather than silently delete (can't
                # touch Z:\) we mark the smaller / older copy as a
                # duplicate of the primary row's filepath, and the
                # Browse grid query filters these out so the grid
                # shows exactly what YouTube shows.
                "ALTER TABLE videos ADD COLUMN is_duplicate_of TEXT",
                # Upload timestamp (unix epoch). Populated from the
                # video file's mtime, which YTArchiver sets to the
                # YouTube upload date via `--mtime` during download
                # (confirmed by memory: "file dates = upload
                # dates"). Used by the Graph tab's Week bucket —
                # segments only carry year+month so bucketing weeks
                # requires joining videos for full-date resolution.
                "ALTER TABLE videos ADD COLUMN upload_ts REAL",
                # Last time a video_id backfill pass attempted this
                # row and couldn't resolve. Lets the UI tell the user
                # "K of N missing were tried unsuccessfully; Y not yet
                # attempted — run Fix IDs" instead of pretending Fix
                # IDs might help every time. Also used by future
                # passes to deprioritize already-exhausted rows.
                "ALTER TABLE videos ADD COLUMN id_backfill_tried_ts REAL",
            ):
                try:
                    _conn.execute(stmt)
                except sqlite3.OperationalError:
                    pass
            # Indexes
            for stmt in (
                "CREATE INDEX IF NOT EXISTS idx_seg_channel ON segments(channel)",
                "CREATE INDEX IF NOT EXISTS idx_seg_ch_yr ON segments(channel, year)",
                "CREATE INDEX IF NOT EXISTS idx_seg_ch_yr_mo ON segments(channel, year, month)",
                "CREATE INDEX IF NOT EXISTS idx_seg_title ON segments(title)",
                "CREATE INDEX IF NOT EXISTS idx_seg_jsonl ON segments(jsonl_path)",
                "CREATE INDEX IF NOT EXISTS idx_seg_video_id ON segments(video_id)",
                "CREATE INDEX IF NOT EXISTS idx_vid_channel ON videos(channel)",
                "CREATE INDEX IF NOT EXISTS idx_vid_ch_yr ON videos(channel, year)",
                "CREATE INDEX IF NOT EXISTS idx_vid_ch_yr_mo ON videos(channel, year, month)",
                "CREATE INDEX IF NOT EXISTS idx_vid_video_id ON videos(video_id)",
                # audit L-16: compound index so the cross-channel
                # duplicate-detection query in prune_missing_videos
                # ("WHERE video_id=? AND filepath != ?") uses an
                # index instead of a full scan. Noticeable difference
                # once videos crosses 100k rows.
                "CREATE INDEX IF NOT EXISTS idx_vid_video_id_channel ON videos(video_id, channel)",
            ):
                try:
                    _conn.execute(stmt)
                except sqlite3.OperationalError:
                    pass
            _conn.commit()
            return _conn
        except sqlite3.Error as e:
            print(f"[index] Could not open DB: {e}")
            return None


# ── Video registration (fixes tkinter bug #4/#5) ────────────────────────

_ID_RE_IN_NAME = re.compile(r"\[([A-Za-z0-9_-]{11})\]")


def _parse_year_month_from_path(filepath: str) -> Tuple[Optional[int], Optional[int]]:
    """Best-effort year/month from a path like .../<channel>/<year>/<Month>/<file>.

    audit E-37: walk parts TAIL-FIRST so the innermost year/month wins.
    Deep archive paths like `Z:\\Archive\\2024\\Channels\\SomeCh\\2020\\
    March\\file.mp4` used to set year=2024 (from archive root), then
    overwrite to 2020 (correct) — but a base path containing a 4-digit
    year-like number mid-path could silently clobber the real channel
    year. Tail-first is unambiguous: the nearest year ancestor matters.
    """
    parts = list(Path(filepath).parts)
    months = ["january","february","march","april","may","june",
              "july","august","september","october","november","december"]
    year: Optional[int] = None
    month: Optional[int] = None
    # Walk from the file back to the root, grabbing month then year on
    # the first hits. "NN Month" patterns (e.g. "01 January") also match.
    for p in reversed(parts):
        low = p.lower().strip()
        # Month hit — either "january" style OR "01 January" style.
        if month is None:
            first_tok = low.split(" ", 1)[0] if " " in low else low
            if first_tok in months:
                month = months.index(first_tok) + 1
                continue
            if (" " in low) and first_tok.isdigit() and low.split(" ", 1)[1] in months:
                # "01 January" — use the text name to be robust.
                month = months.index(low.split(" ", 1)[1]) + 1
                continue
        # Year hit — pure 4-digit in the valid range.
        if p.isdigit() and 1900 < int(p) < 2100:
            year = int(p)
            break
    return year, month


def register_video(filepath: str, channel: str, title: Optional[str] = None,
                   tx_status: str = "pending",
                   video_id: Optional[str] = None,
                   duration_secs: Optional[float] = None,
                   _conn_override: Optional[sqlite3.Connection] = None) -> bool:
    """Add a newly downloaded video to the videos table.

    Called by sync.py each time a .mp4 lands. Browse tab + Index tab both
    read from this table, so the UI updates without a manual re-index.

    If `video_id` is provided (e.g. from yt-dlp's DLTRACK line), it takes
    priority over trying to parse it out of the filename — necessary for
    drop-in-compatible filenames that don't embed `[videoID]`.

    `_conn_override`: caller may supply its OWN sqlite3 connection (from
    `_open_independent()`) to bypass the shared `_db_lock` — used by
    `sweep_new_videos` so the long-running boot sweep doesn't block sync's
    DLTRACK register calls. WAL mode handles cross-connection
    serialization at the SQLite layer.
    """
    use_override = _conn_override is not None
    conn = _conn_override if use_override else _open()
    if conn is None:
        return False
    fp = os.path.normpath(filepath)
    if not title:
        stem = Path(fp).stem
        # Strip " [ID]" suffix if present
        title = re.sub(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$", "", stem).strip() or stem
    # Explicit video_id wins; else try the filename; else look in .info.json
    vid_id = (video_id or "").strip() or None
    if not vid_id:
        m = _ID_RE_IN_NAME.search(os.path.basename(fp))
        if m:
            _cand = m.group(1)
            # Reject all-alphabetic matches — a user's channel's archive has
            # files with suffix `[a-user-channel]` (11 letters, no digit)
            # that match the `[A-Za-z0-9_-]{11}` pattern but aren't
            # real YouTube ids. Real ids are random picks so they
            # essentially always include at least one digit/_/-.
            if not _cand.isalpha():
                vid_id = _cand
    if not vid_id:
        # Drop-in mode: filename no longer carries [id]. Read the
        # .info.json sidecar yt-dlp writes alongside the video.
        try:
            import json as _json
            info_json = Path(fp).with_suffix("").with_suffix(".info.json")
            if not info_json.is_file():
                info_json = Path(fp).parent / (Path(fp).stem + ".info.json")
            if info_json.is_file():
                with info_json.open("r", encoding="utf-8") as f:
                    data = _json.load(f)
                raw = (data.get("id") or "").strip()
                if re.fullmatch(r"[A-Za-z0-9_-]{11}", raw):
                    vid_id = raw
        except Exception:
            pass
    vid_url = f"https://www.youtube.com/watch?v={vid_id}" if vid_id else None
    year, month = _parse_year_month_from_path(fp)
    try:
        size = os.path.getsize(fp) if os.path.isfile(fp) else 0
    except OSError:
        size = 0
    # Capture the file mtime as upload_ts. yt-dlp's `--mtime` sets the
    # file's mtime to the YouTube upload date, so this column carries
    # the true upload date (needed for week-bucket graphing).
    try:
        upload_ts = os.path.getmtime(fp) if os.path.isfile(fp) else None
    except OSError:
        upload_ts = None
    try:
        # When the caller provided their own connection, skip _db_lock
        # entirely — SQLite's WAL handles cross-connection serialization,
        # and acquiring the Python lock would re-introduce the bottleneck
        # this whole feature exists to bypass.
        from contextlib import nullcontext as _nullctx
        _ctx = _nullctx() if use_override else _db_lock
        with _ctx:
            # audit C-10: preserve `added_ts` on re-register.
            # INSERT OR REPLACE silently wiped it every time sweep
            # re-registered an existing video, making "new in last 7
            # days" (Dashboard) and Recent-sort by added_ts useless —
            # every video touched by ANY sweep/invalidation/
            # mark_transcribed counted as "new". UPSERT with an
            # explicit `added_ts=excluded.added_ts` would still reset;
            # instead we write added_ts=? only if the row is new, via
            # COALESCE against the existing value.
            # audit C-8: populate duration_s so "Sort by duration" and
            # per-channel runtime totals actually work. If caller
            # passed duration_secs, use it; otherwise leave NULL /
            # preserve existing value on update (COALESCE pattern).
            _dur = None
            try:
                if duration_secs is not None:
                    _d = float(duration_secs)
                    if _d > 0:
                        _dur = _d
            except (TypeError, ValueError):
                _dur = None
            conn.execute(
                """INSERT INTO videos
                   (title, channel, year, month, filepath, video_id, video_url,
                    size_bytes, duration_s, tx_status, added_ts, upload_ts)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           COALESCE(
                             (SELECT added_ts FROM videos
                              WHERE filepath=? COLLATE NOCASE),
                             ?),
                           ?)
                   ON CONFLICT(filepath) DO UPDATE SET
                     title=excluded.title,
                     channel=excluded.channel,
                     year=excluded.year,
                     month=excluded.month,
                     video_id=excluded.video_id,
                     video_url=excluded.video_url,
                     size_bytes=excluded.size_bytes,
                     duration_s=COALESCE(excluded.duration_s, videos.duration_s),
                     tx_status=excluded.tx_status,
                     upload_ts=excluded.upload_ts
                     /* added_ts deliberately omitted from UPDATE — preserves
                        the original registration timestamp.
                        duration_s uses COALESCE so a re-register with
                        no duration info doesn't wipe a previously-set
                        value. */
                """,
                (title, channel, year, month, fp, vid_id, vid_url,
                 size, _dur, tx_status,
                 fp, time.time(), upload_ts),
            )
            conn.commit()
        # Drop the browse-list cache for this channel so the next grid
        # click picks up the newly-registered video.
        try: invalidate_channel_videos(channel)
        except Exception: pass
        # bug M-14: if a transcript JSONL sidecar is already on disk at
        # register-time (e.g. yt-dlp dropped a .vtt → .jsonl before
        # Whisper even queues), ingest it right now so the Watch view
        # doesn't open with an empty transcript. Without this, the user
        # sees "loading..." until the next full sweep (boot-time or
        # Rescan) — could be hours.
        try:
            _base = os.path.splitext(fp)[0]
            _jp = _base + ".jsonl"
            if os.path.isfile(_jp):
                _display_title = title or os.path.basename(_base)
                ingest_jsonl(fp, _jp, _display_title, channel)
        except Exception:
            pass
        return True
    except sqlite3.Error as e:
        print(f"[index] register_video failed: {e}")
        return False


def mark_video_transcribed(filepath: str) -> bool:
    conn = _open()
    if conn is None:
        return False
    fp = os.path.normpath(filepath)
    try:
        with _db_lock:
            conn.execute("UPDATE videos SET tx_status='transcribed' WHERE filepath=? COLLATE NOCASE", (fp,))
            # Look up the channel so we can invalidate just its cache.
            row = conn.execute(
                "SELECT channel FROM videos WHERE filepath=? COLLATE NOCASE",
                (fp,)).fetchone()
            conn.commit()
        if row and row[0]:
            try: invalidate_channel_videos(row[0])
            except Exception: pass
        return True
    except sqlite3.Error:
        return False


# ── Transcript ingest (from .jsonl sidecar) ─────────────────────────────

def ingest_jsonl(video_filepath: str, jsonl_path: str,
                 title: str, channel: str,
                 _conn_override: Optional[sqlite3.Connection] = None) -> int:
    """Load a .jsonl transcript into segments + FTS. Returns segment count.

    `_conn_override`: see register_video — caller may supply their own
    connection (from `_open_independent()`) so the call doesn't compete
    for `_db_lock`. Used by `sweep_new_videos`.
    """
    use_override = _conn_override is not None
    conn = _conn_override if use_override else _open()
    if conn is None:
        return 0
    fp = os.path.normpath(video_filepath)
    jp = os.path.normpath(jsonl_path)
    if not os.path.isfile(jp):
        return 0

    vid_id = None
    m = _ID_RE_IN_NAME.search(os.path.basename(fp))
    if m:
        vid_id = m.group(1)
    year, month = _parse_year_month_from_path(fp)

    import json
    segments = []
    try:
        with open(jp, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    obj = json.loads(ln)
                except json.JSONDecodeError:
                    continue
                segments.append(obj)
    except OSError:
        return 0

    if not segments:
        return 0

    try:
        # When the caller provided their own connection, skip _db_lock
        # (see register_video for the reasoning).
        from contextlib import nullcontext as _nullctx
        _ctx = _nullctx() if use_override else _db_lock
        with _ctx:
            # audit C-9: FTS5 external-content tables don't auto-sync
            # when rows are deleted from the content table. Without
            # the explicit FTS delete-from-content idiom, re-ingesting
            # a .jsonl leaves orphan FTS rowids pointing at deleted
            # segment IDs — searches return phantom hits that JOIN
            # against an empty segments row. Do the FTS-side delete
            # FIRST so rowids get cleaned out of the FTS index, then
            # DELETE from segments, then re-INSERT.
            conn.execute(
                "INSERT INTO segments_fts(segments_fts, rowid, text) "
                "SELECT 'delete', id, text FROM segments "
                "WHERE jsonl_path=?", (jp,))
            # Clear any existing segments for this jsonl (re-ingest)
            conn.execute("DELETE FROM segments WHERE jsonl_path=?", (jp,))
            rows = []
            for seg in segments:
                # Accept both key shapes so the FTS DB can ingest OLD's
                # long-form JSONLs (they match now) AS WELL AS any stale
                # short-form JSONLs left from earlier builds.
                s_val = seg.get("start") if "start" in seg else seg.get("s", 0)
                e_val = seg.get("end") if "end" in seg else seg.get("e", 0)
                t_val = seg.get("text") if "text" in seg else seg.get("t", "")
                w_val = seg.get("words") if "words" in seg else seg.get("w", [])
                # audit L-17 / L-19: skip segments with no text content
                # (Whisper sometimes emits silence-only segments with
                # empty "t"). Inserting them bloats the FTS index with
                # empty rows the user can never land on. Also skip if
                # w_val is a malformed non-list — json.dumps would still
                # succeed but the saved form would break word-cloud.
                if not (t_val or "").strip():
                    continue
                if not isinstance(w_val, list):
                    w_val = []
                # Also prefer per-entry video_id/title if the JSONL carries
                # them (OLD-compat long-form). Falls back to path-derived.
                seg_vid = (seg.get("video_id") or vid_id or "").strip()
                seg_title = (seg.get("title") or title or "").strip() or title
                rows.append((
                    seg_vid,
                    seg_title,
                    channel,
                    year,
                    month,
                    float(s_val or 0),
                    float(e_val or 0),
                    t_val,
                    jp,
                    json.dumps(w_val, ensure_ascii=False),
                ))
            cur = conn.executemany(
                """INSERT INTO segments
                   (video_id, title, channel, year, month, start_time, end_time,
                    text, jsonl_path, words)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            # Populate FTS
            conn.execute(
                "INSERT INTO segments_fts (rowid, text) "
                "SELECT id, text FROM segments WHERE jsonl_path=?", (jp,),
            )
            # Track indexed file mtime
            conn.execute(
                "INSERT OR REPLACE INTO indexed_files(path, mtime, segment_count) "
                "VALUES (?, ?, ?)",
                (jp, os.path.getmtime(jp), len(rows)),
            )
            # audit D-46: flip tx_status='transcribed' on successful
            # ingest so channel_transcription_stats reflects reality
            # right away. Previously only mark_video_transcribed did
            # this, and if ingest_jsonl was called without a follow-up
            # mark_video_transcribed the Edit-channel footer and stats
            # queries reported stale 'pending' counts for videos that
            # had segments in the index.
            conn.execute(
                "UPDATE videos SET tx_status='transcribed' "
                "WHERE filepath=? COLLATE NOCASE", (fp,))
            conn.commit()
        # Bug [53]: return the actually-inserted row count, not the
        # raw segment count from the JSONL. Empty-text segments and
        # malformed entries are filtered out above (lines 442-451) but
        # still inflated the reported "ingested" total before this fix.
        return len(rows)
    except sqlite3.Error as e:
        print(f"[index] ingest_jsonl failed: {e}")
        return 0


# ── Reads ───────────────────────────────────────────────────────────────

def list_recent_videos(limit: int = 200, channel: Optional[str] = None
                       ) -> List[Dict[str, Any]]:
    """Return the N newest videos (by added_ts), optionally filtered by channel."""
    conn = _open()
    if conn is None:
        return []
    q = ("SELECT title, channel, filepath, video_id, size_bytes, year, month, "
         "tx_status, added_ts FROM videos ")
    args: List[Any] = []
    if channel:
        q += "WHERE channel=? "
        args.append(channel)
    q += "ORDER BY added_ts DESC LIMIT ?"
    args.append(limit)
    with _db_lock:
        cur = conn.execute(q, args)
        out = []
        for row in cur.fetchall():
            title, ch, fp, vid, sz, yr, mo, st, ts = row
            out.append({
                "title": title, "channel": ch, "filepath": fp, "video_id": vid,
                "size_bytes": sz or 0, "year": yr, "month": mo,
                "tx_status": st, "added_ts": ts,
            })
    return out


# ── Browse-tab per-channel video-list cache ───────────────────────────
# Ports YTArchiver.py's _grid_cache (27572 _grid_preload_all). Clicking a
# channel in the Browse grid used to be slow because every visit rebuilt
# the video list from scratch — DB query + metadata JSONL parse +
# thumbnail lookup per video. The cache pre-computes this at startup so
# the second channel click onward is instant.
#
# Keyed by (channel_name, sort, limit, include_thumbs). Invalidated by
# `invalidate_channel_videos(channel)` whenever a video is added / deleted
# / re-transcribed for that channel.
_browse_videos_cache: Dict[Tuple[str, str, int, bool], List[Dict[str, Any]]] = {}
_browse_cache_lock = threading.Lock()


def invalidate_channel_videos(channel: Optional[str] = None) -> None:
    """Drop cached video lists. `channel=None` clears everything."""
    with _browse_cache_lock:
        if channel is None:
            _browse_videos_cache.clear()
        else:
            for key in list(_browse_videos_cache.keys()):
                if key[0] == channel:
                    del _browse_videos_cache[key]


def preload_channel_videos(channel: str,
                           sort: str = "newest",
                           limit: int = 500) -> int:
    """Warm `list_videos_for_channel` into the cache. Returns row count."""
    rows = list_videos_for_channel(channel, sort=sort, limit=limit,
                                    include_thumbs=True)
    with _browse_cache_lock:
        _browse_videos_cache[(channel, sort, limit, True)] = rows
    return len(rows)


def preload_all_channels(channel_names: List[str],
                         progress_cb: Optional[Any] = None,
                         cancel_ev: Optional[Any] = None,
                         sort: str = "newest",
                         limit: int = 500) -> Dict[str, int]:
    """Warm the per-channel video-list cache for every subscribed channel.
    Mirrors OLD YTArchiver.py:27572 `_grid_preload_all` but without the
    200ms per-channel sleep — our DB reads are cheap enough that the
    sleep was just padding wall-clock time. Still iterates one channel
    at a time so progress_cb fires granularly (user sees real progress
    behind the "Loading…" line).

    `progress_cb(idx, total, channel_name)` is called before each channel.
    Returns {channel_name: row_count}.
    """
    out: Dict[str, int] = {}
    total = len(channel_names)
    for i, ch in enumerate(channel_names):
        if cancel_ev is not None and cancel_ev.is_set():
            break
        if progress_cb is not None:
            try: progress_cb(i + 1, total, ch)
            except Exception: pass
        try:
            out[ch] = preload_channel_videos(ch, sort=sort, limit=limit)
        except Exception:
            out[ch] = 0
    return out


def list_videos_for_channel(channel: str, sort: str = "newest",
                            limit: int = 50000, include_thumbs: bool = True
                            ) -> List[Dict[str, Any]]:
    """Videos in a channel, sorted by requested key.

    Returns both `added_ts` (when we registered the video in the DB) and
    `upload_ts` (the file's mtime, which equals the YouTube upload date
    since sync runs yt-dlp with `--mtime`). Sort keys "newest"/"oldest"
    use `upload_ts` first so the grid actually matches upload order; the
    DB-insertion time was what made every video look like "15d ago".

    Results are cached per (channel, sort, limit, include_thumbs) tuple
    so the preloader at startup fills the cache, and runtime clicks on
    channels are instant. See `preload_channel_videos` +
    `invalidate_channel_videos`.
    """
    cache_key = (channel, sort, limit, bool(include_thumbs))
    with _browse_cache_lock:
        hit = _browse_videos_cache.get(cache_key)
        if hit is not None:
            # Return a shallow copy so callers can mutate without poisoning
            # the cache for the next reader.
            return list(hit)
        # Fallback: look for any cached entry with the same (channel,
        # sort, include_thumbs) but a larger limit — a 100k-limit
        # preload satisfies any smaller runtime request. Without
        # this, "preload every video" setting didn't deliver
        # the promised instant-click behavior because preload keyed
        # on 100_000 while the frontend requested 50_000.
        for (c_ch, c_sort, c_lim, c_thumbs), rows in _browse_videos_cache.items():
            if (c_ch == channel and c_sort == sort
                    and c_thumbs == bool(include_thumbs)
                    and c_lim >= limit):
                return list(rows[:limit]) if len(rows) > limit else list(rows)
    conn = _open()
    if conn is None:
        return []
    # Use year/month as the primary sort (stable proxy for upload_ts
    # / file mtime), with added_ts as the per-month tiebreaker. This
    # is important for large channels (a high-video-count channel: 27k videos) where
    # a `added_ts DESC LIMIT 500` would return only whatever 500 rows
    # happened to be indexed most recently — that's often a random
    # year distribution if a bootstrap / reorg touched added_ts
    # out-of-upload-order. Sorting by year first guarantees the grid
    # covers the newest years contiguously.
    order = {
        "newest": "COALESCE(year, 0) DESC, COALESCE(month, 0) DESC, COALESCE(added_ts, 0) DESC",
        "oldest": "COALESCE(year, 99999) ASC, COALESCE(month, 99) ASC, COALESCE(added_ts, 0) ASC",
        "largest": "COALESCE(size_bytes, 0) DESC",
        "title": "title COLLATE NOCASE ASC",
    }.get(sort, "COALESCE(year, 0) DESC, COALESCE(month, 0) DESC, COALESCE(added_ts, 0) DESC")
    with _db_lock:
        # Bug [49]: COLLATE NOCASE so a channel named "MyChannel" matches
        # rows stored as "mychannel" (case drift can come from manual DB
        # edits or older data). Without it, duplicates from the lower-
        # cased rows escape the is_duplicate_of filter and show twice.
        cur = conn.execute(
            f"SELECT title, channel, filepath, video_id, size_bytes, year, month, "
            f"tx_status, added_ts FROM videos "
            f"WHERE channel=? COLLATE NOCASE AND is_duplicate_of IS NULL "
            f"ORDER BY {order} LIMIT ?",
            (channel, limit),
        )
        out = [{
            "title": r[0], "channel": r[1], "filepath": r[2], "video_id": r[3],
            "size_bytes": r[4] or 0, "year": r[5], "month": r[6],
            "tx_status": r[7], "added_ts": r[8],
        } for r in cur.fetchall()]
    # Enrich: upload_ts + view_count (from aggregated metadata) + thumbnails.
    # View count enables "Most Viewed" sort in the Browse grid without
    # making yt-dlp calls here — we rely on the data Cache built up during
    # sync-time metadata fetches.
    metadata_cache: Dict[str, Dict[str, Any]] = {}
    # Secondary title-keyed index, built alongside the video_id one so
    # DB rows with a NULL video_id (common for videos indexed before
    # the video_id column was populated consistently) can still resolve
    # their metadata by title match. Reported: an entire channel's
    # videos had video_id=NULL in the index DB, so every view_count
    # lookup failed — Most Viewed sort was a no-op for all 400+
    # videos because the sort comparator saw only zeros.
    metadata_cache_by_title: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def _norm_title(s: str) -> str:
        """Loose normalization for title matching — lowercase + collapse
        whitespace. Enough to bridge minor rendering differences without
        opening up to false positives.
        """
        return " ".join((s or "").lower().split())

    def _fetch_meta(folder_key):
        """Lazy-load the aggregated metadata JSONL for a given folder.
        Cache by folder so we don't re-parse the same file per row."""
        if folder_key in metadata_cache:
            return metadata_cache[folder_key]
        entries: Dict[str, Any] = {}
        by_title: Dict[str, Any] = {}
        try:
            # Walk up the folder tree looking for .{channel} ... Metadata.jsonl
            cur = folder_key
            for _ in range(4):
                if not cur or not os.path.isdir(cur):
                    break
                for fn in os.listdir(cur):
                    if fn.startswith(".") and fn.endswith("Metadata.jsonl"):
                        try:
                            with open(os.path.join(cur, fn),
                                      "r", encoding="utf-8") as f:
                                for ln in f:
                                    ln = ln.strip()
                                    if not ln: continue
                                    try:
                                        obj = json.loads(ln)
                                        vid = obj.get("video_id", "")
                                        if vid:
                                            entries[vid] = obj
                                        # Title index as a secondary
                                        # lookup path. Same obj can be
                                        # reached by either key.
                                        t = _norm_title(obj.get("title", ""))
                                        if t:
                                            by_title[t] = obj
                                    except json.JSONDecodeError:
                                        pass
                        except OSError:
                            pass
                parent = os.path.dirname(cur)
                if parent == cur: break
                cur = parent
        except Exception:
            pass
        metadata_cache[folder_key] = entries
        metadata_cache_by_title[folder_key] = by_title
        return entries

    def _fetch_meta_by_title(folder_key: str, title: str):
        """Title-keyed fallback for rows with a missing video_id.
        Returns the JSONL entry or None.
        """
        if folder_key not in metadata_cache_by_title:
            _fetch_meta(folder_key) # populates both caches
        return metadata_cache_by_title.get(folder_key, {}).get(
            _norm_title(title))

    # Helper: parse yt-dlp's YYYYMMDD upload_date string into a Unix
    # epoch. Returns 0 on anything unparseable. The aggregated metadata
    # JSONL stores dates in this format (it's what YouTube's API returns).
    def _yyyymmdd_to_epoch(s: str) -> float:
        if not s:
            return 0.0
        s = str(s).strip()
        if len(s) != 8 or not s.isdigit():
            return 0.0
        try:
            from datetime import datetime as _dt, timezone as _tz
            return _dt(int(s[0:4]), int(s[4:6]), int(s[6:8]),
                      tzinfo=_tz.utc).timestamp()
        except (ValueError, OSError):
            return 0.0

    for row in out:
        fp = row.get("filepath") or ""
        vid_id = (row.get("video_id") or "").strip()

        # Upload timestamp — source priority:
        # 1. Aggregated metadata `upload_date` (authoritative YouTube
        # upload date, unaffected by filesystem operations)
        # 2. File mtime (yt-dlp --mtime sets this at download time)
        # 3. added_ts (DB registration — last resort, shows "1mo ago"
        # for a whole channel if the sweep ran a month ago)
        #
        # The user reported every video showing "1mo ago" because recent
        # reorg / bulk-copy operations stomped all the mtimes to the
        # same date. Metadata upload_date is invariant across those.
        meta = None
        if fp and vid_id:
            meta = _fetch_meta(os.path.dirname(fp)).get(vid_id)
        # Fallback — many DB rows (older channels, bulk-imported from
        # folder scans, pre-ID-column upgrades) have a NULL video_id.
        # Title-match against the JSONL's `title` field so those rows
        # still get their view_count populated for the Most Viewed
        # sort. Loose normalization (lower + whitespace collapse).
        if meta is None and fp:
            _t_row = row.get("title", "")
            if _t_row:
                meta = _fetch_meta_by_title(os.path.dirname(fp), _t_row)
        if meta:
            ep = _yyyymmdd_to_epoch(meta.get("upload_date", ""))
            if ep > 0:
                row["upload_ts"] = ep
        if "upload_ts" not in row:
            try:
                if fp and os.path.isfile(fp):
                    row["upload_ts"] = os.path.getmtime(fp)
                    # Bug [103]: flag the fallback so the UI can render
                    # the date with a "~estimated" hint. Without this,
                    # a video whose mtime got reset (re-org, copy across
                    # volumes) shows the wrong date as if it were real.
                    row["upload_ts_source"] = "mtime_fallback"
            except OSError:
                pass

        # View count: fetch from aggregated metadata when available
        if meta:
            # audit H-14: a corrupted JSONL entry with view_count="N/A"
            # would raise ValueError out of this cast and abort the
            # entire Browse grid render for the channel. Guard with
            # try/except and fall back to 0 so one bad row doesn't
            # hide all the others.
            try:
                row["view_count"] = int(meta.get("view_count") or 0)
            except (TypeError, ValueError):
                row["view_count"] = 0
            try:
                row["like_count"] = int(meta.get("like_count") or 0)
            except (TypeError, ValueError):
                row["like_count"] = 0
            # Surface as display "views" too
            v = row["view_count"]
            if v >= 1_000_000:
                row["views"] = f"{v/1_000_000:.1f}M"
            elif v >= 1_000:
                row["views"] = f"{v/1_000:.1f}K"
            elif v > 0:
                row["views"] = str(v)
        if include_thumbs:
            tp = find_thumbnail(fp, row.get("video_id"))
            if tp:
                row["thumbnail"] = tp
                row["thumbnail_url"] = _file_url(tp)

    # Re-sort: `newest`/`oldest` use upload_ts; `most_viewed` uses view_count.
    if sort in ("newest", "oldest"):
        rev = (sort == "newest")
        out.sort(key=lambda r: r.get("upload_ts") or r.get("added_ts") or 0,
                 reverse=rev)
    elif sort == "most_viewed":
        out.sort(key=lambda r: r.get("view_count") or 0, reverse=True)
    # Store in the browse cache so the NEXT click on this channel is
    # instant (no DB + metadata JSONL + thumbnail-walk cost again).
    with _browse_cache_lock:
        _browse_videos_cache[cache_key] = list(out)
    return out


def _file_url(path: str) -> str:
    """Return a URL the webview can use to load `path`.

    Prefers `http://127.0.0.1:<port>/file/<encoded>` (served by
    `backend.local_fileserver`) because WebView2 on Windows blocks
    cross-origin `file://` requests from a `file://` page — thumbnails
    silently fail without a real HTTP origin.

    Falls back to a percent-encoded `file://` URL when the server isn't
    running (e.g. during unit tests).
    """
    if not path:
        return ""
    try:
        from .local_fileserver import get_port, url_for
        if get_port():
            return url_for(path)
    except Exception:
        pass
    from urllib.parse import quote
    p = os.path.abspath(path).replace("\\", "/")
    if not p.startswith("/"):
        p = "/" + p
    return "file://" + quote(p, safe="/:")


def new_videos_in_last_n_days(days: int = 7) -> Dict[str, Any]:
    """Return {videos: N, channels: M} for videos added in the last N days.

    Uses the FTS DB's videos.added_ts column (Unix epoch). Silent-returns zeros
    on any DB error so the caller can show a bar with dashes gracefully.
    """
    out = {"videos": 0, "channels": 0, "channel_list": []}
    try:
        conn = _open()
        if conn is None:
            return out
        import time as _t
        cutoff = _t.time() - (max(1, int(days)) * 86400.0)
        row = conn.execute(
            "SELECT COUNT(*) FROM videos WHERE added_ts >= ?", (cutoff,),
        ).fetchone()
        out["videos"] = int(row[0] or 0) if row else 0
        rows = conn.execute(
            "SELECT DISTINCT channel FROM videos WHERE added_ts >= ? ORDER BY channel",
            (cutoff,),
        ).fetchall()
        out["channel_list"] = [r[0] for r in rows if r[0]]
        out["channels"] = len(out["channel_list"])
    except Exception:
        pass
    return out


def channel_transcription_stats(channel: str) -> Dict[str, int]:
    """Return {total, transcribed, pending, failed} video counts for a channel.

    Matches on channel name via the videos table (NOCASE). Empty channel ->
    zeros. Safe to call with an uninitialized DB (returns zeros).
    """
    out = {"total": 0, "transcribed": 0, "pending": 0, "failed": 0}
    if not channel:
        return out
    try:
        conn = _open()
        if conn is None:
            return out
        # `mark_video_transcribed` writes tx_status='transcribed' (see
        # register/UPDATE at line 274), so the coverage count must match
        # that string. Earlier this query tested 'done' — a mismatch
        # that made fully-transcribed channels read as "0 / N" in the
        # Edit-channel disk-stats footer.
        # audit M-36: exclude duplicate rows (is_duplicate_of NOT NULL)
        # from the counts. The Browse grid hides duplicates already,
        # so the footer "N/M transcribed" should match the visible
        # row count, not include hidden dups.
        row = conn.execute(
            """SELECT
                 COUNT(*) AS total,
                 SUM(CASE WHEN tx_status IN ('transcribed', 'done')
                          THEN 1 ELSE 0 END) AS done,
                 SUM(CASE WHEN tx_status='pending' OR tx_status IS NULL
                          THEN 1 ELSE 0 END) AS pending,
                 SUM(CASE WHEN tx_status='failed' THEN 1 ELSE 0 END) AS failed
               FROM videos
               WHERE channel = ? COLLATE NOCASE
                 AND is_duplicate_of IS NULL""",
            (channel,),
        ).fetchone()
        if row:
            out["total"] = int(row[0] or 0)
            out["transcribed"] = int(row[1] or 0)
            out["pending"] = int(row[2] or 0)
            out["failed"] = int(row[3] or 0)
    except Exception:
        pass
    return out


def find_thumbnail(video_filepath: str,
                    video_id: Optional[str] = None) -> Optional[str]:
    """Return the path to a .jpg / .webp thumbnail sidecar, if present.

    YTArchiver saves thumbnails to `<channel>/<year>/.Thumbnails/<title> [<id>].jpg`
    (YTArchiver.py:26845). In drop-in mode the video file is named
    `<title>.mp4` without an `[id]` suffix, so matching by stem alone misses
    the thumbnail — we fall back to scanning the `.Thumbnails` folder for any
    file whose name contains `[<videoId>]` when the caller passes an id.

    Lookup order:
      1. `<video>.jpg` / .jpeg / .webp / .png (direct sidecar)
      2. `.Thumbnails/<stem>.jpg` (exact stem match)
      3. `.Thumbnails/*[<videoId>].jpg` (scan by id when provided)
      4. Walk up to parent year folder and try `.Thumbnails` there too.
    """
    if not video_filepath:
        return None
    base = os.path.splitext(video_filepath)[0]
    # 1. Direct sidecar
    for ext in (".jpg", ".jpeg", ".webp", ".png"):
        p = base + ext
        if os.path.isfile(p):
            return os.path.normpath(p)

    stem = os.path.basename(base)
    # Search 3 levels of .Thumbnails dirs: same folder, parent (year),
    # grandparent (channel). Original's layout is <channel>/<year>/<Month>/
    # or <channel>/<year>/, with .Thumbnails sitting next to the videos.
    search_dirs = []
    cur = os.path.dirname(video_filepath)
    for _ in range(3):
        if not cur:
            break
        for name in (".Thumbnails", "Thumbnails"):
            p = os.path.join(cur, name)
            if os.path.isdir(p):
                search_dirs.append(p)
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent

    # 2. Exact stem match in any .Thumbnails
    for tf in search_dirs:
        for ext in (".jpg", ".jpeg", ".webp", ".png"):
            p = os.path.join(tf, stem + ext)
            if os.path.isfile(p):
                return os.path.normpath(p)

    # 3. `[<videoId>]` scan when we have the ID
    if video_id:
        tag = f"[{video_id}]"
        for tf in search_dirs:
            try:
                for fn in os.listdir(tf):
                    low = fn.lower()
                    if tag in fn and low.endswith((".jpg", ".jpeg", ".webp", ".png")):
                        return os.path.normpath(os.path.join(tf, fn))
            except OSError:
                continue

    # 4. Prefix match by stem (last resort — catches `<stem> [<id>].jpg`).
    # audit D-48: drop the bare `startswith(stem)` fallback — it used to
    # match unrelated thumbnails whose name coincidentally began with
    # this video's stem ("Intel reveals" stem matches "Intel reveals
    # X-ray secret.jpg"), returning the first os.listdir hit at random.
    # The "<stem> [<id>].jpg" and exact-file-match cases handle the
    # legitimate layouts; anything else is indeterminate and shouldn't
    # silently return a wrong thumbnail.
    for tf in search_dirs:
        try:
            for fn in os.listdir(tf):
                low = fn.lower()
                _name_no_ext = os.path.splitext(fn)[0]
                if (fn.startswith(stem + " [") or _name_no_ext == stem) \
                        and low.endswith((".jpg", ".jpeg", ".webp", ".png")):
                    return os.path.normpath(os.path.join(tf, fn))
        except OSError:
            continue

    return None


def get_segments(video_id: Optional[str] = None, jsonl_path: Optional[str] = None,
                 title: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return ordered segments for a video."""
    conn = _open()
    if conn is None:
        return []
    where = []
    args: List[Any] = []
    if video_id:
        where.append("video_id=?"); args.append(video_id)
    if jsonl_path:
        where.append("jsonl_path=?"); args.append(os.path.normpath(jsonl_path))
    if title and not where:
        where.append("title=?"); args.append(title)
    if not where:
        return []
    # Use AND across multiple filters: when a caller passes both video_id
    # and jsonl_path (the common case from main.py:browse_get_transcript),
    # we want segments that match BOTH, not segments matching either.
    # OR semantics would mash together segments from any video that shares
    # a jsonl_path (combined transcripts) with a different video_id.
    q = ("SELECT start_time, end_time, text, words FROM segments "
         f"WHERE {' AND '.join(where)} ORDER BY start_time")
    with _db_lock:
        cur = conn.execute(q, args)
        out = []
        import json as _j
        for s, e, t, w in cur.fetchall():
            try:
                words = _j.loads(w) if w else []
            except (json.JSONDecodeError, ValueError):
                words = []
            out.append({"s": s, "e": e, "t": t, "w": words})
    return out


def get_segment_context(segment_id: int, before: int = 30,
                        after: int = 30,
                        query: str = "") -> Dict[str, Any]:
    """Return N segments before + hit + N segments after for a search hit.

    `segment_id` is the rowid of the hit in the segments table. Returns:
      { ok, title, channel, video_id, jsonl_path,
        segments: [{id, s, e, t, is_hit}, ...],
        before_more: bool, after_more: bool }

    Used by the Search viewer pane (YTArchiver.py:29598) to show
    ~60 segments of surrounding transcript, with the hit highlighted.
    """
    conn = _open()
    if conn is None:
        return {"ok": False, "error": "DB unavailable"}
    try:
        with _db_lock:
            # Resolve the hit row to find its jsonl_path + timeline position
            hit = conn.execute(
                "SELECT id, video_id, title, channel, jsonl_path, start_time "
                "FROM segments WHERE id=?", (segment_id,)).fetchone()
            if not hit:
                return {"ok": False, "error": "Segment not found"}
            hit_id, vid_id, title, channel, jsonl_path, hit_start = hit
            # Pull the window around the hit within the same jsonl file
            rows_before = conn.execute(
                "SELECT id, start_time, end_time, text FROM segments "
                "WHERE jsonl_path=? AND start_time < ? "
                "ORDER BY start_time DESC LIMIT ?",
                (jsonl_path, hit_start, before)).fetchall()
            rows_after = conn.execute(
                "SELECT id, start_time, end_time, text FROM segments "
                "WHERE jsonl_path=? AND start_time > ? "
                "ORDER BY start_time ASC LIMIT ?",
                (jsonl_path, hit_start, after)).fetchall()
            hit_row = conn.execute(
                "SELECT id, start_time, end_time, text FROM segments "
                "WHERE id=?", (segment_id,)).fetchone()
            # More-available flags: check if there are rows beyond the window
            more_before = 0
            more_after = 0
            if rows_before:
                before_edge = rows_before[-1][1] # earliest start_time in window
                more_before = conn.execute(
                    "SELECT COUNT(*) FROM segments "
                    "WHERE jsonl_path=? AND start_time < ?",
                    (jsonl_path, before_edge)).fetchone()[0] > 0
            if rows_after:
                after_edge = rows_after[-1][1]
                more_after = conn.execute(
                    "SELECT COUNT(*) FROM segments "
                    "WHERE jsonl_path=? AND start_time > ?",
                    (jsonl_path, after_edge)).fetchone()[0] > 0
            # All other segments matching the query in this same video —
            # mark them so the viewer can highlight every hit, not just the
            # one the user clicked.
            other_hits: set = set()
            if query and query.strip():
                try:
                    cur2 = conn.execute(
                        "SELECT s.id FROM segments_fts "
                        "JOIN segments s ON s.id = segments_fts.rowid "
                        "WHERE s.jsonl_path=? AND segments_fts MATCH ?",
                        (jsonl_path, query))
                    other_hits = {r[0] for r in cur2.fetchall()}
                except sqlite3.Error:
                    other_hits = set()
            # Assemble in chronological order
            segments: List[Dict[str, Any]] = []
            for r in reversed(rows_before):
                rid, s, e, t = r
                segments.append({"id": rid, "s": s, "e": e, "t": t,
                                 "is_hit": rid == hit_id or rid in other_hits})
            if hit_row:
                rid, s, e, t = hit_row
                segments.append({"id": rid, "s": s, "e": e, "t": t,
                                 "is_hit": True})
            for r in rows_after:
                rid, s, e, t = r
                segments.append({"id": rid, "s": s, "e": e, "t": t,
                                 "is_hit": rid == hit_id or rid in other_hits})
        return {
            "ok": True, "title": title, "channel": channel,
            "video_id": vid_id, "jsonl_path": jsonl_path,
            "segments": segments,
            "before_more": bool(more_before),
            "after_more": bool(more_after),
        }
    except sqlite3.Error as e:
        return {"ok": False, "error": str(e)}


def bucket_totals(bucket: str = "month",
                  channel: Optional[str] = None) -> Dict[str, int]:
    """Return {bucket_label: total_segments_in_bucket} so the Graph's
    Normalize toggle can divide each bucket's count against its segment
    volume. Matches YTArchiver.py normalize logic that divides word counts
    by per-bucket total then multiplies by 1000.
    """
    conn = _open()
    if conn is None:
        return {}
    group_col = "year || '-' || printf('%02d', month)" if bucket == "month" else "year"
    sql = (f"SELECT {group_col} AS bucket, COUNT(*) "
           " FROM segments")
    args: List[Any] = []
    if channel:
        sql += " WHERE channel=?"
        args.append(channel)
    sql += " GROUP BY bucket"
    try:
        with _db_lock:
            rows = conn.execute(sql, args).fetchall()
    except sqlite3.Error:
        return {}
    return {str(r[0]): int(r[1] or 0) for r in rows if r[0] is not None}


# Rough English stop-word list — enough to keep a 100-word cloud interesting.
# Includes common contractions because the tokenizer allows apostrophes inside
# words ("it's", "i'm", "don't", etc. would otherwise dominate the cloud).
_STOP_WORDS = frozenset("""
a about above after again against all am an and any are aren as at be because
been before being below between both but by can cannot could did do does doing
don down during each few for from further had has have having he her here hers
herself him himself his how i if in into is it its itself just like me more
most my myself no nor not now of off on once only or other our ours ourselves
out over own same she should so some such than that the their theirs them
themselves then there these they this those through to too under until up very
was we were what when where which while who whom why will with would you your
yours yourself yourselves ll ve re ain aren couldn didn doesn don hadn hasn
haven isn mightn mustn needn shan shouldn wasn weren won wouldn also get got
going really know one two three get thing things something anything nothing
go way say said says see saw look right yeah okay hey uh um thats youre were
actually literally basically thats gonna wanna kinda sorta lot lots make makes
it's i'm don't won't can't didn't wasn't doesn't isn't aren't haven't hasn't
weren't wouldn't shouldn't couldn't you're we're they're we've i've you've
they've he's she's that's there's here's what's who's how's where's when's
let's who've you'll i'll we'll they'll he'll she'll i'd you'd we'd they'd
he'd she'd you'll ain't y'all gotta oh ooh ah ahh well alright ok
""".split())


def top_words(channel: Optional[str] = None, top_n: int = 120,
              min_len: int = 3) -> List[Dict[str, Any]]:
    """Return the top-N most-common words across all segments (optionally
    filtered to a single channel). Skips short tokens + stop words so the
    cloud surfaces actually-distinctive vocabulary.

    Returns a list of {word, count} sorted descending by count. Used by
    the Graph sub-mode's Word Cloud chart type.
    """
    conn = _open()
    if conn is None:
        return []
    sql = "SELECT text FROM segments"
    args: List[Any] = []
    if channel:
        sql += " WHERE channel=?"
        args.append(channel)
    # Cap at a large but finite number so a huge archive doesn't OOM us.
    sql += " LIMIT 500000"
    import re as _re
    word_re = _re.compile(r"[a-zA-Z][a-zA-Z']{%d,}" % (min_len - 1))
    counts: Dict[str, int] = {}
    try:
        with _db_lock:
            cur = conn.execute(sql, args)
            for (txt,) in cur:
                if not txt:
                    continue
                for raw in word_re.findall(txt):
                    w = raw.lower().rstrip("'")
                    if w in _STOP_WORDS:
                        continue
                    counts[w] = counts.get(w, 0) + 1
    except sqlite3.Error:
        return []
    # Top-N
    items = sorted(counts.items(), key=lambda x: -x[1])[:int(top_n)]
    return [{"word": w, "count": c} for w, c in items]


def backfill_upload_ts(limit: int = 0) -> Dict[str, int]:
    """Populate `videos.upload_ts` from file mtime for any row where it's
    currently NULL. Called lazily the first time a Week-bucket graph is
    requested so we don't force a full-archive stat walk at startup.

    yt-dlp sets each video file's mtime to the YouTube upload date via
    `--mtime`, so os.path.getmtime(filepath) is the authoritative upload
    timestamp. Missing files silently skip (leave NULL) — those rows
    won't contribute to week-bucket graphs but won't crash the query.

    Returns {filled: N, skipped: M}. `limit=0` means "all rows".
    """
    conn = _open()
    if conn is None:
        return {"filled": 0, "skipped": 0}
    filled = 0
    skipped = 0
    try:
        with _db_lock:
            sql = "SELECT rowid, filepath FROM videos WHERE upload_ts IS NULL"
            if limit > 0:
                sql += f" LIMIT {int(limit)}"
            rows = conn.execute(sql).fetchall()
        for rowid, fp in rows:
            try:
                if fp and os.path.isfile(fp):
                    mtime = os.path.getmtime(fp)
                    with _db_lock:
                        conn.execute(
                            "UPDATE videos SET upload_ts=? WHERE rowid=?",
                            (mtime, rowid))
                    filled += 1
                else:
                    skipped += 1
            except OSError:
                skipped += 1
        with _db_lock:
            conn.commit()
    except sqlite3.Error:
        pass
    return {"filled": filled, "skipped": skipped}


def graph_word_frequency(word: str, channel: Optional[str] = None,
                         bucket: str = "month") -> Dict[str, Any]:
    """Count occurrences of `word` per time bucket.

    bucket ∈ {"year", "month", "week"}. Returns {labels, values}.

    - "year" → group by segments.year
    - "month" → group by "YYYY-MM" from segments.year + segments.month
    - "week" → group by ISO-week key "YYYY-Www" from videos.upload_ts
                (segments only store year+month, so weekly granularity
                requires joining videos + using the file mtime which
                yt-dlp set to the upload date via --mtime). Videos whose
                upload_ts is NULL are skipped from the week plot; the
                caller can trigger `backfill_upload_ts()` to populate.
    """
    conn = _open()
    if conn is None or not word.strip():
        return {"labels": [], "values": []}
    word = word.strip()
    if bucket == "week":
        # audit D-45: LEFT JOIN so segments with NULL video_id (common
        # for legacy rows and drop-in-mode archives without .info.json)
        # still COUNT against the match totals. Without this, the
        # inner join silently excluded them and the week graph showed
        # undercount. We still filter out rows that resolve to NULL
        # upload_ts (no bucket to assign) in the WHERE clause.
        # audit E-36: raw epoch is returned here; ISO-week labels are
        # computed in Python after fetch so week 52-53 → week 1
        # transitions don't split spanning weeks across two labels.
        sql = (
            "SELECT v.upload_ts, COUNT(*) "
            " FROM segments_fts fts "
            " JOIN segments s ON s.id = fts.rowid "
            " LEFT JOIN videos v ON v.video_id = s.video_id "
            " WHERE fts.text MATCH ? "
            " AND v.upload_ts IS NOT NULL"
        )
        args: List[Any] = [word]
        if channel:
            sql += " AND s.channel=?"
            args.append(channel)
        # No GROUP BY here — we aggregate in Python using isocalendar().
    else:
        # FTS5 MATCH to find segments containing the word
        group_col = ("year || '-' || printf('%02d', month)"
                     if bucket == "month" else "year")
        sql = (f"SELECT {group_col} AS bucket, COUNT(*) "
               f" FROM segments_fts fts "
               f" JOIN segments s ON s.id = fts.rowid "
               f" WHERE fts.text MATCH ?")
        args = [word]
        if channel:
            sql += " AND s.channel=?"
            args.append(channel)
        sql += " GROUP BY bucket ORDER BY bucket"
    try:
        with _db_lock:
            rows = conn.execute(sql, args).fetchall()
    except sqlite3.Error as e:
        return {"labels": [], "values": [], "error": str(e)}
    # audit E-36: for week bucket, aggregate in Python using
    # isocalendar() so year-boundary weeks (e.g. 2024-12-30 is in
    # ISO week 2025-W01) don't split into two half-sized bars.
    if bucket == "week":
        import datetime as _dt_w
        counts_by_iso: Dict[str, int] = {}
        for ts, cnt in rows:
            if ts is None:
                continue
            try:
                _dtobj = _dt_w.datetime.fromtimestamp(float(ts))
                iso = _dtobj.isocalendar()
                key = f"{iso.year:04d}-W{iso.week:02d}"
            except Exception:
                continue
            counts_by_iso[key] = counts_by_iso.get(key, 0) + int(cnt)
        _sorted = sorted(counts_by_iso.items())
        labels = [k for k, _ in _sorted]
        values = [v for _, v in _sorted]
    else:
        labels = [str(r[0]) for r in rows if r[0] is not None]
        values = [int(r[1]) for r in rows if r[0] is not None]
    # bug M-13: when the caller requests week-granularity data while
    # backfill_upload_ts is still populating, the query silently returns
    # sparse results. Surface a `backfill_pending` count so the UI can
    # show "Still indexing... N videos pending" instead of letting the
    # user think their channel has no recent activity.
    backfill_pending = 0
    if bucket == "week":
        try:
            with _db_lock:
                row = conn.execute(
                    "SELECT COUNT(*) FROM videos WHERE upload_ts IS NULL"
                ).fetchone()
            if row:
                backfill_pending = int(row[0] or 0)
        except sqlite3.Error:
            pass
    return {"labels": labels, "values": values,
            "backfill_pending": backfill_pending}


def graph_multi(words: List[str], channel: Optional[str] = None,
                bucket: str = "month") -> Dict[str, Any]:
    """Multiple word-frequency series on one x axis.

    Returns { labels: [...], series: [{word, values: [...]}, ...] }
    so the JS can draw one line per word, all sharing the merged time range.
    """
    words = [w.strip() for w in (words or []) if w and w.strip()]
    if not words:
        return {"labels": [], "series": []}
    per_word = {}
    label_set = set()
    for w in words:
        r = graph_word_frequency(w, channel=channel, bucket=bucket)
        mapping = dict(zip(r.get("labels", []), r.get("values", [])))
        per_word[w] = mapping
        label_set.update(mapping.keys())
    labels = sorted(label_set)
    series = []
    for w in words:
        m = per_word[w]
        series.append({"word": w, "values": [m.get(lbl, 0) for lbl in labels]})
    return {"labels": labels, "series": series}


# Alias matching main.py's original call site (Session 11)
graph_word_frequency_multi = graph_multi


def graph_channel_overlay(word: str, channels: List[str],
                          bucket: str = "month") -> Dict[str, Any]:
    """Same word across multiple channels — each channel is a series.

    Returns { labels: [...], series: [{channel, values: [...]}, ...] }.
    """
    channels = [c for c in (channels or []) if c]
    if not word or not channels:
        return {"labels": [], "series": []}
    per_ch = {}
    label_set = set()
    for ch in channels:
        r = graph_word_frequency(word, channel=ch, bucket=bucket)
        mapping = dict(zip(r.get("labels", []), r.get("values", [])))
        per_ch[ch] = mapping
        label_set.update(mapping.keys())
    labels = sorted(label_set)
    series = [{"channel": ch, "values": [per_ch[ch].get(lbl, 0) for lbl in labels]}
              for ch in channels]
    return {"labels": labels, "series": series}


def graph_word_frequency_multi(words: List[str], channel: Optional[str] = None,
                                bucket: str = "month") -> Dict[str, Any]:
    """Run multiple word-frequency queries in one call. Returns a shape
    ready for Chart.js with one dataset per word."""
    out = {"labels": [], "series": []}
    if not words:
        return out
    per = []
    all_labels = set()
    for w in words:
        r = graph_word_frequency(w, channel=channel, bucket=bucket)
        per.append({"word": w, "data": dict(zip(r["labels"], r["values"]))})
        all_labels.update(r["labels"])
    labels = sorted(all_labels)
    out["labels"] = labels
    for p in per:
        out["series"].append({
            "word": p["word"],
            "values": [p["data"].get(l, 0) for l in labels],
        })
    return out


def list_all_channels_in_db() -> List[str]:
    """Return the distinct set of channels that appear in the segments table."""
    conn = _open()
    if conn is None:
        return []
    with _db_lock:
        cur = conn.execute("SELECT DISTINCT channel FROM segments ORDER BY channel COLLATE NOCASE")
        return [r[0] for r in cur.fetchall() if r[0]]


def _sanitize_fts_query(q: str) -> str:
    """Defensive fallback sanitizer for FTS5 MATCH queries.

    The UI exposes AND/OR/NOT/"phrase"/prefix* as power-user operators.
    If the raw query has syntax that FTS5 rejects (unbalanced quotes,
    stray punctuation), this function is tried as a second chance:
    strip everything that isn't word-chars / space / quote / * / - so
    FTS5 treats it as implicit-AND across bare terms — matches OLD's
    YTArchiver.py:29728 stripping behavior.
    """
    import re as _re
    # Keep word chars, spaces, quotes, wildcard, minus (NOT). Drop everything else.
    cleaned = _re.sub(r'[^\w\s"*\-]', " ", q or "")
    # Collapse whitespace
    cleaned = _re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def search_fts(query: str, channel: Optional[str] = None, limit: int = 200,
               year_from: Optional[int] = None, year_to: Optional[int] = None
               ) -> List[Dict[str, Any]]:
    """Run FTS5 MATCH against segments. Returns hits with context.

    Query semantics: power-user operators (AND / OR / NOT / "phrase" / word*)
    pass through to FTS5 as-is on the first attempt. If that raises a syntax
    error (common when users paste something with unbalanced quotes or
    parentheses), the function retries with a sanitizer that strips all
    non-word punctuation and lets FTS5 treat the result as implicit-AND —
    matching YTArchiver.py:29728 behavior. Empty result on second failure.

    Optional `year_from` / `year_to` filter the segment by `segments.year`
    (inclusive). Either bound may be None.
    """
    conn = _open()
    if conn is None or not query.strip():
        return []
    q = ("SELECT s.id, s.video_id, s.title, s.channel, s.start_time, s.text, "
         " s.jsonl_path, snippet(segments_fts, 0, '<mark>', '</mark>', '...', 8) as snip "
         " FROM segments_fts JOIN segments s ON s.id = segments_fts.rowid "
         " WHERE segments_fts MATCH ?")
    args_suffix: List[Any] = []
    suffix = ""
    if channel:
        suffix += " AND s.channel=?"
        args_suffix.append(channel)
    # audit D-49: OR-include s.year IS NULL when a year filter is set.
    # Legacy rows (drop-in mode, pre-path-parsing, channels where the
    # folder layout isn't year-organized) have segments.year=NULL;
    # without the NULL clause the filter silently excluded them even
    # though the user's intent was "all results within this window,
    # including ones we can't place yet". Net effect: year-filtered
    # searches now include NULL-year rows rather than missing them.
    if year_from is not None:
        suffix += " AND (s.year >= ? OR s.year IS NULL)"
        args_suffix.append(int(year_from))
    if year_to is not None:
        suffix += " AND (s.year <= ? OR s.year IS NULL)"
        args_suffix.append(int(year_to))
    suffix += " LIMIT ?"
    args_suffix.append(limit)

    def _run(q_text: str):
        with _db_lock:
            cur = conn.execute(q + suffix, [q_text] + args_suffix)
            return cur.fetchall()

    rows: List[Any] = []
    try:
        rows = _run(query)
    except sqlite3.Error:
        # Retry once with the sanitized query — gives user-typed input a chance
        # to match even with stray punctuation or unbalanced quotes.
        cleaned = _sanitize_fts_query(query)
        if cleaned and cleaned != query:
            try:
                rows = _run(cleaned)
            except sqlite3.Error as e2:
                # Bug [52]: returning [{"error": ...}] poisoned the
                # iterator since callers access r["segment_id"] etc.
                # Print the error and return an empty list so the UI
                # renders "no results" cleanly instead of crashing.
                try: print(f"[search_fts] FTS error: {e2}")
                except Exception: pass
                return []
        else:
            try: print(f"[search_fts] Invalid FTS5 query: {query!r}")
            except Exception: pass
            return []
    return [{
        "segment_id": r[0], "video_id": r[1], "title": r[2], "channel": r[3],
        "start_time": r[4], "text": r[5], "jsonl_path": r[6], "snippet": r[7],
    } for r in rows]


# ── Bookmarks ───────────────────────────────────────────────────────────

def bookmark_add(video_id: str, title: str, channel: str,
                 start_time: float, text: str, note: str = "") -> Optional[int]:
    conn = _open()
    if conn is None:
        return None
    with _db_lock:
        cur = conn.execute(
            "INSERT INTO bookmarks (video_id, title, channel, start_time, text, note) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (video_id, title, channel, start_time, text, note),
        )
        conn.commit()
        return cur.lastrowid


def bookmark_list(limit: int = 500) -> List[Dict[str, Any]]:
    conn = _open()
    if conn is None:
        return []
    with _db_lock:
        cur = conn.execute(
            "SELECT id, video_id, title, channel, start_time, text, note, created "
            "FROM bookmarks ORDER BY created DESC LIMIT ?",
            (limit,),
        )
        return [{
            "id": r[0], "video_id": r[1], "title": r[2], "channel": r[3],
            "start_time": r[4], "text": r[5], "note": r[6], "created": r[7],
        } for r in cur.fetchall()]


def bookmark_remove(bm_id: int) -> bool:
    # audit D-47: return True only when an actual row changed. Old
    # behavior returned True unconditionally, so a stale-id click (e.g.
    # double-click after another session already deleted it) surfaced
    # as "Bookmark removed" while nothing happened, then the next
    # refresh showed the bookmark still there. Now False = nothing
    # matched that id.
    conn = _open()
    if conn is None:
        return False
    with _db_lock:
        cur = conn.execute("DELETE FROM bookmarks WHERE id=?", (bm_id,))
        conn.commit()
    return cur.rowcount > 0


def bookmark_update_note(bm_id: int, note: str) -> bool:
    # audit D-47: same reasoning as bookmark_remove — return False when
    # the id didn't match anything so callers don't show misleading
    # success toasts.
    conn = _open()
    if conn is None:
        return False
    with _db_lock:
        cur = conn.execute(
            "UPDATE bookmarks SET note=? WHERE id=?", (note, bm_id))
        conn.commit()
    return cur.rowcount > 0


# ── Stats ───────────────────────────────────────────────────────────────

def summary() -> Dict[str, Any]:
    conn = _open()
    if conn is None:
        return {"segments": 0, "videos": 0, "channels": 0, "bookmarks": 0}
    with _db_lock:
        seg = conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
        vid = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
        ch = conn.execute("SELECT COUNT(DISTINCT channel) FROM videos").fetchone()[0]
        bm = conn.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0]
    return {"segments": seg, "videos": vid, "channels": ch, "bookmarks": bm}


# ── Startup sweep: register new video files that appeared on disk ──────

def sweep_new_videos(output_dir: str, channels: list,
                     progress_cb=None) -> dict:
    """Walk each channel folder under `output_dir`, register any video
    file not already in the videos table, and ingest any paired .jsonl
    that isn't in segments yet.

    Matches YTArchiver's disk-scan behavior at :3012 _scan_channel_disk_info —
    picks up files added manually or while the app was closed.

    Optional `progress_cb(idx, total, channel_name)` is invoked as each
    channel starts so the caller can update a "Loading… N/M (channel)"
    status line. Called on the same thread as the walk.

    Returns {registered, ingested} counts.

    The sweep uses its OWN sqlite3 connection (via _open_independent)
    so its many per-file writes don't go through the shared `_db_lock`.
    Without this, sync's DLTRACK register_video calls + transcribe's
    FTS-ingest calls all serialized behind the sweep's lock acquisition,
    causing visible "Downloading 100%" hangs of many minutes during
    boot. WAL mode handles cross-connection serialization at the
    SQLite layer instead.
    """
    from pathlib import Path as _Path
    import os as _os

    if not output_dir:
        return {"registered": 0, "ingested": 0}
    # Make sure the shared connection's schema-init has run at least
    # once (creates tables, sets PRAGMAs at the file level).
    _ = _open()
    sweep_conn = _open_independent()
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
    from .archive_scan import load_disk_cache as _load_dc, save_disk_cache as _save_dc
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
        if progress_cb is not None:
            try: progress_cb(i_ch + 1, total_ch, ch_name)
            except Exception: pass
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
                            # for _db_lock — see _open_independent docstring.
                            if ingest_jsonl(fp, jp, title, ch_name,
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
                    register_video(fp, ch_name, _conn_override=sweep_conn)
                    registered += 1
                    # Ingest .jsonl sidecar if present.
                    base = _os.path.splitext(fp)[0]
                    jp = base + ".jsonl"
                    if _os.path.isfile(jp):
                        title = _strip_id.sub("", _os.path.basename(base)) or _os.path.basename(base)
                        if ingest_jsonl(fp, jp, title, ch_name,
                                        _conn_override=sweep_conn):
                            ingested += 1
        # Channel walk completed — stamp the fingerprint so next
        # sweep can skip if unchanged. Stamp AFTER the walk so a
        # crash mid-walk doesn't leave a stale "skip me" flag.
        #
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
            # bug L-1: tightened to `and` — update_disk_cache_for_channel
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
        except Exception:
            pass

    # Close the sweep's private connection — best-effort, don't fail the
    # whole sweep if close raises (DB file is fine either way).
    try:
        sweep_conn.close()
    except Exception:
        pass

    return {"registered": registered, "ingested": ingested,
            "skipped_unchanged": skipped_unchanged,
            "walked": total_ch - skipped_unchanged}


def prune_missing_videos() -> Dict[str, int]:
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
    conn = _open()
    if conn is None:
        return {"videos_removed": 0, "segments_removed": 0,
                "missing": 0, "zero_byte": 0, "duplicate_id": 0}
    videos_removed = 0
    segs_removed = 0
    n_missing = n_zero = n_dup = n_fake_id = 0
    affected_channels: set = set()
    try:
        with _db_lock:
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
                        # audit H-9: cascade the segment delete into
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
                            except Exception:
                                pass
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
                invalidate_channel_videos(_ch)
            except Exception:
                pass
    except Exception as e:
        print(f"[index] prune_missing_videos failed: {e}")
    return {"videos_removed": videos_removed,
            "segments_removed": segs_removed,
            "missing": n_missing, "zero_byte": n_zero,
            "duplicate_id": n_dup,
            "fake_id_cleared": n_fake_id}


# ── Rebuild FTS index from scratch (rebuild button on Index tab) ────────

def rebuild_fts_index() -> Dict[str, Any]:
    """Drop segments_fts virtual table and rebuild it by reinserting every
    row from segments. Safe to run — preserves the segments table itself.
    Returns {ok, rows_indexed} or {ok: False, error}.
    Use when FTS seems broken (search returns nothing despite visible segments)
    or after a DB schema migration.
    """
    conn = _open()
    if conn is None:
        return {"ok": False, "error": "DB unavailable"}
    try:
        with _db_lock:
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
            # bug M-4: `indexed_files` (the table used to compute the
            # "unindexed transcripts" warning banner) is only populated
            # by ingest_jsonl. A pure FTS rebuild would leave the banner
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
