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
from datetime import UTC
from pathlib import Path
from typing import Any

from .log import get_logger
from .ytarchiver_config import TRANSCRIPTION_DB

_log = get_logger(__name__)


_db_lock = threading.RLock()

# Pending tx_status='transcribed' retries — coalesces the burst that
# arrives when ingest races sync_write_video INSERTs (audit: index.py
# H109). One drain thread processes all pending fps serially instead
# of spawning a daemon thread per ingest.
_tx_retry_set: set = set()
_tx_retry_lock = threading.Lock()
_tx_retry_thread: "threading.Thread | None" = None
def _enqueue_tx_retry(fp: str) -> None:
    global _tx_retry_thread
    with _tx_retry_lock:
        _tx_retry_set.add(fp)
        if _tx_retry_thread is not None and _tx_retry_thread.is_alive():
            return
        def _drain():
            import time as _t
            _t.sleep(0.5)
            while True:
                with _tx_retry_lock:
                    if not _tx_retry_set:
                        return
                    _batch = list(_tx_retry_set)
                    _tx_retry_set.clear()
                try:
                    _main = _open()
                    if _main is None:
                        return
                    with _db_lock:
                        for _fp in _batch:
                            try:
                                _main.execute(
                                    "UPDATE videos SET tx_status='transcribed' "
                                    "WHERE filepath=? COLLATE NOCASE", (_fp,))
                            except Exception as e:
                                _log.debug("tx retry one failed: %s", e)
                        _main.commit()
                except Exception as e:
                    _log.debug("tx retry batch failed: %s", e)
        _tx_retry_thread = threading.Thread(target=_drain, daemon=True,
                                             name="tx-retry-coalesce")
        _tx_retry_thread.start()
_conn: sqlite3.Connection | None = None
# Per-jsonl_path locks used by ingest_jsonl. When a caller passes their
# own connection (_conn_override) we skip _db_lock for throughput, but
# the DELETE+INSERT pair inside ingest_jsonl must still be atomic per-
# jsonl_path: otherwise sync's main-thread ingest and sweep's
# parallel ingest can interleave on the SAME .jsonl, producing
# duplicate segments (B's INSERT after A's DELETE+INSERT before A's
# commit) or vanished segments (B's DELETE after A's INSERT before
# A's commit). This dict gives each jsonl_path its own logical lock.
_ingest_locks: dict[str, threading.Lock] = {}
_ingest_locks_lock = threading.Lock()


def _ingest_lock_for(jsonl_path: str) -> threading.Lock:
    """Return the per-path Lock used to serialize ingest_jsonl on the
    same .jsonl across connections. Thread-safe lazy-create."""
    key = os.path.normpath(jsonl_path).lower()
    with _ingest_locks_lock:
        lk = _ingest_locks.get(key)
        if lk is None:
            lk = threading.Lock()
            _ingest_locks[key] = lk
    return lk

# a dedicated read-only connection for Browse queries so
# they never wait on `_db_lock` (held by sync register_video, FTS
# ingest, etc.). WAL handles cross-connection serialization so this
# connection sees the writer's committed state without blocking.
# Wrapped in its own lock because SQLite connections aren't
# free-threaded — readers serialize across this connection only.
_reader_conn: sqlite3.Connection | None = None
_reader_lock = threading.RLock()
# Flag set to True once `_open()` has successfully initialized the
# schema. `_reader_open()` checks this WITHOUT grabbing `_db_lock`
# so a read connection can be returned even while a long-running
# writer (FTS ingest, etc.) holds the writer lock. Without this
# gate, _reader_open's "ensure schema exists" call had to take
# `_db_lock` itself, which made Browse / Metadata / Recent queries
# wait behind ingest_jsonl transactions that hold the lock for
# many seconds at a time.
_schema_inited: bool = False


def _reader_open() -> sqlite3.Connection | None:
    """Open or return the long-lived read-only connection used by
    Browse-style queries. Separate from `_conn` so writer contention
    on `_db_lock` doesn't block the Browse tab during indexing.

    Schema is initialized lazily by the FIRST `_open()` call any
    caller makes; once the `_schema_inited` flag flips True we skip
    re-checking it from this path so we never have to wait on
    `_db_lock`.
    """
    global _reader_conn
    if not _schema_inited:
        # Cold-start path: schema may not exist yet. Take the slow
        # route ONCE to make sure the DB file + tables exist.
        try: _open()
        except Exception as e: _log.debug("swallowed: %s", e)
    with _reader_lock:
        if _reader_conn is not None:
            return _reader_conn
        try:
            TRANSCRIPTION_DB.parent.mkdir(parents=True, exist_ok=True)
            _reader_conn = sqlite3.connect(
                str(TRANSCRIPTION_DB),
                check_same_thread=False, timeout=30.0)
            _reader_conn.execute("PRAGMA journal_mode=WAL")
            _reader_conn.execute("PRAGMA synchronous=NORMAL")
            _reader_conn.execute("PRAGMA query_only=ON")
            return _reader_conn
        except Exception:
            _reader_conn = None
            return None


def _open_independent() -> sqlite3.Connection | None:
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

def _open() -> sqlite3.Connection | None:
    """Open or return the cached connection. Returns None if DB can't be opened."""
    global _conn
    with _db_lock:
        if _conn is not None:
            return _conn
        try:
            TRANSCRIPTION_DB.parent.mkdir(parents=True, exist_ok=True)
            _conn = sqlite3.connect(str(TRANSCRIPTION_DB), check_same_thread=False, timeout=10.0)
            # Check the PRAGMA result — on some filesystems (network
            # shares without shared-memory support, certain DrivePool
            # configs) WAL silently falls back to "delete" mode, and
            # downstream code that assumes WAL semantics
            # (readers-don't-block-writers) would block the entire UI
            # during ingests. Log loudly if WAL didn't engage so the
            # user has a chance to investigate.
            _wal_row = _conn.execute("PRAGMA journal_mode=WAL").fetchone()
            _wal_mode = (_wal_row[0] if _wal_row else "").lower()
            if _wal_mode != "wal":
                _log.warning(
                    "PRAGMA journal_mode=WAL did not engage on %s "
                    "(got %r). Browse/Recent queries may block "
                    "behind sync writers — file an issue with this msg.",
                    TRANSCRIPTION_DB, _wal_mode)
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
            # Patch 6 (2026-05-17): schema versioning via PRAGMA user_version.
            # The legacy ALTER-TABLE-then-ignore-OperationalError pattern below
            # has worked but offers no path forward for non-idempotent
            # migrations (column renames, constraint changes, data backfills).
            # Going forward: bump SCHEMA_VERSION and add a step to _MIGRATIONS
            # that runs once and only once at startup. Current installs are at
            # version 1 (the implicit pre-versioning baseline after all the
            # ALTER statements below succeed).
            try:
                _current_v = _conn.execute("PRAGMA user_version").fetchone()[0]
            except Exception:
                _current_v = 0
            _SCHEMA_VERSION = 1
            # Future migrations: _MIGRATIONS[2] = lambda c: c.execute("...")
            _MIGRATIONS: dict = {}
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
                # Marked when bulk views/likes refresh sees the file's
                # video_id is NOT in YouTube's current flat-playlist
                # response (uploader deleted / privated / unlisted the
                # video). UI uses this to show a red ✗ on per-video
                # tiles and a channel-level "N gone from YT" stat.
                # Future refresh runs can skip these rows so we stop
                # wasting yt-dlp calls on dead vids. Cleared if the
                # vid reappears in the catalog (uploader restored it).
                "ALTER TABLE videos ADD COLUMN removed_from_yt_ts REAL",
                # Persisted "does this video have a thumbnail sidecar
                # somewhere in the channel folder?" flag. Populated
                # lazily by `count_thumbnail_status_bulk` on first
                # walk; updated by `sweep_missing_thumbnails` when a
                # thumb is downloaded. Lets the Settings > Metadata
                # Thumbnails column query the same SQL GROUP BY path
                # as Video IDs — no disk walk on every page open. NULL
                # = "not yet checked", 0 = no thumb, 1 = thumb on disk.
                "ALTER TABLE videos ADD COLUMN has_thumbnail INTEGER",
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
                # COLLATE NOCASE companion — the bare idx_vid_channel
                # index doesn't get used by queries that compare
                # `channel = ? COLLATE NOCASE` (the channel column
                # itself lacks the COLLATE NOCASE attribute, so the
                # index's binary collation differs from the query's).
                # drift_scan._lookup_video_filepaths and several
                # metadata refresh paths use that form; without this
                # index they table-scan the videos table.
                "CREATE INDEX IF NOT EXISTS idx_vid_channel_nocase "
                "ON videos(channel COLLATE NOCASE)",
                "CREATE INDEX IF NOT EXISTS idx_vid_ch_yr ON videos(channel, year)",
                "CREATE INDEX IF NOT EXISTS idx_vid_ch_yr_mo ON videos(channel, year, month)",
                "CREATE INDEX IF NOT EXISTS idx_vid_video_id ON videos(video_id)",
                # compound index so the cross-channel
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
            # run pending migrations (none currently — framework
            # only) and bump user_version once we're at the target. Wrapped
            # in try/except so a future migration bug can't brick startup.
            try:
                for _v in sorted(_MIGRATIONS):
                    if _v > _current_v:
                        _MIGRATIONS[_v](_conn)
                if _current_v != _SCHEMA_VERSION:
                    _conn.execute(f"PRAGMA user_version = {int(_SCHEMA_VERSION)}")
            except Exception as e:
                _log.debug("schema migration step failed: %s", e)
            _conn.commit()
            # Mark schema-ready so future _reader_open() calls can
            # skip the _db_lock-acquiring _open() call entirely.
            global _schema_inited
            _schema_inited = True
            return _conn
        except sqlite3.Error as e:
            print(f"[index] Could not open DB: {e}")
            return None


# ── Video registration (fixes tkinter bug #4/#5) ────────────────────────

_ID_RE_IN_NAME = re.compile(r"\[([A-Za-z0-9_-]{11})\]")


def _parse_year_month_from_path(filepath: str) -> tuple[int | None, int | None]:
    """Best-effort year/month from a path like .../<channel>/<year>/<Month>/<file>.

    walk parts TAIL-FIRST so the innermost year/month wins.
    Deep archive paths like `Z:\\Archive\\2024\\Channels\\SomeCh\\2020\\
    March\\file.mp4` used to set year=2024 (from archive root), then
    overwrite to 2020 (correct) — but a base path containing a 4-digit
    year-like number mid-path could silently clobber the real channel
    year. Tail-first is unambiguous: the nearest year ancestor matters.
    """
    parts = list(Path(filepath).parts)
    months = ["january","february","march","april","may","june",
              "july","august","september","october","november","december"]
    year: int | None = None
    month: int | None = None
    # Walk from the file back to the root, grabbing month then year on
    # the first hits. "NN Month" patterns (e.g. "01 January") also match.
    # Year-hit is constrained to within MAX_YEAR_DEPTH path components
    # from the file so an archive root literally named "2024" doesn't
    # win over the channel-level year folder. Real layouts are at
    # most <channel>/<year>/<month>/file.mp4 (depth 3) or
    # <channel>/<year>/file.mp4 (depth 2).
    MAX_YEAR_DEPTH = 3
    for depth, p in enumerate(reversed(parts), start=0):
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
        # Year hit — pure 4-digit in the valid range. Skip if the
        # component is too far from the file to plausibly be a
        # channel-level year folder.
        if depth > MAX_YEAR_DEPTH:
            break
        if p.isdigit() and 1900 < int(p) < 2100:
            year = int(p)
            break
    return year, month


def register_video(filepath: str, channel: str, title: str | None = None,
                   tx_status: str = "pending",
                   video_id: str | None = None,
                   duration_secs: float | None = None,
                   _conn_override: sqlite3.Connection | None = None) -> bool:
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
    # consolidated into text_utils.extract_video_id.
    from .text_utils import extract_video_id as _extract_vid
    vid_id = _extract_vid(
        fp,
        hint=(video_id or "").strip(),
        reject_alpha_only=True,
        info_json_fallback=True,
    ) or None
    vid_url = f"https://www.youtube.com/watch?v={vid_id}" if vid_id else None
    year, month = _parse_year_month_from_path(fp)
    # Single os.stat() call rather than separate isfile + getsize +
    # getmtime. The triple-call version had a TOCTOU window where Z:\
    # DrivePool could relocate the file between isfile and getsize,
    # giving size=0 / upload_ts=None for a file that DID exist — later
    # prune passes would then misclassify the row as zero_byte and
    # flag it for deletion. Single stat captures a consistent snapshot.
    try:
        st = os.stat(fp)
        size = st.st_size
        upload_ts = st.st_mtime
    except OSError:
        size = 0
        upload_ts = None
    try:
        # When the caller provided their own connection, skip _db_lock
        # entirely — SQLite's WAL handles cross-connection serialization,
        # and acquiring the Python lock would re-introduce the bottleneck
        # this whole feature exists to bypass.
        from contextlib import nullcontext as _nullctx
        _ctx = _nullctx() if use_override else _db_lock

        def _do_register_write():
            # preserve `added_ts` on re-register.
            # INSERT OR REPLACE silently wiped it every time sweep
            # re-registered an existing video, making "new in last 7
            # days" (Dashboard) and Recent-sort by added_ts useless —
            # every video touched by ANY sweep/invalidation/
            # mark_transcribed counted as "new". UPSERT with an
            # explicit `added_ts=excluded.added_ts` would still reset;
            # instead we write added_ts=? only if the row is new, via
            # COALESCE against the existing value.
            # populate duration_s so "Sort by duration" and
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
                     /* tx_status: preserve any non-'pending' value on
                        re-register. Sweep's default tx_status='pending'
                        would otherwise stomp 'transcribed' / 'no_captions'
                        every time it walks the disk, flipping completed
                        videos back to pending and re-queueing them for
                        Whisper. Mark-transitions still work because explicit
                        callers (mark_video_transcribed et al.) pass the
                        non-'pending' value directly. */
                     tx_status=CASE
                       WHEN excluded.tx_status = 'pending'
                            AND videos.tx_status IS NOT NULL
                            AND videos.tx_status != 'pending'
                         THEN videos.tx_status
                       ELSE excluded.tx_status
                     END,
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

        # Retry on a transient "database is locked"/"busy" — a concurrent
        # disk scan can hold the writer lock past the connection timeout,
        # which previously made register_video silently return False and
        # drop a just-downloaded video from the index. Back off and retry
        # so the registration actually lands. Non-transient sqlite errors
        # fall through to the outer handler immediately.
        _attempts = 6
        for _attempt in range(_attempts):
            try:
                with _ctx:
                    _do_register_write()
                break
            except sqlite3.OperationalError as _oe:
                _msg = str(_oe).lower()
                if ("locked" in _msg or "busy" in _msg) and _attempt < _attempts - 1:
                    print(f"[index] register_video DB busy, retry "
                          f"{_attempt + 1}/{_attempts}: {_oe}")
                    time.sleep(0.5 * (_attempt + 1))
                    continue
                raise
        # Drop the browse-list cache for this channel so the next grid
        # click picks up the newly-registered video.
        try: invalidate_channel_videos(channel)
        except Exception as e: _log.debug("swallowed: %s", e)
        # if a transcript JSONL sidecar is already on disk at
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
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return True
    except sqlite3.Error as e:
        print(f"[index] register_video failed: {e}")
        return False


def mark_video_transcribed(filepath: str,
                            _conn_override: "sqlite3.Connection | None" = None) -> bool:
    """Flip the tx_status flag for `filepath` to 'transcribed'.

    `_conn_override` lets the caller pass an independent connection
    (from `_open_independent()`) so this UPDATE doesn't queue behind
    a sync writer holding `_db_lock`. Matches the register_video /
    ingest_jsonl override pattern (audit: index.py H104).
    """
    fp = os.path.normpath(filepath)
    use_override = _conn_override is not None
    # Acquire the connection INSIDE the lock to close the re-check
    # race the old code had between `_open()` and `with _db_lock:`
    # — between those two lines another thread could close + reopen
    # the connection (audit: index.py H107).
    try:
        if use_override:
            conn = _conn_override
            conn.execute("UPDATE videos SET tx_status='transcribed' "
                          "WHERE filepath=? COLLATE NOCASE", (fp,))
            row = conn.execute(
                "SELECT channel FROM videos WHERE filepath=? COLLATE NOCASE",
                (fp,)).fetchone()
            conn.commit()
        else:
            with _db_lock:
                conn = _open()
                if conn is None:
                    return False
                conn.execute("UPDATE videos SET tx_status='transcribed' "
                              "WHERE filepath=? COLLATE NOCASE", (fp,))
                row = conn.execute(
                    "SELECT channel FROM videos WHERE filepath=? COLLATE NOCASE",
                    (fp,)).fetchone()
                conn.commit()
        if row and row[0]:
            try: invalidate_channel_videos(row[0])
            except Exception as e: _log.debug("swallowed: %s", e)
        return True
    except sqlite3.Error:
        return False


# ── Transcript ingest (from .jsonl sidecar) ─────────────────────────────

def ingest_jsonl(video_filepath: str, jsonl_path: str,
                 title: str, channel: str,
                 _conn_override: sqlite3.Connection | None = None) -> int:
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
    # Pick the LAST bracketed 11-char group, not the first. yt-dlp
    # always appends the real video_id last, so a filename like
    # "Foo [bar-channel] [abc12_def-3].mp4" should pick "abc12_def-3"
    # — but .search() returned the FIRST match ("bar-channel") and
    # the channel-tag-leading filename pattern stamped a fake id
    # onto every segment (audit: index.py:526). Reject pure-letter
    # matches too: real YT ids are random picks from the 64-char
    # alphabet and statistically always include a digit, _, or -.
    _matches = _ID_RE_IN_NAME.findall(os.path.basename(fp))
    for _cand in reversed(_matches):
        if not _cand.isalpha():
            vid_id = _cand
            break
    if vid_id is None and _matches:
        # Fall back to last match even if all-alpha — better than
        # nothing for the rare valid YT id that happens to be all
        # letters.
        vid_id = _matches[-1]
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
        # (see register_video for the reasoning). BUT the DELETE+INSERT
        # pair below must still be atomic per-jsonl_path, otherwise two
        # threads ingesting the same .jsonl on different connections
        # can interleave and produce duplicate or vanished segments.
        # A per-path lock here, combined with WAL handling
        # cross-connection serialization at the per-statement level,
        # gives us the right granularity.
        from contextlib import nullcontext as _nullctx
        _ctx = _nullctx() if use_override else _db_lock
        _path_lock = _ingest_lock_for(jp)
        with _path_lock, _ctx:
            # FTS5 external-content tables don't auto-sync
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
            # flip tx_status='transcribed' on successful
            # ingest so channel_transcription_stats reflects reality
            # right away. Previously only mark_video_transcribed did
            # this, and if ingest_jsonl was called without a follow-up
            # mark_video_transcribed the Edit-channel footer and stats
            # queries reported stale 'pending' counts for videos that
            # had segments in the index.
            _tx_cur = conn.execute(
                "UPDATE videos SET tx_status='transcribed' "
                "WHERE filepath=? COLLATE NOCASE", (fp,))
            _tx_rowcount = _tx_cur.rowcount or 0
            conn.commit()
            # If the UPDATE matched 0 rows, the videos row may still be
            # mid-INSERT on another connection (race during a boot
            # sweep that overlaps with sync's per-download path). Retry
            # the flip on the shared writer connection a moment later
            # so the row, once committed, still ends up flagged. Best
            # effort: skip silently if anything fails.
            if _tx_rowcount == 0:
                # Coalesce retries onto a single pending set keyed by
                # filepath so a burst of ingests racing the boot sweep
                # doesn't spawn N daemon threads (audit: index.py
                # H109). One worker thread drains the set serially.
                _enqueue_tx_retry(fp)
        # Bug [53]: return the actually-inserted row count, not the
        # raw segment count from the JSONL. Empty-text segments and
        # malformed entries are filtered out above (lines 442-451) but
        # still inflated the reported "ingested" total before this fix.
        return len(rows)
    except sqlite3.Error as e:
        print(f"[index] ingest_jsonl failed: {e}")
        return 0


# ── Reads ───────────────────────────────────────────────────────────────

def list_recent_videos(limit: int = 200, channel: str | None = None
                       ) -> list[dict[str, Any]]:
    """Return the N newest videos (by added_ts), optionally filtered by channel.

    Uses `_reader_conn` (read-only, separate from `_conn`) so the Recent tab
    never waits on sync's `register_video` write lock. WAL handles the
    cross-connection visibility.
    """
    conn = _reader_open() or _open()
    if conn is None:
        return []
    q = ("SELECT title, channel, filepath, video_id, size_bytes, year, month, "
         "tx_status, added_ts FROM videos ")
    args: list[Any] = []
    if channel:
        q += "WHERE channel=? "
        args.append(channel)
    q += "ORDER BY added_ts DESC LIMIT ?"
    args.append(limit)
    with _reader_lock:
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
# Keyed by (channel_name, sort, limit, include_thumbs). Invalidated by
# `invalidate_channel_videos(channel)` whenever a video is added / deleted
# / re-transcribed for that channel.
_browse_videos_cache: dict[tuple[str, str, int, bool], list[dict[str, Any]]] = {}
_browse_cache_lock = threading.Lock()

# Per-channel thumbnail index cache. Keyed by channel-root path, value
# is {"mtime": float, "thumbs": {vid_id: thumb_path}}. The audit
# previously required a full os.walk of every Browse first-click; with
# this cache, a stale entry is detected via the channel-root mtime
# (Windows bumps a dir's mtime when entries change) and a re-walk only
# happens when the directory was actually touched.
_thumb_index_cache: dict[str, dict[str, Any]] = {}
_thumb_index_cache_lock = threading.Lock()


def invalidate_channel_videos(channel: str | None = None) -> None:
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


def preload_all_channels(channel_names: list[str],
                         progress_cb: Any | None = None,
                         cancel_ev: Any | None = None,
                         sort: str = "newest",
                         limit: int = 500) -> dict[str, int]:
    """Warm the per-channel video-list cache for every subscribed channel.

    Design rule: browse preload should ALWAYS be the bottom priority —
    if a user is downloading or loading the metadata page, that should
    supersede the preload.

    How we deliver on that:
    1. Reads go through `_reader_conn` (separate from the writer's
       `_conn`), so preload never grabs `_db_lock`. WAL mode means
       writers don't block readers and vice versa.
    2. A 30 ms politeness yield between channels lets other Python
       threads (sync worker, GPU worker, HTTP requests for metadata
       page) advance. On 100+ channels this adds ~3 seconds total —
       imperceptible vs the multi-minute preload duration on a real
       archive.
    3. If sync or GPU is actively running, we yield 200 ms instead so
       the user-visible work gets more breathing room.

    Returns {channel_name: row_count}.
    """
    import time as _t
    out: dict[str, int] = {}
    total = len(channel_names)
    for i, ch in enumerate(channel_names):
        if cancel_ev is not None and cancel_ev.is_set():
            break
        if progress_cb is not None:
            try: progress_cb(i + 1, total, ch)
            except Exception as e: _log.debug("swallowed: %s", e)
        try:
            out[ch] = preload_channel_videos(ch, sort=sort, limit=limit)
        except Exception:
            out[ch] = 0
        # Politeness yield. Active sync/GPU = longer yield.
        try:
            from . import sync as _sync
            active = _sync.is_any_sync_active()
        except Exception:
            active = False
        _t.sleep(0.2 if active else 0.03)
    return out


def list_videos_for_channel(channel: str, sort: str = "newest",
                            limit: int = 50000, include_thumbs: bool = True
                            ) -> list[dict[str, Any]]:
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
    # use the long-lived read-only connection
    # (`_reader_conn`, opened on first use below) so this Browse
    # query never contends on `_db_lock` with sync's register_video
    # calls. WAL mode handles cross-connection serialization for us.
    conn = _reader_open()
    if conn is None:
        return []
    order = {
        "newest": "COALESCE(year, 0) DESC, COALESCE(month, 0) DESC, COALESCE(added_ts, 0) DESC",
        "oldest": "COALESCE(year, 99999) ASC, COALESCE(month, 99) ASC, COALESCE(added_ts, 0) ASC",
        "largest": "COALESCE(size_bytes, 0) DESC",
        "title": "title COLLATE NOCASE ASC",
    }.get(sort, "COALESCE(year, 0) DESC, COALESCE(month, 0) DESC, COALESCE(added_ts, 0) DESC")
    with _reader_lock:
        # Select removed_from_yt_ts last so older DBs (where the column
        # may not exist yet during the first run after upgrade) can be
        # handled via try/except fallback.
        try:
            cur = conn.execute(
                f"SELECT title, channel, filepath, video_id, size_bytes, year, month, "
                f"tx_status, added_ts, removed_from_yt_ts FROM videos "
                f"WHERE channel=? COLLATE NOCASE AND is_duplicate_of IS NULL "
                f"ORDER BY {order} LIMIT ?",
                (channel, limit),
            )
            out = [{
                "title": r[0], "channel": r[1], "filepath": r[2], "video_id": r[3],
                "size_bytes": r[4] or 0, "year": r[5], "month": r[6],
                "tx_status": r[7], "added_ts": r[8],
                "removed_from_yt": bool(r[9]),
            } for r in cur.fetchall()]
        except Exception:
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
                "removed_from_yt": False,
            } for r in cur.fetchall()]
    # Enrich: upload_ts + view_count (from aggregated metadata) + thumbnails.
    # View count enables "Most Viewed" sort in the Browse grid without
    # making yt-dlp calls here — we rely on the data Cache built up during
    # sync-time metadata fetches.
    metadata_cache: dict[str, dict[str, Any]] = {}
    # Secondary title-keyed index, built alongside the video_id one so
    # DB rows with a NULL video_id (common for videos indexed before
    # the video_id column was populated consistently) can still resolve
    # their metadata by title match. Reported: an entire channel's
    # videos had video_id=NULL in the index DB, so every view_count
    # lookup failed — Most Viewed sort was a no-op for all 400+
    # videos because the sort comparator saw only zeros.
    metadata_cache_by_title: dict[str, dict[str, dict[str, Any]]] = {}

    # route through canonical helper. Defaults strip trailing
    # punct (".?!"), which is fine here — Most Viewed sort matching never
    # needs to distinguish "Foo!" from "Foo".
    from .text_utils import normalize_title as _norm_title  # type: ignore

    def _fetch_meta(folder_key):
        """Lazy-load the aggregated metadata JSONL for a given folder.
        Cache by folder so we don't re-parse the same file per row."""
        if folder_key in metadata_cache:
            return metadata_cache[folder_key]
        entries: dict[str, Any] = {}
        by_title: dict[str, Any] = {}
        _walk_failed = False
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
        except Exception as e:
            _walk_failed = True
            _log.debug("swallowed: %s", e)
        # Don't cache an EMPTY result that came from an exception path
        # (transient Z:\ DrivePool hiccup, etc.) — caching the empty
        # entries dict permanently zeros metadata for that folder for
        # the rest of the process lifetime. View_counts all show 0,
        # Most Viewed sort breaks. Cache only on clean success or
        # genuinely empty folder.
        if not _walk_failed:
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

    # Filter out rows whose mp4 file no longer exists on disk. Sync
    # can leave behind stale DB entries when a download starts but
    # fails / gets interrupted / the user later deletes the file —
    # those rows would render as gradient placeholders with no
    # thumbnail and clicking them would 404. Cheaper to hide them
    # entirely. Cost: ~one os.path.exists() per row, cached by the
    # browse cache anyway.
    out = [r for r in out
           if r.get("filepath") and os.path.exists(r["filepath"])]

    # Channel-wide thumbnail index. `find_thumbnail` walks UP the path
    # at most 3 levels — it does NOT cross over to sibling year folders.
    # On channels where thumbnails ended up in a different year's
    # `.Thumbnails/` than where the mp4 currently lives (e.g.
    # The PrimeTime had most thumbs in 2025/.Thumbnails/ but the mp4s
    # got re-foldered into 2023/ and 2024/), every Browse video tile
    # rendered as the gradient placeholder. Pre-walk the channel root
    # once, build vid → path, and use it for vid-keyed lookup. Falls
    # back to find_thumbnail for the no-vid / stem-only path.
    _thumb_by_vid: dict[str, str] = {}
    if include_thumbs and out:
        _vid_re_local = re.compile(r"\[([A-Za-z0-9_-]{11})\]")
        # Locate the channel root from any video's filepath. mp4s
        # live at one of:
        #    <root>/<year>/<file>           (split_years=True)
        #    <root>/<year>/<month>/<file>   (split_years + split_months)
        #    <root>/<file>                  (split_years=False)
        # If the mp4's parent dir name is a 4-digit year, go up one
        # level; if the grandparent's name is a 4-digit year and
        # the parent looks like a month bucket, go up two.
        # Earlier bug: walked up the FIRST dirname found, which
        # was just the year folder, so the channel-wide scan only
        # saw one year's .Thumbnails/.
        # Compute candidate channel roots across ALL rows and pick the
        # MOST-COMMON one — first-row-wins was wrong when the first
        # video happened to be in an orphan subfolder, misrouting the
        # whole channel's thumbnail walk (audit: index.py H117).
        _ch_root = None
        _year_re = re.compile(r"^[12][0-9]{3}$")
        _root_votes: dict[str, int] = {}
        for _row in out:
            _fp = _row.get("filepath") or ""
            if not _fp or not os.path.isfile(_fp):
                continue
            _parent = os.path.dirname(_fp)
            _gp = os.path.dirname(_parent)
            _parent_name = os.path.basename(_parent)
            _gp_name = os.path.basename(_gp)
            if _year_re.match(_parent_name):
                _cand = _gp
            elif _year_re.match(_gp_name):
                _cand = os.path.dirname(_gp)
            else:
                _cand = _parent
            if _cand and os.path.isdir(_cand):
                _root_votes[_cand] = _root_votes.get(_cand, 0) + 1
        if _root_votes:
            _ch_root = max(_root_votes.items(), key=lambda kv: kv[1])[0]
        if _ch_root and os.path.isdir(_ch_root):
            # Per-channel thumb index cache. The walk over a large
            # channel root (with many year/month subdirs each
            # holding a .Thumbnails) was the slowest step on
            # first-click Browse — seconds on Z:\ DrivePool. Skip
            # it entirely if the channel root mtime hasn't changed
            # since our last successful walk.
            try:
                _ch_mtime = os.path.getmtime(_ch_root)
            except OSError:
                _ch_mtime = 0.0
            _ch_root_key = os.path.normpath(_ch_root)
            with _thumb_index_cache_lock:
                _cached = _thumb_index_cache.get(_ch_root_key)
            if (_cached is not None
                    and _ch_mtime > 0
                    and _cached.get("mtime") == _ch_mtime):
                _thumb_by_vid = dict(_cached.get("thumbs") or {})
            else:
                try:
                    for _dp, _dns, _fns in os.walk(_ch_root):
                        if os.path.basename(_dp) != ".Thumbnails":
                            continue
                        for _fn in _fns:
                            _low = _fn.lower()
                            if not _low.endswith(
                                    (".jpg", ".jpeg", ".webp", ".png")):
                                continue
                            _m = _vid_re_local.search(_fn)
                            if _m:
                                _thumb_by_vid[_m.group(1)] = os.path.normpath(
                                    os.path.join(_dp, _fn))
                except OSError:
                    pass
                if _ch_mtime > 0:
                    with _thumb_index_cache_lock:
                        _thumb_index_cache[_ch_root_key] = {
                            "mtime": _ch_mtime,
                            "thumbs": dict(_thumb_by_vid),
                        }

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
            from datetime import datetime as _dt
            return _dt(int(s[0:4]), int(s[4:6]), int(s[6:8]),
                      tzinfo=UTC).timestamp()
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
            # a corrupted JSONL entry with view_count="N/A"
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
            tp = None
            _vid = (row.get("video_id") or "").strip()
            # Try the channel-wide vid→path map first (correctly finds
            # thumbs that live in sibling year folders).
            if _vid:
                tp = _thumb_by_vid.get(_vid)
            # Fall back to legacy find_thumbnail for vid-less rows or
            # stem-name layouts that the pre-walk wouldn't catch.
            if not tp:
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
    except Exception as e:
        _log.debug("swallowed: %s", e)
    from urllib.parse import quote
    p = os.path.abspath(path).replace("\\", "/")
    if not p.startswith("/"):
        p = "/" + p
    return "file://" + quote(p, safe="/:")


def new_videos_in_last_n_days(days: int = 7) -> dict[str, Any]:
    """Return {videos: N, channels: M} for videos added in the last N days.

    Uses the FTS DB's videos.added_ts column (Unix epoch). Silent-returns zeros
    on any DB error so the caller can show a bar with dashes gracefully.
    """
    out = {"videos": 0, "channels": 0, "channel_list": []}
    try:
        # Use the reader connection + reader lock — the previous code
        # used _open() (shared writer connection) WITHOUT _db_lock, which
        # races against any concurrent sync writer hitting the same
        # sqlite3.Connection object. Symptom: ProgrammingError "Recursive
        # use of cursors not allowed", or silent wrong counts.
        conn = _reader_open()
        if conn is None:
            return out
        import time as _t
        cutoff = _t.time() - (max(1, int(days)) * 86400.0)
        with _reader_lock:
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
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return out


def channel_transcription_stats(channel: str) -> dict[str, int]:
    """Return {total, transcribed, pending, failed} video counts for a channel.

    Matches on channel name via the videos table (NOCASE). Empty channel ->
    zeros. Safe to call with an uninitialized DB (returns zeros).
    """
    out = {"total": 0, "transcribed": 0, "pending": 0, "failed": 0}
    if not channel:
        return out
    try:
        # Use the reader connection + reader lock (was _open() without
        # _db_lock — same thread-safety race as new_videos_in_last_n_days).
        conn = _reader_open()
        if conn is None:
            return out
        # `mark_video_transcribed` writes tx_status='transcribed' (see
        # register/UPDATE at line 274), so the coverage count must match
        # that string. Earlier this query tested 'done' — a mismatch
        # that made fully-transcribed channels read as "0 / N" in the
        # Edit-channel disk-stats footer.
        # exclude duplicate rows (is_duplicate_of NOT NULL)
        # from the counts. The Browse grid hides duplicates already,
        # so the footer "N/M transcribed" should match the visible
        # row count, not include hidden dups.
        with _reader_lock:
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
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return out


def find_thumbnail(video_filepath: str,
                    video_id: str | None = None) -> str | None:
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
    # drop the bare `startswith(stem)` fallback — it used to
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


def get_segments(video_id: str | None = None, jsonl_path: str | None = None,
                 title: str | None = None) -> list[dict[str, Any]]:
    """Return ordered segments for a video.

    Uses `_reader_open()` (lock-free reader connection) so the user's
    Watch-view click doesn't queue behind sweep_new_videos' FTS-ingest
    transactions during startup. Same migration list_videos_for_channel
    and the Recent tab already had — this path was missed, which made
    clicks during startup-sweep hang for many seconds at a time.
    SQLite WAL mode lets the reader connection read concurrently with
    the writer's ongoing transaction; falls back to _open() only if
    the reader connection somehow failed to initialize.
    """
    conn = _reader_open() or _open()
    if conn is None:
        return []
    # When only video_id is supplied (the Watch-view click path —
    # frontend doesn't know which jsonl_path), pick a canonical
    # jsonl_path for this video first: the one with the
    # most-recently-ingested rows (highest MAX(id)). Without this, a
    # video that's been ingested under more than one jsonl_path (e.g.,
    # a combined `.Channel Transcript.txt` AND a year-split
    # `.Channel 2024 Transcript.txt` both containing the same video)
    # would surface duplicated segments — every line shown twice in
    # the transcript pane, two karaoke cues stacked at the same
    # playback time. The dedup is silent; if the user has dupe data
    # we still play the most-recent ingest (which is what they'd
    # naturally expect after a retranscribe).
    # Track whether we got jsonl_path from the canon lookup vs the
    # caller. Canon paths came straight out of the DB and need NO
    # normalization — re-applying os.path.normpath on a stored path
    # that happens to mix forward/back slashes (the legacy ingest
    # path produced this for most pre-rebuild rows) silently rewrites
    # the input and breaks the literal `jsonl_path = ?` match, which
    # made `get_segments` return zero rows and the watch view fall
    # back to its hardcoded placeholder transcript.
    # Acquire reader lock around both queries so two Watch-view clicks
    # (or one click while list_videos_for_channel is running) don't
    # race on the same _reader_conn cursor. sqlite3.Connection isn't
    # safe for concurrent execute on a single connection — without
    # the lock this would raise "Recursive use of cursors not allowed"
    # or return wrong segment results.
    with _reader_lock:
        _jp_from_canon = False
        if video_id and not jsonl_path:
            try:
                canon = conn.execute(
                    "SELECT jsonl_path FROM segments WHERE video_id=? "
                    "GROUP BY jsonl_path ORDER BY MAX(id) DESC LIMIT 1",
                    (video_id,)
                ).fetchone()
                if canon and canon[0]:
                    jsonl_path = canon[0]
                    _jp_from_canon = True
            except Exception as e:
                _log.debug("swallowed: %s", e)
        where = []
        args: list[Any] = []
        if video_id:
            where.append("video_id=?"); args.append(video_id)
        if jsonl_path:
            where.append("jsonl_path=?")
            args.append(jsonl_path if _jp_from_canon
                        else os.path.normpath(jsonl_path))
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
        cur = conn.execute(q, args)
        _rows = cur.fetchall()
    out = []
    import json as _j
    for s, e, t, w in _rows:
        try:
            words = _j.loads(w) if w else []
        except (json.JSONDecodeError, ValueError):
            words = []
        out.append({"s": s, "e": e, "t": t, "w": words})
    return out


def get_segment_context(segment_id: int, before: int = 30,
                        after: int = 30,
                        query: str = "") -> dict[str, Any]:
    """Return N segments before + hit + N segments after for a search hit.

    `segment_id` is the rowid of the hit in the segments table. Returns:
      { ok, title, channel, video_id, jsonl_path,
        segments: [{id, s, e, t, is_hit}, ...],
        before_more: bool, after_more: bool }

    Used by the Search viewer pane (YTArchiver.py:29598) to show
    ~60 segments of surrounding transcript, with the hit highlighted.
    """
    # `get_segment_context` is a hot path — clicking any search result
    # in Browse > Search hits this. Use the reader connection so a
    # running sweep / ingest can't make the search-viewer pane freeze.
    conn = _reader_open()
    if conn is None:
        return {"ok": False, "error": "DB unavailable"}
    try:
        with _reader_lock:
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
            segments: list[dict[str, Any]] = []
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


# Patch 17 (v71.9): search functions extracted to index_search.py.
# Re-imported here so existing callers (api_mixins.browse_mixin, etc.)
# continue to resolve the names against the index module's namespace.
from .index_search import (  # noqa: F401
    _sanitize_fts_query,
    search_fts,
    search_video_titles,
)

# Patch 17 (v71.9): graph functions extracted to index_graph.py.
from .index_graph import (  # noqa: F401
    backfill_upload_ts,
    bucket_totals,
    graph_channel_overlay,
    graph_multi,
    graph_word_frequency,
    graph_word_frequency_multi,
    list_all_channels_in_db,
    top_words,
)


# ── Stats ───────────────────────────────────────────────────────────────

def summary() -> dict[str, Any]:
    # Read-only stats — use the reader connection so a long-running
    # sweep / ingest doesn't block these basic counts.
    conn = _reader_open()
    if conn is None:
        return {"segments": 0, "videos": 0, "channels": 0, "bookmarks": 0}
    with _reader_lock:
        seg = conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
        vid = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
        ch = conn.execute("SELECT COUNT(DISTINCT channel) FROM videos").fetchone()[0]
        bm = conn.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0]
    return {"segments": seg, "videos": vid, "channels": ch, "bookmarks": bm}



# Patch 20 (v72.2): bookmarks + sweep/prune/rebuild extracted.
from .index_bookmarks import (  # noqa: F401
    bookmark_add,
    bookmark_list,
    bookmark_remove,
    bookmark_update_note,
)
from .index_maintenance import (  # noqa: F401
    prune_missing_videos,
    rebuild_fts_index,
    sweep_new_videos,
)
