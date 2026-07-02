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

import atexit
import json
import os
import re
import sqlite3
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
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
_tx_retry_thread: threading.Thread | None = None


def _invalidate_top_words_cache() -> None:
    try:
        from . import index_graph as _graph
        _graph.invalidate_top_words_cache()
    except Exception as e:
        _log.debug("top_words cache invalidation failed: %s", e)


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
            _reader_conn.execute("PRAGMA busy_timeout=30000")
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
        c.execute("PRAGMA busy_timeout=30000")
        return c
    except Exception:
        return None


def _shutdown_index() -> None:
    """atexit hook: checkpoint + close the long-lived connections on a clean
    exit so the -wal file is truncated (it can otherwise grow unbounded on Z:
    between SQLite's opportunistic auto-checkpoints) and the query planner's
    stats are refreshed. Best-effort; never raises (audit: index.py shutdown
    checkpoint). NOTE: the window 'nuclear' TerminateProcess quit path bypasses
    atexit, so this covers clean exits only."""
    global _conn, _reader_conn
    with _db_lock:
        if _conn is not None:
            try:
                _conn.execute("PRAGMA optimize")
                # PASSIVE (not TRUNCATE): non-blocking. TRUNCATE waits for all
                # other readers/writers and could busy-stall up to busy_timeout
                # (30s) at shutdown if an independent-connection writer is still
                # active (audit r2). PASSIVE merges what it can without stalling;
                # normal-operation auto-checkpoint bounds -wal growth.
                _conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            except Exception as e:
                _log.debug("swallowed: %s", e)
            try:
                _conn.close()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            _conn = None
    with _reader_lock:
        if _reader_conn is not None:
            try:
                _reader_conn.close()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            _reader_conn = None


atexit.register(_shutdown_index)


# ── DB open / schema ────────────────────────────────────────────────────

def _open() -> sqlite3.Connection | None:
    """Open or return the cached connection. Returns None if DB can't be opened."""
    global _conn
    with _db_lock:
        if _conn is not None:
            return _conn
        try:
            TRANSCRIPTION_DB.parent.mkdir(parents=True, exist_ok=True)
            _conn = sqlite3.connect(str(TRANSCRIPTION_DB), check_same_thread=False, timeout=30.0)
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
            # Wait up to 30s for a competing writer's lock instead of failing
            # fast at the old 10s connect timeout — consistent with the reader/
            # independent connections and punct_restore/repair_captions on the
            # same DB (audit: index.py busy_timeout).
            _conn.execute("PRAGMA busy_timeout=30000")
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
            _conn.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS videos_fts USING fts5(
                title,
                content=videos,
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
                added_ts REAL,
                id_backfill_fail_count INTEGER DEFAULT 0,
                id_backfill_excluded_ts REAL
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
            _SCHEMA_VERSION = 2
            _MIGRATIONS: dict = {
                # Populate videos_fts from the existing videos table so
                # title search can use FTS5 MATCH instead of LIKE '%term%'.
                2: lambda c: c.execute(
                    "INSERT INTO videos_fts(videos_fts) VALUES('rebuild')"
                ),
            }
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
                "ALTER TABLE videos ADD COLUMN id_backfill_fail_count INTEGER DEFAULT 0",
                "ALTER TABLE videos ADD COLUMN id_backfill_excluded_ts REAL",
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
                # View / like counts, materialized from the per-channel
                # Metadata.jsonl sidecars so the global "Videos" view can
                # sort the whole archive by views/likes with an indexed
                # query (instead of walking ~100 sidecar files per load).
                # Written by the metadata refresh pass + a one-time
                # backfill (backfill_video_stats). NULL = not yet known.
                "ALTER TABLE videos ADD COLUMN view_count INTEGER",
                "ALTER TABLE videos ADD COLUMN like_count INTEGER",
            ):
                try:
                    _conn.execute(stmt)
                except sqlite3.OperationalError as exc:
                    if "duplicate column name" not in str(exc).lower():
                        _log.warning("schema ALTER failed: %s; stmt=%s",
                                     exc, stmt)
                        raise
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
                # Global "Videos" view sorts: keep the whole-archive
                # ORDER BY ... LIMIT/OFFSET pagination off a full table scan.
                "CREATE INDEX IF NOT EXISTS idx_vid_added_ts ON videos(added_ts)",
                "CREATE INDEX IF NOT EXISTS idx_vid_upload_ts ON videos(upload_ts)",
                # Covering index for the search year-filter's (channel, title)
                # upload-date fallback: segments with an empty video_id resolve
                # their year via MIN(upload_ts) grouped by (channel, title).
                # The 3-col covering index lets that run index-only.
                "CREATE INDEX IF NOT EXISTS idx_vid_chan_title "
                "ON videos(channel, title, upload_ts)",
                "CREATE INDEX IF NOT EXISTS idx_vid_view_count ON videos(view_count)",
                "CREATE INDEX IF NOT EXISTS idx_vid_like_count ON videos(like_count)",
                # PARTIAL COVERING index for the Browse Channels grid's
                # per-channel "last added" query
                #   SELECT channel, MAX(added_ts) FROM videos
                #   WHERE is_duplicate_of IS NULL GROUP BY channel
                # Measured at 689s (!) on a 104k-row / 20GB DB before this:
                # the planner used idx_vid_ch_yr_mo for the GROUP BY but had
                # to random-fetch added_ts + is_duplicate_of from the big
                # table for every row (104k cold lookups). This index carries
                # both grouping cols, is filtered to live rows, and is
                # covering — so the query runs index-only in ~0.01s. Also
                # serves list_all_videos' "recent"/"newest" first-page sorts
                # (they group/scan the same live-rows-by-channel/added_ts set).
                "CREATE INDEX IF NOT EXISTS idx_vid_chan_added_live "
                "ON videos(channel, added_ts) WHERE is_duplicate_of IS NULL",
                # Per-channel Browse paging. These keep the default newest/
                # oldest and most-viewed page loads from scanning/sorting a
                # whole large channel before the first cards can paint.
                "CREATE INDEX IF NOT EXISTS idx_vid_chan_upload_page_live "
                "ON videos(channel COLLATE NOCASE, upload_ts, added_ts) "
                "WHERE is_duplicate_of IS NULL",
                "CREATE INDEX IF NOT EXISTS idx_vid_chan_view_page_live "
                "ON videos(channel COLLATE NOCASE, view_count, added_ts) "
                "WHERE is_duplicate_of IS NULL",
            ):
                try:
                    _conn.execute(stmt)
                except sqlite3.OperationalError as exc:
                    _log.warning("schema index creation failed: %s; stmt=%s",
                                 exc, stmt)
                    raise
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
                _log.error("schema migration step failed; user_version "
                           "left at %s: %s", _current_v, e)
                raise
            _conn.commit()
            # REMOVED: `PRAGMA quick_check` used to run here. On a large
            # archive it reads the ENTIRE database file (20GB / 9M+
            # segments) — minutes of cold disk I/O — and it ran inside this
            # `_db_lock`-held connection-open on the FIRST _open() of every
            # startup. That blocked every other _open()/_reader_open()
            # (Browse, sweep, sync writes, watch-view transcript loads)
            # behind it for the whole scan — the true cause of the
            # multi-minute "Loading channels…" hang that scaled with DB
            # size. Integrity verification does not belong in the hot
            # connection-opener; if we want it back it must be opt-in /
            # rare and run on a dedicated connection off the _db_lock.
            # Mark schema-ready so future _reader_open() calls can
            # skip the _db_lock-acquiring _open() call entirely.
            global _schema_inited
            _schema_inited = True
            return _conn
        except sqlite3.Error as e:
            _log.error("Could not open DB %s: %s", TRANSCRIPTION_DB, e)
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
    # parts[:-1]: NEVER consider the file's basename — titles starting
    # with a month word ('May Day Parade …') matched the month check
    # at depth 0 and blocked the real month FOLDER from ever being
    # read, mis-bucketing the video in Browse/Graph/search filters.
    # start=1 keeps the dir-depth numbering identical to before.
    for depth, p in enumerate(reversed(parts[:-1]), start=1):
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


# ── Content-based video-id recovery from .info.json sidecars ─────────────
# yt-dlp writes a `<name>.info.json` next to every download whose JSON
# contains the real `id` AND the actual output filename it used (`_filename`).
# The old recovery matched the sidecar to the video by *filename stem*, which
# breaks whenever yt-dlp sanitized/trimmed the .mp4 and the .info.json names
# differently (titles with punctuation — SNL etc.). This resolves the id by
# reading the JSON *content* instead: match on the recorded output filename,
# falling back to a normalized-title match. The id is always inside the JSON,
# so it's recoverable regardless of what the sidecar file is named.
_VIDID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
_YT_ID_IN_TEXT_RE = re.compile(
    r"(?:youtube\.com/(?:watch\?(?:[^#\s]*&)?v=|shorts/|embed/)"
    r"|youtu\.be/)([A-Za-z0-9_-]{11})"
)
_TITLE_DATE_SUFFIX_RE = re.compile(
    r"\s*[\(\[]\s*\d{1,2}[.-]\d{1,2}[.-]\d{2,4}\s*[\)\]]\s*$"
)
_SIDECAR_ID_CACHE_MAX = 1000
_sidecar_id_cache: OrderedDict[str, tuple[float, dict]] = OrderedDict()
_sidecar_id_cache_lock = threading.Lock()


def _alnum_key(s: str) -> str:
    """Aggressive normalize for title<->stem matching: lowercase, alnum-only.
    Collapses the punctuation/whitespace/sanitization differences between a
    raw title and a filesystem-sanitized stem."""
    return "".join(c for c in (s or "").lower() if c.isalnum())


def _transcript_identity_title_keys(title: str, norm_fn) -> set[str]:
    """Title keys for strict transcript matching.

    Manual/single downloads often carry a filename date suffix like
    `Title (05.29.26)` while imported transcript rows retain the YouTube title
    `Title`. Treat that one suffix as non-identifying, but keep the normal
    exact normalized key so genuinely different titles still get rejected.
    """
    raw = title or ""
    keys = {norm_fn(raw)}
    stripped = _TITLE_DATE_SUFFIX_RE.sub("", raw).strip()
    if stripped != raw:
        keys.add(norm_fn(stripped))
    return {k for k in keys if k}


def _build_sidecar_id_map(dir_path: str) -> dict:
    """Map a directory's .info.json sidecars to ids."""
    by_name: dict[str, str] = {}
    by_stem: dict[str, str] = {}
    by_title: dict[str, str] = {}
    try:
        names = os.listdir(dir_path)
    except OSError:
        return {"by_name": by_name, "by_stem": by_stem, "by_title": by_title}
    text_sidecar_exts = (
        ".url", ".webloc", ".website", ".txt", ".nfo", ".html", ".htm",
    )

    def _id_from_text(text: str) -> str:
        if not text:
            return ""
        m = _YT_ID_IN_TEXT_RE.search(text)
        if m and _VIDID_RE.fullmatch(m.group(1)):
            return m.group(1)
        m = re.search(r"(?:^|[?&\s])v=([A-Za-z0-9_-]{11})(?:[&#\s]|$)", text)
        if m and _VIDID_RE.fullmatch(m.group(1)):
            return m.group(1)
        return ""

    for fn in names:
        low_fn = fn.lower()
        if not low_fn.endswith(".info.json") and not low_fn.endswith(text_sidecar_exts):
            continue
        full_path = os.path.join(dir_path, fn)
        sidecar_stem = (
            fn[:-len(".info.json")].lower()
            if low_fn.endswith(".info.json")
            else os.path.splitext(fn)[0].lower()
        )
        if not low_fn.endswith(".info.json"):
            try:
                with open(full_path, "r", encoding="utf-8",
                          errors="replace") as f:
                    vid = _id_from_text(f.read(256 * 1024))
            except Exception:
                vid = ""
            if _VIDID_RE.fullmatch(vid or "") and sidecar_stem:
                by_stem.setdefault(sidecar_stem, vid)
            continue
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        vid = str(data.get("id") or "").strip()
        if not _VIDID_RE.fullmatch(vid):
            for key in ("webpage_url", "original_url", "url"):
                vid = _id_from_text(str(data.get(key) or ""))
                if _VIDID_RE.fullmatch(vid):
                    break
        if not _VIDID_RE.fullmatch(vid):
            continue
        if sidecar_stem:
            by_stem.setdefault(sidecar_stem, vid)
        # The exact output filename yt-dlp used (authoritative).
        rec = data.get("_filename") or data.get("filename") or ""
        if not rec:
            rd = data.get("requested_downloads")
            if isinstance(rd, list) and rd and isinstance(rd[0], dict):
                rec = rd[0].get("filepath") or rd[0].get("_filename") or ""
        if rec:
            by_name.setdefault(os.path.basename(str(rec)).lower(), vid)
        tkey = _alnum_key(str(data.get("title") or ""))
        if tkey:
            by_title.setdefault(tkey, vid)
    return {"by_name": by_name, "by_stem": by_stem, "by_title": by_title}


def _resolve_id_from_sidecars(filepath: str) -> str:
    """Recover a video's YouTube id from the .info.json sidecars in its
    folder, matched by JSON *content* (recorded filename, then title).
    Returns "" if nothing matches. Per-directory cached (dir mtime keyed)."""
    try:
        d = os.path.dirname(filepath)
        if not d:
            return ""
        try:
            mt = os.path.getmtime(d)
        except OSError:
            mt = 0.0
        with _sidecar_id_cache_lock:
            cached = _sidecar_id_cache.get(d)
            if cached is None or cached[0] != mt:
                m = _build_sidecar_id_map(d)
                _sidecar_id_cache[d] = (mt, m)
                _sidecar_id_cache.move_to_end(d)
                # Cap the cache so a full-archive backfill doesn't grow it
                # without bound. LRU keeps hot dirs alive during wide scans.
                while len(_sidecar_id_cache) > _SIDECAR_ID_CACHE_MAX:
                    _sidecar_id_cache.popitem(last=False)
            else:
                _sidecar_id_cache.move_to_end(d)
                m = cached[1]
        base = os.path.basename(filepath).lower()
        if base in m["by_name"]:
            return m["by_name"][base]
        raw_stem = os.path.splitext(os.path.basename(filepath))[0].lower()
        if raw_stem and raw_stem in m.get("by_stem", {}):
            return m["by_stem"][raw_stem]
        stem_key = _alnum_key(raw_stem)
        if stem_key:
            if stem_key in m["by_title"]:
                return m["by_title"][stem_key]
            # Trim-tolerant: yt-dlp's --trim-filenames may have truncated the
            # stem, so a sidecar title that STARTS WITH the stem is a match
            # (require a reasonable length to avoid false positives).
            if len(stem_key) >= 16:
                for tkey, vid in m["by_title"].items():
                    if tkey.startswith(stem_key):
                        return vid
    except Exception as e:
        _log.debug("sidecar id resolve failed (%s): %s", filepath, e)
    return ""


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
    # Explicit video_id wins; else try the filename. Sidecar recovery is kept
    # in the cached resolver below so JSON is read through one path.
    from .text_utils import extract_video_id as _extract_vid
    vid_id = _extract_vid(
        fp,
        hint=(video_id or "").strip(),
        reject_alpha_only=True,
        info_json_fallback=False,
    ) or None
    # Content-based recovery: direct sibling sidecar, recorded output filename,
    # then title match, all from one per-directory cached sidecar scan.
    if not vid_id:
        _sc_vid = _resolve_id_from_sidecars(fp)
        if _sc_vid:
            vid_id = _sc_vid
    # Direct sibling sidecars are covered by _resolve_id_from_sidecars.
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
            # Capture the pre-upsert FTS state. videos_fts is an external-
            # content FTS5 table: a NEW row has nothing to delete (issuing the
            # FTS5 'delete' for a never-indexed rowid raises DatabaseError
            # "malformed"), and an UPDATE must delete using the row's OLD
            # title, not the post-upsert one. The previous code always deleted
            # with the new title — which threw on every freshly-downloaded
            # video and, because the surrounding except only caught
            # OperationalError, aborted the ENTIRE registration. Result: new
            # downloads silently never landed in the index (register returned
            # False with no error shown).
            _fts_old = conn.execute(
                "SELECT id, title FROM videos WHERE filepath=? COLLATE NOCASE",
                (fp,)).fetchone()
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
                     /* NEVER overwrite a known id with NULL. The disk sweep
                        re-registers existing files WITHOUT an id; the old
                        `video_id=excluded.video_id` then wiped a good id
                        captured at download time. COALESCE keeps the
                        existing id when the re-register didn't resolve one. */
                     video_id=COALESCE(excluded.video_id, videos.video_id),
                     video_url=COALESCE(excluded.video_url, videos.video_url),
                     id_backfill_fail_count=CASE
                       WHEN excluded.video_id IS NOT NULL THEN 0
                       ELSE videos.id_backfill_fail_count
                     END,
                     id_backfill_excluded_ts=CASE
                       WHEN excluded.video_id IS NOT NULL THEN NULL
                       ELSE videos.id_backfill_excluded_ts
                     END,
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
            # Keep videos_fts in sync (T257). NEW row → insert only; UPDATE →
            # delete the OLD entry (with its OLD title) then insert the new.
            # Catch BOTH OperationalError (pre-migration DB without the FTS
            # table) AND DatabaseError ("malformed" from a stray delete) so an
            # FTS hiccup can never abort the registration of the video itself.
            try:
                _fts_new = conn.execute(
                    "SELECT id, title FROM videos WHERE filepath=? COLLATE NOCASE",
                    (fp,)
                ).fetchone()
                if _fts_new:
                    _fts_id, _fts_title = _fts_new
                    if _fts_old is not None:
                        _old_id, _old_title = _fts_old
                        conn.execute(
                            "INSERT INTO videos_fts(videos_fts, rowid, title)"
                            " VALUES('delete', ?, ?)",
                            (_old_id, _old_title or "")
                        )
                    conn.execute(
                        "INSERT INTO videos_fts(rowid, title) VALUES(?, ?)",
                        (_fts_id, _fts_title or "")
                    )
            except (sqlite3.OperationalError, sqlite3.DatabaseError) as _fe:
                _log.debug("videos_fts sync skipped: %s", _fe)
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
                    _log.warning(
                        "register_video DB busy, retry %d/%d: %s",
                        _attempt + 1, _attempts, _oe)
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
            _log.warning("register_video sidecar ingest failed for %s: %s",
                         fp, e)
        return True
    except sqlite3.Error as e:
        _log.error("register_video failed for %s: %s", filepath, e)
        return False


def mark_video_transcribed(filepath: str,
                            _conn_override: sqlite3.Connection | None = None) -> bool:
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
    except sqlite3.Error as exc:
        _log.warning("mark_video_transcribed failed for %r: %s",
                     filepath, exc)
        return False


def mark_video_no_speech(filepath: str,
                          _conn_override: sqlite3.Connection | None = None) -> bool:
    """Flip the tx_status flag for `filepath` to 'no_speech'.

    Used when Whisper ran successfully but produced an EMPTY transcript
    (silent / music-only video). 'no_speech' is a TERMINAL state, distinct
    from 'transcribed' (has a transcript) and 'pending' (still needs an
    attempt): the video has been checked, there is nothing to transcribe,
    and it must NOT be auto-re-queued. Same connection-override pattern as
    mark_video_transcribed.
    """
    fp = os.path.normpath(filepath)
    use_override = _conn_override is not None
    try:
        if use_override:
            conn = _conn_override
            conn.execute("UPDATE videos SET tx_status='no_speech' "
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
                conn.execute("UPDATE videos SET tx_status='no_speech' "
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


def delete_segments_for_video(filepath: str) -> int:
    """FTS-safe removal of every transcript segment tied to a video FILE.

    Used by the Recent/Browse delete-file endpoints. Two things the old
    inline cleanups got wrong: (1) they deleted by the per-video
    "<stem>.jsonl" path, which matches ZERO rows in the aggregated
    layout (segments.jsonl_path holds the per-folder aggregated file),
    so deleted videos stayed searchable; (2) they skipped the
    external-content FTS5 'delete' insert, leaving stale rowids in the
    FTS shadow that map old text onto recycled rows later. This helper
    deletes by video_id (idx_seg_video_id) with the proper FTS sync,
    plus a legacy per-video-path cleanup for pre-aggregation rows.
    Acquires _db_lock itself — call OUTSIDE any existing _db_lock block.
    Returns the number of segment rows removed."""
    _fp = os.path.normpath(filepath or "")
    if not _fp:
        return 0
    _legacy_jsonl = os.path.splitext(_fp)[0] + ".jsonl"
    removed = 0
    try:
        with _db_lock:
            conn = _open()
            if conn is None:
                return 0
            row = conn.execute(
                "SELECT video_id FROM videos WHERE filepath=? COLLATE NOCASE",
                (_fp,)).fetchone()
            vid = (row[0] or "") if row else ""
            if vid:
                conn.execute(
                    "INSERT INTO segments_fts(segments_fts, rowid, text) "
                    "SELECT 'delete', id, text FROM segments "
                    "WHERE video_id=?", (vid,))
                cur = conn.execute(
                    "DELETE FROM segments WHERE video_id=?", (vid,))
                removed += cur.rowcount
            conn.execute(
                "INSERT INTO segments_fts(segments_fts, rowid, text) "
                "SELECT 'delete', id, text FROM segments "
                "WHERE jsonl_path=? COLLATE NOCASE", (_legacy_jsonl,))
            cur = conn.execute(
                "DELETE FROM segments WHERE jsonl_path=? COLLATE NOCASE",
                (_legacy_jsonl,))
            removed += cur.rowcount
            conn.commit()
        if removed:
            _invalidate_top_words_cache()
    except sqlite3.Error as e:
        _log.warning("delete_segments_for_video failed for %s: %s",
                     filepath, e)
    return removed


def delete_channel_from_index(channel: str) -> dict[str, int]:
    """Remove an ENTIRE channel from the index: every `videos` row plus all
    of its transcript segments (FTS-safe).

    Called when a channel is removed via the UI WITH delete_files=True. The
    Browse / Search / Videos views read the index DB, not the disk — so
    without this the deleted channel's cards keep showing and 404 ("File not
    found — index entry may be stale") when clicked. Mirrors
    delete_segments_for_video's FTS sync, scoped to the channel: push a
    'delete' into segments_fts for every row before dropping it so the FTS
    shadow doesn't strand stale text on recycled rowids. Order matters —
    segments are deleted BEFORE the videos rows the subquery depends on.
    Acquires _db_lock — call OUTSIDE any existing _db_lock block.
    Returns {videos, segments} removed counts."""
    out = {"videos": 0, "segments": 0}
    if not channel:
        return out
    _sub = ("video_id IN (SELECT video_id FROM videos "
            "WHERE channel=? COLLATE NOCASE AND video_id IS NOT NULL)")
    try:
        with _db_lock:
            conn = _open()
            if conn is None:
                return out
            # FTS-safe removal of this channel's segments first.
            conn.execute(
                "INSERT INTO segments_fts(segments_fts, rowid, text) "
                "SELECT 'delete', id, text FROM segments WHERE " + _sub,
                (channel,))
            cur = conn.execute(
                "DELETE FROM segments WHERE " + _sub, (channel,))
            out["segments"] = cur.rowcount or 0
            # Then the video rows for the channel.
            cur = conn.execute(
                "DELETE FROM videos WHERE channel=? COLLATE NOCASE",
                (channel,))
            out["videos"] = cur.rowcount or 0
            conn.commit()
        try:
            invalidate_channel_videos(channel)
        except Exception as e:
            _log.debug("swallowed: %s", e)
    except sqlite3.Error as e:
        _log.debug("delete_channel_from_index(%s) failed: %s", channel, e)
    return out


def update_video_path(old_path: str, new_path: str) -> bool:
    """Re-point a video's row at its new on-disk location after a move.

    reorg physically moves the .mp4 into a different year/month folder but
    the catalog's `videos.filepath` would otherwise still point at the OLD
    location, so Watch playback + the Browse grid show "File not found" for
    relocated videos (the transcript still loads — segments are keyed by
    video_id, not path). This rewrites JUST the filepath old→new; video_id /
    transcript / segments are untouched. Returns True if a row was updated.
    """
    _old = os.path.normpath(old_path)
    _new = os.path.normpath(new_path)
    if _old == _new:
        return False
    try:
        with _db_lock:
            conn = _open()
            if conn is None:
                return False
            cur = conn.execute(
                "UPDATE videos SET filepath=? WHERE filepath=? COLLATE NOCASE",
                (_new, _old))
            row = conn.execute(
                "SELECT channel, video_id FROM videos "
                "WHERE filepath=? COLLATE NOCASE",
                (_new,)).fetchone()
            # Re-point transcript sidecar references in the SAME
            # transaction. reorg moves the per-video .jsonl alongside
            # the .mp4; leaving segments.jsonl_path / indexed_files.path
            # at the OLD location made the next boot sweep treat the
            # moved .jsonl as un-indexed and re-ingest it — duplicating
            # every segment (and FTS hit) for the video. Both sidecar
            # name shapes are covered (legacy "stem.jsonl" and hidden
            # ".stem.jsonl"); aggregated channel/year jsonls never equal
            # these exact paths, so they are untouched.
            _vid = (row[1] or "") if (row and len(row) > 1) else ""
            _od, _ob = os.path.split(os.path.splitext(_old)[0])
            _nd, _nb = os.path.split(os.path.splitext(_new)[0])
            for _oj, _nj in (
                    (os.path.join(_od, _ob + ".jsonl"),
                     os.path.join(_nd, _nb + ".jsonl")),
                    (os.path.join(_od, "." + _ob + ".jsonl"),
                     os.path.join(_nd, "." + _nb + ".jsonl"))):
                if _vid:
                    # video_id scope lets the planner use
                    # idx_seg_video_id while keeping the path compare
                    # NOCASE-robust.
                    conn.execute(
                        "UPDATE segments SET jsonl_path=? "
                        "WHERE video_id=? AND jsonl_path=? COLLATE NOCASE",
                        (_nj, _vid, _oj))
                else:
                    # No video_id: exact match keeps idx_seg_jsonl
                    # usable (NOCASE here would force a full-table
                    # scan per moved file).
                    conn.execute(
                        "UPDATE segments SET jsonl_path=? "
                        "WHERE jsonl_path=?",
                        (_nj, _oj))
                conn.execute(
                    "UPDATE indexed_files SET path=? "
                    "WHERE path=? COLLATE NOCASE",
                    (_nj, _oj))
            conn.commit()
        if row and row[0]:
            try: invalidate_channel_videos(row[0])
            except Exception as e: _log.debug("swallowed: %s", e)
        return cur.rowcount > 0
    except sqlite3.Error:
        return False


# ── Transcript ingest (from .jsonl sidecar) ─────────────────────────────

def ingest_jsonl(video_filepath: str, jsonl_path: str,
                 title: str, channel: str,
                 _conn_override: sqlite3.Connection | None = None,
                 force: bool = False) -> int:
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
    try:
        jsonl_mtime = os.path.getmtime(jp)
    except OSError:
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
            if not force:
                _idx_row = conn.execute(
                    "SELECT mtime, segment_count FROM indexed_files "
                    "WHERE path=?", (jp,)).fetchone()
                if _idx_row is not None:
                    try:
                        _idx_mtime = float(_idx_row[0] or 0)
                        _idx_count = int(_idx_row[1] or 0)
                    except (TypeError, ValueError):
                        _idx_mtime = -1
                        _idx_count = -1
                    _actual_count = conn.execute(
                        "SELECT COUNT(*) FROM segments WHERE jsonl_path=?",
                        (jp,)).fetchone()[0]
                    if (_idx_mtime == jsonl_mtime
                            and _idx_count == int(_actual_count or 0)):
                        conn.execute(
                            "UPDATE videos SET tx_status='transcribed' "
                            "WHERE filepath=? COLLATE NOCASE", (fp,))
                        conn.commit()
                        return _idx_count
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
            conn.executemany(
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
                (jp, jsonl_mtime, len(rows)),
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
            if rows:
                _invalidate_top_words_cache()
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
        _log.error("ingest_jsonl failed for %s: %s", jsonl_path, e)
        return 0


# ── Reads ───────────────────────────────────────────────────────────────

def list_recent_videos(limit: int = 200, channel: str | None = None
                       ) -> list[dict[str, Any]]:
    """Return the N newest videos (by added_ts), optionally filtered by channel.

    Uses `_reader_conn` (read-only, separate from `_conn`) so the Recent tab
    never waits on sync's `register_video` write lock. WAL handles the
    cross-connection visibility.
    """
    conn = _reader_open()
    lock = _reader_lock
    if conn is None:
        conn = _open()
        lock = _db_lock
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
    with lock:
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
_BROWSE_VIDEOS_CACHE_MAX = 30
_ALL_VIDEOS_CACHE_MAX = 20
_THUMB_INDEX_CACHE_MAX = 60
_browse_videos_cache: OrderedDict[
    tuple[str, str, int, bool], list[dict[str, Any]]
] = OrderedDict()
_browse_cache_lock = threading.Lock()

# Global "Videos" (all-videos) view cache. list_all_videos runs its own
# cross-channel query + per-row thumbnail resolution — and that thumbnail
# pass is the ~10s cost on a cold open. Unlike the per-channel grids
# (cached above), the Videos view had no result cache, so it re-paid that
# cost every open even after the Browse preload finished. Cache the
# fully-resolved page (incl. thumbnail URLs) keyed by
# (sort, limit, offset, include_thumbs) so re-opening is instant and
# survives OS file-cache eviction. Cleared wholesale by
# invalidate_channel_videos (any video add/delete/re-transcribe in any
# channel can change the global list). Shares _browse_cache_lock.
_all_videos_cache: OrderedDict[
    tuple[str, int, int, bool, str], dict[str, Any]
] = OrderedDict()

# Per-channel thumbnail index cache. Keyed by channel-root path, value
# is {"mtime": float, "thumbs": {vid_id: thumb_path}}. The audit
# previously required a full os.walk of every Browse first-click; with
# this cache, a stale entry is detected via the channel-root mtime
# (Windows bumps a dir's mtime when entries change) and a re-walk only
# happens when the directory was actually touched.
_thumb_index_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
_thumb_index_cache_lock = threading.Lock()


class _LowPriorityInterrupted(Exception):
    """Raised when startup preload should yield to user-visible work."""


# ── Foreground-browse guard ────────────────────────────────────────────
# The startup sweep + Browse preload both walk the (slow, ~16 MB/s) Z:
# DrivePool to resolve thumbnails. Before this guard there was NO signal
# telling them that the USER is actively loading a Browse view, so a
# cold "Videos" open (up to 60 channel-wide thumbnail walks) ran head-to-
# head with the preload walking the same disk — turning a ~10s open into
# minutes. The fix: the API entry points that serve the Browse tab
# (list_all_videos, list_videos_for_channel) bump this counter for the
# duration of the user's query via `foreground_browse()`, and the
# startup low-priority gate treats a non-zero count as "busy" so sweep +
# preload park and hand the disk to the user. Preload's OWN internal
# calls go straight to the index functions (not the API mixin), so they
# never trip this — no self-deadlock.
_fg_browse_lock = threading.Lock()
_fg_browse_count = 0


def is_foreground_browse_busy() -> bool:
    """True while at least one user-initiated Browse query is in flight."""
    with _fg_browse_lock:
        return _fg_browse_count > 0


@contextmanager
def foreground_browse():
    """Mark a user-initiated Browse query in flight so startup sweep +
    preload yield the Z: pool to it. Re-entrant (counter, not flag)."""
    global _fg_browse_count
    with _fg_browse_lock:
        _fg_browse_count += 1
    try:
        yield
    finally:
        with _fg_browse_lock:
            if _fg_browse_count > 0:
                _fg_browse_count -= 1


def _low_priority_busy(busy_fn: Any | None) -> bool:
    if not callable(busy_fn):
        return False
    try:
        return bool(busy_fn())
    except Exception:
        return False


def _raise_if_low_priority_busy(busy_fn: Any | None) -> None:
    if _low_priority_busy(busy_fn):
        raise _LowPriorityInterrupted()


def _lru_put(cache: OrderedDict, key: Any, value: Any, max_entries: int) -> None:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > max_entries:
        cache.popitem(last=False)


def _browse_videos_cache_put(
        key: tuple[str, str, int, bool],
        rows: list[dict[str, Any]]) -> None:
    _lru_put(_browse_videos_cache, key, rows, _BROWSE_VIDEOS_CACHE_MAX)


def _all_videos_cache_put(
        key: tuple[str, int, int, bool, str],
        page: dict[str, Any]) -> None:
    _lru_put(_all_videos_cache, key, page, _ALL_VIDEOS_CACHE_MAX)


def _thumb_index_cache_put(key: str, entry: dict[str, Any]) -> None:
    _lru_put(_thumb_index_cache, key, entry, _THUMB_INDEX_CACHE_MAX)


def _coerce_count(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _format_compact_count(value: Any) -> str:
    n = _coerce_count(value)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    if n > 0:
        return str(n)
    return ""


def invalidate_channel_videos(channel: str | None = None) -> None:
    """Drop cached video lists. `channel=None` clears everything."""
    with _browse_cache_lock:
        if channel is None:
            _browse_videos_cache.clear()
        else:
            for key in list(_browse_videos_cache.keys()):
                if key[0] == channel:
                    del _browse_videos_cache[key]
        # The global Videos view spans every channel, so any per-channel
        # change can shift it — always clear it wholesale (it's tiny: a
        # few pages of the default sort).
        _all_videos_cache.clear()
    # Also drop the channel-wide thumbnail index. Its freshness stamp is
    # the channel ROOT's mtime, but NTFS only bumps the IMMEDIATE parent
    # dir — new thumbs land in <root>/<year>/.Thumbnails/, so the stamp
    # never changes and the cache stayed permanently stale (gradient
    # placeholders for every new video until restart). We only have the
    # channel NAME here (the cache is keyed by folder path), so clear
    # wholesale — it rebuilds lazily per channel on next access.
    with _thumb_index_cache_lock:
        _thumb_index_cache.clear()


def preload_channel_videos(channel: str,
                           sort: str = "newest",
                           limit: int = 500,
                           low_priority_busy_fn: Any | None = None) -> int:
    """Warm `list_videos_for_channel` into the cache. Returns row count."""
    _raise_if_low_priority_busy(low_priority_busy_fn)
    rows = list_videos_for_channel(channel, sort=sort, limit=limit,
                                    include_thumbs=True,
                                    low_priority_busy_fn=low_priority_busy_fn)
    with _browse_cache_lock:
        _browse_videos_cache_put((channel, sort, limit, True), rows)
    return len(rows)


def preload_all_channels(channel_names: list[str],
                         progress_cb: Any | None = None,
                         cancel_ev: Any | None = None,
                         low_priority_busy_fn: Any | None = None,
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
    3. If sync or GPU is actively running, startup callers can provide
       low_priority_busy_fn; preload then waits between channels and
       interrupts uncached thumbnail walks instead of competing with
       download finalization.

    Returns {channel_name: row_count}.
    """
    import time as _t
    out: dict[str, int] = {}
    total = len(channel_names)

    def _wait_if_busy() -> bool:
        while _low_priority_busy(low_priority_busy_fn):
            if cancel_ev is not None and cancel_ev.is_set():
                return False
            _t.sleep(0.5)
        return True

    i = 0
    while i < total:
        ch = channel_names[i]
        if cancel_ev is not None and cancel_ev.is_set():
            break
        if not _wait_if_busy():
            break
        if progress_cb is not None:
            try: progress_cb(i + 1, total, ch)
            except Exception as e: _log.debug("swallowed: %s", e)
        try:
            out[ch] = preload_channel_videos(
                ch, sort=sort, limit=limit,
                low_priority_busy_fn=low_priority_busy_fn)
        except _LowPriorityInterrupted:
            # User-visible work started mid-channel. Do not cache a
            # partial preload; wait, then retry the same channel later.
            if not _wait_if_busy():
                break
            continue
        except Exception:
            out[ch] = 0
        i += 1
        # Politeness yield. Active sync/GPU = longer yield.
        try:
            from . import sync as _sync
            active = _sync.is_any_sync_active()
        except Exception:
            active = False
        _t.sleep(0.2 if active else 0.03)
    # Warm the global "Videos" view's first page now that every channel's
    # thumbnail data is hot. list_all_videos has its own result cache
    # (_all_videos_cache) but no warmer, so without this the first
    # Browse>Videos click still paid the cold ~10s per-row thumbnail walk.
    # Match videosView.js's default request exactly: sort="recent",
    # PAGE=60, offset=0, include_thumbs=True.
    if (not (cancel_ev is not None and cancel_ev.is_set())
            and _wait_if_busy()):
        try:
            list_all_videos("recent", 60, 0, include_thumbs=True)
        except Exception as e:
            _log.debug("preload all-videos first page failed: %s", e)
    return out


def list_videos_for_channel(channel: str, sort: str = "newest",
                            limit: int = 50000, include_thumbs: bool = True,
                            *,
                            low_priority_busy_fn: Any | None = None
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
            _browse_videos_cache.move_to_end(cache_key)
            # Return a shallow copy so callers can mutate without poisoning
            # the cache for the next reader.
            return list(hit)
        # Fallback: look for any cached entry with the same (channel,
        # sort, include_thumbs) but a larger limit — a 100k-limit
        # preload satisfies any smaller runtime request. Without
        # this, "preload every video" setting didn't deliver
        # the promised instant-click behavior because preload keyed
        # on 100_000 while the frontend requested 50_000.
        fallback_key = None
        fallback_rows = None
        for key, rows in _browse_videos_cache.items():
            c_ch, c_sort, c_lim, c_thumbs = key
            if (c_ch == channel and c_sort == sort
                    and c_thumbs == bool(include_thumbs)
                    and c_lim >= limit):
                fallback_key = key
                fallback_rows = rows
                break
        if fallback_key is not None and fallback_rows is not None:
            _browse_videos_cache.move_to_end(fallback_key)
            return (list(fallback_rows[:limit])
                    if len(fallback_rows) > limit else list(fallback_rows))
    # use the long-lived read-only connection
    # (`_reader_conn`, opened on first use below) so this Browse
    # query never contends on `_db_lock` with sync's register_video
    # calls. WAL mode handles cross-connection serialization for us.
    conn = _reader_open()
    if conn is None:
        return []
    _raise_if_low_priority_busy(low_priority_busy_fn)
    order = {
        "newest": "(upload_ts IS NULL) ASC, upload_ts DESC, COALESCE(added_ts, 0) DESC",
        "oldest": "(upload_ts IS NULL) ASC, upload_ts ASC, COALESCE(added_ts, 0) ASC",
        "largest": "COALESCE(size_bytes, 0) DESC",
        "title": "title COLLATE NOCASE ASC",
        # most_viewed MUST order in SQL: without this key the query ran
        # the default newest-first order, LIMIT truncated to the newest
        # N, and the Python view-count re-sort below only reshuffled
        # those — genuinely top-viewed older videos were absent
        # entirely on channels larger than the limit. The Python
        # re-sort stays as a tie-refiner from JSONL data.
        "most_viewed": "(view_count IS NULL) ASC, view_count DESC, "
                       "COALESCE(added_ts, 0) DESC",
    }.get(sort, "COALESCE(year, 0) DESC, COALESCE(month, 0) DESC, COALESCE(added_ts, 0) DESC")
    with _reader_lock:
        # Select removed_from_yt_ts last so older DBs (where the column
        # may not exist yet during the first run after upgrade) can be
        # handled via try/except fallback.
        try:
            cur = conn.execute(
                f"SELECT title, channel, filepath, video_id, size_bytes, year, month, "
                f"tx_status, added_ts, removed_from_yt_ts, upload_ts, "
                f"view_count, like_count FROM videos "
                f"WHERE channel=? COLLATE NOCASE AND is_duplicate_of IS NULL "
                f"ORDER BY {order} LIMIT ?",
                (channel, limit),
            )
            out = [{
                "title": r[0], "channel": r[1], "filepath": r[2], "video_id": r[3],
                "size_bytes": r[4] or 0, "year": r[5], "month": r[6],
                "tx_status": r[7], "added_ts": r[8],
                "removed_from_yt": bool(r[9]),
                "upload_ts": r[10],
                "view_count": r[11],
                "like_count": r[12],
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
    _raise_if_low_priority_busy(low_priority_busy_fn)
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
        _raise_if_low_priority_busy(low_priority_busy_fn)
        if folder_key in metadata_cache:
            return metadata_cache[folder_key]
        entries: dict[str, Any] = {}
        by_title: dict[str, Any] = {}
        _walk_failed = False
        try:
            # Walk up the folder tree looking for .{channel} ... Metadata.jsonl
            cur = folder_key
            for _ in range(4):
                _raise_if_low_priority_busy(low_priority_busy_fn)
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
        except _LowPriorityInterrupted:
            raise
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
    # Keep this index-only; per-row file probes make huge channels slow
    # to open on network-backed archives.
    out = [r for r in out if r.get("filepath")]

    # Channel-wide thumbnail index. `find_thumbnail` walks UP the path
    # at most 3 levels — it does NOT cross over to sibling year folders.
    # On channels where thumbnails ended up in a different year's
    # `.Thumbnails/` than where the mp4 currently lives (e.g.
    # The PrimeTime had most thumbs in 2025/.Thumbnails/ but the mp4s
    # got re-foldered into 2023/ and 2024/), every Browse video tile
    # rendered as the gradient placeholder. Pre-walk the channel root
    # once, build vid → path, and use it for vid-keyed lookup. Falls
    # back to find_thumbnail for the no-vid / stem-only path.
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
        _root_votes: dict[str, int] = {}
        for _row in out:
            _cand = _channel_root_from_filepath(_row.get("filepath") or "")
            if _cand:
                _root_votes[_cand] = _root_votes.get(_cand, 0) + 1
        if _root_votes:
            _ch_root = max(_root_votes.items(), key=lambda kv: kv[1])[0]
            _thumb_by_vid = _build_channel_thumb_index(
                _ch_root, low_priority_busy_fn=low_priority_busy_fn)

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

    for i, row in enumerate(out):
        if i % 25 == 0:
            _raise_if_low_priority_busy(low_priority_busy_fn)
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
        needs_meta = (
            not row.get("upload_ts")
            or row.get("view_count") is None
            or row.get("like_count") is None
        )
        if needs_meta and fp and vid_id:
            meta = _fetch_meta(os.path.dirname(fp)).get(vid_id)
        # Fallback — many DB rows (older channels, bulk-imported from
        # folder scans, pre-ID-column upgrades) have a NULL video_id.
        # Title-match against the JSONL's `title` field so those rows
        # still get their view_count populated for the Most Viewed
        # sort. Loose normalization (lower + whitespace collapse).
        if needs_meta and meta is None and fp:
            _t_row = row.get("title", "")
            if _t_row:
                meta = _fetch_meta_by_title(os.path.dirname(fp), _t_row)
        if meta:
            ep = _yyyymmdd_to_epoch(meta.get("upload_date", ""))
            if ep > 0:
                row["upload_ts"] = ep
        # Avoid per-row mtime probes here; the DB's added_ts is the
        # cheap last-resort fallback when upload_ts was not materialized.
        if "upload_ts" not in row or not row.get("upload_ts"):
            row["upload_ts"] = row.get("added_ts") or 0
            row["upload_ts_source"] = "index_fallback"

        # View count: fetch from aggregated metadata when available
        if row.get("view_count") is not None:
            row["view_count"] = _coerce_count(row.get("view_count"))
            row["like_count"] = _coerce_count(row.get("like_count"))
            views_label = _format_compact_count(row["view_count"])
            if views_label:
                row["views"] = views_label
        elif meta:
            # a corrupted JSONL entry with view_count="N/A"
            # would raise ValueError out of this cast and abort the
            # entire Browse grid render for the channel. Guard with
            # try/except and fall back to 0 so one bad row doesn't
            # hide all the others.
            row["view_count"] = _coerce_count(meta.get("view_count"))
            row["like_count"] = _coerce_count(meta.get("like_count"))
            # Surface as display "views" too
            views_label = _format_compact_count(row["view_count"])
            if views_label:
                row["views"] = views_label
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
        _browse_videos_cache_put(cache_key, list(out))
    return out


def list_videos_for_channel_page(
        channel: str, sort: str = "newest", limit: int = 120,
        offset: int = 0, include_thumbs: bool = True,
        query: str = "") -> dict[str, Any]:
    """Paginated channel video list for the default Browse drilldown.

    The legacy list_videos_for_channel() endpoint intentionally returns the
    whole channel so year/month grouping can build complete buckets. The
    common ungrouped grid only needs the next screenful; this path keeps the
    SQL sort/limit in SQLite and only resolves thumbnails for rows that will
    be rendered now.
    """
    conn = _reader_open()
    try:
        lim = max(1, min(int(limit or 120), 500))
    except (TypeError, ValueError):
        lim = 120
    try:
        off = max(0, int(offset or 0))
    except (TypeError, ValueError):
        off = 0
    if conn is None:
        return {"rows": [], "has_more": False, "offset": off,
                "next_offset": off}
    order = {
        "newest": "upload_ts DESC, added_ts DESC",
        "oldest": "(upload_ts IS NULL) ASC, upload_ts ASC, "
                  "added_ts ASC",
        "largest": "COALESCE(size_bytes, 0) DESC",
        "title": "title COLLATE NOCASE ASC",
        "most_viewed": "view_count DESC, added_ts DESC",
    }.get((sort or "newest").lower(),
          "upload_ts DESC, added_ts DESC")
    where = ("WHERE channel=? COLLATE NOCASE "
             "AND is_duplicate_of IS NULL")
    params: list[Any] = [channel]
    q = (query or "").strip()
    if q:
        esc = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        where += " AND title LIKE ? ESCAPE '\\'"
        params.append(f"%{esc}%")
    params.extend([lim + 1, off])
    try:
        with _reader_lock:
            cur = conn.execute(
                "SELECT title, channel, filepath, video_id, size_bytes, "
                "year, month, tx_status, added_ts, upload_ts, view_count, "
                "like_count, removed_from_yt_ts, duration_s "
                f"FROM videos {where} "
                f"ORDER BY {order} LIMIT ? OFFSET ?",
                params)
            raw = cur.fetchall()
    except sqlite3.Error as e:
        _log.debug("list_videos_for_channel_page query failed: %s", e)
        return {"rows": [], "has_more": False, "offset": off,
                "next_offset": off}
    has_more = len(raw) > lim
    raw = raw[:lim]
    out = [_build_browse_video_row(r, include_thumbs) for r in raw]
    return {"rows": out, "has_more": has_more, "offset": off,
            "next_offset": off + len(out)}


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
        from .local_fileserver import allow_file, get_port, url_for
        if get_port():
            allow_file(path)
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
        _log.warning("new_videos_in_last_n_days failed: %s", e)
    return out


def channel_transcription_stats(channel: str) -> dict[str, int]:
    """Return {total, transcribed, pending, failed, no_speech} video counts.

    Matches on channel name via the videos table (NOCASE). Empty channel ->
    zeros. Safe to call with an uninitialized DB (returns zeros). `no_speech`
    is a terminal "checked, genuinely silent" state — counted separately from
    `transcribed` (it has no transcript) and excluded from `pending`.
    """
    out = {"total": 0, "transcribed": 0, "pending": 0, "failed": 0,
           "no_speech": 0}
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
                     SUM(CASE WHEN tx_status='failed' THEN 1 ELSE 0 END) AS failed,
                     SUM(CASE WHEN tx_status='no_speech'
                              THEN 1 ELSE 0 END) AS no_speech
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
            out["no_speech"] = int(row[4] or 0)
    except Exception as e:
        _log.warning("channel_transcription_stats failed for %r: %s",
                     channel, e)
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

    for tf in search_dirs:
        for ext in (".jpg", ".jpeg", ".webp", ".png"):
            p = os.path.join(tf, stem + ".local" + ext)
            if os.path.isfile(p):
                return os.path.normpath(p)

    return None


# Regexes for the channel-wide thumbnail index below. Module-level so they
# compile once and are shared by both the Browse grid path
# (list_videos_for_channel) and the Recent tab path
# (find_thumbnail_channelwide).
_THUMB_VID_RE = re.compile(r"\[([A-Za-z0-9_-]{11})\]")
_YEAR_DIR_RE = re.compile(r"^[12][0-9]{3}$")


def _build_channel_thumb_index(
        ch_root: str,
        low_priority_busy_fn: Any | None = None) -> dict[str, str]:
    """Walk a channel root once and return ``{video_id: thumb_path}`` for
    every ``.Thumbnails/*[<id>].(jpg|jpeg|webp|png)`` anywhere beneath it.

    Cached per channel-root by the root's mtime in the module-level
    ``_thumb_index_cache`` — the SAME cache ``list_videos_for_channel``
    uses — so the Browse grid and the Recent tab resolve thumbnails
    identically and never walk the same tree twice. Returns ``{}`` when the
    root is missing or unwalkable.

    (Mirrors the inline walk in ``list_videos_for_channel``; both populate
    and read the shared cache, so a hit from one path serves the other.)
    """
    if not ch_root or not os.path.isdir(ch_root):
        return {}
    try:
        ch_mtime = os.path.getmtime(ch_root)
    except OSError:
        ch_mtime = 0.0
    key = os.path.normpath(ch_root)
    with _thumb_index_cache_lock:
        cached = _thumb_index_cache.get(key)
        if cached is not None:
            _thumb_index_cache.move_to_end(key)
    if cached is not None and ch_mtime > 0 and cached.get("mtime") == ch_mtime:
        return dict(cached.get("thumbs") or {})
    _raise_if_low_priority_busy(low_priority_busy_fn)
    thumbs: dict[str, str] = {}
    try:
        for _dp, _dns, _fns in os.walk(ch_root):
            _raise_if_low_priority_busy(low_priority_busy_fn)
            if os.path.basename(_dp) != ".Thumbnails":
                continue
            for _fn in _fns:
                if not _fn.lower().endswith((".jpg", ".jpeg", ".webp", ".png")):
                    continue
                _m = _THUMB_VID_RE.search(_fn)
                if _m:
                    thumbs[_m.group(1)] = os.path.normpath(
                        os.path.join(_dp, _fn))
    except OSError:
        pass
    if ch_mtime > 0:
        with _thumb_index_cache_lock:
            _thumb_index_cache_put(
                key, {"mtime": ch_mtime, "thumbs": dict(thumbs)})
    return thumbs


def _channel_root_from_filepath(video_filepath: str) -> str | None:
    """Infer the channel-root directory from ONE video's filepath.

    Mirrors the per-row root detection in ``list_videos_for_channel``::

        <root>/<file>                  -> <root>
        <root>/<year>/<file>           -> <root>
        <root>/<year>/<month>/<file>   -> <root>

    A directory named exactly four digits (1900-2099 range) is treated as a
    year bucket; its parent (or grandparent, for year/month layouts) is the
    channel root. Returns ``None`` if the inferred root isn't a real dir.
    """
    if not video_filepath:
        return None
    parent = os.path.dirname(video_filepath)
    gp = os.path.dirname(parent)
    if _YEAR_DIR_RE.match(os.path.basename(parent)):
        cand = gp
    elif _YEAR_DIR_RE.match(os.path.basename(gp)):
        cand = os.path.dirname(gp)
    else:
        cand = parent
    return cand if (cand and os.path.isdir(cand)) else None


def find_thumbnail_channelwide(video_filepath: str,
                               video_id: str | None = None) -> str | None:
    """Resolve a thumbnail the way the Browse grid does: if the cheap up-walk
    misses, scan the WHOLE channel tree by video_id.

    Needed because a video's thumbnail can live in a different year/month
    ``.Thumbnails/`` than where its mp4 currently sits. Real case: an mp4
    foldered under the download month (``2026/06 June/``) while its
    thumbnail was foldered under the upload month
    (``2026/05 May/.Thumbnails/``). ``find_thumbnail`` only walks UP from the
    mp4, so it never crosses into the sibling month and the Recent card fell
    back to a gradient placeholder — even though the Browse grid (which does
    a channel-wide walk) showed the thumbnail fine.

    Ordered narrow-first for speed: the common case (thumbnail co-located on
    the mp4's own path) is a handful of stats via ``find_thumbnail`` and
    never triggers the channel-wide walk. Only genuinely-misplaced rows pay
    for the tree walk, and that result is cached + shared with Browse.
    """
    if not video_filepath:
        return None
    # 1. Cheap narrow up-walk first — handles correctly co-located thumbs.
    tp = find_thumbnail(video_filepath, video_id)
    if tp:
        return tp
    # 2. Miss — thumbnail may be in a sibling year/month .Thumbnails/. Use
    #    the channel-wide by-id index (cached, shared with the Browse grid).
    vid = (video_id or "").strip()
    if vid:
        ch_root = _channel_root_from_filepath(video_filepath)
        if ch_root:
            tp = _build_channel_thumb_index(ch_root).get(vid)
            if tp and os.path.isfile(tp):
                return os.path.normpath(tp)
    return None


# ── Global "Videos" view: materialized view/like stats + paginated list ──

def update_video_stats(updates) -> int:
    """Batch-write view_count/like_count into the videos table.

    `updates`: iterable of (video_id, view_count, like_count). None counts
    are skipped for that field. Called by the metadata-refresh pass (so the
    DB stays current) and by backfill_video_stats. Returns rows updated.
    """
    rows = [u for u in (updates or []) if u and u[0]]
    if not rows:
        return 0
    try:
        with _db_lock:
            conn = _open()
            if conn is None:
                return 0
            n = 0
            for vid, vc, lc in rows:
                try:
                    cur = conn.execute(
                        "UPDATE videos SET "
                        "view_count = COALESCE(?, view_count), "
                        "like_count = COALESCE(?, like_count) "
                        "WHERE video_id = ?",
                        (vc, lc, vid))
                    n += cur.rowcount or 0
                except sqlite3.Error:
                    continue
            conn.commit()
            return n
    except sqlite3.Error:
        return 0


def backfill_video_stats(progress=None) -> dict:
    """One-time: populate videos.view_count/like_count from the per-channel
    Metadata.jsonl sidecars so the global Videos view can sort the whole
    archive by views/likes off an indexed column. Idempotent; safe to re-run.
    """
    try:
        from .metadata.io import _folder_for_channel, _read_metadata_jsonl
        from .ytarchiver_config import load_config
    except Exception as e:
        return {"ok": False, "error": f"import: {e}", "updated": 0}
    channels = (load_config().get("channels") or [])
    total_updated = 0
    n_ch = len(channels)
    for i, ch in enumerate(channels):
        try:
            folder = _folder_for_channel(ch)
        except Exception:
            folder = None
        if folder is None:
            continue
        seen: dict[str, tuple] = {}
        try:
            for dp, _dns, fns in os.walk(str(folder)):
                for fn in fns:
                    if not fn.endswith("Metadata.jsonl"):
                        continue
                    try:
                        data = _read_metadata_jsonl(os.path.join(dp, fn))
                    except Exception:
                        continue
                    for vid, entry in data.items():
                        if not vid:
                            continue
                        vc = entry.get("view_count")
                        lc = entry.get("like_count")
                        if vc is None and lc is None:
                            continue
                        try: vc = int(vc) if vc is not None else None
                        except (TypeError, ValueError): vc = None
                        try: lc = int(lc) if lc is not None else None
                        except (TypeError, ValueError): lc = None
                        seen[vid] = (vc, lc)
        except Exception as e:
            _log.debug("backfill walk failed (%s): %s", folder, e)
        if seen:
            total_updated += update_video_stats(
                [(vid, vc, lc) for vid, (vc, lc) in seen.items()])
        if progress:
            try:
                progress({"done": i + 1, "total": n_ch, "updated": total_updated})
            except Exception:
                pass
    return {"ok": True, "updated": total_updated, "channels": n_ch}


def backfill_video_stats_if_needed(progress=None) -> dict:
    """Run the view/like backfill only if the videos table has NO view_count
    data yet (i.e. first launch after the columns were added). No-op once
    populated, so it's safe to call on every boot."""
    try:
        conn = _reader_open()
        if conn is None:
            return {"ok": False, "skipped": True}
        with _reader_lock:
            row = conn.execute(
                "SELECT 1 FROM videos WHERE view_count IS NOT NULL LIMIT 1"
            ).fetchone()
        if row:
            return {"ok": True, "skipped": True, "reason": "already populated"}
    except sqlite3.Error:
        return {"ok": False, "skipped": True}
    return backfill_video_stats(progress=progress)


def backfill_video_ids_from_sidecars(progress=None) -> dict:
    """Recover video_id for rows where it's NULL/'' by reading the
    .info.json sidecars in each video's folder, matched by content (the id
    yt-dlp recorded, regardless of the sidecar's filename). No network.
    Fixes the 'downloaded but no id' rows so thumbnail fetch + metadata
    refetch work again. Idempotent; safe to re-run."""
    try:
        with _db_lock:
            conn = _open()
            if conn is None:
                return {"ok": False, "error": "no db", "fixed": 0}
            rows = conn.execute(
                "SELECT id, filepath FROM videos "
                "WHERE (video_id IS NULL OR video_id = '') "
                "AND filepath IS NOT NULL "
                "ORDER BY filepath"  # group rows by directory for cache hits
            ).fetchall()
    except sqlite3.Error as e:
        return {"ok": False, "error": str(e), "fixed": 0}
    total = len(rows)
    fixed = 0
    pending: list[tuple] = []

    def _flush():
        nonlocal fixed, pending
        if not pending:
            return
        try:
            with _db_lock:
                c = _open()
                if c is not None:
                    c.executemany(
                        "UPDATE videos SET video_id=?, video_url=? WHERE id=?",
                        pending)
                    c.commit()
                    fixed += len(pending)
        except sqlite3.Error as e:
            _log.debug("id backfill flush failed: %s", e)
        pending = []

    for i, (rid, fp) in enumerate(rows):
        vid = _resolve_id_from_sidecars(fp) if fp else ""
        if vid:
            pending.append((vid, f"https://www.youtube.com/watch?v={vid}", rid))
            if len(pending) >= 500:
                _flush()
        if progress and (i % 1000 == 0 or i == total - 1):
            try: progress({"done": i + 1, "total": total, "fixed": fixed})
            except Exception: pass
    _flush()
    return {"ok": True, "fixed": fixed, "null_rows": total}


def _fmt_video_duration(s) -> str:
    try:
        s = int(float(s or 0))
    except (TypeError, ValueError):
        return ""
    if s <= 0:
        return ""
    h, m, sec = s // 3600, (s % 3600) // 60, s % 60
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def _build_browse_video_row(r, include_thumbs: bool = True) -> dict:
    """Shape one `videos` row into a Browse video-card dict.

    Expects the column order used by list_all_videos / list_manual_videos:
    (title, channel, filepath, video_id, size_bytes, year, month, tx_status,
     added_ts, upload_ts, view_count, like_count, removed_from_yt_ts, duration_s)
    """
    (title, channel, fp, vid, size_b, yr, mo, tx, added, upts,
     vc, lc, rem, dur_s) = r
    d = {
        "title": title or "", "channel": channel or "", "filepath": fp or "",
        "video_id": vid or "", "size_bytes": size_b or 0,
        "year": yr, "month": mo, "tx_status": tx, "added_ts": added,
        "upload_ts": upts, "view_count": vc, "like_count": lc,
        "duration": _fmt_video_duration(dur_s),
        "removed_from_yt": bool(rem), "show_channel": True,
    }
    if upts:
        try:
            from datetime import datetime as _dt
            d["uploaded"] = _dt.fromtimestamp(upts).strftime("%Y-%m-%d")
        except (OverflowError, OSError, ValueError):
            pass
    views_label = _format_compact_count(vc)
    if views_label:
        d["views"] = views_label
    if include_thumbs and fp:
        try:
            tp = find_thumbnail_channelwide(fp, vid)
            if tp:
                d["thumbnail_url"] = _file_url(tp)
        except Exception:
            pass
    return d


def _manual_like_prefix(path: str) -> str:
    """SQL LIKE prefix for a directory: normalized, trailing separator (so
    'Archive' can't match 'ArchiveBad'), LIKE-metacharacters escaped."""
    n = os.path.normpath(path)
    if not n.endswith(("\\", "/")):
        n += os.sep
    return n.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def _manual_where_and_params() -> tuple[str, list]:
    """Build the WHERE that classifies a `videos` row as a single/manual
    download: any indexed video NOT inside a channel archive tree (output_dir)
    or an index-only archive root (tp_archive_roots). Returns ("", []) when
    there are no roots to classify against. Shared by list_manual_videos +
    list_manual_videos_without_id so the classification stays identical.

    Exclusion-ONLY — `video_out_dir` is deliberately NOT used as an *include*:
    a user can set output_dir to a SUBFOLDER of video_out_dir (e.g.
    video_out_dir `…\\YT videos`, channels under `…\\YT videos\\Whole Channels`),
    and an 'under video_out_dir' include would then sweep in every channel
    download. Excluding the channel/archive roots is correct for every layout,
    since manual downloads live outside those roots by definition.
    """
    try:
        from .ytarchiver_config import load_config
        cfg = load_config() or {}
    except Exception:
        cfg = {}
    out_dir = (cfg.get("output_dir") or "").strip()
    roots = ([out_dir] if out_dir else [])
    roots.extend(str(r) for r in (cfg.get("tp_archive_roots") or []) if r)
    if not roots:
        return ("", [])
    where = "WHERE is_duplicate_of IS NULL"
    params: list = []
    for r in roots:
        where += " AND filepath NOT LIKE ? ESCAPE '\\'"
        params.append(_manual_like_prefix(r))
    return (where, params)


def list_manual_videos(include_thumbs: bool = True) -> list[dict]:
    """All index rows that are single/manual downloads — i.e. saved in
    video_out_dir OR outside every managed archive root (custom 'Save to'
    locations). Channel downloads (under output_dir/<channel>/) and index-only
    tp_archive_roots are excluded. Returns the FULL list (the SQL filter keeps
    it small); the caller sorts + paginates. Rows match list_all_videos shape.
    """
    conn = _reader_open()
    if conn is None:
        return []
    where, params = _manual_where_and_params()
    if not where:
        return []
    try:
        with _reader_lock:
            cur = conn.execute(
                "SELECT title, channel, filepath, video_id, size_bytes, year, month, "
                "tx_status, added_ts, upload_ts, view_count, like_count, "
                "removed_from_yt_ts, duration_s, id_backfill_tried_ts, "
                "id_backfill_fail_count, id_backfill_excluded_ts "
                f"FROM videos {where}",
                params)
            raw = cur.fetchall()
    except sqlite3.Error as e:
        _log.debug("list_manual_videos query failed: %s", e)
        return []
    out = []
    for r in raw:
        d = _build_browse_video_row(r[:14], include_thumbs)
        d["id_backfill_tried_ts"] = r[14]
        d["id_backfill_fail_count"] = int(r[15] or 0)
        d["id_backfill_excluded_ts"] = r[16]
        out.append(d)
    return out


def list_manual_videos_without_id(*, include_excluded: bool = False) -> list[dict]:
    """Manual downloads that still have NO video_id — the backfill target.
    Lightweight rows: {filepath, title, duration_s}."""
    conn = _reader_open()
    if conn is None:
        return []
    where, params = _manual_where_and_params()
    if not where:
        return []
    where += " AND (video_id IS NULL OR video_id='')"
    if not include_excluded:
        where += " AND id_backfill_excluded_ts IS NULL"
    try:
        with _reader_lock:
            cur = conn.execute(
                "SELECT filepath, title, duration_s, id_backfill_tried_ts, "
                f"id_backfill_fail_count, id_backfill_excluded_ts FROM videos {where}",
                params)
            raw = cur.fetchall()
    except sqlite3.Error as e:
        _log.debug("list_manual_videos_without_id query failed: %s", e)
        return []
    return [{
        "filepath": r[0] or "",
        "title": r[1] or "",
        "duration_s": r[2],
        "id_backfill_tried_ts": r[3],
        "id_backfill_fail_count": int(r[4] or 0),
        "id_backfill_excluded_ts": r[5],
    } for r in raw]


def list_manual_duplicate_filepaths() -> list[str]:
    """Manual download paths that are indexed only as duplicates."""
    conn = _reader_open()
    if conn is None:
        return []
    try:
        from .ytarchiver_config import load_config
        cfg = load_config() or {}
    except Exception:
        cfg = {}
    roots: list[str] = []
    out_dir = (cfg.get("output_dir") or "").strip()
    if out_dir:
        roots.append(out_dir)
    roots.extend(str(r) for r in (cfg.get("tp_archive_roots") or []) if r)
    if not roots:
        return []
    where = "WHERE is_duplicate_of IS NOT NULL"
    params: list = []
    for r in roots:
        where += " AND filepath NOT LIKE ? ESCAPE '\\'"
        params.append(_manual_like_prefix(r))
    try:
        with _reader_lock:
            cur = conn.execute(f"SELECT filepath FROM videos {where}", params)
            return [r[0] or "" for r in cur.fetchall() if r and r[0]]
    except sqlite3.Error as e:
        _log.debug("list_manual_duplicate_filepaths query failed: %s", e)
        return []


def set_video_duration(filepath: str, duration_secs: float) -> bool:
    """Persist a probed local duration for one indexed video filepath."""
    if not filepath:
        return False
    try:
        dur = float(duration_secs)
    except (TypeError, ValueError):
        return False
    if dur <= 0:
        return False
    conn = _open()
    if conn is None:
        return False
    try:
        with _db_lock:
            cur = conn.execute(
                "UPDATE videos SET duration_s=? "
                "WHERE filepath=? COLLATE NOCASE "
                "AND (duration_s IS NULL OR duration_s<=0)",
                (dur, os.path.normpath(filepath)))
            conn.commit()
            changed = cur.rowcount > 0
        if changed:
            invalidate_channel_videos(None)
        return changed
    except sqlite3.Error as e:
        _log.debug("set_video_duration failed: %s", e)
        return False


def set_manual_video_id(filepath: str, video_id: str, video_url: str = "",
                        channel: str | None = None) -> bool:
    """Write a resolved video_id (+url, +tried stamp) onto a manual download
    that currently has none. Returns True if a row was updated."""
    if not filepath or not video_id:
        return False
    _now = int(time.time())
    _url = video_url or f"https://www.youtube.com/watch?v={video_id}"
    _np = os.path.normpath(filepath)
    _channel = (channel or "").strip()
    with _db_lock:
        conn = _open()
        if conn is None:
            return False
        try:
            _set_channel_sql = ", channel=?" if _channel else ""
            _args = [video_id, _url, _now]
            if _channel:
                _args.append(_channel)
            _args.append(_np)
            cur = conn.execute(
                "UPDATE videos SET video_id=?, video_url=?, "
                "id_backfill_tried_ts=?, id_backfill_fail_count=0, "
                "id_backfill_excluded_ts=NULL "
                f"{_set_channel_sql} "
                "WHERE filepath=? COLLATE NOCASE AND (video_id IS NULL OR video_id='')",
                tuple(_args))
            if (cur.rowcount or 0) == 0 and filepath != _np:
                _args = [video_id, _url, _now]
                if _channel:
                    _args.append(_channel)
                _args.append(filepath)
                cur = conn.execute(
                    "UPDATE videos SET video_id=?, video_url=?, "
                    "id_backfill_tried_ts=?, id_backfill_fail_count=0, "
                    "id_backfill_excluded_ts=NULL "
                    f"{_set_channel_sql} "
                    "WHERE filepath=? COLLATE NOCASE AND (video_id IS NULL OR video_id='')",
                    tuple(_args))
            conn.commit()
            return (cur.rowcount or 0) > 0
        except sqlite3.Error as e:
            _log.warning("set_manual_video_id failed: %s", e)
            return False


def stamp_manual_id_tried(filepath: str) -> None:
    """Mark a manual download as 'backfill attempted' so re-runs can skip it
    and the UI can tell 'tried but unresolved' from 'not yet tried'."""
    if not filepath:
        return
    _now = int(time.time())
    _np = os.path.normpath(filepath)
    with _db_lock:
        conn = _open()
        if conn is None:
            return
        try:
            cur = conn.execute(
                "UPDATE videos SET id_backfill_tried_ts=? WHERE filepath=? COLLATE NOCASE",
                (_now, _np))
            if (cur.rowcount or 0) == 0 and filepath != _np:
                conn.execute(
                    "UPDATE videos SET id_backfill_tried_ts=? WHERE filepath=? COLLATE NOCASE",
                    (_now, filepath))
            conn.commit()
        except sqlite3.Error as e:
            _log.debug("stamp_manual_id_tried failed: %s", e)


def mark_manual_id_backfill_failed(filepath: str, *,
                                   title: str = "",
                                   duration_secs: float | None = None,
                                   exclude_after: int = 3) -> dict[str, Any]:
    """Increment the no-ID manual recovery failure count for one file.

    After `exclude_after` consecutive misses the row is excluded from future
    automatic Recover IDs runs. A later successful set_manual_video_id resets
    the count and exclusion timestamp.
    """
    if not filepath:
        return {"ok": False, "error": "missing filepath"}
    try:
        threshold = max(1, int(exclude_after))
    except (TypeError, ValueError):
        threshold = 3
    _now = int(time.time())
    _np = os.path.normpath(filepath)

    def _update(path: str) -> tuple[bool, dict[str, Any]]:
        with _db_lock:
            conn = _open()
            if conn is None:
                return False, {}
            try:
                cur = conn.execute(
                    "UPDATE videos SET "
                    "id_backfill_tried_ts=?, "
                    "id_backfill_fail_count=COALESCE(id_backfill_fail_count, 0) + 1, "
                    "id_backfill_excluded_ts=CASE "
                    "  WHEN COALESCE(id_backfill_fail_count, 0) + 1 >= ? "
                    "  THEN COALESCE(id_backfill_excluded_ts, ?) "
                    "  ELSE id_backfill_excluded_ts END "
                    "WHERE filepath=? COLLATE NOCASE "
                    "AND (video_id IS NULL OR video_id='')",
                    (_now, threshold, _now, path))
                row = conn.execute(
                    "SELECT id_backfill_fail_count, id_backfill_excluded_ts "
                    "FROM videos WHERE filepath=? COLLATE NOCASE",
                    (path,)).fetchone()
                conn.commit()
                return (cur.rowcount or 0) > 0, {
                    "fail_count": int(row[0] or 0) if row else 0,
                    "excluded_ts": row[1] if row else None,
                }
            except sqlite3.Error as e:
                _log.debug("mark_manual_id_backfill_failed failed: %s", e)
                return False, {"error": str(e)}

    changed, info = _update(_np)
    if not changed and filepath != _np:
        changed, info = _update(filepath)
    if not changed and os.path.isfile(filepath):
        # Root-folder imported files may be discovered before any indexed row
        # exists. Register a no-ID row so repeated misses can be remembered.
        try:
            register_video(
                filepath,
                "Single Videos",
                title or os.path.splitext(os.path.basename(filepath))[0],
                duration_secs=duration_secs)
            changed, info = _update(_np)
            if not changed and filepath != _np:
                changed, info = _update(filepath)
        except Exception as e:
            _log.debug("register before manual-id-fail mark failed: %s", e)
    if changed:
        invalidate_channel_videos(None)
    return {
        "ok": bool(changed),
        "fail_count": int((info or {}).get("fail_count") or 0),
        "excluded": bool((info or {}).get("excluded_ts")),
        "excluded_ts": (info or {}).get("excluded_ts"),
    }


def list_all_videos(sort: str = "recent", limit: int = 60, offset: int = 0,
                    include_thumbs: bool = True, query: str = "") -> dict:
    """Paginated global video list across the whole archive (the Videos view).

    Sorts off materialized DB columns (added_ts / upload_ts / view_count /
    like_count / size_bytes), so no sidecar walks. Returns
    {rows, has_more, offset}. Rows are shaped for the Browse video-card
    renderer (show_channel=True since channels are mixed).
    """
    conn = _reader_open()
    if conn is None:
        return {"rows": [], "has_more": False, "offset": offset}
    order = {
        "recent":  "COALESCE(added_ts, 0) DESC, id DESC",
        "newest":  "(upload_ts IS NULL) ASC, upload_ts DESC, COALESCE(added_ts, 0) DESC",
        "oldest":  "(upload_ts IS NULL) ASC, upload_ts ASC, COALESCE(added_ts, 0) ASC",
        "title":   "title COLLATE NOCASE ASC",
        "channel": "channel COLLATE NOCASE ASC, (upload_ts IS NULL) ASC, upload_ts DESC",
        "views":   "(view_count IS NULL) ASC, view_count DESC, COALESCE(added_ts, 0) DESC",
        "likes":   "(like_count IS NULL) ASC, like_count DESC, COALESCE(added_ts, 0) DESC",
        "largest": "COALESCE(size_bytes, 0) DESC",
    }.get((sort or "recent").lower(), "COALESCE(added_ts, 0) DESC, id DESC")
    try:
        lim = max(1, int(limit)); off = max(0, int(offset))
    except (TypeError, ValueError):
        lim, off = 60, 0
    # Result-cache hit (see _all_videos_cache). Skips the per-row thumbnail
    # disk-walk that makes a cold open ~10s. Return copies so callers can't
    # mutate the cached page.
    _q = (query or "").strip()
    _ck = ((sort or "recent").lower(), lim, off, bool(include_thumbs), _q.lower())
    with _browse_cache_lock:
        _hit = _all_videos_cache.get(_ck)
        if _hit is not None:
            _all_videos_cache.move_to_end(_ck)
    if _hit is not None:
        return {"rows": [dict(r) for r in _hit["rows"]],
                "has_more": _hit["has_more"], "offset": _hit["offset"]}
    try:
        where = "WHERE is_duplicate_of IS NULL"
        params: list[Any] = []
        if _q:
            # Filter by title OR channel (substring, case-insensitive via
            # LIKE). Escape LIKE metacharacters so a literal % / _ in the
            # query doesn't act as a wildcard.
            esc = _q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            like = f"%{esc}%"
            where += (" AND (title LIKE ? ESCAPE '\\' "
                      "OR channel LIKE ? ESCAPE '\\')")
            params.extend([like, like])
        params.extend([lim + 1, off])
        with _reader_lock:
            cur = conn.execute(
                "SELECT title, channel, filepath, video_id, size_bytes, year, month, "
                "tx_status, added_ts, upload_ts, view_count, like_count, "
                "removed_from_yt_ts, duration_s "
                f"FROM videos {where} "
                f"ORDER BY {order} LIMIT ? OFFSET ?",
                params)
            raw = cur.fetchall()
    except sqlite3.Error as e:
        _log.debug("list_all_videos query failed: %s", e)
        return {"rows": [], "has_more": False, "offset": offset}
    has_more = len(raw) > lim
    raw = raw[:lim]

    out = [_build_browse_video_row(r, include_thumbs) for r in raw]
    # Store a copy in the result cache so the next open is instant.
    with _browse_cache_lock:
        _all_videos_cache_put(
            _ck,
            {"rows": [dict(r) for r in out],
             "has_more": has_more, "offset": off})
    return {"rows": out, "has_more": has_more, "offset": off}


def find_archived_by_video_id(video_id: str) -> dict | None:
    """Return {title, channel, filepath} for an archived video matching this
    YouTube id, or None if it isn't in the index.

    Used by the single-URL Download pre-check to warn before re-downloading
    something already archived. Deliberately queries the live index
    (videos.video_id) rather than a separate "already-downloaded" list, so the
    warning always reflects what is ACTUALLY archived right now (delete the
    file and it correctly stops reporting as archived). Rows flagged as
    duplicates are ignored. Never raises.
    """
    vid = (video_id or "").strip()
    if not vid:
        return None
    conn = _reader_open()
    if conn is None:
        return None
    try:
        with _reader_lock:
            cur = conn.execute(
                "SELECT title, channel, filepath FROM videos "
                "WHERE video_id = ? AND is_duplicate_of IS NULL "
                "ORDER BY (filepath IS NOT NULL) DESC, id ASC LIMIT 1",
                (vid,))
            row = cur.fetchone()
    except sqlite3.Error as e:
        _log.debug("find_archived_by_video_id query failed: %s", e)
        return None
    if not row:
        return None
    return {"title": row[0] or "", "channel": row[1] or "",
            "filepath": row[2] or ""}


def video_tx_status(video_id: str | None = None,
                    title: str | None = None) -> str:
    """Return the tx_status for a single video, or "" if unknown.

    Values: 'transcribed' | 'pending' | 'no_speech' | 'no_captions' |
    'failed' | 'done'. Lock-free reader path (same rationale as
    get_segments). Used by browse_get_transcript so the Watch view can tell
    a genuinely-silent video ('no_speech') apart from one that simply hasn't
    been transcribed yet (both have zero segments)."""
    if not video_id and not title:
        return ""
    conn = _reader_open()
    lock = _reader_lock
    if conn is None:
        conn = _open()
        lock = _db_lock
    if conn is None:
        return ""
    try:
        with lock:
            if video_id:
                row = conn.execute(
                    "SELECT tx_status FROM videos WHERE video_id=? LIMIT 1",
                    (video_id,)).fetchone()
                if row:
                    return row[0] or ""
            if title:
                row = conn.execute(
                    "SELECT tx_status FROM videos WHERE title=? LIMIT 1",
                    (title,)).fetchone()
                if row:
                    return row[0] or ""
    except sqlite3.Error as e:
        _log.debug("video_tx_status failed: %s", e)
    return ""


def get_segments(video_id: str | None = None, jsonl_path: str | None = None,
                 title: str | None = None, channel: str | None = None,
                 filepath: str | None = None,
                 strict_identity: bool = False) -> list[dict[str, Any]]:
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
    conn = _reader_open()
    lock = _reader_lock
    if conn is None:
        conn = _open()
        lock = _db_lock
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
    with lock:
        _jp_from_canon = False
        _file_video_id = ""
        if filepath:
            try:
                from .text_utils import extract_video_id as _extract_vid
                _file_video_id = _extract_vid(
                    os.path.normpath(filepath),
                    conn=conn,
                    reject_alpha_only=True,
                )
            except Exception as e:
                _log.debug("watch filepath id resolve failed: %s", e)
            if _file_video_id:
                if video_id and video_id != _file_video_id:
                    _log.warning(
                        "watch transcript id mismatch; using filepath id: "
                        "payload=%s filepath=%s title=%r",
                        video_id, _file_video_id, title)
                video_id = _file_video_id
        # When the video_id resolves to no segments, fall back to matching
        # by title (the way get_segment_context / video_tx_status already do).
        # Legacy rows store video_id="" when the filename had no 11-char YT id
        # AND the JSONL carried none, so those transcripts are searchable (and
        # the search viewer reaches them by segment_id) but a video_id lookup
        # returns nothing — leaving the Watch pane blank. Matching by title
        # (scoped to a canonical jsonl_path to avoid doubling a combined +
        # year-split ingest) makes them viewable in Watch too.
        _match_by_title = False
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
                elif title:
                    # No segments carry this video_id -> match by title.
                    _match_by_title = True
                    try:
                        if channel:
                            tcanon = conn.execute(
                                "SELECT jsonl_path FROM segments "
                                "WHERE title=? AND channel=? COLLATE NOCASE "
                                "GROUP BY jsonl_path "
                                "ORDER BY MAX(id) DESC LIMIT 1",
                                (title, channel),
                            ).fetchone()
                        else:
                            tcanon = conn.execute(
                                "SELECT jsonl_path FROM segments WHERE title=? "
                                "GROUP BY jsonl_path "
                                "ORDER BY MAX(id) DESC LIMIT 1",
                                (title,),
                            ).fetchone()
                        if tcanon and tcanon[0]:
                            jsonl_path = tcanon[0]
                            _jp_from_canon = True
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
            except Exception as e:
                _log.debug("swallowed: %s", e)
        where = []
        args: list[Any] = []
        if video_id and not _match_by_title:
            where.append("video_id=?"); args.append(video_id)
        if _match_by_title and title:
            where.append("title=?"); args.append(title)
            if channel:
                where.append("channel=? COLLATE NOCASE")
                args.append(channel)
        if jsonl_path:
            where.append("jsonl_path=?")
            args.append(jsonl_path if _jp_from_canon
                        else os.path.normpath(jsonl_path))
        if title and not where:
            where.append("title=?"); args.append(title)
            if channel:
                where.append("channel=? COLLATE NOCASE")
                args.append(channel)
        if not where:
            return []
        # Use AND across multiple filters: when a caller passes both video_id
        # and jsonl_path (the common case from main.py:browse_get_transcript),
        # we want segments that match BOTH, not segments matching either.
        # OR semantics would mash together segments from any video that shares
        # a jsonl_path (combined transcripts) with a different video_id.
        q = ("SELECT start_time, end_time, text, words, title, channel "
             "FROM segments "
             f"WHERE {' AND '.join(where)} ORDER BY start_time")
        cur = conn.execute(q, args)
        _rows = cur.fetchall()
        if strict_identity and _rows:
            try:
                from .text_utils import normalize_title as _norm_title
                _wanted_title = _norm_title(title or "")
            except Exception:
                _wanted_title = (title or "").strip().lower()
                _norm_title = lambda s: (s or "").strip().lower()
            _wanted_channel = (channel or "").strip().casefold()
            _sample = _rows[:20]
            _wanted_title_keys = _transcript_identity_title_keys(
                title or "", _norm_title)
            if _wanted_title_keys and not any(
                    _transcript_identity_title_keys(
                        r[4] or "", _norm_title) & _wanted_title_keys
                    for r in _sample):
                _log.warning(
                    "refusing transcript rows with mismatched title: "
                    "wanted=%r got=%r video_id=%r",
                    title, _sample[0][4] if _sample else "", video_id)
                _rows = []
            _sample_channels = [
                (r[5] or "").strip().casefold() for r in _sample
            ]
            if (_rows and _wanted_channel and any(_sample_channels)
                    and _wanted_channel not in _sample_channels):
                _log.warning(
                    "refusing transcript rows with mismatched channel: "
                    "wanted=%r got=%r video_id=%r",
                    channel, _sample[0][5] if _sample else "", video_id)
                _rows = []
    out = []
    import json as _j
    for s, e, t, w, _title, _channel in _rows:
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
            # Pull the window around the hit within the same video.
            # A combined-transcript .jsonl holds MANY videos under ONE
            # jsonl_path, so path-only context leaks other videos' lines.
            # Prefer video_id-only scope when present. On combined-transcript
            # files, a jsonl_path predicate can make SQLite choose the broad
            # path index and scan huge files before applying video_id. The
            # video_id index narrows to one video first and still prevents
            # cross-video context bleed. Fall back to path+title+channel for
            # legacy rows that lack video_id.
            if vid_id:
                scope_sql = "video_id=?"
                scope_args = (vid_id,)
            else:
                scope_sql = "jsonl_path=? AND title=? AND channel=?"
                scope_args = (jsonl_path, title, channel)
            rows_before = conn.execute(
                "SELECT id, start_time, end_time, text FROM segments "
                f"WHERE {scope_sql} AND start_time < ? "
                "ORDER BY start_time DESC LIMIT ?",
                (*scope_args, hit_start, before)).fetchall()
            rows_after = conn.execute(
                "SELECT id, start_time, end_time, text FROM segments "
                f"WHERE {scope_sql} AND start_time > ? "
                "ORDER BY start_time ASC LIMIT ?",
                (*scope_args, hit_start, after)).fetchall()
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
                    f"WHERE {scope_sql} AND start_time < ?",
                    (*scope_args, before_edge)).fetchone()[0] > 0
            if rows_after:
                after_edge = rows_after[-1][1]
                more_after = conn.execute(
                    "SELECT COUNT(*) FROM segments "
                    f"WHERE {scope_sql} AND start_time > ?",
                    (*scope_args, after_edge)).fetchone()[0] > 0
            # All other segments matching the query in this same video —
            # mark them so the viewer can highlight every hit, not just the
            # one the user clicked.
            other_hits: set = set()
            if query and query.strip():
                from .index_search import _normalize_fts_query, _sanitize_fts_query
                match_query = _normalize_fts_query(query)
                try:
                    cur2 = conn.execute(
                        "SELECT s.id FROM segments_fts "
                        "JOIN segments s ON s.id = segments_fts.rowid "
                        f"WHERE {('s.video_id=?' if vid_id else 's.jsonl_path=? AND s.title=? AND s.channel=?')} "
                        "AND segments_fts MATCH ?",
                        (*scope_args, match_query))
                    other_hits = {r[0] for r in cur2.fetchall()}
                except sqlite3.OperationalError:
                    cleaned = _sanitize_fts_query(query)
                    if cleaned and cleaned != match_query:
                        try:
                            cur2 = conn.execute(
                                "SELECT s.id FROM segments_fts "
                                "JOIN segments s ON s.id = segments_fts.rowid "
                                f"WHERE {('s.video_id=?' if vid_id else 's.jsonl_path=? AND s.title=? AND s.channel=?')} "
                                "AND segments_fts MATCH ?",
                                (*scope_args, cleaned))
                            other_hits = {r[0] for r in cur2.fetchall()}
                        except sqlite3.Error as exc:
                            _log.warning("context highlight query failed: %s",
                                         exc)
                            other_hits = set()
                except sqlite3.Error as exc:
                    _log.warning("context highlight query failed: %s", exc)
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
from .index_search import (  # noqa: F401
    _sanitize_fts_query,
    search_fts,
    search_video_titles,
)

# ── Stats ───────────────────────────────────────────────────────────────

def summary() -> dict[str, Any]:
    # Read-only stats on an independent connection so full-table counts do
    # not hold the shared reader lock used by Browse/Search/Watch.
    conn = _open_independent()
    if conn is None:
        return {"segments": 0, "videos": 0, "channels": 0, "bookmarks": 0}
    try:
        seg = conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
        vid = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
        ch = conn.execute(
            "SELECT COUNT(*) FROM ("
            "SELECT 1 FROM videos WHERE channel IS NOT NULL GROUP BY channel"
            ")"
        ).fetchone()[0]
        bm = conn.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0]
        return {"segments": seg, "videos": vid, "channels": ch,
                "bookmarks": bm}
    except sqlite3.Error as e:
        _log.warning("index summary failed: %s", e)
        return {"segments": 0, "videos": 0, "channels": 0, "bookmarks": 0}
    finally:
        try:
            conn.close()
        except Exception:
            pass



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
