"""Additive logical-video / physical-media catalog repository.

The legacy ``videos`` table remains the rollback contract for Patch 4 and
older builds.  Patch 5 owns an additive, normalized projection in
``logical_videos`` and ``media_files``.  Every Patch 5 writer connection
reconciles that projection in the *same SQLite transaction* before commit.

Older binaries can continue writing ``videos``.  Tiny compatibility triggers
only mark the normalized projection dirty; the next Patch 5 open rebuilds and
validates it before normalized reads are enabled.  No migration step stats or
otherwise probes archive files.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CATALOG_SCHEMA_VERSION = 1
CATALOG_PHASES = (
    "legacy",
    "schema_ready",
    "copied",
    "compared",
    "v2_reads",
    "v2_writes",
)
_READ_PHASES = frozenset({"v2_reads", "v2_writes"})
_SQLITE_ASCII_LOWER = str.maketrans(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "abcdefghijklmnopqrstuvwxyz",
)


class CatalogMigrationError(RuntimeError):
    """Raised when the normalized projection cannot be proved equivalent."""


class CatalogBackupError(RuntimeError):
    """Raised when a pre-migration catalog backup cannot be verified."""


@dataclass(frozen=True, slots=True)
class CatalogStatus:
    phase: str
    schema_version: int
    legacy_dirty: bool
    comparison_digest: str
    compared_at: float | None
    last_error: str
    logical_videos: int
    media_files: int

    @property
    def reads_enabled(self) -> bool:
        return self.phase in _READ_PHASES and not self.legacy_dirty


def normalize_media_path(path: Any) -> str:
    """Return the rollback-compatible SQLite identity form for a filepath."""
    value = str(path or "").strip(" ").replace("/", "\\")
    return value.translate(_SQLITE_ASCII_LOWER)


def identity_key_for_row(row: dict[str, Any]) -> tuple[str, str]:
    """Return ``(identity_key, identity_kind)`` without title guessing."""
    video_id = str(row.get("video_id") or "").strip()
    if video_id:
        return f"id:{video_id}", "youtube"
    filepath = normalize_media_path(row.get("filepath"))
    if filepath:
        return f"path:{filepath}", "path"
    return f"legacy-row:{int(row['id'])}", "legacy"


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?",
        (name,),
    ).fetchone() is not None


def catalog_schema_installed(conn: sqlite3.Connection) -> bool:
    return all(
        _table_exists(conn, name)
        for name in ("catalog_state", "logical_videos", "media_files")
    )


def _catalog_rows_digest(
    conn: sqlite3.Connection,
    columns: list[str],
) -> tuple[int, str]:
    """Return a deterministic count/digest for the legacy video catalog."""
    quoted = ", ".join(f'"{name.replace(chr(34), chr(34) * 2)}"' for name in columns)
    digest = hashlib.sha256()
    count = 0
    cursor = conn.execute(f"SELECT {quoted} FROM videos ORDER BY id")
    while True:
        rows = cursor.fetchmany(1_000)
        if not rows:
            break
        for row in rows:
            payload = json.dumps(
                list(row), ensure_ascii=False, separators=(",", ":"), default=str,
            ).encode("utf-8")
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
            count += 1
    return count, digest.hexdigest()


def verify_legacy_catalog_backup(path: str | Path) -> dict[str, Any]:
    """Verify a pre-v5 catalog backup without changing either database."""
    backup_path = Path(path)
    if not backup_path.is_file():
        raise CatalogBackupError("catalog backup file is missing")
    conn = sqlite3.connect(str(backup_path))
    try:
        quick = conn.execute("PRAGMA quick_check").fetchone()
        if not quick or str(quick[0]).lower() != "ok":
            raise CatalogBackupError("catalog backup failed SQLite quick_check")
        meta = dict(conn.execute("SELECT key, value FROM backup_meta"))
        columns = [
            str(row[1]) for row in conn.execute("PRAGMA table_info(videos)")
        ]
        if not columns:
            raise CatalogBackupError("catalog backup has no videos table")
        count, digest = _catalog_rows_digest(conn, columns)
        if count != int(meta.get("source_rows", "-1")):
            raise CatalogBackupError("catalog backup row count does not match")
        if digest != meta.get("source_digest", ""):
            raise CatalogBackupError("catalog backup digest does not match")
        return {
            "path": str(backup_path),
            "rows": count,
            "digest": digest,
            "source_user_version": int(meta.get("source_user_version", "0")),
        }
    except (sqlite3.Error, ValueError) as exc:
        if isinstance(exc, CatalogBackupError):
            raise
        raise CatalogBackupError(f"catalog backup verification failed: {exc}") from exc
    finally:
        conn.close()


def create_verified_legacy_catalog_backup(
    source: sqlite3.Connection,
    path: str | Path,
) -> dict[str, Any]:
    """Create and verify an atomic backup of bookkeeping changed by v5.

    The normalized migration is additive and leaves transcripts untouched, so
    the legacy ``videos`` table is the complete rollback input.  An already
    verified backup is reused; a partial or corrupt file is never trusted.
    """
    backup_path = Path(path)
    if backup_path.exists():
        try:
            return verify_legacy_catalog_backup(backup_path)
        except CatalogBackupError:
            pass

    schema_row = source.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='videos'"
    ).fetchone()
    if not schema_row or not schema_row[0]:
        raise CatalogBackupError("legacy videos table is missing")
    columns = [str(row[1]) for row in source.execute("PRAGMA table_info(videos)")]
    if not columns:
        raise CatalogBackupError("legacy videos table has no columns")
    source_count, source_digest = _catalog_rows_digest(source, columns)
    source_version = int(source.execute("PRAGMA user_version").fetchone()[0])

    backup_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{backup_path.name}.", suffix=".tmp", dir=str(backup_path.parent),
    )
    os.close(fd)
    temp_path = Path(temp_name)
    dest: sqlite3.Connection | None = None
    try:
        dest = sqlite3.connect(str(temp_path))
        dest.execute(str(schema_row[0]))
        dest.execute(
            "CREATE TABLE backup_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        quoted = ", ".join(
            f'"{name.replace(chr(34), chr(34) * 2)}"' for name in columns
        )
        placeholders = ",".join("?" for _ in columns)
        select = source.execute(f"SELECT {quoted} FROM videos ORDER BY id")
        while True:
            rows = select.fetchmany(1_000)
            if not rows:
                break
            dest.executemany(
                f"INSERT INTO videos ({quoted}) VALUES ({placeholders})", rows,
            )
        dest.executemany(
            "INSERT INTO backup_meta(key, value) VALUES(?, ?)",
            (
                ("format", "ytarchiver-catalog-backup-v1"),
                ("source_user_version", str(source_version)),
                ("source_rows", str(source_count)),
                ("source_digest", source_digest),
                ("created_at", str(time.time())),
            ),
        )
        dest.commit()
        dest.close()
        dest = None
        receipt = verify_legacy_catalog_backup(temp_path)
        os.replace(temp_path, backup_path)
        receipt["path"] = str(backup_path)
        return receipt
    except (OSError, sqlite3.Error, CatalogBackupError) as exc:
        raise CatalogBackupError(f"could not create verified catalog backup: {exc}") from exc
    finally:
        if dest is not None:
            dest.close()
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass


def install_catalog_schema(conn: sqlite3.Connection) -> None:
    """Install the additive v2 schema and rollback-compatible dirty triggers."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS catalog_state (
               singleton INTEGER PRIMARY KEY CHECK(singleton=1),
               phase TEXT NOT NULL DEFAULT 'legacy',
               schema_version INTEGER NOT NULL DEFAULT 1,
               legacy_dirty INTEGER NOT NULL DEFAULT 1,
               comparison_digest TEXT NOT NULL DEFAULT '',
               compared_at REAL,
               last_error TEXT NOT NULL DEFAULT ''
           )"""
    )
    conn.execute(
        """INSERT INTO catalog_state(
               singleton, phase, schema_version, legacy_dirty)
           VALUES(1, 'schema_ready', ?, 1)
           ON CONFLICT(singleton) DO UPDATE SET
               schema_version=excluded.schema_version,
               phase=CASE
                   WHEN catalog_state.phase='legacy' THEN 'schema_ready'
                   ELSE catalog_state.phase END""",
        (CATALOG_SCHEMA_VERSION,),
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS logical_videos (
               logical_id INTEGER PRIMARY KEY,
               identity_key TEXT NOT NULL UNIQUE,
               identity_kind TEXT NOT NULL,
               video_id TEXT,
               title TEXT NOT NULL DEFAULT '',
               channel TEXT NOT NULL DEFAULT '',
               video_url TEXT,
               duration_s REAL,
               tx_status TEXT,
               upload_ts REAL,
               added_ts REAL,
               metadata_fetch_failed_ts REAL,
               metadata_fetch_fail_count INTEGER NOT NULL DEFAULT 0,
               removed_from_yt_ts REAL,
               view_count INTEGER,
               like_count INTEGER,
               legacy_canonical_row_id INTEGER,
               source_digest TEXT NOT NULL DEFAULT ''
           )"""
    )
    conn.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_logical_videos_video_id
           ON logical_videos(video_id)
           WHERE trim(COALESCE(video_id, '')) <> ''"""
    )
    conn.execute(
        """CREATE INDEX IF NOT EXISTS idx_logical_videos_chan_title_date
           ON logical_videos(channel, title, upload_ts)"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS media_files (
               media_id INTEGER PRIMARY KEY,
               logical_video_id INTEGER NOT NULL,
               legacy_video_row_id INTEGER NOT NULL UNIQUE,
               filepath TEXT NOT NULL UNIQUE COLLATE NOCASE,
               local_title TEXT NOT NULL DEFAULT '',
               archive_channel TEXT NOT NULL DEFAULT '',
               year INTEGER,
               month INTEGER,
               size_bytes INTEGER,
               duration_s REAL,
               observed_upload_ts REAL,
               downloaded_ts REAL,
               added_ts REAL,
               availability TEXT NOT NULL DEFAULT 'available',
               has_thumbnail INTEGER,
               search_failed_ts REAL,
               id_resolve_failed_ts REAL,
               id_backfill_tried_ts REAL,
               id_backfill_fail_count INTEGER NOT NULL DEFAULT 0,
               id_backfill_excluded_ts REAL,
               legacy_duplicate_of TEXT,
               is_primary INTEGER NOT NULL DEFAULT 0 CHECK(is_primary IN (0,1)),
               FOREIGN KEY(logical_video_id)
                   REFERENCES logical_videos(logical_id) ON DELETE CASCADE
           )"""
    )
    conn.execute(
        """CREATE INDEX IF NOT EXISTS idx_media_files_logical
           ON media_files(logical_video_id)"""
    )
    conn.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_media_files_one_primary
           ON media_files(logical_video_id) WHERE is_primary=1"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS catalog_dirty_keys (
               identity_key TEXT PRIMARY KEY
           )"""
    )
    # Ordinary dual-model commits look up only the identity groups touched by
    # the legacy write. Without this expression index SQLite scans the entire
    # videos table for every downloaded video (and again when downloaded_ts is
    # recorded), even though catalog_dirty_keys already names the exact group.
    conn.execute(
        """CREATE INDEX IF NOT EXISTS idx_videos_catalog_identity_v5
           ON videos(
             CASE
               WHEN trim(COALESCE(video_id, '')) <> ''
                 THEN 'id:' || trim(video_id)
               WHEN trim(COALESCE(filepath, '')) <> ''
                 THEN 'path:' || lower(replace(trim(filepath), '/', char(92)))
               ELSE 'legacy-row:' || CAST(id AS TEXT)
             END
           )"""
    )
    conn.execute(
        """CREATE VIRTUAL TABLE IF NOT EXISTS logical_videos_fts USING fts5(
               title,
               content=logical_videos,
               content_rowid=logical_id
           )"""
    )
    # Dirty-key triggers contain only backward-compatible SQLite statements.
    # Recreate them on open so an interrupted early Patch 5 preview cannot
    # leave an older trigger body installed under the same name.
    for trigger in (
        "catalog_legacy_dirty_ai_v5",
        "catalog_legacy_dirty_au_v5",
        "catalog_legacy_dirty_ad_v5",
        "catalog_segments_dirty_ai_v5",
        "catalog_segments_dirty_au_v5",
        "catalog_segments_dirty_ad_v5",
    ):
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger}")
    for statement in (
        """CREATE TRIGGER IF NOT EXISTS logical_videos_fts_ai_v5
           AFTER INSERT ON logical_videos BEGIN
             INSERT INTO logical_videos_fts(rowid, title)
             VALUES(new.logical_id, new.title);
           END""",
        """CREATE TRIGGER IF NOT EXISTS logical_videos_fts_ad_v5
           AFTER DELETE ON logical_videos BEGIN
             INSERT INTO logical_videos_fts(logical_videos_fts, rowid, title)
             VALUES('delete', old.logical_id, old.title);
           END""",
        """CREATE TRIGGER IF NOT EXISTS logical_videos_fts_au_v5
           AFTER UPDATE OF logical_id, title ON logical_videos BEGIN
             INSERT INTO logical_videos_fts(logical_videos_fts, rowid, title)
             VALUES('delete', old.logical_id, old.title);
             INSERT INTO logical_videos_fts(rowid, title)
             VALUES(new.logical_id, new.title);
           END""",
        # Compatibility contract for Patch 4 rollback: these triggers use
        # ordinary SQLite only and cannot make an old videos write fail.
        """CREATE TRIGGER IF NOT EXISTS catalog_legacy_dirty_ai_v5
           AFTER INSERT ON videos BEGIN
             INSERT INTO catalog_dirty_keys(identity_key)
             SELECT
               CASE
                 WHEN trim(COALESCE(new.video_id, '')) <> ''
                   THEN 'id:' || trim(new.video_id)
                 WHEN trim(COALESCE(new.filepath, '')) <> ''
                   THEN 'path:' || lower(replace(trim(new.filepath), '/', char(92)))
                 ELSE 'legacy-row:' || CAST(new.id AS TEXT)
               END
             WHERE NOT EXISTS (
               SELECT 1 FROM catalog_dirty_keys WHERE identity_key =
                 CASE
                   WHEN trim(COALESCE(new.video_id, '')) <> ''
                     THEN 'id:' || trim(new.video_id)
                   WHEN trim(COALESCE(new.filepath, '')) <> ''
                     THEN 'path:' || lower(replace(trim(new.filepath), '/', char(92)))
                   ELSE 'legacy-row:' || CAST(new.id AS TEXT)
                 END
             );
             UPDATE catalog_state SET legacy_dirty=1 WHERE singleton=1;
           END""",
        """CREATE TRIGGER IF NOT EXISTS catalog_legacy_dirty_au_v5
           AFTER UPDATE ON videos BEGIN
             INSERT INTO catalog_dirty_keys(identity_key)
             SELECT
               CASE
                 WHEN trim(COALESCE(old.video_id, '')) <> ''
                   THEN 'id:' || trim(old.video_id)
                 WHEN trim(COALESCE(old.filepath, '')) <> ''
                   THEN 'path:' || lower(replace(trim(old.filepath), '/', char(92)))
                 ELSE 'legacy-row:' || CAST(old.id AS TEXT)
               END
             WHERE NOT EXISTS (
               SELECT 1 FROM catalog_dirty_keys WHERE identity_key =
                 CASE
                   WHEN trim(COALESCE(old.video_id, '')) <> ''
                     THEN 'id:' || trim(old.video_id)
                   WHEN trim(COALESCE(old.filepath, '')) <> ''
                     THEN 'path:' || lower(replace(trim(old.filepath), '/', char(92)))
                   ELSE 'legacy-row:' || CAST(old.id AS TEXT)
                 END
             );
             INSERT INTO catalog_dirty_keys(identity_key)
             SELECT
               CASE
                 WHEN trim(COALESCE(new.video_id, '')) <> ''
                   THEN 'id:' || trim(new.video_id)
                 WHEN trim(COALESCE(new.filepath, '')) <> ''
                   THEN 'path:' || lower(replace(trim(new.filepath), '/', char(92)))
                 ELSE 'legacy-row:' || CAST(new.id AS TEXT)
               END
             WHERE NOT EXISTS (
               SELECT 1 FROM catalog_dirty_keys WHERE identity_key =
                 CASE
                   WHEN trim(COALESCE(new.video_id, '')) <> ''
                     THEN 'id:' || trim(new.video_id)
                   WHEN trim(COALESCE(new.filepath, '')) <> ''
                     THEN 'path:' || lower(replace(trim(new.filepath), '/', char(92)))
                   ELSE 'legacy-row:' || CAST(new.id AS TEXT)
                 END
             );
             UPDATE catalog_state SET legacy_dirty=1 WHERE singleton=1;
           END""",
        """CREATE TRIGGER IF NOT EXISTS catalog_legacy_dirty_ad_v5
           AFTER DELETE ON videos BEGIN
             INSERT INTO catalog_dirty_keys(identity_key)
             SELECT
               CASE
                 WHEN trim(COALESCE(old.video_id, '')) <> ''
                   THEN 'id:' || trim(old.video_id)
                 WHEN trim(COALESCE(old.filepath, '')) <> ''
                   THEN 'path:' || lower(replace(trim(old.filepath), '/', char(92)))
                 ELSE 'legacy-row:' || CAST(old.id AS TEXT)
               END
             WHERE NOT EXISTS (
               SELECT 1 FROM catalog_dirty_keys WHERE identity_key =
                 CASE
                   WHEN trim(COALESCE(old.video_id, '')) <> ''
                     THEN 'id:' || trim(old.video_id)
                   WHEN trim(COALESCE(old.filepath, '')) <> ''
                     THEN 'path:' || lower(replace(trim(old.filepath), '/', char(92)))
                   ELSE 'legacy-row:' || CAST(old.id AS TEXT)
                 END
             );
             UPDATE catalog_state SET legacy_dirty=1 WHERE singleton=1;
           END""",
        """CREATE TRIGGER IF NOT EXISTS catalog_segments_dirty_ai_v5
           AFTER INSERT ON segments
           WHEN trim(COALESCE(new.video_id, '')) <> '' BEGIN
             INSERT INTO catalog_dirty_keys(identity_key)
             SELECT 'id:' || trim(new.video_id)
             WHERE NOT EXISTS (
               SELECT 1 FROM catalog_dirty_keys
               WHERE identity_key='id:' || trim(new.video_id)
             );
             UPDATE catalog_state SET legacy_dirty=1 WHERE singleton=1;
           END""",
        """CREATE TRIGGER IF NOT EXISTS catalog_segments_dirty_au_v5
           AFTER UPDATE OF video_id, title, channel ON segments
           WHEN trim(COALESCE(old.video_id, '')) <> ''
             OR trim(COALESCE(new.video_id, '')) <> '' BEGIN
             INSERT INTO catalog_dirty_keys(identity_key)
             SELECT 'id:' || trim(old.video_id)
             WHERE trim(COALESCE(old.video_id, '')) <> ''
               AND NOT EXISTS (
                 SELECT 1 FROM catalog_dirty_keys
                 WHERE identity_key='id:' || trim(old.video_id)
               );
             INSERT INTO catalog_dirty_keys(identity_key)
             SELECT 'id:' || trim(new.video_id)
             WHERE trim(COALESCE(new.video_id, '')) <> ''
               AND NOT EXISTS (
                 SELECT 1 FROM catalog_dirty_keys
                 WHERE identity_key='id:' || trim(new.video_id)
               );
             UPDATE catalog_state SET legacy_dirty=1 WHERE singleton=1;
           END""",
        """CREATE TRIGGER IF NOT EXISTS catalog_segments_dirty_ad_v5
           AFTER DELETE ON segments
           WHEN trim(COALESCE(old.video_id, '')) <> '' BEGIN
             INSERT INTO catalog_dirty_keys(identity_key)
             SELECT 'id:' || trim(old.video_id)
             WHERE NOT EXISTS (
               SELECT 1 FROM catalog_dirty_keys
               WHERE identity_key='id:' || trim(old.video_id)
             );
             UPDATE catalog_state SET legacy_dirty=1 WHERE singleton=1;
           END""",
    ):
        conn.execute(statement)


def _rows_as_dicts(conn: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
    cursor = conn.execute(sql)
    names = [str(col[0]) for col in (cursor.description or ())]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _positive_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _canonical_sort_key(row: dict[str, Any]) -> tuple[int, int, int]:
    filepath = str(row.get("filepath") or "").strip()
    available = (
        str(row.get("availability") or "available") == "available"
        and bool(filepath)
    )
    primary_hint = row.get("is_duplicate_of") is None
    return (0 if available else 1, 0 if primary_hint else 1, int(row["id"]))


def _stable_digest(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _project_rows(
    videos: list[dict[str, Any]],
    segment_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    kinds: dict[str, str] = {}
    for row in videos:
        key, kind = identity_key_for_row(row)
        grouped.setdefault(key, []).append(row)
        kinds[key] = kind

    logical: list[dict[str, Any]] = []
    media: list[dict[str, Any]] = []
    for key in sorted(grouped):
        copies = sorted(grouped[key], key=_canonical_sort_key)
        canonical = copies[0]
        durations = [
            value for row in copies
            if (value := _positive_number(row.get("duration_s"))) is not None
        ]
        uploads = [
            float(row["upload_ts"]) for row in copies
            if row.get("upload_ts") is not None
        ]
        canonical_duration = _positive_number(canonical.get("duration_s"))
        duration = canonical_duration or (max(durations) if durations else None)
        upload = (float(canonical["upload_ts"])
                  if canonical.get("upload_ts") is not None
                  else (min(uploads) if uploads else None))
        video_id = str(canonical.get("video_id") or "").strip() or None
        logical_row = {
            "identity_key": key,
            "identity_kind": kinds[key],
            "video_id": video_id,
            "title": str(canonical.get("title") or ""),
            "channel": str(canonical.get("channel") or ""),
            "video_url": canonical.get("video_url"),
            "duration_s": duration,
            "tx_status": canonical.get("tx_status"),
            "upload_ts": upload,
            "added_ts": canonical.get("added_ts"),
            "metadata_fetch_failed_ts": canonical.get(
                "metadata_fetch_failed_ts"),
            "metadata_fetch_fail_count": 0,
            "removed_from_yt_ts": canonical.get("removed_from_yt_ts"),
            "view_count": canonical.get("view_count"),
            "like_count": canonical.get("like_count"),
            "legacy_canonical_row_id": int(canonical["id"]),
        }
        logical_row["source_digest"] = _stable_digest(logical_row)
        logical.append(logical_row)

        physical = [
            row for row in copies if str(row.get("filepath") or "").strip()
        ]
        primary_id = (
            int(canonical["id"])
            if str(canonical.get("filepath") or "").strip()
            else (int(physical[0]["id"]) if physical else None)
        )
        for row in physical:
            media.append({
                "identity_key": key,
                "legacy_video_row_id": int(row["id"]),
                "filepath": str(row["filepath"]).strip(),
                "local_title": str(row.get("title") or ""),
                "archive_channel": str(row.get("channel") or ""),
                "year": row.get("year"),
                "month": row.get("month"),
                "size_bytes": row.get("size_bytes"),
                "duration_s": row.get("duration_s"),
                "observed_upload_ts": row.get("upload_ts"),
                "downloaded_ts": row.get("downloaded_ts"),
                "added_ts": row.get("added_ts"),
                "availability": str(row.get("availability") or "available"),
                "has_thumbnail": row.get("has_thumbnail"),
                "search_failed_ts": row.get("search_failed_ts"),
                "id_resolve_failed_ts": row.get("id_resolve_failed_ts"),
                "id_backfill_tried_ts": row.get("id_backfill_tried_ts"),
                "id_backfill_fail_count": int(
                    row.get("id_backfill_fail_count") or 0),
                "id_backfill_excluded_ts": row.get(
                    "id_backfill_excluded_ts"),
                "legacy_duplicate_of": row.get("is_duplicate_of"),
                "is_primary": int(int(row["id"]) == primary_id),
            })

    known_ids = {str(row.get("video_id") or "") for row in logical}
    for segment in segment_rows:
        video_id = str(segment.get("video_id") or "").strip()
        if not video_id or video_id in known_ids:
            continue
        row = {
            "identity_key": f"id:{video_id}",
            "identity_kind": "youtube",
            "video_id": video_id,
            "title": str(segment.get("title") or ""),
            "channel": str(segment.get("channel") or ""),
            "video_url": None,
            "duration_s": None,
            "tx_status": "transcribed",
            "upload_ts": None,
            "added_ts": None,
            "metadata_fetch_failed_ts": None,
            "metadata_fetch_fail_count": 0,
            "removed_from_yt_ts": None,
            "view_count": None,
            "like_count": None,
            "legacy_canonical_row_id": None,
        }
        row["source_digest"] = _stable_digest(row)
        logical.append(row)

    logical.sort(key=lambda row: row["identity_key"])
    media.sort(key=lambda row: row["legacy_video_row_id"])
    return logical, media


def build_legacy_projection(
    conn: sqlite3.Connection,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build the full normalized projection using SQLite rows only."""
    if not _table_exists(conn, "videos"):
        raise CatalogMigrationError("legacy videos table is missing")
    videos = _rows_as_dicts(conn, "SELECT * FROM videos ORDER BY id")
    segment_rows: list[dict[str, Any]] = []
    if _table_exists(conn, "segments"):
        # Only transcript IDs that have no legacy video row need a synthetic
        # logical owner.  The old query applied trim() and GROUP BY to every
        # transcript row, forcing a full table scan plus a temporary B-tree.
        # On the real 38.5 GB / 54.8-million-segment catalog that made the
        # first Patch 5 open take tens of minutes before failing later in the
        # migration.  Read the narrow video_id index first, then fetch title /
        # channel text only for the usually-small set of transcript-only IDs.
        known_ids = {
            str(row.get("video_id") or "").strip()
            for row in videos
            if str(row.get("video_id") or "").strip()
        }
        raw_ids_by_clean: dict[str, list[str]] = {}
        for value, in conn.execute(
            """SELECT DISTINCT video_id FROM segments
               WHERE video_id IS NOT NULL AND video_id <> ''
               ORDER BY video_id"""
        ):
            raw_id = str(value or "")
            clean_id = raw_id.strip()
            if clean_id and clean_id not in known_ids:
                raw_ids_by_clean.setdefault(clean_id, []).append(raw_id)

        raw_ids = [
            raw_id
            for clean_id in sorted(raw_ids_by_clean)
            for raw_id in raw_ids_by_clean[clean_id]
        ]
        aggregate: dict[str, dict[str, str]] = {}
        for offset in range(0, len(raw_ids), 400):
            batch = raw_ids[offset:offset + 400]
            placeholders = ",".join("?" for _ in batch)
            cursor = conn.execute(
                f"""SELECT video_id,
                           MIN(COALESCE(title, '')) AS title,
                           MIN(COALESCE(channel, '')) AS channel
                    FROM segments
                    WHERE video_id IN ({placeholders})
                    GROUP BY video_id""",
                batch,
            )
            for raw_id, title, channel in cursor.fetchall():
                clean_id = str(raw_id or "").strip()
                if not clean_id or clean_id in known_ids:
                    continue
                clean_title = str(title or "")
                clean_channel = str(channel or "")
                current = aggregate.get(clean_id)
                if current is None:
                    aggregate[clean_id] = {
                        "video_id": clean_id,
                        "title": clean_title,
                        "channel": clean_channel,
                    }
                else:
                    current["title"] = min(current["title"], clean_title)
                    current["channel"] = min(
                        current["channel"], clean_channel)
        segment_rows = [aggregate[key] for key in sorted(aggregate)]
    return _project_rows(videos, segment_rows)


def _projection_for_keys(
    conn: sqlite3.Connection,
    keys: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build only changed identity groups for an ordinary small commit."""
    if not keys:
        return [], []
    placeholders = ",".join("?" for _ in keys)
    key_expr = (
        "CASE "
        "WHEN trim(COALESCE(video_id, '')) <> '' "
        "THEN 'id:' || trim(video_id) "
        "WHEN trim(COALESCE(filepath, '')) <> '' "
        "THEN 'path:' || lower(replace(trim(filepath), '/', char(92))) "
        "ELSE 'legacy-row:' || CAST(id AS TEXT) END"
    )
    cursor = conn.execute(
        f"SELECT * FROM videos WHERE {key_expr} IN ({placeholders}) ORDER BY id",
        keys,
    )
    names = [str(col[0]) for col in (cursor.description or ())]
    videos = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    segment_rows: list[dict[str, Any]] = []
    id_keys = [key[3:] for key in keys if key.startswith("id:")]
    if id_keys and _table_exists(conn, "segments"):
        id_placeholders = ",".join("?" for _ in id_keys)
        cursor = conn.execute(
            f"""SELECT trim(video_id) AS video_id,
                       MIN(COALESCE(title, '')) AS title,
                       MIN(COALESCE(channel, '')) AS channel
                FROM segments
                WHERE video_id IN ({id_placeholders})
                GROUP BY video_id
                ORDER BY video_id""",
            id_keys,
        )
        names = [str(col[0]) for col in (cursor.description or ())]
        segment_rows = [
            dict(zip(names, row, strict=True)) for row in cursor.fetchall()
        ]
    return _project_rows(videos, segment_rows)


_LOGICAL_FIELDS = (
    "identity_key", "identity_kind", "video_id", "title", "channel",
    "video_url", "duration_s", "tx_status", "upload_ts", "added_ts",
    "metadata_fetch_failed_ts", "metadata_fetch_fail_count",
    "removed_from_yt_ts", "view_count", "like_count",
    "legacy_canonical_row_id", "source_digest",
)
_MEDIA_FIELDS = (
    "legacy_video_row_id", "filepath", "local_title", "archive_channel",
    "year", "month", "size_bytes", "duration_s", "observed_upload_ts",
    "downloaded_ts", "added_ts", "availability", "has_thumbnail",
    "search_failed_ts", "id_resolve_failed_ts", "id_backfill_tried_ts",
    "id_backfill_fail_count", "id_backfill_excluded_ts",
    "legacy_duplicate_of", "is_primary",
)


def _projection_digest(
    logical: list[dict[str, Any]], media: list[dict[str, Any]],
) -> str:
    clean_logical = [
        {field: row.get(field) for field in _LOGICAL_FIELDS}
        for row in logical
    ]
    clean_media = [
        {field: row.get(field) for field in _MEDIA_FIELDS}
        for row in media
    ]
    return _stable_digest({"logical": clean_logical, "media": clean_media})


def _read_v2_projection(
    conn: sqlite3.Connection,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    logical = _rows_as_dicts(
        conn,
        """SELECT identity_key, identity_kind, video_id, title, channel,
                  video_url, duration_s, tx_status, upload_ts, added_ts,
                  metadata_fetch_failed_ts, metadata_fetch_fail_count,
                  removed_from_yt_ts, view_count, like_count,
                  legacy_canonical_row_id, source_digest
           FROM logical_videos ORDER BY identity_key""",
    )
    media = _rows_as_dicts(
        conn,
        """SELECT lv.identity_key, mf.legacy_video_row_id, mf.filepath,
                  mf.local_title, mf.archive_channel, mf.year, mf.month,
                  mf.size_bytes, mf.duration_s, mf.observed_upload_ts,
                  mf.downloaded_ts, mf.added_ts, mf.availability,
                  mf.has_thumbnail, mf.search_failed_ts,
                  mf.id_resolve_failed_ts, mf.id_backfill_tried_ts,
                  mf.id_backfill_fail_count, mf.id_backfill_excluded_ts,
                  mf.legacy_duplicate_of, mf.is_primary
           FROM media_files mf
           JOIN logical_videos lv ON lv.logical_id=mf.logical_video_id
           ORDER BY mf.legacy_video_row_id""",
    )
    return logical, media


def _upsert_logical_row(
    conn: sqlite3.Connection, row: dict[str, Any],
) -> int:
    conn.execute(
        """INSERT INTO logical_videos(
               identity_key, identity_kind, video_id, title, channel,
               video_url, duration_s, tx_status, upload_ts, added_ts,
               metadata_fetch_failed_ts, metadata_fetch_fail_count,
               removed_from_yt_ts, view_count, like_count,
               legacy_canonical_row_id, source_digest)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(identity_key) DO UPDATE SET
               identity_kind=excluded.identity_kind,
               video_id=excluded.video_id,
               title=excluded.title,
               channel=excluded.channel,
               video_url=excluded.video_url,
               duration_s=excluded.duration_s,
               tx_status=excluded.tx_status,
               upload_ts=excluded.upload_ts,
               added_ts=excluded.added_ts,
               metadata_fetch_failed_ts=excluded.metadata_fetch_failed_ts,
               metadata_fetch_fail_count=excluded.metadata_fetch_fail_count,
               removed_from_yt_ts=excluded.removed_from_yt_ts,
               view_count=excluded.view_count,
               like_count=excluded.like_count,
               legacy_canonical_row_id=excluded.legacy_canonical_row_id,
               source_digest=excluded.source_digest""",
        tuple(row[field] for field in _LOGICAL_FIELDS),
    )
    result = conn.execute(
        "SELECT logical_id FROM logical_videos WHERE identity_key=?",
        (row["identity_key"],),
    ).fetchone()
    if result is None:
        raise CatalogMigrationError(
            f"logical identity was not persisted: {row['identity_key']}")
    return int(result[0])


def _upsert_media_row(
    conn: sqlite3.Connection,
    row: dict[str, Any],
    logical_id: int,
) -> None:
    values = (
        logical_id,
        *(row[field] for field in _MEDIA_FIELDS if field != "is_primary"),
        0,
    )
    conn.execute(
        """INSERT INTO media_files(
               logical_video_id, legacy_video_row_id, filepath,
               local_title, archive_channel, year, month, size_bytes,
               duration_s, observed_upload_ts, downloaded_ts, added_ts,
               availability, has_thumbnail, search_failed_ts,
               id_resolve_failed_ts, id_backfill_tried_ts,
               id_backfill_fail_count, id_backfill_excluded_ts,
               legacy_duplicate_of, is_primary)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(legacy_video_row_id) DO UPDATE SET
               logical_video_id=excluded.logical_video_id,
               filepath=excluded.filepath,
               local_title=excluded.local_title,
               archive_channel=excluded.archive_channel,
               year=excluded.year,
               month=excluded.month,
               size_bytes=excluded.size_bytes,
               duration_s=excluded.duration_s,
               observed_upload_ts=excluded.observed_upload_ts,
               downloaded_ts=excluded.downloaded_ts,
               added_ts=excluded.added_ts,
               availability=excluded.availability,
               has_thumbnail=excluded.has_thumbnail,
               search_failed_ts=excluded.search_failed_ts,
               id_resolve_failed_ts=excluded.id_resolve_failed_ts,
               id_backfill_tried_ts=excluded.id_backfill_tried_ts,
               id_backfill_fail_count=excluded.id_backfill_fail_count,
               id_backfill_excluded_ts=excluded.id_backfill_excluded_ts,
               legacy_duplicate_of=excluded.legacy_duplicate_of,
               is_primary=0""",
        values,
    )


def reconcile_catalog(
    conn: sqlite3.Connection,
    *,
    fail_after_phase: str = "",
) -> CatalogStatus:
    """Copy, compare, and enable v2 inside the caller's transaction.

    ``fail_after_phase`` exists only for deterministic migration failure tests.
    A savepoint guarantees that any failure restores the prior normalized
    projection; ``CatalogConnection.commit`` additionally rolls back the
    legacy half of a dual write.
    """
    install_catalog_schema(conn)
    logical, media = build_legacy_projection(conn)
    conn.execute("SAVEPOINT catalog_reconcile_v5")
    try:
        conn.execute(
            "UPDATE catalog_state SET phase='schema_ready', last_error='' "
            "WHERE singleton=1"
        )
        if fail_after_phase == "schema_ready":
            raise CatalogMigrationError("injected failure after schema_ready")

        # This is a full rebuild, so replace the normalized projection instead
        # of upserting everything and then deleting stale rows with one SQL
        # placeholder per legacy video.  Large libraries exceed the packaged
        # SQLite variable limit (the live 110,530-row archive failed with
        # "too many SQL variables").  The savepoint still restores the prior
        # projection atomically if any later comparison or FTS check fails.
        conn.execute("DELETE FROM media_files")
        conn.execute("DELETE FROM logical_videos")
        logical_ids: dict[str, int] = {}
        for row in logical:
            logical_ids[row["identity_key"]] = _upsert_logical_row(conn, row)

        for row in media:
            _upsert_media_row(conn, row, logical_ids[row["identity_key"]])

        for row in media:
            if row["is_primary"]:
                conn.execute(
                    "UPDATE media_files SET is_primary=1 "
                    "WHERE legacy_video_row_id=?",
                    (int(row["legacy_video_row_id"]),),
                )
        conn.execute(
            "UPDATE catalog_state SET phase='copied' WHERE singleton=1")
        if fail_after_phase == "copied":
            raise CatalogMigrationError("injected failure after copied")

        actual_logical, actual_media = _read_v2_projection(conn)
        expected_digest = _projection_digest(logical, media)
        actual_digest = _projection_digest(actual_logical, actual_media)
        if expected_digest != actual_digest:
            raise CatalogMigrationError(
                "logical/media comparison failed; normalized reads remain disabled"
            )
        conn.execute(
            """INSERT INTO logical_videos_fts(logical_videos_fts)
               VALUES('rebuild')"""
        )
        conn.execute(
            """INSERT INTO logical_videos_fts(logical_videos_fts, rank)
               VALUES('integrity-check', 1)"""
        )
        now = time.time()
        conn.execute(
            """UPDATE catalog_state
               SET phase='compared', comparison_digest=?, compared_at=?,
                   last_error=''
               WHERE singleton=1""",
            (expected_digest, now),
        )
        if fail_after_phase == "compared":
            raise CatalogMigrationError("injected failure after compared")
        conn.execute(
            "UPDATE catalog_state SET phase='v2_reads' WHERE singleton=1")
        if fail_after_phase == "v2_reads":
            raise CatalogMigrationError("injected failure after v2_reads")
        conn.execute(
            """UPDATE catalog_state
               SET phase='v2_writes', legacy_dirty=0,
                   schema_version=?, last_error=''
               WHERE singleton=1""",
            (CATALOG_SCHEMA_VERSION,),
        )
        conn.execute("DELETE FROM catalog_dirty_keys")
        conn.execute("RELEASE SAVEPOINT catalog_reconcile_v5")
    except BaseException:
        conn.execute("ROLLBACK TO SAVEPOINT catalog_reconcile_v5")
        conn.execute("RELEASE SAVEPOINT catalog_reconcile_v5")
        raise
    return catalog_status(conn)


def _read_v2_projection_for_keys(
    conn: sqlite3.Connection,
    keys: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not keys:
        return [], []
    placeholders = ",".join("?" for _ in keys)
    cursor = conn.execute(
        f"""SELECT identity_key, identity_kind, video_id, title, channel,
                   video_url, duration_s, tx_status, upload_ts, added_ts,
                   metadata_fetch_failed_ts, metadata_fetch_fail_count,
                   removed_from_yt_ts, view_count, like_count,
                   legacy_canonical_row_id, source_digest
            FROM logical_videos
            WHERE identity_key IN ({placeholders})
            ORDER BY identity_key""",
        keys,
    )
    names = [str(col[0]) for col in (cursor.description or ())]
    logical = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    cursor = conn.execute(
        f"""SELECT lv.identity_key, mf.legacy_video_row_id, mf.filepath,
                   mf.local_title, mf.archive_channel, mf.year, mf.month,
                   mf.size_bytes, mf.duration_s, mf.observed_upload_ts,
                   mf.downloaded_ts, mf.added_ts, mf.availability,
                   mf.has_thumbnail, mf.search_failed_ts,
                   mf.id_resolve_failed_ts, mf.id_backfill_tried_ts,
                   mf.id_backfill_fail_count, mf.id_backfill_excluded_ts,
                   mf.legacy_duplicate_of, mf.is_primary
            FROM media_files mf
            JOIN logical_videos lv ON lv.logical_id=mf.logical_video_id
            WHERE lv.identity_key IN ({placeholders})
            ORDER BY mf.legacy_video_row_id""",
        keys,
    )
    names = [str(col[0]) for col in (cursor.description or ())]
    media = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    return logical, media


def reconcile_dirty_catalog(
    conn: sqlite3.Connection,
    *,
    max_incremental_keys: int = 512,
) -> None:
    """Reconcile identity groups touched by ordinary writes in safe batches."""
    state = conn.execute(
        "SELECT phase, legacy_dirty FROM catalog_state WHERE singleton=1"
    ).fetchone()
    if state is None or str(state[0]) != "v2_writes":
        reconcile_catalog(conn)
        return
    if not bool(state[1]):
        return
    keys = [
        str(row[0])
        for row in conn.execute(
            "SELECT identity_key FROM catalog_dirty_keys ORDER BY identity_key"
        )
    ]
    if not keys:
        # A dirty state without trigger-owned keys predates the incremental
        # contract, so only a full comparison can prove equivalence.
        reconcile_catalog(conn)
        return

    batch_size = max(1, int(max_incremental_keys))
    try:
        batch_size = min(
            batch_size,
            max(1, int(conn.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER))),
        )
    except (AttributeError, TypeError, ValueError):
        pass

    conn.execute("SAVEPOINT catalog_incremental_v5")
    try:
        previous = conn.execute(
            "SELECT comparison_digest FROM catalog_state WHERE singleton=1"
        ).fetchone()
        changed_digests: list[str] = []
        for offset in range(0, len(keys), batch_size):
            batch = keys[offset:offset + batch_size]
            logical, media = _projection_for_keys(conn, batch)
            logical_by_key = {row["identity_key"]: row for row in logical}
            media_by_key: dict[str, list[dict[str, Any]]] = {}
            for row in media:
                media_by_key.setdefault(row["identity_key"], []).append(row)

            for key in batch:
                current = conn.execute(
                    "SELECT logical_id FROM logical_videos WHERE identity_key=?",
                    (key,),
                ).fetchone()
                expected = logical_by_key.get(key)
                if expected is None:
                    if current is not None:
                        conn.execute(
                            "DELETE FROM media_files WHERE logical_video_id=?",
                            (int(current[0]),),
                        )
                        conn.execute(
                            "DELETE FROM logical_videos WHERE logical_id=?",
                            (int(current[0]),),
                        )
                    continue

                logical_id = _upsert_logical_row(conn, expected)
                # Replace this one identity's media set. This cannot exceed
                # SQLite's variable limit and stale copies disappear without
                # a giant NOT IN list.
                conn.execute(
                    "DELETE FROM media_files WHERE logical_video_id=?",
                    (logical_id,),
                )
                expected_media = media_by_key.get(key, [])
                for row in expected_media:
                    _upsert_media_row(conn, row, logical_id)
                for row in expected_media:
                    if row["is_primary"]:
                        conn.execute(
                            "UPDATE media_files SET is_primary=1 "
                            "WHERE legacy_video_row_id=?",
                            (int(row["legacy_video_row_id"]),),
                        )

            actual_logical, actual_media = _read_v2_projection_for_keys(
                conn, batch)
            expected_digest = _projection_digest(logical, media)
            actual_digest = _projection_digest(actual_logical, actual_media)
            if expected_digest != actual_digest:
                raise CatalogMigrationError(
                    "incremental logical/media comparison failed")
            changed_digests.append(expected_digest)
            placeholders = ",".join("?" for _ in batch)
            conn.execute(
                f"DELETE FROM catalog_dirty_keys WHERE identity_key IN "
                f"({placeholders})",
                batch,
            )

        transition_digest = _stable_digest({
            "previous": str(previous[0] if previous else ""),
            "changed": changed_digests,
            "keys": keys,
        })
        remaining = int(
            conn.execute("SELECT COUNT(*) FROM catalog_dirty_keys").fetchone()[0]
        )
        conn.execute(
            """UPDATE catalog_state
               SET legacy_dirty=?, comparison_digest=?, compared_at=?,
                   last_error=''
               WHERE singleton=1""",
            (int(remaining > 0), transition_digest, time.time()),
        )
        conn.execute("RELEASE SAVEPOINT catalog_incremental_v5")
    except BaseException:
        conn.execute("ROLLBACK TO SAVEPOINT catalog_incremental_v5")
        conn.execute("RELEASE SAVEPOINT catalog_incremental_v5")
        raise


def catalog_status(conn: sqlite3.Connection) -> CatalogStatus:
    if not catalog_schema_installed(conn):
        return CatalogStatus("legacy", 0, True, "", None, "", 0, 0)
    row = conn.execute(
        """SELECT phase, schema_version, legacy_dirty, comparison_digest,
                  compared_at, last_error
           FROM catalog_state WHERE singleton=1"""
    ).fetchone()
    if row is None:
        return CatalogStatus("legacy", 0, True, "", None, "", 0, 0)
    logical_count = int(
        conn.execute("SELECT COUNT(*) FROM logical_videos").fetchone()[0])
    media_count = int(
        conn.execute("SELECT COUNT(*) FROM media_files").fetchone()[0])
    return CatalogStatus(
        phase=str(row[0]),
        schema_version=int(row[1]),
        legacy_dirty=bool(row[2]),
        comparison_digest=str(row[3] or ""),
        compared_at=float(row[4]) if row[4] is not None else None,
        last_error=str(row[5] or ""),
        logical_videos=logical_count,
        media_files=media_count,
    )


def normalized_reads_enabled(conn: sqlite3.Connection) -> bool:
    try:
        return catalog_status(conn).reads_enabled
    except sqlite3.Error:
        return False


def mark_catalog_legacy_for_rollback(
    conn: sqlite3.Connection, reason: str = "manual rollback",
) -> None:
    """Switch reads back to the untouched legacy model without deleting v2."""
    install_catalog_schema(conn)
    conn.execute(
        """UPDATE catalog_state
           SET phase='legacy', legacy_dirty=1, last_error=?
           WHERE singleton=1""",
        (str(reason or "manual rollback"),),
    )


class CatalogConnection(sqlite3.Connection):
    """Writer connection whose commit point atomically maintains both models."""

    _catalog_reconciling = False
    _catalog_skip_reconcile = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # ``check_same_thread=False`` connections are shared by the app's
        # workers.  A second commit must not end the first commit's catalog
        # savepoint while its normalized projection is still being verified.
        self._catalog_transaction_lock = threading.RLock()

    def __enter__(self) -> CatalogConnection:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        return False

    def commit(self) -> None:
        with self._catalog_transaction_lock:
            if self._catalog_reconciling:
                # A same-thread nested commit is already covered by the outer
                # catalog commit.  Calling SQLite here would release its
                # active savepoint before reconciliation has finished.
                return
            if (
                not self._catalog_skip_reconcile
                and catalog_schema_installed(self)
            ):
                self._catalog_reconciling = True
                try:
                    reconcile_dirty_catalog(self)
                except BaseException:
                    super().rollback()
                    raise
                finally:
                    self._catalog_reconciling = False
            super().commit()

    def rollback(self) -> None:
        with self._catalog_transaction_lock:
            if self._catalog_reconciling:
                raise CatalogMigrationError(
                    "cannot roll back during catalog reconciliation"
                )
            super().rollback()


__all__ = [
    "CATALOG_PHASES",
    "CATALOG_SCHEMA_VERSION",
    "CatalogBackupError",
    "CatalogConnection",
    "CatalogMigrationError",
    "CatalogStatus",
    "build_legacy_projection",
    "catalog_schema_installed",
    "catalog_status",
    "create_verified_legacy_catalog_backup",
    "identity_key_for_row",
    "install_catalog_schema",
    "mark_catalog_legacy_for_rollback",
    "normalize_media_path",
    "normalized_reads_enabled",
    "reconcile_catalog",
    "reconcile_dirty_catalog",
    "verify_legacy_catalog_backup",
]
