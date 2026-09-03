"""Patch 5 logical-video / physical-media migration invariants."""

from __future__ import annotations

import sqlite3
import threading
from unittest import mock

import pytest

from backend.catalog_repository import (
    CatalogBackupError,
    CatalogConnection,
    CatalogMigrationError,
    catalog_status,
    create_verified_legacy_catalog_backup,
    install_catalog_schema,
    normalize_media_path,
    normalized_reads_enabled,
    reconcile_catalog,
    verify_legacy_catalog_backup,
)

_VIDEO_SCHEMA = """CREATE TABLE videos (
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
    id_backfill_excluded_ts REAL,
    search_failed_ts REAL,
    id_resolve_failed_ts REAL,
    metadata_fetch_failed_ts REAL,
    is_duplicate_of TEXT,
    upload_ts REAL,
    id_backfill_tried_ts REAL,
    removed_from_yt_ts REAL,
    has_thumbnail INTEGER,
    view_count INTEGER,
    like_count INTEGER,
    downloaded_ts REAL,
    availability TEXT DEFAULT 'available'
)"""
_OPEN_CONNECTIONS: list[sqlite3.Connection] = []


@pytest.fixture(autouse=True)
def _close_catalog_connections():
    yield
    while _OPEN_CONNECTIONS:
        _OPEN_CONNECTIONS.pop().close()


def _connection(
    path: str = ":memory:", *, check_same_thread: bool = True,
) -> CatalogConnection:
    conn = sqlite3.connect(
        path,
        factory=CatalogConnection,
        check_same_thread=check_same_thread,
    )
    _OPEN_CONNECTIONS.append(conn)
    conn.execute(_VIDEO_SCHEMA)
    conn.execute("""CREATE TABLE segments (
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
    install_catalog_schema(conn)
    return conn


def _insert_video(
    conn: sqlite3.Connection,
    row_id: int,
    *,
    title: str = "Example",
    filepath: str | None = None,
    video_id: str | None = None,
    duplicate_of: str | None = None,
    availability: str = "available",
    duration: float | None = 10.0,
) -> None:
    conn.execute(
        """INSERT INTO videos(
               id,title,channel,filepath,video_id,duration_s,size_bytes,
               tx_status,is_duplicate_of,availability)
           VALUES(?,?,?,?,?,?,100,'transcribed',?,?)""",
        (
            row_id,
            title,
            "Example Channel",
            filepath,
            video_id,
            duration,
            duplicate_of,
            availability,
        ),
    )


def _finish(conn: sqlite3.Connection) -> None:
    reconcile_catalog(conn)
    sqlite3.Connection.commit(conn)


def test_two_physical_copies_become_one_logical_video():
    conn = _connection()
    _insert_video(conn, 1, filepath=r"D:\Archive\one.mp4", video_id="abcdefghijk")
    _insert_video(
        conn,
        2,
        filepath=r"D:\Archive\one-copy.mp4",
        video_id="abcdefghijk",
        duplicate_of=r"D:\Archive\one.mp4",
    )

    _finish(conn)

    assert conn.execute("SELECT COUNT(*) FROM logical_videos").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM media_files").fetchone()[0] == 2
    assert conn.execute(
        "SELECT COUNT(*) FROM media_files WHERE is_primary=1"
    ).fetchone()[0] == 1
    assert catalog_status(conn).phase == "v2_writes"


def test_same_title_without_ids_never_merges_by_title():
    conn = _connection()
    _insert_video(conn, 1, title="Same", filepath=r"D:\A\same.mp4")
    _insert_video(conn, 2, title="Same", filepath=r"D:\B\same.mp4")

    _finish(conn)

    keys = [
        row[0]
        for row in conn.execute(
            "SELECT identity_key FROM logical_videos ORDER BY identity_key"
        )
    ]
    assert len(keys) == 2
    assert all(key.startswith("path:") for key in keys)


def test_idless_unicode_path_matches_legacy_sql_identity_on_commit():
    conn = _connection()
    _finish(conn)
    filepath = " D:/\u00c9XAMPLE//VIDEO.MP4 "

    _insert_video(conn, 1, filepath=filepath)
    conn.commit()

    expected_key = "path:d:\\\u00c9xample\\\\video.mp4"
    assert normalize_media_path(filepath) == expected_key.removeprefix("path:")
    assert conn.execute(
        "SELECT identity_key FROM logical_videos"
    ).fetchone() == (expected_key,)
    assert conn.execute(
        "SELECT COUNT(*) FROM catalog_dirty_keys"
    ).fetchone()[0] == 0
    assert normalized_reads_enabled(conn)


def test_segment_only_video_gets_logical_transcript_owner():
    conn = _connection()
    conn.execute(
        """INSERT INTO segments(
               video_id,title,channel,start_time,end_time,text)
           VALUES('segmentonly','Transcript only','Example Channel',0,1,'words')"""
    )

    _finish(conn)

    assert conn.execute(
        "SELECT video_id,tx_status FROM logical_videos"
    ).fetchone() == ("segmentonly", "transcribed")
    assert conn.execute("SELECT COUNT(*) FROM media_files").fetchone()[0] == 0


def test_segment_text_update_does_not_dirty_or_rebuild_logical_catalog():
    conn = _connection()
    conn.execute(
        """INSERT INTO segments(
               video_id,title,channel,start_time,end_time,text)
           VALUES('textupdate1','Transcript only','Example Channel',0,1,'old')"""
    )
    _finish(conn)

    conn.execute("UPDATE segments SET text='new' WHERE video_id='textupdate1'")

    # Transcript text is not part of the normalized logical/media projection.
    # Marking this write dirty used to leave no key and therefore force a full
    # legacy projection scan at commit time.
    assert conn.execute(
        "SELECT legacy_dirty FROM catalog_state WHERE singleton=1"
    ).fetchone() == (0,)
    assert conn.execute("SELECT COUNT(*) FROM catalog_dirty_keys").fetchone() == (0,)
    conn.commit()
    assert normalized_reads_enabled(conn)


def test_video_upsert_can_rewrite_the_same_identity_without_dirty_key_collision():
    conn = _connection()
    _finish(conn)
    sql = """INSERT INTO videos(
                 title,channel,filepath,video_id,size_bytes,tx_status)
             VALUES(?,?,?,?,100,'transcribed')
             ON CONFLICT(filepath) DO UPDATE SET
               title=excluded.title,
               channel=excluded.channel,
               video_id=excluded.video_id"""

    conn.execute(sql, (
        "Original", "Example Channel", r"D:\Archive\same.mp4", "samevideo01",
    ))
    conn.commit()
    conn.execute(sql, (
        "Retitled", "Example Channel", r"D:\Archive\same.mp4", "samevideo01",
    ))
    conn.commit()

    assert conn.execute(
        "SELECT title,video_id FROM logical_videos"
    ).fetchone() == ("Retitled", "samevideo01")
    assert conn.execute("SELECT COUNT(*) FROM catalog_dirty_keys").fetchone() == (0,)
    assert normalized_reads_enabled(conn)


def test_full_projection_reads_segment_text_only_for_transcript_only_ids():
    conn = _connection()
    conn.execute("CREATE INDEX idx_seg_video_id ON segments(video_id)")
    _insert_video(
        conn, 1, filepath=r"D:\known.mp4", video_id="knownvideo1",
    )
    conn.executemany(
        """INSERT INTO segments(video_id,title,channel,text)
           VALUES(?,?,?,'words')""",
        [
            ("knownvideo1", "Known transcript", "Known Channel"),
            (" segmentonly ", "Transcript only", "Example Channel"),
            ("segmentonly", "Transcript only", "Example Channel"),
        ],
    )
    statements: list[str] = []
    conn.set_trace_callback(statements.append)

    _finish(conn)

    conn.set_trace_callback(None)
    assert conn.execute(
        "SELECT video_id,tx_status FROM logical_videos ORDER BY video_id"
    ).fetchall() == [
        ("knownvideo1", "transcribed"),
        ("segmentonly", "transcribed"),
    ]
    segment_selects = [
        sql for sql in statements
        if "FROM segments" in sql and sql.lstrip().upper().startswith("SELECT")
    ]
    assert any("SELECT DISTINCT video_id" in sql for sql in segment_selects)
    assert not any("GROUP BY trim(video_id)" in sql for sql in segment_selects)


def test_full_reconcile_stays_below_packaged_sql_variable_limit():
    conn = _connection()
    if not hasattr(conn, "setlimit"):
        pytest.skip("sqlite connection limits are unavailable")
    conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 32)
    for row_id in range(1, 81):
        _insert_video(
            conn,
            row_id,
            filepath=rf"D:\Archive\video-{row_id}.mp4",
            video_id=f"limit{row_id:06d}",
        )

    _finish(conn)

    assert catalog_status(conn).logical_videos == 80
    assert catalog_status(conn).media_files == 80
    assert normalized_reads_enabled(conn)


def test_large_incremental_commit_is_batched_below_sql_variable_limit():
    conn = _connection()
    _finish(conn)
    if not hasattr(conn, "setlimit"):
        pytest.skip("sqlite connection limits are unavailable")
    conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 32)
    for row_id in range(1, 81):
        _insert_video(
            conn,
            row_id,
            filepath=rf"D:\Incremental\video-{row_id}.mp4",
            video_id=f"batch{row_id:06d}",
        )

    conn.commit()

    assert catalog_status(conn).logical_videos == 80
    assert catalog_status(conn).media_files == 80
    assert conn.execute(
        "SELECT COUNT(*) FROM catalog_dirty_keys"
    ).fetchone()[0] == 0
    assert normalized_reads_enabled(conn)


def test_incremental_identity_lookup_uses_expression_index():
    conn = _connection()
    key_expr = (
        "CASE "
        "WHEN trim(COALESCE(video_id, '')) <> '' "
        "THEN 'id:' || trim(video_id) "
        "WHEN trim(COALESCE(filepath, '')) <> '' "
        "THEN 'path:' || lower(replace(trim(filepath), '/', char(92))) "
        "ELSE 'legacy-row:' || CAST(id AS TEXT) END"
    )

    plan = conn.execute(
        f"EXPLAIN QUERY PLAN SELECT * FROM videos WHERE {key_expr} IN (?)",
        ("id:lookupvid01",),
    ).fetchall()

    assert any(
        "idx_videos_catalog_identity_v5" in str(row)
        for row in plan
    )


def test_canonical_precedence_prefers_available_then_primary_then_lowest_id():
    conn = _connection()
    _insert_video(
        conn,
        1,
        title="Missing primary",
        filepath=r"D:\missing.mp4",
        video_id="precedence1",
        availability="missing",
    )
    _insert_video(
        conn,
        2,
        title="Available duplicate",
        filepath=r"D:\available.mp4",
        video_id="precedence1",
        duplicate_of=r"D:\missing.mp4",
    )

    _finish(conn)

    assert conn.execute(
        "SELECT title,legacy_canonical_row_id FROM logical_videos"
    ).fetchone() == ("Available duplicate", 2)
    assert conn.execute(
        "SELECT legacy_video_row_id FROM media_files WHERE is_primary=1"
    ).fetchone()[0] == 2


def test_patch4_style_legacy_write_marks_dirty_then_reconciles():
    conn = _connection()
    _insert_video(conn, 1, filepath=r"D:\one.mp4", video_id="legacywrite")
    _finish(conn)
    assert normalized_reads_enabled(conn)

    conn._catalog_skip_reconcile = True
    _insert_video(
        conn, 2, filepath=r"D:\two.mp4", video_id="legacywrite",
        duplicate_of=r"D:\one.mp4",
    )
    sqlite3.Connection.commit(conn)
    conn._catalog_skip_reconcile = False

    assert catalog_status(conn).legacy_dirty
    assert not normalized_reads_enabled(conn)
    _finish(conn)
    assert catalog_status(conn).media_files == 2
    assert normalized_reads_enabled(conn)


def test_user_version_rollback_does_not_remove_or_duplicate_v2():
    conn = _connection()
    _insert_video(conn, 1, filepath=r"D:\one.mp4", video_id="rollbackid1")
    _finish(conn)
    conn.execute("PRAGMA user_version=4")
    install_catalog_schema(conn)
    _finish(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 4
    assert catalog_status(conn).logical_videos == 1
    assert catalog_status(conn).media_files == 1


def test_failed_comparison_stage_rolls_normalized_projection_back():
    conn = _connection()
    _insert_video(conn, 1, filepath=r"D:\one.mp4", video_id="stablemodel")
    _finish(conn)
    before = conn.execute(
        "SELECT identity_key,title FROM logical_videos"
    ).fetchall()

    conn.execute("UPDATE videos SET title='Changed but not committed' WHERE id=1")
    with pytest.raises(CatalogMigrationError, match="injected failure"):
        reconcile_catalog(conn, fail_after_phase="copied")

    assert conn.execute(
        "SELECT identity_key,title FROM logical_videos"
    ).fetchall() == before
    conn.rollback()


def test_old_plain_sqlite_writer_stays_compatible(tmp_path):
    db = tmp_path / "catalog.db"
    conn = _connection(str(db))
    _finish(conn)
    conn.close()

    old = sqlite3.connect(db)
    old.execute(_VIDEO_SCHEMA.replace("CREATE TABLE videos", "CREATE TABLE IF NOT EXISTS videos"))
    old.execute(
        "INSERT INTO videos(id,title,channel,filepath) VALUES(1,'Old','C','D:/old.mp4')"
    )
    old.execute("UPDATE videos SET title='Old updated' WHERE id=1")
    old.commit()
    assert old.execute(
        "SELECT legacy_dirty FROM catalog_state WHERE singleton=1"
    ).fetchone()[0] == 1
    old.execute("DELETE FROM videos WHERE id=1")
    old.commit()
    old.close()


def test_logical_fts_does_not_keep_tokens_after_row_reuse():
    conn = _connection()
    _insert_video(
        conn, 1, title="OldUniqueToken", filepath=r"D:\old.mp4",
        video_id="ftslogical1",
    )
    _finish(conn)
    conn.execute("DELETE FROM videos WHERE id=1")
    _finish(conn)
    _insert_video(
        conn, 1, title="NewDifferentTitle", filepath=r"D:\new.mp4",
        video_id="ftslogical2",
    )
    _finish(conn)

    assert conn.execute(
        "SELECT COUNT(*) FROM logical_videos_fts WHERE logical_videos_fts MATCH ?",
        ("OldUniqueToken",),
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM logical_videos_fts WHERE logical_videos_fts MATCH ?",
        ("NewDifferentTitle",),
    ).fetchone()[0] == 1


def test_normal_writer_commit_updates_both_models_atomically():
    conn = _connection()
    _finish(conn)

    _insert_video(
        conn, 1, filepath=r"D:\atomic.mp4", video_id="atomicwrite",
    )
    conn.commit()

    assert conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0] == 1
    assert catalog_status(conn).logical_videos == 1
    assert catalog_status(conn).media_files == 1
    assert not catalog_status(conn).legacy_dirty


def test_concurrent_commit_cannot_end_incremental_catalog_savepoint():
    import backend.catalog_repository as repository

    conn = _connection(check_same_thread=False)
    _finish(conn)
    _insert_video(
        conn, 1, filepath=r"D:\concurrent.mp4", video_id="concurrent1",
    )
    projection_entered = threading.Event()
    release_projection = threading.Event()
    second_started = threading.Event()
    second_finished = threading.Event()
    errors: list[BaseException] = []
    real_projection = repository._projection_for_keys

    def blocked_projection(connection, keys):
        projection_entered.set()
        assert release_projection.wait(5)
        return real_projection(connection, keys)

    def commit(first: bool) -> None:
        if not first:
            second_started.set()
        try:
            conn.commit()
        except BaseException as exc:
            errors.append(exc)
        finally:
            if not first:
                second_finished.set()

    with mock.patch.object(
        repository, "_projection_for_keys", side_effect=blocked_projection,
    ):
        first = threading.Thread(target=commit, args=(True,))
        second = threading.Thread(target=commit, args=(False,))
        first.start()
        assert projection_entered.wait(2)
        second.start()
        assert second_started.wait(2)
        assert not second_finished.wait(0.2)
        release_projection.set()
        first.join(5)
        second.join(5)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert conn.execute(
        "SELECT COUNT(*) FROM catalog_dirty_keys"
    ).fetchone() == (0,)
    assert not catalog_status(conn).legacy_dirty


def test_normalized_failure_rolls_legacy_half_back(monkeypatch):
    import backend.catalog_repository as repository

    conn = _connection()
    _finish(conn)
    _insert_video(
        conn, 1, filepath=r"D:\rollback.mp4", video_id="dualrollback",
    )
    monkeypatch.setattr(
        repository,
        "reconcile_dirty_catalog",
        lambda _conn: (_ for _ in ()).throw(
            CatalogMigrationError("injected normalized failure")),
    )

    with pytest.raises(CatalogMigrationError, match="normalized failure"):
        conn.commit()

    assert conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM logical_videos").fetchone()[0] == 0


def test_assigning_id_merges_provisional_path_into_existing_logical_video():
    conn = _connection()
    _insert_video(
        conn, 1, filepath=r"D:\known.mp4", video_id="mergeknown1",
    )
    _insert_video(conn, 2, filepath=r"D:\unknown.mp4")
    _finish(conn)
    assert catalog_status(conn).logical_videos == 2

    conn.execute(
        "UPDATE videos SET video_id='mergeknown1' WHERE id=2")
    conn.commit()

    assert catalog_status(conn).logical_videos == 1
    assert catalog_status(conn).media_files == 2
    assert conn.execute(
        "SELECT COUNT(*) FROM media_files WHERE is_primary=1"
    ).fetchone()[0] == 1


def test_search_reads_logical_titles_and_does_not_multiply_transcript_hits(
    monkeypatch,
):
    from backend import index_search

    conn = _connection()
    conn.execute(
        """CREATE VIRTUAL TABLE segments_fts USING fts5(
               text, content=segments, content_rowid=id)"""
    )
    _insert_video(
        conn, 1, title="Unique Logical Title",
        filepath=r"D:\primary.mp4", video_id="searchlogic",
    )
    _insert_video(
        conn, 2, title="Unique Logical Title",
        filepath=r"D:\copy.mp4", video_id="searchlogic",
        duplicate_of=r"D:\primary.mp4",
    )
    conn.execute(
        """INSERT INTO segments(
               id,video_id,title,channel,start_time,end_time,text,jsonl_path)
           VALUES(1,'searchlogic','Unique Logical Title','Example Channel',
                  0,1,'searchable words','D:/transcript.jsonl')"""
    )
    conn.execute("INSERT INTO segments_fts(segments_fts) VALUES('rebuild')")
    _finish(conn)
    monkeypatch.setattr(index_search._index_module(), "_reader_open", lambda: conn)
    index_search._title_search_cache.clear()

    titles = index_search.search_video_titles("Unique")
    transcript = index_search.search_fts("searchable")

    assert len(titles) == 1
    assert titles[0]["video_id"] == "searchlogic"
    assert len(transcript) == 1
    assert transcript[0]["video_id"] == "searchlogic"


def test_connection_context_manager_commits_both_catalog_models():
    conn = _connection()
    _insert_video(conn, 1, filepath=r"D:\before.mp4", video_id="contextvid1")
    _finish(conn)

    with conn:
        conn.execute(
            "UPDATE videos SET title='Committed title' WHERE video_id=?",
            ("contextvid1",),
        )

    row = conn.execute(
        "SELECT title FROM logical_videos WHERE video_id='contextvid1'"
    ).fetchone()
    assert row == ("Committed title",)
    assert normalized_reads_enabled(conn)


def test_pre_v5_catalog_backup_is_atomic_and_verified(tmp_path):
    conn = _connection()
    _insert_video(conn, 1, filepath=r"D:\backup.mp4", video_id="backupvid01")
    sqlite3.Connection.commit(conn)
    backup = tmp_path / "catalog-backup.sqlite3"

    receipt = create_verified_legacy_catalog_backup(conn, backup)
    verified = verify_legacy_catalog_backup(backup)

    assert receipt["rows"] == 1
    assert verified["digest"] == receipt["digest"]
    assert not list(tmp_path.glob("*.tmp"))

    backup.write_bytes(b"not a sqlite database")
    with pytest.raises(CatalogBackupError):
        verify_legacy_catalog_backup(backup)

    rebuilt = create_verified_legacy_catalog_backup(conn, backup)
    assert rebuilt["rows"] == 1
    assert verify_legacy_catalog_backup(backup)["digest"] == rebuilt["digest"]


def test_pre_v5_backup_notice_is_verbose_only(tmp_path, monkeypatch):
    from backend import index
    from backend.log import LogStreamerHandler
    from backend.log_stream import LogStreamer

    database = tmp_path / "transcription_index.db"
    seed = sqlite3.connect(database)
    seed.execute(_VIDEO_SCHEMA)
    seed.execute(
        """INSERT INTO videos(
               id,title,channel,filepath,video_id,tx_status,availability)
           VALUES(1,'Legacy','Example Channel','D:/legacy.mp4',
                  'legacyvid01','transcribed','available')"""
    )
    seed.execute("PRAGMA user_version=4")
    seed.commit()
    seed.close()

    simple = LogStreamer()
    simple.simple_mode = True
    verbose = LogStreamer()
    verbose.simple_mode = False
    handlers = [LogStreamerHandler(simple), LogStreamerHandler(verbose)]
    logger = index._log
    old_level = logger.level
    old_propagate = logger.propagate

    index._shutdown_index()
    monkeypatch.setattr(index, "TRANSCRIPTION_DB", database)
    logger.setLevel("DEBUG")
    logger.propagate = False
    for handler in handlers:
        logger.addHandler(handler)
    try:
        assert index._open() is not None
    finally:
        index._shutdown_index()
        for handler in handlers:
            logger.removeHandler(handler)
            handler.close()
        logger.setLevel(old_level)
        logger.propagate = old_propagate

    phrase = "Verified pre-v5 catalog backup"
    simple_text = "".join(
        segment[0] for line in simple._buffer for segment in line
    )
    matching_verbose_lines = [
        line for line in verbose._buffer
        if phrase in "".join(segment[0] for segment in line)
    ]

    assert phrase not in simple_text
    assert "Preparing the library catalog for Patch 5" in simple_text
    assert "Library catalog upgrade complete" in simple_text
    assert str(database) not in simple_text
    assert len(matching_verbose_lines) == 1
    assert matching_verbose_lines[0][0][1] == "dim"
    assert database.with_name(
        "transcription_index.pre-v5-catalog-backup.sqlite3"
    ).exists()
