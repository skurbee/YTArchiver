"""Foreground catalog query plans, identity safety and bounded contention."""
import atexit
import os
import sqlite3
import tempfile
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-physical-reads-")
atexit.register(_PROFILE.cleanup)
os.environ["APPDATA"] = str(Path(_PROFILE.name) / "roaming")
os.environ["LOCALAPPDATA"] = str(Path(_PROFILE.name) / "local")

from backend import index, index_search


@pytest.fixture
def database(tmp_path, monkeypatch):
    index._shutdown_index()
    index._schema_inited = False
    monkeypatch.setattr(index, "TRANSCRIPTION_DB", tmp_path / "index.db")
    conn = index._open()
    assert conn is not None
    yield conn
    index._shutdown_index()
    index._schema_inited = False


def add_video(conn, identity, title, uploaded=None, added=1):
    conn.execute(
        "INSERT INTO videos(video_id,title,channel,filepath,upload_ts,added_ts) VALUES(?,?,?,?,?,?)",
        (identity, title, "Fixture", f"C:/fixture/{identity or title}.mp4", uploaded, added))


def add_segment(conn, identity, title, *, year=None, start=0, text="processor words", source="fixture.jsonl"):
    conn.execute(
        "INSERT INTO segments(video_id,title,channel,year,start_time,end_time,text,jsonl_path) "
        "VALUES(?,?,?,?,?,?,?,?)", (identity, title, "Fixture", year, start, start + 1, text, source))


def ts(month, day=1, year=2025):
    return datetime(year, month, day, tzinfo=UTC).timestamp()


def test_misleading_bracket_without_candidate_never_runs_ambiguity_scan(database, tmp_path):
    path = tmp_path / "Fixture [old12345678].mp4"
    database.execute(
        "INSERT INTO videos(video_id,title,channel,filepath) VALUES(?,?,?,?)",
        ("new12345678", "Fixture", "Fixture", str(path)))
    database.executemany(
        "INSERT INTO segments(video_id,title,channel,start_time,text,jsonl_path) VALUES(?,?,?,?,?,?)",
        [(f"other{i:06}", f"Other title {i}", "Elsewhere", 0, "words", "other.jsonl") for i in range(3000)])
    database.commit()
    statements = []
    reader = index._reader_open()
    reader.set_trace_callback(statements.append)
    try:
        assert index.get_segments(video_id="new12345678", title="Fixture", channel="Fixture",
                                  filepath=str(path), strict_identity=True) == []
    finally:
        reader.set_trace_callback(None)
    assert not any("SELECT DISTINCT video_id FROM segments WHERE title=" in sql for sql in statements)
    candidate_sql = next(sql for sql in statements if "SELECT 1 FROM segments WHERE video_id=" in sql)
    plan = reader.execute("EXPLAIN QUERY PLAN " + candidate_sql).fetchall()
    assert any("SEARCH segments USING INDEX idx_seg_video_id" in row[3] for row in plan)


@pytest.mark.parametrize("conflict", [False, True])
def test_stale_identity_recovery_requires_unique_case_insensitive_evidence(database, tmp_path, conflict):
    path = tmp_path / "Shared title [good1234567].mp4"
    database.execute(
        "INSERT INTO videos(video_id,title,channel,filepath) VALUES(?,?,?,?)",
        ("bad12345678", "Shared title", "Fixture", str(path)))
    add_segment(database, "good1234567", "Shared title", text="candidate")
    if conflict:
        add_segment(database, "other123456", "SHARED TITLE", text="conflict")
    database.commit()
    rows = index.get_segments(video_id="bad12345678", title="Shared title", channel="Fixture",
                              filepath=str(path), strict_identity=True)
    assert [r["t"] for r in rows] == ([] if conflict else ["candidate"])


def test_existing_catalog_identity_wins_over_matching_bracket(database, tmp_path):
    path = tmp_path / "Shared title [good1234567].mp4"
    database.execute(
        "INSERT INTO videos(video_id,title,channel,filepath) VALUES(?,?,?,?)",
        ("bad12345678", "Shared title", "Fixture", str(path)))
    add_segment(database, "good1234567", "Shared title", text="bracket")
    add_segment(database, "bad12345678", "Shared title", text="catalog")
    database.commit()
    rows = index.get_segments(video_id="bad12345678", title="Shared title", channel="Fixture",
                              filepath=str(path), strict_identity=True)
    assert [r["t"] for r in rows] == ["catalog"]


@pytest.mark.parametrize("normalized", [False, True])
def test_dated_search_preserves_identity_fallback_boundaries_and_dedup(database, monkeypatch, normalized):
    add_video(database, "june1234567", "June", ts(6), ts(7))
    add_video(database, "july1234567", "July", ts(7))
    add_video(database, "fallback111", "Legacy dated", ts(6, 15))
    add_video(database, "fallback222", "Legacy dated", ts(7, 15))
    add_video(database, "exact123456", "Legacy dated", ts(7))
    add_segment(database, "june1234567", "June")
    add_segment(database, "june1234567", "June", source="duplicate.jsonl")
    add_segment(database, "july1234567", "July")
    add_segment(database, "", "Legacy dated")
    add_segment(database, "exact123456", "Legacy dated")
    add_segment(database, "missing1111", "Folder only", year=2025)
    add_segment(database, "unknown1111", "Unknown")
    database.commit()
    monkeypatch.setattr(index_search, "normalized_reads_enabled", lambda _conn: normalized)
    statements = []
    reader = index._reader_open()
    reader.set_trace_callback(statements.append)
    try:
        dated = index_search.search_fts("processor", year_from=2025, year_to=2025,
                                       date_from_ts=ts(6), date_to_ts=ts(7))
        annual = index_search.search_fts("processor", year_from=2025, year_to=2025)
        plain = index_search.search_fts("processor")
    finally:
        reader.set_trace_callback(None)
    assert {(r["video_id"], r["upload_ts"]) for r in dated} == {
        ("june1234567", ts(6)), ("", ts(6, 15))}
    assert len(dated) == 2
    assert {r["video_id"] for r in annual} == {
        "june1234567", "july1234567", "", "exact123456", "missing1111", "unknown1111"}
    assert len(plain) == 6
    queries = [sql for sql in statements if sql.startswith("SELECT s.id, s.video_id,")]
    assert len(queries) == 3
    for query in queries:
        details = [r[3] for r in reader.execute("EXPLAIN QUERY PLAN " + query)]
        assert not any("SCAN v " in line or "MATERIALIZE vt" in line for line in details), details
        assert any("SEARCH v USING INDEX" in line for line in details), details
        if normalized:
            assert any("idx_logical_videos_video_id" in line for line in details), details
        if "SELECT MIN(vt.upload_ts)" in query:
            expected = "idx_logical_videos_chan_title_date" if normalized else "idx_vid_chan_title"
            assert any("SEARCH vt USING COVERING INDEX " + expected in line for line in details), details


def test_reader_lock_wait_is_bounded_and_recovers(database, monkeypatch):
    add_segment(database, "fixture1111", "Fixture")
    database.commit()
    index._reader_open()
    monkeypatch.setattr(index, "_INTERACTIVE_LOCK_SECONDS", 0.03)
    held, release = threading.Event(), threading.Event()

    def holder():
        with index._reader_lock:
            held.set()
            release.wait(2)

    thread = threading.Thread(target=holder)
    thread.start()
    assert held.wait(1)
    started = time.monotonic()
    try:
        with pytest.raises(index.LibraryQueryTimeout, match="another library operation"):
            index.get_segments(video_id="fixture1111")
        assert time.monotonic() - started < 1
    finally:
        release.set()
        thread.join(2)
    assert index.get_segments(video_id="fixture1111")


def test_sql_budget_interrupts_expensive_work_and_restores_connection(database):
    reader = index._reader_open()
    original_busy = reader.execute("PRAGMA busy_timeout").fetchone()[0]
    with index._reader_lock:
        with pytest.raises(index.LibraryQueryTimeout):
            with index._bounded_sql(reader, "Synthetic read", 0.01):
                reader.execute(
                    "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) "
                    "SELECT sum(x) FROM n").fetchone()
        assert reader.execute("SELECT 42").fetchone()[0] == 42
        assert reader.execute("PRAGMA busy_timeout").fetchone()[0] == original_busy


def test_search_timeout_is_not_reported_as_no_results(database, monkeypatch):
    add_segment(database, "fixture1111", "Fixture")
    database.commit()
    with monkeypatch.context() as limited:
        limited.setattr(index, "_INTERACTIVE_QUERY_SECONDS", 0.000001)
        with pytest.raises(index.LibraryQueryTimeout):
            index_search.search_fts("processor")
    assert index_search.search_fts("processor")


def test_identity_timeout_cannot_be_swallowed_as_empty_transcript(database, tmp_path, monkeypatch):
    path = tmp_path / "Shared title [good1234567].mp4"
    add_segment(database, "good1234567", "Shared title")
    database.commit()
    with monkeypatch.context() as limited:
        limited.setattr(index, "_INTERACTIVE_QUERY_SECONDS", 0.000001)
        with pytest.raises(index.LibraryQueryTimeout):
            index.get_segments(video_id="bad12345678", title="Shared title", channel="Fixture",
                               filepath=str(path), strict_identity=True)
    assert database.execute("SELECT COUNT(*) FROM segments").fetchone()[0] == 1


def test_busy_sql_error_is_propagated_instead_of_syntax_retry(database, monkeypatch):
    def busy(*_args):
        error = sqlite3.OperationalError("database is locked")
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY
        raise error

    monkeypatch.setattr(index_search, "_normalize_fts_query", busy)
    with pytest.raises(index.LibraryQueryTimeout, match="library was busy"):
        index_search.search_fts("processor")


def test_title_search_timeout_is_not_retried_as_full_catalog_like(database, monkeypatch):
    add_video(database, "fixture1111", "Processor fixture", ts(6))
    database.commit()
    index_search._title_search_cache.clear()
    statements = []
    reader = index._reader_open()
    reader.set_trace_callback(statements.append)
    try:
        with monkeypatch.context() as limited:
            limited.setattr(index, "_INTERACTIVE_QUERY_SECONDS", 0.000001)
            with pytest.raises(index.LibraryQueryTimeout):
                index_search.search_video_titles("processor")
    finally:
        reader.set_trace_callback(None)
    assert not any(" LIKE " in sql for sql in statements)
    assert index_search.search_video_titles("processor")
