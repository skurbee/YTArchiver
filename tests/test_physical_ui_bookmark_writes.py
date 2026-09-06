"""Bounded, durable bookmark mutations and full-library literal filtering."""
import atexit
import os
import sqlite3
import tempfile
import threading
import time
from pathlib import Path

import pytest

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-physical-bookmarks-")
atexit.register(_PROFILE.cleanup)
os.environ["APPDATA"] = str(Path(_PROFILE.name) / "roaming")
os.environ["LOCALAPPDATA"] = str(Path(_PROFILE.name) / "local")

from backend import index, index_bookmarks
from backend.api_mixins.bookmark_mixin import BookmarkMixin
from backend.catalog_repository import CatalogConnection


@pytest.fixture
def database(tmp_path, monkeypatch):
    index._shutdown_index()
    index._schema_inited = False
    monkeypatch.setattr(index, "TRANSCRIPTION_DB", tmp_path / "index.db")
    monkeypatch.setattr(index, "find_thumbnail", lambda *_args: None)
    conn = index._open()
    assert conn is not None
    yield conn
    index._shutdown_index()
    index._schema_inited = False


def payload(**values):
    return {"video_id": "fixture1111", "title": "Fixture", "channel": "Fixture",
            "start_time": -1, "text": "", "note": "", **values}


def test_repeated_whole_video_bookmark_is_durable_without_transcript(database):
    api = BookmarkMixin()
    first = api.bookmark_add(payload())
    second = api.bookmark_add(payload())
    assert first["ok"] and second == first
    assert not database.in_transaction
    assert database.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0] == 1
    assert database.execute("SELECT COUNT(*) FROM segments").fetchone()[0] == 0
    other = sqlite3.connect(str(index.TRANSCRIPTION_DB))
    try:
        assert other.execute("SELECT start_time FROM bookmarks").fetchone()[0] == -1
    finally:
        other.close()


def test_concurrent_identical_retries_create_one_row_but_notes_remain_distinct(database):
    results = []
    start = threading.Barrier(3)

    def save():
        start.wait(2)
        results.append(BookmarkMixin().bookmark_add(payload()))

    threads = [threading.Thread(target=save) for _ in range(2)]
    for thread in threads:
        thread.start()
    start.wait(2)
    for thread in threads:
        thread.join(3)
    assert len(results) == 2 and all(r["ok"] for r in results)
    assert results[0]["id"] == results[1]["id"]
    distinct = BookmarkMixin().bookmark_add(payload(note="Separate note"))
    assert distinct["ok"] and distinct["id"] != results[0]["id"]
    assert database.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0] == 2


def test_writer_admission_timeout_returns_failure_without_late_insert(database, monkeypatch):
    monkeypatch.setattr(index, "_INTERACTIVE_LOCK_SECONDS", 0.03)
    held, release = threading.Event(), threading.Event()

    def holder():
        with index._db_lock:
            held.set()
            release.wait(2)

    thread = threading.Thread(target=holder)
    thread.start()
    assert held.wait(1)
    started = time.monotonic()
    try:
        result = BookmarkMixin().bookmark_add(payload())
        assert not result["ok"] and result["retryable"]
        assert "could not start" in result["error"]
        assert time.monotonic() - started < 1
    finally:
        release.set()
        thread.join(2)
    assert database.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0] == 0
    assert BookmarkMixin().bookmark_add(payload())["ok"]
    assert database.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0] == 1


def test_external_sqlite_writer_failure_rolls_back_and_retries_cleanly(database, monkeypatch):
    monkeypatch.setattr(index_bookmarks, "_BOOKMARK_WRITE_SECONDS", 0.03)
    blocker = sqlite3.connect(str(index.TRANSCRIPTION_DB))
    blocker.execute("BEGIN IMMEDIATE")
    started = time.monotonic()
    try:
        result = BookmarkMixin().bookmark_add(payload())
        assert not result["ok"] and result["retryable"]
        assert time.monotonic() - started < 1
        assert not database.in_transaction
    finally:
        blocker.rollback()
        blocker.close()
    assert database.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0] == 0
    assert BookmarkMixin().bookmark_add(payload())["ok"]


def test_commit_failure_is_reported_and_cannot_leak_into_later_save(database, monkeypatch):
    original_commit = CatalogConnection.commit

    def fail_commit(_conn):
        raise sqlite3.OperationalError("synthetic disk error")

    with monkeypatch.context() as failed:
        failed.setattr(CatalogConnection, "commit", fail_commit)
        result = BookmarkMixin().bookmark_add(payload())
    assert not result["ok"] and "synthetic disk error" in result["error"]
    assert not database.in_transaction
    assert database.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0] == 0
    assert CatalogConnection.commit is original_commit
    assert BookmarkMixin().bookmark_add(payload())["ok"]
    assert database.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0] == 1


def test_slow_catalog_commit_is_interrupted_and_rolled_back(database, monkeypatch):
    def slow_commit(conn):
        conn.execute(
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) "
            "SELECT sum(x) FROM n").fetchone()

    with monkeypatch.context() as slow:
        slow.setattr(CatalogConnection, "commit", slow_commit)
        slow.setattr(index_bookmarks, "_BOOKMARK_WRITE_SECONDS", 0.01)
        result = BookmarkMixin().bookmark_add(payload())
    assert not result["ok"] and result["retryable"]
    assert not database.in_transaction
    assert database.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0] == 0
    assert BookmarkMixin().bookmark_add(payload())["ok"]


def test_bookmark_cannot_commit_or_rollback_another_unfinished_transaction(database):
    database.execute("INSERT INTO bookmarks(video_id,title) VALUES('pending','Pending')")
    result = BookmarkMixin().bookmark_add(payload())
    assert not result["ok"] and result["retryable"]
    assert database.in_transaction
    assert database.execute("SELECT video_id FROM bookmarks").fetchall() == [("pending",)]
    database.rollback()
    assert BookmarkMixin().bookmark_add(payload())["ok"]


def test_filter_is_literal_and_searches_older_bookmarks_before_limit(database):
    database.executemany(
        "INSERT INTO bookmarks(video_id,title,channel,start_time,note,created) VALUES(?,?,?,?,?,?)",
        [(f"fixture{i}", f"Recent {i}", "Fixture", -1, "", i + 10) for i in range(510)])
    database.execute(
        "INSERT INTO bookmarks(video_id,title,channel,start_time,note,created) VALUES(?,?,?,?,?,?)",
        ("oldfixture1", "Old title", "Older channel", -1, "100%_literal's NOTE", 1))
    database.commit()
    api = BookmarkMixin()
    assert len(api.bookmark_list()["rows"]) == 500
    result = api.bookmark_list("100%_LITERAL's note")
    assert result["ok"] and len(result["rows"]) == 1
    assert result["rows"][0]["video_id"] == "oldfixture1"
    assert len(api.bookmark_list("Older CHANNEL")["rows"]) == 1
    assert len(api.bookmark_list("old TITLE")["rows"]) == 1
    assert api.bookmark_list("100ZZliteral")["rows"] == []


def test_bookmark_update_and_remove_also_have_definite_writer_failure(database, monkeypatch):
    api = BookmarkMixin()
    saved = api.bookmark_add(payload())
    monkeypatch.setattr(index, "_INTERACTIVE_LOCK_SECONDS", 0.02)
    held, release = threading.Event(), threading.Event()

    def holder():
        with index._db_lock:
            held.set()
            release.wait(2)

    thread = threading.Thread(target=holder)
    thread.start()
    assert held.wait(1)
    try:
        for result in (api.bookmark_update_note(saved["id"], "New"), api.bookmark_remove(saved["id"])):
            assert not result["ok"] and result["retryable"]
    finally:
        release.set()
        thread.join(2)
    assert database.execute("SELECT note FROM bookmarks").fetchall() == [("",)]
    assert api.bookmark_update_note(saved["id"], "New")["ok"]
    assert api.bookmark_remove(saved["id"])["ok"]


def test_restore_closed_admission_prevents_all_bookmark_mutations(database, monkeypatch):
    api = BookmarkMixin()
    denied = {"ok": False, "error": "Restore in progress", "code": "WORK_ADMISSION_CLOSED"}
    api._work_admission_error = lambda _operation: denied

    def forbidden(*_args):
        raise AssertionError("closed admission must not call bookmark storage")

    monkeypatch.setattr(index, "bookmark_add", forbidden)
    monkeypatch.setattr(index, "bookmark_remove", forbidden)
    monkeypatch.setattr(index, "bookmark_update_note", forbidden)
    assert api.bookmark_add(payload()) == denied
    assert api.bookmark_remove(1) == denied
    assert api.bookmark_update_note(1, "note") == denied
    assert database.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0] == 0


def test_admission_is_rechecked_after_waiting_for_writer(database):
    api = BookmarkMixin()
    closed, started = threading.Event(), threading.Event()
    checks, results = [], []
    denied = {"ok": False, "error": "Restore in progress"}

    def gate(_operation):
        checks.append(index._db_lock._is_owned())
        return denied if closed.is_set() else None

    def save():
        started.set()
        results.append(api.bookmark_add(payload()))

    api._work_admission_error = gate
    with index._db_lock:
        thread = threading.Thread(target=save)
        thread.start()
        assert started.wait(1)
        closed.set()
    thread.join(2)
    assert results == [denied] and checks == [True]
    assert database.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0] == 0
