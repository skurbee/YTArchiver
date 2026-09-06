"""Deep-check cancellation and background-job regressions; disposable inputs only."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import closing

import pytest

from backend import integrity_scan
from backend.api_mixins import diagnostics_mixin


def fixture_paths(tmp_path):
    archive = tmp_path / "archive"
    archive.mkdir()
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"channels": []}), encoding="utf-8")
    queue = tmp_path / "queue.json"
    queue.write_text("{}", encoding="utf-8")
    database = tmp_path / "index.db"
    with closing(sqlite3.connect(database)) as connection:
        connection.executescript("""
            CREATE TABLE segments(id INTEGER PRIMARY KEY, video_id TEXT, channel TEXT, title TEXT, text TEXT);
            CREATE VIRTUAL TABLE segments_fts USING fts5(text,content=segments,content_rowid=id);
            INSERT INTO segments VALUES(1,'fixturevid1','Fixture','Title','one two three');
            INSERT INTO segments_fts(segments_fts) VALUES('rebuild');
        """)
    return {"archive_path": archive, "config_path": config, "queue_path": queue, "db_path": database}


def test_progress_reaches_expensive_fts_phases_without_changing_database(tmp_path):
    paths = fixture_paths(tmp_path)
    before = hashlib.sha256(paths["db_path"].read_bytes()).digest()
    updates = []
    result = integrity_scan.scan_integrity(**paths, progress=updates.append)
    assert result["preview_only"] and not result["repairs_applied"]
    assert any("Checking saved transcript search entries" in row["phase"] for row in updates)
    assert any("Comparing source text for transcript search" in row["phase"] for row in updates)
    assert all(row["completed"] >= 0 and row["elapsed_seconds"] >= 0 for row in updates)
    assert before == hashlib.sha256(paths["db_path"].read_bytes()).digest()


def test_cancel_at_token_phase_never_returns_clean_or_partial_success(tmp_path):
    paths = fixture_paths(tmp_path)
    cancel = threading.Event()

    def report(state):
        if "Checking saved transcript search entries" in state["phase"]:
            cancel.set()

    result = integrity_scan.scan_integrity(**paths, cancel_event=cancel, progress=report)
    assert result["cancelled"] and not result["ok"]
    assert "healthy" not in result and "summary" not in result
    assert not result["repairs_applied"]


def test_cancel_interrupts_a_sql_statement_before_first_result(tmp_path, monkeypatch):
    paths = fixture_paths(tmp_path)
    cancel = threading.Event()
    opened = []

    def expensive_read(connection):
        opened.append(connection)

        def mark(value):
            if value == 100:
                cancel.set()
            return value

        connection.create_function("mark", 1, mark)
        connection.execute("""WITH RECURSIVE numbers(n) AS (
            VALUES(1) UNION ALL SELECT n+1 FROM numbers WHERE n<10000000)
            SELECT sum(mark(n)) FROM numbers""").fetchone()
        pytest.fail("The SQL progress handler should have interrupted this statement")

    monkeypatch.setattr(integrity_scan, "_load_database_records", expensive_read)
    result = integrity_scan.scan_integrity(**paths, cancel_event=cancel)
    assert result["cancelled"] and not result["ok"]
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        opened[0].execute("SELECT 1")


def test_precancel_does_not_open_any_database(tmp_path, monkeypatch):
    paths = fixture_paths(tmp_path)
    cancel = threading.Event()
    cancel.set()
    monkeypatch.setattr(integrity_scan, "_open_database_read_only", lambda _: pytest.fail("opened database"))
    assert integrity_scan.scan_integrity(**paths, cancel_event=cancel)["cancelled"]


def test_job_is_single_instance_and_cancel_targets_only_current_scan(monkeypatch):
    threads = []
    ready = threading.Event()

    def start(_api, **kwargs):
        assert kwargs["owner"] == "integrity-scan"
        assert kwargs["cancel"] is not None
        thread = threading.Thread(target=kwargs["target"])
        threads.append(thread)
        thread.start()
        return thread

    monkeypatch.setattr(diagnostics_mixin, "start_managed_task", start)

    class Api(diagnostics_mixin.DiagnosticsMixin):
        def _run_integrity_scan(self, *, cancel_event, progress):
            progress({"phase": "Checking fixture", "completed": 42, "unit": "records checked"})
            ready.set()
            assert cancel_event.wait(5)
            return {"ok": False, "cancelled": True, "preview_only": True, "repairs_applied": False}

    api = Api()
    first = api.integrity_scan_start()
    try:
        assert ready.wait(2)
        assert first["started"]
        duplicate = api.integrity_scan_start()
        assert not duplicate["started"] and duplicate["job_id"] == first["job_id"]
        state = api.integrity_scan_state()
        assert state["running"] and state["completed"] == 42
        assert state["elapsed_seconds"] >= 0
        assert not api.integrity_scan_cancel("old-job")["ok"]
        assert api.integrity_scan_cancel(first["job_id"])["ok"]
    finally:
        api.integrity_scan_cancel()
        for thread in threads:
            thread.join(5)
    state = api.integrity_scan_state()
    assert not state["running"] and state["result"]["cancelled"]
    assert len(threads) == 1


def test_start_failure_is_reported_and_allows_retry(monkeypatch):
    class Api(diagnostics_mixin.DiagnosticsMixin):
        pass

    def reject(*args, **kwargs):
        raise RuntimeError("Shutting down")

    monkeypatch.setattr(diagnostics_mixin, "start_managed_task", reject)
    api = Api()
    assert not api.integrity_scan_start()["ok"]
    assert not api.integrity_scan_state()["running"]
    assert not api.integrity_scan_start()["ok"]
