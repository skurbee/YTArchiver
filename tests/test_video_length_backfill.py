"""Regressions for Health > Archive Files > Missing video lengths."""

from __future__ import annotations

import sqlite3
import threading

from backend import index
from backend.metadata import core


class _Stream:
    def __init__(self) -> None:
        self.lines: list[list[list[object]]] = []

    def emit(self, line) -> None:
        self.lines.append(line)

    def text(self) -> str:
        return "".join(
            str(segment[0])
            for line in self.lines
            for segment in line
        )


def _duration_catalog() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE videos(
               filepath TEXT,
               duration_s REAL,
               availability TEXT
           )"""
    )
    conn.executemany(
        "INSERT INTO videos(filepath, duration_s, availability) VALUES(?,?,?)",
        [
            ("available.mp4", None, "available"),
            ("legacy-available.mp4", 0, None),
            ("deleted.f160.mp4", None, "missing"),
            ("partial.f136.mp4", None, "partial"),
            ("already-filled.mp4", 60, "available"),
        ],
    )
    return conn


def test_duration_count_and_backfill_exclude_unavailable_rows(monkeypatch):
    conn = _duration_catalog()
    monkeypatch.setattr(index, "_reader_open", lambda: conn)
    monkeypatch.setattr(index, "_reader_lock", threading.RLock())
    seen: list[str] = []

    def probe(filepaths, *_args, **_kwargs):
        seen.extend(filepaths)
        return dict.fromkeys(filepaths, 42.0)

    monkeypatch.setattr(core, "_probe_durations_bulk", probe)

    assert core.count_missing_durations() == 2
    result = core.backfill_missing_durations(_Stream())

    assert seen == ["available.mp4", "legacy-available.mp4"]
    assert result == {
        "ok": True,
        "total": 2,
        "resolved": 2,
        "failed": 0,
        "unchecked": 0,
        "cancelled": False,
    }
    conn.close()


def test_completed_duration_probe_reports_unreadable_files_without_rerun_loop(
        monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE videos(filepath TEXT, duration_s REAL, availability TEXT)"
    )
    conn.execute(
        "INSERT INTO videos VALUES('broken.mp4', NULL, 'available')"
    )
    monkeypatch.setattr(index, "_reader_open", lambda: conn)
    monkeypatch.setattr(index, "_reader_lock", threading.RLock())
    monkeypatch.setattr(
        core,
        "_probe_durations_bulk",
        lambda filepaths, *_args, **_kwargs: {filepaths[0]: None},
    )
    stream = _Stream()

    result = core.backfill_missing_durations(stream)

    assert result["resolved"] == 0
    assert result["failed"] == 1
    assert result["unchecked"] == 0
    assert "1 available file(s) could not be read by ffprobe" in stream.text()
    assert "re-run" not in stream.text().lower()
    assert stream.lines[-1][0][1] == "summary"
    conn.close()
