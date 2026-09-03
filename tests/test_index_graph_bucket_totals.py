"""Focused correctness and plan contracts for normalized Graph denominators."""

from __future__ import annotations

import datetime as dt
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from unittest import mock

from backend import index, index_graph


def _timestamp(year: int, month: int, day: int) -> float:
    # Production deliberately uses localtime for both SQL calendar buckets and
    # Python ISO weeks. Midday avoids DST/midnight boundary ambiguity.
    return dt.datetime(year, month, day, 12, 0, 0).timestamp()


@contextmanager
def _graph_db(database: str = ":memory:"):
    conn = sqlite3.connect(database, check_same_thread=False)
    conn.executescript("""
        CREATE TABLE videos(
            id INTEGER PRIMARY KEY,
            title TEXT,
            channel TEXT,
            year INTEGER,
            month INTEGER,
            filepath TEXT,
            video_id TEXT,
            duration_s REAL,
            upload_ts REAL,
            availability TEXT,
            is_duplicate_of TEXT
        );
        CREATE TABLE segments(
            id INTEGER PRIMARY KEY,
            video_id TEXT,
            title TEXT,
            channel TEXT,
            year INTEGER,
            month INTEGER,
            start_time REAL,
            end_time REAL,
            text TEXT,
            jsonl_path TEXT
        );
        CREATE INDEX idx_seg_video_id ON segments(video_id);
        CREATE INDEX idx_seg_channel ON segments(channel);
    """)
    index_graph.invalidate_top_words_cache()
    try:
        with mock.patch.object(index, "_reader_open", return_value=conn):
            yield conn
    finally:
        index_graph.invalidate_top_words_cache()
        conn.close()


def _video(
    conn: sqlite3.Connection,
    video_id: str,
    filepath: str,
    upload_ts: float | None,
    *,
    channel: str = "Channel A",
    availability: str = "available",
    duplicate_of: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO videos(title, channel, filepath, video_id, upload_ts, "
        "availability, is_duplicate_of) VALUES(?, ?, ?, ?, ?, ?, ?)",
        (filepath, channel, filepath, video_id, upload_ts, availability,
         duplicate_of),
    )


def _segments(
    conn: sqlite3.Connection,
    count: int,
    video_id: str | None,
    *,
    channel: str = "Channel A",
    year: int = 1999,
    month: int = 1,
) -> None:
    conn.executemany(
        "INSERT INTO segments(video_id, title, channel, year, month, "
        "start_time, end_time, text, jsonl_path) "
        "VALUES(?, 'Transcript', ?, ?, ?, 0, 1, 'text', 'fixture.jsonl')",
        [(video_id, channel, year, month)] * count,
    )


def test_bucket_totals_counts_segments_once_using_exact_canonical_copy() -> None:
    with _graph_db() as conn:
        january = _timestamp(2024, 1, 15)
        february = _timestamp(2024, 2, 15)
        march = _timestamp(2024, 3, 15)
        _video(conn, "duplicate01", "primary.mp4", january)
        _video(
            conn, "duplicate01", "duplicate.mp4", february,
            duplicate_of="primary.mp4",
        )
        _segments(conn, 3, "duplicate01")

        # Availability outranks the stale primary hint in the canonical CTE.
        _video(
            conn, "available02", "missing-primary.mp4", february,
            availability="missing",
        )
        _video(
            conn, "available02", "available-copy.mp4", march,
            duplicate_of="missing-primary.mp4",
        )
        _segments(conn, 2, "available02")
        conn.commit()

        assert index_graph.bucket_totals("month") == {
            "2024-01": 3,
            "2024-03": 2,
        }


def test_idless_segments_remain_distinct_by_bucket_and_channel() -> None:
    with _graph_db() as conn:
        _segments(conn, 2, None, channel="Channel A", year=2020, month=1)
        _segments(conn, 1, "", channel="Channel A", year=2020, month=2)
        _segments(conn, 1, None, channel="Channel B", year=2020, month=1)
        conn.commit()

        assert index_graph.bucket_totals("month") == {
            "2020-01": 3,
            "2020-02": 1,
        }
        assert index_graph.bucket_totals("month", "Channel A") == {
            "2020-01": 2,
            "2020-02": 1,
        }
        assert index_graph.bucket_totals("month", "Channel B") == {
            "2020-01": 1,
        }


def test_bucket_cache_reuses_query_and_tracks_data_plus_manual_revision() -> None:
    with _graph_db() as conn:
        _video(conn, "cachevideo1", "cache.mp4", _timestamp(2023, 6, 1))
        _segments(conn, 1, "cachevideo1")
        conn.commit()
        statements: list[str] = []
        conn.set_trace_callback(statements.append)

        first = index_graph.bucket_totals("year")
        first["mutated-by-caller"] = 99
        second = index_graph.bucket_totals("year")
        assert second == {"2023": 1}

        def aggregate_runs() -> int:
            return sum(
                "segment_counts AS MATERIALIZED" in statement
                for statement in statements
            )

        assert aggregate_runs() == 1

        # Same-connection writes do not increment PRAGMA data_version, so the
        # cache key also includes total_changes.
        _segments(conn, 1, "cachevideo1")
        conn.commit()
        assert index_graph.bucket_totals("year") == {"2023": 2}
        assert aggregate_runs() == 2

        index_graph.invalidate_top_words_cache()
        assert index_graph.bucket_totals("year") == {"2023": 2}
        assert aggregate_runs() == 3


def test_bucket_cache_notices_commits_from_production_style_writer(
    tmp_path,
) -> None:
    database = str(tmp_path / "graph-cache.sqlite3")
    with _graph_db(database) as reader:
        _video(reader, "external001", "external.mp4", _timestamp(2021, 5, 1))
        _segments(reader, 1, "external001")
        reader.commit()
        assert index_graph.bucket_totals("year") == {"2021": 1}

        writer = sqlite3.connect(database)
        try:
            _segments(writer, 1, "external001")
            writer.commit()
        finally:
            writer.close()

        # The long-lived reader's PRAGMA data_version advances for commits
        # made by the separate writer used in production.
        assert index_graph.bucket_totals("year") == {"2021": 2}


def test_simultaneous_cache_misses_run_only_one_aggregate() -> None:
    with _graph_db() as conn:
        _video(conn, "singleflight1", "singleflight.mp4", _timestamp(2020, 2, 1))
        _segments(conn, 10, "singleflight1")
        conn.commit()
        statements: list[str] = []
        conn.set_trace_callback(statements.append)

        real_lookup = index_graph._bucket_cache_lookup
        first_lookup_barrier = threading.Barrier(2)
        local = threading.local()

        def synchronized_first_lookup(*args, **kwargs):
            result = real_lookup(*args, **kwargs)
            if not getattr(local, "completed_first_lookup", False):
                local.completed_first_lookup = True
                first_lookup_barrier.wait(timeout=5)
            return result

        with mock.patch.object(
            index_graph,
            "_bucket_cache_lookup",
            side_effect=synchronized_first_lookup,
        ), ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(index_graph.bucket_totals, "year")
                for _ in range(2)
            ]
            results = [future.result(timeout=10) for future in futures]

        assert results == [{"2020": 10}, {"2020": 10}]
        assert sum(
            "segment_counts AS MATERIALIZED" in statement
            for statement in statements
        ) == 1


def test_week_totals_use_iso_year_and_canonical_dates() -> None:
    with _graph_db() as conn:
        _video(conn, "weekvideo01", "week-1.mp4", _timestamp(2024, 12, 30))
        _video(
            conn, "weekvideo01", "week-1-copy.mp4", _timestamp(2024, 1, 1),
            duplicate_of="week-1.mp4",
        )
        _segments(conn, 2, "weekvideo01")
        _video(conn, "weekvideo02", "week-2.mp4", _timestamp(2024, 12, 23))
        _segments(conn, 1, "weekvideo02")
        conn.commit()

        assert index_graph.bucket_totals("week") == {
            "2024-W52": 1,
            "2025-W01": 2,
        }


def test_global_query_plan_collapses_via_video_id_index_before_joins() -> None:
    with _graph_db() as conn:
        _video(conn, "planvideo01", "plan.mp4", _timestamp(2022, 4, 2))
        _segments(conn, 500, "planvideo01")
        _segments(conn, 3, None, year=2022, month=4)
        conn.commit()
        statements: list[str] = []
        conn.set_trace_callback(statements.append)

        assert index_graph.bucket_totals("month") == {"2022-04": 503}
        aggregate_sql = next(
            statement for statement in statements
            if "segment_counts AS MATERIALIZED" in statement
        )
        plan = conn.execute(
            "EXPLAIN QUERY PLAN " + aggregate_sql
        ).fetchall()
        details = "\n".join(str(row[3]) for row in plan)

        assert "MATERIALIZE segment_counts" in details
        assert "idx_seg_video_id" in details
        assert "USING INTEGER PRIMARY KEY" in details
