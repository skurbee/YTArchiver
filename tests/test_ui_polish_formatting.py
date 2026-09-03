import sqlite3
import threading
from types import SimpleNamespace

from backend import index_search
from backend.ytarchiver_config import channels_for_subs_ui


def test_small_channel_size_uses_mb_instead_of_zero_gb():
    size_bytes = 16 * 1024 * 1024
    rows, _ = channels_for_subs_ui({
        "channels": [{
            "name": "Small channel",
            "size_bytes": size_bytes,
            "size_gb": size_bytes / (1024 ** 3),
            "n_vids": 5,
        }],
    })

    assert rows[0]["size"] == "16 MB"
    assert rows[0]["size_bytes"] == size_bytes


def _search_row(
    row_id,
    *,
    video_id="video-1",
    start=10.0,
    end=12.0,
    text="Repeated words",
    path="X:/Archive/.Transcript.jsonl",
):
    # Production SELECT order. ``end`` is intentionally appended so all
    # established result indexes (especially timestamp at index 8) stay put.
    return (
        row_id,
        video_id,
        "Fixture title",
        "Fixture channel",
        start,
        text,
        path,
        "snippet",
        123,
        end,
    )


def test_search_dedupe_collapses_overlapping_legacy_caption_copy():
    rows = [
        _search_row(1, start=10.0, end=12.0, text="Repeated words",
                    path="X:/Archive/.Transcript.jsonl"),
        _search_row(2, start=10.2, end=12.3, text=" repeated   WORDS ",
                    path=r"X:\Archive\.Transcript.jsonl"),
    ]

    result = index_search._dedupe_segment_hits(rows)

    assert [row[0] for row in result] == [1]


def test_search_dedupe_preserves_real_repeated_speech_and_other_videos():
    rows = [
        _search_row(1, start=10.0, end=11.0),
        # Touching at the boundary is consecutive speech, not overlap.
        _search_row(2, start=11.0, end=12.0, text="repeated words"),
        _search_row(3, video_id="video-2", start=10.2, end=10.8,
                    text="REPEATED WORDS"),
        _search_row(4, start=10.2, end=10.8, text="REPEATED WORDS",
                    path="Y:/Other/.Transcript.jsonl"),
    ]

    result = index_search._dedupe_segment_hits(rows)

    assert [row[0] for row in result] == [1, 2, 3, 4]


def test_search_fts_appended_end_time_does_not_shift_public_result_fields(
    monkeypatch,
):
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.executescript(
        """
        CREATE TABLE videos(video_id TEXT, upload_ts INTEGER, added_ts INTEGER);
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
            jsonl_path TEXT,
            words TEXT
        );
        CREATE VIRTUAL TABLE segments_fts USING fts5(
            text, content='segments', content_rowid='id'
        );
        INSERT INTO videos VALUES('video-1', 123, 99);
        INSERT INTO segments VALUES
            (1,'video-1','Fixture title','Fixture channel',2024,1,
             10.0,12.0,'repeated words','X:/Archive/.Transcript.jsonl',''),
            (2,'video-1','Fixture title','Fixture channel',2024,1,
             10.2,12.3,'REPEATED  WORDS','X:\\Archive\\.Transcript.jsonl',''),
            (3,'video-1','Fixture title','Fixture channel',2024,1,
             15.0,16.0,'repeated words','X:/Archive/.Transcript.jsonl','');
        INSERT INTO segments_fts(segments_fts) VALUES('rebuild');
        """
    )
    fake_index = SimpleNamespace(
        _reader_open=lambda: conn,
        _reader_lock=threading.Lock(),
    )
    monkeypatch.setattr(index_search, "_index_module", lambda: fake_index)

    try:
        result = index_search.search_fts("repeated")
    finally:
        conn.close()

    assert [row["segment_id"] for row in result] == [1, 3]
    assert result[0]["title"] == "Fixture title"
    assert result[0]["channel"] == "Fixture channel"
    assert result[0]["start_time"] == 10.0
    assert result[0]["text"] == "repeated words"
    assert result[0]["jsonl_path"] == "X:/Archive/.Transcript.jsonl"
    assert result[0]["added_ts"] == 123
    assert result[0]["upload_ts"] == 123
