from __future__ import annotations

import sqlite3

from backend.api_mixins import browse_mixin
from backend.api_mixins.browse_mixin import BrowseMixin


def _catalog() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE videos (
            id INTEGER PRIMARY KEY,
            channel TEXT,
            filepath TEXT,
            video_id TEXT,
            size_bytes INTEGER,
            downloaded_ts REAL,
            availability TEXT,
            is_duplicate_of INTEGER
        )
    """)
    conn.executemany(
        """INSERT INTO videos(
               channel, filepath, video_id, size_bytes, downloaded_ts,
               availability, is_duplicate_of)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            ("Channel One", r"Z:\one.mp4", "ABCDEFGHIJK", 100, 1,
             "available", None),
            # A second physical copy of the same logical video counts toward
            # disk size, but not the user-facing video count.
            ("Channel One", r"Z:\one-copy.mp4", "ABCDEFGHIJK", 120, 2,
             "available", 1),
            ("Channel One", r"Z:\no-id.mp4", "", 200, 3,
             "available", None),
            ("Channel One", r"Z:\gone.mp4", "LMNOPQRSTUV", 400, 4,
             "missing", None),
            ("Other Channel", r"Z:\other.mp4", "ZYXWVUTSRQP", 800, 5,
             "available", None),
        ],
    )
    return conn


def _render_channel_row(monkeypatch, conn: sqlite3.Connection, cache: dict):
    channel = {
        "name": "Channel One",
        "folder": "Channel One",
        "url": "https://youtube.example/@channelone",
    }

    class Api(BrowseMixin):
        def _browse_fresh_config(self):
            return {"output_dir": "", "channels": [channel]}

    monkeypatch.setattr(
        browse_mixin.archive_scan, "load_disk_cache", lambda: cache)
    monkeypatch.setattr(
        browse_mixin.index_backend, "_reader_open", lambda: conn)
    monkeypatch.setattr(
        browse_mixin.archive_scan, "subscriber_count_from_media",
        lambda _path: None)
    monkeypatch.setattr(
        browse_mixin.archive_scan, "cache_subscriber_counts",
        lambda _updates: True)
    return Api()._browse_list_channels_impl()[0]


def test_missing_disk_cache_record_uses_exact_catalog_totals(monkeypatch):
    conn = _catalog()
    try:
        row = _render_channel_row(monkeypatch, conn, cache={})
        stats = browse_mixin._catalog_stats_for_channel_keys(
            conn, {"channel one"})["channel one"]
    finally:
        conn.close()

    assert row["n_vids"] == 2
    assert row["size_bytes"] == 420
    assert stats == {
        "n_vids": 2,
        "physical_copies": 3,
        "size_bytes": 420,
    }


def test_present_zero_cache_record_is_not_overridden(monkeypatch):
    conn = _catalog()
    url = "https://youtube.example/@channelone"
    cache = {
        url: {
            "num_vids": 0,
            "physical_copies": 0,
            "size_bytes": 0,
            "last_updated": 1,
        },
    }

    def unexpected_fallback(*_args, **_kwargs):
        raise AssertionError("catalog fallback must not run for cached channels")

    monkeypatch.setattr(
        browse_mixin, "_catalog_stats_for_channel_keys", unexpected_fallback)
    try:
        row = _render_channel_row(monkeypatch, conn, cache=cache)
    finally:
        conn.close()

    assert row["n_vids"] == 0
    assert row["size_bytes"] == 0
