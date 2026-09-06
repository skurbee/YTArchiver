from __future__ import annotations

import threading
from contextlib import contextmanager

from backend import index as _backend_index  # noqa: F401 - initializes split index modules
from backend import index_bookmarks, index_search
from backend.api_mixins import browse_mixin, recent_mixin


def test_global_video_rows_identify_current_subscriptions(monkeypatch):
    monkeypatch.setattr(
        recent_mixin.index_backend,
        "list_all_videos",
        lambda **_kwargs: {
            "rows": [
                {"title": "Current", "channel": "Current Folder"},
                {"title": "Former", "channel": "Former Folder"},
            ],
            "has_more": False,
            "offset": 0,
        },
    )
    api = recent_mixin.RecentMixin()
    api._config = {
        "channels": [{
            "name": "Current display name",
            "folder": "Current Folder",
        }],
    }

    result = api.list_all_videos()

    assert [row["tracked"] for row in result["rows"]] == [True, False]


def test_recent_resolve_uses_payload_channel_for_subscription_provenance(
        monkeypatch):
    api = recent_mixin.RecentMixin()
    api._config = {
        "channels": [{"name": "Current Channel", "folder": "Current Folder"}],
        "recent_downloads": [{
            "title": "Fixture",
            "channel": "Current Channel",
            "filepath": "C:/Fixture/fixture.mp4",
            "video_id": "abc123def45",
        }],
    }
    monkeypatch.setattr(
        api, "_recent_lookup_path_from_identity",
        lambda _identity: "C:/Fixture/fixture.mp4")

    result = api.recent_resolve({
        "title": "Fixture",
        "channel": "Current Channel",
        "filepath": "C:/Fixture/fixture.mp4",
    })

    assert result["tracked"] is True


class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeIndex:
    def __init__(self, rows):
        self.connection = self
        self.rows = rows
        self.calls = []
        self._reader_lock = threading.Lock()

    def _reader_open(self):
        return self.connection

    @contextmanager
    def _interactive_reader(self, _operation):
        yield self.connection

    def execute(self, sql, args):
        self.calls.append((sql, list(args)))
        return _Cursor(self.rows)


def test_transcript_search_applies_exact_graph_bucket_timestamps(monkeypatch):
    start = 1_704_067_200.0
    end = start + (7 * 86_400)
    fake = _FakeIndex([(
        1, "abc123def45", "Fixture", "Channel", 0.0, "fixture text",
        "fixture.jsonl", "<mark>fixture</mark>", start, 1.0,
    )])
    monkeypatch.setattr(index_search, "_index_module", lambda: fake)
    monkeypatch.setattr(
        index_search, "normalized_reads_enabled", lambda _conn: False)

    rows = index_search.search_fts(
        "fixture", date_from_ts=start, date_to_ts=end)

    assert len(rows) == 1
    sql, args = fake.calls[0]
    assert "SELECT MIN(vt.upload_ts) FROM videos vt" in sql
    assert "vt.channel=s.channel AND vt.title=s.title" in sql
    assert ", v.added_ts, 0) >= ?" in sql
    assert ", v.added_ts, 0) < ?" in sql
    assert start in args
    assert end in args


def test_title_search_applies_exact_graph_bucket_timestamps(monkeypatch):
    start = 1_709_251_200.0
    end = 1_711_929_600.0
    fake = _FakeIndex([(
        "abc123def45", "Fixture", "Channel", "fixture.mp4", 2024, start,
    )])
    monkeypatch.setattr(index_search, "_index_module", lambda: fake)
    monkeypatch.setattr(
        index_search, "normalized_reads_enabled", lambda _conn: False)
    with index_search._title_search_cache_lock:
        index_search._title_search_cache.clear()

    rows = index_search.search_video_titles(
        "fixture", date_from_ts=start, date_to_ts=end)

    assert len(rows) == 1
    sql, args = fake.calls[0]
    assert "v.upload_ts >= ?" in sql
    assert "v.upload_ts < ?" in sql
    assert start in args
    assert end in args


def test_manual_catalog_failure_is_not_reported_as_empty(monkeypatch):
    monkeypatch.setattr(
        browse_mixin.index_backend,
        "list_manual_duplicate_filepaths",
        list,
    )
    monkeypatch.setattr(
        browse_mixin.index_backend,
        "list_manual_videos",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("Fixture catalog unavailable")),
    )
    api = browse_mixin.BrowseMixin()
    api._config = {"video_out_dir": ""}

    result = api.list_manual_videos()

    assert result["rows"] == []
    assert result["error"] == "Fixture catalog unavailable"


def test_segment_resolver_uses_catalog_channel_above_year_folder(
        monkeypatch, tmp_path):
    channel_dir = tmp_path / "Current Channel" / "2024"
    channel_dir.mkdir(parents=True)
    jsonl_path = channel_dir / "Fixture.jsonl"
    video_path = channel_dir / "Fixture.mp4"
    jsonl_path.write_text("", encoding="utf-8")
    video_path.write_bytes(b"fixture")

    fake = _FakeIndex([(
        str(video_path), "Fixture", "Current Channel", 240, 1704067200, 1200, None,
    )])
    monkeypatch.setattr(_backend_index, "_reader_open", fake._reader_open)
    monkeypatch.setattr(
        browse_mixin.file_ops,
        "assert_within_managed_roots",
        lambda path, **_kwargs: {"ok": True, "path": path},
    )
    monkeypatch.setattr(
        browse_mixin,
        "load_config",
        lambda: {
            "channels": [{"name": "Current", "folder": "Current Channel"}],
        },
    )
    api = browse_mixin.BrowseMixin()
    api._config = {
        "channels": [{"name": "Current", "folder": "Current Channel"}],
    }

    result = api.browse_resolve_segment(
        str(jsonl_path), "abc123def45", "Fixture")

    assert result["ok"] is True
    assert result["channel"] == "Current Channel"
    assert result["tracked"] is True
    assert result["duration"] == "4:00"
    assert result["views"] == "1.2K"
    assert result["upload_ts"] == 1704067200


def test_bookmark_catalog_unavailable_raises_instead_of_looking_empty(
        monkeypatch):
    monkeypatch.setattr(index_bookmarks._idx, "_reader_open", lambda: None)

    try:
        index_bookmarks.bookmark_list()
    except RuntimeError as error:
        assert "unavailable" in str(error).lower()
    else:
        raise AssertionError("bookmark_list should report an unavailable catalog")
