"""Behavioral checks for explicit catalog/config/filesystem policy boundaries."""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from types import SimpleNamespace

import pytest

from backend.index_bookmarks import BookmarkRepository
from backend.media_identity import (
    canonical_order_sql,
    canonical_sort_key,
    identity_key_for_row,
    logical_key_sql,
    normalize_media_path,
)
from backend.services.catalog_session import CatalogSession, LibraryQueryTimeout
from backend.services.config_snapshot import config_snapshot
from backend.services.managed_roots import ManagedRoots
from backend.services.trash_store import MANIFEST_NAME, TrashStore


@pytest.fixture
def catalog():
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE bookmarks(id INTEGER PRIMARY KEY, video_id TEXT, title TEXT,
            channel TEXT, start_time REAL, text TEXT, note TEXT, created REAL);
        CREATE TABLE videos(id INTEGER PRIMARY KEY, video_id TEXT, filepath TEXT,
            title TEXT, channel TEXT, availability TEXT, is_duplicate_of INTEGER,
            size_bytes INTEGER, year INTEGER, month INTEGER, tx_status TEXT,
            added_ts REAL, upload_ts REAL, view_count INTEGER, like_count INTEGER,
            removed_from_yt_ts REAL, duration_s REAL);
    """)
    lock = threading.RLock()
    session = CatalogSession(
        writer_factory=lambda: conn, reader_factory=lambda: conn,
        independent_factory=lambda: None, writer_lock=lock, reader_lock=lock,
        lock_seconds=lambda: 0.01, query_seconds=lambda: 1.0,
    )
    yield conn, session, BookmarkRepository(session)
    conn.close()


def test_bookmarks_are_independent_and_retry_idempotent(catalog):
    conn, _, bookmarks = catalog
    key = bookmarks.bookmark_add("video", "Title", "Channel", 12.5, "Words", "Note")
    assert bookmarks.bookmark_add("video", "Title", "Channel", 12.5, "Words", "Note") == key
    assert conn.in_transaction is False
    assert bookmarks.bookmark_update_note(key, "Updated")
    assert bookmarks.bookmark_list(query="Updated")[0]["id"] == key
    assert bookmarks.bookmark_remove(key)
    assert not bookmarks.bookmark_remove(key)


def test_session_rolls_back_only_its_failed_transaction(catalog):
    conn, session, _ = catalog
    with pytest.raises(RuntimeError, match="abort"):
        with session.transaction("test"):
            conn.execute("INSERT INTO bookmarks(video_id) VALUES ('rolled-back')")
            raise RuntimeError("abort")
    assert conn.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0] == 0
    conn.execute("INSERT INTO bookmarks(video_id) VALUES ('owned-by-caller')")
    with pytest.raises(LibraryQueryTimeout, match="pending"):
        with session.transaction("test"):
            pytest.fail("must not enter another caller's transaction")
    assert conn.in_transaction
    assert conn.execute("SELECT video_id FROM bookmarks").fetchone()[0] == "owned-by-caller"
    conn.rollback()


def test_session_deadline_restores_connection_busy_timeout(catalog):
    conn, session, _ = catalog
    conn.execute("PRAGMA busy_timeout=1379")
    with pytest.raises(LibraryQueryTimeout):
        with session.bounded_sql(conn, "Expired read", -1):
            conn.execute("SELECT 1").fetchone()
    assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 1379
    assert conn.execute("SELECT 1").fetchone()[0] == 1


def test_session_unavailable_and_admission_failure_do_not_run_work():
    unavailable = CatalogSession(
        writer_factory=lambda: None, reader_factory=lambda: None,
        independent_factory=lambda: None, writer_lock=threading.RLock(),
        reader_lock=threading.RLock(), lock_seconds=lambda: 0.01,
        query_seconds=lambda: 1.0,
    )
    assert unavailable.initialize() is False
    with pytest.raises(RuntimeError, match="unavailable"):
        with unavailable.read("read"):
            pytest.fail("unavailable reader")
    with pytest.raises(RuntimeError, match="unavailable"):
        with unavailable.transaction("write"):
            pytest.fail("unavailable writer")


@pytest.mark.parametrize("row", [
    {"id": 1, "video_id": " abc ", "filepath": "C:/Copy.mp4"},
    {"id": 2, "video_id": None, "filepath": " C:/A/ÉVID.mp4 "},
    {"id": 3, "video_id": "", "filepath": ""},
])
def test_identity_sql_and_projection_preserve_partition_policy(row):
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE videos(id INTEGER, video_id TEXT, filepath TEXT)")
        conn.execute("INSERT INTO videos VALUES (:id,:video_id,:filepath)", row)
        actual = conn.execute(f"SELECT {logical_key_sql()} FROM videos v").fetchone()[0]
        expected, kind = identity_key_for_row(row)
        if kind == "legacy":
            expected = expected.replace("legacy-row:", "row:", 1)
        assert actual == expected
        assert normalize_media_path(" C:/A/ÉVID.mp4 ") == "c:\\a\\Évid.mp4"
    finally:
        conn.close()


def test_canonical_ranking_preserves_availability_and_primary_preference(catalog):
    conn, _, _ = catalog
    rows = [
        {"id": 1, "video_id": "same", "filepath": "a", "availability": "missing", "is_duplicate_of": None},
        {"id": 2, "video_id": "same", "filepath": "b", "availability": "available", "is_duplicate_of": 1},
        {"id": 3, "video_id": "same", "filepath": "c", "availability": "available", "is_duplicate_of": None},
    ]
    conn.executemany("INSERT INTO videos(id,video_id,filepath,availability,is_duplicate_of) "
                     "VALUES (:id,:video_id,:filepath,:availability,:is_duplicate_of)", rows)
    actual = [row[0] for row in conn.execute(f"SELECT id FROM videos v ORDER BY {canonical_order_sql()}")]
    assert actual == [row["id"] for row in sorted(rows, key=canonical_sort_key)] == [3, 2, 1]
    with pytest.raises(ValueError):
        logical_key_sql("v; DROP TABLE videos")


def test_channel_scope_keeps_copy_when_global_primary_belongs_elsewhere(catalog):
    from backend.index import canonical_videos_cte_sql, channel_videos_cte_sql
    conn, _, _ = catalog
    conn.executemany(
        "INSERT INTO videos(id,video_id,filepath,channel,availability,is_duplicate_of) "
        "VALUES (?,?,?,?,?,?)",
        [(1, "shared", "a.mp4", "First", "available", None),
         (2, "shared", "b.mp4", "Second", "available", 1),
         (3, "other", "c.mp4", "Second", "available", None)],
    )
    global_ids = [row[0] for row in conn.execute(
        f"WITH {canonical_videos_cte_sql()} SELECT id FROM canonical_videos ORDER BY id")]
    channel_ids = [row[0] for row in conn.execute(
        f"WITH {channel_videos_cte_sql()} SELECT id FROM channel_videos ORDER BY id", ("Second",))]
    assert global_ids == [1, 3]
    assert channel_ids == [2, 3]


def test_managed_roots_share_enumeration_containment_and_innermost_owner(tmp_path):
    root = tmp_path / "archive"
    nested = root / "manual"
    roots = ManagedRoots.from_config({"output_dir": str(root), "video_out_dir": str(nested),
                                      "tp_archive_roots": [str(root), ""]})
    assert roots.paths == (str(root), str(nested))
    assert roots.contains(str(root))
    assert roots.owner_for(str(nested / "video.mp4")) == str(nested)
    assert not roots.contains(str(tmp_path / "archive-other" / "video.mp4"))
    assert not roots.contains("")
    assert not ManagedRoots.from_config({}).contains(str(root))


def test_managed_roots_resolve_redirects_at_each_check(tmp_path, monkeypatch):
    root = tmp_path / "archive"
    target = str(root / "redirect" / "video.mp4")
    roots = ManagedRoots.from_config({"output_dir": str(root)})
    original = os.path.realpath
    monkeypatch.setattr(os.path, "realpath", lambda path: str(tmp_path / "outside")
                        if str(path) == target else original(path))
    assert not roots.contains(target)


def test_trash_manifest_and_external_restore_marker_share_protocol(tmp_path):
    store = TrashStore()
    entry = tmp_path / ".YTArchiver Trash" / "entry"
    entry.mkdir(parents=True)
    manifest = {"version": 2, "state": "restoring", "files": []}
    path = store.write_manifest(str(entry), manifest)
    assert os.path.basename(path) == MANIFEST_NAME
    assert store.read_manifest(str(entry), archive_root=str(tmp_path)) == (manifest, path)
    marker = store.write_restore_marker(str(entry), manifest, archive_root=str(tmp_path))
    os.unlink(path)
    assert store.read_manifest(str(entry), archive_root=str(tmp_path)) == (manifest, marker)
    assert not store.contains_entry(str(tmp_path / "elsewhere"), str(tmp_path))


def test_trash_failed_publication_preserves_old_manifest(tmp_path, monkeypatch):
    store = TrashStore()
    path = tmp_path / "manifest.json"
    store.publish_object(str(path), {"old": True})
    def refuse_replace(*args):
        raise OSError("publication rejected")
    monkeypatch.setattr(os, "replace", refuse_replace)
    with pytest.raises(OSError, match="publication rejected"):
        store.publish_object(str(path), {"new": True})
    assert json.loads(path.read_text(encoding="utf-8")) == {"old": True}
    assert sorted(p.name for p in tmp_path.iterdir()) == ["manifest.json"]


def test_repository_snapshot_wins_and_is_detached():
    live = {"channels": [{"name": "Current"}]}
    owner = SimpleNamespace(_config={"channels": [{"name": "Stale"}]},
                            services=SimpleNamespace(fresh_config=lambda: live))
    snapshot = config_snapshot(owner, lambda: pytest.fail("unexpected fallback"))
    snapshot["channels"][0]["name"] = "Changed locally"
    assert live["channels"][0]["name"] == "Current"


def test_snapshot_does_not_hide_repository_failure():
    def fail():
        raise OSError("cannot read configuration")
    owner = SimpleNamespace(_config={"stale": True}, services=SimpleNamespace(fresh_config=fail))
    with pytest.raises(OSError, match="cannot read"):
        config_snapshot(owner, dict)


def test_health_summary_uses_repository_even_before_legacy_cache_is_populated(monkeypatch):
    from backend.api_mixins import index_mixin
    api = index_mixin.IndexMixin()
    api._config = None
    api.services = SimpleNamespace(fresh_config=lambda: {"source": "repository"})
    monkeypatch.setattr(index_mixin.archive_scan, "index_summary", lambda cfg=None: cfg)
    assert api.get_index_summary() == {"source": "repository"}


@pytest.mark.parametrize("query", ["buckets", "word"])
def test_graph_handles_reader_becoming_unavailable_between_scopes(catalog, monkeypatch, query):
    from backend import index, index_graph
    conn, _, _ = catalog
    readers = iter([conn, None])
    session = CatalogSession(
        writer_factory=lambda: conn, reader_factory=lambda: next(readers, None),
        independent_factory=lambda: None, writer_lock=threading.RLock(),
        reader_lock=threading.RLock(), lock_seconds=lambda: 0.01,
        query_seconds=lambda: 1.0,
    )
    fake = SimpleNamespace(catalog_session=lambda: session,
                           canonical_videos_cte_sql=index.canonical_videos_cte_sql)
    monkeypatch.setattr(index_graph, "_index", lambda: fake)
    index_graph.invalidate_top_words_cache()
    if query == "buckets":
        assert index_graph.bucket_totals() == {}
    else:
        result = index_graph.graph_word_frequency("example")
        assert result["labels"] == []
        assert result["error"] == "DB unavailable"


def test_background_consumers_report_foreign_transaction_without_committing_it(catalog, tmp_path, monkeypatch):
    from backend import index_graph, index_maintenance
    conn, session, _ = catalog
    path = tmp_path / "video.mp4"
    path.write_bytes(b"video")
    conn.execute("INSERT INTO videos(id,filepath,channel,size_bytes) VALUES (1,?,'Channel',0)",
                 (str(path),))
    assert conn.in_transaction
    fake = SimpleNamespace(catalog_session=lambda: session)
    monkeypatch.setattr(index_graph, "_index", lambda: fake)
    monkeypatch.setattr(index_maintenance, "_idx", fake)
    graph_result = index_graph.backfill_upload_ts()
    size_result = index_maintenance.refresh_channel_file_sizes("Channel", str(tmp_path))
    assert graph_result["filled"] == 0
    assert "active transaction" in graph_result["error"]
    assert size_result["updated"] == 0
    assert "active transaction" in size_result["error"]
    assert conn.in_transaction
    assert conn.execute("SELECT size_bytes,upload_ts FROM videos").fetchone() == (0, None)
    conn.rollback()


def test_config_projection_preserves_public_payload_without_mutating_snapshot():
    from backend.config_views import channels_for_subs_ui
    config = {"channels": [{"name": "Example", "min_duration": 30, "size_bytes": 512,
                            "size_gb": 0, "n_vids": 1}], "output_dir": ""}
    before = json.dumps(config)
    rows, label = channels_for_subs_ui(config)
    assert rows[0]["min"] == "<1m"
    assert rows[0]["folder"] == "Example"
    assert label.endswith("(1 channels)")
    assert json.dumps(config) == before
