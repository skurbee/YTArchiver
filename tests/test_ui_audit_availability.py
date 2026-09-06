"""Availability freshness uses complete catalog evidence and committed state."""
import atexit
import copy
import json
import os
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-availability-")
atexit.register(_PROFILE.cleanup)
os.environ["APPDATA"] = str(Path(_PROFILE.name) / "roaming")
os.environ["LOCALAPPDATA"] = str(Path(_PROFILE.name) / "local")

from backend import index, ytarchiver_config
from backend.metadata import refresh_state, refresh_views


class NoopThread:
    def __init__(self, **_kwargs):
        pass

    def start(self):
        pass


class Catalog(dict):
    complete = True


@pytest.fixture
def library(tmp_path, monkeypatch):
    folder = tmp_path / "Fixture"
    folder.mkdir()
    channel = {"name": "Fixture", "folder": "Fixture", "url": "https://www.youtube.com/@Fixture"}
    config = tmp_path / "config.json"
    content = copy.deepcopy(ytarchiver_config.DEFAULT_CONFIG)
    content.update({"output_dir": str(tmp_path), "channels": [channel]})
    config.write_text(json.dumps(content), encoding="utf-8")
    monkeypatch.setattr(ytarchiver_config, "CONFIG_FILE", config)
    monkeypatch.setattr(ytarchiver_config, "APP_DATA_DIR", tmp_path)
    monkeypatch.setattr(ytarchiver_config, "_CFG_CACHE", {"sig": None, "data": None})
    index._shutdown_index()
    index._schema_inited = False
    monkeypatch.setattr(index, "TRANSCRIPTION_DB", tmp_path / "index.db")
    connection = index._open()
    assert connection is not None
    on_disk = []
    for identity, title in (("aaaaaaaaaa1", "Present"), ("bbbbbbbbbb2", "Removed")):
        path = folder / (title + ".mp4")
        path.write_bytes(b"fixture")
        connection.execute(
            "INSERT INTO videos(filepath,video_id,title,channel,removed_from_yt_ts) VALUES(?,?,?,?,NULL)",
            (str(path), identity, title, "Fixture"))
        on_disk.append((identity, title, 2026, 9, str(path)))
    connection.commit()
    (folder / ".Fixture Metadata.jsonl").write_text(json.dumps({
        "id": "aaaaaaaaaa1", "title": "Present", "view_count": 100,
        "like_count": 10, "comment_count": 1}) + "\n", encoding="utf-8")
    catalog = Catalog({"aaaaaaaaaa1": {"title": "Present", "view_count": 100,
                                      "like_count": 10, "comment_count": 1}})
    monkeypatch.setattr(refresh_views, "_folder_for_channel", lambda _channel: folder)
    monkeypatch.setattr(refresh_views, "find_yt_dlp", lambda: "fixture-unused-tool")
    monkeypatch.setattr(refresh_views, "_scan_channel_videos", lambda _folder: list(on_disk))
    monkeypatch.setattr(refresh_views, "_flat_playlist_bulk_stats", lambda *args, **kwargs: catalog)
    monkeypatch.setattr(refresh_views, "threading", SimpleNamespace(
        Thread=NoopThread, current_thread=threading.current_thread))
    monkeypatch.setattr(refresh_views, "fetch_single_video_metadata", mock.Mock(
        side_effect=AssertionError("Fixture must never start a per-video network request")))
    yield SimpleNamespace(channel=channel, config=config, connection=connection,
                          on_disk=on_disk, catalog=catalog)
    index._shutdown_index()
    index._schema_inited = False


def saved_channel(library):
    return json.loads(library.config.read_text(encoding="utf-8"))["channels"][0]


def run_refresh(library, **kwargs):
    return refresh_views.bulk_refresh_views_likes(library.channel, mock.Mock(), **kwargs)


def test_complete_unscoped_refresh_stamps_count_after_availability_commit(library):
    result = run_refresh(library)
    assert result["ok"]
    saved = saved_channel(library)
    assert saved["last_availability_check_ts"] > 0
    assert saved["availability_checked_count"] == 2
    assert saved["last_views_refresh_ts"] > 0
    assert library.connection.execute(
        "SELECT removed_from_yt_ts FROM videos WHERE video_id='bbbbbbbbbb2'").fetchone()[0] > 0
    assert library.connection.execute(
        "SELECT removed_from_yt_ts FROM videos WHERE video_id='aaaaaaaaaa1'").fetchone()[0] is None


@pytest.mark.parametrize("scope", [{"year": 2026}, {"days": 30}])
def test_scoped_refresh_does_not_certify_whole_archive(library, monkeypatch, scope):
    monkeypatch.setattr(refresh_views, "_filter_recent_on_disk", lambda rows, *_args: rows[:1])
    run_refresh(library, scope=scope)
    saved = saved_channel(library)
    assert saved["last_views_refresh_ts"] > 0
    assert "last_availability_check_ts" not in saved
    assert "availability_checked_count" not in saved


def test_incomplete_catalog_never_marks_missing_videos_or_certifies_archive(library):
    library.catalog.complete = False
    run_refresh(library)
    assert "last_availability_check_ts" not in saved_channel(library)
    assert library.connection.execute(
        "SELECT COUNT(*) FROM videos WHERE removed_from_yt_ts IS NOT NULL").fetchone()[0] == 0


@pytest.mark.parametrize("problem", ["unresolved", "not_indexed", "identity_conflict"])
def test_unresolved_or_unindexed_local_file_prevents_full_certification(library, problem):
    if problem == "unresolved":
        library.on_disk.append(("", "Unresolved title", 2026, 9,
                                str(Path(library.on_disk[0][4]).with_name("Unknown.mp4"))))
    elif problem == "not_indexed":
        library.connection.execute("DELETE FROM videos WHERE video_id='bbbbbbbbbb2'")
    else:
        library.connection.execute("UPDATE videos SET video_id='cccccccccc3' WHERE video_id='bbbbbbbbbb2'")
    library.connection.commit()
    run_refresh(library)
    assert "last_availability_check_ts" not in saved_channel(library)


def test_availability_database_write_failure_rolls_back_and_does_not_certify(library):
    library.connection.execute("CREATE TRIGGER fixture_reject_availability BEFORE UPDATE OF removed_from_yt_ts "
                               "ON videos BEGIN SELECT RAISE(ABORT, 'fixture write failure'); END")
    library.connection.commit()
    run_refresh(library)
    assert "last_availability_check_ts" not in saved_channel(library)
    assert library.connection.execute(
        "SELECT COUNT(*) FROM videos WHERE removed_from_yt_ts IS NOT NULL").fetchone()[0] == 0


def test_cancelled_refresh_is_not_stamped_fresh(library):
    cancel = threading.Event()
    cancel.set()
    run_refresh(library, cancel_event=cancel)
    saved = saved_channel(library)
    assert "last_views_refresh_ts" not in saved
    assert "last_availability_check_ts" not in saved


def test_freshness_details_fail_atomically_when_config_cannot_be_saved(library, monkeypatch):
    before = library.config.read_bytes()
    monkeypatch.setattr(ytarchiver_config, "save_config", lambda _config: False)
    assert not refresh_state.stamp_channel_refresh(library.channel, "last_availability_check_ts",
        details={"availability_checked_count": 2})
    assert library.config.read_bytes() == before
