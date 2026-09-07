"""Completed download snapshots avoid extraction without losing archive data."""

import copy
import importlib.util
import json
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
import yt_dlp

from backend.metadata import fetcher

VIDEO_ID = "fixture0001"
CAPTURED_AT = "2026-09-01T12:34:56+00:00"


def _snapshot():
    return {
        "id": VIDEO_ID,
        "_type": "video",
        "title": "Completed video",
        "description": "Saved description",
        "upload_date": "20260831",
        "duration": 91.5,
        "view_count": 1234,
        "like_count": 98,
        "thumbnail": "https://i.ytimg.com/vi/fixture0001/hqdefault.jpg",
        "comments": [{"author": "Viewer", "text": "Saved comment", "like_count": 7,
                      "timestamp": 12345}],
        "comment_count": 1,
        "ytarchiver_metadata_snapshot": {
            "version": 1,
            "video_id": VIDEO_ID,
            "comments_complete": True,
            "comments_disabled": False,
            "fetched_at": CAPTURED_AT,
        },
    }


@pytest.fixture
def archived_video(monkeypatch, tmp_path):
    video = tmp_path / f"Completed video [{VIDEO_ID}].mp4"
    video.write_bytes(b"fixture media")
    info = video.with_suffix(".info.json")
    stored = {}
    monkeypatch.setattr(fetcher, "_folder_for_channel", lambda _: tmp_path)
    monkeypatch.setattr(fetcher, "_get_metadata_jsonl_path",
                        lambda *_: (str(tmp_path / "metadata.jsonl"), str(tmp_path)))
    monkeypatch.setattr(fetcher, "_read_metadata_jsonl",
                        lambda *_a, **_kw: copy.deepcopy(stored))

    def save(_path, entries):
        stored.clear()
        stored.update(copy.deepcopy(entries))

    writer = mock.Mock(side_effect=save)
    monkeypatch.setattr(fetcher, "_write_metadata_jsonl", writer)
    finder = mock.Mock(return_value="fixture-yt-dlp")
    monkeypatch.setattr(fetcher, "find_yt_dlp", finder)
    remote_entry = {"video_id": VIDEO_ID, "title": "Fresh remote title",
                    "comments": [{"text": "Fresh remote comment"}],
                    "fetched_at": "2026-09-02T00:00:00+00:00"}
    remote = mock.Mock(return_value=fetcher.MetadataFetchResult.success(remote_entry))
    monkeypatch.setattr(fetcher, "_fetch_video_metadata_result", remote)
    thumb = mock.Mock(return_value=True)
    monkeypatch.setattr(fetcher, "_download_thumbnail", thumb)
    monkeypatch.setattr(fetcher, "_invalidate_browse_thumbnail_cache", mock.Mock())
    stats = mock.Mock()
    monkeypatch.setattr("backend.index.update_video_stats", stats)
    monkeypatch.setattr("subprocess.Popen", mock.Mock(side_effect=AssertionError("unexpected process")))
    monkeypatch.setattr("urllib.request.urlopen", mock.Mock(side_effect=AssertionError("unexpected network")))

    def write(payload=None):
        info.write_text(json.dumps(_snapshot() if payload is None else payload), encoding="utf-8")

    def run(**kwargs):
        return fetcher.fetch_single_video_metadata(
            {"name": "Fixture Channel"}, VIDEO_ID, str(video), "Completed video",
            mock.Mock(), emit_inline_log=False, **kwargs)

    return SimpleNamespace(video=video, info=info, stored=stored, write=write, run=run,
                           remote=remote, finder=finder, thumb=thumb, stats=stats, writer=writer)


def test_completed_snapshot_saves_comments_stats_and_thumbnail_without_new_extraction(archived_video):
    archive = archived_video
    archive.write()
    archive.finder.return_value = None  # A local import does not need the tool.
    result = archive.run()
    assert result["ok"] and result["fetched"] and result["thumbnail_saved"]
    archive.finder.assert_not_called()
    archive.remote.assert_not_called()
    assert archive.stored[VIDEO_ID] == {
        "video_id": VIDEO_ID, "title": "Completed video", "description": "Saved description",
        "view_count": 1234, "like_count": 98, "comment_count": 1,
        "upload_date": "20260831", "duration": 91.5,
        "thumbnail_url": "https://i.ytimg.com/vi/fixture0001/hqdefault.jpg",
        "comments": [{"author": "Viewer", "text": "Saved comment", "likes": 7, "time": 12345}],
        "fetched_at": CAPTURED_AT, "comments_fetched_at": CAPTURED_AT,
    }
    assert archive.thumb.call_args.args[0] == archive.stored[VIDEO_ID]["thumbnail_url"]
    assert archive.stats.call_args.args[0] == [(VIDEO_ID, 1234, 98, "20260831")]


@pytest.mark.parametrize("disabled", [False, True])
def test_completed_empty_or_disabled_comments_do_not_refetch(archived_video, disabled):
    data = _snapshot()
    data["ytarchiver_metadata_snapshot"]["comments_disabled"] = disabled
    if disabled:
        # yt-dlp's clean info JSON removes None-valued fields.
        del data["comments"]
        del data["comment_count"]
    else:
        data.update(comments=[], comment_count=0)
    archived_video.write(data)
    result = archived_video.run()
    assert result["ok"]
    assert result["entry"]["comments"] == []
    assert result["entry"]["comments_fetched_at"] == CAPTURED_AT
    archived_video.remote.assert_not_called()


@pytest.mark.parametrize("problem", [
    "missing", "invalid_json", "wrong_id", "wrong_marker_id", "legacy", "partial_comments",
    "missing_comments", "false_marker", "wrong_version", "bad_timestamp", "naive_timestamp",
    "flat_entry", "missing_description", "invalid_comment", "disabled_with_comments",
])
def test_incomplete_or_mismatched_snapshot_uses_governed_fetch(archived_video, problem):
    archive = archived_video
    data = _snapshot()
    marker = data["ytarchiver_metadata_snapshot"]
    if problem == "wrong_id":
        data["id"] = "different01"
    elif problem == "wrong_marker_id":
        marker["video_id"] = "different01"
    elif problem == "legacy":
        del data["ytarchiver_metadata_snapshot"]
    elif problem == "partial_comments":
        data["comment_count"] = None
    elif problem == "missing_comments":
        del data["comments"]
    elif problem == "false_marker":
        marker["comments_complete"] = False
    elif problem == "wrong_version":
        marker["version"] = True
    elif problem == "bad_timestamp":
        marker["fetched_at"] = "invalid"
    elif problem == "naive_timestamp":
        marker["fetched_at"] = "2026-09-01T12:34:56"
    elif problem == "flat_entry":
        data["_type"] = "url"
    elif problem == "missing_description":
        del data["description"]
    elif problem == "invalid_comment":
        data["comments"] = ["not a comment object"]
    elif problem == "disabled_with_comments":
        marker["comments_disabled"] = True
    if problem != "missing":
        archive.write(data)
    if problem == "invalid_json":
        archive.info.write_text('{"id":', encoding="utf-8")
    result = archive.run()
    assert result["ok"]
    archive.remote.assert_called_once()
    assert archive.remote.call_args.kwargs["include_comments"] is True
    assert archive.stored[VIDEO_ID]["title"] == "Fresh remote title"


@pytest.mark.parametrize("scope", ["all", "stats", "comments"])
def test_explicit_refresh_ignores_download_snapshot(archived_video, scope):
    archive = archived_video
    archive.write()
    result = archive.run(refresh=True, refresh_scope=scope)
    assert result["ok"]
    archive.remote.assert_called_once()
    assert archive.remote.call_args.kwargs["include_comments"] is (scope != "stats")


def test_cancel_during_snapshot_read_commits_nothing(archived_video, monkeypatch):
    archive = archived_video
    archive.write()
    cancel = threading.Event()
    read = fetcher._read_download_metadata

    def read_and_cancel(*args):
        result = read(*args)
        cancel.set()
        return result

    monkeypatch.setattr(fetcher, "_read_download_metadata", read_and_cancel)
    assert archive.run(cancel_event=cancel)["cancelled"]
    archive.writer.assert_not_called()
    archive.thumb.assert_not_called()
    archive.stats.assert_not_called()
    archive.remote.assert_not_called()


def test_saved_metadata_is_not_overwritten_by_an_older_download_snapshot(archived_video):
    archive = archived_video
    archive.write()
    archive.stored[VIDEO_ID] = {"title": "Later refreshed metadata"}
    assert archive.run()["skipped"]
    assert archive.stored[VIDEO_ID]["title"] == "Later refreshed metadata"
    archive.writer.assert_not_called()
    archive.remote.assert_not_called()


def test_legacy_filename_suffix_is_supported_without_searching_other_files(archived_video):
    archive = archived_video
    archive.write()
    archive.info.rename(str(archive.video) + ".info.json")
    assert archive.run()["ok"]
    archive.remote.assert_not_called()


@pytest.mark.parametrize("comment_mode", ["comments", "empty", "disabled"])
def test_real_ytdlp_extraction_persists_reusable_metadata_snapshot(
        archived_video, monkeypatch, comment_mode):
    """Exercise yt-dlp's actual PP, post-extraction and info-JSON write order."""
    from yt_dlp.extractor.common import InfoExtractor

    archive = archived_video
    events = []

    class SnapshotFixtureIE(InfoExtractor):
        _VALID_URL = r"snapshotfixture:(?P<id>[A-Za-z0-9_-]+)"

        def _get_comments(self):
            events.append("comments")
            if comment_mode == "disabled":
                raise self.CommentsDisabled
            if comment_mode == "comments":
                yield {"author": "Viewer", "text": "Saved comment", "like_count": 7}

        def _real_extract(self, url):
            events.append("video extraction")
            info = _snapshot()
            for key in ("comments", "comment_count", "ytarchiver_metadata_snapshot"):
                del info[key]
            info.update(id=self._match_id(url),
                        url="https://example.test/fixture.mp4", ext="mp4")
            info["__post_extractor"] = self.extract_comments()
            return info

    plugin_path = (Path(__file__).resolve().parents[1] / "backend" / "yt_dlp_plugins"
                   / "ytarchiver" / "yt_dlp_plugins" / "postprocessor" / "ytarchiver_traffic.py")
    spec = importlib.util.spec_from_file_location("snapshot_traffic_guard", plugin_path)
    plugin = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(plugin)
    monkeypatch.setenv("YTARCHIVER_TRAFFIC_PORT", "12345")
    monkeypatch.setenv("YTARCHIVER_TRAFFIC_TOKEN", "fixture_token_" + "a" * 32)
    logger = mock.Mock()
    with yt_dlp.YoutubeDL({
        "getcomments": True, "writeinfojson": True, "skip_download": True,
        "ignoreerrors": True, "quiet": True, "logger": logger,
        "outtmpl": str(archive.video), "cachedir": False,
    }, auto_init=False) as downloader:
        monkeypatch.setattr(downloader, "urlopen", mock.Mock(
            side_effect=AssertionError("unexpected yt-dlp network request")))
        rpc = mock.Mock(side_effect=AssertionError("unexpected broker request"))
        monkeypatch.setattr(plugin.YTArchiverTrafficGuardPP, "_rpc", rpc)
        downloader.add_info_extractor(SnapshotFixtureIE())
        downloader.add_post_processor(plugin.YTArchiverTrafficGuardPP(downloader),
                                      when="pre_process")
        downloader.extract_info(f"snapshotfixture:{VIDEO_ID}", download=True)
    logger.error.assert_not_called()
    assert events == ["video extraction", "comments"]
    rpc.assert_not_called()
    persisted = json.loads(archive.info.read_text(encoding="utf-8"))
    marker = persisted["ytarchiver_metadata_snapshot"]
    assert marker["video_id"] == VIDEO_ID
    assert marker["comments_complete"] is True
    assert marker["comments_disabled"] is (comment_mode == "disabled")
    result = archive.run()
    assert result["ok"] and result["thumbnail_saved"]
    archive.remote.assert_not_called()
    archive.finder.assert_not_called()
    archive.thumb.assert_called_once()
    assert result["entry"]["comments_fetched_at"] == marker["fetched_at"]
    if comment_mode == "comments":
        assert result["entry"]["comments"] == [
            {"author": "Viewer", "text": "Saved comment", "likes": 7, "time": ""}]
    else:
        assert result["entry"]["comments"] == []
    assert archive.stored[VIDEO_ID]["description"] == "Saved description"
    assert archive.stats.call_args.args[0] == [(VIDEO_ID, 1234, 98, "20260831")]
