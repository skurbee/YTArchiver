"""Cache, checkpoint and executor ownership, using only disposable fixtures."""
import copy
import gc
import json
import sqlite3
import threading
import weakref
from contextlib import contextmanager
from datetime import UTC, datetime
from unittest import mock

import pytest

from backend import index
from backend.executor_utils import run_bounded
from backend.metadata import fetcher, io, refresh_comments, scan
from backend.process_runner import StreamingRunResult


def test_executor_releases_consumed_payloads_before_pass_finishes():
    class Payload:
        pass
    refs, alive = [], []
    def work(item):
        payload = Payload()
        refs.append(weakref.ref(payload))
        return payload
    def consume(result):
        gc.collect()
        alive.append(sum(ref() is not None for ref in refs))
    outcome = run_bounded(range(30), work, consume, max_workers=1, thread_name_prefix="fixture-release")
    assert outcome.completed == 30 and not outcome.cancelled
    assert max(alive) <= 2
    gc.collect()
    assert all(ref() is None for ref in refs)


def test_executor_cancellation_after_callback_still_accounts_for_running_workers():
    cancel, entered, release, done = (threading.Event() for _ in range(4))
    def work(item):
        if item == 0:
            assert entered.wait(2)
            return item
        entered.set()
        release.wait(2)
        done.set()
        return item
    try:
        outcome = run_bounded(range(20), work, lambda result: cancel.set(), max_workers=2,
                              thread_name_prefix="fixture-cancel", is_cancelled=cancel.is_set)
        assert outcome.cancelled and outcome.completed == 1 and outcome.unfinished == 1
    finally:
        release.set()
        assert done.wait(2)


def test_cached_physical_scan_observes_catalog_only_repair_and_removal(monkeypatch, tmp_path):
    video = tmp_path / "A title.mp4"
    video.write_bytes(b"fixture")
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE videos(filepath TEXT, video_id TEXT)")
    @contextmanager
    def reader(**kwargs):
        yield connection
    monkeypatch.setattr(index, "catalog_session", lambda: mock.Mock(reader=reader))
    monkeypatch.setattr(scan, "_channel_fingerprint", lambda folder: 123.0)
    monkeypatch.setattr(scan, "_scan_videos_cache", {})
    try:
        first = scan._scan_channel_videos(tmp_path)
        connection.execute("INSERT INTO videos VALUES (?,?)", (str(video), "abcdefghij1"))
        with mock.patch.object(scan.os, "walk", side_effect=AssertionError("cache hit must not walk")):
            repaired = scan._scan_channel_videos(tmp_path)
            repaired.clear()  # caller changes must not alter the cached enumeration
            assert scan._scan_channel_videos(tmp_path)[0][0] == "abcdefghij1"
            connection.execute("DELETE FROM videos")
            assert scan._scan_channel_videos(tmp_path)[0][0] == ""
        assert first[0][0] == ""
    finally:
        connection.close()


@pytest.mark.parametrize("include_comments", [True, False])
def test_fetch_checkpoint_is_owned_by_comment_request(monkeypatch, include_comments):
    def supervised(process, **kwargs):
        kwargs["on_stdout_line"](json.dumps({"id": "abcdefghij1", "comments": []}))
        return StreamingRunResult(0, [], output_complete=True)
    monkeypatch.setattr(fetcher, "popen_ytdlp", lambda *a, **k: mock.Mock(pid=None))
    monkeypatch.setattr(fetcher, "supervise_streaming_process", supervised)
    monkeypatch.setattr(fetcher, "_find_cookie_source", list)
    monkeypatch.setattr("backend.youtube_traffic.acquire", lambda *a, **k: {"ok": True})
    result = fetcher._fetch_video_metadata_result("fixture", "abcdefghij1", include_comments=include_comments)
    assert result.metadata is not None
    assert ("comments_fetched_at" in result.metadata) is include_comments
    if include_comments:
        assert result.metadata["comments_fetched_at"] == result.metadata["fetched_at"]


@pytest.mark.parametrize("checkpoint,expected_fetches", [(None, 1), ("2026-01-02T00:00:00Z", 0)])
def test_comment_resume_does_not_treat_stats_timestamp_as_checkpoint(monkeypatch, tmp_path, checkpoint, expected_fetches):
    media = tmp_path / "Title.mp4"
    media.write_bytes(b"fixture")
    metadata = tmp_path / ".Fixture Metadata.jsonl"
    entry = {"video_id": "abcdefghij1", "title": "Title", "comments": [{"text": "old"}],
             "fetched_at": "2026-01-03T00:00:00Z"}
    if checkpoint:
        entry["comments_fetched_at"] = checkpoint
    metadata.write_text(json.dumps(entry) + "\n", encoding="utf-8")
    channel = {"name": "Fixture", "_pass_start_ts": datetime(2026, 1, 1, tzinfo=UTC).timestamp()}
    monkeypatch.setattr(refresh_comments, "_folder_for_channel", lambda channel: tmp_path)
    monkeypatch.setattr(refresh_comments, "find_yt_dlp", lambda: "fixture")
    monkeypatch.setattr(refresh_comments, "_scan_channel_videos", lambda folder: [("abcdefghij1", "Title", None, None, str(media))])
    monkeypatch.setattr(refresh_comments, "stamp_channel_refresh", lambda *args: None)
    fetched = mock.Mock(return_value={"ok": True, "entry": copy.deepcopy(entry)})
    monkeypatch.setattr(refresh_comments, "fetch_single_video_metadata", fetched)
    result = refresh_comments.refresh_channel_comments(channel, mock.Mock())
    assert result["ok"] and fetched.call_count == expected_fetches


def test_legacy_timestamp_policy_is_local_and_invalid_stays_unknown():
    naive = datetime(2025, 1, 15, 12, 0)
    assert io._fetched_at_epoch(naive.isoformat()) == naive.timestamp()
    assert io._fetched_at_epoch("2025-01-15T12:00:00Z") == datetime(2025, 1, 15, 12, tzinfo=UTC).timestamp()
    assert io._fetched_at_epoch("bad") is None


@pytest.mark.parametrize("scope", ["stats", "comments", "all"])
def test_scoped_metadata_writer_preserves_or_updates_comment_checkpoint(monkeypatch, tmp_path, scope):
    video_id = "abcdefghij1"
    old = {"video_id": video_id, "comments": [{"text": "old"}],
           "fetched_at": "2025-01-01T00:00:00Z", "comments_fetched_at": "2025-01-01T00:00:00Z"}
    fresh = {"video_id": video_id, "comments": [], "fetched_at": "2026-01-01T00:00:00Z"}
    if scope != "stats":
        fresh["comments_fetched_at"] = fresh["fetched_at"]
    sidecar = tmp_path / ".Fixture Metadata.jsonl"
    sidecar.write_text(json.dumps(old) + "\n", encoding="utf-8")
    media = tmp_path / "Fixture.mp4"
    media.write_bytes(b"fixture")
    monkeypatch.setattr(fetcher, "find_yt_dlp", lambda: "fixture")
    monkeypatch.setattr(fetcher, "_get_metadata_jsonl_path", lambda *args: (str(sidecar), str(tmp_path)))
    monkeypatch.setattr(fetcher, "_fetch_video_metadata_result",
                        lambda *args, **kwargs: fetcher.MetadataFetchResult.success(fresh))
    monkeypatch.setattr(index, "update_video_stats", mock.Mock())
    result = fetcher.fetch_single_video_metadata(
        {"name": "Fixture"}, video_id, str(media), "Fixture", mock.Mock(),
        refresh=True, dest_folder=str(tmp_path), refresh_scope=scope, refresh_thumbnail=False)
    assert result["ok"]
    saved = io._read_metadata_jsonl(str(sidecar))[video_id]
    assert saved["fetched_at"] == fresh["fetched_at"]
    expected = old if scope == "stats" else fresh
    assert saved["comments_fetched_at"] == expected["comments_fetched_at"]
    assert saved["comments"] == expected["comments"]
