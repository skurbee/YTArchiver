"""Post-sync previews release their channel on quota waits and cancellation."""

from __future__ import annotations

import io
import threading
from types import SimpleNamespace
from unittest import mock

import pytest

from backend import index, thumbnails
from backend import youtube_traffic as traffic
from backend.metadata import thumbnails_ops
from backend.services.channel_leases import LeaseOwner, channel_aliases, channel_leases
from backend.sync import core

VIDEO_ID = "fixture0001"
IMAGE = b"\xff\xd8\xff" + b"fixture image" * 3


@pytest.fixture
def sweep(tmp_path, monkeypatch):
    core._reset_post_sync_maintenance_for_tests()
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")
    config = {
        "output_dir": str(tmp_path), "channels": [],
        "youtube_traffic_mode": "custom",
        "youtube_traffic_custom_daily": 100,
        "youtube_traffic_custom_hourly": 100,
        "youtube_traffic_custom_min_gap": 0,
        "youtube_traffic_custom_max_gap": 0,
    }
    monkeypatch.setattr(traffic, "load_config", lambda: config)
    monkeypatch.setattr(traffic, "APP_DATA_DIR", tmp_path)
    monkeypatch.setattr(traffic, "REQUEST_MIN_GAP", 0)
    monkeypatch.setattr(traffic, "REQUEST_MAX_GAP", 0)
    monkeypatch.setattr(core, "load_config", lambda: config)
    channel = {"name": "Channel", "folder": "Channel", "url": "https://example.invalid/channel"}
    directory = tmp_path / "Channel"
    directory.mkdir()
    video = directory / "Video.mp4"
    video.write_bytes(b"existing video")
    monkeypatch.setattr(thumbnails_ops, "_folder_for_channel", lambda _ch: directory)
    monkeypatch.setattr(thumbnails_ops, "_scan_channel_videos", lambda _folder: [
        (VIDEO_ID, "Video", None, None, str(video)),
    ])
    monkeypatch.setattr(thumbnails_ops, "_get_metadata_jsonl_path", lambda *_args: (
        str(directory / "metadata.jsonl"), str(directory)))
    monkeypatch.setattr(thumbnails_ops, "_read_metadata_jsonl", lambda _path: {
        VIDEO_ID: {"title": "Video", "thumbnail_url": f"https://i.ytimg.com/vi/{VIDEO_ID}/hqdefault.jpg"},
    })
    monkeypatch.setattr(index, "_open", lambda: None)
    monkeypatch.setattr(index, "invalidate_channel_videos", mock.Mock())
    monkeypatch.setattr(thumbnails, "invalidate_thumb_cache_entry", mock.Mock())
    monkeypatch.setattr(thumbnails, "_mark_thumbnail_changed", mock.Mock())
    network = mock.Mock(side_effect=AssertionError("Unexpected network request"))
    monkeypatch.setattr(thumbnails.urllib.request, "urlopen", network)
    state = SimpleNamespace(config=config, channel=channel, directory=directory,
                            network=network, video=video)
    yield state
    core._reset_post_sync_maintenance_for_tests()
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")
    assert channel_leases.active_snapshot() == ()
    assert video.read_bytes() == b"existing video"


def _start(sweep, *, wait_for_budget=False):
    done = threading.Event()
    result = []

    def run(channel, *, cancel_event):
        result.append(thumbnails_ops.sweep_missing_thumbnails(
            channel, cancel_event=cancel_event, wait_for_budget=wait_for_budget))
        done.set()

    assert core._bg_channel_maintenance("thumbs", run, sweep.channel,
                                        cancel_event=threading.Event())
    return done, result


def _assert_channel_available(sweep):
    acquired = channel_leases.acquire(
        channel_aliases(sweep.channel, paths=[sweep.directory]),
        LeaseOwner("processing", "fixture-job", label="Transcription"), timeout=1)
    assert acquired.ok and acquired.lease is not None
    acquired.lease.release()


@pytest.mark.parametrize("window", ["hourly", "daily"])
def test_exhausted_quota_defers_sweep_and_releases_channel(sweep, window):
    sweep.config[f"youtube_traffic_custom_{window}"] = 1
    assert traffic.acquire("channel_sync")["ok"]
    done, result = _start(sweep)
    assert done.wait(2), "Thumbnail sweep kept the channel locked waiting for quota"
    assert result == [{"checked": 1, "fetched": 0, "missing": 1}]
    _assert_channel_available(sweep)
    sweep.network.assert_not_called()
    assert traffic.status()["daily_used"] == 1
    assert not traffic.wait_status()["active"]
    assert list(sweep.directory.rglob("*.jpg")) == []


def test_cancel_interrupts_request_pacing_and_releases_channel(sweep, monkeypatch):
    monkeypatch.setattr(traffic, "REQUEST_MIN_GAP", 60)
    monkeypatch.setattr(traffic, "REQUEST_MAX_GAP", 60)
    assert traffic.acquire("youtube_thumbnail")["ok"]
    waiting = threading.Event()
    real_wait = thumbnails._ThumbnailRequestCancel.wait

    def observe_wait(event, timeout=None):
        waiting.set()
        return real_wait(event, timeout)

    monkeypatch.setattr(thumbnails._ThumbnailRequestCancel, "wait", observe_wait)
    done, result = _start(sweep)
    assert waiting.wait(2), "Thumbnail sweep never reached request pacing"
    assert core.post_sync_maintenance_cancel()
    assert done.wait(2), "Maintenance cancellation did not reach the thumbnail request"
    assert result[0]["fetched"] == 0
    assert core.post_sync_maintenance_join(2)
    _assert_channel_available(sweep)
    sweep.network.assert_not_called()
    assert not traffic.wait_status()["active"]


def test_available_budget_fetches_and_commits_preview(sweep):
    class Response(io.BytesIO):
        headers = {"Content-Length": str(len(IMAGE))}
        status = 200

    sweep.network.side_effect = lambda *_args, **_kwargs: Response(IMAGE)
    done, result = _start(sweep)
    assert done.wait(2)
    assert result == [{"checked": 1, "fetched": 1, "missing": 0}]
    assert (sweep.directory / ".Thumbnails" / f"Video [{VIDEO_ID}].jpg").read_bytes() == IMAGE
    assert sweep.network.call_count == 1
    assert traffic.status()["daily_used"] == 1
    _assert_channel_available(sweep)


def test_cancellation_after_response_does_not_commit_thumbnail(sweep):
    cancel = threading.Event()

    class Response(io.BytesIO):
        headers = {"Content-Length": str(len(IMAGE))}
        status = 200

        def read(self, *args):
            cancel.set()
            return super().read(*args)

    sweep.network.side_effect = lambda *_args, **_kwargs: Response(IMAGE)
    result = thumbnails_ops.sweep_missing_thumbnails(sweep.channel, cancel_event=cancel)
    assert result["fetched"] == 0
    assert list(sweep.directory.rglob("*.jpg")) == []


def test_manual_sweep_still_waits_for_quota_and_can_be_cancelled(sweep):
    sweep.config["youtube_traffic_custom_hourly"] = 1
    assert traffic.acquire("channel_sync")["ok"]
    entered = threading.Event()
    cancel = threading.Event()
    result = []

    def on_wait(state):
        if state.get("active"):
            entered.set()

    traffic.add_wait_listener(on_wait)
    worker = threading.Thread(target=lambda: result.append(
        thumbnails_ops.sweep_missing_thumbnails(sweep.channel, cancel_event=cancel)), daemon=True)
    worker.start()
    try:
        assert entered.wait(2), "Manual thumbnail sweep unexpectedly skipped quota waiting"
        assert result == []
        cancel.set()
        worker.join(2)
        assert not worker.is_alive()
        assert result[0]["fetched"] == 0
        sweep.network.assert_not_called()
    finally:
        cancel.set()
        worker.join(2)
