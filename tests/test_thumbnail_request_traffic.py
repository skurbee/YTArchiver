"""Direct thumbnail HTTP requests share the governor; all responses are offline."""

from __future__ import annotations

import io
import threading
import time
import urllib.error
from unittest import mock

import pytest

from backend import thumbnails
from backend import youtube_traffic as traffic

VIDEO_ID = "fixture0001"
URL = f"https://i.ytimg.com/vi/{VIDEO_ID}/maxresdefault.jpg"
IMAGE = b"\xff\xd8\xff" + b"fixture image" * 3


class Response(io.BytesIO):
    def __init__(self, data=IMAGE, *, status=200):
        super().__init__(data)
        self.headers = {"Content-Length": str(len(data))}
        self.status = status


@pytest.fixture
def thumbnail(tmp_path, monkeypatch):
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")
    monkeypatch.setattr(traffic, "APP_DATA_DIR", tmp_path)
    config = {
        "channels": [], "youtube_traffic_mode": "custom",
        "youtube_traffic_custom_daily": 100,
        "youtube_traffic_custom_hourly": 100,
        "youtube_traffic_custom_min_gap": 0,
        "youtube_traffic_custom_max_gap": 0,
    }
    monkeypatch.setattr(traffic, "load_config", lambda: config)
    monkeypatch.setattr(traffic, "REQUEST_MIN_GAP", 0)
    monkeypatch.setattr(traffic, "REQUEST_MAX_GAP", 0)
    monkeypatch.setattr(thumbnails, "_mark_thumbnail_changed", mock.Mock())
    network = mock.Mock(side_effect=AssertionError("Unexpected network request"))
    monkeypatch.setattr(thumbnails.urllib.request, "urlopen", network)
    directory = tmp_path / "thumbnails"
    directory.mkdir()
    yield directory, config, network
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")


@pytest.mark.parametrize("url", [
    URL, "https://ytimg.com/thumb.jpg", "https://I.YTIMG.COM./thumb.jpg",
    "https://yt3.ggpht.com/thumb", "https://lh3.googleusercontent.com/thumb",
    "https://www.youtube.com/thumbnail", "https://youtubei.googleapis.com/thumb",
])
def test_real_youtube_hosts_are_recognized(url):
    assert thumbnails._is_youtube_thumbnail_url(url)


@pytest.mark.parametrize("url", [
    "https://notytimg.com/thumb.jpg", "https://ytimg.com.example.invalid/thumb",
    "https://example.invalid/ytimg.com/thumb.jpg",
    "https://ytimg.com@example.invalid/thumb", "file:///tmp/ytimg.com/thumb.jpg",
    "https://[broken/thumb.jpg",
])
def test_other_hosts_and_local_urls_are_not_youtube(url):
    assert not thumbnails._is_youtube_thumbnail_url(url)


def test_permission_is_persisted_before_each_youtube_request(thumbnail):
    directory, _config, network = thumbnail

    def fetch(request, **_kwargs):
        assert request.full_url == URL
        assert traffic.status()["daily_used"] == 1
        assert traffic.status()["hourly_used"] == 1
        return Response()

    network.side_effect = fetch
    assert thumbnails._download_thumbnail(URL, str(directory), "Title", VIDEO_ID)
    assert (directory / f"Title [{VIDEO_ID}].jpg").read_bytes() == IMAGE
    assert network.call_count == 1


@pytest.mark.parametrize("existing_title", ["Title", "Previous title"])
def test_cached_thumbnail_and_title_rename_do_not_spend_requests(
        thumbnail, existing_title):
    directory, _config, network = thumbnail
    (directory / f"{existing_title} [{VIDEO_ID}].jpg").write_bytes(IMAGE)
    assert thumbnails._download_thumbnail(URL, str(directory), "Title", VIDEO_ID)
    network.assert_not_called()
    assert traffic.status()["daily_used"] == 0
    assert (directory / f"Title [{VIDEO_ID}].jpg").read_bytes() == IMAGE


@pytest.mark.parametrize("url", ["https://example.invalid/thumbnail.jpg", "file:///fixture.jpg"])
def test_non_youtube_thumbnail_does_not_spend_youtube_budget(thumbnail, url):
    directory, _config, network = thumbnail
    network.side_effect = lambda *_a, **_k: Response()
    assert thumbnails._download_thumbnail(url, str(directory), "Title", VIDEO_ID)
    assert network.call_count == 1
    assert traffic.status()["daily_used"] == 0


def test_each_fallback_request_consumes_one_unit(thumbnail):
    directory, _config, network = thumbnail
    charges = []

    def fetch(*_args, **_kwargs):
        charges.append(traffic.status()["daily_used"])
        if len(charges) == 1:
            raise urllib.error.HTTPError(URL, 404, "Not Found", {}, None)
        if len(charges) == 2:
            return Response(b"not an image" * 4)
        return Response()

    network.side_effect = fetch
    assert thumbnails._download_thumbnail(URL, str(directory), "Title", VIDEO_ID)
    assert charges == [1, 2, 3]
    assert traffic.status()["hourly_used"] == 3


@pytest.mark.parametrize("returned_response", [False, True])
def test_youtube_429_records_circuit_and_never_tries_fallback(thumbnail, returned_response):
    directory, _config, network = thumbnail
    response = Response(status=429)
    error = urllib.error.HTTPError(URL, 429, "Too Many Requests", {}, response)
    network.side_effect = (lambda *_a, **_k: response) if returned_response else error
    assert not thumbnails._download_thumbnail(URL, str(directory), "Title", VIDEO_ID)
    assert network.call_count == 1
    assert response.closed
    assert list(directory.iterdir()) == []
    assert traffic.status()["daily_used"] == 1
    assert traffic.acquire("youtube_http")["cooldown"]


def test_other_site_429_does_not_set_youtube_circuit(thumbnail):
    directory, _config, network = thumbnail
    url = "https://example.invalid/thumb.jpg"
    network.side_effect = urllib.error.HTTPError(url, 429, "Too Many Requests", {}, None)
    assert not thumbnails._download_thumbnail(url, str(directory), "Title", VIDEO_ID)
    assert network.call_count == 1
    assert not traffic.circuit_state()["active"]
    assert traffic.status()["daily_used"] == 0


def test_commit_cancellation_interrupts_real_budget_wait(thumbnail):
    directory, config, network = thumbnail
    config["youtube_traffic_custom_hourly"] = 1
    assert traffic.acquire("channel_sync")["ok"]
    permitted = threading.Event()
    permitted.set()
    result = []
    worker = threading.Thread(target=lambda: result.append(
        thumbnails._download_thumbnail(
            URL, str(directory), "Title", VIDEO_ID,
            commit_allowed=permitted.is_set)), daemon=True)
    worker.start()
    try:
        deadline = time.monotonic() + 2
        while not traffic.wait_status()["active"]:
            assert time.monotonic() < deadline, "Thumbnail never reached budget wait"
            time.sleep(0.01)
        permitted.clear()
        worker.join(1)
        assert not worker.is_alive()
        assert result == [False]
        network.assert_not_called()
        assert list(directory.iterdir()) == []
        assert traffic.status()["hourly_used"] == 1
        assert not traffic.wait_status()["active"]
    finally:
        permitted.clear()
        worker.join(1)


def test_failed_ledger_write_does_not_request_or_try_fallback(thumbnail, monkeypatch):
    directory, _config, network = thumbnail
    monkeypatch.setattr(traffic, "_append_locked", lambda _row: False)
    assert not thumbnails._download_thumbnail(URL, str(directory), "Title", VIDEO_ID)
    network.assert_not_called()
    assert traffic.status()["daily_used"] == 0


def test_cancellation_after_request_preserves_existing_thumbnail(thumbnail):
    directory, _config, network = thumbnail
    existing = directory / f"Title [{VIDEO_ID}].jpg"
    existing.write_bytes(IMAGE)
    permitted = threading.Event()
    permitted.set()

    class CancelOnRead(Response):
        def read(self, *args):
            permitted.clear()
            return super().read(*args)

    network.side_effect = lambda *_a, **_k: CancelOnRead(IMAGE + b"new")
    assert not thumbnails._download_thumbnail(
        URL, str(directory), "Title", VIDEO_ID, force=True,
        commit_allowed=permitted.is_set)
    assert existing.read_bytes() == IMAGE
    assert list(directory.iterdir()) == [existing]
    assert traffic.status()["daily_used"] == 1
