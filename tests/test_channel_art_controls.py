"""Artwork prepares a sync without bypassing its request and lifecycle controls."""

from __future__ import annotations

import json
import subprocess
import threading
import urllib.error
from unittest import mock

import pytest

from backend import channel_art, youtube_session

_CHANNEL = "https://www.youtube.com/@example"
_IMAGE = b"\x89PNG\r\n\x1a\n" + b"image bytes"
_METADATA = json.dumps({"thumbnails": [
    {"id": "avatar", "url": "https://yt3.ggpht.com/avatar.jpg"},
    {"id": "banner", "url": "https://yt3.ggpht.com/banner.jpg"},
]})


@pytest.fixture(autouse=True)
def offline():
    with mock.patch.object(channel_art.youtube_traffic, "acquire",
                           return_value={"ok": True}) as acquire, \
            mock.patch.object(channel_art, "find_yt_dlp", return_value="yt-dlp"), \
            mock.patch.object(channel_art, "_find_cookie_source", return_value=[]), \
            mock.patch.object(channel_art, "hide_file_win"), \
            mock.patch.object(youtube_session, "handle_youtube_failure_text",
                              return_value="") as classify, \
            mock.patch.object(channel_art.urllib.request, "urlopen",
                              side_effect=AssertionError("Unexpected HTTP request")):
        yield acquire, classify


def _probe(stdout=_METADATA, stderr="", returncode=0):
    return subprocess.CompletedProcess(["yt-dlp"], returncode, stdout, stderr)


def _response(data=_IMAGE, on_read=None):
    resp = mock.MagicMock()
    resp.__enter__.return_value = resp
    resp.headers = {}

    def read(_limit):
        if on_read:
            on_read()
        return data

    resp.read.side_effect = read
    return resp


def test_probe_and_images_share_budget_and_controls(tmp_path, offline):
    acquire, _ = offline
    cancel, pause = threading.Event(), threading.Event()
    with mock.patch.object(channel_art, "run_ytdlp", return_value=_probe()) as run, \
            mock.patch.object(channel_art.urllib.request, "urlopen",
                              return_value=_response()) as http:
        result = channel_art.fetch_channel_art(
            _CHANNEL, str(tmp_path), cancel_event=cancel, pause_event=pause)

    assert result["ok"] and not result["partial"]
    assert [call.args[0] for call in acquire.call_args_list] == [
        "channel_art", "channel_art_image", "channel_art_image"]
    assert all(call.kwargs == {"cancel_event": cancel, "pause_event": pause}
               for call in acquire.call_args_list)
    assert run.call_args.kwargs["request_cancel_event"] is cancel
    assert run.call_args.kwargs["request_pause_event"] is pause
    assert http.call_count == 2
    assert (tmp_path / ".ChannelArt" / "avatar.jpg").read_bytes() == _IMAGE
    assert (tmp_path / ".ChannelArt" / "banner.jpg").read_bytes() == _IMAGE
    assert (tmp_path / ".ChannelArt" / ".last_attempt").is_file()


@pytest.mark.parametrize("url", ["file:///secret.png", "https:///missing-host", ""])
def test_invalid_image_url_has_no_budget_charge(tmp_path, offline, url):
    acquire, _ = offline
    assert not channel_art._http_get(url, str(tmp_path / "avatar.jpg"))
    acquire.assert_not_called()


def test_fresh_artwork_is_local_and_has_no_budget_charge(tmp_path, offline):
    acquire, _ = offline
    art = tmp_path / ".ChannelArt"
    art.mkdir()
    for name in ("avatar.jpg", "banner.jpg"):
        (art / name).write_bytes(_IMAGE)
    with mock.patch.object(channel_art, "run_ytdlp") as run:
        assert channel_art.fetch_channel_art(_CHANNEL, str(tmp_path))["skipped"]
    acquire.assert_not_called()
    run.assert_not_called()


@pytest.mark.parametrize("url", ["file:///secret.png", "https:///missing-host", "https://[invalid"])
def test_invalid_channel_url_is_a_cosmetic_failure(tmp_path, offline, url):
    acquire, _ = offline
    result = channel_art.fetch_channel_art(url, str(tmp_path))
    assert not result["ok"]
    assert not (tmp_path / ".ChannelArt").exists()
    acquire.assert_not_called()


def test_cancel_before_preparation_does_not_create_art_folder(tmp_path, offline):
    acquire, _ = offline
    cancel = threading.Event()
    cancel.set()
    result = channel_art.fetch_channel_art(_CHANNEL, str(tmp_path), cancel_event=cancel)
    assert result["cancelled"]
    assert not (tmp_path / ".ChannelArt").exists()
    acquire.assert_not_called()


def test_cancel_while_waiting_for_permission_never_starts_probe(tmp_path, offline):
    acquire, _ = offline
    cancel = threading.Event()

    def cancelled(*_args, **_kwargs):
        cancel.set()
        return {"ok": True}

    acquire.side_effect = cancelled
    with mock.patch.object(channel_art, "run_ytdlp") as run:
        result = channel_art.fetch_channel_art(
            _CHANNEL, str(tmp_path), cancel_event=cancel)
    assert result["cancelled"]
    run.assert_not_called()
    assert not (tmp_path / ".ChannelArt" / ".last_attempt").exists()


def test_cancelled_probe_does_not_retry_with_cookies(tmp_path):
    cancel = threading.Event()

    def cancelled(*_args, **_kwargs):
        cancel.set()
        return _probe("", "cancelled", 1)

    with mock.patch.object(channel_art, "run_ytdlp", side_effect=cancelled) as run, \
            mock.patch.object(channel_art, "_find_cookie_source") as cookies:
        result = channel_art.fetch_channel_art(
            _CHANNEL, str(tmp_path), cancel_event=cancel)
    assert result["cancelled"]
    assert run.call_count == 1
    cookies.assert_not_called()


def test_public_rate_limit_is_classified_before_cookie_lookup(tmp_path, offline):
    _, classify = offline
    classify.return_value = "rate_limit"
    with mock.patch.object(channel_art, "run_ytdlp",
                           return_value=_probe("", "HTTP Error 429", 1)) as run, \
            mock.patch.object(channel_art, "_find_cookie_source") as cookies:
        result = channel_art.fetch_channel_art(_CHANNEL, str(tmp_path))
    assert result["rate_limited"]
    assert run.call_count == 1
    cookies.assert_not_called()


def test_rate_limit_stays_fail_closed_if_alert_delivery_fails(tmp_path, offline):
    _, classify = offline
    classify.side_effect = RuntimeError("notification failed")
    with mock.patch.object(channel_art, "run_ytdlp",
                           return_value=_probe("", "HTTP Error 429", 1)) as run, \
            mock.patch.object(channel_art, "_find_cookie_source") as cookies:
        result = channel_art.fetch_channel_art(_CHANNEL, str(tmp_path))
    assert result["rate_limited"]
    assert run.call_count == 1
    cookies.assert_not_called()


def test_manual_pause_waits_then_prepares_art_before_returning(tmp_path):
    pause = threading.Event()
    cancel = threading.Event()
    entered = threading.Event()
    done = threading.Event()
    pause.set()
    result = {}

    def probe(*_args, **_kwargs):
        entered.set()
        return _probe()

    def worker():
        result.update(channel_art.fetch_channel_art(
            _CHANNEL, str(tmp_path), pause_event=pause, cancel_event=cancel))
        done.set()

    with mock.patch.object(channel_art, "run_ytdlp", side_effect=probe), \
            mock.patch.object(channel_art.urllib.request, "urlopen",
                              return_value=_response()):
        thread = threading.Thread(target=worker)
        thread.start()
        try:
            assert not entered.wait(0.15)
            assert not done.is_set()
            pause.clear()
            assert done.wait(2)
            assert result["ok"]
        finally:
            cancel.set()
            pause.clear()
            thread.join(2)
    assert not thread.is_alive()


def test_cancel_during_pause_returns_without_probe(tmp_path):
    cancel, pause = threading.Event(), threading.Event()
    pause.set()
    with mock.patch.object(cancel, "wait", side_effect=lambda _timeout: cancel.set() or True), \
            mock.patch.object(channel_art, "run_ytdlp") as run:
        result = channel_art.fetch_channel_art(
            _CHANNEL, str(tmp_path), cancel_event=cancel, pause_event=pause)
    assert result["cancelled"]
    run.assert_not_called()


def test_cancel_during_image_response_preserves_previous_art(tmp_path):
    cancel = threading.Event()
    dest = tmp_path / "avatar.jpg"
    dest.write_bytes(b"previous image")
    failure = {}
    with mock.patch.object(channel_art.urllib.request, "urlopen",
                           return_value=_response(on_read=cancel.set)):
        assert not channel_art._http_get(
            "https://yt3.ggpht.com/avatar.jpg", str(dest),
            cancel_event=cancel, failure=failure)
    assert failure["cancelled"]
    assert dest.read_bytes() == b"previous image"
    assert not dest.with_suffix(".jpg.tmp").exists()


def test_image_rate_limit_stops_before_second_asset_and_sentinel(tmp_path, offline):
    acquire, classify = offline
    classify.side_effect = ["", "rate_limit"]
    with mock.patch.object(channel_art, "run_ytdlp", return_value=_probe()), \
            mock.patch.object(channel_art.urllib.request, "urlopen",
                              side_effect=urllib.error.HTTPError(
                                  "https://yt3.ggpht.com/avatar.jpg", 429,
                                  "Too Many Requests", {}, None)) as http:
        result = channel_art.fetch_channel_art(_CHANNEL, str(tmp_path))
    assert result["rate_limited"]
    assert http.call_count == 1
    assert acquire.call_count == 2
    assert not (tmp_path / ".ChannelArt" / ".last_attempt").exists()


def test_asset_governor_denial_does_not_create_sentinel(tmp_path, offline):
    acquire, _ = offline
    acquire.side_effect = [{"ok": True}, {"ok": False, "cooldown": True}]
    with mock.patch.object(channel_art, "run_ytdlp", return_value=_probe()):
        result = channel_art.fetch_channel_art(_CHANNEL, str(tmp_path))
    assert result["cooldown"]
    assert not (tmp_path / ".ChannelArt" / ".last_attempt").exists()


def test_cancel_after_last_asset_does_not_write_success_sentinel(tmp_path):
    cancel = threading.Event()

    def image(_url, _path, **_kwargs):
        if _path.endswith("banner.jpg"):
            cancel.set()
        return True

    with mock.patch.object(channel_art, "run_ytdlp", return_value=_probe()), \
            mock.patch.object(channel_art, "_http_get", side_effect=image) as fetch:
        result = channel_art.fetch_channel_art(
            _CHANNEL, str(tmp_path), cancel_event=cancel)
    assert result["cancelled"]
    assert fetch.call_count == 2
    assert not (tmp_path / ".ChannelArt" / ".last_attempt").exists()


def test_bad_image_payload_preserves_previous_art(tmp_path):
    dest = tmp_path / "avatar.jpg"
    dest.write_bytes(b"previous image")
    with mock.patch.object(channel_art.urllib.request, "urlopen",
                           return_value=_response(data=b"<html>failure</html>")):
        assert not channel_art._http_get("https://yt3.ggpht.com/avatar.jpg", str(dest))
    assert dest.read_bytes() == b"previous image"
    assert not dest.with_suffix(".jpg.tmp").exists()
