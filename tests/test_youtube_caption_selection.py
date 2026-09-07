"""Run real yt-dlp caption selection/writing against offline HTTP responses."""

import importlib.util
import io
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest
import yt_dlp
from yt_dlp.networking import Response
from yt_dlp.networking.exceptions import HTTPError

PLUGIN = (Path(__file__).resolve().parents[1] / "backend" / "yt_dlp_plugins"
          / "ytarchiver" / "yt_dlp_plugins" / "postprocessor" / "ytarchiver_traffic.py")
VTT = b"WEBVTT\n\n00:00:00.000 --> 00:00:02.000\nUsable caption words.\n"


@pytest.fixture
def captions(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location("caption_guard", PLUGIN)
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    monkeypatch.setenv("YTARCHIVER_TRAFFIC_PORT", "12345")
    monkeypatch.setenv("YTARCHIVER_TRAFFIC_TOKEN", "fixture_token_" + "a" * 32)
    requests, charges, responses = [], [], {}
    downloader = yt_dlp.YoutubeDL({
        "writesubtitles": True, "writeautomaticsub": True,
        "subtitleslangs": ["en", "en-orig", "en-US", "en-GB"],
        "subtitlesformat": "vtt", "ignoreerrors": True, "retries": 0,
        "quiet": True, "noprogress": True, "logger": mock.Mock(),
        "outtmpl": str(tmp_path / "%(id)s.%(ext)s"),
    }, auto_init=False)

    def urlopen(request):
        url = request if isinstance(request, str) else request.url
        requests.append(url)
        body = responses.get(url, VTT)
        status = body if isinstance(body, int) else 200
        response = Response(io.BytesIO(body if isinstance(body, bytes) else b"error"), url,
                            {"Content-Type": "text/vtt"}, status=status)
        if status != 200:
            raise HTTPError(response)
        return response

    downloader.urlopen = urlopen
    monkeypatch.setattr(guard.YTArchiverTrafficGuardPP, "_rpc",
                        lambda _self, op, kind: charges.append((op, kind)))
    pp = guard.YTArchiverTrafficGuardPP(downloader)

    def track(label):
        return [{"ext": "vtt", "url": f"https://www.youtube.com/api/timedtext?track={label}"}]

    def download(normal=None, automatic=None):
        information = {"id": "fixture0001", "title": "Fixture", "ext": "mp4"}
        information["requested_subtitles"] = downloader.process_subtitles(
            information["id"], normal or {}, automatic or {})
        paths = downloader._write_subtitles(information, str(tmp_path / "fixture0001.mp4"))
        return information, paths

    yield guard, downloader, pp, track, download, requests, charges, responses
    downloader.close()


def test_one_human_english_track_wins_over_all_auto_variants(captions):
    _guard, _ydl, _pp, track, download, requests, charges, _responses = captions
    information, paths = download({"en-US": track("human-us")}, {
        "en": track("auto-en"), "en-orig": track("auto-original"),
        "en-US": track("auto-us"), "en-GB": track("auto-gb")})
    assert list(information["requested_subtitles"]) == ["en-US"]
    assert len(paths) == len(requests) == 1
    assert requests[0].endswith("track=human-us")
    assert charges == [("acquire", "youtube_caption")]
    assert Path(paths[0][0]).read_bytes() == VTT


def test_original_auto_english_wins_over_translated_variant(captions):
    _guard, _ydl, _pp, track, download, requests, _charges, _responses = captions
    information, paths = download(automatic={"en": track("auto-en"), "en-orig": track("original")})
    assert list(information["requested_subtitles"]) == ["en-orig"]
    assert len(paths) == len(requests) == 1
    assert requests[0].endswith("track=original")


@pytest.mark.parametrize("unusable", [b"", b"WEBVTT\n", 403])
def test_failed_or_empty_track_tries_one_alternate_then_stops(captions, unusable):
    _guard, _ydl, _pp, track, download, requests, charges, responses = captions
    responses[track("human")[0]["url"]] = unusable
    information, paths = download({"en": track("human")}, {
        "en-orig": track("original"), "en": track("translated"), "en-GB": track("british")})
    assert list(information["requested_subtitles"]) == ["en-orig"]
    assert len(paths) == 1
    assert requests == [track("human")[0]["url"], track("original")[0]["url"]]
    assert charges == [("acquire", "youtube_caption")] * 2


def test_rate_limit_never_falls_through_to_another_caption(captions):
    guard, _ydl, _pp, track, download, requests, charges, responses = captions
    responses[track("human")[0]["url"]] = 429
    with pytest.raises(guard._TrafficSafetyError):
        download({"en": track("human")}, {"en-orig": track("original")})
    assert requests == [track("human")[0]["url"]]
    assert charges == [("acquire", "youtube_caption"), ("rate_limit", "youtube_caption")]


def test_missing_english_does_not_download_other_languages(captions):
    _guard, _ydl, _pp, track, download, requests, _charges, _responses = captions
    information, paths = download({"fr": track("french")})
    assert not information["requested_subtitles"] and paths == [] and requests == []


def test_local_caption_write_failure_keeps_abort_sentinel_without_more_requests(captions, monkeypatch):
    import builtins

    _guard, _ydl, _pp, track, download, requests, _charges, _responses = captions
    original_open = builtins.open

    def blocked_caption(path, *args, **kwargs):
        if str(path).endswith(".en.vtt"):
            raise PermissionError("fixture caption write blocked")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", blocked_caption)
    _information, paths = download({"en": [{"ext": "vtt", "data": VTT.decode()}]},
                                   {"en-orig": track("original")})
    assert paths is None
    assert requests == []


@pytest.mark.parametrize("result,complete,disabled", [
    ({"comments": [{"text": "Comment"}], "comment_count": 1}, True, False),
    ({"comments": [], "comment_count": 0}, True, False),
    ({"comments": None, "comment_count": None}, True, True),
    ({"comments": [{"text": "Partial"}], "comment_count": None}, False, False),
    ({"comments": [], "comment_count": None}, False, False),
    ({}, False, False),
])
def test_metadata_marker_only_follows_complete_comment_extraction(captions, result, complete, disabled):
    _guard, ydl, pp, _track, _download, requests, _charges, _responses = captions
    ydl.params["getcomments"] = True
    extract = mock.Mock(return_value=result)
    information = {"id": "fixture0001", "__post_extractor": extract}
    pp.run(information)
    extract.assert_not_called()
    assert "ytarchiver_metadata_snapshot" not in information
    ydl.post_extract(information)
    extract.assert_called_once()
    marker = information.get("ytarchiver_metadata_snapshot")
    assert bool(marker) is complete
    if complete:
        assert marker["video_id"] == "fixture0001"
        assert marker["version"] == 1 and marker["comments_complete"] is True
        assert marker["comments_disabled"] is disabled
        assert datetime.fromisoformat(marker["fetched_at"]).utcoffset().total_seconds() == 0
    assert requests == []


def test_interrupted_comment_extraction_does_not_write_completion_marker(captions):
    _guard, ydl, pp, _track, _download, _requests, _charges, _responses = captions
    ydl.params["getcomments"] = True
    information = {"id": "fixture0001", "__post_extractor": mock.Mock(side_effect=RuntimeError("interrupted"))}
    pp.run(information)
    with pytest.raises(RuntimeError, match="interrupted"):
        ydl.post_extract(information)
    assert "ytarchiver_metadata_snapshot" not in information


def test_existing_original_english_caption_is_used_without_fetch(monkeypatch, tmp_path):
    from backend.transcribe import transcribe_vtt as captions_module

    video = tmp_path / "Fixture [fixture0001].mp4"
    video.write_bytes(b"fixture")
    video.with_suffix(".en.vtt").write_text("WEBVTT\n", encoding="utf-8")
    video.with_suffix(".en-orig.vtt").write_bytes(VTT)
    monkeypatch.setattr(captions_module, "_fetch_captions_via_ytdlp",
                        mock.Mock(side_effect=AssertionError("unexpected caption fetch")))
    resolve = mock.Mock(return_value=None)
    monkeypatch.setattr(captions_module, "_resolve_transcript_paths", resolve)
    outcome = captions_module._try_auto_captions(str(video), "Fixture", {}, mock.Mock(), allow_fetch=True)
    # The intentionally unavailable output paths stop persistence after the
    # real parser has accepted the en-orig track instead of launching yt-dlp.
    assert outcome is captions_module._CaptionOutcome.FAILED
    resolve.assert_called_once()
