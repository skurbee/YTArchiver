"""Request controls remain connected across caption and metadata callers."""

from __future__ import annotations

import io
import subprocess
import threading
from types import SimpleNamespace
from unittest import mock

import pytest

from backend import repair_captions, youtube_session
from backend.metadata import catalog
from backend.process_runner import StreamingRunResult
from backend.transcribe import transcribe_vtt


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(youtube_session, "handle_youtube_failure_text", lambda *args, **kwargs: "")
    monkeypatch.setattr(catalog.youtube_traffic, "acquire", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(catalog, "_find_cookie_source", list)
    monkeypatch.setattr(repair_captions, "_find_cookie_source", list)


@pytest.mark.parametrize("operation", ["titles", "catalog"])
def test_metadata_playlist_launch_attaches_controls_before_supervision(monkeypatch, operation):
    cancel, pause = threading.Event(), threading.Event()
    calls = []

    def launch(command, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(catalog, "popen_ytdlp", launch)
    monkeypatch.setattr(catalog, "supervise_streaming_process", lambda *args, **kwargs: StreamingRunResult(0, []))
    if operation == "titles":
        catalog._resolve_ids_by_title("fixture", "fixture-channel", ["unmatched.mp4"], mock.Mock(), cancel, pause)
    else:
        catalog._flat_playlist_bulk_stats("fixture", "fixture-channel", mock.Mock(), cancel, pause)
    assert len(calls) == 1
    assert calls[0]["request_cancel_event"] is cancel
    assert calls[0]["request_pause_event"] is pause


def test_upload_date_probe_keeps_controls_during_child_budget_wait(monkeypatch):
    cancel, pause = threading.Event(), threading.Event()
    calls = []

    def run(command, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(returncode=0, stdout="20260101\n", stderr="")

    monkeypatch.setattr(catalog, "run_ytdlp", run)
    result = catalog._fetch_per_video_upload_dates(
        "fixture", ["abcdefghijk"], mock.Mock(), cancel, pause, max_workers=1)
    assert result == {"abcdefghijk": "20260101"}
    assert len(calls) == 1
    assert calls[0]["request_cancel_event"] is cancel
    assert calls[0]["request_pause_event"] is pause


def test_caption_cancel_reaches_launch_and_avoids_authenticated_retry(tmp_path, monkeypatch):
    cancel = threading.Event()
    calls, permissions = [], []
    monkeypatch.setattr(transcribe_vtt.shutil, "which", lambda name: "fixture")
    monkeypatch.setattr(transcribe_vtt, "_extract_video_id", lambda path: "abcdefghijk")

    def acquire(*args, **kwargs):
        permissions.append(kwargs)
        return {"ok": True}

    def run(command, **kwargs):
        calls.append(kwargs)
        cancel.set()
        return SimpleNamespace(returncode=1, stderr=b"")

    monkeypatch.setattr(transcribe_vtt.youtube_traffic, "acquire", acquire)
    monkeypatch.setattr(transcribe_vtt, "run_ytdlp", run)
    result = transcribe_vtt._fetch_captions_via_ytdlp(
        str(tmp_path / "video.mp4"), mock.Mock(), [], cancel_event=cancel)
    assert result is None
    assert len(calls) == len(permissions) == 1
    assert permissions[0]["cancel_event"] is cancel
    assert calls[0]["request_cancel_event"] is cancel


def test_cancelled_caption_fetch_never_launches(tmp_path, monkeypatch):
    cancel = threading.Event()
    cancel.set()
    monkeypatch.setattr(transcribe_vtt.shutil, "which", lambda name: "fixture")
    monkeypatch.setattr(transcribe_vtt, "_extract_video_id", lambda path: "abcdefghijk")
    launch = mock.Mock(side_effect=AssertionError("Cancelled work must not launch"))
    monkeypatch.setattr(transcribe_vtt, "run_ytdlp", launch)
    assert transcribe_vtt._fetch_captions_via_ytdlp(
        str(tmp_path / "video.mp4"), mock.Mock(), [], cancel_event=cancel) is None
    launch.assert_not_called()


def test_caption_worker_returns_cancelled_when_remote_fallback_is_cancelled(tmp_path, monkeypatch):
    cancel = threading.Event()
    forwarded = []
    fetched = tmp_path / "video.__cap_probe.en.vtt"

    def fetch(path, stream, outputs, *, cancel_event):
        forwarded.append(cancel_event)
        fetched.write_text("WEBVTT\n", encoding="utf-8")
        outputs.append(str(fetched))
        cancel_event.set()
        return str(fetched)

    monkeypatch.setattr(transcribe_vtt, "_fetch_captions_via_ytdlp", fetch)
    result = transcribe_vtt._try_auto_captions(
        str(tmp_path / "video.mp4"), "Video", "Channel", mock.Mock(), cancel_event=cancel)
    assert result is transcribe_vtt._CaptionOutcome.CANCELLED
    assert forwarded == [cancel]
    assert not fetched.exists()


class RepairChild:
    def __init__(self, clock, waits, *, cancel=None):
        self.clock, self.waits = clock, iter(waits)
        self.cancel = cancel
        self.killed = False
        self.terminated = False
        self.stdout, self.stderr = io.StringIO(), io.StringIO()

    def wait(self, timeout):
        if self.terminated:
            return 0
        entry = next(self.waits)
        if entry is None:
            return 0
        self.clock["now"], self.clock["budget"] = entry
        if self.cancel is not None:
            self.cancel.set()
        raise subprocess.TimeoutExpired("fixture", timeout)

    def kill(self):
        self.killed = True

    def terminate(self):
        self.terminated = True


@pytest.mark.parametrize(("waits", "expected_error", "killed"), [
    ([(200, 200), None], "no captions available", False),
    ([(200, 200), (321, 200)], "yt-dlp timeout", True),
])
def test_caption_repair_excludes_budget_wait_but_preserves_actual_timeout(tmp_path, monkeypatch, waits, expected_error, killed):
    clock = {"now": 0.0, "budget": 0.0}
    cancel = threading.Event()
    child = RepairChild(clock, waits)
    calls = []

    def launch(command, **kwargs):
        calls.append(kwargs)
        return child

    monkeypatch.setattr(repair_captions, "popen_ytdlp", launch)
    monkeypatch.setattr(repair_captions, "time", SimpleNamespace(monotonic=lambda: clock["now"]))
    monkeypatch.setattr(repair_captions, "budget_wait_seconds", lambda proc: clock["budget"])
    assert repair_captions._fetch_vtt("fixture", "abcdefghijk", tmp_path, cancel) == (None, expected_error)
    assert child.killed is killed
    assert calls[0]["request_cancel_event"] is cancel


def test_caption_repair_cancel_remains_responsive_inside_budget_wait(tmp_path, monkeypatch):
    clock = {"now": 0.0, "budget": 0.0}
    cancel = threading.Event()
    child = RepairChild(clock, [(200, 200)], cancel=cancel)
    monkeypatch.setattr(repair_captions, "popen_ytdlp", lambda *args, **kwargs: child)
    monkeypatch.setattr(repair_captions, "time", SimpleNamespace(monotonic=lambda: clock["now"]))
    monkeypatch.setattr(repair_captions, "budget_wait_seconds", lambda proc: clock["budget"])
    assert repair_captions._fetch_vtt("fixture", "abcdefghijk", tmp_path, cancel) == (None, "cancelled")
    assert child.terminated and not child.killed
