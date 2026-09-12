"""A complete discovery cache saves repeat listing requests, never partial work."""
import json
import subprocess
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from backend.process_runner import StreamingRunResult
from backend.process_runner import supervise_streaming_process as real_supervise
from backend.sync import discovery_resume as resume
from backend.sync import ytdlp_session

CID = "UC" + "a" * 22
URL = f"https://www.youtube.com/channel/{CID}"
_real_get_known_ids = resume.channel_cache.get_known_ids


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    channel = {"channel_id": CID, "url": URL, "mode": "full", "last_sync": "2026-01-01"}
    monkeypatch.setattr(resume.store.ytarchiver_config, "APP_DATA_DIR", tmp_path)
    monkeypatch.setattr(resume.store.ytarchiver_config, "config_is_writable", lambda: True)
    monkeypatch.setattr(resume.channel_cache, "get_known_ids",
                        lambda _, **_kw: [f"{i:011d}" for i in range(300)])
    monkeypatch.setattr(resume, "_find_cookie_source", list)
    launches = []
    stop = threading.Event()
    data = {"_type": "playlist", "id": CID, "channel_id": CID, "entries": [
        {"_type": "url", "ie_key": "Youtube", "id": "video000001",
         "url": "https://www.youtube.com/watch?v=video000001", "duration": 100},
        {"_type": "url", "ie_key": "Youtube", "id": "video000002",
         "url": "https://www.youtube.com/watch?v=video000002", "duration": 600},
    ]}
    result = StreamingRunResult(0, [])
    lines = [json.dumps(data)]

    def launch(cmd, **kwargs):
        launches.append((cmd, kwargs))
        return SimpleNamespace()

    def supervise(_proc, **kwargs):
        for line in lines:
            kwargs["on_stdout_line"](line)
        return result

    monkeypatch.setattr(resume, "popen_ytdlp_process", launch)
    monkeypatch.setattr(resume, "supervise_streaming_process", supervise)
    monkeypatch.setattr("backend.youtube_session.handle_youtube_failure_text", lambda *_a, **_k: False)
    stream = Mock()

    def prepare(**kwargs):
        return resume.prepare_discovery_resume(channel, URL, str(tmp_path / "media"),
                                               "fixture-ytdlp", stream, **kwargs)

    return SimpleNamespace(channel=channel, data=data, lines=lines, launches=launches,
                           result=result, prepare=prepare, stop=stop, stream=stream,
                           path=tmp_path)


def test_listing_is_saved_once_and_reused_without_launch(fixture):
    first = fixture.prepare()
    assert first is not None
    assert len(fixture.launches) == 1
    assert fixture.launches[0][0][1] == "--ignore-config"
    assert "--flat-playlist" in fixture.launches[0][0]
    assert "--no-quiet" in fixture.launches[0][0]
    saved = json.loads(Path(first.path).read_text())
    assert [v["duration"] for v in saved["entries"]] == [100, 600]
    second = fixture.prepare()
    assert second == first
    assert len(fixture.launches) == 1


@pytest.mark.parametrize("field,value", [
    ("returncode", 1), ("returncode", 101), ("returncode", -9),
    ("output_complete", False), ("cancelled", True), ("timed_out", True),
])
def test_incomplete_listing_never_becomes_a_cache(fixture, field, value):
    setattr(fixture.result, field, value)
    assert fixture.prepare() is None
    assert not list(fixture.path.rglob("*.json"))


@pytest.mark.parametrize("lines", [[], ["{"], ["{}", "{}"], ["WARNING: incomplete listing", "{}"]])
def test_bad_output_falls_back_without_publishing(fixture, lines):
    fixture.lines[:] = lines
    assert fixture.prepare() is None
    assert not list(fixture.path.rglob("*.json"))


@pytest.mark.parametrize("change", [
    {"mode": "fromdate"}, {"mode": "new"}, {"init_complete": True},
    {"initialized": True, "sync_complete": True}, {"last_sync": ""},
    {"channel_id": "UC" + "b" * 22},
])
def test_ordinary_channels_keep_lazy_discovery(fixture, change):
    fixture.channel.update(change)
    assert fixture.prepare() is None
    assert not fixture.launches


def test_small_partial_sync_keeps_lazy_discovery(fixture, monkeypatch):
    monkeypatch.setattr(resume.channel_cache, "get_known_ids", lambda _, **_kw: ["video000001"])
    assert fixture.prepare() is None
    assert not fixture.launches


@pytest.mark.parametrize("signal", ["cancel_event", "pause_event", "kill_current"])
def test_stop_before_listing_spends_no_requests(fixture, signal):
    fixture.stop.set()
    assert fixture.prepare(**{signal: fixture.stop}) is None
    assert not fixture.launches


@pytest.mark.parametrize("signal", ["cancel_event", "pause_event", "kill_current"])
def test_stop_during_listing_discards_complete_looking_output(fixture, monkeypatch, signal):
    def supervise(_proc, **kwargs):
        kwargs["on_stdout_line"](fixture.lines[0])
        fixture.stop.set()
        assert kwargs["cancel_event"].is_set()
        return fixture.result
    monkeypatch.setattr(resume, "supervise_streaming_process", supervise)
    assert fixture.prepare(**{signal: fixture.stop}) is None
    assert not list(fixture.path.rglob("*.json"))


def test_cached_list_still_loads_when_progress_hint_is_stale(fixture, monkeypatch):
    first = fixture.prepare()
    monkeypatch.setattr(resume.channel_cache, "get_known_ids", lambda _, **_kw: None)
    assert fixture.prepare() == first
    assert len(fixture.launches) == 1


def test_successful_completion_retires_only_this_cache(fixture):
    plan = fixture.prepare()
    resume.finish_discovery_resume([plan])
    assert not Path(plan.path).exists()


@pytest.mark.parametrize("warning", [False, True])
def test_real_subprocess_output_is_parsed_before_publishing(fixture, monkeypatch, warning):
    """Exercise the real binary/text boundary and supervisor, without YouTube."""
    fixture.data["entries"][0]["title"] = "A caf\u00e9 fixture"
    lines = ["[youtube:tab] fixture: Downloading page 2",
             json.dumps(fixture.data, ensure_ascii=False)]
    if warning:
        lines.append("WARNING: incomplete listing")
    processes = []

    def spawn(_cmd, **kwargs):
        kwargs.pop("request_cancel_event", None)
        kwargs.pop("request_pause_event", None)
        script = f"import sys; sys.stdout.reconfigure(encoding='utf-8'); print({chr(10).join(lines)!r})"
        proc = subprocess.Popen([sys.executable, "-c", script], **kwargs)
        processes.append(proc)
        return proc

    monkeypatch.setattr(ytdlp_session.youtube_traffic, "acquire", lambda *_a, **_k: {"ok": True})
    monkeypatch.setattr(ytdlp_session, "popen_ytdlp", spawn)
    monkeypatch.setattr(resume, "popen_ytdlp_process", ytdlp_session.popen_ytdlp_process)
    monkeypatch.setattr(resume, "supervise_streaming_process", real_supervise)
    try:
        plan = fixture.prepare()
        assert len(processes) == 1
        assert processes[0].returncode == 0
        if warning:
            assert plan is None
            assert not list(fixture.path.rglob("*.json"))
        else:
            assert plan is not None
            saved = json.loads(Path(plan.path).read_text(encoding="utf-8"))
            assert saved["entries"][0]["title"] == "A caf\u00e9 fixture"
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=5)
            if proc.stdout is not None:
                proc.stdout.close()


def test_existing_download_launcher_keeps_binary_output(fixture, monkeypatch):
    def spawn(_cmd, **kwargs):
        kwargs.pop("request_cancel_event", None)
        kwargs.pop("request_pause_event", None)
        return subprocess.Popen([sys.executable, "-c", "print('fixture')"], **kwargs)

    monkeypatch.setattr(ytdlp_session.youtube_traffic, "acquire", lambda *_a, **_k: {"ok": True})
    monkeypatch.setattr(ytdlp_session, "popen_ytdlp", spawn)
    with ytdlp_session.popen_ytdlp_process(["fixture-ytdlp"]) as proc:
        output, _ = proc.communicate(timeout=5)
    assert output.strip() == b"fixture"


def test_partial_progress_with_no_refresh_timestamp_still_activates(fixture, monkeypatch):
    monkeypatch.setattr(resume.channel_cache, "get_known_ids", _real_get_known_ids)
    monkeypatch.setattr(resume.channel_cache, "_loaded", True)
    monkeypatch.setattr(resume.channel_cache, "_cache", {
        URL: {"last_refreshed": 0, "ids": [f"{i:011d}" for i in range(300)]},
    })
    assert resume.channel_cache.get_cached_ids(URL) is None
    assert fixture.prepare() is not None
    assert len(fixture.launches) == 1
