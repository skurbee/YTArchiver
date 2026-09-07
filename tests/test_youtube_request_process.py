"""The required child guard cannot be bypassed, and its waits are not stalls."""
from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest import mock

import pytest

from backend import process_runner
from backend import youtube_request_process as guard


def test_missing_plugin_prevents_process_start(monkeypatch):
    monkeypatch.setattr(guard.Path, "is_file", lambda self: False)
    with mock.patch.object(process_runner.subprocess, "Popen") as launch:
        with pytest.raises(OSError, match="protection is missing"):
            process_runner.popen_ytdlp(["yt-dlp", "https://www.youtube.com/watch?v=fixture0001"])
    launch.assert_not_called()


def test_launch_error_closes_request_session(monkeypatch):
    session = mock.Mock()
    monkeypatch.setattr(process_runner, "prepare_command", lambda cmd, env: (cmd, {}, session))
    with mock.patch.object(process_runner.subprocess, "Popen", side_effect=OSError("fixture")):
        with pytest.raises(OSError):
            process_runner.popen_ytdlp(["yt-dlp", "fixture"])
    session.close.assert_called_once()


def test_child_argv_requires_plugin_and_does_not_mutate_parent_env(monkeypatch):
    from backend import youtube_request_broker as broker

    session = mock.Mock()
    session.environment.return_value = {"YTARCHIVER_TRAFFIC_TOKEN": "fixture"}
    monkeypatch.setattr(broker, "prepare_launch", lambda: session)
    original = {"FIXTURE": "1"}
    command = [sys.executable, "-m", "yt_dlp", "--simulate", "fixture"]
    argv, env, result = guard.prepare_command(command, original)
    assert argv[:3] == command[:3]
    assert "YTArchiverTrafficGuard:when=pre_process" in argv
    assert "--ignore-config" in argv
    assert result is session and original == {"FIXTURE": "1"}
    assert env["YTARCHIVER_TRAFFIC_TOKEN"] == "fixture"
    with pytest.raises(ValueError, match="cannot be disabled"):
        guard.prepare_command(["yt-dlp", "--no-plugin-dirs", "fixture"])


def test_offline_version_command_needs_no_broker(monkeypatch):
    monkeypatch.setattr(guard.Path, "is_file", lambda self: False)
    command = ["yt-dlp", "--version"]
    assert guard.prepare_command(command) == (command, None, None)


def test_capture_excludes_intentional_wait_and_sends_input_once(monkeypatch):
    clock = [0.0]
    waiting = [0.0]
    monkeypatch.setattr(guard.time, "monotonic", lambda: clock[0])

    class Child:
        def __init__(self):
            self.calls = []

        def communicate(self, input=None, timeout=None):
            self.calls.append(input)
            if len(self.calls) == 1:
                clock[0] += 100.0
                waiting[0] += 100.0
                raise subprocess.TimeoutExpired("fixture", timeout)
            return "ok", ""

    session = mock.Mock()
    session.wait_seconds.side_effect = lambda: waiting[0]
    child = Child()
    guard.attach_session(child, session)
    assert child.communicate(input="once", timeout=1) == ("ok", "")
    assert child.calls == ["once", None]


def test_capture_still_times_out_when_not_waiting(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(guard.time, "monotonic", lambda: clock[0])

    class Child:
        def communicate(self, input=None, timeout=None):
            clock[0] += 1
            raise subprocess.TimeoutExpired("fixture", timeout)

    session = mock.Mock()
    session.wait_seconds.return_value = 0
    child = Child()
    guard.attach_session(child, session)
    with pytest.raises(subprocess.TimeoutExpired):
        child.communicate(timeout=0.1)


def test_streaming_timeout_excludes_only_this_childs_budget_wait():
    # An entirely local child sleeps while its fake governor waits, then emits
    # output. Another child's wait must not affect an unrelated stalled one.
    from backend.subprocess_util import subprocess_creationflags

    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(.4); print('done')"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        creationflags=subprocess_creationflags())
    started = time.monotonic()
    session = mock.Mock()
    session.wait_seconds.side_effect = lambda: min(0.4, time.monotonic() - started)
    child._yta_request_session = session
    lines = []
    result = process_runner.supervise_streaming_process(
        child, on_stdout_line=lines.append, timeout=0.2, idle_timeout=0.2)
    assert not result.timed_out and result.returncode == 0
    assert lines == ["done"]
    other = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        creationflags=subprocess_creationflags())
    result = process_runner.supervise_streaming_process(other, timeout=0.1)
    assert result.timed_out


def test_real_ytdlp_loads_required_plugin_without_contacting_youtube(tmp_path):
    # Simulate a pre-extracted video; the URL is never opened. This exercises
    # yt-dlp's actual plugin loader/constructor, not an application mock.
    info = tmp_path / "fixture.info.json"
    info.write_text(json.dumps({
        "id": "fixture0001", "title": "Offline fixture", "extractor": "generic",
        "webpage_url": "http://127.0.0.1:9/fixture", "ext": "mp4",
        "url": "http://127.0.0.1:9/fixture.mp4",
    }), encoding="utf-8")
    from backend.subprocess_util import subprocess_creationflags
    result = process_runner.run_ytdlp(
        [sys.executable, "-m", "yt_dlp", "--simulate", "--no-check-formats",
         "--load-info-json", str(info), "--print", "%(id)s"],
        capture_output=True, text=True, timeout=30,
        creationflags=subprocess_creationflags())
    assert result.returncode == 0, result.stderr
    assert "fixture0001" in result.stdout
    from backend.ytarchiver_config import APP_DATA_DIR
    assert not (Path(APP_DATA_DIR) / "youtube_traffic.jsonl").exists()


def test_child_request_stops_at_limit_and_resumes_without_restarting(tmp_path, monkeypatch):
    """Real plugin/socket/governor; the child's network callback only prints."""
    from backend import youtube_request_broker as broker
    from backend import youtube_traffic as traffic
    from backend.subprocess_util import subprocess_creationflags

    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")
    monkeypatch.setattr(traffic, "load_config", lambda: {
        "youtube_traffic_mode": "custom", "youtube_traffic_custom_hourly": 2,
        "youtube_traffic_custom_daily": 10, "youtube_traffic_custom_min_gap": 0,
        "youtube_traffic_custom_max_gap": 0,
    })
    now = [100_000.0]
    # This fixture advances only when a rolling slot expires. HTTP pacing has
    # its own deterministic-clock tests, so disable its physical wait here.
    monkeypatch.setattr(traffic, "REQUEST_MIN_GAP", 0)
    monkeypatch.setattr(traffic, "REQUEST_MAX_GAP", 0)
    monkeypatch.setattr(traffic.time, "time", lambda: now[0])
    session = broker.prepare_launch()
    plugin = Path(guard.__file__).parent / (
        "yt_dlp_plugins/ytarchiver/yt_dlp_plugins/postprocessor/ytarchiver_traffic.py")
    code = """
import importlib.util, sys
spec = importlib.util.spec_from_file_location('fixture_guard', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
class Downloader:
    params = {}
    def urlopen(self, request):
        print('request-sent', flush=True)
downloader = Downloader()
module.YTArchiverTrafficGuardPP(downloader)
for _ in range(3):
    downloader.urlopen('https://www.youtube.com/watch?v=fixture0001')
"""
    import os
    child = subprocess.Popen(
        [sys.executable, "-c", code, str(plugin)], stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True,
        env={**os.environ, **session.environment()},
        creationflags=subprocess_creationflags())
    guard.attach_session(child, session)
    lines = []
    results = []
    worker = threading.Thread(target=lambda: results.append(
        process_runner.supervise_streaming_process(
            child, on_stdout_line=lines.append, timeout=10, idle_timeout=10)))
    worker.start()
    try:
        deadline = time.monotonic() + 10
        while len(lines) < 2 and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert lines == ["request-sent"] * 2
        time.sleep(0.35)
        assert child.poll() is None and len(lines) == 2 and session.is_waiting()
        assert traffic.status()["hourly_used"] == 2
        now[0] += 3601
        worker.join(5)
        assert not worker.is_alive()
        assert lines == ["request-sent"] * 3
        assert results[0].returncode == 0 and not results[0].timed_out
        assert len((tmp_path / "traffic.jsonl").read_text().splitlines()) == 3
    finally:
        session.close()
        if child.poll() is None:
            process_runner.stop_owned_process(child)
        worker.join(5)
