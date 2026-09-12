"""Interactive probes have a wall deadline even during real broker waits."""

import os
import subprocess
import sys
import threading
import time

import pytest

from backend import process_runner
from backend import youtube_request_broker as broker
from backend import youtube_traffic as traffic
from backend.subprocess_util import subprocess_creationflags


@pytest.fixture
def isolated_traffic(tmp_path, monkeypatch):
    monkeypatch.setattr(traffic, "APP_DATA_DIR", tmp_path)
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")
    config = {"youtube_traffic_mode": "custom",
              "youtube_traffic_custom_daily": 1,
              "youtube_traffic_custom_hourly": 1,
              "youtube_traffic_custom_min_gap": 0,
              "youtube_traffic_custom_max_gap": 0}
    monkeypatch.setattr(traffic, "load_config", lambda: config)
    yield config
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")


@pytest.mark.parametrize("reason", ["daily_limit", "spacing"])
def test_probe_wall_timeout_reaps_child_and_releases_actual_broker_wait(
        isolated_traffic, monkeypatch, reason):
    if reason == "spacing":
        isolated_traffic["youtube_traffic_custom_daily"] = 100
        isolated_traffic["youtube_traffic_custom_hourly"] = 100
        monkeypatch.setattr(traffic, "REQUEST_MIN_GAP", 30)
        monkeypatch.setattr(traffic, "REQUEST_MAX_GAP", 30)
        assert traffic.acquire("youtube_http", wait_for_slot=False)["ok"]
    # The preflight launch is admitted, but its first guarded HTTP request
    # will need another slot or must wait behind an earlier request's spacing.
    assert traffic.acquire("session_probe", wait_for_slot=False)["ok"]
    ledger_before = traffic.TRAFFIC_FILE.read_bytes()
    waiting_reasons = []
    traffic.add_wait_listener(lambda status: waiting_reasons.append(status.get("reason")))
    actual_broker = broker._RequestBroker()
    request = actual_broker.prepare("")
    code = """
import json, os, socket
with socket.create_connection(('127.0.0.1', int(os.environ['YTARCHIVER_TRAFFIC_PORT']))) as sock:
    sock.sendall((json.dumps({'op': 'acquire', 'kind': 'youtube_http',
        'token': os.environ['YTARCHIVER_TRAFFIC_TOKEN']}) + '\\n').encode())
    print(sock.makefile('rb').readline().decode(), flush=True)
"""

    def prepare(command, env):
        return ([sys.executable, "-c", code],
                {**os.environ, **request.environment()}, request)

    monkeypatch.setattr(process_runner, "prepare_command", prepare)
    # Failsafe prevents a regression from hanging the suite indefinitely. If
    # the deadline stops working, closing the broker lets the child exit and
    # the expected TimeoutExpired assertion fails.
    failsafe = threading.Timer(4, request.close)
    failsafe.start()
    started = time.monotonic()
    try:
        with pytest.raises(subprocess.TimeoutExpired):
            process_runner.run_ytdlp(
                ["fixture-probe"], capture_output=True, text=True,
                timeout=0.5, wall_timeout=0.75,
                creationflags=subprocess_creationflags())
        assert time.monotonic() - started < 3
        assert request.wait_seconds() > 0.25
        if reason == "daily_limit":
            assert reason in waiting_reasons
        child = request._proc
        assert child is not None and child.poll() is not None
        assert child.stdout.closed and child.stderr.closed
        assert not any(record.proc is child
                       for record in process_runner.PROCESS_REGISTRY.snapshot())
        assert request._closed.is_set()
        assert request._token not in actual_broker._sessions
        settled_by = time.monotonic() + 1
        while request.is_waiting() and time.monotonic() < settled_by:
            time.sleep(0.01)
        assert not request.is_waiting()
        assert not traffic.wait_status()["active"]
        assert traffic.TRAFFIC_FILE.read_bytes() == ledger_before
    finally:
        failsafe.cancel()
        request.close()
        if request._proc is not None and request._proc.poll() is None:
            process_runner.stop_owned_process(request._proc)
        actual_broker.close()


def test_wall_timeout_also_bounds_probe_without_request_session(monkeypatch):
    monkeypatch.setattr(process_runner, "prepare_command", lambda command, env: (
        command, env, None))
    with pytest.raises(subprocess.TimeoutExpired):
        process_runner.run_ytdlp(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            capture_output=True, timeout=None, wall_timeout=0.1,
            creationflags=subprocess_creationflags())
    assert not process_runner.PROCESS_REGISTRY.snapshot()
