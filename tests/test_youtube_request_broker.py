"""Only loopback fixtures; traffic state always lives in a temporary profile."""

from __future__ import annotations

import json
import queue
import socket
import threading
import time

import pytest

from backend import youtube_request_broker as broker
from backend import youtube_traffic as traffic


@pytest.fixture(autouse=True)
def isolated_broker(tmp_path, monkeypatch):
    broker._shutdown()
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")
    config = {
        "channels": [], "youtube_traffic_mode": "custom",
        "youtube_traffic_custom_daily": 100,
        "youtube_traffic_custom_hourly": 10,
        "youtube_traffic_custom_min_gap": 0,
        "youtube_traffic_custom_max_gap": 0,
    }
    monkeypatch.setattr(traffic, "load_config", lambda: config)
    # These protocol tests disable pacing explicitly; the pacing suite checks
    # the separate HTTP clock with deterministic time.
    monkeypatch.setattr(traffic, "REQUEST_MIN_GAP", 0)
    monkeypatch.setattr(traffic, "REQUEST_MAX_GAP", 0)
    yield config
    broker._shutdown()
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")


class Child:
    returncode = None

    def poll(self):
        return self.returncode


def rpc(session, **payload):
    environment = session.environment()
    request = {
        "token": environment["YTARCHIVER_TRAFFIC_TOKEN"],
        "op": "acquire", "kind": "youtube_http", **payload,
    }
    with socket.create_connection(("127.0.0.1", int(environment["YTARCHIVER_TRAFFIC_PORT"])), timeout=3) as connection:
        connection.sendall(json.dumps(request).encode() + b"\n")
        with connection.makefile("rb") as response:
            return json.loads(response.readline())


def pending_rpc(session, **payload):
    completed = queue.Queue()

    def run():
        try:
            completed.put(rpc(session, **payload))
        except Exception as error:
            completed.put(error)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return completed


def wait_until(predicate, timeout=2):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "Fixture condition did not arrive"
        time.sleep(0.01)


def test_one_launch_charges_each_request_and_shares_other_launch_usage():
    first, second = broker.prepare_launch(), broker.prepare_launch()
    first.bind(Child())
    second.bind(Child())
    assert first.environment()["YTARCHIVER_TRAFFIC_TOKEN"] != second.environment()["YTARCHIVER_TRAFFIC_TOKEN"]
    assert rpc(first)["ok"]
    assert rpc(first, kind="youtube_caption")["ok"]
    assert rpc(second, kind="youtube_media_manifest")["ok"]
    status = traffic.status()
    assert status["hourly_used"] == status["daily_used"] == 3
    rows = [json.loads(row) for row in traffic.TRAFFIC_FILE.read_text().splitlines()]
    assert [row["kind"] for row in rows] == ["youtube_http", "youtube_caption", "youtube_media_manifest"]


def test_rolling_budget_stops_next_request_and_close_aborts_wait(isolated_broker):
    isolated_broker["youtube_traffic_custom_hourly"] = 1
    first, second = broker.prepare_launch(), broker.prepare_launch()
    assert rpc(first)["ok"]
    result = pending_rpc(second)
    wait_until(second.is_waiting)
    assert result.empty()
    time.sleep(0.04)
    assert second.wait_seconds() > 0
    second.close()
    denied = result.get(timeout=2)
    assert denied["ok"] is False and denied["cancelled"]
    wait_until(lambda: not second.is_waiting())
    assert traffic.status()["hourly_used"] == 1


def test_request_uses_prepared_reservation_on_server_thread(monkeypatch):
    observed = []

    def acquire(kind, **kwargs):
        observed.append((kind, traffic._current_reservation(), threading.get_ident()))
        return {"ok": True}

    monkeypatch.setattr(traffic, "acquire", acquire)
    with traffic.reservation_scope("fixture-sweep"):
        session = broker.prepare_launch()
    assert rpc(session)["ok"]
    assert observed == [("youtube_http", "fixture-sweep", observed[0][2])]
    assert observed[0][2] != threading.get_ident()
    assert traffic._current_reservation() == ""


def test_pause_holds_request_until_resume_without_spending_budget():
    session = broker.prepare_launch()
    pause = threading.Event()
    pause.set()
    session.set_signals(pause_event=pause)
    result = pending_rpc(session)
    wait_until(session.is_waiting)
    assert result.empty()
    assert traffic.status()["hourly_used"] == 0
    pause.clear()
    assert result.get(timeout=2)["ok"]
    assert traffic.status()["hourly_used"] == 1


def test_pause_arriving_during_acquire_retries_only_after_resume(monkeypatch):
    session = broker.prepare_launch()
    pause = threading.Event()
    session.set_signals(pause_event=pause)
    entered = threading.Event()
    calls = []

    def acquire(kind, **kwargs):
        calls.append(kind)
        if len(calls) == 1:
            pause.set()
            entered.set()
            return {"ok": False, "paused": True}
        return {"ok": True}

    monkeypatch.setattr(traffic, "acquire", acquire)
    result = pending_rpc(session)
    assert entered.wait(2)
    assert calls == ["youtube_http"] and result.empty()
    pause.clear()
    assert result.get(timeout=2)["ok"]
    assert calls == ["youtube_http", "youtube_http"]


def test_pause_after_grant_holds_response_without_double_charge(monkeypatch):
    session = broker.prepare_launch()
    pause = threading.Event()
    session.set_signals(pause_event=pause)
    entered = threading.Event()
    calls = []

    def acquire(kind, **kwargs):
        calls.append(kind)
        pause.set()
        entered.set()
        return {"ok": True}

    monkeypatch.setattr(traffic, "acquire", acquire)
    result = pending_rpc(session)
    assert entered.wait(2)
    assert result.empty()
    pause.clear()
    assert result.get(timeout=2)["ok"]
    assert calls == ["youtube_http"]


@pytest.mark.parametrize("control", ["cancel", "child_exit"])
def test_bound_child_exit_or_cancellation_releases_pending_request(control):
    session = broker.prepare_launch()
    child, pause, cancel = Child(), threading.Event(), threading.Event()
    pause.set()
    session.set_signals(pause_event=pause, cancel_event=cancel)
    # Exercise the race where the plugin has connected before Popen returned.
    result = pending_rpc(session)
    wait_until(session.is_waiting)
    session.bind(child)
    if control == "cancel":
        cancel.set()
    else:
        child.returncode = 0
    assert result.get(timeout=2)["ok"] is False
    wait_until(lambda: session._closed.is_set())
    assert rpc(session)["ok"] is False
    assert traffic.status()["hourly_used"] == 0


def test_http_429_immediately_blocks_other_sessions():
    first, second = broker.prepare_launch(), broker.prepare_launch()
    assert rpc(first, op="rate_limit")["ok"]
    assert traffic.circuit_state()["active"]
    result = rpc(second)
    assert result["ok"] is False and "cooldown" in result["error"]
    assert traffic.status()["hourly_used"] == 0


@pytest.mark.parametrize("payload", [
    {"token": "wrong-token"}, {"token": []},
    {"op": "unknown"}, {"kind": "https://private.invalid/signed?token=secret"},
    {"kind": []},
])
def test_invalid_authorization_and_categories_never_consume_budget(payload):
    session = broker.prepare_launch()
    assert rpc(session, **payload)["ok"] is False
    assert traffic.status()["hourly_used"] == 0


def test_oversized_message_is_denied_before_governor():
    session = broker.prepare_launch()
    env = session.environment()
    with socket.create_connection(("127.0.0.1", int(env["YTARCHIVER_TRAFFIC_PORT"])), timeout=2) as connection:
        connection.sendall(b"x" * (broker._MAX_MESSAGE + 1) + b"\n")
        with connection.makefile("rb") as response:
            assert json.loads(response.readline())["ok"] is False
    assert traffic.status()["hourly_used"] == 0


def test_governor_exception_fails_closed_and_response_contains_no_exception_details(monkeypatch):
    session = broker.prepare_launch()

    def unavailable(*args, **kwargs):
        raise OSError("private URL or local path must not escape")

    monkeypatch.setattr(traffic, "acquire", unavailable)
    result = rpc(session)
    assert result == {"ok": False, "error": "YouTube request governor unavailable"}
    assert session._closed.is_set()


def test_lifecycle_reaps_abandoned_unbound_session():
    session = broker.prepare_launch()
    session._created -= broker._UNBOUND_LIFETIME + 1
    wait_until(lambda: session._closed.is_set())
    assert rpc(session)["ok"] is False


def test_wait_accounting_counts_concurrent_waits_once_and_stops_at_completion():
    session = broker.prepare_launch()
    pause = threading.Event()
    pause.set()
    session.set_signals(pause_event=pause)
    started = time.monotonic()
    first, second = pending_rpc(session), pending_rpc(session)
    wait_until(lambda: session._waiting_count == 2)
    time.sleep(0.04)
    assert 0 < session.wait_seconds() <= time.monotonic() - started
    pause.clear()
    assert first.get(timeout=2)["ok"] and second.get(timeout=2)["ok"]
    wait_until(lambda: not session.is_waiting())
    finished_wait = session.wait_seconds()
    time.sleep(0.02)
    assert session.wait_seconds() == finished_wait
