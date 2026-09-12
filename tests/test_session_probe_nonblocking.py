"""Resume preflight must not wait on the worker's traffic budgets or spacing."""

import json
import subprocess
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from backend import youtube_session as session
from backend import youtube_traffic as traffic


@pytest.fixture
def governor(tmp_path, monkeypatch):
    config = {
        "channels": [], "youtube_traffic_mode": "custom",
        "youtube_traffic_custom_daily": 10,
        "youtube_traffic_custom_hourly": 10,
        "youtube_traffic_custom_min_gap": 0,
        "youtube_traffic_custom_max_gap": 0,
    }
    clock = SimpleNamespace(now=1_800_000_000.0)
    clock.time = lambda: clock.now
    clock.sleep = Mock(side_effect=AssertionError("Preflight must not sleep"))
    clock.localtime = time.localtime
    clock.strftime = time.strftime
    monkeypatch.setattr(traffic, "APP_DATA_DIR", tmp_path)
    monkeypatch.setattr(traffic, "time", clock)
    monkeypatch.setattr(traffic, "load_config", lambda: config)
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")
    listener = Mock()
    traffic.add_wait_listener(listener)
    yield SimpleNamespace(config=config, clock=clock, listener=listener)
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")


def _block_slot(governor, reason, *, kind="session_probe"):
    config = governor.config
    age = 30
    if reason == "daily_limit":
        config["youtube_traffic_custom_daily"] = 1
        age = 7200  # The hourly window is already free.
    elif reason == "hourly_limit":
        config["youtube_traffic_custom_hourly"] = 1
    else:
        config["youtube_traffic_custom_min_gap"] = 900
        config["youtube_traffic_custom_max_gap"] = 900
        age = 0.5 if kind == "youtube_http" else 30
    traffic.TRAFFIC_FILE.write_text(json.dumps({
        "ts": governor.clock.now - age,
        "daily_units": 1, "hourly_units": 1,
        "kind": kind, "reservation_id": "",
    }) + "\n", encoding="utf-8")


@pytest.mark.parametrize("reason", ["daily_limit", "hourly_limit", "spacing"])
def test_immediate_permission_defers_without_charging_or_announcing(governor, reason):
    _block_slot(governor, reason)
    before = traffic.TRAFFIC_FILE.read_bytes()
    stream = Mock()

    result = traffic.acquire("session_probe", wait_for_slot=False, stream=stream)

    assert result["deferred"] and not result["ok"]
    assert result["reason"] == reason and result["wait_seconds"] > 0
    assert traffic.TRAFFIC_FILE.read_bytes() == before
    assert traffic.status()["daily_used"] == 1
    assert not traffic.wait_status()["active"]
    governor.clock.sleep.assert_not_called()
    governor.listener.assert_not_called()
    assert not stream.mock_calls


def test_immediate_permission_also_respects_request_spacing(governor):
    _block_slot(governor, "spacing", kind="youtube_http")
    before = traffic.TRAFFIC_FILE.read_bytes()
    result = traffic.acquire("youtube_http", wait_for_slot=False)
    assert result["deferred"] and result["reason"] == "spacing"
    assert traffic.TRAFFIC_FILE.read_bytes() == before
    governor.clock.sleep.assert_not_called()


def test_immediate_permission_charges_available_slot_once(governor):
    result = traffic.acquire("session_probe", wait_for_slot=False)
    assert result["ok"] and not result["override"]
    rows = [json.loads(line) for line in traffic.TRAFFIC_FILE.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["daily_units"] == rows[0]["hourly_units"] == 1
    assert rows[0]["kind"] == "session_probe"
    governor.clock.sleep.assert_not_called()


@pytest.mark.parametrize("kwargs", [{}, {"wait_for_budget": False}])
def test_existing_callers_still_wait_for_spacing(governor, kwargs):
    _block_slot(governor, "spacing")
    started = governor.clock.now

    def advance(seconds):
        governor.clock.now += seconds

    governor.clock.sleep.side_effect = advance
    result = traffic.acquire("session_probe", **kwargs)
    assert result["ok"]
    assert governor.clock.now >= started + 870
    assert governor.clock.sleep.called
    assert traffic.status()["daily_used"] == 2


@pytest.fixture
def configured_session(governor, monkeypatch):
    from backend import deps_installer, process_runner
    from backend.sync import ytdlp_proc

    pause = threading.Event()
    stream = Mock()
    queues = Mock()
    monkeypatch.setattr(session, "_stream", stream)
    monkeypatch.setattr(session, "_pause_event", pause)
    monkeypatch.setattr(session, "_queues", queues)
    monkeypatch.setattr(session, "_cookie_alert_fired", False)
    cookie_status = Mock(return_value={"check_available": True, "signed_in": True})
    monkeypatch.setattr(deps_installer, "firefox_cookie_status", cookie_status)
    monkeypatch.setattr(ytdlp_proc, "_find_cookie_source", lambda **_: [
        "--cookies-from-browser", "firefox"])
    monkeypatch.setattr(ytdlp_proc, "find_yt_dlp", lambda: "fixture-yt-dlp.exe")
    run = Mock(return_value=SimpleNamespace(returncode=0, stdout="fixture\n", stderr=""))
    monkeypatch.setattr(process_runner, "run_ytdlp", run)
    return SimpleNamespace(pause=pause, stream=stream, queues=queues,
                           cookie_status=cookie_status, run=run)


@pytest.mark.parametrize("reason", ["daily_limit", "hourly_limit", "spacing"])
def test_session_preflight_keeps_local_check_but_skips_deferred_probe(
        governor, configured_session, reason):
    _block_slot(governor, reason)
    before = traffic.TRAFFIC_FILE.read_bytes()
    checked = session.check_configured_cookie_session(context="resuming")
    assert checked is True
    configured_session.cookie_status.assert_called_once_with()
    configured_session.run.assert_not_called()
    assert not configured_session.pause.is_set()
    assert traffic.TRAFFIC_FILE.read_bytes() == before
    governor.clock.sleep.assert_not_called()
    governor.listener.assert_not_called()
    # The worker still encounters the same budget/spacing after preflight.
    assert not traffic.eligibility(kind="channel_sync")["allowed"]


def test_signed_out_firefox_still_blocks_even_when_budget_is_exhausted(
        governor, configured_session):
    _block_slot(governor, "daily_limit")
    before = traffic.TRAFFIC_FILE.read_bytes()
    configured_session.cookie_status.return_value = {
        "check_available": True, "signed_in": False, "detail": "Fixture signed out"}
    assert not session.check_configured_cookie_session(context="resuming")
    assert configured_session.pause.is_set()
    configured_session.queues.set_sync_paused.assert_called_once_with(True)
    configured_session.run.assert_not_called()
    assert traffic.TRAFFIC_FILE.read_bytes() == before
    governor.clock.sleep.assert_not_called()


@pytest.mark.parametrize("remote_signed_in", [True, False])
def test_immediate_session_probe_still_checks_server_authentication(
        governor, configured_session, remote_signed_in):
    if not remote_signed_in:
        configured_session.run.return_value = SimpleNamespace(
            returncode=1, stdout="", stderr="ERROR: This playlist is private")
    result = session.check_configured_cookie_session(context="resuming")
    assert result is remote_signed_in
    configured_session.cookie_status.assert_called_once_with()
    configured_session.run.assert_called_once()
    command = configured_session.run.call_args.args[0]
    assert configured_session.run.call_args.kwargs["wall_timeout"] == 25
    assert command[-1] == ":ytfav"
    assert "--cookies-from-browser" in command
    assert traffic.status()["daily_used"] == traffic.status()["hourly_used"] == 1
    assert configured_session.pause.is_set() is not remote_signed_in


def test_active_cooldown_still_blocks_session_preflight(governor, configured_session):
    traffic.record_rate_limit(now=governor.clock.now)
    assert not session.check_configured_cookie_session(context="resuming")
    configured_session.run.assert_not_called()
    assert traffic.circuit_state()["active"]
    assert traffic.status()["daily_used"] == 0
    governor.clock.sleep.assert_not_called()


def test_unknown_circuit_still_prevents_remote_probe(governor, configured_session):
    traffic.CIRCUIT_FILE.write_text("not json", encoding="utf-8")
    # Preserve the existing inconclusive-preflight policy; the worker still
    # cannot launch a request while the persisted circuit is unreadable.
    assert session.check_configured_cookie_session(context="resuming")
    configured_session.run.assert_not_called()
    assert traffic.acquire("channel_sync", wait_for_slot=False)["circuit_error"]
    assert traffic.status()["daily_used"] == 0
    governor.clock.sleep.assert_not_called()


def test_deferred_resume_rearms_authentication_alarm(governor, configured_session):
    _block_slot(governor, "daily_limit")
    assert session.trigger_cookie_alert(reason="Earlier expiry")
    configured_session.stream.reset_mock()
    configured_session.queues.reset_mock()
    assert session.check_configured_cookie_session(context="Resume")
    configured_session.pause.clear()  # The Resume caller releases the worker.

    result = session.handle_youtube_failure_text(
        "ERROR: Sign in to confirm your age", context="downloading")
    assert result == "cookie"
    assert configured_session.pause.is_set()
    configured_session.queues.set_sync_paused.assert_called_once_with(True)
    configured_session.stream.emit_control.assert_called_once_with(
        {"kind": "cookie_alert", "context": "downloading"})
    configured_session.run.assert_not_called()


@pytest.mark.parametrize("outcome", ["timeout", "inconclusive"])
def test_inconclusive_resume_rearms_authentication_alarm(
        governor, configured_session, outcome):
    assert session.trigger_cookie_alert(reason="Earlier expiry")
    configured_session.stream.reset_mock()
    configured_session.queues.reset_mock()
    if outcome == "timeout":
        configured_session.run.side_effect = subprocess.TimeoutExpired("fixture", 25)
    else:
        configured_session.run.return_value = SimpleNamespace(
            returncode=1, stdout="", stderr="ERROR: Network unavailable")
    assert session.check_configured_cookie_session(context="Resume")
    configured_session.pause.clear()

    assert session.handle_youtube_failure_text(
        "ERROR: Sign in to confirm your age", context="downloading") == "cookie"
    assert configured_session.pause.is_set()
    configured_session.stream.emit_control.assert_called_once_with(
        {"kind": "cookie_alert", "context": "downloading"})


def test_queue_resume_returns_and_worker_retains_budget_wait_and_pass_override(
        governor, configured_session, tmp_path, monkeypatch):
    from backend.api_mixins.queue_mixin import QueueMixin
    from backend.queues import QueueState
    from backend.services.queue_repository import QueueRepository

    _block_slot(governor, "daily_limit")
    state = QueueState(QueueRepository(tmp_path / "queue.json"))
    monkeypatch.setattr(state, "save_debounced", lambda: None)
    monkeypatch.setattr(session, "_queues", state)
    state.set_sync_paused(True)
    configured_session.pause.set()

    class Api(QueueMixin):
        def __init__(self):
            self._queues = state
            self._sync_pause = configured_session.pause
            self._on_queue_changed = Mock()

    try:
        result = Api().queue_resume("sync")
        assert result == {"ok": True, "paused": False}
        assert not state.sync_paused and not configured_session.pause.is_set()
        configured_session.run.assert_not_called()
        governor.clock.sleep.assert_not_called()

        pass_id = traffic.begin_sync_pass()
        cancel = SimpleNamespace(is_set=lambda: False)

        def cancel_when_waiting(timeout):
            wait = traffic.wait_status()
            assert wait["active"] and wait["queue"] == "sync"
            assert wait["reason"] == "daily_limit"
            assert timeout > 0
            return True

        cancel.wait = Mock(side_effect=cancel_when_waiting)
        with traffic.request_scope("sync", sync_pass_id=pass_id):
            blocked = traffic.acquire("channel_sync", cancel_event=cancel,
                                      pause_event=configured_session.pause)
            assert blocked["cancelled"]
            cancel.wait.assert_called_once()
            assert traffic.status()["daily_used"] == 1
            assert traffic.override_budget_limits(pass_id=pass_id)["ok"]
            allowed = traffic.acquire("channel_sync", wait_for_slot=False)
            assert allowed["ok"] and allowed["override"]
            assert traffic.status()["daily_used"] == 2
        traffic.finish_sync_pass(pass_id)
    finally:
        state.mark_orphan()
