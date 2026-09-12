"""Exercise the real queue-to-tray method without importing application boot."""

import ast
import time
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

import backend
from backend.tray import activity_spin_color


@pytest.fixture
def queue_tray(monkeypatch):
    source_path = Path(__file__).resolve().parents[1] / "main.py"
    module = ast.parse(source_path.read_text(encoding="utf-8"))
    api_node = next(node for node in module.body if isinstance(node, ast.ClassDef) and node.name == "Api")
    method = next(node for node in api_node.body if isinstance(node, ast.FunctionDef)
                  and node.name == "_on_queue_changed")
    namespace = {"activity_spin_color": activity_spin_color, "time": time, "_log": mock.Mock()}
    exec(compile(ast.Module(body=[method], type_ignores=[]), str(source_path), "exec"), namespace)
    payload = {"sync": [{"status": "running", "name": "Fixture channel"}], "gpu": [],
               "sync_paused": False, "gpu_paused": False}
    waiting = {"active": False}
    monkeypatch.setattr(backend, "youtube_traffic", SimpleNamespace(wait_status=lambda: dict(waiting)), raising=False)
    monkeypatch.setattr(backend, "youtube_session", SimpleNamespace(rate_limit_detected=lambda: False), raising=False)
    tray = mock.Mock(error_active=False)
    api = SimpleNamespace(
        _window=object(), _config={"autorun_sync": False, "autorun_gpu": False},
        _queues=SimpleNamespace(to_ui_payload=lambda: payload),
        _transcribe=SimpleNamespace(is_active=lambda: False), _tray=tray,
        services=SimpleNamespace(event_bus=mock.Mock()),
    )

    def update():
        tray.reset_mock()
        api.services.event_bus.reset_mock()
        namespace["_on_queue_changed"](api)
        namespace["_log"].debug.assert_not_called()
        return api.services.event_bus.update_queues.call_args.args[1]

    return SimpleNamespace(update=update, tray=tray, payload=payload, waiting=waiting, api=api)


def test_running_sync_pauses_at_budget_and_resumes_animation_when_released(queue_tray):
    flow = queue_tray
    flow.update()
    flow.tray.set_traffic_waiting.assert_called_once_with(False)
    flow.tray.start_spin.assert_called_once_with("blue")

    flow.waiting.update(active=True, reason="hourly_limit", until=time.time() + 600)
    flow.update()
    flow.tray.set_traffic_waiting.assert_called_once_with(True)
    flow.tray.stop_spin.assert_called_once()
    flow.tray.start_spin.assert_not_called()
    assert "Paused for YouTube hourly limit" in flow.tray.set_tooltip.call_args.args[0]
    assert "resumes" in flow.tray.set_tooltip.call_args.args[0]

    flow.waiting["active"] = False
    flow.update()
    flow.tray.set_traffic_waiting.assert_called_once_with(False)
    flow.tray.start_spin.assert_called_once_with("blue")
    assert "Syncing: Fixture channel" in flow.tray.set_tooltip.call_args.args[0]


def test_daily_budget_tooltip_uses_known_resume_date(queue_tray):
    flow = queue_tray
    until = time.time() + 86400
    flow.waiting.update(active=True, reason="daily_limit", until=until)
    flow.update()
    local = time.localtime(until)
    tooltip = flow.tray.set_tooltip.call_args.args[0]
    assert "24-hour limit" in tooltip
    assert f"{local.tm_mon}/{local.tm_mday} at " in tooltip
    assert time.strftime("%I:%M %p", local).lstrip("0") in tooltip


@pytest.mark.parametrize("pause_key", ["sync_paused", "sync_paused_active"])
def test_manual_pause_clears_budget_badge_even_before_waiter_cleanup(queue_tray, pause_key):
    flow = queue_tray
    flow.waiting.update(active=True, reason="hourly_limit")
    flow.payload[pause_key] = True
    flow.update()
    flow.tray.set_traffic_waiting.assert_called_once_with(False)
    flow.tray.start_spin.assert_not_called()
    flow.tray.stop_spin.assert_called_once()
    assert "YouTube" not in flow.tray.set_tooltip.call_args.args[0]


def test_sync_completion_clears_budget_badge_with_stale_wait_notification(queue_tray):
    flow = queue_tray
    flow.waiting.update(active=True, reason="hourly_limit")
    flow.payload["sync"] = []
    flow.update()
    flow.tray.set_traffic_waiting.assert_called_once_with(False)
    flow.tray.start_spin.assert_not_called()
    flow.tray.set_tooltip.assert_called_once_with("YTArchiver — Idle")


def test_real_gpu_work_keeps_red_spinner_while_sync_waits(queue_tray):
    flow = queue_tray
    flow.waiting.update(active=True, reason="hourly_limit")
    flow.payload["gpu"] = [{"status": "running", "kind": "transcribe", "title": "Fixture video"}]
    flow.update()
    flow.tray.set_traffic_waiting.assert_called_once_with(False)
    flow.tray.start_spin.assert_called_once_with("red")
    flow.tray.stop_spin.assert_not_called()
    assert "Transcribe: Fixture video" in flow.tray.set_tooltip.call_args.args[0]


def test_budget_tooltip_explains_current_hold_even_with_older_errors(queue_tray):
    flow = queue_tray
    flow.tray.error_active = True
    flow.waiting.update(active=True, reason="hourly_limit")
    flow.update()
    assert "Paused for YouTube hourly limit" in flow.tray.set_tooltip.call_args.args[0]
    flow.waiting["active"] = False
    flow.payload["sync"] = []
    flow.update()
    assert "Errors need attention" in flow.tray.set_tooltip.call_args.args[0]


def test_missing_resume_time_keeps_pause_badge_and_clear_reason(queue_tray):
    flow = queue_tray
    flow.waiting.update(active=True, reason="daily_limit", until="unavailable")
    flow.update()
    flow.tray.set_traffic_waiting.assert_called_once_with(True)
    flow.tray.set_tooltip.assert_called_once_with("YTArchiver — Paused for YouTube 24-hour limit (Sync)")


def test_processing_caption_wait_routes_only_to_processing_and_resumes_red(queue_tray):
    flow = queue_tray
    flow.payload["sync"] = []
    flow.payload["gpu"] = [{"status": "running", "kind": "transcribe", "title": "Fixture video"}]
    flow.waiting.update(active=True, queue="gpu", kind="caption_fetch", reason="daily_limit")
    state = flow.update()
    assert state["sync"]["trafficWaiting"] is False
    assert state["gpu"]["trafficWaiting"] is True
    assert state["gpu"]["trafficWait"]["kind"] == "caption_fetch"
    flow.tray.set_traffic_waiting.assert_called_once_with(True)
    flow.tray.start_spin.assert_not_called()
    flow.tray.set_tooltip.assert_called_once_with(
        "YTArchiver — Paused for YouTube 24-hour limit (Processing)")

    flow.waiting["active"] = False
    state = flow.update()
    assert state["gpu"]["trafficWaiting"] is False
    flow.tray.set_traffic_waiting.assert_called_once_with(False)
    flow.tray.start_spin.assert_called_once_with("red")


def test_sync_keeps_blue_spinner_while_processing_waits(queue_tray):
    flow = queue_tray
    flow.payload["gpu"] = [{"status": "running", "kind": "transcribe", "title": "Fixture video"}]
    flow.waiting.update(active=True, queue="gpu", reason="hourly_limit")
    state = flow.update()
    assert state["sync"]["trafficWaiting"] is False
    assert state["gpu"]["trafficWaiting"] is True
    flow.tray.set_traffic_waiting.assert_called_once_with(False)
    flow.tray.start_spin.assert_called_once_with("blue")
    assert "Syncing: Fixture channel" in flow.tray.set_tooltip.call_args.args[0]


def test_concurrent_waits_keep_the_latest_deadline_for_each_queue(queue_tray):
    flow = queue_tray
    flow.payload["gpu"] = [{"status": "running", "kind": "transcribe", "title": "Fixture video"}]
    now = time.time()
    flow.waiting.update(active=True, queue="background", reason="daily_limit", waits=[
        {"active": True, "queue": "sync", "until": now + 100, "reason": "hourly_limit"},
        {"active": True, "queue": "gpu", "until": now + 300, "reason": "daily_limit"},
        {"active": True, "queue": "sync", "until": now + 200, "reason": "hourly_limit"},
        {"active": True, "queue": "background", "until": now + 900, "reason": "daily_limit"},
    ])
    state = flow.update()
    assert state["sync"]["trafficWaiting"] is True
    assert state["gpu"]["trafficWaiting"] is True
    assert state["sync"]["trafficWait"]["until"] == now + 200
    assert state["gpu"]["trafficWait"]["until"] == now + 300
    flow.tray.set_traffic_waiting.assert_called_once_with(True)
    flow.tray.start_spin.assert_not_called()
    assert "24-hour limit (Sync and Processing)" in flow.tray.set_tooltip.call_args.args[0]


def test_background_wait_does_not_park_either_queue(queue_tray):
    flow = queue_tray
    flow.waiting.update(active=True, queue="background", reason="daily_limit")
    state = flow.update()
    assert not state["sync"]["trafficWaiting"]
    assert not state["gpu"]["trafficWaiting"]
    flow.tray.set_traffic_waiting.assert_called_once_with(False)
    flow.tray.start_spin.assert_called_once_with("blue")


def test_processing_completion_clears_stale_wait(queue_tray):
    flow = queue_tray
    flow.payload["sync"] = []
    flow.waiting.update(active=True, queue="gpu", reason="daily_limit")
    state = flow.update()
    assert not state["gpu"]["trafficWaiting"]
    flow.tray.set_traffic_waiting.assert_called_once_with(False)
    flow.tray.set_tooltip.assert_called_once_with("YTArchiver — Idle")


@pytest.mark.parametrize("pause_key", ["gpu_paused", "gpu_paused_active"])
def test_manual_processing_pause_takes_precedence_over_wait(queue_tray, pause_key):
    flow = queue_tray
    flow.payload["sync"] = []
    flow.payload["gpu"] = [{"status": "running", "kind": "transcribe", "title": "Fixture video"}]
    flow.payload[pause_key] = True
    flow.waiting.update(active=True, queue="gpu", reason="hourly_limit")
    flow.update()
    flow.tray.set_traffic_waiting.assert_called_once_with(False)
    flow.tray.set_queue_paused.assert_called_once_with(True)
    flow.tray.start_spin.assert_not_called()
    assert "YouTube" not in flow.tray.set_tooltip.call_args.args[0]


def test_processing_pause_request_still_spins_until_active_work_finishes(queue_tray):
    flow = queue_tray
    flow.payload["sync"] = []
    flow.payload["gpu"] = [{"status": "running", "kind": "transcribe", "title": "Fixture video"}]
    flow.payload.update(gpu_paused=True, gpu_paused_active=False)
    flow.update()
    flow.tray.start_spin.assert_called_once_with("red")
    flow.tray.set_queue_paused.assert_called_once_with(False)
