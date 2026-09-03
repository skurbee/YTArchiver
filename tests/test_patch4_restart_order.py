"""Regression coverage for the Patch 4 safe-restart handoff."""

from __future__ import annotations

from unittest import mock


class _ImmediateThread:
    def __init__(self, target, **_kwargs):
        self._target = target

    def start(self):
        self._target()


def test_restart_quiesces_old_process_before_spawning_replacement(monkeypatch):
    from backend.api_mixins import window_mixin

    events: list[str] = []
    api = window_mixin.WindowMixin()
    api._shutdown_cleanup_fn = lambda: (
        events.append("quiesced") or {"ok": True})
    api._window = mock.Mock()
    api._window.destroy.side_effect = lambda: events.append("destroyed")

    monkeypatch.setattr(window_mixin.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(window_mixin.threading, "Thread", _ImmediateThread)
    monkeypatch.setattr(
        window_mixin.subprocess,
        "Popen",
        lambda *_args, **_kwargs: events.append("spawned"),
    )
    monkeypatch.setattr(
        window_mixin.os, "_exit", lambda _code: events.append("exited"))

    assert api.app_restart() == {"ok": True}
    assert events == ["quiesced", "spawned", "destroyed", "exited"]


def test_restart_fails_closed_when_old_writers_do_not_stop(monkeypatch):
    from backend.api_mixins import window_mixin

    api = window_mixin.WindowMixin()
    api._shutdown_cleanup_fn = lambda: {
        "ok": False,
        "error": "one writer is still active",
    }
    api._window = mock.Mock()
    spawn = mock.Mock()

    monkeypatch.setattr(window_mixin.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(window_mixin.threading, "Thread", _ImmediateThread)
    monkeypatch.setattr(window_mixin.subprocess, "Popen", spawn)
    exit_mock = mock.Mock()
    monkeypatch.setattr(window_mixin.os, "_exit", exit_mock)

    assert api.app_restart() == {"ok": True}
    spawn.assert_not_called()
    exit_mock.assert_not_called()
    api._window.destroy.assert_not_called()
