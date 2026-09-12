"""First-sync failures must remain retryable without a bootstrap cooldown."""

from __future__ import annotations

import copy
import importlib
import threading
from unittest import mock

import pytest

from backend.queues import QueueState
from backend.services.channel_leases import channel_leases

sync_all = importlib.import_module("backend.sync.sync_all")


@pytest.fixture
def run_bootstrap(tmp_path, monkeypatch):
    channel = {
        "name": "Bootstrap Fixture", "folder": "Bootstrap Fixture",
        "url": "https://www.youtube.com/@BootstrapFixture",
        "mode": "full", "initialized": False, "init_complete": False,
    }
    config = {"channels": [channel], "output_dir": str(tmp_path / "Archive")}
    monkeypatch.setattr(sync_all, "load_config", lambda: copy.deepcopy(config))
    monkeypatch.setattr(sync_all, "ARCHIVE_FILE", str(tmp_path / "archive.txt"))
    monkeypatch.setattr(sync_all, "clear_sync_progress", mock.Mock())
    monkeypatch.setattr(sync_all, "fire_channel_synced_hook", mock.Mock())
    monkeypatch.setattr(
        sync_all.channel_identity, "preflight_channel_identity",
        lambda channel, **_kwargs: {"ok": True, "channel": channel},
    )
    # A new channel must never need a quick-check network request in this flow.
    monkeypatch.setattr(
        sync_all, "quick_check_new_uploads",
        mock.Mock(side_effect=AssertionError("Unexpected YouTube request")),
    )
    cooldown = mock.Mock()
    monkeypatch.setattr(sync_all, "set_batch_cooldown", cooldown)

    def run(result, *, flags=None, signal=None):
        channel.update(flags or {})
        cancel = threading.Event()
        pause = threading.Event()
        skip = threading.Event()

        def download(*_args, **_kwargs):
            if signal == "cancel":
                cancel.set()
            elif signal == "skip":
                skip.set()
            elif signal == "pause":
                pause.set()
                # End the test after the real orchestrator parks the task.
                cancel.set()
            return copy.deepcopy(result)

        download_mock = mock.Mock(side_effect=download)
        monkeypatch.setattr(sync_all, "sync_channel", download_mock)
        queues = QueueState()
        monkeypatch.setattr(queues, "save_now", lambda: True)
        monkeypatch.setattr(queues, "save_debounced", lambda: None)
        monkeypatch.setattr(queues, "_write_resuming_payload", lambda *_a, **_k: True)
        try:
            assert queues.sync_enqueue(channel)
            stream = mock.Mock()
            outcome = sync_all.sync_all(
                stream, queues=queues, add_downloads_from_config=False,
                cancel_event=cancel, pause_event=pause, skip_event=skip,
            )
            download_mock.assert_called_once()
            if signal == "pause":
                assert queues.current_sync is None
                assert len(queues.sync_snapshot()) == 1
            assert channel_leases.active_snapshot() == ()
            if result.get("incomplete"):
                text = "".join(str(segment[0]) for call in stream.emit.call_args_list
                               for segment in call.args[0])
                assert "Pass incomplete:" in text
                assert "Pass complete:" not in text
                assert not outcome["ok"]
                assert outcome["reason"] == "channel_check_incomplete"
                assert outcome["errors"] == result["errors"]
            return cooldown
        finally:
            queues.mark_orphan()

    return run


@pytest.mark.parametrize("result", [
    {"ok": False, "downloaded": 0, "errors": 1, "total": 0},
    {"ok": False, "downloaded": 0, "errors": 0},
    {"ok": True, "downloaded": 0, "errors": 0, "total": 0},
    {"ok": True, "downloaded": 0, "errors": 0},
    {"ok": False, "downloaded": 1, "errors": 0, "total": 100001},
    {"ok": True, "downloaded": 1, "errors": 1, "total": 100001},
    {"ok": True, "downloaded": 1, "errors": 0, "total": 100001,
     "cancelled": True},
    {"ok": True, "downloaded": 1, "errors": 0, "total": 100000},
    {"ok": False, "downloaded": 1, "errors": 1, "total": 100001,
     "incomplete": True},
])
def test_failed_empty_partial_or_small_bootstrap_does_not_cool_down(
        run_bootstrap, result):
    run_bootstrap(result).assert_not_called()


@pytest.mark.parametrize("signal", ["cancel", "skip", "pause"])
def test_interrupted_large_bootstrap_does_not_cool_down(run_bootstrap, signal):
    run_bootstrap(
        {"ok": True, "downloaded": 1, "errors": 0, "total": 100001},
        signal=signal,
    ).assert_not_called()


def test_successful_large_bootstrap_keeps_existing_cooldown(run_bootstrap):
    run_bootstrap(
        {"ok": True, "downloaded": 1, "errors": 0, "total": 100001},
    ).assert_called_once_with("https://www.youtube.com/@BootstrapFixture")


@pytest.mark.parametrize("flags", [
    {"init_complete": True},
    {"mode": "from_date", "from_date": "2026-01-01"},
])
def test_completed_or_date_limited_channel_does_not_cool_down(run_bootstrap, flags):
    run_bootstrap(
        {"ok": True, "downloaded": 1, "errors": 0, "total": 100001},
        flags=flags,
    ).assert_not_called()
