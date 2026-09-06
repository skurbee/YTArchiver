"""Offline full-library history and channel-start scope regressions."""
from __future__ import annotations

import importlib
import json
import os
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-sync-scope-")
os.environ["APPDATA"] = _PROFILE.name
os.environ["LOCALAPPDATA"] = _PROFILE.name
Path(_PROFILE.name, "YTArchiver").mkdir()

from backend import queues  # noqa: E402
from backend.api_mixins.sync_mixin import SyncMixin  # noqa: E402
from backend.services.queue_repository import QueueRepository  # noqa: E402

sync_all = importlib.import_module("backend.sync.sync_all")


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    cfg = {"channels": [
        {"name": name, "url": f"https://www.youtube.com/@{name}"}
        for name in ("First", "Second")], "last_sync": "2020-01-01 01:01"}
    states = []

    def new_state():
        state = queues.QueueState(QueueRepository(tmp_path / "queue.json"))
        states.append(state)
        return state

    @contextmanager
    def transaction():
        yield cfg

    overrides = {
        "load_config": lambda: cfg,
        "ARCHIVE_FILE": str(tmp_path / "absent-archive.txt"),
        "_resolve_sync_task_target": lambda ch: (cfg, ch, frozenset({"isolated"})),
        "_channel_folder_path": lambda *a: "",
        "_check_batch_cooldown": lambda ch: (True, ""),
        "channel_leases": SimpleNamespace(try_acquire=lambda *a: SimpleNamespace(
            ok=True, lease=SimpleNamespace(release=lambda: None))),
        "channel_identity": SimpleNamespace(
            preflight_channel_identity=lambda *a, **k: {"ok": True},
            has_stable_identity=lambda ch: True,
            operational_channel_url=lambda ch: ch["url"]),
        "sync_channel": mock.Mock(return_value={"ok": True, "downloaded": 0, "errors": 0}),
        "fire_channel_synced_hook": lambda: None,
        "_should_batch_limit": lambda *a: False,
        "clear_sync_progress": lambda: None,
        "config_transaction": transaction,
    }
    monkeypatch.setattr(queues, "config_is_writable", lambda: True)
    for name, value in overrides.items():
        monkeypatch.setattr(sync_all, name, value)
    yield SimpleNamespace(cfg=cfg, state=new_state(), new_state=new_state, root=tmp_path)
    for state in states:
        state.mark_orphan()


def run(state, *, full=False, cancel=None, skip=None):
    return sync_all._sync_all_impl(mock.Mock(), cancel or threading.Event(),
        queues=state, add_downloads_from_config=full, skip_event=skip,
        only_with_new=False)


def test_explicit_single_channel_success_keeps_full_library_timestamp(fixture):
    assert fixture.state.sync_enqueue(fixture.cfg["channels"][0])
    assert run(fixture.state)["ok"]
    assert fixture.cfg["last_sync"] == "2020-01-01 01:01"
    assert not fixture.state.current_sync


def test_explicit_full_library_success_updates_once_and_clears_marker(fixture):
    assert run(fixture.state, full=True)["ok"]
    assert sync_all.sync_channel.call_count == 2
    assert fixture.cfg["last_sync"] != "2020-01-01 01:01"
    assert "full_sync_batch" not in json.loads((fixture.root / "queue.json").read_text())
    stamp = fixture.cfg["last_sync"]
    assert run(fixture.state)["ok"]
    assert fixture.cfg["last_sync"] == stamp


@pytest.mark.parametrize("outcome", [
    {"ok": False, "errors": 0}, {"ok": True, "errors": 1},
])
def test_failed_channel_cannot_complete_full_history(fixture, monkeypatch, outcome):
    monkeypatch.setattr(sync_all, "sync_channel", lambda *a, **k: outcome)
    run(fixture.state, full=True)
    assert fixture.cfg["last_sync"] == "2020-01-01 01:01"
    assert not fixture.state.full_sync_completion()


def test_full_scope_survives_pause_shutdown_and_restart(fixture, monkeypatch):
    state = fixture.state
    assert state.sync_enqueue_full_library(fixture.cfg["channels"])["ok"]
    first, second = state.sync_snapshot()
    assert state.sync_promote_task_to_current(first["task_id"])
    assert state.sync_finish_task_durable(first["task_id"], True)
    assert state.sync_promote_task_to_current(second["task_id"])
    # An interrupted current task remains recoverable while the first success
    # stays recorded. A fresh process must finish only that remaining task.
    state.set_sync_paused(True)
    assert state.save_now()
    state.mark_orphan()
    restored = fixture.new_state()
    assert restored.load()
    assert restored.sync_paused
    recovered = restored.get_loaded_resuming()["sync"]
    assert recovered["task_id"] == second["task_id"]
    assert restored.sync_requeue_front(recovered) is not False
    assert restored.clear_resuming_slots()
    restored.set_sync_paused(False)
    assert fixture.cfg["last_sync"] == "2020-01-01 01:01"
    assert run(restored)["ok"]
    assert sync_all.sync_channel.call_count == 1
    assert sync_all.sync_channel.call_args.args[0]["task_id"] == second["task_id"]
    assert fixture.cfg["last_sync"] != "2020-01-01 01:01"


def test_cancelled_pass_keeps_full_timestamp_and_recovery(fixture, monkeypatch):
    cancel = threading.Event()

    def worker(*args, **kwargs):
        cancel.set()
        return {"ok": True, "errors": 0}

    monkeypatch.setattr(sync_all, "sync_channel", worker)
    run(fixture.state, full=True, cancel=cancel)
    assert fixture.cfg["last_sync"] == "2020-01-01 01:01"
    assert fixture.state.current_sync
    assert len(fixture.state.sync_snapshot()) == 1


def test_actual_paused_download_requeues_then_finishes_after_restart(fixture, monkeypatch):
    from backend import pause_helpers

    state = fixture.state
    pause, cancel = threading.Event(), threading.Event()
    visited = []

    def worker(ch, *args, **kwargs):
        visited.append(ch["name"])
        if ch["name"] == "Second":
            pause.set()
            return {"ok": False, "errors": 0}
        return {"ok": True, "errors": 0}

    def shutdown_while_paused(*args, **kwargs):
        assert [item["name"] for item in state.sync_snapshot()] == ["Second"]
        assert not state.current_sync
        assert len(state._full_sync_batch["completed"]) == 1
        assert not state._full_sync_batch["failed"]
        assert state.save_now()
        cancel.set()
        return True

    monkeypatch.setattr(sync_all, "sync_channel", worker)
    monkeypatch.setattr(pause_helpers, "wait_for_resume", shutdown_while_paused)
    sync_all._sync_all_impl(mock.Mock(), cancel, queues=state,
        add_downloads_from_config=True, pause_event=pause, only_with_new=False)
    assert visited == ["First", "Second"]
    assert fixture.cfg["last_sync"] == "2020-01-01 01:01"
    state.mark_orphan()
    restored = fixture.new_state()
    assert restored.load()
    restored.set_sync_paused(False)
    succeeded = mock.Mock(return_value={"ok": True, "errors": 0})
    monkeypatch.setattr(sync_all, "sync_channel", succeeded)
    assert run(restored)["ok"]
    assert succeeded.call_count == 1
    assert succeeded.call_args.args[0]["name"] == "Second"
    assert fixture.cfg["last_sync"] != "2020-01-01 01:01"


def test_full_library_with_no_new_videos_still_completes(fixture, monkeypatch):
    for ch in fixture.cfg["channels"]:
        ch.update(init_complete=True, sync_complete=True, mode="full")
    Path(sync_all.ARCHIVE_FILE).write_text("youtube abcDEF12345\n", encoding="utf-8")
    monkeypatch.setattr(sync_all, "_channel_folder_has_media", lambda *a: True)
    monkeypatch.setattr(sync_all, "quick_check_new_uploads",
        lambda *a, **k: {"ok": True, "has_new": False})
    assert run(fixture.state, full=True)["ok"]
    sync_all.sync_channel.assert_not_called()
    assert fixture.cfg["last_sync"] != "2020-01-01 01:01"


@pytest.mark.parametrize("defer", [False, True])
def test_exact_cancel_or_defer_is_accounted_after_worker_exit(fixture, monkeypatch, defer):
    state = fixture.state
    api = SyncMixin()
    api._queues = state
    api._sync_skip = threading.Event()
    api._log_stream = mock.Mock()
    visited = []

    def worker(ch, stream, token, **kwargs):
        visited.append(ch["name"])
        if len(visited) == 1:
            method = api.sync_defer_current if defer else api.sync_skip_current
            assert method(state.current_sync["task_id"])["ok"]
            assert token.is_set()
        else:
            assert not token.is_set()
        return {"ok": True, "errors": 0}

    monkeypatch.setattr(sync_all, "sync_channel", worker)
    assert run(state, full=True, skip=api._sync_skip)["ok"]
    assert visited == (["First", "Second", "First"] if defer else ["First", "Second"])
    assert (fixture.cfg["last_sync"] != "2020-01-01 01:01") is defer
    assert not state.current_sync
    assert not state.sync_snapshot()


def test_removed_required_channel_cannot_complete_full_history(fixture):
    state = fixture.state
    assert state.sync_enqueue_full_library(fixture.cfg["channels"])["ok"]
    removed = state.sync_snapshot()[1]
    assert state.sync_remove_task(removed["task_id"])
    assert run(state)["ok"]
    assert fixture.cfg["last_sync"] == "2020-01-01 01:01"
    restored = fixture.new_state()
    assert restored.load()
    assert not restored.full_sync_completion()


def test_failed_full_queue_commit_rolls_back_scope_and_rows(fixture, monkeypatch):
    before = fixture.state.sync_snapshot()
    monkeypatch.setattr(fixture.state, "_write_save_payload", lambda *a: False)
    assert not fixture.state.sync_enqueue_full_library(fixture.cfg["channels"])["ok"]
    assert fixture.state.sync_snapshot() == before
    assert not fixture.state._full_sync_batch


def test_failed_result_commit_keeps_current_and_prevents_stamp(fixture, monkeypatch):
    state = fixture.state
    assert state.sync_enqueue_full_library(fixture.cfg["channels"])["ok"]
    write = state._write_save_payload

    def reject_completed(payload):
        if (payload.get("full_sync_batch") or {}).get("completed"):
            return False
        return write(payload)

    monkeypatch.setattr(state, "_write_save_payload", reject_completed)
    result = run(state)
    assert not result["ok"]
    assert result["reason"] == "queue_persistence"
    assert state.current_sync["name"] == "First"
    assert fixture.cfg["last_sync"] == "2020-01-01 01:01"


def test_staged_full_request_keeps_other_kinds_and_existing_ids(fixture):
    state = fixture.state
    download_id = state.sync_enqueue_with_id(fixture.cfg["channels"][0], durable=True)
    metadata_id = state.sync_enqueue_with_id(
        {**fixture.cfg["channels"][0], "kind": "metadata"}, durable=True)
    redownload_id = state.sync_enqueue_with_id(
        {**fixture.cfg["channels"][1], "kind": "redownload"}, durable=True)
    result = state.sync_enqueue_full_library(fixture.cfg["channels"])
    assert result["queued"] == 1
    assert result["skipped"] == 1
    rows = state.sync_snapshot()
    assert [row["task_id"] for row in rows[:3]] == [download_id, metadata_id, redownload_id]
    assert state._full_sync_batch["required"] == [download_id, rows[3]["task_id"]]


def test_new_channel_queue_start_does_not_claim_resume(fixture):
    state = fixture.state
    assert state.sync_enqueue(fixture.cfg["channels"][0])
    stream = mock.Mock()
    sync_all._sync_all_impl(stream, threading.Event(), queues=state,
        add_downloads_from_config=False, only_with_new=False)
    emitted = str(stream.emit.call_args_list)
    assert "Sync pass starting" in emitted
    assert "Resuming" not in emitted


def test_cancelled_sidecar_after_result_cannot_publish_on_restart(fixture):
    state = fixture.state
    assert state.sync_enqueue_full_library(fixture.cfg["channels"][:1])["ok"]
    task = state.sync_snapshot()[0]
    assert state.sync_promote_task_to_current(task["task_id"])
    assert state.sync_record_full_sync_result(task["task_id"], True)
    assert state.replace_current_task_durable("sync", {**task, "cancel_requested": True},
        expected_task_id=task["task_id"])
    restored = fixture.new_state()
    assert restored.load()
    assert not restored.full_sync_completion()


def test_successful_marker_retains_original_time_if_publishing_retries(fixture, monkeypatch):
    state = fixture.state
    assert state.sync_enqueue_full_library(fixture.cfg["channels"][:1])["ok"]
    task = state.sync_snapshot()[0]
    assert state.sync_promote_task_to_current(task["task_id"])
    with mock.patch.object(queues.time, "time", return_value=1750000000):
        assert state.sync_finish_task_durable(task["task_id"], True)
    restored = fixture.new_state()
    assert restored.load()
    assert restored.full_sync_completion()["completed_at"] == 1750000000
    assert run(restored)["ok"]
    assert fixture.cfg["last_sync"] == sync_all.datetime.fromtimestamp(1750000000).strftime("%Y-%m-%d %H:%M")


@pytest.mark.parametrize("staging", ["auto_off", "enqueue_all"])
def test_api_full_staging_creates_durable_scope(fixture, staging):
    api = SyncMixin()
    api._queues = fixture.state
    api._config = fixture.cfg
    api._on_queue_changed = mock.Mock()
    if staging == "auto_off":
        result = api._sync_start_all_inner()
        assert result["started"] is False
    else:
        result = api.sync_enqueue_all_channels()
    assert result["ok"]
    restored = fixture.new_state()
    assert restored.load()
    assert len(restored._full_sync_batch["required"]) == 2
    assert len(restored.sync_snapshot()) == 2
