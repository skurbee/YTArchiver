"""Focused contracts for the subscription-management audit fixes."""

from __future__ import annotations

import contextlib
import copy
import json
import os
import threading
from types import SimpleNamespace
from unittest import mock

import pytest

from backend import subs
from backend.api_mixins import backup_mixin, subs_mixin
from backend.sync.options import normalize_channel_sync_options
from backend.transcribe.core import TranscribeManager


def _memory_transaction(store: dict):
    @contextlib.contextmanager
    def transaction():
        working = copy.deepcopy(store)
        yield working
        store.clear()
        store.update(copy.deepcopy(working))

    return transaction


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"range": "fromdate", "from_date": ""}, "valid year"),
        ({"range": "fromdate", "from_date": "2026-02-30"}, "calendar"),
        ({"min_duration": 20, "max_duration": 10}, "greater"),
        ({"min_duration": -1}, "negative"),
        ({"min_duration": "1.5"}, "whole numbers"),
    ),
)
def test_add_channel_rejects_invalid_date_and_duration_constraints(
        monkeypatch, changes, message):
    monkeypatch.setattr(subs, "load_config", lambda: {
        "channels": [],
        "min_duration": 180,
    })
    payload = {
        "name": "Fixture",
        "url": "https://www.youtube.com/@fixture",
        **changes,
    }

    with pytest.raises(subs.SubsError, match=message):
        subs.add_channel(payload)


def test_sparse_edit_does_not_silently_ignore_an_invalid_duration(monkeypatch):
    url = "https://www.youtube.com/@fixture"
    store = {
        "channels": [{"name": "Fixture", "folder": "Fixture", "url": url}],
        "output_dir": "",
    }
    monkeypatch.setattr(subs, "load_config", lambda: copy.deepcopy(store))
    monkeypatch.setattr(subs, "config_transaction", _memory_transaction(store))

    with pytest.raises(subs.SubsError, match="whole number"):
        subs.update_channel({"url": url}, {"min_duration": "1.5"})


@pytest.mark.parametrize("bad_value", (-1, "not-a-number", 1.5, True))
def test_sync_refuses_an_invalid_stored_duration(bad_value):
    with pytest.raises(ValueError, match="Minimum length"):
        normalize_channel_sync_options({
            "name": "Fixture",
            "url": "https://www.youtube.com/@fixture",
            "min_duration": bad_value,
        })


def test_add_rejects_an_existing_physical_folder_override(monkeypatch):
    store = {
        "channels": [{
            "name": "Existing",
            "folder": "Existing",
            "folder_override": "Shared:Folder",
            "url": "https://www.youtube.com/@existing_fixture",
        }],
    }
    monkeypatch.setattr(subs, "load_config", lambda: copy.deepcopy(store))
    monkeypatch.setattr(subs, "config_transaction", _memory_transaction(store))

    with pytest.raises(subs.SubsError, match="archive folder"):
        subs.add_channel({
            "name": "shared_folder",
            "url": "https://www.youtube.com/@new_fixture",
        })


def test_duplicate_preview_uses_the_physical_archive_folder(monkeypatch):
    monkeypatch.setattr(subs_mixin, "load_config", lambda: {
        "channels": [{
            "name": "Existing",
            "folder": "Existing",
            "folder_override": "Shared:Folder",
            "url": "https://www.youtube.com/@existing_fixture",
        }],
    })

    result = subs_mixin.SubsMixin().subs_check_duplicate(
        "https://www.youtube.com/@different_fixture", "shared_folder")

    assert result["ok"] is True
    assert result["dup_folder"] == "Existing"


def test_channel_import_preserves_and_deduplicates_physical_folder_identity(
        tmp_path, monkeypatch):
    import_file = tmp_path / "channels.json"
    import_file.write_text(json.dumps({
        "channels": [
            {
                "name": "First",
                "folder": "First",
                "folder_override": "Shared:Folder",
                "url": "https://www.youtube.com/@first_fixture",
            },
            {
                "name": "Second",
                "folder": "Second",
                "folder_override": "shared_folder",
                "url": "https://www.youtube.com/@second_fixture",
            },
            {
                "name": "First",
                "folder": "First",
                "folder_override": "Different Folder",
                "url": "https://www.youtube.com/@third_fixture",
            },
        ],
    }), encoding="utf-8")
    store = {"channels": []}
    monkeypatch.setattr(backup_mixin, "config_is_writable", lambda: True)
    monkeypatch.setattr(
        backup_mixin, "config_transaction", _memory_transaction(store))
    monkeypatch.setitem(
        __import__("sys").modules, "webview", SimpleNamespace(OPEN_DIALOG="open"))

    api = backup_mixin.BackupMixin()
    api._window = mock.Mock()
    api._window.create_file_dialog.return_value = str(import_file)
    api._reload_config = mock.Mock()

    result = api.channels_import()

    assert result["ok"] is True
    assert result["added"] == 1
    assert result["skipped"] == 2
    reasons = {item["reason"] for item in result["skipped_reasons"]}
    assert any("archive folder is already used" in reason for reason in reasons)
    assert "channel name is already used" in reasons
    assert store["channels"][0]["folder_override"] == "Shared:Folder"


def test_write_blocked_add_and_edit_are_reported_as_failures(monkeypatch):
    api = subs_mixin.SubsMixin()
    api._reload_config = mock.Mock()
    api.chan_fetch_art = mock.Mock()
    api._transcribe = None

    monkeypatch.setattr(
        subs_mixin.subs_backend,
        "add_channel",
        lambda _payload: {"name": "Fixture", "_write_blocked": True},
    )
    added = api.subs_add_channel({"name": "Fixture"})
    assert added["ok"] is False
    assert added["write_blocked"] is True

    monkeypatch.setattr(
        subs_mixin.subs_backend,
        "get_channel",
        lambda _identity: {"name": "Fixture", "url": "fixture"},
    )
    monkeypatch.setattr(
        subs_mixin.subs_backend,
        "update_channel",
        lambda _identity, _payload, **_kwargs: {
            "name": "Fixture", "_write_blocked": True,
        },
    )
    edited = api.subs_update_channel({"name": "Fixture"}, {"resolution": "720"})
    assert edited["ok"] is False
    assert edited["write_blocked"] is True


def test_channel_edit_holds_processing_boundary_through_path_reconcile(
        monkeypatch):
    events = []

    class Manager:
        @contextlib.contextmanager
        def pending_path_mutation_boundary(self):
            events.append("boundary-enter")
            try:
                yield
            finally:
                events.append("boundary-exit")

        def reconcile_pending_channel_path(
                self, old_path, new_path, *, old_channel, new_channel):
            assert events == ["boundary-enter", "backend-update"]
            events.append("reconciled")
            return {"ok": True, "changed": 1}

    def update(_identity, _payload, **kwargs):
        events.append("backend-update")
        kwargs["pending_path_reconciler"](
            "C:/Archive/Old", "C:/Archive/New", "Old", "New")
        return {"name": "New", "folder": "New", "url": "fixture",
                "_processing_queue_result": {"ok": True, "changed": 1}}

    api = subs_mixin.SubsMixin()
    api._transcribe = Manager()
    api._reload_config = mock.Mock()
    monkeypatch.setattr(
        subs_mixin.subs_backend, "get_channel",
        lambda _identity: {"name": "Old", "url": "fixture"})
    monkeypatch.setattr(subs_mixin.subs_backend, "update_channel", update)

    result = api.subs_update_channel(
        {"name": "Old"}, {"folder": "New"})

    assert result["ok"] is True
    assert result["processing_queue_changed"] == 1
    assert events == [
        "boundary-enter", "backend-update", "reconciled", "boundary-exit"]


def test_undo_uses_the_toast_identity_and_restores_the_exact_snapshot(monkeypatch):
    first = {
        "_undo_id": "undo-first",
        "name": "First",
        "folder": "First",
        "url": "https://www.youtube.com/@first_fixture",
        "min_duration": 180,
        "custom_field": "keep-me",
    }
    second = {
        "_undo_id": "undo-second",
        "name": "Second",
        "folder": "Second",
        "url": "https://www.youtube.com/@second_fixture",
    }
    captured = []

    def restore(snapshot):
        captured.append(copy.deepcopy(snapshot))
        return {"ok": True, "channel": copy.deepcopy(snapshot)}

    monkeypatch.setattr(subs_mixin.subs_backend, "restore_channel_snapshot", restore)
    monkeypatch.setattr("backend.archive_scan.invalidate_channel", lambda _url: None)
    api = subs_mixin.SubsMixin()
    api._removed_channels_stack = [copy.deepcopy(first), copy.deepcopy(second)]
    api._reload_config = mock.Mock()

    result = api.subs_undo_remove("undo-first")

    assert result["ok"] is True
    assert captured == [{key: value for key, value in first.items()
                         if key != "_undo_id"}]
    assert captured[0]["min_duration"] == 180
    assert captured[0]["custom_field"] == "keep-me"
    assert [item["_undo_id"] for item in api._removed_channels_stack] == [
        "undo-second"]


class _QueueDouble:
    def __init__(self, items):
        self.items = copy.deepcopy(items)
        self.restore_calls = []

    def gpu_snapshot(self):
        return copy.deepcopy(self.items)

    def restore_pending_snapshot(self, lane, items):
        assert lane == "gpu"
        self.restore_calls.append(copy.deepcopy(items))
        self.items = copy.deepcopy(items)
        return True


def _manager_double(jobs, queue_items):
    manager = TranscribeManager.__new__(TranscribeManager)
    manager._journal_lock = threading.RLock()
    manager._jobs_lock = threading.Lock()
    manager._jobs = copy.deepcopy(jobs)
    manager._current_job = None
    manager._inline_caption_jobs = []
    manager._queues = _QueueDouble(queue_items)
    manager._stream = mock.Mock()
    manager.saved_journals = []
    manager._write_pending_snapshot = lambda snapshot: (
        manager.saved_journals.append(copy.deepcopy(snapshot)) or True)
    return manager


def test_pending_gpu_jobs_follow_a_channel_folder_rename(tmp_path):
    old_root = tmp_path / "Old"
    new_root = tmp_path / "New"
    path = old_root / "2026" / "video.mp4"
    job = {"task_id": "gpu-1", "kind": "transcribe",
           "path": str(path), "channel": "Old"}
    manager = _manager_double([job], [job])

    result = manager.reconcile_pending_channel_path(
        str(old_root), str(new_root), old_channel="Old", new_channel="New")

    expected = os.path.normpath(str(new_root / "2026" / "video.mp4"))
    assert result == {"ok": True, "changed": 1, "removed": 0}
    assert manager._jobs[0]["path"] == expected
    assert manager._jobs[0]["channel"] == "New"
    assert manager._queues.items[0]["path"] == expected
    assert manager._queues.items[0]["channel"] == "New"
    assert manager.saved_journals[-1][0]["path"] == expected


def test_pending_gpu_jobs_roll_back_when_folder_mutation_does_not_commit(tmp_path):
    old_root = tmp_path / "Old"
    new_root = tmp_path / "New"
    path = old_root / "video.mp4"
    job = {"task_id": "gpu-1", "kind": "compress",
           "path": str(path), "channel": "Old"}
    manager = _manager_double([job], [job])

    with manager.pending_channel_path_mutation(
            str(old_root), str(new_root),
            old_channel="Old", new_channel="New") as control:
        assert control["result"]["ok"] is True
        assert manager._jobs[0]["channel"] == "New"
        # Leaving commit false simulates a config/folder transaction failure.

    assert manager._jobs[0]["path"] == str(path)
    assert manager._jobs[0]["channel"] == "Old"
    assert manager._queues.items[0]["path"] == str(path)
    assert manager._queues.items[0]["channel"] == "Old"


def test_queue_all_reports_partial_failures(monkeypatch):
    captured = []

    class Api(subs_mixin.SubsMixin):
        pass

    api = Api()
    api._log_stream = mock.Mock()
    api._window = mock.Mock()
    api.services = SimpleNamespace(event_bus=SimpleNamespace(
        show_toast_and_refresh_subs=lambda message, kind: captured.append(
            (message, kind))))
    api.chan_transcribe_all = lambda name: (
        {"ok": True, "queued": 2} if name == "Good"
        else {"ok": False, "error": "Folder is missing."})
    monkeypatch.setattr(subs_mixin, "load_config", lambda: {
        "channels": [{"name": "Good"}, {"name": "Broken"}],
    })
    monkeypatch.setattr(
        subs_mixin, "start_managed_task",
        lambda _owner, **kwargs: kwargs["target"](),
    )

    result = api.subs_queue_all()

    assert result == {"ok": True, "started": True}
    assert captured and captured[0][1] == "warn"
    assert "1 could not be checked" in captured[0][0]
    assert "Broken: Folder is missing." in captured[0][0]


def test_bulk_trash_refresh_survives_a_toast_delivery_failure(monkeypatch):
    evaluated = []

    class Api(subs_mixin.SubsMixin):
        pass

    api = Api()
    api._log_stream = mock.Mock()
    api._window = SimpleNamespace(evaluate_js=evaluated.append)
    api.services = SimpleNamespace(event_bus=SimpleNamespace(
        show_toast_and_refresh_subs=mock.Mock(
            side_effect=RuntimeError("toast unavailable"))))
    api.subs_remove_channel = mock.Mock(return_value={
        "ok": True,
        "subscription_removed": True,
        "files_removed": True,
    })
    monkeypatch.setattr(subs_mixin.subs_backend, "get_channel", lambda _identity: {
        "name": "Fixture", "url": "https://www.youtube.com/@fixture"})
    monkeypatch.setattr(
        subs_mixin, "start_managed_task",
        lambda _owner, **kwargs: kwargs["target"](),
    )

    result = api.subs_bulk_delete(["Fixture"], delete_files=True)

    assert result == {"ok": True, "started": True}
    assert evaluated == [
        "if (window._onTrashChanged) window._onTrashChanged();"]
