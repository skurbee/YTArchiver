"""Exercise channel controls through real methods with isolated fake services."""
from __future__ import annotations

import contextlib
import copy
import os
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
from unittest import mock

import pytest

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-controls-actions-")
os.environ["APPDATA"] = _PROFILE.name
os.environ["LOCALAPPDATA"] = _PROFILE.name

from backend import index, redownload, reorg, transcribe  # noqa: E402
from backend.api_mixins import channel_mixin, subs_mixin  # noqa: E402
from backend.api_mixins.channel_mixin import ChannelMixin, channel_transcript_layout  # noqa: E402
from backend.api_mixins.media_ops_mixin import MediaOpsMixin  # noqa: E402
from backend.api_mixins.redownload_mixin import RedownloadMixin  # noqa: E402
from backend.api_mixins.subs_mixin import SubsMixin  # noqa: E402
from backend.api_mixins.sync_mixin import SyncMixin  # noqa: E402
from backend.queues import QueueState  # noqa: E402
from backend.services.channel_leases import (  # noqa: E402
    LeaseOwner,
    channel_aliases,
    channel_leases,
)
from backend.services.queue_repository import QueueRepository  # noqa: E402


def api_for_channel(tmp_path, monkeypatch, **overrides):
    channel = {"name": "Channel", "url": "https://youtube.com/@Channel", **overrides}
    folder = tmp_path / "Channel"
    folder.mkdir(exist_ok=True)
    cfg = {"output_dir": str(tmp_path), "channels": [channel]}
    monkeypatch.setattr(channel_mixin.subs_backend, "get_channel", lambda _identity: channel)
    monkeypatch.setattr(channel_mixin, "load_config", lambda: cfg)
    monkeypatch.setattr(reorg, "load_config", lambda: cfg)
    api = ChannelMixin()
    api._config = cfg
    api._log_stream = mock.Mock()
    api._transcribe = mock.Mock()
    api._transcribe._jobs_lock = threading.Lock()
    api._transcribe._jobs = []
    api._transcribe._current_job = None
    api._queues = mock.Mock(current_sync=None, current_gpu=None)
    api._on_queue_changed = mock.Mock()
    return api, channel, folder, cfg


@pytest.mark.parametrize("lookup_failure", ["missing", "exception", "unresolved"])
def test_pending_transcriptions_survive_unavailable_index_and_missing_files(tmp_path, monkeypatch, lookup_failure):
    api, channel, _folder, _cfg = api_for_channel(tmp_path, monkeypatch, pending_tx_ids=["abc123def45"])
    save = mock.Mock()
    monkeypatch.setattr(channel_mixin, "update_config", save)
    conn = mock.Mock()
    conn.execute.return_value.fetchall.return_value = []
    if lookup_failure == "exception":
        conn.execute.side_effect = sqlite3.OperationalError("index temporarily busy")
    monkeypatch.setattr(index, "_open", lambda: None if lookup_failure == "missing" else conn)
    result = api.chan_transcribe_pending("Channel")
    assert result["queued"] == 0
    assert channel["pending_tx_ids"] == ["abc123def45"]
    save.assert_not_called()
    api._transcribe.enqueue.assert_not_called()
    assert result["ok"] is (lookup_failure == "unresolved")


def test_queue_all_reports_rejected_enqueue_without_inflating_success(tmp_path, monkeypatch):
    api, _channel, folder, _cfg = api_for_channel(tmp_path, monkeypatch)
    (folder / "Video [abc123def45].mp4").write_bytes(b"video")
    api._channel_folder_for_name = lambda _name: (_channel, str(folder))
    monkeypatch.setattr(transcribe, "_scan_existing_transcript_titles", lambda *_args: {})
    monkeypatch.setattr(index, "_reader_open", lambda: None)
    monkeypatch.setattr(index, "_open", lambda: None)
    api._transcribe.enqueue.return_value = False
    result = api.chan_transcribe_all("Channel")
    assert result["ok"] and result["queued"] == 0 and result["skipped"] == 1
    api._transcribe.enqueue.assert_called_once()


def test_transcript_layout_choice_is_saved_and_reused(tmp_path, monkeypatch):
    api, channel, folder, cfg = api_for_channel(tmp_path, monkeypatch, split_years=True)
    api._channel_folder_for_name = lambda _name: (channel, str(folder))
    monkeypatch.setattr(transcribe, "_scan_existing_transcript_titles", lambda *_args: {})
    monkeypatch.setattr(index, "_reader_open", lambda: None)
    monkeypatch.setattr(index, "_open", lambda: None)
    monkeypatch.setattr(channel_mixin, "update_config", lambda callback: (callback(cfg), cfg))
    assert api.chan_transcribe_all("Channel")["needs_choice"]
    assert api.chan_transcribe_all("Channel", combined=True)["combined"]
    assert channel["transcript_combined"] is True
    assert api.chan_transcribe_all("Channel")["combined"]
    del channel["transcript_combined"]
    (folder / "Channel Transcript.txt").write_text("existing combined output")
    assert channel_transcript_layout(channel, str(folder)) is True


def test_bulk_all_gets_choice_before_start_and_preserves_existing_preferences(tmp_path, monkeypatch):
    channels = [{"name": "Known", "split_years": True, "transcript_combined": True},
                {"name": "New", "split_years": True}]
    cfg = {"output_dir": str(tmp_path), "channels": channels}
    monkeypatch.setattr(subs_mixin, "load_config", lambda: cfg)
    starter = mock.Mock(side_effect=lambda *_args, **kwargs: kwargs["target"]())
    monkeypatch.setattr(subs_mixin, "start_managed_task", starter)
    api = SubsMixin()
    api._window = None
    api._log_stream = mock.Mock()
    api.chan_transcribe_all = mock.Mock(return_value={"ok": True, "queued": 1})
    result = api.subs_queue_all()
    assert result["needs_choice"] and not result["started"]
    assert result["channels"] == ["New"]
    starter.assert_not_called()
    assert api.subs_queue_all(combined=False)["started"]
    assert api.chan_transcribe_all.call_args_list == [mock.call("Known", combined=False), mock.call("New", combined=False)]
    channels[1]["transcript_combined"] = False
    api.chan_transcribe_all.reset_mock()
    assert api.subs_queue_all()["started"]
    assert api.chan_transcribe_all.call_args_list == [mock.call("Known"), mock.call("New")]


def test_resolution_scan_has_terminal_marker_and_distinguishes_unknown_portrait_and_mismatch(tmp_path, monkeypatch):
    api, _channel, folder, _cfg = api_for_channel(tmp_path, monkeypatch)
    dimensions = {"portrait.mp4": (720, 1280, ""), "wrong.mp4": (1920, 1080, ""), "unknown.mp4": (None, None, "")}
    for name in dimensions:
        (folder / name).write_bytes(b"video")
    monkeypatch.setattr(redownload, "_ffprobe_media_info", lambda path: dimensions[os.path.basename(path)])
    targets = []
    monkeypatch.setattr(channel_mixin, "start_managed_task", lambda *_args, **kwargs: targets.append(kwargs["target"]))
    started = api.chan_scan_resolution_mismatch("Channel", "720")
    assert started["started"] and not started.get("complete")
    assert api.chan_scan_resolution_mismatch_poll(started["token"]) == {"pending": True}
    targets.pop()()
    result = api.chan_scan_resolution_mismatch_poll(started["token"])
    assert result["complete"] and result["ok"]
    assert (result["total"], result["scanned"], result["unknown"], result["mismatch"]) == (3, 2, 1, 1)
    assert api.chan_scan_resolution_mismatch("Channel", "best")["complete"]


def test_fix_dates_enters_mutating_backend_with_one_lease_and_respects_other_owner(tmp_path, monkeypatch):
    api, channel, folder, _cfg = api_for_channel(tmp_path, monkeypatch)
    monkeypatch.setattr(channel_mixin, "admitted_operation", lambda *_args, **_kwargs: contextlib.nullcontext())
    monkeypatch.setattr(channel_mixin, "start_managed_task", lambda *_args, **kwargs: kwargs["target"]())
    snapshots = []
    run = mock.Mock(side_effect=lambda *_args, **_kwargs: snapshots.append(channel_leases.active_snapshot()) or {"ok": True})
    monkeypatch.setattr(reorg, "_fix_file_dates_impl", run)
    assert api.chan_fix_file_dates({"name": "Channel"})["started"]
    assert len(snapshots) == 1 and len(snapshots[0]) == 1
    assert channel_leases.active_snapshot() == ()
    holder = channel_leases.try_acquire(channel_aliases(channel, paths=str(folder)), LeaseOwner("test", "other"))
    assert holder.ok
    try:
        assert api.chan_fix_file_dates({"name": "Channel"})["started"]
        assert run.call_count == 1
        api._log_stream.emit_error.assert_called()
    finally:
        holder.lease.release()


def test_compression_projection_retains_unknown_duration_bytes(tmp_path, monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE videos(channel TEXT, duration_s REAL, size_bytes INTEGER, is_duplicate_of INTEGER)")
    conn.executemany("INSERT INTO videos VALUES (?,?,?,?)", [
        ("Unknown", None, 1024**3, None), ("Unknown", 0, 1024**3, None),
        ("Mixed", -1, 1024**3, None), ("Mixed", 3600, 2 * 1024**3, None),
        ("Ignored duplicate", 0, 5 * 1024**3, 1),
    ])
    monkeypatch.setattr(index, "_reader_open", lambda: conn)
    result = MediaOpsMixin().compress_dry_run("720")
    assert result["ok"]
    unknown = next(row for row in result["channels"] if row["name"] == "Unknown")
    assert unknown["unknown_videos"] == 2 and unknown["unknown_gb"] == 2
    assert unknown["current_gb"] == unknown["average_gb"] == unknown["generous_gb"] == unknown["below_gb"] == 2
    assert result["total"]["unknown_videos"] == 3
    assert result["total"]["unknown_gb"] == 3
    assert result["total"]["videos"] == 4
    conn.close()


def test_locate_rejects_folder_owned_by_other_channel_and_revalidates_transaction(tmp_path, monkeypatch):
    (tmp_path / "Other").mkdir()
    (tmp_path / "Recovered").mkdir()
    cfg = {"output_dir": str(tmp_path), "channels": [
        {"name": "Missing", "url": "https://youtube.com/@Missing"},
        {"name": "Other", "url": "https://youtube.com/@Other"}]}
    monkeypatch.setattr(subs_mixin, "load_config", lambda: copy.deepcopy(cfg))
    def update(callback):
        live = copy.deepcopy(cfg)
        result = callback(live)
        cfg.update(live)
        return result, copy.deepcopy(cfg)
    monkeypatch.setattr(subs_mixin, "update_config", update)
    monkeypatch.setattr(subs_mixin.archive_scan, "invalidate_channel", mock.Mock())
    monkeypatch.setattr(index, "invalidate_channel_videos", mock.Mock())
    api = SubsMixin()
    api._reload_config = mock.Mock()
    result = api.subs_relocate_channel({"name": "Missing"}, "Other")
    assert not result["ok"] and "already uses" in result["error"]
    assert "folder_override" not in cfg["channels"][0]
    assert channel_leases.active_snapshot() == ()
    assert api.subs_relocate_channel({"name": "Missing"}, "Recovered")["ok"]
    assert cfg["channels"][0]["folder_override"] == "Recovered"
    assert channel_leases.active_snapshot() == ()


@pytest.mark.parametrize("defer", [False, True])
def test_redownload_cancel_and_defer_acknowledge_before_next_item(tmp_path, monkeypatch, defer):
    class Api(ChannelMixin, RedownloadMixin, SyncMixin):
        pass

    channels = [{"name": name, "url": f"https://youtube.com/@{name}"} for name in ("First", "Second")]
    for channel in channels:
        (tmp_path / channel["name"]).mkdir()
    cfg = {"output_dir": str(tmp_path), "channels": channels}
    monkeypatch.setattr(channel_mixin, "load_config", lambda: cfg)
    monkeypatch.setattr(channel_mixin.subs_backend, "get_channel", lambda identity: next(ch for ch in channels if ch["name"] == identity["name"]))
    monkeypatch.setattr(channel_mixin, "start_managed_task", lambda *_args, **_kwargs: None)
    from backend import archive_scan
    monkeypatch.setattr(archive_scan, "invalidate_channel", lambda *_args: None)
    api = Api()
    api._queues = QueueState(QueueRepository(tmp_path / "queue.json"))
    monkeypatch.setattr(api._queues, "save_debounced", lambda: None)
    api._redwnl_pending = []
    api._redwnl_lock = threading.Lock()
    api._redwnl_cancel = threading.Event()
    api._sync_cancel = threading.Event()
    api._sync_skip = threading.Event()
    api._sync_pause = threading.Event()
    api._sync_thread = None
    api._window = None
    api._transcribe = mock.Mock()
    api._autorun = mock.Mock()
    api._on_queue_changed = mock.Mock()
    api._log_stream = mock.Mock()
    api._redownload_queues = lambda: api._queues
    api._redownload_log_stream = lambda: api._log_stream
    workers = []
    def start_worker(target):
        workers.append(target)
        api._sync_thread = SimpleNamespace(is_alive=lambda: True)
        return True
    api._start_sync_thread_locked = start_worker
    assert api.chan_redownload("First", "720")["started"]
    assert api.chan_redownload("Second", "720")["queued"]
    original_id = api._redwnl_pending[0]["rd_task"]["task_id"]
    visited = []
    def run(name, *_args, **kwargs):
        visited.append(name)
        assert not kwargs["cancel_ev"].is_set()
        assert not api._sync_skip.is_set()
        if len(visited) == 1:
            result = (api.sync_defer_current(original_id) if defer
                      else api.sync_skip_current(original_id))
            assert result["ok"]
            assert api._queues.current_sync["task_id"] == original_id
            assert api._queues.current_sync["cancel_requested"] is True
            assert kwargs["cancel_ev"].is_set()
            # Reattachment must wait until this exact running call returns.
            assert [item["ch"]["name"] for item in api._redwnl_pending] == ["Second"]
        elif name == "First":
            assert api._queues.current_sync["task_id"] == original_id
            assert not api._queues.current_sync.get("cancel_requested")
        return {"ok": True}
    monkeypatch.setattr(redownload, "redownload_channel", run)
    assert len(workers) == 1
    workers[0]()
    assert visited == (["First", "Second", "First"] if defer else ["First", "Second"])
    assert api._queues.current_sync is None
    assert api._queues.sync_snapshot() == []
    assert api._redwnl_pending == []
