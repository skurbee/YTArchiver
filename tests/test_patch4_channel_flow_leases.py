from __future__ import annotations

import contextlib
import copy
import os
import tempfile
import threading
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch4-flow-leases-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import redownload, reorg  # noqa: E402
from backend.api_mixins import archive_mixin  # noqa: E402
from backend.api_mixins.archive_mixin import ArchiveMixin  # noqa: E402
from backend.services.channel_leases import (  # noqa: E402
    LeaseOwner,
    channel_aliases,
    channel_leases,
    path_alias,
)
from backend.sync import sync_all as sync_all_function  # noqa: E402
from backend.transcribe.core import (  # noqa: E402
    TranscribeManager,
    _WorkerOutcome,
)

sync_all = __import__("backend.sync.sync_all", fromlist=["sync_all"])


def _holder(job: str, aliases):
    result = channel_leases.try_acquire(
        aliases, LeaseOwner("test-holder", job, label="Existing channel job")
    )
    assert result.ok and result.lease is not None
    return result.lease


def test_reorg_resolves_configured_identity_and_holds_lease_through_impl(
    tmp_path,
    monkeypatch,
):
    archive = tmp_path / "Archive"
    folder = archive / "Live Folder"
    folder.mkdir(parents=True)
    channel = {
        "name": "Live Name",
        "folder_override": "Live Folder",
        "url": "https://www.youtube.com/@LeaseReorg/videos",
    }
    monkeypatch.setattr(
        reorg, "load_config", lambda: {"output_dir": str(archive), "channels": [channel]}
    )
    observed = {}

    def fake_impl(target, *_args, **_kwargs):
        observed["target"] = target
        observed["snapshot"] = channel_leases.active_snapshot()
        return {"ok": True, "moved": 0}

    monkeypatch.setattr(reorg, "_reorg_channel_impl", fake_impl)
    result = reorg.reorg_channel(str(folder), True, False, mock.Mock())

    assert result["ok"]
    assert Path(observed["target"]) == folder
    assert len(observed["snapshot"]) == 1
    held_aliases = set(observed["snapshot"][0].aliases)
    assert "channel-url:youtube.com/@leasereorg" in held_aliases
    assert path_alias(folder) in held_aliases
    assert channel_leases.active_snapshot() == ()


def test_reorg_busy_result_is_visible_and_never_enters_mutating_impl(
    tmp_path,
    monkeypatch,
):
    folder = tmp_path / "Busy Reorg"
    folder.mkdir()
    monkeypatch.setattr(reorg, "load_config", lambda: {"channels": []})
    holder = _holder("reorg-busy", channel_aliases(paths=folder))
    stream = mock.Mock()
    try:
        with mock.patch.object(reorg, "_reorg_channel_impl") as run:
            result = reorg.reorg_channel(str(folder), False, False, stream)
        assert result["ok"] is False
        assert result["busy"] is True
        assert result["reason"] == "channel_busy"
        assert result["blockers"][0]["job_id"] == "reorg-busy"
        run.assert_not_called()
        stream.emit_error.assert_called_once()
    finally:
        holder.release()


def test_fix_dates_preserves_preexisting_cancellation(tmp_path, monkeypatch):
    folder = tmp_path / "Cancelled Dates"
    folder.mkdir()
    monkeypatch.setattr(reorg, "load_config", lambda: {"channels": []})
    cancel = threading.Event()
    cancel.set()
    with mock.patch.object(reorg, "_fix_file_dates_impl") as run:
        result = reorg.fix_file_dates(str(folder), mock.Mock(), cancel)
    assert result == {"ok": False, "cancelled": True, "reason": "cancelled"}
    run.assert_not_called()


def test_redownload_refreshes_stale_folder_and_file_path_before_work(
    tmp_path,
    monkeypatch,
):
    archive = tmp_path / "Archive"
    old_folder = archive / "Old Folder"
    new_folder = archive / "New Folder"
    old_file = old_folder / "2024" / "Video.mp4"
    channel = {
        "name": "Current Name",
        "folder_override": "New Folder",
        "url": "https://youtube.com/@LeaseRedownload",
    }
    monkeypatch.setattr(
        redownload,
        "load_config",
        lambda: {"output_dir": str(archive), "channels": [channel]},
    )
    observed = {}

    def fake_impl(name, url, folder, _resolution, *_args, **kwargs):
        observed.update(
            {
                "name": name,
                "url": url,
                "folder": folder,
                "file": kwargs["only_filepath"],
                "snapshot": channel_leases.active_snapshot(),
            }
        )
        return {"ok": True, "done": 1}

    monkeypatch.setattr(redownload, "_redownload_channel_impl", fake_impl)
    result = redownload.redownload_channel(
        "Stale Name",
        "https://www.youtube.com/@leaseredownload/videos",
        str(old_folder),
        "720",
        mock.Mock(),
        threading.Event(),
        only_video_id="abc123def45",
        only_filepath=str(old_file),
    )

    assert result["ok"]
    assert observed["name"] == "Current Name"
    assert Path(observed["folder"]) == new_folder
    assert Path(observed["file"]) == new_folder / "2024" / "Video.mp4"
    aliases = set(observed["snapshot"][0].aliases)
    assert path_alias(old_folder) in aliases
    assert path_alias(new_folder) in aliases
    assert channel_leases.active_snapshot() == ()


def test_busy_queued_redownload_returns_to_pending_before_wrapper_exits(
    tmp_path,
    monkeypatch,
):
    task = {
        "task_id": "redownload-lease-busy",
        "kind": "redownload",
        "name": "Queued Redownload",
        "url": "https://youtube.com/@LeaseRedownloadBusy",
    }
    folder = tmp_path / "Archive" / "Queued Redownload"
    monkeypatch.setattr(
        redownload,
        "load_config",
        lambda: {"output_dir": str(tmp_path / "Archive"), "channels": [task]},
    )
    _name, _url, _folder, _file, aliases = redownload._resolve_redownload_target(
        task["name"], task["url"], str(folder), ""
    )
    holder = _holder("redownload-blocker", aliases)
    queue = _DurableSyncQueue(task)
    queue.current_sync = queue.pending.pop(0)
    stream = mock.Mock()
    try:
        with mock.patch.object(redownload, "_redownload_channel_impl") as run:
            result = redownload.redownload_channel(
                task["name"],
                task["url"],
                str(folder),
                "720",
                stream,
                threading.Event(),
                queues=queue,
            )
        assert result["ok"] is False
        assert result["reason"] == "channel_busy"
        assert result["requeued"] is True
        assert queue.current_sync is None
        assert [item["task_id"] for item in queue.pending] == ["redownload-lease-busy"]
        run.assert_not_called()
        stream.emit_error.assert_called_once()
    finally:
        holder.release()


class _DurableSyncQueue:
    def __init__(self, task):
        self.pending = [copy.deepcopy(task)]
        self.current_sync = None
        self.sync_paused = False

    def sync_snapshot(self):
        return copy.deepcopy(self.pending)

    def sync_peek_next(self, **_kwargs):
        return copy.deepcopy(self.pending[0]) if self.pending else None

    def sync_promote_task_to_current(self, task_id):
        if not self.pending or self.pending[0]["task_id"] != task_id:
            return False
        self.current_sync = self.pending.pop(0)
        return True

    def sync_requeue_current_front(self, task):
        if self.current_sync is None:
            return False
        self.pending.insert(0, copy.deepcopy(task))
        self.current_sync = None
        return True

    def replace_current_task_durable(self, _lane, replacement, *, expected_task_id=None):
        current_id = str((self.current_sync or {}).get("task_id") or "")
        if current_id != str(expected_task_id or ""):
            return False
        self.current_sync = copy.deepcopy(replacement)
        return True

    def set_sync_pass_progress(self, _current, _total):
        return None


def test_sync_busy_task_is_requeued_and_not_silently_acknowledged(
    tmp_path,
    monkeypatch,
):
    task = {
        "task_id": "sync-lease-busy",
        "kind": "download",
        "name": "Queued Name",
        "folder_override": "Queued Folder",
        "url": "https://www.youtube.com/@LeaseSyncBusy/videos",
    }
    live = {
        **task,
        "name": "Current Name",
        "folder_override": "Current Folder",
        "url": "https://youtube.com/@leasesyncbusy",
    }
    config = {"output_dir": str(tmp_path / "Archive"), "channels": [live]}
    queue = _DurableSyncQueue(task)
    monkeypatch.setattr(sync_all, "load_config", lambda: copy.deepcopy(config))
    monkeypatch.setattr(sync_all, "ARCHIVE_FILE", str(tmp_path / "missing-archive.txt"))
    monkeypatch.setattr(sync_all, "clear_sync_progress", lambda: None)
    monkeypatch.setattr(sync_all, "_check_batch_cooldown", lambda _ch: (True, ""))
    monkeypatch.setattr(
        sync_all,
        "config_transaction",
        lambda: contextlib.nullcontext(config),
    )
    _cfg, _resolved, aliases = sync_all._resolve_sync_task_target(task)
    holder = _holder("sync-blocker", aliases)
    stream = mock.Mock()
    try:
        with mock.patch.object(sync_all, "sync_channel") as run:
            result = sync_all_function(
                stream,
                queues=queue,
                add_downloads_from_config=False,
            )
        assert result["ok"] is False
        assert result["reason"] == "channel_busy"
        assert result["busy"]["task_id"] == "sync-lease-busy"
        assert result["busy"]["requeued"] is True
        assert queue.current_sync is None
        assert [item["task_id"] for item in queue.pending] == ["sync-lease-busy"]
        assert queue.pending[0]["url"] == task["url"]
        run.assert_not_called()
        assert stream.emit_error.called
    finally:
        holder.release()


def test_sync_execution_resolution_keeps_task_scope_but_uses_live_folder(
    tmp_path,
    monkeypatch,
):
    task = {
        "task_id": "metadata-live-target",
        "kind": "metadata",
        "name": "Old Name",
        "folder_override": "Old Folder",
        "url": "https://www.youtube.com/@LeaseLive/videos",
        "refresh": True,
        "scope": {"year": 2025},
    }
    live = {
        "name": "New Name",
        "folder_override": "New Folder",
        "url": "https://youtube.com/@leaselive",
        "mode": "full",
    }
    archive = tmp_path / "Archive"
    monkeypatch.setattr(
        sync_all,
        "load_config",
        lambda: {"output_dir": str(archive), "channels": [live]},
    )

    _cfg, resolved, aliases = sync_all._resolve_sync_task_target(task)

    assert resolved["name"] == "New Name"
    assert resolved["folder_override"] == "New Folder"
    assert resolved["task_id"] == "metadata-live-target"
    assert resolved["kind"] == "metadata"
    assert resolved["refresh"] is True
    assert resolved["scope"] == {"year": 2025}
    assert path_alias(archive / "Old Folder") in aliases
    assert path_alias(archive / "New Folder") in aliases


def test_gpu_output_waits_for_the_same_channel_lease(tmp_path):
    archive = tmp_path / "Archive"
    folder = archive / "Shared Folder"
    folder.mkdir(parents=True)
    media = folder / "2025" / "Video.mp4"
    media.parent.mkdir()
    media.write_bytes(b"video")
    channel = {
        "name": "Shared Name",
        "folder_override": "Shared Folder",
        "url": "https://youtube.com/@SharedLease",
    }
    aliases = channel_aliases(channel, paths=[folder])
    holder = _holder("reorg-holds-channel", aliases)
    manager = TranscribeManager(mock.Mock(), model="small")
    manager.attach_queues(
        None,
        cfg_loader=lambda: {
            "output_dir": str(archive),
            "channels": [channel],
        },
    )
    entered = threading.Event()
    finished = threading.Event()
    returned = []
    job = {
        "task_id": "gpu-same-channel",
        "kind": "compress",
        "channel": "Shared Name",
        "path": str(media),
        "cancel": threading.Event(),
    }

    def mutate(_job):
        entered.set()
        return _WorkerOutcome.SUCCESS

    def run():
        returned.append(manager._run_under_channel_lease(job, mutate))
        finished.set()

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    assert not entered.wait(0.15)
    holder.release()
    assert finished.wait(1.0)
    assert entered.is_set()
    assert returned == [_WorkerOutcome.SUCCESS]
    assert channel_leases.active_snapshot() == ()


def test_gpu_channel_lease_timeout_is_a_wait_not_a_processing_failure(
    tmp_path,
    monkeypatch,
):
    archive = tmp_path / "Archive"
    folder = archive / "Shared Folder"
    folder.mkdir(parents=True)
    media = folder / "Video.mp4"
    media.write_bytes(b"video")
    channel = {
        "name": "Shared Name",
        "folder_override": "Shared Folder",
        "url": "https://youtube.com/@SharedLeaseWait",
    }
    stream = mock.Mock()
    manager = TranscribeManager(stream, model="small")
    manager.attach_queues(
        None,
        cfg_loader=lambda: {
            "output_dir": str(archive),
            "channels": [channel],
        },
    )
    timeout_result = mock.Mock(
        ok=False,
        status="timeout",
        lease=None,
        explanation="Timed out waiting for a channel lease.",
    )
    acquired_result = mock.Mock(
        ok=True,
        status="acquired",
        lease=contextlib.nullcontext(),
        explanation="Lease acquired.",
    )
    acquire = mock.Mock(side_effect=[timeout_result, acquired_result])
    monkeypatch.setattr(channel_leases, "acquire", acquire)
    callback = mock.Mock(return_value=_WorkerOutcome.SUCCESS)
    job = {
        "task_id": "gpu-waits-after-timeout",
        "kind": "transcribe",
        "channel": "Shared Name",
        "path": str(media),
        "cancel": threading.Event(),
    }

    outcome = manager._run_under_channel_lease(job, callback)

    assert outcome is _WorkerOutcome.SUCCESS
    assert acquire.call_count == 2
    callback.assert_called_once_with(job)
    stream.emit_error.assert_not_called()
    stream.emit_text.assert_called_once_with(
        "Processing is waiting for another task on this channel to finish.\n",
        "simpleline_blue",
    )


def test_gpu_channel_lease_wait_still_honors_cancellation(tmp_path, monkeypatch):
    media = tmp_path / "Video.mp4"
    media.write_bytes(b"video")
    cancel = threading.Event()
    cancel.set()
    stream = mock.Mock()
    manager = TranscribeManager(stream, model="small")
    manager.attach_queues(None, cfg_loader=lambda: {"channels": []})
    monkeypatch.setattr(
        channel_leases,
        "acquire",
        mock.Mock(return_value=mock.Mock(
            ok=False,
            status="cancelled",
            lease=None,
            explanation="Lease request was cancelled.",
        )),
    )
    callback = mock.Mock(return_value=_WorkerOutcome.SUCCESS)
    job = {
        "task_id": "gpu-cancelled-while-waiting",
        "kind": "transcribe",
        "path": str(media),
        "cancel": cancel,
    }

    outcome = manager._run_under_channel_lease(job, callback)

    assert outcome is _WorkerOutcome.CANCELLED
    callback.assert_not_called()
    stream.emit_error.assert_not_called()
    stream.emit_text.assert_not_called()


def test_manual_download_reports_busy_before_starting_child(tmp_path):
    output = tmp_path / "Manual Output"
    output.mkdir()
    holder = _holder("maintenance-owns-output", channel_aliases(paths=[output]))
    api = ArchiveMixin()
    try:
        with (
            mock.patch.object(
                archive_mixin.sync_backend, "find_yt_dlp",
                return_value="yt-dlp.exe"),
            mock.patch.object(
                archive_mixin, "load_config",
                return_value={"video_out_dir": str(output)}),
            mock.patch.object(archive_mixin, "_probe_output_folder_writable"),
            mock.patch.object(archive_mixin, "popen_ytdlp") as launch,
        ):
            result = api.archive_single_video(
                "https://youtube.com/watch?v=abcdefghijk")
        assert result["ok"] is False
        assert result["busy"] is True
        assert "Existing channel job" in result["error"]
        assert "maintenance-owns-output" not in result["error"]
        assert "Try again after the active work finishes" in result["error"]
        launch.assert_not_called()
        assert api.archive_single_is_running() is False
    finally:
        holder.release()
