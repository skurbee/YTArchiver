import hashlib
import json
import os
import tempfile
import threading
import zipfile
from pathlib import Path

import pytest

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch4-backup-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import auto_backup  # noqa: E402
from backend.api_mixins import backup_mixin  # noqa: E402
from backend.api_mixins.backup_mixin import BackupMixin  # noqa: E402
from backend.queues import QueueState  # noqa: E402
from backend.services.job_supervisor import JobSupervisor  # noqa: E402
from backend.ytarchiver_config import QUEUE_FILE  # noqa: E402


def _queue_sidecar() -> Path:
    return QUEUE_FILE.with_name(
        f"{QUEUE_FILE.stem}_resuming{QUEUE_FILE.suffix or '.json'}"
    )


def _clear_queue_files() -> None:
    for path in (QUEUE_FILE, _queue_sidecar()):
        path.unlink(missing_ok=True)


def _sync_task(name: str) -> dict:
    return {
        "name": name,
        "url": f"https://www.youtube.com/@{name.lower()}",
        "kind": "download",
    }


def test_queue_pair_stays_on_one_generation_during_forced_transition(
        tmp_path, monkeypatch):
    _clear_queue_files()
    queues = QueueState()
    queues.set_current_sync(_sync_task("BeforeSnapshot"))
    before = dict(queues.current_sync or {})
    transitioned = threading.Event()
    real_write = auto_backup._write_zip_bytes_resource

    def _write_then_transition(
            zipped, content, arcname, cancel_event=None):
        result = real_write(
            zipped, content, arcname, cancel_event=cancel_event)
        if arcname == QUEUE_FILE.name:
            # This is the exact old failure window: queue.json has already
            # entered the ZIP, while the sidecar resource has not been read.
            queues.set_current_sync(_sync_task("AfterSnapshot"))
            transitioned.set()
        return result

    monkeypatch.setattr(
        auto_backup,
        "_write_zip_bytes_resource",
        _write_then_transition,
    )
    archive = tmp_path / "coherent.zip"

    auto_backup.build_backup_zip(str(archive), queue_state=queues)

    assert transitioned.is_set()
    with zipfile.ZipFile(archive, "r") as zipped:
        main_bytes = zipped.read(QUEUE_FILE.name)
        sidecar_bytes = zipped.read(_queue_sidecar().name)
        main = json.loads(main_bytes)
        sidecar = json.loads(sidecar_bytes)
        manifest = json.loads(zipped.read(auto_backup.BACKUP_MANIFEST_NAME))

    assert main["_backup_generation"] == sidecar["_backup_generation"]
    assert main["resuming"]["sync"] == before
    assert sidecar["resuming"]["sync"] == before
    assert json.loads(_queue_sidecar().read_text(
        encoding="utf-8"))["resuming"]["sync"]["name"] == "AfterSnapshot"
    for name, payload in (
        (QUEUE_FILE.name, main_bytes),
        (_queue_sidecar().name, sidecar_bytes),
    ):
        assert manifest["resources"][name]["sha256"] == hashlib.sha256(
            payload).hexdigest()
        assert manifest["resources"][name]["size"] == len(payload)


def test_backup_refuses_live_queue_files_without_their_owner(tmp_path):
    _clear_queue_files()
    QUEUE_FILE.write_text(
        json.dumps({"_schema_version": 3, "sync": [], "gpu": []}),
        encoding="utf-8",
    )
    archive = tmp_path / "unsafe.zip"

    with pytest.raises(RuntimeError, match="live queue state is required"):
        auto_backup.build_backup_zip(str(archive))

    assert not archive.exists()
    assert not Path(f"{archive}.tmp").exists()


def test_cancelled_backup_never_commits_partial_zip(tmp_path):
    _clear_queue_files()
    queues = QueueState()
    cancel = threading.Event()
    cancel.set()
    archive = tmp_path / "cancelled.zip"

    with pytest.raises(auto_backup.BackupCancelled):
        auto_backup.build_backup_zip(
            str(archive), queue_state=queues, cancel_event=cancel)

    assert not archive.exists()
    assert not Path(f"{archive}.tmp").exists()


def test_lifecycle_cancel_between_queue_members_removes_partial_zip(
        tmp_path, monkeypatch):
    _clear_queue_files()
    queues = QueueState()
    queues.set_current_sync(_sync_task("CancelBoundary"))
    cancel = threading.Event()
    archive = tmp_path / "cancelled-mid-generation.zip"
    real_write = auto_backup._write_zip_bytes_resource

    def _cancel_after_main(zipped, content, arcname, cancel_event=None):
        result = real_write(
            zipped, content, arcname, cancel_event=cancel_event)
        if arcname == QUEUE_FILE.name:
            cancel.set()
        return result

    monkeypatch.setattr(
        auto_backup,
        "_write_zip_bytes_resource",
        _cancel_after_main,
    )

    with pytest.raises(auto_backup.BackupCancelled):
        auto_backup.build_backup_zip(
            str(archive), queue_state=queues, cancel_event=cancel)

    assert not archive.exists()
    assert not Path(f"{archive}.tmp").exists()


def test_manual_export_is_visible_to_lifecycle_supervisor(
        tmp_path, monkeypatch):
    _clear_queue_files()
    observed = {}

    class Window:
        def create_file_dialog(self, *_args, **_kwargs):
            return [str(tmp_path / "manual.zip")]

    class Api(BackupMixin):
        def __init__(self):
            self._window = Window()
            self._queues = QueueState()
            self._job_supervisor = JobSupervisor()

    api = Api()

    def _fake_build(out_path, *, queue_state, cancel_event):
        observed["path"] = out_path
        observed["queue_state"] = queue_state
        observed["cancel_event"] = cancel_event
        observed["owners"] = api._job_supervisor.snapshot()["owners"]
        return {
            "files": 2,
            "fts_skipped_reason": "",
        }

    monkeypatch.setattr(backup_mixin, "_build_backup_zip", _fake_build)
    monkeypatch.setattr(
        backup_mixin,
        "update_config",
        lambda mutator: (None, {}),
    )

    result = api.export_full_backup()

    assert result["ok"]
    assert observed["queue_state"] is api._queues
    assert isinstance(observed["cancel_event"], threading.Event)
    assert any(
        row["owner"] == "backup-export" and row["active"]
        for row in observed["owners"]
    )
    assert not any(
        row["owner"] == "backup-export"
        for row in api._job_supervisor.snapshot()["owners"]
    )


def test_manual_export_obeys_closed_work_admission(tmp_path, monkeypatch):
    class Window:
        def create_file_dialog(self, *_args, **_kwargs):
            return [str(tmp_path / "must-not-start.zip")]

    class Api(BackupMixin):
        def __init__(self):
            self._window = Window()
            self._queues = QueueState()
            self._job_supervisor = JobSupervisor()

    api = Api()
    api._job_supervisor.close_admission("backup restore")
    build_called = threading.Event()

    def _unexpected_build(*_args, **_kwargs):
        build_called.set()
        raise AssertionError("backup crossed closed admission")

    monkeypatch.setattr(
        backup_mixin, "_build_backup_zip", _unexpected_build)

    result = api.export_full_backup()

    assert not result["ok"]
    assert "backup restore" in result["error"]
    assert not build_called.is_set()


def test_scheduler_forwards_queue_owner_and_stop_token(tmp_path, monkeypatch):
    queues = QueueState()
    scheduler = auto_backup.AutoBackupScheduler(queue_state=queues)
    captured = {}

    monkeypatch.setattr(
        auto_backup,
        "load_config",
        lambda: {
            "auto_backup_interval": "daily",
            "last_auto_backup_ts": 0,
            "output_dir": str(tmp_path),
        },
    )

    def _fake_run(output_dir, **kwargs):
        captured["output_dir"] = output_dir
        captured.update(kwargs)
        return {"ok": True, "path": "backup.zip", "files": 2}

    monkeypatch.setattr(auto_backup, "run_backup", _fake_run)

    scheduler._tick()

    assert captured["output_dir"] == str(tmp_path)
    assert captured["queue_state"] is queues
    assert captured["cancel_event"] is scheduler._stop
