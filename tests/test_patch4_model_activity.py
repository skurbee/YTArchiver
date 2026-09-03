import json
import os
import tempfile
import threading
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch4-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)


class _Stream:
    def emit_error(self, *_args, **_kwargs):
        pass

    def emit_text(self, *_args, **_kwargs):
        pass

    def emit_dim(self, *_args, **_kwargs):
        pass

    def emit(self, *_args, **_kwargs):
        pass

    def flush(self):
        pass


def test_transcription_jobs_freeze_the_requested_model_at_enqueue(tmp_path):
    from backend.transcribe.core import TranscribeManager

    first = tmp_path / "first.mp4"
    second = tmp_path / "second.mp4"
    first.write_bytes(b"video")
    second.write_bytes(b"video")

    manager = TranscribeManager(_Stream(), model="small")
    manager._ensure_worker = lambda: None
    manager._persist_pending = lambda: True

    assert manager.enqueue(str(first), "First", requested_model="tiny")
    assert manager.swap_model("medium")
    assert manager.enqueue(str(second), "Second")

    assert manager._jobs[0]["requested_model"] == "tiny"
    assert manager._jobs[1]["requested_model"] == "medium"
    snapshot = manager._pending_snapshot()
    assert [row["requested_model"] for row in snapshot] == ["tiny", "medium"]


def test_worker_switches_for_each_job_and_records_the_loaded_model():
    from backend.transcribe.core import TranscribeManager

    manager = TranscribeManager(_Stream(), model="small")
    manager._loaded_model = "small"
    stopped = []
    started = []

    def _stop(*_args, **_kwargs):
        stopped.append(True)
        manager._loaded_model = ""

    def _start(model=None):
        started.append(model)
        manager._loaded_model = model or manager.current_model()
        return True

    manager._stop_subprocess = _stop
    manager.start_subprocess = _start
    job = {"requested_model": "medium"}

    assert manager._prepare_job_model(job)
    assert stopped == [True]
    assert started == ["medium"]
    assert job["actual_model"] == "medium"


def test_start_rejects_a_fake_worker_that_reports_the_wrong_model():
    from backend.process_runner import PROCESS_REGISTRY
    from backend.transcribe.core import TranscribeManager

    class _ReadPipe:
        def __init__(self, lines=()):
            self._lines = list(lines)

        def readline(self):
            return self._lines.pop(0) if self._lines else ""

    class _WritePipe:
        def __init__(self):
            self.closed = False

        def write(self, _value):
            return None

        def flush(self):
            return None

        def close(self):
            self.closed = True

    class _FakeWorker:
        pid = 424242

        def __init__(self):
            self.stdin = _WritePipe()
            self.stdout = _ReadPipe([
                json.dumps({
                    "status": "ready",
                    "device": "cuda",
                    "model": "medium",
                }) + "\n"
            ])
            self.stderr = _ReadPipe()
            self.returncode = None

        def poll(self):
            return self.returncode

        def terminate(self):
            self.returncode = 0

        def kill(self):
            self.returncode = -9

        def wait(self, timeout=None):
            del timeout
            if self.returncode is None:
                self.returncode = 0
            return self.returncode

    manager = TranscribeManager(_Stream(), model="small")
    manager._python311 = "python.exe"
    fake_worker = _FakeWorker()

    with mock.patch(
        "backend.transcribe.core.subprocess.Popen",
        return_value=fake_worker,
    ), mock.patch.object(
        PROCESS_REGISTRY,
        "register",
    ), mock.patch.object(
        PROCESS_REGISTRY,
        "unregister",
    ), mock.patch.object(
        PROCESS_REGISTRY,
        "terminate_process",
    ):
        assert not manager.start_subprocess(model="small")

    assert manager._loaded_model == ""
    assert manager._proc is None
    assert fake_worker.poll() is not None


def test_worker_reported_actual_model_is_durable_and_mismatch_is_rejected():
    from backend.transcribe.core import TranscribeManager

    manager = TranscribeManager(_Stream(), model="medium")
    manager._loaded_model = "medium"
    job = {
        "path": "video.mp4",
        "title": "Video",
        "requested_model": "medium",
        "actual_model": "",
    }
    manager._current_job = job
    durable_snapshots = []
    manager._write_pending_snapshot = lambda snapshot: (
        durable_snapshots.append(snapshot) or True
    )

    assert manager._accept_worker_model_report(
        {"status": "ok", "model": "medium"},
        job,
        phase="result",
    )
    assert job["actual_model"] == "medium"
    assert durable_snapshots[-1][0]["actual_model"] == "medium"

    writes_before_mismatch = len(durable_snapshots)
    assert not manager._accept_worker_model_report(
        {"status": "ok", "model": "small"},
        job,
        phase="result",
    )
    assert job["actual_model"] == "medium"
    assert len(durable_snapshots) == writes_before_mismatch


def test_activity_history_uses_one_jsonl_store_and_updates_by_stable_id(
        tmp_path, monkeypatch):
    from backend.activity_history import ActivityHistoryStore

    history_path = tmp_path / "activity_history.jsonl"
    store = ActivityHistoryStore(history_path, max_entries=20)

    generated_id = store.append("first entry")
    assert generated_id
    assert store.upsert("download-row-1", "download v1")
    assert store.upsert("download-row-1", "download v2")

    records = [json.loads(line) for line in history_path.read_text(
        encoding="utf-8").splitlines()]
    assert [record["id"] for record in records] == [
        generated_id, "download-row-1"]
    assert [record["entry"] for record in records] == [
        "first entry", "download v2"]
    assert store.entries() == ["first entry", "download v2"]


def test_activity_history_migrates_config_once_without_split_brain(tmp_path):
    from backend.activity_history import ActivityHistoryStore

    history_path = tmp_path / "activity_history.jsonl"
    store = ActivityHistoryStore(history_path, max_entries=20)
    assert store.migrate_legacy(["legacy one", "legacy two"])
    assert store.migrate_legacy(["legacy one", "legacy two"])

    records = [json.loads(line) for line in history_path.read_text(
        encoding="utf-8").splitlines()]
    assert [row["entry"] for row in records] == ["legacy one", "legacy two"]
    assert len({row["id"] for row in records}) == 2


def test_activity_repositories_for_one_file_share_the_same_lock(tmp_path):
    from backend.activity_history import ActivityHistoryStore

    history_path = tmp_path / "activity_history.jsonl"
    sync_store = ActivityHistoryStore(history_path)
    autorun_store = ActivityHistoryStore(history_path)

    assert sync_store._lock is autorun_store._lock


def test_activity_clear_is_linearizable_with_append_and_legacy_retirement(
        tmp_path):
    from backend.activity_history import ActivityHistoryStore

    history_path = tmp_path / "activity_history.jsonl"
    clearing_store = ActivityHistoryStore(history_path)
    appending_store = ActivityHistoryStore(history_path)
    assert clearing_store.append("before-clear")

    retirement_entered = threading.Event()
    release_retirement = threading.Event()
    append_finished = threading.Event()
    outcome = {}

    def _retire_legacy():
        retirement_entered.set()
        assert release_retirement.wait(2)
        return 2

    def _clear():
        outcome["removed"] = clearing_store.clear(
            retire_legacy=_retire_legacy)

    def _append():
        outcome["appended"] = appending_store.append("after-clear")
        append_finished.set()

    clear_thread = threading.Thread(target=_clear)
    append_thread = threading.Thread(target=_append)
    clear_thread.start()
    assert retirement_entered.wait(2)
    append_thread.start()
    assert not append_finished.wait(0.05)

    release_retirement.set()
    clear_thread.join(2)
    append_thread.join(2)

    assert outcome["removed"] == 3
    assert outcome["appended"]
    assert appending_store.entries() == ["after-clear"]


def test_activity_history_backup_restore_round_trip(tmp_path):
    from backend.activity_history import ACTIVITY_HISTORY_FILE, ActivityHistoryStore
    from backend.auto_backup import backup_file_entries, build_backup_zip
    from backend.services.restore_coordinator import restore_backup
    from backend.ytarchiver_config import DEFAULT_CONFIG, save_config

    assert save_config(dict(DEFAULT_CONFIG))
    store = ActivityHistoryStore(ACTIVITY_HISTORY_FILE)
    store.clear()
    assert store.append("history generation from backup", entry_id="before")

    exported = {name for name, _path in backup_file_entries()}
    assert ACTIVITY_HISTORY_FILE.name in exported
    archive = tmp_path / "activity-round-trip.zip"
    build_backup_zip(str(archive))

    assert store.clear() == 1
    assert store.append("replacement generation", entry_id="after")
    restored = restore_backup(archive, before_commit=lambda: True)

    assert restored["ok"]
    assert store.records()[0]["id"] == "before"
    assert store.entries() == ["history generation from backup"]
