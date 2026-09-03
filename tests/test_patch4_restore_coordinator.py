import json
import os
import tempfile
import zipfile
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch4-restore-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend.services import restore_coordinator as restore  # noqa: E402
from backend.ytarchiver_config import (  # noqa: E402
    APP_DATA_DIR,
    CONFIG_FILE,
    DISK_CACHE_FILE,
    QUEUE_FILE,
    TRANSCRIPTION_DB,
)


def _write_zip(path: Path, members: dict[str, str | bytes], manifest=None):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zipped:
        for name, value in members.items():
            zipped.writestr(name, value)
        if manifest is not None:
            zipped.writestr(
                restore.BACKUP_MANIFEST_NAME,
                json.dumps(manifest),
            )


def _config(marker: str) -> str:
    return json.dumps({"channels": [], "marker": marker})


def _queue(marker: str) -> str:
    return json.dumps({"sync": [], "gpu": [], "marker": marker})


def test_restore_rejects_unknown_member_before_touching_live_state(tmp_path):
    CONFIG_FILE.write_text(_config("old"), encoding="utf-8")
    archive = tmp_path / "unknown.zip"
    _write_zip(
        archive,
        {
            CONFIG_FILE.name: _config("new"),
            "not-owned-by-ytarchiver.json": "{}",
        },
    )

    result = restore.restore_backup(archive)

    assert not result["ok"]
    assert "unknown resource" in result["error"]
    assert json.loads(CONFIG_FILE.read_text(encoding="utf-8"))["marker"] == "old"


def test_manifest_checksum_mismatch_fails_during_staging(tmp_path):
    CONFIG_FILE.write_text(_config("old"), encoding="utf-8")
    archive = tmp_path / "bad-hash.zip"
    _write_zip(
        archive,
        {CONFIG_FILE.name: _config("new")},
        manifest={
            "resources": {
                CONFIG_FILE.name: {"sha256": "0" * 64, "size": 1},
            }
        },
    )

    result = restore.restore_backup(archive)

    assert not result["ok"]
    assert "Checksum mismatch" in result["error"]
    assert json.loads(CONFIG_FILE.read_text(encoding="utf-8"))["marker"] == "old"


def test_v2_manifest_must_hash_every_restored_resource(tmp_path):
    CONFIG_FILE.write_text(_config("old"), encoding="utf-8")
    archive = tmp_path / "omitted-hash.zip"
    _write_zip(
        archive,
        {CONFIG_FILE.name: _config("tampered")},
        manifest={
            "manifest_version": 2,
            "app": "YTArchiver",
            "backup_type": "app-state",
            "resources": {},
        },
    )

    result = restore.restore_backup(archive)

    assert not result["ok"]
    assert "omits restored resource" in result["error"]
    assert json.loads(CONFIG_FILE.read_text(encoding="utf-8"))["marker"] == "old"


def test_commit_failure_rolls_every_live_resource_back(tmp_path):
    CONFIG_FILE.write_text(_config("old"), encoding="utf-8")
    QUEUE_FILE.write_text(_queue("old"), encoding="utf-8")
    DISK_CACHE_FILE.write_text(json.dumps({"marker": "old"}), encoding="utf-8")
    before = {
        CONFIG_FILE: CONFIG_FILE.read_bytes(),
        QUEUE_FILE: QUEUE_FILE.read_bytes(),
        DISK_CACHE_FILE: DISK_CACHE_FILE.read_bytes(),
    }
    archive = tmp_path / "rollback.zip"
    _write_zip(
        archive,
        {
            CONFIG_FILE.name: _config("new"),
            QUEUE_FILE.name: _queue("new"),
        },
    )

    real_replace = restore.os.replace
    failed = False

    def fail_once(source, target):
        nonlocal failed
        if Path(target) == QUEUE_FILE and not failed:
            failed = True
            raise OSError("forced queue commit failure")
        return real_replace(source, target)

    with mock.patch.object(restore.os, "replace", side_effect=fail_once):
        result = restore.restore_backup(archive)

    assert not result["ok"]
    assert "rolled back" in result["error"]
    for path, original in before.items():
        assert path.read_bytes() == original
    assert not restore.RESTORE_JOURNAL.exists()


def test_success_is_snapshot_semantics_and_removes_absent_sidecars(tmp_path):
    CONFIG_FILE.write_text(_config("old"), encoding="utf-8")
    QUEUE_FILE.write_text(_queue("old"), encoding="utf-8")
    queue_resuming = QUEUE_FILE.with_name(
        f"{QUEUE_FILE.stem}_resuming{QUEUE_FILE.suffix}"
    )
    queue_resuming.write_text(
        json.dumps({"resuming": {"gpu": {"task_id": "stale"}}}),
        encoding="utf-8",
    )
    wal = Path(f"{TRANSCRIPTION_DB}-wal")
    shm = Path(f"{TRANSCRIPTION_DB}-shm")
    wal.write_bytes(b"stale-wal")
    shm.write_bytes(b"stale-shm")
    archive = tmp_path / "valid.zip"
    _write_zip(
        archive,
        {
            CONFIG_FILE.name: _config("new"),
            QUEUE_FILE.name: _queue("restored"),
        },
    )

    result = restore.restore_backup(archive, before_commit=lambda: True)

    assert result["ok"]
    assert result["state_already_committed"]
    assert json.loads(CONFIG_FILE.read_text(encoding="utf-8"))["marker"] == "new"
    assert json.loads(QUEUE_FILE.read_text(encoding="utf-8"))["marker"] == "restored"
    assert not queue_resuming.exists()
    assert not wal.exists()
    assert not shm.exists()
    assert not restore.RESTORE_JOURNAL.exists()


def test_failed_one_way_quiesce_preserves_restart_requirement(tmp_path):
    CONFIG_FILE.write_text(_config("old"), encoding="utf-8")
    archive = tmp_path / "quiesce-failed.zip"
    _write_zip(archive, {CONFIG_FILE.name: _config("new")})

    result = restore.restore_backup(
        archive,
        before_commit=lambda: {
            "ok": False,
            "needs_restart": True,
            "error": "queue owner could not be frozen",
        },
    )

    assert not result["ok"]
    assert result["needs_restart"] is True
    assert "queue owner" in result["error"]
    assert json.loads(CONFIG_FILE.read_text(encoding="utf-8"))["marker"] == "old"


def test_startup_recovery_rolls_back_interrupted_commit(tmp_path):
    CONFIG_FILE.write_text(_config("new-partial"), encoding="utf-8")
    stage_root = APP_DATA_DIR.parent / (
        f"{restore._STAGE_PREFIX}{next(tempfile._get_candidate_names())}"
    )
    rollback = stage_root / "rollback"
    rollback.mkdir(parents=True)
    old = rollback / "0000-config.json"
    old.write_text(_config("old"), encoding="utf-8")
    staged = stage_root / "live" / CONFIG_FILE.name
    # Missing staged file means it was already moved to the live target.
    record = {
        "version": 1,
        "state": "committing",
        "stage_root": str(stage_root),
        "entries": [{
            "target": str(CONFIG_FILE),
            "staged": str(staged),
            "old_backup": str(old),
            "had_old": True,
        }],
    }
    restore._write_journal(record)

    result = restore.recover_interrupted_restore()

    assert result == {"ok": True, "recovered": True, "action": "rolled-back"}
    assert json.loads(CONFIG_FILE.read_text(encoding="utf-8"))["marker"] == "old"
    assert not restore.RESTORE_JOURNAL.exists()
    assert not stage_root.exists()


def test_startup_rollback_is_idempotent_after_old_file_was_restored(tmp_path):
    CONFIG_FILE.write_text(_config("old-restored"), encoding="utf-8")
    stage_root = APP_DATA_DIR.parent / (
        f"{restore._STAGE_PREFIX}{next(tempfile._get_candidate_names())}"
    )
    (stage_root / "rollback").mkdir(parents=True)
    record = {
        "version": 1,
        "state": "rolling_back",
        "stage_root": str(stage_root),
        "entries": [{
            "target": str(CONFIG_FILE),
            "staged": str(stage_root / "live" / CONFIG_FILE.name),
            "old_backup": str(stage_root / "rollback" / "old-config.json"),
            "had_old": True,
            "commit_state": "new_installed",
        }],
    }
    restore._write_journal(record)

    result = restore.recover_interrupted_restore()

    assert result == {"ok": True, "recovered": True, "action": "rolled-back"}
    assert json.loads(CONFIG_FILE.read_text(encoding="utf-8"))["marker"] == "old-restored"
    assert not restore.RESTORE_JOURNAL.exists()


def test_invalid_sqlite_is_rejected_without_replacing_live_database(tmp_path):
    CONFIG_FILE.write_text(_config("old"), encoding="utf-8")
    TRANSCRIPTION_DB.write_bytes(b"old-live-db")
    archive = tmp_path / "bad-db.zip"
    _write_zip(
        archive,
        {
            CONFIG_FILE.name: _config("new"),
            TRANSCRIPTION_DB.name: b"definitely not sqlite",
        },
    )

    result = restore.restore_backup(archive)

    assert not result["ok"]
    assert "database" in result["error"].lower()
    assert TRANSCRIPTION_DB.read_bytes() == b"old-live-db"
