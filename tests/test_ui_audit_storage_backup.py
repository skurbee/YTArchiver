"""Restore and backup regressions using disposable application state only."""
import atexit
import json
import os
import sqlite3
import tempfile
import zipfile
from contextlib import closing
from pathlib import Path
from unittest import mock

import pytest

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-storage-backup-")
atexit.register(_PROFILE.cleanup)
os.environ["APPDATA"] = str(Path(_PROFILE.name) / "roaming")
os.environ["LOCALAPPDATA"] = str(Path(_PROFILE.name) / "local")

from backend import auto_backup
from backend.services import restore_coordinator as restore


@pytest.fixture
def state(tmp_path, monkeypatch):
    profile = tmp_path / "profile"
    profile.mkdir()
    config = profile / "ytarchiver_config.json"
    database = profile / "transcription_index.db"
    queue = profile / "ytarchiver_queue.json"
    for module in (auto_backup, restore):
        monkeypatch.setattr(module, "APP_DATA_DIR", profile)
        monkeypatch.setattr(module, "CONFIG_FILE", config)
        monkeypatch.setattr(module, "TRANSCRIPTION_DB", database)
        monkeypatch.setattr(module, "backup_file_entries", lambda: (
            (config.name, config), (queue.name, queue)))
    monkeypatch.setattr(auto_backup, "QUEUE_FILE", queue)
    monkeypatch.setattr(restore, "RESTORE_JOURNAL", profile / "restore_transaction.json")
    config.write_text(json.dumps({"channels": [], "marker": "original"}), encoding="utf-8")
    return profile, config, database, queue


def bookmark_payload(note="authored note"):
    return {"version": 1, "bookmarks": [{
        "id": 7, "segment_id": 99, "video_id": "abcdefghij1", "title": "Example",
        "channel": "Fixture", "start_time": 42.5, "text": "selected words",
        "note": note, "created": 123456.0,
    }]}


def seed_database(database, note="authored note"):
    auto_backup.write_bookmark_database(database, bookmark_payload(note))
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("CREATE TABLE obsolete_catalog (value TEXT)")
        connection.execute("INSERT INTO obsolete_catalog VALUES ('old catalog')")
        connection.commit()


def legacy_zip(path, config, queue=None):
    with zipfile.ZipFile(path, "w") as zipped:
        zipped.writestr(config.name, json.dumps({"channels": [], "marker": "restored"}))
        if queue:
            zipped.writestr(queue.name, json.dumps({"sync": [], "gpu": [], "marker": "new"}))


def test_failed_rollback_retains_original_and_next_start_recovers(state, tmp_path):
    _profile, config, _database, queue = state
    original = json.dumps({"sync": [], "gpu": [], "marker": "original queue"})
    queue.write_text(original, encoding="utf-8")
    archive = tmp_path / "restore.zip"
    legacy_zip(archive, config, queue)
    real_replace = os.replace

    def deny_queue_destination(source, target):
        if Path(target) == queue:
            raise OSError("transient destination failure")
        return real_replace(source, target)

    with mock.patch.object(restore.os, "replace", side_effect=deny_queue_destination):
        result = restore.restore_backup(archive)
    assert result["recovery_required"]
    journal = json.loads(restore.RESTORE_JOURNAL.read_text())
    assert journal["state"] == "rollback_failed"
    assert Path(journal["stage_root"]).is_dir()
    old = next(Path(entry["old_backup"]) for entry in journal["entries"]
               if entry["target"] == str(queue))
    assert old.read_text() == original
    recovered = restore.recover_interrupted_restore()
    assert recovered["ok"]
    assert queue.read_text() == original
    assert not restore.RESTORE_JOURNAL.exists()


def test_large_index_backup_round_trip_preserves_authored_state(state, tmp_path, monkeypatch):
    _profile, _config, database, _queue = state
    seed_database(database)
    archive = tmp_path / "large-library.zip"
    monkeypatch.setattr(auto_backup, "_FTS_ZIP_CAP", 1)
    exported = auto_backup.build_backup_zip(str(archive))
    assert not exported["fts_included"]
    assert exported["bookmarks_included"] and exported["bookmark_count"] == 1
    with zipfile.ZipFile(archive) as zipped:
        assert database.name not in zipped.namelist()
        assert auto_backup.BOOKMARK_BACKUP_NAME in zipped.namelist()
        saved = json.loads(zipped.read(auto_backup.BOOKMARK_BACKUP_NAME))
        assert saved["bookmarks"][0]["note"] == "authored note"
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("UPDATE bookmarks SET note='later note'")
        connection.commit()
    result = restore.restore_backup(archive)
    assert result["ok"], result
    assert result["bookmarks_source"] == "backup"
    assert result["bookmark_count"] == 1
    saved = auto_backup.read_bookmark_backup(database)["bookmarks"][0]
    assert saved["note"] == "authored note"
    assert saved["start_time"] == 42.5
    assert saved["segment_id"] is None
    with closing(sqlite3.connect(database)) as connection:
        assert not connection.execute("SELECT 1 FROM sqlite_master WHERE name='obsolete_catalog'").fetchone()


def test_legacy_restore_retains_only_current_bookmarks(state, tmp_path):
    _profile, config, database, _queue = state
    seed_database(database, "retain me")
    archive = tmp_path / "legacy.zip"
    legacy_zip(archive, config)
    result = restore.restore_backup(archive)
    assert result["ok"], result
    assert result["bookmarks_source"] == "current_installation"
    assert auto_backup.read_bookmark_backup(database)["bookmarks"][0]["note"] == "retain me"
    with closing(sqlite3.connect(database)) as connection:
        assert not connection.execute("SELECT 1 FROM sqlite_master WHERE name='obsolete_catalog'").fetchone()


def test_legacy_restore_cannot_destroy_unreadable_unique_state(state, tmp_path):
    _profile, config, database, _queue = state
    database.write_bytes(b"unreadable database")
    before = config.read_bytes()
    archive = tmp_path / "legacy.zip"
    legacy_zip(archive, config)
    result = restore.restore_backup(archive)
    assert not result["ok"]
    assert "could not be preserved" in result["error"]
    assert config.read_bytes() == before
    assert database.read_bytes() == b"unreadable database"


def test_malformed_bookmark_resource_rejected_before_mutation(state, tmp_path):
    _profile, config, database, _queue = state
    seed_database(database)
    before = (config.read_bytes(), database.read_bytes())
    archive = tmp_path / "bad.zip"
    legacy_zip(archive, config)
    with zipfile.ZipFile(archive, "a") as zipped:
        zipped.writestr(auto_backup.BOOKMARK_BACKUP_NAME,
                        json.dumps({"version": 1, "bookmarks": [{"id": 1}]}))
    result = restore.restore_backup(archive)
    assert not result["ok"]
    assert (config.read_bytes(), database.read_bytes()) == before


def test_full_database_backup_has_matching_bookmark_resource(state, tmp_path):
    _profile, _config, database, _queue = state
    seed_database(database)
    archive = tmp_path / "full.zip"
    result = auto_backup.build_backup_zip(str(archive))
    assert result["fts_included"]
    staged = restore.stage_backup(archive)
    try:
        expected = auto_backup.read_bookmark_backup(staged.included[database.name])
        assert json.loads(staged.included[auto_backup.BOOKMARK_BACKUP_NAME].read_text()) == expected
    finally:
        import shutil
        assert staged.root.resolve().is_relative_to(tmp_path.resolve())
        shutil.rmtree(staged.root)


def test_bookmark_seed_survives_normal_index_initialization(state, tmp_path, monkeypatch):
    from backend import index
    _profile, _config, database, _queue = state
    seed_database(database)
    monkeypatch.setattr(auto_backup, "_FTS_ZIP_CAP", 1)
    archive = tmp_path / "seed.zip"
    auto_backup.build_backup_zip(str(archive))
    assert restore.restore_backup(archive)["ok"]
    index._shutdown_index()
    index._schema_inited = False
    monkeypatch.setattr(index, "TRANSCRIPTION_DB", database)
    try:
        connection = index._open()
        assert connection is not None
        assert connection.execute("SELECT note,start_time,segment_id FROM bookmarks").fetchone() == (
            "authored note", 42.5, None)
        assert connection.execute("SELECT COUNT(*) FROM segments").fetchone()[0] == 0
    finally:
        index._shutdown_index()
        index._schema_inited = False
