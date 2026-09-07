"""Exercise real SQLite/ZIP exports using only disposable application state."""

import hashlib
import json
import sqlite3
import threading
import zipfile
from contextlib import closing, contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def export_state(tmp_path, monkeypatch):
    # conftest isolates APPDATA/LOCALAPPDATA before these imports.
    from backend import auto_backup
    from backend.api_mixins import backup_mixin

    profile = tmp_path / "profile"
    profile.mkdir()
    destination = tmp_path / "backups"
    destination.mkdir()
    database = profile / "transcription_index.db"
    config_file = profile / "config.json"
    config = {"channels": []}
    config_file.write_text(json.dumps(config), encoding="utf-8")
    auto_backup.write_bookmark_database(database, {"version": 1, "bookmarks": []})
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("CREATE TABLE search_fixture (text TEXT)")
        connection.execute("INSERT INTO search_fixture VALUES ('searchable text')")
        connection.commit()
    monkeypatch.setattr(auto_backup, "APP_DATA_DIR", profile)
    monkeypatch.setattr(auto_backup, "CONFIG_FILE", config_file)
    monkeypatch.setattr(auto_backup, "QUEUE_FILE", profile / "queue.json")
    monkeypatch.setattr(auto_backup, "TRANSCRIPTION_DB", database)
    monkeypatch.setattr(auto_backup, "load_config", lambda: dict(config))
    monkeypatch.setattr(auto_backup, "backup_file_entries", lambda: (
        (config_file.name, config_file),))
    monkeypatch.setattr(auto_backup, "refresh_info_folder", lambda _root: str(destination))

    @contextmanager
    def config_transaction():
        yield config

    def update_config(mutator):
        return mutator(config), dict(config)

    monkeypatch.setattr(auto_backup, "config_transaction", config_transaction)
    monkeypatch.setattr(backup_mixin, "update_config", update_config)
    return SimpleNamespace(
        backup=auto_backup, api=backup_mixin, database=database,
        config=config, destination=destination, archive=destination / "manual.zip")


@pytest.mark.parametrize("entrypoint", ["manual", "automatic"])
@pytest.mark.parametrize("include", [False, True])
def test_manual_and_automatic_exports_honor_saved_preference(
        export_state, entrypoint, include):
    state = export_state
    state.config["backup_include_search_db"] = include
    if entrypoint == "manual":
        from backend.services.job_supervisor import JobSupervisor

        class Window:
            def create_file_dialog(self, *_args, **_kwargs):
                return [str(state.archive)]

        class Api(state.api.BackupMixin):
            _window = Window()
            _job_supervisor = JobSupervisor()

        result = Api().export_full_backup()
    else:
        result = state.backup.run_backup(str(state.destination))
    assert result["ok"], result
    with zipfile.ZipFile(result["path"]) as archive:
        manifest = json.loads(archive.read(state.backup.BACKUP_MANIFEST_NAME))
        assert (state.database.name in archive.namelist()) is include
        assert manifest["fts_db_included"] is include
        assert state.backup.BOOKMARK_BACKUP_NAME in archive.namelist()
        if not include:
            assert "turned off" in manifest["fts_skipped_reason"]
            assert "2 GB" not in manifest["fts_skipped_reason"]


def test_default_export_does_not_skip_database_above_old_size_limit(
        export_state, monkeypatch):
    state = export_state

    class LargeMeasuredDatabase:
        name = state.database.name

        def exists(self):
            return True

        def stat(self):
            return SimpleNamespace(st_size=36 * 1024 ** 3)

        def resolve(self):
            return state.database.resolve()

    monkeypatch.setattr(state.backup, "TRANSCRIPTION_DB", LargeMeasuredDatabase())
    result = state.backup.build_backup_zip(str(state.archive))
    assert result["fts_included"]
    assert result["fts_skipped_reason"] == ""
    with zipfile.ZipFile(state.archive) as archive:
        saved = archive.read(state.database.name)
        assert saved.startswith(b"SQLite format 3")
        manifest = json.loads(archive.read(state.backup.BACKUP_MANIFEST_NAME))
        # Size and hash describe the actual coherent snapshot, including WAL.
        assert manifest["fts_db_size"] == len(saved)
        assert manifest["resources"][state.database.name] == {
            "size": len(saved), "sha256": hashlib.sha256(saved).hexdigest()}


def test_snapshot_includes_committed_wal_and_supports_zip64(
        export_state, tmp_path, monkeypatch):
    state = export_state
    monkeypatch.setattr(zipfile, "ZIP64_LIMIT", 1024)
    with closing(sqlite3.connect(state.database)) as writer:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("INSERT INTO search_fixture VALUES (?)", ("WAL content" * 10000,))
        writer.commit()
        assert Path(str(state.database) + "-wal").stat().st_size > 0
        result = state.backup.build_backup_zip(str(state.archive))
    with zipfile.ZipFile(state.archive) as archive:
        assert archive.getinfo(state.database.name).extract_version >= 45
        saved_path = tmp_path / "saved.db"
        saved_path.write_bytes(archive.read(state.database.name))
    with closing(sqlite3.connect(saved_path)) as snapshot:
        assert snapshot.execute("SELECT count(*) FROM search_fixture").fetchone()[0] == 2
        assert snapshot.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert snapshot.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert result["fts_size"] == saved_path.stat().st_size
    assert not list(state.destination.glob(".ytarchiver-backup-*"))


def test_wal_writer_can_commit_during_copy_without_restarting_snapshot(
        export_state, tmp_path, monkeypatch):
    state = export_state
    real_connect = sqlite3.connect
    copied_steps = []
    with closing(real_connect(state.database)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE padding (data BLOB)")
        writer.execute("INSERT INTO padding VALUES (zeroblob(?))", (8 * 1024 * 1024,))
        writer.commit()

        class BackupSource(sqlite3.Connection):
            def backup(self, destination, *, pages, progress, sleep):
                def record_and_write(status, remaining, total):
                    progress(status, remaining, total)
                    # Guard the regression test itself against an endless retry.
                    assert len(copied_steps) < 10
                    copied_steps.append(remaining)
                    if len(copied_steps) == 1:
                        assert remaining > 0
                        writer.execute("INSERT INTO search_fixture VALUES ('written during backup')")
                        writer.commit()
                return super().backup(destination, pages=pages,
                                      progress=record_and_write, sleep=sleep)

        def source_connection(path, *args, **kwargs):
            if kwargs.get("uri") and str(path).endswith("?mode=ro"):
                kwargs["factory"] = BackupSource
            return real_connect(path, *args, **kwargs)

        monkeypatch.setattr(sqlite3, "connect", source_connection)
        state.backup.build_backup_zip(str(state.archive))
        assert writer.execute("SELECT count(*) FROM search_fixture").fetchone()[0] == 2
    assert copied_steps[-1] == 0
    assert all(a > b for a, b in zip(copied_steps, copied_steps[1:], strict=False))
    with zipfile.ZipFile(state.archive) as archive:
        saved_path = tmp_path / "concurrent.db"
        saved_path.write_bytes(archive.read(state.database.name))
    with closing(real_connect(saved_path)) as snapshot:
        assert snapshot.execute("SELECT text FROM search_fixture").fetchall() == [("searchable text",)]
    assert not list(state.destination.glob(".ytarchiver-backup-*"))


@pytest.mark.parametrize("failure", ["disk_full", "cancelled"])
def test_snapshot_failure_preserves_previous_backup_and_removes_temporary_files(
        export_state, monkeypatch, failure):
    state = export_state
    previous = b"previous valid backup placeholder"
    state.archive.write_bytes(previous)
    before = state.database.read_bytes()
    real_write = state.backup._write_zip_path_resource
    cancel = threading.Event()
    snapshots = []

    def fail_database(zipped, source, arcname, cancel_event=None):
        if arcname == state.database.name:
            snapshots.append(Path(source))
            assert Path(source).parent == state.destination
            assert Path(source).is_file()
            if failure == "disk_full":
                raise OSError("No space left on device")
            cancel.set()
        return real_write(zipped, source, arcname, cancel_event)

    monkeypatch.setattr(state.backup, "_write_zip_path_resource", fail_database)
    expected = OSError if failure == "disk_full" else state.backup.BackupCancelled
    with pytest.raises(expected):
        state.backup.build_backup_zip(str(state.archive), cancel_event=cancel)
    assert snapshots and all(not path.exists() for path in snapshots)
    assert state.archive.read_bytes() == previous
    assert state.database.read_bytes() == before
    assert not Path(str(state.archive) + ".tmp").exists()


def test_automatic_export_failure_does_not_rotate_existing_backups(
        export_state, monkeypatch):
    state = export_state
    def fail_build(*_args, **_kwargs):
        raise OSError("No space left on device")

    monkeypatch.setattr(state.backup, "build_backup_zip", fail_build)
    monkeypatch.setattr(state.backup, "_rotate_backups", lambda *_args: pytest.fail(
        "A failed backup must not discard older backups"))
    result = state.backup.run_backup(str(state.destination))
    assert not result["ok"]
    assert "last_backup_ts" not in state.config
