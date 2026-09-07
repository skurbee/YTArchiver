"""Large-database restore policy exercised with small, isolated SQLite files."""

import hashlib
import json
import sqlite3
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def restore_files(tmp_path, monkeypatch):
    # Root conftest isolates both profile directories before application
    # imports. Every target below is additionally confined to this test.
    from backend.services import restore_coordinator as restore

    app_data = tmp_path / "profile" / "YTArchiver"
    app_data.mkdir(parents=True)
    config = app_data / "config.json"
    cache = app_data / "cache.json"
    database = app_data / "index.db"
    monkeypatch.setattr(restore, "APP_DATA_DIR", app_data)
    monkeypatch.setattr(restore, "CONFIG_FILE", config)
    monkeypatch.setattr(restore, "TRANSCRIPTION_DB", database)
    monkeypatch.setattr(restore, "RESTORE_JOURNAL", app_data / "restore_transaction.json")
    monkeypatch.setattr(
        restore, "backup_file_entries", lambda: ((config.name, config), (cache.name, cache))
    )
    config.write_text(json.dumps({"channels": [], "marker": "original"}), encoding="utf-8")
    database.write_bytes(b"original database must survive a rejected restore")
    return SimpleNamespace(
        restore=restore, app_data=app_data, config=config, cache=cache,
        database=database, temporary=tmp_path,
    )


def _database_payload(path, *, empty_bytes=0, page_size=None):
    connection = sqlite3.connect(path)
    try:
        if page_size is not None:
            connection.execute(f"PRAGMA page_size={int(page_size)}")
        connection.execute("CREATE TABLE sample (value TEXT, padding BLOB)")
        connection.execute(
            "INSERT INTO sample VALUES (?, zeroblob(?))", ("restored", empty_bytes)
        )
        connection.commit()
    finally:
        connection.close()
    return path.read_bytes()


def _write_backup(files, *, database=None, extras=None, corrupt_hash=False):
    if database is None:
        database = _database_payload(files.temporary / "source.db")
    members = {
        files.config.name: json.dumps({"channels": [], "marker": "restored"}).encode(),
        files.database.name: database,
    }
    members.update(extras or {})
    resources = {
        name: {"sha256": hashlib.sha256(value).hexdigest(), "size": len(value)}
        for name, value in members.items()
    }
    if corrupt_hash:
        resources[files.database.name]["sha256"] = "0" * 64
    members[files.restore.BACKUP_MANIFEST_NAME] = json.dumps({
        "manifest_version": 2,
        "app": "YTArchiver",
        "backup_type": "app-state",
        "resources": resources,
    }).encode()
    archive = files.temporary / "backup.zip"
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zipped:
        for name, value in members.items():
            # Exercise ZIP64 headers without creating a multi-gigabyte fixture.
            with zipped.open(name, "w", force_zip64=True) as destination:
                destination.write(value)
    return archive


def _limits(files, **overrides):
    values = {"max_entry_bytes": 2048, "max_total_bytes": 4096, "min_free_bytes": 0}
    values.update(overrides)
    return files.restore.RestoreLimits(**values)


def _assert_original(files):
    assert json.loads(files.config.read_text(encoding="utf-8"))["marker"] == "original"
    assert files.database.read_bytes() == b"original database must survive a rejected restore"
    assert not files.restore.RESTORE_JOURNAL.exists()
    assert not list(files.app_data.parent.glob(".ytarchiver-restore-stage-*"))


def test_database_above_metadata_limits_round_trips_through_zip64(restore_files):
    files = restore_files
    archive = _write_backup(files)
    limits = _limits(files)
    with zipfile.ZipFile(archive) as zipped:
        database_info = zipped.getinfo(files.database.name)
        assert database_info.file_size > limits.max_entry_bytes
        assert database_info.file_size > limits.max_total_bytes
        assert database_info.extract_version >= 45

    result = files.restore.restore_backup(archive, limits=limits)

    assert result["ok"], result
    assert files.database.name in result["restored"]
    assert json.loads(files.config.read_text(encoding="utf-8"))["marker"] == "restored"
    connection = sqlite3.connect(files.database)
    try:
        assert connection.execute("SELECT value FROM sample").fetchone() == ("restored",)
    finally:
        connection.close()
    assert not files.restore.RESTORE_JOURNAL.exists()


@pytest.mark.parametrize(
    ("limits", "padding", "expected"),
    [
        ({"max_entry_bytes": 1024}, 2048, "member is too large: config.json"),
        ({"max_entry_bytes": 4096, "max_total_bytes": 1024}, 1200, "total-size limit"),
    ],
)
def test_ordinary_resource_size_caps_remain(restore_files, limits, padding, expected):
    files = restore_files
    archive = _write_backup(files, extras={
        files.config.name: json.dumps({"channels": [], "padding": "x" * padding}).encode()
    })

    result = files.restore.restore_backup(archive, limits=_limits(files, **limits))

    assert not result["ok"]
    assert expected in result["error"]
    _assert_original(files)


def test_nested_database_name_does_not_receive_exemption(restore_files):
    files = restore_files
    archive = _write_backup(files, extras={"nested/index.db": b"x" * 4096})

    result = files.restore.restore_backup(archive, limits=_limits(files))

    assert not result["ok"]
    assert "member is too large: nested/index.db" in result["error"]
    _assert_original(files)


def test_free_space_requirement_counts_entire_database(restore_files, monkeypatch):
    files = restore_files
    archive = _write_backup(files)
    with zipfile.ZipFile(archive) as zipped:
        expanded_bytes = sum(info.file_size for info in zipped.infolist())
    required = expanded_bytes + expanded_bytes // 10
    checked_paths = []

    def disk_usage(path):
        checked_paths.append(Path(path))
        return SimpleNamespace(free=required - 1)

    monkeypatch.setattr(files.restore.shutil, "disk_usage", disk_usage)
    result = files.restore.restore_backup(archive, limits=_limits(files))

    assert not result["ok"]
    assert "Not enough free space" in result["error"]
    assert f"{required} bytes required" in result["error"]
    assert checked_paths == [files.app_data.parent]
    _assert_original(files)


def test_compressible_database_is_valid_despite_metadata_ratio_cap(restore_files):
    files = restore_files
    # Larger SQLite pages reduce overflow-page overhead enough to exceed the
    # production 500:1 limit consistently, without a large fixture on disk.
    payload = _database_payload(
        files.temporary / "source.db", empty_bytes=2 * 1024 * 1024, page_size=65536
    )
    archive = _write_backup(files, database=payload)
    limits = _limits(files)
    with zipfile.ZipFile(archive) as zipped:
        info = zipped.getinfo(files.database.name)
        assert info.file_size / info.compress_size > limits.max_compression_ratio

    result = files.restore.restore_backup(archive, limits=limits)

    assert result["ok"], result
    assert files.database.read_bytes() == payload


def test_ordinary_resource_compression_ratio_cap_remains(restore_files):
    files = restore_files
    archive = _write_backup(files, extras={
        files.cache.name: json.dumps({"padding": "x" * 4096}).encode()
    })
    limits = _limits(files, max_entry_bytes=8192, max_total_bytes=16384,
                     max_compression_ratio=10)

    result = files.restore.restore_backup(archive, limits=limits)

    assert not result["ok"]
    assert "compression ratio is unsafe: cache.json" in result["error"]
    _assert_original(files)


def test_large_database_still_requires_matching_manifest_hash(restore_files):
    files = restore_files
    archive = _write_backup(files, corrupt_hash=True)

    result = files.restore.restore_backup(archive, limits=_limits(files))

    assert not result["ok"]
    assert "Checksum mismatch" in result["error"]
    _assert_original(files)


def test_large_database_still_requires_valid_sqlite(restore_files):
    files = restore_files
    archive = _write_backup(files, database=b"invalid SQLite content" * 512)

    result = files.restore.restore_backup(archive, limits=_limits(files))

    assert not result["ok"]
    assert "Restored search database is invalid" in result["error"]
    _assert_original(files)
