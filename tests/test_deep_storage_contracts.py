"""Live SQLite, format admission, calendar and scan evidence boundaries."""

import json
import sqlite3
import threading
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

from backend import archive_scan, deps_installer, index, index_graph, index_search
from backend.archive_calendar import calendar_bucket, calendar_sql, upload_date_epoch
from backend.catalog_repository import install_catalog_schema
from backend.integrity_scan import scan_integrity
from backend.services import channel_transactions, restore_coordinator
from backend.services.format_versions import UnsupportedFormatError
from backend.services.sqlite_reads import open_readonly
from backend.youtube_session import check_cookie_source


def test_live_reader_sees_committed_wal_and_keeps_one_snapshot(tmp_path):
    path = tmp_path / "live.sqlite"
    writer = sqlite3.connect(path)
    try:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE records(value)")
        writer.execute("INSERT INTO records VALUES(1)")
        writer.commit()
        reader = open_readonly(path)
        try:
            assert reader.execute("SELECT value FROM records").fetchall() == [(1,)]
            writer.execute("INSERT INTO records VALUES(2)")
            writer.commit()
            assert reader.execute("SELECT value FROM records").fetchall() == [(1,)]
            with pytest.raises(sqlite3.OperationalError):
                reader.execute("DELETE FROM records")
        finally:
            reader.close()
        with open_readonly(path) as current:
            assert current.execute("SELECT count(*) FROM records").fetchone()[0] == 2
        current.close()
    finally:
        writer.close()


def test_public_integrity_scan_includes_uncheckpointed_video(tmp_path):
    archive = tmp_path / "Archive"
    archive.mkdir()
    media = archive / "One [ABCDEFGHIJK].mp4"
    media.write_bytes(b"media")
    config, queue = tmp_path / "config.json", tmp_path / "queue.json"
    config.write_text('{}')
    queue.write_text('{}')
    database = tmp_path / "index.sqlite"
    writer = sqlite3.connect(database)
    try:
        writer.execute("CREATE TABLE videos(id INTEGER PRIMARY KEY,title,channel,filepath,video_id)")
        writer.commit()
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("INSERT INTO videos VALUES(1,'One','Channel',?,'ABCDEFGHIJK')", (str(media),))
        writer.commit()
        result = scan_integrity(archive_path=archive, config_path=config,
                                db_path=database, queue_path=queue)
        assert result["summary"]["videos_rows_seen"] == 1
    finally:
        writer.close()


def test_cookie_probe_reads_live_wal_and_unknown_is_not_signed_out(tmp_path, monkeypatch):
    monkeypatch.setenv("APPDATA", str(tmp_path))
    profile = tmp_path / "Mozilla" / "Firefox" / "Profiles" / "synthetic"
    profile.mkdir(parents=True)
    path = profile / "cookies.sqlite"
    writer = sqlite3.connect(path)
    try:
        writer.execute("CREATE TABLE moz_cookies(host,name,expiry)")
        writer.commit()
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("INSERT INTO moz_cookies VALUES('.youtube.com','SID',0)")
        writer.commit()
        assert deps_installer.firefox_cookie_status()["signed_in"]
    finally:
        writer.close()
    path.write_bytes(b"unreadable SQLite input")
    status = deps_installer.firefox_cookie_status()
    assert status["check_available"] is False
    alert = Mock()
    monkeypatch.setattr("backend.youtube_session.trigger_cookie_alert", alert)
    assert check_cookie_source(["--cookies-from-browser", "firefox"])
    alert.assert_not_called()


def test_future_database_is_unchanged_when_public_index_open_refuses(tmp_path, monkeypatch):
    database = tmp_path / "future.sqlite"
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("CREATE TABLE future_only(value)")
        connection.execute("PRAGMA user_version=99")
    before = database.read_bytes()
    monkeypatch.setattr(index, "TRANSCRIPTION_DB", database)
    monkeypatch.setattr(index, "_conn", None)
    monkeypatch.setattr(index, "_schema_inited", False)
    assert index._open() is None
    assert database.read_bytes() == before
    assert not Path(str(database) + "-wal").exists()


def test_future_projection_rejected_before_any_schema_changes(tmp_path):
    with closing(sqlite3.connect(tmp_path / "future-catalog.sqlite")) as connection:
        connection.execute("CREATE TABLE catalog_state(singleton,schema_version)")
        connection.execute("INSERT INTO catalog_state VALUES(1,99)")
        connection.commit()
        before = list(connection.iterdump())
        with pytest.raises(UnsupportedFormatError):
            install_catalog_schema(connection)
        assert list(connection.iterdump()) == before


@pytest.mark.parametrize("kind", ["channel", "restore"])
def test_future_recovery_journal_is_preserved_without_replay(tmp_path, monkeypatch, kind):
    path = tmp_path / "journal.json"
    path.write_text(json.dumps({"version": 99, "state": "committed"}))
    before = path.read_bytes()
    if kind == "channel":
        monkeypatch.setattr(channel_transactions, "CHANNEL_TRANSACTION_FILE", path)
        result = channel_transactions.recover_channel_transaction()
    else:
        monkeypatch.setattr(restore_coordinator, "RESTORE_JOURNAL", path)
        result = restore_coordinator.recover_interrupted_restore()
    assert not result["ok"] and result["recovery_required"]
    assert "unsupported format" in result["error"]
    assert path.read_bytes() == before


@pytest.mark.parametrize("version", [0, 1, 2, 3])
def test_supported_legacy_channel_journal_decodes(tmp_path, monkeypatch, version):
    path = tmp_path / "journal.json"
    path.write_text(json.dumps({"version": version, "operation": "rename"}))
    monkeypatch.setattr(channel_transactions, "CHANNEL_TRANSACTION_FILE", path)
    assert channel_transactions.load_channel_transaction(strict=True)["version"] == version


@pytest.mark.parametrize("instant,year,month,week", [
    ("2024-12-31T23:59:59+00:00", "2024", "2024-12", "2025-W01"),
    ("2025-01-01T00:00:00+00:00", "2025", "2025-01", "2025-W01"),
    ("2026-01-01T00:15:00+14:00", "2025", "2025-12", "2026-W01"),
])
def test_search_graph_and_python_share_utc_calendar(instant, year, month, week):
    timestamp = datetime.fromisoformat(instant).timestamp()
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE v(logical_upload_ts)")
        connection.execute("INSERT INTO v VALUES(?)", (timestamp,))
        connection.execute("CREATE TABLE s(year,month)")
        connection.execute("INSERT INTO s VALUES(NULL,NULL)")
        for bucket, expected in (("year", year), ("month", month)):
            expr = index_graph._calendar_bucket_expr(bucket)
            assert connection.execute(f"SELECT {expr} FROM v,s").fetchone()[0] == expected
            assert calendar_bucket(timestamp, bucket) == expected
        assert calendar_bucket(timestamp, "week") == week
        assert index_search._year_start_ts(int(year)) <= timestamp
        assert timestamp < index_search._year_start_ts(int(year) + 1)


def test_date_only_upload_day_is_preserved():
    timestamp = upload_date_epoch("20261231")
    assert datetime.fromtimestamp(timestamp, UTC).hour == 12
    assert index._upload_date_to_epoch("20261231") == timestamp
    with closing(sqlite3.connect(":memory:")) as connection:
        assert connection.execute(f"SELECT {calendar_sql('?', 'day')}", (timestamp,)).fetchone()[0] == "2026-12-31"


@pytest.mark.parametrize("failure", ["missing", "walk", "stat", "cancel"])
def test_incomplete_scan_preserves_authoritative_counts(tmp_path, monkeypatch, failure):
    channel = {"name": "Channel", "url": "test-channel"}
    folder = tmp_path / "Channel"
    if failure != "missing":
        folder.mkdir()
        (folder / "Video.mp4").write_bytes(b"video")
    old = {"num_vids": 20, "physical_copies": 20, "size_bytes": 400,
           "last_updated": 100, "count_semantics_version": 2}
    monkeypatch.setattr(archive_scan, "load_config", lambda: {
        "output_dir": str(tmp_path), "channels": [channel]})
    monkeypatch.setattr(index, "_reader_open", lambda: None)
    monkeypatch.setattr(archive_scan, "load_disk_cache", lambda: {channel["url"]: dict(old)})
    saved = []
    monkeypatch.setattr(archive_scan, "save_disk_cache", lambda value: saved.append(value) or True)
    if failure == "walk":
        def walk(_root, *, onerror):
            onerror(PermissionError("unreadable subtree"))
            yield str(folder), [], ["Video.mp4"]
        monkeypatch.setattr(archive_scan.os, "walk", walk)
    elif failure == "stat":
        monkeypatch.setattr(archive_scan.os.path, "getsize", Mock(side_effect=OSError("unreadable file")))
    if failure == "cancel":
        cancelled = threading.Event()
        cancelled.set()
        assert archive_scan.scan_all_channels(stop_if=cancelled.is_set) is None
        assert not saved
        return
    result = archive_scan.scan_all_channels()
    record = result[channel["url"]]
    assert record["scan_complete"] is False
    assert record["num_vids"] == 20 and record["last_updated"] == 100
    published = archive_scan.publish_scan_stats(result)
    assert published[channel["url"]]["size_bytes"] == 400
    assert not archive_scan.cache_coverage([channel], published)["complete"]


def test_existing_empty_channel_is_a_complete_zero_scan(tmp_path, monkeypatch):
    (tmp_path / "Empty").mkdir()
    monkeypatch.setattr(index, "_reader_open", lambda: None)
    result = archive_scan.scan_channel_folder(tmp_path, {"name": "Empty"}, with_status=True)
    assert result.complete and result.n_vids == result.size_bytes == 0
