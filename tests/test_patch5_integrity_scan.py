"""Disposable-path tests for Patch 5's preview-only integrity scanner."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest import mock

from backend import activity_history, ytarchiver_config
from backend.api_mixins import diagnostics_mixin
from backend.integrity_scan import scan_integrity

VIDEO_ID = "abcdefghijk"


def _write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def _create_database(path: Path, media_path: Path, *, broken: bool = False) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE videos (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            channel TEXT NOT NULL,
            filepath TEXT,
            video_id TEXT,
            is_duplicate_of TEXT,
            availability TEXT DEFAULT 'available',
            downloaded_ts REAL
        );
        CREATE TABLE segments (
            id INTEGER PRIMARY KEY,
            video_id TEXT,
            title TEXT NOT NULL,
            channel TEXT NOT NULL,
            text TEXT NOT NULL
        );
        CREATE VIRTUAL TABLE videos_fts USING fts5(
            title, content=videos, content_rowid=id
        );
        CREATE VIRTUAL TABLE segments_fts USING fts5(
            text, content=segments, content_rowid=id
        );
        CREATE TABLE catalog_state (
            singleton INTEGER PRIMARY KEY,
            phase TEXT NOT NULL,
            schema_version INTEGER NOT NULL,
            legacy_dirty INTEGER NOT NULL,
            last_error TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE catalog_dirty_keys (identity_key TEXT PRIMARY KEY);
        CREATE TABLE logical_videos (
            logical_id INTEGER PRIMARY KEY,
            video_id TEXT,
            title TEXT NOT NULL,
            channel TEXT NOT NULL
        );
        CREATE TABLE media_files (
            media_id INTEGER PRIMARY KEY,
            logical_video_id INTEGER,
            filepath TEXT
        );
        CREATE VIRTUAL TABLE logical_videos_fts USING fts5(
            title, content=logical_videos, content_rowid=logical_id
        );
        """
    )
    connection.execute(
        "INSERT INTO videos VALUES(1,'One','Channel',?,?,NULL,'available',100)",
        (str(media_path), VIDEO_ID),
    )
    connection.execute(
        "INSERT INTO segments VALUES(1,?,'One','Channel','spoken words')",
        (VIDEO_ID,),
    )
    connection.execute(
        "INSERT INTO logical_videos VALUES(1,?,'One','Channel')", (VIDEO_ID,))
    connection.execute(
        "INSERT INTO media_files VALUES(1,1,?)", (str(media_path),))
    connection.execute(
        "INSERT INTO catalog_state VALUES(1,'v2_writes',1,0,'')")
    for table in ("videos_fts", "segments_fts", "logical_videos_fts"):
        connection.execute(f"INSERT INTO {table}({table}) VALUES('rebuild')")
    connection.execute(f"PRAGMA user_version={4 if broken else 5}")
    if broken:
        # The scanner reads the raw FTS docsize shadow, so external-content
        # column fallback cannot conceal this missing indexed document.
        connection.execute("DELETE FROM videos_fts_docsize WHERE id=1")
        connection.execute("DELETE FROM segments_fts_docsize WHERE id=1")
        connection.execute(
            "UPDATE catalog_state SET phase='copied',legacy_dirty=1")
        connection.execute(
            "INSERT INTO catalog_dirty_keys VALUES('id:dirtyvideo1')")
        connection.execute(
            "INSERT INTO videos VALUES(2,'Collision','Channel',?,?,NULL,'available',100)",
            (str(media_path.with_name("missing-a.mp4")), "collision01"),
        )
        connection.execute(
            "INSERT INTO videos VALUES(3,'Collision','Channel',?,?,NULL,'available',100)",
            (str(media_path.with_name("missing-b.mp4")), "collision02"),
        )
        connection.execute(
            "INSERT INTO videos VALUES(4,'Duplicate','Channel',?,?,NULL,'available',100)",
            (str(media_path), "duplicate01"),
        )
        connection.execute(
            "INSERT INTO videos VALUES(5,'Duplicate copy','Channel',?,?,NULL,'available',100)",
            (str(media_path.with_name("missing-copy.mp4")), "duplicate01"),
        )
    connection.commit()
    connection.close()


def _fixture(tmp_path: Path, *, broken: bool = False) -> dict[str, Path]:
    archive = tmp_path / "archive"
    channel = archive / "Channel"
    channel.mkdir(parents=True)
    media = channel / f"One [{VIDEO_ID}].mp4"
    media.write_bytes(b"media")
    (channel / "Channel Transcript.txt").write_text(
        f"===({'Txt only' if broken else 'One'}), (01.01.2026), (0:01), "
        f"(WHISPER){'' if broken else f', (youtu.be/{VIDEO_ID})'}===\ntext\n",
        encoding="utf-8",
    )
    jsonl_title = "JSONL only" if broken else "One"
    jsonl_id = "jsonlonly01" if broken else VIDEO_ID
    (channel / ".Channel Transcript.jsonl").write_text(
        json.dumps({
            "video_id": jsonl_id,
            "title": jsonl_title,
            "text": "spoken words",
            "start": 0,
            "end": 1,
        }) + "\n",
        encoding="utf-8",
    )

    config = tmp_path / "config.json"
    config_value = {
        "_migration_v2_pending_tx_ids": not broken,
        "channels": ([{
            "name": "Channel",
            "folder_override": "Missing Override",
        }] if broken else []),
        "autorun_history": ["legacy activity"] if broken else [],
    }
    _write_json(config, config_value)
    activity = tmp_path / "activity.jsonl"
    if broken:
        activity.write_text(
            json.dumps({"id": "activity-1", "entry": "canonical activity"}) + "\n",
            encoding="utf-8",
        )

    queue = tmp_path / "queue.json"
    missing_queue_media = tmp_path / "missing-queue.mp4"
    _write_json(queue, {
        "_schema_version": 1 if broken else 3,
        "sync": [],
        "gpu": ([{
            "task_id": "queue-only",
            "kind": "transcribe",
            "path": str(missing_queue_media),
        }] if broken else []),
    })
    resuming = tmp_path / "queue_resuming.json"
    _write_json(resuming, {
        "_schema_version": 1 if broken else 2,
        "resuming": {},
    })
    journal = tmp_path / "pending.json"
    _write_json(journal, ([{
        "task_id": "journal-only",
        "kind": "transcribe",
        "path": str(tmp_path / "missing-journal.mp4"),
    }] if broken else []))

    download_archive = tmp_path / "download.txt"
    lines = [f"youtube {VIDEO_ID}\n"]
    if broken:
        lines.append("youtube archivebad1\n")
    download_archive.write_text("".join(lines), encoding="utf-8")
    database = tmp_path / "index.db"
    _create_database(database, media, broken=broken)
    return {
        "archive": archive,
        "config": config,
        "database": database,
        "queue": queue,
        "resuming": resuming,
        "journal": journal,
        "download": download_archive,
        "activity": activity,
    }


def _scan(paths: dict[str, Path]):
    return scan_integrity(
        archive_path=paths["archive"],
        config_path=paths["config"],
        db_path=paths["database"],
        queue_path=paths["queue"],
        download_archive_path=paths["download"],
        transcription_recovery_path=paths["journal"],
        activity_history_path=paths["activity"],
    )


def _snapshot(root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*")) if path.is_file()
    }


def test_healthy_explicit_fixture_has_no_repair_proposals(tmp_path):
    paths = _fixture(tmp_path)

    result = _scan(paths)

    assert result["ok"] is True
    assert result["healthy"] is True
    assert result["preview_only"] is True
    assert result["repairs_applied"] is False
    assert result["repair_available"] is False
    assert result["verified_backup_required_before_repair"] is True
    assert result["issues"] == []
    assert result["checks"]["fts"]["videos_fts"]["ok"] is True
    assert result["checks"]["fts"]["segments_fts"]["ok"] is True
    assert result["checks"]["transcript_agreement"]["mismatches"] == 0


def test_scan_reports_all_patch5_repair_domains_without_applying_them(tmp_path):
    paths = _fixture(tmp_path, broken=True)

    result = _scan(paths)
    codes = {issue["code"] for issue in result["issues"]}

    assert result["preview_only"] is True
    assert {
        "fts_rowid_drift",
        "transcript_store_disagreement",
        "canonical_link_disagreement",
        "download_archive_id_without_media",
        "catalog_download_without_media",
        "recovery_target_missing",
        "queue_only_processing_recovery",
        "journal_only_processing_recovery",
        "same_title_distinct_identity",
        "folder_override_missing",
        "activity_history_disagreement",
        "config_migration_pending",
        "queue_migration_pending",
        "resuming_migration_pending",
        "database_migration_pending",
        "catalog_not_current",
    }.issubset(codes)
    assert all(issue["proposed_repair"] for issue in result["issues"])
    assert result["checks"]["migration_state"]["catalog_legacy_dirty"] is True


def test_scan_is_byte_for_byte_read_only_and_creates_no_sqlite_sidecars(tmp_path):
    paths = _fixture(tmp_path, broken=True)
    before = _snapshot(tmp_path)

    first = _scan(paths)
    second = _scan(paths)

    after = _snapshot(tmp_path)
    assert before == after
    assert first["summary"] == second["summary"]
    assert not list(tmp_path.glob("*.db-wal"))
    assert not list(tmp_path.glob("*.db-shm"))
    assert not list(tmp_path.glob("*.db-journal"))


def test_title_fts_stale_token_on_reused_rowid_is_detected(tmp_path):
    paths = _fixture(tmp_path)
    connection = sqlite3.connect(paths["database"])
    connection.execute("UPDATE videos SET title='Replacement token' WHERE id=1")
    connection.commit()
    connection.close()

    result = _scan(paths)

    assert result["checks"]["fts"]["videos_fts"]["source_rows"] == 1
    assert result["checks"]["fts"]["videos_fts"]["indexed_rows"] == 1
    assert "fts_token_drift" in {
        issue["code"] for issue in result["issues"]
    }


def test_missing_explicit_inputs_are_reported_not_created(tmp_path):
    requested = {
        "archive": tmp_path / "missing-archive",
        "config": tmp_path / "missing-config.json",
        "database": tmp_path / "missing.db",
        "queue": tmp_path / "missing-queue.json",
        "download": tmp_path / "missing-download.txt",
        "journal": tmp_path / "missing-journal.json",
        "activity": tmp_path / "missing-activity.jsonl",
    }

    result = _scan(requested)

    assert result["ok"] is False
    assert result["preview_only"] is True
    assert {issue["code"] for issue in result["issues"]} >= {
        "archive_unavailable", "config_unreadable", "database_unreadable",
    }
    assert list(tmp_path.iterdir()) == []


def test_packaged_diagnostics_entrypoint_passes_only_explicit_app_paths(tmp_path):
    paths = _fixture(tmp_path)

    class Api(diagnostics_mixin.DiagnosticsMixin):
        def __init__(self):
            self.services = None
            self._config = {"output_dir": str(paths["archive"])}

    with (
        mock.patch.object(diagnostics_mixin, "CONFIG_FILE", paths["config"]),
        mock.patch.object(ytarchiver_config, "APP_DATA_DIR", tmp_path),
        mock.patch.object(ytarchiver_config, "ARCHIVE_FILE", paths["download"]),
        mock.patch.object(ytarchiver_config, "QUEUE_FILE", paths["queue"]),
        mock.patch.object(
            ytarchiver_config, "TRANSCRIPTION_DB", paths["database"]
        ),
        mock.patch.object(
            activity_history, "ACTIVITY_HISTORY_FILE", paths["activity"]
        ),
    ):
        result = Api().integrity_scan_preview()

    assert result["ok"] is True
    assert result["healthy"] is True
    assert result["preview_only"] is True
    assert result["repairs_applied"] is False
    assert "Export and verify a full backup" in result["backup_notice"]
