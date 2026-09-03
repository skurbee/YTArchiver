from __future__ import annotations

from pathlib import Path
from unittest import mock

from backend import archive_scan, autorun, index, index_maintenance
from backend.api_mixins import (
    diagnostics_mixin,
    index_mixin,
    media_ops_mixin,
    settings_mixin,
)
from backend.transcribe import helpers as transcribe_helpers


def _reset_index() -> None:
    index._shutdown_index()
    index._conn = None
    index._reader_conn = None
    index._schema_inited = False
    index._ingest_locks.clear()


def test_additional_archive_folder_is_included_in_catalog_sweep(
        tmp_path, monkeypatch):
    primary = tmp_path / "Primary"
    extra = tmp_path / "Extra"
    channel = extra / "Fixture Channel" / "2025"
    primary.mkdir()
    channel.mkdir(parents=True)
    media = channel / "Fixture [abc123def45].mp4"
    media.write_bytes(b"fixture media")
    database = tmp_path / "catalog.sqlite3"

    monkeypatch.setattr(index, "TRANSCRIPTION_DB", database)
    monkeypatch.setattr(archive_scan, "load_disk_cache", dict)
    monkeypatch.setattr(archive_scan, "save_disk_cache", lambda _cache: None)
    monkeypatch.setattr(
        index_maintenance, "_reconcile_tx_status_from_transcript_titles",
        lambda *_args, **_kwargs: 0,
    )
    _reset_index()
    try:
        result = index_maintenance._sweep_new_videos_impl(
            str(primary), [], extra_roots=[str(extra), "", str(extra)])
        connection = index._open()
        assert connection is not None
        row = connection.execute(
            "SELECT filepath, channel FROM videos WHERE filepath=?",
            (str(media),),
        ).fetchone()
    finally:
        _reset_index()

    assert result["registered"] == 1
    assert row == (str(media), "Fixture Channel")


def test_mixed_additional_root_keeps_child_channel_identity(
        tmp_path, monkeypatch):
    primary = tmp_path / "Primary"
    extra = tmp_path / "Mixed Archive"
    child = extra / "Child Channel" / "2025"
    primary.mkdir()
    child.mkdir(parents=True)
    loose_media = extra / "Loose Fixture [abc123def45].mp4"
    child_media = child / "Child Fixture [def456ghi78].mp4"
    loose_media.write_bytes(b"loose fixture media")
    child_media.write_bytes(b"child fixture media")
    database = tmp_path / "mixed-catalog.sqlite3"

    monkeypatch.setattr(index, "TRANSCRIPTION_DB", database)
    monkeypatch.setattr(archive_scan, "load_disk_cache", dict)
    monkeypatch.setattr(archive_scan, "save_disk_cache", lambda _cache: None)
    monkeypatch.setattr(
        index_maintenance, "_reconcile_tx_status_from_transcript_titles",
        lambda *_args, **_kwargs: 0,
    )
    _reset_index()
    try:
        result = index_maintenance._sweep_new_videos_impl(
            str(primary), [], extra_roots=[str(extra)])
        connection = index._open()
        assert connection is not None
        rows = connection.execute(
            "SELECT filepath, channel FROM videos"
        ).fetchall()
    finally:
        _reset_index()

    channels_by_path = dict(rows)
    assert result["registered"] == 2
    assert len(rows) == 2
    assert channels_by_path[str(loose_media)] == "Mixed Archive"
    assert channels_by_path[str(child_media)] == "Child Channel"


def test_mixed_root_transcript_scan_prunes_child_channel(tmp_path):
    root = tmp_path / "Mixed Archive"
    child = root / "Child Channel"
    child.mkdir(parents=True)
    header = "===({}), (01.02.2024), (0:01), (YT CAPTIONS)===\nbody\n"
    (root / "Mixed Archive Transcript.txt").write_text(
        header.format("Loose title"), encoding="utf-8")
    (child / "Child Channel Transcript.txt").write_text(
        header.format("Child title"), encoding="utf-8")

    existing = transcribe_helpers._scan_existing_transcript_titles(
        str(root), "Mixed Archive", excluded_roots=[str(child)])
    raw_titles = {raw for raw, _video_id in existing.values()}

    assert "Loose title" in raw_titles
    assert "Child title" not in raw_titles


def test_unindexed_count_includes_extra_roots_without_overlap_or_trash(
        tmp_path, monkeypatch):
    primary = tmp_path / "Primary"
    extra = tmp_path / "Extra"
    channel = extra / "Fixture Channel"
    trash = extra / ".YTArchiver Trash" / "Removed"
    recovery = extra / ".ytarchiver-restore-recovery" / "Interrupted"
    for folder in (primary, channel, trash, recovery):
        folder.mkdir(parents=True)

    primary_jsonl = primary / ".Primary Transcript.jsonl"
    extra_jsonl = channel / ".Fixture Channel Transcript.jsonl"
    trash_jsonl = trash / ".Removed Transcript.jsonl"
    recovery_jsonl = recovery / ".Interrupted Transcript.jsonl"
    for path in (primary_jsonl, extra_jsonl, trash_jsonl, recovery_jsonl):
        path.write_text("{}\n", encoding="utf-8")

    reader = mock.Mock()
    reader.execute.return_value.fetchall.return_value = [
        (str(primary_jsonl),),
    ]
    monkeypatch.setattr(index_mixin.index_backend, "_reader_open", lambda: reader)
    real_walk = index_mixin.os.walk
    walked_roots = []

    def _recording_walk(root, *args, **kwargs):
        walked_roots.append(Path(root).resolve())
        return real_walk(root, *args, **kwargs)

    monkeypatch.setattr(index_mixin.os, "walk", _recording_walk)

    class Api(index_mixin.IndexMixin):
        def __init__(self):
            self._config = {
                "output_dir": str(primary),
                "tp_archive_roots": [
                    str(extra), str(channel), str(extra), "",
                ],
            }

        @staticmethod
        def sync_is_running():
            return False

    result = Api().index_unindexed_count()

    assert result == {
        "ok": True,
        "unindexed": 1,
        "on_disk": 2,
        "indexed": 1,
    }
    assert len(walked_roots) == 2
    assert set(walked_roots) == {primary.resolve(), extra.resolve()}


def test_archive_rescan_sizes_and_progress_include_additional_targets(
        tmp_path, monkeypatch):
    primary = tmp_path / "Primary"
    primary_channel = primary / "Primary Channel"
    extra = tmp_path / "Extra"
    extra_a = extra / "Extra A"
    extra_b = extra / "Extra B"
    for folder in (primary_channel, extra_a, extra_b):
        folder.mkdir(parents=True)

    progress = []

    class Api(media_ops_mixin.MediaOpsMixin):
        def __init__(self):
            self._config = {
                "output_dir": str(primary),
                "channels": [{
                    "name": "Primary Channel",
                    "folder": "Primary Channel",
                }],
                "tp_archive_roots": [str(extra)],
            }
            self._log_stream = mock.Mock()
            self._window = None

        @staticmethod
        def sync_is_running():
            return False

        @staticmethod
        def archive_single_is_running():
            return False

        def _push_archive_rescan_progress(self, state):
            progress.append(dict(state))

    class ImmediateThread:
        def __init__(self, target, daemon=False, name=None):
            self.target = target

        def start(self):
            self.target()

    def _fake_sweep(output_dir, channels, progress_cb=None, extra_roots=None):
        targets, _roots = index.build_archive_scan_plan(
            output_dir, channels, extra_roots)
        assert len(targets) == 3
        # A late callback must not make visible progress move backward.
        for position in (1, 3, 2):
            progress_cb(position, 3, targets[position - 1]["name"])
        return {
            "registered": 0,
            "ingested": 0,
            "agg_ingested": 0,
            "tx_reconciled": 0,
        }

    refresh_sizes = mock.Mock(return_value={"checked": 1, "updated": 1})
    refresh_cache = mock.Mock()
    monkeypatch.setattr(
        media_ops_mixin.index_backend, "is_db_writer_busy", lambda: False)
    monkeypatch.setattr(
        media_ops_mixin.index_backend, "prune_missing_videos", dict)
    monkeypatch.setattr(
        media_ops_mixin.index_backend, "sweep_new_videos", _fake_sweep)
    monkeypatch.setattr(
        media_ops_mixin.index_backend, "refresh_channel_file_sizes",
        refresh_sizes)
    monkeypatch.setattr(
        media_ops_mixin.archive_scan_backend,
        "update_disk_cache_for_channel", refresh_cache)
    monkeypatch.setattr(media_ops_mixin.threading, "Thread", ImmediateThread)

    result = Api().archive_rescan()

    assert result == {"ok": True, "started": True}
    assert {call.args for call in refresh_sizes.call_args_list} == {
        ("Primary Channel", str(primary_channel)),
        ("Extra A", str(extra_a)),
        ("Extra B", str(extra_b)),
    }
    assert refresh_cache.call_count == 1
    counted = [state for state in progress if state.get("total")]
    assert {state["total"] for state in counted} == {7}
    assert [state["current"] for state in counted] == sorted(
        state["current"] for state in counted)
    assert [state["percent"] for state in counted] == sorted(
        state["percent"] for state in counted)
    assert any(
        state["phase"] == "scan" and state["phase_total"] == 3
        for state in counted
    )
    assert progress[-1]["phase"] == "complete"
    assert progress[-1]["current"] == progress[-1]["total"] == 7


def test_settings_normalize_overlapping_additional_archive_folders():
    primary = str(Path("C:/Archive/Main"))
    committed = []

    class Api(settings_mixin.SettingsMixin):
        def __init__(self):
            self._config = {"log_mode": "Simple"}
            self._log_stream = mock.Mock()
            self._transcribe = mock.Mock()
            self._reload_config = mock.Mock()

        def _settings_fresh_config(self):
            return {
                "log_mode": "Simple",
                "output_dir": primary,
                "tp_archive_roots": [],
            }

        def _settings_commit_candidate(self, _original, candidate):
            snapshot = dict(candidate)
            committed.append(snapshot)
            return True, snapshot

    result = Api().settings_save({
        "tp_archive_roots": [
            primary,
            str(Path(primary) / "Nested"),
            str(Path("C:/Archive")),
            str(Path("D:/Other/Nested")),
            str(Path("D:/Other")),
            str(Path("D:/Other")),
            "",
        ],
    })

    assert result["ok"] is True
    assert committed[-1]["tp_archive_roots"] == [
        str(Path("D:/Other").resolve())
    ]


def test_failed_autosync_interval_save_restores_live_scheduler(monkeypatch):
    monkeypatch.setattr(autorun, "load_config", dict)
    monkeypatch.setattr(autorun, "config_is_writable", lambda: True)
    monkeypatch.setattr(
        autorun, "config_transaction",
        mock.Mock(side_effect=OSError("locked")),
    )
    monkeypatch.setattr(autorun.threading, "Timer", mock.Mock())
    scheduler = autorun.AutorunScheduler(lambda: {"started": True})

    result = scheduler.set_interval_mins(60)

    assert result["ok"] is False
    assert result["persisted"] is False
    assert scheduler.get_state()["mins"] == 0
    assert scheduler.get_state()["label"] == "Off"


def test_failed_autosync_mode_save_restores_previous_mode(monkeypatch):
    monkeypatch.setattr(
        autorun, "load_config", lambda: {"autorun_mode": "timer"})
    monkeypatch.setattr(autorun, "config_is_writable", lambda: True)
    monkeypatch.setattr(
        autorun, "config_transaction",
        mock.Mock(side_effect=OSError("locked")),
    )
    scheduler = autorun.AutorunScheduler(lambda: {"started": True})

    result = scheduler.set_mode("clock")

    assert result["ok"] is False
    assert result["persisted"] is False
    assert result["mode"] == "timer"
    assert scheduler.get_state()["mode"] == "timer"


def test_optional_transcription_tools_are_not_reported_as_broken(monkeypatch):
    class Api(diagnostics_mixin.DiagnosticsMixin):
        def __init__(self):
            self._log_stream = mock.Mock()
            self._window = None
            self._transcribe = None
            self.services = None

    monkeypatch.setattr(
        "backend.deps_installer.probe",
        lambda **_kwargs: {
            "ytdlp": {"path": "yt-dlp.exe"},
            "ffmpeg": {"path": "ffmpeg.exe"},
            "ffprobe": {"path": "ffprobe.exe"},
        },
    )

    result = Api().check_dependencies()
    row = next(
        item for item in result["rows"]
        if item["name"] == "AI transcription tools"
    )

    assert row["ok"] is True
    assert row["status"] == "warning"
