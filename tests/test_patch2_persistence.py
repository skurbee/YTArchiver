from __future__ import annotations

import contextlib
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch2-persist-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import (
    archive_scan,
    channel_art,
    channel_cache,
    deps_installer,
    index,
    livestreams,
    provenance,
    redownload,
    reorg,
    repair_captions,
    subs,
    utils,
)
from backend import log_stream as log_stream_module
from backend.metadata import io as metadata_io
from backend.metadata import refresh_fetch
from backend.services import file_ops
from backend.sync import core as sync_core


class _FakePipe:
    def __init__(self, lines: list[str]) -> None:
        self._lines = [line.encode("utf-8") + b"\n" for line in lines]

    def readline(self) -> bytes:
        return self._lines.pop(0) if self._lines else b""

    def close(self) -> None:
        pass


class _FakeProc:
    def __init__(self, lines: list[str]) -> None:
        self.stdout = _FakePipe(lines)
        self.returncode = 0

    def poll(self) -> int:
        return self.returncode

    def wait(self, timeout=None) -> int:
        return self.returncode

    def terminate(self) -> None:
        self.returncode = -15

    def kill(self) -> None:
        self.returncode = -9


class _FakeWatchdog:
    def __init__(self) -> None:
        self.last_output = [0.0]
        self.stop_event = threading.Event()
        self.stalled: dict[str, bool] = {}

    def stop(self, timeout=None) -> None:
        self.stop_event.set()


class _FakeLog:
    def emit(self, *_args, **_kwargs) -> None:
        pass

    def emit_text(self, *_args, **_kwargs) -> None:
        pass

    def emit_error(self, *_args, **_kwargs) -> None:
        pass

    def emit_dim(self, *_args, **_kwargs) -> None:
        pass

    def flush(self) -> None:
        pass


class DependencyPersistenceTests(unittest.TestCase):
    @staticmethod
    def _write_tools(folder: Path, marker: bytes) -> None:
        folder.mkdir(parents=True, exist_ok=True)
        for name in ("yt-dlp.exe", "ffmpeg.exe", "ffprobe.exe"):
            (folder / name).write_bytes(marker + name.encode("ascii"))

    def test_ytdlp_missing_checksum_preserves_existing_binary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            app_data = Path(td)
            bin_dir = app_data / "bin"
            bin_dir.mkdir()
            existing = bin_dir / "yt-dlp.exe"
            existing.write_bytes(b"known-good")

            def fake_download(_url, dest, *_args):
                Path(dest).write_bytes(b"unverified-new")

            with mock.patch.object(deps_installer, "APP_DATA_DIR", app_data), \
                    mock.patch.object(deps_installer, "ensure_bin_on_path"), \
                    mock.patch.object(deps_installer, "_download",
                                      side_effect=fake_download), \
                    mock.patch.object(deps_installer, "_fetch_text",
                                      return_value="not-a-checksum"):
                result = deps_installer.install_ytdlp(force=True)

            self.assertFalse(result["ok"])
            self.assertTrue(result["integrity_error"])
            self.assertEqual(existing.read_bytes(), b"known-good")
            self.assertFalse(list(app_data.glob(".bin-stage-*")))

    def test_core_stage_failure_preserves_complete_existing_toolset(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            app_data = Path(td)
            bin_dir = app_data / "bin"
            bin_dir.mkdir()
            old = {
                "yt-dlp.exe": b"old-ytdlp",
                "ffmpeg.exe": b"old-ffmpeg",
                "ffprobe.exe": b"old-ffprobe",
            }
            for name, content in old.items():
                (bin_dir / name).write_bytes(content)

            def stage_ytdlp(stage, _progress):
                (stage / "yt-dlp.exe").write_bytes(b"new-ytdlp")
                return {"ok": True, "path": str(stage / "yt-dlp.exe")}

            with mock.patch.object(deps_installer, "APP_DATA_DIR", app_data), \
                    mock.patch.object(deps_installer, "ensure_bin_on_path"), \
                    mock.patch.object(deps_installer, "_stage_ytdlp",
                                      side_effect=stage_ytdlp), \
                    mock.patch.object(deps_installer, "_stage_ffmpeg",
                                      side_effect=RuntimeError("extract failed")), \
                    mock.patch.object(deps_installer, "probe",
                                      return_value={"core_ok": True}):
                result = deps_installer.install_core(force=True)

            self.assertFalse(result["ok"])
            for name, content in old.items():
                self.assertEqual((bin_dir / name).read_bytes(), content)

    def test_directory_swap_failure_rolls_existing_toolset_back(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            app_data = Path(td)
            bin_dir = app_data / "bin"
            bin_dir.mkdir()
            (bin_dir / "yt-dlp.exe").write_bytes(b"old")
            stage = app_data / ".bin-stage-test"
            stage.mkdir()
            (stage / "yt-dlp.exe").write_bytes(b"new")
            real_replace = os.replace
            failed_once = False

            def fail_new_directory(src, dest):
                nonlocal failed_once
                if (not failed_once and Path(src) == stage
                        and Path(dest) == bin_dir):
                    failed_once = True
                    raise OSError("simulated swap failure")
                return real_replace(src, dest)

            with mock.patch.object(deps_installer, "APP_DATA_DIR", app_data), \
                    mock.patch.object(deps_installer.os, "replace",
                                      side_effect=fail_new_directory):
                with self.assertRaises(OSError):
                    deps_installer._swap_managed_bin(stage)

            self.assertEqual((bin_dir / "yt-dlp.exe").read_bytes(), b"old")
            self.assertFalse(list(app_data.glob(".bin-backup-*")))

    def test_python_checksum_failure_never_executes_installer(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            app_data = Path(td)

            def fake_download(_url, dest, *_args):
                Path(dest).write_bytes(b"unverified-installer")

            with mock.patch.object(deps_installer, "APP_DATA_DIR", app_data), \
                    mock.patch.object(deps_installer, "_find_python311",
                                      return_value=None), \
                    mock.patch.object(deps_installer, "_download",
                                      side_effect=fake_download), \
                    mock.patch.object(deps_installer, "_fetch_py311_sha256",
                                      return_value=None), \
                    mock.patch.object(deps_installer.subprocess, "run") as run:
                result = deps_installer.install_python311()

            self.assertFalse(result["ok"])
            self.assertTrue(result["integrity_error"])
            run.assert_not_called()

    def test_core_installers_are_serialized(self) -> None:
        active = 0
        max_active = 0
        state_lock = threading.Lock()
        results: list[dict] = []
        with tempfile.TemporaryDirectory() as td:
            app_data = Path(td)
            (app_data / "bin").mkdir()

            def prepare():
                return Path(tempfile.mkdtemp(dir=app_data, prefix="stage-"))

            def stage(stage_dir, _progress):
                nonlocal active, max_active
                with state_lock:
                    active += 1
                    max_active = max(max_active, active)
                time.sleep(0.05)
                (stage_dir / "yt-dlp.exe").write_bytes(b"new")
                with state_lock:
                    active -= 1
                return {"ok": True, "path": str(stage_dir / "yt-dlp.exe")}

            def swap(stage_dir):
                return stage_dir

            with mock.patch.object(deps_installer, "APP_DATA_DIR", app_data), \
                    mock.patch.object(deps_installer, "ensure_bin_on_path"), \
                    mock.patch.object(deps_installer, "_prepare_bin_stage",
                                      side_effect=prepare), \
                    mock.patch.object(deps_installer, "_stage_ytdlp",
                                      side_effect=stage), \
                    mock.patch.object(deps_installer, "_swap_managed_bin",
                                      side_effect=swap):
                threads = [threading.Thread(
                    target=lambda: results.append(
                        deps_installer.install_ytdlp(force=True)))
                    for _ in range(2)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=2)

            self.assertEqual(len(results), 2)
            self.assertTrue(all(result["ok"] for result in results))
        self.assertEqual(max_active, 1)

    def test_interrupted_bin_swap_restores_exact_backup_before_mkdir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            app_data = Path(td)
            live = app_data / "bin"
            stage = app_data / ".bin-stage-crash"
            backup = app_data / ".bin-backup-crash"
            self._write_tools(live, b"old-")
            self._write_tools(stage, b"new-")
            os.replace(live, backup)

            with mock.patch.object(deps_installer, "APP_DATA_DIR", app_data):
                deps_installer._write_bin_swap_journal({
                    "version": 1,
                    "phase": "backup_moved",
                    "stage": stage.name,
                    "backup": backup.name,
                    "had_existing": True,
                })
                recovered = deps_installer.managed_bin_dir()

            self.assertEqual(recovered, live)
            self.assertTrue((live / "yt-dlp.exe").read_bytes().startswith(b"old-"))
            self.assertFalse(stage.exists())
            self.assertFalse(backup.exists())
            self.assertFalse((app_data / ".bin-swap.json").exists())

    def test_completed_bin_swap_keeps_new_live_tools_on_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            app_data = Path(td)
            live = app_data / "bin"
            backup = app_data / ".bin-backup-crash"
            stage = app_data / ".bin-stage-crash"
            self._write_tools(live, b"new-")
            self._write_tools(backup, b"old-")

            with mock.patch.object(deps_installer, "APP_DATA_DIR", app_data):
                deps_installer._write_bin_swap_journal({
                    "version": 1,
                    "phase": "committed",
                    "stage": stage.name,
                    "backup": backup.name,
                    "had_existing": True,
                })
                recovered = deps_installer.managed_bin_dir()

            self.assertEqual(recovered, live)
            self.assertTrue((live / "yt-dlp.exe").read_bytes().startswith(b"new-"))
            self.assertFalse(backup.exists())
            self.assertFalse((app_data / ".bin-swap.json").exists())

    def test_legacy_empty_live_dir_recovers_nonempty_backup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            app_data = Path(td)
            live = app_data / "bin"
            backup = app_data / ".bin-backup-legacy"
            live.mkdir()
            self._write_tools(backup, b"old-")

            with mock.patch.object(deps_installer, "APP_DATA_DIR", app_data):
                recovered = deps_installer.managed_bin_dir()

            self.assertEqual(recovered, live)
            self.assertTrue((live / "ffmpeg.exe").read_bytes().startswith(b"old-"))
            self.assertFalse(backup.exists())


class RepairCheckpointTests(unittest.TestCase):
    def test_db_reconcile_failure_is_not_green_success(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            vtt = root / "video.en.vtt"
            vtt.write_text("captions", encoding="utf-8")
            jsonl = root / ".Channel Transcript.jsonl"
            jsonl.write_text("{}\n", encoding="utf-8")
            segments = [{"s": 0.0, "e": 1.0,
                         "w": [{"t": "hello", "s": 0.0, "e": 1.0}]}]

            with mock.patch.object(repair_captions, "_fetch_vtt_with_backoff",
                                   return_value=(vtt, None)), \
                    mock.patch.object(repair_captions, "_parse_vtt",
                                      return_value=segments), \
                    mock.patch.object(repair_captions, "_replace_jsonl_entry"), \
                    mock.patch.object(repair_captions, "_update_db_words",
                                      return_value=(0, "database is locked")):
                ok, message, count, _words = repair_captions._repair_one_video(
                    "yt-dlp", jsonl, "Title", "abc123def45", "YT CAPTIONS",
                    False, _FakeLog(), tmp_dir=root)

            self.assertFalse(ok)
            self.assertIn("partial", message)
            self.assertIn("DB reconcile failed", message)
            self.assertEqual(count, 1)

    def test_short_db_reconcile_is_a_retryable_partial(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            jsonl = root / ".Channel Transcript.jsonl"
            jsonl.write_text("{}\n", encoding="utf-8")
            segments = [
                {"s": 0.0, "e": 1.0,
                 "w": [{"t": "hello", "s": 0.0, "e": 1.0}]},
                {"s": 1.0, "e": 2.0,
                 "w": [{"t": "world", "s": 1.0, "e": 2.0}]},
            ]

            for updated_rows in (0, 1):
                with self.subTest(updated_rows=updated_rows):
                    vtt = root / "video.en.vtt"
                    vtt.write_text("captions", encoding="utf-8")
                    with mock.patch.object(
                            repair_captions, "_fetch_vtt_with_backoff",
                            return_value=(vtt, None)), mock.patch.object(
                            repair_captions, "_parse_vtt",
                            return_value=segments), mock.patch.object(
                            repair_captions, "_replace_jsonl_entry"), \
                            mock.patch.object(
                                repair_captions, "_update_db_words",
                                return_value=(updated_rows, None)):
                        ok, message, count, _words = (
                            repair_captions._repair_one_video(
                                "yt-dlp", jsonl, "Title", "abc123def45",
                                "YT CAPTIONS", False, _FakeLog(),
                                tmp_dir=root))

                    self.assertFalse(ok)
                    self.assertIn("partial", message)
                    self.assertIn(f"{updated_rows}/2", message)
                    self.assertEqual(count, 2)

    def test_partial_repair_retains_checkpoint_and_retry_progress(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            channel = root / "Channel"
            channel.mkdir()
            transcript = channel / ".Channel Transcript.jsonl"
            transcript.write_text("{}\n", encoding="utf-8")
            candidate = [("abc123def45", "Title", "YT CAPTIONS")]

            with mock.patch.object(repair_captions, "find_yt_dlp",
                                   return_value="yt-dlp"), \
                    mock.patch.object(repair_captions, "_find_cookie_source",
                                      return_value=None), \
                    mock.patch.object(repair_captions, "_collect_yt_videos",
                                      return_value=candidate), \
                    mock.patch.object(repair_captions, "_repair_one_video",
                                      return_value=(False, "partial: DB failed",
                                                    1, 1)), \
                    mock.patch.object(repair_captions, "_open_repair_db_conn",
                                      return_value=None), \
                    mock.patch.object(repair_captions, "_load_checkpoint",
                                      return_value=None), \
                    mock.patch.object(repair_captions, "_load_progress",
                                      return_value=set()), \
                    mock.patch.object(repair_captions, "_save_checkpoint") as save, \
                    mock.patch.object(repair_captions, "_append_progress") as append, \
                    mock.patch.object(repair_captions, "_clear_checkpoint") as clear_cp, \
                    mock.patch.object(repair_captions, "_clear_progress") as clear_prog:
                result = repair_captions.repair_archive(
                    output_dir=str(root), channel_folder="Channel",
                    log_stream=_FakeLog(), scope_url="channel://test")

            self.assertFalse(result["ok"])
            self.assertTrue(result["partial"])
            self.assertEqual(result["failed"], 1)
            save.assert_called_once()
            append.assert_not_called()
            clear_cp.assert_not_called()
            clear_prog.assert_not_called()


class MetadataPersistenceTests(unittest.TestCase):
    def test_target_builder_keeps_failure_exclusion_after_backfill(self) -> None:
        targets = refresh_fetch._metadata_targets(
            ["good0000001", "failed00001", "good0000001", "new00000001"],
            {"good0000001"}, {"good0000001"}, {"failed00001"},
            refresh=False)

        self.assertEqual(targets, ["new00000001"])
        refreshed = refresh_fetch._metadata_targets(
            ["failed00001", "failed00001"], set(), set(), {"failed00001"},
            refresh=True)
        self.assertEqual(refreshed, ["failed00001"])

    def test_metadata_jsonl_skips_non_object_records(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / ".Channel Metadata.jsonl"
            path.write_text(
                "[]\n" + json.dumps({"video_id": "abc123def45",
                                      "title": "Valid"}) + "\n",
                encoding="utf-8")

            result = metadata_io._read_metadata_jsonl(str(path))

            self.assertEqual(list(result), ["abc123def45"])
            with self.assertRaises(ValueError):
                metadata_io._read_metadata_jsonl(str(path), strict=True)

    def test_strict_metadata_jsonl_rejects_torn_line_without_writing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / ".Channel Metadata.jsonl"
            original = (
                json.dumps({"video_id": "abc123def45", "title": "First"})
                + "\n"
                + '{"video_id":"torn0000001","title":'
                + "\n"
                + json.dumps({"video_id": "xyz987uvw65", "title": "Last"})
                + "\n"
            ).encode("utf-8")
            path.write_bytes(original)

            non_strict = metadata_io._read_metadata_jsonl(str(path))
            self.assertEqual(
                set(non_strict), {"abc123def45", "xyz987uvw65"})
            with self.assertRaises(json.JSONDecodeError):
                metadata_io._read_metadata_jsonl(str(path), strict=True)
            self.assertEqual(path.read_bytes(), original)

    def test_redownload_metadata_index_skips_non_object_records(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = root / ".Channel Metadata.jsonl"
            path.write_text(
                "[1, 2]\n" + json.dumps({
                    "video_id": "abc123def45", "title": "Valid",
                    "upload_date": "20260831"}) + "\n",
                encoding="utf-8")

            result = redownload._build_metadata_index(str(root))

            self.assertEqual(result["by_id"]["abc123def45"]["title"], "Valid")

    def test_provenance_title_map_skips_non_object_records(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "transcript.jsonl"
            path.write_text(
                "null\n" + json.dumps({"video_id": "abc123def45",
                                        "title": "Valid"}) + "\n",
                encoding="utf-8")

            result = provenance._title_id_map(str(path))

            self.assertEqual(result, {"Valid": {"abc123def45"}})

    def test_reorg_info_reader_skips_non_object_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            video = Path(td) / "Video.mp4"
            video.write_bytes(b"video")
            video.with_suffix(".info.json").write_text("[]", encoding="utf-8")

            self.assertIsNone(reorg._date_from_info_json(video))


class TrashPersistenceTests(unittest.TestCase):
    @staticmethod
    def _config(root: Path) -> dict:
        return {"output_dir": str(root), "tp_archive_roots": []}

    def test_channel_folder_trash_restore_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            video = channel / "Video.mp4"
            video.write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                trashed = file_ops.safe_rmtree_channel_folder(str(channel))
                listing = file_ops.list_trash_entries(str(root))
                restored = file_ops.restore_trash_entry(
                    trashed["trashed_folder_path"])

            self.assertTrue(trashed["ok"])
            self.assertEqual(listing["entries"][0]["entry_type"],
                             "channel_folder")
            self.assertTrue(restored["ok"], restored.get("error"))
            self.assertEqual(restored["entry_type"], "channel_folder")
            self.assertEqual(video.read_bytes(), b"video")
            self.assertFalse(Path(trashed["trashed_folder_path"]).exists())

    def test_legacy_channel_manifest_is_restorable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            trash_folder = root / ".YTArchiver Trash" / "legacy-channel"
            trash_folder.mkdir(parents=True)
            (trash_folder / "Video.mp4").write_bytes(b"video")
            original = root / "Channel"
            manifest = {
                "original_path": os.path.normpath(str(original)),
                "trashed_path": os.path.normpath(str(trash_folder)),
                "trashed_at": "2026-08-31T00:00:00",
                "reason": "legacy",
            }
            file_ops._write_trash_manifest(str(trash_folder), manifest)

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                result = file_ops.restore_trash_entry(str(trash_folder))

            self.assertTrue(result["ok"], result.get("error"))
            self.assertEqual((original / "Video.mp4").read_bytes(), b"video")

    def test_channel_restore_moves_recovery_manifest_before_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            (channel / "Video.mp4").write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                trashed = file_ops.safe_rmtree_channel_folder(str(channel))

                real_remove = file_ops.os.remove
                def deny_restored_manifest(path):
                    if (Path(path).parent == channel
                            and Path(path).name == ".ytarchiver-trash.json"):
                        raise PermissionError("manifest busy")
                    return real_remove(path)

                with mock.patch.object(file_ops.os, "remove",
                                       side_effect=deny_restored_manifest):
                    restored = file_ops.restore_trash_entry(
                        trashed["trashed_folder_path"])

            self.assertTrue(restored["ok"], restored.get("error"))
            self.assertIn("cleanup_warning", restored)
            self.assertEqual((channel / "Video.mp4").read_bytes(), b"video")
            self.assertTrue((channel / ".ytarchiver-trash.json").is_file())
            self.assertFalse(Path(trashed["trashed_folder_path"]).exists())

    def test_forged_video_restore_destination_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            trash_folder = root / ".YTArchiver Trash" / "entry"
            trash_folder.mkdir(parents=True)
            source = trash_folder / "Video.mp4"
            source.write_bytes(b"video")
            original = root / "Channel" / "Video.mp4"
            outside = Path(td) / "outside.mp4"
            manifest = {
                "version": 1,
                "entry_type": "video",
                "archive_root": str(root),
                "original_path": str(original),
                "trashed_path": str(source),
                "files": [{"role": "video", "original_path": str(outside),
                           "trashed_path": str(source)}],
            }
            file_ops._write_trash_manifest(str(trash_folder), manifest)

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                result = file_ops.restore_trash_entry(str(trash_folder))

            self.assertFalse(result["ok"])
            self.assertIn("unsafe", result["error"])
            self.assertTrue(source.exists())
            self.assertFalse(outside.exists())

    def test_manifest_write_failure_rolls_channel_move_back(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            (channel / "Video.mp4").write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(file_ops, "_write_trash_manifest",
                                      side_effect=OSError("disk full")):
                result = file_ops.safe_rmtree_channel_folder(str(channel))

            self.assertFalse(result["ok"])
            self.assertTrue(channel.is_dir())
            self.assertEqual((channel / "Video.mp4").read_bytes(), b"video")

    def test_video_trash_interruption_is_listed_and_restorable(self) -> None:
        class SimulatedPowerLoss(BaseException):
            pass

        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            video = channel / "Video.mp4"
            video.write_bytes(b"video")
            real_move = file_ops.shutil.move

            def move_then_interrupt(source, destination):
                real_move(source, destination)
                raise SimulatedPowerLoss()

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(file_ops.shutil, "move",
                                      side_effect=move_then_interrupt):
                with self.assertRaises(SimulatedPowerLoss):
                    file_ops.safe_trash_video_file(str(video))

            listing = file_ops.list_trash_entries(str(root))
            self.assertEqual(len(listing["entries"]), 1)
            self.assertEqual(listing["entries"][0]["state"], "pending")
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                restored = file_ops.restore_trash_entry(
                    listing["entries"][0]["trashed_folder_path"])

            self.assertTrue(restored["ok"], restored.get("error"))
            self.assertEqual(video.read_bytes(), b"video")

    def test_video_trash_move_error_after_copy_rolls_file_back(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            video = channel / "Video.mp4"
            video.write_bytes(b"video")
            real_move = file_ops.shutil.move
            calls = 0

            def move_then_fail(source, destination):
                nonlocal calls
                calls += 1
                real_move(source, destination)
                if calls == 1:
                    raise OSError("source cleanup failed")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(file_ops.shutil, "move",
                                      side_effect=move_then_fail):
                result = file_ops.safe_trash_video_file(str(video))

            self.assertFalse(result["ok"])
            self.assertEqual(video.read_bytes(), b"video")
            listing = file_ops.list_trash_entries(str(root))
            self.assertEqual(listing["entries"], [])

    def test_partial_cross_volume_copy_never_overwrites_original(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            video = channel / "Video.mp4"
            video.write_bytes(b"complete original")

            def partial_copy_then_fail(_source, destination):
                Path(destination).write_bytes(b"partial")
                raise OSError("cross-volume copy interrupted")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(file_ops.shutil, "move",
                                      side_effect=partial_copy_then_fail):
                result = file_ops.safe_trash_video_file(str(video))

            self.assertFalse(result["ok"])
            self.assertEqual(video.read_bytes(), b"complete original")
            listing = file_ops.list_trash_entries(str(root))
            self.assertEqual(len(listing["entries"]), 1)
            self.assertEqual(listing["entries"][0]["state"], "pending")
            trash_folder = Path(listing["entries"][0]["trashed_folder_path"])
            self.assertEqual((trash_folder / "Video.mp4").read_bytes(),
                             b"partial")

    def test_video_trash_collision_never_removes_foreign_folder(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            video = channel / "Video.mp4"
            video.write_bytes(b"video")
            trash_root = root / ".YTArchiver Trash"
            foreign = trash_root / "claimed-by-another-operation"
            foreign.mkdir(parents=True)
            sentinel = foreign / "sentinel.bin"
            sentinel.write_bytes(b"do not remove")
            owned = trash_root / "claimed-by-this-operation"

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(
                        file_ops, "_trash_path_for",
                        side_effect=[str(foreign), str(owned)]), \
                    mock.patch.object(file_ops.shutil, "move",
                                      side_effect=OSError("move failed")):
                result = file_ops.safe_trash_video_file(str(video))

            self.assertFalse(result["ok"])
            self.assertEqual(video.read_bytes(), b"video")
            self.assertEqual(sentinel.read_bytes(), b"do not remove")
            self.assertFalse(owned.exists())

    def test_restore_rejects_existing_directory_at_file_destination(
            self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            video = channel / "Video.mp4"
            video.write_bytes(b"video")
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                trashed = file_ops.safe_trash_video_file(str(video))

            video.mkdir()
            trash_folder = Path(trashed["trashed_folder_path"])
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                restored = file_ops.restore_trash_entry(str(trash_folder))

            self.assertFalse(restored["ok"])
            self.assertIn("Destination already exists", restored["error"])
            self.assertTrue(video.is_dir())
            self.assertEqual(list(video.iterdir()), [])
            self.assertEqual((trash_folder / "Video.mp4").read_bytes(),
                             b"video")

    def test_restore_never_treats_symlink_as_already_restored(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            video = channel / "Video.mp4"
            video.write_bytes(b"video")
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                trashed = file_ops.safe_trash_video_file(str(video))

            trash_folder = Path(trashed["trashed_folder_path"])
            (trash_folder / "Video.mp4").unlink()
            destination_key = os.path.normcase(os.path.normpath(str(video)))
            real_lexists = file_ops.os.path.lexists
            real_isfile = file_ops.os.path.isfile
            real_islink = file_ops.os.path.islink

            def is_destination(path) -> bool:
                return (os.path.normcase(os.path.normpath(str(path)))
                        == destination_key)

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(
                        file_ops.os.path, "lexists",
                        side_effect=lambda path: (
                            True if is_destination(path)
                            else real_lexists(path))), \
                    mock.patch.object(
                        file_ops.os.path, "isfile",
                        side_effect=lambda path: (
                            False if is_destination(path)
                            else real_isfile(path))), \
                    mock.patch.object(
                        file_ops.os.path, "islink",
                        side_effect=lambda path: (
                            True if is_destination(path)
                            else real_islink(path))), \
                    mock.patch.object(file_ops.shutil, "move") as move_mock:
                restored = file_ops.restore_trash_entry(str(trash_folder))

            self.assertFalse(restored["ok"])
            self.assertIn("not a regular file", restored["error"])
            move_mock.assert_not_called()

    def test_interrupted_multifile_restore_resumes_from_manifest(self) -> None:
        class SimulatedPowerLoss(BaseException):
            pass

        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            video = channel / "Video.mp4"
            sidecar = channel / "Video.info.json"
            video.write_bytes(b"video")
            sidecar.write_text('{"id": "abc"}', encoding="utf-8")
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                trashed = file_ops.safe_trash_video_file(str(video))

            real_move = file_ops._move_no_replace
            calls = 0

            def move_then_interrupt(source, destination):
                nonlocal calls
                calls += 1
                real_move(source, destination)
                if calls == 1:
                    raise SimulatedPowerLoss()

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(file_ops, "_move_no_replace",
                                      side_effect=move_then_interrupt):
                with self.assertRaises(SimulatedPowerLoss):
                    file_ops.restore_trash_entry(
                        trashed["trashed_folder_path"])

            listing = file_ops.list_trash_entries(str(root))
            self.assertEqual(listing["entries"][0]["state"], "restoring")
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                restored = file_ops.restore_trash_entry(
                    trashed["trashed_folder_path"])

            self.assertTrue(restored["ok"], restored.get("error"))
            self.assertEqual(video.read_bytes(), b"video")
            self.assertEqual(sidecar.read_text(encoding="utf-8"),
                             '{"id": "abc"}')
            self.assertFalse(Path(trashed["trashed_folder_path"]).exists())

    def test_restore_keeps_manifest_visible_when_stray_file_blocks_cleanup(
            self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            video = channel / "Video.mp4"
            video.write_bytes(b"video")
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                trashed = file_ops.safe_trash_video_file(str(video))

            trash_folder = Path(trashed["trashed_folder_path"])
            stray = trash_folder / "unexpected.bin"
            stray.write_bytes(b"keep visible")
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                first = file_ops.restore_trash_entry(str(trash_folder))

            self.assertTrue(first["ok"], first.get("error"))
            self.assertIn("cleanup_warning", first)
            self.assertTrue((trash_folder / ".ytarchiver-trash.json").is_file())
            self.assertEqual(
                file_ops.list_trash_entries(str(root))["entries"][0]["state"],
                "restoring")

            stray.unlink()
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                second = file_ops.restore_trash_entry(str(trash_folder))
            self.assertTrue(second["ok"], second.get("error"))
            self.assertFalse(trash_folder.exists())

    def test_power_loss_after_manifest_removal_keeps_restore_visible(
            self) -> None:
        class SimulatedPowerLoss(BaseException):
            pass

        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            video = channel / "Video.mp4"
            video.write_bytes(b"video")
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                trashed = file_ops.safe_trash_video_file(str(video))

            trash_folder = Path(trashed["trashed_folder_path"])
            real_rmdir = file_ops.os.rmdir

            def late_file_then_interrupt(path):
                if Path(path) == trash_folder:
                    (trash_folder / "late.bin").write_bytes(b"keep visible")
                    raise SimulatedPowerLoss()
                return real_rmdir(path)

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(file_ops.os, "rmdir",
                                      side_effect=late_file_then_interrupt):
                with self.assertRaises(SimulatedPowerLoss):
                    file_ops.restore_trash_entry(str(trash_folder))

            inside_manifest = trash_folder / ".ytarchiver-trash.json"
            recovery_marker = Path(
                file_ops._restore_cleanup_marker_path(str(trash_folder)))
            self.assertFalse(inside_manifest.exists())
            self.assertTrue(recovery_marker.is_file())
            listing = file_ops.list_trash_entries(str(root))
            self.assertEqual(len(listing["entries"]), 1)
            self.assertEqual(listing["entries"][0]["state"], "restoring")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                blocked = file_ops.restore_trash_entry(str(trash_folder))
            self.assertTrue(blocked["ok"], blocked.get("error"))
            self.assertIn("cleanup_warning", blocked)
            self.assertEqual(
                file_ops.list_trash_entries(str(root))["entries"][0]["state"],
                "restoring",
            )

            (trash_folder / "late.bin").unlink()
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                finished = file_ops.restore_trash_entry(str(trash_folder))
            self.assertTrue(finished["ok"], finished.get("error"))
            self.assertFalse(trash_folder.exists())
            self.assertFalse(recovery_marker.exists())

    def test_channel_trash_interruption_carries_manifest_and_restores(self) -> None:
        class SimulatedPowerLoss(BaseException):
            pass

        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            video = channel / "Video.mp4"
            video.write_bytes(b"video")
            real_move = file_ops._move_no_replace

            def move_then_interrupt(source, destination):
                real_move(source, destination)
                raise SimulatedPowerLoss()

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(file_ops, "_move_no_replace",
                                      side_effect=move_then_interrupt):
                with self.assertRaises(SimulatedPowerLoss):
                    file_ops.safe_rmtree_channel_folder(str(channel))

            listing = file_ops.list_trash_entries(str(root))
            self.assertEqual(len(listing["entries"]), 1)
            self.assertEqual(listing["entries"][0]["state"], "pending")
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value=self._config(root)), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                restored = file_ops.restore_trash_entry(
                    listing["entries"][0]["trashed_folder_path"])

            self.assertTrue(restored["ok"], restored.get("error"))
            self.assertEqual(video.read_bytes(), b"video")


class LogRetryTests(unittest.TestCase):
    def test_transient_bridge_failure_retries_swapped_batch(self) -> None:
        delivered = threading.Event()

        class Window:
            def __init__(self) -> None:
                self.calls = 0
                self.payloads: list[str] = []

            def evaluate_js(self, payload: str) -> None:
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("bridge busy")
                self.payloads.append(payload)
                delivered.set()

        window = Window()
        stream = log_stream_module.LogStreamer(window)
        stream._ready = True
        stream.RETRY_BASE_SEC = 0.01

        stream._do_flush([["important\n", "simpleline"]], [])

        self.assertTrue(delivered.wait(timeout=1.0))
        self.assertEqual(window.calls, 2)
        self.assertIn("important", window.payloads[0])
        self.assertEqual(stream._retry_items, 0)

    def test_retry_queue_has_hard_batch_and_item_bounds(self) -> None:
        stream = log_stream_module.LogStreamer(mock.Mock())
        stream._ready = True
        stream.RETRY_BASE_SEC = 10.0
        batch = [[f"line-{i}\n", "simpleline"]
                 for i in range(stream.MAX_BATCH_SIZE)]

        for _ in range(stream.MAX_RETRY_BATCHES + 5):
            stream._queue_retry(batch, [], 1)

        with stream._lock:
            if stream._retry_timer is not None:
                stream._retry_timer.cancel()
                stream._retry_timer = None
            batch_count = len(stream._retry_batches)
            item_count = stream._retry_items
            stream._retry_batches.clear()
            stream._retry_items = 0
        self.assertLessEqual(batch_count, stream.MAX_RETRY_BATCHES)
        self.assertLessEqual(item_count, stream.MAX_RETRY_ITEMS)
        self.assertGreater(stream._retry_dropped, 0)

    def test_failed_inflight_batch_stays_ahead_of_fresh_output(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        class Window:
            def __init__(self) -> None:
                self.lock = threading.Lock()
                self.calls = 0
                self.payloads: list[str] = []

            def evaluate_js(self, payload: str) -> None:
                with self.lock:
                    self.calls += 1
                    call_number = self.calls
                if call_number == 1:
                    entered.set()
                    release.wait(timeout=1.0)
                    raise RuntimeError("bridge busy")
                self.payloads.append(payload)

        class DormantTimer:
            def __init__(self, _delay, callback) -> None:
                self.callback = callback
                self.daemon = False

            def start(self) -> None:
                pass

            def cancel(self) -> None:
                pass

        window = Window()
        stream = log_stream_module.LogStreamer(window)
        stream._ready = True
        stream.RETRY_BASE_SEC = 10.0

        with mock.patch.object(
                log_stream_module.threading, "Timer",
                side_effect=lambda delay, callback: DormantTimer(
                    delay, callback)):
            worker = threading.Thread(target=lambda: stream._do_flush(
                [["older\n", "simpleline"]], []))
            worker.start()
            self.assertTrue(entered.wait(timeout=1.0))

            # This arrives while the older bridge call is still in flight.
            # It must queue, not start a second evaluate_js call.
            stream._do_flush([["newer\n", "simpleline"]], [])
            with window.lock:
                self.assertEqual(window.calls, 1)

            release.set()
            worker.join(timeout=1.0)
            self.assertFalse(worker.is_alive())

            # Drive the two dormant retry timers deterministically.
            stream._retry_flush()
            stream._retry_flush()

        self.assertEqual(len(window.payloads), 2)
        self.assertIn("older", window.payloads[0])
        self.assertIn("newer", window.payloads[1])
        self.assertEqual(stream._retry_items, 0)
        self.assertFalse(stream._bridge_busy)

    def test_retry_timer_is_published_before_start(self) -> None:
        observed: list[bool] = []
        stream = log_stream_module.LogStreamer(mock.Mock())
        stream._ready = True

        class InspectingTimer:
            def __init__(self, _delay, _callback) -> None:
                self.daemon = False

            def start(self) -> None:
                observed.append(stream._retry_timer is self)

            def cancel(self) -> None:
                pass

        with mock.patch.object(
                log_stream_module.threading, "Timer", InspectingTimer):
            stream._queue_retry([["line\n", "simpleline"]], [], 1)

        self.assertEqual(observed, [True])
        with stream._lock:
            stream._retry_batches.clear()
            stream._retry_items = 0
            stream._retry_timer = None

    def test_retry_timer_start_failure_keeps_payload_queued(self) -> None:
        observed: list[bool] = []
        stream = log_stream_module.LogStreamer(mock.Mock())
        stream._ready = True

        class FailingTimer:
            def __init__(self, _delay, _callback) -> None:
                self.daemon = False

            def start(self) -> None:
                observed.append(stream._retry_timer is self)
                raise RuntimeError("timer unavailable")

            def cancel(self) -> None:
                pass

        with mock.patch.object(
                log_stream_module.threading, "Timer", FailingTimer):
            stream._queue_retry([["line\n", "simpleline"]], [], 1)

        self.assertEqual(observed, [True])
        self.assertIsNone(stream._retry_timer)
        self.assertEqual(len(stream._retry_batches), 1)
        self.assertEqual(stream._retry_items, 1)

    def test_retry_timer_start_failure_never_logs_under_stream_lock(self) -> None:
        stream = log_stream_module.LogStreamer(mock.Mock())
        stream._ready = True

        class FailingTimer:
            def __init__(self, _delay, _callback) -> None:
                self.daemon = False

            def start(self) -> None:
                raise RuntimeError("timer unavailable")

            def cancel(self) -> None:
                pass

        # In the installed app, log_stream's logger feeds this same stream.
        # A warning emitted while _retry scheduling holds the stream lock
        # therefore recurses into emit() and deadlocks. Model that path
        # directly and require the queue operation to return promptly.
        def recursive_warning(*_args, **_kwargs) -> None:
            stream.emit_text("timer warning")
        with mock.patch.object(
                log_stream_module.threading, "Timer", FailingTimer), \
                mock.patch.object(
                    log_stream_module._log, "warning",
                    side_effect=recursive_warning):
            worker = threading.Thread(
                target=lambda: stream._queue_retry(
                    [["line\n", "simpleline"]], [], 1),
                daemon=True,
            )
            worker.start()
            worker.join(timeout=1.0)

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(stream._retry_batches), 1)
        self.assertEqual(stream._retry_items, 1)

    def test_exhausted_retry_attempt_is_dropped(self) -> None:
        stream = log_stream_module.LogStreamer(mock.Mock())
        stream._ready = True

        stream._queue_retry(
            [["line\n", "simpleline"]], [],
            stream.MAX_RETRY_ATTEMPTS + 1)

        self.assertEqual(len(stream._retry_batches), 0)
        self.assertEqual(stream._retry_items, 0)
        self.assertEqual(stream._retry_dropped, 1)


class SyncCommitTests(unittest.TestCase):
    @staticmethod
    def _emitted_semantic_tag(stream, wanted: str) -> bool:
        for call in stream.emit.call_args_list:
            if not call.args:
                continue
            for segment in call.args[0] or []:
                if len(segment) < 2:
                    continue
                tags = segment[1]
                if tags == wanted:
                    return True
                if isinstance(tags, (list, tuple)) and wanted in tags:
                    return True
        return False

    def _run_sync(self, root: Path, archive: Path, lines: list[str], *,
                  create_media: Path | list[Path] | None = None,
                  register_result: bool = True,
                  yt_dlp_archive_id: str | None = None,
                  archive_paths_out: list[Path] | None = None,
                  streams_url_value: str | None = None,
                  second_pass_interrupt: str | None = None,
                  disk_space_side_effect: list[bool] | None = None,
                  existing_identity_result: bool | None = None):
        channel = {
            "name": "Test Channel",
            "folder": "Test Channel",
            "url": "https://www.youtube.com/@test-channel",
            "mode": "full",
            "resolution": "1080",
            "auto_transcribe": True,
            "auto_metadata": False,
            "initialized": False,
            "init_complete": False,
            "sync_complete": False,
        }
        stream = mock.Mock()
        cancel_event = (
            threading.Event() if second_pass_interrupt == "cancel" else None)
        pause_event = (
            threading.Event() if second_pass_interrupt == "pause" else None)
        launch_count = 0

        if cancel_event is not None:
            def cancel_at_streams_header(segments, *_args, **_kwargs):
                text = "".join(str(segment[0]) for segment in segments or [])
                if "[Streams]" in text:
                    cancel_event.set()
            stream.emit.side_effect = cancel_at_streams_header

        def launch(*_args, **_kwargs):
            nonlocal launch_count
            launch_count += 1
            if launch_count == 2 and pause_event is not None:
                pause_event.set()
                raise OSError("simulated streams launch pause")
            if create_media is not None:
                media_paths = (create_media if isinstance(create_media, list)
                               else [create_media])
                for media_path in media_paths:
                    media_path.parent.mkdir(parents=True, exist_ok=True)
                    media_path.write_bytes(b"video")
            command = _args[0]
            if "--download-archive" in command:
                archive_index = command.index("--download-archive") + 1
                run_archive = Path(command[archive_index])
                if archive_paths_out is not None:
                    archive_paths_out.append(run_archive)
                if yt_dlp_archive_id:
                    with open(run_archive, "a", encoding="utf-8") as handle:
                        handle.write(f"youtube {yt_dlp_archive_id}\n")
            return _FakeProc(lines)

        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(
                sync_core, "ARCHIVE_FILE", str(archive)))
            stack.enter_context(mock.patch.object(
                sync_core, "find_yt_dlp", return_value="yt-dlp"))
            stack.enter_context(mock.patch.object(
                sync_core, "load_config", return_value={"output_dir": str(root)}))
            stack.enter_context(mock.patch.object(
                sync_core, "config_is_writable", return_value=False))
            if existing_identity_result is not None:
                stack.enter_context(mock.patch.object(
                    sync_core, "existing_media_matches_video_id",
                    return_value=existing_identity_result))
            stack.enter_context(mock.patch.object(
                sync_core, "_find_cookie_source", return_value=[]))
            stack.enter_context(mock.patch.object(
                sync_core, "popen_ytdlp_process", side_effect=launch))
            stack.enter_context(mock.patch.object(
                sync_core, "start_download_watchdog",
                side_effect=lambda *_args, **_kwargs: _FakeWatchdog()))
            for target, name in (
                (sync_core, "finish_ytdlp_process"),
                (sync_core, "write_sync_progress"),
                (sync_core, "set_sync_active"),
                (sync_core, "clear_sync_active"),
                (sync_core, "_hide_sidecar_win"),
                (sync_core, "_record_recent_download"),
                (sync_core, "_bg_channel_maintenance"),
                (livestreams, "drop"),
                (archive_scan, "update_disk_cache_for_channel"),
                (channel_art, "fetch_channel_art"),
            ):
                stack.enter_context(mock.patch.object(target, name))
            append_ids = stack.enter_context(mock.patch.object(
                channel_cache, "append_ids"))
            stack.enter_context(mock.patch.object(
                utils, "check_directory_writable", return_value=True))
            disk_space = stack.enter_context(mock.patch.object(
                utils, "check_disk_space"))
            if disk_space_side_effect is None:
                disk_space.return_value = True
            else:
                disk_space.side_effect = disk_space_side_effect
            stack.enter_context(mock.patch.object(
                subs, "streams_url", return_value=streams_url_value))
            register = stack.enter_context(mock.patch.object(
                index, "register_video", return_value=register_result))
            result = sync_core.sync_channel(
                channel, stream, cancel_event=cancel_event,
                pause_event=pause_event)
        return result, append_ids, register, stream

    def test_observed_id_without_media_is_not_committed(self) -> None:
        video_id = "abc123def45"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            lines = [
                f"[youtube] {video_id}: Downloading webpage",
                (f"DLTRACK:::Missing:::Test Channel:::20260831:::5:::"
                 f"300:::{video_id}"),
            ]

            result, append_ids, register, stream = self._run_sync(
                root, archive, lines)

            self.assertEqual(result["downloaded"], 0)
            self.assertFalse(archive.exists())
            append_ids.assert_not_called()
            register.assert_not_called()
            self.assertFalse(self._emitted_semantic_tag(
                stream, "download_complete"))

    def test_registration_failure_does_not_commit_durable_media_id(self) -> None:
        video_id = "abc123def45"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            media = root / "Test Channel" / "Fresh Video.mp4"
            existing = root / "Test Channel" / "Existing Video.mp4"
            existing.parent.mkdir(parents=True)
            existing.write_bytes(b"existing")
            lines = [
                f"[youtube] {video_id}: Downloading webpage",
                f"[download] Destination: {media}",
                f'[Merger] Merging formats into "{media}"',
                (f"DLTRACK:::Fresh Video:::Test Channel:::20260831:::5:::"
                 f"300:::{video_id}"),
            ]

            result, append_ids, register, stream = self._run_sync(
                root, archive, lines, create_media=media,
                register_result=False)

            self.assertEqual(result["downloaded"], 0)
            self.assertGreaterEqual(result["errors"], 1)
            self.assertFalse(archive.exists())
            append_ids.assert_not_called()
            register.assert_called_once()
            self.assertFalse(self._emitted_semantic_tag(
                stream, "download_complete"))

    def test_green_completion_is_emitted_after_durable_registration(self) -> None:
        video_id = "abc123def45"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            media = root / "Test Channel" / "Fresh Video.mp4"
            lines = [
                f"[youtube] {video_id}: Downloading webpage",
                f"[download] Destination: {media}",
                f'[Merger] Merging formats into "{media}"',
                (f"DLTRACK:::Fresh Video:::Test Channel:::20260831:::5:::"
                 f"300:::{video_id}"),
            ]

            result, _append_ids, register, stream = self._run_sync(
                root, archive, lines, create_media=media,
                register_result=True)

            self.assertEqual(result["downloaded"], 1)
            register.assert_called_once()
            self.assertTrue(self._emitted_semantic_tag(
                stream, "download_complete"))
            self.assertEqual(
                archive.read_text(encoding="utf-8").splitlines(),
                [f"youtube {video_id}"],
            )

    def test_yt_dlp_cannot_write_unregistered_id_to_global_archive(self) -> None:
        video_id = "abc123def45"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            media = root / "Test Channel" / "Fresh Video.mp4"
            existing = root / "Test Channel" / "Existing Video.mp4"
            existing.parent.mkdir(parents=True)
            existing.write_bytes(b"existing")
            lines = [
                f"[youtube] {video_id}: Downloading webpage",
                f"[download] Destination: {media}",
                f'[Merger] Merging formats into "{media}"',
                (f"DLTRACK:::Fresh Video:::Test Channel:::20260831:::5:::"
                 f"300:::{video_id}"),
            ]
            run_archives: list[Path] = []

            result, _cache, register, _stream = self._run_sync(
                root, archive, lines, create_media=media,
                register_result=False, yt_dlp_archive_id=video_id,
                archive_paths_out=run_archives)

            self.assertEqual(result["downloaded"], 0)
            register.assert_called_once()
            self.assertFalse(archive.exists())
            self.assertEqual(len(run_archives), 1)
            self.assertNotEqual(run_archives[0], archive)
            self.assertFalse(run_archives[0].exists())

    def test_partial_run_exits_commit_registered_ids_and_close_private_archive(
            self) -> None:
        video_id = "abc123def45"
        for interrupt in ("cancel", "pause"):
            with self.subTest(interrupt=interrupt), \
                    tempfile.TemporaryDirectory() as td:
                root = Path(td)
                archive = root / "download-archive.txt"
                media = root / "Test Channel" / "Fresh Video.mp4"
                existing = root / "Test Channel" / "Existing Video.mp4"
                existing.parent.mkdir(parents=True)
                existing.write_bytes(b"existing")
                lines = [
                    f"[youtube] {video_id}: Downloading webpage",
                    f"[download] Destination: {media}",
                    f'[Merger] Merging formats into "{media}"',
                    (f"DLTRACK:::Fresh Video:::Test Channel:::20260831:::5:::"
                     f"300:::{video_id}"),
                ]
                run_archives: list[Path] = []

                result, _cache, register, _stream = self._run_sync(
                    root, archive, lines, create_media=media,
                    streams_url_value=(
                        "https://www.youtube.com/@test-channel/streams"),
                    second_pass_interrupt=interrupt,
                    archive_paths_out=run_archives,
                )

                self.assertEqual(result["reason"],
                                 "cancelled" if interrupt == "cancel"
                                 else "paused")
                register.assert_called_once()
                self.assertEqual(
                    archive.read_text(encoding="utf-8").splitlines(),
                    [f"youtube {video_id}"],
                )
                self.assertEqual(len(run_archives), 1)
                self.assertFalse(run_archives[0].exists())

    def test_disk_low_exit_commits_every_registered_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            channel_dir = root / "Test Channel"
            channel_dir.mkdir(parents=True)
            (channel_dir / "Existing Video.mp4").write_bytes(b"existing")
            ids = [f"vid{i:08d}" for i in range(1, 11)]
            media_paths = [channel_dir / f"Fresh Video {i}.mp4"
                           for i in range(1, 11)]
            lines: list[str] = []
            for i, (video_id, media) in enumerate(
                    zip(ids, media_paths, strict=True), 1):
                lines.extend([
                    f"[youtube] {video_id}: Downloading webpage",
                    f"[download] Destination: {media}",
                    f'[Merger] Merging formats into "{media}"',
                    (f"DLTRACK:::Fresh Video {i}:::Test Channel:::"
                     f"20260831:::5:::300:::{video_id}"),
                ])
            run_archives: list[Path] = []

            result, _cache, register, _stream = self._run_sync(
                root, archive, lines, create_media=media_paths,
                archive_paths_out=run_archives,
                disk_space_side_effect=[True, True, False],
            )

            self.assertEqual(result["reason"], "disk_low_midrun")
            self.assertEqual(result["downloaded"], 10)
            self.assertEqual(register.call_count, 10)
            self.assertEqual(
                archive.read_text(encoding="utf-8").splitlines(),
                [f"youtube {video_id}" for video_id in ids],
            )
            self.assertEqual(len(run_archives), 1)
            self.assertFalse(run_archives[0].exists())

    def test_download_archive_fsync_failure_propagates(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            archive = Path(td) / "download-archive.txt"
            with mock.patch.object(sync_core, "ARCHIVE_FILE", str(archive)), \
                    mock.patch.object(
                        sync_core.os, "fsync",
                        side_effect=OSError("durability unavailable")):
                with self.assertRaisesRegex(
                        OSError, "durability unavailable"):
                    sync_core._append_download_archive_ids(["abc123def45"])

    def test_existing_file_second_pass_registers_before_commit(self) -> None:
        video_id = "abc123def45"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            media = root / "Test Channel" / "Fresh Video.mp4"
            fresh_lines = [
                f"[youtube] {video_id}: Downloading webpage",
                f"[download] Destination: {media}",
                f'[Merger] Merging formats into "{media}"',
                (f"DLTRACK:::Fresh Video:::Test Channel:::20260831:::5:::"
                 f"300:::{video_id}"),
            ]
            existing_lines = [
                f"[youtube] {video_id}: Downloading webpage",
                f"[download] {media} has already been downloaded",
                (f"DLTRACK:::Fresh Video:::Test Channel:::20260831:::5:::"
                 f"300:::{video_id}"),
            ]

            first, first_cache, first_register, _ = self._run_sync(
                root, archive, fresh_lines, create_media=media,
                register_result=False)
            second, second_cache, second_register, _ = self._run_sync(
                root, archive, existing_lines, register_result=False,
                existing_identity_result=True)

            self.assertEqual(first["downloaded"], 0)
            first_register.assert_called_once()
            first_cache.assert_not_called()
            self.assertEqual(second["downloaded"], 0)
            self.assertGreaterEqual(second["errors"], 1)
            second_register.assert_called_once()
            second_cache.assert_not_called()

            third, third_cache, third_register, _ = self._run_sync(
                root, archive, existing_lines, register_result=True,
                existing_identity_result=True)

            self.assertEqual(third["downloaded"], 0)
            third_register.assert_called_once()
            third_cache.assert_called_once_with(
                "https://www.youtube.com/@test-channel", [video_id])


if __name__ == "__main__":
    unittest.main()
