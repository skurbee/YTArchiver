from __future__ import annotations

import json
import contextlib
import os
import queue
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from backend import (
    archive_scan,
    autorun as autorun_backend,
    channel_art,
    cmd_server,
    compress,
    deps_installer,
    disk_watch,
    drift_scan,
    fs_safety,
    index,
    index_bookmarks,
    index_maintenance,
    index_graph,
    index_search,
    log_stream,
    local_fileserver,
    livestreams,
    net,
    process_runner,
    punct_alignment,
    punct_restore,
    queues,
    redownload,
    repair_captions,
    reorg,
    subs,
    text_utils,
    thumbnails,
    tray as tray_backend,
    utils,
    ytarchiver_config,
)
from backend.services import AppServices, BridgeEventBus, file_ops
from backend.api_mixins.channel_mixin import ChannelMixin
from backend.api_mixins import _shared as api_shared
from backend.api_mixins import archive_mixin
from backend.api_mixins import backup_mixin
from backend.api_mixins import browse_mixin
from backend.api_mixins import channel_mixin
from backend.api_mixins import diagnostics_mixin
from backend.api_mixins import metadata_mixin
from backend.api_mixins import settings_mixin
from backend.api_mixins import subs_mixin
from backend.api_mixins import sync_mixin
from backend.api_mixins import thumbnail_mixin
from backend.api_mixins import transcribe_mixin
from backend.api_mixins import window_mixin
from backend.api_mixins.browse_mixin import BrowseMixin
from backend.api_mixins.info_mixin import InfoMixin
from backend.api_mixins.index_mixin import IndexMixin
from backend.api_mixins.media_ops_mixin import MediaOpsMixin
from backend.api_mixins.onboarding_mixin import OnboardingMixin
from backend.api_mixins.queue_mixin import QueueMixin
from backend.api_mixins.redownload_mixin import RedownloadMixin
from backend.metadata import io as metadata_io
from backend.api_mixins.recent_mixin import RecentMixin
from backend.api_mixins.sync_mixin import SyncMixin
from backend.api_mixins.thumbnail_mixin import ThumbnailMixin
from backend.api_mixins.transcribe_mixin import TranscribeMixin
from backend.api_mixins.video_mixin import VideoMixin
from backend.api_mixins.window_mixin import WindowMixin
from backend.sync import core as sync_core
from backend.sync import (
    log_rows,
    quickcheck,
    recent_track,
    sync_helpers,
    ytdlp_session,
)
from backend.sync.options import (
    build_match_filter,
    build_output_template,
    normalize_channel_sync_options,
    normalized_date_after,
)
from backend.sync.ytdlp_events import (
    extract_video_id_from_line,
    is_verbose_chatter_line,
)
from backend.metadata import fetcher as metadata_fetcher
from backend.metadata import refresh_views
from backend.transcribe.punct_manager import PunctuationManager
from backend.transcribe import core as transcribe_core
from backend.transcribe import helpers as transcribe_helpers
from backend.transcribe import transcribe_vtt
from backend.transcribe import transcribe_files


class LocalFileServerAllowlistTests(unittest.TestCase):
    def tearDown(self) -> None:
        local_fileserver.set_allowed_roots([])
        local_fileserver._request_token = ""

    def test_allowlist_fails_closed_when_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "thumb.jpg")
            Path(path).write_bytes(b"image")

            local_fileserver.set_allowed_roots([])

            self.assertFalse(local_fileserver._is_under_allowed_root(path))

    def test_sibling_prefix_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = os.path.join(td, "Archive")
            sibling = os.path.join(td, "ArchiveBad")
            os.makedirs(root)
            os.makedirs(sibling)
            path = os.path.join(sibling, "leak.jpg")
            Path(path).write_bytes(b"image")

            local_fileserver.set_allowed_roots([root])

            self.assertFalse(local_fileserver._is_under_allowed_root(path))

    def test_file_under_allowed_root_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = os.path.join(td, "Archive")
            os.makedirs(root)
            path = os.path.join(root, "thumb.jpg")
            Path(path).write_bytes(b"image")

            local_fileserver.set_allowed_roots([root])

            self.assertTrue(local_fileserver._is_under_allowed_root(path))

    def test_url_for_includes_session_token(self) -> None:
        local_fileserver._server_port = 49152
        local_fileserver._request_token = "secret token"

        url = local_fileserver.url_for(r"C:\Archive\Video File.mp4")

        self.assertIn("/file/", url)
        self.assertIn("?t=secret%20token", url)

    def test_file_requests_require_session_token(self) -> None:
        request = mock.Mock()
        request.headers = {}
        parsed = local_fileserver.urllib.parse.urlsplit(
            "/file/C%3A%2FArchive%2Fv.mp4?t=bad")
        local_fileserver._request_token = "good"

        self.assertFalse(local_fileserver._authorized_request(request, parsed))

        parsed = local_fileserver.urllib.parse.urlsplit(
            "/file/C%3A%2FArchive%2Fv.mp4?t=good")
        self.assertTrue(local_fileserver._authorized_request(request, parsed))


class ManagedRootsTests(unittest.TestCase):
    def test_managed_roots_fail_closed_without_configured_roots(self) -> None:
        with mock.patch("backend.ytarchiver_config.load_config",
                        return_value={"output_dir": "", "tp_archive_roots": []}):
            self.assertFalse(utils.is_within_managed_roots(__file__))

    def test_managed_roots_reject_sibling_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = os.path.join(td, "Archive")
            sibling = os.path.join(td, "ArchiveBad")
            os.makedirs(root)
            os.makedirs(sibling)
            path = os.path.join(sibling, "video.mp4")
            Path(path).write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": root,
                                          "tp_archive_roots": []}):
                self.assertFalse(utils.is_within_managed_roots(path))

    def test_managed_roots_accept_child_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = os.path.join(td, "Archive")
            os.makedirs(root)
            path = os.path.join(root, "video.mp4")
            Path(path).write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": root,
                                          "tp_archive_roots": []}):
                self.assertTrue(utils.is_within_managed_roots(path))


class SharedApiHelperTests(unittest.TestCase):
    def test_normalize_dialog_paths_handles_pywebview_return_shapes(
            self) -> None:
        self.assertIsNone(api_shared.normalize_dialog_paths(None))
        self.assertIsNone(api_shared.normalize_dialog_paths(""))
        self.assertIsNone(api_shared.normalize_dialog_paths(()))
        self.assertEqual(api_shared.normalize_dialog_paths("C:/out.zip"),
                         "C:/out.zip")
        self.assertEqual(api_shared.normalize_dialog_paths(("C:/out.zip",)),
                         "C:/out.zip")

    def test_backup_restore_whitelist_uses_export_entry_names(self) -> None:
        exported = {name for name, _path in backup_mixin._backup_file_entries()}
        allowed = backup_mixin._allowed_backup_top_names()

        self.assertTrue(exported)
        self.assertTrue(exported.issubset(allowed))
        self.assertIn("transcription_index.db", allowed)

    def test_clean_import_channel_drops_runtime_and_unknown_fields(self) -> None:
        clean, err = backup_mixin._clean_import_channel({
            "name": "Channel",
            "folder": "Channel",
            "url": "youtube.com/@valid_handle",
            "resolution": "1080",
            "auto_transcribe": True,
            "initialized": True,
            "last_sync": "2099-01-01",
            "unexpected": "poison",
        })

        self.assertFalse(err)
        self.assertIsNotNone(clean)
        assert clean is not None
        self.assertEqual(clean["url"], "https://youtube.com/@valid_handle")
        self.assertEqual(clean["resolution"], "1080")
        self.assertTrue(clean["auto_transcribe"])
        self.assertNotIn("initialized", clean)
        self.assertNotIn("last_sync", clean)
        self.assertNotIn("unexpected", clean)

    def test_clean_import_channel_rejects_spoofed_youtube_host(self) -> None:
        clean, err = backup_mixin._clean_import_channel({
            "name": "Bad",
            "url": "https://youtube.com.evil.example/@bad",
        })

        self.assertIsNone(clean)
        self.assertIn("youtube.com channel link", err)

    def test_backup_preview_surfaces_manifest_fts_skip_reason(self) -> None:
        import zipfile

        class Api(backup_mixin.BackupMixin):
            def __init__(self, path):
                self._window = mock.Mock()
                self._window.create_file_dialog.return_value = str(path)

        with tempfile.TemporaryDirectory() as td:
            zip_path = Path(td) / "backup.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr(backup_mixin._BACKUP_MANIFEST_NAME, json.dumps({
                    "fts_db_included": False,
                    "fts_skipped_reason": "FTS DB skipped - too large",
                }))

            fake_webview = mock.Mock(OPEN_DIALOG="open")
            with mock.patch.dict("sys.modules", {"webview": fake_webview}):
                result = Api(zip_path).import_full_backup_preview()

        self.assertTrue(result["ok"])
        self.assertEqual(result["fts_skipped"], "FTS DB skipped - too large")
        self.assertFalse(result["manifest"]["fts_db_included"])


class ArchiveMixinTests(unittest.TestCase):
    def test_write_probe_uses_unique_tempfile_and_leaves_no_fixed_probe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            archive_mixin._probe_output_folder_writable(td)

            leftovers = list(Path(td).iterdir())

        self.assertEqual(leftovers, [])

    def test_recent_scan_bind_rejects_uncorroborated_file(self) -> None:
        self.assertFalse(archive_mixin._recent_scan_bind_is_corroborated(
            r"C:\Archive\Different Video.mp4",
            "abc123DEF45",
            "Expected Download Title",
        ))

    def test_recent_scan_bind_accepts_sanitized_title_prefix(self) -> None:
        self.assertTrue(archive_mixin._recent_scan_bind_is_corroborated(
            r"C:\Archive\Expected Download Title with extra suffix.mp4",
            "abc123DEF45",
            "Expected Download Title with extra suffix that may be trimmed",
        ))

    def test_choose_ytdlp_candidate_uses_existing_media_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stale = root / "old.mp4"
            final = root / "yt-dlp sanitized title.mp4"
            stale.write_bytes(b"old")
            final.write_bytes(b"video")

            chosen = archive_mixin._choose_existing_ytdlp_candidate(
                [str(stale), str(final)], str(root))

        self.assertEqual(Path(chosen).name, final.name)

    def test_archive_single_running_reports_inflight_urls(self) -> None:
        api = archive_mixin.ArchiveMixin()

        self.assertFalse(api.archive_single_is_running())

        api._archive_single_lock = threading.Lock()
        api._archive_single_inflight = {"https://example.test/video"}

        self.assertTrue(api.archive_single_is_running())

    # ── T326: pure DLTRACK / final-path / outcome helpers ──────────────
    def test_parse_dltrack_anchors_on_trailing_fields(self) -> None:
        # A title containing ':::' must not shift the id field.
        line = ("DLTRACK:::Weird ::: Title:::Some Channel:::20240102"
                ":::1048576:::600:::dQw4w9WgXcQ")
        got = archive_mixin.parse_dltrack(line)
        self.assertEqual(got["title"], "Weird ::: Title")
        self.assertEqual(got["uploader"], "Some Channel")
        self.assertEqual(got["upload_date"], "20240102")
        self.assertEqual(got["filesize"], "1048576")
        self.assertEqual(got["duration"], "600")
        self.assertEqual(got["video_id"], "dQw4w9WgXcQ")

    def test_parse_dltrack_returns_none_on_short_line(self) -> None:
        self.assertIsNone(archive_mixin.parse_dltrack("DLTRACK:::only:::three"))
        self.assertIsNone(archive_mixin.parse_dltrack(""))

    def test_resolve_final_path_prefers_id_match_newest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            old = root / "Some Title [dQw4w9WgXcQ].mp4"
            old.write_bytes(b"old")
            os.utime(old, (1000, 1000))
            new = root / "Some Title (01.02.24) [dQw4w9WgXcQ].mp4"
            new.write_bytes(b"new")
            os.utime(new, (9000, 9000))
            got = archive_mixin.resolve_final_path(
                str(root), "dQw4w9WgXcQ", "Some Title", [])
        self.assertEqual(Path(got).name, new.name)

    def test_resolve_final_path_full_title_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            # Similar-prefix sibling must NOT be matched.
            (root / "Video 1 of 2.mp4").write_bytes(b"x")
            target = root / "Video 1 of 2 the finale.mp4"
            target.write_bytes(b"y")
            got = archive_mixin.resolve_final_path(
                str(root), "", "Video 1 of 2 the finale", [])
        self.assertEqual(Path(got).name, target.name)

    def test_classify_download_outcome_paths(self) -> None:
        c = archive_mixin.classify_download_outcome
        self.assertEqual(c(0, True, True, True, True, [])[0], "killed")
        self.assertEqual(c(0, True, False, True, True, [])[0], "success")
        self.assertEqual(
            c(0, True, False, True, False, [])[0], "downloaded_unindexed")
        # rc 0 + dltrack but file vanished -> failed with the rescan reason
        out, reason = c(0, True, False, False, False, [])
        self.assertEqual(out, "failed")
        self.assertIn("could not find", reason)
        # known stderr signature wins over the generic reason
        self.assertEqual(
            c(1, True, False, False, False, ["private video"]),
            ("failed", "private video"))
        # nonzero rc with no signature reports the exit code
        self.assertEqual(
            c(2, False, False, False, False, []),
            ("failed", "yt-dlp exited with code 2"))


class UtilsTests(unittest.TestCase):
    def test_check_disk_space_fails_closed_on_probe_error(self) -> None:
        # check_disk_space lives in fs_safety after the utils split; patch
        # the shutil + logger in THAT module, not the re-export shim.
        with mock.patch.object(fs_safety.shutil, "disk_usage",
                               side_effect=OSError("offline")):
            with self.assertLogs(fs_safety._log, level="WARNING"):
                self.assertFalse(utils.check_disk_space("Z:/Archive", 1))

    def test_browse_launch_guard_rejects_outside_archive_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            outside = Path(td) / "Elsewhere"
            root.mkdir()
            outside.mkdir()
            video = outside / "video.mp4"
            video.write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}):
                result = browse_mixin._guard_browse_launch_path(
                    str(video), require_file=True)

            self.assertFalse(result["ok"])

    def test_browse_launch_guard_accepts_inside_archive_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            root.mkdir()
            video = root / "video.mp4"
            video.write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}):
                result = browse_mixin._guard_browse_launch_path(
                    str(video), require_file=True)

            self.assertTrue(result["ok"])

    def test_try_find_by_title_matches_non_latin_fuzzy_title(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "中文标题.mp4"
            video.write_bytes(b"video")

            found = utils.try_find_by_title(str(root), "中文标题！")

        self.assertEqual(found, str(video))


    def test_delete_video_sidecars_preserves_txt_and_visible_images(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "Sample Transcript.mp4"
            txt = root / "Sample Transcript.txt"
            jpg = root / "Sample Transcript.jpg"
            jsonl = root / "Sample Transcript.jsonl"
            video.write_bytes(b"video")
            txt.write_text("keep this", encoding="utf-8")
            jpg.write_bytes(b"image")
            jsonl.write_text("{}", encoding="utf-8")

            utils.delete_video_sidecars(str(video))

            self.assertTrue(txt.exists())
            self.assertTrue(jpg.exists())
            self.assertFalse(jsonl.exists())

    def test_delete_video_sidecars_removes_hidden_image_thumbnails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "Sample.mp4"
            webp = root / "Sample.webp"
            video.write_bytes(b"video")
            webp.write_bytes(b"image")

            # delete_video_sidecars lives in fs_safety and calls its OWN
            # _file_has_hidden_attribute — patch it there, not the utils
            # re-export (patching the shim wouldn't affect the real call).
            with mock.patch.object(fs_safety, "_file_has_hidden_attribute",
                                   return_value=True):
                utils.delete_video_sidecars(str(video))

            self.assertFalse(webp.exists())


class CmdServerTests(unittest.TestCase):
    def test_read_body_rejects_oversized_content_length(self) -> None:
        request = mock.Mock()
        request.headers = {
            "Content-Length": str(cmd_server._MAX_POST_BODY_BYTES + 1)}

        with self.assertRaises(cmd_server.RequestBodyTooLarge):
            cmd_server._read_body(request)

        request.rfile.read.assert_not_called()

    def test_read_body_accepts_small_json(self) -> None:
        payload = b'{"ok": true}'
        request = mock.Mock()
        request.headers = {"Content-Length": str(len(payload))}
        request.rfile.read.return_value = payload

        body = cmd_server._read_body(request)

        self.assertEqual(body, {"ok": True})
        request.rfile.read.assert_called_once_with(len(payload))

    def test_builtin_ping_handler_is_liveness_only(self) -> None:
        captured = {}

        def capture(method, path, fn):
            captured[(method, path)] = fn

        with mock.patch.object(cmd_server, "_load_or_create_token",
                               return_value="token"), \
                mock.patch.object(cmd_server, "_port_busy",
                                  return_value=False), \
                mock.patch.object(cmd_server._http_server,
                                  "ThreadingHTTPServer",
                                  side_effect=OSError("stop")), \
                mock.patch.object(cmd_server, "register_handler",
                                  side_effect=capture):
            self.assertFalse(cmd_server.start_server("v99.9"))

        self.assertEqual(captured[("get", "/cmd/ping")]({}),
                         {"alive": True})


class LogStreamerTests(unittest.TestCase):
    def test_emit_clamps_oversized_segments_before_js_bridge(self) -> None:
        stream = log_stream.LogStreamer()
        stream.simple_mode = False
        long_text = "x" * (stream.MAX_SEGMENT_TEXT_CHARS + 100) + "\n"

        stream.emit([[long_text, "simpleline"]])

        stored = stream._buffer[0][0][0]
        self.assertLessEqual(len(stored), stream.MAX_SEGMENT_TEXT_CHARS)
        self.assertTrue(stored.endswith("[truncated]\n"))


class NetMonitorTests(unittest.TestCase):
    def tearDown(self) -> None:
        net.stop_monitor(timeout=1.0)
        net._monitor_thread = None
        net._monitor_stop.clear()

    def test_stop_monitor_interrupts_background_wait(self) -> None:
        old_interval = net._poll_interval_sec
        net._poll_interval_sec = 30.0
        try:
            with mock.patch.object(net, "probe_once", return_value=True):
                net.start_monitor()
                thread = net._monitor_thread
                self.assertIsNotNone(thread)
                assert thread is not None

                stopped = net.stop_monitor(timeout=1.0)

            self.assertTrue(stopped)
            self.assertFalse(thread.is_alive())
        finally:
            net._poll_interval_sec = old_interval


class TrayControllerTests(unittest.TestCase):
    class FakeIcon:
        def __init__(self):
            self.title = ""
            self.icon = None
            self.show_calls = 0
            self.stop_calls = 0

        def _show(self):
            self.show_calls += 1

        def stop(self):
            self.stop_calls += 1

    class FakeImage:
        def __init__(self, name="base"):
            self.name = name

        def copy(self):
            return TrayControllerTests.FakeImage(self.name + "-copy")

    def test_tray_keepalive_reapplies_title_icon_and_reregisters(self) -> None:
        ctrl = tray_backend.TrayController(tooltip="YT Archiver")
        icon = self.FakeIcon()
        ctrl._started = True
        ctrl._icon = icon
        ctrl._base_img = self.FakeImage()

        ctrl._refresh_shell_registration()

        self.assertEqual(icon.title, "YT Archiver")
        self.assertIs(icon.icon, ctrl._base_img)
        self.assertEqual(icon.show_calls, 1)

    def test_tray_keepalive_does_not_overwrite_spin_frame(self) -> None:
        ctrl = tray_backend.TrayController(tooltip="YT Archiver")
        icon = self.FakeIcon()
        ctrl._started = True
        ctrl._icon = icon
        ctrl._base_img = self.FakeImage()
        ctrl._spin_thread = object()

        ctrl._refresh_shell_registration()

        self.assertIsNone(icon.icon)
        self.assertEqual(icon.show_calls, 1)


class FileOpsTests(unittest.TestCase):
    def test_safe_remove_file_rejects_outside_archive_roots(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            outside = Path(td) / "Elsewhere" / "video.mp4"
            root.mkdir()
            outside.parent.mkdir()
            outside.write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}):
                result = file_ops.safe_remove_file(str(outside))

            self.assertFalse(result["ok"])
            self.assertTrue(outside.exists())

    def test_safe_remove_file_deletes_inside_archive_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            root.mkdir()
            video = root / "video.mp4"
            video.write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                result = file_ops.safe_remove_file(str(video))

            self.assertTrue(result["ok"])
            self.assertFalse(video.exists())

    def test_safe_remove_file_refuses_when_config_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            root.mkdir()
            video = root / "video.mp4"
            video.write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=False):
                result = file_ops.safe_remove_file(str(video))

            self.assertFalse(result["ok"])
            self.assertTrue(video.exists())

    # ── T303: trash restore / purge round-trip ────────────────────────
    def test_trash_restore_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            ch = root / "Channel"
            ch.mkdir(parents=True)
            video = ch / "Video.mp4"
            video.write_bytes(b"video-bytes")
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                trashed = file_ops.safe_trash_video_file(str(video))
                self.assertTrue(trashed["ok"])
                self.assertFalse(video.exists())

                listing = file_ops.list_trash_entries(str(root))
                self.assertEqual(len(listing["entries"]), 1)
                folder = listing["entries"][0]["trashed_folder_path"]

                restored = file_ops.restore_trash_entry(folder)
                self.assertTrue(restored["ok"], restored.get("error"))
                self.assertTrue(video.exists())
                self.assertEqual(video.read_bytes(), b"video-bytes")
                self.assertEqual(
                    file_ops.list_trash_entries(str(root))["entries"], [])

    def test_restore_refuses_when_destination_exists(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            ch = root / "Channel"
            ch.mkdir(parents=True)
            video = ch / "Video.mp4"
            video.write_bytes(b"v1")
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                folder = file_ops.safe_trash_video_file(
                    str(video))["trashed_folder_path"]
                video.write_bytes(b"v2-live")  # a new live file at orig path
                restored = file_ops.restore_trash_entry(folder)
            self.assertFalse(restored["ok"])
            self.assertIn("already exists", restored["error"])
            self.assertEqual(video.read_bytes(), b"v2-live")

    def test_purge_removes_entry_and_refuses_outside_trash(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            ch = root / "Channel"
            ch.mkdir(parents=True)
            video = ch / "Video.mp4"
            video.write_bytes(b"v")
            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                folder = file_ops.safe_trash_video_file(
                    str(video))["trashed_folder_path"]
                purged = file_ops.purge_trash_entry(folder)
                self.assertTrue(purged["ok"])
                self.assertFalse(os.path.isdir(folder))
                # a live channel folder is NOT under the trash root -> refuse
                bad = file_ops.purge_trash_entry(str(ch))
            self.assertFalse(bad["ok"])
            self.assertTrue(ch.is_dir())

    def test_safe_remove_file_can_unhide_before_delete(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            root.mkdir()
            transcript = root / ".video.jsonl"
            transcript.write_text("{}", encoding="utf-8")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "unhide_file_win") as unhide:
                result = file_ops.safe_remove_file(
                    str(transcript),
                    require_config_writable=False,
                    reason="test",
                    unhide_first=True,
                )

            self.assertTrue(result["ok"])
            unhide.assert_called_once_with(str(transcript))
            self.assertFalse(transcript.exists())

    def test_safe_rmtree_channel_folder_quarantines_inside_archive_root(
            self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            folder = root / "Channel"
            folder.mkdir(parents=True)
            (folder / "video.mp4").write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                result = file_ops.safe_rmtree_channel_folder(str(folder))

            self.assertTrue(result["ok"])
            self.assertTrue(result["deleted_folder"])
            self.assertFalse(folder.exists())
            trash_path = Path(result["trashed_folder_path"])
            self.assertTrue(trash_path.is_dir())
            self.assertEqual(trash_path.parent.name, ".YTArchiver Trash")
            self.assertTrue((trash_path / "video.mp4").exists())
            manifest = json.loads(
                (trash_path / ".ytarchiver-trash.json").read_text(
                    encoding="utf-8"))
            self.assertEqual(manifest["original_path"],
                             os.path.normpath(str(folder)))

    def test_safe_trash_video_file_quarantines_video_and_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            root.mkdir()
            video = root / "Video [ABCDEFGHIJK].mp4"
            info = root / "Video [ABCDEFGHIJK].info.json"
            captions = root / "Video [ABCDEFGHIJK].en.vtt"
            video.write_bytes(b"video")
            info.write_text("{}", encoding="utf-8")
            captions.write_text("captions", encoding="utf-8")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                result = file_ops.safe_trash_video_file(str(video))

            self.assertTrue(result["ok"])
            self.assertFalse(video.exists())
            self.assertFalse(info.exists())
            self.assertFalse(captions.exists())
            trash_path = Path(result["trashed_folder_path"])
            self.assertEqual(trash_path.parent.name, ".YTArchiver Trash")
            self.assertTrue((trash_path / video.name).exists())
            self.assertTrue((trash_path / info.name).exists())
            self.assertTrue((trash_path / captions.name).exists())
            manifest = json.loads(
                (trash_path / ".ytarchiver-trash.json").read_text(
                    encoding="utf-8"))
            self.assertEqual(manifest["original_path"],
                             os.path.normpath(str(video)))
            self.assertEqual(len(manifest["files"]), 3)

    def test_safe_rmtree_channel_folder_rejects_outside_archive_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            folder = Path(td) / "Elsewhere" / "Channel"
            root.mkdir()
            folder.mkdir(parents=True)

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                result = file_ops.safe_rmtree_channel_folder(str(folder))

            self.assertFalse(result["ok"])
            self.assertTrue(folder.exists())

    def test_safe_rmtree_channel_folder_refuses_when_config_read_only(
            self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            folder = root / "Channel"
            folder.mkdir(parents=True)

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=False):
                result = file_ops.safe_rmtree_channel_folder(str(folder))

            self.assertFalse(result["ok"])
            self.assertTrue(folder.exists())


class CosmeticProbeCookieTests(unittest.TestCase):
    def test_channel_display_name_uses_cookies_only_after_public_failure(
            self) -> None:
        calls: list[list[str]] = []
        outputs = iter(["", "", "Cookie Channel\n"])

        class FakeProc:
            def __init__(self, argv, **_kwargs):
                calls.append(list(argv))
                self.returncode = 0

            def communicate(self, timeout=None):
                return next(outputs), ""

            def kill(self):
                pass

        with mock.patch("backend.sync.find_yt_dlp", return_value="yt-dlp"), \
                mock.patch("backend.sync._find_cookie_source",
                           return_value=["--cookies", "cookies.txt"]) as cookies, \
                mock.patch("subprocess.Popen", side_effect=FakeProc):
            name = subs.fetch_channel_display_name(
                "https://youtube.com/@example",
                timeout_sec=1,
            )

        self.assertEqual(name, "Cookie Channel")
        cookies.assert_called_once()
        self.assertEqual(len(calls), 3)
        self.assertNotIn("--cookies", calls[0])
        self.assertNotIn("--cookies", calls[1])
        self.assertIn("--cookies", calls[2])

    def test_channel_art_uses_cookies_only_after_public_failure(self) -> None:
        calls: list[list[str]] = []

        def fake_run(argv, **_kwargs):
            calls.append(list(argv))
            if len(calls) == 1:
                return mock.Mock(returncode=1, stdout="", stderr="blocked")
            return mock.Mock(returncode=0, stdout='{"thumbnails":[]}', stderr="")

        with tempfile.TemporaryDirectory() as td, \
                mock.patch.object(channel_art, "find_yt_dlp",
                                  return_value="yt-dlp"), \
                mock.patch.object(channel_art, "_find_cookie_source",
                                  return_value=["--cookies", "cookies.txt"]) as cookies, \
                mock.patch.object(channel_art.subprocess, "run",
                                  side_effect=fake_run):
            result = channel_art.fetch_channel_art(
                "https://youtube.com/@example",
                td,
                force=True,
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "no thumbnails in metadata")
        cookies.assert_called_once()
        self.assertEqual(len(calls), 2)
        self.assertNotIn("--cookies", calls[0])
        self.assertIn("--cookies", calls[1])

    def test_channel_art_does_not_throttle_total_download_failure(self) -> None:
        metadata = json.dumps({
            "thumbnails": [
                {"id": "avatar", "url": "https://example.test/avatar.jpg"},
                {"id": "banner", "url": "https://example.test/banner.jpg"},
            ],
        })

        with tempfile.TemporaryDirectory() as td, \
                mock.patch.object(channel_art, "find_yt_dlp",
                                  return_value="yt-dlp"), \
                mock.patch.object(channel_art.subprocess, "run",
                                  return_value=mock.Mock(
                                      returncode=0, stdout=metadata, stderr="")), \
                mock.patch.object(channel_art, "_http_get",
                                  return_value=False):
            result = channel_art.fetch_channel_art(
                "https://youtube.com/@example",
                td,
                force=True,
            )

            sentinel = Path(td) / ".ChannelArt" / ".last_attempt"
            self.assertFalse(result["ok"])
            self.assertFalse(sentinel.exists())

    def test_channel_art_rejects_oversized_content_length_before_read(
            self) -> None:
        class FakeResp:
            headers = {"Content-Length": str(channel_art._HTTP_MAX_BYTES + 1)}

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, *_args):
                raise AssertionError("body should not be read")

        with tempfile.TemporaryDirectory() as td, \
                mock.patch.object(channel_art.urllib.request, "urlopen",
                                  return_value=FakeResp()):
            ok = channel_art._http_get(
                "https://example.test/avatar.jpg",
                str(Path(td) / "avatar.jpg"),
            )

        self.assertFalse(ok)

    def test_channel_art_rejects_non_http_scheme_before_urlopen(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            dest = str(Path(td) / "avatar.jpg")
            with mock.patch.object(channel_art.urllib.request, "urlopen",
                                   side_effect=AssertionError("urlopen")):
                ok = channel_art._http_get("file:///C:/secret.png", dest)

        self.assertFalse(ok)

    def test_deps_installer_rejects_non_http_scheme_before_urlopen(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            dest = Path(td) / "tool.exe"
            with mock.patch.object(deps_installer.urllib.request, "urlopen",
                                   side_effect=AssertionError("urlopen")):
                with self.assertRaises(ValueError):
                    deps_installer._download(
                        "file:///C:/secret.exe", dest, None, "phase", "tool")


class DependencyInstallerTests(unittest.TestCase):
    def test_run_streaming_keeps_longer_tail_and_tags_error_status(self) -> None:
        progress = []

        class FakeProc:
            returncode = 0
            stdout = iter([f"warning line {i}\n" for i in range(45)] +
                          ["ERROR final\n"])

            def wait(self, timeout=None):
                return 0

        with mock.patch.object(deps_installer.subprocess, "Popen",
                               return_value=FakeProc()):
            rc, tail = deps_installer._run_streaming(
                ["pip"], progress.append, "whisper", "Installing", timeout=1)

        self.assertEqual(rc, 0)
        self.assertEqual(len(tail.splitlines()), 40)
        self.assertEqual(progress[-1]["status"], "error")

    def test_install_whisper_stack_warns_when_pip_upgrade_fails(self) -> None:
        progress = []
        with mock.patch.object(deps_installer, "install_python311",
                               return_value={"ok": True,
                                             "path": "python.exe"}), \
                mock.patch.object(deps_installer, "detect_gpu",
                                  return_value={"ok": False, "name": ""}), \
                mock.patch.object(deps_installer, "_run_streaming",
                                  side_effect=[
                                      (1, "pip failed"),
                                      (0, "torch ok"),
                                      (0, "fw ok"),
                                  ]), \
                mock.patch.object(deps_installer, "_whisper_ready",
                                  return_value=True):
            result = deps_installer.install_whisper_stack(progress.append)

        self.assertTrue(result["ok"])
        self.assertTrue(any(p.get("status") == "warning"
                            and "pip upgrade failed" in p.get("msg", "")
                            for p in progress))


class ArchiveScanTests(unittest.TestCase):
    def test_scan_channel_folder_skips_stat_failure_without_sleeping(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            channel_dir = base / "Channel"
            channel_dir.mkdir()

            with mock.patch.object(archive_scan.os, "walk",
                                   return_value=[(str(channel_dir), [], ["Video.mp4"])]), \
                    mock.patch.object(archive_scan.os.path, "getsize",
                                      side_effect=OSError("vanished")), \
                    mock.patch.object(archive_scan.time, "sleep",
                                      side_effect=AssertionError("slept")):
                count, size = archive_scan.scan_channel_folder(
                    base, {"name": "Channel"})

            self.assertEqual((count, size), (0, 0))

    def test_fragment_suffix_counts_without_merge_sibling(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            channel_dir = base / "Channel"
            channel_dir.mkdir()
            (channel_dir / "Release.f140.mp4").write_bytes(b"video")

            count, size = archive_scan.scan_channel_folder(
                base, {"name": "Channel"})

            self.assertEqual((count, size), (1, 5))

    def test_fragment_suffix_skips_when_final_sibling_exists(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            channel_dir = base / "Channel"
            channel_dir.mkdir()
            (channel_dir / "Release.f140.mp4").write_bytes(b"part")
            (channel_dir / "Release.mp4").write_bytes(b"final")

            count, size = archive_scan.scan_channel_folder(
                base, {"name": "Channel"})

            self.assertEqual((count, size), (1, 5))


class MetadataJsonlTests(unittest.TestCase):
    def test_metadata_jsonl_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, ".Channel Metadata.jsonl")
            entries = {
                "a1": {"video_id": "a1", "title": "First"},
                "b2": {"video_id": "b2", "title": "Second"},
            }

            with mock.patch.object(metadata_io, "_hide_file_win"), \
                    mock.patch.object(metadata_io, "_unhide_file_win"):
                metadata_io._write_metadata_jsonl(path, entries)

            self.assertEqual(metadata_io._read_metadata_jsonl(path), entries)

    def test_metadata_jsonl_prefers_newer_duplicate_entry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, ".Channel Metadata.jsonl")
            rows = [
                {"video_id": "dup", "title": "Old",
                 "fetched_at": "2026-01-01T00:00:00Z"},
                {"video_id": "dup", "title": "New",
                 "fetched_at": "2026-02-01T00:00:00Z"},
            ]
            Path(path).write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8")

            loaded = metadata_io._read_metadata_jsonl(path)

            self.assertEqual(loaded["dup"]["title"], "New")

    def test_metadata_jsonl_compares_fetched_at_as_time_not_text(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, ".Channel Metadata.jsonl")
            rows = [
                {"video_id": "dup", "title": "Older by epoch",
                 "fetched_at": "2026-06-14T01:00:00+09:00"},
                {"video_id": "dup", "title": "Newer by epoch",
                 "fetched_at": "2026-06-13T20:00:00+00:00"},
            ]
            Path(path).write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8")

            loaded = metadata_io._read_metadata_jsonl(path)

            self.assertEqual(loaded["dup"]["title"], "Newer by epoch")

    def test_views_refresh_fetched_at_is_utc_aware(self) -> None:
        fetched_at = refresh_views._utc_fetched_at_now()

        self.assertIn("+00:00", fetched_at)
        self.assertIsNotNone(metadata_io._fetched_at_epoch(fetched_at))

    def test_views_refresh_summary_segments_split_labels_and_counts(self) -> None:
        segments = refresh_views._build_refresh_summary_segments(
            name="Channel",
            full_fetched=2,
            updated_in_place=3,
            skipped_same=4,
            full_errors=1,
            no_meta_entry=0,
            disk_count=10,
            bulk_count=10,
            took=1.25,
        )

        self.assertEqual(segments[0], [" \u2014 ", "meta_bracket"])
        self.assertEqual(segments[1], ["Channel: ", "simpleline"])
        self.assertIn(["2", "simpleline_pink"], segments)
        self.assertIn([" with updated counts", "simpleline"], segments)
        self.assertIn(["1", "red"], segments)
        self.assertIn([" errors", "red"], segments)
        self.assertEqual(segments[-1], [" (took 1.2s)\n", "simpleline"])

    def test_views_refresh_summary_segments_explains_zero_matches(self) -> None:
        segments = refresh_views._build_refresh_summary_segments(
            name="Channel",
            full_fetched=0,
            updated_in_place=0,
            skipped_same=0,
            full_errors=0,
            no_meta_entry=0,
            disk_count=3,
            bulk_count=5,
            took=2.0,
        )

        text = "".join(part for part, _tag in segments)
        self.assertIn("no matches", text)
        self.assertIn("3 on disk vs 5 from YouTube", text)

    def test_classify_video_counts_skip_when_unchanged(self) -> None:
        dec = refresh_views._classify_video_counts(
            {"view_count": 100, "like_count": 5, "comment_count": 2},
            {"view_count": 100, "like_count": 5, "comment_count": 2},
            full_fetch_on_change=False)
        self.assertFalse(dec["changed"])
        self.assertFalse(dec["no_flat_data"])
        self.assertEqual(dec["action"], "skip")

    def test_classify_video_counts_changed_in_place_vs_full_fetch(self) -> None:
        base_old = {"view_count": 100}
        changed_stats = {"view_count": 150}
        in_place = refresh_views._classify_video_counts(
            changed_stats, base_old, full_fetch_on_change=False)
        self.assertTrue(in_place["changed"])
        self.assertEqual(in_place["action"], "in_place")
        full = refresh_views._classify_video_counts(
            changed_stats, base_old, full_fetch_on_change=True)
        self.assertEqual(full["action"], "full_fetch")

    def test_classify_video_counts_no_flat_data_forces_fetch(self) -> None:
        # Flat playlist returned no view_count but we have a stored one.
        dec = refresh_views._classify_video_counts(
            {"view_count": None}, {"view_count": 100},
            full_fetch_on_change=False)
        self.assertTrue(dec["no_flat_data"])
        self.assertEqual(dec["action"], "full_fetch")

    def test_classify_video_counts_missing_like_is_not_a_change(self) -> None:
        # like_count missing in flat mode must NOT be treated as a drop.
        dec = refresh_views._classify_video_counts(
            {"view_count": 100, "like_count": None},
            {"view_count": 100, "like_count": 9},
            full_fetch_on_change=False)
        self.assertFalse(dec["changed"])
        self.assertEqual(dec["action"], "skip")

    def test_classify_video_counts_does_not_mutate_old(self) -> None:
        old = {"view_count": 100}
        refresh_views._classify_video_counts(
            {"view_count": 200}, old, full_fetch_on_change=False)
        self.assertEqual(old, {"view_count": 100})

    def test_metadata_jsonl_strict_read_raises_on_io_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, ".Channel Metadata.jsonl")
            Path(path).write_text("{}\n", encoding="utf-8")

            with mock.patch("builtins.open", side_effect=OSError("boom")):
                self.assertEqual(metadata_io._read_metadata_jsonl(path), {})
                with self.assertRaises(OSError):
                    metadata_io._read_metadata_jsonl(path, strict=True)

    def test_metadata_write_fsyncs_parent_directory_on_posix(self) -> None:
        with mock.patch.object(metadata_io.os, "name", "posix"), \
                mock.patch.object(metadata_io.os, "open",
                                  return_value=123) as open_dir, \
                mock.patch.object(metadata_io.os, "fsync") as fsync, \
                mock.patch.object(metadata_io.os, "close") as close:
            metadata_io._fsync_parent_dir("/tmp/archive/.Channel Metadata.jsonl")

        open_dir.assert_called_once()
        fsync.assert_called_once_with(123)
        close.assert_called_once_with(123)


class MetadataFetcherTests(unittest.TestCase):
    def test_fetch_video_metadata_does_not_brace_slice_warning_text(self) -> None:
        class FakeProc:
            returncode = 0

            def communicate(self, timeout=None):
                return (
                    'warning before json {"id":"wrong","title":"Wrong"} tail',
                    "",
                )

        with mock.patch.object(metadata_fetcher, "_find_cookie_source",
                               return_value=[]), \
                mock.patch.object(metadata_fetcher.subprocess, "Popen",
                                  return_value=FakeProc()):
            result = metadata_fetcher._fetch_video_metadata(
                "yt-dlp", "abc123_def4", "Hint")

        self.assertIsNone(result)


class ThumbnailCacheTests(unittest.TestCase):
    def test_thumbnail_exists_rejects_short_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            thumb_dir = Path(td) / ".Thumbnails"
            thumb_dir.mkdir()
            (thumb_dir / "Broken [abc123_def4].jpg").write_bytes(b"")

            self.assertFalse(thumbnails._thumbnail_exists_for(
                str(thumb_dir), "abc123_def4"))

    def test_download_thumbnail_tries_youtube_fallbacks(self) -> None:
        class FakeResponse:
            headers = {"Content-Length": "20"}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self, _limit):
                return b"\xFF\xD8\xFF" + b"thumbnail-bytes"

        with tempfile.TemporaryDirectory() as td:
            thumb_dir = Path(td) / ".Thumbnails"
            thumb_dir.mkdir()
            stream = mock.Mock()
            calls = []

            def fake_urlopen(req, timeout):
                calls.append(req.full_url)
                if len(calls) == 1:
                    raise OSError("404")
                return FakeResponse()

            with mock.patch.object(thumbnails.urllib.request, "urlopen",
                                   side_effect=fake_urlopen):
                ok = thumbnails._download_thumbnail(
                    "https://i.ytimg.com/vi_webp/abc123_def4/maxresdefault.webp",
                    str(thumb_dir), "Video", "abc123_def4", stream=stream)

            self.assertTrue(ok)
            self.assertTrue(thumbnails._thumbnail_exists_for(
                str(thumb_dir), "abc123_def4"))
            self.assertGreaterEqual(len(calls), 2)
            self.assertEqual(stream.emit.call_count, 0)

    def test_channel_fingerprint_tracks_nested_file_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            channel_dir = Path(td) / "Channel"
            month_dir = channel_dir / "2026" / "06 June"
            month_dir.mkdir(parents=True)
            video = month_dir / "Video.mp4"
            video.write_bytes(b"one")
            old_time = 1_700_000_000
            new_time = old_time + 500
            os.utime(channel_dir, (old_time, old_time))
            os.utime(channel_dir / "2026", (old_time, old_time))
            os.utime(month_dir, (old_time, old_time))
            os.utime(video, (new_time, new_time))

            self.assertEqual(thumbnails._channel_fingerprint(channel_dir),
                             float(new_time))


class SyncOptionsTests(unittest.TestCase):
    def test_sync_options_normalize_defaults(self) -> None:
        opts = normalize_channel_sync_options({
            "name": "Channel",
            "url": " https://example.test/channel ",
        })

        self.assertEqual(opts.name, "Channel")
        self.assertEqual(opts.url, "https://example.test/channel")
        self.assertEqual(opts.resolution, "720")
        self.assertFalse(opts.auto_transcribe)
        self.assertEqual(opts.mode, "new")
        self.assertFalse(opts.split_years)

    def test_sync_options_upgrade_legacy_durations(self) -> None:
        stream = mock.Mock()
        persisted = []
        channel = {
            "name": "Channel",
            "url": "https://example.test/channel",
            "min_duration": 30,
            "max_duration": 45,
            "mode": "FromDate",
            "from_date": "2026-01-01",
            "split_years": True,
            "split_months": True,
            "auto_transcribe": True,
        }

        opts = normalize_channel_sync_options(
            channel,
            stream=stream,
            persist_migration=lambda url, mn, mx: persisted.append(
                (url, mn, mx)),
        )

        self.assertEqual(opts.min_duration, 60)
        self.assertEqual(opts.max_duration, 60)
        self.assertTrue(opts.migrated_min_duration)
        self.assertTrue(opts.migrated_max_duration)
        self.assertEqual(channel["min_duration"], 60)
        self.assertEqual(channel["max_duration"], 60)
        self.assertEqual(persisted,
                         [("https://example.test/channel", True, True)])
        self.assertEqual(stream.emit_dim.call_count, 2)
        self.assertEqual(opts.mode, "fromdate")
        self.assertEqual(opts.from_date, "2026-01-01")
        self.assertTrue(opts.split_years)
        self.assertTrue(opts.split_months)
        self.assertTrue(opts.auto_transcribe)

    def test_sync_options_date_after_normalization(self) -> None:
        self.assertEqual(
            normalized_date_after("fromdate", "2026-06-12"),
            "20260612",
        )
        self.assertEqual(
            normalized_date_after("date", "2026/06/12 extra"),
            "20260612",
        )
        self.assertEqual(normalized_date_after("new", "2026-06-12"), "")
        self.assertEqual(normalized_date_after("fromdate", "bad-date"), "")

    def test_sync_options_match_filter(self) -> None:
        self.assertEqual(
            build_match_filter(0, 0),
            "!is_live & !is_upcoming",
        )
        self.assertEqual(
            build_match_filter(60, 3600),
            "!is_live & !is_upcoming & duration>?60 & duration<?3600",
        )

    def test_sync_options_output_template(self) -> None:
        root = Path("C:/Archive/Channel")
        self.assertEqual(
            build_output_template(root, False, False),
            str(root / "%(title)s.%(ext)s"),
        )
        self.assertEqual(
            build_output_template(root, True, False),
            str(root
                / "%(upload_date>%Y|Unknown Year)s"
                / "%(title)s.%(ext)s"),
        )
        self.assertEqual(
            build_output_template(root, True, True),
            str(root
                / "%(upload_date>%Y|Unknown Year)s"
                / "%(upload_date>%m %B|Unknown Month)s"
                / "%(title)s.%(ext)s"),
        )


class YtDlpEventsTests(unittest.TestCase):
    def test_extract_video_id_from_line_rejects_all_alpha_false_hit(self) -> None:
        self.assertEqual(
            extract_video_id_from_line("[download] Destination: file.mp4"),
            "",
        )

    def test_extract_video_id_from_line_accepts_realistic_id(self) -> None:
        self.assertEqual(
            extract_video_id_from_line("[youtube] Abc123_def4: Downloading"),
            "Abc123_def4",
        )

    def test_verbose_chatter_line_detection(self) -> None:
        self.assertTrue(is_verbose_chatter_line("  [Merger] Merging formats"))
        self.assertTrue(is_verbose_chatter_line("[info] Writing video metadata"))
        self.assertTrue(is_verbose_chatter_line("Deleting original file x"))
        self.assertFalse(is_verbose_chatter_line("[download] 42.0%"))


class TextUtilsTests(unittest.TestCase):
    def test_archive_visibility_hides_audio_sidecars(self) -> None:
        self.assertTrue(utils._archive_file_should_be_visible("Video.mp4"))
        self.assertTrue(utils._archive_file_should_be_visible(
            "Channel Transcript.txt"))
        self.assertFalse(utils._archive_file_should_be_visible("audio.m4a"))
        self.assertFalse(utils._archive_file_should_be_visible(
            "rawtranscript.txt"))

    def test_extract_video_id_accepts_all_alpha_db_fallback(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            path = r"Z:\Archive\Channel\Video.mp4"
            conn.execute("CREATE TABLE videos(filepath TEXT, video_id TEXT)")
            conn.execute("INSERT INTO videos(filepath, video_id) VALUES (?, ?)",
                         (path, "ABCDEFGHIJK"))

            self.assertEqual(
                text_utils.extract_video_id(
                    path,
                    conn=conn,
                    reject_alpha_only=True,
                ),
                "ABCDEFGHIJK",
            )
        finally:
            conn.close()


class LivestreamDetectionTests(unittest.TestCase):
    def test_line_looks_live_requires_ytdlp_context(self) -> None:
        self.assertFalse(livestreams.line_looks_live(
            "The event will begin at midnight"))
        self.assertFalse(livestreams.line_looks_live(
            "[download] Destination: The event will begin at midnight.mp4"))

    def test_line_looks_live_accepts_ytdlp_live_messages(self) -> None:
        self.assertTrue(livestreams.line_looks_live(
            "ERROR: [youtube] abc123_def4: This live event will begin at 7 PM"))
        self.assertTrue(livestreams.line_looks_live(
            "[info] abc123_def4: premieres in 2 hours"))


class ChannelMixinTests(unittest.TestCase):
    def test_channel_name_coercion_rejects_bad_bridge_args(self) -> None:
        mixin = ChannelMixin()

        self.assertEqual(mixin._coerce_channel_name(None), "")
        self.assertEqual(mixin._coerce_channel_name(42), "")
        self.assertEqual(mixin._coerce_channel_name({"folder": "  Chan  "}),
                         "Chan")
        self.assertEqual(
            mixin.chan_open_url(None),
            {"ok": False, "error": "Invalid channel argument"},
        )

    def test_chan_open_folder_uses_folder_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            override_dir = Path(td) / "Actual_ Folder"
            override_dir.mkdir()
            mixin = ChannelMixin()

            with mock.patch.object(channel_mixin, "load_config",
                                   return_value={"output_dir": td}), \
                    mock.patch.object(channel_mixin.subs_backend,
                                      "get_channel",
                                      return_value={
                                          "name": "Display Name",
                                          "folder_override": "Actual: Folder",
                                      }), \
                    mock.patch.object(channel_mixin.os.path, "isdir",
                                      wraps=os.path.isdir), \
                    mock.patch.object(channel_mixin.os, "startfile",
                                      create=True) as startfile:
                result = mixin.chan_open_folder("Display Name")

            self.assertTrue(result["ok"])
            startfile.assert_called_once_with(str(override_dir))

    def test_channel_folder_for_name_resolves_path(self) -> None:
        # T347: shared preamble helper returns (ch, folder) on success.
        mixin = ChannelMixin()
        with mock.patch.object(channel_mixin, "load_config",
                               return_value={"output_dir": r"C:\Archive"}), \
                mock.patch.object(channel_mixin.subs_backend, "get_channel",
                                  return_value={"name": "Chan"}):
            resolved = mixin._channel_folder_for_name("Chan")
        self.assertIsInstance(resolved, tuple)
        ch, folder = resolved
        self.assertEqual(ch["name"], "Chan")
        self.assertEqual(os.path.basename(folder), "Chan")

    def test_channel_folder_for_name_errors(self) -> None:
        mixin = ChannelMixin()
        # Channel not found.
        with mock.patch.object(channel_mixin.subs_backend, "get_channel",
                               return_value=None):
            self.assertEqual(
                mixin._channel_folder_for_name("Nope"),
                {"ok": False, "error": "Channel not found"})
        # output_dir unset.
        with mock.patch.object(channel_mixin, "load_config",
                               return_value={"output_dir": ""}), \
                mock.patch.object(channel_mixin.subs_backend, "get_channel",
                                  return_value={"name": "Chan"}):
            self.assertEqual(
                mixin._channel_folder_for_name("Chan"),
                {"ok": False, "error": "output_dir not set"})


class SyncStartLockTests(unittest.TestCase):
    def test_locked_start_refuses_to_replace_live_sync_thread(self) -> None:
        class Api(sync_mixin.SyncMixin):
            def __init__(self):
                self._sync_thread = None

        started = []

        class FakeThread:
            def __init__(self, target, daemon=False):
                self.target = target
                self.daemon = daemon
                self.alive = False

            def start(self):
                self.alive = True
                started.append(self)

            def is_alive(self):
                return self.alive

        api = Api()
        with mock.patch.object(sync_mixin.threading, "Thread", FakeThread):
            self.assertTrue(api._start_sync_thread_locked(lambda: None))
            first_thread = api._sync_thread

            self.assertFalse(api._start_sync_thread_locked(lambda: None))

        self.assertIs(api._sync_thread, first_thread)
        self.assertEqual(len(started), 1)


class DiagnosticsMixinTests(unittest.TestCase):
    def test_check_dependencies_uses_managed_probe_for_core_tools(self) -> None:
        class Api(diagnostics_mixin.DiagnosticsMixin):
            def __init__(self):
                self._log_stream = mock.Mock()
                self._window = None
                self._transcribe = mock.Mock(_python311=r"C:\Python311\python.exe")
                self.services = mock.Mock()

        probe = {
            "ytdlp": {"ok": True, "path": r"C:\Managed\yt-dlp.exe"},
            "ffmpeg": {"ok": True, "path": r"C:\Managed\ffmpeg.exe"},
            "ffprobe": {"ok": True, "path": r"C:\Managed\ffprobe.exe"},
        }

        with mock.patch("backend.deps_installer.probe",
                        return_value=probe):
            result = Api().check_dependencies()

        rows = {row["name"]: row for row in result["rows"]}
        self.assertTrue(rows["yt-dlp"]["ok"])
        self.assertEqual(rows["yt-dlp"]["detail"], r"C:\Managed\yt-dlp.exe")
        self.assertTrue(rows["ffmpeg"]["ok"])
        self.assertEqual(rows["ffmpeg"]["detail"], r"C:\Managed\ffmpeg.exe")
        self.assertTrue(rows["ffprobe"]["ok"])
        self.assertEqual(rows["ffprobe"]["detail"], r"C:\Managed\ffprobe.exe")

    def test_diagnostics_mixin_prefers_app_services_dependencies(self) -> None:
        service_log = mock.Mock()

        with tempfile.TemporaryDirectory() as tmp:
            service_channels = [{
                "name": "Service Channel",
                "url": "https://www.youtube.com/@service",
                "initialized": True,
            }]

            class Api(diagnostics_mixin.DiagnosticsMixin):
                def __init__(self):
                    self._config = {
                        "output_dir": "",
                        "channels": [{"name": "Stale Channel",
                                      "initialized": True}],
                    }
                    self._log_stream = mock.Mock()
                    self.services = AppServices(
                        load_config=lambda: {
                            "output_dir": tmp,
                            "channels": list(service_channels),
                        },
                        save_config=lambda cfg: True,
                        queues=mock.Mock(),
                        log_stream=service_log,
                        transcribe=mock.Mock(),
                        event_bus=mock.Mock(),
                    )

            with mock.patch("backend.api_mixins.diagnostics_mixin.load_config",
                            side_effect=AssertionError("use services")):
                api = Api()
                result = api.check_channel_folders()

        self.assertTrue(result["ok"])
        self.assertEqual(len(result["missing"]), 1)
        self.assertEqual(result["missing"][0]["name"], "Service Channel")
        service_log.emit.assert_called_once()
        service_log.flush.assert_called_once()
        api._log_stream.emit.assert_not_called()
        api._log_stream.flush.assert_not_called()


class ThumbnailMixinTests(unittest.TestCase):
    def test_realign_poll_recovers_partial_lazy_state(self) -> None:
        api = ThumbnailMixin()
        api._realign_jobs = {}

        result = api.realign_poll("missing-token")

        self.assertEqual(result, {"ok": False, "error": "unknown token"})
        self.assertTrue(hasattr(api, "_realign_jobs_lock"))

    def test_thumbnail_mixin_prefers_app_services_dependencies(self) -> None:
        class InlineThread:
            def __init__(self, target, *args, **kwargs):
                self.target = target

            def start(self):
                self.target()

        service_channels = [{"name": "Service Channel"}]
        service_log = mock.Mock()

        class Api(ThumbnailMixin):
            def __init__(self):
                self._config = {"channels": [{"name": "Stale Channel"}]}
                self._log_stream = mock.Mock()
                self.services = AppServices(
                    load_config=lambda: {"channels": list(service_channels)},
                    save_config=lambda cfg: True,
                    queues=mock.Mock(),
                    log_stream=service_log,
                    transcribe=mock.Mock(),
                    event_bus=mock.Mock(),
                )

        api = Api()
        with mock.patch("backend.api_mixins.thumbnail_mixin.load_config",
                        side_effect=AssertionError("use services")), \
                mock.patch("backend.api_mixins.thumbnail_mixin"
                           ".subs_backend.get_channel",
                           return_value=service_channels[0]), \
                mock.patch.object(thumbnail_mixin.threading, "Thread",
                                  side_effect=InlineThread), \
                mock.patch("backend.metadata.count_thumbnail_status_bulk",
                           return_value={"Service Channel": {
                               "total": 1,
                               "with_thumb": 1,
                               "missing": 0,
                           }}) as count_bulk, \
                mock.patch("backend.metadata.sweep_missing_thumbnails",
                           return_value={"fetched": 1,
                                         "missing": 0,
                                         "checked": 1}) as sweep, \
                mock.patch("backend.metadata.realign_misplaced_thumbnails",
                           return_value={"ok": True,
                                         "moved": 0}) as realign:
            status = api.thumbnail_status_bulk(force=True)
            single = api.refetch_thumbnails("Service Channel")
            bulk = api.refetch_thumbnails_all()
            realign_started = api.realign_start(dry_run=False)

        self.assertEqual(
            status["rows"]["Service Channel"]["missing"], 0)
        count_bulk.assert_called_once_with(service_channels, force=True)
        self.assertTrue(single["started"])
        self.assertTrue(bulk["started"])
        self.assertEqual(bulk["channels"], 1)
        self.assertEqual(sweep.call_count, 2)
        self.assertIs(sweep.call_args_list[0].kwargs["stream"], service_log)
        self.assertIs(sweep.call_args_list[1].kwargs["stream"], service_log)
        self.assertTrue(realign_started["started"])
        realign.assert_called_once()
        self.assertEqual(realign.call_args.kwargs["channels"],
                         service_channels)
        self.assertFalse(realign.call_args.kwargs["dry_run"])
        self.assertIs(realign.call_args.kwargs["stream"], service_log)
        service_log.emit_text.assert_called()
        service_log.flush.assert_called()
        api._log_stream.emit_text.assert_not_called()
        api._log_stream.flush.assert_not_called()


class BrowsePreloadPriorityTests(unittest.TestCase):
    def test_preload_all_channels_waits_while_low_priority_busy(self) -> None:
        busy_checks = {"count": 0}
        preload_calls: list[tuple[str, object]] = []

        def busy() -> bool:
            busy_checks["count"] += 1
            return busy_checks["count"] <= 2

        def fake_preload(channel, **kwargs):
            preload_calls.append((channel, kwargs.get("low_priority_busy_fn")))
            return 4

        with mock.patch.object(index, "preload_channel_videos",
                               side_effect=fake_preload), \
                mock.patch.object(index.time, "sleep") as sleep, \
                mock.patch.object(index, "list_all_videos",
                                  return_value={"rows": []}):
            result = index.preload_all_channels(
                ["Channel A"], low_priority_busy_fn=busy)

        self.assertEqual(result, {"Channel A": 4})
        self.assertEqual([c for c, _ in preload_calls], ["Channel A"])
        self.assertIs(preload_calls[0][1], busy)
        self.assertGreaterEqual(sleep.call_count, 2)

    def test_preload_all_channels_retries_after_interruption(self) -> None:
        attempts = {"count": 0}

        def fake_preload(channel, **kwargs):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise index._LowPriorityInterrupted()
            return 7

        with mock.patch.object(index, "preload_channel_videos",
                               side_effect=fake_preload), \
                mock.patch.object(index.time, "sleep"), \
                mock.patch.object(index, "list_all_videos",
                                  return_value={"rows": []}):
            result = index.preload_all_channels(
                ["Channel A"],
                low_priority_busy_fn=lambda: False)

        self.assertEqual(result, {"Channel A": 7})
        self.assertEqual(attempts["count"], 2)


class SubsTests(unittest.TestCase):
    class _InlineThread:
        def __init__(self, target, *args, **kwargs):
            self.target = target

        def start(self):
            self.target()

    def test_relocate_channel_rejects_realpath_escape(self) -> None:
        class Api(subs_mixin.SubsMixin):
            def _reload_config(self):
                pass

        with tempfile.TemporaryDirectory() as td:
            base = Path(td) / "Archive"
            outside = Path(td) / "Outside"
            base.mkdir()
            outside.mkdir()
            target = base / "Link"
            target.mkdir()

            def fake_realpath(path):
                if os.path.normpath(str(path)) == os.path.normpath(str(target)):
                    return str(outside)
                return os.path.abspath(path)

            cfg = {"output_dir": str(base),
                   "channels": [{"url": "u", "name": "n"}]}
            with mock.patch.object(subs_mixin, "load_config",
                                   return_value=cfg), \
                    mock.patch.object(subs_mixin.os.path, "realpath",
                                      side_effect=fake_realpath):
                result = Api().subs_relocate_channel(
                    {"url": "u"}, target.name)

        self.assertFalse(result["ok"])
        self.assertIn("directly under output_dir", result["error"])

    def test_add_channel_persists_atomically_via_transaction(self) -> None:
        # T123: add_channel now does dup-check + append + save inside one
        # config_transaction. Happy path must still land on disk.
        with tempfile.TemporaryDirectory() as td:
            cfg_file = Path(td) / "ytarchiver_config.json"
            cfg_file.write_text(json.dumps(
                {"channels": [], "output_dir": str(td)}), encoding="utf-8")
            with mock.patch.object(ytarchiver_config, "APP_DATA_DIR",
                                   Path(td)), \
                    mock.patch.object(ytarchiver_config, "CONFIG_FILE",
                                      cfg_file):
                result = subs.add_channel(
                    {"url": "@valid_handle", "name": "Valid Handle"})
                loaded = ytarchiver_config.load_config()

        self.assertEqual(result.get("name"), "Valid Handle")
        self.assertNotIn("_write_blocked", result)
        self.assertEqual(loaded["channels"][0]["name"], "Valid Handle")

    def test_add_channel_reports_write_block_on_save_failure(self) -> None:
        # config_transaction raises OSError when the atomic save fails;
        # add_channel must translate that to the {_write_blocked} contract
        # the UI relies on (not propagate the exception).
        with tempfile.TemporaryDirectory() as td:
            cfg_file = Path(td) / "ytarchiver_config.json"
            cfg_file.write_text(json.dumps(
                {"channels": [], "output_dir": str(td)}), encoding="utf-8")
            with mock.patch.object(ytarchiver_config, "APP_DATA_DIR",
                                   Path(td)), \
                    mock.patch.object(ytarchiver_config, "CONFIG_FILE",
                                      cfg_file), \
                    mock.patch.object(ytarchiver_config, "save_config",
                                      return_value=False):
                result = subs.add_channel(
                    {"url": "@valid_handle", "name": "Valid Handle"})

        self.assertTrue(result.get("_write_blocked"))

    def test_validate_channel_url_rejects_spoofed_youtube_hosts(self) -> None:
        for bad in (
                "https://youtube.com.evil.example/@bad",
                "https://notyoutube.com/@bad",
                "not a handle",
        ):
            ok, _err = subs.validate_channel_url(bad)
            self.assertFalse(ok, bad)

    def test_validate_channel_url_accepts_bare_handle(self) -> None:
        ok, err = subs.validate_channel_url("@valid_handle")

        self.assertTrue(ok, err)

    def test_probe_channel_url_normalizes_preview_inputs(self) -> None:
        normalized, err = subs_mixin._normalize_probe_channel_url("@valid_handle")

        self.assertFalse(err)
        self.assertEqual(normalized, "https://www.youtube.com/@valid_handle")

    def test_probe_channel_url_rejects_video_and_playlist_inputs(self) -> None:
        for bad in (
                "https://www.youtube.com/watch?v=abc123_def45",
                "https://www.youtube.com/playlist?list=PL123",
                "https://youtube.com.evil.example/@bad",
                "not a handle",
        ):
            normalized, err = subs_mixin._normalize_probe_channel_url(bad)
            self.assertEqual(normalized, "", bad)
            self.assertTrue(err, bad)

    def test_remove_channel_clears_disk_and_id_caches_after_save(self) -> None:
        cfg = {
            "channels": [{
                "name": "Channel",
                "url": "https://www.youtube.com/@example",
            }],
        }

        with mock.patch.object(subs, "load_config", return_value=cfg), \
                mock.patch.object(subs, "save_config", return_value=True), \
                mock.patch.object(archive_scan, "invalidate_channel") as disk, \
                mock.patch("backend.channel_cache.clear") as id_cache:
            result = subs.remove_channel(
                {"url": "https://www.youtube.com/@example"},
                delete_files=False,
            )

        self.assertTrue(result["ok"])
        disk.assert_called_once_with("https://www.youtube.com/@example")
        id_cache.assert_called_once_with("https://www.youtube.com/@example")

    def test_subs_queue_all_worker_uses_fresh_config(self) -> None:
        class Api(subs_mixin.SubsMixin):
            pass

        api = Api()
        api._config = {"channels": [{"name": "Stale"}]}
        api._log_stream = mock.Mock()
        api._window = None
        api.chan_transcribe_all = mock.Mock(return_value={
            "ok": True, "queued": 1})

        with mock.patch.object(subs_mixin, "load_config",
                               return_value={
                                   "channels": [{"name": "Fresh"}],
                               }), \
                mock.patch.object(subs_mixin.threading, "Thread",
                                  self._InlineThread):
            result = api.subs_queue_all()

        self.assertEqual(result, {"ok": True, "started": True})
        api.chan_transcribe_all.assert_called_once_with("Fresh")

    def test_subs_queue_pending_worker_uses_fresh_config(self) -> None:
        class Api(subs_mixin.SubsMixin):
            pass

        api = Api()
        api._config = {"channels": [{"name": "Stale"}]}
        api._log_stream = mock.Mock()
        api._window = None
        api._on_queue_changed = mock.Mock()
        api.sync_is_running = mock.Mock(return_value=False)
        api.chan_transcribe_pending = mock.Mock(return_value={
            "ok": True, "queued": 1})

        cfg0 = {"output_dir": "Z:/Archive", "channels": []}
        fresh = {
            "output_dir": "Z:/Archive",
            "autorun_sync": False,
            "channels": [{
                "name": "Fresh",
                "url": "https://youtube.com/@fresh",
                "pending_tx_ids": ["abc123_def4"],
            }],
        }
        with mock.patch.object(subs_mixin, "load_config",
                               side_effect=[cfg0, fresh, fresh]), \
                mock.patch.object(subs_mixin.threading, "Thread",
                                  self._InlineThread):
            result = api.subs_queue_pending()

        self.assertEqual(result, {"ok": True, "started": True})
        api.chan_transcribe_pending.assert_called_once_with("Fresh")
        api._on_queue_changed.assert_called_once()


class MetadataMixinTests(unittest.TestCase):
    def test_metadata_choice_resolve_uses_token_map_under_lock(self) -> None:
        class Api(metadata_mixin.MetadataMixin):
            pass

        api = Api()
        api._pending_metadata_choices = {}
        api._pending_metadata_choices_lock = threading.Lock()
        first = {"val": None, "event": threading.Event()}
        second = {"val": None, "event": threading.Event()}
        api._pending_metadata_choices["a"] = first
        api._pending_metadata_choices["b"] = second

        result = api.metadata_choice_resolve("b", "overwrite")

        self.assertTrue(result["ok"])
        self.assertIsNone(first["val"])
        self.assertFalse(first["event"].is_set())
        self.assertEqual(second["val"], "overwrite")
        self.assertTrue(second["event"].is_set())

    def test_metadata_queue_all_prefers_app_services_dependencies(self) -> None:
        service_queues = mock.Mock()
        service_queues.sync_paused = False
        service_queues.sync_enqueue.return_value = True
        service_log = mock.Mock()

        class Api(metadata_mixin.MetadataMixin):
            def __init__(self):
                self._config = {"channels": []}
                self._queues = mock.Mock()
                self._log_stream = mock.Mock()
                self._on_queue_changed = mock.Mock()
                self._maybe_autostart_sync = mock.Mock(return_value=True)
                self.services = AppServices(
                    load_config=lambda: {
                        "channels": [
                            {"name": "Bravo", "url": "b"},
                            {"name": "Alpha", "url": "a"},
                        ],
                    },
                    save_config=lambda cfg: True,
                    queues=service_queues,
                    log_stream=service_log,
                    transcribe=mock.Mock(),
                    event_bus=mock.Mock(),
                )

        api = Api()
        with mock.patch("backend.api_mixins.metadata_mixin.load_config",
                        side_effect=AssertionError("use services")):
            result = api.metadata_queue_all(refresh=True)

        self.assertEqual(result, {
            "ok": True,
            "queued": 2,
            "channels": 2,
            "started": True,
            "paused": False,
        })
        self.assertEqual(
            [call.args[0]["name"]
             for call in service_queues.sync_enqueue.call_args_list],
            ["Alpha", "Bravo"])
        self.assertTrue(all(call.args[0]["refresh"]
                            for call in service_queues.sync_enqueue.call_args_list))
        service_log.emit_text.assert_called_once()
        service_log.flush.assert_called_once()
        api._queues.sync_enqueue.assert_not_called()
        api._log_stream.emit_text.assert_not_called()
        api._on_queue_changed.assert_called_once()
        api._maybe_autostart_sync.assert_called_once()

    def test_metadata_queue_year_prefers_app_services_dependencies(self) -> None:
        service_queues = mock.Mock()
        service_queues.sync_paused = True
        service_log = mock.Mock()

        class Api(metadata_mixin.MetadataMixin):
            def __init__(self):
                self._queues = mock.Mock()
                self._log_stream = mock.Mock()
                self._on_queue_changed = mock.Mock()
                self._maybe_autostart_sync = mock.Mock(return_value=False)
                self.services = AppServices(
                    load_config=lambda: {},
                    save_config=lambda cfg: True,
                    queues=service_queues,
                    log_stream=service_log,
                    transcribe=mock.Mock(),
                    event_bus=mock.Mock(),
                )

        with mock.patch.object(metadata_mixin.subs_backend, "get_channel",
                               return_value={"name": "Fresh", "url": "u"}):
            result = Api().metadata_queue_channel_year(
                {"name": "Fresh"}, "2024", refresh=False)

        self.assertEqual(result, {
            "ok": True,
            "queued": True,
            "year": 2024,
            "refresh": False,
            "started": False,
            "paused": True,
        })
        task = service_queues.sync_enqueue.call_args.args[0]
        self.assertEqual(task["kind"], "metadata")
        self.assertEqual(task["scope"], {"year": 2024})
        service_log.emit_text.assert_called_once()
        service_log.flush.assert_called_once()

class BrowseMixinTests(unittest.TestCase):
    def tearDown(self) -> None:
        browse_mixin._METADATA_DRAWER_CACHE.clear()

    def test_search_context_clamps_before_after(self) -> None:
        class Api(BrowseMixin):
            pass

        api = Api()
        with mock.patch.object(browse_mixin.index_backend,
                               "get_segment_context",
                               return_value={"ok": True}) as ctx:
            api.browse_search_context({
                "segment_id": 1,
                "before": 100000,
                "after": -5,
                "query": "well-known",
            })

        ctx.assert_called_once_with(1, 500, 0, "well-known")

    def test_video_metadata_drawer_uses_cache_for_unchanged_jsonl(self) -> None:
        class Api(BrowseMixin):
            pass

        video_id = "ABCDEFGHIJK"
        api = Api()
        api._config = {}
        browse_mixin._METADATA_DRAWER_CACHE.clear()
        with tempfile.TemporaryDirectory() as td:
            channel_dir = Path(td) / "Channel"
            month_dir = channel_dir / "2026" / "06 June"
            month_dir.mkdir(parents=True)
            video = month_dir / f"Video [{video_id}].mp4"
            video.write_bytes(b"video")
            metadata_path = channel_dir / ".Channel Metadata.jsonl"
            metadata_path.write_text("placeholder", encoding="utf-8")

            with mock.patch("backend.metadata._read_metadata_jsonl",
                            return_value={video_id: {"title": "Video"}}) as read:
                first = api.browse_get_video_metadata({
                    "filepath": str(video),
                    "video_id": video_id,
                    "channel": "Channel",
                })
                second = api.browse_get_video_metadata({
                    "filepath": str(video),
                    "video_id": video_id,
                    "channel": "Channel",
                })

        self.assertTrue(first["ok"])
        self.assertEqual(second["meta"], {"title": "Video"})
        read.assert_called_once_with(str(metadata_path))

    def test_transcript_source_classifier_tail_scans_large_file(self) -> None:
        class Api(BrowseMixin):
            pass

        api = Api()
        with tempfile.TemporaryDirectory() as td, \
                mock.patch.object(
                    browse_mixin, "_TRANSCRIPT_SOURCE_SCAN_BYTES", 128):
            video_dir = Path(td) / "Channel" / "2026"
            video_dir.mkdir(parents=True)
            jsonl_path = video_dir / "Video.jsonl"
            jsonl_path.write_text("{}", encoding="utf-8")
            transcript = video_dir / "Channel 2026 Transcript.txt"
            transcript.write_text(
                "===(Video), (2026-01-01), (00:00), "
                "(YT_CAPTIONS)===\n"
                + ("body\n" * 80)
                + "===(Video), (2026-01-02), (00:00), "
                "(WHISPER:large-v3)===\n",
                encoding="utf-8")

            result = api._classify_transcript_source(
                "Video", str(jsonl_path), "")

        self.assertEqual(result["source"], "whisper")
        self.assertEqual(result["raw"], "WHISPER:large-v3")

    def test_browse_video_url_rejects_outside_managed_roots(self) -> None:
        class Api(BrowseMixin):
            pass

        with tempfile.TemporaryDirectory() as td:
            video = Path(td) / "outside.mp4"
            video.write_bytes(b"video")
            with mock.patch.object(
                    browse_mixin.file_ops, "assert_within_managed_roots",
                    return_value={"ok": False,
                                  "error": "outside managed roots"}):
                result = Api().browse_video_url(str(video))

        self.assertFalse(result["ok"])
        self.assertIn("outside", result["error"])

    def test_browse_resolve_segment_rejects_outside_managed_roots(self) -> None:
        class Api(BrowseMixin):
            pass

        with tempfile.TemporaryDirectory() as td:
            base = Path(td) / "Video"
            jsonl = base.with_suffix(".jsonl")
            video = base.with_suffix(".mp4")
            jsonl.write_text("{}", encoding="utf-8")
            video.write_bytes(b"video")
            with mock.patch.object(
                    browse_mixin.file_ops, "assert_within_managed_roots",
                    return_value={"ok": False,
                                  "error": "outside managed roots"}):
                result = Api().browse_resolve_segment(str(jsonl))

        self.assertFalse(result["ok"])
        self.assertIn("outside", result["error"])

    def test_browse_mixin_prefers_app_services_for_manual_videos(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "Manual Video.mp4"
            video.write_bytes(b"video")
            (Path(tmp) / "Notes.txt").write_text("skip", encoding="utf-8")

            class Api(BrowseMixin):
                def __init__(self):
                    self._config = {"video_out_dir": ""}
                    self.services = AppServices(
                        load_config=lambda: {"video_out_dir": tmp},
                        save_config=lambda cfg: True,
                        queues=mock.Mock(),
                        log_stream=mock.Mock(),
                        transcribe=mock.Mock(),
                        event_bus=mock.Mock(),
                    )

            with mock.patch("backend.api_mixins.browse_mixin.load_config",
                            side_effect=AssertionError("use services")):
                result = Api().list_manual_videos(sort="title")

        self.assertEqual(result["folder"], tmp)
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["rows"][0]["title"], "Manual Video")

    def test_browse_refresh_metadata_prefers_app_services_dependencies(self) -> None:
        video_id = "abc123def45"
        service_log = mock.Mock()

        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / f"Video [{video_id}].mp4"
            video.write_bytes(b"video")

            class Api(BrowseMixin):
                def __init__(self):
                    self._config = {"channels": [{"name": "Stale"}]}
                    self._log_stream = mock.Mock()
                    self._push_recent_refresh = mock.Mock()
                    self.services = AppServices(
                        load_config=lambda: {
                            "output_dir": tmp,
                            "channels": [{
                                "name": "Fresh Channel",
                                "url": "https://www.youtube.com/@fresh",
                            }],
                        },
                        save_config=lambda cfg: True,
                        queues=mock.Mock(),
                        log_stream=service_log,
                        transcribe=mock.Mock(),
                        event_bus=mock.Mock(),
                    )

            api = Api()
            with mock.patch("backend.api_mixins.browse_mixin.load_config",
                            side_effect=AssertionError("use services")), \
                    mock.patch("backend.metadata.fetch_single_video_metadata",
                               return_value={
                                   "ok": True,
                                   "entry": {"title": "Fresh Metadata"},
                               }) as fetch:
                result = api.browse_refresh_video_metadata({
                    "filepath": str(video),
                    "video_id": video_id,
                    "title": "Video",
                    "channel": "Fresh Channel",
                })

        self.assertTrue(result["ok"])
        self.assertEqual(result["meta"], {"title": "Fresh Metadata"})
        self.assertEqual(fetch.call_args.args[:4], (
            {
                "name": "Fresh Channel",
                "url": "https://www.youtube.com/@fresh",
            },
            video_id,
            str(video),
            "Video",
        ))
        self.assertIs(fetch.call_args.args[4], service_log)
        api._log_stream.emit_text.assert_not_called()
        api._push_recent_refresh.assert_called_once()


class RecentMixinTests(unittest.TestCase):
    def test_recent_mixin_prefers_app_services_config(self) -> None:
        saved: list[dict] = []

        class Api(RecentMixin):
            def __init__(self):
                self._config = {
                    "recent_downloads": [{
                        "title": "Stale",
                        "channel": "Old",
                        "download_ts": 1,
                    }],
                }
                self._reload_config = mock.Mock()
                self.services = AppServices(
                    load_config=lambda: {
                        "channels": [{"name": "Fresh Channel"}],
                        "recent_downloads": [{
                            "title": "Fresh",
                            "channel": "Fresh Channel",
                            "download_ts": 2,
                        }],
                    },
                    save_config=lambda cfg: saved.append(dict(cfg)) or True,
                    queues=mock.Mock(),
                    log_stream=mock.Mock(),
                    transcribe=mock.Mock(),
                    event_bus=mock.Mock(),
                )

        api = Api()
        with mock.patch("backend.api_mixins.recent_mixin.load_config",
                        side_effect=AssertionError("use services")), \
                mock.patch("backend.api_mixins.recent_mixin.save_config",
                           side_effect=AssertionError("use services")):
            rows = api.get_recent_downloads()
            cleared = api.clear_recent_downloads()

        self.assertEqual(rows[0]["title"], "Fresh")
        self.assertTrue(cleared["ok"])
        self.assertEqual(saved[-1]["recent_downloads"], [])
        api._reload_config.assert_called_once()

    def test_list_all_videos_clamps_limit_and_coerces_offset(self) -> None:
        mixin = RecentMixin()

        with mock.patch("backend.api_mixins.recent_mixin.index_backend"
                        ".list_all_videos",
                        return_value={"rows": [], "has_more": False,
                                      "offset": 0}) as list_all:
            result = mixin.list_all_videos(limit="10000000",
                                           offset="-5")

        self.assertEqual(result["offset"], 0)
        kwargs = list_all.call_args.kwargs
        self.assertEqual(kwargs["limit"], 1000)
        self.assertEqual(kwargs["offset"], 0)

    def test_list_all_videos_error_returns_coerced_offset(self) -> None:
        mixin = RecentMixin()

        with mock.patch("backend.api_mixins.recent_mixin.index_backend"
                        ".list_all_videos",
                        side_effect=RuntimeError("boom")):
            result = mixin.list_all_videos(limit="bad", offset="bad")

        self.assertEqual(result["rows"], [])
        self.assertFalse(result["has_more"])
        self.assertEqual(result["offset"], 0)
        self.assertIn("boom", result["error"])

    def test_recent_delete_payload_removes_only_matching_filepath(self) -> None:
        mixin = RecentMixin()
        with tempfile.TemporaryDirectory() as td:
            fp1 = str(Path(td) / "one.mp4")
            fp2 = str(Path(td) / "two.mp4")
            Path(fp1).write_text("one", encoding="utf-8")
            Path(fp2).write_text("two", encoding="utf-8")
            cfg = {
                "recent_downloads": [
                    {"title": "Same", "channel": "Chan",
                     "filepath": fp1, "video_id": "AAAAAAAAAAA"},
                    {"title": "Same", "channel": "Chan",
                     "filepath": fp2, "video_id": "BBBBBBBBBBB"},
                ],
            }

            @contextlib.contextmanager
            def _tx():
                yield cfg

            with mock.patch("backend.api_mixins.recent_mixin.load_config",
                            return_value=cfg), \
                    mock.patch("backend.api_mixins.recent_mixin"
                               ".config_is_writable",
                               return_value=True), \
                    mock.patch("backend.ytarchiver_config.save_config",
                               return_value=True), \
                    mock.patch("backend.ytarchiver_config.config_transaction",
                               _tx), \
                    mock.patch("backend.services.file_ops.safe_trash_video_file",
                               return_value={"ok": True}), \
                    mock.patch("backend.index.delete_segments_for_video"), \
                    mock.patch("backend.index._open", return_value=None):
                result = mixin.recent_delete_file({
                    "title": "Same",
                    "channel": "Chan",
                    "filepath": fp1,
                    "video_id": "AAAAAAAAAAA",
                })

        self.assertTrue(result["ok"])
        self.assertEqual(len(cfg["recent_downloads"]), 1)
        self.assertEqual(cfg["recent_downloads"][0]["filepath"], fp2)

    def test_recent_delete_legacy_duplicate_is_ambiguous(self) -> None:
        mixin = RecentMixin()
        cfg = {
            "recent_downloads": [
                {"title": "Same", "channel": "Chan",
                 "filepath": r"C:\one.mp4", "video_id": "AAAAAAAAAAA"},
                {"title": "Same", "channel": "Chan",
                 "filepath": r"C:\two.mp4", "video_id": "BBBBBBBBBBB"},
            ],
        }

        with mock.patch("backend.api_mixins.recent_mixin.load_config",
                        return_value=cfg):
            result = mixin.recent_delete_file("Same", "Chan")

        self.assertFalse(result["ok"])
        self.assertIn("ambiguous", result["error"])

    def test_recent_delete_reports_partial_failure_when_index_cleanup_fails(self) -> None:
        mixin = RecentMixin()
        with tempfile.TemporaryDirectory() as td:
            fp = str(Path(td) / "one.mp4")
            Path(fp).write_text("one", encoding="utf-8")
            cfg = {
                "recent_downloads": [
                    {"title": "One", "channel": "Chan",
                     "filepath": fp, "video_id": "AAAAAAAAAAA"},
                ],
            }

            with mock.patch("backend.api_mixins.recent_mixin.load_config",
                            return_value=cfg), \
                    mock.patch("backend.api_mixins.recent_mixin"
                               ".config_is_writable",
                               return_value=False), \
                    mock.patch("backend.services.file_ops.safe_trash_video_file",
                               return_value={"ok": True}), \
                    mock.patch("backend.index.delete_segments_for_video",
                               side_effect=RuntimeError("db locked")):
                result = mixin.recent_delete_file({
                    "title": "One",
                    "channel": "Chan",
                    "filepath": fp,
                    "video_id": "AAAAAAAAAAA",
                })

        self.assertFalse(result["ok"])
        self.assertTrue(result["file_trashed"])
        self.assertTrue(result["cleanup_failed"])
        self.assertIn("db locked", result["error"])

    def test_recent_delete_reports_config_transaction_failure(self) -> None:
        mixin = RecentMixin()
        with tempfile.TemporaryDirectory() as td:
            fp = str(Path(td) / "one.mp4")
            Path(fp).write_text("one", encoding="utf-8")
            cfg = {
                "recent_downloads": [
                    {"title": "One", "channel": "Chan",
                     "filepath": fp, "video_id": "AAAAAAAAAAA"},
                ],
            }

            with mock.patch("backend.api_mixins.recent_mixin.load_config",
                            return_value=cfg), \
                    mock.patch("backend.api_mixins.recent_mixin"
                               ".config_is_writable",
                               return_value=True), \
                    mock.patch("backend.services.file_ops.safe_trash_video_file",
                               return_value={"ok": True}), \
                    mock.patch("backend.index.delete_segments_for_video"), \
                    mock.patch("backend.index._open", return_value=None), \
                    mock.patch("backend.ytarchiver_config.config_transaction",
                               side_effect=OSError("locked")):
                result = mixin.recent_delete_file({
                    "title": "One",
                    "channel": "Chan",
                    "filepath": fp,
                    "video_id": "AAAAAAAAAAA",
                })

        self.assertFalse(result["ok"])
        self.assertIn("config write failed", result["error"])


class InfoMixinServicesTests(unittest.TestCase):
    def test_info_mixin_prefers_app_services_for_fresh_config_paths(self) -> None:
        saved: list[dict] = []
        service_log = mock.Mock()

        def fresh_config():
            return {
                "channels": [{"name": "One"}, {"name": "Two"}],
                "output_dir": "FreshArchive",
                "url_history": [f"url-{i}" for i in range(25)],
                "last_sync": "2026-06-28 00:01",
            }

        class Api(InfoMixin):
            def __init__(self):
                self._config = {
                    "channels": [],
                    "output_dir": "stale-cache",
                    "url_history": ["stale"],
                    "last_sync": "",
                }
                self._log_stream = mock.Mock()
                self.services = AppServices(
                    load_config=fresh_config,
                    save_config=lambda cfg: saved.append(dict(cfg)) or True,
                    queues=mock.Mock(),
                    log_stream=service_log,
                    transcribe=mock.Mock(),
                    event_bus=mock.Mock(),
                )

            def ytdlp_version(self):
                return {"ok": True, "version": "2026.01.01"}

        api = Api()
        with mock.patch("backend.api_mixins.info_mixin.load_config",
                        side_effect=AssertionError("use services")), \
                mock.patch("backend.api_mixins.info_mixin.save_config",
                           side_effect=AssertionError("use services")), \
                mock.patch("backend.api_mixins.info_mixin.config_is_writable",
                           return_value=True):
            about = api.about_info()
            history = api.url_history()
            api._push_url_history("new-url")
            last_sync = api.get_last_sync_label()

        self.assertEqual(about["channels"], 2)
        self.assertEqual(about["output_dir"], "FreshArchive")
        self.assertEqual(about["ytdlp_version"], "2026.01.01")
        self.assertEqual(history, [f"url-{i}" for i in range(20)])
        self.assertEqual(saved[-1]["url_history"][0], "new-url")
        self.assertEqual(len(saved[-1]["url_history"]), 20)
        self.assertIn("Last Full Sync:", last_sync["label"])
        api._log_stream.emit_dim.assert_not_called()
        service_log.emit_dim.assert_not_called()


class VideoMixinTests(unittest.TestCase):
    def test_video_delete_reports_partial_failure_when_index_cleanup_fails(self) -> None:
        mixin = VideoMixin()
        with tempfile.TemporaryDirectory() as td:
            fp = str(Path(td) / "one.mp4")
            Path(fp).write_text("one", encoding="utf-8")

            with mock.patch("backend.services.file_ops.safe_trash_video_file",
                            return_value={"ok": True}), \
                    mock.patch("backend.index._open",
                               side_effect=RuntimeError("db locked")):
                result = mixin.video_delete_file(fp)

        self.assertFalse(result["ok"])
        self.assertTrue(result["file_trashed"])
        self.assertTrue(result["cleanup_failed"])
        self.assertIn("db locked", result["error"])

    def test_video_redownload_prefers_app_services_dependencies(self) -> None:
        service_queues = mock.Mock()
        service_log = mock.Mock()

        class Api(VideoMixin):
            def __init__(self):
                self._config = {"channels": [{"name": "Stale Channel"}]}
                self._queues = mock.Mock()
                self._log_stream = mock.Mock()
                self._sync_pause = threading.Event()
                self.services = AppServices(
                    load_config=lambda: {
                        "output_dir": r"X:\Archive",
                        "channels": [{
                            "name": "Fresh Channel",
                            "folder_override": "Fresh Folder",
                            "url": "https://www.youtube.com/@fresh",
                        }],
                    },
                    save_config=lambda cfg: True,
                    queues=service_queues,
                    log_stream=service_log,
                    transcribe=mock.Mock(),
                    event_bus=mock.Mock(),
                )

        class FakeReader:
            def execute(self, *args, **kwargs):
                return self

            def fetchone(self):
                return (r"X:\Archive\Fresh Folder\Video.mp4",
                        "Fresh Channel")

        class ImmediateThread:
            def __init__(self, target, daemon=False):
                self.target = target

            def start(self):
                self.target()

        with mock.patch("backend.index._reader_open",
                        return_value=FakeReader()), \
                mock.patch("backend.api_mixins.video_mixin.load_config",
                           side_effect=AssertionError("use services")), \
                mock.patch("backend.api_mixins.video_mixin"
                           ".threading.Thread",
                           ImmediateThread), \
                mock.patch("backend.redownload.redownload_channel") as run:
            result = Api().video_redownload("abc123def45", "Video", "720")

        self.assertTrue(result["ok"])
        self.assertEqual(run.call_args.args[:3], (
            "Fresh Channel",
            "https://www.youtube.com/@fresh",
            r"X:\Archive\Fresh Folder",
        ))
        self.assertEqual(run.call_args.args[3], "720")
        self.assertIs(run.call_args.kwargs["stream"], service_log)
        self.assertIs(run.call_args.kwargs["queues"], service_queues)
        self.assertEqual(run.call_args.kwargs["only_video_id"], "abc123def45")


class LogRowsTests(unittest.TestCase):
    def tearDown(self) -> None:
        log_rows._HIST_INDEX_BY_ROW_ID.clear()

    def test_persist_row_history_keeps_new_row_index_after_trim(self) -> None:
        cfg = {"autorun_history": []}

        with mock.patch("backend.ytarchiver_config.config_is_writable",
                        return_value=True), \
                mock.patch("backend.ytarchiver_config.load_config",
                           return_value=cfg), \
                mock.patch("backend.ytarchiver_config.save_config",
                           return_value=True), \
                mock.patch("backend.autorun.AUTORUN_HISTORY_MAX", 3):
            log_rows._persist_row_history("old0", "old0")
            log_rows._persist_row_history("old1", "old1")
            log_rows._persist_row_history("old2", "old2")
            log_rows._persist_row_history("new", "new0")

            self.assertEqual(cfg["autorun_history"],
                             ["old1", "old2", "new0"])
            self.assertNotIn("old0", log_rows._HIST_INDEX_BY_ROW_ID)
            self.assertEqual(log_rows._HIST_INDEX_BY_ROW_ID["old1"], 0)
            self.assertEqual(log_rows._HIST_INDEX_BY_ROW_ID["old2"], 1)
            self.assertEqual(log_rows._HIST_INDEX_BY_ROW_ID["new"], 2)

            log_rows._persist_row_history("new", "new1")

        self.assertEqual(cfg["autorun_history"],
                         ["old1", "old2", "new1"])

    def test_persist_row_history_prunes_out_of_range_indices(self) -> None:
        cfg = {"autorun_history": ["kept"]}
        log_rows._HIST_INDEX_BY_ROW_ID.update({
            "bad_low": -1,
            "bad_high": 9,
            "ok": 0,
        })

        with mock.patch("backend.ytarchiver_config.config_is_writable",
                        return_value=True), \
                mock.patch("backend.ytarchiver_config.load_config",
                           return_value=cfg), \
                mock.patch("backend.ytarchiver_config.save_config",
                           return_value=True), \
                mock.patch("backend.autorun.AUTORUN_HISTORY_MAX", 3):
            log_rows._persist_row_history("new", "new")

        self.assertNotIn("bad_low", log_rows._HIST_INDEX_BY_ROW_ID)
        self.assertNotIn("bad_high", log_rows._HIST_INDEX_BY_ROW_ID)
        self.assertIn("ok", log_rows._HIST_INDEX_BY_ROW_ID)


class AutorunTests(unittest.TestCase):
    @staticmethod
    def _tx(cfg):
        class Tx:
            def __enter__(self):
                return cfg

            def __exit__(self, exc_type, exc, tb):
                return False

        return Tx()

    def test_interval_reached_log_includes_current_time(self) -> None:
        stream = mock.Mock()
        scheduler = autorun_backend.AutorunScheduler(
            lambda: {"started": True},
            stream=stream,
            sync_busy_fn=lambda: False,
        )

        with mock.patch.object(autorun_backend, "_format_log_time",
                               return_value="7:00pm"):
            scheduler._fire()

        stream.emit_text.assert_any_call(
            "\u2014 Autorun: interval reached at 7:00pm, "
            "kicking Sync Subbed\u2026",
            "simpleline_green",
        )

    def test_fire_skips_when_download_or_sync_busy(self) -> None:
        stream = mock.Mock()
        trigger = mock.Mock(return_value={"started": True})
        scheduler = autorun_backend.AutorunScheduler(
            trigger, stream=stream, sync_busy_fn=lambda: True)

        scheduler._fire()

        trigger.assert_not_called()
        stream.emit_text.assert_any_call(
            "\u2014 Autorun: a download or sync is running \u2014 skipping "
            "this run; will run at the next scheduled time.",
            "simpleline_dim",
        )

    def test_format_log_time_uses_lowercase_non_padded_12_hour_time(self) -> None:
        self.assertEqual(
            autorun_backend._format_log_time(datetime(2026, 6, 16, 19, 0)),
            "7:00pm",
        )

    def test_set_interval_persists_with_config_transaction(self) -> None:
        cfg = {}
        scheduler = autorun_backend.AutorunScheduler(lambda: {"started": True})

        with mock.patch.object(autorun_backend, "config_is_writable",
                               return_value=True), \
                mock.patch.object(autorun_backend, "config_transaction",
                                  return_value=self._tx(cfg)):
            result = scheduler.set_interval_mins(15)

        self.assertTrue(result["persisted"])
        self.assertEqual(cfg["autorun_interval"], 15)

    def test_set_mode_reports_unpersisted_on_transaction_failure(self) -> None:
        scheduler = autorun_backend.AutorunScheduler(lambda: {"started": True})

        with mock.patch.object(autorun_backend, "config_is_writable",
                               return_value=True), \
                mock.patch.object(autorun_backend, "config_transaction",
                                  side_effect=OSError("locked")):
            result = scheduler.set_mode("clock")

        self.assertFalse(result["persisted"])
        self.assertEqual(result["mode"], "clock")

    def test_append_history_writes_jsonl_and_seeds_old_config(self) -> None:
        cfg = {"autorun_history": ["old"]}
        with tempfile.TemporaryDirectory() as td:
            history_file = Path(td) / "autorun_history.jsonl"

            with mock.patch.object(autorun_backend, "AUTORUN_HISTORY_FILE",
                                   history_file), \
                    mock.patch.object(autorun_backend, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(autorun_backend, "load_config",
                                      return_value=cfg):
                ok = autorun_backend.append_history_entry("new")
                entries = autorun_backend._read_history_file(history_file)

        self.assertTrue(ok)
        self.assertEqual(entries, ["old", "new"])

    def test_history_entries_for_ui_prefers_jsonl_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            history_file = Path(td) / "autorun_history.jsonl"
            history_file.write_text(
                json.dumps("[ Auto] 7:00pm, Jun 20 — New — 1 downloaded")
                + "\n",
                encoding="utf-8")

            with mock.patch.object(autorun_backend, "AUTORUN_HISTORY_FILE",
                                   history_file):
                rows = autorun_backend.history_entries_for_ui({
                    "autorun_history": [
                        "[ Auto] 6:00pm, Jun 20 — Old — 1 downloaded"
                    ]
                })

        self.assertEqual(rows[0]["cells"]["channel"], "New")

    def test_clear_history_removes_jsonl_and_old_config_history(self) -> None:
        cfg = {"autorun_history": [{"old": True}]}
        with tempfile.TemporaryDirectory() as td:
            history_file = Path(td) / "autorun_history.jsonl"
            history_file.write_text(json.dumps("new") + "\n", encoding="utf-8")

            with mock.patch.object(autorun_backend, "AUTORUN_HISTORY_FILE",
                                   history_file), \
                    mock.patch.object(autorun_backend, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(autorun_backend, "config_transaction",
                                      return_value=self._tx(cfg)):
                result = autorun_backend.clear_history()

        self.assertTrue(result["ok"])
        self.assertEqual(result["removed"], 2)
        self.assertFalse(history_file.exists())
        self.assertEqual(cfg["autorun_history"], [])


class RecentTrackTests(unittest.TestCase):
    def setUp(self) -> None:
        recent_track.set_recent_changed_hook(None)

    @staticmethod
    def _tx(cfg):
        class Tx:
            def __enter__(self):
                return cfg

            def __exit__(self, exc_type, exc, tb):
                return False

        return Tx()

    def test_record_recent_download_keeps_same_title_different_video_id(
            self) -> None:
        cfg = {
            "recent_downloads": [
                {"title": "Same", "channel": "Chan",
                 "filepath": r"C:\old.mp4", "video_id": "AAAAAAAAAAA"},
            ],
            "auto_index_enabled": False,
        }

        with mock.patch("backend.ytarchiver_config.config_is_writable",
                        return_value=True), \
                mock.patch("backend.ytarchiver_config.config_transaction",
                           return_value=self._tx(cfg)):
            ok = recent_track._record_recent_download(
                r"C:\new.mp4",
                "Chan",
                "Same",
                video_id="BBBBBBBBBBB",
                size_bytes=1,
                duration_secs=1,
            )

        self.assertTrue(ok)
        self.assertEqual(len(cfg["recent_downloads"]), 2)
        self.assertEqual(cfg["recent_downloads"][0]["video_id"],
                         "BBBBBBBBBBB")

    def test_record_recent_download_defers_ffprobe_duration(self) -> None:
        cfg = {"recent_downloads": [], "auto_index_enabled": False}
        started_threads: list[str] = []

        class FakeThread:
            def __init__(self, *args, **kwargs):
                self.name = kwargs.get("name", "")

            def start(self):
                started_threads.append(self.name)

        with mock.patch("backend.ytarchiver_config.config_is_writable",
                        return_value=True), \
                mock.patch("backend.ytarchiver_config.config_transaction",
                           return_value=self._tx(cfg)), \
                mock.patch.object(recent_track.subprocess, "run") as run, \
                mock.patch.object(recent_track.threading, "Thread",
                                  side_effect=FakeThread):
            ok = recent_track._record_recent_download(
                r"C:\new.mp4",
                "Chan",
                "Title",
                video_id="AAAAAAAAAAA",
                size_bytes=1,
                duration_secs=None,
            )

        self.assertTrue(ok)
        run.assert_not_called()
        self.assertIn("recent-duration-backfill", started_threads)


class TranscribeMixinTests(unittest.TestCase):
    def test_transcribe_enqueue_applies_requested_runtime_model(self) -> None:
        class Api(TranscribeMixin):
            def __init__(self):
                self._transcribe = mock.Mock()
                self._transcribe.swap_model.return_value = True
                self._transcribe.enqueue.return_value = True

        api = Api()
        with mock.patch("backend.index._reader_open", return_value=None):
            result = api.transcribe_enqueue(
                r"C:\video.mp4", "Video", "medium")

        self.assertTrue(result["ok"])
        api._transcribe.swap_model.assert_called_once_with("medium")
        api._transcribe.enqueue.assert_called_once()

    def test_transcribe_enqueue_rejects_bad_runtime_model(self) -> None:
        class Api(TranscribeMixin):
            def __init__(self):
                self._transcribe = mock.Mock()

        api = Api()
        with mock.patch("backend.index._reader_open", return_value=None):
            result = api.transcribe_enqueue(r"C:\video.mp4", "Video", "bad")

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "Unsupported model")
        api._transcribe.enqueue.assert_not_called()

    def test_transcribe_folder_aborts_when_candidate_cap_exceeded(self) -> None:
        class ImmediateThread:
            def __init__(self, target, *args, **kwargs):
                self._target = target

            def start(self):
                self._target()

        class Api(TranscribeMixin):
            def __init__(self, folder):
                self._window = mock.Mock()
                self._window.create_file_dialog.return_value = [folder]
                self._transcribe = mock.Mock()
                self._log_stream = mock.Mock()
                self._on_queue_changed = mock.Mock()

            def _apply_runtime_whisper_model(self, model):
                return {"ok": True}

        with tempfile.TemporaryDirectory() as td:
            for i in range(3):
                Path(td, f"video{i}.mp4").write_bytes(b"video")
            api = Api(td)
            fake_webview = mock.Mock(FOLDER_DIALOG="folder")

            with mock.patch.dict("sys.modules", {"webview": fake_webview}), \
                    mock.patch.object(transcribe_mixin,
                                      "_TRANSCRIBE_FOLDER_MAX_CANDIDATES", 2), \
                    mock.patch.object(transcribe_mixin.threading, "Thread",
                                      side_effect=ImmediateThread):
                result = api.transcribe_folder()

        self.assertTrue(result["ok"])
        api._transcribe.enqueue.assert_not_called()
        api._on_queue_changed.assert_not_called()
        api._log_stream.emit_error.assert_called_once()
        self.assertIn("aborted",
                      api._log_stream.emit_error.call_args.args[0])


class PunctuationManagerTests(unittest.TestCase):
    def test_joined_text_and_word_ends_uses_exact_joined_string(self) -> None:
        words = ["same", "same", "cafe\u0301", "emoji\U0001f642"]

        text, offsets = punct_alignment.joined_text_and_word_ends(words)

        self.assertEqual(text, "same same cafe\u0301 emoji\U0001f642")
        self.assertEqual([text[:end] for end in offsets], [
            "same",
            "same same",
            "same same cafe\u0301",
            "same same cafe\u0301 emoji\U0001f642",
        ])

    def test_punctuate_guards_missing_proc_after_start(self) -> None:
        stream = mock.Mock()
        mgr = PunctuationManager(stream)
        mgr._start = mock.Mock(return_value=True)
        mgr._proc = None

        text = mgr.punctuate("this needs punctuation", timeout_sec=0.01)

        self.assertEqual(text, "this needs punctuation")
        stream.emit_dim.assert_not_called()

    def test_rough_duration_from_size_uses_50mb_per_hour(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "small.mp4"
            path.write_bytes(b"x" * 1024)

            with mock.patch.object(transcribe_core.os.path, "getsize",
                                   return_value=25 * 1024 * 1024):
                duration = transcribe_core._rough_duration_from_size(str(path))

        self.assertEqual(duration, 1800.0)

    def test_done_segments_from_download_omit_title_use_indent(self) -> None:
        # from_download => 6-space lead, NO title/channel splice.
        segs = transcribe_core._build_transcription_done_segments(
            {"from_download": True}, "Some Title", "Some Channel",
            "Whisper small, took 55sec, 12.3x realtime",
            dim_tags=["dim"], em_tags=["wb"], lbl_tags=["lbl"],
            txt_tags=["txt"], detail_tags=["det"])
        self.assertEqual(segs[0], ["      ", ["dim"]])
        self.assertEqual(segs[1], ["— ✓ ", ["wb"]])
        self.assertEqual(segs[2], ["Transcription", ["lbl"]])
        text = "".join(part for part, _tag in segs)
        self.assertNotIn("Some Title", text)
        self.assertTrue(segs[-1][0].endswith(
            " (Whisper small, took 55sec, 12.3x realtime)\n"))

    def test_done_segments_standalone_splices_title_and_channel(self) -> None:
        segs = transcribe_core._build_transcription_done_segments(
            {"from_download": False}, "My Title", "My Channel",
            "chunked, took 3min 04sec",
            dim_tags=["dim"], em_tags=["wb"], lbl_tags=["lbl"],
            txt_tags=["txt"], detail_tags=["dim"])
        self.assertEqual(segs[0], [" ", ["dim"]])  # 1-space lead standalone
        text = "".join(part for part, _tag in segs)
        self.assertIn("My Title", text)
        self.assertIn("My Channel", text)
        self.assertIn("(chunked, took 3min 04sec)", text)

    def test_done_segments_standalone_without_title_has_no_splice(self) -> None:
        segs = transcribe_core._build_transcription_done_segments(
            {"from_download": False}, "", "", "took 1sec",
            dim_tags=["dim"], em_tags=["wb"], lbl_tags=["lbl"],
            txt_tags=["txt"], detail_tags=["dim"])
        # lead, em, label, detail only — no title/channel segments.
        self.assertEqual(len(segs), 4)


class PunctRestoreTests(unittest.TestCase):
    def test_update_db_text_matches_tiny_float_drift(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                "CREATE TABLE segments("
                "id INTEGER PRIMARY KEY, video_id TEXT, start_time REAL, "
                "end_time REAL, text TEXT, words TEXT)"
            )
            conn.execute(
                "CREATE VIRTUAL TABLE segments_fts USING fts5("
                "text, content='segments', content_rowid='id')"
            )
            conn.execute(
                "INSERT INTO segments(video_id, start_time, end_time, text, words) "
                "VALUES (?, ?, ?, ?, ?)",
                ("vid1", 1.000001, 2.000001, "raw text", "[]"),
            )
            conn.execute(
                "INSERT INTO segments_fts(rowid, text) VALUES(?, ?)",
                (1, "raw text"),
            )

            updated = punct_restore._update_db_text(
                conn,
                "vid1",
                [{"start": 1.0, "end": 2.0, "text": "Raw text.", "words": []}],
            )

            row = conn.execute(
                "SELECT text FROM segments WHERE video_id='vid1'"
            ).fetchone()
            self.assertEqual(updated, 1)
            self.assertEqual(row[0], "Raw text.")
        finally:
            conn.close()

    def test_update_db_text_rolls_back_when_fts_sync_fails(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                "CREATE TABLE segments("
                "id INTEGER PRIMARY KEY, video_id TEXT, start_time REAL, "
                "end_time REAL, text TEXT, words TEXT)"
            )
            conn.execute(
                "INSERT INTO segments(video_id, start_time, end_time, text, words) "
                "VALUES (?, ?, ?, ?, ?)",
                ("vid1", 1.0, 2.0, "raw text", "[]"),
            )
            conn.commit()

            with self.assertRaises(RuntimeError):
                punct_restore._update_db_text(
                    conn,
                    "vid1",
                    [{"start": 1.0, "end": 2.0,
                      "text": "Raw text.", "words": []}],
                )

            row = conn.execute(
                "SELECT text FROM segments WHERE video_id='vid1'"
            ).fetchone()
            self.assertEqual(row[0], "raw text")
        finally:
            conn.close()


class TranscribeManagerQueueTests(unittest.TestCase):
    def test_compress_enqueue_rejects_outside_archive_root(self) -> None:
        stream = mock.Mock()
        mgr = transcribe_core.TranscribeManager(stream)
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            outside = Path(td) / "Outside"
            root.mkdir()
            outside.mkdir()
            video = outside / "Video.mp4"
            video.write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(mgr, "_ensure_worker") as ensure:
                ok = mgr.compress_enqueue(str(video), "Video")

        self.assertFalse(ok)
        stream.emit_error.assert_called()
        ensure.assert_not_called()

    def test_compress_enqueue_accepts_inside_archive_root(self) -> None:
        stream = mock.Mock()
        mgr = transcribe_core.TranscribeManager(stream)
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            root.mkdir()
            video = root / "Video.mp4"
            video.write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(mgr, "_ensure_worker") as ensure:
                ok = mgr.compress_enqueue(str(video), "Video")

        self.assertTrue(ok)
        ensure.assert_called_once()
        self.assertEqual(mgr._jobs[0]["kind"], "compress")

    def test_enqueue_rejects_duplicate_pending_transcribe_path(self) -> None:
        stream = mock.Mock()
        mgr = transcribe_core.TranscribeManager(stream)
        with tempfile.TemporaryDirectory() as td:
            video = Path(td) / "Video.mp4"
            video.write_bytes(b"video")

            with mock.patch.object(transcribe_core,
                                   "_bump_transcription_pending"), \
                    mock.patch.object(mgr, "_persist_pending"), \
                    mock.patch.object(mgr, "_ensure_worker"):
                first = mgr.enqueue(str(video), "Video")
                second = mgr.enqueue(str(video), "Video")

            self.assertTrue(first)
            self.assertFalse(second)
            self.assertEqual(len(mgr._jobs), 1)

    def test_enqueue_rejects_duplicate_running_transcribe_path(self) -> None:
        stream = mock.Mock()
        mgr = transcribe_core.TranscribeManager(stream)
        with tempfile.TemporaryDirectory() as td:
            video = Path(td) / "Video.mp4"
            video.write_bytes(b"video")
            mgr._current_job = {
                "kind": "transcribe",
                "path": str(video),
            }

            with mock.patch.object(transcribe_core,
                                   "_bump_transcription_pending") as bump, \
                    mock.patch.object(mgr, "_persist_pending"), \
                    mock.patch.object(mgr, "_ensure_worker"):
                queued = mgr.enqueue(str(video), "Video")

            self.assertFalse(queued)
            self.assertEqual(len(mgr._jobs), 0)
            bump.assert_not_called()

    def test_reorder_pending_job_mirrors_gpu_popover_order(self) -> None:
        stream = mock.Mock()
        mgr = transcribe_core.TranscribeManager(stream)
        mgr._jobs = [
            {"kind": "transcribe", "path": "one.mp4"},
            {"kind": "transcribe", "path": "two.mp4"},
            {"kind": "transcribe", "path": "three.mp4"},
        ]

        with mock.patch.object(mgr, "_persist_pending") as persist:
            self.assertTrue(mgr.reorder_pending_job("three.mp4", 0))

        self.assertEqual(
            [job["path"] for job in mgr._jobs],
            ["three.mp4", "one.mp4", "two.mp4"],
        )
        persist.assert_called_once()

    def test_skip_current_holds_job_lock_while_sending_cancel(self) -> None:
        stream = mock.Mock()
        mgr = transcribe_core.TranscribeManager(stream)
        cancel = mock.Mock()
        mgr._current_job = {"kind": "transcribe", "path": "video.mp4",
                            "cancel": cancel}
        lock_held_during_cancel = []

        def fake_send_cancel():
            lock_held_during_cancel.append(
                not mgr._jobs_lock.acquire(blocking=False))
            if not lock_held_during_cancel[-1]:
                mgr._jobs_lock.release()
            return True

        with mock.patch.object(mgr, "_send_cancel_command",
                               side_effect=fake_send_cancel) as send_cancel:
            mgr.skip_current()

        cancel.set.assert_called_once()
        send_cancel.assert_called_once_with()
        self.assertEqual(lock_held_during_cancel, [True])

    def test_cancel_all_holds_job_lock_while_stopping_subprocess(self) -> None:
        stream = mock.Mock()
        mgr = transcribe_core.TranscribeManager(stream)
        cancel = mock.Mock()
        mgr._jobs = [{"path": "queued.mp4", "cancel": mock.Mock()}]
        mgr._current_job = {"kind": "transcribe", "path": "video.mp4",
                            "cancel": cancel}
        lock_held_during_stop = []

        def fake_stop(force=False):
            lock_held_during_stop.append(
                not mgr._jobs_lock.acquire(blocking=False))
            if not lock_held_during_stop[-1]:
                mgr._jobs_lock.release()

        with mock.patch.object(mgr, "_stop_subprocess",
                               side_effect=fake_stop) as stop:
            mgr.cancel_all()

        cancel.set.assert_called_once()
        stop.assert_called_once_with(force=True)
        self.assertEqual(mgr._jobs, [])
        self.assertEqual(lock_held_during_stop, [True])

    def test_send_cancel_command_writes_worker_protocol_message(self) -> None:
        stream = mock.Mock()
        mgr = transcribe_core.TranscribeManager(stream)
        proc = mock.Mock()
        proc.poll.return_value = None
        proc.stdin = mock.Mock()
        proc.stdin.closed = False
        mgr._proc = proc

        self.assertTrue(mgr._send_cancel_command())

        proc.stdin.write.assert_called_once_with(
            json.dumps({"command": "cancel"}) + "\n")
        proc.stdin.flush.assert_called_once()

    def test_start_subprocess_waits_for_concurrent_start(self) -> None:
        stream = mock.Mock()
        mgr = transcribe_core.TranscribeManager(stream)
        proc = mock.Mock()
        proc.poll.return_value = None
        mgr._starting = True
        delay = threading.Event()

        def finish_start():
            delay.wait(0.02)
            with mgr._proc_lock:
                mgr._proc = proc
                mgr._starting = False

        t = threading.Thread(target=finish_start)
        t.start()
        try:
            self.assertTrue(mgr.start_subprocess())
        finally:
            t.join(timeout=1.0)

    def test_wait_for_cancel_ack_accepts_cancelled_status(self) -> None:
        stream = mock.Mock()
        mgr = transcribe_core.TranscribeManager(stream)
        mgr._line_queue = queue.Queue()
        mgr._line_queue.put(json.dumps({"status": "progress", "pct": 1}))
        mgr._line_queue.put(json.dumps({"status": "cancelled"}))

        self.assertTrue(mgr._wait_for_cancel_ack(timeout=0.1))

    def test_whisper_stderr_tail_emits_recent_lines(self) -> None:
        stream = mock.Mock()
        mgr = transcribe_core.TranscribeManager(stream)
        mgr._stderr_buffer = ["old", "CUDA OOM", "driver reset"]

        mgr._emit_whisper_stderr_tail()

        stream.emit_dim.assert_called_once()
        detail = stream.emit_dim.call_args.args[0]
        self.assertIn("CUDA OOM", detail)
        self.assertIn("driver reset", detail)

    def test_whisper_traceback_emits_structured_worker_trace(self) -> None:
        stream = mock.Mock()
        mgr = transcribe_core.TranscribeManager(stream)

        mgr._emit_whisper_traceback({"traceback": "Traceback...ValueError"})

        stream.emit_dim.assert_called_once()
        self.assertIn("Traceback...ValueError",
                      stream.emit_dim.call_args.args[0])

    def test_bump_transcription_pending_uses_config_transaction(self) -> None:
        cfg = {
            "channels": [{
                "name": "Channel",
                "folder": "Channel Folder",
                "transcription_pending": 1,
                "transcription_complete": True,
            }]
        }

        class Tx:
            def __enter__(self):
                return cfg

            def __exit__(self, exc_type, exc, tb):
                return False

        with mock.patch("backend.ytarchiver_config.config_is_writable",
                        return_value=True), \
                mock.patch("backend.ytarchiver_config.config_transaction",
                           return_value=Tx()) as tx:
            transcribe_helpers._bump_transcription_pending("Channel", 1)

        tx.assert_called_once()
        self.assertEqual(cfg["channels"][0]["transcription_pending"], 2)
        self.assertFalse(cfg["channels"][0]["transcription_complete"])

        with mock.patch("backend.ytarchiver_config.config_is_writable",
                        return_value=True), \
                mock.patch("backend.ytarchiver_config.config_transaction",
                           return_value=Tx()):
            transcribe_helpers._bump_transcription_pending("Channel Folder", -2)

        self.assertEqual(cfg["channels"][0]["transcription_pending"], 0)
        self.assertTrue(cfg["channels"][0]["transcription_complete"])


class Python311DiscoveryTests(unittest.TestCase):
    def test_find_python311_rejects_310_and_accepts_validated_311(self) -> None:
        py310 = r"C:\Python310\python.exe"
        py311 = r"C:\Python311\python.exe"

        def fake_isfile(path):
            return path in {py310, py311}

        def fake_run(args, **_kwargs):
            proc = mock.Mock()
            proc.returncode = 0
            proc.stdout = "3.11\n" if args[0] == py311 else "3.10\n"
            return proc

        with mock.patch.object(transcribe_helpers.glob, "glob",
                               return_value=[]), \
                mock.patch.object(transcribe_helpers.os.path, "isfile",
                                  side_effect=fake_isfile), \
                mock.patch("shutil.which", return_value=None), \
                mock.patch.object(transcribe_helpers.subprocess, "run",
                                  side_effect=fake_run):
            self.assertEqual(transcribe_helpers.find_python311(), py311)

    def test_find_python311_returns_none_when_path_python_is_not_311(self) -> None:
        py310 = r"C:\Python310\python.exe"

        def fake_run(_args, **_kwargs):
            proc = mock.Mock()
            proc.returncode = 0
            proc.stdout = "3.10\n"
            return proc

        with mock.patch.object(transcribe_helpers.glob, "glob",
                               return_value=[]), \
                mock.patch.object(transcribe_helpers.os.path, "isfile",
                                  return_value=False), \
                mock.patch("shutil.which",
                           side_effect=lambda name: py310
                           if name == "python" else None), \
                mock.patch.object(transcribe_helpers.subprocess, "run",
                                  side_effect=fake_run):
            self.assertIsNone(transcribe_helpers.find_python311())


class TranscribeVttTests(unittest.TestCase):
    def test_attach_words_to_segments_uses_unbounded_boundary_lookup(self) -> None:
        segments = [
            {"start": 0.0, "end": 40.0, "text": "first"},
            {"start": 30.0, "end": 31.0, "text": "boundary"},
        ]
        all_words = [
            {"w": f"w{i}", "s": i / 10.0, "e": i / 10.0 + 0.05}
            for i in range(401)
        ]

        out = transcribe_vtt._attach_words_to_segments(segments, all_words)

        self.assertEqual(out[1]["w"][0]["w"], "w300")
        self.assertEqual(out[1]["w"][0]["s"], 30.0)

    def test_fetch_captions_prefers_exact_en_without_c_tags(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "Video [abc123_def4].mp4"
            video.write_bytes(b"video")
            base = str(video.with_suffix("")) + ".__cap_probe"
            en_gb = Path(base + ".en-GB.vtt")
            en = Path(base + ".en.vtt")
            en_gb.write_text("WEBVTT\nhello", encoding="utf-8")
            en.write_text("WEBVTT\nhello", encoding="utf-8")

            with mock.patch.object(transcribe_vtt.shutil, "which",
                                   return_value="yt-dlp"), \
                    mock.patch.object(transcribe_vtt.subprocess, "run"):
                picked = transcribe_vtt._fetch_captions_via_ytdlp(
                    str(video), mock.Mock(), [])

        self.assertEqual(picked, str(en))

    def test_auto_caption_success_preserves_existing_user_vtt(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "Video [abc123_def4].mp4"
            video.write_bytes(b"video")
            user_vtt = root / "Video [abc123_def4].en.vtt"
            user_vtt.write_text("WEBVTT", encoding="utf-8")
            txt = root / "Transcript.txt"
            jsonl = root / ".Transcript.jsonl"

            with mock.patch.object(transcribe_vtt, "_parse_vtt",
                                   return_value=[{"s": 0.0, "e": 1.0,
                                                  "t": "hello world"}]), \
                    mock.patch.object(transcribe_vtt,
                                      "_resolve_transcript_paths",
                                      return_value=(str(txt), str(jsonl),
                                                    2026, 6,
                                                    "06.14.2026")), \
                    mock.patch.object(transcribe_vtt,
                                      "_write_transcript_entry"), \
                    mock.patch.object(transcribe_vtt,
                                      "_write_jsonl_entry"), \
                    mock.patch.object(transcribe_vtt,
                                      "_bump_transcription_pending"), \
                    mock.patch("backend.index.ingest_jsonl"), \
                    mock.patch("backend.index.mark_video_transcribed"), \
                    mock.patch("backend.ytarchiver_config"
                               ".remove_pending_tx_id"):
                ok = transcribe_vtt._try_auto_captions(
                    str(video), "Video", "Channel", mock.Mock(), None)

            self.assertTrue(ok)
            self.assertTrue(user_vtt.exists())

    def test_auto_caption_jsonl_failure_does_not_mark_transcribed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "Video [abc123_def4].mp4"
            video.write_bytes(b"video")
            vtt = root / "Video [abc123_def4].en.vtt"
            vtt.write_text("WEBVTT", encoding="utf-8")
            stream = mock.Mock()

            with mock.patch.object(transcribe_vtt, "_parse_vtt",
                                   return_value=[{
                                       "s": 0.0, "e": 1.0, "t": "hello",
                                       "w": [{"w": "hello", "s": 0.0, "e": 1.0}],
                                   }]), \
                    mock.patch.object(
                        transcribe_vtt, "_resolve_transcript_paths",
                        return_value=(str(root / "Channel Transcript.txt"),
                                      str(root / ".Channel Transcript.jsonl"),
                                      2026, 6, "06.14.2026")), \
                    mock.patch.object(transcribe_vtt,
                                      "_write_transcript_entry",
                                      return_value=True), \
                    mock.patch.object(transcribe_vtt,
                                      "_write_jsonl_entry",
                                      return_value=False), \
                    mock.patch.object(transcribe_vtt,
                                      "_bump_transcription_pending") as bump, \
                    mock.patch("backend.index.ingest_jsonl") as ingest, \
                    mock.patch("backend.index.mark_video_transcribed") as mark:
                ok = transcribe_vtt._try_auto_captions(
                    str(video), "Video", "Channel", stream,
                    video_id_hint="abc123_def4")

            self.assertFalse(ok)
            stream.emit_error.assert_called()
            ingest.assert_not_called()
            mark.assert_not_called()
            bump.assert_not_called()


class ReorgTests(unittest.TestCase):
    def test_sidecars_for_matches_bcp47_caption_suffixes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "Video.mp4"
            video.write_bytes(b"video")
            expected = [
                root / "Video.fil.vtt",
                root / "Video.es-419.srt",
                root / "Video.en-orig.vtt",
            ]
            for path in expected:
                path.write_text("WEBVTT", encoding="utf-8")
            unrelated = root / "Video.Part2.vtt"
            unrelated.write_text("WEBVTT", encoding="utf-8")

            found = set(reorg._sidecars_for(video))

        for path in expected:
            self.assertIn(path, found)
        self.assertNotIn(unrelated, found)


class RedownloadTests(unittest.TestCase):
    # ── T201: pure downgrade-refusal guard ────────────────────────────
    def test_refusal_allows_equal_or_better_copy(self) -> None:
        # new copy >= original height -> allowed (no reason).
        self.assertEqual(
            redownload._redownload_refusal_reason(1080, 1080, 1080), "")
        self.assertEqual(
            redownload._redownload_refusal_reason(720, 1080, 1080), "")

    def test_refusal_blocks_lower_resolution(self) -> None:
        reason = redownload._redownload_refusal_reason(1080, 720, 1080)
        self.assertIn("720p", reason)
        self.assertIn("1080p", reason)

    def test_refusal_blocks_unprobeable_new_copy(self) -> None:
        reason = redownload._redownload_refusal_reason(1080, 0, 1080)
        self.assertIn("can't probe the new copy", reason)

    def test_refusal_unknown_orig_uses_target_floor(self) -> None:
        # original height unknown; new copy well below target -> refuse.
        reason = redownload._redownload_refusal_reason(0, 360, 1080)
        self.assertIn("target 1080p", reason)
        # new copy within the 8px tolerance of target -> allowed.
        self.assertEqual(
            redownload._redownload_refusal_reason(0, 1080, 1080), "")
        # neither probeable -> refuse (can't verify quality).
        self.assertIn(
            "can't probe either",
            redownload._redownload_refusal_reason(0, 0, 1080))

    def test_refusal_best_target_only_compares_orig_vs_new(self) -> None:
        # target_h=0 ("best") + unknown orig -> nothing to compare, allow.
        self.assertEqual(
            redownload._redownload_refusal_reason(0, 720, 0), "")

    def test_height_from_metadata_uses_max_video_format_height(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "Title [abc123_def4].mp4"
            video.write_bytes(b"video")
            meta = root / ".Channel Metadata.jsonl"
            meta.write_text(json.dumps({
                "video_id": "abc123_def4",
                "formats": [
                    {"format_id": "audio", "vcodec": "none", "height": 0},
                    {"format_id": "low", "vcodec": "avc1", "height": 360},
                    {"format_id": "high", "vcodec": "vp9", "height": 1080},
                ],
            }) + "\n", encoding="utf-8")

            height = redownload._height_from_metadata_jsonl(str(video))

        self.assertEqual(height, 1080)

    def test_progress_file_stores_broken_counts_with_done_ids(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            saved = redownload._save_progress(
                str(root),
                "https://youtube.com/@example",
                "1080",
                {"done_video"},
                {"broken_video": 2},
            )

            payload = json.loads(
                (root / "_redownload_progress.json").read_text(
                    encoding="utf-8"))
            done, broken = redownload._load_progress_state(
                str(root), "https://youtube.com/@example", "1080")
            legacy_exists = (root / "_redownload_broken_counts.json").exists()

        self.assertTrue(saved)
        self.assertEqual(payload["broken_counts"], {"broken_video": 2})
        self.assertEqual(done, {"done_video"})
        self.assertEqual(broken, {"broken_video": 2})
        self.assertFalse(legacy_exists)

    def test_progress_load_keeps_done_ids_after_resolution_switch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            redownload._save_progress(
                str(root),
                "https://youtube.com/@example",
                "720",
                {"done_video"},
                {"broken_video": 3},
            )

            done, broken = redownload._load_progress_state(
                str(root), "https://youtube.com/@example", "1080")

        self.assertEqual(done, {"done_video"})
        self.assertEqual(broken, {"broken_video": 3})

    def test_progress_state_migrates_and_clears_legacy_broken_counts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            redownload._save_progress(
                str(root),
                "https://youtube.com/@example",
                "720",
                {"done_video"},
            )
            legacy = root / "_redownload_broken_counts.json"
            legacy.write_text(json.dumps({"legacy_video": 2}),
                              encoding="utf-8")

            done, broken = redownload._load_progress_state(
                str(root), "https://youtube.com/@example", "720")
            redownload._clear_progress(str(root))

            progress_exists = (root / "_redownload_progress.json").exists()
            legacy_exists = legacy.exists()

        self.assertEqual(done, {"done_video"})
        self.assertEqual(broken, {"legacy_video": 2})
        self.assertFalse(progress_exists)
        self.assertFalse(legacy_exists)


class RedownloadMixinTests(unittest.TestCase):
    def test_resume_pending_redownloads_preserves_url_and_scope(self) -> None:
        class Api(RedownloadMixin):
            def __init__(self):
                self._queues = mock.Mock()
                self._queues.sync = [{
                    "kind": "redownload",
                    "name": "Display Name",
                    "url": "https://youtube.com/@correct",
                    "redownload_res": "1080",
                    "scope": {"year": "2024", "month": 5},
                }]
                self.chan_redownload = mock.Mock(return_value={"ok": True})

        api = Api()

        result = api.resume_pending_redownloads()

        self.assertEqual(result, {"ok": True, "resumed": 1, "skipped": 0})
        api._queues.sync_remove.assert_called_once_with(
            "https://youtube.com/@correct")
        api.chan_redownload.assert_called_once_with(
            {"url": "https://youtube.com/@correct"},
            "1080",
            scope={"year": "2024", "month": 5},
        )

    def test_redownload_mixin_prefers_app_services_dependencies(self) -> None:
        service_queues = mock.Mock()
        service_queues.sync = [{
            "kind": "redownload",
            "name": "Service Channel",
            "url": "https://youtube.com/@service",
            "redownload_res": "720",
        }]
        service_log = mock.Mock()

        class Api(RedownloadMixin):
            def __init__(self):
                self._queues = mock.Mock()
                self._queues.sync = []
                self._log_stream = mock.Mock()
                self._redwnl_cancel = threading.Event()
                self._sync_pause = threading.Event()
                self._window = None
                self._on_queue_changed = mock.Mock()
                self.chan_redownload = mock.Mock(return_value={"ok": True})
                self.services = AppServices(
                    load_config=lambda: {
                        "channels": [{"url": "https://youtube.com/@service"}],
                        "autorun_interval_mins": 30,
                    },
                    save_config=lambda cfg: True,
                    queues=service_queues,
                    log_stream=service_log,
                    transcribe=mock.Mock(),
                    event_bus=mock.Mock(),
                )

        api = Api()
        with mock.patch("backend.api_mixins.redownload_mixin.load_config",
                        side_effect=AssertionError("use services")), \
                mock.patch("backend.api_mixins.redownload_mixin"
                           ".archive_scan.load_disk_cache",
                           return_value={}), \
                mock.patch("backend.redownload.redownload_channel") as run, \
                mock.patch("backend.archive_scan.invalidate_channel"):
            resume = api.resume_pending_redownloads()
            pending = api.queue_pending_check()
            api._run_redownload_one(
                {"name": "Service Channel",
                 "url": "https://youtube.com/@service"},
                "X:/Archive/Service Channel",
                "best",
                None,
            )

        self.assertEqual(resume, {"ok": True, "resumed": 1, "skipped": 0})
        service_queues.sync_remove.assert_called_once_with(
            "https://youtube.com/@service")
        self.assertEqual(pending["count"], 1)
        self.assertEqual(pending["total"], 1)
        run.assert_called_once()
        self.assertIs(run.call_args.kwargs["stream"], service_log)
        self.assertIs(run.call_args.kwargs["queues"], service_queues)
        self.assertEqual(service_queues.set_current_sync.call_count, 2)
        service_log.emit.assert_called()
        service_log.flush.assert_called()
        api._queues.sync_remove.assert_not_called()
        api._queues.set_current_sync.assert_not_called()
        api._log_stream.emit.assert_not_called()
        api._on_queue_changed.assert_called_once()


class MediaOpsMixinTests(unittest.TestCase):
    def test_reorg_cancel_sets_active_reorg_event(self) -> None:
        class Api(MediaOpsMixin):
            pass

        api = Api()
        api._queues = mock.Mock(current_sync=None, current_gpu=None)
        api._log_stream = mock.Mock()
        started = threading.Event()
        captured = {}

        def fake_reorg_channel(*args, **kwargs):
            captured["cancel_event"] = kwargs["cancel_event"]
            started.set()
            captured["cancel_event"].wait(timeout=1.0)

        with tempfile.TemporaryDirectory() as td, \
                mock.patch("backend.api_mixins.media_ops_mixin"
                           ".subs_backend.get_channel",
                           return_value={"name": "Channel",
                                         "url": "https://example.test/ch"}), \
                mock.patch("backend.api_mixins.media_ops_mixin.load_config",
                           return_value={"output_dir": td}), \
                mock.patch("backend.sync.channel_folder_name",
                           return_value="Channel"), \
                mock.patch("backend.api_mixins.media_ops_mixin"
                           ".reorg_backend.reorg_channel",
                           side_effect=fake_reorg_channel):
            result = api.reorg_channel_folder({"url": "https://example.test/ch"})
            self.assertTrue(started.wait(timeout=1.0))
            cancel_result = api.reorg_cancel()

        self.assertTrue(result["ok"])
        self.assertTrue(cancel_result["ok"])
        self.assertTrue(captured["cancel_event"].is_set())

    def test_sync_start_refuses_while_reorg_running(self) -> None:
        class Api(SyncMixin):
            def sync_is_running(self):
                return False

        api = Api()
        api._sync_thread = None
        api._reorg_running = True

        result = api.sync_start_all()

        self.assertFalse(result["ok"])
        self.assertIn("reorganization is running", result["error"])


class SyncCoreTests(unittest.TestCase):
    def tearDown(self) -> None:
        sync_core._LAST_429_BACKOFF_TS = 0.0

    def test_sync_channel_preflight_does_not_create_channel_folder_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stream = mock.Mock()
            channel = {
                "name": "New Channel",
                "url": "https://www.youtube.com/@newchannel",
            }

            with mock.patch.object(sync_core, "find_yt_dlp",
                                   return_value="yt-dlp"), \
                    mock.patch.object(sync_core, "load_config",
                                      return_value={"output_dir": str(root)}), \
                    mock.patch.object(utils, "check_directory_writable",
                                      return_value=False), \
                    mock.patch.object(utils, "check_disk_space",
                                      return_value=True):
                result = sync_core.sync_channel(channel, stream)

            self.assertFalse(result["ok"])
            self.assertEqual(result["reason"], "write blocked")
            self.assertFalse((root / "New Channel").exists())

    def test_429_backoff_uses_locked_process_wide_cooldown(self) -> None:
        stream = mock.Mock()
        cancel = threading.Event()
        pause = threading.Event()
        sync_core._LAST_429_BACKOFF_TS = 0.0

        with mock.patch.object(sync_core.time, "time",
                               side_effect=[100.0, 120.0, 161.0]), \
                mock.patch.object(sync_core.time, "sleep") as sleep:
            first = sync_core._maybe_429_backoff(stream, cancel, pause)
            second = sync_core._maybe_429_backoff(stream, cancel, pause)
            third = sync_core._maybe_429_backoff(stream, cancel, pause)

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertTrue(third)
        self.assertEqual(sleep.call_count, 60)
        self.assertEqual(stream.emit_text.call_count, 4)

    def test_failed_video_merge_does_not_resurrect_timeout_giveup(self) -> None:
        video_id = "abc123def45"

        next_failed, gave_up = sync_core._merge_failed_video_ids(
            {}, {video_id}, [], {video_id})

        self.assertEqual(next_failed, {})
        self.assertEqual(gave_up, [])

    def test_failed_video_merge_drops_match_filtered_ids(self) -> None:
        filtered_id = "short000001"
        failed_id = "fail0000001"

        next_failed, gave_up = sync_core._merge_failed_video_ids(
            {filtered_id: 2, failed_id: 1},
            {filtered_id, failed_id},
            [],
            filtered_this_run={filtered_id},
        )

        self.assertEqual(next_failed, {failed_id: 2})
        self.assertEqual(gave_up, [])

    def test_archived_failed_video_ids_detects_stale_retry_state(self) -> None:
        video_id = "abc123def45"
        with tempfile.TemporaryDirectory() as td:
            archive_path = Path(td) / "ytarchive.txt"
            archive_path.write_text(
                f"youtube otherVideo1\n"
                f"youtube {video_id}\n",
                encoding="utf-8",
            )

            with mock.patch.object(sync_core, "ARCHIVE_FILE", archive_path):
                found = sync_core._archived_failed_video_ids(
                    {video_id, "missing0001"})

        self.assertEqual(found, {video_id})

    def test_sync_all_pause_mid_download_keeps_task_visible(self) -> None:
        sync_all_module = __import__(
            "backend.sync.sync_all", fromlist=["sync_all"])
        q = queues.QueueState()
        channel = {
            "name": "New Channel",
            "url": "https://www.youtube.com/@newchannel",
        }
        pause = threading.Event()
        cancel = threading.Event()
        seen: dict[str, object] = {}
        calls = {"count": 0}

        def fake_sync_channel(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                pause.set()
                return sync_core.SyncResult(ok=True, downloaded=1, errors=0)
            return sync_core.SyncResult(ok=True, downloaded=0, errors=0)

        def fake_wait_for_resume(pause_event, cancel_event=None, *, tick=0.25):
            seen["snapshot"] = q.sync_snapshot()
            seen["payload"] = q.to_ui_payload()
            pause_event.clear()
            q.set_sync_paused(False)
            return False

        try:
            with mock.patch.object(q, "save_debounced",
                                   return_value=None), \
                    mock.patch.object(sync_all_module, "load_config",
                                      return_value={"channels": [channel]}), \
                    mock.patch.object(sync_all_module, "ARCHIVE_FILE",
                                      "__missing_archive__.txt"), \
                    mock.patch.object(
                        sync_all_module, "config_transaction",
                        side_effect=lambda: contextlib.nullcontext({})), \
                    mock.patch.object(sync_all_module, "clear_sync_progress"), \
                    mock.patch.object(sync_all_module, "sync_channel",
                                      side_effect=fake_sync_channel), \
                    mock.patch.object(sync_all_module, "_should_batch_limit",
                                      return_value=False), \
                    mock.patch("backend.pause_helpers.wait_for_resume",
                               side_effect=fake_wait_for_resume):
                self.assertTrue(q.sync_enqueue(channel))

                result = sync_all_module.sync_all(
                    mock.Mock(),
                    cancel_event=cancel,
                    queues=q,
                    pause_event=pause,
                    add_downloads_from_config=False,
                )

            self.assertTrue(result["ok"])
            self.assertEqual(calls["count"], 2)
            self.assertEqual(seen["snapshot"], [channel])
            payload = seen["payload"]
            self.assertEqual(
                payload["sync"][0]["name"], "Download New Channel")
            self.assertEqual(payload["sync"][0]["status"], "queued")
        finally:
            q.mark_orphan()


class CompressTests(unittest.TestCase):
    def test_compress_refuses_replace_when_source_duration_unknown(self) -> None:
        class FakePopen:
            def __init__(self, cmd, *args, **kwargs):
                self.stderr = []
                self.returncode = 0
                Path(cmd[-1]).write_bytes(b"small")

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                return self.returncode

            def terminate(self):
                self.returncode = -15

            def kill(self):
                self.returncode = -9

        with tempfile.TemporaryDirectory() as td:
            video = Path(td) / "sample.mp4"
            original = b"x" * 100
            video.write_bytes(original)
            stream = mock.Mock()

            with mock.patch.object(compress, "find_ffmpeg",
                                   return_value="ffmpeg"), \
                    mock.patch.object(compress, "get_video_duration",
                                      return_value=0.0), \
                    mock.patch.object(compress, "get_video_codec",
                                      return_value="h264"), \
                    mock.patch.object(compress.subprocess, "Popen",
                                      side_effect=FakePopen), \
                    mock.patch("secrets.token_hex", return_value="abc123"):
                result = compress.compress_video(str(video), stream)

            kept = Path(td) / "sample.compressed.mp4"
            self.assertFalse(result["ok"])
            self.assertEqual(result["reason"], "source_duration_unknown")
            self.assertEqual(result["kept_path"], str(kept))
            self.assertEqual(video.read_bytes(), original)
            self.assertEqual(kept.read_bytes(), b"small")
            self.assertFalse((Path(td) / "sample_TEMP_COMPRESS_"
                              f"{os.getpid()}_abc123.mp4.lock").exists())
            stream.emit_error.assert_called_once()
            self.assertIn("original was not replaced",
                          stream.emit_error.call_args.args[0])

    def test_compress_refuses_to_run_without_temp_lock(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            video = Path(td) / "sample.mp4"
            video.write_bytes(b"x" * 100)
            stream = mock.Mock()

            with mock.patch.object(compress, "find_ffmpeg",
                                   return_value="ffmpeg"), \
                    mock.patch("secrets.token_hex", return_value="abc123"), \
                    mock.patch("builtins.open",
                               side_effect=OSError("lock denied")), \
                    mock.patch.object(compress.subprocess, "Popen") as popen:
                result = compress.compress_video(str(video), stream)

            self.assertFalse(result["ok"])
            self.assertEqual(result["reason"], "lock_failed")
            popen.assert_not_called()
            stream.emit_error.assert_called_once()
            self.assertIn("Could not protect the compression temp file",
                          stream.emit_error.call_args.args[0])


class SyncHelpersTests(unittest.TestCase):
    def test_sweep_orphan_vtts_requires_same_base_media_sibling(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            caption = root / "User Notes.en.vtt"
            caption.write_text("WEBVTT", encoding="utf-8")

            removed = sync_helpers._sweep_orphan_vtts(str(root))

            self.assertEqual(removed, 0)
            self.assertTrue(caption.exists())

    def test_sweep_orphan_vtts_preserves_visible_windows_caption(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "Video.mp4"
            caption = root / "Video.en.vtt"
            video.write_bytes(b"video")
            caption.write_text("WEBVTT", encoding="utf-8")

            with mock.patch.object(sync_helpers.os, "name", "nt"), \
                    mock.patch.object(sync_helpers._utils,
                                      "_file_has_hidden_attribute",
                                      return_value=False):
                removed = sync_helpers._sweep_orphan_vtts(str(root))

            self.assertEqual(removed, 0)
            self.assertTrue(caption.exists())

    def test_sweep_orphan_vtts_removes_hidden_caption_with_media_sibling(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "Video.mp4"
            caption = root / "Video.en.vtt"
            video.write_bytes(b"video")
            caption.write_text("WEBVTT", encoding="utf-8")

            with mock.patch.object(sync_helpers.os, "name", "nt"), \
                    mock.patch.object(sync_helpers._utils,
                                      "_file_has_hidden_attribute",
                                      return_value=True):
                removed = sync_helpers._sweep_orphan_vtts(str(root))

            self.assertEqual(removed, 1)
            self.assertFalse(caption.exists())

    def test_hide_sidecar_win_no_attribute_regression(self) -> None:
        # REGRESSION (2026-06-27): the utils.py split moved _VISIBLE_MEDIA_EXTS
        # / _file_has_hidden_attribute / _archive_file_should_be_visible into
        # fs_attrs but did NOT re-export them from backend.utils. _hide_sidecar_win
        # (called for every downloaded video, inside the DLTRACK gate before
        # downloaded+=1 / register_video) then raised AttributeError, which the
        # handler's catch-all swallowed — silently breaking ALL downloads
        # ("0 downloaded · 0 errors"). Guard the re-export contract AND the call.
        import backend.utils as _u
        for name in ("_VISIBLE_MEDIA_EXTS", "_file_has_hidden_attribute",
                     "_archive_file_should_be_visible"):
            self.assertTrue(hasattr(_u, name),
                            f"backend.utils must re-export {name} "
                            f"(sync_helpers reaches for it via _utils.)")
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "Clip.mp4"
            video.write_bytes(b"v")
            (root / "Clip.info.json").write_text("{}", encoding="utf-8")
            # Must NOT raise (raised AttributeError before the re-export fix).
            sync_helpers._hide_sidecar_win(str(video))


class QuickcheckTests(unittest.TestCase):
    def tearDown(self) -> None:
        quickcheck._quickcheck_bad.clear()

    def test_set_batch_cooldown_updates_channel_in_transaction(self) -> None:
        cfg = {
            "channels": [{
                "url": "https://www.youtube.com/@example",
            }],
        }

        class Tx:
            def __enter__(self):
                return cfg

            def __exit__(self, exc_type, exc, tb):
                return False

        with mock.patch.object(quickcheck, "config_transaction",
                               return_value=Tx()):
            quickcheck.set_batch_cooldown("https://www.youtube.com/@example")

        self.assertIn("init_batch_after", cfg["channels"][0])

    def test_prefetch_channel_total_registers_process(self) -> None:
        proc = mock.Mock()
        proc.stdout = iter([
            "video000001|||not_live\n",
            "video000002|||is_live\n",
            "video000003|||is_upcoming\n",
        ])

        with mock.patch.object(quickcheck, "find_yt_dlp",
                               return_value="yt-dlp"), \
                mock.patch.object(quickcheck, "_find_cookie_source",
                                  return_value=[]), \
                mock.patch.object(quickcheck.subprocess, "Popen",
                                  return_value=proc), \
                mock.patch.object(quickcheck.PROCESS_REGISTRY,
                                  "register") as register, \
                mock.patch.object(quickcheck.PROCESS_REGISTRY,
                                  "unregister") as unregister:
            result = quickcheck.prefetch_channel_total(
                "https://youtube.com/@example", timeout_sec=5)

        self.assertEqual(result, {
            "ok": True,
            "total": 3,
            "lives": 1,
            "upcoming": 1,
        })
        register.assert_called_once_with(proc)
        unregister.assert_called_once_with(proc)

    def test_quick_check_skips_after_repeated_timeouts(self) -> None:
        url = "https://youtube.com/@example"

        with mock.patch.object(quickcheck, "find_yt_dlp",
                               return_value="yt-dlp"), \
                mock.patch.object(quickcheck, "_find_cookie_source",
                                  return_value=[]), \
                mock.patch.object(quickcheck.subprocess, "run",
                                  side_effect=quickcheck.subprocess
                                  .TimeoutExpired("yt-dlp", 1)) as run:
            first = quickcheck.quick_check_new_uploads(
                url, set(), timeout_sec=1)
            second = quickcheck.quick_check_new_uploads(
                url, set(), timeout_sec=1)
            third = quickcheck.quick_check_new_uploads(
                url, set(), timeout_sec=1)

        self.assertTrue(first["timed_out"])
        self.assertTrue(second["timed_out"])
        self.assertTrue(third["quickcheck_skipped"])
        self.assertEqual(run.call_count, 2)


class ProcessRunnerCacheTests(unittest.TestCase):
    def tearDown(self) -> None:
        process_runner.reset_process_binary_caches()
        compress._probe_cache.clear()

    def test_find_yt_dlp_is_cached_and_resettable(self) -> None:
        process_runner.reset_process_binary_caches()
        with mock.patch.object(process_runner.shutil, "which",
                               return_value="yt-dlp.exe") as which:
            self.assertEqual(process_runner.find_yt_dlp(), "yt-dlp.exe")
            self.assertEqual(process_runner.find_yt_dlp(), "yt-dlp.exe")
            self.assertEqual(which.call_count, 1)

            process_runner.reset_process_binary_caches()
            self.assertEqual(process_runner.find_yt_dlp(), "yt-dlp.exe")
            self.assertEqual(which.call_count, 2)

    def test_compress_probe_cache_moves_hits_to_lru_tail(self) -> None:
        compress._probe_cache.clear()
        with mock.patch.object(compress, "_PROBE_CACHE_MAX", 2):
            compress._probe_cache_put(("a.mp4", 1.0), (1.0, "av1"))
            compress._probe_cache_put(("b.mp4", 1.0), (2.0, "h264"))
            with compress._probe_cache_lock:
                _ = compress._probe_cache.get(("a.mp4", 1.0))
                compress._probe_cache.move_to_end(("a.mp4", 1.0))
            compress._probe_cache_put(("c.mp4", 1.0), (3.0, "hevc"))

        self.assertIn(("a.mp4", 1.0), compress._probe_cache)
        self.assertIn(("c.mp4", 1.0), compress._probe_cache)
        self.assertNotIn(("b.mp4", 1.0), compress._probe_cache)

    def test_run_streaming_result_unpacks_like_legacy_tuple(self) -> None:
        result = process_runner.StreamingRunResult(
            0, ["stderr"], cancelled=True)

        rc, tail = result

        self.assertEqual(rc, 0)
        self.assertEqual(tail, ["stderr"])
        self.assertEqual(result[0], 0)
        self.assertEqual(result[1], ["stderr"])
        self.assertTrue(result.cancelled)

    def test_process_registry_register_reaps_dead_entries(self) -> None:
        registry = process_runner.ProcessRegistry()
        dead = mock.Mock()
        dead.poll.return_value = 0
        live = mock.Mock()
        live.poll.return_value = None

        registry.register(dead)
        registry.register(live)

        self.assertEqual(registry._procs, [live])

    def test_process_registry_kill_all_uses_per_process_grace(self) -> None:
        registry = process_runner.ProcessRegistry()
        first = mock.Mock()
        second = mock.Mock()
        first.poll.side_effect = [None, None, None]
        second.poll.return_value = None
        first.wait.side_effect = [
            process_runner.subprocess.TimeoutExpired("first", 1),
            0,
        ]
        second.wait.return_value = 0
        registry.register(first)
        registry.register(second)

        killed = registry.kill_all(timeout=1.25)

        self.assertEqual(killed, 2)
        first.terminate.assert_called_once()
        first.kill.assert_called_once()
        first.wait.assert_any_call(timeout=1.25)
        second.terminate.assert_called_once()
        second.wait.assert_called_once_with(timeout=1.25)
        second.kill.assert_not_called()

    def test_run_streaming_marks_cancel_event_as_cancelled(self) -> None:
        class FakePipe:
            def __init__(self, lines):
                self._lines = list(lines)

            def readline(self):
                if self._lines:
                    return self._lines.pop(0)
                return ""

        class FakeProc:
            def __init__(self):
                self.stdout = FakePipe(["progress\n", ""])
                self.stderr = FakePipe([""])
                self.returncode = None
                self.terminated = False

            def terminate(self):
                self.terminated = True

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                if self.returncode is None:
                    self.returncode = -15
                return self.returncode

        cancel = threading.Event()
        proc = FakeProc()
        runner = process_runner.YtDlpRunner(registry=mock.Mock())

        def on_stdout(_line):
            cancel.set()

        with mock.patch.object(process_runner.subprocess, "Popen",
                               return_value=proc):
            result = runner.run_streaming(
                ["yt-dlp", "url"],
                on_stdout_line=on_stdout,
                cancel_event=cancel,
            )

        self.assertTrue(result.cancelled)
        self.assertTrue(proc.terminated)
        self.assertEqual(result.returncode, -15)


class YtDlpSessionTests(unittest.TestCase):
    def test_popen_ytdlp_process_registers_process(self) -> None:
        proc = mock.Mock()

        with mock.patch.object(ytdlp_session.subprocess, "Popen",
                               return_value=proc) as popen, \
                mock.patch.object(ytdlp_session.PROCESS_REGISTRY,
                                  "register") as register:
            result = ytdlp_session.popen_ytdlp_process(
                ["yt-dlp", "url"], startupinfo="startup")

        self.assertIs(result, proc)
        register.assert_called_once_with(proc)
        kwargs = popen.call_args.kwargs
        self.assertEqual(kwargs["stdin"], ytdlp_session.subprocess.DEVNULL)
        self.assertEqual(kwargs["stdout"], ytdlp_session.subprocess.PIPE)
        self.assertEqual(kwargs["stderr"], ytdlp_session.subprocess.STDOUT)
        self.assertEqual(kwargs["bufsize"], 0)
        self.assertEqual(kwargs["startupinfo"], "startup")

    def test_launch_reports_cancel_before_start(self) -> None:
        cancel_event = mock.Mock()
        cancel_event.is_set.return_value = True
        stream = mock.Mock()

        with mock.patch.object(ytdlp_session.subprocess, "Popen") as popen:
            result = ytdlp_session.launch_ytdlp_process(
                ["yt-dlp", "url"], stream, cancel_event=cancel_event)

        self.assertTrue(result.cancelled)
        popen.assert_not_called()

    def test_download_watchdog_kills_stalled_process(self) -> None:
        proc = mock.Mock()
        proc.poll.return_value = None
        stream = mock.Mock()

        handle = ytdlp_session.start_download_watchdog(
            proc, stream, kill_sec=-1, poll_interval=0.01)
        try:
            for _ in range(50):
                if proc.kill.called:
                    break
                ytdlp_session.time.sleep(0.01)
            self.assertTrue(proc.kill.called)
            self.assertTrue(handle.stalled["hit"])
            stream.emit.assert_called()
            stream.flush.assert_called()
        finally:
            handle.stop(timeout=1)

    def test_download_watchdog_handle_can_join_thread(self) -> None:
        proc = mock.Mock()
        proc.poll.return_value = None
        stream = mock.Mock()

        handle = ytdlp_session.start_download_watchdog(
            proc, stream, kill_sec=999, poll_interval=0.01)
        handle.stop(timeout=1)

        self.assertFalse(handle.thread.is_alive())

    def test_finish_ytdlp_process_closes_and_unregisters(self) -> None:
        proc = mock.Mock()
        proc.returncode = 0

        with mock.patch.object(ytdlp_session.PROCESS_REGISTRY,
                               "unregister") as unregister:
            rc = ytdlp_session.finish_ytdlp_process(proc)

        self.assertEqual(rc, 0)
        proc.wait.assert_called_once_with(timeout=10.0)
        proc.stdout.close.assert_called_once()
        unregister.assert_called_once_with(proc)

    def test_finish_ytdlp_process_escalates_after_timeout(self) -> None:
        proc = mock.Mock()
        proc.returncode = None
        proc.wait.side_effect = [
            ytdlp_session.subprocess.TimeoutExpired("yt-dlp", 10),
            ytdlp_session.subprocess.TimeoutExpired("yt-dlp", 5),
            None,
        ]

        ytdlp_session.finish_ytdlp_process(proc)

        proc.terminate.assert_called_once()
        proc.kill.assert_called_once()
        self.assertEqual(proc.wait.call_args_list[-1],
                         mock.call(timeout=2.0))


class ConfigTests(unittest.TestCase):
    def test_load_config_returns_defaults_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_file = Path(td) / "ytarchiver_config.json"

            with mock.patch.object(ytarchiver_config, "APP_DATA_DIR", Path(td)), \
                    mock.patch.object(ytarchiver_config, "CONFIG_FILE", cfg_file):
                cfg = ytarchiver_config.load_config()

            self.assertEqual(cfg["channels"], [])
            self.assertEqual(cfg["output_dir"], "")

    def test_config_transaction_persists_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_file = Path(td) / "ytarchiver_config.json"
            cfg_file.write_text(json.dumps({
                "_migration_v2_pending_tx_ids": True,
                "channels": [],
                "output_dir": "",
            }), encoding="utf-8")

            with mock.patch.object(ytarchiver_config, "APP_DATA_DIR", Path(td)), \
                    mock.patch.object(ytarchiver_config, "CONFIG_FILE", cfg_file):
                with ytarchiver_config.config_transaction() as cfg:
                    cfg["channels"].append({
                        "name": "Channel",
                        "url": "https://example.test/channel",
                    })

                loaded = ytarchiver_config.load_config()

            self.assertEqual(loaded["channels"][0]["name"], "Channel")

    def test_load_config_uses_mtime_cache_and_returns_copy(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_file = Path(td) / "ytarchiver_config.json"
            cfg_file.write_text(json.dumps({
                "_migration_v2_pending_tx_ids": True,
                "channels": [],
                "output_dir": "X:/Archive",
            }), encoding="utf-8")
            orig_json_load = json.load

            with mock.patch.object(ytarchiver_config, "APP_DATA_DIR", Path(td)), \
                    mock.patch.object(ytarchiver_config, "CONFIG_FILE", cfg_file), \
                    mock.patch.object(ytarchiver_config.json, "load",
                                      wraps=orig_json_load) as json_load:
                first = ytarchiver_config.load_config()
                first["output_dir"] = "mutated"
                second = ytarchiver_config.load_config()

            self.assertEqual(json_load.call_count, 1)
            self.assertEqual(second["output_dir"], "X:/Archive")

    def test_save_config_refreshes_load_cache(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_file = Path(td) / "ytarchiver_config.json"
            cfg = {
                "_migration_v2_pending_tx_ids": True,
                "channels": [],
                "output_dir": "Y:/Archive",
            }
            orig_json_load = json.load

            with mock.patch.object(ytarchiver_config, "APP_DATA_DIR", Path(td)), \
                    mock.patch.object(ytarchiver_config, "CONFIG_FILE", cfg_file):
                self.assertTrue(ytarchiver_config.save_config(cfg))
                with mock.patch.object(ytarchiver_config.json, "load",
                                       wraps=orig_json_load) as json_load:
                    loaded = ytarchiver_config.load_config()

            self.assertEqual(json_load.call_count, 0)
            self.assertEqual(loaded["output_dir"], "Y:/Archive")

    def test_channels_for_subs_ui_uses_unknown_for_bad_last_sync(self) -> None:
        rows, _ = ytarchiver_config.channels_for_subs_ui({
            "channels": [{"name": "Channel", "last_sync": "bad timestamp"}],
            "output_dir": "",
        })

        self.assertEqual(rows[0]["last_sync"], "unknown")

    def test_recent_for_ui_stats_only_bounded_newest_candidates(self) -> None:
        recent = [
            {
                "title": f"Video {i}",
                "channel": "Channel",
                "filepath": f"Z:/Archive/video-{i}.mp4",
                "download_ts": i,
            }
            for i in range(1000)
        ]

        with mock.patch("backend.index.find_thumbnail_channelwide",
                        return_value=None), \
                mock.patch.object(ytarchiver_config.os.path, "isfile",
                                  return_value=True) as isfile:
            rows = ytarchiver_config.recent_for_ui({"recent_downloads": recent})

        self.assertEqual(len(rows), 200)
        self.assertEqual(isfile.call_count, 300)
        self.assertEqual(rows[0]["title"], "Video 999")

    def test_pending_tx_config_failures_log_warning(self) -> None:
        with mock.patch.object(ytarchiver_config, "config_is_writable",
                               return_value=True), \
                mock.patch.object(ytarchiver_config, "config_transaction",
                                  side_effect=OSError("disk locked")):
            with self.assertLogs(ytarchiver_config._log, level="WARNING") as cm:
                ytarchiver_config.append_pending_tx_id("Channel", "abc123_def4")
                removed = ytarchiver_config.remove_pending_tx_id("abc123_def4")

        self.assertFalse(removed)
        self.assertTrue(any("append_pending_tx_id save failed" in msg
                            for msg in cm.output))
        self.assertTrue(any("remove_pending_tx_id save failed" in msg
                            for msg in cm.output))

    def test_set_parent_folder_saves_under_settings_lock(self) -> None:
        class Api(settings_mixin.SettingsMixin):
            def __init__(self):
                self._config = {}
                self._reload_config = mock.Mock()

        class TrackingLock:
            held = False

            def __enter__(self):
                self.held = True

            def __exit__(self, exc_type, exc, tb):
                self.held = False

        lock = TrackingLock()

        def fake_save(cfg):
            self.assertTrue(lock.held)
            self.assertEqual(cfg["output_dir"], target)
            return True

        with tempfile.TemporaryDirectory() as target:
            with mock.patch.object(settings_mixin.SettingsMixin,
                                   "_settings_save_lock", lock), \
                    mock.patch.object(settings_mixin, "load_config",
                                      return_value={"output_dir": ""}), \
                    mock.patch.object(ytarchiver_config, "save_config",
                                      side_effect=fake_save):
                result = Api().set_parent_folder(target)

        self.assertTrue(result["ok"])

    def test_ytdlp_update_clears_cached_version_on_success(self) -> None:
        class Api(settings_mixin.SettingsMixin):
            def __init__(self):
                self._log_stream = mock.Mock()

        class FakeProc:
            stdout = ["updated\n"]
            returncode = 0

            def wait(self):
                return self.returncode

        class ImmediateThread:
            def __init__(self, target, daemon=False):
                self.target = target
                self.daemon = daemon

            def start(self):
                self.target()

        old_cache = dict(settings_mixin.SettingsMixin._ytdlp_version_cache)
        settings_mixin.SettingsMixin._ytdlp_version_cache = {
            "yt-dlp.exe": {"ok": True, "version": "old", "path": "yt-dlp.exe"}
        }
        try:
            with mock.patch.object(settings_mixin.sync_backend, "find_yt_dlp",
                                   return_value="yt-dlp.exe"), \
                    mock.patch.object(settings_mixin.threading, "Thread",
                                      ImmediateThread), \
                    mock.patch("subprocess.Popen", return_value=FakeProc()):
                result = Api().ytdlp_update()
        finally:
            cache_after = dict(settings_mixin.SettingsMixin._ytdlp_version_cache)
            settings_mixin.SettingsMixin._ytdlp_version_cache = old_cache

        self.assertTrue(result["started"])
        self.assertNotIn("yt-dlp.exe", cache_after)

    def test_onboarding_finish_reports_save_failure(self) -> None:
        class Api(OnboardingMixin):
            def _reload_config(self):
                raise AssertionError("_reload_config should not run")

        with mock.patch("backend.api_mixins.onboarding_mixin.load_config",
                        return_value={}), \
                mock.patch("backend.api_mixins.onboarding_mixin.save_config",
                           return_value=False):
            result = Api().onboarding_finish()

        self.assertFalse(result["ok"])
        self.assertIn("config save failed", result["error"])

    def test_transcribe_swap_model_reloads_after_persist_save(self) -> None:
        class Api(TranscribeMixin):
            def __init__(self):
                self._transcribe = mock.Mock()
                self._transcribe.swap_model.return_value = True
                self._reload_config = mock.Mock()

        api = Api()
        with mock.patch("backend.api_mixins.transcribe_mixin.load_config",
                        return_value={}), \
                mock.patch("backend.ytarchiver_config.save_config",
                           return_value=True):
            result = api.transcribe_swap_model("small", persist=True)

        self.assertTrue(result["persisted"])
        api._reload_config.assert_called_once()


class AppServicesTests(unittest.TestCase):
    def test_fresh_config_uses_injected_loader(self) -> None:
        services = AppServices(
            load_config=lambda: {"output_dir": "X:/Archive"},
            save_config=lambda cfg: True,
            queues=mock.Mock(),
            log_stream=mock.Mock(),
            transcribe=mock.Mock(),
            event_bus=mock.Mock(),
        )

        self.assertEqual(services.fresh_config()["output_dir"], "X:/Archive")


class SettingsMixinServicesTests(unittest.TestCase):
    def test_settings_mixin_prefers_app_services_dependencies(self) -> None:
        saved: list[dict] = []
        service_log = mock.Mock()
        service_transcribe = mock.Mock()

        def fresh_config():
            return {
                "output_dir": "FreshArchive",
                "video_out_dir": "FreshManual",
                "whisper_model": "tiny",
                "default_resolution": "720",
                "log_mode": "Simple",
            }

        class Api(settings_mixin.SettingsMixin):
            def __init__(self):
                self._config = {
                    "output_dir": "stale-cache",
                    "log_mode": "Verbose",
                }
                self._log_stream = mock.Mock()
                self._transcribe = mock.Mock()
                self._reload_config = mock.Mock()
                self.services = AppServices(
                    load_config=fresh_config,
                    save_config=lambda cfg: saved.append(dict(cfg)) or True,
                    queues=mock.Mock(),
                    log_stream=service_log,
                    transcribe=service_transcribe,
                    event_bus=mock.Mock(),
                )

        api = Api()
        with mock.patch("backend.api_mixins.settings_mixin.load_config",
                        side_effect=AssertionError("use services")), \
                mock.patch("backend.api_mixins.settings_mixin.config_is_writable",
                           return_value=True):
            loaded = api.settings_load()
            mode_result = api.set_log_mode("Verbose")
            save_result = api.settings_save({
                "log_mode": "Simple",
                "whisper_model": "small",
            })

        self.assertEqual(loaded["output_dir"], "FreshArchive")
        self.assertTrue(mode_result["ok"])
        self.assertTrue(save_result["ok"])
        self.assertEqual(saved[-2]["log_mode"], "Verbose")
        self.assertEqual(saved[-1]["whisper_model"], "small")
        self.assertTrue(service_log.simple_mode)
        service_transcribe.swap_model.assert_called_once_with("small")
        api._log_stream.emit_dim.assert_not_called()
        api._transcribe.swap_model.assert_not_called()
        self.assertEqual(api._reload_config.call_count, 1)

    def test_ytdlp_update_prefers_app_services_log_stream(self) -> None:
        service_log = mock.Mock()

        class Api(settings_mixin.SettingsMixin):
            def __init__(self):
                self._log_stream = mock.Mock()
                self.services = AppServices(
                    load_config=lambda: {},
                    save_config=lambda cfg: True,
                    queues=mock.Mock(),
                    log_stream=service_log,
                    transcribe=mock.Mock(),
                    event_bus=mock.Mock(),
                )

        class FakeProc:
            stdout = ["updated\n"]
            returncode = 0

            def wait(self):
                return self.returncode

        class ImmediateThread:
            def __init__(self, target, daemon=False):
                self.target = target

            def start(self):
                self.target()

        old_cache = dict(settings_mixin.SettingsMixin._ytdlp_version_cache)
        settings_mixin.SettingsMixin._ytdlp_version_cache = {
            "yt-dlp.exe": {"ok": True, "version": "old", "path": "yt-dlp.exe"}
        }
        try:
            with mock.patch.object(settings_mixin.sync_backend, "find_yt_dlp",
                                   return_value="yt-dlp.exe"), \
                    mock.patch.object(settings_mixin.threading, "Thread",
                                      ImmediateThread), \
                    mock.patch("subprocess.Popen", return_value=FakeProc()):
                result = Api().ytdlp_update()
        finally:
            cache_after = dict(settings_mixin.SettingsMixin._ytdlp_version_cache)
            settings_mixin.SettingsMixin._ytdlp_version_cache = old_cache

        self.assertTrue(result["started"])
        self.assertNotIn("yt-dlp.exe", cache_after)
        service_log.emit.assert_any_call([
            ["[Update] ", "update_head"],
            ["Updating yt-dlp...\n", "update_sep"],
        ])
        service_log.emit_dim.assert_called_once_with(" updated")
        service_log.flush.assert_called_once()


class OnboardingMixinServicesTests(unittest.TestCase):
    def test_onboarding_mixin_prefers_app_services_config(self) -> None:
        saved: list[dict] = []

        class Api(OnboardingMixin):
            def __init__(self):
                self._config = {"output_dir": "stale-cache"}
                self._reload_config = mock.Mock()
                self.services = AppServices(
                    load_config=lambda: {
                        "onboarded": False,
                        "output_dir": "X:/Archive",
                    },
                    save_config=lambda cfg: saved.append(dict(cfg)) or True,
                    queues=mock.Mock(),
                    log_stream=mock.Mock(),
                    transcribe=mock.Mock(),
                    event_bus=mock.Mock(),
                )

        api = Api()
        with mock.patch("backend.api_mixins.onboarding_mixin.load_config",
                        side_effect=AssertionError("use services")), \
                mock.patch("backend.api_mixins.onboarding_mixin.save_config",
                           side_effect=AssertionError("use services")), \
                mock.patch("backend.api_mixins.onboarding_mixin._deps.probe",
                           return_value={"yt_dlp": {"ok": True}}):
            state = api.onboarding_state()
            finish = api.onboarding_finish()

        self.assertEqual(state["output_dir"], "X:/Archive")
        self.assertFalse(state["onboarded"])
        self.assertTrue(finish["ok"])
        self.assertEqual(saved[-1]["onboarded"], True)
        api._reload_config.assert_called_once()


class QueueMixinServicesTests(unittest.TestCase):
    def test_queue_mixin_prefers_app_services_dependencies(self) -> None:
        saved: list[dict] = []
        service_queues = mock.Mock()
        service_queues.to_ui_payload.return_value = {"sync": [], "gpu": []}
        service_queues.sync = [{"name": "Queued"}]
        service_log = mock.Mock()
        service_transcribe = mock.Mock()

        class Api(QueueMixin):
            def __init__(self):
                self._queues = mock.Mock()
                self._transcribe = mock.Mock()
                self._log_stream = mock.Mock()
                self._config = None
                self._sync_pause = threading.Event()
                self._on_queue_changed = mock.Mock()
                self.sync_is_running = mock.Mock(return_value=False)
                self.sync_start_all = mock.Mock()
                self.services = AppServices(
                    load_config=lambda: {
                        "autorun_sync": False,
                        "autorun_gpu": False,
                    },
                    save_config=lambda cfg: saved.append(dict(cfg)) or True,
                    queues=service_queues,
                    log_stream=service_log,
                    transcribe=service_transcribe,
                    event_bus=mock.Mock(),
                )

        api = Api()

        self.assertEqual(api.get_queues(), {"sync": [], "gpu": []})
        result = api.queue_auto_set("gpu", True)
        pause_result = api.queue_pause("both")

        self.assertTrue(result["ok"])
        self.assertEqual(saved[-1]["autorun_gpu"], True)
        service_transcribe._ensure_worker.assert_called_once()
        service_log.emit_text.assert_called_once()
        self.assertEqual(pause_result, {"ok": True, "paused": "both"})
        service_queues.set_sync_paused.assert_called_once_with(True)
        service_queues.set_gpu_paused.assert_called_once_with(True)
        service_transcribe.pause.assert_called_once()
        api._queues.to_ui_payload.assert_not_called()
        api._transcribe._ensure_worker.assert_not_called()
        api._log_stream.emit_text.assert_not_called()


class WindowMixinServicesTests(unittest.TestCase):
    def test_confirm_close_remember_prefers_app_services_dependencies(self) -> None:
        saved: list[dict] = []
        service_log = mock.Mock()
        service_queues = mock.Mock()
        started_threads = []

        class FakeThread:
            def __init__(self, target, daemon=False):
                self.target = target
                self.daemon = daemon

            def start(self):
                started_threads.append(self)

        class Api(WindowMixin):
            def __init__(self):
                self._config = {"close_behavior": "stale"}
                self._log_stream = mock.Mock()
                self._queues = mock.Mock()
                self._window = mock.Mock()
                self._reload_config = mock.Mock()
                self._close_dialog_pending = True
                self.services = AppServices(
                    load_config=lambda: {
                        "close_behavior": "prompt",
                        "other": "kept",
                    },
                    save_config=lambda cfg: saved.append(dict(cfg)) or False,
                    queues=service_queues,
                    log_stream=service_log,
                    transcribe=mock.Mock(),
                    event_bus=mock.Mock(),
                )

        api = Api()
        with mock.patch("backend.api_mixins.window_mixin.load_config",
                        side_effect=AssertionError("use services")), \
                mock.patch("backend.api_mixins.window_mixin.save_config",
                           side_effect=AssertionError("use services")), \
                mock.patch.object(window_mixin.threading, "Thread",
                                  side_effect=FakeThread):
            result = api.confirm_close("tray", remember=True)

        self.assertEqual(result, {"ok": True, "action": "tray"})
        self.assertFalse(api._close_dialog_pending)
        self.assertEqual(saved[-1], {
            "close_behavior": "tray",
            "other": "kept",
        })
        service_log.emit_dim.assert_called_once_with(
            " (close behavior preference not saved)")
        api._log_stream.emit_dim.assert_not_called()
        api._reload_config.assert_not_called()
        self.assertEqual(len(started_threads), 1)
        api._window.hide.assert_not_called()


class IndexMixinServicesTests(unittest.TestCase):
    def test_index_count_transcripts_prefers_app_services_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Video Transcript.txt").write_text(
                "transcript", encoding="utf-8")
            (root / ".Channel Video Transcript.jsonl").write_text(
                "{}", encoding="utf-8")
            (root / ".Channel Metadata.jsonl").write_text(
                "{}", encoding="utf-8")

            class Api(IndexMixin):
                def __init__(self):
                    self._config = {"output_dir": ""}
                    self.services = AppServices(
                        load_config=lambda: {"output_dir": tmp},
                        save_config=lambda cfg: True,
                        queues=mock.Mock(),
                        log_stream=mock.Mock(),
                        transcribe=mock.Mock(),
                        event_bus=mock.Mock(),
                    )

            with mock.patch("backend.api_mixins.index_mixin.load_config",
                            side_effect=AssertionError("use services")):
                result = Api().index_count_transcripts()

        self.assertTrue(result["ok"])
        self.assertEqual(result["txt_count"], 1)
        self.assertEqual(result["jsonl_count"], 1)
        self.assertEqual(result["total"], 2)

    def test_index_rebuild_fts_prefers_app_services_log_stream(self) -> None:
        service_log = mock.Mock()

        class ImmediateThread:
            def __init__(self, target, daemon=False):
                self.target = target

            def start(self):
                self.target()

        class Api(IndexMixin):
            def __init__(self):
                self._log_stream = mock.Mock()
                self.services = AppServices(
                    load_config=lambda: {},
                    save_config=lambda cfg: True,
                    queues=mock.Mock(),
                    log_stream=service_log,
                    transcribe=mock.Mock(),
                    event_bus=mock.Mock(),
                )

        api = Api()
        with mock.patch("backend.api_mixins.index_mixin.threading.Thread",
                        ImmediateThread), \
                mock.patch("backend.api_mixins.index_mixin.index_backend"
                           ".rebuild_fts_index",
                           return_value={"ok": True, "rows_indexed": 12}):
            result = api.index_rebuild_fts()

        self.assertEqual(result, {"ok": True, "started": True})
        service_log.emit_text.assert_any_call(
            "Rebuilding FTS search index from scratch\u2026",
            "simpleline_blue")
        service_log.emit_text.assert_any_call(
            "\u2014 FTS rebuild complete: 12 rows indexed.",
            "simpleline_green")
        api._log_stream.emit_text.assert_not_called()
        api._log_stream.emit_error.assert_not_called()
        self.assertFalse(api._fts_rebuild_running)


class TranscribeMixinServicesTests(unittest.TestCase):
    def test_transcribe_mixin_prefers_app_services_dependencies(self) -> None:
        saved: list[dict] = []
        service_transcribe = mock.Mock()
        service_transcribe.swap_model.return_value = True
        service_transcribe.enqueue.return_value = True
        service_transcribe.queue_size.return_value = 7
        service_transcribe.current_model.return_value = "small"
        service_transcribe.is_available.return_value = True
        service_transcribe._python311 = r"C:\Python311\python.exe"
        service_transcribe._worker_script = mock.Mock()
        service_transcribe._worker_script.exists.return_value = True
        service_log = mock.Mock()

        class Api(TranscribeMixin):
            def __init__(self):
                self._transcribe = mock.Mock()
                self._log_stream = mock.Mock()
                self._on_queue_changed = mock.Mock()
                self._reload_config = mock.Mock()
                self.services = AppServices(
                    load_config=lambda: {"whisper_model": "tiny"},
                    save_config=lambda cfg: saved.append(dict(cfg)) or True,
                    queues=mock.Mock(),
                    log_stream=service_log,
                    transcribe=service_transcribe,
                    event_bus=mock.Mock(),
                )

        api = Api()

        with mock.patch("backend.index._reader_open", return_value=None):
            enqueue_result = api.transcribe_enqueue(
                r"C:\video.mp4", "Video", "medium")
        size_result = api.transcribe_queue_size()
        cancel_result = api.transcribe_cancel_all()
        available_result = api.transcribe_available()
        swap_result = api.transcribe_swap_model("small", persist=True)
        current_result = api.transcribe_current_model()
        missing_result = api.transcribe_retranscribe(
            r"C:\missing.mp4", "Missing")

        self.assertTrue(enqueue_result["ok"])
        service_transcribe.swap_model.assert_any_call("medium")
        service_transcribe.enqueue.assert_called_once_with(
            r"C:\video.mp4", "Video", channel="")
        api._on_queue_changed.assert_called_once()
        self.assertEqual(size_result, {"size": 7})
        self.assertEqual(cancel_result, {"ok": True})
        service_transcribe.cancel_all.assert_called_once()
        self.assertEqual(available_result, {
            "ok": True,
            "python311": r"C:\Python311\python.exe",
            "worker_script_exists": True,
        })
        self.assertTrue(swap_result["persisted"])
        service_transcribe.swap_model.assert_any_call("small")
        self.assertEqual(saved[-1]["whisper_model"], "small")
        api._reload_config.assert_called_once()
        self.assertEqual(current_result, {"model": "small"})
        self.assertFalse(missing_result["ok"])
        service_log.emit_text.assert_called_once()
        api._transcribe.enqueue.assert_not_called()
        api._transcribe.swap_model.assert_not_called()
        api._log_stream.emit_text.assert_not_called()


class BridgeEventBusTests(unittest.TestCase):
    def test_js_value_escapes_script_close_sequence(self) -> None:
        self.assertEqual(
            BridgeEventBus.js_value({"html": "</script>"}),
            '{"html": "<\\/script>"}',
        )

    def test_update_queues_emits_render_and_state_calls(self) -> None:
        window = mock.Mock()
        bus = BridgeEventBus(lambda: window)

        self.assertTrue(bus.update_queues(
            {"sync": [], "gpu": []},
            {"sync": {"running": False}, "gpu": {"running": True}},
        ))

        script = window.evaluate_js.call_args.args[0]
        self.assertIn("window.renderQueues", script)
        self.assertIn("window.setQueueState", script)
        self.assertIn('"running": true', script)

    def test_show_toast_can_emit_object_payload(self) -> None:
        window = mock.Mock()
        bus = BridgeEventBus(lambda: window)

        self.assertTrue(bus.show_toast("Missing deps", "error", ttl_ms=12000))

        script = window.evaluate_js.call_args.args[0]
        self.assertIn("window._showToast", script)
        self.assertIn('"ttlMs": 12000', script)

    def test_show_toast_and_refresh_subs_emits_both_calls(self) -> None:
        window = mock.Mock()
        bus = BridgeEventBus(lambda: window)

        self.assertTrue(bus.show_toast_and_refresh_subs("Queued", "ok"))

        script = window.evaluate_js.call_args.args[0]
        self.assertIn("window._showToast", script)
        self.assertIn("window.refreshSubsTable", script)

    def test_evaluate_returns_false_when_window_getter_raises(self) -> None:
        bus = BridgeEventBus(lambda: (_ for _ in ()).throw(RuntimeError("gone")))

        self.assertFalse(bus.evaluate("window.noop()"))

    def test_evaluate_returns_false_when_evaluate_js_raises(self) -> None:
        window = mock.Mock()
        window.evaluate_js.side_effect = RuntimeError("renderer gone")
        bus = BridgeEventBus(lambda: window)

        self.assertFalse(bus.call("_showToast", "hi"))


class QueueStateTests(unittest.TestCase):
    def test_queue_save_load_and_resuming_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            queue_file = Path(td) / "queue.json"
            q1 = queues.QueueState()
            q2 = queues.QueueState()
            try:
                with mock.patch.object(queues, "QUEUE_FILE", queue_file), \
                        mock.patch.object(queues, "config_is_writable",
                                          return_value=True), \
                        mock.patch.object(queues.QueueState, "save_debounced",
                                          return_value=None):
                    self.assertTrue(q1.sync_enqueue(
                        {"name": "Channel", "url": "https://example.test/c"}))
                    self.assertTrue(q1.gpu_enqueue(
                        {"task_id": "gpu-1", "path": "video.mp4"}))
                    q1.current_sync = {"name": "Current",
                                       "url": "https://example.test/current"}
                    q1.current_gpu = {"task_id": "gpu-current",
                                      "path": "current.mp4"}

                    self.assertTrue(q1.save_now())
                    self.assertTrue(q2.load())

                self.assertEqual(q2.sync[0]["name"], "Channel")
                self.assertEqual(q2.gpu[0]["task_id"], "gpu-1")
                self.assertEqual(q2.get_loaded_resuming()["sync"]["name"],
                                 "Current")
                self.assertEqual(q2.get_loaded_resuming()["gpu"]["task_id"],
                                 "gpu-current")
            finally:
                q1.mark_orphan()
                q2.mark_orphan()

    def test_sync_snapshot_is_lock_protected_copy(self) -> None:
        q = queues.QueueState()
        try:
            q.sync = [{"name": "One", "url": "u1"}]

            snap = q.sync_snapshot()
            snap[0]["name"] = "Changed"
            snap.append({"name": "Two", "url": "u2"})

            self.assertEqual(q.sync, [{"name": "One", "url": "u1"}])
        finally:
            q.mark_orphan()

    def test_corrupt_queue_file_is_sidelined(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            queue_file = Path(td) / "queue.json"
            queue_file.write_text("{not json", encoding="utf-8")
            q = queues.QueueState()
            try:
                with mock.patch.object(queues, "QUEUE_FILE", queue_file):
                    self.assertFalse(q.load())
                    self.assertFalse(queue_file.exists())
                    self.assertTrue(Path(str(queue_file) + ".bak").exists())
            finally:
                q.mark_orphan()

    def test_queue_save_failure_warns_once(self) -> None:
        q = queues.QueueState()
        try:
            with mock.patch("builtins.open",
                            side_effect=OSError("disk offline")), \
                    mock.patch.object(queues._log, "warning") as warn:
                self.assertFalse(q._write_save_payload({"sync": []}))
                self.assertFalse(q._write_save_payload({"sync": []}))

            warn.assert_called_once()
            self.assertIn("Queue state could not be saved",
                          warn.call_args.args[0])
        finally:
            q.mark_orphan()

    def test_save_debounced_reuses_single_saver_thread(self) -> None:
        q = queues.QueueState()

        class FakeThread:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs
                self.started = False

            def start(self):
                self.started = True

            def is_alive(self):
                return self.started

        made = []

        def fake_thread(*args, **kwargs):
            t = FakeThread(*args, **kwargs)
            made.append(t)
            return t

        try:
            with mock.patch.object(queues.threading, "Thread",
                                   side_effect=fake_thread), \
                    mock.patch.object(queues.threading, "Timer") as timer:
                q.save_debounced()
                first_deadline = q._save_deadline
                q.save_debounced()
                q.save_debounced()

            self.assertEqual(len(made), 1)
            self.assertGreaterEqual(q._save_deadline, first_deadline)
            timer.assert_not_called()
        finally:
            q.mark_orphan()

    def test_notify_reuses_single_dispatcher_thread(self) -> None:
        q = queues.QueueState()

        class FakeThread:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs
                self.started = False

            def start(self):
                self.started = True

            def is_alive(self):
                return self.started

        made = []

        def fake_thread(*args, **kwargs):
            t = FakeThread(*args, **kwargs)
            made.append(t)
            return t

        try:
            q.add_listener(lambda: None)
            with mock.patch.object(queues.threading, "Thread",
                                   side_effect=fake_thread):
                q._notify()
                q._notify()
                q._notify()

            self.assertEqual(len(made), 1)
            self.assertEqual(made[0].kwargs.get("name"), "queues-notify")
            target = made[0].kwargs.get("target")
            self.assertIs(target.__self__, q)
            self.assertIs(target.__func__, queues.QueueState._notify_loop)
            self.assertTrue(q._notify_dirty)
        finally:
            q.mark_orphan()

    def test_current_item_save_false_schedules_retry(self) -> None:
        q = queues.QueueState()
        try:
            with mock.patch.object(queues, "config_is_writable",
                                   return_value=True), \
                    mock.patch.object(q, "_write_resuming_payload",
                                      return_value=False), \
                    mock.patch.object(q, "_write_save_payload") as full_save, \
                    mock.patch.object(q, "save_debounced") as retry:
                q.set_current_sync({"name": "C", "url": "u"})
                q.set_current_gpu({"title": "V", "path": "v.mp4"})

            self.assertEqual(retry.call_count, 2)
            full_save.assert_not_called()
        finally:
            q.mark_orphan()

    def test_current_item_sidecar_overrides_main_resuming(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            queue_file = Path(td) / "queue.json"
            q1 = queues.QueueState()
            q2 = queues.QueueState()
            try:
                with mock.patch.object(queues, "QUEUE_FILE", queue_file), \
                        mock.patch.object(queues, "config_is_writable",
                                          return_value=True), \
                        mock.patch.object(queues.QueueState, "save_debounced",
                                          return_value=None):
                    q1.current_sync = {"name": "Stale", "url": "old"}
                    self.assertTrue(q1.save_now())

                    q1.set_current_sync({"name": "Current", "url": "new"})
                    sidecar = q1._resuming_file()
                    self.assertTrue(sidecar.exists())
                    self.assertTrue(q2.load())

                self.assertEqual(q2.get_loaded_resuming()["sync"]["name"],
                                 "Current")
            finally:
                q1.mark_orphan()
                q2.mark_orphan()

    def test_empty_current_item_sidecar_clears_main_resuming(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            queue_file = Path(td) / "queue.json"
            q1 = queues.QueueState()
            q2 = queues.QueueState()
            try:
                with mock.patch.object(queues, "QUEUE_FILE", queue_file), \
                        mock.patch.object(queues, "config_is_writable",
                                          return_value=True), \
                        mock.patch.object(queues.QueueState, "save_debounced",
                                          return_value=None):
                    q1.current_sync = {"name": "Stale", "url": "old"}
                    self.assertTrue(q1.save_now())

                    q1.set_current_sync(None)
                    sidecar = q1._resuming_file()
                    self.assertTrue(sidecar.exists())
                    self.assertTrue(q2.load())

                self.assertEqual(q2.get_loaded_resuming(), {})
            finally:
                q1.mark_orphan()
                q2.mark_orphan()

    def test_gpu_payload_includes_kind_title_and_channel(self) -> None:
        q = queues.QueueState()
        try:
            q.current_gpu = {
                "kind": "compress",
                "title": "Video Title",
                "channel": "Channel Name",
                "path": "video.mp4",
            }
            row = q.to_ui_payload()["gpu"][0]

            self.assertEqual(row["kind"], "compress")
            self.assertEqual(row["title"], "Video Title")
            self.assertEqual(row["channel"], "Channel Name")
            self.assertTrue(row["name"].startswith("Compressing "))
        finally:
            q.mark_orphan()

    def test_sync_remove_at_uses_identity_when_index_stale(self) -> None:
        q = queues.QueueState()
        try:
            q.sync = [
                {"name": "One", "url": "https://example.test/one"},
                {"name": "Two", "url": "https://example.test/two"},
            ]

            self.assertTrue(q.sync_remove_at(
                0, expected_url="https://example.test/two"))

            self.assertEqual(q.sync,
                             [{"name": "One",
                               "url": "https://example.test/one"}])
            self.assertFalse(q.sync_remove_at(
                0, expected_url="https://example.test/missing"))
            self.assertEqual(len(q.sync), 1)
        finally:
            q.mark_orphan()

    def test_gpu_remove_at_uses_identity_when_index_stale(self) -> None:
        q = queues.QueueState()
        try:
            q.gpu = [
                {"path": "one.mp4", "bulk_id": "b1"},
                {"path": "two.mp4", "bulk_id": "b2"},
            ]

            self.assertTrue(q.gpu_remove_at(0, expected_path="two.mp4"))

            self.assertEqual(q.gpu, [{"path": "one.mp4", "bulk_id": "b1"}])
            self.assertFalse(q.gpu_remove_at(0, expected_path="missing.mp4"))
            self.assertEqual(len(q.gpu), 1)
        finally:
            q.mark_orphan()

    def test_gpu_pop_matching_uses_identity_when_queue_reordered(self) -> None:
        q = queues.QueueState()
        try:
            q.gpu = [
                {"path": "one.mp4", "bulk_id": "b1"},
                {"path": "two.mp4", "bulk_id": "b2"},
            ]

            popped = q.gpu_pop_matching(expected_path="two.mp4")

            self.assertEqual(popped, {"path": "two.mp4", "bulk_id": "b2"})
            self.assertEqual(q.gpu, [{"path": "one.mp4", "bulk_id": "b1"}])
        finally:
            q.mark_orphan()


class DiskWatchTests(unittest.TestCase):
    def test_retry_tick_recovery_resumes_once_when_raced(self) -> None:
        stream = mock.Mock()
        on_resume = mock.Mock()
        monitor = disk_watch.DiskErrorMonitor(
            stream, mock.Mock(), on_resume, lambda: "Z:\\Archive")
        monitor._active = True
        monitor._path = "Z:\\Archive"
        barrier = threading.Barrier(2)

        def check(_path):
            barrier.wait(timeout=2)
            return True

        with mock.patch.object(disk_watch, "_check_directory_writable",
                               side_effect=check):
            t1 = threading.Thread(target=monitor._retry_tick)
            t2 = threading.Thread(target=monitor._retry_tick)
            t1.start()
            t2.start()
            t1.join(timeout=3)
            t2.join(timeout=3)

        self.assertFalse(t1.is_alive())
        self.assertFalse(t2.is_alive())
        on_resume.assert_called_once()


class SyncMixinQueueTests(unittest.TestCase):
    def test_gpu_defer_removes_existing_duplicate_before_enqueue(self) -> None:
        api = sync_mixin.SyncMixin()
        queues_mock = mock.Mock()
        queues_mock.current_gpu = {"title": "Video", "path": "video.mp4"}
        api._queues = queues_mock
        api._log_stream = mock.Mock()
        api.gpu_skip_current = mock.Mock(return_value={"ok": True})

        result = api.gpu_defer_current()

        self.assertEqual(result, {"ok": True})
        queues_mock.gpu_remove.assert_called_once_with("video.mp4")
        queues_mock.gpu_enqueue.assert_called_once_with(
            {"title": "Video", "path": "video.mp4"})
        api.gpu_skip_current.assert_called_once()


class IndexGraphShapeTests(unittest.TestCase):
    def test_graph_word_frequency_multi_merges_labels_stably(self) -> None:
        fake = {
            "alpha": {"labels": ["2026-01", "2026-03"], "values": [2, 5]},
            "beta": {"labels": ["2026-02", "2026-03"], "values": [7, 11]},
        }

        def _fake_graph(word: str, channel=None, bucket: str = "month"):
            return fake[word]

        with mock.patch.object(index_graph, "graph_word_frequency",
                               side_effect=_fake_graph):
            out = index_graph.graph_word_frequency_multi(
                ["alpha", "beta"], channel="Any", bucket="month")

        self.assertEqual(out["labels"], ["2026-01", "2026-02", "2026-03"])
        self.assertEqual(out["series"][0],
                         {"word": "alpha", "values": [2, 0, 5]})
        self.assertEqual(out["series"][1],
                         {"word": "beta", "values": [0, 7, 11]})

    def test_graph_multi_hoists_week_backfill_pending_count(self) -> None:
        fake_conn = object()

        class FakeIndex:
            def _reader_open(self):
                return fake_conn

        calls = []

        def _fake_graph(word: str, channel=None, bucket: str = "month",
                        _backfill_pending=None):
            calls.append(_backfill_pending)
            return {"labels": ["2026-W01"], "values": [1]}

        with mock.patch.object(index_graph, "_index",
                               return_value=FakeIndex()), \
                mock.patch.object(index_graph, "_week_backfill_pending",
                                  return_value=42) as pending, \
                mock.patch.object(index_graph, "graph_word_frequency",
                                  side_effect=_fake_graph):
            out = index_graph.graph_multi(
                ["alpha", "beta"], channel="Any", bucket="week")

        self.assertEqual(out["labels"], ["2026-W01"])
        self.assertEqual(calls, [42, 42])
        pending.assert_called_once_with(fake_conn)

    def test_top_words_cache_reuses_result_until_invalidated(self) -> None:
        class FakeConn:
            def __init__(self):
                self.calls = 0

            def execute(self, _sql, _args):
                self.calls += 1
                return [("alpha beta alpha",)]

            def close(self):
                pass

        conn = FakeConn()

        class FakeIndex:
            def _open_independent(self):
                return conn

        index_graph.invalidate_top_words_cache()
        with mock.patch.object(index_graph, "_index",
                               return_value=FakeIndex()):
            first = index_graph.top_words(top_n=2)
            second = index_graph.top_words(top_n=2)
            index_graph.invalidate_top_words_cache()
            third = index_graph.top_words(top_n=2)

        self.assertEqual(first, second)
        self.assertEqual(third, first)
        self.assertEqual(conn.calls, 2)

    def test_top_words_capped_query_orders_sample_stably(self) -> None:
        class FakeConn:
            def __init__(self):
                self.sql = ""

            def execute(self, sql, _args):
                self.sql = sql
                return [("alpha beta alpha",)]

            def close(self):
                pass

        conn = FakeConn()

        class FakeIndex:
            def _open_independent(self):
                return conn

        index_graph.invalidate_top_words_cache()
        with mock.patch.object(index_graph, "_index",
                               return_value=FakeIndex()):
            index_graph.top_words(top_n=2)

        self.assertIn("ORDER BY id LIMIT 500000", conn.sql)


class IndexIngestTests(unittest.TestCase):
    def tearDown(self) -> None:
        self._reset_index_module()

    @staticmethod
    def _reset_index_module() -> None:
        try:
            index._shutdown_index()
        finally:
            index._conn = None
            index._reader_conn = None
            index._schema_inited = False
            index._ingest_locks.clear()
            index_search._title_search_cache.clear()

    def test_ingest_reingest_and_delete_keep_fts_in_sync(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "transcription_index.db"
            video_dir = Path(td) / "Channel" / "2026" / "01 January"
            video_dir.mkdir(parents=True)
            video_path = video_dir / "Video [abc123_def4].mp4"
            jsonl_path = video_dir / "Video [abc123_def4].jsonl"
            video_path.write_bytes(b"video")
            jsonl_path.write_text(
                json.dumps({"video_id": "abc123_def4", "title": "Video",
                            "start": 0, "end": 1,
                            "text": "hello archive"}) + "\n" +
                json.dumps({"video_id": "abc123_def4", "title": "Video",
                            "start": 1, "end": 2,
                            "text": ""}) + "\n",
                encoding="utf-8")

            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                self._reset_index_module()
                self.assertTrue(index.register_video(
                    str(video_path), "Channel", "Video",
                    video_id="abc123_def4"))

                inserted = index.ingest_jsonl(
                    str(video_path), str(jsonl_path), "Video", "Channel")

                conn = index._open()
                self.assertIsNotNone(conn)
                assert conn is not None
                self.assertEqual(inserted, 1)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments").fetchone()[0], 1)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments_fts "
                    "WHERE segments_fts MATCH 'hello'").fetchone()[0], 1)

                jsonl_path.write_text(
                    json.dumps({"video_id": "abc123_def4", "title": "Video",
                                "start": 2, "end": 3,
                                "text": "fresh transcript"}) + "\n",
                    encoding="utf-8")
                self.assertEqual(index.ingest_jsonl(
                    str(video_path), str(jsonl_path), "Video", "Channel"), 1)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments_fts "
                    "WHERE segments_fts MATCH 'hello'").fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments_fts "
                    "WHERE segments_fts MATCH 'fresh'").fetchone()[0], 1)

                self.assertEqual(index.delete_segments_for_video(str(video_path)), 1)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments").fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments_fts "
                    "WHERE segments_fts MATCH 'fresh'").fetchone()[0], 0)
                self._reset_index_module()

    def test_watch_segments_use_filepath_id_over_stale_payload_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "transcription_index.db"
            video_path = Path(td) / "Why Cameras Feel Worse [good1234567].mp4"
            video_path.write_bytes(b"video")

            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                self._reset_index_module()
                try:
                    conn = index._open()
                    self.assertIsNotNone(conn)
                    assert conn is not None
                    conn.execute(
                        "INSERT INTO videos(filepath, title, channel, video_id) "
                        "VALUES (?, ?, ?, ?)",
                        (os.path.normpath(str(video_path)),
                         "Why Cameras Feel Worse", "MKBHD", "bad12345678"),
                    )
                    conn.execute(
                        "INSERT INTO segments("
                        "video_id, title, channel, start_time, end_time, "
                        "text, words) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        ("bad12345678", "Electric Cars", "AutoFocus",
                         0, 1, "wrong transcript", "[]"),
                    )
                    conn.execute(
                        "INSERT INTO segments("
                        "video_id, title, channel, start_time, end_time, "
                        "text, words) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        ("good1234567", "Why Cameras Feel Worse", "MKBHD",
                         0, 1, "correct transcript", "[]"),
                    )
                    conn.commit()

                    rows = index.get_segments(
                        video_id="bad12345678",
                        title="Why Cameras Feel Worse",
                        channel="MKBHD",
                        filepath=str(video_path),
                        strict_identity=True,
                    )

                    self.assertEqual([r["t"] for r in rows],
                                     ["correct transcript"])
                finally:
                    self._reset_index_module()

    def test_watch_segments_reject_mismatched_title_for_bad_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "transcription_index.db"

            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                self._reset_index_module()
                try:
                    conn = index._open()
                    self.assertIsNotNone(conn)
                    assert conn is not None
                    conn.execute(
                        "INSERT INTO segments("
                        "video_id, title, channel, start_time, end_time, "
                        "text, words) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        ("bad12345678", "Electric Cars", "AutoFocus",
                         0, 1, "wrong transcript", "[]"),
                    )
                    conn.commit()

                    rows = index.get_segments(
                        video_id="bad12345678",
                        title="Why Cameras Feel Worse",
                        channel="MKBHD",
                        strict_identity=True,
                    )

                    self.assertEqual(rows, [])
                finally:
                    self._reset_index_module()

    def test_ingest_jsonl_skips_unchanged_indexed_file_without_parsing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "transcription_index.db"
            video_dir = Path(td) / "Channel"
            video_dir.mkdir()
            video_path = video_dir / "Video [abc123_def4].mp4"
            jsonl_path = video_dir / "Video [abc123_def4].jsonl"
            video_path.write_bytes(b"video")
            jsonl_path.write_text(
                json.dumps({"video_id": "abc123_def4", "title": "Video",
                            "start": 0, "end": 1,
                            "text": "hello archive"}) + "\n",
                encoding="utf-8")

            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                self._reset_index_module()
                self.assertTrue(index.register_video(
                    str(video_path), "Channel", "Video",
                    video_id="abc123_def4"))
                conn = index._open()
                self.assertIsNotNone(conn)
                assert conn is not None
                conn.execute(
                    "UPDATE videos SET tx_status='pending' WHERE filepath=?",
                    (str(video_path),))
                conn.commit()

                with mock.patch("builtins.open",
                                side_effect=AssertionError("parsed")):
                    count = index.ingest_jsonl(
                        str(video_path), str(jsonl_path), "Video", "Channel")

                self.assertEqual(count, 1)
                self.assertEqual(conn.execute(
                    "SELECT tx_status FROM videos WHERE filepath=?",
                    (str(video_path),)).fetchone()[0], "transcribed")
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments").fetchone()[0], 1)
                self._reset_index_module()

    def test_list_videos_for_channel_uses_index_without_per_row_file_stats(
            self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "transcription_index.db"
            missing_video = (
                r"Z:\Archive\Huge Channel\2026\06 June"
                r"\Huge Video [abc123_def4].mp4"
            )

            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                self._reset_index_module()
                conn = index._open()
                self.assertIsNotNone(conn)
                assert conn is not None
                conn.execute(
                    "INSERT INTO videos("
                    "filepath, title, channel, video_id, size_bytes, "
                    "tx_status, added_ts, upload_ts, view_count, like_count"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (missing_video, "Huge Video", "Huge Channel",
                     "abc123_def4", 123, "pending", 1700000000.0,
                     1710000000.0, 1234, 56),
                )
                conn.commit()

                index._browse_videos_cache.clear()
                with mock.patch.object(index.os.path, "exists",
                                       side_effect=AssertionError("exists")), \
                        mock.patch.object(index.os.path, "isfile",
                                          side_effect=AssertionError("isfile")), \
                        mock.patch.object(index.os.path, "getmtime",
                                          side_effect=AssertionError("getmtime")):
                    rows = index.list_videos_for_channel(
                        "Huge Channel",
                        include_thumbs=False,
                    )

                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["filepath"], missing_video)
                self.assertEqual(rows[0]["upload_ts"], 1710000000.0)
                self.assertEqual(rows[0]["view_count"], 1234)
                self.assertEqual(rows[0]["like_count"], 56)
                self.assertEqual(rows[0]["views"], "1.2K")
                self._reset_index_module()

    def test_index_caches_are_bounded_lru(self) -> None:
        index._browse_videos_cache.clear()
        index._all_videos_cache.clear()
        index._thumb_index_cache.clear()
        with mock.patch.object(index, "_BROWSE_VIDEOS_CACHE_MAX", 2), \
                mock.patch.object(index, "_ALL_VIDEOS_CACHE_MAX", 2), \
                mock.patch.object(index, "_THUMB_INDEX_CACHE_MAX", 2):
            index._browse_videos_cache_put(("A", "newest", 1, True), [])
            index._browse_videos_cache_put(("B", "newest", 1, True), [])
            index._browse_videos_cache.move_to_end(("A", "newest", 1, True))
            index._browse_videos_cache_put(("C", "newest", 1, True), [])
            self.assertIn(("A", "newest", 1, True),
                          index._browse_videos_cache)
            self.assertNotIn(("B", "newest", 1, True),
                             index._browse_videos_cache)

            index._all_videos_cache_put(("recent", 60, 0, True, ""), {})
            index._all_videos_cache_put(("recent", 60, 60, True, ""), {})
            index._all_videos_cache.move_to_end(("recent", 60, 0, True, ""))
            index._all_videos_cache_put(("recent", 60, 120, True, ""), {})
            self.assertIn(("recent", 60, 0, True, ""),
                          index._all_videos_cache)
            self.assertNotIn(("recent", 60, 60, True, ""),
                             index._all_videos_cache)

            index._thumb_index_cache_put("root-a", {"mtime": 1.0})
            index._thumb_index_cache_put("root-b", {"mtime": 1.0})
            index._thumb_index_cache.move_to_end("root-a")
            index._thumb_index_cache_put("root-c", {"mtime": 1.0})
            self.assertIn("root-a", index._thumb_index_cache)
            self.assertNotIn("root-b", index._thumb_index_cache)

    def test_sidecar_id_cache_is_lru(self) -> None:
        with tempfile.TemporaryDirectory() as td, \
                mock.patch.object(index, "_SIDECAR_ID_CACHE_MAX", 2):
            index._sidecar_id_cache.clear()

            def make_pair(dirname: str, vid: str) -> str:
                d = Path(td) / dirname
                d.mkdir()
                mp4 = d / f"Video [{vid}].mp4"
                mp4.write_bytes(b"video")
                sidecar = d / f"Video [{vid}].info.json"
                sidecar.write_text(json.dumps({
                    "id": vid,
                    "_filename": str(mp4),
                    "title": f"Video {vid}",
                }), encoding="utf-8")
                return str(mp4)

            a = make_pair("a", "abc123_def4")
            b = make_pair("b", "bcd234_efg5")
            c = make_pair("c", "cde345_fgh6")

            self.assertEqual(index._resolve_id_from_sidecars(a), "abc123_def4")
            self.assertEqual(index._resolve_id_from_sidecars(b), "bcd234_efg5")
            self.assertEqual(index._resolve_id_from_sidecars(a), "abc123_def4")
            self.assertEqual(index._resolve_id_from_sidecars(c), "cde345_fgh6")

            keys = set(index._sidecar_id_cache.keys())
            self.assertIn(os.path.dirname(a), keys)
            self.assertIn(os.path.dirname(c), keys)
            self.assertNotIn(os.path.dirname(b), keys)

    def test_register_video_resolves_direct_sidecar_through_cached_map(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "transcription_index.db"
            video_dir = Path(td) / "Channel"
            video_dir.mkdir()
            video_path = video_dir / "Direct Sidecar.mp4"
            video_path.write_bytes(b"video")
            (video_dir / "Direct Sidecar.info.json").write_text(
                json.dumps({"id": "abc123_def4"}),
                encoding="utf-8")

            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                self._reset_index_module()
                try:
                    self.assertTrue(index.register_video(
                        str(video_path), "Channel", "Direct Sidecar"))
                    conn = index._open()
                    self.assertIsNotNone(conn)
                    assert conn is not None
                    row = conn.execute(
                        "SELECT video_id FROM videos WHERE filepath=?",
                        (os.path.normpath(str(video_path)),),
                    ).fetchone()
                    self.assertEqual(row[0], "abc123_def4")
                finally:
                    self._reset_index_module()

    def test_summary_uses_independent_connection(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
            db_path = Path(td) / "transcription_index.db"
            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                self._reset_index_module()
                try:
                    conn = index._open()
                    self.assertIsNotNone(conn)
                    assert conn is not None
                    conn.execute(
                        "INSERT INTO videos(filepath, title, channel) "
                        "VALUES ('a.mp4', 'A', 'Chan'), "
                        "('b.mp4', 'B', 'Chan'), "
                        "('c.mp4', 'C', 'Other')")
                    conn.execute(
                        "INSERT INTO segments("
                        "video_id, jsonl_path, title, channel, start_time, "
                        "end_time, text) VALUES ('abc123_def4', 'a.jsonl', "
                        "'A', 'Chan', 0, 1, 'hello')")
                    conn.execute(
                        "INSERT INTO bookmarks("
                        "segment_id, created, note) VALUES (1, 1.0, 'n')")
                    conn.commit()

                    with mock.patch.object(
                            index, "_reader_open",
                            side_effect=AssertionError("reader")):
                        self.assertEqual(index.summary(), {
                            "segments": 1,
                            "videos": 3,
                            "channels": 2,
                            "bookmarks": 1,
                        })
                finally:
                    self._reset_index_module()

    def test_title_search_year_filter_uses_upload_ts_boundaries(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
            db_path = Path(td) / "transcription_index.db"
            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                self._reset_index_module()
                try:
                    conn = index._open()
                    self.assertIsNotNone(conn)
                    assert conn is not None
                    ts_2024 = index_search._year_start_ts(2024) + 10
                    ts_2025 = index_search._year_start_ts(2025) + 10
                    conn.executemany(
                        "INSERT INTO videos("
                        "filepath, title, channel, video_id, upload_ts, year"
                        ") VALUES (?, ?, ?, ?, ?, ?)",
                        [
                            ("a.mp4", "Match Old", "Chan",
                             "abc123_def4", ts_2024, 2024),
                            ("b.mp4", "Match New", "Chan",
                             "bcd234_efg5", ts_2025, 2024),
                        ])
                    conn.commit()

                    rows = index_search.search_video_titles(
                        "Match", year_from=2025, year_to=2025)

                    self.assertEqual([r["title"] for r in rows],
                                     ["Match New"])
                finally:
                    self._reset_index_module()


class IndexMaintenanceTests(unittest.TestCase):
    def tearDown(self) -> None:
        IndexIngestTests._reset_index_module()

    def test_jsonl_needs_ingest_uses_indexed_file_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            jp = Path(td) / "Video.jsonl"
            conn = sqlite3.connect(":memory:")
            try:
                conn.execute(
                    "CREATE TABLE indexed_files("
                    "path TEXT PRIMARY KEY, mtime REAL, segment_count INTEGER)")

                self.assertFalse(index_maintenance._jsonl_needs_ingest(
                    conn, str(jp)))

                jp.write_text("{}", encoding="utf-8")
                mtime = os.path.getmtime(jp)
                self.assertTrue(index_maintenance._jsonl_needs_ingest(
                    conn, str(jp)))

                conn.execute(
                    "INSERT INTO indexed_files(path, mtime, segment_count) "
                    "VALUES (?, ?, ?)",
                    (os.path.normpath(str(jp)), mtime, 1))
                conn.commit()
                self.assertFalse(index_maintenance._jsonl_needs_ingest(
                    conn, str(jp)))

                conn.execute(
                    "UPDATE indexed_files SET mtime=? WHERE path=?",
                    (mtime - 10, os.path.normpath(str(jp))))
                conn.commit()
                self.assertTrue(index_maintenance._jsonl_needs_ingest(
                    conn, str(jp)))
            finally:
                conn.close()

    def test_sweep_new_videos_yields_during_channel_walk_when_busy(self) -> None:
        class FakeRows:
            def fetchall(self):
                return []

        class FakeConn:
            def execute(self, *_args, **_kwargs):
                return FakeRows()

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            channel_dir = root / "Channel"
            channel_dir.mkdir()
            (channel_dir / "Video.mp4").write_bytes(b"video")

            busy_calls = {"count": 0}

            def busy():
                busy_calls["count"] += 1
                return busy_calls["count"] in (4, 5)

            cache = {
                "u": {
                    "num_vids": 1,
                    "size_bytes": 5,
                    "sweep_fingerprint": 0,
                }
            }

            with mock.patch.object(index_maintenance._idx, "_open",
                                   return_value=mock.Mock()), \
                    mock.patch.object(index_maintenance._idx,
                                      "_open_independent",
                                      return_value=FakeConn()), \
                    mock.patch.object(index_maintenance._idx,
                                      "register_video",
                                      return_value=True) as register_video, \
                    mock.patch("backend.archive_scan.load_disk_cache",
                               return_value=cache), \
                    mock.patch("backend.archive_scan.save_disk_cache"), \
                    mock.patch("time.sleep") as sleep:
                result = index_maintenance.sweep_new_videos(
                    str(root),
                    [{"name": "Channel", "url": "u"}],
                    gpu_busy_fn=busy,
                )

        self.assertEqual(result["registered"], 1)
        register_video.assert_called_once()
        sleep.assert_called()
        self.assertGreaterEqual(busy_calls["count"], 5)

    def test_prune_missing_videos_stats_outside_writer_lock(self) -> None:
        class TrackingLock:
            def __init__(self):
                self._lock = threading.RLock()
                self.held = False

            def __enter__(self):
                self._lock.acquire()
                self.held = True
                return self

            def __exit__(self, exc_type, exc, tb):
                self.held = False
                self._lock.release()

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
            db_path = Path(td) / "transcription_index.db"
            missing_path = str(Path(td) / "missing.mp4")
            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                IndexIngestTests._reset_index_module()
                conn = index._open()
                self.assertIsNotNone(conn)
                assert conn is not None
                conn.execute(
                    "INSERT INTO videos("
                    "filepath, title, channel, video_id, size_bytes"
                    ") VALUES (?, ?, ?, ?, ?)",
                    (missing_path, "Missing", "Channel",
                     "abc123_def4", 123),
                )
                conn.commit()

                tracking_lock = TrackingLock()

                def fake_isfile(_path):
                    self.assertFalse(
                        tracking_lock.held,
                        "prune_missing_videos statted files under _db_lock",
                    )
                    return False

                with mock.patch.object(index, "_db_lock", tracking_lock), \
                        mock.patch.object(index_maintenance.os.path,
                                          "isfile",
                                          side_effect=fake_isfile):
                    result = index_maintenance.prune_missing_videos()

                self.assertEqual(result["missing"], 1)
                self.assertEqual(result["videos_removed"], 1)


class BookmarkStorageTests(unittest.TestCase):
    def tearDown(self) -> None:
        IndexIngestTests._reset_index_module()

    def test_bookmark_add_coerces_and_bounds_storage_fields(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
            db_path = Path(td) / "transcription_index.db"
            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                IndexIngestTests._reset_index_module()

                self.assertIsNone(index_bookmarks.bookmark_add(
                    "", "Title", "Channel", 1, "text", "note"))
                bid = index_bookmarks.bookmark_add(
                    " video_id ",
                    "T" * 1200,
                    "C" * 1200,
                    "12.5",
                    "x" * 21000,
                    "n" * 5000,
                )
                rows = index_bookmarks.bookmark_list(limit=10)

        self.assertIsNotNone(bid)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["video_id"], "video_id")
        self.assertEqual(rows[0]["start_time"], 12.5)
        self.assertEqual(len(rows[0]["title"]), 1000)
        self.assertEqual(len(rows[0]["channel"]), 1000)
        self.assertEqual(len(rows[0]["text"]), 20000)
        self.assertEqual(len(rows[0]["note"]), 4000)

    def test_bookmark_list_and_mutations_coerce_bad_inputs(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
            db_path = Path(td) / "transcription_index.db"
            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                IndexIngestTests._reset_index_module()
                first = index_bookmarks.bookmark_add(
                    "v1", "Title 1", "Channel", float("inf"), "text", "")
                second = index_bookmarks.bookmark_add(
                    "v2", "Title 2", "Channel", -5, "text", "")

                rows = index_bookmarks.bookmark_list(limit=-1)
                bad_remove = index_bookmarks.bookmark_remove("nope")
                bad_update = index_bookmarks.bookmark_update_note(0, "note")
                good_update = index_bookmarks.bookmark_update_note(
                    second, "z" * 5000)
                updated = index_bookmarks.bookmark_list(limit=10)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertEqual(len(rows), 1)
        self.assertFalse(bad_remove)
        self.assertFalse(bad_update)
        self.assertTrue(good_update)
        by_id = {row["id"]: row for row in updated}
        self.assertEqual(by_id[first]["start_time"], 0.0)
        self.assertEqual(by_id[second]["start_time"], 0.0)
        self.assertEqual(len(by_id[second]["note"]), 4000)


class NoSpeechStatusTests(unittest.TestCase):
    """Q2: tx_status='no_speech' persistence + per-video lookup + counts.
    Q1: archive-wide transcribed/total %% (no_speech NOT counted)."""

    def tearDown(self) -> None:
        IndexIngestTests._reset_index_module()

    def test_no_speech_persist_lookup_counts_and_archive_pct(self) -> None:
        from backend import archive_scan
        # ignore_cleanup_errors: on Windows the index's reader connection can
        # still hold the .db open at tempdir teardown; the connection is
        # closed by _reset_index_module, but GC timing makes rmtree flaky.
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
            db_path = Path(td) / "transcription_index.db"
            chan_dir = Path(td) / "Chan"
            chan_dir.mkdir(parents=True)
            # Real YouTube ids are 11 chars; the id extractor rejects
            # shorter/alpha-only hints, so use valid 11-char ids (also
            # embedded in the filename as a belt-and-suspenders).
            ids = {"trans": "vTrans00001", "silent": "vSilent0001",
                   "pending": "vPend000001"}
            paths = {}
            for key, vid in ids.items():
                p = chan_dir / f"{key} [{vid}].mp4"
                p.write_bytes(b"v")
                paths[key] = str(p)

            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path), \
                    mock.patch.object(ytarchiver_config, "TRANSCRIPTION_DB",
                                      db_path):
                IndexIngestTests._reset_index_module()
                for key, vid in ids.items():
                    self.assertTrue(index.register_video(
                        paths[key], "Chan", key, video_id=vid))

                # One transcribed, one no_speech; 'pending' stays pending.
                self.assertTrue(index.mark_video_transcribed(paths["trans"]))
                self.assertTrue(index.mark_video_no_speech(paths["silent"]))

                # Per-video lookup distinguishes the states.
                self.assertEqual(
                    index.video_tx_status(video_id=ids["silent"]), "no_speech")
                self.assertEqual(
                    index.video_tx_status(video_id=ids["trans"]), "transcribed")
                self.assertIn(
                    index.video_tx_status(video_id=ids["pending"]),
                    ("pending", ""))

                # channel_transcription_stats: no_speech is its own bucket —
                # NOT counted as transcribed, NOT as pending.
                st = index.channel_transcription_stats("Chan")
                self.assertEqual(st["total"], 3)
                self.assertEqual(st["transcribed"], 1)
                self.assertEqual(st["no_speech"], 1)
                self.assertEqual(st["pending"], 1)

                # Q1: archive-wide coverage = transcribed / total. The silent
                # video is in the denominator but NOT the numerator, so a
                # fully-checked archive with a silent video reads below 100%.
                stats = archive_scan.index_db_stats()
                self.assertEqual(stats["total_videos"], 3)
                self.assertEqual(stats["transcribed_videos"], 1)
                IndexIngestTests._reset_index_module()


class DeleteChannelFromIndexTests(unittest.TestCase):
    """Deleting a channel must purge its videos + segments from the index
    (FTS-safe) so Browse/Search/Videos don't show ghost cards that 404."""

    def tearDown(self) -> None:
        IndexIngestTests._reset_index_module()

    def test_delete_channel_purges_only_that_channel(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
            db_path = Path(td) / "transcription_index.db"

            def _mk(chan: str, vid: str) -> tuple[str, str]:
                d = Path(td) / chan / "2026"
                d.mkdir(parents=True, exist_ok=True)
                mp4 = d / f"V [{vid}].mp4"
                mp4.write_bytes(b"v")
                jl = d / f"V [{vid}].jsonl"
                jl.write_text(json.dumps({
                    "video_id": vid, "title": "V " + vid,
                    "start": 0, "end": 1, "text": "hello " + chan}) + "\n",
                    encoding="utf-8")
                return str(mp4), str(jl)

            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                IndexIngestTests._reset_index_module()
                mp4a, jla = _mk("Doomed", "vDoomed0001")
                mp4b, jlb = _mk("Keepme", "vKeep000001")
                self.assertTrue(index.register_video(
                    mp4a, "Doomed", "V", video_id="vDoomed0001"))
                self.assertTrue(index.register_video(
                    mp4b, "Keepme", "V", video_id="vKeep000001"))
                index.ingest_jsonl(mp4a, jla, "V", "Doomed")
                index.ingest_jsonl(mp4b, jlb, "V", "Keepme")

                conn = index._open()
                assert conn is not None
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM videos").fetchone()[0], 2)

                res = index.delete_channel_from_index("Doomed")
                self.assertGreaterEqual(res["videos"], 1)

                # Doomed gone; Keepme untouched.
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM videos WHERE channel='Doomed'"
                ).fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM videos WHERE channel='Keepme'"
                ).fetchone()[0], 1)
                # Segments + FTS shadow purged for Doomed, intact for Keepme.
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments "
                    "WHERE video_id='vDoomed0001'").fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments_fts "
                    "WHERE segments_fts MATCH 'Doomed'").fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments_fts "
                    "WHERE segments_fts MATCH 'Keepme'").fetchone()[0], 1)
                IndexIngestTests._reset_index_module()


class TranscriptFileTests(unittest.TestCase):
    def test_write_jsonl_entry_returns_false_on_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            parent_file = Path(td) / "not_a_dir"
            parent_file.write_text("occupied", encoding="utf-8")
            path = str(parent_file / ".Channel Transcript.jsonl")

            ok = transcribe_files._write_jsonl_entry(
                path, "vid1", "Title",
                [{"start": 0, "end": 1, "text": "hello", "words": []}],
            )

            self.assertFalse(ok)

    def test_write_jsonl_entry_returns_true_on_success(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, ".Channel Transcript.jsonl")

            with mock.patch.object(transcribe_files, "_hide_file_win"):
                ok = transcribe_files._write_jsonl_entry(
                    path, "vid1", "Title",
                    [{"start": 0, "end": 1, "text": "hello", "words": []}],
                )

            rows = [
                json.loads(line)
                for line in Path(path).read_text(encoding="utf-8").splitlines()
            ]
            self.assertTrue(ok)
            self.assertEqual(rows[0]["video_id"], "vid1")

    def test_replace_jsonl_entry_preserves_same_title_different_video_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, ".Channel Transcript.jsonl")
            original_rows = [
                {"video_id": "keep-id", "title": "Q&A",
                 "start": 0, "end": 1, "text": "keep me"},
                {"video_id": "replace-id", "title": "Q&A",
                 "start": 1, "end": 2, "text": "replace me"},
            ]
            Path(path).write_text(
                "".join(json.dumps(row) + "\n" for row in original_rows),
                encoding="utf-8")

            with mock.patch.object(transcribe_files, "_hide_file_win"), \
                    mock.patch.object(transcribe_files, "_unhide_file_win"):
                removed = transcribe_files._replace_jsonl_entry(
                    path,
                    title="Q&A",
                    video_id="replace-id",
                    new_segments=[{"start": 2, "end": 3,
                                   "text": "new text", "words": []}],
                )

            rows = [
                json.loads(line)
                for line in Path(path).read_text(encoding="utf-8").splitlines()
            ]

            self.assertEqual(removed, {"Q&A"})
            self.assertEqual([row["video_id"] for row in rows],
                             ["keep-id", "replace-id"])
            self.assertEqual(rows[0]["text"], "keep me")
            self.assertEqual(rows[1]["text"], "new text")

    def test_replace_txt_entry_preserves_surrounding_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "Channel Transcript.txt")
            Path(path).write_text(
                "===(First), (01.01.2026), (0:00:01), (OLD)===\n"
                "first body\n\n\n"
                "===(Target), (01.02.2026), (0:00:02), (OLD)===\n"
                "old target\n\n\n"
                "===(Last), (01.03.2026), (0:00:03), (OLD)===\n"
                "last body\n\n\n",
                encoding="utf-8")

            ok = transcribe_files._replace_txt_entry(
                path, "Target", "new target", "WHISPER:test")

            content = Path(path).read_text(encoding="utf-8")
            self.assertTrue(ok)
            self.assertIn("first body", content)
            self.assertIn("last body", content)
            self.assertIn("new target", content)
            self.assertNotIn("old target", content)
            self.assertIn("===(Target), (01.02.2026), (0:00:02), "
                          "(WHISPER:test)===", content)

    def test_replace_txt_entry_uses_old_body_to_disambiguate_same_title(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "Channel Transcript.txt")
            Path(path).write_text(
                "===(Q&A), (01.01.2026), (0:00:01), (OLD)===\n"
                "first video body\n\n\n"
                "===(Q&A), (01.02.2026), (0:00:02), (OLD)===\n"
                "second video body\n\n\n",
                encoding="utf-8")

            ok = transcribe_files._replace_txt_entry(
                path, "Q&A", "new second body", "WHISPER:test",
                old_text_candidates={"second video body"})

            content = Path(path).read_text(encoding="utf-8")
            self.assertTrue(ok)
            self.assertIn("first video body", content)
            self.assertIn("new second body", content)
            self.assertNotIn("second video body", content)

    def test_replace_txt_entry_refuses_ambiguous_same_title_without_identity(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "Channel Transcript.txt")
            Path(path).write_text(
                "===(Q&A), (01.01.2026), (0:00:01), (OLD)===\n"
                "first video body\n\n\n"
                "===(Q&A), (01.02.2026), (0:00:02), (OLD)===\n"
                "second video body\n\n\n",
                encoding="utf-8")

            with self.assertRaises(ValueError):
                transcribe_files._replace_txt_entry(
                    path, "Q&A", "new body", "WHISPER:test")

    def test_transcript_header_parser_preserves_field_like_title_text(self) -> None:
        title = "Debate), (Part Two"
        header = f"===({title}), (01.02.2026), (0:00:02), (YT CAPTIONS)==="

        parsed = transcribe_files.parse_transcript_header(header)

        self.assertEqual(parsed, (title, "01.02.2026", "0:00:02",
                                  "YT CAPTIONS"))
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "Channel Transcript.txt"
            path.write_text(header + "\nbody\n\n\n", encoding="utf-8")

            sources = repair_captions._parse_txt_sources(path)

        self.assertEqual(
            sources[repair_captions._norm_title(title)],
            "YT CAPTIONS")


class RepairCaptionsTests(unittest.TestCase):
    def test_file_contains_any_detects_match_across_chunk_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "data.jsonl"
            path.write_bytes(b"abcde")

            self.assertTrue(repair_captions._file_contains_any(
                path, (b"cd",), chunk_size=1))
            self.assertFalse(repair_captions._file_contains_any(
                path, (b"zz",), chunk_size=2))

    def test_repair_archive_reuses_one_temp_dir_for_video_loop(self) -> None:
        tmp_dirs = []

        def fake_repair(*args, **kwargs):
            tmp_dir = kwargs["tmp_dir"]
            tmp_dirs.append(str(tmp_dir))
            self.assertTrue(tmp_dir.exists())
            return True, "ok", 1, 2

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ch = root / "Channel"
            ch.mkdir()
            jsonl = ch / ".Channel Transcript.jsonl"
            jsonl.write_text("", encoding="utf-8")
            stream = mock.Mock()

            with mock.patch.object(repair_captions, "find_yt_dlp",
                                   return_value="yt-dlp"), \
                    mock.patch.object(repair_captions, "_find_cookie_source",
                                      return_value=True), \
                    mock.patch.object(repair_captions, "_collect_yt_videos",
                                      return_value=[
                                          ("vid00000001", "One", "YT CAPTIONS"),
                                          ("vid00000002", "Two", "YT CAPTIONS"),
                                      ]), \
                    mock.patch.object(repair_captions, "_repair_one_video",
                                      side_effect=fake_repair), \
                    mock.patch.object(repair_captions.time, "sleep"):
                result = repair_captions.repair_archive(
                    output_dir=str(root), log_stream=stream, dry_run=True)

        self.assertTrue(result["ok"])
        self.assertEqual(result["succeeded"], 2)
        self.assertEqual(len(tmp_dirs), 2)
        self.assertEqual(len(set(tmp_dirs)), 1)


class DriftScanTests(unittest.TestCase):
    def test_scan_txt_titles_streams_headers_without_reading_whole_file(self) -> None:
        class LineOnlyFile:
            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return False

            def __iter__(self):
                return iter([
                    "===(Video Title [abc123_def4]), (01.02.2024), "
                    "(0:03), (WHISPER)===\n",
                    "large transcript body that should not matter\n",
                ])

            def read(self, *_args, **_kwargs):
                raise AssertionError("drift scan should not read whole files")

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "Channel Transcript.txt"
            path.write_text("", encoding="utf-8")

            with mock.patch("builtins.open", return_value=LineOnlyFile()):
                titles = drift_scan._scan_txt_titles(td)

        recs = titles[drift_scan._norm_title("Video Title [abc123_def4]")]
        self.assertEqual(recs[0]["raw"], "Video Title [abc123_def4]")
        self.assertEqual(recs[0]["video_id"], "abc123_def4")

    def test_channel_folder_uses_canonical_folder_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ch = {"name": "Display Name", "folder": "Stale Folder",
                  "folder_override": "Actual: Folder"}

            expected = str(root / "Actual_ Folder")

            self.assertEqual(drift_scan._channel_folder(ch, str(root)),
                             expected)
            self.assertEqual(archive_scan._channel_folder_name(ch),
                             "Actual_ Folder")

    def test_rebuild_txt_from_jsonl_matches_normalized_title(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / ".Channel Transcript.jsonl"
            rows = [
                {"video_id": "abc123_def4", "title": "My Video.",
                 "start": 0, "end": 1, "text": "hello"},
                {"video_id": "abc123_def4", "title": "My Video.",
                 "start": 1, "end": 3, "text": "world"},
            ]
            path.write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8",
            )

            rebuilt = drift_scan._rebuild_txt_from_jsonl_entries(
                str(path), ["my video [abc123_def4]"])

            self.assertIn("my video [abc123_def4]", rebuilt)
            self.assertEqual(
                rebuilt["my video [abc123_def4]"]["text"],
                "hello world")
            self.assertEqual(
                rebuilt["my video [abc123_def4]"]["video_id"],
                "abc123_def4")

    def test_apply_channel_reconstruction_uses_recovered_upload_date(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ch_dir = root / "Channel"
            ch_dir.mkdir()
            jsonl = ch_dir / ".Channel Transcript.jsonl"
            jsonl.write_text(
                json.dumps({
                    "video_id": "abc123_def4",
                    "title": "Video",
                    "start": 0,
                    "end": 2,
                    "text": "hello world",
                }) + "\n",
                encoding="utf-8",
            )
            scan_result = {
                "ok": True,
                "jsonl_without_txt": [{
                    "title": "Video",
                    "video_id": "abc123_def4",
                    "jsonl_path": str(jsonl),
                }],
                "txt_without_jsonl": [],
                "fts_phantoms": 0,
            }

            with mock.patch.object(drift_scan, "_recovered_upload_date",
                                   return_value="02.03.2024"):
                result = drift_scan.apply_channel(
                    {"name": "Channel"}, str(root), scan_result=scan_result)

            txt = ch_dir / "Channel Transcript.txt"
            content = txt.read_text(encoding="utf-8")
            self.assertTrue(result["ok"])
            self.assertEqual(result["actions"]["txt_reconstructed"], 1)
            self.assertIn("===(Video), (02.03.2024), (0:00), "
                          "(RECOVERED-FROM-JSONL)===", content)


if __name__ == "__main__":
    unittest.main()
