from __future__ import annotations

import os
import contextlib
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-sync-tests-")
os.environ.setdefault("APPDATA", _TEST_APPDATA.name)

from backend import archive_scan, channel_art, channel_cache, index, livestreams
from backend import subs, utils
from backend.sync import core as sync_core


class _FakePipe:
    def __init__(self, lines: list[str]) -> None:
        self._lines = [line.encode("utf-8") + b"\n" for line in lines]

    def readline(self) -> bytes:
        if self._lines:
            return self._lines.pop(0)
        return b""

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


class SyncArchiveBookkeepingTests(unittest.TestCase):
    def test_existing_notice_only_matches_final_media(self) -> None:
        self.assertIsNone(sync_core._existing_final_media_path(
            "[download] C:\\Archive\\Video.en.vtt has already been downloaded"))
        self.assertIsNone(sync_core._existing_final_media_path(
            "[download] C:\\Archive\\Video.f140.m4a has already been downloaded"))
        self.assertEqual(
            sync_core._existing_final_media_path(
                "[download] C:\\Archive\\Video.mp4 has already been downloaded"),
            "C:\\Archive\\Video.mp4",
        )

    def test_archive_merge_deduplicates_existing_and_repeated_ids(self) -> None:
        first = "abc123def45"
        second = "zyx987wvu65"
        with tempfile.TemporaryDirectory() as td:
            archive = Path(td) / "download-archive.txt"
            archive.write_text(f"youtube {first}\n", encoding="utf-8")

            with mock.patch.object(sync_core, "ARCHIVE_FILE", str(archive)):
                added = sync_core._append_download_archive_ids(
                    [first, second, second, "not-a-video-id"])

            self.assertEqual(added, 1)
            self.assertEqual(
                archive.read_text(encoding="utf-8").splitlines(),
                [f"youtube {first}", f"youtube {second}"],
            )

    def _run_sync(
        self,
        root: Path,
        archive: Path,
        lines: list[str],
        *,
        create_media_on_launch: Path | None = None,
    ):
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

        def fake_launch(*args, **kwargs):
            if create_media_on_launch is not None:
                create_media_on_launch.parent.mkdir(parents=True, exist_ok=True)
                create_media_on_launch.write_bytes(b"video")
            return _FakeProc(lines)

        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(
                sync_core, "ARCHIVE_FILE", str(archive)))
            stack.enter_context(mock.patch.object(
                sync_core, "find_yt_dlp", return_value="yt-dlp"))
            stack.enter_context(mock.patch.object(
                sync_core, "load_config",
                return_value={"output_dir": str(root)}))
            stack.enter_context(mock.patch.object(
                sync_core, "config_is_writable", return_value=False))
            stack.enter_context(mock.patch.object(
                sync_core, "_find_cookie_source", return_value=[]))
            stack.enter_context(mock.patch.object(
                sync_core, "popen_ytdlp_process", side_effect=fake_launch))
            stack.enter_context(mock.patch.object(
                sync_core, "start_download_watchdog",
                side_effect=lambda *a, **k: _FakeWatchdog()))
            for target, name in (
                (sync_core, "finish_ytdlp_process"),
                (sync_core, "write_sync_progress"),
                (sync_core, "set_sync_active"),
                (sync_core, "clear_sync_active"),
                (sync_core, "_hide_sidecar_win"),
                (sync_core, "_record_recent_download"),
                (sync_core, "_bg_channel_maintenance"),
                (livestreams, "drop"),
                (channel_cache, "append_ids"),
                (archive_scan, "update_disk_cache_for_channel"),
                (channel_art, "fetch_channel_art"),
            ):
                stack.enter_context(mock.patch.object(target, name))
            stack.enter_context(mock.patch.object(
                utils, "check_directory_writable", return_value=True))
            stack.enter_context(mock.patch.object(
                utils, "check_disk_space", return_value=True))
            stack.enter_context(mock.patch.object(
                subs, "streams_url", return_value=None))
            stack.enter_context(mock.patch.object(
                index, "register_video", return_value=True))
            result = sync_core.sync_channel(channel, stream)
        return result, stream

    def test_empty_folder_downloads_are_recorded_in_global_archive(self) -> None:
        video_id = "abc123def45"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            media = root / "Test Channel" / "Fresh Video.mp4"
            lines = [
                f"[youtube] {video_id}: Downloading webpage",
                f"[download] Destination: {media}",
                f'[Merger] Merging formats into "{media}"',
                (f"DLTRACK:::Fresh Video:::Test Channel:::20260819:::5:::"
                 f"300:::{video_id}"),
            ]

            result, _stream = self._run_sync(
                root, archive, lines, create_media_on_launch=media)

            self.assertEqual(result["downloaded"], 1)
            self.assertEqual(
                archive.read_text(encoding="utf-8").splitlines(),
                [f"youtube {video_id}"],
            )

    def test_existing_file_after_video_is_not_counted_as_download(self) -> None:
        video_id = "abc123def45"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            archive.write_text("", encoding="utf-8")
            media = root / "Test Channel" / "Existing Video.mp4"
            media.parent.mkdir(parents=True)
            media.write_bytes(b"video")
            lines = [
                f"[youtube] {video_id}: Downloading webpage",
                f"[download] Destination: {media}",
                f"[download] {media} has already been downloaded",
                (f"DLTRACK:::Existing Video:::Test Channel:::20260819:::5:::"
                 f"300:::{video_id}"),
            ]

            result, stream = self._run_sync(root, archive, lines)

            self.assertEqual(result["downloaded"], 0)
            self.assertEqual(result["total"], 1)
            rendered = "\n".join(
                str(call.args[0]) for call in stream.emit.call_args_list)
            self.assertNotIn("— ✓", rendered)


if __name__ == "__main__":
    unittest.main()
