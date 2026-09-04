from __future__ import annotations

import contextlib
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-sync-tests-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import archive_scan, channel_art, channel_cache, index, livestreams, subs, utils
from backend.sync import core as sync_core

STABLE_CHANNEL_ID = "UCaaaaaaaaaaaaaaaaaaaaaa"
STABLE_CHANNEL_URL = f"https://www.youtube.com/channel/{STABLE_CHANNEL_ID}"


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
        queues=None,
        launched_commands: list[list[str]] | None = None,
        channel_overrides: dict | None = None,
        quickcheck_fresh_ids=None,
    ):
        channel = {
            "name": "Test Channel",
            "folder": "Test Channel",
            "url": "https://www.youtube.com/@test-channel",
            # An established subscription carries its verified permanent ID,
            # so syncs enumerate through the immutable /channel/ address and
            # the identity preflight is a no-op.
            "channel_id": STABLE_CHANNEL_ID,
            "mode": "full",
            "resolution": "1080",
            "auto_transcribe": True,
            "auto_metadata": False,
            "initialized": False,
            "init_complete": False,
            "sync_complete": False,
        }
        channel.update(channel_overrides or {})
        stream = mock.Mock()

        def fake_launch(*args, **kwargs):
            if launched_commands is not None and args:
                launched_commands.append(list(args[0]))
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
            result = sync_core.sync_channel(
                channel, stream, queues=queues,
                quickcheck_fresh_ids=quickcheck_fresh_ids)
        return result, stream

    def test_sync_channel_leaves_completion_for_durable_queue_owner(self) -> None:
        """The channel worker must not erase the orchestrator's task ID."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            queue_state = mock.Mock()

            self._run_sync(root, archive, [], queues=queue_state)

        queue_state.set_current_sync.assert_not_called()

    def test_sync_command_does_not_left_trim_the_absolute_output_path(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            commands: list[list[str]] = []

            self._run_sync(
                root, archive, [], launched_commands=commands)

        self.assertTrue(commands)
        self.assertNotIn("--trim-filenames", commands[0])

    def test_channel_walk_runs_before_quickcheck_gap_repair(
        self,
    ) -> None:
        newest = "archived001"
        missing = "missing0001"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            archive.write_text(f"youtube {newest}\n", encoding="utf-8")
            channel_dir = root / "Test Channel"
            channel_dir.mkdir()
            (channel_dir / "Existing.mp4").write_bytes(b"existing")
            commands: list[list[str]] = []

            _result, stream = self._run_sync(
                root,
                archive,
                [],
                launched_commands=commands,
                channel_overrides={
                    "initialized": True,
                    "init_complete": True,
                    "sync_complete": True,
                },
                # Invalid and duplicate values must never become URLs.
                quickcheck_fresh_ids=[missing, missing, "not-a-video-id"],
            )

        self.assertEqual(len(commands), 2)
        self.assertEqual(commands[0][-1], STABLE_CHANNEL_URL)
        self.assertEqual(
            commands[1][-1], f"https://www.youtube.com/watch?v={missing}")
        self.assertIn("--break-on-existing", commands[0])
        self.assertIn("--break-on-existing", commands[1])
        rendered = "\n".join(
            str(call.args[0]) for call in stream.emit.call_args_list)
        self.assertNotIn("[Streams]", rendered)

    def test_channel_commit_skips_redundant_quickcheck_target(self) -> None:
        newest = "archived001"
        fresh = "missing0001"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            archive.write_text(f"youtube {newest}\n", encoding="utf-8")
            channel_dir = root / "Test Channel"
            channel_dir.mkdir()
            (channel_dir / "Existing.mp4").write_bytes(b"existing")
            staged_media = channel_dir / f"Fresh [{fresh}].mp4"
            lines = [
                f"[youtube] {fresh}: Downloading webpage",
                f"[download] Destination: {staged_media}",
                f'[Merger] Merging formats into "{staged_media}"',
                (f"DLTRACK:::Fresh:::Test Channel:::20260819:::5:::"
                 f"30:::{fresh}"),
            ]
            commands: list[list[str]] = []

            result, _stream = self._run_sync(
                root,
                archive,
                lines,
                create_media_on_launch=staged_media,
                launched_commands=commands,
                channel_overrides={
                    "initialized": True,
                    "init_complete": True,
                    "sync_complete": True,
                },
                quickcheck_fresh_ids=[fresh],
            )

        self.assertEqual(result["downloaded"], 1)
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0][-1], STABLE_CHANNEL_URL)

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
            self.assertGreaterEqual(result["errors"], 1)
            self.assertEqual(
                archive.read_text(encoding="utf-8").splitlines(), [])
            rendered = "\n".join(
                str(call.args[0]) for call in stream.emit.call_args_list)
            self.assertNotIn("— ✓", rendered)

    def test_duplicate_titles_keep_both_media_and_commit_both_ids(self) -> None:
        first_id = "archived001"
        second_id = "missing0001"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            channel_dir = root / "Test Channel"
            channel_dir.mkdir(parents=True)
            first_base = channel_dir / f"Same Title [{first_id}]"
            second_base = channel_dir / f"Same Title [{second_id}]"
            first_media = first_base.with_suffix(".mp4")
            second_media = second_base.with_suffix(".mp4")
            first_media.write_bytes(b"first-video")
            second_media.write_bytes(b"second-video")
            first_base.with_suffix(".info.json").write_text(
                json.dumps({"id": first_id}), encoding="utf-8")
            second_base.with_suffix(".info.json").write_text(
                json.dumps({"id": second_id}), encoding="utf-8")
            lines = [
                f"[youtube] {first_id}: Downloading webpage",
                f"[download] Destination: {first_media}",
                f'[Merger] Merging formats into "{first_media}"',
                (f"DLTRACK:::Same Title:::Test Channel:::20260819:::5:::"
                 f"74:::{first_id}"),
                f"[youtube] {second_id}: Downloading webpage",
                f"[download] Destination: {second_media}",
                f'[Merger] Merging formats into "{second_media}"',
                (f"DLTRACK:::Same Title:::Test Channel:::20260819:::5:::"
                 f"56:::{second_id}"),
            ]

            result, _stream = self._run_sync(root, archive, lines)

            self.assertEqual(result["downloaded"], 2)
            self.assertEqual(
                archive.read_text(encoding="utf-8").splitlines(),
                [f"youtube {first_id}", f"youtube {second_id}"],
            )
            self.assertEqual(
                (channel_dir / "Same Title.mp4").read_bytes(), b"first-video")
            self.assertEqual(second_media.read_bytes(), b"second-video")
            self.assertEqual(
                json.loads((channel_dir / "Same Title.info.json").read_text(
                    encoding="utf-8"))["id"],
                first_id,
            )
            self.assertEqual(
                json.loads(second_base.with_suffix(".info.json").read_text(
                    encoding="utf-8"))["id"],
                second_id,
            )

    def test_exact_id_sidecar_beats_unrelated_recent_file_fallback(self) -> None:
        video_id = "missing0001"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            channel_dir = root / "Test Channel"
            channel_dir.mkdir(parents=True)
            actual_base = channel_dir / f"Actual [{video_id}]"
            actual_media = Path(str(actual_base) + ".mp4")
            actual_media.write_bytes(b"actual-video")
            Path(str(actual_base) + ".info.json").write_text(
                json.dumps({"id": video_id}), encoding="utf-8")
            unrelated = channel_dir / "Unrelated.mp4"
            unrelated.write_bytes(b"unrelated-video")
            lines = [
                f"[youtube] {video_id}: Downloading webpage",
                (f"DLTRACK:::Actual:::Test Channel:::20260819:::5:::"
                 f"30:::{video_id}"),
            ]

            with mock.patch.object(
                    sync_core, "_resolve_path_for_vid",
                    return_value=str(actual_media)) as exact_resolver, \
                    mock.patch.object(
                        sync_core, "_scan_recent_video",
                        return_value=str(unrelated)) as recent_scan:
                result, _stream = self._run_sync(root, archive, lines)

            self.assertEqual(result["downloaded"], 1)
            self.assertEqual(
                archive.read_text(encoding="utf-8").splitlines(),
                [f"youtube {video_id}"],
            )
            self.assertEqual(
                (channel_dir / "Actual.mp4").read_bytes(), b"actual-video")
            self.assertEqual(unrelated.read_bytes(), b"unrelated-video")
            exact_resolver.assert_called_once()
            recent_scan.assert_not_called()

    def test_unproven_recent_file_never_enters_download_history(self) -> None:
        video_id = "missing0001"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "download-archive.txt"
            channel_dir = root / "Test Channel"
            channel_dir.mkdir(parents=True)
            unrelated = channel_dir / "Unrelated.mp4"
            unrelated.write_bytes(b"unrelated-video")
            lines = [
                f"[youtube] {video_id}: Downloading webpage",
                (f"DLTRACK:::Missing:::Test Channel:::20260819:::5:::"
                 f"30:::{video_id}"),
            ]

            with mock.patch.object(
                    sync_core, "_resolve_path_for_vid", return_value=None), \
                    mock.patch.object(
                        sync_core, "_scan_recent_video",
                        return_value=str(unrelated)), \
                    mock.patch.object(
                        sync_core, "existing_media_matches_video_id",
                        return_value=False) as identity_check:
                result, _stream = self._run_sync(root, archive, lines)

            self.assertEqual(result["downloaded"], 0)
            self.assertFalse(archive.exists())
            self.assertEqual(unrelated.read_bytes(), b"unrelated-video")
            identity_check.assert_called_once_with(str(unrelated), video_id)


if __name__ == "__main__":
    unittest.main()
