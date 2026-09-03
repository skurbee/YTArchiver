from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(
    prefix="ytarchiver-title-collision-tests-"
)
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import index, livestreams, utils
from backend.api_mixins import archive_mixin
from backend.process_runner import StreamingRunResult
from backend.services.channel_leases import channel_leases
from backend.sync import download_commit as download_commit_module
from backend.sync.download_commit import (
    commit_download,
    existing_media_matches_video_id,
    finalize_collision_safe_bundle,
    is_durable_final_media,
)
from backend.sync.options import build_output_template


class CollisionSafeDownloadTests(unittest.TestCase):
    @staticmethod
    def _write_bundle(
        folder: Path,
        title: str,
        video_id: str,
        *,
        sidecar_video_id: str | None = None,
    ) -> Path:
        base = folder / f"{title} [{video_id}]"
        media = base.with_suffix(".mp4")
        media.write_bytes(("media-" + video_id).encode("ascii"))
        base.with_suffix(".info.json").write_text(
            json.dumps({"id": sidecar_video_id or video_id}),
            encoding="utf-8",
        )
        Path(str(base) + ".en.vtt").write_text(
            "WEBVTT\n", encoding="utf-8"
        )
        return media

    def test_unique_bundle_returns_to_title_only_name(self) -> None:
        video_id = "archived001"
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            staged = self._write_bundle(folder, "Same Title", video_id)

            result = finalize_collision_safe_bundle(str(staged), video_id)

            self.assertTrue(result.ok, result.error)
            self.assertTrue(result.normalized)
            self.assertFalse(result.collision)
            self.assertEqual(Path(result.final_path), folder / "Same Title.mp4")
            self.assertFalse(staged.exists())
            self.assertTrue((folder / "Same Title.mp4").is_file())
            self.assertTrue((folder / "Same Title.info.json").is_file())
            self.assertTrue((folder / "Same Title.en.vtt").is_file())

    def test_same_title_keeps_second_bundle_bound_to_its_id(self) -> None:
        first_id = "archived001"
        second_id = "missing0001"
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            first_staged = self._write_bundle(folder, "Same Title", first_id)
            first = finalize_collision_safe_bundle(
                str(first_staged), first_id
            )
            second_staged = self._write_bundle(folder, "Same Title", second_id)

            second = finalize_collision_safe_bundle(
                str(second_staged), second_id
            )

            self.assertTrue(first.ok, first.error)
            self.assertTrue(second.ok, second.error)
            self.assertTrue(second.collision)
            self.assertFalse(second.normalized)
            self.assertEqual(Path(second.final_path), second_staged)
            self.assertEqual(
                json.loads((folder / "Same Title.info.json").read_text(
                    encoding="utf-8"
                ))["id"],
                first_id,
            )
            self.assertEqual(
                json.loads(Path(
                    str(folder / f"Same Title [{second_id}]") + ".info.json"
                ).read_text(encoding="utf-8"))["id"],
                second_id,
            )
            self.assertEqual(
                (folder / "Same Title.mp4").read_bytes(),
                ("media-" + first_id).encode("ascii"),
            )
            self.assertEqual(
                second_staged.read_bytes(),
                ("media-" + second_id).encode("ascii"),
            )

    def test_mismatched_sidecar_cannot_reach_registration(self) -> None:
        expected = "missing0001"
        other = "archived001"
        with tempfile.TemporaryDirectory() as td:
            staged = self._write_bundle(
                Path(td), "Same Title", expected, sidecar_video_id=other
            )
            registrar = mock.Mock(return_value=True)

            promotion = finalize_collision_safe_bundle(
                str(staged), expected
            )
            committed = commit_download(
                str(staged),
                "Test Channel",
                "Same Title",
                video_id=expected,
                auto_transcribe=False,
                registrar=registrar,
            )

            self.assertFalse(promotion.ok)
            self.assertIn("different video ID", promotion.error)
            self.assertFalse(committed.ok)
            registrar.assert_not_called()

    def test_non_media_sidecar_is_never_a_durable_download(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            sidecar = Path(td) / "Video.info.json"
            sidecar.write_text('{"id":"missing0001"}', encoding="utf-8")

            self.assertFalse(is_durable_final_media(str(sidecar)))

    def test_existing_title_only_file_uses_embedded_id_not_sidecar(self) -> None:
        first_id = "archived001"
        second_id = "missing0001"
        with tempfile.TemporaryDirectory() as td:
            media = Path(td) / "Same Title.mp4"
            media.write_bytes(b"first-video")
            media.with_suffix(".info.json").write_text(
                json.dumps({"id": second_id}), encoding="utf-8"
            )
            embedded_reader = mock.Mock(return_value=first_id)

            self.assertFalse(existing_media_matches_video_id(
                str(media), second_id,
                embedded_id_reader=embedded_reader,
            ))
            self.assertTrue(existing_media_matches_video_id(
                str(media), first_id,
                embedded_id_reader=embedded_reader,
            ))

    def test_id_suffix_is_safe_fallback_when_embedded_tag_is_unavailable(
        self,
    ) -> None:
        video_id = "missing0001"
        with tempfile.TemporaryDirectory() as td:
            media = Path(td) / f"Same Title [{video_id}].mp4"
            media.write_bytes(b"video")

            self.assertTrue(existing_media_matches_video_id(
                str(media), video_id,
                embedded_id_reader=lambda _path: None,
            ))

    def test_long_title_keeps_complete_id_inside_ytdlp_trim_budget(self) -> None:
        video_id = "missing0001"
        with tempfile.TemporaryDirectory() as td:
            template = build_output_template(Path(td), False, False)
            basename_template = os.path.basename(template)

            self.assertEqual(
                basename_template,
                "%(title).170B [%(id)s].%(ext)s",
            )

            for extension in ("mp4", "info.json", "live_chat.json"):
                basename = "A" * 170 + f" [{video_id}].{extension}"
                self.assertIn(f"[{video_id}]", basename)
                self.assertLessEqual(len(basename.encode("utf-8")), 200)

    def test_literal_id_shaped_title_suffix_is_not_a_false_conflict(self) -> None:
        video_id = "missing0001"
        title_suffix = "abcdefghijk"
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            staged = self._write_bundle(
                folder, f"Report [{title_suffix}]", video_id)
            registrar = mock.Mock(return_value=True)

            promotion = finalize_collision_safe_bundle(
                str(staged), video_id)
            committed = commit_download(
                promotion.final_path,
                "Test Channel",
                f"Report [{title_suffix}]",
                video_id=video_id,
                auto_transcribe=False,
                registrar=registrar,
                filename_id_is_provenance=not promotion.normalized,
            )

            self.assertTrue(promotion.ok, promotion.error)
            self.assertTrue(promotion.normalized)
            self.assertTrue(committed.ok, committed.error)
            registrar.assert_called_once()

    def test_target_created_during_promotion_becomes_safe_collision(self) -> None:
        video_id = "missing0001"
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            staged = self._write_bundle(folder, "Same Title", video_id)
            real_move = download_commit_module._rename_without_overwrite

            def racing_move(source: str, destination: str) -> None:
                if destination.endswith(".mp4"):
                    Path(destination).write_bytes(b"other-video")
                    raise FileExistsError(destination)
                real_move(source, destination)

            with mock.patch.object(
                download_commit_module,
                "_rename_without_overwrite",
                side_effect=racing_move,
            ):
                result = finalize_collision_safe_bundle(
                    str(staged), video_id)

            self.assertTrue(result.ok, result.error)
            self.assertTrue(result.collision)
            self.assertFalse(result.normalized)
            self.assertEqual(Path(result.final_path), staged)
            self.assertEqual(staged.read_bytes(), b"media-" + video_id.encode())
            self.assertTrue(Path(
                str(folder / f"Same Title [{video_id}]") + ".info.json"
            ).is_file())
            self.assertFalse((folder / "Same Title.info.json").exists())
            self.assertEqual(
                (folder / "Same Title.mp4").read_bytes(), b"other-video")


class ManualDownloadCollisionTests(unittest.TestCase):
    def test_manual_template_stages_id_and_escapes_literal_percent(self) -> None:
        self.assertEqual(
            archive_mixin.build_manual_output_template_name(
                use_yt_title=True,
                custom_name="",
                add_date=False,
                download_date="09.01.26",
            ),
            "%(title).170B [%(id)s].%(ext)s",
        )
        self.assertEqual(
            archive_mixin.build_manual_output_template_name(
                use_yt_title=True,
                custom_name="",
                add_date=True,
                download_date="09.01.26",
            ),
            "%(title).159B (09.01.26) [%(id)s].%(ext)s",
        )
        self.assertEqual(
            archive_mixin.build_manual_output_template_name(
                use_yt_title=False,
                custom_name="100% <Complete>",
                add_date=False,
                download_date="09.01.26",
            ),
            "100%% _Complete_ [%(id)s].%(ext)s",
        )
        self.assertEqual(
            archive_mixin.build_manual_output_template_name(
                use_yt_title=True,
                custom_name="",
                add_date=False,
                download_date="09.01.26",
                include_video_id=False,
            ),
            "%(title).170B.%(ext)s",
        )

    def test_manual_path_resolver_never_selects_newer_info_sidecar(
        self,
    ) -> None:
        video_id = "missing0001"
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            media = folder / f"Same Title [{video_id}].mp4"
            sidecar = folder / f"Same Title [{video_id}].info.json"
            media.write_bytes(b"video")
            sidecar.write_text(json.dumps({"id": video_id}), encoding="utf-8")
            os.utime(media, (1000, 1000))
            os.utime(sidecar, (2000, 2000))

            resolved = archive_mixin.resolve_final_path(
                str(folder), video_id, "Same Title", [])

            self.assertEqual(Path(resolved), media)

    def test_manual_same_title_keeps_both_media_bound_to_their_ids(
        self,
    ) -> None:
        first_id = "archived001"
        second_id = "missing0001"

        class ImmediateThread:
            def __init__(self, target, **_kwargs):
                self.target = target

            def start(self):
                self.target()

        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        td = temporary.name
        with self.subTest(temp_directory=td):
            folder = Path(td)
            # Keep the process mocks alive; ``id`` alone is not ownership.
            processes = [mock.Mock(name="first-proc"),
                         mock.Mock(name="second-proc")]
            proc_for_id = {
                id(processes[0]): first_id,
                id(processes[1]): second_id,
            }
            launched_commands: list[list[str]] = []

            def launch(command, **_kwargs):
                launched_commands.append(list(command))
                return processes[len(launched_commands) - 1]

            def supervise(proc, *, on_stdout_line, **_kwargs):
                video_id = proc_for_id[id(proc)]
                staged_base = folder / f"Same Title [{video_id}]"
                staged_media = Path(str(staged_base) + ".mp4")
                staged_media.write_bytes(
                    ("media-" + video_id).encode("ascii"))
                Path(str(staged_base) + ".info.json").write_text(
                    json.dumps({"id": video_id}), encoding="utf-8")
                for line in (
                    "DLPRE:::Same Title:::Test Channel",
                    f"[download] Destination: {staged_media}",
                    f'[Merger] Merging formats into "{staged_media}"',
                    ("DLTRACK:::Same Title:::Test Channel:::20260819:::5:::"
                     f"60:::{video_id}"),
                ):
                    on_stdout_line(line)
                return StreamingRunResult(0, [])

            api = archive_mixin.ArchiveMixin()
            api._log_stream = mock.Mock()
            api._window = None
            api._push_url_history = mock.Mock()
            api._push_recent_refresh = mock.Mock()
            registrar = mock.Mock(return_value=True)

            with mock.patch.object(
                    archive_mixin.threading, "Thread", ImmediateThread), \
                    mock.patch.object(
                        archive_mixin.sync_backend, "find_yt_dlp",
                        return_value="yt-dlp"), \
                    mock.patch.object(
                        archive_mixin.sync_backend, "build_format_string",
                        return_value="best"), \
                    mock.patch.object(
                        archive_mixin.sync_backend, "_find_cookie_source",
                        return_value=[]), \
                    mock.patch.object(
                        archive_mixin.sync_backend,
                        "_record_recent_download", return_value=True), \
                    mock.patch.object(
                        archive_mixin, "load_config",
                        return_value={"video_out_dir": str(folder)}), \
                    mock.patch.object(
                        archive_mixin, "_probe_output_folder_writable"), \
                    mock.patch.object(
                        archive_mixin.youtube_traffic, "is_youtube_url",
                        return_value=True), \
                    mock.patch.object(
                        archive_mixin.youtube_traffic, "acquire",
                        return_value={"ok": True}), \
                    mock.patch.object(
                        archive_mixin, "popen_ytdlp", side_effect=launch), \
                    mock.patch.object(
                        archive_mixin, "supervise_streaming_process",
                        side_effect=supervise), \
                    mock.patch.object(
                        index, "register_video", registrar), \
                    mock.patch.object(livestreams, "drop"), \
                    mock.patch.object(utils, "hide_stray_sidecars"):
                first = api.archive_single_video(
                    f"https://youtube.com/watch?v={first_id}",
                    {"grab_metadata": True})
                second = api.archive_single_video(
                    f"https://youtube.com/watch?v={second_id}",
                    {"grab_metadata": True})

            self.assertTrue(first["ok"] and first["started"])
            self.assertTrue(second["ok"] and second["started"])
            self.assertEqual(
                (folder / "Same Title.mp4").read_bytes(),
                ("media-" + first_id).encode("ascii"),
            )
            self.assertEqual(
                (folder / f"Same Title [{second_id}].mp4").read_bytes(),
                ("media-" + second_id).encode("ascii"),
            )
            self.assertEqual(
                json.loads((folder / "Same Title.info.json").read_text(
                    encoding="utf-8"))["id"],
                first_id,
            )
            self.assertEqual(
                json.loads((folder / f"Same Title [{second_id}].info.json")
                           .read_text(encoding="utf-8"))["id"],
                second_id,
            )
            self.assertEqual(registrar.call_count, 2)
            registered_paths = [
                Path(call.args[0]).name for call in registrar.call_args_list
            ]
            self.assertEqual(registered_paths, [
                "Same Title.mp4", f"Same Title [{second_id}].mp4",
            ])
            self.assertEqual(len(launched_commands), 2)
            for command in launched_commands:
                self.assertNotIn("--trim-filenames", command)
                output = command[command.index("--output") + 1]
                self.assertIn("%(title).170B [%(id)s].%(ext)s", output)

    def test_manual_traffic_denial_releases_url_and_folder_lease(self) -> None:
        class ImmediateThread:
            def __init__(self, target, **_kwargs):
                self.target = target

            def start(self):
                self.target()

        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            api = archive_mixin.ArchiveMixin()
            api._log_stream = mock.Mock()
            api._window = None
            api._push_url_history = mock.Mock()
            api._push_recent_refresh = mock.Mock()

            with mock.patch.object(
                    archive_mixin.threading, "Thread", ImmediateThread), \
                    mock.patch.object(
                        archive_mixin.sync_backend, "find_yt_dlp",
                        return_value="yt-dlp"), \
                    mock.patch.object(
                        archive_mixin.sync_backend, "build_format_string",
                        return_value="best"), \
                    mock.patch.object(
                        archive_mixin.sync_backend, "_find_cookie_source",
                        return_value=[]), \
                    mock.patch.object(
                        archive_mixin, "load_config",
                        return_value={"video_out_dir": str(folder)}), \
                    mock.patch.object(
                        archive_mixin, "_probe_output_folder_writable"), \
                    mock.patch.object(
                        archive_mixin.youtube_traffic, "is_youtube_url",
                        return_value=True), \
                    mock.patch.object(
                        archive_mixin.youtube_traffic, "acquire",
                        return_value={"ok": False, "error": "traffic busy"}), \
                    mock.patch.object(archive_mixin, "popen_ytdlp") as launch:
                result = api.archive_single_video(
                    "https://youtube.com/watch?v=missing0001")

            self.assertTrue(result["ok"] and result["started"])
            launch.assert_not_called()
            self.assertFalse(api.archive_single_is_running())
            self.assertEqual(api._archive_single_inflight, set())
            self.assertEqual(api._archive_single_cancel_events, {})
            self.assertFalse(any(
                snapshot.owner == "manual-download"
                and snapshot.job_id == result["task_id"]
                for snapshot in channel_leases.active_snapshot()
            ))

    def test_manual_non_youtube_url_keeps_generic_id_compatibility(self) -> None:
        generic_id = "vimeo-12345"

        class ImmediateThread:
            def __init__(self, target, **_kwargs):
                self.target = target

            def start(self):
                self.target()

        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        folder = Path(temporary.name)
        media = folder / "Example Clip.mp4"
        launched_commands: list[list[str]] = []

        def launch(command, **_kwargs):
            launched_commands.append(list(command))
            return mock.Mock(name="generic-proc")

        def supervise(_proc, *, on_stdout_line, **_kwargs):
            media.write_bytes(b"generic-video")
            for line in (
                "DLPRE:::Example Clip:::Example Site",
                f"[download] Destination: {media}",
                ("DLTRACK:::Example Clip:::Example Site:::20260819:::5:::"
                 f"30:::{generic_id}"),
            ):
                on_stdout_line(line)
            return StreamingRunResult(0, [])

        api = archive_mixin.ArchiveMixin()
        api._log_stream = mock.Mock()
        api._window = None
        api._push_url_history = mock.Mock()
        api._push_recent_refresh = mock.Mock()
        registrar = mock.Mock(return_value=True)

        with mock.patch.object(
                archive_mixin.threading, "Thread", ImmediateThread), \
                mock.patch.object(
                    archive_mixin.sync_backend, "find_yt_dlp",
                    return_value="yt-dlp"), \
                mock.patch.object(
                    archive_mixin.sync_backend, "build_format_string",
                    return_value="best"), \
                mock.patch.object(
                    archive_mixin.sync_backend, "_find_cookie_source",
                    return_value=[]), \
                mock.patch.object(
                    archive_mixin.sync_backend, "_record_recent_download",
                    return_value=True), \
                mock.patch.object(
                    archive_mixin, "load_config",
                    return_value={"video_out_dir": str(folder)}), \
                mock.patch.object(
                    archive_mixin, "_probe_output_folder_writable"), \
                mock.patch.object(
                    archive_mixin.youtube_traffic, "is_youtube_url",
                    return_value=False), \
                mock.patch.object(
                    archive_mixin.youtube_traffic, "acquire") as traffic, \
                mock.patch.object(
                    archive_mixin, "popen_ytdlp", side_effect=launch), \
                mock.patch.object(
                    archive_mixin, "supervise_streaming_process",
                    side_effect=supervise), \
                mock.patch.object(index, "register_video", registrar), \
                mock.patch.object(livestreams, "drop"):
            result = api.archive_single_video(
                "https://vimeo.example/video/12345")

        self.assertTrue(result["ok"] and result["started"])
        self.assertEqual(media.read_bytes(), b"generic-video")
        traffic.assert_not_called()
        registrar.assert_called_once()
        self.assertEqual(registrar.call_args.kwargs["video_id"], generic_id)
        command = launched_commands[0]
        self.assertNotIn("--trim-filenames", command)
        output = command[command.index("--output") + 1]
        self.assertIn("%(title).170B.%(ext)s", output)
        self.assertNotIn("[%(id)s]", output)


if __name__ == "__main__":
    unittest.main()
