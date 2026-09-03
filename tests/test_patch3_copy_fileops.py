"""Regression coverage for copy-sidecar and trash restore transactions."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-copy-tests-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend.services import file_ops


class CopyFileOpsRegressionTests(unittest.TestCase):
    def test_other_same_stem_media_keeps_every_live_sidecar(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ytarchiver-shared-stem-") as td:
            root = Path(td)
            selected = root / "same.mp4"
            survivor = root / "same.mkv"
            selected.write_bytes(b"selected")
            survivor.write_bytes(b"unrelated survivor")
            sidecars = [
                root / "same.jsonl",
                root / "same.info.json",
                root / "same.description",
                root / "same.live_chat.json",
                root / "same.srt",
                root / "same.en.vtt",
            ]
            for sidecar in sidecars:
                sidecar.write_text(
                    '{"video_id":"OTHER_VIDEO","text":"keep"}\n',
                    encoding="utf-8",
                )

            with mock.patch(
                "backend.ytarchiver_config.load_config",
                return_value={"output_dir": str(root)},
            ), mock.patch.object(
                file_ops, "config_is_writable", return_value=True,
            ):
                result = file_ops.safe_trash_video_file(str(selected))

            self.assertTrue(result["ok"], result.get("error"))
            self.assertTrue(survivor.is_file())
            self.assertTrue(all(path.is_file() for path in sidecars))
            trashed_names = {
                path.name
                for path in Path(result["trashed_folder_path"]).iterdir()
            }
            self.assertIn("same.mp4", trashed_names)
            self.assertTrue(trashed_names.isdisjoint(
                {path.name for path in sidecars}))

    def test_single_media_still_trashes_its_sidecar(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ytarchiver-owned-stem-") as td:
            root = Path(td)
            selected = root / "only.mp4"
            sidecar = root / "only.jsonl"
            selected.write_bytes(b"selected")
            sidecar.write_bytes(b"owned sidecar")

            with mock.patch(
                "backend.ytarchiver_config.load_config",
                return_value={"output_dir": str(root)},
            ), mock.patch.object(
                file_ops, "config_is_writable", return_value=True,
            ):
                result = file_ops.safe_trash_video_file(str(selected))

            self.assertTrue(result["ok"], result.get("error"))
            self.assertFalse(sidecar.exists())
            self.assertTrue(
                (Path(result["trashed_folder_path"]) / sidecar.name).is_file())

    def test_prepared_marker_never_owns_an_existing_destination(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ytarchiver-prepared-") as td:
            root = Path(td)
            media = root / "primary.mp4"
            source = root / "derived.jsonl"
            destination = root / "survivor.jsonl"
            media.write_bytes(b"media")
            source.write_bytes(b'{"video_id":"same","text":"keep"}\n')
            destination.write_bytes(source.read_bytes())
            digest = file_ops._sha256_file(str(source))
            token = "0" * 32
            marker_path = file_ops._sidecar_handoff_marker(
                str(destination), token)
            cleanup_token = {
                "token": token,
                "marker_path": marker_path,
                "destination": str(destination),
                "source": str(media),
                "sha256": digest,
            }
            file_ops._write_json_atomic(
                marker_path,
                file_ops._sidecar_handoff_value(
                    str(destination), str(media), token, digest,
                    state="prepared",
                ),
            )

            with mock.patch(
                "backend.ytarchiver_config.load_config",
                return_value={"output_dir": str(root)},
            ):
                retried = file_ops.preserve_sidecar_no_overwrite(
                    str(source), str(destination), source_identity=str(media))

            self.assertTrue(retried["ok"])
            self.assertTrue(retried["existing"])
            self.assertNotIn("cleanup_token", retried)
            rolled_back = file_ops.rollback_preserved_sidecar(cleanup_token)
            self.assertFalse(rolled_back["ok"])
            self.assertTrue(destination.is_file())
            self.assertTrue(Path(marker_path).is_file())

    def test_channel_restore_refuses_destination_created_after_precheck(
            self) -> None:
        with tempfile.TemporaryDirectory(prefix="ytarchiver-channel-race-") as td:
            root = Path(td)
            original = root / "Channel"
            original.mkdir()
            (original / "archive.mp4").write_bytes(b"archive")
            with mock.patch(
                "backend.ytarchiver_config.load_config",
                return_value={"output_dir": str(root)},
            ), mock.patch.object(
                file_ops, "config_is_writable", return_value=True,
            ):
                trashed = file_ops.safe_rmtree_channel_folder(str(original))

            trash_path = Path(trashed["trashed_folder_path"])
            real_move = file_ops._move_no_replace

            def destination_appears(source: str, destination: str) -> None:
                destination_path = Path(destination)
                destination_path.mkdir(parents=True)
                (destination_path / "replacement.txt").write_text(
                    "new live data", encoding="utf-8")
                real_move(source, destination)

            with mock.patch(
                "backend.ytarchiver_config.load_config",
                return_value={"output_dir": str(root)},
            ), mock.patch.object(
                file_ops, "config_is_writable", return_value=True,
            ), mock.patch.object(
                file_ops, "_move_no_replace", side_effect=destination_appears,
            ):
                restored = file_ops.restore_trash_entry(str(trash_path))

            self.assertFalse(restored["ok"])
            self.assertTrue(trash_path.is_dir())
            self.assertTrue((trash_path / "archive.mp4").is_file())
            self.assertEqual(
                (original / "replacement.txt").read_text(encoding="utf-8"),
                "new live data",
            )
            self.assertFalse((original / trash_path.name).exists())

    def test_video_restore_refuses_directory_created_after_precheck(
            self) -> None:
        with tempfile.TemporaryDirectory(prefix="ytarchiver-video-race-") as td:
            root = Path(td)
            channel = root / "Channel"
            channel.mkdir()
            original = channel / "Video.mp4"
            original.write_bytes(b"archive")
            with mock.patch(
                "backend.ytarchiver_config.load_config",
                return_value={"output_dir": str(root)},
            ), mock.patch.object(
                file_ops, "config_is_writable", return_value=True,
            ):
                trashed = file_ops.safe_trash_video_file(str(original))

            trash_path = Path(trashed["trashed_folder_path"])
            trash_video = Path(trashed["trashed_file_path"])
            real_move = file_ops._move_no_replace

            def destination_appears(source: str, destination: str) -> None:
                destination_path = Path(destination)
                destination_path.mkdir(parents=True)
                (destination_path / "replacement.txt").write_text(
                    "new live data", encoding="utf-8")
                real_move(source, destination)

            with mock.patch(
                "backend.ytarchiver_config.load_config",
                return_value={"output_dir": str(root)},
            ), mock.patch.object(
                file_ops, "config_is_writable", return_value=True,
            ), mock.patch.object(
                file_ops, "_move_no_replace", side_effect=destination_appears,
            ):
                restored = file_ops.restore_trash_entry(str(trash_path))

            self.assertFalse(restored["ok"])
            self.assertTrue(trash_video.is_file())
            self.assertEqual(
                (original / "replacement.txt").read_text(encoding="utf-8"),
                "new live data",
            )
            self.assertFalse((original / original.name).exists())


if __name__ == "__main__":
    unittest.main()
