"""Regression coverage for trash allocation and file-attribute fidelity."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-trash-tests-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend.services import file_ops


class TrashSafetyRegressionTests(unittest.TestCase):
    @staticmethod
    def _config(root: Path) -> dict[str, object]:
        return {"output_dir": str(root), "tp_archive_roots": []}

    def test_concurrent_same_name_channels_claim_distinct_trash_folders(
            self) -> None:
        with tempfile.TemporaryDirectory(prefix="ytarchiver-trash-race-") as td:
            root = Path(td) / "Archive"
            first = root / "First" / "Channel"
            second = root / "Second" / "Channel"
            first.mkdir(parents=True)
            second.mkdir(parents=True)
            (first / "first.mp4").write_bytes(b"first")
            (second / "second.mp4").write_bytes(b"second")

            real_trash_path_for = file_ops._trash_path_for
            first_candidates = threading.Barrier(2)
            call_lock = threading.Lock()
            call_count = 0

            def synchronized_candidate(folder_path: str, archive_root: str) -> str:
                nonlocal call_count
                candidate = real_trash_path_for(folder_path, archive_root)
                with call_lock:
                    call_count += 1
                    synchronize = call_count <= 2
                if synchronize:
                    first_candidates.wait(timeout=5)
                return candidate

            fixed_time = datetime(2026, 8, 31, 12, 34, 56)
            with mock.patch(
                "backend.ytarchiver_config.load_config",
                return_value=self._config(root),
            ), mock.patch.object(
                file_ops, "config_is_writable", return_value=True,
            ), mock.patch.object(
                file_ops, "_trash_path_for", side_effect=synchronized_candidate,
            ), mock.patch.object(file_ops, "datetime") as mocked_datetime:
                mocked_datetime.now.return_value = fixed_time
                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = [
                        executor.submit(
                            file_ops.safe_rmtree_channel_folder, str(path))
                        for path in (first, second)
                    ]
                    results = [future.result(timeout=10) for future in futures]

            self.assertTrue(all(result["ok"] for result in results), results)
            trash_paths = [
                Path(result["trashed_folder_path"]) for result in results
            ]
            self.assertEqual(len(set(trash_paths)), 2)
            self.assertFalse(first.exists())
            self.assertFalse(second.exists())

            expected_files = {str(first): "first.mp4", str(second): "second.mp4"}
            for trash_path in trash_paths:
                manifest = json.loads(
                    (trash_path / ".ytarchiver-trash.json").read_text(
                        encoding="utf-8"))
                original = manifest["original_path"]
                self.assertEqual(manifest["state"], "complete")
                self.assertTrue((trash_path / expected_files[original]).is_file())
                self.assertFalse((trash_path / "Channel").exists())

    def test_video_restore_reapplies_each_recorded_hidden_state(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ytarchiver-hidden-state-") as td:
            root = Path(td) / "Archive"
            channel = root / "Channel"
            channel.mkdir(parents=True)
            video = channel / "Video.mp4"
            hidden_sidecar = channel / "Video.jsonl"
            visible_sidecar = channel / "Video.info.json"
            video.write_bytes(b"video")
            hidden_sidecar.write_bytes(b"hidden")
            visible_sidecar.write_bytes(b"visible")

            states = {
                str(video): True,
                str(hidden_sidecar): True,
                str(visible_sidecar): False,
            }
            hidden_calls: list[str] = []

            def is_hidden(path: str) -> bool:
                return states.get(str(path), False)

            def hide(path: str) -> None:
                normalized = str(path)
                hidden_calls.append(normalized)
                states[normalized] = True

            def unhide(path: str) -> None:
                states[str(path)] = False

            with mock.patch(
                "backend.ytarchiver_config.load_config",
                return_value=self._config(root),
            ), mock.patch.object(
                file_ops, "config_is_writable", return_value=True,
            ), mock.patch.object(
                file_ops, "_file_has_hidden_attribute", side_effect=is_hidden,
            ), mock.patch.object(
                file_ops, "hide_file_win", side_effect=hide,
            ), mock.patch.object(
                file_ops, "unhide_file_win", side_effect=unhide,
            ):
                trashed = file_ops.safe_trash_video_file(
                    str(video), unhide_first=True)
                self.assertTrue(trashed["ok"], trashed.get("error"))

                manifest = json.loads(
                    (Path(trashed["trashed_folder_path"])
                     / ".ytarchiver-trash.json").read_text(encoding="utf-8"))
                recorded = {
                    entry["original_path"]: entry["original_hidden"]
                    for entry in manifest["files"]
                }
                self.assertEqual(recorded[str(video)], True)
                self.assertEqual(recorded[str(hidden_sidecar)], True)
                self.assertEqual(recorded[str(visible_sidecar)], False)

                restored = file_ops.restore_trash_entry(
                    trashed["trashed_folder_path"])

            self.assertTrue(restored["ok"], restored.get("error"))
            self.assertTrue(states[str(video)])
            self.assertTrue(states[str(hidden_sidecar)])
            self.assertFalse(states[str(visible_sidecar)])
            self.assertIn(str(video), hidden_calls)
            self.assertIn(str(hidden_sidecar), hidden_calls)
            self.assertNotIn(str(visible_sidecar), hidden_calls)


if __name__ == "__main__":
    unittest.main()
