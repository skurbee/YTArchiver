from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from backend import reorg, repair_captions
from backend.metadata import io as metadata_io
from backend.services import sidecar_store
from backend.transcribe import transcribe_files


class SidecarReadTests(unittest.TestCase):
    def test_missing_is_distinct_from_unreadable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "records.jsonl"
            self.assertFalse(sidecar_store.read_bytes(path).exists)
            path.write_bytes(b'{"ok": true}\n')
            real_open = open

            def denied(candidate, mode="r", *args, **kwargs):
                if Path(candidate) == path and "r" in mode:
                    raise PermissionError("injected denial")
                return real_open(candidate, mode, *args, **kwargs)

            with mock.patch("builtins.open", side_effect=denied):
                with self.assertRaises(sidecar_store.SidecarReadError):
                    sidecar_store.read_bytes(path)

    def test_jsonl_requires_an_object_on_every_nonblank_line(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "records.jsonl"
            path.write_text('{"ok": 1}\n["not", "an", "object"]\n',
                            encoding="utf-8")
            with self.assertRaises(sidecar_store.SidecarValidationError):
                sidecar_store.read_jsonl(path)
            partial = sidecar_store.read_jsonl(path, invalid="skip")
            self.assertEqual(partial.records, ({"ok": 1},))
            self.assertEqual(partial.invalid_lines, (2,))


class AtomicSidecarWriteTests(unittest.TestCase):
    def test_stage_write_denial_leaves_old_bytes_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "state.jsonl"
            old = b'{"old": true}\n'
            target.write_bytes(old)
            real_open = open

            def denied(candidate, mode="r", *args, **kwargs):
                if Path(candidate).parent == target.parent and mode == "xb":
                    raise PermissionError("injected stage denial")
                return real_open(candidate, mode, *args, **kwargs)

            with mock.patch("builtins.open", side_effect=denied):
                with self.assertRaises(sidecar_store.SidecarWriteError):
                    sidecar_store.atomic_write_jsonl(target, [{"new": True}])
            self.assertEqual(target.read_bytes(), old)

    def test_write_and_fsync_failure_leave_old_bytes_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "state.jsonl"
            old = b'{"old": true}\n'
            target.write_bytes(old)

            with mock.patch.object(sidecar_store.os, "fsync",
                                   side_effect=OSError("disk full")):
                with self.assertRaises(sidecar_store.SidecarWriteError):
                    sidecar_store.atomic_write_jsonl(target, [{"new": True}])

            self.assertEqual(target.read_bytes(), old)
            self.assertEqual(list(Path(td).glob("*.stage")), [])

    def test_replace_failure_leaves_old_bytes_and_cleans_stage(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "state.jsonl"
            old = b'{"old": true}\n'
            target.write_bytes(old)
            with mock.patch.object(sidecar_store.os, "replace",
                                   side_effect=PermissionError("busy")):
                with self.assertRaises(sidecar_store.SidecarWriteError):
                    sidecar_store.atomic_write_jsonl(target, [{"new": True}])
            self.assertEqual(target.read_bytes(), old)
            self.assertEqual(list(Path(td).glob("*.stage")), [])

    def test_staged_validation_runs_before_replace(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "state.txt"
            target.write_text("old", encoding="utf-8")

            def reject(_payload: bytes) -> None:
                raise sidecar_store.SidecarValidationError("injected invalid")

            with mock.patch.object(sidecar_store.os, "replace") as replace:
                with self.assertRaises(sidecar_store.SidecarValidationError):
                    sidecar_store.atomic_write_text(
                        target, "new", validator=reject)
            replace.assert_not_called()
            self.assertEqual(target.read_text(encoding="utf-8"), "old")

    def test_stage_is_in_target_directory_and_jsonl_is_read_back_validated(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "state.jsonl"
            real_replace = os.replace
            replacements: list[tuple[Path, Path]] = []

            def observe(source, destination):
                replacements.append((Path(source), Path(destination)))
                return real_replace(source, destination)

            with mock.patch.object(sidecar_store.os, "replace",
                                   side_effect=observe):
                sidecar_store.atomic_write_jsonl(target, [{"ok": True}])
            self.assertEqual(len(replacements), 1)
            self.assertEqual(replacements[0][0].parent, target.parent)
            self.assertEqual(replacements[0][1], target)
            self.assertEqual(
                sidecar_store.read_jsonl(target).records,
                ({"ok": True},),
            )

    def test_atomic_append_refuses_invalid_old_generation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "journal.jsonl"
            old = b'{"old": true}\n["not-an-object"]\n'
            target.write_bytes(old)
            with self.assertRaises(sidecar_store.SidecarValidationError):
                sidecar_store.append_jsonl_object(target, {"new": True})
            self.assertEqual(target.read_bytes(), old)


class ReconciliationMarkerTests(unittest.TestCase):
    def test_partial_marker_survives_and_all_committed_marker_clears(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / ".repair.reconcile.json"
            marker = sidecar_store.begin_reconciliation(
                path,
                operation="caption repair",
                stores=("transcript-jsonl", "search-index"),
                details={"video_id": "abcdefghijk"},
            )
            marker.mark_committed("transcript-jsonl")
            pending = sidecar_store.load_reconciliation_marker(path)
            self.assertIsNotNone(pending)
            self.assertEqual(
                [s["state"] for s in pending["stores"]],
                ["committed", "pending"],
            )
            with self.assertRaises(sidecar_store.SidecarValidationError):
                marker.finish()

            marker.mark_committed("search-index")
            self.assertTrue(marker.finish())
            self.assertFalse(path.exists())

    def test_marker_update_failure_preserves_last_durable_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / ".repair.reconcile.json"
            marker = sidecar_store.begin_reconciliation(
                path,
                operation="caption repair",
                stores=("jsonl", "db"),
            )
            before = json.loads(path.read_text(encoding="utf-8"))
            with mock.patch.object(sidecar_store.os, "replace",
                                   side_effect=PermissionError("injected")):
                with self.assertRaises(sidecar_store.SidecarWriteError):
                    marker.mark_committed("jsonl")
            after = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(after, before)
            self.assertTrue(path.exists())

    def test_corrupt_marker_is_never_treated_as_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / ".repair.reconcile.json"
            path.write_text("[]", encoding="utf-8")
            with self.assertRaises(sidecar_store.SidecarValidationError):
                sidecar_store.load_reconciliation_marker(path)


class MigratedTranscriptWriterTests(unittest.TestCase):
    def test_append_refuses_non_object_old_line_without_changing_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / ".Channel Transcript.jsonl"
            old = b'{"video_id":"old00000001"}\n"invalid-record"\n'
            target.write_bytes(old)
            with mock.patch.object(transcribe_files, "_unhide_file_win"), \
                    mock.patch.object(transcribe_files, "_hide_file_win"):
                ok = transcribe_files._write_jsonl_entry(
                    str(target),
                    "new00000001",
                    "New",
                    [{"s": 0.0, "e": 1.0, "t": "text", "w": []}],
                )
            self.assertFalse(ok)
            self.assertEqual(target.read_bytes(), old)

    def test_metadata_writer_validates_values_before_replace(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / ".Channel Metadata.jsonl"
            old = b'{"video_id":"old00000001"}\n'
            target.write_bytes(old)
            with mock.patch.object(metadata_io, "_unhide_file_win"), \
                    mock.patch.object(metadata_io, "_hide_file_win"):
                with self.assertRaises(sidecar_store.SidecarValidationError):
                    metadata_io._write_metadata_jsonl(
                        str(target), {"bad": ["not-an-object"]})
            self.assertEqual(target.read_bytes(), old)

    def test_reorg_refuses_invalid_jsonl_aggregate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Channel"
            bucket = root / "2026"
            bucket.mkdir(parents=True)
            source = bucket / ".Channel 2026 Metadata.jsonl"
            source.write_text('["not-an-object"]\n', encoding="utf-8")
            stream = mock.Mock()

            moved = reorg._relocate_flat_aggregate_files(
                root, "Channel", stream)

            self.assertEqual(moved, 0)
            self.assertTrue(source.exists())
            self.assertFalse((root / ".Channel Metadata.jsonl").exists())
            stream.emit_error.assert_called_once()

    def test_caption_repair_keeps_marker_when_database_commit_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            jsonl = root / ".Channel Transcript.jsonl"
            jsonl.write_text('{"video_id":"abcdefghijk"}\n', encoding="utf-8")
            vtt = root / "captions.vtt"
            vtt.write_text("WEBVTT", encoding="utf-8")
            marker_path = root / ".repair.reconcile.json"
            segments = [{"s": 0.0, "e": 1.0, "t": "hello", "w": []}]

            with mock.patch.object(
                    repair_captions, "_fetch_vtt_with_backoff",
                    return_value=(vtt, None)), mock.patch.object(
                    repair_captions, "_parse_vtt", return_value=segments), \
                    mock.patch.object(repair_captions,
                                      "_replace_jsonl_entry"), \
                    mock.patch.object(repair_captions, "_update_db_words",
                                      return_value=(0, "database locked")), \
                    mock.patch.object(repair_captions, "_repair_marker_path",
                                      return_value=marker_path):
                ok, message, _segments, _words = (
                    repair_captions._repair_one_video(
                        "yt-dlp",
                        jsonl,
                        "Title",
                        "abcdefghijk",
                        "YT CAPTIONS",
                        False,
                        mock.Mock(),
                        tmp_dir=root,
                    )
                )

            self.assertFalse(ok)
            self.assertIn("partial", message)
            marker = sidecar_store.load_reconciliation_marker(marker_path)
            self.assertEqual(
                [store["state"] for store in marker["stores"]],
                ["committed", "pending"],
            )


if __name__ == "__main__":
    unittest.main()
