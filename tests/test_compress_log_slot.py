"""Compress lines must render under the video row they belong to.

Regression cover for the "✓ Compressed" done line landing under a LATER
channel's "no new videos" row. Compression is queued during a sync pass but
the encode only starts once the GPU worker gets to it — several channels
later — so without a slot reserved at queue time the whole in-place chain
("Encoding …" → progress bar → "✓ Compressed") anchors wherever the log
happened to be at that moment.

The fix mirrors the meta_done_/tx_done_ pattern: sync reserves a
"Compression queued…" placeholder carrying the same `compress_done_<hash>`
marker the encoder later emits, so logs.js `_inplaceKind` replaces it in
place. Two properties make or break that, and both are asserted here:

  1. Placeholder and done line must hash to the SAME marker.
  2. The placeholder must survive Simple mode's verbose-only filter —
     a filtered placeholder never reaches the DOM, and the done line then
     falls back to appending at log bottom (the original bug).
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

# Disposable persistence before any backend import — this module must never
# touch the user's live AppData.
_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-compress-slot-")
os.environ["APPDATA"] = _TEST_APPDATA.name
os.environ["LOCALAPPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import compress  # noqa: E402
from backend.log_stream import _line_is_verbose_only  # noqa: E402


class _FakeLog:
    def __init__(self) -> None:
        self.lines: list[list] = []

    def emit(self, segments) -> None:
        self.lines.append(segments)

    def emit_text(self, text, tag=None) -> None:
        line = text if text.endswith("\n") else text + "\n"
        self.emit([[line, tag]])

    def emit_dim(self, text) -> None:
        self.emit_text(text, "dim")

    def emit_error(self, text) -> None:
        self.emit_text(text, "red")


def _markers(segments) -> set[str]:
    """Every `compress_done_*` tag carried by a line's segments."""
    found = set()
    for seg in segments:
        tag = seg[1] if len(seg) > 1 else None
        tags = tag if isinstance(tag, (list, tuple)) else [tag]
        for t in tags:
            if isinstance(t, str) and t.startswith("compress_done_"):
                found.add(t)
    return found


def _text(segments) -> str:
    return "".join(str(seg[0]) for seg in segments)


class CompressMarkerTests(unittest.TestCase):
    def test_marker_depends_only_on_basename(self):
        """The queue-time path and the encode-time path must agree.

        sync reserves the slot from the download's `final_path`; the encoder
        sees `job["path"]` after normpath, and a channel-folder rename
        retargets that path to a new root. Only the basename is stable
        across all three, so only the basename may feed the hash.
        """
        a = compress.compress_marker_tag(r"D:\Media\Chan\2026\Video.mp4")
        b = compress.compress_marker_tag(r"D:/Media/Renamed/2026/Video.mp4")
        self.assertEqual(a, b)
        self.assertTrue(a.startswith("compress_done_"))
        self.assertNotEqual(
            a, compress.compress_marker_tag(r"D:\Media\Chan\Other.mp4"))

    def test_marker_survives_non_ascii_titles(self):
        """The tag is interpolated into a CSS attribute selector in logs.js."""
        tag = compress.compress_marker_tag('D:/M/Ünïcode "quoted" ⧸ ttl.mp4')
        self.assertRegex(tag, r"^compress_done_[0-9a-f]{12}$")


class CompressPlaceholderTests(unittest.TestCase):
    PATH = r"D:\Media\Chan\2026\Some Video Title [abc123DEF45].mp4"

    def test_placeholder_carries_the_encoder_marker(self):
        log = _FakeLog()
        compress.emit_compress_placeholder(log, self.PATH)
        self.assertEqual(len(log.lines), 1)
        self.assertEqual(
            _markers(log.lines[0]),
            {compress.compress_marker_tag(self.PATH)},
        )

    def test_placeholder_is_not_filtered_by_simple_mode(self):
        """The bug: a verbose-only placeholder never lands in the DOM.

        Simple mode drops any line whose every content segment carries a
        verbose-only primary tag. Such a placeholder is invisible to
        `_inplaceKind`, so the encoder's done line finds nothing to replace
        and appends at log bottom instead.
        """
        log = _FakeLog()
        compress.emit_compress_placeholder(log, self.PATH)
        self.assertFalse(_line_is_verbose_only(log.lines[0]))

    def test_placeholder_nests_under_the_video_row(self):
        """Six spaces — same indent as the Metadata / Transcription rows."""
        log = _FakeLog()
        compress.emit_compress_placeholder(log, self.PATH, from_download=True)
        self.assertTrue(_text(log.lines[0]).startswith("      \u2014 "))

    def test_standalone_compress_keeps_the_shallow_indent(self):
        """Right-click → Compress has no parent video row to nest under."""
        log = _FakeLog()
        compress.emit_compress_placeholder(log, self.PATH, from_download=False)
        self.assertTrue(_text(log.lines[0]).startswith(" \u2014 "))
        self.assertFalse(_text(log.lines[0]).startswith("  "))

    def test_lead_matches_the_transcription_done_line(self):
        """Compress must line up with the sibling row it sits beneath."""
        from backend.transcribe.core import (
            _build_transcription_done_segments,
        )
        tx = _build_transcription_done_segments(
            {"from_download": True}, "T", "C", "auto-captions",
            dim_tags=["dim"], em_tags=["whisper_bracket"],
            lbl_tags=["simpleline_blue"], txt_tags=["simpleline"],
            detail_tags=["tx_detail"])
        self.assertEqual(compress.compress_lead(True), tx[0][0])
        self.assertEqual(
            compress.compress_lead(False),
            _build_transcription_done_segments(
                {"from_download": False}, "T", "C", "d",
                dim_tags=["dim"], em_tags=["whisper_bracket"],
                lbl_tags=["simpleline_blue"], txt_tags=["simpleline"],
                detail_tags=["tx_detail"])[0][0],
        )


class CompressSlotReleaseTests(unittest.TestCase):
    PATH = r"D:\Media\Chan\2026\Video [abc123DEF45].mp4"

    def test_clear_emits_a_control_line_for_the_same_marker(self):
        import json
        log = _FakeLog()
        compress.clear_compress_marker(log, self.PATH)
        self.assertEqual(len(log.lines), 1)
        seg = log.lines[0][0]
        self.assertEqual(seg[1], "__control__")
        payload = json.loads(seg[0])
        self.assertEqual(payload["kind"], "clear_line")
        self.assertEqual(
            payload["marker"], compress.compress_marker_tag(self.PATH))

    def test_missing_input_releases_the_reserved_slot(self):
        """An encode that never starts must not strand its placeholder."""
        log = _FakeLog()
        res = compress.compress_video(
            os.path.join(_TEST_APPDATA.name, "does-not-exist.mp4"), log)
        self.assertFalse(res["ok"])
        self.assertTrue(
            any(seg[0][1] == "__control__" for seg in log.lines if seg),
            "compress_video must clear the slot when the input is gone",
        )


class CompressSlotOwnershipTests(unittest.TestCase):
    """Which dropped jobs still owe the log a compress line."""

    @staticmethod
    def _holds(job):
        from backend.transcribe.core import TranscribeManager
        return TranscribeManager._job_holds_compress_slot(job)

    def test_compress_job_owns_its_slot(self):
        self.assertTrue(self._holds({"kind": "compress", "path": "a.mp4"}))

    def test_transcribe_with_unqueued_followup_owns_the_slot(self):
        self.assertTrue(self._holds({
            "kind": "transcribe", "path": "a.mp4",
            "compress_after": {"quality": "Average"},
        }))

    def test_transcribe_whose_followup_already_queued_does_not(self):
        """The compress job it spawned owns the slot now — don't double-clear."""
        self.assertFalse(self._holds({
            "kind": "transcribe", "path": "a.mp4",
            "compress_after": {"quality": "Average"},
            "_followup_enqueued": True,
        }))

    def test_plain_transcribe_owns_nothing(self):
        self.assertFalse(self._holds({"kind": "transcribe", "path": "a.mp4"}))
        self.assertFalse(self._holds(None))


if __name__ == "__main__":
    unittest.main()
