from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

# Force disposable persistence before importing any backend package. Running
# this module alone must never append activity history to the user's AppData.
_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch2-followup-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import compress, deps_installer, log_stream, repair_captions


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

    def emit_activity(self, cells, alt=False) -> None:
        del cells, alt


class RepairCaptionFollowupTests(unittest.TestCase):
    @staticmethod
    def _create_repair_db(path: Path, jsonl_path: Path) -> None:
        conn = sqlite3.connect(path)
        try:
            conn.executescript("""
                CREATE TABLE segments (
                    id INTEGER PRIMARY KEY,
                    video_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    year INTEGER,
                    month INTEGER,
                    start_time REAL,
                    end_time REAL,
                    text TEXT NOT NULL,
                    jsonl_path TEXT,
                    words TEXT
                );
                CREATE VIRTUAL TABLE segments_fts USING fts5(
                    text, content=segments, content_rowid=id
                );
                CREATE TABLE indexed_files (
                    path TEXT PRIMARY KEY,
                    mtime REAL,
                    segment_count INTEGER
                );
                CREATE TABLE videos (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    filepath TEXT UNIQUE COLLATE NOCASE,
                    video_id TEXT,
                    tx_status TEXT DEFAULT 'pending'
                );
            """)
            jp = os.path.normpath(str(jsonl_path))
            conn.execute(
                "INSERT INTO videos "
                "(title, channel, filepath, video_id, tx_status) "
                "VALUES (?, ?, ?, ?, 'pending')",
                ("Title", "Channel", "missing-video.mp4", "abc123def45"),
            )
            conn.execute(
                "INSERT INTO segments "
                "(video_id, title, channel, start_time, end_time, text, "
                "jsonl_path, words) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("abc123def45", "Title", "Channel", 0.0, 1.0, "old",
                 jp, "[]"),
            )
            conn.execute(
                "INSERT INTO segments_fts(rowid, text) "
                "SELECT id, text FROM segments",
            )
            conn.commit()
        finally:
            conn.close()

    def test_structural_drift_forced_reingest_converges(self) -> None:
        segments = [
            {"s": 0.0, "e": 1.0, "t": "hello",
             "w": [{"t": "hello", "s": 0.0, "e": 1.0}]},
            {"s": 1.0, "e": 2.0, "t": "world",
             "w": [{"t": "world", "s": 1.0, "e": 2.0}]},
        ]
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            jsonl_path = root / ".Channel Transcript.jsonl"
            jsonl_path.write_text(
                "".join(json.dumps({
                    **segment,
                    "video_id": "abc123def45",
                    "title": "Title",
                }) + "\n" for segment in segments),
                encoding="utf-8",
            )
            db_path = root / "transcriptions.db"
            self._create_repair_db(db_path, jsonl_path)
            db_conn = sqlite3.connect(db_path)
            try:
                def fresh_vtt(*_args, **_kwargs):
                    vtt = root / "video.en.vtt"
                    vtt.write_text("captions", encoding="utf-8")
                    return vtt, None

                with mock.patch.object(repair_captions, "TRANSCRIPTION_DB",
                                       str(db_path)), mock.patch.object(
                        repair_captions, "_fetch_vtt_with_backoff",
                        side_effect=fresh_vtt), mock.patch.object(
                        repair_captions, "_parse_vtt",
                        return_value=segments), mock.patch.object(
                        repair_captions, "_replace_jsonl_entry"):
                    first = repair_captions._repair_one_video(
                        "yt-dlp", jsonl_path, "Title", "abc123def45",
                        "YT CAPTIONS", False, _FakeLog(), db_conn=db_conn,
                        tmp_dir=root,
                    )
                    second = repair_captions._repair_one_video(
                        "yt-dlp", jsonl_path, "Title", "abc123def45",
                        "YT CAPTIONS", False, _FakeLog(), db_conn=db_conn,
                        tmp_dir=root,
                    )

                count = db_conn.execute(
                    "SELECT COUNT(*) FROM segments WHERE video_id=?",
                    ("abc123def45",),
                ).fetchone()[0]
            finally:
                db_conn.close()

        self.assertTrue(first[0], first[1])
        self.assertIn("forced re-ingest", first[1])
        self.assertTrue(second[0], second[1])
        self.assertEqual(count, 2)

    def test_failed_forced_ingest_rejects_coincidental_stale_count(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            jsonl_path = root / ".Channel Transcript.jsonl"
            jsonl_path.write_text("{}\n", encoding="utf-8")
            db_path = root / "transcriptions.db"
            self._create_repair_db(db_path, jsonl_path)
            conn = sqlite3.connect(db_path)
            try:
                with mock.patch("backend.index.ingest_jsonl", return_value=0):
                    rebuilt, error = repair_captions._force_reingest_jsonl(
                        jsonl_path, "Title", "abc123def45", conn=conn)
                stale_count = conn.execute(
                    "SELECT COUNT(*) FROM segments WHERE video_id=?",
                    ("abc123def45",),
                ).fetchone()[0]
            finally:
                conn.close()

        self.assertEqual(stale_count, 1)
        self.assertEqual(rebuilt, 0)
        self.assertIn("reported no indexed rows", error)


class DependencySwapFollowupTests(unittest.TestCase):
    def test_post_backup_journal_failure_rolls_back_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            app_data = Path(td)
            live = app_data / "bin"
            live.mkdir()
            (live / "yt-dlp.exe").write_bytes(b"old")
            stage = app_data / ".bin-stage-test"
            stage.mkdir()
            (stage / "yt-dlp.exe").write_bytes(b"new")

            with mock.patch.object(deps_installer, "APP_DATA_DIR", app_data), \
                    mock.patch.object(
                        deps_installer, "_write_bin_swap_journal",
                        side_effect=[None, OSError("journal write failed")],
                    ) as write_journal:
                with self.assertRaisesRegex(OSError, "journal write failed"):
                    deps_installer._swap_managed_bin(stage)

            self.assertEqual(write_journal.call_count, 2)
            self.assertEqual((live / "yt-dlp.exe").read_bytes(), b"old")
            self.assertTrue(stage.exists())
            self.assertFalse(list(app_data.glob(".bin-backup-*")))

    def test_backup_moved_phase_restores_backup_over_partial_live(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            app_data = Path(td)
            live = app_data / "bin"
            live.mkdir()
            (live / "yt-dlp.exe").write_bytes(b"partial-new")
            backup = app_data / ".bin-backup-test"
            backup.mkdir()
            (backup / "yt-dlp.exe").write_bytes(b"complete-old")
            (backup / "ffmpeg.exe").write_bytes(b"complete-old-ffmpeg")
            stage = app_data / ".bin-stage-test"
            stage.mkdir()

            with mock.patch.object(deps_installer, "APP_DATA_DIR", app_data):
                deps_installer._write_bin_swap_journal({
                    "version": 1,
                    "phase": "backup_moved",
                    "stage": stage.name,
                    "backup": backup.name,
                    "had_existing": True,
                })
                deps_installer._recover_interrupted_bin_swap()

            self.assertEqual(
                (live / "yt-dlp.exe").read_bytes(), b"complete-old")
            self.assertTrue((live / "ffmpeg.exe").is_file())
            self.assertFalse(backup.exists())
            self.assertFalse(stage.exists())
            self.assertFalse((app_data / ".bin-swap.json").exists())


class LogTimerFollowupTests(unittest.TestCase):
    class _Window:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def evaluate_js(self, script: str) -> None:
            self.calls.append(script)

    def test_immediate_flush_callback_does_not_strand_timer(self) -> None:
        window = self._Window()
        stream = log_stream.LogStreamer(window)
        stream.mark_ready()

        class ImmediateTimer:
            def __init__(self, _delay, callback) -> None:
                self.callback = callback
                self.daemon = False

            def start(self) -> None:
                self.callback()

            def cancel(self) -> None:
                pass

        with mock.patch.object(log_stream.threading, "Timer", ImmediateTimer):
            stream.emit_text("first")
            self.assertIsNone(stream._flush_timer)
            stream.emit_text("second")

        self.assertIsNone(stream._flush_timer)
        self.assertEqual(len(window.calls), 2)

    def test_flush_timer_start_failure_delivers_final_buffer_synchronously(
            self) -> None:
        window = self._Window()
        stream = log_stream.LogStreamer(window)
        stream.mark_ready()

        class BrokenTimer:
            def __init__(self, _delay, _callback) -> None:
                self.daemon = False

            def start(self) -> None:
                raise RuntimeError("timer unavailable")

        with mock.patch.object(log_stream.threading, "Timer", BrokenTimer):
            stream.emit_text("retained")

        self.assertIsNone(stream._flush_timer)
        self.assertEqual(stream._buffer, [])
        self.assertEqual(len(window.calls), 1)
        self.assertIn("retained", window.calls[0])


class CompressionSummaryFollowupTests(unittest.TestCase):
    @staticmethod
    def _texts_and_tags(stream: _FakeLog) -> tuple[str, list]:
        text = "".join(str(segment[0]) for line in stream.lines
                       for segment in line)
        tags = [segment[1] for line in stream.lines for segment in line]
        return text, tags

    def test_failed_and_cancelled_batches_never_render_green_success(self) -> None:
        failed_stream = _FakeLog()
        with mock.patch.object(
                compress, "compress_video",
                return_value={"ok": False, "error": "encode failed"}):
            result = compress.compress_videos_batch(
                ["video.mp4"], failed_stream, emit_history=False)
        failed_text, failed_tags = self._texts_and_tags(failed_stream)

        self.assertFalse(result["ok"])
        self.assertIn("Batch failed:", failed_text)
        self.assertNotIn("\u2713", failed_text)
        self.assertNotIn("simpleline_green", failed_tags)
        self.assertIn("red", failed_tags)

        cancelled_stream = _FakeLog()
        cancelled = threading.Event()
        cancelled.set()
        result = compress.compress_videos_batch(
            ["video.mp4"], cancelled_stream, cancel_event=cancelled,
            emit_history=False,
        )
        cancelled_text, cancelled_tags = self._texts_and_tags(cancelled_stream)

        self.assertFalse(result["ok"])
        self.assertTrue(result["cancelled"])
        self.assertIn("Batch cancelled:", cancelled_text)
        self.assertNotIn("\u2713", cancelled_text)
        self.assertNotIn("simpleline_green", cancelled_tags)
        self.assertIn("red", cancelled_tags)

    def test_split_cancel_after_green_child_emits_red_aggregate_summary(
            self) -> None:
        stream = _FakeLog()
        cancelled = threading.Event()

        def complete_then_cancel(*_args, **_kwargs):
            cancelled.set()
            return {"ok": True, "orig_bytes": 100, "new_bytes": 50}

        with mock.patch.object(
                compress, "compress_video", side_effect=complete_then_cancel):
            result = compress.compress_videos_batch(
                ["one.mp4", "two.mp4"], stream, batch_size=1,
                cancel_event=cancelled, emit_history=False,
            )

        text, tags = self._texts_and_tags(stream)
        final_text = "".join(str(segment[0]) for segment in stream.lines[-1])
        final_tags = [segment[1] for segment in stream.lines[-1]]
        self.assertFalse(result["ok"])
        self.assertTrue(result["cancelled"])
        self.assertIn("\u2713", text)
        self.assertIn("simpleline_green", tags)
        self.assertIn("Batch cancelled:", final_text)
        self.assertNotIn("\u2713", final_text)
        self.assertNotIn("simpleline_green", final_tags)
        self.assertIn("red", final_tags)


if __name__ == "__main__":
    unittest.main()
