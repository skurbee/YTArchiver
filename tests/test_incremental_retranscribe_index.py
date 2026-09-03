from __future__ import annotations

import atexit
import hashlib
import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(
    prefix="ytarchiver-retranscribe-receipt-")
atexit.register(_TEST_APPDATA.cleanup)
os.environ["APPDATA"] = _TEST_APPDATA.name

from backend import index, index_search
from backend.transcribe import core as transcribe_core
from backend.transcribe import transcribe_files


class RetranscribeReceiptIndexTests(unittest.TestCase):
    """Safety contract for the per-video retranscribe index fast path.

    These tests intentionally obtain replacement data from the real JSONL
    writer. Calling the index delta with raw Whisper segments would permit a
    test-only DB state that differs from the rounded/normalized rows on disk.
    """

    VIDEO_A = "AAAAABBBBB1"
    VIDEO_B = "AAAAABBBBB2"
    VIDEO_C = "AAAAABBBBB3"
    CHANNEL = "Fixture Channel"

    def tearDown(self) -> None:
        self._reset_index_module()

    @staticmethod
    def _reset_index_module() -> None:
        with index._db_lock, index._reader_lock:
            index._shutdown_index()
            index._conn = None
            index._reader_conn = None
            index._schema_inited = False
            index._ingest_locks.clear()
            index._browse_videos_cache.clear()
            index._all_videos_cache.clear()
            index._thumb_index_cache.clear()
            index._download_backfill_signature = None
            index_search._title_search_cache.clear()

    @contextmanager
    def _isolated_index_db(self, root: Path):
        """Close module-owned SQLite handles before Windows removes fixtures."""
        db_path = root / "transcription_index.db"
        with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
            self._reset_index_module()
            try:
                yield
            finally:
                self._reset_index_module()

    @staticmethod
    def _line(video_id: str, title: str, text: str,
              start: float = 0.0, words: list[dict] | None = None) -> dict:
        return {
            "video_id": video_id,
            "title": title,
            "start": start,
            "end": start + 1.0,
            "text": text,
            "words": words or [],
        }

    @staticmethod
    def _worker_segment(text: str, start: float = 0.0,
                        end: float | None = None,
                        words: list[dict] | None = None) -> dict:
        return {
            "s": start,
            "e": start + 1.0 if end is None else end,
            "t": text,
            "w": words if words is not None else [],
        }

    @staticmethod
    def _write_aggregate(path: Path, rows: list[dict]) -> None:
        path.write_text(
            "".join(json.dumps(row) + "\n" for row in rows),
            encoding="utf-8",
        )

    def _seed(self, root: Path, *, duplicate_titles: bool = False,
              blank_target_id: bool = False):
        channel_dir = root / self.CHANNEL / "2026"
        channel_dir.mkdir(parents=True)
        title_a = "Duplicate title" if duplicate_titles else "Video A"
        title_b = "Duplicate title" if duplicate_titles else "Video B"
        video_a = channel_dir / f"{title_a} [{self.VIDEO_A}].mp4"
        video_b = channel_dir / f"{title_b} [{self.VIDEO_B}].mp4"
        video_a.write_bytes(b"video-a")
        video_b.write_bytes(b"video-b")
        aggregate = channel_dir / ".Fixture Channel 2026 Transcript.jsonl"
        rows = [
            self._line("" if blank_target_id else self.VIDEO_A,
                       title_a, "alpha oldword", 0.0),
            self._line("" if blank_target_id else self.VIDEO_A,
                       title_a, "alpha second", 1.0),
            self._line(self.VIDEO_B, title_b, "bravo sibling", 0.0),
        ]
        self._write_aggregate(aggregate, rows)

        self.assertTrue(index.register_video(
            str(video_a), self.CHANNEL, title_a, video_id=self.VIDEO_A))
        self.assertTrue(index.register_video(
            str(video_b), self.CHANNEL, title_b, video_id=self.VIDEO_B))
        self.assertEqual(index.ingest_jsonl(
            str(video_a), str(aggregate), title_a, self.CHANNEL), 3)
        return video_a, video_b, aggregate, title_a, title_b

    def _replace_on_disk(self, aggregate: Path, title: str, video_id: str,
                         segments: list[dict]) -> tuple[set, dict]:
        receipt: dict = {}
        removed = transcribe_files._replace_jsonl_entry(
            str(aggregate), title, video_id, segments,
            receipt_out=receipt,
        )
        self.assertTrue(receipt, "successful writer must publish a receipt")
        return removed, receipt

    @staticmethod
    def _segment_rows(conn, video_id: str) -> list[tuple]:
        return conn.execute(
            "SELECT video_id, title, channel, year, month, start_time, "
            "end_time, text, jsonl_path, words FROM segments "
            "WHERE video_id=? ORDER BY start_time, id",
            (video_id,),
        ).fetchall()

    @staticmethod
    def _fts_count(conn, term: str) -> int:
        return int(conn.execute(
            "SELECT COUNT(*) FROM segments_fts WHERE segments_fts MATCH ?",
            (term,),
        ).fetchone()[0])

    @staticmethod
    def _tracker(conn, path: Path) -> tuple | None:
        return conn.execute(
            "SELECT mtime, segment_count FROM indexed_files WHERE path=?",
            (os.path.normpath(str(path)),),
        ).fetchone()

    def _apply_delta(self, video: Path, receipt: dict,
                     conn=None) -> dict:
        kwargs = {"_conn_override": conn} if conn is not None else {}
        return index.replace_video_segments(
            str(video), self.CHANNEL, receipt, **kwargs)

    def assertDeltaAccepted(self, result: dict, count: int) -> None:
        self.assertIsInstance(result, dict)
        self.assertTrue(result.get("ok"), result)
        self.assertTrue(result.get("used_delta"), result)
        self.assertEqual(result.get("count"), count)

    def assertDeltaRejected(self, result: dict) -> None:
        self.assertIsInstance(result, dict)
        self.assertFalse(result.get("ok"), result)
        self.assertFalse(result.get("used_delta"), result)

    def test_writer_receipt_contains_exact_canonical_rows_committed_to_disk(
            self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self._isolated_index_db(Path(tmp)):
                _video_a, _video_b, aggregate, title_a, _title_b = self._seed(
                    Path(tmp))
                base_bytes = aggregate.read_bytes()
                words = [{"w": "precise", "s": 1.23456, "e": 2.34567}]

                removed, receipt = self._replace_on_disk(
                    aggregate, title_a, self.VIDEO_A,
                    [self._worker_segment(
                        "canonical replacement", 1.2349, 2.3459, words)],
                )

                disk_bytes = aggregate.read_bytes()
                disk_rows = [
                    json.loads(line)
                    for line in disk_bytes.decode("utf-8").splitlines()
                ]
                target_rows = [
                    row for row in disk_rows
                    if row.get("video_id") == self.VIDEO_A
                ]

                self.assertEqual(removed, {title_a})
                self.assertEqual(receipt["version"], 1)
                self.assertEqual(
                    receipt["jsonl_path"], os.path.normpath(str(aggregate)))
                self.assertEqual(receipt["video_id"], self.VIDEO_A)
                self.assertEqual(receipt["title"], title_a)
                self.assertEqual(list(receipt["canonical_records"]), target_rows)
                self.assertEqual(target_rows[0]["start"], 1.23)
                self.assertEqual(target_rows[0]["end"], 2.35)
                self.assertEqual(target_rows[0]["words"], [
                    {"w": "precise", "s": 1.235, "e": 2.346},
                ])
                self.assertEqual(receipt["base_searchable_count"], 3)
                self.assertEqual(receipt["final_searchable_count"], 2)
                self.assertEqual(receipt["removed_searchable_count"], 2)
                self.assertFalse(receipt["removed_blank_video_id"])
                self.assertEqual(
                    receipt["base_generation"]["size"], len(base_bytes))
                self.assertEqual(
                    receipt["final_generation"]["size"], len(disk_bytes))
                self.assertEqual(
                    receipt["final_sha256"],
                    hashlib.sha256(disk_bytes).hexdigest(),
                )

    def test_exact_id_delta_preserves_sibling_fts_status_and_tracker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self._isolated_index_db(Path(tmp)):
                video_a, _video_b, aggregate, title_a, _title_b = self._seed(
                    Path(tmp), duplicate_titles=True)
                conn = index._open()
                assert conn is not None
                sibling_before = self._segment_rows(conn, self.VIDEO_B)
                conn.execute(
                    "UPDATE videos SET tx_status='failed' WHERE video_id=?",
                    (self.VIDEO_B,),
                )
                conn.commit()
                _, receipt = self._replace_on_disk(
                    aggregate, title_a, self.VIDEO_A,
                    [self._worker_segment("alpha brandnew", 3.0)],
                )

                with mock.patch.object(
                    index, "_load_validated_jsonl_segments",
                    side_effect=AssertionError(
                        "accepted delta parsed the complete aggregate"),
                ) as full_parse:
                    result = self._apply_delta(video_a, receipt)

                self.assertDeltaAccepted(result, 1)
                full_parse.assert_not_called()
                rows_a = self._segment_rows(conn, self.VIDEO_A)
                self.assertEqual([row[7] for row in rows_a], ["alpha brandnew"])
                self.assertEqual(
                    self._segment_rows(conn, self.VIDEO_B), sibling_before)
                self.assertEqual(self._fts_count(conn, "oldword"), 0)
                self.assertEqual(self._fts_count(conn, "brandnew"), 1)
                self.assertEqual(self._fts_count(conn, "sibling"), 1)
                tracker = self._tracker(conn, aggregate)
                self.assertIsNotNone(tracker)
                self.assertAlmostEqual(
                    float(tracker[0]), os.path.getmtime(aggregate), places=5)
                self.assertEqual(tracker[1], 2)
                self.assertEqual(tracker[1], conn.execute(
                    "SELECT COUNT(*) FROM segments WHERE jsonl_path=?",
                    (os.path.normpath(str(aggregate)),),
                ).fetchone()[0])
                statuses = dict(conn.execute(
                    "SELECT video_id, tx_status FROM videos "
                    "WHERE video_id IN (?, ?)",
                    (self.VIDEO_A, self.VIDEO_B),
                ).fetchall())
                self.assertEqual(statuses[self.VIDEO_A], "transcribed")
                self.assertEqual(statuses[self.VIDEO_B], "failed")

    def test_same_id_on_other_path_is_displaced_and_tracker_recounted(
            self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._isolated_index_db(root):
                video_a, _video_b, aggregate, title_a, _title_b = self._seed(
                    root)
                conn = index._open()
                assert conn is not None

                other_dir = root / self.CHANNEL / "2025"
                other_dir.mkdir(parents=True)
                video_c = other_dir / f"Video C [{self.VIDEO_C}].mp4"
                video_c.write_bytes(b"video-c")
                other = other_dir / ".Fixture Channel 2025 Transcript.jsonl"
                self._write_aggregate(other, [
                    self._line(self.VIDEO_A, title_a,
                               "target from displaced source", 8.0),
                    self._line(self.VIDEO_C, "Video C",
                               "charlie stable source", 9.0),
                ])
                self.assertTrue(index.register_video(
                    str(video_c), self.CHANNEL, "Video C",
                    video_id=self.VIDEO_C))
                self.assertEqual(index.ingest_jsonl(
                    str(video_c), str(other), "Video C", self.CHANNEL), 2)
                self.assertEqual(
                    {row[8] for row in self._segment_rows(conn, self.VIDEO_A)},
                    {os.path.normpath(str(other))},
                )
                self.assertEqual(self._tracker(conn, aggregate)[1], 1)
                other_mtime = self._tracker(conn, other)[0]

                _, receipt = self._replace_on_disk(
                    aggregate, title_a, self.VIDEO_A,
                    [self._worker_segment("target returned home", 10.0)],
                )
                result = self._apply_delta(video_a, receipt)

                self.assertDeltaAccepted(result, 1)
                self.assertEqual(
                    {row[8] for row in self._segment_rows(conn, self.VIDEO_A)},
                    {os.path.normpath(str(aggregate))},
                )
                self.assertEqual(
                    [row[7] for row in self._segment_rows(conn, self.VIDEO_C)],
                    ["charlie stable source"],
                )
                self.assertEqual(self._tracker(conn, other), (other_mtime, 1))
                self.assertEqual(self._tracker(conn, aggregate)[1], 2)
                self.assertEqual(self._fts_count(conn, "displaced"), 0)
                self.assertEqual(self._fts_count(conn, "returned"), 1)
                self.assertEqual(self._fts_count(conn, "charlie"), 1)

    def test_stale_tracker_rejects_delta_without_mutating_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._isolated_index_db(root):
                video_a, _video_b, aggregate, title_a, _title_b = self._seed(
                    root)
                conn = index._open()
                assert conn is not None
                old_rows = self._segment_rows(conn, self.VIDEO_A)
                _, receipt = self._replace_on_disk(
                    aggregate, title_a, self.VIDEO_A,
                    [self._worker_segment("must use fallback", 4.0)],
                )
                conn.execute(
                    "UPDATE indexed_files SET mtime=mtime-10 WHERE path=?",
                    (os.path.normpath(str(aggregate)),),
                )
                conn.commit()
                stale_tracker = self._tracker(conn, aggregate)

                result = self._apply_delta(video_a, receipt)

                self.assertDeltaRejected(result)
                self.assertEqual(
                    self._segment_rows(conn, self.VIDEO_A), old_rows)
                self.assertEqual(self._tracker(conn, aggregate), stale_tracker)
                self.assertEqual(self._fts_count(conn, "oldword"), 1)
                self.assertEqual(self._fts_count(conn, "fallback"), 0)

    def test_blank_id_record_removal_rejects_delta_without_mutating_index(
            self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._isolated_index_db(root):
                video_a, _video_b, aggregate, title_a, _title_b = self._seed(
                    root, blank_target_id=True)
                conn = index._open()
                assert conn is not None
                old_rows = self._segment_rows(conn, self.VIDEO_A)
                old_tracker = self._tracker(conn, aggregate)
                _, receipt = self._replace_on_disk(
                    aggregate, title_a, self.VIDEO_A,
                    [self._worker_segment("blank id requires fallback", 4.0)],
                )
                self.assertTrue(receipt["removed_blank_video_id"])

                result = self._apply_delta(video_a, receipt)

                self.assertDeltaRejected(result)
                self.assertEqual(
                    self._segment_rows(conn, self.VIDEO_A), old_rows)
                self.assertEqual(self._tracker(conn, aggregate), old_tracker)
                self.assertEqual(self._fts_count(conn, "oldword"), 1)
                self.assertEqual(self._fts_count(conn, "requires"), 0)

    def test_disk_generation_changed_after_receipt_rejects_delta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._isolated_index_db(root):
                video_a, _video_b, aggregate, title_a, _title_b = self._seed(
                    root)
                conn = index._open()
                assert conn is not None
                old_rows = self._segment_rows(conn, self.VIDEO_A)
                _, receipt = self._replace_on_disk(
                    aggregate, title_a, self.VIDEO_A,
                    [self._worker_segment("receipt generation", 4.0)],
                )
                # The real writer restores the Windows hidden attribute.
                # Clear it before simulating an external post-receipt edit.
                transcribe_files._unhide_file_win(str(aggregate))
                aggregate.write_bytes(aggregate.read_bytes() + b"\n")

                result = self._apply_delta(video_a, receipt)

                self.assertDeltaRejected(result)
                self.assertEqual(
                    self._segment_rows(conn, self.VIDEO_A), old_rows)
                self.assertEqual(self._fts_count(conn, "generation"), 0)

    def test_delta_marks_every_physical_copy_of_video_id_transcribed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._isolated_index_db(root):
                video_a, _video_b, aggregate, title_a, _title_b = self._seed(
                    root)
                duplicate_dir = root / "Additional Library" / self.CHANNEL
                duplicate_dir.mkdir(parents=True)
                duplicate = duplicate_dir / f"Copy [{self.VIDEO_A}].mp4"
                duplicate.write_bytes(b"duplicate-video-a")
                self.assertTrue(index.register_video(
                    str(duplicate), self.CHANNEL, "Copy",
                    video_id=self.VIDEO_A))
                conn = index._open()
                assert conn is not None
                duplicate_links_before = conn.execute(
                    "SELECT filepath, is_duplicate_of FROM videos "
                    "WHERE video_id=? ORDER BY filepath",
                    (self.VIDEO_A,),
                ).fetchall()
                self.assertEqual(len(duplicate_links_before), 2)
                conn.execute(
                    "UPDATE videos SET tx_status='pending' WHERE video_id=?",
                    (self.VIDEO_A,),
                )
                conn.commit()

                _, receipt = self._replace_on_disk(
                    aggregate, title_a, self.VIDEO_A,
                    [self._worker_segment("all copies complete", 4.0)],
                )
                result = self._apply_delta(video_a, receipt)

                self.assertDeltaAccepted(result, 1)
                self.assertEqual(conn.execute(
                    "SELECT tx_status FROM videos WHERE video_id=? "
                    "ORDER BY filepath",
                    (self.VIDEO_A,),
                ).fetchall(), [("transcribed",), ("transcribed",)])
                self.assertEqual(conn.execute(
                    "SELECT filepath, is_duplicate_of FROM videos "
                    "WHERE video_id=? ORDER BY filepath",
                    (self.VIDEO_A,),
                ).fetchall(), duplicate_links_before)

    def test_insert_failure_rolls_back_rows_fts_tracker_and_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._isolated_index_db(root):
                video_a, _video_b, aggregate, title_a, _title_b = self._seed(
                    root)
                conn = index._open()
                assert conn is not None
                conn.execute(
                    "UPDATE videos SET tx_status='failed' WHERE video_id=?",
                    (self.VIDEO_A,),
                )
                conn.commit()
                old_target = self._segment_rows(conn, self.VIDEO_A)
                old_sibling = self._segment_rows(conn, self.VIDEO_B)
                old_tracker = self._tracker(conn, aggregate)
                old_status = conn.execute(
                    "SELECT filepath, tx_status FROM videos WHERE video_id=?",
                    (self.VIDEO_A,),
                ).fetchall()
                _, receipt = self._replace_on_disk(
                    aggregate, title_a, self.VIDEO_A,
                    [self._worker_segment("trigger delta failure", 7.0)],
                )
                conn.execute(
                    """CREATE TRIGGER reject_delta_fixture
                       BEFORE INSERT ON segments
                       WHEN new.text='trigger delta failure'
                       BEGIN
                         SELECT RAISE(ABORT, 'forced delta failure');
                       END"""
                )
                conn.commit()

                result = self._apply_delta(video_a, receipt)

                self.assertDeltaRejected(result)
                self.assertEqual(
                    self._segment_rows(conn, self.VIDEO_A), old_target)
                self.assertEqual(
                    self._segment_rows(conn, self.VIDEO_B), old_sibling)
                self.assertEqual(self._tracker(conn, aggregate), old_tracker)
                self.assertEqual(conn.execute(
                    "SELECT filepath, tx_status FROM videos WHERE video_id=?",
                    (self.VIDEO_A,),
                ).fetchall(), old_status)
                self.assertEqual(self._fts_count(conn, "oldword"), 1)
                self.assertEqual(self._fts_count(conn, "trigger"), 0)

    def test_active_override_transaction_is_rejected_without_stealing_it(
            self) -> None:
        """A helper must never commit or roll back its caller's transaction."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._isolated_index_db(root):
                video_a, _video_b, aggregate, title_a, _title_b = self._seed(
                    root)
                shared = index._open()
                assert shared is not None
                old_target = self._segment_rows(shared, self.VIDEO_A)
                old_tracker = self._tracker(shared, aggregate)
                _, receipt = self._replace_on_disk(
                    aggregate, title_a, self.VIDEO_A,
                    [self._worker_segment("must not steal transaction", 7.0)],
                )
                override = index._open_independent()
                self.assertIsNotNone(override)
                assert override is not None
                try:
                    override.execute("BEGIN IMMEDIATE")
                    override.execute(
                        "UPDATE videos SET tx_status='caller_uncommitted' "
                        "WHERE video_id=?",
                        (self.VIDEO_B,),
                    )
                    self.assertTrue(override.in_transaction)

                    result = self._apply_delta(
                        video_a, receipt, conn=override)

                    self.assertDeltaRejected(result)
                    self.assertTrue(
                        override.in_transaction,
                        "delta helper committed or rolled back caller work",
                    )
                    self.assertEqual(override.execute(
                        "SELECT tx_status FROM videos WHERE video_id=?",
                        (self.VIDEO_B,),
                    ).fetchone()[0], "caller_uncommitted")
                    self.assertEqual(
                        self._segment_rows(override, self.VIDEO_A), old_target)
                    self.assertEqual(
                        self._tracker(override, aggregate), old_tracker)
                    self.assertEqual(self._fts_count(override, "oldword"), 1)
                    self.assertEqual(self._fts_count(override, "steal"), 0)
                finally:
                    if override.in_transaction:
                        override.rollback()
                    override.close()

    def test_empty_retranscribe_preserves_existing_transcript_and_status(
            self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._isolated_index_db(root):
                video_a, _video_b, aggregate, title_a, _title_b = self._seed(
                    root)
                txt = aggregate.with_name("Fixture Channel 2026 Transcript.txt")
                txt.write_text("existing readable transcript\n", encoding="utf-8")
                conn = index._open()
                assert conn is not None
                disk_before = (aggregate.read_bytes(), txt.read_bytes())
                rows_before = self._segment_rows(conn, self.VIDEO_A)
                tracker_before = self._tracker(conn, aggregate)
                status_before = conn.execute(
                    "SELECT tx_status FROM videos WHERE filepath=?",
                    (os.path.normpath(str(video_a)),),
                ).fetchone()[0]
                self.assertEqual(status_before, "transcribed")
                manager = transcribe_core.TranscribeManager(
                    mock.Mock(), model="small")

                with (
                    mock.patch.object(
                        transcribe_core, "_resolve_transcript_paths",
                        return_value=(str(txt), str(aggregate), 2026, 1,
                                      "01.01.2026"),
                    ),
                    mock.patch.object(
                        transcribe_core, "_extract_video_id",
                        return_value=self.VIDEO_A,
                    ),
                ):
                    outcome = manager._write_outputs(
                        str(video_a), {"text": "", "segments": []},
                        title=title_a, channel=self.CHANNEL,
                        retranscribe=True, video_id_hint=self.VIDEO_A,
                        job={"retranscribe": True},
                    )

                self.assertNotEqual(outcome.value, "no_speech")
                self.assertEqual(
                    (aggregate.read_bytes(), txt.read_bytes()), disk_before)
                self.assertEqual(
                    self._segment_rows(conn, self.VIDEO_A), rows_before)
                self.assertEqual(self._tracker(conn, aggregate), tracker_before)
                self.assertEqual(conn.execute(
                    "SELECT tx_status FROM videos WHERE filepath=?",
                    (os.path.normpath(str(video_a)),),
                ).fetchone()[0], status_before)
                self.assertEqual(self._fts_count(conn, "oldword"), 1)

    def test_first_time_empty_transcription_still_records_no_speech(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._isolated_index_db(root):
                video = root / f"Silent [{self.VIDEO_A}].mp4"
                video.write_bytes(b"silent fixture")
                self.assertTrue(index.register_video(
                    str(video), self.CHANNEL, "Silent",
                    video_id=self.VIDEO_A))
                manager = transcribe_core.TranscribeManager(
                    mock.Mock(), model="small")

                outcome = manager._write_outputs(
                    str(video), {"text": "", "segments": []},
                    title="Silent", channel=self.CHANNEL,
                    retranscribe=False, video_id_hint=self.VIDEO_A,
                )

                self.assertEqual(outcome.value, "no_speech")
                conn = index._open()
                assert conn is not None
                self.assertEqual(conn.execute(
                    "SELECT tx_status FROM videos WHERE filepath=?",
                    (os.path.normpath(str(video)),),
                ).fetchone()[0], "no_speech")

    def test_retranscribe_output_passes_writer_receipt_to_delta_api(self) -> None:
        stream = mock.Mock()
        manager = transcribe_core.TranscribeManager(stream, model="small")
        result = {
            "model": "small",
            "text": "Fresh transcript.",
            "segments": [self._worker_segment(
                "Fresh transcript.", 1.234, 2.345,
                [{"w": "Fresh", "s": 1.2345, "e": 1.8}])],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            video = root / f"Duplicate title [{self.VIDEO_A}].mp4"
            txt = root / "Fixture Channel Transcript.txt"
            aggregate = root / ".Fixture Channel Transcript.jsonl"
            video.write_bytes(b"video")
            txt.write_text("old text", encoding="utf-8")
            self._write_aggregate(aggregate, [
                self._line(self.VIDEO_A, "Duplicate title", "old text"),
            ])
            independent = mock.Mock()

            with (
                mock.patch.object(
                    transcribe_core, "_resolve_transcript_paths",
                    return_value=(str(txt), str(aggregate), 2026, 1,
                                  "01.01.2026"),
                ),
                mock.patch.object(
                    transcribe_core, "_extract_video_id",
                    return_value=self.VIDEO_A,
                ),
                mock.patch.object(transcribe_core, "_replace_txt_entry"),
                mock.patch.object(
                    transcribe_core,
                    "_hide_per_video_transcript_txt_if_needed",
                ),
                mock.patch.object(
                    manager, "_arm_output_write_intent", return_value=True,
                ),
                mock.patch.object(
                    index, "_open_independent", return_value=independent,
                ),
                mock.patch.object(
                    index, "replace_video_segments", create=True,
                    return_value={"ok": True, "count": 1,
                                  "used_delta": True, "reason": ""},
                ) as replace,
                mock.patch.object(
                    index, "ingest_jsonl",
                    side_effect=AssertionError(
                        "successful receipt delta used full aggregate ingest"),
                ) as full_ingest,
            ):
                outcome = manager._write_outputs(
                    str(video), result, title="Duplicate title",
                    channel=self.CHANNEL, retranscribe=True,
                    video_id_hint=self.VIDEO_A,
                )

            disk_target = [
                json.loads(line) for line in aggregate.read_text().splitlines()
                if json.loads(line).get("video_id") == self.VIDEO_A
            ]

        self.assertIs(outcome, transcribe_core._WorkerOutcome.SUCCESS)
        full_ingest.assert_not_called()
        replace.assert_called_once()
        args, kwargs = replace.call_args
        self.assertEqual(args[:2], (str(video), self.CHANNEL))
        receipt = args[2]
        self.assertEqual(receipt["video_id"], self.VIDEO_A)
        self.assertEqual(list(receipt["canonical_records"]), disk_target)
        self.assertIs(kwargs["_conn_override"], independent)
        independent.close.assert_called_once_with()

    def test_rejected_delta_falls_back_to_forced_full_ingest(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock(), model="small")
        result = {
            "model": "small",
            "text": "Fallback transcript.",
            "segments": [self._worker_segment("Fallback transcript.")],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            video = root / f"Title [{self.VIDEO_A}].mp4"
            txt = root / "Fixture Channel Transcript.txt"
            aggregate = root / ".Fixture Channel Transcript.jsonl"
            video.write_bytes(b"video")
            txt.write_text("old text", encoding="utf-8")
            self._write_aggregate(aggregate, [
                self._line(self.VIDEO_A, "Title", "old text"),
            ])
            independent = mock.Mock()

            with (
                mock.patch.object(
                    transcribe_core, "_resolve_transcript_paths",
                    return_value=(str(txt), str(aggregate), 2026, 1,
                                  "01.01.2026"),
                ),
                mock.patch.object(
                    transcribe_core, "_extract_video_id",
                    return_value=self.VIDEO_A,
                ),
                mock.patch.object(transcribe_core, "_replace_txt_entry"),
                mock.patch.object(
                    transcribe_core,
                    "_hide_per_video_transcript_txt_if_needed",
                ),
                mock.patch.object(
                    manager, "_arm_output_write_intent", return_value=True,
                ),
                mock.patch.object(
                    index, "_open_independent", return_value=independent,
                ),
                mock.patch.object(
                    index, "replace_video_segments", create=True,
                    return_value={"ok": False, "count": 0,
                                  "used_delta": False,
                                  "can_fallback": True,
                                  "reason": "stale tracker"},
                ) as replace,
                mock.patch.object(
                    index, "ingest_jsonl", return_value=1,
                ) as full_ingest,
            ):
                outcome = manager._write_outputs(
                    str(video), result, title="Title", channel=self.CHANNEL,
                    retranscribe=True, video_id_hint=self.VIDEO_A,
                )

        self.assertIs(outcome, transcribe_core._WorkerOutcome.SUCCESS)
        replace.assert_called_once()
        full_ingest.assert_called_once_with(
            str(video), str(aggregate), "Title", self.CHANNEL,
            _conn_override=independent, force=True,
        )
        independent.close.assert_called_once_with()

    def test_delta_database_failure_does_not_start_full_rebuild(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock(), model="small")
        result = {
            "model": "small",
            "text": "Database failure transcript.",
            "segments": [self._worker_segment("Database failure transcript.")],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            video = root / f"Title [{self.VIDEO_A}].mp4"
            txt = root / "Fixture Channel Transcript.txt"
            aggregate = root / ".Fixture Channel Transcript.jsonl"
            video.write_bytes(b"video")
            txt.write_text("old text", encoding="utf-8")
            self._write_aggregate(aggregate, [
                self._line(self.VIDEO_A, "Title", "old text"),
            ])
            independent = mock.Mock()

            with (
                mock.patch.object(
                    transcribe_core, "_resolve_transcript_paths",
                    return_value=(str(txt), str(aggregate), 2026, 1,
                                  "01.01.2026"),
                ),
                mock.patch.object(
                    transcribe_core, "_extract_video_id",
                    return_value=self.VIDEO_A,
                ),
                mock.patch.object(transcribe_core, "_replace_txt_entry"),
                mock.patch.object(
                    transcribe_core,
                    "_hide_per_video_transcript_txt_if_needed",
                ),
                mock.patch.object(
                    manager, "_arm_output_write_intent", return_value=True,
                ),
                mock.patch.object(
                    index, "_open_independent", return_value=independent,
                ),
                mock.patch.object(
                    index, "replace_video_segments", create=True,
                    return_value={"ok": False, "count": 0,
                                  "used_delta": False,
                                  "can_fallback": False,
                                  "reason": "forced database failure"},
                ),
                mock.patch.object(index, "ingest_jsonl") as full_ingest,
            ):
                outcome = manager._write_outputs(
                    str(video), result, title="Title", channel=self.CHANNEL,
                    retranscribe=True, video_id_hint=self.VIDEO_A,
                )

        self.assertIs(outcome, transcribe_core._WorkerOutcome.FAILED)
        full_ingest.assert_not_called()
        independent.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
