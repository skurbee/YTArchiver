from __future__ import annotations

# APPDATA must be redirected before backend imports in this standalone test.
# ruff: noqa: E402, I001

import atexit
import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock


_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch2-tests-")
atexit.register(_TEST_APPDATA.cleanup)
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import (
    archive_scan,
    index,
    index_maintenance,
    punct_restore,
    ytarchiver_config,
)


def _reset_index() -> None:
    index._shutdown_index()
    index._conn = None
    index._reader_conn = None
    index._schema_inited = False
    index._ingest_locks.clear()


@contextmanager
def _temporary_directory():
    """Close SQLite handles before Windows removes the temporary DB."""
    with tempfile.TemporaryDirectory() as directory:
        try:
            yield directory
        finally:
            _reset_index()


class _FailingInsertConnection:
    """Delegate to SQLite but fail after ingest has removed old rows."""

    def __init__(self, connection):
        self.connection = connection

    @property
    def in_transaction(self):
        return self.connection.in_transaction

    def execute(self, *args, **kwargs):
        return self.connection.execute(*args, **kwargs)

    def executemany(self, *_args, **_kwargs):
        raise RuntimeError("injected segment insert failure")

    def commit(self):
        return self.connection.commit()

    def rollback(self):
        return self.connection.rollback()


class Patch2IndexSafetyTests(unittest.TestCase):
    def tearDown(self) -> None:
        _reset_index()

    def _seed_transcript(self, root: Path):
        db_path = root / "transcription_index.db"
        video_path = root / "Video [abc123_def4].mp4"
        jsonl_path = root / "Video [abc123_def4].jsonl"
        video_path.write_bytes(b"video")
        jsonl_path.write_text(
            json.dumps({
                "video_id": "abc123_def4",
                "title": "Video",
                "start": 0,
                "end": 1,
                "text": "known good transcript",
                "words": [],
            }) + "\n",
            encoding="utf-8",
        )
        return db_path, video_path, jsonl_path

    def test_malformed_jsonl_never_replaces_known_good_rows(self) -> None:
        malformed_documents = {
            "torn JSON after a valid line": (
                json.dumps({
                    "start": 0, "end": 1, "text": "partial",
                    "words": [],
                }) + "\n{not-json\n"
            ),
            "non-object line": "[]\n",
            "invalid timestamp": (
                '{"start": 0, "end": "later", "text": "bad", '
                '"words": []}\n'
            ),
            "non-finite timestamp": (
                '{"start": 0, "end": NaN, "text": "bad", '
                '"words": []}\n'
            ),
            "invalid words schema": (
                '{"start": 0, "end": 1, "text": "bad", '
                '"words": {}}\n'
            ),
        }
        with _temporary_directory() as td:
            root = Path(td)
            db_path, video_path, jsonl_path = self._seed_transcript(root)
            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                _reset_index()
                self.assertTrue(index.register_video(
                    str(video_path), "Channel", "Video",
                    video_id="abc123_def4"))
                self.assertEqual(index.ingest_jsonl(
                    str(video_path), str(jsonl_path), "Video", "Channel"), 1)
                conn = index._open()
                self.assertIsNotNone(conn)
                assert conn is not None

                for label, document in malformed_documents.items():
                    with self.subTest(label=label):
                        jsonl_path.write_text(document, encoding="utf-8")
                        self.assertEqual(index.ingest_jsonl(
                            str(video_path), str(jsonl_path), "Video", "Channel",
                            force=True), 0)
                        self.assertEqual(conn.execute(
                            "SELECT text FROM segments WHERE jsonl_path=?",
                            (os.path.normpath(str(jsonl_path)),),
                        ).fetchall(), [("known good transcript",)])
                        self.assertEqual(conn.execute(
                            "SELECT COUNT(*) FROM segments_fts "
                            "WHERE segments_fts MATCH 'known'"
                        ).fetchone()[0], 1)
                        self.assertFalse(conn.in_transaction)

    def test_ingest_rolls_back_non_sqlite_failure_after_delete(self) -> None:
        with _temporary_directory() as td:
            root = Path(td)
            db_path, video_path, jsonl_path = self._seed_transcript(root)
            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                _reset_index()
                self.assertEqual(index.ingest_jsonl(
                    str(video_path), str(jsonl_path), "Video", "Channel"), 1)
                conn = index._open()
                self.assertIsNotNone(conn)
                assert conn is not None
                jsonl_path.write_text(json.dumps({
                    "start": 1,
                    "end": 2,
                    "text": "replacement",
                    "words": [],
                }) + "\n", encoding="utf-8")

                result = index.ingest_jsonl(
                    str(video_path), str(jsonl_path), "Video", "Channel",
                    _conn_override=_FailingInsertConnection(conn), force=True)

                self.assertEqual(result, 0)
                self.assertEqual(conn.execute(
                    "SELECT text FROM segments").fetchall(),
                    [("known good transcript",)])
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments_fts "
                    "WHERE segments_fts MATCH 'known'"
                ).fetchone()[0], 1)
                self.assertFalse(conn.in_transaction)

    def test_failed_index_open_discards_connection_and_next_call_retries(self) -> None:
        with _temporary_directory() as td:
            db_path = Path(td) / "transcription_index.db"
            db_path.write_bytes(b"this is not a sqlite database")
            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                _reset_index()
                self.assertIsNone(index._open())
                self.assertIsNone(index._conn)
                self.assertFalse(index._schema_inited)
                self.assertIsNone(index._reader_open())

                # Closing the failed handle is especially important on
                # Windows: it lets recovery replace the bad file immediately.
                db_path.unlink()
                recovered = index._open()
                self.assertIsNotNone(recovered)
                self.assertTrue(index._schema_inited)
                assert recovered is not None
                self.assertIsNotNone(recovered.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name='videos'"
                ).fetchone())

    def test_prune_requires_two_confirmed_absences(self) -> None:
        with _temporary_directory() as td:
            db_path = Path(td) / "transcription_index.db"
            missing_path = str(Path(td) / "missing.mp4")
            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                _reset_index()
                conn = index._open()
                self.assertIsNotNone(conn)
                assert conn is not None
                conn.execute(
                    "INSERT INTO videos(filepath, title, channel, video_id) "
                    "VALUES (?, ?, ?, ?)",
                    (missing_path, "Missing", "Channel", "abc123_def4"),
                )
                conn.commit()

                first = index_maintenance.prune_missing_videos()
                self.assertEqual(first["videos_removed"], 0)
                self.assertEqual(first["pending_missing"], 1)
                self.assertEqual(conn.execute(
                    "SELECT availability FROM videos WHERE filepath=?",
                    (missing_path,),
                ).fetchone(), ("missing",))

                second = index_maintenance.prune_missing_videos()
                self.assertEqual(second["videos_removed"], 1)
                self.assertEqual(second["missing"], 1)
                self.assertIsNone(conn.execute(
                    "SELECT id FROM videos WHERE filepath=?",
                    (missing_path,),
                ).fetchone())

    def test_prune_treats_permission_error_as_unknown(self) -> None:
        with _temporary_directory() as td:
            db_path = Path(td) / "transcription_index.db"
            protected_path = str(Path(td) / "protected.mp4")
            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                _reset_index()
                conn = index._open()
                self.assertIsNotNone(conn)
                assert conn is not None
                conn.execute(
                    "INSERT INTO videos(filepath, title, channel, video_id, "
                    "availability) VALUES (?, ?, ?, ?, 'missing')",
                    (protected_path, "Protected", "Channel", "abc123_def4"),
                )
                conn.commit()

                with mock.patch.object(
                        index_maintenance.os, "stat",
                        side_effect=PermissionError("access denied")):
                    result = index_maintenance.prune_missing_videos()

                self.assertEqual(result["videos_removed"], 0)
                self.assertEqual(result["unavailable"], 1)
                self.assertEqual(conn.execute(
                    "SELECT availability FROM videos WHERE filepath=?",
                    (protected_path,),
                ).fetchone(), ("missing",))
                self.assertFalse(conn.in_transaction)

    def test_fresh_archive_stats_initializes_schema_before_querying(self) -> None:
        with _temporary_directory() as td:
            db_path = Path(td) / "new" / "transcription_index.db"
            with (
                mock.patch.object(index, "TRANSCRIPTION_DB", db_path),
                mock.patch.object(
                    ytarchiver_config, "TRANSCRIPTION_DB", db_path),
            ):
                _reset_index()
                self.assertFalse(db_path.exists())

                stats = archive_scan.index_db_stats()

                self.assertEqual(stats["segments"], 0)
                self.assertEqual(stats["hours"], 0)
                self.assertEqual(stats["transcribed_videos"], 0)
                self.assertEqual(stats["total_videos"], 0)
                self.assertTrue(index._schema_inited)
                conn = index._open()
                self.assertIsNotNone(conn)
                assert conn is not None
                self.assertIsNotNone(conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name='segments'"
                ).fetchone())

    def test_single_video_lookup_streams_past_four_megabytes(self) -> None:
        with _temporary_directory() as td:
            root = Path(td)
            channel = root / "Channel"
            channel.mkdir()
            jsonl_path = channel / ".Channel Transcript.jsonl"
            txt_path = channel / "Channel Transcript.txt"
            target_id = "late1234567"
            target_title = "Late Video"
            filler = json.dumps({
                "video_id": "fill1234567",
                "title": "Filler",
                "start": 0,
                "end": 1,
                "text": "x" * (4 * 1024 * 1024 + 1024),
            })
            target = json.dumps({
                "video_id": target_id,
                "title": target_title,
                "start": 0,
                "end": 1,
                "text": "target",
            })
            jsonl_path.write_text(
                filler + "\n" + target + "\n", encoding="utf-8")
            txt_path.write_text(
                f"===({target_title}), (01.02.2026), (0:00:01), "
                "(YT CAPTIONS)===\nbody\n\n",
                encoding="utf-8",
            )

            found = punct_restore._find_single_video(root, target_id)

            self.assertEqual(
                found,
                (jsonl_path, target_id, target_title, "YT CAPTIONS"),
            )


if __name__ == "__main__":
    unittest.main()
