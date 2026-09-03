from __future__ import annotations

# APPDATA must be redirected before backend imports in this standalone test.
# ruff: noqa: E402
import atexit
import os
import tempfile
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-fts-drift-")
atexit.register(_TEST_APPDATA.cleanup)
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import drift_scan, index, index_maintenance
from backend.api_mixins import media_ops_mixin


def _reset_index() -> None:
    index._shutdown_index()
    index._conn = None
    index._reader_conn = None
    index._schema_inited = False
    index._ingest_locks.clear()


@contextmanager
def _disposable_index():
    with tempfile.TemporaryDirectory(prefix="ytarchiver-fts-drift-db-") as td:
        root = Path(td)
        with mock.patch.object(index, "TRANSCRIPTION_DB", root / "index.db"):
            _reset_index()
            conn = index._open()
            assert conn is not None
            try:
                yield root, conn
            finally:
                _reset_index()


class _RebuildExitGate:
    """Release a waiting writer exactly when rebuild leaves its DB lock."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.main_ident = threading.get_ident()
        self.writer_may_start = threading.Event()
        self.writer_inserted = threading.Event()
        self.writer_may_commit = threading.Event()
        self._armed = True

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self._lock.release()
        if self._armed and threading.get_ident() == self.main_ident:
            self._armed = False
            self.writer_may_start.set()
            if not self.writer_inserted.wait(5):
                raise AssertionError("concurrent writer did not start")


class Patch3FtsDriftTests(unittest.TestCase):
    def test_drift_detects_recycled_tokens_and_rebuilds_both_fts(self) -> None:
        with _disposable_index() as (root, conn):
            old_video_path = str(root / "old.mp4")
            new_video_path = str(root / "new.mp4")
            jsonl_path = root / "new.jsonl"
            jsonl_path.write_text(
                '{"video_id":"newvideo001","start":0,"end":1,'
                '"text":"newsegmenttoken"}\n',
                encoding="utf-8",
            )

            old_video_row = conn.execute(
                "INSERT INTO videos(title, channel, filepath, video_id) "
                "VALUES('oldtitletoken', 'Channel', ?, 'oldvideo001')",
                (old_video_path,),
            ).lastrowid
            old_segment_row = conn.execute(
                "INSERT INTO segments(video_id, title, channel, text) "
                "VALUES('oldvideo001', 'Old', 'Channel', 'oldsegmenttoken')"
            ).lastrowid
            conn.commit()

            for trigger in (
                    "segments_fts_ai_v4", "segments_fts_ad_v4",
                    "videos_fts_ai_v4", "videos_fts_ad_v4"):
                conn.execute(f"DROP TRIGGER {trigger}")
            conn.execute("DELETE FROM segments WHERE id=?", (old_segment_row,))
            conn.execute("DELETE FROM videos WHERE id=?", (old_video_row,))
            new_video_row = conn.execute(
                "INSERT INTO videos(title, channel, filepath, video_id) "
                "VALUES('newtitletoken', 'Channel', ?, 'newvideo001')",
                (new_video_path,),
            ).lastrowid
            new_segment_row = conn.execute(
                "INSERT INTO segments(video_id, title, channel, text, "
                "jsonl_path) VALUES('newvideo001', 'New', 'Channel', "
                "'newsegmenttoken', ?)",
                (str(jsonl_path),),
            ).lastrowid
            conn.execute(
                "INSERT INTO indexed_files(path, mtime, segment_count) "
                "VALUES('sentinel.jsonl', 1, 99)"
            )
            conn.commit()
            self.assertEqual(new_video_row, old_video_row)
            self.assertEqual(new_segment_row, old_segment_row)

            self.assertGreater(drift_scan._count_fts_phantoms() or 0, 0)
            self.assertTrue(drift_scan.rebuild_fts_index())
            self.assertEqual(drift_scan._count_fts_phantoms(), 0)
            self.assertTrue(index_maintenance.fts_health_check()["ok"])
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos_fts "
                "WHERE videos_fts MATCH 'oldtitletoken'"
            ).fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'oldsegmenttoken'"
            ).fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos_fts "
                "WHERE videos_fts MATCH 'newtitletoken'"
            ).fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'newsegmenttoken'"
            ).fetchone()[0], 1)
            indexed = conn.execute(
                "SELECT path, mtime, segment_count FROM indexed_files"
            ).fetchall()
            # This path had source segments but no prior ingest tracker. A
            # rebuild from DB rows cannot certify the file's current mtime, so
            # zero deliberately forces the next sweep to ingest it.
            self.assertEqual(indexed, [(str(jsonl_path), 0.0, 1)])

    def test_rebuild_preserves_changed_jsonl_as_needing_ingest(self) -> None:
        with _disposable_index() as (root, conn):
            media_path = root / "Video [newvideo001].mp4"
            jsonl_path = root / "Video [newvideo001].jsonl"
            media_path.write_bytes(b"media")
            jsonl_path.write_text(
                '{"video_id":"newvideo001","start":0,"end":1,'
                '"text":"oldtoken"}\n',
                encoding="utf-8",
            )
            self.assertEqual(index.ingest_jsonl(
                str(media_path), str(jsonl_path), "Video", "Channel",
                force=True,
            ), 1)
            ingested_mtime = conn.execute(
                "SELECT mtime FROM indexed_files WHERE path=?",
                (str(jsonl_path),),
            ).fetchone()[0]

            jsonl_path.write_text(
                '{"video_id":"newvideo001","start":0,"end":1,'
                '"text":"newtoken"}\n',
                encoding="utf-8",
            )
            os.utime(jsonl_path, (ingested_mtime + 10, ingested_mtime + 10))
            self.assertTrue(index_maintenance._jsonl_needs_ingest(
                conn, str(jsonl_path)))

            result = index_maintenance.rebuild_fts_index()

            self.assertTrue(result["ok"])
            tracked_mtime = conn.execute(
                "SELECT mtime FROM indexed_files WHERE path=?",
                (str(jsonl_path),),
            ).fetchone()[0]
            self.assertEqual(tracked_mtime, ingested_mtime)
            self.assertTrue(index_maintenance._jsonl_needs_ingest(
                conn, str(jsonl_path)))
            self.assertEqual(conn.execute(
                "SELECT text FROM segments"
            ).fetchone(), ("oldtoken",))
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'newtoken'"
            ).fetchone()[0], 0)

    def test_rebuild_normalizes_null_tracker_mtime_to_unknown(self) -> None:
        with _disposable_index() as (root, conn):
            jsonl_path = root / "legacy.jsonl"
            conn.execute(
                "INSERT INTO segments(video_id, title, channel, text, "
                "jsonl_path) VALUES('legacyvid01', 'Legacy', 'Channel', "
                "'legacytoken', ?)",
                (str(jsonl_path),),
            )
            conn.execute(
                "INSERT INTO indexed_files(path, mtime, segment_count) "
                "VALUES(?, NULL, 1)",
                (str(jsonl_path),),
            )
            conn.commit()

            result = index_maintenance.rebuild_fts_index()

            self.assertTrue(result["ok"])
            self.assertEqual(conn.execute(
                "SELECT mtime, segment_count FROM indexed_files WHERE path=?",
                (str(jsonl_path),),
            ).fetchone(), (0.0, 1))

    def test_rebuild_does_not_commit_a_shared_outer_transaction(self) -> None:
        with _disposable_index() as (_root, conn):
            conn.execute(
                "INSERT INTO app_state(key, value) VALUES('outer', 'pending')")
            self.assertTrue(conn.in_transaction)

            result = index_maintenance.rebuild_fts_index()

            self.assertFalse(result["ok"])
            self.assertIn("active transaction", result["error"])
            self.assertTrue(conn.in_transaction)
            conn.rollback()
            self.assertIsNone(conn.execute(
                "SELECT value FROM app_state WHERE key='outer'"
            ).fetchone())

    def test_failed_rebuild_cannot_rollback_concurrent_writer(self) -> None:
        with _disposable_index() as (root, conn):
            jsonl_path = root / "transcript.jsonl"
            jsonl_path.write_text("{}\n", encoding="utf-8")
            conn.execute(
                "INSERT INTO segments(video_id, title, channel, text, "
                "jsonl_path) VALUES('racevideo01', 'Race', 'Channel', "
                "'racetoken', ?)",
                (str(jsonl_path),),
            )
            conn.execute("""
                CREATE TRIGGER block_indexed_files_insert
                BEFORE INSERT ON indexed_files BEGIN
                    SELECT RAISE(FAIL, 'blocked indexed_files insert');
                END
            """)
            conn.commit()

            gate = _RebuildExitGate()
            writer_errors: list[BaseException] = []

            def _writer() -> None:
                try:
                    if not gate.writer_may_start.wait(5):
                        raise AssertionError("rebuild did not release writer")
                    with gate:
                        conn.execute(
                            "INSERT INTO app_state(key, value) "
                            "VALUES('concurrent', 'committed')"
                        )
                        gate.writer_inserted.set()
                        if not gate.writer_may_commit.wait(5):
                            raise AssertionError("writer commit was not released")
                        conn.commit()
                except BaseException as exc:  # recorded for the test thread
                    writer_errors.append(exc)
                    gate.writer_inserted.set()

            writer = threading.Thread(target=_writer, name="fts-race-writer")
            writer.start()
            try:
                with mock.patch.object(index, "_db_lock", gate), \
                        mock.patch.object(index, "_open", return_value=conn):
                    result = index_maintenance.rebuild_fts_index()
            finally:
                gate.writer_may_commit.set()
                writer.join(5)

            self.assertFalse(writer.is_alive())
            self.assertEqual(writer_errors, [])
            self.assertFalse(result["ok"])
            self.assertIn("blocked indexed_files", result["error"])
            self.assertEqual(conn.execute(
                "SELECT value FROM app_state WHERE key='concurrent'"
            ).fetchone(), ("committed",))

    def test_drift_apply_does_not_claim_a_failed_rebuild(self) -> None:
        scan = {
            "ok": True,
            "txt_without_jsonl": [],
            "jsonl_without_txt": [],
            "fts_phantoms": 1,
        }
        result = drift_scan.apply_channel(
            {}, "", scan_result=scan,
            rebuild_fts_fn=lambda: {"ok": False, "error": "failed"},
        )
        self.assertFalse(result["ok"])
        self.assertTrue(result["partial"])
        self.assertIn("Search index rebuild failed: failed", result["errors"])
        self.assertFalse(result["actions"]["fts_rebuilt"])

    def test_drift_apply_counts_only_confirmed_retranscribe_enqueue(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ytarchiver-drift-enqueue-") as td:
            media_path = Path(td) / "video.mp4"
            media_path.write_bytes(b"media")
            scan = {
                "ok": True,
                "txt_without_jsonl": [{
                    "title": "Video",
                    "video_id": "newvideo001",
                    "auto_repair": True,
                }],
                "jsonl_without_txt": [],
                "fts_health_issues": 0,
            }
            with mock.patch.object(
                    drift_scan, "_resolve_video_filepath",
                    return_value=str(media_path)):
                rejected = drift_scan.apply_channel(
                    {"name": "Channel"}, td, scan_result=scan,
                    enqueue_retranscribe_fn=lambda *_: {
                        "ok": False, "error": "queue full"},
                )
                accepted = drift_scan.apply_channel(
                    {"name": "Channel"}, td, scan_result=scan,
                    enqueue_retranscribe_fn=lambda *_: {"ok": True},
                )

        self.assertFalse(rejected["ok"])
        self.assertTrue(rejected["partial"])
        self.assertEqual(rejected["actions"]["retranscribe_queued"], 0)
        self.assertEqual(rejected["actions"]["retranscribe_failed"], 1)
        self.assertIn("queue full", rejected["errors"][0])
        self.assertTrue(accepted["ok"])
        self.assertFalse(accepted["partial"])
        self.assertEqual(accepted["actions"]["retranscribe_queued"], 1)
        self.assertEqual(accepted["actions"]["retranscribe_failed"], 0)

    def test_media_drift_callback_forwards_backend_rejection(self) -> None:
        backend_result = {"ok": False, "error": "journal unavailable"}
        forwarded: list[dict] = []

        class _ImmediateThread:
            def __init__(self, target, **_kwargs):
                self._target = target

            def start(self):
                self._target()

        class _Api(media_ops_mixin.MediaOpsMixin):
            def __init__(self, output_dir: str):
                self._config = {"output_dir": output_dir}
                self._log_stream = mock.Mock()

            def transcribe_retranscribe(self, *_args):
                return backend_result

        def _apply(_channel, _output_dir, *, enqueue_retranscribe_fn,
                   rebuild_fts_fn):
            self.assertTrue(callable(rebuild_fts_fn))
            forwarded.append(enqueue_retranscribe_fn(
                "video.mp4", "Video", "newvideo001"))
            return {"ok": False, "partial": True,
                    "error": "queue rejected", "errors": ["queue rejected"]}

        with tempfile.TemporaryDirectory(prefix="ytarchiver-drift-media-") as td, \
                mock.patch.object(
                    media_ops_mixin.subs_backend, "get_channel",
                    return_value={"name": "Channel"}), \
                mock.patch.object(drift_scan, "apply_channel", side_effect=_apply), \
                mock.patch.object(
                    media_ops_mixin.threading, "Thread", _ImmediateThread):
            api = _Api(td)
            started = api.drift_apply_channel({"name": "Channel"})
            result = api.drift_apply_channel_poll(started["token"])

        self.assertEqual(forwarded, [backend_result])
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "queue rejected")

    def test_unavailable_fts_health_is_explicit_partial_failure(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ytarchiver-drift-health-") as td:
            channel_folder = Path(td) / "Channel"
            channel_folder.mkdir()
            with mock.patch.object(
                    drift_scan, "_count_fts_phantoms", return_value=None):
                result = drift_scan.scan_channel(
                    {"name": "Channel"}, td)
            rebuild = mock.Mock(return_value={"ok": True})
            applied = drift_scan.apply_channel(
                {"name": "Channel"}, td, scan_result=result,
                rebuild_fts_fn=rebuild,
            )

        self.assertFalse(result["ok"])
        self.assertTrue(result["partial"])
        self.assertIsNone(result["fts_health_issues"])
        self.assertEqual(result["fts_phantoms_error"], "unavailable")
        self.assertIn("could not be verified", result["error"])
        self.assertFalse(applied["ok"])
        rebuild.assert_not_called()


if __name__ == "__main__":
    unittest.main()
