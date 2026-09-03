from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-report-tests-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import (
    archive_scan,
    drift_scan,
    index,
    index_graph,
    text_utils,
    ytarchiver_config,
)
from backend.api_mixins.browse_mixin import BrowseMixin
from backend.api_mixins.media_ops_mixin import _channel_archive_path
from backend.metadata import scan as metadata_scan
from backend.metadata import thumbnails_ops as metadata_thumbnails_ops


class DriftIdentityTests(unittest.TestCase):
    @staticmethod
    def _header(title: str, video_id: str = "") -> str:
        identity = f", (youtu.be/{video_id})" if video_id else ""
        return (f"===({title}), (01.02.2024), (0:01), "
                f"(YT CAPTIONS){identity}===\nbody\n")

    def test_same_title_known_ids_remain_separate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            channel = root / "Channel"
            channel.mkdir()
            (channel / "Channel Transcript.txt").write_text(
                self._header("Shared title", "ABCDEFGHIJK")
                + self._header("Shared title", "LMNOPQRSTUV"),
                encoding="utf-8",
            )
            (channel / ".Channel Transcript.jsonl").write_text(
                json.dumps({
                    "title": "Shared title",
                    "video_id": "ABCDEFGHIJK",
                    "start": 0,
                    "end": 1,
                    "text": "body",
                }) + "\n",
                encoding="utf-8",
            )

            with mock.patch.object(drift_scan, "_count_fts_phantoms",
                                   return_value=0):
                result = drift_scan.scan_channel(
                    {"name": "Channel"}, str(root))

        self.assertTrue(result["ok"])
        self.assertEqual(result["jsonl_without_txt"], [])
        self.assertEqual(
            [row["video_id"] for row in result["txt_without_jsonl"]],
            ["LMNOPQRSTUV"],
        )
        self.assertTrue(result["txt_without_jsonl"][0]["auto_repair"])

    def test_explicit_header_id_outranks_title_bracket(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            channel = root / "Channel"
            channel.mkdir()
            (channel / "Channel Transcript.txt").write_text(
                self._header("Title [ABCDEFGHIJK]", "Zyxwvutsrqp"),
                encoding="utf-8",
            )
            (channel / ".Channel Transcript.jsonl").write_text(
                json.dumps({
                    "title": "Title [ABCDEFGHIJK]",
                    "video_id": "Zyxwvutsrqp",
                    "start": 0,
                    "end": 1,
                    "text": "body",
                }) + "\n",
                encoding="utf-8",
            )
            with mock.patch.object(drift_scan, "_count_fts_phantoms",
                                   return_value=0):
                result = drift_scan.scan_channel(
                    {"name": "Channel"}, str(root))

        self.assertEqual(result["txt_without_jsonl"], [])
        self.assertEqual(result["jsonl_without_txt"], [])

    def test_ambiguous_no_id_title_is_reported_but_never_applied(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            channel = root / "Channel"
            channel.mkdir()
            (channel / "Channel Transcript.txt").write_text(
                self._header("Ambiguous") + self._header("Ambiguous"),
                encoding="utf-8",
            )
            jsonl_path = channel / ".Channel Transcript.jsonl"
            jsonl_path.write_text(
                json.dumps({
                    "title": "Ambiguous", "start": 0, "end": 1,
                    "text": "body",
                }) + "\n",
                encoding="utf-8",
            )
            with mock.patch.object(drift_scan, "_count_fts_phantoms",
                                   return_value=0):
                result = drift_scan.scan_channel(
                    {"name": "Channel"}, str(root))

            enqueue = mock.Mock()
            with mock.patch.object(
                    drift_scan, "_write_transcript_entry_plain") as write:
                applied = drift_scan.apply_channel(
                    {"name": "Channel"}, str(root), scan_result=result,
                    enqueue_retranscribe_fn=enqueue,
                )

        self.assertEqual(len(result["txt_without_jsonl"]), 2)
        self.assertEqual(len(result["jsonl_without_txt"]), 1)
        self.assertEqual(result["totals"], {
            "txt_titles": 2,
            "jsonl_titles": 1,
        })
        self.assertTrue(all(not row["auto_repair"]
                            for row in result["txt_without_jsonl"]
                            + result["jsonl_without_txt"]))
        self.assertEqual(applied["actions"]["ambiguous_skipped"], 3)
        enqueue.assert_not_called()
        write.assert_not_called()

    def test_db_title_fallback_requires_one_logical_candidate(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE videos(
                id INTEGER PRIMARY KEY,
                title TEXT,
                channel TEXT,
                filepath TEXT,
                video_id TEXT,
                duration_s REAL,
                upload_ts REAL,
                availability TEXT,
                is_duplicate_of INTEGER
            )
        """)
        conn.executemany(
            "INSERT INTO videos(title, channel, filepath, video_id) "
            "VALUES ('Same', 'Channel', ?, ?)",
            [("A.mp4", "ABCDEFGHIJK"), ("B.mp4", "LMNOPQRSTUV")],
        )
        try:
            with mock.patch.object(index, "_reader_open", return_value=conn):
                self.assertEqual(
                    drift_scan._resolve_video_filepath("Channel", "Same"), "")
                self.assertEqual(
                    drift_scan._resolve_video_filepath(
                        "Channel", "Same", "ABCDEFGHIJK"),
                    "A.mp4",
                )
        finally:
            conn.close()

    def test_no_id_jsonl_repair_requires_one_canonical_db_id(self) -> None:
        def _catalog(video_ids: list[str]) -> sqlite3.Connection:
            conn = sqlite3.connect(":memory:")
            conn.execute("""
                CREATE TABLE videos(
                    id INTEGER PRIMARY KEY,
                    title TEXT,
                    channel TEXT,
                    filepath TEXT,
                    video_id TEXT,
                    duration_s REAL,
                    upload_ts REAL,
                    availability TEXT,
                    is_duplicate_of INTEGER
                )
            """)
            conn.executemany(
                "INSERT INTO videos(title, channel, filepath, video_id, "
                "availability) VALUES ('Same', 'Channel', ?, ?, 'available')",
                [(f"{idx}.mp4", video_id)
                 for idx, video_id in enumerate(video_ids)],
            )
            return conn

        for video_ids, expected_id in (
                ([], ""),
                (["ABCDEFGHIJK", "LMNOPQRSTUV"], ""),
                (["Zyxwvutsrqp"], "Zyxwvutsrqp"),
        ):
            with self.subTest(video_ids=video_ids), \
                    tempfile.TemporaryDirectory() as td:
                root = Path(td)
                channel = root / "Channel"
                channel.mkdir()
                (channel / ".Channel Transcript.jsonl").write_text(
                    json.dumps({
                        "title": "Same", "start": 0, "end": 1,
                        "text": "body",
                    }) + "\n",
                    encoding="utf-8",
                )
                conn = _catalog(video_ids)
                try:
                    with mock.patch.object(
                            drift_scan, "_count_fts_phantoms",
                            return_value=0), mock.patch.object(
                                index, "_reader_open", return_value=conn):
                        result = drift_scan.scan_channel(
                            {"name": "Channel"}, str(root))
                finally:
                    conn.close()

                orphan = result["jsonl_without_txt"][0]
                self.assertEqual(orphan["video_id"], expected_id)
                self.assertEqual(orphan["auto_repair"], bool(expected_id))
                if not expected_id:
                    self.assertEqual(
                        orphan["identity_warning"],
                        "no unique canonical video ID",
                    )
                    with mock.patch.object(
                            drift_scan,
                            "_write_transcript_entry_plain") as write:
                        applied = drift_scan.apply_channel(
                            {"name": "Channel"}, str(root),
                            scan_result=result,
                        )
                    write.assert_not_called()
                    self.assertEqual(
                        applied["actions"]["ambiguous_skipped"], 1)


class AllLetterVideoIdTests(unittest.TestCase):
    def test_authoritative_sidecar_and_hint_accept_all_letter_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            media = folder / "Video [LMNOPQRSTUV].mp4"
            media.write_bytes(b"video")
            media.with_suffix(".info.json").write_text(
                json.dumps({"id": "ABCDEFGHIJK"}), encoding="utf-8")
            with mock.patch.object(metadata_scan, "_channel_fingerprint",
                                   return_value=0), mock.patch.object(
                    index, "_reader_open", return_value=None), \
                    mock.patch.object(index, "_open", return_value=None):
                rows = metadata_scan._scan_channel_videos(folder)
            self.assertEqual(rows[0][0], "ABCDEFGHIJK")
            self.assertEqual(
                text_utils.extract_video_id(
                    str(media), reject_alpha_only=True,
                    info_json_fallback=True),
                "ABCDEFGHIJK",
            )
        self.assertEqual(
            text_utils.extract_video_id(
                "unlabeled.mp4", hint="ABCDEFGHIJK",
                reject_alpha_only=True),
            "ABCDEFGHIJK",
        )

    def test_unverified_all_letter_filename_label_stays_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            media = folder / "Legacy label [ABCDEFGHIJK].mp4"
            media.write_bytes(b"video")
            self.assertEqual(
                text_utils.extract_video_id(
                    str(media), reject_alpha_only=True),
                "",
            )
            # Compatibility callers that intentionally trust the filename
            # convention can still accept an all-letter ID by shape.
            self.assertEqual(
                text_utils.extract_video_id(str(media)),
                "ABCDEFGHIJK",
            )
            with mock.patch.object(metadata_scan, "_channel_fingerprint",
                                   return_value=0), mock.patch.object(
                    index, "_reader_open", return_value=None), \
                    mock.patch.object(index, "_open", return_value=None):
                rows = metadata_scan._scan_channel_videos(folder)
        self.assertEqual(rows[0][0], "")

    def test_authoritative_db_identity_outranks_ambiguous_filename_label(
            self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            path = r"X:\Archive\Channel\Video [ABCDEFGHIJK].mp4"
            conn.execute("CREATE TABLE videos(filepath TEXT, video_id TEXT)")
            conn.execute(
                "INSERT INTO videos(filepath, video_id) VALUES (?, ?)",
                (path, "Zyxwvutsrqp"),
            )
            self.assertEqual(
                text_utils.extract_video_id(path, conn=conn),
                "Zyxwvutsrqp",
            )
        finally:
            conn.close()

    def test_register_prefers_all_letter_sidecar_to_filename_label(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            media = Path(td) / "Legacy label [ABCDEFGHIJK].mp4"
            media.write_bytes(b"video")
            media.with_suffix(".info.json").write_text(
                json.dumps({"id": "Zyxwvutsrqp"}), encoding="utf-8")
            conn = sqlite3.connect(":memory:")
            conn.execute("""
                CREATE TABLE videos(
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    year INTEGER,
                    month INTEGER,
                    filepath TEXT UNIQUE COLLATE NOCASE,
                    video_id TEXT,
                    video_url TEXT,
                    duration_s REAL,
                    size_bytes INTEGER,
                    tx_status TEXT,
                    added_ts REAL,
                    upload_ts REAL,
                    availability TEXT,
                    id_backfill_fail_count INTEGER DEFAULT 0,
                    id_backfill_excluded_ts REAL
                )
            """)
            try:
                self.assertTrue(index.register_video(
                    str(media), "Channel", _conn_override=conn))
                row = conn.execute(
                    "SELECT video_id FROM videos WHERE filepath=?",
                    (str(media),),
                ).fetchone()
            finally:
                conn.close()
        self.assertEqual(row, ("Zyxwvutsrqp",))

    def test_watch_keeps_payload_id_when_filename_label_is_ambiguous(
            self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE videos(filepath TEXT, video_id TEXT)")
        conn.execute("""
            CREATE TABLE segments(
                id INTEGER PRIMARY KEY,
                video_id TEXT,
                jsonl_path TEXT,
                start_time REAL,
                end_time REAL,
                text TEXT,
                words TEXT,
                title TEXT,
                channel TEXT
            )
        """)
        conn.execute(
            "INSERT INTO segments(video_id, jsonl_path, start_time, end_time, "
            "text, words, title, channel) VALUES "
            "('Zyxwvutsrqp', 'real.jsonl', 0, 1, 'real transcript', '', "
            "'Video', 'Channel')"
        )
        try:
            with mock.patch.object(index, "_reader_open", return_value=conn):
                rows = index.get_segments(
                    video_id="Zyxwvutsrqp",
                    filepath=r"X:\Archive\Legacy [ABCDEFGHIJK].mp4",
                )
        finally:
            conn.close()
        self.assertEqual([row["t"] for row in rows], ["real transcript"])


class ReportingContractTests(unittest.TestCase):
    @staticmethod
    def _create_reporting_db(path: Path, media_paths: list[Path]) -> None:
        conn = sqlite3.connect(path)
        try:
            conn.executescript("""
                CREATE TABLE videos(
                    id INTEGER PRIMARY KEY,
                    title TEXT,
                    channel TEXT,
                    filepath TEXT,
                    video_id TEXT,
                    size_bytes INTEGER,
                    duration_s REAL,
                    tx_status TEXT,
                    downloaded_ts REAL,
                    upload_ts REAL,
                    availability TEXT,
                    is_duplicate_of INTEGER
                );
                CREATE TABLE segments(id INTEGER PRIMARY KEY);
            """)
            conn.executemany(
                "INSERT INTO videos(title, channel, filepath, video_id, "
                "size_bytes, duration_s, tx_status, downloaded_ts, "
                "availability, is_duplicate_of) "
                "VALUES ('Video', 'Channel', ?, 'ABCDEFGHIJK', ?, ?, ?, 0, "
                "'available', ?)",
                [
                    (str(media_paths[0]), 3, 3600, "transcribed", None),
                    (str(media_paths[1]), 5, 7200, "pending", 1),
                ],
            )
            conn.execute(
                "INSERT INTO videos(title, channel, filepath, video_id, "
                "size_bytes, duration_s, tx_status, availability) "
                "VALUES ('Missing', 'Channel', 'missing.mp4', "
                "'LMNOPQRSTUV', 99, 9000, 'transcribed', 'missing')"
            )
            conn.commit()
        finally:
            conn.close()

    def test_maintenance_channel_path_honors_folder_override(self) -> None:
        path = _channel_archive_path(
            r"X:\Archive",
            {"name": "Display", "folder_override": "Actual: Folder"},
        )
        self.assertEqual(path, r"X:\Archive\Actual_ Folder")

    def test_thumbnail_realign_honors_sanitized_folder_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            thumb_dir = root / "Actual_ Folder" / ".Thumbnails"
            thumb_dir.mkdir(parents=True)
            (thumb_dir / "Thumb [ABCDEFGHIJK].jpg").write_bytes(b"jpg")
            channel = {
                "name": "Display",
                "folder_override": "Actual: Folder",
            }
            with mock.patch.object(
                    metadata_thumbnails_ops, "load_config",
                    return_value={"output_dir": str(root)}), \
                    mock.patch.object(index, "_reader_open", return_value=None), \
                    mock.patch.object(index, "_open", return_value=None):
                result = metadata_thumbnails_ops.realign_misplaced_thumbnails(
                    [channel], dry_run=True)

        self.assertTrue(result["ok"])
        self.assertEqual(result["scanned"], 1)
        self.assertEqual(result["orphan_no_db"], 1)

    def test_word_cloud_response_prominently_labels_limited_sample(self) -> None:
        api = BrowseMixin()
        with mock.patch(
                "backend.api_mixins.browse_mixin.index_backend.top_words",
                return_value=[{"word": "archive", "count": 2}]):
            result = api.browse_word_cloud()

        self.assertTrue(result["ok"])
        self.assertTrue(result["sampling"]["limited"])
        self.assertEqual(
            result["sampling"]["limit"], index_graph.TOP_WORDS_SAMPLE_LIMIT)
        self.assertIn("oldest 500,000", result["sampling"]["label"])

    def test_db_and_filesystem_reports_agree_on_logical_copies_and_bytes(
            self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            channel_dir = root / "Channel"
            channel_dir.mkdir()
            media = [channel_dir / "A.mp4", channel_dir / "B.mp4"]
            media[0].write_bytes(b"aaa")
            media[1].write_bytes(b"bbbbb")
            db_path = root / "index.db"
            self._create_reporting_db(db_path, media)
            conn = sqlite3.connect(db_path)
            cache_path = root / "disk-cache.json"
            channel = {"name": "Channel", "url": "channel://test"}
            try:
                with mock.patch.object(index, "_reader_open",
                                       return_value=conn), mock.patch.object(
                        archive_scan, "DISK_CACHE_FILE", cache_path), \
                        mock.patch.object(
                            archive_scan, "load_config",
                            return_value={"output_dir": str(root)}):
                    db_result = archive_scan.update_disk_cache_for_channel(
                        channel)
                    fs_result = archive_scan.update_disk_cache_for_channel(
                        channel, force_filesystem=True)
            finally:
                conn.close()

        expected = {"n_vids": 1, "physical_copies": 2, "size_bytes": 8}
        self.assertEqual(
            {key: db_result[key] for key in expected}, expected)
        self.assertEqual(
            {key: fs_result[key] for key in expected}, expected)

    def test_duration_and_video_count_share_canonical_eligibility(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            media = [root / "A.mp4", root / "B.mp4"]
            db_path = root / "index.db"
            self._create_reporting_db(db_path, media)
            with mock.patch.object(
                    ytarchiver_config, "TRANSCRIPTION_DB", db_path), \
                    mock.patch.object(index, "_schema_inited", True):
                result = archive_scan.index_db_stats()

        self.assertEqual(result["total_videos"], 1)
        self.assertEqual(result["transcribed_videos"], 1)
        self.assertEqual(result["hours"], 1.0)

    def test_graph_year_uses_one_canonical_upload_date(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.executescript("""
            CREATE TABLE videos(
                id INTEGER PRIMARY KEY,
                title TEXT,
                channel TEXT,
                filepath TEXT,
                video_id TEXT,
                duration_s REAL,
                upload_ts REAL,
                availability TEXT,
                is_duplicate_of INTEGER
            );
            CREATE TABLE segments(
                id INTEGER PRIMARY KEY,
                video_id TEXT,
                channel TEXT,
                year INTEGER,
                month INTEGER
            );
            CREATE INDEX idx_seg_video_id ON segments(video_id);
        """)
        conn.executemany(
            "INSERT INTO videos(title, channel, filepath, video_id, "
            "upload_ts, availability, is_duplicate_of) "
            "VALUES ('Video', 'Channel', ?, 'ABCDEFGHIJK', 1720000000, "
            "'available', ?)",
            [("A.mp4", None), ("B.mp4", 1)],
        )
        conn.execute(
            "INSERT INTO segments(video_id, channel, year, month) "
            "VALUES ('ABCDEFGHIJK', 'Channel', 1999, 1)"
        )
        try:
            with mock.patch.object(index, "_reader_open", return_value=conn):
                result = index_graph.bucket_totals("year")
        finally:
            conn.close()

        self.assertEqual(result, {"2024": 1})


if __name__ == "__main__":
    unittest.main()
