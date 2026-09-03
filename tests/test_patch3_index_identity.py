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
from types import SimpleNamespace
from unittest import mock


_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch3-tests-")
atexit.register(_TEST_APPDATA.cleanup)
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import archive_scan, index, index_graph, index_maintenance, subs
from backend.api_mixins.browse_mixin import BrowseMixin
from backend.api_mixins.recent_mixin import RecentMixin
from backend.api_mixins.video_mixin import VideoMixin
from backend.api_mixins.subs_mixin import SubsMixin
from backend.services import file_ops


def _reset_index() -> None:
    index._shutdown_index()
    index._conn = None
    index._reader_conn = None
    index._schema_inited = False
    index._ingest_locks.clear()
    index._browse_videos_cache.clear()
    index._all_videos_cache.clear()
    index_graph.invalidate_top_words_cache()


@contextmanager
def _disposable_index():
    with tempfile.TemporaryDirectory(prefix="ytarchiver-patch3-db-") as td:
        db_path = Path(td) / "transcription_index.db"
        with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
            _reset_index()
            conn = index._open()
            assert conn is not None
            try:
                yield Path(td), conn
            finally:
                _reset_index()


def _insert_video(
        conn, *, title: str, filepath: str | None, video_id: str | None,
        channel: str = "Channel", upload_ts: float | None = None,
        availability: str | None = "available",
        is_duplicate_of: str | None = None,
        tx_status: str = "transcribed",
        duration_s: float | None = None) -> int:
    cur = conn.execute(
        "INSERT INTO videos(title, channel, filepath, video_id, upload_ts, "
        "availability, is_duplicate_of, tx_status, duration_s, added_ts) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
        (title, channel, filepath, video_id, upload_ts, availability,
         is_duplicate_of, tx_status, duration_s))
    return int(cur.lastrowid)


def _insert_segment(conn, *, video_id: str, text: str,
                    channel: str = "Channel") -> int:
    cur = conn.execute(
        "INSERT INTO segments(video_id, title, channel, year, month, "
        "start_time, end_time, text, jsonl_path) "
        "VALUES(?, 'Transcript', ?, 2024, 1, 0, 1, ?, 'aggregate.jsonl')",
        (video_id, channel, text))
    return int(cur.lastrowid)


class Patch3FtsLifecycleTests(unittest.TestCase):
    def test_source_triggers_cover_insert_update_delete_and_rowid_reuse(self) -> None:
        with _disposable_index() as (root, conn):
            old_video = str(root / "old.mp4")
            new_video = str(root / "new.mp4")
            old_vid_row = _insert_video(
                conn, title="Oldtitletoken", filepath=old_video,
                video_id="oldvideo001")
            old_seg_row = _insert_segment(
                conn, video_id="oldvideo001", text="oldsegmenttoken")
            conn.commit()

            conn.execute(
                "UPDATE videos SET title='Updatedtitletoken' WHERE id=?",
                (old_vid_row,))
            conn.execute(
                "UPDATE segments SET text='updatedsegmenttoken' WHERE id=?",
                (old_seg_row,))
            conn.commit()
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos_fts "
                "WHERE videos_fts MATCH 'oldtitletoken'").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos_fts "
                "WHERE videos_fts MATCH 'updatedtitletoken'").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'oldsegmenttoken'").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'updatedsegmenttoken'").fetchone()[0], 1)

            conn.execute("DELETE FROM segments WHERE id=?", (old_seg_row,))
            conn.execute("DELETE FROM videos WHERE id=?", (old_vid_row,))
            new_vid_row = _insert_video(
                conn, title="Newtitletoken", filepath=new_video,
                video_id="newvideo001")
            new_seg_row = _insert_segment(
                conn, video_id="newvideo001", text="newsegmenttoken")
            conn.commit()

            # INTEGER PRIMARY KEY reuses the highest deleted id when the
            # table became empty.  Old tokens must not alias the new rows.
            self.assertEqual(new_vid_row, old_vid_row)
            self.assertEqual(new_seg_row, old_seg_row)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos_fts "
                "WHERE videos_fts MATCH 'updatedtitletoken'").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'updatedsegmenttoken'").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos_fts "
                "WHERE videos_fts MATCH 'newtitletoken'").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'newsegmenttoken'").fetchone()[0], 1)
            self.assertTrue(index_maintenance.fts_health_check()["ok"])

    def test_health_sees_stale_tokens_and_dual_rebuild_repairs_both(self) -> None:
        with _disposable_index() as (root, conn):
            old_path = str(root / "old.mp4")
            new_path = str(root / "new.mp4")
            video_row = _insert_video(
                conn, title="Ghosttitletoken", filepath=old_path,
                video_id="ghostvid001")
            segment_row = _insert_segment(
                conn, video_id="ghostvid001", text="ghostsegmenttoken")
            conn.commit()

            # Simulate the pre-v4 failure: source rows change while delete and
            # insert hooks are absent, then SQLite recycles both rowids.
            for trigger in (
                    "segments_fts_ai_v4", "segments_fts_ad_v4",
                    "videos_fts_ai_v4", "videos_fts_ad_v4"):
                conn.execute(f"DROP TRIGGER {trigger}")
            conn.execute("DELETE FROM segments WHERE id=?", (segment_row,))
            conn.execute("DELETE FROM videos WHERE id=?", (video_row,))
            new_video_row = _insert_video(
                conn, title="Healthytitle", filepath=new_path,
                video_id="healthy0001")
            new_segment_row = _insert_segment(
                conn, video_id="healthy0001", text="healthysegment")
            conn.commit()
            self.assertEqual(new_video_row, video_row)
            self.assertEqual(new_segment_row, segment_row)

            health = index_maintenance.fts_health_check()
            self.assertFalse(health["ok"])
            self.assertFalse(health["indexes"]["segments_fts"]["ok"])
            self.assertFalse(health["indexes"]["videos_fts"]["ok"])

            rebuilt = index_maintenance.rebuild_fts_index()
            self.assertEqual(rebuilt, {
                "ok": True,
                "rows_indexed": 1,
                "video_rows_indexed": 1,
            })
            self.assertTrue(index_maintenance.fts_health_check()["ok"])
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'ghostsegmenttoken'").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos_fts "
                "WHERE videos_fts MATCH 'ghosttitletoken'").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'healthysegment'").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos_fts "
                "WHERE videos_fts MATCH 'healthytitle'").fetchone()[0], 1)

    def test_indexed_files_failure_rolls_back_entire_fts_rebuild(self) -> None:
        with _disposable_index() as (root, conn):
            jsonl_path = str(root / "transcript.jsonl")
            _insert_video(
                conn, title="Rollbacktitle", filepath=str(root / "video.mp4"),
                video_id="rollback001")
            seg_id = _insert_segment(
                conn, video_id="rollback001", text="rollbacktoken")
            conn.execute(
                "UPDATE segments SET jsonl_path=? WHERE id=?",
                (jsonl_path, seg_id))
            conn.execute(
                "INSERT INTO indexed_files(path, mtime, segment_count) "
                "VALUES('sentinel.jsonl', 1, 7)")
            conn.execute("""
                CREATE TRIGGER block_indexed_files_insert
                BEFORE INSERT ON indexed_files BEGIN
                    SELECT RAISE(FAIL, 'blocked indexed_files insert');
                END
            """)
            conn.commit()

            result = index_maintenance.rebuild_fts_index()

            self.assertFalse(result["ok"])
            self.assertIn("blocked indexed_files", result["error"])
            self.assertEqual(conn.execute(
                "SELECT path, segment_count FROM indexed_files"
            ).fetchall(), [("sentinel.jsonl", 7)])
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'rollbacktoken'"
            ).fetchone()[0], 1)


class Patch3LogicalVideoTests(unittest.TestCase):
    def test_missing_channel_folder_does_not_block_subscription_removal(self) -> None:
        """A stale catalog must not strand a sub after its folder moved."""
        with _disposable_index() as (_db_root, conn), \
                tempfile.TemporaryDirectory(
                    prefix="ytarchiver-missing-channel-") as archive_td:
            archive = Path(archive_td)
            missing_folder = archive / "Channel A"
            missing_media = missing_folder / "primary.mp4"
            _insert_video(
                conn, title="Primary", filepath=str(missing_media),
                video_id="missingch001", channel="Channel A")
            _insert_segment(
                conn, video_id="missingch001", text="must remain",
                channel="Channel A")
            conn.commit()

            channel = {
                "name": "Channel A",
                "folder": "Channel A",
                "url": "https://example.invalid/channel-a",
            }
            persisted = {
                "output_dir": str(archive),
                "channels": [channel],
            }

            @contextmanager
            def transaction():
                yield persisted

            mixin = SubsMixin()
            remove_queued = mock.Mock()
            mixin._queues = SimpleNamespace(
                current_sync=None,
                sync_remove_all_for_target=remove_queued,
            )
            mixin._reload_config = mock.Mock()
            mixin._on_queue_changed = mock.Mock()
            with mock.patch.object(
                    subs, "load_config", return_value=persisted), \
                    mock.patch.object(
                        subs, "config_transaction", transaction), \
                    mock.patch(
                        "backend.api_mixins.subs_mixin.load_config",
                        return_value=persisted), \
                    mock.patch.object(
                        index, "prepare_channel_copy_deletion",
                        return_value={
                            "ok": False,
                            "error": "simulated transcript safety failure",
                        }):
                result = mixin.subs_remove_channel(
                    {"url": channel["url"]}, delete_files=True)

            self.assertTrue(result["subscription_removed"])
            self.assertFalse(result["files_removed"])
            self.assertFalse(result["catalog_cleanup_ok"])
            self.assertNotIn(
                "simulated transcript safety failure",
                result["catalog_warning"])
            self.assertIn("already missing", result["catalog_warning"])
            self.assertEqual(persisted["channels"], [])
            remove_queued.assert_called_once_with(channel["url"])

            # The failed handoff is handled conservatively: keep all catalog
            # and transcript rows until the normal index rebuild can repair
            # them, while Browse immediately follows the fresh config.
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos WHERE video_id='missingch001'"
            ).fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT text FROM segments WHERE video_id='missingch001'"
            ).fetchone()[0], "must remain")
            browse = BrowseMixin()
            browse._browse_fresh_config = lambda: persisted
            with mock.patch.object(
                    archive_scan, "load_disk_cache", return_value={}):
                self.assertEqual(browse._browse_list_channels_impl(), [])

    def test_live_channel_preflight_error_is_plain_english(self) -> None:
        with tempfile.TemporaryDirectory(
                prefix="ytarchiver-live-preflight-") as archive_td:
            archive = Path(archive_td)
            folder = archive / "Channel A"
            folder.mkdir()
            channel = {
                "name": "Channel A",
                "folder": "Channel A",
                "url": "https://example.invalid/channel-a",
            }
            mixin = SubsMixin()
            mixin._queues = SimpleNamespace(current_sync=None)
            with mock.patch(
                    "backend.api_mixins.subs_mixin.load_config",
                    return_value={"output_dir": str(archive)}), \
                    mock.patch(
                        "backend.api_mixins.subs_mixin.subs_backend.get_channel",
                        return_value=channel), \
                    mock.patch.object(
                        index, "prepare_channel_copy_deletion",
                        return_value={
                            "ok": False,
                            "error": "Sidecar handoff is outside the archive.",
                        }), \
                    mock.patch(
                        "backend.api_mixins.subs_mixin.subs_backend.remove_channel"
                    ) as remove:
                result = mixin.subs_remove_channel(
                    {"url": channel["url"]}, delete_files=True)

            self.assertFalse(result["ok"])
            self.assertFalse(result["subscription_removed"])
            self.assertEqual(
                result["error_code"], "transcript_preservation_failed")
            self.assertIn("Nothing was changed", result["error"])
            self.assertNotIn("Sidecar handoff", result["error"])
            remove.assert_not_called()

    def test_channel_delete_preserves_registered_custom_survivor(self) -> None:
        """A manual Save-to copy is exact-authorized, not treated as unsafe."""
        with _disposable_index() as (_db_root, conn), \
                tempfile.TemporaryDirectory(
                    prefix="ytarchiver-channel-root-") as archive_td, \
                tempfile.TemporaryDirectory(
                    prefix="ytarchiver-custom-save-to-") as custom_td:
            archive = Path(archive_td)
            folder = archive / "Channel A"
            folder.mkdir()
            primary = folder / "primary.mp4"
            primary.write_bytes(b"primary")
            custom = Path(custom_td) / "manual survivor.mp4"
            custom.write_bytes(b"survivor")
            primary_id = _insert_video(
                conn, title="Channel copy", filepath=str(primary),
                video_id="customsv001", channel="Channel A")
            custom_id = _insert_video(
                conn, title="Manual survivor", filepath=str(custom),
                video_id="customsv001", channel="Channel A",
                is_duplicate_of=str(primary))
            _insert_segment(
                conn, video_id="customsv001", text="preserve this",
                channel="Channel A")
            conn.commit()

            config = {"output_dir": str(archive)}
            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value=config):
                prepared = index.prepare_channel_copy_deletion(
                    ["Channel A"], folder_paths=[str(folder)])

            self.assertTrue(prepared["ok"], prepared.get("error"))
            self.assertEqual(prepared["row_ids"], [primary_id])
            self.assertNotIn(custom_id, prepared["row_ids"])
            custom_jsonl = custom.with_suffix(".jsonl")
            self.assertTrue(custom_jsonl.is_file())
            self.assertEqual(
                json.loads(custom_jsonl.read_text(encoding="utf-8"))["text"],
                "preserve this")
            self.assertEqual(list(folder.glob(".*.derive-*")), [])
            self.assertEqual(list(Path(custom_td).glob(".*.derive-*")), [])

            trashed = archive / ".YTArchiver Trash" / "channel-a"
            trashed.parent.mkdir()
            folder.rename(trashed)
            deleted = index.delete_media_copy_rows(
                prepared["row_ids"], prepared=prepared)
            self.assertTrue(deleted["ok"], deleted.get("error"))
            index.finalize_copy_deletion_preparation(prepared)

            self.assertTrue(custom.is_file())
            self.assertEqual(conn.execute(
                "SELECT id, is_duplicate_of FROM videos "
                "WHERE video_id='customsv001'"
            ).fetchone(), (custom_id, None))
            self.assertEqual(conn.execute(
                "SELECT title, channel, jsonl_path FROM segments "
                "WHERE video_id='customsv001'"
            ).fetchone(), (
                "Manual survivor", "Channel A", str(custom_jsonl)))

    def test_unregistered_external_sidecar_destination_is_rejected(self) -> None:
        with _disposable_index(), \
                tempfile.TemporaryDirectory(
                    prefix="ytarchiver-sidecar-source-") as archive_td, \
                tempfile.TemporaryDirectory(
                    prefix="ytarchiver-unregistered-dest-") as outside_td:
            archive = Path(archive_td)
            source = archive / "source.jsonl"
            source.write_text('{"text":"keep"}\n', encoding="utf-8")
            arbitrary_video = Path(outside_td) / "not registered.mp4"
            arbitrary_video.write_bytes(b"video")
            destination = arbitrary_video.with_suffix(".jsonl")

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(archive)}):
                result = file_ops.preserve_sidecar_no_overwrite(
                    str(source), str(destination),
                    registered_destination_identity={
                        "id": 999999,
                        "filepath": str(arbitrary_video),
                        "video_id": "notreg00001",
                        "channel": "Outside",
                        "title": "Not registered",
                    })

            self.assertFalse(result["ok"])
            self.assertIn("registered download", result["error"])
            self.assertFalse(destination.exists())
            self.assertEqual(
                sorted(path.name for path in Path(outside_td).iterdir()),
                [arbitrary_video.name])

    def test_external_survivor_identity_is_rechecked_before_publish(self) -> None:
        with _disposable_index() as (_db_root, conn), \
                tempfile.TemporaryDirectory(
                    prefix="ytarchiver-race-source-") as archive_td, \
                tempfile.TemporaryDirectory(
                    prefix="ytarchiver-race-survivor-") as custom_td:
            archive = Path(archive_td)
            primary = archive / "primary.mp4"
            primary.write_bytes(b"primary")
            survivor = Path(custom_td) / "survivor.mp4"
            survivor.write_bytes(b"survivor")
            _insert_video(
                conn, title="Primary", filepath=str(primary),
                video_id="identity001", channel="Channel A")
            survivor_id = _insert_video(
                conn, title="Survivor", filepath=str(survivor),
                video_id="identity001", channel="Manual",
                is_duplicate_of=str(primary))
            _insert_segment(
                conn, video_id="identity001", text="do not misapply")
            conn.commit()
            real_write = index._write_derived_jsonl_source

            def replace_catalog_identity(*args, **kwargs):
                staged = real_write(*args, **kwargs)
                conn.execute(
                    "UPDATE videos SET video_id='different01' WHERE id=?",
                    (survivor_id,))
                conn.commit()
                return staged

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(archive)}), \
                    mock.patch.object(
                        index, "_write_derived_jsonl_source",
                        side_effect=replace_catalog_identity):
                result = index.prepare_media_copy_deletion(str(primary))

            self.assertFalse(result["ok"])
            self.assertIn("registered download", result["error"])
            self.assertFalse(survivor.with_suffix(".jsonl").exists())
            self.assertEqual(list(archive.glob(".*.derive-*")), [])
            self.assertEqual(list(Path(custom_td).glob(".*.handoff-*")), [])

    def test_external_survivor_identity_is_rechecked_after_copy(self) -> None:
        """A path reused during staging must not receive the old transcript."""
        with _disposable_index() as (_db_root, conn), \
                tempfile.TemporaryDirectory(
                    prefix="ytarchiver-publish-source-") as archive_td, \
                tempfile.TemporaryDirectory(
                    prefix="ytarchiver-publish-survivor-") as custom_td:
            archive = Path(archive_td)
            primary = archive / "primary.mp4"
            primary.write_bytes(b"primary")
            survivor = Path(custom_td) / "survivor.mp4"
            survivor.write_bytes(b"survivor")
            primary_id = _insert_video(
                conn, title="Primary", filepath=str(primary),
                video_id="publishid01", channel="Channel A")
            survivor_id = _insert_video(
                conn, title="Survivor", filepath=str(survivor),
                video_id="publishid01", channel="Manual",
                is_duplicate_of=str(primary))
            conn.commit()
            source = archive / (
                ".primary.jsonl.derive-00000000000000000000000000000000")
            source.write_text('{"text":"do not misapply"}\n', encoding="utf-8")
            destination = survivor.with_suffix(".jsonl")
            source_identity = {
                "id": primary_id,
                "filepath": str(primary),
                "video_id": "publishid01",
                "channel": "Channel A",
                "title": "Primary",
            }
            destination_identity = {
                "id": survivor_id,
                "filepath": str(survivor),
                "video_id": "publishid01",
                "channel": "Manual",
                "title": "Survivor",
            }
            real_copy = file_ops.shutil.copyfileobj

            def replace_identity_after_copy(*args, **kwargs):
                real_copy(*args, **kwargs)
                conn.execute(
                    "UPDATE videos SET video_id='replacement1' WHERE id=?",
                    (survivor_id,))
                conn.commit()

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(archive)}), \
                    mock.patch.object(
                        file_ops.shutil, "copyfileobj",
                        side_effect=replace_identity_after_copy):
                result = file_ops.preserve_sidecar_no_overwrite(
                    str(source), str(destination),
                    registered_destination_identity=destination_identity,
                    registered_source_identity=source_identity)

            self.assertFalse(result["ok"])
            self.assertIn("registered download", result["error"])
            self.assertFalse(destination.exists())
            self.assertEqual(
                sorted(path.name for path in Path(custom_td).iterdir()),
                [survivor.name])

    def test_channel_catalog_commit_rechecks_survivor_identity(self) -> None:
        with _disposable_index() as (_db_root, conn), \
                tempfile.TemporaryDirectory(
                    prefix="ytarchiver-commit-source-") as archive_td, \
                tempfile.TemporaryDirectory(
                    prefix="ytarchiver-commit-survivor-") as custom_td:
            archive = Path(archive_td)
            folder = archive / "Channel A"
            folder.mkdir()
            primary = folder / "primary.mp4"
            primary.write_bytes(b"primary")
            survivor = Path(custom_td) / "survivor.mp4"
            survivor.write_bytes(b"survivor")
            target_id = _insert_video(
                conn, title="Primary", filepath=str(primary),
                video_id="commitid001", channel="Channel A")
            survivor_id = _insert_video(
                conn, title="Survivor", filepath=str(survivor),
                video_id="commitid001", channel="Manual",
                is_duplicate_of=str(primary))
            _insert_segment(conn, video_id="commitid001", text="preserve")
            conn.commit()
            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(archive)}):
                prepared = index.prepare_channel_copy_deletion(
                    ["Channel A"], folder_paths=[str(folder)])
            self.assertTrue(prepared["ok"], prepared.get("error"))
            self.assertEqual(prepared["row_ids"], [target_id])

            conn.execute(
                "UPDATE videos SET title='Replacement identity' WHERE id=?",
                (survivor_id,))
            conn.commit()
            deleted = index.delete_media_copy_rows(
                prepared["row_ids"], prepared=prepared)

            self.assertFalse(deleted["ok"])
            self.assertIn("Catalog row changed", deleted["error"])
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos WHERE video_id='commitid001'"
            ).fetchone()[0], 2)
            self.assertEqual(conn.execute(
                "SELECT text FROM segments WHERE video_id='commitid001'"
            ).fetchone()[0], "preserve")
            index.finalize_copy_deletion_preparation(prepared)

    def test_channel_reads_rank_copies_inside_requested_channel(self) -> None:
        with _disposable_index() as (root, conn):
            manual_primary = str(root / "manual" / "shared.mp4")
            archive_copy = str(root / "Archive Channel" / "shared.mp4")
            second_archive_copy = str(
                root / "Archive Channel" / "shared-copy.mp4")
            archive_unique = str(root / "Archive Channel" / "unique.mp4")
            for media_path in (
                    manual_primary, archive_copy, second_archive_copy,
                    archive_unique):
                Path(media_path).parent.mkdir(parents=True, exist_ok=True)
                Path(media_path).write_bytes(b"media")

            _insert_video(
                conn, title="Manual primary", filepath=manual_primary,
                video_id="sharedvid01", channel="Manual Downloads",
                upload_ts=100)
            _insert_video(
                conn, title="Archive copy", filepath=archive_copy,
                video_id="sharedvid01", channel="Archive Channel",
                upload_ts=200, is_duplicate_of=manual_primary)
            _insert_video(
                conn, title="Second archive copy",
                filepath=second_archive_copy, video_id="sharedvid01",
                channel="Archive Channel", upload_ts=300,
                is_duplicate_of=manual_primary)
            _insert_video(
                conn, title="Unique archive video", filepath=archive_unique,
                video_id="uniquevid01", channel="Archive Channel",
                upload_ts=400)
            conn.execute(
                "UPDATE videos SET view_count=1, like_count=1, "
                "downloaded_ts=500, size_bytes=10")
            conn.commit()

            full = index.list_videos_for_channel(
                "Archive Channel", include_thumbs=False)
            self.assertEqual(
                {row["filepath"] for row in full},
                {archive_copy, archive_unique})

            first_page = index.list_videos_for_channel_page(
                "Archive Channel", limit=1, include_thumbs=False)
            second_page = index.list_videos_for_channel_page(
                "Archive Channel", limit=1, offset=1,
                include_thumbs=False)
            self.assertTrue(first_page["has_more"])
            self.assertFalse(second_page["has_more"])
            self.assertEqual(first_page["rows"][0]["filepath"], archive_unique)
            self.assertEqual(second_page["rows"][0]["filepath"], archive_copy)

            stats = index.channel_transcription_stats("Archive Channel")
            self.assertEqual(stats["total"], 2)
            self.assertEqual(stats["transcribed"], 2)

            global_rows = index.list_all_videos(
                limit=10, include_thumbs=False)["rows"]
            self.assertIn(manual_primary,
                          {row["filepath"] for row in global_rows})
            self.assertNotIn(archive_copy,
                             {row["filepath"] for row in global_rows})

            cache_file = root / "disk-cache.json"
            with mock.patch.object(
                    archive_scan, "load_config",
                    return_value={"output_dir": str(root)}), \
                    mock.patch.object(
                        archive_scan, "DISK_CACHE_FILE", cache_file):
                cached = archive_scan.update_disk_cache_for_channel({
                    "name": "Archive Channel",
                    "url": "https://example.invalid/archive-channel",
                })
            self.assertEqual(cached["n_vids"], 2)
            self.assertEqual(cached["physical_copies"], 3)

            with mock.patch.object(
                    archive_scan, "load_config",
                    return_value={"output_dir": str(root)}), \
                    mock.patch.object(
                        archive_scan, "DISK_CACHE_FILE", cache_file):
                scanned = archive_scan.update_disk_cache_for_channel({
                    "name": "Archive Channel",
                    "url": "https://example.invalid/archive-channel",
                }, force_filesystem=True)
            self.assertEqual(scanned["n_vids"], 2)
            self.assertEqual(scanned["physical_copies"], 3)

    def test_delete_primary_promotes_copy_and_final_copy_owns_transcript(self) -> None:
        with _disposable_index() as (root, conn):
            primary = str(root / "primary.mp4")
            copy_two = str(root / "copy-two.mp4")
            copy_three = str(root / "copy-three.mp4")
            video_id = "copyvideo01"
            for media_path in (primary, copy_two, copy_three):
                Path(media_path).write_bytes(b"media")
            _insert_video(
                conn, title="Primarytitle", filepath=primary,
                video_id=video_id)
            _insert_video(
                conn, title="Secondtitle", filepath=copy_two,
                video_id=video_id, is_duplicate_of=primary)
            _insert_video(
                conn, title="Thirdtitle", filepath=copy_three,
                video_id=video_id, is_duplicate_of=primary)
            _insert_segment(conn, video_id=video_id, text="ownedtranscript")
            conn.commit()

            first = index.delete_media_copy(primary)
            self.assertTrue(first["ok"])
            self.assertEqual(first["segments"], 0)
            self.assertEqual(first["primary_filepath"], copy_two)
            self.assertEqual(conn.execute(
                "SELECT filepath, is_duplicate_of FROM videos "
                "WHERE video_id=? ORDER BY id", (video_id,)).fetchall(), [
                    (copy_two, None),
                    (copy_three, copy_two),
                ])
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments WHERE video_id=?",
                (video_id,)).fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'ownedtranscript'").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos_fts "
                "WHERE videos_fts MATCH 'primarytitle'").fetchone()[0], 0)

            second = index.delete_media_copy(copy_two)
            self.assertEqual(second["primary_filepath"], copy_three)
            self.assertEqual(conn.execute(
                "SELECT is_duplicate_of FROM videos WHERE filepath=?",
                (copy_three,)).fetchone()[0], None)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments WHERE video_id=?",
                (video_id,)).fetchone()[0], 1)

            final = index.delete_media_copy(copy_three)
            self.assertEqual(final["segments"], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos WHERE video_id=?",
                (video_id,)).fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments WHERE video_id=?",
                (video_id,)).fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'ownedtranscript'").fetchone()[0], 0)

    def test_channel_delete_preserves_cross_channel_copy_and_transcript(self) -> None:
        with _disposable_index() as (root, conn):
            primary = str(root / "a.mp4")
            survivor = str(root / "b.mp4")
            video_id = "crosscopy01"
            Path(primary).write_bytes(b"a")
            Path(survivor).write_bytes(b"b")
            _insert_video(
                conn, title="A", filepath=primary, video_id=video_id,
                channel="Channel A")
            _insert_video(
                conn, title="B", filepath=survivor, video_id=video_id,
                channel="Channel B", is_duplicate_of=primary)
            _insert_segment(
                conn, video_id=video_id, text="crosschanneltranscript",
                channel="Channel A")
            conn.commit()

            result = index.delete_channel_from_index("Channel A")
            self.assertEqual(result, {"videos": 1, "segments": 0})
            self.assertEqual(conn.execute(
                "SELECT filepath, is_duplicate_of FROM videos "
                "WHERE video_id=?", (video_id,)).fetchone(), (survivor, None))
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments WHERE video_id=?",
                (video_id,)).fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT title, channel, jsonl_path FROM segments "
                "WHERE video_id=?", (video_id,)).fetchone(),
                ("B", "Channel B", "aggregate.jsonl"))

    def test_null_path_row_is_ineligible_and_channel_delete_cleans_fts(self) -> None:
        with _disposable_index() as (_root, conn):
            video_id = "nullghost01"
            _insert_video(
                conn, title="Nullghosttoken", filepath=None,
                video_id=video_id, channel="Ghost Channel",
                availability=None)
            _insert_segment(
                conn, video_id=video_id, text="nullsegmenttoken",
                channel="Ghost Channel")
            conn.commit()

            ctes = index.canonical_videos_cte_sql()
            self.assertEqual(conn.execute(
                f"WITH {ctes} SELECT is_available_copy, physical_copy_count "
                "FROM canonical_videos WHERE video_id=?",
                (video_id,)).fetchone(), (0, 0))

            result = index.delete_channel_from_index("Ghost Channel")
            self.assertEqual(result, {"videos": 1, "segments": 1})
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos WHERE video_id=?",
                (video_id,)).fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos_fts "
                "WHERE videos_fts MATCH 'nullghosttoken'").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'nullsegmenttoken'").fetchone()[0], 0)

    def test_prune_null_path_row_preserves_valid_same_id_holder(self) -> None:
        with _disposable_index() as (root, conn):
            survivor = root / "survivor.mp4"
            survivor.write_bytes(b"media")
            video_id = "nullcopy001"
            _insert_video(
                conn, title="Ghost", filepath=None, video_id=video_id,
                channel="Old Channel", availability=None)
            _insert_video(
                conn, title="Survivor", filepath=str(survivor),
                video_id=video_id, channel="New Channel",
                is_duplicate_of="missing-primary.mp4")
            _insert_segment(
                conn, video_id=video_id, text="preservedtoken",
                channel="Old Channel")
            conn.commit()

            result = index_maintenance.prune_missing_videos()

            self.assertEqual(result["videos_removed"], 1)
            self.assertEqual(result["segments_removed"], 0)
            self.assertEqual(conn.execute(
                "SELECT filepath, is_duplicate_of FROM videos WHERE video_id=?",
                (video_id,)).fetchone(), (str(survivor), None))
            self.assertEqual(conn.execute(
                "SELECT title, channel FROM segments WHERE video_id=?",
                (video_id,)).fetchone(), ("Survivor", "New Channel"))
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments_fts "
                "WHERE segments_fts MATCH 'preservedtoken'"
            ).fetchone()[0], 1)

    def test_prepare_delete_hands_sidecar_to_promoted_copy(self) -> None:
        with _disposable_index() as (root, conn):
            primary = root / "primary.mp4"
            survivor = root / "survivor.mp4"
            primary.write_bytes(b"primary")
            survivor.write_bytes(b"survivor")
            source_jsonl = primary.with_suffix(".jsonl")
            survivor_jsonl = survivor.with_suffix(".jsonl")
            payload = b'{"video_id":"handoff001","text":"durable"}\n'
            source_jsonl.write_bytes(payload)
            _insert_video(
                conn, title="Old title", filepath=str(primary),
                video_id="handoff001", channel="Channel A")
            _insert_video(
                conn, title="New title", filepath=str(survivor),
                video_id="handoff001", channel="Channel B",
                is_duplicate_of=str(primary))
            seg_id = _insert_segment(
                conn, video_id="handoff001", text="durable transcript",
                channel="Channel A")
            conn.execute(
                "UPDATE segments SET jsonl_path=? WHERE id=?",
                (str(source_jsonl), seg_id))
            conn.execute(
                "INSERT INTO indexed_files(path, mtime, segment_count) "
                "VALUES(?, 1, 1)", (str(source_jsonl),))
            conn.commit()

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}):
                prepared = index.prepare_media_copy_deletion(str(primary))
            self.assertTrue(prepared["ok"])
            written = [
                json.loads(line)
                for line in survivor_jsonl.read_text(
                    encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(written), 1)
            self.assertEqual(written[0]["video_id"], "handoff001")
            self.assertEqual(written[0]["title"], "New title")
            self.assertEqual(written[0]["channel"], "Channel B")
            self.assertEqual(written[0]["text"], "durable transcript")

            deleted = index.delete_media_copy(str(primary))
            self.assertTrue(deleted["ok"])
            self.assertEqual(conn.execute(
                "SELECT title, channel, jsonl_path FROM segments "
                "WHERE id=?", (seg_id,)).fetchone(),
                ("New title", "Channel B", str(survivor_jsonl)))
            self.assertEqual(conn.execute(
                "SELECT segment_count FROM indexed_files WHERE path=?",
                (str(survivor_jsonl),)).fetchone()[0], 1)

    def test_conflicting_survivor_sidecar_aborts_before_trash(self) -> None:
        with _disposable_index() as (root, conn):
            primary = root / "primary.mp4"
            survivor = root / "survivor.mp4"
            primary.write_bytes(b"primary")
            survivor.write_bytes(b"survivor")
            primary.with_suffix(".jsonl").write_text(
                '{"video_id":"conflict001","text":"source"}\n',
                encoding="utf-8")
            survivor.with_suffix(".jsonl").write_text(
                '{"video_id":"conflict001","text":"different"}\n',
                encoding="utf-8")
            _insert_video(
                conn, title="Primary", filepath=str(primary),
                video_id="conflict001")
            _insert_video(
                conn, title="Survivor", filepath=str(survivor),
                video_id="conflict001", is_duplicate_of=str(primary))
            _insert_segment(conn, video_id="conflict001", text="transcript")
            conn.commit()

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}), mock.patch(
                    "backend.services.file_ops.safe_trash_video_file") as trash:
                result = VideoMixin().video_delete_file(str(primary))

            self.assertFalse(result["ok"])
            self.assertIn("different transcript details", result["error"].lower())
            trash.assert_not_called()
            self.assertTrue(primary.is_file())
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos WHERE video_id='conflict001'"
            ).fetchone()[0], 2)

    def test_prune_repairs_singleton_stale_duplicate_marker(self) -> None:
        with _disposable_index() as (root, conn):
            media = root / "survivor.mp4"
            media.write_bytes(b"media")
            _insert_video(
                conn, title="Visible survivor", filepath=str(media),
                video_id="singleton01", is_duplicate_of="gone.mp4")
            conn.commit()

            result = index_maintenance.prune_missing_videos()
            self.assertGreaterEqual(result["duplicate_id"], 1)
            self.assertIsNone(conn.execute(
                "SELECT is_duplicate_of FROM videos WHERE video_id='singleton01'"
            ).fetchone()[0])
            self.assertEqual(len(index.list_videos_for_channel("Channel")), 1)

    def test_trash_failure_rolls_back_only_preflight_owned_sidecar(self) -> None:
        with _disposable_index() as (root, conn):
            primary = root / "primary.mp4"
            survivor = root / "survivor.mp4"
            primary.write_bytes(b"primary")
            survivor.write_bytes(b"survivor")
            _insert_video(
                conn, title="Primary", filepath=str(primary),
                video_id="rollback001", channel="Old")
            _insert_video(
                conn, title="Survivor", filepath=str(survivor),
                video_id="rollback001", channel="New",
                is_duplicate_of=str(primary))
            _insert_segment(
                conn, video_id="rollback001", text="durable", channel="Old")
            conn.commit()

            config = mock.patch(
                "backend.ytarchiver_config.load_config",
                return_value={"output_dir": str(root)})
            failed_trash = mock.patch(
                "backend.services.file_ops.safe_trash_video_file",
                return_value={"ok": False, "error": "trash failed"})
            with config, failed_trash:
                result = VideoMixin().video_delete_file(str(primary))
            survivor_jsonl = survivor.with_suffix(".jsonl")
            self.assertFalse(result["ok"])
            self.assertFalse(survivor_jsonl.exists())
            self.assertEqual(list(root.glob(".*.handoff-*.json")), [])

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}):
                prepared = index.prepare_media_copy_deletion(str(primary))
                self.assertTrue(prepared["ok"])
                index.finalize_copy_deletion_preparation(prepared)
            user_equivalent = survivor_jsonl.read_bytes()
            with config, failed_trash:
                result = VideoMixin().video_delete_file(str(primary))
            self.assertFalse(result["ok"])
            self.assertEqual(survivor_jsonl.read_bytes(), user_equivalent)

    def test_post_trash_index_failure_keeps_committed_survivor_sidecar(
            self) -> None:
        with _disposable_index() as (root, conn):
            primary = root / "primary.mp4"
            survivor = root / "survivor.mp4"
            primary.write_bytes(b"primary")
            survivor.write_bytes(b"survivor")
            _insert_video(
                conn, title="Primary", filepath=str(primary),
                video_id="indexfail01")
            _insert_video(
                conn, title="Survivor", filepath=str(survivor),
                video_id="indexfail01", is_duplicate_of=str(primary))
            _insert_segment(conn, video_id="indexfail01", text="keep me")
            conn.commit()

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}), mock.patch(
                    "backend.services.file_ops.safe_trash_video_file",
                    return_value={"ok": True,
                                  "trashed_file_path": "trash/primary.mp4"}), \
                    mock.patch.object(
                        index, "delete_media_copy",
                        return_value={"ok": False, "error": "db failed"}):
                result = VideoMixin().video_delete_file(str(primary))

            survivor_jsonl = survivor.with_suffix(".jsonl")
            self.assertFalse(result["ok"])
            self.assertTrue(result["file_trashed"])
            self.assertTrue(index._sidecar_matches_logical_video(
                str(survivor_jsonl), "indexfail01",
                expected_title="Survivor"))
            self.assertEqual(list(root.glob(".*.handoff-*.json")), [])

    def test_indeterminate_channel_move_keeps_handoff_but_premove_throw_rolls_back(
            self) -> None:
        with _disposable_index() as (root, conn):
            primary = root / "primary.mp4"
            moved = root / "trash" / "primary.mp4"
            survivor = root / "survivor.mp4"
            primary.write_bytes(b"primary")
            survivor.write_bytes(b"survivor")
            _insert_video(
                conn, title="Primary", filepath=str(primary),
                video_id="channelerr1", channel="Channel A")
            _insert_video(
                conn, title="Survivor", filepath=str(survivor),
                video_id="channelerr1", channel="Channel B",
                is_duplicate_of=str(primary))
            _insert_segment(conn, video_id="channelerr1", text="keep")
            conn.commit()
            mixin = SubsMixin()
            mixin._queues = SimpleNamespace(current_sync=None)
            channel = {"name": "Channel A", "folder": "Channel A",
                       "url": "https://example.invalid/channel"}

            def move_then_raise(*_args, **_kwargs):
                moved.parent.mkdir()
                primary.rename(moved)
                raise RuntimeError("indeterminate move")

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}), mock.patch(
                    "backend.api_mixins.subs_mixin.subs_backend.get_channel",
                    return_value=channel), mock.patch(
                    "backend.api_mixins.subs_mixin.subs_backend.remove_channel",
                    side_effect=move_then_raise):
                result = mixin.subs_remove_channel(channel, delete_files=True)
            survivor_jsonl = survivor.with_suffix(".jsonl")
            self.assertFalse(result["ok"])
            self.assertTrue(survivor_jsonl.is_file())
            self.assertEqual(list(root.glob(".*.handoff-*.json")), [])

            moved.rename(primary)
            survivor_jsonl.unlink()
            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}), mock.patch(
                    "backend.api_mixins.subs_mixin.subs_backend.get_channel",
                    return_value=channel), mock.patch(
                    "backend.api_mixins.subs_mixin.subs_backend.remove_channel",
                    side_effect=RuntimeError("pre-move failure")):
                result = mixin.subs_remove_channel(channel, delete_files=True)
            self.assertFalse(result["ok"])
            self.assertFalse(survivor_jsonl.exists())
            self.assertEqual(list(root.glob(".*.handoff-*.json")), [])

    def test_channel_preflight_excludes_all_deleted_alias_paths(self) -> None:
        with _disposable_index() as (root, conn):
            first = root / "a-one.mp4"
            second = root / "a-two.mp4"
            outside = root / "b.mp4"
            first.write_bytes(b"one")
            second.write_bytes(b"two")
            _insert_video(
                conn, title="A1", filepath=str(first),
                video_id="channel001", channel="Alias A")
            _insert_video(
                conn, title="A2", filepath=str(second),
                video_id="channel001", channel="Folder A",
                is_duplicate_of=str(first))
            _insert_video(
                conn, title="B", filepath=str(outside),
                video_id="channel001", channel="Channel B",
                is_duplicate_of=str(first))
            _insert_segment(conn, video_id="channel001", text="outside")
            conn.commit()

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}):
                failed = index.prepare_channel_copy_deletion(
                    ["Alias A", "Folder A"])
            self.assertFalse(failed["ok"])
            self.assertFalse(first.with_suffix(".jsonl").exists())
            self.assertFalse(second.with_suffix(".jsonl").exists())

            outside.write_bytes(b"outside")
            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}):
                prepared = index.prepare_channel_copy_deletion(
                    ["Alias A", "Folder A"])
            self.assertTrue(prepared["ok"])
            self.assertTrue(outside.with_suffix(".jsonl").is_file())
            self.assertFalse(first.with_suffix(".jsonl").exists())
            self.assertFalse(second.with_suffix(".jsonl").exists())
            index.finalize_copy_deletion_preparation(prepared)
            purged = index.delete_channels_from_index(
                ["Alias A", "Folder A"])
            self.assertEqual(purged, {"ok": True, "videos": 2, "segments": 0})
            self.assertEqual(conn.execute(
                "SELECT filepath, is_duplicate_of FROM videos "
                "WHERE video_id='channel001'").fetchone(),
                (str(outside), None))
            self.assertEqual(conn.execute(
                "SELECT jsonl_path FROM segments "
                "WHERE video_id='channel001'").fetchone()[0],
                str(outside.with_suffix(".jsonl")))

    def test_promotion_repairs_disk_truth_and_aggregate_ownership(self) -> None:
        with _disposable_index() as (root, conn):
            primary = root / "primary.mp4"
            missing = root / "missing.mp4"
            partial = root / "partial.mp4"
            survivor = root / "survivor.mp4"
            aggregate = root / "Transcript.jsonl"
            primary.write_bytes(b"primary")
            partial.write_bytes(b"partial")
            survivor.write_bytes(b"survivor")
            video_id = "promote0001"
            _insert_video(
                conn, title="Old", filepath=str(primary), video_id=video_id,
                channel="Old Channel", tx_status="transcribed")
            _insert_video(
                conn, title="Ghost", filepath=str(missing), video_id=video_id,
                channel="Ghost", is_duplicate_of=str(primary))
            _insert_video(
                conn, title="Partial", filepath=str(partial),
                video_id=video_id, channel="Partial", availability="partial",
                is_duplicate_of=str(primary))
            _insert_video(
                conn, title="Canonical", filepath=str(survivor),
                video_id=video_id, channel="New Channel",
                availability="missing", is_duplicate_of=str(primary),
                tx_status="pending")
            seg_id = _insert_segment(
                conn, video_id=video_id, text="canonical text",
                channel="Old Channel")
            other_id = _insert_segment(
                conn, video_id="other000001", text="other text",
                channel="Other")
            conn.execute(
                "UPDATE segments SET jsonl_path=? WHERE id IN (?, ?)",
                (str(aggregate), seg_id, other_id))
            conn.execute(
                "INSERT INTO indexed_files(path, mtime, segment_count) "
                "VALUES(?, 1, 2)", (str(aggregate),))
            conn.commit()

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}):
                prepared = index.prepare_media_copy_deletion(str(primary))
            self.assertTrue(prepared["ok"])
            deleted = index.delete_media_copy(str(primary))
            self.assertTrue(deleted["ok"])
            index.finalize_copy_deletion_preparation(prepared)
            survivor_jsonl = survivor.with_suffix(".jsonl")
            promoted = conn.execute(
                "SELECT availability, is_duplicate_of, tx_status FROM videos "
                "WHERE filepath=?", (str(survivor),)).fetchone()
            self.assertEqual(promoted, ("available", None, "transcribed"))
            self.assertEqual(conn.execute(
                "SELECT availability, is_duplicate_of FROM videos "
                "WHERE filepath=?", (str(missing),)).fetchone(),
                ("missing", str(survivor)))
            self.assertEqual(conn.execute(
                "SELECT availability, is_duplicate_of FROM videos "
                "WHERE filepath=?", (str(partial),)).fetchone(),
                ("partial", str(survivor)))
            self.assertEqual(conn.execute(
                "SELECT title, channel, jsonl_path FROM segments WHERE id=?",
                (seg_id,)).fetchone(),
                ("Canonical", "New Channel", str(survivor_jsonl)))
            self.assertEqual(conn.execute(
                "SELECT segment_count FROM indexed_files WHERE path=?",
                (str(aggregate),)).fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT segment_count FROM indexed_files WHERE path=?",
                (str(survivor_jsonl),)).fetchone()[0], 1)

            conn.execute("DELETE FROM segments WHERE video_id=?", (video_id,))
            conn.execute("DELETE FROM indexed_files WHERE path=?",
                         (str(survivor_jsonl),))
            conn.commit()
            ingested = index.ingest_jsonl(
                str(survivor), str(survivor_jsonl),
                "Canonical", "New Channel", force=True)
            self.assertEqual(ingested, 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*), MIN(title), MIN(channel), MIN(jsonl_path) "
                "FROM segments WHERE video_id=?", (video_id,)).fetchone(),
                (1, "Canonical", "New Channel", str(survivor_jsonl)))
            self.assertEqual(conn.execute(
                "SELECT tx_status FROM videos WHERE filepath=?",
                (str(survivor),)).fetchone()[0], "transcribed")

    def test_unknown_survivor_presence_rolls_back_logical_delete(self) -> None:
        with _disposable_index() as (root, conn):
            primary = root / "primary.mp4"
            survivor = root / "survivor.mp4"
            primary.write_bytes(b"primary")
            survivor.write_bytes(b"survivor")
            _insert_video(
                conn, title="Primary", filepath=str(primary),
                video_id="unknown001")
            _insert_video(
                conn, title="Survivor", filepath=str(survivor),
                video_id="unknown001", is_duplicate_of=str(primary))
            _insert_segment(conn, video_id="unknown001", text="keep")
            conn.commit()
            real_stat = os.stat

            def guarded_stat(path, *args, **kwargs):
                if os.path.normcase(os.path.normpath(path)) == os.path.normcase(
                        os.path.normpath(survivor)):
                    raise PermissionError("temporarily inaccessible")
                return real_stat(path, *args, **kwargs)

            with mock.patch.object(index.os, "stat", side_effect=guarded_stat):
                result = index.delete_media_copy(str(primary))
            self.assertFalse(result["ok"])
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos WHERE video_id='unknown001'"
            ).fetchone()[0], 2)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments WHERE video_id='unknown001'"
            ).fetchone()[0], 1)

    def test_handoff_retry_ownership_and_committed_marker_defense(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ytarchiver-handoff-") as td:
            root = Path(td)
            source = root / "source.jsonl"
            destination = root / "destination.jsonl"
            source.write_text('{"video_id":"retry00001","text":"x"}\n',
                              encoding="utf-8")
            config = mock.patch(
                "backend.ytarchiver_config.load_config",
                return_value={"output_dir": str(root)})
            with config:
                first = file_ops.preserve_sidecar_no_overwrite(
                    str(source), str(destination))
                recovered = file_ops.preserve_sidecar_no_overwrite(
                    str(source), str(destination))
            self.assertTrue(first["created"])
            self.assertTrue(recovered["recovered"])
            self.assertEqual(
                recovered["cleanup_token"], first["cleanup_token"])
            self.assertTrue(file_ops.rollback_preserved_sidecar(
                recovered["cleanup_token"])["ok"])
            self.assertFalse(destination.exists())

            with config:
                published = file_ops.preserve_sidecar_no_overwrite(
                    str(source), str(destination))
                recovered = file_ops.preserve_sidecar_no_overwrite(
                    str(source), str(destination))
            self.assertTrue(file_ops.finalize_preserved_sidecar(
                recovered["cleanup_token"])["ok"])
            self.assertTrue(destination.is_file())
            self.assertEqual(list(root.glob(".*.handoff-*.json")), [])

            destination.unlink()
            with config:
                published = file_ops.preserve_sidecar_no_overwrite(
                    str(source), str(destination))
            marker = Path(published["cleanup_token"]["marker_path"])
            real_remove = os.remove

            def fail_marker_remove(path):
                if os.path.normcase(os.path.normpath(path)) == os.path.normcase(
                        os.path.normpath(marker)):
                    raise PermissionError("marker locked")
                return real_remove(path)

            with mock.patch.object(
                    file_ops.os, "remove", side_effect=fail_marker_remove):
                finalized = file_ops.finalize_preserved_sidecar(
                    published["cleanup_token"])
            self.assertFalse(finalized["ok"])
            self.assertTrue(marker.is_file())
            refused = file_ops.rollback_preserved_sidecar(
                published["cleanup_token"])
            self.assertFalse(refused["ok"])
            self.assertTrue(destination.is_file())
            with config:
                retry = file_ops.preserve_sidecar_no_overwrite(
                    str(source), str(destination))
            self.assertTrue(retry["existing"])
            self.assertNotIn("cleanup_token", retry)
            self.assertFalse(marker.exists())
            self.assertTrue(destination.is_file())

    def test_canonical_metadata_falls_back_without_changing_physical_choice(
            self) -> None:
        with _disposable_index() as (root, conn):
            primary = str(root / "primary.mp4")
            duplicate = str(root / "duplicate.mp4")
            _insert_video(
                conn, title="Primary", filepath=primary,
                video_id="metadata001", upload_ts=None, duration_s=0)
            _insert_video(
                conn, title="Duplicate", filepath=duplicate,
                video_id="metadata001", upload_ts=1720000000,
                is_duplicate_of=primary)
            conn.execute(
                "UPDATE videos SET duration_s=3600 WHERE filepath=?",
                (duplicate,))
            _insert_segment(conn, video_id="metadata001", text="datedtoken")
            conn.commit()

            ctes = index.canonical_videos_cte_sql()
            row = conn.execute(
                f"WITH {ctes} SELECT filepath, duration_s, upload_ts, "
                "logical_duration_s, logical_upload_ts "
                "FROM canonical_videos WHERE video_id='metadata001'"
            ).fetchone()
            self.assertEqual(row, (primary, 0, None, 3600, 1720000000))
            self.assertEqual(index_graph.bucket_totals("year"), {"2024": 1})
            with mock.patch(
                    "backend.ytarchiver_config.TRANSCRIPTION_DB",
                    index.TRANSCRIPTION_DB):
                stats = archive_scan.index_db_stats()
            self.assertEqual(stats["total_videos"], 1)
            self.assertEqual(stats["hours"], 1.0)

    def test_canonical_cte_keeps_missing_ids_path_distinct(self) -> None:
        with _disposable_index() as (root, conn):
            first = str(root / "One" / "same.mp4")
            second = str(root / "Two" / "same.mp4")
            Path(first).parent.mkdir()
            Path(second).parent.mkdir()
            _insert_video(
                conn, title="Same title", filepath=first, video_id=None)
            _insert_video(
                conn, title="Same title", filepath=second, video_id="")
            unavailable_primary = str(root / "unavailable-primary.mp4")
            available_copy = str(root / "available-copy.mp4")
            _insert_video(
                conn, title="Unavailable", filepath=unavailable_primary,
                video_id="rankvideo01", availability="missing")
            _insert_video(
                conn, title="Available", filepath=available_copy,
                video_id="rankvideo01", is_duplicate_of=unavailable_primary)
            conn.commit()

            ctes = index.canonical_videos_cte_sql()
            rows = conn.execute(
                f"WITH {ctes} SELECT filepath, logical_video_key, "
                "is_available_copy, physical_copy_count "
                "FROM canonical_videos ORDER BY filepath").fetchall()
            self.assertEqual(len(rows), 3)
            path_rows = [row for row in rows if row[1].startswith("path:")]
            self.assertEqual(len(path_rows), 2)
            ranked = next(row for row in rows if row[1] == "id:rankvideo01")
            self.assertEqual(ranked[0], available_copy)
            self.assertEqual(ranked[2:], (1, 2))

    def test_graph_and_summary_use_one_canonical_row_per_video_id(self) -> None:
        with _disposable_index() as (root, conn):
            primary = str(root / "primary.mp4")
            duplicate = str(root / "duplicate.mp4")
            video_id = "graphvideo1"
            january = 1705276800.0  # 2024-01-15 UTC
            february = 1707955200.0  # 2024-02-15 UTC
            _insert_video(
                conn, title="Primary", filepath=primary, video_id=video_id,
                upload_ts=january)
            _insert_video(
                conn, title="Duplicate", filepath=duplicate,
                video_id=video_id, upload_ts=february,
                is_duplicate_of=primary)
            _insert_segment(conn, video_id=video_id, text="graphuniquetoken")
            conn.commit()

            totals = index_graph.bucket_totals("month")
            self.assertEqual(totals, {"2024-01": 1})
            graph = index_graph.graph_word_frequency(
                "graphuniquetoken", bucket="month")
            self.assertEqual(graph["labels"], ["2024-01"])
            self.assertEqual(graph["values"], [1])
            self.assertEqual(index.summary()["videos"], 1)

    def test_register_repairs_copy_links_before_primary_filtered_reads(
            self) -> None:
        with _disposable_index() as (root, conn):
            first = root / "first.mp4"
            second = root / "second.mp4"
            first.write_bytes(b"first")
            second.write_bytes(b"second")

            self.assertTrue(index.register_video(
                str(first), "Channel A", title="First",
                video_id="register001"))
            self.assertTrue(index.register_video(
                str(second), "Channel B", title="Second",
                video_id="register001"))

            self.assertEqual(conn.execute(
                "SELECT filepath, is_duplicate_of FROM videos "
                "WHERE video_id='register001' ORDER BY id"
            ).fetchall(), [
                (str(first), None),
                (str(second), str(first)),
            ])
            page = index.list_all_videos(limit=10, include_thumbs=False)
            self.assertEqual(
                [(row["filepath"], row["video_id"]) for row in page["rows"]],
                [(str(first), "register001")],
            )

    def test_disk_only_transcript_is_handed_to_surviving_copy(self) -> None:
        with _disposable_index() as (root, conn):
            primary = root / "disk-primary.mp4"
            survivor = root / "disk-survivor.mp4"
            primary.write_bytes(b"primary")
            survivor.write_bytes(b"survivor")
            primary.with_suffix(".jsonl").write_text(
                '{"video_id":"diskonly001","start":0,"end":1,'
                '"text":"disk only"}\n',
                encoding="utf-8",
            )
            _insert_video(
                conn, title="Old", filepath=str(primary),
                video_id="diskonly001", channel="Old")
            _insert_video(
                conn, title="Survivor", filepath=str(survivor),
                video_id="diskonly001", channel="New",
                is_duplicate_of=str(primary))
            conn.commit()

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}), mock.patch(
                    "backend.services.file_ops.config_is_writable",
                    return_value=True), mock.patch(
                    "backend.api_mixins.video_mixin.config_is_writable",
                    return_value=False):
                result = VideoMixin().video_delete_file(str(primary))

            survivor_jsonl = survivor.with_suffix(".jsonl")
            self.assertTrue(result["ok"], result)
            self.assertTrue(survivor_jsonl.is_file())
            self.assertEqual(index.ingest_jsonl(
                str(survivor), str(survivor_jsonl),
                "Survivor", "New", force=True), 1)
            self.assertEqual(conn.execute(
                "SELECT title, channel, text FROM segments "
                "WHERE video_id='diskonly001'"
            ).fetchone(), ("Survivor", "New", "disk only"))

    def test_same_stem_copy_delete_keeps_shared_survivor_sidecar(self) -> None:
        with _disposable_index() as (root, conn):
            primary = root / "same.mp4"
            survivor = root / "same.mkv"
            primary.write_bytes(b"primary")
            survivor.write_bytes(b"survivor")
            _insert_video(
                conn, title="Same", filepath=str(primary),
                video_id="samestem001")
            _insert_video(
                conn, title="Same", filepath=str(survivor),
                video_id="samestem001", is_duplicate_of=str(primary))
            seg_id = _insert_segment(
                conn, video_id="samestem001", text="shared transcript")
            conn.commit()

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}), mock.patch(
                    "backend.services.file_ops.config_is_writable",
                    return_value=True), mock.patch(
                    "backend.api_mixins.video_mixin.config_is_writable",
                    return_value=False):
                result = VideoMixin().video_delete_file(str(primary))

            shared_jsonl = root / "same.jsonl"
            self.assertTrue(result["ok"])
            self.assertTrue(survivor.is_file())
            self.assertTrue(shared_jsonl.is_file())
            self.assertEqual(conn.execute(
                "SELECT jsonl_path FROM segments WHERE id=?", (seg_id,)
            ).fetchone()[0], str(shared_jsonl))

    def test_stale_primary_delete_still_hands_off_sidecar(self) -> None:
        with _disposable_index() as (root, conn):
            missing = root / "missing.mp4"
            survivor = root / "survivor.mp4"
            survivor.write_bytes(b"survivor")
            old_jsonl = missing.with_suffix(".jsonl")
            old_jsonl.write_text(
                '{"video_id":"stalecopy01","start":0,"end":1,'
                '"text":"keep"}\n', encoding="utf-8")
            _insert_video(
                conn, title="Old", filepath=str(missing),
                video_id="stalecopy01", channel="Old")
            _insert_video(
                conn, title="New", filepath=str(survivor),
                video_id="stalecopy01", channel="New",
                is_duplicate_of=str(missing))
            seg_id = _insert_segment(
                conn, video_id="stalecopy01", text="keep", channel="Old")
            conn.execute(
                "UPDATE segments SET jsonl_path=? WHERE id=?",
                (str(old_jsonl), seg_id))
            conn.commit()

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}), mock.patch(
                    "backend.api_mixins.video_mixin.config_is_writable",
                    return_value=False):
                result = VideoMixin().video_delete_file(str(missing))

            survivor_jsonl = survivor.with_suffix(".jsonl")
            self.assertTrue(result["ok"], result)
            self.assertTrue(survivor_jsonl.is_file())
            self.assertEqual(conn.execute(
                "SELECT title, channel, jsonl_path FROM segments WHERE id=?",
                (seg_id,)).fetchone(),
                ("New", "New", str(survivor_jsonl)))

    def test_recent_delete_never_falls_back_from_explicit_missing_copy(
            self) -> None:
        with tempfile.TemporaryDirectory(prefix="ytarchiver-recent-copy-") as td:
            root = Path(td)
            missing = root / "missing.mp4"
            live = root / "live.mp4"
            live.write_bytes(b"live")
            selected = {
                "title": "Same", "channel": "Channel",
                "video_id": "recentcp001", "filepath": str(missing),
            }
            mixin = RecentMixin()
            mixin._config = {
                "recent_downloads": [selected, {
                    **selected, "filepath": str(live),
                }],
            }
            with mock.patch.object(
                    index, "prepare_media_copy_deletion") as prepare, mock.patch(
                    "backend.services.file_ops.safe_trash_video_file") as trash:
                result = mixin.recent_delete_file(selected)

            self.assertFalse(result["ok"])
            self.assertTrue(live.is_file())
            prepare.assert_not_called()
            trash.assert_not_called()

    def test_channel_preflight_uses_folder_containment_and_exact_rows(
            self) -> None:
        with _disposable_index() as (root, conn):
            folder = root / "Channel A"
            folder.mkdir()
            first = folder / "a.mp4"
            mislabeled = folder / "nested" / "b.mp4"
            mislabeled.parent.mkdir()
            first.write_bytes(b"first")
            mislabeled.write_bytes(b"second")
            first_id = _insert_video(
                conn, title="A", filepath=str(first),
                video_id="mislabel001", channel="Channel A")
            second_id = _insert_video(
                conn, title="B", filepath=str(mislabeled),
                video_id="mislabel001", channel="Channel B",
                is_duplicate_of=str(first))
            _insert_segment(
                conn, video_id="mislabel001", text="do not mis-promote",
                channel="Channel A")
            conn.commit()

            prepared = index.prepare_channel_copy_deletion(
                ["Channel A"], folder_paths=[str(folder)])
            self.assertTrue(prepared["ok"])
            self.assertEqual(prepared["row_ids"], [first_id, second_id])
            self.assertFalse(mislabeled.with_suffix(".jsonl").exists())

            folder.rename(root / "trash")
            deleted = index.delete_media_copy_rows(prepared["row_ids"])
            self.assertEqual(deleted, {
                "ok": True, "videos": 2, "segments": 1})
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos WHERE video_id='mislabel001'"
            ).fetchone()[0], 0)

    def test_channel_preflight_canonicalizes_only_lexical_folder_candidates(
            self) -> None:
        with _disposable_index() as (root, conn):
            folder = root / "Target Channel"
            folder.mkdir()
            target = folder / "nested" / "target.mp4"
            target.parent.mkdir()
            target.write_bytes(b"target")
            target_id = _insert_video(
                conn, title="Target", filepath=str(target),
                video_id="prefilter001", channel="Stale Label")
            unrelated_paths = []
            for number in range(1000):
                path = root / "Other Channels" / f"video-{number}.mp4"
                unrelated_paths.append(str(path))
                _insert_video(
                    conn, title=f"Other {number}", filepath=str(path),
                    video_id=f"other{number:06d}", channel="Other")
            sibling = root / "Target Channel old" / "sibling.mp4"
            sibling_path = str(sibling)
            _insert_video(
                conn, title="Sibling", filepath=sibling_path,
                video_id="sibling001", channel="Other")
            conn.commit()

            real_realpath = os.path.realpath
            canonicalized = []

            def counting_realpath(path, *args, **kwargs):
                canonicalized.append(os.path.normpath(os.fspath(path)))
                return real_realpath(path, *args, **kwargs)

            with mock.patch.object(
                    index.os.path, "realpath",
                    side_effect=counting_realpath), mock.patch.object(
                    index, "prepare_media_copy_deletion",
                    return_value={"ok": True, "needed": False}):
                prepared = index.prepare_channel_copy_deletion(
                    ["Target Channel"], folder_paths=[str(folder)])

            self.assertTrue(prepared["ok"])
            self.assertEqual(prepared["row_ids"], [target_id])
            self.assertEqual(len(canonicalized), 2)
            self.assertIn(os.path.normpath(str(folder)), canonicalized)
            self.assertIn(os.path.normpath(str(target)), canonicalized)
            self.assertNotIn(os.path.normpath(sibling_path), canonicalized)
            self.assertFalse(
                set(map(os.path.normpath, unrelated_paths))
                & set(canonicalized))

    def test_indexed_path_recount_uses_jsonl_path_index(self) -> None:
        with _disposable_index() as (root, conn):
            target_path = str(root / "target.jsonl")
            rows = [
                (f"other{number:06d}", f"Other {number}", "Other", 2024,
                 1, 0, 1, "text", str(root / f"other-{number}.jsonl"))
                for number in range(6000)
            ]
            rows.extend([
                ("target001", "Target", "Target", 2024, 1,
                 number, number + 1, "target text", target_path)
                for number in range(3)
            ])
            conn.executemany(
                "INSERT INTO segments(video_id, title, channel, year, month, "
                "start_time, end_time, text, jsonl_path) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            conn.execute(
                "INSERT INTO indexed_files(path, mtime, segment_count) "
                "VALUES(?, 1, 999)", (target_path,))
            conn.commit()

            plan = conn.execute(
                "EXPLAIN QUERY PLAN SELECT COUNT(*) FROM segments "
                "WHERE jsonl_path=?", (target_path,)).fetchall()
            self.assertTrue(any(
                "SEARCH segments" in str(row[3])
                and "idx_seg_jsonl" in str(row[3])
                for row in plan
            ), plan)

            # A full-table scan exceeds this VM budget and is interrupted;
            # the indexed equality lookup completes comfortably below it.
            conn.set_progress_handler(lambda: 1, 5000)
            try:
                index._recount_indexed_path_locked(conn, target_path)
            finally:
                conn.set_progress_handler(None, 0)
            self.assertEqual(conn.execute(
                "SELECT segment_count FROM indexed_files WHERE path=?",
                (target_path,)).fetchone()[0], 3)

    def test_channel_quarantine_failure_never_purges_live_catalog_rows(
            self) -> None:
        mixin = SubsMixin()
        remove_for_target = mock.Mock()
        mixin._queues = SimpleNamespace(
            current_sync=None,
            sync_remove_all_for_target=remove_for_target,
        )
        mixin._reload_config = mock.Mock()
        mixin._on_queue_changed = mock.Mock()
        channel = {
            "name": "Channel A", "folder": "Channel A",
            "url": "https://example.invalid/channel-a",
        }
        prepared = {"ok": True, "cleanup_tokens": [], "row_ids": [7]}
        with mock.patch(
                "backend.api_mixins.subs_mixin.load_config",
                return_value={"output_dir": "C:/Archive"}), mock.patch(
                "backend.api_mixins.subs_mixin.subs_backend.get_channel",
                return_value=channel), mock.patch(
                "backend.api_mixins.subs_mixin.subs_backend.remove_channel",
                return_value={
                    "ok": True, "deleted_folder": False,
                    "delete_error": "folder locked",
                }), mock.patch.object(
                index, "prepare_channel_copy_deletion",
                return_value=prepared), mock.patch.object(
                index, "rollback_copy_deletion_preparation") as rollback, \
                mock.patch.object(
                    index, "delete_media_copy_rows") as purge:
            result = mixin.subs_remove_channel(channel, delete_files=True)

        self.assertFalse(result["ok"])
        self.assertTrue(result["subscription_removed"])
        self.assertFalse(result["files_removed"])
        self.assertIsNone(result["catalog_cleanup_ok"])
        self.assertEqual(result["delete_error"], "folder locked")
        rollback.assert_called_once_with(prepared)
        purge.assert_not_called()
        remove_for_target.assert_called_once_with(channel["url"])

    def test_channel_remove_refetches_under_lock_and_reports_catalog_warning(
            self) -> None:
        class TrackingLock:
            def __init__(self):
                self.held = False

            def __enter__(self):
                self.held = True
                return self

            def __exit__(self, *_args):
                self.held = False

        mixin = SubsMixin()
        lock = TrackingLock()
        mixin._sync_mutation_lock = lock
        remove_for_target = mock.Mock()
        mixin._queues = SimpleNamespace(
            current_sync=None,
            sync_remove_all_for_target=remove_for_target,
        )
        mixin._reload_config = mock.Mock()
        mixin._on_queue_changed = mock.Mock()
        stale = {
            "name": "Old Name", "folder": "Old Folder",
            "url": "https://example.invalid/channel-a",
        }
        current = {
            "name": "Current Name", "folder": "Current Folder",
            "folder_override": "Override Folder",
            "url": stale["url"],
        }
        lock_observations = []

        def get_channel(_identity):
            lock_observations.append(lock.held)
            return stale if len(lock_observations) == 1 else current

        prepared = {"ok": True, "cleanup_tokens": [], "row_ids": [7]}
        with mock.patch(
                "backend.api_mixins.subs_mixin.load_config",
                return_value={"output_dir": "C:/Archive"}), mock.patch(
                "backend.api_mixins.subs_mixin.subs_backend.get_channel",
                side_effect=get_channel), mock.patch(
                "backend.api_mixins.subs_mixin.subs_backend.remove_channel",
                return_value={
                    "ok": True,
                    "deleted_folder": True,
                    "folder_path": "C:/Archive/Override Folder",
                    "trashed_folder_path": "C:/Archive/.trash/current",
                }), mock.patch.object(
                index, "prepare_channel_copy_deletion",
                return_value=prepared) as prepare, mock.patch.object(
                index, "delete_media_copy_rows",
                return_value={"ok": False, "error": "database busy"}), \
                mock.patch.object(
                    index, "finalize_copy_deletion_preparation") as finalize:
            result = mixin.subs_remove_channel(
                {"url": stale["url"]}, delete_files=True)

        self.assertEqual(lock_observations, [False, True])
        prepare.assert_called_once_with(
            ["Current Folder", "Current Name", "Override Folder"],
            folder_paths=[os.path.join("C:/Archive", "Override Folder")],
        )
        self.assertFalse(result["ok"])
        self.assertTrue(result["subscription_removed"])
        self.assertTrue(result["files_removed"])
        self.assertFalse(result["catalog_cleanup_ok"])
        self.assertFalse(result["write_blocked"])
        self.assertIn("database busy", result["catalog_warning"])
        self.assertEqual(result["delete_error"], result["catalog_warning"])
        finalize.assert_called_once_with(prepared)
        remove_for_target.assert_called_once_with(current["url"])
        mixin._reload_config.assert_called_once_with()

    def test_bulk_remove_counts_committed_catalog_warning_as_removed(
            self) -> None:
        mixin = SubsMixin()
        mixin._log_stream = mock.Mock()
        mixin._window = object()
        event_bus = mock.Mock()
        mixin.services = SimpleNamespace(event_bus=event_bus)
        mixin.subs_remove_channel = mock.Mock(return_value={
            "ok": False,
            "subscription_removed": True,
            "catalog_warning": "database busy",
        })

        def run_immediately(*_args, **kwargs):
            kwargs["target"]()

        with mock.patch(
                "backend.api_mixins.subs_mixin.subs_backend.get_channel",
                return_value={
                    "name": "Channel A",
                    "url": "https://example.invalid/channel-a",
                }), mock.patch(
                "backend.api_mixins.subs_mixin.start_managed_task",
                side_effect=run_immediately):
            started = mixin.subs_bulk_delete(
                ["Channel A"], delete_files=True)

        self.assertEqual(started, {"ok": True, "started": True})
        event_bus.show_toast_and_refresh_subs.assert_called_once_with(
            "Removed 1 channel(s). 1 completed with a cleanup warning. "
            "First issue: database busy",
            "warn",
        )

    def test_partial_trash_rollback_commits_survivor_handoff(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ytarchiver-partial-trash-") as td:
            media = Path(td) / "video.mp4"
            media.write_bytes(b"video")
            prepared = {
                "ok": True,
                "cleanup_tokens": [{"marker_path": "marker"}],
            }
            with mock.patch.object(
                    index, "prepare_media_copy_deletion",
                    return_value=prepared), mock.patch(
                    "backend.services.file_ops.assert_within_managed_roots",
                    return_value={"ok": True}), mock.patch(
                    "backend.services.file_ops.safe_trash_video_file",
                    return_value={
                        "ok": False, "error": "partial rollback",
                        "rollback_failed": ["jsonl rollback failed"],
                    }), mock.patch.object(
                    index, "finalize_copy_deletion_preparation") as finalize, \
                    mock.patch.object(
                        index, "rollback_copy_deletion_preparation") as rollback:
                result = VideoMixin().video_delete_file(str(media))

            self.assertFalse(result["ok"])
            finalize.assert_called_once_with(prepared)
            rollback.assert_not_called()

    def test_copy_delete_rejects_transcript_changed_after_preflight(self) -> None:
        with _disposable_index() as (root, conn):
            primary = root / "primary.mp4"
            survivor = root / "survivor.mp4"
            primary.write_bytes(b"primary")
            survivor.write_bytes(b"survivor")
            _insert_video(
                conn, title="Primary", filepath=str(primary),
                video_id="snapshot001", channel="Old")
            _insert_video(
                conn, title="Survivor", filepath=str(survivor),
                video_id="snapshot001", channel="New",
                is_duplicate_of=str(primary))
            _insert_segment(
                conn, video_id="snapshot001", text="before", channel="Old")
            conn.commit()

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}):
                prepared = index.prepare_media_copy_deletion(str(primary))
            _insert_segment(
                conn, video_id="snapshot001", text="after preflight",
                channel="Old")
            conn.commit()

            deleted = index.delete_media_copy(
                str(primary), prepared=prepared)

            self.assertFalse(deleted["ok"])
            self.assertIn("Transcript changed", deleted["error"])
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM videos WHERE video_id='snapshot001'"
            ).fetchone()[0], 2)
            self.assertEqual(conn.execute(
                "SELECT text FROM segments WHERE video_id='snapshot001' "
                "ORDER BY id").fetchall(),
                [("before",), ("after preflight",)])

    def test_channel_delete_rejects_reused_preflight_row_id(self) -> None:
        with _disposable_index() as (root, conn):
            folder = root / "Channel A"
            folder.mkdir()
            target = folder / "target.mp4"
            replacement = root / "replacement.mp4"
            target.write_bytes(b"target")
            replacement.write_bytes(b"replacement")
            target_id = _insert_video(
                conn, title="Target", filepath=str(target),
                video_id="target00001", channel="Channel A")
            conn.commit()
            prepared = index.prepare_channel_copy_deletion(
                ["Channel A"], folder_paths=[str(folder)])
            self.assertEqual(prepared["row_ids"], [target_id])

            conn.execute("DELETE FROM videos WHERE id=?", (target_id,))
            replacement_id = _insert_video(
                conn, title="Replacement", filepath=str(replacement),
                video_id="replace0001", channel="Channel B")
            _insert_segment(
                conn, video_id="replace0001", text="must survive",
                channel="Channel B")
            conn.commit()
            self.assertEqual(replacement_id, target_id)

            deleted = index.delete_media_copy_rows(
                prepared["row_ids"], prepared=prepared)

            self.assertFalse(deleted["ok"])
            self.assertIn("Catalog row changed", deleted["error"])
            self.assertEqual(conn.execute(
                "SELECT filepath FROM videos WHERE id=?", (replacement_id,)
            ).fetchone()[0], str(replacement))
            self.assertEqual(conn.execute(
                "SELECT text FROM segments WHERE video_id='replace0001'"
            ).fetchone()[0], "must survive")

    def test_changed_aggregate_reingest_replaces_promoted_source(self) -> None:
        with _disposable_index() as (root, conn):
            old_folder = root / "Old"
            new_folder = root / "New"
            old_folder.mkdir()
            new_folder.mkdir()
            primary = old_folder / "primary.mp4"
            survivor = new_folder / "survivor.mp4"
            aggregate = old_folder / ".Old Transcript.jsonl"
            primary.write_bytes(b"primary")
            survivor.write_bytes(b"survivor")
            original = {
                "video_id": "aggregate01", "title": "Transcript",
                "start": 0, "end": 1, "text": "original",
            }
            aggregate.write_text(
                json.dumps(original) + "\n", encoding="utf-8")
            _insert_video(
                conn, title="Primary", filepath=str(primary),
                video_id="aggregate01", channel="Old")
            _insert_video(
                conn, title="Survivor", filepath=str(survivor),
                video_id="aggregate01", channel="New",
                is_duplicate_of=str(primary))
            segment_id = _insert_segment(
                conn, video_id="aggregate01", text="original", channel="Old")
            conn.execute(
                "UPDATE segments SET jsonl_path=? WHERE id=?",
                (str(aggregate), segment_id))
            conn.execute(
                "INSERT INTO indexed_files(path, mtime, segment_count) "
                "VALUES(?, ?, 1)",
                (str(aggregate), os.path.getmtime(aggregate)))
            conn.commit()

            with mock.patch(
                    "backend.ytarchiver_config.load_config",
                    return_value={"output_dir": str(root)}):
                prepared = index.prepare_media_copy_deletion(str(primary))
            self.assertTrue(index.delete_media_copy(
                str(primary), prepared=prepared)["ok"])
            index.finalize_copy_deletion_preparation(prepared)

            other = {
                "video_id": "other000001", "title": "Other",
                "start": 0, "end": 1, "text": "new",
            }
            aggregate.write_text(
                json.dumps(original) + "\n" + json.dumps(other) + "\n",
                encoding="utf-8")
            changed_mtime = os.path.getmtime(aggregate) + 2
            os.utime(aggregate, (changed_mtime, changed_mtime))
            self.assertEqual(index.ingest_jsonl(
                str(old_folder / "Old Transcript.txt"), str(aggregate),
                "Old Transcript", "Old"), 2)

            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM segments WHERE video_id='aggregate01'"
            ).fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT jsonl_path FROM segments WHERE video_id='aggregate01'"
            ).fetchone()[0], str(aggregate))
            self.assertIsNone(conn.execute(
                "SELECT segment_count FROM indexed_files WHERE path=?",
                (str(survivor.with_suffix(".jsonl")),)).fetchone())


if __name__ == "__main__":
    unittest.main()
