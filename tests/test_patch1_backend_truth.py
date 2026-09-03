from __future__ import annotations

# APPDATA must be redirected before backend imports in this standalone test.
# ruff: noqa: E402, I001

import os
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch1-tests-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import autorun, compress, livestreams, ytarchiver_config
from backend.api_mixins.index_mixin import IndexMixin
from backend.api_mixins import subs_mixin
from backend.metadata import refresh_comments, refresh_views
from backend.sync.sync_all import _task_result_failure_state


class _NoopThread:
    def __init__(self, *args, **kwargs):
        pass

    def start(self):
        pass


class Patch1BackendTruthTests(unittest.TestCase):
    def test_cancelled_comments_refresh_is_not_stamped_fresh(self) -> None:
        cancel = threading.Event()
        cancel.set()
        channel = {"name": "Channel", "url": "https://example.test/c"}
        with (
            tempfile.TemporaryDirectory() as td,
            mock.patch.object(
                refresh_comments, "_folder_for_channel",
                return_value=Path(td)),
            mock.patch.object(
                refresh_comments, "find_yt_dlp",
                return_value="yt-dlp"),
            mock.patch.object(
                refresh_comments, "_scan_channel_videos",
                return_value=[]),
            mock.patch.object(
                refresh_comments, "stamp_channel_refresh") as stamp,
        ):
            result = refresh_comments.refresh_channel_comments(
                channel, mock.Mock(), cancel_event=cancel)

        self.assertFalse(result["ok"])
        self.assertTrue(result["cancelled"])
        stamp.assert_not_called()

    def test_cancelled_views_refresh_is_not_stamped_fresh(self) -> None:
        cancel = threading.Event()
        cancel.set()
        channel = {"name": "Channel", "url": "https://example.test/c"}
        catalog = {
            "abcdefghijk": {
                "upload_date": "20000101",
                "view_count": 1,
            },
        }
        with (
            tempfile.TemporaryDirectory() as td,
            mock.patch.object(
                refresh_views, "_folder_for_channel",
                return_value=Path(td)),
            mock.patch.object(
                refresh_views, "find_yt_dlp",
                return_value="yt-dlp"),
            mock.patch.object(
                refresh_views, "_flat_playlist_bulk_stats",
                return_value=catalog),
            mock.patch.object(
                refresh_views, "_filter_recent_on_disk",
                return_value=[]),
            mock.patch.object(
                refresh_views, "stamp_channel_refresh") as stamp,
            mock.patch.object(
                refresh_views.threading, "Thread", _NoopThread),
        ):
            result = refresh_views.bulk_refresh_views_likes(
                channel, mock.Mock(), cancel_event=cancel,
                scope={"days": 365})

        self.assertFalse(result["ok"])
        self.assertTrue(result["cancelled"])
        stamp.assert_not_called()

    def test_explicit_false_result_always_counts_as_failure(self) -> None:
        self.assertEqual(
            _task_result_failure_state(
                {"ok": False, "error": "failed"}),
            (1, False))
        self.assertEqual(
            _task_result_failure_state(
                {"ok": False, "cancelled": True}),
            (0, True))

    def test_split_compression_returns_real_totals_and_one_history_row(
            self) -> None:
        stream = mock.Mock()
        results = [
            {"ok": True, "orig_bytes": 100, "new_bytes": 50},
            {"ok": True, "orig_bytes": 100, "new_bytes": 50},
            {"ok": True, "orig_bytes": 100, "new_bytes": 50},
        ]
        with (
            mock.patch.object(
                compress, "compress_video", side_effect=results),
            mock.patch.object(
                autorun, "append_history_entry") as append,
        ):
            result = compress.compress_videos_batch(
                ["a.mp4", "b.mp4", "c.mp4"], stream,
                batch_size=2, channel_name="Channel")

        self.assertTrue(result["ok"])
        self.assertEqual(result["sum_orig"], 300)
        self.assertEqual(result["sum_new"], 150)
        self.assertEqual(result["saved_pct"], 50.0)
        self.assertFalse(result["cancelled"])
        append.assert_called_once()
        self.assertEqual(stream.emit_activity.call_count, 1)

    def test_cancelled_compression_is_not_reported_successful(self) -> None:
        cancel = threading.Event()
        cancel.set()
        result = compress.compress_videos_batch(
            ["a.mp4"], mock.Mock(), cancel_event=cancel)
        self.assertFalse(result["ok"])
        self.assertTrue(result["cancelled"])

    def test_livestream_ignore_rolls_back_when_deferred_save_fails(
            self) -> None:
        old_ids = {"oldvideoid1"}
        entries = [{"video_id": "newvideoid2"}]
        with (
            mock.patch.object(
                livestreams, "_load_ignore", return_value=set(old_ids)),
            mock.patch.object(
                livestreams, "_load", return_value=entries),
            mock.patch.object(
                livestreams, "_save_ignore",
                side_effect=[True, True]) as save_ignore,
            mock.patch.object(
                livestreams, "_save", return_value=False),
            mock.patch.object(livestreams, "_ignore_cache", None),
            mock.patch.object(livestreams, "_ignore_cache_loaded", False),
        ):
            result = livestreams.ignore("newvideoid2")

        self.assertFalse(result)
        self.assertEqual(save_ignore.call_args_list, [
            mock.call({"oldvideoid1", "newvideoid2"}),
            mock.call({"oldvideoid1"}),
        ])

    def test_reset_sync_state_reports_failed_save(self) -> None:
        cfg = {"channels": [{
            "name": "Channel",
            "url": "https://example.test/c",
            "initialized": True,
        }]}
        owner = SimpleNamespace(_reload_config=mock.Mock())
        with (
            mock.patch.object(
                subs_mixin.subs_backend, "get_channel",
                return_value=dict(cfg["channels"][0])),
            mock.patch.object(
                subs_mixin, "update_config", return_value=(0, None)),
        ):
            result = subs_mixin.SubsMixin.subs_reset_sync_state(
                owner, {"url": "https://example.test/c"})

        self.assertFalse(result["ok"])
        self.assertTrue(result["write_blocked"])
        owner._reload_config.assert_not_called()

    def test_sub_minute_filters_render_plainly(self) -> None:
        rows, _label = ytarchiver_config.channels_for_subs_ui({
            "channels": [{
                "name": "Channel",
                "min_duration": 30,
                "max_duration": 59,
            }],
        })
        self.assertEqual(rows[0]["min"], "<1m")
        self.assertEqual(rows[0]["max"], "<1m")

    def test_fts_rebuild_exposes_finished_outcome(self) -> None:
        class ImmediateThread:
            def __init__(self, target, daemon=False):
                self.target = target

            def start(self):
                self.target()

        class Api(IndexMixin):
            def __init__(self):
                self._log_stream = mock.Mock()
                self.services = None

        api = Api()
        with (
            mock.patch(
                "backend.api_mixins.index_mixin.threading.Thread",
                ImmediateThread),
            mock.patch(
                "backend.api_mixins.index_mixin.index_backend"
                ".rebuild_fts_index",
                return_value={"ok": True, "rows_indexed": 42}),
        ):
            result = api.index_rebuild_fts()

        self.assertEqual(result, {"ok": True, "started": True})
        state = api.index_rebuild_fts_state()
        self.assertFalse(state["running"])
        self.assertTrue(state["ok"])
        self.assertEqual(state["rows_indexed"], 42)
        self.assertIsNotNone(state["started_at"])
        self.assertIsNotNone(state["completed_at"])


if __name__ == "__main__":
    unittest.main()
