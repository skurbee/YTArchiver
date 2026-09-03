"""Focused checks for Health's honest read-only status responses."""

from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-health-tests-")
os.environ.setdefault("APPDATA", _TEST_APPDATA.name)
Path(os.environ["APPDATA"], "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import index  # noqa: E402
from backend.api_mixins import index_mixin, metadata_mixin  # noqa: E402
from backend.metadata import thumbnails_ops  # noqa: E402


class IndexSummaryStatusTests(unittest.TestCase):
    def test_checked_read_reports_open_failure_without_changing_default(self):
        expected_legacy = {
            "segments": 0,
            "videos": 0,
            "channels": 0,
            "bookmarks": 0,
        }
        with mock.patch.object(index, "_open_independent", return_value=None):
            self.assertEqual(index.summary(), expected_legacy)
            self.assertEqual(index.summary(report_errors=True), {
                "available": False,
                "error": "Search index could not be read.",
            })

    def test_checked_read_reports_query_failure_and_closes_connection(self):
        connection = mock.Mock()
        connection.execute.side_effect = sqlite3.OperationalError("busy")

        with mock.patch.object(
                index, "_open_independent", return_value=connection):
            result = index.summary(report_errors=True)

        self.assertEqual(result, {
            "available": False,
            "error": "Search index could not be read.",
        })
        connection.close.assert_called_once_with()

    def test_api_flag_is_opt_in_and_forwarded(self):
        api = index_mixin.IndexMixin()
        with mock.patch.object(
                index_mixin.index_backend, "summary",
                return_value={"available": False, "error": "unavailable"},
        ) as summary:
            result = api.index_summary(True)

        self.assertEqual(result["error"], "unavailable")
        summary.assert_called_once_with(report_errors=True)


class MetadataStatusTests(unittest.TestCase):
    _CHANNELS = [{"name": "Example", "folder": "Example"}]

    def test_bulk_checked_read_reports_open_failure_without_changing_default(self):
        with mock.patch.object(index, "_reader_open", return_value=None), \
                mock.patch.object(index, "_open", return_value=None):
            legacy = thumbnails_ops.count_video_id_status_bulk(
                self._CHANNELS, force=True)
            checked = thumbnails_ops.count_video_id_status_bulk(
                self._CHANNELS, force=True, include_status=True)

        self.assertEqual(legacy, {})
        self.assertEqual(checked, {
            "ok": False,
            "rows": {},
            "error": "Metadata status could not be read.",
        })

    def test_health_opt_in_reports_bulk_failure(self):
        api = metadata_mixin.MetadataMixin()
        api._config = {"channels": list(self._CHANNELS)}
        failed = {
            "ok": False,
            "rows": {},
            "error": "Metadata status could not be read.",
        }

        with mock.patch.object(
                metadata_mixin.archive_scan, "enrich_channels_with_stats"), \
                mock.patch(
                    "backend.metadata.count_video_id_status_bulk",
                    return_value=failed,
                ) as bulk:
            result = api.get_channel_metadata_status(False, True)

        self.assertEqual(result, {
            "available": False,
            "error": "Metadata status could not be read.",
        })
        bulk.assert_called_once_with(
            mock.ANY, force=False, include_status=True)

    def test_default_metadata_caller_keeps_list_and_zero_fallback(self):
        api = metadata_mixin.MetadataMixin()
        api._config = {"channels": list(self._CHANNELS)}

        with mock.patch.object(
                metadata_mixin.archive_scan, "enrich_channels_with_stats"), \
                mock.patch(
                    "backend.metadata.count_video_id_status_bulk",
                    return_value={},
                ) as bulk:
            result = api.get_channel_metadata_status()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id_total"], 0)
        self.assertEqual(result[0]["tx_total"], 0)
        bulk.assert_called_once_with(mock.ANY, force=False)


if __name__ == "__main__":
    unittest.main()
