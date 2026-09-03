"""Focused contracts for single-video metadata refresh modes.

These tests deliberately stub every network/process boundary.  They protect
the saved metadata fields that are outside a scoped refresh and verify that a
forced thumbnail refresh really replaces an already-valid image.
"""

from __future__ import annotations

import copy
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from backend import local_fileserver, thumbnails
from backend.api_mixins import browse_mixin
from backend.metadata import fetcher

VIDEO_ID = "abc123_def4"


class _FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.headers = {"Content-Length": str(len(payload))}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False

    def read(self, _limit: int = -1) -> bytes:
        return self._payload


class VideoMetadataRefreshModeTests(unittest.TestCase):
    def _run_scoped_refresh(
        self,
        *,
        scope: str,
        old_entry: dict,
        fetched_entry: dict,
    ) -> tuple[dict, dict, mock.Mock, mock.Mock, mock.Mock]:
        """Run one scoped refresh against an isolated in-memory sidecar."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / f"Saved video [{VIDEO_ID}].mp4"
            video.write_bytes(b"video")
            jsonl = root / ".Fixture Channel Metadata.jsonl"
            stored = {VIDEO_ID: copy.deepcopy(old_entry)}

            def read_sidecar(_path, *, strict=False):
                del strict
                return copy.deepcopy(stored)

            def write_sidecar(_path, entries):
                stored.clear()
                stored.update(copy.deepcopy(entries))

            with (
                mock.patch.object(fetcher, "find_yt_dlp", return_value="yt-dlp"),
                mock.patch.object(
                    fetcher,
                    "_get_metadata_jsonl_path",
                    return_value=(str(jsonl), str(root)),
                ),
                mock.patch.object(
                    fetcher, "_read_metadata_jsonl", side_effect=read_sidecar
                ),
                mock.patch.object(
                    fetcher, "_write_metadata_jsonl", side_effect=write_sidecar
                ),
                mock.patch.object(
                    fetcher,
                    "_fetch_video_metadata",
                    return_value=copy.deepcopy(fetched_entry),
                ) as fetch_metadata,
                mock.patch.object(fetcher, "_download_thumbnail") as download_thumb,
                mock.patch("backend.index.update_video_stats") as update_stats,
            ):
                result = fetcher.fetch_single_video_metadata(
                    {"name": "Fixture Channel"},
                    VIDEO_ID,
                    str(video),
                    "Saved video",
                    mock.Mock(),
                    emit_inline_log=False,
                    refresh=True,
                    dest_folder=str(root),
                    refresh_scope=scope,
                )

            return (
                result,
                stored[VIDEO_ID],
                fetch_metadata,
                download_thumb,
                update_stats,
            )

    def test_stats_refresh_preserves_non_stat_metadata(self) -> None:
        old_comments = [{"author": "Old", "text": "Keep me", "likes": 3}]
        old = {
            "video_id": VIDEO_ID,
            "title": "Saved title",
            "description": "Saved description",
            "view_count": 10,
            "like_count": 2,
            "comment_count": 1,
            "upload_date": "20250101",
            "duration": 61,
            "thumbnail_url": "https://example.test/saved.jpg",
            "comments": old_comments,
            "fetched_at": "2025-01-01T00:00:00+00:00",
            "future_schema_field": {"must": "survive"},
        }
        fetched = {
            "video_id": VIDEO_ID,
            "title": "Remote title that stats must not adopt",
            "description": "Remote description that stats must not adopt",
            "view_count": 900,
            "like_count": 80,
            "comment_count": 12,
            "upload_date": "20260202",
            "duration": 99,
            "thumbnail_url": "https://example.test/new.jpg",
            "comments": [{"text": "Must not replace saved comments"}],
            "fetched_at": "2026-02-02T00:00:00+00:00",
        }

        result, saved, fetch_metadata, download_thumb, update_stats = (
            self._run_scoped_refresh(
                scope="stats", old_entry=old, fetched_entry=fetched
            )
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["refresh_scope"], "stats")
        self.assertEqual(saved["view_count"], 900)
        self.assertEqual(saved["like_count"], 80)
        self.assertEqual(saved["comment_count"], 12)
        self.assertEqual(saved["fetched_at"], fetched["fetched_at"])
        self.assertEqual(saved["comments"], old_comments)
        self.assertEqual(saved["description"], old["description"])
        self.assertEqual(saved["thumbnail_url"], old["thumbnail_url"])
        self.assertEqual(saved["title"], old["title"])
        self.assertEqual(saved["upload_date"], old["upload_date"])
        self.assertEqual(saved["duration"], old["duration"])
        self.assertEqual(saved["future_schema_field"], old["future_schema_field"])
        self.assertFalse(fetch_metadata.call_args.kwargs["include_comments"])
        download_thumb.assert_not_called()
        update_stats.assert_called_once()

    def test_comments_refresh_preserves_stats_and_other_metadata(self) -> None:
        old = {
            "video_id": VIDEO_ID,
            "title": "Saved title",
            "description": "Saved description",
            "view_count": 321,
            "like_count": 45,
            "comment_count": 1,
            "upload_date": "20250101",
            "duration": 61,
            "thumbnail_url": "https://example.test/saved.jpg",
            "comments": [{"text": "Old comment"}],
            "fetched_at": "2025-01-01T00:00:00+00:00",
            "future_schema_field": ["keep", "this"],
        }
        new_comments = [
            {"author": "New", "text": "Fresh comment", "likes": 8}
        ]
        fetched = {
            "video_id": VIDEO_ID,
            "title": "Remote title that comments must not adopt",
            "description": "Remote description that comments must not adopt",
            "view_count": 999,
            "like_count": 100,
            "comment_count": 27,
            "upload_date": "20260202",
            "duration": 99,
            "thumbnail_url": "https://example.test/new.jpg",
            "comments": new_comments,
            "fetched_at": "2026-02-02T00:00:00+00:00",
        }

        result, saved, fetch_metadata, download_thumb, update_stats = (
            self._run_scoped_refresh(
                scope="comments", old_entry=old, fetched_entry=fetched
            )
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["refresh_scope"], "comments")
        self.assertEqual(saved["comments"], new_comments)
        self.assertEqual(saved["comment_count"], 27)
        self.assertEqual(saved["fetched_at"], fetched["fetched_at"])
        self.assertEqual(saved["view_count"], old["view_count"])
        self.assertEqual(saved["like_count"], old["like_count"])
        self.assertEqual(saved["description"], old["description"])
        self.assertEqual(saved["thumbnail_url"], old["thumbnail_url"])
        self.assertEqual(saved["title"], old["title"])
        self.assertEqual(saved["upload_date"], old["upload_date"])
        self.assertEqual(saved["duration"], old["duration"])
        self.assertEqual(saved["future_schema_field"], old["future_schema_field"])
        self.assertTrue(fetch_metadata.call_args.kwargs["include_comments"])
        download_thumb.assert_not_called()
        update_stats.assert_not_called()


class BrowseMetadataRefreshRoutingTests(unittest.TestCase):
    class _Api(browse_mixin.BrowseMixin):
        def __init__(self, root: str) -> None:
            self._config = {
                "output_dir": root,
                "channels": [{
                    "name": "Fixture Channel",
                    "url": "https://www.youtube.com/@fixture",
                }],
            }
            self._log_stream = mock.Mock()
            self._push_recent_refresh = mock.Mock()

    def _run_mode(self, mode: str):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        video = root / f"Saved video [{VIDEO_ID}].mp4"
        video.write_bytes(b"video")
        api = self._Api(str(root))
        cancel = mock.Mock()
        cancel.is_set.return_value = False
        payload = {
            "filepath": str(video),
            "video_id": VIDEO_ID,
            "title": "Saved video",
            "channel": "Fixture Channel",
            "mode": mode,
        }

        catalog_patch = mock.patch.object(
            browse_mixin,
            "_catalog_video_identity",
            return_value={
                "video_id": VIDEO_ID,
                "title": "Saved video",
                "channel": "Fixture Channel",
            },
        )
        fetch_patch = mock.patch(
            "backend.metadata.fetch_single_video_metadata",
            return_value={
                "ok": True,
                "entry": {"video_id": VIDEO_ID, "view_count": 42},
            },
        )
        drawer_patch = mock.patch.object(
            api,
            "browse_get_video_metadata",
            return_value={
                "ok": True,
                "meta": {"video_id": VIDEO_ID, "view_count": 42},
            },
        )
        thumbnail_patch = mock.patch.object(
            api,
            "_browse_repair_video_thumbnail_owned",
            return_value={
                "ok": True,
                "thumbnail_url": "http://127.0.0.1/thumb.jpg?t=token&v=2",
            },
        )
        with (
            catalog_patch,
            fetch_patch as metadata_fetch,
            drawer_patch,
            thumbnail_patch as thumbnail_refresh,
        ):
            result = api._browse_refresh_video_metadata_owned(
                payload, cancel_event=cancel
            )

        return {
            "api": api,
            "cancel": cancel,
            "payload": payload,
            "result": result,
            "metadata_fetch": metadata_fetch,
            "thumbnail_refresh": thumbnail_refresh,
        }

    def test_invalid_mode_is_rejected_before_any_refresh_work(self) -> None:
        with (
            mock.patch("backend.metadata.fetch_single_video_metadata") as fetch,
            mock.patch.object(
                self._Api,
                "_browse_repair_video_thumbnail_owned",
            ) as thumbnail,
        ):
            result = self._Api("unused")._browse_refresh_video_metadata_owned(
                {"mode": "everything-ish"}
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "Invalid metadata refresh option.")
        fetch.assert_not_called()
        thumbnail.assert_not_called()

    def test_stats_and_comments_route_only_the_requested_metadata_scope(
        self,
    ) -> None:
        for mode in ("stats", "comments"):
            with self.subTest(mode=mode):
                run = self._run_mode(mode)
                fetch_call = run["metadata_fetch"].call_args

                self.assertEqual(fetch_call.kwargs["refresh_scope"], mode)
                self.assertFalse(fetch_call.kwargs["refresh_thumbnail"])
                self.assertTrue(fetch_call.kwargs["refresh"])
                self.assertIs(fetch_call.kwargs["cancel_event"], run["cancel"])
                run["thumbnail_refresh"].assert_not_called()
                run["api"]._push_recent_refresh.assert_called_once_with(
                    "Fixture Channel"
                )
                self.assertTrue(run["result"]["ok"])
                self.assertEqual(run["result"]["refresh_mode"], mode)
                self.assertNotIn("thumbnail_refreshed", run["result"])

    def test_all_routes_full_metadata_then_forces_thumbnail_refresh(self) -> None:
        run = self._run_mode("all")
        fetch_call = run["metadata_fetch"].call_args

        self.assertEqual(fetch_call.kwargs["refresh_scope"], "all")
        self.assertFalse(fetch_call.kwargs["refresh_thumbnail"])
        self.assertTrue(fetch_call.kwargs["refresh"])
        self.assertIs(fetch_call.kwargs["cancel_event"], run["cancel"])
        run["thumbnail_refresh"].assert_called_once_with(
            {
                "filepath": run["payload"]["filepath"],
                "video_id": VIDEO_ID,
                "title": "Saved video",
                "channel": "Fixture Channel",
                "force": True,
            },
            cancel_event=run["cancel"],
        )
        self.assertTrue(run["result"]["ok"])
        self.assertEqual(run["result"]["refresh_mode"], "all")
        self.assertTrue(run["result"]["thumbnail_refreshed"])
        self.assertIn("&v=2", run["result"]["thumbnail_url"])
        # Successful thumbnail repair owns the cache/grid invalidation.
        run["api"]._push_recent_refresh.assert_not_called()


class ForcedThumbnailRefreshTests(unittest.TestCase):
    def test_cancel_after_first_candidate_failure_skips_fallback_urls(
        self,
    ) -> None:
        permission = [True]
        attempted_urls: list[str] = []

        def fail_first_and_cancel(request, timeout):
            del timeout
            attempted_urls.append(request.full_url)
            permission[0] = False
            raise OSError("first candidate unavailable")

        with tempfile.TemporaryDirectory() as td:
            thumb_dir = Path(td)
            target = thumb_dir / f"Saved title [{VIDEO_ID}].jpg"

            with (
                mock.patch.object(
                    thumbnails,
                    "_thumbnail_url_candidates",
                    return_value=[
                        "https://example.test/first.jpg",
                        "https://example.test/fallback.jpg",
                    ],
                ),
                mock.patch.object(
                    thumbnails.urllib.request,
                    "urlopen",
                    side_effect=fail_first_and_cancel,
                ),
            ):
                result = thumbnails._download_thumbnail(
                    "https://example.test/original.jpg",
                    str(thumb_dir),
                    "Saved title",
                    VIDEO_ID,
                    commit_allowed=lambda: permission[0],
                    force=True,
                )

            self.assertFalse(result)
            self.assertEqual(
                attempted_urls, ["https://example.test/first.jpg"]
            )
            self.assertFalse(target.exists())
            self.assertFalse(Path(f"{target}.tmp").exists())

    def test_force_download_replaces_an_existing_valid_thumbnail(self) -> None:
        old_image = b"\xff\xd8\xff" + (b"old-thumbnail" * 2)
        new_image = b"\xff\xd8\xff" + (b"new-thumbnail" * 2)

        with tempfile.TemporaryDirectory() as td:
            thumb_dir = Path(td)
            target = thumb_dir / f"Saved title [{VIDEO_ID}].jpg"
            target.write_bytes(old_image)

            with (
                mock.patch.object(
                    thumbnails.urllib.request,
                    "urlopen",
                    return_value=_FakeResponse(new_image),
                ) as urlopen,
                mock.patch.object(thumbnails, "_mark_thumbnail_changed") as changed,
                mock.patch("backend.utils.hide_file_win"),
            ):
                skipped = thumbnails._download_thumbnail(
                    "https://example.test/thumb.jpg",
                    str(thumb_dir),
                    "Saved title",
                    VIDEO_ID,
                    force=False,
                )
                self.assertTrue(skipped)
                self.assertEqual(target.read_bytes(), old_image)
                urlopen.assert_not_called()

                refreshed = thumbnails._download_thumbnail(
                    "https://example.test/thumb.jpg",
                    str(thumb_dir),
                    "Saved title",
                    VIDEO_ID,
                    force=True,
                )

            self.assertTrue(refreshed)
            self.assertEqual(target.read_bytes(), new_image)
            self.assertFalse(Path(f"{target}.tmp").exists())
            urlopen.assert_called_once()
            changed.assert_called_once_with(str(target))

    def test_force_local_generation_replaces_an_existing_valid_thumbnail(
        self,
    ) -> None:
        old_image = b"\xff\xd8\xff" + (b"old-local" * 2)
        new_image = b"\xff\xd8\xff" + (b"new-local" * 2)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "Local clip.mp4"
            thumb_dir = root / ".Thumbnails"
            target = thumb_dir / "Local clip.local.jpg"
            video.write_bytes(b"video")
            thumb_dir.mkdir()
            target.write_bytes(old_image)

            def extract_frame(_ffmpeg, _video, output, _seek, **_kwargs):
                Path(output).write_bytes(new_image)
                return True

            with (
                mock.patch.object(thumbnails, "_hide_file_win"),
                mock.patch("backend.utils.hide_file_win"),
                mock.patch.object(
                    thumbnails, "_find_ffmpeg", return_value="ffmpeg"
                ) as find_ffmpeg,
                mock.patch.object(
                    thumbnails,
                    "_extract_thumbnail_frame",
                    side_effect=extract_frame,
                ) as extract,
                mock.patch.object(
                    thumbnails, "_write_h264_color_repair_clip"
                ) as repair_clip,
                mock.patch.object(thumbnails, "_mark_thumbnail_changed") as changed,
            ):
                skipped_path = thumbnails._generate_local_thumbnail(
                    str(video), str(thumb_dir), force=False
                )
                self.assertEqual(skipped_path, os.path.normpath(str(target)))
                self.assertEqual(target.read_bytes(), old_image)
                find_ffmpeg.assert_not_called()

                refreshed_path = thumbnails._generate_local_thumbnail(
                    str(video), str(thumb_dir), force=True
                )

            self.assertEqual(refreshed_path, os.path.normpath(str(target)))
            self.assertEqual(target.read_bytes(), new_image)
            self.assertFalse(Path(f"{target}.tmp").exists())
            find_ffmpeg.assert_called_once()
            extract.assert_called_once()
            repair_clip.assert_not_called()
            changed.assert_called_once_with(str(target))


class LocalFileServerRevisionTests(unittest.TestCase):
    def test_url_keeps_token_and_changes_revision_after_file_replacement(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "Thumbnail with spaces.jpg"
            path.write_bytes(b"thumbnail")

            with (
                mock.patch.object(local_fileserver, "_server_port", 49152),
                mock.patch.object(local_fileserver, "_request_token", "secret token"),
                mock.patch.object(local_fileserver, "_file_revisions", {}),
                mock.patch.object(
                    local_fileserver.time,
                    "time_ns",
                    side_effect=[101, 202],
                ),
            ):
                original = local_fileserver.url_for(str(path))
                local_fileserver.mark_file_changed(str(path))
                first_refresh = local_fileserver.url_for(str(path))
                local_fileserver.mark_file_changed(str(path))
                second_refresh = local_fileserver.url_for(str(path))

        self.assertIn("?t=secret%20token", original)
        self.assertNotIn("&v=", original)
        self.assertIn("?t=secret%20token&v=101", first_refresh)
        self.assertIn("?t=secret%20token&v=202", second_refresh)
        self.assertNotEqual(first_refresh, second_refresh)


if __name__ == "__main__":
    unittest.main()
