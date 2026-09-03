"""Regressions for unmanaged catalog rows leaking into Browse > Videos."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from unittest import mock

from backend import index
from backend.api_mixins import browse_mixin, recent_mixin, video_mixin
from backend.services import file_ops


def _catalog_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE videos ("
        "id INTEGER PRIMARY KEY, title TEXT, channel TEXT, filepath TEXT, "
        "video_id TEXT, size_bytes INTEGER, year INTEGER, month INTEGER, "
        "tx_status TEXT, added_ts REAL, upload_ts REAL, view_count INTEGER, "
        "like_count INTEGER, removed_from_yt_ts REAL, duration_s REAL, "
        "downloaded_ts REAL, is_duplicate_of TEXT, availability TEXT)"
    )
    return conn


def _insert_video(
    conn: sqlite3.Connection,
    path: Path,
    title: str,
    *,
    video_id: str | None = None,
    duplicate_of: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO videos (title, channel, filepath, video_id, size_bytes, "
        "tx_status, added_ts, upload_ts, downloaded_ts, availability, "
        "is_duplicate_of) "
        "VALUES (?, 'Fixture Channel', ?, ?, 5, 'pending', 1, 1, 1, "
        "'available', ?)",
        (title, str(path), video_id or f"{title.lower():0<11}"[:11],
         duplicate_of),
    )
    conn.commit()


def test_archive_video_page_excludes_registered_files_outside_current_roots():
    with tempfile.TemporaryDirectory(prefix="yta-orphan-catalog-") as td:
        base = Path(td)
        archive = base / "Archive"
        archive.mkdir()
        archived = archive / "Archived.mp4"
        archived_copy = archive / "Archived copy.mp4"
        outside = base / "UI fixture" / "Outside.mp4"
        outside_primary = base / "UI fixture" / "External primary.mp4"
        outside.parent.mkdir()
        archived.write_bytes(b"video")
        archived_copy.write_bytes(b"video")
        outside.write_bytes(b"video")
        outside_primary.write_bytes(b"video")
        conn = _catalog_connection()
        _insert_video(conn, archived, "Archived")
        _insert_video(conn, outside, "Outside")
        _insert_video(
            conn,
            outside_primary,
            "External primary",
            video_id="shared00001",
        )
        _insert_video(
            conn,
            archived_copy,
            "Archived copy",
            video_id="shared00001",
            duplicate_of=str(outside_primary),
        )

        with mock.patch.object(index, "_reader_open", return_value=conn):
            with index._browse_cache_lock:
                index._all_videos_cache.clear()
            result = index.list_all_videos(
                limit=20,
                include_thumbs=False,
                archive_roots=[str(archive)],
            )

        assert {row["title"] for row in result["rows"]} == {
            "Archived",
            "Archived copy",
        }
        assert result["has_more"] is False
        conn.close()


def test_videos_api_passes_channel_archive_roots_but_not_manual_folder():
    api = recent_mixin.RecentMixin()
    api._config = {
        "output_dir": "C:/Archive/Channels",
        "video_out_dir": "C:/Archive/Manual",
        "tp_archive_roots": ["D:/Imported Archive"],
        "channels": [],
    }
    captured = {}

    def fake_list_all_videos(**kwargs):
        captured.update(kwargs)
        return {"rows": [], "has_more": False, "offset": 0}

    with mock.patch.object(
        recent_mixin.index_backend,
        "list_all_videos",
        side_effect=fake_list_all_videos,
    ):
        result = api.list_all_videos()

    assert result["rows"] == []
    assert captured["archive_roots"] == [
        str(Path("C:/Archive/Channels")),
        str(Path("D:/Imported Archive")),
    ]


def test_existing_unmanaged_catalog_row_is_removed_without_touching_file():
    with tempfile.TemporaryDirectory(prefix="yta-orphan-delete-") as td:
        external = Path(td) / "Outside.mp4"
        external.write_bytes(b"leave me")
        identity = {
            "id": 7,
            "filepath": str(external),
            "video_id": "outside0001",
            "channel": "Fixture Channel",
            "title": "Outside",
        }
        api = video_mixin.VideoMixin()

        with mock.patch.object(
            file_ops,
            "assert_within_managed_roots",
            return_value={
                "ok": False,
                "error": "Refusing to operate on a file outside the archive.",
            },
        ), mock.patch.object(
            index,
            "prepare_media_copy_deletion",
            return_value={"ok": True, "row_identity": identity},
        ), mock.patch.object(
            index,
            "delete_media_copy",
            return_value={"ok": True, "found": True, "videos": 1},
        ) as delete_row, mock.patch.object(
            index,
            "finalize_copy_deletion_preparation",
            return_value={"ok": True},
        ), mock.patch.object(
            file_ops,
            "safe_trash_video_file",
        ) as trash_file, mock.patch.object(
            video_mixin,
            "config_is_writable",
            return_value=False,
        ):
            result = api.video_delete_file(str(external))

        assert result["ok"] is True
        assert result["catalog_entry_removed"] is True
        assert result["external_file_preserved"] is True
        assert external.read_bytes() == b"leave me"
        delete_row.assert_called_once()
        trash_file.assert_not_called()


def test_unregistered_unmanaged_file_remains_rejected():
    with tempfile.TemporaryDirectory(prefix="yta-unmanaged-reject-") as td:
        external = Path(td) / "Outside.mp4"
        external.write_bytes(b"leave me")
        api = video_mixin.VideoMixin()

        with mock.patch.object(
            file_ops,
            "assert_within_managed_roots",
            return_value={
                "ok": False,
                "error": "Refusing to operate on a file outside the archive.",
            },
        ), mock.patch.object(
            index,
            "prepare_media_copy_deletion",
            return_value={"ok": True, "needed": False},
        ), mock.patch.object(index, "delete_media_copy") as delete_row:
            result = api.video_delete_file(str(external))

        assert result["ok"] is False
        assert "outside the archive" in result["error"]
        assert external.read_bytes() == b"leave me"
        delete_row.assert_not_called()


def test_manual_review_bridge_rejects_a_path_not_in_pending_review():
    with tempfile.TemporaryDirectory(prefix="yta-review-guard-") as td:
        external = Path(td) / "Injected.mp4"
        external.write_bytes(b"video")
        api = browse_mixin.BrowseMixin()

        with mock.patch.object(
            api,
            "_manual_review_has_filepath",
            return_value=False,
        ), mock.patch.object(index, "set_manual_video_id") as set_id:
            result = api.manual_backfill_apply_pick(
                str(external), "abc123def45", "Injected", "Injected")

        assert result["ok"] is False
        assert "pending review" in result["error"]
        set_id.assert_not_called()


def test_manual_fallback_registration_requires_managed_or_recent_path():
    api = browse_mixin.BrowseMixin()
    api._config = {
        "recent_downloads": [{"filepath": "C:/Custom/Trusted.mp4"}],
    }
    with mock.patch.object(
        file_ops,
        "assert_within_managed_roots",
        return_value={"ok": False},
    ):
        assert api._manual_registration_path_is_trusted(
            "C:/Custom/Trusted.mp4") is True
        assert api._manual_registration_path_is_trusted(
            "C:/Injected/Untrusted.mp4") is False
