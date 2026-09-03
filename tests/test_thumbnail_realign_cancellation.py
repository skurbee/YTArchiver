from __future__ import annotations

import os
import threading
from pathlib import Path
from unittest import mock

from backend import index
from backend.metadata import thumbnails_ops


class _TrackingLock:
    def __init__(self) -> None:
        self.depth = 0
        self.entries = 0

    @property
    def held(self) -> bool:
        return self.depth > 0

    def __enter__(self):
        self.depth += 1
        self.entries += 1
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.depth -= 1


class _FakeConnection:
    def __init__(self, rows, *, on_execute=None) -> None:
        self.rows = list(rows)
        self.on_execute = on_execute

    def execute(self, _sql, *_args):
        if self.on_execute is not None:
            self.on_execute()
        return iter(self.rows)


def _patch_catalog(monkeypatch, *, root: Path, reader_conn,
                   reader_lock: _TrackingLock | None = None,
                   writer_conn=None,
                   writer_lock: _TrackingLock | None = None) -> tuple[
                       _TrackingLock, _TrackingLock, mock.Mock]:
    reader_lock = reader_lock or _TrackingLock()
    writer_lock = writer_lock or _TrackingLock()
    writer_open = mock.Mock(return_value=writer_conn)
    monkeypatch.setattr(
        thumbnails_ops, "load_config", lambda: {"output_dir": str(root)})
    monkeypatch.setattr(index, "_reader_open", lambda: reader_conn)
    monkeypatch.setattr(index, "_open", writer_open)
    monkeypatch.setattr(index, "_reader_lock", reader_lock)
    monkeypatch.setattr(index, "_db_lock", writer_lock)
    return reader_lock, writer_lock, writer_open


def test_cancel_during_catalog_path_checks_stops_before_the_next_stat(
        tmp_path, monkeypatch) -> None:
    channel_root = tmp_path / "Channel"
    channel_root.mkdir()
    rows = [
        (str(channel_root / "one.mp4"), "ABCDEFGHIJK"),
        (str(channel_root / "two.mp4"), "LMNOPQRSTUV"),
    ]
    _patch_catalog(
        monkeypatch, root=tmp_path, reader_conn=_FakeConnection(rows))
    cancel = threading.Event()
    checked_paths: list[str] = []

    def cancel_after_first_stat(path: str) -> bool:
        checked_paths.append(path)
        cancel.set()
        return True

    monkeypatch.setattr(thumbnails_ops.os.path, "isfile",
                        cancel_after_first_stat)

    result = thumbnails_ops.realign_misplaced_thumbnails(
        [{"name": "Channel"}], dry_run=True, cancel_event=cancel)

    assert result["ok"] is True
    assert result["cancelled"] is True
    assert len(checked_paths) == 1
    assert result["scanned"] == 0


def test_catalog_filesystem_stats_happen_after_reader_lock_is_released(
        tmp_path, monkeypatch) -> None:
    channel_root = tmp_path / "Channel"
    channel_root.mkdir()
    video_path = channel_root / "video.mp4"
    reader_lock = _TrackingLock()
    conn = _FakeConnection([(str(video_path), "ABCDEFGHIJK")])
    _patch_catalog(
        monkeypatch,
        root=tmp_path,
        reader_conn=conn,
        reader_lock=reader_lock,
    )
    stat_calls: list[str] = []

    def observe_stat(path: str) -> bool:
        assert reader_lock.held is False
        stat_calls.append(path)
        return True

    monkeypatch.setattr(thumbnails_ops.os.path, "isfile", observe_stat)

    result = thumbnails_ops.realign_misplaced_thumbnails(
        [{"name": "Channel"}], dry_run=True)

    assert result["ok"] is True
    assert reader_lock.entries == 1
    assert stat_calls == [os.path.normpath(str(video_path))]


def test_cancel_inside_thumbnail_loop_skips_remaining_thumbnails(
        tmp_path, monkeypatch) -> None:
    channel_root = tmp_path / "Channel"
    source_thumbs = channel_root / "Source" / ".Thumbnails"
    target_dir = channel_root / "Target"
    source_thumbs.mkdir(parents=True)
    target_dir.mkdir(parents=True)
    video_ids = ("ABCDEFGHIJK", "LMNOPQRSTUV")
    rows = []
    for position, video_id in enumerate(video_ids, 1):
        (source_thumbs / f"Thumb {position} [{video_id}].jpg").write_bytes(
            b"jpg")
        video_path = target_dir / f"Video {position}.mp4"
        video_path.write_bytes(b"mp4")
        rows.append((str(video_path), video_id))

    _patch_catalog(
        monkeypatch, root=tmp_path, reader_conn=_FakeConnection(rows))
    cancel = threading.Event()
    destination_checks: list[str] = []

    def cancel_after_first_destination(path: str) -> bool:
        destination_checks.append(path)
        cancel.set()
        return False

    monkeypatch.setattr(thumbnails_ops.os.path, "exists",
                        cancel_after_first_destination)

    result = thumbnails_ops.realign_misplaced_thumbnails(
        [{"name": "Channel"}], dry_run=True, cancel_event=cancel)

    assert result["ok"] is True
    assert result["cancelled"] is True
    assert result["scanned"] == 1
    assert result["misaligned"] == 1
    assert len(destination_checks) == 1


def test_catalog_query_uses_writer_lock_when_reader_connection_is_unavailable(
        tmp_path, monkeypatch) -> None:
    channel_root = tmp_path / "Channel"
    channel_root.mkdir()
    reader_lock = _TrackingLock()
    writer_lock = _TrackingLock()
    conn = _FakeConnection(
        [], on_execute=lambda: assert_writer_lock_held(writer_lock))
    _, _, writer_open = _patch_catalog(
        monkeypatch,
        root=tmp_path,
        reader_conn=None,
        reader_lock=reader_lock,
        writer_conn=conn,
        writer_lock=writer_lock,
    )

    result = thumbnails_ops.realign_misplaced_thumbnails(
        [{"name": "Channel"}], dry_run=True)

    assert result["ok"] is True
    writer_open.assert_called_once_with()
    assert writer_lock.entries == 1
    assert reader_lock.entries == 0


def assert_writer_lock_held(lock: _TrackingLock) -> None:
    assert lock.held is True
