"""Archive control regressions. All files and profile data are disposable."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
from pathlib import Path
from unittest import mock

import pytest

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-controls-files-")
os.environ["APPDATA"] = _PROFILE.name
os.environ["LOCALAPPDATA"] = _PROFILE.name

from backend import index, redownload, reorg, subscriber_counts  # noqa: E402
from backend.api_mixins.video_mixin import VideoMixin  # noqa: E402


def test_reorg_preserves_different_sidecars_even_when_media_are_identical(tmp_path):
    source, destination = tmp_path / "source", tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    for folder in (source, destination):
        (folder / "Video.mp4").write_bytes(b"identical media")
    (source / "Video.jsonl").write_text("unique source transcript")
    (destination / "Video.jsonl").write_text("different destination transcript")
    stream = mock.Mock()
    assert not reorg._move_video(source / "Video.mp4", destination, stream)
    assert (source / "Video.mp4").exists()
    assert (destination / "Video.mp4").exists()
    assert (source / "Video.jsonl").read_text() == "unique source transcript"
    assert (destination / "Video.jsonl").read_text() == "different destination transcript"
    stream.emit_error.assert_called_once()


@pytest.mark.parametrize("shared", [False, True])
def test_reorg_exact_duplicate_cleanup_preserves_shared_sidecar(tmp_path, shared):
    source, destination = tmp_path / "source", tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    for folder in (source, destination):
        (folder / "Video.mp4").write_bytes(b"identical media")
        (folder / "Video.jsonl").write_text("same transcript")
    if shared:
        (source / "Video.mkv").write_bytes(b"other format")
    assert reorg._move_video(source / "Video.mp4", destination, mock.Mock())
    assert not (source / "Video.mp4").exists()
    assert (destination / "Video.jsonl").read_text() == "same transcript"
    assert (source / "Video.jsonl").exists() is shared


def test_reorg_refuses_same_size_same_mtime_media_with_different_tail(tmp_path):
    source, destination = tmp_path / "source", tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    a, b = source / "Video.mp4", destination / "Video.mp4"
    a.write_bytes(b"x" * (1536 * 1024) + b"a")
    b.write_bytes(b"x" * (1536 * 1024) + b"b")
    os.utime(a, (1_700_000_000, 1_700_000_000))
    os.utime(b, (1_700_000_000, 1_700_000_000))
    assert not reorg._move_video(a, destination, mock.Mock())
    assert a.exists() and b.exists()


@pytest.mark.parametrize("scope,suffix", [
    ({"year": "2024"}, Path("2024")),
    ({"year": "2024", "month": 3}, Path("2024") / "03 March"),
])
def test_redownload_scope_survives_current_channel_resolution(tmp_path, monkeypatch, scope, suffix):
    current = tmp_path / "Current"
    channel = {"name": "Channel", "folder_override": "Current", "url": "https://youtube.com/@Scope"}
    monkeypatch.setattr(redownload, "load_config", lambda: {"output_dir": str(tmp_path), "channels": [channel]})
    result = redownload._resolve_redownload_target(
        "Channel", channel["url"], str(tmp_path / "Old" / suffix), "", scope)
    assert Path(result[2]) == current / suffix


def test_legacy_scoped_folder_is_not_broadened(tmp_path, monkeypatch):
    channel = {"name": "Channel", "url": "https://youtube.com/@Scope"}
    root = tmp_path / "Channel"
    monkeypatch.setattr(redownload, "load_config", lambda: {"output_dir": str(tmp_path), "channels": [channel]})
    result = redownload._resolve_redownload_target("Channel", channel["url"], str(root / "2024"), "")
    assert Path(result[2]) == root / "2024"


@pytest.mark.parametrize("scope", [{"year": ".."}, {"year": "2024", "month": 13}, {"year": "a/b"}, {"month": 1}])
def test_invalid_scope_cannot_fall_back_to_whole_channel(tmp_path, scope):
    with pytest.raises(ValueError):
        redownload.scoped_redownload_folder(str(tmp_path), scope)


def test_same_basename_in_separate_years_keeps_both_physical_files(tmp_path):
    for year in ("2023", "2024"):
        folder = tmp_path / year
        folder.mkdir()
        (folder / "Video [abc123def45].mp4").write_bytes(b"media")
    files = redownload._scan_local_files(str(tmp_path))
    assert len(files) == 2
    matches = redownload._match_files_to_ids(files, {})
    assert {row["filepath"] for row in matches} == set(files.values())
    assert {row["video_id"] for row in matches} == {"abc123def45"}


def test_metadata_fallback_does_not_match_nested_files_twice(tmp_path, monkeypatch):
    known = tmp_path / "2024" / "Known [abc123def45].mp4"
    unresolved = tmp_path / "2025" / "Mystery.mp4"
    for path in (known, unresolved):
        path.parent.mkdir()
        path.write_bytes(b"fixture media")
    monkeypatch.setattr(redownload, "block_if_down", lambda **_kwargs: True)
    monkeypatch.setattr(redownload, "_fetch_yt_catalog", lambda *_args, **_kwargs: {"Known": "abc123def45"})
    monkeypatch.setattr(redownload, "_build_index_filepath_map", lambda _folder: {})
    monkeypatch.setattr(redownload, "_ffprobe_embedded_video_id", lambda *_args: "")
    monkeypatch.setattr(redownload, "_build_metadata_index", lambda _folder: {
        "by_title": {"Mystery": "def123abc45"}, "by_date": {},
        "by_id": {"def123abc45": {"title": "Mystery"}},
    })
    actual_match = redownload._match_files_to_ids
    passes = []
    def record_match(files, *_args, **kwargs):
        matched = actual_match(files, *_args, **kwargs)
        passes.append((dict(files), list(matched)))
        return matched
    monkeypatch.setattr(redownload, "_match_files_to_ids", record_match)

    class MatchingComplete(Exception):
        pass

    # Stop at the next phase boundary, before persistence or download work.
    monkeypatch.setattr(redownload, "_load_progress_state", mock.Mock(side_effect=MatchingComplete))
    with pytest.raises(MatchingComplete):
        redownload._redownload_channel_impl("Channel", "https://youtube.com/@Channel", str(tmp_path), "720", mock.Mock(), threading.Event())
    assert len(passes) == 2
    assert set(passes[1][0].values()) == {str(unresolved)}
    matched_paths = [row["filepath"] for _files, rows in passes for row in rows]
    assert matched_paths == [str(known), str(unresolved)]


@pytest.mark.parametrize("dimensions", [(1280, 720, ""), (None, None, "")])
def test_source_metadata_cannot_override_local_resolution(tmp_path, monkeypatch, dimensions):
    path = tmp_path / "Video.mp4"
    path.write_bytes(b"media")
    monkeypatch.setattr(redownload, "_ffprobe_media_info", lambda _path: dimensions)
    metadata = mock.Mock(return_value=1080)
    monkeypatch.setattr(redownload, "_height_from_metadata_jsonl", metadata)
    assert not redownload._already_at_target(str(path), "1080")
    metadata.assert_not_called()


def test_video_redownload_retains_selected_nonprimary_copy(tmp_path, monkeypatch):
    old, primary = tmp_path / "older.mp4", tmp_path / "primary.mp4"
    old.write_bytes(b"old")
    primary.write_bytes(b"primary")
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE videos(video_id TEXT, filepath TEXT, channel TEXT, is_duplicate_of INTEGER)")
    conn.executemany("INSERT INTO videos VALUES (?,?,?,?)", [
        ("abc123def45", str(old), "Channel", 2),
        ("abc123def45", str(primary), "Channel", None),
    ])
    monkeypatch.setattr(index, "_reader_open", lambda: conn)
    api = VideoMixin()
    api._video_config = lambda: {"output_dir": str(tmp_path), "channels": [{"name": "Channel", "url": "https://youtube.com/@Channel"}]}
    api.chan_redownload = mock.Mock(return_value={"ok": True})
    assert api.video_redownload("abc123def45", "Video", "720", str(old))["ok"]
    assert api.chan_redownload.call_args.kwargs["only_video"]["filepath"] == str(old)
    api.chan_redownload.reset_mock()
    assert api.video_redownload("abc123def45", "Video", "720")["ok"]
    assert api.chan_redownload.call_args.kwargs["only_video"]["filepath"] == str(primary)
    api.chan_redownload.reset_mock()
    assert not api.video_redownload("differentid", "Video", "720", str(old))["ok"]
    old.unlink()
    assert not api.video_redownload("abc123def45", "Video", "720", str(old))["ok"]
    api.chan_redownload.assert_not_called()
    conn.close()


def test_subscriber_count_root_probe_and_video_fallback_support_streams_only_channel():
    runner = mock.Mock()
    runner.binary.return_value = "fake-ytdlp"
    runner.build_argv.side_effect = lambda *args, **_kwargs: list(args)
    runner.run_capture.side_effect = [(0, "NA", ""), (0, "123456", "")]
    result = subscriber_counts.fetch_subscriber_count("https://www.youtube.com/@Streams/streams", runner=runner)
    assert result == {"ok": True, "count": 123456}
    calls = runner.run_capture.call_args_list
    assert len(calls) == 2
    assert all(call.args[0][-1] == "https://www.youtube.com/@Streams" for call in calls)
    assert subscriber_counts._is_transient("This channel does not have a videos tab")
