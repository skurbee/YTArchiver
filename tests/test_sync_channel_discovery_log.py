"""Replay paced channel discovery through the real sync output parser."""

import json
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from types import SimpleNamespace
from unittest import mock

import pytest

from backend.log_stream import _line_is_verbose_only
from backend.sync import core


@dataclass(frozen=True)
class DiscoveryCheckpoint:
    visible: bool


def discovery_visible(rows):
    active = set()
    for row in rows:
        for text, tags in row:
            if tags == "__control__":
                control = json.loads(text)
                if control.get("kind") == "clear_line":
                    active.discard(control.get("marker"))
            elif isinstance(tags, list):
                active.update(tag for tag in tags if tag.startswith("sync_row_discovery_"))
    return bool(active)


@pytest.fixture
def replay(monkeypatch, tmp_path):
    channel = {"name": "Fixture Channel", "url": "https://www.youtube.com/@DiscoveryFixture",
               "mode": "new", "initialized": True, "init_complete": True,
               "auto_metadata": False, "auto_transcribe": False}
    cfg = {"output_dir": str(tmp_path), "channels": [channel]}
    for name, value in {"find_yt_dlp": "fixture-ytdlp", "load_config": cfg,
                        "_find_cookie_source": [], "config_is_writable": False}.items():
        monkeypatch.setattr(core, name, mock.Mock(return_value=value))
    monkeypatch.setattr(core, "ARCHIVE_FILE", str(tmp_path / "archive.txt"))
    for name in ("write_sync_progress", "_bg_channel_maintenance", "finish_ytdlp_process"):
        monkeypatch.setattr(core, name, mock.Mock())
    monkeypatch.setattr("backend.subs.streams_url", lambda _url: None)
    monkeypatch.setattr("backend.utils.check_directory_writable", lambda _path: True)
    monkeypatch.setattr("backend.utils.check_disk_space", lambda *_: True)
    monkeypatch.setattr("backend.net.block_if_down", mock.Mock())
    monkeypatch.setattr("backend.channel_art.fetch_channel_art", mock.Mock(return_value={"ok": True}))
    monkeypatch.setattr("subprocess.Popen", mock.Mock(side_effect=AssertionError("unexpected process")))
    monkeypatch.setattr("urllib.request.urlopen", mock.Mock(side_effect=AssertionError("unexpected network")))
    watchdog = SimpleNamespace(last_output=[0], stop=mock.Mock(), stop_event=threading.Event(),
                               stalled={}, output_complete=True)
    monkeypatch.setattr(core, "start_download_watchdog", mock.Mock(return_value=watchdog))

    @contextmanager
    def transaction():
        yield cfg

    monkeypatch.setattr(core, "config_transaction", transaction)

    def run(lines, *, returncode=0, stop=None, simple=True,
            streams_lines=None, launch_effect=None):
        monkeypatch.setattr(core, "popen_ytdlp_process",
                            mock.Mock(side_effect=launch_effect,
                                      return_value=mock.Mock(returncode=returncode, pid=None)))
        passes = [lines]
        if streams_lines is not None:
            monkeypatch.setattr("backend.subs.streams_url", lambda url: url + "/streams")
            passes.append(streams_lines)
        output_passes = iter(passes)

        def output(*_args):
            for line in next(output_passes):
                if isinstance(line, DiscoveryCheckpoint):
                    rows = [call.args[0] for call in stream.emit.call_args_list]
                    assert discovery_visible(rows) is line.visible
                elif callable(line):
                    line()
                else:
                    yield (line + "\n").encode()

        monkeypatch.setattr(core, "iter_download_output", output)
        stream = mock.Mock(simple_mode=simple)
        result = core._sync_channel_impl(channel, stream, **(stop or {}))
        rows = [call.args[0] for call in stream.emit.call_args_list]
        return SimpleNamespace(result=result, rows=rows, channel=channel)

    return run


def discovery_rows(rows):
    return [row for row in rows if any(
        isinstance(segment[1], list)
        and any(tag.startswith("sync_row_discovery_") for tag in segment[1])
        for segment in row)]


def clear_rows(rows):
    return [json.loads(row[0][0]) for row in rows
            if len(row) == 1 and row[0][1] == "__control__"
            and json.loads(row[0][0]).get("marker", "").startswith("sync_row_discovery_")]


def error_rows(rows):
    return [row for row in rows if any("error_detail" in segment[1] for segment in row)]


def test_many_pages_update_one_visible_row_until_actual_media_download(replay):
    pages = [f"[youtube:tab] UCfixture page {page}: Downloading API JSON"
             for page in range(1, 52)]
    completed = replay(["[youtube:tab] @DiscoveryFixture: Downloading webpage", *pages,
                        "[youtube] fixture0001: Downloading webpage",
                        DiscoveryCheckpoint(True),
                        "[info] fixture0001: Downloading 1 format(s)",
                        DiscoveryCheckpoint(True),
                        "[download] Destination: Fixture.f137.mp4",
                        DiscoveryCheckpoint(False)])
    discovery = discovery_rows(completed.rows)
    assert len(discovery) == 52
    assert len({row[0][1][-1] for row in discovery}) == 1
    assert all(not _line_is_verbose_only(row) for row in discovery)
    assert "page 51" in discovery[-1][0][0]
    assert len(clear_rows(completed.rows)) == 1
    clear_index = next(i for i, row in enumerate(completed.rows)
                       if row[0][1] == "__control__")
    video_index = next(i for i, row in enumerate(completed.rows)
                       if "[youtube] fixture0001" in row[0][0])
    download_index = next(i for i, row in enumerate(completed.rows)
                          if any("Downloading " in text and isinstance(tags, list)
                                 and "simpleline_green" in tags for text, tags in row))
    assert video_index < clear_index
    assert clear_index + 1 == download_index
    assert completed.result["errors"] == 0


def test_lazy_paging_resumes_discovery_and_clears_after_final_page(replay):
    completed = replay([
        "[youtube:tab] UCfixture page 1: Downloading API JSON",
        "[youtube] fixture0001: Downloading webpage",
        "[download] Destination: Fixture.f137.mp4",
        DiscoveryCheckpoint(False),
        "[youtube:tab] UCfixture page 2: Downloading API JSON",
        DiscoveryCheckpoint(True),
        "[download] Finished downloading playlist: Fixture Channel",
    ])
    assert len(discovery_rows(completed.rows)) == 2
    assert len(clear_rows(completed.rows)) == 2
    assert completed.result["errors"] == 0


def test_metadata_skips_sidecars_and_nested_playlist_keep_discovery_visible(replay):
    completed = replay([
        "[youtube:tab] @DiscoveryFixture: Downloading webpage",
        "[youtube] fixture0001: Downloading webpage",
        "[info] fixture0001: Downloading 1 format(s)",
        "[download] fixture0001: has already been recorded in the archive",
        DiscoveryCheckpoint(True),
        "[download] Destination: Fixture.en.vtt",
        "[download] Destination: Fixture.info.json",
        DiscoveryCheckpoint(True),
        "[download] Fixture.mp4 has already been downloaded",
        "DLTRACK:::Fixture:::Fixture Channel:::20260101:::1:::1:::fixture0001",
        DiscoveryCheckpoint(True),
        "[download] Finished downloading playlist: Fixture Videos",
        DiscoveryCheckpoint(True),
        "[youtube:tab] UCfixture page 2: Downloading API JSON",
    ])
    discovery = discovery_rows(completed.rows)
    assert len(discovery) == 2
    assert discovery[0][0][1][-1] == discovery[1][0][1][-1]
    assert len(clear_rows(completed.rows)) == 1
    clear_index = next(i for i, row in enumerate(completed.rows)
                       if row[0][1] == "__control__")
    assert completed.rows.index(discovery[-1]) < clear_index
    assert completed.result["downloaded"] == 0
    assert completed.result["errors"] == 0


def test_discovery_persists_across_channel_videos_and_streams_passes(replay):
    completed = replay([
        "[youtube:tab] @DiscoveryFixture: Downloading webpage",
        "[youtube] fixture0001: Downloading webpage",
        "[download] fixture0001: has already been recorded in the archive",
        "[download] Finished downloading playlist: Fixture Videos",
    ], streams_lines=[
        DiscoveryCheckpoint(True),
        "[youtube:tab] @DiscoveryFixture/streams: Downloading webpage",
        "[download] Finished downloading playlist: Fixture Streams",
    ])
    discovery = discovery_rows(completed.rows)
    assert len(discovery) == 2
    assert len({row[0][1][-1] for row in discovery}) == 1
    assert len(clear_rows(completed.rows)) == 1
    clear_index = next(i for i, row in enumerate(completed.rows)
                       if row[0][1] == "__control__")
    streams_finished = next(i for i, row in enumerate(completed.rows)
                            if "Finished downloading playlist: Fixture Streams" in row[0][0])
    assert streams_finished < clear_index


@pytest.mark.parametrize("event_name", ["pause_event", "cancel_event"])
def test_stop_during_next_pass_launch_clears_previous_discovery(replay, monkeypatch, event_name):
    stopped = threading.Event()
    monkeypatch.setattr(core.time, "sleep", mock.Mock())
    launches = 0

    def launch(*_args, **_kwargs):
        nonlocal launches
        launches += 1
        if launches == 1:
            return mock.Mock(returncode=0, pid=None)
        stopped.set()
        raise OSError("launch interrupted")

    completed = replay([
        "[youtube:tab] @DiscoveryFixture: Downloading webpage",
    ], streams_lines=[], launch_effect=launch, stop={event_name: stopped})
    assert launches == 2
    assert len(discovery_rows(completed.rows)) == 1
    assert len(clear_rows(completed.rows)) == 1
    assert completed.result["reason"] == ("paused" if event_name == "pause_event" else "cancelled")


def test_next_pass_launch_failure_clears_previous_discovery(replay, monkeypatch):
    monkeypatch.setattr(core.time, "sleep", mock.Mock())
    completed = replay([
        "[youtube:tab] @DiscoveryFixture: Downloading webpage",
    ], streams_lines=[], launch_effect=[
        mock.Mock(returncode=0, pid=None),
        OSError("launch failed"), OSError("launch failed"), OSError("launch failed"),
    ])
    assert len(discovery_rows(completed.rows)) == 1
    assert len(clear_rows(completed.rows)) == 1


@pytest.mark.parametrize("event_name", [None, "cancel_event", "pause_event", "kill_current"])
def test_discovery_row_is_cleared_on_eof_and_stop(replay, event_name):
    stopped = threading.Event()
    lines = ["[youtube:tab] UCfixture page 8: Downloading API JSON"]
    if event_name:
        lines.extend([stopped.set, "[youtube:tab] UCfixture page 9: Downloading API JSON"])
    completed = replay(lines, stop={event_name: stopped} if event_name else None)
    assert len(discovery_rows(completed.rows)) == 1
    assert len(clear_rows(completed.rows)) == 1


@pytest.mark.parametrize("error", [
    "ERROR: Response.code is deprecated, use Response.status",
    "ERROR: [youtube:tab] Unable to download API page: connection timed out",
])
def test_channel_error_keeps_raw_detail_and_does_not_claim_a_failed_video(replay, error):
    completed = replay(["[youtube:tab] UCfixture page 1: Downloading API JSON", error], returncode=1)
    errors = error_rows(completed.rows)
    assert len(errors) == 1
    assert "checking this channel" in errors[0][0][0]
    assert [error, "error_raw"] in errors[0]
    assert "this video" not in errors[0][0][0]
    assert completed.result["errors"] == 1
    assert not completed.channel.get("failed_video_ids")


def test_pagination_error_is_not_assigned_to_previous_video(replay):
    completed = replay([
        "[youtube] fixture0001: Downloading webpage",
        "[youtube:tab] UCfixture page 2: Downloading API JSON",
        "ERROR: [youtube:tab] Unexpected channel response",
    ], returncode=1)
    assert "checking this channel" in error_rows(completed.rows)[0][0][0]
    assert not completed.channel.get("failed_video_ids")
    assert completed.result["errors"] == 1


def test_video_error_keeps_video_retry_tracking(replay):
    completed = replay([
        "[youtube:tab] UCfixture page 1: Downloading API JSON",
        "[youtube] fixture0001: Downloading webpage",
        "ERROR: [youtube] fixture0001: Unexpected video response",
    ], returncode=1)
    assert "Couldn't download this video" in error_rows(completed.rows)[0][0][0]
    assert "fixture0001" in completed.channel["failed_video_ids"]
    assert completed.result["errors"] == 1


def test_verbose_mode_preserves_raw_channel_error(replay):
    error = "ERROR: [youtube:tab] Unexpected channel response"
    completed = replay([error], returncode=1, simple=False)
    assert any(error in row[0][0] and row[0][1] == "red" for row in completed.rows)
    assert completed.result["errors"] == 1
