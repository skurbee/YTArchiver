"""Replay paced channel discovery through the real sync output parser."""

import json
import threading
from contextlib import contextmanager
from types import SimpleNamespace
from unittest import mock

import pytest

from backend.log_stream import _line_is_verbose_only
from backend.sync import core


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

    def run(lines, *, returncode=0, stop=None, simple=True):
        monkeypatch.setattr(core, "popen_ytdlp_process",
                            mock.Mock(return_value=mock.Mock(returncode=returncode, pid=None)))

        def output(*_args):
            for line in lines:
                if callable(line):
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


def test_many_pages_update_one_visible_row_then_clear_at_video_start(replay):
    pages = [f"[youtube:tab] UCfixture page {page}: Downloading API JSON"
             for page in range(1, 52)]
    completed = replay(["[youtube:tab] @DiscoveryFixture: Downloading webpage", *pages,
                        "[youtube] fixture0001: Downloading webpage"])
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
    assert clear_index < video_index
    assert completed.result["errors"] == 0


def test_lazy_paging_resumes_discovery_and_clears_after_final_page(replay):
    completed = replay([
        "[youtube:tab] UCfixture page 1: Downloading API JSON",
        "[youtube] fixture0001: Downloading webpage",
        "[youtube:tab] UCfixture page 2: Downloading API JSON",
        "[download] Finished downloading playlist: Fixture Channel",
    ])
    assert len(discovery_rows(completed.rows)) == 2
    assert len(clear_rows(completed.rows)) == 2
    assert completed.result["errors"] == 0


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
