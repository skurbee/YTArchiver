"""A partial channel walk must not become a completed initial archive."""

import copy
import threading
from contextlib import contextmanager
from types import SimpleNamespace
from unittest import mock

import pytest

from backend.sync import core
from backend.sync.download_commit import CollisionSafePathResult, DownloadCommitResult


@pytest.fixture
def replay(tmp_path, monkeypatch):
    channel = {
        "name": "Traversal Fixture", "folder": "Traversal Fixture",
        "url": "https://www.youtube.com/@TraversalFixture", "mode": "full",
        "initialized": False, "init_complete": False, "sync_complete": False,
        "auto_metadata": False, "auto_transcribe": False,
    }
    cfg = {"channels": [channel], "output_dir": str(tmp_path)}
    folder = tmp_path / channel["folder"]
    folder.mkdir()
    archive = tmp_path / "archive.txt"
    archive.write_text("", encoding="utf-8")
    for name, value in {"find_yt_dlp": "fixture-ytdlp", "load_config": cfg,
                        "config_is_writable": True, "_find_cookie_source": []}.items():
        monkeypatch.setattr(core, name, mock.Mock(return_value=value))
    monkeypatch.setattr(core, "ARCHIVE_FILE", str(archive))

    @contextmanager
    def transaction():
        yield cfg

    monkeypatch.setattr(core, "config_transaction", transaction)
    monkeypatch.setattr(core.time, "sleep", lambda _: None)
    for name in ("write_sync_progress", "_record_recent_download", "_hide_sidecar_win",
                 "_bg_channel_maintenance", "finish_ytdlp_process"):
        monkeypatch.setattr(core, name, mock.Mock())
    monkeypatch.setattr(core, "finalize_collision_safe_bundle",
                        lambda path, _vid: CollisionSafePathResult(True, path, False, False))
    monkeypatch.setattr(core, "commit_download", lambda path, _ch, _title, video_id, **_:
                        DownloadCommitResult(True, path, video_id, 300, True, True, ""))
    for target in ("backend.net.block_if_down", "backend.channel_art.fetch_channel_art",
                   "backend.channel_cache.append_ids", "backend.channel_cache.append_filtered_ids",
                   "backend.archive_scan.update_disk_cache_for_channel", "backend.livestreams.drop",
                   "backend.ytarchiver_config.append_pending_tx_id"):
        monkeypatch.setattr(target, mock.Mock())
    monkeypatch.setattr("backend.utils.check_directory_writable", lambda _: True)
    monkeypatch.setattr("backend.utils.check_disk_space", lambda *_: True)
    monkeypatch.setattr("subprocess.Popen", mock.Mock(side_effect=AssertionError("unexpected process")))
    monkeypatch.setattr("urllib.request.urlopen", mock.Mock(side_effect=AssertionError("unexpected network")))

    def run(passes, *, existing=False, stop=None):
        if existing:
            channel.update(initialized=True, init_complete=True, sync_complete=True)
        monkeypatch.setattr("backend.subs.streams_url",
                            lambda url: url + "/streams" if len(passes) > 1 else None)
        pending = list(passes)
        launches = []

        def launch(cmd, **_):
            spec = pending[0]
            launches.append(cmd)
            if spec.get("launch_error"):
                raise OSError("fixture launch failed")
            pending.pop(0)
            return SimpleNamespace(returncode=spec.get("rc", 0), spec=spec)

        monkeypatch.setattr(core, "popen_ytdlp_process", launch)
        monkeypatch.setattr(core, "start_download_watchdog", lambda proc, *_a, **_kw:
                            SimpleNamespace(stop=mock.Mock(), stop_event=threading.Event(),
                                            stalled={"hit": proc.spec.get("stalled", False)},
                                            output_complete=proc.spec.get("complete", True)))

        def output(proc, _watchdog):
            spec = proc.spec
            if spec.get("download"):
                video_id = spec.get("video_id", "fixture0001")
                media = folder / f"{spec.get('title', 'Fixture')}.mp4"
                media.write_bytes(b"committed fixture video")
                yield f"[download] Destination: {media}\n".encode()
                yield f"DLTRACK:::Fixture:::Traversal Fixture:::20260101:::23:::300:::{video_id}\n".encode()
            for line in spec.get("lines", []):
                yield (line + "\n").encode()
            if stop:
                stop[1].set()

        monkeypatch.setattr(core, "iter_download_output", output)
        stream = mock.Mock(simple_mode=True)
        kwargs = {stop[0]: stop[1]} if stop else {}
        result = core._sync_channel_impl(copy.deepcopy(channel), stream, **kwargs)
        text = "".join(str(segment[0]) for call in stream.emit.call_args_list
                       for segment in call.args[0])
        return SimpleNamespace(result=result, channel=channel, stream=stream, text=text,
                               archive=archive, media=folder / "Fixture.mp4", launches=launches)

    return run


@pytest.mark.parametrize("failure", [
    {"rc": -9, "stalled": True},
    {"rc": 0, "stalled": True},
    {"rc": 0, "complete": False},
    {"rc": 42},
])
@pytest.mark.parametrize("secondary_success", [False, True])
def test_partial_download_failure_stays_incomplete_and_preserves_committed_media(
        replay, failure, secondary_success):
    passes = [dict(failure, download=True)]
    if secondary_success:
        passes.append({"rc": 0})
    run = replay(passes)
    assert not run.result["ok"]
    assert run.result["incomplete"]
    assert run.result["reason"] == "channel_check_incomplete"
    assert run.result["downloaded"] == 1
    assert run.result["errors"] == 1
    assert all(not run.channel[key] for key in ("initialized", "init_complete", "sync_complete"))
    assert run.media.read_bytes() == b"committed fixture video"
    assert "youtube fixture0001" in run.archive.read_text(encoding="utf-8")
    assert "Channel check did not finish" in run.text
    assert "sync again" in run.text
    activity = run.stream.emit_activity.call_args.args[0]
    assert activity["errors"] == "1 error"


def test_incomplete_streams_pass_also_keeps_bootstrap_open(replay):
    run = replay([{"download": True}, {"rc": -9, "stalled": True}])
    assert not run.result["ok"]
    assert not run.channel["init_complete"]


def test_streams_launch_failure_cannot_promote_successful_partial_download(replay):
    run = replay([{"download": True}, {"launch_error": True}])
    assert not run.result["ok"]
    assert run.result["errors"] == 1
    assert run.result["incomplete"]
    assert not run.channel["init_complete"]
    assert run.result["downloaded"] == 1


def test_initial_launch_failure_is_incomplete_with_an_error(replay):
    run = replay([{"launch_error": True}])
    assert not run.result["ok"]
    assert run.result["errors"] == 1
    assert run.result["incomplete"]
    assert not run.channel["init_complete"]


def test_next_sync_keeps_full_walk_and_records_an_older_missing_video(replay):
    first = replay([{"download": True, "rc": -9, "stalled": True}])
    assert not first.channel["init_complete"]
    resumed = replay([{"download": True, "video_id": "fixture0002", "title": "Older"}])
    assert "--break-on-existing" not in resumed.launches[0]
    assert resumed.channel["init_complete"]
    assert resumed.result["downloaded"] == 1
    assert resumed.media.is_file()
    assert resumed.media.with_name("Older.mp4").is_file()
    assert set(resumed.archive.read_text(encoding="utf-8").splitlines()) == {
        "youtube fixture0001", "youtube fixture0002"}


def test_existing_channel_keeps_its_established_state_after_partial_failure(replay):
    run = replay([{"download": True, "rc": -9}], existing=True)
    assert not run.result["ok"]
    assert all(run.channel[key] for key in ("initialized", "init_complete", "sync_complete"))
    assert "--break-on-existing" in run.launches[0]


@pytest.mark.parametrize("signal", ["cancel_event", "pause_event", "kill_current"])
def test_user_interruption_is_not_reported_as_a_process_error_or_completed_bootstrap(replay, signal):
    run = replay([{"download": True, "rc": -9}], stop=(signal, threading.Event()))
    assert not run.channel["init_complete"]
    assert not run.result["incomplete"]
    assert run.result["errors"] == 0
    assert "Channel check did not finish" not in run.text
    assert run.media.is_file()


@pytest.mark.parametrize("rc", [0, 1, 101])
@pytest.mark.parametrize("lines", [
    ["[download] fixture0001: has already been recorded in the archive"],
    ["[youtube] fixture0001: Downloading webpage",
     "[download] Fixture does not pass filter (duration > 180), skipping .."],
])
def test_normal_archived_or_filtered_first_walk_still_graduates(replay, rc, lines):
    run = replay([{"rc": rc, "lines": lines}])
    assert run.result["ok"]
    assert not run.result["incomplete"]
    assert run.result["errors"] == 0
    assert run.channel["initialized"] and run.channel["init_complete"]


def test_empty_first_walk_does_not_graduate(replay):
    run = replay([{}])
    assert not run.channel["init_complete"]
