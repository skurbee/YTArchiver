"""Initial channel art and video downloads share one sequential sync task."""

import importlib
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from backend.services.channel_leases import channel_leases
from backend.sync import core


@pytest.fixture
def sync_flow(monkeypatch, tmp_path):
    channel = {
        "name": "Fixture Channel",
        "url": "https://www.youtube.com/@InitialArtFixture",
        "mode": "new",
        "auto_metadata": False,
        "auto_transcribe": False,
    }
    cfg = {"output_dir": str(tmp_path), "channels": [channel]}
    events = []
    art_leases = []
    video_leases = []
    stream = mock.Mock(simple_mode=True)
    for name, value in {
        "find_yt_dlp": "fixture-ytdlp",
        "load_config": cfg,
        "_find_cookie_source": [],
        "config_is_writable": False,
    }.items():
        monkeypatch.setattr(core, name, mock.Mock(return_value=value))
    monkeypatch.setattr(core, "ARCHIVE_FILE", str(tmp_path / "archive.txt"))
    for name in ("write_sync_progress", "_bg_channel_maintenance", "finish_ytdlp_process"):
        monkeypatch.setattr(core, name, mock.Mock())
    monkeypatch.setattr("backend.subs.streams_url", lambda _url: None)
    monkeypatch.setattr("backend.utils.check_directory_writable", lambda _path: True)
    monkeypatch.setattr("backend.utils.check_disk_space", lambda *_: True)
    monkeypatch.setattr("backend.net.block_if_down", mock.Mock())
    monkeypatch.setattr("subprocess.Popen", mock.Mock(side_effect=AssertionError("unexpected process")))
    monkeypatch.setattr("urllib.request.urlopen", mock.Mock(side_effect=AssertionError("unexpected network")))
    watchdog = SimpleNamespace(last_output=[0], stop=mock.Mock(), stop_event=threading.Event(),
                               stalled={}, output_complete=True)
    monkeypatch.setattr(core, "start_download_watchdog", mock.Mock(return_value=watchdog))

    def launch(*_args, **_kwargs):
        events.append("video start")
        video_leases.append(channel_leases.active_snapshot())
        return mock.Mock(returncode=0, pid=None)

    def output(*_args):
        yield b"[download] Finished downloading playlist: Fixture Channel\n"
        events.append("video finish")

    video = mock.Mock(side_effect=launch)
    monkeypatch.setattr(core, "popen_ytdlp_process", video)
    monkeypatch.setattr(core, "iter_download_output", output)

    def run(*, flags=None, art_result=None, art_action=None, stop=None, queued=False):
        channel.update(flags or {})

        def fetch(_url, folder, **kwargs):
            assert Path(folder).is_dir(), "art ran before output-directory preflight"
            events.append("art start")
            art_leases.append(channel_leases.active_snapshot())
            if art_action is not None:
                art_action(kwargs)
            if isinstance(art_result, Exception):
                events.append("art failed")
                raise art_result
            events.append("art finish")
            return art_result if art_result is not None else {"ok": True}

        art = mock.Mock(side_effect=fetch)
        monkeypatch.setattr("backend.channel_art.fetch_channel_art", art)
        if queued:
            from backend.queues import QueueState
            all_sync = importlib.import_module("backend.sync.sync_all")
            monkeypatch.setattr(all_sync, "load_config", lambda: cfg)
            monkeypatch.setattr(all_sync, "ARCHIVE_FILE", str(tmp_path / "archive.txt"))
            monkeypatch.setattr(all_sync, "clear_sync_progress", mock.Mock())
            monkeypatch.setattr(all_sync, "fire_channel_synced_hook", mock.Mock())
            monkeypatch.setattr(all_sync.channel_identity, "preflight_channel_identity",
                                lambda ch, **_: {"ok": True, "channel": ch})
            monkeypatch.setattr(all_sync, "sync_channel", core._sync_channel_impl)
            queues = QueueState()
            monkeypatch.setattr(queues, "save_now", lambda: True)
            monkeypatch.setattr(queues, "save_debounced", lambda: None)
            monkeypatch.setattr(queues, "_write_resuming_payload", lambda *_a, **_k: True)
            try:
                assert queues.sync_enqueue(channel)
                result = all_sync.sync_all(stream, queues=queues, add_downloads_from_config=False)
                assert queues.current_sync is None
                assert queues.sync_snapshot() == []
            finally:
                queues.mark_orphan()
        else:
            result = core._sync_channel_impl(channel, stream, **(stop or {}))
        return SimpleNamespace(result=result, events=events, art=art, video=video, stream=stream,
                               art_leases=art_leases, video_leases=video_leases)

    return run


@pytest.mark.parametrize("flags", [{}, {"initialized": False, "init_complete": False}])
def test_first_sync_finishes_channel_art_before_starting_video_process(sync_flow, flags):
    completed = sync_flow(flags=flags)
    assert completed.result["ok"] is True
    assert completed.events == ["art start", "art finish", "video start", "video finish"]
    completed.art.assert_called_once()
    assert completed.art.call_args.kwargs["force"] is False


@pytest.mark.parametrize("flags", [
    {"initialized": True},
    {"init_complete": True},
    {"initialized": True, "init_complete": True},
])
def test_established_channel_keeps_its_art_refresh_after_videos(sync_flow, flags):
    completed = sync_flow(flags=flags)
    assert completed.result["ok"] is True
    assert completed.events == ["video start", "video finish", "art start", "art finish"]
    completed.art.assert_called_once()


@pytest.mark.parametrize("art_result", [
    {"ok": False, "error": "Fixture artwork unavailable"},
    OSError("Fixture artwork unavailable"),
])
def test_art_failure_does_not_block_video_sync_or_repeat_the_art_attempt(sync_flow, art_result):
    completed = sync_flow(art_result=art_result)
    assert completed.result["ok"] is True
    completed.art.assert_called_once()
    assert completed.events[0] == "art start"
    assert completed.events[-2:] == ["video start", "video finish"]
    assert any("artwork unavailable" in str(call).lower()
               for call in completed.stream.method_calls)


@pytest.mark.parametrize("blocked", ["rate_limited", "cookie_auth_required"])
def test_art_rate_limit_or_auth_failure_stops_before_video_requests(sync_flow, blocked):
    completed = sync_flow(art_result={"ok": False, blocked: True})
    assert completed.result["ok"] is False
    assert completed.result["reason"] == blocked
    assert completed.result[blocked] is True
    assert completed.result["errors"] == 1
    completed.art.assert_called_once()
    completed.video.assert_not_called()


@pytest.mark.parametrize("event_name,reason", [
    ("cancel_event", "cancelled"),
    ("pause_event", "paused"),
    ("kill_current", "cancelled"),
])
def test_stop_during_initial_art_never_starts_video_process(sync_flow, event_name, reason):
    stopped = threading.Event()
    completed = sync_flow(stop={event_name: stopped}, art_action=lambda _kwargs: stopped.set())
    assert completed.result["ok"] is False
    assert completed.result["reason"] == reason
    assert completed.result["downloaded"] == 0
    assert completed.result["errors"] == 0
    completed.video.assert_not_called()
    completed.art.assert_called_once()


def test_initial_art_receives_linked_cancel_and_pause_controls(sync_flow):
    cancelled = threading.Event()
    paused = threading.Event()
    skipped = threading.Event()
    observed = []

    def check_controls(kwargs):
        observed.append(kwargs["pause_event"] is paused)
        observed.append(not kwargs["cancel_event"].is_set())
        skipped.set()
        observed.append(kwargs["cancel_event"].is_set())

    completed = sync_flow(stop={"cancel_event": cancelled, "pause_event": paused,
                                "kill_current": skipped}, art_action=check_controls)
    completed.video.assert_not_called()
    assert observed == [True, True, True]
    assert completed.result["reason"] == "cancelled"


@pytest.mark.parametrize("event_name", ["cancel_event", "pause_event", "kill_current"])
def test_already_stopped_sync_never_fetches_art_or_starts_video(sync_flow, event_name):
    stopped = threading.Event()
    stopped.set()
    completed = sync_flow(stop={event_name: stopped})
    assert completed.result["ok"] is False
    completed.art.assert_not_called()
    completed.video.assert_not_called()


def test_videos_and_streams_passes_share_the_single_initial_art_attempt(sync_flow, monkeypatch):
    monkeypatch.setattr("backend.subs.streams_url",
                        lambda _url: "https://www.youtube.com/@InitialArtFixture/streams")
    completed = sync_flow()
    assert completed.result["ok"] is True
    completed.art.assert_called_once()
    assert completed.events == ["art start", "art finish", "video start", "video finish",
                                "video start", "video finish"]


def test_output_preflight_failure_does_not_start_art_or_videos(sync_flow, monkeypatch):
    monkeypatch.setattr("backend.utils.check_directory_writable", lambda _path: False)
    completed = sync_flow()
    assert completed.result["reason"] == "write blocked"
    completed.art.assert_not_called()
    completed.video.assert_not_called()


def test_sync_now_keeps_one_sync_lease_across_art_then_videos(sync_flow):
    completed = sync_flow(queued=True)
    assert completed.result["ok"] is True
    assert completed.result["busy"] is None
    assert completed.events == ["art start", "art finish", "video start", "video finish"]
    assert len(completed.art_leases) == len(completed.video_leases) == 1
    assert len(completed.art_leases[0]) == 1
    assert len(completed.video_leases[0]) == 1
    art_lease, video_lease = completed.art_leases[0][0], completed.video_leases[0][0]
    assert art_lease.owner == video_lease.owner == "sync"
    assert art_lease.job_id == video_lease.job_id
    assert art_lease.acquired_at == video_lease.acquired_at
    assert channel_leases.active_snapshot() == ()


def _offline_playlist_runner(command, events):
    """Use yt-dlp's real playlist/filter pipeline with no media I/O."""
    import yt_dlp

    options = yt_dlp.parse_options(["--ignore-config", *command[1:]]).ydl_opts
    assert options["lazy_playlist"] is True

    class OfflineYoutubeDL(yt_dlp.YoutubeDL):
        def urlopen(self, *_args, **_kwargs):
            raise AssertionError("unexpected network request")

        def process_info(self, info):
            events.append(("video", info["id"]))

    return OfflineYoutubeDL(options)


def _playlist_video(video_id, *, duration=300, live_status="not_live"):
    return {
        "id": video_id, "title": video_id,
        "url": f"https://example.invalid/{video_id}.mp4",
        "ext": "mp4", "height": 360, "duration": duration,
        "live_status": live_status,
        "is_live": live_status == "is_live",
        "is_upcoming": live_status == "is_upcoming",
        "extractor": "youtube", "extractor_key": "Youtube",
    }


def _channel_playlist(entries, *, playlist_id="fixture-playlist"):
    return {
        "_type": "playlist", "id": playlist_id, "title": "Fixture Channel",
        "extractor": "youtube:tab", "extractor_key": "YoutubeTab",
        "channel_id": "UC" + "a" * 22, "channel": "Fixture Channel",
        "uploader_id": "@InitialArtFixture",
        "uploader_url": "https://www.youtube.com/@InitialArtFixture",
        "entries": entries,
    }


def test_real_ytdlp_downloads_first_page_before_fetching_next_and_keeps_identity(sync_flow, capsys):
    completed = sync_flow(flags={"channel_id": "UC" + "a" * 22})
    command = completed.video.call_args.args[0]
    assert command[-1] == "https://www.youtube.com/channel/UC" + "a" * 22
    events = []

    def pages():
        events.append(("page", 1))
        yield _playlist_video("fixture0001")
        # Eager list(entries) fails here before ever processing that video.
        assert events[-1] == ("video", "fixture0001")
        events.append(("page", 2))
        yield _playlist_video("fixture0002")

    with _offline_playlist_runner(command, events) as downloader:
        downloader.process_ie_result(_channel_playlist(pages()))

    assert events == [("page", 1), ("video", "fixture0001"),
                      ("page", 2), ("video", "fixture0002")]
    tracks = [core._channel_identity.parse_channel_track_line(line)
              for line in capsys.readouterr().out.splitlines()]
    assert any(track and track["channel_id"] == "UC" + "a" * 22 for track in tracks)


def test_real_lazy_ytdlp_preserves_tab_order_and_live_duration_filters(sync_flow):
    completed = sync_flow(flags={"min_duration": 180, "max_duration": 600})
    events = []
    videos = [_playlist_video("fixture0001", duration=60),
              _playlist_video("fixture0002", live_status="is_live"),
              _playlist_video("fixture0003", live_status="is_upcoming"),
              _playlist_video("fixture0004"),
              _playlist_video("fixture0005", duration=900)]
    streams = [_playlist_video("fixture0006", live_status="was_live")]
    playlists = (_channel_playlist(iter(entries), playlist_id=tab)
                 for tab, entries in (("videos", videos), ("streams", streams)))
    with _offline_playlist_runner(completed.video.call_args.args[0], events) as downloader:
        downloader.process_ie_result(_channel_playlist(playlists))
    assert events == [("video", "fixture0004"), ("video", "fixture0006")]


def test_real_lazy_ytdlp_stops_at_archive_hit_before_later_pages(sync_flow, tmp_path):
    from yt_dlp.utils import ExistingVideoReached

    completed = sync_flow(flags={"initialized": True, "init_complete": True})
    events = []

    def pages():
        events.append(("page", 1))
        yield _playlist_video("fixture0001")
        raise AssertionError("archived channel fetched another page")

    archive = tmp_path / "lazy-playlist-archive.txt"
    archive.write_text("youtube fixture0001\n", encoding="utf-8")
    command = list(completed.video.call_args.args[0])
    command[command.index("--download-archive") + 1] = str(archive)
    with _offline_playlist_runner(command, events) as downloader:
        with pytest.raises(ExistingVideoReached):
            downloader.process_ie_result(_channel_playlist(pages()))
    assert events == [("page", 1)]
