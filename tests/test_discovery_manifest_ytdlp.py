"""Exercise saved playlists through real yt-dlp with all network blocked."""
import contextlib
import io
import socket

import pytest
import yt_dlp
from yt_dlp.extractor.youtube import YoutubeIE

from backend.sync import discovery_manifest as manifests
from backend.sync.discovery_resume import ResumePlan

CHANNEL = "UC" + "A" * 22
IDS = ["vidsaved001", "vidshort001", "vidolder001", "vidlater001"]


@pytest.fixture
def replay(tmp_path, monkeypatch):
    monkeypatch.setattr(manifests.ytarchiver_config, "APP_DATA_DIR", tmp_path / "profile")
    monkeypatch.setattr(manifests.ytarchiver_config, "config_is_writable", lambda: True)
    target = f"https://www.youtube.com/channel/{CHANNEL}/videos"
    key = manifests.context_key(CHANNEL, target, str(tmp_path / "archive"))
    plan = ResumePlan(str(manifests.manifest_path(key)), key, f"DISCOVERY_RESUME_END:::{key}")
    archive = tmp_path / "downloaded.txt"
    archive.write_text(f"youtube {IDS[0]}\n", encoding="utf-8")

    def unexpected_network(*_args, **_kwargs):
        raise AssertionError("unexpected network in saved-playlist replay")

    monkeypatch.setattr(socket.socket, "connect", unexpected_network)
    monkeypatch.setattr(socket, "create_connection", unexpected_network)

    def run(previous_status, *, still_upcoming=False, minimum_duration=180, reuse_saved=False):
        raw = {
            "_type": "playlist", "id": CHANNEL, "channel_id": CHANNEL,
            "entries": [
                {"_type": "url", "ie_key": "Youtube", "id": video_id,
                 "url": f"https://www.youtube.com/watch?v={video_id}",
                 "title": "Fixture video", "duration": 120 if video_id == IDS[1] else 600,
                 "live_status": "not_live"}
                for video_id in IDS
            ],
        }
        raw["entries"][-1].update(duration=0, **previous_status)
        if reuse_saved:
            saved = manifests.load_manifest(key, CHANNEL, target, str(tmp_path / "archive"))
        else:
            saved = manifests.save_manifest(key, CHANNEL, target, str(tmp_path / "archive"), raw)
        assert saved is not None
        assert manifests.load_manifest(key, CHANNEL, target, str(tmp_path / "archive")) == saved
        assert "webpage_url" not in saved and "original_url" not in saved
        extracted = []

        def extract(self, url):
            video_id = self._match_id(url)
            extracted.append(video_id)
            future = still_upcoming and video_id == IDS[-1]
            return {
                "id": video_id, "title": "Fixture video",
                "duration": 120 if video_id == IDS[1] else 600,
                "upload_date": "20200101", "channel_id": CHANNEL,
                "is_live": False, "is_upcoming": future,
                "live_status": "is_upcoming" if future else "was_live",
                "webpage_url": url, "extractor": "youtube", "extractor_key": "Youtube",
                "formats": [{"url": "http://127.0.0.1:9/no-network.mp4",
                             "ext": "mp4", "format_id": "fixture"}],
            }

        monkeypatch.setattr(YoutubeIE, "extract", extract)
        stdout, stderr = io.StringIO(), io.StringIO()
        argv = [
            "--ignore-config", "--simulate", "--no-check-formats", "--no-warnings",
            "--no-quiet", "--ignore-errors", "--lazy-playlist", "--no-clean-info-json",
            "--load-info-json", plan.path,
            "--download-archive", str(archive),
            "--match-filter", f"!is_live & !is_upcoming & duration>?{minimum_duration}",
            "--print", "after_video:DLTRACK:::%(id)s",
            "--print", "playlist:CHTRACK:::%(channel_id)s",
            "--print", plan.print_template,
        ]
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = yt_dlp._real_main(argv)
        return code, extracted, stdout.getvalue(), stderr.getvalue(), key

    run.path = manifests.manifest_path(key)
    return run


@pytest.mark.parametrize("previous_status", [
    {"live_status": "is_upcoming"}, {"live_status": "is_live"},
    {"live_status": "post_live"}, {"is_live": True}, {"is_upcoming": True},
])
def test_real_loaded_playlist_skips_archive_and_short_entries_but_refreshes_broadcast(replay,
                                                                                    previous_status):
    code, extracted, output, errors, key = replay(previous_status)
    assert code == 0, errors
    # These are the request savings: no video extractor call for completed
    # downloads or videos reliably below the current duration filter.
    assert extracted == IDS[2:]
    assert [line for line in output.splitlines() if line.startswith("DLTRACK:::")] == [
        f"DLTRACK:::{video_id}" for video_id in IDS[2:]]
    assert f"CHTRACK:::{CHANNEL}" in output
    assert f"DISCOVERY_RESUME_END:::{key}" in output


def test_real_loaded_playlist_still_defers_a_broadcast_that_remains_upcoming(replay):
    code, extracted, output, errors, key = replay({"live_status": "is_upcoming"},
                                               still_upcoming=True)
    assert code == 0, errors
    assert extracted == IDS[2:]
    assert f"DLTRACK:::{IDS[2]}" in output
    assert f"DLTRACK:::{IDS[-1]}" not in output
    assert f"DISCOVERY_RESUME_END:::{key}" in output


def test_lowered_duration_filter_reconsiders_short_video_from_identical_saved_file(replay):
    code, extracted, _output, errors, _key = replay({"live_status": "is_upcoming"})
    assert code == 0, errors
    assert IDS[1] not in extracted
    original = replay.path.read_bytes()
    original_modified = replay.path.stat().st_mtime_ns

    code, extracted, output, errors, key = replay({}, minimum_duration=60, reuse_saved=True)
    assert code == 0, errors
    assert extracted == IDS[1:]
    assert f"DLTRACK:::{IDS[1]}" in output
    assert f"DISCOVERY_RESUME_END:::{key}" in output
    assert replay.path.read_bytes() == original
    assert replay.path.stat().st_mtime_ns == original_modified
