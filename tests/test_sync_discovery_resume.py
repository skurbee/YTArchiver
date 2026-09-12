"""Cached traversal uses ordinary durable commits and cannot hide interruption."""
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from backend.sync import core
from backend.sync import discovery_resume as resume
from backend.sync.discovery_resume import ResumePlan
from tests.test_sync_incomplete_traversal import replay as replay


@pytest.fixture
def cached(replay, monkeypatch, tmp_path):
    plan = ResumePlan(path=str(tmp_path / "saved-list.json"), key="a" * 64,
                      end_marker="DISCOVERY_RESUME_END:::" + "a" * 64)
    prepared = []

    def prepare(channel, target, _output, _yt, _stream, **_kw):
        prepared.append(target)
        if channel.get("init_complete") or target.endswith("/streams"):
            return None
        return plan

    monkeypatch.setattr(core, "prepare_discovery_resume", prepare)
    finish = Mock()
    monkeypatch.setattr(core, "finish_discovery_resume", finish)
    return SimpleNamespace(run=replay, plan=plan, prepared=prepared, finish=finish)


def test_saved_list_runs_before_live_refresh_and_uses_existing_download_options(cached):
    run = cached.run([{"download": True, "lines": [cached.plan.end_marker]}, {}, {}])
    assert run.result["ok"] and run.channel["init_complete"]
    first, refresh, streams = run.launches
    assert first[1] == "--ignore-config"
    assert first[first.index("--load-info-json") + 1] == cached.plan.path
    assert "--no-clean-info-json" in first
    assert "--no-break-on-existing" in first
    assert cached.plan.print_template in first
    assert not any(value.startswith("https://") for value in first)
    assert refresh[-1] == "https://www.youtube.com/@TraversalFixture"
    assert "--break-on-existing" in refresh
    assert streams[-1].endswith("/streams")
    assert "--write-info-json" in first
    assert "--continue" in first
    assert first[first.index("--match-filter") + 1] == refresh[refresh.index("--match-filter") + 1]
    assert cached.prepared == [refresh[-1], streams[-1]]
    cached.finish.assert_called_once_with([cached.plan])


@pytest.mark.parametrize("bad", [
    {}, {"rc": -9}, {"rc": 101}, {"rc": 0, "complete": False},
    {"rc": 0, "stalled": True},
])
def test_saved_list_must_prove_complete_before_any_live_refresh(cached, bad):
    spec = dict(bad, download=True)
    if bad:
        spec["lines"] = [cached.plan.end_marker]
    run = cached.run([spec, {}, {}])
    assert not run.result["ok"]
    assert run.result["incomplete"]
    assert not run.channel["init_complete"]
    assert len(run.launches) == 1
    assert run.media.exists()
    assert "youtube fixture0001" in run.archive.read_text()
    cached.finish.assert_not_called()


def test_pause_then_restart_replays_saved_list_before_channel_requests(cached):
    stop = threading.Event()
    first = cached.run([{"download": True}], stop=("pause_event", stop))
    assert not first.channel["init_complete"]
    assert first.media.exists()
    second = cached.run([
        {"download": True, "video_id": "fixture0002", "title": "Older",
         "lines": [cached.plan.end_marker]}, {}, {},
    ])
    assert "--load-info-json" in second.launches[0]
    assert second.channel["init_complete"]
    assert set(second.archive.read_text().splitlines()) == {
        "youtube fixture0001", "youtube fixture0002"}
    assert second.media.exists() and second.media.with_name("Older.mp4").exists()


@pytest.mark.parametrize("signal", ["cancel_event", "pause_event", "kill_current"])
def test_stop_after_prepare_does_not_launch_or_complete(cached, monkeypatch, signal):
    stop = threading.Event()

    def prepare(*_a, **_kw):
        stop.set()
        return cached.plan

    monkeypatch.setattr(core, "prepare_discovery_resume", prepare)
    run = cached.run([{}], stop=(signal, stop))
    assert not run.launches
    assert not run.channel["init_complete"]
    if signal != "kill_current":
        assert not run.result["ok"]
        assert run.result["reason"] == ("cancelled" if signal == "cancel_event" else "paused")
    cached.finish.assert_not_called()


def test_existing_incremental_channel_keeps_ordinary_path(cached):
    run = cached.run([{"download": True}], existing=True)
    assert "--load-info-json" not in run.launches[0]
    assert "--break-on-existing" in run.launches[0]
    cached.finish.assert_not_called()


def test_fresh_channel_check_failure_preserves_saved_backlog(cached):
    run = cached.run([{"download": True, "lines": [cached.plan.end_marker]},
                      {"rc": -9}, {}])
    assert not run.result["ok"]
    assert not run.channel["init_complete"]
    cached.finish.assert_not_called()


def test_per_video_errors_with_exhausted_list_allow_existing_retry_policy(cached):
    run = cached.run([{"download": True, "rc": 1, "lines": [cached.plan.end_marker]}, {}, {}])
    assert run.channel["init_complete"]
    assert len(run.launches) == 3


def test_unavailable_optional_cache_falls_back_to_ordinary_traversal(cached, monkeypatch):
    monkeypatch.setattr(core, "prepare_discovery_resume", Mock(side_effect=OSError("cache unavailable")))
    run = cached.run([{"download": True}])
    assert run.result["ok"]
    assert "--load-info-json" not in run.launches[0]
    cached.finish.assert_not_called()


def test_persisted_manifest_is_reused_by_actual_core_preparation(cached, monkeypatch, tmp_path):
    channel = core.load_config.return_value["channels"][0]
    channel_id = "UC" + "a" * 22
    target = f"https://www.youtube.com/channel/{channel_id}"
    channel.update(channel_id=channel_id, url=target, last_sync="2026-01-01")
    destination = str(tmp_path / channel["folder"])
    monkeypatch.setattr(resume.store.ytarchiver_config, "APP_DATA_DIR", tmp_path / "profile")
    monkeypatch.setattr(resume.store.ytarchiver_config, "config_is_writable", lambda: True)
    monkeypatch.setattr(resume.channel_cache, "get_known_ids", lambda *_a, **_kw: [])
    key = resume.store.context_key(channel_id, target, destination)
    saved = resume.store.save_manifest(key, channel_id, target, destination, {
        "_type": "playlist", "id": channel_id, "entries": [{
            "_type": "url", "id": "fixture0001", "ie_key": "Youtube",
            "url": "https://www.youtube.com/watch?v=fixture0001", "duration": 300,
        }],
    })
    assert saved is not None
    monkeypatch.setattr(core, "prepare_discovery_resume", resume.prepare_discovery_resume)
    listing = Mock(side_effect=AssertionError("saved list must avoid discovery requests"))
    monkeypatch.setattr(resume, "popen_ytdlp_process", listing)
    plan = resume.ResumePlan(str(resume.store.manifest_path(key)), key,
                             f"DISCOVERY_RESUME_END:::{key}")
    run = cached.run([{"download": True, "lines": [plan.end_marker]}, {}, {}])
    assert run.result["ok"] and run.channel["init_complete"]
    assert run.launches[0][run.launches[0].index("--load-info-json") + 1] == plan.path
    assert run.launches[1][-1] == target
    listing.assert_not_called()
    cached.finish.assert_called_once_with([plan])
