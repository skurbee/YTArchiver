"""Popup grouping survives fresh processes without browser-local storage."""

import copy
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

KEY = "traffic_expiration_group_minutes"


@pytest.fixture
def grouping_state(tmp_path, monkeypatch):
    from backend import ytarchiver_config as config
    from backend.api_mixins import settings_mixin

    roaming = tmp_path / "Roaming"
    local = tmp_path / "Local"
    profile = roaming / "YTArchiver"
    profile.mkdir(parents=True)
    local.mkdir()
    archive = tmp_path / "archive"
    archive.mkdir()
    monkeypatch.setattr(config, "APP_DATA_DIR", profile)
    monkeypatch.setattr(config, "CONFIG_FILE", profile / "ytarchiver_config.json")
    monkeypatch.setattr(config, "TRANSCRIPTION_DB", profile / "index.db")
    monkeypatch.setattr(config, "_CFG_CACHE", {"sig": None, "data": None})
    monkeypatch.setattr(config, "_save_counter", 0)
    monkeypatch.setattr(config, "_config_writes_suspended_reason", "")
    monkeypatch.setattr(settings_mixin.youtube_traffic, "status",
                        lambda _cfg: {"mode": "balanced"})

    class Api(settings_mixin.SettingsMixin):
        def __init__(self):
            self._log_stream = SimpleNamespace()

        def _reload_config(self):
            pass

    return SimpleNamespace(config=config, api=Api(), archive=archive,
                           roaming=roaming, local=local)


def seed_config(state, **updates):
    cfg = copy.deepcopy(state.config.DEFAULT_CONFIG)
    cfg.update(_migration_v2_pending_tx_ids=True,
               output_dir=str(state.archive), video_out_dir=str(state.archive))
    cfg.update(updates)
    assert state.config.save_config(cfg)


def test_new_install_defaults_to_minute_detail_without_writing(grouping_state):
    state = grouping_state
    assert state.api.settings_load()[KEY] == 1
    assert not state.config.CONFIG_FILE.exists()


def test_existing_config_missing_grouping_needs_no_migration(grouping_state):
    state = grouping_state
    cfg = copy.deepcopy(state.config.DEFAULT_CONFIG)
    cfg.update(_migration_v2_pending_tx_ids=True,
               output_dir=str(state.archive), video_out_dir=str(state.archive))
    cfg.pop(KEY)
    state.config.CONFIG_FILE.write_text(json.dumps(cfg), encoding="utf-8")
    before = state.config.CONFIG_FILE.read_bytes()
    assert state.api.settings_load()[KEY] == 1
    assert state.config.CONFIG_FILE.read_bytes() == before


@pytest.mark.parametrize("minutes", [1, 10, 30, 60])
def test_grouping_round_trips_through_disk_and_fresh_process(grouping_state, minutes):
    state = grouping_state
    seed_config(state, unrelated_marker="preserve")
    assert state.api.settings_save({KEY: minutes})["ok"]
    assert state.api.settings_save({"log_mode": "Verbose"})["ok"]
    assert state.api.settings_load()[KEY] == minutes

    # A new interpreter has no module cache or browser profile to restore from.
    # Explicit isolated paths also keep child-process imports off live AppData.
    env = os.environ.copy()
    env.update(APPDATA=str(state.roaming), LOCALAPPDATA=str(state.local))
    script = """
import json
import sys
from pathlib import Path
from backend import ytarchiver_config as config
assert config.CONFIG_FILE.resolve() == Path(sys.argv[1]).resolve()
from backend.api_mixins import settings_mixin
settings_mixin.youtube_traffic.status = lambda _cfg: {"mode": "balanced"}
loaded = settings_mixin.SettingsMixin().settings_load()
print(json.dumps({"minutes": loaded["traffic_expiration_group_minutes"],
                  "marker": config.load_config()["unrelated_marker"],
                  "log_mode": loaded["log_mode"]}))
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(state.config.CONFIG_FILE)],
        env=env, cwd=Path(__file__).resolve().parents[1],
        capture_output=True, text=True, timeout=30, check=True)
    restored = json.loads(result.stdout.strip().splitlines()[-1])
    assert restored == {"minutes": minutes, "marker": "preserve", "log_mode": "Verbose"}


@pytest.mark.parametrize("invalid", [0, 2, 90, True, False, None, "10", 10.0, [], {}])
def test_invalid_save_rejected_before_any_config_changes(grouping_state, monkeypatch, invalid):
    state = grouping_state
    seed_config(state, **{KEY: 30, "log_mode": "Simple"})
    before = state.config.CONFIG_FILE.read_bytes()

    def unexpected_config_access():
        pytest.fail("Invalid grouping must be rejected before reading mutable config")

    monkeypatch.setattr(state.api, "_settings_fresh_config", unexpected_config_access)
    result = state.api.settings_save({KEY: invalid, "log_mode": "Verbose"})
    assert result["ok"] is False
    assert "1, 10, 30, or 60 minutes" in result["error"]
    assert state.config.CONFIG_FILE.read_bytes() == before


@pytest.mark.parametrize("invalid", [0, 2, 90, True, False, None, "10", 10.0, [], {}])
def test_malformed_stored_grouping_displays_minute_detail_without_rewriting(
        grouping_state, invalid):
    state = grouping_state
    seed_config(state, **{KEY: invalid})
    before = state.config.CONFIG_FILE.read_bytes()
    assert state.api.settings_load()[KEY] == 1
    assert state.config.CONFIG_FILE.read_bytes() == before


def test_failed_save_preserves_last_saved_grouping(grouping_state, monkeypatch):
    state = grouping_state
    seed_config(state, **{KEY: 30})
    before = state.config.CONFIG_FILE.read_bytes()
    monkeypatch.setattr(state.config, "save_config", lambda _cfg: False)
    result = state.api.settings_save({KEY: 60})
    assert result["ok"] is False
    assert state.config.CONFIG_FILE.read_bytes() == before
    assert state.api.settings_load()[KEY] == 30


@pytest.mark.parametrize("stored, selected", [(10.0, 10), (30.0, 30), (60.0, 60), (True, 1)])
def test_valid_selection_repairs_equal_but_malformed_stored_type(
        grouping_state, stored, selected):
    state = grouping_state
    seed_config(state, **{KEY: stored})
    assert state.api.settings_load()[KEY] == 1

    assert state.api.settings_save({KEY: selected})["ok"]

    saved = json.loads(state.config.CONFIG_FILE.read_text(encoding="utf-8"))[KEY]
    assert type(saved) is int
    assert saved == selected
    state.config._CFG_CACHE.update(sig=None, data=None)
    assert state.api.settings_load()[KEY] == selected
