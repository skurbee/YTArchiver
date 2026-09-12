"""Backup retention settings and cleanup use only disposable app state."""

import copy
import json
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def retention_state(tmp_path, monkeypatch):
    # The root conftest isolates the profile before application imports.
    from backend import auto_backup
    from backend import ytarchiver_config as config
    from backend.api_mixins import settings_mixin

    profile = tmp_path / "profile"
    profile.mkdir()
    archive = tmp_path / "archive"
    archive.mkdir()
    for module in (config, auto_backup):
        monkeypatch.setattr(module, "APP_DATA_DIR", profile)
        monkeypatch.setattr(module, "CONFIG_FILE", profile / "config.json")
        monkeypatch.setattr(module, "TRANSCRIPTION_DB", profile / "index.db")
    monkeypatch.setattr(auto_backup, "QUEUE_FILE", profile / "queue.json")
    monkeypatch.setattr(auto_backup, "backup_file_entries", lambda: (
        (config.CONFIG_FILE.name, config.CONFIG_FILE),))
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

    return SimpleNamespace(config=config, backup=auto_backup, api=Api(),
                           archive=archive, info=archive / auto_backup.INFO_DIR_NAME)


def seed_config(state, **updates):
    config = copy.deepcopy(state.config.DEFAULT_CONFIG)
    config.update(_migration_v2_pending_tx_ids=True, output_dir=str(state.archive),
                  video_out_dir=str(state.archive))
    config.update(updates)
    assert state.config.save_config(config)
    return config


def seed_backups(state, count=12):
    state.info.mkdir(exist_ok=True)
    backups = []
    for day in range(1, count + 1):
        path = state.info / f"ytarchiver_backup_2020-01-{day:02d}_120000.zip"
        path.write_bytes(f"disposable prior backup {day}".encode())
        backups.append(path)
    return backups


def test_new_install_defaults_to_four_without_writing_config(retention_state):
    state = retention_state
    assert state.config.DEFAULT_CONFIG["auto_backup_keep"] == 4
    assert state.api.settings_load()["auto_backup_keep"] == 4
    assert not state.config.CONFIG_FILE.exists()


def test_existing_config_missing_retention_keeps_four_without_migration(retention_state):
    state = retention_state
    config = copy.deepcopy(state.config.DEFAULT_CONFIG)
    config.update(_migration_v2_pending_tx_ids=True, output_dir=str(state.archive),
                  video_out_dir=str(state.archive))
    config.pop("auto_backup_keep")
    state.config.CONFIG_FILE.write_text(json.dumps(config), encoding="utf-8")
    before = state.config.CONFIG_FILE.read_bytes()
    assert state.api.settings_load()["auto_backup_keep"] == 4
    assert state.config.CONFIG_FILE.read_bytes() == before


@pytest.mark.parametrize("keep", [1, 4, 10])
def test_setting_round_trips_without_backup_or_immediate_cleanup(retention_state, keep):
    state = retention_state
    seed_config(state, unrelated_marker="preserve", backup_include_search_db=False)
    backups = seed_backups(state)
    original = {path: path.read_bytes() for path in backups}

    assert state.api.settings_save({"auto_backup_keep": keep})["ok"]
    assert state.api.settings_save({"log_mode": "Verbose"})["ok"]

    stored = json.loads(state.config.CONFIG_FILE.read_text(encoding="utf-8"))
    assert stored["auto_backup_keep"] == keep
    assert stored["unrelated_marker"] == "preserve"
    assert stored["backup_include_search_db"] is False
    assert state.api.settings_load()["auto_backup_keep"] == keep
    assert {path: path.read_bytes() for path in state.info.iterdir()} == original
    assert stored["last_auto_backup_ts"] == 0


@pytest.mark.parametrize("invalid", [0, 11, True, False, None, "2", 2.5, 4.0, [], {}])
def test_invalid_save_is_rejected_before_partial_changes(retention_state, invalid):
    state = retention_state
    seed_config(state, auto_backup_keep=7, log_mode="Simple")
    before = state.config.CONFIG_FILE.read_bytes()
    result = state.api.settings_save({"auto_backup_keep": invalid, "log_mode": "Verbose"})
    assert result["ok"] is False
    assert "whole number from 1 to 10" in result["error"]
    assert state.config.CONFIG_FILE.read_bytes() == before


@pytest.mark.parametrize("invalid", [0, 11, True, None, "2", 2.5, 4.0, [], {}])
def test_malformed_stored_setting_uses_four_for_display_and_rotation(retention_state, invalid):
    state = retention_state
    seed_config(state, auto_backup_keep=invalid)
    backups = seed_backups(state, 6)
    before = state.config.CONFIG_FILE.read_bytes()
    assert state.api.settings_load()["auto_backup_keep"] == 4
    assert state.backup._rotate_backups(str(state.info), str(state.archive)) == 2
    assert [path for path in backups if path.exists()] == backups[-4:]
    assert state.config.CONFIG_FILE.read_bytes() == before


@pytest.mark.parametrize("keep", [1, 4, 10])
def test_successful_backup_applies_saved_count_and_preserves_manual_exports(
        retention_state, keep):
    state = retention_state
    seed_config(state, auto_backup_keep=keep)
    backups = seed_backups(state)
    manual = state.info / "my_manual_backup.zip"
    lookalike = state.info / "ytarchiver_backup_keep_this.zip"
    for path in (manual, lookalike):
        path.write_bytes(b"preserve")

    result = state.backup.run_backup(str(state.archive))

    assert result["ok"], result
    new_backup = Path(result["path"])
    with zipfile.ZipFile(new_backup) as zipped:
        assert zipped.testzip() is None
        assert state.backup.BACKUP_MANIFEST_NAME in zipped.namelist()
    expected_old = backups[-(keep - 1):] if keep > 1 else []
    assert [path for path in backups if path.exists()] == expected_old
    assert set(state.info.glob("*.zip")) == {
        new_backup, *expected_old, manual, lookalike,
    }
    assert all(path.read_bytes() == b"preserve" for path in (manual, lookalike))
    assert state.config.load_config()["last_auto_backup_path"] == str(new_backup)
    assert f"{keep} scheduled backups are kept" in (
        state.info / state.backup.ABOUT_NAME).read_text(encoding="utf-8")


@pytest.mark.parametrize("cancelled", [False, True])
def test_failed_or_cancelled_backup_preserves_all_existing_backups(
        retention_state, monkeypatch, cancelled):
    state = retention_state
    seed_config(state, auto_backup_keep=1)
    backups = seed_backups(state, 6)
    original = {path: path.read_bytes() for path in backups}

    def fail(*_args, **_kwargs):
        if cancelled:
            raise state.backup.BackupCancelled("cancelled")
        raise OSError("simulated write failure")

    monkeypatch.setattr(state.backup, "build_backup_zip", fail)
    result = state.backup.run_backup(str(state.archive))
    assert result["ok"] is False
    assert bool(result.get("cancelled")) is cancelled
    assert {path: path.read_bytes() for path in backups} == original
    assert state.config.load_config()["last_auto_backup_ts"] == 0
