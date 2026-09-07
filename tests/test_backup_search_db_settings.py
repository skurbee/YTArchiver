"""Search database backup preferences use isolated configuration and files."""

import copy
import json
from types import SimpleNamespace

import pytest


@pytest.fixture
def backup_settings(tmp_path, monkeypatch):
    # The root conftest isolates APPDATA before application imports. Keep
    # each test's config and database under its own temporary directory too.
    from backend import ytarchiver_config as config
    from backend.api_mixins import settings_mixin

    monkeypatch.setattr(config, "APP_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "CONFIG_FILE", tmp_path / "config.json")
    monkeypatch.setattr(config, "TRANSCRIPTION_DB", tmp_path / "index.db")
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

    return SimpleNamespace(config=config, api=Api(), api_type=Api)


def _seed_config(config, **updates):
    seed = copy.deepcopy(config.DEFAULT_CONFIG)
    seed["_migration_v2_pending_tx_ids"] = True
    seed.update(updates)
    assert config.save_config(seed)
    return seed


def test_new_install_includes_search_database_by_default(backup_settings):
    config = backup_settings.config
    assert not config.CONFIG_FILE.exists()
    assert config.DEFAULT_CONFIG["backup_include_search_db"] is True
    assert config.load_config()["backup_include_search_db"] is True
    assert backup_settings.api.settings_load()["backup_include_search_db"] is True
    assert not config.CONFIG_FILE.exists()


def test_existing_config_without_preference_includes_database(backup_settings):
    config = backup_settings.config
    seed = copy.deepcopy(config.DEFAULT_CONFIG)
    seed.pop("backup_include_search_db")
    seed["_migration_v2_pending_tx_ids"] = True
    config.CONFIG_FILE.write_text(json.dumps(seed), encoding="utf-8")
    before = config.CONFIG_FILE.read_bytes()

    assert config.load_config()["backup_include_search_db"] is True
    assert backup_settings.api.settings_load()["backup_include_search_db"] is True
    assert config.CONFIG_FILE.read_bytes() == before


def test_preference_round_trip_survives_unrelated_settings_save(backup_settings):
    config = backup_settings.config
    _seed_config(config, unrelated_marker="preserve")
    api = backup_settings.api

    for enabled in (False, True):
        assert api.settings_save({"backup_include_search_db": enabled})["ok"]
        assert api.settings_save({"log_mode": "Verbose"})["ok"]
        stored = json.loads(config.CONFIG_FILE.read_text(encoding="utf-8"))
        assert stored["backup_include_search_db"] is enabled
        assert stored["unrelated_marker"] == "preserve"
        reloaded = backup_settings.api_type().settings_load()
        assert reloaded["backup_include_search_db"] is enabled
        assert reloaded["log_mode"] == "Verbose"


@pytest.mark.parametrize("invalid", ["false", "true", 0, 1, None, [], {}])
def test_invalid_preference_rejected_without_partial_save(backup_settings, invalid):
    config = backup_settings.config
    _seed_config(config, backup_include_search_db=True, log_mode="Simple")
    before = config.CONFIG_FILE.read_bytes()

    result = backup_settings.api.settings_save({
        "backup_include_search_db": invalid,
        "log_mode": "Verbose",
    })

    assert result["ok"] is False
    assert "true or false" in result["error"]
    assert config.CONFIG_FILE.read_bytes() == before


@pytest.mark.parametrize("invalid", ["false", 0, None])
def test_malformed_stored_preference_does_not_silently_exclude_database(
        backup_settings, invalid):
    _seed_config(backup_settings.config, backup_include_search_db=invalid)
    assert backup_settings.api.settings_load()["backup_include_search_db"] is True


def test_size_reports_current_file_bytes_without_opening_sqlite(
        backup_settings, monkeypatch):
    import sqlite3

    def unexpected_connect(*_args, **_kwargs):
        pytest.fail("Settings must only stat the Search database")

    monkeypatch.setattr(sqlite3, "connect", unexpected_connect)
    database = backup_settings.config.TRANSCRIPTION_DB
    database.write_bytes(b"abc")
    assert backup_settings.api.settings_load()["backup_search_db_size_bytes"] == 3

    database.write_bytes(b"x" * 4097)
    assert backup_settings.api.settings_load()["backup_search_db_size_bytes"] == 4097


def test_missing_database_reports_zero_without_creating_it(backup_settings):
    database = backup_settings.config.TRANSCRIPTION_DB
    assert not database.exists()
    assert backup_settings.api.settings_load()["backup_search_db_size_bytes"] == 0
    assert not database.exists()


@pytest.mark.parametrize("error", [PermissionError("denied"), OSError("unavailable")])
def test_unavailable_database_size_is_unknown(backup_settings, monkeypatch, error):
    class UnavailableDatabase:
        def stat(self):
            raise error

    monkeypatch.setattr(backup_settings.config, "TRANSCRIPTION_DB",
                        UnavailableDatabase())
    assert backup_settings.api.settings_load()["backup_search_db_size_bytes"] is None


def test_large_size_is_not_capped_and_cannot_be_overridden_by_settings(
        backup_settings, monkeypatch):
    size = 35 * 1024 ** 3 + 17
    database = SimpleNamespace(stat=lambda: SimpleNamespace(st_size=size))
    monkeypatch.setattr(backup_settings.config, "TRANSCRIPTION_DB", database)
    _seed_config(backup_settings.config)

    assert backup_settings.api.settings_save({"backup_search_db_size_bytes": 1})["ok"]
    assert backup_settings.api.settings_load()["backup_search_db_size_bytes"] == size
    stored = json.loads(backup_settings.config.CONFIG_FILE.read_text(encoding="utf-8"))
    assert "backup_search_db_size_bytes" not in stored
