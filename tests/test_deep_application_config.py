"""Configuration read/publication ordering with real isolated persistence."""
import copy
import json
import threading

import pytest

from backend import ytarchiver_config as config
from backend.services.config_repository import ConfigRepository


@pytest.mark.parametrize("initial_state", ["current", "migration", "recovery"])
def test_read_generation_cannot_poison_a_newer_save(tmp_path, monkeypatch, initial_state):
    monkeypatch.setattr(config, "APP_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "CONFIG_FILE", tmp_path / "config.json")
    monkeypatch.setattr(config, "_CFG_CACHE", {"sig": None, "data": None})
    monkeypatch.setattr(config, "_save_counter", 0)
    original = dict(config.DEFAULT_CONFIG, audit_marker="old",
                    _migration_v2_pending_tx_ids=True)
    if initial_state == "migration":
        original.pop("legacy_subs_tab", None)
    if initial_state == "recovery":
        config.CONFIG_FILE.write_text("{broken", encoding="utf-8")
        backup = tmp_path / "backups"
        backup.mkdir()
        (backup / "config_fixture.json").write_text(json.dumps(original), encoding="utf-8")
    else:
        config.CONFIG_FILE.write_text(json.dumps(original), encoding="utf-8")

    read_paused = threading.Event()
    release_reader = threading.Event()
    writer_started = threading.Event()
    writer_finished = threading.Event()
    real_copy = copy.deepcopy
    errors = []

    def pause_normalization(value, *args, **kwargs):
        if (threading.current_thread().name == "generation-reader"
                and value is config.DEFAULT_CONFIG and not read_paused.is_set()):
            read_paused.set()
            if not release_reader.wait(3):
                raise AssertionError("reader was not released")
        return real_copy(value, *args, **kwargs)

    monkeypatch.setattr(config.copy, "deepcopy", pause_normalization)

    def read():
        try:
            config.load_config()
        except BaseException as exc:
            errors.append(exc)

    def save():
        writer_started.set()
        try:
            assert config.save_config(dict(original, audit_marker="newer"))
        except BaseException as exc:
            errors.append(exc)
        finally:
            writer_finished.set()

    reader = threading.Thread(target=read, name="generation-reader")
    writer = threading.Thread(target=save, name="generation-writer")
    reader.start()
    try:
        assert read_paused.wait(3)
        writer.start()
        assert writer_started.wait(3)
        assert not writer_finished.wait(0.05), "save crossed an unfinished read generation"
    finally:
        release_reader.set()
        reader.join(3)
        if writer.ident is not None:
            writer.join(3)
    assert not reader.is_alive() and not writer.is_alive()
    assert not errors
    assert config.load_config()["audit_marker"] == "newer"
    repository = ConfigRepository(config.load_config, config.save_config, config.update_config)
    repository.mutate(lambda cfg: cfg.update(audit_unrelated=True))
    stored = json.loads(config.CONFIG_FILE.read_text(encoding="utf-8"))
    assert stored["audit_marker"] == "newer"
    assert stored["audit_unrelated"] is True
