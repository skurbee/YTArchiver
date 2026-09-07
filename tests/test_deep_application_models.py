"""Both public default-model routes share persistence and runtime ordering."""
from __future__ import annotations

import copy
import threading
from types import SimpleNamespace

import pytest

from backend import ytarchiver_config as config
from backend.api_mixins.settings_mixin import SettingsMixin
from backend.api_mixins.transcribe_mixin import TranscribeMixin


class Api(SettingsMixin, TranscribeMixin):
    def __init__(self):
        self.model = "small"
        self.applied = []
        self._transcribe = SimpleNamespace(swap_model=self.apply)
        self._log_stream = SimpleNamespace(emit_dim=lambda *_a: None)

    def apply(self, model):
        self.model = model
        self.applied.append(model)
        return True

    def _reload_config(self):
        pass


@pytest.fixture(autouse=True)
def seed():
    assert config.save_config(copy.deepcopy(config.DEFAULT_CONFIG))


def test_concurrent_public_routes_cannot_split_saved_and_running_defaults(monkeypatch):
    api = Api()
    entered, release, second_done = (threading.Event() for _ in range(3))
    commit = api._settings_commit_candidate

    def hold(original, candidate):
        result = commit(original, candidate)
        entered.set()
        assert release.wait(2)
        return result

    monkeypatch.setattr(api, "_settings_commit_candidate", hold)
    results = []
    first = threading.Thread(target=lambda: results.append(
        api.settings_save({"whisper_model": "medium"})))

    def second_call():
        results.append(api.transcribe_swap_model("tiny"))
        second_done.set()

    second = threading.Thread(target=second_call)
    first.start()
    assert entered.wait(1)
    second.start()
    try:
        assert not second_done.wait(0.05)
        assert api.applied == []
    finally:
        release.set()
        first.join(2)
        second.join(2)
    assert all(result["ok"] for result in results)
    assert api.applied == ["medium", "tiny"]
    assert api.model == config.load_config()["whisper_model"] == "tiny"


@pytest.mark.parametrize("route", ["settings", "popover"])
def test_failed_save_keeps_runtime_default_and_other_settings(monkeypatch, route):
    api = Api()
    before = config.CONFIG_FILE.read_bytes()
    monkeypatch.setattr(config, "save_config", lambda _cfg: False)
    result = (api.settings_save({"whisper_model": "tiny", "log_mode": "Verbose"})
              if route == "settings" else api.transcribe_swap_model("tiny"))
    assert not result["ok"]
    assert not result["persisted"]
    assert api.applied == []
    assert config.CONFIG_FILE.read_bytes() == before


@pytest.mark.parametrize("route", ["settings", "popover"])
def test_runtime_rejection_reports_successful_save_as_deferred(monkeypatch, route):
    api = Api()
    monkeypatch.setattr(api._transcribe, "swap_model", lambda _model: False)
    result = (api.settings_save({"whisper_model": "medium"})
              if route == "settings" else api.transcribe_swap_model("medium"))
    assert result["ok"] and result["persisted"] and result["deferred"]
    assert not result["runtime_applied"]
    assert "restart" in result["error"].lower()
    assert config.load_config()["whisper_model"] == "medium"


def test_unsupported_model_rejects_entire_settings_command():
    api = Api()
    before = config.CONFIG_FILE.read_bytes()
    assert not api.settings_save({"whisper_model": "unknown", "log_mode": "Verbose"})["ok"]
    assert config.CONFIG_FILE.read_bytes() == before
    assert api.applied == []


def test_one_off_model_changes_runtime_only():
    api = Api()
    before = config.CONFIG_FILE.read_bytes()
    result = api.transcribe_swap_model("tiny", persist=False)
    assert result["ok"] and result["runtime_applied"]
    assert not result["persisted"] and not result["deferred"]
    assert api.model == "tiny"
    assert config.CONFIG_FILE.read_bytes() == before
