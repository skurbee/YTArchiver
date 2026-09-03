from __future__ import annotations

import copy
import json
import threading
import time
from types import SimpleNamespace

import pytest

from backend import trash_retention
from backend import ytarchiver_config as config
from backend.api_mixins import settings_mixin as settings_module
from backend.api_mixins.settings_mixin import SettingsMixin


@pytest.fixture
def isolated_config(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "APP_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "CONFIG_FILE", tmp_path / "config.json")
    monkeypatch.setattr(config, "_save_counter", 0)
    monkeypatch.setattr(config, "_config_writes_suspended_reason", "")
    config._cache_config(None, None)
    yield tmp_path
    config._cache_config(None, None)


def _existing_config_seed(**updates):
    seed = {
        "_migration_v2_pending_tx_ids": True,
        "legacy_subs_tab": False,
        "ytdlp_update_mode": "automatic",
    }
    seed.update(updates)
    return seed


def test_new_install_defaults_to_30_days_without_upgrade_grace(
        isolated_config):
    loaded = config.load_config()

    assert loaded["trash_retention_days"] == 30
    assert loaded["trash_retention_grace_until_ts"] == 0.0
    assert not config.CONFIG_FILE.exists()


def test_existing_install_gets_persisted_30_day_upgrade_grace(
        isolated_config, monkeypatch):
    now = 1_700_000_000.0
    monkeypatch.setattr(config.time, "time", lambda: now)
    config.CONFIG_FILE.write_text(
        json.dumps(_existing_config_seed()), encoding="utf-8")

    loaded = config.load_config()
    persisted = json.loads(config.CONFIG_FILE.read_text(encoding="utf-8"))
    expected_grace = now + config.TRASH_RETENTION_UPGRADE_GRACE_SECONDS

    assert loaded["trash_retention_days"] == 30
    assert loaded["trash_retention_grace_until_ts"] == expected_grace
    assert persisted["trash_retention_days"] == 30
    assert persisted["trash_retention_grace_until_ts"] == expected_grace


def test_explicit_existing_policy_is_not_replaced_or_given_upgrade_grace(
        isolated_config):
    seed = _existing_config_seed(trash_retention_days=0)
    config.CONFIG_FILE.write_text(json.dumps(seed), encoding="utf-8")

    loaded = config.load_config()

    assert loaded["trash_retention_days"] == 0
    assert loaded["trash_retention_grace_until_ts"] == 0.0
    persisted = json.loads(config.CONFIG_FILE.read_text(encoding="utf-8"))
    assert "trash_retention_grace_until_ts" not in persisted


def test_failed_upgrade_write_keeps_safe_grace_in_session(
        isolated_config, monkeypatch):
    now = 1_800_000_000.0
    monkeypatch.setattr(config.time, "time", lambda: now)
    config.CONFIG_FILE.write_text(
        json.dumps(_existing_config_seed()), encoding="utf-8")
    monkeypatch.setattr(config, "save_config", lambda _candidate: False)

    loaded = config.load_config()

    assert loaded["trash_retention_days"] == 30
    assert loaded["trash_retention_grace_until_ts"] == (
        now + config.TRASH_RETENTION_UPGRADE_GRACE_SECONDS)
    persisted = json.loads(config.CONFIG_FILE.read_text(encoding="utf-8"))
    assert "trash_retention_days" not in persisted


class _WakeCounter:
    def __init__(self):
        self.count = 0

    def wake(self):
        self.count += 1


class _SettingsHarness(SettingsMixin):
    def __init__(self, cfg, *, save_ok=True, alias=False):
        self.current = copy.deepcopy(cfg)
        self._config = copy.deepcopy(cfg)
        self.save_ok = save_ok
        self.commit_count = 0
        self._log_stream = SimpleNamespace(
            simple_mode=True,
            emit_dim=lambda *_args, **_kwargs: None,
        )
        scheduler = _WakeCounter()
        if alias:
            self._trash_retention_scheduler = scheduler
        else:
            self._trash_retention = scheduler
        self.scheduler = scheduler

    def _settings_fresh_config(self):
        return copy.deepcopy(self.current)

    def _settings_commit_candidate(self, _original, candidate):
        self.commit_count += 1
        if not self.save_ok:
            return False, copy.deepcopy(candidate)
        self.current = copy.deepcopy(candidate)
        return True, copy.deepcopy(candidate)

    def _reload_config(self):
        self._config = copy.deepcopy(self.current)


def _settings_config(days, grace=0.0):
    cfg = copy.deepcopy(config.DEFAULT_CONFIG)
    cfg["trash_retention_days"] = days
    cfg["trash_retention_grace_until_ts"] = grace
    return cfg


@pytest.mark.parametrize("old_days,new_days", [(0, 30), (90, 30)])
def test_enabling_or_shortening_retention_adds_24h_grace_and_wakes(
        monkeypatch, old_days, new_days):
    now = 2_000_000_000.0
    monkeypatch.setattr(settings_module.time, "time", lambda: now)
    monkeypatch.setattr(settings_module, "config_is_writable", lambda: True)
    api = _SettingsHarness(_settings_config(old_days), alias=(old_days == 90))

    result = api.settings_save({"trash_retention_days": str(new_days)})

    assert result["ok"] is True
    assert api.current["trash_retention_days"] == new_days
    assert api.current["trash_retention_grace_until_ts"] == (
        now + config.TRASH_RETENTION_CHANGE_GRACE_SECONDS)
    assert api.scheduler.count == 1
    loaded = api.settings_load()
    assert loaded["trash_retention_days"] == new_days
    assert loaded["trash_retention_grace_until_ts"] == (
        now + config.TRASH_RETENTION_CHANGE_GRACE_SECONDS)


def test_longer_retention_round_trips_without_shortening_existing_grace(
        monkeypatch):
    monkeypatch.setattr(settings_module, "config_is_writable", lambda: True)
    existing_grace = 2_100_000_000.0
    api = _SettingsHarness(_settings_config(30, existing_grace))

    result = api.settings_save({"trash_retention_days": 90})

    assert result["ok"] is True
    assert api.settings_load()["trash_retention_days"] == 90
    assert api.current["trash_retention_grace_until_ts"] == existing_grace
    assert api.scheduler.count == 1


@pytest.mark.parametrize("bad_value", [-1, 3651, True, 1.5, "abc", ""])
def test_invalid_retention_is_rejected_without_save_or_wake(
        monkeypatch, bad_value):
    monkeypatch.setattr(settings_module, "config_is_writable", lambda: True)
    api = _SettingsHarness(_settings_config(30))

    result = api.settings_save({"trash_retention_days": bad_value})

    assert result["ok"] is False
    assert api.commit_count == 0
    assert api.scheduler.count == 0
    assert api.current["trash_retention_days"] == 30


def test_failed_retention_save_does_not_wake_scheduler(monkeypatch):
    monkeypatch.setattr(settings_module, "config_is_writable", lambda: True)
    api = _SettingsHarness(_settings_config(90), save_ok=False)

    result = api.settings_save({"trash_retention_days": 30})

    assert result["ok"] is False
    assert api.scheduler.count == 0
    assert api.current["trash_retention_days"] == 90


def test_scheduler_requires_both_startup_signals_and_grace():
    monotonic = [10.0]
    cleanup_calls = []

    def cleanup(**kwargs):
        cleanup_calls.append(kwargs)
        return {"ok": True, "purged": 2}

    scheduler = trash_retention.TrashRetentionScheduler(
        cleanup_fn=cleanup,
        config_loader=lambda: {
            "trash_retention_days": 30,
            "trash_retention_grace_until_ts": 123.0,
        },
        startup_grace_seconds=20,
        monotonic_clock=lambda: monotonic[0],
        wall_clock=lambda: 500.0,
    )

    first = scheduler.tick()
    assert first["reason"] == "startup"
    assert first["waiting_for"] == ["checks", "indexing"]
    scheduler.notify_startup_ready("checks")
    assert scheduler.tick()["reason"] == "startup"
    scheduler.notify_startup_ready("indexing")
    assert scheduler.tick()["reason"] == "startup_grace"
    monotonic[0] += 20

    result = scheduler.tick(now=700.0)

    assert result["ok"] is True
    assert result["purged"] == 2
    assert cleanup_calls[0]["retention_days"] == 30
    assert cleanup_calls[0]["grace_until_ts"] == 123.0
    assert cleanup_calls[0]["now"] == 700.0
    assert scheduler.snapshot()["last_success_ts"] == 700.0


def test_scheduler_defers_when_busy_and_fails_closed_on_bad_policy():
    calls = []
    scheduler = trash_retention.TrashRetentionScheduler(
        cleanup_fn=lambda **kwargs: calls.append(kwargs),
        config_loader=lambda: {"trash_retention_days": 30},
        busy_fn=lambda: "sync is active",
        startup_grace_seconds=0,
        startup_required_signals=(),
        monotonic_clock=lambda: 0.0,
    )

    deferred = scheduler.tick(now=10.0)

    assert deferred == {
        "ok": True, "deferred": True, "reason": "sync is active"}
    assert calls == []
    assert scheduler.snapshot()["busy_reason"] == "sync is active"

    bad = trash_retention.TrashRetentionScheduler(
        cleanup_fn=lambda **kwargs: calls.append(kwargs),
        config_loader=lambda: {"trash_retention_days": 3651},
        startup_grace_seconds=0,
        startup_required_signals=(),
        monotonic_clock=lambda: 0.0,
    )
    result = bad.tick(now=11.0)
    assert result["ok"] is False
    assert "Invalid Trash retention policy" in result["error"]
    assert calls == []


def test_disabled_scheduler_does_not_call_cleanup():
    calls = []
    scheduler = trash_retention.TrashRetentionScheduler(
        cleanup_fn=lambda **kwargs: calls.append(kwargs),
        config_loader=lambda: {"trash_retention_days": 0},
        startup_grace_seconds=0,
        startup_required_signals=(),
        monotonic_clock=lambda: 0.0,
    )

    result = scheduler.tick(now=12.0)

    assert result["ok"] is True
    assert result["disabled"] is True
    assert calls == []


def test_scheduler_wake_stop_join_and_snapshot():
    condition = threading.Condition()
    call_count = 0

    def cleanup(**_kwargs):
        nonlocal call_count
        with condition:
            call_count += 1
            condition.notify_all()
        return {"ok": True}

    def wait_for_count(expected):
        deadline = time.monotonic() + 2.0
        with condition:
            while call_count < expected:
                remaining = deadline - time.monotonic()
                assert remaining > 0, f"cleanup count stayed at {call_count}"
                condition.wait(remaining)

    scheduler = trash_retention.TrashRetentionScheduler(
        cleanup_fn=cleanup,
        config_loader=lambda: {"trash_retention_days": 30},
        startup_grace_seconds=0,
        startup_required_signals=(),
        interval_seconds=3600,
    )

    assert scheduler.INTERVAL_SECONDS == 24 * 60 * 60
    assert scheduler.start() is True
    wait_for_count(1)
    scheduler.wake()
    wait_for_count(2)
    assert scheduler.snapshot()["running"] is True
    assert scheduler.stop(timeout=2.0) is True
    assert scheduler.join(timeout=0) is True
    snapshot = scheduler.snapshot()
    assert snapshot["running"] is False
    assert snapshot["stop_requested"] is True
