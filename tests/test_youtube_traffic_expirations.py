"""Expiration snapshots follow the same ledger and boundaries as admission."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from backend import youtube_traffic as traffic
from backend.api_mixins.settings_mixin import SettingsMixin


@pytest.fixture
def clock(tmp_path, monkeypatch):
    state = SimpleNamespace(now=1_800_000_000.0)
    config = {
        "youtube_traffic_mode": "custom",
        "youtube_traffic_custom_daily": 1000,
        "youtube_traffic_custom_hourly": 200,
        "youtube_traffic_custom_min_gap": 0,
        "youtube_traffic_custom_max_gap": 0,
        "autorun_interval": -1,
        "channels": [
            {"name": f"Channel {i}", "url": f"https://example/{i}",
             "mode": "full", "init_complete": True}
            for i in range(4)
        ],
    }
    old_paths = traffic.TRAFFIC_FILE, traffic.CIRCUIT_FILE
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")
    monkeypatch.setattr(traffic, "APP_DATA_DIR", tmp_path)
    monkeypatch.setattr(traffic, "load_config", lambda: config)
    monkeypatch.setattr(traffic.time, "time", lambda: state.now)
    state.config = config
    yield state
    traffic._reset_for_tests(*old_paths)


def write_rows(rows):
    traffic.TRAFFIC_FILE.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def assert_matches_status(snapshot):
    assert snapshot["daily_used"] == traffic.status()["daily_used"]
    assert sum(row["units"] for row in snapshot["expirations"]) == snapshot["daily_used"]


def test_empty_schedule_has_a_snapshot_time_and_zero_usage(clock):
    assert traffic.daily_expirations() == {
        "ok": True, "as_of": clock.now, "daily_used": 0, "expirations": [],
    }


def test_groups_legacy_and_current_charges_by_minute_in_time_order(clock):
    minute = clock.now
    clock.now += 15.5
    cutoff = clock.now - traffic.DAY_SECONDS
    write_rows([
        {"ts": cutoff + 104.5, "daily_units": 7, "hourly_units": 7},
        {"ts": cutoff - 1, "units": 99},
        {"ts": cutoff + 0.25, "units": 3},
        {"ts": cutoff, "units": 88},
        {"ts": cutoff + 44.49, "daily_units": 2, "hourly_units": 2},
        {"ts": cutoff + 44.5, "daily_units": 5, "hourly_units": 5},
        {"ts": clock.now, "daily_units": 0, "hourly_units": 20},
    ])

    snapshot = traffic.daily_expirations()

    assert snapshot == {
        "ok": True, "as_of": clock.now, "daily_used": 17,
        "expirations": [
            {"expires_at": minute, "units": 5},
            {"expires_at": minute + 60, "units": 5},
            {"expires_at": minute + 120, "units": 7},
        ],
    }
    assert_matches_status(snapshot)


def test_charges_expire_at_their_exact_time_within_displayed_minute(clock):
    charge = clock.now - traffic.DAY_SECONDS + 45.75
    write_rows([{"ts": charge, "units": 4}])
    clock.now += 45.5

    before = traffic.daily_expirations()
    assert before["expirations"] == [{
        "expires_at": 1_800_000_000, "units": 4,
    }]
    assert_matches_status(before)

    clock.now += 0.25
    after = traffic.daily_expirations()
    assert after["expirations"] == []
    assert_matches_status(after)


@pytest.mark.parametrize("consumed", [0, 1])
def test_active_reservation_refund_and_reload_keep_original_expiry(clock, consumed):
    reserved_at = clock.now
    reservation = traffic.reserve_sweep(clock.config)
    assert reservation["ok"]
    clock.now += 120
    with traffic.reservation_scope(reservation["reservation_id"]):
        for _ in range(consumed):
            assert traffic.acquire("channel_sync")["reserved"]

    active = traffic.daily_expirations()
    assert active["expirations"] == [{
        "expires_at": reserved_at + traffic.DAY_SECONDS,
        "units": reservation["units"],
    }]
    assert_matches_status(active)

    refund = traffic.finish_reservation(reservation["reservation_id"])
    assert refund["refunded"] == reservation["units"] - consumed
    finished = traffic.daily_expirations()
    assert finished["expirations"] == ([{
        "expires_at": reserved_at + traffic.DAY_SECONDS, "units": consumed,
    }] if consumed else [])
    assert_matches_status(finished)
    traffic._reset_for_tests()
    assert traffic.daily_expirations() == finished


def test_restart_keeps_unfinished_reservation_charged(clock):
    reservation = traffic.reserve_sweep(clock.config)
    assert reservation["ok"]
    before = traffic.daily_expirations()

    traffic._reset_for_tests()
    assert traffic.daily_expirations() == before
    assert traffic.finish_reservation(reservation["reservation_id"])["refunded"] == 0
    assert traffic.daily_expirations() == before


def test_legacy_refund_is_folded_into_original_minute_and_orphan_is_ignored(clock):
    reserved_at = clock.now - 120
    write_rows([
        {"ts": reserved_at, "daily_units": 8, "hourly_units": 0,
         "kind": "autosync_sweep_reservation", "reservation_id": "active"},
        {"ts": clock.now, "daily_units": -5, "hourly_units": 0,
         "kind": "autosync_sweep_refund", "reservation_id": "active"},
        {"ts": clock.now, "daily_units": -20, "hourly_units": 0,
         "kind": "autosync_sweep_refund", "reservation_id": "missing"},
        {"ts": clock.now - 60, "units": 2},
    ])

    snapshot = traffic.daily_expirations()
    assert snapshot["expirations"] == [
        {"expires_at": reserved_at + traffic.DAY_SECONDS, "units": 3},
        {"expires_at": clock.now - 60 + traffic.DAY_SECONDS, "units": 2},
    ]
    assert_matches_status(snapshot)
    traffic._reset_for_tests()
    assert traffic.daily_expirations() == snapshot


def test_bridge_returns_live_schedule_and_reports_failures(clock, monkeypatch):
    assert traffic.acquire("manual")["ok"]
    assert SettingsMixin().youtube_traffic_expirations() == traffic.daily_expirations()

    def fail():
        raise RuntimeError("snapshot failed")

    monkeypatch.setattr(traffic, "daily_expirations", fail)
    result = SettingsMixin().youtube_traffic_expirations()
    assert result["ok"] is False
    assert result["error"] == "snapshot failed"
