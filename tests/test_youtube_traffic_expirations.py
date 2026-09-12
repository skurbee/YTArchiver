"""Expiration snapshots follow the same ledger and boundaries as admission."""

from __future__ import annotations

import importlib.util
import inspect
import json
import subprocess
from pathlib import Path
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


def assert_matches_status(snapshot, window="daily"):
    used_field = f"{window}_used"
    assert snapshot[used_field] == traffic.status()[used_field]
    assert sum(row["units"] for row in snapshot["expirations"]) == snapshot[used_field]


@pytest.mark.parametrize("window", ["daily", "hourly"])
def test_empty_schedule_has_a_snapshot_time_and_zero_usage(clock, window):
    assert getattr(traffic, f"{window}_expirations")() == {
        "ok": True, "as_of": clock.now, f"{window}_used": 0, "expirations": [],
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


def test_hourly_groups_its_own_units_and_excludes_older_daily_usage(clock):
    minute = clock.now
    clock.now += 15.5
    cutoff = clock.now - traffic.HOUR_SECONDS
    write_rows([
        {"ts": cutoff + 104.5, "daily_units": 70, "hourly_units": 7},
        {"ts": cutoff - 1, "units": 99},
        {"ts": cutoff + 0.25, "units": 3},
        {"ts": cutoff, "units": 88},
        {"ts": cutoff + 44.49, "daily_units": 25, "hourly_units": 2},
        {"ts": cutoff + 44.5, "daily_units": 50, "hourly_units": 5},
        {"ts": clock.now, "daily_units": 20, "hourly_units": 0},
        {"ts": clock.now, "daily_units": 0, "hourly_units": 11},
    ])

    snapshot = traffic.hourly_expirations()

    assert snapshot == {
        "ok": True, "as_of": clock.now, "hourly_used": 28,
        "expirations": [
            {"expires_at": minute, "units": 5},
            {"expires_at": minute + 60, "units": 5},
            {"expires_at": minute + 120, "units": 7},
            {"expires_at": minute + traffic.HOUR_SECONDS, "units": 11},
        ],
    }
    assert_matches_status(snapshot, "hourly")
    daily = traffic.daily_expirations()
    assert daily["daily_used"] == 355
    assert_matches_status(daily)
    traffic._reset_for_tests()
    assert traffic.hourly_expirations() == snapshot
    assert traffic.daily_expirations() == daily


@pytest.mark.parametrize("window,seconds", [
    ("daily", traffic.DAY_SECONDS), ("hourly", traffic.HOUR_SECONDS),
])
def test_charges_expire_at_their_exact_time_within_displayed_minute(
        clock, window, seconds):
    snapshot_fn = getattr(traffic, f"{window}_expirations")
    charge = clock.now - seconds + 45.75
    write_rows([{"ts": charge, "units": 4}])
    clock.now += 45.5

    before = snapshot_fn()
    assert before["expirations"] == [{
        "expires_at": 1_800_000_000, "units": 4,
    }]
    assert_matches_status(before, window)

    clock.now += 0.25
    after = snapshot_fn()
    assert after["expirations"] == []
    assert_matches_status(after, window)


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
    hourly = traffic.hourly_expirations()
    assert hourly["expirations"] == ([{
        "expires_at": clock.now + traffic.HOUR_SECONDS, "units": consumed,
    }] if consumed else [])
    assert_matches_status(hourly, "hourly")

    refund = traffic.finish_reservation(reservation["reservation_id"])
    assert refund["refunded"] == reservation["units"] - consumed
    finished = traffic.daily_expirations()
    assert finished["expirations"] == ([{
        "expires_at": reserved_at + traffic.DAY_SECONDS, "units": consumed,
    }] if consumed else [])
    assert_matches_status(finished)
    assert traffic.hourly_expirations() == hourly
    traffic._reset_for_tests()
    assert traffic.daily_expirations() == finished
    assert traffic.hourly_expirations() == hourly


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
    hourly = traffic.hourly_expirations()
    assert hourly["expirations"] == [{
        "expires_at": clock.now - 60 + traffic.HOUR_SECONDS, "units": 2,
    }]
    assert_matches_status(hourly, "hourly")
    traffic._reset_for_tests()
    assert traffic.daily_expirations() == snapshot
    assert traffic.hourly_expirations() == hourly


def test_bridge_returns_live_schedule_and_reports_failures(clock, monkeypatch):
    assert traffic.acquire("manual")["ok"]
    assert SettingsMixin().youtube_traffic_expirations() == traffic.daily_expirations()

    def fail():
        raise RuntimeError("snapshot failed")

    monkeypatch.setattr(traffic, "daily_expirations", fail)
    result = SettingsMixin().youtube_traffic_expirations()
    assert result["ok"] is False
    assert result["error"] == "snapshot failed"


def test_native_generated_proxy_forwards_default_hourly_and_daily(clock):
    """Exercise pywebview's named-parameter wrapper, which browser stubs bypass."""
    webview_spec = importlib.util.find_spec("webview")
    assert webview_spec is not None and webview_spec.origin is not None
    api_js = Path(webview_spec.origin).parent / "js" / "api.js"
    assert api_js.is_file()
    method = SettingsMixin.youtube_traffic_expirations
    params = inspect.getfullargspec(method).args[1:]
    calls = [[], ["hourly"], ["daily"]]
    script = r"""
const fs = require('node:fs');
const vm = require('node:vm');
const input = JSON.parse(fs.readFileSync(0, 'utf8'));
global.window = global;
vm.runInThisContext(fs.readFileSync(input.api_js, 'utf8'), {filename: input.api_js});
const forwarded = [];
// Only the host transport is replaced; proxy generation and reply handling are real.
pywebview._jsApiCallback = (name, args, id) => {
  forwarded.push({name, args});
  pywebview._returnValuesCallbacks[name][id]({value: JSON.stringify({ok: true})});
};
pywebview._createApi([{func: input.method, params: input.params}]);
(async () => {
  for (const args of input.calls) {
    const reply = await pywebview.api[input.method](...args);
    if (!reply.ok) throw new Error('Generated proxy did not receive its reply');
  }
  process.stdout.write(JSON.stringify(forwarded));
})().catch(error => {
  process.stderr.write(error.stack);
  process.exitCode = 1;
});
"""
    result = subprocess.run(
        ["node", "-e", script],
        input=json.dumps({"api_js": str(api_js), "method": method.__name__,
                          "params": params, "calls": calls}),
        capture_output=True, text=True, timeout=15, check=False,
    )
    assert result.returncode == 0, result.stderr
    forwarded = json.loads(result.stdout)
    assert forwarded == [{"name": method.__name__, "args": args} for args in calls]

    write_rows([
        {"ts": clock.now - 2 * traffic.HOUR_SECONDS, "units": 9},
        {"ts": clock.now - 60, "daily_units": 5, "hourly_units": 4},
    ])
    for call, period, used in zip(
            forwarded, ("daily", "hourly", "daily"), (14, 4, 14), strict=True):
        snapshot = method(SettingsMixin(), *call["args"])
        assert snapshot == getattr(traffic, f"{period}_expirations")()
        assert snapshot[f"{period}_used"] == used
        assert_matches_status(snapshot, period)


@pytest.mark.parametrize("window", ["daily", "hourly"])
def test_bridge_routes_selected_window_and_reports_failures(clock, monkeypatch, window):
    snapshot_fn = getattr(traffic, f"{window}_expirations")
    assert traffic.acquire("manual")["ok"]
    assert SettingsMixin().youtube_traffic_expirations(window) == snapshot_fn()

    def fail():
        raise RuntimeError("selected snapshot failed")

    monkeypatch.setattr(traffic, f"{window}_expirations", fail)
    result = SettingsMixin().youtube_traffic_expirations(window)
    assert result["ok"] is False
    assert result["code"] == "INTERNAL_ERROR"
    assert result["error"] == "selected snapshot failed"


@pytest.mark.parametrize("window", ["", "weekly", "Hourly", None, 1, [], {}])
def test_bridge_rejects_invalid_window_without_reading_ledger(monkeypatch, window):
    def unexpected_snapshot():
        pytest.fail("Invalid window must not access the traffic ledger")

    monkeypatch.setattr(traffic, "daily_expirations", unexpected_snapshot)
    monkeypatch.setattr(traffic, "hourly_expirations", unexpected_snapshot)
    assert SettingsMixin().youtube_traffic_expirations(window) == {
        "ok": False,
        "code": "INVALID_WINDOW",
        "message": "Window must be 'daily' or 'hourly'.",
        "error": "Window must be 'daily' or 'hourly'.",
        "details": {},
        "retryable": False,
    }
