"""Exercise real governor admission with a deterministic clock and no network."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from backend import youtube_request_broker as broker
from backend import youtube_traffic as traffic


@pytest.fixture
def clock(tmp_path, monkeypatch):
    state = SimpleNamespace(now=100_000.0)

    def wait(timeout=0):
        state.now += timeout
        return False

    config = {
        "channels": [], "youtube_traffic_mode": "custom",
        "youtube_traffic_custom_daily": 1000,
        "youtube_traffic_custom_hourly": 200,
        "youtube_traffic_custom_min_gap": 10,
        "youtube_traffic_custom_max_gap": 10,
    }
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")
    monkeypatch.setattr(traffic, "APP_DATA_DIR", tmp_path)
    monkeypatch.setattr(traffic, "load_config", lambda: config)
    monkeypatch.setattr(traffic.time, "time", lambda: state.now)
    monkeypatch.setattr(traffic.random, "uniform", lambda low, high: low)
    monkeypatch.setattr(traffic, "_override_wakeup", SimpleNamespace(
        wait=wait, clear=lambda: None, set=lambda: None))
    state.config = config
    state.wait = wait
    yield state
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")


def rows():
    return [json.loads(line) for line in traffic.TRAFFIC_FILE.read_text().splitlines()]


def test_requests_keep_their_own_clock_without_restarting_job_pause(clock):
    assert traffic.acquire("channel_sync")["ok"]
    assert traffic.acquire("youtube_http")["ok"]
    assert clock.now == 100_000
    assert traffic.acquire("youtube_caption")["ok"]
    assert clock.now == 100_001
    assert traffic.acquire("video_metadata")["ok"]
    assert clock.now == 100_010  # ten from the job, not ten from its caption
    assert traffic.acquire("youtube_http")["ok"]
    assert clock.now == 100_010  # new job did not impose another ten seconds
    assert traffic.status()["daily_used"] == 5


@pytest.mark.parametrize("kind", sorted(traffic._REQUEST_KINDS))
def test_all_http_categories_share_the_same_gap(clock, kind):
    assert traffic.acquire("youtube_http")["ok"]
    assert traffic.acquire(kind)["ok"]
    assert clock.now == 100_001


def test_broker_sessions_share_http_clock_and_budget(clock):
    first = broker.RequestSession(SimpleNamespace(port=12345), "")
    second = broker.RequestSession(SimpleNamespace(port=12345), "")
    # Keep the broker's control checks, replacing only the physical sleep.
    first._cancel.wait = second._cancel.wait = clock.wait
    assert first.acquire("youtube_http")["ok"]
    clock.now += 0.5
    assert second.acquire("youtube_caption")["ok"]
    assert clock.now == 100_001
    assert traffic.status()["daily_used"] == 2


@pytest.mark.parametrize("field,reason,expiry", [
    ("hourly", "hourly_limit", 3600), ("daily", "daily_limit", 86400),
])
def test_mixed_operations_and_requests_still_hit_exact_caps(clock, field, reason, expiry):
    clock.config[f"youtube_traffic_custom_{field}"] = 2
    assert traffic.acquire("channel_sync")["ok"]
    assert traffic.acquire("youtube_http")["ok"]
    check = traffic.eligibility(kind="youtube_caption")
    assert not check["allowed"]
    assert check["wait_reason"] == reason
    assert check["next_ts"] == pytest.approx(100_000 + expiry + 0.01)
    assert traffic.status()["daily_used"] == 2


def test_reserved_admissions_use_same_split_without_double_charging(clock):
    # A pre-existing daily reservation, as made by scheduled sweep admission.
    assert traffic._append_locked({
        "ts": clock.now, "daily_units": 4, "hourly_units": 0,
        "kind": "autosync_sweep_reservation", "reservation_id": "sweep",
    })
    traffic._loaded = True
    traffic._reservations["sweep"] = {"reserved": 4, "remaining": 4, "ts": clock.now}
    with traffic.reservation_scope("sweep"):
        assert traffic.acquire("channel_sync")["reserved"]
        assert traffic.acquire("youtube_http")["reserved"]
        assert traffic.acquire("youtube_caption")["reserved"]
        assert clock.now == 100_001
        assert traffic.acquire("video_metadata")["reserved"]
        assert clock.now == 100_010
    assert traffic.status()["daily_used"] == traffic.status()["hourly_used"] == 4
    assert traffic._reservations["sweep"]["remaining"] == 0


@pytest.mark.parametrize("control", ["cancel_event", "pause_event"])
def test_control_during_http_gap_does_not_charge(clock, control):
    assert traffic.acquire("youtube_http")["ok"]
    signal = SimpleNamespace(active=False)

    def stop(timeout):
        clock.now += timeout
        signal.active = True
        return True

    signal.is_set = lambda: signal.active
    signal.wait = stop
    outcome = traffic.acquire("youtube_caption", **{control: signal})
    assert outcome["cancelled" if control == "cancel_event" else "paused"]
    assert len(rows()) == 1


def test_restart_recovers_each_clock_from_ledger(clock):
    assert traffic.acquire("channel_sync")["ok"]
    clock.now += 3
    assert traffic.acquire("youtube_http")["ok"]
    assert traffic._append_locked({
        "ts": clock.now + 0.5, "daily_units": 1, "hourly_units": 0,
        "kind": "autosync_sweep_reservation", "reservation_id": "reserved",
    })
    clock.now += 0.5
    traffic._reset_for_tests()
    request = traffic.eligibility(kind="youtube_caption")
    job = traffic.eligibility(kind="video_metadata")
    assert request["next_ts"] == 100_004
    assert job["next_ts"] == 100_010
    assert request["daily_used"] == 3


@pytest.mark.parametrize("kind", ["youtube_http", "channel_sync"])
def test_failed_persistence_advances_neither_clock(clock, monkeypatch, kind):
    monkeypatch.setattr(traffic, "_append_locked", lambda row: False)
    assert traffic.acquire(kind)["ledger_error"]
    assert traffic.eligibility(kind="youtube_http")["allowed"]
    assert traffic.eligibility(kind="channel_sync")["allowed"]
    assert traffic.status()["daily_used"] == 0


def test_http_requests_respect_circuit_even_with_capacity(clock):
    traffic.record_rate_limit(now=clock.now)
    assert traffic.acquire("youtube_http")["cooldown"]
    assert traffic.status()["daily_used"] == 0


def test_budget_override_preserves_both_clocks(clock, monkeypatch):
    monkeypatch.setattr(traffic, "budget_override_active", lambda: True)
    monkeypatch.setattr(traffic.time, "sleep", clock.wait)
    clock.config["youtube_traffic_custom_daily"] = 1
    assert traffic.acquire("channel_sync")["ok"]
    assert traffic.acquire("youtube_http")["ok"]
    assert traffic.acquire("youtube_caption")["ok"]
    assert clock.now == 100_001
    assert traffic.acquire("video_metadata")["ok"]
    assert clock.now == 100_010
    assert traffic.status()["daily_used"] == 4
