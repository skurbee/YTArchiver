"""Rate-limit decisions survive failed persistence; never use real profiles."""
import json
from unittest import mock

import pytest

from backend import youtube_traffic as traffic


@pytest.fixture(autouse=True)
def isolated_circuit(tmp_path, monkeypatch):
    monkeypatch.setattr(traffic, "APP_DATA_DIR", tmp_path)
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")
    monkeypatch.setattr(traffic, "load_config", lambda: {"channels": [{"url": "fixture"}]})
    yield
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")


def test_failed_save_preserves_active_decision_and_can_retry_without_escalation(monkeypatch):
    with mock.patch.object(traffic.os, "replace", side_effect=PermissionError("fixture denied")):
        first = traffic.record_rate_limit(now=1000)
        again = traffic.record_rate_limit(now=1001)
        state = traffic.circuit_state(now=1002)
    assert first["active"] and not first["persisted"]
    assert again["cooldown_until"] == first["cooldown_until"]
    assert state["active"] and state["state_known"] and state["incident_count_7d"] == 1
    assert not state["persisted"]
    retry = traffic.record_rate_limit(now=1003)
    assert retry["persisted"]
    traffic._reset_for_tests()
    restored = traffic.circuit_state(now=1004)
    assert restored["active"] and restored["incident_count_7d"] == 1
    assert restored["cooldown_until"] == first["cooldown_until"]


def test_transient_read_failure_keeps_validated_state_and_real_expiry(monkeypatch):
    first = traffic.record_rate_limit(now=1000)
    with mock.patch.object(type(traffic.CIRCUIT_FILE), "read_text", side_effect=PermissionError("fixture denied")):
        state = traffic.circuit_state(now=1001)
        expired = traffic.circuit_state(now=first["cooldown_until"] + 1)
    assert state["state_known"] and state["active"] and state["error"]
    assert expired["state_known"] and not expired["active"]


@pytest.mark.parametrize("contents", ["not json", "[]", '{"incidents":["bad"]}', '{"cooldown_until":NaN}'])
def test_unknown_startup_state_defers_requests_and_recovers_after_valid_read(contents):
    traffic.CIRCUIT_FILE.write_text(contents, encoding="utf-8")
    state = traffic.circuit_state(now=1000)
    assert not state["state_known"] and not state["active"]
    assert not traffic.eligibility(now=1000)["allowed"]
    sweep = traffic.sweep_eligibility(now=1000)
    assert not sweep["allowed"] and sweep["next_ts"] >= 1060
    assert traffic.acquire("fixture")["circuit_error"]
    traffic.CIRCUIT_FILE.write_text(json.dumps({"incidents": [], "cooldown_until": 0}), encoding="utf-8")
    assert traffic.circuit_state(now=1001)["state_known"]
    assert traffic.eligibility(now=1001)["allowed"]


def test_escalation_remains_bounded_and_duplicate_signals_do_not_escalate():
    first = traffic.record_rate_limit(now=1000)
    duplicate = traffic.record_rate_limit(now=1001)
    second = traffic.record_rate_limit(now=first["cooldown_until"] + 1)
    third = traffic.record_rate_limit(now=second["cooldown_until"] + 1)
    assert [first["cooldown_hours"], second["cooldown_hours"], third["cooldown_hours"]] == [6, 24, 72]
    assert duplicate["cooldown_until"] == first["cooldown_until"]
