"""Pass-scoped caption permissions use real disposable journals and ledgers."""

import json
import os
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from backend import youtube_traffic as traffic
from backend.api_mixins.queue_mixin import QueueMixin
from backend.transcribe import core
from backend.transcribe.recovery import ProcessingRecord
from backend.youtube_request_broker import RequestSession


@pytest.fixture
def setup(tmp_path, monkeypatch):
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")
    config = {"channels": [], "youtube_traffic_mode": "custom",
              "youtube_traffic_custom_daily": 1, "youtube_traffic_custom_hourly": 1,
              "youtube_traffic_custom_min_gap": 0, "youtube_traffic_custom_max_gap": 0}
    monkeypatch.setattr(traffic, "load_config", lambda: config)
    monkeypatch.setattr(traffic, "REQUEST_MIN_GAP", 0)
    monkeypatch.setattr(traffic, "REQUEST_MAX_GAP", 0)
    manager = core.TranscribeManager(Mock(), model="small")
    journal = tmp_path / "pending.json"
    monkeypatch.setattr(core, "_pending_journal_path", lambda: journal)
    monkeypatch.setattr(manager, "_ensure_worker", lambda: None)
    monkeypatch.setattr(core, "_bump_transcription_pending", lambda *_: None)
    monkeypatch.setattr(core, "_try_auto_captions", lambda *_a, **_k: core._CaptionOutcome.UNAVAILABLE)

    def job(name, pass_id="", **kwargs):
        path = tmp_path / f"{name}.mp4"
        path.write_bytes(b"offline fixture")
        return {"task_id": name, "path": str(path), "title": name,
                "kind": "transcribe", "from_download": True, "channel": "Fixture",
                "traffic_sync_pass_id": pass_id, "traffic_override": False,
                "_retry_required": True, "cancel": threading.Event(), **kwargs}

    yield SimpleNamespace(manager=manager, journal=journal, job=job, config=config,
                          path=tmp_path, monkeypatch=monkeypatch)
    traffic._reset_for_tests(tmp_path / "traffic.jsonl", tmp_path / "circuit.json")


def test_override_includes_existing_and_future_matching_jobs_only(setup):
    p = setup
    pass_id = traffic.begin_sync_pass()
    existing = p.job("existing", pass_id)
    other = p.job("older", "older-pass")
    manual = p.job("manual", pass_id, from_download=False)
    p.manager._jobs = [existing, other, manual]
    assert p.manager.authorize_traffic_followups(pass_id) == {"ok": True, "authorized": 1}
    traffic.override_budget_limits()
    late = p.job("late", _download_sync_job_id="originating-sync")
    assert p.manager._stage_inline_caption_recovery(late) == (True, False)
    assert late["traffic_sync_pass_id"] == pass_id
    assert late["traffic_override"] is True
    traffic.finish_sync_pass(pass_id)
    p.manager.finish_traffic_pass(pass_id)
    assert not traffic.budget_override_active()
    saved = {row["task_id"]: row for row in json.loads(p.journal.read_text())}
    assert {key for key, row in saved.items() if row["traffic_override"]} == {"existing", "late"}
    assert existing["traffic_override"] and not other["traffic_override"]
    assert not manual["traffic_override"]
    next_pass = traffic.begin_sync_pass()
    later = p.job("next", _download_sync_job_id="another-sync")
    assert p.manager._stage_inline_caption_recovery(later) == (True, False)
    assert later["traffic_sync_pass_id"] == next_pass != pass_id
    assert not later["traffic_override"]


def test_inline_marker_grant_survives_promotion(setup):
    p = setup
    pass_id = traffic.begin_sync_pass()
    assert p.manager.authorize_traffic_followups(pass_id)["ok"]
    marker = p.job("inline", _download_sync_job_id="sync-job")
    assert p.manager._stage_inline_caption_recovery(marker) == (True, False)
    assert p.manager._promote_inline_caption_recovery(marker)
    assert p.manager._jobs[0]["traffic_override"] is True
    assert p.manager._jobs[0]["traffic_sync_pass_id"] == pass_id
    assert json.loads(p.journal.read_text())[0]["traffic_override"] is True


def test_failed_journal_does_not_publish_grant_to_waiting_worker(setup):
    p = setup
    pass_id = traffic.begin_sync_pass()
    job = p.job("waiting", pass_id)
    p.manager._current_job = job
    assert p.manager._persist_pending()
    original = p.journal.read_bytes()

    def fail(snapshot):
        assert snapshot[0]["traffic_override"] is True
        assert job["traffic_override"] is False
        return False

    p.monkeypatch.setattr(p.manager, "_write_pending_snapshot", fail)
    assert not p.manager.authorize_traffic_followups(pass_id)["ok"]
    assert not job["traffic_override"]
    assert pass_id not in p.manager._authorized_traffic_passes
    assert p.journal.read_bytes() == original


def test_current_inline_and_pending_records_are_authorized_together(setup):
    p = setup
    pass_id = traffic.begin_sync_pass()
    p.manager._current_job = p.job("current", pass_id)
    p.manager._jobs = [p.job("pending", pass_id)]
    p.manager._inline_caption_jobs = [p.job("inline", pass_id)]
    assert p.manager.authorize_traffic_followups(pass_id)["authorized"] == 3
    assert all(row["traffic_override"] for row in json.loads(p.journal.read_text()))


def test_existing_legacy_processing_cohort_is_bounded(setup):
    p = setup
    p.manager._current_job = p.job("current")
    p.manager._jobs = [p.job("legacy"), p.job("other-pass", "other"),
                       p.job("manual", from_download=False), p.job("compress", kind="compress"),
                       p.job("other-channel", channel="Other channel")]
    assert p.manager.authorize_traffic_followups()["authorized"] == 2
    assert not traffic.budget_override_active()
    assert not p.manager._authorized_traffic_passes
    granted = [row["task_id"] for row in json.loads(p.journal.read_text()) if row["traffic_override"]]
    assert granted == ["current", "legacy"]
    new = p.job("new")
    assert p.manager._stage_inline_caption_recovery(new) == (True, False)
    assert not new["traffic_override"]


def test_processing_known_pass_excludes_other_pending_work(setup):
    p = setup
    p.manager._current_job = p.job("current", "first")
    p.manager._jobs = [p.job("same", "first"), p.job("other", "second")]
    assert p.manager.authorize_traffic_followups()["authorized"] == 2
    assert not p.manager._jobs[1]["traffic_override"]
    assert not traffic.budget_override_active()


def test_saved_grant_survives_new_manager_and_fresh_process(setup):
    p = setup
    pass_id = traffic.begin_sync_pass()
    p.manager._jobs = [p.job("saved", pass_id)]
    assert p.manager.authorize_traffic_followups(pass_id)["ok"]
    traffic.finish_sync_pass(pass_id)
    restarted = core.TranscribeManager(Mock(), model="small")
    assert restarted.load_pending() == 1
    assert restarted._jobs[0]["traffic_override"] is True
    assert restarted._jobs[0]["traffic_sync_pass_id"] == pass_id
    code = """
import json, sys, threading
from backend import youtube_traffic as traffic
from backend.transcribe.recovery import ProcessingRecord
from pathlib import Path
root = Path(sys.argv[1])
traffic._reset_for_tests(root / 'child-traffic.jsonl', root / 'child-circuit.json')
traffic.load_config = lambda: {'channels': [], 'youtube_traffic_mode': 'custom',
    'youtube_traffic_custom_daily': 1, 'youtube_traffic_custom_hourly': 1,
    'youtube_traffic_custom_min_gap': 0, 'youtube_traffic_custom_max_gap': 0}
job = ProcessingRecord.decode(json.loads((root/'pending.json').read_text())[0],
    interrupted=True).runtime_payload(cancel_event=threading.Event())
assert traffic.acquire('caption_fetch')['ok']
with traffic.request_scope('gpu', override_allowed=job['traffic_override']):
    grant = traffic.acquire('caption_fetch', wait_for_budget=False)
assert grant['ok'] and grant['override']
assert not traffic.budget_override_active()
assert traffic.acquire('caption_fetch', wait_for_budget=False)['deferred']
print('fresh process scoped grant verified')
"""
    env = dict(os.environ)
    env["APPDATA"] = str(p.path / "child-profile")
    env["LOCALAPPDATA"] = str(p.path / "child-local")
    result = subprocess.run([sys.executable, "-c", code, str(p.path)], env=env,
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert "verified" in result.stdout


@pytest.mark.parametrize("patch", [
    {"traffic_override": "true"}, {"traffic_override": 1},
    {"traffic_sync_pass_id": ""}, {"kind": "compress"}, {"retranscribe": True},
    {"traffic_sync_pass_id": 123}, {"traffic_sync_pass_id": "   "},
    {"traffic_sync_pass_id": True}, {"traffic_sync_pass_id": []},
])
def test_invalid_or_noncaption_records_never_inherit_override(patch):
    saved = {"traffic_sync_pass_id": "pass", "traffic_override": True, **patch}
    assert ProcessingRecord.decode(saved).traffic_override is False


def test_processing_scope_grants_caption_launch_and_broker_requests_only(setup):
    p = setup
    assert traffic.acquire("caption_fetch")["ok"]
    job = p.job("worker", "pass", traffic_override=True)
    original_acquire = traffic.acquire
    p.monkeypatch.setattr(traffic, "acquire", lambda kind, **kw: original_acquire(
        kind, **{**kw, "wait_for_budget": False}))
    observed = []

    def run(current):
        observed.append(traffic.acquire("caption_fetch"))
        broker = RequestSession(SimpleNamespace(port=1, discard=lambda _: None), "")
        observed.append(broker.acquire("youtube_caption"))
        return core._WorkerOutcome.SUCCESS

    p.monkeypatch.setattr(p.manager, "_run_under_channel_lease", lambda job, fn: fn(job))
    p.monkeypatch.setattr(p.manager, "_transcribe_one_unleased", run)
    assert p.manager._transcribe_one(job) is core._WorkerOutcome.SUCCESS
    assert all(row["ok"] for row in observed)
    assert not traffic.current_request_scope()
    assert traffic.acquire("youtube_thumbnail")["deferred"]
    assert not traffic.budget_override_active()


def test_broker_observes_authorization_enabled_after_child_launch(setup):
    p = setup
    job = p.job("waiting", "pass")
    p.manager._current_job = job
    with traffic.request_scope("gpu", override_allowed=lambda: job["traffic_override"]):
        broker = RequestSession(SimpleNamespace(port=1, discard=lambda _: None), "")
    assert traffic.acquire("caption_fetch")["ok"]
    assert p.manager.authorize_traffic_followups()["ok"]
    assert broker.acquire("youtube_caption")["ok"]
    assert traffic.acquire("youtube_thumbnail", wait_for_budget=False)["deferred"]


def test_emergency_circuit_still_blocks_authorized_caption_job(setup):
    traffic.record_rate_limit()
    with traffic.request_scope("gpu", override_allowed=True):
        result = traffic.acquire("caption_fetch", wait_for_budget=False)
    assert not result["ok"] and result["cooldown"]
    assert traffic.status()["daily_used"] == 0


@pytest.mark.parametrize("queue", ["sync", "gpu", "background"])
def test_wait_attribution_and_message_name_actual_queue(setup, queue):
    assert traffic.acquire("caption_fetch")["ok"]
    snapshots = []
    stream = Mock()

    class Cancel:
        def is_set(self):
            return False
        def wait(self, timeout):
            snapshots.append(traffic.wait_status())
            return True

    with traffic.request_scope(queue, task_id="waiting-job"):
        assert traffic.acquire("caption_fetch", cancel_event=Cancel(), stream=stream)["cancelled"]
    current = snapshots[0]
    assert current["queue"] == queue and current["kind"] == "caption_fetch"
    assert current["task_id"] == "waiting-job"
    assert current["waits"][0]["active"]
    label = {"gpu": "Processing", "sync": "sync", "background": "background work"}[queue]
    assert f"{label} will continue" in str(stream.emit.call_args_list)
    assert not traffic.wait_status()["active"]


@pytest.mark.parametrize("limit", ["hourly", "daily"])
def test_nonwaiting_admission_defers_without_charge_sleep_or_visible_wait(setup, limit):
    p = setup
    p.config[f"youtube_traffic_custom_{'daily' if limit == 'hourly' else 'hourly'}"] = 100
    assert traffic.acquire("caption_fetch")["ok"]
    p.monkeypatch.setattr(traffic.time, "sleep", Mock(side_effect=AssertionError("unexpected wait")))
    stream = Mock()
    result = traffic.acquire("youtube_thumbnail", stream=stream, wait_for_budget=False)
    assert result["deferred"] and result["reason"] == f"{limit}_limit"
    assert traffic.status()["daily_used"] == 1
    assert traffic.wait_status()["waits"] == []
    stream.emit.assert_not_called()


def test_gpu_override_api_does_not_enable_global_bypass(setup):
    p = setup
    p.manager._current_job = p.job("current", "pass")
    traffic._set_wait_state({"active": True, "queue": "gpu", "until": 100})
    api = SimpleNamespace(_transcribe=p.manager, _on_queue_changed=Mock(),
                          _queue_log_stream=Mock(return_value=Mock()))
    assert QueueMixin.youtube_traffic_override(api, "gpu")["ok"]
    assert p.manager._current_job["traffic_override"]
    assert not traffic.budget_override_active()
    assert not QueueMixin.youtube_traffic_override(api, "sync")["ok"]


def test_sync_override_api_persists_followups_before_global_activation(setup):
    p = setup
    pass_id = traffic.begin_sync_pass()
    p.manager._jobs = [p.job("pending", pass_id)]
    traffic._set_wait_state({"active": True, "queue": "sync", "until": 100})
    api = SimpleNamespace(_transcribe=p.manager, _on_queue_changed=Mock(),
                          _queue_log_stream=Mock(return_value=Mock()))
    p.monkeypatch.setattr(p.manager, "_write_pending_snapshot", lambda _: False)
    assert not QueueMixin.youtube_traffic_override(api)["ok"]
    assert not traffic.budget_override_active()
    assert not p.manager._jobs[0]["traffic_override"]


def test_sync_finishes_during_authorization_without_orphan_global_override(setup):
    p = setup
    pass_id = traffic.begin_sync_pass()
    p.manager._jobs = [p.job("pending", pass_id)]
    traffic._set_wait_state({"active": True, "queue": "sync", "until": 100})
    write = p.manager._write_pending_snapshot

    def finish_during_save(snapshot):
        traffic.finish_sync_pass(pass_id)
        p.manager.finish_traffic_pass(pass_id)
        return write(snapshot)

    p.monkeypatch.setattr(p.manager, "_write_pending_snapshot", finish_during_save)
    api = SimpleNamespace(_transcribe=p.manager, _on_queue_changed=Mock(),
                          _queue_log_stream=Mock(return_value=Mock()))
    result = QueueMixin.youtube_traffic_override(api)
    assert result["ok"] and result["followups_only"]
    assert p.manager._jobs[0]["traffic_override"]
    assert not p.manager._authorized_traffic_passes
    assert not traffic.budget_override_active()
    new_pass = traffic.begin_sync_pass()
    assert not traffic.override_budget_limits(pass_id=pass_id)["ok"]
    traffic.finish_sync_pass(pass_id)
    assert traffic.current_sync_pass_id() == new_pass


def test_new_processing_job_cannot_receive_stale_wait_override(setup):
    p = setup
    p.manager._current_job = p.job("replacement", "other-pass")
    traffic._set_wait_state({"active": True, "queue": "gpu", "task_id": "completed", "until": 100})
    api = SimpleNamespace(_transcribe=p.manager, _on_queue_changed=Mock(),
                          _queue_log_stream=Mock(return_value=Mock()))
    assert not QueueMixin.youtube_traffic_override(api, "gpu")["ok"]
    assert not p.manager._current_job["traffic_override"]
    traffic._set_wait_state({"active": True, "queue": "gpu", "task_id": "replacement", "until": 100})
    assert not QueueMixin.youtube_traffic_override(api, "gpu", "completed")["ok"]
    assert not p.manager._current_job["traffic_override"]


def test_persisted_processing_grant_wakes_already_waiting_task_promptly(setup):
    p = setup
    assert traffic.acquire("caption_fetch")["ok"]
    job = p.job("current", "pass")
    p.manager._current_job = job
    result = []

    def wait():
        with traffic.request_scope("gpu", override_allowed=lambda: job["traffic_override"]):
            result.append(traffic.acquire("caption_fetch", cancel_event=job["cancel"]))

    worker = threading.Thread(target=wait, daemon=True)
    worker.start()
    try:
        deadline = time.monotonic() + 2
        while not traffic.wait_status()["active"]:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        assert p.manager.authorize_traffic_followups()["ok"]
        worker.join(1)
        assert not worker.is_alive()
        assert result[0]["ok"] and result[0]["override"]
        assert not traffic.budget_override_active()
    finally:
        job["cancel"].set()
        worker.join(1)


def test_nonwaiting_budget_mode_still_obeys_normal_pacing(setup):
    p = setup
    p.config["youtube_traffic_custom_daily"] = 100
    p.config["youtube_traffic_custom_hourly"] = 100
    p.config["youtube_traffic_custom_min_gap"] = 5
    p.config["youtube_traffic_custom_max_gap"] = 5
    assert traffic.acquire("caption_fetch")["ok"]
    now = time.time()
    delays = []
    p.monkeypatch.setattr(traffic.time, "time", lambda: now)

    def sleep(seconds):
        nonlocal now
        delays.append(seconds)
        now += seconds

    p.monkeypatch.setattr(traffic.time, "sleep", sleep)
    # An authorized scope uses sleep rather than waiting on a wakeup event.
    with traffic.request_scope("gpu", override_allowed=True):
        assert traffic.acquire("caption_fetch", wait_for_budget=False)["ok"]
    assert sum(delays) >= 4.9
    assert traffic.status()["daily_used"] == 2


def test_aggregate_wait_snapshot_retains_both_queues(setup):
    barrier = threading.Barrier(3)
    done = threading.Event()

    def wait(queue, until):
        traffic._set_wait_state({"active": True, "queue": queue, "kind": "caption_fetch",
                                 "until": until})
        barrier.wait(timeout=2)
        done.wait(2)
        traffic._set_wait_state(None)

    workers = [threading.Thread(target=wait, args=("sync", 100)),
               threading.Thread(target=wait, args=("gpu", 200))]
    for worker in workers:
        worker.start()
    try:
        barrier.wait(timeout=2)
        snapshot = traffic.wait_status()
        assert snapshot["queue"] == "gpu"
        assert {row["queue"] for row in snapshot["waits"]} == {"sync", "gpu"}
    finally:
        done.set()
        for worker in workers:
            worker.join(2)
    assert not traffic.wait_status()["active"]


def test_sync_override_applies_only_to_matching_pass_context(setup):
    assert traffic.acquire("channel_sync")["ok"]
    old_pass = traffic.begin_sync_pass()
    with traffic.request_scope("sync", sync_pass_id=old_pass):
        old_context = traffic.current_request_scope()
    traffic.finish_sync_pass(old_pass)
    current_pass = traffic.begin_sync_pass()
    assert traffic.override_budget_limits(pass_id=current_pass)["ok"]
    with traffic.request_scope("sync", sync_pass_id=current_pass):
        assert traffic.acquire("channel_sync", wait_for_budget=False)["ok"]
    for context in ({"queue": "gpu"}, {"queue": "background"}, old_context):
        with traffic.request_scope(**context):
            assert traffic.acquire("youtube_http", wait_for_budget=False)["deferred"]
    # A kind string alone cannot impersonate the active sync pass.
    assert traffic.acquire("channel_sync", wait_for_budget=False)["deferred"]


def test_unrelated_wait_does_not_spin_on_active_pass_wakeup(setup):
    assert traffic.acquire("channel_sync")["ok"]
    pass_id = traffic.begin_sync_pass()
    assert traffic.override_budget_limits(pass_id=pass_id)["ok"]
    delays = []

    class Cancel:
        def is_set(self):
            return False
        def wait(self, timeout):
            delays.append(timeout)
            return True

    with traffic.request_scope("background"):
        assert traffic.acquire("youtube_thumbnail", cancel_event=Cancel())["cancelled"]
    assert delays and delays[0] > 0


def test_broker_retains_sync_pass_identity_across_worker_threads(setup):
    assert traffic.acquire("channel_sync")["ok"]
    pass_id = traffic.begin_sync_pass()
    with traffic.request_scope("sync", sync_pass_id=pass_id):
        broker = RequestSession(SimpleNamespace(port=1, discard=lambda _: None), "")
    assert traffic.override_budget_limits(pass_id=pass_id)["ok"]
    result = []
    worker = threading.Thread(target=lambda: result.append(broker.acquire("youtube_http")))
    worker.start()
    worker.join(2)
    assert not worker.is_alive() and result[0]["ok"]
    traffic.finish_sync_pass(pass_id)
    other = traffic.begin_sync_pass()
    traffic.override_budget_limits(pass_id=other)
    original_acquire = traffic.acquire
    setup.monkeypatch.setattr(traffic, "acquire", lambda kind, **kw: original_acquire(
        kind, **{**kw, "wait_for_budget": False}))
    assert not broker.acquire("youtube_http")["ok"]
