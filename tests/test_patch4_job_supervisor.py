from __future__ import annotations

import threading
import time

import pytest

from backend.services.job_supervisor import (
    JobSupervisor,
    OwnerAdapter,
    WorkAdmissionClosed,
)


def test_supervisor_closes_admission_and_lists_every_registered_owner():
    supervisor = JobSupervisor()
    prepared = []
    supervisor.register_owner(OwnerAdapter(
        owner="sync",
        label="Sync worker",
        active=lambda: False,
        prepare=lambda: prepared.append("sync") or True,
        join=lambda _timeout: True,
        force=lambda: 0,
        task_id=lambda: "sync-stable-id",
        details=lambda: {"outcome": "idle"},
    ))

    before = supervisor.snapshot()
    assert before["accepting"] is True
    assert before["owners"] == [{
        "owner": "sync",
        "label": "Sync worker",
        "active": False,
        "task_id": "sync-stable-id",
        "outcome": "idle",
    }]

    report = supervisor.quiesce(reason="test shutdown", timeout=0.1)

    assert report["ok"] is True
    assert prepared == ["sync"]
    assert supervisor.snapshot()["accepting"] is False
    with pytest.raises(WorkAdmissionClosed, match="test shutdown"):
        supervisor.require_admission("another job")


def test_supervisor_uses_one_deadline_then_forces_only_active_owner():
    supervisor = JobSupervisor()
    active = {"slow": True, "idle": False}
    forced = []

    def join_slow(timeout):
        time.sleep(max(0.0, float(timeout)))
        return False

    def force_slow():
        forced.append("slow")
        active["slow"] = False
        return 1

    supervisor.register_owner(OwnerAdapter(
        "slow", "Slow owner", lambda: active["slow"],
        lambda: True, join_slow, force_slow,
    ))
    supervisor.register_owner(OwnerAdapter(
        "idle", "Idle owner", lambda: active["idle"],
        lambda: True, lambda _timeout: True,
        lambda: forced.append("idle"),
    ))

    started = time.monotonic()
    report = supervisor.quiesce(reason="bounded test", timeout=0.15)
    elapsed = time.monotonic() - started

    assert report["ok"] is True
    assert forced == ["slow"]
    assert report["remaining"] == []
    assert elapsed < 0.35


def test_failed_checkpoint_prevents_safe_restore_result():
    supervisor = JobSupervisor()
    supervisor.register_owner(OwnerAdapter(
        "queue", "Durable queue", lambda: False,
        lambda: False, lambda _timeout: True, lambda: 0,
    ))

    report = supervisor.quiesce(reason="restore", timeout=0)

    assert report["ok"] is False
    assert report["remaining"] == []
    assert report["prepared"] == [{"owner": "queue", "prepared": False}]
    assert "checkpoint" in report["error"].lower()


def test_registered_but_not_started_task_cannot_run_after_admission_closes(
        monkeypatch):
    from backend.services import job_supervisor as module

    supervisor = JobSupervisor()
    held = threading.Event()
    release = threading.Event()
    ran = threading.Event()
    original_start = module.threading.Thread.start

    def _held_start(thread):
        if thread.name == "held-start":
            held.set()
            assert release.wait(2)
        return original_start(thread)

    monkeypatch.setattr(module.threading.Thread, "start", _held_start)

    launcher = threading.Thread(target=lambda: supervisor.start_task(
        owner="metadata",
        label="Metadata write",
        target=ran.set,
        name="held-start",
    ))
    launcher.start()
    assert held.wait(2)

    result = {}
    closer = threading.Thread(target=lambda: result.update(
        supervisor.quiesce(reason="restore", timeout=0.5)))
    closer.start()
    deadline = time.monotonic() + 1
    while supervisor.accepting_work() and time.monotonic() < deadline:
        time.sleep(0.005)
    assert not supervisor.accepting_work()
    release.set()
    launcher.join(2)
    closer.join(2)

    assert not ran.is_set()
    assert result["ok"] is True
    assert result["remaining"] == []


def test_prepare_and_force_callbacks_share_one_absolute_deadline():
    supervisor = JobSupervisor()

    def _block():
        time.sleep(1)
        return False

    for index in range(3):
        supervisor.register_owner(OwnerAdapter(
            f"blocked-{index}",
            f"Blocked {index}",
            lambda: True,
            _block,
            lambda timeout: time.sleep(min(1, max(0, timeout))) or False,
            _block,
        ))

    started = time.monotonic()
    report = supervisor.quiesce(reason="bounded", timeout=0.2)
    elapsed = time.monotonic() - started

    assert report["ok"] is False
    assert elapsed < 0.35


def test_operation_scope_is_immediately_joinable_during_quiesce():
    supervisor = JobSupervisor()
    entered = threading.Event()
    exited = threading.Event()

    def _operation():
        with supervisor.operation_scope(
            owner="backup-export",
            label="Export backup",
            task_id="backup-1",
        ) as cancel:
            entered.set()
            cancel.wait(2.0)
        exited.set()

    worker = threading.Thread(target=_operation, name="scoped-operation")
    worker.start()
    assert entered.wait(1.0)

    started = time.monotonic()
    report = supervisor.quiesce(reason="restore", timeout=0.5)
    elapsed = time.monotonic() - started

    worker.join(1.0)
    assert report["ok"] is True
    assert report["remaining"] == []
    assert exited.is_set()
    assert elapsed < 0.4
