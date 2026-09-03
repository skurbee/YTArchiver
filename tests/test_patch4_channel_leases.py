import os
import tempfile
import threading
import time
from pathlib import Path

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch4-leases-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend.services.channel_leases import (  # noqa: E402
    GLOBAL_ARCHIVE_ALIAS,
    ChannelLeaseManager,
    LeaseOwner,
    channel_aliases,
    global_archive_aliases,
    path_alias,
)


def _owner(job_id: str, *, owner: str = "tests", label: str = "Test job") -> LeaseOwner:
    return LeaseOwner(owner, job_id, label=label, task_id=f"task-{job_id}")


def test_channel_aliases_normalize_stable_identity_and_every_known_path(tmp_path):
    current = tmp_path / "Archive" / "Channel"
    equivalent = current / "subfolder" / ".."
    old = tmp_path / "Archive" / "Old Channel"

    first = channel_aliases(
        {"name": "A mutable name", "url": "https://www.youtube.com/@Example/Videos/?x=1"},
        channel_id="UCaBc123",
        paths=[current, old],
    )
    second = channel_aliases(
        {"name": "A different name", "url": "@example"},
        channel_id="UCaBc123",
        paths=equivalent,
    )

    assert "channel-url:youtube.com/@example" in first & second
    assert "channel-id:UCaBc123" in first & second
    assert path_alias(current) in first & second
    assert path_alias(old) in first
    assert all("mutable name" not in alias.lower() for alias in first)


def test_nonblocking_conflict_reports_owner_and_job_metadata():
    manager = ChannelLeaseManager()
    aliases = channel_aliases(url="https://youtube.com/@busy")
    holder = LeaseOwner("reorganize", "move-42", label="Move channel folder", task_id="ui-7")
    held = manager.try_acquire(aliases, holder)
    assert held.ok

    blocked = manager.try_acquire(aliases, _owner("sync-1", owner="sync"))
    assert not blocked.ok
    assert blocked.status == "busy"
    assert blocked.lease is None
    assert len(blocked.blockers) == 1
    assert blocked.blockers[0].owner == "reorganize"
    assert blocked.blockers[0].job_id == "move-42"
    assert blocked.blockers[0].task_id == "ui-7"
    assert "Move channel folder" in blocked.explanation
    assert "move-42" not in blocked.explanation
    assert "ui-7" not in blocked.explanation
    assert "using this channel" not in blocked.explanation
    assert "Try again after the active work finishes" in blocked.explanation
    assert held.lease is not None and held.lease.release()


def test_same_owner_and_job_is_reentrant_until_every_token_is_released():
    manager = ChannelLeaseManager()
    aliases = channel_aliases(url="@repeat")
    owner = _owner("same")
    first = manager.try_acquire(aliases, owner)
    second = manager.try_acquire(aliases, owner)
    assert first.ok and second.ok
    assert manager.active_snapshot()[0].depth == 2

    different_job = manager.try_acquire(aliases, _owner("different"))
    assert different_job.status == "busy"
    assert first.lease is not None and first.lease.release()
    assert manager.try_acquire(aliases, _owner("still-blocked")).status == "busy"
    assert second.lease is not None and second.lease.release()

    free = manager.try_acquire(aliases, _owner("now-free"))
    assert free.ok
    assert free.lease is not None
    assert free.lease.release()
    assert not free.lease.release()


def test_acquire_many_is_atomic_and_never_keeps_a_partial_hold():
    manager = ChannelLeaseManager()
    channel_a = channel_aliases(url="@channel-a")
    channel_b = channel_aliases(url="@channel-b")
    holder = manager.try_acquire(channel_b, _owner("holder"))
    assert holder.ok

    combined = manager.try_acquire_many([channel_a, channel_b], _owner("combined"))
    assert combined.status == "busy"

    # If the failed combined request had retained channel A, this independent
    # request would also be busy.
    independent = manager.try_acquire(channel_a, _owner("independent"))
    assert independent.ok
    assert independent.lease is not None and independent.lease.release()
    assert holder.lease is not None and holder.lease.release()


def test_global_archive_alias_excludes_channels_in_both_directions():
    manager = ChannelLeaseManager()
    channel = channel_aliases(url="@global-test")
    global_owner = _owner("backup", owner="backup", label="Restore backup")
    global_held = manager.try_acquire(global_archive_aliases(), global_owner)
    assert global_held.ok
    assert GLOBAL_ARCHIVE_ALIAS in global_held.lease.aliases
    assert manager.try_acquire(channel, _owner("sync")).status == "busy"

    nested = manager.try_acquire(channel, global_owner)
    assert nested.ok
    assert nested.lease is not None and nested.lease.release()
    assert global_held.lease is not None and global_held.lease.release()

    channel_held = manager.try_acquire(channel, _owner("channel"))
    assert channel_held.ok
    assert manager.try_acquire(global_archive_aliases(), _owner("other-backup")).status == "busy"
    assert channel_held.lease is not None and channel_held.lease.release()


def test_bounded_wait_times_out_without_leaking_a_lease():
    manager = ChannelLeaseManager()
    aliases = channel_aliases(url="@timeout")
    holder = manager.try_acquire(aliases, _owner("holder"))
    started = time.monotonic()
    result = manager.acquire(aliases, _owner("waiter"), timeout=0.05, poll_interval=0.01)
    elapsed = time.monotonic() - started

    assert result.status == "timeout"
    assert result.lease is None
    assert elapsed >= 0.03
    assert elapsed < 0.5
    assert holder.lease is not None and holder.lease.release()
    assert manager.active_snapshot() == ()


def test_bounded_wait_can_be_cancelled_while_another_job_holds_aliases():
    manager = ChannelLeaseManager()
    aliases = channel_aliases(url="@cancel")
    holder = manager.try_acquire(aliases, _owner("holder"))
    cancel = threading.Event()
    started = threading.Event()
    results = []

    def wait_for_lease():
        started.set()
        results.append(
            manager.acquire(
                aliases,
                _owner("waiter"),
                timeout=1.0,
                cancel_event=cancel,
                poll_interval=0.01,
            )
        )

    thread = threading.Thread(target=wait_for_lease, daemon=True)
    thread.start()
    assert started.wait(0.5)
    cancel.set()
    thread.join(0.5)

    assert not thread.is_alive()
    assert len(results) == 1
    assert results[0].status == "cancelled"
    assert results[0].lease is None
    assert holder.lease is not None and holder.lease.release()


def test_waiter_acquires_after_release_notification():
    manager = ChannelLeaseManager()
    aliases = channel_aliases(url="@notification")
    holder = manager.try_acquire(aliases, _owner("holder"))
    started = threading.Event()
    results = []

    def wait_for_lease():
        started.set()
        results.append(manager.acquire(aliases, _owner("waiter"), timeout=1.0, poll_interval=0.2))

    thread = threading.Thread(target=wait_for_lease, daemon=True)
    thread.start()
    assert started.wait(0.5)
    time.sleep(0.02)
    assert holder.lease is not None and holder.lease.release()
    thread.join(0.5)

    assert not thread.is_alive()
    assert results and results[0].ok
    assert results[0].lease is not None and results[0].lease.release()


def test_active_snapshot_is_deterministic_json_ready_and_clears():
    manager = ChannelLeaseManager()
    second = manager.try_acquire("custom:b", _owner("b", label="Second"))
    first = manager.try_acquire("custom:a", _owner("a", label="First"))
    assert second.ok and first.ok

    snapshot = manager.active_snapshot()
    assert [item.job_id for item in snapshot] == ["a", "b"]
    assert snapshot[0].aliases == ("custom:a",)
    assert snapshot[0].depth == 1
    assert snapshot[0].held_seconds >= 0
    assert snapshot[0].as_dict()["aliases"] == ["custom:a"]

    assert first.lease is not None and first.lease.release()
    assert second.lease is not None and second.lease.release()
    assert manager.active_snapshot() == ()


def test_invalid_waits_and_empty_alias_sets_are_rejected():
    manager = ChannelLeaseManager()
    owner = _owner("invalid")
    for timeout in (-1, float("inf"), float("nan")):
        try:
            manager.acquire("custom:a", owner, timeout=timeout)
        except ValueError:
            pass
        else:
            raise AssertionError(f"invalid timeout {timeout!r} was accepted")

    try:
        manager.try_acquire_many([], owner)
    except ValueError:
        pass
    else:
        raise AssertionError("an empty atomic alias request was accepted")
