"""Explicit process lease ownership and fail-closed restart handoff."""
from unittest.mock import Mock

import pytest

from backend.services.instance_lease import InstanceLease


def test_lease_has_one_owner_and_release_is_idempotent():
    acquire = Mock(return_value=(123, False))
    release = Mock(return_value=True)
    lease = InstanceLease(acquire, release)
    assert lease.acquire() and lease.acquire()
    acquire.assert_called_once()
    lease.release()
    lease.release()
    release.assert_called_once_with(123)


def test_failed_release_retains_handle_for_retry():
    release = Mock(side_effect=[False, True])
    lease = InstanceLease(lambda: (123, False), release)
    assert lease.acquire()
    with pytest.raises(OSError, match="release"):
        lease.release()
    lease.release()
    assert release.call_count == 2


def test_existing_instance_closes_only_its_duplicate_handle():
    close = Mock(return_value=True)
    lease = InstanceLease(lambda: (123, True), close)
    assert not lease.acquire()
    lease.release()
    close.assert_called_once_with(123)


def test_restart_does_not_launch_after_lease_release_failure(monkeypatch):
    from backend.api_mixins import window_mixin

    class ImmediateThread:
        def __init__(self, target, **kwargs):
            self.target = target

        def start(self):
            self.target()

    api = window_mixin.WindowMixin()
    api._window = Mock()
    api._shutdown_cleanup_fn = Mock(return_value={"ok": True})
    api._release_instance_lease = Mock(side_effect=OSError("native close failed"))
    spawn = Mock()
    exit_process = Mock()
    monkeypatch.setattr(window_mixin.threading, "Thread", ImmediateThread)
    monkeypatch.setattr(window_mixin.time, "sleep", lambda _: None)
    monkeypatch.setattr(window_mixin.subprocess, "Popen", spawn)
    monkeypatch.setattr(window_mixin.os, "_exit", exit_process)
    assert api.app_restart() == {"ok": True}
    api._shutdown_cleanup_fn.assert_called_once()
    api._release_instance_lease.assert_called_once()
    spawn.assert_not_called()
    api._window.destroy.assert_not_called()
    exit_process.assert_not_called()
