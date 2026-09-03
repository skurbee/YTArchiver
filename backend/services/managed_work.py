"""Small adapters for atomically supervised API and startup work."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from .channel_leases import (
    LeaseAcquireResult,
    LeaseOwner,
    channel_leases,
    global_archive_aliases,
)


@contextmanager
def admitted_operation(
    api: Any,
    *,
    owner: str,
    label: str,
    task_id: str = "",
    cancel: threading.Event | None = None,
) -> Iterator[threading.Event]:
    """Register synchronous work, falling back safely for narrow test doubles."""
    from backend.process_runner import process_owner_scope

    token = cancel or threading.Event()
    stable_id = str(task_id or "").strip() or uuid.uuid4().hex
    supervisor = getattr(api, "_job_supervisor", None)
    if supervisor is None:
        with process_owner_scope(owner, stable_id):
            yield token
        return
    with supervisor.operation_scope(
        owner=owner,
        label=label,
        task_id=stable_id,
        cancel=token,
    ) as admitted_cancel:
        with process_owner_scope(owner, stable_id):
            yield admitted_cancel


def start_managed_task(
    api: Any,
    *,
    owner: str,
    label: str,
    target: Callable[[], Any],
    task_id: str = "",
    cancel: threading.Event | None = None,
    force: Callable[[], Any] | None = None,
    name: str | None = None,
    thread_factory: Callable[..., threading.Thread] = threading.Thread,
) -> threading.Thread:
    """Register-before-start when an Api supervisor is available."""
    from backend.process_runner import process_owner_scope

    stable_id = str(task_id or "").strip() or uuid.uuid4().hex

    def _owned_target() -> Any:
        with process_owner_scope(owner, stable_id):
            return target()

    supervisor = getattr(api, "_job_supervisor", None)
    if supervisor is not None:
        return supervisor.start_task(
            owner=owner,
            label=label,
            target=_owned_target,
            task_id=stable_id,
            cancel=cancel,
            force=force,
            name=name,
            daemon=True,
        )
    try:
        thread = thread_factory(target=_owned_target, name=name, daemon=True)
    except TypeError:
        # A few narrow unit-test doubles model the older two-argument Thread
        # construction.  Production ``threading.Thread`` accepts ``name``.
        thread = thread_factory(target=_owned_target, daemon=True)
    thread.start()
    return thread


def try_global_archive_lease(
    *,
    owner: str,
    label: str,
    task_id: str = "",
    cancel: threading.Event | None = None,
) -> LeaseAcquireResult:
    """Try the conservative all-archive lease used by maintenance jobs."""
    stable_id = str(task_id or "").strip() or uuid.uuid4().hex
    return channel_leases.try_acquire(
        global_archive_aliases(),
        LeaseOwner(
            owner=owner,
            job_id=stable_id,
            task_id=stable_id,
            label=label,
            kind="maintenance",
        ),
        cancel_event=cancel,
    )


def lease_busy_result(result: LeaseAcquireResult) -> dict[str, Any]:
    return {
        "ok": False,
        "started": False,
        "busy": result.status in {"busy", "timeout"},
        "cancelled": result.status == "cancelled",
        "error": result.explanation,
        "blockers": [blocker.as_dict() for blocker in result.blockers],
    }


__all__ = [
    "admitted_operation",
    "lease_busy_result",
    "start_managed_task",
    "try_global_archive_lease",
]
