"""JS-callable Trash API backed by opaque entry identities."""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from backend.services.job_supervisor import WorkAdmissionClosed
from backend.services.managed_work import (
    admitted_operation,
    lease_busy_result,
    start_managed_task,
    try_global_archive_lease,
)
from backend.trash_manager import trash_manager

_operations_lock = threading.Lock()
_operations: dict[str, dict[str, Any]] = {}
_operation_cancels: dict[str, threading.Event] = {}


def _remember(operation_id: str, value: dict[str, Any]) -> None:
    with _operations_lock:
        _operations[operation_id] = dict(value)
        # Bound process-lifetime UI history. Entries are insertion ordered.
        while len(_operations) > 100:
            oldest = next(iter(_operations))
            _operations.pop(oldest, None)
            _operation_cancels.pop(oldest, None)


def _entry_identity(payload: Any) -> tuple[str, int | None] | None:
    if isinstance(payload, str):
        entry_id = payload.strip()
        epoch = None
    elif isinstance(payload, dict):
        entry_id = str(
            payload.get("id") or payload.get("entry_id") or "").strip()
        raw_epoch = payload.get("epoch")
        if raw_epoch in (None, ""):
            epoch = None
        else:
            try:
                epoch = int(raw_epoch)
            except (TypeError, ValueError):
                return None
    else:
        return None
    if not entry_id or (epoch is not None and epoch < 0):
        return None
    return entry_id, epoch


class TrashMixin:
    def _trash_event(self, name: str, payload: dict[str, Any]) -> None:
        services = getattr(self, "services", None)
        event_bus = getattr(services, "event_bus", None)
        if event_bus is not None:
            event_bus.call(name, payload)

    def _trash_start(
        self,
        operation: str,
        label: str,
        work: Callable[[threading.Event], dict[str, Any]],
    ) -> dict[str, Any]:
        operation_id = uuid.uuid4().hex
        cancel = threading.Event()
        started = {
            "ok": True,
            "started": True,
            "operation_id": operation_id,
            "operation": operation,
            "status": "running",
            "started_at": time.time(),
        }
        with _operations_lock:
            _operation_cancels[operation_id] = cancel
        _remember(operation_id, started)

        def _worker() -> None:
            try:
                result = work(cancel)
                final = {
                    **result,
                    "operation_id": operation_id,
                    "operation": operation,
                    "started": True,
                    "status": result.get("status") or (
                        "completed" if result.get("ok") else "failed"),
                    "finished_at": time.time(),
                }
            except Exception as exc:
                final = {
                    "ok": False,
                    "started": True,
                    "operation_id": operation_id,
                    "operation": operation,
                    "status": "failed",
                    "error": f"Internal Trash error: {exc}",
                    "finished_at": time.time(),
                }
            _remember(operation_id, final)
            with _operations_lock:
                _operation_cancels.pop(operation_id, None)
            self._trash_event("_onTrashOperation", final)
            if operation != "refresh":
                try:
                    self._trash_event("_onTrashChanged", trash_manager.list_entries())
                except Exception:
                    pass

        start_managed_task(
            self,
            owner="trash",
            label=label,
            target=_worker,
            task_id=operation_id,
            cancel=cancel,
            name=f"yta-trash-{operation}",
        )
        return started

    def trash_list(self):
        return trash_manager.list_entries()

    def trash_summary(self):
        return trash_manager.summary()

    def trash_refresh(self):
        def _work(_cancel):
            snapshot = trash_manager.list_entries()
            self._trash_event("_onTrashChanged", snapshot)
            return snapshot

        return self._trash_start("refresh", "Refresh Trash", _work)

    def trash_restore(self, payload):
        admission = getattr(self, "_work_admission_error", None)
        if callable(admission):
            blocked = admission("a Trash restore")
            if blocked is not None:
                return blocked
        identity = _entry_identity(payload)
        if identity is None:
            return {"ok": False, "started": False,
                    "error": "Select a current Trash entry and try again."}
        entry_id, epoch = identity
        # A restored channel must not race a sync start between config and
        # folder publication.  Real Api pre-initializes this lock; retain a
        # fallback for narrow test doubles.
        if not hasattr(self, "_sync_mutation_lock"):
            self._sync_mutation_lock = threading.RLock()
        task_id = uuid.uuid4().hex
        try:
            with admitted_operation(
                self,
                owner="trash",
                label="Restore Trash item",
                task_id=task_id,
            ) as cancel:
                lease_result = try_global_archive_lease(
                    owner="trash",
                    label="Restore Trash item",
                    task_id=task_id,
                    cancel=cancel,
                )
                if not lease_result.ok or lease_result.lease is None:
                    return lease_busy_result(lease_result)
                with lease_result.lease, self._sync_mutation_lock:
                    result = trash_manager.restore(
                        entry_id, epoch, cancel_event=cancel)
        except WorkAdmissionClosed as exc:
            return {"ok": False, "started": False, "error": str(exc)}
        if result.get("ok"):
            self._trash_event("_onTrashChanged", trash_manager.summary())
        return result

    def trash_purge(self, payload):
        admission = getattr(self, "_work_admission_error", None)
        if callable(admission):
            blocked = admission("a permanent Trash deletion")
            if blocked is not None:
                return blocked
        identity = _entry_identity(payload)
        if identity is None:
            return {"ok": False, "started": False,
                    "error": "Select a current Trash entry and try again."}
        entry_id, epoch = identity
        task_id = uuid.uuid4().hex
        try:
            with admitted_operation(
                self,
                owner="trash",
                label="Permanently delete Trash item",
                task_id=task_id,
            ) as cancel:
                lease_result = try_global_archive_lease(
                    owner="trash",
                    label="Permanently delete Trash item",
                    task_id=task_id,
                    cancel=cancel,
                )
                if not lease_result.ok or lease_result.lease is None:
                    return lease_busy_result(lease_result)
                with lease_result.lease:
                    result = trash_manager.purge(
                        entry_id, epoch, cancel_event=cancel)
        except WorkAdmissionClosed as exc:
            return {"ok": False, "started": False, "error": str(exc)}
        if result.get("ok"):
            self._trash_event("_onTrashChanged", trash_manager.summary())
        return result

    def trash_empty(self, payload=None):
        admission = getattr(self, "_work_admission_error", None)
        if callable(admission):
            blocked = admission("Empty Trash")
            if blocked is not None:
                return blocked
        root_id = ""
        if isinstance(payload, dict):
            root_id = str(payload.get("root_id") or "").strip()
        task_id = uuid.uuid4().hex
        try:
            with admitted_operation(
                self,
                owner="trash",
                label="Empty Trash",
                task_id=task_id,
            ) as cancel:
                lease_result = try_global_archive_lease(
                    owner="trash",
                    label="Empty Trash",
                    task_id=task_id,
                    cancel=cancel,
                )
                if not lease_result.ok or lease_result.lease is None:
                    return lease_busy_result(lease_result)
                with lease_result.lease:
                    result = trash_manager.empty(
                        root_id, cancel_event=cancel)
        except WorkAdmissionClosed as exc:
            return {"ok": False, "started": False, "error": str(exc)}
        self._trash_event("_onTrashChanged", trash_manager.summary())
        return result

    def trash_open(self, payload):
        identity = _entry_identity(payload)
        if identity is None:
            return {"ok": False,
                    "error": "Select a current Trash entry and try again."}
        return trash_manager.open_entry(*identity)

    def trash_open_folder(self):
        return trash_manager.open_folder()

    def trash_operation_state(self, operation_id):
        key = str(operation_id or "").strip()
        with _operations_lock:
            value = _operations.get(key)
            return dict(value) if value is not None else {
                "ok": False,
                "status": "unknown",
                "error": "Trash operation was not found.",
            }

    def trash_cancel(self, operation_id):
        key = str(operation_id or "").strip()
        with _operations_lock:
            cancel = _operation_cancels.get(key)
        if cancel is None:
            return {"ok": False, "error": "Trash operation is not running."}
        cancel.set()
        return {"ok": True, "cancel_requested": True,
                "operation_id": key}

    # Explicit aliases make the bridge contract self-documenting while keeping
    # concise names available to the UI.
    trash_list_entries = trash_list
    trash_restore_entry = trash_restore
    trash_purge_entry = trash_purge
    trash_open_entry = trash_open
