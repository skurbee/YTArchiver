"""One lifecycle registry for long-lived YTArchiver background owners."""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any


class WorkAdmissionClosed(RuntimeError):
    """Raised when a caller tries to start work during shutdown/restore."""


@dataclass(slots=True)
class OwnerAdapter:
    owner: str
    label: str
    active: Callable[[], bool]
    prepare: Callable[[], bool | None]
    join: Callable[[float], bool]
    force: Callable[[], Any]
    task_id: Callable[[], str] | None = None
    details: Callable[[], Mapping[str, Any]] | None = None


@dataclass(slots=True)
class ManagedTask:
    """One concrete admitted task, registered before it can mutate state."""

    key: str
    owner: str
    label: str
    task_id: str
    thread: threading.Thread
    cancel: threading.Event
    force: Callable[[], Any] | None = None
    started: threading.Event = field(default_factory=threading.Event)


class JobSupervisor:
    """Coordinates admission, checkpoint, bounded join, and exact force-stop.

    Features keep their existing worker implementations, but expose the same
    small ownership contract here.  Shutdown can therefore enumerate and stop
    every registered owner in one deterministic order and one global deadline.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._accepting = True
        self._close_reason = ""
        self._owners: dict[str, OwnerAdapter] = {}
        self._tasks: dict[str, ManagedTask] = {}

    def register_owner(self, adapter: OwnerAdapter) -> None:
        owner = str(adapter.owner or "").strip()
        if not owner:
            raise ValueError("background owner must have a name")
        with self._lock:
            if owner in self._owners:
                raise ValueError(f"background owner already registered: {owner}")
            self._owners[owner] = adapter

    def accepting_work(self) -> bool:
        with self._lock:
            return self._accepting

    def require_admission(self, operation: str = "background work") -> None:
        with self._lock:
            if self._accepting:
                return
            reason = self._close_reason or "application shutdown"
        raise WorkAdmissionClosed(f"Cannot start {operation}: {reason} is in progress")

    def close_admission(self, reason: str = "application shutdown") -> bool:
        with self._lock:
            changed = self._accepting
            self._accepting = False
            self._close_reason = str(reason or "application shutdown")
            return changed

    def _admit_locked(self, operation: str) -> None:
        if self._accepting:
            return
        reason = self._close_reason or "application shutdown"
        raise WorkAdmissionClosed(f"Cannot start {operation}: {reason} is in progress")

    def start_task(
        self,
        *,
        owner: str,
        label: str,
        target: Callable[[], Any],
        task_id: str = "",
        cancel: threading.Event | None = None,
        force: Callable[[], Any] | None = None,
        name: str | None = None,
        daemon: bool = True,
    ) -> threading.Thread:
        """Atomically admit, register, and start one concrete thread.

        Registration happens under the same lock used to close admission,
        before ``Thread.start``.  If shutdown/restore wins after registration,
        it cancels the reservation and the wrapper retires without entering
        user code; there is no unowned check-then-spawn gap.
        """
        if not callable(target):
            raise TypeError("managed task target must be callable")
        owner_name = str(owner or "background").strip() or "background"
        stable_id = str(task_id or "").strip() or uuid.uuid4().hex
        key = f"{owner_name}:{stable_id}:{uuid.uuid4().hex}"
        cancel_event = cancel or threading.Event()

        def _run() -> None:
            try:
                task.started.set()
                # Restore/shutdown may close admission after registration but
                # before the OS thread begins.  That reserved task must retire
                # without ever entering user code.
                if not task.cancel.is_set():
                    target()
            finally:
                with self._lock:
                    self._tasks.pop(key, None)

        thread = threading.Thread(
            target=_run,
            name=name or f"yta-{owner_name}-{stable_id[:8]}",
            daemon=daemon,
        )
        task = ManagedTask(
            key=key,
            owner=owner_name,
            label=str(label or owner_name),
            task_id=stable_id,
            thread=thread,
            cancel=cancel_event,
            force=force,
        )
        with self._lock:
            self._admit_locked(label or owner_name)
            self._tasks[key] = task
        try:
            thread.start()
        except BaseException:
            with self._lock:
                self._tasks.pop(key, None)
            raise
        return thread

    @contextmanager
    def operation_scope(
        self,
        *,
        owner: str,
        label: str,
        task_id: str = "",
        cancel: threading.Event | None = None,
        force: Callable[[], Any] | None = None,
    ):
        """Atomically register a synchronous bridge/background operation."""
        owner_name = str(owner or "background").strip() or "background"
        stable_id = str(task_id or "").strip() or uuid.uuid4().hex
        key = f"{owner_name}:{stable_id}:{uuid.uuid4().hex}"
        task = ManagedTask(
            key=key,
            owner=owner_name,
            label=str(label or owner_name),
            task_id=stable_id,
            thread=threading.current_thread(),
            cancel=cancel or threading.Event(),
            force=force,
        )
        # Unlike ``start_task``, this scope is already executing on its owner
        # thread when it is registered. Mark it started immediately so a
        # concurrent quiesce joins that thread instead of spending the whole
        # lifecycle budget waiting for a start signal that can never arrive.
        task.started.set()
        with self._lock:
            self._admit_locked(label or owner_name)
            self._tasks[key] = task
        try:
            yield task.cancel
        finally:
            with self._lock:
                self._tasks.pop(key, None)

    def _managed_tasks(self) -> list[ManagedTask]:
        with self._lock:
            return list(self._tasks.values())

    def _adapters(self) -> list[OwnerAdapter]:
        with self._lock:
            # Registration order is lifecycle order. The application registers
            # schedulers first, then active workers, and the exact-process
            # safety net last.
            return list(self._owners.values())

    @staticmethod
    def _safe_active(adapter: OwnerAdapter) -> bool:
        try:
            return bool(adapter.active())
        except Exception:
            # A broken status probe cannot prove that its worker stopped.
            return True

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            accepting = self._accepting
            reason = self._close_reason
        owners: list[dict[str, Any]] = []
        for adapter in self._adapters():
            row: dict[str, Any] = {
                "owner": adapter.owner,
                "label": adapter.label,
                "active": self._safe_active(adapter),
                "task_id": "",
            }
            if adapter.task_id is not None:
                try:
                    row["task_id"] = str(adapter.task_id() or "")
                except Exception as exc:
                    row["status_error"] = str(exc)
            if adapter.details is not None:
                try:
                    row.update(dict(adapter.details()))
                except Exception as exc:
                    row["details_error"] = str(exc)
            owners.append(row)
        for task in self._managed_tasks():
            owners.append({
                "owner": task.owner,
                "label": task.label,
                "active": True,
                "task_id": task.task_id,
                "dynamic": True,
                "thread": task.thread.name,
            })

        try:
            from backend.process_runner import PROCESS_REGISTRY
            processes = [
                {
                    "owner": record.owner,
                    "task_id": record.task_id,
                    "role": record.role,
                    "pid": record.pid,
                }
                for record in PROCESS_REGISTRY.snapshot()
            ]
        except Exception as exc:
            processes = [{"status_error": str(exc)}]
        return {
            "accepting": accepting,
            "close_reason": reason,
            "owners": owners,
            "processes": processes,
        }

    @staticmethod
    def _remaining(deadline: float) -> float:
        return max(0.0, deadline - time.monotonic())

    @staticmethod
    def _bounded_callbacks(
        callbacks: list[tuple[str, Callable[[], Any]]],
        deadline: float,
    ) -> list[dict[str, Any]]:
        """Run independent lifecycle callbacks inside one absolute deadline."""
        lock = threading.Lock()
        outcomes: dict[str, tuple[bool, Any]] = {}

        def _call(name: str, callback: Callable[[], Any]) -> None:
            try:
                value = callback()
                result = (True, value)
            except BaseException as exc:
                result = (False, exc)
            with lock:
                outcomes[name] = result

        threads: list[tuple[str, threading.Thread]] = []
        for index, (name, callback) in enumerate(callbacks):
            thread = threading.Thread(
                target=_call,
                args=(name, callback),
                name=f"yta-lifecycle-{index}",
                daemon=True,
            )
            threads.append((name, thread))
            thread.start()
        for _name, thread in threads:
            thread.join(JobSupervisor._remaining(deadline))

        rows: list[dict[str, Any]] = []
        with lock:
            snapshot = dict(outcomes)
        for name, _thread in threads:
            outcome = snapshot.get(name)
            if outcome is None:
                rows.append({"owner": name, "ok": False, "error": "deadline exceeded"})
            elif outcome[0]:
                rows.append({"owner": name, "ok": True, "result": outcome[1]})
            else:
                rows.append({"owner": name, "ok": False, "error": str(outcome[1])})
        return rows

    def prepare_all(self, deadline: float | None = None) -> list[dict[str, Any]]:
        for task in self._managed_tasks():
            task.cancel.set()
        adapters = self._adapters()
        if deadline is None:
            deadline = time.monotonic() + 30.0
        raw = self._bounded_callbacks(
            [(adapter.owner, adapter.prepare) for adapter in adapters], deadline
        )
        return [
            {
                "owner": row["owner"],
                "prepared": bool(row["ok"] and row.get("result") is not False),
                **({"error": row["error"]} if not row["ok"] else {}),
            }
            for row in raw
        ]

    def join_until(self, timeout: float) -> list[str]:
        """Join active owners within one shared monotonic deadline."""
        deadline = time.monotonic() + max(0.0, float(timeout))
        return self._join_until_deadline(deadline)

    def _join_until_deadline(self, deadline: float) -> list[str]:
        for adapter in self._adapters():
            if not self._safe_active(adapter):
                continue
            remaining = max(0.0, deadline - time.monotonic())
            try:
                adapter.join(remaining)
            except Exception:
                pass
        for task in self._managed_tasks():
            if task.thread is threading.current_thread():
                continue
            try:
                if not task.started.is_set():
                    task.started.wait(self._remaining(deadline))
                if task.started.is_set():
                    task.thread.join(self._remaining(deadline))
            except (RuntimeError, TypeError):
                pass
        remaining = [
            adapter.owner for adapter in self._adapters()
            if self._safe_active(adapter)
        ]
        remaining.extend(
            f"{task.owner}:{task.task_id}"
            for task in self._managed_tasks()
        )
        return remaining

    def force_remaining(self, deadline: float | None = None) -> list[dict[str, Any]]:
        """Force only adapters still active; never scan by process name."""
        callbacks = [
            (adapter.owner, adapter.force)
            for adapter in self._adapters()
            if self._safe_active(adapter)
        ]
        callbacks.extend(
            (f"{task.owner}:{task.task_id}", task.force)
            for task in self._managed_tasks()
            if task.force is not None
        )
        if deadline is None:
            deadline = time.monotonic() + 30.0
        return [
            {
                "owner": row["owner"],
                "forced": row["ok"],
                **({"result": row.get("result")} if row["ok"] else
                   {"error": row["error"]}),
            }
            for row in self._bounded_callbacks(callbacks, deadline)
        ]

    def quiesce(self, *, reason: str, timeout: float = 8.0) -> dict[str, Any]:
        """Close admission, checkpoint/cancel owners, and bound all waits."""
        started = time.monotonic()
        budget = max(0.01, float(timeout))
        deadline = started + budget
        prepare_deadline = min(deadline, started + budget * 0.20)
        join_deadline = min(deadline, started + budget * 0.75)
        self.close_admission(reason)
        before = self.snapshot()
        prepared = self.prepare_all(prepare_deadline)
        remaining = self._join_until_deadline(join_deadline)
        forced = self.force_remaining(deadline) if remaining else []
        if remaining:
            remaining = self._join_until_deadline(deadline)
        after = self.snapshot()
        failed_prepares = [row for row in prepared if not row["prepared"]]
        return {
            "ok": not remaining and not failed_prepares,
            "before": before,
            "prepared": prepared,
            "forced": forced,
            "remaining": remaining,
            "after": after,
            "error": (
                "Could not safely checkpoint all background owners"
                if failed_prepares else
                "Background owners did not stop within the deadline"
                if remaining else ""
            ),
        }
