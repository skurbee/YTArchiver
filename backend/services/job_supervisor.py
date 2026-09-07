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
    """Lifecycle queries must be cheap; joins budget their complete call.

    The supervisor additionally bounds foreign callbacks during quiescence.
    A callback that misses its deadline remains owned and prevents a safe
    restore result until it actually returns.
    """
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


@dataclass(slots=True)
class _LifecycleCall:
    owner: str
    thread: threading.Thread
    done: threading.Event = field(default_factory=threading.Event)
    outcome: tuple[bool, Any] | None = None


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
        self._lifecycle_calls: dict[tuple[str, str], _LifecycleCall] = {}

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
        on_cancelled_before_start: Callable[[], Any] | None = None,
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
                elif on_cancelled_before_start is not None:
                    on_cancelled_before_start()
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

    def _bounded_callbacks(
        self,
        callbacks: list[tuple[str, Callable[[], Any]]],
        deadline: float,
        *, phase: str = "callback",
    ) -> list[dict[str, Any]]:
        """Bound the whole callback, reusing an unfinished owner/phase call."""
        calls: list[tuple[str, _LifecycleCall | None]] = []
        for name, callback in callbacks:
            key = (phase, name)
            with self._lock:
                call = self._lifecycle_calls.get(key)
                if call is None or call.done.is_set():
                    if self._remaining(deadline) <= 0:
                        calls.append((name, None))
                        continue

                    def run(fn=callback, callback_key=key):
                        try:
                            outcome = (True, fn())
                        except BaseException as exc:
                            outcome = (False, exc)
                        with self._lock:
                            owned = self._lifecycle_calls[callback_key]
                            owned.outcome = outcome
                            owned.done.set()

                    thread = threading.Thread(
                        target=run, name=f"yta-lifecycle-{phase}-{name}", daemon=True)
                    call = _LifecycleCall(name, thread)
                    self._lifecycle_calls[key] = call
                    try:
                        thread.start()
                    except BaseException as exc:
                        call.outcome = (False, exc)
                        call.done.set()
                calls.append((name, call))
        for _name, call in calls:
            if call is not None:
                call.done.wait(self._remaining(deadline))
        rows: list[dict[str, Any]] = []
        for name, call in calls:
            outcome = call.outcome if call is not None and call.done.is_set() else None
            if outcome is None:
                rows.append({"owner": name, "ok": False, "error": "deadline exceeded"})
            elif outcome[0]:
                rows.append({"owner": name, "ok": True, "result": outcome[1]})
            else:
                rows.append({"owner": name, "ok": False, "error": str(outcome[1])})
        return rows

    def _unfinished_lifecycle_owners(self) -> list[str]:
        with self._lock:
            return sorted({f"lifecycle:{phase}:{name}"
                           for (phase, name), call in self._lifecycle_calls.items()
                           if not call.done.is_set()})

    def _snapshot_until(self, deadline: float) -> dict[str, Any]:
        row = self._bounded_callbacks(
            [("diagnostics", self.snapshot)], deadline, phase="snapshot")[0]
        if row["ok"]:
            return row["result"]
        return {"accepting": self.accepting_work(), "owners": [], "processes": [],
                "error": "Lifecycle inspection did not finish within its deadline"}

    def prepare_all(self, deadline: float | None = None) -> list[dict[str, Any]]:
        for task in self._managed_tasks():
            task.cancel.set()
        adapters = self._adapters()
        if deadline is None:
            deadline = time.monotonic() + 30.0
        raw = self._bounded_callbacks(
            [(adapter.owner, adapter.prepare) for adapter in adapters], deadline,
            phase="prepare",
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
        def join_owner(adapter):
            if self._safe_active(adapter):
                adapter.join(self._remaining(deadline))
            return self._safe_active(adapter)

        results = self._bounded_callbacks(
            [(adapter.owner, lambda a=adapter: join_owner(a))
             for adapter in self._adapters()], deadline, phase="join")
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
        remaining = [row["owner"] for row in results
                     if not row["ok"] or row.get("result")]
        remaining.extend(
            f"{task.owner}:{task.task_id}"
            for task in self._managed_tasks()
        )
        return remaining

    def force_remaining(self, deadline: float | None = None) -> list[dict[str, Any]]:
        """Force only adapters still active; never scan by process name."""
        def force_owner(adapter):
            return (True, adapter.force()) if self._safe_active(adapter) else (False, None)

        callbacks = [(adapter.owner, lambda a=adapter: force_owner(a))
                     for adapter in self._adapters()]
        callbacks.extend(
            (f"{task.owner}:{task.task_id}", lambda t=task: (True, t.force()))
            for task in self._managed_tasks()
            if task.force is not None
        )
        if deadline is None:
            deadline = time.monotonic() + 30.0
        return [
            {
                "owner": row["owner"],
                "forced": row["ok"],
                **({"result": row["result"][1]} if row["ok"] else
                   {"error": row["error"]}),
            }
            for row in self._bounded_callbacks(callbacks, deadline, phase="force")
            if not row["ok"] or row["result"][0]
        ]

    def quiesce(self, *, reason: str, timeout: float = 8.0) -> dict[str, Any]:
        """Close admission, checkpoint/cancel owners, and bound all waits."""
        started = time.monotonic()
        budget = max(0.01, float(timeout))
        deadline = started + budget
        prepare_deadline = min(deadline, started + budget * 0.20)
        join_deadline = min(deadline, started + budget * 0.75)
        self.close_admission(reason)
        before = self._snapshot_until(started + budget * 0.10)
        prepared = self.prepare_all(prepare_deadline)
        remaining = self._join_until_deadline(join_deadline)
        forced = self.force_remaining(deadline) if remaining else []
        if remaining:
            remaining = self._join_until_deadline(deadline)
        after = self._snapshot_until(deadline)
        remaining = sorted(set(remaining + self._unfinished_lifecycle_owners()))
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
