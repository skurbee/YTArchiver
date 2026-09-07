"""Coordinate durable Sync rows with redownload's runtime execution companions."""

from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from ..log import get_logger

_log = get_logger(__name__)


def _task_id(item: dict) -> str:
    return str(item.get("task_id") or "").strip()


@dataclass
class RedownloadQueueCommands:
    queues: Any
    runtime_lock: Any = None
    pending: list[dict] | None = None

    def _boundary(self):
        return self.runtime_lock if self.runtime_lock is not None else nullcontext()

    def remove(self, task_id: str) -> bool:
        with self._boundary():
            if not self.queues.sync_remove_task(task_id, durable=True):
                return False
            if self.pending is not None:
                self.pending[:] = [item for item in self.pending
                                   if _task_id(item.get("rd_task") or {}) != task_id]
            return True

    def reorder(self, task_id: str, new_index: int) -> bool:
        with self._boundary():
            order = self.queues.sync_snapshot()
            source = next((i for i, item in enumerate(order) if _task_id(item) == task_id), -1)
            if source < 0 or new_index < 0 or new_index >= len(order):
                return False
            order.insert(new_index, order.pop(source))
            if not self.queues.sync_reorder(task_id, new_index, durable=True):
                return False
            if self.pending is not None:
                by_id = {_task_id(item.get("rd_task") or {}): item for item in self.pending}
                arranged = [by_id[_task_id(item)] for item in order
                            if (item.get("kind") or "").lower() == "redownload"
                            and _task_id(item) in by_id]
                arranged.extend(item for item in self.pending if item not in arranged)
                self.pending[:] = arranged
            return True

    def acknowledge(self, task_id: str, *, cancelled: bool, stopped: bool,
                    companion: dict[str, Any]) -> bool:
        """Release the exact current slot, then reattach a durable defer once.

        The caller invokes this only after the backend operation returns. A
        normal Cancel has no pending row; global Stop cannot start a new pass.
        """
        with self._boundary():
            current = self.queues.current_sync
            if current is not None and not self.queues.replace_current_task_durable(
                    "sync", None, expected_task_id=task_id):
                _log.warning("Redownload completion remains recoverable: current slot save failed")
                return False
            if not (cancelled and not stopped and task_id and self.pending is not None):
                return True
            deferred = next((dict(item) for item in self.queues.sync_snapshot()
                             if _task_id(item) == task_id and item.get("kind") == "redownload"), None)
            if deferred is None or any(_task_id(item.get("rd_task") or {}) == task_id
                                       for item in self.pending):
                return True
            deferred.pop("cancel_requested", None)
            restored = dict(companion)
            restored.update(rd_task=deferred, scope=deferred.get("scope"),
                            new_res=deferred.get("redownload_res") or companion.get("new_res"))
            self.pending.append(restored)
            return True
