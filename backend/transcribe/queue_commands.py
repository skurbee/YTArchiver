"""Exact-ID processing commands and their cross-store compensation boundary.

The manager owns worker state. This service owns command ordering; explicit
ports make store failures and commit-before-signal behavior testable without
starting a worker or reaching through an API object's private fields.
"""
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..log import get_logger

_log = get_logger(__name__)


@dataclass
class ProcessingQueueCommands:
    journal_lock: Any
    jobs_lock: Any
    pending_jobs: Callable[[], list[dict]]
    current_job: Callable[[], dict | None]
    inline_jobs: Callable[[], list[dict]]
    queue_state: Callable[[], Any]
    persist_pending: Callable[[], bool]
    write_pending_snapshot: Callable[[list[dict]], bool]
    pending_snapshot: Callable[..., list[dict]]
    queue_payload: Callable[[dict], dict]
    send_cancel: Callable[[], None]
    stream: Any
    release_compress_slots: Callable[[list[dict]], None]
    notify_cancelled: Callable[[list[dict]], None]
    bump_pending: Callable[[str, int], None]

    def remove(self, task_ids: set[str]) -> bool:
        queues = self.queue_state()
        if queues is None:
            return False
        with self.journal_lock:
            entries = queues.gpu_items_for_ids(task_ids)
            return self.remove_pending_task_ids_coordinated(
                task_ids,
                lambda: set(queues.gpu_remove_tasks(list(task_ids), durable=True,
                                                   require_all=True)) == task_ids,
                lambda: queues.gpu_restore_items(entries, durable=True))

    def reorder(self, task_id: str, new_index: int) -> bool:
        queues = self.queue_state()
        if queues is None:
            return False
        with self.journal_lock:
            snapshot = queues.gpu_snapshot()
            old_index = next((i for i, item in enumerate(snapshot)
                              if item.get("task_id") == task_id), -1)
            if old_index < 0:
                return False
            return self.reorder_pending_task_coordinated(
                task_id, new_index,
                lambda: queues.gpu_reorder(task_id, new_index, durable=True),
                lambda: queues.gpu_reorder(task_id, old_index, durable=True))

    def cancel(self, task_id: str) -> bool:
        queues = self.queue_state()
        if queues is None:
            return False
        return self.cancel_current_durable(
            task_id, lambda: queues.replace_current_task_durable(
                "gpu", None, expected_task_id=task_id))

    def remove_pending_task_ids_coordinated(
            self, task_ids: set[str], mirror_remove: Callable[[], bool],
            mirror_restore: Callable[[], bool]) -> bool:
        """Remove exact pending jobs only if both durable stores commit.

        QueueState commits while the candidate jobs are absent in memory, then
        the transcription journal commits the same state.  If the journal
        replacement fails, QueueState is restored and the in-memory jobs were
        never allowed to escape the journal boundary.
        """
        wanted = {
            str(task_id or "").strip() for task_id in task_ids
            if str(task_id or "").strip()
        }
        if not wanted or not callable(mirror_remove):
            return False
        removed_jobs: list[dict[str, Any]] = []
        with self.journal_lock:
            with self.jobs_lock:
                original = list(self.pending_jobs())
                keep = []
                for job in self.pending_jobs():
                    if str(job.get("task_id") or "").strip() in wanted:
                        removed_jobs.append(job)
                    else:
                        keep.append(job)
                self.pending_jobs()[:] = keep
            try:
                mirror_saved = bool(mirror_remove())
            except Exception as exc:
                _log.warning("Processing queue removal mirror failed: %s", exc)
                mirror_saved = False
            if not mirror_saved:
                with self.jobs_lock:
                    self.pending_jobs()[:] = original
                return False
            if removed_jobs and not self.persist_pending():
                with self.jobs_lock:
                    self.pending_jobs()[:] = original
                try:
                    restored = bool(mirror_restore())
                except Exception as exc:
                    _log.warning(
                        "Processing queue removal rollback failed: %s", exc)
                    restored = False
                if not restored:
                    self.stream.emit_error(
                        "Could not restore the visible Processing queue after "
                        "a journal failure. Work remains in recovery and queue "
                        "actions are disabled until saving succeeds.")
                self.stream.emit_error(
                    "Could not save task removal; Processing work was kept "
                    "for recovery.")
                return False
        for job in removed_jobs:
            try:
                if (not job.get("retranscribe")
                        and not job.get("_pending_decremented")
                        and not job.get("_skip_pending_counter")):
                    self.bump_pending(
                        job.get("channel") or "", -1)
                    job["_pending_decremented"] = True
            except Exception as exc:
                _log.debug("pending counter cleanup failed: %s", exc)
        self.release_compress_slots(removed_jobs)
        self.notify_cancelled(removed_jobs)
        return True


    def reorder_pending_task_coordinated(
            self, task_id: str, new_index: int,
            mirror_reorder: Callable[[], bool],
            mirror_restore: Callable[[], bool]) -> bool:
        """Reorder one exact job only if QueueState and journal agree."""
        ident = str(task_id or "").strip()
        if not ident or not callable(mirror_reorder):
            return False
        try:
            target_index = int(new_index)
        except (TypeError, ValueError):
            return False
        with self.journal_lock:
            with self.jobs_lock:
                original = list(self.pending_jobs())
                idx = next(
                    (i for i, job in enumerate(self.pending_jobs())
                     if str(job.get("task_id") or "").strip() == ident),
                    -1,
                )
                if idx >= 0:
                    if target_index < 0 or target_index >= len(self.pending_jobs()):
                        return False
                    job = self.pending_jobs().pop(idx)
                    self.pending_jobs().insert(target_index, job)
            try:
                mirror_saved = bool(mirror_reorder())
            except Exception as exc:
                _log.warning("Processing queue reorder mirror failed: %s", exc)
                mirror_saved = False
            if not mirror_saved:
                with self.jobs_lock:
                    self.pending_jobs()[:] = original
                return False
            if idx >= 0 and not self.persist_pending():
                with self.jobs_lock:
                    self.pending_jobs()[:] = original
                try:
                    restored = bool(mirror_restore())
                except Exception as exc:
                    _log.warning(
                        "Processing queue reorder rollback failed: %s", exc)
                    restored = False
                if not restored:
                    self.stream.emit_error(
                        "Could not restore the visible Processing order after "
                        "a journal failure. Queue actions are disabled until "
                        "saving succeeds.")
                self.stream.emit_error(
                    "Could not save Processing order; the previous order was "
                    "kept.")
                return False
        return True


    def cancel_current_durable(
            self, task_id: str,
            clear_visible: Callable[[], bool]) -> bool:
        """Cancel one exact running job after both recovery stores commit."""
        wanted = str(task_id or "").strip()
        if not wanted or not callable(clear_visible):
            return False
        with self.journal_lock:
            with self.jobs_lock:
                job = self.current_job()
                if (not job
                        or str(job.get("task_id") or "").strip() != wanted
                        or "cancel" not in job):
                    return False

            # Commit the journal removal first.  The job is still running and
            # its QueueState current slot remains recoverable until the peer
            # store commits below.
            if not self.write_pending_snapshot(
                    self.pending_snapshot(include_current=False)):
                return False
            try:
                visible_saved = bool(clear_visible())
            except Exception as exc:
                _log.warning(
                    "Processing current-slot cancellation failed: %s", exc)
                visible_saved = False
            if not visible_saved:
                # Restore the first durable store before returning failure.
                # If this compensation itself cannot be saved, QueueState's
                # still-present current slot remains sufficient for recovery.
                self.write_pending_snapshot(
                    self.pending_snapshot(include_current=True))
                return False

            # The worker's outcome must remain a deliberate terminal drop even
            # if cancellation races with an unrelated failure.
            job["_cancel_drop_requested"] = True
            job["cancel"].set()
            try:
                self.send_cancel()
            except Exception as exc:
                _log.debug("cooperative Processing cancel failed: %s", exc)
            return True


    def defer_current(self, task_id: str) -> bool:
        """Cancel the exact running task and place that same ID at the tail."""
        wanted = str(task_id or "").strip()
        if not wanted:
            return False
        reservation = None
        with self.journal_lock:
            with self.jobs_lock:
                job = self.current_job()
                if (not job
                        or str(job.get("task_id") or "").strip() != wanted
                        or "cancel" not in job):
                    return False
                reserved_ids = {
                    str(existing.get("task_id") or "").strip()
                    for existing in [*self.pending_jobs(), *self.inline_jobs()]
                    if str(existing.get("task_id") or "").strip()
                }
            if self.queue_state() is None:
                return False
            try:
                reservation = self.queue_state().gpu_reserve_task(
                    self.queue_payload(job),
                    reserved_task_ids=reserved_ids,
                    required_task_id=wanted,
                )
            except Exception as exc:
                _log.warning("GPU defer queue reservation failed: %s", exc)
                reservation = None
            if not isinstance(reservation, dict):
                return False
            job["_defer_requested"] = True
            if not self.persist_pending():
                job.pop("_defer_requested", None)
                try:
                    self.queue_state().gpu_rollback_reservation(reservation)
                except Exception as exc:
                    _log.warning("GPU defer reservation rollback failed: %s", exc)
                self.stream.emit_error(
                    "Could not save the deferred Processing task; the running "
                    "task was not cancelled.")
                return False
            job["cancel"].set()
            try:
                self.send_cancel()
            except Exception as e:
                _log.debug("swallowed: %s", e)
        return True
