"""
Queue state — persistent multi-queue manager.

Matches YTArchiver's ytarchiver_queue.json schema (YTArchiver.py:34016):
    {
      "sync": [channel_dict, ...],
      "reorg": [[args], ...],
      "video": [],
      "transcribe": [[args], ...],
      "redownload": [dict, ...],
      "metadata": [dict, ...],
      "gpu": [dict, ...],
      "gpu_paused": false,
      "sync_paused": false
    }

Single source of truth. Persists debounced 2s after changes.
Gated by config_is_writable() (same gate as config writes).
"""

from __future__ import annotations

import copy
import json
import os
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from .log import get_logger, swallow
from .services.queue_repository import QueueRepository
from .ytarchiver_config import QUEUE_FILE, config_is_writable

_log = get_logger(__name__)

_QUEUE_SCHEMA_VERSION = 3
_RESUMING_SCHEMA_VERSION = 2


def make_task_id(lane: str) -> str:
    """Return a new opaque queue-task identity.

    The lane prefix is only for diagnostics; callers must treat the complete
    value as opaque and must never derive identity from a URL, path, or row
    position.
    """
    prefix = "gpu" if str(lane or "").lower() == "gpu" else "sync"
    return f"{prefix}-{uuid.uuid4().hex}"


class QueueState:
    """Central queue manager. Thread-safe."""

    def __init__(self, repository: QueueRepository | None = None):
        self._lock = threading.RLock()
        self._repository_explicit = repository is not None
        self._repository = repository or QueueRepository(QUEUE_FILE)
        self.sync: list[dict[str, Any]] = []
        # Redownload, metadata, and other network tasks share this queue and
        # use their `kind` field to select behavior.
        self.gpu: list[dict[str, Any]] = []
        self.gpu_paused: bool = False
        self.sync_paused: bool = False
        # Pause is requested via set_*_paused(True), but the worker may
        # still be mid-operation (e.g. yt-dlp download in progress, or
        # the long re-fetch loop in metadata refresh). The "_active"
        # flags below flip True ONLY when the worker has actually
        # entered its pause-wait block. Frontend uses (paused AND NOT
        # active) to render the Resume button as "blinking" (pause
        # queued but not yet effective) so the user knows their click
        # was registered. Runtime-only — never persisted.
        self.gpu_paused_active: bool = False
        self.sync_paused_active: bool = False
        # True only when gpu_paused was RESTORED from disk on load (i.e. the
        # user paused in a PRIOR session and quit). Lets the enqueue / sync-
        # start paths auto-release a stale prior-session pause when the user
        # initiates fresh work, WITHOUT ever clearing a pause the user set in
        # the current session (that was silently un-pausing a deliberately
        # paused Processing queue — e.g. an auto-sync download resumed it).
        # Any explicit set_gpu_paused() call clears this (it's now a current-
        # session decision). Runtime-only — never persisted.
        self.gpu_pause_restored: bool = False

        # Current in-flight items (not yet re-queued, but shown in popover)
        self.current_sync: dict[str, Any] | None = None
        self.current_gpu: dict[str, Any] | None = None

        # Sync-pass progress: when "Sync Subbed" runs, we don't enqueue 103
        # individual channel items into `self.sync` — we iterate them
        # inline in `sync_start_all`. But the popover shouldn't look like
        # a single-item queue; the user should see "Downloading ChannelName
        # (17/103)" so they know how far along the pass is. These two
        # fields are set / cleared by sync_start_all.
        self.sync_pass_index: int = 0
        self.sync_pass_total: int = 0

        # Debounced save scheduler. A single daemon thread waits until
        # _save_deadline; save_debounced() only pushes the deadline out
        # and signals it, avoiding one Timer thread per queue mutation.
        self._save_cond = threading.Condition(self._lock)
        self._save_thread: threading.Thread | None = None
        self._save_deadline: float | None = None
        # Shortened from 2.0s — a task-killed (Task Manager "End Task")
        # process during the debounce window loses the last queue
        # mutation since SIGTERM doesn't fire on Windows force-kill
        # and atexit is skipped. 0.5s still coalesces normal bursts
        # of enqueue/remove calls (every save_debounced inside a sync
        # iteration lands within ms of each other) but cuts the
        # window-of-loss to a quarter of what it was (audit:
        # main.py:1362).
        self._save_interval_sec = 0.5
        # Save mutex serializes save_now() so immediate current-item saves
        # and the debounced saver cannot both write to the same .tmp file
        # and race on os.replace.
        self._save_io_lock = threading.Lock()
        self._save_failure_warned: bool = False
        # Hot current-item transitions use a tiny authoritative sidecar so
        # large queues are not fully serialized on every channel/job change.
        self._resuming_io_lock = threading.Lock()
        self._resuming_write_seq: int = 0
        self._resuming_last_written_seq: int = 0
        self._resuming_failure_warned: bool = False

        # resuming items pulled from the persisted file
        # (in-flight when the app last shut down). Caller reads via
        # `get_loaded_resuming()` after `load()` to decide how to
        # requeue them. Empty until load() runs.
        self._loaded_resuming: dict[str, Any] = {}
        # False only while a legacy queue has received in-memory IDs but the
        # schema migration has not yet committed to every authoritative file.
        # The UI disables item mutations in that narrow state so it never
        # presents those IDs as durable.
        self._identity_ids_durable: bool = True

        # Listeners notified on any state change (UI push)
        self._listeners: list[Callable[[], None]] = []
        self._notify_cond = threading.Condition(self._lock)
        self._notify_dirty = False
        self._notify_thread: threading.Thread | None = None
        self._notify_stopped = False

        # When True, _atexit_flush is a no-op. Set via mark_orphan()
        # by the caller (main.py) when it discards a QueueState
        # instance that failed to load — without this flag, the
        # orphan's atexit handler still fires at process exit and
        # clobbers the on-disk queue file with its EMPTY in-memory
        # state (overwriting whatever the replacement instance just
        # wrote).
        self._atexit_disabled: bool = False

        # register atexit hook so a crash/kill within the
        # 2s debounce window still flushes. Idempotent — atexit only
        # fires once per process, and _atexit_flush is a no-op when
        # nothing is pending OR when the instance has been marked
        # as an orphan.
        try:
            import atexit as _atx
            _atx.register(self._atexit_flush)
        except Exception as e:
            swallow("atexit flush registration", e)

    def mark_orphan(self) -> None:
        """Caller-side signal that this QueueState should stop background work.

        Use when discarding an instance whose load() raised and replacing it
        with a fresh QueueState.
        """
        with self._save_cond:
            self._atexit_disabled = True
            self._save_deadline = None
            self._save_cond.notify_all()
        with self._notify_cond:
            self._notify_stopped = True
            self._notify_cond.notify_all()

    def _queue_repository(self) -> QueueRepository:
        """Return the owner for the active queue path.

        Tests and recovery tooling have historically redirected ``QUEUE_FILE``
        after constructing QueueState. Preserve that supported seam unless an
        explicit repository was injected.
        """
        if (
            not self._repository_explicit
            and self._repository.main_path != QUEUE_FILE
        ):
            self._repository = QueueRepository(QUEUE_FILE)
        return self._repository

    # ── listener registration ───────────────────────────────────────

    def get_loaded_resuming(self) -> dict[str, Any]:
        """Return items that were in flight when the app last shut down.

        Startup reads this after `load()` to
        decide how to requeue them (typically: append to the tail of
        their respective queues with a "restored" tag). Returns a
        copy; safe to consume."""
        with self._lock:
            return copy.deepcopy(self._loaded_resuming or {})

    def clear_resuming_slots(self, *kinds: str,
                             clear_current: bool = False) -> bool:
        """Forget persisted crash-resume entries for the requested lanes.

        The resuming sidecar is authoritative only while an item is truly
        in-flight. Once startup has converted it back into normal queued work,
        or the user has explicitly cleared/cancelled the queue, keeping the
        sidecar around resurrects stale work on every launch.
        """
        wanted = {str(k or "").strip().lower() for k in kinds}
        wanted.discard("")
        if not wanted:
            wanted = {"sync", "gpu"}

        changed = False
        previous_loaded: dict[str, Any] = {}
        previous_current: dict[str, Any] = {}
        with self._lock:
            for kind in wanted:
                if kind in self._loaded_resuming:
                    previous_loaded[kind] = copy.deepcopy(
                        self._loaded_resuming[kind])
                    self._loaded_resuming.pop(kind, None)
                    changed = True
                if clear_current and kind == "sync" and self.current_sync is not None:
                    previous_current["sync"] = copy.deepcopy(self.current_sync)
                    self.current_sync = None
                    changed = True
                if clear_current and kind == "gpu" and self.current_gpu is not None:
                    previous_current["gpu"] = copy.deepcopy(self.current_gpu)
                    self.current_gpu = None
                    changed = True
            payload = (self._build_resuming_payload_locked()
                       if changed and config_is_writable() else None)

        if not changed:
            return True

        ok = payload is not None
        if ok:
            try:
                ok = self._write_resuming_payload(payload)
            except Exception:
                ok = False
        if ok:
            # The main queue can still contain the old resuming entry (or a
            # just-popped pending copy) until its debounce fires.  Clearing
            # only the sidecar would therefore report success yet resurrect
            # the task after a crash.  Commit both authoritative files.
            ok = self.save_now()
        if not ok:
            # The old sidecar is still authoritative. Restore the exact
            # in-memory slots so the UI/runtime do not claim they were
            # forgotten when fsync/replace failed.
            with self._lock:
                for kind, item in previous_loaded.items():
                    self._loaded_resuming[kind] = item
                if "sync" in previous_current:
                    self.current_sync = previous_current["sync"]
                if "gpu" in previous_current:
                    self.current_gpu = previous_current["gpu"]
                rollback_payload = (self._build_resuming_payload_locked()
                                    if config_is_writable() else None)
            if rollback_payload is not None:
                try:
                    self._write_resuming_payload(rollback_payload)
                except Exception:
                    pass
            self._mark_identity_persistence_failed()
            self._notify()
            return False

        self._notify()
        return ok

    def replace_current_task_durable(
            self, lane: str, replacement: dict[str, Any] | None, *,
            expected_task_id: str | None = None) -> bool:
        """CAS-replace one running slot and synchronously commit its sidecar.

        ``expected_task_id`` distinguishes an exact running-row action from a
        stale click. Passing ``""`` requires the slot to be empty and is used
        to compensate a later journal failure.
        """
        normalized_lane = str(lane or "").strip().lower()
        if normalized_lane not in {"sync", "gpu"}:
            return False
        attr = "current_sync" if normalized_lane == "sync" else "current_gpu"
        changed = False
        with self._lock:
            previous = copy.deepcopy(getattr(self, attr))
            current_id = str((previous or {}).get("task_id") or "").strip()
            if expected_task_id is not None:
                wanted = str(expected_task_id or "").strip()
                if current_id != wanted:
                    return False
            if replacement is None:
                normalized = None
            elif isinstance(replacement, dict):
                normalized, _changed = self._normalize_task(
                    replacement, normalized_lane)
            else:
                return False
            if previous == normalized:
                return True
            setattr(self, attr, normalized)
            changed = True
            payload = (self._build_resuming_payload_locked()
                       if config_is_writable() else None)

        ok = payload is not None
        if ok:
            try:
                ok = self._write_resuming_payload(payload)
            except Exception:
                ok = False
        if ok:
            ok = self.save_now()
        if not ok:
            with self._lock:
                # The worker cannot legitimately advance this slot while the
                # exact action is waiting on its synchronous commit.
                setattr(self, attr, previous)
                rollback_payload = (self._build_resuming_payload_locked()
                                    if config_is_writable() else None)
            if rollback_payload is not None:
                try:
                    self._write_resuming_payload(rollback_payload)
                except Exception:
                    pass
            self._mark_identity_persistence_failed()
            if changed:
                self._notify()
            return False
        if changed:
            self._notify()
        return True

    def add_listener(self, fn: Callable[[], None]):
        # Keep listener mutation under the same lock as notification snapshots.
        with self._lock:
            self._listeners.append(fn)

    def _notify(self):
        """Schedule one latest-state listener dispatch for queue UI updates."""
        try:
            with self._notify_cond:
                if self._notify_stopped or not self._listeners:
                    return
                self._notify_dirty = True
                if (self._notify_thread is None
                        or not self._notify_thread.is_alive()):
                    self._notify_thread = threading.Thread(
                        target=self._notify_loop,
                        daemon=True,
                        name="queues-notify",
                    )
                    self._notify_thread.start()
                self._notify_cond.notify_all()
        except Exception as e:
            swallow("queue notify-thread start", e)

    def _notify_loop(self):
        while True:
            with self._notify_cond:
                while not self._notify_dirty and not self._notify_stopped:
                    self._notify_cond.wait()
                if self._notify_stopped:
                    self._notify_thread = None
                    return
                self._notify_dirty = False
                snapshot = list(self._listeners)

            for fn in snapshot:
                try:
                    fn()
                except Exception as e:
                    swallow("queue change-listener callback", e)

    # ── load/save ────────────────────────────────────────────────────

    @staticmethod
    def _sync_identity_key(ch: dict[str, Any]) -> tuple[str, str, str] | None:
        """Return the uniqueness key used by sync_enqueue: kind + target."""
        if not isinstance(ch, dict):
            return None
        kind = str(ch.get("kind") or "download").strip().lower()
        url = str(ch.get("url") or "").strip()
        if url:
            return (kind, "url", url)
        name = str(ch.get("name") or ch.get("folder") or "").strip()
        if name:
            return (kind, "name", name)
        return None

    @staticmethod
    def _gpu_identity_key(item: dict[str, Any]) -> tuple[str, str] | None:
        """Return the logical dedupe key for one Processing task.

        A transcription and a compression may intentionally target the same
        file, so path alone is not an identity. Permanent ``task_id`` remains
        the only key used for user mutations.
        """
        if not isinstance(item, dict):
            return None
        kind = str(item.get("kind") or "transcribe").strip().lower()
        path = str(item.get("path") or "").strip()
        return (kind, path) if path else None

    @staticmethod
    def _normalize_kind(item: dict[str, Any], lane: str) -> tuple[str, bool]:
        raw = str(item.get("kind") or "").strip().lower()
        if lane == "sync":
            # A few schema-1 recovery files predate the explicit kind field.
            # Redownload-only metadata is unambiguous, so recover its worker
            # routing instead of silently treating it as a normal download.
            inferred = (
                "redownload"
                if not raw and any(item.get(key) for key in (
                    "redownload_res", "only_video_id", "only_filepath"))
                else "download"
            )
        else:
            inferred = "transcribe"
        kind = raw or inferred
        changed = item.get("kind") != kind
        return kind, changed

    @classmethod
    def _normalize_task(cls, item: dict[str, Any], lane: str,
                        seen_ids: set[str] | None = None
                        ) -> tuple[dict[str, Any], bool]:
        """Copy and migrate one task to the permanent-ID schema."""
        normalized = copy.deepcopy(item)
        kind, changed = cls._normalize_kind(normalized, lane)
        normalized["kind"] = kind
        task_id = str(normalized.get("task_id") or "").strip()
        if not task_id or (seen_ids is not None and task_id in seen_ids):
            task_id = make_task_id(lane)
            normalized["task_id"] = task_id
            changed = True
        elif normalized.get("task_id") != task_id:
            normalized["task_id"] = task_id
            changed = True
        if seen_ids is not None:
            seen_ids.add(task_id)
        return normalized, changed

    @classmethod
    def _normalize_task_list(
            cls, items: list[Any], lane: str, seen_ids: set[str]
    ) -> tuple[list[dict[str, Any]], bool]:
        normalized: list[dict[str, Any]] = []
        changed = False
        for item in items:
            if not isinstance(item, dict):
                changed = True
                continue
            task, task_changed = cls._normalize_task(item, lane, seen_ids)
            normalized.append(task)
            changed = changed or task_changed
        return normalized, changed

    @classmethod
    def _dedupe_sync_items(cls, items: list[Any]) -> tuple[list[dict[str, Any]], bool]:
        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        changed = False
        for item in items:
            if not isinstance(item, dict):
                changed = True
                continue
            key = cls._sync_identity_key(item)
            if key is not None:
                if key in seen:
                    changed = True
                    continue
                seen.add(key)
            deduped.append(item)
        return deduped, changed

    @classmethod
    def _dedupe_gpu_items(cls, items: list[Any]) -> tuple[list[dict[str, Any]], bool]:
        """Collapse legacy duplicate logical Processing rows.

        User-visible identity is always ``task_id``, but the worker deliberately
        permits only one pending job for a given ``(kind, path)``.  Keeping two
        legacy rows for that same job would leave one without a corresponding
        recovery-journal entry after reconciliation.
        """
        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        changed = False
        for item in items:
            if not isinstance(item, dict):
                changed = True
                continue
            key = cls._gpu_identity_key(item)
            if key is not None:
                if key in seen:
                    changed = True
                    continue
                seen.add(key)
            deduped.append(item)
        return deduped, changed

    def load(self) -> bool:
        """Load queue state from ytarchiver_queue.json. Returns True on success.

        _load_queue_state: if the JSON is
        corrupt, rename the file to .bak so next launch starts fresh instead
        of soft-locking on the same parse error every time.
        """
        # The hot current-item sidecar is independently durable.  Read it
        # before the main queue so a first-run crash (sidecar committed before
        # the first debounced main save), a missing main file, or a corrupt main
        # file cannot discard the exact in-flight task.
        sidecar_exists, sidecar_resuming = self._load_resuming_sidecar()
        data: dict[str, Any] = {}
        main_needs_rebuild = False
        loaded = self._queue_repository().load_main()
        if loaded.state == "missing":
            if not sidecar_exists:
                return False
            main_needs_rebuild = True
        elif loaded.state == "blocked":
            _log.warning("Queue file could not be loaded safely: %s", loaded.error)
            return False
        elif loaded.state == "sidelined":
            if not sidecar_exists:
                _log.warning(
                    "Queue file was sidelined after load failure: %s",
                    loaded.error,
                )
                return False
            main_needs_rebuild = True
        else:
            data = loaded.data
        schema_normalized = False
        raw_schema_v = data.get("_schema_version", 1)
        try:
            schema_v = int(raw_schema_v or 1)
        except (TypeError, ValueError, OverflowError):
            # Preserve otherwise-readable legacy work. A malformed version
            # marker is migration metadata, not a reason to discard every
            # pending task in the file.
            schema_v = 1
            schema_normalized = True
        raw_sync = data.get("sync", [])
        if not isinstance(raw_sync, list):
            raw_sync = []
        raw_gpu = data.get("gpu", [])
        if not isinstance(raw_gpu, list):
            raw_gpu = []
        seen_ids: set[str] = set()
        sync_items, sync_normalized = self._normalize_task_list(
            raw_sync, "sync", seen_ids)
        sync_items, sync_deduped = self._dedupe_sync_items(sync_items)
        sync_normalized = sync_normalized or sync_deduped
        gpu_items, gpu_normalized = self._normalize_task_list(
            raw_gpu, "gpu", seen_ids)
        gpu_items, gpu_deduped = self._dedupe_gpu_items(gpu_items)
        gpu_normalized = gpu_normalized or gpu_deduped
        with self._lock:
            self.sync = sync_items
            # Unknown keys from older queue schemas are intentionally ignored.
            self.gpu = gpu_items
            self.gpu_paused = bool(data.get("gpu_paused", False))
            self.sync_paused = bool(data.get("sync_paused", False))
            # Mark a restored pause so the enqueue / sync-start paths may
            # auto-release THIS (prior-session) pause but never a fresh one.
            self.gpu_pause_restored = self.gpu_paused

            # resuming-dict handling. New-format files
            # (schema_version 2+) keep in-flight items in a separate
            # `resuming` dict; old-format files put them at the front
            # of the regular queue lists. We surface `resuming` so the
            # caller (main.py startup) can emit a restore notice and
            # decide how to requeue.
            resuming_raw = data.get("resuming") or {}
            if schema_v >= 2 and isinstance(resuming_raw, dict):
                # New format: resuming items are NOT in the regular
                # lists; pull them out and stash for the caller.
                self._loaded_resuming = dict(resuming_raw)
            else:
                # Old format: any item at queue[0] that carries the
                # in-flight marker (_in_flight=True; legacy save
                # pattern wrote it). Requiring the marker prevents
                # mis-classifying every regular schema-1 queue's head
                # item as resuming — a plain dict item without the
                # marker is just a queued task, not in-flight. Pop
                # it off the regular list so it doesn't get processed
                # twice (once as a resuming candidate AND again as a
                # normal head item).
                self._loaded_resuming = {}
                for key in ("sync", "gpu"):
                    lst = getattr(self, key, None)
                    if (lst and isinstance(lst, list)
                            and isinstance(lst[0], dict)
                            and lst[0].get("_in_flight")):
                        restored = lst.pop(0)
                        seen_ids.discard(str(
                            restored.get("task_id") or "").strip())
                        self._loaded_resuming[key] = restored
            if sidecar_exists:
                self._loaded_resuming = sidecar_resuming

            # Queue/resuming files from before schema 3 did not have stable
            # task identities. Normalize the authoritative recovery slots
            # after the pending lists so IDs remain unique within the loaded
            # state. The task kind is normalized at the same boundary so a
            # restored redownload cannot fall through to the download worker.
            normalized_resuming: dict[str, Any] = {}
            resuming_normalized = False
            for lane in ("sync", "gpu"):
                item = self._loaded_resuming.get(lane)
                if not isinstance(item, dict):
                    if item is not None:
                        resuming_normalized = True
                    continue
                normalized, item_changed = self._normalize_task(
                    item, lane, seen_ids)
                pending_items = self.sync if lane == "sync" else self.gpu
                identity = (self._sync_identity_key(normalized)
                            if lane == "sync"
                            else self._gpu_identity_key(normalized))
                if identity is not None and any(
                        (self._sync_identity_key(pending) if lane == "sync"
                         else self._gpu_identity_key(pending)) == identity
                        for pending in pending_items):
                    # Defer commits the same exact job to the pending tail
                    # before signalling cancellation. A crash in that narrow
                    # handoff can leave the old current sidecar too; pending is
                    # the authoritative deferred copy and must run only once.
                    resuming_normalized = True
                    continue
                normalized_resuming[lane] = normalized
                resuming_normalized = resuming_normalized or item_changed
            self._loaded_resuming = normalized_resuming
        migration_needed = (main_needs_rebuild or schema_normalized
                            or schema_v < _QUEUE_SCHEMA_VERSION
                            or sync_normalized or gpu_normalized
                            or resuming_normalized)
        self._identity_ids_durable = not migration_needed
        if migration_needed and config_is_writable():
            # Commit IDs before the first UI render. The resuming sidecar is
            # independently authoritative, so it must commit too when present.
            self._identity_ids_durable = bool(self.save_now())
            if not self._identity_ids_durable:
                self.save_debounced()
        elif migration_needed:
            self.save_debounced()
        self._notify()
        return True

    def _resuming_file(self):
        return self._queue_repository().resuming_path

    def _load_resuming_sidecar(self) -> tuple[bool, dict[str, Any]]:
        loaded = self._queue_repository().load_resuming()
        if loaded.state == "blocked":
            _log.warning(
                "Queue recovery sidecar could not be loaded safely: %s",
                loaded.error,
            )
        return loaded.state == "ok", loaded.data

    def _build_save_payload_locked(self) -> dict[str, Any]:
        """Build the QUEUE_FILE payload from current state. CALLER MUST HOLD
        self._lock — building under a FRESH lock let a concurrent set_current_*
        transition persist the WRONG in-flight `resuming` item (audit r2).

        In-flight items go in a separate `resuming` dict (not at the front of
        the queue lists) so load() requeues them in a controlled way instead of
        re-popping + silently re-processing them.
        """
        payload: dict[str, Any] = {
            "_schema_version": _QUEUE_SCHEMA_VERSION,
            "sync": copy.deepcopy(self.sync),
            "gpu": copy.deepcopy(self.gpu),
            "gpu_paused": self.gpu_paused,
            "sync_paused": self.sync_paused,
        }
        # Preserve loaded-but-not-yet-consumed recovery slots during schema
        # migration. Startup removes them explicitly after requeueing; a
        # background migration save must not erase them first.
        resuming: dict[str, Any] = copy.deepcopy(
            self._loaded_resuming or {})
        if self.current_sync is not None:
            resuming["sync"] = copy.deepcopy(self.current_sync)
        if self.current_gpu is not None:
            resuming["gpu"] = copy.deepcopy(self.current_gpu)
        if resuming:
            payload["resuming"] = resuming
        return payload

    def _write_save_payload(self, payload: dict[str, Any]) -> bool:
        """Atomically replace QUEUE_FILE. Serialized via _save_io_lock so two
        writers can't interleave on the same .tmp."""
        with self._save_io_lock:
            result = self._queue_repository().commit_main(payload)
            if result.ok:
                self._save_failure_warned = False
                return True
            if not self._save_failure_warned:
                _log.warning(
                    "Queue state could not be saved; pending work may not "
                    "resume after a crash until saving succeeds again: %s",
                    result.error,
                )
                self._save_failure_warned = True
            return False

    def _build_resuming_payload_locked(self) -> dict[str, Any]:
        self._resuming_write_seq += 1
        resuming: dict[str, Any] = copy.deepcopy(
            self._loaded_resuming or {})
        if self.current_sync is not None:
            resuming["sync"] = copy.deepcopy(self.current_sync)
        if self.current_gpu is not None:
            resuming["gpu"] = copy.deepcopy(self.current_gpu)
        return {
            "_schema_version": _RESUMING_SCHEMA_VERSION,
            "_seq": self._resuming_write_seq,
            "resuming": resuming,
        }

    def backup_resource_bytes(self) -> dict[str, bytes]:
        """Return a coherent queue/main-sidecar generation for one backup.

        The live main queue is intentionally debounced while current-task
        transitions use a small immediate sidecar. Reading those two files one
        after another can therefore combine different logical generations.
        A full backup instead asks the live owner for both payloads while its
        state lock is held, then serializes the detached copies after releasing
        the lock. The matching marker is diagnostic metadata; normal queue
        loading ignores unknown fields.
        """
        with self._lock:
            main_payload = self._build_save_payload_locked()
            generation = f"backup-{uuid.uuid4().hex}"
            main_payload["_backup_generation"] = generation
            sidecar_payload = {
                "_schema_version": _RESUMING_SCHEMA_VERSION,
                "_seq": max(
                    self._resuming_write_seq,
                    self._resuming_last_written_seq,
                ),
                "_backup_generation": generation,
                "resuming": copy.deepcopy(
                    main_payload.get("resuming") or {}),
            }

        def _encode(payload: dict[str, Any]) -> bytes:
            return json.dumps(payload, indent=2).encode("utf-8")

        return {
            self._queue_repository().main_path.name: _encode(main_payload),
            self._resuming_file().name: _encode(sidecar_payload),
        }

    def _write_resuming_payload(self, payload: dict[str, Any]) -> bool:
        with self._resuming_io_lock:
            seq = int(payload.get("_seq") or 0)
            if seq and seq < self._resuming_last_written_seq:
                return True
            result = self._queue_repository().commit_resuming(payload)
            if result.ok:
                if seq:
                    self._resuming_last_written_seq = seq
                self._resuming_failure_warned = False
                return True
            if not self._resuming_failure_warned:
                _log.warning(
                    "Current queue item could not be saved for crash "
                    "recovery; it will retry with the next queue save: %s",
                    result.error,
                )
                self._resuming_failure_warned = True
            return False

    def save_now(self) -> bool:
        """Serialize + atomically replace QUEUE_FILE and expose durability.

        The state lock remains held through the atomic replacement.  Taking a
        snapshot and releasing the lock before I/O allowed a newer snapshot to
        win the I/O mutex first and then be overwritten by an older writer.
        Exact-ID callers also rely on the return value as their commit boundary.
        """
        changed = False
        saved = False
        with self._lock:
            if config_is_writable():
                payload = self._build_save_payload_locked()
                needs_sidecar_migration = (
                    not self._identity_ids_durable
                    and self._resuming_file().exists())
                sidecar_payload = (
                    self._build_resuming_payload_locked()
                    if needs_sidecar_migration else None)
                main_saved = self._write_save_payload(payload)
                sidecar_saved = True
                if main_saved and sidecar_payload is not None:
                    sidecar_saved = self._write_resuming_payload(
                        sidecar_payload)
                saved = bool(main_saved and sidecar_saved)
            changed = self._identity_ids_durable != saved
            self._identity_ids_durable = saved
        if changed:
            self._notify()
        return saved

    def _mark_identity_persistence_failed(self) -> None:
        """Disable exact-ID UI actions until an authoritative save succeeds."""
        changed = False
        with self._lock:
            if self._identity_ids_durable:
                self._identity_ids_durable = False
                changed = True
        if changed:
            self._notify()

    def save_debounced(self):
        """Schedule a save after _save_interval_sec, coalescing bursts.

        A single reusable daemon thread waits until the latest deadline;
        each call only pushes that deadline out and signals the condition.
        """
        with self._save_cond:
            if getattr(self, "_atexit_disabled", False):
                return
            self._save_deadline = time.monotonic() + self._save_interval_sec
            if self._save_thread is None or not self._save_thread.is_alive():
                self._save_thread = threading.Thread(
                    target=self._debounced_save_loop,
                    daemon=True,
                    name="queues-save",
                )
                self._save_thread.start()
            self._save_cond.notify_all()

    def _debounced_save_loop(self):
        while True:
            with self._save_cond:
                while True:
                    if getattr(self, "_atexit_disabled", False):
                        self._save_deadline = None
                        self._save_thread = None
                        return
                    deadline = self._save_deadline
                    if deadline is None:
                        self._save_thread = None
                        return
                    delay = deadline - time.monotonic()
                    if delay <= 0:
                        self._save_deadline = None
                        break
                    self._save_cond.wait(delay)
            self.save_now()

    def _atexit_flush(self):
        """atexit hook — cancel any pending debounce timer and force a
        synchronous save. No-op if nothing is pending. Called once per
        process at interpreter shutdown.

        Refuses to save when self._atexit_disabled is True — set by
        mark_orphan() so a discarded instance's atexit doesn't clobber
        the live instance's file.
        """
        if getattr(self, "_atexit_disabled", False):
            return
        # Set the disable flag + clear the pending deadline atomically
        # under the condition lock so a concurrent save_debounced cannot
        # schedule work after our final flush.
        # didn't actually hold).
        try:
            with self._save_cond:
                self._atexit_disabled = True
                self._save_deadline = None
                self._save_cond.notify_all()
            self.save_now()
        except Exception as e:
            swallow("atexit queue flush", e)

    # ── sync queue ──────────────────────────────────────────────────

    def sync_enqueue_with_id(
            self, channel: dict[str, Any], *, durable: bool = False
    ) -> str | None:
        """Add a channel to the sync queue if not already present.
        Dedupe is keyed on (kind, url) so a "Download X" and a
        separate "Metadata check X" can coexist — they're different
        units of work even though they target the same channel. Returns the
        permanent task ID, or ``None`` for a logical duplicate.
        """
        save_failed = False
        with self._lock:
            used_ids = {
                str(c.get("task_id") or "").strip()
                for c in self.sync if c.get("task_id")
            }
            item, _changed = self._normalize_task(
                channel, "sync", used_ids)
            key = self._sync_identity_key(item)
            for c in self.sync:
                if key is not None and self._sync_identity_key(c) == key:
                    return None
            original = list(self.sync)
            self.sync.append(item)
            if durable and not self.save_now():
                self.sync[:] = original
                save_failed = True
        if save_failed:
            self._notify()
            return None
        self._notify()
        if not durable:
            self.save_debounced()
        return item["task_id"]

    def sync_enqueue(self, channel: dict[str, Any]) -> bool:
        return self.sync_enqueue_with_id(channel) is not None

    def sync_reserve_task(self, channel: dict[str, Any]) -> str | None:
        """Durably claim or create one logical sync-lane task.

        A caller carrying an existing permanent ID may reclaim the same saved
        row during an in-process handoff.  A new caller adopts an orphan row's
        ID, keeping the runtime chain and persisted popover representation on
        one identity.
        """
        save_failed = False
        with self._lock:
            key = self._sync_identity_key(channel)
            existing = next(
                (item for item in self.sync
                 if key is not None and self._sync_identity_key(item) == key),
                None,
            )
            if existing is not None:
                existing_id = str(existing.get("task_id") or "").strip()
                requested_id = str(channel.get("task_id") or "").strip()
                if not existing_id or (requested_id
                                       and requested_id != existing_id):
                    return None
                task_id = existing_id
                created = False
            else:
                used_ids = {
                    str(item.get("task_id") or "").strip()
                    for item in self.sync if item.get("task_id")
                }
                queued, _changed = self._normalize_task(
                    channel, "sync", used_ids)
                self.sync.append(queued)
                task_id = queued["task_id"]
                created = True
            if not self.save_now():
                if created:
                    self.sync[:] = [
                        item for item in self.sync
                        if str(item.get("task_id") or "").strip() != task_id
                    ]
                save_failed = True
        if save_failed:
            self._notify()
            return None
        self._notify()
        return task_id

    def sync_snapshot(self) -> list[dict[str, Any]]:
        """Return a lock-protected copy of the pending sync queue."""
        with self._lock:
            return copy.deepcopy(self.sync)

    def sync_peek_next(
            self, *, exclude_kinds: set[str] | None = None
    ) -> dict[str, Any] | None:
        """Return the next runnable sync task without surrendering durability.

        Workers inspect the task first, honor pause/cancel gates, and then call
        :meth:`sync_promote_task_to_current`.  This prevents the old
        pop-then-debounce window where a paused worker could save neither a
        pending nor a current copy.
        """
        excluded = {
            str(kind or "").strip().lower()
            for kind in (exclude_kinds or set())
            if str(kind or "").strip()
        }
        with self._lock:
            item = next(
                (task for task in self.sync
                 if str(task.get("kind") or "download").strip().lower()
                 not in excluded),
                None,
            )
            return copy.deepcopy(item) if item is not None else None

    def sync_pop(self) -> dict[str, Any] | None:
        with self._lock:
            if not self.sync:
                return None
            ch = self.sync.pop(0)
        self._notify()
        self.save_debounced()
        return ch

    def sync_pop_next(
            self, *, exclude_kinds: set[str] | None = None
    ) -> dict[str, Any] | None:
        """Pop the first runnable sync item without consuming reserved kinds.

        Redownload rows are durably owned by the redownload chain. A regular
        sync worker must be able to drain tasks around them without ever
        removing those recovery records from QueueState.
        """
        excluded = {
            str(kind or "").strip().lower()
            for kind in (exclude_kinds or set())
            if str(kind or "").strip()
        }
        with self._lock:
            target = next(
                (index for index, item in enumerate(self.sync)
                 if str(item.get("kind") or "download").strip().lower()
                 not in excluded),
                -1,
            )
            if target < 0:
                return None
            item = self.sync.pop(target)
        self._notify()
        self.save_debounced()
        return item

    def sync_remove(self, url: str) -> bool:
        """Legacy internal removal of ONE queued item matching ``url``.

        User-facing mutations call :meth:`sync_remove_task` with an opaque
        ID. This target helper remains for internal legacy/channel cleanup.
        """
        with self._lock:
            target_idx = -1
            for i, c in enumerate(self.sync):
                if c.get("url") == url:
                    target_idx = i
                    break
            if target_idx < 0:
                return False
            del self.sync[target_idx]
        self._notify()
        self.save_debounced()
        return True

    def sync_remove_at(self, idx: int, expected_url: str = "",
                       expected_name: str = "") -> bool:
        """Remove a queued sync item by identity, using `idx` as a fast path.

        `expected_url` / `expected_name` describe what the caller
        thought was at that slot. When either identity field is supplied,
        the index is trusted only if the entry still matches; otherwise we
        search the latest queue snapshot for the identity before deleting.
        Both empty keeps the legacy exact-index behavior.
        """
        with self._lock:
            has_identity = bool(expected_url or expected_name)
            if not has_identity and (idx < 0 or idx >= len(self.sync)):
                return False

            def matches(item: dict[str, Any]) -> bool:
                cur_url = (item.get("url") or "").strip()
                cur_name = (item.get("name")
                            or item.get("folder") or "").strip()
                return ((bool(expected_url) and cur_url == expected_url)
                        or (bool(expected_name) and cur_name == expected_name))

            target_idx = idx
            if has_identity:
                if idx < 0 or idx >= len(self.sync) or not matches(self.sync[idx]):
                    target_idx = next(
                        (i for i, item in enumerate(self.sync)
                         if matches(item)),
                        -1,
                    )
                    if target_idx < 0:
                        return False
            del self.sync[target_idx]
        self._notify()
        self.save_debounced()
        return True

    def sync_remove_by_name(self, name: str) -> bool:
        """Remove the FIRST queued sync item whose name/folder matches
        `name`. Public encapsulated replacement for the queue_mixin
        fallback that used to reach into `self._queues._lock` and
        `self._queues.sync` directly and bypass QueueState's
        invariants (audit: queue_mixin H5).
        """
        if not name:
            return False
        with self._lock:
            target_idx = -1
            for i, c in enumerate(self.sync):
                if (c.get("name") or c.get("folder") or "") == name:
                    target_idx = i
                    break
            if target_idx < 0:
                return False
            del self.sync[target_idx]
        self._notify()
        self.save_debounced()
        return True

    def sync_requeue_front(self, channel: dict[str, Any]) -> bool:
        """Insert `channel` at the front of the sync queue atomically.
        Used by sync_all on a pause-interrupted channel so Resume picks
        the in-flight channel back up first. Replaces a bare
        `queues.sync.insert(0, ch); queues._notify()` pair that bypassed
        `_lock`, racing with concurrent `sync_pop` / `sync_remove` /
        `sync_enqueue` callers (audit: sync/sync_all.py C7).
        """
        with self._lock:
            used_ids = {
                str(c.get("task_id") or "").strip()
                for c in self.sync if c.get("task_id")
            }
            item, _changed = self._normalize_task(
                channel, "sync", used_ids)
            key = self._sync_identity_key(item)
            if key is not None:
                for existing in self.sync:
                    if self._sync_identity_key(existing) == key:
                        return False
            self.sync.insert(0, item)
        self._notify()
        self.save_debounced()
        return True

    def sync_clear(self) -> int:
        """Remove every queued sync task; keep the currently-running one.
        Returns the number removed, or ``-1`` when the durable commit failed.
        On failure the exact in-memory list is restored."""
        with self._lock:
            removed = len(self.sync)
            original = self.sync
            self.sync = []
        if removed:
            self._notify()
            if not self.save_now():
                with self._lock:
                    self.sync = original
                self._notify()
                return -1
        return removed

    def sync_clear_except_kinds(self, excluded_kinds: set[str]) -> int:
        """Durably clear pending sync work except explicitly owned kinds.

        A regular Sync pass must never consume or discard ``redownload``
        rows: those rows share the Sync Tasks display, but a separate worker
        owns their execution. This filtered clear is used only by scheduled
        rate-limit cleanup, where ordinary Sync work is intentionally dropped
        so a later scheduler pass can rebuild it.

        Returns the number removed, or ``-1`` when the durable commit failed.
        The exact prior queue is restored on failure.
        """
        keep_kinds = {
            str(kind or "").strip().lower()
            for kind in (excluded_kinds or set())
            if str(kind or "").strip()
        }
        save_failed = False
        with self._lock:
            # When a legacy-ID migration has not committed to every
            # authoritative file, save_now can replace the main queue and then
            # fail its sidecar migration. A filtered destructive write cannot
            # safely roll that partial commit back, so refuse it until identity
            # durability is established.
            if not self._identity_ids_durable:
                return -1
            original = list(self.sync)
            self.sync = [
                item for item in self.sync
                if str(item.get("kind") or "download").strip().lower()
                in keep_kinds
            ]
            removed = len(original) - len(self.sync)
            if removed and not self.save_now():
                self.sync[:] = original
                save_failed = True
        if removed:
            self._notify()
        return -1 if save_failed else removed

    def gpu_clear(self) -> int:
        """Durably clear pending GPU tasks, rolling memory back on failure."""
        with self._lock:
            removed = len(self.gpu)
            original = self.gpu
            self.gpu = []
        if removed:
            self._notify()
            if not self.save_now():
                with self._lock:
                    self.gpu = original
                self._notify()
                return -1
        return removed

    def restore_pending_snapshot(
            self, lane: str, items: list[dict[str, Any]]) -> bool:
        """Compensate a later peer-store failure with one durable snapshot."""
        normalized_lane = str(lane or "").strip().lower()
        if normalized_lane not in {"sync", "gpu"}:
            return False
        attr = "sync" if normalized_lane == "sync" else "gpu"
        restored = copy.deepcopy(items or [])
        with self._lock:
            current = getattr(self, attr)
            setattr(self, attr, restored)
            if not self.save_now():
                setattr(self, attr, current)
                self._notify()
                return False
        self._notify()
        return True

    def sync_reorder(self, task_id: str, new_index: int, *,
                     durable: bool = False) -> bool:
        """Move one pending sync task, addressed only by permanent ID."""
        wanted = str(task_id or "").strip()
        save_failed = False
        with self._lock:
            idx = next((i for i, c in enumerate(self.sync)
                        if str(c.get("task_id") or "").strip() == wanted), -1)
            if idx < 0 or new_index < 0 or new_index >= len(self.sync):
                return False
            original = list(self.sync)
            item = self.sync.pop(idx)
            self.sync.insert(new_index, item)
            if durable and not self.save_now():
                self.sync[:] = original
                save_failed = True
        if save_failed:
            self._notify()
            return False
        self._notify()
        if not durable:
            self.save_debounced()
        return True

    # ── gpu queue ───────────────────────────────────────────────────

    def gpu_enqueue_with_id(
            self, item: dict[str, Any], *, durable: bool = False
    ) -> str | None:
        """Queue a transcription/encode job for the GPU lane. Dedupes
        by ``(kind, path)`` to prevent double-entries on startup when both
        QueueState.load() (which restores gpu from disk) and the
        transcribe pending-journal recovery might try to add the same
        item. Different job kinds may intentionally target the same file.
        Returns the permanent ID, or ``None`` for a logical duplicate."""
        save_failed = False
        with self._lock:
            used_ids = {
                str(existing.get("task_id") or "").strip()
                for existing in self.gpu if existing.get("task_id")
            }
            queued, _changed = self._normalize_task(item, "gpu", used_ids)
            key = self._gpu_identity_key(queued)
            if key is not None:
                for existing in self.gpu:
                    if self._gpu_identity_key(existing) == key:
                        return None
            original = list(self.gpu)
            self.gpu.append(queued)
            if durable and not self.save_now():
                self.gpu[:] = original
                save_failed = True
        if save_failed:
            self._notify()
            return None
        self._notify()
        if not durable:
            self.save_debounced()
        return queued["task_id"]

    def gpu_enqueue(self, item: dict[str, Any]) -> bool:
        return self.gpu_enqueue_with_id(item) is not None

    def gpu_reserve_task(
            self, item: dict[str, Any], *,
            reserved_task_ids: set[str] | None = None,
            required_task_id: str = "",
    ) -> dict[str, Any] | None:
        """Durably reserve one logical GPU row for an internal job.

        If a crash left the same ``(kind, path)`` row in QueueState, return
        that row's exact ID so the journal can adopt it.  A newly-created row
        is rolled back in memory when the atomic queue-file replacement fails.
        The returned token can be passed to :meth:`gpu_rollback_reservation`
        if the caller's second durable store cannot commit.
        """
        reserved = {
            str(task_id or "").strip()
            for task_id in (reserved_task_ids or set())
            if str(task_id or "").strip()
        }
        required = str(required_task_id or "").strip()
        save_failed = False
        with self._lock:
            key = self._gpu_identity_key(item)
            existing_index = next(
                (index for index, queued in enumerate(self.gpu)
                 if key is not None and self._gpu_identity_key(queued) == key),
                -1,
            )
            previous = None
            if existing_index >= 0:
                existing = self.gpu[existing_index]
                previous = copy.deepcopy(existing)
                task_id = str(existing.get("task_id") or "").strip()
                if required and task_id != required:
                    queue_ids = {
                        str(queued.get("task_id") or "").strip()
                        for index, queued in enumerate(self.gpu)
                        if index != existing_index
                    }
                    if required in reserved or required in queue_ids:
                        return None
                    replacement = copy.deepcopy(existing)
                    replacement["task_id"] = required
                    self.gpu[existing_index] = replacement
                    task_id = required
                if not task_id or (task_id in reserved and task_id != required):
                    return None
                created = False
            else:
                used_ids = {
                    str(queued.get("task_id") or "").strip()
                    for queued in self.gpu if queued.get("task_id")
                } | reserved
                queued, _changed = self._normalize_task(
                    item, "gpu", used_ids)
                self.gpu.append(queued)
                task_id = queued["task_id"]
                created = True
            if not self.save_now():
                if created:
                    self.gpu[:] = [
                        queued for queued in self.gpu
                        if str(queued.get("task_id") or "").strip() != task_id
                    ]
                elif previous is not None:
                    self.gpu[existing_index] = previous
                save_failed = True
        if save_failed:
            self._notify()
            return None
        self._notify()
        return {"task_id": task_id, "created": created,
                "previous": previous, "index": existing_index}

    def gpu_rollback_reservation(self, token: dict[str, Any]) -> bool:
        """Remove a row created by ``gpu_reserve_task`` after peer failure."""
        if not isinstance(token, dict):
            return False
        previous = token.get("previous")
        if not token.get("created") and not isinstance(previous, dict):
            return True
        if token.get("created"):
            return self.gpu_remove(
                str(token.get("task_id") or ""), durable=True)
        task_id = str(token.get("task_id") or "").strip()
        save_failed = False
        with self._lock:
            target = next(
                (index for index, item in enumerate(self.gpu)
                 if str(item.get("task_id") or "").strip() == task_id),
                -1,
            )
            if target < 0:
                return False
            current = self.gpu[target]
            self.gpu[target] = copy.deepcopy(previous)
            if not self.save_now():
                self.gpu[target] = current
                save_failed = True
        self._notify()
        return not save_failed

    def gpu_items_for_ids(
            self, task_ids: set[str]
    ) -> list[tuple[int, dict[str, Any]]]:
        """Snapshot exact pending rows and positions for transaction rollback."""
        wanted = {
            str(task_id or "").strip() for task_id in task_ids
            if str(task_id or "").strip()
        }
        with self._lock:
            return [
                (index, copy.deepcopy(item))
                for index, item in enumerate(self.gpu)
                if str(item.get("task_id") or "").strip() in wanted
            ]

    def gpu_restore_items(
            self, entries: list[tuple[int, dict[str, Any]]], *,
            durable: bool = True,
    ) -> bool:
        """Restore exact removed rows without overwriting concurrent additions."""
        normalized_entries = [
            (int(index), copy.deepcopy(item))
            for index, item in entries
            if isinstance(item, dict)
            and str(item.get("task_id") or "").strip()
        ]
        if not normalized_entries:
            return True
        save_failed = False
        changed = False
        with self._lock:
            original = list(self.gpu)
            present = {
                str(item.get("task_id") or "").strip() for item in self.gpu
            }
            for index, item in sorted(normalized_entries, key=lambda row: row[0]):
                task_id = str(item.get("task_id") or "").strip()
                if task_id in present:
                    continue
                self.gpu.insert(max(0, min(index, len(self.gpu))), item)
                present.add(task_id)
                changed = True
            if changed and durable and not self.save_now():
                self.gpu[:] = original
                save_failed = True
        if save_failed:
            self._notify()
            return False
        if changed:
            self._notify()
            if not durable:
                self.save_debounced()
        return True

    def sync_remove_task(self, task_id: str, *, durable: bool = False) -> bool:
        """Remove exactly one pending sync task by permanent identity."""
        wanted = str(task_id or "").strip()
        if not wanted:
            return False
        save_failed = False
        with self._lock:
            target_idx = next(
                (i for i, item in enumerate(self.sync)
                 if str(item.get("task_id") or "").strip() == wanted),
                -1,
            )
            if target_idx < 0:
                return False
            removed = self.sync.pop(target_idx)
            if durable and not self.save_now():
                self.sync.insert(target_idx, removed)
                save_failed = True
        if save_failed:
            self._notify()
            return False
        self._notify()
        if not durable:
            self.save_debounced()
        return True

    def sync_remove_tasks(self, task_ids: list[str], *,
                          durable: bool = False,
                          require_all: bool = False) -> list[str]:
        """Atomically remove exact sync IDs, rolling back on save failure."""
        wanted = {
            str(task_id or "").strip() for task_id in (task_ids or [])
            if str(task_id or "").strip()
        }
        if not wanted:
            return []
        save_failed = False
        with self._lock:
            removed = [
                str(item.get("task_id") or "").strip()
                for item in self.sync
                if str(item.get("task_id") or "").strip() in wanted
            ]
            if not removed or (require_all and set(removed) != wanted):
                return []
            original = list(self.sync)
            removed_set = set(removed)
            self.sync = [
                item for item in self.sync
                if str(item.get("task_id") or "").strip() not in removed_set
            ]
            if durable and not self.save_now():
                self.sync[:] = original
                save_failed = True
        if save_failed:
            self._notify()
            return []
        self._notify()
        if not durable:
            self.save_debounced()
        return removed

    def sync_remove_all_for_target(self, url: str) -> int:
        """Internal channel cleanup; remove all tasks targeting ``url``."""
        wanted = str(url or "").strip()
        if not wanted:
            return 0
        with self._lock:
            before = len(self.sync)
            self.sync = [
                item for item in self.sync
                if str(item.get("url") or "").strip() != wanted
                and str(item.get("channel_url") or "").strip() != wanted
            ]
            removed = before - len(self.sync)
        if removed:
            self._notify()
            self.save_debounced()
        return removed

    def sync_remove_matching_task(self, task: dict[str, Any]) -> bool:
        """Internal logical-dedupe cleanup used when deferring current work."""
        key = self._sync_identity_key(task)
        if key is None:
            return False
        with self._lock:
            target_idx = next(
                (i for i, item in enumerate(self.sync)
                 if self._sync_identity_key(item) == key),
                -1,
            )
            if target_idx < 0:
                return False
            del self.sync[target_idx]
        self._notify()
        self.save_debounced()
        return True

    def sync_defer_task(self, channel: dict[str, Any]) -> bool:
        """Durably place the exact current sync task at the pending tail."""
        if not isinstance(channel, dict):
            return False
        task_id = str(channel.get("task_id") or "").strip()
        key = self._sync_identity_key(channel)
        if not task_id or key is None:
            return False
        save_failed = False
        with self._lock:
            original = list(self.sync)
            for existing in self.sync:
                existing_id = str(existing.get("task_id") or "").strip()
                if (existing_id == task_id
                        and self._sync_identity_key(existing) != key):
                    return False
            deferred = copy.deepcopy(channel)
            deferred["task_id"] = task_id
            kind, _changed = self._normalize_kind(deferred, "sync")
            deferred["kind"] = kind
            self.sync[:] = [
                existing for existing in self.sync
                if self._sync_identity_key(existing) != key
            ]
            self.sync.append(deferred)
            if not self.save_now():
                self.sync[:] = original
                save_failed = True
        if save_failed:
            self._notify()
            return False
        self._notify()
        return True

    def sync_requeue_current_front(self, channel: dict[str, Any]) -> bool:
        """Durably turn the exact running sync task back into pending work.

        Pause uses this two-commit handoff: first publish the pending copy
        while the current recovery slot still exists, then clear that slot.
        A failure therefore leaves at least one authoritative copy and never
        paints a task as merely pending before it has actually been saved.
        """
        if not isinstance(channel, dict):
            return False
        task_id = str(channel.get("task_id") or "").strip()
        key = self._sync_identity_key(channel)
        if not task_id or key is None:
            return False
        with self._lock:
            current = copy.deepcopy(self.current_sync)
            if (str((current or {}).get("task_id") or "").strip()
                    != task_id
                    or self._sync_identity_key(current or {}) != key):
                return False
            original = list(self.sync)
            for existing in self.sync:
                existing_id = str(existing.get("task_id") or "").strip()
                if existing_id == task_id and self._sync_identity_key(
                        existing) != key:
                    return False
            pending = copy.deepcopy(channel)
            pending["task_id"] = task_id
            kind, _changed = self._normalize_kind(pending, "sync")
            pending["kind"] = kind
            self.sync[:] = [
                existing for existing in self.sync
                if self._sync_identity_key(existing) != key
            ]
            self.sync.insert(0, pending)
            if not self.save_now():
                self.sync[:] = original
                self._notify()
                return False
        self._notify()
        # If this second commit fails, replace_current_task_durable restores
        # the old current slot. The already-saved pending copy remains too;
        # startup identity reconciliation collapses that safe duplication.
        return self.replace_current_task_durable(
            "sync", None, expected_task_id=task_id)

    def sync_promote_task_to_current(self, task_id: str) -> bool:
        """Durably hand one pending sync task to the running slot.

        The current sidecar is committed while the pending row still exists;
        only then is the pending copy removed from the main queue.  A crash at
        either boundary therefore retains at least one authoritative copy.
        """
        wanted = str(task_id or "").strip()
        if not wanted:
            return False
        with self._lock:
            if self.current_sync is not None:
                return False
            item = next(
                (copy.deepcopy(task) for task in self.sync
                 if str(task.get("task_id") or "").strip() == wanted),
                None,
            )
        if item is None:
            return False
        if not self.replace_current_task_durable(
                "sync", item, expected_task_id=""):
            return False
        if self.sync_remove_task(wanted, durable=True):
            return True
        # The pending row is still authoritative. Remove the duplicate current
        # slot so the caller can safely leave its runtime reservation queued.
        if not self.replace_current_task_durable(
                "sync", None, expected_task_id=wanted):
            _log.warning(
                "Could not compensate failed sync task promotion for %s",
                wanted)
        return False

    def gpu_snapshot(self) -> list[dict[str, Any]]:
        """Return a lock-protected copy of the pending GPU queue."""
        with self._lock:
            return copy.deepcopy(self.gpu)

    def gpu_pop(self) -> dict[str, Any] | None:
        with self._lock:
            if not self.gpu:
                return None
            it = self.gpu.pop(0)
        self._notify()
        self.save_debounced()
        return it

    def gpu_pop_matching(self, task_id: str = "", expected_path: str = "",
                         expected_bulk_id: str = "") -> dict[str, Any] | None:
        """Pop the queued GPU row matching the job that actually started."""
        wanted = str(task_id or "").strip()
        ep = str(expected_path or "").strip()
        eb = str(expected_bulk_id or "").strip()
        with self._lock:
            if not self.gpu:
                return None
            target_idx = -1
            if wanted:
                target_idx = next(
                    (i for i, item in enumerate(self.gpu)
                     if str(item.get("task_id") or "").strip() == wanted),
                    -1,
                )
                if target_idx < 0:
                    return None
            elif ep or eb:
                for i, item in enumerate(self.gpu):
                    cur_path = (item.get("path") or "").strip()
                    cur_bulk = str(item.get("bulk_id") or "").strip()
                    if (ep and cur_path == ep) or (eb and cur_bulk == eb):
                        target_idx = i
                        break
            if target_idx < 0:
                target_idx = 0
            it = self.gpu.pop(target_idx)
        self._notify()
        self.save_debounced()
        return it

    def gpu_remove(self, task_id: str, *, durable: bool = False) -> bool:
        """Remove exactly one pending Processing task by permanent ID."""
        wanted = str(task_id or "").strip()
        if not wanted:
            return False
        save_failed = False
        with self._lock:
            target_idx = -1
            for i, item in enumerate(self.gpu):
                if str(item.get("task_id") or "").strip() == wanted:
                    target_idx = i
                    break
            if target_idx < 0:
                return False
            removed = self.gpu.pop(target_idx)
            if durable and not self.save_now():
                self.gpu.insert(target_idx, removed)
                save_failed = True
        if save_failed:
            self._notify()
            return False
        self._notify()
        if not durable:
            self.save_debounced()
        return True

    def gpu_remove_tasks(self, task_ids: list[str], *,
                         durable: bool = False,
                         require_all: bool = False) -> list[str]:
        """Atomically remove the exact Processing IDs represented by a row."""
        wanted = {
            str(task_id or "").strip() for task_id in (task_ids or [])
            if str(task_id or "").strip()
        }
        if not wanted:
            return []
        save_failed = False
        with self._lock:
            removed = [
                str(item.get("task_id") or "").strip()
                for item in self.gpu
                if str(item.get("task_id") or "").strip() in wanted
            ]
            if not removed:
                return []
            if require_all and set(removed) != wanted:
                return []
            original = list(self.gpu)
            removed_set = set(removed)
            self.gpu = [
                item for item in self.gpu
                if str(item.get("task_id") or "").strip() not in removed_set
            ]
            if durable and not self.save_now():
                self.gpu[:] = original
                save_failed = True
        if save_failed:
            self._notify()
            return []
        self._notify()
        if not durable:
            self.save_debounced()
        return removed

    def gpu_remove_legacy_task(self, kind: str, path: str) -> bool:
        """Internal migration cleanup for an in-memory pre-ID task only."""
        wanted = (str(kind or "transcribe").strip().lower(),
                  str(path or "").strip())
        if not wanted[1]:
            return False
        with self._lock:
            target_idx = next(
                (i for i, item in enumerate(self.gpu)
                 if not str(item.get("task_id") or "").strip()
                 and self._gpu_identity_key(item) == wanted),
                -1,
            )
            if target_idx < 0:
                return False
            del self.gpu[target_idx]
        self._notify()
        self.save_debounced()
        return True

    def gpu_remove_at(self, idx: int, expected_path: str = "",
                      expected_bulk_id: str = "") -> bool:
        """Remove a queued GPU item by identity, using `idx` as a fast path."""
        with self._lock:
            has_identity = bool(expected_path or expected_bulk_id)
            if not has_identity and (idx < 0 or idx >= len(self.gpu)):
                return False

            def matches(item: dict[str, Any]) -> bool:
                cur_path = (item.get("path") or "").strip()
                cur_bulk = str(item.get("bulk_id") or "").strip()
                return ((bool(expected_path) and cur_path == expected_path)
                        or (bool(expected_bulk_id)
                            and cur_bulk == expected_bulk_id))

            target_idx = idx
            if has_identity:
                if idx < 0 or idx >= len(self.gpu) or not matches(self.gpu[idx]):
                    target_idx = next(
                        (i for i, item in enumerate(self.gpu)
                         if matches(item)),
                        -1,
                    )
                    if target_idx < 0:
                        return False
            del self.gpu[target_idx]
        self._notify()
        self.save_debounced()
        return True

    def gpu_remove_bulk(self, bulk_id: str) -> int:
        """Remove every GPU queue item sharing a `bulk_id`. Returns the
        number dropped. Used when the coalesced "Transcribe {ch} (N
        videos)" row is removed from the context menu — one click should
        drop all N videos, not just the top one."""
        if not bulk_id:
            return 0
        with self._lock:
            before = len(self.gpu)
            self.gpu = [i for i in self.gpu
                        if str(i.get("bulk_id") or "") != bulk_id]
            dropped = before - len(self.gpu)
        if dropped:
            self._notify()
            self.save_debounced()
        return dropped

    def gpu_reorder(self, task_id: str, new_index: int, *,
                    durable: bool = False) -> bool:
        wanted = str(task_id or "").strip()
        save_failed = False
        with self._lock:
            idx = next((i for i, t in enumerate(self.gpu)
                        if str(t.get("task_id") or "").strip() == wanted), -1)
            if idx < 0 or new_index < 0 or new_index >= len(self.gpu):
                return False
            original = list(self.gpu)
            item = self.gpu.pop(idx)
            self.gpu.insert(new_index, item)
            if durable and not self.save_now():
                self.gpu[:] = original
                save_failed = True
        if save_failed:
            self._notify()
            return False
        self._notify()
        if not durable:
            self.save_debounced()
        return True

    # ── current-task tracking ───────────────────────────────────────

    def set_current_sync(self, ch: dict[str, Any] | None):
        # Snapshot the save payload INSIDE the same lock that sets the value,
        # so we persist exactly what we set — not whatever a concurrent
        # set_current_* leaves live when save_now would re-read (audit r2
        # snapshot race). Persist immediately because a Windows force-kill
        # skips atexit and a 0.5s debounce can lose this transition (H106).
        with self._lock:
            if ch:
                self.current_sync, _changed = self._normalize_task(
                    ch, "sync")
            else:
                self.current_sync = None
            _payload = (self._build_resuming_payload_locked()
                        if config_is_writable() else None)
        self._notify()
        if _payload is not None:
            try:
                if not self._write_resuming_payload(_payload):
                    self._mark_identity_persistence_failed()
                    self.save_debounced()
            except Exception:
                self._mark_identity_persistence_failed()
                self.save_debounced()
        else:
            self._mark_identity_persistence_failed()

    def set_sync_pass_progress(self, index: int, total: int) -> None:
        """Record `(index, total)` so the popover label reads
        'Downloading {name} ({index}/{total})'. Called by sync_start_all
        at the top of each channel iteration. `index=0, total=0` clears
        the pass state (no pass active)."""
        with self._lock:
            self.sync_pass_index = max(0, int(index))
            self.sync_pass_total = max(0, int(total))
        self._notify()

    def set_current_gpu(self, item: dict[str, Any] | None):
        # Snapshot under the SAME lock that sets the value (audit r2 snapshot
        # race) + persist immediately (H106). The GPU lane's in-flight item is
        # the most expensive unit of work (a multi-minute Whisper run), so
        # dropping it from `resuming` on a force-kill is costly.
        with self._lock:
            if item:
                self.current_gpu, _changed = self._normalize_task(
                    item, "gpu")
            else:
                self.current_gpu = None
            _payload = (self._build_resuming_payload_locked()
                        if config_is_writable() else None)
        self._notify()
        if _payload is not None:
            try:
                if not self._write_resuming_payload(_payload):
                    self._mark_identity_persistence_failed()
                    self.save_debounced()
            except Exception:
                self._mark_identity_persistence_failed()
                self.save_debounced()
        else:
            self._mark_identity_persistence_failed()

    # ── UI payload ──────────────────────────────────────────────────

    def to_ui_payload(self) -> dict[str, Any]:
        """Return the shape the queue popovers expect (see web/logs.js renderQueues)."""
        # Snapshot all needed state under the lock, then build the payload
        # (label formatting + bulk coalescing + os.path.basename) OUTSIDE the
        # lock. The master lock was being held across ~100 lines of pure CPU
        # work, serializing the sync/GPU workers (sync_pop/gpu_pop) behind
        # every UI render (audit: queues to_ui_payload lock-hold). deepcopy so
        # the post-lock formatting can't race a concurrent dict mutation.
        with self._lock:
            # Shallow copies suffice: the post-lock formatting only READS
            # scalar fields and builds new dicts (never mutates these), and
            # queued task dicts are effectively immutable once enqueued. A
            # shallow snapshot is race-safe AND avoids deep-copying the whole
            # queue on every UI notify (audit r2: deepcopy was a perf regression).
            cur_sync = dict(self.current_sync) if self.current_sync else None
            sync_q = list(self.sync)
            cur_gpu = dict(self.current_gpu) if self.current_gpu else None
            gpu_q = list(self.gpu)
            pass_total = self.sync_pass_total
            pass_index = self.sync_pass_index
            gpu_paused = self.gpu_paused
            sync_paused = self.sync_paused
            gpu_paused_active = self.gpu_paused_active
            sync_paused_active = self.sync_paused_active
            sync_count = len(sync_q) + (1 if cur_sync else 0)
            gpu_count = len(gpu_q) + (1 if cur_gpu else 0)

        # Test doubles and older in-process callers sometimes assign current_*
        # directly instead of using the setters. Keep rendering fail-safe while
        # the setters remain the authoritative persisted path.
        if cur_sync and not cur_sync.get("task_id"):
            cur_sync, _changed = self._normalize_task(cur_sync, "sync")
        if cur_gpu and not cur_gpu.get("task_id"):
            cur_gpu, _changed = self._normalize_task(cur_gpu, "gpu")
        sync_q = [
            (item if item.get("task_id") else
             self._normalize_task(item, "sync")[0])
            for item in sync_q
        ]
        gpu_q = [
            (item if item.get("task_id") else
             self._normalize_task(item, "gpu")[0])
            for item in gpu_q
        ]

        sync_list = []
        if cur_sync:
            # When a Sync-Subbed pass is running, decorate the active channel
            # label with "(N/total)" so the popover shows pass progress.
            label = self._task_label_sync(cur_sync, running=True)
            if pass_total > 0 and pass_index > 0:
                label = f"{label} ({pass_index}/{pass_total})"
            sync_list.append({
                "task_id": cur_sync["task_id"],
                "name": label,
                "status": "running",
                "kind": (cur_sync.get("kind") or "download"),
                "url": (cur_sync.get("url") or "").strip(),
                "channel_name": (cur_sync.get("name")
                                  or cur_sync.get("folder") or "").strip(),
                "pending_index": None,
                "represented_task_ids": [cur_sync["task_id"]],
                "draggable": False,
            })
        for pending_index, ch in enumerate(sync_q):
            sync_list.append({
                "task_id": ch["task_id"],
                "name": self._task_label_sync(ch, running=False),
                "status": "queued",
                "kind": (ch.get("kind") or "download"),
                "url": (ch.get("url") or "").strip(),
                "channel_name": (ch.get("name")
                                  or ch.get("folder") or "").strip(),
                "pending_index": pending_index,
                "pending_start": pending_index,
                "pending_end": pending_index,
                "represented_task_ids": [ch["task_id"]],
                "draggable": True,
            })

        gpu_list = []
        # Track which bulk_ids are represented by the running item so the
        # still-queued remainder collapses into one "Transcribe {ch} (N more)".
        running_bulk_id = ""
        if cur_gpu:
            running_bulk_id = str(cur_gpu.get("bulk_id") or "")
            gpu_list.append({
                "task_id": cur_gpu["task_id"],
                "name": self._task_label_gpu(cur_gpu, running=True,
                                             bulk_context=None),
                "status": "running",
                "path": (cur_gpu.get("path") or "").strip(),
                "bulk_id": running_bulk_id,
                "bulk_total": int(cur_gpu.get("bulk_total") or 0),
                "bulk_index": int(cur_gpu.get("bulk_index") or 0),
                "kind": (cur_gpu.get("kind") or "transcribe"),
                "title": (cur_gpu.get("title") or ""),
                "channel": (cur_gpu.get("channel") or "").strip(),
                "pending_index": None,
                "represented_task_ids": [cur_gpu["task_id"]],
                "draggable": False,
            })
        # Coalesce queued items by bulk_id. First pass: count per bulk_id.
        # Second pass: emit one row per bulk (or per-item if no bulk_id).
        bulk_counts: dict[str, int] = {}
        bulk_channels: dict[str, str] = {}
        bulk_members: dict[str, list[tuple[int, dict[str, Any]]]] = {}
        for pending_index, t in enumerate(gpu_q):
            bid = str(t.get("bulk_id") or "")
            if bid:
                bulk_counts[bid] = bulk_counts.get(bid, 0) + 1
                bulk_members.setdefault(bid, []).append((pending_index, t))
                if bid not in bulk_channels:
                    bulk_channels[bid] = (t.get("channel") or "").strip()
        seen_bulks: set = set()
        for pending_index, t in enumerate(gpu_q):
            bid = str(t.get("bulk_id") or "")
            if bid and bid in seen_bulks:
                continue
            if bid and bulk_counts.get(bid, 0) > 1:
                # Emit one condensed row for the whole bulk.
                ch_name = bulk_channels.get(bid) or (t.get("channel") or "?")
                remaining = bulk_counts[bid]
                # If part of this bulk is the "running" slot, the queued
                # remainder is one short of bulk_total.
                if bid == running_bulk_id:
                    label = f"Transcribe {ch_name} ({remaining} more)"
                else:
                    label = f"Transcribe {ch_name} ({remaining} videos)"
                members = bulk_members.get(bid, [(pending_index, t)])
                member_indices = [idx for idx, _item in members]
                member_ids = [
                    str(item.get("task_id") or "")
                    for _idx, item in members
                ]
                gpu_list.append({
                    # A grouped row is not itself a fake task. Its primary ID
                    # is only a display anchor; every mutation receives the
                    # complete exact member-ID list below.
                    "task_id": member_ids[0],
                    "name": label,
                    "status": "queued",
                    "bulk_id": bid,
                    "bulk_count": remaining,
                    "kind": (t.get("kind") or "transcribe"),
                    "title": ch_name,
                    "channel": ch_name,
                    "task_ids": member_ids,
                    "represented_task_ids": member_ids,
                    "pending_indices": member_indices,
                    "pending_start": min(member_indices),
                    "pending_end": max(member_indices),
                    "pending_index": min(member_indices),
                    "draggable": False,
                })
                seen_bulks.add(bid)
            else:
                gpu_list.append({
                    "task_id": t["task_id"],
                    "name": self._task_label_gpu(t, running=False,
                                                 bulk_context=None),
                    "status": "queued",
                    "path": (t.get("path") or "").strip(),
                    "bulk_id": str(t.get("bulk_id") or ""),
                    "kind": (t.get("kind") or "transcribe"),
                    "title": (t.get("title") or ""),
                    "channel": (t.get("channel") or "").strip(),
                    "pending_index": pending_index,
                    "pending_start": pending_index,
                    "pending_end": pending_index,
                    "represented_task_ids": [t["task_id"]],
                    "draggable": True,
                })
        return {
            "sync": sync_list,
            "gpu": gpu_list,
            "sync_count": sync_count,
            "gpu_count": gpu_count,
            "gpu_paused": gpu_paused,
            "sync_paused": sync_paused,
            # Pause-pending vs pause-active distinction so the UI can blink
            # the Resume button between "user clicked pause" and "worker
            # actually entered pause-wait".
            "gpu_paused_active": gpu_paused_active,
            "sync_paused_active": sync_paused_active,
            "identity_ids_durable": self._identity_ids_durable,
        }

    @staticmethod
    def _task_label_sync(ch: dict[str, Any], running: bool) -> str:
        """Pos 1 (running) uses present-continuous, other slots use the plain verb.
        Branches on `kind` so the popover shows meaningful labels for
        non-download sync-queue items (metadata recheck, etc.).
        Label must START with a verb that `colorizeTaskName` recognizes
        so the popover rows get color-coded — "Metadata" → pink,
        "Download" → green, etc.
        """
        name = ch.get("name") or ch.get("folder") or "?"
        status_label = str(ch.get("_status_label") or "").strip()
        if running and status_label:
            return f"{status_label} \u2014 {name}"
        kind = (ch.get("kind") or "download").lower()
        if kind == "metadata":
            # Keep "Metadata" as the leading word so `colorizeTaskName`
            # in logs.js picks the pink `qv-meta` class. "the
            # check metadata part of these tasks in queue are supposed
            # to be colored pink LIKE THEY WERE IN PREVIOUS VERSION."
            return f"Metadata check \u2014 {name}"
        if kind == "metadata_comments":
            # Comments-refresh task. Leading "Metadata" word so
            # colorizeTaskName picks the pink qv-meta class \u2014 these
            # were showing as "Download X" (green) before, which
            # misled users into thinking videos were being downloaded.
            return f"Metadata comments \u2014 {name}"
        if kind == "videoid_backfill":
            # Fix IDs task — share the Metadata color family (pink)
            # since it's a metadata-kind repair, not a download. Label
            # starts with "Metadata" so colorizeTaskName picks up the
            # pink `qv-meta` class like the other metadata rows.
            return f"Metadata ID fix \u2014 {name}"
        if kind == "repair_yt_captions":
            # Repair YT auto-captions task. Leading "Metadata" so
            # colorizeTaskName picks the pink qv-meta class \u2014 it's a
            # transcript-side repair, not a download.
            return f"Metadata repair YT captions \u2014 {name}"
        if kind == "punct_restore":
            # Restore transcript punctuation task \u2014 same pink color
            # family as the other transcript-side repair tools.
            return f"Metadata restore punctuation \u2014 {name}"
        if kind == "provenance":
            # Embed file tags task \u2014 same pink (metadata-family)
            # color as the other archive-repair tools.
            return f"Metadata embed file tags \u2014 {name}"
        if kind == "redownload":
            # Classic showed active redownload as "Redownload
            # ChannelName (480p)" with a Pause/Resume state.
            # Leading word must be recognized by colorizeTaskName
            # so the row picks up the redownload (chartreuse) color.
            res = str(ch.get("redownload_res") or "").strip()
            res_label = ""
            if res:
                res_label = f" ({'Best' if res == 'best' else res + 'p'})"
            verb = "Redownloading" if running else "Redownload"
            return f"{verb} {name}{res_label}"
        verb = "Downloading" if running else "Download"
        return f"{verb} {name}"

    @staticmethod
    def _task_label_gpu(t: dict[str, Any], running: bool,
                        bulk_context: dict[str, Any] | None = None) -> str:
        # `bulk_context` is reserved for future coalesce-label overrides
        # from to_ui_payload (per-video label remains the same for now).
        title = t.get("title") or os.path.basename(t.get("path", "?")).rsplit(".", 1)[0]
        raw_kind = (t.get("kind") or "transcribe").lower()
        if raw_kind == "transcribe":
            verb = "Transcribing" if running else "Transcribe"
        elif raw_kind == "encode":
            verb = "Encoding" if running else "Encode"
        elif raw_kind == "compress":
            verb = "Compressing" if running else "Compress"
        else:
            verb = raw_kind.capitalize()
        # When the job is part of a bulk and is currently running, decorate
        # it with "(X/total)" so the user can see progress through the batch.
        if running:
            bi = int(t.get("bulk_index") or 0)
            bt = int(t.get("bulk_total") or 0)
            if bt > 1:
                return f"{verb} {title} ({bi + 1}/{bt})"
        return f"{verb} {title}"

    # ── pause state ─────────────────────────────────────────────────

    def set_gpu_paused(self, paused: bool, restored: bool = False):
        with self._lock:
            old_paused = self.gpu_paused
            self.gpu_paused = bool(paused)
            # Any explicit set is a current-session decision, so it's no
            # longer a "restored" pause the auto-release paths may clear —
            # UNLESS the caller is the launch-restore path re-affirming a
            # pause that load() already marked restored (`restored=True`).
            # Without this carve-out, main.py's `set_gpu_paused(True)` right
            # after load() wiped the restored flag load() had just set, so
            # the sync-start / enqueue auto-release ("fresh work + Auto on
            # → drain the restored backlog") could NEVER fire — a fresh
            # auto-sync download sat parked until the user hit Resume by
            # hand even with Auto checked.
            self.gpu_pause_restored = bool(paused and restored)
            # Only reset the active flag on a True→False transition.
            # Previously this reset on EVERY call, so a redundant
            # pause (e.g. tray + UI both flipping the bit) wrongly
            # cleared `gpu_paused_active` while the worker was still
            # parked — the UI showed a "blinking" half-paused state
            # until the worker re-set it.
            if old_paused and not paused:
                self.gpu_paused_active = False
        self._notify()
        self.save_debounced()

    def set_sync_paused(self, paused: bool):
        with self._lock:
            old_paused = self.sync_paused
            self.sync_paused = bool(paused)
            if old_paused and not paused:
                self.sync_paused_active = False
        self._notify()
        self.save_debounced()

    def set_sync_paused_active(self, active: bool):
        """Worker-side hook: flip True when the sync worker has actually
        entered its pause-wait block, False on exit. Frontend reads this
        to distinguish "pause requested" (button blinks) vs "actually
        paused" (button solid)."""
        with self._lock:
            new_val = bool(active)
            if self.sync_paused_active == new_val:
                return  # no change → no notify (avoid renderQueues spam)
            self.sync_paused_active = new_val
        self._notify()

    def set_gpu_paused_active(self, active: bool):
        """Worker-side hook for the GPU/transcribe queue (see set_sync_paused_active)."""
        with self._lock:
            new_val = bool(active)
            if self.gpu_paused_active == new_val:
                return
            self.gpu_paused_active = new_val
        self._notify()

    # ── stats ───────────────────────────────────────────────────────

    def counts(self) -> dict[str, int]:
        """Return pending plus in-flight counts for both queue lanes."""
        with self._lock:
            return {
                "sync": len(self.sync) + (1 if self.current_sync else 0),
                "gpu": len(self.gpu) + (1 if self.current_gpu else 0),
            }

    # ── restore-on-launch helpers ───────────────────────────────────
    def has_sync_pipeline_items(self) -> bool:
        """Return whether startup restored pending sync-lane work."""
        with self._lock:
            return bool(self.sync)

    def has_gpu_items(self) -> bool:
        with self._lock:
            return bool(self.gpu)
