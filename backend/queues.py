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
      "order": [["kind", "id"], ...],
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
from collections.abc import Callable
from typing import Any

from .log import get_logger
from .ytarchiver_config import QUEUE_FILE, config_is_writable

_log = get_logger(__name__)


class QueueState:
    """Central queue manager. Thread-safe."""

    def __init__(self):
        self._lock = threading.RLock()
        self.sync: list[dict[str, Any]] = []
        # LOW FIX (audit 5.23 LOW-3): removed five vestigial sub-queue
        # lists (reorg, video, transcribe, redownload, metadata). They
        # were initialized, persisted, loaded, and counted, but no code
        # outside this file ever appended to or popped from them.
        # Redownload / transcribe / comments-refresh / etc. all ride
        # `self.sync` with a `kind="..."` discriminator (see
        # _task_label_sync). Verified by grep across the entire repo.
        self.gpu: list[dict[str, Any]] = []
        self.order: list[list] = [] # [[kind, id], ...]
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

        # Current in-flight items (not yet re-queued, but shown in popover)
        self.current_sync: dict[str, Any] | None = None
        self.current_gpu: dict[str, Any] | None = None
        # LOW FIX (audit 5.23 LOW-3): current_redownload / current_metadata
        # removed alongside their parent lists. No code assigned them, so
        # save_now never populated the corresponding `resuming` keys and
        # requeue_resuming's redownload/metadata branches were unreachable.

        # Sync-pass progress: when "Sync Subbed" runs, we don't enqueue 103
        # individual channel items into `self.sync` — we iterate them
        # inline in `sync_start_all`. But the popover shouldn't look like
        # a single-item queue; the user should see "Downloading ChannelName
        # (17/103)" so they know how far along the pass is. These two
        # fields are set / cleared by sync_start_all.
        self.sync_pass_index: int = 0
        self.sync_pass_total: int = 0

        # Debounced save scheduler
        self._save_timer: threading.Timer | None = None
        # Shortened from 2.0s — a task-killed (Task Manager "End Task")
        # process during the debounce window loses the last queue
        # mutation since SIGTERM doesn't fire on Windows force-kill
        # and atexit is skipped. 0.5s still coalesces normal bursts
        # of enqueue/remove calls (every save_debounced inside a sync
        # iteration lands within ms of each other) but cuts the
        # window-of-loss to a quarter of what it was (audit:
        # main.py:1362).
        self._save_interval_sec = 0.5
        # Save mutex — serializes save_now() so two near-simultaneous
        # debounce-timer fires (the in-progress one + a freshly-scheduled
        # one from a new save_debounced call) can't both write to the
        # same .tmp file and race on os.replace.
        self._save_io_lock = threading.Lock()

        # resuming items pulled from the persisted file
        # (in-flight when the app last shut down). Caller reads via
        # `get_loaded_resuming()` after `load()` to decide how to
        # requeue them. Empty until load() runs.
        self._loaded_resuming: dict[str, Any] = {}

        # Listeners notified on any state change (UI push)
        self._listeners: list[Callable[[], None]] = []

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
            _log.debug("swallowed: %s", e)

    def mark_orphan(self) -> None:
        """Caller-side signal that this QueueState should NOT participate
        in atexit-save. Use when discarding an instance whose load()
        raised (and replacing it with a fresh QueueState)."""
        self._atexit_disabled = True

    # ── listener registration ───────────────────────────────────────

    def get_loaded_resuming(self) -> dict[str, Any]:
        """Patch 1 (v66.5): items that were in-flight when the app
        last shut down. Caller (main.py boot) reads after `load()` to
        decide how to requeue them (typically: append to the tail of
        their respective queues with a "restored" tag). Returns a
        copy; safe to consume."""
        with self._lock:
            return dict(self._loaded_resuming or {})

    def add_listener(self, fn: Callable[[], None]):
        # LOW FIX (audit 5.23 LOW-4): hold _lock around the listener-list
        # mutation. Today listeners are only added once at startup so the
        # race is theoretical, but the rest of QueueState's invariant is
        # "any shared mutable state goes through _lock" — keep this site
        # consistent so a future caller that registers a listener mid-run
        # can't race the snapshot in _notify.
        with self._lock:
            self._listeners.append(fn)

    def _notify(self):
        # Fire listeners on a background thread so the mutation path
        # (sync_enqueue, sync_pop, etc.) doesn't block on
        # window.evaluate_js round-trips. Previously every queue
        # mutation blocked the calling worker thread on Chromium's JS
        # engine — a fast download burst against 50 channels
        # serialized through evaluate_js, slowing throughput and
        # making the UI feel unresponsive. The listeners are
        # idempotent and run on a short-lived daemon thread; ordering
        # is preserved because we snapshot the listener list before
        # dispatching.
        # LOW FIX (audit 5.23 LOW-4): snapshot under _lock to pair with
        # the locked add_listener above. Cheap (single list copy) and
        # closes the theoretical add-during-snapshot race.
        with self._lock:
            snapshot = list(self._listeners)
        if not snapshot:
            return
        def _fire():
            for fn in snapshot:
                try:
                    fn()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
        try:
            threading.Thread(target=_fire, daemon=True,
                             name="queues-notify").start()
        except Exception as e:
            _log.debug("swallowed: %s", e)

    # ── load/save ────────────────────────────────────────────────────

    def load(self) -> bool:
        """Load queue state from ytarchiver_queue.json. Returns True on success.

        _load_queue_state: if the JSON is
        corrupt, rename the file to .bak so next launch starts fresh instead
        of soft-locking on the same parse error every time.
        """
        if not QUEUE_FILE.exists():
            return False
        try:
            with QUEUE_FILE.open("r", encoding="utf-8") as f:
                raw = f.read()
        except OSError:
            return False
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            # Corrupt queue file — rename to .bak so recovery is possible
            # and next launch isn't stuck on the same error forever.
            try:
                bak = str(QUEUE_FILE) + ".bak"
                os.replace(str(QUEUE_FILE), bak)
            except OSError:
                try: os.remove(str(QUEUE_FILE))
                except OSError: pass
            return False
        with self._lock:
            self.sync = list(data.get("sync", []))
            # LOW FIX (audit 5.23 LOW-3): no longer load five dead lists
            # (reorg / video / transcribe / redownload / metadata). If an
            # old queue file on disk still has those keys, they're silently
            # ignored. None of those kinds carried real items in practice
            # — they all ride the sync queue with a `kind=` discriminator.
            self.gpu = list(data.get("gpu", []))
            self.order = list(data.get("order", []))
            self.gpu_paused = bool(data.get("gpu_paused", False))
            self.sync_paused = bool(data.get("sync_paused", False))

            # resuming-dict handling. New-format files
            # (schema_version 2+) keep in-flight items in a separate
            # `resuming` dict; old-format files put them at the front
            # of the regular queue lists. We surface `resuming` so the
            # caller (main.py startup) can emit a restore notice and
            # decide how to requeue.
            schema_v = int(data.get("_schema_version", 1) or 1)
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
                # LOW FIX (audit 5.23 LOW-3): removed redownload / metadata
                # from this loop — those attributes no longer exist and
                # they never carried real items in practice anyway.
                for key in ("sync", "gpu"):
                    lst = getattr(self, key, None)
                    if (lst and isinstance(lst, list)
                            and isinstance(lst[0], dict)
                            and lst[0].get("_in_flight")):
                        self._loaded_resuming[key] = lst.pop(0)
        self._notify()
        return True

    def save_now(self) -> bool:
        """Serialize + atomically replace QUEUE_FILE. Gated by env var."""
        if not config_is_writable():
            return False
        with self._lock:
            # LOW FIX (audit 5.23 LOW-3): payload no longer writes
            # five always-empty arrays (reorg / video / transcribe /
            # redownload / metadata). Saves a few bytes per write and
            # removes a foot-gun: a future contributor reading the
            # JSON file would assume those queues are wired up.
            payload = {
                "sync": copy.deepcopy(self.sync),
                "gpu": copy.deepcopy(self.gpu),
                "order": copy.deepcopy(self.order),
                "gpu_paused": self.gpu_paused,
                "sync_paused": self.sync_paused,
            }
            # in-flight items now persist in a separate
            # `resuming` dict instead of being inserted at the front of
            # the regular queue lists. Old behavior re-popped the same
            # item on next boot and treated it as a normal queued job,
            # which silently re-processed it (partial downloads got
            # re-run, retranscribes silently reverted to regular
            # transcribes). Putting them in `resuming` means load()
            # can emit a visible "restart notice" and requeue them in
            # a controlled way rather than letting them race the fresh
            # boot state.
            resuming: dict[str, Any] = {}
            if self.current_sync is not None:
                resuming["sync"] = copy.deepcopy(self.current_sync)
            # LOW FIX (audit 5.23 LOW-3): removed current_redownload /
            # current_metadata writes — those attributes no longer exist
            # (nothing ever assigned them, so the corresponding resuming
            # keys never landed in practice).
            if self.current_gpu is not None:
                resuming["gpu"] = copy.deepcopy(self.current_gpu)
            if resuming:
                payload["resuming"] = resuming
                payload["_schema_version"] = 2
            # removed the legacy-style front-insertion
            # that was being written ALONGSIDE the `resuming` dict.
            # The duplicate write made every in-flight item appear
            # TWICE on the next launch — once in `resuming` and once
            # at the front of the regular queue list. `load()` would
            # pick one path, and the OTHER would silently leak as a
            # phantom queue item. Now writes are clean: items are in
            # exactly ONE place.
        # Serialize the file-write step so two save_now invocations
        # can't both be partway through writing the same .tmp file.
        with self._save_io_lock:
            try:
                tmp = str(QUEUE_FILE) + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)
                os.replace(tmp, QUEUE_FILE)
                return True
            except OSError:
                return False

    def save_debounced(self):
        """Schedule a save for _save_interval_sec from now (coalesces bursts).

        an atexit hook is registered at construction
        (see __init__) so that if the app is killed or crashes during
        the 2-second debounce window, pending changes still flush to
        disk. Without this, close-during-queue-edit silently lost the
        latest enqueue/remove.

        Each call cancels any pending timer and re-arms a fresh one, so
        a burst of edits coalesces correctly to a single save AFTER the
        burst quiets. Previously the early-return on existing-timer
        meant only the FIRST edit was guaranteed to land — edits that
        arrived within the 2s window weren't persisted until the next
        unrelated debounce trigger or app shutdown.
        """
        with self._lock:
            if self._save_timer is not None:
                try: self._save_timer.cancel()
                except Exception as e: _log.debug("swallowed: %s", e)
                self._save_timer = None
            t = threading.Timer(self._save_interval_sec, self._do_debounced_save)
            t.daemon = True
            t.start()
            self._save_timer = t

    def _do_debounced_save(self):
        with self._lock:
            self._save_timer = None
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
        # Set the disable flag FIRST so any concurrent
        # save_debounced caller can't schedule a fresh timer between
        # our cancel and the save_now (audit: queues H121). The
        # `_save_io_lock` then guards against an already-firing
        # save_now landing after us with stale data.
        try:
            self._atexit_disabled = True
            with self._lock:
                t = self._save_timer
                self._save_timer = None
            if t is not None:
                try: t.cancel()
                except Exception as e: _log.debug("swallowed: %s", e)
            self.save_now()
        except Exception as e:
            _log.debug("swallowed: %s", e)

    # ── sync queue ──────────────────────────────────────────────────

    def sync_enqueue(self, channel: dict[str, Any]) -> bool:
        """Add a channel to the sync queue if not already present.
        Dedupe is keyed on (kind, url) so a "Download X" and a
        separate "Metadata check X" can coexist — they're different
        units of work even though they target the same channel.
        """
        url = channel.get("url", "")
        kind = (channel.get("kind") or "download").lower()
        with self._lock:
            for c in self.sync:
                if (c.get("url") == url and
                        (c.get("kind") or "download").lower() == kind):
                    return False
            self.sync.append(copy.deepcopy(channel))
            self.order.append(["sync", url])
        self._notify()
        self.save_debounced()
        return True

    def sync_pop(self) -> dict[str, Any] | None:
        with self._lock:
            if not self.sync:
                return None
            ch = self.sync.pop(0)
            # remove only the FIRST matching order entry
            # (not all of them). Same URL can legitimately have multiple
            # sync jobs queued (e.g. a Download task and a separate
            # Metadata-recheck task — `sync_enqueue` dedupes on
            # (kind, url), not url alone). Wiping all order entries for
            # that URL dropped the bookkeeping for the OTHER pending
            # job, which then dispatched out of insertion order.
            _u = ch.get("url")
            for _i, _o in enumerate(self.order):
                if _o and _o[0] == "sync" and _o[1] == _u:
                    self.order.pop(_i)
                    break
        self._notify()
        self.save_debounced()
        return ch

    def sync_remove(self, url: str) -> bool:
        """Remove ONE queued sync item matching `url`.

        Two callers (frontend X click + main.py fallback). When the
        same URL appears multiple times in the queue (e.g. a download
        + a metadata refresh for the same channel), only the FIRST
        matching item is removed — the X click is a per-row action,
        not a per-channel sweep.

        For exact-row removal (the common case from the popover X
        click), prefer sync_remove_at(idx, expected_url) which
        identifies the item by position — see logs.js:1077.
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
            # Drop ONE matching order entry (same first-match rule).
            for j, o in enumerate(self.order):
                if o and o[0] == "sync" and o[1] == url:
                    del self.order[j]
                    break
        self._notify()
        self.save_debounced()
        return True

    def sync_remove_at(self, idx: int, expected_url: str = "",
                       expected_name: str = "") -> bool:
        """Remove the queued sync item at exactly `idx`, with an
        identity guard so we don't accidentally delete a different
        item if the queue shifted between paint and click.

        `expected_url` / `expected_name` describe what the caller
        thought was at that slot — we refuse to delete if neither
        matches. Both empty = skip the guard (legacy callers).
        """
        with self._lock:
            if idx < 0 or idx >= len(self.sync):
                return False
            item = self.sync[idx]
            if expected_url or expected_name:
                cur_url = (item.get("url") or "").strip()
                cur_name = (item.get("name")
                            or item.get("folder") or "").strip()
                # Allow either match — frontend may pass whichever
                # field was visible. Refuse only when BOTH disagree.
                _url_ok = (not expected_url) or (cur_url == expected_url)
                _name_ok = (not expected_name) or (cur_name == expected_name)
                if not (_url_ok or _name_ok):
                    return False
            removed_url = (item.get("url") or "").strip()
            del self.sync[idx]
            # Drop the matching order entry (first one with this URL).
            if removed_url:
                for j, o in enumerate(self.order):
                    if o and o[0] == "sync" and o[1] == removed_url:
                        del self.order[j]
                        break
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
            removed_url = (self.sync[target_idx].get("url") or "").strip()
            del self.sync[target_idx]
            if removed_url:
                for j, o in enumerate(self.order):
                    if o and o[0] == "sync" and o[1] == removed_url:
                        del self.order[j]
                        break
        self._notify()
        self.save_debounced()
        return True

    def sync_requeue_front(self, channel: dict[str, Any]) -> None:
        """Insert `channel` at the front of the sync queue atomically.
        Used by sync_all on a pause-interrupted channel so Resume picks
        the in-flight channel back up first. Replaces a bare
        `queues.sync.insert(0, ch); queues._notify()` pair that bypassed
        `_lock`, racing with concurrent `sync_pop` / `sync_remove` /
        `sync_enqueue` callers (audit: sync/sync_all.py C7).
        """
        url = channel.get("url", "")
        with self._lock:
            self.sync.insert(0, copy.deepcopy(channel))
            self.order.insert(0, ["sync", url])
        self._notify()
        self.save_debounced()

    def sync_clear(self) -> int:
        """Remove every queued sync task; keep the currently-running one.
        Returns the number of queued items removed."""
        with self._lock:
            removed = len(self.sync)
            self.sync = []
            self.order = [o for o in self.order if not (o and o[0] == "sync")]
        if removed:
            self._notify()
            self.save_debounced()
        return removed

    def gpu_clear(self) -> int:
        """Remove every queued GPU task; keep the currently-running one."""
        with self._lock:
            removed = len(self.gpu)
            self.gpu = []
            self.order = [o for o in self.order if not (o and o[0] == "gpu")]
        if removed:
            self._notify()
            self.save_debounced()
        return removed

    def sync_reorder(self, url: str, new_index: int) -> bool:
        with self._lock:
            idx = next((i for i, c in enumerate(self.sync) if c.get("url") == url), -1)
            if idx < 0 or new_index < 0 or new_index >= len(self.sync):
                return False
            item = self.sync.pop(idx)
            self.sync.insert(new_index, item)
        self._notify()
        self.save_debounced()
        return True

    # ── gpu queue ───────────────────────────────────────────────────

    def gpu_enqueue(self, item: dict[str, Any]) -> bool:
        """Queue a transcription/encode job for the GPU lane. Dedupes
        by `path` to prevent double-entries on startup when both
        QueueState.load() (which restores gpu from disk) and the
        transcribe pending-journal recovery might try to add the same
        item. Returns True if the item was added, False if a duplicate
        was already present."""
        path = (item.get("path") or "").strip()
        with self._lock:
            if path:
                for existing in self.gpu:
                    if (existing.get("path") or "").strip() == path:
                        return False
            self.gpu.append(copy.deepcopy(item))
        self._notify()
        self.save_debounced()
        return True

    def gpu_pop(self) -> dict[str, Any] | None:
        with self._lock:
            if not self.gpu:
                return None
            it = self.gpu.pop(0)
        self._notify()
        self.save_debounced()
        return it

    def gpu_remove(self, task_id: str) -> bool:
        """Remove ONE queued GPU item matching `task_id` (id or path).
        First-match semantics — when the same path appears twice the
        X click only drops the one the user clicked. For exact-row
        removal, prefer gpu_remove_at(idx, expected_path)."""
        with self._lock:
            target_idx = -1
            for i, item in enumerate(self.gpu):
                if (item.get("id") or item.get("path")) == task_id:
                    target_idx = i
                    break
            if target_idx < 0:
                return False
            del self.gpu[target_idx]
        self._notify()
        self.save_debounced()
        return True

    def gpu_remove_at(self, idx: int, expected_path: str = "",
                      expected_bulk_id: str = "") -> bool:
        """Remove the queued GPU item at exactly `idx`, with an
        identity guard. See sync_remove_at — same idea."""
        with self._lock:
            if idx < 0 or idx >= len(self.gpu):
                return False
            item = self.gpu[idx]
            if expected_path or expected_bulk_id:
                cur_path = (item.get("path") or "").strip()
                cur_bulk = str(item.get("bulk_id") or "").strip()
                _path_ok = (not expected_path) or (cur_path == expected_path)
                _bulk_ok = (not expected_bulk_id) or (cur_bulk == expected_bulk_id)
                if not (_path_ok or _bulk_ok):
                    return False
            del self.gpu[idx]
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

    def gpu_reorder(self, task_id: str, new_index: int) -> bool:
        with self._lock:
            idx = next((i for i, t in enumerate(self.gpu)
                        if (t.get("id") or t.get("path")) == task_id), -1)
            if idx < 0 or new_index < 0 or new_index >= len(self.gpu):
                return False
            item = self.gpu.pop(idx)
            self.gpu.insert(new_index, item)
        self._notify()
        self.save_debounced()
        return True

    # ── current-task tracking ───────────────────────────────────────

    def set_current_sync(self, ch: dict[str, Any] | None):
        with self._lock:
            self.current_sync = copy.deepcopy(ch) if ch else None
        self._notify()
        # Persist immediately. Force-kill (Windows "End Task", power
        # loss) doesn't run atexit, and a 0.5s debounce can lose this
        # transition — meaning the "resuming" dict on next launch
        # misses the in-flight channel (audit: queues H106). The
        # transition rate is low (one per channel start/end), so the
        # extra disk write is negligible.
        try:
            self.save_now()
        except Exception:
            self.save_debounced()

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
        with self._lock:
            self.current_gpu = copy.deepcopy(item) if item else None
        self._notify()

    # ── UI payload ──────────────────────────────────────────────────

    def to_ui_payload(self) -> dict[str, Any]:
        """Return the shape the queue popovers expect (see web/logs.js renderQueues)."""
        with self._lock:
            sync_list = []
            if self.current_sync:
                # When a Sync-Subbed pass is running, decorate the active
                # channel label with "(N/total)" so the popover shows
                # progress through the pass. Outside of a pass, we just
                # render the channel name plain.
                label = self._task_label_sync(self.current_sync, running=True)
                if self.sync_pass_total > 0 and self.sync_pass_index > 0:
                    label = f"{label} ({self.sync_pass_index}/{self.sync_pass_total})"
                sync_list.append({
                    "name": label,
                    "status": "running",
                    # Identifiers used by the right-click "Remove from
                    # queue" context menu → api.queues_sync_remove.
                    # Without these the JS fell back to the display name
                    # which didn't match the backend's URL-keyed removal.
                    "url": (self.current_sync.get("url") or "").strip(),
                    "channel_name": (self.current_sync.get("name")
                                      or self.current_sync.get("folder")
                                      or "").strip(),
                })
            for ch in self.sync:
                sync_list.append({
                    "name": self._task_label_sync(ch, running=False),
                    "status": "queued",
                    "url": (ch.get("url") or "").strip(),
                    "channel_name": (ch.get("name")
                                      or ch.get("folder") or "").strip(),
                })

            gpu_list = []
            # Track which bulk_ids are being represented by the running
            # item so the still-queued remainder from that bulk collapses
            # into a single "Transcribe {ch} (N more)" row.
            running_bulk_id = ""
            if self.current_gpu:
                running_bulk_id = str(self.current_gpu.get("bulk_id") or "")
                gpu_list.append({
                    "name": self._task_label_gpu(self.current_gpu, running=True,
                                                 bulk_context=None),
                    "status": "running",
                    "path": (self.current_gpu.get("path") or "").strip(),
                    "bulk_id": running_bulk_id,
                })
            # Coalesce queued items by bulk_id. First pass: count items per
            # bulk_id. Second pass: emit one row per bulk (or per-item if
            # no bulk_id).
            bulk_counts: dict[str, int] = {}
            bulk_channels: dict[str, str] = {}
            for t in self.gpu:
                bid = str(t.get("bulk_id") or "")
                if bid:
                    bulk_counts[bid] = bulk_counts.get(bid, 0) + 1
                    if bid not in bulk_channels:
                        bulk_channels[bid] = (t.get("channel") or "").strip()
            seen_bulks: set = set()
            for t in self.gpu:
                bid = str(t.get("bulk_id") or "")
                if bid and bid in seen_bulks:
                    continue
                if bid and bulk_counts.get(bid, 0) > 1:
                    # Emit one condensed row for the whole bulk.
                    ch_name = bulk_channels.get(bid) or (t.get("channel") or "?")
                    remaining = bulk_counts[bid]
                    # If part of this bulk is already the "running" slot,
                    # the queued remainder is one short of bulk_total.
                    if bid == running_bulk_id:
                        label = f"Transcribe {ch_name} ({remaining} more)"
                    else:
                        label = f"Transcribe {ch_name} ({remaining} videos)"
                    gpu_list.append({
                        "name": label,
                        "status": "queued",
                        "bulk_id": bid,
                        "bulk_count": remaining,
                    })
                    seen_bulks.add(bid)
                else:
                    gpu_list.append({
                        "name": self._task_label_gpu(t, running=False,
                                                     bulk_context=None),
                        "status": "queued",
                        "path": (t.get("path") or "").strip(),
                        "bulk_id": str(t.get("bulk_id") or ""),
                    })
            return {
                "sync": sync_list,
                "gpu": gpu_list,
                "gpu_paused": self.gpu_paused,
                "sync_paused": self.sync_paused,
                # Pause-pending vs pause-active distinction so the UI
                # can blink the Resume button between "user clicked
                # pause" and "worker actually entered pause-wait".
                "gpu_paused_active": self.gpu_paused_active,
                "sync_paused_active": self.sync_paused_active,
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

    def set_gpu_paused(self, paused: bool):
        with self._lock:
            old_paused = self.gpu_paused
            self.gpu_paused = bool(paused)
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
        # LOW FIX (audit 5.23 LOW-3): trimmed redownload/metadata/reorg/
        # transcribe/video keys. Those lists no longer exist (see __init__
        # comment) and were always zero. No production caller reads this
        # method today (grep showed zero hits) but keep sync + gpu around
        # in case a future caller does.
        with self._lock:
            return {
                "sync": len(self.sync) + (1 if self.current_sync else 0),
                "gpu": len(self.gpu) + (1 if self.current_gpu else 0),
            }

    # ── restore-on-launch helpers ───────────────────────────────────
    def has_sync_pipeline_items(self) -> bool:
        """True if the sync queue has items.
        Used after load() to decide whether to force-pause (Project rule: launching with items in queue must never auto-start).

        LOW FIX (audit 5.23 LOW-3): used to OR-check five other queue
        lists; those are gone now, so this collapses to just self.sync.
        """
        with self._lock:
            return bool(self.sync)

    def has_gpu_items(self) -> bool:
        with self._lock:
            return bool(self.gpu)
