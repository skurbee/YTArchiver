"""
QueueMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. QueueMixin is the first staged AppServices migration
slice: methods prefer `self.services.*` and keep private-attribute
fallbacks only for partial test doubles / transitional callers.
"""
from __future__ import annotations

import time

from backend.ytarchiver_config import load_config, save_config, update_config

from ._shared import _log


class QueueMixin:
    def _queue_services(self):
        return getattr(self, "services", None)

    def _queue_state(self):
        services = self._queue_services()
        q = getattr(services, "queues", None) if services is not None else None
        return q if q is not None else self._queues

    def _queue_transcribe(self):
        services = self._queue_services()
        tx = getattr(services, "transcribe", None) if services is not None else None
        return tx if tx is not None else self._transcribe

    def _queue_log_stream(self):
        services = self._queue_services()
        stream = (getattr(services, "log_stream", None)
                  if services is not None else None)
        return stream if stream is not None else self._log_stream

    def _queue_config(self):
        cfg = getattr(self, "_config", None)
        if cfg is not None:
            return cfg
        services = self._queue_services()
        if services is not None:
            return services.fresh_config()
        return load_config()

    def _queue_save_config(self, cfg):
        services = self._queue_services()
        if services is not None:
            return services.save_config(cfg)
        return save_config(cfg)

    def _queue_update_config(self, mutator):
        """Commit one queue preference without saving a stale snapshot."""
        services = self._queue_services()
        mutate = (getattr(services, "mutate_config", None)
                  if services is not None else None)
        if callable(mutate):
            return mutate(mutator)
        return update_config(mutator)

    def _queue_gpu_count(self):
        queues = self._queue_state()
        try:
            counts = queues.counts()
            return max(0, int(counts.get("gpu") or 0))
        except Exception:
            pass
        try:
            return max(0, len(getattr(queues, "gpu", []) or []))
        except Exception:
            return 0

    @staticmethod
    def _queue_task_count_label(count):
        if count <= 0:
            return "the queue"
        suffix = "" if count == 1 else "s"
        return f"{count:,} queued task{suffix}"

    def get_queues(self):
        """Return the real live queue state — empty list when nothing's queued.

        Earlier builds returned synthetic sample rows for "preview feel" when
        both queues were empty; that was a Phase-0 placeholder that made the
        user see unclearable fake items. Removed — if it's empty, show empty.
        """
        return self._queue_state().to_ui_payload()


    def queue_auto_get(self):
        """Return current state of the Sync + GPU queue "Auto" checkboxes.
        When Auto is on, adding an item to an empty queue auto-starts it.
        Mirrors YTArchiver.py config keys autorun_gpu + autorun_sync.
        """
        cfg = self._queue_config()
        return {
            "sync": bool(cfg.get("autorun_sync", False)),
            "gpu": bool(cfg.get("autorun_gpu", False)),
        }


    def queue_auto_set(self, kind, enabled):
        """Persist the Auto checkbox state for sync/gpu queue.
        `kind` must be "sync" or "gpu".
        For GPU, also wake the transcribe worker when toggled ON so
        any queued-but-parked jobs actually fire (the worker was
        sleeping on the `_auto_enabled()` gate — it needs a nudge to
        re-check). Matches rule: unchecking Auto keeps incoming
        tasks parked; re-checking releases them.
        """
        if kind not in ("sync", "gpu"):
            return {"ok": False, "error": "kind must be sync or gpu"}
        key = "autorun_gpu" if kind == "gpu" else "autorun_sync"
        try:
            self._queue_update_config(
                lambda live: live.__setitem__(key, bool(enabled)))
            if getattr(self, "_config", None) is not None:
                self._config[key] = bool(enabled)
            if kind == "gpu" and enabled:
                # Kick the worker in case it was parked on the Auto
                # gate AND there are jobs sitting in the internal list.
                try: self._queue_transcribe()._ensure_worker()
                except Exception as e: _log.debug("swallowed: %s", e)
                # push the updated queue state to the UI so the
                # Start/Pause button flips to the correct rendered state
                # immediately. Sync path does this via sync_start_all
                # (→ _on_queue_changed); GPU path was missing the push.
                try: self._on_queue_changed()
                except Exception as e: _log.debug("swallowed: %s", e)
                try:
                    self._queue_log_stream().emit_text(
                        " - Auto-processing enabled — queue will drain.",
                        "simpleline_green")
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            elif kind == "gpu" and not enabled:
                # emit an unambiguous log line when the user
                # disables GPU Auto mid-sync so they understand the
                # behavior — the in-flight task will complete, then new
                # GPU processing jobs will sit in the queue until they
                # re-enable Auto or click Start in the GPU Tasks popover.
                try:
                    self._queue_log_stream().emit_text(
                        " - Auto-processing disabled — new GPU tasks will queue "
                        "instead of starting automatically. (In-flight task finishes first.)",
                        "simpleline_blue")
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                try: self._on_queue_changed()
                except Exception as e: _log.debug("swallowed: %s", e)
            elif kind == "sync" and enabled:
                # Symmetric with GPU: if the user toggles Auto ON and
                # the sync queue has items (e.g., they clicked Sync
                # Subbed with Auto off, then changed their mind),
                # spin up the worker so the queue actually drains.
                # Without this, the enqueued tasks would sit idle
                # until the user clicked Start in the popover.
                try:
                    has_items = bool(self._queue_state().sync)
                    if has_items and not self.sync_is_running():
                        self.sync_start_all(add_downloads_from_config=False)
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            return {"ok": True, "enabled": bool(enabled)}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    # ─── Queue mutations (right-click menu) ────────────────────────────

    def queues_sync_remove(self, task_id):
        """Remove one pending sync item by opaque task ID only."""
        ident = str(task_id or "").strip()
        if not ident:
            return {"ok": False, "error": "task_id required"}
        queues = self._queue_state()
        queued = next(
            (item for item in queues.sync_snapshot()
             if str(item.get("task_id") or "").strip() == ident),
            None,
        )
        if queued and (queued.get("kind") or "").lower() == "redownload":
            ok = self._remove_pending_redownload_exact(ident, queues)
        else:
            ok = queues.sync_remove_task(ident, durable=True)
        self._on_queue_changed()
        return {"ok": ok}


    def queues_sync_remove_at(self, idx, expected_url="", expected_name=""):
        """Reject stale index/URL mutations from pre-ID frontends."""
        return {"ok": False,
                "error": "Queue changed; refresh and retry with task_id"}


    def _drop_pending_jobs(self, predicate):
        """Mirror a popover X-click into the TranscribeManager's `_jobs`
        list so the user-removed item doesn't get popped + re-displayed
        as the active task when the worker's turn comes for it."""
        try:
            return self._queue_transcribe().remove_pending_jobs(predicate)
        except Exception as e:
            _log.debug("swallowed: %s", e)
            return 0

    def _remove_pending_redownload_exact(self, task_id, queues):
        """Commit one redownload removal in both runtime and durable queues."""
        lock = getattr(self, "_redwnl_lock", None)
        pending = getattr(self, "_redwnl_pending", None)
        if lock is None or not isinstance(pending, list):
            return queues.sync_remove_task(task_id, durable=True)
        with lock:
            index = next(
                (i for i, item in enumerate(pending)
                 if str(((item or {}).get("rd_task") or {}).get("task_id")
                        or "").strip() == task_id),
                -1,
            )
            if not queues.sync_remove_task(task_id, durable=True):
                return False
            if index >= 0:
                pending.pop(index)
            return True

    def _reorder_pending_redownload_exact(
            self, task_id, new_index, queues) -> bool:
        """Keep the redownload chain's relative order equal to QueueState."""
        lock = getattr(self, "_redwnl_lock", None)
        pending = getattr(self, "_redwnl_pending", None)
        if lock is None or not isinstance(pending, list):
            return queues.sync_reorder(task_id, new_index, durable=True)
        with lock:
            queue_order = queues.sync_snapshot()
            source_index = next(
                (i for i, item in enumerate(queue_order)
                 if str(item.get("task_id") or "").strip() == task_id),
                -1,
            )
            if (source_index < 0 or new_index < 0
                    or new_index >= len(queue_order)):
                return False
            moved = queue_order.pop(source_index)
            queue_order.insert(new_index, moved)
            if not queues.sync_reorder(task_id, new_index, durable=True):
                return False
            by_id = {
                str(((item or {}).get("rd_task") or {}).get("task_id")
                    or "").strip(): item
                for item in pending
                if str(((item or {}).get("rd_task") or {}).get("task_id")
                       or "").strip()
            }
            ordered_ids = [
                str(item.get("task_id") or "").strip()
                for item in queue_order
                if (item.get("kind") or "").lower() == "redownload"
                and str(item.get("task_id") or "").strip() in by_id
            ]
            reordered = [by_id[ident] for ident in ordered_ids]
            reordered.extend(
                item for item in pending if item not in reordered)
            pending[:] = reordered
            return True


    def queues_gpu_remove(self, task_id):
        """Remove one pending Processing job by opaque task ID only."""
        ident = str(task_id or "").strip()
        if not ident:
            return {"ok": False, "error": "task_id required"}
        queues = self._queue_state()
        entries = queues.gpu_items_for_ids({ident})
        manager = self._queue_transcribe()
        coordinate = getattr(
            manager, "remove_pending_task_ids_coordinated", None)
        if not callable(coordinate):
            return {"ok": False,
                    "error": "Processing queue coordinator unavailable"}
        ok = coordinate(
            {ident},
            lambda: queues.gpu_remove(ident, durable=True),
            lambda: queues.gpu_restore_items(entries, durable=True),
        )
        self._on_queue_changed()
        return ({"ok": True} if ok else
                {"ok": False, "error": "Task removal could not be saved"})


    def queues_gpu_remove_at(self, idx, expected_path="", expected_bulk_id=""):
        """Reject stale index/path mutations from pre-ID frontends."""
        return {"ok": False,
                "error": "Queue changed; refresh and retry with task_id"}


    def queues_gpu_remove_bulk(self, bulk_id):
        """Reject ambiguous bulk-label removal; grouped rows send exact IDs."""
        return {"ok": False,
                "error": "Grouped removal requires exact task_ids"}


    def queues_gpu_remove_many(self, task_ids):
        """Remove the exact Processing tasks represented by a grouped row."""
        if not isinstance(task_ids, list):
            return {"ok": False, "error": "task_ids must be a list"}
        wanted = [str(task_id or "").strip() for task_id in task_ids]
        wanted = [task_id for task_id in wanted if task_id]
        if not wanted or len(set(wanted)) != len(wanted):
            return {"ok": False, "error": "unique task_ids required"}
        queues = self._queue_state()
        wanted_set = set(wanted)
        entries = queues.gpu_items_for_ids(wanted_set)
        manager = self._queue_transcribe()
        coordinate = getattr(
            manager, "remove_pending_task_ids_coordinated", None)
        if not callable(coordinate):
            return {"ok": False,
                    "error": "Processing queue coordinator unavailable"}
        removed: list[str] = []

        def _remove_exact() -> bool:
            nonlocal removed
            removed = queues.gpu_remove_tasks(
                wanted, durable=True, require_all=True)
            return set(removed) == wanted_set

        ok = coordinate(
            wanted_set, _remove_exact,
            lambda: queues.gpu_restore_items(entries, durable=True),
        )
        self._on_queue_changed()
        return {"ok": ok, "dropped": len(removed) if ok else 0,
                "task_ids": removed if ok else [],
                **({} if ok else
                   {"error": "Grouped task removal could not be saved"})}


    def queues_sync_reorder(self, task_id, new_index):
        # Reject None/missing new_index explicitly — `int(None or 0)`
        # silently defaulted to index 0, sending unrelated drops to
        # the top of the queue (audit: queue_mixin L11).
        if new_index is None:
            return {"ok": False, "error": "new_index required"}
        try:
            _idx = int(new_index)
        except (TypeError, ValueError):
            return {"ok": False, "error": f"Invalid new_index: {new_index!r}"}
        ident = str(task_id or "").strip()
        if not ident:
            return {"ok": False, "error": "task_id required"}
        queues = self._queue_state()
        queued = next(
            (item for item in queues.sync_snapshot()
             if str(item.get("task_id") or "").strip() == ident),
            None,
        )
        if queued and (queued.get("kind") or "").lower() == "redownload":
            ok = self._reorder_pending_redownload_exact(
                ident, _idx, queues)
        else:
            ok = queues.sync_reorder(ident, _idx, durable=True)
        self._on_queue_changed()
        return {"ok": ok}


    def queues_gpu_reorder(self, task_id, new_index):
        if new_index is None:
            return {"ok": False, "error": "new_index required"}
        try:
            _idx = int(new_index)
        except (TypeError, ValueError):
            return {"ok": False, "error": f"Invalid new_index: {new_index!r}"}
        ident = str(task_id or "").strip()
        if not ident:
            return {"ok": False, "error": "task_id required"}
        queues = self._queue_state()
        snapshot = queues.gpu_snapshot()
        old_index = next(
            (i for i, item in enumerate(snapshot)
             if str(item.get("task_id") or "").strip() == ident),
            -1,
        )
        if old_index < 0:
            return {"ok": False, "error": "Queue changed; task not found"}
        manager = self._queue_transcribe()
        coordinate = getattr(
            manager, "reorder_pending_task_coordinated", None)
        if not callable(coordinate):
            return {"ok": False,
                    "error": "Processing queue coordinator unavailable"}
        ok = coordinate(
            ident, _idx,
            lambda: queues.gpu_reorder(ident, _idx, durable=True),
            lambda: queues.gpu_reorder(ident, old_index, durable=True),
        )
        self._on_queue_changed()
        return ({"ok": True} if ok else
                {"ok": False, "error": "Task order could not be saved"})


    # ─── Global pause / resume / skip (both queues) ────────────────────

    def queue_pause(self, which="both"):
        """Pause the sync queue, GPU queue, or both (`which` in:
        'sync' | 'gpu' | 'both'). Persisted to queue state."""
        queues = self._queue_state()
        if which in ("sync", "both"):
            self._sync_pause.set()
            queues.set_sync_paused(True)
            # Pausing is local state only. Do not spend a YouTube request to
            # verify a session when the user is explicitly stopping work.
        if which in ("gpu", "both"):
            queues.set_gpu_paused(True)
            # The TranscribeManager worker (transcribe + compress jobs) is the
            # GPU lane — only pause it for gpu/both, NOT for a sync-only pause,
            # so the two popover Pause buttons are independent (audit r2: a
            # sync resume was secretly un-pausing a deliberately-paused GPU).
            self._queue_transcribe().pause()
        # A global Pause while sync is active is announced by sync_all when
        # the current channel actually reaches its pause point.  Emitting the
        # Processing notice here as well produced two adjacent pause lines for
        # one click.  Keep immediate feedback for GPU-only pauses (and for a
        # global pause when no sync worker exists), but let the active sync own
        # the single combined log announcement otherwise.
        _sync_running = False
        try:
            _is_running = getattr(self, "sync_is_running", None)
            if callable(_is_running):
                _sync_running = bool(_is_running())
        except Exception:
            _sync_running = False
        if which == "gpu" or (which == "both" and not _sync_running):
            try:
                from backend.pause_helpers import emit_paused
                emit_paused(
                    self._queue_log_stream(),
                    label="Processing queue — current job will finish",
                )
            except Exception as e:
                _log.debug("swallowed: %s", e)
        self._on_queue_changed()
        return {"ok": True, "paused": which}


    def youtube_traffic_override(self):
        """Force the current sync past configured rolling traffic ceilings."""
        try:
            from backend import youtube_traffic
            waiting = youtube_traffic.wait_status()
            if not waiting.get("active"):
                self._on_queue_changed()
                return {
                    "ok": False,
                    "error": "The sync is no longer waiting for a traffic slot.",
                }
            result = youtube_traffic.override_budget_limits()
            try:
                self._queue_log_stream().emit([[
                    "\u25b6 YouTube traffic safety override enabled for this "
                    "sync pass. Hourly and 24-hour ceilings will be ignored; "
                    "emergency rate-limit protection remains active.\n",
                    "simpleline",
                ]])
            except Exception as e:
                _log.debug("traffic override log failed: %s", e)
            self._on_queue_changed()
            return result
        except Exception as e:
            _log.warning("traffic override failed: %s", e)
            return {"ok": False, "error": str(e)}


    def queue_resume(self, which="both"):
        """Resume a paused queue."""
        queues = self._queue_state()
        gpu_count = self._queue_gpu_count() if which in ("gpu", "both") else 0
        if which in ("sync", "both"):
            try:
                from backend import youtube_traffic
                circuit = youtube_traffic.circuit_state()
                if circuit.get("active"):
                    resume = time.strftime(
                        "%I:%M%p",
                        time.localtime(float(circuit["cooldown_until"])),
                    ).lstrip("0").lower()
                    self._on_queue_changed()
                    return {
                        "ok": False,
                        "paused": True,
                        "rate_limited": True,
                        "error": (
                            "YouTube rate-limit cooldown is active until "
                            f"{resume}."),
                    }
            except Exception as e:
                _log.debug("resume rate-limit check failed: %s", e)
            # Never release queued YouTube work into a known-expired Firefox
            # session.  A successful re-check also re-arms the cookie alarm
            # for a future expiry.
            try:
                from backend.youtube_session import (
                    check_configured_cookie_session,
                    reset_rate_limit_alert,
                )
                if not check_configured_cookie_session(context="Resume"):
                    self._on_queue_changed()
                    return {
                        "ok": False,
                        "paused": True,
                        "error": "Firefox YouTube sign-in expired",
                    }
                reset_rate_limit_alert()
            except Exception as e:
                _log.debug("resume YouTube-session check failed: %s", e)
            self._sync_pause.clear()
            queues.set_sync_paused(False)
        if which in ("gpu", "both"):
            queues.set_gpu_paused(False)
            # request_drain() (not plain resume()) so a backlog accumulated
            # while Auto is off actually drains. resume() only clears the
            # pause Event; the worker would immediately re-park on the Auto
            # gate. request_drain() arms a one-shot drain that empties the
            # backlog regardless, then re-parks.
            if not self._queue_transcribe().request_drain():
                # Restore the painted pause state. request_drain fails before
                # starting work when QueueState/journal reconciliation cannot
                # commit, so reporting success here would leave a green toast
                # over a queue that never moved.
                queues.set_gpu_paused(True)
                self._queue_transcribe().pause()
                if which == "both":
                    self._sync_pause.set()
                    queues.set_sync_paused(True)
                self._on_queue_changed()
                return {
                    "ok": False,
                    "paused": True,
                    "error": "Processing recovery state could not be saved",
                }
            try:
                self._queue_log_stream().emit_text(
                    " - Processing queue resumed - draining "
                    f"{self._queue_task_count_label(gpu_count)}.",
                    "simpleline_green")
            except Exception as e:
                _log.debug("swallowed: %s", e)
        self._on_queue_changed()
        return {"ok": True, "paused": False}


    def queue_is_paused(self):
        """Return current paused state for each queue."""
        queues = self._queue_state()
        return {
            "sync": bool(queues.sync_paused),
            "gpu": bool(queues.gpu_paused),
        }


    def gpu_start(self, which="gpu"):
        """One-shot 'Start' for the Processing queue: drain the backlog now
        even though Auto is off, WITHOUT re-enabling Auto. Auto stays the
        user's manual gate — new arrivals keep queuing until they click Start
        again. Also clears any lingering pause so a paused-at-launch queue
        starts cleanly. (`which` accepted for call-signature symmetry with
        queue_pause/queue_resume; the Processing queue is the only drainable
        one.)"""
        queues = self._queue_state()
        gpu_count = self._queue_gpu_count()
        try:
            started = self._queue_transcribe().request_drain()
        except Exception as e:
            return {"ok": False, "error": str(e)}
        if not started:
            queues.set_gpu_paused(True)
            self._queue_transcribe().pause()
            self._on_queue_changed()
            return {"ok": False,
                    "error": "Processing recovery state could not be saved"}
        queues.set_gpu_paused(False)
        try:
            self._on_queue_changed()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        try:
            self._queue_log_stream().emit_text(
                " - Processing started - draining "
                f"{self._queue_task_count_label(gpu_count)}. "
                "Auto stays off.",
                "simpleline_green")
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return {"ok": True}
