"""
QueueMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class QueueMixin:

    def get_queues(self):
        """Return the real live queue state — empty list when nothing's queued.

        Earlier builds returned synthetic sample rows for "preview feel" when
        both queues were empty; that was a Phase-0 placeholder that made the
        user see unclearable fake items. Removed — if it's empty, show empty.
        """
        return self._queues.to_ui_payload()


    def queue_auto_get(self):
        """Return current state of the Sync + GPU queue "Auto" checkboxes.
        When Auto is on, adding an item to an empty queue auto-starts it.
        Mirrors YTArchiver.py config keys autorun_gpu + autorun_sync.
        """
        cfg = self._config or load_config()
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
            from backend.ytarchiver_config import save_config as _sc
            cfg = load_config()
            cfg[key] = bool(enabled)
            if not _sc(cfg):
                return {"ok": False, "error": "Config write failed (write-gate off?)"}
            if self._config is not None:
                self._config[key] = bool(enabled)
            if kind == "gpu" and enabled:
                # Kick the worker in case it was parked on the Auto
                # gate AND there are jobs sitting in the internal list.
                try: self._transcribe._ensure_worker()
                except Exception as e: _log.debug("swallowed: %s", e)
                # push the updated queue state to the UI so the
                # Start/Pause button flips to the correct rendered state
                # immediately. Sync path does this via sync_start_all
                # (→ _on_queue_changed); GPU path was missing the push.
                try: self._on_queue_changed()
                except Exception as e: _log.debug("swallowed: %s", e)
                try:
                    self._log_stream.emit_text(
                        " - GPU Auto enabled — queue will drain.",
                        "simpleline_green")
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            elif kind == "gpu" and not enabled:
                # emit an unambiguous log line when the user
                # disables GPU Auto mid-sync so they understand the
                # behavior — the in-flight transcription will complete,
                # then new arrivals will sit in the queue until they
                # re-enable Auto or click Start in the GPU Tasks popover.
                try:
                    self._log_stream.emit_text(
                        " - GPU Auto disabled — incoming transcriptions "
                        "will queue. (In-flight job finishes first.)",
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
                    has_items = bool(self._queues.sync)
                    if has_items and not self.sync_is_running():
                        self.sync_start_all(add_downloads_from_config=False)
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            return {"ok": True, "enabled": bool(enabled)}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    # ─── Queue mutations (right-click menu) ────────────────────────────

    def queues_sync_remove(self, identifier):
        """Remove ONE pending sync item by URL or channel name.
        First-match semantics — when the queue has duplicates (same
        channel queued for download AND metadata refresh), only the
        first one drops. The popover X-click should prefer
        `queues_sync_remove_at` (index-based with identity guard);
        this method is a fallback for callers without an index."""
        ident = str(identifier or "").strip()
        ok = self._queues.sync_remove(ident)
        if not ok and ident:
            # Name / folder fallback — first match only. Routed
            # through QueueState's public sync_remove_by_name() so the
            # _lock/_notify/save_debounced invariants are honored
            # inside the class instead of bypassing encapsulation
            # (audit: queue_mixin H5).
            ok = self._queues.sync_remove_by_name(ident)
        self._on_queue_changed()
        return {"ok": ok}


    def queues_sync_remove_at(self, idx, expected_url="", expected_name=""):
        """Remove the sync queue item at exactly `idx` (the row index
        the user actually clicked X on in the popover). The optional
        identity hints prevent deleting the wrong item if the queue
        shifted between paint and click."""
        try:
            i = int(idx)
        except (TypeError, ValueError):
            return {"ok": False, "error": "bad index"}
        ok = self._queues.sync_remove_at(
            i,
            expected_url=str(expected_url or "").strip(),
            expected_name=str(expected_name or "").strip(),
        )
        self._on_queue_changed()
        return {"ok": ok}


    def _drop_pending_jobs(self, predicate):
        """Mirror a popover X-click into the TranscribeManager's `_jobs`
        list so the user-removed item doesn't get popped + re-displayed
        as the active task when the worker's turn comes for it."""
        try:
            self._transcribe.remove_pending_jobs(predicate)
        except Exception as e:
            _log.debug("swallowed: %s", e)


    def queues_gpu_remove(self, identifier):
        """Remove ONE pending GPU job by path (preferred) or bulk_id.
        First-match semantics. The popover X-click should prefer
        `queues_gpu_remove_at` (index-based with identity guard)."""
        ident = str(identifier or "").strip()
        if not ident:
            return {"ok": False}
        ok = self._queues.gpu_remove(ident)
        if ok:
            # Single removal — match by path (or id when path absent).
            self._drop_pending_jobs(
                lambda j, p=ident: (j.get("path") or "") == p
                or (j.get("id") or "") == p)
        else:
            # Fallback: treat as bulk_id.
            dropped = self._queues.gpu_remove_bulk(ident)
            ok = dropped > 0
            if ok:
                self._drop_pending_jobs(
                    lambda j, b=ident: str(j.get("bulk_id") or "") == b)
        self._on_queue_changed()
        return {"ok": ok}


    def queues_gpu_remove_at(self, idx, expected_path="", expected_bulk_id=""):
        """Remove the GPU queue item at exactly `idx` (the row index
        the user actually clicked X on). For coalesced "Transcribe X
        (N videos)" rows the popover should call queues_gpu_remove_bulk
        instead — this drops a single slot."""
        try:
            i = int(idx)
        except (TypeError, ValueError):
            return {"ok": False, "error": "bad index"}
        ep = str(expected_path or "").strip()
        eb = str(expected_bulk_id or "").strip()
        ok = self._queues.gpu_remove_at(i, expected_path=ep, expected_bulk_id=eb)
        if ok:
            if ep:
                self._drop_pending_jobs(
                    lambda j, p=ep: (j.get("path") or "") == p)
            elif eb:
                self._drop_pending_jobs(
                    lambda j, b=eb: str(j.get("bulk_id") or "") == b)
        self._on_queue_changed()
        return {"ok": ok}


    def queues_gpu_remove_bulk(self, bulk_id):
        """Drop every GPU job with a matching `bulk_id` (coalesced row
        removal). Called from the queue-popover context menu when the
        user removes a "Transcribe {ch} (N videos)" row."""
        bid = str(bulk_id or "")
        dropped = self._queues.gpu_remove_bulk(bid)
        if dropped > 0:
            self._drop_pending_jobs(
                lambda j, b=bid: str(j.get("bulk_id") or "") == b)
        self._on_queue_changed()
        return {"ok": dropped > 0, "dropped": dropped}


    def queues_sync_reorder(self, identifier, new_index):
        # Reject None/missing new_index explicitly — `int(None or 0)`
        # silently defaulted to index 0, sending unrelated drops to
        # the top of the queue (audit: queue_mixin L11).
        if new_index is None:
            return {"ok": False, "error": "new_index required"}
        try:
            _idx = int(new_index)
        except (TypeError, ValueError):
            return {"ok": False, "error": f"Invalid new_index: {new_index!r}"}
        ok = self._queues.sync_reorder(str(identifier or ""), _idx)
        self._on_queue_changed()
        return {"ok": ok}


    def queues_gpu_reorder(self, identifier, new_index):
        if new_index is None:
            return {"ok": False, "error": "new_index required"}
        try:
            _idx = int(new_index)
        except (TypeError, ValueError):
            return {"ok": False, "error": f"Invalid new_index: {new_index!r}"}
        ok = self._queues.gpu_reorder(str(identifier or ""), _idx)
        self._on_queue_changed()
        return {"ok": ok}


    # ─── Global pause / resume / skip (both queues) ────────────────────

    def queue_pause(self, which="both"):
        """Pause the sync queue, GPU queue, or both (`which` in:
        'sync' | 'gpu' | 'both'). Persisted to queue state."""
        if which in ("sync", "both"):
            self._sync_pause.set()
            self._queues.set_sync_paused(True)
            self._transcribe.pause() # covers mixed queues via TranscribeManager
        if which in ("gpu", "both"):
            self._queues.set_gpu_paused(True)
            self._transcribe.pause()
        self._on_queue_changed()
        return {"ok": True, "paused": which}


    def queue_resume(self, which="both"):
        """Resume a paused queue."""
        if which in ("sync", "both"):
            self._sync_pause.clear()
            self._queues.set_sync_paused(False)
            self._transcribe.resume()
        if which in ("gpu", "both"):
            self._queues.set_gpu_paused(False)
            self._transcribe.resume()
        self._on_queue_changed()
        return {"ok": True, "paused": False}


    def queue_is_paused(self):
        """Return current paused state for each queue."""
        return {
            "sync": bool(self._queues.sync_paused),
            "gpu": bool(self._queues.gpu_paused),
        }
