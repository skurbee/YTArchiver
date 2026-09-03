"""
SyncMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

import os
import re
import threading
import time
import uuid

from backend import subs as subs_backend
from backend import sync as sync_backend
from backend import youtube_session, youtube_traffic
from backend.log import swallow
from backend.process_runner import PROCESS_REGISTRY, process_owner_scope
from backend.services.job_supervisor import WorkAdmissionClosed
from backend.services.managed_work import start_managed_task
from backend.ytarchiver_config import ARCHIVE_FILE, load_config

from ._shared import _api_err, _log


class SyncMixin:

    def sync_is_running(self):
        return bool(self._sync_thread and self._sync_thread.is_alive())


    def _start_sync_thread_locked(self, target):
        """Assign and start _sync_thread only if no sync worker is alive."""
        if not hasattr(self, "_sync_start_lock"):
            self._sync_start_lock = threading.Lock()
        with self._sync_start_lock:
            if self.sync_is_running():
                return False
            return self._start_sync_thread_unlocked(target)

    def _start_sync_thread_unlocked(self, target):
        """Register a sync-lane worker before its OS thread can begin."""
        sync_job_id = f"sync-{uuid.uuid4().hex}"
        cancel = getattr(self, "_sync_cancel", None)
        if not isinstance(cancel, threading.Event):
            cancel = threading.Event()

        def _owned_target():
            with process_owner_scope("sync", sync_job_id):
                target()

        try:
            self._sync_thread = start_managed_task(
                self,
                owner="sync",
                label="Sync and redownload lane",
                task_id=sync_job_id,
                cancel=cancel,
                target=_owned_target,
                name="sync-worker",
                thread_factory=threading.Thread,
            )
            self._sync_start_error = ""
            return True
        except WorkAdmissionClosed as exc:
            self._sync_start_error = str(exc)
            return False


    def _maybe_autostart_sync(self):
        """Auto-fire the sync worker after a queue-enqueue UI action.

        Two gates:
          1) Worker already alive → don't double-start (sync_start_all
             would be harmless, but this skips the noise).
          2) Queue is paused → respect the pause. UX rule:
             if the queue is paused, enqueuing adds to the queue
             but does NOT start it. The user has to resume manually.

        Returns True if the worker was actually started, False otherwise.
        Callers can include this flag in their JSON response so the JS
        toast can say "Queued — click Resume to start" instead of the
        default "Queued and started" wording.
        """
        try:
            if self.sync_is_running():
                return False
            if bool(self._queues.sync_paused):
                return False
            self.sync_start_all(add_downloads_from_config=False)
            return True
        except Exception:
            return False


    def _drain_pending_redownload_after_sync(self):
        """Start the redownload chain after a regular sync releases the lane."""
        try:
            with self._redwnl_lock:
                if not self._redwnl_pending:
                    return
                # chan_redownload re-appends this item and starts the shared
                # worker now that the regular sync thread has exited.
                first = self._redwnl_pending.pop(0)
            ch = first.get("ch") or {}
            new_res = first.get("new_res") or "best"
            kwargs = {
                "scope": first.get("scope"),
                "only_video": first.get("only_video"),
            }
            restored_id = str(
                (first.get("rd_task") or {}).get("task_id") or "").strip()
            if restored_id:
                kwargs["task_id"] = restored_id
            result = self.chan_redownload(ch, new_res, **kwargs)
            if not isinstance(result, dict) or not result.get("ok"):
                # The durable QueueState row was deliberately retained. Keep
                # its execution companion too; otherwise the row remains
                # visible but cannot run again until a process restart.
                with self._redwnl_lock:
                    current_id = str(
                        (self._queues.current_sync or {}).get("task_id")
                        or "").strip()
                    pending_ids = {
                        str(((item or {}).get("rd_task") or {}).get("task_id")
                            or "").strip()
                        for item in self._redwnl_pending
                    }
                    if (restored_id != current_id
                            and restored_id not in pending_ids):
                        self._redwnl_pending.insert(0, first)
                _log.warning(
                    "post-sync redownload remained queued: %s",
                    (result or {}).get("error")
                    if isinstance(result, dict) else "invalid response")
        except Exception as e:
            try:
                with self._redwnl_lock:
                    pending_ids = {
                        str(((item or {}).get("rd_task") or {}).get("task_id")
                            or "").strip()
                        for item in self._redwnl_pending
                    }
                    restored_id = str(
                        ((first or {}).get("rd_task") or {}).get("task_id")
                        or "").strip()
                    if restored_id and restored_id not in pending_ids:
                        self._redwnl_pending.insert(0, first)
            except Exception:
                pass
            _log.warning(
                "post-sync redownload drain failed for queued task: %s", e)


    def sync_start_all(self, add_downloads_from_config=True, scheduled=False,
                       traffic_reservation_id=None):
        """Kick off the sync worker thread.

        `add_downloads_from_config=True` (default, for Sync Subbed):
        enqueue a `kind=download` task for every subscribed channel
        before the worker starts processing.

        `add_downloads_from_config=False`: spawn the worker but don't
        add anything to the queue. Used by metadata/compress auto-fire
        paths that just need to drain whatever's already queued.
        a bug: metadata_queue_all was calling sync_start_all
        (which always added 103 downloads) instead of just starting
        the worker \u2014 so "Queued metadata for 103 channels" turned
        into "Sync pass starting (206 channels)."
        """
        admission = getattr(self, "_work_admission_error", None)
        if callable(admission):
            blocked = admission("a sync")
            if blocked is not None:
                return blocked
        # Hold a lock around the is-running check + thread spawn. Old
        # code did check-then-spawn outside a lock, so two near-
        # simultaneous calls (autorun timer + user-clicked Start)
        # could both pass the check and spawn parallel worker threads
        # (audit: sync_mixin.py:46). The check inside _start_sync_locked
        # is now atomic with the assignment.
        if not hasattr(self, "_sync_start_lock"):
            self._sync_start_lock = threading.Lock()
        # Non-blocking attempt — if another caller is mid-startup we
        # treat it the same as "already running".
        _acquired = self._sync_start_lock.acquire(blocking=False)
        if not _acquired:
            return {"ok": False, "error": "Sync already starting"}
        try:
            if self.sync_is_running():
                return {"ok": False, "error": "Sync already running"}
            if getattr(self, "_reorg_running", False):
                return {"ok": False,
                        "error": "Folder reorganization is running. Wait for it to finish before syncing."}
            if not sync_backend.find_yt_dlp():
                return {"ok": False, "error": "yt-dlp not found. Install yt-dlp or place yt-dlp.exe next to the app."}
            return self._sync_start_all_inner(
                add_downloads_from_config, scheduled=scheduled,
                traffic_reservation_id=traffic_reservation_id)
        finally:
            try: self._sync_start_lock.release()
            except Exception: pass


    def _sync_start_all_inner(self, add_downloads_from_config=True,
                              scheduled=False,
                              traffic_reservation_id=None):
        """Inner body of sync_start_all. Caller must hold
        _sync_start_lock. Encapsulates the original logic so the
        atomic check-and-spawn wrapper stays small.
        """
        # Auto-off + fresh "Sync Subbed" click: enqueue every channel
        # but DON'T spawn the worker. User must manually click Start in
        # the Sync Tasks popover (or toggle Auto on). Matches classic
        # behavior where Auto-off means the queue is a shopping list,
        # not a spin-up. The internal metadata/compress path uses
        # add_downloads_from_config=False — those paths already have
        # items queued and just need the worker drained, so they
        # bypass this gate.
        if add_downloads_from_config:
            try:
                cfg = self._config or load_config()
                if not bool(cfg.get("autorun_sync", False)):
                    # Don't double-queue if a prior Sync Subbed already
                    # staged all the download tasks.
                    existing_dl = any(
                        (c.get("kind") or "download").lower() == "download"
                        for c in self._queues.sync)
                    queued = 0
                    if not existing_dl:
                        for ch in cfg.get("channels", []):
                            if self._queues.sync_enqueue(ch):
                                queued += 1
                    self._on_queue_changed()
                    # return both `queued` (new items just
                    # added this call) AND `total_queued` (items sitting
                    # in the queue, including already-queued ones).
                    # Callers that only care about "is anything queued"
                    # can use `total_queued` without guessing.
                    try:
                        total_queued = len(self._queues.sync)
                    except Exception:
                        total_queued = queued
                    return {"ok": True, "started": False,
                            "queued": queued,
                            "total_queued": total_queued}
            except Exception:
                # If anything goes wrong here, fall through to the
                # old behavior (start the worker). Better to over-fire
                # than to silently drop the user's action.
                pass
        # Clear every event that could have been left set by a previous pass:
        # cancel — fired by "Clear Queue" or the Cancel button
        # skip — fired by "Skip current"
        # pause — fired by the Pause dialog, and NEVER auto-cleared before
        # this fix. Without this clear, starting a new pass after
        # a paused-and-cancelled pass would immediately re-enter
        # the "\u23F8 Sync paused at ..." wait loop with no way
        # to resume via the UI because the dialog-Pause button
        # is meant for mid-pass pausing, not from a cold start.
        # Overrides are scoped to one sync pass. A fresh pass always starts
        # with the configured rolling ceilings armed.
        youtube_traffic.clear_budget_override()
        self._sync_cancel.clear()
        self._sync_skip.clear()
        self._sync_pause.clear()
        if not hasattr(self, "_sync_clear_requested"):
            self._sync_clear_requested = threading.Event()
        self._sync_clear_requested.clear()
        # Mirror the pause-clear onto the QueueState flag too. `queue_pause`
        # sets both the threading.Event AND `QueueState.sync_paused`, but
        # only the Event was cleared here — so a new pass saw `sync_paused`
        # still True, the Pause button flipped to "Resume", and clicking
        # it fired `queue_resume` with no effect. Clear both.
        try: self._queues.set_sync_paused(False)
        except Exception as e: _log.warning("sync start: could not clear sync_paused flag; Pause button may show stale state: %s", e)
        # Release ONLY a pause restored from a prior session (user paused then
        # quit) so leftover restored transcribe work isn't stuck behind it when
        # a fresh sync starts. A pause the user set in the CURRENT session is
        # left intact — clearing it on every sync start silently un-paused a
        # deliberately-paused Processing queue (an auto-sync catching a download
        # resumed it mid-batch; user-reported). gpu_pause_restored flips False
        # the moment the user touches the pause this session.
        try:
            if getattr(self._queues, "gpu_pause_restored", False):
                self._queues.set_gpu_paused(False)
                self._transcribe.resume()
        except Exception as e: _log.warning("sync start: could not clear restored GPU pause: %s", e)
        # Start tray icon spin animation so the user can see sync is live
        # even when the window is minimized. Matches YTArchiver.py:3526
        # _tray_start_spin(red=False).
        try:
            if getattr(self, "_tray", None):
                self._tray.start_spin("blue")
                self._tray.set_tooltip("YTArchiver \u2014 Syncing...")
        except Exception as e:
            _log.debug("swallowed: %s", e)
        def _run():
            sync_result = {}
            try:
                youtube_session.begin_sync_scope(
                    background=bool(scheduled),
                    cancel_event=self._sync_cancel,
                )
                with youtube_traffic.reservation_scope(
                        traffic_reservation_id):
                    sync_result = sync_backend.sync_all(
                        self._log_stream, self._sync_cancel,
                        queues=self._queues,
                        transcribe_mgr=self._transcribe,
                        pause_event=self._sync_pause,
                        skip_event=self._sync_skip,
                        add_downloads_from_config=bool(
                            add_downloads_from_config),
                        autosync=bool(scheduled),
                        clear_event=self._sync_clear_requested,
                    )
            except Exception as e:
                self._log_stream.emit_error(f"Sync crashed: {e}")
            finally:
                youtube_session.end_sync_scope()
                youtube_traffic.finish_reservation(
                    traffic_reservation_id)
                youtube_traffic.clear_budget_override()
                # Stop the tray spin + restore idle tooltip.
                try:
                    if getattr(self, "_tray", None):
                        self._tray.stop_spin()
                        self._tray.set_tooltip("YTArchiver \u2014 Idle")
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                # Clear stale sync-progress so a companion display leaves
                # the Sync source. Mirrors OLD's
                # _clear_sync_progress() call at the end of every sync path
                # (YTArchiver.py:6972, :7052, :7128, :19671, :23364).
                try: sync_backend.clear_sync_progress()
                except Exception as e: _log.debug("swallowed: %s", e)
                # Tell the autorun scheduler this sync completed — it was
                # holding its countdown at "Syncing..." and now resumes
                # counting down from a full interval. Matches classic's
                # `_schedule_autorun(iv)` inside the sync finally
                # (YTArchiver.py:23380).
                rate_limited_run = (
                    scheduled
                    and isinstance(sync_result, dict)
                    and bool(sync_result.get("rate_limited"))
                )
                try:
                    if rate_limited_run:
                        self._autorun.defer_after_rate_limit()
                    else:
                        self._autorun.notify_sync_done()
                except Exception as e:
                    _log.warning(
                        "autorun rearm failed; countdown may not reset: %s", e)
                if rate_limited_run:
                    retry_when = "at the next scheduled run"
                    try:
                        _state = self._autorun.get_state()
                        _next_ts = _state.get("next_fire_ts")
                        if _next_ts:
                            _clock = time.strftime(
                                "%I:%M%p", time.localtime(float(_next_ts)))
                            retry_when = f"at {_clock.lstrip('0').lower()}"
                        elif _state.get("label") not in (None, "", "Off"):
                            retry_when = f"in {_state['label']}"
                    except Exception as e:
                        _log.debug(
                            "autosync rate-limit retry label failed: %s", e)
                    self._log_stream.emit_error(
                        "Auto-sync ended because YouTube rate-limited the "
                        "account. The scheduled sync queue was cleared; "
                        f"auto-sync will try again {retry_when}.")
                # Refresh the in-memory config snapshot so consumers that
                # read self._config (Last Full Sync label, channel
                # listing, etc.) see the new last_sync timestamp + any
                # initialized/sync_complete flags the sync just wrote.
                try: self._reload_config()
                except Exception as e: _log.warning("post-sync config reload failed; Last Full Sync label may be stale: %s", e)
                # Push the new "Last Full Sync" label to the UI now,
                # not 60 seconds later when the JS tick happens to fire.
                try:
                    if self._window is not None:
                        self._window.evaluate_js(
                            "(function(){"
                            " if (!window.pywebview || !window.pywebview.api) return;"
                            " var api = window.pywebview.api;"
                            " if (!api.get_last_sync_label) return;"
                            " api.get_last_sync_label().then(function(r){"
                            "   if (!r || !r.label) return;"
                            "   var el = document.getElementById('last-full-sync');"
                            "   if (el) el.textContent = r.label;"
                            " }).catch(function(){});"
                            "})();")
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                self._log_stream.flush()
                self._on_queue_changed()
                # drain any pending redownloads that were
                # queued WHILE this sync was running. chan_redownload
                # appends to _redwnl_pending and returns immediately
                # when sync is alive; the chain-worker only fires on
                # the initial spawn. Without this hook, items sit
                # there forever. Defer to after our finally returns
                # so _sync_thread.is_alive() reads False when the
                # chain worker is spawned.
                try:
                    followup_cancel = threading.Event()

                    def _post_sync_followup():
                        # Wait until this sync worker's finally has retired it
                        # from the shared lane, then drain redownload work and
                        # publish a truthful idle/running UI state.
                        if followup_cancel.wait(0.6):
                            return
                        self._drain_pending_redownload_after_sync()
                        if not followup_cancel.is_set():
                            self._on_queue_changed()

                    start_managed_task(
                        self,
                        owner="sync-followup",
                        label="Post-sync queue follow-up",
                        target=_post_sync_followup,
                        cancel=followup_cancel,
                        name="post-sync-followup",
                        thread_factory=threading.Thread,
                    )
                except Exception as e:
                    # Admission closing is safe: the durable queue row remains
                    # and shutdown/restore owns the next transition.
                    _log.debug("post-sync follow-up not started: %s", e)
        if not self._start_sync_thread_unlocked(_run):
            return {
                "ok": False,
                "started": False,
                "error": getattr(
                    self, "_sync_start_error", "Sync could not be started"),
            }
        self._on_queue_changed()
        return {"ok": True, "started": True}


    def sync_cancel(self):
        # drain the redownload pending list on cancel.
        # Before, a cancel stopped the currently-running redownload
        # but the next 2+ items in `_redwnl_pending` would still
        # run silently when the worker looped around. Now cancel
        # means cancel everything, matching user expectation.
        self._sync_cancel.set()
        # Clear paused state symmetrically with the cancel. Without
        # this, the queue would be empty but `_sync_pause` /
        # `queues.sync_paused` / `queues.gpu_paused` stayed set, so
        # the global Pause/Resume button kept showing "Resume" forever
        # with nothing to resume (audit: sync_mixin H16). Matches the
        # cleanup the redownload-chain path does at chan_redownload.
        try:
            self._sync_pause.clear()
        except Exception as e: _log.debug("swallowed: %s", e)
        try:
            self._queues.set_sync_paused(False)
            self._queues.set_gpu_paused(False)
        except Exception as e: _log.debug("swallowed: %s", e)
        try:
            self._transcribe.resume()
        except Exception as e: _log.debug("swallowed: %s", e)
        try:
            # Hold _redwnl_lock for the drain so a concurrent chan_
            # redownload worker can't pop(0) from an empty list mid-
            # clear and IndexError. Same protection used in
            # chan_cancel_redownload.
            with self._redwnl_lock:
                _drained = len(self._redwnl_pending)
                self._redwnl_pending.clear()
                # Global cancel stops the IN-FLIGHT redownload too.
                # Set under _redwnl_lock so the chain worker's per-item
                # clear (same lock) can't interleave and wipe this.
                self._redwnl_cancel.set()
            if _drained:
                # Notify the UI so the queue popover clears visually.
                self._on_queue_changed()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return {"ok": True}


    def sync_clear_queue(self):
        """Drop every queued sync task AND fire cancel so the current pass
        stops at the next channel boundary. Distinct from `sync_cancel` in
        that it ALSO empties `QueueState.sync` so the Sync Tasks popover
        goes empty; cancel alone just aborts the in-flight pass while
        leaving queued items in place. UI exposes this as `Clear Queue`.
        """
        pending_snapshot = []
        try:
            pending_snapshot = self._queues.sync_snapshot()
            _was_running = bool(self._queues.current_sync)
            removed = self._queues.sync_clear()
            if removed < 0:
                return {"ok": False, "removed": 0,
                        "running": _was_running,
                        "error": "Sync queue could not be saved"}
            if not self._queues.clear_resuming_slots(
                    "sync", clear_current=True):
                self._queues.restore_pending_snapshot(
                    "sync", pending_snapshot)
                return {"ok": False, "removed": 0,
                        "running": _was_running,
                        "error": "Running sync task could not be saved as cancelled"}
        except Exception as exc:
            _log.warning("sync queue clear transaction failed: %s", exc)
            try:
                self._queues.restore_pending_snapshot(
                    "sync", pending_snapshot)
            except Exception:
                pass
            return {"ok": False, "removed": 0,
                    "running": bool(getattr(
                        self._queues, "current_sync", None)),
                    "error": "Sync queue could not be saved"}

        # Both QueueState files are committed before any live cancellation is
        # signalled.  A failed response therefore cannot stop recoverable work.
        if not hasattr(self, "_sync_clear_requested"):
            self._sync_clear_requested = threading.Event()
        self._sync_clear_requested.set()
        self._sync_cancel.set()
        # Clear Queue must also drain + stop the redownload chain.
        # sync_clear() above already emptied its UI rows; leaving
        # _redwnl_pending populated would let the worker resurrect and
        # run items the user just cleared (previously the stale shared
        # cancel event aborted them invisibly — an accident, not a
        # design). Drain + set under _redwnl_lock, same as sync_cancel.
        try:
            with self._redwnl_lock:
                self._redwnl_pending.clear()
                self._redwnl_cancel.set()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        # Report whether a channel was actively running so the UI can
        # show clearer feedback ("stopping current download" vs "nothing
        # was queued").
        self._on_queue_changed()
        return {"ok": True, "removed": removed, "running": _was_running}


    def sync_force_stop(self):
        """Hard stop: clear queue AND kill any in-flight child subprocesses.

        Soft cancel (`sync_clear_queue`) sets the cancel event but the
        worker only notices between subprocess output lines. yt-dlp's
        flat-playlist call against a 10k-video channel can sit blocked
        for 5-10 minutes on a single fetch with no output, so soft
        cancel feels broken — the queue clears visually but you stare
        at the running task forever.

        Sync worker threads attach owner metadata to every yt-dlp process.
        Force Stop targets only those registered ``owner="sync"`` trees.
        Manual downloads, Processing/GPU work, metadata, and the updater are
        separate owners and cannot be selected by this operation.

        Returns {ok, removed, killed} so the UI can toast a useful
        message ("Stopped — 12 queued cleared, 3 subprocesses killed.").
        """
        clear_result = self.sync_clear_queue()
        if not clear_result.get("ok"):
            return {"ok": False, "removed": 0, "killed": 0,
                    "error": clear_result.get("error")
                    or "Sync queue could not be saved"}
        removed = int(clear_result.get("removed") or 0)
        killed = 0
        try:
            killed = PROCESS_REGISTRY.terminate_owner("sync", timeout=2.0)
        except Exception as e:
            try:
                self._log_stream.emit_error(
                    f"force-stop: owned process cleanup failed: {e}")
            except Exception as e:
                _log.debug("swallowed: %s", e)
        try:
            self._log_stream.emit_text(
                f" — Force-stop: cleared {removed} queued, "
                f"killed {killed} subprocess(es).",
                "simpleline_pink")
            self._log_stream.flush()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        self._on_queue_changed()
        return {"ok": True, "removed": removed, "killed": killed}


    def gpu_clear_queue(self):
        """Drop every queued GPU task. Currently-running job (if any) is
        also cancelled — subprocess killed, popover slot cleared, and
        the pending journal rewritten so nothing resurrects on the next
        launch.
        """
        try:
            removed = len(self._queues.gpu_snapshot())
            if not self._transcribe.cancel_all():
                return {"ok": False, "removed": 0,
                        "error": "Processing queue could not be saved"}
        except Exception as exc:
            _log.warning("Processing queue clear failed: %s", exc)
            return {"ok": False, "removed": 0,
                    "error": "Processing queue could not be saved"}
        self._on_queue_changed()
        return {"ok": True, "removed": removed}


    def sync_enqueue_all_channels(self):
        """Append every subscribed channel to the sync queue without
        starting the worker. Right-click on Sync Subbed: "add to end of
        queue". Dedupe is handled by `sync_enqueue` (kind+url key), so
        channels already queued or currently running are skipped.
        Returns {ok, queued, skipped, total_queued}.
        """
        admission = getattr(self, "_work_admission_error", None)
        if callable(admission):
            blocked = admission("a sync queue change")
            if blocked is not None:
                return blocked
        try:
            cfg = self._config or load_config()
            channels = cfg.get("channels", []) or []
            queued = 0
            skipped = 0
            for ch in channels:
                if self._queues.sync_enqueue(ch):
                    queued += 1
                else:
                    skipped += 1
            try: self._on_queue_changed()
            except Exception as e: _log.debug("swallowed: %s", e)
            try: total_queued = len(self._queues.sync)
            except Exception: total_queued = queued
            return {"ok": True, "queued": queued,
                    "skipped": skipped, "total_queued": total_queued}
        except Exception as e:
            return _api_err("INTERNAL_ERROR", str(e))


    def sync_prefetch_channel(self, identity):
        """Probe a channel for total video + live counts before sync starts.
        Best-effort — returns {ok, total, lives, upcoming}.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        task_id = f"sync-prefetch-{uuid.uuid4().hex}"
        cancel = threading.Event()
        def _run():
            try:
                if cancel.is_set():
                    return
                r = sync_backend.prefetch_channel_total(ch.get("url", ""))
                if r.get("ok") and not cancel.is_set():
                    self._log_stream.emit([
                        ["[Prefetch] ", "sync_bracket"],
                        [f"{ch.get('name', '?')}: ", "simpleline_blue"],
                        [f"{r.get('total', 0)} total, "
                         f"{r.get('lives', 0)} live, "
                         f"{r.get('upcoming', 0)} upcoming\n",
                         "simpleline"],
                    ])
                    self._log_stream.flush()
            except Exception as e:
                _log.debug("swallowed: %s", e)
        try:
            start_managed_task(
                self,
                owner="channel-prefetch",
                label="Prefetch channel totals",
                task_id=task_id,
                cancel=cancel,
                target=_run,
                name="channel-total-prefetch",
                thread_factory=threading.Thread,
            )
        except WorkAdmissionClosed as exc:
            return {"ok": False, "started": False, "error": str(exc)}
        return {"ok": True, "started": True}


    def sync_quick_check(self, identity):
        """Check the first 5 videos of a channel against our archive to see
        if there's anything new. Returns {ok, has_new, checked, fresh_ids}.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        try:
            from backend.channel_cache import get_cached_ids as _cc_load
            cached = _cc_load(ch.get("url", "")) or []
        except Exception:
            cached = []
        return sync_backend.quick_check_new_uploads(
            ch.get("url", ""), cached)


    def _sync_quick_check_no_new_channel(self, ch):
        url = (ch.get("url") or "").strip()
        mode = (ch.get("mode") or "full").lower()
        sync_ok = bool(ch.get("sync_complete", False))
        if ch.get("init_complete", False):
            sync_ok = True
        if (not url or not ch.get("init_complete", False) or not sync_ok
                or mode not in ("full", "new", "fromdate")
                or (ch.get("failed_video_ids") or {})):
            return False
        try:
            from backend.sync.sync_all import _channel_folder_has_media
            if not _channel_folder_has_media(load_config(), ch):
                return False
        except Exception as e:
            swallow("single quickcheck disk gate", e)

        known = set()
        vid_re = re.compile(r"^[A-Za-z0-9_-]{11}$")
        try:
            if os.path.isfile(ARCHIVE_FILE):
                with open(ARCHIVE_FILE, "r", encoding="utf-8",
                          errors="replace") as af:
                    for line in af:
                        parts = line.strip().split(None, 1)
                        if len(parts) == 2 and vid_re.match(parts[1]):
                            known.add(parts[1])
        except OSError:
            pass
        opts = None
        cc = None
        try:
            from backend import channel_cache as cc
            from backend.sync.options import normalize_channel_sync_options
            opts = normalize_channel_sync_options(ch)
            known.update(cc.get_filtered_ids(
                url, opts.min_duration, opts.max_duration))
        except Exception as e:
            swallow("single quickcheck filtered cache", e)
        if not known:
            return False
        try:
            min_duration = opts.min_duration if opts is not None else 0
            max_duration = opts.max_duration if opts is not None else 0
            qc = sync_backend.quick_check_new_uploads(
                url, known, check_count=5, timeout_sec=30,
                min_duration=min_duration,
                max_duration=max_duration)
            if qc.get("filtered_ids") and cc is not None:
                try:
                    cc.append_filtered_ids(
                        url, qc.get("filtered_ids") or [],
                        min_duration, max_duration)
                except Exception as e:
                    swallow("single quickcheck filtered-id append", e)
            return bool(qc.get("ok") and not qc.get("has_new"))
        except Exception as e:
            swallow("single quickcheck", e)
            return False


    def sync_skip_current(self, task_id=""):
        """Skip the currently-running sync item and advance to the next.

        Sets _sync_skip only. sync_all passes _sync_skip as `kill_current`
        to sync_channel, which terminates the in-flight yt-dlp subprocess
        when it sees the flag — no longer overloading _sync_cancel, so a
        rapid Skip+Cancel sequence can't swallow the Cancel.
        """
        try:
            wanted = str(task_id or "").strip()
            current_id = str(
                (self._queues.current_sync or {}).get("task_id") or "").strip()
            if not wanted or wanted != current_id:
                return {"ok": False,
                        "error": "Queue changed; task is no longer running"}
            if not self._queues.replace_current_task_durable(
                    "sync", None, expected_task_id=wanted):
                return {"ok": False,
                        "error": "Current sync task could not be saved as cancelled"}
            # The durable queue/current transition is the commit point.  Do
            # not signal yt-dlp before it succeeds or a failed API response
            # could still cancel recoverable work.
            self._sync_skip.set()
            try:
                self._log_stream.emit([
                    ["[Sync] ", "sync_bracket"],
                    ["Skip current channel \u2014 moving on\n", "simpleline"],
                ])
                self._log_stream.flush()
            except Exception as exc:
                _log.debug("sync skip log failed: %s", exc)
            return {"ok": True}
        except Exception as e:
            return _api_err("INTERNAL_ERROR", str(e))


    def gpu_skip_current(self, task_id=""):
        """Durably cancel the exact running Processing task."""
        try:
            wanted = str(task_id or "").strip()
            current_id = str(
                (self._queues.current_gpu or {}).get("task_id") or "").strip()
            if not wanted or wanted != current_id:
                return {"ok": False,
                        "error": "Queue changed; task is no longer running"}

            committed = self._transcribe.cancel_current_durable(
                wanted,
                lambda: self._queues.replace_current_task_durable(
                    "gpu", None, expected_task_id=wanted),
            )
            if not committed:
                return {"ok": False,
                        "error": "Current Processing task could not be saved as cancelled"}
            try:
                self._log_stream.emit([
                    ["[GPU] ", "trans_bracket"],
                    ["Skip current GPU job \u2014 moving on\n", "simpleline"],
                ])
                self._log_stream.flush()
            except Exception as exc:
                _log.debug("GPU skip log failed: %s", exc)
            return {"ok": True}
        except Exception as e:
            return _api_err("INTERNAL_ERROR", str(e))

    def sync_defer_current(self, task_id=""):
        """Send the currently-running sync task to the END of the queue,
        then cancel the running pass so the next queued item picks up.
        Different from sync_skip_current \u2014 `skip` drops the task; `defer`
        keeps it but reorders it for later. Used by the right-click
        "Skip this job" action where the user wants "do this one later,
        not lose it".

        Strips the `_pass_start_ts` cursor so the deferred task starts a
        fresh pass when it eventually runs again \u2014 otherwise its first
        re-entry would skip every video already refreshed in this aborted
        pass and produce an empty "no videos in scope" result.
        """
        try:
            cur = self._queues.current_sync
            wanted = str(task_id or "").strip()
            if not wanted or wanted != str(
                    (cur or {}).get("task_id") or "").strip():
                return {"ok": False, "error": "Queue changed; task is no longer running"}
            if cur:
                deferred = dict(cur)
                deferred.pop("_pass_start_ts", None)
                if not self._queues.sync_defer_task(deferred):
                    return {"ok": False,
                            "error": "Deferred task could not be saved"}
                self._log_stream.emit([
                    ["[Sync] ", "sync_bracket"],
                    [(f"Deferred {deferred.get('name') or deferred.get('url') or 'current job'}"
                      " \u2014 sent to end of queue\n"), "simpleline"],
                ])
            # Now skip the in-flight run so the next queued item starts.
            result = self.sync_skip_current(task_id)
            if isinstance(result, dict) and not result.get("ok") and cur:
                try:
                    self._queues.sync_remove_task(
                        str(deferred.get("task_id") or ""), durable=True)
                except Exception as e:
                    _log.warning("sync defer rollback failed: %s", e)
            return result
        except Exception as e:
            return _api_err("INTERNAL_ERROR", str(e))


    def gpu_defer_current(self, task_id=""):
        """Send the currently-running GPU task to the END of the GPU
        queue, then cancel the running job. See sync_defer_current
        rationale.
        """
        try:
            cur = self._queues.current_gpu
            wanted = str(task_id or "").strip()
            if not wanted or wanted != str(
                    (cur or {}).get("task_id") or "").strip():
                return {"ok": False, "error": "Queue changed; task is no longer running"}
            if cur:
                manager = getattr(self, "_transcribe", None)
                defer_exact = getattr(manager, "defer_current", None)
                if wanted and callable(defer_exact):
                    if not defer_exact(wanted):
                        return {"ok": False,
                                "error": "Queue changed; task is no longer running"}
                    self._log_stream.emit([
                        ["[GPU] ", "trans_bracket"],
                        [(f"Deferred {cur.get('title') or cur.get('path') or 'current job'}"
                          " — sent to end of queue\n"), "simpleline"],
                    ])
                    self._log_stream.flush()
                    return {"ok": True}

                # Internal compatibility for a pre-ID current slot/test double.
                # Current frontend calls always take the exact path above.
                deferred = dict(cur)
                try:
                    key = str(deferred.get("task_id") or "").strip()
                    if not key and not wanted:
                        # In-process migration compatibility for a legacy
                        # current slot. New UI requests always carry task_id.
                        key = str(deferred.get("path") or "").strip()
                    if key:
                        self._queues.gpu_remove(key)
                    elif deferred.get("bulk_id"):
                        self._queues.gpu_remove_bulk(str(deferred.get("bulk_id") or ""))
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                self._queues.gpu_enqueue(deferred)
                self._log_stream.emit([
                    ["[GPU] ", "trans_bracket"],
                    [(f"Deferred {deferred.get('title') or deferred.get('path') or 'current job'}"
                      " \u2014 sent to end of queue\n"), "simpleline"],
                ])
            return self.gpu_skip_current(task_id)
        except Exception as e:
            return _api_err("INTERNAL_ERROR", str(e))


    def sync_one_channel(self, identity):
        """Sync just one channel (used by context-menu 'Sync now').

        Always enqueue the channel and drain it through sync_all so the same
        pause, restart, and recovery semantics apply whether the lane is idle
        or already running.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        # Always put single-channel work through QueueState + sync_all.  The
        # former idle fast path below launched sync_channel directly, so a
        # pause-interrupted new-channel bootstrap had no pending queue entry
        # to persist and restore after an app/computer restart.
        already_running = self.sync_is_running()
        if not already_running and not sync_backend.find_yt_dlp():
            return {"ok": False, "error": "yt-dlp not found"}
        ch_name = ch.get("name") or ch.get("folder", "")
        try:
            added = bool(self._queues.sync_enqueue(ch))
        except Exception as e:
            return {"ok": False, "error": f"Could not queue: {e}"}
        try: self._on_queue_changed()
        except Exception as e: _log.debug("swallowed: %s", e)

        # Report a deliberately paused/restored queue even when its worker is
        # still alive in the pause wait. The UI needs this truth to explain
        # that the newly queued work will not start until Resume is pressed.
        if bool(self._queues.sync_paused):
            return {"ok": True, "queued": added, "started": False,
                    "paused": True, "name": ch_name}

        # A live worker will pick the task up between queue iterations.
        if already_running or self.sync_is_running():
            return {"ok": True, "queued": added, "started": False,
                    "name": ch_name}

        started = self.sync_start_all(add_downloads_from_config=False)
        if not started or not started.get("ok"):
            return {"ok": False,
                    "error": ((started or {}).get("error")
                              or "Sync could not be started"),
                    "queued": added, "name": ch_name}
        return {"ok": True, "queued": added, "started": True,
                "name": ch_name}
