"""
SyncMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class SyncMixin:

    def sync_is_running(self):
        return bool(self._sync_thread and self._sync_thread.is_alive())


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


    def sync_start_all(self, add_downloads_from_config=True):
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
            if not sync_backend.find_yt_dlp():
                return {"ok": False, "error": "yt-dlp not found. Install yt-dlp or place yt-dlp.exe next to the app."}
            return self._sync_start_all_inner(add_downloads_from_config)
        finally:
            try: self._sync_start_lock.release()
            except Exception: pass


    def _sync_start_all_inner(self, add_downloads_from_config=True):
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
        self._sync_cancel.clear()
        self._sync_skip.clear()
        self._sync_pause.clear()
        # Mirror the pause-clear onto the QueueState flag too. `queue_pause`
        # sets both the threading.Event AND `QueueState.sync_paused`, but
        # only the Event was cleared here — so a new pass saw `sync_paused`
        # still True, the Pause button flipped to "Resume", and clicking
        # it fired `queue_resume` with no effect. Clear both.
        try: self._queues.set_sync_paused(False)
        except Exception as e: _log.debug("swallowed: %s", e)
        # Starting sync implies "resume all work" — clear the GPU pause
        # flag too so transcribe jobs dispatched from this pass actually
        # process instead of piling up behind a stale paused flag left
        # over from a prior session.
        try:
            self._queues.set_gpu_paused(False)
            self._transcribe.resume()
        except Exception as e: _log.debug("swallowed: %s", e)
        # Start tray icon spin animation so the user can see sync is live
        # even when the window is minimized. Matches YTArchiver.py:3526
        # _tray_start_spin(red=False).
        try:
            if getattr(self, "_tray", None):
                self._tray.start_spin("blue")
                self._tray.set_tooltip("YT Archiver \u2014 Syncing...")
        except Exception as e:
            _log.debug("swallowed: %s", e)
        def _run():
            try:
                sync_backend.sync_all(self._log_stream, self._sync_cancel,
                                      queues=self._queues,
                                      transcribe_mgr=self._transcribe,
                                      pause_event=self._sync_pause,
                                      skip_event=self._sync_skip,
                                      add_downloads_from_config=bool(
                                          add_downloads_from_config))
            except Exception as e:
                self._log_stream.emit_error(f"Sync crashed: {e}")
            finally:
                # Stop the tray spin + restore idle tooltip.
                try:
                    if getattr(self, "_tray", None):
                        self._tray.stop_spin()
                        self._tray.set_tooltip("YT Archiver \u2014 Idle")
                        # Clear session download badge — fresh pass next time
                        try: self._tray.set_badge(0)
                        except Exception as e: _log.debug("swallowed: %s", e)
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                self._session_dl_count = 0
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
                try: self._autorun.notify_sync_done()
                except Exception as e: _log.debug("swallowed: %s", e)
                # Refresh the in-memory config snapshot so consumers that
                # read self._config (Last Full Sync label, channel
                # listing, etc.) see the new last_sync timestamp + any
                # initialized/sync_complete flags the sync just wrote.
                try: self._reload_config()
                except Exception as e: _log.debug("swallowed: %s", e)
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
                def _maybe_drain_redwnl():
                    try:
                        with self._redwnl_lock:
                            pending = list(self._redwnl_pending)
                        if not pending:
                            return
                        # Fire the existing chain-worker entry point.
                        # chan_redownload with an existing-pending
                        # list spawns the worker if no sync is running.
                        first = pending[0]
                        ch = first.get("ch") or {}
                        new_res = first.get("new_res") or "best"
                        # Pop the head; chan_redownload re-appends.
                        with self._redwnl_lock:
                            if self._redwnl_pending:
                                self._redwnl_pending.pop(0)
                        self.chan_redownload(ch, new_res)
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                try: threading.Timer(0.6, _maybe_drain_redwnl).start()
                except Exception as e: _log.debug("swallowed: %s", e)
                # Scheduled second push AFTER this thread's finally
                # actually returns. Without this, _on_queue_changed
                # runs while we're still inside _run, so
                # `self._sync_thread.is_alive()` reads True and the
                # Sync Tasks icon keeps blinking after the queue
                # finishes. this was reported The Timer fires
                # 500ms later when the thread has definitely exited.
                try: threading.Timer(0.5, self._on_queue_changed).start()
                except Exception as e: _log.debug("swallowed: %s", e)
        self._sync_thread = threading.Thread(target=_run, daemon=True)
        self._sync_thread.start()
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
        removed = 0
        try:
            removed = self._queues.sync_clear()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        self._sync_cancel.set()
        self._on_queue_changed()
        return {"ok": True, "removed": removed}


    def sync_force_stop(self):
        """Hard stop: clear queue AND kill any in-flight child subprocesses.

        Soft cancel (`sync_clear_queue`) sets the cancel event but the
        worker only notices between subprocess output lines. yt-dlp's
        flat-playlist call against a 10k-video channel can sit blocked
        for 5-10 minutes on a single fetch with no output, so soft
        cancel feels broken — the queue clears visually but you stare
        at the running task forever.

        This API also walks `psutil.Process(os.getpid()).children(recursive=True)`
        and kills every yt-dlp / ffmpeg / ffprobe child. The worker
        thread sees its subprocess died, returns from the call with
        whatever partial output it got, checks `_sync_cancel` (set
        below), and bails out of the task loop immediately.

        Returns {ok, removed, killed} so the UI can toast a useful
        message ("Stopped — 12 queued cleared, 3 subprocesses killed.").
        """
        removed = 0
        try:
            removed = self._queues.sync_clear()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        # Drain the redownload pending list too (same as sync_cancel
        # does — without this a queued redownload chain would silently
        # resume on the next loop iteration). Same lock protection as
        # sync_cancel.
        try:
            with self._redwnl_lock:
                self._redwnl_pending.clear()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        self._sync_cancel.set()
        killed = 0
        try:
            import psutil
            _names_to_kill = ("yt-dlp", "yt-dlp.exe",
                              "ffmpeg", "ffmpeg.exe",
                              "ffprobe", "ffprobe.exe")
            _us = psutil.Process(os.getpid())
            # recursive=True walks the whole descendant tree, so
            # grandchildren spawned by yt-dlp itself also get cleaned up.
            for _child in _us.children(recursive=True):
                try:
                    _nm = (_child.name() or "").lower()
                except Exception:
                    _nm = ""
                if _nm not in _names_to_kill:
                    continue
                try:
                    _child.kill()
                    killed += 1
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            # Wait briefly so the worker thread can observe the dead
            # subprocess and unwind before we report success.
            try:
                _gone, _alive = psutil.wait_procs(
                    [c for c in _us.children(recursive=True)
                     if (c.name() or "").lower() in _names_to_kill],
                    timeout=1.5)
                # Anything still alive after kill+wait is unusual but
                # not fatal — log it and let the worker take care of it
                # at its next subprocess.run boundary.
            except Exception as e:
                _log.debug("swallowed: %s", e)
        except Exception as e:
            try:
                self._log_stream.emit_error(
                    f"force-stop: psutil walk failed: {e}")
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
        removed = 0
        try:
            removed = self._queues.gpu_clear()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        try:
            self._transcribe.cancel_all()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        # Belt-and-suspenders: also do the same defensive cleanup as
        # gpu_skip_current so a wedged worker can't leave a phantom
        # "running" row in the popover or a journal entry that comes
        # back on restart. See gpu_skip_current docstring for rationale.
        try:
            self._transcribe.skip_current()  # kills subprocess if any
        except Exception as e:
            _log.debug("swallowed: %s", e)
        try:
            self._queues.set_current_gpu(None)
        except Exception as e:
            _log.debug("swallowed: %s", e)
        try:
            self._transcribe.drop_running_from_journal()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        self._on_queue_changed()
        return {"ok": True, "removed": removed}


    def sync_enqueue_all_channels(self):
        """Append every subscribed channel to the sync queue without
        starting the worker. Right-click on Sync Subbed: "add to end of
        queue". Dedupe is handled by `sync_enqueue` (kind+url key), so
        channels already queued or currently running are skipped.
        Returns {ok, queued, skipped, total_queued}.
        """
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
            return {"ok": False, "error": str(e)}


    def sync_prefetch_channel(self, identity):
        """Probe a channel for total video + live counts before sync starts.
        Best-effort — returns {ok, total, lives, upcoming}.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        def _run():
            try:
                r = sync_backend.prefetch_channel_total(ch.get("url", ""))
                if r.get("ok"):
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
        threading.Thread(target=_run, daemon=True).start()
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


    def sync_skip_current(self):
        """Skip the currently-running sync item and advance to the next.

        Sets a skip flag that the sync loop polls on each channel iteration,
        and also sets the cancel event so the in-flight yt-dlp subprocess for
        the current channel terminates promptly. The sync worker then clears
        the cancel event and moves on to the next channel.
        """
        try:
            self._sync_skip.set()
            # Kill the current yt-dlp process cleanly — the sync loop sees
            # the skip flag and clears the cancel event before the next one.
            self._sync_cancel.set()
            self._log_stream.emit([
                ["[Sync] ", "sync_bracket"],
                ["Skip current channel \u2014 moving on\n", "simpleline"],
            ])
            self._log_stream.flush()
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def gpu_skip_current(self):
        """Skip the currently-running GPU (transcribe / compress / metadata)
        job and advance to the next one.

        Belt-and-suspenders: in addition to signaling cancel + killing
        the subprocess (the worker's normal cleanup path), we also
        immediately clear `queues.current_gpu` and forcibly omit the
        running job from the pending journal. If the worker is healthy
        and reaches its own `finally` block, those overwrites are
        redundant. If the worker is hung \u2014 which is precisely when the
        user is clicking Cancel \u2014 the popover updates immediately AND
        the task doesn't resurrect from the journal on next launch.
        """
        try:
            # 1. Normal cancel \u2014 fire the cancel event + kill subprocess.
            self._transcribe.skip_current()
            # 2. Force-clear the running-slot in the popover so the user
            #    sees immediate feedback instead of waiting on whatever
            #    the worker is doing. Idempotent with the worker's own
            #    set_current_gpu(None) in its finally block.
            try:
                self._queues.set_current_gpu(None)
            except Exception as e:
                _log.debug("swallowed: %s", e)
            # 3. Pre-emptively rewrite the pending journal without the
            #    running job. If the worker recovers and reaches its
            #    own _persist_pending(), this gets re-overwritten with
            #    the same content. If the worker hangs forever, this
            #    ensures the task doesn't come back on restart.
            try:
                self._transcribe.drop_running_from_journal()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            self._log_stream.emit([
                ["[GPU] ", "trans_bracket"],
                ["Skip current GPU job \u2014 moving on\n", "simpleline"],
            ])
            self._log_stream.flush()
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def sync_defer_current(self):
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
            if cur:
                deferred = dict(cur)
                deferred.pop("_pass_start_ts", None)
                # Drop any pre-existing queued entry with the same URL
                # FIRST so the dedupe inside sync_enqueue can't skip
                # our append. Without this, defer could become a no-op
                # (existing queue entry stays at its old position) and
                # "send to END of queue" became "may or may not append"
                # (audit: sync_mixin H18).
                try:
                    self._queues.sync_remove(deferred.get("url") or "")
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                self._queues.sync_enqueue(deferred)
                self._log_stream.emit([
                    ["[Sync] ", "sync_bracket"],
                    [(f"Deferred {deferred.get('name') or deferred.get('url') or 'current job'}"
                      " \u2014 sent to end of queue\n"), "simpleline"],
                ])
            # Now skip the in-flight run so the next queued item starts.
            return self.sync_skip_current()
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def gpu_defer_current(self):
        """Send the currently-running GPU task to the END of the GPU
        queue, then cancel the running job. See sync_defer_current
        rationale.
        """
        try:
            cur = self._queues.current_gpu
            if cur:
                deferred = dict(cur)
                self._queues.gpu_enqueue(deferred)
                self._log_stream.emit([
                    ["[GPU] ", "trans_bracket"],
                    [(f"Deferred {deferred.get('title') or deferred.get('path') or 'current job'}"
                      " \u2014 sent to end of queue\n"), "simpleline"],
                ])
            return self.gpu_skip_current()
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def sync_one_channel(self, identity):
        """Sync just one channel (used by context-menu 'Sync now').

        if a sync is already running, enqueue this channel
        on the existing sync worker rather than erroring out. The user
        expects right-click → Sync now to "just add it to the queue"
        whether or not sync is idle.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        if self.sync_is_running():
            # Hand off to the live sync worker as a queued item. The
            # worker picks up new channels off self._queues.sync between
            # current passes.
            try:
                added = bool(self._queues.sync_enqueue(ch))
            except Exception as e:
                return {"ok": False, "error": f"Could not queue: {e}"}
            try: self._on_queue_changed()
            except Exception as e: _log.debug("swallowed: %s", e)
            ch_name = ch.get("name") or ch.get("folder", "")
            return {"ok": True, "queued": added, "name": ch_name}
        if not sync_backend.find_yt_dlp():
            return {"ok": False, "error": "yt-dlp not found"}
        self._sync_cancel.clear()
        self._sync_pause.clear()
        ch_name = ch.get("name") or ch.get("folder", "")
        try:
            if getattr(self, "_tray", None):
                self._tray.start_spin("blue")
                self._tray.set_tooltip(f"YT Archiver \u2014 Syncing {ch_name}")
        except Exception as e:
            _log.debug("swallowed: %s", e)
        def _run():
            # Mirror sync_start_all's visual framing for single-channel
            # syncs: start-of-pass header, [1/1] live row, sync_channel
            # call, [1/1] done row, end-of-pass footer. Without this the
            # manual "Sync now" flow silently ran and the user never saw
            # the usual "[1/1] Name — no new videos" line they expected.
            import time as _t

            from backend.sync import (
                _ROW_EMIT_PASS_ID,
                _fmt_duration,
                _new_pass_id,
                _short_summary,
                _sync_row_emit,
            )
            # Unique pass id so this channel's [1/1] row doesn't replace
            # a prior pass's [1/1] row in the scrollback (same bug class
            # as the autorun sync_all collision).
            _ROW_EMIT_PASS_ID.id = _new_pass_id()
            t0 = _t.time()
            try:
                self._log_stream.emit([
                    ["=== Sync pass starting ", "header"],
                    ["(1 channel) ===\n", "header"],
                ])
                _sync_row_emit(self._log_stream, 1, 1, ch_name)
                res = sync_backend.sync_channel(
                    ch, self._log_stream, self._sync_cancel,
                    queues=self._queues,
                    transcribe_mgr=self._transcribe,
                    pause_event=self._sync_pause,
                    pass_idx=1, pass_total=1,
                ) or {}
                _dl = int(res.get("downloaded", 0) or 0)
                _err = int(res.get("errors", 0) or 0)
                _sync_row_emit(
                    self._log_stream, 1, 1, ch_name,
                    summary=_short_summary(_dl, _err),
                    name_tag="simpleline_green" if _dl > 0 else "simpleline",
                    summary_tag="simpleline_green" if _dl > 0 else "dim",
                )
                self._log_stream.emit([
                    ["\n=== Pass complete: ", "header"],
                    [f"{_dl} downloaded \u00b7 {_err} errors \u00b7 took "
                     f"{_fmt_duration(_t.time() - t0)} ===\n", "header"],
                ])
            except Exception as e:
                self._log_stream.emit_error(f"Sync crashed: {e}")
            finally:
                try: _ROW_EMIT_PASS_ID.id = ""
                except Exception as e: _log.debug("swallowed: %s", e)
                try:
                    if getattr(self, "_tray", None):
                        self._tray.stop_spin()
                        self._tray.set_tooltip("YT Archiver \u2014 Idle")
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                # Clear stale sync-progress. Single-channel sync was the
                # bug path — sync_channel writes progress but never cleared
                # on its own, leaving a companion display stuck on the
                # Sync screen. (OLD's _clear_sync_progress; YTArchiver.py:19671)
                try: sync_backend.clear_sync_progress()
                except Exception as e: _log.debug("swallowed: %s", e)
                self._log_stream.flush()
                self._on_queue_changed()
                # Reset autorun countdown so it doesn't keep showing
                # "Syncing..." now that this single-channel sync finished.
                try: self._autorun.notify_sync_done()
                except Exception as e: _log.debug("swallowed: %s", e)
                # Delayed second push so the Sync Tasks icon stops
                # blinking after a single-channel sync finishes
                # (same rationale as sync_start_all's fix).
                try: threading.Timer(0.5, self._on_queue_changed).start()
                except Exception as e: _log.debug("swallowed: %s", e)
        self._sync_thread = threading.Thread(target=_run, daemon=True)
        self._sync_thread.start()
        self._on_queue_changed()
        return {"ok": True, "started": True}
