"""
MediaOpsMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class MediaOpsMixin:

    def archive_rescan(self):
        """Run the startup disk-sweep on demand — picks up files added
        manually or while the app was offline. Also prunes DB entries
        whose files no longer exist (fixes stale `(1)` duplicates and
        any yt-dlp intermediate rows that got indexed before the
        `.fNNN-X` filter landed).
        """
        def _run():
            try:
                cfg = self._config or load_config()
                output_dir = (cfg.get("output_dir") or "").strip()
                if not output_dir:
                    self._log_stream.emit_error("No output_dir configured.")
                    return
                # Step 1: prune DB entries for files no longer on disk
                # / 0-byte phantoms / duplicate-id rows. Emit before and
                # after so the user sees it's doing something — # "I click Rescan, nothing happens, then 5 min later
                # nothing changed."
                self._log_stream.emit_text(
                    "Rescan: pruning stale DB entries...",
                    "simpleline_blue")
                self._log_stream.flush()
                pruned = index_backend.prune_missing_videos()
                if (pruned.get("videos_removed") or pruned.get("duplicate_id")
                        or pruned.get("fake_id_cleared")):
                    _parts = []
                    if pruned.get("missing"):
                        _parts.append(f"{pruned['missing']} missing file(s)")
                    if pruned.get("zero_byte"):
                        _parts.append(f"{pruned['zero_byte']} 0-byte phantom(s)")
                    if pruned.get("duplicate_id"):
                        _parts.append(
                            f"{pruned['duplicate_id']} duplicate(s) flagged")
                    if pruned.get("fake_id_cleared"):
                        _parts.append(
                            f"{pruned['fake_id_cleared']} fake video_id(s) cleared")
                    self._log_stream.emit_text(
                        " \u2014 Pruned: " + ", ".join(_parts) + ".",
                        "simpleline_green")
                else:
                    self._log_stream.emit_text(
                        " \u2014 No stale entries to prune.", "dim")
                self._log_stream.flush()
                # Step 2: sweep for new files.
                channels = cfg.get("channels", [])
                self._log_stream.emit_text(
                    f"Rescan: scanning {len(channels)} channel folder(s) "
                    f"for new files...", "simpleline_blue")
                self._log_stream.flush()
                sweep = index_backend.sweep_new_videos(output_dir, channels)
                self._log_stream.emit_text(
                    f"\u2014 Rescan complete: "
                    f"+{sweep.get('registered', 0)} videos, "
                    f"+{sweep.get('ingested', 0)} transcripts ingested.",
                    "simpleline_green")
                # Push a refresh signal to the frontend so the Browse
                # grid re-queries — the backend-side cache is already
                # invalidated but the currently-rendered grid is still
                # HTML from the last fetch. "the videos are
                # still there after rescan."
                if self._window is not None:
                    try:
                        self._window.evaluate_js(
                            "if (window._onArchiveRescanComplete) "
                            "window._onArchiveRescanComplete();")
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
            except Exception as e:
                self._log_stream.emit_error(f"Rescan failed: {e}")
            finally:
                self._log_stream.flush()
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}


    def archive_repair_hidden_sidecars(self):
        """Settings tool: walk the whole archive and set the Windows
        HIDDEN attribute on any sidecar that is currently visible —
        enforcing the contract that each archive folder shows only the
        videos + the conjoined Transcript.txt when Explorer's 'show
        hidden files' is off. Catches .info.json (incl. yt-dlp's
        double-dot `Title..info.json`), .description, stray thumbnails,
        .jsonl sidecars, .last_attempt state, etc.

        Idempotent and safe to run any time — already-hidden files are
        skipped with no extra syscall. Streams per-channel progress + a
        final tally to the main log. Runs in the background.
        """
        if getattr(self, "_hide_repair_running", False):
            return {"ok": True, "already_running": True}
        self._hide_repair_running = True

        def _run():
            try:
                from .. import utils as _u
                cfg = self._config or load_config()
                output_dir = (cfg.get("output_dir") or "").strip()
                if not output_dir or not os.path.isdir(output_dir):
                    self._log_stream.emit_error(
                        "Hidden-sidecar repair: no valid output_dir configured.")
                    return
                channels = sorted(
                    cfg.get("channels", []) or [],
                    key=lambda c: (c.get("name") or c.get("folder") or "").lower())
                self._log_stream.emit_text(
                    f"Hidden-sidecar repair: scanning {len(channels)} channel "
                    f"folder(s) for visible sidecars…", "simpleline_blue")
                self._log_stream.flush()
                total_hidden = 0
                folders_touched = 0
                seen_dirs = set()
                for ch in channels:
                    name = (ch.get("name") or ch.get("folder") or "").strip()
                    if not name:
                        continue
                    folder = os.path.join(output_dir, name)
                    if not os.path.isdir(folder):
                        continue
                    key = os.path.normcase(os.path.normpath(folder))
                    if key in seen_dirs:
                        continue
                    seen_dirs.add(key)
                    try:
                        n = _u.hide_stray_sidecars(folder, recursive=True)
                    except Exception as _pe:
                        self._log_stream.emit_error(
                            f"  - {name}: scan failed ({_pe})")
                        continue
                    if n > 0:
                        total_hidden += n
                        folders_touched += 1
                        self._log_stream.emit_text(
                            f"  - {name}: hid {n} sidecar(s)", "simpleline_green")
                        self._log_stream.flush()
                # Loose singles + anything directly under the archive root
                # (non-recursive — channel subfolders were handled above).
                try:
                    n_root = _u.hide_stray_sidecars(output_dir, recursive=False)
                    if n_root > 0:
                        total_hidden += n_root
                        folders_touched += 1
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                if total_hidden > 0:
                    self._log_stream.emit_text(
                        f"— Hidden-sidecar repair complete: hid "
                        f"{total_hidden} previously-visible sidecar(s) across "
                        f"{folders_touched} folder(s). Archive folders now "
                        f"show only videos + transcripts.", "simpleline_green")
                else:
                    self._log_stream.emit_text(
                        "— Hidden-sidecar repair: all clean — every "
                        "sidecar is already hidden.", "simpleline_green")
                self._log_stream.flush()
            except Exception as e:
                self._log_stream.emit_error(f"Hidden-sidecar repair failed: {e}")
            finally:
                self._hide_repair_running = False
                self._log_stream.flush()

        threading.Thread(target=_run, daemon=True,
                         name="hide-sidecar-repair").start()
        return {"ok": True, "started": True}


    # ─── Video-length backfill (manual local ffprobe pass) ────────────

    def video_lengths_missing_count(self):
        """How many archived videos have no stored length yet. Read-only;
        the Settings 'Check / fix video lengths' button calls this first to
        show the count before kicking off the (potentially long) pass."""
        try:
            from ..metadata.core import count_missing_durations
            return {"ok": True, "missing": int(count_missing_durations())}
        except Exception as e:
            return {"ok": False, "error": str(e), "missing": 0}

    def video_lengths_backfill_running(self):
        """Whether a backfill pass is active (so the button can show Stop vs
        Check/fix even after a page reload)."""
        return {"ok": True,
                "running": bool(getattr(self, "_dur_backfill_running", False))}

    def video_lengths_backfill_start(self):
        """Fill every missing video length by ffprobing the files locally
        (no YouTube). Runs in the background; progress streams to the main
        log; stop via video_lengths_backfill_cancel. Idempotent / resumable
        — only touches rows still missing a length, so a stopped run
        continues where it left off next start."""
        if getattr(self, "_dur_backfill_running", False):
            return {"ok": True, "already_running": True}
        self._dur_backfill_running = True
        self._dur_backfill_cancel = threading.Event()

        def _run():
            try:
                from ..metadata.core import backfill_missing_durations
                res = backfill_missing_durations(
                    self._log_stream, self._dur_backfill_cancel)
                # Always notify the UI on completion so the Settings button
                # flips back to idle; pass the count filled so the frontend
                # can refresh the Videos grid only when something changed.
                if self._window is not None:
                    try:
                        _n = int(res.get("resolved") or 0)
                        self._window.evaluate_js(
                            "if(window._onVideoLengthsBackfilled)"
                            f"window._onVideoLengthsBackfilled({_n});")
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
            except Exception as e:
                try:
                    self._log_stream.emit_error(f"Video-length fix failed: {e}")
                except Exception as _e2:
                    _log.debug("swallowed: %s", _e2)
            finally:
                self._dur_backfill_running = False
                try:
                    self._log_stream.flush()
                except Exception as e:
                    _log.debug("swallowed: %s", e)

        threading.Thread(target=_run, name="dur-backfill", daemon=True).start()
        return {"ok": True, "started": True}

    def video_lengths_backfill_cancel(self):
        """Stop an in-progress video-length fix. Lengths filled so far are
        kept; re-running resumes the rest."""
        ev = getattr(self, "_dur_backfill_cancel", None)
        if ev is not None:
            ev.set()
        return {"ok": True,
                "running": bool(getattr(self, "_dur_backfill_running", False))}


    # ─── Transcript drift scan (feature H-2) ──────────────────────────

    def reorg_cancel(self):
        """Stop an in-progress folder reorganization at its next checkpoint."""
        ev = getattr(self, "_reorg_cancel", None)
        if ev is not None:
            ev.set()
        return {"ok": True,
                "running": bool(getattr(self, "_reorg_running", False))}


    def drift_scan_channel(self, identity):
        """Scan one channel's transcript files for drift between the
        aggregated .txt, hidden .jsonl, and FTS index.

        Spawns a background worker so the js_api thread doesn't freeze
        the UI while we walk three sources for a large channel (audit:
        media_ops_mixin.py:93). Returns a token immediately; caller
        polls drift_scan_channel_poll. The synchronous behavior
        (return the scan result directly) is preserved when called
        from worker context — detect via thread name.
        """
        from backend import drift_scan as _ds
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        cfg = self._config or load_config()
        output_dir = (cfg.get("output_dir") or "").strip()
        if not output_dir:
            return {"ok": False, "error": "output_dir is not configured"}
        # Worker thread + token-poll pattern. Same shape as the other
        # async js_api methods (chan_scan_resolution_mismatch et al.).
        import uuid as _uuid
        token = _uuid.uuid4().hex
        if not hasattr(self, "_drift_scan_results"):
            self._drift_scan_results = {}
        if not hasattr(self, "_drift_scan_lock"):
            self._drift_scan_lock = threading.Lock()
        # Sweep abandoned entries (>10 min) so the dict can't grow
        # unbounded if user navigates away mid-scan (audit:
        # media_ops_mixin H10).
        import time as _t_mod
        _now_ts = _t_mod.time()
        with self._drift_scan_lock:
            _stale = [k for k, v in self._drift_scan_results.items()
                      if isinstance(v, dict)
                      and (_now_ts - (v.get("_ts") or _now_ts)) > 600]
            for k in _stale:
                self._drift_scan_results.pop(k, None)
            self._drift_scan_results[token] = {
                "ok": True, "pending": True, "_ts": _now_ts}
        def _run():
            try:
                res = _ds.scan_channel(ch, output_dir)
                if isinstance(res, dict):
                    res["_ts"] = _t_mod.time()
                with self._drift_scan_lock:
                    self._drift_scan_results[token] = res
            except Exception as e:
                with self._drift_scan_lock:
                    self._drift_scan_results[token] = {
                        "ok": False, "error": str(e),
                        "_ts": _t_mod.time()}
        threading.Thread(target=_run, daemon=True,
                         name="drift-scan-channel").start()
        return {"ok": True, "pending": True, "token": token}

    def drift_scan_channel_poll(self, token):
        """Poll the drift_scan_channel worker by token. Returns
        {ok, pending} while running, the full scan result once done."""
        lock = getattr(self, "_drift_scan_lock", None)
        results = getattr(self, "_drift_scan_results", {})
        if lock is None:
            return {"ok": False, "error": "unknown token"}
        with lock:
            res = results.get(token)
            if res is None:
                return {"ok": False, "error": "unknown token"}
            if res.get("pending"):
                return {"ok": True, "pending": True}
            try: del results[token]
            except KeyError: pass
            return res


    def drift_apply_channel(self, identity):
        """Apply the three drift fixes for one channel:
          A. Queue Whisper retranscribe for each TXT-without-JSONL entry
             whose video file can be located in the FTS videos table.
          B. Reconstruct TXT entries from .jsonl segments for each
             JSONL-without-TXT entry (body = concat of segment text,
             date = .jsonl mtime, src_tag = "RECOVERED-FROM-JSONL").
          C. Rebuild FTS if phantom count > 0.

        Runs a fresh scan internally so the apply always acts on current
        state."""
        from backend import drift_scan as _ds
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        cfg = self._config or load_config()
        output_dir = (cfg.get("output_dir") or "").strip()
        if not output_dir:
            return {"ok": False, "error": "output_dir is not configured"}

        # Hook: queue a Whisper retranscribe. Wraps self.transcribe_retranscribe
        # so the drift_scan module stays decoupled from the Api class.
        def _enqueue_retranscribe(filepath, title, video_id):
            self.transcribe_retranscribe(filepath, title, video_id)

        # Spawn the apply on a worker thread + token-poll. drift_apply
        # walks every drift entry and calls _enqueue_retranscribe per
        # entry; on a large channel this blocked the js_api thread for
        # multiple seconds (audit: media_ops_mixin.py:140). Same
        # pattern as drift_scan_channel above.
        import uuid as _uuid
        token = _uuid.uuid4().hex
        if not hasattr(self, "_drift_apply_results"):
            self._drift_apply_results = {}
        if not hasattr(self, "_drift_apply_lock"):
            self._drift_apply_lock = threading.Lock()
        # TTL sweep (audit: media_ops_mixin H10).
        import time as _t_mod
        _now_ts = _t_mod.time()
        with self._drift_apply_lock:
            _stale = [k for k, v in self._drift_apply_results.items()
                      if isinstance(v, dict)
                      and (_now_ts - (v.get("_ts") or _now_ts)) > 600]
            for k in _stale:
                self._drift_apply_results.pop(k, None)
            self._drift_apply_results[token] = {
                "ok": True, "pending": True, "_ts": _now_ts}
        def _run():
            try:
                result = _ds.apply_channel(
                    ch, output_dir,
                    enqueue_retranscribe_fn=_enqueue_retranscribe,
                    rebuild_fts_fn=_ds.rebuild_fts_index)
                if result.get("ok"):
                    a = result.get("actions", {})
                    parts = []
                    if a.get("txt_reconstructed"):
                        parts.append(f"{a['txt_reconstructed']} .txt rebuilt")
                    if a.get("retranscribe_queued"):
                        parts.append(f"{a['retranscribe_queued']} queued for Whisper")
                    if a.get("retranscribe_skipped"):
                        parts.append(f"{a['retranscribe_skipped']} skipped (video file missing)")
                    if a.get("fts_rebuilt"):
                        parts.append("FTS rebuilt")
                    ch_name = ch.get("name") or ch.get("folder", "")
                    if parts:
                        self._log_stream.emit_text(
                            f" \u2014 Drift fix for {ch_name}: "
                            f"{' \u00b7 '.join(parts)}.", "simpleline_pink")
                    else:
                        self._log_stream.emit_text(
                            f" \u2014 Drift fix for {ch_name}: no actions taken.",
                            "dim")
                    self._log_stream.flush()
                    if a.get("retranscribe_queued", 0) > 0:
                        # Surface a breadcrumb when the autostart
                        # loses the lock race against another caller
                        # (UI Sync click, autorun, etc.) so the user
                        # has a clue why "drift fixed + queued" was
                        # followed by no sync activity (audit:
                        # media_ops_mixin H21).
                        if not self._maybe_autostart_sync():
                            try:
                                self._log_stream.emit_dim(
                                    " — (autostart skipped — another sync "
                                    "start is already in flight)")
                            except Exception:
                                pass
                with self._drift_apply_lock:
                    self._drift_apply_results[token] = result
            except Exception as e:
                with self._drift_apply_lock:
                    self._drift_apply_results[token] = {"ok": False, "error": str(e)}
        threading.Thread(target=_run, daemon=True,
                         name="drift-apply-channel").start()
        return {"ok": True, "pending": True, "token": token}


    def drift_apply_channel_poll(self, token):
        """Poll the drift_apply_channel worker by token. Returns
        {ok, pending} while running, the full apply result once done."""
        lock = getattr(self, "_drift_apply_lock", None)
        results = getattr(self, "_drift_apply_results", {})
        if lock is None:
            return {"ok": False, "error": "unknown token"}
        with lock:
            res = results.get(token)
            if res is None:
                return {"ok": False, "error": "unknown token"}
            if res.get("pending"):
                return {"ok": True, "pending": True}
            try: del results[token]
            except KeyError: pass
            return res


    # ─── Repair YT auto-captions (v64.7 parser fix) ────────────────────

    def repair_yt_captions(self, payload):
        """Queue a Repair YT auto-captions task on the sync queue.

        The task serializes alongside downloads, metadata refreshes,
        and other YT-hitting work so we never run multiple yt-dlp
        processes against YouTube in parallel. The user sees it in
        the Sync Tasks popover and can pause/cancel like any other
        task. Progress streams to the main log.

        payload keys (all optional):
          channel: channel folder name to limit scope; "" = all channels
          video_id: single video to repair (overrides channel)
          dry_run: bool — fetch + parse but don't write anything

        Both YT CAPTIONS and YT+PUNCTUATION sources are candidates. A
        per-video downgrade guard inside repair_captions skips any
        YT+PUNCTUATION video where YT's current VTT is still lowercase
        (re-parsing would strip the restored punctuation across the
        whole transcript — worse than the bug we're fixing).
        """
        cfg = self._config or load_config()
        output_dir = (cfg.get("output_dir") or "").strip()
        if not output_dir:
            return {"ok": False, "error": "output_dir is not configured"}
        payload = payload or {}
        channel = (payload.get("channel") or "").strip()
        video_id = (payload.get("video_id") or "").strip()
        dry_run = bool(payload.get("dry_run"))

        # The popover and downstream dispatch identify tasks by url +
        # kind, so we build a stable, unique-per-scope synthetic url.
        if video_id:
            scope_name = f"video {video_id}"
            scope_url = f"repair:video:{video_id}"
        elif channel:
            scope_name = channel
            scope_url = f"repair:channel:{channel}"
        else:
            scope_name = "All channels"
            scope_url = "repair:all"

        task = {
            "kind": "repair_yt_captions",
            "name": scope_name,
            "folder": scope_name,
            "url": scope_url,
            "channel_folder": channel or None,
            "video_id": video_id or None,
            "dry_run": dry_run,
        }
        try:
            queued = self._queues.sync_enqueue(task)
        except Exception as e:
            return {"ok": False, "error": str(e)}
        if not queued:
            # Don't pair ok:True with an `error` field — that mixed
            # contract used to trip JS callers into rendering both
            # a success AND an error toast (audit: media_ops_mixin.
            # py:236 + :295). Surface dedupe as a distinct `duplicate`
            # flag so callers branch cleanly.
            return {"ok": True, "queued": False, "duplicate": True,
                    "reason": "Already queued for this scope"}
        self._on_queue_changed()
        started = self._maybe_autostart_sync()
        return {"ok": True, "queued": True, "started": started,
                "paused": bool(self._queues.sync_paused),
                "scope": scope_name, "dry_run": dry_run}


    # ─── Restore transcript punctuation (v66.3 follow-up to v64.7 repair) ─

    def punct_restore_segments(self, payload):
        """Queue a Restore transcript punctuation task on the sync queue.

        Walks the archive's per-segment text for YT-captioned videos and
        runs each segment through the punctuation-restoration model so
        the right-panel transcript reads as proper sentences instead of
        a lowercase wall of text. No YT calls — pure local CPU/GPU work.
        Serializes on the sync queue so it doesn't compete with an
        in-flight download / repair pass for the punct model's GPU slot.

        payload keys (all optional):
          channel: channel folder name to limit scope; "" = all channels
          video_id: single video to punctuate (overrides channel)
          dry_run: bool — load the model + parse but don't write
        """
        cfg = self._config or load_config()
        output_dir = (cfg.get("output_dir") or "").strip()
        if not output_dir:
            return {"ok": False, "error": "output_dir is not configured"}
        payload = payload or {}
        channel = (payload.get("channel") or "").strip()
        video_id = (payload.get("video_id") or "").strip()
        dry_run = bool(payload.get("dry_run"))

        if video_id:
            scope_name = f"video {video_id}"
            scope_url = f"punct:video:{video_id}"
        elif channel:
            scope_name = channel
            scope_url = f"punct:channel:{channel}"
        else:
            scope_name = "All channels"
            scope_url = "punct:all"

        task = {
            "kind": "punct_restore",
            "name": scope_name,
            "folder": scope_name,
            "url": scope_url,
            "channel_folder": channel or None,
            "video_id": video_id or None,
            "dry_run": dry_run,
        }
        try:
            queued = self._queues.sync_enqueue(task)
        except Exception as e:
            return {"ok": False, "error": str(e)}
        if not queued:
            # Don't pair ok:True with an `error` field — that mixed
            # contract used to trip JS callers into rendering both
            # a success AND an error toast (audit: media_ops_mixin.
            # py:236 + :295). Surface dedupe as a distinct `duplicate`
            # flag so callers branch cleanly.
            return {"ok": True, "queued": False, "duplicate": True,
                    "reason": "Already queued for this scope"}
        self._on_queue_changed()
        started = self._maybe_autostart_sync()
        return {"ok": True, "queued": True, "started": started,
                "paused": bool(self._queues.sync_paused),
                "scope": scope_name, "dry_run": dry_run}


    # ─── Compress dry-run (feature F8) ─────────────────────────────────

    def compress_dry_run(self, output_res="720"):
        """Project how much disk space compression WOULD save if enabled
        globally at the given output_res. Walks the index DB (no
        ffprobe), aggregating each channel's total video count +
        cumulative duration, then computes projected post-compress
        size for each of the three quality tiers.

        Returns {
          ok, output_res,
          channels: [{name, videos, hours, current_gb, generous_gb,
                       average_gb, below_gb}],
          total: {videos, hours, current_gb, generous_gb, average_gb, below_gb}
        }
        Purely read-only; does not modify anything.
        """
        try:
            from backend import compress as _cpx
            from backend import index as _idx
            # Use the reader connection so this aggregate SELECT doesn't
            # queue behind sweep / ingest_jsonl writers during startup.
            rconn = _idx._reader_open()
            if rconn is None:
                return {"ok": False, "error": "Index DB unavailable"}
            presets = _cpx._COMPRESS_PRESETS.get(str(output_res))
            if not presets:
                return {"ok": False,
                        "error": f"No compress preset for output_res={output_res!r}"}
            # Aggregate per-channel: videos + duration + size. Duration
            # may be NULL for older rows — treat those as 0 hours so
            # they don't inflate projected savings (worst-case the real
            # savings are larger than reported).
            with _idx._reader_lock:
                rows = rconn.execute(
                    "SELECT channel, COUNT(*), "
                    "       COALESCE(SUM(duration_s), 0), "
                    "       COALESCE(SUM(size_bytes), 0) "
                    "FROM videos "
                    "WHERE is_duplicate_of IS NULL "
                    "GROUP BY channel "
                    "ORDER BY SUM(size_bytes) DESC"
                ).fetchall()
            # Per-channel projection.
            out_channels = []
            tot_videos = 0
            tot_hours = 0.0
            tot_current = 0.0
            tot_gen = 0.0
            tot_avg = 0.0
            tot_below = 0.0
            for name, n, dur_s, bytes_ in rows:
                hours = float(dur_s) / 3600.0 if dur_s else 0.0
                current_gb = float(bytes_) / (1024 ** 3) if bytes_ else 0.0
                # MB/hr → GB for the whole channel at each tier
                gen_gb = (presets["Generous"] * hours) / 1024
                avg_gb = (presets["Average"] * hours) / 1024
                below_gb = (presets["Below Average"] * hours) / 1024
                out_channels.append({
                    "name": name or "(unknown)",
                    "videos": int(n),
                    "hours": round(hours, 1),
                    "current_gb": round(current_gb, 1),
                    "generous_gb": round(gen_gb, 1),
                    "average_gb": round(avg_gb, 1),
                    "below_gb": round(below_gb, 1),
                })
                tot_videos += int(n)
                tot_hours += hours
                tot_current += current_gb
                tot_gen += gen_gb
                tot_avg += avg_gb
                tot_below += below_gb
            return {
                "ok": True,
                "output_res": str(output_res),
                "channels": out_channels,
                "total": {
                    "videos": tot_videos,
                    "hours": round(tot_hours, 1),
                    "current_gb": round(tot_current, 1),
                    "generous_gb": round(tot_gen, 1),
                    "average_gb": round(tot_avg, 1),
                    "below_gb": round(tot_below, 1),
                },
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}


    # ─── Compress / Reorg ──────────────────────────────────────────────

    def compress_video_file(self, filepath, quality="Average", output_res="720"):
        """Queue an AV1 NVENC compression task onto the shared GPU
        queue. rule: the GPU task list is the user's "permission
        to bog down my computer" — so standalone compress must NOT
        fire immediately off a bare thread. It enqueues, then the
        TranscribeManager worker picks it up when Auto is on (or when
        the user manually kicks the queue).
        """
        if not filepath:
            return {"ok": False, "error": "filepath required"}
        try:
            title = os.path.splitext(os.path.basename(filepath))[0]
        except Exception:
            title = filepath
        # Try to derive the channel from the filepath's parent folder
        # for nicer queue labels.
        try:
            channel = os.path.basename(os.path.dirname(filepath))
        except Exception:
            channel = ""
        ok = self._transcribe.compress_enqueue(
            filepath, title=title, channel=channel,
            quality=quality, output_res=output_res)
        return {"ok": bool(ok), "queued": bool(ok)}


    def compress_videos_batch(self, paths, quality="Average", output_res="720",
                              redo_on_larger=True):
        """Queue a list of videos onto the shared GPU queue — one
        compress task per path. Serializes through the same worker as
        transcribe so the GPU isn't slammed with parallel NVENC
        sessions. `redo_on_larger` isn't meaningful at enqueue time
        (it's a per-job retry flag handled inside compress_video), so
        we attach it as a job hint for future use but don't branch on
        it here.
        """
        paths = paths or []
        queued = 0
        for p in paths:
            try:
                title = os.path.splitext(os.path.basename(p))[0]
                channel = os.path.basename(os.path.dirname(p))
            except Exception:
                title, channel = p, ""
            if self._transcribe.compress_enqueue(
                    p, title=title, channel=channel,
                    quality=quality, output_res=output_res):
                queued += 1
        return {"ok": True, "queued": queued, "count": len(paths)}


    def reorg_channel_folder(self, identity, split_years=True, split_months=False,
                             recheck_dates=False):
        """Reorg a channel's folder into year/month subfolders.

        `recheck_dates=True` re-reads .info.json sidecars and fixes file mtimes
        before grouping (matches YTArchiver's Re-check Dates option).
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        from backend.sync import channel_folder_name
        folder = os.path.join(base, channel_folder_name(ch))
        if not hasattr(self, "_sync_start_lock"):
            self._sync_start_lock = threading.Lock()
        _start_lock_acquired = self._sync_start_lock.acquire(blocking=False)
        if not _start_lock_acquired:
            return {"ok": False,
                    "error": "Sync or reorganization is already starting"}
        if getattr(self, "_reorg_running", False):
            try:
                self._sync_start_lock.release()
            except Exception:
                pass
            return {"ok": True, "already_running": True}

        def _release_start_lock():
            try:
                self._sync_start_lock.release()
            except Exception:
                pass

        # Refuse reorg when sync, redownload, or compress is currently
        # writing to this channel's folder. Without this guard, reorg
        # could move files out from under an in-flight yt-dlp download
        # → corrupted partial files, sync errors, orphan .part files.
        ch_name = ch.get("name") or ch.get("folder") or ""
        ch_url = (ch.get("url") or "").strip()
        if ch_name and ch_url:
            try:
                _cur = self._queues.current_sync
                if _cur and _cur.get("url") == ch_url:
                    _release_start_lock()
                    return {"ok": False,
                            "error": f"Sync is currently downloading "
                                     f"{ch_name} — wait for it to finish "
                                     f"before reorganizing."}
                _cur_g = self._queues.current_gpu
                if _cur_g and _cur_g.get("channel") == ch_name:
                    _release_start_lock()
                    return {"ok": False,
                            "error": f"GPU work (transcribe/encode) is "
                                     f"running against {ch_name} — wait "
                                     f"for it to finish before reorganizing."}
            except Exception as e:
                _log.debug("swallowed: %s", e)
        # "Re-apply organization" (Subs > Reorg folder) passes
        # split_years=split_months=None as a sentinel meaning "use THIS
        # channel's configured folder org" rather than a specific layout.
        # Previously bool(None) silently collapsed to False, so Re-apply
        # FLATTENED the channel instead of re-splitting it. Resolve None
        # from the channel's split_years/split_months flags. The explicit
        # Flat (False,False) / Split-by-year (True,...) menu items still
        # pass real booleans and are honored as-is.
        _sy = bool(ch.get("split_years")) if split_years is None else bool(split_years)
        _sm = bool(ch.get("split_months")) if split_months is None else bool(split_months)
        # Reorg gets its OWN cancel event — it used to share
        # self._sync_cancel, which every sync-stop path SETS and only a
        # new sync start CLEARS. Run a reorg after stopping a sync and
        # it aborted at video #1 ("Reorg cancelled") or stopped partway
        # through recheck_dates, leaving the folder half-done.
        _reorg_cancel = threading.Event()
        self._reorg_cancel = _reorg_cancel
        self._reorg_running = True
        def _run():
            try:
                reorg_backend.reorg_channel(folder,
                                            split_years=_sy,
                                            split_months=_sm,
                                            stream=self._log_stream,
                                            cancel_event=_reorg_cancel,
                                            recheck_dates=bool(recheck_dates))
            finally:
                try:
                    self._log_stream.flush()
                finally:
                    try:
                        with self._sync_start_lock:
                            if getattr(self, "_reorg_cancel", None) is _reorg_cancel:
                                self._reorg_cancel = None
                            self._reorg_running = False
                    except Exception:
                        self._reorg_running = False
        try:
            threading.Thread(target=_run, daemon=True).start()
        except Exception as e:
            self._reorg_cancel = None
            self._reorg_running = False
            _release_start_lock()
            return {"ok": False, "error": str(e)}
        _release_start_lock()
        return {"ok": True, "started": True}
