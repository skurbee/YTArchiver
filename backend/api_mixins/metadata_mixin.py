"""
MetadataMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class MetadataMixin:

    def _push_metadata_refresh(self):
        """Trigger a re-render of Settings > Metadata so the `XXm ago`
        timestamps update after a refresh pass completes. Without this,
        the tab keeps showing pre-pass values ("35m ago") even though
        the backend just stamped a fresh `last_views_refresh_ts` —
        user has to click Reload manually.

        Called from the sync worker after every metadata / metadata_
        comments / videoid_backfill task finishes. evaluate_js is
        cheap when the tab isn't visible (JS guard returns quickly),
        so no throttling needed for typical refresh cadence.
        """
        if self._window is None:
            return
        try:
            # Config may have just been re-saved (bulk_refresh_views_likes
            # writes last_views_refresh_ts) — reload so any subsequent
            # js_api calls see the new stamps.
            try: self._reload_config()
            except Exception as e: _log.debug("swallowed: %s", e)
            self._window.evaluate_js(
                "window._refreshMetadataTab && window._refreshMetadataTab();")
        except Exception as e:
            try: self._log_stream.emit_dim(
                f"(metadata tab refresh push failed: {e})")
            except Exception as e: _log.debug("swallowed: %s", e)


    def _push_subs_table_refresh(self):
        """Refresh the Subs tab's channel table so its "Last Sync"
        column reflects per-channel completion as a sync pass advances.
        Without this, the column stayed frozen at boot-time values until
        the user clicked away and back.

        Fired by `sync.active_state.fire_channel_synced_hook` after each
        channel's done-row emit in sync_all. evaluate_js is cheap when
        the tab isn't visible, so no throttling needed at the per-
        channel cadence of a normal sync pass.
        """
        if self._window is None:
            return
        try:
            # The just-finished sync_channel wrote a fresh `last_sync`
            # into the on-disk config — reload so subsequent js_api
            # roundtrips (refreshSubsTable → get_subs_channels) read
            # the new values.
            try: self._reload_config()
            except Exception as e: _log.debug("swallowed: %s", e)
            self._window.evaluate_js(
                "window.refreshSubsTable && window.refreshSubsTable();")
        except Exception as e:
            try: self._log_stream.emit_dim(
                f"(subs tab refresh push failed: {e})")
            except Exception as e: _log.debug("swallowed: %s", e)


    # ─── Metadata (manual "Recheck" from context menu) ──────────────────

    def metadata_recheck_channel(self, identity):
        """Slow-path playlist walk + fetch missing metadata.

        Enqueues a `kind: "metadata"` item on the sync queue so the
        Tasks popover shows it and the Sync pause/cancel buttons can
        pause or clear it. The sync worker loop dispatches metadata
        items to `fetch_channel_metadata`. Matches rule:
        "every channel's metadata check should show as its own sync
        task." If the sync thread isn't already running and Sync Auto
        is on, sync_start_all kicks it off; otherwise the item sits
        queued until the user resumes.

        When pre-existing metadata is detected on disk, pops the 3-button
        dialog OLD YTArchiver uses (Check for New / Refresh Counts / Cancel)
        so the user can pick between fast-skip-existing and slow-refresh-all.
        Matches YTArchiver.py:26669 _metadata_choice_dialog.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}

        # Count pre-existing metadata entries under this channel's folder.
        cfg = self._config or load_config()
        base = (cfg.get("output_dir") or "").strip()
        existing_count = 0
        if base:
            try:
                from backend.metadata import _read_metadata_jsonl as _rmj
                from backend.sync import channel_folder_name as _cfn
                cfolder = os.path.join(base, _cfn(ch))
                for dp, _dns, fns in os.walk(cfolder):
                    for fn in fns:
                        if fn.startswith(".") and fn.endswith("Metadata.jsonl"):
                            existing_count += len(_rmj(os.path.join(dp, fn)))
            except Exception:
                existing_count = 0

        ch_name = ch.get("name") or ch.get("folder", "")

        def _enqueue_task(refresh_mode):
            """Drop a `kind: "metadata"` item on the sync queue and
            fire the sync worker if needed."""
            task = dict(ch)
            task["kind"] = "metadata"
            task["refresh"] = bool(refresh_mode)
            try:
                self._queues.sync_enqueue(task)
            except Exception as e:
                _log.debug("swallowed: %s", e)
            self._on_queue_changed()
            # Kick the sync worker to drain the queue item we just
            # added. Pass `add_downloads_from_config=False` so the
            # worker does NOT enqueue 103 download tasks on top of our
            # metadata task \u2014 rule: "everything should be in
            # the task list; don't add things the user didn't ask for."
            try:
                cfg = load_config() or {}
                if cfg.get("autorun_sync", False) and not self.sync_is_running():
                    self.sync_start_all(add_downloads_from_config=False)
            except Exception as e:
                _log.debug("swallowed: %s", e)

        # If there's existing metadata, prompt the user. Otherwise, just
        # enqueue a normal fetch-new-only pass.
        if existing_count > 0:
            def _prompt_then_enqueue():
                choice = (self._prompt_metadata_already_downloaded(
                    ch_name, existing_count) or {}).get("choice", "skip")
                if choice in ("skip", "cancel"):
                    self._log_stream.emit_text(
                        f" \u2014 Metadata for {ch_name}: cancelled.",
                        "simpleline_pink")
                    self._log_stream.flush()
                    return
                # "append" = Check for New (fast, skip-existing)
                # "overwrite" = Refresh Counts (re-hit every video)
                _enqueue_task(choice == "overwrite")
            threading.Thread(target=_prompt_then_enqueue, daemon=True).start()
        else:
            _enqueue_task(False)
        return {"ok": True, "queued": True}


    # ─── Feature H-14: year-scoped metadata from grid year-head ctx ─────

    def metadata_queue_channel_year(self, identity, year, refresh=False):
        """Queue a year-scoped metadata task for one channel.

        Called from the Browse video-grid year-head right-click menu
        (app.js:4624, parallel to chan_redownload's year scope). Drops
        a `kind: "metadata"` item on the sync queue with
        `scope: {"year": N}` so `fetch_channel_metadata` filters
        on-disk videos to that year before processing. Everything else
        (skip-previously-failed, refresh-counts behavior, Sync Tasks
        popover visibility, pause/cancel) works identically to the
        whole-channel metadata flow — this is just a scope refinement.

        `refresh=False`: fetch metadata for on-disk videos in YEAR that
        don't yet have it (fast, skip-existing).
        `refresh=True`:  re-hit every on-disk video in YEAR to refresh
        views/likes (slow, re-fetch-all).
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        try:
            year_int = int(year)
        except (TypeError, ValueError):
            return {"ok": False, "error": f"Invalid year: {year!r}"}
        task = dict(ch)
        task["kind"] = "metadata"
        task["refresh"] = bool(refresh)
        task["scope"] = {"year": year_int}
        try:
            self._queues.sync_enqueue(task)
        except Exception as e:
            return {"ok": False, "error": str(e)}
        self._on_queue_changed()
        label = "refresh" if refresh else "download"
        ch_name = ch.get("name") or ch.get("folder", "")
        self._log_stream.emit_text(
            f" \u2014 Queued metadata {label} for {ch_name} ({year_int}) "
            f"on Sync Tasks.", "simpleline_pink")
        self._log_stream.flush()
        # Mirror metadata_queue_all's H-7 behavior — but the paused gate
        # still applies (see _maybe_autostart_sync).
        started = self._maybe_autostart_sync()
        return {"ok": True, "queued": True, "year": year_int,
                "refresh": bool(refresh),
                "started": started,
                "paused": bool(self._queues.sync_paused)}


    # ─── Settings > Metadata tab: per-channel refresh status ─────────────

    def get_channel_metadata_status(self, force=False):
        """Return per-channel metadata refresh status for Settings > Metadata.

        Powers the table in settings-view-metadata (index.html:753). Each
        row shows the last time views/likes and comments were refreshed
        for that channel, so stale channels float to the top when sorted
        oldest-first.

        Pulls straight from `self._config["channels"]` — the timestamps
        (`last_views_refresh_ts`, `last_comments_refresh_ts`) get stamped
        by `bulk_refresh_views_likes` and `refresh_channel_comments` in
        backend/metadata.py when those paths finish successfully.

        Returns list[dict] with keys: name, folder, url, video_count,
        last_views_refresh_ts, last_comments_refresh_ts.
        """
        cfg = self._config if self._config is not None else load_config()
        channels = list(cfg.get("channels", []) or [])
        if not channels:
            return []
        # Enrich a copy with n_vids so we don't mutate the live config.
        import copy as _copy
        ch_copy = _copy.deepcopy(channels)
        try:
            # When force=True, rescan disk for fresh n_vids/size before
            # enriching. Otherwise enrich pulls from the cached disk
            # scan (cheap and fast — same data shown across the app).
            if force:
                try:
                    archive_scan.scan_all_channels()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            archive_scan.enrich_channels_with_stats(ch_copy)
        except Exception as e:
            _log.debug("swallowed: %s", e)
        # Pull video-id DB counts so the Metadata tab can show a
        # per-channel status indicator (green if every on-disk file
        # has a resolvable video_id, warn if some missing, red if
        # none).
        # Bulk path: ONE GROUP BY query covers every channel at once.
        # Was 3 COUNT(*) per channel * 100+ channels = 300+ serialized
        # queries holding the FTS DB lock; took 30+ seconds and would
        # appear hung when another op (ingest, sweep) was contending
        # for the lock. The bulk variant returns in under a second.
        try:
            from backend.metadata import (
                count_video_id_status as _cvids,
            )
            from backend.metadata import (
                count_video_id_status_bulk as _cvids_bulk,
            )
        except Exception:
            _cvids_bulk, _cvids = None, None
        _id_lookup = _cvids_bulk(ch_copy, force=bool(force)) if _cvids_bulk else {}
        _empty_ids = {"total": 0, "with_id": 0, "missing": 0, "tried_failed": 0}
        rows = []
        for ch in ch_copy:
            _ch_key = (ch.get("name") or ch.get("folder") or "").lower()
            _idstats = _id_lookup.get(_ch_key)
            if _idstats is None:
                # Fall back to the per-channel query when the bulk
                # lookup didn't cover this channel (e.g. case-drift
                # between config name and DB channel column).
                _idstats = _cvids(ch) if _cvids else _empty_ids
            rows.append({
                "name": ch.get("name") or ch.get("folder") or "",
                "folder": ch.get("folder") or "",
                "url": ch.get("url") or "",
                "video_count": int(ch.get("n_vids") or 0),
                "last_views_refresh_ts": ch.get("last_views_refresh_ts"),
                "last_comments_refresh_ts": ch.get("last_comments_refresh_ts"),
                "id_total": _idstats.get("total", 0),
                "id_with_id": _idstats.get("with_id", 0),
                "id_missing": _idstats.get("missing", 0),
                # Count of missing videos that the backfill pass has
                # already tried and couldn't resolve. Powers the UI
                # "K tried / Y not yet attempted" breakdown so the
                # user knows when Fix IDs is worth re-running.
                "id_tried_failed": _idstats.get("tried_failed", 0),
                # Count of videos known to have been removed from
                # YouTube since download (set by bulk_refresh_views_likes
                # when a previously-known vid disappears from the
                # flat-playlist response). Shown in the Metadata table
                # so the user can tell a channel's "real" coverage
                # vs total file count.
                "removed_from_yt": _idstats.get("removed_from_yt", 0),
            })
        # Sort oldest-refresh-first by default so stale channels float up.
        # A missing timestamp (never refreshed) sorts as oldest (ts=0).
        rows.sort(key=lambda r: (r.get("last_views_refresh_ts") or 0,
                                 (r.get("name") or "").lower()))
        return rows


    def metadata_refresh_comments_all(self, only_recent_days=30):
        """Queue a comments-only refresh for every saved channel.

        Bulk version of metadata_refresh_comments_channel (Settings >
        Metadata > "Refresh comments — all channels" button). Defaults
        to a 30-day scope because comments are the slow per-video path
        and most of the value sits in recently-uploaded videos (users
        often catch videos within 30 minutes of upload, before
        comments exist).
        """
        cfg = self._config or load_config()
        channels = sorted(cfg.get("channels", []),
                          key=lambda c: (c.get("name") or "").lower())
        if not channels:
            return {"ok": False, "error": "No channels configured"}
        try:
            _d = int(only_recent_days) if only_recent_days is not None else None
            if _d is not None and _d <= 0:
                _d = None
        except (TypeError, ValueError):
            _d = None
        queued = 0
        for ch in channels:
            try:
                task = dict(ch)
                task["kind"] = "metadata_comments"
                if _d is not None:
                    task["only_recent_days"] = _d
                if self._queues.sync_enqueue(task):
                    queued += 1
            except Exception as e:
                self._log_stream.emit_error(
                    f"Comments enqueue failed for {ch.get('name')}: {e}")
        self._on_queue_changed()
        scope_str = f" (last {_d}d)" if _d else ""
        self._log_stream.emit_text(
            f" \u2014 Queued comments refresh{scope_str} for {queued} "
            f"channel(s) on Sync Tasks.", "simpleline_pink")
        self._log_stream.flush()
        # Auto-kick the worker \u2014 but only if the queue isn't paused.
        # See _maybe_autostart_sync docstring.
        started = self._maybe_autostart_sync() if queued > 0 else False
        return {"ok": True, "queued": queued, "channels": len(channels),
                "only_recent_days": _d, "started": started,
                "paused": bool(self._queues.sync_paused)}


    def metadata_backfill_ids_channel(self, identity, mode="fast"):
        """Queue a one-shot video_id backfill for a single channel.

        Powers Settings > Metadata's per-row "Fix IDs" button. Lands
        on the sync queue as `kind: "videoid_backfill"` so the user
        sees it in Sync Tasks and can pause / cancel like any other
        sync item. Backend dispatch routes to
        backend.metadata.backfill_video_ids.

        `mode` is "fast" (default — flat-playlist catalog + duration
        matching, ~30-60s) or "thorough" (also does per-video
        upload_date fetch for unresolved-candidate vids, ~minutes-
        to-hours). The JS-side picker prompts the user which to use.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        task = dict(ch)
        task["kind"] = "videoid_backfill"
        task["mode"] = "thorough" if mode == "thorough" else "fast"
        try:
            self._queues.sync_enqueue(task)
        except Exception as e:
            return {"ok": False, "error": str(e)}
        self._on_queue_changed()
        started = self._maybe_autostart_sync()
        return {"ok": True, "queued": True, "started": started,
                "paused": bool(self._queues.sync_paused),
                "mode": task["mode"]}


    def metadata_backfill_ids_all(self, only_missing=True, mode="fast"):
        """Queue a video_id backfill for every saved channel. Default
        `only_missing=True` skips channels whose DB already reports
        zero missing IDs — no point paying yt-dlp time for channels
        that don't need it. Pass False to force-queue everything.

        Important for users migrating from the tkinter-era YTArchiver:
        filenames never carried `[id]` brackets and no .info.json
        sidecars got archived, so the index DB's video_id column is
        NULL for thousands of rows. Without the backfill, the bulk
        views/likes refresh path can't match any on-disk file to its
        YouTube row.
        """
        cfg = self._config or load_config()
        channels = sorted(cfg.get("channels", []),
                          key=lambda c: (c.get("name") or "").lower())
        if not channels:
            return {"ok": False, "error": "No channels configured"}
        # Audit #4: use the BULK count path (single GROUP BY) instead of
        # N per-channel COUNT queries. Previous loop did 100+ serialized
        # queries on every click against a 9M-row table. The bulk path
        # is keyed by lowercased channel name.
        _bulk_status: Dict[str, Dict[str, Any]] = {}
        if only_missing:
            try:
                from backend.metadata import count_video_id_status_bulk as _cvids_bulk
                _bulk_status = _cvids_bulk(channels) or {}
            except Exception:
                _bulk_status = {}
        queued = 0
        skipped = 0
        for ch in channels:
            if only_missing and _bulk_status:
                _key = (ch.get("name") or ch.get("folder") or "").lower()
                st = _bulk_status.get(_key) or {}
                if st.get("total", 0) > 0 and st.get("missing", 0) == 0:
                    skipped += 1
                    continue
            try:
                task = dict(ch)
                task["kind"] = "videoid_backfill"
                task["mode"] = "thorough" if mode == "thorough" else "fast"
                if self._queues.sync_enqueue(task):
                    queued += 1
            except Exception as e:
                self._log_stream.emit_error(
                    f"Backfill enqueue failed for {ch.get('name')}: {e}")
        self._on_queue_changed()
        started = self._maybe_autostart_sync() if queued > 0 else False
        return {"ok": True, "queued": queued,
                "skipped_up_to_date": skipped,
                "channels": len(channels),
                "started": started,
                "paused": bool(self._queues.sync_paused),
                "mode": "thorough" if mode == "thorough" else "fast"}


    def metadata_refresh_views_channel(self, identity):
        """Per-channel views/likes refresh — no prompt, straight enqueue.

        metadata_recheck_channel prompts the user when existing metadata
        is found (Check for New / Refresh Counts / Cancel). The Settings
        > Metadata table's per-row "Refresh views" action is always
        "refresh counts", so this method skips the prompt and enqueues
        a refresh=True metadata task directly. Uses the fast bulk path
        (bulk_refresh_views_likes) via fetch_channel_metadata's
        refresh=True delegate.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        task = dict(ch)
        task["kind"] = "metadata"
        task["refresh"] = True
        try:
            self._queues.sync_enqueue(task)
        except Exception as e:
            return {"ok": False, "error": str(e)}
        self._on_queue_changed()
        # No "Queued ..." log line — the pass-starting banner
        # emitted by sync.py already states the action, so an
        # earlier "Queued views/likes refresh for X on Sync Tasks."
        # just duplicated info one line above.
        started = self._maybe_autostart_sync()
        return {"ok": True, "queued": True, "started": started,
                "paused": bool(self._queues.sync_paused)}


    # ─── Refresh comments (separate per-channel action) ────────────────

    def metadata_refresh_comments_channel(self, identity,
                                           only_recent_days=None):
        """Per-channel comments-only refresh.

        Separate from views/likes refresh because comments require
        per-video yt-dlp calls (no bulk mode exists), so it's always
        the slow path — worth it when pulling community updates for
        videos caught within minutes of upload (no comments at
        download time, decent comments a week later).

        `only_recent_days` optionally scopes to videos uploaded in
        the last N days. None = all videos for the channel.

        Enqueues on the sync queue as a `kind: "metadata_comments"`
        task (dispatched by sync.py:2693's kind-router) so the user
        sees it in the Sync Tasks popover with pause / cancel
        controls like any other sync task.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        task = dict(ch)
        task["kind"] = "metadata_comments"
        if only_recent_days is not None:
            try:
                _d = int(only_recent_days)
                if _d > 0:
                    task["only_recent_days"] = _d
            except (TypeError, ValueError):
                pass
        try:
            self._queues.sync_enqueue(task)
        except Exception as e:
            return {"ok": False, "error": str(e)}
        self._on_queue_changed()
        ch_name = ch.get("name") or ch.get("folder", "")
        scope_str = (f" (last {task['only_recent_days']}d)"
                     if task.get("only_recent_days") else "")
        self._log_stream.emit_text(
            f" \u2014 Queued comments refresh for {ch_name}{scope_str} "
            f"on Sync Tasks.", "simpleline_pink")
        self._log_stream.flush()
        started = self._maybe_autostart_sync()
        return {"ok": True, "queued": True,
                "only_recent_days": task.get("only_recent_days"),
                "started": started,
                "paused": bool(self._queues.sync_paused)}


    def metadata_queue_all(self, refresh=False):
        """Enqueue every saved channel as a `kind: "metadata"` sync
        task — each one becomes its own row in the Sync Tasks popover
        so the user can see, pause, and cancel. Matches rule
        that background work must always be represented in a task
        list. `refresh=True` triggers the refresh variant (re-hits
        every video) instead of skip-existing.

        Mirrors YTArchiver.py:28296 _secret_download_all_metadata (new metadata)
        and :28326 _secret_refresh_all_metadata (views/likes refresh only).
        """
        cfg = self._config or load_config()
        channels = sorted(cfg.get("channels", []),
                          key=lambda c: (c.get("name") or "").lower())
        if not channels:
            return {"ok": False, "error": "No channels configured"}
        queued = 0
        for ch in channels:
            try:
                task = dict(ch)
                task["kind"] = "metadata"
                task["refresh"] = bool(refresh)
                if self._queues.sync_enqueue(task):
                    queued += 1
            except Exception as e:
                self._log_stream.emit_error(
                    f"Metadata enqueue failed for {ch.get('name')}: {e}")
        self._on_queue_changed()
        label = "refresh" if refresh else "download"
        self._log_stream.emit_text(
            f" \u2014 Queued metadata {label} for {queued} channel(s) "
            f"on Sync Tasks.", "simpleline_pink")
        self._log_stream.flush()
        # always auto-fire the worker when the user explicitly
        # clicked "Queue all metadata" / "Refresh views/likes" — the
        # old code gated this on `autorun_sync=True`, so users with
        # autorun off saw "Queued for N channels" and then nothing
        # happened because the worker never started. _maybe_autostart_sync
        # ALSO respects the paused flag — if the user has the queue paused,
        # we enqueue but don't auto-resume.
        started = self._maybe_autostart_sync() if queued > 0 else False
        return {"ok": True, "queued": queued, "channels": len(channels),
                "started": started,
                "paused": bool(self._queues.sync_paused)}


    def _prompt_metadata_already_downloaded(self, channel_name, count):
        # Underscore prefix hides this from the js_api surface (pywebview
        # convention) so a future JS caller can't accidentally wedge
        # the bridge thread on the 2-minute wait. Only the backend
        # sync/recheck worker should invoke this method (see
        # metadata_recheck_channel above).
        """Ask user via JS dialog: Skip / Overwrite / Append. Returns choice string.

        Intended to be called BY the backend sync worker when it detects
        existing metadata. Bridges back into JS askChoice via evaluate_js.
        """
        if self._window is None:
            return {"choice": "skip"}
        import json as _json
        result = {"val": None, "event": threading.Event()}
        token = id(result)
        try:
            # Keyed by `token = id(result)` so two concurrent prompts
            # don't overwrite each other's pending slot. The previous
            # single-slot `self._pending_metadata_choice` would let
            # the second prompt clobber the first; the user's first
            # dismissal then resolved the SECOND prompt's event with
            # the wrong choice and the original timed out 120s later
            # defaulting to "skip" (audit: metadata_mixin H1).
            if not hasattr(self, "_pending_metadata_choices") or \
                    self._pending_metadata_choices is None:
                self._pending_metadata_choices = {}
            self._pending_metadata_choices[token] = result
            # Legacy single-slot kept only as a fallback for any
            # resolver caller that doesn't echo the token back.
            self._pending_metadata_choice = result
            # Create a one-shot global callback the JS side writes into
            js = (
                "(async () => {"
                f" const c = await window.askMetadataAlreadyDownloaded({_json.dumps(channel_name)}, {int(count)});"
                f" window.pywebview.api.metadata_choice_resolve({_json.dumps(token)}, c);"
                "})()"
            )
            self._window.evaluate_js(js)
            result["event"].wait(timeout=120)
            return {"choice": result["val"] or "skip"}
        except Exception:
            return {"choice": "skip"}
        finally:
            try:
                self._pending_metadata_choices.pop(token, None)
            except Exception:
                pass


    def metadata_choice_resolve(self, _token, val):
        """JS calls this when the user picks a choice. Routes by token
        so concurrent prompts each get their own response. MUST be
        public: pywebview never exposes underscore-prefixed methods on
        the js_api bridge, so the old _-prefixed name was unreachable
        and every prompt timed out to "skip" after 120s."""
        try:
            pending_map = getattr(self, "_pending_metadata_choices", None) or {}
            pending = pending_map.get(_token)
            if pending is None:
                # Fallback to single-slot for legacy callers.
                pending = getattr(self, "_pending_metadata_choice", None)
        except Exception:
            pending = getattr(self, "_pending_metadata_choice", None)
        if pending:
            pending["val"] = val
            pending["event"].set()
        return {"ok": True}
