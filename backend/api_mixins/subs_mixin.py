"""
SubsMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class SubsMixin:

    def get_subs_channels(self):
        """Return (rows, total_label) for the Subs table. Real data if avail.

        Enriches each row with n_vids / size_gb / size from the disk cache
        (ytarchiver_disk_cache.json) so counts match what YTArchiver shows.
        """
        if self._config is not None and self._config.get("channels"):
            # Enrich a copy so we don't mutate the in-memory config
            import copy as _copy
            cfg_copy = _copy.deepcopy(self._config)
            archive_scan.enrich_channels_with_stats(cfg_copy.get("channels", []))
            return channels_for_subs_ui(cfg_copy)
        return [], "0 channels · 0 videos · 0 GB"


    # ─── Subs CRUD (writes go to real %APPDATA%/YTArchiver/ytarchiver_config.json) ───

    def subs_is_writable(self):
        """Whether YTArchiver can write to the config file right now."""
        return config_is_writable()


    def subs_check_duplicate(self, url, folder, exclude_identity=None):
        """Return {dup_url: existing_name|None, dup_folder: existing_name|None}
        so the Add or Edit dialog can warn before actually trying to commit.

        `exclude_identity` (audit U-5): when running this check during an
        EDIT (not Add), pass the identity of the channel being edited so
        we don't flag the channel as a duplicate of itself. Identity dict
        with `name` / `folder` / `url` keys (any subset works — we exclude
        on the first match).
        """
        try:
            cfg = load_config()
            channels = cfg.get("channels", []) or []
            url_norm = (url or "").strip().lower().rstrip("/")
            folder_norm = (folder or "").strip().lower()
            # Build exclusion criteria from the identity dict.
            ex_url = ""
            ex_name = ""
            ex_folder = ""
            if isinstance(exclude_identity, dict):
                ex_url = (exclude_identity.get("url") or "").strip().lower().rstrip("/")
                ex_name = (exclude_identity.get("name") or "").strip().lower()
                ex_folder = (exclude_identity.get("folder") or "").strip().lower()
            dup_url = None
            dup_folder = None
            for ch in channels:
                u = (ch.get("url") or "").strip().lower().rstrip("/")
                n = (ch.get("name") or "").strip().lower()
                f = (ch.get("folder") or "").strip().lower()
                # Skip the channel being edited (identified by URL,
                # name, OR folder — any match counts).
                if (ex_url and u == ex_url) \
                        or (ex_name and n == ex_name) \
                        or (ex_folder and f == ex_folder):
                    continue
                if url_norm and u == url_norm:
                    dup_url = ch.get("name") or ch.get("folder") or ch.get("url")
                if folder_norm and (n == folder_norm or f == folder_norm):
                    dup_folder = ch.get("name") or ch.get("folder")
            return {"ok": True, "dup_url": dup_url, "dup_folder": dup_folder}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def subs_preview_folder_name(self, url):
        """Probe yt-dlp for a channel URL's metadata so the user can see
        what folder name they'd get BEFORE committing. Mirrors
        YTArchiver.py:17162 do_preview_folder → _probe.

        Runs on a background thread; returns {ok, channel, folder} when
        done via the persisted `_pending_preview` slot, polled via
        `subs_preview_folder_poll`.
        """
        url = (url or "").strip()
        if not url:
            return {"ok": False, "error": "No URL"}
        yt = sync_backend.find_yt_dlp()
        if not yt:
            return {"ok": False, "error": "yt-dlp not found"}
        # uuid4 for the token. Old `id(url) + time.time()*ms` collided
        # when two previews fired within the same millisecond AND
        # Python recycled the id for a short-lived string (audit:
        # subs_mixin.py:97). uuid4 is collision-free in practice.
        import uuid as _uuid
        token = _uuid.uuid4().hex
        # Lock-protected pending-preview dict. js_api and worker
        # threads both mutate it (set→pending, set→result, pop on
        # poll), so a bare dict could drop entries on concurrent set+
        # pop (audit: subs_mixin.py:98).
        if not hasattr(self, "_pending_previews"):
            self._pending_previews = {}
        if not hasattr(self, "_pending_previews_lock"):
            self._pending_previews_lock = threading.Lock()
        # Sweep entries older than 10 minutes on every new submit so
        # the dict can't grow unbounded when users abandon previews
        # (modal dismissed, navigated away, race with another preview)
        # without ever polling. Same TTL pattern applies to
        # _pending_res_scans / _drift_scan_results / _drift_apply_results
        # (audit: subs_mixin H10).
        import time as _t_mod
        _now_ts = _t_mod.time()
        with self._pending_previews_lock:
            _stale = [k for k, v in self._pending_previews.items()
                      if isinstance(v, dict)
                      and (_now_ts - (v.get("_ts") or _now_ts)) > 600]
            for k in _stale:
                self._pending_previews.pop(k, None)
            self._pending_previews[token] = {
                "ok": False, "pending": True, "_ts": _now_ts}
        def _run():
            import subprocess as _sp
            try:
                cmd = [
                    yt, "--flat-playlist", "--print", "channel",
                    "--print", "uploader",
                    *sync_backend._find_cookie_source(),
                    "--playlist-end", "1", url,
                ]
                r = _sp.run(cmd, capture_output=True, text=True, timeout=25,
                            startupinfo=sync_backend._startupinfo,
                            creationflags=(0x08000000 if os.name == "nt" else 0))
                out = (r.stdout or "").strip().splitlines()
                name = (out[0] if out else "").strip() or (out[1] if len(out) > 1 else "").strip()
                if not name:
                    with self._pending_previews_lock:
                        self._pending_previews[token] = {
                            "ok": False, "error": "yt-dlp returned nothing",
                            "_ts": _t_mod.time()}
                    return
                folder = sync_backend.sanitize_folder(name)
                with self._pending_previews_lock:
                    self._pending_previews[token] = {
                        "ok": True, "channel": name, "folder": folder,
                        "_ts": _t_mod.time()}
            except Exception as e:
                with self._pending_previews_lock:
                    self._pending_previews[token] = {
                        "ok": False, "error": str(e), "_ts": _t_mod.time()}
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "token": token}


    def subs_preview_folder_poll(self, token):
        """Poll a pending folder-preview result by token. Returns
        {ok, pending} while running, or the final {ok, channel, folder}
        once `_run` sets it.
        """
        lock = getattr(self, "_pending_previews_lock", None)
        pend = getattr(self, "_pending_previews", {})
        if lock is not None:
            with lock:
                res = pend.get(token)
                if res is None:
                    return {"ok": False, "error": "unknown token"}
                if res.get("pending"):
                    return {"ok": True, "pending": True}
                # One-shot: pop the result inside the lock so a second
                # poll racing with this one can't double-deliver.
                try: del pend[token]
                except KeyError: pass
                return res
        # Defensive fallback if lock somehow isn't initialized yet.
        res = pend.get(token)
        if res is None:
            return {"ok": False, "error": "unknown token"}
        if res.get("pending"):
            return {"ok": True, "pending": True}
        try: del pend[token]
        except KeyError: pass
        return res


    def subs_add_channel(self, payload):
        """Add a new channel. Returns {ok, channel?, error?}.

        Also kicks off a one-time channel-art fetch in the background so the
        Browse grid shows the avatar/banner immediately — matches OLD
        YTArchiver behavior where adding a channel triggers
        `_fetch_channel_art`.
        """
        try:
            ch = subs_backend.add_channel(payload or {})
            self._reload_config()
            # Fire-and-forget channel-art fetch — but only when the
            # channel record actually committed to disk. If the config
            # write was gated (`_write_blocked`), skip the art fetch
            # so we don't leave .ChannelArt/ files for a channel
            # whose subs entry will revert on next reload (audit:
            # subs_mixin.py:144).
            try:
                name = ch.get("name") or ch.get("folder", "")
                if name and not ch.get("_write_blocked"):
                    self.chan_fetch_art(name, False)
            except Exception as e:
                _log.debug("swallowed: %s", e)
            return {"ok": True, "channel": ch,
                    "write_blocked": ch.get("_write_blocked", False)}
        except subs_backend.SubsError as e:
            return {"ok": False, "error": str(e)}
        except Exception as e:
            return {"ok": False, "error": f"Internal error: {e}"}


    def subs_update_channel(self, identity, payload):
        """Update an existing channel matched by identity (url/name/folder)."""
        try:
            ch = subs_backend.update_channel(identity or {}, payload or {})
            self._reload_config()
            # surface folder-rename failures so the user
            # knows the on-disk folder didn't move. Config was kept at
            # the old name (subs.py rollback).
            resp = {"ok": True, "channel": ch,
                    "write_blocked": ch.get("_write_blocked", False)}
            if ch.get("_folder_rename_error"):
                resp["folder_rename_error"] = ch["_folder_rename_error"]
            return resp
        except subs_backend.SubsError as e:
            return {"ok": False, "error": str(e)}
        except Exception as e:
            return {"ok": False, "error": f"Internal error: {e}"}


    def subs_remove_channel(self, identity, delete_files=False):
        """Remove a channel by identity. Pushes the removed dict onto the
        `_removed_channels_stack` so future subs_undo_remove calls can
        unwind in reverse-remove order (newest undo first).

        Previously stored a single slot — removing two channels in
        succession, then undoing once, left the second one unrecoverable.

        If `delete_files=True`, the channel's on-disk folder (videos +
        transcripts + metadata + thumbnails) is recursively deleted. Undo
        only restores the subscription, not the files.
        """
        try:
            # Snapshot before removal for undo
            ch_snap = subs_backend.get_channel(identity or {})
            # refuse delete_files=True while sync is actively
            # processing this channel — shutil.rmtree racing yt-dlp's
            # active writes can crash sync, partially-delete files, or
            # leave orphan temp dirs. Sub is not removed either since
            # that side effect would also surprise a live sync.
            if delete_files and ch_snap:
                _target_url = (ch_snap.get("url") or "").strip()
                # Hold the sync-mutation lock for BOTH the check and
                # the subs_backend.remove_channel() call below (which
                # is what actually calls rmtree). Without the lock,
                # a sync worker could start touching this channel
                # between the active-sync check and the rmtree —
                # racing yt-dlp's writes against rmtree's directory
                # walk. The lock is reentrant so sync_start_all
                # taking it elsewhere doesn't self-deadlock.
                if not hasattr(self, "_sync_mutation_lock"):
                    self._sync_mutation_lock = threading.RLock()
                with self._sync_mutation_lock:
                    try:
                        # The OLD guard read self._current_sync_channel, which is
                        # never assigned anywhere — so it always saw "" and never
                        # fired, letting rmtree race a live sync's writes. Compare
                        # the delete target against the REAL active-sync state
                        # (QueueState.current_sync, set via set_current_sync()).
                        _cur = getattr(self._queues, "current_sync", None) or {}
                        _cur_url = (_cur.get("url") or "").strip()
                        _cur_name = (_cur.get("name") or "").strip()
                        _t_name = (ch_snap.get("name") or "").strip()
                        if ((_target_url and _cur_url and _cur_url == _target_url)
                                or (_t_name and _cur_name and _cur_name == _t_name)):
                            return {
                                "ok": False,
                                "error": ("Sync is currently running on this "
                                          "channel. Cancel or pause the sync "
                                          "first, then retry the delete."),
                            }
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                    # Take the rmtree branch INSIDE the lock so an
                    # incoming sync start can't slip past our check.
                    result = subs_backend.remove_channel(
                        identity or {}, delete_files=bool(delete_files))
            else:
                result = subs_backend.remove_channel(
                    identity or {}, delete_files=bool(delete_files))
            ok = bool(result.get("ok"))
            # When the files were physically deleted, also purge the channel's
            # rows from the index DB (videos + transcript segments). Browse /
            # Search / Videos read the index, not the disk — without this the
            # removed channel's cards linger and 404 ("File not found — index
            # entry may be stale") when clicked. Match every identifier the
            # videos.channel column might hold (name / folder / override).
            if ok and delete_files and ch_snap:
                try:
                    from backend import index as _idx
                    _names = set()
                    for _k in ("name", "folder", "folder_override"):
                        _v = (ch_snap.get(_k) or "").strip()
                        if _v:
                            _names.add(_v)
                    for _nm in _names:
                        _idx.delete_channel_from_index(_nm)
                except Exception as e:
                    _log.debug("index purge after channel delete failed: %s", e)
            if ok and ch_snap and not delete_files:
                if not hasattr(self, "_removed_channels_stack"):
                    self._removed_channels_stack = []
                self._removed_channels_stack.append(ch_snap)
                # Bound the stack so we don't grow unbounded across
                # a long session of repeated removes.
                if len(self._removed_channels_stack) > 50:
                    self._removed_channels_stack = (
                        self._removed_channels_stack[-50:])
            # drop any queued sync tasks for this
            # channel so a removed channel doesn't keep getting
            # synced (which recreates the folder and confuses the
            # log). Best-effort — removal is authoritative even if
            # queue cleanup fails.
            if ok and ch_snap:
                try:
                    _ch_url = (ch_snap.get("url") or "").strip()
                    if _ch_url:
                        self._queues.sync_remove(_ch_url)
                        self._on_queue_changed()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            self._reload_config()
            return {
                "ok": ok,
                "write_blocked": not ok,
                "can_undo": bool(ch_snap and ok and not delete_files),
                "deleted_folder": bool(result.get("deleted_folder")),
                "folder_path": result.get("folder_path"),
                "delete_error": result.get("delete_error"),
            }
        except subs_backend.SubsError as e:
            return {"ok": False, "error": str(e)}
        except Exception as e:
            return {"ok": False, "error": f"Internal error: {e}"}


    def subs_undo_remove(self):
        """Restore the most recently removed channel. Pops from a stack
        so multiple consecutive removes can be undone one-at-a-time
        in LIFO order.
        """
        stack = getattr(self, "_removed_channels_stack", None)
        # Distinguish `stack is None` (legacy — attr was never set,
        # consult single-slot fallback) from `stack == []` (set but
        # empty — there's nothing to undo, return immediately). The
        # old `if not stack:` collapsed both into the legacy branch
        # which broke LIFO ordering when a later exception path
        # appended back to the stack mid-undo (audit: subs_mixin H12).
        if stack is None:
            legacy = getattr(self, "_last_removed_channel", None)
            if legacy:
                self._last_removed_channel = None
                ch = legacy
            else:
                return {"ok": False, "error": "Nothing to undo"}
        elif not stack:
            return {"ok": False, "error": "Nothing to undo"}
        else:
            ch = stack.pop()
        try:
            payload = dict(ch)
            # add_channel expects 'folder' / 'name'; strip anything that might confuse it
            payload["folder"] = ch.get("name") or ch.get("folder")
            result = subs_backend.add_channel(payload)
            self._reload_config()
            # pop the disk-cache entry for the restored channel
            # so the next Subs-table render triggers a fresh rescan
            # instead of showing "—" or stale counts. invalidate_channel
            # spawns a background rescan that repopulates num_vids/
            # size_bytes.
            try:
                from backend import archive_scan as _as
                _url = (result.get("url") or ch.get("url") or "").strip()
                if _url:
                    _as.invalidate_channel(_url)
            except Exception as e:
                _log.debug("swallowed: %s", e)
            return {
                "ok": True,
                "channel": result,
                "more_undo_available": bool(
                    getattr(self, "_removed_channels_stack", None)),
            }
        except subs_backend.SubsError as e:
            # Restore so the user can retry. Cover BOTH the stack and
            # the legacy single-slot branches — previously the legacy
            # branch had cleared `_last_removed_channel` before the
            # restore attempt, and on exception the channel was lost
            # forever with no fallback.
            if stack is not None:
                stack.append(ch)
            else:
                self._last_removed_channel = ch
            return {"ok": False, "error": str(e)}
        except Exception as e:
            if stack is not None:
                stack.append(ch)
            else:
                self._last_removed_channel = ch
            return {"ok": False, "error": str(e)}


    def subs_reset_sync_state(self, identity):
        """audit SM-1: clear a channel's bootstrap / sync-state flags
        so the next sync does a fresh full-walk. Useful when the
        user wipes the folder manually or wants to re-bootstrap
        after a filter change.

        Clears: initialized, sync_complete, batch_resume_index,
                init_batch_after, init_complete, last_sync.
        Preserves: everything else (channels/url/filters/etc).
        """
        try:
            ch_snap = subs_backend.get_channel(identity or {})
            if not ch_snap:
                return {"ok": False, "error": "Channel not found"}
            cfg = load_config()
            _url = (ch_snap.get("url") or "").strip().rstrip("/")
            _flags = ("initialized", "sync_complete", "init_complete",
                      "batch_resume_index", "init_batch_after", "last_sync")
            _cleared = 0
            for c in cfg.get("channels", []):
                _c_url = (c.get("url") or "").strip().rstrip("/")
                if _c_url != _url:
                    continue
                for k in _flags:
                    if k in c:
                        c.pop(k, None)
                        _cleared += 1
                break
            save_config(cfg)
            self._reload_config()
            return {"ok": True, "cleared_flags": _cleared,
                    "channel": ch_snap.get("name") or ch_snap.get("folder") or ""}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def subs_get_channel(self, identity):
        """Fetch a single channel's full record (for populating the edit panel).

        Min/max durations are converted to minutes here to match the UI unit.
        On save, `_payload_to_channel` converts back to seconds for storage.
        """
        try:
            ch = subs_backend.get_channel_for_ui(identity or {})
            return {"ok": True, "channel": ch} if ch else {"ok": False, "error": "Not found"}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def subs_test_url(self, url):
        """Probe a channel URL via yt-dlp, return the canonical name + video count."""
        url = (url or "").strip()
        if not url:
            return {"ok": False, "error": "Empty URL"}
        yt = sync_backend.find_yt_dlp()
        if not yt:
            return {"ok": False, "error": "yt-dlp not found"}
        import subprocess as _sp

        from backend.subs import normalize_channel_url
        try:
            normalized = normalize_channel_url(url)
            cookies = sync_backend._find_cookie_source()
            # Get channel name (from first video)
            r1 = _sp.run([yt, "--flat-playlist", "--playlist-end", "1",
                         "--print", "channel", "--no-warnings", "--quiet",
                         *cookies, normalized],
                        capture_output=True, text=True, timeout=15,
                        startupinfo=sync_backend._startupinfo)
            name = (r1.stdout or "").strip().split("\n")[0] or ""
            # Get total count (best-effort)
            r2 = _sp.run([yt, "--flat-playlist", "--print", "%(playlist_count)s",
                         "--playlist-end", "1", "--no-warnings", "--quiet",
                         *cookies, normalized],
                        capture_output=True, text=True, timeout=15,
                        startupinfo=sync_backend._startupinfo)
            count_raw = (r2.stdout or "").strip().split("\n")[0]
            total = int(count_raw) if count_raw.isdigit() else None
            return {"ok": bool(name), "name": name, "total": total, "url": normalized}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def subs_get_defaults(self):
        """Return the user's default channel settings for the Restore-defaults button.

        min_duration / max_duration are returned in MINUTES to match the
        unit the edit-panel input accepts + displays.
        """
        cfg = self._config or load_config()
        # cfg["min_duration"] is SECONDS (180 = 3 min) per YTArchiver's schema
        raw_min_secs = int(cfg.get("min_duration", 180) or 0)
        # read user-configured defaults from config if set,
        # fall back to conservative defaults. Previously these were
        # hardcoded constants so the "Restore defaults" button in the
        # edit panel always clobbered user preference with the constant
        # values — if user set Settings>General auto_transcribe=true
        # and then clicked Restore on a channel, the channel flipped
        # to false regardless of their stated preference.
        return {
            "resolution": cfg.get("default_resolution", "720"),
            "min_duration": max(0, raw_min_secs // 60),
            "max_duration": 0,
            "auto_metadata": bool(cfg.get("default_auto_metadata", True)),
            "auto_transcribe": bool(cfg.get("default_auto_transcribe", False)),
            "compress_enabled": bool(cfg.get("default_compress_enabled", False)),
            "mode": (cfg.get("default_mode") or "new"),
            "folder_org": (cfg.get("default_folder_org") or "years"),
        }


    # ─── Bulk channel operations (feature F7) ──────────────────────────

    def subs_bulk_update(self, names, changes):
        """Apply a small set of whitelisted changes to N channels at once.

        `names` — list of channel folder / display names.
        `changes` — dict with keys from the whitelist below:
          resolution, auto_transcribe, auto_metadata,
          compress_enabled, compress_level, compress_output_res,
          compress_batch_size.
        Returns {ok, updated, failed}. Guarded to the whitelist so the
        UI can't accidentally wipe urls / folder names / anything
        load-bearing.
        """
        if not isinstance(names, list) or not names:
            return {"ok": False, "error": "No channels selected"}
        if not isinstance(changes, dict) or not changes:
            return {"ok": False, "error": "No changes specified"}
        _ALLOWED = {"resolution", "auto_transcribe", "auto_metadata",
                    "compress_enabled", "compress_level",
                    "compress_output_res", "compress_batch_size"}
        clean = {k: v for k, v in changes.items() if k in _ALLOWED}
        if not clean:
            return {"ok": False, "error": "No allowed fields in changes"}
        updated = 0
        failed = []
        write_blocked = []
        for n in names:
            try:
                ch = subs_backend.get_channel({"name": n}) \
                     or subs_backend.get_channel({"folder": n})
                if not ch:
                    failed.append({"name": n, "reason": "not found"})
                    continue
                # Pass ONLY the whitelisted changes. `clean` carries no
                # 'url' key, so update_channel takes its SPARSE merge
                # path (subs.py) and every other field survives
                # untouched. The old dict(ch)+update(clean) payload had
                # a truthy url, which routed the full DISK-shape record
                # through the UI-shape _payload_to_channel rebuild:
                # min/max durations re-multiplied x60, mode forced to
                # 'new', folder org forced to years, and _apply_defaults
                # reset last_sync / pending_tx_ids / folder_override on
                # every bulk-updated channel.
                _res = subs_backend.update_channel(
                    {"url": ch.get("url", ""), "name": ch.get("name", "")},
                    dict(clean))
                # Detect save failures the backend signals via
                # `_write_blocked: True` (my Fix 10 made update_channel
                # roll back its in-memory mutation and return that
                # marker rather than raise). Without surfacing it,
                # bulk_update silently reported "updated:N" while
                # half of those updates never landed on disk.
                if isinstance(_res, dict) and _res.get("_write_blocked"):
                    write_blocked.append(n)
                    failed.append({
                        "name": n,
                        "reason": "save_config failed (disk full / locked?); update rolled back",
                    })
                else:
                    updated += 1
            except Exception as e:
                failed.append({"name": n, "reason": str(e)})
        self._reload_config()
        return {"ok": True, "updated": updated, "failed": failed,
                "write_blocked": write_blocked}


    def subs_bulk_delete(self, names, delete_files=False):
        """Delete N channels at once. `delete_files=True` also removes
        the on-disk folders. Returns {ok, started}.

        The per-channel shutil.rmtree calls can take many minutes on
        TB-scale channels; the work runs on a background thread so the
        bridge call returns immediately. The result toast + Subs table
        refresh are pushed via evaluate_js when the worker finishes.
        """
        if not isinstance(names, list) or not names:
            return {"ok": False, "error": "No channels selected"}
        if not hasattr(self, "_removed_channels_stack"):
            self._removed_channels_stack = []

        def _bd_worker():
            deleted = 0
            failed = []
            for n in names:
                try:
                    ch = subs_backend.get_channel({"name": n}) \
                         or subs_backend.get_channel({"folder": n})
                    if not ch:
                        failed.append({"name": n, "reason": "not found"})
                        continue
                    res = self.subs_remove_channel(
                        {"url": ch.get("url", "")},
                        delete_files=bool(delete_files))
                    if res.get("ok"):
                        deleted += 1
                    else:
                        failed.append({"name": n,
                                       "reason": res.get("error", "unknown")})
                except Exception as e:
                    failed.append({"name": n, "reason": str(e)})
            self._log_stream.emit([["[Subs] ", "sync_bracket"],
                                    [f"Bulk delete: {deleted} removed"
                                     + (f" ({len(failed)} failed)" if failed else "")
                                     + ".\n", "simpleline"]])
            self._log_stream.flush()
            try:
                if self._window is not None:
                    _msg = f"Removed {deleted} channel(s)."
                    if failed:
                        _msg += f" {len(failed)} failed."
                    _kind = "ok" if not failed else "warn"
                    self.services.event_bus.show_toast_and_refresh_subs(
                        _msg, _kind)
            except Exception as e:
                _log.debug("swallowed: %s", e)

        threading.Thread(target=_bd_worker, daemon=True,
                         name="subs_bulk_delete").start()
        return {"ok": True, "started": True}



    def subs_bulk_queue_metadata(self, names, refresh=False):
        """Queue a metadata fetch (or refresh) for N channels at once.
        Thin wrapper around the per-channel enqueue path that
        `metadata_queue_all` uses.
        """
        if not isinstance(names, list) or not names:
            return {"ok": False, "error": "No channels selected"}
        queued = 0
        failed = []
        for n in names:
            try:
                ch = subs_backend.get_channel({"name": n}) \
                     or subs_backend.get_channel({"folder": n})
                if not ch:
                    failed.append({"name": n, "reason": "not found"})
                    continue
                task = dict(ch)
                task["kind"] = "metadata"
                task["refresh"] = bool(refresh)
                if self._queues.sync_enqueue(task):
                    queued += 1
            except Exception as e:
                failed.append({"name": n, "reason": str(e)})
        self._on_queue_changed()
        # Auto-fire the worker — gated on paused state.
        started = self._maybe_autostart_sync() if queued > 0 else False
        return {"ok": True, "queued": queued, "failed": failed,
                "started": started,
                "paused": bool(self._queues.sync_paused)}


    def subs_queue_pending(self):
        """Left-click of the Subs header "↺ Queue Pending" button.

        Walks every subscribed channel; for any with `transcription_pending > 0`
        (or that have new videos without `.jsonl` sidecars), queues a bulk
        transcribe. `chan_transcribe_pending` is real-state aware — it
        scans aggregate transcripts + DB, skips channels already fully
        transcribed, and resets stale counters so the badge self-heals.

        Matches YTArchiver.py:5808 _queue_pending_transcriptions.

        The walk + per-channel scan moved to a background thread so the
        bridge call returns immediately. Final tally + Subs refresh
        land via evaluate_js when the worker finishes.
        """
        cfg = self._config or load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        from backend.sync import channel_folder_name as _cfn

        def _qp_worker():
            tx_added = 0
            mt_added = 0
            for ch in cfg.get("channels", []):
                ch_name = ch.get("name") or ch.get("folder") or ""
                if not ch_name:
                    continue
                _ = os.path.join(base, _cfn(ch))
                pending_ids = ch.get("pending_tx_ids") or []
                if isinstance(pending_ids, list) and len(pending_ids) > 0:
                    r = self.chan_transcribe_pending(ch_name)
                    if r and r.get("ok") and r.get("queued", 0) > 0:
                        tx_added += 1
                if int(ch.get("metadata_pending") or 0) > 0:
                    try:
                        task = dict(ch)
                        task["kind"] = "metadata"
                        task["refresh"] = False
                        if self._queues.sync_enqueue(task):
                            mt_added += 1
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
            if mt_added or tx_added:
                try:
                    self._on_queue_changed()
                    cfg2 = load_config() or {}
                    if (cfg2.get("autorun_sync", False) and
                            not self.sync_is_running() and mt_added > 0):
                        self.sync_start_all(add_downloads_from_config=False)
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            parts = []
            if tx_added: parts.append(f"{tx_added} for transcription")
            if mt_added: parts.append(f"{mt_added} for metadata")
            if parts:
                self._log_stream.emit([["[Subs] ", "sync_bracket"],
                                        [f"\u21ba Queued {', '.join(parts)}.\n",
                                         "simpleline_green"]])
                _toast_msg = f"Queued {', '.join(parts)}."
                _toast_kind = "ok"
            else:
                self._log_stream.emit([["[Subs] ", "sync_bracket"],
                                        ["No channels with pending transcriptions or metadata.\n",
                                         "dim"]])
                _toast_msg = "No pending channels."
                _toast_kind = "warn"
            self._log_stream.flush()
            try:
                if self._window is not None:
                    self.services.event_bus.show_toast_and_refresh_subs(
                        _toast_msg, _toast_kind)
            except Exception as e:
                _log.debug("swallowed: %s", e)

        threading.Thread(target=_qp_worker, daemon=True,
                         name="subs_queue_pending").start()
        return {"ok": True, "started": True}



    def subs_queue_all(self):
        """Right-click of the "↺ Queue Pending" button — queues ALL channels
        for transcribe. Matches YTArchiver.py:5844 _queue_all_transcriptions.

        Walk + per-channel scan moved to a background thread so the
        bridge call returns immediately. Final tally toast lands via
        evaluate_js when the worker finishes.
        """
        cfg = self._config or load_config()
        channels = cfg.get("channels", []) or []

        def _qa_worker():
            queued = 0
            for ch in channels:
                name = ch.get("name") or ch.get("folder") or ""
                if not name:
                    continue
                r = self.chan_transcribe_all(name)
                if r and r.get("ok") and r.get("queued", 0) > 0:
                    queued += 1
            self._log_stream.emit([["[Subs] ", "sync_bracket"],
                                    [f"\u21ba Queued all: {queued} channels\n",
                                     "simpleline_green"]])
            self._log_stream.flush()
            try:
                if self._window is not None:
                    self.services.event_bus.show_toast_and_refresh_subs(
                        f"Queued {queued} channels.", "ok")
            except Exception as e:
                _log.debug("swallowed: %s", e)

        threading.Thread(target=_qa_worker, daemon=True,
                         name="subs_queue_all").start()
        return {"ok": True, "started": True}



    def subs_relocate_channel(self, identity, new_folder_name):
        """Update a channel's folder_override to point at a different on-disk
        folder (used when the original folder is gone but the user has it
        elsewhere). `new_folder_name` must be a subfolder of output_dir.

        Mirrors YTArchiver.py:33700 "locate" branch of the missing-folder
        dialog. Never moves files — just updates the config pointer.
        """
        if not identity or not new_folder_name:
            return {"ok": False, "error": "identity + new_folder_name required"}
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        # Validate the INPUT shape: new_folder_name must be a single bare
        # folder name (no separators, not absolute, not . / ..) so
        # os.path.join below can't be coerced into escaping output_dir. The
        # dirname check further down stays as the second layer (audit:
        # subs_relocate_channel containment).
        if (os.sep in new_folder_name or "/" in new_folder_name
                or os.path.isabs(new_folder_name)
                or new_folder_name in (".", "..")):
            return {"ok": False,
                    "error": "Folder name must be a single folder under output_dir."}
        target = os.path.normpath(os.path.join(base, new_folder_name))
        if not os.path.isdir(target):
            return {"ok": False, "error": f"Folder not found: {target}"}
        # Guard: must live inside output_dir (prevent folder_override escapes)
        if os.path.dirname(target) != os.path.normpath(base):
            return {"ok": False,
                    "error": "Target folder must live directly under output_dir"}
        try:
            # Require a non-empty identity field and compare with truthiness
            # guards — otherwise a folder-only identity makes both sides
            # None == None → True and rewrites the FIRST channel's folder
            # (audit r2).
            _id_url = (identity.get("url") or "").strip()
            _id_name = (identity.get("name") or "").strip()
            if not _id_url and not _id_name:
                return {"ok": False, "error": "identity needs a url or name"}
            for ch in cfg.get("channels", []):
                if ((_id_url and ch.get("url") == _id_url)
                        or (_id_name and ch.get("name") == _id_name)):
                    ch["folder_override"] = os.path.basename(target)
                    break
            from backend.ytarchiver_config import save_config as _sc
            if not _sc(cfg):
                return {"ok": False, "error": "Save blocked (write-gate off)"}
            self._reload_config()
            return {"ok": True, "folder_override": os.path.basename(target)}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def subs_browse_for_channel_folder(self, channel_name):
        """Open native folder picker; returns the selected folder's basename
        (must be inside output_dir) or an error."""
        try:
            import webview as _wv
            if self._window is None:
                return {"ok": False, "error": "No window"}
            cfg = load_config()
            base = (cfg.get("output_dir") or "").strip()
            paths = self._window.create_file_dialog(
                _wv.FOLDER_DIALOG, directory=base,
            )
            if not paths:
                return {"ok": False, "cancelled": True}
            picked = paths if isinstance(paths, str) else paths[0]
            picked = os.path.normpath(picked)
            if os.path.dirname(picked) != os.path.normpath(base):
                return {"ok": False,
                        "error": f"Pick a subfolder of:\n {base}"}
            return {"ok": True,
                    "folder_name": os.path.basename(picked),
                    "full_path": picked}
        except Exception as e:
            return {"ok": False, "error": str(e)}
