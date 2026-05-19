"""
SettingsMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class SettingsMixin:

    def set_log_mode(self, mode):
        """UI toggled log mode. Pushes filter state into LogStreamer + persists (gated)."""
        if mode not in ("Simple", "Verbose"):
            return False
        # Persist to disk FIRST, then mutate in-memory state on success.
        # If the save fails (permission, write-gate off, disk full),
        # leaving self._config unchanged keeps the in-memory state
        # consistent with what the next reload will read.
        persisted = False
        try:
            from backend.ytarchiver_config import save_config as _sc
            cfg = load_config()
            cfg["log_mode"] = mode
            persisted = bool(_sc(cfg))
        except Exception:
            persisted = False
        if persisted:
            if self._config is not None:
                self._config["log_mode"] = mode
            # LogStreamer respects `simple_mode` when filtering dim/verbose lines
            self._log_stream.simple_mode = (mode == "Simple")
        return persisted


    # ─── Autorun scheduler ─────────────────────────────────────────────

    def autorun_set(self, label_or_mins):
        """Accept a label like '30 min' / '1 hr' / 'Off' OR an integer minutes."""
        if isinstance(label_or_mins, str):
            return self._autorun.set_interval_label(label_or_mins)
        try:
            return self._autorun.set_interval_mins(int(label_or_mins))
        except Exception:
            return {"ok": False, "error": "bad value"}


    def autorun_state(self):
        return self._autorun.get_state()


    # ─── Settings dialog: load / save all tunables ─────────────────────

    def settings_load(self):
        cfg = self._config or load_config()
        return {
            "output_dir": cfg.get("output_dir", ""),
            "video_out_dir": cfg.get("video_out_dir", ""),
            "whisper_model": cfg.get("whisper_model", "small"),
            "default_resolution": cfg.get("default_resolution", "720"),
            "log_mode": cfg.get("log_mode", "Simple"),
            # Index tab surfaces these directly — must round-trip.
            "tp_archive_roots": list(cfg.get("tp_archive_roots") or []),
            "auto_index_enabled": bool(cfg.get("auto_index_enabled", False)),
            "auto_index_threshold": int(cfg.get("auto_index_threshold", 10) or 10),
            # Startup knobs (Settings > General surfaces these too).
            "disk_scan_staleness_hours": int(cfg.get("disk_scan_staleness_hours", 24) or 0),
            "browse_preload_limit": int(cfg.get("browse_preload_limit", 150) or 150),
            "browse_preload_all": bool(cfg.get("browse_preload_all", False)),
            "last_disk_scan_ts": float(cfg.get("last_disk_scan_ts", 0) or 0),
            # Subs table column visibility toggles. Default False for
            # new users — the column is optional polish, not core info.
            "show_avg_size": bool(cfg.get("show_avg_size", False)),
            # Recent tab view mode — "list" (legacy) or "grid" (thumbnail
            # cards). Default "grid" for new users — the thumbnail view
            # reads more naturally at a glance.
            "recent_view_mode": (cfg.get("recent_view_mode") or "grid"),
            # X-button behavior — "ask" (default modal), "tray"
            # (minimize silently), or "quit" (exit silently). Read by
            # _on_closing at main.py:7552; also written by the close
            # modal's "Remember my choice" checkbox via confirm_close.
            "close_behavior": (cfg.get("close_behavior") or "ask"),
            # Watch view persisted preferences. These were write-only
            # before — settings_save accepted them but settings_load
            # never returned them, so they only survived via
            # localStorage. Surface them here so the cross-session
            # restore actually works from the canonical config too.
            "transcript_font_size": cfg.get("transcript_font_size"),
            "transcript_pane_width": cfg.get("transcript_pane_width"),
            "caption_overlay_size": (cfg.get("caption_overlay_size") or ""),
            "caption_overlay_bg": (cfg.get("caption_overlay_bg") or ""),
        }


    def settings_save(self, data):
        if not config_is_writable():
            return {"ok": False, "error": "Write-gate off"}
        cfg = load_config()
        # Track the OLD whisper model so we can hot-apply a change to
        # the running TranscribeManager (audit U-7). Settings_save was
        # persisting the new model + reloading config, but the
        # TranscribeManager's loaded subprocess kept using the OLD
        # model — only a full app restart picked up the change.
        _old_whisper = (cfg.get("whisper_model") or "").strip()
        if data.get("output_dir"): cfg["output_dir"] = os.path.normpath(data["output_dir"])
        if data.get("video_out_dir"): cfg["video_out_dir"] = os.path.normpath(data["video_out_dir"])
        if data.get("whisper_model"): cfg["whisper_model"] = data["whisper_model"]
        if data.get("default_resolution"): cfg["default_resolution"] = data["default_resolution"]
        if data.get("log_mode") in ("Simple", "Verbose"):
            cfg["log_mode"] = data["log_mode"]
        # Index-tab persistence: archive roots + auto-index toggle + threshold.
        if isinstance(data.get("tp_archive_roots"), list):
            cfg["tp_archive_roots"] = [str(r) for r in data["tp_archive_roots"] if r]
        if "auto_index_enabled" in data:
            cfg["auto_index_enabled"] = bool(data["auto_index_enabled"])
        if "auto_index_threshold" in data:
            try:
                cfg["auto_index_threshold"] = max(1, min(9999, int(data["auto_index_threshold"])))
            except Exception as e: _log.debug("swallowed: %s", e)
        # Startup knobs — all three round-trip here.
        if "disk_scan_staleness_hours" in data:
            try:
                cfg["disk_scan_staleness_hours"] = max(0, min(10_000,
                    int(data["disk_scan_staleness_hours"])))
            except Exception as e: _log.debug("swallowed: %s", e)
        if "browse_preload_limit" in data:
            try:
                cfg["browse_preload_limit"] = max(1, min(100_000,
                    int(data["browse_preload_limit"])))
            except Exception as e: _log.debug("swallowed: %s", e)
        if "browse_preload_all" in data:
            cfg["browse_preload_all"] = bool(data["browse_preload_all"])
        # Subs table column visibility
        if "show_avg_size" in data:
            cfg["show_avg_size"] = bool(data["show_avg_size"])
        # Recent tab view mode — only accept known values.
        if data.get("recent_view_mode") in ("list", "grid"):
            cfg["recent_view_mode"] = data["recent_view_mode"]
        # .txt: transcript viewer text size (px). Bounded so a bad
        # value (e.g. NaN) can't render the Watch view unreadable.
        if "transcript_font_size" in data:
            try:
                _tx_fs = float(data["transcript_font_size"])
                if _tx_fs >= 8 and _tx_fs <= 40:
                    cfg["transcript_font_size"] = _tx_fs
            except Exception as e:
                _log.debug("swallowed: %s", e)
        # .txt: transcript pane width (CSS flex-basis, in px). Adjustable
        # via drag-splitter between video and transcript panels.
        if "transcript_pane_width" in data:
            try:
                _tx_pw = int(data["transcript_pane_width"])
                if _tx_pw >= 200 and _tx_pw <= 1400:
                    cfg["transcript_pane_width"] = _tx_pw
            except Exception as e:
                _log.debug("swallowed: %s", e)
        # Watch view caption overlay preferences. The watchActions.js
        # toolbar selects write these keys via settings_save, but until
        # this audit they had no save clause and were silently dropped.
        # Validated enums match the frontend's _CAP_SIZES / _CAP_BGS.
        if data.get("caption_overlay_size") in ("off", "small", "medium", "large"):
            cfg["caption_overlay_size"] = data["caption_overlay_size"]
        if data.get("caption_overlay_bg") in ("translucent", "outline", "none"):
            cfg["caption_overlay_bg"] = data["caption_overlay_bg"]
        # .txt: close-button behavior — "ask" (default modal),
        # "quit" (exit immediately), or "tray" (minimize to tray).
        if data.get("close_behavior") in ("ask", "quit", "tray"):
            cfg["close_behavior"] = data["close_behavior"]
        from backend.ytarchiver_config import save_config as _sc
        if not _sc(cfg):
            return {"ok": False, "error": "Save failed"}
        self._reload_config()
        # Push log mode into LogStreamer
        self._log_stream.simple_mode = (cfg["log_mode"] == "Simple")
        # Audit U-7: hot-apply Whisper model change so the next job
        # uses the new model without requiring a full app restart.
        # The GPU popover already exposes per-job swap via
        # transcribe_swap_model — route through the same path.
        _new_whisper = (cfg.get("whisper_model") or "").strip()
        if _new_whisper and _new_whisper != _old_whisper:
            try:
                if hasattr(self._transcribe, "swap_model"):
                    self._transcribe.swap_model(_new_whisper)
            except Exception as _e:
                # Log + continue — settings still saved successfully,
                # the user just needs to restart for the change to bite.
                try:
                    self._log_stream.emit_dim(
                        f" (whisper model swap deferred until restart: {_e})")
                except Exception as e:
                    _log.debug("swallowed: %s", e)
        return {"ok": True}


    # ─── yt-dlp version / update ───────────────────────────────────────

    def ytdlp_version(self):
        """Return current yt-dlp version string."""
        yt = sync_backend.find_yt_dlp()
        if not yt:
            return {"ok": False, "error": "yt-dlp not found"}
        try:
            import subprocess as _sp
            r = _sp.run([yt, "--version"], capture_output=True, text=True,
                        timeout=10, startupinfo=sync_backend._startupinfo)
            ver = (r.stdout or "").strip().split("\n")[0] or "unknown"
            return {"ok": True, "version": ver, "path": yt}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def ytdlp_update(self):
        """Run yt-dlp -U in a background thread; stream output to the log."""
        yt = sync_backend.find_yt_dlp()
        if not yt:
            return {"ok": False, "error": "yt-dlp not found"}
        def _run():
            import subprocess as _sp
            self._log_stream.emit([
                ["[Update] ", "update_head"],
                ["Updating yt-dlp...\n", "update_sep"],
            ])
            try:
                proc = _sp.Popen([yt, "-U"],
                                  stdout=_sp.PIPE, stderr=_sp.STDOUT,
                                  encoding="utf-8", errors="replace", bufsize=1,
                                  startupinfo=sync_backend._startupinfo)
                for line in proc.stdout:
                    self._log_stream.emit_dim(" " + line.rstrip())
                proc.wait()
                # check proc.returncode before declaring
                # success. Old code always emitted "update complete"
                # even on non-zero exit (most common cause: the yt-dlp
                # exe is locked by a running sync, so the self-update
                # fails but the banner still claimed it worked).
                if proc.returncode == 0:
                    self._log_stream.emit([["[Update] ", "update_head"],
                                            ["yt-dlp update complete.\n", "update_sep"]])
                else:
                    self._log_stream.emit_error(
                        f"yt-dlp update failed (exit code {proc.returncode}). "
                        "If a sync is running, stop it and try again — the "
                        ".exe can't be replaced while it's open.")
            except Exception as e:
                self._log_stream.emit_error(f"yt-dlp update failed: {e}")
            finally:
                self._log_stream.flush()
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}


    def set_parent_folder(self, path):
        """Update config['output_dir'] (gated by write env var)."""
        if not path:
            return {"ok": False, "error": "path required"}
        path = os.path.normpath(path)
        # verify the directory is accessible + writable before
        # we commit it. Previously any path was saved blindly, so a
        # read-only / unplugged / permission-denied path would be
        # accepted; later sync attempts would fail with cryptic
        # "write-gate blocked" errors. Probe with a real tmp file +
        # rmdir so we catch permission issues that os.access (advisory
        # on Windows) misses.
        if not os.path.isdir(path):
            return {"ok": False,
                    "error": f"Folder doesn't exist or isn't accessible: {path}"}
        _test_dir = os.path.join(path, ".ytarch-write-test")
        try:
            os.makedirs(_test_dir, exist_ok=True)
            try:
                os.rmdir(_test_dir)
            except OSError:
                pass
        except OSError as _pe:
            return {"ok": False,
                    "error": f"Folder isn't writable: {_pe}"}
        cfg = load_config()
        cfg["output_dir"] = path
        from backend.ytarchiver_config import save_config as _sc
        ok = _sc(cfg)
        if ok:
            self._reload_config()
            return {"ok": True, "path": path}
        return {"ok": False, "write_blocked": True, "path": path,
                "error": "Write-gate off"}
