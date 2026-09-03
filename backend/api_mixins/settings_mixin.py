"""
SettingsMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They prefer AppServices when present for config, log,
and transcribe dependencies, with legacy private Api attributes kept
as fallback state.
"""
from __future__ import annotations

import copy
import os
import re
import sys
import threading
import time
import uuid

from backend import sync as sync_backend
from backend import youtube_traffic
from backend.archive_capacity import normalize_archive_capacity_warning
from backend.services.job_supervisor import WorkAdmissionClosed
from backend.services.managed_work import start_managed_task
from backend.ytarchiver_config import (
    TRASH_RETENTION_CHANGE_GRACE_SECONDS,
    TRASH_RETENTION_DEFAULT_DAYS,
    TRASH_RETENTION_MAX_DAYS,
    config_is_writable,
    load_config,
)

from ._shared import _api_err, _log


def _parse_trash_retention_days(value):
    """Return a validated retention setting without lossy coercion."""
    if isinstance(value, bool):
        raise ValueError("boolean is not a retention period")
    if isinstance(value, int):
        days = value
    elif isinstance(value, float) and value.is_integer():
        days = int(value)
    elif isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()):
        days = int(value.strip())
    else:
        raise ValueError("retention period must be a whole number")
    if days != 0 and not 1 <= days <= TRASH_RETENTION_MAX_DAYS:
        raise ValueError(
            f"retention period must be 0 or 1-{TRASH_RETENTION_MAX_DAYS}")
    return days


def _stored_trash_retention_days(value, fallback=TRASH_RETENTION_DEFAULT_DAYS):
    try:
        return _parse_trash_retention_days(value)
    except (TypeError, ValueError):
        return fallback


def _stored_trash_retention_grace(value):
    try:
        grace = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    # NaN is the only float unequal to itself. Infinity is also unsuitable
    # for a persisted policy timestamp.
    if grace != grace or grace in (float("inf"), float("-inf")):
        return 0.0
    return max(0.0, grace)


class SettingsMixin:
    def _settings_services(self):
        return getattr(self, "services", None)

    def _settings_config(self):
        services = self._settings_services()
        if services is not None:
            return services.fresh_config()
        cfg = getattr(self, "_config", None)
        if cfg is not None:
            return cfg
        return load_config()

    def _settings_fresh_config(self):
        services = self._settings_services()
        if services is not None:
            return services.fresh_config()
        return load_config()

    def _settings_save_config(self, cfg):
        services = self._settings_services()
        if services is not None:
            return services.save_config(cfg)
        from backend.ytarchiver_config import save_config as _save_config
        return _save_config(cfg)

    def _settings_commit_candidate(self, original, candidate):
        """Commit only fields changed by this Settings request."""
        missing = object()
        updates = {
            key: copy.deepcopy(value)
            for key, value in candidate.items()
            if original.get(key, missing) != value
        }
        removals = {
            key for key in original
            if key not in candidate
        }

        def _mutate(live):
            for key in removals:
                live.pop(key, None)
            live.update(copy.deepcopy(updates))

        try:
            services = self._settings_services()
            if services is not None and hasattr(services, "mutate_config"):
                _result, snapshot = services.mutate_config(_mutate)
            elif services is not None:
                snapshot = services.fresh_config()
                _mutate(snapshot)
                if not services.save_config(snapshot):
                    raise OSError("config save failed")
            else:
                from backend.ytarchiver_config import update_config
                _result, snapshot = update_config(_mutate)
            return True, snapshot
        except Exception as exc:
            _log.warning("settings transaction failed: %s", exc)
            return False, candidate

    def _settings_log_stream(self):
        services = self._settings_services()
        stream = (getattr(services, "log_stream", None)
                  if services is not None else None)
        return stream if stream is not None else self._log_stream

    def _settings_transcribe(self):
        services = self._settings_services()
        transcribe = (getattr(services, "transcribe", None)
                      if services is not None else None)
        return transcribe if transcribe is not None else self._transcribe


    def set_log_mode(self, mode):
        """UI toggled log mode. Pushes filter state into LogStreamer +
        persists (gated). Returns the {ok, error?} dict shape that the
        rest of the Api surface uses, instead of a raw bool — JS
        callers that read `.ok` used to see `undefined` and couldn't
        tell success from failure (audit: settings_mixin H6)."""
        if mode not in ("Simple", "Verbose"):
            return {"ok": False, "error": "Invalid mode"}
        persisted = False
        save_exc = ""
        with SettingsMixin._settings_save_lock:
            try:
                cfg = self._settings_fresh_config()
                original_cfg = copy.deepcopy(cfg)
                cfg["log_mode"] = mode
                persisted, _saved_cfg = self._settings_commit_candidate(
                    original_cfg, cfg)
            except Exception as _se:
                persisted = False
                save_exc = str(_se)
            if persisted:
                cfg_ref = getattr(self, "_config", None)
                if cfg_ref is not None:
                    cfg_ref["log_mode"] = mode
                # LogStreamer respects `simple_mode` when filtering dim/verbose lines
                self._settings_log_stream().simple_mode = (mode == "Simple")
            else:
                # Save failed — reload from disk so in-memory state
                # matches whatever's actually persisted, not the
                # caller's intent.
                try: self._reload_config()
                except Exception: pass
        if persisted:
            return {"ok": True}
        return {"ok": False, "error": save_exc or "Config write failed"}


    # ─── Autorun scheduler ─────────────────────────────────────────────

    def autorun_set(self, label_or_mins):
        """Accept a label like '30 min' / '1 hr' / 'Off' OR an integer
        minutes. Booleans are rejected explicitly so a JS coercion
        (e.g. `autorun_set(false)` meaning "off") doesn't silently
        land as `int(True)==1` minute (audit: settings_mixin H7)."""
        if isinstance(label_or_mins, bool):
            return {"ok": False,
                    "error": "Pass a label string ('Off', '30 min', ...) "
                             "or an integer minute count, not a boolean."}
        if isinstance(label_or_mins, str):
            try:
                return self._autorun.set_interval_label(label_or_mins)
            except ValueError as _ve:
                return {"ok": False, "error": f"Invalid label: {_ve}"}
        try:
            mins = int(label_or_mins)
        except (TypeError, ValueError) as _ve:
            return {"ok": False, "error": f"Bad value: {_ve}"}
        try:
            return self._autorun.set_interval_mins(mins)
        except ValueError as _ve:
            return {"ok": False, "error": f"Invalid minutes: {_ve}"}


    def autorun_state(self):
        return self._autorun.get_state()

    def autorun_set_mode(self, mode):
        """Switch auto-sync firing between 'timer' (countdown) and 'clock'
        (wall-clock aligned). Persists + reschedules the pending fire."""
        try:
            return self._autorun.set_mode(mode)
        except Exception as e:
            return _api_err("CONFIG_SAVE_FAILED", str(e))


    # ─── Launch at boot (Windows Registry) ────────────────────────────

    def autorun_set_clock_time(self, minutes):
        """Set a 12h/24h clock schedule in minutes after midnight."""
        try:
            return self._autorun.set_clock_anchor(int(minutes))
        except (TypeError, ValueError) as e:
            return _api_err("INVALID_CLOCK_TIME", str(e))
        except Exception as e:
            return _api_err("CONFIG_SAVE_FAILED", str(e))

    _BOOT_REG_PATH = r"Software\Microsoft\Windows\CurrentVersion\Run"
    _BOOT_REG_NAME = "YTArchiver"

    def launch_at_boot_get(self):
        """Read current boot-launch state from the Windows Registry."""
        try:
            import winreg as _wr
            with _wr.OpenKey(_wr.HKEY_CURRENT_USER, self._BOOT_REG_PATH) as k:
                try:
                    val, _ = _wr.QueryValueEx(k, self._BOOT_REG_NAME)
                    return {"enabled": True,
                            "minimized": "--start-minimized" in str(val)}
                except FileNotFoundError:
                    return {"enabled": False, "minimized": False}
        except Exception as e:
            _log.debug("launch_at_boot_get: %s", e)
            return {"enabled": False, "minimized": False}

    def launch_at_boot_set(self, enabled, minimized=False):
        """Set or clear the Windows Registry boot-launch entry."""
        try:
            import winreg as _wr
            if enabled:
                exe = sys.executable
                cmd = f'"{exe}"'
                if minimized:
                    cmd += " --start-minimized"
                with _wr.OpenKey(_wr.HKEY_CURRENT_USER, self._BOOT_REG_PATH,
                                 access=_wr.KEY_SET_VALUE) as k:
                    _wr.SetValueEx(k, self._BOOT_REG_NAME, 0, _wr.REG_SZ, cmd)
            else:
                try:
                    with _wr.OpenKey(_wr.HKEY_CURRENT_USER, self._BOOT_REG_PATH,
                                     access=_wr.KEY_SET_VALUE) as k:
                        _wr.DeleteValue(k, self._BOOT_REG_NAME)
                except FileNotFoundError:
                    pass
            return {"ok": True}
        except Exception as e:
            _log.warning("launch_at_boot_set failed: %s", e)
            return _api_err("INTERNAL_ERROR", str(e))

    # ─── Settings dialog: load / save all tunables ─────────────────────

    def settings_load(self):
        cfg = self._settings_config()
        cap = normalize_archive_capacity_warning(cfg)
        traffic = youtube_traffic.status(cfg)
        return {
            "output_dir": cfg.get("output_dir", ""),
            "video_out_dir": cfg.get("video_out_dir", ""),
            "whisper_model": cfg.get("whisper_model", "small"),
            "default_resolution": cfg.get("default_resolution", "720"),
            "log_mode": cfg.get("log_mode", "Simple"),
            "legacy_subs_tab": bool(cfg.get("legacy_subs_tab", False)),
            # yt-dlp release channel the updater targets: "stable" or
            # "nightly" (beta). Surfaced under Downloader updates.
            "ytdlp_channel": (cfg.get("ytdlp_channel") or "stable"),
            "ytdlp_update_mode": (
                cfg.get("ytdlp_update_mode")
                if cfg.get("ytdlp_update_mode") in
                ("automatic", "notify", "off")
                else ("off" if int(
                    cfg.get("ytdlp_update_check_days", 1) or 0) == 0
                      else "automatic")
            ),
            "ytdlp_update_check_days": int(
                cfg.get("ytdlp_update_check_days", 1) or 0),
            "last_ytdlp_update_check_ts": float(
                cfg.get("last_ytdlp_update_check_ts", 0) or 0),
            # Health > Search Index surfaces these directly — must round-trip.
            "tp_archive_roots": list(cfg.get("tp_archive_roots") or []),
            "auto_index_enabled": bool(cfg.get("auto_index_enabled", False)),
            "auto_index_threshold": int(cfg.get("auto_index_threshold", 10) or 10),
            # Storage/library background-check controls surfaced in Settings.
            "disk_scan_staleness_hours": int(cfg.get("disk_scan_staleness_hours", 24) or 0),
            "archive_capacity_warning_mode": cap["mode"],
            "archive_capacity_warning_percent": cap["percent"],
            "archive_capacity_warning_free_gb": cap["free_gb"],
            "last_disk_scan_ts": float(cfg.get("last_disk_scan_ts", 0) or 0),
            "last_backup_ts": float(cfg.get("last_backup_ts", 0) or 0),
            "last_auto_backup_ts": float(
                cfg.get("last_auto_backup_ts", 0) or 0),
            # Automatic full-backup cadence shown beside manual backup tools.
            "auto_backup_interval": (cfg.get("auto_backup_interval")
                                     or "off"),
            "trash_retention_days": _stored_trash_retention_days(
                cfg.get("trash_retention_days")),
            # Read-only policy metadata. settings_save always computes this
            # server-side so a caller cannot bypass a safety grace period.
            "trash_retention_grace_until_ts": (
                _stored_trash_retention_grace(
                    cfg.get("trash_retention_grace_until_ts"))),
            # Subs table column visibility toggles. Default False for
            # new users — the column is optional polish, not core info.
            "show_avg_size": bool(cfg.get("show_avg_size", False)),
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
            "caption_overlay_mode": (cfg.get("caption_overlay_mode") or ""),
            "youtube_traffic_mode": traffic["mode"],
            "youtube_traffic_custom_daily": int(
                cfg.get("youtube_traffic_custom_daily", 750) or 750),
            "youtube_traffic_custom_hourly": int(
                cfg.get("youtube_traffic_custom_hourly", 90) or 90),
            "youtube_traffic_custom_min_gap": int(
                cfg.get("youtube_traffic_custom_min_gap", 10) or 0),
            "youtube_traffic_custom_max_gap": int(
                cfg.get("youtube_traffic_custom_max_gap", 20) or 0),
            "youtube_traffic": traffic,
        }


    # Process-wide lock around settings_save's load→mutate→save. Old
    # code did this unlocked, so two near-simultaneous calls (e.g.
    # window_state.save firing while the user clicked Save in Settings)
    # could both load_config, mutate independent copies, and the loser
    # silently overwrote the winner (audit: settings_mixin.py:99-196).
    _settings_save_lock = threading.RLock()

    def settings_save(self, data):
        admission = getattr(self, "_work_admission_error", None)
        if callable(admission):
            blocked = admission("a settings change")
            if blocked is not None:
                return blocked
        if not config_is_writable():
            return {
                "ok": False,
                "error": ("Settings are temporarily read-only. Restart "
                          "YTArchiver and try again."),
            }
        with SettingsMixin._settings_save_lock:
            return self._settings_save_inner(data)

    def _settings_save_inner(self, data):
        cfg = self._settings_fresh_config()
        original_cfg = copy.deepcopy(cfg)
        _old_ytdlp_channel = str(
            cfg.get("ytdlp_channel") or "stable").strip().lower()
        _old_update_mode = self._normalize_ytdlp_update_mode(cfg)
        _old_trash_retention_days = _stored_trash_retention_days(
            cfg.get("trash_retention_days"), fallback=0)
        _trash_retention_changed = False
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
        if "legacy_subs_tab" in data:
            cfg["legacy_subs_tab"] = bool(data["legacy_subs_tab"])
        # yt-dlp release channel — only accept the two known values.
        if data.get("ytdlp_channel") in ("stable", "nightly"):
            if data["ytdlp_channel"] != cfg.get("ytdlp_channel"):
                # A channel switch needs a fresh remote comparison even when
                # the previous channel was checked moments ago.
                cfg["last_ytdlp_update_check_ts"] = 0.0
                cfg["ytdlp_update_pending_version"] = ""
                cfg["ytdlp_update_pending_channel"] = ""
            cfg["ytdlp_channel"] = data["ytdlp_channel"]
        if data.get("ytdlp_update_mode") in ("automatic", "notify", "off"):
            cfg["ytdlp_update_mode"] = data["ytdlp_update_mode"]
            if data["ytdlp_update_mode"] != "automatic":
                cfg["ytdlp_update_pending_version"] = ""
                cfg["ytdlp_update_pending_channel"] = ""
            if (data["ytdlp_update_mode"] != "off"
                    and data["ytdlp_update_mode"] != _old_update_mode):
                # Enabling checks should take effect now, not after the old
                # interval happens to expire.
                cfg["last_ytdlp_update_check_ts"] = 0.0
                try:
                    if int(cfg.get("ytdlp_update_check_days", 0) or 0) < 1:
                        cfg["ytdlp_update_check_days"] = 1
                except (TypeError, ValueError):
                    cfg["ytdlp_update_check_days"] = 1
        if "ytdlp_update_check_days" in data:
            try:
                cfg["ytdlp_update_check_days"] = max(
                    1, min(365, int(data["ytdlp_update_check_days"])))
            except Exception as e:
                _log.debug("swallowed: %s", e)
        # Index-tab persistence: archive roots + auto-index toggle + threshold.
        if isinstance(data.get("tp_archive_roots"), list):
            primary_key = os.path.normcase(os.path.abspath(os.path.normpath(
                str(cfg.get("output_dir") or "").strip()
            ))) if cfg.get("output_dir") else ""
            clean_roots: list[str] = []

            def _within(path: str, root: str) -> bool:
                if not path or not root:
                    return False
                try:
                    return os.path.commonpath([path, root]) == root
                except (OSError, ValueError):
                    return False

            for raw_root in data["tp_archive_roots"]:
                value = str(raw_root or "").strip()
                if not value:
                    continue
                normalized = os.path.abspath(os.path.normpath(value))
                key = os.path.normcase(normalized)
                # The primary archive already covers all of its descendants.
                if (key == primary_key
                        or (primary_key and (
                            _within(key, primary_key)
                            or _within(primary_key, key)))):
                    continue
                clean_keys = [os.path.normcase(path) for path in clean_roots]
                if key in clean_keys or any(
                        _within(key, existing) for existing in clean_keys):
                    continue
                # If a broader root is added after a nested one, keep only the
                # broader root. The index sweep walks recursively.
                clean_roots = [
                    path for path in clean_roots
                    if not _within(os.path.normcase(path), key)
                ]
                clean_roots.append(normalized)
            cfg["tp_archive_roots"] = clean_roots
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
        # v80 auto-backup cadence — only the four known values.
        if data.get("auto_backup_interval") in ("off", "daily", "weekly",
                                                "monthly"):
            cfg["auto_backup_interval"] = data["auto_backup_interval"]
        if "trash_retention_days" in data:
            try:
                _new_trash_retention_days = _parse_trash_retention_days(
                    data["trash_retention_days"])
            except (TypeError, ValueError):
                return {
                    "ok": False,
                    "error": (
                        "Trash retention must be 0 (Never) or a whole "
                        f"number from 1 to {TRASH_RETENTION_MAX_DAYS} days."
                    ),
                }
            _trash_retention_changed = (
                _new_trash_retention_days != _old_trash_retention_days)
            cfg["trash_retention_days"] = _new_trash_retention_days
            if _trash_retention_changed:
                if _new_trash_retention_days == 0:
                    cfg["trash_retention_grace_until_ts"] = 0.0
                elif (_old_trash_retention_days == 0
                      or _new_trash_retention_days
                      < _old_trash_retention_days):
                    # Enabling cleanup or shortening its window may make old
                    # entries newly eligible. Preserve any longer existing
                    # grace and guarantee at least 24 hours to reconsider.
                    cfg["trash_retention_grace_until_ts"] = max(
                        _stored_trash_retention_grace(cfg.get(
                            "trash_retention_grace_until_ts")),
                        time.time() + TRASH_RETENTION_CHANGE_GRACE_SECONDS,
                    )
        if data.get("archive_capacity_warning_mode") in ("percent", "free_gb"):
            cfg["archive_capacity_warning_mode"] = data["archive_capacity_warning_mode"]
        if "archive_capacity_warning_percent" in data:
            try:
                cfg["archive_capacity_warning_percent"] = max(1, min(100,
                    int(data["archive_capacity_warning_percent"])))
            except Exception as e: _log.debug("swallowed: %s", e)
        if "archive_capacity_warning_free_gb" in data:
            try:
                cfg["archive_capacity_warning_free_gb"] = max(1, min(1_000_000,
                    int(data["archive_capacity_warning_free_gb"])))
            except Exception as e: _log.debug("swallowed: %s", e)
        # Subs table column visibility
        if "show_avg_size" in data:
            cfg["show_avg_size"] = bool(data["show_avg_size"])
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
        # Validated enums match the frontend's caption preference sets.
        if data.get("caption_overlay_size") in ("off", "small", "medium", "large"):
            cfg["caption_overlay_size"] = data["caption_overlay_size"]
        if data.get("caption_overlay_bg") in ("translucent", "outline", "none"):
            cfg["caption_overlay_bg"] = data["caption_overlay_bg"]
        if data.get("caption_overlay_mode") in ("single", "phrase3", "default"):
            cfg["caption_overlay_mode"] = data["caption_overlay_mode"]
        # .txt: close-button behavior — "ask" (default modal),
        # "quit" (exit immediately), or "tray" (minimize to tray).
        if data.get("close_behavior") in ("ask", "quit", "tray"):
            cfg["close_behavior"] = data["close_behavior"]
        if data.get("youtube_traffic_mode") in (
                "conservative", "balanced", "custom", "unlimited"):
            cfg["youtube_traffic_mode"] = data["youtube_traffic_mode"]
        _budget_autosync_disabled = False
        if (cfg.get("youtube_traffic_mode") == "unlimited"
                and int(cfg.get("autorun_interval", 0) or 0) == -1):
            cfg["autorun_interval"] = 0
            _budget_autosync_disabled = True
        _traffic_bounds = {
            "youtube_traffic_custom_daily": (1, 100_000),
            "youtube_traffic_custom_hourly": (1, 10_000),
            "youtube_traffic_custom_min_gap": (0, 3600),
            "youtube_traffic_custom_max_gap": (0, 3600),
        }
        for _key, (_low, _high) in _traffic_bounds.items():
            if _key not in data:
                continue
            try:
                cfg[_key] = max(_low, min(_high, int(data[_key])))
            except (TypeError, ValueError):
                return {"ok": False, "error": f"Invalid {_key} value"}
        if (cfg.get("youtube_traffic_custom_max_gap", 20)
                < cfg.get("youtube_traffic_custom_min_gap", 10)):
            cfg["youtube_traffic_custom_max_gap"] = int(
                cfg.get("youtube_traffic_custom_min_gap", 10))
        saved, committed_cfg = self._settings_commit_candidate(
            original_cfg, cfg)
        if not saved:
            return {"ok": False, "error": "Save failed"}
        cfg = committed_cfg
        self._reload_config()
        _new_ytdlp_channel = str(
            cfg.get("ytdlp_channel") or "stable").strip().lower()
        _new_update_mode = self._normalize_ytdlp_update_mode(cfg)
        if (_new_ytdlp_channel != _old_ytdlp_channel
                or _new_update_mode != _old_update_mode):
            # A request waiting for an idle window must not outlive the
            # setting that created it. Running updates are allowed to finish;
            # only not-yet-started work is invalidated here.
            self._ensure_ytdlp_update_runtime()
            with self._ytdlp_update_state_lock:
                pending = self._ytdlp_update_pending
                if (pending is not None
                        and (str(pending.get("channel") or "stable").lower()
                             != _new_ytdlp_channel
                             or (pending.get("automatic")
                                 and _new_update_mode != "automatic"))):
                    self._ytdlp_update_pending = None
        # Push log mode into LogStreamer
        self._settings_log_stream().simple_mode = (cfg["log_mode"] == "Simple")
        # Audit U-7: hot-apply Whisper model change so the next job
        # uses the new model without requiring a full app restart.
        # The GPU popover already exposes per-job swap via
        # transcribe_swap_model — route through the same path.
        _new_whisper = (cfg.get("whisper_model") or "").strip()
        if _new_whisper and _new_whisper != _old_whisper:
            try:
                transcribe = self._settings_transcribe()
                if hasattr(transcribe, "swap_model"):
                    transcribe.swap_model(_new_whisper)
            except Exception as _e:
                # Log + continue — settings still saved successfully,
                # the user just needs to restart for the change to bite.
                try:
                    self._settings_log_stream().emit_dim(
                        f" (whisper model swap deferred until restart: {_e})")
                except Exception as e:
                    _log.debug("swallowed: %s", e)
        if any(key in data for key in (
                "ytdlp_channel", "ytdlp_update_mode",
                "ytdlp_update_check_days")):
            # Wake the long-lived monitor so a newly-enabled or shortened
            # interval is applied immediately without restarting the app.
            try:
                self.wake_ytdlp_update_monitor()
            except Exception as e:
                _log.debug("yt-dlp update monitor wake failed: %s", e)
        if _trash_retention_changed:
            # The scheduler is optional during tests and early application
            # construction. Wake it only after the config transaction has
            # succeeded so failed saves cannot act on an uncommitted policy.
            scheduler = getattr(self, "_trash_retention", None)
            if scheduler is None:
                scheduler = getattr(
                    self, "_trash_retention_scheduler", None)
            wake = getattr(scheduler, "wake", None)
            if callable(wake):
                try:
                    wake()
                except Exception as e:
                    _log.debug("Trash retention scheduler wake failed: %s", e)
        return {
            "ok": True,
            "budget_autosync_disabled": _budget_autosync_disabled,
        }

    def youtube_traffic_status(self):
        """Live rolling usage and autosync recommendation for Settings."""
        try:
            return youtube_traffic.status(self._settings_fresh_config())
        except Exception as e:
            return _api_err("INTERNAL_ERROR", str(e))


    # ─── yt-dlp version / update ───────────────────────────────────────

    # Session-cached yt-dlp version. The subprocess timeout=10 was
    # blocking the JS-bridge thread on every About-dialog open or
    # diagnostics scan — when Defender was scanning yt-dlp.exe this
    # could feel like a 10s UI freeze (audit: settings_mixin.py:
    # 206-213). Cache by yt-dlp path so a user pointing at a new
    # binary still re-probes.
    _ytdlp_version_cache: dict[str, dict] = {}
    _ytdlp_update_check_lock = threading.Lock()
    _ytdlp_update_check_running = False
    _ytdlp_runtime_init_lock = threading.Lock()
    _ytdlp_release_apis = {
        "stable": "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest",
        "nightly": (
            "https://api.github.com/repos/yt-dlp/"
            "yt-dlp-nightly-builds/releases/latest"
        ),
    }
    _ytdlp_update_max_attempts = 5

    def ytdlp_version(self):
        """Return current yt-dlp version string."""
        yt = sync_backend.find_yt_dlp()
        if not yt:
            return {"ok": False, "error": "yt-dlp not found"}
        _cached = SettingsMixin._ytdlp_version_cache.get(yt)
        if _cached is not None:
            return _cached
        try:
            import subprocess as _sp
            r = _sp.run([yt, "--version"], capture_output=True, text=True,
                        timeout=10, startupinfo=sync_backend._startupinfo)
            ver = (r.stdout or "").strip().split("\n")[0] or "unknown"
            _result = {
                "ok": True,
                "version": ver,
                "path": yt,
                "managed": self._is_managed_ytdlp(yt),
                "auto_updatable": self._can_auto_update_ytdlp(yt),
            }
            SettingsMixin._ytdlp_version_cache[yt] = _result
            return _result
        except Exception as e:
            return _api_err("MISSING_DEPENDENCY", str(e))


    @staticmethod
    def _ytdlp_version_tuple(version):
        """Return a comparable numeric tuple for stable or nightly versions."""
        match = re.search(r"(\d{4}(?:\.\d+){2,})", str(version or ""))
        if not match:
            return ()
        try:
            return tuple(int(part) for part in match.group(1).split("."))
        except ValueError:
            return ()

    @staticmethod
    def _normalize_ytdlp_update_mode(cfg):
        mode = str((cfg or {}).get("ytdlp_update_mode") or "").strip().lower()
        if mode in ("automatic", "notify", "off"):
            return mode
        try:
            return ("off" if int(
                (cfg or {}).get("ytdlp_update_check_days", 1) or 0) == 0
                    else "automatic")
        except (TypeError, ValueError):
            return "automatic"

    @staticmethod
    def _is_managed_ytdlp(path):
        """True only for YTArchiver's app-data-managed executable."""
        if not path:
            return False
        try:
            from backend.ytarchiver_config import APP_DATA_DIR
            actual = os.path.normcase(os.path.abspath(os.fspath(path)))
            managed = os.path.normcase(os.path.abspath(os.fspath(
                APP_DATA_DIR / "bin" / "yt-dlp.exe")))
            return actual == managed
        except Exception:
            return False

    def _can_auto_update_ytdlp(self, path):
        """True for app-managed or recognizable standalone yt-dlp builds.

        Official Windows standalone builds are multi-megabyte PE files and
        implement yt-dlp's own ``--update-to`` command even when the user has
        placed them somewhere else on PATH. Pip-generated console launchers
        are tiny wrappers and must remain under their package manager.
        """
        if self._is_managed_ytdlp(path):
            return True
        if not path:
            return False
        try:
            actual = os.path.realpath(os.fspath(path))
            if not os.path.isfile(actual) or os.path.getsize(actual) < 1_000_000:
                return False
            if os.name == "nt":
                if not actual.lower().endswith(".exe"):
                    return False
                with open(actual, "rb") as executable:
                    return executable.read(2) == b"MZ"
            return os.access(actual, os.X_OK)
        except OSError:
            return False

    def _ensure_ytdlp_update_runtime(self):
        if hasattr(self, "_ytdlp_update_state_lock"):
            return
        with SettingsMixin._ytdlp_runtime_init_lock:
            if hasattr(self, "_ytdlp_update_state_lock"):
                return
            self._ytdlp_update_state_lock = threading.Lock()
            self._ytdlp_update_pending = None
            self._ytdlp_update_running = False
            self._ytdlp_monitor_wake = threading.Event()
            self._ytdlp_monitor_stop = threading.Event()
            self._ytdlp_monitor_thread = None
            self._ytdlp_update_thread = None
            self._ytdlp_check_not_before = 0.0
            self._ytdlp_update_active_cancel = None

    def wake_ytdlp_update_monitor(self):
        self._ensure_ytdlp_update_runtime()
        self._ytdlp_monitor_wake.set()
        return {"ok": True}

    def start_ytdlp_update_monitor(self):
        """Start the persistent due-check/idle-install monitor once."""
        admission = getattr(self, "_work_admission_error", None)
        if callable(admission):
            blocked = admission("the yt-dlp update monitor")
            if blocked is not None:
                return blocked
        self._ensure_ytdlp_update_runtime()
        with self._ytdlp_update_state_lock:
            thread = self._ytdlp_monitor_thread
            if thread is not None and thread.is_alive():
                self._ytdlp_monitor_wake.set()
                return {"ok": True, "started": False, "running": True}
            self._ytdlp_monitor_stop.clear()
            try:
                thread = start_managed_task(
                    self,
                    owner="monitor-ytdlp",
                    label="yt-dlp update monitor",
                    task_id=f"ytdlp-monitor-{uuid.uuid4().hex}",
                    cancel=self._ytdlp_monitor_stop,
                    target=self._ytdlp_update_monitor_loop,
                    name="ytdlp-update-monitor",
                    thread_factory=threading.Thread,
                )
            except WorkAdmissionClosed as exc:
                self._ytdlp_monitor_stop.set()
                return {"ok": False, "started": False, "error": str(exc)}
            self._ytdlp_monitor_thread = thread
        return {"ok": True, "started": True}

    def stop_ytdlp_update_monitor(self, timeout=1.0):
        """Stop future checks/installs during application shutdown."""
        if not hasattr(self, "_ytdlp_monitor_stop"):
            return {"ok": True, "stopped": False}
        self._ytdlp_monitor_stop.set()
        self._ytdlp_monitor_wake.set()
        with self._ytdlp_update_state_lock:
            pending = self._ytdlp_update_pending
            pending_cancel = (
                pending.get("cancel_event")
                if isinstance(pending, dict) else None)
            active_cancel = self._ytdlp_update_active_cancel
        for cancel_event in (pending_cancel, active_cancel):
            if cancel_event is not None:
                cancel_event.set()
        thread = getattr(self, "_ytdlp_monitor_thread", None)
        if (thread is not None and thread.is_alive()
                and thread is not threading.current_thread()):
            thread.join(timeout=max(0.0, float(timeout)))
        return {"ok": True, "stopped": True}

    def _ytdlp_update_monitor_loop(self):
        while not self._ytdlp_monitor_stop.is_set():
            try:
                wait_seconds = self._ytdlp_update_monitor_once()
            except Exception as exc:
                _log.debug("yt-dlp update monitor tick failed: %s", exc)
                wait_seconds = 900.0
            self._ytdlp_monitor_wake.wait(
                timeout=max(1.0, min(float(wait_seconds), 900.0)))
            self._ytdlp_monitor_wake.clear()

    def _ytdlp_update_monitor_once(self, now=None):
        """Run one scheduler tick; separated for deterministic tests."""
        self._ensure_ytdlp_update_runtime()
        now = time.time() if now is None else float(now)
        cfg = self._settings_fresh_config()
        mode = self._normalize_ytdlp_update_mode(cfg)
        configured_channel = str(
            cfg.get("ytdlp_channel") or "stable").strip().lower()

        with self._ytdlp_update_state_lock:
            pending = self._ytdlp_update_pending
            if (pending and pending.get("automatic")
                    and (mode != "automatic"
                         or str(pending.get("channel") or "stable").lower()
                         != configured_channel)):
                self._ytdlp_update_pending = None
                pending = None

        # Rehydrate an update discovered before a crash/restart. The remote
        # check timestamp is deliberately not advanced until it installs.
        persisted_version = str(
            cfg.get("ytdlp_update_pending_version") or "").strip()
        persisted_channel = str(
            cfg.get("ytdlp_update_pending_channel") or "").strip().lower()
        if (mode == "automatic" and persisted_version
                and persisted_channel == configured_channel
                and pending is None):
            yt = sync_backend.find_yt_dlp()
            if yt and self._can_auto_update_ytdlp(yt):
                self._queue_ytdlp_update(
                    yt=yt, channel=persisted_channel,
                    automatic=True, latest=persisted_version,
                    record_check=True)

        with self._ytdlp_update_state_lock:
            pending = self._ytdlp_update_pending
            running = self._ytdlp_update_running
        if running:
            return 30.0
        if pending:
            result = self._maybe_start_ytdlp_update(now=now)
            if result.get("started"):
                return 30.0
            with self._ytdlp_update_state_lock:
                pending = self._ytdlp_update_pending or {}
                not_before = float(pending.get("not_before") or 0.0)
            return max(3.0, min(60.0, not_before - now))

        if mode == "off":
            return 900.0
        try:
            days = max(1, min(365, int(
                cfg.get("ytdlp_update_check_days", 1) or 1)))
            last_check = float(
                cfg.get("last_ytdlp_update_check_ts", 0) or 0)
        except (TypeError, ValueError):
            days, last_check = 1, 0.0
        due_at = last_check + (days * 86_400) if last_check > 0 else now
        not_before = max(due_at, float(self._ytdlp_check_not_before or 0.0))
        if now >= not_before:
            result = self.check_ytdlp_update()
            return 60.0 if result.get("started") else 900.0
        return max(1.0, min(900.0, not_before - now))

    def _ytdlp_update_busy(self):
        """Conservative idle check before replacing the yt-dlp executable."""
        stop = getattr(self, "_ytdlp_monitor_stop", None)
        if stop is not None and stop.is_set():
            return True
        for method_name in ("sync_is_running", "archive_single_is_running"):
            try:
                method = getattr(self, method_name, None)
                if callable(method) and method():
                    return True
            except Exception:
                pass
        try:
            from backend.sync.active_state import is_sync_work_active
            if is_sync_work_active():
                return True
        except Exception:
            pass
        try:
            from backend.process_runner import PROCESS_REGISTRY
            if PROCESS_REGISTRY.alive_count() > 0:
                return True
        except Exception:
            pass
        # Some older specialized paths still launch yt-dlp directly instead
        # of registering it. Scan this app's child tree as a second guard.
        try:
            import psutil
            parent = psutil.Process(os.getpid())
            for child in parent.children(recursive=True):
                try:
                    name = (child.name() or "").lower()
                    if "yt-dlp" in name or "yt_dlp" in name:
                        return True
                except Exception:
                    continue
        except Exception:
            pass
        return False

    def _push_ytdlp_update_status(self, status, message, version=""):
        window = getattr(self, "_window", None)
        if window is None:
            return
        try:
            import json as _json
            payload = _json.dumps({
                "status": str(status),
                "message": str(message),
                "version": str(version or ""),
            })
            window.evaluate_js(
                "window._onYtdlpUpdateStatus && "
                f"window._onYtdlpUpdateStatus({payload});")
        except Exception as exc:
            _log.debug("yt-dlp update UI push failed: %s", exc)

    def _record_ytdlp_update_check(self, checked_at, *, clear_pending=True):
        """Persist a completed check/update without disturbing other fields."""
        with SettingsMixin._settings_save_lock:
            cfg = self._settings_fresh_config()
            original_cfg = copy.deepcopy(cfg)
            cfg["last_ytdlp_update_check_ts"] = float(checked_at)
            if clear_pending:
                cfg["ytdlp_update_pending_version"] = ""
                cfg["ytdlp_update_pending_channel"] = ""
            saved, _snapshot = self._settings_commit_candidate(
                original_cfg, cfg)
            if not saved:
                return False
            cached = getattr(self, "_config", None)
            if isinstance(cached, dict):
                cached["last_ytdlp_update_check_ts"] = float(checked_at)
                if clear_pending:
                    cached["ytdlp_update_pending_version"] = ""
                    cached["ytdlp_update_pending_channel"] = ""
            return True

    def _record_ytdlp_pending_update(self, latest, channel):
        """Persist a discovered automatic update until install succeeds."""
        with SettingsMixin._settings_save_lock:
            cfg = self._settings_fresh_config()
            original_cfg = copy.deepcopy(cfg)
            cfg["ytdlp_update_pending_version"] = str(latest or "")
            cfg["ytdlp_update_pending_channel"] = str(channel or "stable")
            saved, _snapshot = self._settings_commit_candidate(
                original_cfg, cfg)
            if not saved:
                return False
            cached = getattr(self, "_config", None)
            if isinstance(cached, dict):
                cached["ytdlp_update_pending_version"] = str(latest or "")
                cached["ytdlp_update_pending_channel"] = str(
                    channel or "stable")
            return True

    def _queue_ytdlp_update(self, *, yt, channel, automatic,
                            latest="", current="", record_check=False):
        """Coalesce one update request and start it when safely idle."""
        admission = getattr(self, "_work_admission_error", None)
        if callable(admission):
            blocked = admission("a yt-dlp update")
            if blocked is not None:
                return blocked
        self._ensure_ytdlp_update_runtime()
        payload = {
            "yt": str(yt),
            "channel": channel if channel in ("stable", "nightly")
                       else "stable",
            "automatic": bool(automatic),
            "latest": str(latest or ""),
            "current": str(current or ""),
            "record_check": bool(record_check),
            "attempt": 0,
            "not_before": 0.0,
            "idle_sampled": False,
            "task_id": f"ytdlp-update-{uuid.uuid4().hex}",
            "cancel_event": threading.Event(),
        }
        with self._ytdlp_update_state_lock:
            if self._ytdlp_update_running:
                return {"ok": True, "started": False, "running": True}
            existing = self._ytdlp_update_pending
            if existing is not None:
                # A user-clicked request takes priority over an automatic one;
                # otherwise keep the newest release identity/channel.
                if existing.get("automatic") is False and automatic:
                    return {"ok": True, "started": False, "pending": True}
                payload["attempt"] = int(existing.get("attempt") or 0)
            self._ytdlp_update_pending = payload
        self._ytdlp_monitor_wake.set()
        return self._maybe_start_ytdlp_update()

    def _maybe_start_ytdlp_update(self, now=None):
        """Start the coalesced request after a stable idle window."""
        self._ensure_ytdlp_update_runtime()
        now = time.time() if now is None else float(now)
        with self._ytdlp_update_state_lock:
            if self._ytdlp_update_running:
                return {"ok": True, "started": False, "running": True}
            payload = self._ytdlp_update_pending
            if payload is None:
                return {"ok": True, "started": False, "pending": False}
            if now < float(payload.get("not_before") or 0.0):
                return {"ok": True, "started": False, "pending": True}

        if self._ytdlp_update_busy():
            with self._ytdlp_update_state_lock:
                if self._ytdlp_update_pending is payload:
                    payload["idle_sampled"] = False
                    payload["not_before"] = now + 60.0
            return {"ok": True, "started": False, "pending": True,
                    "busy": True}

        # Automatic installs take two idle samples three seconds apart. This
        # closes most races with older raw yt-dlp call sites that have not yet
        # entered PROCESS_REGISTRY. Windows' executable lock remains the final
        # safety net and a failed replacement is retained for retry.
        if payload.get("automatic") and not payload.get("idle_sampled"):
            with self._ytdlp_update_state_lock:
                if self._ytdlp_update_pending is payload:
                    payload["idle_sampled"] = True
                    payload["not_before"] = now + 3.0
            return {"ok": True, "started": False, "pending": True,
                    "confirming_idle": True}

        # Atomically block every new yt-dlp launch, then repeat the broad idle
        # check while that block is held. A normal launch that won the race is
        # registered before this reservation can succeed; a launch that loses
        # waits until the updater releases the executable.
        from backend.process_runner import YTDLP_UPDATE_GATE
        if not YTDLP_UPDATE_GATE.try_reserve_update(self._ytdlp_update_busy):
            with self._ytdlp_update_state_lock:
                if self._ytdlp_update_pending is payload:
                    payload["idle_sampled"] = False
                    payload["not_before"] = now + 60.0
            return {"ok": True, "started": False, "pending": True,
                    "busy": True}
        payload["gate_reserved"] = True

        with self._ytdlp_update_state_lock:
            if (self._ytdlp_update_pending is not payload
                    or self._ytdlp_update_running):
                YTDLP_UPDATE_GATE.release_update()
                payload["gate_reserved"] = False
                return {"ok": True, "started": False, "pending": True}
            self._ytdlp_update_pending = None
            self._ytdlp_update_running = True
        try:
            cancel_event = payload.get("cancel_event")
            if not isinstance(cancel_event, threading.Event):
                cancel_event = threading.Event()
                payload["cancel_event"] = cancel_event
            thread = start_managed_task(
                self,
                owner="ytdlp-updater",
                label="Update the managed yt-dlp executable",
                task_id=str(payload.get("task_id") or ""),
                cancel=cancel_event,
                target=lambda: self._run_ytdlp_update(payload),
                name="ytdlp-update-worker",
                thread_factory=threading.Thread,
            )
            self._ytdlp_update_thread = thread
            return {"ok": True, "started": True,
                    "automatic": bool(payload.get("automatic"))}
        except Exception as exc:
            if payload.get("gate_reserved"):
                YTDLP_UPDATE_GATE.release_update()
                payload["gate_reserved"] = False
            with self._ytdlp_update_state_lock:
                self._ytdlp_update_running = False
                self._ytdlp_update_pending = payload
            return {"ok": False, "started": False, "error": str(exc)}

    def _run_ytdlp_update(self, payload):
        import subprocess as _sp

        from backend.process_runner import supervise_streaming_process

        yt = payload["yt"]
        channel = payload["channel"]
        automatic = bool(payload.get("automatic"))
        label = "beta (nightly)" if channel == "nightly" else "stable"
        log_stream = self._settings_log_stream()
        prefix = "Automatic" if automatic else "Manual"
        log_stream.emit([
            ["[Update] ", "update_head"],
            [f"{prefix} yt-dlp update to {label}...\n", "update_sep"],
        ])
        task_id = str(
            payload.setdefault(
                "task_id", f"ytdlp-update-{uuid.uuid4().hex}"))
        cancel_event = payload.get("cancel_event")
        if not isinstance(cancel_event, threading.Event):
            cancel_event = threading.Event()
            payload["cancel_event"] = cancel_event
        with self._ytdlp_update_state_lock:
            self._ytdlp_update_active_cancel = cancel_event
        success = False
        retry_automatic = False
        try:
            # A bare channel switch only changes repositories when yt-dlp
            # considers the target version newer.  That leaves a newer
            # nightly build in place when the user has selected Stable and
            # causes the same "update" to be retried forever.  The release
            # check already resolved an exact tag, so use it when available;
            # an explicit channel@tag request supports both upgrades and
            # intentional downgrades.
            latest = str(payload.get("latest") or "").strip()
            update_target = f"{channel}@{latest}" if latest else channel
            proc = _sp.Popen([yt, "--update-to", update_target],
                             stdout=_sp.PIPE, stderr=_sp.STDOUT,
                             encoding="utf-8", errors="replace", bufsize=1,
                             startupinfo=sync_backend._startupinfo)
            result = supervise_streaming_process(
                proc,
                on_stdout_line=lambda line: log_stream.emit_dim(" " + line),
                cancel_event=cancel_event,
                timeout=900,
                owner="ytdlp-updater",
                task_id=task_id,
                role="self-update",
            )
            # Finalize already-exited Popen objects. This is non-blocking for a
            # real child and preserves compatibility with small legacy test
            # doubles whose state transition occurs in wait().
            try:
                proc.wait(timeout=0)
            except TypeError:
                proc.wait()
            except _sp.TimeoutExpired:
                pass
            if result.cancelled:
                raise RuntimeError("yt-dlp update cancelled")
            if result.timed_out:
                raise RuntimeError("yt-dlp updater timed out after 15 minutes")
            if result.returncode != 0:
                raise RuntimeError(
                    f"yt-dlp updater exited with code {result.returncode}")

            SettingsMixin._ytdlp_version_cache.pop(yt, None)
            latest = payload.get("latest") or ""
            installed = latest
            if automatic and latest:
                verified = self.ytdlp_version()
                if not verified.get("ok"):
                    raise RuntimeError(
                        "the updater exited successfully but the installed "
                        "version could not be verified")
                installed = str(verified.get("version") or "").strip()
                expected_tuple = self._ytdlp_version_tuple(latest)
                installed_tuple = self._ytdlp_version_tuple(installed)
                correct_channel_shape = (
                    len(installed_tuple) > 3 if channel == "nightly"
                    else len(installed_tuple) == 3
                )
                if (not expected_tuple or not installed_tuple
                        or not correct_channel_shape
                        or installed_tuple < expected_tuple):
                    raise RuntimeError(
                        "the updater exited successfully but reported "
                        f"version {installed or 'unknown'}; expected {channel} "
                        f"release {latest} or newer")
            success = True
            if payload.get("record_check"):
                with SettingsMixin._settings_save_lock:
                    fresh_cfg = self._settings_fresh_config()
                    fresh_channel = str(
                        fresh_cfg.get("ytdlp_channel") or "stable"
                    ).strip().lower()
                    if fresh_channel == channel:
                        self._record_ytdlp_update_check(time.time())
                    else:
                        # A channel switch made while the updater was already
                        # running remains due. Do not let the old channel's
                        # success postpone the newly selected one.
                        log_stream.emit_dim(
                            " [Update] Channel changed while updating; the "
                            "new channel remains due for a check.")
            display_version = installed or "the latest release"
            message = (f"yt-dlp updated to {display_version}; "
                       "no restart required.")
            log_stream.emit([["[Update] ", "update_head"],
                             [message + "\n", "update_sep"]])
            self._push_ytdlp_update_status("success", message,
                                            installed or "")
        except Exception as exc:
            if automatic:
                next_attempt = int(payload.get("attempt") or 0) + 1
                stopping = self._ytdlp_monitor_stop.is_set()
                fresh_cfg = self._settings_fresh_config()
                fresh_channel = str(
                    fresh_cfg.get("ytdlp_channel") or "stable"
                ).strip().lower()
                settings_current = (
                    self._normalize_ytdlp_update_mode(fresh_cfg) == "automatic"
                    and fresh_channel == channel)
                retry_automatic = (
                    not stopping
                    and settings_current
                    and next_attempt < SettingsMixin._ytdlp_update_max_attempts)
                if retry_automatic:
                    delay = min(
                        21_600.0, 900.0 * (2 ** min(next_attempt - 1, 5)))
                    payload["attempt"] = next_attempt
                    payload["idle_sampled"] = False
                    payload["not_before"] = time.time() + delay
                    wait_label = (
                        f"{int(delay // 3600)} hour(s)" if delay >= 3600
                        else f"{int(delay // 60)} minutes")
                    message = (f"Automatic yt-dlp update failed: {exc}. "
                               f"Retrying automatically in {wait_label}.")
                elif stopping:
                    message = f"Automatic yt-dlp update stopped: {exc}."
                elif not settings_current:
                    message = (
                        f"Automatic yt-dlp update failed: {exc}. It will not "
                        "retry because the update settings changed.")
                else:
                    if payload.get("record_check"):
                        self._record_ytdlp_update_check(time.time())
                    message = (
                        f"Automatic yt-dlp update failed after {next_attempt} "
                        f"attempts: {exc}. Automatic retries are paused until "
                        "the next scheduled check; you can update manually in "
                        "Health → Tools.")
                log_stream.emit_error(message)
                self._push_ytdlp_update_status(
                    "deferred" if retry_automatic else "error", message)
            else:
                message = f"yt-dlp update failed: {exc}"
                log_stream.emit_error(message)
                self._push_ytdlp_update_status("error", message)
        finally:
            if payload.get("gate_reserved"):
                try:
                    from backend.process_runner import YTDLP_UPDATE_GATE
                    YTDLP_UPDATE_GATE.release_update()
                finally:
                    payload["gate_reserved"] = False
            log_stream.flush()
            with self._ytdlp_update_state_lock:
                self._ytdlp_update_running = False
                if self._ytdlp_update_active_cancel is cancel_event:
                    self._ytdlp_update_active_cancel = None
                if automatic and not success and retry_automatic:
                    self._ytdlp_update_pending = payload
            self._ytdlp_monitor_wake.set()

    def check_ytdlp_update(self, force=False):
        """Check the selected yt-dlp channel for a newer release.

        The persistent monitor calls this whenever the configured elapsed-day
        interval is due. The network request remains asynchronous. Automatic
        mode persists a discovered managed-copy update and hands it to the
        idle-time coordinator; notify mode only reports it.
        """
        cfg = self._settings_fresh_config()
        mode = self._normalize_ytdlp_update_mode(cfg)
        try:
            interval_days = max(
                1, min(365, int(cfg.get("ytdlp_update_check_days", 1) or 1)))
        except (TypeError, ValueError):
            interval_days = 1
        now = time.time()
        try:
            last_check = float(cfg.get("last_ytdlp_update_check_ts", 0) or 0)
        except (TypeError, ValueError):
            last_check = 0.0

        if not force:
            if mode == "off":
                return {"ok": True, "started": False, "disabled": True}
            interval_seconds = interval_days * 86_400
            if 0 < last_check <= now and now - last_check < interval_seconds:
                return {"ok": True, "started": False, "due": False}

        with SettingsMixin._ytdlp_update_check_lock:
            if SettingsMixin._ytdlp_update_check_running:
                return {"ok": True, "started": False, "running": True}
            SettingsMixin._ytdlp_update_check_running = True

        task_id = f"ytdlp-update-check-{uuid.uuid4().hex}"
        cancel = threading.Event()

        def _run():
            try:
                if cancel.is_set():
                    return
                import json as _json
                import urllib.request as _ur

                info = self.ytdlp_version()
                if not info.get("ok"):
                    raise RuntimeError(info.get("error") or "yt-dlp not found")
                current = (info.get("version") or "").strip()
                current_tuple = self._ytdlp_version_tuple(current)
                if not current_tuple:
                    raise RuntimeError(f"unrecognized installed version: {current}")

                channel = (cfg.get("ytdlp_channel") or "stable").strip().lower()
                if channel not in SettingsMixin._ytdlp_release_apis:
                    channel = "stable"
                req = _ur.Request(
                    SettingsMixin._ytdlp_release_apis[channel],
                    headers={"User-Agent": "YTArchiver"},
                )
                with _ur.urlopen(req, timeout=8) as resp:
                    data = _json.loads(resp.read(1_000_000))
                if cancel.is_set():
                    return
                latest = (data.get("tag_name") or "").strip().lstrip("v")
                latest_tuple = self._ytdlp_version_tuple(latest)
                if not latest_tuple:
                    raise RuntimeError("release service returned no version")

                # Stable builds have a three-part date. Nightly builds add a
                # build-time component; selecting Stable is an explicit
                # request to switch back even when that numeric tuple sorts
                # below the installed nightly build.
                channel_switch = channel == "stable" and len(current_tuple) > 3
                update_available = latest_tuple > current_tuple or channel_switch
                # Settings may change while GitHub is responding. Hold the
                # same lock as settings_save while validating and recording
                # this result so an old Automatic/Stable request cannot land
                # after the user has selected Off, Notify, or Nightly.
                with SettingsMixin._settings_save_lock:
                    fresh_cfg = self._settings_fresh_config()
                    fresh_mode = self._normalize_ytdlp_update_mode(fresh_cfg)
                    fresh_channel = str(
                        fresh_cfg.get("ytdlp_channel") or "stable"
                    ).strip().lower()
                    if fresh_channel not in SettingsMixin._ytdlp_release_apis:
                        fresh_channel = "stable"
                    if fresh_mode == "off" or fresh_channel != channel:
                        self._ytdlp_check_not_before = 0.0
                        return

                    if not update_available:
                        self._record_ytdlp_update_check(now)
                        self._ytdlp_check_not_before = 0.0
                        return

                    label = (
                        "beta (nightly)" if channel == "nightly" else "stable")
                    yt_path = (
                        info.get("path") or sync_backend.find_yt_dlp() or "")
                    auto_updatable = self._can_auto_update_ytdlp(yt_path)
                    if fresh_mode == "automatic" and auto_updatable:
                        self._record_ytdlp_pending_update(latest, channel)
                        queued = self._queue_ytdlp_update(
                            yt=yt_path, channel=channel, automatic=True,
                            latest=latest, current=current, record_check=True)
                        self._settings_log_stream().emit([
                            ["[Update] ", "update_head"],
                            [f"yt-dlp {latest} is available on the {label} "
                             f"channel (installed: {current}). Automatic "
                             "update queued for the next idle window.\n",
                             "update_sep"],
                        ])
                        self._push_ytdlp_update_status(
                            "pending",
                            "yt-dlp update queued until YouTube work is idle.",
                            latest)
                        if queued.get("started"):
                            self._settings_log_stream().emit_dim(
                                " [Update] Idle window confirmed; updater "
                                "started.")
                    else:
                        self._record_ytdlp_update_check(now)
                        reason = ""
                        if fresh_mode == "automatic" and not auto_updatable:
                            reason = (
                                " This yt-dlp installation is controlled by "
                                "a package manager or is not a standalone "
                                "build, so it will not be changed "
                                "automatically.")
                        notice = (
                            f"yt-dlp {latest} is available on the {label} "
                            f"channel (installed: {current}).{reason} Update "
                            "it in Health → Tools → yt-dlp → Update.")
                        self._settings_log_stream().emit([
                            ["⚠ ", "red"], [notice + "\n", "red"],
                        ])
                        self._settings_log_stream().flush()
                        self._push_ytdlp_update_status(
                            "available", notice, latest)
                self._ytdlp_check_not_before = 0.0
            except Exception as e:
                self._ytdlp_check_not_before = time.time() + 3_600.0
                try:
                    self._settings_log_stream().emit_dim(
                        f"[Update] yt-dlp check skipped: {e}; retrying later.")
                except Exception as log_error:
                    _log.debug("ytdlp update check log failed: %s", log_error)
            finally:
                with SettingsMixin._ytdlp_update_check_lock:
                    SettingsMixin._ytdlp_update_check_running = False
                wake = getattr(self, "_ytdlp_monitor_wake", None)
                if wake is not None:
                    wake.set()

        try:
            start_managed_task(
                self,
                owner="ytdlp-update-check",
                label="Check for yt-dlp updates",
                task_id=task_id,
                cancel=cancel,
                target=_run,
                name="ytdlp-update-check",
                thread_factory=threading.Thread,
            )
        except WorkAdmissionClosed as exc:
            with SettingsMixin._ytdlp_update_check_lock:
                SettingsMixin._ytdlp_update_check_running = False
            return {"ok": False, "started": False, "error": str(exc)}
        return {"ok": True, "started": True, "due": True}

    def ytdlp_update(self):
        """Queue a user-requested update through the shared coordinator.

        Targets the configured release channel via `--update-to <channel>`
        (stable or nightly). Using `--update-to` rather than the older `-U`
        lets the Beta toggle both switch channels AND update in one step —
        e.g. picking Beta runs `--update-to nightly`, and switching back
        runs `--update-to stable` (which downgrades to latest stable)."""
        yt = sync_backend.find_yt_dlp()
        if not yt:
            return {"ok": False, "error": "yt-dlp not found"}
        cfg = self._settings_config()
        channel = (cfg.get("ytdlp_channel") or "stable").strip().lower()
        if channel not in ("stable", "nightly"):
            channel = "stable"
        result = self._queue_ytdlp_update(
            yt=yt, channel=channel, automatic=False, record_check=True)
        if result.get("pending") and result.get("busy"):
            self._settings_log_stream().emit_dim(
                "[Update] yt-dlp update queued until current work is idle.")
        return result


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
        with SettingsMixin._settings_save_lock:
            cfg = self._settings_fresh_config()
            original_cfg = copy.deepcopy(cfg)
            cfg["output_dir"] = path
            ok, _snapshot = self._settings_commit_candidate(
                original_cfg, cfg)
        if ok:
            self._reload_config()
            return {"ok": True, "path": path}
        return {"ok": False, "write_blocked": True, "path": path,
                "error": ("Settings are temporarily read-only. Restart "
                          "YTArchiver and try again.")}
