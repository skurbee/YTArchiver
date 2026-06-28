"""
InfoMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

import os
from datetime import datetime

import os
import re
import sys
import threading
from datetime import datetime

from ._shared import _log
from backend.ytarchiver_config import CONFIG_FILE, config_is_writable, load_config, save_config
from backend.version import APP_VERSION, APP_VERSION_DATE


def _format_last_sync_label(ts_str):
    """Format stored last_sync timestamp (YYYY-MM-DD HH:MM) like YTArchiver.py:22157.
    Module-level helper used by `InfoMixin.get_last_sync_label`."""
    if not ts_str:
        return "Last Full Sync: Not yet synced"
    try:
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M")
    except Exception:
        return f"Last Full Sync: {ts_str}"
    now = datetime.now()
    diff_mins = max(0, int((now - dt).total_seconds() // 60))
    time_part = dt.strftime("%I:%M%p").lstrip("0").lower()
    # Use non-padded day where possible
    try:
        date_part = dt.strftime("%b %-d") if os.name != "nt" else dt.strftime("%b ") + str(dt.day)
    except Exception:
        date_part = dt.strftime("%b %d")
    if diff_mins < 1:
        ago = "just now"
    elif diff_mins < 60:
        ago = f"{diff_mins} min{'s' if diff_mins != 1 else ''} ago"
    else:
        hrs = diff_mins // 60
        if hrs < 24:
            ago = f"{hrs} hr{'s' if hrs != 1 else ''} ago"
        else:
            days = hrs // 24
            ago = f"{days} day{'s' if days != 1 else ''} ago"
    return f"Last Full Sync: {time_part}, {date_part} ({ago})"


class InfoMixin:

    # ─── Environment / capabilities ──────────────────────────────────────
    def get_runtime_info(self):
        cfg = self._config or {}
        return {
            "has_real_config": self._config is not None,
            "config_path": str(CONFIG_FILE),
            "log_mode": cfg.get("log_mode", "Simple"),
            "autorun_interval": cfg.get("autorun_interval", 0),
            "last_sync": cfg.get("last_sync", ""),
            "output_dir": (cfg.get("output_dir") or "").strip(),
            "first_run": not bool((cfg.get("output_dir") or "").strip()
                                  and cfg.get("channels")),
            # Authoritative first-run-wizard gate. True once the user has
            # completed (or skipped through) onboarding. The wizard trigger
            # in seedLogs.js keys off this (plus a missing output_dir as a
            # belt-and-suspenders fallback) rather than the old output_dir-
            # only check that could silently skip.
            #
            # MIGRATION: existing installs set up before the wizard existed
            # have a config with output_dir already set but NO `onboarded`
            # key (defaults to False). Treat "has an archive folder" as
            # already-onboarded so the wizard never nags users who were
            # already up and running. Brand-new machines (no output_dir)
            # still get the wizard.
            "onboarded": bool(cfg.get("onboarded"))
                         or bool((cfg.get("output_dir") or "").strip()),
            # No real config on disk yet == brand-new machine.
            "has_config_file": self._config is not None,
            # Subs-table column visibility toggle — piggybacked on runtime
            # info so the JS can apply the class BEFORE the first
            # renderSubsTable call and avoid a flash of the hidden column.
            "show_avg_size": bool(cfg.get("show_avg_size", False)),
            # Recent tab view mode — "list" (legacy table) or "grid"
            # (thumbnail cards). Piggybacked here so the JS can set the
            # initial visibility of either view before the first render
            # and avoid a flash of the wrong view.
            "recent_view_mode": (cfg.get("recent_view_mode") or "grid"),
        }


    # ─── Phase 0: log seeding ────────────────────────────────────────────

    def ping(self):
        return "pong"


    def get_header_version(self):
        """Live version string for the HTML header strip. JS calls this
        on DOMContentLoaded and overwrites #header-version so the label
        can never drift from APP_VERSION — the index.html hardcoded
        placeholder is cosmetic fallback only."""
        return {"version": APP_VERSION, "date": APP_VERSION_DATE}


    def get_activity_log_history(self):
        """
        Return activity-log entries. When the real config is loaded, the
        authoritative source is `config['autorun_history']` — an empty
        list here means the user intentionally cleared the log, so we
        return [] (NOT the fictional sample data). Sample data is only
        used when there's no config at all (preview / demo mode).
        """
        if self._config is not None:
            from backend.autorun import history_entries_for_ui
            return history_entries_for_ui(self._config)
        return []


    def autorun_history_clear(self):
        """Empty config['autorun_history'] and persist. Called by the
        Activity-log Clear button. After this, a relaunch will show an
        empty activity log instead of re-loading the old entries.
        `_clear_autorun_history`.
        """
        from backend.autorun import clear_history as _ch
        res = _ch()
        self._reload_config()
        # push the cleared state to the frontend so the visible
        # activity log clears immediately instead of waiting for the
        # next unrelated push or tab switch. The renderer accepts an
        # empty array; no dedicated `clearActivityLog` shim is needed.
        try:
            if self._window is not None:
                self._window.evaluate_js(
                    "if (window.renderActivityLog) window.renderActivityLog([]);"
                    "if (window._syncActivityLogVisibility) "
                    "window._syncActivityLogVisibility();"
                    "if (window._syncClearButtonVisibility) "
                    "window._syncClearButtonVisibility();")
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return res


    def get_initial_main_log(self):
        """Return a big batch of main-log lines for initial render.

        Always empty in production — JS renders a clean blank log until
        real content arrives via the push pipeline.
        """
        return []


    # ─── About info ────────────────────────────────────────────────────

    def about_info(self):
        cfg = self._config or load_config()
        yt_ver = "unknown"
        try:
            r = self.ytdlp_version()
            if r.get("ok"):
                yt_ver = r["version"]
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return {
            "app_name": "YTArchiver",
            "app_version": APP_VERSION,
            "channels": len(cfg.get("channels", [])),
            "config_path": str(CONFIG_FILE),
            "output_dir": cfg.get("output_dir", ""),
            "ytdlp_version": yt_ver,
            "python_version": sys.version.split()[0],
        }


    # ─── URL history (for autocomplete on the URL field) ──────────────

    def url_history(self):
        """Return recently-typed YouTube URLs (latest first, max 20)."""
        cfg = load_config()
        # Apply the 20-item cap on read too. Older configs / imported
        # configs may have grown past the cap on disk (audit:
        # info_mixin.py:166), so we re-trim here defensively.
        return list(cfg.get("url_history", []) or [])[:20]


    # Process-wide lock for url_history mutation. Two near-simultaneous
    # downloads finishing within ms both used to load_config + mutate +
    # save_config without coordination, so the second save_config could
    # overwrite the first's append (audit: info_mixin.py:162-171).
    _url_history_lock = threading.Lock()


    def _push_url_history(self, url):
        if not config_is_writable():
            return
        with InfoMixin._url_history_lock:
            cfg = load_config()
            hist = [u for u in (cfg.get("url_history", []) or []) if u != url]
            hist.insert(0, url)
            del hist[20:]
            cfg["url_history"] = hist
            from backend.ytarchiver_config import save_config as _sc
            _ok = _sc(cfg)
            # Don't silently drop the URL if the save fails (write-gate
            # toggled off mid-call, disk full). Emit a dim line so the
            # user can investigate why their autocomplete history isn't
            # updating (audit: info_mixin H13).
            if not _ok:
                try:
                    self._log_stream.emit_dim(
                        f"URL history save failed (config write-gate or disk) "
                        f"— '{(url or '')[:60]}' not added to autocomplete.")
                except Exception:
                    pass


    # ─── Last Full Sync live label ──────────────────────────────────────

    def get_last_sync_label(self):
        """Return a formatted 'Last Full Sync: HH:MMam/pm, Mon D (X ago)' string.

        Always reads from disk. The old `self._config or load_config()`
        path used a stale boot-cached snapshot that never got refreshed
        after sync wrote a new `last_sync` value to disk — the label
        sat at its placeholder forever after the first sync of a
        fresh session.
        """
        cfg = load_config()
        ts = cfg.get("last_sync", "") or ""
        return {"label": _format_last_sync_label(ts)}
