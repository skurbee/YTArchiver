"""Application information and URL history with explicit state dependencies."""

from __future__ import annotations

import os
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from backend.log import get_logger
from backend.services.config_repository import ConfigRepository
from backend.services.ports import LogSink
from backend.version import APP_VERSION

_log = get_logger(__name__)


def format_last_sync_label(ts_str):
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


@dataclass
class ApplicationInformation:
    config: ConfigRepository
    config_path: str
    log: LogSink
    can_write: Callable[[], bool]
    _url_lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    def runtime_info(self, snapshot: dict | None) -> dict:
        cfg = snapshot or {}
        return {
            "has_real_config": snapshot is not None,
            "config_path": self.config_path,
            "log_mode": cfg.get("log_mode", "Simple"),
            "autorun_interval": cfg.get("autorun_interval", 0),
            "last_sync": cfg.get("last_sync", ""),
            "output_dir": (cfg.get("output_dir") or "").strip(),
            "first_run": not bool((cfg.get("output_dir") or "").strip() and cfg.get("channels")),
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
            "onboarded": bool(cfg.get("onboarded")) or bool((cfg.get("output_dir") or "").strip()),
            # No real config on disk yet == brand-new machine.
            "has_config_file": snapshot is not None,
            # Subs-table column visibility toggle — piggybacked on runtime
            # info so the JS can apply the class BEFORE the first
            # renderSubsTable call and avoid a flash of the hidden column.
            "show_avg_size": bool(cfg.get("show_avg_size", False)),
        }

    def about(self, tool_version: Callable[[], dict]) -> dict:
        cfg = self.config.load()
        yt_ver = "unknown"
        try:
            r = tool_version()
            if r.get("ok"):
                yt_ver = r["version"]
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return {
            "app_name": "YTArchiver",
            "app_version": APP_VERSION,
            "channels": len(cfg.get("channels", [])),
            "config_path": self.config_path,
            "output_dir": cfg.get("output_dir", ""),
            "ytdlp_version": yt_ver,
            "python_version": sys.version.split()[0],
        }

    def url_history(self) -> list[str]:
        return list(self.config.load().get("url_history", []) or [])[:20]

    def push_url(self, url: str) -> None:
        if not self.can_write():
            return
        with self._url_lock:

            def update(config):
                history = [item for item in (config.get("url_history", []) or []) if item != url]
                config["url_history"] = [url, *history][:20]

            try:
                self.config.mutate(update)
            except Exception as exc:
                try:
                    self.log.emit_dim(
                        f"URL history could not be saved — "
                        f"'{(url or '')[:60]}' was not added to autocomplete."
                    )
                except Exception:
                    pass
                _log.warning("URL history save failed: %s", exc)

    def last_sync_label(self) -> dict[str, str]:
        value = self.config.load().get("last_sync", "") or ""
        return {"label": format_last_sync_label(value)}
