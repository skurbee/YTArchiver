"""Startup disk counting with explicit storage, progress and event ports."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from backend import archive_scan
from backend.log import get_logger
from backend.services.config_repository import ConfigRepository
from backend.services.event_bus import BridgeEventBus
from backend.services.ports import LogSink

_log = get_logger(__name__)


class ArchiveScanPort(Protocol):
    def heal_malformed_cache_entries(self) -> int: ...
    def cache_coverage(self, channels: list, cache: dict | None = None) -> dict: ...
    def scan_all_channels(
        self, *, progress_cb: Callable, stop_if: Callable, cfg: dict
    ) -> dict | None: ...
    def publish_scan_stats(self, scanned: dict) -> dict: ...
    def index_summary(self, cfg: dict | None = None) -> dict: ...


@dataclass
class StartupDiskScan:
    config: ConfigRepository
    log: LogSink
    events: BridgeEventBus
    publish_config: Callable[[dict[str, Any]], None]
    progress: dict[str, dict[str, str]]
    archive: ArchiveScanPort = archive_scan
    clock: Callable[[], float] = time.time

    def flush(self) -> None:
        try:
            self.log.flush()
        except Exception as exc:
            _log.debug("Startup log flush failed: %s", exc)

    def run(self, cancel_event, cfg, busy):
        """Refresh disk-scan cache after Stage 1 makes the UI usable."""
        if cancel_event.is_set():
            return
        try:
            # issue #134: drop any cache entries that only contain a
            # `sweep_fingerprint` (no num_vids/size_bytes). Those can
            # be left over from older code paths; if present, they
            # show as "—" in Subs table + Index summary. Force a
            # walk when any are found so the next pass fills them in.
            dropped = self.archive.heal_malformed_cache_entries()
            coverage = self.archive.cache_coverage(self.config.load().get("channels", []))
            stale_hours = int(cfg.get("disk_scan_staleness_hours", 24) or 0)
            last_ts = float(cfg.get("last_disk_scan_ts", 0) or 0)
            age_hours = (self.clock() - last_ts) / 3600.0 if last_ts > 0 else 1e9
            do_walk = (
                (stale_hours <= 0)
                or (age_hours >= stale_hours)
                or (last_ts == 0)
                or (dropped > 0)
                or not coverage["complete"]
            )

            if do_walk:
                self.progress["sweep"]["phase"] = "Scanning disk"
                self.progress["sweep"]["detail"] = ""

                def _on_walk(ch_name, idx, total):
                    clean = (ch_name or "")[:32]
                    self.progress["sweep"]["phase"] = "Scanning disk"
                    self.progress["sweep"]["detail"] = f"{idx + 1}/{total} \u2014 {clean}"

                walked = self.archive.scan_all_channels(
                    cfg=self.config.load(),
                    progress_cb=_on_walk, stop_if=lambda: cancel_event.is_set() or busy()
                )
                if cancel_event.is_set():
                    return
                if walked is None:
                    self.progress["sweep"]["phase"] = ""
                    self.progress["sweep"]["detail"] = ""
                    self.log.emit_dim(
                        " Disk scan deferred — sync or foreground work "
                        "started; existing cache preserved."
                    )
                    self.flush()
                    return False
                if walked:
                    # Merge counts into the current cache so concurrent
                    # subscriber metadata and newer sync results survive.
                    published = self.archive.publish_scan_stats(walked)
                    coverage = self.archive.cache_coverage(
                        self.config.load().get("channels", []), published
                    )
                    # Persist the timestamp so next boot can decide
                    # staleness. Previously the exception handler
                    # silently swallowed failures — reported
                    # disk scan running every launch, which means
                    # this save wasn't sticking. Now we surface
                    # the outcome so a silent failure (write-gate
                    # off, disk full, permissions, etc.) is visible
                    # in the log instead of mysteriously rescanning
                    # forever.
                    if coverage["complete"]:
                        try:
                            _unused, c2 = self.config.mutate(
                                lambda live: live.__setitem__("last_disk_scan_ts", self.clock())
                            )
                            self.publish_config(c2)
                        except Exception as _se:
                            self.log.emit_error(f"Disk scan timestamp save raised: {_se}")
                            self.flush()
            else:
                # Explicit dim log line when we SKIP the scan so
                # the user can tell it's honoring the staleness
                # setting. Verbose-only.
                age_str = f"{age_hours:.1f}h" if age_hours < 72 else f"{age_hours / 24:.1f}d"
                self.log.emit_dim(
                    f" Disk scan skipped \u2014 last run was {age_str} ago, "
                    f"staleness threshold is {stale_hours}h."
                )
                self.flush()
            # Emit the milestone from the freshly-walked (or still-cached) totals.
            t = self.archive.index_summary(cfg=self.config.load())["cards"]
            if not t["scan_complete"]:
                self.log.emit_dim(
                    " Saved disk scan is incomplete "
                    f"({t['scanned_channels']}/{t['total_channels']} channels)."
                )
            elif t["videos"] > 0:
                self.log.emit_text(
                    f"--- Disk scan complete ({t['channels']} channels \u00b7 "
                    f"{t['videos']:,} videos \u00b7 "
                    f"{t['size_gb'] / 1024:.1f} TB) ---",
                    "simpleline_green",
                )
            else:
                self.log.emit_text("--- Disk scan complete ---", "simpleline_green")
            self.flush()
            # issue #134: Subs table was rendered at boot using
            # whatever was in the cache at that moment — which for
            # healed/invalidated channels was an empty record that
            # maps to "—". Now that Stage 2 has just written fresh
            # stats, ask the UI to re-fetch. Without this push the
            # user has to click Subs → some other tab → Subs to see
            # the numbers fill in.
            try:
                if self.events is not None:
                    self.events.evaluate(
                        "if (window.refreshSubsTable) "
                        "window.refreshSubsTable();"
                        "if (document.getElementById('panel-health')"
                        "?.classList.contains('active')) {"
                        "if (!document.getElementById('settings-view-library')"
                        "?.hidden && window._refreshIndexStats) "
                        "window._refreshIndexStats();"
                        "if (!document.getElementById('settings-view-overview')"
                        "?.hidden && window._refreshHealthOverview) "
                        "window._refreshHealthOverview();}"
                    )
            except Exception as e:
                _log.debug("swallowed: %s", e)
        except Exception as e:
            self.log.emit_error(f"Disk scan error: {e}")
            self.flush()
