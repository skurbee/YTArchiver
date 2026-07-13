"""
sync.recent_track — Recent-tab download tracking.

Extracted from sync/core.py. Owns the per-download record write to
`config['recent_downloads']` plus the JS-side change hook that drives
the Recent tab's live refresh.

Public surface (re-exported by backend.sync):
    _record_recent_download(filepath, channel, title, ...)
    set_recent_changed_hook(hook)

The change hook is module-level state owned here so the record-download
function can fire it directly without going back through core.py.
"""
from __future__ import annotations

import os
import subprocess
import threading
import time
from typing import Any

from ..log import get_logger, swallow

_log = get_logger(__name__)


# Hook set by main.py Api.__init__ so the Recent tab auto-refreshes
# when a download completes. Module-level state — set_recent_changed_hook
# mutates this; _record_recent_download reads it.
_on_recent_changed_hook: Any | None = None

# Module-wide lock around the load-modify-save of recent_downloads +
# downloads_since_last_index. Two concurrent sync_channel writers
# previously could both load_config, both prepend their entry, and the
# loser's recent-entry silently disappeared (audit: recent_track.py:140).
# The counter increment shares the same critical section so the
# auto-index trigger doesn't either-fire-too-often or skip entirely
# (audit: recent_track.py:184).
_recent_write_lock = threading.Lock()


def _probe_duration_seconds(filepath: str) -> str:
    try:
        from ..subprocess_util import make_startupinfo, subprocess_creationflags
        r = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1",
             filepath],
            capture_output=True, text=True, timeout=6,
            startupinfo=make_startupinfo(),
            creationflags=subprocess_creationflags(),
        )
        raw = (r.stdout or "").strip()
        return str(int(float(raw))) if raw else ""
    except Exception as e:
        swallow("ffprobe duration probe", e)
        return ""


def _backfill_recent_duration(filepath: str) -> None:
    duration_s = _probe_duration_seconds(filepath)
    if not duration_s:
        return
    # Keep the authoritative Browse catalog in sync with the legacy Recent
    # config. Previously this background probe repaired only the Download-tab
    # card, so the same video could show a duration there and remain blank in
    # Browse forever.
    try:
        from .. import index as _idx
        _idx.set_video_duration(filepath, float(duration_s))
    except Exception as e:
        swallow("catalog duration backfill", e)
    try:
        from ..ytarchiver_config import (
            config_is_writable,
            config_transaction,
        )
        if not config_is_writable():
            return
        changed = False
        with _recent_write_lock:
            with config_transaction() as cfg:
                for entry in cfg.get("recent_downloads", []) or []:
                    if (entry.get("filepath") == filepath
                            and not entry.get("duration")):
                        entry["duration"] = duration_s
                        changed = True
                        break
        if changed:
            fire_recent_changed_hook()
    except Exception as e:
        swallow("recent duration backfill", e)


def set_recent_changed_hook(hook: Any | None) -> None:
    """Main.py wires this in __init__ so the Recent tab auto-refreshes
    when a download completes. Hook receives an optional channel name
    (the download's channel, when known) — caller re-fetches the current
    recent_downloads list and pushes to the UI."""
    global _on_recent_changed_hook
    _on_recent_changed_hook = hook


def fire_recent_changed_hook(channel: str | None = None) -> None:
    """Best-effort fire of the registered hook. Used by sync/core.py's
    DLTRACK handler to push a live Recent-tab refresh right after a
    download lands. `channel` (when known) lets the UI target the matching
    channel grid. Safe no-op when no hook is wired (tests, headless)."""
    if _on_recent_changed_hook is not None:
        try:
            _on_recent_changed_hook(channel)
        except Exception as e:
            swallow("recent-changed hook", e)


def _record_recent_download(filepath: str, channel: str, title: str,
                             video_id: str = "",
                             upload_date: str = "",
                             size_bytes: int | None = None,
                             duration_secs: float | None = None) -> bool:
    """Push a fresh entry onto config['recent_downloads'] (newest first).

    Keeps the list capped at 500 entries. Silently no-ops when the write
    gate is off. Schema matches the original tkinter app's record_download
    exactly — field names + types so both apps can read each other's
    entries:

      title str
      channel str
      date str "YYYYMMDD" — upload date, NOT formatted
      size str raw bytes count as a string, e.g. "1234567"
      duration str raw seconds as a string, e.g. "383"
      filepath str
      video_url str
      download_ts float unix timestamp

    `size_bytes` and `duration_secs` are optional — when the caller
    already knows them (the DLTRACK handler does, both come straight
    from yt-dlp), pass them through to skip the redundant disk work.
    Without that fast path, ffprobe is spawned to parse duration off
    the newly-merged .mp4, which on a contended slow disk (Z: DrivePool
    during the boot sweep) can stall the whole DLTRACK handler for
    several seconds per download.
    """
    if not filepath:
        return False
    from ..ytarchiver_config import config_is_writable, config_transaction
    if not config_is_writable():
        return False
    try:
        # Raw bytes — read as `int(size)`. Must be a plain integer
        # string, NOT a human-readable "5.2 MB".
        if size_bytes is not None:
            _size_bytes = int(size_bytes)
        else:
            _size_bytes = 0
            try:
                _size_bytes = os.path.getsize(filepath)
            except OSError:
                pass

        # Raw seconds — read as `int(duration)`. Must be integer string
        # of seconds, NOT "3:45".
        duration_s = ""
        if duration_secs is not None and duration_secs > 0:
            try:
                duration_s = str(int(float(duration_secs)))
            except (TypeError, ValueError):
                duration_s = ""
        _needs_duration_backfill = not duration_s

        # Prefer yt-dlp's emitted upload_date (from DLTRACK) over file
        # mtime. On some Windows network drives + Z: drivepool setups,
        # --mtime silently fails to set mtime on the new file, leaving
        # mtime=download-time. We use the authoritative YYYYMMDD value
        # yt-dlp already knows and only fall back to mtime if it wasn't
        # provided.
        date_str = ""
        _ud = (upload_date or "").strip()
        if len(_ud) == 8 and _ud.isdigit():
            date_str = _ud
        else:
            try:
                from datetime import datetime as _dt
                date_str = _dt.fromtimestamp(os.path.getmtime(filepath)).strftime("%Y%m%d")
            except OSError:
                pass

        # Combine recent_downloads update + auto-index counter bump
        # into ONE load/modify/save cycle so the lock holds across a
        # single I/O round-trip instead of two. The previous two
        # back-to-back lock+load+save cycles roughly doubled the
        # latency and stalled concurrent download finishers serially
        # (audit: recent_track H41).
        _fire_sweep = False
        with _recent_write_lock:
            with config_transaction() as cfg:
                entries = list(cfg.get("recent_downloads", []) or [])
                def _same_recent_entry(e: dict[str, Any]) -> bool:
                    if video_id:
                        return ((e.get("video_id") or "") == video_id
                                or e.get("filepath") == filepath)
                    if filepath:
                        return e.get("filepath") == filepath
                    return (e.get("title") == title
                            and e.get("channel") == channel)

                entries = [e for e in entries if not _same_recent_entry(e)]
                _completed_ts = time.time()
                entries.insert(0, {
                    "title": title or "",
                    "channel": channel or "",
                    "date": date_str,             # YYYYMMDD
                    "size": str(int(_size_bytes)),  # raw bytes as string
                    "duration": duration_s,         # raw seconds as string
                    "filepath": filepath,
                    "video_url": (
                        f"https://www.youtube.com/watch?v={video_id}"
                        if video_id else ""),
                    # Store video_id explicitly so recent_for_ui's
                    # find_thumbnail lookup doesn't have to parse it back out
                    # of video_url. The fallback parse still works, but the
                    # explicit field is cheaper and avoids URL-format coupling.
                    "video_id": video_id or "",
                    "download_ts": _completed_ts,  # unix float
                })
                cfg["recent_downloads"] = entries[:500]
                # Auto-index counter bump (was a separate second lock+
                # load+save; merged here per H41).
                if cfg.get("auto_index_enabled", False):
                    threshold = int(cfg.get("auto_index_threshold", 10) or 10)
                    _legacy = int(cfg.pop("_auto_index_counter", 0) or 0)
                    counter = int(
                        cfg.get("downloads_since_last_index", 0) or 0
                    ) + _legacy + 1
                    if counter >= threshold:
                        _fire_sweep = True
                        cfg["downloads_since_last_index"] = 0
                    else:
                        cfg["downloads_since_last_index"] = counter

        # Persist the same completion timestamp in the Browse catalog. This
        # is the semantic bridge from the legacy config list to the canonical
        # video row; rescans never call it, so discovered old files cannot
        # masquerade as recent downloads.
        try:
            from .. import index as _idx
            if not _idx.record_video_download(
                    filepath, video_id=video_id,
                    downloaded_ts=_completed_ts,
                    duration_secs=duration_secs):
                swallow("catalog download completion",
                        RuntimeError(f"catalog row not found for {filepath}"))
        except Exception as e:
            swallow("catalog download completion", e)

        # Live refresh push to the Recent tab so a download shows up
        # immediately without needing a restart. Hook set by main.py's
        # Api.__init__; safe no-op when unset (unit tests).
        if _on_recent_changed_hook is not None:
            try: _on_recent_changed_hook(channel)
            except Exception as e: swallow("recent-changed hook", e)
        if _fire_sweep:
            # Spawn the sweep OUTSIDE the lock so it can't deadlock
            # another writer waiting for the same lock.
            import threading as _thr
            def _bg_sweep():
                try:
                    from .. import index as _idx
                    from ..ytarchiver_config import load_config
                    cfg_s = load_config()
                    output_dir = (cfg_s.get("output_dir") or "").strip()
                    if output_dir:
                        _idx.sweep_new_videos(output_dir, cfg_s.get("channels", []))
                except Exception as e:
                    swallow("bg index sweep", e)
            _thr.Thread(target=_bg_sweep, daemon=True).start()
        if _needs_duration_backfill:
            threading.Thread(
                target=_backfill_recent_duration,
                args=(filepath,),
                daemon=True,
                name="recent-duration-backfill",
            ).start()
        return True
    except Exception as e:
        swallow("record-recent-download", e)
        return False
