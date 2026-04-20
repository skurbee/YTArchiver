"""
Read/write YTArchiver's real config file for drop-in compatibility.

Uses the same JSON file the legacy tkinter app uses:
    %APPDATA%\\YTArchiver\\ytarchiver_config.json (Windows)
    ~/.config/YTArchiver/ytarchiver_config.json (Unix)

That way saved channels, autorun history, log_mode, recent_downloads, and
every other preference carry over with zero migration.

NEVER write to this file while the legacy YTArchiver is also running — the
process has a single-instance mutex but the config file itself has no lock.
Keep reads safe; gated writes below go through config_is_writable().
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional


# Same derivation as YTArchiver.py lines 91-94
if os.name == "nt":
    APP_DATA_DIR = Path(os.environ.get("APPDATA", os.path.expanduser("~"))) / "YTArchiver"
else:
    APP_DATA_DIR = Path(os.path.expanduser("~")) / ".config" / "YTArchiver"

CONFIG_FILE = APP_DATA_DIR / "ytarchiver_config.json"
ARCHIVE_FILE = APP_DATA_DIR / "ytarchiver_archive.txt"
QUEUE_FILE = APP_DATA_DIR / "ytarchiver_queue.json"
DISK_CACHE_FILE = APP_DATA_DIR / "ytarchiver_disk_cache.json"
# FTS5 index DB (matches YTArchiver.py:23439 _TP_DB_PATH)
TRANSCRIPTION_DB = APP_DATA_DIR / "transcription_index.db"
SEEN_FILTER_TITLES = APP_DATA_DIR / "ytarchiver_seen_filters.txt"
# Per-channel cached video ID lists (so sync skips the slow playlist walk)
CHANNEL_ID_CACHE = APP_DATA_DIR / "ytarchiver_channel_ids.json"

# Matches YTArchiver.py DEFAULT_CONFIG at line 149
DEFAULT_CONFIG = {
    "output_dir": str(Path.home() / "Channel Archives"),
    "video_out_dir": str(Path.home() / "Video Downloads"),
    "vid_date_file": True,
    "vid_add_date": False,
    "min_duration": 0,
    "channels": [],
    "recent_downloads": [],
    "autorun_interval": 0,
    "autorun_history": [],
    "log_mode": "Simple",
    "autorun_gpu": False,
    "autorun_sync": False,
    "chan_col_widths": {},
    "recent_col_widths": {},
    "deps_checked": False,
    "auto_index_enabled": False,
    "auto_index_threshold": 10,
    "downloads_since_last_index": 0,
    "last_sync": "",
    "whisper_model": "small",
    "tp_archive_roots": [], # extra index-only roots (transcription parser)
    "url_history": [], # recent archive URLs (single-video downloads)
    # Disk-scan staleness: skip the 20-40s boot walk if the cache was
    # written within this many hours. 0 = always walk (OLD behavior).
    "disk_scan_staleness_hours": 24,
    # Browse preload: warm N videos per channel so Browse clicks are
    # instant. Higher = more RAM, more boot time. the user's large
    # archive at 150 ≈ ~17 MB; at "all" ≈ ~110 MB. Default 150.
    "browse_preload_limit": 150,
    "browse_preload_all": False, # override limit and preload every row
    # Timestamp of the last completed disk walk — compared against
    # disk_scan_staleness_hours to decide whether to skip on next boot.
    "last_disk_scan_ts": 0.0,
}

_config_lock = threading.Lock()

# Per-channel defaults (matches YTArchiver.py CHANNEL_DEFAULTS at line 173,
# extended with the full set of fields actually stored).
CHANNEL_DEFAULTS_ALL = {
    "name": "",
    "folder": "",
    "folder_override": "", # set when on-disk folder differs from name
    "url": "",
    "resolution": "720",
    "mode": "new",
    "min_duration": 0,
    "max_duration": 0,
    "split_years": False,
    "split_months": False,
    "auto_transcribe": False,
    "auto_metadata": True,
    "compress_enabled": False,
    "compress_level": "",
    "compress_output_res": "",
    "compress_batch_size": 20,
    "last_sync": "",
    "from_date": "",
    "date_after": "", # YYYY-MM-DD lower bound for sync
    "initialized": False, # set after first sync completes
    "init_complete": False, # full-bootstrap done (all pages walked)
    "init_batch_after": "", # ISO timestamp — batch cooldown end
    "batch_resume_index": 0, # resume index for large-channel batch walks
    "transcription_complete": False,
    "transcription_pending": 0,
    "metadata_pending": 0,
}


def load_config() -> Dict[str, Any]:
    """Load the real YTArchiver config. Falls back to defaults if missing.

    Recovery path: if the primary file is corrupt, try the most recent
    dated snapshot in `backups/` before giving up and returning defaults.
    """
    if not CONFIG_FILE.exists():
        return dict(DEFAULT_CONFIG)
    try:
        with _config_lock:
            with CONFIG_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)
        merged = dict(DEFAULT_CONFIG)
        merged.update(data)
        return merged
    except (json.JSONDecodeError, OSError) as e:
        print(f"[config] WARNING: failed to load {CONFIG_FILE}: {e}")
        # Attempt recovery from the most recent dated snapshot
        try:
            backup_dir = APP_DATA_DIR / "backups"
            if backup_dir.is_dir():
                snaps = sorted(backup_dir.glob("config_*.json"),
                               key=lambda p: p.stat().st_mtime, reverse=True)
                for snap in snaps:
                    try:
                        with snap.open("r", encoding="utf-8") as f:
                            data = json.load(f)
                        print(f"[config] recovered from snapshot: {snap.name}")
                        merged = dict(DEFAULT_CONFIG)
                        merged.update(data)
                        # Sideline the corrupt file so the next launch uses the snapshot
                        try:
                            CONFIG_FILE.rename(CONFIG_FILE.with_suffix(".json.corrupt"))
                        except OSError:
                            pass
                        return merged
                    except (json.JSONDecodeError, OSError):
                        continue
        except Exception as _r:
            print(f"[config] recovery attempt failed: {_r}")
        return dict(DEFAULT_CONFIG)


def config_file_exists() -> bool:
    return CONFIG_FILE.exists()


def config_is_writable() -> bool:
    """Always writable — kept as a function so existing call sites
    don't break, but the env-var gate is gone. YTArchiver is the
    primary app now; there's no side-by-side-with-tkinter scenario
    to protect against.
    """
    return True


def backup_config_on_start(keep: int = 10) -> Optional[str]:
    """Copy the current config.json to a dated snapshot in
    %APPDATA%\\YTArchiver\\backups\\config_YYYY-MM-DD_HHMMSS.json.

    Keeps only the most recent `keep` snapshots. Non-fatal on any error.
    Returns the path written, or None on skip/failure.
    """
    import shutil, datetime as _dt
    try:
        if not CONFIG_FILE.exists():
            return None
        backup_dir = APP_DATA_DIR / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        ts = _dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        dst = backup_dir / f"config_{ts}.json"
        shutil.copy2(str(CONFIG_FILE), str(dst))
        # Prune to `keep` most-recent
        snaps = sorted(backup_dir.glob("config_*.json"),
                       key=lambda p: p.stat().st_mtime, reverse=True)
        for old in snaps[keep:]:
            try: old.unlink()
            except OSError: pass
        return str(dst)
    except OSError:
        return None


def save_config(cfg: Dict[str, Any]) -> bool:
    """Save config back to disk. Gated by config_is_writable()."""
    if not config_is_writable():
        print("[config] write blocked")
        return False
    try:
        APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
        with _config_lock:
            # Write-via-temp for atomicity (matches tkinter app's save_config)
            tmp = CONFIG_FILE.with_suffix(".json.tmp")
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2)
            tmp.replace(CONFIG_FILE)
        return True
    except OSError as e:
        print(f"[config] ERROR: failed to save: {e}")
        return False


# ── Helpers the UI actually needs ───────────────────────────────────────

def channels_for_subs_ui(cfg: Dict[str, Any]):
    """
    Transform config['channels'] into the row dict format the Subs table
    renderer expects. Returns (rows, total_label).
    """
    channels = sorted(
        cfg.get("channels", []),
        key=lambda c: c.get("name", "").lower(),
    )
    rows = []
    total_gb = 0.0
    for ch in channels:
        folder = ch.get("name", "") or ch.get("folder", "")
        res = (ch.get("resolution", "") or "").strip() or "—"
        mode = ch.get("mode", "")
        # YTArchiver stores min/max as SECONDS on disk (180 = 3 minutes).
        # Display in minutes to match the original UI + user expectation.
        min_d = int(ch.get("min_duration", 0) or 0)
        max_d = int(ch.get("max_duration", 0) or 0)
        min_mins = max(0, min_d // 60)
        max_mins = max(0, max_d // 60)
        # Last-sync shown as relative ("10hr ago") to match YTArchiver.py:5307
        ls_raw = (ch.get("last_sync") or "").strip()
        ls_str = "Never"
        if ls_raw:
            try:
                import datetime as _dt
                dt = _dt.datetime.strptime(ls_raw, "%Y-%m-%d %H:%M")
                diff_mins = int((_dt.datetime.now() - dt).total_seconds() // 60)
                if diff_mins < 1:
                    ls_str = "just now"
                elif diff_mins < 60:
                    ls_str = f"{diff_mins}m ago"
                elif diff_mins < 1440:
                    ls_str = f"{diff_mins // 60}hr ago"
                elif diff_mins < 43200:
                    ls_str = f"{diff_mins // 1440}d ago"
                else:
                    ls_str = f"{diff_mins // 43200}mo ago"
            except Exception:
                ls_str = ls_raw[:12]
        # Original YTArchiver shows "A ✓" when the channel has auto-<X>=true
        # and the <X>-enabled flag is also on (e.g. `auto_transcribe=True` AND
        # the channel's been transcribed at least once). Match that here by
        # prefixing "A " to the checkmark when the auto_* flag is set.
        def _mark(auto_key: str, enabled: bool) -> str:
            is_auto = bool(ch.get(auto_key))
            if enabled and is_auto: return "A \u2713"
            if enabled: return "\u2713"
            return "\u2014"
        # Average video size = total size ÷ number of videos. the user wants a
        # quick way to eyeball which channels are shipping big files vs
        # tons of small ones. Displayed in MB (most channels are in that
        # range; GB shown when average is over a gig).
        n_v = int(ch.get("n_vids", 0) or 0)
        sz_gb = float(ch.get("size_gb", 0) or 0)
        if n_v > 0 and sz_gb > 0:
            avg_mb = (sz_gb * 1024.0) / n_v
            if avg_mb >= 1024:
                avg_str = f"{avg_mb/1024:.1f} GB"
            elif avg_mb >= 100:
                avg_str = f"{int(avg_mb)} MB"
            else:
                avg_str = f"{avg_mb:.0f} MB"
        else:
            avg_str = "\u2014"

        rows.append({
            "folder": folder,
            "res": res + ("p" if res.isdigit() else ""),
            "min": f"{min_mins}m" if min_mins else "—",
            "max": f"{max_mins}m" if max_mins else "—",
            "compress": "\u2713" if ch.get("compress_enabled") else "\u2014",
            # Transcribe / Metadata treat the auto flag itself as "enabled".
            "transcribe": _mark("auto_transcribe", bool(ch.get("auto_transcribe"))),
            "metadata": _mark("auto_metadata", bool(ch.get("auto_metadata"))),
            "last_sync": ls_str,
            "n_vids": f"{ch.get('n_vids', 0):,}" if ch.get("n_vids") else "—",
            "size": f"{ch.get('size_gb', 0):.1f} GB" if ch.get("size_gb") else "—",
            "avg_size": avg_str,
            # Pending counters for the "Queue Pending" badge in the subs header.
            # `transcription_pending` is incremented by sync when videos land
            # on auto_transcribe channels; `metadata_pending` similarly for
            # channels with auto_metadata on.
            "_pending_tx": int(ch.get("transcription_pending", 0) or 0),
            "_pending_meta": int(ch.get("metadata_pending", 0) or 0),
        })
        total_gb += ch.get("size_gb", 0) or 0

    if total_gb >= 1024:
        total_label = f"Total: {total_gb/1024:.1f} TB"
    elif total_gb > 0:
        total_label = f"Total: {total_gb:.1f} GB"
    else:
        total_label = f"Total: \u2014 ({len(rows)} channels)"
    return rows, total_label


def _fmt_time_ago(ts) -> str:
    """Mirror YTArchiver.py:32677 _fmt_time_ago."""
    try:
        diff = time.time() - float(ts or 0)
    except (TypeError, ValueError):
        return ""
    if diff <= 0: return ""
    if diff < 60: return "just now"
    if diff < 3600: return f"{int(diff // 60)}m ago"
    if diff < 86400: return f"{int(diff // 3600)}h ago"
    return f"{int(diff // 86400)}d ago"


def _fmt_size(raw) -> str:
    """Mirror YTArchiver.py:32686 _fmt_size. Accepts int or numeric string."""
    try:
        b = int(raw)
    except (TypeError, ValueError):
        return ""
    if b >= 1_073_741_824: return f"{b / 1_073_741_824:.1f} GB"
    if b >= 1_048_576: return f"{b / 1_048_576:.0f} MB"
    if b >= 1_024: return f"{b / 1_024:.0f} KB"
    return f"{b} B"


def _fmt_dur(raw) -> str:
    """Mirror YTArchiver.py:32697 _fmt_dur. Accepts int seconds or string."""
    try:
        s = int(raw)
    except (TypeError, ValueError):
        return ""
    if s <= 0:
        return ""
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def _extract_video_id(video_url: str) -> str:
    """Parse the v=XXXX param from a YouTube URL."""
    if not video_url:
        return ""
    import re
    m = re.search(r'[?&]v=([A-Za-z0-9_-]{11})', video_url)
    return m.group(1) if m else ""


def recent_for_ui(cfg: Dict[str, Any]):
    """Transform config['recent_downloads'] into UI-ready rows.

    Real entries stored by YTArchiver contain `size` in raw bytes (as str),
    `duration` in raw seconds (as str), `date` as YYYYMMDD, and
    `download_ts` as Unix epoch. We format those into the display strings
    the Recent tab expects.

    Also resolves each row's thumbnail sidecar (via `backend.index.find_thumbnail`)
    and exposes it as `thumbnail_url` so the optional grid-card Recent view
    can render real thumbnails. The legacy table view ignores these extras.
    """
    # Pull thumbnail resolver lazily — avoids import-cycle risk since
    # backend.index already imports this module for some helpers.
    try:
        from .index import find_thumbnail as _find_thumb, _file_url as _thumb_url
    except Exception:
        _find_thumb = None
        _thumb_url = None

    out = []
    for r in cfg.get("recent_downloads", [])[:200]:
        # Prefer download_ts for the "time ago" column; fall back to any
        # already-formatted `time` field an older config might carry.
        t = _fmt_time_ago(r.get("download_ts")) or r.get("time", "") or ""
        # Size / duration are raw — format them like the original did.
        size_raw = r.get("size", "")
        size_disp = _fmt_size(size_raw)
        dur_disp = _fmt_dur(r.get("duration", ""))
        fp = r.get("filepath", "")
        vid = r.get("video_id") or _extract_video_id(r.get("video_url", ""))

        # Thumbnail resolution for the grid-card view. Best-effort — if the
        # sidecar isn't on disk the grid falls back to its gradient placeholder.
        thumbnail_url = ""
        if fp and _find_thumb and _thumb_url:
            try:
                tp = _find_thumb(fp, vid)
                if tp:
                    thumbnail_url = _thumb_url(tp)
            except Exception:
                pass

        # size_bytes — raw int for the grid meta line (also used by the JS
        # `_fmtBytes` helper if it wants to re-format).
        try: size_bytes = int(size_raw) if size_raw not in ("", None) else 0
        except Exception: size_bytes = 0

        # uploaded — prefer explicit `date` (YYYYMMDD) on the entry, fall
        # back to `download_ts` so the grid card still shows something.
        uploaded_disp = ""
        date_str = str(r.get("date") or "")
        if len(date_str) == 8 and date_str.isdigit():
            uploaded_disp = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        elif r.get("download_ts"):
            try:
                import datetime as _dt
                uploaded_disp = _dt.datetime.fromtimestamp(
                    float(r["download_ts"])).strftime("%Y-%m-%d")
            except Exception:
                uploaded_disp = ""

        out.append({
            "title": r.get("title", ""),
            "channel": r.get("channel", ""),
            "time": t,
            "duration": dur_disp,
            "size": size_disp,
            # Pass through identifiers so the UI can double-click to open
            # in Watch view with the real video file.
            "filepath": fp,
            "video_id": vid,
            # Grid-card extras (ignored by the list view).
            "thumbnail_url": thumbnail_url,
            "size_bytes": size_bytes,
            "uploaded": uploaded_disp,
            "download_ts": r.get("download_ts") or 0,
        })
    return out


def autorun_history_entries_for_ui(cfg: Dict[str, Any]):
    """
    Parse config['autorun_history'] into structured cells for the grid-aligned
    activity-log renderer.

    Real YTArchiver stores each entry as one string like:
        "[Metdta] 3:16pm, Apr 10 \u2014 ExampleChannel \u2014 5 fetched \u00b7 2800 skipped \u00b7 0 errors \u00b7 took 36s"

    We split it by em-dashes into (kind, time/date, channel, body), then split
    the body on bullet-dots into primary/secondary/errors/took.
    """
    import re
    entries = cfg.get("autorun_history", [])[-100:]
    out = []
    alt = False
    for entry in entries:
        if not isinstance(entry, str):
            continue
        m = re.match(r"^\s*\[\s*(\w+)\s*\]\s*(.*)$", entry)
        if not m:
            out.append({
                "cells": {"kind": "", "time_date": entry,
                          "channel": "", "primary": "", "secondary": "",
                          "errors": "", "took": "", "row_tag": ""},
                "alt": alt,
            })
            alt = not alt
            continue
        kind = m.group(1).strip()
        rest = m.group(2)
        # Split by em-dash surrounded by whitespace
        parts = [p.strip() for p in re.split(r"\s+\u2014\s+", rest)]
        time_date = parts[0] if len(parts) > 0 else ""
        channel = parts[1] if len(parts) > 1 else ""
        body = parts[2] if len(parts) > 2 else ""

        # Split body on middle-dot "·"
        bparts = [p.strip() for p in body.split("\u00b7")]
        primary = bparts[0] if len(bparts) > 0 else ""
        secondary = ""
        tertiary = ""
        errors = ""
        took = ""
        if len(bparts) >= 5:
            # Consolidated [Dwnld] shape ( merged row):
            # primary · transcribed · metadata · errors · took
            # Each count gets its own grid cell in the UI so a wider
            # window uses its horizontal space cleanly (vs. cramming
            # two counts into one cell with internal ellipsis).
            secondary = bparts[1]
            tertiary = bparts[2]
            errors, took = bparts[3], bparts[4]
        elif len(bparts) == 4:
            # Metdta shape: primary, skipped/refreshed, errors, took
            secondary, errors, took = bparts[1], bparts[2], bparts[3]
        elif len(bparts) == 3:
            # Simpler shape: primary, errors, took
            errors, took = bparts[1], bparts[2]
        elif len(bparts) == 2:
            took = bparts[1]

        tag = _hist_tag_for_kind(kind, body) or ""
        out.append({
            "cells": {
                "kind": kind,
                "time_date": time_date,
                "channel": channel,
                "primary": primary,
                "secondary": secondary,
                "tertiary": tertiary,
                "errors": errors,
                "took": took,
                "row_tag": tag,
            },
            "alt": alt,
        })
        alt = not alt
    return out


def _hist_tag_for_kind(kind: str, rest: str):
    """Pick the row_tag color for a kind. Each match family accepts
    either a non-zero integer OR a single \u2713 checkmark — the latter
    represents "exactly 1 of this" per single-video polish.
    """
    import re
    # Either "\u2713 foo" or "N foo" (N >= 1) counts as "work happened".
    def _done(pattern: str) -> bool:
        check = re.search(r"\u2713\s+(?:" + pattern + r")\b", rest)
        if check:
            return True
        m = re.search(r"\b(\d+)\s+(?:" + pattern + r")\b", rest)
        return bool(m and int(m.group(1)) > 0)

    if kind == "Trnscr":
        if _done("transcribed"):
            return "hist_blue"
    elif kind == "Metdta":
        if _done("fetched") or _done("refreshed"):
            return "hist_pink"
    elif kind in ("Manual", "Auto", "Dwnld"):
        if _done("downloaded"):
            return "hist_green"
    elif kind == "ReDwnl":
        if _done("replaced") or "running..." in rest:
            return "hist_redwnl"
    elif kind == "Cmprss":
        if _done("compressed"):
            return "hist_compress"
    elif kind == "Reorg":
        if _done("moved|reorganized"):
            return "hist_reorg"
    return None
