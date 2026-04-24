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
    # No archive-root default. First-launch flow forces the user to
    # pick a folder via the welcome modal before the app is usable.
    # Previously we assumed `~/Channel Archives` which silently got
    # baked into config on first load, leaving the app half-configured
    # if the user dismissed the welcome prompt.
    "output_dir": "",
    "video_out_dir": "",
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

_config_lock = threading.RLock()  # audit F-36: reentrant so nested
# load_config→save_config→load_config sequences don't self-deadlock.
# Several helpers (append_pending_tx_id, remove_pending_tx_id, etc.)
# acquire this lock and can recurse via save_config → migration trigger
# → save_config again. A non-reentrant Lock would wedge the first such
# path the moment it recursed.

# audit E-39: periodic backup trigger. Writes a dated snapshot every
# _BACKUP_EVERY_N_SAVES save_config() calls so recovery windows are
# hourly (typical users save ~1-10 times per hour in normal use)
# rather than only-at-startup. Counter resets on process restart;
# backup_config_on_start still fires the first one at launch.
_save_counter = 0
_BACKUP_EVERY_N_SAVES = 20


def _long_path(p: str) -> str:
    """audit F-31: Windows \\?\\-prefix long paths (>240 chars). Other
    modules can import and adopt when their derived paths might exceed
    MAX_PATH. Config paths here are always short so the helper is not
    applied to CONFIG_FILE itself — output_dir and tp_archive_roots
    callers are the risk surface.

    No-op on non-Windows and on paths already prefixed."""
    if os.name != "nt" or not p:
        return p
    if p.startswith("\\\\?\\") or len(p) <= 240:
        return p
    # UNC paths need the \\?\UNC\ form
    if p.startswith("\\\\"):
        return "\\\\?\\UNC\\" + p.lstrip("\\")
    return "\\\\?\\" + p

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
    # Authoritative pending list: video IDs that downloaded onto this
    # channel without entering the auto-transcribe path (channel had
    # auto_transcribe=False at download time). Queue Pending reads
    # this list directly instead of folder-scanning. Drained by
    # `remove_pending_tx_id` when a transcribe completes for the id.
    "pending_tx_ids": [],
}


def _migrate_pending_tx_ids(cfg: Dict[str, Any]) -> None:
    """First-launch-after-v47.7 migration.

    For every channel, if `pending_tx_ids` is missing we add it as an
    empty list AND zero `transcription_pending` so a drifted legacy
    counter (e.g. 730) doesn't light up the Subs "-X" indicator for a
    channel that's actually fully transcribed. Fresh downloads after
    launch populate the list naturally via the sync-download hook.

    Idempotent: runs on every load() but only mutates channels that
    don't already have the field.
    """
    for ch in cfg.get("channels", []) or []:
        if not isinstance(ch, dict):
            continue
        if "pending_tx_ids" not in ch or not isinstance(
                ch.get("pending_tx_ids"), list):
            ch["pending_tx_ids"] = []
            # audit D-44: previous migration unconditionally zeroed
            # transcription_pending and flipped transcription_complete=True
            # for EVERY channel missing pending_tx_ids — which silently
            # wiped real in-flight pending counts when someone upgraded
            # with a channel legitimately mid-transcribe. Now: only
            # reset to "complete" if the stored counter is already 0 or
            # missing (truly no pending work). If the counter is > 0
            # we leave it alone; next sync pass will reconcile naturally
            # via the pending_tx_ids append path.
            _legit_pending = int(ch.get("transcription_pending") or 0)
            if _legit_pending <= 0:
                ch["transcription_pending"] = 0
                ch["transcription_complete"] = True


def append_pending_tx_id(channel_name: str, video_id: str) -> None:
    """Record a downloaded video as pending-transcription for its
    channel. Called from sync.py when a video lands AND the channel
    has auto_transcribe=False. No-op if the ID is already in the list
    (idempotent — repeated sync passes can't double-count).

    Silent on any error: the counter is user-visible but not
    load-bearing, so we never raise into the sync pipeline."""
    if not channel_name or not video_id:
        return
    try:
        if not config_is_writable():
            return
        cfg = load_config()
        changed = False
        for ch in cfg.get("channels", []) or []:
            if (ch.get("name") or "") != channel_name:
                continue
            ids = ch.get("pending_tx_ids")
            if not isinstance(ids, list):
                ids = []
                ch["pending_tx_ids"] = ids
            if video_id in ids:
                return
            ids.append(video_id)
            ch["transcription_pending"] = len(ids)
            ch["transcription_complete"] = False
            changed = True
            break
        if changed:
            save_config(cfg)
    except Exception:
        pass


def remove_pending_tx_id(video_id: str) -> bool:
    """Drop a completed transcription's video ID from whichever
    channel's pending list it's in. Called from the transcribe
    worker's completion path.

    Returns True if any list actually changed (useful for telemetry).
    Silent on error; never raises.
    """
    if not video_id:
        return False
    try:
        if not config_is_writable():
            return False
        cfg = load_config()
        changed = False
        for ch in cfg.get("channels", []) or []:
            ids = ch.get("pending_tx_ids")
            if not isinstance(ids, list):
                continue
            if video_id in ids:
                ids.remove(video_id)
                ch["transcription_pending"] = len(ids)
                if not ids:
                    ch["transcription_complete"] = True
                changed = True
        if changed:
            save_config(cfg)
        return changed
    except Exception:
        return False


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
        # Run migrations exactly once per config, then stamp a flag so
        # subsequent load_config calls skip the work. Previously
        # _migrate_pending_tx_ids ran on every load — idempotent but
        # wasteful, and any future migration accidentally breaking
        # idempotency would silently corrupt state.
        if not merged.get("_migration_v2_pending_tx_ids"):
            _migrate_pending_tx_ids(merged)
            # audit D-43: only stamp the migration flag AFTER
            # save_config returns True. Previously we set the flag,
            # then wrapped save in except:pass — if the save failed
            # (antivirus lock, OneDrive sync), the migration re-ran
            # every launch, and its zero-out of transcription_pending
            # silently wiped real pending work between boots. Setting
            # the flag post-save guarantees the re-run only happens
            # if we truly never persisted.
            try:
                if save_config(merged):
                    merged["_migration_v2_pending_tx_ids"] = True
                    # Re-save once more so the flag itself lands on disk.
                    save_config(merged)
                else:
                    print("[config] migration save failed; will retry next launch")
            except Exception as _me:
                print(f"[config] migration save exception: {_me}")
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
    """Save config back to disk. Gated by config_is_writable().

    audit C-8: fsyncs the temp file before os.replace so a power loss
    or BSOD between write and rename can't commit a zero-byte /
    truncated file over the real one. Also cheap (<10ms per save for
    a typical config).

    audit E-39: triggers a dated snapshot every _BACKUP_EVERY_N_SAVES
    saves so the recovery chain is minutes/hours old rather than
    hours/days. backup_config_on_start still handles the at-launch
    snapshot.
    """
    global _save_counter
    if not config_is_writable():
        print("[config] write blocked")
        return False
    try:
        APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
        # audit SM-8: trim autorun_history on save so the config
        # file can't grow unbounded across years of use. UI only
        # shows the last 100 entries; keep 500 on-disk for some
        # scroll headroom + recovery buffer. Trimming in-place on
        # the passed dict is fine — the UI uses a fresh read of the
        # last 100 slice per render, so in-memory state stays consistent.
        try:
            _hist = cfg.get("autorun_history")
            if isinstance(_hist, list) and len(_hist) > 500:
                cfg["autorun_history"] = _hist[-500:]
        except Exception:
            pass
        with _config_lock:
            # Write-via-temp for atomicity (matches tkinter app's save_config)
            tmp = CONFIG_FILE.with_suffix(".json.tmp")
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2)
                # audit C-8: flush + fsync before closing so the
                # os.replace below commits a file whose contents are
                # physically on disk (not just in the OS write cache).
                try:
                    f.flush()
                    os.fsync(f.fileno())
                except OSError:
                    pass
            tmp.replace(CONFIG_FILE)
        # audit E-39: periodic snapshot. Runs outside the lock because
        # backup_config_on_start does its own I/O. Non-fatal on failure.
        _save_counter += 1
        if _save_counter >= _BACKUP_EVERY_N_SAVES:
            _save_counter = 0
            try:
                backup_config_on_start(keep=20)
            except Exception:
                pass
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
    # Pre-derive the archive root once — we need it to check for
    # `_redownload_progress.json` in each channel's folder so the
    # Subs table can flag channels with unfinished redownloads.
    _base_dir = (cfg.get("output_dir") or "").strip()
    try:
        from .sync import channel_folder_name as _cfn_for_redwnl
    except Exception:
        _cfn_for_redwnl = None
    rows = []
    total_gb = 0.0
    for ch in channels:
        folder = ch.get("name", "") or ch.get("folder", "")
        res = (ch.get("resolution", "") or "").strip() or "—"
        mode = ch.get("mode", "")
        # YTArchiver stores min/max as SECONDS on disk (180 = 3 minutes).
        # Display in minutes to match the original UI + user expectation.
        # audit F-35: sub-minute durations used to floor to "0m" which the
        # renderer then dashed to "—", hiding a real filter. Now:
        # values between 1-59 seconds show as "<1m" so the user knows
        # the filter is set (just non-zero rather than invisible).
        min_d = int(ch.get("min_duration", 0) or 0)
        max_d = int(ch.get("max_duration", 0) or 0)
        min_mins = max(0, min_d // 60)
        max_mins = max(0, max_d // 60)
        # Expose sub-minute sentinels so the UI can render specially.
        if 0 < min_d < 60 and min_mins == 0:
            min_mins = -1  # signals "<1m"
        if 0 < max_d < 60 and max_mins == 0:
            max_mins = -1
        # Last-sync shown as relative ("10hr ago") to match YTArchiver.py:5307.
        # audit D-55: parser now tolerates both the legacy naive-string
        # format AND an epoch-float value. Epoch is DST-safe and compares
        # directly to time.time(); new code writing last_sync can emit
        # either. Naive strings get best-effort parsing but are marked
        # approximate (read as local-TZ without adjustment, so two days
        # per year the delta will be off by ±1h — tolerable for a UI
        # relative-time display).
        ls_raw_val = ch.get("last_sync")
        ls_str = "Never"
        _diff_secs: Optional[float] = None
        if isinstance(ls_raw_val, (int, float)) and ls_raw_val > 0:
            _diff_secs = max(0.0, time.time() - float(ls_raw_val))
        elif isinstance(ls_raw_val, str) and ls_raw_val.strip():
            ls_raw = ls_raw_val.strip()
            try:
                import datetime as _dt
                dt = _dt.datetime.strptime(ls_raw, "%Y-%m-%d %H:%M")
                _diff_secs = max(0.0,
                                 (_dt.datetime.now() - dt).total_seconds())
            except Exception:
                ls_str = ls_raw[:12]
        if _diff_secs is not None:
            diff_mins = int(_diff_secs // 60)
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
        # Original YTArchiver shows "A ✓" when the channel has auto-<X>=true
        # and the <X>-enabled flag is also on (e.g. `auto_transcribe=True` AND
        # the channel's been transcribed at least once). Match that here by
        # prefixing "A " to the checkmark when the auto_* flag is set.
        # Pending deltas for "A ✓ -X" display: when a channel with
        # auto_transcribe=True has new videos that escaped the auto
        # path (sync downloaded them with the flag momentarily off,
        # or a pipeline hiccup), the Subs cell shows how far behind
        # we are.
        _pending_tx_list = ch.get("pending_tx_ids") or []
        _pending_tx_n = len(_pending_tx_list) if isinstance(_pending_tx_list, list) else 0

        def _mark(auto_key: str, enabled: bool, behind: int = 0) -> str:
            is_auto = bool(ch.get(auto_key))
            delta = f" -{behind}" if behind > 0 else ""
            if enabled and is_auto: return f"A \u2713{delta}"
            if enabled: return f"\u2713{delta}"
            if behind > 0: return f"\u2014 -{behind}"
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

        # Pending-redownload probe: the redownload pipeline persists a
        # `_redownload_progress.json` next to the channel's videos while
        # a pass is in flight. When that file is present the Subs table
        # paints a small chartreuse dot next to the channel name so the
        # user sees at a glance which channels still have unfinished
        # resolution upgrades. We ALSO parse the file to extract the
        # target resolution so the right-click menu can show "Continue
        # Redownload at 480p" instead of the generic "Redownload at..."
        # submenu when this channel has pending work.
        _pending_redwnl = False
        _pending_redwnl_res = ""
        if _base_dir and _cfn_for_redwnl is not None:
            try:
                import os as _os
                _ch_folder = _os.path.join(_base_dir, _cfn_for_redwnl(ch))
                _pp = _os.path.join(_ch_folder,
                                     "_redownload_progress.json")
                if _os.path.isfile(_pp):
                    _pending_redwnl = True
                    try:
                        import json as _j
                        with open(_pp, "r", encoding="utf-8") as _f:
                            _data = _j.load(_f)
                        _pending_redwnl_res = (
                            _data.get("resolution") or "").strip()
                    except Exception:
                        pass
            except Exception:
                _pending_redwnl = False

        rows.append({
            "folder": folder,
            "res": res + ("p" if res.isdigit() else ""),
            "min": f"{min_mins}m" if min_mins else "—",
            "max": f"{max_mins}m" if max_mins else "—",
            "compress": "\u2713" if ch.get("compress_enabled") else "\u2014",
            # Transcribe / Metadata treat the auto flag itself as "enabled".
            # `_pending_tx_n` is the length of pending_tx_ids: videos that
            # downloaded while auto_transcribe was off (the -X indicator).
            "transcribe": _mark("auto_transcribe",
                                 bool(ch.get("auto_transcribe")),
                                 behind=_pending_tx_n),
            "metadata": _mark("auto_metadata", bool(ch.get("auto_metadata"))),
            "last_sync": ls_str,
            "n_vids": f"{ch.get('n_vids', 0):,}" if ch.get("n_vids") else "—",
            "size": f"{ch.get('size_gb', 0):.1f} GB" if ch.get("size_gb") else "—",
            "avg_size": avg_str,
            # Queue-Pending badge derives from the authoritative ID lists.
            # `transcription_pending` is kept as a back-compat mirror but
            # is no longer the source of truth — the IDs are.
            "_pending_tx": _pending_tx_n,
            "_pending_meta": int(ch.get("metadata_pending", 0) or 0),
            # Chartreuse dot indicator in the Subs folder cell — True
            # when `_redownload_progress.json` exists for this channel.
            "_pending_redownload": _pending_redwnl,
            # Saved target resolution so the right-click menu can show
            # "Continue Redownload at 480p" instead of the generic
            # "Redownload at..." submenu when there's pending work.
            "_redownload_res": _pending_redwnl_res,
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
    # Treat None / 0 / empty string as missing, not as Unix epoch.
    # Without this, missing download_ts rendered as "54 years ago".
    if not ts:
        return ""
    try:
        diff = time.time() - float(ts)
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
    # Sort newest-first by download_ts BEFORE slicing. Without the
    # explicit sort, users with >200 lifetime downloads could hide
    # fresh entries: any past code path that appended to the END of
    # `recent_downloads` instead of the front leaves new entries in
    # positions 201+, silently truncated by the [:200] slice. Sort
    # guarantees the newest 200 are always shown regardless of
    # insertion order.
    _all_recent = cfg.get("recent_downloads", []) or []
    _sorted_recent = sorted(
        _all_recent,
        key=lambda r: (r.get("download_ts") or 0),
        reverse=True,
    )[:200]
    for r in _sorted_recent:
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
        # audit F-34: validate YYYYMMDD parses as a real calendar date
        # before accepting it — otherwise "99999999" stored on a
        # corrupted entry renders as "9999-99-99" and confuses the UI.
        uploaded_disp = ""
        date_str = str(r.get("date") or "")
        _date_ok = False
        if len(date_str) == 8 and date_str.isdigit():
            try:
                import datetime as _dt_v
                _dt_v.datetime.strptime(date_str, "%Y%m%d")
                _date_ok = True
            except ValueError:
                _date_ok = False
        if _date_ok:
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
            # Missing download_ts used to default to 0 (Unix epoch), which
            # the time-ago formatter rendered as "54 years ago" and pushed
            # rows to the top under descending-time sort. None lets the
            # display layer show "—" or similar.
            "download_ts": r.get("download_ts") or None,
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
            if kind == "ReDwnl":
                # ReDwnl body: replaced · skipped · errors · took
                # Pack all 3 counts into the first 3 num columns
                # (primary · secondary · tertiary) and leave the
                # errors cell empty. Otherwise the middle tertiary
                # column (reserved for [Dwnld]'s metadata count)
                # renders empty and produces a huge gap between
                # "skipped" and "errors" in the grid. The errors
                # count's "N errors" regex still gets its red
                # highlight from _HIST_HILITE regardless of which
                # cell it sits in.
                secondary = bparts[1]
                tertiary = bparts[2]
                took = bparts[3]
            else:
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
