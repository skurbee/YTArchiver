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

import copy
import json
import math
import os
import threading
import time
from pathlib import Path
from typing import Any

from .log import get_logger, swallow

_log = get_logger(__name__)


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

# Automatic Trash retention is intentionally conservative. A brand-new
# install uses the 30-day policy immediately, but an existing install that
# predates the setting receives a full 30-day grace period before the first
# unattended permanent deletion can be considered.
TRASH_RETENTION_DEFAULT_DAYS = 30
TRASH_RETENTION_MAX_DAYS = 3650
TRASH_RETENTION_UPGRADE_GRACE_SECONDS = 30 * 24 * 60 * 60
TRASH_RETENTION_CHANGE_GRACE_SECONDS = 24 * 60 * 60


def _safe_retention_grace(value: Any) -> float:
    try:
        grace = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, grace) if math.isfinite(grace) else 0.0

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
    # Auto-sync firing mode: "timer" (persistent countdown) or "clock"
    # (wall-clock boundaries; 12h/24h schedules can choose an anchor time).
    "autorun_mode": "clock",
    "autorun_clock_time_12": 0,
    "autorun_clock_time_24": 0,
    # Persistent global YouTube traffic governor.  Presets intentionally use
    # app-level yt-dlp operation units rather than claiming exact HTTP request
    # counts (one extractor launch can make several internal requests).
    "youtube_traffic_mode": "conservative",
    "youtube_traffic_custom_daily": 750,
    "youtube_traffic_custom_hourly": 90,
    "youtube_traffic_custom_min_gap": 10,
    "youtube_traffic_custom_max_gap": 20,
    "autorun_history": [],
    "log_mode": "Simple",
    "show_activity_log": True,
    # Fresh installs manage channels in Browse by default. load_config()
    # overrides this to True exactly once for pre-existing config files that
    # do not yet contain the key, preserving the established Subs workflow
    # for users who upgrade from an earlier release.
    "legacy_subs_tab": False,
    # yt-dlp release channel the in-app updater targets. "stable" (the
    # default, safest) or "nightly" (yt-dlp's beta channel — carries
    # YouTube fixes days-to-weeks ahead of stable; use it when stable is
    # current but downloads still 403). Read by ytdlp_update().
    "ytdlp_channel": "stable",
    # Keep yt-dlp current while the app remains open. Automatic updates are
    # limited to the app-managed copy; external installations fall back to a
    # notification so YTArchiver never silently mutates another toolchain.
    "ytdlp_update_mode": "automatic",
    # Persisted elapsed-time cadence. The monitor checks at launch when due
    # and continues to honor this interval without an app restart.
    "ytdlp_update_check_days": 1,
    "last_ytdlp_update_check_ts": 0.0,
    # A managed-copy update discovered while the app is busy survives a
    # crash/restart and is retried before the next remote check.
    "ytdlp_update_pending_version": "",
    "ytdlp_update_pending_channel": "",
    "autorun_gpu": False,
    "autorun_sync": False,
    "chan_col_widths": {},
    "recent_col_widths": {},
    "deps_checked": False,
    # First-run wizard completion flag. False -> the blocking onboarding
    # wizard (archive-folder picker + dependency installer) is shown on
    # launch. Set True once the user finishes (or skips through) the
    # wizard. Drives a backend-confirmed first-run check so the wizard
    # can't silently no-op the way the old output_dir-only check could.
    "onboarded": False,
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
    "archive_capacity_warning_mode": "percent",
    "archive_capacity_warning_percent": 90,
    "archive_capacity_warning_free_gb": 100,
    # Timestamp of the last completed disk walk — compared against
    # disk_scan_staleness_hours to decide whether to skip on next boot.
    "last_disk_scan_ts": 0.0,
    # Timestamp of the last successful full backup export (epoch float).
    # Surfaced in Settings so the user knows how stale their backup is.
    "last_backup_ts": 0.0,
    # v80 auto-backup: "off" | "daily" | "weekly" | "monthly". When on,
    # the AutoBackupScheduler writes the full-state export into
    # `<archive root>\YTArchiver Info\` on that cadence.
    "auto_backup_interval": "off",
    # Include the Search database in manual and scheduled full backups,
    # regardless of its size. Only an explicit False opts out.
    "backup_include_search_db": True,
    # Timestamp of the last SCHEDULED backup (manual exports don't
    # touch this — the schedule stays honest even if the user also
    # exports by hand).
    "last_auto_backup_ts": 0.0,
    # App-managed Trash is kept for 30 days by default. 0 means Never.
    # The grace timestamp is maintained by migrations/settings code and is
    # deliberately not user-settable through settings_save.
    "trash_retention_days": TRASH_RETENTION_DEFAULT_DAYS,
    "trash_retention_grace_until_ts": 0.0,
}

_config_lock = threading.RLock()  # reentrant so nested
# load_config→save_config→load_config sequences don't self-deadlock.
# Several helpers (append_pending_tx_id, remove_pending_tx_id, etc.)
# acquire this lock and can recurse via save_config → migration trigger
# → save_config again. A non-reentrant Lock would wedge the first such
# path the moment it recursed.

# RMW transaction lock. The pattern
#     cfg = load_config(); cfg["foo"] = bar; save_config(cfg)
# was used in 127 places — each call individually thread-safe (above)
# but the COMBINED read-modify-write was NOT atomic across threads. Two
# concurrent appends to channels[].pending_tx_ids could lose one. The
# `config_transaction()` context manager below wraps both operations
# under one lock acquisition so RMW is genuinely atomic.
_config_tx_lock = threading.RLock()
_in_tx = threading.local()  # .active set True for the duration of a config_transaction
_config_write_gate_lock = threading.Lock()
_config_writes_suspended_reason = ""

# periodic backup trigger. Writes a dated snapshot every
# _BACKUP_EVERY_N_SAVES save_config() calls so recovery windows are
# hourly (typical users save ~1-10 times per hour in normal use)
# rather than only-at-startup. Counter resets on process restart;
# backup_config_on_start still fires the first one at launch.
_save_counter = 0
_BACKUP_EVERY_N_SAVES = 20
_CFG_CACHE: dict[str, Any] = {"sig": None, "data": None}


def _config_file_sig() -> tuple[str, int, int] | None:
    try:
        st = CONFIG_FILE.stat()
        return (str(CONFIG_FILE), int(st.st_mtime_ns), int(st.st_size))
    except OSError:
        return None


def _cache_config(sig: tuple[str, int, int] | None,
                  cfg: dict[str, Any] | None) -> None:
    if sig is None or cfg is None:
        _CFG_CACHE["sig"] = None
        _CFG_CACHE["data"] = None
        return
    _CFG_CACHE["sig"] = sig
    _CFG_CACHE["data"] = copy.deepcopy(cfg)


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


def _migrate_pending_tx_ids(cfg: dict[str, Any]) -> None:
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
            # previous migration unconditionally zeroed
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

    uses config_transaction for atomic RMW.

    Logs persistence errors but never raises into the sync pipeline; the
    counter is user-visible but not load-bearing."""
    if not channel_name or not video_id:
        return
    try:
        if not config_is_writable():
            return
        with config_transaction() as cfg:
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
                break
    except Exception as e:
        _log.warning("append_pending_tx_id save failed for %r/%r: %s",
                     channel_name, video_id, e)


def remove_pending_tx_id(video_id: str) -> bool:
    """Drop a completed transcription's video ID from whichever
    channel's pending list it's in. Called from the transcribe
    worker's completion path.

    uses config_transaction for atomic RMW.

    Returns True if any list actually changed (useful for telemetry).
    Logs persistence errors but never raises.
    """
    if not video_id:
        return False
    try:
        if not config_is_writable():
            return False
        changed = False
        with config_transaction() as cfg:
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
        return changed
    except Exception as e:
        _log.warning("remove_pending_tx_id save failed for %r: %s",
                     video_id, e)
        return False


def load_config() -> dict[str, Any]:
    """Load the real YTArchiver config. Falls back to defaults if missing.

    Recovery path: if the primary file is corrupt, try the most recent
    dated snapshot in `backups/` before giving up and returning defaults.
    """
    # Reading, normalization, migration/recovery and cache publication belong
    # to the same generation. Releasing this lock after json.load let an older
    # reader overwrite a newer save's cache using the newer file signature.
    # The lock is reentrant because migration and transactions use save_config.
    with _config_lock:
        return _load_config_locked()


def _load_config_locked() -> dict[str, Any]:
    """Read and publish one generation; caller holds ``_config_lock``."""
    sig = _config_file_sig()
    if sig is None:
        return copy.deepcopy(DEFAULT_CONFIG)
    try:
        with _config_lock:
            if (_CFG_CACHE["sig"] == sig
                    and isinstance(_CFG_CACHE["data"], dict)):
                return copy.deepcopy(_CFG_CACHE["data"])
            with CONFIG_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)
        merged = copy.deepcopy(DEFAULT_CONFIG)
        merged.update(data)
        # Existing installs predate the Browse-first preference. Preserve
        # their established layout by enabling Dense Subs only when an
        # on-disk config exists but has never stored this key. New installs
        # take DEFAULT_CONFIG's False value, and explicit choices are kept.
        _needs_dense_subs_default = "legacy_subs_tab" not in data
        # The old launch-only checker used 0 days as its Off switch. Preserve
        # that explicit choice while migrating positive intervals to the new
        # automatic long-running monitor.
        _needs_ytdlp_update_mode = "ytdlp_update_mode" not in data
        # Trash used to require an explicit manual purge. Existing installs
        # must not begin permanently deleting already-present entries merely
        # because a new default appeared during an upgrade.
        _needs_trash_retention_policy = "trash_retention_days" not in data
        _trash_upgrade_grace_until = (
            time.time() + TRASH_RETENTION_UPGRADE_GRACE_SECONDS
            if _needs_trash_retention_policy else 0.0
        )

        # Run migrations exactly once per config, then stamp a flag/key so
        # subsequent load_config calls skip the work. Previously
        # _migrate_pending_tx_ids ran on every load — idempotent but
        # wasteful, and any future migration accidentally breaking
        # idempotency would silently corrupt state.
        if (not merged.get("_migration_v2_pending_tx_ids")
                or _needs_dense_subs_default
                or _needs_ytdlp_update_mode
                or _needs_trash_retention_policy):
            # Run the migration on a DEEP COPY first. If save_config
            # fails (antivirus lock, OneDrive sync, disk full), the
            # in-memory `merged` we return must NOT carry the migrated
            # values — otherwise the caller keeps using the wiped
            # state for the rest of the session, and any later
            # save_config call from a different path commits the wipe
            # WITHOUT the migration flag, causing the migration to
            # re-run next launch and re-wipe. The migration's
            # destructive zero-out of transcription_pending and flip of
            # transcription_complete makes this a real data-loss path.
            _candidate = copy.deepcopy(merged)
            if not merged.get("_migration_v2_pending_tx_ids"):
                _migrate_pending_tx_ids(_candidate)
                _candidate["_migration_v2_pending_tx_ids"] = True
            if _needs_dense_subs_default:
                _candidate["legacy_subs_tab"] = True
            if _needs_ytdlp_update_mode:
                try:
                    _old_ytdlp_days = int(
                        data.get("ytdlp_update_check_days", 1) or 0)
                except (TypeError, ValueError):
                    _old_ytdlp_days = 1
                _candidate["ytdlp_update_mode"] = (
                    "off" if _old_ytdlp_days == 0 else "automatic")
            if _needs_trash_retention_policy:
                _candidate["trash_retention_days"] = (
                    TRASH_RETENTION_DEFAULT_DAYS)
                _candidate["trash_retention_grace_until_ts"] = max(
                    _safe_retention_grace(_candidate.get(
                        "trash_retention_grace_until_ts", 0.0)),
                    _trash_upgrade_grace_until,
                )
            if getattr(_in_tx, 'active', False):
                # Inside a config_transaction: adopt migrated state now;
                # the outer transaction's exit-save will persist it.
                merged = _candidate
            else:
                try:
                    if save_config(_candidate):
                        # Only adopt the migrated state into `merged` after
                        # the save lands on disk. Now in-memory and on-disk
                        # agree, so subsequent saves can't silently lose
                        # the migration flag.
                        merged = _candidate
                    else:
                        _log.warning(
                            "migration save failed; will retry next launch")
                except Exception as _me:
                    _log.error("migration save exception: %s", _me)
            if _needs_trash_retention_policy:
                # Even when the migration write fails, retain just these two
                # non-destructive values in the session snapshot. Returning
                # DEFAULT_CONFIG's active 30-day policy with a zero grace
                # would make a transient disk-full/antivirus error capable of
                # enabling immediate permanent deletion. A later successful
                # config save will persist this safe pair together.
                merged["trash_retention_days"] = (
                    TRASH_RETENTION_DEFAULT_DAYS)
                merged["trash_retention_grace_until_ts"] = max(
                    _safe_retention_grace(merged.get(
                        "trash_retention_grace_until_ts", 0.0)),
                    _trash_upgrade_grace_until,
                )
        with _config_lock:
            # A successful migration atomically replaced CONFIG_FILE, so the
            # signature captured before migration is stale. Cache against the
            # current file signature or the next load needlessly re-reads and
            # can re-enter migration bookkeeping.
            _cache_config(_config_file_sig(), merged)
        return copy.deepcopy(merged)
    except (json.JSONDecodeError, OSError) as e:
        _log.warning("failed to load %s: %s", CONFIG_FILE, e)
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
                        _log.warning("recovered from snapshot: %s", snap.name)
                        merged = copy.deepcopy(DEFAULT_CONFIG)
                        merged.update(data)
                        # A recovered snapshot is still an existing install.
                        # Apply the same compatibility default while retaining
                        # any explicit True/False value already stored.
                        if "legacy_subs_tab" not in data:
                            merged["legacy_subs_tab"] = True
                        if "ytdlp_update_mode" not in data:
                            try:
                                _old_ytdlp_days = int(
                                    data.get("ytdlp_update_check_days", 1) or 0)
                            except (TypeError, ValueError):
                                _old_ytdlp_days = 1
                            merged["ytdlp_update_mode"] = (
                                "off" if _old_ytdlp_days == 0
                                else "automatic")
                        if "trash_retention_days" not in data:
                            merged["trash_retention_days"] = (
                                TRASH_RETENTION_DEFAULT_DAYS)
                            merged["trash_retention_grace_until_ts"] = max(
                                _safe_retention_grace(merged.get(
                                    "trash_retention_grace_until_ts",
                                    0.0)),
                                time.time()
                                + TRASH_RETENTION_UPGRADE_GRACE_SECONDS,
                            )
                        # Sideline the corrupt file so the next launch uses the snapshot
                        try:
                            # Use a unique timestamp suffix so
                            # repeated corruption events preserve
                            # forensic evidence instead of overwriting
                            # the single .json.corrupt slot every
                            # time.
                            _ts = int(time.time())
                            _corrupt_path = CONFIG_FILE.with_suffix(
                                f".json.corrupt.{_ts}")
                            CONFIG_FILE.rename(_corrupt_path)
                        except OSError:
                            pass
                        # Persist the recovered snapshot back to
                        # CONFIG_FILE immediately. After the sideline
                        # rename above CONFIG_FILE no longer exists, so
                        # without this every later load — same session
                        # or next launch — short-circuits to factory
                        # defaults at the top of this function, and the
                        # first save_config from any caller would
                        # permanently commit the wiped state.
                        # Skip the extra save if we're already inside a
                        # config_transaction — its exit-save handles it,
                        # avoiding a double-write mid-transaction.
                        if not getattr(_in_tx, 'active', False):
                            try:
                                if save_config(merged):
                                    _log.warning(
                                        "recovered config persisted back "
                                        "to %s", CONFIG_FILE.name)
                                else:
                                    _log.error(
                                        "recovered config could NOT be "
                                        "persisted — restore %s manually "
                                        "from backups/ before changing any "
                                        "settings", CONFIG_FILE)
                            except Exception as _pe:
                                _log.error(
                                    "recovered-config save failed: %s", _pe)
                        return merged
                    except (json.JSONDecodeError, OSError):
                        continue
        except Exception as _r:
            _log.error("recovery attempt failed: %s", _r)
        return copy.deepcopy(DEFAULT_CONFIG)


# atomic read-modify-write context manager. Use
# instead of the legacy `cfg = load_config(); cfg[...] = ...; save_config(cfg)`
# pattern when you need the read and write to be linked.
# Usage:
#     from backend.ytarchiver_config import config_transaction
#     with config_transaction() as cfg:
#         cfg["channels"][0]["last_sync"] = "2026-05-17"
#     # Auto-saved on context exit. If an exception escapes the block,
#     # the save is SKIPPED so a half-mutated cfg doesn't get persisted.
import contextlib as _ctxlib  # noqa: E402 (intentional late import)


class ConfigUnchanged(Exception):
    """Signal that a config transaction intentionally made no changes."""


@_ctxlib.contextmanager
def config_transaction():
    """Atomic load-modify-save with a single lock acquisition.

    The yielded dict is the live config — mutate it in place. On normal
    exit, save_config is called. On exception inside the block, the
    save is skipped (best-effort: the on-disk file is unchanged).

    The underlying _config_tx_lock is reentrant (RLock) so nested
    transactions don't deadlock. _in_tx.active is set for the duration so
    load_config's internal migration/recovery saves are suppressed — the
    outer transaction's exit-save handles persistence instead, preventing
    a double-write that could snapshot an intermediate state.

    Holds _config_lock for the WHOLE block (load + mutate + save) too, so a
    plain save_config() from another thread is excluded for the transaction's
    duration. Without it, the transaction (which took _config_tx_lock) and a
    bare save_config (which takes _config_lock) raced and lost updates (audit
    r2). Both are RLocks acquired in a consistent order, and load_config/
    save_config re-acquire _config_lock reentrantly. Keep transaction blocks
    short — they run while holding _config_lock.
    """
    _was_in_tx = getattr(_in_tx, 'active', False)
    _in_tx.active = True
    try:
        with _config_tx_lock, _config_lock:
            cfg = load_config()
            try:
                yield cfg
            except Exception:
                # Don't persist partial mutations — re-raise to caller.
                raise
            else:
                if _was_in_tx:
                    # Nested call: outer transaction's exit-save handles it.
                    return
                # Raise on save failure so the caller can react. Previously
                # a failed save (disk full, antivirus lock) was swallowed
                # — the caller's transaction succeeded silently in memory
                # but never landed on disk, and a later load would return
                # the stale state with no signal that the mutation was
                # lost.
                if not save_config(cfg):
                    raise OSError(
                        "config_transaction: save_config returned False — "
                        "mutation not persisted; check log for details")
    finally:
        _in_tx.active = _was_in_tx


@_ctxlib.contextmanager
def locked_config_snapshot():
    """Yield one config snapshot while excluding concurrent app saves.

    This is intentionally read-only.  It lets a destructive operation make
    its final policy decision and commit its filesystem boundary while the
    Settings writer is unable to change that policy between those two steps.
    """
    with _config_lock:
        yield load_config()


def update_config(mutator):
    """Run one focused config mutation and return its result + snapshot.

    Feature code should use this instead of loading and later saving a whole
    document. Disjoint updates then serialize under the same transaction and
    cannot erase one another with stale snapshots.
    """
    if not callable(mutator):
        raise TypeError("config mutator must be callable")
    result = None
    snapshot = None
    with config_transaction() as cfg:
        result = mutator(cfg)
        snapshot = copy.deepcopy(cfg)
    return result, snapshot


def config_file_exists() -> bool:
    return CONFIG_FILE.exists()


def suspend_config_writes(reason: str = "application state replacement") -> bool:
    """Permanently freeze this process's stale config writers.

    Backup restore swaps the live config underneath the running process.  The
    old process must therefore become read-only before that swap; otherwise a
    late window-state/settings callback can overwrite the restored document.
    A restart creates a fresh process and naturally reopens the gate.
    """
    global _config_writes_suspended_reason
    with _config_write_gate_lock:
        changed = not bool(_config_writes_suspended_reason)
        _config_writes_suspended_reason = str(
            reason or "application state replacement")
        return changed


def config_writes_suspended_reason() -> str:
    with _config_write_gate_lock:
        return _config_writes_suspended_reason


def config_is_writable() -> bool:
    """Return false once restore freezes this process's stale writers."""
    with _config_write_gate_lock:
        return not bool(_config_writes_suspended_reason)


def backup_config_on_start(keep: int = 10) -> str | None:
    """Copy the current config.json to a dated snapshot in
    %APPDATA%\\YTArchiver\\backups\\config_YYYY-MM-DD_HHMMSS.json.

    Keeps only the most recent `keep` snapshots. Non-fatal on any error.
    Returns the path written, or None on skip/failure.
    """
    import datetime as _dt
    import shutil
    try:
        if not CONFIG_FILE.exists():
            return None
        backup_dir = APP_DATA_DIR / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        ts = _dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        dst = backup_dir / f"config_{ts}.json"
        shutil.copy2(str(CONFIG_FILE), str(dst))
        # Prune to `keep` most-recent. Sort by mtime DESC, then by
        # filename DESC as a tiebreaker — the filename has a
        # second-resolution timestamp, so when two snapshots share an
        # mtime (shutil.copy2 preserves it) the lexicographic order on
        # the dated filename gives a deterministic newest-first
        # ordering (audit: ytarchiver_config.py:393-418).
        snaps = sorted(backup_dir.glob("config_*.json"),
                       key=lambda p: (p.stat().st_mtime, p.name),
                       reverse=True)
        for old in snaps[keep:]:
            try: old.unlink()
            except OSError: pass
        return str(dst)
    except OSError:
        return None


def save_config(cfg: dict[str, Any]) -> bool:
    """Save config back to disk unless restore has frozen this process.

    The config lock is acquired before the write-gate lock everywhere.  The
    gate stays held through the atomic replacement, so
    ``suspend_config_writes`` cannot return while an already-admitted stale
    writer is still able to replace the restored config.
    """
    with _config_lock, _config_write_gate_lock:
        if _config_writes_suspended_reason:
            _log.warning("write blocked")
            return False
        return _save_config_locked(cfg)


def _save_config_locked(cfg: dict[str, Any]) -> bool:
    """Write one config snapshot while the lock and write gate are held.

    fsyncs the temp file before os.replace so a power loss
    or BSOD between write and rename can't commit a zero-byte /
    truncated file over the real one. Also cheap (<10ms per save for
    a typical config).

    triggers a dated snapshot every _BACKUP_EVERY_N_SAVES
    saves so the recovery chain is minutes/hours old rather than
    hours/days. backup_config_on_start still handles the at-launch
    snapshot.
    """
    global _save_counter
    try:
        should_backup = False
        APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
        # Trim autorun_history on
        # save so the config file can't grow unbounded across years.
        # UI shows the last 10,000 entries; on-disk cap matches so
        # nothing is silently dropped. At ~150 entries/day on a
        # ~100-channel / 2-hour-interval workload that's a couple
        # months of scroll history. JSON entry is ~250 bytes →
        # ~2.5 MB worst-case in config.json. Trimming in-place on the
        # passed dict is fine — the UI uses a fresh read per render.
        # Snapshot a serialization-only view of cfg so the autorun_history
        # trim doesn't mutate the caller's dict. The previous in-place
        # trim caused callers inside config_transaction to see a
        # silently-shrunken list immediately after save — calls like
        # cfg["autorun_history"].append(...) post-save would land on
        # the trimmed copy and produce inconsistent in-memory snapshots.
        cfg_for_write = cfg
        try:
            _hist = cfg.get("autorun_history")
            if isinstance(_hist, list) and len(_hist) > 10000:
                cfg_for_write = dict(cfg)
                cfg_for_write["autorun_history"] = _hist[-10000:]
        except Exception as e:
            swallow("autorun-history trim", e)
        with _config_lock:
            # Write-via-temp for atomicity (matches tkinter app's save_config)
            # Wrap both paths with _long_path so an OneDrive-redirected
            # APPDATA on a deeply-nested user profile (>240 chars) can
            # still write — previously OSError was silently logged and
            # the user's settings stopped persisting with no signal.
            tmp = CONFIG_FILE.with_suffix(".json.tmp")
            _tmp_path = _long_path(str(tmp))
            _cfg_path = _long_path(str(CONFIG_FILE))
            # Capture the pre-existing hidden attribute on Windows so
            # we can re-apply it after os.replace, which otherwise
            # exposes a previously-hidden config file in Explorer
            # (audit: ytarchiver_config H120). FILE_ATTRIBUTE_HIDDEN
            # is bit 0x02; getfileattributes returns -1 if missing.
            _was_hidden = False
            if os.name == "nt":
                try:
                    import ctypes as _ctypes
                    _attrs = _ctypes.windll.kernel32.GetFileAttributesW(
                        _cfg_path)
                    if _attrs != 0xFFFFFFFF and (_attrs & 0x02):
                        _was_hidden = True
                except Exception:
                    pass
            with open(_tmp_path, "w", encoding="utf-8") as f:
                json.dump(cfg_for_write, f, indent=2)
                # flush + fsync before closing so the
                # os.replace below commits a file whose contents are
                # physically on disk (not just in the OS write cache).
                try:
                    f.flush()
                    os.fsync(f.fileno())
                except OSError:
                    pass
            os.replace(_tmp_path, _cfg_path)
            if _was_hidden and os.name == "nt":
                try:
                    import ctypes as _ctypes
                    _ctypes.windll.kernel32.SetFileAttributesW(
                        _cfg_path, 0x02)
                except Exception:
                    pass
            # fsync the parent directory entry too so a power loss
            # immediately after replace doesn't leave the directory
            # pointing at a half-committed inode. Only POSIX supports
            # directory fsync; on Windows there's no direct equivalent,
            # and the dated backups path is the recovery story.
            if os.name != "nt":
                try:
                    _dfd = os.open(str(APP_DATA_DIR), os.O_RDONLY)
                    try:
                        os.fsync(_dfd)
                    finally:
                        os.close(_dfd)
                except OSError as e:
                    swallow("config fsync", e)
            _cache_config(_config_file_sig(), cfg_for_write)
            _save_counter += 1
            if _save_counter >= _BACKUP_EVERY_N_SAVES:
                _save_counter = 0
                should_backup = True
        # periodic snapshot. Runs outside the lock because
        # backup_config_on_start does its own I/O. Non-fatal on failure.
        if should_backup:
            try:
                backup_config_on_start(keep=20)
            except Exception as e:
                swallow("config backup-on-save", e)
        return True
    except OSError as e:
        _log.error("failed to save: %s", e)
        return False


# Compatibility exports. Persistence itself does not build UI payloads.
from .config_views import (  # noqa: E402
    _hist_tag_for_kind as _hist_tag_for_kind,
)
from .config_views import (
    _last_sync_epoch as _last_sync_epoch,
)
from .config_views import (
    autorun_history_entries_for_ui as autorun_history_entries_for_ui,
)
from .config_views import (
    channels_for_subs_ui as channels_for_subs_ui,
)
from .config_views import (
    recent_for_ui as recent_for_ui,
)
from .view_format import (  # noqa: E402
    _extract_video_id as _extract_video_id,
)
from .view_format import (
    _fmt_dur as _fmt_dur,
)
from .view_format import (
    _fmt_size as _fmt_size,
)
from .view_format import (
    _fmt_time_ago as _fmt_time_ago,
)
