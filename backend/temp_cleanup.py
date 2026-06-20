"""
Startup temp-file cleanup — YTArchiver.py:2099-2190 port.

Walks every subscribed channel folder on launch and removes files left behind
by cancelled yt-dlp / ffmpeg runs: `.part`, `.temp`, `.ytdl`, `.fNNN.ext`
fragments, and orphaned `_TEMP_COMPRESS` / `_BACKLOG_TEMP` working folders.

Also callable post-sync / post-redownload as a defensive sweep.
"""

from __future__ import annotations

import os
import shutil
import time

from .fs_search import is_partial_artifact
from .log import get_logger
from .log_stream import LogStreamer
from .sync import channel_folder_name as _cfn
from .ytarchiver_config import load_config

_log = get_logger(__name__)


_STALE_TEMP_DIRS = ("_TEMP_COMPRESS", "_BACKLOG_TEMP")

# minimum age before a temp dir is considered "stale".
# Without this, startup_cleanup_temps (called on app launch) or a second
# instance launching during a long compress pass would nuke an in-flight
# _TEMP_COMPRESS mid-write, silently failing the encode. Compress now
# writes a .lock sidecar inside _TEMP_COMPRESS at encode start and
# removes it on completion — presence of .lock means "active", skip
# regardless of age.
_MIN_TEMP_AGE_SEC = 30 * 60  # 30 minutes
_RECENT_WRITE_STAT_LIMIT = 200


def _dir_is_active(full: str) -> bool:
    """True if the directory contains a fresh .lock sidecar (active
    encode). Also returns True if the directory itself is newer than
    _MIN_TEMP_AGE_SEC — protects against the race where a new encode
    just started and hasn't written its .lock yet.

    Stale .lock files (older than 24 hours) are IGNORED — they're
    almost certainly orphans from a crashed compress whose __del__-
    based cleanup never fired. Without this, GB-sized encode dirs
    would sit in _TEMP_COMPRESS forever, the .lock file blocking
    every future cleanup pass.
    """
    _STALE_LOCK_AGE_SEC = 24 * 3600
    try:
        _lock = os.path.join(full, ".lock")
        if os.path.exists(_lock):
            try:
                _lock_age = time.time() - os.path.getmtime(_lock)
            except OSError:
                _lock_age = 0
            if _lock_age < _STALE_LOCK_AGE_SEC:
                return True
            # Stale lock — fall through to age check + cleanup.
        age = time.time() - os.path.getmtime(full)
        if age < _MIN_TEMP_AGE_SEC:
            return True
        # Also walk the contents for a recent file write — Windows
        # doesn't always bump the directory mtime when files inside
        # are appended-to (yt-dlp progressive writes), so a long
        # active download to an old-looking dir could otherwise be
        # rmtree'd out from under itself (audit: temp_cleanup H110).
        try:
            _RECENT_WRITE_SEC = 60
            _now = time.time()
            _checked = 0
            for _root, _dns, _fns in os.walk(full):
                for _fn in _fns:
                    _checked += 1
                    if _checked > _RECENT_WRITE_STAT_LIMIT:
                        return False
                    try:
                        if (_now - os.path.getmtime(
                                os.path.join(_root, _fn))) < _RECENT_WRITE_SEC:
                            return True
                    except OSError:
                        continue
        except OSError:
            return True
    except OSError:
        # If we can't stat it, err on the side of NOT deleting.
        return True
    return False


# Consolidated into fs_search.is_partial_artifact (identical logic).
# Kept as a thin alias so anything that imports `is_partial_file` from
# this module continues to work.
is_partial_file = is_partial_artifact


def cleanup_folder(folder: str) -> int:
    """Recursively remove partial files under `folder`. Returns count."""
    if not folder or not os.path.isdir(folder):
        return 0
    cleaned = 0
    failed: list[str] = []
    for dp, dns, fns in os.walk(folder):
        # Drop any stale temp working dirs (skip ones still in active use)
        drop = [d for d in dns if d in _STALE_TEMP_DIRS]
        for d in drop:
            full = os.path.join(dp, d)
            if _dir_is_active(full):
                # Don't recurse into an active temp dir either — its
                # partial files belong to the running encode.
                dns.remove(d)
                continue
            try:
                shutil.rmtree(full, ignore_errors=True)
                cleaned += 1
            except Exception as e:
                _log.debug("swallowed: %s", e)
            dns.remove(d)
        for f in fns:
            if is_partial_file(f):
                fp = os.path.join(dp, f)
                # age-gate partial-file removal so an
                # in-flight compress / yt-dlp download isn't deleted
                # out from under itself. Windows file locks usually
                # protect this, but on a network-fast share or a
                # release-after-write window the race is real.
                try:
                    age = time.time() - os.path.getmtime(fp)
                    if age < _MIN_TEMP_AGE_SEC:
                        continue
                except OSError:
                    continue
                try:
                    os.remove(fp)
                    cleaned += 1
                except OSError:
                    failed.append(fp)
    # Retry locked files once after a short delay (Windows)
    if failed:
        time.sleep(1.0)
        for fp in failed:
            try:
                os.remove(fp)
                cleaned += 1
            except OSError:
                pass
    return cleaned


def startup_cleanup_temps(stream: LogStreamer) -> int:
    """Walk every subscribed channel folder and nuke partial / temp files.

    Invoked on launch so a fresh session starts with a clean archive tree.
    """
    try:
        cfg = load_config()
    except Exception:
        return 0
    base = (cfg.get("output_dir") or "").strip()
    if not base or not os.path.isdir(base):
        return 0
    channels = cfg.get("channels", []) or []
    total = 0
    for ch in channels:
        ch_folder = os.path.join(base, _cfn(ch))
        total += cleanup_folder(ch_folder)
    if total and stream is not None:
        stream.emit([["[Startup] ", "sync_bracket"],
                     [f"\U0001f9f9 Cleaned {total} leftover temp file(s).\n",
                      "dim"]])
        stream.flush()
    return total
