"""
Per-channel video ID cache — shaves the slow `yt-dlp --flat-playlist` walk
off every sync of big channels.

Schema in `ytarchiver_channel_ids.json`:
    {
      "<channel_url>": {
        "last_refreshed": 1745000000.0,
        "ids": ["abc_VIDEOID1", "abc_VIDEOID2", ...]
      },
      ...
    }

Strategy:
  - Sync asks `get_cached_ids(url)` first. If cache is fresh (< 6 h) we
    skip the playlist walk entirely and just use `--break-on-existing`
    on the full channel URL.
  - When sync completes, we append the newly-downloaded IDs to the
    cache so subsequent syncs stay fast.
  - Full refresh (`refresh_cache_via_ytdlp`) rebuilds the cache for one
    channel by running `yt-dlp --flat-playlist --print id` in the
    background.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterable

from .utils import atomic_write, load_json_safe
from .ytarchiver_config import CHANNEL_ID_CACHE, config_is_writable

_lock = threading.Lock()
_cache: dict[str, dict] = {}
_loaded = False

STALE_AFTER_SEC = 6 * 3600 # 6 hours


def _load_locked():
    global _loaded, _cache
    if _loaded:
        return
    _loaded = True
    data = load_json_safe(CHANNEL_ID_CACHE)
    if isinstance(data, dict):
        _cache = data


def _save_locked() -> bool:
    """Persist `_cache` atomically. Returns True on success, False on
    failure (write-gate disabled or OSError). Patch A: return value is
    now used by append_ids() to revert in-memory state if the write
    fails, so a crash-loop doesn't grow the in-memory cache faster than
    the on-disk copy."""
    if not config_is_writable():
        return False
    try:
        CHANNEL_ID_CACHE.parent.mkdir(parents=True, exist_ok=True)
        with atomic_write(CHANNEL_ID_CACHE) as f:
            json.dump(_cache, f)
        return True
    except OSError:
        return False


def get_cached_ids(url: str) -> list[str] | None:
    """Return the cached ID list if fresh, else None."""
    with _lock:
        _load_locked()
        rec = _cache.get(url)
        if not rec:
            return None
        age = time.time() - float(rec.get("last_refreshed", 0))
        # Negative age = system clock went backwards (NTP, VM time
        # warp). Treat as stale so a clock skew doesn't make the
        # cache appear fresh forever (audit: channel_cache L46).
        if age < 0 or age > STALE_AFTER_SEC:
            return None
        ids = rec.get("ids", [])
        return list(ids) if isinstance(ids, list) else None


def set_cached_ids(url: str, ids: Iterable[str]):
    with _lock:
        _load_locked()
        _cache[url] = {
            "last_refreshed": time.time(),
            "ids": list(ids),
        }
        _save_locked()


def append_ids(url: str, new_ids: Iterable[str]):
    """Merge new IDs into the cache (front of list).

    DON'T touch last_refreshed here. Only
    `set_cached_ids` / `refresh_cache_via_ytdlp` should reset the
    timestamp, because those are the paths that actually walk the
    full channel. Updating last_refreshed on every 1-video append
    made a cache built 3 months ago look perpetually fresh, so sync
    skipped the full --flat-playlist walk forever — even though the
    cache was wildly incomplete by then.
    """
    new = [i for i in new_ids if i]
    if not new:
        return
    with _lock:
        _load_locked()
        # Patch A: write-first-then-commit pattern so a save failure
        # doesn't leave the in-memory cache ahead of disk. Build a new
        # rec, snapshot the original, swap in the new one, call save.
        # If save returns False, revert. Without this, a failed save
        # would still have the merged IDs in memory; next append re-
        # merges the same IDs (still correct via dedup, but the cache
        # mtime bumps for no reason and the disk/memory divergence is
        # confusing during diagnosis).
        original_rec = _cache.get(url)  # may be None
        if original_rec is None:
            rec = {"last_refreshed": 0.0, "ids": []}
        else:
            # Copy so we don't mutate original_rec in place.
            rec = {"last_refreshed": original_rec.get("last_refreshed", 0.0),
                   "ids": list(original_rec.get("ids", []))}
        # Prepend newly-downloaded IDs (they're latest)
        rec["ids"] = list(new) + [i for i in rec["ids"] if i not in set(new)]
        # NOTE: intentionally not updating rec["last_refreshed"] here.
        _cache[url] = rec
        if not _save_locked():
            # Revert to pre-merge state — disk is the source of truth.
            if original_rec is None:
                _cache.pop(url, None)
            else:
                _cache[url] = original_rec


def clear(url: str | None = None):
    with _lock:
        _load_locked()
        if url is None:
            _cache.clear()
        else:
            _cache.pop(url, None)
        _save_locked()


def counts() -> dict[str, int]:
    with _lock:
        _load_locked()
        return {u: len(v.get("ids", [])) for u, v in _cache.items()}
