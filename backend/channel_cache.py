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
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .ytarchiver_config import CHANNEL_ID_CACHE, config_is_writable


_lock = threading.Lock()
_cache: Dict[str, dict] = {}
_loaded = False

STALE_AFTER_SEC = 6 * 3600 # 6 hours


def _load_locked():
    global _loaded, _cache
    if _loaded:
        return
    _loaded = True
    if not CHANNEL_ID_CACHE.exists():
        return
    try:
        with CHANNEL_ID_CACHE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _cache = data
    except (OSError, json.JSONDecodeError):
        pass


def _save_locked():
    if not config_is_writable():
        return
    try:
        CHANNEL_ID_CACHE.parent.mkdir(parents=True, exist_ok=True)
        tmp = str(CHANNEL_ID_CACHE) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_cache, f)
        import os as _os
        _os.replace(tmp, CHANNEL_ID_CACHE)
    except OSError:
        pass


def get_cached_ids(url: str) -> Optional[List[str]]:
    """Return the cached ID list if fresh, else None."""
    with _lock:
        _load_locked()
        rec = _cache.get(url)
        if not rec:
            return None
        age = time.time() - float(rec.get("last_refreshed", 0))
        if age > STALE_AFTER_SEC:
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

    audit D-53: DON'T touch last_refreshed here. Only
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
        # Preserve existing last_refreshed (or default to 0 if brand-new).
        # Only `set_cached_ids` / `refresh_cache_via_ytdlp` should bump it.
        rec = _cache.setdefault(url, {"last_refreshed": 0.0, "ids": []})
        # Prepend newly-downloaded IDs (they're latest)
        merged = list(new) + [i for i in rec.get("ids", []) if i not in set(new)]
        rec["ids"] = merged
        # NOTE: intentionally not updating rec["last_refreshed"] here.
        _save_locked()


def clear(url: Optional[str] = None):
    with _lock:
        _load_locked()
        if url is None:
            _cache.clear()
        else:
            _cache.pop(url, None)
        _save_locked()


def counts() -> Dict[str, int]:
    with _lock:
        _load_locked()
        return {u: len(v.get("ids", [])) for u, v in _cache.items()}
