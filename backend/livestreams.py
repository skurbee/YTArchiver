"""
Livestream detection + deferred watchlist.

Matches YTArchiver's "upcoming" handling: when yt-dlp reports a video
is a live stream or scheduled premiere, skip it for now but remember
it so the next sync can pick it up once it's finished streaming.

Journal lives at `%APPDATA%\\YTArchiver\\ytarchiver_livestream_defer.json`.
Format: list of dicts { video_id, title, channel_url, first_seen_ts }.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, List, Optional

from .ytarchiver_config import APP_DATA_DIR


_JOURNAL = APP_DATA_DIR / "ytarchiver_livestream_defer.json"
_IGNORE_JOURNAL = APP_DATA_DIR / "ytarchiver_livestream_ignore.json"
_DRAWER_STATE = APP_DATA_DIR / "ytarchiver_livestream_drawer.json"
_lock = threading.Lock()


def _load() -> List[Dict[str, Any]]:
    if not _JOURNAL.exists():
        return []
    try:
        with _JOURNAL.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (OSError, json.JSONDecodeError):
        return []


def _save(items: List[Dict[str, Any]]) -> bool:
    try:
        APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = str(_JOURNAL) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(items, f, indent=2)
        import os as _os
        _os.replace(tmp, _JOURNAL)
        return True
    except OSError:
        return False


def _load_ignore() -> set:
    """Load the permanent-ignore set. These video IDs are never again
    offered to the user as deferred livestreams — useful for premieres
    that were cancelled, or streams the user simply doesn't want.
    """
    if not _IGNORE_JOURNAL.exists():
        return set()
    try:
        with _IGNORE_JOURNAL.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(x for x in data if isinstance(x, str))
    except (OSError, json.JSONDecodeError):
        pass
    return set()


def _save_ignore(ids: set) -> bool:
    try:
        APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = str(_IGNORE_JOURNAL) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(sorted(ids), f, indent=2)
        import os as _os
        _os.replace(tmp, _IGNORE_JOURNAL)
        return True
    except OSError:
        return False


def ignore(video_id: str) -> bool:
    """Permanently skip this video. Adds it to the ignore set AND
    removes it from the deferred list if present. Future sync passes
    that encounter this ID via line_looks_live will NOT re-defer it
    because defer() checks the ignore set first.
    """
    global _ignore_cache, _ignore_cache_loaded
    if not video_id:
        return False
    with _lock:
        ids = _load_ignore()
        ids.add(video_id)
        _save_ignore(ids)
        # Invalidate the cache — next is_ignored() re-reads.
        _ignore_cache = ids
        _ignore_cache_loaded = True
        # Also drop from deferred so it disappears from the drawer.
        items = _load()
        new = [it for it in items if it.get("video_id") != video_id]
        if len(new) != len(items):
            _save(new)
    return True


_ignore_cache: Optional[set] = None
_ignore_cache_loaded = False


def is_ignored(video_id: str) -> bool:
    """audit F-54: in-memory cache of the ignore set. Old code read
    the file from disk on EVERY call, which was called in tight loops
    during sync. Now the set is lazy-loaded once and invalidated by
    `ignore()` on write. Wrapped in the module lock so
    concurrent-write races are defined (mark_seen-style duplicate
    lines are no longer possible)."""
    global _ignore_cache, _ignore_cache_loaded
    if not video_id:
        return False
    with _lock:
        if not _ignore_cache_loaded:
            _ignore_cache = _load_ignore()
            _ignore_cache_loaded = True
        return video_id in (_ignore_cache or set())


def defer(video_id: str, title: str = "", channel_url: str = "") -> bool:
    """Add a livestream/premiere to the deferred list (dedup'd by video_id).
    Silently no-ops for video_ids in the permanent-ignore set.
    """
    if not video_id:
        return False
    if is_ignored(video_id):
        return False
    with _lock:
        items = _load()
        if any(it.get("video_id") == video_id for it in items):
            return False
        items.append({
            "video_id": video_id,
            "title": title or "",
            "channel_url": channel_url or "",
            "first_seen_ts": time.time(),
        })
        return _save(items)


def snooze_drawer(seconds: float) -> bool:
    """Hide the deferred-livestreams drawer from the UI for `seconds`
    from now. UI reads `drawer_state()` to check whether to display.
    Used by the "Retry in 24hrs / 1 week" dropdown to suppress the
    drawer's nagging for a user-chosen interval.
    """
    try:
        APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
        until = time.time() + max(0.0, float(seconds or 0))
        tmp = str(_DRAWER_STATE) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"snooze_until_ts": until}, f)
        import os as _os
        _os.replace(tmp, _DRAWER_STATE)
        return True
    except OSError:
        return False


def drawer_state() -> Dict[str, Any]:
    """Return {snooze_until_ts, now_ts, visible} for the UI to decide
    whether to render the drawer. `visible=False` means a snooze is
    active and the drawer should stay hidden regardless of how many
    items are in the journal.
    """
    now = time.time()
    until = 0.0
    if _DRAWER_STATE.exists():
        try:
            with _DRAWER_STATE.open("r", encoding="utf-8") as f:
                data = json.load(f)
            until = float(data.get("snooze_until_ts") or 0)
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            until = 0.0
    return {
        "snooze_until_ts": until,
        "now_ts": now,
        "visible": until <= now,
    }


def list_deferred() -> List[Dict[str, Any]]:
    with _lock:
        return _load()


def drop(video_id: str) -> bool:
    """Remove a deferred entry (call once sync succeeds)."""
    if not video_id:
        return False
    with _lock:
        items = _load()
        new = [it for it in items if it.get("video_id") != video_id]
        if len(new) == len(items):
            return False
        return _save(new)


def count() -> int:
    return len(list_deferred())


# ── Livestream-line detector for yt-dlp stdout ─────────────────────────

# Patterns yt-dlp uses when a video is live / scheduled / premiere.
# audit E-41: stricter phrases so ordinary upload titles containing
# "is live" (e.g. "Tom's wedding is live now!") don't silently defer.
# yt-dlp's actual live-detection messages are full-sentence, not
# fragment matches — this list uses the longer authoritative phrases
# yt-dlp emits. Fragmentary matches could still legitimately apply
# to some yt-dlp variants; rely on the surrounding context (error
# prefix) via the caller's other filters to disambiguate.
_LIVE_MARKERS = (
    "this video is live",
    "this live stream",
    "is currently live",
    "premieres in",
    "scheduled live",
    "live event starts in",
    "will begin at",
    "scheduled to start",
    "this live event",
    "waiting for stream",
)


def line_looks_live(line: str) -> bool:
    """Cheap heuristic: does this yt-dlp stdout line indicate a live/scheduled stream?"""
    low = (line or "").lower()
    return any(m in low for m in _LIVE_MARKERS)
