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


def defer(video_id: str, title: str = "", channel_url: str = "") -> bool:
    """Add a livestream/premiere to the deferred list (dedup'd by video_id)."""
    if not video_id:
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
_LIVE_MARKERS = (
    "is live",
    "is currently live",
    "premieres in",
    "scheduled live",
    "starts in",
    "will begin at",
    "scheduled to start",
    "this live event",
)


def line_looks_live(line: str) -> bool:
    """Cheap heuristic: does this yt-dlp stdout line indicate a live/scheduled stream?"""
    low = (line or "").lower()
    return any(m in low for m in _LIVE_MARKERS)
