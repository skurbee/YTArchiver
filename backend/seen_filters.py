"""
Seen-filters — persistent list of video titles we've already shown the
user as "[Skip] — filtered" so we don't keep re-logging the same shorts
every sync pass.

Matches YTArchiver.py's SEEN_FILTER_TITLES_FILE at line 102.
"""

from __future__ import annotations

import threading
from typing import Iterable, Set

from .ytarchiver_config import SEEN_FILTER_TITLES, config_is_writable


_lock = threading.Lock()
_cache: Set[str] = set()
_loaded: bool = False


def _load_locked():
    global _loaded, _cache
    if _loaded:
        return
    _loaded = True
    if not SEEN_FILTER_TITLES.exists():
        return
    try:
        with SEEN_FILTER_TITLES.open("r", encoding="utf-8") as f:
            for line in f:
                ln = line.strip()
                if ln:
                    _cache.add(ln)
    except OSError:
        pass


def is_seen(title: str) -> bool:
    """Return True if we've logged this title's filter-skip before."""
    if not title:
        return False
    with _lock:
        _load_locked()
        return title.strip() in _cache


def mark_seen(title: str) -> bool:
    """Add a title to the seen list. Appends to disk if writable.
    Returns True if the title was new."""
    if not title:
        return False
    t = title.strip()
    with _lock:
        _load_locked()
        if t in _cache:
            return False
        _cache.add(t)
    if config_is_writable():
        try:
            SEEN_FILTER_TITLES.parent.mkdir(parents=True, exist_ok=True)
            with SEEN_FILTER_TITLES.open("a", encoding="utf-8") as f:
                f.write(t + "\n")
        except OSError:
            pass
    return True


def clear():
    """Nuke the cache + file."""
    with _lock:
        _cache.clear()
    if config_is_writable():
        try:
            if SEEN_FILTER_TITLES.exists():
                SEEN_FILTER_TITLES.unlink()
        except OSError:
            pass


def count() -> int:
    with _lock:
        _load_locked()
        return len(_cache)
