"""
Seen-filters — persistent list of video titles we've already shown the
user as "[Skip] — filtered" so we don't keep re-logging the same shorts
every sync pass.

Matches YTArchiver.py's SEEN_FILTER_TITLES_FILE at line 102.
"""

from __future__ import annotations

import threading

from .ytarchiver_config import SEEN_FILTER_TITLES, config_is_writable

_lock = threading.Lock()
_cache: set[str] = set()
_cache_lower: set[str] = set()  # parallel lowercased copy for O(1) case-insensitive lookup
_loaded: bool = False


def _load_locked():
    global _loaded, _cache, _cache_lower
    if _loaded:
        return
    _loaded = True
    if not SEEN_FILTER_TITLES.exists():
        return
    try:
        with SEEN_FILTER_TITLES.open("r", encoding="utf-8") as f:
            for line in f:
                ln = line.strip()
                # Skip obviously-corrupt lines (concatenated entries
                # left by an unlocked append race). Heuristic: any
                # single title >2KB is suspect — real YouTube titles
                # cap at ~100 chars. Without this, garbage from a
                # crash-during-append would pollute the cache and
                # never match real entries.
                if ln and len(ln) <= 2048:
                    _cache.add(ln)
                    _cache_lower.add(ln.lower())
    except OSError:
        pass


def is_seen(title: str) -> bool:
    """Return True if we've logged this title's filter-skip before."""
    if not title:
        return False
    with _lock:
        _load_locked()
        # case-insensitive match so channels that re-use
        # a title with different casing ("The Video" vs "the video")
        # don't emit duplicate [Skip] log lines for what's really
        # the same video.
        # Bug [19]: use the parallel lowercased set for O(1) lookup.
        # The previous {t.lower() for t in _cache} comprehension
        # rebuilt the entire set on every call (O(N) per check, GIL
        # held throughout) — meaningful CPU on a thousands-entry filter.
        return title.strip().lower() in _cache_lower


def mark_seen(title: str) -> bool:
    """Add a title to the seen list. Appends to disk if writable.
    Returns True if the title was new."""
    if not title:
        return False
    t = title.strip()
    with _lock:
        _load_locked()
        # Case-insensitive dedup (matches audit M-16 in is_seen).
        _lower = t.lower()
        if _lower in _cache_lower:
            return False
        _cache.add(t)
        _cache_lower.add(_lower)
    if config_is_writable():
        # Hold _lock across the file write so concurrent mark_seen
        # calls can't interleave bytes within a single line. Python's
        # text-append is NOT atomic at the line level on Windows
        # without O_APPEND; without this lock, two threads writing
        # different titles produced corrupted concatenated entries
        # (e.g. "Title oneTitle two\n") that polluted the cache.
        with _lock:
            try:
                SEEN_FILTER_TITLES.parent.mkdir(parents=True, exist_ok=True)
                with SEEN_FILTER_TITLES.open("a", encoding="utf-8") as f:
                    f.write(t + "\n")
            except OSError:
                pass
    return True


def clear():
    """Nuke the cache + file."""
    # Hold _lock across BOTH the cache clear AND the file unlink so a
    # concurrent mark_seen can't slip an entry into the cache that
    # then doesn't match disk after the unlink — previously the cache
    # diverged from disk after a clear-vs-mark race.
    with _lock:
        _cache.clear()
        _cache_lower.clear()
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
