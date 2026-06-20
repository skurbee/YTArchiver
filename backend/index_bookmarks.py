"""
index_bookmarks — bookmark CRUD over the SQLite index.

Extracted from backend/index.py (Patch 20, v72.2). Four small ops:

    bookmark_add(video_id, title, channel, start_time, text, note="") -> int|None
    bookmark_list(limit=500) -> list[dict]
    bookmark_remove(bm_id) -> bool
    bookmark_update_note(bm_id, note) -> bool

The schema lives in index.py (`bookmarks` table is created there during
`_idx._open()`). This module just provides the user-facing CRUD on top.
Connection + lock primitives are imported via `_idx`.
"""
from __future__ import annotations

from typing import Any

from . import index as _idx

_BOOKMARK_TEXT_MAX = 20000
_BOOKMARK_NOTE_MAX = 4000
_BOOKMARK_SHORT_TEXT_MAX = 1000
_BOOKMARK_LIMIT_MAX = 5000


def _bounded_text(value: Any, max_len: int) -> str:
    return str(value or "")[:max_len]


def _coerce_start_time(value: Any) -> float:
    try:
        import math
        out = float(value or 0)
        return out if math.isfinite(out) and out >= 0 else 0.0
    except (TypeError, ValueError):
        return 0.0


def _coerce_positive_int(value: Any) -> int | None:
    try:
        out = int(value)
        return out if out > 0 else None
    except (TypeError, ValueError):
        return None


def _coerce_limit(value: Any) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        out = 500
    return max(1, min(out, _BOOKMARK_LIMIT_MAX))


def bookmark_add(video_id: str, title: str, channel: str,
                 start_time: float, text: str, note: str = "") -> int | None:
    video_id = _bounded_text(video_id, _BOOKMARK_SHORT_TEXT_MAX).strip()
    if not video_id:
        return None
    title = _bounded_text(title, _BOOKMARK_SHORT_TEXT_MAX)
    channel = _bounded_text(channel, _BOOKMARK_SHORT_TEXT_MAX)
    start_time = _coerce_start_time(start_time)
    text = _bounded_text(text, _BOOKMARK_TEXT_MAX)
    note = _bounded_text(note, _BOOKMARK_NOTE_MAX)
    conn = _idx._open()
    if conn is None:
        return None
    # Set `created` explicitly (unix epoch) rather than leaning on the
    # column DEFAULT. Older index DBs were created with a literal
    # `DEFAULT '%s'` (the strftime wrapper was lost), so new rows inherited
    # the bare placeholder string "%s" — which then showed up verbatim in
    # the CSV export's "created" column. `CREATE TABLE IF NOT EXISTS` can't
    # repair an existing table's baked-in default, so we write the value
    # ourselves and bypass the default entirely.
    import time as _time
    created = _time.time()
    with _idx._db_lock:
        cur = conn.execute(
            "INSERT INTO bookmarks (video_id, title, channel, start_time, text, note, created) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (video_id, title, channel, start_time, text, note, created),
        )
        conn.commit()
        return cur.lastrowid


def bookmark_list(limit: int = 500) -> list[dict[str, Any]]:
    conn = _idx._reader_open()
    if conn is None:
        return []
    limit = _coerce_limit(limit)
    with _idx._reader_lock:
        cur = conn.execute(
            "SELECT id, video_id, title, channel, start_time, text, note, created "
            "FROM bookmarks ORDER BY created DESC LIMIT ?",
            (limit,),
        )
        return [{
            "id": r[0], "video_id": r[1], "title": r[2], "channel": r[3],
            "start_time": r[4], "text": r[5], "note": r[6], "created": r[7],
        } for r in cur.fetchall()]


def bookmark_remove(bm_id: int) -> bool:
    # return True only when an actual row changed. Old
    # behavior returned True unconditionally, so a stale-id click (e.g.
    # double-click after another session already deleted it) surfaced
    # as "Bookmark removed" while nothing happened, then the next
    # refresh showed the bookmark still there. Now False = nothing
    # matched that id.
    bm_id = _coerce_positive_int(bm_id)
    if bm_id is None:
        return False
    conn = _idx._open()
    if conn is None:
        return False
    with _idx._db_lock:
        cur = conn.execute("DELETE FROM bookmarks WHERE id=?", (bm_id,))
        conn.commit()
    return cur.rowcount > 0


def bookmark_update_note(bm_id: int, note: str) -> bool:
    # same reasoning as bookmark_remove — return False when
    # the id didn't match anything so callers don't show misleading
    # success toasts.
    bm_id = _coerce_positive_int(bm_id)
    if bm_id is None:
        return False
    note = _bounded_text(note, _BOOKMARK_NOTE_MAX)
    conn = _idx._open()
    if conn is None:
        return False
    with _idx._db_lock:
        cur = conn.execute(
            "UPDATE bookmarks SET note=? WHERE id=?", (note, bm_id))
        conn.commit()
    return cur.rowcount > 0
