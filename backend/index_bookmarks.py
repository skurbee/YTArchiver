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


def bookmark_add(video_id: str, title: str, channel: str,
                 start_time: float, text: str, note: str = "") -> int | None:
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
    conn = _idx._open()
    if conn is None:
        return False
    with _idx._db_lock:
        cur = conn.execute(
            "UPDATE bookmarks SET note=? WHERE id=?", (note, bm_id))
        conn.commit()
    return cur.rowcount > 0
