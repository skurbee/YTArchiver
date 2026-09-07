"""
index_bookmarks — bookmark CRUD over the SQLite index.

Focused bookmark operations extracted from backend/index.py:

    bookmark_add(video_id, title, channel, start_time, text, note="") -> int|None
    bookmark_list(limit=500) -> list[dict]
    bookmark_remove(bm_id) -> bool
    bookmark_update_note(bm_id, note) -> bool

The schema and production connection factories live in index.py.
BookmarkRepository depends on an explicit session and presentation callbacks;
it can be instantiated independently of the application and its profile.
"""
from __future__ import annotations

import time
from typing import Any

from .services.catalog_session import CatalogSession

_BOOKMARK_TEXT_MAX = 20000
_BOOKMARK_NOTE_MAX = 4000
_BOOKMARK_SHORT_TEXT_MAX = 1000
_BOOKMARK_LIMIT_MAX = 5000
_BOOKMARK_WRITE_SECONDS = 3.0
_VIDEO_SELECT = (
    "SELECT title, channel, filepath, video_id, size_bytes, year, month, "
    "tx_status, added_ts, upload_ts, view_count, like_count, "
    "removed_from_yt_ts, duration_s FROM videos"
)





def _bounded_text(value: Any, max_len: int) -> str:
    return str(value or "")[:max_len]


def _coerce_start_time(value: Any) -> float:
    try:
        import math
        out = float(value or 0)
        if not math.isfinite(out):
            return 0.0
        return -1.0 if out < 0 else out
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


class BookmarkRepository:
    """Bookmark CRUD with explicit transaction and optional UI enrichment ports."""

    def __init__(self, session: CatalogSession, *, video_row=None,
                 find_thumbnail=None, file_url=None):
        self.session = session
        self._video_row = video_row or (lambda *args, **kwargs: {})
        self._find_thumbnail = find_thumbnail or (lambda *args: "")
        self._file_url = file_url or (lambda path: path)

    def enrich_video_fields(self, conn, item: dict[str, Any]) -> None:
        row = None
        video_id = (item.get("video_id") or "").strip()
        title = (item.get("title") or "").strip()
        channel = (item.get("channel") or "").strip()
        if video_id:
            row = conn.execute(
                _VIDEO_SELECT
                + " WHERE video_id=? AND COALESCE(availability,'available')='available' "
                  "ORDER BY CASE WHEN COALESCE(is_duplicate_of,'')='' THEN 0 ELSE 1 END, "
                  "COALESCE(added_ts, upload_ts, 0) DESC LIMIT 1",
                (video_id,),
            ).fetchone()
        if not video_id and title and channel:
            candidates = conn.execute(
                _VIDEO_SELECT
                + " WHERE title=? AND channel=? "
                  "ORDER BY COALESCE(added_ts, upload_ts, 0) DESC LIMIT 2",
                (title, channel),
            ).fetchall()
            if len(candidates) == 1:
                row = candidates[0]
        if row is None:
            return
        try:
            video = self._video_row(row, include_thumbs=False)
        except Exception:
            return
        if not item.get("title"):
            item["title"] = video.get("title") or ""
        if not item.get("channel"):
            item["channel"] = video.get("channel") or ""
        if not item.get("video_id"):
            item["video_id"] = video.get("video_id") or ""
        for key in (
            "filepath", "size_bytes", "duration", "uploaded", "upload_ts",
            "views", "view_count", "tx_status", "removed_from_yt",
        ):
            if video.get(key) not in (None, ""):
                item[key] = video.get(key)
        fp = item.get("filepath") or ""
        if fp:
            try:
                tp = self._find_thumbnail(fp, item.get("video_id") or "")
                if tp:
                    item["thumbnail_url"] = self._file_url(tp)
            except Exception:
                pass


    def bookmark_add(self, video_id: str, title: str, channel: str,
                     start_time: float, text: str, note: str = "") -> int | None:
        video_id = _bounded_text(video_id, _BOOKMARK_SHORT_TEXT_MAX).strip()
        if not video_id:
            return None
        title = _bounded_text(title, _BOOKMARK_SHORT_TEXT_MAX)
        channel = _bounded_text(channel, _BOOKMARK_SHORT_TEXT_MAX)
        start_time = _coerce_start_time(start_time)
        text = _bounded_text(text, _BOOKMARK_TEXT_MAX)
        note = _bounded_text(note, _BOOKMARK_NOTE_MAX)
        # Set `created` explicitly (unix epoch) rather than leaning on the
        # column DEFAULT. Older index DBs were created with a literal
        # `DEFAULT '%s'` (the strftime wrapper was lost), so new rows inherited
        # the bare placeholder string "%s" — which then showed up verbatim in
        # the CSV export's "created" column. `CREATE TABLE IF NOT EXISTS` can't
        # repair an existing table's baked-in default, so we write the value
        # ourselves and bypass the default entirely.
        created = time.time()
        with self.session.transaction("Saving the bookmark", seconds=_BOOKMARK_WRITE_SECONDS) as conn:
            # A retry after an uncertain bridge response must not create another
            # identical bookmark. BEGIN IMMEDIATE serializes concurrent retries;
            # a different note or excerpt remains a separate, intentional save.
            existing = conn.execute(
                "SELECT id FROM bookmarks WHERE video_id=? AND start_time=? "
                "AND COALESCE(text,'')=? AND COALESCE(note,'')=? ORDER BY id LIMIT 1",
                (video_id, start_time, text, note),
            ).fetchone()
            if existing is not None:
                return existing[0]
            cur = conn.execute(
                "INSERT INTO bookmarks (video_id, title, channel, start_time, text, note, created) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (video_id, title, channel, start_time, text, note, created),
            )
            return cur.lastrowid


    def bookmark_list(self, limit: int = 500, query: str = "") -> list[dict[str, Any]]:
        limit = _coerce_limit(limit)
        query = _bounded_text(query, _BOOKMARK_SHORT_TEXT_MAX).strip()
        where = ""
        args: list[Any] = []
        if query:
            # instr treats %, _ and quotes literally. Filtering precedes LIMIT,
            # so an older bookmark remains discoverable outside the first page.
            where = (" WHERE instr(lower(COALESCE(title,'')), lower(?)) > 0"
                     " OR instr(lower(COALESCE(channel,'')), lower(?)) > 0"
                     " OR instr(lower(COALESCE(note,'')), lower(?)) > 0")
            args.extend([query, query, query])
        args.append(limit)
        with self.session.read("Loading bookmarks") as conn:
            cur = conn.execute(
                "SELECT id, video_id, title, channel, start_time, text, note, created "
                f"FROM bookmarks{where} ORDER BY created DESC LIMIT ?",
                args,
            )
            rows = [{
                "id": r[0], "video_id": r[1], "title": r[2], "channel": r[3],
                "start_time": r[4], "text": r[5], "note": r[6], "created": r[7],
            } for r in cur.fetchall()]
            for item in rows:
                self.enrich_video_fields(conn, item)
            return rows


    def bookmark_remove(self, bm_id: int) -> bool:
        # return True only when an actual row changed. Old
        # behavior returned True unconditionally, so a stale-id click (e.g.
        # double-click after another session already deleted it) surfaced
        # as "Bookmark removed" while nothing happened, then the next
        # refresh showed the bookmark still there. Now False = nothing
        # matched that id.
        bm_id = _coerce_positive_int(bm_id)
        if bm_id is None:
            return False
        with self.session.transaction("Removing the bookmark", seconds=_BOOKMARK_WRITE_SECONDS) as conn:
            cur = conn.execute("DELETE FROM bookmarks WHERE id=?", (bm_id,))
        return cur.rowcount > 0


    def bookmark_update_note(self, bm_id: int, note: str) -> bool:
        # same reasoning as bookmark_remove — return False when
        # the id didn't match anything so callers don't show misleading
        # success toasts.
        bm_id = _coerce_positive_int(bm_id)
        if bm_id is None:
            return False
        note = _bounded_text(note, _BOOKMARK_NOTE_MAX)
        with self.session.transaction("Updating the bookmark note", seconds=_BOOKMARK_WRITE_SECONDS) as conn:
            cur = conn.execute(
                "UPDATE bookmarks SET note=? WHERE id=?", (note, bm_id))
        return cur.rowcount > 0



def _repository() -> BookmarkRepository:
    # Lazy compatibility adapter: repository instances do not import index.
    from . import index
    return index.bookmark_repository()


def bookmark_add(video_id: str, title: str, channel: str,
                 start_time: float, text: str, note: str = "") -> int | None:
    return _repository().bookmark_add(video_id, title, channel, start_time, text, note)


def bookmark_list(limit: int = 500, query: str = "") -> list[dict[str, Any]]:
    return _repository().bookmark_list(limit, query)


def bookmark_remove(bm_id: int) -> bool:
    return _repository().bookmark_remove(bm_id)


def bookmark_update_note(bm_id: int, note: str) -> bool:
    return _repository().bookmark_update_note(bm_id, note)
