"""
index_search — FTS5 search + video-title search over the SQLite index.

Extracted from backend/index.py (Patch 17, v71.9). Owns the search
endpoints the Browse > Search tab calls into:

    search_video_titles(query, ...)  — LIKE-based title scan
    search_fts(query, ...)           — FTS5 MATCH over transcript segments
    _sanitize_fts_query(q)           — punctuation stripper for FTS5

The connection + lock primitives live in `index.py`; this module
reaches for them via `from . import index as _idx`.
"""
from __future__ import annotations

import sqlite3
from typing import Any

from . import index as _idx
from .log import get_logger

_log = get_logger(__name__)


def _sanitize_fts_query(q: str) -> str:
    """Defensive fallback sanitizer for FTS5 MATCH queries.

    The UI exposes AND/OR/NOT/"phrase"/prefix* as power-user operators.
    If the raw query has syntax that FTS5 rejects (unbalanced quotes,
    stray punctuation), this function is tried as a second chance:
    strip everything that isn't word-chars / space / quote / * / - so
    FTS5 treats it as implicit-AND across bare terms — matches OLD's
    YTArchiver.py:29728 stripping behavior.
    """
    import re as _re
    # Keep word chars, spaces, quotes, wildcard, minus (NOT). Drop everything else.
    cleaned = _re.sub(r'[^\w\s"*\-]', " ", q or "")
    # Collapse whitespace
    cleaned = _re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _normalize_fts_query(raw: str) -> str:
    """Make a user-typed query safe for FTS5 MATCH *without* breaking the
    supported operators.

    The Search UI exposes five operators that arrive as literal text in the
    box: AND / OR / NOT, "exact phrase" (double quotes) and word* (trailing
    wildcard). Everything else is a plain term — but FTS5 treats characters
    like '-' (inside "well-known"), stray quotes, ':' and '^' as query
    syntax, so a hyphenated word errors out / matches nothing (the reported
    bug) and a stray quote aborts the parse.

    Tokenize (keeping balanced "phrases" intact), pass operator keywords /
    phrases / trailing wildcards through unchanged, and wrap any bare term
    containing FTS5-special characters in double quotes so FTS5 treats it as
    a literal phrase: "well-known" -> "well known" (adjacent), exactly what
    the user expects. The except-retry sanitizer in search_fts stays as a
    last-ditch fallback. Verified against a live FTS5 table across all five
    operators plus hyphen / apostrophe / percent / unbalanced-quote inputs.
    """
    import re as _re
    raw = (raw or "").strip()
    if not raw:
        return raw
    _OPS = {"AND", "OR", "NOT", "NEAR"}
    _TOKENS = _re.compile(r'"[^"]*"|\S+')
    _CLEAN = _re.compile(r'[^\W_]+', _re.UNICODE)  # bareword: alnum + non-ASCII

    def _tok(t: str) -> str:
        # Balanced "exact phrase" — keep as written (drop if empty).
        if len(t) >= 2 and t[0] == '"' and t[-1] == '"' and t.count('"') % 2 == 0:
            return "" if t.strip('"').strip() == "" else t
        if t in _OPS:                      # bare operator keyword — keep
            return t
        lead = ""                          # peel grouping parens so "(a OR b)" survives
        while t[:1] == "(":
            lead += "("; t = t[1:]
        trail = ""
        while t[-1:] == ")":
            trail = ")" + trail; t = t[:-1]
        star = ""                          # preserve a trailing wildcard
        if t[-1:] == "*":
            star = "*"; t = t[:-1]
        t = t.replace('"', "")             # drop stray quotes from a bare term
        if t == "":
            return lead + trail
        if _CLEAN.fullmatch(t):            # plain word — leave bare
            return lead + t + star + trail
        return lead + '"' + t + '"' + star + trail   # specials → literal phrase

    out = [x for x in (_tok(t) for t in _TOKENS.findall(raw)) if x]
    result = " ".join(out).strip()
    return result or raw


def search_video_titles(query: str,
                          channel: Any | None = None,
                          limit: int = 200,
                          sort: str = "newest",
                          year_from: int | None = None,
                          year_to: int | None = None,
                          ) -> list[dict[str, Any]]:
    """Global title-only search across the archive's videos.

    `channel` scopes the search: None / empty list → all channels;
    a string → that one channel; a list of strings → that subset.
    Title is LIKE-based, case-insensitive. Result shape mirrors
    search_fts so the frontend renderer can swap modes without
    restructuring.

    `sort` accepts:
      "newest"  → upload date DESC (default, oldest behavior)
      "oldest"  → upload date ASC
      "channel" → channel name ASC, then upload date DESC within a channel
      "title"   → title ASC (alphabetical)
    Unknown values fall back to "newest".
    """
    if not query or not query.strip():
        return []
    # Use the dedicated reader connection so this query doesn't queue
    # behind a long-running write (sweep_new_videos, ingest_jsonl, etc.).
    # SQLite WAL mode allows readers to proceed in parallel with the
    # single writer at the file level; only Python's _db_lock was
    # serializing everything before this swap.
    conn = _idx._reader_open()
    if conn is None:
        return []
    # Allow multi-word queries to match in any order — split on
    # whitespace and AND each word together.
    parts = [p.strip() for p in query.strip().split() if p.strip()]
    if not parts:
        return []
    # Escape LIKE wildcards so a query containing % or _ matches those
    # characters literally instead of acting as a wildcard (which made
    # e.g. "%" match every title). ESCAPE '\' tells SQLite that a
    # backslash-prefixed %/_/\ is a literal.
    def _esc_like(s: str) -> str:
        return (s.replace("\\", "\\\\")
                 .replace("%", "\\%")
                 .replace("_", "\\_"))
    where_clauses = " AND ".join(
        ["title LIKE ? COLLATE NOCASE ESCAPE '\\'"] * len(parts))
    args: list[Any] = [f"%{_esc_like(p)}%" for p in parts]
    # Channel scope: accept string (legacy) or list (new multi-select).
    chan_sql = ""
    if isinstance(channel, str) and channel.strip():
        chan_sql = " AND channel = ?"
        args.append(channel.strip())
    elif isinstance(channel, (list, tuple)) and channel:
        _names = [str(c).strip() for c in channel if str(c).strip()]
        if _names:
            placeholders = ",".join(["?"] * len(_names))
            chan_sql = f" AND channel IN ({placeholders})"
            args.extend(_names)
    # Year scope (inclusive). Mirror the FTS leg: OR-include year IS NULL
    # so videos we couldn't date yet aren't silently dropped when the
    # user sets a year window.
    year_sql = ""
    if year_from is not None:
        year_sql += " AND (year >= ? OR year IS NULL)"
        args.append(int(year_from))
    if year_to is not None:
        year_sql += " AND (year <= ? OR year IS NULL)"
        args.append(int(year_to))
    args.append(int(limit))
    # Translate sort key → SQL ORDER BY clause.
    order_sql = {
        "oldest":  "ts ASC",
        "newest":  "ts DESC",
        "channel": "channel COLLATE NOCASE ASC, ts DESC",
        "title":   "title COLLATE NOCASE ASC",
    }.get((sort or "newest").lower(), "ts DESC")
    try:
        with _idx._reader_lock:
            cur = conn.execute(
                f"SELECT video_id, title, channel, filepath, year, "
                f"COALESCE(upload_ts, added_ts, 0) AS ts "
                f"FROM videos WHERE {where_clauses}"
                f"{chan_sql}"
                f"{year_sql} "
                f"AND is_duplicate_of IS NULL "
                f"ORDER BY {order_sql} LIMIT ?",
                args)
            rows = cur.fetchall()
    except sqlite3.Error as e:
        try: print(f"[search_video_titles] error: {e}")
        except Exception as e: _log.debug("swallowed: %s", e)
        return []
    return [{
        "video_id": r[0] or "",
        "title": r[1] or "",
        "channel": r[2] or "",
        "filepath": r[3] or "",
        "year": r[4],
        "ts": r[5],
        # added_ts/upload_ts under the same key the transcript leg
        # uses so the JS merge-sort can apply newest/oldest ordering
        # without a missing-field fall-back to 0 (audit: H148).
        "added_ts": r[5] or 0,
        "upload_ts": r[5] or 0,
    } for r in rows]


def search_fts(query: str, channel: Any | None = None, limit: int = 200,
               year_from: int | None = None, year_to: int | None = None,
               sort: str = "relevance",
               ) -> list[dict[str, Any]]:
    """Run FTS5 MATCH against segments. Returns hits with context.

    Query semantics: power-user operators (AND / OR / NOT / "phrase" / word*)
    pass through to FTS5 as-is on the first attempt. If that raises a syntax
    error (common when users paste something with unbalanced quotes or
    parentheses), the function retries with a sanitizer that strips all
    non-word punctuation and lets FTS5 treat the result as implicit-AND —
    matching YTArchiver.py:29728 behavior. Empty result on second failure.

    Optional `year_from` / `year_to` filter the segment by `segments.year`
    (inclusive). Either bound may be None.

    `sort` accepts:
      "relevance" → FTS5 bm25 rank (default; most-relevant first)
      "newest"    → video upload date DESC
      "oldest"    → video upload date ASC
      "channel"   → channel name ASC, then in-video chronological
      "title"     → video title ASC, then in-video chronological
    """
    # Reader connection — see search_video_titles above for rationale.
    conn = _idx._reader_open()
    if conn is None or not query.strip():
        return []
    # Pull v.upload_ts via LEFT JOIN so newest/oldest sort works even
    # when the date-based ORDER BY references the videos table.
    # LEFT JOIN (not INNER) so rows without a matching videos entry
    # (legacy seed data, FTS phantoms) still appear; they sort to the
    # end on date sorts because upload_ts is NULL.
    q = ("SELECT s.id, s.video_id, s.title, s.channel, s.start_time, s.text, "
         " s.jsonl_path, snippet(segments_fts, 0, '<mark>', '</mark>', '...', 8) as snip "
         " FROM segments_fts JOIN segments s ON s.id = segments_fts.rowid "
         " LEFT JOIN videos v ON s.video_id <> '' AND v.video_id = s.video_id "
         " WHERE segments_fts MATCH ?")
    args_suffix: list[Any] = []
    suffix = ""
    # Channel scope: string (legacy single-channel) or list (new
    # multi-select). Empty list / None = all channels.
    if isinstance(channel, str) and channel.strip():
        suffix += " AND s.channel=?"
        args_suffix.append(channel.strip())
    elif isinstance(channel, (list, tuple)) and channel:
        _names = [str(c).strip() for c in channel if str(c).strip()]
        if len(_names) == 1:
            suffix += " AND s.channel=?"
            args_suffix.append(_names[0])
        elif _names:
            placeholders = ",".join(["?"] * len(_names))
            suffix += f" AND s.channel IN ({placeholders})"
            args_suffix.extend(_names)
    # OR-include s.year IS NULL when a year filter is set.
    # Legacy rows (drop-in mode, pre-path-parsing, channels where the
    # folder layout isn't year-organized) have segments.year=NULL;
    # without the NULL clause the filter silently excluded them even
    # though the user's intent was "all results within this window,
    # including ones we can't place yet". Net effect: year-filtered
    # searches now include NULL-year rows rather than missing them.
    if year_from is not None:
        suffix += " AND (s.year >= ? OR s.year IS NULL)"
        args_suffix.append(int(year_from))
    if year_to is not None:
        suffix += " AND (s.year <= ? OR s.year IS NULL)"
        args_suffix.append(int(year_to))
    # Translate sort key → ORDER BY. For date sorts, NULLS LAST so
    # rows without an upload_ts (legacy data) don't dominate the top
    # of an "oldest first" sort. SQLite syntax for that is the
    # "(<col> IS NULL)" sort-key trick.
    _sort_key = (sort or "relevance").lower()
    if _sort_key == "newest":
        suffix += (" ORDER BY (v.upload_ts IS NULL) ASC, "
                   "v.upload_ts DESC, s.start_time ASC")
    elif _sort_key == "oldest":
        suffix += (" ORDER BY (v.upload_ts IS NULL) ASC, "
                   "v.upload_ts ASC, s.start_time ASC")
    elif _sort_key == "channel":
        suffix += (" ORDER BY s.channel COLLATE NOCASE ASC, "
                   "v.upload_ts DESC, s.start_time ASC")
    elif _sort_key == "title":
        suffix += " ORDER BY s.title COLLATE NOCASE ASC, s.start_time ASC"
    # else: relevance — leave FTS5's default rank ordering (no ORDER BY)
    suffix += " LIMIT ?"
    args_suffix.append(limit)

    def _run(q_text: str):
        with _idx._reader_lock:
            cur = conn.execute(q + suffix, [q_text] + args_suffix)
            return cur.fetchall()

    # Proactively normalize so plain terms containing FTS5-special chars
    # (e.g. the hyphen in "well-known", a stray quote, "%") match literally
    # instead of erroring / silently matching nothing — while the supported
    # AND/OR/NOT/"phrase"/word* operators pass through untouched. The
    # except-retry below remains as a last-ditch fallback.
    rows: list[Any] = []
    _qnorm = _normalize_fts_query(query)
    try:
        rows = _run(_qnorm)
    except sqlite3.Error:
        # Roll back any aborted txn state on the shared reader
        # connection before retrying — otherwise the second _run
        # can inherit a "transaction aborted" state and fail with
        # an opaque error instead of returning rows (audit:
        # index_search H122).
        try: conn.rollback()
        except sqlite3.Error: pass
        # Retry once with the sanitized query — gives user-typed input a chance
        # to match even with stray punctuation or unbalanced quotes.
        cleaned = _sanitize_fts_query(query)
        if cleaned and cleaned != query:
            try:
                rows = _run(cleaned)
            except sqlite3.Error as e2:
                # Bug [52]: returning [{"error": ...}] poisoned the
                # iterator since callers access r["segment_id"] etc.
                # Print the error and return an empty list so the UI
                # renders "no results" cleanly instead of crashing.
                try: print(f"[search_fts] FTS error: {e2}")
                except Exception as e: _log.debug("swallowed: %s", e)
                return []
        else:
            try: print(f"[search_fts] Invalid FTS5 query: {query!r}")
            except Exception as e: _log.debug("swallowed: %s", e)
            return []
    return [{
        "segment_id": r[0], "video_id": r[1], "title": r[2], "channel": r[3],
        "start_time": r[4], "text": r[5], "jsonl_path": r[6], "snippet": r[7],
    } for r in rows]
