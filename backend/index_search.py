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


def _dedupe_segment_hits(rows: list[Any]) -> list[Any]:
    """Collapse duplicate segment rows while preserving query order.

    Some long-lived indexes contain the same transcript segment twice with
    only path spelling changed (`Z:/...` vs `Z:\\...`). Search should show the
    hit once; the first row keeps its segment id for context loading.
    """
    seen: set[tuple[Any, ...]] = set()
    out: list[Any] = []
    for r in rows:
        video_key = (r[1] or "").strip().lower()
        if not video_key:
            video_key = "|".join((
                (r[2] or "").strip().lower(),
                (r[3] or "").strip().lower(),
                (r[6] or "").replace("\\", "/").strip().lower(),
            ))
        try:
            start_key: Any = round(float(r[4] or 0), 3)
        except Exception:
            start_key = r[4]
        key = (
            video_key,
            start_key,
            (r[5] or "").strip(),
            (r[7] or "").strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


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
    # Year scope (inclusive). Mirror the FTS leg: prefer the video's
    # UPLOAD year (upload_ts = file mtime = YT upload date), since the
    # folder-derived `year` column is NULL for flat / non-year-organized
    # channels — which made the old "(year >= ? OR year IS NULL)" filter a
    # NO-OP. Fall back to `year` only when upload_ts is missing; stay
    # lenient (include) only when both are unknown.
    year_sql = ""
    if year_from is not None:
        year_sql += (" AND (CAST(strftime('%Y', upload_ts, 'unixepoch') AS INTEGER) >= ?"
                     " OR (upload_ts IS NULL AND year >= ?)"
                     " OR (upload_ts IS NULL AND year IS NULL))")
        args.append(int(year_from))
        args.append(int(year_from))
    if year_to is not None:
        year_sql += (" AND (CAST(strftime('%Y', upload_ts, 'unixepoch') AS INTEGER) <= ?"
                     " OR (upload_ts IS NULL AND year <= ?)"
                     " OR (upload_ts IS NULL AND year IS NULL))")
        args.append(int(year_to))
        args.append(int(year_to))
    requested_limit = max(1, int(limit))
    args.append(requested_limit)
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
    # When a year filter is active, ALSO resolve the upload date via a
    # (channel, title) fallback for segments whose video_id is empty (and so
    # don't join to their correctly-dated videos row). Without it those
    # undated segments leak through any year window — e.g. a flat,
    # Tesla-heavy channel still showed 2024 results under a 2008 filter even
    # after the upload_ts fix, because ~4.6% of segments globally have an
    # empty video_id. The derived table is pre-grouped (one row per
    # channel+title) so it can never duplicate result rows, and it's only
    # joined when a year filter is set, so plain searches pay no cost.
    # Backed by idx_vid_chan_title(channel, title, upload_ts).
    _year_active = (year_from is not None) or (year_to is not None)
    _vt_join = (
        " LEFT JOIN (SELECT channel, title, MIN(upload_ts) AS uts FROM videos "
        " WHERE upload_ts IS NOT NULL GROUP BY channel, title) vt "
        " ON v.upload_ts IS NULL AND vt.channel = s.channel AND vt.title = s.title "
    ) if _year_active else ""
    _ts_expr = (
        "COALESCE(v.upload_ts, vt.uts, v.added_ts, 0)"
        if _year_active else
        "COALESCE(v.upload_ts, v.added_ts, 0)"
    )
    q = ("SELECT s.id, s.video_id, s.title, s.channel, s.start_time, s.text, "
         " s.jsonl_path, snippet(segments_fts, 0, '<mark>', '</mark>', '...', 8) as snip, "
         f" {_ts_expr} AS ts "
         " FROM segments_fts JOIN segments s ON s.id = segments_fts.rowid "
         " LEFT JOIN videos v ON s.video_id <> '' AND v.video_id = s.video_id "
         + _vt_join +
         " WHERE segments_fts MATCH ?")
    requested_limit = max(1, int(limit))
    # Dedupe happens after SQLite returns rows because the duplicates differ
    # only in index metadata. Pull a little extra so a duplicate-heavy index
    # still fills the requested page with unique hits.
    query_limit = min(requested_limit * 3, 1000)
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
    # Year filter. Prefer the video's UPLOAD year (v.upload_ts = file mtime
    # = YT upload date) as the authoritative source. segments.year is
    # folder-derived and is NULL for flat / drop-in / non-year-organized
    # channels (the common case), which made the old
    # "(s.year >= ? OR s.year IS NULL)" filter a NO-OP — every NULL-year
    # segment passed, so the window never constrained anything (a "2008"
    # filter still returned 2024 segments). Now: filter on the upload year
    # when we have it, fall back to the folder-derived s.year when
    # upload_ts is missing, and stay lenient (include the row) only when
    # BOTH sources are unknown.
    if year_from is not None:
        suffix += (" AND (CAST(strftime('%Y', COALESCE(v.upload_ts, vt.uts), 'unixepoch') AS INTEGER) >= ?"
                   " OR (COALESCE(v.upload_ts, vt.uts) IS NULL AND s.year >= ?)"
                   " OR (COALESCE(v.upload_ts, vt.uts) IS NULL AND s.year IS NULL))")
        args_suffix.append(int(year_from))
        args_suffix.append(int(year_from))
    if year_to is not None:
        suffix += (" AND (CAST(strftime('%Y', COALESCE(v.upload_ts, vt.uts), 'unixepoch') AS INTEGER) <= ?"
                   " OR (COALESCE(v.upload_ts, vt.uts) IS NULL AND s.year <= ?)"
                   " OR (COALESCE(v.upload_ts, vt.uts) IS NULL AND s.year IS NULL))")
        args_suffix.append(int(year_to))
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
    else:
        # relevance: FTS5 does NOT rank by default — without an explicit
        # ORDER BY rank it returns matches in ascending-rowid order
        # (oldest-ingested first), which LIMIT then truncates to. `rank`
        # is FTS5's bm25 auxiliary column; unambiguous here because
        # segments_fts is the only FTS table in the query.
        suffix += " ORDER BY rank"
    suffix += " LIMIT ?"
    args_suffix.append(query_limit)

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
    rows = _dedupe_segment_hits(rows)[:requested_limit]
    return [{
        "segment_id": r[0], "video_id": r[1], "title": r[2], "channel": r[3],
        "start_time": r[4], "text": r[5], "jsonl_path": r[6], "snippet": r[7],
        "added_ts": r[8] or 0, "upload_ts": r[8] or 0,
    } for r in rows]
