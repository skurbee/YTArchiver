"""
index_graph — word-frequency graphing + per-bucket aggregate stats.

Extracted from backend/index.py (Patch 17, v71.9). Powers the
Browse > Graph view:

    bucket_totals(bucket, channel=None)
        — {bucket_label: total_segments} for normalization
    top_words(channel=None, top_n=120, ...)
        — most-frequent words across the FTS5 corpus
    backfill_upload_ts(limit=0)
        — back-populate segments.upload_ts from videos.upload_ts
    graph_word_frequency(word, channel=None, ...)
        — single-word time series
    graph_multi(words, ...)
        — overlay multiple words on the same chart
    graph_channel_overlay(word, channels, ...)
        — same word across multiple channels
    graph_word_frequency_multi(...)
        — alias / variant for the multi-word path
    list_all_channels_in_db()
        — distinct channels present in the segments table

Connection + lock primitives live in index.py; this module reaches
for them via `from . import index as _idx`.
"""
from __future__ import annotations

import os
import sqlite3
from typing import Any

from . import index as _idx
from .log import get_logger

_log = get_logger(__name__)


def bucket_totals(bucket: str = "month",
                  channel: str | None = None) -> dict[str, int]:
    """Return {bucket_label: total_segments_in_bucket} so the Graph's
    Normalize toggle can divide each bucket's count against its segment
    volume. Matches YTArchiver.py normalize logic that divides word counts
    by per-bucket total then multiplies by 1000.
    """
    conn = _idx._reader_open()
    if conn is None:
        return {}
    group_col = "year || '-' || printf('%02d', month)" if bucket == "month" else "year"
    sql = (f"SELECT {group_col} AS bucket, COUNT(*) "
           " FROM segments")
    args: list[Any] = []
    if channel:
        sql += " WHERE channel=?"
        args.append(channel)
    sql += " GROUP BY bucket"
    try:
        with _idx._reader_lock:
            rows = conn.execute(sql, args).fetchall()
    except sqlite3.Error:
        return {}
    return {str(r[0]): int(r[1] or 0) for r in rows if r[0] is not None}


# Rough English stop-word list — enough to keep a 100-word cloud interesting.
# Includes common contractions because the tokenizer allows apostrophes inside
# words ("it's", "i'm", "don't", etc. would otherwise dominate the cloud).
_STOP_WORDS = frozenset("""
a about above after again against all am an and any are aren as at be because
been before being below between both but by can cannot could did do does doing
don down during each few for from further had has have having he her here hers
herself him himself his how i if in into is it its itself just like me more
most my myself no nor not now of off on once only or other our ours ourselves
out over own same she should so some such than that the their theirs them
themselves then there these they this those through to too under until up very
was we were what when where which while who whom why will with would you your
yours yourself yourselves ll ve re ain aren couldn didn doesn don hadn hasn
haven isn mightn mustn needn shan shouldn wasn weren won wouldn also get got
going really know one two three get thing things something anything nothing
go way say said says see saw look right yeah okay hey uh um thats youre were
actually literally basically thats gonna wanna kinda sorta lot lots make makes
it's i'm don't won't can't didn't wasn't doesn't isn't aren't haven't hasn't
weren't wouldn't shouldn't couldn't you're we're they're we've i've you've
they've he's she's that's there's here's what's who's how's where's when's
let's who've you'll i'll we'll they'll he'll she'll i'd you'd we'd they'd
he'd she'd you'll ain't y'all gotta oh ooh ah ahh well alright ok
""".split())


def top_words(channel: str | None = None, top_n: int = 120,
              min_len: int = 3) -> list[dict[str, Any]]:
    """Return the top-N most-common words across all segments (optionally
    filtered to a single channel). Skips short tokens + stop words so the
    cloud surfaces actually-distinctive vocabulary.

    Returns a list of {word, count} sorted descending by count. Used by
    the Graph sub-mode's Word Cloud chart type.
    """
    conn = _idx._reader_open()
    if conn is None:
        return []
    sql = "SELECT text FROM segments"
    args: list[Any] = []
    if channel:
        sql += " WHERE channel=?"
        args.append(channel)
    # Cap at a large but finite number so a huge archive doesn't OOM us.
    sql += " LIMIT 500000"
    import re as _re
    word_re = _re.compile(r"[a-zA-Z][a-zA-Z']{%d,}" % (min_len - 1))
    counts: dict[str, int] = {}
    try:
        with _idx._reader_lock:
            cur = conn.execute(sql, args)
            for (txt,) in cur:
                if not txt:
                    continue
                for raw in word_re.findall(txt):
                    w = raw.lower().rstrip("'")
                    if w in _STOP_WORDS:
                        continue
                    counts[w] = counts.get(w, 0) + 1
    except sqlite3.Error:
        return []
    # Top-N
    items = sorted(counts.items(), key=lambda x: -x[1])[:int(top_n)]
    return [{"word": w, "count": c} for w, c in items]


def backfill_upload_ts(limit: int = 0) -> dict[str, int]:
    """Populate `videos.upload_ts` from file mtime for any row where it's
    currently NULL. Called lazily the first time a Week-bucket graph is
    requested so we don't force a full-archive stat walk at startup.

    yt-dlp sets each video file's mtime to the YouTube upload date via
    `--mtime`, so os.path.getmtime(filepath) is the authoritative upload
    timestamp. Missing files silently skip (leave NULL) — those rows
    won't contribute to week-bucket graphs but won't crash the query.

    Returns {filled: N, skipped: M}. `limit=0` means "all rows".
    """
    # Read the rowid list via the reader connection so we don't block
    # on a live sweep / ingest. The UPDATE phase needs the writer
    # connection — they have to be separate handles because the reader
    # has PRAGMA query_only=ON.
    reader = _idx._reader_open()
    writer = _idx._open()
    if reader is None or writer is None:
        return {"filled": 0, "skipped": 0}
    filled = 0
    skipped = 0
    try:
        with _idx._reader_lock:
            sql = "SELECT rowid, filepath FROM videos WHERE upload_ts IS NULL"
            if limit > 0:
                sql += f" LIMIT {int(limit)}"
            rows = reader.execute(sql).fetchall()
        for rowid, fp in rows:
            try:
                if fp and os.path.isfile(fp):
                    mtime = os.path.getmtime(fp)
                    with _idx._db_lock:
                        writer.execute(
                            "UPDATE videos SET upload_ts=? WHERE rowid=?",
                            (mtime, rowid))
                    filled += 1
                else:
                    skipped += 1
            except OSError:
                skipped += 1
        with _idx._db_lock:
            writer.commit()
    except sqlite3.Error:
        pass
    return {"filled": filled, "skipped": skipped}


def graph_word_frequency(word: str, channel: str | None = None,
                         bucket: str = "month") -> dict[str, Any]:
    """Count occurrences of `word` per time bucket.

    bucket ∈ {"year", "month", "week"}. Returns {labels, values}.

    - "year" → group by segments.year
    - "month" → group by "YYYY-MM" from segments.year + segments.month
    - "week" → group by ISO-week key "YYYY-Www" from videos.upload_ts
                (segments only store year+month, so weekly granularity
                requires joining videos + using the file mtime which
                yt-dlp set to the upload date via --mtime). Videos whose
                upload_ts is NULL are skipped from the week plot; the
                caller can trigger `backfill_upload_ts()` to populate.
    """
    conn = _idx._reader_open()
    if conn is None or not word.strip():
        return {"labels": [], "values": []}
    word = word.strip()
    if bucket == "week":
        # LEFT JOIN so segments with NULL video_id (common
        # for legacy rows and drop-in-mode archives without .info.json)
        # still COUNT against the match totals. Without this, the
        # inner join silently excluded them and the week graph showed
        # undercount. We still filter out rows that resolve to NULL
        # upload_ts (no bucket to assign) in the WHERE clause.
        # raw epoch is returned here; ISO-week labels are
        # computed in Python after fetch so week 52-53 → week 1
        # transitions don't split spanning weeks across two labels.
        sql = (
            "SELECT v.upload_ts, COUNT(*) "
            " FROM segments_fts fts "
            " JOIN segments s ON s.id = fts.rowid "
            " LEFT JOIN videos v ON v.video_id = s.video_id "
            " WHERE fts.text MATCH ? "
            " AND v.upload_ts IS NOT NULL"
        )
        args: list[Any] = [word]
        if channel:
            sql += " AND s.channel=?"
            args.append(channel)
        # No GROUP BY here — we aggregate in Python using isocalendar().
    else:
        # FTS5 MATCH to find segments containing the word
        group_col = ("year || '-' || printf('%02d', month)"
                     if bucket == "month" else "year")
        sql = (f"SELECT {group_col} AS bucket, COUNT(*) "
               f" FROM segments_fts fts "
               f" JOIN segments s ON s.id = fts.rowid "
               f" WHERE fts.text MATCH ?")
        args = [word]
        if channel:
            sql += " AND s.channel=?"
            args.append(channel)
        sql += " GROUP BY bucket ORDER BY bucket"
    try:
        with _idx._reader_lock:
            rows = conn.execute(sql, args).fetchall()
    except sqlite3.Error as e:
        return {"labels": [], "values": [], "error": str(e)}
    # for week bucket, aggregate in Python using
    # isocalendar() so year-boundary weeks (e.g. 2024-12-30 is in
    # ISO week 2025-W01) don't split into two half-sized bars.
    if bucket == "week":
        import datetime as _dt_w
        counts_by_iso: dict[str, int] = {}
        for ts, cnt in rows:
            if ts is None:
                continue
            try:
                _dtobj = _dt_w.datetime.fromtimestamp(float(ts))
                iso = _dtobj.isocalendar()
                key = f"{iso.year:04d}-W{iso.week:02d}"
            except Exception:
                continue
            counts_by_iso[key] = counts_by_iso.get(key, 0) + int(cnt)
        _sorted = sorted(counts_by_iso.items())
        labels = [k for k, _ in _sorted]
        values = [v for _, v in _sorted]
    else:
        labels = [str(r[0]) for r in rows if r[0] is not None]
        values = [int(r[1]) for r in rows if r[0] is not None]
    # when the caller requests week-granularity data while
    # backfill_upload_ts is still populating, the query silently returns
    # sparse results. Surface a `backfill_pending` count so the UI can
    # show "Still indexing... N videos pending" instead of letting the
    # user think their channel has no recent activity.
    backfill_pending = 0
    if bucket == "week":
        try:
            with _idx._reader_lock:
                row = conn.execute(
                    "SELECT COUNT(*) FROM videos WHERE upload_ts IS NULL"
                ).fetchone()
            if row:
                backfill_pending = int(row[0] or 0)
        except sqlite3.Error:
            pass
    return {"labels": labels, "values": values,
            "backfill_pending": backfill_pending}


def graph_multi(words: list[str], channel: str | None = None,
                bucket: str = "month") -> dict[str, Any]:
    """Multiple word-frequency series on one x axis.

    Returns { labels: [...], series: [{word, values: [...]}, ...] }
    so the JS can draw one line per word, all sharing the merged time range.
    """
    words = [w.strip() for w in (words or []) if w and w.strip()]
    if not words:
        return {"labels": [], "series": []}
    per_word = {}
    label_set = set()
    for w in words:
        r = graph_word_frequency(w, channel=channel, bucket=bucket)
        mapping = dict(zip(r.get("labels", []), r.get("values", [])))
        per_word[w] = mapping
        label_set.update(mapping.keys())
    labels = sorted(label_set)
    series = []
    for w in words:
        m = per_word[w]
        series.append({"word": w, "values": [m.get(lbl, 0) for lbl in labels]})
    return {"labels": labels, "series": series}


# Alias matching main.py's original call site (Session 11)
graph_word_frequency_multi = graph_multi


def graph_channel_overlay(word: str, channels: list[str],
                          bucket: str = "month") -> dict[str, Any]:
    """Same word across multiple channels — each channel is a series.

    Returns { labels: [...], series: [{channel, values: [...]}, ...] }.
    """
    channels = [c for c in (channels or []) if c]
    if not word or not channels:
        return {"labels": [], "series": []}
    per_ch = {}
    label_set = set()
    for ch in channels:
        r = graph_word_frequency(word, channel=ch, bucket=bucket)
        mapping = dict(zip(r.get("labels", []), r.get("values", [])))
        per_ch[ch] = mapping
        label_set.update(mapping.keys())
    labels = sorted(label_set)
    series = [{"channel": ch, "values": [per_ch[ch].get(lbl, 0) for lbl in labels]}
              for ch in channels]
    return {"labels": labels, "series": series}


def graph_word_frequency_multi(words: list[str], channel: str | None = None,
                                bucket: str = "month") -> dict[str, Any]:
    """Run multiple word-frequency queries in one call. Returns a shape
    ready for Chart.js with one dataset per word."""
    out = {"labels": [], "series": []}
    if not words:
        return out
    per = []
    all_labels = set()
    for w in words:
        r = graph_word_frequency(w, channel=channel, bucket=bucket)
        per.append({"word": w, "data": dict(zip(r["labels"], r["values"]))})
        all_labels.update(r["labels"])
    labels = sorted(all_labels)
    out["labels"] = labels
    for p in per:
        out["series"].append({
            "word": p["word"],
            "values": [p["data"].get(l, 0) for l in labels],
        })
    return out


def list_all_channels_in_db() -> list[str]:
    """Return the distinct set of channels that appear in the segments table."""
    conn = _idx._reader_open()
    if conn is None:
        return []
    with _idx._reader_lock:
        cur = conn.execute("SELECT DISTINCT channel FROM segments ORDER BY channel COLLATE NOCASE")
        return [r[0] for r in cur.fetchall() if r[0]]
