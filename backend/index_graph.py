"""
index_graph — word-frequency graphing + per-bucket aggregate stats.

Graph queries extracted from backend/index.py. Powers Browse > Graph:

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
    list_all_channels_in_db()
        — distinct channels present in the segments table

Connection + lock primitives live in index.py; this module reaches
for them via `from . import index as _idx`.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from collections import OrderedDict
from typing import Any

from .log import get_logger, swallow

_log = get_logger(__name__)
_TOP_WORDS_CACHE_MAX = 24
_TOP_WORDS_CACHE_REVISION = 0
_TOP_WORDS_CACHE: OrderedDict[
    tuple[int, str, int, int], list[dict[str, Any]]
] = OrderedDict()
_BUCKET_TOTALS_CACHE_MAX = 32
_BUCKET_TOTALS_CACHE: OrderedDict[
    tuple[int, int, int, int, str, str], dict[str, int]
] = OrderedDict()
_TOP_WORDS_CACHE_LOCK = threading.Lock()
TOP_WORDS_SAMPLE_LIMIT = 500_000
TOP_WORDS_SAMPLE_LABEL = (
    "Limited sample: word frequencies use at most the oldest 500,000 "
    "transcript segments, not the complete archive."
)


def _calendar_bucket_expr(bucket: str) -> str:
    """SQL bucket using canonical upload time, then segment path metadata."""
    if bucket == "year":
        return (
            "COALESCE("
            "strftime('%Y', v.logical_upload_ts, 'unixepoch', 'localtime'), "
            "CASE WHEN s.year IS NOT NULL THEN CAST(s.year AS TEXT) "
            "ELSE NULL END)"
        )
    return (
        "COALESCE("
        "strftime('%Y-%m', v.logical_upload_ts, 'unixepoch', 'localtime'), "
        "CASE WHEN s.year IS NOT NULL AND s.month IS NOT NULL "
        "THEN CAST(s.year AS TEXT) || '-' || printf('%02d', s.month) "
        "ELSE NULL END)"
    )


def invalidate_top_words_cache() -> None:
    global _TOP_WORDS_CACHE_REVISION
    with _TOP_WORDS_CACHE_LOCK:
        _TOP_WORDS_CACHE_REVISION += 1
        _TOP_WORDS_CACHE.clear()
        # Segment ingestion changes both the word-cloud sample and the
        # denominators used by normalized graphs.  Keep one shared revision so
        # every existing ingest invalidation site also retires bucket totals.
        _BUCKET_TOTALS_CACHE.clear()


def _index():
    """Lazy import to avoid the index <-> index_graph re-export cycle."""
    from . import index
    return index


def _bucket_cache_revision(conn: sqlite3.Connection) -> tuple[int, int, int]:
    """Return a cheap connection/database revision for bucket-total caching.

    Segment ingestion already bumps the module revision below, but bucket
    labels also depend on ``videos.upload_ts`` and canonical-copy metadata.
    Those can change without touching a segment. ``PRAGMA data_version``
    notices commits made by the separate production writer connection, while
    ``total_changes`` covers focused tests and any caller that intentionally
    supplies the same connection for reads and writes. The connection identity
    prevents a reopened/different database with coincidentally equal counters
    from inheriting an old result.
    """
    try:
        with _index()._reader_lock:
            row = conn.execute("PRAGMA data_version").fetchone()
        data_version = int(row[0] or 0) if row else 0
    except (sqlite3.Error, TypeError, ValueError):
        data_version = 0
    try:
        total_changes = int(conn.total_changes)
    except (sqlite3.Error, AttributeError, TypeError, ValueError):
        total_changes = 0
    return id(conn), data_version, total_changes


def _bucket_cache_lookup(
    conn: sqlite3.Connection,
    bucket: str,
    channel: str | None,
) -> tuple[tuple[int, int, int, int, str, str], dict[str, int] | None]:
    connection_id, data_version, total_changes = _bucket_cache_revision(conn)
    with _TOP_WORDS_CACHE_LOCK:
        cache_key = (
            _TOP_WORDS_CACHE_REVISION,
            connection_id,
            data_version,
            total_changes,
            bucket,
            channel or "",
        )
        cached = _BUCKET_TOTALS_CACHE.get(cache_key)
        if cached is None:
            return cache_key, None
        _BUCKET_TOTALS_CACHE.move_to_end(cache_key)
        return cache_key, dict(cached)


def bucket_totals(bucket: str = "month",
                  channel: str | None = None) -> dict[str, int]:
    """Return {bucket_label: total_segments_in_bucket} so the Graph's
    Normalize toggle can divide each bucket's count against its segment
    volume. Matches YTArchiver.py normalize logic that divides word counts
    by per-bucket total then multiplies by 1000.
    """
    bucket = bucket if bucket in {"year", "month", "week"} else "month"
    channel = channel if isinstance(channel, str) and channel else None
    conn = _index()._reader_open()
    if conn is None:
        return {}
    cache_key, cached = _bucket_cache_lookup(conn, bucket, channel)
    if cached is not None:
        return cached
    canonical_ctes = _index().canonical_videos_cte_sql()
    # The old query joined every one of the archive's ~55 million transcript
    # rows to the canonical-video window CTE and evaluated strftime() for every
    # row.  On the real archive that monopolized the shared reader for minutes
    # and made a later local Watch page look frozen.
    #
    # Segment counts are constant per video, so first collapse the narrow
    # video_id index to one row per video.  Only then join the ~110k aggregate
    # rows to canonical video metadata.  MIN(id) supplies one representative
    # segment for the legacy year/month fallback without reading transcript
    # text or table rows during the large scan.  All segments for an ingested
    # transcript carry the same path-derived year/month.
    seg_source = "segments AS seg"
    if channel is None:
        seg_source += " INDEXED BY idx_seg_video_id"
    count_where = (
        "seg.video_id IS NOT NULL AND seg.video_id <> ''"
    )
    count_args: list[Any] = []
    if channel:
        count_where += " AND seg.channel=?"
        count_args.append(channel)
    segment_counts_cte = (
        "segment_counts AS MATERIALIZED ("
        "SELECT seg.video_id, COUNT(*) AS segment_count, "
        "MIN(seg.id) AS first_segment_id "
        f"FROM {seg_source} WHERE {count_where} "
        "GROUP BY seg.video_id)"
    )

    def _cache(result: dict[str, int]) -> dict[str, int]:
        with _TOP_WORDS_CACHE_LOCK:
            # A revision may have changed while the SQL was running. Cache only
            # under the snapshot that started this request; a newer caller
            # naturally misses it rather than observing stale totals.
            _BUCKET_TOTALS_CACHE[cache_key] = dict(result)
            _BUCKET_TOTALS_CACHE.move_to_end(cache_key)
            while len(_BUCKET_TOTALS_CACHE) > _BUCKET_TOTALS_CACHE_MAX:
                _BUCKET_TOTALS_CACHE.popitem(last=False)
        return result

    if bucket == "week":
        # Week totals MUST be keyed by the same ISO-week label that
        # word_frequency() emits ("YYYY-Www"), computed in Python from
        # videos.upload_ts. segments only store year+month, so the old
        # fall-through grouped by YEAR and returned year keys ("2015")
        # that never matched the week-keyed word counts — so Normalize +
        # Week divided every bucket by a missing denominator and the
        # chart rendered all zeros. Mirror word_frequency's week JOIN +
        # isocalendar() bucketing exactly so the keys line up.
        sql = (
            f"WITH {canonical_ctes}, {segment_counts_cte} "
            "SELECT v.logical_upload_ts, SUM(sc.segment_count) "
            "FROM segment_counts sc "
            "JOIN canonical_videos v ON v.video_id = sc.video_id "
            "WHERE v.logical_upload_ts IS NOT NULL "
            "GROUP BY v.logical_upload_ts"
        )
        try:
            with _index()._reader_lock:
                # Another caller may have completed the same expensive scan
                # while this one waited for the shared reader. Re-read both
                # the SQLite revision and cache under that serialization lock
                # before doing any aggregate work.
                cache_key, cached = _bucket_cache_lookup(
                    conn, bucket, channel)
                if cached is not None:
                    return cached
                rows = conn.execute(sql, count_args).fetchall()
        except sqlite3.Error as exc:
            _log.warning("bucket_totals week query failed: %s", exc)
            return {}
        import datetime as _dt_w
        totals: dict[str, int] = {}
        for ts, cnt in rows:
            if ts is None:
                continue
            try:
                _dtobj = _dt_w.datetime.fromtimestamp(float(ts))
                iso = _dtobj.isocalendar()
                key = f"{iso.year:04d}-W{iso.week:02d}"
            except Exception:
                continue
            totals[key] = totals.get(key, 0) + int(cnt or 0)
        return _cache(totals)
    # Mirror graph_word_frequency's month bucketing EXACTLY so the Normalize
    # denominator keys line up: prefer the month from videos.upload_ts (the
    # mtime = true upload date), LEFT JOIN so path-only legacy segments fall
    # back to CAST(year AS TEXT) year/month. TEXT keys keep ORDER stable and
    # NULL-safe (date-less segments drop out, never "0000"). See the long
    # note in graph_word_frequency() for why the path month is unreliable.
    # Year and month both prefer the selected canonical video's upload date.
    # Path-derived segment fields are an orphan/unmatched-ID compatibility
    # fallback.  Truly ID-less rows are counted separately because grouping
    # every blank ID into one representative row would collapse unrelated
    # transcripts.
    bucket_expr = _calendar_bucket_expr(bucket)
    if bucket == "year":
        orphan_bucket_expr = (
            "CASE WHEN s.year IS NOT NULL THEN CAST(s.year AS TEXT) "
            "ELSE NULL END"
        )
    else:
        orphan_bucket_expr = (
            "CASE WHEN s.year IS NOT NULL AND s.month IS NOT NULL "
            "THEN CAST(s.year AS TEXT) || '-' || printf('%02d', s.month) "
            "ELSE NULL END"
        )
    orphan_source = "segments AS s"
    if channel is None:
        orphan_source += " INDEXED BY idx_seg_video_id"
    orphan_where = "(s.video_id IS NULL OR s.video_id = '')"
    args = list(count_args)
    if channel:
        orphan_where += " AND s.channel=?"
        args.append(channel)
    sql = (
        f"WITH {canonical_ctes}, {segment_counts_cte}, "
        "raw_bucket_totals AS ("
        f"SELECT {bucket_expr} AS bucket, SUM(sc.segment_count) AS n "
        "FROM segment_counts sc "
        "LEFT JOIN canonical_videos v ON v.video_id = sc.video_id "
        "LEFT JOIN segments s ON s.id = sc.first_segment_id "
        "GROUP BY bucket "
        "UNION ALL "
        f"SELECT {orphan_bucket_expr} AS bucket, COUNT(*) AS n "
        f"FROM {orphan_source} WHERE {orphan_where} GROUP BY bucket) "
        "SELECT bucket, SUM(n) FROM raw_bucket_totals "
        "WHERE bucket IS NOT NULL GROUP BY bucket"
    )
    try:
        with _index()._reader_lock:
            cache_key, cached = _bucket_cache_lookup(conn, bucket, channel)
            if cached is not None:
                return cached
            rows = conn.execute(sql, args).fetchall()
    except sqlite3.Error as exc:
        _log.warning("bucket_totals query failed: %s", exc)
        return {}
    return _cache({
        str(r[0]): int(r[1] or 0) for r in rows if r[0] is not None
    })


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
    try:
        top_n_i = max(1, int(top_n))
    except (TypeError, ValueError):
        top_n_i = 120
    try:
        min_len_i = max(1, int(min_len))
    except (TypeError, ValueError):
        min_len_i = 3
    with _TOP_WORDS_CACHE_LOCK:
        cache_key = (_TOP_WORDS_CACHE_REVISION, channel or "", top_n_i,
                     min_len_i)
        cached = _TOP_WORDS_CACHE.get(cache_key)
        if cached is not None:
            _TOP_WORDS_CACHE.move_to_end(cache_key)
            return [dict(row) for row in cached]
    # Use an INDEPENDENT connection (not the shared reader) so this 500k-row
    # scan + Python word-aggregation doesn't hold _reader_lock and freeze every
    # other reader (Browse / Search / Watch) for the whole duration of a
    # Word-Cloud open on a huge archive (audit r2). WAL handles concurrent
    # reads at the DB layer; we close the connection in finally.
    conn = _index()._open_independent()
    if conn is None:
        return []
    sql = "SELECT text FROM segments"
    args: list[Any] = []
    if channel:
        sql += " WHERE channel=?"
        args.append(channel)
    # Cap at a large but finite number so a huge archive doesn't OOM us.
    # ORDER BY id makes capped samples stable across runs.
    sql += f" ORDER BY id LIMIT {TOP_WORDS_SAMPLE_LIMIT}"
    import re as _re
    word_re = _re.compile(rf"[a-zA-Z][a-zA-Z']{{{min_len_i - 1},}}")
    counts: dict[str, int] = {}
    try:
        cur = conn.execute(sql, args)
        for (txt,) in cur:
            if not txt:
                continue
            for raw in word_re.findall(txt):
                w = raw.lower().rstrip("'")
                if w in _STOP_WORDS:
                    continue
                counts[w] = counts.get(w, 0) + 1
    except sqlite3.Error as exc:
        _log.warning("top_words query failed: %s", exc)
        return []
    finally:
        try:
            conn.close()
        except Exception as exc:
            swallow("close top-words reader connection", exc)
    # Top-N
    items = sorted(counts.items(), key=lambda x: -x[1])[:top_n_i]
    result = [{"word": w, "count": c} for w, c in items]
    with _TOP_WORDS_CACHE_LOCK:
        _TOP_WORDS_CACHE[cache_key] = [dict(row) for row in result]
        _TOP_WORDS_CACHE.move_to_end(cache_key)
        while len(_TOP_WORDS_CACHE) > _TOP_WORDS_CACHE_MAX:
            _TOP_WORDS_CACHE.popitem(last=False)
    return result


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
    reader = _index()._reader_open()
    writer = _index()._open()
    if reader is None or writer is None:
        return {"filled": 0, "skipped": 0}
    filled = 0
    skipped = 0
    batch: list[tuple[float, int]] = []
    batch_size = 500

    def _flush_batch() -> None:
        nonlocal filled, batch
        if not batch:
            return
        with _index()._db_lock:
            writer.executemany(
                "UPDATE videos SET upload_ts=? WHERE rowid=?",
                batch)
            writer.commit()
        filled += len(batch)
        batch = []

    try:
        with _index()._reader_lock:
            sql = "SELECT rowid, filepath FROM videos WHERE upload_ts IS NULL"
            if limit > 0:
                sql += f" LIMIT {int(limit)}"
            rows = reader.execute(sql).fetchall()
        for rowid, fp in rows:
            try:
                if fp and os.path.isfile(fp):
                    mtime = os.path.getmtime(fp)
                    batch.append((mtime, rowid))
                    if len(batch) >= batch_size:
                        _flush_batch()
                else:
                    skipped += 1
            except OSError:
                skipped += 1
        _flush_batch()
    except sqlite3.Error as exc:
        try:
            with _index()._db_lock:
                writer.rollback()
        except sqlite3.Error as rollback_exc:
            swallow("roll back graph upload-time backfill", rollback_exc)
        _log.warning("backfill_upload_ts failed after %d filled/%d skipped: %s",
                     filled, skipped, exc)
    if filled:
        invalidate_top_words_cache()
    return {"filled": filled, "skipped": skipped}


def _week_backfill_pending(conn) -> int:
    try:
        canonical_ctes = _index().canonical_videos_cte_sql()
        with _index()._reader_lock:
            row = conn.execute(
                f"WITH {canonical_ctes} "
                "SELECT COUNT(*) FROM canonical_videos "
                "WHERE logical_upload_ts IS NULL AND is_available_copy=1"
            ).fetchone()
        return int(row[0] or 0) if row else 0
    except sqlite3.Error:
        return 0


def graph_word_frequency(word: str, channel: str | None = None,
                         bucket: str = "month",
                         _backfill_pending: int | None = None
                         ) -> dict[str, Any]:
    """Count occurrences of `word` per time bucket.

    bucket ∈ {"year", "month", "week"}. Returns {labels, values}.

    - "year" → canonical video upload year, then segments.year fallback
    - "month" → canonical upload month, then segment year/month fallback
    - "week" → group by ISO-week key "YYYY-Www" from videos.upload_ts
                (segments only store year+month, so weekly granularity
                requires joining videos + using the file mtime which
                yt-dlp set to the upload date via --mtime). Videos whose
                upload_ts is NULL are skipped from the week plot; the
                caller can trigger `backfill_upload_ts()` to populate.
    """
    conn = _index()._reader_open()
    if conn is None or not word.strip():
        return {"labels": [], "values": []}
    word = word.strip()
    canonical_ctes = _index().canonical_videos_cte_sql()
    # Normalize the same way Search does so hyphenated / punctuated terms
    # (e.g. "well-known") plot real data instead of silently rendering an
    # empty chart. Lazy import to avoid any import cycle at module load.
    try:
        from .index_search import _normalize_fts_query as _norm_fts
        word = _norm_fts(word)
    except Exception as exc:
        swallow("normalize graph search term", exc)
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
            f"WITH {canonical_ctes} "
            "SELECT v.logical_upload_ts, COUNT(*) "
            " FROM segments_fts fts "
            " JOIN segments s ON s.id = fts.rowid "
            " LEFT JOIN canonical_videos v "
            "   ON s.video_id <> '' AND v.video_id = s.video_id "
            " WHERE fts.text MATCH ? "
            " AND v.logical_upload_ts IS NOT NULL"
        )
        args: list[Any] = [word]
        if channel:
            sql += " AND s.channel=?"
            args.append(channel)
        # GROUP BY upload_ts so each video returns its own (ts, count) row.
        # Without it the bare COUNT(*) aggregate collapses the ENTIRE result
        # to a single row (one arbitrary ts + the full match total), which
        # rendered the week plot as one wildly-inflated bogus bucket per word
        # (e.g. "2015-W12 ~80k"). ISO-week grouping is still done in Python
        # below so year-boundary weeks (e.g. 2024-12-30 → 2025-W01) don't
        # split across two labels.
        sql += " GROUP BY v.logical_upload_ts"
    else:
        # FTS5 MATCH to find segments containing the word.
        args = [word]
        # Prefer the canonical video's upload date.  Only orphan segments
        # fall back to path-derived year/month; duplicate physical copies can
        # therefore neither multiply counts nor contribute competing dates.
        bucket_expr = _calendar_bucket_expr(bucket)
        sql = (f"WITH {canonical_ctes} "
               f"SELECT {bucket_expr} AS bucket, COUNT(*) "
               f" FROM segments_fts fts "
               f" JOIN segments s ON s.id = fts.rowid "
               f" LEFT JOIN canonical_videos v "
               f"   ON s.video_id <> '' AND v.video_id = s.video_id "
               f" WHERE fts.text MATCH ?")
        if channel:
            sql += " AND s.channel=?"
            args.append(channel)
        sql += " GROUP BY bucket ORDER BY bucket"
    try:
        with _index()._reader_lock:
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
        backfill_pending = (_backfill_pending if _backfill_pending is not None
                            else _week_backfill_pending(conn))
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
    backfill_pending = None
    if bucket == "week":
        conn = _index()._reader_open()
        if conn is not None:
            backfill_pending = _week_backfill_pending(conn)
    for w in words:
        if bucket == "week":
            r = graph_word_frequency(
                w, channel=channel, bucket=bucket,
                _backfill_pending=backfill_pending)
        else:
            r = graph_word_frequency(w, channel=channel, bucket=bucket)
        mapping = dict(zip(r.get("labels", []), r.get("values", []), strict=False))
        per_word[w] = mapping
        label_set.update(mapping.keys())
    labels = sorted(label_set)
    series = []
    for w in words:
        m = per_word[w]
        series.append({"word": w, "values": [m.get(lbl, 0) for lbl in labels]})
    return {"labels": labels, "series": series}

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
    backfill_pending = None
    if bucket == "week":
        conn = _index()._reader_open()
        if conn is not None:
            backfill_pending = _week_backfill_pending(conn)
    for ch in channels:
        if bucket == "week":
            r = graph_word_frequency(
                word, channel=ch, bucket=bucket,
                _backfill_pending=backfill_pending)
        else:
            r = graph_word_frequency(word, channel=ch, bucket=bucket)
        mapping = dict(zip(r.get("labels", []), r.get("values", []), strict=False))
        per_ch[ch] = mapping
        label_set.update(mapping.keys())
    labels = sorted(label_set)
    series = [{"channel": ch, "values": [per_ch[ch].get(lbl, 0) for lbl in labels]}
              for ch in channels]
    return {"labels": labels, "series": series}


def list_all_channels_in_db() -> list[str]:
    """Return the distinct set of channels that appear in the segments table."""
    conn = _index()._reader_open()
    if conn is None:
        return []
    with _index()._reader_lock:
        cur = conn.execute("SELECT DISTINCT channel FROM segments ORDER BY channel COLLATE NOCASE")
        return [r[0] for r in cur.fetchall() if r[0]]
