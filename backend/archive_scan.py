"""
Archive scanner — reads YTArchiver's existing disk-usage cache for fast
per-channel stats, with a fallback filesystem walk if the cache is
missing.

YTArchiver maintains `%APPDATA%\\YTArchiver\\ytarchiver_disk_cache.json` in the format:
    {
      "<channel_url>": {
        "num_vids": <int>,
        "size_bytes": <int>,
        "last_updated": <float unix ts>
      },
      ...
    }

YTArchiver reads this directly so the Subs table + Index tab show the user's
real numbers on startup. If the cache is missing, we fall back to walking
the filesystem — slow but correct.
"""

from __future__ import annotations

import json
import os
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .ytarchiver_config import DISK_CACHE_FILE, load_config


# Matches YTArchiver.py:134 _CHANNEL_VIDEO_EXTS
_CHANNEL_VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".avi", ".wav", ".mp3", ".m4a", ".flac")

# Partial file suffixes — never count these as real videos
_PARTIAL_SUFFIXES = (".part", ".tmp", ".temp", ".download", ".ytdl")


def _is_partial(fn: str) -> bool:
    """Return True if filename looks like a partial / temporary download,
    OR a yt-dlp intermediate track file whose stem ends with `.fNNN` or
    `.fNNN-X` (e.g. `<title>.f140-7.m4a`). These are produced during
    multi-track downloads and normally deleted after the merge, but a
    crashed / force-killed merge leaves them behind. Without this filter
    they show up in the Browse grid with broken titles like
    "Intel just did an AMD.f140-7" — this was reported Also covers
    our own `_TEMP_COMPRESS` suffix from aborted compress jobs.
    """
    fn_l = fn.lower()
    if fn_l.endswith(_PARTIAL_SUFFIXES):
        return True
    if ".part-" in fn_l or fn_l.endswith(".part"):
        return True
    if "_temp_compress" in fn_l:
        return True
    # yt-dlp intermediate: stem's last dot-segment is `f<digits>` or
    # `f<digits>-<digits>` (format code, optionally with DRC/track index).
    # audit E-38: require additional signals before classifying as a
    # yt-dlp partial so legitimate video titles ending in `.f<digits>`
    # aren't silently excluded. The .f<digits> suffix tail is ONLY
    # trustworthy as a yt-dlp partial marker when there's ALSO a
    # sibling .part or a matching merged file (meaning this one is
    # clearly an intermediate). As a simpler proxy, bound the digit
    # tail length — real yt-dlp format codes are 2-4 digits.
    import os as _os
    stem = _os.path.splitext(fn)[0]
    dot = stem.rfind(".")
    if dot >= 0:
        tail = stem[dot + 1:]
        if tail and tail[0].lower() == "f" and 2 <= len(tail) <= 8:
            core = tail[1:].replace("-", "")
            # yt-dlp format codes are typically short (e.g. 137, 140,
            # 400-4). Require the digit core to be ≤ 5 chars so
            # titles like "Release.f1500" (accidentally matching the
            # pattern with 4 chars) aren't rejected.
            if core.isdigit() and len(core) <= 5:
                return True
    return False


# ── Cache reading (fast path) ──────────────────────────────────────────

def load_disk_cache() -> Dict[str, Dict[str, Any]]:
    """Load ytarchiver_disk_cache.json if present. Returns {} on any error."""
    if not DISK_CACHE_FILE.exists():
        return {}
    try:
        with DISK_CACHE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def save_disk_cache(cache: Dict[str, Dict[str, Any]]) -> bool:
    """Atomic write back to ytarchiver_disk_cache.json. Returns True on success.
    Mirrors YTArchiver.py:2991 _save_disk_cache.
    """
    try:
        DISK_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = DISK_CACHE_FILE.with_suffix(DISK_CACHE_FILE.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
        tmp.replace(DISK_CACHE_FILE)
        return True
    except OSError:
        return False


def update_disk_cache_for_channel(channel: Dict[str, Any]) -> Dict[str, Any]:
    """Re-walk the channel folder, update its cache entry, save, and return
    the new stats. Mirrors YTArchiver.py:3136 _update_disk_cache_for_channel.
    """
    from .ytarchiver_config import load_config
    cfg = load_config()
    base = (cfg.get("output_dir") or "").strip()
    if not base:
        return {"n_vids": 0, "size_bytes": 0, "size_gb": 0.0}
    from pathlib import Path as _P
    n_vids, total_bytes = scan_channel_folder(_P(base), channel)
    cache = load_disk_cache()
    url = channel.get("url", "").strip()
    if url:
        cache[url] = {
            "num_vids": int(n_vids),
            "size_bytes": int(total_bytes),
            "last_updated": time.time(),
        }
        save_disk_cache(cache)
    return {"n_vids": n_vids,
            "size_bytes": total_bytes,
            "size_gb": total_bytes / (1024 ** 3)}


def stats_for_channel(channel: Dict[str, Any], cache: Optional[Dict[str, Any]] = None
                      ) -> Dict[str, Any]:
    """Return stats for one channel: {n_vids, size_gb, size_bytes, cached, stale_secs}."""
    if cache is None:
        cache = load_disk_cache()
    url = channel.get("url", "").strip()
    rec = cache.get(url)
    if not rec:
        return {"n_vids": 0, "size_bytes": 0, "size_gb": 0.0,
                "cached": False, "stale_secs": None}
    n_vids = int(rec.get("num_vids", 0))
    size_bytes = int(rec.get("size_bytes", 0))
    last = float(rec.get("last_updated", 0) or 0)
    stale = (time.time() - last) if last > 0 else None
    return {
        "n_vids": n_vids,
        "size_bytes": size_bytes,
        "size_gb": size_bytes / (1024 ** 3),
        "cached": True,
        "stale_secs": stale,
    }


def enrich_channels_with_stats(channels: list, cache: Optional[Dict[str, Any]] = None) -> list:
    """Attach n_vids / size_gb to each channel dict in-place. Returns the list."""
    if cache is None:
        cache = load_disk_cache()
    for ch in channels:
        st = stats_for_channel(ch, cache)
        ch["n_vids"] = st["n_vids"]
        ch["size_bytes"] = st["size_bytes"]
        ch["size_gb"] = st["size_gb"]
    return channels


def invalidate_channel(ch_url: str) -> bool:
    """Drop the disk-cache entry and kick off a background rescan so the
    Subs table re-reads fresh `num_vids` / `size_bytes` instead of the
    `—` placeholder it would otherwise show until the next startup walk.

    issue #134: previously this only popped the entry. The Subs
    row then rendered as "—" because `stats_for_channel` returns zeros on
    a missing entry. Worse, the next sweep_new_videos pass would stamp
    `sweep_fingerprint` into the now-empty entry (via `setdefault`), and
    the startup staleness check (24h) would skip the full rescan — so
    the "—" persisted across restarts until the user manually invoked a
    rebuild. Re-scanning synchronously in a daemon thread mirrors the
    classic YTArchiver.py:3198 _invalidate_channel_disk_cache behavior.
    """
    if not ch_url:
        return False
    cache = load_disk_cache()
    if ch_url in cache:
        del cache[ch_url]
        save_disk_cache(cache)

    def _rescan():
        try:
            from .ytarchiver_config import load_config
            cfg = load_config()
            ch = next(
                (c for c in cfg.get("channels", [])
                 if (c.get("url") or "").strip() == ch_url),
                None)
            if ch is not None:
                update_disk_cache_for_channel(ch)
        except Exception:
            pass

    import threading as _th
    _th.Thread(target=_rescan, daemon=True).start()
    return True


def heal_malformed_cache_entries() -> int:
    """Drop cache entries that lack `num_vids`/`size_bytes` so the next
    Subs render triggers a proper rescan instead of showing `—`.

    Older code paths (sweep_new_videos's `setdefault(ch_url, {})` +
    fingerprint write) could leave behind entries containing only a
    `sweep_fingerprint` — no video count, no size. Those entries
    short-circuited the "missing" check in the startup disk walk and
    the fix loop in `stats_for_channel`, so affected channels showed
    blank Size / # Vids columns forever. Call this once at startup to
    purge the bad rows; `sweep_new_videos` itself is also patched to
    stop creating them on new runs.

    Returns the number of entries dropped.
    """
    cache = load_disk_cache()
    dropped = [
        url for url, rec in list(cache.items())
        if not isinstance(rec, dict)
           or "num_vids" not in rec or "size_bytes" not in rec
    ]
    if not dropped:
        return 0
    for url in dropped:
        del cache[url]
    save_disk_cache(cache)
    return len(dropped)


def archive_totals(cache: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Aggregate totals across every channel in the cache."""
    if cache is None:
        cache = load_disk_cache()
    total_vids = 0
    total_bytes = 0
    n_channels = 0
    for url, rec in cache.items():
        if not isinstance(rec, dict):
            continue
        n_channels += 1
        total_vids += int(rec.get("num_vids", 0))
        total_bytes += int(rec.get("size_bytes", 0))
    return {
        "channels": n_channels,
        "videos": total_vids,
        "size_bytes": total_bytes,
        "size_gb": total_bytes / (1024 ** 3),
    }


# ── Fallback filesystem walk ───────────────────────────────────────────

def _channel_folder_name(ch: Dict[str, Any]) -> str:
    """Best-guess folder name for a channel (matches YTArchiver's default).

    Priority matches the original (YTArchiver.py:2799):
      folder_override → folder → name
    folder_override exists when the on-disk folder was renamed after the
    channel display name changed (e.g. "Valve News Network" → "Tyler McVicker").
    """
    return (ch.get("folder_override")
            or ch.get("folder")
            or ch.get("name")
            or "").strip()


def scan_channel_folder(base_dir: Path, channel: Dict[str, Any]) -> Tuple[int, int]:
    """Walk a channel's folder, return (num_vids, total_bytes).

    Mirrors YTArchiver.py:3012 _scan_channel_disk_info for the two counts we need.
    """
    folder_name = _channel_folder_name(channel)
    if not folder_name:
        return (0, 0)
    ch_folder = base_dir / folder_name
    if not ch_folder.is_dir():
        return (0, 0)
    n_vids = 0
    total = 0
    zero_byte = 0
    for dp, _dns, fns in os.walk(ch_folder):
        for fn in fns:
            if not fn.lower().endswith(_CHANNEL_VIDEO_EXTS):
                continue
            if _is_partial(fn):
                continue
            fp = os.path.join(dp, fn)
            try:
                size = os.path.getsize(fp)
                # Skip 0-byte phantom files (failed downloads that
                # left an empty placeholder). Counting them would
                # inflate the per-channel video count vs what the
                # grid actually renders.
                if size == 0:
                    zero_byte += 1
                    continue
                total += size
                n_vids += 1
            except OSError:
                pass
    # Bug [24]: surface 0-byte files instead of silently dropping them.
    # Previously this was completely invisible — a channel with 100
    # zero-byte placeholders showed clean stats while the orphans
    # lingered on disk. Print to stdout so it shows in the activity log.
    if zero_byte:
        try:
            print(f"[archive_scan] {folder_name}: skipped {zero_byte} "
                  f"0-byte phantom video file(s) (likely interrupted "
                  f"downloads — safe to delete or re-sync)")
        except Exception:
            pass
    # audit D-50: subtract any rows the FTS DB has flagged as
    # duplicates for this channel. list_videos_for_channel filters
    # `is_duplicate_of IS NULL`, so after a prune the Browse grid
    # shows (n_vids - duplicates) while this function returned
    # the raw on-disk count — leaving the Subs "videos" column
    # and Browse grid disagreeing silently. Now both views agree.
    try:
        from . import index as _idx
        _conn = _idx._open()
        if _conn is not None:
            ch_name = (channel.get("name") or channel.get("folder")
                       or "").strip()
            if ch_name:
                with _idx._db_lock:
                    _dup_row = _conn.execute(
                        "SELECT COUNT(*) FROM videos "
                        "WHERE channel=? COLLATE NOCASE "
                        "AND is_duplicate_of IS NOT NULL",
                        (ch_name,)).fetchone()
                if _dup_row:
                    _n_dup = int(_dup_row[0] or 0)
                    n_vids = max(0, n_vids - _n_dup)
    except Exception:
        pass
    return (n_vids, total)


def scan_all_channels(progress_cb=None) -> Dict[str, Dict[str, Any]]:
    """Walk the entire archive. Slow — use only when cache is missing/stale.

    progress_cb(current_ch_name: str, done: int, total: int) — optional.
    """
    cfg = load_config()
    base_str = (cfg.get("output_dir") or "").strip()
    if not base_str:
        return {}
    base_dir = Path(base_str)
    channels = cfg.get("channels", [])
    result: Dict[str, Dict[str, Any]] = {}
    now = time.time()
    total = len(channels)
    for i, ch in enumerate(channels):
        if progress_cb:
            try:
                progress_cb(ch.get("name", ""), i, total)
            except Exception:
                pass
        n_vids, size_bytes = scan_channel_folder(base_dir, ch)
        url = ch.get("url", "").strip()
        if url:
            result[url] = {
                "num_vids": n_vids,
                "size_bytes": size_bytes,
                "last_updated": now,
            }
    return result


# ── Index tab summary ──────────────────────────────────────────────────

def index_summary() -> Dict[str, Any]:
    """Return stats for the Browse > Index sub-mode.

    Provides per-card counters plus a per-channel table.
    """
    cfg = load_config()
    cache = load_disk_cache()
    channels = cfg.get("channels", [])

    tot = archive_totals(cache)
    # Count how many channels have auto_transcribe ON
    transcribed_channels = sum(1 for c in channels if c.get("auto_transcribe"))
    # Pull index-DB-side stats: segments count, total hours of indexed
    # video, and the actual .db file size on disk. The Index Statistics
    # panel is meant to describe the SEARCHABLE INDEX, not the underlying
    # archive — so the panel shows .db size here, not the multi-TB
    # archive size (which is surfaced elsewhere via the Browse grid).
    segments_count = 0
    hours = 0.0
    index_db_bytes = 0
    try:
        from . import index as _idx
        from .ytarchiver_config import TRANSCRIPTION_DB as _DB
        if _DB.exists():
            try:
                index_db_bytes = _DB.stat().st_size
            except OSError:
                pass
        _conn = _idx._open()
        if _conn is not None:
            with _idx._db_lock:
                _row = _conn.execute("SELECT COUNT(*) FROM segments").fetchone()
                if _row:
                    segments_count = int(_row[0] or 0)
                # Hours of video — prefer the summed videos.duration_s
                # column (populated by register_video). For rows where
                # duration_s is NULL, fall back to the maximum
                # segments.end_time per video so transcribed videos
                # without explicit duration metadata still contribute.
                _row = _conn.execute(
                    "SELECT COALESCE(SUM(duration_s), 0) FROM videos "
                    "WHERE duration_s IS NOT NULL").fetchone()
                if _row:
                    hours += float(_row[0] or 0) / 3600.0
                _row = _conn.execute(
                    "SELECT COALESCE(SUM(max_end), 0) FROM ("
                    "  SELECT MAX(s.end_time) AS max_end "
                    "  FROM segments s "
                    "  LEFT JOIN videos v ON v.video_id = s.video_id "
                    "  WHERE v.duration_s IS NULL OR v.video_id IS NULL "
                    "  GROUP BY s.video_id"
                    ")").fetchone()
                if _row:
                    hours += float(_row[0] or 0) / 3600.0
    except Exception:
        pass
    per_channel = []
    for ch in channels:
        st = stats_for_channel(ch, cache)
        per_channel.append({
            "folder": ch.get("name") or ch.get("folder", ""),
            "n_vids": st["n_vids"],
            "size_gb": st["size_gb"],
            "size": _fmt_size(st["size_bytes"]),
            "auto_transcribe": bool(ch.get("auto_transcribe")),
        })
    per_channel.sort(key=lambda r: (-r["size_gb"], (r["folder"] or "").lower()))
    return {
        "cards": {
            "channels": len(channels),
            "videos": tot["videos"],
            "size_gb": tot["size_gb"],
            "size_label": _fmt_size(tot["size_bytes"]),
            # New: index-DB-side stats. The frontend renders these as
            # the Index Statistics panel; archive_size is still returned
            # for callers that want it but isn't shown in the Index panel.
            "segments": segments_count,
            "hours": round(hours, 1) if hours > 0 else 0,
            "index_db_bytes": index_db_bytes,
            "index_db_size_label": _fmt_size(index_db_bytes),
            "transcribed_channels": transcribed_channels,
            "transcribed_pct_channels":
                (transcribed_channels * 100.0 / len(channels)) if channels else 0.0,
        },
        "per_channel": per_channel,
    }


def _fmt_size(b: int) -> str:
    if b <= 0:
        return "\u2014"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    v = float(b)
    while v >= 1024 and i < len(units) - 1:
        v /= 1024
        i += 1
    return f"{v:.1f} {units[i]}" if i >= 2 else f"{int(v)} {units[i]}"
