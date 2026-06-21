"""
metadata.scan — channel folder enumeration + .info.json reader.

Patch 19 phase M2 (v69.1): extracted from metadata/legacy.py.

Public surface (re-imported into legacy.py):
    _VIDEO_EXTS                  tuple of video file extensions we track
    _scan_videos_cache           per-folder fingerprint cache
    _scan_videos_cache_lock      its lock
    _scan_channel_videos(folder) walk + return [(vid, title, y, m, fp), ...]
    _group_by_metadata_path(...) bucket scan results by JSONL path
    _read_info_json_vid(fp)      read .info.json sidecar → video_id

The SQL ESCAPE fix from v68.7 lives in this module: `_scan_channel_videos`
queries the videos table with `LIKE ? ESCAPE '\\'` (proper escape).
The earlier `'\'` form parsed to `ESCAPE ''` and silently 500'd every call,
which is what caused the Thumbnails 0% regression.
"""
from __future__ import annotations

import json
import os
import re
import threading
from pathlib import Path
from typing import Any

from ..log import get_logger
from .io import _get_metadata_jsonl_path, _year_month_from_path
from ..thumbnails import _channel_fingerprint
from ..utils import sqlite_like_escape as _like_esc

_log = get_logger(__name__)


# Extensions we treat as videos (or audio rips). yt-dlp can produce any
# of these as the final output depending on the channel's resolution
# pref (e.g. "audio" mode lands a .m4a, "best" can land a .webm if no
# H.264 source exists).
from ..fs_search import MEDIA_EXTS_TUPLE as _VIDEO_EXTS  # unified media set

# Per-folder fingerprint cache. Adding a new download bumps the folder
# mtime, so the fingerprint changes naturally and the cache invalidates.
# Bounded to _SCAN_CACHE_MAX entries to prevent slow leak across days of
# runtime when many one-off folders (renamed channels, scratch dirs) get
# scanned (audit: metadata/scan.py:78-83). Eviction policy: when the cap
# is hit on insert, drop the oldest insertion-order entry. dict in
# Python 3.7+ preserves insertion order, so iterating `next(iter(d))`
# returns the oldest key.
_scan_videos_cache: dict[str, tuple[float, list]] = {}
_scan_videos_cache_lock = threading.Lock()
_SCAN_CACHE_MAX = 256


def _scan_channel_videos(folder: Path) -> list[tuple[str, str, int | None, int | None, str]]:
    """Walk `folder` and yield (video_id, title, year, month, filepath)
    for every video file.

    video_id lookup priority:
      1. Trailing `[id]` bracket in filename (legacy naming).
      2. Index DB's `videos` table via filepath (current + OLD's actual
         naming — `%(title)s.%(ext)s` with NO id bracket, so the DB is
         the only mapping). users hit this: the metadata recheck saw
         all 642 playlist IDs as "not on disk" because this function
         returned vid_id="" for every file (no bracket to parse), so
         the `by_id` map was empty, the caller treated every ID as
         new, and 642 blank-title log rows scrolled past.

    Year/month come from the file's OWN folder (via `_year_month_from_path`)
    so the metadata JSONL + thumbnail bucket co-locates with the mp4. The
    download foldered it by `upload_date`; reading the folder back avoids
    yt-dlp's `--mtime` drifting into a different month for premieres /
    scheduled uploads. Falls back to UTC mtime for flat layouts.
    """
    out = []
    if not folder.is_dir():
        return out
    # Patch D: fingerprint-cached early-return. If the folder's
    # recursive mtime fingerprint matches what we computed last time,
    # return the cached result instead of re-walking.
    _folder_str = str(folder)
    try:
        _fp = _channel_fingerprint(folder)
    except Exception:
        _fp = 0.0
    if _fp > 0:
        with _scan_videos_cache_lock:
            _cached = _scan_videos_cache.get(_folder_str)
        if _cached is not None and _cached[0] == _fp:
            return list(_cached[1])
    # Match a YouTube video_id bracket at the END of the stem. A
    # separate post-filter rejects matches that are pure letters —
    # a user's channel archive has 13 files with the filename
    # suffix `[a-user-channel]` (the channel name), which is exactly
    # 11 letters and matches the `[A-Za-z0-9_-]{11}` pattern. Real
    # YouTube video_ids are random 11-char picks from that set and
    # statistically always include at least one digit/_/-.
    bracket_re = re.compile(r"\[([A-Za-z0-9_-]{11})\]\s*$")
    _vid_looks_fake = lambda s: s.isalpha() # all-letters → not a YT id
    # Pre-load videos-table rows for this channel folder so we can
    # fill in missing video_ids without N queries.
    # Use the read-only connection so this scan doesn't contend with
    # sync's register_video writers on `_db_lock` — critical because
    # this function is called from the parallel thumbnail walker.
    #
    # v68.7 ESCAPE-clause fix: `ESCAPE '\\'` (proper escape for the
    # SQLite LIKE syntax). Earlier `'\'` parsed as `ESCAPE ''` (empty
    # char) which SQLite rejected with `OperationalError: ESCAPE
    # expression must be a single character` — exception swallowed,
    # `fp_to_id` stayed empty, every OLD-naming file got vid_id="".
    fp_to_id: dict[str, str] = {}
    try:
        from .. import index as _idx
        conn = _idx._reader_open() or _idx._open()
        if conn is not None:
            with _idx._reader_lock:
                rows = conn.execute(
                    "SELECT filepath, video_id FROM videos "
                    "WHERE filepath LIKE ? ESCAPE '\\'",
                    (_like_esc(str(folder)) + "%",)).fetchall()
            for fp, vid in rows:
                if fp and vid:
                    fp_to_id[os.path.normpath(fp).lower()] = vid
    except Exception as e:
        _log.debug("swallowed: %s", e)
    for dp, _dns, fns in os.walk(str(folder)):
        for fn in fns:
            low = fn.lower()
            if not low.endswith(_VIDEO_EXTS):
                continue
            if "_temp_compress" in low or low.endswith(".part"):
                continue
            fp = os.path.join(dp, fn)
            stem, _ext = os.path.splitext(fn)
            m = bracket_re.search(stem)
            vid_id = m.group(1) if m else ""
            if vid_id and _vid_looks_fake(vid_id):
                vid_id = ""
            if not vid_id:
                vid_id = fp_to_id.get(os.path.normpath(fp).lower(), "")
            # Strip the trailing `[...]` suffix from the title even
            # if it was a fake one — we still don't want it in the
            # title display.
            title = re.sub(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$", "", stem) or stem
            # Year/month from the file's OWN folder so the metadata JSONL +
            # thumbnail bucket tracks where the mp4 actually lives (matches
            # the download's upload_date foldering). Falls back to UTC mtime
            # for flat / unparseable layouts. See _year_month_from_path.
            year, month = _year_month_from_path(fp)
            out.append((vid_id, title, year, month, fp))
    if _fp > 0:
        with _scan_videos_cache_lock:
            # LRU-ish behavior via dict insertion order. If the key is
            # already present we re-key by popping first; this moves
            # it to the end (most-recent). If we're at the cap, drop
            # the oldest entry before insertion.
            try:
                if _folder_str in _scan_videos_cache:
                    del _scan_videos_cache[_folder_str]
                while len(_scan_videos_cache) >= _SCAN_CACHE_MAX:
                    _oldest = next(iter(_scan_videos_cache))
                    del _scan_videos_cache[_oldest]
            except Exception:
                pass
            _scan_videos_cache[_folder_str] = (_fp, list(out))
    return out


def _group_by_metadata_path(ch_name: str, folder_path: str,
                            split_years: bool, split_months: bool,
                            videos: list[tuple[str, str, int | None, int | None, str]]
                            ) -> dict[str, dict[str, Any]]:
    """Bucket videos by which aggregated .{ch} Metadata.jsonl they belong to.
    Returns {jsonl_path: {"subfolder":..., "videos":[...]}}.
    """
    groups: dict[str, dict[str, Any]] = {}
    for vid_id, title, y, m, fp in videos:
        jp, subf = _get_metadata_jsonl_path(
            ch_name, folder_path, split_years, split_months, y, m)
        g = groups.setdefault(jp, {"subfolder": subf, "videos": []})
        g["videos"].append((vid_id, title, y, m, fp))
    return groups


def _read_info_json_vid(filepath: str) -> str:
    """Return the video_id from a .info.json sidecar beside a video
    file (written by yt-dlp --write-info-json). Returns '' if no
    sidecar, the JSON doesn't parse, or the `id` field isn't a
    valid 11-char YouTube id.

    Checks the two common naming conventions:
      a) `<stem>.info.json` beside the video
      b) `<filename>.info.json` (rare but seen in legacy archives)
    """
    try:
        base_stem, _ = os.path.splitext(filepath)
        candidates = [
            base_stem + ".info.json",
            filepath + ".info.json",
        ]
        for sidecar in candidates:
            if not os.path.isfile(sidecar):
                continue
            try:
                with open(sidecar, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                continue
            raw = (data.get("id") or "").strip()
            if re.fullmatch(r"[A-Za-z0-9_-]{11}", raw):
                return raw
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return ""
