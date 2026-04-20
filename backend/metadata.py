"""
Metadata fetcher — writes OLD YTArchiver's aggregated format.

Output layout (must match YTArchiver.py exactly for drop-in replacement):

  .{ch_name} Metadata.jsonl                      (no split)
  {year}/.{ch_name} {year} Metadata.jsonl         (year-split)
  {year}/{MM Month}/.{ch_name} {Month} {YY} Metadata.jsonl   (year+month)

  Each file is a hidden (Windows HIDDEN attr) JSONL. One JSON per line,
  keyed by `video_id`. Dict schema per entry:
    {"video_id", "title", "description", "view_count", "like_count",
     "comment_count", "upload_date", "duration", "thumbnail_url",
     "comments":[{"author","text","likes","time"}, ...], "fetched_at"}

Thumbnails live next to the aggregated JSONL in a hidden `.Thumbnails/`
folder, one `.jpg` per video named `{title} [{video_id}].jpg`.

See YTArchiver.py:26539 (_get_metadata_jsonl_path), :26560 (read),
:26583 (write), :26719 (fetch per-video), :26784 (thumbnail download).
"""

from __future__ import annotations

import ctypes
import json
import os
import re
import subprocess
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .ytarchiver_config import load_config
from .log_stream import LogStreamer
from .sync import find_yt_dlp, sanitize_folder, _startupinfo, _find_cookie_source


# YouTube IDs are 11 chars of [A-Za-z0-9_-]
_ID_RE = re.compile(r"[A-Za-z0-9_-]{11}")

# Shared with transcribe.py + reorg.py — see backend.utils.MONTH_FOLDERS.
from .utils import MONTH_FOLDERS as _MONTH_NAMES, utf8_subprocess_env as _utf8_env


# ── OLD-compat helpers ──────────────────────────────────────────────────

def _hide_file_win(path: str) -> None:
    """Set Windows HIDDEN attribute. Matches YTArchiver.py:8499."""
    if os.name == "nt":
        try:
            ctypes.windll.kernel32.SetFileAttributesW(str(path), 0x02)
        except Exception:
            pass


def _unhide_file_win(path: str) -> None:
    """Clear Windows HIDDEN attribute before a write (so Python's `open` can
    truncate/rewrite without tripping Explorer's locked-file behavior)."""
    if os.name == "nt":
        try:
            ctypes.windll.kernel32.SetFileAttributesW(str(path), 0x80)  # NORMAL
        except Exception:
            pass


def _folder_for_channel(ch: Dict[str, Any]) -> Optional[Path]:
    cfg = load_config()
    base = (cfg.get("output_dir") or "").strip()
    if not base:
        return None
    folder_name = sanitize_folder((ch.get("folder_override") or "").strip()
                                  or ch.get("name", ""))
    # Refuse to point at the `_unnamed/` graveyard folder. Without this,
    # metadata writes would share the folder for any channel whose name
    # blanked out post-add — losing metadata attribution the same way
    # sync silently lost 28 videos during a prior sync pass. Callers that check for
    # None here surface the error; callers that blindly use the path
    # won't corrupt the archive because `_unnamed/` contains only
    # explicitly-moved residue.
    if not folder_name or folder_name == "_unnamed":
        return None
    return Path(base) / folder_name


def _get_metadata_jsonl_path(ch_name: str, folder_path: str,
                             split_years: bool, split_months: bool,
                             year: Optional[int] = None,
                             month: Optional[int] = None) -> Tuple[str, str]:
    """Mirror of YTArchiver.py:26539. Returns (jsonl_path, subfolder)."""
    if not split_years:
        fname = f".{ch_name} Metadata.jsonl"
        return (os.path.join(folder_path, fname), folder_path)
    if split_years and split_months and year and month:
        month_num = int(month) if isinstance(month, str) and str(month).isdigit() else month
        month_full = _MONTH_NAMES.get(month_num, f"{month_num:02d} Unknown")
        month_name = month_full.split(" ", 1)[1]
        yr_short = str(year)[-2:]
        subfolder = os.path.join(folder_path, str(year), month_full)
        fname = f".{ch_name} {month_name} {yr_short} Metadata.jsonl"
        return (os.path.join(subfolder, fname), subfolder)
    if split_years and year:
        subfolder = os.path.join(folder_path, str(year))
        fname = f".{ch_name} {year} Metadata.jsonl"
        return (os.path.join(subfolder, fname), subfolder)
    fname = f".{ch_name} Metadata.jsonl"
    return (os.path.join(folder_path, fname), folder_path)


def _read_metadata_jsonl(jsonl_path: str) -> Dict[str, Dict[str, Any]]:
    """Load aggregated metadata JSONL into {video_id: entry}.
    Matches YTArchiver.py:26560."""
    existing: Dict[str, Dict[str, Any]] = {}
    if not os.path.isfile(jsonl_path):
        return existing
    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    vid = entry.get("video_id", "")
                    if vid:
                        existing[vid] = entry
                except json.JSONDecodeError:
                    continue
    except Exception:
        pass
    return existing


def _write_metadata_jsonl(jsonl_path: str,
                          entries_dict: Dict[str, Dict[str, Any]]) -> None:
    """Write all entries (whole dict) to the aggregated JSONL, hiding on Win.
    Matches YTArchiver.py:26583."""
    os.makedirs(os.path.dirname(jsonl_path) or ".", exist_ok=True)
    if os.name == "nt" and os.path.isfile(jsonl_path):
        _unhide_file_win(jsonl_path)
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for _vid, data in entries_dict.items():
            f.write(json.dumps(data, ensure_ascii=False) + "\n")
    _hide_file_win(jsonl_path)


def _ensure_thumbnails_dir(subfolder: str) -> str:
    """Create .Thumbnails/ inside subfolder, hide it on Windows, return the path."""
    thumb_dir = os.path.join(subfolder, ".Thumbnails")
    try:
        os.makedirs(thumb_dir, exist_ok=True)
    except OSError:
        return thumb_dir
    if os.name == "nt":
        try:
            ctypes.windll.kernel32.SetFileAttributesW(
                os.path.normpath(thumb_dir), 0x02)
        except Exception:
            pass
    return thumb_dir


def _download_thumbnail(url: str, thumb_dir: str,
                        title: str, video_id: str) -> None:
    """Download a thumbnail to `{thumb_dir}/{safe_title} [{video_id}].jpg`.
    Dedupes against an existing file with the same [{video_id}] bracket.
    Matches YTArchiver.py:26784 exactly."""
    if not url or not video_id:
        return
    safe_title = re.sub(r'[<>:"/\\|?*]', '_', title or "")[:100]
    fname = f"{safe_title} [{video_id}].jpg"
    fpath = os.path.join(thumb_dir, fname)
    if os.path.isfile(fpath):
        return

    # Dedup: if a thumb with this [{video_id}] already exists under a
    # different title (YT renamed the video), rename it instead of writing
    # a duplicate.
    try:
        if os.path.isdir(thumb_dir):
            bracket = f"[{video_id}]"
            for existing in os.listdir(thumb_dir):
                if not existing.lower().endswith(
                        (".jpg", ".jpeg", ".png", ".webp")):
                    continue
                if bracket in existing and existing != fname:
                    existing_path = os.path.join(thumb_dir, existing)
                    existing_ext = os.path.splitext(existing)[1]
                    new_fname = f"{safe_title} [{video_id}]{existing_ext}"
                    new_path = os.path.join(thumb_dir, new_fname)
                    try:
                        os.replace(existing_path, new_path)
                        return
                    except OSError:
                        pass
    except OSError:
        pass

    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            img_data = resp.read()
        with open(fpath, "wb") as f:
            f.write(img_data)
    except Exception:
        pass  # non-fatal


def _fetch_video_metadata(yt: str, video_id: str,
                          title_hint: str = "") -> Optional[Dict[str, Any]]:
    """Fetch metadata for a single video via yt-dlp --dump-json.
    Returns the OLD-schema dict, or None on failure.
    Matches YTArchiver.py:26719."""
    cmd = [
        yt,
        "--dump-json", "--no-download", "--no-warnings",
        "--ignore-errors", "--skip-download",
        "--write-comments",
        "--extractor-args",
        "youtube:comment_sort=top;max_comments=50,50,0,0",
        *_find_cookie_source(),
        f"https://www.youtube.com/watch?v={video_id}",
    ]
    try:
        proc = subprocess.Popen(
            cmd, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            encoding="utf-8", errors="replace",
            startupinfo=_startupinfo,
            env=_utf8_env(),
        )
    except OSError:
        return None
    try:
        stdout, _ = proc.communicate(timeout=120)
    except subprocess.TimeoutExpired:
        proc.kill()
        try: proc.communicate(timeout=5)
        except Exception: pass
        return None
    if proc.returncode != 0:
        return None

    # yt-dlp may print warnings before the JSON dump — slice to the first
    # balanced JSON object.
    js = stdout.find("{")
    je = stdout.rfind("}")
    if js < 0 or je <= js:
        return None
    try:
        data = json.loads(stdout[js:je + 1])
    except Exception:
        return None

    comments = []
    for c in (data.get("comments") or [])[:50]:
        comments.append({
            "author": c.get("author", ""),
            "text":   c.get("text", ""),
            "likes":  c.get("like_count", 0),
            "time":   c.get("timestamp") or c.get("time_text", ""),
        })

    return {
        "video_id":      video_id,
        "title":         data.get("title", title_hint),
        "description":   data.get("description", ""),
        "view_count":    data.get("view_count", 0),
        "like_count":    data.get("like_count", 0),
        "comment_count": data.get("comment_count", 0),
        "upload_date":   data.get("upload_date", ""),
        "duration":      data.get("duration", 0),
        "thumbnail_url": data.get("thumbnail", ""),
        "comments":      comments,
        "fetched_at":    datetime.now().isoformat(),
    }


# ── Video-file discovery (for bucketing into year/month groups) ─────────

_VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v",
               ".wav", ".mp3", ".m4a", ".flac")


def _scan_channel_videos(folder: Path) -> List[Tuple[str, str, Optional[int], Optional[int], str]]:
    """Walk `folder` and yield (video_id, title, year, month, filepath)
    for every video file.

    video_id lookup priority:
      1. Trailing `[id]` bracket in filename (legacy naming).
      2. Index DB's `videos` table via filepath (current + OLD's actual
         naming — `%(title)s.%(ext)s` with NO id bracket, so the DB is
         the only mapping). Scott hit this: the metadata recheck saw
         all 642 playlist IDs as "not on disk" because this function
         returned vid_id="" for every file (no bracket to parse), so
         the `by_id` map was empty, the caller treated every ID as
         new, and 642 blank-title log rows scrolled past.

    Year/month come from the file's mtime (yt-dlp `--mtime` makes mtime =
    upload date). This matches how OLD groups files for metadata writing.
    """
    out = []
    if not folder.is_dir():
        return out
    # Match a YouTube video_id bracket at the END of the stem. A
    # separate post-filter rejects matches that are pure letters —
    # Scott's ColdFusion archive has 13 files with the filename
    # suffix `[ColdFustion]` (the channel name), which is exactly
    # 11 letters and matches the `[A-Za-z0-9_-]{11}` pattern. Real
    # YouTube video_ids are random 11-char picks from that set and
    # statistically always include at least one digit/_/-.
    bracket_re = re.compile(r"\[([A-Za-z0-9_-]{11})\]\s*$")
    _vid_looks_fake = lambda s: s.isalpha()  # all-letters → not a YT id
    # Pre-load videos-table rows for this channel folder so we can
    # fill in missing video_ids without N queries.
    fp_to_id: Dict[str, str] = {}
    try:
        from . import index as _idx
        conn = _idx._open()
        if conn is not None:
            with _idx._db_lock:
                rows = conn.execute(
                    "SELECT filepath, video_id FROM videos "
                    "WHERE filepath LIKE ?",
                    (str(folder) + "%",)).fetchall()
            for fp, vid in rows:
                if fp and vid:
                    fp_to_id[os.path.normpath(fp).lower()] = vid
    except Exception:
        pass
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
            # if it was a fake one (ColdFustion) — we still don't
            # want it in the title display.
            title = re.sub(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$", "", stem) or stem
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(fp))
                year, month = mtime.year, mtime.month
            except OSError:
                year, month = None, None
            out.append((vid_id, title, year, month, fp))
    return out


def _group_by_metadata_path(ch_name: str, folder_path: str,
                            split_years: bool, split_months: bool,
                            videos: List[Tuple[str, str, Optional[int], Optional[int], str]]
                            ) -> Dict[str, Dict[str, Any]]:
    """Bucket videos by which aggregated .{ch} Metadata.jsonl they belong to.
    Returns {jsonl_path: {"subfolder":..., "videos":[...]}}.
    """
    groups: Dict[str, Dict[str, Any]] = {}
    for vid_id, title, y, m, fp in videos:
        jp, subf = _get_metadata_jsonl_path(
            ch_name, folder_path, split_years, split_months, y, m)
        g = groups.setdefault(jp, {"subfolder": subf, "videos": []})
        g["videos"].append((vid_id, title, y, m, fp))
    return groups


# ── Public API ──────────────────────────────────────────────────────────

def fetch_single_video_metadata(channel: Dict[str, Any],
                                video_id: str,
                                file_path: str,
                                title_hint: str,
                                stream: LogStreamer,
                                emit_inline_log: bool = True
                                ) -> Dict[str, Any]:
    """Fetch metadata for ONE just-downloaded video, inline per-video.

    Unlike `fetch_metadata_for_videos` (which walks the channel folder to
    group videos by year/month), this one "snipes" the exact video: we
    already know its file path and mtime, so we compute year/month from
    that and write straight to the correct aggregated JSONL.

    Emits one log line by default — "  \u2014 Metadata downloaded" — in
    pink, matching the format the user asked for:
        [Sync] ...
          Downloading Title...
          \u2014 \u2713 Title                Channel   04.18.26    (26 MB)
          \u2014 Metadata downloaded

    Called from sync.py's DLTRACK handler (dispatched to a background
    thread so it doesn't back-pressure the yt-dlp stdout reader).

    Returns {ok, fetched|skipped|error}.
    """
    if not video_id or not file_path:
        return {"ok": False, "error": "missing id or path"}

    folder = _folder_for_channel(channel)
    if folder is None:
        return {"ok": False, "error": "no output_dir"}

    yt = find_yt_dlp()
    if not yt:
        return {"ok": False, "error": "yt-dlp missing"}

    name = channel.get("name") or channel.get("folder") or "?"
    split_years  = bool(channel.get("split_years"))
    split_months = bool(channel.get("split_months"))

    # Compute year/month from file mtime — yt-dlp --mtime sets mtime to
    # the YouTube upload date, so this is authoritative. Falls back to
    # "now" if the file's somehow missing (shouldn't happen right after a
    # successful download).
    year: Optional[int] = None
    month: Optional[int] = None
    try:
        mt = datetime.fromtimestamp(os.path.getmtime(file_path))
        year, month = mt.year, mt.month
    except OSError:
        pass

    jp, subfolder = _get_metadata_jsonl_path(
        name, str(folder), split_years, split_months, year, month)

    existing = _read_metadata_jsonl(jp)
    if video_id in existing:
        # Already have metadata for this id — nothing to do. No log.
        return {"ok": True, "skipped": True}

    entry = _fetch_video_metadata(yt, video_id, title_hint)
    if entry is None:
        if emit_inline_log:
            stream.emit([
                ["  \u2014 ", "dim"],
                ["Metadata fetch failed\n", "red"],
            ])
        return {"ok": False, "error": "yt-dlp dump-json failed"}

    existing[video_id] = entry
    try:
        _write_metadata_jsonl(jp, existing)
    except Exception as e:
        return {"ok": False, "error": f"write failed: {e}"}

    # Thumbnail (best-effort).
    if entry.get("thumbnail_url"):
        thumb_dir = _ensure_thumbnails_dir(subfolder)
        _download_thumbnail(
            entry["thumbnail_url"], thumb_dir,
            title_hint or entry.get("title", ""), video_id)

    if emit_inline_log:
        # Per-video metadata done line. Matches the three-line simple-mode
        # summary spec Scott locked in:
        #     — ✓ <title>  —  <channel>  (size)        [download done,  green]
        #     — ✓ Transcription  (details)              [transcription done, blue]
        #     — ✓ Metadata downloaded                   [metadata done,  pink]
        # Pink em-dash + checkmark + "Metadata downloaded".
        stream.emit([
            ["  ",                     "dim"],
            ["\u2014 \u2713 ",         "meta_bracket"],
            ["Metadata downloaded\n",  "simpleline_pink"],
        ])
    return {"ok": True, "fetched": True}


def fetch_metadata_for_videos(channel: Dict[str, Any],
                              video_ids: Iterable[str],
                              stream: LogStreamer,
                              cancel_event: Optional[threading.Event] = None,
                              refresh: bool = False,
                              pause_event: Optional[threading.Event] = None
                              ) -> Dict[str, Any]:
    """Fetch metadata for the given video IDs into the aggregated JSONL(s).

    Also downloads each video's thumbnail into the corresponding
    `.Thumbnails/` subfolder. No per-video `.info.json` is written — this
    matches OLD YTArchiver.py:14212-14327.

    Handles year/month splitting: videos in different year folders write to
    different aggregated JSONLs.
    """
    ids = [vid.strip() for vid in video_ids if vid and vid.strip()]
    if not ids:
        return {"ok": True, "fetched": 0, "skipped": 0, "errors": 0}

    folder = _folder_for_channel(channel)
    if folder is None:
        stream.emit_error("Metadata: output_dir is not configured.")
        return {"ok": False, "error": "no output_dir"}
    folder.mkdir(parents=True, exist_ok=True)

    yt = find_yt_dlp()
    if not yt:
        stream.emit_error("Metadata: yt-dlp not found.")
        return {"ok": False, "error": "yt-dlp missing"}

    name = channel.get("name") or channel.get("folder") or "?"
    split_years  = bool(channel.get("split_years"))
    split_months = bool(channel.get("split_months"))

    # Walk the folder to find videos on disk matching these IDs — we need
    # their year/month to route each fetch to the correct aggregated JSONL.
    all_videos = _scan_channel_videos(folder)
    by_id = {v[0]: v for v in all_videos if v[0]}
    wanted = [by_id[vid] for vid in ids if vid in by_id]
    # Videos whose ID we want but aren't yet on disk — drop to the channel-root bucket.
    for vid in ids:
        if vid not in by_id:
            wanted.append((vid, "", None, None, ""))

    groups = _group_by_metadata_path(name, str(folder),
                                     split_years, split_months, wanted)

    stream.emit([
        ["[Metdta] ", "meta_bracket"],
        [f"{name} ",                    "simpleline"],
        ["\u2014 ",                     "meta_bracket"],
        [f"fast-fetch {len(ids)} id(s)\n", "simpleline"],
    ])

    total = sum(len(g["videos"]) for g in groups.values())
    t0 = time.time()
    fetched = skipped = errors = refreshed = 0
    idx = 0

    for jp, g in groups.items():
        if cancel_event is not None and cancel_event.is_set():
            break
        # Pause — bail out of the metadata walk the moment the user
        # hits Pause. Each fetch is a single HTTP call so partial
        # results are fine; re-running later picks up from where we
        # left off (via skip-existing).
        if pause_event is not None and pause_event.is_set():
            stream.emit([["  \u23F8 Paused \u2014 stopping metadata walk.\n",
                          "simpleline"]])
            break
        existing = _read_metadata_jsonl(jp)
        thumb_dir = _ensure_thumbnails_dir(g["subfolder"])
        changed = False

        def _has_thumbnail_for(vid: str) -> bool:
            """Check if any thumbnail file in this group's .Thumbnails
            folder matches `[vid]`. Scott's case: 2 videos had metadata
            but no thumbnail — the old skip-if-in-existing check treated
            those as "complete" and never re-downloaded the thumbnail."""
            if not vid or not os.path.isdir(thumb_dir):
                return False
            bracket = f"[{vid}]"
            try:
                for _fn in os.listdir(thumb_dir):
                    if bracket in _fn and _fn.lower().endswith(
                            (".jpg", ".jpeg", ".png", ".webp")):
                        return True
            except OSError:
                pass
            return False

        for vid_id, title, _y, _m, _fp in g["videos"]:
            if cancel_event is not None and cancel_event.is_set():
                break
            if pause_event is not None and pause_event.is_set():
                break
            if not vid_id:
                errors += 1
                continue
            idx += 1
            is_refresh_hit = vid_id in existing and refresh
            # Thumbnail-only gap: metadata exists but the image file
            # doesn't. Re-run the API fetch so we get a fresh
            # thumbnail_url and download it, but don't overwrite the
            # existing JSONL entry (it's fine as-is).
            needs_thumb_only = (vid_id in existing and not refresh
                                and not _has_thumbnail_for(vid_id))
            if vid_id in existing and not refresh and not needs_thumb_only:
                skipped += 1
                continue
            _reason = ("Thumbnail" if needs_thumb_only else "Metadata")
            # Color discipline: only the pink parts are the ones that
            # identify the task source (brackets, em-dash, tag label).
            # Numbers and titles render in the default color so they
            # read clearly. Scott's rule.
            stream.emit([
                ["  [",            "meta_bracket"],
                [str(idx),         "simpleline"],
                ["/",              "meta_bracket"],
                [str(total),       "simpleline"],
                ["] ",             "meta_bracket"],
                [_reason,          "simpleline_pink"],
                [" \u2014 ",       "meta_bracket"],
                [f"{title[:90]}\n", "simpleline"],
            ])
            entry = _fetch_video_metadata(yt, vid_id, title)
            if entry is None:
                errors += 1
                # Mark this video_id as permanently failed so future
                # rechecks don't re-hit yt-dlp for it. Matches OLD's
                # `metadata_fetch_failed_ts` pattern — cleared on
                # refresh=True via fetch_channel_metadata's purge.
                try:
                    from . import index as _idx
                    conn = _idx._open()
                    if conn is not None:
                        with _idx._db_lock:
                            conn.execute(
                                "UPDATE videos SET metadata_fetch_failed_ts=? "
                                "WHERE video_id=?",
                                (time.time(), vid_id))
                            conn.commit()
                except Exception:
                    pass
                continue
            if is_refresh_hit:
                # Merge: update counts + comments, keep other fields
                old = existing[vid_id]
                old["view_count"]    = entry.get("view_count", old.get("view_count", 0))
                old["like_count"]    = entry.get("like_count", old.get("like_count", 0))
                old["comment_count"] = entry.get("comment_count", old.get("comment_count", 0))
                old["comments"]      = entry.get("comments", old.get("comments", []))
                old["fetched_at"]    = entry.get("fetched_at", "")
                if entry.get("thumbnail_url"):
                    old["thumbnail_url"] = entry["thumbnail_url"]
                refreshed += 1
                changed = True
            elif needs_thumb_only:
                # JSONL entry stays as-is; only the thumbnail is being
                # backfilled. `changed` stays False so we don't rewrite
                # the JSONL for a thumbnail-only fetch.
                fetched += 1
            else:
                existing[vid_id] = entry
                fetched += 1
                changed = True
            if entry.get("thumbnail_url"):
                _download_thumbnail(entry["thumbnail_url"], thumb_dir,
                                    title or entry.get("title", ""), vid_id)

        if changed:
            try:
                _write_metadata_jsonl(jp, existing)
            except Exception as e:
                stream.emit_error(f"Could not write {jp}: {e}")

    elapsed = time.time() - t0
    summary_parts = []
    if fetched:   summary_parts.append(f"{fetched} fetched")
    if refreshed: summary_parts.append(f"{refreshed} refreshed")
    if skipped:   summary_parts.append(f"{skipped} skipped")
    if errors:    summary_parts.append(f"{errors} errors")
    summary = " \u00b7 ".join(summary_parts) or "nothing to do"
    stream.emit([
        ["  \u2013 ", "dim"],
        [f"Metadata {name} \u2014 ", "simpleline"],
        [summary, "dim"],
        [f" \u00b7 took {elapsed:.1f}s\n", "dim"],
    ])
    # Persist to autorun history + emit activity-log row so the user sees
    # [Metdta] entries with pink coloring (matches OLD _record_metadata at
    # YTArchiver.py:22633). Only persist when something actually happened —
    # the "history of work done, not noisy status feed" rule.
    if fetched or refreshed or errors:
        try:
            from . import autorun as _ar
            # Metdta primary: "N fetched" when new arrivals, else "N refreshed".
            # Secondary column uses "existing" label (OLD:22657) instead of "skipped".
            if refreshed and not fetched:
                primary = f"{refreshed} refreshed"
            else:
                primary = f"{fetched} fetched"
            _ar.append_history_entry(
                _ar.format_history_entry("Metdta", name, primary,
                                         secondary=f"{skipped} existing",
                                         errors=errors, took_sec=elapsed))
        except Exception:
            pass
        # Activity log row — coloured pink via _hist_tag_for_kind
        try:
            now = datetime.now()
            time_str = now.strftime("%I:%M%p").lstrip("0").lower()
            date_str = now.strftime("%b %d").replace(" 0", " ")
            primary_ui = (f"{refreshed} refreshed" if refreshed and not fetched
                          else f"{fetched} fetched")
            row_tag = "hist_pink" if (fetched or refreshed) else ""
            # Use the same `Xm Ys` helper as the rest of the activity
            # log so a 215-second run reads as "took 3m 35s" rather
            # than "took 215.6s" (Scott's request — matches OLD
            # YTArchiver's compact duration format).
            _el_int = int(elapsed or 0)
            if _el_int < 60:
                _took_str = f"{_el_int}s"
            elif _el_int < 3600:
                _took_str = f"{_el_int // 60}m {_el_int % 60}s"
            else:
                _took_str = f"{_el_int // 3600}h {(_el_int % 3600) // 60}m"
            stream.emit_activity({
                "kind":      "Metdta",
                "time_date": f"{time_str}, {date_str}",
                "channel":   name,
                "primary":   primary_ui,
                "secondary": f"{skipped} existing",
                "errors":    f"{errors} errors",
                "took":      f"took {_took_str}",
                "row_tag":   row_tag,
            })
        except Exception:
            pass
    return {"ok": True, "fetched": fetched, "skipped": skipped,
            "errors": errors, "refreshed": refreshed, "took": elapsed}


def _resolve_ids_by_title(yt: str, url: str,
                          unmatched_files: List[str],
                          stream: LogStreamer,
                          cancel_event: Optional[threading.Event] = None,
                          pause_event: Optional[threading.Event] = None
                          ) -> Dict[str, str]:
    """Walk the channel's playlist ONCE to fetch (id, title) pairs,
    then match the unmatched filepaths to playlist entries by
    normalized title. Returns {filepath: video_id}.

    This is the fallback for files that were dropped into a channel
    folder without id-bearing filenames and without a normal sync
    pass (which is how the index DB normally learns the id). By
    matching on title we rescue them without requiring the user to
    rename files. Expensive enough (one yt-dlp playlist walk) that
    we only fire it when there's at least one unmatched file.
    """
    if not url or not unmatched_files:
        return {}
    try:
        proc = subprocess.Popen(
            [yt, "--flat-playlist",
             "--print", "%(id)s\t%(title)s",
             *_find_cookie_source(), url],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            encoding="utf-8", errors="replace",
            bufsize=1, startupinfo=_startupinfo,
            env=_utf8_env(),
        )
    except OSError:
        return {}
    # Collect playlist entries as title → list of ids so duplicate
    # titles (rare but possible — e.g. a re-uploaded video with the
    # same title as the original) don't silently overwrite each other.
    playlist: Dict[str, list] = {}
    try:
        for line in proc.stdout:
            if cancel_event is not None and cancel_event.is_set():
                try: proc.terminate()
                except Exception: pass
                break
            if pause_event is not None and pause_event.is_set():
                try: proc.terminate()
                except Exception: pass
                break
            parts = line.rstrip().split("\t", 1)
            if len(parts) != 2:
                continue
            vid, title = parts[0].strip(), parts[1].strip()
            if _ID_RE.fullmatch(vid) and title:
                key = _normalize_title_for_match(title)
                playlist.setdefault(key, []).append(vid)
        try: proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.terminate()
    except Exception:
        try: proc.terminate()
        except Exception: pass
    # Also group unmatched files by normalized title so we never
    # assign the same id to multiple files. Scott's Apple Explained
    # case: `History of the iPhone (1).mp4`, `(2).mp4`, etc. — if we
    # stripped the `(N)` suffix they'd all collide onto one
    # playlist id and we'd silently duplicate. We don't strip, and
    # we ONLY match when BOTH sides are unambiguous (exactly one
    # file + exactly one playlist id for the same key).
    files_by_key: Dict[str, list] = {}
    for fp in unmatched_files:
        stem = os.path.splitext(os.path.basename(fp))[0]
        key = _normalize_title_for_match(stem)
        files_by_key.setdefault(key, []).append(os.path.normpath(fp))

    # Title-match assigns the id even when it's already claimed by a
    # different file — this is exactly the "same YouTube video got
    # downloaded twice under different titles" case (YouTuber renamed
    # the video; the old download sits on disk with old title, new
    # download under new title). Both files get the same id, then
    # `prune_missing_videos` resolves the duplicate: keeps the row
    # with the largest `size_bytes` as the primary and marks the
    # others as duplicates (`is_duplicate_of=<primary filepath>`) so
    # the Browse grid hides them but the files stay on disk.
    out: Dict[str, str] = {}
    for key, files in files_by_key.items():
        vids = playlist.get(key, [])
        if len(files) == 1 and len(vids) == 1:
            out[files[0]] = vids[0]
        # else: genuinely ambiguous (multiple files with same title AND
        # multiple playlist entries with same title) — skip.
    return out


def _normalize_title_for_match(title: str) -> str:
    """Normalize a title for fuzzy matching between playlist titles
    and filenames. Strips whitespace + common Windows-illegal-char
    substitutions that the filename might carry but the YouTube
    title doesn't (and vice versa), and lowercases. yt-dlp replaces
    `?` `:` `|` etc. with similar-looking unicode chars or `_` when
    writing filenames, so we strip those + collapse whitespace."""
    import re as _re
    t = title.lower()
    # Unify common substitutions
    t = t.replace("\uFF1A", ":").replace("\uFF1F", "?").replace("\uFF5C", "|")
    t = t.replace("\u2044", "/").replace("\uFF0F", "/")
    # Strip windows-illegal chars on both sides (filename can't have
    # these so playlist title gets stripped to match)
    t = _re.sub(r'[<>:"/\\|?*]', '', t)
    # Collapse whitespace
    t = _re.sub(r"\s+", " ", t).strip()
    return t


def fetch_channel_metadata(channel: Dict[str, Any],
                           stream: LogStreamer,
                           cancel_event: Optional[threading.Event] = None,
                           refresh: bool = False,
                           pause_event: Optional[threading.Event] = None
                           ) -> Dict[str, Any]:
    """Fill in missing metadata for this channel's on-disk videos.

    Two modes:
      - refresh=False (DEFAULT): DISK-DRIVEN. Enumerate videos on disk
        via `_scan_channel_videos` (filename `[id]` bracket first, then
        index-DB filepath lookup). Compare against existing JSONL IDs.
        Fetch only the missing handful. NO playlist walk — because the
        playlist would include ~hundreds of channel-videos that aren't
        downloaded, all of which are irrelevant for this job. Scott:
        "I scrolled through the entire channel, there's only two
        videos that are missing thumbnails. How did you even come up
        with 176?" — the 176 were undownloaded playlist entries my
        old code was pointlessly hitting.
      - refresh=True: "Refresh Counts" mode — re-hit every on-disk
        video's YouTube API (views/likes/comments change over time).
        Still disk-driven, but every id is a target.
    """
    folder = _folder_for_channel(channel)
    if folder is None:
        stream.emit_error("Metadata: output_dir is not configured.")
        return {"ok": False, "error": "no output_dir"}
    folder.mkdir(parents=True, exist_ok=True)

    yt = find_yt_dlp()
    if not yt:
        stream.emit_error("Metadata: yt-dlp not found.")
        return {"ok": False, "error": "yt-dlp missing"}

    name = channel.get("name") or channel.get("folder") or "?"
    stream.emit([["[Metdta] ", "meta_bracket"],
                 [f"Rechecking {name}...\n", "simpleline"]])

    # 1. Enumerate videos ON DISK. `_scan_channel_videos` returns
    #    (video_id, title, year, month, filepath) — video_id is
    #    filled in either from filename `[id]` bracket or from the
    #    index DB (filepath → video_id lookup).
    on_disk = _scan_channel_videos(folder)
    on_disk_ids = [v[0] for v in on_disk if v[0]]
    # Previously-failed fetches + previously-failed id-resolves —
    # OLD-YTArchiver compatible skip logic. Videos marked in the DB
    # as "already tried and it didn't work" are NOT retried this run.
    # On refresh=True we clear the flags and try again.
    _failed_fetch: set = set()
    _failed_id_files: set = set()
    try:
        from . import index as _idx
        conn = _idx._open()
        if conn is not None:
            ch_name = channel.get("name") or channel.get("folder") or ""
            with _idx._db_lock:
                if refresh:
                    conn.execute(
                        "UPDATE videos SET metadata_fetch_failed_ts=NULL, "
                        "id_resolve_failed_ts=NULL WHERE channel=?",
                        (ch_name,))
                    conn.commit()
                else:
                    for (_vid,) in conn.execute(
                            "SELECT video_id FROM videos WHERE channel=? "
                            "AND metadata_fetch_failed_ts IS NOT NULL "
                            "AND video_id IS NOT NULL AND video_id != ''",
                            (ch_name,)).fetchall():
                        _failed_fetch.add(_vid)
                    for (_fp,) in conn.execute(
                            "SELECT filepath FROM videos WHERE channel=? "
                            "AND id_resolve_failed_ts IS NOT NULL "
                            "AND (video_id IS NULL OR video_id='')",
                            (ch_name,)).fetchall():
                        if _fp:
                            _failed_id_files.add(os.path.normpath(_fp).lower())
    except Exception:
        pass
    # Files we couldn't resolve to a video_id. Skip ones we already
    # gave up on in a previous run.
    unmatched_files = [v[4] for v in on_disk if not v[0]
                        and os.path.normpath(v[4]).lower() not in _failed_id_files]
    n_without_id = len(unmatched_files)
    n_perm_no_id = sum(1 for v in on_disk if not v[0]
                        and os.path.normpath(v[4]).lower() in _failed_id_files)

    # 2. Read existing metadata JSONLs.
    have_meta: set = set()
    jsonl_count = 0
    for dp, _dns, fns in os.walk(str(folder)):
        for fn in fns:
            if fn.endswith("Metadata.jsonl"):
                jsonl_count += 1
                have_meta.update(_read_metadata_jsonl(os.path.join(dp, fn)).keys())

    # 3. Enumerate existing THUMBNAILS. Scott's case: 2 videos had
    #    metadata JSONL entries but no thumbnail file on disk — the
    #    earlier logic only checked metadata so those 2 showed as
    #    "complete" and weren't re-fetched. Thumbnails are stored as
    #    `<safe_title> [<video_id>].<ext>` inside `.Thumbnails/`
    #    subfolders (one per year/month split). We extract the
    #    bracketed video_id from each filename.
    have_thumb: set = set()
    thumb_file_count = 0
    _thumb_id_re = re.compile(r"\[([A-Za-z0-9_-]{11})\]")
    for dp, _dns, fns in os.walk(str(folder)):
        # Only look in .Thumbnails/ folders — avoids picking up a
        # bracketed id from an unrelated file elsewhere in the tree.
        if os.path.basename(dp).lower() != ".thumbnails":
            continue
        for fn in fns:
            low = fn.lower()
            if not low.endswith((".jpg", ".jpeg", ".png", ".webp")):
                continue
            thumb_file_count += 1
            m = _thumb_id_re.search(fn)
            if m:
                have_thumb.add(m.group(1))

    # 4. Compute targets: missing metadata OR missing thumbnail.
    #    Dedupe via dict (preserves insertion order) so a video whose
    #    id accidentally appears multiple times in `on_disk_ids`
    #    (historical bug: 13 ColdFusion files all got assigned the
    #    fake id "ColdFustion" from filename-suffix parsing)
    #    doesn't produce 13 identical fetch attempts.
    #    Skip videos whose metadata fetch previously failed (deleted /
    #    private / region-locked) — `_failed_fetch` set comes from
    #    the DB and gets cleared on refresh=True.
    seen: set = set()
    deduped_ids: list = []
    for _vid in on_disk_ids:
        if _vid not in seen:
            seen.add(_vid)
            deduped_ids.append(_vid)
    if refresh:
        targets = list(deduped_ids)
    else:
        targets = [vid for vid in deduped_ids
                   if (vid not in have_meta or vid not in have_thumb)
                   and vid not in _failed_fetch]

    # Breakdown so the user can see exactly what the scan found
    # (metadata coverage vs thumbnail coverage are now reported
    # separately). Only the em-dash prefix is pink — title/number
    # content stays default (simpleline) so it reads clearly against
    # the dark background. Scott's rule: "colored em dash indicating
    # what task it is from. Any brackets or anything should be pink.
    # The actual metadata tag should be pink but no actual title or
    # numbers or anything should be pink."
    missing_meta = sum(1 for vid in on_disk_ids if vid not in have_meta)
    missing_thumb = sum(1 for vid in on_disk_ids if vid not in have_thumb)
    _perm_failed = sum(1 for vid in on_disk_ids if vid in _failed_fetch)
    _parts = [
        f"{len(on_disk):,} on disk \u00b7 "
        f"metadata: {len(have_meta):,}/{len(on_disk_ids):,} "
        f"({missing_meta} missing) \u00b7 "
        f"thumbnails: {len(have_thumb):,}/{len(on_disk_ids):,} "
        f"({missing_thumb} missing)"
    ]
    if _perm_failed:
        _parts.append(f"{_perm_failed:,} previously failed (skipped)")
    _parts.append(f"{len(targets):,} need fetching")
    stream.emit([
        ["  \u2014 ", "meta_bracket"],
        [" \u00b7 ".join(_parts) + "\n", "simpleline"],
    ])
    if n_without_id:
        # Some on-disk files couldn't be matched to a video_id via the
        # filename-bracket path or the index DB. Scott's case: 2 recent
        # videos were manually dropped into the 2026/ folder with
        # bracket-less filenames AND no corresponding DB row got
        # populated with an id. They show up with no thumbnail in the
        # grid, and without an id we can't fetch metadata either.
        # FALLBACK: walk the channel's YouTube playlist ONCE (one
        # yt-dlp --flat-playlist call) to grab every (id, title) pair
        # for this channel, then match unmatched files to playlist
        # entries by normalized title. Any matches get backfilled into
        # the index DB so this fallback doesn't need to run on next
        # recheck.
        stream.emit([
            ["  \u26A0 ", "meta_bracket"],
            [f"{n_without_id:,} on-disk video(s) couldn't be matched "
             f"to a video_id \u2014 checking channel playlist for id "
             f"match by title...\n", "simpleline"],
        ])
        url = channel.get("url", "").strip()
        resolved = _resolve_ids_by_title(
            yt, url, unmatched_files, stream, cancel_event, pause_event)
        if resolved:
            # Backfill the index DB. For each resolved (filepath → id)
            # pair: check if any OTHER DB row already claims this id.
            # If yes → this is a duplicate download of the same
            # YouTube video (YouTuber renamed the video, both
            # downloads sit on disk). Mark the smaller / newer copy
            # as `is_duplicate_of=<primary filepath>` so the Browse
            # grid hides it. If no → normal backfill, just populate
            # the id on the existing row.
            n_duplicates_flagged = 0
            try:
                from . import index as _idx
                conn = _idx._open()
                if conn is not None:
                    with _idx._db_lock:
                        for _fp, _vid in resolved.items():
                            existing = conn.execute(
                                "SELECT filepath, size_bytes FROM videos "
                                "WHERE video_id=? AND filepath != ? "
                                "COLLATE NOCASE",
                                (_vid, _fp)).fetchone()
                            if existing:
                                # Figure out which file (existing vs
                                # new) is primary. Keep the larger
                                # one as primary; flag the smaller
                                # as duplicate-of the primary.
                                existing_fp, existing_size = existing
                                try:
                                    new_size = os.path.getsize(_fp)
                                except OSError:
                                    new_size = 0
                                if new_size > (existing_size or 0):
                                    # New file wins — flip existing
                                    # row to duplicate, assign id to
                                    # new row.
                                    conn.execute(
                                        "UPDATE videos SET video_id=?, "
                                        "video_url=?, is_duplicate_of=NULL "
                                        "WHERE filepath=? COLLATE NOCASE",
                                        (_vid,
                                         f"https://www.youtube.com/watch?v={_vid}",
                                         _fp))
                                    conn.execute(
                                        "UPDATE videos SET is_duplicate_of=? "
                                        "WHERE filepath=? COLLATE NOCASE",
                                        (_fp, existing_fp))
                                else:
                                    # Existing wins — new row is the
                                    # duplicate.
                                    conn.execute(
                                        "UPDATE videos SET video_id=?, "
                                        "video_url=?, is_duplicate_of=? "
                                        "WHERE filepath=? COLLATE NOCASE",
                                        (_vid,
                                         f"https://www.youtube.com/watch?v={_vid}",
                                         existing_fp, _fp))
                                n_duplicates_flagged += 1
                            else:
                                conn.execute(
                                    "UPDATE videos SET video_id=?, video_url=? "
                                    "WHERE filepath=? COLLATE NOCASE",
                                    (_vid,
                                     f"https://www.youtube.com/watch?v={_vid}",
                                     _fp))
                        conn.commit()
            except Exception:
                pass
            if n_duplicates_flagged:
                stream.emit([
                    ["  \u26A0 ", "meta_bracket"],
                    [f"{n_duplicates_flagged:,} duplicate download(s) "
                     f"detected \u2014 hidden from grid (files still on "
                     f"disk).\n", "simpleline"],
                ])
                # Drop the Browse grid cache for this channel so the
                # next click on it queries fresh and reflects the
                # duplicate filtering.
                try:
                    from . import index as _idx
                    _idx.invalidate_channel_videos(
                        channel.get("name") or channel.get("folder", ""))
                except Exception:
                    pass
            stream.emit([
                ["  \u2713 ", "simpleline_green"],
                [f"Matched {len(resolved):,} of {n_without_id:,} "
                 f"by title \u2014 backfilled into index.\n",
                 "simpleline"],
            ])
            # Add the newly-resolved ids into our working sets so the
            # target calculation below picks them up for fetching.
            for _fp, _vid in resolved.items():
                on_disk_ids.append(_vid)
            # Re-enter the target computation with the new ids.
        still_unmatched = [fp for fp in unmatched_files
                            if os.path.normpath(fp) not in (resolved or {})]
        if still_unmatched:
            # Mark these files in the DB as "id-resolve-failed" so
            # future rechecks skip them instead of re-running the
            # playlist walk. Matches OLD YTArchiver's pattern where
            # unresolvable files stop wasting API calls after the
            # first attempt. `refresh=True` clears the flag above, so
            # the user can force a retry via "Refresh Counts".
            try:
                from . import index as _idx
                conn = _idx._open()
                if conn is not None:
                    _now = time.time()
                    with _idx._db_lock:
                        for _fp in still_unmatched:
                            conn.execute(
                                "UPDATE videos SET id_resolve_failed_ts=? "
                                "WHERE filepath=? COLLATE NOCASE",
                                (_now, os.path.normpath(_fp)))
                        conn.commit()
            except Exception:
                pass
            stream.emit([
                ["  \u2014 ", "meta_bracket"],
                [f"{len(still_unmatched):,} file(s) couldn't be matched "
                 f"even by playlist title (deleted? re-uploaded with "
                 f"new id? filename edited?). Marked as permanent skip.\n",
                 "simpleline"],
            ])
            for _fp in still_unmatched[:5]:
                stream.emit([
                    ["      \u2022 ", "meta_bracket"],
                    [f"{os.path.basename(_fp)}\n", "simpleline"],
                ])
            if len(still_unmatched) > 5:
                stream.emit([
                    ["      ", "dim"],
                    [f"\u2026 and {len(still_unmatched) - 5:,} more\n", "dim"],
                ])
    if n_perm_no_id:
        stream.emit([
            ["  \u2014 ", "meta_bracket"],
            [f"{n_perm_no_id:,} file(s) previously marked as "
             f"unresolvable \u2014 skipping (use Refresh to retry).\n",
             "simpleline"],
        ])

        # Recompute targets with the newly-resolved ids in scope.
        if refresh:
            targets = list(on_disk_ids)
        else:
            targets = [vid for vid in on_disk_ids
                       if vid not in have_meta or vid not in have_thumb]

    if not targets:
        stream.emit([["  \u2713 ", "simpleline_green"],
                     ["All metadata + thumbnails up to date.\n", "simpleline"]])
        return {"ok": True, "fetched": 0, "skipped": len(on_disk_ids),
                "errors": 0}

    return fetch_metadata_for_videos(channel, targets, stream,
                                     cancel_event, refresh=refresh,
                                     pause_event=pause_event)


# ── Back-compat shim: earlier builds had an `existing_info_ids` helper ──
# that sync.py or other modules may still import. Keep it so we don't
# break imports even though nothing writes .info.json anymore.
def existing_info_ids(folder: Path) -> set:
    """Deprecated: scanned legacy per-video `.info.json` sidecars.
    Returns a set of video IDs found via aggregated metadata JSONLs."""
    if not isinstance(folder, Path):
        folder = Path(folder)
    if not folder.is_dir():
        return set()
    found = set()
    # Walk for .{ch_name} Metadata.jsonl files regardless of channel name
    for dp, _dns, fns in os.walk(str(folder)):
        for fn in fns:
            if fn.startswith(".") and fn.endswith("Metadata.jsonl"):
                found.update(_read_metadata_jsonl(os.path.join(dp, fn)).keys())
    # Also keep compat with any leftover per-video .info.json files
    bracket_re = re.compile(r"\[([A-Za-z0-9_-]{11})\]")
    for dp, _dns, fns in os.walk(str(folder)):
        for fn in fns:
            if not fn.endswith(".info.json"):
                continue
            m = bracket_re.findall(fn)
            if m:
                found.add(m[-1])
    return found
