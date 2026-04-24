"""
Metadata fetcher — writes OLD YTArchiver's aggregated format.

Output layout (must match YTArchiver.py exactly for drop-in replacement):

  .{ch_name} Metadata.jsonl (no split)
  {year}/.{ch_name} {year} Metadata.jsonl (year-split)
  {year}/{MM Month}/.{ch_name} {Month} {YY} Metadata.jsonl (year+month)

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
            ctypes.windll.kernel32.SetFileAttributesW(str(path), 0x80) # NORMAL
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
    # audit H-15: track corrupt-line count and log a warning if any.
    # Previously bad JSON lines were silently skipped (`continue`),
    # leaving the user with no signal that metadata for some videos
    # was effectively missing. With a counter and warning, the user
    # can investigate the file instead of wondering why views/likes
    # never refresh for certain videos.
    _bad_lines = 0
    _total_lines = 0
    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                _total_lines += 1
                try:
                    entry = json.loads(line)
                    vid = entry.get("video_id", "")
                    if vid:
                        existing[vid] = entry
                except json.JSONDecodeError:
                    _bad_lines += 1
                    continue
    except Exception:
        pass
    if _bad_lines > 0:
        try:
            print(
                f"[metadata] {jsonl_path}: {_bad_lines}/{_total_lines} "
                f"JSONL lines were corrupt and skipped. "
                f"Metadata for those videos will appear missing.")
        except Exception:
            pass
    return existing


def _write_metadata_jsonl(jsonl_path: str,
                          entries_dict: Dict[str, Dict[str, Any]]) -> None:
    """Write all entries (whole dict) to the aggregated JSONL, hiding on Win.
    Matches YTArchiver.py:26583.

    audit C-6: atomic write via .tmp + os.replace. A crash or power
    loss mid-write used to truncate the entire channel's metadata
    file (potentially thousands of video records). Write to a temp
    file in the same directory first, fsync it, then atomically rename
    over the destination.
    """
    os.makedirs(os.path.dirname(jsonl_path) or ".", exist_ok=True)
    if os.name == "nt" and os.path.isfile(jsonl_path):
        _unhide_file_win(jsonl_path)
    tmp_path = jsonl_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        for _vid, data in entries_dict.items():
            f.write(json.dumps(data, ensure_ascii=False) + "\n")
        try:
            f.flush()
            os.fsync(f.fileno())
        except OSError:
            pass
    os.replace(tmp_path, jsonl_path)
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
                        title: str, video_id: str,
                        stream=None) -> None:
    """Download a thumbnail to `{thumb_dir}/{safe_title} [{video_id}].jpg`.
    Dedupes against an existing file with the same [{video_id}] bracket.
    Matches YTArchiver.py:26784 exactly.

    `stream` (optional) — if provided, emits a verbose-only dim
    diagnostic line on fetch failure. Without this, a missing
    thumbnail in Browse view was impossible to diagnose because
    the exception was silently swallowed.
    """
    if not url or not video_id:
        return
    safe_title = re.sub(r'[<>:"/\\|?*]', '_', title or "")[:100]
    fname = f"{safe_title} [{video_id}].jpg"
    fpath = os.path.join(thumb_dir, fname)
    if os.path.isfile(fpath):
        return

    # Dedup: if a thumb with this [{video_id}] already exists under a
    # different title (YT renamed the video), rename it instead of writing
    # a duplicate. audit F-13: rename only if the existing file is recent
    # (<30 days); otherwise fall through to re-download so a stale thumb
    # from years ago gets refreshed with the current YouTube URL.
    try:
        if os.path.isdir(thumb_dir):
            bracket = f"[{video_id}]"
            for existing in os.listdir(thumb_dir):
                if not existing.lower().endswith(
                        (".jpg", ".jpeg", ".png", ".webp")):
                    continue
                if bracket in existing and existing != fname:
                    existing_path = os.path.join(thumb_dir, existing)
                    _is_recent = False
                    try:
                        import time as _t
                        _is_recent = (_t.time() - os.path.getmtime(existing_path)
                                      ) < (30 * 86400)
                    except OSError:
                        pass
                    existing_ext = os.path.splitext(existing)[1]
                    new_fname = f"{safe_title} [{video_id}]{existing_ext}"
                    new_path = os.path.join(thumb_dir, new_fname)
                    try:
                        os.replace(existing_path, new_path)
                        if _is_recent:
                            return
                        # Fall through to re-download (YT likely has
                        # a newer thumbnail; old one renamed for backup).
                        break
                    except OSError:
                        pass
    except OSError:
        pass

    # audit C-7: atomic write via .tmp + os.replace. Interrupt or crash
    # during write used to leave a 0-byte .jpg at the target path.
    # Because the next run sees isfile=True and skips, the broken image
    # gets cached permanently. Also validate JPEG magic bytes before
    # committing so a truncated HTML error page doesn't masquerade as
    # a thumbnail. audit F-14: cap read at 20 MB — YouTube thumbs are
    # typically <200 KB so anything bigger is suspicious.
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            img_data = resp.read(20 * 1024 * 1024)
        if not img_data or len(img_data) < 16:
            raise ValueError(f"empty/short response ({len(img_data)} bytes)")
        # JPEG: FF D8 FF. PNG: 89 50 4E 47. WEBP: RIFF....WEBP.
        _magic_ok = (img_data[:3] == b"\xFF\xD8\xFF"
                     or img_data[:4] == b"\x89PNG"
                     or (img_data[:4] == b"RIFF" and img_data[8:12] == b"WEBP"))
        if not _magic_ok:
            raise ValueError("not a recognized image format")
        tmp_path = fpath + ".tmp"
        with open(tmp_path, "wb") as f:
            f.write(img_data)
            try:
                f.flush()
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(tmp_path, fpath)
    except Exception as _te:
        # Non-fatal, but no longer invisible: emit a verbose-only
        # diagnostic so the user can see WHY a Browse thumbnail is
        # missing (404, timeout, disk-write failure, etc.) instead
        # of just seeing a placeholder with no hint.
        if stream is not None:
            try:
                stream.emit([
                    [" \u26A0 Thumbnail fetch failed ", "dim"],
                    [f"[{video_id}]: {_te}\n", "dim"],
                ])
            except Exception:
                pass


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
        # audit E-11: distinguish timeout from real failure. Callers
        # use the `_timeout` sentinel to skip marking the video with
        # persistent `metadata_fetch_failed_ts` — a 120s timeout is
        # more likely a slow network than a dead video, and the old
        # behavior permanently flagged these until manual Refresh.
        return {"_timeout": True}
    if proc.returncode != 0:
        return None

    # audit F-26: yt-dlp --dump-json writes exactly one JSON object on
    # stdout followed by newline. Parse line-by-line looking for a
    # line that starts with `{` and parses cleanly. This is robust
    # against warning chatter that contains literal `{` characters
    # (e.g. jinja-ish template errors, thumbnail URLs with braces).
    data: Optional[Dict[str, Any]] = None
    for _line in stdout.splitlines():
        _ls = _line.strip()
        if not _ls or _ls[0] != "{":
            continue
        try:
            data = json.loads(_ls)
            break
        except Exception:
            continue
    if data is None:
        # Fall back to the old slice-between-first-and-last-brace
        # parse, which handles pretty-printed multi-line output.
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
            "text": c.get("text", ""),
            "likes": c.get("like_count", 0),
            "time": c.get("timestamp") or c.get("time_text", ""),
        })

    return {
        "video_id": video_id,
        "title": data.get("title", title_hint),
        "description": data.get("description", ""),
        "view_count": data.get("view_count", 0),
        "like_count": data.get("like_count", 0),
        "comment_count": data.get("comment_count", 0),
        "upload_date": data.get("upload_date", ""),
        "duration": data.get("duration", 0),
        "thumbnail_url": data.get("thumbnail", ""),
        "comments": comments,
        "fetched_at": datetime.now().isoformat(),
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
         the only mapping). users hit this: the metadata recheck saw
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
    # a user's channel archive has 13 files with the filename
    # suffix `[a-user-channel]` (the channel name), which is exactly
    # 11 letters and matches the `[A-Za-z0-9_-]{11}` pattern. Real
    # YouTube video_ids are random 11-char picks from that set and
    # statistically always include at least one digit/_/-.
    bracket_re = re.compile(r"\[([A-Za-z0-9_-]{11})\]\s*$")
    _vid_looks_fake = lambda s: s.isalpha() # all-letters → not a YT id
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
            # if it was a fake one (a-user-channel) — we still don't
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

    Emits one log line by default — " \u2014 Metadata downloaded" — in
    pink, matching the format the user asked for:
        [Sync] ...
          Downloading Title...
          \u2014 \u2713 Title Channel 04.18.26 (26 MB)
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
    split_years = bool(channel.get("split_years"))
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
    # audit E-11: `{"_timeout": True}` sentinel signals a transient
    # 120s fetch timeout (slow network) rather than a true failure.
    # Return without marking anything; caller can retry later.
    if isinstance(entry, dict) and entry.get("_timeout"):
        if emit_inline_log:
            stream.emit([
                [" \u2014 ", "dim"],
                ["Metadata fetch timed out (will retry next pass)\n", "dim"],
            ])
        return {"ok": False, "error": "timeout", "transient": True}
    if entry is None:
        if emit_inline_log:
            stream.emit([
                [" \u2014 ", "dim"],
                ["Metadata fetch failed\n", "red"],
            ])
        return {"ok": False, "error": "yt-dlp dump-json failed"}

    existing[video_id] = entry
    try:
        _write_metadata_jsonl(jp, existing)
    except Exception as e:
        return {"ok": False, "error": f"write failed: {e}"}

    # Thumbnail (best-effort). Stream passed through so fetch errors
    # surface as verbose-only dim log lines instead of disappearing.
    if entry.get("thumbnail_url"):
        thumb_dir = _ensure_thumbnails_dir(subfolder)
        _download_thumbnail(
            entry["thumbnail_url"], thumb_dir,
            title_hint or entry.get("title", ""), video_id,
            stream=stream if emit_inline_log else None)

    if emit_inline_log:
        # Per-video metadata done line. Matches the three-line simple-mode
        # summary spec locked in:
        # — ✓ <title> — <channel> (size) [download done, green]
        # — ✓ Transcription (details) [transcription done, blue]
        # — ✓ Metadata downloaded [metadata done, pink + white]
        # Pink em-dash + checkmark + pink "Metadata", then white
        # "downloaded". user spec: color the subject, not the
        # verb — "(pink)— (pink)Metadata (white)downloaded".
        stream.emit([
            [" ", "dim"],
            ["\u2014 \u2713 ", "meta_bracket"],
            ["Metadata ", "simpleline_pink"],
            ["downloaded\n", "simpleline"],
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
    split_years = bool(channel.get("split_years"))
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
        ["  \u2014 ", "meta_bracket"],
        [f"{name} ", "simpleline"],
        ["\u2014 ", "meta_bracket"],
        [f"fast-fetch {len(ids)} id(s)\n", "simpleline"],
    ])

    total = sum(len(g["videos"]) for g in groups.values())
    t0 = time.time()
    fetched = skipped = errors = refreshed = thumb_only = 0
    idx = 0

    # Sticky active status line pinned at the bottom of the log while
    # the metadata fetch runs — mirrors classic YTArchiver.py:14207
    # `_start_simple_anim(ch_name, 1, _fetch_total, mode="metadata")`.
    # Each per-video update fires a `clear_line` control to drop the
    # old active line and re-emits a fresh one at the current DOM
    # bottom via the `metadata_active` marker.
    import json as _json
    def _emit_active(_i: int, _n: int):
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "metadata_active"}),
             "__control__"],
        ])
        # Color discipline: only [ / ] + "Fetching Metadata:" render
        # in the metadata color; numbers + channel name stay white.
        stream.emit([
            ["[", ["meta_bracket", "metadata_active"]],
            [str(_i), ["simpleline", "metadata_active"]],
            ["/", ["meta_bracket", "metadata_active"]],
            [str(_n), ["simpleline", "metadata_active"]],
            ["] ", ["meta_bracket", "metadata_active"]],
            ["Fetching Metadata: ", ["meta_bracket", "metadata_active"]],
            [f"{name}\u2026\n", ["simpleline", "metadata_active"]],
        ])

    def _clear_active():
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "metadata_active"}),
             "__control__"],
        ])

    for jp, g in groups.items():
        if cancel_event is not None and cancel_event.is_set():
            break
        # audit E-12: wait-on-pause loop (not break-on-pause). Old
        # behavior was "pause = cancel" because the loop bailed out
        # entirely; user lost partial progress of the current group.
        # Now we block in-place until Resume (or Cancel) and continue
        # where we left off. Mirrors the redownload.py pause pattern
        # around line 651-666.
        if pause_event is not None and pause_event.is_set():
            stream.emit([[" \u23F8 Paused \u2014 waiting.\n",
                          "simpleline"]])
            while (pause_event.is_set()
                   and not (cancel_event is not None and cancel_event.is_set())):
                time.sleep(0.5)
            if cancel_event is not None and cancel_event.is_set():
                break
            stream.emit([[" \u25B6 Resumed.\n", "simpleline"]])
        existing = _read_metadata_jsonl(jp)
        thumb_dir = _ensure_thumbnails_dir(g["subfolder"])
        changed = False

        def _has_thumbnail_for(vid: str) -> bool:
            """Check if any thumbnail file in this group's .Thumbnails
            folder matches `[vid]`. a case: 2 videos had metadata
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
            # audit E-12: wait-on-pause inside the inner per-video loop
            # too (not just the outer group loop). Without this, pause
            # during a big group would still march through the rest of
            # the videos before the outer loop's next iteration checks.
            if pause_event is not None and pause_event.is_set():
                while (pause_event.is_set()
                       and not (cancel_event is not None and cancel_event.is_set())):
                    time.sleep(0.5)
                if cancel_event is not None and cancel_event.is_set():
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
            # issue #136: distinguish a refresh hit from a fresh
            # fetch in the per-video log line so the user can SEE the
            # refresh actually doing work (previously every line just
            # said "Metadata — <title>" regardless, and the summary row
            # also ignored refresh counts, so the feature looked broken).
            if needs_thumb_only:
                _reason = "Thumbnail"
            elif refresh and vid_id in existing:
                _reason = "Refresh"
            else:
                _reason = "Metadata"
            # Color discipline: only the pink parts are the ones that
            # identify the task source (brackets, em-dash, tag label).
            # Numbers and titles render in the default color so they
            # read clearly. rule.
            stream.emit([
                [" [", "meta_bracket"],
                [str(idx), "simpleline"],
                ["/", "meta_bracket"],
                [str(total), "simpleline"],
                ["] ", "meta_bracket"],
                [_reason, "simpleline_pink"],
                [" \u2014 ", "meta_bracket"],
                [f"{title[:90]}\n", "simpleline"],
            ])
            _emit_active(idx, total)
            entry = _fetch_video_metadata(yt, vid_id, title)
            # audit E-11: transient timeout sentinel — count as "will
            # retry" rather than a permanent failure so future rechecks
            # still try this video. No persistent flag set.
            if isinstance(entry, dict) and entry.get("_timeout"):
                errors += 1
                stream.emit([
                    [" \u2014 ", "dim"],
                    ["Metadata timeout (will retry next pass) \u2014 ", "dim"],
                    [f"{title[:90]}\n", "simpleline"],
                ])
                continue
            if entry is None:
                errors += 1
                # bug L-5: surface a per-video error line so the user
                # knows WHICH titles failed (previously only the
                # summary count emerged, making diagnosis impossible).
                stream.emit([
                    [" \u2717 ", "red"],
                    ["Metadata failed \u2014 ", "red"],
                    [f"{title[:90]}\n", "simpleline"],
                ])
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
                old["view_count"] = entry.get("view_count", old.get("view_count", 0))
                old["like_count"] = entry.get("like_count", old.get("like_count", 0))
                old["comment_count"] = entry.get("comment_count", old.get("comment_count", 0))
                old["comments"] = entry.get("comments", old.get("comments", []))
                old["fetched_at"] = entry.get("fetched_at", "")
                if entry.get("thumbnail_url"):
                    old["thumbnail_url"] = entry["thumbnail_url"]
                refreshed += 1
                changed = True
            elif needs_thumb_only:
                # JSONL entry stays as-is; only the thumbnail is being
                # backfilled. `changed` stays False so we don't rewrite
                # the JSONL for a thumbnail-only fetch.
                # bug M-11: count thumbnail-only refetches separately
                # from true metadata fetches so the summary + activity
                # log distinguish the two (was silently lumped under
                # `fetched`, making thumbnail-only runs look like full
                # metadata pulls).
                thumb_only += 1
            else:
                existing[vid_id] = entry
                fetched += 1
                changed = True
            if entry.get("thumbnail_url"):
                _download_thumbnail(entry["thumbnail_url"], thumb_dir,
                                    title or entry.get("title", ""), vid_id,
                                    stream=stream)

        if changed:
            try:
                _write_metadata_jsonl(jp, existing)
            except Exception as e:
                stream.emit_error(f"Could not write {jp}: {e}")

    # Drop the sticky active line before the summary so the "Metadata
    # X — N fetched ..." footer doesn't sit below a phantom "Fetching
    # Metadata: X..." line that's no longer accurate.
    _clear_active()

    elapsed = time.time() - t0
    summary_parts = []
    if fetched: summary_parts.append(f"{fetched} fetched")
    if refreshed: summary_parts.append(f"{refreshed} refreshed")
    if thumb_only: summary_parts.append(f"{thumb_only} thumbnails")
    if skipped: summary_parts.append(f"{skipped} skipped")
    if errors: summary_parts.append(f"{errors} errors")
    summary = " \u00b7 ".join(summary_parts) or "nothing to do"
    stream.emit([
        [" \u2013 ", "dim"],
        [f"Metadata {name} \u2014 ", "simpleline"],
        [summary, "dim"],
        [f" \u00b7 took {elapsed:.1f}s\n", "dim"],
    ])
    # Both activity-log row emit AND history persistence moved to
    # sync.py's `emit_metadata_activity_row` so ALL metadata-kind
    # tasks (views/likes refresh, comments refresh, ID backfill,
    # legacy fetch) produce a single identical [Metdta] row with
    # a single persisted history entry. Previously this path emitted
    # + persisted locally while the bulk paths didn't emit at all,
    # so views/likes refresh never appeared in the activity log
    # and legacy fetches produced nothing at all from sync.py's
    # worker loop.
    return {"ok": True, "fetched": fetched, "skipped": skipped,
            "errors": errors, "refreshed": refreshed,
            "thumb_only": thumb_only, "took": elapsed}


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
    # assign the same id to multiple files. a user's channel
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


_ID_RE_11 = re.compile(r"^[A-Za-z0-9_-]{11}$")


def _flat_playlist_bulk_stats(yt: str, ch_url: str,
                               stream: LogStreamer,
                               cancel_event: Optional[threading.Event] = None,
                               pause_event: Optional[threading.Event] = None
                               ) -> Dict[str, Dict[str, Any]]:
    """ONE yt-dlp --flat-playlist call returning per-video stats for
    the whole channel. Returns {video_id: {view_count, like_count,
    comment_count}} (None values where yt-dlp's flat-playlist path
    doesn't populate that field — YouTube reliably returns view_count
    but like_count / comment_count are often null in flat mode).

    This is the smart-refresh primitive: compared to the old path of
    `--dump-json` per video (one HTTP round-trip each), this folds
    an entire channel's view-count data into a single request. Users
    reported a 404-video channel taking ~1h17m under the per-video
    approach — the flat-playlist equivalent typically finishes in
    well under a minute.

    Caller decides what to do with the stats; see bulk_refresh_views_likes.
    """
    if not ch_url:
        return {}
    cmd = [
        yt,
        "--flat-playlist",
        "--lazy-playlist",
        "--no-warnings",
        "--skip-download",
        # TAB-separated so titles (which can contain pipes / commas)
        # never collide with the field separator. Title is included so
        # the caller can fall back to title-matching for legacy archive
        # files whose filenames lack [video_id] brackets AND aren't
        # registered in the videos-table with a video_id — the
        # default archive layout per tkinter-era downloads.
        "--print",
        # Extended with upload_date + duration so backfill_video_ids
        # can disambiguate title-near-duplicates using the file's
        # mtime (== YT upload date when yt-dlp ran with --mtime) and
        # the on-disk duration. Keeping it one pass so we don't
        # double the API traffic.
        "%(id)s\t%(view_count)s\t%(like_count)s\t%(comment_count)s\t%(title)s\t%(upload_date)s\t%(duration)s",
        *_find_cookie_source(),
        ch_url,
    ]
    out: Dict[str, Dict[str, Any]] = {}
    try:
        proc = subprocess.Popen(
            cmd, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            encoding="utf-8", errors="replace", bufsize=1,
            startupinfo=_startupinfo, env=_utf8_env(),
        )
    except OSError as e:
        stream.emit_error(f"Metadata: could not start stats fetch: {e}")
        return {}
    # Progress tick during the catalog fetch — on large channels (10k+
    # videos) this can take 30-60s and the caller's "Resolving video
    # IDs for X..." line otherwise looks frozen the whole time. Emit
    # every _PROGRESS_TICK_EVERY parsed rows OR every
    # _PROGRESS_TICK_SECS so the user sees something happening.
    _PROGRESS_TICK_EVERY = 500
    _PROGRESS_TICK_SECS = 5.0
    _tick_count = 0
    _last_tick_ts = time.time()
    try:
        for raw in proc.stdout:
            if cancel_event is not None and cancel_event.is_set():
                try: proc.terminate()
                except Exception: pass
                break
            # Honor pause without dropping the subprocess — same
            # pattern as _fetch_yt_catalog in redownload.py.
            while (pause_event is not None and pause_event.is_set()
                   and not (cancel_event is not None and cancel_event.is_set())):
                time.sleep(0.25)
            line = (raw or "").rstrip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 4:
                continue
            vid = parts[0].strip()
            if not _ID_RE_11.fullmatch(vid):
                continue
            _tick_count += 1
            _now = time.time()
            if (_tick_count % _PROGRESS_TICK_EVERY == 0
                    or (_now - _last_tick_ts) >= _PROGRESS_TICK_SECS):
                # Non-dim tag so the tick stays visible in Simple
                # mode — the user's specifically watching this
                # long-running op for a heartbeat.
                try:
                    stream.emit([[f"  \u2014 Fetched {_tick_count:,} videos "
                                 f"from YouTube catalog\u2026\n",
                                 "simpleline"]])
                except Exception:
                    pass
                _last_tick_ts = _now
            def _num(s: str) -> Optional[int]:
                s = (s or "").strip()
                if not s or s in ("NA", "None", "null"):
                    return None
                try:
                    return int(float(s))
                except (TypeError, ValueError):
                    return None
            _title = parts[4].strip() if len(parts) >= 5 else ""
            out[vid] = {
                "view_count": _num(parts[1]),
                "like_count": _num(parts[2]),
                "comment_count": _num(parts[3]),
                "title": _title,
                # New fields (Colbert backfill fix): upload_date and
                # duration for non-title disambiguation in
                # backfill_video_ids. yt-dlp emits upload_date as
                # YYYYMMDD (or "NA" if unknown); duration as seconds.
                "upload_date": (parts[5].strip()
                                if len(parts) >= 6 else ""),
                "duration": _num(parts[6]) if len(parts) >= 7 else None,
            }
        try: proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.terminate()
    except Exception as e:
        stream.emit_dim(f" (stats read error: {e})")
        try: proc.terminate()
        except Exception: pass
    return out


def bulk_refresh_views_likes(channel: Dict[str, Any],
                              stream: LogStreamer,
                              cancel_event: Optional[threading.Event] = None,
                              pause_event: Optional[threading.Event] = None,
                              scope: Optional[Dict[str, Any]] = None,
                              full_fetch_on_change: bool = False,
                              ) -> Dict[str, Any]:
    """Fast view-count refresh path. Uses one flat-playlist call to
    get per-video view/like/comment counts, compares against the
    existing metadata.jsonl, and only does a full --dump-json fetch
    for videos whose counts actually changed (to also pick up updated
    top-comments, descriptions, etc.).

    `full_fetch_on_change=False` skips even that second pass and just
    updates the count fields in-place — useful for "i only care about
    the view count, don't waste any more yt-dlp calls" flows.

    `scope={"year": N}` honors the year-scoped refresh introduced for
    the Browse grid year-head right-click.

    Returns the same shape as fetch_channel_metadata so the sync
    worker's summary-parser keeps working: `{ok, fetched, refreshed,
    errors, skipped, bulk_fetched}`. `bulk_fetched` is new and lets
    callers know how many videos were resolved via the fast path
    (= "considered" rather than "re-fetched").
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
    ch_url = (channel.get("url") or "").strip()
    if not ch_url:
        stream.emit_error(
            f"Metadata: {name} has no URL — can't refresh.")
        return {"ok": False, "error": "no url"}

    _scope_year: Optional[int] = None
    if scope and isinstance(scope.get("year"), int):
        _scope_year = int(scope["year"])
    _banner = f" ({_scope_year} only)" if _scope_year is not None else ""
    # Log kept user-friendly — previously said "(flat-playlist)" which
    # is an implementation detail (yt-dlp mode) the user doesn't need
    # to see in Simple mode.
    stream.emit([["  \u2014 ", "meta_bracket"],
                 [f"Refreshing {name}{_banner}...\n", "simpleline"]])

    t0 = time.time()
    bulk = _flat_playlist_bulk_stats(yt, ch_url, stream,
                                     cancel_event, pause_event)
    if not bulk:
        stream.emit([
            [" \u26A0 ", "meta_bracket"],
            [f"Bulk-stats returned no data for {name} — "
             f"channel may be empty / private / geo-locked.\n", "simpleline"],
        ])
        return {"ok": False, "error": "bulk_empty",
                "fetched": 0, "refreshed": 0, "errors": 0, "skipped": 0,
                "bulk_fetched": 0}

    # Enumerate on-disk videos so we only refresh ones we actually
    # have files for (mirrors fetch_channel_metadata's disk-driven
    # philosophy — never pay yt-dlp time for playlist entries with
    # no archive file).
    on_disk = _scan_channel_videos(folder)
    if _scope_year is not None:
        on_disk = [v for v in on_disk if v[2] == _scope_year]

    # Title-fallback resolution. The default archive layout has NO
    # `[video_id]` bracket in filenames AND many legacy-tkinter-era
    # registrations landed in the videos-table with video_id=NULL.
    # Without a second matching strategy every file shows as "missing"
    # and the whole bulk pass reports "no matches". Fix: build a
    # normalized-title → video_id map from the bulk data and resolve
    # empty vid_ids via title lookup. The normalization aggressively
    # folds whitespace and punctuation so minor filesystem sanitization
    # differences (en-dash → hyphen, colons dropped, etc.) still match.
    def _norm_title(s: str) -> str:
        if not s:
            return ""
        s = s.lower()
        # Drop a trailing `[id]` remnant if present (belt-and-suspenders).
        s = re.sub(r"\s*\[[a-z0-9_-]{11}\]\s*$", "", s)
        # Collapse any run of non-alphanumeric into a single space,
        # strip. This handles filesystem-sanitized colons, question
        # marks, smart quotes, en-dashes, etc.
        s = re.sub(r"[^a-z0-9]+", " ", s)
        return s.strip()

    _title_to_vid: Dict[str, str] = {}
    _ambiguous_titles: set = set()
    for _vid, _stats in bulk.items():
        _nt = _norm_title(_stats.get("title") or "")
        if not _nt:
            continue
        if _nt in _title_to_vid and _title_to_vid[_nt] != _vid:
            _ambiguous_titles.add(_nt)
        else:
            _title_to_vid[_nt] = _vid

    # Resolve vid_ids for on-disk tuples that came back empty. Track
    # which (filepath, video_id) pairs we backfilled so we can persist
    # them to the index DB after the scan — next run skips the title
    # match entirely because the DB lookup at _scan_channel_videos
    # fills fp_to_id.
    _title_resolved: List[Tuple[str, str, str]] = []  # (fp, vid, title)
    _resolved_on_disk = []
    for (_v, _t, _y, _m, _fp) in on_disk:
        if _v:
            _resolved_on_disk.append((_v, _t, _y, _m, _fp))
            continue
        _nt = _norm_title(_t)
        if not _nt or _nt in _ambiguous_titles:
            _resolved_on_disk.append((_v, _t, _y, _m, _fp))
            continue
        _guess = _title_to_vid.get(_nt, "")
        if _guess:
            _resolved_on_disk.append((_guess, _t, _y, _m, _fp))
            _title_resolved.append((_fp, _guess, _t))
        else:
            _resolved_on_disk.append((_v, _t, _y, _m, _fp))
    on_disk = _resolved_on_disk

    # Backfill resolved video_ids into the videos-table so future
    # bulk refreshes skip the title-match dance.
    if _title_resolved:
        try:
            from . import index as _idx
            _conn = _idx._open()
            if _conn is not None:
                with _idx._db_lock:
                    for _fp, _vid, _ttl in _title_resolved:
                        _vurl = f"https://www.youtube.com/watch?v={_vid}"
                        try:
                            _conn.execute(
                                "UPDATE videos SET video_id=?, video_url=? "
                                "WHERE filepath=? COLLATE NOCASE "
                                "AND (video_id IS NULL OR video_id='')",
                                (_vid, _vurl, _fp))
                        except Exception:
                            pass
                    _conn.commit()
        except Exception:
            pass
        # User-friendly wording: dropped "(no [id] in filename)"
        # technicality — users shouldn't have to know about the
        # internal DB state to understand what happened.
        stream.emit([
            [" \u2014 ", "meta_bracket"],
            [f"Matched {len(_title_resolved)} video(s) by title "
             f"\u2014 saved their YouTube IDs.\n", "simpleline"],
        ])

    on_disk_ids = {v[0] for v in on_disk if v[0]}

    # Load existing metadata across all on-disk JSONLs, keyed by id.
    # Track which JSONL each entry came from so we can write it back.
    existing_by_id: Dict[str, Dict[str, Any]] = {}
    jsonl_by_id: Dict[str, str] = {}
    for dp, _dns, fns in os.walk(str(folder)):
        for fn in fns:
            if not fn.endswith("Metadata.jsonl"):
                continue
            jp = os.path.join(dp, fn)
            entries = _read_metadata_jsonl(jp)
            for vid, entry in entries.items():
                existing_by_id[vid] = entry
                jsonl_by_id[vid] = jp

    # Walk the bulk data, update or flag-for-fetch per video.
    changed_ids: List[str] = []       # to re-fetch via --dump-json
    updated_in_place = 0              # just bumped counts in existing entry
    skipped_same = 0                  # counts unchanged, nothing to do
    missing_on_disk = 0               # in bulk but no on-disk file
    no_meta_entry = 0                 # on disk but no existing metadata

    for vid, stats in bulk.items():
        if vid not in on_disk_ids:
            missing_on_disk += 1
            continue
        old = existing_by_id.get(vid)
        if old is None:
            # Haven't fetched this video's full metadata yet — always
            # full-fetch it regardless of full_fetch_on_change. We
            # literally have no record for this video, so there's
            # nothing to "update in place" — we have to do the full
            # --dump-json to create the entry. full_fetch_on_change
            # only governs whether CHANGED-COUNT entries also get
            # re-fetched (which re-pulls comments too, so the
            # views/likes refresh path now sets it False to keep
            # comments out of scope).
            no_meta_entry += 1
            changed_ids.append(vid)
            continue
        # Decide whether anything moved enough to warrant a full fetch.
        _view_new = stats.get("view_count")
        _like_new = stats.get("like_count")
        _comment_new = stats.get("comment_count")
        _view_old = old.get("view_count")
        _like_old = old.get("like_count")
        _comment_old = old.get("comment_count")
        _changed = False
        if _view_new is not None and _view_new != _view_old:
            _changed = True
        # like_count often missing in flat mode; only flag if it's
        # explicitly different (not when the old had a real value and
        # the new is None — that's a bulk-mode gap, not a real drop).
        if (_like_new is not None and _like_old is not None
                and _like_new != _like_old):
            _changed = True
        if (_comment_new is not None and _comment_old is not None
                and _comment_new != _comment_old):
            _changed = True

        # Always update the stats in-place — even if unchanged, bump
        # `fetched_at` so the "last refreshed" timestamp is accurate.
        if _view_new is not None:
            old["view_count"] = _view_new
        if _like_new is not None:
            old["like_count"] = _like_new
        if _comment_new is not None:
            old["comment_count"] = _comment_new
        old["fetched_at"] = datetime.now().isoformat()

        if _changed and full_fetch_on_change:
            changed_ids.append(vid)
        elif _changed:
            updated_in_place += 1
        else:
            skipped_same += 1

    # Persist the in-place-updated entries. Group by jsonl path so we
    # only rewrite each file once.
    dirty_paths: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for vid, entry in existing_by_id.items():
        jp = jsonl_by_id.get(vid)
        if jp is None:
            continue
        dirty_paths.setdefault(jp, {})[vid] = entry
    for jp, entries in dirty_paths.items():
        # Load current contents, merge our updates on top, rewrite.
        full = _read_metadata_jsonl(jp)
        full.update(entries)
        try:
            _write_metadata_jsonl(jp, full)
        except OSError as e:
            stream.emit_dim(f" (metadata write failed for {os.path.basename(jp)}: {e})")

    # Secondary pass: full --dump-json fetch for videos whose counts
    # changed (picks up new comments, updated descriptions, etc.).
    # Reuses the existing per-video fetch_single_video_metadata path
    # so the logging + error handling stays consistent.
    full_fetched = 0
    full_errors = 0
    if changed_ids:
        # With full_fetch_on_change=False (the views/likes-refresh
        # default), this list only contains videos that had NO
        # existing metadata entry — we're filling in first-time
        # metadata, not comment refresh. Wording updated so a user
        # who clicked "Refresh views/likes" doesn't see a line
        # claiming we're pulling comments.
        _n = len(changed_ids)
        _what = ("new entries \u2014 fetching full metadata..."
                 if not full_fetch_on_change
                 else "have updated counts \u2014 re-fetching details...")
        stream.emit([
            [" \u2014 ", "meta_bracket"],
            [f"{_n} video(s) {_what}\n", "simpleline"],
        ])
        # Build a video_id → (filepath, title) map from on_disk so we
        # can pass filepath + title_hint to fetch_single_video_metadata
        # (signature: channel, video_id, file_path, title_hint, stream).
        # The prior call had `stream` in the title_hint slot AND passed
        # a nonexistent `cancel_event` kwarg — every per-video fetch
        # raised TypeError and got caught as an error (users reported
        # 40/40 errors on a test channel). Fixed by passing args in
        # the right order; cancel/pause are still honored by the
        # wrapping loop.
        fp_by_id: Dict[str, Tuple[str, str]] = {}
        for (_v, _t, _y, _m, _fp) in on_disk:
            if _v and _fp:
                fp_by_id[_v] = (_fp, _t or "")
        # Progress tick: emit a dim "[N/total] processed" line every
        # _PROGRESS_TICK_EVERY videos OR every _PROGRESS_TICK_SECS
        # so a 600-video channel doesn't look stuck for an hour
        # between the initial "N video(s) have updated counts..."
        # line and the final summary. User flagged this as "refresh
        # views got stuck" on Bernie Sanders (610 videos).
        _PROGRESS_TICK_EVERY = 25
        _PROGRESS_TICK_SECS = 20.0
        _last_tick_ts = time.time()
        _processed = 0
        _total = len(changed_ids)
        for vid in changed_ids:
            if cancel_event is not None and cancel_event.is_set():
                break
            while (pause_event is not None and pause_event.is_set()
                   and not (cancel_event is not None and cancel_event.is_set())):
                time.sleep(0.25)
            _pair = fp_by_id.get(vid)
            if not _pair:
                _processed += 1
                continue
            fp, title_hint = _pair
            try:
                res = fetch_single_video_metadata(
                    channel, vid, fp, title_hint, stream,
                    emit_inline_log=False)
                if res.get("ok"):
                    full_fetched += 1
                elif not res.get("transient"):
                    full_errors += 1
            except Exception as _e:
                stream.emit_dim(f" (full fetch failed for {vid}: {_e})")
                full_errors += 1
            _processed += 1
            # Emit progress tick on count OR time boundary, whichever
            # comes first. Skip the very last one (final summary line
            # replaces it immediately below).
            _now = time.time()
            if _processed < _total and (
                    _processed % _PROGRESS_TICK_EVERY == 0
                    or (_now - _last_tick_ts) >= _PROGRESS_TICK_SECS):
                try:
                    stream.emit_dim(
                        f" \u2014 [{_processed}/{_total}] "
                        f"fetching metadata\u2026")
                except Exception:
                    pass
                _last_tick_ts = _now

    # Stamp last-refresh timestamp on the channel config. Separate
    # from per-video fetched_at so the Subs UI can say "refreshed
    # N minutes ago" for the whole channel.
    try:
        from . import ytarchiver_config as _cfg
        cfg = _cfg.load_config()
        ch_url_norm = ch_url.rstrip("/")
        now_ts = time.time()
        for ch in cfg.get("channels", []):
            if (ch.get("url") or "").rstrip("/") == ch_url_norm:
                ch["last_views_refresh_ts"] = now_ts
                break
        _cfg.save_config(cfg)
    except Exception:
        pass

    took = time.time() - t0
    # Tagged emit: channel name + labels render white, counts render
    # pink, errors red. Previously the whole line was one pink blob
    # which users called out as visual noise ("channel name should be
    # white, labels should be white, only the numbers highlight").
    # "via bulk path" dropped — user-facing log doesn't need to
    # surface the internal code path.
    _err_color = "red" if full_errors else "simpleline_pink"
    tagged: List[List[str]] = [
        [" \u2014 ", "meta_bracket"],
        [f"{name}: ", "simpleline"],
    ]
    _first = True
    def _sep():
        if not _first:
            tagged.append([" \u00b7 ", "simpleline"])
    _emitted_something = False
    if full_fetched:
        _sep()
        tagged.append([f"{full_fetched}", "simpleline_pink"])
        tagged.append([" with updated counts", "simpleline"])
        _first = False
        _emitted_something = True
    if updated_in_place:
        _sep()
        tagged.append([f"{updated_in_place}", "simpleline_pink"])
        tagged.append([" counts updated in place", "simpleline"])
        _first = False
        _emitted_something = True
    if skipped_same:
        _sep()
        tagged.append([f"{skipped_same}", "simpleline_pink"])
        tagged.append([" unchanged", "simpleline"])
        _first = False
        _emitted_something = True
    if full_errors:
        _sep()
        tagged.append([f"{full_errors}", _err_color])
        tagged.append([" errors", _err_color])
        _first = False
        _emitted_something = True
    if no_meta_entry and not full_fetched:
        _sep()
        tagged.append([f"{no_meta_entry}", "simpleline_pink"])
        tagged.append([" need first fetch", "simpleline"])
        _first = False
        _emitted_something = True
    if not _emitted_something:
        # Zero matches across all counters. Normally the title-fallback
        # loop above resolves legacy-tkinter archive files — so hitting
        # this branch means even title-matching failed. Usually:
        # (a) empty channel folder, (b) ambiguous titles (duplicates
        # skipped for safety), or (c) filesystem-sanitized titles too
        # divergent to match.
        _n_disk = len(on_disk)
        _n_bulk = len(bulk)
        if _n_disk == 0:
            tagged.append(["no videos on disk for this channel",
                           "simpleline"])
        elif _n_bulk == 0:
            tagged.append(["channel returned no videos", "simpleline"])
        else:
            tagged.append([
                f"no matches ({_n_disk} on disk vs {_n_bulk} from YouTube "
                f"\u2014 titles too divergent to match)", "simpleline"])
    tagged.append([f" (took {took:.1f}s)\n", "simpleline"])
    stream.emit(tagged)
    return {
        "ok": True,
        "fetched": no_meta_entry,
        "refreshed": full_fetched + updated_in_place,
        "errors": full_errors,
        "skipped": skipped_same,
        "bulk_fetched": len(bulk),
        "took": took,
    }


def count_video_id_status(channel: Dict[str, Any]) -> Dict[str, Any]:
    """Cheap DB-only count: how many on-disk videos have a resolvable
    video_id stored in the index `videos` table? Powers the Settings >
    Metadata "Video IDs" column so the user can spot channels that
    need a one-time backfill (common for archives migrated from the
    tkinter-era YTArchiver, which never wrote [id] brackets into
    filenames nor .info.json sidecars).

    Returns {total, with_id, missing, tried_failed}:
      * total:        row count for files under the channel folder.
      * with_id:      rows where video_id is non-NULL / non-empty.
      * missing:      total - with_id.
      * tried_failed: rows that are still missing AND have an
                      id_backfill_tried_ts — i.e. the backfill
                      pass attempted them and every strategy
                      returned no match. Separates "probably
                      genuinely unresolvable (renamed, removed,
                      title-drift beyond fuzzy threshold)" from
                      "never tried — run Fix IDs".

    Purely a DB-read — no disk walk, no yt-dlp. Safe to call for
    every channel on a tab render.
    """
    folder = _folder_for_channel(channel)
    if folder is None:
        return {"total": 0, "with_id": 0, "missing": 0, "tried_failed": 0}
    try:
        from . import index as _idx
        conn = _idx._open()
        if conn is None:
            return {"total": 0, "with_id": 0, "missing": 0, "tried_failed": 0}
        _pat = str(folder) + "%"
        with _idx._db_lock:
            _total = conn.execute(
                "SELECT COUNT(*) FROM videos WHERE filepath LIKE ?",
                (_pat,)).fetchone()[0]
            _with_id = conn.execute(
                "SELECT COUNT(*) FROM videos WHERE filepath LIKE ? "
                "AND video_id IS NOT NULL AND video_id != ''",
                (_pat,)).fetchone()[0]
            # Rows still missing an id that have been through the
            # backfill pass at least once. Column won't exist on
            # very old DBs; guarded query falls back to 0.
            try:
                _tried = conn.execute(
                    "SELECT COUNT(*) FROM videos WHERE filepath LIKE ? "
                    "AND (video_id IS NULL OR video_id='') "
                    "AND id_backfill_tried_ts IS NOT NULL",
                    (_pat,)).fetchone()[0]
            except Exception:
                _tried = 0
        return {
            "total": int(_total or 0),
            "with_id": int(_with_id or 0),
            "missing": int((_total or 0) - (_with_id or 0)),
            "tried_failed": int(_tried or 0),
        }
    except Exception:
        return {"total": 0, "with_id": 0, "missing": 0, "tried_failed": 0}


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
    except Exception:
        pass
    return ""


def _norm_title_for_match(s: str) -> str:
    """Normalization used by backfill_video_ids.

    Lowercase, strip trailing `[VIDEO_ID]` tag, collapse every non-
    alphanumeric run to a single space. Shared by every matching
    strategy so candidates compare apples-to-apples.
    """
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"\s*\[[a-z0-9_-]{11}\]\s*$", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()


def backfill_video_ids(channel: Dict[str, Any],
                       stream: LogStreamer,
                       cancel_event: Optional[threading.Event] = None,
                       pause_event: Optional[threading.Event] = None,
                       ) -> Dict[str, Any]:
    """One-shot video_id backfill with multi-strategy resolution.

    For every on-disk file without a video_id in the DB, try in order:

      1. `.info.json` sidecar (zero-cost, no network)
      2. Exact normalized-title match against YouTube's current
         flat-playlist
      3. Substring title match (local title subset of YT title, or
         vice-versa) when the single candidate is unambiguous
      4. Upload-date match (file mtime YYYYMMDD → YT upload_date),
         disambiguated by duration when >1 candidate exists
      5. Fuzzy title match via difflib.get_close_matches (0.80
         cutoff, rejects ambiguous near-ties)
      6. Stamp `id_backfill_tried_ts` so the UI can distinguish
         "tried but genuinely unresolvable" from "not yet attempted"

    This is the dedicated fix for archives where title-rename,
    suffix-add, or emoji-drift on the channel side has silently
    broken the cheap exact-match path. Counts so small that
    exact-match alone gets <20% (e.g. late-night TV channels with
    systematic " | The Late Show" suffix additions).

    Returns {ok, resolved, resolved_by_info_json, resolved_by_exact,
             resolved_by_substring, resolved_by_date,
             resolved_by_fuzzy, already_set, ambiguous,
             unresolved_now_tried, took}.
    """
    folder = _folder_for_channel(channel)
    if folder is None:
        stream.emit_error("Backfill: output_dir is not configured.")
        return {"ok": False, "error": "no output_dir"}
    yt = find_yt_dlp()
    if not yt:
        stream.emit_error("Backfill: yt-dlp not found.")
        return {"ok": False, "error": "yt-dlp missing"}
    name = channel.get("name") or channel.get("folder") or "?"
    ch_url = (channel.get("url") or "").strip()
    if not ch_url:
        stream.emit_error(f"Backfill: {name} has no URL.")
        return {"ok": False, "error": "no url"}

    stream.emit([["  \u2014 ", "meta_bracket"],
                 [f"Resolving video IDs for {name}...\n", "simpleline"]])

    t0 = time.time()
    bulk = _flat_playlist_bulk_stats(yt, ch_url, stream,
                                     cancel_event, pause_event)
    if not bulk:
        stream.emit([
            [" \u26A0 ", "meta_bracket"],
            [f"YouTube returned no video list for {name}.\n", "simpleline"],
        ])
        return {"ok": False, "error": "bulk_empty"}

    # ── Build indices over the YT catalog ────────────────────────────

    # 1. exact normalized title → vid, with ambiguity tracking
    title_to_vid: Dict[str, str] = {}
    title_ambiguous: set = set()
    # 2. upload_date (YYYYMMDD) → [(vid, norm_title, duration_s)]
    date_to_cands: Dict[str, List[Tuple[str, str, Optional[float]]]] = {}
    # 3. token → set of vids (for fuzzy prefilter)
    token_to_vids: Dict[str, set] = {}
    # Keep a dense list of (vid, norm_title) for difflib.get_close_matches
    norm_titles: List[str] = []
    norm_title_to_vid: Dict[str, str] = {}
    # Map vid → duration (seconds) for disambiguation
    vid_to_duration: Dict[str, Optional[float]] = {}

    for _vid, _stats in bulk.items():
        _raw_title = _stats.get("title") or ""
        _nt = _norm_title_for_match(_raw_title)
        _upload = (_stats.get("upload_date") or "").strip()
        _dur = _stats.get("duration")
        try:
            _dur_f = float(_dur) if _dur is not None else None
        except (TypeError, ValueError):
            _dur_f = None
        vid_to_duration[_vid] = _dur_f
        if _nt:
            if _nt in title_to_vid and title_to_vid[_nt] != _vid:
                title_ambiguous.add(_nt)
            else:
                title_to_vid[_nt] = _vid
            if _nt not in norm_title_to_vid:
                norm_titles.append(_nt)
                norm_title_to_vid[_nt] = _vid
            for _tok in _nt.split():
                if len(_tok) >= 3:
                    token_to_vids.setdefault(_tok, set()).add(_vid)
        if _upload and len(_upload) == 8 and _upload.isdigit():
            date_to_cands.setdefault(_upload, []).append(
                (_vid, _nt, _dur_f))

    # ── Scan on-disk videos + resolution passes ──────────────────────

    # Pull local duration_s from the index DB in one query (avoids
    # ffprobe per file). Populated by register_video for fresh
    # downloads; older rows may be NULL.
    _local_durations: Dict[str, Optional[float]] = {}
    try:
        from . import index as _idx
        _conn_pre = _idx._open()
        if _conn_pre is not None:
            _pat = str(folder) + "%"
            with _idx._db_lock:
                for _row in _conn_pre.execute(
                        "SELECT filepath, duration_s FROM videos "
                        "WHERE filepath LIKE ?", (_pat,)):
                    _local_durations[os.path.normpath(_row[0])] = _row[1]
    except Exception:
        pass

    on_disk = _scan_channel_videos(folder)
    already_set = 0
    ambiguous_hits = 0
    unresolved = 0
    resolved_by = {"info_json": 0, "exact": 0, "substring": 0,
                   "date": 0, "fuzzy": 0}
    to_backfill: List[Tuple[str, str, str]] = []  # (filepath, vid, how)
    # Files that failed every strategy — stamp the tried timestamp.
    tried_failed_paths: List[str] = []

    def _days_diff(d1: str, d2: str) -> Optional[int]:
        """Return |d1 - d2| in days for two YYYYMMDD strings. None on
        any parse error or if either is empty."""
        if not d1 or not d2 or len(d1) != 8 or len(d2) != 8:
            return None
        try:
            import datetime as _dt
            dt1 = _dt.datetime.strptime(d1, "%Y%m%d").date()
            dt2 = _dt.datetime.strptime(d2, "%Y%m%d").date()
            return abs((dt1 - dt2).days)
        except Exception:
            return None

    # Scott's rule: "I'd rather have missing info than incorrect
    # info". Any title-based strategy (substring or fuzzy) that
    # picks a candidate MUST also have an upload_date within
    # _DATE_WINDOW_DAYS of the local file's mtime. yt-dlp's
    # --mtime sets file mtime to the upload date so these should
    # match exactly; ±1 day covers timezone drift without opening
    # the door to "similar title, different video" collisions.
    _DATE_WINDOW_DAYS = 1

    def _date_confirms(vid: str, local_day: str) -> bool:
        """True when the candidate vid's upload_date is within
        _DATE_WINDOW_DAYS of the local file's day. Missing date on
        either side = reject (conservative — absence of evidence is
        not evidence of a match)."""
        if not local_day:
            return False
        _ud = (bulk.get(vid, {}).get("upload_date") or "").strip()
        diff = _days_diff(local_day, _ud)
        return diff is not None and diff <= _DATE_WINDOW_DAYS

    def _find_substring_match(needle_nt: str, local_day: str) -> str:
        """Walk the candidate list looking for a SINGLE candidate whose
        normalized title contains the needle, or vice-versa. Length
        ratio must be >=0.7 so "the" doesn't match every video.
        Result must also pass the date window check."""
        if not needle_nt or len(needle_nt) < 5:
            return ""
        # Short-circuit via token prefilter: need at least 2 shared
        # tokens of length >=3 to even consider.
        needle_tokens = [t for t in needle_nt.split() if len(t) >= 3]
        if len(needle_tokens) < 2:
            return ""
        from collections import Counter as _Counter
        counter: _Counter = _Counter()
        for _tok in needle_tokens:
            if _tok in token_to_vids:
                for _v in token_to_vids[_tok]:
                    counter[_v] += 1
        _candidate_vids = [v for v, n in counter.items() if n >= 2]
        hits = []
        for _v in _candidate_vids:
            _cnt = bulk.get(_v) or {}
            _cnt_nt = _norm_title_for_match(_cnt.get("title") or "")
            if not _cnt_nt:
                continue
            _short, _long = sorted([len(needle_nt), len(_cnt_nt)])
            if _long == 0 or _short / _long < 0.7:
                continue
            if needle_nt in _cnt_nt or _cnt_nt in needle_nt:
                hits.append(_v)
                if len(hits) > 1:
                    break  # multiple hits — fall through to date filter
        if not hits:
            return ""
        if len(hits) == 1:
            # Single title hit — still require date agreement so we
            # don't accept a rename that happens to share a substring.
            return hits[0] if _date_confirms(hits[0], local_day) else ""
        # Multiple substring hits — tiebreak by date. Need exactly
        # one candidate to land inside the date window.
        date_hits = [v for v in hits if _date_confirms(v, local_day)]
        return date_hits[0] if len(date_hits) == 1 else ""

    def _find_fuzzy_match(needle_nt: str, local_day: str) -> str:
        """Fuzzy match via difflib. Returns '' unless (a) there's a
        clear winner above the cutoff AND (b) its upload_date agrees
        with the local file's mtime day. Multiple near-tie matches
        fall through to a date-based tiebreak (rather than being
        rejected outright) per Scott's request: 'maybe if more than
        1 hit, try and determine which is correct based on date?'."""
        if not needle_nt or len(needle_nt) < 5:
            return ""
        # Prefilter by shared tokens so we don't run SequenceMatcher
        # against every title in a 10K-video channel.
        needle_tokens = [t for t in needle_nt.split() if len(t) >= 3]
        if len(needle_tokens) < 2:
            return ""
        from collections import Counter as _Counter
        counter: _Counter = _Counter()
        for _tok in needle_tokens:
            if _tok in token_to_vids:
                for _v in token_to_vids[_tok]:
                    counter[_v] += 1
        _shortlist_vids = [v for v, n in counter.items() if n >= 2]
        if not _shortlist_vids:
            return ""
        _shortlist_titles = []
        _title_to_vid_local: Dict[str, str] = {}
        for _v in _shortlist_vids:
            _t = bulk.get(_v, {}).get("title") or ""
            _nt = _norm_title_for_match(_t)
            if _nt:
                _shortlist_titles.append(_nt)
                _title_to_vid_local[_nt] = _v
        from difflib import get_close_matches, SequenceMatcher
        # Ask for more matches than before (5 instead of 3) so the
        # date-based tiebreak has room to operate when several
        # similar-ish titles pass the ratio cutoff.
        matches = get_close_matches(needle_nt, _shortlist_titles,
                                     n=5, cutoff=0.80)
        if not matches:
            return ""
        # Apply date filter to ALL candidates above the cutoff. This
        # is the core of Scott's "date tiebreak when titles are
        # similar" rule — a title that fuzzy-matches several videos
        # only resolves if exactly one of them also lines up on
        # upload date.
        date_approved: List[Tuple[str, float]] = []  # (vid, ratio)
        for _m in matches:
            _v = _title_to_vid_local.get(_m)
            if not _v:
                continue
            if not _date_confirms(_v, local_day):
                continue
            _r = SequenceMatcher(None, needle_nt, _m).ratio()
            date_approved.append((_v, _r))
        if not date_approved:
            return ""
        if len(date_approved) == 1:
            return date_approved[0][0]
        # Multiple candidates both pass title + date. Take the one
        # with the highest ratio, but ONLY if it's clearly ahead of
        # the next one (>=0.05 margin) — otherwise it's still too
        # close to call and we decline rather than guess.
        date_approved.sort(key=lambda r: r[1], reverse=True)
        if date_approved[0][1] - date_approved[1][1] >= 0.05:
            return date_approved[0][0]
        return ""

    def _find_date_match(filepath: str,
                          local_dur: Optional[float]) -> str:
        """Match by file mtime YYYYMMDD == YT upload_date. When
        multiple YT videos land on the same day, disambiguate by
        duration (within 2s)."""
        try:
            _mtime = os.path.getmtime(filepath)
        except OSError:
            return ""
        try:
            import datetime as _dt
            _day = _dt.datetime.fromtimestamp(_mtime).strftime("%Y%m%d")
        except Exception:
            return ""
        cands = date_to_cands.get(_day, [])
        if not cands:
            return ""
        if len(cands) == 1:
            return cands[0][0]
        if local_dur is None or local_dur <= 0:
            return ""
        best = ""
        best_diff = 3.0  # must match within 2s (strict); 3s threshold
        for (_v, _nt, _yd) in cands:
            if _yd is None or _yd <= 0:
                continue
            _diff = abs(_yd - local_dur)
            if _diff < best_diff:
                best_diff = _diff
                best = _v
            elif abs(_diff - best_diff) < 0.5 and best:
                # Two videos same day, near-equal duration — ambiguous.
                return ""
        return best

    # Per-file progress tick — the real time sink on a 10k-video
    # channel is this loop (fuzzy shortlist iteration). Emit every
    # _MATCH_TICK_EVERY files OR _MATCH_TICK_SECS so the log reflects
    # ongoing work. Also note progress right before the loop starts
    # so the user sees the transition from "fetching catalog" to
    # "matching files".
    _MATCH_TICK_EVERY = 200
    _MATCH_TICK_SECS = 5.0
    _match_total = len(on_disk)
    if _match_total > 0:
        try:
            stream.emit([[f"  \u2014 Catalog has {len(bulk):,} videos \u00b7 "
                         f"matching {_match_total:,} local file(s)\u2026\n",
                         "simpleline"]])
        except Exception:
            pass
    _match_processed = 0
    _match_last_tick = time.time()

    for (_v, _t, _y, _m, _fp) in on_disk:
        if cancel_event is not None and cancel_event.is_set():
            break
        while (pause_event is not None and pause_event.is_set()
               and not (cancel_event is not None and cancel_event.is_set())):
            time.sleep(0.25)
        # Bump the tick BEFORE any continue branch so every file
        # counted toward progress, regardless of which strategy
        # path it took (skipped, resolved, or unresolved).
        _match_processed += 1
        _now = time.time()
        if (_match_total > 1000
                and (_match_processed % _MATCH_TICK_EVERY == 0
                     or (_now - _match_last_tick) >= _MATCH_TICK_SECS)
                and _match_processed < _match_total):
            try:
                _so_far = sum(resolved_by.values())
                stream.emit([[f"  \u2014 [{_match_processed:,}/"
                             f"{_match_total:,}] matched {_so_far:,} "
                             f"so far\u2026\n", "simpleline"]])
            except Exception:
                pass
            _match_last_tick = _now
        if _v:
            already_set += 1
            continue

        # Compute file's mtime day once — used by every title-based
        # strategy as a safety check (Scott: "if 2 videos have very
        # similar titles that could cause issues — maybe if more
        # than 1 hit, try and determine which is correct based on
        # date?"). yt-dlp's --mtime sets file mtime to the YT upload
        # date so this should be an exact match when the file is
        # untouched.
        _local_day = ""
        try:
            import datetime as _dt
            _local_day = _dt.datetime.fromtimestamp(
                os.path.getmtime(_fp)).strftime("%Y%m%d")
        except Exception:
            _local_day = ""

        # Strategy 1: info.json sidecar
        _side_vid = _read_info_json_vid(_fp)
        if _side_vid:
            to_backfill.append((_fp, _side_vid, "info_json"))
            resolved_by["info_json"] += 1
            continue

        _nt = _norm_title_for_match(_t)

        # Strategy 2: exact normalized title — no date check since
        # an exact normalized title collision is already strong
        # evidence; adding date would just shrink the coverage.
        if _nt and _nt not in title_ambiguous:
            _exact = title_to_vid.get(_nt, "")
            if _exact:
                to_backfill.append((_fp, _exact, "exact"))
                resolved_by["exact"] += 1
                continue

        # Strategy 3: substring (date-checked)
        if _nt:
            _sub = _find_substring_match(_nt, _local_day)
            if _sub:
                to_backfill.append((_fp, _sub, "substring"))
                resolved_by["substring"] += 1
                continue

        # Strategy 4: date (single-candidate, or duration-disambiguated)
        _local_dur = _local_durations.get(os.path.normpath(_fp))
        _by_date = _find_date_match(_fp, _local_dur)
        if _by_date:
            to_backfill.append((_fp, _by_date, "date"))
            resolved_by["date"] += 1
            continue

        # Strategy 5: fuzzy difflib (date-checked)
        if _nt:
            _fuzzy = _find_fuzzy_match(_nt, _local_day)
            if _fuzzy:
                to_backfill.append((_fp, _fuzzy, "fuzzy"))
                resolved_by["fuzzy"] += 1
                continue

        # Track ambiguous vs genuinely unresolvable
        if _nt and _nt in title_ambiguous:
            ambiguous_hits += 1
        else:
            unresolved += 1
        tried_failed_paths.append(_fp)

    resolved = sum(resolved_by.values())

    # ── Persist: UPDATE resolved rows + stamp tried-failed rows ──────

    _now_ts = time.time()
    try:
        from . import index as _idx
        conn = _idx._open()
        if conn is not None:
            with _idx._db_lock:
                for _fp, _vid, _how in to_backfill:
                    _vurl = f"https://www.youtube.com/watch?v={_vid}"
                    try:
                        conn.execute(
                            "UPDATE videos SET video_id=?, video_url=?, "
                            "id_backfill_tried_ts=? "
                            "WHERE filepath=? COLLATE NOCASE "
                            "AND (video_id IS NULL OR video_id='')",
                            (_vid, _vurl, _now_ts, _fp))
                    except Exception:
                        pass
                # Stamp tried-ts on rows that failed every strategy
                # so the UI can tell the user these are probably
                # genuinely unresolvable (title changed too much,
                # channel renamed them, etc.).
                for _fp in tried_failed_paths:
                    try:
                        conn.execute(
                            "UPDATE videos SET id_backfill_tried_ts=? "
                            "WHERE filepath=? COLLATE NOCASE "
                            "AND (video_id IS NULL OR video_id='')",
                            (_now_ts, _fp))
                    except Exception:
                        pass
                conn.commit()
    except Exception as _e:
        stream.emit_error(f"Backfill DB write failed: {_e}")

    took = time.time() - t0
    _parts = []
    if resolved:
        _parts.append(f"{resolved} resolved")
        _breakdown_bits = []
        if resolved_by["info_json"]:
            _breakdown_bits.append(f"{resolved_by['info_json']} .info.json")
        if resolved_by["exact"]:
            _breakdown_bits.append(f"{resolved_by['exact']} exact")
        if resolved_by["substring"]:
            _breakdown_bits.append(f"{resolved_by['substring']} substring")
        if resolved_by["date"]:
            _breakdown_bits.append(f"{resolved_by['date']} date+dur")
        if resolved_by["fuzzy"]:
            _breakdown_bits.append(f"{resolved_by['fuzzy']} fuzzy")
        if _breakdown_bits:
            _parts.append("(" + ", ".join(_breakdown_bits) + ")")
    if already_set:
        _parts.append(f"{already_set} already set")
    if ambiguous_hits:
        _parts.append(f"{ambiguous_hits} ambiguous")
    if unresolved:
        _parts.append(f"{unresolved} unresolved")
    if not _parts:
        _parts.append("no on-disk videos")
    _summary = " \u00b7 ".join(_parts)
    _tag = "simpleline_pink" if resolved else "dim"
    stream.emit([
        [" \u2014 ", "meta_bracket"],
        [f"{name}: {_summary} (took {took:.1f}s)\n", _tag],
    ])
    return {
        "ok": True,
        "resolved": resolved,
        "resolved_by": resolved_by,
        "already_set": already_set,
        "ambiguous": ambiguous_hits,
        "unresolved": unresolved,
        "unresolved_now_tried": len(tried_failed_paths),
        "took": took,
    }


def refresh_channel_comments(channel: Dict[str, Any],
                              stream: LogStreamer,
                              cancel_event: Optional[threading.Event] = None,
                              pause_event: Optional[threading.Event] = None,
                              only_recent_days: Optional[int] = None,
                              ) -> Dict[str, Any]:
    """Per-channel comment refresh. Re-fetches full metadata (via
    --dump-json --write-comments) for every on-disk video the
    channel has a metadata entry for. Motivating use case: videos
    caught within 30 min of upload typically have no "good"
    comments yet — this lets users pull a fresh top-50 a week later
    without re-fetching ALL metadata fields.

    `only_recent_days` optionally scopes to videos uploaded within
    the last N days (using the upload_date stored in the metadata
    entry) so a 4000-video channel doesn't take hours if you just
    want recent community updates. `None` = all videos.

    This is ALWAYS a slow path — comments require per-video API
    calls, no bulk mode exists — so the function is separate from
    bulk_refresh_views_likes (which is deliberately fast). Both
    can be run by the user independently; there's no dependency
    between them.

    Returns {ok, fetched, errors, skipped, took}.
    """
    folder = _folder_for_channel(channel)
    if folder is None:
        return {"ok": False, "error": "no output_dir"}
    yt = find_yt_dlp()
    if not yt:
        return {"ok": False, "error": "yt-dlp missing"}

    name = channel.get("name") or channel.get("folder") or "?"
    _scope = f" (last {only_recent_days}d)" if only_recent_days else ""
    stream.emit([["  \u2014 ", "meta_bracket"],
                 [f"Refreshing comments for {name}{_scope}...\n",
                  "simpleline"]])

    on_disk = _scan_channel_videos(folder)
    fp_by_id: Dict[str, str] = {}
    for (_v, _t, _y, _m, _fp) in on_disk:
        if _v and _fp:
            fp_by_id[_v] = _fp

    # Collect every existing metadata entry. For recent-days scope,
    # filter by upload_date stored on the entry.
    targets: List[Tuple[str, str]] = []  # (video_id, filepath)
    cutoff_yyyymmdd: Optional[str] = None
    if only_recent_days and only_recent_days > 0:
        from datetime import timedelta as _td
        cutoff_yyyymmdd = (datetime.now() - _td(days=only_recent_days)
                           ).strftime("%Y%m%d")
    for dp, _dns, fns in os.walk(str(folder)):
        for fn in fns:
            if not fn.endswith("Metadata.jsonl"):
                continue
            jp = os.path.join(dp, fn)
            for vid, entry in _read_metadata_jsonl(jp).items():
                if vid not in fp_by_id:
                    continue
                if cutoff_yyyymmdd:
                    ud = str(entry.get("upload_date") or "")
                    if not ud or ud < cutoff_yyyymmdd:
                        continue
                targets.append((vid, fp_by_id[vid]))

    total = len(targets)
    if total == 0:
        stream.emit([[" \u2014 No videos match the comment-refresh "
                      "scope.\n", "dim"]])
        return {"ok": True, "fetched": 0, "errors": 0, "skipped": 0,
                "took": 0}

    t0 = time.time()
    fetched = 0
    errors = 0
    for i, (vid, fp) in enumerate(targets, 1):
        if cancel_event is not None and cancel_event.is_set():
            break
        while (pause_event is not None and pause_event.is_set()
               and not (cancel_event is not None and cancel_event.is_set())):
            time.sleep(0.25)
        if i == 1 or i % 25 == 0:
            stream.emit_dim(
                f"    \u2014 comments refresh: {i}/{total}...")
        try:
            res = fetch_single_video_metadata(
                channel, vid, fp, stream, cancel_event=cancel_event,
                emit_inline_log=False)
            if res.get("ok"):
                fetched += 1
            elif not res.get("transient"):
                errors += 1
        except Exception:
            errors += 1

    # Stamp separate last-comments-refresh timestamp on the channel.
    try:
        from . import ytarchiver_config as _cfg
        cfg = _cfg.load_config()
        ch_url_norm = (channel.get("url") or "").rstrip("/")
        now_ts = time.time()
        for ch in cfg.get("channels", []):
            if (ch.get("url") or "").rstrip("/") == ch_url_norm:
                ch["last_comments_refresh_ts"] = now_ts
                break
        _cfg.save_config(cfg)
    except Exception:
        pass

    took = time.time() - t0
    _tag = "red" if errors else "simpleline_pink"
    stream.emit([
        [" \u2014 ", "meta_bracket"],
        [f"{name}: comments refreshed — {fetched} ok, "
         f"{errors} errors (took {took:.1f}s)\n", _tag],
    ])
    return {"ok": True, "fetched": fetched, "errors": errors,
            "skipped": 0, "took": took}


def fetch_channel_metadata(channel: Dict[str, Any],
                           stream: LogStreamer,
                           cancel_event: Optional[threading.Event] = None,
                           refresh: bool = False,
                           pause_event: Optional[threading.Event] = None,
                           scope: Optional[Dict[str, Any]] = None,
                           ) -> Dict[str, Any]:
    """Fill in missing metadata for this channel's on-disk videos.

    Two modes:
      - refresh=False (DEFAULT): DISK-DRIVEN. Enumerate videos on disk
        via `_scan_channel_videos` (filename `[id]` bracket first, then
        index-DB filepath lookup). Compare against existing JSONL IDs.
        Fetch only the missing handful. NO playlist walk — because the
        playlist would include ~hundreds of channel-videos that aren't
        downloaded, all of which are irrelevant for this job.
      - refresh=True: "Refresh views/likes" — delegates to
        bulk_refresh_views_likes() which does one flat-playlist call
        for all videos, then only full-fetches ones whose counts
        changed. Users reported the old every-video-full-fetch path
        taking 1h17m for a 404-video channel; the bulk path typically
        finishes in well under a minute.

    `scope` restricts which on-disk videos are considered:
      - `{"year": 2024}` — only videos whose upload year (from mtime)
        matches. Used by the Browse video-grid year-head context menu
        to offer year-scoped metadata refresh (feature H-14). Videos
        whose year can't be determined (mtime lookup failed) are
        excluded from scoped passes.
    """
    # Smart-refresh short-circuit: when the caller wants refresh=True,
    # go straight to the bulk path. That function knows to only
    # full-fetch videos with changed counts; for a channel where 99%
    # of view-counts haven't moved, it becomes ~1 API call instead
    # of ~N. Error cases (bulk returns empty) fall back below.
    if refresh:
        _res = bulk_refresh_views_likes(channel, stream,
                                        cancel_event=cancel_event,
                                        pause_event=pause_event,
                                        scope=scope,
                                        full_fetch_on_change=True)
        # Fall through to the old path ONLY if the bulk path couldn't
        # get any data at all (e.g. channel URL stripped, yt-dlp
        # returned empty, private channel). That path is still
        # useful as a safety net — it at least does the
        # disk-driven fetch for newly-added missing metadata.
        if _res.get("ok") or _res.get("bulk_fetched", 0) > 0:
            return _res
        stream.emit_dim(
            " (fast refresh returned nothing — falling back to "
            "per-video refresh)")
        # Continue into the legacy path below.

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
    # feature H-14: when scope has a year, banner shows the scope
    # ("Rechecking Foo (2024 only)..."); otherwise unchanged.
    _scope_year: Optional[int] = None
    if scope and isinstance(scope.get("year"), int):
        _scope_year = int(scope["year"])
    _scope_banner = f" ({_scope_year} only)" if _scope_year is not None else ""
    stream.emit([["  \u2014 ", "meta_bracket"],
                 [f"Rechecking {name}{_scope_banner}...\n", "simpleline"]])

    # 1. Enumerate videos ON DISK. `_scan_channel_videos` returns
    # (video_id, title, year, month, filepath) — video_id is
    # filled in either from filename `[id]` bracket or from the
    # index DB (filepath → video_id lookup).
    on_disk = _scan_channel_videos(folder)
    # feature H-14: year-scoped filter. Entries where year is None
    # (mtime couldn't be resolved) are excluded from scoped passes —
    # if we can't place them in a year, we can't honor the year scope.
    if _scope_year is not None:
        on_disk = [v for v in on_disk if v[2] == _scope_year]
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

    # 3. Enumerate existing THUMBNAILS. a case: 2 videos had
    # metadata JSONL entries but no thumbnail file on disk — the
    # earlier logic only checked metadata so those 2 showed as
    # "complete" and weren't re-fetched. Thumbnails are stored as
    # `<safe_title> [<video_id>].<ext>` inside `.Thumbnails/`
    # subfolders (one per year/month split). We extract the
    # bracketed video_id from each filename.
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
    # Dedupe via dict (preserves insertion order) so a video whose
    # id accidentally appears multiple times in `on_disk_ids`
    # (historical bug: 13 a user's channel files all got assigned the
    # fake id "a-user-channel" from filename-suffix parsing)
    # doesn't produce 13 identical fetch attempts.
    # Skip videos whose metadata fetch previously failed (deleted /
    # private / region-locked) — `_failed_fetch` set comes from
    # the DB and gets cleared on refresh=True.
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
    # the dark background. rule: "colored em dash indicating
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
        [" \u2014 ", "meta_bracket"],
        [" \u00b7 ".join(_parts) + "\n", "simpleline"],
    ])
    if n_without_id:
        # Some on-disk files couldn't be matched to a video_id via the
        # filename-bracket path or the index DB. a case: 2 recent
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
            [" \u26A0 ", "meta_bracket"],
            [f"{n_without_id:,} on-disk video(s) couldn't be matched "
             f"to a YouTube ID \u2014 checking channel for title "
             f"match...\n", "simpleline"],
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
                    [" \u26A0 ", "meta_bracket"],
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
                [" \u2713 ", "simpleline_green"],
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
                [" \u2014 ", "meta_bracket"],
                [f"{len(still_unmatched):,} file(s) couldn't be matched "
                 f"even by title (deleted? re-uploaded with new ID? "
                 f"filename edited?). Marked as permanent skip.\n",
                 "simpleline"],
            ])
            for _fp in still_unmatched[:5]:
                stream.emit([
                    [" \u2022 ", "meta_bracket"],
                    [f"{os.path.basename(_fp)}\n", "simpleline"],
                ])
            if len(still_unmatched) > 5:
                stream.emit([
                    [" ", "dim"],
                    [f"\u2026 and {len(still_unmatched) - 5:,} more\n", "dim"],
                ])
    if n_perm_no_id:
        stream.emit([
            [" \u2014 ", "meta_bracket"],
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
        stream.emit([[" \u2713 ", "simpleline_green"],
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
