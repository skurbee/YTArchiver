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
    # Use the read-only connection so this scan doesn't contend with
    # sync's register_video writers on `_db_lock` — critical because
    # this function is called from the parallel thumbnail walker.
    fp_to_id: Dict[str, str] = {}
    try:
        from . import index as _idx
        conn = _idx._reader_open() or _idx._open()
        if conn is not None:
            with _idx._reader_lock:
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
                                emit_inline_log: bool = True,
                                refresh: bool = False,
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
    if video_id in existing and not refresh:
        # Already have metadata for this id — nothing to do. No log.
        # `refresh=True` (comments refresh) bypasses this so the entry
        # gets re-fetched with current comments/views/likes.
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
        # Issues #139/#144/#148: tag with meta_done_<vid> so the
        # emit REPLACES the placeholder sync.py reserved under this
        # video's block rather than landing at log bottom after later
        # channels' rows have scrolled in.
        _md_marker = f"meta_done_{video_id}" if video_id else ""
        _md_tag = lambda *extra: [t for t in (_md_marker, *extra) if t]
        stream.emit([
            [" ", _md_tag("dim")],
            ["\u2014 \u2713 ", _md_tag("meta_bracket")],
            ["Metadata ", _md_tag("simpleline_pink")],
            ["downloaded\n", _md_tag("simpleline")],
        ])
    # Return the entry so callers (refresh_channel_comments) can
    # diff old-vs-new to count "unchanged" videos.
    return {"ok": True, "fetched": True, "entry": entry}


def _enter_pause_wait(stream: LogStreamer, label: str, queues) -> None:
    """Worker hit a pause-wait. Tell the queues UI ("actually paused")
    and emit a one-shot Paused log line so the user sees the pause
    take effect, not just see the button stop blinking. Mirrors the
    pattern in sync.py:_wait_if_paused so the log style matches.
    """
    if queues is not None:
        try: queues.set_sync_paused_active(True)
        except Exception: pass
    try:
        from datetime import datetime as _dt
        _now = _dt.now().strftime("%I:%M%p").lstrip("0").lower()
        stream.emit([
            ["\u23F8 Paused at ", "simpleline"],
            [_now, "simpleline"],
            [f" \u2014 {label} \u2014 click Resume.\n", "dim"],
        ])
    except Exception:
        pass


def _exit_pause_wait(stream: LogStreamer, label: str, queues) -> None:
    """Worker exiting pause-wait (resumed or cancelled)."""
    if queues is not None:
        try: queues.set_sync_paused_active(False)
        except Exception: pass
    try:
        from datetime import datetime as _dt
        _now = _dt.now().strftime("%I:%M%p").lstrip("0").lower()
        stream.emit([
            ["\u25B6 Resumed at ", "simpleline_green"],
            [_now, "simpleline_green"],
            [".\n", "dim"],
        ])
    except Exception:
        pass


def fetch_metadata_for_videos(channel: Dict[str, Any],
                              video_ids: Iterable[str],
                              stream: LogStreamer,
                              cancel_event: Optional[threading.Event] = None,
                              refresh: bool = False,
                              pause_event: Optional[threading.Event] = None,
                              queues=None,
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
        stream.emit_error("Archive folder isn't configured. Set it in Settings → General.")
        return {"ok": False, "error": "no output_dir"}
    folder.mkdir(parents=True, exist_ok=True)

    yt = find_yt_dlp()
    if not yt:
        stream.emit_error("Can't refresh video info — the download tool (yt-dlp) isn't installed.")
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

    # Simple-mode users see human-readable "Refreshing N video(s)..."
    # Verbose-mode users additionally see the technical "fast-fetch
    # N id(s)" label (dim-tagged so Simple mode filters it).
    stream.emit([
        ["  \u2014 ", "meta_bracket"],
        [f"{name} ", "simpleline"],
        ["\u2014 ", "meta_bracket"],
        [f"refreshing {len(ids)} video(s)\u2026\n", "simpleline"],
    ])
    stream.emit([
        ["    \u2014 ", ["dim"]],
        [f"fast-fetch {len(ids)} id(s)\n", ["dim"]],
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
            _enter_pause_wait(stream,
                              f"{channel.get('name', '?')} (metadata fetch)",
                              queues)
            while (pause_event.is_set()
                   and not (cancel_event is not None and cancel_event.is_set())):
                time.sleep(0.5)
            _exit_pause_wait(stream,
                             f"{channel.get('name', '?')} (metadata fetch)",
                             queues)
            if cancel_event is not None and cancel_event.is_set():
                break
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
                _enter_pause_wait(stream,
                                  f"{channel.get('name', '?')} (metadata fetch)",
                                  queues)
                while (pause_event.is_set()
                       and not (cancel_event is not None and cancel_event.is_set())):
                    time.sleep(0.5)
                _exit_pause_wait(stream,
                                 f"{channel.get('name', '?')} (metadata fetch)",
                                 queues)
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
                               pause_event: Optional[threading.Event] = None,
                               queues=None,
                               progress_cb: Optional[Callable[[int], None]] = None,
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
        # CRITICAL (2026-05-14): without `skip=webpage`, yt-dlp ≥2026.x
        # returns "NA" for view_count / like_count / comment_count on
        # every entry in a channel's `/videos` tab. The library parses
        # the initial webpage payload by default and that payload no
        # longer carries per-video stats. `skip=webpage` forces yt-dlp
        # to use the InnerTube playlist endpoint instead, which DOES
        # include view_count. Without this, bulk_refresh_views_likes
        # was silently skipping every video because the "new" count
        # was None and `_view_new != _view_old` short-circuited to
        # False. Empirically: 0% of vids had view counts without it;
        # 83% return real exact view counts with it.
        #
        # `skip=authcheck` is required IN COMBINATION when cookies are
        # passed (--cookies-from-browser firefox in our case). Without
        # it yt-dlp errors out: "Playlists that require authentication
        # may not extract correctly without a successful webpage
        # download". This pair is the supported workaround per
        # yt-dlp's own suggestion.
        "--extractor-args", "youtubetab:skip=webpage,authcheck",
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
    # CAPTURE stderr instead of throwing it away. Earlier this was
    # `DEVNULL`, which meant when bulk-stats came back empty we had
    # zero diagnostic — the user just saw "Bulk-stats returned no data"
    # without any clue why. Now we drain stderr on a side thread and,
    # if the call returns empty, the caller can emit the captured
    # stderr as a verbose-only `dim` line so the user (in Verbose mode)
    # can see the real yt-dlp error.
    out: Dict[str, Dict[str, Any]] = {}
    _stderr_buf: List[str] = []
    try:
        proc = subprocess.Popen(
            cmd, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            encoding="utf-8", errors="replace", bufsize=1,
            startupinfo=_startupinfo, env=_utf8_env(),
        )
    except OSError as e:
        stream.emit_error(f"Couldn't start fetching video stats: {e}")
        return {}
    def _drain_stderr():
        try:
            for line in proc.stderr:
                if line:
                    _stderr_buf.append(line.rstrip())
        except Exception:
            pass
    _stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
    _stderr_thread.start()
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
            if pause_event is not None and pause_event.is_set():
                _enter_pause_wait(stream, "catalog walk", queues)
                while (pause_event.is_set()
                       and not (cancel_event is not None and cancel_event.is_set())):
                    time.sleep(0.25)
                _exit_pause_wait(stream, "catalog walk", queues)
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
                # If a caller (e.g. bulk_refresh_views_likes) provided
                # a progress callback, fold the count into THEIR active
                # heartbeat line instead of emitting a separate
                # "Fetched N from catalog" line. That way the user sees
                # ONE updating line per channel, not two side-by-side.
                if progress_cb is not None:
                    try: progress_cb(_tick_count)
                    except Exception: pass
                else:
                    # In-place update on a single line ("backfill_progress"
                    # is registered in logs.js _inplaceKind so each emit
                    # with this marker replaces the previous one instead
                    # of appending). Cleared by clear_line when the final
                    # summary emits so the transient counter doesn't
                    # persist after completion.
                    try:
                        stream.emit([[f"  \u2014 Fetched {_tick_count:,} videos "
                                     f"from YouTube catalog\u2026\n",
                                     ["simpleline", "backfill_progress"]]])
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
    # If the call returned nothing useful, surface whatever yt-dlp put
    # on stderr as a verbose-only line so users in Verbose mode can
    # actually debug the failure. Simple mode users still just see the
    # higher-level "Initial check unsuccessful..." line emitted by the
    # caller. Cap at 6 lines so a yt-dlp traceback doesn't flood the
    # log; if the user needs more they can re-run with Verbose mode and
    # check the streamed stderr in the terminal.
    if not out and _stderr_buf:
        try:
            _stderr_thread.join(timeout=0.5)
        except Exception:
            pass
        _trimmed = [ln for ln in _stderr_buf if ln.strip()][:6]
        for _ln in _trimmed:
            stream.emit([
                ["   — yt-dlp: ", ["dim"]],
                [_ln + "\n", ["dim"]],
            ])

    # AUTO-RETRY for @handle URLs that fail bulk-stats. Discovered
    # 2026-05-15: yt-dlp 2026.03.17 + `youtubetab:skip=webpage,authcheck`
    # can't resolve some channel @handles (ColdFusion specifically:
    # "Failed to resolve url" error), but the same channel works via
    # the canonical /channel/UC.../videos URL form. The skip=webpage
    # arg is REQUIRED for bulk view counts (without it every entry's
    # view_count is "NA"), so we can't just drop the arg. Instead:
    # when the call returns empty AND the URL is the @handle form,
    # spend 1 extra yt-dlp call to resolve the channel_id, then retry
    # the bulk-stats call against /channel/UC.../videos. Saves the
    # 25+ minute per-video fallback for ColdFusion (and any other
    # channel where the handle path fails).
    if not out and "/@" in (ch_url or ""):
        canonical = _resolve_channel_id_url(yt, ch_url)
        if canonical and canonical != ch_url:
            stream.emit([
                ["   — ", ["dim"]],
                [f"retrying bulk-stats with canonical channel URL "
                 f"({canonical})\n", ["dim"]],
            ])
            # Recursive call into ourselves with the canonical URL.
            # Will not recurse twice because the canonical URL doesn't
            # contain /@ — so the retry guard above won't fire again.
            return _flat_playlist_bulk_stats(
                yt, canonical, stream, cancel_event, pause_event,
                queues=queues, progress_cb=progress_cb)
    return out


def _resolve_channel_id_url(yt: str, handle_url: str) -> str:
    """Convert a `/@handle` channel URL to the canonical
    `/channel/UC.../videos` form by asking yt-dlp for one video's
    channel_id. Returns empty string on failure.

    Costs one yt-dlp invocation (~2-4s) — used only as a one-off retry
    when bulk-stats fails for the handle form. Most channels never hit
    this path because their @handle resolves cleanly.
    """
    if not handle_url or not yt:
        return ""
    try:
        proc = subprocess.run(
            [yt, "--skip-download", "--no-warnings",
             "--print", "%(channel_id)s",
             "--playlist-end", "1",
             *_find_cookie_source(),
             handle_url],
            capture_output=True, text=True, timeout=20,
            encoding="utf-8", errors="replace",
            startupinfo=_startupinfo, env=_utf8_env(),
        )
        cid = (proc.stdout or "").strip().split("\n", 1)[0].strip()
        if cid and cid.startswith("UC") and len(cid) >= 20:
            return f"https://www.youtube.com/channel/{cid}/videos"
    except Exception:
        pass
    return ""


def bulk_refresh_views_likes(channel: Dict[str, Any],
                              stream: LogStreamer,
                              cancel_event: Optional[threading.Event] = None,
                              pause_event: Optional[threading.Event] = None,
                              scope: Optional[Dict[str, Any]] = None,
                              full_fetch_on_change: bool = False,
                              queues=None,
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
        stream.emit_error("Archive folder isn't configured. Set it in Settings → General.")
        return {"ok": False, "error": "no output_dir"}
    folder.mkdir(parents=True, exist_ok=True)

    yt = find_yt_dlp()
    if not yt:
        stream.emit_error("Can't refresh video info — the download tool (yt-dlp) isn't installed.")
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
    # Tag the per-channel transitional emits with `views_refresh_progress`
    # so each replaces the previous in-place. Cleared via clear_line just
    # before the final summary so Simple mode ends with the channel
    # header + the catalog-walk counter + the summary, no transitional
    # noise. (User asked: "should look like [3 lines], just delete those
    # lines when all is finished".)
    stream.emit([["  \u2014 ", ["meta_bracket", "views_refresh_progress"]],
                 [f"Refreshing {name}{_banner}...\n",
                  ["simpleline", "views_refresh_progress"]]])

    t0 = time.time()
    # Heartbeat thread — re-emits the in-place "Refreshing X..." line
    # every 3s with elapsed time + current sub-phase so the user always
    # sees motion. Without this the line sits silent while yt-dlp spins
    # up + walks the catalog (many seconds on cold-cookie firefox; can
    # be 30s+ before any output streams).
    _hb_phase = ["fetching catalog from YouTube"]  # mutable holder
    _hb_catalog_count = [0]  # running count from _flat_playlist_bulk_stats
    _hb_alive = [True]
    def _heartbeat():
        from .utils import format_elapsed as _fmt_el
        while _hb_alive[0]:
            time.sleep(3)
            if not _hb_alive[0]:
                break
            try:
                _el = int(time.time() - t0)
                # Fold the catalog count into the phase string when known
                # so the user sees ONE active line per channel, not two.
                _phase = _hb_phase[0]
                if _hb_catalog_count[0] > 0:
                    _phase = (f"{_phase} \u00b7 "
                              f"{_hb_catalog_count[0]:,} videos in catalog")
                stream.emit([
                    ["  \u2014 ", ["meta_bracket", "views_refresh_progress"]],
                    [f"Refreshing {name}{_banner} \u2014 {_phase} ({_fmt_el(_el)})\n",
                     ["simpleline", "views_refresh_progress"]],
                ])
            except Exception:
                pass
    _hb_thread = threading.Thread(target=_heartbeat, daemon=True)
    _hb_thread.start()

    def _catalog_progress(n):
        _hb_catalog_count[0] = int(n)

    bulk = _flat_playlist_bulk_stats(yt, ch_url, stream,
                                     cancel_event, pause_event,
                                     queues=queues,
                                     progress_cb=_catalog_progress)
    # Lock in the final catalog count once the walk completes.
    _hb_catalog_count[0] = max(_hb_catalog_count[0], len(bulk))
    _hb_phase[0] = "matching local files"
    if not bulk:
        _hb_alive[0] = False  # stop heartbeat
        # Replace the in-place "Refreshing X..." line with this warning
        # so the user sees the failure cleanly instead of two side-by-
        # side lines (the warning + the orphaned Refreshing... line).
        stream.emit([
            [" \u26A0 ", ["meta_bracket", "views_refresh_progress"]],
            [f"Initial check unsuccessful for {name} — "
             f"trying per-video lookup...\n",
             ["simpleline", "views_refresh_progress"]],
        ])
        # Verbose-only diagnostic. `dim` tag is in VERBOSE_ONLY_TAGS, so
        # Simple mode hides this line entirely while Verbose users still
        # see the technical context. Per Scott (2026-05-14): "Simple
        # mode should be easily readable for someone who knows literally
        # nothing about nothing."
        stream.emit([
            ["   — ", ["dim", "views_refresh_progress"]],
            [f"Bulk-stats returned no data for {name} — "
             f"channel may be empty / private / geo-locked, or yt-dlp "
             f"hit a transient YouTube block.\n",
             ["dim", "views_refresh_progress"]],
        ])
        return {"ok": False, "error": "bulk_empty",
                "fetched": 0, "refreshed": 0, "errors": 0, "skipped": 0,
                "bulk_fetched": 0}

    # Verbose-only: announce the bulk-stats walk landed and how many
    # videos it found. Helps the user follow the multi-phase flow.
    stream.emit([
        ["   — ", ["dim"]],
        [f"bulk-stats: {len(bulk):,} videos retrieved from YouTube "
         f"catalog (took {time.time() - t0:.1f}s)\n", ["dim"]],
    ])

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
            [" \u2014 ", ["meta_bracket", "views_refresh_progress"]],
            [f"Matched {len(_title_resolved)} video(s) by title "
             f"\u2014 saved their YouTube IDs.\n",
             ["simpleline", "views_refresh_progress"]],
        ])

    # \u2500\u2500 Removed-from-YT detection (cheap, runs every bulk refresh) \u2500\u2500
    # Walk the resolved on-disk list: any local file whose video_id
    # is NOT in the flat-playlist response was deleted / privated /
    # unlisted by the channel since download. Stamp the row so the UI
    # can show a red \u2717 on the per-video tile + a channel-level "N
    # gone from YT" counter. Inverse: any file currently marked
    # removed whose vid HAS returned to the catalog gets the
    # timestamp cleared (uploader restored / unprivated the video).
    try:
        _now_rm = time.time()
        _newly_removed: List[str] = []
        _newly_restored: List[str] = []
        from . import index as _idx
        _conn_rm = _idx._open()
        if _conn_rm is not None:
            _pat = str(folder) + "%"
            _db_state: Dict[str, Tuple[Optional[str], Optional[float]]] = {}
            with _idx._db_lock:
                for _row in _conn_rm.execute(
                        "SELECT filepath, video_id, removed_from_yt_ts "
                        "FROM videos WHERE filepath LIKE ?", (_pat,)):
                    _db_state[os.path.normpath(_row[0])] = (
                        _row[1], _row[2])
            for (_v, _t, _y, _m, _fp) in on_disk:
                if not _v:
                    continue
                _key = os.path.normpath(_fp)
                _db_vid, _db_removed_ts = _db_state.get(_key, (None, None))
                _is_in_catalog = (_v in bulk)
                if not _is_in_catalog and _db_removed_ts is None:
                    _newly_removed.append(_fp)
                elif _is_in_catalog and _db_removed_ts is not None:
                    _newly_restored.append(_fp)
            if _newly_removed or _newly_restored:
                with _idx._db_lock:
                    for _fp in _newly_removed:
                        try:
                            _conn_rm.execute(
                                "UPDATE videos SET removed_from_yt_ts=? "
                                "WHERE filepath=? COLLATE NOCASE",
                                (_now_rm, _fp))
                        except Exception:
                            pass
                    for _fp in _newly_restored:
                        try:
                            _conn_rm.execute(
                                "UPDATE videos SET removed_from_yt_ts=NULL "
                                "WHERE filepath=? COLLATE NOCASE",
                                (_fp,))
                        except Exception:
                            pass
                    _conn_rm.commit()
                if _newly_removed:
                    stream.emit([
                        [" \u26a0 ", ["meta_bracket", "views_refresh_progress"]],
                        [f"{len(_newly_removed)} video(s) no longer on "
                         f"YouTube (removed / privated since last sync).\n",
                         ["simpleline", "views_refresh_progress"]],
                    ])
                if _newly_restored:
                    stream.emit([
                        [" \u2713 ", ["meta_bracket", "views_refresh_progress"]],
                        [f"{len(_newly_restored)} previously-removed video(s) "
                         f"are back on YouTube.\n",
                         ["simpleline", "views_refresh_progress"]],
                    ])
    except Exception as _rm_e:
        try:
            stream.emit_error(
                f"removed-from-YT detection failed for {name}: {_rm_e}")
        except Exception:
            pass

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

        # "No flat data" detection (2026-05-14 fix): if flat-playlist
        # returned None for view_count BUT we have a stored value, we
        # can't tell whether it changed. The old code silently treated
        # this as "same" and skipped — meaning bulk refresh was a no-op
        # for any video yt-dlp's flat-playlist didn't return counts for.
        # Now we route it through the per-video fetch path so we
        # actually get current counts. ~17% of videos still need this
        # even with the `youtubetab:skip=webpage` extractor arg
        # (members-only, very recent uploads, etc.).
        _no_flat_data = (_view_new is None and _view_old is not None)

        # Always update the stats in-place — even if unchanged, bump
        # `fetched_at` so the "last refreshed" timestamp is accurate.
        if _view_new is not None:
            old["view_count"] = _view_new
        if _like_new is not None:
            old["like_count"] = _like_new
        if _comment_new is not None:
            old["comment_count"] = _comment_new
        old["fetched_at"] = datetime.now().isoformat()

        # VERBOSE-ONLY per-video diff trace. Compact one-liner showing
        # the old→new counts and the decision. With 1000s of videos
        # per channel this floods the log — that's intentional in
        # Verbose mode (Scott: "grossly oversaturated with information").
        # Simple mode hides via `dim` tag.
        def _fmt_cnt(n):
            return "—" if n is None else f"{n:,}"
        if _no_flat_data:
            _decision = "no flat data → full fetch"
        elif _changed and full_fetch_on_change:
            _decision = "changed → full fetch"
        elif _changed:
            _decision = "changed → in-place update"
        else:
            _decision = "unchanged → skip"
        stream.emit([
            ["    — ", ["dim"]],
            [f"{vid} · views {_fmt_cnt(_view_old)}→{_fmt_cnt(_view_new)} · "
             f"likes {_fmt_cnt(_like_old)}→{_fmt_cnt(_like_new)} · "
             f"{_decision}\n", ["dim"]],
        ])

        if _no_flat_data:
            # Force a full per-video fetch — only path that can give
            # us a current view count for this vid.
            changed_ids.append(vid)
        elif _changed and full_fetch_on_change:
            changed_ids.append(vid)
        elif _changed:
            updated_in_place += 1
        else:
            skipped_same += 1

    # Persist the in-place-updated entries. Group by jsonl path so we
    # only rewrite each file once.
    _hb_phase[0] = "writing updated counts"
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
        _hb_phase[0] = (f"fetching full metadata for {_n} new entries"
                        if not full_fetch_on_change
                        else f"re-fetching details for {_n} updated videos")
        stream.emit([
            [" \u2014 ", ["meta_bracket", "views_refresh_progress"]],
            [f"{_n} video(s) {_what}\n",
             ["simpleline", "views_refresh_progress"]],
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
            # Pause-wait between videos. The user might have clicked
            # Pause minutes ago — they're waiting on this exact loop
            # to land here. Emit a Paused log line + signal active.
            if pause_event is not None and pause_event.is_set():
                _enter_pause_wait(stream, f"{name} (metadata refresh)", queues)
                while (pause_event.is_set()
                       and not (cancel_event is not None and cancel_event.is_set())):
                    time.sleep(0.25)
                _exit_pause_wait(stream, f"{name} (metadata refresh)", queues)
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
            # Update the heartbeat phase with [N/total] so the
            # 3-second heartbeat tick shows live progress.
            _hb_phase[0] = f"refreshing metadata [{_processed}/{_total}]"
            _last_tick_ts = time.time()

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

    # Stop the heartbeat thread BEFORE the clear_line + summary so
    # the in-place line doesn't get re-painted on top of the summary.
    _hb_alive[0] = False
    took = time.time() - t0
    # Drop all the per-channel transitional lines tagged with
    # `views_refresh_progress` ("Refreshing X...", "N video(s) have
    # updated counts...", "[N/M] fetching metadata..."). The summary
    # line below stays as the only post-completion artifact alongside
    # the catalog-walk counter (which uses `backfill_progress` and
    # is preserved). Mirrors the same clear_line pattern used by
    # backfill_video_ids' final summary.
    try:
        import json as _json
        stream.emit([[_json.dumps({
            "kind": "clear_line", "marker": "views_refresh_progress"}),
            "__control__"]])
    except Exception:
        pass
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


def _thumbnail_exists_for(thumb_dir: str, video_id: str) -> bool:
    """True iff any thumbnail file in `thumb_dir` carries `[video_id]`."""
    if not thumb_dir or not video_id or not os.path.isdir(thumb_dir):
        return False
    bracket = f"[{video_id}]"
    try:
        for fn in os.listdir(thumb_dir):
            if bracket in fn and fn.lower().endswith(
                    (".jpg", ".jpeg", ".png", ".webp")):
                return True
    except OSError:
        pass
    return False


def sweep_missing_thumbnails(channel: Dict[str, Any], stream=None
                              ) -> Dict[str, int]:
    """Issue #147/#158: scan a channel folder for .mp4 files that lack a
    thumbnail in `.Thumbnails/` and download any missing ones from the
    URLs cached in metadata.jsonl. Use after a sync pass to catch
    thumbnails that yt-dlp's bulk download missed (rate-limited, racy,
    transient network blips). Returns {checked, fetched, missing}.
    """
    folder = _folder_for_channel(channel)
    if not folder or not folder.exists():
        return {"checked": 0, "fetched": 0, "missing": 0}
    checked = fetched = still_missing = 0
    # _scan_channel_videos returns (vid_id, title, year, month, filepath).
    # Group by (year, month) so each metadata.jsonl is read exactly once
    # per bucket. The jsonl path depends on the channel's split_years +
    # split_months config — feed those plus the year/month into
    # `_get_metadata_jsonl_path` so it builds the right path. (Earlier
    # version passed the channel dict where `split_years: bool` was
    # expected and `sub` where the channel root was expected — single-
    # channel refetch threw "missing 1 required positional argument:
    # 'split_months'". Fixed by passing args correctly.)
    ch_root = str(folder)
    name = channel.get("name") or channel.get("folder") or ""
    split_years = bool(channel.get("split_years", False))
    split_months = bool(channel.get("split_months", False))
    # Pre-walk every .Thumbnails dir under the channel root once and
    # collect the full vid-id set. This means we treat thumbs as
    # "exists" if ANY .Thumbnails dir in the channel has them — not
    # just the one next to the mp4. Bug surfaced on The PrimeTime
    # where most thumbs lived in 2025/.Thumbnails/ but their mp4s
    # had been re-foldered to 2023/ and 2024/ — the refetcher kept
    # re-downloading thumbs that already existed in a sibling year.
    _id_re = re.compile(r"\[([A-Za-z0-9_-]{11})\]")
    _all_thumb_vids: set = set()
    try:
        for _dp, _dns, _fns in os.walk(ch_root):
            if os.path.basename(_dp) != ".Thumbnails":
                continue
            for _fn in _fns:
                if not _fn.lower().endswith(
                        (".jpg", ".jpeg", ".png", ".webp")):
                    continue
                _m = _id_re.search(_fn)
                if _m:
                    _all_thumb_vids.add(_m.group(1))
    except Exception:
        pass
    by_bucket: Dict[Tuple[Optional[int], Optional[int]],
                    List[Tuple[str, str]]] = {}
    for vid_id, _title, _y, _m, path in _scan_channel_videos(folder):
        if not vid_id:
            continue
        by_bucket.setdefault((_y, _m), []).append((path, vid_id))
    for (yr, mo), items in by_bucket.items():
        jp, sub = _get_metadata_jsonl_path(
            name, ch_root, split_years, split_months, yr, mo)
        thumb_dir = _ensure_thumbnails_dir(sub)
        meta = _read_metadata_jsonl(jp) if jp else {}
        for path, vid_id in items:
            checked += 1
            # Already covered somewhere in the channel? Skip the
            # re-download. (Was: only checked thumb_dir adjacent to
            # the mp4, missing channel-wide reorgs.)
            if vid_id in _all_thumb_vids:
                continue
            entry = meta.get(vid_id) or {}
            url = entry.get("thumbnail_url") or ""
            title = entry.get("title") or os.path.splitext(
                os.path.basename(path))[0]
            if not url:
                still_missing += 1
                continue
            try:
                _download_thumbnail(url, thumb_dir, title, vid_id,
                                     stream=stream)
                if _thumbnail_exists_for(thumb_dir, vid_id):
                    fetched += 1
                    _all_thumb_vids.add(vid_id)
                    # Mark the DB flag so Settings > Metadata's
                    # Thumbnails column reflects the new state on its
                    # next query without re-walking.
                    try:
                        from . import index as _idx
                        _c = _idx._open()
                        if _c is not None:
                            with _idx._db_lock:
                                _c.execute(
                                    "UPDATE videos SET has_thumbnail=1 "
                                    "WHERE video_id=?", (vid_id,))
                                _c.commit()
                    except Exception:
                        pass
                else:
                    still_missing += 1
            except Exception:
                still_missing += 1
    return {"checked": checked, "fetched": fetched,
            "missing": still_missing}


def realign_misplaced_thumbnails(channels: Optional[List[Dict[str, Any]]] = None,
                                  dry_run: bool = True,
                                  stream=None) -> Dict[str, Any]:
    """Survey + (optionally) move thumbnails that ended up in a different
    year/month folder than the mp4 they belong to.

    Mechanism: each thumbnail filename carries a `[video_id]` tag.
    For every `.Thumbnails/*.{jpg,jpeg,webp,png}` under each channel
    folder, look up the mp4 with that video_id in the index DB; if
    the mp4's parent folder differs from the thumbnail's parent
    folder (the one ABOVE its `.Thumbnails/` dir), the thumb is
    misplaced and should live next to the mp4.

    Same-volume rename via `os.replace` so no copy/delete cycle and
    no risk of corruption.

    `dry_run=True` (default) just reports; `dry_run=False` actually
    moves files. Returns:
      {
        scanned, aligned, misaligned, moved, skipped_dest_exists,
        orphan_no_db, per_channel: {name: {misaligned, moved, ...}}
      }
    """
    from . import index as _idx
    out_dir = (load_config() or {}).get("output_dir") or ""
    if not out_dir:
        return {"ok": False, "error": "no output_dir"}

    # Build vid → mp4_parent map from the DB (only rows where the
    # file actually exists). One query.
    vid_to_mp4_parent: Dict[str, str] = {}
    try:
        conn = _idx._reader_open() or _idx._open()
        if conn is not None:
            with _idx._reader_lock:
                for fp, vid in conn.execute(
                        "SELECT filepath, video_id FROM videos "
                        "WHERE video_id IS NOT NULL AND video_id<>''"):
                    if fp and vid and os.path.isfile(fp):
                        vid_to_mp4_parent[vid] = os.path.normpath(
                            os.path.dirname(fp))
    except Exception as e:
        if stream:
            try: stream.emit_error(f"Couldn't read the archive index for thumbnail repair: {e}")
            except Exception: pass
        return {"ok": False, "error": str(e)}

    if channels is None:
        channels = (load_config() or {}).get("channels", []) or []

    id_re = re.compile(r"\[([A-Za-z0-9_-]{11})\]")
    scanned = aligned = misaligned = moved = skipped_dest = orphan = 0
    per_channel: Dict[str, Dict[str, int]] = {}

    for ch in channels:
        name = ch.get("name") or ch.get("folder") or ""
        folder = ch.get("folder_override") or ch.get("folder") or name
        if not folder:
            continue
        ch_root = os.path.join(out_dir, folder)
        if not os.path.isdir(ch_root):
            continue
        pc = {"misaligned": 0, "moved": 0, "skipped_dest_exists": 0,
              "orphan_no_db": 0}
        for dp, _dns, fns in os.walk(ch_root):
            if os.path.basename(dp) != ".Thumbnails":
                continue
            thumb_parent = os.path.normpath(os.path.dirname(dp))
            for fn in fns:
                if not fn.lower().endswith(
                        (".jpg", ".jpeg", ".webp", ".png")):
                    continue
                m = id_re.search(fn)
                if not m:
                    continue
                scanned += 1
                vid = m.group(1)
                mp4_parent = vid_to_mp4_parent.get(vid)
                if mp4_parent is None:
                    orphan += 1
                    pc["orphan_no_db"] += 1
                    continue
                if thumb_parent.lower() == mp4_parent.lower():
                    aligned += 1
                    continue
                # Misaligned. Compute target.
                misaligned += 1
                pc["misaligned"] += 1
                target_dir = os.path.join(mp4_parent, ".Thumbnails")
                target_path = os.path.join(target_dir, fn)
                source_path = os.path.join(dp, fn)
                if os.path.exists(target_path):
                    # Duplicate already at destination — skip the move
                    # to avoid losing data. User can manually consolidate.
                    skipped_dest += 1
                    pc["skipped_dest_exists"] += 1
                    continue
                if dry_run:
                    continue
                # Actually move. Ensure target dir exists.
                try:
                    os.makedirs(target_dir, exist_ok=True)
                    os.replace(source_path, target_path)
                    moved += 1
                    pc["moved"] += 1
                    # Confirm flag for this vid (it should already
                    # be 1 if a prior walk ran, but ensure correctness
                    # so the Thumbnails column stays accurate).
                    try:
                        from . import index as _idx2
                        _c2 = _idx2._open()
                        if _c2 is not None:
                            with _idx2._db_lock:
                                _c2.execute(
                                    "UPDATE videos SET has_thumbnail=1 "
                                    "WHERE video_id=?", (vid,))
                                _c2.commit()
                    except Exception:
                        pass
                except Exception as e:
                    if stream:
                        try:
                            stream.emit_error(
                                f"realign: failed to move "
                                f"{source_path} → {target_path}: {e}")
                        except Exception: pass
        if any(v > 0 for v in pc.values()):
            per_channel[name] = pc

    if stream and not dry_run:
        try:
            stream.emit_text(
                f" — Realigned {moved} misplaced thumbnail(s) across "
                f"{len(per_channel)} channel(s). "
                f"({skipped_dest} skipped — duplicate at target.)",
                "simpleline_pink")
            stream.flush()
        except Exception:
            pass

    return {
        "ok": True,
        "scanned": scanned,
        "aligned": aligned,
        "misaligned": misaligned,
        "moved": moved,
        "skipped_dest_exists": skipped_dest,
        "orphan_no_db": orphan,
        "per_channel": per_channel,
        "dry_run": bool(dry_run),
    }


def _thumb_cache_path() -> str:
    """Path to the persisted thumbnail-coverage cache."""
    from .ytarchiver_config import APP_DATA_DIR
    return os.path.join(str(APP_DATA_DIR), "thumbnail_status_cache.json")


def _load_thumb_cache() -> Dict[str, Dict[str, Any]]:
    """Load the persisted thumbnail-status cache. Returns {} on miss
    or corruption. Shape: {channel_name_lower: {fingerprint, total,
    with_thumb, missing, ts}}.
    """
    p = _thumb_cache_path()
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return {}


def _save_thumb_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    """Persist the thumbnail-status cache. Atomic via tmp+replace."""
    p = _thumb_cache_path()
    tmp = p + ".tmp"
    try:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cache, f)
        os.replace(tmp, p)
    except OSError:
        try: os.remove(tmp)
        except OSError: pass


def _channel_fingerprint(folder: Path) -> float:
    """Max mtime across the channel folder + one level of subdirs.
    Adding a new download bumps the immediate parent dir's mtime, so
    a one-level walk is enough to detect new content. Mirrors the
    fingerprint pattern used by sweep_new_videos.
    """
    if not folder.exists():
        return 0.0
    try:
        mx = folder.stat().st_mtime
    except OSError:
        return 0.0
    try:
        for entry in os.scandir(folder):
            try:
                if entry.is_dir(follow_symlinks=False):
                    m = entry.stat(follow_symlinks=False).st_mtime
                    if m > mx:
                        mx = m
                    # One more level deep (covers year/month splits).
                    for sub in os.scandir(entry.path):
                        try:
                            if sub.is_dir(follow_symlinks=False):
                                ms = sub.stat(
                                    follow_symlinks=False).st_mtime
                                if ms > mx:
                                    mx = ms
                        except OSError:
                            pass
            except OSError:
                pass
    except OSError:
        pass
    return mx


def count_thumbnail_status_bulk(channels: List[Dict[str, Any]],
                                  force: bool = False
                                  ) -> Dict[str, Dict[str, Any]]:
    """Issue #154: count thumbnail coverage per channel. Returns
    {channel_lower: {total, with_thumb, missing}}.

    CACHED + INCREMENTAL (2026-05-13): results are persisted to
    `thumbnail_status_cache.json` keyed by channel name. On the next
    call we compare the channel folder's recursive mtime fingerprint
    against the cached value — unchanged channels return cached
    results instantly, changed channels get re-walked.

    `force=True` ignores the cache and re-walks every channel. Wire
    this from a "Force recheck" button when the user wants fresh
    numbers (e.g. after manually adding thumbnails outside the app).

    Parallelized via ThreadPoolExecutor for the channels that DO
    need a fresh walk — 8 workers because each is mostly waiting on
    pooled-drive I/O latency, not CPU.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    cache = {} if force else _load_thumb_cache()
    out: Dict[str, Dict[str, Any]] = {}
    needs_walk: List[Tuple[Dict[str, Any], Path, str, float]] = []

    # FAST PATH (2026-05-14): when `force=False`, query the DB column
    # `has_thumbnail` instead of walking disk. The column is populated
    # by the prior disk walk + by `sweep_missing_thumbnails` so it's
    # the source of truth most of the time. Falls back to the disk
    # walk for any channel that has ANY row with has_thumbnail=NULL
    # (means we haven't done the one-time backfill yet).
    if not force:
        try:
            from . import index as _idx
            conn = _idx._reader_open() or _idx._open()
            if conn is not None:
                with _idx._reader_lock:
                    # Per-channel: total, sum(has_thumbnail), count NULL.
                    # NULL count > 0 → channel needs a backfill walk.
                    db_stats = {}
                    for r in conn.execute(
                            "SELECT channel, COUNT(*) AS total, "
                            "  SUM(CASE WHEN has_thumbnail=1 THEN 1 ELSE 0 END) AS with_thumb, "
                            "  SUM(CASE WHEN has_thumbnail IS NULL THEN 1 ELSE 0 END) AS unknown "
                            "FROM videos GROUP BY channel"):
                        nm = (r[0] or "").lower()
                        db_stats[nm] = {
                            "total": int(r[1] or 0),
                            "with_thumb": int(r[2] or 0),
                            "unknown": int(r[3] or 0),
                        }
                # Apply to channels that have a fully-populated column.
                # Channels with ANY NULL fall through to the disk walk.
                for ch in (channels or []):
                    nm = (ch.get("name") or ch.get("folder") or "").lower()
                    if not nm:
                        continue
                    s = db_stats.get(nm)
                    if s and s["total"] > 0 and s["unknown"] == 0:
                        out[nm] = {
                            "total": s["total"],
                            "with_thumb": s["with_thumb"],
                            "missing": max(0, s["total"] - s["with_thumb"]),
                        }
        except Exception:
            pass

    # Pass 1: figure out which channels can use the cache.
    for ch in (channels or []):
        folder = _folder_for_channel(ch)
        name = (ch.get("name") or ch.get("folder") or "").lower()
        if not folder or not folder.exists() or not name:
            continue
        if name in out:
            # Already filled by the DB fast path above.
            continue
        fp = _channel_fingerprint(folder)
        cached = cache.get(name)
        if (not force and cached
                and cached.get("fingerprint") == fp
                and "total" in cached):
            out[name] = {
                "total": cached.get("total", 0),
                "with_thumb": cached.get("with_thumb", 0),
                "missing": cached.get("missing", 0),
            }
            continue
        needs_walk.append((ch, folder, name, fp))

    if not needs_walk:
        return out

    # Pass 2: walk the stale/missing channels in parallel.
    #
    # Bug fix (2026-05-14): the previous algorithm checked each mp4
    # against the .Thumbnails dir SITTING NEXT TO IT, missing thumbs
    # that lived elsewhere in the same channel (e.g. The PrimeTime
    # had 2025/.Thumbnails/ containing thumbs for files now in 2023/
    # and 2024/ after reorg — counter reported "42% thumbnails" when
    # disk actually held a 1:1 thumb-for-mp4 match). Now we collect
    # EVERY `[vid_id]` from EVERY .Thumbnails/ in the channel folder
    # once, then check membership per-mp4.
    def _count_one(item):
        ch, folder, name, fp = item
        total = with_thumb = 0
        # Collect every video_id present in any .Thumbnails/ under
        # this channel folder. One folder walk; cheap.
        all_thumb_vids: set = set()
        try:
            id_re = re.compile(r"\[([A-Za-z0-9_-]{11})\]")
            for dp, _dns, fns in os.walk(str(folder)):
                if os.path.basename(dp) != ".Thumbnails":
                    continue
                for fn in fns:
                    if not fn.lower().endswith(
                            (".jpg", ".jpeg", ".png", ".webp")):
                        continue
                    m = id_re.search(fn)
                    if m:
                        all_thumb_vids.add(m.group(1))
        except Exception:
            pass
        # Persist the per-vid has_thumbnail flag so the next call
        # hits the SQL fast path instead of re-walking. Bulk UPDATE
        # by `video_id` (channel-scoped to avoid cross-channel
        # collisions if two channels happen to share an id).
        rows_for_db: List[Tuple[int, str]] = []
        try:
            for vid_id, _title, _y, _m, path in _scan_channel_videos(folder):
                total += 1
                has = 1 if (vid_id and vid_id in all_thumb_vids) else 0
                if vid_id:
                    rows_for_db.append((has, vid_id))
                if has:
                    with_thumb += 1
        except Exception:
            pass
        # Write the flag back to the DB. One transaction per channel.
        try:
            if rows_for_db:
                from . import index as _idx
                _conn = _idx._open()
                if _conn is not None:
                    with _idx._db_lock:
                        _conn.executemany(
                            "UPDATE videos SET has_thumbnail=? "
                            "WHERE video_id=?",
                            rows_for_db)
                        _conn.commit()
        except Exception:
            pass
        return (name, fp, {
            "total": total,
            "with_thumb": with_thumb,
            "missing": max(0, total - with_thumb),
        })

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_count_one, item) for item in needs_walk]
        for fut in as_completed(futures):
            try:
                name, fp, stats = fut.result()
                out[name] = stats
                # Update cache with fresh values + fingerprint.
                cache[name] = {
                    "fingerprint": fp,
                    "total": stats["total"],
                    "with_thumb": stats["with_thumb"],
                    "missing": stats["missing"],
                    "ts": time.time(),
                }
            except Exception:
                pass

    _save_thumb_cache(cache)
    return out


# TTL cache for the Video-IDs GROUP-BY query. The query itself is
# usually <1s on a 92k-row table, but caching the result means the
# Metadata page is instant on every visit instead of running a fresh
# scan each open. TTL is short (60s) because the videos table churns
# every time sync downloads a video — long TTL would surface stale
# numbers right when the user is most likely looking.
_VIDEO_ID_CACHE_TTL_SEC = 60.0
_video_id_cache_state: Dict[str, Any] = {"ts": 0.0, "rows": {}}
# Audit #6: guard concurrent reads/writes from multiple worker threads
# (Settings > Metadata page loads from JS bridge thread; bulk refresh
# pipeline running on the sync worker; etc.). Reads + writes are
# fast (dict copy), so a single Lock is fine — no need for RLock.
_video_id_cache_lock = threading.Lock()


def count_video_id_status_bulk(channels: List[Dict[str, Any]],
                                  force: bool = False
                                  ) -> Dict[str, Dict[str, Any]]:
    """Single-query batch version of count_video_id_status.

    Returns {channel_name: {total, with_id, missing, tried_failed}}
    keyed by channel name (lowercased for case-insensitive lookup).
    Falls back to per-channel queries if the batch query fails.

    Why this exists: the per-channel function runs 3 COUNT(*) queries
    against a 9M+ row table, holding the FTS DB lock the whole time.
    With 100+ channels that's 300+ serialized queries — Settings >
    Metadata table took 30+ seconds to load and would visibly hang
    when another DB op (sweep_new_videos, ingest_jsonl) was holding
    the lock. This collapses the work into one GROUP BY query that
    completes in under a second on the same data.
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not channels:
        return out
    # TTL cache shortcut: if the same data was computed recently AND
    # the caller didn't ask for a force-refresh, return the cached
    # rows. Avoids hitting the DB on every Metadata-page open.
    if not force:
        try:
            with _video_id_cache_lock:
                now = time.time()
                age = now - float(_video_id_cache_state.get("ts") or 0)
                cached_rows = _video_id_cache_state.get("rows") or {}
                if cached_rows and age < _VIDEO_ID_CACHE_TTL_SEC:
                    # Return a shallow copy so callers can't mutate
                    # cached state from outside the lock.
                    return dict(cached_rows)
        except Exception:
            pass
    try:
        from . import index as _idx
        # Issue #153 follow-on: route through the read-only `_reader_conn`
        # so this Settings > Metadata table query never waits behind
        # sync's `register_video` writers on `_db_lock`. WAL handles
        # cross-connection serialization. Falls back to the shared
        # `_conn` if the reader isn't available.
        conn = _idx._reader_open() or _idx._open()
        if conn is None:
            return out
        # Use GROUP BY on the raw `channel` column (NOT LOWER(channel))
        # so the existing idx_vid_channel index can serve the query.
        # LOWER() forces a full table scan, which on a 9M-row table
        # took 30+ seconds per call. Case-folding for cross-case
        # matching happens in Python below — typically no-op since
        # channel names rarely vary in case across rows.
        with _idx._reader_lock:
            try:
                _has_tried_col = True
                _has_removed_col = True
                rows = conn.execute(
                    "SELECT channel, "
                    "  COUNT(*) AS total, "
                    "  SUM(CASE WHEN video_id IS NOT NULL AND video_id != '' "
                    "           THEN 1 ELSE 0 END) AS with_id, "
                    "  SUM(CASE WHEN (video_id IS NULL OR video_id = '') "
                    "           AND id_backfill_tried_ts IS NOT NULL "
                    "           THEN 1 ELSE 0 END) AS tried, "
                    "  SUM(CASE WHEN removed_from_yt_ts IS NOT NULL "
                    "           THEN 1 ELSE 0 END) AS removed "
                    "FROM videos GROUP BY channel"
                ).fetchall()
            except Exception:
                # Older DB without removed_from_yt_ts column.
                _has_removed_col = False
                try:
                    _has_tried_col = True
                    rows = conn.execute(
                        "SELECT channel, "
                        "  COUNT(*) AS total, "
                        "  SUM(CASE WHEN video_id IS NOT NULL AND video_id != '' "
                        "           THEN 1 ELSE 0 END) AS with_id, "
                        "  SUM(CASE WHEN (video_id IS NULL OR video_id = '') "
                        "           AND id_backfill_tried_ts IS NOT NULL "
                        "           THEN 1 ELSE 0 END) AS tried "
                        "FROM videos GROUP BY channel"
                    ).fetchall()
                except Exception:
                    _has_tried_col = False
                    rows = conn.execute(
                        "SELECT channel, "
                        "  COUNT(*) AS total, "
                        "  SUM(CASE WHEN video_id IS NOT NULL AND video_id != '' "
                        "           THEN 1 ELSE 0 END) AS with_id "
                        "FROM videos GROUP BY channel"
                    ).fetchall()
        # Merge case-variant channels in Python (e.g. "MyChan" + "mychan"
        # → one entry under "mychan"). Sums the counts so duplicates from
        # case-drifted rows aren't lost.
        for r in rows:
            ch_raw = r[0] or ""
            ch_low = ch_raw.lower()
            total = int(r[1] or 0)
            with_id = int(r[2] or 0)
            tried = int(r[3] or 0) if _has_tried_col and len(r) > 3 else 0
            removed = (int(r[4] or 0) if _has_removed_col and len(r) > 4
                       else 0)
            cur = out.get(ch_low)
            if cur is None:
                out[ch_low] = {
                    "total": total,
                    "with_id": with_id,
                    "missing": max(0, total - with_id),
                    "tried_failed": tried,
                    "removed_from_yt": removed,
                }
            else:
                cur["total"] += total
                cur["with_id"] += with_id
                cur["missing"] = max(0, cur["total"] - cur["with_id"])
                cur["tried_failed"] += tried
                cur["removed_from_yt"] = cur.get("removed_from_yt", 0) + removed
    except Exception:
        return {}
    # Refresh the TTL cache so the next page-load gets instant data.
    try:
        with _video_id_cache_lock:
            _video_id_cache_state["ts"] = time.time()
            _video_id_cache_state["rows"] = out
    except Exception:
        pass
    return out


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
        # Use the read-only connection so this single-channel fallback
        # doesn't contend with writers on `_db_lock`. Called from the
        # Settings > Metadata bulk loader when the GROUP-BY path can't
        # cover a specific channel (case drift between config name +
        # DB column).
        conn = _idx._reader_open() or _idx._open()
        if conn is None:
            return {"total": 0, "with_id": 0, "missing": 0, "tried_failed": 0}
        _pat = str(folder) + "%"
        with _idx._reader_lock:
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


def _probe_file_duration(filepath: str) -> Optional[float]:
    """Single-file ffprobe call returning duration in seconds. None on
    any error. Used by _probe_durations_bulk to fill `videos.duration_s`
    for files that came from the tkinter-era importer (which never
    probed duration, leaving NULL across the board).
    """
    try:
        proc = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries",
             "format=duration", "-of",
             "default=noprint_wrappers=1:nokey=1", filepath],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            startupinfo=_startupinfo, env=_utf8_env(),
            timeout=10, encoding="utf-8", errors="replace")
    except Exception:
        return None
    raw = (proc.stdout or "").strip()
    try:
        v = float(raw)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _probe_durations_bulk(filepaths: List[str], stream: LogStreamer,
                          cancel_event: Optional[threading.Event] = None,
                          pause_event: Optional[threading.Event] = None,
                          max_workers: int = 6,
                          ) -> Dict[str, Optional[float]]:
    """Probe duration for a batch of files in parallel.

    Reason this exists: backfill_video_ids' duration-match strategy
    needs `local_dur` to disambiguate same-day same-title YT
    candidates. The tkinter-era importer never populated
    `videos.duration_s`, so on migrated archives every duration is
    NULL — strategies that compare against duration get zero data
    and fail silently. This helper fills the gap with one ffprobe
    call per file, ~70ms each, parallelized 6-wide → ~12s for 1000
    files. Results write back to `videos.duration_s` so subsequent
    runs skip the probe entirely (the SELECT in the caller pulls
    them out of the DB).
    """
    out: Dict[str, Optional[float]] = {}
    if not filepaths:
        return out
    from concurrent.futures import ThreadPoolExecutor, as_completed
    _t0 = time.time()
    _total = len(filepaths)
    try:
        stream.emit([[f"  — Probing duration for {_total:,} file(s)"
                     f" via ffprobe…\n",
                     ["simpleline", "backfill_progress"]]])
    except Exception:
        pass
    _last_tick = time.time()
    _done = 0
    with ThreadPoolExecutor(max_workers=max_workers,
                            thread_name_prefix="dur-probe") as ex:
        fut_to_fp = {ex.submit(_probe_file_duration, fp): fp
                     for fp in filepaths}
        for fut in as_completed(fut_to_fp):
            fp = fut_to_fp[fut]
            try:
                out[fp] = fut.result()
            except Exception:
                out[fp] = None
            _done += 1
            if cancel_event is not None and cancel_event.is_set():
                break
            _now = time.time()
            if (_now - _last_tick) >= 1.5 and _done < _total:
                try:
                    stream.emit([[f"  — Probing duration "
                                 f"[{_done:,}/{_total:,}]…\n",
                                 ["simpleline", "backfill_progress"]]])
                except Exception:
                    pass
                _last_tick = _now
    # Persist probed durations to the DB so the next pass doesn't
    # re-probe the same files. One transaction, write only the
    # successful probes (None values stay NULL — re-probing them is
    # cheap and might succeed if the file was being written during
    # the first attempt).
    try:
        from . import index as _idx
        _conn = _idx._open()
        if _conn is not None:
            with _idx._db_lock:
                for _fp, _d in out.items():
                    if _d is None or _d <= 0:
                        continue
                    try:
                        _conn.execute(
                            "UPDATE videos SET duration_s=? "
                            "WHERE filepath=? COLLATE NOCASE "
                            "AND (duration_s IS NULL OR duration_s<=0)",
                            (_d, _fp))
                    except Exception:
                        pass
                _conn.commit()
    except Exception:
        pass
    try:
        _resolved_n = sum(1 for v in out.values() if v and v > 0)
        stream.emit([[f"  — Probed {_resolved_n:,}/{_total:,} duration(s)"
                     f" in {time.time() - _t0:.1f}s\n",
                     ["simpleline", "backfill_progress"]]])
    except Exception:
        pass
    return out


def _fetch_per_video_upload_dates(yt: str, vids: List[str],
                                   stream: LogStreamer,
                                   cancel_event: Optional[threading.Event] = None,
                                   pause_event: Optional[threading.Event] = None,
                                   max_workers: int = 4,
                                   queues=None,
                                   ) -> Dict[str, str]:
    """For each vid, run a per-video yt-dlp extraction to get the real
    upload_date (YYYYMMDD). Flat-playlist returns "NA" for upload_date,
    so this is the slow-but-thorough path that THOROUGH backfill mode
    uses to enable date-confirmed matching for unresolved files.

    Parallelized 4-wide to stay under YouTube's rate-limit. ~3s/vid
    sequentially → ~0.75s/vid wall-clock with 4 workers. Caller
    chooses the candidate shortlist; this helper just iterates.

    Returns {vid: "YYYYMMDD" or ""}. Failures are recorded as "" so
    the caller can tell "tried but didn't get a date" from "never tried".
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    out: Dict[str, str] = {}
    if not vids:
        return out
    _total = len(vids)
    _t0 = time.time()

    def _fetch_one(vid: str) -> Tuple[str, str]:
        url = f"https://www.youtube.com/watch?v={vid}"
        cmd = [yt, "--skip-download", "--no-warnings",
               "--print", "%(upload_date)s",
               *_find_cookie_source(), url]
        try:
            proc = subprocess.run(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                startupinfo=_startupinfo, env=_utf8_env(),
                timeout=30, encoding="utf-8", errors="replace")
        except Exception:
            return (vid, "")
        raw = (proc.stdout or "").strip()
        # Accept first valid YYYYMMDD on any line (yt-dlp may emit
        # multiple lines for live/upcoming videos).
        for line in raw.splitlines():
            line = line.strip()
            if line and len(line) == 8 and line.isdigit():
                return (vid, line)
        return (vid, "")

    _last_tick = time.time()
    _done = 0
    try:
        stream.emit([[f"  — Fetching upload_date for {_total:,} candidate"
                     f"(s) (thorough pass)…\n",
                     ["simpleline", "backfill_progress"]]])
    except Exception:
        pass
    with ThreadPoolExecutor(max_workers=max_workers,
                            thread_name_prefix="ud-fetch") as ex:
        fut_to_vid = {ex.submit(_fetch_one, v): v for v in vids}
        for fut in as_completed(fut_to_vid):
            if cancel_event is not None and cancel_event.is_set():
                break
            if pause_event is not None and pause_event.is_set():
                # Worker threads don't honor pause mid-call (they're
                # blocking on subprocess.run), but the AS_COMPLETED
                # loop can hold off scheduling new work — practical
                # effect is a brief delay before pause takes hold.
                while (pause_event.is_set()
                       and not (cancel_event is not None
                                and cancel_event.is_set())):
                    time.sleep(0.25)
            try:
                vid, date = fut.result()
                out[vid] = date
            except Exception:
                pass
            _done += 1
            _now = time.time()
            if (_now - _last_tick) >= 2.0 and _done < _total:
                _ok = sum(1 for v in out.values() if v)
                try:
                    stream.emit([[f"  — Thorough fetch "
                                 f"[{_done:,}/{_total:,}] · "
                                 f"{_ok:,} dates resolved…\n",
                                 ["simpleline", "backfill_progress"]]])
                except Exception:
                    pass
                _last_tick = _now
    try:
        _ok_n = sum(1 for v in out.values() if v)
        stream.emit([[f"  — Per-video date fetch: {_ok_n:,}/{_total:,}"
                     f" resolved in {time.time() - _t0:.1f}s\n",
                     ["simpleline", "backfill_progress"]]])
    except Exception:
        pass
    return out


def backfill_video_ids(channel: Dict[str, Any],
                       stream: LogStreamer,
                       cancel_event: Optional[threading.Event] = None,
                       pause_event: Optional[threading.Event] = None,
                       queues=None,
                       mode: str = "fast",
                       ) -> Dict[str, Any]:
    """One-shot video_id backfill with multi-strategy resolution.

    `mode="fast"` (default): use yt-dlp --flat-playlist once for the
    catalog (NA upload_date but real duration), ffprobe local files
    for duration, then run the title + duration strategies. The
    date-confirmed strategies still run but contribute ~0 because
    flat-playlist returns NA dates — they're harmless. Typical
    runtime 30-120s for a 1000-file channel.

    `mode="thorough"`: after the fast pass, take every file still
    unresolved, build a token-prefiltered shortlist of YT candidate
    vids per file, do a per-video yt-dlp call for each candidate
    vid (~3s sequential, ~0.75s 4-wide parallel) to fetch real
    upload_date, then re-run the date-confirmed strategies. Adds
    minutes-to-hours depending on unresolved count, but resolves
    the rename-heavy + same-duration-collision case that fast
    can't (e.g. daily late-night shows with constant ~13min run
    time).

    For every on-disk file without a video_id in the DB, try in order:

      1. `.info.json` sidecar (zero-cost, no network)
      2. Exact normalized-title match against YouTube's current
         flat-playlist
      3. Duration match (NEW): unique YT vid whose duration is
         within ±2s of the local file's ffprobe'd duration. The
         channel-level "EWU Bodycam" case where renamed titles
         leave 26% via exact match alone but durations are unique
         to the second is the textbook win for this strategy.
      4. Substring title match (local title subset of YT title, or
         vice-versa) — requires date confirmation, so silent on
         fast mode but contributes in thorough mode
      5. Upload-date match (file mtime YYYYMMDD → YT upload_date),
         disambiguated by duration when >1 candidate exists
      6. Fuzzy title match via difflib.get_close_matches (0.80
         cutoff, rejects ambiguous near-ties; high-confidence-no-
         date escape at 0.95)
      7. Stamp `id_backfill_tried_ts` so the UI can distinguish
         "tried but genuinely unresolvable" from "not yet attempted"

    Returns {ok, resolved, resolved_by, already_set, ambiguous,
             unresolved_now_tried, took, mode}.
    """
    folder = _folder_for_channel(channel)
    if folder is None:
        stream.emit_error("Archive folder isn't configured. Set it in Settings → General.")
        return {"ok": False, "error": "no output_dir"}
    yt = find_yt_dlp()
    if not yt:
        stream.emit_error("Can't look up missing video IDs — the download tool (yt-dlp) isn't installed.")
        return {"ok": False, "error": "yt-dlp missing"}
    name = channel.get("name") or channel.get("folder") or "?"
    ch_url = (channel.get("url") or "").strip()
    if not ch_url:
        stream.emit_error(f"{name} has no channel URL on file — can't look up missing video IDs.")
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
    # Inverse: integer-seconds duration → list of vids. The
    # duration-match strategy (Strategy 3) hits this. Bucketed at
    # 1-second resolution; the match function then sweeps ±2s
    # buckets to handle re-encode drift.
    duration_bucket_to_vids: Dict[int, List[str]] = {}

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
        if _dur_f is not None and _dur_f > 0:
            duration_bucket_to_vids.setdefault(
                int(round(_dur_f)), []).append(_vid)

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
    resolved_by = {"info_json": 0, "exact": 0, "duration": 0,
                   "substring": 0, "date": 0, "fuzzy": 0,
                   "thorough_substring": 0, "thorough_date": 0,
                   "thorough_fuzzy": 0}
    to_backfill: List[Tuple[str, str, str]] = []  # (filepath, vid, how)
    # Files that failed every strategy — stamp the tried timestamp.
    tried_failed_paths: List[str] = []

    # ── Local duration backfill (always runs in both modes) ──────────
    # Tkinter-era archives have NULL duration_s on every migrated row,
    # which kills the new duration-match strategy AND strategy 4's
    # multi-candidate disambiguation. ffprobe what's missing — write
    # back to the DB so subsequent runs skip the probe.
    _files_without_vid = [_fp for (_v, _t, _y, _m, _fp) in on_disk
                          if not _v]
    _files_needing_probe = [_fp for _fp in _files_without_vid
                            if _local_durations.get(os.path.normpath(_fp))
                            is None]
    if _files_needing_probe:
        _probed = _probe_durations_bulk(_files_needing_probe, stream,
                                         cancel_event=cancel_event,
                                         pause_event=pause_event)
        for _fp, _d in _probed.items():
            if _d is not None and _d > 0:
                _local_durations[os.path.normpath(_fp)] = _d

    # Helper used by Strategy 3 (duration match). Returns the unique
    # YT vid whose duration is within ±2s of the local file's, OR ""
    # if zero / multiple candidates exist (with title-similarity
    # tiebreak for the multi case when there's a clear winner).
    def _find_duration_match(local_dur: Optional[float],
                              needle_nt: str) -> str:
        if local_dur is None or local_dur <= 0:
            return ""
        # Sweep ±2 second buckets around the local duration.
        _center = int(round(local_dur))
        cands: List[str] = []
        for _off in (-2, -1, 0, 1, 2):
            for _v in duration_bucket_to_vids.get(_center + _off, []):
                _ydur = vid_to_duration.get(_v)
                if _ydur is None:
                    continue
                if abs(_ydur - local_dur) <= 2.0:
                    cands.append(_v)
        if not cands:
            return ""
        # De-dup (a vid could land in two adjacent buckets via int
        # rounding) but keep order so the "first match" path is stable.
        seen: set = set()
        uniq: List[str] = []
        for _v in cands:
            if _v not in seen:
                seen.add(_v)
                uniq.append(_v)
        if len(uniq) == 1:
            return uniq[0]
        # Multiple duration-near-ties. Only accept if there's a clear
        # title-similarity winner — otherwise fall through to other
        # strategies. Scott's "I'd rather have missing info than
        # incorrect info" rule applies here too.
        if not needle_nt:
            return ""
        from difflib import SequenceMatcher
        scored = []
        for _v in uniq:
            _yt = _norm_title_for_match(bulk.get(_v, {}).get("title") or "")
            if not _yt:
                continue
            _r = SequenceMatcher(None, needle_nt, _yt).ratio()
            scored.append((_v, _r))
        if not scored:
            return ""
        scored.sort(key=lambda r: r[1], reverse=True)
        # Need top ≥ 0.50 AND ≥ 0.15 clear of #2.
        if scored[0][1] < 0.50:
            return ""
        if len(scored) >= 2 and (scored[0][1] - scored[1][1]) < 0.15:
            return ""
        return scored[0][0]

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
        # Score everything up front so both the date-approved path
        # AND the high-confidence-no-date escape can reuse the
        # ratios without re-running SequenceMatcher.
        scored: List[Tuple[str, float]] = []  # (vid, ratio)
        for _m in matches:
            _v = _title_to_vid_local.get(_m)
            if not _v:
                continue
            _r = SequenceMatcher(None, needle_nt, _m).ratio()
            scored.append((_v, _r))
        if not scored:
            return ""

        # Apply date filter to ALL candidates above the cutoff. This
        # is the core of Scott's "date tiebreak when titles are
        # similar" rule — a title that fuzzy-matches several videos
        # only resolves if exactly one of them also lines up on
        # upload date.
        date_approved = [(v, r) for (v, r) in scored
                         if _date_confirms(v, local_day)]
        if len(date_approved) == 1:
            return date_approved[0][0]
        if len(date_approved) >= 2:
            # Multiple pass title + date. Take the highest ratio,
            # but only if >=0.05 clear of the next — otherwise too
            # close to call and we decline rather than guess.
            date_approved.sort(key=lambda r: r[1], reverse=True)
            if date_approved[0][1] - date_approved[1][1] >= 0.05:
                return date_approved[0][0]
            return ""

        # High-confidence-no-date escape (Scott's request). When the
        # date-approved path found nothing, fall back to accepting
        # a match if there's exactly ONE candidate with ratio
        # >= 0.95. That similarity is basically "near-identical
        # string"; two different videos rarely hit 0.95 on the
        # normalized-title form. Useful when a file's mtime was
        # bumped (re-encode, tool touch, missing --mtime on old
        # downloads) so the date check rejects an otherwise-
        # obvious match. Any ambiguity at the 0.95 threshold and
        # we bail — the conservative principle still holds.
        _HIGH_CONF = 0.95
        _high = [(v, r) for (v, r) in scored if r >= _HIGH_CONF]
        if len(_high) == 1:
            return _high[0][0]
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
            # Same "backfill_progress" marker so this transition
            # line REPLACES the last "Fetched N videos..." tick
            # (both phases share one in-place line).
            stream.emit([[f"  \u2014 Catalog has {len(bulk):,} videos \u00b7 "
                         f"matching {_match_total:,} local file(s)\u2026\n",
                         ["simpleline", "backfill_progress"]]])
        except Exception:
            pass
    _match_processed = 0
    _match_last_tick = time.time()

    for (_v, _t, _y, _m, _fp) in on_disk:
        if cancel_event is not None and cancel_event.is_set():
            break
        if pause_event is not None and pause_event.is_set():
            _enter_pause_wait(stream,
                              f"{channel.get('name', '?')} (ID backfill)",
                              queues)
            while (pause_event.is_set()
                   and not (cancel_event is not None and cancel_event.is_set())):
                time.sleep(0.25)
            _exit_pause_wait(stream,
                             f"{channel.get('name', '?')} (ID backfill)",
                             queues)
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
                             f"so far\u2026\n",
                             ["simpleline", "backfill_progress"]]])
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

        # Strategy 3 (NEW): duration match. Flat-playlist gives us
        # CLEAN per-second durations on every YT video; ffprobe gives
        # us clean local durations. When the local duration uniquely
        # matches one YT video within ±2s, accept — no date check
        # needed. For the EWU Bodycam case (~1 full + few shorts/day,
        # wildly different durations) this is the textbook win.
        _local_dur = _local_durations.get(os.path.normpath(_fp))
        if _local_dur is not None and _local_dur > 0:
            _by_dur = _find_duration_match(_local_dur, _nt)
            if _by_dur:
                to_backfill.append((_fp, _by_dur, "duration"))
                resolved_by["duration"] += 1
                continue

        # Strategy 4: substring (date-checked)
        if _nt:
            _sub = _find_substring_match(_nt, _local_day)
            if _sub:
                to_backfill.append((_fp, _sub, "substring"))
                resolved_by["substring"] += 1
                continue

        # Strategy 5: date (single-candidate, or duration-disambiguated)
        _by_date = _find_date_match(_fp, _local_dur)
        if _by_date:
            to_backfill.append((_fp, _by_date, "date"))
            resolved_by["date"] += 1
            continue

        # Strategy 6: fuzzy difflib (date-checked)
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

    # ── Thorough mode: per-video upload_date fetch for unresolved ────
    # Only runs when mode == "thorough" AND there's something the
    # fast pass couldn't resolve. Builds a candidate-vid union from
    # the token shortlists of each unresolved file's normalized title,
    # then fetches per-video upload_date for those (typically far
    # fewer than the full catalog). With real upload_dates in hand,
    # re-runs strategies 4-6 only for the unresolved files.
    _thorough_attempted = False
    if (mode == "thorough" and tried_failed_paths
            and not (cancel_event is not None and cancel_event.is_set())):
        _thorough_attempted = True
        # Snapshot unresolved files' data (we no longer have the
        # _t, _y, _m, _fp tuple at this point).
        _unresolved_meta: List[Tuple[str, str, str]] = []
        # Re-derive (filepath, norm_title, local_day) for each.
        _unres_set = set(tried_failed_paths)
        for (_v0, _t0, _y0, _m0, _fp0) in on_disk:
            if _fp0 not in _unres_set:
                continue
            _nt0 = _norm_title_for_match(_t0)
            try:
                import datetime as _dt
                _ld0 = _dt.datetime.fromtimestamp(
                    os.path.getmtime(_fp0)).strftime("%Y%m%d")
            except Exception:
                _ld0 = ""
            _unresolved_meta.append((_fp0, _nt0, _ld0))

        # Union of token-shortlists across all unresolved files.
        # Each unresolved file contributes up to ~20 candidates;
        # heavy token overlap keeps the union much smaller than
        # (#unresolved * 20).
        _candidate_vids: set = set()
        for (_fp0, _nt0, _ld0) in _unresolved_meta:
            if not _nt0:
                continue
            _toks = [t for t in _nt0.split() if len(t) >= 3]
            if len(_toks) < 2:
                continue
            from collections import Counter as _Counter
            _c: _Counter = _Counter()
            for _tok in _toks:
                if _tok in token_to_vids:
                    for _vc in token_to_vids[_tok]:
                        _c[_vc] += 1
            # Threshold ≥ 2 shared tokens (same as substring/fuzzy
            # prefilter). Cap at top 20 per file to avoid runaway
            # candidate sets on titles with many common tokens.
            _shortlist = [v for v, n in _c.most_common(20) if n >= 2]
            _candidate_vids.update(_shortlist)
        # Also include vids that share a duration bucket with any
        # unresolved file's local duration — captures the case
        # where Strategy 3 had a multi-match and bailed without a
        # clear title winner. Adds dates so Strategy 5 might bite.
        for (_fp0, _nt0, _ld0) in _unresolved_meta:
            _ld = _local_durations.get(os.path.normpath(_fp0))
            if _ld is None or _ld <= 0:
                continue
            _ctr = int(round(_ld))
            for _off in (-2, -1, 0, 1, 2):
                _candidate_vids.update(
                    duration_bucket_to_vids.get(_ctr + _off, []))
        # Strip vids that already have an upload_date (no point
        # re-fetching).
        _candidate_vids = {v for v in _candidate_vids
                           if not (bulk.get(v, {}).get("upload_date") or "")
                              .strip().isdigit()
                           or len((bulk.get(v, {}).get("upload_date")
                                   or "").strip()) != 8}
        _cand_list = sorted(_candidate_vids)
        if _cand_list:
            _yt = find_yt_dlp()
            _fetched = _fetch_per_video_upload_dates(
                _yt, _cand_list, stream,
                cancel_event=cancel_event, pause_event=pause_event,
                queues=queues)
            # Patch upload_date back into `bulk` and rebuild
            # date_to_cands so the date-checked strategies have
            # data to work with.
            for _vid, _date in _fetched.items():
                if _date and len(_date) == 8 and _date.isdigit():
                    if _vid in bulk:
                        bulk[_vid]["upload_date"] = _date
                    date_to_cands.setdefault(_date, []).append(
                        (_vid,
                         _norm_title_for_match(
                             bulk.get(_vid, {}).get("title") or ""),
                         vid_to_duration.get(_vid)))

            # Re-run strategies 4-6 for unresolved files with new dates.
            _still_unresolved: List[str] = []
            for (_fp0, _nt0, _ld0) in _unresolved_meta:
                if cancel_event is not None and cancel_event.is_set():
                    _still_unresolved.append(_fp0)
                    continue
                _ldur = _local_durations.get(os.path.normpath(_fp0))
                # Strategy 4 retry (substring + real date)
                if _nt0:
                    _sub2 = _find_substring_match(_nt0, _ld0)
                    if _sub2:
                        to_backfill.append((_fp0, _sub2, "thorough_substring"))
                        resolved_by["thorough_substring"] += 1
                        continue
                # Strategy 5 retry (date + duration)
                _by_date2 = _find_date_match(_fp0, _ldur)
                if _by_date2:
                    to_backfill.append((_fp0, _by_date2, "thorough_date"))
                    resolved_by["thorough_date"] += 1
                    continue
                # Strategy 6 retry (fuzzy + real date)
                if _nt0:
                    _fz2 = _find_fuzzy_match(_nt0, _ld0)
                    if _fz2:
                        to_backfill.append((_fp0, _fz2, "thorough_fuzzy"))
                        resolved_by["thorough_fuzzy"] += 1
                        continue
                _still_unresolved.append(_fp0)
            # Update counters now that some unresolved files got
            # resolved via the thorough pass.
            _newly_resolved_thorough = (resolved_by["thorough_substring"]
                                        + resolved_by["thorough_date"]
                                        + resolved_by["thorough_fuzzy"])
            unresolved = max(0, unresolved - _newly_resolved_thorough)
            tried_failed_paths = _still_unresolved

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
        stream.emit_error(f"Couldn't save the recovered video IDs: {_e}")

    took = time.time() - t0
    _parts = []
    if resolved:
        _parts.append(f"{resolved} resolved")
        _breakdown_bits = []
        if resolved_by["info_json"]:
            _breakdown_bits.append(f"{resolved_by['info_json']} .info.json")
        if resolved_by["exact"]:
            _breakdown_bits.append(f"{resolved_by['exact']} exact")
        if resolved_by["duration"]:
            _breakdown_bits.append(f"{resolved_by['duration']} duration")
        if resolved_by["substring"]:
            _breakdown_bits.append(f"{resolved_by['substring']} substring")
        if resolved_by["date"]:
            _breakdown_bits.append(f"{resolved_by['date']} date+dur")
        if resolved_by["fuzzy"]:
            _breakdown_bits.append(f"{resolved_by['fuzzy']} fuzzy")
        _thorough_total = (resolved_by["thorough_substring"]
                           + resolved_by["thorough_date"]
                           + resolved_by["thorough_fuzzy"])
        if _thorough_total:
            _breakdown_bits.append(f"{_thorough_total} thorough")
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
    # Clear the sticky "backfill_progress" in-place line so the
    # transient counter ("Fetched 1,000 videos from YouTube
    # catalog...") doesn't linger beside the final summary.
    try:
        import json as _json
        stream.emit([[_json.dumps({
            "kind": "clear_line", "marker": "backfill_progress"}),
            "__control__"]])
    except Exception:
        pass
    # Final summary tag: pink on newly-resolved work, plain-white
    # simpleline otherwise. The dim tag made "already-set + some
    # unresolved" summaries look faded even though they're a
    # normal successful outcome — no new work to do because every
    # video was already set. Only fall back to dim if there was
    # literally nothing to report (no on-disk videos).
    if resolved:
        _tag = "simpleline_pink"
    elif _parts == ["no on-disk videos"]:
        _tag = "dim"
    else:
        _tag = "simpleline"
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
        "mode": mode,
        "thorough_attempted": _thorough_attempted,
    }


def refresh_channel_comments(channel: Dict[str, Any],
                              stream: LogStreamer,
                              cancel_event: Optional[threading.Event] = None,
                              pause_event: Optional[threading.Event] = None,
                              only_recent_days: Optional[int] = None,
                              queues=None,
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
    # (video_id, filepath, title_hint, old_comments). Capturing
    # old_comments here lets us count "unchanged" videos after
    # the refetch without re-reading the JSONL.
    targets: List[Tuple[str, str, str, list]] = []
    cutoff_yyyymmdd: Optional[str] = None
    if only_recent_days and only_recent_days > 0:
        from datetime import timedelta as _td
        cutoff_yyyymmdd = (datetime.now() - _td(days=only_recent_days)
                           ).strftime("%Y%m%d")
    # Mid-channel resume support. When the task is paused mid-run and
    # the app is closed + reopened, the queue restores `current_sync`
    # back to the front of the sync queue and we land here again with
    # the same dict. Without this, we'd rebuild targets from i=1 and
    # silently re-fetch the videos we already did in the prior partial
    # pass. Track a `_pass_start_ts` on the task dict the FIRST time
    # we run; on subsequent resumptions of the same dict, filter out
    # any entry whose fetched_at >= _pass_start_ts (already refreshed
    # in this pass). Manual re-trigger = brand-new dict, so no skip.
    #
    # NB: queues.set_current_sync uses copy.deepcopy(), so mutating
    # `channel` alone DOESN'T propagate to queues.current_sync and
    # therefore wouldn't survive a save_now. We re-call set_current_sync
    # with the mutated dict to push the new field through.
    _pass_start_ts: float = float(channel.get("_pass_start_ts") or 0.0)
    if _pass_start_ts <= 0:
        _pass_start_ts = time.time()
        channel["_pass_start_ts"] = _pass_start_ts
        if queues is not None:
            try:
                queues.set_current_sync(channel)
                queues.save_debounced()
            except Exception:
                pass
    def _entry_already_done_this_pass(entry: dict) -> bool:
        fa = entry.get("fetched_at") or ""
        if not fa:
            return False
        try:
            # ISO format from datetime.now().isoformat() — no tz, local.
            ts = datetime.fromisoformat(str(fa)).timestamp()
        except (ValueError, TypeError):
            return False
        return ts >= _pass_start_ts
    _skipped_already_done = 0
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
                if _entry_already_done_this_pass(entry):
                    _skipped_already_done += 1
                    continue
                _title = str(entry.get("title") or "")
                _old_comments = entry.get("comments") or []
                targets.append((vid, fp_by_id[vid], _title, _old_comments))
    if _skipped_already_done > 0:
        stream.emit_dim(
            f"    — resuming: skipping {_skipped_already_done} video(s) "
            f"already refreshed in this pass")

    total = len(targets)
    if total == 0:
        stream.emit([[" \u2014 No videos match the comment-refresh "
                      "scope.\n", "dim"]])
        return {"ok": True, "fetched": 0, "errors": 0, "skipped": 0,
                "took": 0}

    # Sticky live-updating progress line \u2014 mirrors fetch_metadata_for_videos.
    # Each emission clears the previous "comments_refresh_active" line and
    # writes a new one at the bottom, so the user sees [N/total] tick up
    # in place. Without this, a 100-video channel takes ~15 minutes with
    # no UI feedback (Simple mode filters dim-tagged progress lines).
    import json as _json
    def _emit_active(_i: int, _n: int):
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "comments_refresh_active"}),
             "__control__"],
        ])
        stream.emit([
            ["    [", ["meta_bracket", "comments_refresh_active"]],
            [str(_i), ["simpleline", "comments_refresh_active"]],
            ["/", ["meta_bracket", "comments_refresh_active"]],
            [str(_n), ["simpleline", "comments_refresh_active"]],
            ["] ", ["meta_bracket", "comments_refresh_active"]],
            ["Refreshing comments: ", ["meta_bracket", "comments_refresh_active"]],
            [f"{name}\u2026\n", ["simpleline", "comments_refresh_active"]],
        ])

    def _clear_active():
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "comments_refresh_active"}),
             "__control__"],
        ])

    t0 = time.time()
    fetched = 0
    errors = 0
    unchanged = 0
    for i, (vid, fp, title_hint, old_comments) in enumerate(targets, 1):
        if cancel_event is not None and cancel_event.is_set():
            break
        if pause_event is not None and pause_event.is_set():
            _clear_active()
            _enter_pause_wait(stream,
                              f"{channel.get('name', '?')} (comments refresh)",
                              queues)
            while (pause_event.is_set()
                   and not (cancel_event is not None and cancel_event.is_set())):
                time.sleep(0.25)
            _exit_pause_wait(stream,
                             f"{channel.get('name', '?')} (comments refresh)",
                             queues)
        _emit_active(i, total)
        try:
            res = fetch_single_video_metadata(
                channel, vid, fp, title_hint, stream,
                emit_inline_log=False, refresh=True)
            if res.get("ok"):
                fetched += 1
                # Did the comments actually change? Python list-
                # of-dict equality is byte-exact; this catches
                # like-count updates AND comment add/remove.
                _new_comments = (res.get("entry") or {}).get("comments") or []
                if _new_comments == old_comments:
                    unchanged += 1
            elif not res.get("transient"):
                errors += 1
        except Exception:
            errors += 1
    _clear_active()

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
    # Lazy import to avoid circular (sync.py imports metadata).
    from .sync import _fmt_duration as _dur
    took_str = _dur(took)
    # Color discipline: when ANY comments refreshed, keep the
    # success portion pink and only the errors count goes red.
    # A single deleted/private video shouldn't paint the whole
    # line red when 80 others succeeded.
    base_color = "simpleline_pink" if fetched > 0 else "red"
    segs = [
        [" \u2014 ", "meta_bracket"],
        [f"{name}: comments refreshed — ", base_color],
        [f"{fetched} ok", base_color],
    ]
    if unchanged:
        segs.append([", ", base_color])
        segs.append([f"{unchanged} unchanged", base_color])
    if errors:
        segs.append([", ", base_color])
        segs.append([f"{errors} errors", "red"])
    segs.append([f" (took {took_str})\n", base_color])
    stream.emit(segs)
    return {"ok": True, "fetched": fetched, "errors": errors,
            "unchanged": unchanged, "skipped": 0, "took": took}


def fetch_channel_metadata(channel: Dict[str, Any],
                           stream: LogStreamer,
                           cancel_event: Optional[threading.Event] = None,
                           refresh: bool = False,
                           pause_event: Optional[threading.Event] = None,
                           scope: Optional[Dict[str, Any]] = None,
                           queues=None,
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
                                        full_fetch_on_change=True,
                                        queues=queues)
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
        stream.emit_error("Archive folder isn't configured. Set it in Settings → General.")
        return {"ok": False, "error": "no output_dir"}
    folder.mkdir(parents=True, exist_ok=True)

    yt = find_yt_dlp()
    if not yt:
        stream.emit_error("Can't refresh video info — the download tool (yt-dlp) isn't installed.")
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
    # FIX (2026-05-14): the ratio used to read `len(have_meta)/len(on_disk_ids)`
    # which was misleading because `have_meta` includes orphan files for
    # videos no longer on disk. Example seen with ColdFusion:
    #   `metadata: 513/512 (0 missing)` \u2014 513 metadata files but only
    #    512 unique video IDs on disk; 1 orphan metadata file.
    #   `thumbnails: 497/512 (20 missing)` \u2014 497 thumbs but only 492
    #    match current videos (5 orphan); display claimed 20 missing
    #    but 512-497=15 didn't math.
    # Now X = covered (videos WITH the asset), Y = total on-disk videos,
    # so X/Y is a real coverage ratio and X + missing = Y always holds.
    # Orphan files get their own callout when present.
    n_videos = len(on_disk_ids)
    covered_meta = n_videos - missing_meta
    covered_thumb = n_videos - missing_thumb
    orphan_meta = max(0, len(have_meta) - covered_meta)
    orphan_thumb = max(0, len(have_thumb) - covered_thumb)
    def _coverage_str(label: str, covered: int, total: int,
                       missing: int, orphan: int) -> str:
        s = f"{label}: {covered:,}/{total:,}"
        if missing and orphan:
            s += f" ({missing:,} missing, {orphan:,} stale)"
        elif missing:
            s += f" ({missing:,} missing)"
        elif orphan:
            s += f" \u2713 ({orphan:,} stale)"
        else:
            s += " \u2713"
        return s
    _meta_str = _coverage_str("metadata", covered_meta, n_videos,
                               missing_meta, orphan_meta)
    _thumb_str = _coverage_str("thumbnails", covered_thumb, n_videos,
                                missing_thumb, orphan_thumb)
    _parts = [
        f"{len(on_disk):,} on disk \u00b7 "
        f"{_meta_str} \u00b7 "
        f"{_thumb_str}"
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
                                     pause_event=pause_event,
                                     queues=queues)


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
