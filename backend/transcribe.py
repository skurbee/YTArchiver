"""
Transcribe — manages the persistent faster-whisper subprocess + a GPU queue.

Architecture (mirrors YTArchiver.py:9102 _start_whisper_process):
  - A single long-lived Python 3.11 subprocess runs `whisper_worker.py`
  - Model loads once (can be several GB / many seconds on first run)
  - Requests queued in memory; worker processes one at a time
  - Progress + results stream via JSON on stdout

Output file layout (must match YTArchiver.py for drop-in replacement):
  {ch_name} Transcript.txt (no split)
  {year}/{ch_name} {year} Transcript.txt (year-split)
  {year}/{MM Month}/{ch_name} {Month} {YY} Transcript.txt (year+month split)

  Entry format inside the .txt file (triple-newline separated):
    ===({title}), ({MM.DD.YYYY}), ({H:MM:SS}), ({SOURCE})===
    {transcript text}

  Hidden sidecar: .{ch_name} ... Transcript.jsonl next to the .txt, one
  JSON per segment with long-form keys:
    {"video_id":..., "title":..., "start":..., "end":..., "text":...,
     "words":[{"w":..., "s":..., "e":...}, ...]}
"""

from __future__ import annotations

import ctypes
import glob
import json
import os
import queue
import re
import shutil
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from .log_stream import LogStreamer


_startupinfo = None
if os.name == "nt":
    _startupinfo = subprocess.STARTUPINFO()
    _startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    _startupinfo.wShowWindow = 0


# ── OLD YTArchiver-compatible transcript file helpers ──────────────────
# These mirror the file layout + content format the legacy YTArchiver.py
# uses so we're a bit-for-bit drop-in replacement. Do NOT change these
# names or formats — OLD's scan/match logic depends on them exactly.

# Shared with metadata.py + reorg.py — see backend.utils.MONTH_FOLDERS.
from .utils import MONTH_FOLDERS as _MONTH_NAMES


def _hide_file_win(path: str) -> None:
    """Set the Windows HIDDEN attribute on `path` so the JSONL sidecar
    doesn't clutter Explorer views. Matches YTArchiver.py:8499."""
    if os.name == "nt":
        try:
            ctypes.windll.kernel32.SetFileAttributesW(str(path), 0x02) # FILE_ATTRIBUTE_HIDDEN
        except Exception:
            pass


def _get_transcript_filename(ch_name: str, folder_path: str,
                             split_years: bool, split_months: bool,
                             combined: bool,
                             year: Optional[int] = None,
                             month: Optional[int] = None) -> Tuple[str, str]:
    """Mirror of YTArchiver.py:11771 _get_transcript_filename.
    Returns (txt_path, subfolder)."""
    if combined or (not split_years):
        return (os.path.join(folder_path, f"{ch_name} Transcript.txt"), folder_path)

    if split_years and split_months and year and month:
        month_num = int(month) if isinstance(month, str) and str(month).isdigit() else month
        month_name_full = _MONTH_NAMES.get(month_num, f"{month_num:02d} Unknown")
        month_name = month_name_full.split(" ", 1)[1] # "January"
        yr_short = str(year)[-2:] # "24"
        subfolder = os.path.join(folder_path, str(year), month_name_full)
        fname = f"{ch_name} {month_name} {yr_short} Transcript.txt"
        return (os.path.join(subfolder, fname), subfolder)

    if split_years and year:
        subfolder = os.path.join(folder_path, str(year))
        fname = f"{ch_name} {year} Transcript.txt"
        return (os.path.join(subfolder, fname), subfolder)

    return (os.path.join(folder_path, f"{ch_name} Transcript.txt"), folder_path)


def _get_jsonl_sidecar(txt_path: str) -> str:
    """Hidden JSONL sidecar next to a transcript .txt file.
    Returns .../.{ch_name} ... Transcript.jsonl — matches YTArchiver.py:8490."""
    dirname = os.path.dirname(txt_path)
    basename = os.path.basename(txt_path)
    root_name, _ = os.path.splitext(basename)
    return os.path.join(dirname, "." + root_name + ".jsonl")


def _format_upload_date(date_str: str) -> str:
    """YYYYMMDD -> (MM.DD.YYYY). Matches YTArchiver.py:11757."""
    if len(date_str) == 8 and date_str.isdigit():
        return f"({date_str[4:6]}.{date_str[6:8]}.{date_str[:4]})"
    return f"({date_str})" if date_str else "(Unknown date)"


def _format_duration_hms(secs: float) -> str:
    """Duration in H:MM:SS (or M:SS). Matches YTArchiver.py's _format_duration_hms."""
    try:
        total = int(round(float(secs)))
    except Exception:
        return ""
    if total <= 0:
        return ""
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _generate_distributed_words(text: str, start: float, end: float) -> List[dict]:
    """Evenly distribute word-level timestamps across a segment.
    Used when the upstream source didn't provide real word-level timings.
    Matches YTArchiver.py:8478 _generate_distributed_words."""
    words = (text or "").split()
    if not words:
        return []
    dur = max(end - start, 0.01)
    step = dur / len(words)
    return [{"w": w,
             "s": round(start + i * step, 3),
             "e": round(start + (i + 1) * step, 3)}
            for i, w in enumerate(words)]


def _write_jsonl_entry(jsonl_path: str, video_id: str, title: str,
                       segments: List[dict]) -> None:
    """Append long-form JSONL entries for one video. Matches YTArchiver.py:8508.

    Each line:
      {"video_id":..., "title":..., "start":..., "end":...,
       "text":..., "words":[{"w","s","e"}, ...]}

    Note: segments from NEW's internal format use short keys {s,e,t,w}. This
    helper accepts EITHER short-form or long-form keys and always writes
    long-form to disk.
    """
    try:
        _jsonl_dir = os.path.dirname(jsonl_path)
        if _jsonl_dir:
            os.makedirs(_jsonl_dir, exist_ok=True)

        # Build lines in memory so a disk failure mid-write doesn't leave
        # half-a-line on disk.
        new_lines = []
        for seg in segments:
            # Accept either short-form (s/e/t/w) or long-form (start/end/text/words)
            s = seg.get("start") if "start" in seg else seg.get("s", 0.0)
            e = seg.get("end") if "end" in seg else seg.get("e", 0.0)
            t = seg.get("text") if "text" in seg else seg.get("t", "")
            raw_words = seg.get("words") if "words" in seg else seg.get("w")
            entry = {
                "video_id": video_id or "",
                "title": title,
                "start": round(float(s or 0), 2),
                "end": round(float(e or 0), 2),
                "text": t or "",
            }
            if raw_words:
                # Normalize word records to long-form too (OLD uses "w"/"s"/"e" inside
                # the words array, same as our short-form — already correct)
                entry["words"] = [
                    {"w": w.get("w") if isinstance(w, dict) else str(w),
                     "s": round(float((w.get("s") if isinstance(w, dict) else 0) or 0), 3),
                     "e": round(float((w.get("e") if isinstance(w, dict) else 0) or 0), 3)}
                    for w in raw_words
                ]
            else:
                entry["words"] = _generate_distributed_words(
                    entry["text"], entry["start"], entry["end"])
            new_lines.append(json.dumps(entry, ensure_ascii=False) + "\n")

        # Repair a truncated last line from a previous crash (YTArchiver.py:8538).
        if os.path.isfile(jsonl_path):
            try:
                with open(jsonl_path, "rb") as _chk:
                    _chk.seek(0, 2)
                    _fsize = _chk.tell()
                    if _fsize > 0:
                        _read_back = min(_fsize, 8192)
                        _chk.seek(_fsize - _read_back)
                        _tail = _chk.read().decode("utf-8", errors="replace")
                        _last_nl = _tail.rfind("\n", 0, len(_tail) - 1)
                        _last_line = _tail[_last_nl + 1:].strip() if _last_nl >= 0 else _tail.strip()
                        if _last_line:
                            try:
                                json.loads(_last_line)
                            except (json.JSONDecodeError, ValueError):
                                _trunc_pos = (_fsize - _read_back + _last_nl + 1) if _last_nl >= 0 else 0
                                if _trunc_pos > 0:
                                    with open(jsonl_path, "r+b") as _fix:
                                        _fix.seek(_trunc_pos)
                                        _fix.truncate()
            except Exception:
                pass

        with open(jsonl_path, "a", encoding="utf-8") as f:
            f.writelines(new_lines)
        _hide_file_win(jsonl_path)
    except Exception:
        # JSONL sidecar is best-effort; don't block the .txt write on failure.
        pass


def _write_transcript_entry(txt_path: str, title: str,
                            upload_date: str, duration_secs: float,
                            source_tag: str, text: str) -> bool:
    """Append one formatted block to the aggregated Transcript.txt.
    Format (YTArchiver.py:15458):
      ===(title), (MM.DD.YYYY), (H:MM:SS), (SOURCE)===
      {text}
      [triple newline]
    """
    try:
        os.makedirs(os.path.dirname(txt_path), exist_ok=True)
        date_fmt = _format_upload_date(upload_date or "")
        dur_raw = _format_duration_hms(duration_secs or 0) or ""
        dur_fmt = f"({dur_raw})" if dur_raw else "(Unknown length)"
        src_fmt = source_tag if source_tag.startswith("(") else f"({source_tag})"
        entry = f"===({title}), {date_fmt}, {dur_fmt}, {src_fmt}===\n{text}\n\n\n"
        with open(txt_path, "a", encoding="utf-8") as f:
            f.write(entry)
        return True
    except Exception:
        return False


# Header pattern for the per-entry "===(title), (date), (duration), (source)==="
# line in the aggregated Transcript.txt. Captures title (group 1), date
# (group 2), duration (group 3), source tag (group 4). Matches OLD
# YTArchiver.py:28997 `_HEADER_RE`.
_HEADER_RE = re.compile(
    r'^===\(([^)]*)\),\s*(\([^)]*\)),\s*(\([^)]*\)),\s*(\([^)]*\))===',
    re.MULTILINE)


def _replace_jsonl_entry(jsonl_path: str, title: str, video_id: str,
                         new_segments: List[dict]) -> set:
    """Surgically swap this video's entries in the aggregated .jsonl.

    Matches OLD YTArchiver.py:29093 `_replace_jsonl_entry` — used by the
    retranscribe flow to replace the old auto-captions / older-Whisper
    entries with the newly-transcribed ones WITHOUT blowing away the
    other videos that share the aggregated file.

    Matches on BOTH title AND video_id — catches the case where a title
    drifted between transcriptions (e.g. YouTube normalized "huge
    change.." → "huge change..." after the first auto-caption pass).
    Returns the set of distinct titles that were removed so the caller
    can feed them into `_replace_txt_entry` for the same cleanup on the
    .txt side.
    """
    # Clear Windows hidden/readonly so we can write (re-hidden by
    # _write_jsonl_entry on append).
    if os.name == "nt":
        try:
            _norm = os.path.normpath(jsonl_path)
            ctypes.windll.kernel32.SetFileAttributesW(_norm, 0x80) # NORMAL
        except Exception:
            pass
        try:
            import stat
            os.chmod(jsonl_path, stat.S_IWRITE | stat.S_IREAD)
        except Exception:
            pass

    old_lines: List[str] = []
    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            old_lines = f.readlines()
    except FileNotFoundError:
        pass

    kept: List[str] = []
    removed_titles: set = set()
    vid_norm = (video_id or "").strip()
    tit_key = _norm_title(title)
    for line in old_lines:
        ls = line.strip()
        if not ls:
            continue
        try:
            obj = json.loads(ls)
            seg_title = (obj.get("title") or "").strip()
            seg_vid = (obj.get("video_id") or "").strip()
            # Match by normalized title (punctuation-insensitive) OR by
            # video_id. Either signal means this segment belongs to the
            # video being re-transcribed and should be swapped out.
            if (seg_title and _norm_title(seg_title) == tit_key) or \
               (vid_norm and seg_vid == vid_norm):
                if seg_title:
                    removed_titles.add(seg_title)
                continue # drop this line
        except Exception:
            pass
        kept.append(line if line.endswith("\n") else line + "\n")

    # Atomic write of the kept lines.
    tmp = jsonl_path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.writelines(kept)
        os.replace(tmp, jsonl_path)
    except OSError:
        try: os.remove(tmp)
        except OSError: pass
        return removed_titles

    # Append the new segments (and re-hide the file on Windows).
    _write_jsonl_entry(jsonl_path, video_id, title, new_segments)
    return removed_titles


def _replace_txt_entry(txt_path: str, title: str, new_text: str,
                       source_tag: str,
                       extra_titles_to_remove=None) -> bool:
    """Surgically swap this video's `===(…)===\\n<body>\\n\\n\\n` block in
    the aggregated Transcript.txt. Matches OLD YTArchiver.py:29020
    `_replace_txt_entry`.

    `extra_titles_to_remove` is the set returned by `_replace_jsonl_entry`
    — additional titles discovered via video_id match. Passing them here
    lets the .txt pass remove stale title-drifted entries consistently.

    `source_tag` can be "(WHISPER:small)" or the bare model name; stored
    verbatim as the 4th bracketed field on the header line so the
    ArchivePlayer / Browse source banner can detect it.

    Returns True on success. Appends the new entry inheriting the OLD
    entry's date + duration so provenance is preserved across
    re-transcriptions.
    """
    try:
        try:
            with open(txt_path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
        except FileNotFoundError:
            content = ""

        # Build purge set as NORMALIZED keys (NFC + lowercase +
        # whitespace-collapsed + trailing-punct stripped). Without this
        # the check below misses "Title." vs "Title" variants that the
        # retranscribe flow legitimately needs to swap out — which is
        # what caused the triple-block duplication in v47.6 and older.
        purge = {_norm_title(t) for t in (extra_titles_to_remove or ())}
        purge.add(_norm_title(title))
        purge.discard("")

        # Remove each matching entry (header line through the next header
        # or EOF). Iterate from the end so earlier match positions stay
        # valid as we slice. Capture date+duration from the FIRST removed
        # entry so the new block inherits the provenance.
        matches = list(_HEADER_RE.finditer(content))
        new_content = content
        found_old = False
        date_fmt = "(Unknown date)"
        dur_fmt = "(Unknown length)"
        captured = False
        for i in range(len(matches) - 1, -1, -1):
            m = matches[i]
            entry_key = _norm_title(m.group(1))
            if entry_key not in purge:
                continue
            end = matches[i + 1].start() if i + 1 < len(matches) else len(new_content)
            if not captured:
                # Matches group indices of _HEADER_RE: (title, date, dur, src)
                date_fmt = m.group(2)
                dur_fmt = m.group(3)
                captured = True
            new_content = new_content[:m.start()] + new_content[end:]
            found_old = True

        src_fmt = source_tag if source_tag.startswith("(") else f"({source_tag})"
        new_entry = f"===({title}), {date_fmt}, {dur_fmt}, {src_fmt}===\n{new_text}\n\n\n"

        new_content = new_content.rstrip("\n") + "\n\n\n" if new_content.strip() else ""
        new_content += new_entry

        os.makedirs(os.path.dirname(txt_path) or ".", exist_ok=True)
        tmp = txt_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(new_content)
        os.replace(tmp, txt_path)
        return True
    except Exception:
        return False


def _norm_title(s: str) -> str:
    """Normalize a title for comparison: NFC unicode, strip, collapse
    internal whitespace, lowercase, strip trailing `.?!` punctuation.

    The trailing-punctuation strip is critical for the retranscribe
    path: Whisper's stored title is "title." with a period while
    YouTube captions wrote the same video as "title" without one, so
    `_replace_txt_entry` / `_replace_jsonl_entry` failed their exact-
    match check and appended duplicate blocks instead of surgically
    swapping. This normalization brings both sides to the same key.
    """
    import unicodedata as _ud
    if not s:
        return ""
    t = _ud.normalize("NFC", s).strip().lower()
    # Collapse internal whitespace to single spaces.
    t = re.sub(r"\s+", " ", t)
    # Strip trailing . ? ! punctuation (possibly stacked).
    t = re.sub(r"[.?!]+$", "", t).rstrip()
    return t


def _scan_existing_transcript_titles(folder_path: str, ch_name: str) -> dict:
    """Return `{norm_title: (raw_title, video_id_or_empty)}` for every
    entry in ANY Transcript.txt under `folder_path`.

    Earlier this only considered files whose name started with `ch_name`,
    which missed aggregate files whose filename drifted from the channel
    name (rename, special-char stripping, per-year split with different
    prefix, etc.). The scan is now permissive: ANY `*Transcript.txt`
    under the channel folder contributes its titles. Unicode-normalized
    keys mean trailing-whitespace / combining-mark differences stop
    producing false "needs transcribing" matches.

    Mirrors YTArchiver.py:11800 but uses dict output so callers can do
    both title-match and videoID-match lookups.
    """
    existing: dict = {}
    # Captures title (group 1) up to the FIRST `), (` — greedy-enough
    # for most cases. Titles that legitimately contain `), (` would need
    # a stricter grammar but that's rare enough to ignore.
    pattern = re.compile(r"^===\((.+?)\),\s*\(")
    # `[ABCDEF12345]` YouTube id suffix (for extracting ids stored in
    # aggregate titles that were written with the `[id]` tail still on).
    id_pattern = re.compile(r"\[([A-Za-z0-9_-]{11})\]\s*$")
    if not folder_path or not os.path.isdir(folder_path):
        return existing
    for dirpath, _dirs, files in os.walk(folder_path):
        for f in files:
            if not f.endswith("Transcript.txt"):
                continue
            try:
                with open(os.path.join(dirpath, f), "r", encoding="utf-8") as fh:
                    for line in fh:
                        if not line.startswith("===("):
                            continue
                        m = pattern.match(line.strip())
                        if not m:
                            continue
                        raw = m.group(1)
                        vid_id = ""
                        im = id_pattern.search(raw)
                        if im:
                            vid_id = im.group(1)
                        # Store TWO variants so callers can match either:
                        # title-with-[id] OR title-without-[id].
                        raw_plain = id_pattern.sub("", raw).strip() or raw
                        existing[_norm_title(raw)] = (raw, vid_id)
                        existing[_norm_title(raw_plain)] = (raw, vid_id)
            except Exception:
                pass
    return existing


def _lookup_channel(channel_name: str) -> Optional[Dict[str, Any]]:
    """Look up a channel dict in config by name. Lightweight: just for
    resolving split_years/split_months when writing transcripts."""
    if not channel_name:
        return None
    try:
        from . import ytarchiver_config as _cfg
        cfg = _cfg.load_config()
        for ch in cfg.get("channels", []):
            if (ch.get("name") or "") == channel_name or \
               (ch.get("folder") or ch.get("folder_override") or "") == channel_name:
                return ch
    except Exception:
        pass
    return None


def _resolve_transcript_paths(video_path: str, title: str,
                              channel_name: str,
                              combined_override: Optional[bool] = None
                              ) -> Optional[Tuple[str, str, int, int, str]]:
    """Figure out (txt_path, jsonl_path, upload_ts_year, upload_ts_month,
    upload_date_yyyymmdd) for a given video.

    Uses the video file's mtime (which yt-dlp --mtime sets to the YT upload
    date) for the year/month selection + upload-date display string.

    `combined_override`: if True, write to a single channel-root transcript
    even if split_years is on (user picked "Combined" in the first-time
    dialog). If False, force per-year even if split_years is off. If None,
    follow the channel's split_years setting (OLD-compatible default).

    Returns None if we can't resolve (no channel config, no folder, etc.).
    """
    ch = _lookup_channel(channel_name) or {}
    # Channel folder derivation — same rule as backend.sync.channel_folder_name
    try:
        from .sync import channel_folder_name as _cfn
        folder_name = _cfn(ch) if ch else (channel_name or "")
    except Exception:
        folder_name = channel_name or ""
    # Base folder is the parent chain above the video
    # (video is at {base}/{folder}/.../ )
    try:
        base_root = (ytarchiver_config_output_dir() or "").strip()
    except Exception:
        base_root = ""
    if not base_root:
        # Fall back: walk up from the video file to find the channel folder
        folder_path = None
        vp_parent = os.path.dirname(os.path.abspath(video_path))
        while vp_parent:
            if os.path.basename(vp_parent) == folder_name:
                folder_path = vp_parent
                break
            parent = os.path.dirname(vp_parent)
            if parent == vp_parent:
                break
            vp_parent = parent
        if folder_path is None:
            folder_path = os.path.dirname(os.path.dirname(os.path.abspath(video_path)))
    else:
        folder_path = os.path.join(base_root, folder_name)
    split_years = bool(ch.get("split_years"))
    split_months = bool(ch.get("split_months"))
    # mtime → upload date
    try:
        mtime = datetime.fromtimestamp(os.path.getmtime(video_path))
        upload_date = mtime.strftime("%Y%m%d")
        year, month = mtime.year, mtime.month
    except OSError:
        upload_date = ""
        now = datetime.now()
        year, month = now.year, now.month
    # combined rule:
    # override True → always combined
    # override False → always per-year
    # override None → legacy default: combined iff not split_years
    if combined_override is True:
        _combined = True
    elif combined_override is False:
        _combined = False
    else:
        _combined = not split_years
    txt_path, _subfolder = _get_transcript_filename(
        channel_name or folder_name, folder_path,
        split_years, split_months, combined=_combined,
        year=year, month=month)
    jsonl_path = _get_jsonl_sidecar(txt_path)
    return (txt_path, jsonl_path, year, month, upload_date)


def ytarchiver_config_output_dir() -> str:
    """Safe helper to read output_dir without a hard import-time dependency
    — some modules import transcribe at collection time."""
    try:
        from . import ytarchiver_config as _cfg
        return (_cfg.load_config().get("output_dir") or "").strip()
    except Exception:
        return ""


def _bump_transcription_pending(channel_name: str, delta: int) -> None:
    """Increment (or decrement, with negative delta) the channel's
    `transcription_pending` counter. When the counter reaches 0 and there
    are 0 further queued jobs, sets `transcription_complete=True`.

    Mirrors YTArchiver.py:14629-14630, :15530-15531 update sites. Silent on
    any error — counter drift is cosmetic (Subs-tab indicator), not
    destructive.
    """
    if not channel_name:
        return
    try:
        from . import ytarchiver_config as _cfg
        if not _cfg.config_is_writable():
            return
        cfg = _cfg.load_config()
        changed = False
        for ch in cfg.get("channels", []):
            name = ch.get("name") or ""
            folder = ch.get("folder") or ch.get("folder_override") or ""
            if name == channel_name or folder == channel_name:
                cur = int(ch.get("transcription_pending", 0) or 0)
                new = max(0, cur + int(delta))
                if new != cur:
                    ch["transcription_pending"] = new
                    changed = True
                if new == 0 and delta < 0:
                    # Only flip `complete` when we just finished a job, not
                    # when we re-initialize to 0.
                    ch["transcription_complete"] = True
                    changed = True
                elif delta > 0 and ch.get("transcription_complete"):
                    # Queued a new job on a previously-complete channel →
                    # re-mark as incomplete until the new job finishes.
                    ch["transcription_complete"] = False
                    changed = True
                break
        if changed:
            _cfg.save_config(cfg)
    except Exception:
        pass


# Chunked-transcription thresholds (mirror YTArchiver.py:11151)
# Videos longer than this are split with ffmpeg before sending to Whisper
# so RAM doesn't blow up. 30s overlap between chunks avoids mid-sentence
# truncation; duplicate segments in the overlap zone are dropped on merge.
_CHUNK_DURATION_SECS = 7200 # 2 hours per chunk
_CHUNK_OVERLAP_SECS = 30
_CHUNK_MIN_DURATION = 7800 # below this, do a single-pass transcribe

# Module-level counter for per-job unique inplace tags. Every
# transcription run (auto-captions or Whisper) gets a fresh
# `whisper_job_<N>` tag that stamps every log emit in that job's
# lifecycle, so progress/done lines replace each other within a job
# but stay independent of other jobs. See `_transcribe_one`.
_JOB_COUNTER = 0


def _ffprobe_duration(filepath: str) -> Optional[float]:
    """Return the file's duration in seconds via ffprobe, or None."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1",
             filepath],
            capture_output=True, text=True, timeout=20,
            creationflags=(0x08000000 if os.name == "nt" else 0),
        )
        val = (r.stdout or "").strip()
        return float(val) if val else None
    except Exception:
        return None


# ── Auto-captions fast-path helper ─────────────────────────────────────

def _fetch_captions_via_ytdlp(video_path: str, stream: LogStreamer,
                              fetched_paths_out: List[str]) -> Optional[str]:
    """Probe yt-dlp for captions and write a .vtt next to the video.

    Mirrors YTArchiver.py:11641 `_fetch_auto_captions`: tries without cookies
    first (fast — skips Firefox DB read), falls back to with-cookies on 403 /
    empty result. Adds any written file to `fetched_paths_out` so the caller
    can clean up after parsing.

    Returns the path to the written .vtt (auto-caption preferred over manual
    subs so we get <c>-tag word timing), or None if no captions exist or
    yt-dlp is unavailable.
    """
    yt = shutil.which("yt-dlp") or shutil.which("yt-dlp.exe")
    if not yt:
        return None
    vid_id = ""
    m = re.search(r"\[([A-Za-z0-9_-]{11})\]\s*$",
                  os.path.splitext(os.path.basename(video_path))[0])
    if m:
        vid_id = m.group(1)
    else:
        try:
            from . import index as _idx
            conn = _idx._open()
            if conn is not None:
                with _idx._db_lock:
                    row = conn.execute(
                        "SELECT video_id FROM videos WHERE filepath=? "
                        "COLLATE NOCASE LIMIT 1",
                        (os.path.normpath(video_path),)).fetchone()
                if row and row[0]:
                    vid_id = row[0]
        except Exception:
            pass
    if not vid_id:
        return None # can't probe without a video ID

    base = os.path.splitext(video_path)[0]
    temp_base = base + ".__cap_probe"
    video_url = f"https://www.youtube.com/watch?v={vid_id}"

    def _glob_vtts() -> List[str]:
        return glob.glob(temp_base + "*.vtt")

    def _cleanup():
        for _p in glob.glob(temp_base + "*"):
            try: os.remove(_p)
            except OSError: pass

    def _run(use_cookies: bool) -> bool:
        cmd = [
            yt, "--skip-download",
            "--write-sub", "--write-auto-sub",
            "--sub-lang", "en", "--sub-format", "vtt",
            "-o", temp_base + ".%(ext)s",
            "--no-playlist",
            "--force-overwrites",
        ]
        if use_cookies:
            try:
                from .sync import _find_cookie_source
                cmd += list(_find_cookie_source())
            except Exception:
                cmd += ["--cookies-from-browser", "firefox"]
        cmd.append(video_url)
        try:
            # Capture stderr instead of /dev/nulling it so yt-dlp's
            # explanation for a failed caption fetch (cookies expired,
            # member-only, region-lock, etc.) surfaces in the log
            # instead of silently vanishing. User's view goes from
            # "why did Whisper run on this video?" to a clear dim line
            # pointing at the root cause. Only emit when the stderr
            # looks like an auth/cookie issue — generic "no captions
            # available" is noise.
            r = subprocess.run(cmd, stdin=subprocess.DEVNULL,
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE,
                               timeout=120, startupinfo=_startupinfo)
            try:
                err_text = (r.stderr or b"").decode(
                    "utf-8", errors="replace")
            except Exception:
                err_text = ""
            if err_text and use_cookies:
                _lower = err_text.lower()
                if any(p in _lower for p in (
                        "sign in to confirm",
                        "cookies are missing",
                        "cookies are invalid",
                        "failed to extract any player response",
                        "this video is private",
                        "this video is members",
                )):
                    try:
                        # First matching error line, trimmed.
                        first_err = next(
                            (ln.strip() for ln in err_text.splitlines()
                             if ln.strip().lower().startswith(("error", "warning"))),
                            err_text.strip().splitlines()[0])[:160]
                        stream.emit([
                            [" \u26A0 Caption fetch blocked: ", "dim"],
                            [f"{first_err}\n", "dim"],
                        ])
                    except Exception:
                        pass
            return True
        except Exception:
            return False

    # Pass 1: cookieless
    _run(False)
    vtts = _glob_vtts()
    if not vtts:
        _cleanup()
        # Pass 2: with cookies (some channels require auth for captions)
        _run(True)
        vtts = _glob_vtts()
    if not vtts:
        _cleanup()
        return None

    # Prefer auto-generated VTT — it has <c> tags with per-word timestamps.
    pick = vtts[0]
    if len(vtts) > 1:
        for vf in vtts:
            try:
                with open(vf, "r", encoding="utf-8") as fh:
                    sample = fh.read(2000)
                if "<c>" in sample or "<c " in sample:
                    pick = vf
                    break
            except Exception:
                pass
    # Track every fetched file (including unpicked alternates) for cleanup.
    fetched_paths_out.extend(vtts)
    return pick


def _try_auto_captions(video_path: str, title: str, channel: str,
                        stream: LogStreamer,
                        punct_mgr=None,
                        job_tag: str = "") -> bool:
    """If yt-dlp wrote a .en.vtt (or similar) next to the video, parse it
    into the aggregated channel Transcript.txt + hidden JSONL sidecar,
    then ingest into FTS — skip Whisper entirely.

    `job_tag` is the per-job unique inplace kind (e.g. `whisper_job_7`)
    that stamps every emit from this transcription so progress/done
    lines replace EACH OTHER within the job but never stomp another
    job's lines. reported a high-video-count channel's 2-video transcription
    lines disappearing entirely — root cause was all emits sharing
    the generic `whisper_*` kind, so video 2's "Loading punctuation…"
    replaced video 1's "— ✓ Transcription" done line.

    `punct_mgr` is an optional PunctuationManager. When provided AND
    the model loads successfully, the parsed caption text gets run
    through the punctuation-restoration pass (matches OLD YTArchiver.py:
    15437-15439 `_punctuate_text(text)` call) and the stored source
    tag becomes `(YT+PUNCTUATION)` instead of `(YT CAPTIONS)` so the
    Watch-view source banner shows "punctuation restored". Segments
    in the .jsonl also get punctuated per-segment (matches NEW's
    Whisper punct pass for consistent .jsonl quality).

    Output matches YTArchiver.py:15449-15478 exactly. Returns True on
    success; False if no usable auto-sub file exists."""
    base = os.path.splitext(video_path)[0]
    candidates = [
        f"{base}.en.vtt", f"{base}.en-US.vtt", f"{base}.en-GB.vtt",
        f"{base}.en-us.vtt", f"{base}.en-gb.vtt", f"{base}.vtt",
        f"{base}.en.ttml", f"{base}.en.srt",
    ]
    vtt = next((p for p in candidates if os.path.isfile(p)), None)

    # Fallback: if sync didn't get a .vtt (e.g. auto-transcribe was off at
    # sync time, or yt-dlp's caption fetch failed transiently), try yt-dlp
    # directly here — matches OLD's `_fetch_auto_captions` (YTArchiver.py:11641)
    # which runs a cookieless probe first, then retries with cookies on 403.
    _fetched_temp: List[str] = []
    if not vtt:
        vtt = _fetch_captions_via_ytdlp(video_path, stream, _fetched_temp)
        if vtt:
            candidates.append(vtt) # let the downstream cleanup delete it

    if not vtt:
        return False

    t0 = time.time()
    try:
        segs = _parse_vtt(vtt)
    except Exception as _ve:
        # bug L-6: surface the parse failure before bailing. Old code
        # silently returned False, causing the caller to emit "No
        # auto-captions available — using Whisper..." even though the
        # captions WERE present, just unparseable. A dim warning here
        # makes the distinction visible without derailing the fallback.
        try:
            self._stream.emit_dim(
                f" (auto-captions parse failed: {_ve} \u2014 "
                f"falling back to Whisper)")
        except Exception:
            pass
        # Clean up any temp files we fetched before bailing.
        for _p in _fetched_temp:
            try: os.remove(_p)
            except OSError: pass
        return False
    if not segs:
        for _p in _fetched_temp:
            try: os.remove(_p)
            except OSError: pass
        return False

    # Resolve aggregated transcript paths for this channel (matches OLD layout).
    paths = _resolve_transcript_paths(video_path, title, channel)
    if paths is None:
        return False
    txt_path, jsonl_path, _year, _month, upload_date = paths
    try:
        os.makedirs(os.path.dirname(txt_path), exist_ok=True)
    except OSError:
        pass

    # Extract video id — same two-step fallback as _write_outputs: try the
    # filename suffix first, then fall back to the FTS DB lookup for
    # OLD-compat drop-in filenames (no `[id]` suffix).
    vid_id = ""
    m = re.search(r"\[([A-Za-z0-9_-]{11})\]\s*$", os.path.splitext(os.path.basename(video_path))[0])
    if m:
        vid_id = m.group(1)
    else:
        try:
            from . import index as _idx
            conn = _idx._open()
            if conn is not None:
                with _idx._db_lock:
                    row = conn.execute(
                        "SELECT video_id FROM videos WHERE filepath=? "
                        "COLLATE NOCASE LIMIT 1",
                        (os.path.normpath(video_path),)).fetchone()
                if row and row[0]:
                    vid_id = row[0]
        except Exception:
            pass

    # Append formatted entry to the aggregated .txt + hidden .jsonl.
    full_text = " ".join(s["t"] for s in segs if s.get("t")).strip()
    duration = segs[-1]["e"] if segs else 0

    # Punctuation restoration — mirrors OLD YTArchiver.py:15437-15439.
    # YT auto-captions arrive as a run-on stream of lowercase words;
    # running them through the punct model restores casing + commas
    # + periods so the .txt reads like a real transcript. Source tag
    # flips to `YT+PUNCTUATION` so ArchivePlayer / Watch banner can
    # detect the upgraded quality. Silently falls back to raw captions
    # if the punct model isn't available or the call fails.
    src_tag = "YT CAPTIONS"
    if punct_mgr is not None and full_text:
        try:
            # `job_tag` (e.g. `whisper_job_7`) makes this line
            # replace ONLY this video's prior "Loading punctuation..."
            # or similar, and get replaced ONLY by this video's final
            # "— ✓ Transcription" done line. Without it, any other
            # video's whisper_* emit would stomp this line.
            _tag_list = ["transcribe_using"]
            if job_tag:
                _tag_list.append(job_tag)
            else:
                _tag_list.append("whisper_progress")
            stream.emit([[" Adding punctuation...\n", _tag_list]])
            punct_text = punct_mgr.punctuate(full_text)
            if punct_text and punct_text != full_text:
                full_text = punct_text
                # Per-segment punct so the JSONL (and therefore FTS /
                # Watch-view karaoke text) is punctuated consistently.
                # Matches NEW's Whisper flow's per-segment pass.
                for seg in segs:
                    t = (seg.get("t") or "").strip()
                    if t:
                        try:
                            pt = punct_mgr.punctuate(t)
                            if pt:
                                seg["t"] = pt
                        except Exception:
                            pass
                src_tag = "YT+PUNCTUATION"
        except Exception as _pe:
            stream.emit_dim(f" (punctuation skipped: {_pe})")

    _write_transcript_entry(txt_path, title, upload_date, duration,
                            src_tag, full_text)
    _write_jsonl_entry(jsonl_path, vid_id, title, segs)

    # Clean up the .vtt sidecar — OLD deletes these immediately after parsing.
    for _p in candidates:
        if os.path.isfile(_p):
            try: os.remove(_p)
            except OSError: pass

    # FTS ingest — use the new aggregated .jsonl path
    try:
        from . import index as _idx
        _idx.ingest_jsonl(video_path, jsonl_path, title, channel)
        _idx.mark_video_transcribed(video_path)
    except Exception:
        pass
    # Decrement transcription_pending / set transcription_complete on 0.
    _bump_transcription_pending(channel, -1)
    # Drop this video's ID from the authoritative pending list so the
    # Subs "-X" indicator shrinks. `vid_id` was extracted earlier in
    # this function for jsonl + FTS writes.
    if vid_id:
        try:
            from . import ytarchiver_config as _cfg
            _cfg.remove_pending_tx_id(vid_id)
        except Exception:
            pass

    took = time.time() - t0
    realtime = f"{duration/took:.1f}x" if took > 0 and duration > 0 else ""
    # Per-video done line. Stamp every segment with the per-job
    # `job_tag` (unique per video) so this line REPLACES this
    # video's in-progress lines AND gets left alone by other
    # videos' transcriptions. ALSO stamp with `tx_done_<vid>` so the
    # done line lands at the placeholder sync.py reserved under the
    # channel's block rather than at the bottom of the log.
    # `_inplaceKind` prioritizes `tx_done_` over `whisper_job_`.
    _tx_tag = f"tx_done_{vid_id}" if vid_id else ""
    _em_tag = [t for t in ("whisper_bracket", job_tag, _tx_tag) if t]
    _dim_tag = [t for t in ("dim", job_tag, _tx_tag) if t]
    _lbl_tag = [t for t in ("simpleline_blue", job_tag, _tx_tag) if t]
    stream.emit([
        [" ", _dim_tag],
        ["\u2014 \u2713 ", _em_tag],
        ["Transcription", _lbl_tag],
        [f" (auto-captions, took {took:.0f}s, {realtime} realtime)\n", _dim_tag],
    ])
    return True


def _ts_to_sec(ts: str) -> float:
    """Convert 'HH:MM:SS.mmm' or 'MM:SS.mmm' to float seconds.
    Mirrors YTArchiver.py:8245-8252."""
    ts = ts.replace(",", ".").strip()
    parts = ts.split(":")
    if len(parts) == 3:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
    if len(parts) == 2:
        return int(parts[0]) * 60 + float(parts[1])
    return 0.0


def _parse_vtt(path: str) -> list:
    """Full-fidelity port of YTArchiver.py:8223 `_parse_vtt_to_segments`.

    Three steps:
      1. Parse raw cues from the VTT and extract per-word <c>-tag timestamps
         (the YouTube auto-caption markup: `word<00:00:00.480><c> next</c>`).
      2. Merge rolling-caption overlap where each new cue repeats the tail of
         the previous cue plus a few new words — flush at 30s cap.
      3. Attach per-word timestamps back onto the merged segments; fall back
         to distributed timings when no <c> tags were present (manual subs).

    Returns a list of short-key segments: [{s, e, t, w: [{w, s, e}, ...]}, ...]
    — compatible with `_write_jsonl_entry` and `_try_auto_captions`.
    """
    import html as _html_mod
    import re as _re
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            raw = f.read()
    except OSError:
        return []

    # ── Step 1: Parse raw cues (text + original raw lines for <c> extraction) ──
    raw_cues = [] # (start, end, joined_text, [raw_content_lines])
    lines = raw.split("\n")
    current_start = None
    current_end = None
    current_text = []
    current_raw = []
    ts_line_re = _re.compile(r'(\d[\d:.]+)\s*-->\s*(\d[\d:.]+)')
    for line in lines:
        line = line.strip()
        m = ts_line_re.match(line)
        if m:
            if current_text and current_start is not None:
                raw_cues.append((current_start, current_end,
                                 " ".join(current_text), list(current_raw)))
            current_start = _ts_to_sec(m.group(1))
            current_end = _ts_to_sec(m.group(2))
            current_text = []
            current_raw = []
            continue
        if not line or line.startswith("WEBVTT") or line.startswith("NOTE") \
                or line.startswith("Kind:") or line.startswith("Language:"):
            continue
        if _re.match(r'^\d+$', line):
            continue
        if 'align:' in line or 'position:' in line:
            continue
        current_raw.append(line)
        cleaned = _re.sub(r'<[^>]+>', '', line)
        cleaned = _html_mod.unescape(cleaned)
        cleaned = _re.sub(r'\s+', ' ', cleaned).strip()
        if cleaned:
            current_text.append(cleaned)
    if current_text and current_start is not None:
        raw_cues.append((current_start, current_end,
                         " ".join(current_text), list(current_raw)))
    if not raw_cues:
        return []

    # ── Step 1b: Extract per-word timestamps from <c> tags ──
    ctag_re = _re.compile(r'<(\d[\d:.]+)><c[^>]*>(.*?)</c>')
    all_words = []
    has_ctags = False
    for cue_idx, (cue_s, cue_e, _, raw_lines) in enumerate(raw_cues):
        for raw_line in raw_lines:
            tags = list(ctag_re.finditer(raw_line))
            if tags:
                has_ctags = True
                # First cue's untagged prefix is the genuine first word(s).
                if cue_idx == 0:
                    first_tag_pos = raw_line.find('<')
                    if first_tag_pos > 0:
                        prefix = _html_mod.unescape(raw_line[:first_tag_pos]).strip()
                        for pw in prefix.split():
                            if pw.strip():
                                all_words.append({"w": pw.strip(), "s": cue_s})
                for mm in tags:
                    ts = _ts_to_sec(mm.group(1))
                    word_text = _html_mod.unescape(mm.group(2)).strip()
                    for w in word_text.split():
                        if w.strip():
                            all_words.append({"w": w.strip(), "s": ts})
    # No <c> tags (manual subs): distribute word starts across cue duration.
    if not has_ctags:
        for cue_s, cue_e, text, _ in raw_cues:
            dur = cue_e - cue_s
            words_in_cue = text.strip().split()
            n = len(words_in_cue)
            for wi, w in enumerate(words_in_cue):
                all_words.append({
                    "w": w,
                    "s": round(cue_s + dur * wi / max(n, 1), 3),
                })
    # Compute end times (each word ends when the next begins).
    all_words.sort(key=lambda w: w["s"])
    for i in range(len(all_words) - 1):
        all_words[i]["e"] = round(all_words[i + 1]["s"], 3)
    if all_words:
        all_words[-1]["e"] = round(raw_cues[-1][1], 3)
        for w in all_words:
            w["s"] = round(w["s"], 3)

    # ── Step 2: Merge overlapping rolling cues, flush at 30s cap ──
    MAX_SEG_SECS = 30.0
    segments = []
    seg_start = raw_cues[0][0]
    seg_end = raw_cues[0][1]
    seg_text = raw_cues[0][2]
    for i in range(1, len(raw_cues)):
        _s, _e, _t, _ = raw_cues[i]
        is_overlap = False
        if seg_text and _t:
            seg_words_list = seg_text.split()
            new_words_list = _t.split()
            max_overlap = min(len(seg_words_list),
                              len(new_words_list) - 1, 20)
            for ol in range(max_overlap, 0, -1):
                if seg_words_list[-ol:] == new_words_list[:ol]:
                    extra = " ".join(new_words_list[ol:])
                    is_overlap = True
                    if (_e - seg_start) > MAX_SEG_SECS:
                        if seg_text.strip():
                            segments.append({
                                "start": seg_start, "end": seg_end,
                                "text": seg_text.strip()})
                        seg_start = _s
                        seg_end = _e
                        seg_text = _t
                    else:
                        if extra:
                            seg_text += " " + extra
                        seg_end = _e
                    break
            # Catch near-zero-duration "echo" cues that just repeat the tail.
            if not is_overlap and (_e - _s) < 0.1:
                is_overlap = True
                seg_end = max(seg_end, _e)
        if not is_overlap:
            if seg_text.strip():
                segments.append({
                    "start": seg_start, "end": seg_end,
                    "text": seg_text.strip()})
            seg_start = _s
            seg_end = _e
            seg_text = _t
    if seg_text.strip():
        segments.append({"start": seg_start, "end": seg_end,
                         "text": seg_text.strip()})

    # ── Step 2b: Split any segment that still exceeds the cap ──
    capped = []
    for seg in segments:
        dur = seg["end"] - seg["start"]
        if dur <= MAX_SEG_SECS:
            capped.append(seg)
            continue
        words = seg["text"].split()
        n = max(2, int(dur / MAX_SEG_SECS) + (1 if dur % MAX_SEG_SECS > 0 else 0))
        cdur = dur / n
        wper = max(1, len(words) // n)
        for ci in range(n):
            w0 = ci * wper
            w1 = w0 + wper if ci < n - 1 else len(words)
            ct = " ".join(words[w0:w1])
            if not ct:
                continue
            cs = round(seg["start"] + ci * cdur, 2)
            ce = round(min(seg["end"], seg["start"] + (ci + 1) * cdur), 2)
            capped.append({"start": cs, "end": ce, "text": ct})
    segments = capped

    # ── Step 3: Attach per-word timestamps back onto the merged segments ──
    out = []
    if all_words:
        widx = 0
        for seg in segments:
            seg_words = []
            back_limit = 200
            while back_limit > 0 and widx > 0 and widx < len(all_words) \
                    and all_words[widx]["s"] >= seg["start"] - 0.5:
                widx -= 1
                back_limit -= 1
            while widx < len(all_words) and all_words[widx]["s"] < seg["start"] - 0.5:
                widx += 1
            scan = widx
            while scan < len(all_words) and all_words[scan]["s"] <= seg["end"] + 0.5:
                seg_words.append(all_words[scan])
                scan += 1
            if not seg_words:
                seg_words = _generate_distributed_words(
                    seg["text"], seg["start"], seg["end"])
            out.append({
                "s": round(seg["start"], 2),
                "e": round(seg["end"], 2),
                "t": seg["text"],
                "w": seg_words,
            })
            widx = scan
    else:
        for seg in segments:
            out.append({
                "s": round(seg["start"], 2),
                "e": round(seg["end"], 2),
                "t": seg["text"],
                "w": _generate_distributed_words(
                    seg["text"], seg["start"], seg["end"]),
            })
    return out


# ── Python 3.11 discovery (same pattern as YTArchiver.py:8653) ─────────

def find_python311() -> Optional[str]:
    import shutil as _shutil
    candidates: List[str] = []
    bases = [
        os.path.expandvars(r"%LOCALAPPDATA%\Programs\Python"),
        r"C:\Python311",
        r"C:\Python310",
        os.path.expandvars(r"%PROGRAMFILES%\Python311"),
        os.path.expandvars(r"%PROGRAMFILES(X86)%\Python311"),
    ]
    for base in bases:
        candidates.extend(glob.glob(os.path.join(base, "Python311*", "python.exe")))
        p = os.path.join(base, "python.exe")
        if os.path.isfile(p):
            candidates.append(p)
    if candidates:
        return candidates[0]
    for name in ("python3.11", "python"):
        found = _shutil.which(name)
        if found:
            return found
    # Final fallback — common location
    fallback = os.path.expandvars(r"%LOCALAPPDATA%\Programs\Python\Python311\python.exe")
    return fallback if os.path.isfile(fallback) else None


# ── Manager ────────────────────────────────────────────────────────────

class PunctuationManager:
    """Persistent punctuation-restoration subprocess. Cheap when idle.

    Call `punctuate(text)` with the raw whisper output; returns the
    capitalised / punctuated version. Subprocess boots on first call
    and stays alive between calls.
    """

    def __init__(self, stream: LogStreamer):
        self._stream = stream
        self._proc: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._starting = False
        self._worker_script = Path(__file__).resolve().parent / "punct_worker.py"
        self._python311: Optional[str] = None
        # Set by `_transcribe_one` before each punctuate() call so the
        # "Loading punctuation model..." emit carries the current
        # video's per-job inplace tag. Without it that line shares the
        # generic "whisper" kind and can get stomped by other jobs.
        self._job_tag: str = ""

    def is_available(self) -> bool:
        return self._worker_script.exists() and (self._python311 or find_python311()) is not None

    def _start(self) -> bool:
        with self._lock:
            if self._proc is not None and self._proc.poll() is None:
                return True
            if self._starting:
                return False
            self._starting = True
        try:
            py = self._python311 or find_python311()
            if not py:
                self._stream.emit_error("Punctuation: Python 3.11 not found.")
                return False
            self._python311 = py
            # Per-job tag so this line joins the current video's
            # inplace family and gets replaced by "Adding
            # punctuation..." → then by the final "— ✓ Transcription"
            # done line. Falls back to the generic kind if no tag
            # was set.
            _tags = ["transcribe_using"]
            _tags.append(self._job_tag if self._job_tag else "whisper_progress")
            self._stream.emit([[
                " Loading punctuation model...\n", _tags]])
            env = os.environ.copy()
            self._proc = subprocess.Popen(
                [py, str(self._worker_script)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, bufsize=1, startupinfo=_startupinfo, env=env,
            )
            # Wait for ready
            ready: List[Optional[str]] = [None]
            def _read():
                try:
                    ready[0] = self._proc.stdout.readline().strip()
                except Exception:
                    pass
            t = threading.Thread(target=_read, daemon=True)
            t.start()
            t.join(timeout=300)
            if t.is_alive():
                self._stream.emit_error("Punctuation model timed out loading.")
                self._stop()
                return False
            line = ready[0]
            if not line:
                return False
            info = json.loads(line)
            if info.get("status") != "ready":
                self._stream.emit_error(f"Punct start: {info}")
                self._stop()
                return False
            self._stream.emit_text(f" \u2014 \u2713 Punctuation model loaded ({info.get('device', '?').upper()}).",
                                    "simpleline_green")
            return True
        except Exception as e:
            self._stream.emit_error(f"Failed to start punctuation: {e}")
            self._stop()
            return False
        finally:
            with self._lock:
                self._starting = False

    def _stop(self):
        with self._lock:
            if self._proc is not None:
                try:
                    self._proc.kill()
                except Exception:
                    pass
                self._proc = None

    def punctuate(self, text: str, timeout_sec: float = 60.0) -> str:
        """Run text through the punctuation model. Returns original text on failure."""
        if not text or len(text.split()) < 3:
            return text
        if self._proc is None or self._proc.poll() is not None:
            if not self._start():
                return text
        try:
            req = json.dumps({"text": text}) + "\n"
            with self._lock:
                self._proc.stdin.write(req)
                self._proc.stdin.flush()
                # Synchronous read — one request/response at a time
                line = self._proc.stdout.readline().strip()
            if not line:
                return text
            resp = json.loads(line)
            if resp.get("status") == "ok":
                return resp.get("text", text) or text
            return text
        except Exception as e:
            self._stream.emit_dim(f" (punctuation skipped: {e})")
            # Subprocess may be wedged — kill so next call restarts cleanly
            self._stop()
            return text


def _pending_journal_path() -> Path:
    """Where the pending-transcribe journal lives.

    Matches YTArchiver.py:14650 pattern: <channel_folder>/_whisper_pending.json.
    We keep a global one at APPDATA/ytarchiver_pending_transcribe.json so
    the manager can recover ALL queued work across channels on restart.
    """
    from .ytarchiver_config import APP_DATA_DIR
    return APP_DATA_DIR / "ytarchiver_pending_transcribe.json"


class TranscribeManager:
    """Manages the whisper subprocess + a GPU queue."""

    def current_model(self) -> str:
        """Return the whisper model this manager is currently using for
        new jobs. Public accessor — main.py's `transcribe_current_model`
        used to reach into `self._model` directly, which would silently
        break on any future refactor of the internals.
        """
        return self._model

    def __init__(self, stream: LogStreamer, model: str = "large-v3"):
        self._stream = stream
        self._model = model
        self._proc: Optional[subprocess.Popen] = None
        self._proc_lock = threading.Lock()
        self._line_queue: Optional[queue.Queue] = None
        self._starting = False
        self._reader_thread: Optional[threading.Thread] = None
        self._python311 = find_python311()
        self._worker_script = Path(__file__).resolve().parent / "whisper_worker.py"
        # Optional punctuation model — lazy-loaded, reused across jobs.
        self._punct = PunctuationManager(stream)
        self._punctuate_enabled = True

        # Queue of jobs. Each job = {path, title, cb, cancel_event}
        self._jobs: List[Dict[str, Any]] = []
        self._jobs_lock = threading.Lock()
        self._worker_thread: Optional[threading.Thread] = None
        self._cancel_all = threading.Event()
        self._paused = threading.Event()
        self._current_job: Optional[Dict[str, Any]] = None
        # Per-batch stats for autorun_history [Trnscr] rows. Mirrors
        # YTArchiver.py:22575 _record_transcription — one row per channel
        # with done/err counts + elapsed time. Flushed when the worker
        # drains. Keyed by channel name.
        self._batch_stats: Dict[str, Dict[str, Any]] = {}
        # Reference to the shared QueueState. Attached by the app wrapper
        # after construction (main.py can't pass it in __init__ because
        # QueueState is constructed later). When None, the manager
        # maintains its own internal job list only — no UI popover sync.
        # When set, enqueue/worker mirror into queues.gpu + current_gpu
        # so the GPU Tasks popover shows what's pending / running.
        self._queues = None
        # Config-driven Auto gate. When autorun_gpu=False, new jobs sit
        # in the queue without firing. Checked each worker iteration so
        # toggling the Auto checkbox mid-pass takes effect between jobs.
        self._cfg_loader = None # set via attach_queues

    def attach_queues(self, queues, cfg_loader=None) -> None:
        """Connect this manager to the shared QueueState.
        `cfg_loader` is an optional callable returning the live config
        dict; used to read `autorun_gpu` each worker iteration so the
        Auto checkbox actually gates firing (not just display)."""
        self._queues = queues
        self._cfg_loader = cfg_loader

    def get_channel_batch_stats(self, channel_name: str) -> Dict[str, int]:
        """Synchronous snapshot of this channel's transcription batch
        stats. Used by sync_channel at end-of-pass to fold transcribed
        counts into the consolidated activity-log [Dwnld] row — auto-
        captions typically complete during the download so the count
        is accurate by sync_channel's exit. Whisper may still be running.
        """
        s = self._batch_stats.get(channel_name) or {}
        return {"done": int(s.get("done", 0) or 0),
                "err": int(s.get("err", 0) or 0)}

    def consume_channel_batch_stats(self, channel_name: str) -> None:
        """Mark this channel's batch stats as already consumed by a
        sync-originated [Dwnld] row emission. Subsequent calls to
        _flush_batch_stats will skip it so the user doesn't see a
        duplicate [Trnscr] row for the same transcriptions.
        """
        try: self._batch_stats.pop(channel_name, None)
        except Exception: pass

    def _auto_enabled(self) -> bool:
        """True if the GPU Auto checkbox says "go". When False, the
        worker parks without popping the next job — tasks sit visible
        in the popover until the user re-enables Auto or clicks Start.
        Defaults to True when no config loader is attached (preserves
        legacy behavior for tests / preview mode)."""
        if self._cfg_loader is None:
            return True
        try:
            cfg = self._cfg_loader() or {}
            return bool(cfg.get("autorun_gpu", False))
        except Exception:
            return True

    # ── Lifecycle ────────────────────────────────────────────────────

    def is_available(self) -> bool:
        return bool(self._python311) and self._worker_script.exists()

    def swap_model(self, new_model: str) -> bool:
        """Change the whisper model. If the worker is running, stop it so
        the next job spins it back up with the new model. In-flight job is
        not aborted — it finishes on the current model, then the next job
        picks up the new one.
        """
        if not new_model:
            return False
        self._model = new_model
        # Kill the current subprocess so the next job triggers a restart
        # with the new WHISPER_MODEL env var baked in.
        try:
            self._stop_subprocess()
        except Exception:
            pass
        self._stream.emit_text(
            f" \u2014 Whisper model queued to swap to '{new_model}' "
            f"on next job.", "simpleline_blue")
        return True

    def start_subprocess(self, model: Optional[str] = None) -> bool:
        """Start the persistent whisper worker. Returns True when ready."""
        with self._proc_lock:
            if self._proc is not None and self._proc.poll() is None:
                return True
            if self._starting:
                return False
            self._starting = True
            self._proc = None

        try:
            if not self._python311:
                self._stream.emit_error("Whisper: Python 3.11 not found. Install from python.org.")
                return False
            m = model or self._model
            self._stream.emit_text(
                f" Transcribing — Loading Whisper model ({m}) on GPU...",
                "transcribe_using")

            env = os.environ.copy()
            env["WHISPER_MODEL"] = m
            env["WHISPER_DEVICE"] = "cuda"
            env["WHISPER_COMPUTE"] = "float16"

            self._proc = subprocess.Popen(
                [self._python311, str(self._worker_script)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, bufsize=1, startupinfo=_startupinfo, env=env,
            )

            # Wait for "ready" (model load can take minutes on first download)
            ready_result: List[Optional[str]] = [None]
            def _read_ready():
                try:
                    ready_result[0] = self._proc.stdout.readline().strip()
                except Exception:
                    pass
            t = threading.Thread(target=_read_ready, daemon=True)
            t.start()
            t.join(timeout=600) # 10 min for model download + load
            if t.is_alive():
                self._stream.emit_error("Whisper timed out loading model.")
                self._stop_subprocess()
                return False

            line = ready_result[0]
            if not line:
                self._stream.emit_error("Whisper did not send ready message.")
                self._stop_subprocess()
                return False
            try:
                info = json.loads(line)
            except json.JSONDecodeError:
                self._stream.emit_error(f"Whisper sent non-JSON: {line[:200]}")
                self._stop_subprocess()
                return False
            if info.get("status") != "ready":
                self._stream.emit_error(f"Whisper status: {info}")
                self._stop_subprocess()
                return False

            dev = info.get("device", "?").upper()
            # Verbose-only subprocess-spawn diagnostic. PRIMARY tag
            # must be `transcribe_using` (in VERBOSE_ONLY_TAGS) so
            # `_line_is_verbose_only` drops the whole line in Simple
            # mode. In Verbose mode it renders in the transcribe
            # color (blue). This line has no inplace marker so it
            # would otherwise land at whatever log position the
            # sync pass is at — typically under a later channel's
            # "no new videos" row, persisting there forever.
            self._stream.emit([
                [" \u2014 \u2713 ", "transcribe_using"],
                [f"Whisper model loaded ({m}, {dev}).\n", "transcribe_using"],
            ])
            if info.get("cuda_fallback_reason"):
                self._stream.emit_dim(
                    f" [CUDA fallback] Fell back to CPU: {info['cuda_fallback_reason']}")

            # Start the stdout reader thread
            self._line_queue = queue.Queue()
            proc_ref = self._proc
            def _reader(q=self._line_queue):
                try:
                    for ln in iter(proc_ref.stdout.readline, ""):
                        try:
                            q.put(ln)
                        except Exception:
                            break
                except Exception:
                    pass
                try:
                    q.put(None)
                except Exception:
                    pass
            self._reader_thread = threading.Thread(target=_reader, daemon=True)
            self._reader_thread.start()
            return True
        except Exception as e:
            self._stream.emit_error(f"Failed to start whisper: {e}")
            self._stop_subprocess()
            return False
        finally:
            with self._proc_lock:
                self._starting = False

    def _stop_subprocess(self, force: bool = False):
        with self._proc_lock:
            if self._proc is None:
                return
            try:
                if force:
                    self._proc.kill()
                else:
                    try:
                        self._proc.terminate()
                    except Exception:
                        self._proc.kill()
            except Exception:
                pass
            self._proc = None
            self._line_queue = None

    # ── Queue + worker loop ──────────────────────────────────────────

    def enqueue(self, path: str, title: str = "",
                channel: str = "",
                combined: Optional[bool] = None,
                on_complete: Optional[Callable] = None,
                retranscribe: bool = False,
                video_id: str = "",
                bulk_id: str = "",
                bulk_total: int = 0,
                bulk_index: int = 0) -> bool:
        """Queue a video for transcription.

        `channel` is optional; if provided it's stored on the job so the
        FTS ingest at completion uses the right channel name (matters
        when the video path is structured differently from
        <base>/<channel>/<file>).

        `combined` overrides the channel's split_years-based output split:
          - None : follow ch.split_years (OLD-compatible default)
          - True : write to one channel-root transcript even if organized
          - False : write per-year even if the channel isn't organized

        Matches OLD's "Follow organization / Combined" first-time dialog
        (YTArchiver.py:5919). See `chan_transcribe_all` Api for the
        UI handshake that decides the `combined` value.

        `retranscribe=True` marks this as a RE-transcription — the worker
        will call `_replace_jsonl_entry` + `_replace_txt_entry` instead
        of the normal append-only writers, so the old entry in the
        aggregated files gets surgically swapped (matches OLD
        YTArchiver.py:16369 `_run_retranscribe_job`). `video_id` is used
        by the replace-jsonl pass to catch title-drifted duplicates.
        """
        path = str(path)
        if not os.path.isfile(path):
            self._stream.emit_error(f"Transcribe: file not found: {path}")
            return False
        _job_title = title or os.path.basename(path)
        with self._jobs_lock:
            self._jobs.append({
                "path": path,
                "title": _job_title,
                "channel": channel,
                "combined_override": combined,
                "cb": on_complete,
                "cancel": threading.Event(),
                "retranscribe": bool(retranscribe),
                "video_id": (video_id or "").strip(),
                "bulk_id": bulk_id or "",
                "bulk_total": int(bulk_total or 0),
                "bulk_index": int(bulk_index or 0),
            })
        # Mirror the job into the shared GPU queue so the Tasks popover
        # shows the pending work. this was flagged: auto-transcribe on
        # a channel would write to our internal `_jobs` list but the
        # popover stayed empty, so there was no visible record of the
        # transcription being queued. `kind=transcribe` + `title`
        # matches the shape `_task_label_gpu` reads.
        #
        # `bulk_id`/`bulk_total` carry a coalesce hint — when N videos from
        # the same channel are queued in one "Queue Pending" / "Transcribe
        # All" click, they all share a bulk_id and the popover collapses
        # them into a single "Transcribing {ch} (X videos)" row.
        if self._queues is not None:
            try:
                self._queues.gpu_enqueue({
                    "kind": "transcribe",
                    "title": _job_title,
                    "path": path,
                    "channel": channel,
                    "bulk_id": bulk_id,
                    "bulk_total": int(bulk_total or 0),
                    "bulk_index": int(bulk_index or 0),
                })
            except Exception:
                pass
        # Bump `transcription_pending` for the channel so the Subs-tab
        # auto-indicator stays in sync with OLD's behavior (YTArchiver.py:
        # 14629 and friends set this counter during sync → transcribe flow).
        _bump_transcription_pending(channel, 1)
        self._persist_pending()
        self._ensure_worker()
        return True

    def compress_enqueue(self, path: str, title: str = "",
                         channel: str = "", quality: str = "Average",
                         output_res: str = "720",
                         on_complete: Optional[Callable] = None) -> bool:
        """Queue a video for AV1 NVENC compression via the same GPU
        worker that handles transcription.

        rule: "the GPU task list whole purpose, especially with
        the auto checkbox, is almost like permission to bog down my
        computer." Standalone compress (right-click → Compress, Subs
        batch Compress) used to bypass the queue entirely and fire ffmpeg
        immediately from a bare thread; that ignored the Auto gate. Now
        it enqueues a `kind: "compress"` job so:
          - the task is visible in the GPU Tasks popover
          - the Auto checkbox gates firing (same as transcribe)
          - multiple compresses serialize on the same GPU instead of
            stampeding into parallel NVENC sessions.
        """
        path = str(path)
        if not os.path.isfile(path):
            self._stream.emit_error(f"Compress: file not found: {path}")
            return False
        _job_title = title or os.path.splitext(os.path.basename(path))[0]
        with self._jobs_lock:
            self._jobs.append({
                "kind": "compress",
                "path": path,
                "title": _job_title,
                "channel": channel,
                "quality": quality,
                "output_res": str(output_res),
                "cb": on_complete,
                "cancel": threading.Event(),
            })
        if self._queues is not None:
            try:
                self._queues.gpu_enqueue({
                    "kind": "compress",
                    "title": _job_title,
                    "path": path,
                    "channel": channel,
                })
            except Exception:
                pass
        self._persist_pending()
        self._ensure_worker()
        return True

    # ── Pending journal (survives restart) ──

    def _persist_pending(self):
        """Write current pending jobs to disk so a crash/restart recovers them."""
        try:
            import json as _json
            with self._jobs_lock:
                snapshot = [
                    {"path": j["path"], "title": j.get("title", ""),
                     "channel": j.get("channel", "")}
                    for j in self._jobs
                ]
            # Include in-flight job at top
            if self._current_job:
                snapshot.insert(0, {
                    "path": self._current_job["path"],
                    "title": self._current_job.get("title", ""),
                    "channel": self._current_job.get("channel", ""),
                })
            p = _pending_journal_path()
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = str(p) + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                _json.dump(snapshot, f, indent=2)
            os.replace(tmp, p)
        except Exception:
            pass

    def load_pending(self) -> int:
        """Load any jobs left behind from a previous session. Returns count.

        "Already transcribed" means the title already has an entry in the
        aggregated {channel} Transcript.txt (matches YTArchiver.py's
        _scan_existing_transcripts). Falls back to checking a legacy
        per-video {base}.jsonl sidecar from older builds.
        """
        try:
            import json as _json
            p = _pending_journal_path()
            if not p.exists():
                return 0
            with p.open("r", encoding="utf-8") as f:
                jobs = _json.load(f)
            if not isinstance(jobs, list):
                return 0

            # Cache existing-title sets per channel so we don't re-walk the
            # folder N times.
            _title_cache: Dict[str, set] = {}
            def _already_transcribed(video_path: str, title: str, channel: str) -> bool:
                # Legacy per-video .jsonl from an earlier build
                base = os.path.splitext(video_path)[0]
                if os.path.isfile(base + ".jsonl"):
                    return True
                # Aggregated Transcript.txt scan
                paths = _resolve_transcript_paths(video_path, title, channel)
                if paths is None:
                    return False
                txt_path, _jp, _y, _m, _ud = paths
                folder = os.path.dirname(os.path.dirname(txt_path)) \
                         if os.path.basename(os.path.dirname(txt_path)).isdigit() \
                         else os.path.dirname(txt_path)
                cache_key = f"{channel}::{folder}"
                titles = _title_cache.get(cache_key)
                if titles is None:
                    titles = _scan_existing_transcript_titles(folder, channel)
                    _title_cache[cache_key] = titles
                # `titles` is a dict keyed by the normalized title form.
                return _norm_title(title) in titles

            recovered = 0
            for j in jobs:
                path = j.get("path") or ""
                if not path or not os.path.isfile(path):
                    continue
                if _already_transcribed(path, j.get("title", ""), j.get("channel", "")):
                    continue
                self.enqueue(path, j.get("title", ""), j.get("channel", ""))
                recovered += 1
            return recovered
        except Exception:
            return 0

    def queue_size(self) -> int:
        with self._jobs_lock:
            n = len(self._jobs)
        if self._current_job:
            n += 1
        return n

    def cancel_all(self):
        self._cancel_all.set()
        with self._jobs_lock:
            self._jobs.clear()
        if self._current_job:
            self._current_job["cancel"].set()

    def pause(self):
        self._paused.set()

    def resume(self):
        self._paused.clear()

    def is_active(self) -> bool:
        """True if a GPU job is currently running OR jobs remain queued.

        Earlier versions returned `self._worker_thread.is_alive()`, but
        that leaves the blink state stuck ON after the last job finishes:
        the worker sets `_current_job=None` + fires `set_current_gpu(None)`
        → `_on_queue_changed` runs → `is_alive()` still True → blink keeps
        going → next loop iteration finally breaks out → thread dies →
        no final notify fires → UI never repaints to idle.

        Using job-state instead of thread-liveness breaks that race: once
        the queue is empty and no job is running, is_active() returns False
        immediately and the final notify paints the button to idle.
        """
        if self._current_job is not None:
            return True
        with self._jobs_lock:
            return len(self._jobs) > 0

    def skip_current(self):
        """Cancel the currently-running job but keep the queue + worker alive.

        Fires the per-job cancel event so _transcribe_one returns promptly;
        worker loop then picks up the next job. No-op if nothing running.
        """
        job = self._current_job
        if job and "cancel" in job:
            try:
                job["cancel"].set()
            except Exception:
                pass

    def _ensure_worker(self):
        if self._worker_thread is None or not self._worker_thread.is_alive():
            self._cancel_all.clear()
            self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self._worker_thread.start()

    def _worker_loop(self):
        # Whisper env sanity check only — NOT the model load. The actual
        # `start_subprocess()` (which loads the model onto GPU and prints
        # the "Loading Whisper model..." banner) is deferred to
        # `_transcribe_one`, which only fires it after the auto-captions
        # fast-path misses. On channels where YouTube auto-captions cover
        # everything (most podcasts / news / interview content), we never
        # load Whisper at all — OLD YTArchiver's _fetch_auto_captions path
        # has the same short-circuit.
        if not self.is_available():
            self._stream.emit_error(
                "Whisper: Python 3.11 not found. Install from python.org "
                "to enable transcription.")
            with self._jobs_lock:
                self._jobs.clear()
            return

        while not self._cancel_all.is_set():
            # Two gates at the top of the loop before popping a job:
            # 1. `_paused` — set by queue_pause("gpu") or disk-watchdog.
            # Parks the worker thread without draining the queue,
            # so tasks stay visible in the popover with "paused"
            # status. Matches OLD's _wait_if_paused pattern.
            # 2. Auto-checkbox — when `autorun_gpu` is False, incoming
            # jobs sit in the queue without firing. rule:
            # "if the GPU task list auto box is unchecked and a
            # transcription task gets kicked over there, it
            # doesn't fire." We poll both every 250ms.
            while (not self._cancel_all.is_set() and
                   (self._paused.is_set() or not self._auto_enabled())):
                time.sleep(0.25)
            if self._cancel_all.is_set():
                break
            with self._jobs_lock:
                if not self._jobs:
                    break
                job = self._jobs.pop(0)
            self._current_job = job
            # Reflect "now running" in the shared GPU queue: pop the
            # matching popover entry off `queues.gpu` and stamp it as
            # `current_gpu` so the popover's top row switches to
            # "Transcribing X" / "Compressing X" while the rest shrink
            # upward. Label verb comes from the job's `kind`.
            _job_kind = job.get("kind") or "transcribe"
            if self._queues is not None:
                try:
                    popped = self._queues.gpu_pop()
                    self._queues.set_current_gpu({
                        "kind": _job_kind,
                        "title": job.get("title", ""),
                        "path": job.get("path", ""),
                        "channel": job.get("channel", ""),
                    })
                except Exception:
                    pass
            # Track per-channel stats so we can emit a [Trnscr] history
            # row when the worker drains. Matches OLD's _record_transcription.
            ch_name = (job.get("channel") or "").strip() or "\u2014"
            stats = self._batch_stats.setdefault(ch_name,
                {"start": time.time(), "done": 0, "err": 0})
            crashed = False
            try:
                if _job_kind == "compress":
                    self._compress_one(job)
                else:
                    self._transcribe_one(job)
            except Exception as e:
                self._stream.emit_error(f"{_job_kind.capitalize()} crashed: {e}")
                crashed = True
            finally:
                if crashed:
                    stats["err"] += 1
                else:
                    stats["done"] += 1
                # bug C-2: if a transcribe job crashed or early-returned
                # without reaching the success-path decrement, the pending
                # counter would leak (-1, -2, -3 stuck on the Subs row
                # forever). Any non-retranscribe transcribe job that didn't
                # set `_pending_decremented` gets drained here.
                if (_job_kind != "compress"
                        and not job.get("retranscribe")
                        and not job.get("_pending_decremented")):
                    try:
                        ch_for_decrement = (job.get("channel") or "").strip()
                        if ch_for_decrement:
                            _bump_transcription_pending(ch_for_decrement, -1)
                        _vid = job.get("video_id") or ""
                        if _vid:
                            from . import ytarchiver_config as _cfg
                            _cfg.remove_pending_tx_id(_vid)
                    except Exception:
                        pass
                self._current_job = None
                # Clear the "running" slot on completion so the popover
                # returns to idle (or shows the next queued item as the
                # next iteration sets its own current_gpu).
                if self._queues is not None:
                    try:
                        self._queues.set_current_gpu(None)
                    except Exception:
                        pass
                self._persist_pending()

        # Flush per-channel batch stats to autorun_history + activity log.
        # One row per channel processed in this worker session.
        try:
            self._flush_batch_stats()
        except Exception:
            pass
        self._stream.flush()

    def _compress_one(self, job: Dict[str, Any]):
        """Run one compress job from the GPU queue — delegates to
        backend.compress.compress_video(). Shares the same worker
        thread as transcribe so only one GPU task runs at a time."""
        if job["cancel"].is_set():
            return
        try:
            from . import compress as _cmp
        except Exception as e:
            self._stream.emit_error(f"Compress: import failed: {e}")
            return
        try:
            res = _cmp.compress_video(
                job["path"],
                self._stream,
                quality=job.get("quality", "Average"),
                output_res=str(job.get("output_res", "720")),
                cancel_event=job["cancel"],
            )
        except Exception as e:
            self._stream.emit_error(f"Compress: {e}")
            return
        if job.get("cb"):
            try: job["cb"](res)
            except Exception: pass

    def _flush_batch_stats(self):
        """Emit [Trnscr] autorun_history rows for MANUAL transcribe-only
        channels (right-click \u2192 Transcribe on a channel/video).

        Sync-originated channels are skipped two ways:
          (a) a bug: fast auto-captions finish BEFORE sync_channel
              ends, so we check `sync.is_sync_active(name)` and leave
              those stats in place — sync_channel will read+emit them
              when it finishes.
          (b) Normal case: sync_channel already called
              `consume_channel_batch_stats()` and the entry is gone.
        """
        if not self._batch_stats:
            return
        try:
            from . import autorun as _ar
        except Exception:
            self._batch_stats.clear()
            return
        try:
            from . import sync as _sync
        except Exception:
            _sync = None
        from datetime import datetime as _dt
        now = _dt.now()
        time_str = now.strftime("%I:%M%p").lstrip("0").lower()
        date_str = now.strftime("%b %d").replace(" 0", " ")
        # Iterate over a snapshot of keys — we selectively pop emitted
        # channels instead of clearing wholesale, so sync-active channels'
        # stats stay put for sync_channel to consume at its end.
        for ch_name in list(self._batch_stats.keys()):
            if _sync is not None and _sync.is_sync_active(ch_name):
                continue # leave for sync_channel to emit as [Dwnld]
            s = self._batch_stats.pop(ch_name, None) or {}
            done = int(s.get("done", 0))
            err = int(s.get("err", 0))
            if done == 0 and err == 0:
                continue # no work actually happened
            elapsed = time.time() - float(s.get("start", time.time()))

            # If sync just emitted a [Dwnld] row for this channel and
            # the transcribe count was 0 at the time (Whisper still
            # running), patch that same row in place by re-emitting
            # with the registered row_id instead of appending a
            # separate [Trnscr]. The UI's `data-row-id` lookup swaps
            # the row contents. Result: one consolidated row with the
            # final counts, no duplicate.
            pending = None
            if _sync is not None:
                try:
                    pending = _sync.pop_pending_dwnld_row(ch_name)
                except Exception:
                    pending = None
            if pending is not None:
                try:
                    # Total elapsed = time since sync_channel started
                    # (NOT just the transcribe portion) so the "took"
                    # cell reflects the whole channel's pass duration.
                    _total_elapsed = time.time() - float(
                        pending.get("elapsed_start", time.time()))
                    _sync.emit_consolidated_auto_row(
                        self._stream, ch_name,
                        downloaded=int(pending.get("downloaded", 0)),
                        transcribed=done,
                        metadata=int(pending.get("metadata", 0)),
                        errors=int(pending.get("errors", 0)) + err,
                        elapsed=float(_total_elapsed),
                        kind="Dwnld",
                        row_id=str(pending.get("row_id") or ""),
                    )
                except Exception:
                    pass
                continue

            # No recent [Dwnld] row for this channel — emit a
            # standalone [Trnscr] as before (manual transcribe flow,
            # etc.).
            primary = f"{done} transcribed"
            try:
                _ar.append_history_entry(
                    _ar.format_history_entry("Trnscr", ch_name,
                                             primary, secondary="",
                                             errors=err, took_sec=elapsed))
            except Exception:
                pass
            try:
                self._stream.emit_activity({
                    "kind": "Trnscr",
                    "time_date": f"{time_str}, {date_str}",
                    "channel": "" if ch_name == "\u2014" else ch_name,
                    "primary": primary,
                    "secondary": "",
                    "errors": f"{err} errors",
                    "took": f"took {int(elapsed)}s" if elapsed < 60
                                 else f"took {int(elapsed)//60}m {int(elapsed)%60}s",
                    "row_tag": "hist_blue" if done > 0 else "",
                })
            except Exception:
                pass

    def _transcribe_one(self, job: Dict[str, Any]):
        path = job["path"]
        title = job["title"]
        if job["cancel"].is_set():
            return

        # Unique-per-job inplace kind. Every emit from this job's
        # lifecycle (Loading punctuation model, Adding punctuation,
        # Whisper progress ticks, final done line) carries this tag
        # so they replace EACH OTHER within the job but stay
        # independent of other jobs' emits. Without this, video 2's
        # "Loading punctuation..." would stomp video 1's done line
        # when two videos for the same channel get transcribed in
        # sequence. Store on the job so `punct_mgr` can pick it up.
        global _JOB_COUNTER
        _JOB_COUNTER += 1
        job_tag = f"whisper_job_{_JOB_COUNTER}"
        job["job_tag"] = job_tag

        # ── Auto-captions fast-path ──
        # If yt-dlp already dropped a .vtt subtitle sidecar for this video
        # (English captions), parse it straight into .jsonl + .txt — way
        # faster than running Whisper and usually just as good for recent
        # podcast / news-type content.
        #
        # Skipped for retranscribe jobs: when the user explicitly asks to
        # Re-transcribe with Whisper, the whole point is to REPLACE the
        # auto-captions transcript with a Whisper one. Taking the VTT
        # fast-path here would just regenerate the auto-captions entry.
        #
        # Passes `self._punct` so the fetched captions get the same
        # punctuation-restoration pass OLD YTArchiver.py:15437 runs.
        # Captions written WITH punct get the `YT+PUNCTUATION` source
        # tag; captions written without get plain `YT CAPTIONS`.
        _punct_for_captions = self._punct if self._punctuate_enabled else None
        # Tell PunctuationManager which job_tag to use for its
        # "Loading punctuation model..." emit so that line joins
        # this video's inplace family.
        if _punct_for_captions is not None:
            try: _punct_for_captions._job_tag = job_tag
            except Exception: pass
        if (not job.get("retranscribe") and
                _try_auto_captions(path, title, job.get("channel", ""),
                                   self._stream,
                                   punct_mgr=_punct_for_captions,
                                   job_tag=job_tag)):
            if job.get("cb"):
                try:
                    job["cb"]({"auto_captions": True})
                except Exception:
                    pass
            return

        # Auto-captions path missed — either no .vtt available, yt-dlp
        # couldn't fetch captions for this video, or the VTT parse came
        # back empty. was flagged the silent-failure case: NO log
        # line at all between "Metadata downloaded" and the next
        # channel's header. Emit a visible fallback line so the user
        # sees Whisper take over instead of the transcription just
        # vanishing. Uses the `whisper_progress` inplace family so the
        # final "— ✓ Transcription" done line replaces this too.
        if not job.get("retranscribe"):
            self._stream.emit([[
                " No auto-captions available \u2014 using Whisper\u2026\n",
                ["transcribe_using", job_tag],
            ]])

        if self._proc is None or self._proc.poll() is not None:
            if not self.start_subprocess():
                # Subprocess failed to start — emit an error so the
                # user knows why the transcription silently died
                # (Python 3.11 missing, GPU driver wrong, model
                # download failed, etc.). Without this the job just
                # disappears from view. `emit_error` routes to the
                # red error style.
                self._stream.emit_error(
                    f"Whisper failed to start \u2014 transcription for "
                    f"\"{title}\" skipped. Check Python 3.11 install "
                    f"+ CUDA drivers.")
                if job.get("cb"):
                    try: job["cb"]({"ok": False, "reason": "whisper_start_failed"})
                    except Exception: pass
                return

        # ── Chunked path for long videos (>~2 hours) ──
        # Splits the file into overlapping WAV chunks with ffmpeg, transcribes
        # each, and merges segments (offset timestamps, drop overlap dupes).
        # Matches YTArchiver.py:11139 _whisper_transcribe_chunked.
        duration = _ffprobe_duration(path) or 0.0
        if duration >= _CHUNK_MIN_DURATION:
            self._transcribe_chunked(job, duration)
            return

        # Progress line — ports OLD YTArchiver.py:11340 exactly:
        # "[1/1] Transcribing "<title>", 0%..."
        # The whole line carries the `whisper_progress` tag, which the
        # JS inplace detector treats as a single replace-in-place line
        # — each pct tick overwrites the previous one. The `[1/1]`
        # counter is literal for now; when batch transcription lands we
        # can swap in a real idx/total. Title truncated to match OLD's
        # _trunc_pad_title visual width (40 chars).
        _disp_title = title[:40].rstrip()
        _t_start = time.time() # for the "[1/1] … — done (Ns)" line below
        def _emit_progress(pct, suffix=""):
            # All segments carry the per-job tag so every tick
            # replaces the previous tick for THIS video, and the
            # final done line replaces the last tick without
            # touching any OTHER video's transcription lines.
            self._stream.emit([
                ["[", ["whisper_bracket", job_tag]],
                ["1", ["whisper_prefix", job_tag]],
                ["/", ["whisper_bracket", job_tag]],
                ["1", ["whisper_prefix", job_tag]],
                ["] ", ["whisper_bracket", job_tag]],
                ["Transcribing", [job_tag]],
                [f' "{_disp_title}"', [job_tag]],
                [", ", [job_tag]],
                [f"{pct}%", ["whisper_pct", job_tag]],
                [f"{suffix}...\n", [job_tag]],
            ])
        _emit_progress(0)

        # Request
        req = json.dumps({"path": path, "duration": 0}) + "\n"
        try:
            self._proc.stdin.write(req)
            self._proc.stdin.flush()
        except Exception as e:
            self._stream.emit_error(f"Write to whisper failed: {e}")
            self._stop_subprocess()
            return

        # Read responses until we get "ok" or "error"
        last_pct = -1
        result = None
        while True:
            if job["cancel"].is_set() or self._cancel_all.is_set():
                self._stream.emit_text(" \u26d4 Transcription cancelled.", "red")
                self._stop_subprocess()
                return
            try:
                line = self._line_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if line is None:
                self._stream.emit_error("Whisper subprocess ended unexpectedly.")
                return
            try:
                msg = json.loads(line.strip())
            except json.JSONDecodeError:
                continue
            status = msg.get("status")
            if status == "progress":
                pct = int(msg.get("pct", 0))
                if pct != last_pct:
                    last_pct = pct
                    _emit_progress(pct)
                continue
            if status == "starting":
                continue
            if status == "ok":
                result = msg
                break
            if status == "error":
                err = msg.get('text', 'unknown')
                # CUDA OOM recovery: kill the subprocess, fall back to CPU,
                # and requeue this job at the front.
                low = err.lower()
                if ("cuda" in low and ("out of memory" in low or "oom" in low)) or "cublas" in low:
                    self._stream.emit_error(f"Whisper CUDA error: {err}")
                    self._stream.emit_text(
                        " \u21A9 Falling back to CPU mode for this session.",
                        "simpleline_blue")
                    self._stop_subprocess(force=True)
                    # Force CPU on next start
                    os.environ["WHISPER_DEVICE"] = "cpu"
                    os.environ["WHISPER_COMPUTE"] = "default"
                    # Requeue this job (but not in a loop — bail if it fails again)
                    if not job.get("_retried_cpu"):
                        job["_retried_cpu"] = True
                        with self._jobs_lock:
                            self._jobs.insert(0, job)
                    return
                self._stream.emit_error(f"Whisper error: {err}")
                return

        # Write output files + ingest into FTS index
        if result:
            channel = job.get("channel") or ""
            # Run punctuation pass over the raw text (and each segment's t)
            if self._punctuate_enabled:
                # bug L-7: track whether punct succeeded so the source
                # tag can reflect reality. Previously a failed punct
                # pass left the tag as "(WHISPER:model)" even though
                # the text was unpunctuated — users assumed punctuation
                # was present in the Watch banner.
                result["_punct_attempted"] = True
                result["_punct_success"] = False
                try:
                    raw_text = result.get("text", "") or ""
                    if raw_text:
                        punct_text = self._punct.punctuate(raw_text)
                        if punct_text and punct_text != raw_text:
                            result["text"] = punct_text
                            # Also run punctuation per-segment so the .jsonl
                            # matches what search / transcript view shows
                            for seg in result.get("segments", []):
                                t = seg.get("t", "")
                                if t and len(t.split()) >= 3:
                                    pt = self._punct.punctuate(t)
                                    if pt:
                                        seg["t"] = pt
                            result["_punct_success"] = True
                except Exception as _pe:
                    self._stream.emit_dim(f" (punctuation pass skipped: {_pe})")
            self._write_outputs(path, result, title=title, channel=channel,
                                combined_override=job.get("combined_override"),
                                retranscribe=bool(job.get("retranscribe")),
                                video_id_hint=job.get("video_id", ""))
            # Done line — in-place replaces the sync.py-reserved
            # `tx_done_<vid>` placeholder under the channel's block
            # (`_inplaceKind` prioritizes `tx_done_` over `whisper_job_`),
            # so the final line lands at the right scroll position
            # instead of wherever the GPU worker happened to finish.
            # The `whisper_job_<N>` tag is retained for in-batch
            # progress-tick replacement.
            _elapsed = max(1, int(time.time() - _t_start))
            _time_str = (f"{_elapsed // 60}min {_elapsed % 60:02d}sec"
                          if _elapsed >= 60 else f"{_elapsed}sec")
            _vid_for_marker = (job.get("video_id") or "").strip()
            _tx_tag = f"tx_done_{_vid_for_marker}" if _vid_for_marker else ""
            # _tx_tag FIRST in each tag list so logs.js `_inplaceKind`
            # resolves this line to tx_done_<vid> and matches the
            # placeholder emitted by sync.py. Putting _tx_tag last let
            # the renderer hit `whisper_job_N` first and return that,
            # so the done line couldn't find the placeholder and
            # appended fresh below — leaving both "⏳ Transcription
            # queued…" and "✓ Transcription (took Xsec)" visible.
            # (logs.js has also been fixed to scan all tags with
            # tx_done_ priority first; this is belt-and-suspenders.)
            _dim_tags = [t for t in (_tx_tag, "dim", job_tag) if t]
            _em_tags = [t for t in (_tx_tag, "whisper_bracket", job_tag) if t]
            _lbl_tags = [t for t in (_tx_tag, "simpleline_blue", job_tag) if t]
            self._stream.emit([
                [" ", _dim_tags],
                ["\u2014 \u2713 ", _em_tags],
                ["Transcription", _lbl_tags],
                [f" (took {_time_str})\n", _dim_tags],
            ])
            if job.get("cb"):
                try:
                    job["cb"](result)
                except Exception:
                    pass

    def _transcribe_chunked(self, job: Dict[str, Any], total_duration: float):
        """Port of YTArchiver.py:11139 _whisper_transcribe_chunked.

        ffmpeg splits the audio into 2h windows with 30s of overlap; each
        chunk is transcribed individually and their segment lists are merged
        with timestamps offset, dropping duplicates in the overlap zone.
        """
        import tempfile as _tf
        path = job["path"]
        title = job["title"]
        channel = job.get("channel", "")
        cancel = job["cancel"]
        hours = total_duration / 3600.0
        n_chunks = max(1, int(total_duration / _CHUNK_DURATION_SECS) +
                       (1 if total_duration % _CHUNK_DURATION_SECS > 0 else 0))
        _disp_title_chunked = title[:40].rstrip()
        _t_start_chunked = time.time()
        self._stream.emit([
            ["Transcribing ", "transcribe_using"],
            [f'"{title}"', "transcribe_title"],
            [" \u2014 ", "dim"],
            [f"{hours:.1f}h, {n_chunks} sections\n", "simpleline"],
        ])

        all_text_parts: List[str] = []
        all_segments: List[Dict[str, Any]] = []
        chunk_dir = _tf.mkdtemp(prefix="yt_whisper_chunk_")
        try:
            for ci in range(n_chunks):
                if cancel.is_set() or self._cancel_all.is_set():
                    break
                # Respect pause between chunks
                while self._paused.is_set() and not cancel.is_set():
                    time.sleep(0.5)
                if cancel.is_set():
                    break

                start_sec = ci * _CHUNK_DURATION_SECS
                if ci > 0:
                    start_sec -= _CHUNK_OVERLAP_SECS
                end_sec = min(start_sec + _CHUNK_DURATION_SECS +
                              (_CHUNK_OVERLAP_SECS if ci > 0 else 0),
                              total_duration)
                chunk_dur = end_sec - start_sec
                if chunk_dur <= 0:
                    break

                chunk_path = os.path.join(chunk_dir, f"chunk_{ci:03d}.wav")
                ff_cmd = [
                    "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
                    "-ss", str(start_sec), "-t", str(chunk_dur),
                    "-i", path, "-vn", "-ac", "1", "-ar", "16000",
                    "-acodec", "pcm_s16le", chunk_path,
                ]
                try:
                    subprocess.run(
                        ff_cmd, check=True, capture_output=True,
                        timeout=600,
                        creationflags=(0x08000000 if os.name == "nt" else 0),
                    )
                except Exception as e:
                    self._stream.emit_error(f"Section {ci+1}/{n_chunks} split failed: {e}")
                    continue

                # Hand the chunk to Whisper via the persistent subprocess.
                section_prefix = f" Section {ci+1}/{n_chunks},"
                t_start = time.time()
                result = self._transcribe_single_file(chunk_path, job,
                                                      _log_prefix=section_prefix)
                t_elapsed = time.time() - t_start
                try: os.remove(chunk_path)
                except Exception: pass

                if not result:
                    self._stream.emit([
                        [f" Section {ci+1}/{n_chunks} \u2014 no speech\n", "simpleline"],
                    ])
                    continue

                # Log per-section summary
                cd_m, cd_s = divmod(int(chunk_dur), 60)
                te_m, te_s = divmod(int(t_elapsed), 60)
                te_str = f"{te_m}min {te_s:02d}sec" if te_m else f"{te_s}sec"
                rt = f"{chunk_dur / t_elapsed:.1f}x realtime" if t_elapsed > 0 else ""
                self._stream.emit([
                    [f" Section {ci+1}/{n_chunks} done "
                     f"({cd_m}m{cd_s:02d}s, {te_str}, {rt})\n", "simpleline_blue"],
                ])

                txt = result.get("text") or ""
                if txt:
                    all_text_parts.append(txt)
                segs = result.get("segments") or []
                # Offset timestamps, drop overlap duplicates from the new chunk
                for s in segs:
                    if "s" in s: s["s"] = round(s["s"] + start_sec, 2)
                    if "e" in s: s["e"] = round(s["e"] + start_sec, 2)
                    for w in s.get("w", []):
                        if "s" in w: w["s"] = round(w["s"] + start_sec, 3)
                        if "e" in w: w["e"] = round(w["e"] + start_sec, 3)
                if ci > 0 and segs:
                    overlap_boundary = start_sec + _CHUNK_OVERLAP_SECS
                    segs = [s for s in segs if s.get("s", 0) >= overlap_boundary - 2]
                all_segments.extend(segs)

            # Merge result
            if not all_segments and not all_text_parts:
                return
            merged = {
                "text": " ".join(all_text_parts) if all_text_parts else "",
                "segments": all_segments,
            }
            # Optional punctuation pass on the merged text (same as single-pass)
            if self._punctuate_enabled and merged["text"]:
                try:
                    punct = self._punct.punctuate(merged["text"])
                    if punct: merged["text"] = punct
                except Exception:
                    pass
            self._write_outputs(path, merged, title=title, channel=channel,
                                combined_override=job.get("combined_override"),
                                retranscribe=bool(job.get("retranscribe")),
                                video_id_hint=job.get("video_id", ""))
            # Done line — REPLACES the last whisper_progress chunk line
            # in place via `whisper_progress` inplace kind. Matches OLD
            # YTArchiver.py:16495 format with (chunked) suffix to
            # distinguish the long-video path.
            _elapsed_c = max(1, int(time.time() - _t_start_chunked))
            _time_str_c = (f"{_elapsed_c // 60}min {_elapsed_c % 60:02d}sec"
                            if _elapsed_c >= 60 else f"{_elapsed_c}sec")
            # Simple-mode per-video summary (chunked variant). Same
            # three-line-per-video spec as the non-chunked path; the
            # suffix notes (chunked, <time>) so long-video behavior is
            # still visible without the title/index clutter. Tagged
            # with this job's `job_tag` so it survives past later
            # videos' transcription emits.
            _job_tag_ch = job.get("job_tag", "") or ""
            _em_tag = ["whisper_bracket", _job_tag_ch] if _job_tag_ch else "whisper_bracket"
            _dim_tag = ["dim", _job_tag_ch] if _job_tag_ch else "dim"
            _lbl_tag = ["simpleline_blue", _job_tag_ch] if _job_tag_ch else "simpleline_blue"
            self._stream.emit([
                [" ", _dim_tag],
                ["\u2014 \u2713 ", _em_tag],
                ["Transcription", _lbl_tag],
                [f" (chunked, took {_time_str_c})\n", _dim_tag],
            ])
            if job.get("cb"):
                try: job["cb"](merged)
                except Exception: pass
        finally:
            try: shutil.rmtree(chunk_dir, ignore_errors=True)
            except Exception: pass

    def _transcribe_single_file(self, path: str, job: Dict[str, Any],
                                 _log_prefix: str = "") -> Optional[Dict[str, Any]]:
        """Send one file to the persistent whisper subprocess and collect the
        result. Used by the chunked path to do each section. Returns the
        parsed JSON from the worker (keys: text, segments) or None."""
        if self._proc is None or self._proc.poll() is not None:
            if not self.start_subprocess():
                return None
        try:
            req = json.dumps({"path": path, "duration": 0}) + "\n"
            self._proc.stdin.write(req)
            self._proc.stdin.flush()
        except Exception as e:
            self._stream.emit_error(f"Write to whisper failed: {e}")
            return None
        while True:
            if job["cancel"].is_set() or self._cancel_all.is_set():
                return None
            try:
                line = self._line_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if line is None:
                self._stream.emit_error("Whisper subprocess ended unexpectedly.")
                return None
            try:
                msg = json.loads(line.strip())
            except json.JSONDecodeError:
                continue
            status = msg.get("status")
            if status in ("progress", "starting"):
                continue
            if status == "ok":
                return msg
            if status == "error":
                self._stream.emit_error(
                    f"Whisper error{(' (' + _log_prefix.strip() + ')') if _log_prefix else ''}: "
                    f"{msg.get('text', 'unknown')}")
                return None

    def _write_outputs(self, video_path: str, result: Dict[str, Any],
                       title: str = "", channel: str = "",
                       combined_override: Optional[bool] = None,
                       retranscribe: bool = False,
                       video_id_hint: str = ""):
        """Write a transcript entry to the aggregated {ch} Transcript.txt
        + hidden JSONL sidecar. Matches YTArchiver.py:15449-15478 output
        layout exactly, so OLD YTArchiver can read transcripts written
        here (and vice versa) with zero drift.

        `combined_override` mirrors the job-level flag; forwarded to
        `_resolve_transcript_paths` so the user's first-time
        "Follow / Combined" choice is honoured per video.

        `retranscribe=True` swaps the default append-writers for the
        surgical replace-writers so the old entry for this video gets
        removed from BOTH aggregated files before the new one is
        appended — prevents duplicates in the .txt / .jsonl + the FTS DB.
        Matches YTArchiver.py:16455-16474 retranscribe sequence.
        `video_id_hint` provides the canonical id when the filename
        doesn't carry `[videoId]` — helps `_replace_jsonl_entry` find
        title-drifted stale entries.
        """
        if not title:
            title = os.path.basename(video_path).rsplit(".", 1)[0]
            # Strip any trailing " [videoId]" if the stem has one
            title = re.sub(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$", "", title) or title
        if not channel:
            # Channel = parent folder name (or parent-of-parent when year-split)
            parent = os.path.basename(os.path.dirname(video_path))
            grand = os.path.basename(os.path.dirname(os.path.dirname(video_path)))
            # Heuristic: if parent is a year like "2024" or matches "01 January",
            # the real channel is one level higher.
            if parent.isdigit() or " " in parent and parent.split(" ", 1)[0].isdigit():
                channel = grand
            else:
                channel = parent

        # Resolve OLD-layout paths for this video.
        paths = _resolve_transcript_paths(video_path, title, channel,
                                          combined_override=combined_override)
        if paths is None:
            # Fall back to per-video sidecar in the video's folder (degraded).
            base = os.path.splitext(video_path)[0]
            txt_path = base + ".txt"
            jsonl_path = base + ".jsonl"
            upload_date = ""
        else:
            txt_path, jsonl_path, _y, _m, upload_date = paths

        text = (result.get("text") or "").strip()
        segs = result.get("segments", []) or []

        # Extract video id — OLD-compat filenames don't carry the `[id]`
        # suffix so we can't rely on the filename alone. Try in order:
        # 0. caller-supplied hint (retranscribe flow passes the id from
        # the Browse/watch view where it's already known)
        # 1. `[videoId]` suffix on filename (legacy + mixed archives)
        # 2. FTS DB `videos` table keyed by filepath (populated by sync's
        # DLTRACK line — this is the reliable path for drop-in mode)
        vid_id = (video_id_hint or "").strip()
        if not vid_id:
            stem = os.path.splitext(os.path.basename(video_path))[0]
            m = re.search(r"\[([A-Za-z0-9_-]{11})\]\s*$", stem)
            if m:
                vid_id = m.group(1)
            else:
                try:
                    from . import index as _idx
                    conn = _idx._open()
                    if conn is not None:
                        with _idx._db_lock:
                            row = conn.execute(
                                "SELECT video_id FROM videos WHERE filepath=? "
                                "COLLATE NOCASE LIMIT 1",
                                (os.path.normpath(video_path),)).fetchone()
                        if row and row[0]:
                            vid_id = row[0]
                except Exception:
                    pass

        # Source tag: use the manager's active model so the Transcript.txt
        # header carries the right "(WHISPER:<model>)" tag even when
        # whisper_worker.py's response dict doesn't include "model"
        # (which it doesn't — only status/text/segments come back).
        # Without this, the Watch view banner shows just "Whisper
        # transcription" with no model name. this was flagged
        model_name = (result.get("model") or self._model or "").strip()
        # bug L-7: when punctuation was attempted but failed, append
        # "+NO-PUNCT" to the source tag so the Watch banner accurately
        # reflects that the transcript is unpunctuated. Otherwise the
        # user sees "Whisper:large-v3" and assumes punct is present.
        _punct_attempted = bool(result.get("_punct_attempted"))
        _punct_success = bool(result.get("_punct_success"))
        _punct_suffix = ""
        if _punct_attempted and not _punct_success:
            _punct_suffix = "+NO-PUNCT"
        if model_name:
            source_tag = f"(WHISPER:{model_name}{_punct_suffix})"
        else:
            source_tag = f"(WHISPER{_punct_suffix})"
        # Diagnostic — emit the tag we're about to write so we can
        # confirm it landed correctly. Visible in Verbose log mode.
        try:
            self._stream.emit_dim(
                f" (writing transcript source_tag={source_tag!r})")
        except Exception:
            pass

        duration = segs[-1].get("end", segs[-1].get("e", 0)) if segs else 0

        if retranscribe:
            # Surgically swap the old entries in both aggregated files.
            # Mirrors YTArchiver.py:16462-16474: jsonl FIRST so its
            # video_id-based purge can report back any title-drifted
            # stale entries for the txt pass to also clean up.
            try:
                extra_titles = _replace_jsonl_entry(
                    jsonl_path, title, vid_id, segs) or set()
            except Exception as _je:
                self._stream.emit_error(
                    f"Could not update {os.path.basename(jsonl_path)}: {_je}")
                extra_titles = set()
            try:
                _replace_txt_entry(txt_path, title, text, source_tag,
                                   extra_titles_to_remove=extra_titles)
            except Exception as _te:
                self._stream.emit_error(
                    f"Could not update {os.path.basename(txt_path)}: {_te}")
        else:
            if not _write_transcript_entry(txt_path, title, upload_date,
                                           duration, source_tag, text):
                self._stream.emit_error(f"Could not write transcript to {txt_path}")
                return
            _write_jsonl_entry(jsonl_path, vid_id, title, segs)

        # Ingest into FTS index — `ingest_jsonl` does a DELETE WHERE
        # jsonl_path=? first, so re-ingesting after a retranscribe
        # wipes + rebuilds the segments for the WHOLE aggregated file
        # (harmless for the other videos sharing it — their lines in
        # the .jsonl are untouched and get re-inserted as-is).
        try:
            from . import index as _idx
            _idx.ingest_jsonl(video_path, jsonl_path, title, channel)
            _idx.mark_video_transcribed(video_path)
        except Exception as e:
            self._stream.emit_dim(f" (index ingest skipped: {e})")
        # Decrement transcription_pending / set transcription_complete on 0.
        # Matches YTArchiver.py:14629-14630. Skip the decrement on
        # retranscribe — it wasn't incremented when the Re-transcribe
        # button was clicked (unlike a normal sync-triggered transcribe).
        if not retranscribe:
            _bump_transcription_pending(channel, -1)
            # Drain the authoritative pending-ID list too.
            if vid_id:
                try:
                    from . import ytarchiver_config as _cfg
                    _cfg.remove_pending_tx_id(vid_id)
                except Exception:
                    pass
            # bug C-2: mark decrement-done so the worker-loop's
            # exception finally doesn't decrement AGAIN on the success
            # path. The finally's decrement exists only for error paths
            # (Whisper crash, OOM, venv missing) that used to leak the
            # counter and leave the Subs row stuck at `-N`.
            job["_pending_decremented"] = True
