"""
transcribe.helpers — small pure helpers used by TranscribeManager +
PunctuationManager.

Extracted from transcribe/core.py (Patch 16, v71.8). Owns the
path/title normalization, channel lookup, transcript-file scanning,
ffprobe duration probe, transcription-pending counter, output-dir
resolver, and the Python 3.11 discovery for the Whisper subprocess.

Constants:
    _CHUNK_DURATION_SECS, _CHUNK_OVERLAP_SECS, _CHUNK_MIN_DURATION
        — long-video chunking thresholds used by TranscribeManager
"""
from __future__ import annotations

import glob
import os
import re
import subprocess
from datetime import datetime
from typing import Any

from ..log import get_logger
from ..transcribe_paths import (
    _get_jsonl_sidecar,
    _get_transcript_filename,
)
from .transcribe_files import _HEADER_RE

_log = get_logger(__name__)


def _norm_title(s: str) -> str:
    """Patch 1 (v66.5): delegates to backend.text_utils.normalize_title.
    Kept as a thin alias to avoid touching every internal caller in
    this file. New code should import from text_utils directly.
    """
    from ..text_utils import normalize_title
    return normalize_title(s)


def _extract_video_id(video_path: str, hint: str = "") -> str:
    """Patch 11 (v67.7): consolidates the three inline filename-regex +
    videos-table-fallback blocks that lived in _fetch_captions_via_ytdlp,
    _try_auto_captions, and _write_outputs.

    Strategy: hint -> filename `[id]` suffix -> FTS DB lookup.
    Returns "" on no-match. Never raises.
    """
    from ..text_utils import extract_video_id as _canon
    # Quick paths (no DB):
    quick = _canon(video_path, hint=hint)
    if quick:
        return quick
    # FTS DB fallback — pure read of the videos table. Use the reader
    # connection so transcribe / auto-caption ID extraction never has
    # to wait behind a long-running sweep or ingest write.
    try:
        from .. import index as _idx
        conn = _idx._reader_open()
        if conn is not None:
            with _idx._reader_lock:
                return _canon(video_path, hint=hint, conn=conn)
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return ""


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
    # use the full _HEADER_RE (defined above at module
    # level) which captures all four groups with proper delimiter
    # handling, instead of the prior non-greedy pattern that truncated
    # titles containing `), (`. A title like "Episode 3 (cont), (2024
    # edition)" used to register under the wrong normalized key
    # ("Episode 3 (cont") so the pre-dedupe check missed it and
    # re-transcribed → duplicate entries.
    id_pattern = re.compile(r"\[([A-Za-z0-9_-]{11})\]\s*$")
    if not folder_path or not os.path.isdir(folder_path):
        return existing
    for dirpath, _dirs, files in os.walk(folder_path):
        for f in files:
            if not f.endswith("Transcript.txt"):
                continue
            try:
                with open(os.path.join(dirpath, f), "r", encoding="utf-8") as fh:
                    content = fh.read()
                for m in _HEADER_RE.finditer(content):
                    raw = (m.group(1) or "").strip()
                    if not raw:
                        continue
                    vid_id = ""
                    im = id_pattern.search(raw)
                    if im:
                        vid_id = im.group(1)
                    # Store TWO variants so callers can match either:
                    # title-with-[id] OR title-without-[id].
                    raw_plain = id_pattern.sub("", raw).strip() or raw
                    existing[_norm_title(raw)] = (raw, vid_id)
                    existing[_norm_title(raw_plain)] = (raw, vid_id)
            except Exception as e:
                _log.debug("swallowed: %s", e)
    return existing


def _lookup_channel(channel_name: str) -> dict[str, Any] | None:
    """Look up a channel dict in config by name. Lightweight: just for
    resolving split_years/split_months when writing transcripts."""
    if not channel_name:
        return None
    try:
        from .. import ytarchiver_config as _cfg
        cfg = _cfg.load_config()
        for ch in cfg.get("channels", []):
            if (ch.get("name") or "") == channel_name or \
               (ch.get("folder") or ch.get("folder_override") or "") == channel_name:
                return ch
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return None


def _resolve_transcript_paths(video_path: str, title: str,
                              channel_name: str,
                              combined_override: bool | None = None
                              ) -> tuple[str, str, int, int, str] | None:
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
        from ..sync import channel_folder_name as _cfn
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
        from .. import ytarchiver_config as _cfg
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
        from .. import ytarchiver_config as _cfg
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
    except Exception as e:
        _log.debug("swallowed: %s", e)


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


def _ffprobe_duration(filepath: str) -> float | None:
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
# ── Python 3.11 discovery (same pattern as YTArchiver.py:8653) ─────────

def find_python311() -> str | None:
    """Locate a Python 3.11 executable to run the Whisper worker.

    Whisper (specifically faster-whisper + its CTranslate2 / CUDA wheels)
    is pinned to Python 3.11 — the wheels for 3.13 don't exist on PyPI
    yet, and we don't want to bundle CUDA into the main app. So the
    pywebview shell runs on Python 3.13 and shells out to a separate
    Python 3.11 process for transcription work.

    Search order (first hit wins):
      1. `%LOCALAPPDATA%\\Programs\\Python\\Python311*\\python.exe`
         (the per-user install path the official Python installer uses
         by default — this is where most installs land)
      2. `C:\\Python311\\python.exe` and `C:\\Python310\\python.exe`
         (old "all users" location, kept as a backstop)
      3. `%PROGRAMFILES%\\Python311\\python.exe` and the WOW64 variant
      4. Whatever `python3.11` or `python` resolves to on PATH
      5. A hard-coded last-ditch path under `%LOCALAPPDATA%`

    Returns the absolute path to `python.exe`, or `None` if nothing
    suitable was found — callers should surface a friendly "please
    install Python 3.11" message rather than crashing.
    """
    import shutil as _shutil
    candidates: list[str] = []
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
