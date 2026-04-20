"""
Reorg — move video files into year/month subfolders based on mtime.

Matches YTArchiver's reorg behavior: file modification time is treated as
the YouTube upload date (per feedback_ytarchiver_filedate.md memory). Files
get moved into <channel>/<year>/ if split_years, and
<channel>/<year>/<Month>/ if split_months.

Three modes:
  - reorg_none(channel_folder)          flatten back to one folder
  - reorg_years(channel_folder)         split into yyyy/
  - reorg_months(channel_folder)        split into yyyy/MonthName/
"""

from __future__ import annotations

import os
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .log_stream import LogStreamer


_VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".avi", ".mov", ".flv", ".wmv", ".m4v",
               ".wav", ".mp3", ".m4a", ".flac")
_SIDECAR_EXTS = (".txt", ".jsonl", ".info.json", ".jpg", ".jpeg", ".png", ".webp",
                 ".vtt", ".srt", ".description")


# Must match YTArchiver.py:7483 MONTH_NAMES exactly — OLD's sync writes
# "01 January", "02 February", etc. and reorg must produce the same names
# so the two apps never create sibling folders for the same month.
# Shared with metadata.py + transcribe.py — see backend.utils.MONTH_FOLDERS.
from .utils import MONTH_FOLDERS as _MONTH_FOLDERS
# Legacy name kept for any callers that imported it; points to the plain
# month name (not used for new writes any more).
_MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"]


def _gather_video_files(root: Path) -> List[Path]:
    """Walk `root` for video-type files (skipping our temp files)."""
    out: List[Path] = []
    for dp, _dns, fns in os.walk(root):
        for fn in fns:
            if "_TEMP_COMPRESS" in fn or fn.endswith(".part"):
                continue
            if fn.lower().endswith(_VIDEO_EXTS):
                out.append(Path(dp) / fn)
    return out


def _sidecars_for(video: Path) -> List[Path]:
    """Return any .txt/.jsonl/.info.json/.jpg files that share the video stem."""
    stem = video.stem
    folder = video.parent
    out: List[Path] = []
    for p in folder.iterdir():
        if not p.is_file():
            continue
        if p == video:
            continue
        if p.name.startswith(stem) and p.suffix.lower() in _SIDECAR_EXTS:
            out.append(p)
        elif p.name.startswith(stem + ".") and p.name.endswith(".info.json"):
            out.append(p)
    return out


def _move_video(video: Path, target_dir: Path, stream: LogStreamer) -> bool:
    """Move a video and all of its sidecars to `target_dir`."""
    if video.parent == target_dir:
        return True
    target_dir.mkdir(parents=True, exist_ok=True)
    dst = target_dir / video.name
    if dst.exists():
        stream.emit_dim(f"  [skip] already exists at destination: {video.name}")
        return False
    sidecars = _sidecars_for(video)
    try:
        shutil.move(str(video), str(dst))
        for sc in sidecars:
            sc_dst = target_dir / sc.name
            if not sc_dst.exists():
                try:
                    shutil.move(str(sc), str(sc_dst))
                except OSError:
                    pass
        return True
    except OSError as e:
        stream.emit_error(f"Move failed for {video.name}: {e}")
        return False


def _cleanup_empty_dirs(root: Path):
    """Remove empty subdirectories under `root` (but not `root` itself)."""
    for dp, dns, fns in os.walk(root, topdown=False):
        p = Path(dp)
        if p == root:
            continue
        try:
            if not any(p.iterdir()):
                p.rmdir()
        except OSError:
            pass


# ── Public entry points ────────────────────────────────────────────────

def _date_from_info_json(video: Path) -> Optional[datetime]:
    """Read `.info.json` sidecar and pull the YouTube upload_date field."""
    candidates = [
        video.with_suffix("").with_suffix(".info.json"),
        video.parent / (video.stem + ".info.json"),
    ]
    for p in candidates:
        if p.is_file():
            try:
                import json as _json
                with p.open("r", encoding="utf-8") as f:
                    data = _json.load(f)
                raw = data.get("upload_date") or data.get("release_date") or ""
                if raw and len(raw) == 8 and raw.isdigit():
                    return datetime(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
            except (OSError, ValueError):
                pass
    return None


def fix_file_dates(channel_folder: str, stream: LogStreamer,
                   cancel_event: Optional[threading.Event] = None
                   ) -> Dict[str, Any]:
    """Walk the channel folder and update each video file's mtime to match
    its YouTube upload date (from the .info.json sidecar).

    Lighter-weight alternative to a full reorg — does not move files, only
    fixes dates. Useful when the user cares about date-sorted Recent view
    but doesn't want a year/month folder split.

    Returns {ok, updated, skipped, missing}.
    """
    root = Path(channel_folder)
    if not root.is_dir():
        return {"ok": False, "error": f"Folder not found: {channel_folder}"}

    stream.emit([["[Reorg] ", "reorg_bracket"],
                 [f"Recheck file dates for {root.name}\u2026\n", "simpleline"]])

    updated = 0
    skipped = 0
    missing = 0
    for p in root.rglob("*"):
        if cancel_event is not None and cancel_event.is_set():
            break
        if not p.is_file():
            continue
        if p.suffix.lower() not in (".mp4", ".mkv", ".webm", ".m4a", ".mov"):
            continue
        d = _date_from_info_json(p)
        if d is None:
            missing += 1
            continue
        target_ts = d.timestamp()
        try:
            cur = p.stat().st_mtime
            if abs(cur - target_ts) < 24 * 3600:
                skipped += 1
                continue
            os.utime(str(p), (target_ts, target_ts))
            updated += 1
        except OSError:
            missing += 1

    stream.emit([["[Reorg] ", "reorg_bracket"],
                 [f"Date fix complete: {updated} updated \u00b7 "
                  f"{skipped} already correct \u00b7 {missing} missing info\n",
                  "simpleline_green"]])
    return {"ok": True, "updated": updated, "skipped": skipped, "missing": missing}


def reorg_channel(channel_folder: str, split_years: bool, split_months: bool,
                  stream: LogStreamer,
                  cancel_event: Optional[threading.Event] = None,
                  use_mtime: bool = True,
                  recheck_dates: bool = False) -> Dict[str, Any]:
    """
    Move all videos in `channel_folder` into the requested layout.

    If split_years=False and split_months=False: flatten into channel root.
    If split_years=True, split_months=False: move into <root>/<year>/
    If split_months=True (implies years): move into <root>/<year>/<Month>/

    When `recheck_dates=True`, re-read the real upload date from each
    video's `.info.json` sidecar before deciding the target folder (and
    update the file's mtime to match). Slower but authoritative.
    """
    root = Path(channel_folder)
    if not root.is_dir():
        return {"ok": False, "error": "folder not found"}

    stream.emit([["[Reorg]  ", "reorg_bracket"],
                 [f"{root.name} \u2014 ", "simpleline_reorg"],
                 [f"years={split_years} months={split_months}"
                  f"{' (recheck dates)' if recheck_dates else ''}\n", "simpleline"]])

    videos = _gather_video_files(root)
    if not videos:
        stream.emit_dim("  (no video files found)")
        return {"ok": True, "moved": 0, "skipped": 0}

    moved = 0
    skipped = 0
    errors = 0
    redated = 0
    t0 = time.time()

    for i, video in enumerate(videos, 1):
        if cancel_event is not None and cancel_event.is_set():
            stream.emit_text("  \u26d4 Reorg cancelled.", "red")
            break
        # Determine target dir based on split flags
        d = None
        if recheck_dates:
            d = _date_from_info_json(video)
            if d is not None:
                # Also sync the file's mtime so future non-recheck runs are correct
                try:
                    ts_new = d.timestamp()
                    os.utime(video, (ts_new, ts_new))
                    redated += 1
                except OSError:
                    pass
        if d is None:
            try:
                ts = video.stat().st_mtime if use_mtime else time.time()
            except OSError:
                ts = time.time()
            d = datetime.fromtimestamp(ts)
        if split_months:
            # Use "01 January" format to match OLD's sync template.
            target = root / f"{d.year}" / _MONTH_FOLDERS[d.month]
        elif split_years:
            target = root / f"{d.year}"
        else:
            target = root

        if video.parent == target:
            skipped += 1
            continue
        if _move_video(video, target, stream):
            moved += 1
            if moved % 25 == 0:
                stream.emit_dim(f"  \u2014 {moved} moved so far...")
        else:
            errors += 1

    _cleanup_empty_dirs(root)

    took = time.time() - t0
    sec_bits = [f"{moved} moved \u00b7 {skipped} already in place"]
    if recheck_dates:
        sec_bits.append(f"{redated} dates fixed")
    sec_bits.append(f"{errors} errors \u00b7 took {took:.1f}s")
    stream.emit([["  \u2713 ", "simpleline_green"],
                 [f"Reorg done: ", "simpleline"],
                 [" \u00b7 ".join(sec_bits) + "\n", "simpleline_reorg"]])

    return {"ok": True, "moved": moved, "skipped": skipped, "errors": errors,
            "redated": redated, "took": took}
