"""
Reorg — move video files into year/month subfolders based on mtime.

Matches YTArchiver's reorg behavior: file modification time is treated as
the YouTube upload date (per feedback_ytarchiver_filedate.md memory). Files
get moved into <channel>/<year>/ if split_years, and
<channel>/<year>/<Month>/ if split_months.

Three modes:
  - reorg_none(channel_folder) flatten back to one folder
  - reorg_years(channel_folder) split into yyyy/
  - reorg_months(channel_folder) split into yyyy/MonthName/
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


_VIDEO_SIBLING_EXTS = (".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v",
                       ".wav", ".mp3", ".m4a", ".flac")


def _sidecars_for(video: Path) -> List[Path]:
    """Return any .txt/.jsonl/.info.json/.jpg files that share the video stem.

    audit D-8: match on EXACT stem equality (file stem == video stem),
    not prefix. Old code used `p.name.startswith(stem)`, which matched
    `X_Part2.txt` to video `X.mp4` → moving X.mp4 dragged Part2's
    transcripts along for the ride. Now we compare stems exactly,
    which also handles the dot-before-extension convention correctly.
    """
    stem = video.stem
    folder = video.parent
    out: List[Path] = []
    # audit M-2: language-suffixed caption files have a two-dot
    # extension (e.g. `X.en.vtt`, `X.es.vtt`). Path.stem on those
    # returns `X.en`, so the exact-stem-equality check misses
    # them. Maintain a small whitelist of known language codes so
    # compound suffixes attach to the video they belong to.
    _LANG_CODES = ("en", "es", "fr", "de", "ja", "ko", "pt", "it",
                   "ru", "zh", "zh-Hans", "zh-Hant", "ar", "hi",
                   "tr", "nl", "sv", "pl", "id", "vi", "th")
    for p in folder.iterdir():
        if not p.is_file():
            continue
        if p == video:
            continue
        # Exact stem match handles the common case:
        #   X.mp4 → X.txt, X.jsonl, X.info.json, X.jpg
        if p.stem == stem and p.suffix.lower() in _SIDECAR_EXTS:
            out.append(p)
            continue
        # Compound suffix exception: `.info.json` is two extensions so
        # Path.stem gives `X.info`, not `X`. Match via explicit prefix.
        if p.name == stem + ".info.json":
            out.append(p)
            continue
        # audit M-2: `X.en.vtt`, `X.es.srt`, etc. — Path.stem is
        # `X.en`, so pop the language suffix off and re-compare.
        _sub_exts = (".vtt", ".srt", ".ass", ".ttml")
        if p.suffix.lower() in _sub_exts:
            _outer_stem = p.stem  # e.g. "X.en"
            if "." in _outer_stem:
                _base, _lang = _outer_stem.rsplit(".", 1)
                if _base == stem and _lang in _LANG_CODES:
                    out.append(p)
    return out


def _has_video_sibling(video: Path) -> bool:
    """bug H-8 helper: True iff another video file with the SAME stem but
    a different media extension exists in the same folder. When two
    primaries share a stem (e.g. `X.mp4` and `X.mkv` — happens with
    aborted-then-retried yt-dlp downloads), moving one's sidecars
    silently orphans the other. In that case we COPY sidecars instead
    of moving, so both destinations keep their metadata."""
    stem = video.stem
    folder = video.parent
    try:
        for p in folder.iterdir():
            if not p.is_file() or p == video:
                continue
            if p.stem == stem and p.suffix.lower() in _VIDEO_SIBLING_EXTS:
                return True
    except OSError:
        pass
    return False


def _move_video(video: Path, target_dir: Path, stream: LogStreamer) -> bool:
    """Move a video and all of its sidecars to `target_dir`.

    bug H-8: if another video file shares this stem (e.g. `X.mp4` +
    `X.mkv`), copy shared sidecars instead of moving them — otherwise
    the sibling ends up orphaned without metadata.
    """
    if video.parent == target_dir:
        return True
    target_dir.mkdir(parents=True, exist_ok=True)
    dst = target_dir / video.name
    if dst.exists():
        # audit E-13: destination collision. Before, we silently left
        # the source in place AND kept going, which for a previously-
        # interrupted reorg produced duplicate files in both folders
        # with no dedupe. Now: if dst and src look identical (same
        # size + mtime within 2s), assume this is a resumed reorg and
        # remove the source as cleanup. If they differ, emit an error
        # so the user can investigate instead of silent leak.
        try:
            _s_stat = video.stat()
            _d_stat = dst.stat()
            _same = (_s_stat.st_size == _d_stat.st_size
                     and abs(_s_stat.st_mtime - _d_stat.st_mtime) <= 2.0)
        except OSError:
            _same = False
        if _same:
            try:
                video.unlink()
                stream.emit_dim(
                    f" [dedup] removed duplicate source: {video.name}")
                # Also move/remove sidecars so they don't linger.
                for _sc in _sidecars_for(video):
                    _sc_dst = target_dir / _sc.name
                    if _sc_dst.exists():
                        try: _sc.unlink()
                        except OSError: pass
                    else:
                        try: shutil.move(str(_sc), str(_sc_dst))
                        except OSError: pass
                return True
            except OSError as _ue:
                stream.emit_dim(
                    f" [skip] duplicate but couldn't remove source: "
                    f"{video.name} ({_ue})")
                return False
        else:
            stream.emit_error(
                f" [conflict] different file already at destination "
                f"for {video.name} \u2014 leaving both in place.")
            return False
    sidecars = _sidecars_for(video)
    has_sibling = _has_video_sibling(video)
    try:
        shutil.move(str(video), str(dst))
        # audit C-6: track sidecar failures so a partial orphan state
        # is visible instead of silently swallowed. Each failed
        # sidecar leaves metadata behind in the old folder; the user
        # needs a line in the log to know about it.
        _sc_failed: list[tuple[str, str]] = []
        for sc in sidecars:
            sc_dst = target_dir / sc.name
            if sc_dst.exists():
                continue
            try:
                if has_sibling:
                    # Shared sidecar — copy instead of move so the
                    # leftover primary still has it when it's processed
                    # in a later reorg pass.
                    shutil.copy2(str(sc), str(sc_dst))
                else:
                    shutil.move(str(sc), str(sc_dst))
            except OSError as _sce:
                _sc_failed.append((sc.name, str(_sce)))
        if _sc_failed:
            # Warn once per video with all failures. Non-fatal — the
            # primary move succeeded — but the user can investigate
            # (usually a file lock or permissions issue).
            _names = ", ".join(n for n, _ in _sc_failed)
            stream.emit_error(
                f"Sidecar move failed for {video.name}: {_names} "
                f"(video moved; metadata left in old folder)")
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

    # Em-dash prefix (reorg color) + white body — matches classic
    # simpleline_reorg painter output. No "[Reorg]" bracket tag.
    stream.emit([["  \u2014 ", "simpleline_reorg"],
                 [f"Recheck file dates for {root.name}\u2026\n", "simpleline"]])

    updated = 0
    skipped = 0
    missing = 0
    for p in root.rglob("*"):
        if cancel_event is not None and cancel_event.is_set():
            break
        if not p.is_file():
            continue
        # audit E-14: use the shared _VIDEO_EXTS constant so "Fix file
        # dates" handles the same file types as the full reorg walker.
        # Previously the hard-coded 5-ext list silently skipped
        # .avi / .flv / .wmv / .m4v / .wav / .mp3 / .flac archives,
        # leaving their mtimes wrong after a full reorg had already
        # moved them.
        if p.suffix.lower() not in _VIDEO_EXTS:
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

    stream.emit([["  \u2014 \u2713 ", "simpleline_green"],
                 [f"Date fix complete: {updated} updated \u00b7 "
                  f"{skipped} already correct \u00b7 {missing} missing info\n",
                  "simpleline"]])
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

    stream.emit([["  \u2014 ", "simpleline_reorg"],
                 [f"{root.name} ", "simpleline"],
                 ["\u2014 ", "simpleline_reorg"],
                 [f"years={split_years} months={split_months}"
                  f"{' (recheck dates)' if recheck_dates else ''}\n", "simpleline"]])

    videos = _gather_video_files(root)
    if not videos:
        stream.emit_dim(" (no video files found)")
        return {"ok": True, "moved": 0, "skipped": 0}

    moved = 0
    skipped = 0
    errors = 0
    redated = 0
    t0 = time.time()
    _ch_name = root.name

    # Sticky active status line — `[i/n] Reorganizing: ChannelName...`
    # pinned at the log's bottom. Mirrors classic's `mode="reorg"`
    # anim (YTArchiver.py:1977 _ANIM_MODES). `clear_line` control
    # drops the old line so each update lands at the current DOM
    # bottom instead of being replaced at the original position.
    import json as _json
    _total_videos = len(videos)
    def _emit_active(_i: int):
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "reorg_active"}),
             "__control__"],
        ])
        # Color discipline: only [ / ] + "Reorganizing:" render in the
        # reorg color; numbers + channel name stay white.
        stream.emit([
            ["[", ["reorg_bracket", "reorg_active"]],
            [str(_i), ["simpleline", "reorg_active"]],
            ["/", ["reorg_bracket", "reorg_active"]],
            [str(_total_videos), ["simpleline", "reorg_active"]],
            ["] ", ["reorg_bracket", "reorg_active"]],
            ["Reorganizing: ", ["reorg_bracket", "reorg_active"]],
            [f"{_ch_name}\u2026\n", ["simpleline", "reorg_active"]],
        ])

    def _clear_active():
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "reorg_active"}),
             "__control__"],
        ])

    for i, video in enumerate(videos, 1):
        if cancel_event is not None and cancel_event.is_set():
            stream.emit_text(" \u26d4 Reorg cancelled.", "red")
            break
        _emit_active(i)
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
            # `.get()` with fallback so a corrupt upload_date (month
            # outside 1-12) doesn't KeyError and abort the entire
            # reorg pass mid-run with files half-moved.
            target = root / f"{d.year}" / _MONTH_FOLDERS.get(
                d.month, "00 Unknown")
        elif split_years:
            target = root / f"{d.year}"
        else:
            target = root

        if video.parent == target:
            skipped += 1
            continue
        if _move_video(video, target, stream):
            moved += 1
            # audit F-28: re-stamp the moved file's mtime to the date
            # we chose for it. On StableBit DrivePool pooled drives a
            # cross-physical-drive move can reset mtime to "now", and
            # future reorg passes would then classify this file under
            # today's year instead of its upload year — silently
            # rotating files through folders.
            try:
                _moved_path = target / video.name
                ts_stamp = d.timestamp()
                os.utime(_moved_path, (ts_stamp, ts_stamp))
            except (OSError, ValueError):
                pass
            # Every 10 instead of 25 — on a ≤24-video reorg the old
            # threshold never fired, making the pass look stalled.
            if moved % 10 == 0:
                stream.emit_dim(f" \u2014 {moved} moved so far...")
        else:
            errors += 1

    # audit F-27: skip the empty-dirs sweep when the pass was
    # cancelled. Half-moved state with some files in year folders and
    # others still flat is a legitimate intermediate; sweeping
    # away the emptied source folders in that state would remove
    # useful structure the user might want to resume into.
    if cancel_event is not None and cancel_event.is_set():
        stream.emit_dim(" \u2014 cancel: skipping empty-folder cleanup.")
    else:
        _cleanup_empty_dirs(root)

    # Drop the sticky active-status line before the done-summary.
    _clear_active()

    took = time.time() - t0
    sec_bits = [f"{moved} moved \u00b7 {skipped} already in place"]
    if recheck_dates:
        sec_bits.append(f"{redated} dates fixed")
    sec_bits.append(f"{errors} errors \u00b7 took {took:.1f}s")
    stream.emit([["  \u2014 \u2713 ", "simpleline_green"],
                 ["Reorg done: ", "simpleline"],
                 [" \u00b7 ".join(sec_bits) + "\n", "simpleline"]])

    return {"ok": True, "moved": moved, "skipped": skipped, "errors": errors,
            "redated": redated, "took": took}
