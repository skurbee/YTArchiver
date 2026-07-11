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
import re
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .log import get_logger
from .log_stream import LogStreamer

_log = get_logger(__name__)

from .fs_search import MEDIA_EXTS_TUPLE as _VIDEO_EXTS  # unified media set

_SIDECAR_EXTS = (".txt", ".jsonl", ".info.json", ".jpg", ".jpeg", ".png", ".webp",
                 ".vtt", ".srt", ".description")
_CAPTION_LANG_RE = re.compile(
    r"^(?:[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*|orig)$",
    re.IGNORECASE,
)


# Must match YTArchiver.py:7483 MONTH_NAMES exactly — OLD's sync writes
# "01 January", "02 February", etc. and reorg must produce the same names
# so the two apps never create sibling folders for the same month.
# Shared with metadata.py + transcribe.py — see backend.utils.MONTH_FOLDERS.
from .utils import MONTH_FOLDERS as _MONTH_FOLDERS
from .utils import sampled_files_equal

_REORG_SKIP_DIRS = ("_TEMP_COMPRESS", "_BACKLOG_TEMP", "_REDOWNLOAD_TEMP")


def _gather_video_files(root: Path) -> list[Path]:
    """Walk `root` for video-type files (skipping our temp files +
    temp working dirs).

    Old code filtered by filename only ("_TEMP_COMPRESS" in fn),
    but the walk still descended INTO `*_TEMP_COMPRESS/` directories
    and collected videos inside — letting a concurrent reorg pull
    an active compress's temp video out from under it (audit:
    reorg.py:51-54). Skip those dirs entirely via the `dns[:]`
    mutation pattern.
    """
    out: list[Path] = []
    for dp, dns, fns in os.walk(root):
        # In-place mutation of dns prunes the walk before descent.
        dns[:] = [d for d in dns if d not in _REORG_SKIP_DIRS]
        for fn in fns:
            if "_TEMP_COMPRESS" in fn or fn.endswith(".part"):
                continue
            if fn.lower().endswith(_VIDEO_EXTS):
                out.append(Path(dp) / fn)
    return out


_VIDEO_SIBLING_EXTS = (".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v",
                       ".wav", ".mp3", ".m4a", ".flac")


def _sidecars_for(video: Path) -> list[Path]:
    """Return any .txt/.jsonl/.info.json/.jpg files that share the video stem.

    match on EXACT stem equality (file stem == video stem),
    not prefix. Old code used `p.name.startswith(stem)`, which matched
    `X_Part2.txt` to video `X.mp4` → moving X.mp4 dragged Part2's
    transcripts along for the ride. Now we compare stems exactly,
    which also handles the dot-before-extension convention correctly.
    """
    stem = video.stem
    folder = video.parent
    out: list[Path] = []
    # Language-suffixed caption files have a two-dot extension
    # (`X.en.vtt`, `X.es-419.srt`, `X.en-orig.vtt`). Path.stem on
    # those returns `X.en`, so the exact-stem-equality check misses
    # them. Match by structure instead of a fixed language whitelist.
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
        # `X.en.vtt`, `X.es.srt`, etc. — Path.stem is
        # `X.en`, so pop the language suffix off and re-compare.
        _sub_exts = (".vtt", ".srt", ".ass", ".ttml")
        if p.suffix.lower() in _sub_exts:
            _outer_stem = p.stem  # e.g. "X.en"
            if "." in _outer_stem:
                _base, _lang = _outer_stem.rsplit(".", 1)
                if _base == stem and _CAPTION_LANG_RE.match(_lang):
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


def _move_video(video: Path, target_dir: Path, stream: LogStreamer,
                dry_run: bool = False) -> bool:
    """Move a video and all of its sidecars to `target_dir`.

    if another video file shares this stem (e.g. `X.mp4` +
    `X.mkv`), copy shared sidecars instead of moving them — otherwise
    the sibling ends up orphaned without metadata.

    `dry_run=True` logs the intended move and sidecar handling without
    actually touching the filesystem. Returns True (success signal) so
    the caller's counters still tick over.
    """
    if video.parent == target_dir:
        return True
    if dry_run:
        scs = _sidecars_for(video)
        sc_count = len(scs)
        stream.emit([
            ["[dry-run] ", ["dim"]],
            [f"would move {video.name} ", None],
            [f"→ {target_dir.relative_to(target_dir.parent.parent) if target_dir.parent.parent in target_dir.parents else target_dir.name}/ ",
             ["dim"]],
            [f"(+{sc_count} sidecar{'s' if sc_count != 1 else ''})\n", ["dim"]],
        ])
        return True
    target_dir.mkdir(parents=True, exist_ok=True)
    dst = target_dir / video.name
    if dst.exists():
        # destination collision. Before, we silently left
        # the source in place AND kept going, which for a previously-
        # interrupted reorg produced duplicate files in both folders
        # with no dedupe. Now: if dst and src look identical
        # (same size + mtime within 2s AND first-MB + last-MB byte
        # content matches), assume this is a resumed reorg and remove
        # the source as cleanup. If they differ, emit an error so the
        # user can investigate instead of silent leak.
        try:
            _s_stat = video.stat()
            _d_stat = dst.stat()
            _same_meta = (_s_stat.st_size == _d_stat.st_size
                          and abs(_s_stat.st_mtime - _d_stat.st_mtime) <= 2.0)
        except OSError:
            _same_meta = False
        _same = False
        if _same_meta:
            # Content-sample compare (size + head/mid/tail 1MB windows) before
            # treating dst as a duplicate and moving/removing the source.
            # Shared helper so this delete path uses the SAME 3-window check as
            # redownload's replace path \u2014 reorg previously had only head+tail
            # (audit: sampled_files_equal).
            _same = sampled_files_equal(str(video), str(dst))
        if _same:
            # Handle sidecars FIRST (move or remove). The source video
            # is deleted LAST so that any sidecar failure mid-loop
            # leaves the source video in place (orphaned but
            # recoverable) instead of deleted while half its sidecars
            # are stranded at the source path.
            _sc_err = None
            for _sc in _sidecars_for(video):
                _sc_dst = target_dir / _sc.name
                try:
                    if _sc_dst.exists():
                        _sc.unlink()
                    else:
                        shutil.move(str(_sc), str(_sc_dst))
                except OSError as _se:
                    _sc_err = _se
                    break
            if _sc_err is not None:
                stream.emit_dim(
                    f" [skip] duplicate but sidecar handling failed: "
                    f"{video.name} ({_sc_err})")
                return False
            try:
                video.unlink()
                stream.emit_dim(
                    f" [dedup] removed duplicate source: {video.name}")
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
    # Refuse cross-volume reorg moves. shutil.move falls back to
    # copy+delete across volumes, which is NOT atomic — a cancel or
    # crash between the copy and the delete leaves the file in BOTH
    # locations with no automatic cleanup. On Z:\ DrivePool this can
    # happen if the pool reassigns the physical drive mid-pool.
    # Better to refuse and let the user move manually than risk a
    # silent duplicate.
    try:
        if os.name == "nt":
            _src_drv, _ = os.path.splitdrive(os.path.abspath(str(video)))
            _dst_drv, _ = os.path.splitdrive(os.path.abspath(str(dst)))
            _same_vol = _src_drv.lower() == _dst_drv.lower()
        else:
            _same_vol = (os.stat(str(video)).st_dev ==
                         os.stat(str(target_dir)).st_dev)
    except OSError:
        _same_vol = False
    if not _same_vol:
        stream.emit_error(
            f" [skip] {video.name}: source and destination on different "
            f"volumes — move manually to avoid a non-atomic copy+delete.")
        return False
    try:
        # Move sidecars FIRST, then the video. Reversed from prior
        # order so a sidecar failure leaves the video in the source
        # folder (recoverable: next reorg pass retries cleanly)
        # rather than at the destination with stranded sidecars in
        # the source (audit: reorg H125). Shared sidecars are still
        # copied (not moved) because the source video sticks around
        # for the same-folder sibling.
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
                # Re-apply the Windows hidden attribute if the
                # original sidecar was hidden. shutil.move across
                # volumes (copy+delete) does NOT preserve the
                # hidden attribute on Windows, exposing
                # `.{ch} Metadata.jsonl`, `.{stem}.jsonl`,
                # `.Thumbnails/` etc. in Explorer at the new
                # location. Same-volume rename usually preserves it,
                # but apply unconditionally for safety — hide is a
                # no-op when already hidden.
                if sc.name.startswith(".") or "Thumbnails" in sc.name:
                    try:
                        from .utils import hide_file_win
                        hide_file_win(str(sc_dst))
                    except Exception as _he:
                        _log.debug("swallowed: %s", _he)
            except OSError as _sce:
                _sc_failed.append((sc.name, str(_sce)))
        if _sc_failed:
            # Sidecar move(s) failed — abort the video move so we
            # don't end up with a dest-side video missing its
            # sidecars. The partial sidecars at the destination
            # will be cleaned up on the next reorg pass when both
            # the video AND its remaining sidecars try to move
            # together again (audit: reorg H125).
            _names = ", ".join(n for n, _ in _sc_failed)
            stream.emit_error(
                f" [skip] {video.name}: sidecar move failed "
                f"({_names}) — video left in source folder for retry")
            return False
        # Sidecars are at the destination — now move the video.
        shutil.move(str(video), str(dst))
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

def _flat_aggregate_target(root: Path, ch_name: str, p: Path) -> Path | None:
    name = p.name
    base = re.escape(ch_name)
    bucket = r"(?:\d{4}|[A-Za-z]+ \d{2})"
    if re.fullmatch(rf"{base} {bucket} Transcript\.txt", name):
        return root / f"{ch_name} Transcript.txt"
    if re.fullmatch(rf"\.{base} {bucket} Transcript\.jsonl", name):
        return root / f".{ch_name} Transcript.jsonl"
    if re.fullmatch(rf"\.{base} {bucket} Metadata\.jsonl", name):
        return root / f".{ch_name} Metadata.jsonl"
    return None


def _relocate_flat_aggregate_files(root: Path, ch_name: str,
                                   stream: LogStreamer,
                                   dry_run: bool = False) -> int:
    moved = 0
    for dp, dns, fns in os.walk(root):
        p_dir = Path(dp)
        if p_dir == root:
            continue
        dns[:] = [d for d in dns if d not in _REORG_SKIP_DIRS]
        for fn in fns:
            src = p_dir / fn
            target = _flat_aggregate_target(root, ch_name, src)
            if target is None:
                continue
            if target.exists():
                stream.emit_dim(
                    f" [skip] aggregate file already exists at root: "
                    f"{target.name}; left {src.name} in place.")
                continue
            if dry_run:
                stream.emit_dim(
                    f" [dry-run] would move aggregate {src.name} "
                    f"to {target.name}.")
                moved += 1
                continue
            try:
                shutil.move(str(src), str(target))
                if target.name.startswith("."):
                    try:
                        from .utils import hide_file_win
                        hide_file_win(str(target))
                    except Exception as _he:
                        _log.debug("swallowed: %s", _he)
                moved += 1
            except OSError as e:
                stream.emit_error(
                    f"Move failed for aggregate file {src.name}: {e}")
    return moved


def _date_from_info_json(video: Path) -> datetime | None:
    """Read `.info.json` sidecar and pull the YouTube upload_date field."""
    candidates = [
        # Single, exact form. The old first candidate
        # video.with_suffix("").with_suffix(".info.json") mangled
        # dotted titles ('Vol. 2' → 'Vol.info.json') and — checked
        # FIRST — could silently read a DIFFERENT video's upload date
        # when the mangled name collided with a shorter-titled
        # sibling. For dot-free stems it was byte-identical to this
        # one anyway.
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
                   cancel_event: threading.Event | None = None
                   ) -> dict[str, Any]:
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
        # use the shared _VIDEO_EXTS constant so "Fix file
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
            # Verbose-only: log every video that couldn't be dated.
            stream.emit([
                ["    — ", ["dim"]],
                [f"{p.name} · no info.json sidecar (skipped)\n",
                 ["dim"]],
            ])
            continue
        target_ts = d.timestamp()
        try:
            cur = p.stat().st_mtime
            if abs(cur - target_ts) < 24 * 3600:
                skipped += 1
                stream.emit([
                    ["    — ", ["dim"]],
                    [f"{p.name} · mtime already matches "
                     f"{d.strftime('%Y-%m-%d')} (no change)\n", ["dim"]],
                ])
                continue
            os.utime(str(p), (target_ts, target_ts))
            updated += 1
            # Verbose-only: per-video before/after.
            from datetime import datetime as _dt
            _cur_str = _dt.fromtimestamp(cur).strftime('%Y-%m-%d')
            _new_str = d.strftime('%Y-%m-%d')
            stream.emit([
                ["    — ", ["dim"]],
                [f"{p.name} · mtime {_cur_str} → {_new_str}\n",
                 ["dim"]],
            ])
        except OSError as _oe:
            missing += 1
            stream.emit([
                ["    — ", ["dim"]],
                [f"{p.name} · OSError: {_oe}\n", ["dim"]],
            ])

    _was_cancelled = bool(cancel_event is not None and cancel_event.is_set())
    if _was_cancelled:
        # Honest summary \u2014 the old unconditional "complete" line made a
        # cancelled pass look finished. Files stamped so far are kept;
        # re-running resumes (already-correct files are skipped cheaply).
        stream.emit([["  \u26d4 ", "red"],
                     [f"Date fix cancelled: {updated} updated \u00b7 "
                      f"{skipped} already correct \u00b7 {missing} missing info "
                      f"\u2014 re-run to finish the rest.\n",
                      "simpleline"]])
    else:
        stream.emit([["  \u2014 \u2713 ", "simpleline_green"],
                     [f"Date fix complete: {updated} updated \u00b7 "
                      f"{skipped} already correct \u00b7 {missing} missing info\n",
                      "simpleline"]])
    return {"ok": True, "updated": updated, "skipped": skipped,
            "missing": missing, "cancelled": _was_cancelled}


def reorg_channel(channel_folder: str, split_years: bool, split_months: bool,
                  stream: LogStreamer,
                  cancel_event: threading.Event | None = None,
                  use_mtime: bool = True,
                  recheck_dates: bool = False,
                  dry_run: bool = False) -> dict[str, Any]:
    """
    Move all videos in `channel_folder` into the requested layout.

    If split_years=False and split_months=False: flatten into channel root.
    If split_years=True, split_months=False: move into <root>/<year>/
    If split_months=True (implies years): move into <root>/<year>/<Month>/

    When `recheck_dates=True`, re-read the real upload date from each
    video's `.info.json` sidecar before deciding the target folder (and
    update the file's mtime to match). Slower but authoritative.

    `dry_run=True` logs every intended move + the final empty-dir
    cleanup without touching the filesystem. The returned `moved` count
    reflects how many moves WOULD have happened.
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
    aggregate_moved = 0
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
        _date_source = ""
        if recheck_dates:
            d = _date_from_info_json(video)
            if d is not None:
                _date_source = "info.json sidecar"
                # Also sync the file's mtime so future non-recheck runs are correct
                try:
                    ts_new = d.timestamp()
                    os.utime(video, (ts_new, ts_new))
                    redated += 1
                except OSError as e:
                    stream.emit_text(
                        f" ⚠ couldn't stamp upload-date mtime on "
                        f"{video.name}; future mtime-based reorg may "
                        f"misplace it ({e}).\n",
                        "red",
                    )
        if d is None:
            try:
                ts = video.stat().st_mtime if use_mtime else time.time()
                _date_source = ("file mtime" if use_mtime
                                else "current time (fallback)")
            except OSError:
                ts = time.time()
                _date_source = "current time (stat failed)"
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

        # VERBOSE-ONLY per-video decision trace. Lets Verbose users
        # see exactly which date source drove each move and where each
        # file is being routed. Suppressed in Simple mode (dim tag).
        try:
            _rel_src = video.relative_to(root)
        except ValueError:
            _rel_src = video.name
        try:
            _rel_tgt = target.relative_to(root)
        except ValueError:
            _rel_tgt = target.name
        stream.emit([
            ["    \u2014 ", ["dim"]],
            [f"{video.name} \u00b7 date {d.strftime('%Y-%m-%d')} "
             f"(from {_date_source}) \u00b7 "
             f"{_rel_src} \u2192 {_rel_tgt or '<root>'}\n", ["dim"]],
        ])

        if video.parent == target:
            skipped += 1
            continue
        if _move_video(video, target, stream, dry_run=dry_run):
            moved += 1
            # re-stamp the moved file's mtime to the date
            # we chose for it. On StableBit DrivePool pooled drives a
            # cross-physical-drive move can reset mtime to "now", and
            # future reorg passes would then classify this file under
            # today's year instead of its upload year — silently
            # rotating files through folders.
            if not dry_run:
                try:
                    _moved_path = target / video.name
                    ts_stamp = d.timestamp()
                    os.utime(_moved_path, (ts_stamp, ts_stamp))
                    # Re-stamp any sidecars that moved alongside the
                    # video. Cross-pool moves (DrivePool boundary) can
                    # reset sidecar mtimes to "now", and any future
                    # mtime-based logic (drift detection, "what's
                    # changed since last sync" probes) would then
                    # treat the sidecars as freshly-modified.
                    for _sc in _sidecars_for(_moved_path):
                        try:
                            os.utime(_sc, (ts_stamp, ts_stamp))
                        except OSError as e:
                            stream.emit_dim(
                                f" (couldn't stamp sidecar mtime on "
                                f"{_sc.name}: {e})")
                except OSError as e:
                    stream.emit_text(
                        f" ⚠ couldn't stamp upload-date mtime on "
                        f"{video.name}; future mtime-based reorg may "
                        f"misplace it ({e}).\n",
                        "red",
                    )
                except ValueError as e:
                    stream.emit_dim(
                        f" (couldn't compute upload-date mtime for "
                        f"{video.name}: {e})")
                # Re-point the index at the moved file so Watch playback +
                # the Browse grid keep finding it. Without this, reorg
                # physically moves the .mp4 but videos.filepath still points
                # at the OLD folder -> relocated videos showed "File not
                # found" (the transcript still loaded, since segments are
                # keyed by video_id, not path).
                try:
                    from . import index as _idx
                    _repointed = _idx.update_video_path(
                        str(video), str(target / video.name))
                    if not _repointed:
                        msg = ("moved on disk but catalog re-point matched "
                               f"0 rows: {video.name}")
                        _log.warning(msg)
                        stream.emit_dim(f" ({msg}; rescan may be needed)")
                except Exception as _ie:
                    _log.warning("catalog re-point failed for %s: %s",
                                 video, _ie)
            # Every 10 instead of 25 — on a ≤24-video reorg the old
            # threshold never fired, making the pass look stalled.
            if moved % 10 == 0:
                stream.emit_dim(f" \u2014 {moved} moved so far...")
        else:
            errors += 1

    # skip the empty-dirs sweep when the pass was
    # cancelled. Half-moved state with some files in year folders and
    # others still flat is a legitimate intermediate; sweeping
    # away the emptied source folders in that state would remove
    # useful structure the user might want to resume into.
    if cancel_event is not None and cancel_event.is_set():
        stream.emit_dim(" \u2014 cancel: skipping empty-folder cleanup.")
    elif dry_run:
        if not split_years and not split_months:
            aggregate_moved = _relocate_flat_aggregate_files(
                root, _ch_name, stream, dry_run=True)
        stream.emit_dim(" \u2014 [dry-run] would sweep empty folders under "
                        f"{root.name}/")
    else:
        if not split_years and not split_months:
            aggregate_moved = _relocate_flat_aggregate_files(
                root, _ch_name, stream, dry_run=False)
        _cleanup_empty_dirs(root)

    # Drop the sticky active-status line before the done-summary.
    _clear_active()

    took = time.time() - t0
    sec_bits = [f"{moved} moved \u00b7 {skipped} already in place"]
    if aggregate_moved:
        sec_bits.append(f"{aggregate_moved} aggregate files moved")
    if recheck_dates:
        sec_bits.append(f"{redated} dates fixed")
    sec_bits.append(f"{errors} errors \u00b7 took {took:.1f}s")
    stream.emit([["  \u2014 \u2713 ", "simpleline_green"],
                 ["Reorg done: ", "simpleline"],
                 [" \u00b7 ".join(sec_bits) + "\n", "simpleline"]])

    return {"ok": True, "moved": moved, "skipped": skipped, "errors": errors,
            "redated": redated, "aggregate_moved": aggregate_moved,
            "took": took}
