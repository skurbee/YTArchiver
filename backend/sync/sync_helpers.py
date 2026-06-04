"""
sync.sync_helpers — small per-file / formatting helpers used by sync_channel.

Extracted from sync/core.py (Patch 14, v71.6). Pure helpers with no
mutable state — safe to import anywhere without circular concerns.

Public surface (re-exported via sync/__init__.py for back-compat):
    _hide_sidecar_win(video_path)
    _sweep_orphan_vtts(channel_folder) -> int
    _scan_recent_video(channel_dir) -> str | None
    _resolve_final_mp4(dest_path) -> str | None
    _resolve_path_for_vid(channel_dir, vid) -> str | None
    _fmt_duration(seconds) -> str
    _fmt_size(size_bytes) -> str
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

from .. import utils as _utils


# yt-dlp's per-track format-suffix intermediates: `video.f135.mp4`,
# `video.f140-16.m4a`, `video.f140-drc.m4a`. Used by `_scan_recent_video`
# to skip these when picking the most-recent merged output.
_F_SUFFIX_RE = re.compile(r"\.f\d+(?:-[A-Za-z0-9]+)?\.[A-Za-z0-9]+$")


def _hide_sidecar_win(video_path: str) -> None:
    """Set Windows HIDDEN attribute on a video's .info.json sidecar so
    Explorer shows only the video + Transcript.txt in archive folders."""
    if not video_path:
        return
    sidecar = os.path.splitext(video_path)[0] + ".info.json"
    if os.path.isfile(sidecar):
        _utils.hide_file_win(sidecar)


def _sweep_orphan_vtts(channel_folder: str, cancel_event=None) -> int:
    """Delete orphan `.vtt` / `.ttml` / `.srt` caption sidecars under a
    channel folder. Called after each sync pass — ensures the archive
    stays clean even when auto-transcribe is off or when the transcribe
    fast-path crashed mid-run.

    Accepts an optional cancel_event so a multi-TB archive cancel
    feels responsive (audit: sync/sync_helpers.py:51).
    """
    if not channel_folder or not os.path.isdir(channel_folder):
        return 0
    removed = 0
    exts = (".vtt", ".ttml", ".srt")
    for dp, _dns, fns in os.walk(channel_folder):
        if cancel_event is not None and cancel_event.is_set():
            break
        # Skip the hidden Thumbnails folder
        base = os.path.basename(dp)
        if base == ".Thumbnails" or base == ".ChannelArt":
            continue
        for fn in fns:
            if fn.lower().endswith(exts):
                try:
                    os.remove(os.path.join(dp, fn))
                    removed += 1
                except OSError:
                    pass
    return removed


def _scan_recent_video(channel_dir) -> str | None:
    """Last-resort fallback: scan a channel folder tree for the most
    recent video file (.mp4 / .mkv / .webm) created in the last 10 min.
    Mirrors YTArchiver.py:18350 — when Merger + Destination parsing both
    fail to hand us a valid path (obscure formats, unicode filename
    oddities, FixupM3u8 variants), we fall back to "what's the newest
    file on disk". Uses ctime on Windows because `--mtime` resets mtime
    to the upload date (often years old) which defeats the recency check.
    """
    try:
        channel_dir = str(channel_dir)
        if not channel_dir or not os.path.isdir(channel_dir):
            return None
        exts = (".mp4", ".mkv", ".webm")
        now = time.time()
        tkey = os.path.getctime if os.name == "nt" else os.path.getmtime
        best_path = None
        best_t = 0.0
        for dp, _dns, fns in os.walk(channel_dir):
            bn = os.path.basename(dp)
            if bn in (".Thumbnails", ".ChannelArt"):
                continue
            for fn in fns:
                if not fn.lower().endswith(exts):
                    continue
                # Skip yt-dlp format-suffix intermediates (e.g. `video.f135.mp4`,
                # `video.f140-drc.mp4`). DLTRACK can fire before the merger
                # deletes these — scan would otherwise return the intermediate
                # as the "most recent video" and transcribe would later fail
                # with "file not found" when the merge cleanup runs.
                if _F_SUFFIX_RE.search(fn):
                    continue
                fp = os.path.join(dp, fn)
                try:
                    t = tkey(fp)
                except OSError:
                    continue
                if (now - t) > 600:
                    continue
                if t > best_t:
                    best_t = t
                    best_path = fp
        return best_path
    except Exception:
        return None


def _resolve_final_mp4(dest_path: str) -> str | None:
    """yt-dlp's 'Destination:' line shows intermediate paths too (video.f137.mp4,
    video.en.vtt, video.en-orig.vtt, video.description, etc.). Return the
    final merged .mp4 path ONLY when the destination is a real video track —
    otherwise return None so the caller skips transcribe enqueue + recent
    recording for captions/metadata sidecars.
    """
    p = Path(dest_path)
    ext = p.suffix.lower()
    # Skip non-video destinations — captions, descriptions, info.json, etc.
    if ext not in (".mp4", ".mkv", ".webm", ".m4a", ".mp3", ".flac",
                   ".wav", ".opus", ".ogg"):
        return None
    stem = p.stem
    if "." in stem:
        parts = stem.split(".")
        last = parts[-1]
        # Strip yt-dlp's format selector suffix — `.fNNN` (e.g. `.f137`) for
        # simple single-track formats, OR `.fNNN-X` / `.fNNN-drc` / `.fNNN-1`
        # etc. when the source has multiple audio tracks / DRC variants.
        # Pattern: `f` followed by a digit, then anything (or nothing).
        # Earlier version used `last[1:].isdigit()` which failed on
        # `f140-16` because `.isdigit()` rejects the dash → downloaded
        # counter never incremented for any channel with multi-track
        # audio (observed on bodycam / multi-language content).
        if (len(last) >= 2 and last[0] == "f" and last[1].isdigit()):
            stem = ".".join(parts[:-1])
        # Strip language codes left from `--write-subs` (e.g. `.en`, `.en-orig`,
        # `.en-us`). These don't appear on merged video outputs but defensive
        # handling prevents future caption-related regressions.
        elif last.lower() in ("en", "en-orig", "en-us", "en-gb",
                               "en-uk", "es", "fr", "de", "pt", "it"):
            stem = ".".join(parts[:-1])
    # preserve the ORIGINAL container extension when it was
    # already a known video format. Hardcoding .mp4 broke DLTRACK path
    # resolution when yt-dlp merged to .mkv / .webm (happens when the
    # selected codec combo can't mux into mp4). Recent tab / Browse
    # grid would then point at a non-existent .mp4 and fall back to
    # scan-recent which may pick up the wrong file.
    _video_container_exts = (".mp4", ".mkv", ".webm")
    _target_ext = ext if ext in _video_container_exts else ".mp4"
    final = p.parent / f"{stem}{_target_ext}"
    # Return regardless of existence — file may still be writing when we enqueue
    return str(final)


# Media containers a merged yt-dlp download can land in.
_RESOLVE_MEDIA_EXTS = (".mp4", ".mkv", ".webm")


def _resolve_path_for_vid(channel_dir, vid: str) -> str | None:
    """GUARANTEED YouTube-id → file binding for a freshly-downloaded video.

    The DLTRACK line ALWAYS carries the authoritative YouTube id, but the
    normal path resolution (Merger line → Destination strip → recent-file
    scan) can miss on unicode / format / FixupM3u8 oddities — the "DLTRACK
    orphan". When that happens the old code silently dropped a known id and
    let the disk sweep re-register the file later with no id at all.

    This closes the gap. Because the sync ALWAYS runs yt-dlp with
    `--write-info-json`, yt-dlp has — milliseconds before DLTRACK fires —
    written a `<base>.info.json` next to the merged media file, and that
    JSON's `"id"` field is the authoritative YouTube id (the same id that
    is in the DLTRACK line). We find the `.info.json` whose id == vid and
    return the co-located media file. Newest-first ordering makes the
    just-downloaded orphan's sidecar the very first one we parse, so the
    common case is effectively O(1) even in a multi-thousand-video folder.

    Returns the media file path, or None if no matching sidecar/file is
    found (genuinely pathological — caller surfaces that loudly).
    """
    try:
        channel_dir = str(channel_dir)
        vid = (vid or "").strip()
        if not vid or not channel_dir or not os.path.isdir(channel_dir):
            return None
        cands = []
        for dp, _dns, fns in os.walk(channel_dir):
            bn = os.path.basename(dp)
            if bn in (".Thumbnails", ".ChannelArt"):
                continue
            for fn in fns:
                if fn.endswith(".info.json"):
                    cands.append(os.path.join(dp, fn))
        # Newest sidecar first — a just-downloaded orphan sorts to the top,
        # so we match on the first parse instead of walking thousands.
        try:
            cands.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        except OSError:
            pass
        for jpath in cands:
            try:
                with open(jpath, "r", encoding="utf-8") as jf:
                    jd = json.load(jf) or {}
            except Exception:
                continue
            if str(jd.get("id") or "").strip() != vid:
                continue
            dp = os.path.dirname(jpath)
            base = os.path.basename(jpath)[: -len(".info.json")]
            # 1) Co-located media file sharing the sidecar's base name —
            #    yt-dlp writes `<base>.info.json` next to `<base>.<ext>`.
            for ext in _RESOLVE_MEDIA_EXTS:
                cand = os.path.join(dp, base + ext)
                if os.path.isfile(cand):
                    return cand
            # 2) The JSON's own recorded final path, if the media got
            #    renamed away from the sidecar base by a post-processor.
            for key in ("_filename", "filename"):
                rec = jd.get(key)
                if rec and os.path.isfile(str(rec)):
                    return str(rec)
            for rd in (jd.get("requested_downloads") or []):
                rec = rd.get("filepath") or rd.get("_filename")
                if rec and os.path.isfile(str(rec)):
                    return str(rec)
        return None
    except Exception:
        return None


def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    hours = seconds // 3600
    rem = seconds - hours * 3600
    return f"{hours}h {rem // 60}m"


def _fmt_size(size_bytes) -> str:
    """Human-readable byte size. Mirrors YTArchiver.py — used in the
    "— ✓ Title — Channel (NN MB)" download confirmation line."""
    try:
        n = int(size_bytes)
    except (TypeError, ValueError):
        return ""
    if n <= 0:
        return ""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n:.1f} PB"
