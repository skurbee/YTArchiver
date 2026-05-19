"""
Thumbnails — fetch + on-disk layout for the per-channel `.Thumbnails/`
sidecar folders.

Extracted from `metadata.py` in Patch 6 (2026-05-17) so the thumbnail
download + atomic-write + status-cache code lives in one focused module
instead of being scattered across the 4400-line metadata file.

Public API (used by metadata.py — internal underscore-prefixed names
preserved so existing call sites work via re-export):

    _ensure_thumbnails_dir(subfolder) -> str
        Create + hide `.Thumbnails/` under subfolder.

    _download_thumbnail(url, thumb_dir, title, video_id, stream=None)
        Atomic fetch of one thumbnail. Magic-byte validates JPEG/PNG/WEBP
        before commit. Emits a dim diagnostic on failure if `stream`
        provided. No-op if file already exists.

    _thumbnail_exists_for(thumb_dir, video_id) -> bool
        Cheap check: does any *.jpg/jpeg/png/webp in thumb_dir contain
        `[video_id]` in its filename?

    _thumb_cache_path() -> str
    _load_thumb_cache() -> {channel_lower: {fingerprint, total, ...}}
    _save_thumb_cache(cache)
        Persisted status-cache for the Settings > Metadata page so
        opening the tab doesn't trigger a fresh disk walk every time.

    _channel_fingerprint(folder) -> float
        Max mtime across the channel folder + one level of subdirs. Used
        by the cache to detect when a channel has new content.

Status-counting helpers (sweep_missing_thumbnails, realign_misplaced_
thumbnails, count_thumbnail_status_bulk) stayed in metadata.py because
they depend heavily on metadata.py internals like _folder_for_channel,
_scan_channel_videos, and _get_metadata_jsonl_path.
"""

from __future__ import annotations

import json
import os
import re
import urllib.request
from pathlib import Path
from typing import Any

from .log import get_logger
from .utils import hide_file_win as _hide_file_win

_log = get_logger(__name__)


def _ensure_thumbnails_dir(subfolder: str) -> str:
    """Create .Thumbnails/ inside subfolder, hide it on Windows, return the path."""
    thumb_dir = os.path.join(subfolder, ".Thumbnails")
    try:
        os.makedirs(thumb_dir, exist_ok=True)
    except OSError:
        return thumb_dir
    _hide_file_win(os.path.normpath(thumb_dir))
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
    # a duplicate. rename only if the existing file is recent
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

    # atomic write via .tmp + os.replace. Interrupt or crash
    # during write used to leave a 0-byte .jpg at the target path.
    # Because the next run sees isfile=True and skips, the broken image
    # gets cached permanently. Also validate JPEG magic bytes before
    # committing so a truncated HTML error page doesn't masquerade as
    # a thumbnail. cap read at 20 MB — YouTube thumbs are
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
        # Patch fix (v68.4): debug-level log of the exact path written.
        # Helps diagnose "Recent card shows gradient placeholder" when
        # the thumbnail IS on disk somewhere but find_thumbnail's
        # search path doesn't reach it.
        _log.debug("thumbnail written: %s", fpath)
    except Exception as _te:
        # Non-fatal, but no longer invisible: emit a verbose-only
        # diagnostic so the user can see WHY a Browse thumbnail is
        # missing (404, timeout, disk-write failure, etc.) instead
        # of just seeing a placeholder with no hint.
        if stream is not None:
            try:
                stream.emit([
                    [" ⚠ Thumbnail fetch failed ", "dim"],
                    [f"[{video_id}]: {_te}\n", "dim"],
                ])
            except Exception as e:
                _log.debug("swallowed: %s", e)


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


# ── Status-cache (used by Settings > Metadata page) ─────────────────────

def _thumb_cache_path() -> str:
    """Path to the persisted thumbnail-coverage cache."""
    from .ytarchiver_config import APP_DATA_DIR
    return os.path.join(str(APP_DATA_DIR), "thumbnail_status_cache.json")


def _load_thumb_cache() -> dict[str, dict[str, Any]]:
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


def _save_thumb_cache(cache: dict[str, dict[str, Any]]) -> None:
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
