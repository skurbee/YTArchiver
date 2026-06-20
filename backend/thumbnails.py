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


def _thumbnail_url_candidates(url: str, video_id: str) -> list[str]:
    """Return thumbnail URLs to try, from best/original to safe fallbacks."""
    candidates: list[str] = []

    def _add(candidate: str) -> None:
        candidate = (candidate or "").strip()
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    _add(url)
    if video_id and "ytimg.com/" in (url or ""):
        # YouTube often omits maxresdefault for a video while still
        # serving hqdefault/mqdefault, so try those before giving up.
        base_webp = f"https://i.ytimg.com/vi_webp/{video_id}"
        base_jpg = f"https://i.ytimg.com/vi/{video_id}"
        for quality in ("maxresdefault", "sddefault",
                        "hqdefault", "mqdefault"):
            _add(f"{base_webp}/{quality}.webp")
            _add(f"{base_jpg}/{quality}.jpg")
    return candidates


def _download_thumbnail(url: str, thumb_dir: str,
                        title: str, video_id: str,
                        stream=None) -> bool:
    """Download a thumbnail to `{thumb_dir}/{safe_title} [{video_id}].jpg`.
    Dedupes against an existing file with the same [{video_id}] bracket.
    Matches YTArchiver.py:26784 exactly.

    `stream` (optional) — if provided, emits a verbose-only dim
    diagnostic line on fetch failure. Without this, a missing
    thumbnail in Browse view was impossible to diagnose because
    the exception was silently swallowed.
    """
    if not url or not video_id:
        return False
    # Also strip control chars (incl. NUL) and trim trailing dots /
    # spaces so the resulting filename is valid on NTFS — bare
    # `[<>:"/\\|?*]` substitution missed those classes (audit:
    # thumbnails H84).
    _src_title = title or ""
    _safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', _src_title)
    _safe = _safe.rstrip(". ")[:100] or "untitled"
    safe_title = _safe
    fname = f"{safe_title} [{video_id}].jpg"
    fpath = os.path.join(thumb_dir, fname)
    if os.path.isfile(fpath):
        return True

    # Dedup: if a thumb with this [{video_id}] already exists under a
    # different title (YT renamed the video), rename it instead of writing
    # a duplicate. rename only if the existing file is recent
    # (<30 days); otherwise fall through to re-download so a stale thumb
    # from years ago gets refreshed with the current YouTube URL.
    # The stale variant is deleted only AFTER the replacement download
    # commits — deleting up front destroyed the only surviving copy
    # whenever the fetch failed, and for removed/delisted videos the
    # cached thumbnail_url 404s forever, making that loss deterministic.
    _stale_old_thumb = None
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
                    if _is_recent:
                        existing_ext = os.path.splitext(existing)[1]
                        new_fname = f"{safe_title} [{video_id}]{existing_ext}"
                        new_path = os.path.join(thumb_dir, new_fname)
                        try:
                            os.replace(existing_path, new_path)
                            return True
                        except OSError:
                            pass
                    else:
                        # Stale (>30d): leave it untouched on disk and
                        # fall through to re-download. The success path
                        # below removes it once the new file is
                        # committed — otherwise _thumbnail_exists_for
                        # would report True for both files and Browse
                        # picked arbitrarily (audit: thumbnails.py:
                        # 110-118).
                        _stale_old_thumb = existing_path
                        break
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
        _MAX_BYTES = 20 * 1024 * 1024
        img_data = None
        _last_error: Exception | None = None
        for candidate_url in _thumbnail_url_candidates(url, video_id):
            try:
                req = urllib.request.Request(
                    candidate_url, headers={"User-Agent": "Mozilla/5.0"})
                # Pre-check Content-Length: YouTube thumbs are typically <200KB,
                # and we cap at 20MB. A misbehaving server reporting 100MB+
                # gets refused without burning a slow read (audit:
                # thumbnails.py:130-141).
                with urllib.request.urlopen(req, timeout=30) as resp:
                    try:
                        _cl = resp.headers.get("Content-Length")
                        if _cl and int(_cl) > _MAX_BYTES:
                            raise ValueError(
                                f"Content-Length {_cl} exceeds 20MB cap")
                    except (TypeError, ValueError) as _cle:
                        if "exceeds" in str(_cle):
                            raise
                    candidate_data = resp.read(_MAX_BYTES)
                if not candidate_data or len(candidate_data) < 16:
                    raise ValueError(
                        f"empty/short response ({len(candidate_data)} bytes)")
                # JPEG: FF D8 FF. PNG: 89 50 4E 47. WEBP: RIFF....WEBP.
                _magic_ok = (candidate_data[:3] == b"\xFF\xD8\xFF"
                             or candidate_data[:4] == b"\x89PNG"
                             or (candidate_data[:4] == b"RIFF"
                                 and candidate_data[8:12] == b"WEBP"))
                if not _magic_ok:
                    raise ValueError("not a recognized image format")
                img_data = candidate_data
                break
            except Exception as _candidate_error:
                _last_error = _candidate_error
        if img_data is None:
            raise _last_error or ValueError("thumbnail unavailable")
        tmp_path = fpath + ".tmp"
        try:
            with open(tmp_path, "wb") as f:
                f.write(img_data)
                try:
                    f.flush()
                    os.fsync(f.fileno())
                except OSError:
                    pass
            os.replace(tmp_path, fpath)
        except Exception:
            # Clean up the orphan .tmp file before re-raising so a
            # disk-full / permission failure doesn't leave a half-
            # written .tmp inside .Thumbnails/ that accumulates over
            # repeated failures.
            try: os.remove(tmp_path)
            except OSError: pass
            raise
        # Re-apply the hidden attribute. Although the parent
        # .Thumbnails/ folder is hidden, files inside a hidden folder
        # are NOT automatically hidden on Windows — `dir /a` would
        # show them. If a user ever un-hides the folder, the contents
        # would become visible. Belt-and-suspenders per the "hidden
        # sidecars ULTIMATE RULE" memory.
        try:
            from .utils import hide_file_win
            hide_file_win(fpath)
        except Exception as e:
            _log.debug("swallowed: %s", e)
        # Patch fix (v68.4): debug-level log of the exact path written.
        # Helps diagnose "Recent card shows gradient placeholder" when
        # the thumbnail IS on disk somewhere but find_thumbnail's
        # search path doesn't reach it.
        _log.debug("thumbnail written: %s", fpath)
        # New download is committed — NOW drop the stale differently-
        # titled variant for the same [video_id] (deferred from the
        # dedup pass above so a failed fetch can never destroy the
        # only surviving copy).
        if _stale_old_thumb and _stale_old_thumb != fpath:
            try:
                os.remove(_stale_old_thumb)
            except OSError:
                pass
        return True
    except Exception as _te:
        # Non-fatal, but no longer invisible: emit a verbose-only
        # diagnostic so the user can see WHY a Browse thumbnail is
        # missing (404, timeout, disk-write failure, etc.) instead
        # of just seeing a placeholder with no hint.
        if stream is not None:
            try:
                stream.emit([
                    ["Thumbnail preview unavailable ", "dim"],
                    [f"[{video_id}]: {_te}\n", "dim"],
                ])
            except Exception as e:
                _log.debug("swallowed: %s", e)
        return False


def _thumbnail_exists_for(thumb_dir: str, video_id: str) -> bool:
    """True iff a valid thumbnail file in `thumb_dir` carries `[video_id]`."""
    if not thumb_dir or not video_id or not os.path.isdir(thumb_dir):
        return False
    bracket = f"[{video_id}]"
    try:
        for fn in os.listdir(thumb_dir):
            if bracket in fn and fn.lower().endswith(
                    (".jpg", ".jpeg", ".png", ".webp")):
                path = os.path.join(thumb_dir, fn)
                try:
                    if os.path.getsize(path) < 16:
                        continue
                    with open(path, "rb") as f:
                        head = f.read(12)
                    magic_ok = (
                        head[:3] == b"\xFF\xD8\xFF"
                        or head[:4] == b"\x89PNG"
                        or (head[:4] == b"RIFF" and head[8:12] == b"WEBP")
                    )
                    if magic_ok:
                        return True
                except OSError:
                    continue
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
    """Max mtime across the channel folder plus shallow year/month content.

    Directory mtimes catch newly added files, but not every in-place file
    edit or replace on remote/storage-backed filesystems. Include file mtimes
    through the usual channel/year/month layout so thumbnail status cache
    invalidates when existing media changes.
    """
    if not folder.exists():
        return 0.0
    try:
        mx = folder.stat().st_mtime
    except OSError:
        return 0.0
    def _scan(path: str | os.PathLike, depth: int) -> None:
        nonlocal mx
        try:
            with os.scandir(path) as it:
                for entry in it:
                    try:
                        st = entry.stat(follow_symlinks=False)
                        if st.st_mtime > mx:
                            mx = st.st_mtime
                        if depth > 0 and entry.is_dir(follow_symlinks=False):
                            _scan(entry.path, depth - 1)
                    except OSError:
                        pass
        except OSError:
            pass

    _scan(folder, 2)
    return mx
