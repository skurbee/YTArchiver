"""
sync.ytdlp_proc — yt-dlp invocation primitives.

Patch 18 phase 2 (v68.8): extracted from sync/legacy.py. Contains the
pure-helper layer of sync — no shared mutable sync state, no log
emission, no row-tracking. Just:

  - cookie source discovery + cache (browser probe / cookies.txt)
  - yt-dlp executable lookup
  - format-string builder
  - folder-name sanitization
  - channel-folder resolver
  - `/videos` tab normalization
  - batch-file write/cleanup

Higher-level orchestration (sync_channel, sync_all, the DLTRACK
handler, etc.) lives in sync/core.py, which re-imports the names
below so existing callers keep resolving them as module-local.

External callers that did `from backend.sync import find_yt_dlp`
keep working because backend/sync/__init__.py re-exports these
and the package's __all__ surface is unchanged.
"""

from __future__ import annotations

import os
import re
import shutil
import threading
from pathlib import Path
from typing import Any

from ..log import get_logger

_log = get_logger(__name__)


# ── Resolution options ────────────────────────────────────────────────

RESOLUTION_OPTIONS = ["audio", "144", "240", "360", "480", "720",
                     "1080", "1440", "2160", "best"]


# ── Cookie source discovery ───────────────────────────────────────────
# Firefox is the ONLY browser we extract cookies from. Chromium browsers
# (Chrome/Brave/Edge/Vivaldi/Opera) use app-bound cookie encryption on
# Windows that yt-dlp cannot read — auto-selecting one just produced a
# confusing "could not get chrome cookies" error at download time. So the
# order is: Firefox if present → else a user-provided cookies.txt in
# %APPDATA%\YTArchiver\cookies.txt → else unauthenticated (public only).

_COOKIE_BROWSERS = ("firefox",)
_cookie_source_cached: list[str] | None = None
# Audit #7: lock around the cache so probe + reset from different
# worker threads (sync worker, transcribe worker, JS bridge thread)
# can't race. The probe is cheap (one cached list copy after first
# call) but the reset path also runs unsynchronized, and concurrent
# `None`-write while another thread is mid-probe could return a
# partially-populated browser key.
_cookie_source_lock = threading.Lock()


def _find_cookie_source() -> list[str]:
    """Return the yt-dlp cookie args to use (the '--cookies-from-browser X'
    pair or '--cookies /path/to/cookies.txt' pair, or an empty list)."""
    global _cookie_source_cached
    with _cookie_source_lock:
        if _cookie_source_cached is not None:
            return list(_cookie_source_cached)

        # Manual override — user can drop cookies.txt in APPDATA\YTArchiver\
        try:
            from ..ytarchiver_config import APP_DATA_DIR
            manual = APP_DATA_DIR / "cookies.txt"
            if manual.exists():
                _cookie_source_cached = ["--cookies", str(manual)]
                return list(_cookie_source_cached)
        except Exception as e:
            _log.debug("swallowed: %s", e)

        # Firefox only (Chromium browsers intentionally NOT probed — their
        # cookies are unreadable on Windows; see note at top of section).
        appdata = os.environ.get("APPDATA") or ""
        known_paths = {
            "firefox": os.path.join(appdata, "Mozilla", "Firefox", "Profiles"),
        }
        for browser in _COOKIE_BROWSERS:
            p = known_paths.get(browser)
            if p and os.path.isdir(p):
                _cookie_source_cached = ["--cookies-from-browser", browser]
                return list(_cookie_source_cached)

        # No Firefox + no cookies.txt — run unauthenticated (public content).
        _cookie_source_cached = []
        return list(_cookie_source_cached)


def reset_cookie_cache():
    """Clear the cached probe result — call after user changes browser choice."""
    global _cookie_source_cached
    with _cookie_source_lock:
        _cookie_source_cached = None


# ── yt-dlp discovery ──────────────────────────────────────────────────

def find_yt_dlp() -> str | None:
    """Locate yt-dlp.exe. Checks PATH first, then common bundled locations."""
    p = shutil.which("yt-dlp") or shutil.which("yt-dlp.exe")
    if p:
        return p
    # Path resolution: this file lives at backend/sync/ytdlp_proc.py.
    #   .parent             = backend/sync/
    #   .parent.parent      = backend/
    #   .parent.parent.parent = project root (or PyInstaller bundle root)
    candidates = [
        Path.cwd() / "yt-dlp.exe",
        Path(__file__).resolve().parent.parent.parent / "yt-dlp.exe",
        Path(__file__).resolve().parent.parent / "yt-dlp.exe",
        Path.home() / "Desktop" / "yt-dlp.exe",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return None


# ── Format string (verbatim port from YTArchiver.py:2730) ─────────────

def build_format_string(resolution: str) -> str:
    """Build yt-dlp --format string. Prefers H.264+AAC for native MP4 merging."""
    resolution = str(resolution).lower().strip()
    if resolution == "audio":
        return "bestaudio[ext=m4a]/bestaudio[acodec^=mp4a]/bestaudio/best"

    h = f"[height<={resolution}]" if resolution != "best" else ""
    base = (
        f"(bestvideo{h}[vcodec^=avc]+bestaudio[acodec^=mp4a])"
        f"/(bestvideo{h}[vcodec^=avc]+bestaudio)"
        f"/(bestvideo{h}[vcodec!^=av01]+bestaudio[acodec^=mp4a])"
        f"/(bestvideo{h}[vcodec!^=av01]+bestaudio)"
        f"/(bestvideo{h}+bestaudio)"
        f"/best{h}"
    )
    if resolution == "best":
        return base

    # Adjacent-resolution fallbacks.
    # wrap the int() in a try/except so a corrupted config
    # value (e.g. "720p" instead of "720") doesn't kill the sync pass
    # with a cryptic traceback. Fall back to "best" so the sync still
    # completes (with a wider format range than intended, but no data
    # loss).
    try:
        res_int = int(resolution)
    except (ValueError, TypeError):
        return base + "/best"
    res_above = None
    res_below = None
    _num_opts = [r for r in RESOLUTION_OPTIONS if r.isdigit()]
    for i, r in enumerate(_num_opts):
        if int(r) == res_int:
            if i + 1 < len(_num_opts):
                res_above = _num_opts[i + 1]
            if i > 0:
                res_below = _num_opts[i - 1]
            break
    fallbacks = ""
    if res_above:
        ha = f"[height<={res_above}]"
        fallbacks += f"/(bestvideo{ha}+bestaudio)/best{ha}"
    if res_below:
        hb = f"[height<={res_below}]"
        fallbacks += f"/(bestvideo{hb}+bestaudio)/best{hb}"
    fallbacks += "/(bestvideo+bestaudio)/best"
    return base + fallbacks


# ── Folder sanitization (verbatim port from YTArchiver.py:2790) ───────

_RESERVED_NAMES = frozenset({
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
})


def sanitize_folder(name: str) -> str:
    """Turn an arbitrary channel name into a safe Windows folder name.

    Replaces Windows-illegal characters (`< > : " / \\ | ? *` and any
    control byte 0x00-0x1F) with underscores, trims surrounding spaces
    and trailing dots, falls back to `_unnamed` when the input is empty
    or only consisted of illegal characters, and prefixes an underscore
    onto any of the OS-reserved device names (CON, PRN, AUX, NUL, COM1-9,
    LPT1-9) — Windows blocks those even as ordinary file/folder names.

    Verbatim behavior port of the original tkinter app's helper at
    YTArchiver.py:2790. Anything that touches the archive on disk
    (sync, reorg, metadata, redownload) must go through this function
    before constructing a folder path so the path is always creatable.
    """
    result = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', name).strip().rstrip('. ')
    if not result:
        result = "_unnamed"
    if result.upper().split('.')[0] in _RESERVED_NAMES:
        result = "_" + result
    return result


def channel_folder_name(ch: dict[str, Any]) -> str:
    """Return the safe on-disk folder name for a channel record.

    Honors the user's per-channel `folder_override` if it's set — that's
    the "Folder override" textbox in the Subs tab settings that lets
    the user rename a channel's folder without renaming the channel
    itself (useful when YouTube changes a creator's display name).
    Falls back to the channel `name` otherwise. Both routes go through
    `sanitize_folder()` so the result is always Windows-safe.

    A channel record with neither field set will resolve to `_unnamed/`,
    which is sync.py's "this channel record is broken, don't write
    anything new to it" graveyard folder.
    """
    return sanitize_folder((ch.get("folder_override") or "").strip()
                           or ch.get("name", ""))


# ── /videos tab normalization ─────────────────────────────────────────

def _ensure_videos_tab(url: str) -> str:
    """Append `/videos` to a channel URL so yt-dlp targets only the main
    uploads tab, not the multi-tab playlist (Videos + Live + Shorts).
    Mirrors YTArchiver.py:2594 exactly — only rewrites @Handle, /channel/,
    /c/, /user/ URLs. Leaves video URLs + arbitrary URLs alone.
    """
    u = (url or "").rstrip("/")
    if (("/@" in u or "/channel/" in u or "/c/" in u or "/user/" in u)
            and not u.endswith("/videos")):
        u += "/videos"
    return u


# ── Batch-file write/cleanup for yt-dlp --batch-file ──────────────────

def build_batch_file(video_ids) -> str | None:
    """Write video IDs as full YouTube URLs to a temp file for --batch-file.

    Returns the temp file path, or None on error. Mirrors YTArchiver.py:17993
    _build_batch_file. The caller is responsible for calling
    `cleanup_batch_file()` after yt-dlp finishes.
    """
    from ..ytarchiver_config import APP_DATA_DIR
    path = str(APP_DATA_DIR / "batch_urls_temp.txt")
    try:
        with open(path, "w", encoding="utf-8") as f:
            for vid in video_ids or []:
                vid = (vid or "").strip()
                if vid:
                    f.write(f"https://www.youtube.com/watch?v={vid}\n")
        return path
    except OSError:
        return None


def cleanup_batch_file() -> None:
    """Remove the temp batch-URL file if present."""
    from ..ytarchiver_config import APP_DATA_DIR
    path = APP_DATA_DIR / "batch_urls_temp.txt"
    try:
        if path.exists():
            path.unlink()
    except OSError:
        pass
