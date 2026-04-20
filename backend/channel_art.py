"""
Channel art fetcher — YTArchiver.py:2605 (_fetch_channel_art) port.

Downloads a channel's avatar + banner into <channel_folder>/.ChannelArt/ so
the Browse grid can use real profile pictures instead of gradient placeholders.

Best-effort — any failure here is swallowed so a missing avatar never blocks
a metadata sweep or sync pass.
"""

from __future__ import annotations

import ctypes
import json
import os
import subprocess
import time
import urllib.request
from typing import Any, Dict, Optional

from .sync import find_yt_dlp, _find_cookie_source


_CHANNEL_ART_REFRESH_DAYS = 30 # YTArchiver refreshes monthly


def _hide_folder_win(path: str) -> None:
    if os.name != "nt":
        return
    try:
        ctypes.windll.kernel32.SetFileAttributesW(os.path.normpath(path), 0x02)
    except Exception:
        pass


def _pick_by_prefix(thumbs, prefix: str) -> Optional[Dict[str, Any]]:
    """Return the largest-area thumbnail whose id starts with `prefix`."""
    best = None
    best_area = 0
    for t in thumbs:
        tid = (t.get("id") or "").lower()
        if not tid.startswith(prefix):
            continue
        w = t.get("width") or 0
        h = t.get("height") or 0
        area = (w or 1) * (h or 1)
        if area >= best_area:
            best = t
            best_area = area
    return best


def _http_get(url: str, dest: str, timeout: int = 30) -> bool:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        with open(dest, "wb") as f:
            f.write(data)
        return True
    except Exception:
        return False


def fetch_channel_art(ch_url: str, folder_path: str, force: bool = False
                      ) -> Dict[str, Any]:
    """Download channel avatar + banner via yt-dlp metadata dump.

    Files written:
      <folder_path>/.ChannelArt/avatar.jpg
      <folder_path>/.ChannelArt/banner.jpg

    Skips if both exist and are < 30 days old (unless force=True).

    Returns {ok, avatar_path?, banner_path?, skipped?, error?}.
    """
    if not ch_url or not folder_path:
        return {"ok": False, "error": "ch_url + folder_path required"}

    art_dir = os.path.join(folder_path, ".ChannelArt")
    avatar_path = os.path.join(art_dir, "avatar.jpg")
    banner_path = os.path.join(art_dir, "banner.jpg")

    if not force:
        try:
            cutoff = _CHANNEL_ART_REFRESH_DAYS * 86400
            now = time.time()
            if (os.path.isfile(avatar_path) and
                os.path.isfile(banner_path) and
                (now - os.path.getmtime(avatar_path)) < cutoff and
                (now - os.path.getmtime(banner_path)) < cutoff):
                return {"ok": True, "skipped": True,
                        "avatar_path": avatar_path,
                        "banner_path": banner_path}
        except OSError:
            pass

    try:
        os.makedirs(art_dir, exist_ok=True)
    except OSError as e:
        return {"ok": False, "error": f"mkdir failed: {e}"}
    _hide_folder_win(art_dir)

    base_url = ch_url.rstrip("/")
    if base_url.endswith("/videos"):
        base_url = base_url[:-len("/videos")]

    yt_dlp = find_yt_dlp() or "yt-dlp"
    cmd = [
        yt_dlp, "--skip-download", "--no-warnings",
        "--dump-single-json", "--flat-playlist", "--playlist-items", "0",
    ]
    cmd += _find_cookie_source() or []
    cmd.append(base_url)

    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60,
            encoding="utf-8", errors="replace",
            creationflags=(0x08000000 if os.name == "nt" else 0),
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "yt-dlp timed out"}
    except FileNotFoundError:
        return {"ok": False, "error": "yt-dlp not found"}
    except Exception as e:
        return {"ok": False, "error": f"yt-dlp failed: {e}"}

    if proc.returncode != 0 or not (proc.stdout or "").strip():
        return {"ok": False, "error": "yt-dlp returned empty / error"}

    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        return {"ok": False, "error": f"JSON parse: {e}"}

    thumbs = data.get("thumbnails") or []
    if not thumbs:
        return {"ok": False, "error": "no thumbnails in metadata"}

    avatar = _pick_by_prefix(thumbs, "avatar")
    banner = _pick_by_prefix(thumbs, "banner")

    got = {"ok": True}
    if avatar and avatar.get("url") and _http_get(avatar["url"], avatar_path):
        got["avatar_path"] = avatar_path
    if banner and banner.get("url") and _http_get(banner["url"], banner_path):
        got["banner_path"] = banner_path
    if "avatar_path" not in got and "banner_path" not in got:
        return {"ok": False, "error": "avatar + banner downloads failed"}
    return got


def avatar_path_for(folder_path: str) -> Optional[str]:
    """Return the avatar.jpg path if present, else None."""
    if not folder_path:
        return None
    p = os.path.join(folder_path, ".ChannelArt", "avatar.jpg")
    return p if os.path.isfile(p) else None


def banner_path_for(folder_path: str) -> Optional[str]:
    if not folder_path:
        return None
    p = os.path.join(folder_path, ".ChannelArt", "banner.jpg")
    return p if os.path.isfile(p) else None


def banner_thumb_path_for(folder_path: str) -> Optional[str]:
    """Return a cached small (max 640px wide) banner thumb if one exists.

    Originals are 2048x1152+ / ~350KB; decoding 100+ of them on the
    Browse grid stalls scroll rendering. The thumb is ~30KB and decodes
    in ~1ms, so scrolling stays smooth even on 100+ channel archives.
    Created lazily by `ensure_banner_thumb()` (called on demand when the
    grid is about to render, or from the startup preload pass).
    """
    if not folder_path:
        return None
    p = os.path.join(folder_path, ".ChannelArt", "banner_small.jpg")
    return p if os.path.isfile(p) else None


def avatar_thumb_path_for(folder_path: str) -> Optional[str]:
    """Return a cached avatar thumb (max 128px) if one exists."""
    if not folder_path:
        return None
    p = os.path.join(folder_path, ".ChannelArt", "avatar_small.jpg")
    return p if os.path.isfile(p) else None


def _make_thumb(src: str, dst: str, max_w: int) -> bool:
    """Resize `src` image to at most `max_w` pixels wide, save to `dst`.
    No-op if src doesn't exist or dst already exists + is newer than src.
    Returns True if dst now exists."""
    if not os.path.isfile(src):
        return False
    if os.path.isfile(dst):
        try:
            if os.path.getmtime(dst) >= os.path.getmtime(src):
                return True
        except OSError:
            pass
    try:
        from PIL import Image
    except ImportError:
        return False
    try:
        with Image.open(src) as im:
            im.thumbnail((max_w, max_w * 2), Image.Resampling.LANCZOS)
            # Strip alpha / weird modes — convert to RGB for JPEG output
            if im.mode not in ("RGB", "L"):
                im = im.convert("RGB")
            im.save(dst, "JPEG", quality=82, optimize=True, progressive=True)
        return True
    except Exception:
        return False


def ensure_banner_thumb(folder_path: str, max_w: int = 640) -> Optional[str]:
    """Lazily generate banner_small.jpg from banner.jpg. Returns thumb path
    (or the original banner path as fallback if thumbnailing failed)."""
    if not folder_path:
        return None
    src = os.path.join(folder_path, ".ChannelArt", "banner.jpg")
    if not os.path.isfile(src):
        return None
    dst = os.path.join(folder_path, ".ChannelArt", "banner_small.jpg")
    if _make_thumb(src, dst, max_w):
        return dst
    return src # fall back to the original rather than breaking rendering


def ensure_avatar_thumb(folder_path: str, max_w: int = 128) -> Optional[str]:
    """Lazily generate avatar_small.jpg from avatar.jpg."""
    if not folder_path:
        return None
    src = os.path.join(folder_path, ".ChannelArt", "avatar.jpg")
    if not os.path.isfile(src):
        return None
    dst = os.path.join(folder_path, ".ChannelArt", "avatar_small.jpg")
    if _make_thumb(src, dst, max_w):
        return dst
    return src
