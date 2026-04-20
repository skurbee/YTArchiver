"""
Startup temp-file cleanup — YTArchiver.py:2099-2190 port.

Walks every subscribed channel folder on launch and removes files left behind
by cancelled yt-dlp / ffmpeg runs: `.part`, `.temp`, `.ytdl`, `.fNNN.ext`
fragments, and orphaned `_TEMP_COMPRESS` / `_BACKLOG_TEMP` working folders.

Also callable post-sync / post-redownload as a defensive sweep.
"""

from __future__ import annotations

import os
import re
import shutil
import time
from typing import List

from .log_stream import LogStreamer
from .ytarchiver_config import load_config
from .sync import channel_folder_name as _cfn


_PARTIAL_FRAG_RE = re.compile(r'\.f\d{1,4}(?:-\d+)?\.[a-z0-9]{3,4}$', re.IGNORECASE)
_STALE_TEMP_DIRS = ("_TEMP_COMPRESS", "_BACKLOG_TEMP")


def is_partial_file(name: str) -> bool:
    """Return True if `name` looks like a yt-dlp / ffmpeg temp artifact."""
    low = name.lower()
    if low.endswith((".part", ".temp", ".ytdl")):
        return True
    if ".part." in low or ".temp." in low:
        return True
    if "_temp_compress" in low:
        return True
    if _PARTIAL_FRAG_RE.search(name):
        return True
    base, ext = os.path.splitext(name)
    if ext.lower() in (".webm", ".m4a", ".mp4") and re.search(r'\.f\d{1,4}(?:-\d+)?$', base):
        return True
    return False


def cleanup_folder(folder: str) -> int:
    """Recursively remove partial files under `folder`. Returns count."""
    if not folder or not os.path.isdir(folder):
        return 0
    cleaned = 0
    failed: List[str] = []
    for dp, dns, fns in os.walk(folder):
        # Drop any stale temp working dirs
        drop = [d for d in dns if d in _STALE_TEMP_DIRS]
        for d in drop:
            full = os.path.join(dp, d)
            try:
                shutil.rmtree(full, ignore_errors=True)
                cleaned += 1
            except Exception:
                pass
            dns.remove(d)
        for f in fns:
            if is_partial_file(f):
                fp = os.path.join(dp, f)
                try:
                    os.remove(fp)
                    cleaned += 1
                except OSError:
                    failed.append(fp)
    # Retry locked files once after a short delay (Windows)
    if failed:
        time.sleep(1.0)
        for fp in failed:
            try:
                os.remove(fp)
                cleaned += 1
            except OSError:
                pass
    return cleaned


def startup_cleanup_temps(stream: LogStreamer) -> int:
    """Walk every subscribed channel folder and nuke partial / temp files.

    Invoked on launch so a fresh session starts with a clean archive tree.
    """
    try:
        cfg = load_config()
    except Exception:
        return 0
    base = (cfg.get("output_dir") or "").strip()
    if not base or not os.path.isdir(base):
        return 0
    channels = cfg.get("channels", []) or []
    total = 0
    for ch in channels:
        ch_folder = os.path.join(base, _cfn(ch))
        total += cleanup_folder(ch_folder)
    if total and stream is not None:
        stream.emit([["[Startup] ", "sync_bracket"],
                     [f"\U0001f9f9 Cleaned {total} leftover temp file(s).\n",
                      "dim"]])
        stream.flush()
    return total
