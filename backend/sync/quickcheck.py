"""
sync.quickcheck — pre-sync probes + per-channel batch-limit gating.

Patch 18 phase 3 (v68.8): extracted from sync/legacy.py. Contains:

  - `prefetch_channel_total(url)` — fast yt-dlp flat-playlist enumeration
    to learn a channel's total + live + upcoming count BEFORE sync.
  - `quick_check_new_uploads(url, archived_ids, ...)` — check first N
    uploads; short-circuit the per-channel sync when nothing's new.
  - `_check_batch_cooldown(ch)` — gate channels that hit the 72h
    bootstrap cooldown.
  - `_should_batch_limit(ch, ch_total)` — does this channel need batch
    cooldown rules applied at all?
  - `set_batch_cooldown(ch_url)` — write the 72h timestamp.

Module-level constants `_BATCH_LIMIT` and `_BATCH_COOLDOWN_HOURS` are
also defined here (legacy.py re-imports both).
"""
from __future__ import annotations

import os
import re
import subprocess
import time
from datetime import datetime as _dt
from datetime import timedelta as _td
from typing import Any

from .. import utils as _utils
from ..log import get_logger
from ..ytarchiver_config import load_config
from .ytdlp_proc import _ensure_videos_tab, _find_cookie_source, find_yt_dlp

_log = get_logger(__name__)


# Bootstrap batch-cooldown gating.
_BATCH_LIMIT = 100000           # YTArchiver.py:17503
_BATCH_COOLDOWN_HOURS = 72      # YTArchiver.py:17504


def prefetch_channel_total(ch_url: str, timeout_sec: int = 30
                            ) -> dict[str, Any]:
    """Query YouTube for a channel's total video count + live-stream count
    before kicking off sync. Mirrors YTArchiver.py:17590 _prefetch_total and
    :18017 _prefetch_livestreams — purely informational, never blocks sync.

    Returns {ok, total, lives, upcoming, error?}.
    """
    yt_dlp = find_yt_dlp()
    if not yt_dlp or not ch_url:
        return {"ok": False, "error": "yt-dlp missing or no URL"}
    cmd = [
        yt_dlp, "--flat-playlist", "--no-warnings",
        "--print", "%(id)s|||%(live_status)s",
    ]
    cmd += _find_cookie_source() or []
    cmd.append(ch_url)
    total = 0
    lives = 0
    upcoming = 0
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, encoding="utf-8", errors="replace",
            creationflags=(0x08000000 if os.name == "nt" else 0),
            env=_utils.utf8_subprocess_env(),
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        deadline = time.time() + float(timeout_sec)
        for line in proc.stdout:
            if time.time() > deadline:
                # Drain stdout in a background thread before terminate
                # so a full pipe doesn't deadlock the subsequent wait
                # (audit: sync/quickcheck.py:87). yt-dlp can dump
                # output faster than we consume it on a wide-screen
                # console; without the drain, proc.terminate() then
                # proc.wait() can hang because the OS pipe buffer is
                # full and the child blocks on write().
                try: proc.terminate()
                except Exception: pass
                try:
                    import threading as _th
                    def _drain():
                        try:
                            while proc.stdout.readline():
                                pass
                        except Exception: pass
                    _th.Thread(target=_drain, daemon=True).start()
                except Exception: pass
                break
            raw = line.strip()
            if "|||" not in raw:
                continue
            _, status = raw.split("|||", 1)
            status = status.strip().lower()
            total += 1
            if status == "is_live":
                lives += 1
            elif status == "is_upcoming":
                upcoming += 1
    finally:
        # Bound the post-kill wait so a child refusing to die can't
        # leak a Windows handle until GC (audit: sync/quickcheck.py:88).
        try: proc.wait(timeout=5)
        except Exception:
            try: proc.kill()
            except Exception as e: _log.debug("swallowed: %s", e)
            try: proc.wait(timeout=5)
            except Exception as e: _log.debug("swallowed: %s", e)
    return {"ok": True, "total": total, "lives": lives, "upcoming": upcoming}


def quick_check_new_uploads(ch_url: str, archived_ids,
                            check_count: int = 5, timeout_sec: int = 30
                            ) -> dict[str, Any]:
    """Probe the first N videos of a channel to see if any are NOT in our
    archive already. Short-circuit for channels with nothing new.

    Mirrors YTArchiver.py:17943 _quick_check_new_uploads exactly:
      - `_ensure_videos_tab(url)` so the multi-tab playlist doesn't suck
        in the Live/Shorts tabs
      - `--lazy-playlist` so yt-dlp stops enumerating once it has enough
      - `--playlist-end N` (not `--playlist-items 1:N`) — the OLD flag
      - `archived_ids` can be a list or set; we coerce to a set for O(1)
    Returns {ok, has_new, checked, fresh_ids}.
    """
    yt_dlp = find_yt_dlp()
    if not yt_dlp or not ch_url:
        return {"ok": False, "error": "yt-dlp missing or no URL"}
    qc_url = _ensure_videos_tab(ch_url)
    cmd = [
        yt_dlp,
        "--flat-playlist", "--lazy-playlist",
        "--playlist-end", str(int(check_count)),
        "--print", "id",
        "--no-warnings",
    ]
    cmd += _find_cookie_source() or []
    cmd.append(qc_url)
    if isinstance(archived_ids, set):
        archived_set = archived_ids
    else:
        archived_set = {x.strip() for x in (archived_ids or []) if x}
    checked: list[str] = []
    fresh: list[str] = []
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=float(timeout_sec),
            encoding="utf-8", errors="replace",
            creationflags=(0x08000000 if os.name == "nt" else 0),
            env=_utils.utf8_subprocess_env(),
        )
    except subprocess.TimeoutExpired:
        # yt-dlp didn't finish in time, but that's NOT a launch failure
        # — it's a transient slow YouTube response. Treat the same as
        # "no result", which the OLD-behavior empty path treats as
        # "might have new" so the sync pipeline does a full walk
        # rather than skipping (audit: sync/quickcheck.py:129).
        return {"ok": True, "has_new": True,
                "checked": 0, "fresh_ids": [], "timed_out": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    for raw in (proc.stdout or "").splitlines():
        raw = raw.strip()
        if not raw or not re.fullmatch(r'[A-Za-z0-9_-]{11}', raw):
            continue
        checked.append(raw)
        if raw not in archived_set:
            fresh.append(raw)
    # Empty result = treat as "might have new" per OLD's behavior
    # (line 17980-17981: `if not ids: return True`).
    if not checked:
        return {"ok": True, "has_new": True,
                "checked": 0, "fresh_ids": []}
    return {"ok": True, "has_new": bool(fresh),
            "checked": len(checked), "fresh_ids": fresh}


def _check_batch_cooldown(ch: dict[str, Any]) -> tuple[bool, str]:
    """Return (can_proceed, cooldown_label).

    Channels that haven't been fully initialized AND have >100k videos get
    a 72-hour cooldown between syncs to avoid hammering YouTube for pagination
    during bootstrap. Mirrors YTArchiver.py:17507 _check_batch_cooldown.
    """
    batch_after = ch.get("init_batch_after")
    if not batch_after:
        return True, ""
    try:
        cooldown_dt = _dt.fromisoformat(batch_after).replace(tzinfo=None)
        if _dt.now() >= cooldown_dt:
            return True, ""
        time_str = cooldown_dt.strftime("%I:%M%p").lstrip("0").lower()
        date_str = cooldown_dt.strftime("%b %d")
        return False, f"{time_str}, {date_str}"
    except (ValueError, TypeError):
        return True, ""


def _should_batch_limit(ch: dict[str, Any], ch_total: int) -> bool:
    """Return True if this channel should be subject to batch cooldown rules.

    bug W-9 clarification: the two flags look similar but gate
    different things.
      * `initialized`     = "first sync completed AT LEAST ONCE"
                            (even if it walked nothing useful —
                             bug S-6 tightened that path).
      * `init_complete`   = "full bootstrap has walked the whole
                             catalog" (definite: not paused mid-walk).
      * `_check_batch_cooldown` (separate fn, elsewhere) enforces the
        72h cooldown timestamp; THIS fn just decides if batch-limit
        rules apply at all.
    Order matters: mode must be full (channel-wide mode), init_complete
    short-circuits out (already past bootstrap), and only THEN we fall
    through to channel-size checks.
    """
    if ch.get("mode", "full") != "full":
        return False
    if ch.get("init_complete", False):
        return False
    if ch_total > 0:
        return ch_total > _BATCH_LIMIT
    # Count unavailable — batch limit if channel isn't initialized yet
    return not ch.get("initialized", False)


def set_batch_cooldown(ch_url: str) -> None:
    """Apply a 72h cooldown to a channel (called after a bootstrap run)."""
    from .. import subs as _subs
    cfg = load_config()
    # Normalize once for the comparison key so trailing slash / www / scheme
    # variants between the live URL and the config-stored URL still match.
    try:
        target = _subs.normalize_channel_url(ch_url)
    except Exception:
        target = ch_url
    changed = False
    for cfg_ch in cfg.get("channels", []):
        cfg_url = cfg_ch.get("url", "")
        try:
            cfg_norm = _subs.normalize_channel_url(cfg_url)
        except Exception:
            cfg_norm = cfg_url
        if cfg_norm == target or cfg_url == ch_url:
            cfg_ch["init_batch_after"] = (_dt.now() + _td(hours=_BATCH_COOLDOWN_HOURS)).isoformat()
            changed = True
    if changed:
        try:
            from ..ytarchiver_config import save_config as _sc
            _sc(cfg)
        except Exception as e:
            _log.debug("swallowed: %s", e)
