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
also defined here (re-exported via the sync package).
"""
from __future__ import annotations

import os
import re
import subprocess
import threading
import time
from datetime import datetime as _dt
from datetime import timedelta as _td
from typing import Any

from .. import utils as _utils
from ..log import get_logger, swallow
from ..process_runner import PROCESS_REGISTRY
from ..ytarchiver_config import config_transaction, load_config
from .ytdlp_proc import _ensure_videos_tab, _find_cookie_source, find_yt_dlp

_log = get_logger(__name__)


# Bootstrap batch-cooldown gating.
_BATCH_LIMIT = 100000           # YTArchiver.py:17503
_BATCH_COOLDOWN_HOURS = 72      # YTArchiver.py:17504
_QUICKCHECK_BAD_THRESHOLD = 2
_QUICKCHECK_COOLDOWN_SEC = 3600.0
_quickcheck_bad: dict[str, dict[str, float]] = {}
_quickcheck_bad_lock = threading.Lock()


def _quickcheck_key(ch_url: str) -> str:
    return _ensure_videos_tab(ch_url or "").strip().lower()


def _quickcheck_skip_state(ch_url: str) -> dict[str, Any] | None:
    key = _quickcheck_key(ch_url)
    now = time.time()
    with _quickcheck_bad_lock:
        state = _quickcheck_bad.get(key)
        if not state:
            return None
        until = float(state.get("until") or 0)
        if until > now:
            return {
                "ok": True,
                "has_new": True,
                "checked": 0,
                "fresh_ids": [],
                "quickcheck_skipped": True,
                "cooldown_until": until,
            }
        if until:
            _quickcheck_bad.pop(key, None)
    return None


def _record_quickcheck_bad(ch_url: str, reason: str) -> None:
    key = _quickcheck_key(ch_url)
    now = time.time()
    with _quickcheck_bad_lock:
        state = _quickcheck_bad.setdefault(key, {"count": 0.0, "until": 0.0})
        state["count"] = float(state.get("count") or 0) + 1.0
        state["last"] = now
        state["reason"] = reason
        if state["count"] >= _QUICKCHECK_BAD_THRESHOLD:
            state["until"] = now + _QUICKCHECK_COOLDOWN_SEC


def _clear_quickcheck_bad(ch_url: str) -> None:
    with _quickcheck_bad_lock:
        _quickcheck_bad.pop(_quickcheck_key(ch_url), None)


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
        PROCESS_REGISTRY.register(proc)
    except Exception as e:
        swallow("process-registry register", e)
    timer = None
    timeout_hit = {"hit": False}
    try:
        def _kill_on_timeout() -> None:
            timeout_hit["hit"] = True
            try:
                proc.kill()
            except Exception as e:
                swallow("timer kill", e)

        timer = threading.Timer(float(timeout_sec), _kill_on_timeout)
        timer.daemon = True
        timer.start()
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
                except Exception as e: swallow("deadline terminate", e)
                try:
                    import threading as _th
                    def _drain():
                        try:
                            while proc.stdout.readline():
                                pass
                        except Exception as e: swallow("stdout drain", e)
                    _th.Thread(target=_drain, daemon=True).start()
                except Exception as e: swallow("drain thread start", e)
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
        if timer is not None:
            try:
                timer.cancel()
            except Exception as e:
                swallow("timer cancel", e)
        # Bound the post-kill wait so a child refusing to die can't
        # leak a Windows handle until GC (audit: sync/quickcheck.py:88).
        try: proc.wait(timeout=5)
        except Exception:
            try: proc.kill()
            except Exception as e: swallow("proc kill", e)
            try: proc.wait(timeout=5)
            except Exception as e: swallow("proc wait", e)
        try:
            PROCESS_REGISTRY.unregister(proc)
        except Exception as e:
            swallow("process-registry unregister", e)
    result = {"ok": True, "total": total, "lives": lives,
              "upcoming": upcoming}
    if timeout_hit.get("hit"):
        result["timed_out"] = True
    return result


def quick_check_new_uploads(ch_url: str, archived_ids,
                            check_count: int = 5, timeout_sec: int = 30,
                            min_duration: int = 0,
                            max_duration: int = 0
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
    skipped = _quickcheck_skip_state(ch_url)
    if skipped is not None:
        return skipped
    qc_url = _ensure_videos_tab(ch_url)
    def _clean_duration(value: object) -> int:
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    min_dur = _clean_duration(min_duration)
    max_dur = _clean_duration(max_duration)
    use_duration_filter = bool(min_dur or max_dur)
    print_expr = ("%(id)s|||%(duration)s|||%(live_status)s"
                  if use_duration_filter else "id")
    cmd = [
        yt_dlp,
        "--flat-playlist", "--lazy-playlist",
        "--playlist-end", str(int(check_count)),
        "--print", print_expr,
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
    filtered: list[str] = []
    def _duration_filtered(duration_raw: str, live_status: str) -> bool:
        status = (live_status or "").strip().lower()
        if status in ("is_live", "is_upcoming"):
            return True
        try:
            dur = float((duration_raw or "").strip())
        except (TypeError, ValueError):
            return False
        if min_dur and dur <= min_dur:
            return True
        if max_dur and dur >= max_dur:
            return True
        return False

    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=float(timeout_sec),
            encoding="utf-8", errors="replace",
            # stdin=DEVNULL so a signal sent to the parent doesn't
            # propagate into yt-dlp via shared stdin and abort the
            # quick-check (audit: quickcheck L20).
            stdin=subprocess.DEVNULL,
            creationflags=(0x08000000 if os.name == "nt" else 0),
            env=_utils.utf8_subprocess_env(),
        )
    except subprocess.TimeoutExpired:
        # yt-dlp didn't finish in time, but that's NOT a launch failure
        # — it's a transient slow YouTube response. Treat the same as
        # "no result", which the OLD-behavior empty path treats as
        # "might have new" so the sync pipeline does a full walk
        # rather than skipping (audit: sync/quickcheck.py:129).
        _record_quickcheck_bad(ch_url, "timeout")
        return {"ok": True, "has_new": True,
                "checked": 0, "fresh_ids": [], "timed_out": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    for raw in (proc.stdout or "").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        if use_duration_filter and "|||" in raw:
            parts = raw.split("|||", 2)
            vid = parts[0].strip()
            duration_raw = parts[1] if len(parts) > 1 else ""
            live_status = parts[2] if len(parts) > 2 else ""
        else:
            vid = raw
            duration_raw = ""
            live_status = ""
        if not re.fullmatch(r'[A-Za-z0-9_-]{11}', vid):
            continue
        checked.append(vid)
        if vid in archived_set:
            continue
        if use_duration_filter and _duration_filtered(duration_raw, live_status):
            filtered.append(vid)
            continue
        fresh.append(vid)
    # Empty result = treat as "might have new" per OLD's behavior
    # (line 17980-17981: `if not ids: return True`).
    if not checked:
        _record_quickcheck_bad(ch_url, "empty")
        return {"ok": True, "has_new": True,
                "checked": 0, "fresh_ids": [], "empty_probe": True}
    _clear_quickcheck_bad(ch_url)
    return {"ok": True, "has_new": bool(fresh),
            "checked": len(checked), "fresh_ids": fresh,
            "filtered_ids": filtered}


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
    # Normalize once for the comparison key so trailing slash / www / scheme
    # variants between the live URL and the config-stored URL still match.
    try:
        target = _subs.normalize_channel_url(ch_url)
    except Exception:
        target = ch_url
    try:
        with config_transaction() as cfg:
            for cfg_ch in cfg.get("channels", []):
                cfg_url = cfg_ch.get("url", "")
                try:
                    cfg_norm = _subs.normalize_channel_url(cfg_url)
                except Exception:
                    cfg_norm = cfg_url
                if cfg_norm == target or cfg_url == ch_url:
                    cfg_ch["init_batch_after"] = (
                        _dt.now() + _td(hours=_BATCH_COOLDOWN_HOURS)
                    ).isoformat()
    except Exception as e:
        swallow("batch-cooldown config update", e)
