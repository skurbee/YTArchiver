"""
Autorun — scheduled sync + activity-log history.

Maps to YTArchiver's `autorun_interval` config and the `autorun_history`
list. When enabled, a Sync Subbed pass fires every N minutes. Each real
run also appends an entry to `config["autorun_history"]` so it shows in
the activity log on the next launch.

AUTORUN_OPTIONS ports YTArchiver.py:22210 verbatim.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from .ytarchiver_config import load_config, save_config, config_is_writable


AUTORUN_OPTIONS = {
    "Off": 0, "30 min": 30, "1 hr": 60, "2 hr": 120,
    "4 hr": 240, "6 hr": 360, "12 hr": 720, "24 hr": 1440,
}
AUTORUN_LABELS = list(AUTORUN_OPTIONS.keys())

AUTORUN_HISTORY_MAX = 100


class AutorunScheduler:
    """Periodic Sync Subbed trigger.

    Matches classic (YTArchiver.py:22764 `_run_autorun` + :23395
    `_schedule_autorun`): when the interval timer fires while a sync is
    already running, the fire is postponed 60s AND the countdown is held
    at "Syncing now..." instead of restarting. The next interval does
    NOT begin counting down until the current sync finishes.
    """

    def __init__(self,
                 sync_trigger: Callable[[], None],
                 stream=None,
                 sync_busy_fn: Optional[Callable[[], bool]] = None):
        self._sync_trigger = sync_trigger
        self._stream = stream
        # Optional callable: returns True if a sync is currently running.
        # Used to postpone a fire (classic _sync_pipeline_busy) AND to
        # hold the countdown visible-but-paused via get_state().
        self._sync_busy_fn = sync_busy_fn
        self._interval_mins = 0
        self._lock = threading.RLock()
        self._timer: Optional[threading.Timer] = None
        self._next_fire_ts: Optional[float] = None
        # True between _fire() kicking sync and notify_sync_done() firing.
        # While set, get_state() surfaces seconds_remaining=None so the UI
        # shows "Syncing..." and the timer isn't rearmed until completion.
        self._waiting_for_sync_done = False

    # ── interval management ─────────────────────────────────────────

    def set_interval_label(self, label: str) -> Dict[str, Any]:
        mins = AUTORUN_OPTIONS.get(label, 0)
        return self.set_interval_mins(mins)

    def set_interval_mins(self, mins: int) -> Dict[str, Any]:
        with self._lock:
            self._interval_mins = int(mins or 0)
            self._cancel_timer_locked()
            self._waiting_for_sync_done = False
            if self._interval_mins > 0:
                self._schedule_next_locked()
        # Persist to config (gated).
        # audit D-42: check save_config return so a write-gate failure
        # surfaces to the caller instead of silently keeping the old
        # interval on disk. User sets "1 hr", restart later, finds it
        # reverted — previously no error shown.
        persisted = True
        if config_is_writable():
            cfg = load_config()
            cfg["autorun_interval"] = self._interval_mins
            persisted = bool(save_config(cfg))
        else:
            persisted = False
        return {"ok": True, "mins": self._interval_mins, "persisted": persisted}

    def get_state(self) -> Dict[str, Any]:
        # Check sync busy state OUTSIDE the lock — the callback may
        # acquire its own locks and could deadlock if we hold this one.
        busy = False
        if self._sync_busy_fn:
            try: busy = bool(self._sync_busy_fn())
            except Exception: busy = False
        with self._lock:
            # "Waiting" covers two cases: (a) we fired a sync ourselves
            # and are awaiting notify_sync_done, OR (b) ANY sync is
            # currently running (manual Sync Subbed, single-channel sync,
            # etc.) — classic's _tick_countdown shows "Waiting for queue..."
            # for all such cases.
            waiting = self._waiting_for_sync_done or busy
            overdue = 0
            if waiting:
                remaining = None
            elif self._next_fire_ts:
                # Bug [27]: also surface negative deltas (autorun is
                # past-due but hasn't fired yet — e.g., system asleep
                # past the scheduled time, or a long modal blocked the
                # tick thread). seconds_remaining stays clamped at 0
                # for backwards compat with the existing UI; new
                # overdue_seconds lets a future UI display "Overdue by X".
                _delta = int(self._next_fire_ts - time.time())
                remaining = max(0, _delta)
                if _delta < 0:
                    overdue = -_delta
            else:
                remaining = None
            return {
                "mins": self._interval_mins,
                "label": next((k for k, v in AUTORUN_OPTIONS.items()
                              if v == self._interval_mins), "Off"),
                "seconds_remaining": remaining,
                "overdue_seconds": overdue,
                "waiting_for_sync": waiting,
            }

    # ── scheduling ──────────────────────────────────────────────────

    def _cancel_timer_locked(self):
        if self._timer is not None:
            try:
                self._timer.cancel()
            except Exception:
                pass
            self._timer = None
        self._next_fire_ts = None

    def _schedule_next_locked(self, sec: Optional[int] = None):
        """Schedule the next _fire(). `sec` defaults to the configured
        interval; callers pass an explicit value (e.g. 60) to postpone a
        fire without changing the interval the user configured."""
        if sec is None:
            sec = self._interval_mins * 60
        if sec <= 0:
            return
        self._next_fire_ts = time.time() + sec
        t = threading.Timer(sec, self._fire)
        t.daemon = True
        t.start()
        self._timer = t

    def _fire(self):
        """Interval timer fired. Postpone 60s if sync is already running,
        otherwise kick a sync and enter "waiting for completion" state
        (countdown held until notify_sync_done() is called).
        """
        # If a sync is already running, defer 60s. Classic's
        # `_sync_pipeline_busy()` check at YTArchiver.py:22769 — without
        # this, autorun calls sync_start_all which errors out and the
        # timer never re-arms correctly.
        # audit D-16: if no busy-fn was wired, treat as "busy" (safer
        # default than "not busy", which could double-launch Sync
        # Subbed if autorun fires while a manual pass is still running).
        if self._sync_busy_fn is None:
            busy = True
        elif self._sync_busy_fn:
            try: busy = bool(self._sync_busy_fn())
            except Exception: busy = False
        else:
            busy = False
        if busy:
            if self._stream:
                self._stream.emit_text(
                    "\u2014 Autorun: sync still running, checking again in 60s\u2026",
                    "simpleline_dim")
            with self._lock:
                self._waiting_for_sync_done = False
                if self._interval_mins > 0:
                    self._schedule_next_locked(sec=60)
            return
        # Path A: interval elapsed + sync idle → kick the sync. Entering
        # waiting-for-completion state means the countdown holds at
        # "Syncing..." until notify_sync_done() is invoked by the sync
        # finally block (main.py sync_start_all._run).
        try:
            if self._stream:
                self._stream.emit_text(
                    "\u2014 Autorun: interval reached, kicking Sync Subbed\u2026",
                    "simpleline_green")
            with self._lock:
                self._waiting_for_sync_done = True
                self._next_fire_ts = None
                self._timer = None
            self._sync_trigger()
        except Exception as e:
            # If trigger itself blew up, unblock the wait so the scheduler
            # doesn't get stuck forever. Rearm with the full interval.
            if self._stream:
                self._stream.emit_error(f"Autorun trigger failed: {e}")
            with self._lock:
                self._waiting_for_sync_done = False
                if self._interval_mins > 0:
                    self._schedule_next_locked()

    def notify_sync_done(self):
        """Called by the sync worker when its run completes. If we fired
        this sync via the autorun timer, rearm the countdown with a fresh
        full interval — matches classic's `_schedule_autorun(iv)` call
        inside the sync finally block (YTArchiver.py:23380).
        Also called after manual Sync Subbed runs so the timer visually
        resets from now rather than finishing its mid-sync countdown.
        """
        with self._lock:
            if self._interval_mins <= 0:
                self._waiting_for_sync_done = False
                return
            # Cancel any pending timer (e.g. the 60s retry if a prior fire
            # bailed on busy) and start a fresh full-interval countdown.
            self._cancel_timer_locked()
            self._waiting_for_sync_done = False
            self._schedule_next_locked()

    def cancel(self):
        with self._lock:
            self._cancel_timer_locked()
            self._waiting_for_sync_done = False


# ── Activity-log history append ────────────────────────────────────────

_HISTORY_LOCK = threading.Lock()


def append_history_entry(entry: str, kind: str = "Auto") -> bool:
    """Append an autorun-history entry to config['autorun_history'].

    IMPORTANT: matches YTArchiver.py:22565 exactly — newest entry at the
    END of the list, trim via `hist[-MAX:]` to keep the last N. Earlier
    builds reversed this (insert(0)/keep first N) which scrambled
    history chronology when alternating OLD/NEW runs.

    audit E-35: module-level lock wraps the load-modify-save cycle so
    two near-simultaneous completions (rare but possible when multiple
    sources trigger autorun notifications) can't race and drop an
    entry. Without the lock, the second load_config saw a stale cfg
    missing the first's append.
    """
    if not config_is_writable():
        return False
    with _HISTORY_LOCK:
        cfg = load_config()
        hist = cfg.setdefault("autorun_history", [])
        hist.append(entry)
        if len(hist) > AUTORUN_HISTORY_MAX:
            cfg["autorun_history"] = hist[-AUTORUN_HISTORY_MAX:]
        save_config(cfg)
    return True


def clear_history() -> Dict[str, Any]:
    """Empty config['autorun_history'] and persist. Returns the count
    of entries that were removed. Matches OLD YTArchiver.py:22243
    `_clear_autorun_history` semantics — the user's "Clear" button on
    the activity-log strip clears BOTH the visible log and the saved
    history, so a relaunch doesn't resurrect the entries.
    """
    if not config_is_writable():
        return {"ok": False, "error": "write-gate off", "removed": 0}
    try:
        cfg = load_config()
        removed = len(cfg.get("autorun_history") or [])
        cfg["autorun_history"] = []
        save_config(cfg)
        return {"ok": True, "removed": removed}
    except Exception as e:
        return {"ok": False, "error": str(e), "removed": 0}


def format_history_entry(kind: str, channel: str,
                         primary: str, secondary: str = "",
                         errors: int = 0, took_sec: float = 0) -> str:
    """Render a history line exactly like YTArchiver.py:22559-22562.

    Format: "[ Auto] 3:16pm, Apr 10 — Channel — 3 downloaded · 0 skipped · 0 errors · took 36s"

    The `primary` argument is "<N> <label>" (e.g. "3 downloaded",
    "14 transcribed", "5 fetched", "2 compressed"). OLD has a separate
    `_record_*` function per kind with a hard-coded label; this function
    generalizes by parsing the label out of `primary` so the caller only
    has to know their own verb. Mirrors OLD's column alignment:
      - Count right-justified width 4
      - Label left-justified width 11
      - "skipped"/"existing" column right-justified width 4

    Key details:
      - Kind is padded to 6 chars via `center(6)` so `[ Auto ]` and `[Metdta]`
        line up when the column renders.
      - Time + date combined then ljust(16).
    """
    now = datetime.now()
    time_part = now.strftime("%I:%M%p").lstrip("0").lower()
    # bug L-14: always strip leading zeros the same way regardless of
    # platform. `%-d` is POSIX-only and was inconsistent on Windows —
    # sometimes rendering as "Apr 4" and sometimes "Apr 04" across a
    # single session. Build it explicitly.
    date_part = f"{now.strftime('%b')} {now.day}"

    # Parse "<N> <label>" out of primary so each kind gets its right verb
    # ("3 downloaded", "14 transcribed", "5 fetched", etc.). OLD's record_*
    # functions each hard-code their own verb; we just extract it from the
    # caller-supplied primary string so the color-picker in the UI matches.
    def _split_count_label(s: str, default_label: str = "downloaded"):
        try:
            parts = (s or "").strip().split(None, 1)
            if len(parts) == 2:
                return int(parts[0]), parts[1].strip()
            if len(parts) == 1:
                # Bare number — treat as count with default label
                return int(parts[0]), default_label
        except Exception:
            pass
        return 0, default_label
    dl, primary_label = _split_count_label(primary, "downloaded")

    # Secondary: most kinds use "skipped"; Metdta uses "existing". Preserve
    # whichever label the caller passed — we only lift out the count.
    def _first_int(s: str, default: int = 0) -> int:
        try:
            return int((s or "").strip().split()[0])
        except Exception:
            return default
    skipped = _first_int(secondary)
    # Default secondary label is "skipped"; Metdta kind uses "existing" per
    # OLD YTArchiver.py:22657. Honor a caller-supplied label in secondary.
    secondary_label = "skipped"
    try:
        parts = (secondary or "").strip().split(None, 1)
        if len(parts) >= 2:
            secondary_label = parts[1].strip()
    except Exception:
        pass
    err = int(errors or 0)

    # Took label
    took = _fmt_took(int(took_sec or 0))

    # Assemble with OLD's exact spacing
    ts_date = f"{time_part}, {date_part}".ljust(16)
    kind_tag = f"[{kind.center(6)}]" if len(kind) < 6 else f"[{kind}]"
    ch_part = f" {channel} \u2014" if channel else " " * 7
    line = (f"{kind_tag} {ts_date} \u2014{ch_part}"
            f" {dl:>4} {primary_label:<11} \u00b7 "
            f"{skipped:>4} {secondary_label} \u00b7 {err:>1} errors \u00b7 took {took}")
    return line


def _fmt_took(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    h = seconds // 3600
    return f"{h}h {(seconds - h*3600) // 60}m"
