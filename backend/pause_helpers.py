"""
Centralized pause-wait helpers.

consolidates four near-identical pause-wait
implementations that previously lived inline in:
  - sync.py:_wait_if_paused (the download-pass version with row-repaint)
  - metadata.py:_enter_pause_wait / _exit_pause_wait
  - repair_captions.py:_wait_if_paused
  - punct_restore.py:_wait_if_paused

The core semantics they all share:
  1. If pause_event isn't set, return immediately.
  2. Emit a "Paused" log line and flip queues.set_sync_paused_active(True)
     so the Sync Tasks popover shows the pause has actually taken effect
     (vs just being requested — the button stops blinking).
  3. Block until either pause_event clears (Resume) or cancel_event is
     set (Cancel).
  4. Clear the active-pause flag and emit "Resumed".

The sync.py version additionally repaints the active row with a PAUSED
prefix; callers needing custom emit behavior can use emit_paused /
emit_resumed + wait_for_resume directly to compose their own wrapper.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime


def _now_clock_str() -> str:
    """Return time as "h:mmam" / "h:mmpm" — matches the existing log
    formatting in metadata._enter_pause_wait."""
    try:
        return datetime.now().strftime("%I:%M%p").lstrip("0").lower()
    except Exception:
        return ""


def emit_paused(stream, label: str = "", queues=None) -> None:
    """Emit the standard 'Paused at h:mmam — <label> — click Resume.'
    line and flip queues.set_sync_paused_active(True). Idempotent —
    callers don't need to check pause_event before invoking; they
    typically already have."""
    if queues is not None:
        try:
            queues.set_sync_paused_active(True)
        except Exception:
            pass
    if stream is None:
        return
    now = _now_clock_str()
    label_text = f" — {label}" if label else ""
    try:
        stream.emit([
            ["⏸ Paused at ", "simpleline"],
            [now, "simpleline"],
            [f"{label_text} — click Resume.\n", "dim"],
        ])
    except Exception:
        # Fallback to emit_text for streams that lack .emit
        try:
            stream.emit_text(
                " — Paused. Click Resume in Sync Tasks to continue.\n",
                "simpleline")
            try: stream.flush()
            except Exception: pass
        except Exception:
            pass


def emit_resumed(stream, label: str = "", queues=None,
                 *, cancelled: bool = False) -> None:
    """Emit 'Resumed at h:mmam.' and clear queues.set_sync_paused_active.
    If cancelled, only clears the flag (no Resumed line — the cancel
    path emits its own cancel message)."""
    if queues is not None:
        try:
            queues.set_sync_paused_active(False)
        except Exception:
            pass
    if cancelled or stream is None:
        return
    now = _now_clock_str()
    try:
        stream.emit([
            ["▶ Resumed at ", "simpleline_green"],
            [now, "simpleline_green"],
            [".\n", "dim"],
        ])
    except Exception:
        try:
            stream.emit_text(" — Resumed.\n", "simpleline")
            try: stream.flush()
            except Exception: pass
        except Exception:
            pass


def wait_for_resume(
    pause_event: threading.Event | None,
    cancel_event: threading.Event | None = None,
    *,
    tick: float = 0.25,
) -> bool:
    """Block while pause_event is set. Returns True if cancelled.

    Uses pause_event.wait(tick) so cancel + resume can both wake the
    loop within `tick` seconds. tick=0.25 matches sync.py's existing
    rate; tick=0.5 matches repair_captions / punct_restore.
    """
    if pause_event is None or not pause_event.is_set():
        return False
    while pause_event.is_set():
        if cancel_event is not None and cancel_event.is_set():
            return True
        try:
            pause_event.wait(timeout=tick)
        except Exception:
            time.sleep(tick)
    return False


def wait_while_paused(
    pause_event: threading.Event | None,
    cancel_event: threading.Event | None = None,
    *,
    stream=None,
    label: str = "",
    queues=None,
    tick: float = 0.25,
) -> bool:
    """Full pause-wait pattern: emit Paused, block until resume/cancel,
    emit Resumed. Returns True if cancelled.

    This is the high-level helper repair_captions and punct_restore
    use. sync.py and metadata.py have additional row-painting needs;
    they compose emit_paused + wait_for_resume + emit_resumed directly.
    """
    if pause_event is None or not pause_event.is_set():
        return False
    emit_paused(stream, label, queues)
    cancelled = wait_for_resume(pause_event, cancel_event, tick=tick)
    emit_resumed(stream, label, queues, cancelled=cancelled)
    return cancelled
