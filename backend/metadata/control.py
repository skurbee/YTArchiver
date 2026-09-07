"""Pause feedback shared by metadata operations."""

from __future__ import annotations

from ..log_stream import LogStreamer


def _enter_pause_wait(stream: LogStreamer, label: str, queues) -> None:
    """Worker hit a pause-wait. Tell the queues UI ("actually paused")
    and emit a one-shot Paused log line so the user sees the pause
    take effect, not just see the button stop blinking.

    routes through pause_helpers.emit_paused \u2014 single source
    of truth for the pause/resume log style.
    """
    from ..pause_helpers import emit_paused
    emit_paused(stream, label=label, queues=queues)

def _exit_pause_wait(stream: LogStreamer, label: str, queues) -> None:
    """Worker exiting pause-wait (resumed or cancelled).

    routes through pause_helpers.emit_resumed.
    """
    from ..pause_helpers import emit_resumed
    emit_resumed(stream, label=label, queues=queues)
