"""
sync.active_state — in-flight sync_channel tracking.

Thread-safe set of channel names currently being synced. Used by the
transcribe worker to know whether emitting a standalone [Trnscr]
activity row would race the [Dwnld] row from the same channel —
`sync_channel` wraps its entry with `set_sync_active(name)` and exit
with `clear_sync_active(name)`, so the transcribe worker can check
`is_sync_active(name)` and hold its row until sync finishes.

Also exposes the metadata-changed hook (used by Settings → Metadata
to auto-refresh its `XXm ago` timestamps after every metadata pass).
The hook lives here rather than in core.py so the giant module's
state surface shrinks.

Public surface (re-exported by backend.sync):
    set_sync_active(name)
    clear_sync_active(name)
    is_sync_active(name) -> bool
    is_any_sync_active() -> bool
    set_metadata_changed_hook(hook)
    fire_metadata_changed_hook()
"""
from __future__ import annotations

import threading
from typing import Any

from ..log import get_logger, swallow

_log = get_logger(__name__)


# Channels with an in-flight `sync_channel` call.
_active_sync_channels: set[str] = set()
_active_sync_lock = threading.Lock()


def set_sync_active(channel_name: str) -> None:
    """Mark `channel_name` as currently being synced.

    Called at the top of `sync_channel(name=...)` so the transcribe
    worker can tell whether a `[Trnscr]` activity row would be racing
    a `[Dwnld]` row from the same channel.
    """
    with _active_sync_lock:
        _active_sync_channels.add(channel_name)


def clear_sync_active(channel_name: str) -> None:
    """Unmark `channel_name` once its `sync_channel` call has returned.

    Idempotent: discards from the set, doesn't error if the name isn't
    there (e.g. when sync_channel exits early via cancellation).
    """
    with _active_sync_lock:
        _active_sync_channels.discard(channel_name)


def is_sync_active(channel_name: str) -> bool:
    """True iff a `sync_channel` call for `channel_name` is in flight.

    Threadsafe — the set is read under a lock so callers from the
    transcribe worker, the autorun scheduler, and the UI bridge get
    a consistent view.
    """
    with _active_sync_lock:
        return channel_name in _active_sync_channels


def is_any_sync_active() -> bool:
    """True iff at least one channel is currently mid-sync."""
    with _active_sync_lock:
        return len(_active_sync_channels) > 0


# Metadata-changed hook — Settings → Metadata auto-refreshes its
# `XXm ago` timestamps after every metadata / metadata_comments /
# videoid_backfill task completes.
_on_metadata_changed_hook: Any | None = None


def set_metadata_changed_hook(hook: Any | None) -> None:
    """Main.py wires this so Settings > Metadata auto-refreshes its
    `XXm ago` timestamps after any metadata / metadata_comments /
    videoid_backfill task completes."""
    global _on_metadata_changed_hook
    _on_metadata_changed_hook = hook


def fire_metadata_changed_hook() -> None:
    """Best-effort fire of the registered hook. Safe no-op when unset."""
    if _on_metadata_changed_hook is not None:
        try:
            _on_metadata_changed_hook()
        except Exception as e:
            swallow("metadata-changed hook", e)


# Channel-synced hook — fires each time `sync_all` finishes one channel
# (after the done-row emit + config write). Main.py wires this to a
# debounced JS push so the Subs tab's "Last Sync" column updates as
# channels finish, not only at end-of-pass.
_on_channel_synced_hook: Any | None = None


def set_channel_synced_hook(hook: Any | None) -> None:
    """Main.py wires this so the Subs tab auto-refreshes after each
    channel finishes syncing (per-channel live "Last Sync" updates)."""
    global _on_channel_synced_hook
    _on_channel_synced_hook = hook


def fire_channel_synced_hook() -> None:
    """Best-effort fire of the registered hook. Safe no-op when unset."""
    if _on_channel_synced_hook is not None:
        try:
            _on_channel_synced_hook()
        except Exception as e:
            swallow("channel-synced hook", e)
