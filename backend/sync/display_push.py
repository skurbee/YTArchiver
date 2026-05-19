"""
sync.display_push — sync-progress JSON writes for a companion display.

Patch 18 phase 3 (v68.8): extracted from sync/legacy.py.

A companion display tool can poll `<APP_DATA>/sync_progress.json`
to render the current channel + per-pass totals on its own UI. Sync
writes that file on every "Channel idx/total" tick. This module owns
that file and its locked state.

Public surface (re-exported via sync/__init__.py for back-compat):
    write_sync_progress(channel_name, idx, total, downloaded, skipped, errors)
    clear_sync_progress()
    _sync_progress_path()
    _SYNC_PROGRESS_STATE
    _SYNC_PROGRESS_LOCK
"""
from __future__ import annotations

import json
import os
import threading

from ..log import get_logger

_log = get_logger(__name__)


# ── State ─────────────────────────────────────────────────────────────

_SYNC_PROGRESS_STATE = {"totals": {"dl": 0, "skip": 0, "err": 0}}
# Audit #5: `_SYNC_PROGRESS_STATE` is mutated from the sync worker
# thread AND from any external polling reader / clear_sync_progress
# callers on shutdown. The previous `t["dl"] += ...` was a read-modify-
# write without protection — concurrent writes can drop increments.
# Lock all touches to the totals dict.
_SYNC_PROGRESS_LOCK = threading.Lock()


def _sync_progress_path() -> str:
    from ..ytarchiver_config import APP_DATA_DIR
    return os.path.join(str(APP_DATA_DIR), "sync_progress.json")


def write_sync_progress(channel_name: str = "",
                        idx: int = 0, total: int = 0,
                        downloaded: int = 0, skipped: int = 0,
                        errors: int = 0) -> None:
    """Write sync state to sync_progress.json for a companion display."""
    try:
        # Accumulate session totals so external readers see consistent
        # numbers. Totals are reset by clear_sync_progress.
        with _SYNC_PROGRESS_LOCK:
            t = _SYNC_PROGRESS_STATE["totals"]
            t["dl"] += int(downloaded or 0)
            t["skip"] += int(skipped or 0)
            t["err"] += int(errors or 0)
            data = {
                "running": True,
                "channel": channel_name or "",
                "idx": int(idx or 1),
                "total": int(total or 1),
                "dl": t["dl"],
                "skip": t["skip"],
                "err": t["err"],
            }
        path = _sync_progress_path()
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except Exception as e:
        _log.debug("swallowed: %s", e)


def clear_sync_progress() -> None:
    """Remove sync_progress.json when the pass ends + reset totals."""
    with _SYNC_PROGRESS_LOCK:
        _SYNC_PROGRESS_STATE["totals"] = {"dl": 0, "skip": 0, "err": 0}
    try:
        p = _sync_progress_path()
        if os.path.exists(p):
            os.remove(p)
    except Exception as e:
        _log.debug("swallowed: %s", e)
