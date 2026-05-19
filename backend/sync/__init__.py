"""
backend.sync package — sync orchestration entry point.

Originally a single `backend/sync.py` (~4,200 lines). Split into focused
submodules across multiple sittings:

    sync/core.py         — sync_channel + sync_all + the orchestration core
    sync/ytdlp_proc.py   — env + subprocess + cookies + format string
    sync/quickcheck.py   — prefetch + quick-check + batch cooldown
    sync/log_rows.py     — log emit + activity rows + row tracking
    sync/display_push.py — sync-progress JSON writes for companion display

This `__init__.py` re-exports every previously-public symbol so external
callers keep using `from backend.sync import sync_channel, ...` unchanged.
"""
from __future__ import annotations

# Star-import picks up every public (non-underscore) symbol.
from .core import *  # noqa: F401,F403

# Explicit underscore-prefix re-exports — external callers reach in for
# these names directly, so the package boundary has to surface them.
from .core import (  # noqa: F401
    _DOWNLOADING_RE,
    _F_SUFFIX_RE,
    _MERGE_RE,
    _PROG_RE,
    _ROW_EMIT_PASS_ID,
    _TITLE_RE,
    _VIDID_RE,
    _bracket_segments,
    _check_batch_cooldown,
    _count_cell,
    _ensure_videos_tab,
    _find_cookie_source,
    _fmt_duration,
    _hide_sidecar_win,
    _log,
    _new_pass_id,
    _persist_row_history,
    _record_recent_download,
    _resolve_final_mp4,
    _scan_recent_video,
    _short_summary,
    _should_batch_limit,
    _startupinfo,
    _sweep_orphan_vtts,
    _sync_row_emit,
)
