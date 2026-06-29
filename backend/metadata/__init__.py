"""
backend.metadata package — yt-dlp metadata pipeline entry point.

Originally a single `backend/metadata.py` (~4,200 lines). Split into:

    metadata/core.py            — primary entry points
    metadata/scan.py            — iter_channel_jsonls + scan helpers
    metadata/io.py              — JSONL I/O and path helpers
    metadata/normalize.py       — title-normalization shims
    metadata/fetcher.py         — per-video metadata fetches
    metadata/refresh.py         — bulk_refresh_views_likes pipeline
    metadata/thumbnails_ops.py  — sweep/realign thumbnail status

This `__init__.py` re-exports every previously-public symbol so external
callers (api_mixins, sync, main) keep using `from backend.metadata
import bulk_refresh_views_likes` unchanged.
"""
from __future__ import annotations

from .core import *  # noqa: F401,F403

# Explicit underscore-name re-exports — external callers reach in.
# (_read_metadata_jsonl is defined in .io and re-exported below; no
# separate .core re-export needed — it would just shadow the same object.)
from .io import (  # noqa: F401
    _folder_for_channel,
    _get_metadata_jsonl_path,
    _hide_file_win,
    _read_metadata_jsonl,
    _unhide_file_win,
    _write_metadata_jsonl,
)
