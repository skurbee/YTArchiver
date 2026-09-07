"""
metadata.refresh — package shim re-exporting the three refresh kinds.

Refresh behavior is split by operation:

    refresh_views.py    — bulk_refresh_views_likes (~640 lines)
    refresh_comments.py — refresh_channel_comments (~205 lines)
    refresh_fetch.py    — fetch_channel_metadata   (~425 lines)

This module keeps the previously-public surface stable so external
callers (api_mixins.metadata_mixin, sync_all.py, repair_captions.py,
etc.) using `from backend.metadata.refresh import X` keep resolving.
"""
from __future__ import annotations

from .catalog import (  # noqa: F401
    _ID_RE,
    _ID_RE_11,
    _fetch_per_video_upload_dates,
    _flat_playlist_bulk_stats,
    _resolve_channel_id_url,
    _resolve_ids_by_title,
)
from .control import (  # noqa: F401
    _enter_pause_wait,
    _exit_pause_wait,
)
from .durations import (  # noqa: F401
    _probe_durations_bulk,
    _probe_file_duration,
)
from .identity import (  # noqa: F401
    backfill_video_ids,
    existing_info_ids,
)
from .refresh_comments import refresh_channel_comments  # noqa: F401
from .refresh_fetch import fetch_channel_metadata  # noqa: F401
from .refresh_views import bulk_refresh_views_likes  # noqa: F401
