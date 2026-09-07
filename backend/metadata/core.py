"""Compatibility surface for independently owned metadata operations.

Internal callers import the responsible leaf module directly. This module
retains historical entry points without introducing a dependency back from
catalog, identity, duration or refresh implementations.
"""

from .catalog import (  # noqa: F401
    _fetch_per_video_upload_dates,
    _flat_playlist_bulk_stats,
    _resolve_channel_id_url,
    _resolve_ids_by_title,
)
from .control import _enter_pause_wait, _exit_pause_wait  # noqa: F401
from .durations import (  # noqa: F401
    _probe_durations_bulk,
    _probe_file_duration,
    backfill_missing_durations,
    count_missing_durations,
)
from .fetcher import fetch_metadata_for_videos, fetch_single_video_metadata
from .identity import backfill_video_ids, existing_info_ids
from .io import _folder_for_channel, _read_metadata_jsonl  # noqa: F401
from .normalize import _norm_title_for_match, _normalize_title_for_match  # noqa: F401
from .refresh_comments import refresh_channel_comments
from .refresh_fetch import fetch_channel_metadata
from .refresh_views import bulk_refresh_views_likes
from .scan import _read_info_json_vid, _scan_channel_videos  # noqa: F401
from .thumbnails_ops import (
    count_thumbnail_status_bulk,
    count_video_id_status,
    count_video_id_status_bulk,
    realign_misplaced_thumbnails,
    sweep_missing_thumbnails,
)

__all__ = [
    "fetch_single_video_metadata",
    "fetch_metadata_for_videos",
    "bulk_refresh_views_likes",
    "refresh_channel_comments",
    "fetch_channel_metadata",
    "sweep_missing_thumbnails",
    "realign_misplaced_thumbnails",
    "count_thumbnail_status_bulk",
    "count_video_id_status_bulk",
    "count_video_id_status",
    "backfill_video_ids",
    "existing_info_ids",
]
