"""
metadata._refresh_proxies — lazy proxy wrappers for helpers living in
metadata/core.py.

PEP 562 module __getattr__ does NOT fire for LOAD_GLOBAL lookups inside
function bodies (only for attribute access on the module from outside),
so we use explicit proxy functions that lazy-import core at call time.
By then both modules are fully loaded so the circular reference is
harmless.

Imported by refresh_views.py, refresh_comments.py, refresh_fetch.py.
"""
from __future__ import annotations

import re

_ID_RE = re.compile(r"[A-Za-z0-9_-]{11}")
_ID_RE_11 = re.compile(r"^[A-Za-z0-9_-]{11}$")


def _flat_playlist_bulk_stats(*args, **kwargs):
    from .core import _flat_playlist_bulk_stats as _impl
    return _impl(*args, **kwargs)


def _resolve_ids_by_title(*args, **kwargs):
    from .core import _resolve_ids_by_title as _impl
    return _impl(*args, **kwargs)


def _resolve_channel_id_url(*args, **kwargs):
    from .core import _resolve_channel_id_url as _impl
    return _impl(*args, **kwargs)


def _fetch_per_video_upload_dates(*args, **kwargs):
    from .core import _fetch_per_video_upload_dates as _impl
    return _impl(*args, **kwargs)


def _probe_file_duration(*args, **kwargs):
    from .core import _probe_file_duration as _impl
    return _impl(*args, **kwargs)


def _probe_durations_bulk(*args, **kwargs):
    from .core import _probe_durations_bulk as _impl
    return _impl(*args, **kwargs)


def backfill_video_ids(*args, **kwargs):
    from .core import backfill_video_ids as _impl
    return _impl(*args, **kwargs)


def existing_info_ids(*args, **kwargs):
    from .core import existing_info_ids as _impl
    return _impl(*args, **kwargs)


def _enter_pause_wait(*args, **kwargs):
    from .core import _enter_pause_wait as _impl
    return _impl(*args, **kwargs)


def _exit_pause_wait(*args, **kwargs):
    from .core import _exit_pause_wait as _impl
    return _impl(*args, **kwargs)
