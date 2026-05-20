"""
metadata.refresh_comments — per-channel comment refresh.

Extracted from metadata/refresh.py (Patch 22, v72.4).

Re-fetches full metadata so comments come back fresh for videos
uploaded recently (or any video whose comments the user wants
refreshed). The "Refresh comments" button on Settings → Metadata
drives this.
"""
from __future__ import annotations

import os
import re
import threading
import time
from datetime import datetime
from typing import Any

from ..log import get_logger
from ..log_stream import LogStreamer
from ..metadata_io import (
    _folder_for_channel,
    _read_metadata_jsonl,
    _write_metadata_jsonl,
)
from ..sync import find_yt_dlp
from ..text_utils import normalize_title as _canon_norm_title
from ..utils import sqlite_like_escape as _like_esc
from .fetcher import fetch_single_video_metadata
from .scan import _scan_channel_videos
from ._refresh_proxies import (
    _ID_RE,
    _enter_pause_wait,
    _exit_pause_wait,
    _flat_playlist_bulk_stats,
    _resolve_ids_by_title,
    backfill_video_ids,
)

_log = get_logger(__name__)


def refresh_channel_comments(channel: dict[str, Any],
                              stream: LogStreamer,
                              cancel_event: threading.Event | None = None,
                              pause_event: threading.Event | None = None,
                              only_recent_days: int | None = None,
                              queues=None,
                              ) -> dict[str, Any]:
    """Per-channel comment refresh. Re-fetches full metadata (via
    --dump-json --write-comments) for every on-disk video the
    channel has a metadata entry for. Motivating use case: videos
    caught within 30 min of upload typically have no "good"
    comments yet — this lets users pull a fresh top-50 a week later
    without re-fetching ALL metadata fields.

    `only_recent_days` optionally scopes to videos uploaded within
    the last N days (using the upload_date stored in the metadata
    entry) so a 4000-video channel doesn't take hours if you just
    want recent community updates. `None` = all videos.

    This is ALWAYS a slow path — comments require per-video API
    calls, no bulk mode exists — so the function is separate from
    bulk_refresh_views_likes (which is deliberately fast). Both
    can be run by the user independently; there's no dependency
    between them.

    Returns {ok, fetched, errors, skipped, took}.
    """
    folder = _folder_for_channel(channel)
    if folder is None:
        return {"ok": False, "error": "no output_dir"}
    yt = find_yt_dlp()
    if not yt:
        return {"ok": False, "error": "yt-dlp missing"}

    name = channel.get("name") or channel.get("folder") or "?"

    on_disk = _scan_channel_videos(folder)
    fp_by_id: dict[str, str] = {}
    for (_v, _t, _y, _m, _fp) in on_disk:
        if _v and _fp:
            fp_by_id[_v] = _fp

    # Collect every existing metadata entry. For recent-days scope,
    # filter by upload_date stored on the entry.
    # (video_id, filepath, title_hint, old_comments). Capturing
    # old_comments here lets us count "unchanged" videos after
    # the refetch without re-reading the JSONL.
    targets: list[tuple[str, str, str, list]] = []
    cutoff_yyyymmdd: str | None = None
    if only_recent_days and only_recent_days > 0:
        from datetime import timedelta as _td
        cutoff_yyyymmdd = (datetime.now() - _td(days=only_recent_days)
                           ).strftime("%Y%m%d")
    # Mid-channel resume support. When the task is paused mid-run and
    # the app is closed + reopened, the queue restores `current_sync`
    # back to the front of the sync queue and we land here again with
    # the same dict. Without this, we'd rebuild targets from i=1 and
    # silently re-fetch the videos we already did in the prior partial
    # pass. Track a `_pass_start_ts` on the task dict the FIRST time
    # we run; on subsequent resumptions of the same dict, filter out
    # any entry whose fetched_at >= _pass_start_ts (already refreshed
    # in this pass). Manual re-trigger = brand-new dict, so no skip.
    # NB: queues.set_current_sync uses copy.deepcopy(), so mutating
    # `channel` alone DOESN'T propagate to queues.current_sync and
    # therefore wouldn't survive a save_now. We re-call set_current_sync
    # with the mutated dict to push the new field through.
    _pass_start_ts: float = float(channel.get("_pass_start_ts") or 0.0)
    if _pass_start_ts <= 0:
        _pass_start_ts = time.time()
        channel["_pass_start_ts"] = _pass_start_ts
        if queues is not None:
            try:
                queues.set_current_sync(channel)
                queues.save_debounced()
            except Exception as e:
                _log.debug("swallowed: %s", e)
    def _entry_already_done_this_pass(entry: dict) -> bool:
        fa = entry.get("fetched_at") or ""
        if not fa:
            return False
        try:
            # ISO format from datetime.now().isoformat() — no tz, local.
            # On naive datetimes .timestamp() interprets as local time,
            # which means a DST transition during a multi-hour pass
            # shifts the comparison by ±3600s — skipping a video that
            # hadn't actually been refreshed, OR re-refreshing one
            # that had. Add a 1-hour tolerance to the comparison so
            # DST drift can't slip past it.
            ts = datetime.fromisoformat(str(fa)).timestamp()
        except (ValueError, TypeError):
            return False
        # Subtract 3600 so a comparison near a DST transition still
        # correctly classifies "fetched during THIS pass" — false
        # negatives (re-fetching) are acceptable; false positives
        # (skipping a real refresh) are the actual bug.
        return ts >= (_pass_start_ts - 3600)
    _skipped_already_done = 0
    for dp, _dns, fns in os.walk(str(folder)):
        for fn in fns:
            if not fn.endswith("Metadata.jsonl"):
                continue
            jp = os.path.join(dp, fn)
            for vid, entry in _read_metadata_jsonl(jp).items():
                if vid not in fp_by_id:
                    continue
                if cutoff_yyyymmdd:
                    ud = str(entry.get("upload_date") or "")
                    if not ud or ud < cutoff_yyyymmdd:
                        continue
                if _entry_already_done_this_pass(entry):
                    _skipped_already_done += 1
                    continue
                _title = str(entry.get("title") or "")
                _old_comments = entry.get("comments") or []
                targets.append((vid, fp_by_id[vid], _title, _old_comments))
    if _skipped_already_done > 0:
        stream.emit_dim(
            f"    — resuming: skipping {_skipped_already_done} video(s) "
            f"already refreshed in this pass")

    total = len(targets)
    if total == 0:
        stream.emit([[" \u2014 No videos match the comment-refresh "
                      "scope.\n", "dim"]])
        return {"ok": True, "fetched": 0, "errors": 0, "skipped": 0,
                "took": 0}

    # Sticky live-updating progress line \u2014 mirrors fetch_metadata_for_videos.
    # Each emission clears the previous "comments_refresh_active" line and
    # writes a new one at the bottom, so the user sees [N/total] tick up
    # in place. Without this, a 100-video channel takes ~15 minutes with
    # no UI feedback (Simple mode filters dim-tagged progress lines).
    import json as _json
    def _emit_active(_i: int, _n: int):
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "comments_refresh_active"}),
             "__control__"],
        ])
        stream.emit([
            ["    [", ["meta_bracket", "comments_refresh_active"]],
            [str(_i), ["simpleline", "comments_refresh_active"]],
            ["/", ["meta_bracket", "comments_refresh_active"]],
            [str(_n), ["simpleline", "comments_refresh_active"]],
            ["] ", ["meta_bracket", "comments_refresh_active"]],
            ["Refreshing comments: ", ["meta_bracket", "comments_refresh_active"]],
            [f"{name}\u2026\n", ["simpleline", "comments_refresh_active"]],
        ])

    def _clear_active():
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "comments_refresh_active"}),
             "__control__"],
        ])

    t0 = time.time()
    fetched = 0
    errors = 0
    unchanged = 0
    for i, (vid, fp, title_hint, old_comments) in enumerate(targets, 1):
        if cancel_event is not None and cancel_event.is_set():
            break
        if pause_event is not None and pause_event.is_set():
            _clear_active()
            _enter_pause_wait(stream,
                              f"{channel.get('name', '?')} (comments refresh)",
                              queues)
            while (pause_event.is_set()
                   and not (cancel_event is not None and cancel_event.is_set())):
                time.sleep(0.25)
            _exit_pause_wait(stream,
                             f"{channel.get('name', '?')} (comments refresh)",
                             queues)
        _emit_active(i, total)
        try:
            res = fetch_single_video_metadata(
                channel, vid, fp, title_hint, stream,
                emit_inline_log=False, refresh=True)
            if res.get("ok"):
                fetched += 1
                # Did the comments actually change? Python list-
                # of-dict equality is byte-exact; this catches
                # like-count updates AND comment add/remove.
                _new_comments = (res.get("entry") or {}).get("comments") or []
                if _new_comments == old_comments:
                    unchanged += 1
            elif not res.get("transient"):
                errors += 1
        except Exception:
            errors += 1
    _clear_active()

    # Stamp separate last-comments-refresh timestamp on the channel.
    try:
        from .. import ytarchiver_config as _cfg
        cfg = _cfg.load_config()
        ch_url_norm = (channel.get("url") or "").rstrip("/")
        now_ts = time.time()
        for ch in cfg.get("channels", []):
            if (ch.get("url") or "").rstrip("/") == ch_url_norm:
                ch["last_comments_refresh_ts"] = now_ts
                break
        _cfg.save_config(cfg)
    except Exception as e:
        _log.debug("swallowed: %s", e)

    took = time.time() - t0
    # Per-channel summary is rendered by the sync loop's `_sync_row_emit`
    # done-row in rich single-line form. No inline emit here.
    return {"ok": True, "fetched": fetched, "errors": errors,
            "unchanged": unchanged, "skipped": 0, "took": took}

