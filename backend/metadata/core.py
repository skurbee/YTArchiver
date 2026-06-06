"""
Metadata fetcher — writes OLD YTArchiver's aggregated format.

Output layout (must match YTArchiver.py exactly for drop-in replacement):

  .{ch_name} Metadata.jsonl (no split)
  {year}/.{ch_name} {year} Metadata.jsonl (year-split)
  {year}/{MM Month}/.{ch_name} {Month} {YY} Metadata.jsonl (year+month)

  Each file is a hidden (Windows HIDDEN attr) JSONL. One JSON per line,
  keyed by `video_id`. Dict schema per entry:
    {"video_id", "title", "description", "view_count", "like_count",
     "comment_count", "upload_date", "duration", "thumbnail_url",
     "comments":[{"author","text","likes","time"}, ...], "fetched_at"}

Thumbnails live next to the aggregated JSONL in a hidden `.Thumbnails/`
folder, one `.jpg` per video named `{title} [{video_id}].jpg`.

See YTArchiver.py:26539 (_get_metadata_jsonl_path), :26560 (read),
:26583 (write), :26719 (fetch per-video), :26784 (thumbnail download).
"""

from __future__ import annotations

import os
import re
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..log_stream import LogStreamer
from ..sync import _find_cookie_source, _startupinfo, find_yt_dlp

# YouTube IDs are 11 chars of [A-Za-z0-9_-]

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

_ID_RE = re.compile(r"[A-Za-z0-9_-]{11}")

# Shared with transcribe.py + reorg.py — see backend.utils.MONTH_FOLDERS.
from ..log import get_logger

# Patch fix (v68.2): `_like_esc` is referenced 4 times in this module
# (lines ~253, ~1461, ~2457, ~2878) for sqlite LIKE-pattern escaping
# of `\`, `%`, `_` in channel folder paths. The function was never
# defined module-locally; the call sites silently NameError'd, the
# enclosing try/except swallowed it, and `fp_to_id` ended up empty.
# Channels with OLD-style filenames (no `[id]` bracket) then had
# vid_id="" for every file, so the thumbnail-status check wrote 0 to
# the DB for every video — Settings > Metadata table showed "X 0%"
# for those channels permanently. Alias the canonical helper here.
from ..utils import sqlite_like_escape as _like_esc
from ..utils import utf8_subprocess_env as _utf8_env

_log = get_logger(__name__)

# Patch 6 (2026-05-17): thumbnail helpers extracted to thumbnails.py.
# Re-imported here so existing call sites inside metadata.py keep
# working unchanged.


# ── OLD-compat helpers ──────────────────────────────────────────────────
# the JSONL I/O + path/hide helpers below were moved
# to backend/metadata_io.py. They're re-imported into this module's
# namespace so existing call sites (e.g.
# `from backend.metadata import _read_metadata_jsonl`) keep working
# unchanged. The actual implementation now lives ONCE in metadata_io.
from ..metadata_io import (
    _folder_for_channel,
    _read_metadata_jsonl,
)

# Patch 19 phase M3 (v69.4): fetcher helpers moved to fetcher.py.
# Re-imported so internal callers in this file keep resolving them.
from .fetcher import (
    fetch_metadata_for_videos,
    fetch_single_video_metadata,
)

# Patch 19 phase M1 (v68.10): title-normalization wrappers moved out.
# Re-imported so internal callers in this file keep resolving them.
from .normalize import (
    _norm_title_for_match,
    _normalize_title_for_match,
)

# Patch 19 phase M6 (v69.4): refresh helpers moved to refresh.py.
# Re-imported so internal callers in this file keep resolving them.
from .refresh import (
    bulk_refresh_views_likes,
    fetch_channel_metadata,
    refresh_channel_comments,
)

# Patch 19 phase M2 (v69.1): scan helpers moved to scan.py.
from .scan import (
    _read_info_json_vid,
    _scan_channel_videos,
)


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




def _resolve_ids_by_title(yt: str, url: str,
                          unmatched_files: list[str],
                          stream: LogStreamer,
                          cancel_event: threading.Event | None = None,
                          pause_event: threading.Event | None = None
                          ) -> dict[str, str]:
    """Walk the channel's playlist ONCE to fetch (id, title) pairs,
    then match the unmatched filepaths to playlist entries by
    normalized title. Returns {filepath: video_id}.

    This is the fallback for files that were dropped into a channel
    folder without id-bearing filenames and without a normal sync
    pass (which is how the index DB normally learns the id). By
    matching on title we rescue them without requiring the user to
    rename files. Expensive enough (one yt-dlp playlist walk) that
    we only fire it when there's at least one unmatched file.
    """
    if not url or not unmatched_files:
        return {}
    try:
        proc = subprocess.Popen(
            [yt, "--flat-playlist",
             "--print", "%(id)s\t%(title)s",
             *_find_cookie_source(), url],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            encoding="utf-8", errors="replace",
            bufsize=1, startupinfo=_startupinfo,
            env=_utf8_env(),
        )
    except OSError:
        return {}
    # Collect playlist entries as title → list of ids so duplicate
    # titles (rare but possible — e.g. a re-uploaded video with the
    # same title as the original) don't silently overwrite each other.
    playlist: dict[str, list] = {}
    # `with proc.stdout` closes the pipe FD even on break/exception,
    # eliminating the per-call FD leak that accumulated across many
    # cancelled passes (audit: metadata/core.py H90).
    try:
        with proc.stdout:
            for line in proc.stdout:
                if cancel_event is not None and cancel_event.is_set():
                    try: proc.terminate()
                    except Exception as e: _log.debug("swallowed: %s", e)
                    break
                if pause_event is not None and pause_event.is_set():
                    try: proc.terminate()
                    except Exception as e: _log.debug("swallowed: %s", e)
                    break
                parts = line.rstrip().split("\t", 1)
                if len(parts) != 2:
                    continue
                vid, title = parts[0].strip(), parts[1].strip()
                if _ID_RE.fullmatch(vid) and title:
                    key = _normalize_title_for_match(title)
                    playlist.setdefault(key, []).append(vid)
        try: proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.terminate()
    except Exception:
        try: proc.terminate()
        except Exception as e: _log.debug("swallowed: %s", e)
    # Also group unmatched files by normalized title so we never
    # assign the same id to multiple files. a user's channel
    # case: `History of the iPhone (1).mp4`, `(2).mp4`, etc. — if we
    # stripped the `(N)` suffix they'd all collide onto one
    # playlist id and we'd silently duplicate. We don't strip, and
    # we ONLY match when BOTH sides are unambiguous (exactly one
    # file + exactly one playlist id for the same key).
    files_by_key: dict[str, list] = {}
    for fp in unmatched_files:
        stem = os.path.splitext(os.path.basename(fp))[0]
        key = _normalize_title_for_match(stem)
        files_by_key.setdefault(key, []).append(os.path.normpath(fp))

    # Title-match assigns the id even when it's already claimed by a
    # different file — this is exactly the "same YouTube video got
    # downloaded twice under different titles" case (YouTuber renamed
    # the video; the old download sits on disk with old title, new
    # download under new title). Both files get the same id, then
    # `prune_missing_videos` resolves the duplicate: keeps the row
    # with the largest `size_bytes` as the primary and marks the
    # others as duplicates (`is_duplicate_of=<primary filepath>`) so
    # the Browse grid hides them but the files stay on disk.
    out: dict[str, str] = {}
    for key, files in files_by_key.items():
        vids = playlist.get(key, [])
        if len(files) == 1 and len(vids) == 1:
            out[files[0]] = vids[0]
        # else: genuinely ambiguous (multiple files with same title AND
        # multiple playlist entries with same title) — skip.
    return out


# Patch 19 phase M1: _normalize_title_for_match moved to normalize.py.
# Imported via the package-level re-import block at top of this file.

_ID_RE_11 = re.compile(r"^[A-Za-z0-9_-]{11}$")


def _flat_playlist_bulk_stats(yt: str, ch_url: str,
                               stream: LogStreamer,
                               cancel_event: threading.Event | None = None,
                               pause_event: threading.Event | None = None,
                               queues=None,
                               progress_cb: Callable[[int], None] | None = None,
                               ) -> dict[str, dict[str, Any]]:
    """ONE yt-dlp --flat-playlist call returning per-video stats for
    the whole channel. Returns {video_id: {view_count, like_count,
    comment_count}} (None values where yt-dlp's flat-playlist path
    doesn't populate that field — YouTube reliably returns view_count
    but like_count / comment_count are often null in flat mode).

    This is the smart-refresh primitive: compared to the old path of
    `--dump-json` per video (one HTTP round-trip each), this folds
    an entire channel's view-count data into a single request. Users
    reported a 404-video channel taking ~1h17m under the per-video
    approach — the flat-playlist equivalent typically finishes in
    well under a minute.

    Caller decides what to do with the stats; see bulk_refresh_views_likes.
    """
    if not ch_url:
        return {}
    cmd = [
        yt,
        "--flat-playlist",
        "--lazy-playlist",
        "--no-warnings",
        "--skip-download",
        # CRITICAL (2026-05-14): without `skip=webpage`, yt-dlp ≥2026.x
        # returns "NA" for view_count / like_count / comment_count on
        # every entry in a channel's `/videos` tab. The library parses
        # the initial webpage payload by default and that payload no
        # longer carries per-video stats. `skip=webpage` forces yt-dlp
        # to use the InnerTube playlist endpoint instead, which DOES
        # include view_count. Without this, bulk_refresh_views_likes
        # was silently skipping every video because the "new" count
        # was None and `_view_new != _view_old` short-circuited to
        # False. Empirically: 0% of vids had view counts without it;
        # 83% return real exact view counts with it.
        # `skip=authcheck` is required IN COMBINATION when cookies are
        # passed (--cookies-from-browser firefox in our case). Without
        # it yt-dlp errors out: "Playlists that require authentication
        # may not extract correctly without a successful webpage
        # download". This pair is the supported workaround per
        # yt-dlp's own suggestion.
        "--extractor-args", "youtubetab:skip=webpage,authcheck",
        # TAB-separated so titles (which can contain pipes / commas)
        # never collide with the field separator. Title is included so
        # the caller can fall back to title-matching for legacy archive
        # files whose filenames lack [video_id] brackets AND aren't
        # registered in the videos-table with a video_id — the
        # default archive layout per tkinter-era downloads.
        "--print",
        # Extended with upload_date + duration so backfill_video_ids
        # can disambiguate title-near-duplicates using the file's
        # mtime (== YT upload date when yt-dlp ran with --mtime) and
        # the on-disk duration. Keeping it one pass so we don't
        # double the API traffic.
        "%(id)s\t%(view_count)s\t%(like_count)s\t%(comment_count)s\t%(title)s\t%(upload_date)s\t%(duration)s",
        *_find_cookie_source(),
        ch_url,
    ]
    # CAPTURE stderr instead of throwing it away. Earlier this was
    # `DEVNULL`, which meant when bulk-stats came back empty we had
    # zero diagnostic — the user just saw "Bulk-stats returned no data"
    # without any clue why. Now we drain stderr on a side thread and,
    # if the call returns empty, the caller can emit the captured
    # stderr as a verbose-only `dim` line so the user (in Verbose mode)
    # can see the real yt-dlp error.
    out: dict[str, dict[str, Any]] = {}
    _stderr_buf: list[str] = []
    try:
        proc = subprocess.Popen(
            cmd, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            encoding="utf-8", errors="replace", bufsize=1,
            startupinfo=_startupinfo, env=_utf8_env(),
        )
    except OSError as e:
        stream.emit_error(f"Couldn't start fetching video stats: {e}")
        return {}
    def _drain_stderr():
        try:
            for line in proc.stderr:
                if line:
                    _stderr_buf.append(line.rstrip())
        except Exception as e:
            _log.debug("swallowed: %s", e)
    _stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
    _stderr_thread.start()
    # Progress tick during the catalog fetch — on large channels (10k+
    # videos) this can take 30-60s and the caller's "Resolving video
    # IDs for X..." line otherwise looks frozen the whole time. Emit
    # every _PROGRESS_TICK_EVERY parsed rows OR every
    # _PROGRESS_TICK_SECS so the user sees something happening.
    _PROGRESS_TICK_EVERY = 500
    _PROGRESS_TICK_SECS = 5.0
    _tick_count = 0
    _last_tick_ts = time.time()
    # Hang-detect: read stdout on a side thread feeding a queue, and
    # let the main loop queue.get(timeout=) so a stalled yt-dlp (HTTP
    # read that never returns) can be detected and terminated instead
    # of blocking forever (audit: metadata/core.py:382-386).
    import queue as _queue
    _stdout_q: _queue.Queue = _queue.Queue(maxsize=1000)
    _STDOUT_SENTINEL = object()
    def _drain_stdout():
        try:
            for line in proc.stdout:
                _stdout_q.put(line)
        except Exception as e:
            _log.debug("swallowed: %s", e)
        finally:
            try: _stdout_q.put(_STDOUT_SENTINEL)
            except Exception: pass
    _stdout_thread = threading.Thread(target=_drain_stdout, daemon=True,
                                       name="yt-catalog-stdout-drain")
    _stdout_thread.start()
    # If no new line arrives for 60s, treat as stall and terminate.
    _STALL_TIMEOUT_S = 60.0
    try:
        while True:
            try:
                raw = _stdout_q.get(timeout=_STALL_TIMEOUT_S)
            except _queue.Empty:
                # Stalled — give up and terminate the subprocess.
                stream.emit_dim(
                    f" (catalog walk stalled — no output for "
                    f"{int(_STALL_TIMEOUT_S)}s, terminating)")
                try: proc.terminate()
                except Exception as e: _log.debug("swallowed: %s", e)
                break
            if raw is _STDOUT_SENTINEL:
                break
            if cancel_event is not None and cancel_event.is_set():
                try: proc.terminate()
                except Exception as e: _log.debug("swallowed: %s", e)
                break
            # Honor pause without dropping the subprocess — same
            # pattern as _fetch_yt_catalog in redownload.py.
            if pause_event is not None and pause_event.is_set():
                _enter_pause_wait(stream, "catalog walk", queues)
                while (pause_event.is_set()
                       and not (cancel_event is not None and cancel_event.is_set())):
                    time.sleep(0.25)
                _exit_pause_wait(stream, "catalog walk", queues)
            line = (raw or "").rstrip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 4:
                continue
            vid = parts[0].strip()
            if not _ID_RE_11.fullmatch(vid):
                continue
            _tick_count += 1
            _now = time.time()
            if (_tick_count % _PROGRESS_TICK_EVERY == 0
                    or (_now - _last_tick_ts) >= _PROGRESS_TICK_SECS):
                # If a caller (e.g. bulk_refresh_views_likes) provided
                # a progress callback, fold the count into THEIR active
                # heartbeat line instead of emitting a separate
                # "Fetched N from catalog" line. That way the user sees
                # ONE updating line per channel, not two side-by-side.
                if progress_cb is not None:
                    try: progress_cb(_tick_count)
                    except Exception as e: _log.debug("swallowed: %s", e)
                else:
                    # In-place update on a single line ("backfill_progress"
                    # is registered in logs.js _inplaceKind so each emit
                    # with this marker replaces the previous one instead
                    # of appending). Cleared by clear_line when the final
                    # summary emits so the transient counter doesn't
                    # persist after completion.
                    try:
                        stream.emit([[f"  \u2014 Fetched {_tick_count:,} videos "
                                     f"from YouTube catalog\u2026\n",
                                     ["simpleline", "backfill_progress"]]])
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                _last_tick_ts = _now
            def _num(s: str) -> int | None:
                s = (s or "").strip()
                if not s or s in ("NA", "None", "null"):
                    return None
                try:
                    return int(float(s))
                except (TypeError, ValueError):
                    return None
            _title = parts[4].strip() if len(parts) >= 5 else ""
            out[vid] = {
                "view_count": _num(parts[1]),
                "like_count": _num(parts[2]),
                "comment_count": _num(parts[3]),
                "title": _title,
                # New fields (Colbert backfill fix): upload_date and
                # duration for non-title disambiguation in
                # backfill_video_ids. yt-dlp emits upload_date as
                # YYYYMMDD (or "NA" if unknown); duration as seconds.
                "upload_date": (parts[5].strip()
                                if len(parts) >= 6 else ""),
                "duration": _num(parts[6]) if len(parts) >= 7 else None,
            }
        try: proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.terminate()
            # Reap the zombie so it doesn't linger until GC (audit:
            # metadata/core.py L42). 5s cap matches our other
            # terminate-then-wait patterns.
            try: proc.wait(timeout=5)
            except Exception: pass
    except Exception as e:
        stream.emit_dim(f" (stats read error: {e})")
        try:
            proc.terminate()
            try: proc.wait(timeout=5)
            except Exception: pass
        except Exception as e: _log.debug("swallowed: %s", e)
    # If the call returned nothing useful, surface whatever yt-dlp put
    # on stderr as a verbose-only line so users in Verbose mode can
    # actually debug the failure. Simple mode users still just see the
    # higher-level "Initial check unsuccessful..." line emitted by the
    # caller. Cap at 6 lines so a yt-dlp traceback doesn't flood the
    # log; if the user needs more they can re-run with Verbose mode and
    # check the streamed stderr in the terminal.
    if not out and _stderr_buf:
        try:
            _stderr_thread.join(timeout=0.5)
        except Exception as e:
            _log.debug("swallowed: %s", e)
        _trimmed = [ln for ln in _stderr_buf if ln.strip()][:6]
        for _ln in _trimmed:
            stream.emit([
                ["   — yt-dlp: ", ["dim"]],
                [_ln + "\n", ["dim"]],
            ])

    # AUTO-RETRY for @handle URLs that fail bulk-stats. Discovered
    # 2026-05-15: yt-dlp 2026.03.17 + `youtubetab:skip=webpage,authcheck`
    # can't resolve some channel @handles (ColdFusion specifically:
    # "Failed to resolve url" error), but the same channel works via
    # the canonical /channel/UC.../videos URL form. The skip=webpage
    # arg is REQUIRED for bulk view counts (without it every entry's
    # view_count is "NA"), so we can't just drop the arg. Instead:
    # when the call returns empty AND the URL is the @handle form,
    # spend 1 extra yt-dlp call to resolve the channel_id, then retry
    # the bulk-stats call against /channel/UC.../videos. Saves the
    # 25+ minute per-video fallback for ColdFusion (and any other
    # channel where the handle path fails).
    if not out and "/@" in (ch_url or ""):
        canonical = _resolve_channel_id_url(yt, ch_url)
        if canonical and canonical != ch_url:
            stream.emit([
                ["   — ", ["dim"]],
                [f"retrying bulk-stats with canonical channel URL "
                 f"({canonical})\n", ["dim"]],
            ])
            # Recursive call into ourselves with the canonical URL.
            # Will not recurse twice because the canonical URL doesn't
            # contain /@ — so the retry guard above won't fire again.
            return _flat_playlist_bulk_stats(
                yt, canonical, stream, cancel_event, pause_event,
                queues=queues, progress_cb=progress_cb)
    return out


def _resolve_channel_id_url(yt: str, handle_url: str) -> str:
    """Convert a `/@handle` channel URL to the canonical
    `/channel/UC.../videos` form by asking yt-dlp for one video's
    channel_id. Returns empty string on failure.

    Costs one yt-dlp invocation (~2-4s) — used only as a one-off retry
    when bulk-stats fails for the handle form. Most channels never hit
    this path because their @handle resolves cleanly.
    """
    if not handle_url or not yt:
        return ""
    try:
        proc = subprocess.run(
            [yt, "--skip-download", "--no-warnings",
             "--print", "%(channel_id)s",
             "--playlist-end", "1",
             *_find_cookie_source(),
             handle_url],
            capture_output=True, text=True, timeout=20,
            encoding="utf-8", errors="replace",
            startupinfo=_startupinfo, env=_utf8_env(),
        )
        cid = (proc.stdout or "").strip().split("\n", 1)[0].strip()
        if cid and cid.startswith("UC") and len(cid) >= 20:
            return f"https://www.youtube.com/channel/{cid}/videos"
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return ""


from .thumbnails_ops import (
    count_thumbnail_status_bulk,
    count_video_id_status,
    count_video_id_status_bulk,
    realign_misplaced_thumbnails,
    sweep_missing_thumbnails,
)

# Patch 19 phase M1: _norm_title_for_match moved to normalize.py.


def _probe_file_duration(filepath: str) -> float | None:
    """Single-file ffprobe call returning duration in seconds. None on
    any error. Used by _probe_durations_bulk to fill `videos.duration_s`
    for files that came from the tkinter-era importer (which never
    probed duration, leaving NULL across the board).
    """
    try:
        proc = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries",
             "format=duration", "-of",
             "default=noprint_wrappers=1:nokey=1", filepath],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            startupinfo=_startupinfo, env=_utf8_env(),
            timeout=10, encoding="utf-8", errors="replace")
    except Exception:
        return None
    raw = (proc.stdout or "").strip()
    try:
        v = float(raw)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _probe_durations_bulk(filepaths: list[str], stream: LogStreamer,
                          cancel_event: threading.Event | None = None,
                          pause_event: threading.Event | None = None,
                          max_workers: int = 6,
                          ) -> dict[str, float | None]:
    """Probe duration for a batch of files in parallel.

    Reason this exists: backfill_video_ids' duration-match strategy
    needs `local_dur` to disambiguate same-day same-title YT
    candidates. The tkinter-era importer never populated
    `videos.duration_s`, so on migrated archives every duration is
    NULL — strategies that compare against duration get zero data
    and fail silently. This helper fills the gap with one ffprobe
    call per file, ~70ms each, parallelized 6-wide → ~12s for 1000
    files. Results write back to `videos.duration_s` so subsequent
    runs skip the probe entirely (the SELECT in the caller pulls
    them out of the DB).
    """
    out: dict[str, float | None] = {}
    if not filepaths:
        return out
    from concurrent.futures import ThreadPoolExecutor, as_completed
    _t0 = time.time()
    _total = len(filepaths)
    try:
        stream.emit([[f"  — Probing duration for {_total:,} file(s)"
                     f" via ffprobe…\n",
                     ["simpleline", "backfill_progress"]]])
    except Exception as e:
        _log.debug("swallowed: %s", e)
    _last_tick = time.time()
    _done = 0
    with ThreadPoolExecutor(max_workers=max_workers,
                            thread_name_prefix="dur-probe") as ex:
        fut_to_fp = {ex.submit(_probe_file_duration, fp): fp
                     for fp in filepaths}
        for fut in as_completed(fut_to_fp):
            fp = fut_to_fp[fut]
            try:
                out[fp] = fut.result()
            except Exception:
                out[fp] = None
            _done += 1
            if cancel_event is not None and cancel_event.is_set():
                break
            _now = time.time()
            if (_now - _last_tick) >= 1.5 and _done < _total:
                try:
                    stream.emit([[f"  — Probing duration "
                                 f"[{_done:,}/{_total:,}]…\n",
                                 ["simpleline", "backfill_progress"]]])
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                _last_tick = _now
    # Persist probed durations to the DB so the next pass doesn't
    # re-probe the same files. One transaction, write only the
    # successful probes (None values stay NULL — re-probing them is
    # cheap and might succeed if the file was being written during
    # the first attempt).
    try:
        from .. import index as _idx
        _conn = _idx._open()
        if _conn is not None:
            with _idx._db_lock:
                for _fp, _d in out.items():
                    if _d is None or _d <= 0:
                        continue
                    try:
                        _conn.execute(
                            "UPDATE videos SET duration_s=? "
                            "WHERE filepath=? COLLATE NOCASE "
                            "AND (duration_s IS NULL OR duration_s<=0)",
                            (_d, _fp))
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                _conn.commit()
    except Exception as e:
        _log.debug("swallowed: %s", e)
    try:
        _resolved_n = sum(1 for v in out.values() if v and v > 0)
        stream.emit([[f"  — Probed {_resolved_n:,}/{_total:,} duration(s)"
                     f" in {time.time() - _t0:.1f}s\n",
                     ["simpleline", "backfill_progress"]]])
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return out


def count_missing_durations() -> int:
    """How many archived videos have no stored duration_s. Read-only."""
    try:
        from .. import index as _idx
        conn = _idx._reader_open()
        if conn is None:
            return 0
        with _idx._reader_lock:
            return int(conn.execute(
                "SELECT COUNT(*) FROM videos "
                "WHERE duration_s IS NULL OR duration_s<=0").fetchone()[0])
    except Exception as e:
        _log.debug("count_missing_durations failed: %s", e)
        return 0


def backfill_missing_durations(stream: LogStreamer,
                               cancel_event: threading.Event | None = None,
                               pause_event: threading.Event | None = None,
                               ) -> dict:
    """Fill videos.duration_s for every archived file that's missing it by
    ffprobing the file locally (no YouTube). The on-disk file is the only
    accurate source — the disk-sweep/import paths register rows without a
    duration. Idempotent: only touches rows still NULL/0, so a cancelled run
    resumes cleanly on the next start. Progress + the actual duration_s
    writes are handled by _probe_durations_bulk. Returns
    {ok, total, resolved, cancelled}."""
    from .. import index as _idx
    conn = _idx._reader_open()
    if conn is None:
        return {"ok": False, "error": "index unavailable",
                "total": 0, "resolved": 0}
    with _idx._reader_lock:
        rows = conn.execute(
            "SELECT filepath FROM videos "
            "WHERE (duration_s IS NULL OR duration_s<=0) "
            "AND filepath IS NOT NULL AND filepath!='' "
            "ORDER BY rowid").fetchall()
    filepaths = [r[0] for r in rows if r and r[0]]
    total = len(filepaths)
    if not total:
        try:
            stream.emit([["  — Every video already has a length. Nothing to "
                          "do.\n", "simpleline"]])
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return {"ok": True, "total": 0, "resolved": 0, "cancelled": False}
    try:
        stream.emit([[f" Checking video lengths — {total:,} missing. "
                      f"Reading each file with ffprobe…\n", "header"]])
    except Exception as e:
        _log.debug("swallowed: %s", e)
    probed = _probe_durations_bulk(filepaths, stream, cancel_event, pause_event)
    resolved = sum(1 for v in probed.values() if v and v > 0)
    cancelled = bool(cancel_event and cancel_event.is_set())
    try:
        _verb = "Stopped — " if cancelled else "Done — "
        stream.emit([[f" {_verb}filled {resolved:,} of {total:,} video "
                      f"length(s)" + (" (re-run to finish the rest)."
                      if cancelled or resolved < total else ".") + "\n",
                      "simpleline_green"]])
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return {"ok": True, "total": total, "resolved": resolved,
            "cancelled": cancelled}


def _fetch_per_video_upload_dates(yt: str, vids: list[str],
                                   stream: LogStreamer,
                                   cancel_event: threading.Event | None = None,
                                   pause_event: threading.Event | None = None,
                                   max_workers: int = 4,
                                   queues=None,
                                   ) -> dict[str, str]:
    """For each vid, run a per-video yt-dlp extraction to get the real
    upload_date (YYYYMMDD). Flat-playlist returns "NA" for upload_date,
    so this is the slow-but-thorough path that THOROUGH backfill mode
    uses to enable date-confirmed matching for unresolved files.

    Parallelized 4-wide to stay under YouTube's rate-limit. ~3s/vid
    sequentially → ~0.75s/vid wall-clock with 4 workers. Caller
    chooses the candidate shortlist; this helper just iterates.

    Returns {vid: "YYYYMMDD" or ""}. Failures are recorded as "" so
    the caller can tell "tried but didn't get a date" from "never tried".
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    out: dict[str, str] = {}
    if not vids:
        return out
    _total = len(vids)
    _t0 = time.time()

    def _fetch_one(vid: str) -> tuple[str, str]:
        url = f"https://www.youtube.com/watch?v={vid}"
        cmd = [yt, "--skip-download", "--no-warnings",
               "--print", "%(upload_date)s",
               *_find_cookie_source(), url]
        try:
            proc = subprocess.run(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                startupinfo=_startupinfo, env=_utf8_env(),
                timeout=30, encoding="utf-8", errors="replace")
        except Exception:
            return (vid, "")
        raw = (proc.stdout or "").strip()
        # Accept first valid YYYYMMDD on any line (yt-dlp may emit
        # multiple lines for live/upcoming videos).
        for line in raw.splitlines():
            line = line.strip()
            if line and len(line) == 8 and line.isdigit():
                return (vid, line)
        return (vid, "")

    _last_tick = time.time()
    _done = 0
    try:
        stream.emit([[f"  — Fetching upload_date for {_total:,} candidate"
                     f"(s) (thorough pass)…\n",
                     ["simpleline", "backfill_progress"]]])
    except Exception as e:
        _log.debug("swallowed: %s", e)
    with ThreadPoolExecutor(max_workers=max_workers,
                            thread_name_prefix="ud-fetch") as ex:
        fut_to_vid = {ex.submit(_fetch_one, v): v for v in vids}
        for fut in as_completed(fut_to_vid):
            if cancel_event is not None and cancel_event.is_set():
                break
            if pause_event is not None and pause_event.is_set():
                # Worker threads don't honor pause mid-call (they're
                # blocking on subprocess.run), but the AS_COMPLETED
                # loop can hold off scheduling new work — practical
                # effect is a brief delay before pause takes hold.
                while (pause_event.is_set()
                       and not (cancel_event is not None
                                and cancel_event.is_set())):
                    time.sleep(0.25)
            try:
                vid, date = fut.result()
                out[vid] = date
            except Exception as e:
                _log.debug("swallowed: %s", e)
            _done += 1
            _now = time.time()
            if (_now - _last_tick) >= 2.0 and _done < _total:
                _ok = sum(1 for v in out.values() if v)
                try:
                    stream.emit([[f"  — Thorough fetch "
                                 f"[{_done:,}/{_total:,}] · "
                                 f"{_ok:,} dates resolved…\n",
                                 ["simpleline", "backfill_progress"]]])
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                _last_tick = _now
    try:
        _ok_n = sum(1 for v in out.values() if v)
        stream.emit([[f"  — Per-video date fetch: {_ok_n:,}/{_total:,}"
                     f" resolved in {time.time() - _t0:.1f}s\n",
                     ["simpleline", "backfill_progress"]]])
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return out


def backfill_video_ids(channel: dict[str, Any],
                       stream: LogStreamer,
                       cancel_event: threading.Event | None = None,
                       pause_event: threading.Event | None = None,
                       queues=None,
                       mode: str = "fast",
                       ) -> dict[str, Any]:
    """One-shot video_id backfill with multi-strategy resolution.

    `mode="fast"` (default): use yt-dlp --flat-playlist once for the
    catalog (NA upload_date but real duration), ffprobe local files
    for duration, then run the title + duration strategies. The
    date-confirmed strategies still run but contribute ~0 because
    flat-playlist returns NA dates — they're harmless. Typical
    runtime 30-120s for a 1000-file channel.

    `mode="thorough"`: after the fast pass, take every file still
    unresolved, build a token-prefiltered shortlist of YT candidate
    vids per file, do a per-video yt-dlp call for each candidate
    vid (~3s sequential, ~0.75s 4-wide parallel) to fetch real
    upload_date, then re-run the date-confirmed strategies. Adds
    minutes-to-hours depending on unresolved count, but resolves
    the rename-heavy + same-duration-collision case that fast
    can't (e.g. daily late-night shows with constant ~13min run
    time).

    For every on-disk file without a video_id in the DB, try in order:

      1. `.info.json` sidecar (zero-cost, no network)
      2. Exact normalized-title match against YouTube's current
         flat-playlist
      3. Duration match (NEW): unique YT vid whose duration is
         within ±2s of the local file's ffprobe'd duration. The
         channel-level "EWU Bodycam" case where renamed titles
         leave 26% via exact match alone but durations are unique
         to the second is the textbook win for this strategy.
      4. Substring title match (local title subset of YT title, or
         vice-versa) — requires date confirmation, so silent on
         fast mode but contributes in thorough mode
      5. Upload-date match (file mtime YYYYMMDD → YT upload_date),
         disambiguated by duration when >1 candidate exists
      6. Fuzzy title match via difflib.get_close_matches (0.80
         cutoff, rejects ambiguous near-ties; high-confidence-no-
         date escape at 0.95)
      7. Stamp `id_backfill_tried_ts` so the UI can distinguish
         "tried but genuinely unresolvable" from "not yet attempted"

    Returns {ok, resolved, resolved_by, already_set, ambiguous,
             unresolved_now_tried, took, mode}.
    """
    folder = _folder_for_channel(channel)
    if folder is None:
        stream.emit_error("Archive folder isn't configured. Set it in Settings → General.")
        return {"ok": False, "error": "no output_dir"}
    yt = find_yt_dlp()
    if not yt:
        stream.emit_error("Can't look up missing video IDs — the download tool (yt-dlp) isn't installed.")
        return {"ok": False, "error": "yt-dlp missing"}
    name = channel.get("name") or channel.get("folder") or "?"
    ch_url = (channel.get("url") or "").strip()
    if not ch_url:
        stream.emit_error(f"{name} has no channel URL on file — can't look up missing video IDs.")
        return {"ok": False, "error": "no url"}

    stream.emit([["  \u2014 ", "meta_bracket"],
                 [f"Resolving video IDs for {name}...\n", "simpleline"]])

    t0 = time.time()
    bulk = _flat_playlist_bulk_stats(yt, ch_url, stream,
                                     cancel_event, pause_event)
    if not bulk:
        stream.emit([
            [" \u26A0 ", "meta_bracket"],
            [f"YouTube returned no video list for {name}.\n", "simpleline"],
        ])
        return {"ok": False, "error": "bulk_empty"}

    # ── Build indices over the YT catalog ────────────────────────────

    # 1. exact normalized title → vid, with ambiguity tracking
    title_to_vid: dict[str, str] = {}
    title_ambiguous: set = set()
    # 2. upload_date (YYYYMMDD) → [(vid, norm_title, duration_s)]
    date_to_cands: dict[str, list[tuple[str, str, float | None]]] = {}
    # 3. token → set of vids (for fuzzy prefilter)
    token_to_vids: dict[str, set] = {}
    # Keep a dense list of (vid, norm_title) for difflib.get_close_matches
    norm_titles: list[str] = []
    norm_title_to_vid: dict[str, str] = {}
    # Map vid → duration (seconds) for disambiguation
    vid_to_duration: dict[str, float | None] = {}
    # Inverse: integer-seconds duration → list of vids. The
    # duration-match strategy (Strategy 3) hits this. Bucketed at
    # 1-second resolution; the match function then sweeps ±2s
    # buckets to handle re-encode drift.
    duration_bucket_to_vids: dict[int, list[str]] = {}

    for _vid, _stats in bulk.items():
        # YT catalog can legitimately list the same video twice (rare
        # unlisted-then-relisted case). Skip if we already indexed it
        # so Strategy 3's bucket lists don't end up with duplicate
        # vids (audit: metadata/core.py:805-838).
        if _vid in vid_to_duration:
            continue
        _raw_title = _stats.get("title") or ""
        _nt = _norm_title_for_match(_raw_title)
        _upload = (_stats.get("upload_date") or "").strip()
        _dur = _stats.get("duration")
        try:
            _dur_f = float(_dur) if _dur is not None else None
        except (TypeError, ValueError):
            _dur_f = None
        vid_to_duration[_vid] = _dur_f
        if _nt:
            if _nt in title_to_vid and title_to_vid[_nt] != _vid:
                title_ambiguous.add(_nt)
            else:
                title_to_vid[_nt] = _vid
            if _nt not in norm_title_to_vid:
                norm_titles.append(_nt)
                norm_title_to_vid[_nt] = _vid
            for _tok in _nt.split():
                if len(_tok) >= 3:
                    token_to_vids.setdefault(_tok, set()).add(_vid)
        if _upload and len(_upload) == 8 and _upload.isdigit():
            date_to_cands.setdefault(_upload, []).append(
                (_vid, _nt, _dur_f))
        if _dur_f is not None and _dur_f > 0:
            duration_bucket_to_vids.setdefault(
                int(round(_dur_f)), []).append(_vid)

    # ── Scan on-disk videos + resolution passes ──────────────────────

    # Pull local duration_s from the index DB in one query (avoids
    # ffprobe per file). Populated by register_video for fresh
    # downloads; older rows may be NULL.
    _local_durations: dict[str, float | None] = {}
    try:
        from .. import index as _idx
        # Pure read — reader connection avoids queueing behind sweep.
        _conn_pre = _idx._reader_open()
        if _conn_pre is not None:
            _pat = _like_esc(str(folder)) + "%"
            with _idx._reader_lock:
                for _row in _conn_pre.execute(
                        "SELECT filepath, duration_s FROM videos "
                        "WHERE filepath LIKE ? ESCAPE '\\'", (_pat,)):
                    _local_durations[os.path.normpath(_row[0])] = _row[1]
    except Exception as e:
        _log.debug("swallowed: %s", e)

    on_disk = _scan_channel_videos(folder)
    already_set = 0
    ambiguous_hits = 0
    unresolved = 0
    resolved_by = {"info_json": 0, "exact": 0, "duration": 0,
                   "substring": 0, "date": 0, "fuzzy": 0,
                   "thorough_substring": 0, "thorough_date": 0,
                   "thorough_fuzzy": 0}
    to_backfill: list[tuple[str, str, str]] = []  # (filepath, vid, how)
    # Files that failed every strategy — stamp the tried timestamp.
    tried_failed_paths: list[str] = []

    # ── Local duration backfill (always runs in both modes) ──────────
    # Tkinter-era archives have NULL duration_s on every migrated row,
    # which kills the new duration-match strategy AND strategy 4's
    # multi-candidate disambiguation. ffprobe what's missing — write
    # back to the DB so subsequent runs skip the probe.
    _files_without_vid = [_fp for (_v, _t, _y, _m, _fp) in on_disk
                          if not _v]
    _files_needing_probe = [_fp for _fp in _files_without_vid
                            if _local_durations.get(os.path.normpath(_fp))
                            is None]
    if _files_needing_probe:
        _probed = _probe_durations_bulk(_files_needing_probe, stream,
                                         cancel_event=cancel_event,
                                         pause_event=pause_event)
        for _fp, _d in _probed.items():
            if _d is not None and _d > 0:
                _local_durations[os.path.normpath(_fp)] = _d

    # Helper used by Strategy 3 (duration match). Returns the unique
    # YT vid whose duration is within ±2s of the local file's, OR ""
    # if zero / multiple candidates exist (with title-similarity
    # tiebreak for the multi case when there's a clear winner).
    def _find_duration_match(local_dur: float | None,
                              needle_nt: str) -> str:
        if local_dur is None or local_dur <= 0:
            return ""
        # Sweep ±2 second buckets around the local duration.
        _center = int(round(local_dur))
        cands: list[str] = []
        for _off in (-2, -1, 0, 1, 2):
            for _v in duration_bucket_to_vids.get(_center + _off, []):
                _ydur = vid_to_duration.get(_v)
                if _ydur is None:
                    continue
                if abs(_ydur - local_dur) <= 2.0:
                    cands.append(_v)
        if not cands:
            return ""
        # De-dup (a vid could land in two adjacent buckets via int
        # rounding) but keep order so the "first match" path is stable.
        seen: set = set()
        uniq: list[str] = []
        for _v in cands:
            if _v not in seen:
                seen.add(_v)
                uniq.append(_v)
        if len(uniq) == 1:
            return uniq[0]
        # Multiple duration-near-ties. Only accept if there's a clear
        # title-similarity winner — otherwise fall through to other
        # strategies. The "rather have missing info than incorrect
        # info" rule applies here too.
        if not needle_nt:
            return ""
        from difflib import SequenceMatcher
        scored = []
        for _v in uniq:
            _yt = _norm_title_for_match(bulk.get(_v, {}).get("title") or "")
            if not _yt:
                continue
            _r = SequenceMatcher(None, needle_nt, _yt).ratio()
            scored.append((_v, _r))
        if not scored:
            return ""
        scored.sort(key=lambda r: r[1], reverse=True)
        # Need top ≥ 0.50 AND ≥ 0.15 clear of #2.
        if scored[0][1] < 0.50:
            return ""
        if len(scored) >= 2 and (scored[0][1] - scored[1][1]) < 0.15:
            return ""
        return scored[0][0]

    def _days_diff(d1: str, d2: str) -> int | None:
        """Return |d1 - d2| in days for two YYYYMMDD strings. None on
        any parse error or if either is empty."""
        if not d1 or not d2 or len(d1) != 8 or len(d2) != 8:
            return None
        try:
            import datetime as _dt
            dt1 = _dt.datetime.strptime(d1, "%Y%m%d").date()
            dt2 = _dt.datetime.strptime(d2, "%Y%m%d").date()
            return abs((dt1 - dt2).days)
        except Exception:
            return None

    # Rule: rather have missing info than incorrect info. Any
    # title-based strategy (substring or fuzzy) that
    # picks a candidate MUST also have an upload_date within
    # _DATE_WINDOW_DAYS of the local file's mtime. yt-dlp's
    # --mtime sets file mtime to the upload date so these should
    # match exactly; ±1 day covers timezone drift without opening
    # the door to "similar title, different video" collisions.
    _DATE_WINDOW_DAYS = 1

    def _date_confirms(vid: str, local_day: str) -> bool:
        """True when the candidate vid's upload_date is within
        _DATE_WINDOW_DAYS of the local file's day. Missing date on
        either side = reject (conservative — absence of evidence is
        not evidence of a match)."""
        if not local_day:
            return False
        _ud = (bulk.get(vid, {}).get("upload_date") or "").strip()
        diff = _days_diff(local_day, _ud)
        return diff is not None and diff <= _DATE_WINDOW_DAYS

    def _find_substring_match(needle_nt: str, local_day: str) -> str:
        """Walk the candidate list looking for a SINGLE candidate whose
        normalized title contains the needle, or vice-versa. Length
        ratio must be >=0.7 so "the" doesn't match every video.
        Result must also pass the date window check."""
        if not needle_nt or len(needle_nt) < 5:
            return ""
        # Short-circuit via token prefilter: need at least 2 shared
        # tokens of length >=3 to even consider.
        needle_tokens = [t for t in needle_nt.split() if len(t) >= 3]
        if len(needle_tokens) < 2:
            return ""
        from collections import Counter as _Counter
        counter: _Counter = _Counter()
        for _tok in needle_tokens:
            if _tok in token_to_vids:
                for _v in token_to_vids[_tok]:
                    counter[_v] += 1
        _candidate_vids = [v for v, n in counter.items() if n >= 2]
        hits = []
        for _v in _candidate_vids:
            _cnt = bulk.get(_v) or {}
            _cnt_nt = _norm_title_for_match(_cnt.get("title") or "")
            if not _cnt_nt:
                continue
            _short, _long = sorted([len(needle_nt), len(_cnt_nt)])
            if _long == 0 or _short / _long < 0.7:
                continue
            if needle_nt in _cnt_nt or _cnt_nt in needle_nt:
                hits.append(_v)
                if len(hits) > 1:
                    break  # multiple hits — fall through to date filter
        if not hits:
            return ""
        if len(hits) == 1:
            # Single title hit — still require date agreement so we
            # don't accept a rename that happens to share a substring.
            return hits[0] if _date_confirms(hits[0], local_day) else ""
        # Multiple substring hits — tiebreak by date. Need exactly
        # one candidate to land inside the date window.
        date_hits = [v for v in hits if _date_confirms(v, local_day)]
        return date_hits[0] if len(date_hits) == 1 else ""

    def _find_fuzzy_match(needle_nt: str, local_day: str) -> str:
        """Fuzzy match via difflib. Returns '' unless (a) there's a
        clear winner above the cutoff AND (b) its upload_date agrees
        with the local file's mtime day. Multiple near-tie matches
        fall through to a date-based tiebreak (rather than being
        rejected outright): if more than 1 hit, determine which is
        correct based on date."""
        if not needle_nt or len(needle_nt) < 5:
            return ""
        # Prefilter by shared tokens so we don't run SequenceMatcher
        # against every title in a 10K-video channel.
        needle_tokens = [t for t in needle_nt.split() if len(t) >= 3]
        if len(needle_tokens) < 2:
            return ""
        from collections import Counter as _Counter
        counter: _Counter = _Counter()
        for _tok in needle_tokens:
            if _tok in token_to_vids:
                for _v in token_to_vids[_tok]:
                    counter[_v] += 1
        _shortlist_vids = [v for v, n in counter.items() if n >= 2]
        if not _shortlist_vids:
            return ""
        # Detect titles that collide (two different vids normalize to
        # the same title — re-uploads, "Part 1" duplicates, daily-show
        # repeats). Drop those from the candidate set rather than
        # arbitrarily picking the last-written vid, which would risk
        # stamping the wrong video_id onto an unrelated local file.
        # Mirrors Strategy 2's title_ambiguous discipline.
        _title_to_vid_local: dict[str, str] = {}
        _ambiguous_titles: set[str] = set()
        for _v in _shortlist_vids:
            _t = bulk.get(_v, {}).get("title") or ""
            _nt = _norm_title_for_match(_t)
            if not _nt:
                continue
            if _nt in _title_to_vid_local and _title_to_vid_local[_nt] != _v:
                _ambiguous_titles.add(_nt)
                continue
            _title_to_vid_local[_nt] = _v
        # Strip ambiguous titles from both the lookup and the search set.
        for _amb in _ambiguous_titles:
            _title_to_vid_local.pop(_amb, None)
        _shortlist_titles = [t for t in _title_to_vid_local.keys()]
        from difflib import SequenceMatcher, get_close_matches
        # Ask for more matches than before (5 instead of 3) so the
        # date-based tiebreak has room to operate when several
        # similar-ish titles pass the ratio cutoff.
        matches = get_close_matches(needle_nt, _shortlist_titles,
                                     n=5, cutoff=0.80)
        if not matches:
            return ""
        # Score everything up front so both the date-approved path
        # AND the high-confidence-no-date escape can reuse the
        # ratios without re-running SequenceMatcher.
        scored: list[tuple[str, float]] = []  # (vid, ratio)
        for _m in matches:
            _v = _title_to_vid_local.get(_m)
            if not _v:
                continue
            _r = SequenceMatcher(None, needle_nt, _m).ratio()
            scored.append((_v, _r))
        if not scored:
            return ""

        # Apply date filter to ALL candidates above the cutoff. This
        # is the core of the "date tiebreak when titles are similar"
        # rule — a title that fuzzy-matches several videos
        # only resolves if exactly one of them also lines up on
        # upload date.
        date_approved = [(v, r) for (v, r) in scored
                         if _date_confirms(v, local_day)]
        if len(date_approved) == 1:
            return date_approved[0][0]
        if len(date_approved) >= 2:
            # Multiple pass title + date. Take the highest ratio,
            # but only if >=0.05 clear of the next — otherwise too
            # close to call and we decline rather than guess.
            date_approved.sort(key=lambda r: r[1], reverse=True)
            if date_approved[0][1] - date_approved[1][1] >= 0.05:
                return date_approved[0][0]
            return ""

        # High-confidence-no-date escape: when the date-approved
        # path found nothing, fall back to accepting
        # a match if there's exactly ONE candidate with ratio
        # >= 0.95. That similarity is basically "near-identical
        # string"; two different videos rarely hit 0.95 on the
        # normalized-title form. Useful when a file's mtime was
        # bumped (re-encode, tool touch, missing --mtime on old
        # downloads) so the date check rejects an otherwise-
        # obvious match. Any ambiguity at the 0.95 threshold and
        # we bail — the conservative principle still holds.
        _HIGH_CONF = 0.95
        _high = [(v, r) for (v, r) in scored if r >= _HIGH_CONF]
        if len(_high) == 1:
            return _high[0][0]
        return ""

    def _find_date_match(filepath: str,
                          local_dur: float | None) -> str:
        """Match by file mtime YYYYMMDD == YT upload_date. When
        multiple YT videos land on the same day, disambiguate by
        duration (within 2s)."""
        try:
            _mtime = os.path.getmtime(filepath)
        except OSError:
            return ""
        try:
            import datetime as _dt
            # Use UTC, not local time. yt-dlp --mtime sets the file
            # mtime to YT upload time in UTC; converting through
            # local time misclassified the day for files uploaded
            # near a day boundary in the user's timezone (Central
            # Time = 6h offset) — audit: metadata/core.py:1116-1119.
            _day = _dt.datetime.utcfromtimestamp(_mtime).strftime("%Y%m%d")
        except Exception:
            return ""
        # Try the exact UTC day first, then ±1 day to catch any
        # timezone-related drift. Strategy 5 has its own duration
        # disambiguation downstream so wider candidate sets are safe.
        cands = list(date_to_cands.get(_day, []))
        try:
            _d_obj = _dt.datetime.strptime(_day, "%Y%m%d")
            for _delta in (-1, 1):
                _adj = (_d_obj + _dt.timedelta(days=_delta)).strftime("%Y%m%d")
                cands.extend(date_to_cands.get(_adj, []))
        except Exception:
            pass
        if not cands:
            return ""
        if len(cands) == 1:
            return cands[0][0]
        if local_dur is None or local_dur <= 0:
            return ""
        best = ""
        best_diff = 3.0  # must match within 2s (strict); 3s threshold
        for (_v, _nt, _yd) in cands:
            if _yd is None or _yd <= 0:
                continue
            _diff = abs(_yd - local_dur)
            if _diff < best_diff:
                best_diff = _diff
                best = _v
            elif abs(_diff - best_diff) < 0.5 and best:
                # Two videos same day, near-equal duration — ambiguous.
                return ""
        return best

    # Per-file progress tick — the real time sink on a 10k-video
    # channel is this loop (fuzzy shortlist iteration). Emit every
    # _MATCH_TICK_EVERY files OR _MATCH_TICK_SECS so the log reflects
    # ongoing work. Also note progress right before the loop starts
    # so the user sees the transition from "fetching catalog" to
    # "matching files".
    _MATCH_TICK_EVERY = 200
    _MATCH_TICK_SECS = 5.0
    _match_total = len(on_disk)
    if _match_total > 0:
        try:
            # Same "backfill_progress" marker so this transition
            # line REPLACES the last "Fetched N videos..." tick
            # (both phases share one in-place line).
            stream.emit([[f"  \u2014 Catalog has {len(bulk):,} videos \u00b7 "
                         f"matching {_match_total:,} local file(s)\u2026\n",
                         ["simpleline", "backfill_progress"]]])
        except Exception as e:
            _log.debug("swallowed: %s", e)
    _match_processed = 0
    _match_last_tick = time.time()

    for (_v, _t, _y, _m, _fp) in on_disk:
        if cancel_event is not None and cancel_event.is_set():
            break
        if pause_event is not None and pause_event.is_set():
            _enter_pause_wait(stream,
                              f"{channel.get('name', '?')} (ID backfill)",
                              queues)
            while (pause_event.is_set()
                   and not (cancel_event is not None and cancel_event.is_set())):
                time.sleep(0.25)
            _exit_pause_wait(stream,
                             f"{channel.get('name', '?')} (ID backfill)",
                             queues)
        # Bump the tick BEFORE any continue branch so every file
        # counted toward progress, regardless of which strategy
        # path it took (skipped, resolved, or unresolved).
        _match_processed += 1
        _now = time.time()
        if (_match_total > 1000
                and (_match_processed % _MATCH_TICK_EVERY == 0
                     or (_now - _match_last_tick) >= _MATCH_TICK_SECS)
                and _match_processed < _match_total):
            try:
                _so_far = sum(resolved_by.values())
                stream.emit([[f"  \u2014 [{_match_processed:,}/"
                             f"{_match_total:,}] matched {_so_far:,} "
                             f"so far\u2026\n",
                             ["simpleline", "backfill_progress"]]])
            except Exception as e:
                _log.debug("swallowed: %s", e)
            _match_last_tick = _now
        if _v:
            already_set += 1
            continue

        # Compute file's mtime day once — used by every title-based
        # strategy as a safety check: if 2 videos have very similar
        # titles that could cause issues, and more than 1 hit comes
        # back, determine which is correct based on
        # date?"). yt-dlp's --mtime sets file mtime to the YT upload
        # date so this should be an exact match when the file is
        # untouched.
        _local_day = ""
        try:
            import datetime as _dt
            # UTC to match the canonical reader at core.py:1174.
            # Mixed UTC/local readers route the same mtime into two
            # different day buckets at TZ boundaries, causing
            # double-write to two JSONLs (audit: core.py H80).
            _local_day = _dt.datetime.utcfromtimestamp(
                os.path.getmtime(_fp)).strftime("%Y%m%d")
        except Exception:
            _local_day = ""

        # Strategy 1: info.json sidecar
        _side_vid = _read_info_json_vid(_fp)
        if _side_vid:
            to_backfill.append((_fp, _side_vid, "info_json"))
            resolved_by["info_json"] += 1
            continue

        _nt = _norm_title_for_match(_t)

        # Strategy 2: exact normalized title — no date check since
        # an exact normalized title collision is already strong
        # evidence; adding date would just shrink the coverage.
        if _nt and _nt not in title_ambiguous:
            _exact = title_to_vid.get(_nt, "")
            if _exact:
                to_backfill.append((_fp, _exact, "exact"))
                resolved_by["exact"] += 1
                continue

        # Strategy 3 (NEW): duration match. Flat-playlist gives us
        # CLEAN per-second durations on every YT video; ffprobe gives
        # us clean local durations. When the local duration uniquely
        # matches one YT video within ±2s, accept — no date check
        # needed. For the EWU Bodycam case (~1 full + few shorts/day,
        # wildly different durations) this is the textbook win.
        _local_dur = _local_durations.get(os.path.normpath(_fp))
        if _local_dur is not None and _local_dur > 0:
            _by_dur = _find_duration_match(_local_dur, _nt)
            if _by_dur:
                to_backfill.append((_fp, _by_dur, "duration"))
                resolved_by["duration"] += 1
                continue

        # Strategy 4: substring (date-checked)
        if _nt:
            _sub = _find_substring_match(_nt, _local_day)
            if _sub:
                to_backfill.append((_fp, _sub, "substring"))
                resolved_by["substring"] += 1
                continue

        # Strategy 5: date (single-candidate, or duration-disambiguated)
        _by_date = _find_date_match(_fp, _local_dur)
        if _by_date:
            to_backfill.append((_fp, _by_date, "date"))
            resolved_by["date"] += 1
            continue

        # Strategy 6: fuzzy difflib (date-checked)
        if _nt:
            _fuzzy = _find_fuzzy_match(_nt, _local_day)
            if _fuzzy:
                to_backfill.append((_fp, _fuzzy, "fuzzy"))
                resolved_by["fuzzy"] += 1
                continue

        # Track ambiguous vs genuinely unresolvable
        if _nt and _nt in title_ambiguous:
            ambiguous_hits += 1
        else:
            unresolved += 1
        tried_failed_paths.append(_fp)

    # ── Thorough mode: per-video upload_date fetch for unresolved ────
    # Only runs when mode == "thorough" AND there's something the
    # fast pass couldn't resolve. Builds a candidate-vid union from
    # the token shortlists of each unresolved file's normalized title,
    # then fetches per-video upload_date for those (typically far
    # fewer than the full catalog). With real upload_dates in hand,
    # re-runs strategies 4-6 only for the unresolved files.
    _thorough_attempted = False
    if (mode == "thorough" and tried_failed_paths
            and not (cancel_event is not None and cancel_event.is_set())):
        _thorough_attempted = True
        # Snapshot unresolved files' data (we no longer have the
        # _t, _y, _m, _fp tuple at this point).
        _unresolved_meta: list[tuple[str, str, str]] = []
        # Re-derive (filepath, norm_title, local_day) for each.
        _unres_set = set(tried_failed_paths)
        for (_v0, _t0, _y0, _m0, _fp0) in on_disk:
            if _fp0 not in _unres_set:
                continue
            _nt0 = _norm_title_for_match(_t0)
            try:
                import datetime as _dt
                # UTC (audit: core.py H80) — same fix as line 1275.
                _ld0 = _dt.datetime.utcfromtimestamp(
                    os.path.getmtime(_fp0)).strftime("%Y%m%d")
            except Exception:
                _ld0 = ""
            _unresolved_meta.append((_fp0, _nt0, _ld0))

        # Union of token-shortlists across all unresolved files.
        # Each unresolved file contributes up to ~20 candidates;
        # heavy token overlap keeps the union much smaller than
        # (#unresolved * 20).
        _candidate_vids: set = set()
        for (_fp0, _nt0, _ld0) in _unresolved_meta:
            if not _nt0:
                continue
            _toks = [t for t in _nt0.split() if len(t) >= 3]
            if len(_toks) < 2:
                continue
            from collections import Counter as _Counter
            _c: _Counter = _Counter()
            for _tok in _toks:
                if _tok in token_to_vids:
                    for _vc in token_to_vids[_tok]:
                        _c[_vc] += 1
            # Threshold ≥ 2 shared tokens (same as substring/fuzzy
            # prefilter). Cap at top 20 per file to avoid runaway
            # candidate sets on titles with many common tokens.
            _shortlist = [v for v, n in _c.most_common(20) if n >= 2]
            _candidate_vids.update(_shortlist)
        # Also include vids that share a duration bucket with any
        # unresolved file's local duration — captures the case
        # where Strategy 3 had a multi-match and bailed without a
        # clear title winner. Adds dates so Strategy 5 might bite.
        for (_fp0, _nt0, _ld0) in _unresolved_meta:
            _ld = _local_durations.get(os.path.normpath(_fp0))
            if _ld is None or _ld <= 0:
                continue
            _ctr = int(round(_ld))
            for _off in (-2, -1, 0, 1, 2):
                _candidate_vids.update(
                    duration_bucket_to_vids.get(_ctr + _off, []))
        # Strip vids that already have a valid 8-digit upload_date
        # (no point re-fetching). The previous boolean expression
        # was `not (X) or Y` — Python precedence made empty-date vids
        # WRONGLY excluded from the candidate set, which were exactly
        # the ones that needed re-fetching most.
        def _has_valid_date(v: str) -> bool:
            _d = (bulk.get(v, {}).get("upload_date") or "").strip()
            return len(_d) == 8 and _d.isdigit()
        _candidate_vids = {v for v in _candidate_vids
                           if not _has_valid_date(v)}
        _cand_list = sorted(_candidate_vids)
        if _cand_list:
            _yt = find_yt_dlp()
            _fetched = _fetch_per_video_upload_dates(
                _yt, _cand_list, stream,
                cancel_event=cancel_event, pause_event=pause_event,
                queues=queues)
            # Patch upload_date back into `bulk` and rebuild
            # date_to_cands so the date-checked strategies have
            # data to work with.
            for _vid, _date in _fetched.items():
                if _date and len(_date) == 8 and _date.isdigit():
                    if _vid in bulk:
                        bulk[_vid]["upload_date"] = _date
                    date_to_cands.setdefault(_date, []).append(
                        (_vid,
                         _norm_title_for_match(
                             bulk.get(_vid, {}).get("title") or ""),
                         vid_to_duration.get(_vid)))

            # Re-run strategies 4-6 for unresolved files with new dates.
            _still_unresolved: list[str] = []
            for (_fp0, _nt0, _ld0) in _unresolved_meta:
                if cancel_event is not None and cancel_event.is_set():
                    _still_unresolved.append(_fp0)
                    continue
                _ldur = _local_durations.get(os.path.normpath(_fp0))
                # Strategy 4 retry (substring + real date)
                if _nt0:
                    _sub2 = _find_substring_match(_nt0, _ld0)
                    if _sub2:
                        to_backfill.append((_fp0, _sub2, "thorough_substring"))
                        resolved_by["thorough_substring"] += 1
                        continue
                # Strategy 5 retry (date + duration)
                _by_date2 = _find_date_match(_fp0, _ldur)
                if _by_date2:
                    to_backfill.append((_fp0, _by_date2, "thorough_date"))
                    resolved_by["thorough_date"] += 1
                    continue
                # Strategy 6 retry (fuzzy + real date)
                if _nt0:
                    _fz2 = _find_fuzzy_match(_nt0, _ld0)
                    if _fz2:
                        to_backfill.append((_fp0, _fz2, "thorough_fuzzy"))
                        resolved_by["thorough_fuzzy"] += 1
                        continue
                _still_unresolved.append(_fp0)
            # Update counters now that some unresolved files got
            # resolved via the thorough pass.
            _newly_resolved_thorough = (resolved_by["thorough_substring"]
                                        + resolved_by["thorough_date"]
                                        + resolved_by["thorough_fuzzy"])
            unresolved = max(0, unresolved - _newly_resolved_thorough)
            tried_failed_paths = _still_unresolved

    resolved = sum(resolved_by.values())

    # ── Persist: UPDATE resolved rows + stamp tried-failed rows ──────

    _now_ts = time.time()
    try:
        from .. import index as _idx
        conn = _idx._open()
        if conn is not None:
            with _idx._db_lock:
                for _fp, _vid, _how in to_backfill:
                    _vurl = f"https://www.youtube.com/watch?v={_vid}"
                    # Try the path AS-GIVEN first; if rowcount=0,
                    # fall back to normpath form. register_video
                    # stores normpath'd paths, but the on-disk walk
                    # producing this filepath might already match,
                    # might not. Trying both forms covers legacy rows
                    # whose stored filepath was inserted before
                    # register_video added normpath.
                    _np = os.path.normpath(_fp)
                    try:
                        _cur = conn.execute(
                            "UPDATE videos SET video_id=?, video_url=?, "
                            "id_backfill_tried_ts=? "
                            "WHERE filepath=? COLLATE NOCASE "
                            "AND (video_id IS NULL OR video_id='')",
                            (_vid, _vurl, _now_ts, _np))
                        if (_cur.rowcount or 0) == 0 and _fp != _np:
                            conn.execute(
                                "UPDATE videos SET video_id=?, video_url=?, "
                                "id_backfill_tried_ts=? "
                                "WHERE filepath=? COLLATE NOCASE "
                                "AND (video_id IS NULL OR video_id='')",
                                (_vid, _vurl, _now_ts, _fp))
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                # Stamp tried-ts on rows that failed every strategy
                # so the UI can tell the user these are probably
                # genuinely unresolvable (title changed too much,
                # channel renamed them, etc.). Same path-form fallback
                # as above.
                for _fp in tried_failed_paths:
                    _np = os.path.normpath(_fp)
                    try:
                        _cur = conn.execute(
                            "UPDATE videos SET id_backfill_tried_ts=? "
                            "WHERE filepath=? COLLATE NOCASE "
                            "AND (video_id IS NULL OR video_id='')",
                            (_now_ts, _np))
                        if (_cur.rowcount or 0) == 0 and _fp != _np:
                            conn.execute(
                                "UPDATE videos SET id_backfill_tried_ts=? "
                                "WHERE filepath=? COLLATE NOCASE "
                                "AND (video_id IS NULL OR video_id='')",
                                (_now_ts, _fp))
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                conn.commit()
    except Exception as _e:
        stream.emit_error(f"Couldn't save the recovered video IDs: {_e}")

    took = time.time() - t0
    _parts = []
    if resolved:
        _parts.append(f"{resolved} resolved")
        _breakdown_bits = []
        if resolved_by["info_json"]:
            _breakdown_bits.append(f"{resolved_by['info_json']} .info.json")
        if resolved_by["exact"]:
            _breakdown_bits.append(f"{resolved_by['exact']} exact")
        if resolved_by["duration"]:
            _breakdown_bits.append(f"{resolved_by['duration']} duration")
        if resolved_by["substring"]:
            _breakdown_bits.append(f"{resolved_by['substring']} substring")
        if resolved_by["date"]:
            _breakdown_bits.append(f"{resolved_by['date']} date+dur")
        if resolved_by["fuzzy"]:
            _breakdown_bits.append(f"{resolved_by['fuzzy']} fuzzy")
        _thorough_total = (resolved_by["thorough_substring"]
                           + resolved_by["thorough_date"]
                           + resolved_by["thorough_fuzzy"])
        if _thorough_total:
            _breakdown_bits.append(f"{_thorough_total} thorough")
        if _breakdown_bits:
            _parts.append("(" + ", ".join(_breakdown_bits) + ")")
    if already_set:
        _parts.append(f"{already_set} already set")
    if ambiguous_hits:
        _parts.append(f"{ambiguous_hits} ambiguous")
    if unresolved:
        _parts.append(f"{unresolved} unresolved")
    if not _parts:
        _parts.append("no on-disk videos")
    _summary = " \u00b7 ".join(_parts)
    # Clear the sticky "backfill_progress" in-place line so the
    # transient counter ("Fetched 1,000 videos from YouTube
    # catalog...") doesn't linger beside the final summary.
    try:
        import json as _json
        stream.emit([[_json.dumps({
            "kind": "clear_line", "marker": "backfill_progress"}),
            "__control__"]])
    except Exception as e:
        _log.debug("swallowed: %s", e)
    # Final summary tag: pink on newly-resolved work, plain-white
    # simpleline otherwise. The dim tag made "already-set + some
    # unresolved" summaries look faded even though they're a
    # normal successful outcome — no new work to do because every
    # video was already set. Only fall back to dim if there was
    # literally nothing to report (no on-disk videos).
    if resolved:
        _tag = "simpleline_pink"
    elif _parts == ["no on-disk videos"]:
        _tag = "dim"
    else:
        _tag = "simpleline"
    stream.emit([
        [" \u2014 ", "meta_bracket"],
        [f"{name}: {_summary} (took {took:.1f}s)\n", _tag],
    ])
    return {
        "ok": True,
        "resolved": resolved,
        "resolved_by": resolved_by,
        "already_set": already_set,
        "ambiguous": ambiguous_hits,
        "unresolved": unresolved,
        "unresolved_now_tried": len(tried_failed_paths),
        "took": took,
        "mode": mode,
        "thorough_attempted": _thorough_attempted,
    }




def existing_info_ids(folder: Path) -> set:
    """Deprecated: scanned legacy per-video `.info.json` sidecars.
    Returns a set of video IDs found via aggregated metadata JSONLs."""
    if not isinstance(folder, Path):
        folder = Path(folder)
    if not folder.is_dir():
        return set()
    found = set()
    # Walk for .{ch_name} Metadata.jsonl files regardless of channel name
    for dp, _dns, fns in os.walk(str(folder)):
        for fn in fns:
            if fn.startswith(".") and fn.endswith("Metadata.jsonl"):
                found.update(_read_metadata_jsonl(os.path.join(dp, fn)).keys())
    # Also keep compat with any leftover per-video .info.json files
    bracket_re = re.compile(r"\[([A-Za-z0-9_-]{11})\]")
    for dp, _dns, fns in os.walk(str(folder)):
        for fn in fns:
            if not fn.endswith(".info.json"):
                continue
            m = bracket_re.findall(fn)
            if m:
                found.add(m[-1])
    return found
