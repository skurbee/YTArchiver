"""YouTube catalog and upload-date probes, independent of refresh orchestration."""

from __future__ import annotations

import os
import re
import subprocess
import threading
import time
from collections.abc import Callable
from typing import Any

from .. import youtube_traffic
from ..executor_utils import WorkResult, run_bounded
from ..log import get_logger
from ..log_stream import LogStreamer
from ..process_runner import popen_ytdlp, run_ytdlp, supervise_streaming_process
from ..subprocess_util import make_startupinfo
from ..utils import utf8_subprocess_env as _utf8_env
from ..ytdlp_options import _find_cookie_source
from .control import _enter_pause_wait, _exit_pause_wait
from .normalize import _normalize_title_for_match

_log = get_logger(__name__)
_startupinfo = make_startupinfo()
_ID_RE = re.compile(r"[A-Za-z0-9_-]{11}")
_ID_RE_11 = re.compile(r"^[A-Za-z0-9_-]{11}$")

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
    permission = youtube_traffic.acquire(
        "metadata_title_resolve", cancel_event=cancel_event, stream=stream)
    if not permission.get("ok"):
        return {}
    try:
        proc = popen_ytdlp(
            [yt, "--flat-playlist",
             "--print", "%(id)s\t%(title)s",
             *_find_cookie_source(), url],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            encoding="utf-8", errors="replace",
            bufsize=1, startupinfo=_startupinfo,
            env=_utf8_env(),
            request_cancel_event=cancel_event, request_pause_event=pause_event,
        )
    except OSError:
        return {}
    # Collect playlist entries as title → list of ids so duplicate
    # titles (rare but possible — e.g. a re-uploaded video with the
    # same title as the original) don't silently overwrite each other.
    playlist: dict[str, list] = {}
    session_failed = threading.Event()

    class StopTitleWalk:
        # Title matching pauses by terminating; bulk stats suspend consumption.
        def is_set(self) -> bool:
            return any(event is not None and event.is_set()
                       for event in (cancel_event, pause_event, session_failed))

    def consume_title(line: str) -> None:
        try:
            from ..youtube_session import handle_youtube_failure_text
            if handle_youtube_failure_text(line, context="matching local files to YouTube"):
                session_failed.set()
                return
        except Exception as exc:
            _log.debug("title-match YouTube guard failed: %s", exc)
        parts = line.rstrip().split("\t", 1)
        if len(parts) != 2:
            return
        vid, title = parts[0].strip(), parts[1].strip()
        if _ID_RE.fullmatch(vid) and title:
            playlist.setdefault(_normalize_title_for_match(title), []).append(vid)

    captured = supervise_streaming_process(
        proc, on_stdout_line=consume_title, cancel_event=StopTitleWalk(),
        idle_timeout=60.0, role="metadata_title_match")
    if not captured.output_complete or captured.returncode != 0:
        # A partial playlist cannot prove a title is unique: a later row may
        # contain the duplicate which makes this match unsafe.
        return {}
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
    permission = youtube_traffic.acquire(
        "metadata_channel_catalog", cancel_event=cancel_event, stream=stream)
    if not permission.get("ok"):
        return {}
    try:
        proc = popen_ytdlp(
            cmd, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            encoding="utf-8", errors="replace", bufsize=1,
            startupinfo=_startupinfo, env=_utf8_env(),
            request_cancel_event=cancel_event, request_pause_event=pause_event,
        )
    except OSError as e:
        stream.emit_error(f"Couldn't start fetching video stats: {e}")
        return {}
    _PROGRESS_TICK_EVERY = 500
    _PROGRESS_TICK_SECS = 5.0
    _tick_count = 0
    _last_tick_ts = time.time()

    def _consume_catalog_line(raw: str) -> None:
        nonlocal _tick_count, _last_tick_ts
        line = (raw or "").rstrip()
        if not line:
            return
        parts = line.split("\t")
        if len(parts) < 4:
            return
        vid = parts[0].strip()
        if not _ID_RE_11.fullmatch(vid):
            return
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
            # New fields for same-title backfill: upload_date and
            # duration for non-title disambiguation in
            # backfill_video_ids. yt-dlp emits upload_date as
            # YYYYMMDD (or "NA" if unknown); duration as seconds.
            "upload_date": (parts[5].strip()
                            if len(parts) >= 6 else ""),
            "duration": _num(parts[6]) if len(parts) >= 7 else None,
        }

    captured = supervise_streaming_process(
        proc, on_stdout_line=_consume_catalog_line, on_stderr_line=_stderr_buf.append,
        cancel_event=cancel_event, pause_event=pause_event, idle_timeout=60.0,
        on_pause_change=lambda paused: (
            _enter_pause_wait if paused else _exit_pause_wait)(stream, "catalog walk", queues),
        role="metadata_catalog")
    _walk_complete = captured.output_complete
    if captured.timed_out:
        stream.emit_dim(" (catalog walk stalled — no output for 60s, terminating)")
    _stderr_text = "\n".join(_stderr_buf)
    _failure_kind = ""
    if _stderr_text:
        try:
            from ..youtube_session import handle_youtube_failure_text
            _failure_kind = handle_youtube_failure_text(
                _stderr_text,
                context="fetching a channel catalog",
                stream=stream,
                pause_event=pause_event,
                queues=queues,
            )
        except Exception as e:
            _log.debug("catalog YouTube-failure classification failed: %s", e)

    # If the call returned nothing useful, surface whatever yt-dlp put
    # on stderr as a verbose-only line so users in Verbose mode can
    # actually debug the failure. Simple mode users still just see the
    # higher-level "Initial check unsuccessful..." line emitted by the
    # caller. Cap at 6 lines so a yt-dlp traceback doesn't flood the
    # log; if the user needs more they can re-run with Verbose mode and
    # check the streamed stderr in the terminal.
    if not out and _stderr_buf:
        _trimmed = [ln for ln in _stderr_buf if ln.strip()][:6]
        for _ln in _trimmed:
            stream.emit([
                ["   — yt-dlp: ", ["dim"]],
                [_ln + "\n", ["dim"]],
            ])

    # AUTO-RETRY for @handle URLs that fail bulk-stats. Discovered
    # 2026-05-15: yt-dlp 2026.03.17 + `youtubetab:skip=webpage,authcheck`
    # can't resolve some channel @handles ("Failed to resolve url"),
    # but the same channel works via
    # the canonical /channel/UC.../videos URL form. The skip=webpage
    # arg is REQUIRED for bulk view counts (without it every entry's
    # view_count is "NA"), so we can't just drop the arg. Instead:
    # when the call returns empty AND the URL is the @handle form,
    # spend 1 extra yt-dlp call to resolve the channel_id, then retry
    # the bulk-stats call against /channel/UC.../videos. Saves the
    # slow per-video fallback for any channel where the handle path fails.
    if not out and not _failure_kind and "/@" in (ch_url or ""):
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
    # Attach the completeness flag out-of-band (dict subclass) so the
    # by-id mapping shape every consumer iterates stays untouched.
    class _BulkResult(dict):
        """Bulk stats mapping with an attached completion flag."""

        complete = False
        cookie_auth_required = False
        rate_limited = False
    _res = _BulkResult(out)
    try:
        _res.complete = bool(_walk_complete and proc.returncode == 0)
    except Exception:
        _res.complete = False
    _res.cookie_auth_required = _failure_kind == "cookie"
    _res.rate_limited = _failure_kind == "rate_limit"
    return _res

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
    permission = youtube_traffic.acquire("channel_id_resolve")
    if not permission.get("ok"):
        return ""
    try:
        proc = run_ytdlp(
            [yt, "--skip-download", "--no-warnings",
             "--print", "%(channel_id)s",
             "--playlist-end", "1",
             *_find_cookie_source(),
             handle_url],
            capture_output=True, text=True, timeout=20,
            encoding="utf-8", errors="replace",
            startupinfo=_startupinfo, env=_utf8_env(),
        )
        try:
            from ..youtube_session import handle_youtube_failure_text
            if handle_youtube_failure_text(
                    proc.stderr or "",
                    context="resolving a YouTube channel"):
                return ""
        except Exception as e:
            _log.debug("channel resolve YouTube guard failed: %s", e)
        cid = (proc.stdout or "").strip().split("\n", 1)[0].strip()
        if cid and cid.startswith("UC") and len(cid) >= 20:
            return f"https://www.youtube.com/channel/{cid}/videos"
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return ""

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
    out: dict[str, str] = {}
    if not vids:
        return out
    _total = len(vids)
    _t0 = time.time()

    def _fetch_one(vid: str) -> tuple[str, str]:
        # Honor pause/cancel HERE — executor worker threads keep
        # pulling queued tasks regardless of what the as_completed
        # loop does (as_completed only OBSERVES futures), so this is
        # the only place pause can actually stop new fetches from
        # launching and cancel can stop the queue from draining at
        # full network rate.
        if cancel_event is not None and cancel_event.is_set():
            return (vid, "")
        while pause_event is not None and pause_event.is_set():
            if cancel_event is not None and cancel_event.is_set():
                return (vid, "")
            time.sleep(0.25)
        url = f"https://www.youtube.com/watch?v={vid}"
        cmd = [yt, "--skip-download", "--no-warnings",
               "--print", "%(upload_date)s",
               *_find_cookie_source(), url]
        permission = youtube_traffic.acquire(
            "upload_date", cancel_event=cancel_event, stream=stream)
        if not permission.get("ok"):
            return (vid, "")
        try:
            proc = run_ytdlp(
                cmd, capture_output=True,
                startupinfo=_startupinfo, env=_utf8_env(),
                timeout=30, encoding="utf-8", errors="replace",
                request_cancel_event=cancel_event, request_pause_event=pause_event)
        except Exception:
            return (vid, "")
        try:
            from ..youtube_session import handle_youtube_failure_text
            if handle_youtube_failure_text(
                    proc.stderr or "",
                    context="fetching a YouTube upload date"):
                return (vid, "")
        except Exception as e:
            _log.debug("upload-date YouTube guard failed: %s", e)
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
    def _cancelled() -> bool:
        return bool(cancel_event is not None and cancel_event.is_set())

    def _record_date(result: WorkResult[str, tuple[str, str]]) -> None:
        nonlocal _done, _last_tick
        if result.error is not None or result.value is None:
            out[result.item] = ""
            if result.error is not None:
                _log.debug("upload-date worker failed: %s", result.error)
        else:
            vid, date = result.value
            out[vid] = date
        _done += 1
        _now = time.time()
        if (_now - _last_tick) >= 2.0 and _done < _total:
            _ok = sum(1 for value in out.values() if value)
            try:
                stream.emit([[f"  — Thorough fetch "
                             f"[{_done:,}/{_total:,}] · "
                             f"{_ok:,} dates resolved…\n",
                             ["simpleline", "backfill_progress"]]])
            except Exception as e:
                _log.debug("swallowed: %s", e)
            _last_tick = _now

    run_bounded(
        vids,
        _fetch_one,
        _record_date,
        max_workers=max_workers,
        thread_name_prefix="ud-fetch",
        is_cancelled=_cancelled,
    )
    try:
        _ok_n = sum(1 for v in out.values() if v)
        stream.emit([[f"  — Per-video date fetch: {_ok_n:,}/{_total:,}"
                     f" resolved in {time.time() - _t0:.1f}s\n",
                     ["simpleline", "backfill_progress"]]])
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return out
