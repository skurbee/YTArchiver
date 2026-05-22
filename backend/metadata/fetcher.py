"""
metadata.fetcher — per-video yt-dlp metadata fetches.

Patch 19 phase M3 (v69.4): extracted from metadata/legacy.py.

Public surface (re-imported into legacy.py):
    _fetch_video_metadata(yt, video_id, title_hint)
        Single yt-dlp --dump-json call. Returns OLD-schema dict or None.
        Has the 3-attempt retry-with-backoff and the {"_timeout": True}
        sentinel from Patch D.

    fetch_single_video_metadata(channel, video_id, fp, title, stream, ...)
        Inline per-video fetch used by sync.py's DLTRACK handler right
        after a download lands. Snipes the exact video by file mtime.

    fetch_metadata_for_videos(channel, video_ids, stream, ...)
        Group-by-(year, month) bulk fetch with parallel pre-fetch
        (3 workers, jittered submission) + sticky active line.

The pause helpers (_enter_pause_wait / _exit_pause_wait) used by
fetch_metadata_for_videos are defined locally here as thin wrappers
around pause_helpers.emit_paused / emit_resumed — they're also
defined in legacy.py for the other call sites still living there.
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from ..log import get_logger
from ..log_stream import LogStreamer
from ..metadata_io import (
    _folder_for_channel,
    _get_metadata_jsonl_path,
    _read_metadata_jsonl,
    _write_metadata_jsonl,
)
from ..sync import _find_cookie_source, _startupinfo, find_yt_dlp
from ..thumbnails import _download_thumbnail, _ensure_thumbnails_dir
from ..utils import utf8_subprocess_env as _utf8_env
from .scan import _group_by_metadata_path, _scan_channel_videos


# Module-scoped tracking for in-flight metadata-fetch subprocesses.
# Used so a cancel during a bulk pre-fetch can forcibly kill the
# yt-dlp processes WITHOUT touching sync's separate yt-dlp procs
# (which use the global PROCESS_REGISTRY). Set is mutated under lock
# so concurrent worker threads can add/remove safely.
_inflight_procs: set[subprocess.Popen] = set()
_inflight_procs_lock = threading.Lock()


def _kill_inflight_metadata_procs() -> int:
    """Terminate every in-flight _fetch_video_metadata yt-dlp Popen.
    Called by the bulk pre-fetch cancel handler. Returns count killed."""
    with _inflight_procs_lock:
        procs = list(_inflight_procs)
    killed = 0
    for p in procs:
        try:
            if p.poll() is None:
                p.kill()
                killed += 1
        except Exception as e:
            _log.debug("swallowed: %s", e)
    return killed

_log = get_logger(__name__)


def _enter_pause_wait(stream: LogStreamer, label: str, queues) -> None:
    """Worker hit a pause-wait. Routes through pause_helpers.emit_paused."""
    from ..pause_helpers import emit_paused
    emit_paused(stream, label=label, queues=queues)


def _exit_pause_wait(stream: LogStreamer, label: str, queues) -> None:
    """Worker exiting pause-wait. Routes through pause_helpers.emit_resumed."""
    from ..pause_helpers import emit_resumed
    emit_resumed(stream, label=label, queues=queues)


def _fetch_video_metadata(yt: str, video_id: str,
                          title_hint: str = "") -> dict[str, Any] | None:
    """Fetch metadata for a single video via yt-dlp --dump-json.
    Returns the OLD-schema dict, or None on failure.
    Matches YTArchiver.py:26719."""
    cmd = [
        yt,
        "--dump-json", "--no-download", "--no-warnings",
        "--ignore-errors", "--skip-download",
        "--write-comments",
        "--extractor-args",
        "youtube:comment_sort=top;max_comments=50,50,0,0",
        *_find_cookie_source(),
        f"https://www.youtube.com/watch?v={video_id}",
    ]
    # Patch D: retry-with-backoff on timeout. Previously a single
    # 120s timeout — if YouTube was slow on attempt 1, we gave up.
    # Now: 3 attempts with timeouts 60s, 60s, 90s plus a short sleep
    # between. Total worst-case still ~210s (down from a fail-then-
    # retry on next pass which could be hours later).
    stdout = ""
    _rc: int | None = None
    _attempts = (60, 60, 90)
    for _attempt_idx, _attempt_timeout in enumerate(_attempts):
        try:
            # CREATE_NEW_PROCESS_GROUP so taskkill /T /F on cancel/timeout
            # also reaps spawned ffmpeg/curl children. Without it,
            # proc.kill() only kills yt-dlp itself and orphaned child
            # processes pile up over a long refresh — eventually
            # exhausting handles or holding the cookie file lock
            # (audit: metadata/fetcher.py:99-111). 0x00000200 is
            # CREATE_NEW_PROCESS_GROUP on Windows.
            _creationflags = 0
            if os.name == "nt":
                _creationflags = 0x00000200  # CREATE_NEW_PROCESS_GROUP
            proc = subprocess.Popen(
                cmd, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                encoding="utf-8", errors="replace",
                startupinfo=_startupinfo,
                env=_utf8_env(),
                creationflags=_creationflags,
            )
        except OSError:
            return None
        # Register this yt-dlp Popen in a module-scoped set so a
        # cancel-during-bulk-fetch can forcibly kill in-flight metadata
        # fetches without disturbing sync's downloads. Each call can
        # chew up to ~210s across its three attempts; without targeted
        # kill, cancel feels "stuck" for many minutes during a bulk
        # pre-fetch (max_workers=3, ~1000 videos).
        with _inflight_procs_lock:
            _inflight_procs.add(proc)
        try:
            try:
                stdout, _ = proc.communicate(timeout=_attempt_timeout)
                _rc = proc.returncode
                break
            except subprocess.TimeoutExpired:
                # Use taskkill /T /F on Windows so the entire process
                # tree (yt-dlp + any ffmpeg/curl children it spawned)
                # gets reaped, not just the top-level yt-dlp (audit:
                # metadata/fetcher.py:99-111).
                _reaped = False
                if os.name == "nt":
                    try:
                        subprocess.run(
                            ["taskkill", "/T", "/F", "/PID", str(proc.pid)],
                            capture_output=True, timeout=5,
                            creationflags=0x08000000,  # CREATE_NO_WINDOW
                        )
                        _reaped = True
                    except Exception as _tk:
                        _log.debug("taskkill /T failed: %s", _tk)
                if not _reaped:
                    try: proc.kill()
                    except Exception: pass
                try: proc.communicate(timeout=5)
                except Exception as e: _log.debug("swallowed: %s", e)
                if _attempt_idx == len(_attempts) - 1:
                    # All attempts exhausted — still a transient signal,
                    # not a permanent failure. audit E-11 sentinel.
                    return {"_timeout": True}
                # Short backoff before retry
                time.sleep(2 ** _attempt_idx)  # 1s, 2s
        finally:
            with _inflight_procs_lock:
                _inflight_procs.discard(proc)
    if _rc is None or _rc != 0:
        return None

    # yt-dlp --dump-json writes exactly one JSON object on stdout.
    # Try the WHOLE stdout first — that's the canonical case and
    # handles descriptions/comments containing literal `{` characters
    # (which broke the line-by-line + find/rfind heuristic on videos
    # where the description had a `{` near the top: the slice would
    # extract an invalid sub-substring and treat the fetch as failed,
    # which then set metadata_fetch_failed_ts and made the video
    # un-retryable on future rechecks). Fall back to the line-scan
    # only if the whole-stdout parse fails (e.g. extractor warnings
    # leaked onto stdout).
    data: dict[str, Any] | None = None
    try:
        data = json.loads(stdout)
        if not isinstance(data, dict):
            data = None
    except Exception:
        data = None
    if data is None:
        for _line in stdout.splitlines():
            _ls = _line.strip()
            if not _ls or _ls[0] != "{":
                continue
            try:
                data = json.loads(_ls)
                break
            except Exception:
                continue
    if data is None:
        # Last-resort slice-between-first-and-last-brace parse. Same
        # caveat as before — fragile against descriptions containing
        # `{` — kept for back-compat but the whole-stdout path above
        # should win in normal operation.
        js = stdout.find("{")
        je = stdout.rfind("}")
        if js < 0 or je <= js:
            return None
        try:
            data = json.loads(stdout[js:je + 1])
        except Exception:
            return None

    comments = []
    for c in (data.get("comments") or [])[:50]:
        comments.append({
            "author": c.get("author", ""),
            "text": c.get("text", ""),
            "likes": c.get("like_count", 0),
            "time": c.get("timestamp") or c.get("time_text", ""),
        })

    return {
        "video_id": video_id,
        "title": data.get("title", title_hint),
        "description": data.get("description", ""),
        "view_count": data.get("view_count", 0),
        "like_count": data.get("like_count", 0),
        "comment_count": data.get("comment_count", 0),
        "upload_date": data.get("upload_date", ""),
        "duration": data.get("duration", 0),
        "thumbnail_url": data.get("thumbnail", ""),
        "comments": comments,
        "fetched_at": datetime.now().isoformat(),
    }


def fetch_single_video_metadata(channel: dict[str, Any],
                                video_id: str,
                                file_path: str,
                                title_hint: str,
                                stream: LogStreamer,
                                emit_inline_log: bool = True,
                                refresh: bool = False,
                                ) -> dict[str, Any]:
    """Fetch metadata for ONE just-downloaded video, inline per-video.

    Unlike `fetch_metadata_for_videos` (which walks the channel folder to
    group videos by year/month), this one "snipes" the exact video: we
    already know its file path and mtime, so we compute year/month from
    that and write straight to the correct aggregated JSONL.

    Emits one log line by default — " — Metadata downloaded" — in
    pink, matching the format the user asked for:
        [Sync] ...
          Downloading Title...
          — ✓ Title Channel 04.18.26 (26 MB)
          — Metadata downloaded

    Called from sync.py's DLTRACK handler (dispatched to a background
    thread so it doesn't back-pressure the yt-dlp stdout reader).

    Returns {ok, fetched|skipped|error}.
    """
    if not video_id or not file_path:
        return {"ok": False, "error": "missing id or path"}

    folder = _folder_for_channel(channel)
    if folder is None:
        return {"ok": False, "error": "no output_dir"}

    yt = find_yt_dlp()
    if not yt:
        return {"ok": False, "error": "yt-dlp missing"}

    name = channel.get("name") or channel.get("folder") or "?"
    split_years = bool(channel.get("split_years"))
    split_months = bool(channel.get("split_months"))

    # Compute year/month from file mtime — yt-dlp --mtime sets mtime to
    # the YouTube upload date IN UTC, so we must read it as UTC too.
    # Using local time here would file near-midnight-UTC uploads under
    # the wrong day/month/year bucket (asymmetric with the UTC reader
    # elsewhere, producing duplicate JSONL entries).
    year: int | None = None
    month: int | None = None
    try:
        mt = datetime.fromtimestamp(os.path.getmtime(file_path), tz=timezone.utc)
        year, month = mt.year, mt.month
    except OSError:
        pass

    jp, subfolder = _get_metadata_jsonl_path(
        name, str(folder), split_years, split_months, year, month)

    existing = _read_metadata_jsonl(jp)
    if video_id in existing and not refresh:
        # Already have metadata for this id — nothing to do. No log.
        # `refresh=True` (comments refresh) bypasses this so the entry
        # gets re-fetched with current comments/views/likes.
        return {"ok": True, "skipped": True}

    entry = _fetch_video_metadata(yt, video_id, title_hint)
    # `{"_timeout": True}` sentinel signals a transient
    # 120s fetch timeout (slow network) rather than a true failure.
    # Return without marking anything; caller can retry later.
    if isinstance(entry, dict) and entry.get("_timeout"):
        if emit_inline_log:
            # Indent — this line nests under the parent " — ✓ Title …"
            # video row from sync.py, matching the metadata/transcription
            # done lines below.
            stream.emit([
                ["      — ", "dim"],
                ["Metadata fetch timed out (will retry next pass)\n", "dim"],
            ])
        return {"ok": False, "error": "timeout", "transient": True}
    if entry is None:
        if emit_inline_log:
            stream.emit([
                ["      — ", "dim"],
                ["Metadata fetch failed\n", "red"],
            ])
        return {"ok": False, "error": "yt-dlp dump-json failed"}

    existing[video_id] = entry
    _jsonl_write_failed = False
    try:
        _write_metadata_jsonl(jp, existing)
    except Exception as e:
        # Don't return early — still attempt the thumbnail so the
        # user gets SOMETHING (audit: fetcher H93). The caller sees
        # ok=False with the underlying error so the failure is still
        # surfaced.
        _jsonl_write_failed = True
        _jsonl_err = str(e)

    # Thumbnail (best-effort). Stream passed through so fetch errors
    # surface as verbose-only dim log lines instead of disappearing.
    if entry.get("thumbnail_url"):
        thumb_dir = _ensure_thumbnails_dir(subfolder)
        _download_thumbnail(
            entry["thumbnail_url"], thumb_dir,
            title_hint or entry.get("title", ""), video_id,
            stream=stream if emit_inline_log else None)

    if emit_inline_log:
        # Per-video metadata done line. Matches the three-line simple-mode
        # summary spec locked in:
        # — ✓ <title> — <channel> (size) [download done, green]
        # — ✓ Transcription (details) [transcription done, blue]
        # — ✓ Metadata downloaded [metadata done, pink + white]
        # Pink em-dash + checkmark + pink "Metadata", then white
        # "downloaded". user spec: color the subject, not the
        # verb — "(pink)— (pink)Metadata (white)downloaded".
        # Issues #139/#144/#148: tag with meta_done_<vid> so the
        # emit REPLACES the placeholder sync.py reserved under this
        # video's block rather than landing at log bottom after later
        # channels' rows have scrolled in.
        _md_marker = f"meta_done_{video_id}" if video_id else ""
        _md_tag = lambda *extra: [t for t in (_md_marker, *extra) if t]
        # Indent — this line nests visually under the parent " — ✓ Title
        # … (size)" video row that sync.py emits for this same video.
        # 6 leading spaces gives a clean two-level outline:
        #   [N/M] Channel
        #    — ✓ Title (12 MB)
        #         — ✓ Metadata downloaded
        #         — ✓ Transcription (...)
        stream.emit([
            ["      ", _md_tag("dim")],
            ["— ✓ ", _md_tag("meta_bracket")],
            ["Metadata ", _md_tag("simpleline_pink")],
            ["downloaded\n", _md_tag("simpleline")],
        ])
    # Return the entry so callers (refresh_channel_comments) can
    # diff old-vs-new to count "unchanged" videos. If the jsonl write
    # failed we still attempted the thumbnail, but the contract
    # demands ok=False in that case (audit: fetcher H93).
    if _jsonl_write_failed:
        return {"ok": False, "error": f"jsonl write failed: {_jsonl_err}",
                "entry": entry}
    return {"ok": True, "fetched": True, "entry": entry}


def fetch_metadata_for_videos(channel: dict[str, Any],
                              video_ids: Iterable[str],
                              stream: LogStreamer,
                              cancel_event: threading.Event | None = None,
                              refresh: bool = False,
                              pause_event: threading.Event | None = None,
                              queues=None,
                              ) -> dict[str, Any]:
    """Fetch metadata for the given video IDs into the aggregated JSONL(s).

    Also downloads each video's thumbnail into the corresponding
    `.Thumbnails/` subfolder. No per-video `.info.json` is written — this
    matches.

    Handles year/month splitting: videos in different year folders write to
    different aggregated JSONLs.
    """
    ids = [vid.strip() for vid in video_ids if vid and vid.strip()]
    if not ids:
        return {"ok": True, "fetched": 0, "skipped": 0, "errors": 0}

    folder = _folder_for_channel(channel)
    if folder is None:
        stream.emit_error("Archive folder isn't configured. Set it in Settings → General.")
        return {"ok": False, "error": "no output_dir"}
    folder.mkdir(parents=True, exist_ok=True)

    yt = find_yt_dlp()
    if not yt:
        stream.emit_error("Can't refresh video info — the download tool (yt-dlp) isn't installed.")
        return {"ok": False, "error": "yt-dlp missing"}

    name = channel.get("name") or channel.get("folder") or "?"
    split_years = bool(channel.get("split_years"))
    split_months = bool(channel.get("split_months"))

    # Walk the folder to find videos on disk matching these IDs — we need
    # their year/month to route each fetch to the correct aggregated JSONL.
    all_videos = _scan_channel_videos(folder)
    by_id = {v[0]: v for v in all_videos if v[0]}
    wanted = [by_id[vid] for vid in ids if vid in by_id]
    # Videos whose ID we want but aren't yet on disk — drop to the channel-root bucket.
    for vid in ids:
        if vid not in by_id:
            wanted.append((vid, "", None, None, ""))

    groups = _group_by_metadata_path(name, str(folder),
                                     split_years, split_months, wanted)

    # Simple-mode users see human-readable "Refreshing N video(s)..."
    # Verbose-mode users additionally see the technical "fast-fetch
    # N id(s)" label (dim-tagged so Simple mode filters it).
    stream.emit([
        ["  — ", "meta_bracket"],
        [f"{name} ", "simpleline"],
        ["— ", "meta_bracket"],
        [f"refreshing {len(ids)} video(s)…\n", "simpleline"],
    ])
    stream.emit([
        ["    — ", ["dim"]],
        [f"fast-fetch {len(ids)} id(s)\n", ["dim"]],
    ])

    total = sum(len(g["videos"]) for g in groups.values())
    t0 = time.time()
    fetched = skipped = errors = refreshed = thumb_only = 0
    idx = 0

    # Sticky active status line pinned at the bottom of the log while
    # the metadata fetch runs — mirrors classic YTArchiver.py:14207
    # `_start_simple_anim(ch_name, 1, _fetch_total, mode="metadata")`.
    # Each per-video update fires a `clear_line` control to drop the
    # old active line and re-emits a fresh one at the current DOM
    # bottom via the `metadata_active` marker.
    import json as _json
    def _emit_active(_i: int, _n: int):
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "metadata_active"}),
             "__control__"],
        ])
        # Color discipline: only [ / ] + "Fetching Metadata:" render
        # in the metadata color; numbers + channel name stay white.
        stream.emit([
            ["[", ["meta_bracket", "metadata_active"]],
            [str(_i), ["simpleline", "metadata_active"]],
            ["/", ["meta_bracket", "metadata_active"]],
            [str(_n), ["simpleline", "metadata_active"]],
            ["] ", ["meta_bracket", "metadata_active"]],
            ["Fetching Metadata: ", ["meta_bracket", "metadata_active"]],
            [f"{name}…\n", ["simpleline", "metadata_active"]],
        ])

    def _clear_active():
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "metadata_active"}),
             "__control__"],
        ])

    for jp, g in groups.items():
        if cancel_event is not None and cancel_event.is_set():
            break
        # wait-on-pause loop (not break-on-pause). Old
        # behavior was "pause = cancel" because the loop bailed out
        # entirely; user lost partial progress of the current group.
        # Now we block in-place until Resume (or Cancel) and continue
        # where we left off. Mirrors the redownload.py pause pattern
        # around line 651-666.
        if pause_event is not None and pause_event.is_set():
            _enter_pause_wait(stream,
                              f"{channel.get('name', '?')} (metadata fetch)",
                              queues)
            while (pause_event.is_set()
                   and not (cancel_event is not None and cancel_event.is_set())):
                time.sleep(0.5)
            _exit_pause_wait(stream,
                             f"{channel.get('name', '?')} (metadata fetch)",
                             queues)
            if cancel_event is not None and cancel_event.is_set():
                break
        existing = _read_metadata_jsonl(jp)
        thumb_dir = _ensure_thumbnails_dir(g["subfolder"])
        changed = False

        # Hoist the thumbnail listing once per group. Old code did
        # os.listdir() inside _has_thumbnail_for, called per-video,
        # so a 1000-video group did 1000 redundant directory walks
        # against the same .Thumbnails folder (audit: metadata/
        # fetcher.py:425-440). Build a set of `[vid]` substrings
        # extracted from the listing and check membership in O(1).
        _thumb_brackets: set[str] = set()
        if os.path.isdir(thumb_dir):
            try:
                _IMG_EXTS = (".jpg", ".jpeg", ".png", ".webp")
                for _fn in os.listdir(thumb_dir):
                    if not _fn.lower().endswith(_IMG_EXTS):
                        continue
                    # Extract every `[xxx]` chunk from the filename and
                    # add to the set IF the inner string looks like a
                    # real YouTube video id (exactly 11 chars from the
                    # YT id alphabet). Tolerates weirder filenames
                    # without admitting non-id bracket groups that
                    # could false-match a different video's real id
                    # (audit: fetcher.py L1).
                    _i = 0
                    while True:
                        _o = _fn.find("[", _i)
                        if _o < 0:
                            break
                        _c = _fn.find("]", _o + 1)
                        if _c < 0:
                            break
                        _inner = _fn[_o+1:_c]
                        if len(_inner) == 11 and all(
                                c.isalnum() or c in "_-" for c in _inner):
                            _thumb_brackets.add(_fn[_o:_c+1])
                        _i = _c + 1
            except OSError:
                pass

        def _has_thumbnail_for(vid: str) -> bool:
            """Check if any thumbnail file in this group's .Thumbnails
            folder matches `[vid]`. a case: 2 videos had metadata
            but no thumbnail — the old skip-if-in-existing check treated
            those as "complete" and never re-downloaded the thumbnail."""
            if not vid:
                return False
            return f"[{vid}]" in _thumb_brackets

        # Patch F (followup): parallel pre-fetch metadata for this
        # group. Previously the inner loop called _fetch_video_metadata
        # synchronously, one video at a time — for a 500-video refresh
        # that's ~20-30 min of subprocess-launch wall time. With 3
        # concurrent workers + per-submission jitter to avoid YouTube
        # rate-limit, we get ~3x speedup. Cancel + pause are honored
        # during submission AND result drain.
        _to_prefetch = []
        for _v_id, _v_title, _y, _m, _fp in g["videos"]:
            if not _v_id:
                continue
            _is_refresh_hit_pf = _v_id in existing and refresh
            _needs_thumb_only_pf = (_v_id in existing and not refresh
                                    and not _has_thumbnail_for(_v_id))
            if _v_id in existing and not refresh and not _needs_thumb_only_pf:
                continue
            _to_prefetch.append((_v_id, _v_title))

        _prefetched: dict[str, dict[str, Any] | None] = {}
        if _to_prefetch:
            import concurrent.futures as _cf
            import random as _random
            _pf_done = 0
            _pf_total = len(_to_prefetch)
            _pf_last_tick = time.time()
            try:
                with _cf.ThreadPoolExecutor(
                        max_workers=3,
                        thread_name_prefix="yta-meta-prefetch") as _pf_pool:
                    _pf_futs: dict[Any, str] = {}
                    for _pf_vid, _pf_title in _to_prefetch:
                        if cancel_event is not None and cancel_event.is_set():
                            break
                        if pause_event is not None and pause_event.is_set():
                            while (pause_event.is_set()
                                   and not (cancel_event is not None
                                            and cancel_event.is_set())):
                                time.sleep(0.5)
                            if cancel_event is not None and cancel_event.is_set():
                                break
                        # Per-submission jitter to stagger the burst
                        # against YouTube. 0-200ms is small enough not
                        # to dominate the wall-clock budget.
                        time.sleep(_random.uniform(0, 0.2))
                        _fut = _pf_pool.submit(
                            _fetch_video_metadata, yt, _pf_vid, _pf_title)
                        _pf_futs[_fut] = _pf_vid
                    for _fut in _cf.as_completed(_pf_futs):
                        if cancel_event is not None and cancel_event.is_set():
                            for _f in _pf_futs:
                                _f.cancel()
                            # .cancel() only stops futures that haven't
                            # started yet; in-flight workers keep
                            # running until their subprocess timeout
                            # (~210s worst case). Kill the in-flight
                            # metadata-fetch yt-dlp procs targeted via
                            # the module-scoped registry so cancel feels
                            # responsive — without touching sync's own
                            # yt-dlp procs in PROCESS_REGISTRY.
                            try:
                                _killed = _kill_inflight_metadata_procs()
                                if _killed:
                                    _log.info(
                                        "metadata cancel killed %d "
                                        "in-flight yt-dlp procs", _killed)
                            except Exception as _ke:
                                _log.debug("swallowed: %s", _ke)
                            break
                        _vid_done = _pf_futs[_fut]
                        try:
                            _prefetched[_vid_done] = _fut.result()
                        except Exception:
                            _prefetched[_vid_done] = None
                        _pf_done += 1
                        _now = time.time()
                        if _now - _pf_last_tick > 1.0 or _pf_done == _pf_total:
                            _pf_last_tick = _now
                            try:
                                stream.emit_dim(
                                    f"  Pre-fetching metadata: "
                                    f"{_pf_done}/{_pf_total}…")
                            except Exception as _pe:
                                _log.debug(
                                    "prefetch progress emit failed: %s", _pe)
            except Exception as _pf_err:
                _log.warning(
                    "metadata pre-fetch pool failed (%s); falling back "
                    "to sequential fetch in the existing loop", _pf_err)
                _prefetched = {}

        for vid_id, title, _y, _m, _fp in g["videos"]:
            if cancel_event is not None and cancel_event.is_set():
                break
            # wait-on-pause inside the inner per-video loop
            # too (not just the outer group loop). Without this, pause
            # during a big group would still march through the rest of
            # the videos before the outer loop's next iteration checks.
            if pause_event is not None and pause_event.is_set():
                _enter_pause_wait(stream,
                                  f"{channel.get('name', '?')} (metadata fetch)",
                                  queues)
                while (pause_event.is_set()
                       and not (cancel_event is not None and cancel_event.is_set())):
                    time.sleep(0.5)
                _exit_pause_wait(stream,
                                 f"{channel.get('name', '?')} (metadata fetch)",
                                 queues)
                if cancel_event is not None and cancel_event.is_set():
                    break
            if not vid_id:
                errors += 1
                continue
            idx += 1
            is_refresh_hit = vid_id in existing and refresh
            # Thumbnail-only gap: metadata exists but the image file
            # doesn't. Re-run the API fetch so we get a fresh
            # thumbnail_url and download it, but don't overwrite the
            # existing JSONL entry (it's fine as-is).
            needs_thumb_only = (vid_id in existing and not refresh
                                and not _has_thumbnail_for(vid_id))
            if vid_id in existing and not refresh and not needs_thumb_only:
                skipped += 1
                continue
            # issue #136: distinguish a refresh hit from a fresh
            # fetch in the per-video log line so the user can SEE the
            # refresh actually doing work (previously every line just
            # said "Metadata — <title>" regardless, and the summary row
            # also ignored refresh counts, so the feature looked broken).
            if needs_thumb_only:
                _reason = "Thumbnail"
            elif refresh and vid_id in existing:
                _reason = "Refresh"
            else:
                _reason = "Metadata"
            # Color discipline: only the pink parts are the ones that
            # identify the task source (brackets, em-dash, tag label).
            # Numbers and titles render in the default color so they
            # read clearly. rule.
            stream.emit([
                [" [", "meta_bracket"],
                [str(idx), "simpleline"],
                ["/", "meta_bracket"],
                [str(total), "simpleline"],
                ["] ", "meta_bracket"],
                [_reason, "simpleline_pink"],
                [" — ", "meta_bracket"],
                [f"{title[:90]}\n", "simpleline"],
            ])
            _emit_active(idx, total)
            # Patch F: use the prefetched result if we already fetched
            # it in the parallel pre-fetch loop above. Falls back to
            # synchronous fetch if (a) pre-fetch was skipped due to
            # error, or (b) this vid_id wasn't in the prefetch set
            # (shouldn't happen with the same skip-logic, but defensive).
            if vid_id in _prefetched:
                entry = _prefetched[vid_id]
            else:
                entry = _fetch_video_metadata(yt, vid_id, title)
            # transient timeout sentinel — count as "will
            # retry" rather than a permanent failure so future rechecks
            # still try this video. No persistent flag set.
            if isinstance(entry, dict) and entry.get("_timeout"):
                errors += 1
                stream.emit([
                    [" — ", "dim"],
                    ["Metadata timeout (will retry next pass) — ", "dim"],
                    [f"{title[:90]}\n", "simpleline"],
                ])
                continue
            if entry is None:
                errors += 1
                # surface a per-video error line so the user
                # knows WHICH titles failed (previously only the
                # summary count emerged, making diagnosis impossible).
                stream.emit([
                    [" ✗ ", "red"],
                    ["Metadata failed — ", "red"],
                    [f"{title[:90]}\n", "simpleline"],
                ])
                # Mark this video_id as permanently failed only after
                # N consecutive failed attempts — soft-failure
                # tracking. The previous one-shot stamp meant a
                # single transient yt-dlp error (throttle, network
                # blip surviving all 3 retries) marked the video as
                # failed forever, and future rechecks would skip it
                # via the _failed_fetch set. Now we use a counter
                # column `metadata_fetch_fail_count`; only after 3
                # consecutive failures do we stamp the permanent
                # flag. The counter resets on success (handled by
                # the merge/insert path elsewhere).
                _PERM_FAIL_THRESHOLD = 3
                try:
                    from .. import index as _idx
                    # Move _idx._open() INSIDE the lock + add an
                    # explicit try/except/rollback around the UPDATE
                    # batch. Old code opened outside the lock and let
                    # an exception leak after a partial COALESCE
                    # increment — flag write could half-apply (audit:
                    # metadata/fetcher.py:611-619).
                    with _idx._db_lock:
                        conn = _idx._open()
                        if conn is not None:
                            try:
                                # Ensure the column exists (lazy migration
                                # — cheap, only happens once per process).
                                try:
                                    conn.execute(
                                        "ALTER TABLE videos ADD COLUMN "
                                        "metadata_fetch_fail_count INTEGER "
                                        "DEFAULT 0")
                                except Exception:
                                    pass
                                conn.execute(
                                    "UPDATE videos SET "
                                    "metadata_fetch_fail_count = "
                                    "COALESCE(metadata_fetch_fail_count, 0) + 1 "
                                    "WHERE video_id=?",
                                    (vid_id,))
                                row = conn.execute(
                                    "SELECT MIN(metadata_fetch_fail_count) "
                                    "FROM videos WHERE video_id=?",
                                    (vid_id,)).fetchone()
                                _fail_n = int(row[0] or 0) if row else 0
                                if _fail_n >= _PERM_FAIL_THRESHOLD:
                                    conn.execute(
                                        "UPDATE videos SET "
                                        "metadata_fetch_failed_ts=? "
                                        "WHERE video_id=?",
                                        (time.time(), vid_id))
                                conn.commit()
                            except Exception as _ue:
                                try: conn.rollback()
                                except Exception: pass
                                _log.warning(
                                    "fail-count UPDATE rolled back: %s", _ue)
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                continue
            if is_refresh_hit:
                # Merge: update counts + comments, keep other fields
                old = existing[vid_id]
                old["view_count"] = entry.get("view_count", old.get("view_count", 0))
                old["like_count"] = entry.get("like_count", old.get("like_count", 0))
                old["comment_count"] = entry.get("comment_count", old.get("comment_count", 0))
                old["comments"] = entry.get("comments", old.get("comments", []))
                old["fetched_at"] = entry.get("fetched_at", "")
                if entry.get("thumbnail_url"):
                    old["thumbnail_url"] = entry["thumbnail_url"]
                refreshed += 1
                changed = True
            elif needs_thumb_only:
                # JSONL entry stays as-is; only the thumbnail is being
                # backfilled. `changed` stays False so we don't rewrite
                # the JSONL for a thumbnail-only fetch.
                # count thumbnail-only refetches separately
                # from true metadata fetches so the summary + activity
                # log distinguish the two (was silently lumped under
                # `fetched`, making thumbnail-only runs look like full
                # metadata pulls).
                thumb_only += 1
            else:
                existing[vid_id] = entry
                fetched += 1
                changed = True
            if entry.get("thumbnail_url"):
                _download_thumbnail(entry["thumbnail_url"], thumb_dir,
                                    title or entry.get("title", ""), vid_id,
                                    stream=stream)

        if changed:
            try:
                _write_metadata_jsonl(jp, existing)
            except Exception as e:
                stream.emit_error(f"Could not write {jp}: {e}")

    # Drop the sticky active line before the summary so the "Metadata
    # X — N fetched ..." footer doesn't sit below a phantom "Fetching
    # Metadata: X..." line that's no longer accurate.
    _clear_active()

    elapsed = time.time() - t0
    summary_parts = []
    if fetched: summary_parts.append(f"{fetched} fetched")
    if refreshed: summary_parts.append(f"{refreshed} refreshed")
    if thumb_only: summary_parts.append(f"{thumb_only} thumbnails")
    if skipped: summary_parts.append(f"{skipped} skipped")
    if errors: summary_parts.append(f"{errors} errors")
    summary = " · ".join(summary_parts) or "nothing to do"
    stream.emit([
        [" – ", "dim"],
        [f"Metadata {name} — ", "simpleline"],
        [summary, "dim"],
        [f" · took {elapsed:.1f}s\n", "dim"],
    ])
    # Both activity-log row emit AND history persistence moved to
    # sync.py's `emit_metadata_activity_row` so ALL metadata-kind
    # tasks (views/likes refresh, comments refresh, ID backfill,
    # legacy fetch) produce a single identical [Metdta] row with
    # a single persisted history entry. Previously this path emitted
    # + persisted locally while the bulk paths didn't emit at all,
    # so views/likes refresh never appeared in the activity log
    # and legacy fetches produced nothing at all from sync.py's
    # worker loop.
    return {"ok": True, "fetched": fetched, "skipped": skipped,
            "errors": errors, "refreshed": refreshed,
            "thumb_only": thumb_only, "took": elapsed}
