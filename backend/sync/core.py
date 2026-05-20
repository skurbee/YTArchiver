"""
Sync — subprocess wrapper around yt-dlp.

Strategy: don't rewrite YTArchiver's sync logic. Invoke yt-dlp directly
with the same flags, stream stdout to the UI via LogStreamer. Matches
YTArchiver.py:9992 dl_cmd pattern.

Scope for this module's first cut:
  - Find yt-dlp.exe (same lookup YTArchiver does)
  - Build format string (verbatim port of build_format_string at :2730)
  - Sync one channel: invoke yt-dlp, stream stdout line-by-line
  - Respect cancel_event for clean termination
  - Emit a structured activity-log row on completion

Not yet in scope (need separate sessions):
  - Per-video metadata sidecars (.info.json, .nfo)
  - Auto-transcribe / auto-compress queue dispatch
  - Thumbnail sidecar download
  - Year/month folder split
  - Duration filtering (min/max)
  - From-date mode
  - Livestream handling
  - Retry / redownload logic
"""

from __future__ import annotations

import os
import re
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .. import utils as _utils
from ..log import get_logger
from ..log_stream import LogStreamer
from ..ytarchiver_config import ARCHIVE_FILE, config_is_writable, load_config, save_config

__all__ = [
    "RESOLUTION_OPTIONS",
    "reset_cookie_cache",
    "find_yt_dlp",
    "build_format_string",
    "sanitize_folder",
    "channel_folder_name",
    "build_batch_file",
    "cleanup_batch_file",
    "write_sync_progress",
    "clear_sync_progress",
    "prefetch_channel_total",
    "quick_check_new_uploads",
    "set_batch_cooldown",
    "emit_metadata_activity_row",
    "emit_consolidated_auto_row",
    "register_pending_dwnld_row",
    "pop_pending_dwnld_row",
    "SyncResult",
    "sync_channel",
    "set_recent_changed_hook",
    "set_metadata_changed_hook",
    "set_sync_active",
    "clear_sync_active",
    "is_sync_active",
    "is_any_sync_active",
    "sync_all",
]

_log = get_logger(__name__)


# ── Patch 18 phase 2 (v68.8): pure helpers moved to ytdlp_proc.py ─────
# These names are re-imported here so internal call sites in this file
# and external callers (`from backend.sync import find_yt_dlp`, etc.)
# keep resolving them as before.
# Patch 18 phase 4 (v69.4): log row emitters moved to log_rows.py.
# Re-imported so internal callers in this file keep resolving them.
from .log_rows import (  # noqa: F401  (re-exports for backend.sync surface)
    _ROW_EMIT_PASS_ID,
    _bracket_segments,
    _count_cell,
    _new_pass_id,
    _persist_row_history,
    _short_summary,
    _sync_row_emit,
    emit_consolidated_auto_row,
    emit_metadata_activity_row,
    pop_pending_dwnld_row,
    register_pending_dwnld_row,
)
from .quickcheck import (
    _check_batch_cooldown,
    _should_batch_limit,
    prefetch_channel_total,
    quick_check_new_uploads,
    set_batch_cooldown,
)
# Recent-tab download tracking lives in its own module so the hook
# global + setter + record function travel together. Re-imported here
# so internal callers (sync_channel's DLTRACK handler) keep resolving
# `_record_recent_download` as before.
from .recent_track import (  # noqa: F401
    _record_recent_download,
    set_recent_changed_hook,
)

# ── Patch 18 phase 3 (v68.8): display-push + quickcheck moved out ─────
from .display_push import (
    clear_sync_progress,
    write_sync_progress,
)

# Patch 14 (v71.6): file/format helpers moved to sync_helpers.py.
# Re-imported here so sync_channel and other call sites keep resolving
# the names. External callers reach for these via backend.sync.* —
# the package __init__ re-exports them from this module.
from .sync_helpers import (  # noqa: F401
    _F_SUFFIX_RE,
    _fmt_duration,
    _fmt_size,
    _hide_sidecar_win,
    _resolve_final_mp4,
    _scan_recent_video,
    _sweep_orphan_vtts,
)
from .ytdlp_proc import (  # noqa: F401  (re-exports for backend.sync surface)
    RESOLUTION_OPTIONS,
    _ensure_videos_tab,
    _find_cookie_source,
    build_batch_file,
    build_format_string,
    channel_folder_name,
    cleanup_batch_file,
    find_yt_dlp,
    reset_cookie_cache,
    sanitize_folder,
)


_ID_IN_FILENAME = re.compile(r"\[([A-Za-z0-9_-]{11})\]")

# ── Progress parsing ───────────────────────────────────────────────────

_PROG_RE = re.compile(r"\[download\]\s+(\d+(?:\.\d+)?)%")
_TITLE_RE = re.compile(r"\[download\]\s+Destination:\s+(.+)$")
# Authoritative final path comes from the Merger / ffmpeg / FixupM3u8 log
# line. — we want every flavor of yt-dlp's
# final-file announcement (merge of separate tracks, remux for M3U8, etc.).
_MERGE_RE = re.compile(
    # Match the FULL quoted path even when the video title contains
    # internal quote characters — yt-dlp emits the merger target as
    # `Merging formats into "PATH"` and PATH can legitimately contain
    # quote marks (e.g. a title like `the "Nice Way" video.mp4`).
    # Non-greedy `"(.+?)"` used to stop at the first internal quote,
    # producing a truncated path; _path_to_counter.get() then missed
    # and the done line orphaned instead of replacing the progress
    # row. Greedy `"(.+)"` with end-of-line anchor captures the whole
    # path — yt-dlp always terminates the merger line with `"` + EOL.
    r'\[(?:Merger|ffmpeg|FixupM3u8)\]\s+(?:Merging|Remuxing|Converting)[^"]*"(.+)"\s*$')
_DOWNLOADING_RE = re.compile(r"\[info\]\s+([^:]+):\s+Downloading\s+\d+\s+format")
# Video-ID capture from `[youtube] VIDID:`, `[info] VIDID:`, `[download] VIDID:`
# lines yt-dlp prints throughout the lifecycle of a single video.
# 11-char `[A-Za-z0-9_-]` ID followed by `:`. Deliberately excludes
# `[youtube:tab]` (channel/playlist enumeration) which has a `:` inside
# the brackets — `\]` after the tag name keeps `[youtube:tab]` from
# matching.
_VIDID_RE = re.compile(r"\[(?:youtube|info|download)\]\s+([A-Za-z0-9_-]{11}):")

# Module-level download-row counter. The `dlrow_<N>` inplace kind must be
# globally unique across the whole sync run (NOT per-channel), otherwise
# channel B's first Downloading line would collide with channel A's last
# "✓ done" line (both would be `dlrow_1`) and the new Downloading would
# REPLACE the old Done in the DOM — making done lines disappear
# as soon as the next channel starts. Monotonic across all sync_channel
# calls in a single process lifetime.
_DLROW_COUNTER: int = 0


# Firefox / cookie-browser signed-out alert — emit ONCE per sync pass
# so a failing first channel doesn't spam the log with the same red
# block for every subsequent channel. Reset in `sync_all` at pass
# start. reported: classic YTArchiver would emit
#   ▌ PLEASE INSTALL FIREFOX, SIGN IN TO YOUTUBE, AND TRY AGAIN.
# when yt-dlp returned a cookie-extract / sign-in error. Overhaul was
# silent — user's browser cookies could go stale for weeks with no
# log feedback.
_COOKIE_ALERT_FIRED: bool = False
# Patch A: lock for the check-and-set on _COOKIE_ALERT_FIRED. Without
# this, two concurrent channel-sync threads can both observe the flag
# as False, both set it True, both emit the banner — duplicate alert.
_cookie_alert_lock = threading.Lock()
# Serialize ARCHIVE_FILE appends across the entire process. Two channel-
# sync threads writing concurrently can interleave bytes mid-line, and
# the loader silently drops malformed lines (audit: sync/core.py:1654).
_archive_write_lock = threading.Lock()
# Module-level lock for atomic load-modify-save of channel-keyed
# config writes (failed_video_ids, etc.) from sync.py code paths.
# ytarchiver_config.save_config() has its own lock for the write
# itself, but doesn't protect across read-modify-write at this layer.
_config_write_lock = threading.Lock()


# startupinfo now comes from subprocess_util (one
# source of truth shared with compress.py and transcribe.py).
from ..subprocess_util import make_startupinfo as _make_startupinfo

_startupinfo = _make_startupinfo()


# ── Sync one channel ───────────────────────────────────────────────────

class SyncResult(dict):
    """Result dict with ok/reason/counts."""


def sync_channel(channel: dict[str, Any], stream: LogStreamer,
                 cancel_event: threading.Event | None = None,
                 queues=None, transcribe_mgr=None,
                 pause_event: threading.Event | None = None,
                 pass_idx: int = 1,
                 pass_total: int = 1) -> SyncResult:
    """
    Sync one channel: fetch new videos via yt-dlp, stream progress.

    If `transcribe_mgr` is given and the channel has auto_transcribe=True,
    each newly-downloaded video is enqueued for whisper transcription.
    If `queues` is given, the current_sync marker is updated.
    If `pause_event` is given, the yt-dlp subprocess is terminated the
    moment the event is set — so pausing during a download actually
    stops the download instead of making the user wait for the current
    channel to finish. "the only thing we can't stop right in
    the middle of is Whisper transcriptions." Partial downloads are
    fine because yt-dlp's `--download-archive` + partial-file handling
    pick up where they left off on next run.

    `pass_idx` / `pass_total` are the 1-based position of this channel
    within the current sync pass and the total channel count. Used for
    the sync-progress JSON file so any companion display can render
    "3/47" rather than the hard-coded "1/1" from single-channel syncs.
    """
    # Module-level download-row counter is incremented inside the
    # Destination-line handler below. `global` must be declared before
    # ANY reference to `_DLROW_COUNTER` in this function, otherwise
    # Python flags the read in the DLTRACK `_done_kind` line as "name
    # used prior to global declaration".
    global _DLROW_COUNTER
    name = channel.get("name") or channel.get("folder") or "?"
    url = channel.get("url", "").strip()
    resolution = channel.get("resolution", "720")
    auto_tx = bool(channel.get("auto_transcribe"))
    # Mark the channel as "sync in progress" so the transcribe worker's
    # _flush_batch_stats skips it — sync_channel will emit the final
    # consolidated [Dwnld] row at its end with the transcribe count
    # read synchronously from transcribe_mgr. See a bug: fast
    # auto-captions finish before sync_channel ends, flush fired first
    # and emitted a duplicate [Trnscr] row.
    set_sync_active(name)
    # YTArchiver stores min/max as SECONDS on disk (e.g. 180 = 3 minutes).
    # UI shows/accepts minutes; conversion happens in subs.py / main.py.
    # Migration: values < 60 are legacy raw seconds from the v1 schema, bump
    # to 60 so a "30 s" entry becomes 1 min instead of being lost to 0.
    min_dur = int(channel.get("min_duration") or 0)
    max_dur = int(channel.get("max_duration") or 0)
    # Bug [40]: surface the legacy migration so users know why their
    # filter changed. Previously silent — a user who set min_duration=30
    # years ago would wake up to find sub-1-min videos no longer
    # downloading with no log entry explaining why.
    _migrated_min = False
    _migrated_max = False
    if 0 < min_dur < 60:
        try:
            stream.emit_dim(f" (legacy min_duration {min_dur}s upgraded to 60s)")
        except Exception as e:
            _log.debug("swallowed: %s", e)
        min_dur = 60
        _migrated_min = True
    if 0 < max_dur < 60:
        try:
            stream.emit_dim(f" (legacy max_duration {max_dur}s upgraded to 60s)")
        except Exception as e:
            _log.debug("swallowed: %s", e)
        max_dur = 60
        _migrated_max = True
    # Persist the upgraded values so the migration line doesn't fire on
    # every sync pass forever (audit: sync/core.py:266). Match channel
    # by URL only (same key the failed_video_ids writer uses).
    if _migrated_min or _migrated_max:
        try:
            with _config_write_lock:
                _cfgm = load_config()
                _our_url = (channel.get("url") or "").strip()
                for _ch in _cfgm.get("channels", []):
                    if (_ch.get("url") or "").strip() == _our_url:
                        if _migrated_min:
                            _ch["min_duration"] = 60
                        if _migrated_max:
                            _ch["max_duration"] = 60
                        break
                save_config(_cfgm)
            # Update the in-memory channel too so this pass uses the
            # bumped values without re-reading the config.
            if _migrated_min:
                channel["min_duration"] = 60
            if _migrated_max:
                channel["max_duration"] = 60
        except Exception as e:
            _log.debug("min/max migration persist swallowed: %s", e)
    mode = (channel.get("mode") or "new").lower() # "new" | "full" | "fromdate"
    from_date = (channel.get("from_date") or "").strip()
    # Folder-org flags: matches YTArchiver.py:17257 split_years / split_months
    split_years = bool(channel.get("split_years"))
    split_months = bool(channel.get("split_months"))

    if not url:
        return SyncResult(ok=False, reason="No URL", downloaded=0, errors=0)

    if queues is not None:
        queues.set_current_sync(channel)

    # Live progress for companion display — write at the START of the
    # channel so the display updates as each new channel kicks off,
    # not just when the
    # previous one completes. End-of-channel write below overwrites with
    # the final download count. Mirrors classic's pattern of calling
    # _write_sync_progress at multiple points per channel.
    try:
        write_sync_progress(channel_name=name,
                            idx=int(pass_idx or 1),
                            total=max(int(pass_total or 1), int(pass_idx or 1)),
                            downloaded=0, skipped=0, errors=0)
    except Exception as e:
        _log.debug("swallowed: %s", e)

    yt = find_yt_dlp()
    if not yt:
        stream.emit([["ERROR: ", "red"],
                     ["The download tool (yt-dlp) isn't installed. "
                      "Download it from yt-dlp.org and place yt-dlp.exe "
                      "next to YTArchiver, then restart.\n", "red"]])
        return SyncResult(ok=False, reason="yt-dlp missing", downloaded=0, errors=0)

    # Resolve output folder
    cfg = load_config()
    base_dir = Path((cfg.get("output_dir") or "").strip() or Path.cwd())
    folder_name = channel_folder_name(channel)
    # Guard against the "_unnamed" fallback — refuse to sync when both the
    # channel name AND folder_override are empty/whitespace. OLD's archive
    # has a residue `_unnamed/` folder from channels whose names resolved
    # blank during a sync in April 2026 (yt-dlp returned empty uploader +
    # no folder_override set); those videos lost their channel attribution
    # and had to be manually moved later. Surface the error instead of
    # silently dumping mystery videos into a shared graveyard folder.
    if folder_name == "_unnamed":
        stream.emit([["ERROR: ", "red"],
                     [f"Cannot sync '{channel.get('url', '<no url>')}' \u2014 "
                      "channel has no name or folder_override. Edit the "
                      "channel in the Subs tab and set a name first.\n", "red"]])
        return SyncResult(ok=False, reason="blank channel name",
                          downloaded=0, errors=0)
    ch_dir = base_dir / folder_name
    ch_dir.mkdir(parents=True, exist_ok=True)

    # Pre-flight disk checks (mirrors YTArchiver.py:2314/2332).
    # check_directory_writable creates + deletes a probe file so we fail
    # fast with a clear message instead of letting yt-dlp crash mid-download.
    # A minimum of 500MB free keeps room for the largest plausible single
    # video; actual videos are streamed so we don't need to pre-allocate.
    from ..utils import check_directory_writable, check_disk_space
    if not check_directory_writable(str(ch_dir)):
        stream.emit([["ERROR: ", "red"],
                     [f"Cannot write to {ch_dir} \u2014 disk may be full, read-only, or disconnected.\n", "red"]])
        return SyncResult(ok=False, reason="write blocked", downloaded=0, errors=0)
    # 2-tier disk space check. Hard-fail under 100MB
    # (smaller than a typical 720p 10-minute video — downloads WILL
    # fail mid-stream and leave partial files). Soft-warn under 500MB.
    if not check_disk_space(str(ch_dir), 100 * 1024 * 1024):
        stream.emit([["ERROR: ", "red"],
                     [f"Less than 100 MB free at {ch_dir} \u2014 refusing to start download.\n", "red"]])
        return SyncResult(ok=False, reason="disk_low", downloaded=0, errors=0)
    if not check_disk_space(str(ch_dir), 500 * 1024 * 1024):
        stream.emit([["\u26a0 ", "red"],
                     [f"Less than 500 MB free at {ch_dir} \u2014 downloads may fail mid-stream.\n", "red"]])
        # warn but don't block — user may still want to try

    fmt = build_format_string(resolution)
    # Output template matches YTArchiver.py:17257-17267 — files live under
    # the channel folder, optionally split into <year>/[<month>/] subfolders.
    # Crucially, the filename is `%(title)s.%(ext)s` WITHOUT a `[%(id)s]`
    # suffix — this keeps the file naming identical to what the original
    # tkinter app produces, so we're a drop-in replacement that touches
    # the user's archive without creating dupes.
    if split_years and split_months:
        output_template = str(ch_dir
            / "%(upload_date>%Y|Unknown Year)s"
            / "%(upload_date>%m %B|Unknown Month)s"
            / "%(title)s.%(ext)s")
    elif split_years:
        output_template = str(ch_dir
            / "%(upload_date>%Y|Unknown Year)s"
            / "%(title)s.%(ext)s")
    else:
        output_template = str(ch_dir / "%(title)s.%(ext)s")

    cmd = [
        yt,
        "--newline", "--no-quiet",
        "--mtime", # file mtime = YT upload date (matches original)
        # --continue lets yt-dlp resume a partial .part
        # file if the app crashes or is restarted mid-download.
        # Without this flag, a restart discards the partial and
        # re-downloads the whole video. Especially valuable for
        # large 4K files on flaky connections.
        "--continue",
        "--trim-filenames", "200",
        "--format", fmt,
        "--merge-output-format", "mp4",
        "--ppa", "Merger:-c copy",
        "--sleep-requests", "0.25", # match original's throttle
        "--output", output_template,
        *_find_cookie_source(),
        "--ignore-errors",
        "--no-warnings",
        # --write-info-json drops a tiny sidecar next to
        # every downloaded .mp4 carrying the video_id, upload date,
        # description, and tags. Solves the < 100% Video-ID match rate
        # on freshly-downloaded channels — if DLTRACK ever misses a
        # video (transient yt-dlp internal failure, fixup-m3u8 quirk,
        # post-processing edge case), the sidecar lets register_video
        # / backfill_video_ids recover the id from the most authoritative
        # local source. Sidecars are <30 KB each — negligible footprint
        # against multi-GB video files.
        "--write-info-json",
        # Suppress yt-dlp's channel/playlist-level info.json drops. With
        # --write-info-json alone, yt-dlp writes a sidecar for every
        # IE Result it processes — including the channel home page and
        # its /videos, /streams, /shorts tabs. Those entries have no
        # upload_date, so the `%(upload_date>%Y|Unknown Year)s` template
        # writes them into per-channel `Unknown Year/Unknown Month/`
        # subfolders. Useless to the archive (they don't reference any
        # video) and they polluted every channel's tree.
        "--no-write-playlist-metafiles",
        # After each video completes, emit one line we can parse to map
        # title→id (filenames don't carry the ID in drop-in mode). Matches
        # YTArchiver.py:17281 DLTRACK line.
        "--print",
        "after_video:DLTRACK:::%(title)s:::%(uploader)s:::%(upload_date)s:::%(filesize,filesize_approx)s:::%(duration)s:::%(id)s",
    ]
    # Write VTT captions ONLY when auto-transcribe is enabled for this
    # channel. The transcribe fast-path (_try_auto_captions) parses the VTT
    # into the aggregated Transcript.txt and then deletes the .vtt files.
    # If auto-transcribe is off, VTTs would just pollute the archive with
    # no cleanup path — OLD never writes them in that state.
    if channel.get("auto_transcribe"):
        cmd += [
            "--write-auto-subs", "--write-subs",
            "--sub-langs", "en.*,en",
            "--sub-format", "vtt",
            "--convert-subs", "vtt",
        ]
    # Sanitize pipe characters from titles before template evaluation;
    # required when split_years is on because `|` in a title collides with
    # the `%(upload_date>%Y|Unknown Year)s` fallback syntax. Matches
    # YTArchiver.py:17289.
    if split_years:
        cmd += ["--replace-in-metadata", "title", "\\|", "-"]

    # ── Archive + break-on-existing gating ────────────────────
    # Matches YTArchiver.py:23004 exactly: `break_on_existing` is
    # NOT tied to the mode. It's tied to whether the channel has
    # been successfully synced at least once before. First-ever sync
    # of any mode walks the whole channel (to backfill old videos);
    # every sync after that bails at the first archive hit.
    # The earlier "full means full walk forever" logic was wrong — in
    # OLD, full mode means "first sync grabs everything, subsequent
    # syncs only grab new uploads". The download-archive file is what
    # prevents re-downloading across runs.
    cmd += ["--download-archive", str(ARCHIVE_FILE)]
    # sync_complete default flipped False → True was
    # wrong. A channel missing this key (import, migration, or a
    # legacy config) had its fast-path gated on True-by-default,
    # meaning a brand-new or mid-bootstrap channel could take
    # --break-on-existing on its first walk and skip middle
    # videos. False default is the safe pick: fast-path only
    # after an explicit flip from the config-write path below.
    _is_init = bool(channel.get("initialized", False))
    _sync_ok = bool(channel.get("sync_complete", False))
    if channel.get("init_complete", False):
        _sync_ok = True
    if _is_init and _sync_ok:
        cmd.append("--break-on-existing")
    # Date modes additionally constrain by upload date.
    if mode in ("fromdate", "date") and from_date:
        _d = from_date.replace("-", "").replace("/", "")
        if len(_d) >= 8 and _d[:8].isdigit():
            cmd += ["--dateafter", _d[:8]]

    # ── Duration + liveness filter ──────────────────────────────
    # min_dur / max_dur are SECONDS (matches YTArchiver's on-disk schema);
    # yt-dlp --match-filter duration is also in seconds.
    # Use the `>?` / `<?` variants (tolerate missing duration) so we don't
    # silently drop videos whose metadata didn't include `duration`, which
    # matches YTArchiver.py:17249 behavior.
    # Skip both live and upcoming streams — they can't be downloaded mid-
    # stream, and the livestream defer journal catches them for retry later.
    # NOTE: min_dur=0 OR max_dur=0 deliberately means "no limit on that side"
    # (matches the UI semantics where empty = unbounded). Don't add a
    # defensive duration>?0 floor — it would change behavior for existing
    # configs that rely on 0 = unbounded.
    match_parts = ["!is_live", "!is_upcoming"]
    if min_dur > 0:
        match_parts.append(f"duration>?{min_dur}")
    if max_dur > 0:
        match_parts.append(f"duration<?{max_dur}")
    cmd += ["--match-filter", " & ".join(match_parts)]

    # Build the list of yt-dlp URLs to run. OLD does a second pass against
    # /streams after the main /videos enumeration (YTArchiver.py:6761) so
    # past livestreams that YouTube filed under /streams (not /videos) are
    # also caught. `streams_url` returns None for non-channel URLs.
    from .. import subs as _subs
    _urls_to_run: list[str] = [url]
    _streams_url = _subs.streams_url(url)
    if _streams_url and _streams_url != url:
        _urls_to_run.append(_streams_url)

    # prepend explicit retry URLs for video IDs that failed
    # on a prior sync pass. Without this, --break-on-existing on later
    # syncs hits a newer archived video and stops walking, leaving the
    # failed videos permanently un-retried. failed_video_ids is keyed by
    # vid → retry_count so we can drop IDs that fail 3+ times in a row
    # (likely deleted / region-locked / permanently broken).
    _prior_failed = channel.get("failed_video_ids") or {}
    if isinstance(_prior_failed, list):
        # Legacy shape — flat list. Convert to dict on the fly.
        _prior_failed = {v: 1 for v in _prior_failed if isinstance(v, str)}
    _retry_vids = [v for v, c in _prior_failed.items()
                   if isinstance(c, int) and 0 < c < 3]
    if _retry_vids:
        stream.emit([
            [" ↻ Retrying ", "simpleline_green"],
            [f"{len(_retry_vids)}", "simpleline_green"],
            [" previously failed video", "simpleline"],
            ["(s)\n" if len(_retry_vids) != 1 else "\n", "simpleline"],
        ])
        # Prepend so retries run before the main channel walk.
        _retry_urls = [f"https://www.youtube.com/watch?v={_v}"
                        for _v in _retry_vids]
        _urls_to_run = _retry_urls + _urls_to_run

    t_start = time.time()
    # Per-channel [Sync] Starting / Done emits are gone — sync_start_all's
    # loop now renders a single-line `[N/total] Name \u2014 summary` row
    # that updates in-place. The URL still logs as a verbose-only trace.
    stream.emit([[f" URL: {url}\n", "dim"]])

    # Pause if we know the network is down (don't waste yt-dlp on dead pipes)
    try:
        from .. import net as _net
        _net.block_if_down(stream=stream,
                            check_cancel=lambda: cancel_event and cancel_event.is_set())
    except Exception as e:
        _log.debug("swallowed: %s", e)

    # Counters that must persist across the /videos + /streams passes.
    downloaded = 0
    errors = 0
    # count archived-skip hits (--break-on-existing line)
    # so SyncResult can report a real `total` = downloaded + skipped.
    # Used by the batch-cooldown heuristic in sync_all().
    _archived_skipped = 0
    # track which video IDs hit an error during this run.
    # At end of sync we diff against successful downloads and persist
    # the leftovers into channel["failed_video_ids"] so the next sync
    # pass can prepend them as explicit retry targets — otherwise
    # --break-on-existing skips past chronologically-newer successes
    # and these videos are silently lost.
    _failed_this_run: set = set()
    current_title = ""
    dest_path = ""
    # Path captured from `[Merger] Merging formats into "PATH"` — this is
    # the AUTHORITATIVE final output path. Mirrors OLD YTArchiver's
    # `current_merge_dest` (see YTArchiver.py:18625). Without this, the
    # DLTRACK handler was derived from the last Destination line via a
    # regex strip, which silently gave a wrong path for any format with
    # a dashed suffix (`.f140-16.m4a`, `.f251-drc.webm`, etc.) and dropped
    # the download from the counter + recent tab + transcribe queue.
    merge_dest_path = ""
    # Per-video counter for in-place dl lines. Bumps every time a new
    # merged .mp4 is announced. Each video's Downloading-line +
    # progress ticks + final "✓ done" line share ONE `dlrow_<N>`
    # inplace kind so they all replace each other at a single position
    # in the log. Matches OLD YTArchiver behavior where the progress
    # bar replaces the Downloading line and the done line then replaces
    # the bar — one row per video, not three.
    # NB: uses the MODULE-level `_DLROW_COUNTER` (not a fresh local) so
    # counters don't collide across channel boundaries. If channel A
    # emitted `dlrow_1…dlrow_3`, channel B starts at `dlrow_4`.
    # `_path_to_counter` stays local because it's only queried within
    # this channel's DLTRACK flow.
    _path_to_counter: dict[str, int] = {}
    # Display title per-path, stashed by the Destination branch so the
    # Progress branch can rebuild the full "— Downloading <title> NN%"
    # line. Without this the progress tick has no access to the title
    # (current_title has the [id] suffix still attached, and display_title
    # is a local inside the Destination `if` block). A small dict, one
    # entry per merged .mp4, cleared at channel end with the other
    # per-channel state.
    _path_to_display_title: dict[str, str] = {}
    # Parallel lookup: video_id -> dlrow counter. Path-based matching can
    # fail when yt-dlp's filename sanitization substitutes characters
    # differently across the Destination and Merger lines (e.g. `:` in a
    # title becomes `：` U+FF1A or `?` depending on yt-dlp version and
    # output codepage). The vid is always emitted clean in `[youtube]
    # VIDID:` lines and again in the DLTRACK record, so it's a more
    # reliable join key. Filled at Destination time from `current_vid_id`
    # (tracked from preceding `[youtube]`/`[info]`/`[download]` lines),
    # consulted at DLTRACK as a fallback when path-match misses.
    _vid_to_counter: dict[str, int] = {}
    # Most-recently-seen video id from yt-dlp output. Always brackets
    # the Destination line that triggers the dlrow creation, so it's the
    # correct id to associate with the new dlrow.
    current_vid_id: str = ""
    # dlrow_N values that have already been CLOSED by a DLTRACK ✓ done
    # emit. yt-dlp sometimes dribbles a final "[download] 100% of X in
    # Y" progress line AFTER the DLTRACK has fired — if we let it
    # through, the late progress tick emits with the SAME dlrow_N
    # marker, inplace-replacing the done line and leaving the user
    # staring at "Downloading Title 100%" with no ✓. Hit on 2-video
    # channels where video 1's done got clobbered by a late tick
    # before video 2's Destination bumped the counter. Once a dlrow_N
    # is in this set, subsequent progress ticks targeting it are
    # dropped.
    _closed_dlrows: set = set()
    downloaded_ids: list[str] = [] # fast-path metadata target list
    # title -> video_id, filled from DLTRACK::: emitted after each video.
    # Needed because filenames no longer carry the [id] suffix (drop-in mode).
    _title_to_id: dict[str, str] = {}
    # Dedupe per-video "Downloading" announcements — yt-dlp emits a
    # "Destination:" line for every intermediate track (.f137, .f139) plus
    # the merge target, so we'd otherwise log 3 lines per video and
    # triple-count the `downloaded` tally. Keyed by merged .mp4 path.
    _title_announced: dict[str, bool] = {}

    # ── Inline metadata pipeline ──────────────────────────────────────
    # Design intent: "when a sync download kicks out a metadata
    # task, we shouldn't have to walk entire channels. We know the exact
    # video ID that we just downloaded. We should be able to 'snipe' that
    # information." And he wants the log to read like:
    # \u2014 \u2713 Title Channel 04.18.26 (26 MB)
    # \u2014 Metadata downloaded
    # So we run a single-worker ThreadPoolExecutor per sync pass: each
    # DLTRACK submits a task that fetches metadata for JUST that video
    # (no channel walk). One-at-a-time keeps us from hammering yt-dlp.
    # Transcription stays kicked out to the GPU queue because it's
    # genuinely compute-heavy.
    _meta_exec = None
    _meta_counts = {"fetched": 0, "skipped": 0, "errors": 0}
    # lock around _meta_counts mutations + reads. The
    # executor is single-worker today so increments are serialized, but
    # the main thread also reads _meta_counts.get("fetched") at the
    # consolidated-row emit below — that read races the worker write.
    # Lock makes the read-after-write deterministic and future-proofs
    # if max_workers is ever bumped above 1.
    _meta_counts_lock = threading.Lock()
    def _bump_meta_counts(key: str) -> None:
        with _meta_counts_lock:
            _meta_counts[key] = _meta_counts.get(key, 0) + 1
    def _read_meta_count(key: str) -> int:
        with _meta_counts_lock:
            return int(_meta_counts.get(key, 0) or 0)
    if channel.get("auto_metadata"):
        try:
            from concurrent.futures import ThreadPoolExecutor
            _meta_exec = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="yta-meta")
        except Exception:
            _meta_exec = None

    # Patch F (followup): defensive leak guard. The explicit
    # _meta_exec.shutdown(wait=True) at the function's happy-path end
    # (line ~1880) handles normal completion. But this function is
    # 1700+ lines with 11 return statements, and an exception escaping
    # before the explicit shutdown leaves the executor's worker thread
    # orphaned until interpreter shutdown. _ExecGuard's __del__ fires
    # when this function's frame is reclaimed (immediately on return
    # under CPython refcount semantics), ensuring a non-blocking
    # shutdown even on exception paths. Idempotent — calling shutdown
    # twice is safe.
    class _ExecGuard:
        __slots__ = ("_exec",)
        def __init__(self, ex): self._exec = ex
        def __del__(self):
            try:
                # wait=True so the guard cleanup actually blocks until
                # any in-flight metadata task finishes before the next
                # sync_channel call starts. cancel_futures=True drops
                # any not-yet-started work. The earlier wait=False
                # variant let already-running tasks keep mutating
                # channel config AFTER sync_channel had returned —
                # racing the next channel's config writes (audit:
                # sync/core.py:644).
                self._exec.shutdown(wait=True, cancel_futures=True)
            except Exception:
                pass
    _meta_guard = _ExecGuard(_meta_exec) if _meta_exec is not None else None

    def _submit_inline_metadata(vid_id: str, title: str, final_path: str):
        """Dispatch a per-video metadata fetch onto the meta executor.
        No-op when auto_metadata is off. Never raises."""
        if _meta_exec is None or not vid_id or not final_path:
            return

        def _task():
            try:
                # bail early on BOTH cancel AND pause so a
                # backlog of metadata fetches doesn't "catch up" with a
                # wall of "Metadata downloaded" lines after the user
                # clicks Pause. Cancel is permanent; pause is
                # recoverable — re-submit the task to the front of a
                # pending-on-resume list. For now we drop on pause
                # rather than persist; the metadata refresh will
                # re-pick it up via the next sync pass.
                if cancel_event is not None and cancel_event.is_set():
                    return
                if pause_event is not None and pause_event.is_set():
                    return
                from .. import metadata as _meta
                res = _meta.fetch_single_video_metadata(
                    channel, vid_id, final_path, title, stream)
                if res.get("ok") and res.get("fetched"):
                    _bump_meta_counts("fetched")
                elif res.get("ok") and res.get("skipped"):
                    _bump_meta_counts("skipped")
                else:
                    _bump_meta_counts("errors")
            except Exception:
                _bump_meta_counts("errors")
            # Now that metadata + thumbnail are on disk, re-push the
            # Recent tab so the grid-card view picks up the new
            # thumbnail. Without this, recent_for_ui runs its
            # find_thumbnail() scan BEFORE the jpg is written and the
            # card renders with an empty gradient placeholder forever.
            # Best-effort — hook is only set in app runtime.
            from .recent_track import fire_recent_changed_hook
            fire_recent_changed_hook()

        try:
            _meta_exec.submit(_task)
        except Exception as e:
            _log.debug("swallowed: %s", e)

    # track returncode per pass instead of relying on
    # `proc.returncode` after the loop (which reads the LAST proc only;
    # may also be None if the loop broke via proc.terminate without
    # wait). Empty list means no pass ran (pure cancel before launch).
    _pass_returncodes: list[int | None] = []

    # Run yt-dlp once per target URL (main channel + optional /streams pass).
    # v69.3 fix: initialize `proc` BEFORE the loop so the post-loop
    # `_exit_for_caller = ... proc.returncode if proc else 0` below
    # doesn't UnboundLocalError when cancel_event / pause_event fires on
    # the first iteration's gate (lines 813/823) before reaching the
    # per-iteration `proc = None` at L837. Reported: "Sync crashed:
    # cannot access local variable 'proc' where it is not associated
    # with a value" when resuming a paused sync.
    proc = None
    for _pass_idx, _target_url in enumerate(_urls_to_run):
        if cancel_event is not None and cancel_event.is_set():
            break
        # If the user paused during the main pass (yt-dlp's stdout
        # loop broke out after emitting its "Paused — stopping
        # current download" line), do NOT proceed to /streams.
        # Otherwise /streams launches a fresh yt-dlp which hits
        # the pause check on its very first stdout iteration and
        # emits a second "Paused — stopping current download" line
        # that the user never asked for. Reported: 2x "Paused
        # stopping current download" + 1x "Sync paused at H:MMpm".
        if pause_event is not None and pause_event.is_set():
            break
        if _pass_idx > 0:
            # /streams header is verbose-only diagnostic. All segments
            # tagged 'dim' so `_line_is_verbose_only` drops the line in
            # Simple mode. In Verbose mode the user sees it with
            # bracket-style colors (handled by the 'dim' CSS rule).
            stream.emit([
                [" [Streams] ", "dim"],
                [f"Checking {_target_url} for past livestreams...\n", "dim"],
            ])
        cmd_this = cmd + [_target_url]

        # Retry loop for transient launch failures
        proc = None
        for attempt in range(3):
            if cancel_event is not None and cancel_event.is_set():
                # Audit fix (sync/core.py:757): close the previous-iteration
                # proc handle before bailing. Without this, a /streams retry
                # cancel left the /videos pass's Popen object + its stdout
                # pipe dangling until GC. _ExecGuard handles the metadata
                # executor; this just covers the yt-dlp proc resources.
                try:
                    if proc is not None:
                        try:
                            if proc.stdout is not None:
                                proc.stdout.close()
                        except Exception:
                            pass
                        try:
                            if proc.poll() is None:
                                proc.terminate()
                        except Exception:
                            pass
                except Exception:
                    pass
                return SyncResult(ok=False, reason="cancelled",
                                  downloaded=downloaded, errors=errors)
            try:
                # Binary mode (no `encoding=`) so we can decode each line
                # ourselves — yt-dlp.exe (frozen) doesn't always respect
                # PYTHONIOENCODING, so we try UTF-8 first and fall back
                # to cp1252 via `_utils.decode_subprocess_line` below.
                proc = subprocess.Popen(
                    cmd_this, stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    # bufsize=0 (unbuffered) for the binary pipe. Old
                    # bufsize=1 is line-buffering, which Python only
                    # honors in TEXT mode — in binary mode it falls
                    # back to block buffering with a DeprecationWarning,
                    # causing yt-dlp's "Downloading <title> NN%"
                    # inplace progress to arrive in bursts instead of
                    # streaming (audit: sync/core.py:764).
                    bufsize=0, startupinfo=_startupinfo,
                    env=_utils.utf8_subprocess_env(),
                )
                # register with ProcessRegistry so
                # shutdown's kill_all() reaps cleanly without the
                # psutil child-scanning fallback.
                try:
                    from ..process_runner import PROCESS_REGISTRY
                    PROCESS_REGISTRY.register(proc)
                except Exception as _re:
                    _log.debug("swallowed: %s", _re)
                break
            except OSError as e:
                if attempt == 2:
                    stream.emit([["ERROR: ", "red"], [f"Couldn't start the download tool after 3 tries: {e}\n", "red"]])
                    # Main pass failed — can't continue; /streams pass skipped
                    if _pass_idx == 0:
                        return SyncResult(ok=False, reason="launch failed",
                                          downloaded=0, errors=0)
                    proc = None
                    break
                stream.emit_dim(f" Launch attempt {attempt+1} failed ({e}); retrying in 2s...")
                time.sleep(2)
        if proc is None:
            continue # streams pass launch failed — skip, main pass completed

        # Manual line iteration on the bytes stream so we can apply our
        # UTF-8-first-cp1252-fallback decoder (`_utils.decode_subprocess_line`).
        for _line_bytes in iter(proc.stdout.readline, b""):
            line = _utils.decode_subprocess_line(_line_bytes)
            if cancel_event is not None and cancel_event.is_set():
                try:
                    proc.terminate()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                stream.emit([[" \u26d4 Cancelled.\n", "red"]])
                break

            # Pause-immediate — terminate the yt-dlp subprocess the
            # moment the user hits Pause. Without this, pause only
            # took effect BETWEEN channels (sync_all's `_wait_if_paused`
            # gate at the top of each iteration), so pressing pause
            # mid-download made the user wait for the current channel
            # to finish all of its videos. yt-dlp's `--continue`
            # handles the partial on next run. Pause is surfaced to
            # the user by `_wait_if_paused()` which emits the
            # "Sync paused at H:MMpm — click Resume." line once the
            # outer loop advances — no need for a noisy "Paused —
            # stopping current download" line here.
            if pause_event is not None and pause_event.is_set():
                try:
                    proc.terminate()
                except Exception as e:
                    _log.debug("yt-dlp terminate on pause failed: %s", e)
                break

            s = line.rstrip()
            if not s:
                continue

            # Track video_id across the lifecycle of the current video so
            # the Destination handler below can store it alongside the
            # dlrow counter. yt-dlp prints `[youtube] VIDID:`, `[info]
            # VIDID:`, and `[download] VIDID:` lines BEFORE the Destination
            # line, so by the time Destination fires, current_vid_id is
            # the correct id for that video. Used by DLTRACK's path-match
            # fallback when filename character substitution (`:` -> `：`
            # etc) causes the path lookup to miss.
            # `[download] Destination: foo.mp4` is a known false positive:
            # the word `Destination` is exactly 11 letters and matches
            # the [A-Za-z0-9_-]{11} class. Real YT video IDs are base64-
            # url, which is letters + digits + `-_` — they almost always
            # contain at least one non-letter. Rejecting all-alpha
            # candidates filters the "Destination" footgun without
            # rejecting legitimate IDs in practice.
            _vid_m = _VIDID_RE.search(s)
            if _vid_m:
                _vid_cand = _vid_m.group(1)
                if not _vid_cand.isalpha():
                    current_vid_id = _vid_cand

            # yt-dlp emits informational chatter that looks
            # error-ish to users — [info] subtitle writes, [Merger] /
            # [Remuxer] / [FixupM3u8] lines, "Deleting original file"
            # cleanups. These are normal output for every successful
            # download. The error counter ignores them, but they were
            # leaking into Simple mode through downstream fall-through
            # paths in some yt-dlp versions. Always tag as dim early so
            # `_line_is_verbose_only` reliably drops them in Simple mode.
            _s_strip = s.lstrip()
            if (_s_strip.startswith("[info]")
                    or _s_strip.startswith("[Merger]")
                    or _s_strip.startswith("[Remuxer]")
                    or _s_strip.startswith("[FixupM3u8]")
                    or _s_strip.startswith("[ExtractAudio]")
                    or _s_strip.startswith("[Metadata]")
                    or _s_strip.startswith("Deleting original file")):
                # Still capture [Merger] path before suppressing the
                # visible line — downstream code reads merge_dest_path.
                if _s_strip.startswith("[Merger]"):
                    _mm0 = _MERGE_RE.search(s)
                    if _mm0:
                        merge_dest_path = _mm0.group(1).strip()
                        # Cross-stamp _path_to_counter so the DLTRACK
                        # lookup can resolve the Merger's path even when
                        # yt-dlp sanitized the intermediate .fNNN
                        # Destination paths differently than the merged
                        # output (observed: title with `"` rendered as
                        # bare-stripped in the .f137 sub-track filename
                        # but as fullwidth `＂` in the merged .mp4
                        # output → path-match misses → DLTRACK orphan).
                        # The "youngest pending" entry in _title_announced
                        # is this video (yt-dlp processes sequentially
                        # within a subprocess) so cloning its counter
                        # under the Merger path repairs the join.
                        for _pp, _pv in _title_announced.items():
                            if _pv == "pending":
                                _existing_n = _path_to_counter.get(_pp)
                                if _existing_n is not None:
                                    _path_to_counter[merge_dest_path] = _existing_n
                                break
                stream.emit([[f" {s}\n", "dim"]])
                continue

            # ── Cookie / sign-in error detection ───────────────────────
            # yt-dlp surfaces stale-cookie / signed-out-of-YouTube
            # failures in a few ways, all caught here:
            # "ERROR: [youtube] <id>: Sign in to confirm you're not a bot"
            # "ERROR: unable to extract cookies from firefox"
            # "WARNING: cookies are missing/invalid"
            # Classic YTArchiver flagged these with a big red block
            # telling the user to sign back in; overhaul was silent,
            # so the user could run stale-cookie syncs for days with
            # no visible feedback. Fires ONCE per sync pass
            # (`_COOKIE_ALERT_FIRED`) — subsequent channels don't
            # re-emit the same banner. Does NOT set cancel_event:
            # public videos still download without auth, so letting
            # the pass continue is strictly better than aborting.
            global _COOKIE_ALERT_FIRED
            # Patch A: atomic check-and-set under _cookie_alert_lock so
            # concurrent channel-syncs don't both fire the banner.
            with _cookie_alert_lock:
                _should_emit_cookie_alert = False
                if not _COOKIE_ALERT_FIRED:
                    _sl = s.lower()
                    if (("sign in to confirm" in _sl)
                        or ("cookies are missing" in _sl)
                        or ("cookies are invalid" in _sl)
                        or ("failed to extract any player response" in _sl
                            and "sign in" in _sl)
                        or ("error:" in _sl and "cookie" in _sl
                            and ("extract" in _sl or "sign in" in _sl))):
                        _COOKIE_ALERT_FIRED = True
                        _should_emit_cookie_alert = True
            if _should_emit_cookie_alert:
                _bar = "\u2588" * 65
                stream.emit([["\n" + _bar + "\n", "red"]])
                stream.emit([["\u2588  ", "red"],
                             ["Browser is signed out of YouTube.",
                              "red"],
                             ["\n", "red"]])
                stream.emit([["\u2588  ", "red"],
                             ["Sign in to YouTube in Firefox (or your "
                              "default browser) so YTArchiver can",
                              "red"],
                             ["\n", "red"]])
                stream.emit([["\u2588  ", "red"],
                             ["reuse the session cookie. Public "
                              "videos still download without signing in.",
                              "red"],
                             ["\n", "red"]])
                stream.emit([[_bar + "\n\n", "red"]])

            # DLTRACK:::Title:::Uploader:::YYYYMMDD:::bytes:::secs:::videoID
            # Emitted by yt-dlp's --print after_video:... directive ONLY
            # when a video actually downloads + merges successfully. If
            # the file was already on disk (archive hit, or --no-overwrites
            # skip), DLTRACK does not fire — so this is the authoritative
            # "real download" signal.
            # ── Merger line capture ─────────────────────────────────
            # `[Merger] Merging formats into "PATH"` is the AUTHORITATIVE
            # final output path — yt-dlp emits it once after combining
            # video+audio tracks. We prefer this over Destination-line
            # parsing because the Destination line shows per-track
            # intermediates (`.f140-16.m4a`) whose suffix shape isn't
            # always strippable. Mirrors YTArchiver.py:18625.
            _mm = _MERGE_RE.search(s)
            if _mm:
                merge_dest_path = _mm.group(1).strip()
                # Cross-stamp _path_to_counter — see the matching block
                # in the verbose-suppress filter above for the full why.
                # Belt-and-suspenders: this fallback capture site fires
                # for [Merger] variants that didn't hit the suppress
                # filter (e.g. tools that wrap yt-dlp output with
                # additional prefixes), so it needs the same cross-stamp.
                for _pp, _pv in _title_announced.items():
                    if _pv == "pending":
                        _existing_n = _path_to_counter.get(_pp)
                        if _existing_n is not None:
                            _path_to_counter[merge_dest_path] = _existing_n
                        break
                # Also cover Remuxer / FixupM3u8 — same semantic meaning.
                # Fall through so the line still gets logged.

            if s.startswith("DLTRACK:::"):
                try:
                    # Patch A: validate field count before unpacking.
                    # yt-dlp's DLTRACK template emits exactly 7 ::: -
                    # separated fields. A malformed line (yt-dlp format
                    # drift, unusual metadata, etc.) previously raised
                    # ValueError straight into the swallowing except
                    # below — the download silently failed to register.
                    # Now: warn + continue so the rest of the for-loop
                    # processes other lines normally.
                    _parts = s.split(":::", 6)
                    if len(_parts) != 7:
                        _log.warning(
                            "DLTRACK malformed (%d parts, expected 7): %s",
                            len(_parts), s[:200])
                        continue
                    _, t, upl, ud, sz, dur, vid = _parts
                    t = (t or "").strip()
                    vid = (vid or "").strip()
                    if t and vid:
                        _title_to_id[t] = vid
                        if vid not in downloaded_ids:
                            downloaded_ids.append(vid)
                    # Priority 1: merge_dest_path (captured from [Merger]
                    # line, the authoritative final .mp4).
                    # Priority 2: Destination-line strip via
                    # _resolve_final_mp4 (handles .fNNN / .fNNN-X suffixes).
                    # Priority 3: Scan channel folder for the most recent
                    # video file whose ctime is within 10 min — covers
                    # edge cases where neither merger nor destination gave
                    # a usable path (FixupM3u8 remux of a stream, etc.).
                    final_path = None
                    if merge_dest_path and os.path.isfile(merge_dest_path):
                        final_path = merge_dest_path
                    elif dest_path:
                        final_path = _resolve_final_mp4(dest_path)
                    if (not final_path or not os.path.isfile(final_path)):
                        # Fallback directory scan — mirrors OLD line 18350.
                        final_path = _scan_recent_video(ch_dir)
                    if final_path and os.path.isfile(final_path) and vid:
                        # First DLTRACK per final_path wins — counts the
                        # download, emits the "\u2014 \u2713" confirmation with
                        # title / channel / size / path / URL / duration
                        # (mirrors verbose emit),
                        # and upgrades the tri-state (missing / "pending" / True).
                        _prev = _title_announced.get(final_path)
                        if _prev is not True:
                            _title_announced[final_path] = True
                            _hide_sidecar_win(final_path)
                            downloaded += 1
                            # Companion display would read downloaded=0
                            # for entire channel-length syncs because
                            # write_sync_progress was only called once
                            # at channel start (and once at end). Push
                            # the +1 increment per real video here so
                            # single-channel syncs (and bulk syncs)
                            # both surface live DL counters.
                            try:
                                write_sync_progress(
                                    channel_name=name,
                                    idx=int(pass_idx or 1),
                                    total=max(int(pass_total or 1),
                                              int(pass_idx or 1)),
                                    downloaded=1, skipped=0, errors=0)
                            except Exception as e:
                                _log.debug("swallowed: %s", e)
                            _display = (t or re.sub(
                                r"\s*\[[A-Za-z0-9_-]{11}\]\s*$", "",
                                os.path.splitext(os.path.basename(final_path))[0]
                            ) or os.path.basename(final_path))
                            # File size on disk — authoritative (DLTRACK fires
                            # after merge is complete).
                            try:
                                _size_bytes = os.path.getsize(final_path)
                            except OSError:
                                _size_bytes = 0
                            _size_str = _fmt_size(_size_bytes) if _size_bytes else ""
                            _size_tag = f" ({_size_str})" if _size_str else ""
                            # Duration pretty-format (mirrors OLD line 18476).
                            _dur_str = ""
                            try:
                                _ds = int(float(dur)) if dur else 0
                                if _ds > 0:
                                    _dm, _ds2 = divmod(_ds, 60)
                                    _dh, _dm2 = divmod(_dm, 60)
                                    _dur_str = (f"{_dh}h {_dm2:02d}m {_ds2:02d}s"
                                                 if _dh else f"{_dm2}m {_ds2:02d}s")
                            except (TypeError, ValueError):
                                pass
                            _video_url = f"https://www.youtube.com/watch?v={vid}"
                            # Title line — always emit (OLD does this in both modes).
                            # simple mode: single dense line with title/size
                            # "— ✓ <title> (NNN MB)"
                            # verbose mode: same line + Path/URL/Duration subfields
                            # (Channel name is already shown in the [Dwnld]
                            # header line above the block, so repeating it
                            # here was just extra noise.)
                            # Inplace: reuse the `dlrow_<N>` kind that the
                            # Downloading emit used, so the ✓ done line
                            # REPLACES the Downloading line in simple mode
                            # (matches OLD's single-line-per-download pattern).
                            # Path-match robustness — literal key lookup
                            # can miss when the Merger/DLTRACK path differs
                            # from the Destination path by slash style,
                            # casing, or an abs-vs-rel flip. Fall back to
                            # a basename match across all announced paths.
                            # If STILL no match, we must NOT fall through
                            # to `_DLROW_COUNTER` — by the time this
                            # DLTRACK fires, yt-dlp may have already
                            # started extracting the NEXT video (its
                            # Destination bumped the counter), so the
                            # current counter no longer points to this
                            # video's row. Lucy observed: video 1's
                            # done line replaced video 2's progress row,
                            # orphaning video 1's progress row as a
                            # stuck "Downloading 100%". A fresh unique
                            # marker is strictly safer — worst case the
                            # done line appears at the log bottom,
                            # leaving the old progress line in place
                            # (tolerable, much better than cross-wiring).
                            _dlrow_n = _path_to_counter.get(final_path)
                            if _dlrow_n is None:
                                _bn = os.path.basename(final_path).lower()
                                for _k, _v in _path_to_counter.items():
                                    if os.path.basename(_k).lower() == _bn:
                                        _dlrow_n = _v
                                        break
                            if _dlrow_n is None and vid:
                                # Filename-substitution fallback: when path
                                # comparison fails (e.g. yt-dlp rendered `:`
                                # as `：` in one line and `?` in another),
                                # match by video_id. yt-dlp guarantees the
                                # same id appears in both the [youtube]
                                # VIDID: line and the DLTRACK record, so this
                                # is the most reliable join key.
                                _dlrow_n = _vid_to_counter.get(vid)
                            if _dlrow_n is None:
                                # Last-resort: youngest pending counter.
                                # yt-dlp processes videos sequentially
                                # inside a single subprocess, so the
                                # only `_title_announced[path] ==
                                # "pending"` entry at DLTRACK time is
                                # this video. Catches cases where path,
                                # basename, AND vid lookups all missed
                                # (e.g. Destination intermediate paths
                                # were sanitized differently than the
                                # Merger output, AND `[youtube] VIDID:`
                                # wasn't captured before Destination
                                # fired). Iterating in insertion order
                                # picks the most recent pending entry.
                                _pending_n = None
                                for _pp, _pv in _title_announced.items():
                                    if _pv == "pending":
                                        _pn = _path_to_counter.get(_pp)
                                        if _pn is not None:
                                            _pending_n = _pn
                                _dlrow_n = _pending_n
                            if _dlrow_n is None:
                                _done_kind = f"dlrow_orphan_{vid or id(final_path)}"
                                # Diagnostic only — Simple mode never
                                # sees this (dim → verbose-only). The
                                # noisy `_log.warning` that doubled this
                                # line in Simple mode was demoted to
                                # debug to stop scaring the user when
                                # the failure is purely cosmetic (the
                                # download itself succeeded; the
                                # orphaned in-place row gets cleaned up
                                # by the post-channel sweep).
                                try:
                                    stream.emit_dim(
                                        f" ⚠ DLTRACK orphan: no dlrow "
                                        f"match for vid={vid!r} "
                                        f"path={final_path!r}")
                                except Exception as _de:
                                    _log.debug(
                                        "dlrow orphan warning emit failed: %s",
                                        _de)
                                _log.debug(
                                    "DLTRACK orphan: vid=%s path=%s",
                                    vid, final_path)
                                # Path-match fell through to orphan. The
                                # done line lands at log bottom; the
                                # actual orphaned in-place row (whichever
                                # dlrow this video was anchored to) gets
                                # cleaned up by the post-channel sweep
                                # at the end of sync_channel — sweeping
                                # `_path_to_counter.values() - _closed_dlrows`
                                # catches every dlrow that never got a
                                # done-line, regardless of how the path
                                # match failed. Trying to guess the
                                # right counter HERE was wrong: when
                                # video 1's DLTRACK fires after video
                                # 2's Destination, _DLROW_COUNTER points
                                # to video 2, so clearing
                                # `dlrow_{_DLROW_COUNTER}` would wipe
                                # video 2's actively-downloading row.
                            else:
                                _done_kind = f"dlrow_{_dlrow_n}"
                                # Close this dlrow — any further progress
                                # ticks (late 100% etc) will be dropped
                                # so they can't clobber this done line.
                                _closed_dlrows.add(_dlrow_n)
                            stream.emit([
                                [" ", ["dim", _done_kind]],
                                ["\u2014 \u2713 ", ["simpleline_green", _done_kind]],
                                [f"{_display}", ["simpleline", _done_kind]],
                                [f"{_size_tag}\n", ["dim", _done_kind]],
                            ])
                            # Path / URL / Duration — verbose-only sub-details
                            # (all `dim`, which `_line_is_verbose_only` drops
                            # in simple mode).
                            try:
                                _rel_fp = os.path.relpath(final_path)
                            except ValueError:
                                _rel_fp = final_path
                            stream.emit([[" Path: ", "dim"],
                                         [f"{_rel_fp}\n", "dim"]])
                            stream.emit([[" URL: ", "dim"],
                                         [f"{_video_url}\n", "dim"]])
                            if _dur_str:
                                stream.emit([[" Duration: ", "dim"],
                                             [f"{_dur_str}\n", "dim"]])
                        try:
                            from .. import index as _idx
                            # pass the duration through so
                            # duration_s lands in the videos table.
                            # `dur` at this point is a float (seconds)
                            # coming from yt-dlp's DLTRACK line.
                            try:
                                _dur_val = float(dur) if dur else None
                            except (TypeError, ValueError):
                                _dur_val = None
                            _idx.register_video(
                                final_path, name, t,
                                tx_status="pending" if auto_tx else "no_captions",
                                video_id=vid,
                                duration_secs=_dur_val)
                        except Exception as _re:
                            # surface the failure so the user
                            # knows a download they just saw succeed is
                            # actually invisible in Browse/Search. Old
                            # code silently swallowed this.
                            stream.emit_dim(
                                f" (index register failed for {t!r}: {_re})")
                        try:
                            # Pass size + duration through so the function
                            # doesn't need to spawn ffprobe / re-stat the
                            # video file. DLTRACK already gave us both
                            # (`_size_bytes` was just computed above for
                            # the ✓ line, `_dur_val` is `dur` from the
                            # DLTRACK record). Skipping ffprobe matters
                            # when Z: is contended by the boot sweep —
                            # the subprocess can otherwise stall 5+
                            # seconds per download.
                            _rec_size = _size_bytes if _size_bytes else None
                            _record_recent_download(final_path, name, t, vid,
                                                    upload_date=(ud or "").strip(),
                                                    size_bytes=_rec_size,
                                                    duration_secs=_dur_val)
                        except Exception as _re2:
                            # Recent tab goes stale silently
                            # on any cache write failure without this.
                            stream.emit_dim(
                                f" (recent downloads write failed: {_re2})")
                        # if this vid was previously deferred
                        # as a livestream/premiere, drop it from the
                        # deferred journal now that we've successfully
                        # grabbed the recording. Without this, the
                        # Deferred Livestreams drawer accumulates stale
                        # entries forever; only the manual Ignore button
                        # ever removed them.
                        try:
                            from .. import livestreams as _ls
                            _ls.drop(vid)
                        except Exception as e:
                            _log.debug("swallowed: %s", e)
                        # Issues #139/#144/#148: emit a meta_done_<vid>
                        # placeholder BEFORE the metadata fetch fires
                        # async. The done line tags the same marker so
                        # it lands AT the placeholder's position rather
                        # than scrolling in below later channels'
                        # rows. Mirrors the tx_done_<vid> pattern.
                        _meta_marker = f"meta_done_{vid}" if vid else ""
                        if _meta_marker:
                            stream.emit([
                                [" — ⏳ ",
                                 ["meta_bracket", _meta_marker]],
                                ["Metadata queued…\n",
                                 ["simpleline", _meta_marker]],
                            ])
                        # Inline metadata fetch — no channel walk.
                        _submit_inline_metadata(vid, t, final_path)
                        # If auto_transcribe is OFF, remember the video ID on
                        # the channel so Queue Pending can later snipe the
                        # exact file without folder-scanning. Spec: the
                        # Queue Pending ticker counts up when a channel
                        # without auto transcription enabled downloads a
                        # video.
                        if not auto_tx:
                            try:
                                from .. import ytarchiver_config as _cfg
                                _cfg.append_pending_tx_id(name, vid)
                            except Exception as _re3:
                                # pending transcribe list
                                # silently loses the ID otherwise —
                                # user would later see "Queue Pending"
                                # miss this video without knowing why.
                                stream.emit_dim(
                                    f" (pending-transcribe list write failed: {_re3})")
                        # Auto-transcribe: queue the completed video for whisper
                        if auto_tx and transcribe_mgr is not None:
                            cb = None
                            if channel.get("compress_enabled"):
                                _comp_lvl = channel.get("compress_level") or "Average"
                                _comp_res = str(channel.get("compress_output_res") or "720")
                                def _chain_compress(_result, _fp=final_path, _q=_comp_lvl,
                                                    _r=_comp_res, _s=stream, _ce=cancel_event):
                                    try:
                                        from .. import compress as _cmp
                                        _cmp.compress_video(_fp, _s, quality=_q, output_res=_r,
                                                             cancel_event=_ce)
                                    except Exception as _e:
                                        _s.emit_error(f"Auto-compress failed: {_e}")
                                cb = _chain_compress
                            # Reserve a slot in the log for the transcription
                            # completion line so it renders under THIS channel's
                            # block when the async GPU job finishes — not
                            # interleaved with later channels' "no new videos"
                            # rows or orphaned at the bottom of the log.
                            # `_inplaceKind` prioritizes `tx_done_` so when the
                            # done line emits with `["dim", whisper_job_N,
                            # f"tx_done_{vid}"]` it finds this placeholder and
                            # replaces it in place.
                            _tx_marker = f"tx_done_{vid}"
                            # Placeholder MUST include a non-verbose-only
                            # segment so it survives Simple mode's
                            # `_line_is_verbose_only` filter and actually
                            # lands in DOM. Otherwise the subsequent
                            # transcribe-done emit (with same tx_done_
                            # marker) can't find the placeholder to
                            # replace and appends at log bottom — under
                            # whichever channel the sync pass is
                            # currently processing, not under this
                            # video's section. Using `whisper_bracket`
                            # for the em-dash + hourglass so the line
                            # visually matches the eventual ✓ done
                            # emit's em-dash color.
                            stream.emit([
                                [" \u2014 \u23F3 ", ["whisper_bracket", _tx_marker]],
                                ["Transcription queued\u2026\n",
                                 ["simpleline", _tx_marker]],
                            ])
                            transcribe_mgr.enqueue(final_path, t,
                                                    channel=name,
                                                    on_complete=cb,
                                                    video_id=vid,
                                                    from_download=True)
                        # If auto_transcribe is off but compress_enabled is on,
                        # route the compress task through the SHARED GPU queue
                        # (rule: "every compress is a GPU task").
                        # Falls back to inline direct-fire if the transcribe
                        # manager isn't attached (e.g. tests).
                        elif channel.get("compress_enabled"):
                            _comp_lvl = channel.get("compress_level") or "Average"
                            _comp_res = str(channel.get("compress_output_res") or "720")
                            if transcribe_mgr is not None:
                                try:
                                    transcribe_mgr.compress_enqueue(
                                        final_path,
                                        title=os.path.splitext(
                                            os.path.basename(final_path))[0],
                                        channel=name,
                                        quality=_comp_lvl,
                                        output_res=_comp_res)
                                except Exception as _e:
                                    stream.emit_error(f"Couldn't queue video compression: {_e}")
                            else:
                                try:
                                    from .. import compress as _cmp
                                    _cmp.compress_video(final_path, stream,
                                                         quality=_comp_lvl,
                                                         output_res=_comp_res,
                                                         cancel_event=cancel_event)
                                except Exception as _e:
                                    stream.emit_error(f"Video compression failed: {_e}")
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                # Reset per-video path captures so the NEXT video in the
                # same yt-dlp pass doesn't inherit this one's paths.
                # Mirrors YTArchiver.py:18556.
                merge_dest_path = ""
                dest_path = ""
                continue # don't render this control line to the user

            # "already been downloaded" — yt-dlp skipping existing file.
            # No counter bump, no transcribe enqueue, no metadata fetch.
            if "has already been downloaded" in s:
                # Dim / verbose-only. In Simple mode the "\u2014 Downloading X"
                # line we announced on the Destination will stay, but we
                # won't add any more noise. The pass summary "0 downloaded"
                # is the authoritative count.
                stream.emit([[" ", "dim"], [f"{s[:140]}\n", "dim"]])
                continue

            # "already been recorded in the archive" — yt-dlp hitting a
            # video ID that's in the --download-archive file. Expected
            # during the /streams pass (past livestreams) and with
            # --break-on-existing: the ONE matching video that causes
            # yt-dlp to stop the walk logs this line. Pure noise for
            # the user — filter it as dim so Simple mode hides it.
            if "has already been recorded in the archive" in s:
                # count archived-skip lines toward the
                # total-walked tally so the batch-cooldown heuristic
                # (`_should_batch_limit(ch, total)`) has a real number
                # to compare against. Before this, `total` was never
                # populated and large channels never tripped the
                # cooldown.
                _archived_skipped += 1
                stream.emit([[" ", "dim"], [f"{s[:140]}\n", "dim"]])
                continue

            # Destination line — stash the path so DLTRACK can reuse it,
            # AND emit a visible "\u2014 Downloading X" line so the user knows
            # work is happening. We still defer counter / transcribe /
            # metadata until DLTRACK fires (only real merges emit DLTRACK),
            # but Simple mode needs SOME signal between "Starting" and
            # "Done" or it looks stuck during yt-dlp's multi-second
            # channel enumeration + download.
            m = _TITLE_RE.search(s)
            if m:
                dest_path = m.group(1).strip()
                final_path = _resolve_final_mp4(dest_path)
                if final_path is None:
                    # Sidecar destination (.vtt / .description / .info.json
                    # / etc). Dim trace only.
                    stream.emit([[f" {s[:140]}\n", "dim"]])
                    continue
                current_title = os.path.basename(dest_path).rsplit(".", 1)[0]
                display_title = re.sub(
                    r"\s*\[[A-Za-z0-9_-]{11}\]\s*$", "", current_title
                ).strip() or current_title
                if "." in display_title:
                    _last_bit = display_title.rsplit(".", 1)[-1]
                    # Same `f<digit>...` pattern as _resolve_final_mp4 —
                    # handles .f137 (single track) and .f140-16 / .f251-drc
                    # (multi-track / DRC) suffix cases consistently.
                    if (len(_last_bit) >= 2 and _last_bit[0] == "f"
                            and _last_bit[1].isdigit()):
                        display_title = display_title.rsplit(".", 1)[0]
                # Announce once per merged .mp4 — yt-dlp emits a Destination
                # per intermediate track (.f137, .f139) + the merge target.
                if not _title_announced.get(final_path):
                    _title_announced[final_path] = "pending"
                    # Bump the per-video counter + stash it against this
                    # final_path so the DLTRACK "✓ done" emit below can
                    # find the same inplace kind and REPLACE this
                    # Downloading row in place (matches OLD behavior
                    # where the progress bar then the done line both
                    # replace the Downloading line at position X).
                    # Monotonic across the entire sync run — see the
                    # `_DLROW_COUNTER` module-level declaration. `global`
                    # is declared at the top of sync_channel so this
                    # assignment (and the read above in the DLTRACK
                    # branch) both resolve to the module-level name.
                    _DLROW_COUNTER += 1
                    _path_to_counter[final_path] = _DLROW_COUNTER
                    _path_to_display_title[final_path] = display_title
                    # Parallel join key by video_id — sidesteps filename-
                    # character-substitution mismatches (`:` titles becoming
                    # `：` or `?` in the file path) that defeat path-based
                    # lookups in the DLTRACK handler below.
                    if current_vid_id:
                        _vid_to_counter[current_vid_id] = _DLROW_COUNTER
                    _dl_kind = f"dlrow_{_DLROW_COUNTER}"
                    stream.emit([
                        [" ", ["dim", _dl_kind]],
                        ["\u2014 Downloading ", ["simpleline_green", _dl_kind]],
                        [f"{display_title}\n", ["simpleline", _dl_kind]],
                    ])
                continue

            # Progress line — emits the SAME 3-segment shape as the
            # Destination "Downloading <title>" line with a green pct
            # suffix appended to the title segment. Uses the same
            # `dlrow_<N>` inplace marker so each tick replaces the
            # previous line at a single DOM position, and the final
            # ✓ done line (also tagged `dlrow_<N>`) cleanly replaces
            # the last tick.
            # Critical: the emit MUST NO-OP if `_path_to_counter` has
            # no entry matching the current `_DLROW_COUNTER`. This
            # guards against a yt-dlp progress line arriving BEFORE
            # the Destination branch bumped the counter for this
            # video (e.g. after a sidecar-only Destination, or at
            # channel boundaries where `_DLROW_COUNTER` points to
            # the PREVIOUS channel's already-done video). Without
            # this guard, the emit would tag with a stale `dlrow_<N>`
            # and IN-PLACE REPLACE some earlier channel's done line
            # way up in the log. reported two symptoms:
            # - "Downloading 100%" with no title (stale counter,
            #   _path_to_counter miss → empty title fallback)
            # - Lines appearing in wrong places (stale counter
            #   caused inplace to replace the wrong DOM node).
            # Dropping the emit entirely when we can't resolve the
            # title is strictly safer — better to skip a progress
            # tick than corrupt the log.
            m = _PROG_RE.search(s)
            if m:
                pct = m.group(1)
                _prog_kind = f"dlrow_{_DLROW_COUNTER}"
                # If this dlrow was already closed by a DLTRACK done
                # emit, drop the progress tick. yt-dlp occasionally
                # dribbles a late "[download] 100%" line after the
                # done marker; without this gate it replaces the ✓
                # done line with a stuck "Downloading Title 100%".
                if _DLROW_COUNTER in _closed_dlrows:
                    continue
                # Resolve display_title for the current in-flight
                # video by looking up which final_path was assigned
                # this _DLROW_COUNTER value.
                _disp = None
                for _fp, _ctr in _path_to_counter.items():
                    if _ctr == _DLROW_COUNTER:
                        _disp = _path_to_display_title.get(_fp)
                        break
                # If no title is resolvable, drop the tick silently.
                # The previous "Bug [98]" fallback emitted a visible
                # "Downloading #N" placeholder, but yt-dlp emits
                # progress for sidecars / metadata / pre-flight before
                # the main-video Destination line \u2014 so that fallback
                # produced visible orphan lines for every channel
                # that had a fast first-tick (Dr Insanity #0, etc).
                # The original "UI looks stuck" risk Bug [98] tried to
                # address turned out to be theoretical \u2014 in practice
                # Destination fires before any visible progress for
                # the actual video, so the lookup succeeds for real
                # video progress. Sidecars / metadata are fast enough
                # that dropping their ticks is invisible.
                if not _disp:
                    continue
                stream.emit([
                    [" ", ["dim", _prog_kind]],
                    ["\u2014 Downloading ", ["simpleline_green", _prog_kind]],
                    [f"{_disp} ", ["simpleline", _prog_kind]],
                    [f"{pct}%\n", ["dlprogress_pct", _prog_kind]],
                ])
                continue

            # Livestream / scheduled premiere — defer instead of treating as error
            try:
                from .. import livestreams as _ls
                if _ls.line_looks_live(s):
                    id_m = re.search(r"\b([A-Za-z0-9_-]{11})\b", s)
                    vid = id_m.group(1) if id_m else ""
                    # if the line didn't carry a visible
                    # video_id (age-gated streams hit 403 BEFORE the id
                    # appears in yt-dlp output), try extracting from
                    # the URL form `/watch?v=<id>` that yt-dlp usually
                    # prints alongside. Last-resort fallback catches
                    # the case the regex missed.
                    if not vid:
                        url_m = re.search(r"(?:watch\?v=|youtu\.be/|shorts/)"
                                          r"([A-Za-z0-9_-]{11})", s)
                        if url_m:
                            vid = url_m.group(1)
                    # if the regex misses, nothing gets written
                    # to the deferred journal AND the next sync re-
                    # detects + re-logs the same stream forever. Emit a
                    # dim warning so the user at least sees it.
                    if not vid:
                        stream.emit_dim(
                            " (live-detect: couldn't extract video_id from "
                            "yt-dlp output — deferral skipped)")
                    # Clean the title stored in the deferred journal —
                    # current_title is the most recent Destination-line
                    # filename stem which often still carries yt-dlp's
                    # format-selector suffix (e.g. `.f140-4`, `.f137`).
                    # Strip it so the drawer shows a readable title.
                    _clean_title = (current_title or "").strip()
                    if "." in _clean_title:
                        _last = _clean_title.rsplit(".", 1)[-1]
                        if (len(_last) >= 2 and _last[0] == "f"
                                and _last[1].isdigit()):
                            _clean_title = _clean_title.rsplit(".", 1)[0]
                    # Classify which kind of live-state for a tight
                    # single-line log message. "is_upcoming" /
                    # "premieres in" / "scheduled" → upcoming premiere.
                    # Anything else that matched line_looks_live →
                    # currently-live stream.
                    _s_low = s.lower()
                    if ("is_upcoming" in _s_low
                            or "premieres in" in _s_low
                            or "scheduled" in _s_low
                            or "starts in" in _s_low
                            or "will begin" in _s_low):
                        _kind = "upcoming premiere"
                    else:
                        _kind = "currently live"
                    if vid:
                        _ls.defer(vid, title=_clean_title, channel_url=url)
                        stream.emit([
                            [" [Live] ", "livestream"],
                            [f"Deferred \u2014 {_clean_title[:80] or vid} "
                             f"({_kind}).\n", "simpleline"],
                        ])
                    else:
                        stream.emit([
                            [" [Live] ", "livestream"],
                            [f"Deferred \u2014 {_kind} stream on this channel.\n",
                             "simpleline"],
                        ])
                    continue
            except Exception as e:
                _log.debug("swallowed: %s", e)

            # Error lines.
            # require the error/warning tokens to appear
            # with a trailing colon or bracket, so a video whose
            # literal title contains the word "error" (or "warning")
            # doesn't falsely bump the error counter on its DLTRACK
            # line. yt-dlp's own error format is "ERROR: ..." (or
            # "WARNING: ..."), which the stricter regex matches.
            low = s.lower()
            if (re.search(r"\berror[\s:\]\[]", low)
                    or re.search(r"\bwarning[\s:\]\[]", low)):
                # Benign non-errors that yt-dlp prints as ERROR lines.
                # extended the allowlist so the error counter
                # doesn't inflate on these "noise" lines. Counter
                # remains accurate for real failures.
                # private / deleted / terminated videos
                # aren't errors — they're content that YouTube
                # no-longer-serves. Without classifying them as benign,
                # the error counter inflates on channels with any
                # removed-over-time content ("sync had 47 errors!"
                # when actually the channel was fine).
                _BENIGN_FRAGMENTS = (
                    "does not have a streams tab",
                    "does not have a shorts tab",
                    "does not have a releases tab",
                    "does not have a community tab",
                    "does not have a posts tab",
                    "unable to extract uploader",  # yt-dlp warns when uploader is blank
                    "unable to extract n function",  # transient yt-dlp version issue
                    "precondition check failed",  # known-transient signed-url hiccup
                    "private video",  # expected: uploader made video private
                    "video unavailable",  # deleted or region-locked
                    "has been terminated",  # creator account terminated
                    "removed by the uploader",  # creator deleted this video
                    "removed by the user",
                    "this video is not available",
                )
                if any(frag in low for frag in _BENIGN_FRAGMENTS):
                    stream.emit([[f" {s}\n", "dim"]])
                    continue
                # yt-dlp's internal retry messages. Format examples:
                #   [download] Got error: 1048576 bytes read, 9298191
                #     more expected. Retrying (1/10)...
                #   [download] Got server HTTP error 429. Retrying (2/10)...
                #   ERROR: unable to download video data: HTTPSConnection...
                #     Retrying (3/10)...
                # These are TRANSIENT hiccups — yt-dlp is about to try
                # again and almost always succeeds. If all retries fail,
                # yt-dlp emits a final error line WITHOUT "Retrying" that
                # this branch falls through to (which IS a real error).
                # Without this filter the user sees a scary red "Got
                # error" line for a download that completed fine, plus
                # the error counter bumps incorrectly.
                if "retrying (" in low:
                    stream.emit([[f" {s}\n", "dim"]])
                    continue
                # Members-only content — not an error, just a skip.
                # Auto-archive the id
                # so next sync skips it instantly instead of re-trying.
                if "members-only content" in low or "get access to members" in low:
                    _vid_m = re.search(r"\b([A-Za-z0-9_-]{11})\b", s)
                    _vid_id = _vid_m.group(1) if _vid_m else ""
                    _title = current_title or (_vid_id or "video")
                    stream.emit([
                        ["[SKIP]", "filterskip"],
                        [f" {_title} \u2014 Members-only content.\n",
                         "filterskip_dim"],
                    ])
                    if _vid_id:
                        # Serialize through the module-level lock so a
                        # second sync thread appending at the same
                        # instant can't interleave bytes mid-line.
                        # Also write the line in one os.write so the
                        # OS sees an atomic single-line append
                        # (Windows guarantees < 4KB writes are atomic).
                        try:
                            with _archive_write_lock:
                                with open(ARCHIVE_FILE, "ab") as _af:
                                    _af.write(
                                        f"youtube {_vid_id}\n".encode("utf-8"))
                                    try: _af.flush()
                                    except OSError: pass
                        except OSError:
                            pass
                    continue
                stream.emit([[f" {s}\n", "red" if "error" in low else "filterskip"]])
                if "error" in low:
                    errors += 1
                    # capture the video ID that failed so we
                    # can retry it on the next sync. yt-dlp's error lines
                    # generally include the offending [yt id] in
                    # "ERROR: [youtube] <id>: ..." format. Extract any
                    # 11-char ID pattern and stash it; we deduplicate
                    # against the success set below before persisting.
                    # Match the FIRST 11-char alnum token in the error
                    # line — but reject all-letter matches (audit:
                    # sync/core.py:1670). Real YT ids are random picks
                    # from [A-Za-z0-9_-], statistically always include
                    # a digit/_/-; a benign error message can contain
                    # natural-language 11-letter words ("permissions",
                    # "downloading") that would otherwise get stuffed
                    # into the failed list and re-tried as a yt-dlp URL.
                    _err_vid_m = re.search(r"\b([A-Za-z0-9_-]{11})\b", s)
                    if _err_vid_m and not _err_vid_m.group(1).isalpha():
                        _failed_this_run.add(_err_vid_m.group(1))
                # Rate-limit detection: 429 or HTTP 429 in the output → pause
                # for 30s before continuing. yt-dlp retries internally but we
                # want the log to show we're waiting rather than hammering.
                if "429" in low or "too many requests" in low or "rate limit" in low:
                    stream.emit_text(
                        " \u23F8 Rate-limited by YouTube \u2014 backing off 30s...",
                        "red")
                    for _ in range(30):
                        if cancel_event is not None and cancel_event.is_set():
                            break
                        time.sleep(1)
                    stream.emit_text(" \u25B6 Resuming.", "simpleline_green")
                continue

            # Default: dim
            stream.emit([[f" {s}\n", "dim"]])

        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            # terminate is async on Windows (TerminateProcess) — wait
            # briefly for the process to actually exit, then force-kill
            # if it's still alive. Previously a hung yt-dlp.exe stayed
            # running while sync_channel returned, and proc.returncode
            # was None so the "no completed pass" downgrade logic at
            # line 2120 misreported.
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                    proc.wait(timeout=2)
                except Exception as e:
                    _log.debug("swallowed: %s", e)
        # close stdout explicitly — Python's subprocess.Popen
        # with PIPE on a binary pipe can keep the FD reserved past wait
        # otherwise.
        try:
            if proc.stdout is not None:
                proc.stdout.close()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        # record this pass's returncode for the final
        # _ok check. None = terminated mid-flight without wait (treated
        # below as not-a-failure if any other pass succeeded).
        _pass_returncodes.append(proc.returncode)
        # unregister proc from PROCESS_REGISTRY so it
        # doesn't accumulate dead procs across many channels.
        try:
            from ..process_runner import PROCESS_REGISTRY
            PROCESS_REGISTRY.unregister(proc)
        except Exception as _re:
            _log.debug("swallowed: %s", _re)
        # End of per-URL pass (main /videos or /streams). Loop picks up the
        # next URL if there is one.

    elapsed = time.time() - t_start
    took = _fmt_duration(elapsed)

    # Per-channel [Sync] Done line is now rendered by sync_start_all via
    # _sync_row_emit — one compact `[N/total] Name \u2014 summary` row that
    # replaces the live header in place. No duplicate line here.

    # Push per-channel totals so a companion display can show live progress.
    # pass_idx/pass_total come from sync_all's queue-driven loop so an
    # external display reads e.g. "7/47" — single-channel syncs default
    # to 1/1. Fixes bug where bulk syncs always showed 1/1.
    # Issue #140 follow-on: the per-DLTRACK write inside the yt-dlp loop
    # already incremented `dl` by 1 for every real download. Passing 0
    # here keeps the channel-name/idx refresh but stops doubling the
    # count. Errors still get reported once per channel summary.
    try:
        write_sync_progress(channel_name=name,
                            idx=int(pass_idx or 1),
                            total=max(int(pass_total or 1), int(pass_idx or 1)),
                            downloaded=0,
                            skipped=0, errors=errors)
    except Exception as e:
        _log.debug("swallowed: %s", e)

    # refresh the disk cache for this channel now that the
    # download pass completed. Without this hook the Subs table's
    # # Vids / Size columns stay blank for newly-added channels until
    # the next periodic disk scan fires (up to 24h depending on the
    # user's setting). Cheap when called per-channel; only walks one
    # folder.
    # Short-circuit the post-download helpers on cancel. Both walk Z:
    # for every channel video and ignore cancel_event otherwise, so a
    # cancel during a download-heavy channel used to feel sluggish
    # (audit: sync/core.py:1737).
    _post_sync_cancelled = (cancel_event is not None and cancel_event.is_set())
    if downloaded > 0 and not _post_sync_cancelled:
        try:
            from .. import archive_scan as _as
            _as.update_disk_cache_for_channel(channel)
        except Exception as _as_e:
            stream.emit_dim(
                f" (disk cache refresh skipped: {_as_e})")

    # Issues #147/#158: post-sync thumbnail sweep. Pulls thumbnails for
    # any video in this channel that's missing one but has a cached
    # thumbnail_url in metadata.jsonl. Catches the "half my channel's
    # thumbnails never landed" symptom — rate-limit / transient HTTP
    # failures during the bulk download path. Runs after metadata so
    # the JSONL is current.
    if downloaded > 0 and not _post_sync_cancelled:
        try:
            from .. import metadata as _md
            _sweep = _md.sweep_missing_thumbnails(channel, stream=stream)
            if _sweep.get("fetched"):
                stream.emit_dim(
                    f" (thumbnail sweep: {_sweep['fetched']} fetched, "
                    f"{_sweep['missing']} still missing)")
        except Exception as _ts_e:
            stream.emit_dim(
                f" (thumbnail sweep skipped: {_ts_e})")

    # persist the updated failed_video_ids list.
    # - Successful retries (vid appears in downloaded_ids) get removed.
    # - New failures get added with retry_count = 1.
    # - Existing retries that failed AGAIN get count++ (cap at 3 to
    #   give up on permanently-broken IDs and stop log spam).
    try:
        _next_failed: dict[str, int] = {}
        for _v, _c in (_prior_failed or {}).items():
            if not isinstance(_v, str) or not isinstance(_c, int):
                continue
            if _v in downloaded_ids:
                continue  # retry succeeded — drop
            if _v in _failed_this_run:
                _next_failed[_v] = min(3, _c + 1)
            elif _c < 3:
                _next_failed[_v] = _c  # retry pending; not seen this run
        for _v in _failed_this_run:
            if _v in downloaded_ids:
                continue  # error line but DLTRACK fired later — succeeded
            if _v in _next_failed:
                continue  # already accounted for above
            _next_failed[_v] = 1
        # Drop IDs that hit the retry cap so they stop being attempted.
        _next_failed = {v: c for v, c in _next_failed.items() if c < 3}
        # Only write if something actually changed (avoid config churn
        # on every successful sync).
        if _next_failed != (_prior_failed or {}):
            channel["failed_video_ids"] = _next_failed
            try:
                # Patch A: serialize the load-modify-save against other
                # sync threads that may be writing the same config file.
                # Without this lock, two concurrent channel-sync threads
                # can both load config, both update their channel's
                # failed_video_ids, and the second save silently
                # overwrites the first.
                with _config_write_lock:
                    _cfg2 = load_config()
                    # Match on URL only. Old `url == OR name == ` would
                    # mis-route the failed-id list when two channels
                    # share a display name (e.g. both renamed "News")
                    # — the second channel's failed_ids could overwrite
                    # the first's (audit: sync/core.py:1798). URL is
                    # the channel's only true unique key.
                    _our_url = (channel.get("url") or "").strip()
                    if not _our_url:
                        # Last-resort fallback when this channel has no
                        # URL — match by name. Rare path.
                        _our_name = channel.get("name") or ""
                        for _ch in _cfg2.get("channels", []):
                            if (_ch.get("name") or "") == _our_name:
                                _ch["failed_video_ids"] = _next_failed
                                break
                    else:
                        for _ch in _cfg2.get("channels", []):
                            if (_ch.get("url") or "").strip() == _our_url:
                                _ch["failed_video_ids"] = _next_failed
                                break
                    save_config(_cfg2)
            except Exception as _fce:
                stream.emit_dim(
                    f" (failed-id list save skipped: {_fce})")
    except Exception as e:
        _log.debug("swallowed: %s", e)

    # Clear current-sync marker
    if queues is not None:
        queues.set_current_sync(None)

    # Drain the inline-metadata executor. Every video dispatched during
    # this pass is now waiting (or running) on the one worker thread.
    # If a cancel was raised, drop pending tasks immediately rather
    # than waiting for them — old wait=True with no cancel check made
    # the whole sync UI freeze whenever a metadata fetch was stuck on
    # a slow YouTube call (audit: sync/core.py:1820).
    if _meta_exec is not None:
        try:
            if cancel_event is not None and cancel_event.is_set():
                # Cancel-fast: drop queued, don't wait for in-flight
                # (they'll be torn down by the broader cancel pass).
                _meta_exec.shutdown(wait=False, cancel_futures=True)
            else:
                _meta_exec.shutdown(wait=True, cancel_futures=True)
        except Exception as e:
            _log.debug("swallowed: %s", e)

    # Consolidated activity-log row — request: replace the
    # historical 3 rows with ONE [Dwnld] row per channel per sync pass
    # showing "N downloaded \u00b7 N transcribed \u00b7 N metadata".
    # Emit synchronously here, reading transcribed count from the
    # transcribe manager's current batch_stats. Auto-captions finish
    # during the download (parallel with yt-dlp) so by this point the
    # count is accurate. Whisper may still be running — emit what we
    # have; standalone [Trnscr] won't follow because we also consume
    # the transcribe manager's batch entry for this channel below.
    # Only emit when something actually happened (downloaded > 0 OR
    # errors) so idle sync passes don't spam the activity log.
    if downloaded > 0 or errors > 0:
        _meta_fetched = _read_meta_count("fetched")
        _tx_done = 0
        _tx_err = 0
        if auto_tx and transcribe_mgr is not None:
            try:
                _stats = transcribe_mgr.get_channel_batch_stats(name)
                _tx_done = int(_stats.get("done", 0) or 0)
                _tx_err = int(_stats.get("err", 0) or 0)
                # DON'T consume here — if Whisper is still running for
                # videos from this channel, _flush_batch_stats will
                # later update this same row with the final transcribed
                # count. If we consume now, we lose the updated stats
                # and the row sticks at "0 transcribed" forever.
                # Consume only when auto_tx is off OR the transcribe
                # manager is idle by the time this row emits.
                try:
                    _is_idle = not bool(transcribe_mgr.is_active())
                except Exception:
                    _is_idle = False
                if _is_idle:
                    transcribe_mgr.consume_channel_batch_stats(name)
            except Exception as e:
                _log.debug("swallowed: %s", e)
        _row_id = emit_consolidated_auto_row(
            stream, name,
            downloaded=int(downloaded or 0),
            transcribed=_tx_done,
            metadata=_meta_fetched,
            errors=int(errors or 0) + _tx_err,
            elapsed=float(elapsed),
            kind="Dwnld")
        # Stash this row so a late transcribe-complete can patch it
        # in place (retroactive update of the transcribed cell).
        if auto_tx and transcribe_mgr is not None:
            try:
                register_pending_dwnld_row(
                    name, _row_id,
                    downloaded=int(downloaded or 0),
                    metadata=_meta_fetched,
                    errors=int(errors or 0),
                    elapsed_start=t_start)
            except Exception as _re4:
                # if the activity-log row register fails, the
                # sync completed but the user sees no history row.
                # Surfacing via dim log at least confirms the sync ran.
                stream.emit_dim(
                    f" (activity-log register failed: {_re4})")

    # Remember the fresh IDs in the channel cache for the next sync pass
    # (lets the quick-check-new-uploads fast path skip enumeration).
    if downloaded_ids and (cancel_event is None or not cancel_event.is_set()):
        try:
            from .. import channel_cache as _cc
            _cc.append_ids(url, downloaded_ids)
        except Exception as e:
            _log.debug("swallowed: %s", e)

    # Update config last_sync + initialized/sync_complete flags. These
    # are load-bearing for the `--break-on-existing` gate above: OLD
    # stamps `initialized=True` and `sync_complete=True` after a
    # successful sync (YTArchiver.py:22963, :19227, :22932) so every
    # subsequent sync on that channel uses `--break-on-existing` and
    # bails fast. Without this write, new channels never graduate to
    # the fast-sync path.
    if config_is_writable():
        # normalize the URL once here so the channel-match
        # loop below can't silently fail on a trailing-slash /
        # www-prefix / casing difference. Without normalization, a
        # mismatch meant `_dirty` stayed False, no channel was
        # updated, initialized never flipped → sync stuck in slow
        # path forever.
        try:
            from .. import subs as _subs_norm
            _url_norm = _subs_norm.normalize_channel_url(url) or url
        except Exception:
            _url_norm = url
        _url_norm = (_url_norm or "").strip().rstrip("/")
        _config_write_err: str | None = None
        try:
            # `now` was referenced below without ever being
            # defined in this scope. The outer try/except silently swallowed
            # the NameError so `last_sync`, `initialized`, and `sync_complete`
            # never got written when a sync actually downloaded anything —
            # invisibly defeating the --break-on-existing fast-path on every
            # subsequent sync.
            now = datetime.now()
            cfg2 = load_config()
            _dirty = False
            # require AT LEAST ONE video to have been walked
            # (downloaded > 0 OR errors > 0, meaning yt-dlp actually
            # ran end-to-end) before stamping `initialized=True`. A
            # first sync that 0-downloads AND 0-errors almost certainly
            # hit a filter wall (strict duration filter, empty
            # playlist, auth failure) — stamping initialized there locks
            # the channel into fast-path forever even though it was
            # never actually bootstrapped.
            # only stamp initialized when downloaded > 0.
            # Treating errors as "walked meaningfully" was wrong —
            # a first sync that cookies-expired or got blanket-
            # --match-filter'd has errors > 0 but zero real walk, and
            # marking it initialized locks the channel into fast-path
            # forever (--break-on-existing stops the walk at the first
            # archive hit). Real first-sync success needs an actual
            # download.
            _walked_meaningfully = (downloaded > 0)
            _meta_did_fetch = _read_meta_count("fetched") > 0
            _now_ts = time.time()
            # update last_sync on every SUCCESSFUL pass, not
            # just ones with new downloads. A channel with 0 new videos
            # is still being checked — last_sync going stale-for-months
            # made users think their channel wasn't syncing when it
            # was. No new downloads doesn't mean no activity.
            _matched_any = False
            for c in cfg2.get("channels", []):
                # compare normalized URLs so a saved
                # `youtube.com/@ch/` and a live `www.youtube.com/@ch`
                # still match the same row.
                _c_url = (c.get("url", "") or "").strip().rstrip("/")
                try:
                    _c_norm = _subs_norm.normalize_channel_url(_c_url) or _c_url
                except Exception:
                    _c_norm = _c_url
                _c_norm = (_c_norm or "").strip().rstrip("/")
                if _c_norm != _url_norm and _c_url != url:
                    continue
                _matched_any = True
                # Capture the pre-update `initialized` state so we can
                # tell whether this pass is a bootstrap (first-ever
                # sync of a brand-new channel, EVERY video freshly
                # fetched → timestamps truly mean "all metadata is
                # current") versus an incremental sync (thousands of
                # stale entries + a handful of fresh ones → stamping
                # "refreshed just now" would be a lie).
                _was_bootstrap_pass = (not c.get("initialized", False)
                                       and _walked_meaningfully)
                c["last_sync"] = now.strftime("%Y-%m-%d %H:%M")
                _dirty = True
                # if the user paused or cancelled mid-pass
                # of a FIRST-ever sync, do NOT stamp initialized/
                # sync_complete. The previous behavior stamped both flags
                # the moment downloaded > 0, so a "pause + clear queue"
                # at 3/4 through a 200-video bootstrap permanently
                # locked the channel into --break-on-existing fast-path,
                # leaving the unfetched 1/4 unreachable on every
                # subsequent sync ("no new videos"). Only graduate to
                # the fast-path when a sync completes WITHOUT being
                # interrupted.
                _was_interrupted = bool(
                    (cancel_event is not None and cancel_event.is_set())
                    or (pause_event is not None and pause_event.is_set())
                )
                if _walked_meaningfully and not _was_interrupted:
                    if not c.get("initialized", False):
                        c["initialized"] = True
                        _dirty = True
                    if not c.get("sync_complete", False):
                        c["sync_complete"] = True
                        _dirty = True
                elif _walked_meaningfully and _was_interrupted:
                    # Surface the interruption so the user knows why
                    # this channel will do a full walk again next time.
                    try:
                        stream.emit_dim(
                            " (sync interrupted before completion — "
                            "channel will do another full walk on the "
                            "next sync to catch missed videos)")
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                # Only stamp the channel-level refresh timestamps on a
                # bootstrap pass. For incremental syncs (4 new videos
                # out of 1000), 996 entries still hold older fetched_at
                # stamps — stamping "just now" would misrepresent the
                # freshness of the bulk of the channel's data. Design
                # intent: the column should say "today" for a brand-
                # new channel add, NOT for a small trickle-in on an
                # existing channel.
                if _meta_did_fetch and _was_bootstrap_pass:
                    c["last_views_refresh_ts"] = _now_ts
                    c["last_comments_refresh_ts"] = _now_ts
                    _dirty = True
                break
            if downloaded > 0:
                cfg2["last_sync"] = now.strftime("%Y-%m-%d %H:%M")
                _dirty = True
            if _dirty:
                save_config(cfg2)
            if not _matched_any:
                # surface the mismatch as a dim warning.
                # Before, a URL-normalization mismatch silently no-
                # op'd the whole config write; no log line existed
                # to explain why the channel never graduated to
                # fast-path.
                _config_write_err = (
                    f"config update: no channel matched URL "
                    f"{url!r} (normalized {_url_norm!r}) — "
                    f"last_sync/initialized/sync_complete not written.")
        except (OSError, PermissionError, ValueError, KeyError) as _ce:
            # narrow the catch. Bare `except Exception: pass`
            # was hiding config-write failures (disk full, file lock,
            # JSON parse error from a corrupted config). Now we log a
            # dim warning so the user can investigate instead of
            # puzzling over why syncs never gain the fast-path flags.
            _config_write_err = f"config update failed: {_ce}"
        if _config_write_err:
            try:
                stream.emit_dim(f" ({_config_write_err})")
            except Exception as e:
                _log.debug("swallowed: %s", e)

    # Safety net: after the sync pass completes, sweep the channel folder
    # for orphan .vtt caption files and delete them. yt-dlp can drop
    # `.vtt` sidecars for videos it skipped via --download-archive (rare
    # but happens when subs get added to an old video). Previously ran
    # on every channel, which meant an os.walk of every archive folder
    # on every sync pass — on large archives this was the dominant per-
    # channel cost. Now only sweep when something actually downloaded
    # (new .vtt files can only arrive with new video files in this
    # config). If orphan .vtt accumulation ever happens outside a
    # download event, it gets caught on the next pass that does download.
    if downloaded > 0:
        try:
            _swept = _sweep_orphan_vtts(str(ch_dir), cancel_event=cancel_event)
            if _swept > 0:
                stream.emit([[" ", "dim"],
                             [f"Swept {_swept} orphan caption file(s).\n", "dim"]])
        except Exception as _sve:
            stream.emit_dim(f" (vtt sweep skipped: {_sve})")

    # Also refresh the channel's avatar/banner art. Internal 30-day
    # threshold on channel_art.fetch_channel_art means this is near-free
    # when art is already current. Matches OLD auto-fetch behavior.
    try:
        from .. import channel_art as _ca
        _ca.fetch_channel_art(url or "", str(ch_dir), force=False)
    except Exception as _ae:
        stream.emit_dim(f" (channel-art refresh skipped: {_ae})")

    # Clear the sync-active flag — allow transcribe._flush_batch_stats to
    # emit standalone [Trnscr] rows for any transcriptions that slip in
    # after this point (shouldn't happen for sync-originated jobs since
    # we already consumed + emitted, but harmless if it does).
    clear_sync_active(name)

    # Post-channel orphan cleanup: any dlrow_<N> we created via a
    # Destination line but never closed via a DLTRACK done emit is an
    # orphan (path-match failure, sidecar-only Destination, etc.). The
    # in-place line for it is sitting visible as "Downloading Title
    # 100%" with no done-line replacement. Sweep them now and emit
    # clear_line markers so they vanish from the log before the next
    # channel's rows render.
    try:
        import json as _json_mod_clean
        for _ctr in set(_path_to_counter.values()) - _closed_dlrows:
            _stuck_kind = f"dlrow_{_ctr}"
            try:
                stream.emit([
                    [_json_mod_clean.dumps({
                        "kind": "clear_line",
                        "marker": _stuck_kind,
                    }), "__control__"],
                ])
            except Exception as e:
                _log.debug("swallowed: %s", e)
    except Exception as e:
        _log.debug("swallowed: %s", e)

    # derive _ok from ANY pass's returncode instead of
    # reading the last proc.returncode (which could be None if the loop
    # broke via terminate, and could belong to a /streams pass that
    # failed even if the main pass succeeded). 0 = clean exit;
    # 1 = "some entries failed" (already counted in errors). Anything
    # else = crash. If all rcs are None (cancelled/paused before any
    # pass finished) treat as ok-but-partial when downloaded > 0.
    _good_rcs = [rc for rc in _pass_returncodes if rc in (0, 1)]
    _crashed = [rc for rc in _pass_returncodes if rc is not None and rc not in (0, 1)]
    if _good_rcs:
        _ok = True
    elif _crashed:
        _ok = False
    else:
        # All None: every pass was terminated mid-flight. Treat as
        # successful if any work was actually done (partial cancel),
        # otherwise as a no-op failure.
        _ok = (downloaded > 0)
    # Report the most informative exit code: prefer a real one, fall
    # back to last proc.returncode if it exists, else 0.
    _exit_for_caller = next(iter(_good_rcs), None)
    if _exit_for_caller is None:
        _exit_for_caller = next(iter(_crashed), proc.returncode if proc else 0)
    return SyncResult(ok=_ok, downloaded=downloaded, errors=errors,
                      took=took, exit=_exit_for_caller,
                      total=downloaded + _archived_skipped)












from .active_state import (  # noqa: F401
    set_sync_active,
    clear_sync_active,
    is_sync_active,
    is_any_sync_active,
    set_metadata_changed_hook,
    fire_metadata_changed_hook,
)








# Patch 14 (v71.6): sync_all moved to sync_all.py. Re-import here so
# `from backend.sync.core import sync_all` continues to resolve and
# the package __init__ star-import surface stays unchanged.
from .sync_all import sync_all  # noqa: F401
