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

import json
import os
import re
import shutil
import subprocess
import threading
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .ytarchiver_config import load_config, save_config, ARCHIVE_FILE, config_is_writable
from .log_stream import LogStreamer
from . import utils as _utils


# YouTube ID regex — 11 chars of [A-Za-z0-9_-] surrounded by brackets
_ID_IN_FILENAME = re.compile(r"\[([A-Za-z0-9_-]{11})\]")


RESOLUTION_OPTIONS = ["audio", "144", "240", "360", "480", "720", "1080", "1440", "2160", "best"]


# ── Cookie source discovery ────────────────────────────────────────────
# YTArchiver hardcoded --cookies-from-browser firefox. That breaks on
# machines without Firefox. Try each browser in order, fall back to a
# user-provided cookies.txt file in %APPDATA%\YTArchiver\cookies.txt.

_COOKIE_BROWSERS = ("firefox", "chrome", "brave", "edge", "vivaldi", "opera")
_cookie_source_cached: Optional[List[str]] = None


def _find_cookie_source() -> List[str]:
    """Return the yt-dlp cookie args to use (the '--cookies-from-browser X'
    pair or '--cookies /path/to/cookies.txt' pair, or an empty list)."""
    global _cookie_source_cached
    if _cookie_source_cached is not None:
        return _cookie_source_cached

    # Manual override — user can drop cookies.txt in APPDATA\YTArchiver\
    try:
        from .ytarchiver_config import APP_DATA_DIR
        manual = APP_DATA_DIR / "cookies.txt"
        if manual.exists():
            _cookie_source_cached = ["--cookies", str(manual)]
            return _cookie_source_cached
    except Exception:
        pass

    # Probe each browser's profile directory to see if it exists
    appdata = os.environ.get("APPDATA") or ""
    localdata = os.environ.get("LOCALAPPDATA") or ""
    known_paths = {
        "firefox": os.path.join(appdata, "Mozilla", "Firefox", "Profiles"),
        "chrome": os.path.join(localdata, "Google", "Chrome", "User Data"),
        "brave": os.path.join(localdata, "BraveSoftware", "Brave-Browser", "User Data"),
        "edge": os.path.join(localdata, "Microsoft", "Edge", "User Data"),
        "vivaldi": os.path.join(localdata, "Vivaldi", "User Data"),
        "opera": os.path.join(appdata, "Opera Software", "Opera Stable"),
    }
    for browser in _COOKIE_BROWSERS:
        p = known_paths.get(browser)
        if p and os.path.isdir(p):
            _cookie_source_cached = ["--cookies-from-browser", browser]
            return _cookie_source_cached

    # No cookies available — yt-dlp will just hit public content without auth
    _cookie_source_cached = []
    return _cookie_source_cached


def reset_cookie_cache():
    """Clear the cached probe result — call after user changes browser choice."""
    global _cookie_source_cached
    _cookie_source_cached = None


# ── yt-dlp discovery ───────────────────────────────────────────────────

def find_yt_dlp() -> Optional[str]:
    """Locate yt-dlp.exe. Checks PATH first, then common bundled locations."""
    p = shutil.which("yt-dlp") or shutil.which("yt-dlp.exe")
    if p:
        return p
    # Common install paths — PATH first (via shutil.which above), then a
    # handful of places the bundled app might keep a copy next to itself.
    candidates = [
        Path.cwd() / "yt-dlp.exe",
        Path(__file__).resolve().parent.parent / "yt-dlp.exe",
        Path.home() / "Desktop" / "yt-dlp.exe",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return None


# ── Format string (verbatim port from YTArchiver.py:2730) ──────────────

def build_format_string(resolution: str) -> str:
    """Build yt-dlp --format string. Prefers H.264+AAC for native MP4 merging."""
    resolution = str(resolution).lower().strip()
    if resolution == "audio":
        return "bestaudio[ext=m4a]/bestaudio[acodec^=mp4a]/bestaudio/best"

    h = f"[height<={resolution}]" if resolution != "best" else ""
    base = (
        f"(bestvideo{h}[vcodec^=avc]+bestaudio[acodec^=mp4a])"
        f"/(bestvideo{h}[vcodec^=avc]+bestaudio)"
        f"/(bestvideo{h}[vcodec!^=av01]+bestaudio[acodec^=mp4a])"
        f"/(bestvideo{h}[vcodec!^=av01]+bestaudio)"
        f"/(bestvideo{h}+bestaudio)"
        f"/best{h}"
    )
    if resolution == "best":
        return base

    # Adjacent-resolution fallbacks.
    # audit E-6: wrap the int() in a try/except so a corrupted config
    # value (e.g. "720p" instead of "720") doesn't kill the sync pass
    # with a cryptic traceback. Fall back to "best" so the sync still
    # completes (with a wider format range than intended, but no data
    # loss).
    try:
        res_int = int(resolution)
    except (ValueError, TypeError):
        return base + "/best"
    res_above = None
    res_below = None
    _num_opts = [r for r in RESOLUTION_OPTIONS if r.isdigit()]
    for i, r in enumerate(_num_opts):
        if int(r) == res_int:
            if i + 1 < len(_num_opts):
                res_above = _num_opts[i + 1]
            if i > 0:
                res_below = _num_opts[i - 1]
            break
    fallbacks = ""
    if res_above:
        ha = f"[height<={res_above}]"
        fallbacks += f"/(bestvideo{ha}+bestaudio)/best{ha}"
    if res_below:
        hb = f"[height<={res_below}]"
        fallbacks += f"/(bestvideo{hb}+bestaudio)/best{hb}"
    fallbacks += "/(bestvideo+bestaudio)/best"
    return base + fallbacks


# ── Folder sanitization (verbatim port from YTArchiver.py:2790) ────────

_RESERVED_NAMES = frozenset({
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
})

def sanitize_folder(name: str) -> str:
    result = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', name).strip().rstrip('. ')
    if not result:
        result = "_unnamed"
    if result.upper().split('.')[0] in _RESERVED_NAMES:
        result = "_" + result
    return result


def channel_folder_name(ch: Dict[str, Any]) -> str:
    return sanitize_folder((ch.get("folder_override") or "").strip() or ch.get("name", ""))


# ── Progress parsing ───────────────────────────────────────────────────

_PROG_RE = re.compile(r"\[download\]\s+(\d+(?:\.\d+)?)%")
_TITLE_RE = re.compile(r"\[download\]\s+Destination:\s+(.+)$")
# Authoritative final path comes from the Merger / ffmpeg / FixupM3u8 log
# line. Mirrors OLD YTArchiver.py:18625 — we want every flavor of yt-dlp's
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


# ── Startup info for Windows (hide console window) ─────────────────────

_startupinfo = None
if os.name == "nt":
    _startupinfo = subprocess.STARTUPINFO()
    _startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    _startupinfo.wShowWindow = 0


# ── Sync one channel ───────────────────────────────────────────────────

class SyncResult(dict):
    """Result dict with ok/reason/counts."""


def _bracket_segments(label: str, bracket_tag: str = "sync_bracket",
                      label_tag: str = "simpleline",
                      trailing_space: bool = True,
                      extra_tag: Optional[str] = None) -> list:
    """Build log segments for a bracketed header, e.g. [Sync] or [1/103].

    Returns a list shaped for `stream.emit(segments)`. ONLY the `[`, `]`
    and inner `/` (for "N/M") get the bracket color; the word / numbers
    stay in the neutral label tag. Matches the OLD visual: green
    punctuation, white numbers / words.

    `extra_tag` (optional): an additional tag appended to every segment's
    tag list. Used for in-place replacement markers (e.g. "sync_row_12"
    so channel 12's live + done emits replace each other in place).
    """
    def _merge(primary: str) -> Any:
        return [primary, extra_tag] if extra_tag else primary

    segs = [["[", _merge(bracket_tag)]]
    parts = label.split("/")
    for i, part in enumerate(parts):
        if i > 0:
            segs.append(["/", _merge(bracket_tag)])
        segs.append([part, _merge(label_tag)])
    segs.append(["]" + (" " if trailing_space else ""), _merge(bracket_tag)])
    return segs


# Module-level pass-id counter + lock. Each invocation of `sync_all()`
# (or `sync_one_channel`) calls `_new_pass_id()` once to get a unique
# token and stashes it on `_ROW_EMIT_PASS_ID.id`; `_sync_row_emit`
# reads that thread-local by default and appends the token to its
# in-place-replace marker so passes never collide.
#
# Without this, the autorun-triggered second pass's `sync_row_1` emit
# would find the first pass's `sync_row_1` DOM element (still in
# scrollback) and silently replace its content — leaving the user
# staring at a log that seemed to skip most of the channel iteration.
_PASS_ID_COUNTER = 0
_PASS_ID_LOCK = threading.Lock()
_ROW_EMIT_PASS_ID = threading.local()


def _new_pass_id() -> str:
    global _PASS_ID_COUNTER
    with _PASS_ID_LOCK:
        _PASS_ID_COUNTER += 1
        return f"p{_PASS_ID_COUNTER}"


def _sync_row_emit(stream: "LogStreamer", idx: int, total: int,
                   name: str, summary: Optional[str] = None,
                   name_tag: str = "simpleline_green",
                   summary_tag: str = "simpleline",
                   pass_id: str = "") -> None:
    """Emit a single `[N/total] Name — summary` line that replaces in-
    place across re-emissions for the same channel index WITHIN ONE
    sync pass.

    `pass_id` disambiguates markers across passes: without it, a second
    sync pass's channel-1 row would replace the first pass's channel-1
    row WAY up in the log (at its DOM position from 8 minutes ago) and
    the user sees nothing new at the current scroll position. Callers
    should pass a unique id (`_new_pass_id()`) when starting a pass.

    summary=None → "live" row (just `[N/total] Name`).
    summary=str → "done" row (appends ` — summary`). Pad with spaces
                   so the em-dash column aligns roughly at col 34.
    """
    # Fall back to the thread-local stashed by sync_all / sync_one_channel
    # if the caller didn't pass one explicitly.
    if not pass_id:
        pass_id = getattr(_ROW_EMIT_PASS_ID, "id", "") or ""
    marker = (f"sync_row_{pass_id}_{idx}" if pass_id
              else f"sync_row_{idx}")
    segs = _bracket_segments(f"{idx}/{total}", extra_tag=marker)
    if summary is None:
        segs.append([f"{name}\n", [name_tag, marker]])
    else:
        # Pad the channel name to align the em-dash at a consistent column
        name_col = 34
        padded = name if len(name) >= name_col else name + " " * (name_col - len(name))
        segs.append([padded, [name_tag, marker]])
        segs.append([f" \u2014 {summary}\n", [summary_tag, marker]])
    stream.emit(segs)


def _short_summary(downloaded: int, errors: int) -> str:
    """Compact one-phrase summary for a channel row done-emit.
    Matches OLD's per-channel summary style ('no new videos' / '3 new')."""
    if downloaded <= 0 and errors <= 0:
        return "no new videos"
    if errors <= 0:
        return f"{downloaded} new video{'s' if downloaded != 1 else ''}"
    if downloaded <= 0:
        return f"{errors} error{'s' if errors != 1 else ''}"
    return (f"{downloaded} new \u00b7 "
            f"{errors} error{'s' if errors != 1 else ''}")


def sync_channel(channel: Dict[str, Any], stream: LogStreamer,
                 cancel_event: Optional[threading.Event] = None,
                 queues=None, transcribe_mgr=None,
                 pause_event: Optional[threading.Event] = None,
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
    the TuneShine sync-progress file so the external display shows
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
    if 0 < min_dur < 60:
        try:
            stream.emit_dim(f" (legacy min_duration {min_dur}s upgraded to 60s)")
        except Exception:
            pass
        min_dur = 60
    if 0 < max_dur < 60:
        try:
            stream.emit_dim(f" (legacy max_duration {max_dur}s upgraded to 60s)")
        except Exception:
            pass
        max_dur = 60
    mode = (channel.get("mode") or "new").lower() # "new" | "full" | "fromdate"
    from_date = (channel.get("from_date") or "").strip()
    # Folder-org flags: matches YTArchiver.py:17257 split_years / split_months
    split_years = bool(channel.get("split_years"))
    split_months = bool(channel.get("split_months"))

    if not url:
        return SyncResult(ok=False, reason="No URL", downloaded=0, errors=0)

    if queues is not None:
        queues.set_current_sync(channel)

    # TuneShine live progress — write at the START of the channel so the
    # display updates as each new channel kicks off, not just when the
    # previous one completes. End-of-channel write below overwrites with
    # the final download count. Mirrors classic's pattern of calling
    # _write_sync_progress at multiple points per channel.
    try:
        write_sync_progress(channel_name=name,
                            idx=int(pass_idx or 1),
                            total=max(int(pass_total or 1), int(pass_idx or 1)),
                            downloaded=0, skipped=0, errors=0)
    except Exception:
        pass

    yt = find_yt_dlp()
    if not yt:
        stream.emit([["ERROR: ", "red"], ["yt-dlp not found. Install it or put yt-dlp.exe next to the app.\n", "red"]])
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
    from .utils import check_directory_writable, check_disk_space
    if not check_directory_writable(str(ch_dir)):
        stream.emit([["ERROR: ", "red"],
                     [f"Cannot write to {ch_dir} \u2014 disk may be full, read-only, or disconnected.\n", "red"]])
        return SyncResult(ok=False, reason="write blocked", downloaded=0, errors=0)
    # audit F-20: 2-tier disk space check. Hard-fail under 100MB
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
        # audit F-21: --continue lets yt-dlp resume a partial .part
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
    #
    # The earlier "full means full walk forever" logic was wrong — in
    # OLD, full mode means "first sync grabs everything, subsequent
    # syncs only grab new uploads". The download-archive file is what
    # prevents re-downloading across runs.
    cmd += ["--download-archive", str(ARCHIVE_FILE)]
    # audit M-46: sync_complete default flipped False → True was
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
    from . import subs as _subs
    _urls_to_run: List[str] = [url]
    _streams_url = _subs.streams_url(url)
    if _streams_url and _streams_url != url:
        _urls_to_run.append(_streams_url)

    t_start = time.time()
    # Per-channel [Sync] Starting / Done emits are gone — sync_start_all's
    # loop now renders a single-line `[N/total] Name \u2014 summary` row
    # that updates in-place. The URL still logs as a verbose-only trace.
    stream.emit([[f" URL: {url}\n", "dim"]])

    # Pause if we know the network is down (don't waste yt-dlp on dead pipes)
    try:
        from . import net as _net
        _net.block_if_down(stream=stream,
                            check_cancel=lambda: cancel_event and cancel_event.is_set())
    except Exception:
        pass

    # Counters that must persist across the /videos + /streams passes.
    downloaded = 0
    errors = 0
    # audit E-4: count archived-skip hits (--break-on-existing line)
    # so SyncResult can report a real `total` = downloaded + skipped.
    # Used by the batch-cooldown heuristic in sync_all().
    _archived_skipped = 0
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
    #
    # NB: uses the MODULE-level `_DLROW_COUNTER` (not a fresh local) so
    # counters don't collide across channel boundaries. If channel A
    # emitted `dlrow_1…dlrow_3`, channel B starts at `dlrow_4`.
    # `_path_to_counter` stays local because it's only queried within
    # this channel's DLTRACK flow.
    _path_to_counter: Dict[str, int] = {}
    # Display title per-path, stashed by the Destination branch so the
    # Progress branch can rebuild the full "— Downloading <title> NN%"
    # line. Without this the progress tick has no access to the title
    # (current_title has the [id] suffix still attached, and display_title
    # is a local inside the Destination `if` block). A small dict, one
    # entry per merged .mp4, cleared at channel end with the other
    # per-channel state.
    _path_to_display_title: Dict[str, str] = {}
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
    downloaded_ids: List[str] = [] # fast-path metadata target list
    # title -> video_id, filled from DLTRACK::: emitted after each video.
    # Needed because filenames no longer carry the [id] suffix (drop-in mode).
    _title_to_id: Dict[str, str] = {}
    # Dedupe per-video "Downloading" announcements — yt-dlp emits a
    # "Destination:" line for every intermediate track (.f137, .f139) plus
    # the merge target, so we'd otherwise log 3 lines per video and
    # triple-count the `downloaded` tally. Keyed by merged .mp4 path.
    _title_announced: Dict[str, bool] = {}

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
    if channel.get("auto_metadata"):
        try:
            from concurrent.futures import ThreadPoolExecutor
            _meta_exec = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="yta-meta")
        except Exception:
            _meta_exec = None

    def _submit_inline_metadata(vid_id: str, title: str, final_path: str):
        """Dispatch a per-video metadata fetch onto the meta executor.
        No-op when auto_metadata is off. Never raises."""
        if _meta_exec is None or not vid_id or not final_path:
            return

        def _task():
            try:
                if cancel_event is not None and cancel_event.is_set():
                    return
                from . import metadata as _meta
                res = _meta.fetch_single_video_metadata(
                    channel, vid_id, final_path, title, stream)
                if res.get("ok") and res.get("fetched"):
                    _meta_counts["fetched"] += 1
                elif res.get("ok") and res.get("skipped"):
                    _meta_counts["skipped"] += 1
                else:
                    _meta_counts["errors"] += 1
            except Exception:
                _meta_counts["errors"] += 1
            # Now that metadata + thumbnail are on disk, re-push the
            # Recent tab so the grid-card view picks up the new
            # thumbnail. Without this, recent_for_ui runs its
            # find_thumbnail() scan BEFORE the jpg is written and the
            # card renders with an empty gradient placeholder forever.
            # Best-effort — hook is only set in app runtime.
            if _on_recent_changed_hook is not None:
                try: _on_recent_changed_hook()
                except Exception: pass

        try:
            _meta_exec.submit(_task)
        except Exception:
            pass

    # Run yt-dlp once per target URL (main channel + optional /streams pass).
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
                    bufsize=1, startupinfo=_startupinfo,
                    env=_utils.utf8_subprocess_env(),
                )
                break
            except OSError as e:
                if attempt == 2:
                    stream.emit([["ERROR: ", "red"], [f"Could not launch yt-dlp after 3 tries: {e}\n", "red"]])
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
                except Exception:
                    pass
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
                except Exception:
                    pass
                break

            s = line.rstrip()
            if not s:
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
                    _bar = "\u2588" * 65
                    stream.emit([["\n" + _bar + "\n", "red"]])
                    stream.emit([["\u2588  ", "red"],
                                 ["Browser is signed out of YouTube.",
                                  "red"],
                                 ["\n", "red"]])
                    stream.emit([["\u2588  ", "red"],
                                 ["Sign in to YouTube in Firefox (or your "
                                  "default browser) so yt-dlp can", "red"],
                                 ["\n", "red"]])
                    stream.emit([["\u2588  ", "red"],
                                 ["reuse the session cookie. Public "
                                  "videos still download without auth.",
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
                # Also cover Remuxer / FixupM3u8 — same semantic meaning.
                # Fall through so the line still gets logged.

            if s.startswith("DLTRACK:::"):
                try:
                    _, t, upl, ud, sz, dur, vid = s.split(":::", 6)
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
                        # (mirrors OLD YTArchiver.py:18443-18481 verbose emit),
                        # and upgrades the tri-state (missing / "pending" / True).
                        _prev = _title_announced.get(final_path)
                        if _prev is not True:
                            _title_announced[final_path] = True
                            downloaded += 1
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
                            # simple mode: single dense line with title/channel/size
                            # "— ✓ <title> — <channel> (NNN MB)"
                            # verbose mode: same line + Path/URL/Duration subfields
                            # Inplace: reuse the `dlrow_<N>` kind that the
                            # Downloading emit used, so the ✓ done line
                            # REPLACES the Downloading line in simple mode
                            # (matches OLD's single-line-per-download pattern).
                            #
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
                            if _dlrow_n is None:
                                _done_kind = f"dlrow_orphan_{vid or id(final_path)}"
                                # Path-match fell through to orphan, so the
                                # progress row for THIS video is still
                                # sitting at its own dlrow_<N> in the DOM
                                # with no marker match to our done line.
                                # Best-effort cleanup: emit a clear_line
                                # control for `dlrow_{_DLROW_COUNTER}` so
                                # whatever stuck Downloading row was
                                # anchored to the most recent counter
                                # gets removed from DOM. Also add it to
                                # _closed_dlrows so any late progress
                                # ticks for this counter are dropped.
                                import json as _json_mod
                                _stuck_kind = f"dlrow_{_DLROW_COUNTER}"
                                try:
                                    stream.emit([
                                        [_json_mod.dumps({
                                            "kind": "clear_line",
                                            "marker": _stuck_kind,
                                        }), "__control__"],
                                    ])
                                except Exception:
                                    pass
                                _closed_dlrows.add(_DLROW_COUNTER)
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
                                [f" \u2014 {name}", ["simpleline", _done_kind]],
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
                            from . import index as _idx
                            # audit C-8: pass the duration through so
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
                            # bug S-1: surface the failure so the user
                            # knows a download they just saw succeed is
                            # actually invisible in Browse/Search. Old
                            # code silently swallowed this.
                            stream.emit_dim(
                                f" (index register failed for {t!r}: {_re})")
                        try:
                            _record_recent_download(final_path, name, t, vid,
                                                    upload_date=(ud or "").strip())
                        except Exception as _re2:
                            # bug S-2: Recent tab goes stale silently
                            # on any cache write failure without this.
                            stream.emit_dim(
                                f" (recent downloads write failed: {_re2})")
                        # bug C-3: if this vid was previously deferred
                        # as a livestream/premiere, drop it from the
                        # deferred journal now that we've successfully
                        # grabbed the recording. Without this, the
                        # Deferred Livestreams drawer accumulates stale
                        # entries forever; only the manual Ignore button
                        # ever removed them.
                        try:
                            from . import livestreams as _ls
                            _ls.drop(vid)
                        except Exception:
                            pass
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
                                from . import ytarchiver_config as _cfg
                                _cfg.append_pending_tx_id(name, vid)
                            except Exception as _re3:
                                # bug S-3: pending transcribe list
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
                                        from . import compress as _cmp
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
                                                    video_id=vid)
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
                                    stream.emit_error(f"Compress enqueue failed: {_e}")
                            else:
                                try:
                                    from . import compress as _cmp
                                    _cmp.compress_video(final_path, stream,
                                                         quality=_comp_lvl,
                                                         output_res=_comp_res,
                                                         cancel_event=cancel_event)
                                except Exception as _e:
                                    stream.emit_error(f"Compress failed: {_e}")
                except Exception:
                    pass
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
                # audit E-4: count archived-skip lines toward the
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
            #
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
                # Bug [98]: emit a fallback progress line even when
                # the title isn't resolvable yet (the Destination line
                # for this counter hasn't been processed). Previously
                # we silently dropped the tick, leaving the UI looking
                # stuck for a video that's actually downloading.
                if not _disp:
                    _disp = f"#{_DLROW_COUNTER}"
                stream.emit([
                    [" ", ["dim", _prog_kind]],
                    ["\u2014 Downloading ", ["simpleline_green", _prog_kind]],
                    [f"{_disp} ", ["simpleline", _prog_kind]],
                    [f"{pct}%\n", ["dlprogress_pct", _prog_kind]],
                ])
                continue

            # Livestream / scheduled premiere — defer instead of treating as error
            try:
                from . import livestreams as _ls
                if _ls.line_looks_live(s):
                    id_m = re.search(r"\b([A-Za-z0-9_-]{11})\b", s)
                    vid = id_m.group(1) if id_m else ""
                    # audit F-22: if the line didn't carry a visible
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
                    # bug L-3: if the regex misses, nothing gets written
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
            except Exception:
                pass

            # Error lines.
            # audit F-21: require the error/warning tokens to appear
            # with a trailing colon or bracket, so a video whose
            # literal title contains the word "error" (or "warning")
            # doesn't falsely bump the error counter on its DLTRACK
            # line. yt-dlp's own error format is "ERROR: ..." (or
            # "WARNING: ..."), which the stricter regex matches.
            low = s.lower()
            if (re.search(r"\berror[\s:\]\[]", low)
                    or re.search(r"\bwarning[\s:\]\[]", low)):
                # Benign non-errors that yt-dlp prints as ERROR lines.
                # bug L-4: extended the allowlist so the error counter
                # doesn't inflate on these "noise" lines. Counter
                # remains accurate for real failures.
                # audit D-4: private / deleted / terminated videos
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
                # Members-only content — not an error, just a skip.
                # Matches OLD YTArchiver.py:18770. Auto-archive the id
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
                        try:
                            with open(ARCHIVE_FILE, "a",
                                      encoding="utf-8") as _af:
                                _af.write(f"youtube {_vid_id}\n")
                        except OSError:
                            pass
                    continue
                stream.emit([[f" {s}\n", "red" if "error" in low else "filterskip"]])
                if "error" in low:
                    errors += 1
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
            proc.terminate()
        # End of per-URL pass (main /videos or /streams). Loop picks up the
        # next URL if there is one.

    elapsed = time.time() - t_start
    took = _fmt_duration(elapsed)

    # Per-channel [Sync] Done line is now rendered by sync_start_all via
    # _sync_row_emit — one compact `[N/total] Name \u2014 summary` row that
    # replaces the live header in place. No duplicate line here.

    # TuneShine: push per-channel totals so the display shows live progress.
    # pass_idx/pass_total come from sync_all's queue-driven loop so the
    # external TuneShine display reads e.g. "7/47" — single-channel syncs
    # default to 1/1. Fixes bug where bulk syncs always showed 1/1.
    try:
        write_sync_progress(channel_name=name,
                            idx=int(pass_idx or 1),
                            total=max(int(pass_total or 1), int(pass_idx or 1)),
                            downloaded=downloaded,
                            skipped=0, errors=errors)
    except Exception:
        pass

    # Clear current-sync marker
    if queues is not None:
        queues.set_current_sync(None)

    # Drain the inline-metadata executor. Every video dispatched during
    # this pass is now waiting (or running) on the one worker thread.
    # Shutdown(wait=True) blocks until all queued tasks finish.
    if _meta_exec is not None:
        try:
            _meta_exec.shutdown(wait=True)
        except Exception:
            pass

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
        _meta_fetched = int(_meta_counts.get("fetched", 0) or 0)
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
            except Exception:
                pass
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
                # bug S-8: if the activity-log row register fails, the
                # sync completed but the user sees no history row.
                # Surfacing via dim log at least confirms the sync ran.
                stream.emit_dim(
                    f" (activity-log register failed: {_re4})")

    # Remember the fresh IDs in the channel cache for the next sync pass
    # (lets the quick-check-new-uploads fast path skip enumeration).
    if downloaded_ids and (cancel_event is None or not cancel_event.is_set()):
        try:
            from . import channel_cache as _cc
            _cc.append_ids(url, downloaded_ids)
        except Exception:
            pass

    # Update config last_sync + initialized/sync_complete flags. These
    # are load-bearing for the `--break-on-existing` gate above: OLD
    # stamps `initialized=True` and `sync_complete=True` after a
    # successful sync (YTArchiver.py:22963, :19227, :22932) so every
    # subsequent sync on that channel uses `--break-on-existing` and
    # bails fast. Without this write, new channels never graduate to
    # the fast-sync path.
    if config_is_writable():
        # audit H-1: normalize the URL once here so the channel-match
        # loop below can't silently fail on a trailing-slash /
        # www-prefix / casing difference. Without normalization, a
        # mismatch meant `_dirty` stayed False, no channel was
        # updated, initialized never flipped → sync stuck in slow
        # path forever.
        try:
            from . import subs as _subs_norm
            _url_norm = _subs_norm.normalize_channel_url(url) or url
        except Exception:
            _url_norm = url
        _url_norm = (_url_norm or "").strip().rstrip("/")
        _config_write_err: Optional[str] = None
        try:
            # bug C-1: `now` was referenced below without ever being
            # defined in this scope. The outer try/except silently swallowed
            # the NameError so `last_sync`, `initialized`, and `sync_complete`
            # never got written when a sync actually downloaded anything —
            # invisibly defeating the --break-on-existing fast-path on every
            # subsequent sync.
            now = datetime.now()
            cfg2 = load_config()
            _dirty = False
            # bug S-6: require AT LEAST ONE video to have been walked
            # (downloaded > 0 OR errors > 0, meaning yt-dlp actually
            # ran end-to-end) before stamping `initialized=True`. A
            # first sync that 0-downloads AND 0-errors almost certainly
            # hit a filter wall (strict duration filter, empty
            # playlist, auth failure) — stamping initialized there locks
            # the channel into fast-path forever even though it was
            # never actually bootstrapped.
            # audit D-5: only stamp initialized when downloaded > 0.
            # Treating errors as "walked meaningfully" was wrong —
            # a first sync that cookies-expired or got blanket-
            # --match-filter'd has errors > 0 but zero real walk, and
            # marking it initialized locks the channel into fast-path
            # forever (--break-on-existing stops the walk at the first
            # archive hit). Real first-sync success needs an actual
            # download.
            _walked_meaningfully = (downloaded > 0)
            _meta_did_fetch = int(_meta_counts.get("fetched", 0) or 0) > 0
            _now_ts = time.time()
            # audit D-2: update last_sync on every SUCCESSFUL pass, not
            # just ones with new downloads. A channel with 0 new videos
            # is still being checked — last_sync going stale-for-months
            # made users think their channel wasn't syncing when it
            # was. No new downloads doesn't mean no activity.
            _matched_any = False
            for c in cfg2.get("channels", []):
                # audit H-1: compare normalized URLs so a saved
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
                if _walked_meaningfully:
                    if not c.get("initialized", False):
                        c["initialized"] = True
                        _dirty = True
                    if not c.get("sync_complete", False):
                        c["sync_complete"] = True
                        _dirty = True
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
                # audit H-1: surface the mismatch as a dim warning.
                # Before, a URL-normalization mismatch silently no-
                # op'd the whole config write; no log line existed
                # to explain why the channel never graduated to
                # fast-path.
                _config_write_err = (
                    f"config update: no channel matched URL "
                    f"{url!r} (normalized {_url_norm!r}) — "
                    f"last_sync/initialized/sync_complete not written.")
        except (OSError, PermissionError, ValueError, KeyError) as _ce:
            # audit H-2: narrow the catch. Bare `except Exception: pass`
            # was hiding config-write failures (disk full, file lock,
            # JSON parse error from a corrupted config). Now we log a
            # dim warning so the user can investigate instead of
            # puzzling over why syncs never gain the fast-path flags.
            _config_write_err = f"config update failed: {_ce}"
        if _config_write_err:
            try:
                stream.emit_dim(f" ({_config_write_err})")
            except Exception:
                pass

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
            _swept = _sweep_orphan_vtts(str(ch_dir))
            if _swept > 0:
                stream.emit([[" ", "dim"],
                             [f"Swept {_swept} orphan caption file(s).\n", "dim"]])
        except Exception as _sve:
            stream.emit_dim(f" (vtt sweep skipped: {_sve})")

    # Also refresh the channel's avatar/banner art. Internal 30-day
    # threshold on channel_art.fetch_channel_art means this is near-free
    # when art is already current. Matches OLD auto-fetch behavior.
    try:
        from . import channel_art as _ca
        _ca.fetch_channel_art(url or "", str(ch_dir), force=False)
    except Exception as _ae:
        stream.emit_dim(f" (channel-art refresh skipped: {_ae})")

    # Clear the sync-active flag — allow transcribe._flush_batch_stats to
    # emit standalone [Trnscr] rows for any transcriptions that slip in
    # after this point (shouldn't happen for sync-originated jobs since
    # we already consumed + emitted, but harmless if it does).
    clear_sync_active(name)

    # audit D-3: ok is now True only when yt-dlp exited with a
    # recognized code (0 = clean, 1 = "some entries failed" which
    # we've already accounted for in the errors counter). Any other
    # return code means yt-dlp crashed mid-walk; callers can surface
    # the failure instead of reporting "no new videos" on a channel
    # that actually had more content to grab.
    _ok = proc.returncode in (0, 1)
    return SyncResult(ok=_ok, downloaded=downloaded, errors=errors,
                      took=took, exit=proc.returncode,
                      total=downloaded + _archived_skipped)


# ── TuneShine sync-progress JSON ────────────────────────────────────────
# Writes current sync state to a shared JSON file so the TuneShine display
# display tool can render progress without talking to us over HTTP.
# Mirrors YTArchiver.py:302 _write_sync_progress / :326 _clear_sync_progress.

_SYNC_PROGRESS_STATE = {"totals": {"dl": 0, "skip": 0, "err": 0}}

def _sync_progress_path() -> str:
    from .ytarchiver_config import APP_DATA_DIR
    return os.path.join(str(APP_DATA_DIR), "sync_progress.json")


def write_sync_progress(channel_name: str = "",
                        idx: int = 0, total: int = 0,
                        downloaded: int = 0, skipped: int = 0,
                        errors: int = 0) -> None:
    """Write sync state to sync_progress.json for TuneShine to read."""
    try:
        # Accumulate session totals so TuneShine sees the same numbers
        # OLD shows in its own header. Totals are reset by clear_sync_progress.
        t = _SYNC_PROGRESS_STATE["totals"]
        t["dl"] += int(downloaded or 0)
        t["skip"] += int(skipped or 0)
        t["err"] += int(errors or 0)
        data = {
            "running": True,
            "channel": channel_name or "",
            "idx": int(idx or 1),
            "total": int(total or 1),
            "dl": t["dl"],
            "skip": t["skip"],
            "err": t["err"],
        }
        path = _sync_progress_path()
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except Exception:
        pass


def clear_sync_progress() -> None:
    """Remove sync_progress.json when the pass ends + reset totals."""
    _SYNC_PROGRESS_STATE["totals"] = {"dl": 0, "skip": 0, "err": 0}
    try:
        p = _sync_progress_path()
        if os.path.exists(p):
            os.remove(p)
    except Exception:
        pass


def _sweep_orphan_vtts(channel_folder: str) -> int:
    """Delete orphan `.vtt` / `.ttml` / `.srt` caption sidecars under a
    channel folder. Called after each sync pass — ensures the archive
    stays clean even when auto-transcribe is off or when the transcribe
    fast-path crashed mid-run.
    """
    if not channel_folder or not os.path.isdir(channel_folder):
        return 0
    removed = 0
    exts = (".vtt", ".ttml", ".srt")
    for dp, _dns, fns in os.walk(channel_folder):
        # Skip the hidden Thumbnails folder
        base = os.path.basename(dp)
        if base == ".Thumbnails" or base == ".ChannelArt":
            continue
        for fn in fns:
            if fn.lower().endswith(exts):
                try:
                    os.remove(os.path.join(dp, fn))
                    removed += 1
                except OSError:
                    pass
    return removed


def _scan_recent_video(channel_dir) -> Optional[str]:
    """Last-resort fallback: scan a channel folder tree for the most
    recent video file (.mp4 / .mkv / .webm) created in the last 10 min.
    Mirrors YTArchiver.py:18350 — when Merger + Destination parsing both
    fail to hand us a valid path (obscure formats, unicode filename
    oddities, FixupM3u8 variants), we fall back to "what's the newest
    file on disk". Uses ctime on Windows because `--mtime` resets mtime
    to the upload date (often years old) which defeats the recency check.
    """
    try:
        channel_dir = str(channel_dir)
        if not channel_dir or not os.path.isdir(channel_dir):
            return None
        exts = (".mp4", ".mkv", ".webm")
        now = time.time()
        tkey = os.path.getctime if os.name == "nt" else os.path.getmtime
        best_path = None
        best_t = 0.0
        for dp, _dns, fns in os.walk(channel_dir):
            bn = os.path.basename(dp)
            if bn in (".Thumbnails", ".ChannelArt"):
                continue
            for fn in fns:
                if not fn.lower().endswith(exts):
                    continue
                fp = os.path.join(dp, fn)
                try:
                    t = tkey(fp)
                except OSError:
                    continue
                if (now - t) > 600:
                    continue
                if t > best_t:
                    best_t = t
                    best_path = fp
        return best_path
    except Exception:
        return None


def _resolve_final_mp4(dest_path: str) -> Optional[str]:
    """yt-dlp's 'Destination:' line shows intermediate paths too (video.f137.mp4,
    video.en.vtt, video.en-orig.vtt, video.description, etc.). Return the
    final merged .mp4 path ONLY when the destination is a real video track —
    otherwise return None so the caller skips transcribe enqueue + recent
    recording for captions/metadata sidecars.
    """
    p = Path(dest_path)
    ext = p.suffix.lower()
    # Skip non-video destinations — captions, descriptions, info.json, etc.
    if ext not in (".mp4", ".mkv", ".webm", ".m4a", ".mp3", ".flac",
                   ".wav", ".opus", ".ogg"):
        return None
    stem = p.stem
    if "." in stem:
        parts = stem.split(".")
        last = parts[-1]
        # Strip yt-dlp's format selector suffix — `.fNNN` (e.g. `.f137`) for
        # simple single-track formats, OR `.fNNN-X` / `.fNNN-drc` / `.fNNN-1`
        # etc. when the source has multiple audio tracks / DRC variants.
        # Pattern: `f` followed by a digit, then anything (or nothing).
        # Earlier version used `last[1:].isdigit()` which failed on
        # `f140-16` because `.isdigit()` rejects the dash → downloaded
        # counter never incremented for any channel with multi-track
        # audio (observed on bodycam / multi-language content).
        if (len(last) >= 2 and last[0] == "f" and last[1].isdigit()):
            stem = ".".join(parts[:-1])
        # Strip language codes left from `--write-subs` (e.g. `.en`, `.en-orig`,
        # `.en-us`). These don't appear on merged video outputs but defensive
        # handling prevents future caption-related regressions.
        elif last.lower() in ("en", "en-orig", "en-us", "en-gb",
                               "en-uk", "es", "fr", "de", "pt", "it"):
            stem = ".".join(parts[:-1])
    # audit F-18: preserve the ORIGINAL container extension when it was
    # already a known video format. Hardcoding .mp4 broke DLTRACK path
    # resolution when yt-dlp merged to .mkv / .webm (happens when the
    # selected codec combo can't mux into mp4). Recent tab / Browse
    # grid would then point at a non-existent .mp4 and fall back to
    # scan-recent which may pick up the wrong file.
    _video_container_exts = (".mp4", ".mkv", ".webm")
    _target_ext = ext if ext in _video_container_exts else ".mp4"
    final = p.parent / f"{stem}{_target_ext}"
    # Return regardless of existence — file may still be writing when we enqueue
    return str(final)


def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    hours = seconds // 3600
    rem = seconds - hours * 3600
    return f"{hours}h {rem // 60}m"


def _fmt_size(size_bytes) -> str:
    """Human-readable byte size. Mirrors YTArchiver.py — used in the
    "— ✓ Title — Channel (NN MB)" download confirmation line."""
    try:
        n = int(size_bytes)
    except (TypeError, ValueError):
        return ""
    if n <= 0:
        return ""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


# ── Sync all subscribed channels ───────────────────────────────────────

_BATCH_LIMIT = 100000 # YTArchiver.py:17503
_BATCH_COOLDOWN_HOURS = 72 # YTArchiver.py:17504


def prefetch_channel_total(ch_url: str, timeout_sec: int = 30
                            ) -> Dict[str, Any]:
    """Query YouTube for a channel's total video count + live-stream count
    before kicking off sync. Mirrors YTArchiver.py:17590 _prefetch_total and
    :18017 _prefetch_livestreams — purely informational, never blocks sync.

    Returns {ok, total, lives, upcoming, error?}.
    """
    yt_dlp = find_yt_dlp()
    if not yt_dlp or not ch_url:
        return {"ok": False, "error": "yt-dlp missing or no URL"}
    cmd = [
        yt_dlp, "--flat-playlist", "--no-warnings",
        "--print", "%(id)s|||%(live_status)s",
    ]
    cmd += _find_cookie_source() or []
    cmd.append(ch_url)
    total = 0
    lives = 0
    upcoming = 0
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, encoding="utf-8", errors="replace",
            creationflags=(0x08000000 if os.name == "nt" else 0),
            env=_utils.utf8_subprocess_env(),
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        deadline = time.time() + float(timeout_sec)
        for line in proc.stdout:
            if time.time() > deadline:
                proc.terminate()
                break
            raw = line.strip()
            if "|||" not in raw:
                continue
            _, status = raw.split("|||", 1)
            status = status.strip().lower()
            total += 1
            if status == "is_live":
                lives += 1
            elif status == "is_upcoming":
                upcoming += 1
    finally:
        try: proc.wait(timeout=5)
        except Exception:
            try: proc.kill()
            except Exception: pass
    return {"ok": True, "total": total, "lives": lives, "upcoming": upcoming}


def _ensure_videos_tab(url: str) -> str:
    """Append `/videos` to a channel URL so yt-dlp targets only the main
    uploads tab, not the multi-tab playlist (Videos + Live + Shorts).
    Mirrors YTArchiver.py:2594 exactly — only rewrites @Handle, /channel/,
    /c/, /user/ URLs. Leaves video URLs + arbitrary URLs alone.
    """
    u = (url or "").rstrip("/")
    if (("/@" in u or "/channel/" in u or "/c/" in u or "/user/" in u)
            and not u.endswith("/videos")):
        u += "/videos"
    return u


def quick_check_new_uploads(ch_url: str, archived_ids,
                            check_count: int = 5, timeout_sec: int = 30
                            ) -> Dict[str, Any]:
    """Probe the first N videos of a channel to see if any are NOT in our
    archive already. Short-circuit for channels with nothing new.

    Mirrors YTArchiver.py:17943 _quick_check_new_uploads exactly:
      - `_ensure_videos_tab(url)` so the multi-tab playlist doesn't suck
        in the Live/Shorts tabs
      - `--lazy-playlist` so yt-dlp stops enumerating once it has enough
      - `--playlist-end N` (not `--playlist-items 1:N`) — the OLD flag
      - `archived_ids` can be a list or set; we coerce to a set for O(1)
    Returns {ok, has_new, checked, fresh_ids}.
    """
    yt_dlp = find_yt_dlp()
    if not yt_dlp or not ch_url:
        return {"ok": False, "error": "yt-dlp missing or no URL"}
    qc_url = _ensure_videos_tab(ch_url)
    cmd = [
        yt_dlp,
        "--flat-playlist", "--lazy-playlist",
        "--playlist-end", str(int(check_count)),
        "--print", "id",
        "--no-warnings",
    ]
    cmd += _find_cookie_source() or []
    cmd.append(qc_url)
    if isinstance(archived_ids, set):
        archived_set = archived_ids
    else:
        archived_set = {x.strip() for x in (archived_ids or []) if x}
    checked: List[str] = []
    fresh: List[str] = []
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=float(timeout_sec),
            encoding="utf-8", errors="replace",
            creationflags=(0x08000000 if os.name == "nt" else 0),
            env=_utils.utf8_subprocess_env(),
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}
    for raw in (proc.stdout or "").splitlines():
        raw = raw.strip()
        if not raw or not re.fullmatch(r'[A-Za-z0-9_-]{11}', raw):
            continue
        checked.append(raw)
        if raw not in archived_set:
            fresh.append(raw)
    # Empty result = treat as "might have new" per OLD's behavior
    # (line 17980-17981: `if not ids: return True`).
    if not checked:
        return {"ok": True, "has_new": True,
                "checked": 0, "fresh_ids": []}
    return {"ok": True, "has_new": bool(fresh),
            "checked": len(checked), "fresh_ids": fresh}


def _check_batch_cooldown(ch: Dict[str, Any]) -> Tuple[bool, str]:
    """Return (can_proceed, cooldown_label).

    Channels that haven't been fully initialized AND have >100k videos get
    a 72-hour cooldown between syncs to avoid hammering YouTube for pagination
    during bootstrap. Mirrors YTArchiver.py:17507 _check_batch_cooldown.
    """
    from datetime import datetime as _dt
    batch_after = ch.get("init_batch_after")
    if not batch_after:
        return True, ""
    try:
        cooldown_dt = _dt.fromisoformat(batch_after).replace(tzinfo=None)
        if _dt.now() >= cooldown_dt:
            return True, ""
        time_str = cooldown_dt.strftime("%I:%M%p").lstrip("0").lower()
        date_str = cooldown_dt.strftime("%b %d")
        return False, f"{time_str}, {date_str}"
    except (ValueError, TypeError):
        return True, ""


def _should_batch_limit(ch: Dict[str, Any], ch_total: int) -> bool:
    """Return True if this channel should be subject to batch cooldown rules.

    bug W-9 clarification: the two flags look similar but gate
    different things.
      * `initialized`     = "first sync completed AT LEAST ONCE"
                            (even if it walked nothing useful —
                             bug S-6 tightened that path).
      * `init_complete`   = "full bootstrap has walked the whole
                             catalog" (definite: not paused mid-walk).
      * `_check_batch_cooldown` (separate fn, elsewhere) enforces the
        72h cooldown timestamp; THIS fn just decides if batch-limit
        rules apply at all.
    Order matters: mode must be full (channel-wide mode), init_complete
    short-circuits out (already past bootstrap), and only THEN we fall
    through to channel-size checks.
    """
    if ch.get("mode", "full") != "full":
        return False
    if ch.get("init_complete", False):
        return False
    if ch_total > 0:
        return ch_total > _BATCH_LIMIT
    # Count unavailable — batch limit if channel isn't initialized yet
    return not ch.get("initialized", False)


# UI push hook — main.py registers a callable here so the Recent tab
# refreshes live whenever a download lands. No-op if unset (unit tests).
_on_recent_changed_hook: Optional[Any] = None
_on_metadata_changed_hook: Optional[Any] = None


def set_recent_changed_hook(hook: Optional[Any]) -> None:
    """Main.py wires this in __init__ so the Recent tab auto-refreshes
    when a download completes. Hook gets no args — caller re-fetches the
    current recent_downloads list and pushes to the UI."""
    global _on_recent_changed_hook
    _on_recent_changed_hook = hook


def set_metadata_changed_hook(hook: Optional[Any]) -> None:
    """Main.py wires this so Settings > Metadata auto-refreshes its
    `XXm ago` timestamps after any metadata / metadata_comments /
    videoid_backfill task completes. Without this, the tab keeps
    showing pre-pass stamps until the user clicks Reload."""
    global _on_metadata_changed_hook
    _on_metadata_changed_hook = hook


# Channels with an in-flight `sync_channel` call. Used by
# transcribe._flush_batch_stats to skip channels whose consolidated
# [Dwnld] row isn't ready to emit yet (sync_channel is still running).
# Without this, the transcribe worker drains before sync_channel ends,
# flush fires, and a standalone [Trnscr] row beats sync_channel to the
# activity log. "a single line. [Dwnld]...". `set_sync_active`
# wraps sync_channel entry, `clear_sync_active` wraps exit.
_active_sync_channels: set = set()
_active_sync_lock = threading.Lock()


def set_sync_active(channel_name: str) -> None:
    with _active_sync_lock:
        _active_sync_channels.add(channel_name)


def clear_sync_active(channel_name: str) -> None:
    with _active_sync_lock:
        _active_sync_channels.discard(channel_name)


def is_sync_active(channel_name: str) -> bool:
    with _active_sync_lock:
        return channel_name in _active_sync_channels


def _count_cell(n: int, label: str) -> str:
    """Render a count cell. If n == 1, return "\u2713 {label}" instead of
    "1 {label}" — single-video polish for transcribed + metadata.
    For 0 we still show the numeric form so the user can see
    "0 transcribed" when a channel has auto_transcribe off. For >= 2
    we show the numeric count.

    NOTE: `downloaded` is ALWAYS rendered numerically (never \u2713) per
     follow-up: "leave the downloaded part as a number. ... 1
    downloaded (check) transcribed (check) metadata". Callers emit
    downloaded via f"{n} downloaded" directly.
    """
    if n == 1:
        return f"\u2713 {label}"
    return f"{n} {label}"


def emit_metadata_activity_row(stream: "LogStreamer",
                                channel_name: str,
                                primary: str,
                                secondary: str,
                                errors: int,
                                elapsed: float,
                                green: bool = True) -> str:
    """Emit a [Metdta] activity-log row for metadata / comments / ID
    backfill tasks. Parallel to the [Dwnld] row `sync_channel` emits
    at end of a download pass. Users flagged that metadata refreshes
    were landing only in the main log, not the activity log above —
    this fills that gap so all background work has a matching history
    entry.

    Uses 2 data cells instead of [Dwnld]'s 3 (no transcribed cell —
    metadata passes never transcribe) so the row reads:
        [Mtadta] [time,date] — [channel] —
        [primary] [secondary] [N errors] [took X]

    `primary` example: "61 refreshed", "12 comments refreshed",
                       "40 IDs backfilled".
    `secondary` example: "5 new" (new metadata entries), "2 ambiguous"
                         — additional info beyond the primary count.
                         Pass empty string when nothing to add.
    """
    now = datetime.now()
    time_str = (now.strftime("%-I:%M%p") if os.name != "nt"
                else now.strftime("%I:%M%p").lstrip("0")).lower()
    date_str = now.strftime("%b %d").replace(" 0", " ")
    took = _fmt_duration(elapsed)
    row_id = f"metdta_{channel_name}_{int(time.time())}"
    # Kind label is "Metdta" (6 chars, matches the existing classic
    # rows emitted by fetch_channel_metadata's legacy path). Row tag
    # is hist_pink — metadata-kind rows have always rendered pink in
    # the activity log; `green` was only used here to signal "nothing
    # happened" vs "something happened", NOT to force green tinting.
    # When no work happened we leave the tag blank (default color).
    had_work = green and errors == 0 and primary not in (
        "up to date", "no videos in scope", "no IDs to backfill",
        "failed")
    payload = {
        "kind": "Metdta",
        "time_date": f"{time_str}, {date_str}",
        "channel": channel_name,
        "primary": primary,
        "secondary": secondary,
        "tertiary": "",
        "errors": f"{errors} errors",
        "took": f"took {took}",
        "row_tag": "hist_pink" if had_work else "",
        "row_id": row_id,
    }
    stream.emit_activity(payload)
    try:
        kind_tag = f"[{'Metdta'.center(6)}]"
        ts_date = f"{time_str}, {date_str}".ljust(16)
        ch_part = f" {channel_name} \u2014" if channel_name else " " * 7
        # Mirror the [Dwnld] persistence format but with 2 cells
        # where downloaded / transcribed / metadata would be.
        body = (f"{primary:<14} \u00b7 "
                f"{(secondary or '-'):<15} \u00b7 "
                f"{int(errors or 0)} errors \u00b7 "
                f"took {took}")
        line = f"{kind_tag} {ts_date} \u2014{ch_part} {body}"
        _persist_row_history(row_id, line)
    except Exception:
        pass
    return row_id


def emit_consolidated_auto_row(stream: "LogStreamer",
                                channel_name: str,
                                downloaded: int,
                                transcribed: int,
                                metadata: int,
                                errors: int,
                                elapsed: float,
                                kind: str = "Dwnld",
                                row_id: Optional[str] = None) -> str:
    """Emit ONE combined activity-log row replacing the historical trio
    of [Auto] + [Trnscr] + [Metdta]. UI receives four count cells:
        [kind] [time,date] \u2014 [channel] \u2014
        [primary=N downloaded] [secondary=N transcribed]
        [tertiary=N metadata] [errors=N errors] [took=took X]
    Per `downloaded` is ALWAYS numeric. `transcribed` and
    `metadata` use a \u2713 check when their count is exactly 1.
    Persisted string body is 5 bullets:
        N downloaded \u00b7 <N|\u2713> transcribed \u00b7 <N|\u2713> metadata \u00b7 N errors \u00b7 took X

    If `row_id` is provided (or generated), the UI tags the row with
    `data-row-id=<row_id>` so a later call with the same id replaces
    that row in place — used by the transcribe-complete hook to
    retroactively update a row that fired while Whisper was still
    running. Returns the row_id (existing or newly generated) so the
    caller can stash it for later updates.
    """
    now = datetime.now()
    time_str = (now.strftime("%-I:%M%p") if os.name != "nt"
                else now.strftime("%I:%M%p").lstrip("0")).lower()
    date_str = now.strftime("%b %d").replace(" 0", " ")
    took = _fmt_duration(elapsed)
    primary_s = f"{int(downloaded or 0)} downloaded"
    secondary_s = _count_cell(int(transcribed or 0), "transcribed")
    tertiary_s = _count_cell(int(metadata or 0), "metadata")
    if not row_id:
        # Channel + start-of-pass timestamp — two calls within the same
        # sync pass for the same channel share an id, but a fresh pass
        # minutes later gets a new one.
        row_id = f"dwnld_{channel_name}_{int(time.time())}"
    stream.emit_activity({
        "kind": kind,
        "time_date": f"{time_str}, {date_str}",
        "channel": channel_name,
        "primary": primary_s,
        "secondary": secondary_s,
        "tertiary": tertiary_s,
        "errors": f"{errors} errors",
        "took": f"took {took}",
        "row_tag": "hist_green" if downloaded > 0 else "",
        "row_id": row_id,
    })
    # Persist directly (bypassing format_history_entry so the checkmark
    # forms round-trip cleanly instead of being truncated to "0" by
    # the count-extraction logic). Use ljust/rjust padding so rendered
    # rows in activity-log history view line up visually.
    try:
        from . import autorun as _ar
        kind_tag = f"[{kind.center(6)}]" if len(kind) < 6 else f"[{kind}]"
        ts_date = f"{time_str}, {date_str}".ljust(16)
        ch_part = f" {channel_name} \u2014" if channel_name else " " * 7
        body = (f"{primary_s:<14} \u00b7 "
                    f"{secondary_s:<15} \u00b7 "
                    f"{tertiary_s:<13} \u00b7 "
                    f"{int(errors or 0)} errors \u00b7 "
                    f"took {took}")
        line = f"{kind_tag} {ts_date} \u2014{ch_part} {body}"
        # Row-ID-aware persist: if this row_id has already been
        # written to config (previous emit for same row), REPLACE
        # the persisted line instead of appending. Prevents
        # duplicate [Dwnld] rows when the transcribe-complete hook
        # re-emits with updated counts.
        _persist_row_history(row_id, line)
    except Exception:
        pass
    return row_id


# Row-ID-aware history persistence. `emit_consolidated_auto_row` can
# be called twice for the SAME row_id — first when sync_channel
# finishes (transcribe count may still be 0), then retroactively when
# the transcribe worker drains. Without tracking which config index
# each row_id owns, the retroactive call appends a SECOND line to
# `autorun_history`, producing duplicate [Dwnld] rows on next load
# (and sometimes immediately if renderActivityLog re-runs). This
# dict maps row_id -> index in config["autorun_history"] so the
# retroactive path overwrites the correct slot.
_HIST_INDEX_BY_ROW_ID: Dict[str, int] = {}
_HIST_INDEX_LOCK = threading.Lock()


def _persist_row_history(row_id: str, line: str) -> None:
    """Append `line` to config['autorun_history'], or replace the
    previously-persisted entry if this row_id has already been
    written. Deduplicates retroactive updates of the same row.
    """
    try:
        from . import autorun as _ar
        from . import ytarchiver_config as _cfg
    except Exception:
        return
    if not _cfg.config_is_writable():
        return
    try:
        cfg = _cfg.load_config()
        hist = cfg.setdefault("autorun_history", [])
        with _HIST_INDEX_LOCK:
            existing_idx = _HIST_INDEX_BY_ROW_ID.get(row_id) if row_id else None
            if (existing_idx is not None
                    and 0 <= existing_idx < len(hist)):
                # Retroactive update — replace the previous line.
                hist[existing_idx] = line
            else:
                hist.append(line)
                # Trim + shift any tracked indices if we exceeded cap.
                if len(hist) > _ar.AUTORUN_HISTORY_MAX:
                    trim_n = len(hist) - _ar.AUTORUN_HISTORY_MAX
                    hist = hist[-_ar.AUTORUN_HISTORY_MAX:]
                    cfg["autorun_history"] = hist
                    for _k, _v in list(_HIST_INDEX_BY_ROW_ID.items()):
                        if _v < trim_n:
                            _HIST_INDEX_BY_ROW_ID.pop(_k, None)
                        else:
                            _HIST_INDEX_BY_ROW_ID[_k] = _v - trim_n
                if row_id:
                    _HIST_INDEX_BY_ROW_ID[row_id] = len(hist) - 1
        _cfg.save_config(cfg)
    except Exception:
        pass


# Registry: channel_name -> (row_id, downloaded, metadata, errors,
# start_time). Populated by sync_channel after it emits a [Dwnld] row
# whose transcribed-count may be incomplete (Whisper still running).
# `_flush_batch_stats` in transcribe.py checks this and re-emits the
# [Dwnld] row with the updated transcribed count using the same
# row_id, so the UI updates the existing row in place instead of
# appending a separate [Trnscr].
_RECENT_DWNLD_ROWS: Dict[str, Dict[str, Any]] = {}
_RECENT_DWNLD_LOCK = threading.Lock()


def register_pending_dwnld_row(channel_name: str, row_id: str,
                                 downloaded: int, metadata: int,
                                 errors: int, elapsed_start: float) -> None:
    """Called by sync_channel right after emit_consolidated_auto_row so
    a subsequent transcribe-complete update can find the row and patch
    its transcribed cell instead of emitting a separate [Trnscr]."""
    with _RECENT_DWNLD_LOCK:
        _RECENT_DWNLD_ROWS[channel_name] = {
            "row_id": row_id,
            "downloaded": int(downloaded or 0),
            "metadata": int(metadata or 0),
            "errors": int(errors or 0),
            "elapsed_start": float(elapsed_start or time.time()),
            "registered_at": time.time(),
        }


def pop_pending_dwnld_row(channel_name: str,
                          max_age_sec: float = 1800.0
                          ) -> Optional[Dict[str, Any]]:
    """Fetch + clear this channel's pending [Dwnld] registry entry if
    it exists and is fresher than `max_age_sec` (30 min default).
    Returns None when there's no recent row to update (transcribe
    completion should fall back to emitting a standalone [Trnscr])."""
    with _RECENT_DWNLD_LOCK:
        entry = _RECENT_DWNLD_ROWS.get(channel_name)
        if entry is None:
            return None
        if time.time() - entry.get("registered_at", 0) > max_age_sec:
            _RECENT_DWNLD_ROWS.pop(channel_name, None)
            return None
        _RECENT_DWNLD_ROWS.pop(channel_name, None)
        return entry


def _record_recent_download(filepath: str, channel: str, title: str,
                             video_id: str = "",
                             upload_date: str = "") -> None:
    """Push a fresh entry onto config['recent_downloads'] (newest first).

    Keeps the list capped at 500 entries. Silently no-ops when the write
    gate is off. Mirrors YTArchiver.py:33224 record_download EXACTLY —
    field names + types match so OLD YTArchiver can read our entries
    without garbling the Recent tab columns.

    OLD schema (what we MUST write):
      title str
      channel str
      date str "YYYYMMDD" — upload date, NOT formatted
      size str raw bytes count as a string, e.g. "1234567"
      duration str raw seconds as a string, e.g. "383"
      filepath str
      video_url str
      download_ts float unix timestamp
    """
    if not filepath:
        return
    from .ytarchiver_config import load_config, save_config, config_is_writable
    if not config_is_writable():
        return
    try:
        # Raw bytes — OLD reads this as `int(size)`. Must be a plain integer
        # string, NOT a human-readable "5.2 MB".
        size_bytes = 0
        try:
            size_bytes = os.path.getsize(filepath)
        except OSError:
            pass

        # Raw seconds — OLD reads this as `int(duration)`. Must be integer
        # string of seconds, NOT "3:45".
        duration_s = ""
        try:
            r = subprocess.run(
                ["ffprobe", "-v", "error",
                 "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1",
                 filepath],
                capture_output=True, text=True, timeout=6,
                creationflags=(0x08000000 if os.name == "nt" else 0),
            )
            raw = (r.stdout or "").strip()
            if raw:
                duration_s = str(int(float(raw)))
        except Exception:
            pass

        # audit E-5: prefer yt-dlp's emitted upload_date (from DLTRACK)
        # over file mtime. On some Windows network drives + Z: drivepool
        # setups, --mtime silently fails to set mtime on the new file,
        # leaving mtime=download-time. Old code wrote "today" as the
        # upload date in that case; now we use the authoritative
        # YYYYMMDD value yt-dlp already knows and only fall back to
        # mtime if it wasn't provided.
        date_str = ""
        _ud = (upload_date or "").strip()
        if len(_ud) == 8 and _ud.isdigit():
            date_str = _ud
        else:
            try:
                from datetime import datetime as _dt
                date_str = _dt.fromtimestamp(os.path.getmtime(filepath)).strftime("%Y%m%d")
            except OSError:
                pass

        cfg = load_config()
        entries = list(cfg.get("recent_downloads", []) or [])
        # Dedupe same filepath or same title+channel
        entries = [e for e in entries
                   if e.get("filepath") != filepath
                   and not (e.get("title") == title and e.get("channel") == channel)]
        entries.insert(0, {
            "title": title or "",
            "channel": channel or "",
            "date": date_str, # YYYYMMDD — OLD shape
            "size": str(int(size_bytes)), # raw bytes as string
            "duration": duration_s, # raw seconds as string
            "filepath": filepath,
            "video_url": (f"https://www.youtube.com/watch?v={video_id}"
                            if video_id else ""),
            "download_ts": time.time(), # unix float — OLD shape
        })
        cfg["recent_downloads"] = entries[:500]
        save_config(cfg)

        # Push a live refresh to the Recent tab so a download shows up
        # immediately without needing a restart. Hook is set by main.py's
        # Api.__init__; safe no-op when unset (unit tests).
        if _on_recent_changed_hook is not None:
            try: _on_recent_changed_hook()
            except Exception: pass

        # Auto-index trigger: after every Nth download, kick off a background
        # FTS ingest of any new .jsonl files on disk. Mirrors YTArchiver.py
        # _maybe_auto_index (32104) + the call at 33247 right after record_download.
        if cfg.get("auto_index_enabled", False):
            threshold = int(cfg.get("auto_index_threshold", 10) or 10)
            # bug L-13: canonicalize on `downloads_since_last_index` (the
            # DEFAULT_CONFIG-declared field). Older code wrote to a
            # separate `_auto_index_counter` which meant the documented
            # field was never read and a "next auto-index in N"
            # progress widget couldn't be built on top of it. Migrate
            # any legacy `_auto_index_counter` into the canonical key
            # on first write so no downloads are lost.
            _legacy = int(cfg.pop("_auto_index_counter", 0) or 0)
            counter = int(cfg.get("downloads_since_last_index", 0) or 0) \
                      + _legacy + 1
            if counter >= threshold:
                cfg["downloads_since_last_index"] = 0
                save_config(cfg)
                # Fire off a background sweep — re-ingests any .jsonl that
                # wasn't already indexed (cheap no-op for already-indexed ones).
                import threading as _thr
                def _bg_sweep():
                    try:
                        from . import index as _idx
                        output_dir = (cfg.get("output_dir") or "").strip()
                        if output_dir:
                            _idx.sweep_new_videos(output_dir, cfg.get("channels", []))
                    except Exception:
                        pass
                _thr.Thread(target=_bg_sweep, daemon=True).start()
            else:
                cfg["downloads_since_last_index"] = counter
                save_config(cfg)
    except Exception:
        pass


def build_batch_file(video_ids) -> Optional[str]:
    """Write video IDs as full YouTube URLs to a temp file for --batch-file.

    Returns the temp file path, or None on error. Mirrors YTArchiver.py:17993
    _build_batch_file. The caller is responsible for calling
    `cleanup_batch_file()` after yt-dlp finishes.
    """
    from .ytarchiver_config import APP_DATA_DIR
    path = str(APP_DATA_DIR / "batch_urls_temp.txt")
    try:
        with open(path, "w", encoding="utf-8") as f:
            for vid in video_ids or []:
                vid = (vid or "").strip()
                if vid:
                    f.write(f"https://www.youtube.com/watch?v={vid}\n")
        return path
    except OSError:
        return None


def cleanup_batch_file() -> None:
    """Remove the temp batch-URL file if present."""
    from .ytarchiver_config import APP_DATA_DIR
    path = APP_DATA_DIR / "batch_urls_temp.txt"
    try:
        if path.exists():
            path.unlink()
    except OSError:
        pass


def set_batch_cooldown(ch_url: str) -> None:
    """Apply a 72h cooldown to a channel (called after a bootstrap run)."""
    from datetime import datetime as _dt, timedelta as _td
    from . import subs as _subs
    cfg = load_config()
    # Normalize once for the comparison key so trailing slash / www / scheme
    # variants between the live URL and the config-stored URL still match.
    try:
        target = _subs.normalize_channel_url(ch_url)
    except Exception:
        target = ch_url
    changed = False
    for cfg_ch in cfg.get("channels", []):
        cfg_url = cfg_ch.get("url", "")
        try:
            cfg_norm = _subs.normalize_channel_url(cfg_url)
        except Exception:
            cfg_norm = cfg_url
        if cfg_norm == target or cfg_url == ch_url:
            cfg_ch["init_batch_after"] = (_dt.now() + _td(hours=_BATCH_COOLDOWN_HOURS)).isoformat()
            changed = True
    if changed:
        try:
            from .ytarchiver_config import save_config as _sc
            _sc(cfg)
        except Exception:
            pass


def sync_all(stream: LogStreamer, cancel_event: Optional[threading.Event] = None,
             only_with_new: bool = True, queues=None, transcribe_mgr=None,
             pause_event: Optional[threading.Event] = None,
             skip_event: Optional[threading.Event] = None,
             add_downloads_from_config: bool = True) -> Dict[str, Any]:
    """
    Sync every channel in config["channels"] sequentially.

    `pause_event`: while set, the loop blocks between channels (~0.5s poll).
    `skip_event`: if set mid-channel, the cancel_event is fired to kill the
                   current yt-dlp subprocess; the outer loop then clears both
                   events and advances to the next channel. Total cancellation
                   still works via cancel_event directly.
    """
    cfg = load_config()
    channels = cfg.get("channels", [])
    if not channels:
        stream.emit(_bracket_segments("Sync") +
                    [["No channels subscribed.\n", "simpleline"]])
        return {"ok": False, "reason": "no channels", "total": 0}

    # ENQUEUE DECISION:
    # - `add_downloads_from_config=True` (Sync Subbed) AND no
    # download tasks queued yet → add every subscribed channel as
    # a download task. `sync_enqueue` dedupes on (kind, url) so
    # pre-existing metadata tasks stay intact; downloads append
    # alongside.
    # - `add_downloads_from_config=False` (worker started just to
    # drain the queue, e.g. from `metadata_queue_all`) → never
    # touch the queue. Process whatever is there and stop.
    # - `add_downloads_from_config=True` BUT download tasks already
    # exist (paused-then-resumed Sync Subbed) → resume mode, keep
    # the existing queue as-is.
    # a user hit bug where queuing 103 metadata tasks auto-fired the
    # worker via sync_start_all, which in turn added 103 downloads —
    # "Sync pass starting (206 channels)" when he only asked for 103
    # metadata checks.
    _resume_mode = False
    if queues is not None:
        try:
            existing_dl = any(
                (c.get("kind") or "download").lower() == "download"
                for c in queues.sync
            )
            if existing_dl:
                _resume_mode = True
            elif add_downloads_from_config:
                for _ch in channels:
                    queues.sync_enqueue(_ch)
            # else: worker was started just to drain the queue — do
            # not touch it.
        except Exception:
            pass

    # Per-pass unique id — stashed on a thread-local that
    # `_sync_row_emit` reads by default, so every call site inside the
    # sync loop picks it up without having to pass it explicitly.
    # Autorun-fired second passes were silently replacing the first
    # pass's rows in-place (far above the current scroll) because
    # `sync_row_1` collided across passes; a fresh id per pass fixes
    # it cleanly. Cleared in the `finally` at the bottom of this func.
    _ROW_EMIT_PASS_ID.id = _new_pass_id()

    # Reset the cookie-sign-out alert flag so this pass can emit the
    # red banner once if yt-dlp surfaces a sign-in / cookie-extract
    # error. Without resetting, a fix-then-resync wouldn't show the
    # all-clear path — the flag stays True from the prior pass.
    global _COOKIE_ALERT_FIRED
    _COOKIE_ALERT_FIRED = False

    # Start-of-pass header — show total remaining work, not len(config).
    # In resume mode that's the restored queue size; fresh mode it's the
    # whole channel list we just enqueued. Exclude kind=redownload
    # entries — those are handled by Api._redwnl_worker and appear in
    # the queue only for popover visibility; counting them in "Sync
    # pass starting (N channels)" would over-report.
    try:
        _queue_snapshot = [
            c for c in (queues.sync if queues is not None else [])
            if (c.get("kind") or "download").lower() != "redownload"
        ]
        _starting_total = len(_queue_snapshot) if queues is not None else len(channels)
    except Exception:
        _queue_snapshot = []
        _starting_total = len(channels)

    # Label the banner with the actual action the queue represents
    # instead of the generic "Sync pass". A queue of views-refresh items
    # now says "Views/likes refresh starting" so the user isn't
    # second-guessing what the pass is doing. Mixed kinds fall back to
    # "Sync pass".
    def _pass_label(items):
        if not items:
            return "Sync pass"
        kinds = {(c.get("kind") or "download").lower() for c in items}
        if kinds == {"download"}:
            return "Sync pass"
        if kinds == {"metadata_comments"}:
            return "Comments refresh"
        if kinds == {"videoid_backfill"}:
            return "Video ID backfill"
        if kinds == {"metadata"}:
            # All refresh=True → views/likes refresh; all refresh=False →
            # metadata download; mixed → generic metadata pass.
            refreshes = {bool(c.get("refresh")) for c in items}
            if refreshes == {True}:
                return "Views/likes refresh"
            if refreshes == {False}:
                return "Metadata download"
            return "Metadata pass"
        return "Sync pass"  # mixed kinds

    _label = _pass_label(_queue_snapshot if queues is not None else channels)
    if _resume_mode:
        stream.emit([[f"=== Resuming {_label.lower()} ", "header"],
                     [f"({_starting_total} channels remaining) ===\n", "header"]])
    else:
        stream.emit([[f"=== {_label} starting ", "header"],
                     [f"({_starting_total} channels) ===\n", "header"]])

    sum_dl = 0
    sum_err = 0
    skipped = 0
    # Per-kind accumulators so the Pass-complete line can say the
    # verb that actually happened — "X refreshed" for a views/likes
    # pass, "X comments refreshed" for a comments pass, "X IDs
    # backfilled" for a video_id backfill, etc. Before, only sum_dl
    # / sum_err tracked anything, so a refresh pass always read as
    # "0 downloaded" even when 900 videos had their counts updated.
    sum_meta_refreshed = 0    # bulk_refresh_views_likes `refreshed`
    sum_meta_fetched = 0      # new metadata entries (first-time fetch)
    sum_comments_refreshed = 0
    sum_ids_backfilled = 0
    t_start = time.time()

    # Load the global download-archive ONCE into a set for O(1) membership
    # tests. This backs the per-channel "quick check" fast path below, which
    # probes the first 5 videos of each channel and short-circuits the full
    # yt-dlp walk when everything is already archived. Mirrors the OLD
    # YTArchiver _load_archived_ids + _quick_check_new_uploads pairing.
    _known_ids: set = set()
    try:
        if os.path.isfile(ARCHIVE_FILE):
            with open(ARCHIVE_FILE, "r", encoding="utf-8", errors="replace") as _af:
                for _line in _af:
                    # Format: "youtube VIDEOID\n" — split and keep the id
                    _parts = _line.strip().split(None, 1)
                    if len(_parts) == 2:
                        _known_ids.add(_parts[1])
    except OSError:
        pass

    def _now_clock() -> str:
        # "1:03am" style, matching OLD's log format.
        now = datetime.now()
        return now.strftime("%-I:%M%p") if os.name != "nt" \
               else now.strftime("%I:%M%p").lstrip("0")

    # Mid-pass pause state. `_last_live` tracks the row we most recently
    # painted as live so we can re-paint it as PAUSED: Name when the user
    # pauses between channels, then back to live when they Resume.
    # `total` is dynamic (processed + remaining) and gets updated on each
    # pop — matches YTArchiver.py:19138 `current_total = processed + 1 +
    # len(_sync_queue)`. Initial value is the starting queue size.
    _last_live = {"i": 0, "total": _starting_total, "name": ""}

    def _wait_if_paused():
        """If pause_event is set, log pause + wait until resumed.
        Re-paints the last live row as PAUSED and back. Idempotent."""
        if pause_event is None or not pause_event.is_set():
            return
        # Re-paint the last live row (if any) in paused style.
        if _last_live["name"]:
            _sync_row_emit(stream,
                           _last_live["i"], _last_live["total"],
                           f"PAUSED: {_last_live['name']}",
                           name_tag="simpleline", summary_tag="dim")
        stream.emit([
            ["\u23F8 Sync paused at ", "simpleline"],
            [_now_clock().lower(), "simpleline"],
            [" \u2014 click Resume.\n", "dim"],
        ])
        # Tell the UI the pause is now ACTUALLY in effect (vs just
        # requested). Frontend stops blinking the Resume button.
        if queues is not None:
            try: queues.set_sync_paused_active(True)
            except Exception: pass
        # Block until resumed (or cancelled).
        while pause_event.is_set():
            if cancel_event is not None and cancel_event.is_set():
                if queues is not None:
                    try: queues.set_sync_paused_active(False)
                    except Exception: pass
                return
            time.sleep(0.25)
        # Resumed — clear the active flag.
        if queues is not None:
            try: queues.set_sync_paused_active(False)
            except Exception: pass
        stream.emit([
            ["\u25B6 Sync resumed at ", "simpleline_green"],
            [_now_clock().lower(), "simpleline_green"],
            [".\n", "dim"],
        ])
        # Re-paint the last row back to live (without PAUSED prefix).
        if _last_live["name"]:
            _sync_row_emit(stream,
                           _last_live["i"], _last_live["total"],
                           _last_live["name"])

    # QUEUE-DRIVEN LOOP (ports YTArchiver.py:19130-19144 exactly).
    # Pop from queues.sync until it's empty. No config iteration —
    # that's the root-cause bug hit where a resumed half-pass
    # would restart from A because the loop walked config instead of
    # the queue. `_processed` counts what we've done in THIS invocation;
    # `total` stays stable at the INITIAL queue size to keep the row
    # denominator steady across pauses (bug L-2: old code recomputed
    # per iteration, so pausing+resuming made [3/7] drift to [3/4] as
    # remaining items drained — confusing.)
    _processed = 0
    try:
        _initial_total = sum(
            1 for c in queues.sync
            if (c.get("kind") or "download").lower() != "redownload"
        ) if queues is not None else 0
    except Exception:
        _initial_total = 0
    while True:
        # Pop next channel off the queue. When the queue is empty, we're
        # done — this is how the loop terminates, naturally supporting
        # both fresh passes (queue was fully enqueued above) and resume
        # (queue was restored from disk with a subset).
        ch = None
        if queues is not None:
            try:
                ch = queues.sync_pop()
            except Exception:
                ch = None
        if ch is None:
            break
        # Skip redownload tasks — they live in queues.sync only for
        # Sync Tasks popover visibility; Api._redwnl_worker drains a
        # separate `_redwnl_pending` list to actually run them (with
        # the right resolution, sample-confirm bridge, etc). Falling
        # through here would mis-process them as regular sync
        # downloads. reported symptom: "=== Sync pass starting (N
        # channels) === [1/N] ChannelName — no new videos" appearing
        # while a redownload of that channel was correctly running
        # in the popover — because both workers popped the same task.
        if (ch.get("kind") or "").lower() == "redownload":
            # Don't count this pop against `_processed` — the user
            # didn't ask sync_all to do anything with it, so the
            # "1/total" display should stay accurate to real syncs.
            continue
        _processed += 1
        i = _processed
        # bug L-2: use the INITIAL total captured above rather than
        # recomputing from `remaining + processed` each pass. The
        # denominator stays stable across pauses/resumes. Fall back to
        # the dynamic calc when initial was 0 (rare) so we still
        # display something sensible.
        try:
            _remaining = sum(
                1 for c in queues.sync
                if (c.get("kind") or "download").lower() != "redownload"
            ) if queues is not None else 0
        except Exception:
            _remaining = 0
        total = max(_initial_total, _processed + _remaining) \
                if _initial_total else (_processed + _remaining)
        # Honor pause request before we start this channel — if the user
        # paused mid-pass, we park here and re-paint the last-live row
        # as PAUSED. Matches OLD's pause-at-top-of-channel behavior.
        _wait_if_paused()
        if cancel_event is not None and cancel_event.is_set():
            # If this was a skip-rather-than-cancel, keep going.
            if skip_event is not None and skip_event.is_set():
                cancel_event.clear()
                skip_event.clear()
                skipped += 1
                _sync_row_emit(stream, i, total, ch.get("name", "?"),
                               summary="skipped",
                               name_tag="dim", summary_tag="dim")
                continue
            stream.emit([["\n\u26d4 Pass cancelled.\n", "red"]])
            break
        # Batch cooldown check — skip channels still cooling down from a
        # bootstrap batch (>100k videos, not yet init_complete).
        can_proceed, cooldown_label = _check_batch_cooldown(ch)
        if not can_proceed:
            skipped += 1
            _sync_row_emit(stream, i, total, ch.get("name", "?"),
                           summary=f"cooldown until {cooldown_label}",
                           name_tag="dim", summary_tag="dim")
            continue
        # Emit the "live" row for this channel (header only, no summary).
        # sync_channel does its work; afterwards we emit the "done" row
        # with the same sync_row_<i> marker so it replaces the header in
        # place, giving the user a single consolidated line per channel.
        ch_name = ch.get("name", "?")
        _last_live.update({"i": i, "total": total, "name": ch_name})
        _sync_row_emit(stream, i, total, ch_name)
        # If user hit Pause between the cooldown check and now, honor it
        # before kicking off yt-dlp.
        _wait_if_paused()

        # Kind dispatch. Download items (the default / no `kind` key)
        # take the full sync_channel path; metadata items take the
        # fetch_channel_metadata path. This is how metadata recheck
        # tasks become first-class queue citizens — visible in the
        # Sync Tasks popover, pausable, and cancellable via the same
        # controls as downloads. rule: "every channel's
        # metadata check should show as its own sync task."
        _ch_kind = (ch.get("kind") or "download").lower()
        _task_t0 = time.time()  # per-task timer for [Mtadta] activity row
        # Set current_sync for non-download kinds too. Without this,
        # the popover head row is empty during metadata / comments /
        # backfill passes, which breaks the "Pause is taking effect"
        # blink (paintBlinkState requires a running head row to compute
        # `sync_running=true`). Cleared at the end of the iteration
        # below so the next channel doesn't inherit a stale row.
        if _ch_kind in ("metadata", "metadata_comments", "videoid_backfill"):
            if queues is not None:
                try: queues.set_current_sync(ch)
                except Exception: pass
        if _ch_kind == "metadata":
            try:
                from . import metadata as _meta
                # feature H-14: year-scoped metadata tasks carry a
                # `scope: {"year": N}` set by
                # Api.metadata_queue_channel_year so the backend filters
                # on-disk videos to that year before processing. None
                # for whole-channel tasks (existing behavior unchanged).
                _res = _meta.fetch_channel_metadata(
                    ch, stream, cancel_event,
                    refresh=bool(ch.get("refresh")),
                    pause_event=pause_event,
                    scope=ch.get("scope"),
                    queues=queues)
                # Detect pause-interrupted metadata walk — same
                # re-enqueue-at-front treatment as downloads.
                if (pause_event is not None and pause_event.is_set()
                        and queues is not None):
                    try:
                        queues.sync.insert(0, ch)
                        queues._notify()
                    except Exception:
                        pass
                    _sync_row_emit(stream, i, total, ch_name,
                                   summary="paused",
                                   name_tag="simpleline",
                                   summary_tag="simpleline")
                    _last_live["name"] = ""
                    _processed -= 1
                    continue
                _fetched = int(_res.get("fetched", 0) or 0)
                _refreshed = int(_res.get("refreshed", 0) or 0)
                _errors_meta = int(_res.get("errors", 0) or 0)
                # Roll into pass-wide accumulators so Pass complete
                # reports accurate numbers per kind.
                sum_meta_fetched += _fetched
                sum_meta_refreshed += _refreshed
                sum_err += _errors_meta
                # issue #136 + bug H-4: when the user runs "Refresh
                # views/likes" (refresh=True), every on-disk video is
                # re-hit and counts roll into `refreshed`, not
                # `fetched`. The old summary ignored `refreshed` and
                # errors entirely, so the task row said "up to date"
                # even on a refresh pass with partial failures — hid
                # real problems.
                _parts: List[str] = []
                if _fetched:
                    _parts.append(f"{_fetched} new")
                if _refreshed:
                    _parts.append(f"{_refreshed} refreshed")
                if _errors_meta:
                    _parts.append(f"{_errors_meta} errors")
                if not _parts:
                    _parts.append("up to date")
                _summary = " \u00b7 ".join(_parts)
                _summary_tag = ("red" if _errors_meta else
                                "simpleline_pink" if (_fetched or _refreshed)
                                else "dim")
                _sync_row_emit(stream, i, total, ch_name,
                               summary=_summary,
                               name_tag="simpleline",
                               summary_tag=_summary_tag)
                # Activity-log row — mirrors the [Dwnld] row pattern.
                # Primary verb reflects what the pass primarily did
                # (refreshed counts vs. first-time metadata fetch);
                # secondary carries the less-common complement.
                if _refreshed > 0 and _fetched > 0:
                    _a_primary = f"{_refreshed} refreshed"
                    _a_secondary = f"{_fetched} new"
                elif _refreshed > 0:
                    _a_primary = f"{_refreshed} refreshed"
                    _a_secondary = ""
                elif _fetched > 0:
                    _a_primary = f"{_fetched} fetched"
                    _a_secondary = ""
                else:
                    _a_primary = "up to date"
                    _a_secondary = ""
                emit_metadata_activity_row(
                    stream, ch_name,
                    primary=_a_primary, secondary=_a_secondary,
                    errors=_errors_meta,
                    elapsed=time.time() - _task_t0,
                    green=(_errors_meta == 0))
            except Exception as _me:
                stream.emit_error(f"Metadata failed for {ch_name}: {_me}")
                _sync_row_emit(stream, i, total, ch_name,
                               summary="failed",
                               name_tag="dim", summary_tag="red")
                emit_metadata_activity_row(
                    stream, ch_name,
                    primary="failed", secondary="",
                    errors=1, elapsed=time.time() - _task_t0,
                    green=False)
            _last_live["name"] = ""
            # Push Settings > Metadata tab refresh — last_views_refresh_ts
            # may have just been stamped on the channel config.
            if _on_metadata_changed_hook is not None:
                try: _on_metadata_changed_hook()
                except Exception: pass
            continue
        # Comments-only refresh task. Separate from `metadata` because
        # comments can only be fetched per-video (no bulk mode) and
        # users wanted them as a distinct user-triggered action — NOT
        # bundled into "Refresh views/likes". Task dict may carry
        # `only_recent_days` to scope to the most-recent uploads.
        if _ch_kind == "metadata_comments":
            try:
                from . import metadata as _meta
                _res = _meta.refresh_channel_comments(
                    ch, stream, cancel_event=cancel_event,
                    pause_event=pause_event,
                    only_recent_days=ch.get("only_recent_days"),
                    queues=queues)
                # Honor pause the same way the metadata branch does.
                if (pause_event is not None and pause_event.is_set()
                        and queues is not None):
                    try:
                        queues.sync.insert(0, ch)
                        queues._notify()
                    except Exception:
                        pass
                    _sync_row_emit(stream, i, total, ch_name,
                                   summary="paused",
                                   name_tag="simpleline",
                                   summary_tag="simpleline")
                    _last_live["name"] = ""
                    _processed -= 1
                    continue
                _fetched = int(_res.get("fetched", 0) or 0)
                _errors_c = int(_res.get("errors", 0) or 0)
                sum_comments_refreshed += _fetched
                sum_err += _errors_c
                _parts = []
                if _fetched:
                    _parts.append(f"{_fetched} comments refreshed")
                if _errors_c:
                    _parts.append(f"{_errors_c} errors")
                if not _parts:
                    _parts.append("no videos in scope")
                _summary = " \u00b7 ".join(_parts)
                _summary_tag = ("red" if _errors_c else
                                "simpleline_pink" if _fetched else "dim")
                _sync_row_emit(stream, i, total, ch_name,
                               summary=_summary,
                               name_tag="simpleline",
                               summary_tag=_summary_tag)
                _a_primary = (f"{_fetched} comments refreshed"
                              if _fetched else "no videos in scope")
                emit_metadata_activity_row(
                    stream, ch_name,
                    primary=_a_primary, secondary="",
                    errors=_errors_c, elapsed=time.time() - _task_t0,
                    green=(_errors_c == 0))
            except Exception as _ce:
                stream.emit_error(
                    f"Comments refresh failed for {ch_name}: {_ce}")
                _sync_row_emit(stream, i, total, ch_name,
                               summary="failed",
                               name_tag="dim", summary_tag="red")
                emit_metadata_activity_row(
                    stream, ch_name,
                    primary="failed", secondary="",
                    errors=1, elapsed=time.time() - _task_t0,
                    green=False)
            _last_live["name"] = ""
            # Push Metadata-tab refresh (last_comments_refresh_ts may
            # have just been stamped).
            if _on_metadata_changed_hook is not None:
                try: _on_metadata_changed_hook()
                except Exception: pass
            continue
        # Video-id backfill task. One-shot resolution + DB write for
        # archives migrated from the tkinter-era YTArchiver that have
        # no [id] bracket in filenames and no .info.json sidecars —
        # without this, the bulk views/likes refresh path can't match
        # ANY on-disk file to its YouTube row. Separate from the
        # views/likes kind because it's deliberately a fast, cheap
        # prerequisite pass, not an actual metadata refresh.
        if _ch_kind == "videoid_backfill":
            try:
                from . import metadata as _meta
                _res = _meta.backfill_video_ids(
                    ch, stream, cancel_event=cancel_event,
                    pause_event=pause_event,
                    queues=queues)
                if (pause_event is not None and pause_event.is_set()
                        and queues is not None):
                    try:
                        queues.sync.insert(0, ch)
                        queues._notify()
                    except Exception:
                        pass
                    _sync_row_emit(stream, i, total, ch_name,
                                   summary="paused",
                                   name_tag="simpleline",
                                   summary_tag="simpleline")
                    _last_live["name"] = ""
                    _processed -= 1
                    continue
                _resolved = int(_res.get("resolved", 0) or 0)
                _unresolved = int(_res.get("unresolved", 0) or 0)
                _already = int(_res.get("already_set", 0) or 0)
                sum_ids_backfilled += _resolved
                _parts = []
                if _resolved:
                    _parts.append(f"{_resolved} backfilled")
                if _already:
                    _parts.append(f"{_already} already had ID")
                if _unresolved:
                    _parts.append(f"{_unresolved} unresolved")
                if not _parts:
                    _parts.append(_res.get("error") or "nothing to do")
                _summary = " \u00b7 ".join(_parts)
                _summary_tag = ("simpleline_pink" if _resolved
                                else "dim" if not _res.get("error")
                                else "red")
                _sync_row_emit(stream, i, total, ch_name,
                               summary=_summary,
                               name_tag="simpleline",
                               summary_tag=_summary_tag)
                _a_primary = (f"{_resolved} IDs backfilled" if _resolved
                              else "no IDs to backfill")
                _a_secondary = (f"{_unresolved} unresolved"
                                if _unresolved else "")
                _a_err = 1 if _res.get("error") else 0
                emit_metadata_activity_row(
                    stream, ch_name,
                    primary=_a_primary, secondary=_a_secondary,
                    errors=_a_err, elapsed=time.time() - _task_t0,
                    green=(_a_err == 0))
            except Exception as _be:
                stream.emit_error(
                    f"ID backfill failed for {ch_name}: {_be}")
                _sync_row_emit(stream, i, total, ch_name,
                               summary="failed",
                               name_tag="dim", summary_tag="red")
                emit_metadata_activity_row(
                    stream, ch_name,
                    primary="failed", secondary="",
                    errors=1, elapsed=time.time() - _task_t0,
                    green=False)
            _last_live["name"] = ""
            # Push Metadata-tab refresh — the Video IDs column status
            # just changed (resolved count went up, missing went down).
            if _on_metadata_changed_hook is not None:
                try: _on_metadata_changed_hook()
                except Exception: pass
            continue
        # ── Quick-check fast path ────────────────────────────────────
        # Extra speedup on top of `--break-on-existing`: probe the first
        # 5 video IDs via `--flat-playlist --lazy-playlist --playlist-end
        # 5` and check them against the download archive. If all 5 are
        # already archived, skip the full yt-dlp run entirely. For a
        # 1000+ video channel, this saves the API pagination cost
        # (~45 pages × 0.25s sleep = ~11s) on top of what break-on-
        # existing already saves.
        #
        # Gating mirrors YTArchiver.py:22984 exactly:
        # init_complete AND sync_complete AND mode == "full"
        # The fast-path exists BECAUSE full-mode channels can't rely on
        # `--break-on-existing` doing the work alone after a bootstrap
        # (in case a mid-channel video is missing and needs backfill).
        # For sub/date modes, the main break-on-existing path is already
        # fast enough — no need for the probe.
        _ch_url = (ch.get("url") or "").strip()
        _ch_mode = (ch.get("mode") or "full").lower()
        _ch_is_init = bool(ch.get("initialized", False))
        _ch_sync_ok = bool(ch.get("sync_complete", True))
        if ch.get("init_complete", False):
            _ch_sync_ok = True
        _fast_path_eligible = (
            ch.get("init_complete", False) and
            _ch_sync_ok and
            _ch_mode == "full"
        )
        if _known_ids and _ch_url and _fast_path_eligible:
            _qc = quick_check_new_uploads(
                _ch_url, _known_ids, check_count=5, timeout_sec=30)
            if _qc.get("ok") and not _qc.get("has_new"):
                _sync_row_emit(stream, i, total, ch_name,
                               summary="no new videos",
                               name_tag="simpleline", summary_tag="dim")
                _last_live["name"] = ""
                continue
        res = sync_channel(ch, stream, cancel_event,
                           queues=queues, transcribe_mgr=transcribe_mgr,
                           pause_event=pause_event,
                           pass_idx=i, pass_total=total)
        _dl = int(res.get("downloaded", 0) or 0)
        _err = int(res.get("errors", 0) or 0)
        sum_dl += _dl
        sum_err += _err
        # Detect "paused mid-download": pause_event set and the
        # readline loop bailed out. Put this channel back at the
        # FRONT of the queue so Resume continues it instead of
        # silently skipping. yt-dlp's `--continue` + download-archive
        # picks up where it left off, so no data is lost.
        if (pause_event is not None and pause_event.is_set()
                and queues is not None):
            try:
                queues.sync.insert(0, ch)
                queues._notify()
            except Exception:
                pass
            _sync_row_emit(stream, i, total, ch_name,
                           summary="paused",
                           name_tag="simpleline", summary_tag="simpleline")
            _last_live["name"] = ""
            _processed -= 1 # undo the count — will retry this one
            # Loop will hit _wait_if_paused() on next iter and block.
            continue
        # Replace the live row with a compact summary.
        _sync_row_emit(stream, i, total, ch_name,
                       summary=_short_summary(_dl, _err),
                       name_tag="simpleline_green" if _dl > 0 else "simpleline",
                       summary_tag="simpleline_green" if _dl > 0 else "dim")
        # Clear the "live" marker so a pause between channels doesn't
        # re-paint this row (which is now DONE with a summary).
        _last_live["name"] = ""
        # If this was a batch-limited bootstrap run, apply the next cooldown.
        # We only set cooldown when the channel hadn't finished initializing
        # and this pass hit the BATCH_LIMIT threshold.
        if _should_batch_limit(ch, res.get("total", 0)):
            set_batch_cooldown(ch.get("url", ""))

    elapsed = time.time() - t_start
    # Per-kind summary: the action verb on the Pass complete line
    # now reflects what the pass actually did. Previously it always
    # said "N downloaded" even for a views/likes refresh where no
    # download happened — confusing ("why does it say 0 downloaded
    # when I just refreshed 912 videos?").
    # Pass complete line styling:
    #   === brackets                 → green+bold (simplestatus_green)
    #   "Pass complete:"             → white+bold (simplestatus_white)
    #   action verb (N refreshed…)   → green+bold for non-zero,
    #                                  white+bold for zero (still
    #                                  readable, just less celebratory)
    #   separators, errors when 0,
    #   skipped, "took Ns"           → bright white (simpleline)
    #   errors when > 0              → red
    # Prior build used `header` (muted #a0aabb) for body parts, which
    # users reported as "near unreadable" on their displays. Switched
    # the body to `simpleline` (bright --c-text) + `simplestatus_white`
    # for the primary label.
    emit_parts: List[List[str]] = [
        ["=== ", "simplestatus_green"],
        ["Pass complete: ", "simplestatus_white"],
    ]
    _verb_chunks: List[Tuple[str, str]] = []  # (text, tag) pairs for action verbs
    if sum_dl > 0:
        _verb_chunks.append((f"{sum_dl} downloaded", "simplestatus_green"))
    if sum_meta_refreshed > 0:
        _verb_chunks.append((f"{sum_meta_refreshed} refreshed", "simplestatus_green"))
    if sum_meta_fetched > 0:
        _verb_chunks.append((f"{sum_meta_fetched} metadata fetched", "simplestatus_green"))
    if sum_comments_refreshed > 0:
        _verb_chunks.append((f"{sum_comments_refreshed} comments refreshed", "simplestatus_green"))
    if sum_ids_backfilled > 0:
        _verb_chunks.append((f"{sum_ids_backfilled} IDs backfilled", "simplestatus_green"))
    if not _verb_chunks:
        # All counters zero. Pick a sensible 0-verb matching the
        # pass label so the user sees "something ran" rather than a
        # blank summary. White+bold for zero-count (still fully
        # readable, just not the celebratory green).
        if _label == "Views/likes refresh":
            _verb_chunks.append(("0 refreshed", "simplestatus_white"))
        elif _label == "Comments refresh":
            _verb_chunks.append(("0 comments refreshed", "simplestatus_white"))
        elif _label == "Video ID backfill":
            _verb_chunks.append(("0 IDs backfilled", "simplestatus_white"))
        elif _label == "Metadata download":
            _verb_chunks.append(("0 metadata fetched", "simplestatus_white"))
        else:
            _verb_chunks.append(("0 downloaded", "simplestatus_white"))
    # Interleave with separators.
    for _i, (_txt, _tag) in enumerate(_verb_chunks):
        if _i > 0:
            emit_parts.append([" \u00b7 ", "simpleline"])
        emit_parts.append([_txt, _tag])
    # Errors: red if non-zero, white if 0.
    emit_parts.append([" \u00b7 ", "simpleline"])
    emit_parts.append([f"{sum_err} errors",
                       "red" if sum_err > 0 else "simpleline"])
    if skipped:
        emit_parts.append([" \u00b7 ", "simpleline"])
        emit_parts.append([f"{skipped} skipped", "simpleline"])
    emit_parts.append([" \u00b7 ", "simpleline"])
    emit_parts.append([f"took {_fmt_duration(elapsed)} ", "simpleline"])
    emit_parts.append(["===\n", "simplestatus_green"])
    stream.emit(emit_parts)
    # Clean up: drop any remaining queued items (pass may have broken
    # out early on cancel), clear the running-slot, and clear the
    # pass-progress decoration. The popover should return to empty
    # state once this runs.
    if queues is not None:
        try: queues.sync_clear()
        except Exception: pass
        try: queues.set_current_sync(None)
        except Exception: pass
        try: queues.set_sync_pass_progress(0, 0)
        except Exception: pass
    # TuneShine: clear the sync-progress file so the display goes idle.
    try: clear_sync_progress()
    except Exception: pass
    # Clear the thread-local pass_id so stray `_sync_row_emit` calls
    # after this function returns don't tag rows with a dead pass.
    try: _ROW_EMIT_PASS_ID.id = ""
    except Exception: pass
    return {"ok": True, "downloaded": sum_dl, "errors": sum_err,
            "skipped": skipped,
            "took": _fmt_duration(elapsed), "total": total}
