"""Repair YT auto-caption per-word arrays for already-archived videos.

A parsing bug in `_parse_vtt` (fixed v64.7) caused two visible artifacts in
the Watch-view transcript for any video transcribed via YT auto-captions
before the fix:

  1. Untagged "rollover" words at the start of every continuation cue
     (e.g. `New` in `New<00:00:02.879><c> Jersey</c>`) were silently
     dropped. Visible as missing words: `New`, `>> Convertible?`,
     `for`, `aggressive,`, `weeks`, etc.

  2. A 0.5s slop in the Step-3 word window pulled the next segment's
     first 1-3 words into the prior segment's array. Visible as
     duplicated phrases at segment boundaries (e.g. `heading to heading
     to a data center`).

The aggregated Transcript.txt is unaffected — the bug was only in the
per-word `words` payload. This module re-fetches each YT-captioned
video's VTT, runs the (now fixed) `_parse_vtt`, and rewrites:
  - the hidden `.{name} Transcript.jsonl` (via `_replace_jsonl_entry`)
  - the `segments.words` column in `transcription_index.db` (targeted
    UPDATE on (video_id, start_time, end_time) — leaves FTS / row IDs /
    bookmarks intact)

Whisper-transcribed videos are skipped (detected via the `(SOURCE)` tag
in the Transcript.txt section header).
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path

from .log import get_logger
from .sync import _find_cookie_source, _startupinfo, find_yt_dlp
from .transcribe import _parse_vtt, _replace_jsonl_entry
from .transcribe.transcribe_files import parse_transcript_header
from .ytarchiver_config import TRANSCRIPTION_DB

_log = get_logger(__name__)

# Suppress Windows console windows on every yt-dlp subprocess call —
# without this each fetch flashes a black console window that steals
# focus from the user's foreground app.
_CREATE_NO_WINDOW = 0x08000000 if os.name == "nt" else 0

# YouTube rate-limit knobs.
# Every per-video fetch hits YT's caption endpoint. A naive serial loop
# at ~5s/video (network-limited) gives ~0.2 requests/second, which YT
# was empirically still 429-ing after ~40 videos. The base sleep below
# spaces fetches out further; the backoff schedule kicks in only when
# YT actually returns 429 so we don't pay the cost on the happy path.
_RATE_LIMIT_SLEEP_SEC = 1.0
_RATE_LIMIT_BACKOFFS = (30, 90, 300)  # retries 1, 2, 3 on 429
_RATE_LIMIT_RE = re.compile(
    r"\b(429|Too Many Requests|rate[ -]?limited)\b", re.IGNORECASE)


# ── Progress persistence (cross-restart resume) ────────────────────────
# A repair pass over 86k videos is a multi-day operation, so we have to
# survive app restarts. After every video we append the video_id to a
# per-scope text file under %APPDATA%\YTArchiver\repair_progress\. On
# the next start of the same scope we load it as a set and skip any
# already-processed ids. Append-only means we never write more than a
# single short line per video — none of the "save the whole set every
# N videos" thrashing.
# Scope = the task's `url` field ("repair:all" / "repair:channel:Vox" /
# "repair:video:abc"), so partial runs of different scopes coexist
# without collision.

def _progress_dir() -> Path:
    """%APPDATA%\\YTArchiver\\repair_progress\\ — lives next to the
    transcription index DB."""
    base = Path(TRANSCRIPTION_DB).parent
    d = base / "repair_progress"
    try:
        d.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    return d


def _scope_slug(scope_url: str) -> str:
    """File-safe slug for the per-scope progress file. Lowercased,
    non-alnum collapsed to underscores, capped at 80 chars."""
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", scope_url or "unknown")
    return safe[:80].strip("_") or "unknown"


def _progress_path(scope_url: str) -> Path:
    return _progress_dir() / f"{_scope_slug(scope_url)}.txt"


def _load_progress(scope_url: str) -> set:
    """Return the set of video_ids already processed in prior runs of
    this scope. Empty set on no-file / read-failure."""
    f = _progress_path(scope_url)
    if not f.exists():
        return set()
    try:
        with open(f, "r", encoding="utf-8") as fh:
            return {line.strip() for line in fh if line.strip()}
    except OSError:
        return set()


def _append_progress(scope_url: str, video_id: str) -> None:
    """Mark this video as done for the given scope (append one line)."""
    f = _progress_path(scope_url)
    try:
        with open(f, "a", encoding="utf-8") as fh:
            fh.write(video_id + "\n")
    except OSError:
        pass  # non-fatal: we'll just retry the video on resume


def _clear_progress(scope_url: str) -> None:
    """Remove the progress file — used after a clean full-completion."""
    f = _progress_path(scope_url)
    try:
        f.unlink()
    except OSError:
        pass


# ── Work-list checkpoint (skip the scan on resume) ─────────────────────
# The progress file (above) only records which video_ids were processed.
# To resume cleanly we still had to re-scan the entire archive on every
# restart, because the work list itself lived only in memory. For a
# 100-channel archive that's ~70 seconds of scan time the user has to
# sit through every time they relaunch the app. The checkpoint stores
# the post-scan work list so the next start can skip directly to the
# per-video loop.

def _checkpoint_path(scope_url: str) -> Path:
    return _progress_dir() / f"{_scope_slug(scope_url)}.work.json"


def _save_checkpoint(scope_url: str, work: list) -> None:
    """Persist the scanned work list so a future resume can skip the
    scan. `work` is a list of (jsonl_path, video_id, title, src_tag)
    tuples; we serialize as plain lists for JSON compatibility."""
    if not scope_url or not work:
        return
    f = _checkpoint_path(scope_url)
    payload = {
        "scope_url": scope_url,
        "scanned_at": datetime.now().isoformat(),
        "work": [[str(jp), vid, t, tag] for (jp, vid, t, tag) in work],
    }
    tmp = str(f) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
        os.replace(tmp, f)
    except OSError:
        try: os.remove(tmp)
        except OSError: pass


def _load_checkpoint(scope_url: str) -> list | None:
    """Return the cached work list as a list of tuples, or None if no
    checkpoint exists / the file is unreadable."""
    if not scope_url:
        return None
    f = _checkpoint_path(scope_url)
    if not f.exists():
        return None
    try:
        with open(f, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError, ValueError):
        return None
    out = []
    dropped_missing = 0
    for row in data.get("work", []):
        if isinstance(row, list) and len(row) >= 4:
            jp = Path(row[0])
            # Drop entries whose jsonl_path no longer exists — typically
            # happens after a drive remap (Z:\ → Y:\) or after the user
            # moved their archive. Without this filter, every video in
            # the checkpoint would fail-fetch against a nonexistent
            # path and produce a massive failure rate on resume.
            if not jp.exists():
                dropped_missing += 1
                continue
            out.append((jp, row[1], row[2], row[3]))
    if dropped_missing:
        _log.warning(
            "repair checkpoint: dropped %d entries whose jsonl_path no longer exists",
            dropped_missing)
    return out or None


def _clear_checkpoint(scope_url: str) -> None:
    if not scope_url:
        return
    try:
        _checkpoint_path(scope_url).unlink()
    except OSError:
        pass


# Source-tag values written into Transcript.txt section headers by
# transcribe.py. Whisper rows look like `(WHISPER:small)` and are skipped.
YT_CAPTION_TAGS = {"YT CAPTIONS", "YT+PUNCTUATION"}

# consolidated onto text_utils.normalize_title_loose
# (NFKC + lower + strip ALL punct + collapse whitespace). Previous local
# copy was the same minus NFKC normalization.
from .text_utils import normalize_title_loose as _norm_title


def _parse_txt_sources(txt_path: Path) -> dict:
    """Return `{normalized_title: source_tag}` from a Transcript.txt file."""
    out: dict = {}
    try:
        with open(txt_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                parsed = parse_transcript_header(line)
                if parsed:
                    title, _date, _dur, src = parsed
                    out[_norm_title(title)] = src
    except OSError:
        pass
    return out


def _find_txt_for_jsonl(jsonl_path: Path) -> Path:
    """`.Foo Transcript.jsonl` -> `Foo Transcript.txt` (drops leading dot)."""
    name = jsonl_path.name
    if name.startswith("."):
        name = name[1:]
    return jsonl_path.parent / (name[: -len(".jsonl")] + ".txt")


def _fetch_vtt(yt_dlp: str, video_id: str,
               out_dir: Path,
               cancel_event: threading.Event | None = None
               ) -> tuple[Path | None, str | None]:
    """Run yt-dlp to fetch a single video's auto-caption VTT.

    Passes the same `--cookies-from-browser` (or `--cookies cookies.txt`)
    args every other YTArchiver yt-dlp call uses. Authenticated requests
    get higher YT quotas (fewer 429s) and unlock age-gated content that
    would otherwise come back as "Sign in to confirm your age".

    `cancel_event` is polled while the subprocess runs so a Cancel
    during a single hung fetch can break out promptly instead of
    waiting for the full 120s timeout (previously the only way the
    fetch could end early).
    """
    cmd = [
        yt_dlp,
        *_find_cookie_source(),
        "--skip-download", "--write-auto-subs",
        "--sub-lang", "en", "--sub-format", "vtt",
        "--no-warnings",
        "-o", str(out_dir / f"{video_id}.%(ext)s"),
        f"https://www.youtube.com/watch?v={video_id}",
    ]
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True,
            startupinfo=_startupinfo, creationflags=_CREATE_NO_WINDOW,
        )
    except FileNotFoundError:
        return None, f"yt-dlp not found: {yt_dlp}"
    # Poll loop — checks cancel_event every 250ms.
    deadline = time.time() + 120.0
    while True:
        if cancel_event is not None and cancel_event.is_set():
            try:
                proc.terminate()
                proc.wait(timeout=3)
            except Exception:
                try: proc.kill()
                except Exception: pass
            return None, "cancelled"
        try:
            r_code = proc.wait(timeout=0.25)
            break
        except subprocess.TimeoutExpired:
            if time.time() >= deadline:
                try: proc.kill()
                except Exception: pass
                return None, "yt-dlp timeout"
    try:
        _stderr_text = proc.stderr.read() if proc.stderr else ""
        _stdout_text = proc.stdout.read() if proc.stdout else ""
    except Exception:
        _stderr_text = ""
        _stdout_text = ""
    if r_code != 0:
        msg = (_stderr_text or _stdout_text or "").splitlines()
        tail = msg[-1] if msg else "yt-dlp failed"
        return None, tail[:200]
    for cand in out_dir.glob(f"{video_id}*.vtt"):
        return cand, None
    return None, "no captions available"


def _open_repair_db_conn() -> sqlite3.Connection | None:
    """Open one shared sqlite3 connection for the lifetime of a repair
    pass. WAL mode is set explicitly here so a cold-start invocation
    (e.g. CLI) doesn't leave the DB in default rollback-journal mode,
    which would block every reader during each UPDATE. Returns None if
    the DB file is missing or open fails."""
    if not Path(TRANSCRIPTION_DB).exists():
        return None
    try:
        # check_same_thread=False mirrors what index._open() uses, so
        # this conn behaves consistently if a future caller passes it
        # across threads (audit: repair_captions.py:300).
        conn = sqlite3.connect(str(TRANSCRIPTION_DB), timeout=30.0,
                               check_same_thread=False)
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.Error as e:
            _log.debug("swallowed: %s", e)
        return conn
    except sqlite3.Error as e:
        _log.error("repair conn open failed: %s", e)
        return None


def _update_db_words(video_id: str,
                     new_segments: list,
                     conn: sqlite3.Connection | None = None
                     ) -> tuple[int, str | None]:
    """Targeted UPDATE on the segments table — column-only, FTS untouched.

    Match on (video_id, start_time, end_time). The parser fix doesn't
    change segmentation or timing, only the `w` array contents, so
    every row matches exactly.

    If `conn` is provided, reuse it (caller is responsible for close).
    Otherwise open + close per call (slow path; preserved for back-compat
    but not used by repair_archive's main loop anymore).
    """
    owns_conn = False
    if conn is None:
        conn = _open_repair_db_conn()
        if conn is None:
            return 0, f"DB not found at {TRANSCRIPTION_DB}"
        owns_conn = True
    try:
        updated = 0
        for seg in new_segments:
            s_val = seg.get("start") if "start" in seg else seg.get("s", 0)
            e_val = seg.get("end") if "end" in seg else seg.get("e", 0)
            w_val = seg.get("words") if "words" in seg else seg.get("w", [])
            if not isinstance(w_val, list):
                w_val = []
            # Float-equality match is unreliable because re-parsing the
            # same VTT can produce tiny FP drift (e.g. 2.879 vs
            # 2.8790000000000002). Use a small tolerance window so the
            # UPDATE actually lands. The 0.01s window is much smaller
            # than any real Whisper segment (~3-10s).
            cur = conn.execute(
                "UPDATE segments SET words=? "
                "WHERE video_id=? "
                "AND ABS(start_time - ?) < 0.01 "
                "AND ABS(end_time - ?) < 0.01",
                (json.dumps(w_val, ensure_ascii=False), video_id,
                 float(s_val or 0), float(e_val or 0)))
            updated += cur.rowcount
        # Commit ONCE per video instead of per-segment — per-segment
        # commits across an 80k-video corpus produced up to 4M commits
        # and dominated wall time. A single commit per video preserves
        # the prior "one bad row doesn't wipe siblings" property
        # because each video gets its own atomic transaction (audit:
        # repair_captions H57).
        conn.commit()
        return updated, None
    except sqlite3.Error as e:
        try: conn.rollback()
        except Exception as _re: _log.debug("swallowed: %s", _re)
        return 0, f"DB update: {e}"
    finally:
        if owns_conn:
            conn.close()


_PUNCT_CHARS_RE = re.compile(r"[.,!?;:]")
_CAP_WORD_RE = re.compile(r"\b[A-Z][a-z]+")


def _looks_punctuated(segments: list, sample: int = 8) -> bool:
    """Heuristic: does this parsed transcript look like the modern punctuated
    YT auto-cap output, or the older all-lowercase raw form?

    Sentence-ending punctuation OR a multi-letter capitalized word in the
    first N segments => punctuated. The legacy raw format has neither.

    Sample from BOTH ends + the middle so a video with a sparse intro
    (music, "[music]", single-word reactions) isn't falsely classified
    as unpunctuated and downgraded. The old version only looked at the
    first N segments, so any punctuated video with a music-only intro
    failed the heuristic (audit: repair_captions H71).
    """
    if not segments:
        return False
    _n = len(segments)
    _samples: list = []
    _samples.extend(segments[:sample])
    if _n > sample:
        # Middle slice
        _mid = max(sample, (_n - sample) // 2)
        _samples.extend(segments[_mid:_mid + sample])
    if _n > sample * 2:
        # Last slice
        _samples.extend(segments[-sample:])
    text = " ".join((s.get("t") or s.get("text") or "") for s in _samples)
    if not text.strip():
        return False
    if _PUNCT_CHARS_RE.search(text):
        return True
    if _CAP_WORD_RE.search(text):
        return True
    return False


def _fetch_vtt_with_backoff(yt_dlp: str, video_id: str, out_dir: Path,
                            log_stream, cancel_event: threading.Event | None
                            ) -> tuple[Path | None, str | None]:
    """Wrap `_fetch_vtt` with 429 detection + exponential backoff.

    On YT rate-limit (HTTP 429), sleep through the next backoff window
    then retry the same video. The schedule (30s, 90s, 5min) gives YT
    time to release the throttle; if all three retries still 429 we
    give up on that video and let the caller log it as FAIL.
    """
    vtt, err = _fetch_vtt(yt_dlp, video_id, out_dir, cancel_event=cancel_event)
    if vtt or not err or not _RATE_LIMIT_RE.search(err):
        return vtt, err
    for attempt, backoff in enumerate(_RATE_LIMIT_BACKOFFS, 1):
        log_stream.emit_text(
            f"   YT rate limit (429) hit — backing off {backoff}s "
            f"before retry {attempt}/{len(_RATE_LIMIT_BACKOFFS)}\n",
            "simpleline")
        log_stream.flush()
        # cancel-aware sleep
        if cancel_event is not None:
            if cancel_event.wait(timeout=backoff):
                return None, "cancelled during backoff"
        else:
            time.sleep(backoff)
        vtt, err = _fetch_vtt(yt_dlp, video_id, out_dir, cancel_event=cancel_event)
        if vtt:
            return vtt, None
        if not err or not _RATE_LIMIT_RE.search(err):
            return None, err  # different error, don't keep retrying
    return None, "rate-limited (all retries exhausted)"


def _repair_one_video(yt_dlp: str, jsonl_path: Path, title: str,
                      video_id: str, source_tag: str, dry_run: bool,
                      log_stream,
                      cancel_event: threading.Event | None = None,
                      db_conn: sqlite3.Connection | None = None,
                      tmp_dir: Path | None = None,
                      ) -> tuple[bool, str, int, int]:
    """Fetch+parse+write a single video. Returns (ok, msg, n_segs, n_words).

    Downgrade guard: if the original was YT+PUNCTUATION (punctuation
    was restored at ingest because the source VTT was lowercase) and
    YouTube's CURRENT VTT is still lowercase, repairing would visibly
    strip the capitalization + punctuation across the whole transcript.
    Skip those videos rather than downgrade them.
    """
    def _run_repair(_tmp_dir: Path) -> tuple[bool, str, int, int]:
        vtt, err = _fetch_vtt_with_backoff(
            yt_dlp, video_id, _tmp_dir, log_stream, cancel_event)
        if not vtt:
            return False, f"fetch: {err}", 0, 0
        try:
            new_segs = _parse_vtt(str(vtt))
            if not new_segs:
                return False, "parser produced no segments", 0, 0
            total_words = sum(len(s.get("w") or []) for s in new_segs)
            if source_tag == "YT+PUNCTUATION" and not _looks_punctuated(new_segs):
                return False, "skipped: new VTT is lowercase (would downgrade punctuation)", 0, 0
            if dry_run:
                return True, "DRY-RUN", len(new_segs), total_words
            try:
                _replace_jsonl_entry(str(jsonl_path), title, video_id, new_segs)
            except Exception as e:
                return False, f"JSONL replace: {e}", 0, 0
            db_rows, db_err = _update_db_words(video_id, new_segs, conn=db_conn)
            if db_err:
                return True, f"DB skipped: {db_err}", len(new_segs), total_words
            return True, f"{db_rows} DB rows", len(new_segs), total_words
        finally:
            try:
                Path(vtt).unlink(missing_ok=True)
            except OSError:
                pass

    if tmp_dir is not None:
        return _run_repair(tmp_dir)
    with tempfile.TemporaryDirectory(prefix="ytarc_repair_") as tmp:
        return _run_repair(Path(tmp))


_VID_RE = re.compile(rb'"video_id"\s*:\s*"([^"]+)"')
_TITLE_RE = re.compile(rb'"title"\s*:\s*"((?:[^"\\]|\\.)*)"')


def _file_contains_any(path: Path, needles: tuple[bytes, ...],
                       chunk_size: int = 1024 * 1024) -> bool:
    """Return True if any needle appears in path without full-file read."""
    if not needles:
        return False
    max_needle = max(len(n) for n in needles)
    overlap = max(0, max_needle - 1)
    tail = b""
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                return False
            hay = tail + chunk
            if any(n in hay for n in needles):
                return True
            tail = hay[-overlap:] if overlap else b""


def _collect_yt_videos(jsonl_path: Path) -> list:
    """Return `[(video_id, title, source_tag), ...]` for every YT-caption
    video in `jsonl_path` (both raw and punct-restored). Each video appears
    at most once even though the JSONL stores many segments per video.

    Fast path: jsonl files for large channels are 20MB+ with hundreds of
    thousands of segment rows. Full `json.loads` per line takes minutes.
    Instead we regex video_id and title (the only fields we need) and skip
    consecutive lines that share the same video_id — typical archives
    group all segments of a video together, so the same-vid skip flies
    past 99% of the file.

    The downgrade-vs-repair decision is made later, per-video, inside
    `_repair_one_video` — after we've fetched the new VTT and can inspect
    whether YT is currently serving a punctuated version.
    """
    txt = _find_txt_for_jsonl(jsonl_path)
    if not txt.exists():
        return []
    sources = _parse_txt_sources(txt)
    if not sources:
        return []
    seen: dict = {}
    last_vid = b""
    try:
        with open(jsonl_path, "rb") as f:
            for raw in f:
                vm = _VID_RE.search(raw)
                if not vm:
                    continue
                vid_b = vm.group(1)
                if vid_b == last_vid:
                    continue  # same video as the previous segment row
                last_vid = vid_b
                vid = vid_b.decode("ascii", errors="replace")
                if vid in seen:
                    continue
                tm = _TITLE_RE.search(raw)
                if not tm:
                    continue
                # Cheap JSON-escape unescaping — titles only ever contain
                # \", \\, and Unicode literals which utf-8 decode handles.
                title = (tm.group(1).decode("utf-8", errors="replace")
                         .replace('\\"', '"').replace("\\\\", "\\"))
                tag = sources.get(_norm_title(title), "")
                if tag in YT_CAPTION_TAGS:
                    seen[vid] = (title, tag)
    except OSError:
        return []
    return [(vid, t, tag) for vid, (t, tag) in seen.items()]


def _find_jsonl_for_video_id(root: Path, video_id: str
                             ) -> tuple[Path | None, str | None]:
    """Scan every JSONL under `root` for an entry with this video_id.
    Returns `(jsonl_path, title)` or `(None, None)`.

    Reads in binary mode and uses _VID_RE.search to detect the id
    BEFORE parsing JSON per line. Old code json.loads'd every line
    of every JSONL up front — on a large archive (100k+ lines per
    channel) that turned single-video repair into a multi-minute
    startup (audit: repair_captions.py:455). Now: O(jsonls * bytes-
    to-match) with early-break when the substring shows up.
    """
    _needle = (b'"video_id": "' + video_id.encode("ascii") + b'"')
    _needle_alt = (b'"video_id":"' + video_id.encode("ascii") + b'"')
    for j in root.rglob(".*Transcript.jsonl"):
        try:
            # Scan in chunks so a 20MB+ channel JSONL is not materialized
            # just to reject a non-match.
            if not _file_contains_any(j, (_needle, _needle_alt)):
                continue
            # Found a hit — parse line-by-line to extract the title.
            with open(j, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if (obj.get("video_id") or "").strip() == video_id:
                        return j, (obj.get("title") or "").strip()
        except OSError:
            continue
    return None, None


def repair_archive(*, output_dir: str, log_stream,
                   channel_folder: str | None = None,
                   video_id: str | None = None,
                   dry_run: bool = False,
                   cancel_event: threading.Event | None = None,
                   pause_event: threading.Event | None = None,
                   queues=None,
                   scope_url: str | None = None) -> dict:
    """Run the repair across the archive (or a subset).

    Emits progress lines to `log_stream` (a `LogStreamer`). One log line
    per video plus a final summary. Returns a dict with counts.

    Scope precedence: `video_id` (one video) > `channel_folder` (one
    channel folder under `output_dir`) > everything under `output_dir`.

    Both `YT CAPTIONS` and `YT+PUNCTUATION` sources are candidates; the
    punctuation-downgrade guard inside `_repair_one_video` decides per
    video whether to write or skip based on the actual VTT YT serves now.

    Runs inline on the calling thread (the sync worker). Checks
    `cancel_event` between videos and during the channel scan so a
    Sync Tasks cancel can interrupt mid-pass. `pause_event` blocks
    between videos when set — same semantics as the download path.
    """
    def _cancelled() -> bool:
        return bool(cancel_event is not None and cancel_event.is_set())

    def _wait_if_paused() -> None:
        # routes through pause_helpers.wait_while_paused.
        # Same semantics — emit Paused, wait, emit Resumed, flip queue
        # pause-active flag in between.
        from .pause_helpers import wait_while_paused
        wait_while_paused(pause_event, cancel_event,
                          stream=log_stream, queues=queues, tick=0.5)

    yt_dlp = find_yt_dlp()
    if not yt_dlp:
        log_stream.emit_error(" — Repair YT captions: yt-dlp not found.\n")
        log_stream.flush()
        return {"ok": False, "error": "yt-dlp not found"}

    # Adaptive pacing: authenticated requests get a much higher YT
    # quota, so we can run ~3x faster when cookies are configured. No
    # cookies → fall back to the conservative 1.0s pause that's been
    # measured to keep us under the anonymous rate limit.
    _has_cookies = bool(_find_cookie_source())
    _per_fetch_sleep = 0.3 if _has_cookies else _RATE_LIMIT_SLEEP_SEC
    log_stream.emit_text(
        f" — yt-dlp cookies: {'attached' if _has_cookies else 'none'} "
        f"(per-fetch pacing: {_per_fetch_sleep:.1f}s)\n",
        "simpleline")
    log_stream.flush()

    root = Path(output_dir)
    if not root.exists():
        log_stream.emit_error(
            f" — Repair YT captions: archive root missing ({root}).\n")
        log_stream.flush()
        return {"ok": False, "error": "archive root not found"}

    # Build the work list as (jsonl_path, video_id, title, source_tag).
    # If a prior run of this same scope saved a checkpoint, load that
    # and skip the scan entirely — for a 100-channel archive the scan
    # is ~70 seconds the user otherwise has to sit through on every
    # restart. The progress file filters out already-processed videos
    # below, so the resume picks up exactly where the prior run stopped.
    work: list = []
    used_checkpoint = False
    if video_id:
        j, t = _find_jsonl_for_video_id(root, video_id)
        if j and t:
            tag = _parse_txt_sources(_find_txt_for_jsonl(j)).get(
                _norm_title(t), "")
            work.append((j, video_id, t, tag))
        else:
            log_stream.emit_error(
                f" — video_id {video_id} not found in any JSONL.\n")
    elif scope_url and not dry_run and _load_checkpoint(scope_url):
        cached = _load_checkpoint(scope_url)
        work = cached or []
        used_checkpoint = True
        log_stream.emit_text(
            f" — Using cached work list from prior scan "
            f"({len(work):,} videos). Skipping re-scan.\n",
            "simpleline")
        log_stream.flush()

    if not used_checkpoint and not video_id:
        scope = root / channel_folder if channel_folder else root
        if not scope.exists():
            log_stream.emit_error(f" — folder not found: {scope}\n")
            log_stream.flush()
            return {"ok": False, "error": "folder not found"}
        # `simpleline` tag so the scan progress shows in Simple mode —
        # `dim` would have been quietly filtered out by the simple-mode
        # gate in LogStreamer, making the scan appear hung.
        if channel_folder:
            # Single channel — one "scanning" line then plow through.
            log_stream.emit_text(
                f" — Scanning {scope.name}...\n", "simpleline")
            log_stream.flush()
            for j in scope.rglob(".*Transcript.jsonl"):
                # Honor cancel inside the rglob walk too — the
                # original only checked at channel boundaries, so on a
                # huge channel the rglob enumeration alone could run
                # for tens of seconds while the user thought Cancel
                # was hung.
                if _cancelled():
                    log_stream.emit_text(
                        " — Cancelled during scan.\n", "simpleline")
                    log_stream.flush()
                    break
                for vid, t, tag in _collect_yt_videos(j):
                    work.append((j, vid, t, tag))
        else:
            # All channels — per-channel progress so the user knows the
            # scan is alive even when a particular channel takes a while.
            channel_dirs = sorted(p for p in scope.iterdir() if p.is_dir())
            log_stream.emit_text(
                f" — Scanning {len(channel_dirs)} channels for "
                f"YT-caption videos...\n", "simpleline")
            log_stream.flush()
            for idx, ch_dir in enumerate(channel_dirs, 1):
                if _cancelled():
                    log_stream.emit_text(
                        f"   Scan cancelled after {idx - 1} channels.\n",
                        "simpleline")
                    log_stream.flush()
                    break
                before = len(work)
                for j in ch_dir.rglob(".*Transcript.jsonl"):
                    for vid, t, tag in _collect_yt_videos(j):
                        work.append((j, vid, t, tag))
                added = len(work) - before
                log_stream.emit_text(
                    f"   [{idx}/{len(channel_dirs)}] {ch_dir.name}: "
                    f"{added} candidate(s)\n", "simpleline")
                if idx % 3 == 0:
                    log_stream.flush()
            log_stream.flush()
        # Cache the scan so the next restart of this scope can skip the
        # whole "Scanning N channels..." phase entirely.
        if scope_url and work:
            _save_checkpoint(scope_url, work)

    # Resume-from-progress. Skip any video_ids we already processed in
    # a prior run of this same scope. Logged so the user knows the
    # count they're starting from. dry_run never persists or loads.
    already_done: set = set()
    skipped_resume = 0
    if scope_url and not dry_run:
        already_done = _load_progress(scope_url)
        if already_done:
            before_filter = len(work)
            work = [(j, vid, t, tag) for (j, vid, t, tag) in work
                    if vid not in already_done]
            skipped_resume = before_filter - len(work)
            log_stream.emit_text(
                f" — Resuming: {skipped_resume:,} video(s) already done "
                f"on a prior run; {len(work):,} remaining.\n",
                "simpleline")
            log_stream.flush()

    log_stream.emit_text(
        f" — {len(work):,} YT-caption video(s) to process.\n",
        "simpleline")
    log_stream.flush()
    # Per-video lines stay `dim` (Verbose mode only — 80k+ of them
    # would drown the Simple log). Emit a `simpleline` milestone every
    # MILESTONE_EVERY videos so Simple-mode users still see steady
    # progress instead of one banner followed by silent hours. Lowered
    # from 25 to 10 — at ~5s/video that's a milestone every ~50s,
    # which feels alive without flooding the log.
    MILESTONE_EVERY = 10
    # Surface "(N/total)" on the popover sync row via the existing
    # sync_pass_progress fields. Without this the popover stays static
    # on "Repair — All channels" for the whole multi-hour pass.
    if queues is not None and len(work) > 0:
        try: queues.set_sync_pass_progress(0, len(work))
        except Exception as e: _log.debug("swallowed: %s", e)

    ok_count = 0
    fail_count = 0
    skip_count = 0  # downgrade-guard skips, tracked separately
    cancelled_early = False
    # Open one shared sqlite3 connection for the whole pass. With ~80k
    # videos per full repair, opening/closing per video was wasting tens
    # of thousands of file-handle operations and prevented the WAL
    # pragma from taking effect on cold-start invocations.
    db_conn = None if dry_run else _open_repair_db_conn()
    repair_tmp = tempfile.TemporaryDirectory(prefix="ytarc_repair_")
    repair_tmp_dir = Path(repair_tmp.name)
    try:
      for i, (j, vid, t, tag) in enumerate(work, 1):
        _wait_if_paused()
        if _cancelled():
            cancelled_early = True
            log_stream.emit_text(
                f"   Cancelled after {i - 1}/{len(work)} videos.\n", "dim")
            break
        success, msg, n_segs, n_words = _repair_one_video(
            yt_dlp, j, t, vid, tag, dry_run, log_stream, cancel_event,
            db_conn=db_conn, tmp_dir=repair_tmp_dir)
        # Record progress only for a GENUINE success or a deliberate
        # "skipped:" downgrade, so a restart resumes after the last truly-done
        # video. Do NOT persist hard FAILs, the "DB skipped" partial (JSONL
        # written but the DB UPDATE failed), rate-limited aborts, or cancels —
        # those MUST be retried on the next resume rather than silently skipped
        # forever (audit r2). (Permanent fails like "no captions" re-check each
        # run, which also catches captions added to YT later.)
        _persist_this = (
            scope_url and not dry_run
            and (
                (success and "DB skipped" not in (msg or ""))
                or (msg or "").startswith("skipped:")
            )
        )
        if _persist_this:
            _append_progress(scope_url, vid)
        short = (t[:60] + "…") if len(t) > 63 else t
        prefix = f"   [{i}/{len(work)}] "
        if success:
            ok_count += 1
            log_stream.emit_text(
                f"{prefix}OK   {vid}  {short}  "
                f"({n_segs} segs / {n_words} words / {msg})\n", "dim")
        elif msg.startswith("skipped:"):
            # Punctuation-downgrade guard tripped — not an error, just a
            # decision to leave this video alone.
            skip_count += 1
            log_stream.emit_text(
                f"{prefix}SKIP {vid}  {short}  — {msg}\n", "dim")
        else:
            fail_count += 1
            log_stream.emit_error(
                f"{prefix}FAIL {vid}  {short}  — {msg}\n")
        # Flush every few videos so the verbose-mode user sees live
        # progress rather than one big dump at the end.
        if i % 5 == 0:
            log_stream.flush()
        # Simple-mode milestone — visible regardless of dim filter so
        # the user can see the repair is making progress.
        if i % MILESTONE_EVERY == 0 or i == len(work):
            log_stream.emit_text(
                f" — [{i:,}/{len(work):,}] "
                f"{ok_count:,} repaired · {skip_count:,} skipped"
                + (f" · {fail_count:,} failed" if fail_count else "")
                + "\n",
                "simpleline")
            log_stream.flush()
        # Update the popover sync row's "(N/total)" label every video.
        # set_sync_pass_progress is debounced (no-op when value matches)
        # so this is cheap.
        if queues is not None:
            try: queues.set_sync_pass_progress(i, len(work))
            except Exception as e: _log.debug("swallowed: %s", e)
        # Gentle rate-limit between fetches so YT doesn't 429 us. Use
        # cancel_event.wait so a Sync Tasks cancel still breaks out
        # promptly instead of having to sit through the sleep first.
        # Per-fetch sleep adapts to cookie availability (faster when
        # authenticated — see _per_fetch_sleep above).
        if i < len(work):
            if cancel_event is not None:
                if cancel_event.wait(timeout=_per_fetch_sleep):
                    cancelled_early = True
                    log_stream.emit_text(
                        f"   Cancelled after {i}/{len(work)} videos.\n",
                        "simpleline")
                    break
            else:
                time.sleep(_per_fetch_sleep)
    finally:
        try:
            repair_tmp.cleanup()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        if db_conn is not None:
            try: db_conn.close()
            except Exception as e: _log.debug("swallowed: %s", e)

    status_word = "Cancelled" if cancelled_early else "Repair done"
    log_stream.emit_text(
        f" — {status_word}: {ok_count} repaired, {skip_count} skipped, "
        f"{fail_count} failed"
        + (f" (+{skipped_resume:,} from prior runs)" if skipped_resume else "")
        + (" (DRY-RUN — no writes).\n" if dry_run else ".\n"),
        "simpleline_pink")
    log_stream.flush()

    # Clean full-completion → drop both the progress and checkpoint
    # files so the next time this scope is requested it starts fresh.
    # If cancelled or any failures remain, keep them so resume picks
    # up where we left off.
    if (scope_url and not dry_run and not cancelled_early
            and fail_count == 0):
        _clear_progress(scope_url)
        _clear_checkpoint(scope_url)

    return {"ok": True, "succeeded": ok_count, "skipped": skip_count,
            "failed": fail_count, "total": len(work),
            "from_prior": skipped_resume,
            "cancelled": cancelled_early}
