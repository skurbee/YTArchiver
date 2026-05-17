"""Restore punctuation to per-segment text for YT-captioned videos.

Walks each video's segments in the segments DB + the channel's aggregated
JSONL sidecar, runs the existing `PunctuationManager` (`punct_worker.py`,
loaded once for the whole pass) over each segment's text, and writes the
punctuated form back to:
  - `segments.text` column in transcription_index.db
  - the `text` field of the matching line in the JSONL sidecar

The `words` array is left untouched — its per-word timestamps drive the
overlay-captions karaoke, which Scott prefers to keep in raw-lowercase
form (short captions with commas/periods mid-utterance read awkwardly).
The watch-view right-panel renderer aligns the punctuated text with the
raw word timestamps to preserve karaoke + click-to-seek there.

No YT calls. Pure local CPU/GPU work. Reuses the sync-queue / progress /
checkpoint infrastructure from `repair_captions` for pause/resume/cancel
parity across the two tools.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

from .transcribe import PunctuationManager, _replace_jsonl_entry
from .ytarchiver_config import TRANSCRIPTION_DB
from .repair_captions import (
    _norm_title, _parse_txt_sources, _find_txt_for_jsonl,
    _load_progress, _append_progress, _clear_progress,
    _load_checkpoint, _save_checkpoint, _clear_checkpoint,
    YT_CAPTION_TAGS,
)


# Same "is this already punctuated?" heuristic the YT repair uses.
_PUNCT_CHARS_RE = re.compile(r"[.,!?;:]")


def _already_punctuated(text: str) -> bool:
    """Quick check: does this segment text already have sentence punctuation?
    If yes there's nothing to do for it — skip."""
    return bool(_PUNCT_CHARS_RE.search(text or ""))


def _segments_for_video(conn: sqlite3.Connection, video_id: str) -> list:
    """Pull this video's segments from the DB in chronological order.

    Returns list of dicts with the long-form keys `_replace_jsonl_entry`
    expects: start, end, text, words (parsed from JSON).
    """
    rows = conn.execute(
        "SELECT start_time, end_time, text, words FROM segments "
        "WHERE video_id=? ORDER BY start_time", (video_id,)
    ).fetchall()
    segs = []
    for s, e, t, w in rows:
        try:
            words = json.loads(w) if w else []
        except (json.JSONDecodeError, ValueError):
            words = []
        segs.append({
            "start": float(s or 0),
            "end": float(e or 0),
            "text": t or "",
            "words": words,
        })
    return segs


def _update_db_text(conn: sqlite3.Connection, video_id: str,
                    segments: list) -> int:
    """Targeted UPDATE on segments.text — column-only, FTS untouched
    (text shape unchanged, only punctuation added)."""
    updated = 0
    for seg in segments:
        cur = conn.execute(
            "UPDATE segments SET text=? "
            "WHERE video_id=? AND start_time=? AND end_time=?",
            (seg["text"], video_id, seg["start"], seg["end"]))
        updated += cur.rowcount
    conn.commit()
    return updated


def _resolve_video_filepath(conn: sqlite3.Connection, video_id: str) -> str:
    """Look up the on-disk video path so _replace_jsonl_entry has it.
    Empty string if not found — _replace_jsonl_entry tolerates that."""
    try:
        row = conn.execute(
            "SELECT filepath FROM videos WHERE video_id=? LIMIT 1",
            (video_id,)).fetchone()
        return row[0] if row and row[0] else ""
    except sqlite3.Error:
        return ""


def _enumerate_jsonls(scope: Path) -> list:
    """All Transcript.jsonl files under the given scope path."""
    return sorted(scope.rglob(".*Transcript.jsonl"))


def _enumerate_yt_videos(jsonl_path: Path) -> list:
    """`[(video_id, title, jsonl_path, src_tag), ...]` — every YT-captioned
    video referenced in this JSONL whose Transcript.txt header tags it as
    YT CAPTIONS or YT+PUNCTUATION. One row per video (deduped on video_id)."""
    txt = _find_txt_for_jsonl(jsonl_path)
    if not txt.exists():
        return []
    sources = _parse_txt_sources(txt)
    if not sources:
        return []
    vid_re = re.compile(rb'"video_id"\s*:\s*"([^"]+)"')
    title_re = re.compile(rb'"title"\s*:\s*"((?:[^"\\]|\\.)*)"')
    seen: dict = {}
    last_vid = b""
    try:
        with open(jsonl_path, "rb") as f:
            for raw in f:
                vm = vid_re.search(raw)
                if not vm:
                    continue
                vid_b = vm.group(1)
                if vid_b == last_vid:
                    continue
                last_vid = vid_b
                vid = vid_b.decode("ascii", errors="replace")
                if vid in seen:
                    continue
                tm = title_re.search(raw)
                if not tm:
                    continue
                title = (tm.group(1).decode("utf-8", errors="replace")
                         .replace('\\"', '"').replace("\\\\", "\\"))
                tag = sources.get(_norm_title(title), "")
                if tag in YT_CAPTION_TAGS:
                    seen[vid] = (title, tag)
    except OSError:
        return []
    return [(vid, t, jsonl_path, tag) for vid, (t, tag) in seen.items()]


def restore_punctuation_archive(*, output_dir: str, log_stream,
                                 channel_folder: Optional[str] = None,
                                 video_id: Optional[str] = None,
                                 dry_run: bool = False,
                                 cancel_event: Optional[threading.Event] = None,
                                 pause_event: Optional[threading.Event] = None,
                                 queues=None,
                                 scope_url: Optional[str] = None) -> dict:
    """Run the punctuation pass across the archive (or a subset).

    Same shape as repair_captions.repair_archive — emits progress per
    video, supports cancel/pause/resume, persists per-video progress
    and a post-scan checkpoint.
    """
    def _cancelled() -> bool:
        return bool(cancel_event is not None and cancel_event.is_set())

    def _wait_if_paused() -> None:
        if pause_event is None or not pause_event.is_set():
            return
        log_stream.emit_text(
            " — Paused. Click Resume in Sync Tasks to continue.\n",
            "simpleline")
        log_stream.flush()
        if queues is not None:
            try: queues.set_sync_paused_active(True)
            except Exception: pass
        try:
            while pause_event.is_set():
                if _cancelled():
                    return
                pause_event.wait(timeout=0.5)
        finally:
            if queues is not None:
                try: queues.set_sync_paused_active(False)
                except Exception: pass
            if not _cancelled():
                log_stream.emit_text(" — Resumed.\n", "simpleline")
                log_stream.flush()

    root = Path(output_dir)
    if not root.exists():
        log_stream.emit_error(
            f" — Restore punctuation: archive root missing ({root}).\n")
        log_stream.flush()
        return {"ok": False, "error": "archive root not found"}

    # Spin up the punct subprocess once for the whole pass. The first
    # punctuate() call loads the model (~5-15s); subsequent calls are
    # quick. If Python 3.11 isn't installed the manager fails early and
    # we bail with a clear error.
    punct_mgr = PunctuationManager(log_stream)
    if not punct_mgr.is_available():
        log_stream.emit_error(
            " — Restore punctuation: Python 3.11 + punctuation worker "
            "not available. (Install Python 3.11 — same dependency as "
            "the Whisper transcription path.)\n")
        log_stream.flush()
        return {"ok": False, "error": "punctuation worker unavailable"}

    # Build work list — same checkpoint / scan pattern as repair_captions.
    work: list = []  # [(jsonl_path, video_id, title, src_tag), ...]
    used_checkpoint = False
    if video_id:
        # Single-video: find its jsonl by scanning. Cheap enough.
        for j in root.rglob(".*Transcript.jsonl"):
            try:
                with open(j, "rb") as f:
                    payload = f.read(4 * 1024 * 1024)  # first 4MB scan
            except OSError:
                continue
            if (b'"' + video_id.encode() + b'"') in payload:
                # Pull the title from that line
                for vid, t, jp, tag in _enumerate_yt_videos(j):
                    if vid == video_id:
                        work.append((jp, vid, t, tag))
                        break
                break
        if not work:
            log_stream.emit_error(
                f" — video_id {video_id} not found.\n")
    elif scope_url and not dry_run and _load_checkpoint(scope_url):
        cached = _load_checkpoint(scope_url)
        if cached:
            work = cached
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
        if channel_folder:
            log_stream.emit_text(
                f" — Scanning {scope.name}...\n", "simpleline")
            log_stream.flush()
            for j in _enumerate_jsonls(scope):
                for vid, t, jp, tag in _enumerate_yt_videos(j):
                    work.append((jp, vid, t, tag))
        else:
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
                for j in _enumerate_jsonls(ch_dir):
                    for vid, t, jp, tag in _enumerate_yt_videos(j):
                        work.append((jp, vid, t, tag))
                added = len(work) - before
                log_stream.emit_text(
                    f"   [{idx}/{len(channel_dirs)}] {ch_dir.name}: "
                    f"{added} candidate(s)\n", "simpleline")
                if idx % 3 == 0:
                    log_stream.flush()
            log_stream.flush()
        if scope_url and work:
            _save_checkpoint(scope_url, work)

    # Filter out already-processed videos (resume after restart).
    already_done: set = set()
    skipped_resume = 0
    if scope_url and not dry_run:
        already_done = _load_progress(scope_url)
        if already_done:
            before_filter = len(work)
            work = [tup for tup in work if tup[1] not in already_done]
            skipped_resume = before_filter - len(work)
            log_stream.emit_text(
                f" — Resuming: {skipped_resume:,} video(s) already done "
                f"on a prior run; {len(work):,} remaining.\n",
                "simpleline")
            log_stream.flush()

    log_stream.emit_text(
        f" — {len(work):,} YT-caption video(s) to punctuate.\n",
        "simpleline")
    log_stream.flush()
    MILESTONE_EVERY = 25
    if queues is not None and len(work) > 0:
        try: queues.set_sync_pass_progress(0, len(work))
        except Exception: pass

    # Open a single DB connection for the whole pass — the SQLite writes
    # are fast enough that one shared connection (with PRAGMA
    # busy_timeout) outperforms per-video opens.
    try:
        conn = sqlite3.connect(str(TRANSCRIPTION_DB), timeout=30.0)
        conn.execute("PRAGMA busy_timeout=30000")
    except sqlite3.Error as e:
        log_stream.emit_error(
            f" — Restore punctuation: DB open failed: {e}\n")
        log_stream.flush()
        return {"ok": False, "error": f"DB open: {e}"}

    ok_count = 0
    skip_count = 0  # already punctuated
    fail_count = 0
    cancelled_early = False
    try:
        for i, (jsonl_path, vid, title, tag) in enumerate(work, 1):
            _wait_if_paused()
            if _cancelled():
                cancelled_early = True
                log_stream.emit_text(
                    f"   Cancelled after {i - 1}/{len(work)} videos.\n",
                    "simpleline")
                break

            segs = _segments_for_video(conn, vid)
            if not segs:
                fail_count += 1
                short = (title[:55] + "…") if len(title) > 58 else title
                log_stream.emit_error(
                    f"   [{i}/{len(work)}] FAIL {vid}  {short} — "
                    f"no DB segments\n")
                if scope_url and not dry_run:
                    _append_progress(scope_url, vid)
                continue

            # Skip if the concatenated text already looks punctuated.
            # Cheap O(1) check that lets modern videos sail through.
            joined = " ".join((s.get("text") or "")[:120] for s in segs[:8])
            if _already_punctuated(joined):
                skip_count += 1
                short = (title[:55] + "…") if len(title) > 58 else title
                log_stream.emit_text(
                    f"   [{i}/{len(work)}] SKIP {vid}  {short} — "
                    f"already punctuated\n", "dim")
                if scope_url and not dry_run:
                    _append_progress(scope_url, vid)
                if queues is not None:
                    try: queues.set_sync_pass_progress(i, len(work))
                    except Exception: pass
                continue

            # Run each segment's text through punctuate(). Failures fall
            # through to "no change" — better than dropping the video.
            new_segs = []
            for seg in segs:
                old_text = seg["text"]
                if not old_text.strip():
                    new_segs.append(seg)
                    continue
                try:
                    new_text = punct_mgr.punctuate(old_text, timeout_sec=30.0)
                except Exception:
                    new_text = old_text
                ns = dict(seg)
                ns["text"] = new_text or old_text
                new_segs.append(ns)

            if dry_run:
                ok_count += 1
                short = (title[:55] + "…") if len(title) > 58 else title
                log_stream.emit_text(
                    f"   [{i}/{len(work)}] OK   {vid}  {short}  "
                    f"({len(new_segs)} segs / DRY-RUN)\n", "dim")
                if i % MILESTONE_EVERY == 0 or i == len(work):
                    log_stream.emit_text(
                        f" — [{i:,}/{len(work):,}] "
                        f"{ok_count:,} punctuated · {skip_count:,} skipped"
                        + (f" · {fail_count:,} failed" if fail_count else "")
                        + "\n", "simpleline")
                    log_stream.flush()
                if queues is not None:
                    try: queues.set_sync_pass_progress(i, len(work))
                    except Exception: pass
                continue

            # Write back: DB column update + JSONL rewrite.
            try:
                _update_db_text(conn, vid, new_segs)
                _replace_jsonl_entry(str(jsonl_path), title, vid, new_segs)
            except Exception as e:
                fail_count += 1
                short = (title[:55] + "…") if len(title) > 58 else title
                log_stream.emit_error(
                    f"   [{i}/{len(work)}] FAIL {vid}  {short} — "
                    f"write-back: {e}\n")
                if scope_url:
                    _append_progress(scope_url, vid)
                continue

            ok_count += 1
            if scope_url:
                _append_progress(scope_url, vid)
            short = (title[:55] + "…") if len(title) > 58 else title
            log_stream.emit_text(
                f"   [{i}/{len(work)}] OK   {vid}  {short}  "
                f"({len(new_segs)} segs)\n", "dim")
            if i % 5 == 0:
                log_stream.flush()
            if i % MILESTONE_EVERY == 0 or i == len(work):
                log_stream.emit_text(
                    f" — [{i:,}/{len(work):,}] "
                    f"{ok_count:,} punctuated · {skip_count:,} skipped"
                    + (f" · {fail_count:,} failed" if fail_count else "")
                    + "\n", "simpleline")
                log_stream.flush()
            if queues is not None:
                try: queues.set_sync_pass_progress(i, len(work))
                except Exception: pass
    finally:
        try:
            punct_mgr._stop()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    status_word = "Cancelled" if cancelled_early else "Punctuation pass done"
    log_stream.emit_text(
        f" — {status_word}: {ok_count} punctuated, {skip_count} skipped, "
        f"{fail_count} failed"
        + (f" (+{skipped_resume:,} from prior runs)" if skipped_resume else "")
        + (" (DRY-RUN — no writes).\n" if dry_run else ".\n"),
        "simpleline_pink")
    log_stream.flush()

    if (scope_url and not dry_run and not cancelled_early
            and fail_count == 0):
        _clear_progress(scope_url)
        _clear_checkpoint(scope_url)

    return {"ok": True, "succeeded": ok_count, "skipped": skip_count,
            "failed": fail_count, "total": len(work),
            "from_prior": skipped_resume,
            "cancelled": cancelled_early}
