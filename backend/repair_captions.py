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
from pathlib import Path
from typing import Optional

from .transcribe import _parse_vtt, _replace_jsonl_entry
from .sync import find_yt_dlp
from .ytarchiver_config import TRANSCRIPTION_DB


# Source-tag values written into Transcript.txt section headers by
# transcribe.py. Whisper rows look like `(WHISPER:small)` and are skipped.
YT_CAPTION_TAGS = {"YT CAPTIONS", "YT+PUNCTUATION"}

_HEADER_RE = re.compile(
    r"^===\((.+?)\),\s*\((.+?)\),\s*\((.+?)\),\s*\((.+?)\)===\s*$"
)


def _norm_title(s: str) -> str:
    """Punctuation-insensitive title key. Mirrors `transcribe._norm_title`
    so we match the txt-side header even if smart quotes / em-dashes drift."""
    return re.sub(r"[^\w\s]", "", s or "").strip().lower()


def _parse_txt_sources(txt_path: Path) -> dict:
    """Return `{normalized_title: source_tag}` from a Transcript.txt file."""
    out: dict = {}
    try:
        with open(txt_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                m = _HEADER_RE.match(line.strip())
                if m:
                    out[_norm_title(m.group(1))] = m.group(4).strip()
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
               out_dir: Path) -> tuple[Optional[Path], Optional[str]]:
    """Run yt-dlp to fetch a single video's auto-caption VTT."""
    cmd = [
        yt_dlp,
        "--skip-download", "--write-auto-subs",
        "--sub-lang", "en", "--sub-format", "vtt",
        "--no-warnings",
        "-o", str(out_dir / f"{video_id}.%(ext)s"),
        f"https://www.youtube.com/watch?v={video_id}",
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        return None, "yt-dlp timeout"
    except FileNotFoundError:
        return None, f"yt-dlp not found: {yt_dlp}"
    if r.returncode != 0:
        msg = (r.stderr or r.stdout or "").splitlines()
        tail = msg[-1] if msg else "yt-dlp failed"
        return None, tail[:200]
    for cand in out_dir.glob(f"{video_id}*.vtt"):
        return cand, None
    return None, "no captions available"


def _update_db_words(video_id: str,
                     new_segments: list) -> tuple[int, Optional[str]]:
    """Targeted UPDATE on the segments table — column-only, FTS untouched.

    Match on (video_id, start_time, end_time). The parser fix doesn't
    change segmentation or timing, only the `w` array contents, so
    every row matches exactly.
    """
    if not Path(TRANSCRIPTION_DB).exists():
        return 0, f"DB not found at {TRANSCRIPTION_DB}"
    try:
        conn = sqlite3.connect(str(TRANSCRIPTION_DB), timeout=30.0)
    except sqlite3.Error as e:
        return 0, f"DB open: {e}"
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        updated = 0
        for seg in new_segments:
            s_val = seg.get("start") if "start" in seg else seg.get("s", 0)
            e_val = seg.get("end") if "end" in seg else seg.get("e", 0)
            w_val = seg.get("words") if "words" in seg else seg.get("w", [])
            if not isinstance(w_val, list):
                w_val = []
            cur = conn.execute(
                "UPDATE segments SET words=? "
                "WHERE video_id=? AND start_time=? AND end_time=?",
                (json.dumps(w_val, ensure_ascii=False), video_id,
                 float(s_val or 0), float(e_val or 0)))
            updated += cur.rowcount
        conn.commit()
        return updated, None
    except sqlite3.Error as e:
        return 0, f"DB update: {e}"
    finally:
        conn.close()


def _repair_one_video(yt_dlp: str, jsonl_path: Path, title: str,
                      video_id: str, dry_run: bool
                      ) -> tuple[bool, str, int, int]:
    """Fetch+parse+write a single video. Returns (ok, msg, n_segs, n_words)."""
    with tempfile.TemporaryDirectory(prefix="ytarc_repair_") as tmp:
        tmp_dir = Path(tmp)
        vtt, err = _fetch_vtt(yt_dlp, video_id, tmp_dir)
        if not vtt:
            return False, f"fetch: {err}", 0, 0
        new_segs = _parse_vtt(str(vtt))
        if not new_segs:
            return False, "parser produced no segments", 0, 0
        total_words = sum(len(s.get("w") or []) for s in new_segs)
        if dry_run:
            return True, "DRY-RUN", len(new_segs), total_words
        try:
            _replace_jsonl_entry(str(jsonl_path), title, video_id, new_segs)
        except Exception as e:
            return False, f"JSONL replace: {e}", 0, 0
        db_rows, db_err = _update_db_words(video_id, new_segs)
        if db_err:
            return True, f"DB skipped: {db_err}", len(new_segs), total_words
        return True, f"{db_rows} DB rows", len(new_segs), total_words


def _collect_yt_videos(jsonl_path: Path,
                       include_punctuated: bool) -> list:
    """Return `[(video_id, title, source_tag), ...]` for the YT-caption
    videos in `jsonl_path`. Each video appears at most once even though
    JSONL stores many segments per video.
    """
    txt = _find_txt_for_jsonl(jsonl_path)
    if not txt.exists():
        return []
    sources = _parse_txt_sources(txt)
    accepted = set(YT_CAPTION_TAGS)
    if not include_punctuated:
        accepted.discard("YT+PUNCTUATION")
    seen: dict = {}
    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                ls = line.strip()
                if not ls:
                    continue
                try:
                    obj = json.loads(ls)
                except json.JSONDecodeError:
                    continue
                vid = (obj.get("video_id") or "").strip()
                title = (obj.get("title") or "").strip()
                if not vid or not title or vid in seen:
                    continue
                tag = sources.get(_norm_title(title), "")
                if tag in accepted:
                    seen[vid] = (title, tag)
    except OSError:
        return []
    return [(vid, t, tag) for vid, (t, tag) in seen.items()]


def _find_jsonl_for_video_id(root: Path, video_id: str
                             ) -> tuple[Optional[Path], Optional[str]]:
    """Scan every JSONL under `root` for an entry with this video_id.
    Returns `(jsonl_path, title)` or `(None, None)`."""
    for j in root.rglob(".*Transcript.jsonl"):
        try:
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
                   channel_folder: Optional[str] = None,
                   video_id: Optional[str] = None,
                   dry_run: bool = False,
                   include_punctuated: bool = False) -> dict:
    """Run the repair across the archive (or a subset).

    Emits progress lines to `log_stream` (a `LogStreamer`). One log line
    per video plus a final summary. Returns a dict with counts.

    Scope precedence: `video_id` (one video) > `channel_folder` (one
    channel folder under `output_dir`) > everything under `output_dir`.
    """
    yt_dlp = find_yt_dlp()
    if not yt_dlp:
        log_stream.emit_error(" — Repair YT captions: yt-dlp not found.\n")
        log_stream.flush()
        return {"ok": False, "error": "yt-dlp not found"}

    root = Path(output_dir)
    if not root.exists():
        log_stream.emit_error(
            f" — Repair YT captions: archive root missing ({root}).\n")
        log_stream.flush()
        return {"ok": False, "error": "archive root not found"}

    log_stream.emit_header(
        f"Repair YT auto-captions{' (DRY-RUN)' if dry_run else ''}\n")

    # Build the work list as a flat sequence of (jsonl_path, video_id, title)
    work: list = []
    if video_id:
        j, t = _find_jsonl_for_video_id(root, video_id)
        if j and t:
            work.append((j, video_id, t))
        else:
            log_stream.emit_error(
                f" — video_id {video_id} not found in any JSONL.\n")
    else:
        scope = root / channel_folder if channel_folder else root
        if not scope.exists():
            log_stream.emit_error(f" — folder not found: {scope}\n")
            log_stream.flush()
            return {"ok": False, "error": "folder not found"}
        for j in scope.rglob(".*Transcript.jsonl"):
            for vid, t, _tag in _collect_yt_videos(j, include_punctuated):
                work.append((j, vid, t))

    log_stream.emit_text(
        f" — {len(work)} YT-caption video(s) to process.\n",
        "simpleline")
    log_stream.flush()

    ok_count = 0
    fail_count = 0
    for i, (j, vid, t) in enumerate(work, 1):
        success, msg, n_segs, n_words = _repair_one_video(
            yt_dlp, j, t, vid, dry_run)
        short = (t[:60] + "…") if len(t) > 63 else t
        prefix = f"   [{i}/{len(work)}] "
        if success:
            ok_count += 1
            log_stream.emit_text(
                f"{prefix}OK   {vid}  {short}  "
                f"({n_segs} segs / {n_words} words / {msg})\n", "dim")
        else:
            fail_count += 1
            log_stream.emit_error(
                f"{prefix}FAIL {vid}  {short}  — {msg}\n")
        # Flush every few videos so the user sees live progress rather
        # than one big dump at the end.
        if i % 5 == 0:
            log_stream.flush()

    log_stream.emit_text(
        f" — Repair done: {ok_count} succeeded, {fail_count} failed"
        + (" (DRY-RUN — no writes).\n" if dry_run else ".\n"),
        "simpleline_pink")
    log_stream.flush()

    return {"ok": True, "succeeded": ok_count, "failed": fail_count,
            "total": len(work)}
