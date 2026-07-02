"""
metadata.manual_backfill — recover YouTube video IDs for single/manual
downloads that have no ID (e.g. grabbed years ago by another tool).

These files carry no embedded URL/ID, but ~most carry the original title
(and often the upload date) in the container metadata — which survives a
filename rename. We turn that into an ID by searching YouTube and matching
on DURATION, which is the reliable discriminator.

Per-file cascade:
  1. ffprobe the container for embedded title + date + duration. Fall back
     to the filename (date-suffix stripped) when no embedded title.
  2. yt-dlp `ytsearchN:<title>` -> candidates (id, title, duration, channel).
  3. Keep only candidates within +/- DUR_TOLERANCE_S of the local duration;
     rank by fuzzy title similarity.
  4. Decide:
       - one clear winner (strong title, no close rival) -> AUTO: write the id.
       - 2+ plausible -> REVIEW: persist candidates for the user to pick.
       - none -> stamp "tried", skip.

Safe by design: a wrong id writes wrong metadata, so nothing ambiguous is
ever auto-applied. Pure decision logic (`decide`) is separated from all IO
so it can be unit-tested without a network.
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from difflib import SequenceMatcher
from typing import Any

from ..log import get_logger, swallow
from ..ytarchiver_config import APP_DATA_DIR
from .normalize import _normalize_title_for_match

_log = get_logger(__name__)

# Tunables.
DUR_TOLERANCE_S = 2.0       # duration must match within +/- this many seconds
SEARCH_N = 20              # how many YouTube search results to pull per video
TITLE_AUTO = 0.85         # title similarity needed to auto-accept
TITLE_REVIEW_MIN = 0.55   # below this, a candidate isn't even worth reviewing
RIVAL_MARGIN = 0.10       # winner must beat #2 by this much to auto-accept
_PER_SEARCH_SLEEP_S = 0.6  # base pacing between searches (politeness vs YT)
_RATE_LIMIT_BACKOFFS = (30, 90, 300)  # 429 backoff schedule

# Where REVIEW items are persisted for the (separate) picker UI.
REVIEW_FILE = APP_DATA_DIR / "manual_id_review.jsonl"

_CREATE_NO_WINDOW = 0x08000000 if os.name == "nt" else 0


# ── Pure decision logic (no IO — unit-tested) ───────────────────────────

def _title_similarity(a: str, b: str) -> float:
    na, nb = _normalize_title_for_match(a or ""), _normalize_title_for_match(b or "")
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


def decide(local_title: str, local_dur: float | None,
           candidates: list[dict[str, Any]], *,
           dur_tol: float = DUR_TOLERANCE_S,
           auto_sim: float = TITLE_AUTO,
           min_sim: float = TITLE_REVIEW_MIN,
           rival_margin: float = RIVAL_MARGIN) -> dict[str, Any]:
    """Classify a video against its search candidates.

    Returns {"decision": "auto"|"review"|"none", "best"?, "shortlist": [...]}.
    Duration is a HARD filter: a candidate with no known duration, or outside
    the tolerance window, is never eligible — that's the safety guarantee.
    """
    if not local_dur or local_dur <= 0:
        # No local duration -> we can't safely match on length. Punt to review
        # only if a title is a near-exact hit; otherwise give up.
        scored = []
        for c in candidates:
            sim = _title_similarity(local_title, c.get("title") or "")
            if sim >= 0.95:
                scored.append({**c, "title_sim": sim, "dur_delta": None})
        scored.sort(key=lambda c: -c["title_sim"])
        return {"decision": "review" if scored else "none",
                "shortlist": scored[:5]}

    in_window: list[dict[str, Any]] = []
    for c in candidates:
        cd = c.get("duration")
        try:
            cd = float(cd) if cd is not None else None
        except (TypeError, ValueError):
            cd = None
        if cd is None:
            continue
        delta = abs(cd - local_dur)
        if delta <= dur_tol:
            in_window.append({
                **c,
                "duration": cd,
                "dur_delta": round(delta, 2),
                "title_sim": round(_title_similarity(local_title, c.get("title") or ""), 4),
            })
    in_window.sort(key=lambda c: (-c["title_sim"], c["dur_delta"]))
    if not in_window:
        return {"decision": "none", "shortlist": []}

    best = in_window[0]
    if best["title_sim"] >= auto_sim and (
            len(in_window) == 1
            or best["title_sim"] - in_window[1]["title_sim"] >= rival_margin):
        return {"decision": "auto", "best": best, "shortlist": in_window[:5]}

    plausible = [c for c in in_window if c["title_sim"] >= min_sim]
    if plausible:
        return {"decision": "review", "best": plausible[0], "shortlist": plausible[:5]}
    return {"decision": "none", "shortlist": in_window[:5]}


def _strip_filename_noise(name: str) -> str:
    """Clean a filename stem into a search query: drop a trailing
    " (MM.DD.YY)" date suffix and common quality/junk tags."""
    import re as _re
    s = os.path.splitext(name)[0]
    s = _re.sub(r"\s*\(\d{2}\.\d{2}\.\d{2}\)\s*$", "", s)      # (05.11.23)
    s = _re.sub(r"\s*[\[(](?:\d{3,4}p|hd|4k|60fps)[\])]\s*", " ", s, flags=_re.I)
    return s.strip()


# ── IO: ffprobe local file, yt-dlp search ───────────────────────────────

def _probe_local(ffprobe: str, filepath: str) -> dict[str, Any]:
    """Return {duration, title, date} read from the container via ffprobe.
    title/date come from the embedded format tags (survive renames)."""
    out: dict[str, Any] = {"duration": None, "title": "", "date": ""}
    try:
        r = subprocess.run(
            [ffprobe, "-v", "quiet", "-print_format", "json", "-show_format", filepath],
            capture_output=True, text=True, timeout=30,
            creationflags=_CREATE_NO_WINDOW)
        fmt = (json.loads(r.stdout or "{}").get("format") or {})
        tags = {k.lower(): v for k, v in (fmt.get("tags") or {}).items()}
        try:
            out["duration"] = float(fmt.get("duration")) if fmt.get("duration") else None
        except (TypeError, ValueError):
            out["duration"] = None
        out["title"] = (tags.get("title") or "").strip()
        out["date"] = (tags.get("date") or tags.get("creation_time") or "").strip()
    except Exception as e:
        _log.debug("ffprobe local probe failed for %r: %s", filepath, e)
    return out


def _ytsearch(yt: str, query: str, n: int,
              cancel_event: threading.Event | None = None) -> list[dict[str, Any]]:
    """yt-dlp `ytsearchN:<query>` -> candidate dicts (id/title/duration/channel)."""
    from ..sync.ytdlp_proc import _find_cookie_source
    if not query.strip():
        return []
    cmd = [yt, f"ytsearch{n}:{query}", "--flat-playlist",
           "--no-warnings", "--ignore-errors",
           "--print", "%(id)s\t%(title)s\t%(duration)s\t%(channel)s\t%(view_count)s",
           *_find_cookie_source()]
    out: list[dict[str, Any]] = []
    try:
        proc = subprocess.Popen(
            cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, encoding="utf-8", errors="replace",
            creationflags=_CREATE_NO_WINDOW)
    except OSError:
        return out
    try:
        with proc.stdout:
            for line in proc.stdout:
                if cancel_event is not None and cancel_event.is_set():
                    try: proc.terminate()
                    except Exception as e: swallow("ytsearch cancel terminate", e)
                    break
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 4:
                    continue
                vid, title, dur, channel = parts[0], parts[1], parts[2], parts[3]
                if len(vid) != 11:
                    continue
                try:
                    dur_f = float(dur) if dur not in ("", "NA", "None") else None
                except (TypeError, ValueError):
                    dur_f = None
                view_count = None
                if len(parts) >= 5:
                    try:
                        vc_raw = (parts[4] or "").replace(",", "").strip()
                        if vc_raw not in ("", "NA", "None"):
                            view_count = int(float(vc_raw))
                    except (TypeError, ValueError):
                        view_count = None
                out.append({"id": vid, "title": title, "duration": dur_f,
                            "channel": channel, "view_count": view_count})
        try: proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.terminate()
    except Exception as e:
        _log.debug("ytsearch read failed: %s", e)
        try: proc.terminate()
        except Exception as ee: swallow("ytsearch terminate", ee)
    return out


def _persist_review(item: dict[str, Any]) -> None:
    try:
        REVIEW_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(REVIEW_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    except Exception as e:
        _log.debug("persist review item failed: %s", e)


# ── Orchestrator ────────────────────────────────────────────────────────

def backfill_manual_video_ids(stream, *,
                              cancel_event: threading.Event | None = None,
                              pause_event: threading.Event | None = None,
                              dry_run: bool = False,
                              limit: int | None = None) -> dict[str, Any]:
    """Walk the no-ID manual downloads and try to recover each one's video_id.

    AUTO matches are written to the index immediately (unless `dry_run`);
    REVIEW matches are appended to REVIEW_FILE for the picker; NONE are
    stamped tried. Streams a progress line per video. Honors cancel/pause and
    backs off on YouTube 429s.
    """
    from ..process_runner import find_yt_dlp, find_ffprobe
    from .. import index as _idx

    def _emit(text, tag="dim"):
        try: stream.emit_text(text, tag)
        except Exception as e: swallow("manual-backfill emit", e)

    yt = find_yt_dlp()
    ffprobe = find_ffprobe()
    if not yt:
        _emit(" — Recover IDs: yt-dlp not found.", "red"); return {"ok": False, "error": "yt-dlp not found"}
    if not ffprobe:
        _emit(" — Recover IDs: ffprobe not found.", "red"); return {"ok": False, "error": "ffprobe not found"}

    # Work list = the actual single-download FILES on disk (the backlog often
    # isn't in the index — it was grabbed years ago by another tool), PLUS any
    # indexed manual videos still missing an id. Skip ones already resolved.
    try:
        from ..ytarchiver_config import load_config as _load_cfg
        vod = ((_load_cfg() or {}).get("video_out_dir") or "").strip()
    except Exception:
        vod = ""
    resolved_paths: set[str] = set()
    excluded_paths: set[str] = set()
    try:
        for r in _idx.list_manual_videos(include_thumbs=False):
            key = os.path.normcase(os.path.normpath(r.get("filepath") or ""))
            if not key:
                continue
            if r.get("video_id"):
                resolved_paths.add(key)
            elif r.get("id_backfill_excluded_ts"):
                excluded_paths.add(key)
    except Exception as e:
        _log.debug("resolved-set read failed: %s", e)
    _VIDEO_EXTS = {".mp4", ".webm", ".mkv", ".m4v", ".mov", ".avi"}
    files: list[str] = []
    seen: set[str] = set()
    # (a) Loose files directly in video_out_dir (NON-recursive — never descend
    #     into the channel root, which can sit under video_out_dir).
    if vod and os.path.isdir(vod):
        try:
            for ent in os.scandir(vod):
                if not ent.is_file(follow_symlinks=False):
                    continue
                if os.path.splitext(ent.name)[1].lower() not in _VIDEO_EXTS:
                    continue
                key = os.path.normcase(os.path.normpath(ent.path))
                if key in resolved_paths or key in excluded_paths or key in seen:
                    continue
                seen.add(key)
                files.append(ent.path)
        except OSError as e:
            _log.debug("video_out_dir scan failed: %s", e)
    # (b) Indexed manual videos still missing an id (custom 'Save to' spots).
    try:
        for r in _idx.list_manual_videos_without_id():
            fp = r.get("filepath") or ""
            key = os.path.normcase(os.path.normpath(fp))
            if (not fp or key in resolved_paths or key in excluded_paths
                    or key in seen):
                continue
            seen.add(key)
            files.append(fp)
    except Exception as e:
        _log.debug("index no-id read failed: %s", e)
    if limit:
        files = files[:int(limit)]
    total = len(files)
    # Clear any prior review file so the picker reflects this run.
    if not dry_run:
        try: REVIEW_FILE.unlink(missing_ok=True)
        except Exception as e: swallow("clear review file", e)

    skipped_excluded = len(excluded_paths)
    _emit(f" — Recover IDs: {total} manual download(s) without an ID"
          f"{' (dry run)' if dry_run else ''}…", "simpleline")
    if skipped_excluded:
        _emit(f"   Skipping {skipped_excluded} excluded file(s) that already "
              f"failed ID recovery repeatedly.", "dim")
    resolved = review = none = 0
    newly_excluded = 0
    _last_429 = 0.0

    for i, fp in enumerate(files, 1):
        if cancel_event is not None and cancel_event.is_set():
            _emit(f"   Cancelled after {i - 1}/{total}.", "dim"); break
        while (pause_event is not None and pause_event.is_set()
               and not (cancel_event is not None and cancel_event.is_set())):
            time.sleep(0.4)

        if not fp or not os.path.isfile(fp):
            none += 1
            continue
        probed = _probe_local(ffprobe, fp)
        local_title = probed["title"] or _strip_filename_noise(os.path.basename(fp))
        local_dur = probed["duration"]
        short = (local_title[:55] + "…") if len(local_title) > 58 else local_title

        sidecar_id = ""
        try:
            sidecar_id = _idx._resolve_id_from_sidecars(fp)
        except Exception as e:
            _log.debug("manual sidecar id resolve failed: %s", e)
        if sidecar_id:
            resolved += 1
            _emit(f"   [{i}/{total}] + {short}  ->  {sidecar_id}  "
                  f"(from sidecar)", "simpleline_green")
            if not dry_run:
                try:
                    wrote = _idx.set_manual_video_id(
                        fp, sidecar_id,
                        f"https://www.youtube.com/watch?v={sidecar_id}")
                    if not wrote:
                        _idx.register_video(
                            fp, "Single Videos", local_title,
                            video_id=sidecar_id, duration_secs=local_dur)
                        try:
                            _idx.stamp_manual_id_tried(fp)
                        except Exception as e:
                            swallow("stamp sidecar tried", e)
                except Exception as e:
                    _emit(f"       (index write failed: {e})", "red")
                try:
                    from .fetcher import fetch_single_video_metadata
                    fetch_single_video_metadata(
                        {"name": "Single Videos",
                         "split_years": False, "split_months": False},
                        sidecar_id, fp, local_title, stream,
                        emit_inline_log=False, refresh=True,
                        dest_folder=os.path.dirname(fp))
                except Exception as e:
                    _log.debug("manual sidecar metadata fetch failed: %s", e)
            if cancel_event is None or not cancel_event.is_set():
                time.sleep(min(_PER_SEARCH_SLEEP_S, 0.1))
            continue

        cands = _ytsearch(yt, local_title, SEARCH_N, cancel_event)
        # 429 backoff: an empty result right after others succeeded often = rate
        # limit. Cheap heuristic — sleep through the schedule and retry once.
        if not cands and time.time() - _last_429 > 60:
            for backoff in _RATE_LIMIT_BACKOFFS:
                if cancel_event is not None and cancel_event.is_set():
                    break
                _emit(f"   (possible rate-limit — waiting {backoff}s)", "dim")
                if cancel_event is not None and cancel_event.wait(timeout=backoff):
                    break
                cands = _ytsearch(yt, local_title, SEARCH_N, cancel_event)
                if cands:
                    _last_429 = time.time(); break

        d = decide(local_title, local_dur, cands)
        decision = d["decision"]
        if decision == "auto":
            best = d["best"]
            resolved += 1
            _emit(f"   [{i}/{total}] ✓ {short}  →  {best['id']}  "
                  f"(±{best['dur_delta']}s, title {best['title_sim']:.2f})",
                  "simpleline_green")
            if not dry_run:
                # Fill the id onto an existing index row; if the file isn't in
                # the index yet (the typical backlog case), register it fresh
                # with the matched video's channel/title. Either way it ends up
                # indexed with its recovered id.
                try:
                    wrote = _idx.set_manual_video_id(
                        fp, best["id"],
                        f"https://www.youtube.com/watch?v={best['id']}",
                        channel=best.get("channel") or "")
                    if not wrote:
                        _idx.register_video(
                            fp, best.get("channel") or "Single Videos",
                            best.get("title") or local_title,
                            video_id=best["id"], duration_secs=local_dur)
                        try:
                            _idx.stamp_manual_id_tried(fp)
                        except Exception as e:
                            swallow("stamp auto tried", e)
                except Exception as e:
                    _emit(f"       (index write failed: {e})", "red")
                # Fetch metadata now the id is known (writes the JSONL next to
                # the file, where browse_get_video_metadata will find it).
                try:
                    from .fetcher import fetch_single_video_metadata
                    fetch_single_video_metadata(
                        {"name": best.get("channel") or "Single Videos",
                         "split_years": False, "split_months": False},
                        best["id"], fp, best.get("title") or local_title,
                        stream, emit_inline_log=False, refresh=True,
                        dest_folder=os.path.dirname(fp))
                except Exception as e:
                    _log.debug("manual backfill metadata fetch failed: %s", e)
        elif decision == "review":
            review += 1
            _emit(f"   [{i}/{total}] ? {short}  —  {len(d['shortlist'])} "
                  f"possible match(es), needs review", "simpleline_pink")
            if not dry_run:
                _persist_review({
                    "filepath": fp, "title": local_title,
                    "duration": local_dur, "candidates": d["shortlist"],
                })
        else:
            none += 1
            _emit(f"   [{i}/{total}] ✗ {short}  —  no confident match", "dim")
            if not dry_run:
                try:
                    marked = _idx.mark_manual_id_backfill_failed(
                        fp, title=local_title, duration_secs=local_dur)
                    if marked.get("excluded"):
                        newly_excluded += 1
                        _emit("       Excluded from future Recover IDs runs "
                              f"after {marked.get('fail_count') or 3} misses.",
                              "dim")
                except Exception as e: swallow("stamp tried", e)

        # Pace politely between searches.
        if cancel_event is None or not cancel_event.is_set():
            time.sleep(_PER_SEARCH_SLEEP_S)

    _emit(f" — Recover IDs done: {resolved} resolved, {review} need review, "
          f"{none} no match (of {total}).", "simpleline_green")
    return {"ok": True, "total": total, "resolved": resolved,
            "review": review, "none": none, "dry_run": dry_run,
            "skipped_excluded": skipped_excluded,
            "newly_excluded": newly_excluded}
