"""
Redownload pipeline — YTArchiver.py:10405 (_backlog_redownload_channel) port.

Re-download existing channel videos at a new resolution and replace the old
files. Scans local video files, matches them to YouTube video IDs, downloads
each at the new resolution, and swaps.

Progress is persisted to `<channel_folder>/_redownload_progress.json` so a
cancelled redownload can resume from where it left off.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from .sync import (
    _find_cookie_source, find_yt_dlp,
    channel_folder_name as _ch_folder_name,
)

# YTArchiver uses .mp4/.mkv/.webm + a few audio/edge cases for local scans
_VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".m4a", ".mov")
from .ytarchiver_config import load_config
from .net import block_if_down
from .log_stream import LogStreamer
from .utils import utf8_subprocess_env as _utf8_env


_YT_ID_RE = re.compile(r'\b([A-Za-z0-9_-]{11})\b')
_PAGE_RE = re.compile(r'page (\d+)')
_RES_LADDER_FORMAT = (
    # yt-dlp -f string: bestvideo[height<=H]+bestaudio/best[height<=H]
    "bestvideo[height<={h}]+bestaudio/best[height<={h}]"
)


def _progress_path(folder: str) -> str:
    return os.path.join(folder, "_redownload_progress.json")


def _load_progress(folder: str, ch_url: str, new_res: str) -> Set[str]:
    try:
        pf = _progress_path(folder)
        if not os.path.isfile(pf):
            return set()
        with open(pf, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("resolution") == new_res and data.get("ch_url") == ch_url:
            return set(data.get("done_ids", []))
    except Exception:
        pass
    return set()


def _save_progress(folder: str, ch_url: str, new_res: str, done: Set[str]) -> None:
    try:
        os.makedirs(folder, exist_ok=True)
        pf = _progress_path(folder)
        tmp = pf + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"ch_url": ch_url, "resolution": new_res,
                       "done_ids": list(done)}, f)
        os.replace(tmp, pf)
    except Exception:
        pass


def _clear_progress(folder: str) -> None:
    try:
        pf = _progress_path(folder)
        if os.path.isfile(pf):
            os.remove(pf)
    except Exception:
        pass


def _extract_id_from_filename(name: str) -> Optional[str]:
    """YTArchiver names videos like 'Title [VIDEOID].ext'. Extract VIDEOID if so."""
    # Look for bracketed 11-char token at the end of the filename (before ext).
    stem = os.path.splitext(name)[0]
    # Prefer bracketed form: Title [aBc123_XyZ]
    m = re.search(r'\[([A-Za-z0-9_-]{11})\]\s*$', stem)
    if m:
        return m.group(1)
    # Fallback: any 11-char token
    m = _YT_ID_RE.search(stem)
    return m.group(1) if m else None


def _scan_local_files(folder: str) -> Dict[str, str]:
    """Walk channel folder, return {filename: fullpath} for all video files.

    Skips temp subdirs (_BACKLOG_TEMP, _TEMP_COMPRESS) to match YTArchiver.
    """
    out: Dict[str, str] = {}
    if not os.path.isdir(folder):
        return out
    for dp, dns, fns in os.walk(folder):
        dns[:] = [d for d in dns if d not in ("_BACKLOG_TEMP", "_TEMP_COMPRESS")]
        for f in fns:
            low = f.lower()
            if not low.endswith(_VIDEO_EXTS):
                continue
            if low.endswith(".part"):
                continue
            out[f] = os.path.join(dp, f)
    return out


def _fetch_yt_catalog(ch_url: str, cancel_ev: threading.Event,
                      pause_ev: Optional[threading.Event],
                      stream: LogStreamer) -> Dict[str, str]:
    """Run yt-dlp --flat-playlist to enumerate the channel. Returns {title: id}."""
    yt_dlp = find_yt_dlp() or "yt-dlp"
    enum_cmd = [
        yt_dlp, "--flat-playlist",
        "--print", "%(id)s|||%(title)s",
        "--no-warnings", "--verbose",
    ]
    enum_cmd += _find_cookie_source() or []
    enum_cmd.append(ch_url)
    try:
        proc = subprocess.Popen(
            enum_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
            creationflags=(0x08000000 if os.name == "nt" else 0), # CREATE_NO_WINDOW
            env=_utf8_env(),
        )
    except Exception as e:
        stream.emit([["  \u2014", "simpleline_redwnl"],
                     [f" yt-dlp spawn failed: {e}\n", "red"]])
        return {}
    result: Dict[str, str] = {}
    last_page = 0
    try:
        for raw in proc.stdout:
            if cancel_ev.is_set():
                proc.terminate()
                break
            line = raw.strip()
            if "|||" in line:
                vid_id, title = line.split("|||", 1)
                vid_id = vid_id.strip()
                title = title.strip()
                if re.fullmatch(r'[A-Za-z0-9_-]{11}', vid_id):
                    result[title] = vid_id
            else:
                pm = _PAGE_RE.search(line)
                if pm:
                    pg = int(pm.group(1))
                    if pg >= last_page + 10:
                        last_page = pg
                        stream.emit([["  \u2014", "simpleline_redwnl"],
                                     [f" Scanning catalog (page {pg})\u2026\n",
                                      "simpleline"]])
    finally:
        try:
            proc.wait(timeout=300)
        except Exception:
            try: proc.kill()
            except Exception: pass
    return result


def _build_metadata_index(folder: str) -> Dict[str, Any]:
    """Load all aggregated `.{ch} Metadata.jsonl` files under `folder`
    into three indices for fast matching:
        by_title: {title.lower(): video_id}
        by_date:  {YYYYMMDD: [(video_id, title_orig), ...]}
        by_id:    {video_id: {title, upload_date}}

    Used by `_match_files_to_ids` to catch videos YouTube has renamed
    since original download (local filename stem no longer matches
    the current catalog title) and to match 1-video-per-day channels
    by file mtime alone.
    """
    out = {"by_title": {}, "by_date": {}, "by_id": {}}
    if not folder or not os.path.isdir(folder):
        return out
    for dp, _dns, fns in os.walk(folder):
        for fn in fns:
            if not (fn.startswith(".") and fn.endswith("Metadata.jsonl")):
                continue
            try:
                with open(os.path.join(dp, fn), "r", encoding="utf-8") as f:
                    for ln in f:
                        ln = ln.strip()
                        if not ln:
                            continue
                        try:
                            obj = json.loads(ln)
                        except Exception:
                            continue
                        vid = (obj.get("video_id") or "").strip()
                        title = (obj.get("title") or "").strip()
                        upload_date = str(obj.get("upload_date") or "").strip()
                        if not vid:
                            continue
                        out["by_id"][vid] = {
                            "title": title, "upload_date": upload_date}
                        if title:
                            out["by_title"][title.lower()] = vid
                        if upload_date and len(upload_date) == 8:
                            out["by_date"].setdefault(
                                upload_date, []).append((vid, title))
            except OSError:
                continue
    return out


def _match_files_to_ids(local_files: Dict[str, str],
                         yt_title_to_id: Dict[str, str],
                         meta_index: Optional[Dict[str, Any]] = None
                         ) -> List[Dict[str, str]]:
    """Build list of {filename, filepath, video_id, title} for matched files.

    Matching priority (stop at first hit):
      1. Filename has `[VIDEOID]` token (fast path — no catalog needed)
      2. Stem equals a current YT catalog title exactly (case-insensitive)
      3. Stem equals a LOCAL metadata title exactly — catches videos
         YouTube has renamed since download. The aggregated
         `.{ch} Metadata.jsonl` records each video's title at metadata-
         fetch time; a later YT-side rename leaves the local filename
         matching the OLD title, which only this index can resolve.
      4. File mtime → YYYYMMDD → local metadata's by_date index.
         yt-dlp `--mtime` stamps file mtime to the YouTube upload date,
         so this is a bijection for channels that upload ≤ 1 video/day.
         For higher-frequency channels with multiple videos on one date,
         the step narrows the candidate set and then re-tries substring
         match within that day's entries only.
      5. Substring fallback against the current YT catalog (existing).

    Without steps 3-4 (added because a channel with only 25/41 matched
    by title had mostly rename-drift + rare multi-video days), the
    redownload pipeline silently skipped 16 files.
    """
    matched: List[Dict[str, str]] = []
    yt_lower = {t.lower(): (t, vid) for t, vid in yt_title_to_id.items()}
    meta_by_title = (meta_index or {}).get("by_title") or {}
    meta_by_date = (meta_index or {}).get("by_date") or {}
    meta_by_id = (meta_index or {}).get("by_id") or {}
    for fname, fpath in local_files.items():
        stem = os.path.splitext(fname)[0]
        vid_id = _extract_id_from_filename(fname)
        title = stem
        # 1. [VIDEOID] in filename — fast path
        if vid_id:
            matched.append({"filename": fname, "filepath": fpath,
                            "video_id": vid_id, "title": title})
            continue
        low = stem.lower()
        # 2. Exact title match in YT catalog
        if low in yt_lower:
            t, vid = yt_lower[low]
            matched.append({"filename": fname, "filepath": fpath,
                            "video_id": vid, "title": t})
            continue
        # 3. Exact title match in LOCAL metadata (catches renames)
        if low in meta_by_title:
            vid = meta_by_title[low]
            t_orig = meta_by_id.get(vid, {}).get("title") or stem
            matched.append({"filename": fname, "filepath": fpath,
                            "video_id": vid, "title": t_orig})
            continue
        # 4. MTIME-DATE fallback against local metadata
        try:
            import datetime as _dt
            mt_date = _dt.datetime.fromtimestamp(
                os.path.getmtime(fpath)).strftime("%Y%m%d")
        except OSError:
            mt_date = ""
        if mt_date and mt_date in meta_by_date:
            candidates = meta_by_date[mt_date]
            if len(candidates) == 1:
                vid, t_orig = candidates[0]
                matched.append({"filename": fname, "filepath": fpath,
                                "video_id": vid, "title": t_orig or stem})
                continue
            # Multiple videos that day — pick the best substring match.
            best = None
            for vid, t_orig in candidates:
                t_low = (t_orig or "").lower()
                if t_low and (low in t_low or t_low in low):
                    best = (vid, t_orig)
                    break
            if best:
                vid, t_orig = best
                matched.append({"filename": fname, "filepath": fpath,
                                "video_id": vid, "title": t_orig or stem})
                continue
        # 5. Substring fallback in YT catalog (existing behavior)
        for t_lower, (t_orig, vid) in yt_lower.items():
            if t_lower in low or low in t_lower:
                matched.append({"filename": fname, "filepath": fpath,
                                "video_id": vid, "title": t_orig})
                break
    return matched


def _find_ffprobe() -> str:
    """Locate ffprobe the same way compress.find_ffmpeg locates ffmpeg:
    PATH first, then the app dir + cwd. Returns "ffprobe" as a
    last-resort string so `subprocess.run` fails cleanly (rather than
    silently No-op'ing) when truly absent.

    Before this fix, `_ffprobe_height` hard-coded the bare string
    "ffprobe" which only worked when the exe was on PATH. For users
    running the bundled build without ffmpeg installed system-wide,
    every height probe failed → `_already_at_target` returned False →
    redownload re-fetched every file even when it was already at the
    target resolution, producing "0% smaller" noise (file content
    byte-identical to the original).
    """
    import shutil as _sh
    p = _sh.which("ffprobe") or _sh.which("ffprobe.exe")
    if p:
        return p
    from pathlib import Path as _P
    for c in [
        _P.cwd() / "ffprobe.exe",
        _P(__file__).resolve().parent.parent / "ffprobe.exe",
    ]:
        if c.exists():
            return str(c)
    return "ffprobe"


def _ffprobe_height(filepath: str) -> Optional[int]:
    """Return the video stream's height, or None if ffprobe isn't usable."""
    try:
        r = subprocess.run(
            [_find_ffprobe(), "-v", "error",
             "-select_streams", "v:0",
             "-show_entries", "stream=height",
             "-of", "default=noprint_wrappers=1:nokey=1",
             filepath],
            capture_output=True, text=True, timeout=20,
            creationflags=(0x08000000 if os.name == "nt" else 0),
        )
        out = (r.stdout or "").strip().splitlines()
        if out:
            return int(out[0])
    except Exception:
        pass
    return None


def _already_at_target(filepath: str, new_res: str) -> bool:
    """True if the file's height already meets the target resolution."""
    if new_res == "best":
        return False # "best" means upgrade — always re-fetch
    try:
        target = int(new_res)
    except Exception:
        return False
    h = _ffprobe_height(filepath)
    if h is None:
        return False
    # Allow small deltas (e.g. 1078 is effectively 1080)
    return h >= (target - 8)


def _download_one(video_id: str, new_res: str, out_dir: str,
                  stream: LogStreamer, cancel_ev: threading.Event) -> Optional[str]:
    """Download a single video ID at the target resolution into a temp dir
    keyed by `video_id`. Returns the temp filepath on success, None on
    failure/cancel. The caller `os.replace`s it onto the original filename
    so the name NEVER changes between old and new copies.

    Matches YTArchiver.py:9988 exactly — output is `{temp_dir}/{vid_id}.mp4`
    so the final `os.replace(temp, original_filepath)` preserves the stem
    that OLD's sync + scan logic depend on.
    """
    if cancel_ev.is_set():
        return None
    yt_dlp = find_yt_dlp() or "yt-dlp"
    url = f"https://www.youtube.com/watch?v={video_id}"
    if new_res == "best":
        try:
            from .sync import build_format_string as _bfs
            fmt = _bfs("best")
        except Exception:
            fmt = "bestvideo+bestaudio/best"
    else:
        try:
            from .sync import build_format_string as _bfs
            fmt = _bfs(new_res)
        except Exception:
            fmt = _RES_LADDER_FORMAT.format(h=new_res)
    # Temp dir lives INSIDE the channel folder so we're on the same volume —
    # `os.replace` is atomic on the same filesystem. Matches OLD pattern.
    temp_dir = os.path.join(out_dir, "_REDOWNLOAD_TEMP")
    try:
        os.makedirs(temp_dir, exist_ok=True)
    except OSError:
        pass
    dl_path = os.path.join(temp_dir, f"{video_id}.mp4")
    cmd = [
        yt_dlp,
        "--newline", "--no-quiet",
        "--mtime",
        "--trim-filenames", "200",
        "-f", fmt,
        "--merge-output-format", "mp4",
        "--ppa", "Merger:-c copy",
        "-o", dl_path,
        "--no-download-archive",
        *(_find_cookie_source() or []),
        url,
    ]
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
            creationflags=(0x08000000 if os.name == "nt" else 0),
            env=_utf8_env(),
        )
    except Exception as e:
        stream.emit([["  \u2014", "simpleline_redwnl"],
                     [f" spawn failed for {video_id}: {e}\n", "red"]])
        return None
    dest: Optional[str] = None
    for raw in proc.stdout:
        if cancel_ev.is_set():
            try: proc.terminate()
            except Exception: pass
            break
        line = raw.rstrip()
        if "[Merger]" in line and "Merging formats into" in line:
            m = re.search(r'"([^"]+)"', line)
            if m: dest = m.group(1)
        elif line.startswith("[download] Destination:"):
            dest = line.split("Destination:", 1)[1].strip()
    try: proc.wait(timeout=10)
    except Exception:
        try: proc.kill()
        except Exception: pass
    if proc.returncode != 0:
        return None
    # If yt-dlp produced a different extension after merge (e.g. .mkv),
    # locate the real file. OLD's scan pattern (YTArchiver.py:10035).
    if not os.path.isfile(dl_path):
        try:
            for fn in os.listdir(temp_dir):
                if fn.startswith(video_id) and "_compressed" not in fn:
                    dl_path = os.path.join(temp_dir, fn)
                    break
        except OSError:
            pass
    if not os.path.isfile(dl_path):
        return dest # Fall back to whatever yt-dlp reported
    return dl_path


_SAMPLE_SIZE = 10


def _fmt_mb(size_bytes: float) -> str:
    """Compact MB/GB pretty-print. Mirrors OLD `_fmt_enc_size`."""
    mb = float(size_bytes) / (1024.0 * 1024.0)
    if mb >= 1024:
        return f"{mb / 1024.0:.2f} GB"
    if mb >= 100:
        return f"{mb:.0f} MB"
    return f"{mb:.1f} MB"


def redownload_channel(ch_name: str, ch_url: str, folder: str, new_res: str,
                       stream: LogStreamer,
                       cancel_ev: threading.Event,
                       pause_ev: Optional[threading.Event] = None,
                       confirm_cb: Optional[Callable[[float, str, str, int],
                                                     str]] = None,
                       ) -> Dict[str, Any]:
    """Run the full redownload pipeline synchronously. Returns a summary.

    Caller is responsible for threading + queue bookkeeping.

    `confirm_cb` — optional "check 10 then re-ask" hook. After the first
    10 successful replacements, the pipeline pauses and calls
    `confirm_cb(avg_pct, direction, res_label, sample_n)` where direction
    is "larger" or "smaller". Return value controls the continuation:
      - "continue"  → keep going at the current resolution
      - "cancel"    → stop the redownload cleanly (cancel flag is set)
      - a numeric resolution string like "1080" or "best" → switch
        resolution and re-sample (another 10-video check at the new target)
    If `confirm_cb` is None the sample-check is skipped entirely (matches
    the behavior for resumed runs or runs smaller than the sample size).
    Mirrors OLD's `_show_sample_popup` at YTArchiver.py:10711.
    """
    res_label = "Best" if new_res == "best" else f"{new_res}p"
    stream.emit([["=== Resolution Redownload: ", "header"],
                 [f"{ch_name} ({res_label}) ===\n", "header"]])

    # Pause-on-entry — makes the Resume from a restored pause clean.
    if pause_ev is not None and pause_ev.is_set() and not cancel_ev.is_set():
        stream.emit([
            [f"  \u23F8 ", "pauselog"],
            ["Redownload paused \u2014 click Resume.\n", "pauselog"],
        ])
        while pause_ev.is_set() and not cancel_ev.is_set():
            time.sleep(0.25)
        if not cancel_ev.is_set():
            stream.emit([
                [f"  \u25B6 ", "simpleline_redwnl"],
                ["Redownload resumed.\n", "simpleline"],
            ])

    # 1. Local scan
    local = _scan_local_files(folder)
    if not local:
        stream.emit([["  \u2014", "simpleline_redwnl"],
                     [" No video files found.\n", "simpleline"]])
        return {"ok": True, "done": 0, "skipped": 0, "errors": 0, "total": 0}
    stream.emit([["  \u2014", "simpleline_redwnl"],
                 [f" Found {len(local)} local file(s).\n", "simpleline"]])

    if cancel_ev.is_set():
        return {"ok": False, "cancelled": True}

    # 2. Fetch YouTube catalog (with internet-block protection)
    if not block_if_down(cancel_ev):
        return {"ok": False, "cancelled": True}
    stream.emit([["  \u2014", "simpleline_redwnl"],
                 [" Fetching YouTube video list\u2026\n", "simpleline"]])
    yt_titles = _fetch_yt_catalog(ch_url, cancel_ev, pause_ev, stream)
    if cancel_ev.is_set():
        return {"ok": False, "cancelled": True}
    if not yt_titles:
        stream.emit([["  \u2014", "simpleline_redwnl"],
                     [" YouTube catalog fetch failed.\n", "red"]])
        return {"ok": False, "done": 0, "errors": 1, "total": 0}

    # 3. Match. Load the aggregated `.{ch} Metadata.jsonl` files under
    #    the channel folder to build secondary title + by-date indices.
    #    This rescues rename-drift videos (YouTube changed the title
    #    after we downloaded; local filename still matches the OLD
    #    title stored in our metadata) and enables mtime-date-based
    #    matching for low-upload-frequency channels.
    meta_index = _build_metadata_index(folder)
    matched = _match_files_to_ids(local, yt_titles, meta_index=meta_index)
    stream.emit([["  \u2014", "simpleline_redwnl"],
                 [f" Matched {len(matched)}/{len(local)} files to YouTube IDs.\n",
                  "simpleline"]])

    # 4. Resume support
    done = _load_progress(folder, ch_url, new_res)
    if done:
        stream.emit([["  \u2014", "simpleline_redwnl"],
                     [f" Resuming \u2014 {len(done)} already redownloaded.\n",
                      "simpleline"]])

    total_to_do = max(0, len(matched) - len(done))
    # Sample-and-confirm: only meaningful if the caller provided the cb AND
    # we're not resuming AND the job is big enough that "first 10" is a
    # useful preview. Matches OLD at YTArchiver.py:10683-10692.
    sample_done = bool(done) or total_to_do <= _SAMPLE_SIZE or confirm_cb is None
    sample_results: List[Any] = []  # list of (orig_size, new_size) tuples
    if not sample_done:
        stream.emit([["  \u2014", "simpleline_redwnl"],
                     [f" Matched {total_to_do} file(s). Checking the "
                      f"first {_SAMPLE_SIZE} at {res_label}\u2026\n",
                      "simpleline"]])

    # 5. Per-file redownload
    n_done = 0
    n_skipped = 0
    n_err = 0
    # Track wall-clock start so the completion-or-cancel activity-log
    # entry can report "took Xm Ys" (matches classic's
    # _record_redownload_finish at YTArchiver.py:22678).
    import time as _t
    _t_start = _t.time()
    # Use a mutable list for current target so the sample-confirm branch
    # can rewrite it mid-loop without breaking the closure.
    cur_res = [new_res]
    cur_res_label = [res_label]
    # Active status line — the sticky `[N/total] Redownloading: Ch...`
    # that classic YTArchiver pins at the bottom of the log during a
    # redownload pass (YTArchiver.py:10696 _start_simple_anim).
    # Implementation: emit a line tagged `redwnl_active` (data-inplace
    # marker); before each update we fire a `clear_line` control so
    # the OLD line is REMOVED from the DOM, and the new line appends
    # at the current bottom (below any completed entries emitted since
    # the last active update). Result: the active row always stays
    # at the visual bottom without covering completed entries above.
    import json as _json
    def _emit_active(current_idx: int, total: int):
        # Clear the previous active line first so the new one lands
        # at the current bottom instead of in-place-replacing up-log.
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "redwnl_active"}),
             "__control__"],
        ])
        # Color discipline (per user spec): ONLY [ / ] + "Redownloading:"
        # render in the redownload color; numbers + channel name render
        # white. Matches the visible per-item entry format elsewhere.
        stream.emit([
            ["[", ["simpleline_redwnl", "redwnl_active"]],
            [str(current_idx), ["simpleline", "redwnl_active"]],
            ["/", ["simpleline_redwnl", "redwnl_active"]],
            [str(total), ["simpleline", "redwnl_active"]],
            ["] ", ["simpleline_redwnl", "redwnl_active"]],
            ["Redownloading: ", ["simpleline_redwnl", "redwnl_active"]],
            [f"{ch_name}\u2026\n", ["simpleline", "redwnl_active"]],
        ])

    def _clear_active():
        """Drop the active line when the pass ends (before the
        === Redownload complete === footer)."""
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "redwnl_active"}),
             "__control__"],
        ])

    _live_total = len(matched)
    for idx, item in enumerate(matched):
        if cancel_ev.is_set():
            break
        # Pause loop — emit a one-time "Paused — click Resume" notice
        # when the user hits Pause between files so the log mirrors
        # classic's behavior (YTArchiver.py:8111). The `_was_paused`
        # flag prevents the notice from re-emitting on every 250ms
        # poll; a matching "Resumed" line fires when pause clears.
        if pause_ev is not None and pause_ev.is_set() and not cancel_ev.is_set():
            # Also drop the sticky active-status line so the pause
            # notice isn't sandwiched between "Redownloading..." and
            # later entries.
            _clear_active()
            stream.emit([
                [f"  \u23F8 ", "pauselog"],
                ["Redownload paused \u2014 click Resume.\n", "pauselog"],
            ])
            while pause_ev.is_set() and not cancel_ev.is_set():
                time.sleep(0.25)
            if not cancel_ev.is_set():
                stream.emit([
                    [f"  \u25B6 ", "simpleline_redwnl"],
                    ["Redownload resumed.\n", "simpleline"],
                ])
        vid = item["video_id"]
        fp = item["filepath"]
        file_num = idx + 1
        if vid in done:
            # Previously-redownloaded in this resumed pass. Emit a
            # dim one-liner so the [N/total] sequence stays visible
            # instead of jumping in random increments. Reported gap
            # symptoms: numbers skipped from 1294 to 1300 to 1314
            # with no log between — the missing entries were all
            # silent "already done" skips from the resume list.
            stream.emit([
                ["[", "simpleline_redwnl"],
                [str(file_num), "simpleline"],
                ["/", "simpleline_redwnl"],
                [str(_live_total), "simpleline"],
                ["] ", "simpleline_redwnl"],
                [f"{item['title']} \u2014 already done.\n", "dim"],
            ])
            _emit_active(file_num, _live_total)
            n_skipped += 1
            continue
        if _already_at_target(fp, cur_res[0]):
            # File already at target resolution — skip it. Emit a
            # completed-style `[N/total] filename — already at Xp. skip.`
            # line that STAYS in the log (not marker-tagged), then
            # move on.
            stream.emit([
                ["[", "simpleline_redwnl"],
                [str(file_num), "simpleline"],
                ["/", "simpleline_redwnl"],
                [str(_live_total), "simpleline"],
                ["] ", "simpleline_redwnl"],
                [f"{item['title']} already at {cur_res_label[0]} \u2014 skip.\n",
                 "simpleline"],
            ])
            # Bump the active status line back to the DOM bottom.
            # Reported: the sticky active line stuck in position above
            # newly-emitted completed/skip entries because `_emit_active`
            # only fires at the TOP of each iteration that reaches the
            # download path — skip paths like this one left a stale
            # active line from the PREVIOUS iteration hanging above the
            # skip notice. Re-emit here (and after every other loop-
            # body emit) to restore the "active is always last line"
            # invariant.
            _emit_active(file_num, _live_total)
            done.add(vid)
            _save_progress(folder, ch_url, cur_res[0], done)
            n_skipped += 1
            continue
        # Emit / update the sticky active line for this video.
        _emit_active(file_num, _live_total)
        orig_size = 0
        try:
            orig_size = os.path.getsize(fp)
        except OSError:
            pass
        out_dir = os.path.dirname(fp) or folder
        new_fp = _download_one(vid, cur_res[0], out_dir, stream, cancel_ev)
        if not new_fp or not os.path.isfile(new_fp):
            # Emit a visible error line so the [N/total] sequence
            # doesn't have unexplained gaps — same root-cause fix as
            # the "already done" silent skip above.
            stream.emit([
                ["[", "simpleline_redwnl"],
                [str(file_num), "simpleline"],
                ["/", "simpleline_redwnl"],
                [str(_live_total), "simpleline"],
                ["] ", "simpleline_redwnl"],
                [f"{item['title']} \u2014 download failed.\n", "red"],
            ])
            _emit_active(file_num, _live_total)
            n_err += 1
            continue
        # Preserve the ORIGINAL filename — OLD does `os.replace(temp, fp)`.
        try:
            new_size = 0
            try:
                new_size = os.path.getsize(new_fp)
            except OSError:
                pass
            # Belt-and-suspenders for the `already-at-target` path: if
            # ffprobe missed (returned None and we downloaded anyway) but
            # the resulting file is byte-identical to the original, the
            # video was already at this resolution. Discard the temp
            # file, emit the same "skip" line `_already_at_target` would
            # have, and count it as skipped rather than logging a bogus
            # "0% smaller" re-download entry.
            if (orig_size > 0 and new_size == orig_size
                    and os.path.isfile(fp)):
                _ident = False
                try:
                    # Chunked compare so a 10GB file doesn't try to
                    # live in RAM twice. 256KB chunks is enough that
                    # modern SSDs saturate well before buffer size
                    # becomes the bottleneck.
                    _ident = True
                    with open(fp, "rb") as _a, open(new_fp, "rb") as _b:
                        while True:
                            _ca = _a.read(256 * 1024)
                            _cb = _b.read(256 * 1024)
                            if _ca != _cb:
                                _ident = False
                                break
                            if not _ca:
                                break
                except OSError:
                    _ident = False
                if _ident:
                    try: os.remove(new_fp)
                    except OSError: pass
                    stream.emit([
                        ["[", "simpleline_redwnl"],
                        [str(file_num), "simpleline"],
                        ["/", "simpleline_redwnl"],
                        [str(_live_total), "simpleline"],
                        ["] ", "simpleline_redwnl"],
                        [f"{item['title']} already at {cur_res_label[0]} "
                         f"\u2014 skip.\n", "simpleline"],
                    ])
                    # Re-push the active status line to the DOM bottom.
                    _emit_active(file_num, _live_total)
                    done.add(vid)
                    _save_progress(folder, ch_url, cur_res[0], done)
                    n_skipped += 1
                    continue
            new_ext = os.path.splitext(new_fp)[1].lower()
            orig_ext = os.path.splitext(fp)[1].lower()
            target_fp = fp
            if new_ext and new_ext != orig_ext:
                # Two-phase commit so a failed os.replace doesn't lose
                # the original. Pre-fix path: os.remove(fp) then
                # os.replace(new_fp, target_fp). If replace failed
                # (permission, target exists from concurrent write,
                # etc.) the original was gone and the new file was
                # stranded in temp. Now we rename the original aside
                # first; if replace succeeds, drop the aside; if it
                # fails, rename the aside back.
                target_fp = os.path.splitext(fp)[0] + new_ext
                aside_fp = fp + ".old"
                aside_valid = False
                try:
                    os.rename(fp, aside_fp)
                    aside_valid = True
                except OSError:
                    # Original rename failed — try the old-behavior
                    # unlink path as a fallback (same risk window as
                    # before, no worse). Most likely cause of failure
                    # here is cross-device rename, which a plain
                    # remove also can't fix.
                    try: os.remove(fp)
                    except OSError: pass
                try:
                    os.replace(new_fp, target_fp)
                except OSError:
                    # Replace failed — roll back if we have an aside.
                    if aside_valid:
                        try: os.rename(aside_fp, fp)
                        except OSError: pass
                    raise
                # Replace succeeded — drop the aside.
                if aside_valid:
                    try: os.remove(aside_fp)
                    except OSError: pass
            else:
                os.replace(new_fp, target_fp)
            done.add(vid)
            _save_progress(folder, ch_url, cur_res[0], done)
            n_done += 1
            # Completed entry — TWO lines that stack above the active
            # status line. Format mirrors classic YTArchiver.py:10723+11067:
            #   [N/total] filename.mp4
            #       — ✓ 11.4 MB → 42.7 MB (273% larger)
            # The `[N/total]` + filename line has no inplace marker so it
            # accumulates. The `redwnl_active` line on the iteration's
            # next pass replaces itself via its marker; the completed
            # line stays above because its markerless line was appended
            # BEFORE the new active line emit.
            _fname = os.path.basename(target_fp)
            stream.emit([
                ["[", "simpleline_redwnl"],
                [str(file_num), "simpleline"],
                ["/", "simpleline_redwnl"],
                [str(_live_total), "simpleline"],
                ["] ", "simpleline_redwnl"],
                [f"{_fname}\n", "simpleline"],
            ])
            if orig_size > 0 and new_size > 0:
                sz_ratio = (new_size / orig_size - 1) * 100
                sz_dir = "larger" if sz_ratio >= 0 else "smaller"
                stream.emit([
                    ["    \u2014 \u2713 ", "simpleline_redwnl"],
                    [f"{_fmt_mb(orig_size)} \u2192 "
                     f"{_fmt_mb(new_size)} "
                     f"({abs(sz_ratio):.0f}% {sz_dir})\n",
                     "simpleline"],
                ])
            elif new_size > 0:
                stream.emit([
                    ["    \u2014 \u2713 ", "simpleline_redwnl"],
                    [f"{_fmt_mb(new_size)} "
                     f"(replaced at {cur_res_label[0]})\n",
                     "simpleline"],
                ])
            # Re-push the active status line to the DOM bottom after
            # the completed-entry + size-diff emit pair. Without this,
            # the [N/total] filename line and its size-diff child land
            # BELOW the sticky "Redownloading: <channel>" line,
            # violating the "active line is always last" invariant.
            _emit_active(file_num, _live_total)
            # Track sample stats (only successful replacements with a
            # known orig size). Once we hit _SAMPLE_SIZE we fire the cb.
            if not sample_done and orig_size > 0 and new_size > 0:
                sample_results.append((orig_size, new_size))
                if (len(sample_results) >= _SAMPLE_SIZE
                        and (idx + 1) < len(matched)
                        and not cancel_ev.is_set()
                        and confirm_cb is not None):
                    avg_pct = (sum((n / o - 1) * 100
                                   for o, n in sample_results)
                               / len(sample_results))
                    direction = "larger" if avg_pct >= 0 else "smaller"
                    stream.emit([
                        ["  \u2014", "simpleline_redwnl"],
                        [f" Sample of {len(sample_results)} files complete. "
                         f"Average size: {abs(avg_pct):.0f}% {direction}. "
                         f"Awaiting confirmation\u2026\n",
                         "simpleline"],
                    ])
                    try:
                        choice = confirm_cb(float(avg_pct), direction,
                                            cur_res_label[0],
                                            len(sample_results))
                    except Exception as e:
                        stream.emit([
                            ["  \u2014", "simpleline_redwnl"],
                            [f" sample-confirm callback errored: {e}. "
                             f"Continuing.\n", "red"],
                        ])
                        choice = "continue"
                    if choice == "cancel":
                        stream.emit([
                            ["  \u2014", "simpleline_redwnl"],
                            [" \u26d4 Redownload cancelled by user "
                             "after sample.\n", "red"],
                        ])
                        cancel_ev.set()
                        break
                    elif choice == "continue":
                        stream.emit([
                            ["  \u2014", "simpleline_redwnl"],
                            [f" \u25B6 Continuing redownload at "
                             f"{cur_res_label[0]}.\n", "simpleline"],
                        ])
                        sample_done = True
                    else:
                        # Numeric resolution switch ("1080", "best", etc.)
                        new_target = str(choice).strip().lower()
                        cur_res[0] = new_target
                        cur_res_label[0] = ("Best" if new_target == "best"
                                            else f"{new_target}p")
                        stream.emit([
                            ["  \u2014", "simpleline_redwnl"],
                            [f" \u21bb Switching to {cur_res_label[0]}. "
                             f"Re-sampling\u2026\n", "simpleline"],
                        ])
                        sample_results = []
                        # sample_done stays False so we re-check at new res
        except Exception as e:
            stream.emit([["  \u2014", "simpleline_redwnl"],
                         [f" replace failed: {e}\n", "red"]])
            n_err += 1
    # Remove the temp dir if empty (redownload complete or cancelled clean).
    try:
        _tmp = os.path.join(folder, "_REDOWNLOAD_TEMP")
        if os.path.isdir(_tmp) and not os.listdir(_tmp):
            os.rmdir(_tmp)
    except OSError:
        pass

    # 6. Clear progress only if a full pass completed without cancel
    if not cancel_ev.is_set():
        _clear_progress(folder)

    # Drop the sticky redwnl_active line so the === Redownload
    # complete === footer doesn't sit below a phantom "Redownloading"
    # line that's no longer accurate.
    _clear_active()

    stream.emit([["=== Redownload complete: ", "header"],
                 [f"{n_done} done \u00b7 {n_skipped} skipped \u00b7 {n_err} errors ===\n",
                  "header"]])

    # Persist a [ReDwnl] row to the activity-log history AND push it
    # to the live UI. Fires on both full completion AND cancel
    # (partial). Mirrors classic YTArchiver's
    # _record_redownload_finish (YTArchiver.py:22678).
    #
    # Both emit paths are required: append_history_entry() writes to
    # config['autorun_history'] so the row survives restart, but does
    # NOT push to the running UI. stream.emit_activity() pushes to
    # the live activity-log DOM but does NOT persist. Without the
    # emit_activity call, the [ReDwnl] row only shows up after the
    # next app launch — reported symptom: "no activity log from this
    # channel's redownload" immediately after completion, despite
    # the other recently-completed channels' rows being visible.
    try:
        from . import autorun as _ar
        from datetime import datetime as _dt
        _now = _dt.now()
        _ts = (_now.strftime("%-I:%M%p") if os.name != "nt"
               else _now.strftime("%I:%M%p").lstrip("0")).lower()
        _date = _now.strftime("%b %d").replace(" 0", " ")
        _elapsed = int(_t.time() - _t_start)
        if _elapsed >= 3600:
            _dur = f"took {_elapsed // 3600}h {(_elapsed % 3600) // 60:02d}m"
        elif _elapsed >= 60:
            _dur = f"took {_elapsed // 60}m {_elapsed % 60:02d}s"
        else:
            _dur = f"took {_elapsed}s"
        _ts_date = f"{_ts}, {_date}"
        _ch_part = f" {ch_name} \u2014" if ch_name else ""
        _body = (f"{n_done} replaced \u00b7 "
                 f"{n_skipped} skipped \u00b7 "
                 f"{n_err} errors \u00b7 "
                 f"{_dur}")
        _line = f"[ReDwnl] {_ts_date} \u2014{_ch_part} {_body}"
        _ar.append_history_entry(_line)
        # Live UI push — cells map matches the layout
        # autorun_history_entries_for_ui builds from the persisted
        # line. [ReDwnl] packs its 3 counts into primary / secondary
        # / tertiary so the errors cell can collapse to 0 via the
        # hist-row-ReDwnl CSS override (took snaps tight to counts).
        try:
            stream.emit_activity({
                "kind": "ReDwnl",
                "time_date": _ts_date,
                "channel": ch_name,
                "primary": f"{n_done} replaced",
                "secondary": f"{n_skipped} skipped",
                "tertiary": f"{n_err} errors",
                "errors": "",
                "took": _dur,
                "row_tag": "hist_redwnl" if n_done > 0 else "",
            })
        except Exception:
            pass
    except Exception:
        pass

    return {"ok": True, "done": n_done, "skipped": n_skipped, "errors": n_err,
            "total": len(matched)}
