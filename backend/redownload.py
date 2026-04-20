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
            creationflags=(0x08000000 if os.name == "nt" else 0),  # CREATE_NO_WINDOW
            env=_utf8_env(),
        )
    except Exception as e:
        stream.emit([["[Redwnl]", "redwnl_bracket"],
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
                        stream.emit([["[Redwnl]", "redwnl_bracket"],
                                     [f" Scanning catalog (page {pg})\u2026\n",
                                      "simpleline_redwnl"]])
    finally:
        try:
            proc.wait(timeout=300)
        except Exception:
            try: proc.kill()
            except Exception: pass
    return result


def _match_files_to_ids(local_files: Dict[str, str],
                         yt_title_to_id: Dict[str, str]) -> List[Dict[str, str]]:
    """Build list of {filename, filepath, video_id, title} for matched files.

    Matching priority:
      1. Filename has `[VIDEOID]` token (fast path — no yt catalog needed)
      2. Stem (no ext) equals a yt title exactly
      3. Substring match (stem contains or is contained by a yt title)
    """
    matched: List[Dict[str, str]] = []
    # Normalize yt titles to lower for fuzzy match
    yt_lower = {t.lower(): (t, vid) for t, vid in yt_title_to_id.items()}
    for fname, fpath in local_files.items():
        stem = os.path.splitext(fname)[0]
        vid_id = _extract_id_from_filename(fname)
        title = stem
        if vid_id:
            matched.append({"filename": fname, "filepath": fpath,
                            "video_id": vid_id, "title": title})
            continue
        # Fuzzy by title
        low = stem.lower()
        if low in yt_lower:
            t, vid = yt_lower[low]
            matched.append({"filename": fname, "filepath": fpath,
                            "video_id": vid, "title": t})
            continue
        # Substring fallback
        for t_lower, (t_orig, vid) in yt_lower.items():
            if t_lower in low or low in t_lower:
                matched.append({"filename": fname, "filepath": fpath,
                                "video_id": vid, "title": t_orig})
                break
    return matched


def _ffprobe_height(filepath: str) -> Optional[int]:
    """Return the video stream's height, or None if ffprobe isn't usable."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error",
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
        return False  # "best" means upgrade — always re-fetch
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
        stream.emit([["[Redwnl]", "redwnl_bracket"],
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
        return dest  # Fall back to whatever yt-dlp reported
    return dl_path


def redownload_channel(ch_name: str, ch_url: str, folder: str, new_res: str,
                       stream: LogStreamer,
                       cancel_ev: threading.Event,
                       pause_ev: Optional[threading.Event] = None,
                       ) -> Dict[str, Any]:
    """Run the full redownload pipeline synchronously. Returns a summary.

    Caller is responsible for threading + queue bookkeeping.
    """
    res_label = "Best" if new_res == "best" else f"{new_res}p"
    stream.emit([["=== Resolution Redownload: ", "header"],
                 [f"{ch_name} ({res_label}) ===\n", "header"]])

    # Pause-on-entry — makes the ✓Resume from a restored pause clean.
    if pause_ev is not None and pause_ev.is_set() and not cancel_ev.is_set():
        stream.emit([["[Redwnl]", "redwnl_bracket"],
                     [" Paused \u2014 click Resume.\n", "pauselog"]])
        while pause_ev.is_set() and not cancel_ev.is_set():
            time.sleep(0.25)

    # 1. Local scan
    local = _scan_local_files(folder)
    if not local:
        stream.emit([["[Redwnl]", "redwnl_bracket"],
                     [" No video files found.\n", "simpleline_redwnl"]])
        return {"ok": True, "done": 0, "skipped": 0, "errors": 0, "total": 0}
    stream.emit([["[Redwnl]", "redwnl_bracket"],
                 [f" Found {len(local)} local file(s).\n", "simpleline_redwnl"]])

    if cancel_ev.is_set():
        return {"ok": False, "cancelled": True}

    # 2. Fetch YouTube catalog (with internet-block protection)
    if not block_if_down(cancel_ev):
        return {"ok": False, "cancelled": True}
    stream.emit([["[Redwnl]", "redwnl_bracket"],
                 [" Fetching YouTube video list\u2026\n", "simpleline_redwnl"]])
    yt_titles = _fetch_yt_catalog(ch_url, cancel_ev, pause_ev, stream)
    if cancel_ev.is_set():
        return {"ok": False, "cancelled": True}
    if not yt_titles:
        stream.emit([["[Redwnl]", "redwnl_bracket"],
                     [" YouTube catalog fetch failed.\n", "red"]])
        return {"ok": False, "done": 0, "errors": 1, "total": 0}

    # 3. Match
    matched = _match_files_to_ids(local, yt_titles)
    stream.emit([["[Redwnl]", "redwnl_bracket"],
                 [f" Matched {len(matched)}/{len(local)} files to YouTube IDs.\n",
                  "simpleline_redwnl"]])

    # 4. Resume support
    done = _load_progress(folder, ch_url, new_res)
    if done:
        stream.emit([["[Redwnl]", "redwnl_bracket"],
                     [f" Resuming \u2014 {len(done)} already redownloaded.\n",
                      "simpleline_redwnl"]])

    # 5. Per-file redownload
    n_done = 0
    n_skipped = 0
    n_err = 0
    for item in matched:
        if cancel_ev.is_set():
            break
        # Pause loop
        if pause_ev is not None:
            while pause_ev.is_set() and not cancel_ev.is_set():
                time.sleep(0.25)
        vid = item["video_id"]
        if vid in done:
            n_skipped += 1
            continue
        fp = item["filepath"]
        if _already_at_target(fp, new_res):
            stream.emit([["[Redwnl]", "redwnl_bracket"],
                         [f" {item['title']} already at {res_label} \u2014 skip.\n",
                          "simpleline_redwnl"]])
            done.add(vid)
            _save_progress(folder, ch_url, new_res, done)
            n_skipped += 1
            continue
        stream.emit([["[Redwnl]", "redwnl_bracket"],
                     [f" {item['title']} \u2192 {res_label}\u2026\n",
                      "simpleline_redwnl"]])
        out_dir = os.path.dirname(fp) or folder
        new_fp = _download_one(vid, new_res, out_dir, stream, cancel_ev)
        if not new_fp or not os.path.isfile(new_fp):
            n_err += 1
            continue
        # Preserve the ORIGINAL filename — OLD does `os.replace(temp, fp)`.
        # This way OLD's sync + title-based scans never see a rename even
        # if YouTube changed the video's title since our local copy.
        try:
            # If the new file extension differs (e.g. original was .mkv, new
            # is .mp4), adjust the target filename to match the new extension
            # so we don't leave both files on disk.
            new_ext = os.path.splitext(new_fp)[1].lower()
            orig_ext = os.path.splitext(fp)[1].lower()
            target_fp = fp
            if new_ext and new_ext != orig_ext:
                target_fp = os.path.splitext(fp)[0] + new_ext
                # Remove the old-extension file so we don't leave a duplicate.
                try: os.remove(fp)
                except OSError: pass
            os.replace(new_fp, target_fp)
            done.add(vid)
            _save_progress(folder, ch_url, new_res, done)
            n_done += 1
        except Exception as e:
            stream.emit([["[Redwnl]", "redwnl_bracket"],
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

    stream.emit([["=== Redownload complete: ", "header"],
                 [f"{n_done} done \u00b7 {n_skipped} skipped \u00b7 {n_err} errors ===\n",
                  "header"]])
    return {"ok": True, "done": n_done, "skipped": n_skipped, "errors": n_err,
            "total": len(matched)}
