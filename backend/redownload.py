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
from collections.abc import Callable
from typing import Any

from .sync import (
    _find_cookie_source,
    find_yt_dlp,
)

# YTArchiver uses .mp4/.mkv/.webm + a few audio/edge cases for local scans
_VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".m4a", ".mov")
from .log import get_logger
from .log_stream import LogStreamer
from .net import block_if_down
from .utils import sampled_files_equal
from .utils import utf8_subprocess_env as _utf8_env

_log = get_logger(__name__)


_YT_ID_RE = re.compile(r'\b([A-Za-z0-9_-]{11})\b')
_PAGE_RE = re.compile(r'page (\d+)')
_RES_LADDER_FORMAT = (
    # yt-dlp -f string: bestvideo[height<=H]+bestaudio/best[height<=H]
    "bestvideo[height<={h}]+bestaudio/best[height<={h}]"
)


def _progress_path(folder: str) -> str:
    return os.path.join(folder, "_redownload_progress.json")


def _legacy_broken_counts_path(folder: str) -> str:
    return os.path.join(folder, "_redownload_broken_counts.json")


def _coerce_broken_counts(data: Any) -> dict[str, int]:
    if not isinstance(data, dict):
        return {}
    return {str(k): int(v) for k, v in data.items()
            if isinstance(v, (int, float))}


def _load_legacy_broken_counts(folder: str) -> dict[str, int]:
    try:
        pf = _legacy_broken_counts_path(folder)
        if not os.path.isfile(pf):
            return {}
        with open(pf, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        return _coerce_broken_counts(data)
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return {}


def _load_progress_state(
        folder: str, ch_url: str, new_res: str
) -> tuple[set[str], dict[str, int]]:
    done: set[str] = set()
    broken_counts: dict[str, int] = {}
    try:
        pf = _progress_path(folder)
        if os.path.isfile(pf):
            with open(pf, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            if (data.get("resolution") == new_res
                    and data.get("ch_url") == ch_url):
                done = set(data.get("done_ids", []))
                broken_counts = _coerce_broken_counts(
                    data.get("broken_counts"))
    except Exception as e:
        _log.debug("swallowed: %s", e)
    # Migration path for pre-T301 runs. We keep reading the legacy sidecar,
    # but new writes fold the counts into _redownload_progress.json.
    broken_counts.update(_load_legacy_broken_counts(folder))
    return done, broken_counts


def _load_progress(folder: str, ch_url: str, new_res: str) -> set[str]:
    done, _broken_counts = _load_progress_state(folder, ch_url, new_res)
    return done


def _save_progress(folder: str, ch_url: str, new_res: str, done: set[str],
                   broken_counts: dict[str, int] | None = None) -> bool:
    """Persist the redownload progress dict atomically. Returns True on
    success, False on failure (caller can surface a warning when False
    so the user knows resume might re-do already-completed videos —
    audit: redownload.py:935-959)."""
    try:
        os.makedirs(folder, exist_ok=True)
        pf = _progress_path(folder)
        tmp = pf + ".tmp"
        payload = {"ch_url": ch_url, "resolution": new_res,
                   "done_ids": list(done)}
        if broken_counts is not None:
            payload["broken_counts"] = dict(broken_counts)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        os.replace(tmp, pf)
        return True
    except Exception as e:
        _log.debug("swallowed: %s", e)
        return False


def _clear_progress(folder: str) -> None:
    try:
        for pf in (_progress_path(folder), _legacy_broken_counts_path(folder)):
            if os.path.isfile(pf):
                os.remove(pf)
    except Exception as e:
        _log.debug("swallowed: %s", e)


def _extract_id_from_filename(name: str, known_ids=None) -> str | None:
    """YTArchiver names videos like 'Title [VIDEOID].ext'. Extract VIDEOID if so."""
    # Look for bracketed 11-char token at the end of the filename (before ext).
    stem = os.path.splitext(name)[0]
    # Prefer bracketed form: Title [aBc123_XyZ]
    m = re.search(r'\[([A-Za-z0-9_-]{11})\]\s*$', stem)
    if m:
        return m.group(1)
    # Fallback: any bare 11-char token — but when the caller supplies
    # known_ids, only trust a candidate that actually exists in the
    # catalog/metadata. Plain 11-letter words ('programming',
    # 'information') match the shape; trusting them short-circuited
    # the title-match ladder and produced permanent bogus-id download
    # failures — with a nonzero chance of a wrong-video replace.
    m = _YT_ID_RE.search(stem)
    if not m:
        return None
    cand = m.group(1)
    if known_ids is not None and cand not in known_ids:
        return None
    return cand


def _scan_local_files(folder: str) -> dict[str, str]:
    """Walk channel folder, return {filename: fullpath} for all video files.

    Skips temp subdirs (_BACKLOG_TEMP, _TEMP_COMPRESS, _REDOWNLOAD_TEMP) so
    orphan downloads from a previous failed redownload pass don't get
    matched + promoted into the archive as if they were originals.
    """
    out: dict[str, str] = {}
    if not os.path.isdir(folder):
        return out
    for dp, dns, fns in os.walk(folder):
        dns[:] = [d for d in dns if d not in (
            "_BACKLOG_TEMP", "_TEMP_COMPRESS", "_REDOWNLOAD_TEMP")]
        for f in fns:
            low = f.lower()
            if not low.endswith(_VIDEO_EXTS):
                continue
            if low.endswith(".part"):
                continue
            out[f] = os.path.join(dp, f)
    return out


def _fetch_yt_catalog(ch_url: str, cancel_ev: threading.Event,
                      pause_ev: threading.Event | None,
                      stream: LogStreamer,
                      queues=None) -> dict[str, str]:
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
                     [f" Couldn't start the download tool: {e}\n", "red"]])
        return {}
    result: dict[str, str] = {}
    last_page = 0
    try:
        for raw in proc.stdout:
            if cancel_ev.is_set():
                proc.terminate()
                break
            # honor pause_ev during the catalog walk. Without
            # this, a paused user has to wait until the full playlist
            # enumeration completes (minutes on a 10k-video channel)
            # before the pause actually acts. Mirrors the pause-wait
            # pattern used in the download loop at lines ~651-666.
            if pause_ev is not None and pause_ev.is_set():
                if queues is not None:
                    try: queues.set_sync_paused_active(True)
                    except Exception as e: _log.debug("swallowed: %s", e)
                while (pause_ev.is_set() and not cancel_ev.is_set()):
                    time.sleep(0.25)
                if queues is not None:
                    try: queues.set_sync_paused_active(False)
                    except Exception as e: _log.debug("swallowed: %s", e)
            if cancel_ev.is_set():
                # Close stdout BEFORE terminate so a full pipe doesn't
                # leave the child blocked on write while we wait for
                # it to die (audit: redownload.py:152-188). Without
                # this, terminate() on Windows could hang for the
                # full 300s wait-timeout.
                try:
                    if proc.stdout is not None:
                        proc.stdout.close()
                except Exception: pass
                try: proc.terminate()
                except Exception: pass
                break
            line = raw.strip()
            if "|||" in line:
                vid_id, title = line.split("|||", 1)
                vid_id = vid_id.strip()
                title = title.strip()
                if re.fullmatch(r'[A-Za-z0-9_-]{11}', vid_id):
                    _prev = result.get(title)
                    if _prev is not None and _prev != vid_id:
                        # Duplicate title with a DIFFERENT id (weekly
                        # shows, reuploads, 'Q&A'): last-wins used to
                        # silently bind title-matched files to whichever
                        # id enumerated last — the wrong-content-replace
                        # class. Empty string = ambiguous sentinel; the
                        # matcher refuses exact-title binding for these.
                        result[title] = ""
                    elif _prev is None:
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
            except Exception as e: _log.debug("swallowed: %s", e)
            # Second wait after kill so Windows releases the handle
            # immediately rather than leaking it until GC.
            try: proc.wait(timeout=5)
            except Exception as e: _log.debug("swallowed: %s", e)
    return result


def _build_metadata_index(folder: str) -> dict[str, Any]:
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
    # count JSONL-parse failures per file so a corrupt
    # metadata file doesn't silently drop half its entries from the
    # match index. If any file has notable corruption, emit a dim
    # warning so the user can investigate before redownload runs
    # against an incomplete match set.
    _bad_by_file: dict[str, int] = {}
    _total_by_file: dict[str, int] = {}
    for dp, _dns, fns in os.walk(folder):
        for fn in fns:
            if not (fn.startswith(".") and fn.endswith("Metadata.jsonl")):
                continue
            _full = os.path.join(dp, fn)
            try:
                # utf-8-sig so an externally-edited jsonl with a UTF-8
                # BOM doesn't drop its first line on read (audit:
                # redownload.py:215-244).
                with open(_full, "r", encoding="utf-8-sig") as f:
                    for ln in f:
                        ln = ln.strip()
                        if not ln:
                            continue
                        _total_by_file[_full] = _total_by_file.get(_full, 0) + 1
                        try:
                            obj = json.loads(ln)
                        except Exception:
                            _bad_by_file[_full] = _bad_by_file.get(_full, 0) + 1
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
    # Surface parse stats via print (log_stream isn't available here;
    # caller can decide whether to convert to a log line if desired).
    for _fp, _bad in _bad_by_file.items():
        _total = _total_by_file.get(_fp, 0)
        if _total > 0 and _bad > 0:
            print(f"[redownload] {_fp}: {_bad}/{_total} JSONL lines corrupt "
                  f"(matched video IDs may be incomplete)")
    return out


def _match_files_to_ids(local_files: dict[str, str],
                         yt_title_to_id: dict[str, str],
                         meta_index: dict[str, Any] | None = None
                         ) -> list[dict[str, str]]:
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
    matched: list[dict[str, str]] = []
    # Bug [22]: NFC-normalize before lowercasing so non-ASCII titles
    # match across composed/decomposed Unicode forms. yt-dlp emits NFC
    # but some filesystems (notably macOS HFS+) store filenames as NFD;
    # without normalization, "Café" (NFC) on disk wouldn't match "Café"
    # (NFD) from the YouTube catalog. Note: we use NFC, not norm_ascii,
    # so non-Latin titles (Japanese, Cyrillic, Arabic) still compare
    # correctly instead of getting stripped to empty strings.
    import unicodedata as _ud
    def _norm(s: str) -> str:
        try:
            return _ud.normalize("NFC", s).lower()
        except Exception:
            return s.lower()
    yt_lower = {_norm(t): (t, vid) for t, vid in yt_title_to_id.items()}
    meta_by_title = (meta_index or {}).get("by_title") or {}
    # Re-key meta_by_title with the same normalization so step 3 lookups
    # use a consistent form. Safe to do here because meta_by_title was
    # built upstream from arbitrary strings; we normalize on read.
    meta_by_title_norm = {_norm(k): v for k, v in meta_by_title.items()}
    meta_by_date = (meta_index or {}).get("by_date") or {}
    meta_by_id = (meta_index or {}).get("by_id") or {}
    # Bare (un-bracketed) 11-char tokens are only trusted as IDs when
    # they exist in the catalog or local metadata — see
    # _extract_id_from_filename. ("" is the catalog's ambiguous-title
    # sentinel, never a real id.)
    _known_ids = (set(yt_title_to_id.values()) | set(meta_by_id.keys())) - {""}
    # Ambiguous-title census for the LOCAL metadata side: by_title is
    # last-wins-clobbered at build time, but by_id keeps every record,
    # so collisions are recoverable here. A normalized title shared by
    # two different ids must never exact-match ("rather missing than
    # incorrect" — binding to the survivor replaces a file with the
    # OTHER video's content).
    from collections import Counter as _Counter
    _meta_title_counts = _Counter(
        _norm((rec or {}).get("title") or "")
        for rec in meta_by_id.values())
    _ambiguous_meta = {k for k, c in _meta_title_counts.items()
                       if c > 1 and k}
    # Loose form for step-4 title confirmation: strips the Windows-
    # illegal chars (and their fullwidth lookalikes) that filename
    # sanitization removes/replaces, so "Title: Part 2" (YT) still
    # compares equal to "Title Part 2" (disk).
    _illegal_re = re.compile(
        r'[<>:"/\\|?*：？＂｜＜＞＊⧸⁄]')
    def _loose(s: str) -> str:
        s = _illegal_re.sub(" ", _norm(s))
        return re.sub(r"\s+", " ", s).strip()
    for fname, fpath in local_files.items():
        stem = os.path.splitext(fname)[0]
        vid_id = _extract_id_from_filename(fname, _known_ids)
        title = stem
        # 1. [VIDEOID] in filename — fast path
        if vid_id:
            matched.append({"filename": fname, "filepath": fpath,
                            "video_id": vid_id, "title": title})
            continue
        low = _norm(stem)
        # 2. Exact title match in YT catalog. vid == "" is the
        # duplicate-title sentinel from _fetch_yt_catalog — two videos
        # share this title, so exact-title binding would be a coin
        # flip; fall through to the date matcher instead.
        if low in yt_lower:
            t, vid = yt_lower[low]
            if vid:
                matched.append({"filename": fname, "filepath": fpath,
                                "video_id": vid, "title": t})
                continue
            _log.warning("redownload: %r title is ambiguous in the YT "
                         "catalog (multiple ids) — not title-matching", fname)
        # 3. Exact title match in LOCAL metadata (catches renames) —
        # refused when the census above shows the title is shared by
        # more than one id.
        if low in meta_by_title_norm and low not in _ambiguous_meta:
            vid = meta_by_title_norm[low]
            t_orig = meta_by_id.get(vid, {}).get("title") or stem
            matched.append({"filename": fname, "filepath": fpath,
                            "video_id": vid, "title": t_orig})
            continue
        # 4. MTIME-DATE fallback against local metadata. The date is
        # computed in UTC to match YouTube's upload_date convention —
        # yt-dlp --mtime stamps the UTC upload timestamp, and the old
        # LOCAL-time conversion shifted any 00:00-~06:00-UTC upload to
        # the previous day, binding the file to the adjacent day's
        # video with no title check at all.
        try:
            import datetime as _dt
            mt_date = _dt.datetime.fromtimestamp(
                os.path.getmtime(fpath),
                tz=_dt.UTC).strftime("%Y%m%d")
        except OSError:
            mt_date = ""
        if mt_date and mt_date in meta_by_date:
            candidates = meta_by_date[mt_date]
            stem_loose = _loose(stem)
            if len(candidates) == 1:
                vid, t_orig = candidates[0]
                t_loose = _loose(t_orig or "")
                # Date alone is too weak to bind — require the titles
                # to agree once sanitization differences are stripped.
                if stem_loose and t_loose and (
                        stem_loose == t_loose
                        or stem_loose in t_loose
                        or t_loose in stem_loose):
                    matched.append({"filename": fname, "filepath": fpath,
                                    "video_id": vid,
                                    "title": t_orig or stem})
                    continue
            else:
                # Multiple videos that day — pick the best loose-title
                # match (loose form so punctuated titles still hit).
                best = None
                for vid, t_orig in candidates:
                    t_loose = _loose(t_orig or "")
                    if t_loose and stem_loose and (
                            stem_loose in t_loose or t_loose in stem_loose):
                        best = (vid, t_orig)
                        break
                if best:
                    vid, t_orig = best
                    matched.append({"filename": fname, "filepath": fpath,
                                    "video_id": vid,
                                    "title": t_orig or stem})
                    continue
        # the previous step-5 substring fallback
        # ("if t_lower in low or low in t_lower") matched too aggressively
        # on common-prefix titles like "Episode 1" vs "Episode 11" and
        # caused _download_one to overwrite files with the WRONG video's
        # content. The 4 strategies above are safe; falling through here
        # means we genuinely couldn't identify the file — skip it.
        # Log so the user sees previously-silent skips.
        try:
            _log.warning("redownload: no safe match for %r — skipping",
                         fname)
        except Exception:
            pass
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


def _ffprobe_height(filepath: str) -> int | None:
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
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return None


def _height_from_metadata_jsonl(filepath: str) -> int | None:
    """Patch 20 (v67.8): read the video's height from the aggregated
    .Metadata.jsonl sidecar that already lives next to the file, instead
    of spawning ffprobe (~50-200 ms × N at the start of every redownload).

    The .Metadata.jsonl is written by sync/backfill and carries every
    YT-side field, including `height` (when bulk_refresh_views_likes has
    visited the video) or — older entries — the max video-format height.

    Returns None if no jsonl is present or no height field is found.
    Caller falls back to ffprobe on None.
    """
    try:
        import json as _json
        from pathlib import Path as _P
        fp = _P(filepath)
        # The video's Metadata.jsonl lives at the channel-folder level.
        # Walk up the tree looking for `.<channel> Metadata.jsonl` (the
        # canonical no-split name) or any sibling `.* Metadata.jsonl`.
        # We try the parent first (year/month split layout) then up one
        # more (year split) then the channel root.
        for _d in (fp.parent, fp.parent.parent, fp.parent.parent.parent):
            if not _d.is_dir():
                continue
            try:
                cand = list(_d.glob("*Metadata.jsonl")) + \
                       list(_d.glob(".*Metadata.jsonl"))
            except Exception:
                cand = []
            if not cand:
                continue
            # Pull video_id from filename `[id]` bracket
            import re as _re
            m = _re.search(r"\[([A-Za-z0-9_-]{11})\]", fp.name)
            if not m:
                return None
            target_vid = m.group(1)
            for jsonl in cand:
                try:
                    # utf-8-sig handles BOM-prefixed jsonl files (audit:
                    # redownload.py:454).
                    with jsonl.open("r", encoding="utf-8-sig") as f:
                        for line in f:
                            line = line.strip()
                            if not line or '"video_id"' not in line:
                                continue
                            if target_vid not in line:
                                continue
                            try:
                                obj = _json.loads(line)
                            except Exception:
                                continue
                            if (obj.get("video_id") or "") != target_vid:
                                continue
                            h = obj.get("height")
                            if isinstance(h, (int, float)) and h > 0:
                                return int(h)
                            # Older entries may have height only inside the
                            # formats list. Pick the max real video stream
                            # height; formats[0] is often audio-only or the
                            # lowest quality.
                            fmts = obj.get("formats") or []
                            if fmts and isinstance(fmts, list):
                                heights = [
                                    int(f.get("height") or 0)
                                    for f in fmts
                                    if isinstance(f, dict)
                                    and f.get("vcodec") not in (None, "none")
                                    and isinstance(f.get("height"),
                                                   (int, float))
                                    and f.get("height") > 0
                                ]
                                if heights:
                                    return max(heights)
                            return None
                except Exception:
                    continue
            return None  # We found a jsonl but it didn't have this video
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return None


def _already_at_target(filepath: str, new_res: str) -> bool:
    """True if the file's height already meets the target resolution."""
    if new_res == "best":
        return False # "best" means upgrade — always re-fetch
    try:
        target = int(new_res)
    except Exception:
        return False
    # try .Metadata.jsonl first — it's a single line
    # read vs spawning ffprobe (~50-200ms). Fall back to ffprobe on miss.
    h = _height_from_metadata_jsonl(filepath)
    if h is None:
        h = _ffprobe_height(filepath)
    if h is None:
        return False
    # Allow small deltas (e.g. 1078 is effectively 1080)
    return h >= (target - 8)


def _download_one(video_id: str, new_res: str, out_dir: str,
                  stream: LogStreamer, cancel_ev: threading.Event) -> str | None:
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
        # resume partials on restart (same rationale as
        # the sync path). Redownload stages into _REDOWNLOAD_TEMP
        # and the full partial is there to resume from.
        "--continue",
        "--trim-filenames", "200",
        "-f", fmt,
        "--merge-output-format", "mp4",
        "--ppa", "Merger:-c copy",
        "-o", dl_path,
        "--no-download-archive",
        *(_find_cookie_source() or []),
        url,
    ]
    # VERBOSE-ONLY: log the URL, target resolution, format string and
    # full yt-dlp command before we kick the subprocess off. Lets the
    # user reproduce the exact download in a terminal if it fails.
    _cmd_str = " ".join(repr(c) if " " in c or '"' in c else c for c in cmd)
    if len(_cmd_str) > 600:
        _cmd_str = _cmd_str[:600] + "\u2026"
    stream.emit([
        ["    \u2014 ", ["dim"]],
        [f"redownload {video_id} \u00b7 target {new_res}p \u00b7 format `{fmt}`\n",
         ["dim"]],
    ])
    stream.emit([
        ["    \u2014 ", ["dim"]],
        [f"url: {url}\n", ["dim"]],
    ])
    stream.emit([
        ["    \u2014 ", ["dim"]],
        [f"output: {dl_path}\n", ["dim"]],
    ])
    stream.emit([
        ["    \u2014 ", ["dim"]],
        [f"yt-dlp cmd: {_cmd_str}\n", ["dim"]],
    ])
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
            creationflags=(0x08000000 if os.name == "nt" else 0),
            env=_utf8_env(),
        )
    except Exception as e:
        stream.emit([["  \u2014", "simpleline_redwnl"],
                     [f" Couldn't start the download tool for {video_id}: {e}\n",
                      "red"]])
        return None
    dest: str | None = None
    _cancelled = False
    for raw in proc.stdout:
        if cancel_ev.is_set():
            try: proc.terminate()
            except Exception as e: _log.debug("swallowed: %s", e)
            _cancelled = True
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
        except Exception as e: _log.debug("swallowed: %s", e)
        # Second wait after kill so Windows releases the proc handle
        # promptly. Repeated download failures without this leaked
        # one handle per failure until GC (audit: redownload.py:
        # 605-608).
        try: proc.wait(timeout=5)
        except Exception as e: _log.debug("swallowed: %s", e)
    # Patch C: surface non-zero returncode so silent failures are
    # visible. yt-dlp emits its actual error message on stderr, which
    # was being merged into stdout via STDOUT redirection above (line
    # 502) — the body of that output has already been consumed and
    # potentially emitted as dim lines, but the user never saw a
    # summary indicator that "this download failed." Now: if rc != 0
    # AND we weren't cancelled by the user, log an error so the
    # surrounding UI knows to surface a toast/badge.
    if (not _cancelled) and proc.returncode is not None and proc.returncode != 0:
        stream.emit_error(
            f"yt-dlp exited with code {proc.returncode} for "
            f"{video_id} — see lines above for details.")
    # on cancel mid-download, the .part / intermediate files
    # sit in _REDOWNLOAD_TEMP/ forever because the end-of-run rmdir only
    # clears empty dirs. Sweep everything we might have written before
    # returning so cancels don't leak GBs per use.
    if _cancelled:
        # broaden the cancel cleanup to catch every
        # file yt-dlp may have produced — .mkv/.mp4 merge
        # intermediates, .tmp, .ytdl (resume state), .frag (HLS
        # fragments) — not just `video_id*`+`.part`. Redownload
        # temp dir is scratch-only, so a broad listdir-and-delete
        # is safe here; anything legitimately in-flight for
        # another video already lives under its own subdir or
        # file-name prefix so would match the known prefixes. We
        # still restrict the sweep to files NOT currently open
        # by prefix-matching video_id or any of the temp exts.
        _cancel_exts = (".part", ".part-Frag", ".ytdl", ".tmp",
                        ".frag", ".fragment")
        try:
            for fn in os.listdir(temp_dir):
                _full = os.path.join(temp_dir, fn)
                if not os.path.isfile(_full):
                    continue
                if (fn.startswith(video_id)
                        or fn.endswith(_cancel_exts)):
                    try:
                        os.remove(_full)
                    except OSError:
                        pass
        except OSError:
            pass
        return None
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
        # dl_path (the -o forced path) is gone — fall back to yt-dlp's
        # reported `dest`, but ONLY if it's a real file under temp_dir.
        # `dest` is parsed from yt-dlp stdout (influenced by the title
        # template), so apply the same containment guard as the dl_path
        # branch below rather than trusting it blindly (audit: redownload
        # return-dest containment).
        try:
            if dest and os.path.isfile(dest):
                _temp_real = os.path.realpath(temp_dir)
                _dest_real = os.path.realpath(dest)
                if os.path.commonpath([_temp_real, _dest_real]) == _temp_real:
                    return dest
        except (ValueError, OSError):
            pass
        return None
    # Sanity: confirm the resolved path actually lives under temp_dir.
    # A malicious upload_date / title template could try to escape via
    # ../ or absolute path — refuse to return anything outside the
    # temp dir (audit: redownload H105).
    try:
        _temp_real = os.path.realpath(temp_dir)
        _dest_real = os.path.realpath(dl_path)
        if os.path.commonpath([_temp_real, _dest_real]) != _temp_real:
            return None
    except (ValueError, OSError):
        return None
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
                       pause_ev: threading.Event | None = None,
                       confirm_cb: Callable[[float, str, str, int], str] | None = None,
                       queues=None,
                       only_video_id: str = "",
                       ) -> dict[str, Any]:
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
            ["  \u23F8 ", "pauselog"],
            ["Redownload paused \u2014 click Resume.\n", "pauselog"],
        ])
        if queues is not None:
            try: queues.set_sync_paused_active(True)
            except Exception as e: _log.debug("swallowed: %s", e)
        while pause_ev.is_set() and not cancel_ev.is_set():
            time.sleep(0.25)
        if queues is not None:
            try: queues.set_sync_paused_active(False)
            except Exception as e: _log.debug("swallowed: %s", e)
        if not cancel_ev.is_set():
            stream.emit([
                ["  \u25B6 ", "simpleline_redwnl"],
                ["Redownload resumed.\n", "simpleline"],
            ])

    # 1. Local scan
    # when folder is missing or unreadable, distinguish
    # "really no videos" from "wrong folder path". The previous
    # message "No video files found" tripped the user up on channels
    # that obviously had hundreds of files \u2014 the root cause was a
    # mis-built folder argument upstream that pointed at the parent
    # archive root instead of the channel subfolder.
    local = _scan_local_files(folder)
    if not local:
        if not folder:
            stream.emit([["  \u2014", "simpleline_redwnl"],
                         [" Redownload aborted \u2014 channel folder is empty in "
                          "the config. Re-add the channel or fix output_dir.\n",
                          "red"]])
        elif not os.path.isdir(folder):
            stream.emit([["  \u2014", "simpleline_redwnl"],
                         [f" Redownload aborted \u2014 folder not found: "
                          f"{folder}\n", "red"]])
        else:
            stream.emit([["  \u2014", "simpleline_redwnl"],
                         [f" No video files in {folder}.\n", "simpleline"]])
        return {"ok": True, "done": 0, "skipped": 0, "errors": 0, "total": 0}
    stream.emit([["  \u2014", "simpleline_redwnl"],
                 [f" Found {len(local)} local file(s).\n", "simpleline"]])

    if cancel_ev.is_set():
        return {"ok": False, "cancelled": True}

    # 2. Fetch YouTube catalog (with internet-block protection)
    if not block_if_down(stream=stream, check_cancel=cancel_ev.is_set):
        return {"ok": False, "cancelled": True}
    stream.emit([["  \u2014", "simpleline_redwnl"],
                 [" Fetching YouTube video list\u2026\n", "simpleline"]])
    yt_titles = _fetch_yt_catalog(ch_url, cancel_ev, pause_ev, stream,
                                  queues=queues)
    if cancel_ev.is_set():
        return {"ok": False, "cancelled": True}
    if not yt_titles:
        stream.emit([["  \u2014", "simpleline_redwnl"],
                     [" YouTube catalog fetch failed.\n", "red"]])
        # Keep the progress file. The old "stale catalog-vs-progress
        # mapping" rationale for wiping it was wrong: done_ids are
        # immutable YouTube video IDs, independent of any catalog
        # view. Wiping on a transient network blip threw away
        # thousands of completed-video records AND un-quarantined
        # permanently-broken videos (their 3-strike marks live in
        # `done` too), restarting their strike cycles.
        return {"ok": False, "done": 0, "errors": 1, "total": 0}

    # 3. Match. Load the aggregated `.{ch} Metadata.jsonl` files under
    #    the channel folder to build secondary title + by-date indices.
    #    This rescues rename-drift videos (YouTube changed the title
    #    after we downloaded; local filename still matches the OLD
    #    title stored in our metadata) and enables mtime-date-based
    #    matching for low-upload-frequency channels.
    meta_index = _build_metadata_index(folder)
    matched = _match_files_to_ids(local, yt_titles, meta_index=meta_index)
    if only_video_id:
        # Single-video redownload (Watch/Browse "Redownload" on ONE video):
        # keep only the requested id so the per-video button doesn't
        # re-download the ENTIRE channel (audit r2). Reuses the whole tested
        # match/replace/containment pipeline for that one file.
        matched = [m for m in matched
                   if (m.get("video_id") or "") == only_video_id]

    # 4. Resume support
    done, broken_counts = _load_progress_state(folder, ch_url, new_res)
    if done:
        stream.emit([["  \u2014", "simpleline_redwnl"],
                     [f" Resuming \u2014 {len(done)} already redownloaded.\n",
                      "simpleline"]])

    total_to_do = max(0, len(matched) - len(done))
    # Sample-and-confirm: only meaningful if the caller provided the cb AND
    # we're not resuming AND the job is big enough that "first 10" is a
    # useful preview. Matches OLD at YTArchiver.py:10683-10692.
    sample_done = bool(done) or total_to_do <= _SAMPLE_SIZE or confirm_cb is None
    sample_results: list[Any] = []  # list of (orig_size, new_size) tuples
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
                ["  \u23F8 ", "pauselog"],
                ["Redownload paused \u2014 click Resume.\n", "pauselog"],
            ])
            # emit a subtle paused-activity tick every
            # 60s so a long pause doesn't look like a crashed worker.
            # Without this, pausing for 30 min showed no log activity
            # and users thought the app died.
            _paused_since = time.time()
            _last_tick = _paused_since
            if queues is not None:
                try: queues.set_sync_paused_active(True)
                except Exception as e: _log.debug("swallowed: %s", e)
            while pause_ev.is_set() and not cancel_ev.is_set():
                time.sleep(0.25)
                _now = time.time()
                if _now - _last_tick >= 60.0:
                    _mins = int((_now - _paused_since) // 60)
                    try:
                        stream.emit([
                            ["  \u23F8 ", "pauselog"],
                            [f"Still paused ({_mins}m)\u2026\n", "dim"],
                        ])
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                    _last_tick = _now
            if queues is not None:
                try: queues.set_sync_paused_active(False)
                except Exception as e: _log.debug("swallowed: %s", e)
            if not cancel_ev.is_set():
                stream.emit([
                    ["  \u25B6 ", "simpleline_redwnl"],
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
            broken_counts.pop(vid, None)
            _save_progress(folder, ch_url, cur_res[0], done, broken_counts)
            n_skipped += 1
            continue
        # Emit / update the sticky active line for this video.
        _emit_active(file_num, _live_total)
        # Capture the original's height BEFORE anything can replace it
        # — the resolution guard below refuses quality downgrades.
        orig_h = _height_from_metadata_jsonl(fp)
        if orig_h is None:
            orig_h = _ffprobe_height(fp)
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
            # if the new file is drastically smaller than the
            # original (<10% of size), it's almost certainly a broken
            # download (geo-blocked fallback, "video unavailable" stub,
            # members-only error page). Real down-resolution downloads
            # (4K → 480p) still produce files 15-20% of source size.
            # Refuse to replace the original and treat as a download
            # failure instead.
            if (orig_size > 0 and new_size > 0
                    and new_size < (orig_size * 0.10)):
                stream.emit([
                    ["[", "simpleline_redwnl"],
                    [str(file_num), "simpleline"],
                    ["/", "simpleline_redwnl"],
                    [str(_live_total), "simpleline"],
                    ["] ", "simpleline_redwnl"],
                    [f"{item['title']} \u2014 broken download "
                     f"({new_size:,} vs original {orig_size:,} bytes) "
                     f"\u2014 keeping original.\n", "red"],
                ])
                try: os.remove(new_fp)
                except OSError: pass
                # Track broken-download counter in the per-channel
                # _redownload_progress.json so a permanently-broken
                # video (geo-blocked, deleted, region-locked) doesn't
                # retry forever across resume runs. After 3
                # consecutive broken downloads, add to `done` so the
                # next pass skips it (audit: redownload.py:1011-1015).
                try:
                    _bc = int(broken_counts.get(vid, 0)) + 1
                    if _bc >= 3:
                        stream.emit([
                            ["  \u2014 ", "simpleline_redwnl"],
                            [f"{vid}: 3 broken downloads in a row \u2014 "
                             f"quarantining (delete from "
                             f"_redownload_progress.json to retry).\n",
                             "dim"],
                        ])
                        # Quarantine: mark as done so this pass + future
                        # resumes skip the video.
                        done.add(vid)
                        broken_counts.pop(vid, None)
                    else:
                        broken_counts[vid] = _bc
                    if not _save_progress(folder, ch_url, cur_res[0], done,
                                          broken_counts):
                        stream.emit_dim(
                            "  \u2014 warning: progress file save failed; "
                            "broken-download retry state may be stale.")
                except Exception as _qe:
                    _log.debug("swallowed: %s", _qe)
                _emit_active(file_num, _live_total)
                n_err += 1
                continue
            # Belt-and-suspenders for the `already-at-target` path: if
            # ffprobe missed (returned None and we downloaded anyway) but
            # the resulting file is byte-identical to the original, the
            # video was already at this resolution. Discard the temp
            # file, emit the same "skip" line `_already_at_target` would
            # have, and count it as skipped rather than logging a bogus
            # "0% smaller" re-download entry.
            if (orig_size > 0 and new_size == orig_size
                    and os.path.isfile(fp)):
                # Head/mid/tail sampled compare (shared with reorg's dedup —
                # see backend.utils.sampled_files_equal) instead of a full
                # chunked compare: the original full-file read burned hours of
                # disk I/O on a mostly-at-target rescan for a vanishingly rare
                # same-size/different-content collision.
                _ident = sampled_files_equal(fp, new_fp)
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
                    broken_counts.pop(vid, None)
                    _save_progress(folder, ch_url, cur_res[0], done,
                                   broken_counts)
                    n_skipped += 1
                    continue
            # Resolution guard — never replace an original with a WORSE
            # copy. _already_at_target returning False only means
            # "below target OR unknown"; without this check, a 4K file
            # whose height couldn't be probed got replaced by a 1080
            # download, and a 720p file whose video YouTube now serves
            # at 360p max got silently downgraded. Permanent quality
            # loss either way.
            _new_h = _ffprobe_height(new_fp)
            _tgt_h = 0
            try:
                _tgt_h = int(cur_res[0])
            except Exception:
                _tgt_h = 0  # "best" — only the orig-vs-new compare applies
            _refuse = ""
            if orig_h and _new_h:
                if _new_h < orig_h:
                    _refuse = (f"new copy is {_new_h}p, original is "
                               f"{orig_h}p")
            elif orig_h and not _new_h:
                _refuse = (f"can't probe the new copy's resolution "
                           f"(original is {orig_h}p)")
            elif not orig_h and _tgt_h:
                if _new_h and _new_h < _tgt_h - 8:
                    _refuse = (f"original's resolution unknown and new "
                               f"copy is only {_new_h}p (target {_tgt_h}p)")
                elif not _new_h:
                    _refuse = ("can't probe either file's resolution "
                               "(ffprobe unavailable?)")
            if _refuse:
                try: os.remove(new_fp)
                except OSError: pass
                stream.emit([
                    ["[", "simpleline_redwnl"],
                    [str(file_num), "simpleline"],
                    ["/", "simpleline_redwnl"],
                    [str(_live_total), "simpleline"],
                    ["] ", "simpleline_redwnl"],
                    [f"{item['title']} — refusing replace: {_refuse} "
                     f"— keeping original.\n", "red"],
                ])
                _emit_active(file_num, _live_total)
                n_err += 1
                continue
            new_ext = os.path.splitext(new_fp)[1].lower()
            orig_ext = os.path.splitext(fp)[1].lower()
            target_fp = fp
            if new_ext and new_ext != orig_ext:
                # Refuse cross-extension replacement. yt-dlp is
                # invoked with --merge-output-format mp4 so this
                # should never happen — when it DOES (rare codec
                # combination that yt-dlp couldn't remux), the
                # rename leaves sidecars (transcripts, jsonl,
                # thumbnails) keyed to the old stem orphaned. Sync,
                # metadata refresh, and Watch view all key on filename
                # and would silently miss the file. Safer to abort
                # this video's redownload and emit a clear error so
                # the user can investigate manually.
                try:
                    if os.path.isfile(new_fp):
                        os.remove(new_fp)
                except OSError:
                    pass
                stream.emit_error(
                    f"  [skip] {os.path.basename(fp)}: yt-dlp produced "
                    f"{new_ext} instead of {orig_ext}. Sidecar rename "
                    f"unsupported — skipping to avoid metadata orphans.")
                continue
                # NOTE (audit: redownload H101): the two-phase-commit
                # block that previously lived here was unreachable
                # (the `continue` above always fires for cross-
                # extension yt-dlp output). Removed to eliminate
                # confusion — same-extension replace runs in the
                # `else` branch below.
            else:
                # Same-extension replace path. Apply the same
                # retry+jitter loop the cross-extension path has — a
                # single transient lock (VLC preview, antivirus,
                # Explorer thumb cache) should not throw away minutes
                # of yt-dlp work.
                import random as _random
                _last_err = None
                _ok = False
                for _try in range(3):
                    try:
                        os.replace(new_fp, target_fp)
                        _last_err = None
                        _ok = True
                        break
                    except OSError as _re:
                        _last_err = _re
                        if _try < 2:
                            time.sleep(2.0 + _random.uniform(0, 1.0))
                if not _ok:
                    try:
                        if os.path.isfile(new_fp):
                            os.remove(new_fp)
                    except OSError: pass
                    raise _last_err if _last_err else OSError(
                        "os.replace failed after 3 attempts")
            done.add(vid)
            broken_counts.pop(vid, None)
            if not _save_progress(folder, ch_url, cur_res[0], done,
                                  broken_counts):
                stream.emit_dim(
                    "  — warning: progress file save failed; resume "
                    "may retry this video on next run.")
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
    # Walks the entire channel tree, not just the channel root, because
    # per-video subfolder _REDOWNLOAD_TEMP/ dirs (created when the
    # video lived under year/month splits) are NEVER cleaned by a
    # root-only rmdir. Without this sweep, those subfolder temps
    # accumulated permanently AND their orphan downloads could be
    # promoted as originals on the next pass.
    try:
        for _dp, _dns, _fns in os.walk(folder):
            for _d in list(_dns):
                if _d == "_REDOWNLOAD_TEMP":
                    _tmp = os.path.join(_dp, _d)
                    try:
                        if not os.listdir(_tmp):
                            os.rmdir(_tmp)
                    except OSError:
                        pass
    except OSError:
        pass

    # 6. Clear progress only if a full pass completed without cancel
    # AND without errors — errored ids are not in `done`, so keeping
    # the file makes the next run retry exactly the failures instead
    # of re-downloading every previously completed video.
    if not cancel_ev.is_set() and n_err == 0:
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
    # Both emit paths are required: append_history_entry() writes to
    # config['autorun_history'] so the row survives restart, but does
    # NOT push to the running UI. stream.emit_activity() pushes to
    # the live activity-log DOM but does NOT persist. Without the
    # emit_activity call, the [ReDwnl] row only shows up after the
    # next app launch — reported symptom: "no activity log from this
    # channel's redownload" immediately after completion, despite
    # the other recently-completed channels' rows being visible.
    try:
        from datetime import datetime as _dt

        from . import autorun as _ar
        _now = _dt.now()
        # `%-I` is glibc-only; musl + Windows reject it. Use the
        # cross-platform `lstrip("0")` workaround everywhere (audit:
        # redownload L48).
        _ts = _now.strftime("%I:%M%p").lstrip("0").lower()
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
        except Exception as e:
            _log.debug("swallowed: %s", e)
    except Exception as e:
        _log.debug("swallowed: %s", e)

    return {"ok": True, "done": n_done, "skipped": n_skipped, "errors": n_err,
            "total": len(matched)}
