"""
ArchiveMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import threading
import time
import urllib.request
from datetime import datetime
from typing import Any, Optional

from ._shared import _log
from backend.log import swallow
from backend.ytarchiver_config import load_config
from backend import sync as sync_backend

# Module-level init lock — the bridge attribute (_archive_single_lock)
# is created lazily on first call, and without an outer lock two near-
# simultaneous first-time calls from JS could both pass the `hasattr`
# check and create their own set+Lock objects, leaving two yt-dlp
# processes both thinking they "hold" the URL guard. Use this module
# lock to make the lazy init atomic.
_archive_init_lock = threading.Lock()


def _probe_output_folder_writable(base: str) -> None:
    import tempfile as _tempfile
    os.makedirs(base, exist_ok=True)
    with _tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=base,
            prefix=".__ytarchiver_write_probe_",
            suffix=".tmp", delete=True) as f:
        f.write("ok")


def _recent_scan_bind_is_corroborated(path: str, video_id: str,
                                      title: str) -> bool:
    """Guard single-video recent-scan fallback against wrong-file binding."""
    if not path:
        return False
    import re as _re
    stem = os.path.splitext(os.path.basename(path))[0]
    stem_no_date = _re.sub(r"\s*\(\d{2}\.\d{2}\.\d{2}\)\s*$", "", stem)
    low_name = os.path.basename(path).lower()
    vid = (video_id or "").strip().lower()
    if vid and vid in low_name:
        return True
    title_sane = _re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_",
                         title or "").strip()
    if not title_sane:
        return False
    prefix = title_sane[:50].rstrip().lower()
    if len(prefix) < 12:
        return False
    stem_low = stem_no_date.lower()
    title_low = title_sane.lower()
    return stem_low.startswith(prefix) or (
        len(stem_low) >= 12 and title_low.startswith(stem_low))


_BINDABLE_MEDIA_EXTS = {
    ".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v",
    ".mp3", ".m4a", ".opus", ".ogg", ".wav", ".flac",
}


def _is_under_folder(path: str, folder: str) -> bool:
    try:
        p = os.path.normcase(os.path.abspath(path))
        f = os.path.normcase(os.path.abspath(folder))
        return os.path.commonpath([p, f]) == f
    except Exception:
        return False


def _clean_ytdlp_path(path: str) -> str:
    path = (path or "").strip()
    if len(path) >= 2 and path[0] == path[-1] and path[0] in ("'", '"'):
        path = path[1:-1]
    return path.strip()


def _choose_existing_ytdlp_candidate(
        candidates: list[str], folder: str) -> str:
    """Return a trustworthy media path captured from yt-dlp output."""
    for raw in reversed(candidates):
        path = _clean_ytdlp_path(raw)
        if (path and os.path.isfile(path)
                and os.path.splitext(path)[1].lower() in _BINDABLE_MEDIA_EXTS
                and _is_under_folder(path, folder)):
            return path
    return ""


# ── T326: pure, unit-testable pieces extracted from archive_single_video._run
# These were inline closures with no test seam; the filename-reconstruction
# fallbacks in particular have a history of binding the wrong file. Pulling
# them out lets the highest-risk logic be exercised without a live yt-dlp run.


def parse_dltrack(line: str) -> Optional[dict]:
    """Parse a ``DLTRACK:::...`` manifest line into its fields.

    Format: ``DLTRACK:::<title>:::<uploader>:::<upload_date>:::<filesize>:::
    <duration>:::<id>``. Anchors on the trailing 5 fixed fields and rejoins
    the middle as the title, so a title containing a literal ``:::`` cannot
    shift the id field. Returns ``None`` when the line has fewer than the
    7 required parts (caller treats that as "skip indexing").
    """
    parts = (line or "").split(":::")
    if len(parts) < 7:
        return None
    return {
        "title": ":::".join(parts[1:-5]).strip(),
        "uploader": parts[-5],
        "upload_date": parts[-4],
        "filesize": parts[-3],
        "duration": parts[-2],
        "video_id": (parts[-1] or "").strip(),
    }


def resolve_final_path(base: str, video_id: str, title: str,
                       candidate_paths: list[str]) -> str:
    """Reconstruct the on-disk path of a freshly single-downloaded video.

    Tries, in order: a trustworthy candidate captured from yt-dlp's
    Destination/Merger output, then a glob match on the video id in the
    filename (newest mtime wins), then a full sanitized-title match, and
    finally a corroborated recent-scan bind. Returns "" if nothing matched.
    The recency tie-breaks mean a user with two copies binds the fresh one.
    """
    import glob as _glob
    final_path = _choose_existing_ytdlp_candidate(candidate_paths, base)
    try:
        vid_candidates = []
        if video_id:
            for g in _glob.glob(os.path.join(base, "*")):
                if video_id in os.path.basename(g):
                    vid_candidates.append(g)
        if vid_candidates:
            vid_candidates.sort(
                key=lambda p: os.path.getmtime(p)
                if os.path.isfile(p) else 0,
                reverse=True)
            final_path = vid_candidates[0]
        if not final_path and title:
            # Fallback: match by full sanitized title.
            title_sane = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", title)
            title_candidates = []
            for g in _glob.glob(os.path.join(base, "*")):
                stem = os.path.splitext(os.path.basename(g))[0]
                # Match if the stem EQUALS the full sanitized title
                # (possibly with a " (MM.DD.YY)" date suffix stripped).
                stem_no_date = re.sub(
                    r"\s*\(\d{2}\.\d{2}\.\d{2}\)\s*$", "", stem)
                if stem == title_sane or stem_no_date == title_sane:
                    title_candidates.append(g)
            if title_candidates:
                title_candidates.sort(
                    key=lambda p: os.path.getmtime(p)
                    if os.path.isfile(p) else 0,
                    reverse=True)
                final_path = title_candidates[0]
    except Exception as e:
        swallow("final-path title match", e)
    # GUARANTEED binding fallback. The id was captured from DLTRACK but
    # neither id-in-filename nor the sanitized-title match found the file
    # (unicode / trim-filenames / punctuation oddities). A single-video
    # download produces exactly one fresh file, so the newest media file
    # just written under `base` is it — bind the id through it instead of
    # dropping it. (_scan_recent_video uses ctime on Windows, so the
    # --mtime upload-date stamp can't defeat the recency check.)
    if (not final_path or not os.path.isfile(final_path)) and video_id:
        try:
            rp = sync_backend._scan_recent_video(base)
            if (rp and os.path.isfile(rp)
                    and _recent_scan_bind_is_corroborated(
                        rp, video_id, title)):
                final_path = rp
                _log.info(
                    "single-video bind via recent-scan: vid=%s -> %s",
                    video_id, rp)
            elif rp:
                _log.warning(
                    "single-video recent-scan candidate not corroborated; "
                    "refusing bind: vid=%s title=%r candidate=%s",
                    video_id, title, rp)
        except Exception as se:
            _log.debug("recent-scan bind failed: %s", se)
    return final_path


def classify_download_outcome(returncode, has_dltrack, killed,
                              file_exists, registered,
                              stderr_errors) -> tuple[str, str]:
    """Decide the terminal state of a single-video download.

    Returns ``(outcome, reason)`` where outcome is one of
    ``"killed"`` / ``"success"`` / ``"downloaded_unindexed"`` / ``"failed"``.
    A nonzero rc OR a missing DLTRACK OR a file that vanished from disk all
    block a success claim; ``reason`` is only meaningful for ``"failed"``.
    """
    if killed:
        return ("killed", "")
    dl_ok = (returncode == 0) and bool(has_dltrack)
    if dl_ok and file_exists and registered:
        return ("success", "")
    if dl_ok and file_exists and not registered:
        return ("downloaded_unindexed", "")
    # Either yt-dlp itself failed, or it reported success but the file is
    # missing from disk at decision time.
    if dl_ok and not file_exists:
        reason = ("download finished, but YTArchiver could not find the "
                  "saved video file; run Rescan or try the download again")
    elif stderr_errors:
        reason = stderr_errors[0]
    elif returncode not in (0, None):
        reason = f"yt-dlp exited with code {returncode}"
    else:
        reason = "no video was produced"
    return ("failed", reason)


class ArchiveMixin:

    def archive_single_is_running(self) -> bool:
        try:
            with self._archive_single_lock:
                return bool(self._archive_single_inflight)
        except Exception:
            return False

    # ─── Already-archived pre-check (Download-tab warning) ──────────────

    def single_video_archived(self, url):
        """Does this pasted URL point to a video already in the archive?

        Lightweight, read-only pre-check for the Download tab — it NEVER
        downloads anything. Returns
        ``{ok, archived, video_id, title, channel}``. The frontend uses it to
        warn ("already archived — download anyway?") before calling
        archive_single_video.

        We resolve the YouTube id from the URL and look it up in the live
        index (videos.video_id) rather than tracking a separate
        "already-downloaded" id list, so the answer always reflects what is
        actually archived right now. On any failure we report not-archived so
        the check can never block a legitimate download.
        """
        try:
            from ..view_format import _extract_video_id
            vid = _extract_video_id((url or "").strip())
            if not vid:
                return {"ok": True, "archived": False, "video_id": ""}
            from .. import index as _idx
            hit = _idx.find_archived_by_video_id(vid)
            if not hit:
                return {"ok": True, "archived": False, "video_id": vid}
            return {"ok": True, "archived": True, "video_id": vid,
                    "title": hit.get("title", ""),
                    "channel": hit.get("channel", "")}
        except Exception as e:
            _log.debug("single_video_archived check failed: %s", e)
            return {"ok": True, "archived": False, "video_id": ""}

    # ─── Single-URL archive (Enter on URL field) ───────────────────────

    def archive_single_video(self, url, options=None):
        """Download a single YouTube URL immediately (no channel walk).

        Output layout mirrors YTArchiver.py:17313 build_video_cmd exactly so
        single-video downloads are indistinguishable from OLD's:
          - Target dir: `cfg['video_out_dir']` (NOT `output_dir`/channels),
            no `%(uploader)s` subfolder, no `[id]` suffix.
          - Filename: `{title}.mp4` or `{title} (MM.DD.YY).mp4` when add_date.
          - Custom name sanitizer: `[<>:"/\\|?*\x00-\x1f]` → `_`.
          - `--mtime` when date_file is True so mtime = YT upload date.

        Concurrency guard: a per-URL lock prevents the user from
        spawning parallel yt-dlp processes for the same URL by
        double-clicking. Different URLs are still allowed in parallel.
        """
        import re as _re
        url = (url or "").strip()
        if not url:
            return {"ok": False, "error": "Empty URL"}
        # audit DT-2: normalize the URL before using/storing it.
        # Strip fragment (#t=30) and unrelated query params; keep
        # only the `v=<id>` param for watch URLs. Prevents history
        # pollution (three different entries for the same video)
        # and avoids confusing yt-dlp with tracking params.
        def _canonicalize_yt_url(u: str) -> str:
            try:
                from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
                p = urlparse(u)
                # Drop fragment always.
                # Keep only v= on watch URLs; keep everything on short URLs
                # (youtu.be/<id>) since the path IS the id.
                if "youtube" in (p.netloc or "").lower() and \
                        p.path.rstrip("/").endswith("/watch"):
                    qs = parse_qs(p.query or "")
                    v = (qs.get("v") or [""])[0]
                    new_q = urlencode({"v": v}) if v else ""
                    return urlunparse((p.scheme or "https", p.netloc,
                                       p.path, "", new_q, ""))
                # Clean fragment off everything else.
                return urlunparse((p.scheme or "https", p.netloc,
                                   p.path, p.params, p.query, ""))
            except Exception:
                return u
        url = _canonicalize_yt_url(url)
        # Security: require an http(s) scheme before the URL reaches yt-dlp as
        # a positional arg. A pasted value beginning with '-' (e.g.
        # "--exec=calc.exe") has no scheme and would otherwise be parsed by
        # yt-dlp as an OPTION rather than a URL — an argument-injection /
        # command-execution vector (audit r2). Scheme-only (not host) so
        # non-YouTube yt-dlp-supported sites still work.
        try:
            from urllib.parse import urlparse as _up2
            if _up2(url).scheme not in ("http", "https"):
                return {"ok": False,
                        "error": "Only http(s) URLs can be downloaded."}
        except Exception:
            return {"ok": False, "error": "Invalid URL."}
        # Concurrency guard — track in-flight URLs so a rapid
        # double-click doesn't launch two yt-dlp processes fighting
        # over the same filename. Lazy init wrapped in a module-level
        # lock so two near-simultaneous first-time calls can't each
        # build a separate set+Lock and both think they "hold" the URL.
        if not hasattr(self, "_archive_single_inflight"):
            with _archive_init_lock:
                if not hasattr(self, "_archive_single_inflight"):
                    self._archive_single_lock = threading.Lock()
                    self._archive_single_inflight = set()
        with self._archive_single_lock:
            if url in self._archive_single_inflight:
                return {"ok": False,
                        "error": "Already downloading this URL"}
            self._archive_single_inflight.add(url)
        if not sync_backend.find_yt_dlp():
            # audit DT-1: DON'T record URL in history on a failed
            # launch. History write is moved to the success path
            # in _run() below. A URL that fails validation shouldn't
            # pollute the autocomplete dropdown.
            try:
                self._archive_single_inflight.discard(url)
            except Exception as e:
                swallow("inflight discard on yt-dlp missing", e)
            return {"ok": False, "error": "yt-dlp not found"}
        cfg = load_config()
        opts = options if isinstance(options, dict) else {}
        # Target folder: custom save_to, then video_out_dir (dedicated single-
        # video destination), then fall back to output_dir (the channel root)
        # as a last resort. OLD uses `video_out_dir` exclusively.
        base = (opts.get("save_to") or cfg.get("video_out_dir")
                or cfg.get("output_dir") or "").strip()
        if not base:
            try:
                self._archive_single_inflight.discard(url)
            except Exception as e:
                swallow("inflight discard on no output-dir", e)
            return {"ok": False, "error": "No output_dir configured"}
        # audit DT-3: verify target folder is writable before
        # launching yt-dlp. Creates the dir if it doesn't exist;
        # bails with a clear error if the dir cannot be written.
        try:
            _probe_output_folder_writable(base)
        except OSError as _fe:
            try:
                self._archive_single_inflight.discard(url)
            except Exception as e:
                swallow("inflight discard on folder not writable", e)
            return {"ok": False,
                    "error": f"Output folder not writable: {base} ({_fe})"}
        # audit DT-11 / DT-5: validate custom name if "use YT title"
        # is off. An empty/whitespace-only custom name would silently
        # fall back to YT title at line 5596 below, which isn't what
        # the user expected.
        _use_yt = opts.get("use_yt_title", True)
        _cname = (opts.get("custom_name") or "").strip()
        if not _use_yt:
            _safe_cname = _re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_",
                                  _cname).strip().rstrip(".")
            if not _safe_cname:
                try:
                    self._archive_single_inflight.discard(url)
                except Exception as e:
                    swallow("inflight discard on bad custom name", e)
                return {"ok": False,
                        "error": "Custom name is empty or all special chars. "
                                 "Enable 'Use YT title' or enter a real name."}
        import subprocess as _sp
        yt = sync_backend.find_yt_dlp()
        res = str(opts.get("resolution") or "1080").strip()
        fmt = sync_backend.build_format_string(res)
        # Filename: OLD-compat template. The date suffix uses TODAY's date
        # (download date), NOT the upload date — matches YTArchiver.py:17316.
        dl_date = datetime.now().strftime("%m.%d.%y")
        use_yt_title = opts.get("use_yt_title", True)
        add_date = bool(opts.get("add_date", False))
        custom_name = (opts.get("custom_name") or "").strip()
        if not use_yt_title and custom_name:
            safe = _re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", custom_name).strip().rstrip(".")
            if add_date:
                fname = f"{safe} ({dl_date}).%(ext)s"
            else:
                fname = f"{safe}.%(ext)s"
        else:
            fname = f"%(title)s ({dl_date}).%(ext)s" if add_date else "%(title)s.%(ext)s"
        out_tpl = os.path.join(base, fname)

        # Option: date_file — apply YT upload date as file mtime (default: True)
        date_file = opts.get("date_file", True)
        # Option: grab_metadata — also fetch the .info.json + thumbnail so a
        # one-off download isn't a bare .mp4 (enables the Watch metadata
        # drawer + a grid thumbnail). Both sidecars are hidden after the
        # download so the folder still shows only the video (+ transcript).
        grab_metadata = bool(opts.get("grab_metadata", False))

        # Per-URL inplace-replace marker so the "[Dwnld] ..." line stays
        # at the same scroll position from URL → filename → NN% → Done.
        # logs.js's _inplaceKind already routes `dwnld_done_*` prefixes
        # through its in-place-replacement path.
        import hashlib as _hashlib
        _marker_tag = "dwnld_done_" + _hashlib.md5(
            url.encode("utf-8", "replace")
        ).hexdigest()[:12]

        def _run():
            # Initial state — no title known yet; show the URL until the
            # DLPRE (before_dl) line arrives with the real title + channel.
            _state = {"fname": url, "last_pct": -1,
                      "have_title": False, "final_size": 0,
                      "registered": False, "recorded": False,
                      "final_path": ""}
            _killed = False

            def _emit_dwnld(suffix=""):
                """Replace the inplace [Dwnld] line. suffix is e.g.
                ' - 42%' or ' - Done.' or '' (no progress yet)."""
                self._log_stream.emit([
                    ["[Dwnld] ", ["simpleline_green", _marker_tag]],
                    [f"{_state['fname']}{suffix}\n",
                     ["simpleline", _marker_tag]],
                ])

            _emit_dwnld()
            # Mirror YTArchiver.py:17327 build_video_cmd exactly — skip the
            # mp4 merge args when downloading audio-only.
            cmd = [yt, "--newline", "--no-quiet", "--continue",
                   # Bound the per-connection wait the same way the sync path
                   # does (sync/core.py). Without these, a single-URL download
                   # of a CDN-unreachable video grinds for many minutes on
                   # yt-dlp's defaults (~10 retries x ~20s connect timeout)
                   # instead of giving up in ~45s.
                   "--retries", "3", "--socket-timeout", "15"]
            if date_file:
                cmd.append("--mtime")
            cmd += ["--trim-filenames", "200", "--format", fmt]
            if res != "audio":
                cmd += ["--merge-output-format", "mp4", "--ppa", "Merger:-c copy"]
            # Optional metadata + thumbnail for one-off downloads. yt-dlp
            # writes "<base>.info.json" + "<base>.jpg" next to the video;
            # find_thumbnail resolves the co-located .jpg directly, and the
            # sidecars are hidden post-download (see below).
            if grab_metadata:
                cmd += ["--write-info-json", "--write-thumbnail",
                        "--convert-thumbnails", "jpg"]
            cmd += [
                "--output", out_tpl,
                # before_dl fires after extraction but BEFORE the first byte
                # downloads, so the log row can show "Title - Channel" (what
                # the user wants to see) instead of the raw URL while the
                # download runs.
                "--print",
                "before_dl:DLPRE:::%(title)s:::%(uploader)s",
                "--print",
                "after_video:DLTRACK:::%(title)s:::%(uploader)s:::%(upload_date)s:::%(filesize,filesize_approx)s:::%(duration)s:::%(id)s",
                *sync_backend._find_cookie_source(),
                url,
            ]
            try:
                proc = _sp.Popen(cmd, stdin=_sp.DEVNULL,
                                 stdout=_sp.PIPE, stderr=_sp.STDOUT,
                                 encoding="utf-8", errors="replace",
                                 bufsize=1, startupinfo=sync_backend._startupinfo)
            except Exception as e:
                # Update the in-place [Dwnld] row with a final failed
                # state so it doesn't sit forever showing just the URL.
                # Use the same marker so the inplace selector finds and
                # replaces it (audit: archive_mixin H17).
                try:
                    self._log_stream.emit([
                        ["[Dwnld] ", ["red", _marker_tag]],
                        [f"{_state['fname']} - failed (Launch failed: {e})\n",
                         ["red", _marker_tag]],
                    ])
                except Exception:
                    pass
                self._log_stream.emit_error(f"Launch failed: {e}")
                # Release the in-flight lock on launch failure so a
                # retry isn't blocked forever.
                try:
                    with self._archive_single_lock:
                        self._archive_single_inflight.discard(url)
                except Exception as e:
                    swallow("inflight discard on launch failure", e)
                return
            # parse the DLTRACK line so single-video downloads
            # land in the videos index, the Recent tab, and the FTS
            # index — same as channel-sync downloads. Previously this
            # loop just echoed stdout to the log and the file ended up
            # on disk invisible to the Browse grid / Recent / Search.
            _dltrack = None
            _stderr_errors = []
            _candidate_paths = []
            # Regexes for parsing yt-dlp progress + destination lines.
            _dest_re = _re.compile(r"^\[download\]\s+Destination:\s+(.+)$")
            _merge_re = _re.compile(
                r'^\[Merger\]\s+Merging formats into\s+"(.+)"$')
            _done_re = _re.compile(
                r"^\[download\]\s+(.+?)\s+has already been downloaded")
            _extract_re = _re.compile(
                r"^\[ExtractAudio\]\s+Destination:\s+(.+)$")
            _pct_re = _re.compile(r"^\[download\]\s+(\d+(?:\.\d+)?)%")
            try:
                for line in proc.stdout:
                    _line = line.rstrip()
                    # Always feed dim stdout so verbose mode still sees
                    # yt-dlp's raw output. Simple mode filters dim out.
                    self._log_stream.emit_dim(" " + _line)
                    if _line.startswith("DLTRACK:::"):
                        _dltrack = _line
                    # before_dl line — switch the row to "Title - Channel"
                    # as soon as yt-dlp resolves the video, before the
                    # download bytes start. Fires once per format (video +
                    # audio for a merge) but the assignment is idempotent.
                    if _line.startswith("DLPRE:::"):
                        _pp = _line.split(":::")
                        _t = _pp[1].strip() if len(_pp) > 1 else ""
                        _u = _pp[2].strip() if len(_pp) > 2 else ""
                        if _t and not _state["have_title"]:
                            _state["fname"] = f"{_t} - {_u}" if _u else _t
                            _state["have_title"] = True
                            _emit_dwnld(
                                f" - {_state['last_pct']}%"
                                if _state["last_pct"] >= 0 else "")
                        continue
                    for _rx in (_merge_re, _done_re, _extract_re):
                        _m_path = _rx.match(_line)
                        if _m_path:
                            _candidate_paths.append(_m_path.group(1).strip())
                            break
                    # Capture filename from yt-dlp's Destination line — only
                    # as a fallback when DLPRE never gave us a title.
                    _m_dest = _dest_re.match(_line)
                    if _m_dest:
                        _dest_path = _m_dest.group(1).strip()
                        _candidate_paths.append(_dest_path)
                        if not _state["have_title"]:
                            _state["fname"] = os.path.basename(_dest_path)
                            _emit_dwnld()
                    else:
                        # Parse progress percentage and update inplace
                        # line at 5% boundaries (mirrors compress.py).
                        _m_pct = _pct_re.match(_line)
                        if _m_pct:
                            _pct = int(float(_m_pct.group(1)))
                            if _pct != _state["last_pct"] and (
                                    _pct % 5 == 0 or _state["last_pct"] < 0):
                                _state["last_pct"] = _pct
                                _emit_dwnld(f" - {_pct}%")
                    # capture known-failure yt-dlp error
                    # signatures so the post-run branch can surface a
                    # toast instead of leaving the user staring at dim
                    # stdout with no actionable info.
                    _ll = _line.lower()
                    if "error" in _ll:
                        if "members-only" in _ll or "join this channel" in _ll:
                            _stderr_errors.append("members-only content")
                        elif "private video" in _ll:
                            _stderr_errors.append("private video")
                        elif "video unavailable" in _ll or "this video is unavailable" in _ll:
                            _stderr_errors.append("video unavailable (deleted or region-locked)")
                        elif "cookies are missing" in _ll or "sign in to confirm" in _ll:
                            _stderr_errors.append("YouTube wants a sign-in (sign into YouTube in Firefox; its cookies are missing or expired)")
                # Wait with a generous watchdog timeout (15 minutes).
                # Without this, a wedged yt-dlp (network stall, post-
                # processor hang, sign-in prompt) keeps the in-flight
                # URL stuck in _archive_single_inflight forever —
                # "Already downloading" until app restart.
                try:
                    proc.wait(timeout=900)
                except _sp.TimeoutExpired:
                    _killed = True
                    try:
                        proc.terminate()
                        proc.wait(timeout=5)
                    except Exception as e:
                        swallow("watchdog terminate", e)
                    try:
                        if proc.poll() is None:
                            proc.kill()
                            proc.wait(timeout=2)
                    except Exception as e:
                        swallow("watchdog kill", e)
                    # Also update the in-place [Dwnld] row with the
                    # failed state so the user sees it resolved
                    # instead of stuck at "downloading..." forever
                    # (audit: archive_mixin H17).
                    try:
                        self._log_stream.emit([
                            ["[Dwnld] ", ["red", _marker_tag]],
                            [f"{_state['fname']} - failed (watchdog timeout)\n",
                             ["red", _marker_tag]],
                        ])
                    except Exception:
                        pass
                    self._log_stream.emit_error(
                        "[Dwnld] Watchdog: yt-dlp hung; killed after 15 min.")
                # audit DT-1: only write to URL history now that the
                # download actually ran. Previously written on submit,
                # which polluted history with any URL the user
                # clicked even if it failed.
                if proc.returncode == 0:
                    try:
                        self._push_url_history(url)
                    except Exception as e:
                        swallow("url-history write", e)
                # Post-download bookkeeping — emulate the channel-sync
                # path's register_video + _record_recent_download hooks.
                if _dltrack:
                    try:
                        # audit DT-8: guard against yt-dlp output format
                        # changes / missing fields. parse_dltrack returns
                        # None (instead of raising IndexError) when the line
                        # is short, anchors on the trailing 5 fixed fields so
                        # a ':::' in the title can't shift the id, and rejoins
                        # the middle as the title. See T326 extraction.
                        _dt = parse_dltrack(_dltrack)
                        if _dt is None:
                            _nparts = len((_dltrack or "").split(":::"))
                            self._log_stream.emit_dim(
                                f" (DLTRACK parsing: only {_nparts} "
                                f"parts, expected 7 — indexing skipped)")
                            raise ValueError("dltrack parse")
                        _uploader = _dt["uploader"]
                        _vid = _dt["video_id"]
                        _title = _dt["title"]
                        # Resolve the final filepath on disk via the shared
                        # id-glob / title-match / recent-scan fallbacks
                        # (T326 extraction — see resolve_final_path). When
                        # multiple candidates match, the newest by mtime wins
                        # (the fresh download), and the title fallback requires
                        # a full-title match so similar long-title prefixes
                        # ("Video 1:...", "Video 2:...") don't collide.
                        final_path = resolve_final_path(
                            base, _vid, _title, _candidate_paths)
                        if _vid and (not final_path
                                     or not os.path.isfile(final_path)):
                            # Never silently drop an authoritative id.
                            _log.error(
                                "single-video download captured id %s but "
                                "could not bind a file under %s — id at risk",
                                _vid, base)
                            try:
                                self._log_stream.emit_dim(
                                    f" ⚠ downloaded {_vid} but could not match "
                                    f"it to a file — video ID not recorded; "
                                    f"re-download to capture it")
                            except Exception as _be:
                                _log.debug("single-video orphan warn failed: %s", _be)
                        if final_path and os.path.isfile(final_path):
                            _state["final_path"] = final_path
                            try:
                                _state["final_size"] = os.path.getsize(final_path)
                            except OSError:
                                pass
                            _channel_name = _uploader or "Single Videos"
                            try:
                                from backend import index as _idx
                                _state["registered"] = bool(_idx.register_video(
                                    final_path, _channel_name, _title,
                                    tx_status="no_captions",
                                    video_id=_vid))
                            except Exception as _re_err:
                                self._log_stream.emit_dim(
                                    f" (index register failed: {_re_err})")
                            try:
                                _state["recorded"] = bool(
                                    sync_backend._record_recent_download(
                                        final_path, _channel_name, _title, _vid))
                            except Exception as _re_err:
                                self._log_stream.emit_dim(
                                    f" (recent downloads write failed: {_re_err})")
                            # Hide the freshly-written metadata/thumbnail
                            # sidecars so the loose-download folder still
                            # shows only the video (+ any Transcript.txt).
                            if grab_metadata:
                                try:
                                    from .. import utils as _u
                                    _u.hide_stray_sidecars(
                                        os.path.dirname(final_path),
                                        recursive=False,
                                        hide_per_video_transcripts=True)
                                except Exception as _he:
                                    swallow("hide sidecars after download", _he)
                            # Drop from deferred livestream journal if
                            # this was a previously-deferred premiere
                            # that's now finished (matches bug C-3).
                            if _vid:
                                try:
                                    from backend import livestreams as _ls
                                    _ls.drop(_vid)
                                except Exception as e:
                                    swallow("deferred-livestream drop", e)
                    except Exception as _pe:
                        self._log_stream.emit_dim(
                            f" (DLTRACK post-processing failed: {_pe})")
                # Resolve the [Dwnld] row to a definitive end state. yt-dlp
                # reporting returncode 0 + a DLTRACK line is NOT enough to
                # claim success — the file can vanish from disk afterward,
                # or the index registration can silently fail (locked DB),
                # leaving a row that reads "106 MB" while the Watch view
                # shows "File not found" and the video is in no list. So
                # re-verify the file is on disk at decision time and only
                # declare success when it ALSO landed in the index.
                # The watchdog branch already emitted its own failure line.
                _fp_now = _state.get("final_path") or ""
                _file_exists = bool(_fp_now) and os.path.isfile(_fp_now)
                _registered = bool(_state.get("registered"))
                _recorded = bool(_state.get("recorded"))
                _outcome, _reason = classify_download_outcome(
                    proc.returncode, bool(_dltrack), _killed,
                    _file_exists, _registered, _stderr_errors)
                if _outcome == "killed":
                    pass
                elif _outcome == "success":
                    _sz = _state.get("final_size") or 0
                    if _sz > 0:
                        from backend.utils import format_bytes as _fmtb
                        _emit_dwnld(f" - {_fmtb(_sz)}")
                    else:
                        _emit_dwnld(" - Done.")
                    # File is on disk and indexed, but the Recent-tab entry
                    # failed to write — note it without downgrading success.
                    if not _recorded:
                        self._log_stream.emit_dim(
                            " (not added to Recent — try Rescan)")
                elif _outcome == "downloaded_unindexed":
                    # Downloaded fine but the index write was dropped (most
                    # likely a locked DB during a concurrent disk scan). The
                    # file is on disk but won't appear in Browse/Watch until
                    # a Rescan picks it up. Make that visible — don't claim
                    # a clean success.
                    self._log_stream.emit([
                        ["[Dwnld] ", ["dlwarn", _marker_tag]],
                        [f"{_state['fname']} — downloaded but not "
                         "indexed; run Rescan to add it.\n",
                         ["dlwarn", _marker_tag]],
                    ])
                    try:
                        if self._window is not None:
                            self.services.event_bus.show_toast(
                                "Downloaded but not indexed — run Rescan",
                                "warn")
                    except Exception as e:
                        swallow("downloaded-not-indexed toast", e)
                else:  # "failed" — _reason already classified above
                    # Replace the inplace [Dwnld] line with a red failure.
                    self._log_stream.emit([
                        ["[Dwnld] ", ["red", _marker_tag]],
                        [f"{_state['fname']} \u2014 failed ({_reason}).\n",
                         ["red", _marker_tag]],
                    ])
                    try:
                        if self._window is not None:
                            self.services.event_bus.show_toast(
                                f"Download failed: {_reason}", "error")
                    except Exception as e:
                        swallow("download-failed toast", e)
                self._log_stream.flush()
                # Push a Recent-tab refresh so the new video appears
                # immediately instead of waiting for the next tab
                # switch. Matches the channel-sync push hook.
                try:
                    self._push_recent_refresh()
                except Exception as e:
                    swallow("recent-refresh push", e)
            except Exception as _ue:
                # Any unhandled error in the stream/parse loop must still
                # resolve the [Dwnld] row — otherwise the thread dies in
                # finally (releasing the lock) and the row sits forever
                # showing just the URL with no result.
                try:
                    self._log_stream.emit([
                        ["[Dwnld] ", ["red", _marker_tag]],
                        [f"{_state['fname']} — failed ({_ue}).\n",
                         ["red", _marker_tag]],
                    ])
                    self._log_stream.flush()
                except Exception as e:
                    swallow("unhandled-error row emit", e)
            finally:
                # Always release the URL guard, even on exception, so
                # the user can retry without restarting the app.
                try:
                    with self._archive_single_lock:
                        self._archive_single_inflight.discard(url)
                except Exception as e:
                    swallow("inflight discard in finally", e)

        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}
