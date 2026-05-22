"""
ArchiveMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


# Module-level init lock — the bridge attribute (_archive_single_lock)
# is created lazily on first call, and without an outer lock two near-
# simultaneous first-time calls from JS could both pass the `hasattr`
# check and create their own set+Lock objects, leaving two yt-dlp
# processes both thinking they "hold" the URL guard. Use this module
# lock to make the lazy init atomic.
_archive_init_lock = threading.Lock()


class ArchiveMixin:

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
                _log.debug("swallowed: %s", e)
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
                _log.debug("swallowed: %s", e)
            return {"ok": False, "error": "No output_dir configured"}
        # audit DT-3: verify target folder is writable before
        # launching yt-dlp. Creates the dir if it doesn't exist;
        # bails with a clear error if the dir cannot be written.
        try:
            os.makedirs(base, exist_ok=True)
            _probe = os.path.join(base, ".__ytarchiver_write_probe.tmp")
            with open(_probe, "w", encoding="utf-8") as _f:
                _f.write("ok")
            try: os.remove(_probe)
            except OSError: pass
        except OSError as _fe:
            try:
                self._archive_single_inflight.discard(url)
            except Exception as e:
                _log.debug("swallowed: %s", e)
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
                    _log.debug("swallowed: %s", e)
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

        # Per-URL inplace-replace marker so the "[Dwnld] ..." line stays
        # at the same scroll position from URL → filename → NN% → Done.
        # logs.js's _inplaceKind already routes `dwnld_done_*` prefixes
        # through its in-place-replacement path.
        import hashlib as _hashlib
        _marker_tag = "dwnld_done_" + _hashlib.md5(
            url.encode("utf-8", "replace")
        ).hexdigest()[:12]

        def _run():
            # Initial state — no filename known yet; show the URL.
            _state = {"fname": url, "last_pct": -1}

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
            cmd = [yt, "--newline", "--no-quiet", "--continue"]
            if date_file:
                cmd.append("--mtime")
            cmd += ["--trim-filenames", "200", "--format", fmt]
            if res != "audio":
                cmd += ["--merge-output-format", "mp4", "--ppa", "Merger:-c copy"]
            cmd += [
                "--output", out_tpl,
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
            except OSError as e:
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
                    _log.debug("swallowed: %s", e)
                return
            # parse the DLTRACK line so single-video downloads
            # land in the videos index, the Recent tab, and the FTS
            # index — same as channel-sync downloads. Previously this
            # loop just echoed stdout to the log and the file ended up
            # on disk invisible to the Browse grid / Recent / Search.
            _dltrack = None
            _stderr_errors = []
            # Regexes for parsing yt-dlp progress + destination lines.
            _dest_re = _re.compile(r"^\[download\]\s+Destination:\s+(.+)$")
            _pct_re = _re.compile(r"^\[download\]\s+(\d+(?:\.\d+)?)%")
            try:
                for line in proc.stdout:
                    _line = line.rstrip()
                    # Always feed dim stdout so verbose mode still sees
                    # yt-dlp's raw output. Simple mode filters dim out.
                    self._log_stream.emit_dim(" " + _line)
                    if _line.startswith("DLTRACK:::"):
                        _dltrack = _line
                    # Capture filename from yt-dlp's Destination line
                    # and switch the inplace [Dwnld] line to show it.
                    _m_dest = _dest_re.match(_line)
                    if _m_dest:
                        _state["fname"] = os.path.basename(_m_dest.group(1).strip())
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
                            _stderr_errors.append("YouTube wants a sign-in (Firefox/Chrome cookies not found or expired)")
                # Wait with a generous watchdog timeout (15 minutes).
                # Without this, a wedged yt-dlp (network stall, post-
                # processor hang, sign-in prompt) keeps the in-flight
                # URL stuck in _archive_single_inflight forever —
                # "Already downloading" until app restart.
                try:
                    proc.wait(timeout=900)
                except _sp.TimeoutExpired:
                    try:
                        proc.terminate()
                        proc.wait(timeout=5)
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                    try:
                        if proc.poll() is None:
                            proc.kill()
                            proc.wait(timeout=2)
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
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
                        f"[Dwnld] Watchdog: yt-dlp hung; killed after 15 min.")
                # audit DT-1: only write to URL history now that the
                # download actually ran. Previously written on submit,
                # which polluted history with any URL the user
                # clicked even if it failed.
                if proc.returncode == 0:
                    try:
                        self._push_url_history(url)
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                # Post-download bookkeeping — emulate the channel-sync
                # path's register_video + _record_recent_download hooks.
                if _dltrack:
                    try:
                        parts = _dltrack.split(":::")
                        # Format: "DLTRACK" + title, uploader, upload_date,
                        # filesize, duration, id
                        # audit DT-8: guard against yt-dlp output format
                        # changes / missing fields. The hard-indexed
                        # parts[1]/parts[6] would raise IndexError and
                        # abort post-download indexing silently.
                        if len(parts) < 7:
                            self._log_stream.emit_dim(
                                f" (DLTRACK parsing: only {len(parts)} "
                                f"parts, expected 7 — indexing skipped)")
                            raise ValueError("dltrack parse")
                        _title = parts[1] if len(parts) > 1 else ""
                        _uploader = parts[2] if len(parts) > 2 else ""
                        _vid = parts[6] if len(parts) > 6 else ""
                        # Resolve the final filepath on disk. Template
                        # was `<title> (MM.DD.YY).ext` or `<title>.ext`
                        # under `base`. Scan `base` for something
                        # matching the video ID / title.
                        # when multiple candidate files
                        # match, prefer the NEWEST by mtime (this is
                        # the fresh download). Old code picked the
                        # first glob hit, which for users with two
                        # copies pointed at the stale/duplicate file.
                        # title-prefix fallback now requires
                        # a full-title match (not just first 50 chars)
                        # so series with similar long-title prefixes
                        # ("Video 1:...", "Video 2:...") don't collide.
                        final_path = ""
                        try:
                            import glob as _glob
                            _vid_candidates = []
                            if _vid:
                                for _g in _glob.glob(os.path.join(base, "*")):
                                    if _vid in os.path.basename(_g):
                                        _vid_candidates.append(_g)
                            if _vid_candidates:
                                _vid_candidates.sort(
                                    key=lambda p: os.path.getmtime(p)
                                    if os.path.isfile(p) else 0,
                                    reverse=True)
                                final_path = _vid_candidates[0]
                            if not final_path and _title:
                                # Fallback: match by full sanitized title.
                                _title_sane = _re.sub(
                                    r'[<>:"/\\|?*\x00-\x1f]', "_", _title)
                                _title_candidates = []
                                for _g in _glob.glob(os.path.join(base, "*")):
                                    _stem = os.path.splitext(
                                        os.path.basename(_g))[0]
                                    # Match if the stem EQUALS the full
                                    # sanitized title (possibly with a
                                    # " (MM.DD.YY)" date suffix stripped).
                                    _stem_no_date = _re.sub(
                                        r"\s*\(\d{2}\.\d{2}\.\d{2}\)\s*$",
                                        "", _stem)
                                    if _stem == _title_sane or _stem_no_date == _title_sane:
                                        _title_candidates.append(_g)
                                if _title_candidates:
                                    _title_candidates.sort(
                                        key=lambda p: os.path.getmtime(p)
                                        if os.path.isfile(p) else 0,
                                        reverse=True)
                                    final_path = _title_candidates[0]
                        except Exception as e:
                            _log.debug("swallowed: %s", e)
                        if final_path and os.path.isfile(final_path):
                            _channel_name = _uploader or "Single Videos"
                            try:
                                from backend import index as _idx
                                _idx.register_video(
                                    final_path, _channel_name, _title,
                                    tx_status="no_captions",
                                    video_id=_vid)
                            except Exception as _re:
                                self._log_stream.emit_dim(
                                    f" (index register failed: {_re})")
                            try:
                                sync_backend._record_recent_download(
                                    final_path, _channel_name, _title, _vid)
                            except Exception as _re:
                                self._log_stream.emit_dim(
                                    f" (recent downloads write failed: {_re})")
                            # Drop from deferred livestream journal if
                            # this was a previously-deferred premiere
                            # that's now finished (matches bug C-3).
                            if _vid:
                                try:
                                    from backend import livestreams as _ls
                                    _ls.drop(_vid)
                                except Exception as e:
                                    _log.debug("swallowed: %s", e)
                    except Exception as _pe:
                        self._log_stream.emit_dim(
                            f" (DLTRACK post-processing failed: {_pe})")
                # if a DLTRACK line never arrived AND yt-dlp
                # logged a known-failure pattern, report that to the
                # user via a visible error line + toast so they know
                # why their download vanished.
                if not _dltrack and _stderr_errors:
                    _reason = _stderr_errors[0]
                    # Replace the inplace [Dwnld] line with a red failure.
                    self._log_stream.emit([
                        ["[Dwnld] ", ["red", _marker_tag]],
                        [f"{_state['fname']} \u2014 failed ({_reason}).\n",
                         ["red", _marker_tag]],
                    ])
                    try:
                        if self._window is not None:
                            import json as _json
                            self._window.evaluate_js(
                                "window._showToast && window._showToast("
                                f"{_json.dumps(f'Download failed: {_reason}')},"
                                " 'error');")
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                else:
                    _emit_dwnld(" - Done.")
                self._log_stream.flush()
                # Push a Recent-tab refresh so the new video appears
                # immediately instead of waiting for the next tab
                # switch. Matches the channel-sync push hook.
                try:
                    self._push_recent_refresh()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            finally:
                # Always release the URL guard, even on exception, so
                # the user can retry without restarting the app.
                try:
                    with self._archive_single_lock:
                        self._archive_single_inflight.discard(url)
                except Exception as e:
                    _log.debug("swallowed: %s", e)

        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}
