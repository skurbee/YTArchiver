"""
TranscribeMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class TranscribeMixin:

    # ─── Transcribe ─────────────────────────────────────────────────────

    def transcribe_enqueue(self, path, title=""):
        """Queue a video for transcription."""
        try:
            ok = self._transcribe.enqueue(path, title)
        except Exception as _e:
            # surface the error instead of the silent
            # {ok: False} that the old code returned. Caller can
            # toast the actual reason (file not found, no whisper
            # worker, etc.) rather than a generic failure.
            return {"ok": False, "error": str(_e)}
        # audit L-13/L-14: nudge the UI queue popover so freshly-
        # enqueued items show up immediately instead of waiting for
        # the next automatic poll (~500ms).
        try:
            self._on_queue_changed()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return {"ok": ok}


    def transcribe_folder(self):
        """Prompt for a folder, recursively queue every untranscribed video.

        Mirrors YTArchiver.py:16505 _run_manual_transcription_folder. Skips
        files that already have a .jsonl sidecar.

        Both the modal dialog AND the folder walk now run on a worker
        thread. Previously the dialog was opened on the js_api bridge
        thread, freezing the UI thread for the entire duration the user
        was in the native folder picker (anywhere from a few seconds to
        a minute on a slow system). With the dialog on a worker thread,
        the bridge returns immediately and result toast/log lines push
        via evaluate_js when work finishes.
        """
        if self._window is None:
            return {"ok": False, "error": "No window"}

        def _run():
            try:
                import webview as _wv
                paths = self._window.create_file_dialog(_wv.FOLDER_DIALOG)
            except Exception as e:
                self._log_stream.emit_error(
                    f"[GPU] transcribe_folder dialog failed: {e}")
                self._log_stream.flush()
                return
            if not paths:
                return  # user cancelled
            folder = paths if isinstance(paths, str) else paths[0]
            queued = 0
            skipped = 0
            for dp, _dns, fns in os.walk(folder):
                for fn in fns:
                    if not fn.lower().endswith((".mp4", ".mkv", ".webm", ".m4a", ".mov")):
                        continue
                    video = os.path.join(dp, fn)
                    base = os.path.splitext(video)[0]
                    if os.path.isfile(base + ".jsonl"):
                        skipped += 1
                        continue
                    title = os.path.splitext(fn)[0]
                    self._transcribe.enqueue(video, title)
                    queued += 1
            self._log_stream.emit([
                ["[GPU] ", "trans_bracket"],
                [f"Transcribe folder \u2014 {os.path.basename(folder)}: ", "simpleline_blue"],
                [f"{queued} queued, {skipped} already done\n", "simpleline"],
            ])
            self._log_stream.flush()
        threading.Thread(target=_run, daemon=True,
                         name="transcribe-folder-dialog").start()
        return {"ok": True, "started": True}


    def transcribe_retranscribe(self, path, title="", video_id="",
                                 _on_complete_extra=None):
        """Queue a re-transcription of a video with the current Whisper model.
        Mirrors YTArchiver.py:16369 `_run_retranscribe_job`.

        Transcripts live in AGGREGATED per-folder files (one `.txt` and
        one hidden `.jsonl` per channel / year / month folder, containing
        entries for every video in that folder). So "re-transcribe" is
        NOT a delete-and-rebuild — it's a surgical swap:

          1. Run Whisper on the video file.
          2. In the aggregated `.jsonl`: remove the old line for this
             video_id + title, append the new segments.
          3. In the aggregated `.txt`: remove the old `===…===\\n<body>\\n\\n`
             block, append the new one (preserving date + duration from
             the old header so provenance survives the swap).
          4. Re-ingest the `.jsonl` so FTS reflects the new segments.

        All four steps happen inside the transcribe worker once the
        Whisper pass finishes (see `_write_outputs(retranscribe=True)`
        in transcribe.py). This Api just queues the job.
        """
        if not path or not os.path.isfile(path):
            try:
                self._log_stream.emit_text(
                    f" — Re-transcribe rejected: file not found — "
                    f"{title or path}", "red")
            except Exception:
                pass
            return {"ok": False, "error": "File not found"}
        # Extension check — Whisper would otherwise spend minutes
        # failing on a JSON sidecar or arbitrary text file passed via
        # a malformed call (audit: transcribe_mixin H19).
        _MEDIA_EXTS = (".mp4", ".mkv", ".webm", ".m4a", ".mov",
                       ".avi", ".mp3", ".wav", ".flac", ".m4v", ".wmv")
        if not path.lower().endswith(_MEDIA_EXTS):
            try:
                self._log_stream.emit_text(
                    f" — Re-transcribe rejected: not a media file — "
                    f"{title or path}", "red")
            except Exception:
                pass
            return {"ok": False,
                    "error": "Not a media file (expected .mp4/.mkv/.webm/etc)"}
        # Best-effort derive the video_id if the caller didn't supply one.
        # The replace helpers use it to catch title-drifted stale entries
        # that a title-only match would miss. Lookup order mirrors
        # `_write_outputs`:
        # hint → `[videoId]` suffix on filename → FTS videos table.
        # Also look up the channel name from the index DB so the
        # [Trnscr] activity-log row shows the channel instead of
        # em-dash. "no channel name?"
        vid_id = (video_id or "").strip()
        channel_name = ""
        if not vid_id:
            import re as _re
            stem = os.path.splitext(os.path.basename(path))[0]
            m = _re.search(r"\[([A-Za-z0-9_-]{11})\]\s*$", stem)
            if m:
                vid_id = m.group(1)
        # Use the dedicated reader connection so this SELECT doesn't
        # queue behind sweep_new_videos / ingest_jsonl / register_video
        # writers holding `_db_lock`. The previous code path took the
        # writer lock for a single-row SELECT, which during startup
        # (background sweep + FTS ingest running) could block for
        # several minutes — exactly long enough for the user to think
        # the click did nothing. WAL mode means readers on a separate
        # connection never wait on writers at the SQLite layer.
        try:
            from backend.index import _reader_lock, _reader_open
            rconn = _reader_open()
            if rconn is not None:
                # Look up both normpath AND the raw path COLLATE NOCASE.
                # Stored rows may have been inserted with a different
                # slash direction than the JS-supplied path (audit:
                # transcribe_mixin.py:147), and COLLATE NOCASE handles
                # case but not slash mixing.
                _np = os.path.normpath(path)
                with _reader_lock:
                    row = rconn.execute(
                        "SELECT video_id, channel FROM videos WHERE filepath=? "
                        "COLLATE NOCASE LIMIT 1",
                        (_np,)).fetchone()
                    if not row and _np != path:
                        row = rconn.execute(
                            "SELECT video_id, channel FROM videos WHERE filepath=? "
                            "COLLATE NOCASE LIMIT 1",
                            (path,)).fetchone()
                if row:
                    if not vid_id and row[0]:
                        vid_id = row[0]
                    if row[1]:
                        channel_name = row[1]
        except Exception as e:
            _log.debug("swallowed: %s", e)
        # Completion hook: push a JS event when the job finishes so the
        # Watch view can refetch the transcript + re-render its source
        # banner (replacing the "approximate" warning with the new
        # Whisper banner). Mirrors ArchivePlayer's `_ytStartProgressPoll`
        # transition-detection pattern but reactive instead of polled.
        _self = self
        _vid = vid_id
        _path = os.path.normpath(path)
        def _on_done(_result):
            try:
                if _self._window is not None:
                    import json as _json
                    payload = _json.dumps({"video_id": _vid, "filepath": _path})
                    _self._window.evaluate_js(
                        f"if (window._onRetranscribeComplete) "
                        f"window._onRetranscribeComplete({payload});")
            except Exception as e:
                _log.debug("swallowed: %s", e)
            # Extra hook for callers (e.g. _handle_retranscribe model
            # restore — audit: main.py H20). Always fires whether or
            # not the JS push above succeeded.
            try:
                if callable(_on_complete_extra):
                    _on_complete_extra(_result)
            except Exception as e:
                _log.debug("swallowed: %s", e)
        ok = self._transcribe.enqueue(
            path,
            title or os.path.basename(os.path.splitext(path)[0]),
            channel=channel_name,
            retranscribe=True,
            video_id=vid_id,
            on_complete=_on_done,
        )
        # If enqueue rejected the job (queue full, manager down), fire
        # the completion hook synchronously so the JS Watch view's
        # pending state clears instead of spinning forever (audit:
        # transcribe_mixin H19).
        if not ok:
            try:
                _on_done({"ok": False, "error": "enqueue rejected"})
            except Exception as e:
                _log.debug("swallowed: %s", e)
        # Visible log line so the user can see the click was honored,
        # even when Whisper isn't loading immediately (GPU Auto off,
        # already-running job, etc.). Previously a successful enqueue
        # was silent — the only feedback was the toast, which is easy
        # to miss if it fires under a modal or off-screen.
        if ok:
            try:
                _disp = title or os.path.basename(path)
                _ch = f" ({channel_name})" if channel_name else ""
                self._log_stream.emit_text(
                    f" — Queued re-transcribe: {_disp}{_ch}",
                    "simpleline_blue")
            except Exception:
                pass
        return {"ok": ok, "video_id": vid_id}


    def transcribe_queue_size(self):
        return {"size": self._transcribe.queue_size()}


    def transcribe_cancel_all(self):
        self._transcribe.cancel_all()
        return {"ok": True}


    def transcribe_available(self):
        """Check whether YTArchiver can run whisper (needs Python 3.11)."""
        # Cache worker_script.exists() — Path.exists() hits the FS
        # every call, and on a slow mount this added UI hitch every
        # time the Transcribe section refreshed (audit:
        # transcribe_mixin.py:206-208).
        _cached = getattr(self, "_worker_script_exists_cached", None)
        if _cached is None:
            try:
                _cached = bool(self._transcribe._worker_script.exists())
            except Exception:
                _cached = False
            self._worker_script_exists_cached = _cached
        return {
            "ok": self._transcribe.is_available(),
            "python311": self._transcribe._python311,
            "worker_script_exists": _cached,
        }


    def transcribe_swap_model(self, new_model, persist=True):
        """Swap the whisper model mid-queue. Current job finishes; next job
        picks up the new model.

        `persist=True` (default, used by the GPU popover's "set default"
        dropdown): also saves the new model as `whisper_model` in config
        so future launches use it by default.

        `persist=False` (used by the one-off re-transcribe model picker
        modal): only swaps the runtime model — doesn't touch the
        Settings default. "manual retranscriptions have nothing
        to do with that [settings default] and should have no influence
        on that setting."
        """
        if not new_model or new_model not in ("tiny", "small", "medium", "large-v3"):
            return {"ok": False, "error": "Unsupported model"}
        ok = self._transcribe.swap_model(new_model)
        if ok and persist:
            if self._config is not None:
                self._config["whisper_model"] = new_model
            # Acquire the same settings_save lock so a parallel
            # settings_save can't load_config, see the OLD whisper
            # model, mutate, and clobber our write (audit:
            # transcribe_mixin.py:212-239).
            try:
                from backend.api_mixins.settings_mixin import SettingsMixin
                _lock = SettingsMixin._settings_save_lock
            except Exception:
                _lock = None
            try:
                from backend.ytarchiver_config import save_config as _sc
                if _lock is not None:
                    with _lock:
                        cfg = load_config()
                        cfg["whisper_model"] = new_model
                        _sc(cfg)
                else:
                    cfg = load_config()
                    cfg["whisper_model"] = new_model
                    _sc(cfg)
            except Exception as e:
                _log.debug("swallowed: %s", e)
        return {"ok": ok, "model": new_model, "persisted": bool(ok and persist)}


    def transcribe_current_model(self):
        """Return the model the transcribe manager will use for the next job."""
        return {"model": self._transcribe.current_model()}
