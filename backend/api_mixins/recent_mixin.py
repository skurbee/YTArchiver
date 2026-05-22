"""
RecentMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class RecentMixin:

    def get_recent_downloads(self):
        """Return real recent-downloads from config. Empty list when none.

        Earlier builds fell back to a synthetic sample set which populated the
        Recent tab with fake videos the user couldn't delete. Removed.
        """
        cfg = self._config if self._config is not None else load_config()
        return recent_for_ui(cfg)


    def clear_recent_downloads(self):
        """Empty the recent_downloads list. Files on disk are untouched.

        Wired to the Recent tab's "Clear list" button. The previous .txt
        report noted the button did nothing — the API had been missing
        since the pywebview port; the Tkinter version had its own
        equivalent. Returns {ok: bool, error?: str}.
        """
        try:
            from backend.ytarchiver_config import save_config as _sc
            cfg = self._config if self._config is not None else load_config()
            cfg["recent_downloads"] = []
            ok = _sc(cfg)
            if not ok:
                return {"ok": False, "error": "Config write failed."}
            try: self._reload_config()
            except Exception as e: _log.debug("swallowed: %s", e)
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def _push_recent_refresh(self):
        """Re-fetch recent_downloads and push to the UI's Recent grid/list.

        Called from backend.sync._record_recent_download every time a new
        video lands, so the Recent tab updates live ("does the
        Recents tab not auto update/refresh when a download happens?").
        Safe no-op when the window isn't ready yet.
        """
        if self._window is None:
            return
        try:
            import json as _json
            # Reload config fresh since _record_recent_download just wrote
            # to disk; self._config may be stale.
            try: self._reload_config()
            except Exception as e: _log.debug("swallowed: %s", e)
            rows = self.get_recent_downloads() or []
            # default=str so a Path or datetime sneaking into a row
            # doesn't raise TypeError, silently freeze the Recent tab
            # (audit: recent_mixin.py:64-65).
            js = f"window.renderRecentTable && window.renderRecentTable({_json.dumps(rows, default=str)});"
            self._window.evaluate_js(js)
        except Exception as e:
            # Best-effort — never let a UI push crash the download pipeline.
            try: self._log_stream.emit_dim(f"(recent refresh push failed: {e})")
            except Exception as e: _log.debug("swallowed: %s", e)


    # ─── Recent tab actions ────────────────────────────────────────────

    def _recent_lookup_path(self, title, channel):
        """Find the on-disk filepath for a Recent row by title + channel.

        Resolution order:
          1. Config `recent_downloads` explicit path (if file still exists)
          2. Index DB `videos.filepath` by title + channel
          3. Walk the channel folder by title / [videoId] / fuzzy ASCII
             match via `utils.try_find_by_title` — recovers files the user
             manually moved between year/month split layouts.
        """
        cfg = load_config()
        video_id_hint = ""
        # Iterate ALL matching entries — old code returned at the
        # first stored_path that existed, but with duplicates from
        # re-download cycles the FIRST match wasn't always the
        # newest (audit: recent_mixin.py:88-93). Pick the entry
        # whose file exists with the most recent mtime.
        _candidates = []
        for r in cfg.get("recent_downloads", []):
            if r.get("title") == title and r.get("channel") == channel:
                video_id_hint = r.get("video_id", "") or video_id_hint
                _sp = r.get("filepath", "") or ""
                if _sp:
                    _candidates.append(_sp)
        _best = None
        _best_mt = -1.0
        for _sp in _candidates:
            try:
                if os.path.isfile(_sp):
                    _mt = os.path.getmtime(_sp)
                    if _mt > _best_mt:
                        _best_mt = _mt
                        _best = _sp
            except OSError:
                continue
        if _best:
            return _best
        stored_path = _candidates[0] if _candidates else ""
        # DB fallback
        try:
            vids = index_backend.list_recent_videos(limit=500, channel=channel)
            for v in vids:
                if v.get("title") == title:
                    if not video_id_hint:
                        video_id_hint = v.get("video_id", "") or ""
                    if not stored_path:
                        stored_path = v.get("filepath", "") or stored_path
                    if v.get("filepath") and os.path.isfile(v["filepath"]):
                        return v["filepath"]
        except Exception as e:
            _log.debug("swallowed: %s", e)
        # Moved-file recovery — walk the channel folder by title / videoId
        try:
            from backend.utils import try_locate_moved_file
            base = (cfg.get("output_dir") or "").strip()
            if base and channel:
                from backend.sync import channel_folder_name as _cfn
                # Find the channel record to derive the folder name
                ch = None
                for c in cfg.get("channels", []):
                    if (c.get("name") == channel or c.get("folder") == channel):
                        ch = c
                        break
                ch_folder = os.path.join(base, _cfn(ch) if ch else channel)
                found = try_locate_moved_file(stored_path, title, ch_folder,
                                               video_id_hint)
                if found and os.path.isfile(found):
                    return found
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return None


    def recent_play(self, title, channel):
        fp = self._recent_lookup_path(title, channel)
        if not fp:
            return {"ok": False, "error": "File not found"}
        return self.browse_open_video(fp)


    def recent_requeue(self, title, channel):
        """Re-download the YouTube URL stored for this Recent entry.
        Mirrors OLD YTArchiver.py Recent right-click "Re-queue download".

        Returns {ok, queued} or {ok:False, error}.
        """
        try:
            cfg = self._config or load_config()
            for r in cfg.get("recent_downloads", []):
                if r.get("title") == title and r.get("channel") == channel:
                    url = (r.get("video_url") or "").strip()
                    if not url:
                        vid = (r.get("video_id") or "").strip()
                        if vid:
                            url = f"https://www.youtube.com/watch?v={vid}"
                    if not url:
                        return {"ok": False,
                                "error": "No URL saved for this recent entry."}
                    # Delegate to single-video download. Uses the user's
                    # saved video_out_dir + resolution defaults.
                    return self.archive_single_video(url, options={})
            return {"ok": False, "error": "Recent entry not found."}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def recent_resolve(self, title, channel):
        """Return {ok, filepath, video_id} for a Recent row, using the same
        three-step lookup as recent_play (config → DB → fuzzy walk). Used by
        the right-click "Play video" action to hand off to the Browse Watch
        view instead of spawning VLC."""
        fp = self._recent_lookup_path(title, channel)
        if not fp:
            return {"ok": False, "error": "File not found"}
        # Best-effort video_id lookup — check config.recent_downloads first,
        # fall back to the FTS DB row.
        vid = ""
        try:
            cfg = self._config or load_config()
            for r in cfg.get("recent_downloads", []):
                if r.get("title") == title and r.get("channel") == channel:
                    vid = (r.get("video_id") or "").strip()
                    if not vid:
                        # parse from video_url if present
                        import re as _re
                        m = _re.search(r"[?&]v=([A-Za-z0-9_-]{11})",
                                       r.get("video_url") or "")
                        if m: vid = m.group(1)
                    break
        except Exception as e:
            _log.debug("swallowed: %s", e)
        if not vid:
            # Reader connection so this fallback doesn't queue behind
            # writers during startup sweep / ingest.
            try:
                from backend import index as _idx
                rconn = _idx._reader_open()
                if rconn is not None:
                    with _idx._reader_lock:
                        row = rconn.execute(
                            "SELECT video_id FROM videos WHERE title=? AND channel=? "
                            "ORDER BY added_ts DESC LIMIT 1",
                            (title, channel)).fetchone()
                    if row and row[0]:
                        vid = row[0]
            except Exception as e:
                _log.debug("swallowed: %s", e)
        return {"ok": True, "filepath": fp, "video_id": vid}


    def recent_show_in_explorer(self, title, channel):
        fp = self._recent_lookup_path(title, channel)
        if not fp:
            return {"ok": False, "error": "File not found"}
        return self.browse_show_in_explorer(fp)


    def recent_open_youtube(self, title, channel):
        """Open the YouTube page for this recent video (if we have video_id)."""
        import re as _re
        import webbrowser
        # Prefer the video_id stashed on the recent_downloads entry —
        # single-video downloads via archive_single_video write files
        # without a `[VIDEOID]` suffix in the filename, so the regex
        # path used to always fail for those rows even though the ID
        # was sitting right there in config (audit: recent_mixin.py:
        # 213-223).
        try:
            cfg = load_config()
            for r in (cfg.get("recent_downloads") or []):
                if r.get("title") == title and r.get("channel") == channel:
                    vid = (r.get("video_id") or "").strip()
                    if vid:
                        webbrowser.open(f"https://www.youtube.com/watch?v={vid}")
                        return {"ok": True}
                    break
        except Exception as _e:
            _log.debug("swallowed: %s", _e)
        # Filename-suffix fallback for older entries that pre-date the
        # video_id field.
        fp = self._recent_lookup_path(title, channel)
        if fp:
            m = _re.search(r"\[([A-Za-z0-9_-]{11})\]", os.path.basename(fp))
            if m:
                webbrowser.open(f"https://www.youtube.com/watch?v={m.group(1)}")
                return {"ok": True}
        return {"ok": False, "error": "No video ID available"}


    def recent_delete_file(self, title, channel):
        """Delete the file from disk + remove from recent_downloads list."""
        fp = self._recent_lookup_path(title, channel)
        if not fp:
            return {"ok": False, "error": "File not found"}
        # Refuse the destructive os.remove if config writes are blocked
        # — otherwise we delete the file but can't update the
        # recent_downloads list, leaving the user with a stale entry
        # pointing at a missing file with no way to clean it up
        # (audit: recent_mixin H22).
        if not config_is_writable():
            return {"ok": False,
                    "error": "Config is currently read-only "
                             "(write-gate or read-only filesystem). "
                             "Refusing to delete file when the recent "
                             "list can't be updated."}
        try:
            os.remove(fp)
        except OSError as e:
            return {"ok": False, "error": str(e)}
        # Drop sidecars. audit F-24 list lives in utils.delete_video_sidecars.
        from backend.utils import delete_video_sidecars
        delete_video_sidecars(fp)
        # Mirror video_mixin.video_delete_file's index cleanup so the
        # FTS / videos rows tied to this filepath are dropped too.
        # Without this, Browse + Search kept returning "file not
        # found" hits for the deleted file (audit: recent_mixin.py:
        # 226-246).
        try:
            from backend import index as _idx
            _conn = _idx._open()
            if _conn is not None:
                _stem, _ext = os.path.splitext(fp)
                _sidecar = _stem + ".jsonl"
                with _idx._db_lock:
                    _conn.execute(
                        "DELETE FROM segments WHERE jsonl_path = ? COLLATE NOCASE",
                        (_sidecar,))
                    _conn.execute(
                        "DELETE FROM videos WHERE filepath = ? COLLATE NOCASE",
                        (fp,))
                    _conn.commit()
        except Exception as _e:
            _log.debug("swallowed: %s", _e)
        # Remove from recent_downloads (if writable)
        if config_is_writable():
            cfg = load_config()
            cfg["recent_downloads"] = [r for r in cfg.get("recent_downloads", [])
                                        if not (r.get("title") == title and r.get("channel") == channel)]
            from backend.ytarchiver_config import save_config as _sc
            if not _sc(cfg):
                return {"ok": False, "error": "File deleted but config write failed; recent_downloads may show stale entry"}
        return {"ok": True}
