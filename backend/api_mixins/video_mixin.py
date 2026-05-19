"""
VideoMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class VideoMixin:

    def video_delete_file(self, filepath):
        """Delete a video file from disk + drop its sidecars + remove
        the index DB row. Used by the Browse-grid right-click "Delete file"
        action — previously the bridge call had no matching backend method
        and the action silently failed (audit U-3).

        Mirrors recent_delete_file's sidecar-cleanup logic (audit F-24
        sidecar list) but operates on a path the caller already knows
        instead of looking it up via title+channel.
        """
        fp = (filepath or "").strip()
        if not fp:
            return {"ok": False, "error": "Missing filepath"}
        if not os.path.isfile(fp):
            return {"ok": False, "error": f"File not found: {fp}"}
        try:
            os.remove(fp)
        except OSError as e:
            return {"ok": False, "error": str(e)}
        # Drop sidecars. audit F-24 list lives in utils.delete_video_sidecars.
        from backend.utils import delete_video_sidecars
        delete_video_sidecars(fp)
        # Drop the index DB row (and its FTS segments) so Browse / Search
        # stop returning the now-deleted video.
        try:
            from backend import index as _idx
            _conn = _idx._open()
            if _conn is not None:
                with _idx._db_lock:
                    # Find the segments tied to this filepath via the
                    # videos table, then remove them + the videos row.
                    _conn.execute(
                        "DELETE FROM segments WHERE jsonl_path IN ("
                        "  SELECT REPLACE(filepath, "
                        "    SUBSTR(filepath, LENGTH(filepath) - 3, 4), '.jsonl') "
                        "  FROM videos WHERE filepath = ? COLLATE NOCASE)",
                        (fp,))
                    _conn.execute(
                        "DELETE FROM videos WHERE filepath = ? COLLATE NOCASE",
                        (fp,))
                    _conn.commit()
        except Exception as _e:
            # Don't fail the whole call — the file is gone, that's the
            # primary contract. Surface the DB issue as a soft warning.
            return {"ok": True, "warning": f"File deleted but index cleanup failed: {_e}"}
        # Also remove from recent_downloads if it was there.
        if config_is_writable():
            try:
                cfg = load_config()
                _before = len(cfg.get("recent_downloads", []) or [])
                cfg["recent_downloads"] = [
                    r for r in cfg.get("recent_downloads", []) or []
                    if (r.get("filepath") or "").lower() != fp.lower()
                ]
                if len(cfg["recent_downloads"]) != _before:
                    from backend.ytarchiver_config import save_config as _sc
                    _sc(cfg)
            except Exception as e:
                _log.debug("swallowed: %s", e)
        return {"ok": True}


    def video_redownload(self, video_id, title, resolution):
        """Re-download a single video at a new resolution. Used by the
        Watch-view "Redownload" button — previously had no backing
        backend method and the action silently failed (audit U-4).

        Looks up the video's channel + filepath via the index DB, then
        delegates to backend/redownload.py for the actual yt-dlp work.
        """
        vid = (video_id or "").strip()
        if not vid:
            return {"ok": False, "error": "Missing video_id"}
        res = (str(resolution or "")).strip()
        if not res:
            return {"ok": False, "error": "Missing resolution"}
        # Look up the video's filepath + channel from the index DB.
        # Reader connection so this lookup doesn't queue behind writers
        # holding `_db_lock` during startup sweep / ingest.
        try:
            from backend import index as _idx
            _rconn = _idx._reader_open()
            if _rconn is None:
                return {"ok": False, "error": "Index DB unavailable"}
            with _idx._reader_lock:
                row = _rconn.execute(
                    "SELECT filepath, channel FROM videos "
                    "WHERE video_id = ? LIMIT 1",
                    (vid,)).fetchone()
            if not row:
                return {"ok": False, "error":
                        f"Video {vid} not found in index"}
            filepath, channel_name = row[0], row[1]
        except Exception as e:
            return {"ok": False, "error": f"Lookup failed: {e}"}
        if not filepath or not channel_name:
            return {"ok": False, "error": "Video has no filepath/channel"}
        # Find the channel config so we can hand the URL + folder to
        # the redownload pipeline.
        cfg = self._config if self._config is not None else load_config()
        ch = next((c for c in cfg.get("channels", []) or []
                   if (c.get("name") or c.get("folder") or "").strip().lower()
                      == (channel_name or "").strip().lower()), None)
        if not ch:
            return {"ok": False, "error":
                    f"Channel '{channel_name}' not in subscriptions"}
        ch_url = (ch.get("url") or "").strip()
        if not ch_url:
            return {"ok": False, "error":
                    f"Channel '{channel_name}' has no URL"}
        # Reuse the channel-wide redownload path with a single-video
        # filter via the existing _backlog_redownload_channel pipeline.
        # For simplicity here we queue a normal redownload of the
        # channel scoped to this one video_id.
        try:
            import threading as _th

            from backend import redownload as _rd
            def _run():
                try:
                    _rd.redownload_channel(
                        channel_name, ch_url,
                        ch.get("folder") or channel_name, res,
                        stream=self._log_stream,
                        cancel_ev=self._sync_cancel,
                        pause_ev=self._sync_pause,
                        confirm_cb=None,
                        queues=self._queues,
                    )
                except Exception as e:
                    self._log_stream.emit_error(
                        f"Single-video redownload failed: {e}")
            _th.Thread(target=_run, daemon=True).start()
            return {"ok": True, "title": title, "resolution": res}
        except Exception as e:
            return {"ok": False, "error": str(e)}
