"""
VideoMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

import os
import re
import threading

from ._shared import _log
from backend.ytarchiver_config import config_is_writable, load_config, save_config


class VideoMixin:

    def video_delete_file(self, filepath):
        """Move a video file to app trash and remove
        the index DB row. Used by the Browse-grid right-click "Delete file"
        action — previously the bridge call had no matching backend method
        and the action silently failed (audit U-3).

        Mirrors recent_delete_file's sidecar-trash logic (audit F-24
        sidecar list) but operates on a path the caller already knows
        instead of looking it up via title+channel.
        """
        fp = (filepath or "").strip()
        if not fp:
            return {"ok": False, "error": "Missing filepath"}
        if not os.path.isfile(fp):
            return {"ok": False, "error": f"File not found: {fp}"}
        # Defense-in-depth: the JS bridge is the trust boundary, so refuse to
        # os.remove a path resolving OUTSIDE the archive roots this app
        # manages — a crafted/compromised filepath must not delete arbitrary
        # files (audit: video_mixin containment).
        from backend.services.file_ops import safe_trash_video_file
        trashed = safe_trash_video_file(
            fp, require_config_writable=True, reason="video_delete_file")
        if not trashed.get("ok"):
            return trashed
        # Refuse the destructive os.remove if config writes are blocked
        # — see recent_mixin H22 for the same precondition.
        # Drop sidecars. audit F-24 list lives in utils.delete_video_sidecars.
        # Drop the index DB row (and its FTS segments) so Browse / Search
        # stop returning the now-deleted video.
        try:
            from backend import index as _idx
            _conn = _idx._open()
            if _conn is not None:
                # Sidecar .jsonl path was derived via SQL REPLACE() on the
                # 4-char extension — but SQLite's REPLACE swaps ALL
                # occurrences of the substring (audit: video_mixin.py:46-55).
                # A path like "C:\.mp4-archive\foo.mp4" mangled to
                # "C:\.jsonl-archive\foo.jsonl" and the DELETE missed.
                # Compute the sidecar path in Python and parameterize it
                # straight into the IN clause.
                # FTS-safe, video_id-keyed segment removal — see
                # index.delete_segments_for_video (the old per-video
                # jsonl_path DELETE was a no-op in the aggregated
                # layout and skipped the FTS5 'delete' sync on legacy
                # rows). Runs BEFORE the videos row drop (the helper
                # resolves video_id from it) and takes _db_lock itself.
                _idx.delete_segments_for_video(fp)
                with _idx._db_lock:
                    _conn.execute(
                        "DELETE FROM videos WHERE filepath = ? COLLATE NOCASE",
                        (fp,))
                    _conn.commit()
        except Exception as _e:
            # Don't fail the whole call — the file is gone, that's the
            # primary contract. Surface the DB issue as a soft warning.
            warning = f"File moved to trash but index cleanup failed: {_e}"
            return {"ok": False, "file_trashed": True,
                    "cleanup_failed": True, "error": warning,
                    "warning": warning,
                    "trashed_file_path": trashed.get("trashed_file_path"),
                    "trashed_folder_path": trashed.get("trashed_folder_path")}
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
        return {"ok": True,
                "trashed_file_path": trashed.get("trashed_file_path"),
                "trashed_folder_path": trashed.get("trashed_folder_path")}


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
                # Friendlier message during transient startup state —
                # the index DB takes a moment to come up after launch
                # and the old terse "Index DB unavailable" gave no
                # hint that retrying would help (audit: video_mixin.
                # py:107).
                return {"ok": False,
                        "error": "Index is still initializing — try again "
                                 "in a moment."}
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
        # redownload_channel scans + os.path.isdir() the folder, so it needs
        # the FULL channel-root path (output_dir + folder), NOT a bare folder
        # name. Passing the bare name made single-video redownload abort with
        # "folder not found: <name>" even though the folder exists under
        # output_dir. Build the full path the same way reorg/sync do.
        import os as _os

        from backend.sync import channel_folder_name as _cfn
        _base = (cfg.get("output_dir") or "").strip()
        try:
            _ch_folder = (_os.path.join(_base, _cfn(ch)) if _base
                          else (ch.get("folder") or channel_name))
        except Exception:
            _ch_folder = ch.get("folder") or channel_name
        # Redownload JUST this one video: reuse the channel redownload
        # pipeline (match → containment → replace) but pass only_video_id so it
        # filters to this single file instead of re-downloading the WHOLE
        # channel (audit r2: this per-video button was a whole-channel redownload).
        try:
            import threading as _th

            from backend import redownload as _rd
            # Per-run event — the shared _sync_cancel stays set after
            # any stopped sync, ghost-cancelling this single-video
            # redownload instantly in that window.
            _vid_cancel = _th.Event()
            def _run():
                try:
                    _rd.redownload_channel(
                        channel_name, ch_url,
                        _ch_folder, res,
                        stream=self._log_stream,
                        cancel_ev=_vid_cancel,
                        pause_ev=self._sync_pause,
                        confirm_cb=None,
                        queues=self._queues,
                        only_video_id=vid,
                    )
                except Exception as e:
                    self._log_stream.emit_error(
                        f"Single-video redownload failed: {e}")
            _th.Thread(target=_run, daemon=True).start()
            return {"ok": True, "title": title, "resolution": res}
        except Exception as e:
            return {"ok": False, "error": str(e)}
