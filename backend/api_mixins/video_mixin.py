"""
VideoMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They prefer AppServices when present, with legacy
private Api attributes kept as fallback state.
"""
from __future__ import annotations

import os
import threading
import uuid

from backend.services.managed_work import start_managed_task
from backend.ytarchiver_config import (
    config_is_writable,
    load_config,
    save_config,
    update_config,
)

from ._shared import _log


class VideoMixin:
    def _video_services(self):
        return getattr(self, "services", None)

    def _video_config(self):
        services = self._video_services()
        if services is not None:
            return services.fresh_config()
        cfg = getattr(self, "_config", None)
        if cfg is not None:
            return cfg
        return load_config()

    def _video_save_config(self, cfg):
        services = self._video_services()
        if services is not None:
            return services.save_config(cfg)
        return save_config(cfg)

    def _video_update_config(self, mutator):
        """Commit one catalog-adjacent patch against current config."""
        services = self._video_services()
        mutate = (getattr(services, "mutate_config", None)
                  if services is not None else None)
        if callable(mutate):
            return mutate(mutator)
        return update_config(mutator)

    def _video_log_stream(self):
        services = self._video_services()
        stream = (getattr(services, "log_stream", None)
                  if services is not None else None)
        return stream if stream is not None else self._log_stream

    def _video_queues(self):
        services = self._video_services()
        queues = (getattr(services, "queues", None)
                  if services is not None else None)
        return queues if queues is not None else self._queues


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
            return {"ok": False, "error": "Select a saved video first."}
        from backend import index as _idx
        prepared = _idx.prepare_media_copy_deletion(fp)
        if not prepared.get("ok"):
            return {
                "ok": False,
                "error": prepared.get("error") or
                         "Could not preserve the logical transcript.",
            }
        if not os.path.isfile(fp):
            # The file is already gone, so there is nothing destructive to do
            # on disk. Treat Delete as "remove stale catalog entry" instead of
            # trapping the user with an undeletable ghost card forever.
            try:
                _prepared_kw = ({"prepared": prepared}
                                if prepared.get("row_identity") else {})
                cleanup = _idx.delete_media_copy(fp, **_prepared_kw)
                if not cleanup.get("ok"):
                    _idx.finalize_copy_deletion_preparation(prepared)
                    return {"ok": False,
                            "error": cleanup.get("error") or
                                     "Index cleanup failed"}
                if not cleanup.get("found"):
                    _idx.finalize_copy_deletion_preparation(prepared)
                    return {"ok": True, "stale_entry_removed": False,
                            "message": "File and catalog entry are already gone."}
            except Exception as _e:
                _idx.finalize_copy_deletion_preparation(prepared)
                return {"ok": False,
                        "error": f"Could not remove stale catalog entry: {_e}"}
            _idx.finalize_copy_deletion_preparation(prepared)
            if config_is_writable():
                try:
                    def _drop_stale_recent(cfg):
                        cfg["recent_downloads"] = [
                            r for r in cfg.get("recent_downloads", []) or []
                            if (r.get("filepath") or "").lower() != fp.lower()
                        ]

                    self._video_update_config(_drop_stale_recent)
                except Exception as e:
                    _log.debug("stale recent cleanup failed: %s", e)
            return {"ok": True, "stale_entry_removed": True,
                    "message": "Stale Browse entry removed; no file existed on disk."}
        # Browse can contain legacy/manual catalog rows whose exact file lives
        # outside every *current* archive root (for example after a custom
        # Save-to folder changes).  The catalog row authorizes removing that
        # row, never deleting the external file.  This branch is intentionally
        # before safe_trash_video_file: quarantine needs a configured owning
        # root and must continue to reject arbitrary external paths.
        from backend.services import file_ops as _file_ops
        managed_guard = _file_ops.assert_within_managed_roots(fp)
        if not managed_guard.get("ok"):
            if not isinstance(prepared.get("row_identity"), dict):
                return managed_guard
            try:
                cleanup = _idx.delete_media_copy(fp, prepared=prepared)
            except Exception as _e:
                cleanup = {"ok": False, "error": str(_e)}
            if not cleanup.get("ok"):
                _idx.rollback_copy_deletion_preparation(prepared)
                return {
                    "ok": False,
                    "error": cleanup.get("error") or
                             "Could not remove the catalog entry.",
                }
            finalized = _idx.finalize_copy_deletion_preparation(prepared)
            if config_is_writable():
                try:
                    def _drop_external_recent(cfg):
                        cfg["recent_downloads"] = [
                            r for r in cfg.get("recent_downloads", []) or []
                            if (r.get("filepath") or "").lower() != fp.lower()
                        ]

                    self._video_update_config(_drop_external_recent)
                except Exception as e:
                    _log.debug("external recent cleanup failed: %s", e)
            response = {
                "ok": True,
                "catalog_entry_removed": True,
                "external_file_preserved": True,
                "message": (
                    "Removed from YTArchiver. The external file was left "
                    "in place."
                ),
            }
            if not finalized.get("ok"):
                response["warning"] = (
                    "The catalog entry was removed and the external file was "
                    "left in place, but transcript cleanup needs attention."
                )
            return response
        # Defense-in-depth: the JS bridge is the trust boundary, so refuse to
        # os.remove a path resolving OUTSIDE the archive roots this app
        # manages — a crafted/compromised filepath must not delete arbitrary
        # files (audit: video_mixin containment).
        from backend.services.file_ops import safe_trash_video_file
        trashed = safe_trash_video_file(
            fp, require_config_writable=True, reason="video_delete_file",
            excluded_sidecar_paths=prepared.get("preserved_sidecar_paths"),
            catalog_context=prepared.get("row_identity"))
        if not trashed.get("ok"):
            if trashed.get("rollback_failed"):
                _idx.finalize_copy_deletion_preparation(prepared)
            else:
                _idx.rollback_copy_deletion_preparation(prepared)
            return trashed
        # Refuse the destructive os.remove if config writes are blocked
        # — see recent_mixin H22 for the same precondition.
        # Drop sidecars. audit F-24 list lives in utils.delete_video_sidecars.
        # Drop the index DB row (and its FTS segments) so Browse / Search
        # stop returning the now-deleted video.
        try:
            _prepared_kw = ({"prepared": prepared}
                            if prepared.get("row_identity") else {})
            cleanup = _idx.delete_media_copy(fp, **_prepared_kw)
            if not cleanup.get("ok"):
                raise RuntimeError(
                    cleanup.get("error") or "Index cleanup failed")
        except Exception as _e:
            finalized = _idx.finalize_copy_deletion_preparation(prepared)
            # Don't fail the whole call — the file is gone, that's the
            # primary contract. Surface the DB issue as a soft warning.
            _log.warning("video_delete_file library cleanup failed: %s", _e)
            if not finalized.get("ok"):
                _log.warning(
                    "video_delete_file recovery-marker cleanup failed: %s",
                    finalized.get("error") or "unknown error",
                )
            warning = (
                "The video was moved to Trash, but Browse and Search could "
                "not be updated. Run Rescan archive to finish cleanup."
            )
            return {"ok": False, "file_trashed": True,
                    "cleanup_failed": True, "error": warning,
                    "warning": warning,
                    "trashed_file_path": trashed.get("trashed_file_path"),
                     "trashed_folder_path": trashed.get("trashed_folder_path")}
        finalized = _idx.finalize_copy_deletion_preparation(prepared)
        # Also remove from recent_downloads if it was there.
        if config_is_writable():
            try:
                def _drop_recent(cfg):
                    cfg["recent_downloads"] = [
                        r for r in cfg.get("recent_downloads", []) or []
                        if (r.get("filepath") or "").lower() != fp.lower()
                    ]

                self._video_update_config(_drop_recent)
            except Exception as e:
                _log.debug("swallowed: %s", e)
        response = {"ok": True,
                "trashed_file_path": trashed.get("trashed_file_path"),
                "trashed_folder_path": trashed.get("trashed_folder_path")}
        if not finalized.get("ok"):
            _log.warning(
                "video_delete_file recovery-marker cleanup failed: %s",
                finalized.get("error") or "unknown error",
            )
            response["warning"] = (
                "The video was moved to Trash, but YTArchiver could not "
                "finish its cleanup. Restart YTArchiver and run Rescan archive."
            )
        return response


    def video_redownload(self, video_id, title, resolution):
        """Re-download a single video at a new resolution. Used by the
        Watch-view "Redownload" button — previously had no backing
        backend method and the action silently failed (audit U-4).

        Looks up the video's channel + filepath via the index DB, then
        delegates to backend/redownload.py for the actual yt-dlp work.
        """
        admission = getattr(self, "_work_admission_error", None)
        if callable(admission):
            blocked = admission("a video redownload")
            if blocked is not None:
                return blocked
        vid = (video_id or "").strip()
        if not vid:
            return {"ok": False,
                    "error": "This video does not have a YouTube ID."}
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
            return {"ok": False,
                    "error": "YTArchiver could not find this video's saved file."}
        # Find the channel config so we can hand the URL + folder to
        # the redownload pipeline.
        cfg = self._video_config()
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
        # Route Browse/Watch work through the shared sync/redownload lane.
        # When a sync is already active this becomes a visible queued task and
        # starts only after the current pass releases the lane.
        queue_redownload = getattr(self, "chan_redownload", None)
        if callable(queue_redownload):
            return queue_redownload(
                {"name": channel_name},
                res,
                only_video={
                    "video_id": vid,
                    "filepath": filepath,
                    "title": title,
                },
            )

        # Isolated VideoMixin users/tests do not compose ChannelMixin. Keep a
        # direct fallback, still using the exact-file fast path.
        try:
            from backend import redownload as _rd
            log_stream = self._video_log_stream()
            queues = self._video_queues()
            # Per-run event — the shared _sync_cancel stays set after
            # any stopped sync, ghost-cancelling this single-video
            # redownload instantly in that window.
            _vid_cancel = threading.Event()
            task_id = f"video-redownload-{uuid.uuid4().hex}"
            def _run():
                try:
                    _rd.redownload_channel(
                        channel_name, ch_url,
                        _ch_folder, res,
                        stream=log_stream,
                        cancel_ev=_vid_cancel,
                        pause_ev=self._sync_pause,
                        confirm_cb=None,
                        queues=queues,
                        only_video_id=vid,
                        only_filepath=filepath,
                        only_title=title,
                    )
                except Exception as e:
                    log_stream.emit_error(
                        f"Single-video redownload failed: {e}")
            start_managed_task(
                self,
                owner="redownload",
                label=f"Redownload {title or vid}",
                task_id=task_id,
                cancel=_vid_cancel,
                target=_run,
                name="single-video-redownload",
                thread_factory=threading.Thread,
            )
            return {"ok": True, "title": title, "resolution": res}
        except Exception as e:
            return {"ok": False, "error": str(e)}
