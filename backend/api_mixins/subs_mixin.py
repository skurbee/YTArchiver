"""
SubsMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

import os
import threading
import uuid
from contextlib import nullcontext

from backend import archive_scan, youtube_traffic
from backend import subs as subs_backend
from backend import sync as sync_backend
from backend.process_runner import run_ytdlp
from backend.services.job_supervisor import WorkAdmissionClosed
from backend.services.managed_work import start_managed_task
from backend.ytarchiver_config import (
    channels_for_subs_ui,
    config_is_writable,
    load_config,
    update_config,
)

from ._shared import _log

_SUBS_PROBE_TIMEOUT_SEC = 15


def _direct_child_realpath(base: str, target: str) -> bool:
    """True when target resolves as a direct child of base."""
    try:
        rb = os.path.normcase(os.path.realpath(base)).rstrip("/\\")
        rt = os.path.normcase(os.path.realpath(target)).rstrip("/\\")
        parent = os.path.dirname(rt)
        return bool(rb and rt and parent == rb)
    except Exception:
        return False


def _normalize_probe_channel_url(url: str) -> tuple[str, str]:
    """Return (normalized_url, error) for yt-dlp channel probes."""
    raw = (url or "").strip()
    if not raw:
        return "", "Empty URL"
    try:
        from backend.subs import normalize_channel_url
        normalized = normalize_channel_url(raw).strip()
        if not normalized.startswith(("http://", "https://")):
            return "", "Enter a YouTube channel URL or @handle"
        from urllib.parse import urlparse
        parsed = urlparse(normalized)
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        if host not in ("youtube.com", "youtu.be"):
            return "", "Enter a YouTube channel URL or @handle"
        path = (parsed.path or "").lower()
        if "/watch" in path or "/playlist" in path:
            return "", "Enter a channel URL, not a video or playlist URL"
        return normalized, ""
    except Exception as e:
        return "", str(e)


class SubsMixin:

    def _subs_transcribe_manager(self):
        services = getattr(self, "services", None)
        manager = getattr(services, "transcribe", None) \
            if services is not None else None
        return manager if manager is not None else getattr(
            self, "_transcribe", None)

    def get_subs_channels(self):
        """Return (rows, total_label) for the Subs table. Real data if avail.

        Enriches each row with n_vids / size_gb / size from the disk cache
        (ytarchiver_disk_cache.json) so counts match what YTArchiver shows.
        """
        if self._config is not None and self._config.get("channels"):
            # Enrich a copy so we don't mutate the in-memory config
            import copy as _copy
            cfg_copy = _copy.deepcopy(self._config)
            archive_scan.enrich_channels_with_stats(cfg_copy.get("channels", []))
            return channels_for_subs_ui(cfg_copy)
        return [], "0 channels · 0 videos · 0 GB"


    # ─── Subs CRUD (writes go to real %APPDATA%/YTArchiver/ytarchiver_config.json) ───

    def subs_is_writable(self):
        """Whether YTArchiver can write to the config file right now."""
        return config_is_writable()


    def subs_check_duplicate(self, url, folder, exclude_identity=None):
        """Return {dup_url: existing_name|None, dup_folder: existing_name|None}
        so the Add or Edit dialog can warn before actually trying to commit.

        `exclude_identity` (audit U-5): when running this check during an
        EDIT (not Add), pass the identity of the channel being edited so
        we don't flag the channel as a duplicate of itself. Identity dict
        with `name` / `folder` / `url` keys (any subset works — we exclude
        on the first match).
        """
        try:
            cfg = load_config()
            channels = cfg.get("channels", []) or []
            url_norm = (url or "").strip().lower().rstrip("/")
            folder_text = str(folder or "").strip()
            folder_norm = (sync_backend.channel_folder_name({
                "name": folder_text,
                "folder": folder_text,
                "folder_override": folder_text,
            }).casefold() if folder_text else "")
            # Build exclusion criteria from the identity dict.
            ex_url = ""
            ex_name = ""
            ex_folder = ""
            if isinstance(exclude_identity, dict):
                ex_url = (exclude_identity.get("url") or "").strip().lower().rstrip("/")
                ex_name = (exclude_identity.get("name") or "").strip().lower()
                ex_folder = (exclude_identity.get("folder") or "").strip().lower()
            dup_url = None
            dup_folder = None
            for ch in channels:
                u = (ch.get("url") or "").strip().lower().rstrip("/")
                n = (ch.get("name") or "").strip().lower()
                f = (ch.get("folder") or "").strip().lower()
                physical_folder = sync_backend.channel_folder_name(
                    ch).casefold()
                # Skip the channel being edited (identified by URL,
                # name, OR folder — any match counts).
                if (ex_url and u == ex_url) \
                        or (ex_name and n == ex_name) \
                        or (ex_folder and f == ex_folder):
                    continue
                if url_norm and u == url_norm:
                    dup_url = ch.get("name") or ch.get("folder") or ch.get("url")
                if folder_norm and physical_folder == folder_norm:
                    dup_folder = ch.get("name") or ch.get("folder")
            return {"ok": True, "dup_url": dup_url, "dup_folder": dup_folder}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def subs_preview_folder_name(self, url):
        """Probe yt-dlp for a channel URL's metadata so the user can see
        what folder name they'd get BEFORE committing. Mirrors
        YTArchiver.py:17162 do_preview_folder → _probe.

        Runs on a background thread; returns {ok, channel, folder} when
        done via the persisted `_pending_preview` slot, polled via
        `subs_preview_folder_poll`.
        """
        url, url_error = _normalize_probe_channel_url(url)
        if url_error:
            return {"ok": False, "error": url_error}
        yt = sync_backend.find_yt_dlp()
        if not yt:
            return {"ok": False, "error": "yt-dlp not found"}
        # uuid4 for the token. Old `id(url) + time.time()*ms` collided
        # when two previews fired within the same millisecond AND
        # Python recycled the id for a short-lived string (audit:
        # subs_mixin.py:97). uuid4 is collision-free in practice.
        import uuid as _uuid
        token = _uuid.uuid4().hex
        # Lock-protected pending-preview dict. js_api and worker
        # threads both mutate it (set→pending, set→result, pop on
        # poll), so a bare dict could drop entries on concurrent set+
        # pop (audit: subs_mixin.py:98).
        if not hasattr(self, "_pending_previews"):
            self._pending_previews = {}
        if not hasattr(self, "_pending_previews_lock"):
            self._pending_previews_lock = threading.Lock()
        # Sweep entries older than 10 minutes on every new submit so
        # the dict can't grow unbounded when users abandon previews
        # (modal dismissed, navigated away, race with another preview)
        # without ever polling. Same TTL pattern applies to
        # _pending_res_scans / _drift_scan_results / _drift_apply_results
        # (audit: subs_mixin H10).
        import time as _t_mod
        _now_ts = _t_mod.time()
        with self._pending_previews_lock:
            _stale = [k for k, v in self._pending_previews.items()
                      if isinstance(v, dict)
                      and (_now_ts - (v.get("_ts") or _now_ts)) > 600]
            for k in _stale:
                self._pending_previews.pop(k, None)
            self._pending_previews[token] = {
                "ok": False, "pending": True, "_ts": _now_ts}
        cancel = threading.Event()
        def _run():
            try:
                if cancel.is_set():
                    return
                cmd = [
                    yt, "--flat-playlist", "--print", "channel",
                    "--print", "uploader",
                    *sync_backend._find_cookie_source(),
                    "--playlist-end", "1", url,
                ]
                permission = youtube_traffic.acquire(
                    "channel_preview")
                if not permission.get("ok"):
                    raise RuntimeError(
                        permission.get("error")
                        or "YouTube traffic governor cancelled")
                r = run_ytdlp(cmd, capture_output=True, text=True,
                            timeout=_SUBS_PROBE_TIMEOUT_SEC,
                            startupinfo=sync_backend._startupinfo,
                            creationflags=(0x08000000 if os.name == "nt" else 0))
                if cancel.is_set():
                    return
                out = (r.stdout or "").strip().splitlines()
                name = (out[0] if out else "").strip() or (out[1] if len(out) > 1 else "").strip()
                if not name:
                    with self._pending_previews_lock:
                        self._pending_previews[token] = {
                            "ok": False, "error": "yt-dlp returned nothing",
                            "_ts": _t_mod.time()}
                    return
                folder = sync_backend.sanitize_folder(name)
                with self._pending_previews_lock:
                    self._pending_previews[token] = {
                        "ok": True, "channel": name, "folder": folder,
                        "_ts": _t_mod.time()}
            except Exception as e:
                with self._pending_previews_lock:
                    self._pending_previews[token] = {
                        "ok": False, "error": str(e), "_ts": _t_mod.time()}
        try:
            start_managed_task(
                self,
                owner="channel-preview",
                label="Preview a subscription folder name",
                task_id=f"channel-preview-{token}",
                cancel=cancel,
                target=_run,
                name="channel-folder-preview",
                thread_factory=threading.Thread,
            )
        except WorkAdmissionClosed as exc:
            with self._pending_previews_lock:
                self._pending_previews.pop(token, None)
            return {"ok": False, "started": False, "error": str(exc)}
        return {"ok": True, "token": token}


    def subs_preview_folder_poll(self, token):
        """Poll a pending folder-preview result by token. Returns
        {ok, pending} while running, or the final {ok, channel, folder}
        once `_run` sets it.
        """
        lock = getattr(self, "_pending_previews_lock", None)
        pend = getattr(self, "_pending_previews", {})
        if lock is not None:
            with lock:
                res = pend.get(token)
                if res is None:
                    return {"ok": False, "error": "unknown token"}
                if res.get("pending"):
                    return {"ok": True, "pending": True}
                # One-shot: pop the result inside the lock so a second
                # poll racing with this one can't double-deliver.
                try: del pend[token]
                except KeyError: pass
                return res
        # Defensive fallback if lock somehow isn't initialized yet.
        res = pend.get(token)
        if res is None:
            return {"ok": False, "error": "unknown token"}
        if res.get("pending"):
            return {"ok": True, "pending": True}
        try: del pend[token]
        except KeyError: pass
        return res


    def subs_add_channel(self, payload):
        """Add a new channel. Returns {ok, channel?, error?}.

        Also kicks off a one-time channel-art fetch in the background so the
        Browse grid shows the avatar/banner immediately — matches OLD
        YTArchiver behavior where adding a channel triggers
        `_fetch_channel_art`.
        """
        try:
            ch = subs_backend.add_channel(payload or {})
            self._reload_config()
            # Fire-and-forget channel-art fetch — but only when the
            # channel record actually committed to disk. If the config
            # write was gated (`_write_blocked`), skip the art fetch
            # so we don't leave .ChannelArt/ files for a channel
            # whose subs entry will revert on next reload (audit:
            # subs_mixin.py:144).
            try:
                name = ch.get("name") or ch.get("folder", "")
                if name and not ch.get("_write_blocked"):
                    self.chan_fetch_art(name, False)
            except Exception as e:
                _log.debug("swallowed: %s", e)
            if ch.get("_write_blocked"):
                return {
                    "ok": False,
                    "channel": ch,
                    "write_blocked": True,
                    "error": (
                        "The channel could not be saved. Check that the app "
                        "data folder is writable, then try again."
                    ),
                }
            return {"ok": True, "channel": ch, "write_blocked": False}
        except subs_backend.SubsError as e:
            return {"ok": False, "error": str(e)}
        except Exception as e:
            return {"ok": False, "error": f"Internal error: {e}"}


    def subs_update_channel(self, identity, payload):
        """Update an existing channel matched by identity (url/name/folder)."""
        try:
            old_ch = subs_backend.get_channel(identity or {}) or {}
            manager = self._subs_transcribe_manager()
            update_kwargs = {}
            if manager is not None and hasattr(
                    manager, "reconcile_pending_channel_path"):
                update_kwargs["pending_path_reconciler"] = (
                    lambda old_path, new_path, old_name, new_name:
                        manager.reconcile_pending_channel_path(
                            old_path, new_path,
                            old_channel=old_name, new_channel=new_name)
                )
            # Keep the Processing worker from starting a remapped job between
            # the queue-journal update and the actual folder rename. The
            # manager lock is reentrant, so the reconciler can save its journal
            # inside this one transaction boundary.
            mutation_boundary = (
                manager.pending_path_mutation_boundary()
                if manager is not None and hasattr(
                    manager, "pending_path_mutation_boundary")
                else nullcontext()
            )
            with mutation_boundary:
                ch = subs_backend.update_channel(
                    identity or {}, payload or {}, **update_kwargs)
            self._reload_config()
            if ch.get("_write_blocked"):
                return {
                    "ok": False,
                    "channel": ch,
                    "write_blocked": True,
                    "recovery_required": bool(ch.get("_recovery_required")),
                    "error": str(
                        ch.get("_error")
                        or ch.get("_rollback_error")
                        or "The channel changes could not be saved."
                    ),
                }
            # surface folder-rename failures so the user
            # knows the on-disk folder didn't move. Config was kept at
            # the old name (subs.py rollback).
            resp = {"ok": True, "channel": ch,
                    "write_blocked": ch.get("_write_blocked", False)}
            if (
                "folder_org" in (payload or {})
                and not ch.get("_write_blocked", False)
                and (
                    bool(old_ch.get("split_years")) != bool(ch.get("split_years"))
                    or bool(old_ch.get("split_months")) != bool(ch.get("split_months"))
                )
            ):
                resp["folder_org_changed"] = True
            if ch.get("_folder_rename_error"):
                resp["folder_rename_error"] = ch["_folder_rename_error"]
            queue_result = ch.get("_processing_queue_result") or {}
            if queue_result:
                resp["processing_queue_changed"] = int(
                    queue_result.get("changed") or 0)
            return resp
        except subs_backend.SubsError as e:
            return {"ok": False, "error": str(e)}
        except Exception as e:
            return {"ok": False, "error": f"Internal error: {e}"}


    def subs_remove_channel(self, identity, delete_files=False):
        """Remove a channel by identity. Pushes the removed dict onto the
        `_removed_channels_stack` so future subs_undo_remove calls can
        unwind in reverse-remove order (newest undo first).

        Previously stored a single slot — removing two channels in
        succession, then undoing once, left the second one unrecoverable.

        If `delete_files=True`, the channel's on-disk folder (videos +
        transcripts + metadata + thumbnails) is moved to the app trash.
        Undo only restores the subscription, not the files.
        """
        _prepared = {"cleanup_tokens": []}
        _preparation_pending = False
        _catalog_preflight_warning = None
        _processing_reconcile_result = None
        _processing_reconcile_committed = False
        _not_committed = {
            "subscription_removed": False,
            "files_removed": False,
            "catalog_cleanup_ok": None,
            "catalog_warning": None,
        }
        try:
            # Snapshot before removal for undo
            ch_snap = subs_backend.get_channel(identity or {})
            # refuse delete_files=True while sync is actively
            # processing this channel — moving/removing the folder while
            # yt-dlp is writing into it
            # active writes can crash sync, partially-delete files, or
            # leave orphan temp dirs. Sub is not removed either since
            # that side effect would also surprise a live sync.
            if delete_files:
                # Hold the sync-mutation lock for BOTH the check and
                # the subs_backend.remove_channel() call below (which
                # is what actually moves the folder). Without the lock,
                # a sync worker could start touching this channel
                # between the active-sync check and the folder move —
                # racing yt-dlp's writes against filesystem mutation
                # walk. The lock is reentrant so sync_start_all
                # taking it elsewhere doesn't self-deadlock.
                if not hasattr(self, "_sync_mutation_lock"):
                    self._sync_mutation_lock = threading.RLock()
                with self._sync_mutation_lock:
                    # The edit dialog may have been open while another task
                    # changed this subscription. Resolve the current channel
                    # only after mutation admission so folder names and the
                    # active-sync check cannot use a stale pre-lock snapshot.
                    ch_snap = subs_backend.get_channel(identity or {})
                    if not ch_snap:
                        return {
                            "ok": False,
                            "error": "Channel not found",
                            **_not_committed,
                        }
                    _target_url = (ch_snap.get("url") or "").strip()
                    try:
                        # The OLD guard read self._current_sync_channel, which is
                        # never assigned anywhere — so it always saw "" and never
                        # fired, letting folder mutation race a live sync's writes. Compare
                        # the delete target against the REAL active-sync state
                        # (QueueState.current_sync, set via set_current_sync()).
                        _cur = getattr(self._queues, "current_sync", None) or {}
                        _cur_url = (_cur.get("url") or "").strip()
                        _cur_name = (_cur.get("name") or "").strip()
                        _t_name = (ch_snap.get("name") or "").strip()
                        if ((_target_url and _cur_url and _cur_url == _target_url)
                                or (_t_name and _cur_name and _cur_name == _t_name)):
                            return {
                                "ok": False,
                                "error": ("Sync is currently running on this "
                                          "channel. Cancel or pause the sync "
                                          "first, then retry the delete."),
                                **_not_committed,
                            }
                    except Exception as e:
                        _log.warning("active-sync check before channel remove failed; delete may proceed while sync is running: %s", e)
                    # A channel folder move also moves every per-video JSONL.
                    # Preserve any transcript whose same-ID physical survivor
                    # lives in another channel before the destructive move.
                    from backend import index as _idx
                    _delete_names = {
                        (ch_snap.get(_key) or "").strip()
                        for _key in ("name", "folder", "folder_override")
                        if (ch_snap.get(_key) or "").strip()
                    }
                    _delete_folder_paths = []
                    _cfg = load_config() or {}
                    _archive_root = str(_cfg.get("output_dir") or "").strip()
                    _folder_name = sync_backend.channel_folder_name(ch_snap)
                    if _archive_root and _folder_name:
                        _delete_folder_paths.append(
                            os.path.join(_archive_root, _folder_name))
                    _prepared = _idx.prepare_channel_copy_deletion(
                        sorted(_delete_names),
                        folder_paths=_delete_folder_paths)
                    if not _prepared.get("ok"):
                        # The preflight protects data before a LIVE folder is
                        # moved.  Older builds could move the folder to Trash
                        # but leave the subscription behind.  In that state a
                        # stale catalog row (especially one with an unmanaged
                        # duplicate survivor) can make preflight fail even
                        # though there is no folder left to move.  Do not let
                        # that catalog-only problem strand the subscription.
                        # Remove config only, explicitly disabling file
                        # mutation so a folder that appears in this narrow
                        # window can never be moved without preflight.
                        if any(os.path.isdir(path)
                               for path in _delete_folder_paths):
                            _log.warning(
                                "channel removal transcript preflight failed: %s",
                                _prepared.get("error") or "unknown error")
                            return {
                                "ok": False,
                                "error": (
                                    "YTArchiver did not remove this channel "
                                    "because it could not safely preserve a "
                                    "transcript for another downloaded copy. "
                                    "Nothing was changed. Try again, or rebuild "
                                    "the library index if it keeps happening."),
                                "error_code": "transcript_preservation_failed",
                                **_not_committed,
                            }
                        _log.debug(
                            "channel catalog preflight failed after its folder "
                            "was already missing: %s",
                            _prepared.get("error") or "unknown error")
                        _catalog_preflight_warning = (
                            "The downloaded folder was already missing, so "
                            "its stale catalog entries were left untouched "
                            "to protect transcript data.")
                        _prepared = {"cleanup_tokens": [], "row_ids": []}
                        result = subs_backend.remove_channel(
                            identity or {}, delete_files=False)
                    else:
                        _preparation_pending = True
                        # Take the folder-move branch INSIDE the lock so an
                        # incoming sync start can't slip past our check.
                        _processing_root = (_delete_folder_paths[0]
                                            if _delete_folder_paths else "")
                        _processing_manager = self._subs_transcribe_manager()
                        if (_processing_root and _processing_manager is not None
                                and hasattr(
                                    _processing_manager,
                                    "pending_channel_path_mutation")):
                            with _processing_manager.pending_channel_path_mutation(
                                    _processing_root,
                                    old_channel=(ch_snap.get("name")
                                                 or ch_snap.get("folder") or "")
                            ) as queue_control:
                                _processing_reconcile_result = \
                                    queue_control["result"]
                                if not _processing_reconcile_result.get("ok"):
                                    raise subs_backend.SubsError(
                                        _processing_reconcile_result.get("error")
                                        or (
                                            "Queued Processing tasks could not "
                                            "be prepared for channel removal."
                                        )
                                    )
                                result = subs_backend.remove_channel(
                                    identity or {},
                                    delete_files=bool(delete_files))
                                # If rollback could not return an already-moved
                                # folder from Trash, do not restore queued jobs
                                # to the now-missing live path. Startup recovery
                                # owns the folder decision; the user can queue
                                # work again after that recovery finishes.
                                folder_left_archive = bool(
                                    result.get("deleted_folder")
                                    and result.get("_recovery_required"))
                                queue_control["commit"] = bool(
                                    (result.get("ok")
                                     and not result.get("delete_error"))
                                    or folder_left_archive)
                                _processing_reconcile_committed = bool(
                                    queue_control["commit"])
                        else:
                            result = subs_backend.remove_channel(
                                identity or {},
                                delete_files=bool(delete_files))
            else:
                result = subs_backend.remove_channel(
                    identity or {}, delete_files=bool(delete_files))
            subscription_removed = bool(result.get("ok"))
            files_removed = bool(result.get("deleted_folder"))
            catalog_cleanup_ok = None
            catalog_warning = None
            ok = subscription_removed
            folder_delete_failed = bool(
                delete_files and result.get("delete_error"))
            if _preparation_pending and (not ok or folder_delete_failed):
                _idx.rollback_copy_deletion_preparation(_prepared)
                _preparation_pending = False
            if folder_delete_failed:
                # subs.remove_channel may durably remove the subscription even
                # when quarantine failed.  Never interpret that config success
                # as authorization to purge catalog rows for files still live.
                ok = False
            if _catalog_preflight_warning and subscription_removed:
                # Config removal is committed, so callers must refresh their
                # channel lists.  Report catalog cleanup as the only partial
                # failure, without exposing the internal sidecar safety error
                # that caused the safe fallback.
                ok = False
                catalog_cleanup_ok = False
                catalog_warning = _catalog_preflight_warning
                result["delete_error"] = catalog_warning
            # When the files were moved out of the archive, also purge the channel's
            # rows from the index DB (videos + transcript segments). Browse /
            # Search / Videos read the index, not the disk — without this the
            # removed channel's cards linger and 404 ("File not found — index
            # entry may be stale") when clicked. Match every identifier the
            # videos.channel column might hold (name / folder / override).
            if ok and delete_files and ch_snap:
                try:
                    from backend import index as _idx
                    _purged = _idx.delete_media_copy_rows(
                        _prepared.get("row_ids") or [], prepared=_prepared)
                    if not _purged.get("ok"):
                        raise RuntimeError(
                            _purged.get("error") or "Index purge failed")
                    _idx.finalize_copy_deletion_preparation(_prepared)
                    _preparation_pending = False
                    catalog_cleanup_ok = True
                except Exception as e:
                    if _preparation_pending:
                        _idx.finalize_copy_deletion_preparation(_prepared)
                        _preparation_pending = False
                    ok = False
                    catalog_cleanup_ok = False
                    catalog_warning = (
                        "Folder moved to trash but index cleanup failed: "
                        f"{e}")
                    # Keep the established field for older frontends while
                    # exposing the warning under an explicit catalog field.
                    result["delete_error"] = catalog_warning
                    _log.debug("index purge after channel delete failed: %s", e)
            undo_id = ""
            if ok and ch_snap and not delete_files:
                if not hasattr(self, "_removed_channels_stack"):
                    self._removed_channels_stack = []
                undo_id = uuid.uuid4().hex
                undo_snapshot = dict(ch_snap)
                undo_snapshot["_undo_id"] = undo_id
                self._removed_channels_stack.append(undo_snapshot)
                # Bound the stack so we don't grow unbounded across
                # a long session of repeated removes.
                if len(self._removed_channels_stack) > 50:
                    self._removed_channels_stack = (
                        self._removed_channels_stack[-50:])
            processing_queue_warning = ""
            processing_queue_removed = 0
            if (_processing_reconcile_committed
                    and _processing_reconcile_result is not None
                    and _processing_reconcile_result.get("ok")):
                processing_queue_removed = int(
                    _processing_reconcile_result.get("removed") or 0)
            elif subscription_removed and files_removed and ch_snap:
                old_root = str(
                    result.get("folder_path")
                    or (_delete_folder_paths[0]
                        if _delete_folder_paths else "")
                )
                manager = self._subs_transcribe_manager()
                if (old_root and manager is not None and hasattr(
                        manager, "reconcile_pending_channel_path")):
                    queue_result = manager.reconcile_pending_channel_path(
                        old_root,
                        old_channel=(ch_snap.get("name")
                                     or ch_snap.get("folder") or ""),
                    )
                    processing_queue_removed = int(
                        queue_result.get("removed") or 0)
                    if not queue_result.get("ok"):
                        processing_queue_warning = (
                            queue_result.get("error")
                            or "Queued Processing tasks could not be removed."
                        )

            # drop any queued sync tasks for this
            # channel so a removed channel doesn't keep getting
            # synced (which recreates the folder and confuses the
            # log). Best-effort — removal is authoritative even if
            # queue cleanup fails.
            if subscription_removed and ch_snap:
                try:
                    _ch_url = (ch_snap.get("url") or "").strip()
                    if _ch_url:
                        self._queues.sync_remove_all_for_target(_ch_url)
                        self._on_queue_changed()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            self._reload_config()
            return {
                "ok": ok,
                # A later catalog warning does not mean the config write was
                # blocked: the subscription has already been removed.
                "write_blocked": not subscription_removed,
                "subscription_removed": subscription_removed,
                "files_removed": files_removed,
                "catalog_cleanup_ok": catalog_cleanup_ok,
                "catalog_warning": catalog_warning,
                "can_undo": bool(
                    ch_snap and subscription_removed and not delete_files),
                "undo_id": undo_id,
                "processing_queue_removed": processing_queue_removed,
                "processing_queue_warning": processing_queue_warning,
                "deleted_folder": bool(result.get("deleted_folder")),
                "folder_path": result.get("folder_path"),
                "trashed_folder_path": result.get("trashed_folder_path"),
                "error": result.get("error"),
                "delete_error": result.get("delete_error"),
            }
        except subs_backend.SubsError as e:
            if _preparation_pending:
                _idx.rollback_copy_deletion_preparation(_prepared)
            return {"ok": False, "error": str(e), **_not_committed}
        except Exception as e:
            if _preparation_pending:
                try:
                    _idx.rollback_copy_deletion_preparation(_prepared)
                except Exception:
                    pass
            return {
                "ok": False,
                "error": f"Internal error: {e}",
                **_not_committed,
            }


    def subs_undo_remove(self, undo_id=None):
        """Restore one removed channel, normally addressed by its toast ID."""
        stack = getattr(self, "_removed_channels_stack", None)
        wanted_undo_id = str(undo_id or "").strip()
        stack_index = None
        # Distinguish `stack is None` (legacy — attr was never set,
        # consult single-slot fallback) from `stack == []` (set but
        # empty — there's nothing to undo, return immediately). The
        # old `if not stack:` collapsed both into the legacy branch
        # which broke LIFO ordering when a later exception path
        # appended back to the stack mid-undo (audit: subs_mixin H12).
        if stack is None:
            legacy = getattr(self, "_last_removed_channel", None)
            if legacy:
                self._last_removed_channel = None
                ch = legacy
            else:
                return {"ok": False, "error": "Nothing to undo"}
        elif not stack:
            return {"ok": False, "error": "Nothing to undo"}
        else:
            if wanted_undo_id:
                stack_index = next(
                    (index for index in range(len(stack) - 1, -1, -1)
                     if str(stack[index].get("_undo_id") or "")
                     == wanted_undo_id),
                    None,
                )
                if stack_index is None:
                    return {
                        "ok": False,
                        "error": "That removal is no longer available to undo.",
                    }
            else:
                stack_index = len(stack) - 1
            ch = stack.pop(stack_index)
        try:
            payload = dict(ch)
            payload.pop("_undo_id", None)
            # Undo restores the exact disk-shaped snapshot.  The public Add
            # path accepts UI-shaped minutes and fills today's defaults, so
            # routing a saved record through it multiplies duration limits and
            # can change settings that were never part of the removal.
            restored = subs_backend.restore_channel_snapshot(payload)
            if not restored.get("ok"):
                raise subs_backend.SubsError(
                    restored.get("error") or "The channel could not be restored.")
            result = restored.get("channel") or payload
            self._reload_config()
            # pop the disk-cache entry for the restored channel
            # so the next Subs-table render triggers a fresh rescan
            # instead of showing "—" or stale counts. invalidate_channel
            # spawns a background rescan that repopulates num_vids/
            # size_bytes.
            try:
                from backend import archive_scan as _as
                _url = (result.get("url") or ch.get("url") or "").strip()
                if _url:
                    _as.invalidate_channel(_url)
            except Exception as e:
                _log.debug("swallowed: %s", e)
            return {
                "ok": True,
                "channel": result,
                "undo_id": wanted_undo_id or str(ch.get("_undo_id") or ""),
                "more_undo_available": bool(
                    getattr(self, "_removed_channels_stack", None)),
            }
        except subs_backend.SubsError as e:
            # Restore so the user can retry. Cover BOTH the stack and
            # the legacy single-slot branches — previously the legacy
            # branch had cleared `_last_removed_channel` before the
            # restore attempt, and on exception the channel was lost
            # forever with no fallback.
            if stack is not None:
                stack.insert(stack_index if stack_index is not None
                             else len(stack), ch)
            else:
                self._last_removed_channel = ch
            return {"ok": False, "error": str(e)}
        except Exception as e:
            if stack is not None:
                stack.insert(stack_index if stack_index is not None
                             else len(stack), ch)
            else:
                self._last_removed_channel = ch
            return {"ok": False, "error": str(e)}


    def subs_reset_sync_state(self, identity):
        """audit SM-1: clear a channel's bootstrap / sync-state flags
        so the next sync does a fresh full-walk. Useful when the
        user wipes the folder manually or wants to re-bootstrap
        after a filter change.

        Clears: initialized, sync_complete, batch_resume_index,
                init_batch_after, init_complete, last_sync.
        Preserves: everything else (channels/url/filters/etc).
        """
        try:
            ch_snap = subs_backend.get_channel(identity or {})
            if not ch_snap:
                return {"ok": False, "error": "Channel not found"}
            _url = (ch_snap.get("url") or "").strip().rstrip("/")
            _flags = ("initialized", "sync_complete", "init_complete",
                      "batch_resume_index", "init_batch_after", "last_sync")

            def _reset(live_cfg):
                cleared = 0
                for channel in live_cfg.get("channels", []):
                    channel_url = (
                        channel.get("url") or "").strip().rstrip("/")
                    if channel_url != _url:
                        continue
                    for key in _flags:
                        if key in channel:
                            channel.pop(key, None)
                            cleared += 1
                    break
                return cleared

            _cleared, _snapshot = update_config(_reset)
            if _snapshot is None:
                return {
                    "ok": False,
                    "error": "Could not save the reset sync state",
                    "write_blocked": True,
                }
            self._reload_config()
            return {"ok": True, "cleared_flags": _cleared,
                    "channel": ch_snap.get("name") or ch_snap.get("folder") or ""}
        except OSError as e:
            return {"ok": False, "error": str(e), "write_blocked": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def subs_get_channel(self, identity):
        """Fetch a single channel's full record (for populating the edit panel).

        Min/max durations are converted to minutes here to match the UI unit.
        On save, `_payload_to_channel` converts back to seconds for storage.
        """
        try:
            ch = subs_backend.get_channel_for_ui(identity or {})
            return {"ok": True, "channel": ch} if ch else {"ok": False, "error": "Not found"}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def subs_test_url(self, url):
        """Probe a channel URL via yt-dlp, return the canonical name + video count."""
        normalized, url_error = _normalize_probe_channel_url(url)
        if url_error:
            return {"ok": False, "error": url_error}
        yt = sync_backend.find_yt_dlp()
        if not yt:
            return {"ok": False, "error": "yt-dlp not found"}
        try:
            cookies = sync_backend._find_cookie_source()
            # Get channel name (from first video)
            permission = youtube_traffic.acquire("channel_url_test")
            if not permission.get("ok"):
                return {
                    "ok": False,
                    "error": permission.get("error")
                    or "YouTube traffic governor cancelled",
                }
            r1 = run_ytdlp([yt, "--flat-playlist", "--playlist-end", "1",
                         "--print", "channel", "--no-warnings", "--quiet",
                         *cookies, normalized],
                        capture_output=True, text=True,
                        timeout=_SUBS_PROBE_TIMEOUT_SEC,
                        startupinfo=sync_backend._startupinfo)
            name = (r1.stdout or "").strip().split("\n")[0] or ""
            # Get total count (best-effort)
            permission = youtube_traffic.acquire("channel_url_test")
            if not permission.get("ok"):
                return {
                    "ok": False,
                    "error": permission.get("error")
                    or "YouTube traffic governor cancelled",
                }
            r2 = run_ytdlp([yt, "--flat-playlist", "--print", "%(playlist_count)s",
                         "--playlist-end", "1", "--no-warnings", "--quiet",
                         *cookies, normalized],
                        capture_output=True, text=True,
                        timeout=_SUBS_PROBE_TIMEOUT_SEC,
                        startupinfo=sync_backend._startupinfo)
            count_raw = (r2.stdout or "").strip().split("\n")[0]
            total = int(count_raw) if count_raw.isdigit() else None
            return {"ok": bool(name), "name": name, "total": total, "url": normalized}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def subs_get_defaults(self):
        """Return the user's default channel settings for the Restore-defaults button.

        min_duration / max_duration are returned in MINUTES to match the
        unit the edit-panel input accepts + displays.
        """
        cfg = self._config or load_config()
        # cfg["min_duration"] is SECONDS (180 = 3 min) per YTArchiver's schema
        raw_min_secs = int(cfg.get("min_duration", 180) or 0)
        # read user-configured defaults from config if set,
        # fall back to conservative defaults. Previously these were
        # hardcoded constants so the "Restore defaults" button in the
        # edit panel always clobbered user preference with the constant
        # values — if user set Settings>General auto_transcribe=true
        # and then clicked Restore on a channel, the channel flipped
        # to false regardless of their stated preference.
        return {
            "resolution": cfg.get("default_resolution", "720"),
            "min_duration": max(0, raw_min_secs // 60),
            "max_duration": 0,
            "auto_metadata": bool(cfg.get("default_auto_metadata", True)),
            "auto_transcribe": bool(cfg.get("default_auto_transcribe", True)),
            "compress_enabled": bool(cfg.get("default_compress_enabled", False)),
            "mode": (cfg.get("default_mode") or "new"),
            "folder_org": (cfg.get("default_folder_org") or "years"),
        }


    # ─── Bulk channel operations (feature F7) ──────────────────────────

    def subs_bulk_update(self, names, changes):
        """Apply a small set of whitelisted changes to N channels at once.

        `names` — list of channel folder / display names.
        `changes` — dict with keys from the whitelist below:
          resolution, auto_transcribe, auto_metadata,
          compress_enabled, compress_level, compress_output_res.
        Returns {ok, updated, failed}. Guarded to the whitelist so the
        UI can't accidentally wipe urls / folder names / anything
        load-bearing.
        """
        if not isinstance(names, list) or not names:
            return {"ok": False, "error": "No channels selected"}
        if not isinstance(changes, dict) or not changes:
            return {"ok": False, "error": "No changes specified"}
        _ALLOWED = {"resolution", "auto_transcribe", "auto_metadata",
                    "compress_enabled", "compress_level",
                    "compress_output_res"}
        clean = {k: v for k, v in changes.items() if k in _ALLOWED}
        if not clean:
            return {"ok": False, "error": "No allowed fields in changes"}
        updated = 0
        failed = []
        write_blocked = []
        for n in names:
            try:
                ch = subs_backend.get_channel({"name": n}) \
                     or subs_backend.get_channel({"folder": n})
                if not ch:
                    failed.append({"name": n, "reason": "not found"})
                    continue
                # Pass ONLY the whitelisted changes. `clean` carries no
                # 'url' key, so update_channel takes its SPARSE merge
                # path (subs.py) and every other field survives
                # untouched. The old dict(ch)+update(clean) payload had
                # a truthy url, which routed the full DISK-shape record
                # through the UI-shape _payload_to_channel rebuild:
                # min/max durations re-multiplied x60, mode forced to
                # 'new', folder org forced to years, and _apply_defaults
                # reset last_sync / pending_tx_ids / folder_override on
                # every bulk-updated channel.
                _res = subs_backend.update_channel(
                    {"url": ch.get("url", ""), "name": ch.get("name", "")},
                    dict(clean))
                # Detect save failures the backend signals via
                # `_write_blocked: True` (my Fix 10 made update_channel
                # roll back its in-memory mutation and return that
                # marker rather than raise). Without surfacing it,
                # bulk_update silently reported "updated:N" while
                # half of those updates never landed on disk.
                if isinstance(_res, dict) and _res.get("_write_blocked"):
                    write_blocked.append(n)
                    failed.append({
                        "name": n,
                        "reason": "Changes could not be saved.",
                    })
                else:
                    updated += 1
            except Exception as e:
                failed.append({"name": n, "reason": str(e)})
        self._reload_config()
        return {"ok": bool(updated or not failed),
                "error": (failed[0]["reason"] if failed and not updated else ""),
                "updated": updated, "failed": failed,
                "write_blocked": write_blocked}


    def subs_bulk_delete(self, names, delete_files=False):
        """Delete N channels at once. `delete_files=True` also removes
        the on-disk folders. Returns {ok, started}.

        The per-channel folder moves can take time on
        TB-scale channels; the work runs on a background thread so the
        bridge call returns immediately. The result toast + Subs table
        refresh are pushed via evaluate_js when the worker finishes.
        """
        if not isinstance(names, list) or not names:
            return {"ok": False, "error": "No channels selected"}
        requested_names = list(dict.fromkeys(
            str(name or "").strip() for name in names
            if str(name or "").strip()))
        if not requested_names:
            return {"ok": False, "error": "No channels selected"}
        if not hasattr(self, "_removed_channels_stack"):
            self._removed_channels_stack = []
        task_id = f"subs-bulk-delete-{uuid.uuid4().hex}"
        cancel = threading.Event()

        def _bd_worker():
            deleted = 0
            trashed = 0
            failed = []
            warnings = []
            for n in requested_names:
                if cancel.is_set():
                    break
                try:
                    ch = subs_backend.get_channel({"name": n}) \
                         or subs_backend.get_channel({"folder": n})
                    if not ch:
                        failed.append({"name": n, "reason": "not found"})
                        continue
                    res = self.subs_remove_channel(
                        {"url": ch.get("url", "")},
                        delete_files=bool(delete_files))
                    if res.get("ok") or res.get("subscription_removed"):
                        deleted += 1
                        if res.get("files_removed") or res.get("deleted_folder"):
                            trashed += 1
                        warning = " ".join(filter(None, (
                            res.get("catalog_warning") or res.get("delete_error"),
                            res.get("processing_queue_warning"),
                        )))
                        if warning:
                            warnings.append({"name": n, "reason": warning})
                    else:
                        failed.append({"name": n,
                                       "reason": (res.get("error")
                                                  or res.get("delete_error")
                                                  or res.get("catalog_warning")
                                                  or "unknown error")})
                except Exception as e:
                    failed.append({"name": n, "reason": str(e)})
            self._log_stream.emit([["[Subs] ", "sync_bracket"],
                                    [f"Bulk delete: {deleted} removed"
                                     + (f" ({len(failed)} failed)" if failed else "")
                                     + ".\n", "simpleline"]])
            self._log_stream.flush()
            try:
                if self._window is not None:
                    _msg = f"Removed {deleted} channel(s)."
                    if failed:
                        _msg += f" {len(failed)} failed."
                    if warnings:
                        _msg += (f" {len(warnings)} completed with a "
                                 "cleanup warning.")
                    if cancel.is_set():
                        remaining = max(
                            0, len(requested_names) - deleted - len(failed))
                        if remaining:
                            _msg += f" {remaining} cancelled."
                    detail = (failed[0]["reason"] if failed
                              else warnings[0]["reason"] if warnings else "")
                    if detail:
                        _msg += f" First issue: {detail}"
                    _kind = "ok" if not failed and not warnings else "warn"
                    self.services.event_bus.show_toast_and_refresh_subs(
                        _msg, _kind)
            except Exception as e:
                _log.debug("swallowed: %s", e)
            # Keep Trash refresh independent from the result-toast path.  A
            # missing event bus must not leave Browse > Trash stale after the
            # folders were already moved successfully.
            if trashed:
                try:
                    if self._window is not None:
                        self._window.evaluate_js(
                            "if (window._onTrashChanged) "
                            "window._onTrashChanged();")
                except Exception as e:
                    _log.debug("Trash refresh after bulk remove failed: %s", e)

        try:
            start_managed_task(
                self,
                owner="subscription-maintenance",
                label="Remove selected subscriptions",
                task_id=task_id,
                cancel=cancel,
                target=_bd_worker,
                name="subs_bulk_delete",
                thread_factory=threading.Thread,
            )
        except WorkAdmissionClosed as exc:
            return {"ok": False, "started": False, "error": str(exc)}
        return {"ok": True, "started": True}



    def subs_bulk_queue_metadata(self, names, refresh=False):
        """Queue a metadata fetch (or refresh) for N channels at once.
        Thin wrapper around the per-channel enqueue path that
        `metadata_queue_all` uses.
        """
        if not isinstance(names, list) or not names:
            return {"ok": False, "error": "No channels selected"}
        queued = 0
        failed = []
        already_queued = 0
        for n in names:
            try:
                ch = subs_backend.get_channel({"name": n}) \
                     or subs_backend.get_channel({"folder": n})
                if not ch:
                    failed.append({"name": n, "reason": "not found"})
                    continue
                task = dict(ch)
                task["kind"] = "metadata"
                task["refresh"] = bool(refresh)
                if self._queues.sync_enqueue(task):
                    queued += 1
                else:
                    already_queued += 1
            except Exception as e:
                failed.append({"name": n, "reason": str(e)})
        self._on_queue_changed()
        # Auto-fire the worker — gated on paused state.
        started = self._maybe_autostart_sync() if queued > 0 else False
        return {"ok": bool(queued or already_queued or not failed),
                "error": (failed[0]["reason"]
                          if failed and not (queued or already_queued) else ""),
                "queued": queued, "already_queued": already_queued,
                "failed": failed,
                "started": started,
                "paused": bool(self._queues.sync_paused)}


    def subs_queue_pending(self):
        """Left-click of the Subs header "↺ Queue Pending" button.

        Walks every subscribed channel; for any with `transcription_pending > 0`
        (or that have new videos without `.jsonl` sidecars), queues a bulk
        transcribe. `chan_transcribe_pending` is real-state aware — it
        scans aggregate transcripts + DB, skips channels already fully
        transcribed, and resets stale counters so the badge self-heals.

        Matches YTArchiver.py:5808 _queue_pending_transcriptions.

        The walk + per-channel scan moved to a background thread so the
        bridge call returns immediately. Final tally + Subs refresh
        land via evaluate_js when the worker finishes.
        """
        cfg0 = load_config()
        base = (cfg0.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "No archive folder is configured."}
        from backend.sync import channel_folder_name as _cfn
        if not hasattr(self, "_subs_queue_walk_lock"):
            self._subs_queue_walk_lock = threading.Lock()
        with self._subs_queue_walk_lock:
            if getattr(self, "_subs_queue_walk_active", False):
                return {
                    "ok": False,
                    "started": False,
                    "error": "A channel queue check is already running.",
                }
            self._subs_queue_walk_active = True
        task_id = f"subs-queue-pending-{uuid.uuid4().hex}"
        cancel = threading.Event()

        def _qp_worker_body():
            cfg = load_config() or {}
            worker_base = (cfg.get("output_dir") or "").strip() or base
            channels = [dict(ch) for ch in (cfg.get("channels", []) or [])
                        if isinstance(ch, dict)]
            tx_added = 0
            mt_added = 0
            failed = []
            for ch in channels:
                if cancel.is_set():
                    break
                ch_name = ch.get("name") or ch.get("folder") or ""
                if not ch_name:
                    continue
                try:
                    _ = os.path.join(worker_base, _cfn(ch))
                    pending_ids = ch.get("pending_tx_ids") or []
                    if isinstance(pending_ids, list) and pending_ids:
                        r = self.chan_transcribe_pending(ch_name)
                        if not r or not r.get("ok"):
                            failed.append({
                                "name": ch_name,
                                "reason": ((r or {}).get("error")
                                           or "Transcription work could not be checked."),
                            })
                        elif r.get("queued", 0) > 0:
                            tx_added += 1
                    if int(ch.get("metadata_pending") or 0) > 0:
                        task = dict(ch)
                        task["kind"] = "metadata"
                        task["refresh"] = False
                        if self._queues.sync_enqueue(task):
                            mt_added += 1
                except Exception as e:
                    failed.append({"name": ch_name, "reason": str(e)})
            if mt_added or tx_added:
                try:
                    self._on_queue_changed()
                    cfg2 = load_config() or {}
                    if (cfg2.get("autorun_sync", False) and
                            not self.sync_is_running() and mt_added > 0):
                        self.sync_start_all(add_downloads_from_config=False)
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            parts = []
            if tx_added: parts.append(f"{tx_added} for transcription")
            if mt_added: parts.append(f"{mt_added} for metadata")
            if parts:
                self._log_stream.emit([["[Subs] ", "sync_bracket"],
                                        [f"\u21ba Queued {', '.join(parts)}.\n",
                                         "simpleline_green"]])
                _toast_msg = f"Queued {', '.join(parts)}."
                _toast_kind = "ok"
            else:
                self._log_stream.emit([["[Subs] ", "sync_bracket"],
                                        ["No channels with pending transcriptions or metadata.\n",
                                         "dim"]])
                _toast_msg = "No pending channels."
                _toast_kind = "warn"
            if failed:
                first = failed[0]
                _toast_msg += (
                    f" {len(failed)} channel(s) could not be checked. "
                    f"First issue: {first['name']}: {first['reason']}"
                )
                _toast_kind = "warn"
            if cancel.is_set():
                _toast_msg += " Check cancelled."
                _toast_kind = "warn"
            self._log_stream.flush()
            try:
                if self._window is not None:
                    self.services.event_bus.show_toast_and_refresh_subs(
                        _toast_msg, _toast_kind)
            except Exception as e:
                _log.debug("swallowed: %s", e)

        def _qp_worker():
            try:
                _qp_worker_body()
            except Exception as exc:
                _log.warning("Queue-pending channel check failed: %s", exc)
                try:
                    if self._window is not None:
                        self.services.event_bus.show_toast_and_refresh_subs(
                            "Queue check stopped unexpectedly. Try again.",
                            "error")
                except Exception as notify_error:
                    _log.debug("Queue-check failure notification failed: %s",
                               notify_error)
            finally:
                with self._subs_queue_walk_lock:
                    self._subs_queue_walk_active = False

        try:
            start_managed_task(
                self,
                owner="queue-maintenance",
                label="Queue pending subscription work",
                task_id=task_id,
                cancel=cancel,
                target=_qp_worker,
                name="subs_queue_pending",
                thread_factory=threading.Thread,
            )
        except WorkAdmissionClosed as exc:
            with self._subs_queue_walk_lock:
                self._subs_queue_walk_active = False
            return {"ok": False, "started": False, "error": str(exc)}
        except Exception as exc:
            with self._subs_queue_walk_lock:
                self._subs_queue_walk_active = False
            return {"ok": False, "started": False, "error": str(exc)}
        return {"ok": True, "started": True}



    def subs_queue_all(self, combined=None):
        """Right-click of the "↺ Queue Pending" button — queues ALL channels
        for transcribe. Matches YTArchiver.py:5844 _queue_all_transcriptions.

        Walk + per-channel scan moved to a background thread so the
        bridge call returns immediately. Final tally toast lands via
        evaluate_js when the worker finishes.
        """
        if combined is not None and not isinstance(combined, bool):
            return {"ok": False, "started": False,
                    "error": "Choose Combined or Follow organization."}
        if combined is None:
            from .channel_mixin import channel_transcript_layout
            cfg = load_config() or {}
            base = str(cfg.get("output_dir") or "").strip()
            choices = []
            for channel in cfg.get("channels") or []:
                if not isinstance(channel, dict) or not channel.get("split_years"):
                    continue
                folder = os.path.join(base, sync_backend.channel_folder_name(channel))
                if channel_transcript_layout(channel, folder) is None:
                    choices.append(channel.get("name") or channel.get("folder") or "")
            if choices:
                return {"ok": True, "started": False, "needs_choice": True,
                        "channels": choices, "org_label": "Year / month"}
        if not hasattr(self, "_subs_queue_walk_lock"):
            self._subs_queue_walk_lock = threading.Lock()
        with self._subs_queue_walk_lock:
            if getattr(self, "_subs_queue_walk_active", False):
                return {
                    "ok": False,
                    "started": False,
                    "error": "A channel queue check is already running.",
                }
            self._subs_queue_walk_active = True
        task_id = f"subs-queue-all-{uuid.uuid4().hex}"
        cancel = threading.Event()

        def _qa_worker_body():
            cfg = load_config() or {}
            channels = [dict(ch) for ch in (cfg.get("channels", []) or [])
                        if isinstance(ch, dict)]
            queued = 0
            failed = []
            for ch in channels:
                if cancel.is_set():
                    break
                name = ch.get("name") or ch.get("folder") or ""
                if not name:
                    continue
                try:
                    r = (self.chan_transcribe_all(name) if combined is None
                         else self.chan_transcribe_all(name, combined=combined))
                    if not r or not r.get("ok"):
                        failed.append({
                            "name": name,
                            "reason": ((r or {}).get("error")
                                       or "Transcription work could not be checked."),
                        })
                    elif r.get("needs_choice"):
                        failed.append({"name": name,
                                       "reason": "Choose a transcript output layout, then retry Queue All."})
                    elif r.get("queued", 0) > 0:
                        queued += 1
                except Exception as e:
                    failed.append({"name": name, "reason": str(e)})
            message = f"Queued {queued} channel(s)."
            kind = "ok" if queued else "warn"
            if failed:
                first = failed[0]
                message += (
                    f" {len(failed)} could not be checked. "
                    f"First issue: {first['name']}: {first['reason']}"
                )
                kind = "warn"
            if cancel.is_set():
                message += " Check cancelled."
                kind = "warn"
            self._log_stream.emit([["[Subs] ", "sync_bracket"],
                                    [f"\u21ba {message}\n",
                                     ("simpleline_green" if kind == "ok"
                                      else "dim")]])
            self._log_stream.flush()
            try:
                if self._window is not None:
                    self.services.event_bus.show_toast_and_refresh_subs(
                        message, kind)
            except Exception as e:
                _log.debug("swallowed: %s", e)

        def _qa_worker():
            try:
                _qa_worker_body()
            except Exception as exc:
                _log.warning("Queue-all channel check failed: %s", exc)
                try:
                    if self._window is not None:
                        self.services.event_bus.show_toast_and_refresh_subs(
                            "Queue-all check stopped unexpectedly. Try again.",
                            "error")
                except Exception as notify_error:
                    _log.debug("Queue-all failure notification failed: %s",
                               notify_error)
            finally:
                with self._subs_queue_walk_lock:
                    self._subs_queue_walk_active = False

        try:
            start_managed_task(
                self,
                owner="queue-maintenance",
                label="Queue all subscriptions for transcription",
                task_id=task_id,
                cancel=cancel,
                target=_qa_worker,
                name="subs_queue_all",
                thread_factory=threading.Thread,
            )
        except WorkAdmissionClosed as exc:
            with self._subs_queue_walk_lock:
                self._subs_queue_walk_active = False
            return {"ok": False, "started": False, "error": str(exc)}
        except Exception as exc:
            with self._subs_queue_walk_lock:
                self._subs_queue_walk_active = False
            return {"ok": False, "started": False, "error": str(exc)}
        return {"ok": True, "started": True}



    def subs_relocate_channel(self, identity, new_folder_name):
        """Update a channel's folder_override to point at a different on-disk
        folder (used when the original folder is gone but the user has it
        elsewhere). `new_folder_name` must be a subfolder of output_dir.

        Mirrors YTArchiver.py:33700 "locate" branch of the missing-folder
        dialog. Never moves files — just updates the config pointer.
        """
        if (not isinstance(identity, dict) or not identity
                or not isinstance(new_folder_name, str) or not new_folder_name):
            return {"ok": False, "error": "identity + new_folder_name required"}
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "No archive folder is configured."}
        # Validate the INPUT shape: new_folder_name must be a single bare
        # folder name (no separators, not absolute, not . / ..) so
        # os.path.join below can't be coerced into escaping output_dir. The
        # dirname check further down stays as the second layer (audit:
        # subs_relocate_channel containment).
        if (os.sep in new_folder_name or "/" in new_folder_name
                or os.path.isabs(new_folder_name)
                or new_folder_name in (".", "..")):
            return {"ok": False,
                    "error": "Choose a single folder inside your archive folder."}
        target = os.path.normpath(os.path.join(base, new_folder_name))
        if not os.path.isdir(target):
            return {"ok": False, "error": f"Folder not found: {target}"}
        # Guard: must resolve directly inside output_dir. normpath/dirname
        # alone accepts symlinks, junctions, case aliases, and 8.3 paths.
        if not _direct_child_realpath(base, target):
            return {"ok": False,
                    "error": "The channel folder must be directly inside your archive folder."}
        try:
            # Require a non-empty identity field and compare with truthiness
            # guards — otherwise a folder-only identity makes both sides
            # None == None → True and rewrites the FIRST channel's folder
            # (audit r2).
            _id_url = (identity.get("url") or "").strip()
            _id_name = (identity.get("name") or "").strip()
            if not _id_url and not _id_name:
                return {"ok": False, "error": "identity needs a url or name"}

            folder_override = os.path.basename(target)
            index = subs_backend._find_channel(cfg.get("channels") or [], identity)
            if index is None:
                return {"ok": False, "error": "Channel not found"}
            original = dict(cfg["channels"][index])
            old_folder = sync_backend.channel_folder_name(original)
            old_path = os.path.join(base, old_folder)
            updated = {**original, "folder_override": folder_override}
            admission = subs_backend._mutation_lease(
                original, updated_channel=updated, paths=(old_path, target),
                label=f"Locate archive for {original.get('name') or old_folder}")
            if not admission.ok or admission.lease is None:
                return {"ok": False, "busy": True, "error": admission.explanation}

            def _relocate(live_cfg):
                live_base = (live_cfg.get("output_dir") or "").strip()
                live_target = os.path.normpath(
                    os.path.join(live_base, folder_override))
                if (not live_base
                        or os.path.normcase(os.path.realpath(live_target))
                        != os.path.normcase(os.path.realpath(target))):
                    raise RuntimeError(
                        "Archive root changed while relocating; try again")
                if not os.path.isdir(live_target) or not _direct_child_realpath(live_base, live_target):
                    raise RuntimeError("The chosen archive folder changed. Choose it again.")
                channels = live_cfg.get("channels") or []
                live_index = subs_backend._find_channel(channels, identity)
                if live_index is None:
                    return False
                channel = channels[live_index]
                if sync_backend.channel_folder_name(channel) != old_folder:
                    raise RuntimeError("The channel folder changed. Reload and try again.")
                for position, other in enumerate(channels):
                    if position == live_index or not isinstance(other, dict):
                        continue
                    other_path = os.path.join(live_base, sync_backend.channel_folder_name(other))
                    if os.path.normcase(os.path.realpath(other_path)) == os.path.normcase(os.path.realpath(live_target)):
                        raise RuntimeError(
                            f"Another channel already uses this archive folder: {other.get('name') or other.get('folder') or 'channel'}.")
                channel["folder_override"] = folder_override
                return True

            with admission.lease:
                found, _snapshot = update_config(_relocate)
            if not found:
                return {"ok": False, "error": "Channel not found"}
            try:
                archive_scan.invalidate_channel(original.get("url") or "")
                from backend import index as index_backend
                index_backend.invalidate_channel_videos(original.get("name") or old_folder)
            except Exception as exc:
                _log.debug("Relocated channel cache invalidation failed: %s", exc)
            self._reload_config()
            return {"ok": True, "folder_override": folder_override}
        except OSError as e:
            return {"ok": False, "error": str(e), "write_blocked": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def subs_browse_for_channel_folder(self, channel_name):
        """Open native folder picker; returns the selected folder's basename
        (must be inside output_dir) or an error."""
        try:
            import webview as _wv
            if self._window is None:
                return {"ok": False, "error": "No window"}
            cfg = load_config()
            base = (cfg.get("output_dir") or "").strip()
            paths = self._window.create_file_dialog(
                _wv.FOLDER_DIALOG, directory=base,
            )
            if not paths:
                return {"ok": False, "cancelled": True}
            picked = paths if isinstance(paths, str) else paths[0]
            picked = os.path.normpath(picked)
            if not _direct_child_realpath(base, picked):
                return {"ok": False,
                        "error": f"Pick a subfolder of:\n {base}"}
            return {"ok": True,
                    "folder_name": os.path.basename(picked),
                    "full_path": picked}
        except Exception as e:
            return {"ok": False, "error": str(e)}
