"""
IndexMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They prefer AppServices when present for config and
log dependencies, with legacy private Api attributes kept as fallback
state.
"""
from __future__ import annotations

import os
import threading
import time
import uuid

from backend import archive_scan
from backend import index as index_backend
from backend.services.job_supervisor import WorkAdmissionClosed
from backend.services.managed_work import (
    admitted_operation,
    lease_busy_result,
    start_managed_task,
    try_global_archive_lease,
)
from backend.ytarchiver_config import load_config

from ._shared import _log

_fts_state_init_lock = threading.Lock()


class IndexMixin:
    def _index_services(self):
        return getattr(self, "services", None)

    def _index_config(self):
        services = self._index_services()
        if services is not None:
            return services.fresh_config()
        cfg = getattr(self, "_config", None)
        if cfg is not None:
            return cfg
        return load_config()

    def _index_log_stream(self):
        services = self._index_services()
        stream = (getattr(services, "log_stream", None)
                  if services is not None else None)
        return stream if stream is not None else self._log_stream


    def get_index_summary(self):
        """Return Index tab data: cards + per-channel breakdown."""
        if self._config is None:
            # never return None to JS — the caller at
            # app.js:2813 does `.then((idx) => ... idx.get(...))`
            # which blows up on null. Empty dict keeps the Index
            # tab render-safe in demo / pre-config mode.
            return {
                "cards": [], "per_channel": [],
                "total_videos": 0, "total_size_bytes": 0,
            }
        return archive_scan.index_summary()


    def get_index_db_stats(self):
        """Slow index-DB-side stats (segments, hours, .db file size).
        Split from get_index_summary so it doesn't block the boot
        sequence — on a large archive the
        COUNT + JOIN aggregate runs for many seconds. Settings panel
        calls this async after the basics render."""
        try:
            return archive_scan.index_db_stats()
        except Exception as e:
            return {"segments": 0, "hours": 0, "index_db_bytes": 0,
                    "index_db_size_label": "\u2014",
                    "error": str(e)}


    def index_summary(self, report_errors=False):
        """Segments / videos / channels / bookmarks counts from the index DB."""
        return index_backend.summary(report_errors=bool(report_errors))


    def index_remove_archive_root(self, folder):
        """Forget one Additional archive folder without deleting its files."""
        raw_root = str(folder or "").strip()
        if not raw_root:
            return {"ok": False, "error": "Archive folder is required."}
        root = os.path.abspath(os.path.normpath(raw_root))
        cfg = self._index_config()
        primary_raw = str(cfg.get("output_dir") or "").strip()
        primary = (os.path.abspath(os.path.normpath(primary_raw))
                   if primary_raw else "")
        overlaps_primary = False
        if primary:
            try:
                root_key = os.path.normcase(root)
                primary_key = os.path.normcase(primary)
                common = os.path.commonpath([root_key, primary_key])
                overlaps_primary = common in {root_key, primary_key}
            except (OSError, ValueError):
                overlaps_primary = False
        if overlaps_primary:
            return {"ok": False,
                    "error": "This folder overlaps the primary archive."}
        task_id = f"remove-index-root-{uuid.uuid4().hex}"
        cancel = threading.Event()
        try:
            with admitted_operation(
                self,
                owner="index-maintenance",
                label="Remove additional archive folder from Search",
                task_id=task_id,
                cancel=cancel,
            ):
                admission = try_global_archive_lease(
                    owner="index-maintenance",
                    label="Remove additional archive folder from Search",
                    task_id=task_id,
                    cancel=cancel,
                )
                if not admission.ok or admission.lease is None:
                    return lease_busy_result(admission)
                try:
                    return index_backend.delete_catalog_under_root(root)
                finally:
                    admission.lease.release()
        except WorkAdmissionClosed as exc:
            return {"ok": False, "error": str(exc)}


    def index_count_transcripts(self, folder=None):
        """Count transcript + hidden JSONL files under `folder` (default:
        config.output_dir). Used by the "Delete All Transcriptions" 2-step
        confirm on the Index tab. Mirrors YTArchiver.py:31946 _count_files.
        """
        try:
            if not folder:
                cfg = self._index_config()
                folder = (cfg.get("output_dir") or "").strip()
            if not folder or not os.path.isdir(folder):
                return {"ok": False, "error": "Folder not found"}
            txt_count = jsonl_count = 0
            total_bytes = 0
            for dp, _dns, fns in os.walk(folder):
                for fn in fns:
                    fl = fn.lower()
                    fp = os.path.join(dp, fn)
                    if (fl.endswith(("transcript.txt", "transcription.txt"))
                            and not fn.startswith(".")):
                        txt_count += 1
                        try: total_bytes += os.path.getsize(fp)
                        except OSError: pass
                    elif (fl.endswith(".jsonl") and fn.startswith(".")
                            and not fl.endswith("metadata.jsonl")):
                        # metadata.jsonl exclusion: aggregated metadata
                        # sidecars (".{ch} Metadata.jsonl" etc.) are
                        # hidden dot-jsonls too — they are NOT
                        # transcripts and must never be counted (or
                        # deleted) by this feature.
                        jsonl_count += 1
                        try: total_bytes += os.path.getsize(fp)
                        except OSError: pass
            return {"ok": True,
                    "folder": folder,
                    "txt_count": txt_count,
                    "jsonl_count": jsonl_count,
                    "total": txt_count + jsonl_count,
                    "total_bytes": total_bytes}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def index_delete_all_transcripts(self, folder=None, confirm_token=""):
        """PERMANENTLY delete all transcript + hidden JSONL files under
        `folder`. Requires `confirm_token == "YES-DELETE-ALL"` so the JS
        side has to explicitly pass it after the 2-step dialog.

        Mirrors YTArchiver.py:31985 _delete_worker. Runs on a background
        thread; emits per-100-files progress to the log.
        """
        if confirm_token != "YES-DELETE-ALL":
            return {"ok": False, "error": "Missing confirm token"}
        if not folder:
            cfg = self._index_config()
            folder = (cfg.get("output_dir") or "").strip()
        if not folder or not os.path.isdir(folder):
            return {"ok": False, "error": "Folder not found"}
        # Containment: this is the most destructive bridge method (recursive
        # delete of transcripts + their index entries). The `folder` arg crosses
        # the JS trust boundary, so refuse anything outside the archive roots
        # this app manages — incl. tp_archive_roots (audit r2).
        from backend.utils import is_within_managed_roots
        if not is_within_managed_roots(folder):
            return {"ok": False,
                    "error": "Refusing to delete transcripts outside the archive."}
        cancel = threading.Event()
        task_id = f"delete-transcripts-{uuid.uuid4().hex}"
        # Re-entry guard — double-click on the button or rapid retries
        # used to launch parallel sweeps over the same tree, racing on
        # os.remove + on the DELETE FROM segments.
        if not hasattr(self, "_delete_transcripts_lock"):
            self._delete_transcripts_lock = threading.Lock()
            self._delete_transcripts_running = False
        with self._delete_transcripts_lock:
            if self._delete_transcripts_running:
                return {"ok": False,
                        "error": "Delete-all-transcripts is already running"}
            self._delete_transcripts_running = True
        def _run():
            log_stream = self._index_log_stream()
            log_stream.emit_text(
                f"\u26A0 Deleting all transcripts under {folder}\u2026",
                "red")
            log_stream.flush()
            deleted = 0
            errors = 0
            for dp, _dns, fns in os.walk(folder):
                if cancel.is_set():
                    break
                for fn in fns:
                    if cancel.is_set():
                        break
                    fl = fn.lower()
                    fp = os.path.join(dp, fn)
                    hit = False
                    if fl.endswith(("transcript.txt", "transcription.txt")) \
                            and not fn.startswith("."):
                        hit = True
                    elif (fl.endswith(".jsonl") and fn.startswith(".")
                            and not fl.endswith("metadata.jsonl")):
                        # NEVER touch metadata sidecars: the aggregated
                        # ".{ch} Metadata.jsonl" files are hidden
                        # dot-jsonls too, and the blanket pattern used
                        # to delete the ENTIRE metadata archive
                        # (descriptions/comments/counts — unrecoverable
                        # for removed videos) along with transcripts.
                        hit = True
                    if not hit:
                        continue
                    try:
                        from backend.services.file_ops import safe_remove_file
                        result = safe_remove_file(
                            fp,
                            require_config_writable=False,
                            reason="index_delete_all_transcripts",
                            unhide_first=True,
                        )
                        if not result.get("ok"):
                            raise OSError(result.get("error") or "delete failed")
                        deleted += 1
                        if deleted % 100 == 0:
                            log_stream.emit_dim(f" deleted {deleted}\u2026")
                            log_stream.flush()
                    except Exception:
                        errors += 1
            # Reconcile only actually missing transcript sources in this
            # folder, including a partially cancelled/failed deletion pass.
            index_cleared = False
            try:
                result = index_backend.clear_missing_transcripts_under_root(folder)
                index_cleared = bool(result.get("ok"))
                if not index_cleared:
                    errors += 1
                    log_stream.emit_error(result.get("error") or "Index cleanup failed")
            except Exception as e:
                errors += 1
                _log.debug("swallowed: %s", e)
            log_stream.emit_text(
                f"\u2014 Deleted {deleted} transcript file(s), {errors} errors. "
                + ("Search entries updated for this folder." if index_cleared
                   else "Search cleanup needs a retry."),
                "simpleline_red")
            log_stream.flush()
            try:
                message = (
                    f"Transcript deletion finished: {deleted} file(s) deleted."
                )
                if errors:
                    message += f" {errors} file(s) could not be deleted."
                event_bus = self.services.event_bus
                event_bus.show_toast(
                    message, "warn" if errors else "ok", ttl_ms=8000)
                event_bus.evaluate(
                    "if (window._refreshIndexStats) "
                    "window._refreshIndexStats();"
                    "if (window._refreshMetadataTab) "
                    "window._refreshMetadataTab({force:true});"
                )
            except Exception as exc:
                _log.debug(
                    "delete-all-transcripts completion push failed: %s", exc)
        def _run_wrapped():
            try:
                _run()
            finally:
                lease.release()
                with self._delete_transcripts_lock:
                    self._delete_transcripts_running = False
        try:
            with admitted_operation(
                self,
                owner="index-maintenance",
                label="Delete all transcripts",
                task_id=task_id,
                cancel=cancel,
            ):
                admission = try_global_archive_lease(
                    owner="index-maintenance",
                    label="Delete all transcripts",
                    task_id=task_id,
                    cancel=cancel,
                )
                if not admission.ok or admission.lease is None:
                    with self._delete_transcripts_lock:
                        self._delete_transcripts_running = False
                    return lease_busy_result(admission)
                lease = admission.lease
                try:
                    start_managed_task(
                        self,
                        owner="index-maintenance",
                        label="Delete all transcripts",
                        task_id=task_id,
                        cancel=cancel,
                        target=_run_wrapped,
                        name="delete-all-transcripts",
                        thread_factory=threading.Thread,
                    )
                except Exception:
                    lease.release()
                    raise
        except WorkAdmissionClosed as exc:
            with self._delete_transcripts_lock:
                self._delete_transcripts_running = False
            return {"ok": False, "started": False, "error": str(exc)}
        return {"ok": True, "started": True}


    def index_unindexed_count(self):
        """Count transcripts on disk that haven't been ingested into FTS yet.

        Walks every configured archive root looking for
        `.{ch_name} ... Transcript.jsonl` files whose path isn't in the
        indexed_files table. Returns the count so the Search/Graph views can
        show an amber warning banner.
        """
        def _sync_busy() -> bool:
            try:
                if self.sync_is_running():
                    return True
            except Exception:
                pass
            try:
                from backend.sync.active_state import is_sync_work_active
                return bool(is_sync_work_active())
            except Exception:
                return False

        def _deferred_result():
            cached = getattr(self, "_index_unindexed_count_cache", None)
            result = dict(cached) if isinstance(cached, dict) else {
                "ok": True, "unindexed": 0, "on_disk": 0, "indexed": 0,
            }
            result["deferred"] = True
            return result

        try:
            if _sync_busy():
                return _deferred_result()
            cfg = self._index_config()
            output_dir = (cfg.get("output_dir") or "").strip()
            configured_extras = cfg.get("tp_archive_roots") or []
            if not isinstance(configured_extras, (list, tuple)):
                configured_extras = []

            def _path_key(path):
                return os.path.normcase(os.path.abspath(os.path.normpath(path)))

            def _under(path_key, root_key):
                try:
                    return os.path.commonpath([path_key, root_key]) == root_key
                except (OSError, ValueError):
                    return False

            # Normalize and collapse overlapping roots before walking.  This
            # also handles a legacy config where an additional root is an
            # ancestor or descendant of the primary archive.
            candidate_roots = []
            seen_root_keys = set()
            for raw_root in [output_dir, *configured_extras]:
                value = str(raw_root or "").strip()
                if not value:
                    continue
                root = os.path.abspath(os.path.normpath(value))
                root_key = _path_key(root)
                if root_key in seen_root_keys or not os.path.isdir(root):
                    continue
                seen_root_keys.add(root_key)
                candidate_roots.append((root, root_key))
            roots = []
            for root, root_key in sorted(
                    candidate_roots, key=lambda item: len(item[1])):
                if any(_under(root_key, known_key)
                       for _known_root, known_key in roots):
                    continue
                roots.append((root, root_key))
            if not roots:
                result = {
                    "ok": True, "unindexed": 0,
                    "on_disk": 0, "indexed": 0,
                }
                self._index_unindexed_count_cache = dict(result)
                return result

            # Collect every aggregated JSONL on disk. Trash and interrupted
            # restore staging areas are intentionally not part of the live
            # searchable archive.
            on_disk = set()
            ignored_dirs = {
                ".ytarchiver trash",
                ".ytarchiver-restore-recovery",
            }
            for root, _root_key in roots:
                for dp, dns, fns in os.walk(root):
                    if _sync_busy():
                        return _deferred_result()
                    dns[:] = [
                        name for name in dns
                        if name.casefold() not in ignored_dirs
                    ]
                    for fn in fns:
                        if _sync_busy():
                            return _deferred_result()
                        if (fn.startswith(".")
                                and fn.endswith("Transcript.jsonl")):
                            on_disk.add(_path_key(os.path.join(dp, fn)))
            # Pull the indexed set from the DB. Use the reader connection
            # so this big SELECT doesn't queue behind sweep / ingest_jsonl
            # writers holding `_db_lock` during startup.
            indexed = set()
            try:
                rconn = index_backend._reader_open()
                if rconn is not None:
                    with index_backend._reader_lock:
                        for (path,) in rconn.execute("SELECT path FROM indexed_files").fetchall():
                            if path:
                                indexed.add(_path_key(path))
            except Exception as e:
                _log.debug("swallowed: %s", e)
            unindexed = len(on_disk - indexed)
            result = {"ok": True, "unindexed": unindexed,
                      "on_disk": len(on_disk), "indexed": len(indexed)}
            self._index_unindexed_count_cache = dict(result)
            return result
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def _ensure_fts_rebuild_state(self):
        """Lazily create per-API rebuild state for older/test Api objects."""
        if (hasattr(self, "_fts_rebuild_lock")
                and hasattr(self, "_fts_rebuild_state")):
            return
        with _fts_state_init_lock:
            if not hasattr(self, "_fts_rebuild_lock"):
                self._fts_rebuild_lock = threading.Lock()
            if not hasattr(self, "_fts_rebuild_state"):
                self._fts_rebuild_state = {
                    "running": bool(getattr(
                        self, "_fts_rebuild_running", False)),
                    "started_at": None,
                    "completed_at": None,
                    "ok": None,
                    "rows_indexed": None,
                    "error": None,
                }
            self._fts_rebuild_running = bool(
                self._fts_rebuild_state["running"])


    def index_rebuild_fts_state(self):
        """Return the current/last FTS rebuild outcome for UI polling."""
        self._ensure_fts_rebuild_state()
        with self._fts_rebuild_lock:
            return dict(self._fts_rebuild_state)


    def index_rebuild_fts(self):
        """Drop + rebuild the FTS5 virtual table from scratch. Runs on a
        background thread and emits progress to the log. Returns immediately.
        """
        # Re-entry guard \u2014 double-click would launch two concurrent
        # DROP+REBUILD passes that race on the same FTS table, leaving
        # the index in a partial/garbled state until the user noticed
        # and clicked Rebuild a third time.
        self._ensure_fts_rebuild_state()
        cancel = threading.Event()
        task_id = f"fts-rebuild-{uuid.uuid4().hex}"
        with self._fts_rebuild_lock:
            if self._fts_rebuild_running:
                return {"ok": False,
                        "error": "Search index rebuild is already running"}
            self._fts_rebuild_running = True
            self._fts_rebuild_state = {
                "running": True,
                "started_at": time.time(),
                "completed_at": None,
                "ok": None,
                "rows_indexed": None,
                "error": None,
            }
        def _run():
            log_stream = self._index_log_stream()
            outcome = {
                "ok": False,
                "rows_indexed": None,
                "error": "Search index rebuild ended without a result",
            }
            try:
                log_stream.emit_text(
                    "Rebuilding transcript search index…", "simpleline_blue")
                log_stream.flush()
                res = (
                    {"ok": False, "error": "cancelled"}
                    if cancel.is_set()
                    else index_backend.rebuild_fts_index()
                )
                if res.get("ok"):
                    outcome = {
                        "ok": True,
                        "rows_indexed": int(res.get("rows_indexed", 0) or 0),
                        "error": None,
                    }
                    log_stream.emit_text(
                        f"— Search index rebuild complete: {res.get('rows_indexed', 0):,} entries indexed.",
                        "simpleline_green")
                else:
                    outcome["error"] = str(
                        res.get("error") or "unknown error")
                    log_stream.emit_error(
                        f"Search index rebuild failed: {res.get('error', 'unknown')}")
            except Exception as e:
                outcome["error"] = str(e)
                log_stream.emit_error(f"Search index rebuild failed: {e}")
            finally:
                lease.release()
                with self._fts_rebuild_lock:
                    self._fts_rebuild_running = False
                    self._fts_rebuild_state = {
                        "running": False,
                        "started_at": self._fts_rebuild_state["started_at"],
                        "completed_at": time.time(),
                        **outcome,
                    }
                try:
                    log_stream.flush()
                except Exception as e:
                    _log.debug("FTS rebuild log flush failed: %s", e)
        try:
            with admitted_operation(
                self,
                owner="index-maintenance",
                label="Rebuild search index",
                task_id=task_id,
                cancel=cancel,
            ):
                admission = try_global_archive_lease(
                    owner="index-maintenance",
                    label="Rebuild search index",
                    task_id=task_id,
                    cancel=cancel,
                )
                if not admission.ok or admission.lease is None:
                    with self._fts_rebuild_lock:
                        self._fts_rebuild_running = False
                        self._fts_rebuild_state.update({
                            "running": False,
                            "completed_at": time.time(),
                            "ok": False,
                            "error": admission.explanation,
                        })
                    return lease_busy_result(admission)
                lease = admission.lease
                try:
                    start_managed_task(
                        self,
                        owner="index-maintenance",
                        label="Rebuild search index",
                        task_id=task_id,
                        cancel=cancel,
                        target=_run,
                        name="fts-rebuild",
                        thread_factory=threading.Thread,
                    )
                except Exception:
                    lease.release()
                    raise
        except WorkAdmissionClosed as e:
            with self._fts_rebuild_lock:
                self._fts_rebuild_running = False
                self._fts_rebuild_state.update({
                    "running": False,
                    "completed_at": time.time(),
                    "ok": False,
                    "error": str(e),
                })
            return {"ok": False, "started": False, "error": str(e)}
        except Exception as e:
            with self._fts_rebuild_lock:
                self._fts_rebuild_running = False
                self._fts_rebuild_state.update({
                    "running": False,
                    "completed_at": time.time(),
                    "ok": False,
                    "error": str(e),
                })
            return {"ok": False,
                    "error": f"Could not start search index rebuild: {e}"}
        return {"ok": True, "started": True}
