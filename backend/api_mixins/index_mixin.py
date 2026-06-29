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

from ._shared import _log
from backend.ytarchiver_config import load_config
from backend import archive_scan
from backend import index as index_backend


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
        sequence — on a large archive (9M+ segments / 16GB DB) the
        COUNT + JOIN aggregate runs for many seconds. Settings panel
        calls this async after the basics render."""
        try:
            return archive_scan.index_db_stats()
        except Exception as e:
            return {"segments": 0, "hours": 0, "index_db_bytes": 0,
                    "index_db_size_label": "\u2014",
                    "error": str(e)}


    def index_summary(self):
        """Segments / videos / channels / bookmarks counts from the index DB."""
        return index_backend.summary()


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
        # delete of transcripts + a full FTS wipe). The `folder` arg crosses
        # the JS trust boundary, so refuse anything outside the archive roots
        # this app manages — incl. tp_archive_roots (audit r2).
        from backend.utils import is_within_managed_roots
        if not is_within_managed_roots(folder):
            return {"ok": False,
                    "error": "Refusing to delete transcripts outside the archive."}
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
                for fn in fns:
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
            # Also clear the FTS index — no point keeping ingested data that
            # points to files we just deleted.
            try:
                conn = index_backend._open()
                if conn is not None:
                    with index_backend._db_lock:
                        conn.execute("DELETE FROM segments")
                        conn.execute("DELETE FROM segments_fts")
                        conn.execute("DELETE FROM indexed_files")
                        conn.execute("UPDATE videos SET tx_status='pending'")
                        conn.commit()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            log_stream.emit_text(
                f"\u2014 Deleted {deleted} transcript file(s), {errors} errors. "
                "FTS index cleared.",
                "simpleline_red")
            log_stream.flush()
        def _run_wrapped():
            try:
                _run()
            finally:
                with self._delete_transcripts_lock:
                    self._delete_transcripts_running = False
        threading.Thread(target=_run_wrapped, daemon=True).start()
        return {"ok": True, "started": True}


    def index_unindexed_count(self):
        """Count transcripts on disk that haven't been ingested into FTS yet.

        Walks the output_dir looking for `.{ch_name} ... Transcript.jsonl`
        files whose path isn't in the indexed_files table. Returns the
        count so the Search/Graph views can show an amber warning banner
        (YTArchiver.py:24756 _update_index_warning).
        """
        try:
            cfg = self._index_config()
            output_dir = (cfg.get("output_dir") or "").strip()
            if not output_dir or not os.path.isdir(output_dir):
                return {"ok": True, "unindexed": 0}
            # Collect every aggregated JSONL on disk
            on_disk = set()
            for dp, _dns, fns in os.walk(output_dir):
                for fn in fns:
                    if fn.startswith(".") and fn.endswith("Transcript.jsonl"):
                        on_disk.add(os.path.normpath(os.path.join(dp, fn)))
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
                                indexed.add(os.path.normpath(path))
            except Exception as e:
                _log.debug("swallowed: %s", e)
            unindexed = len(on_disk - indexed)
            return {"ok": True, "unindexed": unindexed, "on_disk": len(on_disk),
                    "indexed": len(indexed)}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def index_rebuild_fts(self):
        """Drop + rebuild the FTS5 virtual table from scratch. Runs on a
        background thread and emits progress to the log. Returns immediately.
        """
        # Re-entry guard \u2014 double-click would launch two concurrent
        # DROP+REBUILD passes that race on the same FTS table, leaving
        # the index in a partial/garbled state until the user noticed
        # and clicked Rebuild a third time.
        if not hasattr(self, "_fts_rebuild_lock"):
            self._fts_rebuild_lock = threading.Lock()
            self._fts_rebuild_running = False
        with self._fts_rebuild_lock:
            if self._fts_rebuild_running:
                return {"ok": False, "error": "FTS rebuild already running"}
            self._fts_rebuild_running = True
        def _run():
            log_stream = self._index_log_stream()
            try:
                log_stream.emit_text(
                    "Rebuilding FTS search index from scratch\u2026", "simpleline_blue")
                log_stream.flush()
                res = index_backend.rebuild_fts_index()
                if res.get("ok"):
                    log_stream.emit_text(
                        f"\u2014 FTS rebuild complete: {res.get('rows_indexed', 0):,} rows indexed.",
                        "simpleline_green")
                else:
                    log_stream.emit_error(
                        f"FTS rebuild failed: {res.get('error', 'unknown')}")
            except Exception as e:
                log_stream.emit_error(f"FTS rebuild crashed: {e}")
            finally:
                log_stream.flush()
                with self._fts_rebuild_lock:
                    self._fts_rebuild_running = False
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}
