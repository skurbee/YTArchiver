"""
IndexMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class IndexMixin:

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
                cfg = self._config or load_config()
                folder = (cfg.get("output_dir") or "").strip()
            if not folder or not os.path.isdir(folder):
                return {"ok": False, "error": "Folder not found"}
            txt_count = jsonl_count = 0
            total_bytes = 0
            for dp, _dns, fns in os.walk(folder):
                for fn in fns:
                    fl = fn.lower()
                    fp = os.path.join(dp, fn)
                    if ((fl.endswith("transcript.txt") or
                         fl.endswith("transcription.txt"))
                            and not fn.startswith(".")):
                        txt_count += 1
                        try: total_bytes += os.path.getsize(fp)
                        except OSError: pass
                    elif fl.endswith(".jsonl") and fn.startswith("."):
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
            cfg = self._config or load_config()
            folder = (cfg.get("output_dir") or "").strip()
        if not folder or not os.path.isdir(folder):
            return {"ok": False, "error": "Folder not found"}
        def _run():
            self._log_stream.emit_text(
                f"\u26A0 Deleting all transcripts under {folder}\u2026",
                "red")
            self._log_stream.flush()
            deleted = 0
            errors = 0
            for dp, _dns, fns in os.walk(folder):
                for fn in fns:
                    fl = fn.lower()
                    fp = os.path.join(dp, fn)
                    hit = False
                    if (fl.endswith("transcript.txt") or fl.endswith("transcription.txt")) \
                            and not fn.startswith("."):
                        hit = True
                    elif fl.endswith(".jsonl") and fn.startswith("."):
                        hit = True
                    if not hit:
                        continue
                    try:
                        # Un-hide so Python can remove it on Windows
                        from backend.utils import unhide_file_win
                        unhide_file_win(fp)
                        os.remove(fp)
                        deleted += 1
                        if deleted % 100 == 0:
                            self._log_stream.emit_dim(f" deleted {deleted}\u2026")
                            self._log_stream.flush()
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
            self._log_stream.emit_text(
                f"\u2014 Deleted {deleted} transcript file(s), {errors} errors. "
                "FTS index cleared.",
                "simpleline_red")
            self._log_stream.flush()
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}


    def index_unindexed_count(self):
        """Count transcripts on disk that haven't been ingested into FTS yet.

        Walks the output_dir looking for `.{ch_name} ... Transcript.jsonl`
        files whose path isn't in the indexed_files table. Returns the
        count so the Search/Graph views can show an amber warning banner
        (YTArchiver.py:24756 _update_index_warning).
        """
        try:
            cfg = self._config or load_config()
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
        def _run():
            try:
                self._log_stream.emit_text(
                    "Rebuilding FTS search index from scratch\u2026", "simpleline_blue")
                self._log_stream.flush()
                res = index_backend.rebuild_fts_index()
                if res.get("ok"):
                    self._log_stream.emit_text(
                        f"\u2014 FTS rebuild complete: {res.get('rows_indexed', 0):,} rows indexed.",
                        "simpleline_green")
                else:
                    self._log_stream.emit_error(
                        f"FTS rebuild failed: {res.get('error', 'unknown')}")
            except Exception as e:
                self._log_stream.emit_error(f"FTS rebuild crashed: {e}")
            finally:
                self._log_stream.flush()
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}
