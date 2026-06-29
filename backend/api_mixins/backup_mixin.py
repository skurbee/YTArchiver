"""
BackupMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from ._shared import _api_err, webview, normalize_dialog_paths
from backend.ytarchiver_config import CONFIG_FILE, config_is_writable, load_config, save_config
from backend import subs as subs_backend


_BACKUP_MANIFEST_NAME = "ytarchiver_backup_manifest.json"


def _backup_file_entries():
    from backend.ytarchiver_config import (
        APP_DATA_DIR,
        CHANNEL_ID_CACHE,
        DISK_CACHE_FILE,
        QUEUE_FILE,
        SEEN_FILTER_TITLES,
    )
    return (
        (CONFIG_FILE.name, CONFIG_FILE),
        (QUEUE_FILE.name, QUEUE_FILE),
        (DISK_CACHE_FILE.name, DISK_CACHE_FILE),
        (SEEN_FILTER_TITLES.name, SEEN_FILTER_TITLES),
        (CHANNEL_ID_CACHE.name, CHANNEL_ID_CACHE),
        ("ytarchiver_livestream_defer.json",
         APP_DATA_DIR / "ytarchiver_livestream_defer.json"),
        ("ytarchiver_pending_transcribe.json",
         APP_DATA_DIR / "ytarchiver_pending_transcribe.json"),
    )


def _allowed_backup_top_names() -> set[str]:
    from backend.ytarchiver_config import TRANSCRIPTION_DB
    return {name for name, _path in _backup_file_entries()} | {
        _BACKUP_MANIFEST_NAME,
        TRANSCRIPTION_DB.name,
    }


_CHANNEL_IMPORT_MAX = 10000
_CHANNEL_IMPORT_STRING_LIMITS = {
    "name": 200,
    "folder": 200,
    "url": 500,
    "resolution": 32,
    "mode": 32,
    "from_date": 32,
    "date_after": 32,
    "compress_level": 64,
    "compress_output_res": 32,
    "output_dir": 500,
}
_CHANNEL_IMPORT_ALLOWED_KEYS = frozenset(_CHANNEL_IMPORT_STRING_LIMITS) | {
    "min_duration",
    "max_duration",
    "split_years",
    "split_months",
    "auto_transcribe",
    "auto_metadata",
    "compress_enabled",
    "compress_batch_size",
}
_CHANNEL_IMPORT_BOOL_KEYS = {
    "split_years",
    "split_months",
    "auto_transcribe",
    "auto_metadata",
    "compress_enabled",
}
_CHANNEL_IMPORT_INT_KEYS = {
    "min_duration",
    "max_duration",
    "compress_batch_size",
}


def _clean_import_channel(ch: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    raw_url = str(ch.get("url") or "").strip()
    ok, err = subs_backend.validate_channel_url(raw_url)
    if not ok:
        return None, err
    clean: dict[str, Any] = {
        "url": subs_backend.normalize_channel_url(raw_url),
    }
    for key in _CHANNEL_IMPORT_ALLOWED_KEYS:
        if key == "url" or key not in ch:
            continue
        val = ch.get(key)
        if key in _CHANNEL_IMPORT_STRING_LIMITS:
            clean[key] = str(val or "").strip()[
                :_CHANNEL_IMPORT_STRING_LIMITS[key]]
        elif key in _CHANNEL_IMPORT_BOOL_KEYS:
            clean[key] = bool(val)
        elif key in _CHANNEL_IMPORT_INT_KEYS:
            try:
                clean[key] = max(0, int(val))
            except (TypeError, ValueError):
                continue
    if not (clean.get("name") or clean.get("folder")):
        return None, "missing channel name/folder"
    if not clean.get("name"):
        clean["name"] = clean.get("folder", "")
    if not clean.get("folder"):
        clean["folder"] = clean.get("name", "")
    return clean, ""


class BackupMixin:

    # ─── Channel list export / import ──────────────────────────────────

    def channels_export(self):
        try:
            import json as _json

            import webview as _wv
            cfg = load_config()
            if self._window is None:
                return {"ok": False, "error": "No window"}
            paths = self._window.create_file_dialog(
                _wv.SAVE_DIALOG, save_filename="ytarchiver_channels.json",
                file_types=("JSON (*.json)",),
            )
            path = normalize_dialog_paths(paths)
            if not path:
                return {"ok": False, "cancelled": True}
            with open(path, "w", encoding="utf-8") as f:
                _json.dump({
                    "exported_from": "YTArchiver",
                    "channels": cfg.get("channels", []),
                }, f, indent=2)
            return {"ok": True, "path": path, "count": len(cfg.get("channels", []))}
        except Exception as e:
            return _api_err("BACKUP_WRITE_FAILED", str(e))


    def channels_import(self):
        try:
            import json as _json

            import webview as _wv
            if self._window is None:
                return {"ok": False, "error": "No window"}
            paths = self._window.create_file_dialog(
                _wv.OPEN_DIALOG, allow_multiple=False,
                file_types=("JSON (*.json)", "All files (*.*)"),
            )
            path = normalize_dialog_paths(paths)
            if not path:
                return {"ok": False, "cancelled": True}
            with open(path, "r", encoding="utf-8") as f:
                data = _json.load(f)
            imported = data.get("channels", []) if isinstance(data, dict) else data
            if not isinstance(imported, list):
                return {"ok": False, "error": "Not a channel list"}
            if len(imported) > _CHANNEL_IMPORT_MAX:
                return {"ok": False,
                        "error": (f"Channel import too large "
                                  f"({len(imported)} > {_CHANNEL_IMPORT_MAX})")}
            if not config_is_writable():
                return {"ok": False, "error": "Write-gate off"}
            cfg = load_config()
            existing_urls = {
                subs_backend.normalize_channel_url(c.get("url", ""))
                for c in cfg.get("channels", [])
            }
            added = 0
            # track WHY each entry was skipped so the UI can
            # tell the user (previously just reported a raw count with
            # no way to debug a partial import).
            skipped_reasons: List[Dict[str, str]] = []
            for ch in imported:
                if not isinstance(ch, dict):
                    skipped_reasons.append({
                        "name": "(unknown)",
                        "reason": "not a valid channel object",
                    })
                    continue
                if not ch.get("url"):
                    skipped_reasons.append({
                        "name": ch.get("name") or "(no name)",
                        "reason": "missing URL",
                    })
                    continue
                clean_ch, clean_err = _clean_import_channel(ch)
                if not clean_ch:
                    skipped_reasons.append({
                        "name": ch.get("name") or ch.get("url") or "(unknown)",
                        "reason": clean_err or "invalid channel",
                    })
                    continue
                if clean_ch["url"] in existing_urls:
                    skipped_reasons.append({
                        "name": clean_ch.get("name") or clean_ch["url"],
                        "reason": "already subscribed",
                    })
                    continue
                cfg.setdefault("channels", []).append(clean_ch)
                existing_urls.add(clean_ch["url"])
                added += 1
            cfg["channels"].sort(key=lambda c: (c.get("name") or "").lower())
            from backend.ytarchiver_config import save_config as _sc
            if not _sc(cfg):
                return {"ok": False, "error": "Save failed"}
            self._reload_config()
            return {"ok": True, "added": added,
                    "skipped": len(skipped_reasons),
                    "skipped_reasons": skipped_reasons}
        except Exception as e:
            return _api_err("BACKUP_READ_FAILED", str(e))


    def export_full_backup(self):
        """ZIP the user's config + queue state + cached ID list + seen-filters
        + disk cache + livestream journal into a user-picked file.

        also include the FTS transcript index DB when it's
        small enough to fit (< 2GB). Previously the DB was
        unconditionally skipped, which meant "full backup" restore
        returned a usable archive browser that then had EVERY
        transcript search return empty until the user kicked off a
        full re-transcribe. Now the authoritative search index rides
        along in the ZIP too — the backup is actually full.

        The 2GB cap is a pragmatic stop: ZIP deflate slows dramatically
        past that size and the ZIP64 format has its own constraints.
        For archives where the DB exceeds the cap, the UI surfaces a
        size warning so users can decide to export manually.
        """
        try:
            import zipfile as _zf

            import webview as _wv

            from backend.ytarchiver_config import (
                APP_DATA_DIR,
                TRANSCRIPTION_DB,
            )
            # opt-in include of the FTS DB if it fits. NOTE: the DB is
            # NOT appended to `_backup_file_entries()` — it gets a safe sqlite3
            # backup-API snapshot below instead of a raw file copy
            # (raw copy of a live WAL-mode DB misses every transaction
            # still in the -wal and can tear mid-read).
            _fts_skipped_reason = ""
            _fts_size = 0
            _include_fts = False
            try:
                if TRANSCRIPTION_DB.exists():
                    _fts_sz = TRANSCRIPTION_DB.stat().st_size
                    _fts_size = int(_fts_sz)
                    if _fts_sz < 2 * 1024 * 1024 * 1024:
                        _include_fts = True
                    else:
                        _fts_skipped_reason = (
                            f"FTS DB skipped — too large "
                            f"({_fts_sz / (1024**3):.1f} GB > 2 GB). "
                            f"Back up manually if needed.")
            except OSError:
                pass
            if self._window is None:
                return {"ok": False, "error": "No window"}
            import datetime as _dt
            ts = _dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
            paths = self._window.create_file_dialog(
                _wv.SAVE_DIALOG,
                save_filename=f"ytarchiver_backup_{ts}.zip",
                # Default to *.zip so Export matches the Restore open-dialog
                # filter (Restore filters to "Backup ZIP (*.zip)"). Without
                # this the save dialog defaulted to "All files" — a confusing
                # round-trip mismatch.
                file_types=("Backup ZIP (*.zip)", "All files (*.*)"),
            )
            out_path = normalize_dialog_paths(paths)
            if not out_path:
                return {"ok": False, "cancelled": True}
            n = 0
            # Write to a .tmp file then os.replace into the final
            # path. A crash / power loss / disk full mid-zip
            # previously partially overwrote the user's previous
            # backup ZIP at out_path, leaving them with no
            # recoverable backup. With the tmp+replace pattern, the
            # old file stays intact until the new zip closes cleanly.
            tmp_path = out_path + ".tmp"
            try:
                with _zf.ZipFile(tmp_path, "w", _zf.ZIP_DEFLATED) as zf:
                    for arcname, p in _backup_file_entries():
                        if p.exists():
                            zf.write(str(p), arcname=arcname)
                            n += 1
                    if _include_fts:
                        # Consistent point-in-time snapshot of the live
                        # WAL-mode DB via sqlite3's backup API. zipping
                        # the .db file raw silently dropped the -wal's
                        # committed transactions and could produce a
                        # torn (unreadably corrupt) copy if a
                        # checkpoint ran mid-read — discovered only at
                        # restore time.
                        import sqlite3 as _sq3
                        import tempfile as _tf
                        _fd, _snap = _tf.mkstemp(suffix=".db")
                        os.close(_fd)
                        try:
                            _src = _sq3.connect(
                                f"file:{TRANSCRIPTION_DB}?mode=ro",
                                uri=True, timeout=60)
                            try:
                                _dst = _sq3.connect(_snap)
                                try:
                                    _src.backup(_dst)
                                finally:
                                    _dst.close()
                            finally:
                                _src.close()
                            zf.write(_snap, arcname=TRANSCRIPTION_DB.name)
                            n += 1
                        finally:
                            try: os.remove(_snap)
                            except OSError: pass
                    zf.writestr(_BACKUP_MANIFEST_NAME, json.dumps({
                        "app": "YTArchiver",
                        "backup_type": "app-state",
                        "fts_db_included": bool(_include_fts),
                        "fts_db_size": _fts_size,
                        "fts_skipped_reason": _fts_skipped_reason,
                    }, indent=2, sort_keys=True))
                    n += 1
                    backup_dir = APP_DATA_DIR / "backups"
                    if backup_dir.is_dir():
                        snaps = sorted(backup_dir.glob("config_*.json"),
                                       key=lambda pp: pp.stat().st_mtime, reverse=True)
                        if snaps:
                            zf.write(str(snaps[0]), arcname=f"backups/{snaps[0].name}")
                            n += 1
                os.replace(tmp_path, out_path)
            except Exception:
                try: os.remove(tmp_path)
                except OSError: pass
                raise
            # Record backup timestamp so Settings can show staleness (T295).
            import time as _bk_time
            _backup_ts = _bk_time.time()
            try:
                _c2 = load_config()
                _c2["last_backup_ts"] = _backup_ts
                save_config(_c2)
            except Exception:
                pass
            _resp = {"ok": True, "path": out_path, "files": n,
                     "last_backup_ts": _backup_ts}
            if _fts_skipped_reason:
                _resp["fts_skipped"] = _fts_skipped_reason
            return _resp
        except Exception as e:
            return _api_err("BACKUP_WRITE_FAILED", str(e))


    def import_full_backup_preview(self):
        """Audit U-11: read-only preview of a backup ZIP before restoring.

        Opens the file picker, reads the ZIP's manifest (file names +
        sizes + modification times) WITHOUT extracting anything, and
        returns it so the frontend can show a confirmation modal.
        Frontend then passes the path back to import_full_backup() to
        commit the restore. Splits the previous one-click restore into
        a preview-then-confirm flow so the user can see what they're
        about to overwrite.
        """
        try:
            import zipfile as _zf

            import webview as _wv

            from backend.ytarchiver_config import APP_DATA_DIR
            if self._window is None:
                return {"ok": False, "error": "No window"}
            paths = self._window.create_file_dialog(
                _wv.OPEN_DIALOG,
                allow_multiple=False,
                file_types=("Backup ZIP (*.zip)", "All files (*.*)"),
            )
            zip_path = normalize_dialog_paths(paths)
            if not zip_path:
                return {"ok": False, "cancelled": True}
            try:
                with _zf.ZipFile(zip_path, "r") as zf:
                    items = []
                    total_bytes = 0
                    manifest = {}
                    try:
                        if _BACKUP_MANIFEST_NAME in zf.namelist():
                            manifest = json.loads(
                                zf.read(_BACKUP_MANIFEST_NAME).decode("utf-8"))
                    except Exception:
                        manifest = {}
                    for info in zf.infolist():
                        if info.is_dir():
                            continue
                        # Validate date_time tuple. zipfile sets it to
                        # (0,0,0,0,0,0) when the ZIP entry's date is
                        # missing / malformed — render as "unknown"
                        # rather than "0000-00-00 00:00" (audit:
                        # backup_mixin.py:242-251).
                        _dt_tuple = getattr(info, "date_time", None)
                        if (isinstance(_dt_tuple, tuple)
                                and len(_dt_tuple) >= 5
                                and _dt_tuple[0] >= 1980
                                and 1 <= _dt_tuple[1] <= 12
                                and 1 <= _dt_tuple[2] <= 31):
                            _mod = (
                                f"{_dt_tuple[0]:04d}-"
                                f"{_dt_tuple[1]:02d}-"
                                f"{_dt_tuple[2]:02d} "
                                f"{_dt_tuple[3]:02d}:"
                                f"{_dt_tuple[4]:02d}"
                            )
                        else:
                            _mod = "unknown"
                        items.append({
                            "name": info.filename,
                            "size": info.file_size,
                            "size_label": self._fmt_bytes_short(info.file_size),
                            "modified": _mod,
                        })
                        total_bytes += info.file_size
            except Exception as e:
                return {"ok": False, "error": f"Not a valid ZIP: {e}"}
            return {
                "ok": True,
                "zip_path": zip_path,
                "items": items,
                "manifest": manifest,
                "fts_skipped": manifest.get("fts_skipped_reason", ""),
                "total_bytes": total_bytes,
                "total_label": self._fmt_bytes_short(total_bytes),
                "snapshot_target": str(APP_DATA_DIR / "backups" /
                                        "config_pre_restore_*.json"),
            }
        except Exception as e:
            return _api_err("BACKUP_READ_FAILED", str(e))

    @staticmethod
    def _fmt_bytes_short(b):
        try: b = int(b or 0)
        except (TypeError, ValueError): return "0 B"
        if b < 1024: return f"{b} B"
        if b < 1024 * 1024: return f"{b / 1024:.1f} KB"
        if b < 1024 ** 3: return f"{b / (1024 * 1024):.1f} MB"
        return f"{b / (1024 ** 3):.2f} GB"


    def import_full_backup(self, zip_path=None):
        """Restore a previously-exported backup ZIP into %APPDATA%\\YTArchiver.

        Before overwriting any existing files, the current config is snapshotted
        to backups/config_pre_restore_YYYY-MM-DD_HHMMSS.json so the user can roll
        back. Gated by config_is_writable() — a read-only probe still
        lists the ZIP's contents so the frontend can confirm before committing.

        Audit U-11: `zip_path` may be supplied by the caller (after the
        user confirmed the preview). When None, falls back to opening
        the file picker directly (legacy one-click flow).
        """
        try:
            import datetime as _dt
            import shutil as _sh
            import zipfile as _zf

            import webview as _wv

            from backend.ytarchiver_config import (
                APP_DATA_DIR,
                CONFIG_FILE,
                config_is_writable,
            )
            if self._window is None:
                return {"ok": False, "error": "No window"}
            zip_path = (zip_path or "").strip()
            if not zip_path:
                paths = self._window.create_file_dialog(
                    _wv.OPEN_DIALOG,
                    allow_multiple=False,
                    file_types=("Backup ZIP (*.zip)", "All files (*.*)"),
                )
                zip_path = normalize_dialog_paths(paths)
                if not zip_path:
                    return {"ok": False, "cancelled": True}

            # First pass: list contents (read-only; safe even if gated off).
            try:
                with _zf.ZipFile(zip_path, "r") as zf:
                    names = [n for n in zf.namelist() if not n.endswith("/")]
            except Exception as e:
                return {"ok": False, "error": f"Not a valid ZIP: {e}"}
            if not names:
                return {"ok": False, "error": "Backup is empty"}

            # Whitelist — only restore files we recognise from export.
            # also allow the FTS index DB so backups that
            # include it can restore cleanly.
            allowed_top = _allowed_backup_top_names()

            if not config_is_writable():
                return {
                    "ok": False,
                    "write_blocked": True,
                    "zip_path": zip_path,
                    "names": names,
                    "error": "Write-gate off",
                }

            # Snapshot current config BEFORE touching anything.
            backup_dir = APP_DATA_DIR / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            ts = _dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
            snap_path = backup_dir / f"config_pre_restore_{ts}.json"
            if CONFIG_FILE.exists():
                try:
                    _sh.copy2(str(CONFIG_FILE), str(snap_path))
                except Exception as e:
                    return {"ok": False, "error": f"Pre-restore snapshot failed: {e}"}

            # Extract whitelisted files.
            restored = []
            skipped = []
            # resolve APP_DATA_DIR once for path-containment
            # checks. Any target that doesn't resolve under it after
            # path-join gets rejected. This blocks a crafted ZIP with
            # "backups/../../../../Windows/File.json"-style names from
            # writing outside APP_DATA_DIR even though each individual
            # component looks innocuous.
            _app_data_resolved = Path(str(APP_DATA_DIR)).resolve()
            with _zf.ZipFile(zip_path, "r") as zf:
                for name in names:
                    # reject entries containing `..` OR a
                    # drive letter / absolute path. ZipFile preserves
                    # the raw name, which on Windows can contain
                    # drive-qualified paths that write anywhere.
                    _bad_chars = (".." in name.split("/")
                                  or name.startswith(("/", "\\"))
                                  or (len(name) > 1 and name[1] == ":"))
                    if _bad_chars:
                        skipped.append(f"{name} (rejected — suspicious path)")
                        continue
                    # Strip any directory prefix for top-level files; keep
                    # backups/config_*.json in its folder.
                    if name.startswith("backups/") and name.endswith(".json"):
                        target = APP_DATA_DIR / name
                    else:
                        base = os.path.basename(name)
                        if base not in allowed_top:
                            skipped.append(name)
                            continue
                        target = APP_DATA_DIR / base
                    # final containment check — resolve the target
                    # and require it to be a TRUE child of
                    # APP_DATA_DIR, not just a string prefix-match.
                    # The previous startswith() check would let a
                    # path like .../YTArchiver2/file pass when
                    # APP_DATA_DIR resolves to .../YTArchiver,
                    # allowing extraction outside the directory via
                    # crafted ZIP names. Path.is_relative_to gives
                    # the strict parent-child check.
                    try:
                        _t_resolved = Path(str(target)).resolve()
                        try:
                            _ok_contained = _t_resolved.is_relative_to(_app_data_resolved)
                        except AttributeError:
                            # Python <3.9 fallback — compare parents.
                            _ok_contained = (
                                _app_data_resolved in _t_resolved.parents
                                or _t_resolved == _app_data_resolved)
                        if not _ok_contained:
                            skipped.append(f"{name} (rejected — escapes APP_DATA_DIR)")
                            continue
                    except Exception:
                        skipped.append(f"{name} (rejected — path resolve failed)")
                        continue
                    # Atomic extract — write to .restore.tmp then
                    # os.replace into target. Previously a crash /
                    # disk-full / read error mid-write truncated the
                    # live target file (config.json / queue.json /
                    # the DB). After this fix the live target stays
                    # intact until the new content is fully buffered
                    # to disk.
                    try:
                        target.parent.mkdir(parents=True, exist_ok=True)
                        _tmp = str(target) + ".restore.tmp"
                        try:
                            with zf.open(name, "r") as src, open(_tmp, "wb") as dst:
                                dst.write(src.read())
                            os.replace(_tmp, str(target))
                        except Exception:
                            try: os.remove(_tmp)
                            except OSError: pass
                            raise
                        restored.append(target.name)
                    except Exception as e:
                        skipped.append(f"{name} ({e})")

            # Force a reload of in-memory config.
            self._reload_config()
            return {
                "ok": True,
                "files_restored": len(restored),
                "restored": restored,
                "skipped": skipped,
                "pre_restore_snapshot": str(snap_path) if CONFIG_FILE.exists() else None,
                "zip_path": zip_path,
                "needs_restart": True,
            }
        except Exception as e:
            return _api_err("BACKUP_READ_FAILED", str(e))
