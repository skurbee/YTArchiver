"""
BackupMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

import json
from typing import Any

from backend import subs as subs_backend

# v80: the zip-writing core + file list moved to backend/auto_backup.py
# so the scheduled auto-backup and this manual export share ONE
# implementation. These aliases keep existing call sites and tests
# working unchanged.
from backend.auto_backup import (
    BACKUP_MANIFEST_NAME as _BACKUP_MANIFEST_NAME,
)
from backend.auto_backup import (
    BackupCancelled as _BackupCancelled,
)
from backend.auto_backup import (
    backup_file_entries as _backup_file_entries,
)
from backend.auto_backup import (
    build_backup_zip as _build_backup_zip,
)
from backend.services.managed_work import admitted_operation
from backend.ytarchiver_config import (
    config_is_writable,
    config_transaction,
    load_config,
    update_config,
)

from ._shared import _api_err, normalize_dialog_paths


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
    "folder_override": 200,
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
                return {
                    "ok": False,
                    "error": ("Settings are temporarily read-only. Restart "
                              "YTArchiver and try again."),
                }
            added = 0
            # track WHY each entry was skipped so the UI can
            # tell the user (previously just reported a raw count with
            # no way to debug a partial import).
            skipped_reasons: list[dict[str, str]] = []
            with config_transaction() as cfg:
                from backend.sync import channel_folder_name

                existing_urls = {
                    subs_backend.normalize_channel_url(
                        c.get("url", "")).rstrip("/")
                    for c in cfg.get("channels", []) if isinstance(c, dict)
                }
                existing_folders = {
                    channel_folder_name(channel).strip().casefold():
                        str(channel.get("name") or channel.get("folder") or "")
                    for channel in cfg.get("channels", [])
                    if isinstance(channel, dict)
                    and channel_folder_name(channel).strip()
                }
                existing_names = {
                    str(channel.get("name") or channel.get("folder") or "")
                        .strip().casefold():
                        str(channel.get("name") or channel.get("folder") or "")
                    for channel in cfg.get("channels", [])
                    if isinstance(channel, dict)
                    and str(channel.get("name") or channel.get("folder") or "")
                        .strip()
                }
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
                            "name": (
                                ch.get("name") or ch.get("url") or "(unknown)"
                            ),
                            "reason": clean_err or "invalid channel",
                        })
                        continue
                    url_identity = clean_ch["url"].rstrip("/")
                    if url_identity in existing_urls:
                        skipped_reasons.append({
                            "name": clean_ch.get("name") or clean_ch["url"],
                            "reason": "already subscribed",
                        })
                        continue
                    name_identity = str(
                        clean_ch.get("name") or clean_ch.get("folder") or ""
                    ).strip().casefold()
                    if name_identity and name_identity in existing_names:
                        skipped_reasons.append({
                            "name": clean_ch.get("name") or clean_ch["url"],
                            "reason": "channel name is already used",
                        })
                        continue
                    folder_identity = channel_folder_name(
                        clean_ch).strip().casefold()
                    if folder_identity and folder_identity in existing_folders:
                        owner = existing_folders[folder_identity]
                        skipped_reasons.append({
                            "name": clean_ch.get("name") or clean_ch["url"],
                            "reason": (
                                "archive folder is already used"
                                + (f" by {owner}" if owner else "")
                            ),
                        })
                        continue
                    cfg.setdefault("channels", []).append(clean_ch)
                    existing_urls.add(url_identity)
                    if folder_identity:
                        existing_folders[folder_identity] = str(
                            clean_ch.get("name") or clean_ch.get("folder") or ""
                        )
                    if name_identity:
                        existing_names[name_identity] = str(
                            clean_ch.get("name") or clean_ch.get("folder") or ""
                        )
                    added += 1
                cfg["channels"].sort(
                    key=lambda channel: (channel.get("name") or "").lower()
                )
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
            import webview as _wv

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
            # v80: the actual zip write (atomic tmp+replace, FTS
            # snapshot via the sqlite3 backup API, manifest, latest
            # config snapshot) lives in auto_backup.build_backup_zip —
            # shared with the scheduled auto-backup.
            services = getattr(self, "services", None)
            queue_state = (getattr(services, "queues", None)
                           if services is not None else None)
            if queue_state is None:
                queue_state = getattr(self, "_queues", None)
            try:
                with admitted_operation(
                    self,
                    owner="backup-export",
                    label="Full backup export",
                ) as cancel_event:
                    _stats = _build_backup_zip(
                        out_path,
                        queue_state=queue_state,
                        cancel_event=cancel_event,
                    )
                    # Keep the timestamp write inside the admitted operation.
                    # Restore must not swap config after the ZIP commits but
                    # before this final state mutation retires.
                    import time as _bk_time
                    _backup_ts = _bk_time.time()
                    try:
                        update_config(
                            lambda cfg: cfg.__setitem__(
                                "last_backup_ts", _backup_ts))
                    except Exception:
                        pass
            except _BackupCancelled as exc:
                return {
                    "ok": False,
                    "cancelled": True,
                    "error": str(exc),
                }
            _resp = {"ok": True, "path": out_path,
                     "files": _stats["files"],
                     "last_backup_ts": _backup_ts}
            if _stats["fts_skipped_reason"]:
                _resp["fts_skipped"] = _stats["fts_skipped_reason"]
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
        """Validate, stage, and atomically restore application state.

        Live files are replaced only after the complete ZIP passes limits,
        checksum, JSON, and SQLite validation. The old in-memory state owners
        are frozen before commit and the UI must restart afterward.
        """
        try:
            import webview as _wv

            from backend.services.restore_coordinator import restore_backup
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

            if not config_is_writable():
                return {
                    "ok": False,
                    "write_blocked": True,
                    "zip_path": zip_path,
                    "error": ("Settings are temporarily read-only. Restart "
                              "YTArchiver and try again."),
                }
            prepare = getattr(self, "_prepare_restore_commit_fn", None)
            if not callable(prepare):
                return {
                    "ok": False,
                    "error": (
                        "Restore safety coordinator is not ready. Restart "
                        "YTArchiver and try again."
                    ),
                }
            result = restore_backup(zip_path, before_commit=prepare)
            result["zip_path"] = zip_path
            if result.get("ok"):
                # Shutdown must not flush any pre-restore in-memory snapshot.
                self._restore_state_committed = True
            elif getattr(self, "_restore_quiesced", False):
                # Validation has already passed and this process crossed the
                # one-way freeze boundary. Even when commit/rollback reports a
                # failure, using the old in-memory owners again would be unsafe.
                result["needs_restart"] = True
            return result
        except Exception as e:
            result = _api_err("BACKUP_READ_FAILED", str(e))
            if getattr(self, "_restore_quiesced", False):
                result["needs_restart"] = True
            return result
