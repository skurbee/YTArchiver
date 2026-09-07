"""Remove one additional Search root without taking ownership of media files."""
from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

from .config_repository import CONFIG_COMMAND_LOCK, ConfigRepository


def _key(path):
    return os.path.normcase(os.path.abspath(os.path.normpath(str(path))))


class ArchiveRootCommands:
    def __init__(self, config: ConfigRepository,
                 delete_catalog: Callable[[str], dict[str, Any]]):
        self._config = config
        self._delete_catalog = delete_catalog

    def remove(self, folder: str, *, cancelled: Callable[[], bool] = lambda: False):
        raw = str(folder or "").strip()
        root = os.path.abspath(os.path.normpath(raw)) if raw else ""
        result = {"ok": False, "removed": False, "already_removed": False,
                  "root": root, "videos": 0, "segments": 0,
                  "config_restored": True, "retryable": False}
        if not root:
            return {**result, "error": "Archive folder is required."}
        with CONFIG_COMMAND_LOCK:
            try:
                cfg = self._config.load()
                primary = str(cfg.get("output_dir") or "").strip()
                if primary:
                    try:
                        common = os.path.commonpath([_key(root), _key(primary)])
                        if common in {_key(root), _key(primary)}:
                            return {**result, "error": "This folder overlaps the primary archive."}
                    except ValueError:
                        pass  # Separate drives cannot overlap.
                if not any(_key(value) == _key(root)
                           for value in cfg.get("tp_archive_roots", []) if value):
                    return {**result, "ok": True, "already_removed": True}
                if cancelled():
                    return {**result, "cancelled": True, "retryable": True,
                            "error": "Folder removal cancelled."}

                # Keep the root configured until cleanup and its commit finish.
                # Failed/interrupted operations therefore remain visible for a
                # safe repeat; no hidden intent or whole-config rollback exists.
                cleanup = self._delete_catalog(root)
                result.update({name: int(cleanup.get(name) or 0)
                               for name in ("videos", "segments")})
                if not cleanup.get("ok"):
                    return {**result, "retryable": True,
                            "error": str(cleanup.get("error") or "Catalog cleanup failed.")}
                if cancelled():
                    return {**result, "cancelled": True, "retryable": True,
                            "catalog_cleaned": True,
                            "error": "Catalog cleaned; folder remains configured. Retry removal."}

                def remove_owned_root(live):
                    live["tp_archive_roots"] = [
                        value for value in live.get("tp_archive_roots", [])
                        if not value or _key(value) != _key(root)]

                try:
                    self._config.mutate(remove_owned_root)
                except Exception as exc:
                    return {**result, "retryable": True, "catalog_cleaned": True,
                            "error": ("Catalog cleaned, but the folder remains configured "
                                      f"because its removal could not be saved: {exc}")}
                return {**result, "ok": True, "removed": True}
            except Exception as exc:
                return {**result, "retryable": True, "error": str(exc)}
