"""Managed filesystem operations.

Destructive app actions should come through this layer so path containment,
config writability, and sidecar cleanup rules stay consistent.
"""

from __future__ import annotations

import os
import shutil
from typing import Any

from backend.utils import (
    delete_video_sidecars,
    is_within_managed_roots,
    unhide_file_win,
)
from backend.ytarchiver_config import config_is_writable


def _result(ok: bool, **extra: Any) -> dict[str, Any]:
    return {"ok": ok, **extra}


def assert_within_managed_roots(path: str) -> dict[str, Any]:
    """Return ok only when path resolves under configured archive roots."""
    if not path:
        return _result(False, error="No path provided")
    if not is_within_managed_roots(path):
        return _result(
            False,
            error="Refusing to operate on a file outside the archive.",
        )
    return _result(True, path=os.path.normpath(path))


def safe_remove_file(path: str, *, require_config_writable: bool = True,
                     reason: str = "",
                     unhide_first: bool = False) -> dict[str, Any]:
    """Remove one managed file after containment and writability checks."""
    guard = assert_within_managed_roots(path)
    if not guard.get("ok"):
        return guard
    if require_config_writable and not config_is_writable():
        return _result(
            False,
            error=("Config is currently read-only. Refusing to delete file "
                   "when app state cannot be updated."),
        )
    try:
        if unhide_first:
            unhide_file_win(path)
        os.remove(path)
    except OSError as exc:
        return _result(False, error=str(exc))
    return _result(True, path=os.path.normpath(path), reason=reason)


def safe_remove_sidecars(video_path: str) -> dict[str, Any]:
    """Best-effort sidecar cleanup for a managed video path."""
    guard = assert_within_managed_roots(video_path)
    if not guard.get("ok"):
        return guard
    delete_video_sidecars(video_path)
    return _result(True, path=os.path.normpath(video_path))


def safe_rmtree_channel_folder(
    folder_path: str,
    *,
    require_config_writable: bool = True,
    reason: str = "",
) -> dict[str, Any]:
    """Recursively remove one managed channel folder."""
    guard = assert_within_managed_roots(folder_path)
    if not guard.get("ok"):
        return guard
    if require_config_writable and not config_is_writable():
        return _result(
            False,
            error=("Config is currently read-only. Refusing to delete folder "
                   "when app state cannot be updated."),
            folder_path=os.path.normpath(folder_path),
        )
    if os.path.islink(folder_path):
        return _result(
            False,
            error="Refusing to recursively delete a symlink.",
            folder_path=os.path.normpath(folder_path),
        )
    if not os.path.isdir(folder_path):
        return _result(
            True,
            deleted_folder=False,
            folder_path=os.path.normpath(folder_path),
            reason=reason,
        )

    failed_paths: list[tuple[str, str]] = []

    def _onerr(_fn: Any, path: str, exc_info: Any) -> None:
        try:
            failed_paths.append((path, str(exc_info[1])))
        except Exception:
            failed_paths.append((str(path), "?"))

    shutil.rmtree(folder_path, onerror=_onerr)
    result = _result(
        True,
        deleted_folder=not failed_paths,
        folder_path=os.path.normpath(folder_path),
        reason=reason,
    )
    if failed_paths:
        result["delete_error"] = (
            f"{len(failed_paths)} item(s) could not be removed "
            f"(first: {failed_paths[0][0]})")
        result["delete_partial_failures"] = failed_paths
    return result
