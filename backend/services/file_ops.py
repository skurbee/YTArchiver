"""Managed filesystem operations.

Destructive app actions should come through this layer so path containment,
config writability, and sidecar cleanup rules stay consistent.
"""

from __future__ import annotations

import os
import json
import shutil
from datetime import datetime
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


def _managed_root_for(path: str) -> str:
    """Return the configured managed root containing path, or empty string."""
    try:
        from backend.ytarchiver_config import load_config
        cfg = load_config() or {}
    except Exception:
        return ""
    roots: list[str] = []
    output_dir = (cfg.get("output_dir") or "").strip()
    if output_dir:
        roots.append(output_dir)
    roots.extend(str(r) for r in (cfg.get("tp_archive_roots") or []) if r)
    try:
        target = os.path.normcase(os.path.realpath(path))
    except (ValueError, OSError):
        return ""
    matches: list[tuple[int, str]] = []
    for root in roots:
        try:
            real_root = os.path.normcase(os.path.realpath(root))
            if os.path.commonpath([target, real_root]) == real_root:
                matches.append((len(real_root), os.path.realpath(root)))
        except (ValueError, OSError):
            continue
    if not matches:
        return ""
    return max(matches)[1]


def _trash_path_for(folder_path: str, archive_root: str) -> str:
    trash_root = os.path.join(archive_root, ".YTArchiver Trash")
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    base = os.path.basename(os.path.normpath(folder_path)) or "channel"
    base = base.replace(os.sep, "_").replace("/", "_").replace("\\", "_")
    candidate = os.path.join(trash_root, f"{stamp}-{base}")
    suffix = 1
    while os.path.exists(candidate):
        suffix += 1
        candidate = os.path.join(trash_root, f"{stamp}-{base}-{suffix}")
    return candidate


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
    """Move one managed channel folder to the app trash/quarantine."""
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
            error="Refusing to move a symlinked channel folder.",
            folder_path=os.path.normpath(folder_path),
        )
    if not os.path.isdir(folder_path):
        return _result(
            True,
            deleted_folder=False,
            folder_path=os.path.normpath(folder_path),
            reason=reason,
        )

    archive_root = _managed_root_for(folder_path)
    if not archive_root:
        return _result(
            False,
            error="Could not resolve archive root for channel folder.",
            folder_path=os.path.normpath(folder_path),
        )
    try:
        trash_path = _trash_path_for(folder_path, archive_root)
        os.makedirs(os.path.dirname(trash_path), exist_ok=True)
        shutil.move(folder_path, trash_path)
        manifest = {
            "original_path": os.path.normpath(folder_path),
            "trashed_path": os.path.normpath(trash_path),
            "trashed_at": datetime.now().isoformat(timespec="seconds"),
            "reason": reason,
        }
        with open(
            os.path.join(trash_path, ".ytarchiver-trash.json"),
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
    except OSError as exc:
        return _result(
            False,
            error=str(exc),
            deleted_folder=False,
            folder_path=os.path.normpath(folder_path),
            reason=reason,
        )
    result = _result(
        True,
        deleted_folder=not os.path.exists(folder_path),
        folder_path=os.path.normpath(folder_path),
        trashed_folder_path=os.path.normpath(trash_path),
        reason=reason,
    )
    return result
