"""Managed filesystem operations.

Destructive app actions should come through this layer so path containment,
config writability, and sidecar cleanup rules stay consistent.
"""

from __future__ import annotations

import os
import glob
import json
import shutil
from datetime import datetime
from typing import Any

from backend.fs_safety import _file_has_hidden_attribute
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
    # Single-video downloads live here, outside the channel tree (mirrors
    # is_within_managed_roots so containment stays consistent).
    video_out_dir = (cfg.get("video_out_dir") or "").strip()
    if video_out_dir:
        roots.append(video_out_dir)
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


def _video_sidecar_paths(video_path: str) -> list[str]:
    """Return app-managed sidecars that should follow a trashed video."""
    if not video_path:
        return []
    base = os.path.splitext(video_path)[0]
    paths: list[str] = []
    for ext in (
        ".jsonl",
        ".info.json",
        ".description",
        ".live_chat.json",
        ".srt",
    ):
        paths.append(base + ext)
    for ext in (".jpg", ".jpeg", ".webp", ".png"):
        image_path = base + ext
        try:
            if os.path.isfile(image_path) and _file_has_hidden_attribute(
                    image_path):
                paths.append(image_path)
        except OSError:
            continue
    base_glob = glob.escape(base)
    for pat in (
        base_glob + ".*.vtt",
        base_glob + ".*.srt",
        base_glob + ".*.ttml",
    ):
        try:
            paths.extend(glob.glob(pat))
        except OSError:
            continue

    seen: set[str] = set()
    result: list[str] = []
    for path in paths:
        try:
            norm = os.path.normcase(os.path.normpath(path))
        except (TypeError, ValueError):
            continue
        if norm in seen:
            continue
        seen.add(norm)
        if os.path.isfile(path):
            result.append(path)
    return result


def _unique_child_path(parent: str, basename: str) -> str:
    candidate = os.path.join(parent, basename)
    stem, ext = os.path.splitext(basename)
    suffix = 1
    while os.path.exists(candidate):
        suffix += 1
        candidate = os.path.join(parent, f"{stem}-{suffix}{ext}")
    return candidate


def safe_trash_video_file(
    video_path: str,
    *,
    require_config_writable: bool = True,
    reason: str = "",
    unhide_first: bool = False,
) -> dict[str, Any]:
    """Move one managed video and its sidecars to app trash/quarantine."""
    guard = assert_within_managed_roots(video_path)
    if not guard.get("ok"):
        return guard
    if require_config_writable and not config_is_writable():
        return _result(
            False,
            error=("Config is currently read-only. Refusing to move file "
                   "to trash when app state cannot be updated."),
            path=os.path.normpath(video_path),
        )
    if os.path.islink(video_path):
        return _result(
            False,
            error="Refusing to move a symlinked video file.",
            path=os.path.normpath(video_path),
        )
    if not os.path.isfile(video_path):
        return _result(
            False,
            error="File not found.",
            path=os.path.normpath(video_path),
        )

    archive_root = _managed_root_for(video_path)
    if not archive_root:
        return _result(
            False,
            error="Could not resolve archive root for video file.",
            path=os.path.normpath(video_path),
        )

    moved_files: list[dict[str, str]] = []
    try:
        trash_folder = _trash_path_for(video_path, archive_root)
        os.makedirs(trash_folder, exist_ok=False)
        if unhide_first:
            unhide_file_win(video_path)
        trashed_video = _unique_child_path(
            trash_folder, os.path.basename(video_path))
        shutil.move(video_path, trashed_video)
        moved_files.append({
            "role": "video",
            "original_path": os.path.normpath(video_path),
            "trashed_path": os.path.normpath(trashed_video),
        })

        for sidecar in _video_sidecar_paths(video_path):
            try:
                unhide_file_win(sidecar)
                trashed_sidecar = _unique_child_path(
                    trash_folder, os.path.basename(sidecar))
                shutil.move(sidecar, trashed_sidecar)
                moved_files.append({
                    "role": "sidecar",
                    "original_path": os.path.normpath(sidecar),
                    "trashed_path": os.path.normpath(trashed_sidecar),
                })
            except OSError:
                continue

        manifest = {
            "original_path": os.path.normpath(video_path),
            "trashed_path": os.path.normpath(trashed_video),
            "trashed_folder_path": os.path.normpath(trash_folder),
            "trashed_at": datetime.now().isoformat(timespec="seconds"),
            "reason": reason,
            "files": moved_files,
        }
        with open(
            os.path.join(trash_folder, ".ytarchiver-trash.json"),
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
    except OSError as exc:
        return _result(
            False,
            error=str(exc),
            path=os.path.normpath(video_path),
            reason=reason,
        )

    return _result(
        True,
        path=os.path.normpath(video_path),
        trashed_file_path=os.path.normpath(trashed_video),
        trashed_folder_path=os.path.normpath(trash_folder),
        files=moved_files,
        reason=reason,
    )


# ── T303: trash restore / purge / list ────────────────────────────────────
# The quarantine writers above move files into {archive_root}/.YTArchiver
# Trash/<stamp>-<name>/ with a .ytarchiver-trash.json manifest. These read
# that manifest back so the user can recover a mistaken delete or empty the
# trash. All three are containment-checked against the trash root so a
# malformed/forged manifest path can never restore-clobber or purge outside
# the trash directory.


def _is_within_trash_root(trashed_folder_path: str, archive_root: str) -> bool:
    try:
        trash_root = os.path.realpath(
            os.path.join(archive_root, ".YTArchiver Trash"))
        target = os.path.realpath(trashed_folder_path)
        return (target != trash_root
                and os.path.commonpath([target, trash_root]) == trash_root)
    except (ValueError, OSError):
        return False


def _read_trash_manifest(trashed_folder_path: str):
    manifest_path = os.path.join(
        trashed_folder_path, ".ytarchiver-trash.json")
    try:
        with open(manifest_path, encoding="utf-8") as f:
            return json.load(f), manifest_path
    except (OSError, ValueError):
        return None, manifest_path


def list_trash_entries(archive_root: str) -> dict[str, Any]:
    """List quarantined entries under {archive_root}/.YTArchiver Trash.

    Read-only. Returns {ok, entries:[{trashed_folder_path, original_path,
    trashed_at, reason, file_count}]} newest-first.
    """
    trash_root = os.path.join(archive_root, ".YTArchiver Trash")
    entries: list[dict[str, Any]] = []
    if not os.path.isdir(trash_root):
        return _result(True, entries=entries)
    for name in os.listdir(trash_root):
        folder = os.path.join(trash_root, name)
        manifest, _ = _read_trash_manifest(folder)
        if manifest is None:
            continue
        files = manifest.get("files") or []
        entries.append({
            "trashed_folder_path": os.path.normpath(folder),
            "original_path": manifest.get("original_path", ""),
            "trashed_at": manifest.get("trashed_at", ""),
            "reason": manifest.get("reason", ""),
            "file_count": len(files),
        })
    entries.sort(key=lambda e: e.get("trashed_at", ""), reverse=True)
    return _result(True, entries=entries)


def restore_trash_entry(trashed_folder_path: str, *,
                        archive_root: str = "",
                        require_config_writable: bool = True
                        ) -> dict[str, Any]:
    """Move a quarantined entry's files back to their original paths.

    Refuses if the folder isn't inside .YTArchiver Trash, if config is
    read-only, or if ANY destination already exists (never clobber a live
    file). On full success removes the now-empty trash folder + manifest.
    """
    if require_config_writable and not config_is_writable():
        return _result(False, error="Config is read-only; cannot restore.")
    manifest, manifest_path = _read_trash_manifest(trashed_folder_path)
    if manifest is None:
        return _result(False, error="Trash manifest not found or unreadable.")
    root = archive_root or _managed_root_for(
        manifest.get("original_path", ""))
    if not root or not _is_within_trash_root(trashed_folder_path, root):
        return _result(
            False, error="Refusing to restore from outside the app trash.")
    files = manifest.get("files") or []
    # Pre-check: refuse the whole restore if any destination already exists
    # so a half-restore can't leave the archive in a mixed state.
    for entry in files:
        dest = entry.get("original_path") or ""
        if dest and os.path.exists(dest):
            return _result(
                False,
                error=f"Destination already exists: {dest}. Restore aborted.")
    restored: list[str] = []
    for entry in files:
        src = entry.get("trashed_path") or ""
        dest = entry.get("original_path") or ""
        if not src or not dest or not os.path.exists(src):
            continue
        try:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.move(src, dest)
            restored.append(os.path.normpath(dest))
        except OSError as exc:
            return _result(False, error=str(exc), restored=restored)
    try:
        os.remove(manifest_path)
        os.rmdir(trashed_folder_path)
    except OSError:
        pass  # leftover stray files — harmless; folder stays in trash list
    return _result(True, restored=restored)


def purge_trash_entry(trashed_folder_path: str, *,
                      archive_root: str = "") -> dict[str, Any]:
    """Permanently delete one quarantined entry (rmtree).

    Containment-checked against .YTArchiver Trash so it can never escape the
    trash directory. This is the explicit user "empty trash" action — the
    only sanctioned permanent delete in the trash model.
    """
    root = archive_root
    if not root:
        manifest, _ = _read_trash_manifest(trashed_folder_path)
        root = _managed_root_for(
            (manifest or {}).get("original_path", "")) if manifest else ""
    if not root or not _is_within_trash_root(trashed_folder_path, root):
        return _result(False, error="Refusing to purge outside the app trash.")
    try:
        shutil.rmtree(trashed_folder_path)
    except OSError as exc:
        return _result(False, error=str(exc))
    return _result(True, purged=os.path.normpath(trashed_folder_path))


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
