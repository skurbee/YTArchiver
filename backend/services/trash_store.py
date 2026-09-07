"""Trash persistence protocol shared by filesystem transactions and orchestration.

Owns manifest names, durable JSON publication and restore-recovery discovery.
Callers retain operation leases, epoch checks, and move/rollback sequencing.
This module has no catalog or configuration dependency.
"""
from __future__ import annotations

import json
import os
from typing import Any

from .atomic_json import publish_json

MANIFEST_NAME = ".ytarchiver-trash.json"
MANIFEST_VERSION = 2
RESTORE_RECOVERY_DIR = ".ytarchiver-restore-recovery"
PURGE_RECOVERY_DIR = ".ytarchiver-purge-recovery"
PURGE_PREFIX = ".ytarchiver-purge-"


class TrashStore:
    """Durable manifest and journal I/O; formats remain rollback compatible."""

    def publish_object(self, path: str, value: dict[str, Any]) -> str:
        """Atomically publish and flush one JSON object at *path*."""
        return publish_json(path, value)


    def write_manifest(self, folder: str, manifest: dict[str, Any]) -> str:
        """Atomically publish a trash manifest inside *folder*."""
        manifest_path = os.path.join(folder, MANIFEST_NAME)
        return self.publish_object(manifest_path, manifest)


    def restore_marker_path(self, trashed_folder_path: str) -> str:
        """Return the outside-the-entry marker used during final cleanup."""
        folder = os.path.normpath(trashed_folder_path)
        trash_root = os.path.dirname(folder)
        return os.path.join(
            trash_root,
            RESTORE_RECOVERY_DIR,
            f"{os.path.basename(folder)}.json",
        )


    def write_restore_marker(self,
        trashed_folder_path: str,
        manifest: dict[str, Any],
        *,
        archive_root: str = "",
    ) -> str:
        """Publish recovery metadata outside a folder before its manifest moves."""
        marker_path = self.restore_marker_path(trashed_folder_path)
        recovery_dir = os.path.dirname(marker_path)
        if not self.restore_recovery_is_safe(
                trashed_folder_path, archive_root=archive_root):
            raise OSError("Trash restore recovery folder is not safely contained.")
        os.makedirs(recovery_dir, exist_ok=True)
        if not self.restore_recovery_is_safe(
                trashed_folder_path, archive_root=archive_root):
            raise OSError(
                "Trash restore recovery folder is a link, junction, or outside "
                "the archive.")
        return self.publish_object(marker_path, manifest)


    def contains_entry(self, trashed_folder_path: str, archive_root: str) -> bool:
        try:
            archive_root = os.path.realpath(archive_root)
            trash_path = os.path.join(archive_root, ".YTArchiver Trash")
            is_junction = getattr(os.path, "isjunction", lambda _path: False)
            if os.path.islink(trash_path) or is_junction(trash_path):
                return False
            trash_root = os.path.realpath(trash_path)
            if (trash_root == archive_root
                    or os.path.commonpath([trash_root, archive_root])
                    != archive_root):
                return False
            target = os.path.realpath(trashed_folder_path)
            return (target != trash_root
                    and os.path.commonpath([target, trash_root]) == trash_root)
        except (ValueError, OSError):
            return False


    def restore_recovery_is_safe(self,
        trashed_folder_path: str,
        *,
        archive_root: str = "",
    ) -> bool:
        folder = os.path.normpath(trashed_folder_path)
        root = os.path.normpath(
            archive_root or os.path.dirname(os.path.dirname(folder)))
        recovery_dir = os.path.dirname(self.restore_marker_path(folder))
        is_junction = getattr(os.path, "isjunction", lambda _path: False)
        try:
            return (
                bool(root)
                and self.contains_entry(folder, root)
                and not os.path.islink(recovery_dir)
                and not is_junction(recovery_dir)
                and self.contains_entry(recovery_dir, root)
            )
        except (OSError, TypeError, ValueError):
            return False


    def read_manifest(self,
        trashed_folder_path: str,
        *,
        archive_root: str = "",
    ):
        inside_path = os.path.join(
            trashed_folder_path, MANIFEST_NAME)
        recovery_path = self.restore_marker_path(trashed_folder_path)
        manifest_paths = [inside_path]
        if self.restore_recovery_is_safe(
                trashed_folder_path, archive_root=archive_root):
            manifest_paths.append(recovery_path)
        for manifest_path in manifest_paths:
            try:
                with open(manifest_path, encoding="utf-8") as f:
                    manifest = json.load(f)
                if isinstance(manifest, dict):
                    return manifest, manifest_path
            except (OSError, ValueError):
                continue
        return None, inside_path



TRASH_STORE = TrashStore()
