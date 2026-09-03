"""User-facing Trash orchestration.

The filesystem layer stores recovery manifests.  This module is the trust
boundary used by the JS bridge: callers identify entries only by opaque IDs
and epochs returned by :meth:`TrashManager.list_entries`; client-provided file
paths are never accepted.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend import index as index_backend
from backend import index_maintenance, subs
from backend.log import get_logger
from backend.services import file_ops
from backend.ytarchiver_config import (
    TRASH_RETENTION_MAX_DAYS,
    load_config,
    locked_config_snapshot,
)

_log = get_logger(__name__)
_MANIFEST_NAME = ".ytarchiver-trash.json"
_PURGE_PREFIX = ".ytarchiver-purge-"
_PURGE_RECOVERY_DIR = ".ytarchiver-purge-recovery"
_ENTRY_ID_RE = re.compile(r"^[a-fA-F0-9]{32,64}$")


def _path_key(path: str) -> str:
    try:
        return os.path.normcase(os.path.realpath(path))
    except (OSError, TypeError, ValueError):
        return ""


def _root_id(path: str) -> str:
    return hashlib.sha256(("trash-root\0" + _path_key(path)).encode(
        "utf-8", "surrogatepass")).hexdigest()[:32]


def _legacy_entry_id(root: str, folder: str, manifest: dict[str, Any]) -> str:
    seed = "\0".join((
        "legacy-trash-entry",
        _path_key(root),
        os.path.basename(os.path.normpath(folder)).casefold(),
        str(manifest.get("transaction_id") or ""),
        str(manifest.get("trashed_at") or ""),
        str(manifest.get("original_path") or ""),
    ))
    return hashlib.sha256(seed.encode("utf-8", "surrogatepass")).hexdigest()


def _purge_marker_path(entry_path: str, entry_id: str) -> str:
    return os.path.join(
        os.path.dirname(entry_path),
        _PURGE_RECOVERY_DIR,
        f"{entry_id}.json",
    )


def _trash_root_is_safe(archive_root: str, trash_root: str = "") -> bool:
    """Reject a Trash root that redirects outside its configured archive."""
    candidate = trash_root or os.path.join(archive_root, ".YTArchiver Trash")
    is_junction = getattr(os.path, "isjunction", lambda _path: False)
    try:
        if os.path.islink(candidate) or is_junction(candidate):
            return False
        root_real = os.path.normcase(os.path.realpath(archive_root))
        trash_real = os.path.normcase(os.path.realpath(candidate))
        return (
            trash_real != root_real
            and os.path.commonpath([trash_real, root_real]) == root_real
        )
    except (OSError, TypeError, ValueError):
        return False


def _purge_recovery_dir_is_safe(path: str, archive_root: str) -> bool:
    is_junction = getattr(os.path, "isjunction", lambda _path: False)
    try:
        return (
            not os.path.islink(path)
            and not is_junction(path)
            and _trash_root_is_safe(archive_root)
            and file_ops._is_within_trash_root(path, archive_root)
        )
    except (OSError, TypeError, ValueError):
        return False


def _as_timestamp(value: Any, default: float = 0.0) -> float:
    if isinstance(value, datetime):
        try:
            if value.tzinfo is None:
                value = value.replace(tzinfo=UTC)
            parsed = value.timestamp()
            return parsed if math.isfinite(parsed) else default
        except (OSError, OverflowError, ValueError):
            return default
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    text = str(value or "").strip()
    if not text:
        return default
    try:
        parsed = float(text)
        return parsed if math.isfinite(parsed) else default
    except (OverflowError, ValueError):
        pass
    try:
        parsed = datetime.fromisoformat(
            text.replace("Z", "+00:00")).timestamp()
        return parsed if math.isfinite(parsed) else default
    except (OSError, OverflowError, ValueError):
        return default


def _automatic_removal_timestamp(
    manifest: dict[str, Any],
    *,
    now_ts: float,
) -> float:
    """Return a trustworthy removal time for unattended deletion.

    Display/listing may fall back to folder metadata, but automatic permanent
    deletion never does.  A missing, invalid, future, or materially
    inconsistent timestamp is preserved for manual review.
    """
    text = str(manifest.get("trashed_at") or "").strip()
    removed_at = _as_timestamp(text, 0.0)
    if removed_at <= 0 or removed_at > now_ts + 300:
        return 0.0
    raw_epoch = manifest.get("epoch")
    if raw_epoch not in (None, ""):
        epoch = _as_timestamp(raw_epoch, 0.0)
        # New v2 writers record both values seconds apart.  A large mismatch
        # means at least one field was copied/corrupted; fail closed.
        if epoch <= 0 or abs(epoch - removed_at) > 24 * 60 * 60:
            return 0.0
    return removed_at


def _strict_retention_policy(config: dict[str, Any]) -> tuple[int, float]:
    raw_days = config.get("trash_retention_days", 0)
    if isinstance(raw_days, bool):
        raise ValueError("Trash retention cannot be a boolean")
    if isinstance(raw_days, int):
        days = raw_days
    elif isinstance(raw_days, float) and raw_days.is_integer():
        days = int(raw_days)
    elif (isinstance(raw_days, str)
          and raw_days.strip().lstrip("+-").isdigit()):
        days = int(raw_days.strip())
    else:
        raise ValueError("Trash retention is not a whole number")
    if days != 0 and not 1 <= days <= TRASH_RETENTION_MAX_DAYS:
        raise ValueError(
            f"Trash retention must be 0 or 1-{TRASH_RETENTION_MAX_DAYS}")

    raw_grace = config.get("trash_retention_grace_until_ts", 0.0)
    grace = _as_timestamp(raw_grace, -1.0)
    if grace < 0:
        raise ValueError("Trash retention grace timestamp is invalid")
    return days, grace


def configured_archive_roots(cfg: dict[str, Any] | None = None) -> list[str]:
    """Return canonical, deduplicated roots that may own an app Trash."""
    value = cfg if isinstance(cfg, dict) else (load_config() or {})
    candidates = [
        value.get("output_dir"),
        value.get("video_out_dir"),
        *(value.get("tp_archive_roots") or []),
    ]
    roots: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        text = str(candidate or "").strip()
        if not text:
            continue
        absolute = os.path.abspath(text)
        key = _path_key(absolute)
        if not key or key in seen:
            continue
        seen.add(key)
        roots.append(absolute)
    return roots


def _manifest_epoch(manifest: dict[str, Any], entry_path: str) -> int:
    raw = manifest.get("epoch")
    try:
        epoch = int(raw)
    except (TypeError, ValueError):
        epoch = int(_as_timestamp(manifest.get("trashed_at"), 0))
    if epoch > 0:
        return epoch
    try:
        return int(os.path.getmtime(entry_path))
    except OSError:
        return 0


def _entry_type(manifest: dict[str, Any]) -> str:
    files = manifest.get("files")
    value = str(manifest.get("entry_type") or "").strip()
    if value:
        return value
    return "video" if isinstance(files, list) else "channel_folder"


def _entry_id(root: str, entry_path: str,
              manifest: dict[str, Any]) -> str:
    recorded = str(manifest.get("entry_id") or "").strip()
    if _ENTRY_ID_RE.fullmatch(recorded):
        return recorded.lower()
    return _legacy_entry_id(root, entry_path, manifest)


def _entry_file_stats(entry_path: str, manifest: dict[str, Any] | None
                      ) -> tuple[int, int, int, int]:
    """Return ``(bytes, file_count, untracked, missing_tracked)``."""
    if not os.path.isdir(entry_path):
        return 0, 0, 0, 0
    files = manifest.get("files") if isinstance(manifest, dict) else None
    needs_exact_paths = not isinstance(manifest, dict) or isinstance(files, list)
    actual: set[str] = set()
    actual_count = 0
    total_bytes = 0
    for dirpath, _dirnames, filenames in os.walk(entry_path):
        for filename in filenames:
            if filename == _MANIFEST_NAME or filename.startswith(
                    f"{_MANIFEST_NAME}.tmp-"):
                continue
            path = os.path.join(dirpath, filename)
            actual_count += 1
            if needs_exact_paths:
                actual.add(_path_key(path))
            try:
                total_bytes += max(0, int(os.path.getsize(path)))
            except OSError:
                pass
    if not isinstance(manifest, dict):
        return total_bytes, actual_count, actual_count, 0
    if not isinstance(files, list):
        # A channel manifest owns the complete directory tree atomically.
        return total_bytes, actual_count, 0, 0

    recorded_folder = str(
        manifest.get("trashed_folder_path") or entry_path)
    expected: set[str] = set()
    for record in files:
        if not isinstance(record, dict):
            continue
        recorded_path = str(record.get("trashed_path") or "")
        if not recorded_path:
            continue
        try:
            relative = os.path.relpath(recorded_path, recorded_folder)
        except (OSError, ValueError):
            continue
        if relative == os.pardir or relative.startswith(os.pardir + os.sep):
            continue
        expected.add(_path_key(os.path.join(entry_path, relative)))
    expected.discard("")
    return (
        total_bytes,
        actual_count,
        len(actual - expected),
        len(expected - actual),
    )


def _recorded_file_stats(
    manifest: dict[str, Any] | None,
) -> tuple[int, int, int, int]:
    """Return cheap manifest-only stats for the sidebar summary.

    The detailed Trash view still walks each entry so it can verify tracked
    files before offering destructive actions.  The always-on sidebar badge
    only needs an item count and must not recursively read a large archive at
    application startup.
    """
    if not isinstance(manifest, dict):
        return 0, 0, 1, 0
    files = manifest.get("files")
    if not isinstance(files, list):
        return 0, 0, 0, 0
    count = 0
    size_bytes = 0
    for record in files:
        if not isinstance(record, dict):
            continue
        if record.get("trashed_path") or record.get("original_path"):
            count += 1
        try:
            size_bytes += max(0, int(record.get("source_size") or 0))
        except (TypeError, ValueError):
            pass
    return size_bytes, count, 0, 0


def _infer_channel_url(folder: str) -> str:
    """Best-effort URL hint for legacy channel entries; never creates config."""
    checked = 0
    for dirpath, _dirnames, filenames in os.walk(folder):
        for filename in filenames:
            if not filename.endswith(".info.json"):
                continue
            checked += 1
            if checked > 100:
                return ""
            try:
                with open(os.path.join(dirpath, filename), encoding="utf-8") as handle:
                    value = json.load(handle)
            except (OSError, ValueError):
                continue
            if not isinstance(value, dict):
                continue
            for key in ("channel_url", "uploader_url", "webpage_url"):
                candidate = str(value.get(key) or "").strip()
                if ("youtube.com/@" in candidate
                        or "youtube.com/channel/" in candidate
                        or "youtube.com/c/" in candidate
                        or "youtube.com/user/" in candidate):
                    return candidate
    return ""


class TrashManager:
    """Serialize and validate all user-visible Trash mutations."""

    def __init__(self) -> None:
        self._mutation_lock = threading.RLock()

    def _scan(self, cfg: dict[str, Any] | None = None, *,
              include_stats: bool = True,
              ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        config = cfg if isinstance(cfg, dict) else (load_config() or {})
        roots_out: list[dict[str, Any]] = []
        entries: list[dict[str, Any]] = []
        for root in configured_archive_roots(config):
            rid = _root_id(root)
            trash_root = os.path.join(root, ".YTArchiver Trash")
            root_record = {
                "root_id": rid,
                "path": os.path.normpath(root),
                "trash_exists": os.path.isdir(trash_root),
                "available": os.path.isdir(root),
                "error": "",
            }
            roots_out.append(root_record)
            if not os.path.isdir(trash_root):
                continue
            if not _trash_root_is_safe(root, trash_root):
                root_record["error"] = (
                    "Trash is a link, junction, or resolves outside the "
                    "configured archive. YTArchiver left it untouched."
                )
                continue
            discovered: set[str] = set()
            try:
                children = list(os.scandir(trash_root))
            except OSError as exc:
                root_record["available"] = False
                root_record["error"] = str(exc)
                continue
            for child in children:
                if child.name in {
                        file_ops._RESTORE_RECOVERY_DIR,
                        _PURGE_RECOVERY_DIR}:
                    continue
                entry_path = child.path
                if not child.is_dir(follow_symlinks=False):
                    # Show unexpected direct children, but never authorize an
                    # operation on them.
                    try:
                        size = max(0, int(child.stat(follow_symlinks=False).st_size))
                    except OSError:
                        size = 0
                    invalid_id = hashlib.sha256((
                        "invalid-trash-child\0" + rid + "\0" + child.name.casefold()
                    ).encode("utf-8", "surrogatepass")).hexdigest()
                    entries.append({
                        "entry_id": invalid_id,
                        "epoch": 0,
                        "root_id": rid,
                        "entry_type": "unknown",
                        "state": "invalid",
                        "display_name": child.name,
                        "original_path": "",
                        "trashed_at": "",
                        "reason": "",
                        "size_bytes": size,
                        "file_count": 1,
                        "untracked_count": 1,
                        "missing_tracked_count": 0,
                        "manifest_version": 0,
                        "restore_scope": "unavailable",
                        "can_restore": False,
                        "can_purge": False,
                        "warnings": ["This item has no valid Trash manifest."],
                        "_entry_path": entry_path,
                        "_root_path": root,
                        "_manifest": None,
                    })
                    continue
                discovered.add(child.name.casefold())
                entries.append(self._describe_entry(
                    root, rid, entry_path, include_stats=include_stats))

            # A video restore publishes a marker outside the entry before its
            # final rmdir.  Enumerate marker-only residue as well as folders.
            recovery_dir = os.path.join(trash_root, file_ops._RESTORE_RECOVERY_DIR)
            if (os.path.isdir(recovery_dir)
                    and _purge_recovery_dir_is_safe(recovery_dir, root)):
                try:
                    markers = list(os.scandir(recovery_dir))
                except OSError:
                    markers = []
                for marker in markers:
                    if not marker.is_file(follow_symlinks=False) \
                            or not marker.name.endswith(".json"):
                        continue
                    basename = marker.name[:-len(".json")]
                    if basename.casefold() in discovered:
                        continue
                    entries.append(self._describe_entry(
                        root,
                        rid,
                        os.path.join(trash_root, basename),
                        marker_path=marker.path,
                        include_stats=include_stats,
                    ))
        entries.sort(key=lambda item: (
            int(item.get("epoch") or 0), str(item.get("display_name") or "")
        ), reverse=True)
        return roots_out, entries

    def _describe_entry(self, root: str, root_id: str, entry_path: str, *,
                        marker_path: str = "",
                        include_stats: bool = True) -> dict[str, Any]:
        staged_name = os.path.basename(entry_path)
        if not marker_path and staged_name.startswith(_PURGE_PREFIX):
            staged_id = staged_name[len(_PURGE_PREFIX):].strip().lower()
            if _ENTRY_ID_RE.fullmatch(staged_id):
                candidate = _purge_marker_path(entry_path, staged_id)
                if (_purge_recovery_dir_is_safe(
                        os.path.dirname(candidate), root)
                        and os.path.isfile(candidate)):
                    marker_path = candidate
        manifest, manifest_path = file_ops._read_trash_manifest(
            entry_path, archive_root=root)
        warnings: list[str] = []
        if not isinstance(manifest, dict) and marker_path:
            try:
                with open(marker_path, encoding="utf-8") as handle:
                    loaded = json.load(handle)
                is_purge_marker = (
                    os.path.basename(os.path.dirname(marker_path))
                    == _PURGE_RECOVERY_DIR
                )
                if is_purge_marker:
                    expected_id = staged_name[len(_PURGE_PREFIX):].strip().lower()
                    wrapper_valid = (
                        isinstance(loaded, dict)
                        and str(loaded.get("entry_id") or "").lower()
                        == expected_id
                        and _path_key(str(loaded.get("staged_path") or ""))
                        == _path_key(entry_path)
                        and isinstance(loaded.get("manifest"), dict)
                        and str(loaded["manifest"].get("entry_id") or "").lower()
                        == expected_id
                    )
                    loaded = loaded["manifest"] if wrapper_valid else None
                elif (isinstance(loaded, dict)
                      and isinstance(loaded.get("manifest"), dict)):
                    loaded = loaded["manifest"]
                manifest = loaded if isinstance(loaded, dict) else None
                manifest_path = marker_path
            except (OSError, ValueError):
                manifest = None
        if include_stats:
            size_bytes, file_count, untracked, missing = _entry_file_stats(
                entry_path, manifest)
        else:
            size_bytes, file_count, untracked, missing = (
                _recorded_file_stats(manifest))
        if not isinstance(manifest, dict):
            eid = hashlib.sha256((
                "invalid-trash-entry\0" + root_id + "\0"
                + os.path.basename(entry_path).casefold()
            ).encode("utf-8", "surrogatepass")).hexdigest()
            return {
                "entry_id": eid,
                "epoch": 0,
                "root_id": root_id,
                "entry_type": "unknown",
                "state": "invalid",
                "display_name": os.path.basename(entry_path),
                "original_path": "",
                "trashed_at": "",
                "reason": "",
                "size_bytes": size_bytes,
                "file_count": file_count,
                "untracked_count": untracked,
                "missing_tracked_count": missing,
                "manifest_version": 0,
                "restore_scope": "unavailable",
                "can_restore": False,
                "can_purge": False,
                "warnings": ["This item has no valid Trash manifest."],
                "_entry_path": entry_path,
                "_root_path": root,
                "_manifest": None,
                "_manifest_path": manifest_path,
            }
        try:
            version = int(manifest.get("version") or 0)
        except (TypeError, ValueError):
            version = -1
        state = str(manifest.get("state") or "complete")
        etype = _entry_type(manifest)
        snapshot = manifest.get("channel_snapshot")
        context = manifest.get("catalog_context")
        original = str(manifest.get("original_path") or "")
        display = ""
        if isinstance(snapshot, dict):
            display = str(snapshot.get("name") or snapshot.get("folder") or "")
        if not display and isinstance(context, dict):
            display = str(context.get("title") or context.get("channel") or "")
        if not display:
            display = os.path.basename(os.path.normpath(original or entry_path))
        supported = version in {0, 1, 2}
        staged = os.path.basename(entry_path).startswith(_PURGE_PREFIX)
        marker_only = bool(marker_path and not os.path.isdir(entry_path))
        if not supported:
            warnings.append("This Trash entry was created by an unsupported version.")
        if untracked:
            warnings.append(
                f"This entry contains {untracked} file(s) not recorded by its manifest.")
        if missing:
            warnings.append(
                f"This entry is missing {missing} file(s) recorded by its manifest.")
        if marker_only:
            warnings.append("Restore cleanup was interrupted; the recovery marker remains.")
        if staged:
            warnings.append("Permanent deletion was interrupted and can be retried.")
        has_snapshot = isinstance(snapshot, dict) and bool(snapshot)
        restore_scope = (
            "full" if etype == "channel_folder" and has_snapshot
            else "files_and_catalog" if etype == "video" and isinstance(context, dict)
            else "files_only"
        )
        complete = state == "complete" and supported
        resumable_video = (
            etype == "video"
            and state in {"pending", "restoring"}
            and supported
            and isinstance(manifest.get("files"), list)
            and bool(manifest.get("files"))
        )
        return {
            "entry_id": _entry_id(root, entry_path, manifest),
            "epoch": _manifest_epoch(manifest, entry_path),
            "root_id": root_id,
            "entry_type": etype,
            "state": state,
            "display_name": display,
            "original_path": os.path.normpath(original) if original else "",
            "trashed_at": str(manifest.get("trashed_at") or ""),
            "reason": str(manifest.get("reason") or ""),
            "size_bytes": size_bytes,
            "file_count": file_count,
            "untracked_count": untracked,
            "missing_tracked_count": missing,
            "manifest_version": version,
            "restore_scope": restore_scope,
            "can_restore": bool(
                (complete or resumable_video)
                and os.path.isdir(entry_path)
                and not staged),
            "can_purge": bool(complete and os.path.isdir(entry_path)
                              and not untracked and (not missing or staged)),
            "warnings": warnings,
            "warning": "; ".join(warnings),
            "_entry_path": entry_path,
            "_root_path": root,
            "_manifest": manifest,
            "_manifest_path": manifest_path,
        }

    @staticmethod
    def _public(entry: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in entry.items()
                if not key.startswith("_")}

    def list_entries(self, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
        config = cfg if isinstance(cfg, dict) else (load_config() or {})
        roots, entries = self._scan(config)
        public = [self._public(entry) for entry in entries]
        try:
            retention_days = int(config.get("trash_retention_days") or 0)
        except (TypeError, ValueError):
            retention_days = 0
        return {
            "ok": True,
            "roots": roots,
            "entries": public,
            "item_count": len(public),
            "file_count": sum(int(entry.get("file_count") or 0)
                              for entry in public),
            "untracked_count": sum(1 for entry in public
                                   if int(entry.get("untracked_count") or 0)
                                   or entry.get("state") == "invalid"),
            "retention_days": retention_days,
            "retention_grace_until_ts": _as_timestamp(
                config.get("trash_retention_grace_until_ts"), 0.0),
        }

    def summary(self, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
        config = cfg if isinstance(cfg, dict) else (load_config() or {})
        _roots, entries = self._scan(config, include_stats=False)
        try:
            retention_days = int(config.get("trash_retention_days") or 0)
        except (TypeError, ValueError):
            retention_days = 0
        return {
            "ok": True,
            "item_count": len(entries),
            "file_count": sum(int(entry.get("file_count") or 0)
                              for entry in entries),
            "untracked_count": sum(
                1 for entry in entries
                if int(entry.get("untracked_count") or 0)
                or entry.get("state") == "invalid"),
            "retention_days": retention_days,
            "retention_grace_until_ts": _as_timestamp(
                config.get("trash_retention_grace_until_ts"), 0.0),
        }

    def _resolve(self, entry_id: str, epoch: Any = None,
                 cfg: dict[str, Any] | None = None) -> dict[str, Any] | None:
        wanted_id = str(entry_id or "").strip().lower()
        wanted_epoch: int | None
        if epoch in (None, ""):
            wanted_epoch = None
        else:
            try:
                wanted_epoch = int(epoch)
            except (TypeError, ValueError):
                return None
        _roots, entries = self._scan(cfg, include_stats=False)
        matches = [entry for entry in entries
                   if str(entry.get("entry_id") or "").lower() == wanted_id
                   and (wanted_epoch is None
                        or int(entry.get("epoch") or 0) == wanted_epoch)]
        if len(matches) != 1:
            return None
        return self._refresh_known_entry(matches[0])

    def _refresh_known_entry(
        self,
        entry: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Revalidate one internally discovered entry without rescanning all."""
        root = str(entry.get("_root_path") or "")
        entry_path = str(entry.get("_entry_path") or "")
        if not root or not entry_path or not _trash_root_is_safe(root):
            return None
        marker_path = str(entry.get("_manifest_path") or "")
        if marker_path == os.path.join(entry_path, _MANIFEST_NAME):
            marker_path = ""
        if not os.path.isdir(entry_path) and not (
                marker_path and os.path.isfile(marker_path)):
            return None
        # Channel entries own their complete directory atomically; walking a
        # many-thousand-file channel is not needed to validate its manifest.
        # Video entries have a short explicit file list and still receive the
        # exact tracked/untracked comparison before deletion.
        include_stats = entry.get("entry_type") != "channel_folder"
        current = self._describe_entry(
            root,
            str(entry.get("root_id") or _root_id(root)),
            entry_path,
            marker_path=marker_path,
            include_stats=include_stats,
        )
        if (str(current.get("entry_id") or "").lower()
                != str(entry.get("entry_id") or "").lower()
                or int(current.get("epoch") or 0)
                != int(entry.get("epoch") or 0)):
            return None
        return current

    @staticmethod
    def _matching_channel_for_path(cfg: dict[str, Any], path: str
                                  ) -> dict[str, Any] | None:
        from backend.sync import channel_folder_name

        base = str(cfg.get("output_dir") or "").strip()
        wanted = _path_key(path)
        matches = []
        for channel in cfg.get("channels", []) or []:
            if not isinstance(channel, dict) or not base:
                continue
            candidate = os.path.join(base, channel_folder_name(channel))
            if _path_key(candidate) == wanted:
                matches.append(channel)
        return dict(matches[0]) if len(matches) == 1 else None

    def restore(self, entry_id: str, epoch: Any = None, *,
                cancel_event: threading.Event | None = None) -> dict[str, Any]:
        with self._mutation_lock:
            entry = self._resolve(entry_id, epoch)
            if entry is None:
                return {"ok": False, "error": "Trash entry is stale or no longer exists."}
            state = str(entry.get("state") or "")
            resumable_video = (
                entry.get("entry_type") == "video"
                and state in {"pending", "restoring"}
            )
            if ((state != "complete" and not resumable_video)
                    or not entry.get("can_restore")):
                return {"ok": False,
                        "error": (
                            "Only complete or safely resumable Trash entries "
                            "can be restored."
                        )}
            if cancel_event is not None and cancel_event.is_set():
                return {"ok": False, "cancelled": True, "error": "Restore cancelled."}
            manifest = entry.get("_manifest") or {}
            etype = entry.get("entry_type")
            if etype == "channel_folder":
                return self._restore_channel(entry, manifest, cancel_event)
            if etype == "video":
                return self._restore_video(entry, manifest)
            return {"ok": False, "error": "Unsupported Trash entry type."}

    def _restore_channel(self, entry: dict[str, Any], manifest: dict[str, Any],
                         cancel_event: threading.Event | None) -> dict[str, Any]:
        snapshot = manifest.get("channel_snapshot")
        snapshot = dict(snapshot) if isinstance(snapshot, dict) and snapshot else None
        inferred_url = "" if snapshot else _infer_channel_url(entry["_entry_path"])
        subscription_result = {"ok": True, "added": False,
                               "already_present": False}
        if snapshot is not None:
            subscription_result = subs.restore_channel_snapshot(snapshot)
            if not subscription_result.get("ok"):
                return {"ok": False,
                        "error": subscription_result.get("error") or
                                 "Could not restore the saved subscription.",
                        "files_restored": False,
                        "subscription_restored": False}
        restored = file_ops.restore_trash_entry(
            entry["_entry_path"], archive_root=entry["_root_path"])
        if not restored.get("ok"):
            rollback = {"ok": True, "removed": False}
            if snapshot is not None and subscription_result.get("added"):
                rollback = subs.rollback_restored_channel_snapshot(snapshot)
            error = restored.get("error") or "Folder restore failed."
            if not rollback.get("ok"):
                error += " The subscription rollback also failed: " + str(
                    rollback.get("error") or "unknown error")
            return {"ok": False, "error": error,
                    "files_restored": False,
                    "subscription_restored": bool(
                        snapshot and subscription_result.get("added")
                        and not rollback.get("removed")),
                    "rollback": rollback}

        fresh = load_config() or {}
        channel = snapshot or self._matching_channel_for_path(
            fresh, str(manifest.get("original_path") or ""))
        catalog = None
        warnings: list[str] = []
        if channel is not None and not (
                cancel_event is not None and cancel_event.is_set()):
            catalog = index_maintenance.restore_channel_catalog(
                channel,
                str(manifest.get("original_path") or ""),
                cancel_event=cancel_event,
            )
            if not catalog.get("ok"):
                warnings.append(
                    "Files were restored, but the catalog rebuild did not finish: "
                    + str(catalog.get("error") or "unknown error"))
        elif channel is None:
            warning = (
                "Files were restored, but this older Trash entry did not save "
                "the subscription settings. Add the channel again to resume syncing."
            )
            if inferred_url:
                warning += f" The archived sidecars suggest: {inferred_url}"
            warnings.append(warning)
        cleanup_warning = str(restored.get("cleanup_warning") or "")
        if cleanup_warning:
            warnings.append("Restore cleanup needs attention: " + cleanup_warning)
        catalog_ok = bool(catalog and catalog.get("ok"))
        return {
            "ok": True,
            "status": "completed_with_warnings" if warnings else "completed",
            "entry_id": entry["entry_id"],
            "files_restored": True,
            "subscription_restored": bool(
                subscription_result.get("added")
                or subscription_result.get("already_present")),
            "subscription_present": bool(snapshot or channel),
            "catalog_restored": catalog_ok,
            "needs_subscription": channel is None,
            "needs_rescan": bool(channel is not None and not catalog_ok),
            "inferred_channel_url": inferred_url,
            "catalog": catalog,
            "warnings": warnings,
            "warning": "; ".join(warnings),
            "entry_type": "channel_folder",
        }

    def _restore_video(self, entry: dict[str, Any],
                       manifest: dict[str, Any]) -> dict[str, Any]:
        context = manifest.get("catalog_context")
        context = dict(context) if isinstance(context, dict) else {}
        restored = file_ops.restore_trash_entry(
            entry["_entry_path"], archive_root=entry["_root_path"])
        if not restored.get("ok"):
            return {"ok": False, "error": restored.get("error") or
                    "Video restore failed.", "files_restored": False}
        filepath = str(context.get("filepath") or manifest.get("original_path") or "")
        channel = str(context.get("channel") or "").strip()
        title = str(context.get("title") or "").strip()
        if not channel:
            matched = self._matching_channel_for_path(load_config() or {}, filepath)
            if matched:
                channel = str(matched.get("name") or matched.get("folder") or "")
        catalog_ok = False
        warnings: list[str] = []
        if filepath and channel:
            try:
                catalog_ok = bool(index_backend.register_video(
                    filepath,
                    channel,
                    title or None,
                    video_id=str(context.get("video_id") or "") or None,
                ))
                jsonl_path = os.path.splitext(filepath)[0] + ".jsonl"
                if os.path.isfile(jsonl_path):
                    index_backend.ingest_jsonl(
                        filepath,
                        jsonl_path,
                        title or Path(filepath).stem,
                        channel,
                    )
                index_backend.invalidate_channel_videos(channel)
            except Exception as exc:
                warnings.append("Catalog registration failed: " + str(exc))
        else:
            warnings.append(
                "The file was restored, but this older Trash entry did not "
                "save enough catalog information. Run Rescan to add it to Browse."
            )
        if not catalog_ok and filepath and channel and not warnings:
            warnings.append(
                "The file was restored, but it could not be added to Browse. Run Rescan."
            )
        cleanup_warning = str(restored.get("cleanup_warning") or "")
        if cleanup_warning:
            warnings.append("Restore cleanup needs attention: " + cleanup_warning)
        return {
            "ok": True,
            "status": "completed_with_warnings" if warnings else "completed",
            "entry_id": entry["entry_id"],
            "files_restored": True,
            "catalog_restored": catalog_ok,
            "needs_rescan": not catalog_ok,
            "warnings": warnings,
            "warning": "; ".join(warnings),
            "entry_type": "video",
        }

    def purge(self, entry_id: str, epoch: Any = None, *,
              cfg: dict[str, Any] | None = None,
              cancel_event: threading.Event | None = None,
              automatic_now: float | None = None,
              automatic_retention_days: int = 0,
              automatic_grace_until: float = 0.0,
              _known_entry: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._mutation_lock:
            def _cancelled_result() -> dict[str, Any]:
                return {
                    "ok": False,
                    "cancelled": True,
                    "entry_id": entry_id,
                    "purged": False,
                    "error": "Permanent deletion cancelled.",
                }

            if cancel_event is not None and cancel_event.is_set():
                return _cancelled_result()
            entry = (
                self._refresh_known_entry(_known_entry)
                if isinstance(_known_entry, dict)
                else self._resolve(entry_id, epoch, cfg)
            )
            if entry is None:
                return {"ok": False, "error": "Trash entry is stale or no longer exists."}
            if entry.get("state") != "complete":
                return {"ok": False,
                        "error": "Incomplete Trash entries cannot be permanently deleted."}
            source = entry["_entry_path"]
            if (not _trash_root_is_safe(entry["_root_path"])
                    or not file_ops._is_within_trash_root(
                        source, entry["_root_path"])):
                return {
                    "ok": False,
                    "error": (
                        "Trash containment could not be verified; permanent "
                        "deletion was refused."
                    ),
                }
            already_staged = os.path.basename(source).startswith(_PURGE_PREFIX)
            if int(entry.get("untracked_count") or 0) or (
                    int(entry.get("missing_tracked_count") or 0)
                    and not already_staged):
                return {"ok": False,
                        "error": "Trash contents do not match the manifest; purge was refused."}
            if not entry.get("can_purge"):
                return {"ok": False,
                        "error": "This Trash entry cannot be permanently deleted safely."}
            try:
                from backend.services.channel_transactions import (
                    load_channel_transaction,
                )

                journal = load_channel_transaction(strict=True) or {}
                journal_path = str(journal.get("trashed_folder_path") or "")
                recorded_path = str((entry.get("_manifest") or {}).get(
                    "trashed_folder_path") or "")
                if journal_path and _path_key(journal_path) in {
                        _path_key(source), _path_key(recorded_path)}:
                    return {"ok": False,
                            "error": "This entry belongs to an active channel operation."}
            except Exception as exc:
                _log.warning("Trash purge journal check failed closed: %s", exc)
                return {
                    "ok": False,
                    "retryable": True,
                    "error": (
                        "Channel recovery state could not be verified; "
                        "permanent deletion was refused."
                    ),
                }
            staged = source if already_staged else os.path.join(
                os.path.dirname(source), _PURGE_PREFIX + entry["entry_id"])
            if not already_staged and os.path.exists(staged):
                return {"ok": False,
                        "error": "A prior permanent-delete stage already exists."}
            if cancel_event is not None and cancel_event.is_set():
                return _cancelled_result()
            purge_marker = _purge_marker_path(staged, entry["entry_id"])

            def _remove_purge_marker() -> None:
                recovery_dir = os.path.dirname(purge_marker)
                if not _purge_recovery_dir_is_safe(
                        recovery_dir, entry["_root_path"]):
                    return
                try:
                    os.remove(purge_marker)
                except OSError:
                    pass
                try:
                    os.rmdir(recovery_dir)
                except OSError:
                    pass

            def _stage_entry() -> dict[str, Any] | None:
                recovery_dir = os.path.dirname(purge_marker)
                try:
                    if not _trash_root_is_safe(entry["_root_path"]):
                        raise OSError("Trash root is not safely contained.")
                    os.makedirs(recovery_dir, exist_ok=True)
                    if not _purge_recovery_dir_is_safe(
                            recovery_dir, entry["_root_path"]):
                        raise OSError(
                            "Trash recovery folder is a link, junction, or "
                            "outside the archive.")
                    file_ops._write_json_atomic(purge_marker, {
                        "version": 1,
                        "entry_id": entry["entry_id"],
                        "source_path": os.path.normpath(source),
                        "staged_path": os.path.normpath(staged),
                        "manifest": dict(entry.get("_manifest") or {}),
                    })
                except (OSError, TypeError, ValueError) as exc:
                    return {
                        "ok": False,
                        "retryable": True,
                        "error": (
                            "Could not save permanent-delete recovery state: "
                            f"{exc}"
                        ),
                    }
                if not already_staged and (
                        cancel_event is not None
                        and cancel_event.is_set()):
                    _remove_purge_marker()
                    return _cancelled_result()
                if not already_staged:
                    try:
                        os.rename(source, staged)
                    except OSError as exc:
                        _remove_purge_marker()
                        return {"ok": False, "retryable": True,
                                "error": str(exc)}
                elif cancel_event is not None and cancel_event.is_set():
                    # This entry was staged by an earlier attempt. Keep its
                    # recovery state intact so a later retry can resume.
                    return _cancelled_result()
                return None

            stage_error: dict[str, Any] | None = None
            if automatic_now is None:
                stage_error = _stage_entry()
            else:
                try:
                    # Keep the live Settings policy stable across the final
                    # check and the irreversible same-volume rename. A user
                    # change that commits first wins; one that starts later
                    # applies to the next item/run.
                    with locked_config_snapshot() as commit_cfg:
                        live_days, live_grace = _strict_retention_policy(
                            commit_cfg)
                        effective_days = max(
                            live_days, int(automatic_retention_days or 0))
                        effective_grace = max(
                            live_grace,
                            _as_timestamp(automatic_grace_until),
                        )
                        now_ts = _as_timestamp(automatic_now)
                        removed_at = _automatic_removal_timestamp(
                            entry.get("_manifest") or {}, now_ts=now_ts)
                        roots = {_path_key(path) for path in
                                 configured_archive_roots(commit_cfg)}
                        policy_allows = (
                            file_ops.config_is_writable()
                            and live_days > 0
                            and effective_days > 0
                            and (not effective_grace
                                 or now_ts >= effective_grace)
                            and _path_key(entry["_root_path"]) in roots
                            and 0 < removed_at <= (
                                now_ts - effective_days * 86400)
                        )
                        if not policy_allows:
                            stage_error = {
                                "ok": False,
                                "policy_changed": True,
                                "error": (
                                    "Automatic cleanup policy changed before "
                                    "permanent deletion."
                                ),
                            }
                        else:
                            stage_error = _stage_entry()
                except Exception as exc:
                    stage_error = {
                        "ok": False,
                        "policy_changed": True,
                        "error": (
                            "Automatic cleanup policy could not be verified: "
                            f"{exc}"
                        ),
                    }
            if stage_error is not None:
                return stage_error
            result = file_ops.purge_trash_entry(
                staged, archive_root=entry["_root_path"])
            if result.get("ok"):
                _remove_purge_marker()
            # Once rmtree begins, some files may already be gone.  Never rename
            # that partial directory back to a normal-looking complete entry;
            # leave the clearly staged path for an explicit retry.
            response = {
                "ok": bool(result.get("ok")),
                "entry_id": entry["entry_id"],
                "purged": bool(result.get("ok")),
                "retryable": bool(not result.get("ok") and os.path.isdir(staged)),
                "error": result.get("error", ""),
            }
            if result.get("ok") and entry.get("entry_type") != "channel_folder":
                response["freed_bytes"] = int(entry.get("size_bytes") or 0)
            return response

    def empty(self, root_id: str = "", *,
              cancel_event: threading.Event | None = None) -> dict[str, Any]:
        with self._mutation_lock:
            _roots, entries = self._scan(include_stats=False)
            selected = [entry for entry in entries
                        if not root_id or entry.get("root_id") == root_id]
            purged = 0
            freed_bytes = 0
            freed_bytes_known = True
            skipped: list[dict[str, Any]] = []
            for entry in selected:
                if cancel_event is not None and cancel_event.is_set():
                    return {"ok": False, "cancelled": True, "purged": purged,
                            "skipped": skipped, "error": "Empty Trash cancelled."}
                result = self.purge(
                    entry["entry_id"], entry["epoch"],
                    cancel_event=cancel_event,
                    _known_entry=entry,
                )
                if result.get("cancelled"):
                    return {"ok": False, "cancelled": True,
                            "purged": purged, "skipped": skipped,
                            "error": "Empty Trash cancelled."}
                if result.get("ok"):
                    purged += 1
                    if "freed_bytes" in result:
                        freed_bytes += int(result.get("freed_bytes") or 0)
                    else:
                        freed_bytes_known = False
                else:
                    skipped.append({"entry_id": entry["entry_id"],
                                    "error": result.get("error") or "Purge refused."})
            response = {"ok": not skipped, "purged": purged,
                        "failed": len(skipped), "skipped": skipped,
                        "error": (
                            "Some entries require attention." if skipped else "")}
            if freed_bytes_known:
                response["freed_bytes"] = freed_bytes
            return response

    def open_entry(self, entry_id: str, epoch: Any = None) -> dict[str, Any]:
        entry = self._resolve(entry_id, epoch)
        if entry is None or not os.path.isdir(entry.get("_entry_path", "")):
            return {"ok": False, "error": "Trash entry is stale or unavailable."}
        if not hasattr(os, "startfile"):
            return {"ok": False, "error": "Opening folders is only supported on Windows."}
        try:
            os.startfile(entry["_entry_path"])  # type: ignore[attr-defined]
        except OSError as exc:
            return {"ok": False, "error": str(exc)}
        return {"ok": True, "opened": True, "entry_id": entry["entry_id"]}

    def open_folder(self) -> dict[str, Any]:
        roots = configured_archive_roots()
        if not roots:
            return {"ok": False, "error": "No archive folder is configured."}
        trash_root = next((
            os.path.join(root, ".YTArchiver Trash") for root in roots
            if (os.path.isdir(os.path.join(root, ".YTArchiver Trash"))
                and _trash_root_is_safe(
                    root, os.path.join(root, ".YTArchiver Trash")))
        ), "")
        if not trash_root:
            try:
                trash_root = file_ops.ensure_trash_root(roots[0])
            except OSError as exc:
                return {"ok": False, "error": str(exc)}
        if not hasattr(os, "startfile"):
            return {"ok": False, "error": "Opening folders is only supported on Windows."}
        try:
            os.startfile(trash_root)  # type: ignore[attr-defined]
        except OSError as exc:
            return {"ok": False, "error": str(exc)}
        return {"ok": True, "opened": True}

    def purge_expired(
        self,
        cfg: dict[str, Any],
        now: Any,
        grace_until: Any,
        cancel_event: threading.Event | None,
        retention_days: int | None = None,
    ) -> dict[str, Any]:
        """Purge complete, tracked entries older than configured retention."""
        now_ts = _as_timestamp(now)
        if now_ts <= 0:
            return {"ok": False, "enabled": False, "purged": 0,
                    "skipped": [], "error": "Cleanup time is invalid."}
        try:
            policy_days, policy_grace = _strict_retention_policy(cfg)
        except (TypeError, ValueError) as exc:
            return {"ok": False, "enabled": False, "purged": 0,
                    "skipped": [], "error": str(exc)}
        # The scheduler passes the already-validated values too. A mismatch
        # means the caller and config snapshot disagree; use the more
        # conservative live snapshot instead of widening deletion authority.
        if retention_days is not None:
            try:
                requested_days = int(retention_days)
            except (TypeError, ValueError):
                requested_days = 0
            if requested_days <= 0:
                policy_days = 0
            elif policy_days > 0:
                policy_days = max(policy_days, requested_days)
        grace_ts = max(_as_timestamp(grace_until), policy_grace)
        if policy_days <= 0:
            return {"ok": True, "enabled": False, "purged": 0, "skipped": []}
        if grace_ts and now_ts < grace_ts:
            return {"ok": True, "enabled": True, "grace_active": True,
                    "purged": 0, "skipped": []}
        cutoff = now_ts - policy_days * 86400
        with self._mutation_lock:
            _roots, entries = self._scan(cfg, include_stats=False)
            candidates = []
            for entry in entries:
                manifest = entry.get("_manifest")
                if (not isinstance(manifest, dict)
                        or manifest.get("state") != "complete"):
                    continue
                removed_at = _automatic_removal_timestamp(
                    manifest, now_ts=now_ts)
                if 0 < removed_at <= cutoff:
                    candidates.append(entry)
            purged = 0
            skipped: list[dict[str, Any]] = []
            retryable_failure = False
            for entry in candidates:
                if cancel_event is not None and cancel_event.is_set():
                    return {"ok": False, "cancelled": True, "enabled": True,
                            "purged": purged, "skipped": skipped}
                # Settings can change while a large batch is running. Re-read
                # immediately before every irreversible staged rename so
                # switching to Never, extending retention, or starting a new
                # grace period stops further deletion in this same pass.
                try:
                    live_cfg = load_config() or {}
                    live_days, live_grace = _strict_retention_policy(live_cfg)
                except Exception as exc:
                    return {
                        "ok": False,
                        "enabled": True,
                        "purged": purged,
                        "skipped": skipped,
                        "error": (
                            "Trash cleanup stopped because the live policy "
                            f"could not be verified: {exc}"
                        ),
                    }
                if not file_ops.config_is_writable():
                    return {
                        "ok": False,
                        "enabled": True,
                        "purged": purged,
                        "skipped": skipped,
                        "error": "Trash cleanup stopped while settings are read-only.",
                    }
                if live_days <= 0:
                    return {"ok": True, "enabled": False,
                            "policy_changed": True, "purged": purged,
                            "skipped": skipped}
                if live_grace and now_ts < live_grace:
                    return {"ok": True, "enabled": True,
                            "grace_active": True, "policy_changed": True,
                            "purged": purged, "skipped": skipped}
                current = self._refresh_known_entry(entry)
                if current is None:
                    skipped.append({"entry_id": entry["entry_id"],
                                    "error": "Entry changed before cleanup."})
                    continue
                manifest = current.get("_manifest")
                removed_at = (
                    _automatic_removal_timestamp(manifest, now_ts=now_ts)
                    if isinstance(manifest, dict)
                    and manifest.get("state") == "complete"
                    else 0.0
                )
                live_cutoff = now_ts - live_days * 86400
                if not (0 < removed_at <= live_cutoff):
                    return {
                        "ok": True,
                        "enabled": True,
                        "policy_changed": True,
                        "purged": purged,
                        "skipped": skipped,
                    }
                result = self.purge(
                    current["entry_id"], current["epoch"], cfg=live_cfg,
                    cancel_event=cancel_event,
                    automatic_now=now_ts,
                    automatic_retention_days=policy_days,
                    automatic_grace_until=grace_ts,
                    _known_entry=current)
                if result.get("ok"):
                    purged += 1
                elif result.get("policy_changed"):
                    return {
                        "ok": True,
                        "enabled": True,
                        "policy_changed": True,
                        "purged": purged,
                        "skipped": skipped,
                    }
                elif result.get("cancelled"):
                    return {
                        "ok": False,
                        "cancelled": True,
                        "enabled": True,
                        "purged": purged,
                        "skipped": skipped,
                    }
                else:
                    skipped.append({"entry_id": entry["entry_id"],
                                    "error": result.get("error") or "Purge refused.",
                                    "retryable": bool(result.get("retryable"))})
                    retryable_failure = (
                        retryable_failure or bool(result.get("retryable")))
            return {"ok": not retryable_failure, "enabled": True,
                    "purged": purged, "skipped": skipped,
                    "warnings": skipped}


trash_manager = TrashManager()


def purge_expired(*, config: dict[str, Any], retention_days: int,
                  grace_until_ts: Any, cancel_event: threading.Event | None,
                  now: Any) -> dict[str, Any]:
    """Scheduler-friendly module-level retention entry point."""
    return trash_manager.purge_expired(
        config,
        now,
        grace_until_ts,
        cancel_event,
        retention_days=retention_days,
    )
