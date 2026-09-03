"""Managed filesystem operations.

Destructive app actions should come through this layer so path containment,
config writability, and sidecar cleanup rules stay consistent.
"""

from __future__ import annotations

import errno
import glob
import hashlib
import json
import os
import shutil
import sys
import time
import uuid
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from backend.fs_safety import _file_has_hidden_attribute
from backend.fs_search import MEDIA_EXTS_TUPLE
from backend.utils import (
    delete_video_sidecars,
    hide_file_win,
    is_within_managed_roots,
    unhide_file_win,
)
from backend.ytarchiver_config import config_is_writable

_RESTORE_RECOVERY_DIR = ".ytarchiver-restore-recovery"
_TRASH_MANIFEST_VERSION = 2


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
    while (os.path.exists(candidate)
           or os.path.exists(_restore_cleanup_marker_path(candidate))):
        suffix += 1
        candidate = os.path.join(trash_root, f"{stamp}-{base}-{suffix}")
    return candidate


def ensure_trash_root(archive_root: str) -> str:
    """Create the app-owned Trash directory and keep it hidden on Windows.

    Centralizing this matters because channel-folder trash used to create the
    directory with a bare ``os.makedirs`` call.  A fresh archive could
    therefore expose the implementation folder alongside the user's channels.
    """
    trash_root = os.path.join(archive_root, ".YTArchiver Trash")
    os.makedirs(trash_root, exist_ok=True)
    # A symlink/junction at this exact name would turn an apparently-contained
    # purge into a recursive delete somewhere else.  The archive root itself
    # may legitimately be a mounted/pool path; the app-owned child may not be
    # a redirect of any kind.
    is_junction = getattr(os.path, "isjunction", lambda _path: False)
    if os.path.islink(trash_root) or is_junction(trash_root):
        raise OSError(
            "YTArchiver Trash is a link or junction; refusing to use it.")
    try:
        real_archive_root = os.path.normcase(os.path.realpath(archive_root))
        real_trash_root = os.path.normcase(os.path.realpath(trash_root))
        contained = (
            real_trash_root != real_archive_root
            and os.path.commonpath(
                [real_trash_root, real_archive_root]) == real_archive_root
        )
    except (OSError, TypeError, ValueError):
        contained = False
    if not contained:
        raise OSError(
            "YTArchiver Trash resolves outside the configured archive root.")
    try:
        hide_file_win(trash_root)
    except OSError:
        # Hiding is presentation-only.  Never turn an otherwise recoverable
        # quarantine operation into a destructive failure because the archive
        # filesystem does not support Windows attributes.
        pass
    return trash_root


def channel_trash_destination(
    folder_path: str,
    archive_root: str,
    transaction_id: str,
) -> str:
    """Return the one deterministic trash path reserved by a transaction.

    Unlike the timestamp allocator used by standalone trash actions, this
    path can be written to the outer folder/config journal *before* the
    directory moves.  Startup recovery can therefore locate the exact entry
    even if the process stops between the move and its next checkpoint.
    """
    token = str(transaction_id or "").strip().lower()
    if len(token) != 32 or any(char not in "0123456789abcdef" for char in token):
        raise ValueError("Channel trash transaction id is invalid.")
    base = os.path.basename(os.path.normpath(folder_path)) or "channel"
    base = base.replace(os.sep, "_").replace("/", "_").replace("\\", "_")
    trash_root = os.path.join(archive_root, ".YTArchiver Trash")
    return os.path.abspath(os.path.join(trash_root, f"{token}-{base}"))


def _allocate_trash_folder(folder_path: str, archive_root: str) -> str:
    """Atomically create and return a collision-free trash folder."""
    ensure_trash_root(archive_root)
    while True:
        candidate = _trash_path_for(folder_path, archive_root)
        try:
            os.makedirs(candidate, exist_ok=False)
        except FileExistsError:
            # Another operation claimed the candidate after _trash_path_for's
            # existence check. Recompute a suffixed path and try again; the
            # colliding folder belongs to that other operation.
            continue
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
            error=("Settings are temporarily read-only, so no files were "
                   "deleted. Restart YTArchiver and try again."),
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


def _has_other_live_same_stem_media(video_path: str) -> bool:
    """Fail closed when another media file shares this sidecar stem."""
    selected_key = _resolved_path_key(video_path)
    base = os.path.splitext(os.path.abspath(video_path))[0]
    base_key = os.path.normcase(os.path.normpath(base))
    parent = os.path.dirname(base)
    try:
        with os.scandir(parent) as entries:
            for entry in entries:
                candidate_stem = os.path.splitext(
                    os.path.abspath(entry.path))[0]
                if (os.path.normcase(os.path.normpath(candidate_stem))
                        != base_key):
                    continue
                try:
                    is_media = (
                        (entry.is_file(follow_symlinks=False)
                         or entry.is_symlink())
                        and os.path.splitext(entry.name)[1].lower()
                        in MEDIA_EXTS_TUPLE
                    )
                except OSError:
                    # An inaccessible same-stem candidate cannot be proven
                    # absent, so leave the sidecars in place.
                    return True
                if not is_media:
                    continue
                candidate_key = _resolved_path_key(entry.path)
                if not candidate_key or candidate_key != selected_key:
                    return True
    except OSError:
        # If the directory cannot be enumerated, moving an ambiguously owned
        # sidecar is less safe than leaving it beside the selected media.
        return True
    return False


def _files_equal(first: str, second: str) -> bool:
    """Compare two files without loading transcript sidecars into memory."""
    try:
        if os.path.getsize(first) != os.path.getsize(second):
            return False
        with open(first, "rb") as left, open(second, "rb") as right:
            while True:
                left_chunk = left.read(1024 * 1024)
                right_chunk = right.read(1024 * 1024)
                if left_chunk != right_chunk:
                    return False
                if not left_chunk:
                    return True
    except OSError:
        return False


def _resolved_path_key(path: Any) -> str:
    """Return an exact comparison key without trusting textual aliases."""
    try:
        raw_path = os.fspath(path)
        if not isinstance(raw_path, str) or not raw_path:
            return ""
        return os.path.normcase(
            os.path.realpath(os.path.abspath(raw_path)))
    except (TypeError, ValueError, OSError):
        return ""


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _sidecar_handoff_marker(destination: str, token: str) -> str:
    parent = os.path.dirname(destination)
    basename = os.path.basename(destination)
    return os.path.join(parent, f".{basename}.handoff-{token}.json")


def _sidecar_handoff_value(
    destination: str,
    source: str,
    token: str,
    digest: str,
    *,
    state: str,
) -> dict[str, str]:
    return {
        "kind": "ytarchiver-sidecar-handoff-v1",
        "token": token,
        "destination": os.path.normpath(destination),
        "source": os.path.normpath(source),
        "sha256": digest,
        "state": state,
    }


def _read_sidecar_handoff_marker(marker_path: str) -> dict[str, Any] | None:
    try:
        with open(marker_path, "r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _valid_sidecar_cleanup_token(cleanup_token: Any) -> dict[str, str] | None:
    if not isinstance(cleanup_token, dict):
        return None
    token = str(cleanup_token.get("token") or "")
    marker_path = os.path.normpath(
        str(cleanup_token.get("marker_path") or ""))
    destination = os.path.normpath(
        str(cleanup_token.get("destination") or ""))
    source = os.path.normpath(str(cleanup_token.get("source") or ""))
    digest = str(cleanup_token.get("sha256") or "")
    if (not token or not marker_path or not destination or not source
            or len(digest) != 64):
        return None
    if marker_path != _sidecar_handoff_marker(destination, token):
        return None
    return {
        "token": token,
        "marker_path": marker_path,
        "destination": destination,
        "source": source,
        "sha256": digest,
    }


def _orphaned_sidecar_handoff(
        destination: str, source: str,
        digest: str) -> dict[str, str] | None:
    """Find an interrupted handoff that owns an identical destination."""
    pattern = os.path.join(
        os.path.dirname(destination),
        f".{glob.escape(os.path.basename(destination))}.handoff-*.json",
    )
    for marker_path in sorted(glob.glob(pattern)):
        marker = _read_sidecar_handoff_marker(marker_path)
        if not marker:
            continue
        token = str(marker.get("token") or "")
        marker_source = os.path.normpath(str(marker.get("source") or ""))
        cleanup = {
            "token": token,
            "marker_path": os.path.normpath(marker_path),
            "destination": os.path.normpath(destination),
            "source": marker_source,
            "sha256": digest,
        }
        valid = _valid_sidecar_cleanup_token(cleanup)
        if valid is None:
            continue
        expected = _sidecar_handoff_value(
            destination, marker_source, token, digest,
            state=str(marker.get("state") or ""),
        )
        if any(marker.get(key) != value for key, value in expected.items()):
            continue
        if marker.get("state") == "committed":
            try:
                os.remove(marker_path)
            except OSError:
                pass
            continue
        if os.path.normcase(marker_source) != os.path.normcase(
                os.path.normpath(source)):
            continue
        # A prepared marker records intent only.  The destination may have
        # been created by another concurrent handoff, so it never proves
        # deletion ownership even when the bytes happen to match.
        if marker.get("state") != "published":
            continue
        try:
            if _sha256_file(destination) == digest:
                return valid
        except OSError:
            continue
    return None


def rollback_preserved_sidecar(cleanup_token: Any) -> dict[str, Any]:
    """Remove only a sidecar proven to have been created by our handoff.

    The durable marker and content digest are both required before deleting a
    published destination.  A repeated rollback is harmless; a missing marker
    never authorizes removal of an existing file.
    """
    cleanup = _valid_sidecar_cleanup_token(cleanup_token)
    if cleanup is None:
        return _result(
            False,
            error="YTArchiver could not verify the pending transcript cleanup.",
            technical_detail="Invalid sidecar cleanup token.",
        )
    marker_path = cleanup["marker_path"]
    destination = cleanup["destination"]
    marker = _read_sidecar_handoff_marker(marker_path)
    if marker is None:
        if not os.path.exists(destination):
            return _result(True, already_clean=True)
        return _result(
            False,
            error=("YTArchiver could not verify the saved transcript, so "
                   "nothing was deleted."),
            technical_detail="Sidecar handoff marker is missing.",
        )
    if not os.path.isfile(cleanup["source"]):
        finalized = finalize_preserved_sidecar(cleanup)
        return _result(
            False,
            error=("The original video is no longer available, so the saved "
                   "transcript was kept."),
            preserved=True,
            marker_finalized=bool(finalized.get("ok")),
        )
    expected = _sidecar_handoff_value(
        destination, cleanup["source"], cleanup["token"], cleanup["sha256"],
        state=str(marker.get("state") or ""),
    )
    if (marker.get("state") != "published"
            or any(marker.get(key) != value
                   for key, value in expected.items())):
        return _result(
            False,
            error=("YTArchiver could not verify the saved transcript, so it "
                   "was kept."),
            technical_detail=("Sidecar handoff marker is not rollback-eligible "
                              "or does not match the cleanup token."),
        )
    try:
        if os.path.exists(destination):
            if _sha256_file(destination) != cleanup["sha256"]:
                return _result(
                    False,
                    error=("The saved transcript changed after it was copied, "
                           "so it was kept."),
                )
            os.remove(destination)
        try:
            os.remove(marker_path)
        except FileNotFoundError:
            pass
    except OSError as exc:
        return _result(
            False,
            error=f"YTArchiver could not finish undoing transcript cleanup: {exc}",
            technical_detail=f"Could not roll back sidecar handoff: {exc}",
        )
    return _result(True, removed=True)


def finalize_preserved_sidecar(cleanup_token: Any) -> dict[str, Any]:
    """Commit one prepared handoff by removing its ownership marker only."""
    cleanup = _valid_sidecar_cleanup_token(cleanup_token)
    if cleanup is None:
        return _result(
            False,
            error="YTArchiver could not verify the pending transcript cleanup.",
            technical_detail="Invalid sidecar cleanup token.",
        )
    marker_path = cleanup["marker_path"]
    marker = _read_sidecar_handoff_marker(marker_path)
    if marker is None:
        return _result(True, already_finalized=True)
    expected = _sidecar_handoff_value(
        cleanup["destination"], cleanup["source"], cleanup["token"],
        cleanup["sha256"],
        state=str(marker.get("state") or ""),
    )
    if any(marker.get(key) != value for key, value in expected.items()):
        return _result(
            False,
            error="YTArchiver could not verify the completed transcript cleanup.",
            technical_detail=("Sidecar handoff marker does not match the "
                              "cleanup token."),
        )
    try:
        _write_json_atomic(
            marker_path,
            _sidecar_handoff_value(
                cleanup["destination"], cleanup["source"], cleanup["token"],
                cleanup["sha256"], state="committed"),
        )
        hide_file_win(marker_path)
        os.remove(marker_path)
    except FileNotFoundError:
        pass
    except OSError as exc:
        return _result(
            False,
            error=f"YTArchiver could not finish transcript cleanup: {exc}",
            technical_detail=f"Could not finalize sidecar handoff: {exc}",
        )
    return _result(True)


def _registered_video_identity_path(identity: Any) -> str:
    if not isinstance(identity, dict):
        return ""
    video_path = os.path.normpath(str(identity.get("filepath") or ""))
    if not video_path:
        return ""
    try:
        from backend import index as index_backend
        if index_backend.is_registered_video_identity(identity):
            return video_path
    except Exception:
        pass
    return ""


def _available_registered_video(identity: Any) -> str:
    video_path = _registered_video_identity_path(identity)
    return video_path if video_path and os.path.isfile(video_path) else ""


def authorize_sidecar_handoff_staging_directory(
        directory: str, *, registered_video_identity: Any = None
        ) -> dict[str, Any]:
    """Authorize where an internal derived-JSONL temporary may be created."""
    staging_dir = os.path.normpath(directory or "")
    if not staging_dir:
        return _result(False, error="Missing transcript staging folder.")
    if registered_video_identity is not None:
        # A stale primary may already be missing while its exact catalog row
        # and transcript still need handing off.  Staging authorization is
        # about the catalog identity and directory; only the destination
        # survivor must still have an available media file.
        video_path = _registered_video_identity_path(
            registered_video_identity)
        if (not video_path
                or _resolved_path_key(os.path.dirname(video_path))
                != _resolved_path_key(staging_dir)):
            return _result(
                False,
                error=("YTArchiver cannot safely create transcript recovery "
                       "data because the selected source video changed."),
            )
        return _result(True, authorization="registered_video")
    if _managed_root_for(os.path.join(staging_dir, ".ytarchiver-staging")):
        return _result(True, authorization="managed_root")
    return _result(
        False,
        error=("YTArchiver cannot safely create transcript recovery data "
               "beside this unregistered video."),
    )


def _authorized_derived_sidecar_source(
        source: str, registered_video_identity: Any) -> bool:
    guard = authorize_sidecar_handoff_staging_directory(
        os.path.dirname(source),
        registered_video_identity=registered_video_identity,
    )
    if not guard.get("ok"):
        return False
    name = os.path.basename(source)
    if not name.startswith(".") or ".derive-" not in name:
        return False
    token = name.rsplit(".derive-", 1)[-1]
    try:
        return len(token) == 32 and uuid.UUID(hex=token).hex == token.lower()
    except (ValueError, AttributeError):
        return False


def authorize_sidecar_handoff_destination(
        destination: str, *, registered_video_identity: Any = None
        ) -> dict[str, Any]:
    """Authorize one transcript-sidecar destination before any file is made.

    Most destinations must live under a configured archive root.  Manual
    downloads are the narrow exception: the user may choose any Save-to
    folder, and the exact resulting video path is recorded in the catalog.
    For that case, allow only the normal sibling JSONL path for an existing,
    exact catalog row.  A caller-supplied arbitrary path is not sufficient.
    """
    destination = os.path.normpath(destination or "")
    if not destination:
        return _result(
            False,
            error="Transcript cleanup information is incomplete.",
            technical_detail="Missing sidecar handoff destination.",
        )
    video_path = ""
    if registered_video_identity is not None:
        video_path = _available_registered_video(registered_video_identity)
        if not video_path:
            return _result(
                False,
                error=("The surviving video no longer matches the exact "
                       "registered download selected for transcript recovery."),
            )
    elif _managed_root_for(destination):
        return _result(True, authorization="managed_root")
    else:
        return _result(
            False,
            error=("The surviving video is outside the archive and is not an "
                   "available registered download."),
        )
    stem = os.path.splitext(video_path)[0]
    parent, basename = os.path.split(stem)
    allowed = {
        _resolved_path_key(stem + ".jsonl"),
        _resolved_path_key(os.path.join(parent, "." + basename + ".jsonl")),
    }
    if not allowed or _resolved_path_key(destination) not in allowed:
        return _result(
            False,
            error=("The transcript destination does not belong to the exact "
                   "registered surviving video."),
        )
    return _result(
        True,
        authorization="registered_video",
        registered_video_path=video_path,
    )


def preserve_sidecar_no_overwrite(
        source: str, destination: str, *,
        source_identity: str | None = None,
        registered_destination_identity: Any = None,
        registered_source_identity: Any = None) -> dict[str, Any]:
    """Durably publish a sidecar at *destination* without overwriting.

    The source is copied and fsynced to a same-directory temporary file, then
    atomically published without replacement.  A pre-existing identical
    destination is accepted; conflicting content fails closed.  Windows uses
    no-replace ``os.rename``; other platforms use ``os.link`` so a concurrent
    destination can never be overwritten.
    """
    source = os.path.normpath(source or "")
    destination = os.path.normpath(destination or "")
    source_identity = os.path.normpath(source_identity or source)
    if not source or not destination:
        return _result(
            False,
            error="Transcript cleanup information is incomplete.",
            technical_detail="Missing sidecar handoff path.",
        )
    if os.path.normcase(source) == os.path.normcase(destination):
        return _result(True, path=destination, existing=True)
    destination_guard = authorize_sidecar_handoff_destination(
        destination,
        registered_video_identity=registered_destination_identity,
    )
    source_authorized = (
        _authorized_derived_sidecar_source(
            source, registered_source_identity)
        if registered_source_identity is not None
        else bool(_managed_root_for(source))
    )
    if not source_authorized:
        return _result(
            False,
            error=("YTArchiver could not safely preserve the transcript, so "
                   "nothing was moved to Trash."),
            technical_detail="Sidecar handoff source is outside the archive.",
        )
    if not destination_guard.get("ok"):
        return _result(
            False,
            error=destination_guard.get("error") or (
                "The surviving video is not an authorized transcript "
                "destination."),
        )
    if not os.path.isfile(source):
        return _result(False,
                       error="The transcript details to preserve could not be found.")
    try:
        source_digest = _sha256_file(source)
    except OSError as exc:
        return _result(False,
                       error=f"Could not read the saved transcript details: {exc}")
    if os.path.exists(destination):
        if _files_equal(source, destination):
            orphan = _orphaned_sidecar_handoff(
                destination, source_identity, source_digest)
            if orphan is not None:
                return _result(
                    True,
                    path=destination,
                    existing=False,
                    created=True,
                    recovered=True,
                    cleanup_token=orphan,
                )
            return _result(True, path=destination, existing=True)
        return _result(
            False,
            error=("The surviving video already has different transcript "
                   "details."),
        )

    parent = os.path.dirname(destination)
    token = uuid.uuid4().hex
    marker_path = _sidecar_handoff_marker(destination, token)
    temp_path = os.path.join(
        parent, f".{os.path.basename(destination)}.handoff-{uuid.uuid4().hex}")
    cleanup_token: dict[str, str] | None = None
    published = False
    try:
        os.makedirs(parent, exist_ok=True)
        digest = source_digest
        cleanup_token = {
            "token": token,
            "marker_path": marker_path,
            "destination": destination,
            "source": source_identity,
            "sha256": digest,
        }
        _write_json_atomic(
            marker_path,
            _sidecar_handoff_value(
                destination, source_identity, token, digest, state="prepared"),
        )
        hide_file_win(marker_path)
        with open(source, "rb") as src, open(temp_path, "xb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
            dst.flush()
            os.fsync(dst.fileno())

        # The exact survivor/source rows can change while a large transcript
        # is being copied.  Revalidate immediately before publication so an
        # old transcript can never be attached to a replacement catalog row
        # that reused the same path.
        destination_recheck = authorize_sidecar_handoff_destination(
            destination,
            registered_video_identity=registered_destination_identity,
        )
        source_rechecked = (
            _authorized_derived_sidecar_source(
                source, registered_source_identity)
            if registered_source_identity is not None
            else bool(_managed_root_for(source))
        )
        if not destination_recheck.get("ok") or not source_rechecked:
            try:
                os.remove(marker_path)
            except OSError:
                pass
            return _result(
                False,
                error=(destination_recheck.get("error")
                       if not destination_recheck.get("ok")
                       else "Transcript recovery source changed before publish."),
            )
        try:
            if os.name == "nt":
                # MoveFileW-backed rename is atomic and refuses to replace an
                # existing destination.  It also works on pooled Windows
                # volumes that may not expose hard-link support.
                os.rename(temp_path, destination)
            else:
                os.link(temp_path, destination)
            published = True
        except FileExistsError:
            if not _files_equal(source, destination):
                return _result(
                    False,
                    error=("The surviving video already has different "
                           "transcript details."),
                )
            try:
                os.remove(marker_path)
            except OSError:
                pass
            return _result(True, path=destination, existing=True, created=False)
        if _sha256_file(destination) != digest:
            raise OSError("published sidecar verification failed")
        hide_file_win(destination)
        _write_json_atomic(
            marker_path,
            _sidecar_handoff_value(
                destination, source_identity, token, digest, state="published"),
        )
        hide_file_win(marker_path)
        return _result(
            True,
            path=destination,
            existing=False,
            created=True,
            cleanup_token=cleanup_token,
        )
    except OSError as exc:
        if cleanup_token is not None and (published or os.path.exists(marker_path)):
            rollback_preserved_sidecar(cleanup_token)
        return _result(False,
                       error=f"Could not preserve the transcript details: {exc}")
    finally:
        try:
            os.remove(temp_path)
        except OSError:
            pass


def _unique_child_path(parent: str, basename: str) -> str:
    candidate = os.path.join(parent, basename)
    stem, ext = os.path.splitext(basename)
    suffix = 1
    while os.path.exists(candidate):
        suffix += 1
        candidate = os.path.join(parent, f"{stem}-{suffix}{ext}")
    return candidate


def _write_json_atomic(path: str, value: dict[str, Any]) -> str:
    """Atomically publish and flush one JSON object at *path*."""
    tmp_path = f"{path}.tmp-{uuid.uuid4().hex}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(value, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
    return path


def _write_trash_manifest(folder: str, manifest: dict[str, Any]) -> str:
    """Atomically publish a trash manifest inside *folder*."""
    manifest_path = os.path.join(folder, ".ytarchiver-trash.json")
    return _write_json_atomic(manifest_path, manifest)


def _restore_original_hidden_state(path: str, original_hidden: Any) -> None:
    """Restore a manifest entry's recorded Windows hidden state exactly.

    Older manifests have no ``original_hidden`` field.  Leave those files
    untouched rather than guessing from their role or filename.
    """
    if original_hidden is True:
        hide_file_win(path)
    elif original_hidden is False:
        unhide_file_win(path)


def _restore_cleanup_marker_path(trashed_folder_path: str) -> str:
    """Return the outside-the-entry marker used during final cleanup."""
    folder = os.path.normpath(trashed_folder_path)
    trash_root = os.path.dirname(folder)
    return os.path.join(
        trash_root,
        _RESTORE_RECOVERY_DIR,
        f"{os.path.basename(folder)}.json",
    )


def _write_restore_cleanup_marker(
    trashed_folder_path: str,
    manifest: dict[str, Any],
    *,
    archive_root: str = "",
) -> str:
    """Publish recovery metadata outside a folder before its manifest moves."""
    marker_path = _restore_cleanup_marker_path(trashed_folder_path)
    recovery_dir = os.path.dirname(marker_path)
    if not _restore_recovery_dir_is_safe(
            trashed_folder_path, archive_root=archive_root):
        raise OSError("Trash restore recovery folder is not safely contained.")
    os.makedirs(recovery_dir, exist_ok=True)
    if not _restore_recovery_dir_is_safe(
            trashed_folder_path, archive_root=archive_root):
        raise OSError(
            "Trash restore recovery folder is a link, junction, or outside "
            "the archive.")
    return _write_json_atomic(marker_path, manifest)


def _is_strictly_within(path: str, root: str) -> bool:
    """True when *path* resolves below (not equal to) *root*."""
    try:
        target = os.path.normcase(os.path.realpath(path))
        real_root = os.path.normcase(os.path.realpath(root))
        return (target != real_root
                and os.path.commonpath([target, real_root]) == real_root)
    except (TypeError, ValueError, OSError):
        return False


def _move_no_replace(source: str, destination: str) -> None:
    """Atomically move within one archive volume without replacing *destination*.

    YTArchiver is deployed on Windows, where ``os.rename`` maps to a
    no-replace move.  Linux's ``renameat2(RENAME_NOREPLACE)`` provides the
    equivalent behavior for development and CI.  Unsupported directory moves
    fail closed instead of falling back to ``shutil.move``'s nesting rules.
    """
    if os.name == "nt":
        os.rename(source, destination)
        return

    if sys.platform.startswith("linux"):
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = getattr(libc, "renameat2", None)
        if renameat2 is not None:
            renameat2.argtypes = (
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_uint,
            )
            renameat2.restype = ctypes.c_int
            if renameat2(
                -100,  # AT_FDCWD
                os.fsencode(source),
                -100,
                os.fsencode(destination),
                1,  # RENAME_NOREPLACE
            ) == 0:
                return
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error), destination)

    # A hard-link publication is an atomic no-replace move for regular files.
    # If unlinking the source fails, keeping both paths is recoverable and
    # safer than deleting either copy.
    if os.path.isfile(source) and not os.path.islink(source):
        os.link(source, destination)
        os.unlink(source)
        return
    raise OSError(
        errno.ENOTSUP,
        "Atomic no-replace directory moves are unsupported on this platform",
        destination,
    )


def safe_trash_video_file(
    video_path: str,
    *,
    require_config_writable: bool = True,
    reason: str = "",
    unhide_first: bool = False,
    excluded_sidecar_paths: Iterable[str | os.PathLike[str]] | None = None,
    catalog_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Move one managed video and its unshared sidecars to quarantine.

    ``excluded_sidecar_paths`` is an exact-path allowlist for sidecars that
    another live media copy still owns.  Canonical comparison keys prevent a
    relative path, separator difference, symlink, or case alias from bypassing
    the exclusion while leaving all existing callers backward-compatible.
    """
    guard = assert_within_managed_roots(video_path)
    if not guard.get("ok"):
        return guard
    if require_config_writable and not config_is_writable():
        return _result(
            False,
            error=("Settings are temporarily read-only, so no files were "
                   "moved to Trash. Restart YTArchiver and try again."),
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

    raw_exclusions: Iterable[str | os.PathLike[str]]
    if isinstance(excluded_sidecar_paths, (str, os.PathLike)):
        raw_exclusions = (excluded_sidecar_paths,)
    else:
        raw_exclusions = excluded_sidecar_paths or ()
    excluded_sidecar_keys = {
        key for path in raw_exclusions
        if (key := _resolved_path_key(path))
    }

    moved_files: list[dict[str, Any]] = []
    trash_folder = ""
    trash_folder_owned = False
    try:
        trash_folder = _allocate_trash_folder(video_path, archive_root)
        trash_folder_owned = True
        planned_files: list[dict[str, Any]] = []
        reserved: set[str] = set()

        def _reserve_destination(source: str) -> str:
            candidate = _unique_child_path(
                trash_folder, os.path.basename(source))
            stem, ext = os.path.splitext(candidate)
            suffix = 1
            key = os.path.normcase(candidate)
            while key in reserved:
                suffix += 1
                candidate = f"{stem}-{suffix}{ext}"
                key = os.path.normcase(candidate)
            reserved.add(key)
            return candidate

        trashed_video = _reserve_destination(video_path)
        planned_files.append({
            "role": "video",
            "original_path": os.path.normpath(video_path),
            "trashed_path": os.path.normpath(trashed_video),
            "source_size": os.stat(video_path).st_size,
            "original_hidden": _file_has_hidden_attribute(video_path),
        })
        shared_stem = _has_other_live_same_stem_media(video_path)
        for sidecar in _video_sidecar_paths(video_path):
            if (shared_stem
                    or _resolved_path_key(sidecar) in excluded_sidecar_keys):
                continue
            planned_files.append({
                "role": "sidecar",
                "original_path": os.path.normpath(sidecar),
                "trashed_path": os.path.normpath(
                    _reserve_destination(sidecar)),
                "source_size": os.stat(sidecar).st_size,
                "original_hidden": _file_has_hidden_attribute(sidecar),
            })

        manifest = {
            "version": _TRASH_MANIFEST_VERSION,
            "entry_id": uuid.uuid4().hex,
            "epoch": int(time.time()),
            "entry_type": "video",
            "state": "pending",
            "archive_root": os.path.normpath(archive_root),
            "original_path": os.path.normpath(video_path),
            "trashed_path": os.path.normpath(trashed_video),
            "trashed_folder_path": os.path.normpath(trash_folder),
            "trashed_at": datetime.now().isoformat(timespec="seconds"),
            "reason": reason,
            "files": planned_files,
        }
        if isinstance(catalog_context, dict) and catalog_context:
            # This is deliberately a compact catalog identity, not a database
            # dump.  It is enough to recreate the physical row after restore.
            manifest["catalog_context"] = dict(catalog_context)
        # Publish recovery intent before the first source is moved. A process
        # interruption can now always be discovered and reversed.
        _write_trash_manifest(trash_folder, manifest)
        for entry in planned_files:
            source = entry["original_path"]
            destination = entry["trashed_path"]
            if entry["role"] == "video" and unhide_first:
                unhide_file_win(source)
            elif entry["role"] == "sidecar":
                unhide_file_win(source)
            # Record the in-flight move before calling into shutil.  A
            # cross-volume move can copy the destination and then raise while
            # removing the source; rollback must still know about that file.
            moved_files.append(dict(entry))
            shutil.move(source, destination)
        manifest["state"] = "complete"
        _write_trash_manifest(trash_folder, manifest)
    except OSError as exc:
        rollback_failed: list[str] = []
        for entry in reversed(moved_files):
            src = entry.get("trashed_path", "")
            dest = entry.get("original_path", "")
            try:
                src_exists = bool(src and os.path.exists(src))
                dest_exists = bool(dest and os.path.exists(dest))
                if src_exists and dest_exists:
                    # A cross-volume shutil.move can leave the complete
                    # source in place plus a partial destination before it
                    # raises.  The original is authoritative; never move the
                    # possibly partial trash copy over it.  Keep both and the
                    # recovery manifest for explicit inspection/purge.
                    rollback_failed.append(
                        f"kept original and quarantined conflicting copy: {src}")
                elif src_exists and dest:
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    shutil.move(src, dest)
                if dest and os.path.isfile(dest):
                    _restore_original_hidden_state(
                        dest, entry.get("original_hidden"))
            except OSError as rollback_exc:
                rollback_failed.append(str(rollback_exc))
        if not rollback_failed:
            try:
                if trash_folder_owned and os.path.isdir(trash_folder):
                    shutil.rmtree(trash_folder)
            except OSError:
                pass
        return _result(
            False,
            error=str(exc),
            path=os.path.normpath(video_path),
            reason=reason,
            rollback_failed=rollback_failed,
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


def _restore_recovery_dir_is_safe(
    trashed_folder_path: str,
    *,
    archive_root: str = "",
) -> bool:
    folder = os.path.normpath(trashed_folder_path)
    root = os.path.normpath(
        archive_root or os.path.dirname(os.path.dirname(folder)))
    recovery_dir = os.path.dirname(_restore_cleanup_marker_path(folder))
    is_junction = getattr(os.path, "isjunction", lambda _path: False)
    try:
        return (
            bool(root)
            and _is_within_trash_root(folder, root)
            and not os.path.islink(recovery_dir)
            and not is_junction(recovery_dir)
            and _is_within_trash_root(recovery_dir, root)
        )
    except (OSError, TypeError, ValueError):
        return False


def _read_trash_manifest(
    trashed_folder_path: str,
    *,
    archive_root: str = "",
):
    inside_path = os.path.join(
        trashed_folder_path, ".ytarchiver-trash.json")
    recovery_path = _restore_cleanup_marker_path(trashed_folder_path)
    manifest_paths = [inside_path]
    if _restore_recovery_dir_is_safe(
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
        if name == _RESTORE_RECOVERY_DIR or not os.path.isdir(folder):
            continue
        manifest, _ = _read_trash_manifest(folder, archive_root=archive_root)
        if manifest is None:
            continue
        files = manifest.get("files")
        entry_type = manifest.get("entry_type") or (
            "video" if isinstance(files, list) else "channel_folder")
        entries.append({
            "trashed_folder_path": os.path.normpath(folder),
            "original_path": manifest.get("original_path", ""),
            "trashed_at": manifest.get("trashed_at", ""),
            "reason": manifest.get("reason", ""),
            "entry_type": entry_type,
            "state": manifest.get("state", "complete"),
            "file_count": len(files) if isinstance(files, list) else 1,
        })
    entries.sort(key=lambda e: e.get("trashed_at", ""), reverse=True)
    return _result(True, entries=entries)


def restore_trash_entry(trashed_folder_path: str, *,
                        archive_root: str = "",
                        require_config_writable: bool = True,
                        expected_transaction_id: str = "",
                        ) -> dict[str, Any]:
    """Move a quarantined entry's files back to their original paths.

    Refuses if the folder isn't inside .YTArchiver Trash, if config is
    read-only, or if ANY destination already exists (never clobber a live
    file). On full success removes the now-empty trash folder + manifest.
    """
    if require_config_writable and not config_is_writable():
        return _result(False, error="Config is read-only; cannot restore.")
    manifest, manifest_path = _read_trash_manifest(
        trashed_folder_path, archive_root=archive_root)
    if manifest is None:
        return _result(False, error="Trash manifest not found or unreadable.")
    if expected_transaction_id:
        recorded_transaction_id = str(
            manifest.get("transaction_id") or "").strip()
        if recorded_transaction_id != str(expected_transaction_id).strip():
            return _result(
                False,
                error="Trash manifest does not match the removal transaction.",
            )
    original_path = manifest.get("original_path")
    if not isinstance(original_path, str) or not original_path:
        return _result(False, error="Trash manifest has no original path.")
    managed_root = _managed_root_for(original_path)
    root = archive_root or managed_root
    if not managed_root:
        return _result(False, error="Trash destination is outside the archive.")
    try:
        roots_match = (os.path.normcase(os.path.realpath(root))
                       == os.path.normcase(os.path.realpath(managed_root)))
    except (TypeError, ValueError, OSError):
        roots_match = False
    if not roots_match:
        return _result(False, error="Trash archive root does not match config.")
    recorded_root = manifest.get("archive_root")
    if recorded_root:
        try:
            roots_match = (os.path.normcase(os.path.realpath(recorded_root))
                           == os.path.normcase(os.path.realpath(root)))
        except (TypeError, ValueError, OSError):
            roots_match = False
        if not roots_match:
            return _result(False, error="Trash manifest archive root mismatch.")
    if not root or not _is_within_trash_root(trashed_folder_path, root):
        return _result(
            False, error="Refusing to restore from outside the app trash.")
    if (not _is_strictly_within(original_path, root)
            or _is_within_trash_root(original_path, root)):
        return _result(
            False, error="Refusing to restore to an unsafe destination.")

    files = manifest.get("files")
    entry_type = manifest.get("entry_type") or (
        "video" if isinstance(files, list) else "channel_folder")

    # Channel folders are moved as one directory.  Older manifests did not
    # carry entry_type/files; treating that exact legacy shape as a folder
    # restores entries already present in users' trash.
    if entry_type == "channel_folder":
        recorded_src = manifest.get("trashed_path") or trashed_folder_path
        if (os.path.normcase(os.path.realpath(recorded_src))
                != os.path.normcase(os.path.realpath(trashed_folder_path))):
            return _result(False, error="Trash folder manifest path mismatch.")
        if os.path.exists(original_path):
            return _result(
                False, error=f"Destination already exists: {original_path}. "
                "Restore aborted.")
        try:
            os.makedirs(os.path.dirname(original_path), exist_ok=True)
            _move_no_replace(trashed_folder_path, original_path)
        except OSError as exc:
            return _result(False, error=str(exc), restored=[])
        # Move the manifest with the folder and remove it only after the
        # restore is durable.  Deleting it before the move created a crash
        # window where the only recovery record vanished while the folder was
        # still in trash.
        restored_manifest = os.path.join(
            original_path, os.path.basename(manifest_path))
        try:
            os.remove(restored_manifest)
        except OSError as exc:
            return _result(
                True,
                entry_type="channel_folder",
                restored=[os.path.normpath(original_path)],
                cleanup_warning=str(exc),
            )
        return _result(True, entry_type="channel_folder",
                       restored=[os.path.normpath(original_path)])

    if entry_type != "video" or not isinstance(files, list) or not files:
        return _result(False, error="Trash manifest has no restorable files.")

    manifest_state = manifest.get("state", "complete")
    resumable_restore = manifest_state in {"pending", "restoring"}
    if manifest_state not in {"pending", "restoring", "complete"}:
        return _result(
            False, error=f"Trash entry is in an unsupported state: "
            f"{manifest_state}.")

    # A complete trash entry has every file in quarantine. Publish the
    # recovery intent before moving the first one back. If the process stops
    # mid-restore, the next launch can distinguish already-restored files from
    # real destination conflicts and continue safely.
    if manifest_state == "complete":
        manifest["state"] = "restoring"
        try:
            _write_trash_manifest(trashed_folder_path, manifest)
        except OSError as exc:
            return _result(
                False, error=f"Could not start recoverable restore: {exc}.")
        resumable_restore = True

    validated: list[tuple[str, str, str, bool | None]] = []
    seen_src: set[str] = set()
    seen_dest: set[str] = set()
    for entry in files:
        if not isinstance(entry, dict):
            return _result(False, error="Trash manifest contains a bad file entry.")
        src = entry.get("trashed_path")
        dest = entry.get("original_path")
        if not isinstance(src, str) or not isinstance(dest, str):
            return _result(False, error="Trash manifest contains a bad file path.")
        src_key = os.path.normcase(os.path.realpath(src))
        dest_key = os.path.normcase(os.path.realpath(dest))
        if (not _is_strictly_within(src, trashed_folder_path)
                or not _is_strictly_within(dest, root)
                or _is_within_trash_root(dest, root)):
            return _result(False, error="Trash manifest contains an unsafe path.")
        if src_key in seen_src or dest_key in seen_dest:
            return _result(False, error="Trash manifest contains duplicate paths.")
        original_hidden = entry.get("original_hidden")
        if not isinstance(original_hidden, bool):
            original_hidden = None
        seen_src.add(src_key)
        seen_dest.add(dest_key)
        src_present = os.path.lexists(src)
        src_is_file = os.path.isfile(src) and not os.path.islink(src)
        dest_present = os.path.lexists(dest)
        dest_is_file = os.path.isfile(dest) and not os.path.islink(dest)
        if src_present and not src_is_file:
            return _result(
                False, error=f"Trashed source is not a regular file: {src}.")
        if resumable_restore:
            if src_is_file and dest_present:
                return _result(
                    False, error=f"Destination already exists: {dest}. "
                    "Restore aborted.")
            if not src_is_file and not dest_present:
                return _result(
                    False, error=f"Both source and destination are missing: "
                    f"{dest}.")
            if not src_is_file and not dest_is_file:
                return _result(
                    False, error=f"Restore destination is not a regular file: "
                    f"{dest}.")
            state = "move" if src_is_file else "already_original"
            validated.append((src, dest, state, original_hidden))
            continue
        if not src_is_file:
            return _result(False, error=f"Trashed source is missing: {src}.")
        if dest_present:
            return _result(
                False, error=f"Destination already exists: {dest}. Restore aborted.")
        validated.append((src, dest, "move", original_hidden))

    try:
        for src, dest, state, original_hidden in validated:
            if state != "already_original":
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                _move_no_replace(src, dest)
            # Apply this immediately after each move.  If the process stops
            # later, the already-original recovery path repeats it safely.
            _restore_original_hidden_state(dest, original_hidden)
    except OSError as exc:
        # Leave the manifest in `restoring` state. Rolling files backward here
        # creates the same partial-copy overwrite risk as the trash operation;
        # the next call can instead reconcile each source/destination pair.
        return _result(False, error=str(exc), restored=[], resumable=True)

    restored = [
        os.path.normpath(dest)
        for _src, dest, _state, _original_hidden in validated
    ]
    try:
        remaining = [name for name in os.listdir(trashed_folder_path)
                     if name != os.path.basename(manifest_path)]
    except OSError as exc:
        return _result(True, restored=restored,
                       cleanup_warning=str(exc), entry_type="video")
    if remaining:
        return _result(
            True,
            restored=restored,
            cleanup_warning=("Trash folder contains untracked files; kept "
                             "the recovery manifest: " + ", ".join(remaining)),
            entry_type="video",
        )
    recovery_marker = _restore_cleanup_marker_path(trashed_folder_path)
    inside_manifest = os.path.join(
        trashed_folder_path, ".ytarchiver-trash.json")
    manifest_is_inside = (
        os.path.normcase(os.path.abspath(manifest_path))
        == os.path.normcase(os.path.abspath(inside_manifest))
    )
    if manifest_is_inside:
        # A non-empty folder cannot be removed while its manifest is inside
        # it. Publish the same recovery record outside the entry first, then
        # remove the inside copy. A power loss or a file appearing before
        # rmdir can no longer leave an unlisted, unrecoverable trash folder.
        try:
            _write_restore_cleanup_marker(
                trashed_folder_path, manifest, archive_root=root)
            os.remove(manifest_path)
        except OSError as exc:
            return _result(True, restored=restored,
                           cleanup_warning=str(exc), entry_type="video")
    try:
        # The outside marker remains authoritative until folder removal is
        # proven. If this fails for any reason, list_trash_entries can still
        # discover the entry through that marker.
        os.rmdir(trashed_folder_path)
    except OSError as exc:
        return _result(True, restored=restored,
                       cleanup_warning=str(exc), entry_type="video")
    marker_warning = ""
    if _restore_recovery_dir_is_safe(
            trashed_folder_path, archive_root=root):
        try:
            os.remove(recovery_marker)
        except OSError as exc:
            marker_warning = str(exc)
        try:
            os.rmdir(os.path.dirname(recovery_marker))
        except OSError:
            pass
    else:
        marker_warning = "Trash restore recovery folder is not safely contained."
    if marker_warning:
        return _result(True, restored=restored,
                       cleanup_warning=marker_warning, entry_type="video")
    return _result(True, restored=restored, entry_type="video")


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
    recovery_marker = _restore_cleanup_marker_path(trashed_folder_path)
    if _restore_recovery_dir_is_safe(
            trashed_folder_path, archive_root=root):
        try:
            os.remove(recovery_marker)
        except OSError:
            pass
        try:
            os.rmdir(os.path.dirname(recovery_marker))
        except OSError:
            pass
    return _result(True, purged=os.path.normpath(trashed_folder_path))


def safe_rmtree_channel_folder(
    folder_path: str,
    *,
    require_config_writable: bool = True,
    reason: str = "",
    reserved_trash_path: str = "",
    transaction_id: str = "",
    channel_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Move one managed channel folder to the app trash/quarantine."""
    guard = assert_within_managed_roots(folder_path)
    if not guard.get("ok"):
        return guard
    if require_config_writable and not config_is_writable():
        return _result(
            False,
            error=("Settings are temporarily read-only, so no folders were "
                   "moved to Trash. Restart YTArchiver and try again."),
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
    if not _is_strictly_within(folder_path, archive_root):
        return _result(
            False,
            error="Refusing to move the archive root itself to trash.",
            folder_path=os.path.normpath(folder_path),
        )
    reserved_path = str(reserved_trash_path or "").strip()
    if bool(reserved_path) != bool(transaction_id):
        return _result(
            False,
            error=("A recoverable folder move requires both its reserved "
                   "trash path and transaction id."),
            folder_path=os.path.normpath(folder_path),
        )
    if reserved_path:
        try:
            expected_path = channel_trash_destination(
                folder_path,
                archive_root,
                transaction_id,
            )
            if (_resolved_path_key(reserved_path)
                    != _resolved_path_key(expected_path)):
                raise ValueError(
                    "Reserved trash path does not match the transaction.")
        except ValueError as exc:
            return _result(
                False,
                error=str(exc),
                folder_path=os.path.normpath(folder_path),
            )
        reserved_path = expected_path
        if (os.path.exists(reserved_path)
                or os.path.exists(_restore_cleanup_marker_path(reserved_path))):
            return _result(
                False,
                error="The reserved trash destination already exists.",
                folder_path=os.path.normpath(folder_path),
            )
    trash_path = ""
    entry_id = uuid.uuid4().hex
    epoch = int(time.time())
    try:
        ensure_trash_root(archive_root)
        while True:
            trash_path = (
                reserved_path
                or _trash_path_for(folder_path, archive_root)
            )
            ensure_trash_root(archive_root)
            manifest = {
                "version": _TRASH_MANIFEST_VERSION,
                "entry_id": entry_id,
                "epoch": epoch,
                "entry_type": "channel_folder",
                "state": "pending",
                "archive_root": os.path.normpath(archive_root),
                "original_path": os.path.normpath(folder_path),
                "trashed_path": os.path.normpath(trash_path),
                "trashed_folder_path": os.path.normpath(trash_path),
                "trashed_at": datetime.now().isoformat(timespec="seconds"),
                "reason": reason,
            }
            if isinstance(channel_snapshot, dict) and channel_snapshot:
                # Preserve the exact on-disk channel object.  Restore must not
                # route this through the Add form, which performs unit
                # conversions and applies today's defaults.
                manifest["channel_snapshot"] = dict(channel_snapshot)
            if transaction_id:
                manifest["transaction_id"] = str(transaction_id).strip()
            # The manifest starts inside the source folder so the atomic
            # no-replace directory move carries both content and its recovery
            # record.  The move itself exclusively claims this exact trash
            # folder; a concurrent winner causes a retry, never nesting.
            _write_trash_manifest(folder_path, manifest)
            try:
                _move_no_replace(folder_path, trash_path)
            except FileExistsError:
                if reserved_path:
                    raise
                continue
            break
        manifest["state"] = "complete"
        _write_trash_manifest(trash_path, manifest)
    except OSError as exc:
        rollback_error = ""
        if trash_path and os.path.isdir(trash_path) and not os.path.exists(folder_path):
            try:
                os.makedirs(os.path.dirname(folder_path), exist_ok=True)
                _move_no_replace(trash_path, folder_path)
            except OSError as rollback_exc:
                rollback_error = str(rollback_exc)
        if os.path.isdir(folder_path):
            try:
                os.remove(os.path.join(
                    folder_path, ".ytarchiver-trash.json"))
            except OSError:
                pass
        return _result(
            False,
            error=str(exc),
            deleted_folder=False,
            folder_path=os.path.normpath(folder_path),
            reason=reason,
            rollback_error=rollback_error,
        )
    result = _result(
        True,
        deleted_folder=not os.path.exists(folder_path),
        folder_path=os.path.normpath(folder_path),
        trashed_folder_path=os.path.normpath(trash_path),
        reason=reason,
    )
    return result
