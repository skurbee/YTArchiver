"""Single commit boundary for completed media downloads."""

from __future__ import annotations

import glob
import json
import os
import re
import shutil
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..fs_search import VIDEO_AND_AUDIO_EXTS

RegisterVideo = Callable[..., bool]
EmbeddedIdReader = Callable[[str], str | None]

_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
_VIDEO_ID_SUFFIX_RE = re.compile(r"\s+\[([A-Za-z0-9_-]{11})\]$")
_VIDEO_URL_ID_RE = re.compile(
    r"(?:[?&]v=|youtu\.be/|youtube\.com/(?:shorts|live)/)"
    r"([A-Za-z0-9_-]{11})(?:\b|$)",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class DownloadCommitResult:
    ok: bool
    final_path: str
    video_id: str
    duration_seconds: float | None
    durable_media: bool
    registered: bool
    error: str = ""


@dataclass(frozen=True, slots=True)
class CollisionSafePathResult:
    """Result of promoting one ID-suffixed yt-dlp output bundle."""

    ok: bool
    final_path: str
    normalized: bool
    collision: bool
    error: str = ""


def is_durable_final_media(path: str | None) -> bool:
    """Return whether a path names completed, non-empty local media."""
    if not path:
        return False
    try:
        name = os.path.basename(path).lower()
        if (
            name.endswith((".part", ".ytdl", ".tmp"))
            or "_temp_compress" in name
        ):
            return False
        if os.path.splitext(name)[1] not in VIDEO_AND_AUDIO_EXTS:
            return False
        return os.path.isfile(path) and os.path.getsize(path) > 0
    except OSError:
        return False


def _duration_seconds(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _id_from_info_json(media_path: str) -> str:
    """Return the ID in the media's exact sibling ``.info.json``."""
    info_path = os.path.splitext(media_path)[0] + ".info.json"
    try:
        with open(info_path, "r", encoding="utf-8") as handle:
            data = json.load(handle) or {}
    except (OSError, ValueError, TypeError):
        return ""
    video_id = str(data.get("id") or "").strip()
    if _VIDEO_ID_RE.fullmatch(video_id):
        return video_id
    for key in ("webpage_url", "original_url", "url"):
        match = _VIDEO_URL_ID_RE.search(str(data.get(key) or ""))
        if match:
            return match.group(1)
    return ""


def local_media_identity_conflicts(
    path: str,
    expected_video_id: str,
    *,
    filename_id_is_provenance: bool = True,
) -> bool:
    """Return True when local filename/sidecar evidence names another ID.

    Fresh download events are already bound to their yt-dlp lifecycle, so a
    missing sidecar is not itself an error.  Any evidence that *is* present
    must agree before registration can make the ID permanent.
    """
    expected = str(expected_video_id or "").strip()
    if not _VIDEO_ID_RE.fullmatch(expected):
        return True
    evidence: set[str] = set()
    stem = os.path.splitext(os.path.basename(path or ""))[0]
    suffix_match = _VIDEO_ID_SUFFIX_RE.search(stem)
    if suffix_match and filename_id_is_provenance:
        evidence.add(suffix_match.group(1))
    sidecar_id = _id_from_info_json(path)
    if sidecar_id:
        evidence.add(sidecar_id)
    return bool(evidence and evidence != {expected})


def _find_ffprobe() -> str:
    found = shutil.which("ffprobe") or shutil.which("ffprobe.exe")
    if found:
        return found
    for candidate in (
        Path.cwd() / "ffprobe.exe",
        Path(__file__).resolve().parents[2] / "ffprobe.exe",
    ):
        if candidate.exists():
            return str(candidate)
    return "ffprobe"


def embedded_media_video_id(path: str) -> str | None:
    """Read the canonical YouTube ID embedded in the media comment tag."""
    try:
        result = subprocess.run(
            [
                _find_ffprobe(),
                "-v", "error",
                "-show_entries", "format_tags=comment",
                "-of", "json",
                path,
            ],
            capture_output=True,
            text=True,
            timeout=20,
            creationflags=(0x08000000 if os.name == "nt" else 0),
        )
        payload = json.loads(result.stdout or "{}")
        comment = str(((payload.get("format") or {}).get("tags") or {}).get(
            "comment") or "")
        match = _VIDEO_URL_ID_RE.search(comment)
        return match.group(1) if match else None
    except (OSError, subprocess.SubprocessError, ValueError, TypeError):
        return None


def existing_media_matches_video_id(
    path: str,
    expected_video_id: str,
    *,
    embedded_id_reader: EmbeddedIdReader | None = None,
) -> bool:
    """Verify an existing-file skip before its ID enters download history.

    A title-only ``.info.json`` can be overwritten by a different video with
    the same title, so it is deliberately not sufficient evidence here.  The
    embedded watch URL is authoritative.  ID-suffixed files retain a safe
    filename fallback for older/test media without embedded metadata.
    """
    expected = str(expected_video_id or "").strip()
    if not is_durable_final_media(path) or not _VIDEO_ID_RE.fullmatch(expected):
        return False
    reader = embedded_id_reader or embedded_media_video_id
    embedded = str(reader(path) or "").strip()
    if embedded:
        return embedded == expected
    stem = os.path.splitext(os.path.basename(path))[0]
    suffix_match = _VIDEO_ID_SUFFIX_RE.search(stem)
    return bool(suffix_match and suffix_match.group(1) == expected)


def _rename_without_overwrite(source: str, destination: str) -> None:
    """Move one file in-place without ever replacing an existing target."""
    if os.path.lexists(destination):
        raise FileExistsError(destination)
    if os.name == "nt":
        # Windows rename is no-replace.  This also works on the user's pooled
        # SMB archive where hard links are commonly unavailable.
        os.rename(source, destination)
        return
    # POSIX rename replaces its target.  link+unlink gives us an atomic
    # no-replace destination on the same filesystem.
    os.link(source, destination)
    os.unlink(source)


def finalize_collision_safe_bundle(
    final_path: str,
    video_id: str,
) -> CollisionSafePathResult:
    """Normalize a completed ID-suffixed yt-dlp bundle when safely possible.

    yt-dlp writes every fresh sync as ``Title [video-id].ext`` so distinct
    videos can never overwrite one another's media or sidecars.  Once the
    download is durable, this function renames the media and every same-stem
    sidecar back to the familiar ``Title.ext`` form only when *all* targets are
    free.  A real title collision simply keeps the ID-suffixed bundle.
    """
    path = os.path.normpath(str(final_path or ""))
    expected = str(video_id or "").strip()
    if not is_durable_final_media(path):
        return CollisionSafePathResult(
            False, path, False, False,
            "final media is missing, partial, or empty",
        )
    if not _VIDEO_ID_RE.fullmatch(expected):
        return CollisionSafePathResult(
            False, path, False, False, "video ID is invalid",
        )
    if local_media_identity_conflicts(path, expected):
        return CollisionSafePathResult(
            False, path, False, False,
            "filename or info sidecar belongs to a different video ID",
        )

    source_base, media_ext = os.path.splitext(path)
    marker = f" [{expected}]"
    if not source_base.endswith(marker):
        # Backward-compatible adapter/test path.  Production's new template
        # always carries the marker, but an already-created legacy output can
        # still use the verified commit path without a forced rename.
        return CollisionSafePathResult(True, path, False, False)
    destination_base = source_base[:-len(marker)]
    if not destination_base:
        return CollisionSafePathResult(True, path, False, True)
    destination_media = destination_base + media_ext

    try:
        candidates = [
            candidate for candidate in glob.glob(glob.escape(source_base) + ".*")
            if os.path.isfile(candidate)
            and not candidate.lower().endswith((".part", ".ytdl", ".tmp"))
        ]
    except OSError as exc:
        return CollisionSafePathResult(False, path, False, False, str(exc))
    # Windows path spelling is case-insensitive and yt-dlp can report the same
    # file with different slash/case spellings.  De-duplicate by filesystem
    # identity spelling so one source can never be scheduled twice.
    path_key = os.path.normcase(os.path.abspath(path))
    unique_candidates: dict[str, str] = {}
    for candidate in candidates:
        key = os.path.normcase(os.path.abspath(candidate))
        unique_candidates.setdefault(key, candidate)
    unique_candidates.setdefault(path_key, path)
    # Sidecars first, media last.  If anything fails before the final move,
    # the authoritative media remains at its original ID-bound path.
    candidates = sorted(
        unique_candidates.values(),
        key=lambda item: os.path.normcase(os.path.abspath(item)) == path_key,
    )
    moves = [
        (source, destination_base + source[len(source_base):])
        for source in candidates
    ]
    if any(os.path.lexists(destination) for _source, destination in moves):
        return CollisionSafePathResult(True, path, False, True)

    completed: list[tuple[str, str]] = []
    try:
        for source, destination in moves:
            _rename_without_overwrite(source, destination)
            completed.append((source, destination))
    except OSError as exc:
        rollback_errors: list[str] = []
        for source, destination in reversed(completed):
            try:
                if os.path.exists(destination) and not os.path.exists(source):
                    _rename_without_overwrite(destination, source)
            except OSError as rollback_exc:
                rollback_errors.append(str(rollback_exc))
        # A competing safe promotion can create the same title-only target
        # after our preflight but before one of our no-replace moves.  Once
        # every move we completed has rolled back and our ID-bound media is
        # still durable, that is an ordinary title collision, not a failed
        # download.  Keep our suffixed bundle and allow it to commit.
        if (isinstance(exc, FileExistsError)
                and not rollback_errors
                and is_durable_final_media(path)):
            return CollisionSafePathResult(True, path, False, True)
        detail = str(exc)
        if rollback_errors:
            detail += "; rollback failed: " + "; ".join(rollback_errors)
        current_path = (
            destination_media if os.path.isfile(destination_media) else path
        )
        return CollisionSafePathResult(
            False, current_path, current_path == destination_media, False,
            detail,
        )
    return CollisionSafePathResult(True, destination_media, True, False)


def commit_download(
    final_path: str,
    channel: str,
    title: str | None,
    *,
    video_id: str | None,
    auto_transcribe: bool,
    duration: Any = None,
    upload_date: str = "",
    registrar: RegisterVideo | None = None,
    filename_id_is_provenance: bool = True,
) -> DownloadCommitResult:
    """Validate final media and register it as one all-or-fail result."""
    path = str(final_path or "")
    vid = str(video_id or "").strip()
    duration_seconds = _duration_seconds(duration)
    if not is_durable_final_media(path):
        return DownloadCommitResult(
            False,
            path,
            vid,
            duration_seconds,
            False,
            False,
            "final media is missing, partial, or empty",
        )
    if vid and local_media_identity_conflicts(
        path,
        vid,
        filename_id_is_provenance=filename_id_is_provenance,
    ):
        return DownloadCommitResult(
            False,
            path,
            vid,
            duration_seconds,
            True,
            False,
            "final media identity does not match the requested video ID",
        )
    if registrar is None:
        from .. import index

        registrar = index.register_video
    try:
        registered = bool(registrar(
            path,
            channel,
            title or None,
            tx_status="pending" if auto_transcribe else "no_captions",
            video_id=vid or None,
            duration_secs=duration_seconds,
            upload_date=str(upload_date or "").strip(),
        ))
    except Exception as exc:
        return DownloadCommitResult(
            False,
            path,
            vid,
            duration_seconds,
            True,
            False,
            str(exc),
        )
    if not registered:
        return DownloadCommitResult(
            False,
            path,
            vid,
            duration_seconds,
            True,
            False,
            "index rejected the final media",
        )
    return DownloadCommitResult(
        True,
        path,
        vid,
        duration_seconds,
        True,
        True,
    )


__all__ = [
    "CollisionSafePathResult",
    "DownloadCommitResult",
    "commit_download",
    "embedded_media_video_id",
    "existing_media_matches_video_id",
    "finalize_collision_safe_bundle",
    "is_durable_final_media",
    "local_media_identity_conflicts",
]
