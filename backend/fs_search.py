"""
Canonical filesystem-search helpers — consolidates the file-walker
patterns previously duplicated across reorg.py, redownload.py,
archive_scan.py, metadata.py, and index.py.

single source of truth for video-extension lists and
channel-folder walking. Different callers needed slightly different ext
sets (audio-included vs. video-only, leniency on .avi/.mov/.flv) — this
module provides named constants for each variant so callers can be
explicit about what they want.

Public API:
    VIDEO_EXTS_CORE          frozenset of {.mp4 .mkv .webm .mov .m4v}
    VIDEO_EXTS_EXTENDED      core + {.avi .flv .wmv}
    AUDIO_EXTS               frozenset of {.wav .mp3 .m4a .flac .ogg .opus}
    VIDEO_AND_AUDIO_EXTS     extended + audio
    walk_channel_videos(folder, *, exts=VIDEO_EXTS_EXTENDED, skip_partial=True) -> Iterator[Path]
    walk_channel_files(folder, exts) -> Iterator[Path]
    is_partial_artifact(name, dir_path=None) -> bool
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterable, Iterator
from pathlib import Path

VIDEO_EXTS_CORE: frozenset[str] = frozenset({
    ".mp4", ".mkv", ".webm", ".mov", ".m4v",
})

VIDEO_EXTS_EXTENDED: frozenset[str] = VIDEO_EXTS_CORE | frozenset({
    ".avi", ".flv", ".wmv",
})

AUDIO_EXTS: frozenset[str] = frozenset({
    ".wav", ".mp3", ".m4a", ".flac", ".ogg", ".opus",
})

VIDEO_AND_AUDIO_EXTS: frozenset[str] = VIDEO_EXTS_EXTENDED | AUDIO_EXTS

# Tuple form for str.endswith()/`in` callers (membership order is irrelevant).
# Single source of truth so archive_scan / index_maintenance / metadata.scan /
# reorg all classify the same files as media — earlier each kept its own tuple
# and they drifted (.flv/.wmv in reorg but not the disk-count) (audit:
# VIDEO_EXTS divergence).
MEDIA_EXTS_TUPLE: tuple[str, ...] = tuple(sorted(VIDEO_AND_AUDIO_EXTS))


_PARTIAL_FRAG_RE = re.compile(
    r"\.f\d{1,4}(?:-\d+)?\.[a-z0-9]{3,4}$", re.IGNORECASE)


def _has_partial_fragment_evidence(name: str,
                                   dir_path: str | os.PathLike | None) -> bool:
    """Return True when a `.fNNN` media file has sibling evidence of a merge.

    A title can legitimately end with `.f140.mp4`. Treat it as a partial
    only when the caller gives us its directory and we can see a final merged
    sibling, or a `.part` sidecar from an interrupted yt-dlp run. Callers that
    only have a filename keep the historical conservative-filter behavior.
    """
    if dir_path is None:
        return True
    try:
        names = set(os.listdir(dir_path))
    except OSError:
        return False
    if name + ".part" in names:
        return True
    base, ext = os.path.splitext(name)
    m = re.search(r"\.f\d{1,4}(?:-\d+)?$", base, re.IGNORECASE)
    if not m:
        return False
    final_name = base[:m.start()] + ext
    if final_name in names or final_name + ".part" in names:
        return True
    final_prefix = base[:m.start()] + "."
    low_name = name.lower()
    for other in names:
        if other.lower() == low_name:
            continue
        other_ext = os.path.splitext(other)[1].lower()
        if other_ext in MEDIA_EXTS_TUPLE and other.startswith(final_prefix):
            return True
    return False


def is_partial_artifact(name: str,
                        dir_path: str | os.PathLike | None = None) -> bool:
    """True if `name` looks like a yt-dlp / ffmpeg temp / partial file.

    Matches the rule already used in temp_cleanup.is_partial_file but
    centralized here so any walker can filter consistently. Patterns:
      - *.part, *.temp, *.ytdl
      - *.part.* / *.temp.* (extension after fragment marker)
      - *_TEMP_COMPRESS* (anywhere in name)
      - *.fNNN.ext fragment marker (yt-dlp multi-stream remnants)
    """
    if not name:
        return False
    low = name.lower()
    if low.endswith((".part", ".temp", ".ytdl")):
        return True
    if ".part." in low or ".temp." in low:
        return True
    if "_temp_compress" in low:
        return True
    if _PARTIAL_FRAG_RE.search(name):
        return _has_partial_fragment_evidence(name, dir_path)
    base, ext = os.path.splitext(name)
    if ext.lower() in (".webm", ".m4a", ".mp4") and re.search(
            r"\.f\d{1,4}(?:-\d+)?$", base):
        return _has_partial_fragment_evidence(name, dir_path)
    return False


def walk_channel_files(folder: str | os.PathLike,
                       exts: Iterable[str]) -> Iterator[Path]:
    """Yield every file under `folder` whose suffix (lowercased) is in `exts`.

    Skips Windows-hidden sidecar files (those starting with `.`). Does
    NOT skip partial artifacts — caller should pass `skip_partial=True`
    to walk_channel_videos for that.
    """
    folder_str = os.fspath(folder)
    if not folder_str or not os.path.isdir(folder_str):
        return
    ext_set = frozenset(e.lower() for e in exts)
    for dp, _dns, fns in os.walk(folder_str):
        for fn in fns:
            if fn.startswith("."):
                continue
            ext = os.path.splitext(fn)[1].lower()
            if ext in ext_set:
                yield Path(dp) / fn


def walk_channel_videos(folder: str | os.PathLike,
                        *,
                        exts: Iterable[str] | None = None,
                        skip_partial: bool = True) -> Iterator[Path]:
    """Yield every video file under `folder`. Default ext set is
    VIDEO_EXTS_EXTENDED (the broadest video-only set). Pass
    VIDEO_AND_AUDIO_EXTS to also catch audio-only archives.

    With skip_partial=True (default), yt-dlp / ffmpeg temp artifacts
    are filtered out so callers don't have to do the check themselves.
    """
    use_exts = frozenset(exts) if exts is not None else VIDEO_EXTS_EXTENDED
    for p in walk_channel_files(folder, use_exts):
        if skip_partial and is_partial_artifact(p.name, p.parent):
            continue
        yield p
