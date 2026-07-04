"""Windows file-attribute management (split from utils.py)."""
from __future__ import annotations

import ctypes
import os
import re

from .fs_search import VIDEO_EXTS_EXTENDED as _FS_VIDEO_EXTS
from .log import get_logger

_log = get_logger(__name__)

_VISIBLE_MEDIA_EXTS = tuple(sorted(_FS_VIDEO_EXTS))
_TRANSCRIPT_SUFFIX = " transcript.txt"
_TRANSCRIPT_STEM_SUFFIX = " transcript"
_YT_ID_SUFFIX_RE = re.compile(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$")


def _strip_youtube_id_suffix(stem: str) -> str:
    return _YT_ID_SUFFIX_RE.sub("", stem) or stem


def _manual_transcript_media_key(name: str) -> str:
    root, ext = os.path.splitext(name)
    if ext.lower() != ".txt":
        return ""
    if not root.lower().endswith(_TRANSCRIPT_STEM_SUFFIX):
        return ""
    return root[: -len(_TRANSCRIPT_STEM_SUFFIX)].lower()


def _archive_file_should_be_visible(
        name: str, *, hide_per_video_transcripts: bool = False,
        media_stems: set[str] | None = None) -> bool:
    """Whitelist test: True for video/media files and the conjoined
    `… Transcript.txt`. Everything else is a sidecar that must be hidden.
    Matches the user contract: an archive folder shows only the videos +
    one transcript .txt when 'show hidden files' is off."""
    low = name.lower()
    if low.endswith(_VISIBLE_MEDIA_EXTS):
        return True
    if low.endswith(_TRANSCRIPT_SUFFIX):
        if hide_per_video_transcripts:
            media_key = _manual_transcript_media_key(name)
            if media_key and media_key in (media_stems or set()):
                return False
        return True
    return False


def hide_file_win(path) -> None:
    """Set the Windows HIDDEN attribute on a file or folder. No-op on
    non-Windows.

    Used for sidecar files (e.g. `.{name} Metadata.jsonl`) and folders
    (e.g. `.Thumbnails/`, `.ChannelArt/`) that should be invisible to
    Explorer but readable by the app.

    Checks the SetFileAttributesW return value (Windows returns 0 on
    failure). Logs a warning on failure rather than silently swallowing —
    per the "ULTIMATE RULE" memory, sidecars MUST stay hidden; a silent
    failure here would expose internals to the user's archive view.
    Preserves the file's other attributes (read-only, system, archive
    bit) by OR-ing FILE_ATTRIBUTE_HIDDEN into the current value rather
    than passing 0x02 alone.
    """
    if os.name != "nt":
        return
    try:
        p = str(path)
        FILE_ATTRIBUTE_HIDDEN = 0x02
        INVALID_FILE_ATTRIBUTES = 0xFFFFFFFF
        cur = ctypes.windll.kernel32.GetFileAttributesW(p)
        if cur == INVALID_FILE_ATTRIBUTES:
            # File may not exist; nothing to do. Don't warn — callers
            # often invoke this defensively against paths that may have
            # already been deleted.
            return
        new = cur | FILE_ATTRIBUTE_HIDDEN
        if new == cur:
            return  # already hidden
        ok = ctypes.windll.kernel32.SetFileAttributesW(p, new)
        if not ok:
            err = ctypes.windll.kernel32.GetLastError()
            _log.warning("hide_file_win failed for %s (err=%s)", p, err)
    except Exception as e:
        _log.debug("swallowed: %s", e)


def unhide_file_win(path) -> None:
    """Clear the Windows HIDDEN attribute. No-op on non-Windows.

    Companion to `hide_file_win` — used before atomic rewrites of hidden
    sidecars so `os.replace` can target the file. Re-hide after the
    rewrite with `hide_file_win`.

    Preserves other attributes (read-only, system, archive bit) by
    masking out only FILE_ATTRIBUTE_HIDDEN instead of overwriting all
    attributes with FILE_ATTRIBUTE_NORMAL (0x80). The old behavior
    clobbered the archive bit, breaking backup tools that rely on it.
    """
    if os.name != "nt":
        return
    try:
        p = str(path)
        FILE_ATTRIBUTE_HIDDEN = 0x02
        FILE_ATTRIBUTE_NORMAL = 0x80
        INVALID_FILE_ATTRIBUTES = 0xFFFFFFFF
        cur = ctypes.windll.kernel32.GetFileAttributesW(p)
        if cur == INVALID_FILE_ATTRIBUTES:
            return
        new = cur & ~FILE_ATTRIBUTE_HIDDEN
        # Per MSDN: FILE_ATTRIBUTE_NORMAL is only valid when set ALONE;
        # SetFileAttributesW will fail if NORMAL is combined with other
        # attributes. Replace with NORMAL only if no other bits remain.
        if new == 0:
            new = FILE_ATTRIBUTE_NORMAL
        if new == cur:
            return  # already not hidden
        ok = ctypes.windll.kernel32.SetFileAttributesW(p, new)
        if not ok:
            err = ctypes.windll.kernel32.GetLastError()
            _log.warning("unhide_file_win failed for %s (err=%s)", p, err)
    except Exception as e:
        _log.debug("swallowed: %s", e)


def _file_has_hidden_attribute(path: str) -> bool:
    """True when Windows marks the file hidden; False elsewhere/fail-closed."""
    if os.name != "nt":
        return False
    FILE_ATTRIBUTE_HIDDEN = 0x02
    INVALID_FILE_ATTRIBUTES = 0xFFFFFFFF
    try:
        attrs = ctypes.windll.kernel32.GetFileAttributesW(str(path))
    except Exception:
        return False
    return attrs != INVALID_FILE_ATTRIBUTES and bool(attrs & FILE_ATTRIBUTE_HIDDEN)


def _set_hidden_if_needed(path, entry=None) -> bool:
    """Set FILE_ATTRIBUTE_HIDDEN on `path` only if it isn't already
    hidden, preserving other attribute bits. Returns True iff the item
    was visible and is now hidden (so callers can count real changes).

    When `entry` is an os.DirEntry, its cached `st_file_attributes`
    (populated for free by the directory scan on Windows) is used to
    test the hidden bit without an extra GetFileAttributesW syscall —
    so a sweep over an already-clean archive costs ~one syscall per
    *new* stray, not per file."""
    if os.name != "nt":
        return False
    FILE_ATTRIBUTE_HIDDEN = 0x02
    INVALID_FILE_ATTRIBUTES = 0xFFFFFFFF
    attrs = None
    if entry is not None:
        try:
            attrs = entry.stat(follow_symlinks=False).st_file_attributes
        except (OSError, AttributeError, ValueError):
            attrs = None
    try:
        p = str(path)
        if attrs is None:
            attrs = ctypes.windll.kernel32.GetFileAttributesW(p)
            if attrs == INVALID_FILE_ATTRIBUTES:
                return False
        if attrs & FILE_ATTRIBUTE_HIDDEN:
            return False  # already hidden
        ok = ctypes.windll.kernel32.SetFileAttributesW(
            p, attrs | FILE_ATTRIBUTE_HIDDEN)
        if not ok:
            err = ctypes.windll.kernel32.GetLastError()
            _log.warning("hide stray failed for %s (err=%s)", p, err)
            return False
        return True
    except Exception as e:
        _log.debug("swallowed: %s", e)
        return False


def hide_stray_sidecars(folder, recursive=True, cancel_event=None,
                        hide_per_video_transcripts=False) -> int:
    """Sweep `folder` and set the Windows HIDDEN attribute on every file
    that is NOT a video/media file and NOT a `… Transcript.txt`, plus any
    dot-prefixed subdirectory (`.Thumbnails`, `.ChannelArt`). This is the
    bulletproof enforcement of the archive-folder contract — robust to
    any sidecar naming oddity (e.g. yt-dlp's double-dot
    `Title..info.json`) because it's a whitelist, not a name-reconstruct.

    Returns the count of items NEWLY hidden (were visible, now hidden).
    No-op on non-Windows. Idempotent and cheap on a clean archive: uses
    os.scandir so already-hidden items cost no extra syscall. Dot-dirs
    are hidden but not descended into (a hidden parent hides its whole
    subtree in Explorer)."""
    if os.name != "nt":
        return 0
    folder = str(folder)
    if not folder or not os.path.isdir(folder):
        return 0
    newly = 0
    stack = [folder]
    while stack:
        if cancel_event is not None and cancel_event.is_set():
            break
        d = stack.pop()
        try:
            it = os.scandir(d)
        except OSError:
            continue
        with it:
            try:
                entries = list(it)
            except OSError:
                continue
            media_stems: set[str] = set()
            if hide_per_video_transcripts:
                for entry in entries:
                    try:
                        if not entry.is_file(follow_symlinks=False):
                            continue
                        root, ext = os.path.splitext(entry.name)
                        if ext.lower() not in _VISIBLE_MEDIA_EXTS:
                            continue
                        media_stems.add(root.lower())
                        media_stems.add(_strip_youtube_id_suffix(root).lower())
                    except OSError:
                        continue
            for entry in entries:
                if cancel_event is not None and cancel_event.is_set():
                    break
                try:
                    name = entry.name
                    if entry.is_dir(follow_symlinks=False):
                        if name.startswith("."):
                            # Hide the dot-dir; don't descend — a hidden
                            # parent already hides its whole subtree.
                            if _set_hidden_if_needed(entry.path, entry):
                                newly += 1
                            continue
                        if recursive:
                            stack.append(entry.path)
                        continue
                    # Regular file.
                    if _archive_file_should_be_visible(
                            name,
                            hide_per_video_transcripts=hide_per_video_transcripts,
                            media_stems=media_stems):
                        continue
                    if _set_hidden_if_needed(entry.path, entry):
                        newly += 1
                except OSError:
                    continue
    return newly
