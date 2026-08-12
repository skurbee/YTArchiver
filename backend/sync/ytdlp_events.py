"""Small yt-dlp stdout parsing helpers."""

from __future__ import annotations

import re

YTDLP_VIDEO_ID_RE = re.compile(
    r"\[(?:youtube|info|download)\]\s+([A-Za-z0-9_-]{11}):")

_FALSE_POSITIVE_VIDEO_ID_WORDS = {"destination"}

_VERBOSE_CHATTER_PREFIXES = (
    "[info]",
    "[Merger]",
    "[Remuxer]",
    "[FixupM3u8]",
    "[ExtractAudio]",
    "[Metadata]",
    "Deleting original file",
)


def extract_video_id_from_line(line: str) -> str:
    """Return the current yt-dlp video id, rejecting known false hits."""
    match = YTDLP_VIDEO_ID_RE.search(line or "")
    if not match:
        return ""
    candidate = match.group(1)
    if candidate.lower() in _FALSE_POSITIVE_VIDEO_ID_WORDS:
        return ""
    return candidate


def is_verbose_chatter_line(line: str) -> bool:
    """True for normal yt-dlp operational chatter hidden in Simple mode."""
    return (line or "").lstrip().startswith(_VERBOSE_CHATTER_PREFIXES)
