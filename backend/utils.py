"""
Shared utility helpers — direct ports from YTArchiver.py.

This module collects the small, stateless helpers the rest of the backend
previously inlined or skipped. Each function name is kept close to the
original's so `git blame`-style searches across both codebases still work.

Concern-area sub-modules (split out for maintainability):
  proc_utils.py — subprocess/process helpers
  fmt_utils.py  — byte/time/duration formatting
  fs_attrs.py   — Windows file-attribute management (hide/unhide)
  fs_safety.py  — disk checks, archive containment, atomic I/O, sidecar cleanup

All public names from those modules are re-exported here so existing
`from .utils import X` call sites continue to work without changes.
"""

from __future__ import annotations

import os
import re
import unicodedata

from .log import get_logger

# Re-exports from concern-area sub-modules (backward-compat shims).  # noqa: E402
from .fmt_utils import (  # noqa: F401
    fmt_time_ago,
    format_bytes,
    format_duration_hms,
    format_elapsed,
    format_enc_size,
)
from .fs_attrs import (  # noqa: F401
    _VISIBLE_MEDIA_EXTS,
    _archive_file_should_be_visible,
    _file_has_hidden_attribute,
    hide_file_win,
    hide_stray_sidecars,
    unhide_file_win,
)
from .fs_safety import (  # noqa: F401
    atomic_write,
    check_directory_writable,
    check_disk_space,
    delete_video_sidecars,
    is_within_managed_roots,
    load_json_safe,
    sampled_files_equal,
)
from .proc_utils import (  # noqa: F401
    decode_subprocess_line,
    ffprobe_is_compressed,
    kill_process,
    managed_popen,
    utf8_subprocess_env,
)

_log = get_logger(__name__)


# ── Single source of truth for year/month folder naming ───────────────
# OLD YTArchiver's sync template writes channels into `{year}/{MM Month}/`
# subfolders. All three of reorg/metadata/transcribe need to round-trip
# against those same folder names, so they share this one dict.
MONTH_FOLDERS = {
    1: "01 January",
    2: "02 February",
    3: "03 March",
    4: "04 April",
    5: "05 May",
    6: "06 June",
    7: "07 July",
    8: "08 August",
    9: "09 September",
    10: "10 October",
    11: "11 November",
    12: "12 December",
}


# ── Unicode title normalization (YTArchiver.py:7765) ───────────────────

_NORM_ASCII_RE = re.compile(r'[^A-Za-z0-9]+')


def norm_ascii(text: str) -> str:
    """Return an ASCII-only, lower-case, punctuation-stripped form for matching.

    Handles NFC/NFD Unicode forms so titles with combining marks (e.g.
    café) still match titles written as `cafe`. Used for fuzzy title
    matching in the file-date fixer + recent-file recovery.
    """
    if not text:
        return ""
    nfc = unicodedata.normalize("NFC", text)
    nfkd = unicodedata.normalize("NFKD", nfc)
    ascii_only = nfkd.encode("ascii", "ignore").decode("ascii")
    ascii_key = _NORM_ASCII_RE.sub(" ", ascii_only).strip().lower()
    if ascii_key:
        return ascii_key
    fallback = unicodedata.normalize("NFKC", nfc)
    return re.sub(r"[^\w\s]+", " ", fallback).strip().lower()


# ── Channel helpers ────────────────────────────────────────────────────

def channel_has_transcripts(channel_folder: str) -> bool:
    """True if any `.jsonl` sidecar exists anywhere in the folder tree."""
    if not channel_folder or not os.path.isdir(channel_folder):
        return False
    for dp, _dns, fns in os.walk(channel_folder):
        for fn in fns:
            if fn.lower().endswith(".jsonl"):
                return True
    return False


# ── Recent-file locator (YTArchiver.py:32900 / 32946 / 32966) ──────────

_VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".m4a", ".mov")


def try_find_by_title(channel_folder: str, title: str,
                      video_id: str = "") -> str | None:
    """Locate a file under `channel_folder` whose stem matches `title`
    (case-insensitive, ignoring punctuation and year/month folder splits).

    Tries an exact-stem match first, then a `[videoId]` token match, then
    an ASCII-fuzzy match on the title. Returns the first hit or None.
    """
    # early-return on the first exact or [id] hit instead
    # of walking the whole folder before returning. For a 5000-video
    # channel, an exact match at depth 1 used to walk every subfolder
    # before returning. Also: bail early once we have an ascii-fuzzy
    # candidate and the remaining walk is likely to just duplicate it.
    if not channel_folder or not os.path.isdir(channel_folder):
        return None
    norm_title = norm_ascii(title)
    vid_norm = (video_id or "").strip().lower()
    first_ascii_match: str | None = None
    _title_stripped = (title or "").strip()
    for dp, _dns, fns in os.walk(channel_folder):
        for fn in fns:
            low = fn.lower()
            if not low.endswith(_VIDEO_EXTS):
                continue
            stem = os.path.splitext(fn)[0]
            # 1. Exact stem match (after strip)
            if stem.strip() == _title_stripped:
                return os.path.join(dp, fn)
            # 2. Bracketed video-id match
            if vid_norm and f"[{vid_norm}]" in low:
                return os.path.join(dp, fn)
            # 3. Fuzzy ASCII stem — capture first hit, keep walking in
            # case a later exact/bracket match trumps it.
            if norm_title and not first_ascii_match:
                if norm_ascii(stem) == norm_title:
                    first_ascii_match = os.path.join(dp, fn)
    return first_ascii_match


def try_locate_moved_file(original_path: str, title: str,
                          channel_folder: str,
                          video_id: str = "") -> str | None:
    """Given a stored `original_path` that no longer resolves, try to find
    the moved/renamed file. Checks the channel folder via `try_find_by_title`.
    Returns the recovered absolute path or None.
    """
    if original_path and os.path.isfile(original_path):
        return original_path
    if channel_folder and os.path.isdir(channel_folder):
        found = try_find_by_title(channel_folder, title, video_id)
        if found:
            return found
    return None


# ── SQLite LIKE pattern escape ─────────────────────────────────────────

def sqlite_like_escape(s: str) -> str:
    """Escape `%` and `_` for SQLite LIKE patterns; also escape `\\`.

    LIKE treats `%` (any chars) and `_` (any single char) as wildcards.
    Folder paths commonly contain underscores in channel folder names
    (e.g. `Some_Channel`), which without escaping cause unintended
    matches in `WHERE filepath LIKE ?` queries — `Some_Channel%`
    would also match `SomeXChannelFoo`.

    Use with `ESCAPE '\\\\'` on the LIKE clause:

        pattern = sqlite_like_escape(str(folder)) + "%"
        conn.execute(
            "SELECT ... FROM videos WHERE filepath LIKE ? ESCAPE '\\\\'",
            (pattern,))
    """
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
