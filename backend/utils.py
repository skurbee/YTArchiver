"""
Shared utility helpers — direct ports from YTArchiver.py.

This module collects the small, stateless helpers the rest of the backend
previously inlined or skipped. Each function name is kept close to the
original's so `git blame`-style searches across both codebases still work.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
import unicodedata
from typing import Dict, Optional


def utf8_subprocess_env() -> Dict[str, str]:
    """Return a copy of os.environ with PYTHONIOENCODING forced to utf-8.

    On Windows, Python subprocess stdout defaults to the console's code
    page (typically cp1252), which mangles non-ASCII characters yt-dlp
    emits in video titles (curly apostrophes, em-dashes, etc.). Reading
    those bytes back with `encoding="utf-8", errors="replace"` produces
    U+FFFD replacement chars like "World\u2019s" -> "World\ufffds".

    Forcing PYTHONIOENCODING=utf-8 in the subprocess env tells the
    child Python runtime (including frozen yt-dlp.exe builds) to
    reconfigure sys.stdout to UTF-8 so our reader sees valid UTF-8.

    Use via: `subprocess.Popen(..., env=utf8_subprocess_env())`.
    """
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    # Best-effort belt-and-suspenders for yt-dlp: its own re-encoding
    # layer checks this too (yt_dlp/utils/_utils.py:preferredencoding).
    env["PYTHONUTF8"] = "1"
    # LC_ALL = C.UTF-8 covers tools that read POSIX locale rather than
    # PYTHONIOENCODING (e.g. some yt-dlp helpers, ffmpeg).
    env["LC_ALL"] = "C.UTF-8"
    env["LANG"] = "C.UTF-8"
    return env


def decode_subprocess_line(line_bytes: bytes) -> str:
    """Decode a single line from yt-dlp / ffmpeg stdout.

    Tries UTF-8 first (which is what yt-dlp emits when PYTHONIOENCODING
    is set correctly). If that fails because the frozen yt-dlp.exe
    bootstrap ignored the env var and fell back to cp1252, decode as
    cp1252 so characters like U+2019 (\u2019, curly apostrophe) round-
    trip cleanly instead of becoming U+FFFD replacement chars.

    Belt-and-suspenders companion to `utf8_subprocess_env()` — reported replacement chars in titles even after the env var fix,
    suggesting yt-dlp.exe isn't consistently respecting the setting.
    """
    if not line_bytes:
        return ""
    try:
        return line_bytes.decode("utf-8")
    except UnicodeDecodeError:
        pass
    # cp1252 has no "unmapped" bytes in 0x80-0x9F for \x81, \x8D, \x8F,
    # \x90, \x9D — those raise UnicodeDecodeError without `errors`.
    # errors="replace" replaces ONLY those rare bytes, not the whole line.
    return line_bytes.decode("cp1252", errors="replace")


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


# ── Format helpers (YTArchiver.py:2815 / 9299 / 9308) ──────────────────

def format_bytes(n: int, dash_if_zero: bool = True) -> str:
    """Pretty-print a byte count. 0 → '\u2014' when dash_if_zero."""
    n = int(n or 0)
    if n <= 0 and dash_if_zero:
        return "\u2014"
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    i = 0
    v = float(n)
    while v >= 1024 and i < len(units) - 1:
        v /= 1024.0
        i += 1
    if i == 0:
        return f"{int(v)} {units[i]}"
    return f"{v:.{1 if i >= 2 else 0}f} {units[i]}"


def format_duration_hms(secs: float) -> str:
    """Format seconds as `H:MM:SS` or `MM:SS` (when under 1 hour)."""
    try:
        s = int(float(secs or 0))
    except (TypeError, ValueError):
        return "0:00"
    if s < 0:
        s = 0
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def format_elapsed(secs: float) -> str:
    """Compact elapsed-time format for in-line log strings.

    Rules (per Scott: "always fold into Xm XXs"):
      < 60s      -> "Xs"            ("47s")
      < 1h       -> "Xm YYs"        ("3m 21s", zero-padded seconds)
      >= 1h      -> "Xh Ym YYs"     ("1h 5m 03s")

    Use this anywhere the user-facing log would otherwise show raw
    seconds for a duration (heartbeats, "took N", elapsed counters).
    Never emit bare "201s" — fold via this helper instead.
    """
    try:
        s = int(float(secs or 0))
    except (TypeError, ValueError):
        return "0s"
    if s < 0:
        s = 0
    if s < 60:
        return f"{s}s"
    h, rem = divmod(s, 3600)
    m, ss = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {ss:02d}s"
    return f"{m}m {ss:02d}s"


def format_enc_size(mb: float) -> str:
    """Format a megabyte count for encode progress display (e.g. '1.23 GB')."""
    try:
        mb = float(mb)
    except (TypeError, ValueError):
        return "\u2014"
    if mb >= 1024:
        return f"{mb / 1024:.2f} GB"
    return f"{mb:.1f} MB"


def fmt_time_ago(ts: float) -> str:
    """Human-friendly 'N min ago' style for Recent tab. ts is Unix epoch."""
    try:
        age = max(0.0, time.time() - float(ts))
    except (TypeError, ValueError):
        return ""
    if age < 60: return "just now"
    if age < 3600: return f"{int(age / 60)} min ago"
    if age < 86400: return f"{int(age / 3600)} h ago"
    if age < 86400 * 30: return f"{int(age / 86400)} d ago"
    if age < 86400 * 365: return f"{int(age / 2592000)} mo ago"
    return f"{int(age / 31536000)} yr ago"


# ── Unicode title normalization (YTArchiver.py:7765) ───────────────────

_NORM_ASCII_RE = re.compile(r'[^A-Za-z0-9]+')


def norm_ascii(text: str) -> str:
    """Return an ASCII-only, lower-case, punctuation-stripped form for matching.

    Handles NFC/NFD Unicode forms so titles with combining marks (e.g.
    caf\u00e9) still match titles written as `cafe`. Used for fuzzy title
    matching in the file-date fixer + recent-file recovery.
    """
    if not text:
        return ""
    nfc = unicodedata.normalize("NFC", text)
    nfkd = unicodedata.normalize("NFKD", nfc)
    ascii_only = nfkd.encode("ascii", "ignore").decode("ascii")
    return _NORM_ASCII_RE.sub(" ", ascii_only).strip().lower()


# ── Disk pre-flight checks (YTArchiver.py:2314 / 2332) ─────────────────

def check_directory_writable(path: str) -> bool:
    """Can we create + delete a probe file inside `path`? True if yes."""
    if not path:
        return False
    try:
        if not os.path.isdir(path):
            return False
        # audit F-32: clean up any stale probe files from a previous
        # run (crashed process, antivirus-blocked unlink, etc.) before
        # writing a new one. Without this, the archive root accumulates
        # `.yta_probe_<PID>` litter over time.
        try:
            for _f in os.listdir(path):
                if _f.startswith(".yta_probe_"):
                    try: os.remove(os.path.join(path, _f))
                    except OSError: pass
        except OSError:
            pass
        probe = os.path.join(path, f".yta_probe_{os.getpid()}")
        with open(probe, "w", encoding="utf-8") as f:
            f.write("ok")
        try: os.remove(probe)
        except OSError: pass
        return True
    except OSError:
        return False


def check_disk_space(path: str, required_bytes: int) -> bool:
    """True if `path`'s filesystem has at least `required_bytes` free."""
    if not path or required_bytes <= 0:
        return True
    try:
        free = shutil.disk_usage(path).free
        return free >= int(required_bytes)
    except (OSError, ValueError):
        return True # fail open — don't block on probe errors


# ── Subprocess cleanup (YTArchiver.py:2243 / 9214 / 9262) ──────────────

def kill_process(proc: Optional[subprocess.Popen], timeout: float = 2.0) -> None:
    """Terminate then kill a child process, swallowing errors.

    Sends SIGTERM, waits up to `timeout`, then SIGKILL. No-op if proc is
    None or already exited.
    """
    if proc is None:
        return
    try:
        if proc.poll() is not None:
            return
        try:
            proc.terminate()
        except Exception:
            pass
        try:
            proc.wait(timeout=float(timeout))
            return
        except subprocess.TimeoutExpired:
            pass
        try:
            proc.kill()
        except Exception:
            pass
        try:
            proc.wait(timeout=1.0)
        except Exception:
            pass
    except Exception:
        pass


# ── ffprobe: is the file already AV1/NVENC-compressed? ────────────────
# (YTArchiver.py:9336 _ffprobe_is_compressed)

def ffprobe_is_compressed(filepath: str) -> bool:
    """Heuristic: True if the video was produced by this app's compress
    pipeline. We stamp compressed files with `encoder=ytarchive_nvenc` in
    the format metadata; ffprobe reads that tag.
    """
    if not filepath or not os.path.isfile(filepath):
        return False
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "format_tags=encoder",
             "-of", "default=noprint_wrappers=1:nokey=1",
             filepath],
            capture_output=True, text=True, timeout=10,
            creationflags=(0x08000000 if os.name == "nt" else 0),
        )
        tag = (r.stdout or "").strip().lower()
        return "ytarchive_nvenc" in tag or "av1_nvenc" in tag
    except Exception:
        return False


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
                      video_id: str = "") -> Optional[str]:
    """Locate a file under `channel_folder` whose stem matches `title`
    (case-insensitive, ignoring punctuation and year/month folder splits).

    Tries an exact-stem match first, then a `[videoId]` token match, then
    an ASCII-fuzzy match on the title. Returns the first hit or None.
    """
    # audit F-33: early-return on the first exact or [id] hit instead
    # of walking the whole folder before returning. For a 5000-video
    # channel, an exact match at depth 1 used to walk every subfolder
    # before returning. Also: bail early once we have an ascii-fuzzy
    # candidate and the remaining walk is likely to just duplicate it.
    if not channel_folder or not os.path.isdir(channel_folder):
        return None
    norm_title = norm_ascii(title)
    vid_norm = (video_id or "").strip().lower()
    first_ascii_match: Optional[str] = None
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
                          video_id: str = "") -> Optional[str]:
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
