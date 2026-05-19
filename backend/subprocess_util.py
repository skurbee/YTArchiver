"""
Canonical subprocess helpers — consolidates the startupinfo /
creationflags / env wiring that was previously copy-pasted across
compress.py, sync.py, and transcribe.py.

single source of truth so a flag change (e.g. adding
PYTHONUTF8 to the env or switching the CREATE_NO_WINDOW constant) lands
in ONE place instead of three. Existing module-level `_startupinfo`
constants in those files now delegate to `make_startupinfo()`.

Public API:
    make_startupinfo() -> subprocess.STARTUPINFO | None
    subprocess_creationflags() -> int
    utf8_env(extra: dict | None = None) -> dict[str, str]
    decode_subprocess_line(line: bytes) -> str  (re-export from utils)
"""

from __future__ import annotations

import os
import subprocess

# Re-export so callers can `from .subprocess_util import decode_subprocess_line`
from .utils import decode_subprocess_line, utf8_subprocess_env  # noqa: F401

# Windows constant: hides the console window when spawning native exes
# (yt-dlp.exe, ffmpeg.exe, ffprobe.exe). 0x08000000 = CREATE_NO_WINDOW.
# No-op on non-Windows.
CREATE_NO_WINDOW = 0x08000000


def make_startupinfo() -> subprocess.STARTUPINFO | None:
    """Return a STARTUPINFO that hides the console on Windows, None on
    other platforms. Use via:

        subprocess.Popen(argv, startupinfo=make_startupinfo(), ...)

    Identical to the inlined pattern across compress.py:54-58,
    sync.py:315-319, transcribe.py:44-48 — now in one place.
    """
    if os.name != "nt":
        return None
    si = subprocess.STARTUPINFO()
    si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    return si


def subprocess_creationflags() -> int:
    """Return the appropriate creationflags for subprocess.Popen on the
    current platform. 0 on non-Windows; CREATE_NO_WINDOW on Windows so
    native executables don't flash a console window."""
    return CREATE_NO_WINDOW if os.name == "nt" else 0


def utf8_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Alias for utils.utf8_subprocess_env with optional overrides.

    Provided so callers can do:
        env = utf8_env({"WHISPER_DEVICE": "cuda"})
    instead of:
        env = utf8_subprocess_env(); env["WHISPER_DEVICE"] = "cuda"

    The extra dict's keys override anything in the base utf8 env.
    """
    env = utf8_subprocess_env()
    if extra:
        env.update(extra)
    return env
