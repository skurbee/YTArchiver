"""Subprocess and process-management utilities (split from utils.py)."""
from __future__ import annotations

import contextlib
import os
import subprocess

from .log import get_logger

_log = get_logger(__name__)


def utf8_subprocess_env() -> dict[str, str]:
    """Return a copy of os.environ with PYTHONIOENCODING forced to utf-8.

    On Windows, Python subprocess stdout defaults to the console's code
    page (typically cp1252), which mangles non-ASCII characters yt-dlp
    emits in video titles (curly apostrophes, em-dashes, etc.). Reading
    those bytes back with `encoding="utf-8", errors="replace"` produces
    U+FFFD replacement chars like "World’s" -> "World�s".

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
    cp1252 so characters like U+2019 (’, curly apostrophe) round-
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


def kill_process(proc: subprocess.Popen | None, timeout: float = 2.0) -> None:
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
        except Exception as e:
            _log.debug("swallowed: %s", e)
        try:
            proc.wait(timeout=float(timeout))
            return
        except subprocess.TimeoutExpired:
            pass
        try:
            proc.kill()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        try:
            proc.wait(timeout=1.0)
        except Exception as e:
            _log.debug("swallowed: %s", e)
    except Exception as e:
        _log.debug("swallowed: %s", e)


def ffprobe_is_compressed(filepath: str) -> bool:
    """Heuristic: True if the video was produced by this app's compress
    pipeline.

    v80 reality check: this docstring used to claim compressed files
    are stamped `encoder=ytarchive_nvenc` — they never were (the MP4
    muxer force-writes `encoder=Lavf...`, so an explicit encoder tag
    can't survive). Files compressed before v80 carry
    `comment=ytarchiver_compressed=1`; v80+ compressions carry no stamp
    (the comment tag now holds the video's provenance URL instead, and
    compressed-ness is verified by the codec probe at encode time).
    Detects the legacy comment stamp plus the historical encoder
    markers. No current call sites — kept as the documented public
    helper for the question.
    """
    if not filepath or not os.path.isfile(filepath):
        return False
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "format_tags=encoder,comment",
             "-of", "default=noprint_wrappers=1:nokey=1",
             filepath],
            capture_output=True, text=True, timeout=10,
            creationflags=(0x08000000 if os.name == "nt" else 0),
        )
        tag = (r.stdout or "").strip().lower()
        return ("ytarchive_nvenc" in tag or "av1_nvenc" in tag
                or "ytarchiver_compressed" in tag)
    except Exception:
        return False


@contextlib.contextmanager
def managed_popen(*args, **kwargs):
    """Popen variant that guarantees cleanup on exit.

    Wraps subprocess.Popen so that if an exception escapes the `with`
    block — or the block exits normally without the caller calling
    `.wait()` — the subprocess is terminate/wait/kill'd via
    `kill_process()` instead of leaking until garbage collection.

    Same arguments as `subprocess.Popen`.

    Usage:
        with managed_popen(["yt-dlp", url], stdout=subprocess.PIPE,
                           text=True) as proc:
            for line in proc.stdout:
                ...
        # Even if the loop body raises, proc gets terminate/kill'd here.

    Note: existing call sites that already pair `Popen` with a `try/
    finally proc.wait()/kill()` pattern are equally safe — this helper
    is the cleaner replacement for new code or refactors.
    """
    proc = subprocess.Popen(*args, **kwargs)
    try:
        yield proc
    finally:
        # Skip terminate when the proc already exited normally —
        # kill_process eats the no-op anyway, but avoiding the
        # spurious "trying to terminate a dead PID" path keeps logs
        # cleaner and is the right cosmetic shape (audit:
        # utils.py:494-520).
        try:
            if proc.poll() is None:
                kill_process(proc)
        except Exception:
            kill_process(proc)
