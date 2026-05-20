"""
Centralized subprocess management — yt-dlp + ffmpeg / ffprobe.

consolidates the 11+ ad-hoc Popen call sites scattered
across sync.py, metadata.py, redownload.py, compress.py, transcribe.py,
channel_art.py, repair_captions.py, and api_mixins/diagnostics_mixin.py.

Before this module: every call site independently located yt-dlp, built
its own cookie args, set its own startupinfo, decided whether to use
creationflags, and the only way shutdown reaped zombies was psutil
child-scanning + string-matching `"yt-dlp" in name`.

After: every yt-dlp invocation goes through `YtDlpRunner`, every Popen
is registered with `ProcessRegistry`, and shutdown just calls
`registry.kill_all()`. PID tracking eliminates the brute-force child-
scanning at shutdown.

Migration is incremental — Patch 3 wires in the probe-style call sites
(subs.fetch_channel_display_name, prefetch_channel_total,
quick_check_new_uploads, channel_art, ytdlp_version). The 1,800-line
sync_channel main pass is intentionally left on the legacy path until
its decomposition in Patch 7; the registry catches its Popens anyway
via the optional `track(proc)` call so shutdown still cleans up.

Public API:
    PROCESS_REGISTRY: ProcessRegistry  (module-level singleton)
    YtDlpRunner: invocation wrapper with consistent flag/cookie/env
    FfmpegRunner: same shape for ffmpeg/ffprobe
    find_yt_dlp() -> Path | None  (re-exported for backward compat)
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterable
from pathlib import Path

from .log import get_logger
from .subprocess_util import (
    make_startupinfo,
    subprocess_creationflags,
    utf8_env,
)

_log = get_logger(__name__)


# ── ProcessRegistry ───────────────────────────────────────────────────

class ProcessRegistry:
    """Tracks live child processes for clean shutdown.

    Every Popen registered here is killed on `kill_all()` — used at app
    shutdown to ensure no zombie yt-dlp / ffmpeg / ffprobe lingers.
    Replaces the psutil child-scanning + name-matching hack in main.py's
    _shutdown_cleanup.

    Thread-safe. Idempotent: re-registering or unregistering an unknown
    proc is a no-op.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._procs: list[subprocess.Popen] = []

    def register(self, proc: subprocess.Popen) -> subprocess.Popen:
        """Track `proc` for shutdown cleanup. Returns the same proc."""
        if proc is None:
            return proc
        with self._lock:
            self._procs.append(proc)
        return proc

    def unregister(self, proc: subprocess.Popen) -> None:
        """Stop tracking `proc`. Call after wait()/poll() returns a
        non-None code, so the registry doesn't accumulate dead procs."""
        if proc is None:
            return
        with self._lock:
            try:
                self._procs.remove(proc)
            except ValueError:
                pass

    def reap_dead(self) -> int:
        """Drop already-exited procs from the registry. Returns count
        removed. Optional housekeeping — kill_all is safe regardless."""
        removed = 0
        with self._lock:
            still = []
            for p in self._procs:
                try:
                    if p.poll() is None:
                        still.append(p)
                    else:
                        removed += 1
                except Exception:
                    still.append(p)
            self._procs = still
        return removed

    def alive_count(self) -> int:
        """Diagnostic: number of currently-tracked, still-running procs."""
        with self._lock:
            return sum(1 for p in self._procs if p.poll() is None)

    def kill_all(self, timeout: float = 5.0) -> int:
        """Terminate every tracked process. Returns count terminated.

        Sends terminate, waits up to `timeout` total, then kills any
        survivors. Used in main.py's shutdown path.
        """
        with self._lock:
            procs = list(self._procs)
            self._procs.clear()
        if not procs:
            return 0
        deadline = time.time() + max(0.0, timeout)
        for p in procs:
            try:
                if p.poll() is None:
                    p.terminate()
            except Exception as e:
                _log.debug("swallowed: %s", e)
        for p in procs:
            try:
                remaining = max(0.05, deadline - time.time())
                p.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                try:
                    p.kill()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            except Exception as e:
                _log.debug("swallowed: %s", e)
        return len(procs)


# Module-level singleton — the rest of the codebase imports this.
PROCESS_REGISTRY = ProcessRegistry()


# ── yt-dlp locator (re-exports the legacy one for now) ───────────────

def _exe_dir_candidates() -> list[Path]:
    """Paths to check next to the YTArchiver executable. Frozen builds
    place yt-dlp.exe alongside YTArchiver.exe, not in cwd — when the user
    launches a shortcut, cwd is wherever the shortcut lives (audit:
    process_runner.py:148-164). Path(sys.executable).parent catches both
    the frozen exe case and the python script + venv case.
    """
    out: list[Path] = []
    try:
        out.append(Path(sys.executable).resolve().parent)
    except Exception:
        pass
    # PyInstaller's _MEIPASS is the runtime unpack dir (read-only); we
    # don't expect yt-dlp.exe THERE, but the directory containing the
    # exe IS the parent of _MEIPASS, which sys.executable already
    # returns. So no extra _MEIPASS check needed.
    return out


def find_yt_dlp() -> str | None:
    """Locate yt-dlp.exe. Identical behavior to sync.find_yt_dlp but
    available without importing sync (which pulls in heavy deps).
    Result is NOT cached here — each caller pays one shutil.which.
    A future patch can add caching to the runner instance."""
    p = shutil.which("yt-dlp") or shutil.which("yt-dlp.exe")
    if p:
        return p
    candidates = [
        *( _exe_dir / "yt-dlp.exe" for _exe_dir in _exe_dir_candidates() ),
        Path.cwd() / "yt-dlp.exe",
        Path(__file__).resolve().parent.parent / "yt-dlp.exe",
        Path.home() / "Desktop" / "yt-dlp.exe",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return None


def find_ffprobe() -> str | None:
    """Locate ffprobe — PATH first, then sibling-of-app dir."""
    p = shutil.which("ffprobe") or shutil.which("ffprobe.exe")
    if p:
        return p
    candidates = [
        *( _exe_dir / "ffprobe.exe" for _exe_dir in _exe_dir_candidates() ),
        Path.cwd() / "ffprobe.exe",
        Path(__file__).resolve().parent.parent / "ffprobe.exe",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return None


# ── YtDlpRunner ──────────────────────────────────────────────────────

# Type alias for the cookie-provider callback. Returns a list of yt-dlp
# args (e.g. ["--cookies-from-browser", "firefox"]) or empty list. The
# legacy `sync._find_cookie_source` matches this signature.
CookieProvider = Callable[[], list[str]]


class YtDlpRunner:
    """Single source of truth for yt-dlp invocations.

    Use one instance app-wide (typically attached to the Api class).
    All call sites flow through `build_argv` for consistent flags,
    then choose `run_capture` (for probe-style short-lived calls)
    or `run_streaming` (for long-running passes whose stdout the
    caller wants line-by-line).

    The constructor takes a `cookie_provider` callable that returns
    the yt-dlp cookie args (so this module stays independent of
    `sync._find_cookie_source` while still using its result).
    """

    def __init__(self,
                 cookie_provider: CookieProvider | None = None,
                 registry: ProcessRegistry | None = None,
                 binary_finder: Callable[[], str | None] = find_yt_dlp):
        # Default to an empty-args lambda — `list` as a type works by
        # coincidence (list() returns []) but is type-confusing if a
        # future caller passes a non-callable (audit:
        # process_runner.py:208).
        self._cookies = cookie_provider or (lambda: [])
        self._registry = registry or PROCESS_REGISTRY
        self._binary_finder = binary_finder
        self._binary_cached: str | None = None
        self._binary_lock = threading.Lock()

    def binary(self) -> str | None:
        """Return the yt-dlp executable path, cached after first call."""
        with self._binary_lock:
            if self._binary_cached:
                return self._binary_cached
            p = self._binary_finder()
            self._binary_cached = p
            return p

    def reset_binary_cache(self) -> None:
        """Forget the cached executable path (e.g. after install update)."""
        with self._binary_lock:
            self._binary_cached = None

    def build_argv(self, *extra: str,
                   include_cookies: bool = True,
                   include_quiet: bool = True) -> list[str]:
        """Construct a yt-dlp argv. Patterns shared by all callers go
        here; per-call flags come in via `*extra`.

        Defaults applied:
          --no-warnings  (always, unless include_quiet=False)
          --no-progress  (always for non-streaming calls)
          cookie args from cookie_provider (if include_cookies)

        Returns [] if yt-dlp not locatable.
        """
        binary = self.binary()
        if not binary:
            return []
        argv: list[str] = [binary]
        if include_quiet:
            argv.append("--no-warnings")
        if include_cookies:
            try:
                argv.extend(self._cookies() or [])
            except Exception as e:
                _log.debug("swallowed: %s", e)
        argv.extend(extra)
        return argv

    def run_capture(self, argv: Iterable[str],
                    *, timeout: float = 30.0,
                    extra_env: dict | None = None
                    ) -> tuple[int, str, str]:
        """Run yt-dlp synchronously, capture stdout+stderr. Returns
        (returncode, stdout_str, stderr_str).

        On launch failure or timeout, returns (-1, "", error_message).
        Always registers the proc with the global registry so an app
        shutdown mid-call doesn't leak.
        """
        argv = list(argv)
        if not argv:
            return -1, "", "yt-dlp not found"
        try:
            proc = subprocess.Popen(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                startupinfo=make_startupinfo(),
                creationflags=subprocess_creationflags(),
                env=utf8_env(extra_env or None),
            )
        except OSError as e:
            return -1, "", f"launch failed: {e}"
        self._registry.register(proc)
        try:
            try:
                stdout, stderr = proc.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                except Exception:
                    pass
                # Bound the post-kill drain too. If kill failed silently
                # (e.g. AV injection holding the process alive), the
                # un-timeouted communicate would hang the calling thread
                # forever.
                try:
                    proc.communicate(timeout=5)
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                # Return an empty stdout instead of the partial buffer —
                # caller code that checks stdout first would otherwise
                # parse partial output as a valid result when stderr
                # says "timeout".
                return -1, "", "timeout"
        finally:
            self._registry.unregister(proc)
        return proc.returncode, stdout or "", stderr or ""

    def run_streaming(self, argv: Iterable[str],
                      *, on_stdout_line: Callable[[str], None] | None = None,
                      cancel_event: threading.Event | None = None,
                      extra_env: dict | None = None
                      ) -> tuple[int, list[str]]:
        """Run yt-dlp and stream stdout line by line via `on_stdout_line`.

        Used for long-running passes (channel sync) where the caller
        wants to react to each progress line as it arrives. If
        `cancel_event` fires, the process is terminated.

        Stderr is also drained on a background thread (last 200 lines
        captured for the return tuple's diagnostic list).

        Returns (returncode, stderr_tail).
        """
        argv = list(argv)
        if not argv:
            return -1, ["yt-dlp not found"]
        try:
            proc = subprocess.Popen(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                startupinfo=make_startupinfo(),
                creationflags=subprocess_creationflags(),
                env=utf8_env(extra_env or None),
            )
        except OSError as e:
            return -1, [f"launch failed: {e}"]
        self._registry.register(proc)
        from collections import deque
        stderr_tail: deque = deque(maxlen=200)

        def _drain_stderr():
            try:
                if proc.stderr is None:
                    return
                for ln in iter(proc.stderr.readline, ""):
                    if not ln:
                        break
                    stderr_tail.append(ln.rstrip())
            except Exception as e:
                _log.debug("swallowed: %s", e)

        t = threading.Thread(target=_drain_stderr, daemon=True,
                             name="yta-ytdlp-stderr")
        t.start()
        try:
            if proc.stdout is not None:
                for line in iter(proc.stdout.readline, ""):
                    if cancel_event is not None and cancel_event.is_set():
                        try:
                            proc.terminate()
                        except Exception:
                            pass
                        break
                    if on_stdout_line:
                        try:
                            on_stdout_line(line.rstrip("\n"))
                        except Exception as e:
                            _log.debug("swallowed: %s", e)
            # Full terminate→wait→kill→wait cleanup. Previously after
            # wait timeout we'd terminate() but never wait/kill again,
            # leaving a process that ignored SIGTERM running until app
            # shutdown's kill_all reaped it.
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    proc.terminate()
                except Exception:
                    pass
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    try:
                        proc.kill()
                        proc.wait(timeout=2)
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
        finally:
            self._registry.unregister(proc)
            # Join the stderr drain thread so its appends to
            # stderr_tail can't race the list(stderr_tail) snapshot
            # below. Best-effort; thread is daemon so it'll die with
            # the process anyway if join times out.
            try:
                t.join(timeout=2.0)
            except Exception as e:
                _log.debug("swallowed: %s", e)
        return proc.returncode if proc.returncode is not None else -1, list(stderr_tail)


# ── FfmpegRunner ─────────────────────────────────────────────────────

class FfmpegRunner:
    """Same shape as YtDlpRunner but for ffmpeg / ffprobe.

    Less consolidated for now — compress.py keeps its own ffmpeg Popen
    because of the streaming-progress requirement. This class exists
    for the simpler ffprobe-style probes scattered through the
    codebase (duration probes, codec detection, etc.).
    """

    def __init__(self,
                 registry: ProcessRegistry | None = None,
                 ffprobe_finder: Callable[[], str | None] = find_ffprobe):
        self._registry = registry or PROCESS_REGISTRY
        self._ffprobe_finder = ffprobe_finder
        self._ffprobe_cached: str | None = None
        self._lock = threading.Lock()

    def ffprobe(self) -> str | None:
        with self._lock:
            if self._ffprobe_cached:
                return self._ffprobe_cached
            p = self._ffprobe_finder()
            self._ffprobe_cached = p
            return p

    def probe_capture(self, argv: Iterable[str],
                      *, timeout: float = 20.0
                      ) -> tuple[int, str, str]:
        """Run an ffprobe argv (full path included) and capture output.
        Returns (rc, stdout, stderr).
        """
        argv = list(argv)
        if not argv:
            return -1, "", "ffprobe not found"
        try:
            proc = subprocess.Popen(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                startupinfo=make_startupinfo(),
                creationflags=subprocess_creationflags(),
            )
        except OSError as e:
            return -1, "", f"launch failed: {e}"
        self._registry.register(proc)
        try:
            try:
                out, err = proc.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                except Exception:
                    pass
                out, err = proc.communicate()
                return -1, out or "", "timeout"
        finally:
            self._registry.unregister(proc)
        return proc.returncode, out or "", err or ""
