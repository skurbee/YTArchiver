"""Shared subprocess lifecycle primitives for yt-dlp and ffmpeg.

`ProcessRegistry` tracks child processes for deterministic shutdown.
`YtDlpRunner` and `FfmpegRunner` provide consistent command execution,
environment handling, cancellation, and registry integration. Callers that
need specialized streaming behavior may register subprocesses directly.

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
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path

from . import youtube_traffic
from .log import get_logger
from .subprocess_util import (
    make_startupinfo,
    subprocess_creationflags,
    utf8_env,
)

_log = get_logger(__name__)


class StreamingRunResult:
    """Backward-compatible result for YtDlpRunner.run_streaming."""

    __slots__ = ("returncode", "stderr_tail", "cancelled")

    def __init__(self, returncode: int, stderr_tail: list[str],
                 cancelled: bool = False):
        self.returncode = returncode
        self.stderr_tail = stderr_tail
        self.cancelled = cancelled

    def __iter__(self):
        yield self.returncode
        yield self.stderr_tail

    def __len__(self):
        return 2

    def __getitem__(self, idx):
        return (self.returncode, self.stderr_tail)[idx]


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
            still = []
            for existing in self._procs:
                try:
                    if existing.poll() is None:
                        still.append(existing)
                except Exception:
                    still.append(existing)
            self._procs = still
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

        Sends terminate to each still-running process, waits up to
        `timeout` for that process, then kills it if it is still alive.
        Used in main.py's shutdown path.
        """
        with self._lock:
            procs = list(self._procs)
            self._procs.clear()
        if not procs:
            return 0
        per_proc_timeout = max(0.0, timeout)
        terminated = 0
        for p in procs:
            try:
                if p.poll() is not None:
                    continue
                terminated += 1
                p.terminate()
                try:
                    p.wait(timeout=per_proc_timeout)
                except subprocess.TimeoutExpired:
                    if p.poll() is None:
                        try:
                            p.kill()
                        except Exception as e:
                            _log.debug("swallowed: %s", e)
                    try:
                        p.wait(timeout=0.25)
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
            except Exception as e:
                _log.debug("swallowed: %s", e)
        return terminated


# Module-level singleton — the rest of the codebase imports this.
PROCESS_REGISTRY = ProcessRegistry()


# ── yt-dlp update launch gate ─────────────────────────────────────────────

class YtDlpUpdateGate:
    """Atomically hand the yt-dlp executable to its self-updater.

    Ordinary launches briefly enter ``launch_slot`` while they create and
    register a child process.  The updater reserves the exclusive side only
    after no launch is between those two steps, then re-checks the process
    registry while new launches are blocked.  This closes the otherwise tiny
    check-then-spawn race without serializing normal yt-dlp processes.
    """

    def __init__(self):
        self._condition = threading.Condition()
        self._launchers = 0
        self._update_reserved = False

    @contextmanager
    def launch_slot(self):
        with self._condition:
            while self._update_reserved:
                self._condition.wait()
            self._launchers += 1
        try:
            yield
        finally:
            with self._condition:
                self._launchers = max(0, self._launchers - 1)
                self._condition.notify_all()

    def try_reserve_update(self, busy_check: Callable[[], bool]) -> bool:
        """Reserve exclusive launch access when the app is still idle."""
        with self._condition:
            if self._update_reserved or self._launchers:
                return False
            self._update_reserved = True
        try:
            if busy_check():
                self.release_update()
                return False
        except Exception:
            self.release_update()
            raise
        return True

    def release_update(self) -> None:
        with self._condition:
            if not self._update_reserved:
                return
            self._update_reserved = False
            self._condition.notify_all()

    def update_reserved(self) -> bool:
        with self._condition:
            return self._update_reserved


YTDLP_UPDATE_GATE = YtDlpUpdateGate()


def popen_ytdlp(*args, registry: ProcessRegistry | None = None, **kwargs):
    """Launch and register yt-dlp as one update-gate transaction."""
    target_registry = registry or PROCESS_REGISTRY
    with YTDLP_UPDATE_GATE.launch_slot():
        proc = subprocess.Popen(*args, **kwargs)
        target_registry.register(proc)
    return proc


def run_ytdlp(*args, **kwargs):
    """Run a synchronous yt-dlp probe without racing self-update."""
    with YTDLP_UPDATE_GATE.launch_slot():
        return subprocess.run(*args, **kwargs)


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


@lru_cache(maxsize=1)
def _find_yt_dlp_cached() -> str | None:
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


@lru_cache(maxsize=1)
def _find_ffprobe_cached() -> str | None:
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

def find_yt_dlp() -> str | None:
    """Locate yt-dlp.exe, cached process-wide."""
    return _find_yt_dlp_cached()


def find_ffprobe() -> str | None:
    """Locate ffprobe, cached process-wide."""
    return _find_ffprobe_cached()


def reset_process_binary_caches() -> None:
    """Clear module-level process binary lookup caches."""
    _find_yt_dlp_cached.cache_clear()
    _find_ffprobe_cached.cache_clear()


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
        self._cookies = cookie_provider or list
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
        reset_process_binary_caches()

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
                    extra_env: dict | None = None,
                    traffic_kind: str = "yt_dlp_probe",
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
        permission = youtube_traffic.acquire(traffic_kind)
        if not permission.get("ok"):
            reason = (
                "cancelled" if permission.get("cancelled")
                else permission.get("error") or "YouTube traffic budget blocked"
            )
            return -1, "", str(reason)
        try:
            proc = popen_ytdlp(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                startupinfo=make_startupinfo(),
                creationflags=subprocess_creationflags(),
                env=utf8_env(extra_env or None),
                registry=self._registry,
            )
        except OSError as e:
            return -1, "", f"launch failed: {e}"
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
        try:
            from .youtube_session import handle_youtube_failure_text
            handle_youtube_failure_text(
                "\n".join((stdout or "", stderr or "")),
                context="running yt-dlp",
            )
        except Exception as e:
            _log.debug("YtDlpRunner session guard failed: %s", e)
        return proc.returncode, stdout or "", stderr or ""

    def run_streaming(self, argv: Iterable[str],
                      *, on_stdout_line: Callable[[str], None] | None = None,
                      cancel_event: threading.Event | None = None,
                      extra_env: dict | None = None,
                      traffic_kind: str = "yt_dlp_download",
                      ) -> StreamingRunResult:
        """Run yt-dlp and stream stdout line by line via `on_stdout_line`.

        Used for long-running passes (channel sync) where the caller
        wants to react to each progress line as it arrives. If
        `cancel_event` fires, the process is terminated.

        Stderr is also drained on a background thread (last 200 lines
        captured for the return tuple's diagnostic list).

        Returns a StreamingRunResult. It still unpacks as
        (returncode, stderr_tail), and exposes `.cancelled` so callers can
        distinguish user cancellation from a launch/process failure.
        """
        argv = list(argv)
        if not argv:
            return StreamingRunResult(-1, ["yt-dlp not found"])
        permission = youtube_traffic.acquire(
            traffic_kind, cancel_event=cancel_event)
        if not permission.get("ok"):
            reason = (
                "cancelled" if permission.get("cancelled")
                else permission.get("error") or "YouTube traffic budget blocked"
            )
            return StreamingRunResult(
                -1, [str(reason)], cancelled=bool(
                    permission.get("cancelled")))
        try:
            proc = popen_ytdlp(
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
                registry=self._registry,
            )
        except OSError as e:
            return StreamingRunResult(-1, [f"launch failed: {e}"])
        from collections import deque
        stderr_tail: deque = deque(maxlen=200)
        cancelled = False

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
                        cancelled = True
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
                if cancel_event is not None and cancel_event.is_set():
                    cancelled = True
                    if proc.poll() is None:
                        try:
                            proc.terminate()
                        except Exception:
                            pass
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
        rc = proc.returncode if proc.returncode is not None else -1
        _stderr_result = list(stderr_tail)
        try:
            from .youtube_session import handle_youtube_failure_text
            handle_youtube_failure_text(
                "\n".join(_stderr_result), context="running yt-dlp")
        except Exception as e:
            _log.debug("YtDlpRunner streaming session guard failed: %s", e)
        return StreamingRunResult(
            rc, _stderr_result, cancelled=cancelled)


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
