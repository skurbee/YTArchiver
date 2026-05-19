"""
transcribe.punct_manager — PunctuationManager subprocess wrapper.

Extracted from transcribe/core.py (Patch 16, v71.8). Owns the
persistent `punct_worker.py` subprocess that restores punctuation +
capitalization on Whisper's raw lowercase output. The subprocess is
started lazily on first `.punctuate()` call and kept alive between
calls (model load is the expensive part).

Public surface (re-exported via transcribe/__init__.py for back-compat):
    PunctuationManager
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
from pathlib import Path

from ..log import get_logger
from ..log_stream import LogStreamer
from ..subprocess_util import make_startupinfo as _make_startupinfo
from .helpers import find_python311

_log = get_logger(__name__)
_startupinfo = _make_startupinfo()


class PunctuationManager:
    """Persistent punctuation-restoration subprocess. Cheap when idle.

    Call `punctuate(text)` with the raw whisper output; returns the
    capitalised / punctuated version. Subprocess boots on first call
    and stays alive between calls.
    """

    def __init__(self, stream: LogStreamer):
        self._stream = stream
        self._proc: subprocess.Popen | None = None
        # reentrant lock so punctuate()'s exception handler
        # can call _stop() without self-deadlocking. Old non-reentrant
        # Lock caused a wedged punct model to freeze every subsequent
        # transcribe (the exception handler in punctuate tried to
        # re-acquire the lock it already held via the outer with block).
        self._lock = threading.RLock()
        self._starting = False
        # Patch 19 fix (v68.2): this file moved from backend/transcribe.py
        # to backend/transcribe/legacy.py. The worker script lives at
        # backend/punct_worker.py (and is bundled into <bundle>/backend/
        # by PyInstaller), so go up one more level.
        self._worker_script = Path(__file__).resolve().parent.parent / "punct_worker.py"
        self._python311: str | None = None
        # Set by `_transcribe_one` before each punctuate() call so the
        # "Loading punctuation model..." emit carries the current
        # video's per-job inplace tag. Without it that line shares the
        # generic "whisper" kind and can get stomped by other jobs.
        self._job_tag: str = ""
        # Bug [43]: track whether the most recent punctuate() call hit
        # the per-call timeout, separately from other failures. Lets
        # callers append "+TIMEOUT" to the source tag so the user can
        # see at-a-glance why the transcript is unpunctuated.
        self.last_was_timeout: bool = False

    def is_available(self) -> bool:
        return self._worker_script.exists() and (self._python311 or find_python311()) is not None

    def _start(self) -> bool:
        with self._lock:
            if self._proc is not None and self._proc.poll() is None:
                return True
            if self._starting:
                return False
            self._starting = True
        try:
            py = self._python311 or find_python311()
            if not py:
                self._stream.emit_error("Punctuation: Python 3.11 not found.")
                return False
            self._python311 = py
            # Per-job tag so this line joins the current video's
            # inplace family and gets replaced by "Adding
            # punctuation..." → then by the final "— ✓ Transcription"
            # done line. Falls back to the generic kind if no tag
            # was set.
            _tags = ["transcribe_using"]
            _tags.append(self._job_tag if self._job_tag else "whisper_progress")
            self._stream.emit([[
                " Loading punctuation model...\n", _tags]])
            env = os.environ.copy()
            self._proc = subprocess.Popen(
                [py, str(self._worker_script)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, bufsize=1, startupinfo=_startupinfo, env=env,
            )
            # Wait for ready
            ready: list[str | None] = [None]
            def _read():
                try:
                    ready[0] = self._proc.stdout.readline().strip()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            t = threading.Thread(target=_read, daemon=True)
            t.start()
            t.join(timeout=300)
            if t.is_alive():
                self._stream.emit_error("Punctuation model timed out loading.")
                self._stop()
                return False
            line = ready[0]
            if not line:
                return False
            info = json.loads(line)
            if info.get("status") != "ready":
                self._stream.emit_error(f"Punct start: {info}")
                self._stop()
                return False
            # Simple mode hides this \u2014 it's diagnostic chrome, not a
            # user-facing milestone, and it can't carry the active job's
            # `tx_done_<vid>` marker (this subprocess is shared across
            # jobs and doesn't know which video triggered the start).
            # In Simple mode that meant the line landed at the log bottom
            # and stuck around as orphan text after the actual " \u2014 \u2713
            # Transcription (\u2026)" done line replaced its slot under the
            # video row. Emit only in Verbose so the developer can still
            # see "model loaded (CUDA)" when debugging.
            if not getattr(self._stream, "simple_mode", True):
                self._stream.emit_text(
                    f" \u2014 \u2713 Punctuation model loaded "
                    f"({info.get('device', '?').upper()}).",
                    "simpleline_green")
            return True
        except Exception as e:
            self._stream.emit_error(f"Failed to start punctuation: {e}")
            self._stop()
            return False
        finally:
            with self._lock:
                self._starting = False

    def _stop(self):
        with self._lock:
            if self._proc is not None:
                try:
                    self._proc.kill()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                self._proc = None

    def punctuate(self, text: str, timeout_sec: float = 60.0) -> str:
        """Run text through the punctuation model. Returns original text on failure.

        timeout_sec is now actually honored. The stdout
        readline happens in a daemon thread with join(timeout); if the
        subprocess wedges, the call returns the raw text and kills the
        subprocess so the NEXT call restarts it clean. Previously the
        blocking readline could hang forever, wedging every subsequent
        transcription because the lock was held the whole time.
        """
        if not text or len(text.split()) < 3:
            return text
        # Bug [43]: reset timeout flag at the start of each call so the
        # caller can read it after this call returns.
        self.last_was_timeout = False
        if self._proc is None or self._proc.poll() is not None:
            if not self._start():
                return text
        try:
            req = json.dumps({"text": text}) + "\n"
            with self._lock:
                self._proc.stdin.write(req)
                self._proc.stdin.flush()
                # Read in a helper thread so we can bound the wait.
                _result: dict[str, Any] = {"line": None, "err": None}
                def _reader():
                    try:
                        _result["line"] = self._proc.stdout.readline()
                    except Exception as _re:
                        _result["err"] = _re
                _t = threading.Thread(target=_reader, daemon=True)
                _t.start()
                _t.join(timeout_sec)
                if _t.is_alive():
                    # Wedged — kill subprocess to unblock the reader
                    # thread (its readline will return empty once stdout
                    # closes) and treat as failed pass.
                    try:
                        self._proc.kill()
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                    self._proc = None
                    self.last_was_timeout = True
                    self._stream.emit_dim(
                        f" (punctuation timed out after {timeout_sec:.0f}s)")
                    return text
                line = (_result.get("line") or "").strip()
            if not line:
                return text
            resp = json.loads(line)
            if resp.get("status") == "ok":
                return resp.get("text", text) or text
            return text
        except Exception as e:
            self._stream.emit_dim(f" (punctuation skipped: {e})")
            # Subprocess may be wedged — kill so next call restarts cleanly.
            # Safe to call _stop() now that self._lock is an RLock.
            self._stop()
            return text
