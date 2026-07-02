"""
Transcribe — manages the persistent faster-whisper subprocess + a GPU queue.

Architecture (mirrors YTArchiver.py:9102 _start_whisper_process):
  - A single long-lived Python 3.11 subprocess runs `whisper_worker.py`
  - Model loads once (can be several GB / many seconds on first run)
  - Requests queued in memory; worker processes one at a time
  - Progress + results stream via JSON on stdout

Output file layout (must match YTArchiver.py for drop-in replacement):
  {ch_name} Transcript.txt (no split)
  {year}/{ch_name} {year} Transcript.txt (year-split)
  {year}/{MM Month}/{ch_name} {Month} {YY} Transcript.txt (year+month split)

  Entry format inside the .txt file (triple-newline separated):
    ===({title}), ({MM.DD.YYYY}), ({H:MM:SS}), ({SOURCE})===
    {transcript text}

  Hidden sidecar: .{ch_name} ... Transcript.jsonl next to the .txt, one
  JSON per segment with long-form keys:
    {"video_id":..., "title":..., "start":..., "end":..., "text":...,
     "words":[{"w":..., "s":..., "e":...}, ...]}
"""

from __future__ import annotations

import json
import os
import queue
import re
import shutil
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..log_stream import LogStreamer

# startupinfo now comes from subprocess_util (one
# source of truth shared with compress.py and sync.py).
from ..subprocess_util import make_startupinfo as _make_startupinfo

__all__ = [
    "ytarchiver_config_output_dir",
    "find_python311",
    "PunctuationManager",
    "TranscribeManager",
]

_startupinfo = _make_startupinfo()


# ── OLD YTArchiver-compatible transcript file helpers ──────────────────
# These mirror the file layout + content format the legacy YTArchiver.py
# uses so we're a bit-for-bit drop-in replacement. Do NOT change these
# names or formats — OLD's scan/match logic depends on them exactly.

# Shared with metadata.py + reorg.py — see backend.utils.MONTH_FOLDERS.

from ..log import get_logger

_log = get_logger(__name__)


def _hide_per_video_transcript_txt_if_needed(video_path: str,
                                             txt_path: str) -> None:
    """Hide loose/manual per-video Transcript.txt sidecars.

    Channel aggregate transcripts are intentionally visible, but manual single
    videos write `<video stem> Transcript.txt` next to the media file. Those
    should behave like metadata/jsonl sidecars in Explorer.
    """
    try:
        video_dir = os.path.normcase(os.path.normpath(
            os.path.dirname(video_path)))
        txt_dir = os.path.normcase(os.path.normpath(os.path.dirname(txt_path)))
        if video_dir != txt_dir:
            return
        stem = os.path.splitext(os.path.basename(video_path))[0]
        stem = re.sub(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$", "", stem) or stem
        expected = f"{stem} Transcript.txt"
        if os.path.normcase(os.path.basename(txt_path)) != os.path.normcase(expected):
            return
        from .paths import _hide_file_win
        _hide_file_win(txt_path)
    except Exception as e:
        _log.debug("manual transcript txt hide failed: %s", e)


# path + format + hide helpers moved to
# transcribe/paths.py. Re-imported here so internal calls and external
# `from backend.transcribe import _foo` callers keep working.
from .paths import (  # noqa: F401
    _get_jsonl_sidecar,
    _get_transcript_filename,
)

# Patch 16 (v71.8): pure helpers + PunctuationManager extracted to
# helpers.py + punct_manager.py. Re-imported here so this module's
# namespace + the package __init__ surface keep the previously-public
# names visible.
from .helpers import (  # noqa: F401
    _CHUNK_DURATION_SECS,
    _CHUNK_MIN_DURATION,
    _CHUNK_OVERLAP_SECS,
    _bump_transcription_pending,
    _extract_video_id,
    _ffprobe_duration,
    _lookup_channel,
    _norm_title,
    _resolve_transcript_paths,
    _scan_existing_transcript_titles,
    find_python311,
    ytarchiver_config_output_dir,
)
from .punct_manager import PunctuationManager  # noqa: F401

# ── Patch 19 phase T1 (v68.9): file writers moved to transcribe_files.py ─
# Internal callers (_transcribe_one, _write_outputs, retranscribe flows)
# expect these names in this module's namespace.
from .transcribe_files import (
    _HEADER_RE,  # noqa: F401
    _jsonl_text_candidates_from_bytes,
    _replace_jsonl_entry,
    _replace_txt_entry,
    _write_jsonl_entry,
    _write_transcript_entry,
)

# ── Patch 19 phase T2 (v68.9): VTT path moved to transcribe_vtt.py ───
from .transcribe_vtt import (  # noqa: F401  (re-exports for backend.transcribe surface)
    _parse_vtt,
    _try_auto_captions,
)

# ── Manager ────────────────────────────────────────────────────────────

def _pending_journal_path() -> Path:
    """Where the pending-transcribe journal lives.

    Matches YTArchiver.py:14650 pattern: <channel_folder>/_whisper_pending.json.
    We keep a global one at APPDATA/ytarchiver_pending_transcribe.json so
    the manager can recover ALL queued work across channels on restart.
    """
    from ..ytarchiver_config import APP_DATA_DIR
    return APP_DATA_DIR / "ytarchiver_pending_transcribe.json"


def _rough_duration_from_size(path: str) -> float:
    """Estimate duration from size using the existing ~50 MB/hour heuristic."""
    try:
        size = os.path.getsize(path)
    except OSError:
        return 0.0
    if size <= 0:
        return 0.0
    return max(1.0, (float(size) / (50 * 1024 * 1024)) * 3600.0)


def _punct_align_segments(punct_text: str, segments: list) -> None:
    """Re-derive per-segment punctuated text from the already-punctuated whole text.

    Avoids N subprocess round-trips by word-count-aligning the punctuated
    output back to each segment's raw token count (T150). Falls back silently
    on word-count mismatch (rare — happens when the model splits/merges a
    contraction).
    """
    if not punct_text or not segments:
        return
    raw_counts = [len((seg.get("t") or "").split()) for seg in segments]
    total_raw = sum(raw_counts)
    punct_words = punct_text.split()
    if total_raw != len(punct_words):
        return
    idx = 0
    for seg, n in zip(segments, raw_counts):
        if n >= 3:
            seg["t"] = " ".join(punct_words[idx:idx + n])
        idx += n


def _build_transcription_done_segments(job: dict, title: str, channel: str,
                                       detail_text: str, *,
                                       dim_tags, em_tags, lbl_tags,
                                       txt_tags, detail_tags) -> list:
    """Build the shared "— ✓ Transcription [— title — channel] (detail)"
    done-line segments used by BOTH the single-pass and chunked paths.

    The two paths previously duplicated this ~20-line construction
    near-verbatim (T167). They differ only in (a) their tag families —
    single-pass threads a tx_done_<vid> inplace marker, chunked uses the
    job_tag — and (b) the trailing detail text, so those are passed in.
    The lead indent (6 spaces under a [Dwnld] row vs 1 space standalone)
    and the standalone title/channel splice rule are identical and live
    here. Pure + unit-testable; no I/O.
    """
    lead = "      " if job.get("from_download") else " "
    segs = [
        [lead, dim_tags],
        ["— ✓ ", em_tags],
        ["Transcription", lbl_tags],
    ]
    # When not part of a download flow (sync emits a "Downloaded — title —
    # channel" line just above), splice the title/channel into the done
    # line so a standalone transcribe is identifiable on its own.
    if not job.get("from_download"):
        seg_title = (title or "").strip()
        seg_channel = (channel or "").strip()
        if seg_title:
            segs.append([" — ", dim_tags])
            segs.append([seg_title, txt_tags])
            if seg_channel:
                segs.append([" — ", dim_tags])
                segs.append([seg_channel, txt_tags])
    segs.append([f" ({detail_text})\n", detail_tags])
    return segs


class TranscribeManager:
    """Manages the whisper subprocess + a GPU queue."""

    def current_model(self) -> str:
        """Return the whisper model this manager is currently using for
        new jobs. Public accessor — main.py's `transcribe_current_model`
        used to reach into `self._model` directly, which would silently
        break on any future refactor of the internals.
        """
        return self._model

    def __init__(self, stream: LogStreamer, model: str = "large-v3"):
        self._stream = stream
        self._model = model
        self._proc: subprocess.Popen | None = None
        self._proc_lock = threading.Lock()
        self._line_queue: queue.Queue | None = None
        self._starting = False
        self._reader_thread: threading.Thread | None = None
        self._stderr_drain_thread: threading.Thread | None = None
        self._stderr_buffer = None
        self._python311 = find_python311()
        # Patch 19 fix (v68.2): this file moved from backend/transcribe.py
        # to backend/transcribe/legacy.py. The worker script is bundled
        # at <bundle>/backend/whisper_worker.py per the PyInstaller spec,
        # so go up one more level.
        self._worker_script = Path(__file__).resolve().parent.parent / "whisper_worker.py"
        # Optional punctuation model — lazy-loaded, reused across jobs.
        # Routed through the process-singleton getter so the
        # Restore-Punctuation pass and the live transcribe worker
        # share one subprocess + VRAM allocation (audit: H44).
        try:
            from .punct_manager import get_shared_punct_manager
            self._punct = get_shared_punct_manager(stream)
        except Exception:
            self._punct = PunctuationManager(stream)
        self._punctuate_enabled = True

        # Queue of jobs. Each job = {path, title, cb, cancel_event}
        self._jobs: list[dict[str, Any]] = []
        self._jobs_lock = threading.Lock()
        # flipped True when OOM forces a subprocess into
        # CPU mode. After the next successful transcribe completes,
        # we reset WHISPER_DEVICE back to "cuda" and force a restart
        # so subsequent jobs try GPU again. Without this flag, one
        # OOM early in a session stuck every later video in slow CPU
        # transcription for the rest of the run.
        self._cpu_fallback_active = False
        # Deferred-swap flag: swap_model() sets this and the worker loop-top
        # applies it when idle. Declared here so it isn't lazily materialized
        # via getattr on first swap (audit r2).
        self._pending_model_restart = False
        self._worker_thread: threading.Thread | None = None
        self._cancel_all = threading.Event()
        self._paused = threading.Event()
        self._current_job: dict[str, Any] | None = None
        # Per-batch stats for autorun_history [Trnscr] rows. Mirrors
        # YTArchiver.py:22575 _record_transcription — one row per channel
        # with done/err counts + elapsed time. Flushed when the worker
        # drains. Keyed by channel name.
        self._batch_stats: dict[str, dict[str, Any]] = {}
        # Compress jobs get their OWN stats dict — mixed into
        # _batch_stats they flushed as "N transcribed" rows and
        # inflated the consolidated [Dwnld] transcribe count.
        self._compress_stats: dict[str, dict[str, Any]] = {}
        # Reference to the shared QueueState. Attached by the app wrapper
        # after construction (main.py can't pass it in __init__ because
        # QueueState is constructed later). When None, the manager
        # maintains its own internal job list only — no UI popover sync.
        # When set, enqueue/worker mirror into queues.gpu + current_gpu
        # so the GPU Tasks popover shows what's pending / running.
        self._queues = None
        # Config-driven Auto gate. When autorun_gpu=False, new jobs sit
        # in the queue without firing. Checked each worker iteration so
        # toggling the Auto checkbox mid-pass takes effect between jobs.
        self._cfg_loader = None # set via attach_queues

    def _whisper_stderr_tail(self, max_lines: int = 8,
                             max_chars: int = 1200) -> str:
        try:
            lines = [str(ln).strip() for ln in list(self._stderr_buffer or [])
                     if str(ln).strip()]
        except Exception:
            return ""
        if not lines:
            return ""
        tail = "\n".join(lines[-max_lines:])
        return tail[-max_chars:]

    def _emit_whisper_stderr_tail(self,
                                  label: str = "Whisper stderr") -> None:
        tail = self._whisper_stderr_tail()
        if tail:
            self._stream.emit_dim(f" ({label}: {tail})")

    def _emit_whisper_traceback(self, msg: dict[str, Any]) -> None:
        trace = str(msg.get("traceback") or "").strip()
        if trace:
            self._stream.emit_dim(f" (Whisper traceback: {trace[-1200:]})")

    def _send_cancel_command(self) -> bool:
        with self._proc_lock:
            proc = self._proc
            if (proc is None or proc.poll() is not None
                    or proc.stdin is None or proc.stdin.closed):
                return False
            try:
                proc.stdin.write(json.dumps({"command": "cancel"}) + "\n")
                proc.stdin.flush()
                return True
            except Exception as e:
                _log.debug("whisper cancel command failed: %s", e)
                return False

    def _wait_for_cancel_ack(self, timeout: float = 5.0) -> bool:
        q = self._line_queue
        if q is None:
            return False
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                line = q.get(timeout=min(0.25, max(0.01, deadline - time.time())))
            except queue.Empty:
                continue
            if line is None:
                return False
            try:
                msg = json.loads(line.strip())
            except json.JSONDecodeError:
                continue
            status = msg.get("status")
            if status in {"cancelled", "ok", "error"}:
                return True
        return False

    def _graceful_cancel_current(self, timeout: float = 5.0) -> bool:
        if not self._send_cancel_command():
            return False
        return self._wait_for_cancel_ack(timeout=timeout)

    def _snapshot_worker_io(self):
        with self._proc_lock:
            return self._proc, self._line_queue

    def _wait_for_starting_subprocess(self, timeout: float = 600.0) -> bool:
        """Wait for a concurrent start_subprocess call to finish."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._proc_lock:
                if not self._starting:
                    return bool(
                        self._proc is not None
                        and self._proc.poll() is None)
            time.sleep(0.1)
        return False

    def attach_queues(self, queues, cfg_loader=None) -> None:
        """Connect this manager to the shared QueueState.
        `cfg_loader` is an optional callable returning the live config
        dict; used to read `autorun_gpu` each worker iteration so the
        Auto checkbox actually gates firing (not just display)."""
        self._queues = queues
        self._cfg_loader = cfg_loader

    def get_channel_batch_stats(self, channel_name: str) -> dict[str, int]:
        """Synchronous snapshot of this channel's transcription batch
        stats. Used by sync_channel at end-of-pass to fold transcribed
        counts into the consolidated activity-log [Dwnld] row — auto-
        captions typically complete during the download so the count
        is accurate by sync_channel's exit. Whisper may still be running.
        """
        s = self._batch_stats.get(channel_name) or {}
        return {"done": int(s.get("done", 0) or 0),
                "err": int(s.get("err", 0) or 0)}

    def consume_channel_batch_stats(self, channel_name: str) -> None:
        """Mark this channel's batch stats as already consumed by a
        sync-originated [Dwnld] row emission. Subsequent calls to
        _flush_batch_stats will skip it so the user doesn't see a
        duplicate [Trnscr] row for the same transcriptions.
        """
        try: self._batch_stats.pop(channel_name, None)
        except Exception as e: _log.debug("swallowed: %s", e)

    def _auto_enabled(self) -> bool:
        """True if the GPU Auto checkbox says "go". When False, the
        worker parks without popping the next job — tasks sit visible
        in the popover until the user re-enables Auto or clicks Start.
        Defaults to True when no config loader is attached (preserves
        legacy behavior for tests / preview mode)."""
        if self._cfg_loader is None:
            return True
        try:
            cfg = self._cfg_loader() or {}
            return bool(cfg.get("autorun_gpu", False))
        except Exception:
            return True

    # ── Lifecycle ────────────────────────────────────────────────────

    def is_available(self) -> bool:
        return bool(self._python311) and self._worker_script.exists()

    def swap_model(self, new_model: str) -> bool:
        """Change the whisper model. If the worker is running, stop it so
        the next job spins it back up with the new model. In-flight job is
        not aborted — it finishes on the current model, then the next job
        picks up the new one.
        """
        if not new_model:
            return False
        if new_model == self._model:
            return True  # already on this model; nothing to do
        self._model = new_model
        # Defer the subprocess restart to the worker thread's next idle
        # loop-top instead of stopping it from this (UI/bridge) thread.
        # Stopping here races _transcribe_one's read loop and kills an
        # in-flight job mid-transcription (audit: swap_model race). The flag
        # lets any in-flight job finish on the OLD model; the worker stops
        # the subprocess when idle and the next job reloads the new model.
        self._pending_model_restart = True
        # If the worker isn't running (idle, no queued jobs), the loop-top
        # won't apply the swap — stop the subprocess HERE so the old model
        # doesn't linger in VRAM until the next enqueue. Safe: no job is in
        # flight when the worker thread isn't alive (audit r2 idle-VRAM gap).
        _wt = getattr(self, "_worker_thread", None)
        if _wt is None or not _wt.is_alive():
            try:
                self._stop_subprocess()
                self._pending_model_restart = False
            except Exception as e:
                _log.debug("swallowed: %s", e)
        self._stream.emit_text(
            f" \u2014 Whisper model queued to swap to '{new_model}' "
            f"on next job.", "simpleline_blue")
        return True

    def start_subprocess(self, model: str | None = None) -> bool:
        """Start the persistent whisper worker. Returns True when ready."""
        wait_for_start = False
        with self._proc_lock:
            if self._proc is not None and self._proc.poll() is None:
                return True
            if self._starting:
                wait_for_start = True
            else:
                self._starting = True
                self._proc = None

        if wait_for_start:
            return self._wait_for_starting_subprocess()

        try:
            if not self._python311:
                self._stream.emit_error("Transcription requires Python 3.11. Install it from python.org.")
                return False
            m = model or self._model
            self._stream.emit_text(
                f" Transcribing — Loading Whisper model ({m}) on GPU...",
                "transcribe_using")

            env = os.environ.copy()
            env["WHISPER_MODEL"] = m
            # Honor a one-shot CPU-fallback on this instance instead
            # of mutating os.environ globally (audit: H51). The OOM
            # handler sets `self._cpu_fallback_active`; we read it
            # here so the next subprocess starts in CPU mode without
            # polluting process-wide env that any sibling subprocess
            # would inherit.
            if getattr(self, "_cpu_fallback_active", False):
                env["WHISPER_DEVICE"] = "cpu"
                env["WHISPER_COMPUTE"] = "default"
            else:
                env["WHISPER_DEVICE"] = "cuda"
                env["WHISPER_COMPUTE"] = "float16"

            # capture stderr so crashes during model load
            # or transcription land somewhere diagnosable. Previously
            # stderr was piped to DEVNULL, making CUDA driver stderr
            # messages invisible — the only crash signal was the
            # "ended unexpectedly" line from the parent, with no
            # detail about WHY. Parent now reads leftover stderr on
            # abnormal exit (see _stop_subprocess).
            self._proc = subprocess.Popen(
                [self._python311, str(self._worker_script)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, bufsize=1, startupinfo=_startupinfo, env=env,
            )

            # drain stderr on a background thread. Without
            # this, whisper subprocess can DEADLOCK when it writes enough
            # stderr (per-segment warnings, model load messages) to fill
            # the OS pipe buffer (~64KB on Windows) — whisper blocks on
            # write while we read only stdout. The drain thread keeps the
            # buffer empty AND captures the last 200 lines into a ring
            # buffer for inclusion in error reports.
            from collections import deque as _deque
            self._stderr_buffer = _deque(maxlen=200)
            _stderr_proc = self._proc
            _stderr_sink = self._stderr_buffer
            def _drain_stderr():
                try:
                    for ln in iter(_stderr_proc.stderr.readline, ""):
                        if not ln:
                            break
                        _stderr_sink.append(ln.rstrip())
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            self._stderr_drain_thread = threading.Thread(
                target=_drain_stderr, daemon=True,
                name="yta-whisper-stderr")
            self._stderr_drain_thread.start()

            # Wait for "ready" (model load can take minutes on first download)
            ready_result: list[str | None] = [None]
            def _read_ready():
                try:
                    ready_result[0] = self._proc.stdout.readline().strip()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            t = threading.Thread(target=_read_ready, daemon=True)
            t.start()
            t.join(timeout=600) # 10 min for model download + load
            if t.is_alive():
                self._stream.emit_error("Transcription took too long to start.")
                self._emit_whisper_stderr_tail()
                self._stop_subprocess()
                return False

            line = ready_result[0]
            if not line:
                self._stream.emit_error("Transcription tool didn't respond. Try again.")
                self._emit_whisper_stderr_tail()
                self._stop_subprocess()
                return False
            try:
                info = json.loads(line)
            except json.JSONDecodeError:
                # Keep raw `line` content but verbose-tag it so only
                # Verbose-mode users see the gibberish. Simple-mode
                # users get a cleaner one-liner.
                self._stream.emit_error("Transcription tool sent unexpected data — try again.")
                self._stream.emit([
                    ["   — ", ["dim"]],
                    [f"raw payload: {line[:200]}\n", ["dim"]],
                ])
                self._emit_whisper_stderr_tail()
                self._stop_subprocess()
                return False
            if info.get("status") != "ready":
                self._stream.emit_error("Transcription tool failed to initialize.")
                self._stream.emit([
                    ["   — ", ["dim"]],
                    [f"status: {info}\n", ["dim"]],
                ])
                self._emit_whisper_traceback(info)
                self._emit_whisper_stderr_tail()
                self._stop_subprocess()
                return False

            dev = info.get("device", "?").upper()
            # Verbose-only subprocess-spawn diagnostic. PRIMARY tag
            # must be `transcribe_using` (in VERBOSE_ONLY_TAGS) so
            # `_line_is_verbose_only` drops the whole line in Simple
            # mode. In Verbose mode it renders in the transcribe
            # color (blue). This line has no inplace marker so it
            # would otherwise land at whatever log position the
            # sync pass is at — typically under a later channel's
            # "no new videos" row, persisting there forever.
            self._stream.emit([
                [" \u2014 \u2713 ", "transcribe_using"],
                [f"Whisper model loaded ({m}, {dev}).\n", "transcribe_using"],
            ])
            if info.get("cuda_fallback_reason"):
                self._stream.emit_dim(
                    f" [CUDA fallback] Fell back to CPU: {info['cuda_fallback_reason']}")

            # Start the stdout reader thread
            self._line_queue = queue.Queue()
            proc_ref = self._proc
            def _reader(q=self._line_queue):
                try:
                    for ln in iter(proc_ref.stdout.readline, ""):
                        try:
                            q.put(ln)
                        except Exception:
                            break
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                try:
                    q.put(None)
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            self._reader_thread = threading.Thread(target=_reader, daemon=True)
            self._reader_thread.start()
            return True
        except Exception as e:
            self._stream.emit_error(f"Failed to start whisper: {e}")
            self._stop_subprocess()
            return False
        finally:
            with self._proc_lock:
                self._starting = False

    def _stop_subprocess(self, force: bool = False):
        with self._proc_lock:
            if self._proc is None:
                return
            # Close stdin BEFORE terminating so the worker's blocking
            # `for line in sys.stdin:` reader sees EOF and exits its
            # for-loop cleanly. Without this, the worker process
            # could hang waiting on the read-end of stdin until the
            # OS reclaims pipes.
            try:
                if self._proc.stdin is not None and not self._proc.stdin.closed:
                    self._proc.stdin.close()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            try:
                if force:
                    self._proc.kill()
                else:
                    try:
                        self._proc.terminate()
                    except Exception:
                        self._proc.kill()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            # Push a None sentinel onto the queue if a consumer is
            # currently blocked on .get() — without this, after a
            # forced kill the reader's queue.get(timeout=0.5) takes
            # the full timeout before noticing the subprocess died.
            try:
                _q = self._line_queue
                if _q is not None:
                    _q.put(None)
            except Exception as e:
                _log.debug("swallowed: %s", e)
            # Drain the old reader thread before nulling proc + queue.
            # Without this, a subprocess restart left the old reader
            # alive reading from a dead pipe — a zombie thread until
            # daemon cleanup at process exit (audit: H48 + H64). 2s
            # join cap so a wedged reader can't block the next start.
            try:
                _rt = self._reader_thread
                if _rt is not None and _rt.is_alive():
                    _rt.join(timeout=2.0)
            except Exception as e:
                _log.debug("swallowed: %s", e)
            self._reader_thread = None
            try:
                _st = self._stderr_drain_thread
                if _st is not None and _st.is_alive():
                    _st.join(timeout=2.0)
            except Exception as e:
                _log.debug("swallowed: %s", e)
            self._stderr_drain_thread = None
            self._proc = None
            self._line_queue = None

    # ── Queue + worker loop ──────────────────────────────────────────

    def enqueue(self, path: str, title: str = "",
                channel: str = "",
                combined: bool | None = None,
                on_complete: Callable | None = None,
                retranscribe: bool = False,
                video_id: str = "",
                bulk_id: str = "",
                bulk_total: int = 0,
                bulk_index: int = 0,
                from_download: bool = False) -> bool:
        """Queue a video for transcription.

        `channel` is optional; if provided it's stored on the job so the
        FTS ingest at completion uses the right channel name (matters
        when the video path is structured differently from
        <base>/<channel>/<file>).

        `combined` overrides the channel's split_years-based output split:
          - None : follow ch.split_years (OLD-compatible default)
          - True : write to one channel-root transcript even if organized
          - False : write per-year even if the channel isn't organized

        Matches OLD's "Follow organization / Combined" first-time dialog
        (YTArchiver.py:5919). See `chan_transcribe_all` Api for the
        UI handshake that decides the `combined` value.

        `retranscribe=True` marks this as a RE-transcription — the worker
        will call `_replace_jsonl_entry` + `_replace_txt_entry` instead
        of the normal append-only writers, so the old entry in the
        aggregated files gets surgically swapped (matches `_run_retranscribe_job`). `video_id` is used
        by the replace-jsonl pass to catch title-drifted duplicates.
        """
        path = str(path)
        if not os.path.isfile(path):
            self._stream.emit_error(f"Transcribe: file not found: {path}")
            return False
        _job_title = title or os.path.basename(path)
        _path_key = os.path.normcase(os.path.normpath(os.path.abspath(path)))
        with self._jobs_lock:
            def _same_transcribe_path(job: dict[str, Any] | None) -> bool:
                if not job or (job.get("kind") or "transcribe") != "transcribe":
                    return False
                job_path = job.get("path") or ""
                if not job_path:
                    return False
                return (
                    os.path.normcase(os.path.normpath(os.path.abspath(job_path)))
                    == _path_key
                )

            if any(_same_transcribe_path(j) for j in self._jobs) \
                    or _same_transcribe_path(self._current_job):
                return False
            self._jobs.append({
                "path": path,
                "title": _job_title,
                "channel": channel,
                "combined_override": combined,
                "cb": on_complete,
                "cancel": threading.Event(),
                "retranscribe": bool(retranscribe),
                "video_id": (video_id or "").strip(),
                "bulk_id": bulk_id or "",
                "bulk_total": int(bulk_total or 0),
                "bulk_index": int(bulk_index or 0),
                "from_download": bool(from_download),
            })
        # Mirror the job into the shared GPU queue so the Tasks popover
        # shows the pending work. this was flagged: auto-transcribe on
        # a channel would write to our internal `_jobs` list but the
        # popover stayed empty, so there was no visible record of the
        # transcription being queued. `kind=transcribe` + `title`
        # matches the shape `_task_label_gpu` reads.
        # `bulk_id`/`bulk_total` carry a coalesce hint — when N videos from
        # the same channel are queued in one "Queue Pending" / "Transcribe
        # All" click, they all share a bulk_id and the popover collapses
        # them into a single "Transcribing {ch} (X videos)" row.
        if self._queues is not None:
            try:
                self._queues.gpu_enqueue({
                    "kind": "transcribe",
                    "title": _job_title,
                    "path": path,
                    "channel": channel,
                    "bulk_id": bulk_id,
                    "bulk_total": int(bulk_total or 0),
                    "bulk_index": int(bulk_index or 0),
                })
            except Exception as e:
                _log.debug("swallowed: %s", e)
        # Bump `transcription_pending` for the channel so the Subs-tab
        # auto-indicator stays in sync with OLD's behavior (YTArchiver.py:
        # 14629 and friends set this counter during sync → transcribe flow).
        _bump_transcription_pending(channel, 1)
        self._persist_pending()
        # Auto-clear a launch-time pause when a NEW job arrives AND the
        # GPU Auto checkbox is on. The launch-time pause is meant to
        # stop RESTORED items from auto-firing, not to block fresh
        # user-initiated work. Without this clear, every new retranscribe
        # / right-click "Re-transcribe" sat in the queue until the user
        # manually clicked Start — confusing because Auto was on.
        try:
            # Hold the jobs lock around the check + clear so a
            # concurrent queue_pause("gpu") can't slip its set()
            # between our `is_set()` read and our clear(), leaving
            # paused=True when we thought we'd just cleared it
            # (audit: transcribe/core.py H67).
            with self._jobs_lock:
                if (self._auto_enabled() and self._paused.is_set()
                        and self._queues is not None
                        and getattr(self._queues, "gpu_paused", False)):
                    self._paused.clear()
                    self._queues.set_gpu_paused(False)
        except Exception as e:
            _log.debug("swallowed: %s", e)
        self._ensure_worker()
        return True

    def compress_enqueue(self, path: str, title: str = "",
                         channel: str = "", quality: str = "Average",
                         output_res: str = "720",
                         on_complete: Callable | None = None) -> bool:
        """Queue a video for AV1 NVENC compression via the same GPU
        worker that handles transcription.

        rule: "the GPU task list whole purpose, especially with
        the auto checkbox, is almost like permission to bog down my
        computer." Standalone compress (right-click → Compress, Subs
        batch Compress) used to bypass the queue entirely and fire ffmpeg
        immediately from a bare thread; that ignored the Auto gate. Now
        it enqueues a `kind: "compress"` job so:
          - the task is visible in the GPU Tasks popover
          - the Auto checkbox gates firing (same as transcribe)
          - multiple compresses serialize on the same GPU instead of
            stampeding into parallel NVENC sessions.
        """
        path = str(path)
        if not os.path.isfile(path):
            self._stream.emit_error(f"Compress: file not found: {path}")
            return False
        path = os.path.normpath(path)
        try:
            from ..utils import is_within_managed_roots
            if not is_within_managed_roots(path):
                self._stream.emit_error(
                    "Compress: refusing to queue file outside the archive.")
                return False
        except Exception as e:
            _log.warning("compress containment check failed for %s: %s",
                         path, e)
            self._stream.emit_error(
                "Compress: could not verify archive containment.")
            return False
        _job_title = title or os.path.splitext(os.path.basename(path))[0]
        with self._jobs_lock:
            self._jobs.append({
                "kind": "compress",
                "path": path,
                "title": _job_title,
                "channel": channel,
                "quality": quality,
                "output_res": str(output_res),
                "cb": on_complete,
                "cancel": threading.Event(),
            })
        if self._queues is not None:
            try:
                self._queues.gpu_enqueue({
                    "kind": "compress",
                    "title": _job_title,
                    "path": path,
                    "channel": channel,
                })
            except Exception as e:
                _log.debug("swallowed: %s", e)
        self._persist_pending()
        self._ensure_worker()
        return True

    # ── Pending journal (survives restart) ──

    def drop_running_from_journal(self):
        """Rewrite the pending journal as if the currently-running job
        doesn't exist. Used by `gpu_skip_current` as belt-and-suspenders
        cleanup: if the worker reaches its normal `finally` block this
        is redundant (the finally also rewrites the journal without
        `current_job`); if the worker hangs and never reaches the
        finally, this prevents the task from resurrecting on next
        launch.

        Deliberately writes the journal with only `self._jobs` (the
        queued tail) — drops whatever's in `self._current_job`. The
        worker is responsible for clearing `self._current_job` later
        via its own finally; this method doesn't touch that field to
        avoid racing the worker thread.
        """
        try:
            import json as _json
            def _snap(j: dict[str, Any]) -> dict[str, Any]:
                return {
                    "path": j.get("path", ""),
                    "title": j.get("title", ""),
                    "channel": j.get("channel", ""),
                    "video_id": j.get("video_id", ""),
                    "retranscribe": bool(j.get("retranscribe")),
                    "combined_override": j.get("combined_override"),
                    "bulk_id": j.get("bulk_id", ""),
                    "bulk_total": int(j.get("bulk_total", 0) or 0),
                    "bulk_index": int(j.get("bulk_index", 0) or 0),
                    "kind": j.get("kind", "transcribe"),
                    "from_download": bool(j.get("from_download")),
                }
            with self._jobs_lock:
                snapshot = [_snap(j) for j in self._jobs]
            # Note: current_job is INTENTIONALLY excluded.
            p = _pending_journal_path()
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = str(p) + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                _json.dump(snapshot, f, indent=2)
            os.replace(tmp, p)
        except Exception as e:
            _log.debug("swallowed: %s", e)

    def _persist_pending(self):
        """Write current pending jobs to disk so a crash/restart recovers them."""
        try:
            import json as _json
            # serialize ALL job fields needed to rehydrate
            # correctly on restart. Before this, only (path, title,
            # channel) were written — so a killed-mid-retranscribe job
            # came back as a regular transcribe (retranscribe flag
            # lost), which took the auto-captions fast path and left
            # the old Whisper entry duplicated in the .txt/.jsonl.
            # Same risk for video_id (used by replace_*_entry helpers
            # to catch title-drifted stale entries) and for
            # combined_override / bulk_id which affect path resolution
            # and batch tracking.
            def _snap(j: dict[str, Any]) -> dict[str, Any]:
                return {
                    "path": j.get("path", ""),
                    "title": j.get("title", ""),
                    "channel": j.get("channel", ""),
                    "video_id": j.get("video_id", ""),
                    "retranscribe": bool(j.get("retranscribe")),
                    "combined_override": j.get("combined_override"),
                    "bulk_id": j.get("bulk_id", ""),
                    # Persist bulk batch metadata so the popover tooltip
                    # ("Transcribing X (3 of 5)") reflects the original
                    # batch on recovery, not just the surviving items.
                    "bulk_total": int(j.get("bulk_total", 0) or 0),
                    "bulk_index": int(j.get("bulk_index", 0) or 0),
                    "kind": j.get("kind", "transcribe"),
                    "from_download": bool(j.get("from_download")),
                }
            with self._jobs_lock:
                snapshot = [_snap(j) for j in self._jobs]
            # Include in-flight job at top
            if self._current_job:
                snapshot.insert(0, _snap(self._current_job))
            p = _pending_journal_path()
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = str(p) + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                _json.dump(snapshot, f, indent=2)
            os.replace(tmp, p)
        except Exception as e:
            _log.debug("swallowed: %s", e)

    def load_pending(self) -> int:
        """Load any jobs left behind from a previous session. Returns count.

        "Already transcribed" means the title already has an entry in the
        aggregated {channel} Transcript.txt (matches YTArchiver.py's
        _scan_existing_transcripts). Falls back to checking a legacy
        per-video {base}.jsonl sidecar from older builds.
        """
        try:
            import json as _json
            p = _pending_journal_path()
            if not p.exists():
                return 0
            with p.open("r", encoding="utf-8") as f:
                jobs = _json.load(f)
            if not isinstance(jobs, list):
                return 0

            # Cache existing-title sets per channel so we don't re-walk the
            # folder N times.
            _title_cache: dict[str, set] = {}
            def _already_transcribed(video_path: str, title: str, channel: str) -> bool:
                # Legacy per-video .jsonl from an earlier build
                base = os.path.splitext(video_path)[0]
                if os.path.isfile(base + ".jsonl"):
                    return True
                # Aggregated Transcript.txt scan
                paths = _resolve_transcript_paths(video_path, title, channel)
                if paths is None:
                    return False
                txt_path, _jp, _y, _m, _ud = paths
                folder = os.path.dirname(os.path.dirname(txt_path)) \
                         if os.path.basename(os.path.dirname(txt_path)).isdigit() \
                         else os.path.dirname(txt_path)
                cache_key = f"{channel}::{folder}"
                titles = _title_cache.get(cache_key)
                if titles is None:
                    titles = _scan_existing_transcript_titles(folder, channel)
                    _title_cache[cache_key] = titles
                # `titles` is a dict keyed by the normalized title form.
                return _norm_title(title) in titles

            recovered = 0
            for j in jobs:
                path = j.get("path") or ""
                if not path or not os.path.isfile(path):
                    continue
                if _already_transcribed(path, j.get("title", ""), j.get("channel", "")):
                    continue
                # rehydrate all saved job fields (retranscribe,
                # video_id, combined_override, bulk_*) so a restarted
                # retranscribe stays a retranscribe and title-drifted
                # stale entries get caught via video_id lookup.
                self.enqueue(
                    path,
                    title=j.get("title", ""),
                    channel=j.get("channel", ""),
                    combined=j.get("combined_override"),
                    retranscribe=bool(j.get("retranscribe")),
                    video_id=j.get("video_id", ""),
                    bulk_id=j.get("bulk_id", ""),
                    bulk_total=int(j.get("bulk_total", 0) or 0),
                    bulk_index=int(j.get("bulk_index", 0) or 0),
                    from_download=bool(j.get("from_download")),
                )
                recovered += 1
            # Recovery succeeded — drop the journal file. The next
            # _persist_pending() call will rewrite it from current
            # state. Without this, a second crash before the next
            # persist would re-enqueue the same jobs a second time.
            try:
                p.unlink()
            except OSError:
                pass
            return recovered
        except Exception:
            return 0

    def queue_size(self) -> int:
        with self._jobs_lock:
            n = len(self._jobs)
        if self._current_job:
            n += 1
        return n

    def remove_pending_jobs(self, predicate) -> int:
        """Remove pending jobs from `_jobs` where predicate(job) is True.
        Returns the count removed.

        Used by the queue-popover removal handlers in queue_mixin so a
        click on "X" drops the job from BOTH the persistent `_queues.gpu`
        list AND this manager's `_jobs` work-list. Without this, the
        worker_loop would still pop the user-removed item from `_jobs`
        when its turn came, and the popover would suddenly show it as
        the active job — the "removed task came back" bug.

        Decrements `transcription_pending` for the channel so the Subs-
        tab indicator stays accurate when removed jobs were
        sync-originated (non-retranscribe).
        """
        if not callable(predicate):
            return 0
        removed = 0
        with self._jobs_lock:
            keep = []
            for j in self._jobs:
                try:
                    match = bool(predicate(j))
                except Exception:
                    match = False
                if match:
                    removed += 1
                    try:
                        if (not j.get("retranscribe")
                                and not j.get("_pending_decremented")):
                            _bump_transcription_pending(
                                j.get("channel") or "", -1)
                            j["_pending_decremented"] = True
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                else:
                    keep.append(j)
            self._jobs[:] = keep
        return removed

    def reorder_pending_job(self, identifier: str, new_index: int) -> bool:
        """Mirror a GPU popover reorder into the manager's pending jobs."""
        ident = str(identifier or "").strip()
        if not ident:
            return False
        try:
            target_index = int(new_index)
        except (TypeError, ValueError):
            return False
        with self._jobs_lock:
            if target_index < 0 or target_index >= len(self._jobs):
                return False
            idx = next(
                (
                    i for i, job in enumerate(self._jobs)
                    if (job.get("id") or job.get("path") or "") == ident
                ),
                -1,
            )
            if idx < 0:
                return False
            job = self._jobs.pop(idx)
            self._jobs.insert(target_index, job)
        try:
            self._persist_pending()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return True

    def cancel_all(self):
        self._cancel_all.set()
        with self._jobs_lock:
            self._jobs.clear()
            job = self._current_job
            if job and "cancel" in job:
                job["cancel"].set()
            # Keep the job lock through the subprocess stop so the
            # worker cannot clear _current_job, pop the next job, and
            # start a new subprocess between our cancel signal and kill.
            if job:
                try:
                    self._stop_subprocess(force=True)
                except Exception as e:
                    _log.debug("swallowed: %s", e)
        # also clear the shared GPU popover queue so the UI
        # doesn't keep showing phantom "pending" rows for tasks that
        # have been cancelled. The worker's finally-path would normally
        # do this one-at-a-time, but on cancel_all the worker breaks
        # out of the loop immediately and the popover stays stale.
        if self._queues is not None:
            try:
                self._queues.gpu_clear()
            except Exception as e:
                _log.debug("swallowed: %s", e)

    def pause(self):
        self._paused.set()

    def resume(self):
        self._paused.clear()

    def is_active(self) -> bool:
        """True if a GPU job is currently running OR jobs remain queued.

        Earlier versions returned `self._worker_thread.is_alive()`, but
        that leaves the blink state stuck ON after the last job finishes:
        the worker sets `_current_job=None` + fires `set_current_gpu(None)`
        → `_on_queue_changed` runs → `is_alive()` still True → blink keeps
        going → next loop iteration finally breaks out → thread dies →
        no final notify fires → UI never repaints to idle.

        Using job-state instead of thread-liveness breaks that race: once
        the queue is empty and no job is running, is_active() returns False
        immediately and the final notify paints the button to idle.
        """
        with self._jobs_lock:
            return self._current_job is not None or len(self._jobs) > 0

    def skip_current(self):
        """Cancel the currently-running job but keep the queue + worker alive.

        Fires the per-job cancel event so _transcribe_one returns promptly;
        worker loop then picks up the next job. No-op if nothing running.

        Also sends the worker's cooperative cancel command so the GPU
        work stops without killing the warm Whisper subprocess. The
        read loop falls back to a force-kill if the worker does not
        acknowledge cancellation promptly.
        """
        with self._jobs_lock:
            job = self._current_job
            if job and "cancel" in job:
                try:
                    job["cancel"].set()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                try:
                    self._send_cancel_command()
                except Exception as e:
                    _log.debug("swallowed: %s", e)

    def _ensure_worker(self):
        if self._worker_thread is None or not self._worker_thread.is_alive():
            self._cancel_all.clear()
            self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self._worker_thread.start()

    def _worker_loop(self):
        # Whisper env sanity check only — NOT the model load. The actual
        # `start_subprocess()` (which loads the model onto GPU and prints
        # the "Loading Whisper model..." banner) is deferred to
        # `_transcribe_one`, which only fires it after the auto-captions
        # fast-path misses. On channels where YouTube auto-captions cover
        # everything (most podcasts / news / interview content), we never
        # load Whisper at all — OLD YTArchiver's _fetch_auto_captions path
        # has the same short-circuit.
        if not self.is_available():
            self._stream.emit_error(
                "Whisper: Python 3.11 not found. Install from python.org "
                "to enable transcription.")
            with self._jobs_lock:
                self._jobs.clear()
            return

        while not self._cancel_all.is_set():
            # Two gates at the top of the loop before popping a job:
            # 1. `_paused` — set by queue_pause("gpu") or disk-watchdog.
            # Parks the worker thread without draining the queue,
            # so tasks stay visible in the popover with "paused"
            # status. Matches OLD's _wait_if_paused pattern.
            # 2. Auto-checkbox — when `autorun_gpu` is False, incoming
            # jobs sit in the queue without firing. rule:
            # "if the GPU task list auto box is unchecked and a
            # transcription task gets kicked over there, it
            # doesn't fire." We poll both every 250ms.
            # Track whether we've signaled "actually paused" to the UI
            # so we only call set_gpu_paused_active(True) once per
            # entry into pause-wait (and clear it once on exit). The
            # outer Auto-disabled gate doesn't count as a "pause" —
            # only the explicit _paused flag does.
            _signaled_paused_active = False
            while (not self._cancel_all.is_set() and
                   (self._paused.is_set() or not self._auto_enabled())):
                if (self._paused.is_set()
                        and not _signaled_paused_active
                        and self._queues is not None):
                    try:
                        self._queues.set_gpu_paused_active(True)
                        _signaled_paused_active = True
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                time.sleep(0.25)
            # Either we exited because cancel fired or because both
            # _paused and Auto-disabled cleared. Drop the active flag
            # if we set it.
            if _signaled_paused_active and self._queues is not None:
                try: self._queues.set_gpu_paused_active(False)
                except Exception as e: _log.debug("swallowed: %s", e)
            if self._cancel_all.is_set():
                break
            # Apply a deferred model swap now that we're idle (no job in
            # flight): stop the old-model subprocess so the next job's
            # start_subprocess reloads with the new self._model. Done here
            # (worker thread, loop-top) rather than in swap_model (UI thread)
            # to avoid killing an in-flight job mid-read (audit: swap_model race).
            if getattr(self, "_pending_model_restart", False):
                self._pending_model_restart = False
                try:
                    self._stop_subprocess()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            with self._jobs_lock:
                if not self._jobs:
                    break
                job = self._jobs.pop(0)
                self._current_job = job
            # Journal the in-flight job state IMMEDIATELY so a crash
            # between here and the finally-end _persist_pending call
            # doesn't lose the in-flight job (audit: transcribe/
            # core.py:912-934). Best-effort — failure here doesn't
            # disturb the actual transcription path.
            try:
                self._persist_pending()
            except Exception as _pe:
                _log.debug("swallowed: %s", _pe)
            # Reflect "now running" in the shared GPU queue: pop the
            # matching popover entry off `queues.gpu` and stamp it as
            # `current_gpu` so the popover's top row switches to
            # "Transcribing X" / "Compressing X" while the rest shrink
            # upward. Label verb comes from the job's `kind`.
            _job_kind = job.get("kind") or "transcribe"
            if self._queues is not None:
                try:
                    self._queues.gpu_pop_matching(
                        expected_path=job.get("path", ""),
                        expected_bulk_id=job.get("bulk_id", ""),
                    )
                    self._queues.set_current_gpu({
                        "kind": _job_kind,
                        "title": job.get("title", ""),
                        "path": job.get("path", ""),
                        "channel": job.get("channel", ""),
                    })
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            # Track per-channel stats so we can emit a [Trnscr] history
            # row when the worker drains. Matches OLD's _record_transcription.
            ch_name = (job.get("channel") or "").strip() or "\u2014"
            if _job_kind == "compress":
                stats = self._compress_stats.setdefault(ch_name,
                    {"start": time.time(), "done": 0, "err": 0})
            else:
                stats = self._batch_stats.setdefault(ch_name,
                    {"start": time.time(), "done": 0, "err": 0})
            crashed = False
            try:
                if _job_kind == "compress":
                    self._compress_one(job)
                else:
                    self._transcribe_one(job)
            except RuntimeError as _empty_e:
                # "Whisper returned an empty transcript" — this is a
                # SUCCESSFUL but speechless run (silent intro, music-
                # only video, vad_filter rejected everything), not a
                # crash. Surface as a calm dim line, NOT a scary red
                # "Transcribe crashed" entry (audit: transcribe/
                # core.py:1881-1886).
                _msg_lower = str(_empty_e).lower()
                if "cancelled before write" in _msg_lower:
                    # "Cancel All" raced into _write_outputs — silent.
                    # Counts as done so the err counter doesn't bump.
                    crashed = False
                elif "empty transcript" in _msg_lower:
                    _vid_for_empty = (job.get("video_id") or "").strip()
                    _empty_marker = f"tx_done_{_vid_for_empty}" if _vid_for_empty else ""
                    _empty_tags = [t for t in (_empty_marker, "dim") if t]
                    self._stream.emit([[
                        f" — no speech detected in {job.get('title') or 'video'}.\n",
                        _empty_tags,
                    ]])
                    # Not a crash — counts as done (avoids polluting
                    # the error counter for a benign outcome).
                    crashed = False
                else:
                    # Genuine RuntimeError — treat as a crash.
                    _vid_for_err = (job.get("video_id") or "").strip()
                    _marker = f"tx_done_{_vid_for_err}" if _vid_for_err else ""
                    _err_tags = [t for t in (_marker, "red") if t]
                    self._stream.emit([[
                        f"{_job_kind.capitalize()} crashed: {_empty_e}\n",
                        _err_tags,
                    ]])
                    crashed = True
            except Exception as e:
                # audit SR-3 (user screenshot): if a transcribe
                # job crashes, the error line must still REPLACE the
                # sync.py-reserved `tx_done_<vid>` placeholder under
                # the channel that owns this video — not land at the
                # log tail (wherever sync is currently processing).
                # Without the marker the "Transcribe crashed" line
                # orphaned itself under unrelated later channels.
                _vid_for_err = (job.get("video_id") or "").strip()
                _marker = f"tx_done_{_vid_for_err}" if _vid_for_err else ""
                _err_tags = [t for t in (_marker, "red") if t]
                # Use the structured emit form so we can carry the
                # marker; emit_error doesn't accept tag lists.
                self._stream.emit([[
                    f"{_job_kind.capitalize()} crashed: {e}\n",
                    _err_tags,
                ]])
                crashed = True
            finally:
                if job.get("_requeued_no_tally"):
                    job.pop("_requeued_no_tally", None)
                elif crashed:
                    stats["err"] += 1
                else:
                    stats["done"] += 1
                # if a transcribe job crashed or early-returned
                # without reaching the success-path decrement, the pending
                # counter would leak (-1, -2, -3 stuck on the Subs row
                # forever). Any non-retranscribe transcribe job that didn't
                # set `_pending_decremented` gets drained here.
                if (_job_kind != "compress"
                        and not job.get("retranscribe")
                        and not job.get("_pending_decremented")):
                    try:
                        ch_for_decrement = (job.get("channel") or "").strip()
                        if ch_for_decrement:
                            _bump_transcription_pending(ch_for_decrement, -1)
                        _vid = job.get("video_id") or ""
                        if _vid:
                            from .. import ytarchiver_config as _cfg
                            _cfg.remove_pending_tx_id(_vid)
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                with self._jobs_lock:
                    if self._current_job is job:
                        self._current_job = None
                # audit D-10 / if the previous job had been
                # forced into CPU mode via OOM fallback, reset the env
                # back to CUDA regardless of whether the fallback job
                # itself succeeded or crashed. Before, the reset was
                # gated on `not crashed` — a crashed fallback-job
                # left WHISPER_DEVICE=cpu in the env forever, so
                # every subsequent transcribe ran on CPU until app
                # restart (user reports "transcription mysteriously
                # slow until I relaunch").
                if (self._cpu_fallback_active
                        and _job_kind != "compress"):
                    self._cpu_fallback_active = False
                    try:
                        # No global env pop needed any more — the
                        # next _start_subprocess reads the instance
                        # flag (now False) and rebuilds env on GPU
                        # automatically (audit: H51).
                        self._stop_subprocess(force=True)
                        _reset_label = (
                            "\u21A9 Resetting to GPU mode for next job."
                            if not crashed else
                            "\u21A9 Resetting to GPU mode (fallback job crashed "
                            "\u2014 giving GPU another try).")
                        self._stream.emit_text(
                            " " + _reset_label, "simpleline_blue")
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                # Clear the "running" slot on completion so the popover
                # returns to idle (or shows the next queued item as the
                # next iteration sets its own current_gpu).
                if self._queues is not None:
                    try:
                        self._queues.set_current_gpu(None)
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                self._persist_pending()

        # Flush per-channel batch stats to autorun_history + activity log.
        # One row per channel processed in this worker session.
        try:
            self._flush_batch_stats()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        self._stream.flush()

    def _compress_one(self, job: dict[str, Any]):
        """Run one compress job from the GPU queue — delegates to
        backend.compress.compress_video(). Shares the same worker
        thread as transcribe so only one GPU task runs at a time."""
        if job["cancel"].is_set():
            return
        try:
            from .. import compress as _cmp
        except Exception as e:
            self._stream.emit_error(f"Compress: import failed: {e}")
            return
        try:
            res = _cmp.compress_video(
                job["path"],
                self._stream,
                quality=job.get("quality", "Average"),
                output_res=str(job.get("output_res", "720")),
                cancel_event=job["cancel"],
            )
        except Exception as e:
            self._stream.emit_error(f"Compress: {e}")
            return
        if job.get("cb"):
            try: job["cb"](res)
            except Exception as e: _log.debug("swallowed: %s", e)

    def _flush_batch_stats(self):
        """Emit [Trnscr] autorun_history rows for MANUAL transcribe-only
        channels (right-click \u2192 Transcribe on a channel/video).

        Sync-originated channels are skipped two ways:
          (a) a bug: fast auto-captions finish BEFORE sync_channel
              ends, so we check `sync.is_sync_active(name)` and leave
              those stats in place — sync_channel will read+emit them
              when it finishes.
          (b) Normal case: sync_channel already called
              `consume_channel_batch_stats()` and the entry is gone.
        """
        if not self._batch_stats and not self._compress_stats:
            return
        try:
            from .. import autorun as _ar
        except Exception:
            self._batch_stats.clear()
            return
        try:
            from .. import sync as _sync
        except Exception:
            _sync = None
        from datetime import datetime as _dt
        now = _dt.now()
        time_str = now.strftime("%I:%M%p").lstrip("0").lower()
        date_str = now.strftime("%b %d").replace(" 0", " ")
        # Iterate over a snapshot of keys — we selectively pop emitted
        # channels instead of clearing wholesale, so sync-active channels'
        # stats stay put for sync_channel to consume at its end.
        for ch_name in list(self._batch_stats.keys()):
            if _sync is not None and _sync.is_sync_active(ch_name):
                continue # leave for sync_channel to emit as [Dwnld]
            s = self._batch_stats.pop(ch_name, None) or {}
            done = int(s.get("done", 0))
            err = int(s.get("err", 0))
            if done == 0 and err == 0:
                continue # no work actually happened
            elapsed = time.time() - float(s.get("start", time.time()))

            # If sync just emitted a [Dwnld] row for this channel and
            # the transcribe count was 0 at the time (Whisper still
            # running), patch that same row in place by re-emitting
            # with the registered row_id instead of appending a
            # separate [Trnscr]. The UI's `data-row-id` lookup swaps
            # the row contents. Result: one consolidated row with the
            # final counts, no duplicate.
            pending = None
            if _sync is not None:
                try:
                    pending = _sync.pop_pending_dwnld_row(ch_name)
                except Exception:
                    pending = None
            if pending is not None:
                try:
                    # Total elapsed = time since sync_channel started
                    # (NOT just the transcribe portion) so the "took"
                    # cell reflects the whole channel's pass duration.
                    _total_elapsed = time.time() - float(
                        pending.get("elapsed_start", time.time()))
                    _sync.emit_consolidated_auto_row(
                        self._stream, ch_name,
                        downloaded=int(pending.get("downloaded", 0)),
                        transcribed=done,
                        metadata=int(pending.get("metadata", 0)),
                        errors=int(pending.get("errors", 0)) + err,
                        elapsed=float(_total_elapsed),
                        kind="Dwnld",
                        row_id=str(pending.get("row_id") or ""),
                    )
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                continue

            # No recent [Dwnld] row for this channel — emit a
            # standalone [Trnscr] as before (manual transcribe flow,
            # etc.).
            primary = f"{done} transcribed"
            try:
                _ar.append_history_entry(
                    _ar.format_history_entry("Trnscr", ch_name,
                                             primary, secondary="",
                                             errors=err, took_sec=elapsed))
            except Exception as e:
                _log.debug("swallowed: %s", e)
            try:
                self._stream.emit_activity({
                    "kind": "Trnscr",
                    "time_date": f"{time_str}, {date_str}",
                    "channel": "" if ch_name == "\u2014" else ch_name,
                    "primary": primary,
                    "secondary": "",
                    "errors": f"{err} errors",
                    "took": f"took {int(elapsed)}s" if elapsed < 60
                                 else f"took {int(elapsed)//60}m {int(elapsed)%60}s",
                    "row_tag": "hist_blue" if done > 0 else "",
                })
            except Exception as e:
                _log.debug("swallowed: %s", e)

        # [Cmprss] rows \u2014 live compress jobs get their own history +
        # activity rows (tag/kind matching ytarchiver_config's reload
        # recolor). They previously flushed through the loop above as
        # "N transcribed" and inflated the consolidated [Dwnld] count.
        for ch_name in list(self._compress_stats.keys()):
            s = self._compress_stats.pop(ch_name, None) or {}
            done = int(s.get("done", 0))
            err = int(s.get("err", 0))
            if done == 0 and err == 0:
                continue
            elapsed = time.time() - float(s.get("start", time.time()))
            primary = f"{done} compressed"
            try:
                _ar.append_history_entry(
                    _ar.format_history_entry("Cmprss", ch_name,
                                             primary, secondary="",
                                             errors=err, took_sec=elapsed))
            except Exception as e:
                _log.debug("swallowed: %s", e)
            try:
                self._stream.emit_activity({
                    "kind": "Cmprss",
                    "time_date": f"{time_str}, {date_str}",
                    "channel": "" if ch_name == "\u2014" else ch_name,
                    "primary": primary,
                    "secondary": "",
                    "errors": f"{err} errors",
                    "took": f"took {int(elapsed)}s" if elapsed < 60
                                 else f"took {int(elapsed)//60}m {int(elapsed)%60}s",
                    "row_tag": "hist_compress" if done > 0 else "",
                })
            except Exception as e:
                _log.debug("swallowed: %s", e)

    def _transcribe_one(self, job: dict[str, Any]):
        path = job["path"]
        title = job["title"]
        if job["cancel"].is_set():
            return

        # if GPU Auto was unchecked AFTER this job was
        # popped but BEFORE we started processing, re-park it at the
        # front of the queue and bail. Without this guard the worker
        # would keep firing auto-captions / Whisper for several
        # already-popped jobs even though the user explicitly asked
        # for queue-up behavior.
        if not self._auto_enabled():
            with self._jobs_lock:
                self._jobs.insert(0, job)
            return

        # Unique-per-job inplace kind. Every emit from this job's
        # lifecycle (Loading punctuation model, Adding punctuation,
        # Whisper progress ticks, final done line) carries this tag
        # so they replace EACH OTHER within the job but stay
        # independent of other jobs' emits. Without this, video 2's
        # "Loading punctuation..." would stomp video 1's done line
        # when two videos for the same channel get transcribed in
        # sequence. Store on the job so `punct_mgr` can pick it up.
        # Thread-safe job-id allocation via the locked accessor in
        # helpers.py (audit: L27). Previously the bare `+= 1` could
        # double-issue an id under contention.
        from . import helpers as _h
        _my_job_id = _h.next_job_id()
        job_tag = f"whisper_job_{_my_job_id}"
        job["job_tag"] = job_tag

        # ── Auto-captions fast-path ──
        # If yt-dlp already dropped a .vtt subtitle sidecar for this video
        # (English captions), parse it straight into .jsonl + .txt — way
        # faster than running Whisper and usually just as good for recent
        # podcast / news-type content.
        # Skipped for retranscribe jobs: when the user explicitly asks to
        # Re-transcribe with Whisper, the whole point is to REPLACE the
        # auto-captions transcript with a Whisper one. Taking the VTT
        # fast-path here would just regenerate the auto-captions entry.
        # Passes `self._punct` so the fetched captions get the same
        # punctuation-restoration pass runs.
        # Captions written WITH punct get the `YT+PUNCTUATION` source
        # tag; captions written without get plain `YT CAPTIONS`.
        _punct_for_captions = self._punct if self._punctuate_enabled else None
        # Tell PunctuationManager which job_tag to use for its
        # "Loading punctuation model..." emit so that line joins
        # this video's inplace family.
        if _punct_for_captions is not None:
            try: _punct_for_captions._job_tag = job_tag
            except Exception as e: _log.debug("swallowed: %s", e)
        if (not job.get("retranscribe") and
                _try_auto_captions(path, title, job.get("channel", ""),
                                   self._stream,
                                   punct_mgr=_punct_for_captions,
                                   job_tag=job_tag,
                                   video_id_hint=job.get("video_id", ""),
                                   from_download=bool(job.get("from_download")))):
            if job.get("cb"):
                try:
                    job["cb"]({"auto_captions": True})
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            return

        # Auto-captions path missed — either no .vtt available, yt-dlp
        # couldn't fetch captions for this video, or the VTT parse came
        # back empty. was flagged the silent-failure case: NO log
        # line at all between "Metadata downloaded" and the next
        # channel's header. Emit a visible fallback line so the user
        # sees Whisper take over instead of the transcription just
        # vanishing. Uses the `whisper_progress` inplace family so the
        # final "— ✓ Transcription" done line replaces this too.
        if not job.get("retranscribe"):
            self._stream.emit([[
                " No auto-captions available \u2014 using Whisper\u2026\n",
                ["transcribe_using", job_tag],
            ]])

        proc, _line_q = self._snapshot_worker_io()
        if proc is None or proc.poll() is not None:
            if not self.start_subprocess():
                # Subprocess failed to start — emit an error so the
                # user knows why the transcription silently died
                # (Python 3.11 missing, GPU driver wrong, model
                # download failed, etc.). Without this the job just
                # disappears from view. `emit_error` routes to the
                # red error style.
                self._stream.emit_error(
                    f"Whisper failed to start \u2014 transcription for "
                    f"\"{title}\" skipped. Check Python 3.11 install "
                    f"+ CUDA drivers.")
                if job.get("cb"):
                    try: job["cb"]({"ok": False, "reason": "whisper_start_failed"})
                    except Exception as e: _log.debug("swallowed: %s", e)
                return

        # ── Chunked path for long videos (>~2 hours) ──
        # Splits the file into overlapping WAV chunks with ffmpeg, transcribes
        # each, and merges segments (offset timestamps, drop overlap dupes).
        # Matches YTArchiver.py:11139 _whisper_transcribe_chunked.
        duration = _ffprobe_duration(path) or 0.0
        # Fallback heuristic when ffprobe fails (missing binary, file
        # locked, timeout) — a 0.0 duration would skip the chunked
        # branch on a 4-hour video and OOM Whisper trying to load the
        # whole audio at once. Use file-size as a rough proxy: typical
        # 1080p mp4 is ~50 MB/hr. A file >120 MB roughly maps to >2 h.
        # Track whether the duration is a real ffprobe value or just a
        # chunking-routing sentinel — the realtime-ratio emit below
        # would otherwise print a fabricated "1.2x realtime" derived
        # from `_CHUNK_MIN_DURATION` instead of the true video length
        # (audit: transcribe/core.py H70).
        _duration_is_real = duration > 0.0
        if duration <= 0.0:
            duration = _rough_duration_from_size(path)
            try:
                _sz = os.path.getsize(path)
                # 120 MB threshold ~= 2 hour 1080p — conservative; we'd
                # rather chunk a short video unnecessarily than OOM a
                # long one. Chunking on a short video is just slower,
                # not broken.
                if _sz > 120 * 1024 * 1024:
                    duration = float(_CHUNK_MIN_DURATION)
            except OSError:
                pass
        job["_duration_is_real"] = _duration_is_real
        if duration >= _CHUNK_MIN_DURATION:
            self._transcribe_chunked(job, duration)
            return

        # Progress line — ports but rewritten
        # per 2026-04-23 user feedback notes on the 3rd screenshot:
        #  * "[1/1]" counter → replaced with a colored em-dash. A 1/1
        #    placeholder for the never-built batch feature was
        #    clutter; the line now reads naturally as a continuation
        #    of the channel's block.
        #  * Every tick carries the `tx_done_<vid>` marker so
        #    `_inplaceKind` resolves it to the placeholder sync.py
        #    reserved under THIS channel's header. Without this, the
        #    progress line landed at the log tail (wherever sync was
        #    currently processing) — on a 103-channel pass that meant
        #    the "89%..." tick for channel 69 appeared under channel
        #    72's header, visually orphaned. Now each tick replaces
        #    the reserved slot in place and stays glued to channel 69.
        #  * The per-job `job_tag` stays alongside `tx_done_<vid>` so
        #    ticks also can replace each other within this video's
        #    family (belt-and-suspenders — `_inplaceKind` prefers the
        #    `tx_done_` prefix so that path wins anyway).
        # Title truncated to match OLD's _trunc_pad_title visual width.
        _disp_title = title[:40].rstrip()
        _t_start = time.time() # for the " — ✓ Transcription (took Xs)" line below
        _vid_marker = (job.get("video_id") or "").strip()
        _tx_marker = f"tx_done_{_vid_marker}" if _vid_marker else ""
        _tag = lambda *extra: [t for t in (_tx_marker, job_tag, *extra) if t]
        # Match the done line's indent so the in-place "Transcribing
        # 25%..." -> "- v Transcription (...)" replacement
        # doesn't visibly jump from 1-space to 6-space leading whitespace.
        _prog_lead = "      " if job.get("from_download") else " "
        def _emit_progress(pct, suffix=""):
            # Em-dash + space in the whisper_bracket color matches the
            # other inline per-video lines (download ✓, metadata ✓,
            # etc.) so the block reads as one visual unit.
            self._stream.emit([
                [f"{_prog_lead}\u2014 ", _tag("whisper_bracket")],
                ["Transcribing", _tag()],
                [f' "{_disp_title}"', _tag()],
                [", ", _tag()],
                [f"{pct}%", _tag("whisper_pct")],
                [f"{suffix}...\n", _tag()],
            ])
        _emit_progress(0)

        # Request. Pass the parent's ffprobe duration as a fallback so
        # the worker's progress emitter still has a denominator even
        # when faster-whisper's info.duration comes back None/0 (audit:
        # transcribe/core.py:1303 / 1729). vad_filter occasionally
        # rejects everything on silent-intro videos and reports 0,
        # which silently disabled all "[%]" progress emits before this.
        req = json.dumps({
            "path": path,
            "duration": 0,
            "duration_fallback": float(duration) if duration else 0.0,
        }) + "\n"
        proc, q = self._snapshot_worker_io()
        if proc is None or q is None:
            return
        try:
            proc.stdin.write(req)
            proc.stdin.flush()
        except Exception as e:
            # Suppress the error toast when cancel was already
            # requested — BrokenPipeError on cancel is normal cleanup,
            # not a real failure (audit: transcribe/core.py:1303-1310).
            if not (job["cancel"].is_set() or self._cancel_all.is_set()):
                self._stream.emit_error(f"Write to whisper failed: {e}")
            self._stop_subprocess()
            return

        # Read responses until we get "ok" or "error"
        last_pct = -1
        result = None
        while True:
            if job["cancel"].is_set() or self._cancel_all.is_set():
                # tag the cancel line with this job's
                # inplace family so it REPLACES the last progress
                # tick in place. Old behavior emitted an untagged
                # red line that landed at the log tail while the
                # "25%..." tick stayed visible above it, confusing
                # the user into thinking both were still active.
                _job_tag_c = job.get("job_tag", "") or ""
                _tag_list = ["red"]
                if _job_tag_c:
                    _tag_list.append(_job_tag_c)
                self._stream.emit([
                    [" \u26d4 Transcription cancelled.\n", _tag_list]
                ])
                if self._cancel_all.is_set():
                    self._stop_subprocess(force=True)
                elif not self._graceful_cancel_current():
                    self._stop_subprocess(force=True)
                return
            try:
                _proc_snapshot, q = self._snapshot_worker_io()
                if q is None:
                    if job["cancel"].is_set() or self._cancel_all.is_set():
                        return
                    self._stream.emit_error("Transcription stopped unexpectedly. Try again.")
                    return
                line = q.get(timeout=0.5)
            except queue.Empty:
                continue
            if line is None:
                self._stream.emit_error("Transcription stopped unexpectedly. Try again.")
                self._emit_whisper_stderr_tail()
                return
            try:
                msg = json.loads(line.strip())
            except json.JSONDecodeError:
                continue
            status = msg.get("status")
            if status == "progress":
                pct = int(msg.get("pct", 0))
                if pct != last_pct:
                    last_pct = pct
                    _emit_progress(pct)
                continue
            if status == "starting":
                continue
            if status == "cancelled":
                return
            if status == "ok":
                result = msg
                break
            if status == "error":
                err = msg.get('text', 'unknown')
                # CUDA OOM recovery: kill the subprocess, fall back to CPU,
                # and requeue this job at the front.
                low = err.lower()
                if ("cuda" in low and ("out of memory" in low or "oom" in low)) or "cublas" in low:
                    self._stream.emit_error(f"Transcription ran out of GPU memory: {err}")
                    self._stream.emit_text(
                        " \u21A9 Falling back to CPU mode for this job.",
                        "simpleline_blue")
                    self._stop_subprocess(force=True)
                    # Flag-only: the next _start_subprocess reads
                    # `self._cpu_fallback_active` and builds its env
                    # accordingly. No more os.environ mutation (audit:
                    # H51) — global mutation would leak into any
                    # sibling subprocess spawned in between.
                    self._cpu_fallback_active = True
                    # Requeue this job (but not in a loop — bail if it fails again)
                    if not job.get("_retried_cpu"):
                        job["_retried_cpu"] = True
                        # Mark the pending-counter as already
                        # decremented for THIS failed attempt so the
                        # outer worker-loop's per-iteration finally
                        # skips the decrement. Without this flag the
                        # finally block decrements pending for both
                        # the failed CUDA attempt AND the CPU retry,
                        # causing transcription_pending to underflow
                        # to 0 prematurely while the retry is still
                        # running.
                        job["_pending_decremented"] = True
                        job["_requeued_no_tally"] = True
                        with self._jobs_lock:
                            self._jobs.insert(0, job)
                    return
                self._stream.emit_error(f"Transcription error: {err}")
                self._emit_whisper_traceback(msg)
                return

        # Write output files + ingest into FTS index
        if result:
            channel = job.get("channel") or ""
            # Run punctuation pass over the raw text (and each segment's t)
            if self._punctuate_enabled:
                # track whether punct succeeded so the source
                # tag can reflect reality. Previously a failed punct
                # pass left the tag as "(WHISPER:model)" even though
                # the text was unpunctuated — users assumed punctuation
                # was present in the Watch banner.
                # Only mark `_punct_attempted = True` when we ACTUALLY
                # call punctuate(). For silent videos with empty text
                # the prior code set attempted=True but never made the
                # call, so the source tag wrongly read "+NO-PUNCT"
                # (audit: H73).
                result["_punct_success"] = False
                result["_punct_timeout"] = False  # bug [43]
                result.setdefault("_punct_attempted", False)
                try:
                    raw_text = result.get("text", "") or ""
                    if raw_text:
                        result["_punct_attempted"] = True
                        punct_text = self._punct.punctuate(raw_text)
                        # Bug [43]: surface a timeout-specific signal so
                        # downstream code (source tag, summary log) can
                        # distinguish "model wedged" from other failures.
                        if getattr(self._punct, "last_was_timeout", False):
                            result["_punct_timeout"] = True
                        if punct_text and punct_text != raw_text:
                            result["text"] = punct_text
                            # Align punctuated whole-text back to segments by
                            # word offset — no per-segment subprocess calls
                            # (T150). Pure Python, completes in microseconds.
                            _punct_align_segments(
                                punct_text, result.get("segments", []))
                            result["_punct_success"] = True
                except Exception as _pe:
                    self._stream.emit_dim(f" (punctuation pass skipped: {_pe})")
            self._write_outputs(path, result, title=title, channel=channel,
                                combined_override=job.get("combined_override"),
                                retranscribe=bool(job.get("retranscribe")),
                                video_id_hint=job.get("video_id", ""),
                                job=job)
            # audit A-1 real fix: set _pending_decremented on the job
            # dict HERE (caller scope, where `job` actually exists)
            # instead of inside _write_outputs which doesn't take job
            # as a parameter. Previously this line lived at
            # _write_outputs:2973 and raised NameError on every
            # Whisper transcription — the .txt/.jsonl wrote fine but
            # the done line, FTS ingest, and pending-counter
            # bookkeeping all got skipped. Mirrors the cleaner fix I
            # described in the audit; the edit just never actually
            # landed the first time around.
            if not job.get("retranscribe"):
                job["_pending_decremented"] = True
            # Done line — in-place replaces the sync.py-reserved
            # `tx_done_<vid>` placeholder under the channel's block
            # (`_inplaceKind` prioritizes `tx_done_` over `whisper_job_`),
            # so the final line lands at the right scroll position
            # instead of wherever the GPU worker happened to finish.
            # The `whisper_job_<N>` tag is retained for in-batch
            # progress-tick replacement.
            _elapsed = max(1, int(time.time() - _t_start))
            _time_str = (f"{_elapsed // 60}min {_elapsed % 60:02d}sec"
                          if _elapsed >= 60 else f"{_elapsed}sec")
            # include the model name and realtime ratio so
            # the done line reads "Transcription (Whisper small, took
            # 55sec, 12.3x realtime)" instead of just "(took 55sec)".
            _model_label = (self._model or "").strip()
            # Only emit a realtime ratio when we have a real ffprobe
            # duration — otherwise the displayed "Nx realtime" is
            # derived from a chunking-routing sentinel, not the actual
            # video length (audit: H70).
            _realtime_str = (f"{duration / _elapsed:.1f}x realtime"
                             if _elapsed > 0 and duration > 0
                             and job.get("_duration_is_real", True) else "")
            _detail_parts = []
            if _model_label:
                _detail_parts.append(f"Whisper {_model_label}")
            _detail_parts.append(f"took {_time_str}")
            if _realtime_str:
                _detail_parts.append(_realtime_str)
            _detail_str = ", ".join(_detail_parts)
            _vid_for_marker = (job.get("video_id") or "").strip()
            _tx_tag = f"tx_done_{_vid_for_marker}" if _vid_for_marker else ""
            # _tx_tag FIRST in each tag list so logs.js `_inplaceKind`
            # resolves this line to tx_done_<vid> and matches the
            # placeholder emitted by sync.py. Putting _tx_tag last let
            # the renderer hit `whisper_job_N` first and return that,
            # so the done line couldn't find the placeholder and
            # appended fresh below — leaving both "⏳ Transcription
            # queued…" and "✓ Transcription (took Xsec)" visible.
            # (logs.js has also been fixed to scan all tags with
            # tx_done_ priority first; this is belt-and-suspenders.)
            _dim_tags = [t for t in (_tx_tag, "dim", job_tag) if t]
            # Parens detail (Whisper model, elapsed, realtime) uses
            # `tx_detail` — brighter than `dim` so the detail is actually
            # readable but still subordinate to the main label.
            _detail_tags = [t for t in (_tx_tag, "tx_detail", job_tag) if t]
            _em_tags = [t for t in (_tx_tag, "whisper_bracket", job_tag) if t]
            _lbl_tags = [t for t in (_tx_tag, "simpleline_blue", job_tag) if t]
            _txt_tags = [t for t in (_tx_tag, "simpleline", job_tag) if t]
            # Indent under the parent " \u2014 \u2713 Title (size)" video row when
            # this transcription is part of a sync's download flow.
            # Standalone transcribes (Transcribe File, drift retranscribe,
            # Watch-view retranscribe) have no parent video line in the
            # log, so keep the original 1-space indent \u2014 those rows also
            # splice the title onto the end of the done line below.
            # Shared done-line builder (T167); single-pass threads the
            # tx_done_<vid> marker via the *_tags families and uses the
            # tx_detail trailing tag.
            _segs = _build_transcription_done_segments(
                job, title, channel, _detail_str,
                dim_tags=_dim_tags, em_tags=_em_tags, lbl_tags=_lbl_tags,
                txt_tags=_txt_tags, detail_tags=_detail_tags)
            self._stream.emit(_segs)
            if job.get("cb"):
                try:
                    job["cb"](result)
                except Exception as e:
                    _log.debug("swallowed: %s", e)

    def _transcribe_chunked(self, job: dict[str, Any], total_duration: float):
        """Port of YTArchiver.py:11139 _whisper_transcribe_chunked.

        ffmpeg splits the audio into 2h windows with 30s of overlap; each
        chunk is transcribed individually and their segment lists are merged
        with timestamps offset, dropping duplicates in the overlap zone.
        """
        import tempfile as _tf
        path = job["path"]
        title = job["title"]
        channel = job.get("channel", "")
        cancel = job["cancel"]
        hours = total_duration / 3600.0
        n_chunks = max(1, int(total_duration / _CHUNK_DURATION_SECS) +
                       (1 if total_duration % _CHUNK_DURATION_SECS > 0 else 0))
        _disp_title_chunked = title[:40].rstrip()
        _t_start_chunked = time.time()
        self._stream.emit([
            ["Transcribing ", "transcribe_using"],
            [f'"{title}"', "transcribe_title"],
            [" \u2014 ", "dim"],
            [f"{hours:.1f}h, {n_chunks} sections\n", "simpleline"],
        ])

        all_text_parts: list[str] = []
        all_segments: list[dict[str, Any]] = []
        chunk_dir = _tf.mkdtemp(prefix="yt_whisper_chunk_")
        try:
            for ci in range(n_chunks):
                if cancel.is_set() or self._cancel_all.is_set():
                    break
                # Respect pause between chunks. A 2h chunk could keep the
                # user waiting many minutes; signal "actually paused" so
                # the Resume button stops blinking once we land here.
                if self._paused.is_set() and not cancel.is_set():
                    if self._queues is not None:
                        try: self._queues.set_gpu_paused_active(True)
                        except Exception as e: _log.debug("swallowed: %s", e)
                    while self._paused.is_set() and not cancel.is_set():
                        time.sleep(0.5)
                    if self._queues is not None:
                        try: self._queues.set_gpu_paused_active(False)
                        except Exception as e: _log.debug("swallowed: %s", e)
                if cancel.is_set():
                    break

                start_sec = ci * _CHUNK_DURATION_SECS
                if ci > 0:
                    start_sec -= _CHUNK_OVERLAP_SECS
                end_sec = min(start_sec + _CHUNK_DURATION_SECS +
                              (_CHUNK_OVERLAP_SECS if ci > 0 else 0),
                              total_duration)
                chunk_dur = end_sec - start_sec
                if chunk_dur <= 0:
                    break

                chunk_path = os.path.join(chunk_dir, f"chunk_{ci:03d}.wav")
                ff_cmd = [
                    "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
                    "-ss", str(start_sec), "-t", str(chunk_dur),
                    "-i", path, "-vn", "-ac", "1", "-ar", "16000",
                    "-acodec", "pcm_s16le", chunk_path,
                ]
                # Scale the ffmpeg-split timeout with chunk_dur — old
                # hard-coded 600s could expire mid-split on slow disks
                # (Z: DrivePool + AV) for a 2h chunk, dropping that
                # whole section from the merged transcript (audit:
                # transcribe/core.py:1564-1581). Allow at least 3x
                # realtime per second of chunk audio, with a 1200s
                # floor for short chunks. If a section STILL times
                # out, fail the whole chunked transcribe rather than
                # silently continuing with a 2-hour hole.
                _ff_timeout = max(1200.0, chunk_dur * 3.0)
                try:
                    subprocess.run(
                        ff_cmd, check=True, capture_output=True,
                        timeout=_ff_timeout,
                        creationflags=(0x08000000 if os.name == "nt" else 0),
                    )
                except subprocess.TimeoutExpired as _toe:
                    self._stream.emit_error(
                        f"Section {ci+1}/{n_chunks} split timed out after "
                        f"{int(_ff_timeout)}s — aborting chunked transcribe "
                        f"to avoid silent gaps in the merged transcript.")
                    return
                except Exception as e:
                    self._stream.emit_error(
                        f"Section {ci+1}/{n_chunks} split failed: {e} "
                        f"— aborting chunked transcribe to avoid silent gaps "
                        f"in the merged transcript.")
                    return

                # Hand the chunk to Whisper via the persistent subprocess.
                section_prefix = f" Section {ci+1}/{n_chunks},"
                t_start = time.time()
                result = self._transcribe_single_file(chunk_path, job,
                                                      _log_prefix=section_prefix)
                t_elapsed = time.time() - t_start
                try: os.remove(chunk_path)
                except Exception as e: _log.debug("swallowed: %s", e)

                if not result:
                    if cancel.is_set():
                        return
                    self._stream.emit([
                        [f" Section {ci+1}/{n_chunks} \u2014 no speech\n", "simpleline"],
                    ])
                    continue

                # Log per-section summary
                cd_m, cd_s = divmod(int(chunk_dur), 60)
                te_m, te_s = divmod(int(t_elapsed), 60)
                te_str = f"{te_m}min {te_s:02d}sec" if te_m else f"{te_s}sec"
                rt = f"{chunk_dur / t_elapsed:.1f}x realtime" if t_elapsed > 0 else ""
                self._stream.emit([
                    [f" Section {ci+1}/{n_chunks} done "
                     f"({cd_m}m{cd_s:02d}s, {te_str}, {rt})\n", "simpleline_blue"],
                ])

                txt = result.get("text") or ""
                if txt:
                    all_text_parts.append(txt)
                segs = result.get("segments") or []
                # Offset timestamps, drop overlap duplicates from the new chunk
                for s in segs:
                    if "s" in s: s["s"] = round(s["s"] + start_sec, 2)
                    if "e" in s: s["e"] = round(s["e"] + start_sec, 2)
                    for w in s.get("w", []):
                        if "s" in w: w["s"] = round(w["s"] + start_sec, 3)
                        if "e" in w: w["e"] = round(w["e"] + start_sec, 3)
                if ci > 0 and segs:
                    # Strict >= overlap_boundary so segments whose start
                    # falls in [boundary-2, boundary) aren't counted
                    # by BOTH chunks. Old `>= boundary - 2` buffer
                    # let 2 seconds of segments at the seam land
                    # twice in the merged .jsonl on 2h+ videos
                    # (audit: transcribe/core.py:1611-1622).
                    overlap_boundary = start_sec + _CHUNK_OVERLAP_SECS
                    segs = [s for s in segs if s.get("s", 0) >= overlap_boundary]
                all_segments.extend(segs)

            # Merge result
            if not all_segments and not all_text_parts:
                return
            # Rebuild text from deduped segments, NOT from all_text_parts.
            # all_text_parts contains each chunk's full body including the
            # 30s overlap window, so joining them duplicated ~60s of
            # speech at every chunk seam in the merged .txt while the
            # .jsonl segments were correctly deduped — the two sidecars
            # diverged by content on every multi-hour video (audit:
            # transcribe/core.py C9). Single-source from segments now.
            if all_segments:
                merged_text = " ".join(
                    (s.get("t") or s.get("text") or "").strip()
                    for s in all_segments
                    if (s.get("t") or s.get("text") or "").strip()
                )
            else:
                merged_text = " ".join(all_text_parts)
            merged = {
                "text": merged_text,
                "segments": all_segments,
            }
            # Optional punctuation pass on the merged text (same as single-pass).
            # also iterate each segment and punctuate its text
            # so the .jsonl (source of Watch-view karaoke + FTS search)
            # reads consistently punctuated. Previously only the merged
            # concatenated text got punctuated, leaving .jsonl segments
            # as raw lowercase Whisper output — Watch view and search
            # results looked different from the .txt.
            merged["_punct_attempted"] = False
            merged["_punct_success"] = False
            if self._punctuate_enabled and merged["text"]:
                merged["_punct_attempted"] = True
                try:
                    punct = self._punct.punctuate(merged["text"])
                    if punct and punct != merged["text"]:
                        merged["text"] = punct
                        merged["_punct_success"] = True
                        # Align punctuated whole-text back to segments by
                        # word offset — no per-segment subprocess calls (T150).
                        _punct_align_segments(punct, merged["segments"])
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            self._write_outputs(path, merged, title=title, channel=channel,
                                combined_override=job.get("combined_override"),
                                retranscribe=bool(job.get("retranscribe")),
                                video_id_hint=job.get("video_id", ""),
                                job=job)
            # audit A-1 real fix: same as the single-pass caller above.
            # Flag belongs on the `job` dict in caller scope; never
            # inside _write_outputs (which has no `job` param).
            if not job.get("retranscribe"):
                job["_pending_decremented"] = True
            # Done line — REPLACES the last whisper_progress chunk line
            # in place via `whisper_progress` inplace kind. Matches OLD
            # YTArchiver.py:16495 format with (chunked) suffix to
            # distinguish the long-video path.
            _elapsed_c = max(1, int(time.time() - _t_start_chunked))
            _time_str_c = (f"{_elapsed_c // 60}min {_elapsed_c % 60:02d}sec"
                            if _elapsed_c >= 60 else f"{_elapsed_c}sec")
            # Simple-mode per-video summary (chunked variant). Same
            # three-line-per-video spec as the non-chunked path; the
            # suffix notes (chunked, <time>) so long-video behavior is
            # still visible without the title/index clutter. Tagged
            # with this job's `job_tag` so it survives past later
            # videos' transcription emits.
            _job_tag_ch = job.get("job_tag", "") or ""
            _em_tag = ["whisper_bracket", _job_tag_ch] if _job_tag_ch else "whisper_bracket"
            _dim_tag = ["dim", _job_tag_ch] if _job_tag_ch else "dim"
            _lbl_tag = ["simpleline_blue", _job_tag_ch] if _job_tag_ch else "simpleline_blue"
            _txt_tag = ["simpleline", _job_tag_ch] if _job_tag_ch else "simpleline"
            # Match the lead-indent used by the single-pass done line
            # at 1624: 6 spaces when this transcribe was triggered by a
            # download (so it threads under the [Dwnld] row), one
            # space otherwise. Without this the chunked path produced
            # a single-space line that visually misaligned in sync logs
            # (audit: H62).
            # Shared done-line builder (T167); chunked uses the job_tag tag
            # families and a dim trailing detail with the "chunked, took\u2026"
            # text instead of the single-pass model/realtime detail.
            _segs_c = _build_transcription_done_segments(
                job, title, channel, f"chunked, took {_time_str_c}",
                dim_tags=_dim_tag, em_tags=_em_tag, lbl_tags=_lbl_tag,
                txt_tags=_txt_tag, detail_tags=_dim_tag)
            self._stream.emit(_segs_c)
            if job.get("cb"):
                try: job["cb"](merged)
                except Exception as e: _log.debug("swallowed: %s", e)
        finally:
            try: shutil.rmtree(chunk_dir, ignore_errors=True)
            except Exception as e: _log.debug("swallowed: %s", e)

    def _transcribe_single_file(self, path: str, job: dict[str, Any],
                                 _log_prefix: str = "") -> dict[str, Any] | None:
        """Send one file to the persistent whisper subprocess and collect the
        result. Used by the chunked path to do each section. Returns the
        parsed JSON from the worker (keys: text, segments) or None.

        emits in-place progress ticks tagged with the
        current job's `job_tag` + the section prefix. Before this,
        chunked transcription looked frozen: a 6-hour video would
        show 3 "Section N/M done" lines over 2 hours of wall time
        with zero feedback in between. Now each chunk displays its
        own progress bar. also honors pause INSIDE
        the read loop so a 2-hour chunk can be paused mid-run.
        """
        proc, _line_q = self._snapshot_worker_io()
        if proc is None or proc.poll() is not None:
            if not self.start_subprocess():
                return None
            proc, _line_q = self._snapshot_worker_io()
        if proc is None:
            return None
        try:
            # Pass ffprobe duration as fallback so the worker can still
            # render progress on chunks where info.duration is 0
            # (audit: transcribe/core.py:1303 / 1729).
            _chunk_dur = _ffprobe_duration(path) or 0.0
            req = json.dumps({
                "path": path, "duration": 0,
                "duration_fallback": float(_chunk_dur),
            }) + "\n"
            proc.stdin.write(req)
            proc.stdin.flush()
        except Exception as e:
            self._stream.emit_error(f"Write to whisper failed: {e}")
            return None
        _last_pct = -1
        _job_tag_p = (job.get("job_tag") or "") if isinstance(job, dict) else ""
        _prefix_str = (_log_prefix or "").strip()
        while True:
            if job["cancel"].is_set() or self._cancel_all.is_set():
                if self._cancel_all.is_set():
                    self._stop_subprocess(force=True)
                elif not self._graceful_cancel_current():
                    self._stop_subprocess(force=True)
                return None
            # pause also polled inside the read loop so a
            # long chunk mid-transcription can actually pause, not
            # just at chunk boundaries. Signal "actually paused" so the
            # Resume button stops blinking once we land in the wait.
            if (self._paused.is_set()
                    and not job["cancel"].is_set()
                    and not self._cancel_all.is_set()):
                if self._queues is not None:
                    try: self._queues.set_gpu_paused_active(True)
                    except Exception as e: _log.debug("swallowed: %s", e)
                while (self._paused.is_set()
                       and not job["cancel"].is_set()
                       and not self._cancel_all.is_set()):
                    time.sleep(0.5)
                if self._queues is not None:
                    try: self._queues.set_gpu_paused_active(False)
                    except Exception as e: _log.debug("swallowed: %s", e)
            if job["cancel"].is_set() or self._cancel_all.is_set():
                if self._cancel_all.is_set():
                    self._stop_subprocess(force=True)
                elif not self._graceful_cancel_current():
                    self._stop_subprocess(force=True)
                return None
            try:
                _proc_snapshot, q = self._snapshot_worker_io()
                if q is None:
                    if job["cancel"].is_set() or self._cancel_all.is_set():
                        return None
                    self._stream.emit_error("Transcription stopped unexpectedly. Try again.")
                    return None
                line = q.get(timeout=0.5)
            except queue.Empty:
                continue
            if line is None:
                self._stream.emit_error("Transcription stopped unexpectedly. Try again.")
                self._emit_whisper_stderr_tail()
                return None
            try:
                msg = json.loads(line.strip())
            except json.JSONDecodeError:
                continue
            status = msg.get("status")
            if status == "progress":
                # audit D-20 + SR-3: emit an in-place progress bar
                # tagged with the per-job inplace family AND the
                # tx_done_<vid> marker so each tick replaces the
                # sync.py-reserved placeholder under the channel's
                # block (not the log tail, which drifts as sync
                # moves on to later channels). `tx_done_` wins in
                # `_inplaceKind` so the line stays glued to the
                # reserved slot.
                pct = int(msg.get("pct", 0))
                if pct != _last_pct:
                    _last_pct = pct
                    _vid_p = (job.get("video_id") or "").strip() if isinstance(job, dict) else ""
                    _marker_p = f"tx_done_{_vid_p}" if _vid_p else ""
                    _tag_list = [t for t in (_marker_p, "whisper_progress", _job_tag_p) if t]
                    _label = f"{_prefix_str} {pct}%..." if _prefix_str else f"{pct}%..."
                    self._stream.emit([[_label + "\n", _tag_list]])
                continue
            if status == "starting":
                continue
            if status == "cancelled":
                return None
            if status == "ok":
                return msg
            if status == "error":
                self._stream.emit_error(
                    f"Whisper error{(' (' + _log_prefix.strip() + ')') if _log_prefix else ''}: "
                    f"{msg.get('text', 'unknown')}")
                self._emit_whisper_traceback(msg)
                return None

    def _write_outputs(self, video_path: str, result: dict[str, Any],
                       title: str = "", channel: str = "",
                       combined_override: bool | None = None,
                       retranscribe: bool = False,
                       video_id_hint: str = "",
                       job: dict | None = None):
        """Write a transcript entry to the aggregated {ch} Transcript.txt
        + hidden JSONL sidecar. Matches YTArchiver.py:15449-15478 output
        layout exactly, so OLD YTArchiver can read transcripts written
        here (and vice versa) with zero drift.

        `combined_override` mirrors the job-level flag; forwarded to
        `_resolve_transcript_paths` so the user's first-time
        "Follow / Combined" choice is honoured per video.

        `retranscribe=True` swaps the default append-writers for the
        surgical replace-writers so the old entry for this video gets
        removed from BOTH aggregated files before the new one is
        appended — prevents duplicates in the .txt / .jsonl + the FTS DB.
        Matches YTArchiver.py:16455-16474 retranscribe sequence.
        `video_id_hint` provides the canonical id when the filename
        doesn't carry `[videoId]` — helps `_replace_jsonl_entry` find
        title-drifted stale entries.
        """
        # Bail early on cancel_all OR per-job Skip so the user's Skip
        # click during the Whisper response stage doesn't still commit
        # a transcript to disk (audit: transcribe/core.py:783-799 +
        # transcribe H60 — previously only `_cancel_all` was checked,
        # letting a per-job `job["cancel"]` slip past).
        if self._cancel_all.is_set():
            raise RuntimeError("cancelled before write")
        try:
            _job_cancel_ev = (job or {}).get("cancel") if isinstance(job, dict) else None
            if _job_cancel_ev is not None and _job_cancel_ev.is_set():
                raise RuntimeError("cancelled before write (per-job skip)")
        except RuntimeError:
            raise
        except Exception:
            pass
        if not title:
            title = os.path.basename(video_path).rsplit(".", 1)[0]
            # Strip any trailing " [videoId]" if the stem has one
            title = re.sub(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$", "", title) or title
        if not channel:
            # Channel = parent folder name (or parent-of-parent when year-split).
            # bound the "looks like a year/month folder" test
            # so channels with names starting with a digit (e.g.
            # "5 Minute Crafts", or similar) don't get their
            # grandparent misidentified as the channel. Require
            # either a 4-digit year OR a "NN Month" pattern with NN
            # in [01..12].
            parent = os.path.basename(os.path.dirname(video_path))
            grand = os.path.basename(os.path.dirname(os.path.dirname(video_path)))
            # Heuristic: if parent is a year like "2024" or matches "01 January",
            # the real channel is one level higher.
            _is_year = (parent.isdigit() and len(parent) == 4
                        and 1900 < int(parent) < 2100)
            _is_month = False
            if " " in parent:
                _first, _rest = parent.split(" ", 1)
                if _first.isdigit() and 1 <= int(_first) <= 12:
                    # "01 January" format — the rest is a month name.
                    _month_names = {"january", "february", "march", "april",
                                    "may", "june", "july", "august",
                                    "september", "october", "november", "december"}
                    if _rest.strip().lower() in _month_names:
                        _is_month = True
            if _is_year or _is_month:
                channel = grand
            else:
                channel = parent

        # Resolve OLD-layout paths for this video.
        paths = _resolve_transcript_paths(video_path, title, channel,
                                          combined_override=combined_override)
        if paths is None:
            # Fall back to per-video sidecar in the video's folder (degraded).
            base = os.path.splitext(video_path)[0]
            txt_path = base + ".txt"
            jsonl_path = base + ".jsonl"
            upload_date = ""
        else:
            txt_path, jsonl_path, _y, _m, upload_date = paths

        text = (result.get("text") or "").strip()
        segs = result.get("segments", []) or []

        # refuse to write an "empty-but-successful" transcript.
        # Whisper can return rc=0 with `text=""` when audio is pure
        # silence, corrupted, or the model produced no output at all.
        # Before this guard, the empty result was written to disk and
        # the FTS index was updated to mark the video transcribed,
        # blocking any future retranscribe. Treat as an error so the
        # caller can surface it and leave the video un-transcribed.
        _has_any_seg_text = any(
            (s.get("t") or s.get("text") or "").strip()
            for s in segs if isinstance(s, dict))
        if not text and not _has_any_seg_text:
            # Persist a TERMINAL 'no_speech' status so this silent / music-
            # only video is not re-attempted by auto + bulk transcribe passes,
            # and so the Watch view can say "No speech detected" instead of the
            # generic "No transcript available." We STILL raise below so the
            # worker loop emits the benign "no speech detected" line and counts
            # the job as done (not an error). Best-effort: a DB hiccup here
            # must not turn a benign empty result into a hard crash.
            try:
                from .. import index as _idx
                _idx.mark_video_no_speech(video_path)
            except Exception as _nse:
                _log.debug("mark_video_no_speech(%s) failed: %s",
                           video_path, _nse)
            raise RuntimeError(
                "Whisper returned an empty transcript "
                "(no text, no non-empty segments) — refusing to "
                "write an empty .txt/.jsonl and mark the video "
                "transcribed. Audio may be silent or unreadable.")

        # Extract video id — OLD-compat filenames don't carry the `[id]`
        # suffix. Order: hint -> filename `[id]` -> FTS `videos` table.
        # consolidated into _extract_video_id helper.
        vid_id = _extract_video_id(video_path, hint=video_id_hint or "")

        # Source tag: use the manager's active model so the Transcript.txt
        # header carries the right "(WHISPER:<model>)" tag even when
        # whisper_worker.py's response dict doesn't include "model"
        # (which it doesn't — only status/text/segments come back).
        # Without this, the Watch view banner shows just "Whisper
        # transcription" with no model name. this was flagged
        model_name = (result.get("model") or self._model or "").strip()
        # when punctuation was attempted but failed, append
        # "+NO-PUNCT" to the source tag so the Watch banner accurately
        # reflects that the transcript is unpunctuated. Otherwise the
        # user sees "Whisper:large-v3" and assumes punct is present.
        _punct_attempted = bool(result.get("_punct_attempted"))
        _punct_success = bool(result.get("_punct_success"))
        _punct_timeout = bool(result.get("_punct_timeout"))
        _punct_suffix = ""
        if _punct_attempted and not _punct_success:
            # Distinguish timeout from generic no-punct so the user
            # can tell at-a-glance why a transcript is unpunctuated.
            # The _punct_timeout flag was already being set by the
            # punctuation manager but the source tag never read it
            # (audit: transcribe/core.py H69).
            _punct_suffix = "+TIMEOUT" if _punct_timeout else "+NO-PUNCT"
        if model_name:
            source_tag = f"(WHISPER:{model_name}{_punct_suffix})"
        else:
            source_tag = f"(WHISPER{_punct_suffix})"
        # Diagnostic — emit the tag we're about to write so we can
        # confirm it landed correctly. Visible in Verbose log mode.
        try:
            self._stream.emit_dim(
                f" (writing transcript source_tag={source_tag!r})")
        except Exception as e:
            _log.debug("swallowed: %s", e)

        duration = segs[-1].get("end", segs[-1].get("e", 0)) if segs else 0

        if retranscribe:
            # Surgically swap the old entries in both aggregated files.
            # Mirrors YTArchiver.py:16462-16474: jsonl FIRST so its
            # video_id-based purge can report back any title-drifted
            # stale entries for the txt pass to also clean up.
            # two-step replace was non-atomic — if .jsonl
            # succeeded but .txt failed (lock, permission) the video
            # ended up with new segments + old text, permanently
            # inconsistent. Mitigation: try .jsonl first; if it
            # fails, abort before touching .txt so the old content
            # remains intact on BOTH files. If .jsonl succeeds but
            # .txt fails, surface a prominent error and attempt a
            # roll-back by re-reading the backup we captured first.
            _jsonl_backup: bytes | None = None
            _backup_failed = False
            try:
                with open(jsonl_path, "rb") as _jb:
                    _jsonl_backup = _jb.read()
            except FileNotFoundError:
                # First-ever retranscribe on a fresh .jsonl — no
                # backup needed because there's no prior state to
                # roll back to.
                _jsonl_backup = None
            except OSError as _bke:
                _jsonl_backup = None
                _backup_failed = True
                # Fail FAST before touching the .jsonl when we can't
                # capture a backup — otherwise a .txt failure later
                # would leave new .jsonl + old .txt with no way to
                # recover (audit: transcribe/core.py H52).
                self._stream.emit_error(
                    f"Refusing retranscribe of "
                    f"{os.path.basename(jsonl_path)}: backup capture "
                    f"failed ({_bke}). Files left untouched.")
                return
            try:
                extra_titles = _replace_jsonl_entry(
                    jsonl_path, title, vid_id, segs) or set()
            except Exception as _je:
                self._stream.emit_error(
                    f"Could not update {os.path.basename(jsonl_path)}: {_je}"
                    f" — .txt left unchanged to avoid split-state.")
                return
            try:
                _old_txt_candidates = _jsonl_text_candidates_from_bytes(
                    _jsonl_backup, title, vid_id)
                _replace_txt_entry(txt_path, title, text, source_tag,
                                   extra_titles_to_remove=extra_titles,
                                   old_text_candidates=_old_txt_candidates)
            except Exception as _te:
                self._stream.emit_error(
                    f"Could not update {os.path.basename(txt_path)}: {_te}"
                    f" — attempting .jsonl roll-back to prevent split-state.")
                # Best-effort .jsonl roll-back so the two files stay
                # consistent. If the roll-back itself fails the user
                # is notified with a clear message.
                if _jsonl_backup is not None:
                    try:
                        # Atomic, hidden-aware roll-back. The old
                        # in-place open('wb') NEVER worked here:
                        # _replace_jsonl_entry re-hides the file in its
                        # finally block, and on Windows CreateFileW
                        # refuses to truncate a FILE_ATTRIBUTE_HIDDEN
                        # file (PermissionError) — so the roll-back was
                        # dead code for every hidden transcript .jsonl.
                        import tempfile as _tf

                        from .paths import _hide_file_win as _rb_hide
                        _fd, _rb_tmp = _tf.mkstemp(
                            suffix=".jsonl.tmp",
                            dir=os.path.dirname(jsonl_path) or ".")
                        with os.fdopen(_fd, "wb") as _jw:
                            _jw.write(_jsonl_backup)
                            try:
                                _jw.flush()
                                os.fsync(_jw.fileno())
                            except OSError:
                                pass
                        try: _rb_hide(_rb_tmp)
                        except Exception: pass
                        os.replace(_rb_tmp, jsonl_path)
                        try: _rb_hide(jsonl_path)
                        except Exception: pass
                        self._stream.emit_error(
                            f"Rolled {os.path.basename(jsonl_path)} back "
                            f"— files consistent; retry retranscribe "
                            f"when {os.path.basename(txt_path)} is "
                            f"writable.")
                    except OSError as _re:
                        self._stream.emit_error(
                            f"Roll-back of {os.path.basename(jsonl_path)} "
                            f"FAILED: {_re}. Files may be out of sync; "
                            f"retry retranscribe when writable.")
                # Either way, do NOT fall through to the FTS ingest —
                # the txt update failed, so indexing the new segments
                # (or re-marking the video transcribed) would certify
                # a state the visible transcript doesn't match.
                return
            _hide_per_video_transcript_txt_if_needed(video_path, txt_path)
        else:
            if not _write_transcript_entry(txt_path, title, upload_date,
                                           duration, source_tag, text):
                self._stream.emit_error(f"Could not write transcript to {txt_path}")
                return
            _hide_per_video_transcript_txt_if_needed(video_path, txt_path)
            if not _write_jsonl_entry(jsonl_path, vid_id, title, segs):
                self._stream.emit_error(
                    f"Could not write transcript JSONL to {jsonl_path} "
                    f"— not marking {os.path.basename(video_path)} transcribed")
                return

        # Ingest into FTS index — `ingest_jsonl` does a DELETE WHERE
        # jsonl_path=? first, so re-ingesting after a retranscribe
        # wipes + rebuilds the segments for the WHOLE aggregated file
        # (harmless for the other videos sharing it — their lines in
        # the .jsonl are untouched and get re-inserted as-is).
        try:
            from .. import index as _idx
            _idx.ingest_jsonl(video_path, jsonl_path, title, channel)
            _idx.mark_video_transcribed(video_path)
        except Exception as e:
            # Bug [101]: was emit_dim — invisible in Simple log mode. The
            # transcript file IS on disk but FTS is out of sync (search
            # won't find this video). User-actionable, so use the red
            # convention used elsewhere for warnings/failures so it shows
            # in Simple mode too.
            self._stream.emit_text(
                f" \u26a0 FTS index sync failed for {os.path.basename(video_path)}: {e}",
                "red")

        # Decrement transcription_pending / set transcription_complete on 0.
        # Matches YTArchiver.py:14629-14630. Skip the decrement on
        # retranscribe — it wasn't incremented when the Re-transcribe
        # button was clicked (unlike a normal sync-triggered transcribe).
        if not retranscribe:
            _bump_transcription_pending(channel, -1)
            # Drain the authoritative pending-ID list too.
            if vid_id:
                try:
                    from .. import ytarchiver_config as _cfg
                    _cfg.remove_pending_tx_id(vid_id)
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            # mark decrement-done so the worker-loop's
            # exception finally doesn't decrement AGAIN on the success
            # path. The finally's decrement exists only for error paths
            # (Whisper crash, OOM, venv missing) that used to leak the
            # counter and leave the Subs row stuck at `-N`.
            # the flag-set moved to the CALLERS because
            # _write_outputs doesn't take `job` as a parameter —
            # referencing it here raised NameError on every Whisper
            # transcription. See the two `job["_pending_decremented"]
            # = True` assignments in _transcribe_one / _transcribe_chunked.
            # (No code here — this comment is just a tombstone so
            # `git log -S job\[` and future greps find it.)
