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

import copy
import json
import os
import queue
import re
import shutil
import subprocess
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ..log_stream import LogStreamer
from ..queues import make_task_id

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
_SUPPORTED_WHISPER_MODELS = frozenset({"tiny", "small", "medium", "large-v3"})


# ── OLD YTArchiver-compatible transcript file helpers ──────────────────
# These mirror the file layout + content format the legacy YTArchiver.py
# uses so we're a bit-for-bit drop-in replacement. Do NOT change these
# names or formats — OLD's scan/match logic depends on them exactly.

# Shared with metadata.py + reorg.py — see backend.utils.MONTH_FOLDERS.

from ..log import get_logger

_log = get_logger(__name__)


# path + format + hide helpers moved to
# transcribe/paths.py. Re-imported here so internal calls and external
# `from backend.transcribe import _foo` callers keep working.
# Pure helpers and PunctuationManager live in helpers.py and
# punct_manager.py. Re-imported here so this module's
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
from .job_execution import (
    TranscriptionJobExecutor,
    apply_control_signals,
    execution_decision,
)
from .job_execution import WorkerOutcome as _WorkerOutcome
from .paths import (  # noqa: F401
    _get_jsonl_sidecar,
    _get_transcript_filename,
    _hide_per_video_transcript_txt_if_needed,
)
from .punct_manager import PunctuationManager  # noqa: F401

# ── Aggregated transcript writers ─────────────────────────────────────
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

# ── VTT caption path ──────────────────────────────────────────────────
from .transcribe_vtt import (  # noqa: F401  (re-exports for backend.transcribe surface)
    _CaptionOutcome,
    _parse_vtt,
    _try_auto_captions,
)

# ── Manager ────────────────────────────────────────────────────────────

def _pending_journal_path() -> Path:
    """Where the pending-transcribe journal lives.

    A global journal at APPDATA/ytarchiver_pending_transcribe.json lets
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
    for seg, n in zip(segments, raw_counts, strict=True):
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


def _build_transcription_finalizing_segments(
        job: dict, title: str, *, lead: str | None = None) -> list:
    """Build the tagged phase line shared by short and chunked jobs."""
    video_id = str(job.get("video_id") or "").strip()
    marker = f"tx_done_{video_id}" if video_id else ""
    job_tag = str(job.get("job_tag") or "").strip()
    tags = lambda *extra: [
        value for value in (marker, job_tag, *extra) if value
    ]
    if lead is None:
        lead = "      " if job.get("from_download") else " "
    return [
        [f"{lead}— ", tags("whisper_bracket")],
        ["Finalizing transcript", tags("whisper_finalizing")],
        [f' "{(title or "")[:40].rstrip()}"', tags()],
        ["...\n", tags()],
    ]


_JOB_EXECUTOR = TranscriptionJobExecutor()


def _coerce_caption_outcome(value: Any) -> _CaptionOutcome:
    """Normalize legacy bool mocks/extensions to the detailed contract."""
    if isinstance(value, _CaptionOutcome):
        return value
    return (_CaptionOutcome.SUCCESS if value
            else _CaptionOutcome.UNAVAILABLE)


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
        # ``_model`` is only the default for jobs queued in the future.
        # Running provenance is tied to the model loaded in the child process.
        self._loaded_model = ""
        self._proc: subprocess.Popen | None = None
        self._proc_lock = threading.Lock()
        self._line_queue: queue.Queue | None = None
        self._starting = False
        self._reader_thread: threading.Thread | None = None
        self._stderr_drain_thread: threading.Thread | None = None
        self._stderr_buffer = None
        self._python311 = find_python311()
        # The worker is bundled beside the transcribe package under backend/.
        self._worker_script = Path(__file__).resolve().parent.parent / "whisper_worker.py"
        # Optional punctuation model — lazy-loaded, reused across jobs.
        # Routed through the process-singleton getter so the
        # Restore-Punctuation pass and the live transcribe worker
        # share one subprocess and VRAM allocation.
        try:
            from .punct_manager import get_shared_punct_manager
            self._punct = get_shared_punct_manager(stream)
        except Exception:
            self._punct = PunctuationManager(stream)
        self._punctuate_enabled = True

        # Queue of jobs. Each job = {path, title, cb, cancel_event}
        self._jobs: list[dict[str, Any]] = []
        self._jobs_lock = threading.Lock()
        # Every recovery-journal transition is serialized from runtime-state
        # snapshot through atomic replace. RLock lets a state transition hold
        # the boundary while calling the public persistence helper (which is
        # intentionally patchable in tests/extensions).
        self._journal_lock = threading.RLock()
        # Synchronous native-caption ingestion runs on the sync thread rather
        # than the GPU worker. Keep its pre-write recovery marker outside the
        # runnable queue while still including it in every durable snapshot.
        self._inline_caption_jobs: list[dict[str, Any]] = []
        self._active_inline_promotion: dict[str, Any] | None = None
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
        # Shutdown is intentionally distinct from user "Clear all". Clear is
        # a terminal drop and erases both recovery stores; shutdown must keep
        # the exact current task recoverable and merely stop taking new work.
        self._shutdown_requested = threading.Event()
        self._paused = threading.Event()
        # One-shot "Start" drain. When set, the worker processes the current
        # backlog even though the Auto checkbox (autorun_gpu) is OFF, then
        # self-clears the moment the queue empties and re-parks. This is the
        # "click Start to process what's stacked up, but leave Auto off"
        # path — Auto stays the user's manual gate; new arrivals keep queuing.
        self._manual_drain = threading.Event()
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
        self._stats_lock = threading.Lock()
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
        with self._stats_lock:
            s = self._batch_stats.get(channel_name) or {}
            return {"done": int(s.get("done", 0) or 0),
                    "err": int(s.get("err", 0) or 0)}

    def record_inline_transcription(self, channel_name: str) -> None:
        """Count a caption ingest completed synchronously by sync_channel."""
        ch_name = (channel_name or "").strip() or "—"
        with self._stats_lock:
            stats = self._batch_stats.setdefault(
                ch_name, {"start": time.time(), "done": 0, "err": 0})
            stats["done"] = int(stats.get("done", 0) or 0) + 1

    def consume_channel_batch_stats(self, channel_name: str) -> None:
        """Mark this channel's batch stats as already consumed by a
        sync-originated [Dwnld] row emission. Subsequent calls to
        _flush_batch_stats will skip it so the user doesn't see a
        duplicate [Trnscr] row for the same transcriptions.
        """
        try:
            with self._stats_lock:
                self._batch_stats.pop(channel_name, None)
        except Exception as e: _log.debug("swallowed: %s", e)

    def has_pending_transcription(self, channel_name: str) -> bool:
        """Return whether this channel still has queued/running model work."""
        wanted = (channel_name or "").strip().casefold()

        def _matches(job: dict[str, Any] | None) -> bool:
            if not job or (job.get("kind") or "transcribe") == "compress":
                return False
            return (job.get("channel") or "").strip().casefold() == wanted

        with self._jobs_lock:
            return _matches(self._current_job) or any(
                _matches(job) for job in self._jobs)

    def _finish_successful_job(
            self, job: dict[str, Any], result: Any,
            terminal_outcome: _WorkerOutcome = _WorkerOutcome.SUCCESS) -> bool:
        """Checkpoint completion, run its callback once, and commit follow-up.

        The transcription output is already durable when this method runs. Its
        job must remain as a completion-only recovery marker until a requested
        compression job is itself committed. Otherwise ``compress_enqueue``
        returning False lets the worker remove the transcription and silently
        loses that follow-up. The marker also prevents a retry from re-running
        transcript writers or the callback.
        """
        followup = dict(job.get("compress_after") or {})
        job["_output_complete"] = True
        job["_completed_outcome"] = terminal_outcome.value
        if result is not None:
            # Runtime-only: if the completion checkpoint itself transiently
            # fails, a same-process retry can still deliver the original
            # callback payload. It is deliberately excluded from the journal.
            job["_completion_result"] = result
        if followup and not job.get("_followup_enqueued"):
            job["_followup_pending"] = True

        with self._jobs_lock:
            tracked = (self._current_job is job
                       or any(existing is job for existing in self._jobs)
                       or any(existing is job
                              for existing in self._inline_caption_jobs))
        if tracked and not self._persist_pending():
            self._stream.emit_error(
                "Transcription output finished, but its completion recovery "
                "state could not be saved. Follow-up work was not started.")
            return False

        if not job.get("_callback_done"):
            # Mark before invoking external callback code so re-entrant or
            # same-process recovery paths cannot call it twice. Callbacks are
            # not serializable and therefore are intentionally absent after a
            # process restart.
            job["_callback_done"] = True
            callback = job.get("cb")
            callback_result = job.pop("_completion_result", result)
            if callback:
                try:
                    callback(callback_result)
                except Exception as e:
                    _log.debug("swallowed: %s", e)
        else:
            job.pop("_completion_result", None)

        if not followup or job.get("_followup_enqueued"):
            job.pop("_followup_pending", None)
            return True

        # Set the committed-side flags before calling compress_enqueue. Its
        # own atomic journal snapshot then contains BOTH this completion marker
        # and the new compression job. A crash immediately after replace sees
        # `_followup_enqueued` and cannot enqueue a duplicate on recovery.
        job["_followup_enqueued"] = True
        job.pop("_followup_pending", None)
        try:
            queued = self.compress_enqueue(
                job.get("path", ""),
                title=job.get("title", ""),
                channel=job.get("channel", ""),
                quality=followup.get("quality", "Average"),
                output_res=str(followup.get("output_res", "720")),
                # Inherit the parent transcribe's nesting so the
                # compress line lands under the same video row its
                # Metadata / Transcription siblings used.
                from_download=bool(job.get("from_download")),
            )
        except Exception as exc:
            _log.warning("compression follow-up enqueue failed: %s", exc)
            queued = False
        if queued:
            return True

        job.pop("_followup_enqueued", None)
        job["_followup_pending"] = True
        self._stream.emit_error(
            "Transcription finished, but video compression could not be saved "
            "in Processing. The completion task was kept for retry.")
        return False

    def _retry_completed_followup(
            self, job: dict[str, Any]) -> _WorkerOutcome:
        """Resume only completion handoff; never run transcript output again."""
        try:
            terminal = _WorkerOutcome(
                job.get("_completed_outcome") or _WorkerOutcome.SUCCESS)
        except ValueError:
            terminal = _WorkerOutcome.SUCCESS
        if not self._finish_successful_job(
                job, None, terminal_outcome=terminal):
            return _WorkerOutcome.CLEANUP_FAILED
        return terminal

    def _stage_inline_caption_recovery(
            self, marker: dict[str, Any]) -> tuple[bool, bool]:
        """Persist a non-runnable caption marker before sync-side writes.

        Returns ``(staged, duplicate)``. The marker stays in the ordinary
        pending journal, so a process interruption reloads it as a native
        caption recovery job even though it is not exposed to the live worker
        until the synchronous attempt finishes or fails.
        """
        wanted = self._job_path_key(marker.get("path", ""))
        with self._journal_lock:
            with self._jobs_lock:
                existing = [*self._inline_caption_jobs, *self._jobs]
                if self._current_job:
                    existing.append(self._current_job)
                if any(self._job_path_key(j.get("path", "")) == wanted
                       for j in existing):
                    return False, True
                self._inline_caption_jobs.append(marker)
            if self._persist_pending():
                return True, False
            with self._jobs_lock:
                self._inline_caption_jobs = [
                    job for job in self._inline_caption_jobs
                    if job is not marker
                ]
            return False, False

    def _clear_inline_caption_recovery(self, marker: dict[str, Any]) -> bool:
        """Clear a marker only after caption files and index all committed."""
        with self._journal_lock:
            with self._jobs_lock:
                if not any(job is marker for job in self._inline_caption_jobs):
                    return False
                self._inline_caption_jobs = [
                    job for job in self._inline_caption_jobs
                    if job is not marker
                ]
            if self._persist_pending():
                return True
            # The old on-disk snapshot still contains the marker. Restore the
            # runtime representation so a later promotion/retry matches it.
            with self._jobs_lock:
                self._inline_caption_jobs.append(marker)
            return False

    def _promote_inline_caption_recovery(
            self, marker: dict[str, Any]) -> bool:
        """Move an already-durable marker into the runnable GPU queue."""
        with self._journal_lock:
            with self._jobs_lock:
                if not any(job is marker
                           for job in self._inline_caption_jobs):
                    return False
            # Preserve the public enqueue call shape. While this journal
            # boundary is held, enqueue recognizes the active marker and moves
            # its already-durable record inline->runnable without a second
            # persistence gap.
            self._active_inline_promotion = marker
            try:
                queued = self.enqueue(
                    marker.get("path", ""),
                    marker.get("title", ""),
                    channel=marker.get("channel", ""),
                    video_id=marker.get("video_id", ""),
                    from_download=True,
                    compress_after=dict(marker.get("compress_after") or {}),
                )
            finally:
                self._active_inline_promotion = None
            if queued:
                return True
            return False

    def route_download_transcription(
            self, path: str, title: str, channel: str = "",
            video_id: str = "", compress_after: dict[str, str] | None = None,
            on_processing_queued: Callable | None = None) -> str:
        """Finish cheap native captions in sync; queue model work in Processing.

        Returns ``"inline"`` when already-punctuated local YouTube captions
        were ingested immediately, ``"processing"`` when punctuation/Whisper
        work was queued, ``"duplicate"`` when that path was already queued,
        or ``"failed"`` when recovery state could not be made durable.
        """
        followup = dict(compress_after or {})
        marker = {
            "task_id": make_task_id("gpu"),
            "kind": "transcribe",
            "path": str(path),
            "title": title or os.path.basename(path),
            "channel": channel,
            "video_id": (video_id or "").strip(),
            "combined_override": None,
            "retranscribe": False,
            "bulk_id": "",
            "bulk_total": 0,
            "bulk_index": 0,
            "from_download": True,
            "compress_after": followup,
            "requested_model": str(self._model or "").strip(),
            "actual_model": "",
            "cb": None,
            "cancel": threading.Event(),
            "_retry_required": True,
            "_write_intent": True,
            "_caption_recovery": True,
            # Inline work never reserved the ordinary Processing counter.
            # If the process dies before promotion, recovery must neither
            # increment nor later decrement that cosmetic counter.
            "_skip_pending_counter": True,
        }
        staged, duplicate = self._stage_inline_caption_recovery(marker)
        if duplicate:
            return "duplicate"
        if not staged:
            self._stream.emit_error(
                "Could not save native-caption recovery state; caption files "
                "were left untouched.")
            return "failed"

        try:
            caption_outcome = _coerce_caption_outcome(_try_auto_captions(
                path, title, channel, self._stream,
                punct_mgr=None,
                video_id_hint=video_id,
                from_download=True,
                allow_fetch=False,
                prepunctuated_only=True,
                update_pending=False,
            ))
        except Exception as exc:
            _log.warning("synchronous auto-caption attempt failed: %s", exc)
            caption_outcome = _CaptionOutcome.FAILED

        if caption_outcome is _CaptionOutcome.SUCCESS:
            # Keep the caption marker durable until any requested compression
            # follow-up has committed too. Clearing it first made a transient
            # compress_enqueue(False) silently lose that requested work.
            followup_committed = self._finish_successful_job(
                marker, {"auto_captions": True})
            if (followup_committed
                    and self._clear_inline_caption_recovery(marker)):
                self.record_inline_transcription(channel)
                return "inline"

        # The marker was committed before the first possible sidecar write.
        # Promote that same durable record rather than enqueueing a second job;
        # native-caption retry is idempotent by v2 video id and can safely
        # recover TXT-only, TXT+JSONL, or index-only interruption points.
        if on_processing_queued is not None:
            try:
                on_processing_queued()
            except Exception as e:
                _log.debug("processing-queued callback failed: %s", e)
        if not self._promote_inline_caption_recovery(marker):
            self._stream.emit_error(
                "Native-caption recovery is saved, but could not be added to "
                "the live Processing queue. It will be restored on restart.")
        return "processing"

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
        desired_model = (model or self._model or "").strip()
        wait_for_start = False
        with self._proc_lock:
            if self._proc is not None and self._proc.poll() is None:
                return self._loaded_model == desired_model
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
            m = desired_model
            self._stream.emit_text(
                f" Transcribing — Loading Whisper model ({m}) on GPU...",
                "transcribe_using")

            env = os.environ.copy()
            env["WHISPER_MODEL"] = m
            # Honor a one-shot CPU fallback on this instance instead
            # of mutating os.environ globally. The OOM
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
            try:
                from ..process_runner import PROCESS_REGISTRY
                with self._jobs_lock:
                    task_id = str(
                        (self._current_job or {}).get("task_id") or "")
                PROCESS_REGISTRY.register(
                    self._proc, owner="processing", task_id=task_id,
                    role="whisper")
            except Exception as exc:
                _log.debug("Whisper process registration failed: %s", exc)

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

            # The child, not the launch environment, is authoritative about
            # what it loaded.  Requiring the worker-owned value closes a
            # provenance gap where a stale/wrong child could be labelled with
            # the model the parent merely *asked* it to load.
            reported_model = str(info.get("model") or "").strip()
            if (reported_model not in _SUPPORTED_WHISPER_MODELS
                    or reported_model != m):
                self._stream.emit_error(
                    "Transcription tool reported the wrong Whisper model "
                    f"(requested {m!r}, worker reported "
                    f"{reported_model or '<missing>'!r}). Task kept for retry."
                )
                self._emit_whisper_stderr_tail()
                self._stop_subprocess()
                return False
            self._loaded_model = reported_model

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
                [f"Whisper model loaded ({reported_model}, {dev}).\n", "transcribe_using"],
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
                self._loaded_model = ""
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
            proc = self._proc
            try:
                if force:
                    proc.kill()
                else:
                    try:
                        proc.terminate()
                    except Exception:
                        proc.kill()
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
            # daemon cleanup at process exit. The two-second
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
            try:
                from ..process_runner import PROCESS_REGISTRY
                try:
                    proc.wait(timeout=0.25 if force else 1.0)
                except Exception:
                    PROCESS_REGISTRY.terminate_process(proc, timeout=2.0)
                if proc.poll() is not None:
                    PROCESS_REGISTRY.unregister(proc)
            except Exception as exc:
                _log.debug("Whisper process unregister failed: %s", exc)
            self._proc = None
            self._line_queue = None
            self._loaded_model = ""

    def _prepare_job_model(self, job: dict[str, Any]) -> bool:
        """Load the immutable model captured by one transcription job."""
        requested = str(
            job.get("requested_model") or self._model or "").strip()
        if requested not in _SUPPORTED_WHISPER_MODELS:
            self._stream.emit_error(
                f"Transcription task has unsupported model: {requested!r}")
            return False
        job["requested_model"] = requested
        if self._loaded_model and self._loaded_model != requested:
            self._stop_subprocess()
        if not self.start_subprocess(model=requested):
            return False
        return self._accept_worker_model_report(
            {"model": self._loaded_model},
            job,
            phase="ready handshake",
        )

    def _accept_worker_model_report(
        self,
        payload: dict[str, Any],
        job: dict[str, Any],
        *,
        phase: str,
    ) -> bool:
        """Validate and durably record child-reported model provenance."""
        reported = str(payload.get("model") or "").strip()
        requested = str(job.get("requested_model") or "").strip()
        loaded = str(self._loaded_model or "").strip()
        if (reported not in _SUPPORTED_WHISPER_MODELS
                or reported != requested
                or (loaded and reported != loaded)):
            self._stream.emit_error(
                f"Whisper {phase} model mismatch: requested "
                f"{requested or '<missing>'!r}, loaded "
                f"{loaded or '<missing>'!r}, worker reported "
                f"{reported or '<missing>'!r}. Task kept for retry."
            )
            return False

        previous = str(job.get("actual_model") or "").strip()
        if previous == reported:
            return True
        job["actual_model"] = reported
        if not self._persist_pending():
            if previous:
                job["actual_model"] = previous
            else:
                job.pop("actual_model", None)
            self._stream.emit_error(
                "Could not save the worker-reported Whisper model; task kept "
                "for retry before writing any transcript."
            )
            return False
        if self._queues is not None:
            try:
                self._queues.set_current_gpu(self._queue_payload_for_job(job))
            except Exception as exc:
                _log.warning(
                    "could not refresh current GPU model provenance: %s", exc
                )
        return True

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
                from_download: bool = False,
                compress_after: dict[str, str] | None = None,
                requested_model: str = "",
                on_state: Callable | None = None,
                _retry_required: bool = False,
                _retry_as_replace: bool = False,
                _write_intent: bool = False,
                _caption_recovery: bool = False,
                _skip_pending_counter: bool = False,
                _cleanup_only: bool = False,
                _no_speech_pending: bool = False,
                _stats_tallied: bool = False,
                _output_complete: bool = False,
                _callback_done: bool = False,
                _followup_pending: bool = False,
                _followup_enqueued: bool = False,
                _completed_outcome: str = "",
                _task_id: str = "",
                _restoring: bool = False) -> bool:
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
        if self._shutdown_requested.is_set():
            self._stream.emit_error(
                "Processing is shutting down; the new task was not accepted.")
            return False
        path = str(path)
        if not os.path.isfile(path):
            self._stream.emit_error(f"Transcribe: file not found: {path}")
            return False
        _job_title = title or os.path.basename(path)
        _path_key = os.path.normcase(os.path.normpath(os.path.abspath(path)))
        promotion = self._active_inline_promotion
        if (promotion is not None
                and self._job_path_key(promotion.get("path", "")) != _path_key):
            promotion = None
        if promotion is not None:
            # The identical durable marker is changing only runtime ownership.
            # Preserve every restart-semantic field from that marker.
            combined = promotion.get("combined_override")
            retranscribe = bool(promotion.get("retranscribe"))
            bulk_id = promotion.get("bulk_id", "") or ""
            bulk_total = int(promotion.get("bulk_total", 0) or 0)
            bulk_index = int(promotion.get("bulk_index", 0) or 0)
            _retry_required = bool(promotion.get("_retry_required"))
            _retry_as_replace = bool(promotion.get("_retry_as_replace"))
            _write_intent = bool(promotion.get("_write_intent"))
            _caption_recovery = bool(promotion.get("_caption_recovery"))
            _skip_pending_counter = bool(
                promotion.get("_skip_pending_counter"))
            _output_complete = bool(promotion.get("_output_complete"))
            _callback_done = bool(promotion.get("_callback_done"))
            _followup_pending = bool(promotion.get("_followup_pending"))
            _followup_enqueued = bool(promotion.get("_followup_enqueued"))
            _completed_outcome = str(
                promotion.get("_completed_outcome") or "")
            _task_id = str(promotion.get("task_id") or _task_id or "")
            requested_model = str(
                promotion.get("requested_model") or requested_model or "")
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

        job = {
            "task_id": str(_task_id or "").strip() or make_task_id("gpu"),
            "kind": "transcribe",
            "path": path,
            "title": _job_title,
            "channel": channel,
            "combined_override": combined,
            "cb": on_complete,
            # Runtime-only Watch feedback. Journal snapshots deliberately
            # omit callbacks; a restarted window reconstructs its own state.
            "state_cb": on_state,
            "cancel": threading.Event(),
            "retranscribe": bool(retranscribe),
            "video_id": (video_id or "").strip(),
            "bulk_id": bulk_id or "",
            "bulk_total": int(bulk_total or 0),
            "bulk_index": int(bulk_index or 0),
            "from_download": bool(from_download),
            "compress_after": dict(compress_after or {}),
            # Freeze the model at enqueue time. Later settings or one-off
            # choices apply only to jobs queued after that change.
            "requested_model": str(
                requested_model or self._model or "").strip(),
            "actual_model": str(
                (promotion or {}).get("actual_model") or "").strip(),
            # Recovery-only flags. A failed job must bypass the normal
            # "already transcribed" restore filter. Caption recovery keeps
            # the native sidecar retry path; other write intent retries use
            # the surgical replacement writers.
            "_retry_required": bool(_retry_required),
            "_retry_as_replace": bool(_retry_as_replace),
            "_write_intent": bool(_write_intent),
            "_caption_recovery": bool(_caption_recovery),
            "_skip_pending_counter": bool(_skip_pending_counter),
            "_cleanup_only": bool(_cleanup_only),
            "_no_speech_pending": bool(_no_speech_pending),
            "_stats_tallied": bool(_stats_tallied),
            "_output_complete": bool(_output_complete),
            "_callback_done": bool(_callback_done),
            "_followup_pending": bool(_followup_pending),
            "_followup_enqueued": bool(_followup_enqueued),
            "_completed_outcome": str(_completed_outcome or ""),
        }
        # Reserve one durable visible identity before the job may start.  A
        # stale QueueState row for this logical path is adopted instead of
        # creating a second hidden ID.  If the journal's second commit fails,
        # the reservation is rolled back before returning.
        reservation = None
        with self._journal_lock:
            with self._jobs_lock:
                if (any(_same_transcribe_path(j) for j in self._jobs)
                        or any(_same_transcribe_path(j)
                               for j in self._inline_caption_jobs
                               if j is not promotion)
                        or _same_transcribe_path(self._current_job)):
                    return False
                reserved_ids = {
                    str(existing.get("task_id") or "").strip()
                    for existing in [
                        *self._jobs, *self._inline_caption_jobs,
                        self._current_job,
                    ]
                    if existing is not None and existing is not promotion
                    and str(existing.get("task_id") or "").strip()
                }
            if self._queues is not None:
                try:
                    reservation = self._queues.gpu_reserve_task(
                        self._queue_payload_for_job(job),
                        reserved_task_ids=reserved_ids,
                        required_task_id=(job["task_id"] if promotion is not None
                                          else ""),
                    )
                except Exception as exc:
                    _log.warning("Processing queue reservation failed: %s", exc)
                    reservation = None
                if not isinstance(reservation, dict):
                    self._stream.emit_error(
                        "Could not save the visible Processing queue; task was "
                        "not started.")
                    return False
                job["task_id"] = str(
                    reservation.get("task_id") or "").strip()
                if not job["task_id"]:
                    return False
            with self._jobs_lock:
                self._jobs.append(job)
                if promotion is not None:
                    self._inline_caption_jobs = [
                        existing for existing in self._inline_caption_jobs
                        if existing is not promotion
                    ]
            if not self._persist_pending():
                with self._jobs_lock:
                    if job in self._jobs:
                        self._jobs.remove(job)
                    if (promotion is not None
                            and not any(existing is promotion
                                        for existing in self._inline_caption_jobs)):
                        self._inline_caption_jobs.append(promotion)
                if reservation is not None:
                    try:
                        self._queues.gpu_rollback_reservation(reservation)
                    except Exception as exc:
                        _log.warning(
                            "Processing reservation rollback failed: %s", exc)
                self._stream.emit_error(
                    "Could not save the transcription queue; task was not "
                    "started. Check that the app data folder is writable.")
                return False
        # Bump `transcription_pending` for the channel so the Subs-tab
        # auto-indicator stays in sync with OLD's behavior (YTArchiver.py:
        # 14629 and friends set this counter during sync → transcribe flow).
        if (not _restoring and not _cleanup_only
                and not _skip_pending_counter):
            _bump_transcription_pending(channel, 1)
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
                if (not _restoring and self._auto_enabled()
                        and self._paused.is_set()
                        and self._queues is not None
                        and getattr(self._queues, "gpu_paused", False)
                        and getattr(self._queues, "gpu_pause_restored", False)):
                    # Only auto-release a pause RESTORED from a prior session
                    # (the "don't auto-fire restored items until fresh work"
                    # convenience). A pause the user set THIS session is left
                    # intact so an incoming auto-sync/download can't silently
                    # resume a deliberately-paused Processing queue.
                    self._paused.clear()
                    self._queues.set_gpu_paused(False)
        except Exception as e:
            _log.debug("swallowed: %s", e)
        self._ensure_worker()
        return True

    def compress_enqueue(self, path: str, title: str = "",
                         channel: str = "", quality: str = "Average",
                         output_res: str = "720",
                         on_complete: Callable | None = None,
                         from_download: bool = False) -> bool:
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
        if self._shutdown_requested.is_set():
            self._stream.emit_error(
                "Processing is shutting down; the new task was not accepted.")
            return False
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
        job = {
            "task_id": make_task_id("gpu"),
            "kind": "compress",
            "path": path,
            "title": _job_title,
            "channel": channel,
            "quality": quality,
            "output_res": str(output_res),
            "from_download": bool(from_download),
            "cb": on_complete,
            "cancel": threading.Event(),
        }
        reservation = None
        with self._journal_lock:
            with self._jobs_lock:
                reserved_ids = {
                    str(existing.get("task_id") or "").strip()
                    for existing in [
                        *self._jobs, *self._inline_caption_jobs,
                        self._current_job,
                    ]
                    if existing is not None
                    and str(existing.get("task_id") or "").strip()
                }
                duplicate = any(
                    self._job_identity_key(existing)
                    == self._job_identity_key(job)
                    for existing in [*self._jobs, self._current_job]
                    if existing is not None
                )
            if duplicate:
                return False
            if self._queues is not None:
                try:
                    reservation = self._queues.gpu_reserve_task(
                        self._queue_payload_for_job(job),
                        reserved_task_ids=reserved_ids,
                    )
                except Exception as exc:
                    _log.warning("Compression queue reservation failed: %s", exc)
                    reservation = None
                if not isinstance(reservation, dict):
                    self._stream.emit_error(
                        "Could not save the visible compression queue; task "
                        "was not started.")
                    return False
                job["task_id"] = str(
                    reservation.get("task_id") or "").strip()
            with self._jobs_lock:
                self._jobs.append(job)
            if not self._persist_pending():
                with self._jobs_lock:
                    if job in self._jobs:
                        self._jobs.remove(job)
                if reservation is not None:
                    try:
                        self._queues.gpu_rollback_reservation(reservation)
                    except Exception as exc:
                        _log.warning(
                            "Compression reservation rollback failed: %s", exc)
                self._stream.emit_error(
                    "Could not save the compression queue; task was not "
                    "started. Check that the app data folder is writable.")
                return False
        self._ensure_worker()
        return True

    # ── Pending journal (survives restart) ──

    @staticmethod
    def _snapshot_pending_job(j: dict[str, Any]) -> dict[str, Any]:
        """Return the restart-safe, JSON-serializable portion of one job."""
        return {
            "task_id": j.get("task_id", ""),
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
            "quality": j.get("quality", "Average"),
            "output_res": str(j.get("output_res", "720")),
            "compress_after": dict(j.get("compress_after") or {}),
            "requested_model": str(j.get("requested_model") or ""),
            "actual_model": str(j.get("actual_model") or ""),
            "retry_required": bool(j.get("_retry_required")),
            "retry_as_replace": bool(j.get("_retry_as_replace")),
            "write_intent": bool(j.get("_write_intent")),
            "caption_recovery": bool(j.get("_caption_recovery")),
            "skip_pending_counter": bool(j.get("_skip_pending_counter")),
            "cleanup_only": bool(j.get("_cleanup_only")),
            "no_speech_pending": bool(j.get("_no_speech_pending")),
            "stats_tallied": bool(j.get("_stats_tallied")),
            "output_complete": bool(j.get("_output_complete")),
            "callback_done": bool(j.get("_callback_done")),
            "followup_pending": bool(j.get("_followup_pending")),
            "followup_enqueued": bool(j.get("_followup_enqueued")),
            "completed_outcome": str(j.get("_completed_outcome") or ""),
            "defer_requested": bool(j.get("_defer_requested")),
        }

    def _pending_snapshot(self, *, include_current: bool = True) -> list[dict]:
        """Snapshot every durable job while the journal boundary is held."""
        snap = self._snapshot_pending_job
        with self._jobs_lock:
            snapshot = [snap(j) for j in self._inline_caption_jobs]
            snapshot.extend(snap(j) for j in self._jobs)
            if include_current and self._current_job:
                snapshot.insert(0, snap(self._current_job))
        return snapshot

    @staticmethod
    def _write_pending_snapshot(snapshot: list[dict]) -> bool:
        """Fsync and atomically replace the journal using a unique temp."""
        tmp = ""
        fd = -1
        try:
            import tempfile as _tempfile

            p = _pending_journal_path()
            p.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = _tempfile.mkstemp(
                prefix=f".{p.name}.", suffix=".tmp", dir=str(p.parent))
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                fd = -1
                json.dump(snapshot, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, p)
            tmp = ""
            return True
        except Exception as e:
            _log.warning("could not persist transcription recovery journal: %s", e)
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
            if tmp:
                try:
                    os.remove(tmp)
                except OSError:
                    pass
            return False

    def drop_running_from_journal(self) -> bool:
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
        with self._journal_lock:
            return self._write_pending_snapshot(
                self._pending_snapshot(include_current=False))

    def clear_pending_journal(self) -> bool:
        """Persist an empty pending-transcribe journal."""
        with self._journal_lock:
            return self._write_pending_snapshot([])

    def _persist_pending(self) -> bool:
        """Durably write pending jobs; report whether the journal committed."""
        with self._journal_lock:
            return self._write_pending_snapshot(self._pending_snapshot())

    def load_pending(self) -> int:
        """Load any jobs left behind from a previous session. Returns count.

        "Already transcribed" means the title already has an entry in the
        aggregated {channel} Transcript.txt (matches YTArchiver.py's
        _scan_existing_transcripts). Falls back to checking a legacy
        per-video {base}.jsonl sidecar from older builds.
        """
        try:
            import json as _json

            # Cache existing-title sets per channel so we don't re-walk the
            # folder N times.
            _title_cache: dict[str, set] = {}
            _no_speech_title_cache: dict[str, set] = {}
            def _already_transcribed(video_path: str, title: str,
                                     channel: str, video_id: str = "") -> bool:
                try:
                    from .. import index as _idx
                    vid = (video_id or _extract_video_id(video_path) or "").strip()
                    tx_status = _idx.video_tx_status(
                        video_id=vid or None,
                        title=(title or None),
                        channel=(channel or None),
                    ).lower()
                    if tx_status == "no_speech":
                        return True
                    conn = _idx._reader_open() or _idx._open()
                    if conn is not None:
                        lock = (_idx._reader_lock
                                if _idx._reader_open() is not None
                                else _idx._db_lock)
                        fp = os.path.normpath(video_path or "")
                        with lock:
                            row = conn.execute(
                                "SELECT tx_status FROM videos "
                                "WHERE filepath=? COLLATE NOCASE LIMIT 1",
                                (fp,),
                            ).fetchone()
                        if row and (row[0] or "").lower() == "no_speech":
                            return True
                    ch_key = (channel or "").strip().lower()
                    if ch_key:
                        titles = _no_speech_title_cache.get(ch_key)
                        if titles is None:
                            titles = set()
                            conn = _idx._reader_open() or _idx._open()
                            if conn is not None:
                                lock = (_idx._reader_lock
                                        if _idx._reader_open() is not None
                                        else _idx._db_lock)
                                with lock:
                                    rows = conn.execute(
                                        "SELECT title FROM videos "
                                        "WHERE channel=? COLLATE NOCASE "
                                        "AND tx_status='no_speech'",
                                        (channel,),
                                    ).fetchall()
                                titles = {
                                    _norm_title(str(r[0] or ""))
                                    for r in rows if r and r[0]
                                }
                            _no_speech_title_cache[ch_key] = titles
                        if _norm_title(title or "") in titles:
                            return True
                except Exception as e:
                    _log.debug("no_speech restore skip lookup failed: %s", e)
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

            # Treat startup recovery as one journal transaction. Re-enqueueing
            # each saved job through the public helpers used to replace an
            # N-item journal with the first recovered item; a crash midway
            # through the loop then permanently lost the remaining N-1 jobs.
            # Holding this boundary from read through the single replacement
            # also prevents a concurrent fresh enqueue from being overwritten
            # by a stale startup snapshot.
            with self._journal_lock:
                p = _pending_journal_path()
                if not p.exists():
                    return 0
                with p.open("r", encoding="utf-8") as f:
                    jobs = _json.load(f)
                if not isinstance(jobs, list):
                    return 0

                queue_snapshot: list[dict[str, Any]] = []
                queue_task_ids: dict[tuple[str, str], str] = {}
                queue_id_owners: dict[str, tuple[str, str]] = {}
                queue_order: dict[tuple[str, str], int] = {}
                queue_snapshot_loaded = True
                if self._queues is not None:
                    try:
                        queue_snapshot = self._queues.gpu_snapshot()
                        for queue_index, queued in enumerate(queue_snapshot):
                            key = self._job_identity_key(queued)
                            task_id = str(
                                queued.get("task_id") or "").strip()
                            if key is not None:
                                queue_order.setdefault(key, queue_index)
                            if key is not None and task_id:
                                queue_task_ids.setdefault(key, task_id)
                                queue_id_owners.setdefault(task_id, key)
                    except Exception as exc:
                        _log.debug("GPU identity migration snapshot failed: %s",
                                   exc)
                        queue_snapshot_loaded = False
                if not queue_snapshot_loaded:
                    self._stream.emit_error(
                        "Could not read the durable Processing queue; startup "
                        "recovery remains paused for the next launch.")
                    return 0

                stale_queue_task_ids: list[str] = []
                candidates: list[dict[str, Any]] = []
                with self._jobs_lock:
                    existing = [*self._inline_caption_jobs, *self._jobs]
                    if self._current_job:
                        existing.append(self._current_job)
                    known_jobs = {
                        key for job in existing
                        if (key := self._job_identity_key(job)) is not None
                    }
                    assigned_ids = {
                        str(job.get("task_id") or "").strip()
                        for job in existing
                        if str(job.get("task_id") or "").strip()
                    }

                for saved in jobs:
                    if not isinstance(saved, dict):
                        continue
                    path = str(saved.get("path") or "")
                    if not path or not os.path.isfile(path):
                        continue
                    kind = str(saved.get("kind") or "transcribe").lower()
                    job_key = (kind, self._job_path_key(path))
                    if not job_key[1] or job_key in known_jobs:
                        continue
                    queue_task_id = queue_task_ids.get(job_key, "")
                    saved_task_id = str(
                        saved.get("task_id") or "").strip()
                    task_id = ""
                    if (queue_task_id
                            and queue_task_id not in assigned_ids
                            and queue_id_owners.get(queue_task_id) == job_key):
                        task_id = queue_task_id
                    elif (saved_task_id
                          and saved_task_id not in assigned_ids
                          and queue_id_owners.get(saved_task_id, job_key)
                          == job_key):
                        task_id = saved_task_id
                    while not task_id or task_id in assigned_ids:
                        task_id = make_task_id("gpu")
                        # A journal-only recovery row must not steal an ID
                        # from a QueueState row that has yet to be reconciled.
                        if task_id in queue_id_owners:
                            task_id = ""

                    # A RE-transcribe or recovery job intentionally targets an
                    # already-transcribed video. Only an ordinary stale
                    # transcribe entry may be dropped during startup cleanup.
                    if (kind != "compress" and not saved.get("retranscribe")
                            and not saved.get("retry_required")
                            and not saved.get("write_intent")
                            and not saved.get("cleanup_only")
                            and not saved.get("no_speech_pending")
                            and not saved.get("output_complete")
                            and _already_transcribed(
                                path, saved.get("title", ""),
                                saved.get("channel", ""),
                                saved.get("video_id", ""))):
                        if queue_task_id:
                            stale_queue_task_ids.append(
                                queue_task_id)
                        continue

                    title = saved.get("title") or os.path.basename(path)
                    if kind == "compress":
                        # Preserve compress_enqueue's archive-containment
                        # fail-closed behavior without exposing a partially
                        # restored job to its public enqueue/start side effects.
                        try:
                            from ..utils import is_within_managed_roots
                            if not is_within_managed_roots(path):
                                continue
                        except Exception as exc:
                            _log.warning(
                                "compress recovery containment check failed "
                                "for %s: %s", path, exc)
                            continue
                        runtime_job: dict[str, Any] = {
                            "task_id": task_id,
                            "kind": "compress",
                            "path": os.path.normpath(path),
                            "title": title,
                            "channel": saved.get("channel", ""),
                            "quality": saved.get("quality", "Average"),
                            "output_res": str(saved.get("output_res", "720")),
                            "cb": None,
                            "cancel": threading.Event(),
                        }
                    else:
                        caption_recovery = bool(
                            saved.get("caption_recovery"))
                        write_intent = bool(saved.get("write_intent"))
                        runtime_job = {
                            "task_id": task_id,
                            "kind": "transcribe",
                            "path": path,
                            "title": title,
                            "channel": saved.get("channel", ""),
                            "combined_override": saved.get(
                                "combined_override"),
                            "cb": None,
                            "cancel": threading.Event(),
                            "retranscribe": bool(saved.get("retranscribe")),
                            "video_id": (
                                saved.get("video_id") or "").strip(),
                            "bulk_id": saved.get("bulk_id", "") or "",
                            "bulk_total": int(
                                saved.get("bulk_total", 0) or 0),
                            "bulk_index": int(
                                saved.get("bulk_index", 0) or 0),
                            "from_download": bool(
                                saved.get("from_download")),
                            "compress_after": dict(
                                saved.get("compress_after") or {}),
                            "requested_model": str(
                                saved.get("requested_model")
                                or self._model or ""),
                            "actual_model": str(
                                saved.get("actual_model") or ""),
                            "_retry_required": bool(
                                saved.get("retry_required")
                                or write_intent
                                or saved.get("cleanup_only")
                                or saved.get("no_speech_pending")),
                            "_retry_as_replace": bool(
                                saved.get("retry_as_replace")
                                or (write_intent and not caption_recovery)),
                            "_write_intent": write_intent,
                            "_caption_recovery": caption_recovery,
                            "_skip_pending_counter": bool(
                                saved.get("skip_pending_counter")),
                            "_cleanup_only": bool(
                                saved.get("cleanup_only")),
                            "_no_speech_pending": bool(
                                saved.get("no_speech_pending")),
                            "_stats_tallied": bool(
                                saved.get("stats_tallied")),
                            "_output_complete": bool(
                                saved.get("output_complete")),
                            "_callback_done": bool(
                                saved.get("callback_done")),
                            "_followup_pending": bool(
                                saved.get("followup_pending")),
                            "_followup_enqueued": bool(
                                saved.get("followup_enqueued")),
                            "_completed_outcome": str(
                                saved.get("completed_outcome") or ""),
                        }

                    candidates.append(runtime_job)
                    known_jobs.add(job_key)
                    assigned_ids.add(task_id)

                # QueueState is authoritative for visible order.  A deferred
                # current job is durably moved to the tail before cancellation,
                # while the pre-crash journal may still list it first.  The
                # stable sort preserves journal order for journal-only rows.
                candidates.sort(key=lambda job: queue_order.get(
                    self._job_identity_key(job), len(queue_order)))

                # Reconcile QueueState first, then atomically replace the
                # normalized journal, and only then expose jobs to the worker.
                # If either durable store fails, restore the exact QueueState
                # snapshot and leave the old journal/runtime untouched.
                queue_reconciled = True
                if self._queues is not None:
                    try:
                        if stale_queue_task_ids:
                            removed = self._queues.gpu_remove_tasks(
                                stale_queue_task_ids,
                                durable=True,
                                require_all=True,
                            )
                            queue_reconciled = (
                                set(removed) == set(stale_queue_task_ids))
                        reserved_ids = {
                            str(job.get("task_id") or "").strip()
                            for job in existing
                            if str(job.get("task_id") or "").strip()
                        }
                        for job in candidates:
                            if not queue_reconciled:
                                break
                            wanted_id = str(
                                job.get("task_id") or "").strip()
                            token = self._queues.gpu_reserve_task(
                                self._queue_payload_for_job(job),
                                reserved_task_ids=reserved_ids,
                                required_task_id=wanted_id,
                            )
                            queue_reconciled = (
                                isinstance(token, dict)
                                and str(token.get("task_id") or "").strip()
                                == wanted_id)
                            if queue_reconciled:
                                reserved_ids.add(wanted_id)
                    except Exception as exc:
                        _log.warning(
                            "Processing queue reconciliation failed: %s", exc)
                        queue_reconciled = False
                    if not queue_reconciled:
                        try:
                            self._queues.restore_pending_snapshot(
                                "gpu", queue_snapshot)
                        except Exception as exc:
                            _log.warning(
                                "Processing queue compensation failed: %s",
                                exc)
                        self._stream.emit_error(
                            "Could not reconcile restored Processing tasks; "
                            "startup recovery remains paused for the next "
                            "launch.")
                        return 0

                snap = self._snapshot_pending_job
                with self._jobs_lock:
                    normalized_journal = [
                        snap(job) for job in self._inline_caption_jobs
                    ]
                    normalized_journal.extend(
                        snap(job) for job in [*self._jobs, *candidates])
                    if self._current_job:
                        normalized_journal.insert(
                            0, snap(self._current_job))
                if not self._write_pending_snapshot(normalized_journal):
                    if self._queues is not None:
                        try:
                            self._queues.restore_pending_snapshot(
                                "gpu", queue_snapshot)
                        except Exception as exc:
                            _log.warning(
                                "Processing queue compensation failed: %s",
                                exc)
                    self._stream.emit_error(
                        "Could not save restored Processing tasks; startup "
                        "recovery was left unchanged for the next launch.")
                    return 0

                with self._jobs_lock:
                    self._jobs.extend(candidates)

            # Both durable stores now describe the same exact jobs and IDs.
            # Restored jobs remain paused until an explicit Start or enqueue.
            return len(candidates)
        except Exception:
            return 0

    def queue_size(self) -> int:
        with self._jobs_lock:
            n = len(self._jobs)
        if self._current_job:
            n += 1
        return n

    def remove_pending_task_ids_coordinated(
            self, task_ids: set[str], mirror_remove: Callable[[], bool],
            mirror_restore: Callable[[], bool]) -> bool:
        """Remove exact pending jobs only if both durable stores commit.

        QueueState commits while the candidate jobs are absent in memory, then
        the transcription journal commits the same state.  If the journal
        replacement fails, QueueState is restored and the in-memory jobs were
        never allowed to escape the journal boundary.
        """
        wanted = {
            str(task_id or "").strip() for task_id in task_ids
            if str(task_id or "").strip()
        }
        if not wanted or not callable(mirror_remove):
            return False
        removed_jobs: list[dict[str, Any]] = []
        with self._journal_lock:
            with self._jobs_lock:
                original = list(self._jobs)
                keep = []
                for job in self._jobs:
                    if str(job.get("task_id") or "").strip() in wanted:
                        removed_jobs.append(job)
                    else:
                        keep.append(job)
                self._jobs[:] = keep
            try:
                mirror_saved = bool(mirror_remove())
            except Exception as exc:
                _log.warning("Processing queue removal mirror failed: %s", exc)
                mirror_saved = False
            if not mirror_saved:
                with self._jobs_lock:
                    self._jobs[:] = original
                return False
            if removed_jobs and not self._persist_pending():
                with self._jobs_lock:
                    self._jobs[:] = original
                try:
                    restored = bool(mirror_restore())
                except Exception as exc:
                    _log.warning(
                        "Processing queue removal rollback failed: %s", exc)
                    restored = False
                if not restored:
                    self._stream.emit_error(
                        "Could not restore the visible Processing queue after "
                        "a journal failure. Work remains in recovery and queue "
                        "actions are disabled until saving succeeds.")
                self._stream.emit_error(
                    "Could not save task removal; Processing work was kept "
                    "for recovery.")
                return False
        for job in removed_jobs:
            try:
                if (not job.get("retranscribe")
                        and not job.get("_pending_decremented")
                        and not job.get("_skip_pending_counter")):
                    _bump_transcription_pending(
                        job.get("channel") or "", -1)
                    job["_pending_decremented"] = True
            except Exception as exc:
                _log.debug("pending counter cleanup failed: %s", exc)
        self._release_compress_slots(removed_jobs)
        self._notify_jobs_cancelled(removed_jobs)
        return True

    def reorder_pending_task_coordinated(
            self, task_id: str, new_index: int,
            mirror_reorder: Callable[[], bool],
            mirror_restore: Callable[[], bool]) -> bool:
        """Reorder one exact job only if QueueState and journal agree."""
        ident = str(task_id or "").strip()
        if not ident or not callable(mirror_reorder):
            return False
        try:
            target_index = int(new_index)
        except (TypeError, ValueError):
            return False
        with self._journal_lock:
            with self._jobs_lock:
                original = list(self._jobs)
                idx = next(
                    (i for i, job in enumerate(self._jobs)
                     if str(job.get("task_id") or "").strip() == ident),
                    -1,
                )
                if idx >= 0:
                    if target_index < 0 or target_index >= len(self._jobs):
                        return False
                    job = self._jobs.pop(idx)
                    self._jobs.insert(target_index, job)
            try:
                mirror_saved = bool(mirror_reorder())
            except Exception as exc:
                _log.warning("Processing queue reorder mirror failed: %s", exc)
                mirror_saved = False
            if not mirror_saved:
                with self._jobs_lock:
                    self._jobs[:] = original
                return False
            if idx >= 0 and not self._persist_pending():
                with self._jobs_lock:
                    self._jobs[:] = original
                try:
                    restored = bool(mirror_restore())
                except Exception as exc:
                    _log.warning(
                        "Processing queue reorder rollback failed: %s", exc)
                    restored = False
                if not restored:
                    self._stream.emit_error(
                        "Could not restore the visible Processing order after "
                        "a journal failure. Queue actions are disabled until "
                        "saving succeeds.")
                self._stream.emit_error(
                    "Could not save Processing order; the previous order was "
                    "kept.")
                return False
        return True

    @staticmethod
    def _pending_path_within(path: str, root: str) -> bool:
        """Return whether a queued media path belongs to one channel root."""
        try:
            candidate = os.path.normcase(os.path.abspath(str(path or "")))
            boundary = os.path.normcase(os.path.abspath(str(root or "")))
            return bool(candidate and boundary
                        and os.path.commonpath([candidate, boundary]) == boundary
                        and candidate != boundary)
        except (OSError, ValueError):
            return False

    def reconcile_pending_channel_path(
            self, old_root: str, new_root: str = "", *,
            old_channel: str = "", new_channel: str = "") -> dict[str, Any]:
        """Remap or remove pending work after a channel-folder mutation.

        A rename supplies ``new_root`` and keeps the same task IDs and order.
        Moving a channel to Trash leaves ``new_root`` empty, which removes its
        queued Processing work. QueueState and the transcription recovery
        journal are committed together and both roll back if either save fails.
        """
        raw_old_root = str(old_root or "").strip()
        if not raw_old_root:
            return {"ok": False, "error": "Old channel folder is required."}
        old_root = os.path.abspath(raw_old_root)
        new_root = os.path.abspath(str(new_root or "")) if new_root else ""

        def _matches(job: dict[str, Any] | None) -> bool:
            return bool(job and self._pending_path_within(
                str(job.get("path") or ""), old_root))

        def _new_path(path: str) -> str:
            relative = os.path.relpath(os.path.abspath(path), old_root)
            return os.path.normpath(os.path.join(new_root, relative))

        with self._journal_lock:
            with self._jobs_lock:
                if _matches(self._current_job) or any(
                        _matches(job) for job in self._inline_caption_jobs):
                    return {
                        "ok": False,
                        "busy": True,
                        "error": (
                            "Processing is currently using this channel. "
                            "Pause or cancel that task and try again."
                        ),
                    }
                original_jobs = list(self._jobs)
                original_fields = [
                    (job, job.get("path", ""), job.get("channel", ""))
                    for job in self._jobs if _matches(job)
                ]
                if new_root:
                    for job, path, _channel in original_fields:
                        job["path"] = _new_path(str(path))
                        if new_channel:
                            job["channel"] = new_channel
                else:
                    self._jobs[:] = [job for job in self._jobs
                                     if not _matches(job)]

            queue_before: list[dict[str, Any]] = []
            queue_after: list[dict[str, Any]] = []
            queue_changed = 0
            if self._queues is not None:
                try:
                    queue_before = self._queues.gpu_snapshot()
                    for item in queue_before:
                        if not _matches(item):
                            queue_after.append(item)
                            continue
                        queue_changed += 1
                        if new_root:
                            changed = dict(item)
                            changed["path"] = _new_path(
                                str(item.get("path") or ""))
                            if new_channel:
                                changed["channel"] = new_channel
                            queue_after.append(changed)
                    if queue_changed and not self._queues.restore_pending_snapshot(
                            "gpu", queue_after):
                        raise OSError("Could not save the Processing queue.")
                except Exception as exc:
                    with self._jobs_lock:
                        self._jobs[:] = original_jobs
                        for job, path, channel in original_fields:
                            job["path"] = path
                            job["channel"] = channel
                    return {"ok": False, "error": str(exc)}

            runtime_changed = len(original_fields)
            if (runtime_changed or queue_changed) and not \
                    self._write_pending_snapshot(self._pending_snapshot()):
                with self._jobs_lock:
                    self._jobs[:] = original_jobs
                    for job, path, channel in original_fields:
                        job["path"] = path
                        job["channel"] = channel
                queue_rollback_ok = True
                if self._queues is not None and queue_changed:
                    try:
                        queue_rollback_ok = bool(
                            self._queues.restore_pending_snapshot(
                                "gpu", queue_before))
                    except Exception as exc:
                        _log.warning(
                            "Processing queue path rollback failed: %s", exc)
                        queue_rollback_ok = False
                return {
                    "ok": False,
                    "recovery_required": not queue_rollback_ok,
                    "error": (
                        "Could not save the Processing recovery journal."
                        + (" The Processing queue also could not be restored; "
                           "restart YTArchiver before changing this channel "
                           "folder."
                           if not queue_rollback_ok else "")
                    ),
                }

        return {
            "ok": True,
            "changed": max(runtime_changed, queue_changed),
            "removed": 0 if new_root else max(runtime_changed, queue_changed),
        }

    @contextmanager
    def pending_path_mutation_boundary(self):
        """Prevent a queued task from starting during a folder mutation.

        Path reconciliation and the filesystem/config transaction must be one
        boundary. Otherwise the worker can pop a newly remapped task after the
        journal save but before the folder itself has moved.
        """
        with self._journal_lock:
            yield

    @contextmanager
    def pending_channel_path_mutation(
            self, old_root: str, new_root: str = "", *,
            old_channel: str = "", new_channel: str = ""):
        """Hold Processing's journal boundary around a folder transaction.

        The yielded dict contains ``result`` and a ``commit`` flag. Callers set
        ``commit`` only after their config/filesystem transaction succeeds;
        otherwise the exact pre-mutation Processing state is restored.
        """
        with self._journal_lock:
            with self._jobs_lock:
                original_jobs = list(self._jobs)
                original_fields = [
                    (job, job.get("path", ""), job.get("channel", ""))
                    for job in self._jobs
                ]
            queue_before = (self._queues.gpu_snapshot()
                            if self._queues is not None else [])
            result = self.reconcile_pending_channel_path(
                old_root, new_root,
                old_channel=old_channel, new_channel=new_channel)
            control = {"result": result, "commit": False}
            try:
                yield control
            finally:
                if result.get("ok") and not control.get("commit"):
                    with self._jobs_lock:
                        self._jobs[:] = original_jobs
                        for job, path, channel in original_fields:
                            job["path"] = path
                            job["channel"] = channel
                    queue_ok = True
                    if self._queues is not None:
                        queue_ok = self._queues.restore_pending_snapshot(
                            "gpu", queue_before)
                    journal_ok = self._write_pending_snapshot(
                        self._pending_snapshot())
                    if not queue_ok or not journal_ok:
                        raise RuntimeError(
                            "Could not restore Processing tasks after the "
                            "channel-folder operation was cancelled."
                        )

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
        removed_jobs: list[dict[str, Any]] = []
        with self._journal_lock:
            with self._jobs_lock:
                original = list(self._jobs)
                keep = []
                for job in self._jobs:
                    try:
                        match = bool(predicate(job))
                    except Exception:
                        match = False
                    if match:
                        removed_jobs.append(job)
                    else:
                        keep.append(job)
                self._jobs[:] = keep
            if removed_jobs and not self._persist_pending():
                with self._jobs_lock:
                    self._jobs[:] = original
                self._stream.emit_error(
                    "Could not save task removal; Processing work was kept "
                    "for recovery.")
                return 0
        for job in removed_jobs:
            try:
                if (not job.get("retranscribe")
                        and not job.get("_pending_decremented")
                        and not job.get("_skip_pending_counter")):
                    _bump_transcription_pending(
                        job.get("channel") or "", -1)
                    job["_pending_decremented"] = True
            except Exception as e:
                _log.debug("pending counter cleanup failed: %s", e)
        self._release_compress_slots(removed_jobs)
        self._notify_jobs_cancelled(removed_jobs)
        return len(removed_jobs)

    def reorder_pending_job(self, identifier: str, new_index: int) -> bool:
        """Mirror a GPU popover reorder into the manager's pending jobs."""
        ident = str(identifier or "").strip()
        if not ident:
            return False
        try:
            target_index = int(new_index)
        except (TypeError, ValueError):
            return False
        with self._journal_lock:
            with self._jobs_lock:
                if target_index < 0 or target_index >= len(self._jobs):
                    return False
                original = list(self._jobs)
                idx = next(
                    (
                        i for i, job in enumerate(self._jobs)
                        if str(job.get("task_id") or "").strip() == ident
                    ),
                    -1,
                )
                if idx < 0:
                    # Internal migration compatibility for a runtime list
                    # created from a pre-ID journal. The current frontend
                    # never sends paths; it sends the opaque ID above.
                    idx = next(
                        (i for i, job in enumerate(self._jobs)
                         if not str(job.get("task_id") or "").strip()
                         and str(job.get("path") or "").strip() == ident),
                        -1,
                    )
                if idx < 0:
                    return False
                job = self._jobs.pop(idx)
                self._jobs.insert(target_index, job)
            if self._persist_pending():
                return True
            with self._jobs_lock:
                self._jobs[:] = original
            self._stream.emit_error(
                "Could not save Processing order; the previous order was kept.")
            return False

    def cancel_all(self, *, clear_visible: bool = True) -> bool:
        """Durably clear all Processing work before signalling cancellation.

        QueueState and the recovery journal are separate crash-recovery
        stores.  Neither the cancel event nor the subprocess kill is exposed
        until both stores have committed.  On a peer-store failure the first
        commit is compensated and the live worker remains untouched.
        """
        discarded_jobs: list[dict[str, Any]] = []
        with self._journal_lock:
            # Hold the journal boundary through the QueueState transaction so
            # the worker cannot pop/finalize a job between the two stores.
            queue_snapshot: list[dict[str, Any]] = []
            recovery_snapshot: dict[str, Any] | None = None
            if clear_visible and self._queues is not None:
                try:
                    queue_snapshot = self._queues.gpu_snapshot()
                    recovery_snapshot = copy.deepcopy(
                        self._queues.current_gpu)
                    if recovery_snapshot is None:
                        recovery_snapshot = copy.deepcopy(
                            self._queues.get_loaded_resuming().get("gpu"))
                    if self._queues.gpu_clear() < 0:
                        return False
                    if not self._queues.clear_resuming_slots(
                            "gpu", clear_current=True):
                        self._queues.restore_pending_snapshot(
                            "gpu", queue_snapshot)
                        return False
                except Exception as exc:
                    _log.warning("Processing queue clear failed: %s", exc)
                    try:
                        self._queues.restore_pending_snapshot(
                            "gpu", queue_snapshot)
                    except Exception:
                        pass
                    return False

            if not self.clear_pending_journal():
                if clear_visible and self._queues is not None:
                    try:
                        self._queues.restore_pending_snapshot(
                            "gpu", queue_snapshot)
                        if recovery_snapshot is not None:
                            self._queues.replace_current_task_durable(
                                "gpu", recovery_snapshot,
                                expected_task_id="",
                            )
                    except Exception as exc:
                        _log.warning(
                            "Processing clear compensation failed: %s", exc)
                self._paused.set()
                self._stream.emit_error(
                    "Could not save queue cancellation; pending recovery work "
                    "was kept. Retry Clear after the app data folder is "
                    "writable.")
                return False

            # Both durable stores are empty.  Only now mutate runtime state or
            # signal the worker; a reported failure can never have cancelled
            # work whose recovery rows were retained.
            self._cancel_all.set()
            self._manual_drain.clear()
            with self._jobs_lock:
                discarded_jobs = [
                    *self._jobs,
                    *self._inline_caption_jobs,
                ]
                self._jobs.clear()
                self._inline_caption_jobs.clear()
                job = self._current_job
                if job:
                    job["_cancel_drop_requested"] = True
                if job and "cancel" in job:
                    job["cancel"].set()
                # Keep the job lock through subprocess stop so the worker
                # cannot finalize/pop between our signal and the kill.
                if job:
                    try:
                        self._stop_subprocess(force=True)
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
        self._release_compress_slots(discarded_jobs)
        self._notify_jobs_cancelled(discarded_jobs)
        return True

    def begin_shutdown(self) -> bool:
        """Close admission and durably defer the active task, if any.

        This never calls ``cancel_all``: quitting is not user authorization to
        discard queued work.  ``defer_current`` first reserves the same stable
        task ID at the pending tail in both stores, then cooperatively cancels
        only the active attempt.
        """
        self._shutdown_requested.set()
        self._manual_drain.clear()
        with self._jobs_lock:
            current = self._current_job
            task_id = str((current or {}).get("task_id") or "").strip()
        if not task_id:
            return self._persist_pending()
        if self.defer_current(task_id):
            return True
        # Even if tail reservation failed, both the existing journal current
        # record and QueueState resuming slot remain authoritative. Signal the
        # attempt only after a final journal checkpoint succeeds.
        if not self._persist_pending():
            return False
        with self._jobs_lock:
            current = self._current_job
            if current is not None:
                current["_shutdown_retry"] = True
                cancel = current.get("cancel")
                if cancel is not None:
                    cancel.set()
        try:
            self._send_cancel_command()
        except Exception as exc:
            _log.debug("shutdown cooperative cancel failed: %s", exc)
        return True

    def join_shutdown(self, timeout: float) -> bool:
        """Wait a bounded duration for the processing worker to checkpoint."""
        worker = self._worker_thread
        if worker is None or not worker.is_alive():
            return True
        worker.join(timeout=max(0.0, float(timeout)))
        return not worker.is_alive()

    def force_shutdown(self) -> bool:
        """Stop only this manager's owned children after checkpointing."""
        if not self._shutdown_requested.is_set() and not self.begin_shutdown():
            return False
        with self._jobs_lock:
            current = self._current_job
            if current is not None:
                current["_shutdown_retry"] = True
                cancel = current.get("cancel")
                if cancel is not None:
                    cancel.set()
        try:
            self._stop_subprocess(force=True)
        except Exception as exc:
            _log.warning("forced Whisper shutdown failed: %s", exc)
        punct = getattr(self, "_punct", None)
        if punct is not None and getattr(punct, "_proc", None):
            try:
                from backend.utils import kill_process
                kill_process(punct._proc)
            except Exception as exc:
                _log.warning("forced punctuation shutdown failed: %s", exc)
        try:
            from ..process_runner import PROCESS_REGISTRY
            PROCESS_REGISTRY.terminate_owner("processing", timeout=2.0)
        except Exception as exc:
            _log.warning("forced processing-child shutdown failed: %s", exc)
        return True

    def shutdown_snapshot(self) -> dict[str, Any]:
        with self._jobs_lock:
            current = self._current_job
            return {
                "owner": "processing",
                "accepting": not self._shutdown_requested.is_set(),
                "worker_alive": bool(
                    self._worker_thread is not None
                    and self._worker_thread.is_alive()),
                "current_task_id": str(
                    (current or {}).get("task_id") or ""),
                "pending": len(self._jobs) + len(self._inline_caption_jobs),
            }

    @staticmethod
    def _notify_job_runtime_state(job: dict[str, Any] | None,
                                  state: str, **details: Any) -> None:
        """Notify an attached UI without consuming the success callback."""
        if not isinstance(job, dict):
            return
        callback = job.get("state_cb")
        if not callable(callback):
            return
        payload = {
            "state": str(state or ""),
            "video_id": str(job.get("video_id") or ""),
            "filepath": os.path.normpath(str(job.get("path") or "")),
            **details,
        }
        try:
            callback(payload)
        except Exception as exc:
            _log.debug("transcription state callback failed: %s", exc)

    @staticmethod
    def _job_holds_compress_slot(job: dict[str, Any]) -> bool:
        """Does this job own a reserved compress log slot?

        sync.py emits a `Compression queued…` placeholder under the
        video's own row the moment a compress is requested, so the later
        `Encoding…`/progress/`✓ Compressed` lines replace it in place
        instead of appending under an unrelated channel. Only the encode
        itself ever fills that slot, so work that dies before the encode
        starts has to take the placeholder with it.
        """
        if not isinstance(job, dict):
            return False
        if str(job.get("kind") or "") == "compress":
            return True
        # A transcribe job still carrying an un-enqueued follow-up: the
        # compress it promised will now never be queued.
        return bool(job.get("compress_after")
                    and not job.get("_followup_enqueued"))

    def _release_compress_slot(self, job: dict[str, Any]) -> None:
        """Drop *job*'s reserved compress log slot, if it holds one."""
        if not self._job_holds_compress_slot(job):
            return
        path = str(job.get("path") or "")
        if not path:
            return
        try:
            from ..compress import clear_compress_marker
            clear_compress_marker(self._stream, path)
        except Exception as exc:
            _log.debug("compress slot release failed: %s", exc)

    def _release_compress_slots(self, jobs: list[dict[str, Any]]) -> None:
        """Same, for a batch of jobs dropped before the worker saw them."""
        seen: set[int] = set()
        for job in jobs or []:
            if id(job) in seen:
                continue
            seen.add(id(job))
            self._release_compress_slot(job)

    @classmethod
    def _notify_jobs_cancelled(cls, jobs: list[dict[str, Any]]) -> None:
        """Clear UI state for jobs removed before the worker can see them."""
        notified: set[int] = set()
        for job in jobs:
            identity = id(job)
            if identity in notified:
                continue
            notified.add(identity)
            cls._notify_job_runtime_state(
                job, "cancelled", message="Re-transcription cancelled")

    def pause(self):
        self._paused.set()
        with self._jobs_lock:
            current = self._current_job
        self._notify_job_runtime_state(
            current, "paused", message="Paused — resume from Processing")

    def resume(self):
        self._paused.clear()
        with self._jobs_lock:
            current = self._current_job
        self._notify_job_runtime_state(
            current, "resuming", message="Resuming transcription…")

    @staticmethod
    def _job_path_key(path: str) -> str:
        if not path:
            return ""
        return os.path.normcase(
            os.path.normpath(os.path.abspath(str(path))))

    @classmethod
    def _job_identity_key(
            cls, job: dict[str, Any] | None) -> tuple[str, str] | None:
        if not job:
            return None
        path_key = cls._job_path_key(job.get("path") or "")
        if not path_key:
            return None
        kind = str(job.get("kind") or "transcribe").strip().lower()
        return kind, path_key

    @staticmethod
    def _queue_payload_for_job(job: dict[str, Any]) -> dict[str, Any]:
        """Return the persisted/UI queue representation of a runtime job."""
        payload = {
            "task_id": job.get("task_id", ""),
            "kind": (job.get("kind") or "transcribe").lower(),
            "title": job.get("title", ""),
            "path": job.get("path", ""),
            "channel": job.get("channel", ""),
            "bulk_id": job.get("bulk_id", ""),
            "bulk_total": int(job.get("bulk_total", 0) or 0),
            "bulk_index": int(job.get("bulk_index", 0) or 0),
            "retry_required": bool(job.get("_retry_required")),
            "retry_as_replace": bool(job.get("_retry_as_replace")),
            "write_intent": bool(job.get("_write_intent")),
            "caption_recovery": bool(job.get("_caption_recovery")),
            "skip_pending_counter": bool(
                job.get("_skip_pending_counter")),
            "cleanup_only": bool(job.get("_cleanup_only")),
            "no_speech_pending": bool(job.get("_no_speech_pending")),
            "stats_tallied": bool(job.get("_stats_tallied")),
            "output_complete": bool(job.get("_output_complete")),
            "callback_done": bool(job.get("_callback_done")),
            "followup_pending": bool(job.get("_followup_pending")),
            "followup_enqueued": bool(job.get("_followup_enqueued")),
            "completed_outcome": str(
                job.get("_completed_outcome") or ""),
        }
        if payload["kind"] == "compress":
            payload.update({
                "quality": job.get("quality", "Average"),
                "output_res": str(job.get("output_res", "720")),
                "from_download": bool(job.get("from_download")),
            })
        else:
            payload.update({
                "combined_override": job.get("combined_override"),
                "retranscribe": bool(job.get("retranscribe")),
                "video_id": (job.get("video_id") or "").strip(),
                "from_download": bool(job.get("from_download")),
                "compress_after": dict(job.get("compress_after") or {}),
                "requested_model": str(
                    job.get("requested_model") or ""),
                "actual_model": str(job.get("actual_model") or ""),
            })
        return payload

    def _restore_runtime_jobs_from_queue(self) -> int:
        """Merge persisted GPU tasks into the worker's runtime queue.

        QueueState survives app restarts and supplies the Tasks UI. The worker
        also has an in-memory list, which can be empty after a crash or when an
        older pending journal is missing. Reconcile both before manual Start
        so the worker cannot exit while persisted tasks remain visible.
        """
        if self._queues is None:
            return 0
        try:
            persisted = self._queues.gpu_snapshot()
        except Exception as e:
            _log.warning("could not snapshot persisted GPU queue: %s", e)
            return 0

        restored = 0
        with self._journal_lock:
            with self._jobs_lock:
                original_runtime_jobs = list(self._jobs)
                known = {
                    self._job_identity_key(job)
                    for job in [*self._inline_caption_jobs, *self._jobs]
                    if self._job_identity_key(job) is not None
                }
                if self._job_identity_key(self._current_job) is not None:
                    known.add(self._job_identity_key(self._current_job))

                for item in persisted:
                    path = str(item.get("path") or "")
                    kind = (item.get("kind") or "transcribe").lower()
                    key = (kind, self._job_path_key(path))
                    if not key[1] or key in known or not os.path.isfile(path):
                        continue
                    job: dict[str, Any] = {
                        "task_id": (str(item.get("task_id") or "").strip()
                                    or make_task_id("gpu")),
                        "kind": kind,
                        "path": path,
                        "title": item.get("title") or os.path.basename(path),
                        "channel": item.get("channel", ""),
                        "cb": None,
                        "cancel": threading.Event(),
                    }
                    if kind == "compress":
                        job.update({
                            "quality": item.get("quality", "Average"),
                            "output_res": str(item.get("output_res", "720")),
                            "from_download": bool(
                                item.get("from_download")),
                        })
                    else:
                        job.update({
                            "kind": "transcribe",
                            "combined_override": item.get("combined_override"),
                            "retranscribe": bool(item.get("retranscribe")),
                            "video_id": (item.get("video_id") or "").strip(),
                            "bulk_id": item.get("bulk_id", "") or "",
                            "bulk_total": int(item.get("bulk_total", 0) or 0),
                            "bulk_index": int(item.get("bulk_index", 0) or 0),
                            "from_download": bool(item.get("from_download")),
                            "compress_after": dict(
                                item.get("compress_after") or {}),
                            "requested_model": str(
                                item.get("requested_model")
                                or self._model or ""),
                            "actual_model": str(
                                item.get("actual_model") or ""),
                            "_retry_required": bool(
                                item.get("retry_required")),
                            "_retry_as_replace": bool(
                                item.get("retry_as_replace")),
                            "_write_intent": bool(item.get("write_intent")),
                            "_caption_recovery": bool(
                                item.get("caption_recovery")),
                            "_skip_pending_counter": bool(
                                item.get("skip_pending_counter")),
                            "_cleanup_only": bool(item.get("cleanup_only")),
                            "_no_speech_pending": bool(
                                item.get("no_speech_pending")),
                            "_stats_tallied": bool(
                                item.get("stats_tallied")),
                            "_output_complete": bool(
                                item.get("output_complete")),
                            "_callback_done": bool(
                                item.get("callback_done")),
                            "_followup_pending": bool(
                                item.get("followup_pending")),
                            "_followup_enqueued": bool(
                                item.get("followup_enqueued")),
                            "_completed_outcome": str(
                                item.get("completed_outcome") or ""),
                        })
                    self._jobs.append(job)
                    known.add(key)
                    restored += 1

                # QueueState owns the user-visible pending order. Reconcile
                # the complete runtime tail (not merely newly-added orphans)
                # so a partial/missing journal cannot undo a pre-crash defer.
                persisted_order = {
                    str(item.get("task_id") or "").strip(): index
                    for index, item in enumerate(persisted)
                    if str(item.get("task_id") or "").strip()
                }
                stable_fallback = {
                    id(job): index for index, job in enumerate(self._jobs)
                }
                self._jobs.sort(key=lambda job: (
                    persisted_order.get(
                        str(job.get("task_id") or "").strip(),
                        len(persisted_order)),
                    stable_fallback[id(job)],
                ))
                reordered = any(
                    before is not after
                    for before, after in zip(
                        original_runtime_jobs, self._jobs, strict=False)
                )

            if (restored or reordered) and not self._persist_pending():
                with self._jobs_lock:
                    self._jobs[:] = original_runtime_jobs
                self._stream.emit_error(
                    "Could not save restored Processing tasks; they were not "
                    "started and remain in the durable task queue.")
                return -1
        if restored:
            _log.info("restored %d GPU task(s) into runtime queue", restored)
        return restored

    def request_drain(self) -> bool:
        """One-shot 'Start': drain the queued jobs now even though the Auto
        checkbox is off, WITHOUT turning Auto back on. Clears any lingering
        pause, arms the manual-drain gate, and (re)starts the worker. The
        worker self-clears the gate the instant the queue empties, so future
        arrivals keep queuing until the user clicks Start again."""
        if self._shutdown_requested.is_set():
            return False
        if self._restore_runtime_jobs_from_queue() < 0:
            return False
        self._paused.clear()
        self._manual_drain.set()
        self._ensure_worker()
        return True

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

    def cancel_current_durable(
            self, task_id: str,
            clear_visible: Callable[[], bool]) -> bool:
        """Cancel one exact running job after both recovery stores commit."""
        wanted = str(task_id or "").strip()
        if not wanted or not callable(clear_visible):
            return False
        with self._journal_lock:
            with self._jobs_lock:
                job = self._current_job
                if (not job
                        or str(job.get("task_id") or "").strip() != wanted
                        or "cancel" not in job):
                    return False

            # Commit the journal removal first.  The job is still running and
            # its QueueState current slot remains recoverable until the peer
            # store commits below.
            if not self._write_pending_snapshot(
                    self._pending_snapshot(include_current=False)):
                return False
            try:
                visible_saved = bool(clear_visible())
            except Exception as exc:
                _log.warning(
                    "Processing current-slot cancellation failed: %s", exc)
                visible_saved = False
            if not visible_saved:
                # Restore the first durable store before returning failure.
                # If this compensation itself cannot be saved, QueueState's
                # still-present current slot remains sufficient for recovery.
                self._write_pending_snapshot(
                    self._pending_snapshot(include_current=True))
                return False

            # The worker's outcome must remain a deliberate terminal drop even
            # if cancellation races with an unrelated failure.
            job["_cancel_drop_requested"] = True
            job["cancel"].set()
            try:
                self._send_cancel_command()
            except Exception as exc:
                _log.debug("cooperative Processing cancel failed: %s", exc)
            return True

    def defer_current(self, task_id: str) -> bool:
        """Cancel the exact running task and place that same ID at the tail."""
        wanted = str(task_id or "").strip()
        if not wanted:
            return False
        reservation = None
        with self._journal_lock:
            with self._jobs_lock:
                job = self._current_job
                if (not job
                        or str(job.get("task_id") or "").strip() != wanted
                        or "cancel" not in job):
                    return False
                reserved_ids = {
                    str(existing.get("task_id") or "").strip()
                    for existing in [*self._jobs, *self._inline_caption_jobs]
                    if str(existing.get("task_id") or "").strip()
                }
            if self._queues is None:
                return False
            try:
                reservation = self._queues.gpu_reserve_task(
                    self._queue_payload_for_job(job),
                    reserved_task_ids=reserved_ids,
                    required_task_id=wanted,
                )
            except Exception as exc:
                _log.warning("GPU defer queue reservation failed: %s", exc)
                reservation = None
            if not isinstance(reservation, dict):
                return False
            job["_defer_requested"] = True
            if not self._persist_pending():
                job.pop("_defer_requested", None)
                try:
                    self._queues.gpu_rollback_reservation(reservation)
                except Exception as exc:
                    _log.warning("GPU defer reservation rollback failed: %s", exc)
                self._stream.emit_error(
                    "Could not save the deferred Processing task; the running "
                    "task was not cancelled.")
                return False
            job["cancel"].set()
            try:
                self._send_cancel_command()
            except Exception as e:
                _log.debug("swallowed: %s", e)
        return True

    def _ensure_worker(self):
        if self._shutdown_requested.is_set():
            return
        if self._worker_thread is None or not self._worker_thread.is_alive():
            self._cancel_all.clear()
            self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self._worker_thread.start()

    def _arm_output_write_intent(self, job: dict[str, Any] | None) -> bool:
        """Durably mark a job recoverable before its first sidecar mutation."""
        if not isinstance(job, dict):
            return True
        if job.get("_write_intent"):
            # Loaded recovery jobs can only carry this flag because it already
            # existed in the committed journal. The current first attempt also
            # calls this helper only once, before any output mutation.
            return True
        job["_write_intent"] = True
        job["_retry_required"] = True
        # Do not switch the current first attempt to replacement. If the
        # process dies after this marker, load_pending maps write_intent to
        # replacement before any recovered work can append.
        with self._jobs_lock:
            tracked = (self._current_job is job
                       or any(existing is job for existing in self._jobs))
        if tracked and not self._persist_pending():
            # No output has changed yet. Remove the in-memory authorization so
            # a later manual retry must successfully persist it before writing.
            job.pop("_write_intent", None)
            self._stream.emit_error(
                "Could not save transcription recovery state; transcript "
                "files were left untouched and the task was kept for retry.")
            return False
        return True

    def _restore_job_after_outcome(self, job: dict[str, Any],
                                   outcome: _WorkerOutcome) -> None:
        """Put retryable work back in both durable queue representations."""
        if outcome not in {
                _WorkerOutcome.FAILED,
                _WorkerOutcome.RETRY,
                _WorkerOutcome.CLEANUP_FAILED}:
            return
        if self._cancel_all.is_set():
            return
        deferred = bool(job.pop("_defer_requested", False))
        if deferred:
            job["cancel"] = threading.Event()
        elif outcome is _WorkerOutcome.FAILED:
            job["_retry_required"] = True
            if (job.get("_write_intent")
                    and not job.get("_caption_recovery")
                    and not job.get("_no_speech_pending")
                    and not job.get("_cleanup_only")):
                job["_retry_as_replace"] = True
        elif outcome is _WorkerOutcome.CLEANUP_FAILED:
            job["_cleanup_only"] = True
            # A cancelled work item may still need its durable pending-ID
            # cleanup retried; give that cleanup-only item a fresh event.
            job["cancel"] = threading.Event()
        with self._jobs_lock:
            if not any(existing is job for existing in self._jobs):
                if deferred:
                    self._jobs.append(job)
                else:
                    self._jobs.insert(0, job)
        if self._queues is not None:
            try:
                self._queues.gpu_enqueue(self._queue_payload_for_job(job))
                target = 0
                if deferred:
                    target = max(0, len(self._queues.gpu_snapshot()) - 1)
                self._queues.gpu_reorder(job.get("task_id", ""), target)
            except Exception as e:
                _log.warning("could not restore failed GPU queue item: %s", e)

    @staticmethod
    def _pending_id_present(video_id: str) -> bool | None:
        """Return whether config still contains *video_id*, or None on error."""
        try:
            from .. import ytarchiver_config as _cfg
            cfg = _cfg.load_config()
            for channel in cfg.get("channels", []) or []:
                ids = channel.get("pending_tx_ids")
                if isinstance(ids, list) and video_id in ids:
                    return True
            return False
        except Exception as exc:
            _log.warning("could not verify pending transcription id %s: %s",
                         video_id, exc)
            return None

    def _finish_terminal_pending(self, job: dict[str, Any]) -> bool:
        """Durably drain terminal bookkeeping; return whether it is complete."""
        if ((job.get("kind") or "transcribe") == "compress"
                or job.get("retranscribe")
                or job.get("_pending_decremented")):
            return True
        try:
            channel = (job.get("channel") or "").strip()
            video_id = (job.get("video_id") or "").strip()
            skip_counter = bool(job.get("_skip_pending_counter"))
            if video_id:
                from .. import ytarchiver_config as _cfg
                removed = _cfg.remove_pending_tx_id(video_id)
                if not removed:
                    if self._pending_id_present(video_id) is not False:
                        _log.warning(
                            "pending transcription id %s remains after "
                            "completion; retaining cleanup-only recovery",
                            video_id)
                        return False
                    # The ID was never in the authoritative list (for example
                    # a manual right-click transcribe), so balance enqueue's
                    # legacy cosmetic counter explicitly.
                    if channel and not skip_counter:
                        _bump_transcription_pending(channel, -1)
            elif channel and not skip_counter:
                # Legacy jobs without an authoritative ID still use the
                # cosmetic counter. ID-bearing jobs are recalculated by
                # remove_pending_tx_id in one config transaction.
                _bump_transcription_pending(channel, -1)
            job["_pending_decremented"] = True
            job.pop("_cleanup_only", None)
            return True
        except Exception as e:
            _log.warning("could not finish transcription bookkeeping: %s", e)
            return False

    @staticmethod
    def _mark_no_speech_durable(video_path: str) -> bool:
        try:
            from .. import index as _idx
            return bool(_idx.mark_video_no_speech(video_path))
        except Exception as exc:
            _log.warning("mark_video_no_speech(%s) failed: %s",
                         video_path, exc)
            return False

    def _retry_no_speech_classification(
            self, job: dict[str, Any]) -> _WorkerOutcome:
        """Retry only the durable no-speech marker, never Whisper output."""
        if not self._mark_no_speech_durable(job.get("path", "")):
            return _WorkerOutcome.FAILED
        job.pop("_no_speech_pending", None)
        if not self._finish_successful_job(
                job, {"no_speech": True},
                terminal_outcome=_WorkerOutcome.NO_SPEECH):
            return _WorkerOutcome.CLEANUP_FAILED
        return _WorkerOutcome.NO_SPEECH

    def _worker_loop(self):
        # Whisper availability is checked only after native captions miss in
        # `_transcribe_one`. Cleanup-only recovery, durable no-speech retries,
        # compression, and local caption ingest do not need the 3.11 worker and
        # must remain able to drain when that optional environment is missing.
        while (not self._cancel_all.is_set()
               and not self._shutdown_requested.is_set()):
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
            while (not self._cancel_all.is_set()
                   and not self._shutdown_requested.is_set() and
                   (self._paused.is_set()
                    or not (self._auto_enabled()
                            or self._manual_drain.is_set()))):
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
            if (self._cancel_all.is_set()
                    or self._shutdown_requested.is_set()):
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
            pop_persisted = False
            with self._journal_lock:
                with self._jobs_lock:
                    if not self._jobs:
                        # Queue drained. If this was a one-shot manual "Start"
                        # (Auto still off), disarm it now so future arrivals
                        # queue again instead of auto-draining.
                        self._manual_drain.clear()
                        break
                    job = self._jobs.pop(0)
                    self._current_job = job
                # A worker may not execute a job until the queued->current
                # transition is durable. On failure, restore the exact runtime
                # state represented by the still-authoritative old journal.
                pop_persisted = self._persist_pending()
                if not pop_persisted:
                    with self._jobs_lock:
                        if self._current_job is job:
                            self._current_job = None
                        self._jobs.insert(0, job)
            if not pop_persisted:
                self._manual_drain.clear()
                self._paused.set()
                if self._queues is not None:
                    try:
                        self._queues.set_gpu_paused(True)
                    except Exception as e:
                        _log.warning("could not pause unpersisted GPU queue: %s", e)
                self._stream.emit_error(
                    "Could not save the running-task recovery snapshot; no "
                    "processing was started. Press Start to retry after the "
                    "app data folder is writable.")
                break
            # Reflect "now running" in the shared GPU queue: pop the
            # matching popover entry off `queues.gpu` and stamp it as
            # `current_gpu` so the popover's top row switches to
            # "Transcribing X" / "Compressing X" while the rest shrink
            # upward. Label verb comes from the job's `kind`.
            _job_kind = job.get("kind") or "transcribe"
            _was_cleanup_only = bool(job.get("_cleanup_only"))
            if (_job_kind == "transcribe"
                    and not job.get("_callback_done")):
                if job.get("_output_complete"):
                    self._notify_job_runtime_state(
                        job, "finalizing", message="Finishing transcript…")
                elif (job.get("_retry_required")
                      and not _was_cleanup_only):
                    self._notify_job_runtime_state(
                        job, "resuming", message="Resuming transcription…")
            if self._queues is not None:
                try:
                    self._queues.gpu_pop_matching(
                        task_id=job.get("task_id", ""),
                        expected_path=job.get("path", ""),
                        expected_bulk_id=job.get("bulk_id", ""),
                    )
                    self._queues.set_current_gpu({
                        "task_id": job.get("task_id", ""),
                        "kind": _job_kind,
                        "title": job.get("title", ""),
                        "path": job.get("path", ""),
                        "channel": job.get("channel", ""),
                        "bulk_id": job.get("bulk_id", ""),
                        "bulk_total": int(job.get("bulk_total") or 0),
                        "bulk_index": int(job.get("bulk_index") or 0),
                        "requested_model": str(
                            job.get("requested_model") or ""),
                        "actual_model": str(job.get("actual_model") or ""),
                    })
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            # Track per-channel stats so we can emit a [Trnscr] history
            # row when the worker drains. Matches OLD's _record_transcription.
            ch_name = (job.get("channel") or "").strip() or "\u2014"
            with self._stats_lock:
                if _job_kind == "compress":
                    stats = self._compress_stats.setdefault(ch_name,
                        {"start": time.time(), "done": 0, "err": 0})
                else:
                    stats = self._batch_stats.setdefault(ch_name,
                        {"start": time.time(), "done": 0, "err": 0})
            outcome = _WorkerOutcome.FAILED
            stop_after_job = False
            from ..process_runner import process_owner_scope
            _process_scope = process_owner_scope(
                "processing", str(job.get("task_id") or ""))
            _process_scope.__enter__()
            def _execute_job(job=job, job_kind=_job_kind):
                if job.get("_output_complete"):
                    return self._retry_completed_followup(job)
                if job.get("_cleanup_only"):
                    return (
                        _WorkerOutcome.SUCCESS
                        if self._finish_terminal_pending(job)
                        else _WorkerOutcome.CLEANUP_FAILED)
                if job.get("_no_speech_pending"):
                    return self._retry_no_speech_classification(job)
                if job_kind == "compress":
                    return self._compress_one(job)
                return self._transcribe_one(job)

            def _invalid_result(returned, job_kind=_job_kind) -> None:
                self._stream.emit_error(
                    f"{job_kind.capitalize()} ended without an explicit "
                    "outcome; task left queued for retry.")
                _log.error("%s worker returned invalid outcome %r",
                           job_kind, returned)

            def _execution_error(
                error: BaseException,
                job=job,
                job_kind=_job_kind,
            ) -> None:
                # audit SR-3 (user screenshot): if a transcribe
                # job crashes, the error line must still REPLACE the
                # sync.py-reserved `tx_done_<vid>` placeholder under
                # the channel that owns this video — not land at the
                # log tail (wherever sync is currently processing).
                # Without the marker the "Transcribe crashed" line
                # orphaned itself under unrelated later channels.
                _vid_for_err = (job.get("video_id") or "").strip()
                _marker = f"tx_done_{_vid_for_err}" if _vid_for_err else ""
                _err_tags = [t for t in (
                    _marker, "red", job.get("job_tag", "")) if t]
                # Use the structured emit form so we can carry the
                # marker; emit_error doesn't accept tag lists.
                self._stream.emit([[
                    f"{job_kind.capitalize()} crashed: {error}\n",
                    _err_tags,
                ]])

            try:
                outcome = _JOB_EXECUTOR.run(
                    _execute_job,
                    on_invalid=_invalid_result,
                    on_error=_execution_error,
                )
            finally:
                _process_scope.__exit__(None, None, None)
                # A user Skip/Cancel that races with an error is still an
                # intentional drop, not work we should resurrect.
                shutdown_retry = (
                    self._shutdown_requested.is_set()
                    and not job.get("_cancel_drop_requested")
                    and not job.get("_output_complete")
                )
                if shutdown_retry:
                    # A quit/restart cancellation is a retryable interruption,
                    # never a terminal user cancellation.  Keep the job in the
                    # journal even if the child reports "cancelled" first.
                    job["_shutdown_retry"] = True
                outcome = apply_control_signals(
                    outcome,
                    shutdown_requested=self._shutdown_requested.is_set(),
                    cancel_drop_requested=bool(
                        job.get("_cancel_drop_requested")),
                    defer_requested=bool(job.get("_defer_requested")),
                    cancel_requested=bool(
                        job["cancel"].is_set() or self._cancel_all.is_set()),
                    output_complete=bool(job.get("_output_complete")),
                    write_intent=bool(job.get("_write_intent")),
                )

                tally_outcome = outcome
                if execution_decision(outcome).terminal:
                    if not self._finish_terminal_pending(job):
                        job["_cleanup_only"] = True
                        outcome = _WorkerOutcome.CLEANUP_FAILED

                if tally_outcome is _WorkerOutcome.NO_SPEECH:
                    _vid_for_empty = (job.get("video_id") or "").strip()
                    _empty_marker = (
                        f"tx_done_{_vid_for_empty}" if _vid_for_empty else "")
                    _empty_tags = [t for t in (
                        _empty_marker, "dim", job.get("job_tag", "")) if t]
                    self._stream.emit([[
                        f" — no speech detected in "
                        f"{job.get('title') or 'video'}.\n",
                        _empty_tags,
                    ]])

                terminal_finished = execution_decision(outcome).terminal
                terminal_checkpoint_saved = True
                if terminal_finished:
                    # Before removing either recovery store, checkpoint that
                    # all output/callback work is terminal. If QueueState's
                    # current-slot clear then fails, the journal retry performs
                    # cleanup only and cannot re-run a transcript/compression.
                    job["_cleanup_only"] = True
                    # Successful transcription/no-speech paths already
                    # checkpoint `_output_complete` before returning. Every
                    # other terminal path (including a cancelled transcription)
                    # must commit the cleanup-only marker before its visible
                    # current slot is cleared; otherwise a crash can replay the
                    # old journal entry as executable work.
                    needs_terminal_checkpoint = not job.get("_output_complete")
                    if (needs_terminal_checkpoint
                            and not self._persist_pending()):
                        terminal_checkpoint_saved = False
                        outcome = _WorkerOutcome.CLEANUP_FAILED

                visible_current_cleared = True
                if terminal_finished and not terminal_checkpoint_saved:
                    # The current QueueState slot is still the authoritative
                    # recovery copy. Never clear it after the peer journal
                    # refused the terminal checkpoint.
                    visible_current_cleared = False
                elif self._queues is not None:
                    try:
                        visible = self._queues.current_gpu
                        if isinstance(visible, dict):
                            visible_id = str(
                                visible.get("task_id") or "").strip()
                            job_id = str(job.get("task_id") or "").strip()
                            durable_replace = getattr(
                                type(self._queues),
                                "replace_current_task_durable", None)
                            if visible_id != job_id or not job_id:
                                visible_current_cleared = False
                            elif callable(durable_replace):
                                visible_current_cleared = bool(
                                    self._queues.replace_current_task_durable(
                                        "gpu", None,
                                        expected_task_id=job_id))
                            else:
                                # Transitional queue adapters do not expose a
                                # synchronous commit result. Preserve their old
                                # behavior without weakening real QueueState.
                                self._queues.set_current_gpu(None)
                    except Exception as exc:
                        _log.warning(
                            "could not durably finalize Processing current "
                            "slot: %s", exc)
                        visible_current_cleared = False
                if terminal_finished and not visible_current_cleared:
                    outcome = _WorkerOutcome.CLEANUP_FAILED

                self._restore_job_after_outcome(job, outcome)
                if outcome is _WorkerOutcome.CANCELLED:
                    # FAILED/RETRY keep the task in Processing, so their
                    # reserved compress slot stays valid until the retry
                    # fills it. A cancelled job is gone for good.
                    self._release_compress_slot(job)
                if execution_decision(outcome).pause_for_retry:
                    # A deterministic failure must not hot-loop. The task
                    # remains first in both queues; Start/Resume retries it.
                    stop_after_job = True
                    self._manual_drain.clear()
                    self._paused.set()
                    if self._queues is not None:
                        try:
                            self._queues.set_gpu_paused(True)
                        except Exception as e:
                            _log.warning("could not pause failed GPU queue: %s", e)
                    if outcome is _WorkerOutcome.CLEANUP_FAILED:
                        if job.get("_followup_pending"):
                            self._stream.emit_error(
                                "Transcription finished, but its compression "
                                "follow-up could not be saved — completion "
                                "task kept in Processing. Press Start to retry.")
                        else:
                            self._stream.emit_error(
                                "Transcription finished, but pending-list "
                                "cleanup could not be saved — cleanup task "
                                "kept in Processing. Press Start to retry.")
                    else:
                        self._stream.emit_error(
                            f"{_job_kind.capitalize()} failed — task kept in "
                            "Processing. Press Start to retry.")

                if (not job.get("_stats_tallied")
                        and tally_outcome in {_WorkerOutcome.SUCCESS,
                                              _WorkerOutcome.NO_SPEECH}):
                    with self._stats_lock:
                        stats["done"] += 1
                    job["_stats_tallied"] = True
                elif (not job.get("_stats_tallied")
                      and tally_outcome is _WorkerOutcome.FAILED):
                    with self._stats_lock:
                        stats["err"] += 1
                journal_saved = False
                with self._journal_lock:
                    with self._jobs_lock:
                        if self._current_job is job:
                            self._current_job = None
                    journal_saved = self._persist_pending()
                    if not journal_saved and not self._cancel_all.is_set():
                        with self._jobs_lock:
                            if not any(existing is job for existing in self._jobs):
                                if tally_outcome in {
                                        _WorkerOutcome.SUCCESS,
                                        _WorkerOutcome.NO_SPEECH,
                                        _WorkerOutcome.CANCELLED}:
                                    # Outputs/callbacks already completed. A
                                    # journal-clear retry must never run them a
                                    # second time; it only retries durable task
                                    # removal on the next manual Start.
                                    job["_cleanup_only"] = True
                                    job["cancel"] = threading.Event()
                                self._jobs.insert(0, job)
                if not journal_saved and not self._cancel_all.is_set():
                    stop_after_job = True
                    self._manual_drain.clear()
                    self._paused.set()
                    if self._queues is not None:
                        try:
                            self._queues.gpu_enqueue(
                                self._queue_payload_for_job(job))
                            self._queues.gpu_reorder(
                                job.get("task_id", ""), 0)
                            self._queues.set_gpu_paused(True)
                        except Exception as e:
                            _log.warning(
                                "could not expose journal recovery task: %s", e)
                    self._stream.emit_error(
                        "Task output finished, but its recovery journal could "
                        "not be finalized — task kept in Processing. Press "
                        "Start to retry journal cleanup.")
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
                        and _job_kind != "compress"
                        and outcome is not _WorkerOutcome.RETRY):
                    self._cpu_fallback_active = False
                    try:
                        # No global env pop needed any more — the
                        # next _start_subprocess reads the instance
                        # flag (now False) and rebuilds env on GPU
                        # automatically.
                        self._stop_subprocess(force=True)
                        _reset_label = (
                            "\u21A9 Resetting to GPU mode for next job."
                            if outcome is not _WorkerOutcome.FAILED else
                            "\u21A9 Resetting to GPU mode (fallback job crashed "
                            "\u2014 giving GPU another try).")
                        self._stream.emit_text(
                            " " + _reset_label, "simpleline_blue")
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                # Clear the "running" slot on completion so the popover
                # returns to idle (or shows the next queued item as the
                # next iteration sets its own current_gpu).
                if (self._queues is not None
                        and not callable(getattr(
                            type(self._queues),
                            "replace_current_task_durable", None))):
                    try:
                        self._queues.set_current_gpu(None)
                    except Exception as e:
                        _log.debug("swallowed: %s", e)

                # A successful job consumes its completion callback and clears
                # Watch state there. Outcomes that do not consume that callback
                # need a separate signal so the UI cannot remain on a stale
                # active phase. A cleanup-only retry after cancellation has no
                # success callback by design, so clear it as cancelled once the
                # durable cleanup succeeds.
                if (_job_kind == "transcribe"
                        and _was_cleanup_only
                        and outcome is _WorkerOutcome.SUCCESS
                        and not job.get("_output_complete")
                        and not job.get("_callback_done")):
                    self._notify_job_runtime_state(
                        job, "cancelled",
                        message="Re-transcription cancelled")
                elif (_job_kind == "transcribe"
                      and not job.get("_callback_done")):
                    if outcome in {
                            _WorkerOutcome.FAILED,
                            _WorkerOutcome.CLEANUP_FAILED}:
                        self._notify_job_runtime_state(
                            job, "needs_attention",
                            message=("Needs attention — retry from "
                                     "Processing"))
                    elif outcome is _WorkerOutcome.CANCELLED:
                        self._notify_job_runtime_state(
                            job, "cancelled",
                            message="Re-transcription cancelled")
                    elif outcome is _WorkerOutcome.RETRY:
                        self._notify_job_runtime_state(
                            job, "queued",
                            message="Waiting to retry")

            if stop_after_job:
                break

        # Flush per-channel batch stats to autorun_history + activity log.
        # One row per channel processed in this worker session.
        try:
            self._flush_batch_stats()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        self._stream.flush()

    def _channel_aliases_for_job(self, job: dict[str, Any]):
        """Resolve the same channel root aliases used by sync/reorganization."""
        from backend.services.channel_leases import channel_aliases

        path = os.path.abspath(str(job.get("path") or ""))
        cfg = self._cfg_loader() if callable(self._cfg_loader) else {}
        cfg = cfg if isinstance(cfg, dict) else {}
        output_dir = os.path.abspath(str(cfg.get("output_dir") or "")) \
            if cfg.get("output_dir") else ""
        requested_name = str(job.get("channel") or "").strip().casefold()
        matched = None
        root = ""
        try:
            from ..sync import channel_folder_name
            for channel in cfg.get("channels", []) or []:
                folder_name = channel_folder_name(channel)
                candidate = (os.path.join(output_dir, folder_name)
                             if output_dir and folder_name else "")
                names = {
                    str(channel.get(key) or "").strip().casefold()
                    for key in ("name", "folder", "folder_override")
                }
                path_matches = bool(
                    candidate and os.path.commonpath([path, candidate])
                    == os.path.commonpath([candidate]))
                if (requested_name and requested_name in names) or path_matches:
                    matched = channel
                    root = candidate
                    break
        except (OSError, ValueError):
            matched = None
            root = ""
        if not root and output_dir:
            try:
                relative = os.path.relpath(path, output_dir)
                first = relative.split(os.sep, 1)[0]
                if first not in {"", ".", ".."} and not relative.startswith(
                        ".." + os.sep):
                    root = os.path.join(output_dir, first)
            except (OSError, ValueError):
                pass
        if not root:
            root = os.path.dirname(path) or path
        return channel_aliases(matched, paths=[root])

    def _run_under_channel_lease(self, job: dict[str, Any], callback):
        from backend.services.channel_leases import LeaseOwner, channel_leases

        task_id = str(job.get("task_id") or make_task_id("gpu"))
        aliases = self._channel_aliases_for_job(job)
        owner = LeaseOwner(
            "processing", task_id,
            label="GPU processing", task_id=task_id,
            kind=str(job.get("kind") or "transcribe"))
        waiting_reported = False
        while True:
            result = channel_leases.acquire(
                aliases,
                owner,
                # A download can legitimately own this channel for longer
                # than five seconds.  Use a bounded wait so cancellation stays
                # responsive, then keep waiting instead of turning ordinary
                # sync/Processing overlap into a failed, paused queue.
                timeout=5.0,
                cancel_event=job.get("cancel"),
            )
            if result.ok and result.lease is not None:
                with result.lease:
                    return callback(job)
            if self._shutdown_requested.is_set():
                return _WorkerOutcome.RETRY
            if job.get("cancel") is not None and job["cancel"].is_set():
                return _WorkerOutcome.CANCELLED
            if result.status == "timeout":
                if not waiting_reported:
                    self._stream.emit_text(
                        "Processing is waiting for another task on this "
                        "channel to finish.\n",
                        "simpleline_blue",
                    )
                    waiting_reported = True
                continue
            self._stream.emit_error(
                "Processing could not start: " + result.explanation)
            return _WorkerOutcome.FAILED

    def _compress_one(self, job: dict[str, Any]) -> _WorkerOutcome:
        return self._run_under_channel_lease(job, self._compress_one_unleased)

    def _compress_one_unleased(self, job: dict[str, Any]) -> _WorkerOutcome:
        """Run one compress job from the GPU queue — delegates to
        backend.compress.compress_video(). Shares the same worker
        thread as transcribe so only one GPU task runs at a time."""
        if job["cancel"].is_set():
            return _WorkerOutcome.CANCELLED
        try:
            from .. import compress as _cmp
        except Exception as e:
            self._stream.emit_error(f"Compress: import failed: {e}")
            return _WorkerOutcome.FAILED
        try:
            from ..process_runner import process_owner_scope
            with process_owner_scope(
                    "processing", str(job.get("task_id") or "")):
                res = _cmp.compress_video(
                    job["path"],
                    self._stream,
                    quality=job.get("quality", "Average"),
                    output_res=str(job.get("output_res", "720")),
                    cancel_event=job["cancel"],
                    process_owner="processing",
                    task_id=str(job.get("task_id") or ""),
                    from_download=bool(job.get("from_download")),
                )
        except Exception as e:
            self._stream.emit_error(f"Compress: {e}")
            return _WorkerOutcome.FAILED
        if not isinstance(res, dict) or not res.get("ok"):
            if (job["cancel"].is_set()
                    or (isinstance(res, dict)
                        and res.get("reason") == "cancelled")):
                return _WorkerOutcome.CANCELLED
            return _WorkerOutcome.FAILED
        if job.get("cb"):
            try: job["cb"](res)
            except Exception as e: _log.debug("swallowed: %s", e)
        return _WorkerOutcome.SUCCESS

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

    def _transcribe_one(self, job: dict[str, Any]) -> _WorkerOutcome:
        return self._run_under_channel_lease(job, self._transcribe_one_unleased)

    def _transcribe_one_unleased(self, job: dict[str, Any]) -> _WorkerOutcome:
        path = job["path"]
        title = job["title"]
        # ``_retried_cpu`` is scoped to one GPU->CPU execution cycle. A
        # downstream write/index failure retains the job, but must not make a
        # later manual retry permanently ineligible for CPU fallback.
        if not self._cpu_fallback_active:
            job.pop("_retried_cpu", None)
        if job["cancel"].is_set():
            return _WorkerOutcome.CANCELLED

        # if GPU Auto was unchecked AFTER this job was
        # popped but BEFORE we started processing, re-park it at the
        # front of the queue and bail. Without this guard the worker
        # would keep firing auto-captions / Whisper for several
        # already-popped jobs even though the user explicitly asked
        # for queue-up behavior. Exception: a one-shot manual "Start"
        # (self._manual_drain) is an explicit request to process the
        # backlog now, so it overrides the Auto-off re-park.
        if not (self._auto_enabled() or self._manual_drain.is_set()):
            return _WorkerOutcome.RETRY

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
        caption_outcome = _CaptionOutcome.UNAVAILABLE
        if (not job.get("retranscribe")
                and (job.get("_caption_recovery")
                     or not job.get("_retry_as_replace"))):
            if self._current_job is job:
                if not self._arm_output_write_intent(job):
                    return _WorkerOutcome.FAILED
            caption_outcome = _coerce_caption_outcome(_try_auto_captions(
                path, title, job.get("channel", ""), self._stream,
                punct_mgr=_punct_for_captions,
                job_tag=job_tag,
                video_id_hint=job.get("video_id", ""),
                from_download=bool(job.get("from_download")),
                combined_override=job.get("combined_override"),
                cancel_event=job.get("cancel"),
                update_pending=False))
            if caption_outcome is _CaptionOutcome.CANCELLED:
                return _WorkerOutcome.CANCELLED
            if caption_outcome is _CaptionOutcome.SUCCESS:
                if not self._finish_successful_job(
                        job, {"auto_captions": True}):
                    return _WorkerOutcome.CLEANUP_FAILED
                return _WorkerOutcome.SUCCESS
            if caption_outcome in {
                    _CaptionOutcome.FAILED, _CaptionOutcome.PARTIAL}:
                job["_retry_required"] = True
                if (caption_outcome is _CaptionOutcome.PARTIAL
                        and not job.get("_caption_recovery")):
                    job["_retry_as_replace"] = True
                self._persist_pending()
                return _WorkerOutcome.FAILED

        if (job.get("_caption_recovery")
                and caption_outcome is _CaptionOutcome.UNAVAILABLE):
            # If the local caption source disappeared after the interruption,
            # Whisper may recover the job, but it must use surgical replacement
            # because TXT and/or JSONL could already have committed.
            job["_retry_as_replace"] = True

        # Auto-captions path missed — either no .vtt available, yt-dlp
        # couldn't fetch captions for this video, or the VTT parse came
        # back empty. was flagged the silent-failure case: NO log
        # line at all between "Metadata downloaded" and the next
        # channel's header. Emit a visible fallback line so the user
        # sees Whisper take over instead of the transcription just
        # vanishing. Uses the `whisper_progress` inplace family so the
        # final "— ✓ Transcription" done line replaces this too.
        if (not job.get("retranscribe")
                and caption_outcome is _CaptionOutcome.UNAVAILABLE):
            self._stream.emit([[
                " No auto-captions available \u2014 using Whisper\u2026\n",
                ["transcribe_using", job_tag],
            ]])

        if not self.is_available():
            self._stream.emit_error(
                "Whisper: Python 3.11 not found. Install from python.org "
                "to enable transcription. Task kept for retry.")
            return _WorkerOutcome.FAILED

        if job["cancel"].is_set() or self._cancel_all.is_set():
            return _WorkerOutcome.CANCELLED
        if not self._prepare_job_model(job):
            return _WorkerOutcome.FAILED

        proc, _line_q = self._snapshot_worker_io()
        if proc is None or proc.poll() is not None:
            if not self.start_subprocess(
                    model=str(job.get("requested_model") or self._model)):
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
                return _WorkerOutcome.FAILED

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
            return self._transcribe_chunked(job, duration)

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
            if job["cancel"].is_set() or self._cancel_all.is_set():
                return _WorkerOutcome.CANCELLED
            return _WorkerOutcome.FAILED
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
            if job["cancel"].is_set() or self._cancel_all.is_set():
                return _WorkerOutcome.CANCELLED
            return _WorkerOutcome.FAILED

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
                return _WorkerOutcome.CANCELLED
            try:
                _proc_snapshot, q = self._snapshot_worker_io()
                if q is None:
                    if job["cancel"].is_set() or self._cancel_all.is_set():
                        return _WorkerOutcome.CANCELLED
                    self._stream.emit_error("Transcription stopped unexpectedly. Try again.")
                    return _WorkerOutcome.FAILED
                line = q.get(timeout=0.5)
            except queue.Empty:
                continue
            if line is None:
                self._stream.emit_error("Transcription stopped unexpectedly. Try again.")
                self._emit_whisper_stderr_tail()
                return _WorkerOutcome.FAILED
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
                return _WorkerOutcome.CANCELLED
            if status == "ok":
                if not self._accept_worker_model_report(
                    msg,
                    job,
                    phase="result",
                ):
                    return _WorkerOutcome.FAILED
                result = msg
                # Recognition is complete, but punctuation, transcript writes,
                # and FTS indexing still remain. Do not leave the last 99% tick
                # on screen during that work -- it makes a healthy finalization
                # phase look like Whisper itself has hung.
                # Machine-readable phase tag lets the Watch progress control
                # leave its last 99% value behind while punctuation,
                # transcript writes, and indexing run.
                self._stream.emit(_build_transcription_finalizing_segments(
                    job, _disp_title, lead=_prog_lead))
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
                    # Retry once on CPU. The worker owns the requeue so pending
                    # counters/journal/UI state all stay in one place.
                    if not job.get("_retried_cpu"):
                        job["_retried_cpu"] = True
                        return _WorkerOutcome.RETRY
                    return _WorkerOutcome.FAILED
                self._stream.emit_error(f"Transcription error: {err}")
                self._emit_whisper_traceback(msg)
                return _WorkerOutcome.FAILED

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
                # call, so the source tag wrongly read "+NO-PUNCT".
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
            replace_existing = bool(
                job.get("retranscribe") or job.get("_retry_as_replace"))
            output_outcome = self._write_outputs(
                path, result, title=title, channel=channel,
                combined_override=job.get("combined_override"),
                retranscribe=replace_existing,
                video_id_hint=job.get("video_id", ""),
                job=job)
            if output_outcome is _WorkerOutcome.NO_SPEECH:
                if not self._finish_successful_job(
                        job, {"no_speech": True},
                        terminal_outcome=_WorkerOutcome.NO_SPEECH):
                    return _WorkerOutcome.CLEANUP_FAILED
                return output_outcome
            if output_outcome is not _WorkerOutcome.SUCCESS:
                return output_outcome
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
            _model_label = str(
                job.get("actual_model")
                or job.get("requested_model")
                or self._loaded_model or "").strip()
            # Only emit a realtime ratio when we have a real ffprobe
            # duration — otherwise the displayed "Nx realtime" is
            # derived from a chunking-routing sentinel, not the actual
            # video length.
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
            if not self._finish_successful_job(job, result):
                return _WorkerOutcome.CLEANUP_FAILED
            return _WorkerOutcome.SUCCESS

        return _WorkerOutcome.FAILED

    def _transcribe_chunked(self, job: dict[str, Any],
                            total_duration: float) -> _WorkerOutcome:
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
                    return _WorkerOutcome.CANCELLED
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
                if cancel.is_set() or self._cancel_all.is_set():
                    return _WorkerOutcome.CANCELLED

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
                # (pooled archive + antivirus) for a long chunk, dropping that
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
                    return _WorkerOutcome.FAILED
                except Exception as e:
                    self._stream.emit_error(
                        f"Section {ci+1}/{n_chunks} split failed: {e} "
                        f"— aborting chunked transcribe to avoid silent gaps "
                        f"in the merged transcript.")
                    return _WorkerOutcome.FAILED

                # Hand the chunk to Whisper via the persistent subprocess.
                section_prefix = f" Section {ci+1}/{n_chunks},"
                t_start = time.time()
                chunk_outcome, result = self._transcribe_single_file(
                    chunk_path, job, _log_prefix=section_prefix)
                t_elapsed = time.time() - t_start
                try: os.remove(chunk_path)
                except Exception as e: _log.debug("swallowed: %s", e)

                if chunk_outcome is _WorkerOutcome.CANCELLED:
                    return _WorkerOutcome.CANCELLED
                if chunk_outcome is not _WorkerOutcome.SUCCESS:
                    return _WorkerOutcome.FAILED
                if not result or not (
                        (result.get("text") or "").strip()
                        or any((seg.get("t") or seg.get("text") or "").strip()
                               for seg in (result.get("segments") or [])
                               if isinstance(seg, dict))):
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

            # Merge result. An all-silent video deliberately flows through the
            # output classifier below so it becomes terminal NO_SPEECH instead
            # of an ambiguous normal return.
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
            # Recognition for every chunk is complete, but merged punctuation,
            # sidecar writes, and search indexing still remain. Match the
            # short-video Watch state instead of leaving the last section's
            # percentage on screen throughout that work.
            self._stream.emit(_build_transcription_finalizing_segments(
                job, _disp_title_chunked))
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
            replace_existing = bool(
                job.get("retranscribe") or job.get("_retry_as_replace"))
            output_outcome = self._write_outputs(
                path, merged, title=title, channel=channel,
                combined_override=job.get("combined_override"),
                retranscribe=replace_existing,
                video_id_hint=job.get("video_id", ""),
                job=job)
            if output_outcome is _WorkerOutcome.NO_SPEECH:
                if not self._finish_successful_job(
                        job, {"no_speech": True},
                        terminal_outcome=_WorkerOutcome.NO_SPEECH):
                    return _WorkerOutcome.CLEANUP_FAILED
                return output_outcome
            if output_outcome is not _WorkerOutcome.SUCCESS:
                return output_outcome
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
            # a single-space line that visually misaligned in sync logs.
            # Shared done-line builder (T167); chunked uses the job_tag tag
            # families and a dim trailing detail with the "chunked, took\u2026"
            # text instead of the single-pass model/realtime detail.
            _segs_c = _build_transcription_done_segments(
                job, title, channel, f"chunked, took {_time_str_c}",
                dim_tags=_dim_tag, em_tags=_em_tag, lbl_tags=_lbl_tag,
                txt_tags=_txt_tag, detail_tags=_dim_tag)
            self._stream.emit(_segs_c)
            if not self._finish_successful_job(job, merged):
                return _WorkerOutcome.CLEANUP_FAILED
            return _WorkerOutcome.SUCCESS
        finally:
            try: shutil.rmtree(chunk_dir, ignore_errors=True)
            except Exception as e: _log.debug("swallowed: %s", e)

    def _transcribe_single_file(self, path: str, job: dict[str, Any],
                                 _log_prefix: str = "") -> tuple[
                                     _WorkerOutcome, dict[str, Any] | None]:
        """Send one file to the persistent whisper subprocess and collect the
        result. Used by the chunked path to do each section. Returns the
        explicit outcome plus the parsed worker JSON when successful.

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
            if not self.start_subprocess(
                    model=str(job.get("requested_model") or self._model)):
                return _WorkerOutcome.FAILED, None
            proc, _line_q = self._snapshot_worker_io()
        if proc is None:
            return _WorkerOutcome.FAILED, None
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
            if job["cancel"].is_set() or self._cancel_all.is_set():
                return _WorkerOutcome.CANCELLED, None
            return _WorkerOutcome.FAILED, None
        _last_pct = -1
        _job_tag_p = (job.get("job_tag") or "") if isinstance(job, dict) else ""
        _prefix_str = (_log_prefix or "").strip()
        while True:
            if job["cancel"].is_set() or self._cancel_all.is_set():
                if self._cancel_all.is_set():
                    self._stop_subprocess(force=True)
                elif not self._graceful_cancel_current():
                    self._stop_subprocess(force=True)
                return _WorkerOutcome.CANCELLED, None
            # Whisper cannot pause inference in flight. Continue draining its
            # result and keep the UI in "finishing current task" until the next
            # chunk/job boundary, where the worker can actually become idle.
            if job["cancel"].is_set() or self._cancel_all.is_set():
                if self._cancel_all.is_set():
                    self._stop_subprocess(force=True)
                elif not self._graceful_cancel_current():
                    self._stop_subprocess(force=True)
                return _WorkerOutcome.CANCELLED, None
            try:
                _proc_snapshot, q = self._snapshot_worker_io()
                if q is None:
                    if job["cancel"].is_set() or self._cancel_all.is_set():
                        return _WorkerOutcome.CANCELLED, None
                    self._stream.emit_error("Transcription stopped unexpectedly. Try again.")
                    return _WorkerOutcome.FAILED, None
                line = q.get(timeout=0.5)
            except queue.Empty:
                continue
            if line is None:
                self._stream.emit_error("Transcription stopped unexpectedly. Try again.")
                self._emit_whisper_stderr_tail()
                return _WorkerOutcome.FAILED, None
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
                return _WorkerOutcome.CANCELLED, None
            if status == "ok":
                if not self._accept_worker_model_report(
                    msg,
                    job,
                    phase="result",
                ):
                    return _WorkerOutcome.FAILED, None
                return _WorkerOutcome.SUCCESS, msg
            if status == "error":
                self._stream.emit_error(
                    f"Whisper error{(' (' + _log_prefix.strip() + ')') if _log_prefix else ''}: "
                    f"{msg.get('text', 'unknown')}")
                self._emit_whisper_traceback(msg)
                return _WorkerOutcome.FAILED, None

    def _write_outputs(self, video_path: str, result: dict[str, Any],
                       title: str = "", channel: str = "",
                       combined_override: bool | None = None,
                       retranscribe: bool = False,
                       video_id_hint: str = "",
                       job: dict | None = None) -> _WorkerOutcome:
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
        def _output_cancelled():
            per_job = job.get("cancel") if isinstance(job, dict) else None
            return self._cancel_all.is_set() or (per_job is not None and per_job.is_set())

        if _output_cancelled():
            return _WorkerOutcome.CANCELLED
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

        if _output_cancelled():
            return _WorkerOutcome.CANCELLED
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
            # A re-transcribe is a proposed replacement for content the user
            # already has. If the new Whisper run hears nothing, keep that
            # known-good transcript and its searchable/indexed state instead
            # of relabeling the video no-speech while stale words remain.
            if retranscribe:
                result["_existing_transcript_kept"] = True
                self._stream.emit_text(
                    f" \u26a0 No speech was detected in "
                    f"{os.path.basename(video_path)}; existing transcript kept.",
                    "yellow")
                return _WorkerOutcome.SUCCESS
            # Persist a TERMINAL 'no_speech' status so this silent / music-
            # only video is not re-attempted by auto + bulk transcribe passes,
            # and so the Watch view can say "No speech detected" instead of the
            # generic "No transcript available." We STILL raise below so the
            # worker loop emits the benign "no speech detected" line and counts
            # the job as done (not an error). The classification itself is
            # required durable state: without it, auto/bulk passes will queue
            # this silent video forever. Retain a classification-only retry
            # instead of re-running Whisper when the DB write fails.
            if self._mark_no_speech_durable(video_path):
                if isinstance(job, dict):
                    job.pop("_no_speech_pending", None)
                return _WorkerOutcome.NO_SPEECH
            if isinstance(job, dict):
                job["_no_speech_pending"] = True
                job["_retry_required"] = True
                self._persist_pending()
            self._stream.emit_error(
                f"No speech was detected, but its durable status could not "
                f"be saved for {os.path.basename(video_path)}. Task kept for "
                "classification retry.")
            return _WorkerOutcome.FAILED

        # Extract video id — OLD-compat filenames don't carry the `[id]`
        # suffix. Order: hint -> filename `[id]` -> FTS `videos` table.
        # consolidated into _extract_video_id helper.
        vid_id = _extract_video_id(video_path, hint=video_id_hint or "")

        # Source tag: prefer the validated model reported by the worker.  The
        # durable job value is the fallback for a merged chunk result built in
        # this parent process after each child response was validated.
        model_name = str(
            result.get("model")
            or (job or {}).get("actual_model")
            or (job or {}).get("requested_model")
            or self._loaded_model or "").strip()
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
        if _output_cancelled():
            return _WorkerOutcome.CANCELLED
        if not self._arm_output_write_intent(job):
            return _WorkerOutcome.FAILED

        _jsonl_replacement_receipt: dict[str, Any] = {}
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
                # Fail FAST before touching the .jsonl when we can't
                # capture a backup — otherwise a .txt failure later
                # would leave new .jsonl + old .txt with no way to
                # recover (audit: transcribe/core.py H52).
                self._stream.emit_error(
                    f"Refusing retranscribe of "
                    f"{os.path.basename(jsonl_path)}: backup capture "
                    f"failed ({_bke}). Files left untouched.")
                return _WorkerOutcome.FAILED
            # Backup capture and journal writes can take time. Accept Cancel
            # until the first output changes, then finish the paired commit.
            if _output_cancelled():
                return _WorkerOutcome.CANCELLED
            try:
                extra_titles = _replace_jsonl_entry(
                    jsonl_path, title, vid_id, segs,
                    receipt_out=_jsonl_replacement_receipt) or set()
            except Exception as _je:
                self._stream.emit_error(
                    f"Could not update {os.path.basename(jsonl_path)}: {_je}"
                    f" — .txt left unchanged to avoid split-state.")
                return _WorkerOutcome.FAILED
            try:
                _old_txt_candidates = _jsonl_text_candidates_from_bytes(
                    _jsonl_backup, title, vid_id)
                _replace_txt_entry(txt_path, title, text, source_tag,
                                   extra_titles_to_remove=extra_titles,
                                   old_text_candidates=_old_txt_candidates,
                                   video_id=vid_id,
                                   upload_date=upload_date,
                                   duration_secs=duration)
            except Exception as _te:
                self._stream.emit_error(
                    f"Could not update {os.path.basename(txt_path)}: {_te}"
                    f" — attempting .jsonl roll-back to prevent split-state.")
                # Best-effort .jsonl roll-back so the two files stay
                # consistent. If the roll-back itself fails the user
                # is notified with a clear message.
                if _jsonl_backup is not None:
                    _rb_tmp = ""
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
                            _jw.flush()
                            os.fsync(_jw.fileno())
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
                        if _rb_tmp:
                            try: os.remove(_rb_tmp)
                            except OSError: pass
                        self._stream.emit_error(
                            f"Roll-back of {os.path.basename(jsonl_path)} "
                            f"FAILED: {_re}. Files may be out of sync; "
                            f"retry retranscribe when writable.")
                else:
                    # The replacement created a brand-new JSONL. Restoring the
                    # prior "missing" state means deleting it when the paired
                    # TXT replacement fails.
                    try:
                        from ..utils import unhide_file_win as _rb_unhide
                        _rb_unhide(os.path.normpath(jsonl_path))
                        os.remove(jsonl_path)
                        self._stream.emit_error(
                            f"Removed newly-created "
                            f"{os.path.basename(jsonl_path)} during roll-back "
                            "— files consistent; retry when TXT is writable.")
                    except FileNotFoundError:
                        pass
                    except OSError as _re:
                        self._stream.emit_error(
                            f"Roll-back delete of "
                            f"{os.path.basename(jsonl_path)} FAILED: {_re}. "
                            "Files may be out of sync; retry when writable.")
                # Either way, do NOT fall through to the FTS ingest —
                # the txt update failed, so indexing the new segments
                # (or re-marking the video transcribed) would certify
                # a state the visible transcript doesn't match.
                return _WorkerOutcome.FAILED
            _hide_per_video_transcript_txt_if_needed(video_path, txt_path)
        else:
            if _output_cancelled():
                return _WorkerOutcome.CANCELLED
            if not _write_transcript_entry(txt_path, title, upload_date,
                                           duration, source_tag, text,
                                           video_id=vid_id):
                self._stream.emit_error(f"Could not write transcript to {txt_path}")
                return _WorkerOutcome.FAILED
            if isinstance(job, dict) and not job.get("retranscribe"):
                # From this point a retry must replace, not append. Persisting
                # this stage makes a JSONL/index failure restart-safe without
                # duplicating the already-written TXT entry.
                job["_retry_as_replace"] = True
                job["_retry_required"] = True
                self._persist_pending()
            _hide_per_video_transcript_txt_if_needed(video_path, txt_path)
            if not _write_jsonl_entry(jsonl_path, vid_id, title, segs):
                self._stream.emit_error(
                    f"Could not write transcript JSONL to {jsonl_path} "
                    f"— not marking {os.path.basename(video_path)} transcribed")
                return _WorkerOutcome.FAILED

        # Ingest into the search index. A verified re-transcribe receipt lets
        # us replace only this video's rows. If its identity, file generation,
        # or prior index tracker cannot be proven, fall back to the established
        # full validated ingest of the current aggregate.
        try:
            from .. import index as _idx
            # Use a dedicated writer connection. The shared ingest path first
            # waits for the process-wide `_db_lock`; a long maintenance query
            # can hold that Python lock indefinitely even when SQLite itself is
            # ready to accept this tiny per-video ingest. That left completed
            # transcript files on disk while the queue stayed at 99% forever.
            # The independent connection bypasses unrelated Python lock holders;
            # WAL/busy_timeout still serializes actual SQLite writers safely.
            _ingest_conn = _idx._open_independent()
            if _ingest_conn is None:
                raise RuntimeError("could not open an independent index connection")
            try:
                _ingested = 0
                if (retranscribe and vid_id
                        and _jsonl_replacement_receipt):
                    _delta = _idx.replace_video_segments(
                        video_path, channel, _jsonl_replacement_receipt,
                        _conn_override=_ingest_conn)
                    if _delta.get("ok"):
                        _ingested = int(_delta.get("count") or 0)
                    elif _delta.get("can_fallback", True):
                        self._stream.emit_dim(
                            " (using full transcript index refresh: "
                            f"{_delta.get('reason') or 'verification unavailable'})")
                        _ingested = _idx.ingest_jsonl(
                            video_path, jsonl_path, title, channel,
                            _conn_override=_ingest_conn, force=True)
                    else:
                        raise RuntimeError(
                            "per-video index update failed: "
                            f"{_delta.get('reason') or 'database write failed'}")
                else:
                    _ingested = _idx.ingest_jsonl(
                        video_path, jsonl_path, title, channel,
                        _conn_override=_ingest_conn)
            finally:
                _ingest_conn.close()
            if not _ingested:
                raise RuntimeError("index ingest returned no transcript segments")
        except Exception as e:
            # Bug [101]: was emit_dim — invisible in Simple log mode. The
            # transcript file IS on disk but FTS is out of sync (search
            # won't find this video). User-actionable, so use the red
            # convention used elsewhere for warnings/failures so it shows
            # in Simple mode too.
            self._stream.emit_text(
                f" \u26a0 FTS index sync failed for {os.path.basename(video_path)}: {e}",
                "red")
            return _WorkerOutcome.FAILED

        return _WorkerOutcome.SUCCESS
