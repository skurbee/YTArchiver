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
import time
from collections import deque
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from queue import Empty, Full, Queue

from . import youtube_traffic
from .log import get_logger
from .subprocess_util import (
    make_startupinfo,
    subprocess_creationflags,
    utf8_env,
)
from .youtube_request_process import (
    attach_session,
    budget_wait_seconds,
    prepare_command,
    request_session,
    set_request_signals,
)

_log = get_logger(__name__)


_PROCESS_OWNER_CONTEXT = threading.local()


@contextmanager
def process_owner_scope(owner: str, task_id: str = ""):
    """Supply default ownership to launches on the current worker thread.

    This lets a top-level worker claim legacy helper launches without making
    ownership global or risking another thread's unrelated process. Explicit
    ``popen_ytdlp`` ownership still takes precedence.
    """
    previous = getattr(_PROCESS_OWNER_CONTEXT, "value", None)
    _PROCESS_OWNER_CONTEXT.value = (
        str(owner or "unowned"), str(task_id or ""))
    try:
        yield
    finally:
        if previous is None:
            try:
                del _PROCESS_OWNER_CONTEXT.value
            except AttributeError:
                pass
        else:
            _PROCESS_OWNER_CONTEXT.value = previous


class StreamingRunResult:
    """Backward-compatible result for YtDlpRunner.run_streaming."""

    __slots__ = ("returncode", "stderr_tail", "cancelled", "timed_out", "output_complete")

    def __init__(self, returncode: int, stderr_tail: list[str],
                 cancelled: bool = False, timed_out: bool = False,
                 output_complete: bool = True):
        self.returncode = returncode
        self.stderr_tail = stderr_tail
        self.cancelled = cancelled
        self.timed_out = timed_out
        self.output_complete = output_complete

    def __iter__(self):
        yield self.returncode
        yield self.stderr_tail

    def __len__(self):
        return 2

    def __getitem__(self, idx):
        return (self.returncode, self.stderr_tail)[idx]


# ── ProcessRegistry ───────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class ProcessRecord:
    """One app-owned process and the job that is allowed to stop it."""

    proc: subprocess.Popen
    owner: str
    task_id: str
    role: str
    pid: int | None
    create_time: float | None


class ProcessRegistry:
    """Tracks app-owned child processes without guessing from image names.

    Records carry an owner and stable task ID so a feature-level force stop
    can target only its own process trees.  ``kill_all`` remains as the
    backwards-compatible whole-app emergency operation.

    All waits in one terminate operation run concurrently against one
    monotonic deadline.  Ten stuck children therefore consume one timeout,
    not ten timeouts in sequence.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._records: list[ProcessRecord] = []

    @property
    def _procs(self) -> list[subprocess.Popen]:
        """Legacy diagnostic view retained for older callers/tests."""
        with self._lock:
            return [record.proc for record in self._records]

    @staticmethod
    def _is_alive(proc: subprocess.Popen) -> bool:
        try:
            return proc.poll() is None
        except Exception:
            # A broken diagnostic method must not make us forget a process
            # that may still be running.
            return True

    @staticmethod
    def _pid_for(proc: subprocess.Popen) -> int | None:
        try:
            pid = getattr(proc, "pid", None)
            if isinstance(pid, int) and not isinstance(pid, bool) and pid > 0:
                return pid
        except Exception:
            pass
        return None

    @staticmethod
    def _create_time_for(pid: int | None) -> float | None:
        if pid is None:
            return None
        try:
            import psutil
            return float(psutil.Process(pid).create_time())
        except Exception:
            return None

    @staticmethod
    def _matches(record: ProcessRecord, *, owner: str | None = None,
                 task_id: str | None = None,
                 role: str | None = None,
                 proc: subprocess.Popen | None = None) -> bool:
        if proc is not None and record.proc is not proc:
            return False
        if owner is not None and record.owner != str(owner):
            return False
        if task_id is not None and record.task_id != str(task_id):
            return False
        if role is not None and record.role != str(role):
            return False
        return True

    def _prune_dead_locked(self) -> int:
        before = len(self._records)
        self._records = [
            record for record in self._records
            if self._is_alive(record.proc)
        ]
        return before - len(self._records)

    def register(self, proc: subprocess.Popen, *, owner: str = "unowned",
                 task_id: str = "", role: str = "") -> subprocess.Popen:
        """Track ``proc`` and return it, preserving the old call shape.

        Re-registering the same Popen updates its metadata instead of adding a
        duplicate.  Calls that have not migrated yet are visibly labelled
        ``unowned`` rather than being silently attributed to another feature.
        """
        if proc is None:
            return proc
        scoped_owner, scoped_task = getattr(
            _PROCESS_OWNER_CONTEXT, "value", ("unowned", ""))
        owner_value = str(owner or "unowned")
        task_value = str(task_id or "")
        if owner_value == "unowned" and scoped_owner != "unowned":
            owner_value = str(scoped_owner)
            if not task_value:
                task_value = str(scoped_task or "")
        role_value = str(role or "")
        pid = self._pid_for(proc)
        record = ProcessRecord(
            proc=proc,
            owner=owner_value,
            task_id=task_value,
            role=role_value,
            pid=pid,
            create_time=self._create_time_for(pid),
        )
        with self._lock:
            self._prune_dead_locked()
            self._records = [
                existing for existing in self._records
                if existing.proc is not proc
            ]
            self._records.append(record)
        return proc

    def unregister(self, proc: subprocess.Popen) -> None:
        """Stop tracking `proc`. Call after wait()/poll() returns a
        non-None code, so the registry doesn't accumulate dead procs."""
        if proc is None:
            return
        with self._lock:
            self._records = [
                record for record in self._records
                if record.proc is not proc
            ]

    def reap_dead(self) -> int:
        """Drop already-exited procs from the registry. Returns count
        removed. Optional housekeeping — kill_all is safe regardless."""
        with self._lock:
            return self._prune_dead_locked()

    def snapshot(self, *, owner: str | None = None,
                 task_id: str | None = None,
                 role: str | None = None) -> list[ProcessRecord]:
        """Return a stable metadata snapshot of matching live processes."""
        with self._lock:
            self._prune_dead_locked()
            return [
                record for record in self._records
                if self._matches(
                    record, owner=owner, task_id=task_id, role=role)
            ]

    def alive_count(self, *, owner: str | None = None,
                    task_id: str | None = None,
                    role: str | None = None) -> int:
        """Return the number of matching live registered roots."""
        return len(self.snapshot(owner=owner, task_id=task_id, role=role))

    def _take(self, *, owner: str | None = None,
              task_id: str | None = None,
              role: str | None = None,
              proc: subprocess.Popen | None = None) -> list[ProcessRecord]:
        with self._lock:
            self._prune_dead_locked()
            selected: list[ProcessRecord] = []
            kept: list[ProcessRecord] = []
            for record in self._records:
                if self._matches(
                        record, owner=owner, task_id=task_id,
                        role=role, proc=proc):
                    selected.append(record)
                else:
                    kept.append(record)
            self._records = kept
            return selected

    def _take_many(
            self, *, owners: Iterable[str] | None = None,
            processes: Iterable[subprocess.Popen] | None = None,
    ) -> list[ProcessRecord]:
        owner_values = (
            {str(value) for value in owners} if owners is not None else None)
        process_ids = (
            {id(value) for value in processes}
            if processes is not None else None)
        with self._lock:
            self._prune_dead_locked()
            selected: list[ProcessRecord] = []
            kept: list[ProcessRecord] = []
            for record in self._records:
                owner_match = (
                    owner_values is not None and record.owner in owner_values)
                process_match = (
                    process_ids is not None and id(record.proc) in process_ids)
                if owner_match or process_match:
                    selected.append(record)
                else:
                    kept.append(record)
            self._records = kept
            return selected

    @staticmethod
    def _descendants(record: ProcessRecord) -> list[object]:
        """Return only descendants of this exact registered process."""
        if record.pid is None:
            return []
        try:
            import psutil
            root = psutil.Process(record.pid)
            if record.create_time is not None:
                # PIDs are reusable.  Refuse to walk a different process that
                # acquired this PID after the registered root exited.
                if abs(float(root.create_time()) - record.create_time) > 0.01:
                    return []
            return list(root.children(recursive=True))
        except Exception:
            return []

    @staticmethod
    def _psutil_alive(proc: object) -> bool:
        try:
            import psutil
            return bool(proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE)
        except Exception:
            return False

    @staticmethod
    def _start_root_waiters(records: list[ProcessRecord],
                            wait_timeout: float):
        """Wait for roots concurrently so total latency is one deadline."""
        waiters = []
        for record in records:
            done = threading.Event()
            stopped = threading.Event()

            def _wait(rec=record, done_event=done, stopped_event=stopped):
                try:
                    rec.proc.wait(timeout=wait_timeout)
                except Exception:
                    pass
                else:
                    stopped_event.set()
                finally:
                    done_event.set()

            thread = threading.Thread(
                target=_wait,
                name=f"process-wait-{record.pid or 'unknown'}",
                daemon=True,
            )
            thread.start()
            waiters.append((record, thread, done, stopped))
        return waiters

    @staticmethod
    def _join_waiters(waiters, deadline: float) -> None:
        for _record, thread, _done, _stopped in waiters:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            thread.join(timeout=remaining)

    def _restore_survivors(self, records: list[ProcessRecord]) -> None:
        if not records:
            return
        with self._lock:
            known = {id(record.proc) for record in self._records}
            for record in records:
                if id(record.proc) not in known:
                    self._records.append(record)
                    known.add(id(record.proc))

    def _terminate_records(self, records: list[ProcessRecord],
                           timeout: float) -> int:
        active = [record for record in records if self._is_alive(record.proc)]
        if not active:
            return 0

        budget = max(0.0, float(timeout))
        started = time.monotonic()
        deadline = started + budget
        grace_deadline = started + (budget * 0.7)

        # Capture each exact tree before stopping its root.  No image-name or
        # process-wide descendant scan is involved.
        trees = {id(record): self._descendants(record) for record in active}

        # Signal every tree before waiting for any one tree.  This is what
        # makes the timeout global rather than N times the supplied value.
        for record in active:
            try:
                record.proc.terminate()
            except Exception as exc:
                _log.debug("process terminate failed: %s", exc)
        for record in active:
            for child in reversed(trees[id(record)]):
                try:
                    child.terminate()
                except Exception as exc:
                    _log.debug("descendant terminate failed: %s", exc)

        waiters = self._start_root_waiters(active, budget)
        self._join_waiters(waiters, grace_deadline)

        # Escalate every remaining member together, then spend only the
        # remainder of the same global deadline reaping them.
        force_wait: list[ProcessRecord] = []
        waiter_by_record = {id(row[0]): row for row in waiters}
        for record in active:
            _rec, _thread, _done, stopped = waiter_by_record[id(record)]
            if stopped.is_set():
                continue
            if not self._is_alive(record.proc):
                stopped.set()
                continue
            try:
                record.proc.kill()
                force_wait.append(record)
            except Exception as exc:
                _log.debug("process kill failed: %s", exc)
        for record in active:
            for child in reversed(trees[id(record)]):
                if not self._psutil_alive(child):
                    continue
                try:
                    child.kill()
                except Exception as exc:
                    _log.debug("descendant kill failed: %s", exc)

        # A waiter that was still blocked in Popen.wait will wake after kill.
        self._join_waiters(waiters, deadline)
        # If its first wait already returned TimeoutExpired, reap it with a
        # second concurrent wait using only the remaining global budget.
        remaining = max(0.0, deadline - time.monotonic())
        retry_waiters = self._start_root_waiters(
            [record for record in force_wait
             if waiter_by_record[id(record)][2].is_set()
             and not waiter_by_record[id(record)][3].is_set()],
            remaining,
        )
        self._join_waiters(retry_waiters, deadline)
        retry_by_record = {id(row[0]): row for row in retry_waiters}

        # Give killed descendants the same remaining deadline, without ever
        # inspecting siblings or unrelated children.
        while time.monotonic() < deadline:
            if not any(
                    self._psutil_alive(child)
                    for children in trees.values() for child in children):
                break
            time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))

        survivors: list[ProcessRecord] = []
        for record in active:
            stopped = waiter_by_record[id(record)][3].is_set()
            retry = retry_by_record.get(id(record))
            if retry is not None:
                stopped = stopped or retry[3].is_set()
            if not stopped and self._is_alive(record.proc):
                survivors.append(record)
        self._restore_survivors(survivors)
        return len(active)

    def terminate_owner(self, owner: str, timeout: float = 5.0) -> int:
        """Stop every process owned by ``owner`` within one deadline."""
        return self._terminate_records(
            self._take(owner=str(owner)), timeout)

    def terminate_owners(self, owners: Iterable[str],
                         timeout: float = 5.0) -> int:
        """Stop an exact owner set within one shared deadline."""
        return self._terminate_records(
            self._take_many(owners=owners), timeout)

    def terminate_job(self, task_id: str, timeout: float = 5.0,
                      *, owner: str | None = None) -> int:
        """Stop one stable task's process roots, optionally under an owner."""
        return self._terminate_records(
            self._take(owner=owner, task_id=str(task_id)), timeout)

    def terminate_process(self, proc: subprocess.Popen,
                          timeout: float = 5.0) -> int:
        """Stop the exact registered root and its descendants."""
        return self._terminate_records(self._take(proc=proc), timeout)

    def terminate_processes(self, processes: Iterable[subprocess.Popen],
                            timeout: float = 5.0) -> int:
        """Stop exact registered roots together within one shared deadline."""
        return self._terminate_records(
            self._take_many(processes=processes), timeout)

    def kill_all(self, timeout: float = 5.0) -> int:
        """Whole-app emergency stop, bounded by one global deadline."""
        return self._terminate_records(self._take(), timeout)


# Module-level singleton — the rest of the codebase imports this.
PROCESS_REGISTRY = ProcessRegistry()


def stop_owned_process(proc, *, registry=None, timeout: float = 4.0) -> None:
    """Stop this exact child tree and reap it within one cleanup budget."""
    target_registry = registry or PROCESS_REGISTRY
    deadline = time.monotonic() + max(0.0, timeout)
    try:
        target_registry.terminate_process(proc, timeout=max(0.0, timeout / 2))
    except Exception as exc:
        _log.debug("owned process termination failed: %s", exc)
    try:
        if proc.poll() is not None:
            return
    except Exception:
        pass
    try:
        proc.terminate()
    except Exception as exc:
        _log.debug("process terminate fallback failed: %s", exc)
    try:
        proc.wait(timeout=max(0.0, (deadline - time.monotonic()) * 0.7))
        return
    except Exception:
        pass
    try:
        proc.kill()
    except Exception as exc:
        _log.debug("process kill fallback failed: %s", exc)
    try:
        proc.wait(timeout=max(0.0, deadline - time.monotonic()))
    except Exception as exc:
        _log.debug("process reap fallback failed: %s", exc)


def finish_owned_process(proc, *, registry=None, wait_timeout: float = 10.0,
                         stop_timeout: float = 7.0,
                         output_reader: ProcessOutputReader | None = None) -> int | None:
    """Finalize a child after its output reader has stopped.

    A surviving child remains registered for application shutdown. Callers
    with active readers must close those readers first; closing an inherited
    pipe while another thread is blocked in readline can itself block.
    """
    target_registry = registry or PROCESS_REGISTRY
    try:
        proc.wait(timeout=wait_timeout)
    except Exception:
        stop_owned_process(proc, registry=target_registry, timeout=stop_timeout)
    returncode = getattr(proc, "returncode", None)
    if output_reader is not None:
        output_reader.close()
    if returncode is not None:
        pipes = () if output_reader is not None else (
            getattr(proc, "stdout", None), getattr(proc, "stderr", None))
        for pipe in pipes:
            try:
                if pipe is not None:
                    pipe.close()
            except Exception as exc:
                _log.debug("process pipe close failed: %s", exc)
        target_registry.unregister(proc)
    return returncode


class ProcessOutputReader:
    """Bounded pipe draining shared by supervisors and binary parser adapters.

    Values retain their original str/bytes form. One consumer owns callbacks
    or parsing; readers only transfer lines. close() never waits on a held
    pipe's I/O lock and has one total join deadline for both readers.
    """

    def __init__(self, proc, *, task_id: str = ""):
        self._proc = proc
        self._queue: Queue = Queue(maxsize=512)
        self._stop = threading.Event()
        self._closed = False
        self.failed = threading.Event()
        self._done = {name: threading.Event() for name in ("stdout", "stderr")}
        self._threads = [threading.Thread(
            target=self._drain, args=(getattr(proc, name, None), name),
            daemon=True, name=f"process-{name}-{task_id or 'unknown'}",
        ) for name in self._done]

    def start(self):
        for thread in self._threads:
            thread.start()
        return self

    def _drain(self, pipe, channel: str) -> None:
        try:
            if pipe is None:
                return
            readline = getattr(pipe, "readline", None)
            iterator = None if callable(readline) else iter(pipe)
            while not self._stop.is_set():
                if callable(readline):
                    line = readline()
                    if not line:
                        break
                else:
                    try:
                        line = next(iterator)
                    except StopIteration:
                        break
                while not self._stop.is_set():
                    try:
                        self._queue.put((channel, line), timeout=0.05)
                        break
                    except Full:
                        continue
        except Exception as exc:
            self.failed.set()
            _log.debug("stream reader stopped: %s", exc)
        finally:
            self._done[channel].set()

    def read(self, timeout: float = 0.1):
        try:
            return self._queue.get(timeout=timeout)
        except Empty:
            return None

    @property
    def finished(self) -> bool:
        return all(event.is_set() for event in self._done.values()) and self._queue.empty()

    def pending(self):
        while True:
            try:
                yield self._queue.get_nowait()
            except Empty:
                return

    def close(self, timeout: float = 1.0) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        deadline = time.monotonic() + max(0.0, timeout)
        for thread in self._threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        for channel, done in self._done.items():
            if not done.is_set():
                continue
            try:
                pipe = getattr(self._proc, channel, None)
                if pipe is not None:
                    pipe.close()
            except Exception as exc:
                _log.debug("stream pipe close failed: %s", exc)


class CancellationSignals:
    """Read-only union of caller cancellation and operation-local stop signals."""

    def __init__(self, *events):
        self._events = tuple(event for event in events if event is not None)

    def is_set(self) -> bool:
        return any(event.is_set() for event in self._events)


def supervise_streaming_process(
        proc: subprocess.Popen, *,
        registry: ProcessRegistry | None = None,
        on_stdout_line: Callable[[str], None] | None = None,
        on_stderr_line: Callable[[str], None] | None = None,
        cancel_event: threading.Event | None = None,
        pause_event: threading.Event | None = None,
        on_pause_change: Callable[[bool], None] | None = None,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        exit_timeout: float = 10.0,
        owner: str = "unowned",
        task_id: str = "",
        role: str = "streaming",
) -> StreamingRunResult:
    """Supervise an already-launched streaming child without blocking on I/O.

    This is the process-agnostic half of :meth:`YtDlpRunner.run_streaming`.
    It is also used by installers and the yt-dlp self-updater, which cannot
    launch through the ordinary yt-dlp gate. Both pipes are drained through a
    bounded queue while timeout/cancel checks run on the caller thread.
    """
    target_registry = registry or PROCESS_REGISTRY
    try:
        target_registry.register(
            proc, owner=owner, task_id=task_id, role=role)
    except TypeError:
        # Preserve compatibility with small embedders implementing the old
        # one-argument registry protocol.
        target_registry.register(proc)

    set_request_signals(proc, cancel_event, pause_event)
    budget_accounted = budget_wait_seconds(proc)
    stderr_tail: deque = deque(maxlen=200)
    cancelled = False
    timed_out = False
    output_reader = ProcessOutputReader(proc, task_id=task_id).start()
    output_complete = False
    callback_failed = False
    paused_at: float | None = None

    started = time.monotonic()
    last_activity = started
    operation_deadline = (
        None if timeout is None
        else started + max(0.0, float(timeout))
    )
    post_exit_deadline: float | None = None
    closed_pipes_deadline: float | None = None

    def _call_line(callback, line: str) -> None:
        nonlocal callback_failed
        if callback is None:
            return
        try:
            callback(line.rstrip("\n"))
        except Exception as exc:
            callback_failed = True
            _log.debug("stream callback failed: %s", exc)

    def _consume(channel: str, line: str, *, callbacks: bool = True) -> None:
        nonlocal last_activity, post_exit_deadline
        last_activity = time.monotonic()
        if channel == "stderr":
            stderr_tail.append(line.rstrip())
            if callbacks:
                _call_line(on_stderr_line, line)
        elif callbacks:
            _call_line(on_stdout_line, line)
        if post_exit_deadline is not None:
            # A busy final backlog is still useful output. Bound silence
            # from inherited pipe handles, rather than callback/drain time.
            post_exit_deadline = time.monotonic() + 1.0

    def _pause_changed(paused: bool) -> None:
        if on_pause_change is not None:
            try:
                on_pause_change(paused)
            except Exception as exc:
                _log.debug("process pause callback failed: %s", exc)

    def _poll_process():
        try:
            return proc.poll()
        except Exception:
            # Tiny test doubles and a few legacy embedders expose only the
            # Popen-compatible ``returncode`` attribute. A non-None value is
            # just as authoritative as poll() for an already-finished child.
            return getattr(proc, "returncode", None)

    try:
        while True:
            now = time.monotonic()
            budget_now = budget_wait_seconds(proc)
            budget_elapsed = max(0.0, budget_now - budget_accounted)
            budget_accounted = budget_now
            # Manual pause below already excludes its entire duration. Avoid
            # counting the overlapping request wait twice on resume.
            if paused_at is None:
                last_activity += budget_elapsed
                if operation_deadline is not None:
                    operation_deadline += budget_elapsed
            if cancel_event is not None and cancel_event.is_set():
                cancelled = True
                stop_owned_process(proc, registry=target_registry)
                break
            if pause_event is not None and pause_event.is_set():
                if paused_at is None:
                    paused_at = now
                    _pause_changed(True)
                time.sleep(0.1)
                continue
            if paused_at is not None:
                pause_duration = now - paused_at
                last_activity += pause_duration
                if operation_deadline is not None:
                    operation_deadline += pause_duration
                if post_exit_deadline is not None:
                    post_exit_deadline += pause_duration
                if closed_pipes_deadline is not None:
                    closed_pipes_deadline += pause_duration
                paused_at = None
                _pause_changed(False)
            returncode = _poll_process()
            if ((operation_deadline is not None and now >= operation_deadline)
                    or (returncode is None and idle_timeout is not None
                        and now - last_activity >= idle_timeout)):
                timed_out = True
                stop_owned_process(proc, registry=target_registry)
                break
            if returncode is not None:
                if post_exit_deadline is None:
                    post_exit_deadline = now + 1.0
                if output_reader.finished:
                    break
                if now >= post_exit_deadline:
                    break
            elif output_reader.finished:
                if closed_pipes_deadline is None:
                    closed_pipes_deadline = now + max(0.0, exit_timeout)
                elif now >= closed_pipes_deadline:
                    timed_out = True
                    stop_owned_process(proc, registry=target_registry)
                    break

            poll_slice = 0.1
            deadlines = [deadline for deadline in (
                operation_deadline, post_exit_deadline,
                closed_pipes_deadline) if deadline is not None]
            if deadlines:
                poll_slice = min(
                    poll_slice,
                    max(0.001, min(deadlines) - time.monotonic()),
                )
            item = output_reader.read(timeout=poll_slice)
            if item is not None:
                _consume(*item)
    finally:
        output_complete = (output_reader.finished and not output_reader.failed.is_set()
                           and not callback_failed
                           and not (cancelled or timed_out))
        output_reader.close()
        if paused_at is not None:
            _pause_changed(False)
        for channel, line in output_reader.pending():
            _consume(channel, line, callbacks=not (cancelled or timed_out))
        output_complete = output_complete and not callback_failed
        still_alive = _poll_process() is None
        if still_alive:
            try:
                target_registry.register(
                    proc, owner=owner, task_id=task_id, role=role)
            except TypeError:
                target_registry.register(proc)
            except Exception as exc:
                _log.debug("survivor re-registration failed: %s", exc)
        else:
            target_registry.unregister(proc)

    returncode = getattr(proc, "returncode", None)
    return StreamingRunResult(
        returncode if returncode is not None else -1,
        list(stderr_tail),
        cancelled=cancelled,
        timed_out=timed_out,
        output_complete=output_complete,
    )


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


def popen_ytdlp(*args, registry: ProcessRegistry | None = None,
                owner: str | None = None, task_id: str = "",
                role: str = "yt-dlp", request_cancel_event=None,
                request_pause_event=None, **kwargs):
    """Launch and register yt-dlp as one update-gate transaction.

    Ownership keywords are consumed here and never forwarded to ``Popen``.
    Existing callers remain source-compatible while migrated callers can be
    stopped by exact owner or stable task ID.
    """
    target_registry = registry or PROCESS_REGISTRY
    scoped_owner, scoped_task_id = getattr(
        _PROCESS_OWNER_CONTEXT, "value", ("yt-dlp", ""))
    effective_owner = str(owner) if owner is not None else scoped_owner
    effective_task_id = str(task_id or scoped_task_id)
    command = args[0] if args else kwargs.pop("args")
    session = None
    proc = None
    try:
        with YTDLP_UPDATE_GATE.launch_slot():
            # Create authorization after any updater wait, so the unbound
            # session cannot expire while another process updates yt-dlp.
            command, child_env, session = prepare_command(command, kwargs.get("env"))
            if session is not None:
                kwargs["env"] = child_env
                session.set_signals(cancel_event=request_cancel_event,
                                    pause_event=request_pause_event)
            proc = subprocess.Popen(command, *args[1:], **kwargs)
            attach_session(proc, session)
            target_registry.register(
                proc, owner=effective_owner, task_id=effective_task_id, role=role)
    except BaseException:
        if session is not None:
            session.close()
        if proc is not None:
            stop_owned_process(proc, registry=target_registry)
        raise
    return proc


def run_ytdlp(*args, input=None, capture_output=False, timeout=None,
              wall_timeout=None, check=False, **kwargs):
    """Capture a registered probe, optionally bounding traffic waits too.

    ``timeout`` excludes intentional request-budget waits. ``wall_timeout``
    additionally limits total capture time after launch, including those waits.
    """
    if input is not None:
        if kwargs.get("stdin") is not None:
            raise ValueError("stdin and input arguments may not both be used")
        kwargs["stdin"] = subprocess.PIPE
    if capture_output:
        if kwargs.get("stdout") is not None or kwargs.get("stderr") is not None:
            raise ValueError("stdout/stderr cannot be used with capture_output")
        kwargs.update(stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc = popen_ytdlp(*args, **kwargs)
    try:
        capture_options = {"input": input, "timeout": timeout}
        if wall_timeout is not None:
            if request_session(proc) is not None:
                capture_options["wall_timeout"] = wall_timeout
            else:
                capture_options["timeout"] = (min(timeout, wall_timeout)
                    if timeout is not None else wall_timeout)
        stdout, stderr = proc.communicate(**capture_options)
        returncode = proc.poll()
        if check and returncode:
            raise subprocess.CalledProcessError(returncode, proc.args, stdout, stderr)
        return subprocess.CompletedProcess(proc.args, returncode, stdout, stderr)
    except BaseException:
        stop_owned_process(proc)
        raise
    finally:
        session = request_session(proc)
        if session is not None:
            session.close()
        PROCESS_REGISTRY.unregister(proc)
        for pipe in (proc.stdin, proc.stdout, proc.stderr):
            if pipe is not None:
                pipe.close()


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
                      on_stderr_line: Callable[[str], None] | None = None,
                      cancel_event: threading.Event | None = None,
                      timeout: float | None = None,
                      owner: str = "yt-dlp",
                      task_id: str = "",
                      extra_env: dict | None = None,
                      traffic_kind: str = "yt_dlp_download",
                      ) -> StreamingRunResult:
        """Run yt-dlp and stream stdout line by line via `on_stdout_line`.

        Used for long-running passes (channel sync) where the caller
        wants to react to each progress line as it arrives. Timeout and
        cancellation are monitored independently of child output, so a silent
        process cannot hide from either one.

        Reader threads drain both pipes into a bounded queue. The calling
        thread remains the supervisor and owns callbacks, timeout checks, and
        exact-tree termination. Stderr's last 200 lines are retained.

        Returns a StreamingRunResult. It still unpacks as
        (returncode, stderr_tail), and exposes `.cancelled` so callers can
        distinguish user cancellation from a launch/process failure.
        `.timed_out` reports the separate timeout outcome.
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
                owner=owner,
                task_id=task_id,
                role="streaming",
            )
        except OSError as e:
            return StreamingRunResult(-1, [f"launch failed: {e}"])
        result = supervise_streaming_process(
            proc,
            registry=self._registry,
            on_stdout_line=on_stdout_line,
            on_stderr_line=on_stderr_line,
            cancel_event=cancel_event,
            timeout=timeout,
            owner=owner,
            task_id=task_id,
            role="streaming",
        )
        try:
            from .youtube_session import handle_youtube_failure_text
            handle_youtube_failure_text(
                "\n".join(result.stderr_tail), context="running yt-dlp")
        except Exception as e:
            _log.debug("YtDlpRunner streaming session guard failed: %s", e)
        return result


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
                      *, timeout: float = 20.0,
                      owner: str = "ffprobe",
                      task_id: str = "",
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
        self._registry.register(
            proc, owner=owner, task_id=task_id, role="probe")
        try:
            try:
                out, err = proc.communicate(timeout=timeout)
            except subprocess.TimeoutExpired as exc:
                out = exc.output or ""
                try:
                    self._registry.terminate_process(proc, timeout=2.0)
                except Exception as stop_exc:
                    _log.debug("ffprobe tree stop failed: %s", stop_exc)
                    try:
                        proc.kill()
                    except Exception:
                        pass
                # Draining after kill must itself be bounded. A failed kill or
                # inherited pipe handle previously made this communicate wait
                # forever.
                try:
                    drained_out, _drained_err = proc.communicate(timeout=1.0)
                    if drained_out:
                        out = drained_out
                except Exception as drain_exc:
                    _log.debug("ffprobe post-kill drain failed: %s", drain_exc)
                return -1, out or "", "timeout"
        finally:
            self._registry.unregister(proc)
        return proc.returncode, out or "", err or ""
