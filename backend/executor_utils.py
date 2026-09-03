"""Small, explicit helpers for cancellation-safe thread pools.

``ThreadPoolExecutor`` context managers always call ``shutdown(wait=True)``
on exit.  That is a good default for short, non-cancellable work, but it can
turn a Cancel button into a many-minute wait when a worker is blocked on an
external process or a pooled drive.  The helpers here keep submission bounded
and make the success and cancellation shutdown paths explicit.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from threading import Event
from time import monotonic
from typing import Any


@dataclass(frozen=True)
class ExecutorRun:
    """Summary returned by :func:`run_bounded`.

    ``unfinished`` counts already-running workers that did not cooperate
    within the optional cancellation grace period.  Queued work is cancelled
    before this value is measured.
    """

    cancelled: bool
    completed: int
    unfinished: int


@dataclass(frozen=True)
class WorkResult[ItemT, ResultT]:
    item: ItemT
    value: ResultT | None
    error: BaseException | None


class LinkedCancelEvent:
    """A locally-settable cancellation token linked to a parent event."""

    def __init__(self, parent: Event | None = None) -> None:
        self._parent = parent
        self._local = Event()

    def set(self) -> None:
        self._local.set()

    def is_set(self) -> bool:
        return self._local.is_set() or bool(
            self._parent is not None and self._parent.is_set())

    def wait(self, timeout: float | None = None) -> bool:
        if self.is_set():
            return True
        deadline = None if timeout is None else monotonic() + max(0.0, timeout)
        while not self.is_set():
            if deadline is None:
                interval = 0.05
            else:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    return self.is_set()
                interval = min(0.05, remaining)
            self._local.wait(interval)
        return True


def cancel_executor(
    executor: ThreadPoolExecutor,
    futures: Iterable[Future[Any]],
    *,
    grace_seconds: float = 0.0,
) -> int:
    """Cancel queued work and return without an unbounded executor join.

    Already-running Python calls cannot be force-stopped safely.  They receive
    at most ``grace_seconds`` to notice their operation's cancellation token.
    The caller must keep durable writes behind its own cancellation check.
    """

    snapshot = tuple(futures)
    for future in snapshot:
        future.cancel()
    executor.shutdown(wait=False, cancel_futures=True)
    running = tuple(future for future in snapshot if not future.done())
    if running and grace_seconds > 0:
        _done, not_done = wait(running, timeout=max(0.0, grace_seconds))
        return len(not_done)
    return len(running)


def drain_executor(
    executor: ThreadPoolExecutor,
    futures: Iterable[Future[Any]],
    *,
    is_cancelled: Callable[[], bool] | None = None,
    poll_seconds: float = 0.05,
    cancel_grace_seconds: float = 0.0,
) -> ExecutorRun:
    """Drain submitted work while retaining a responsive cancel path."""

    snapshot = tuple(futures)
    executor.shutdown(wait=False, cancel_futures=False)
    pending = {future for future in snapshot if not future.done()}
    while pending:
        if is_cancelled is not None and is_cancelled():
            unfinished = cancel_executor(
                executor,
                snapshot,
                grace_seconds=cancel_grace_seconds,
            )
            completed = sum(future.done() and not future.cancelled()
                            for future in snapshot)
            return ExecutorRun(True, completed, unfinished)
        _done, pending = wait(
            pending,
            timeout=max(0.01, poll_seconds),
            return_when=FIRST_COMPLETED,
        )
    # Every tracked task is complete, so this final join cannot inherit a
    # stuck user operation; it only lets the executor retire its idle thread.
    executor.shutdown(wait=True, cancel_futures=False)
    completed = sum(not future.cancelled() for future in snapshot)
    return ExecutorRun(False, completed, 0)


def run_bounded[ItemT, ResultT](
    items: Iterable[ItemT],
    worker: Callable[[ItemT], ResultT],
    on_complete: Callable[[WorkResult[ItemT, ResultT]], None],
    *,
    max_workers: int,
    thread_name_prefix: str,
    is_cancelled: Callable[[], bool] | None = None,
    max_in_flight: int | None = None,
    poll_seconds: float = 0.05,
    cancel_grace_seconds: float = 0.0,
) -> ExecutorRun:
    """Run work with a bounded executor queue and cancellation polling.

    Results are delivered on the calling thread, so workers can be kept free
    of durable writes.  By default only ``max_workers`` futures exist at once;
    therefore a large input cannot fill the executor with thousands of jobs
    that continue launching after cancellation.
    """

    workers = max(1, int(max_workers))
    limit = max(workers, int(max_in_flight or workers))
    source = iter(items)
    executor = ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix=thread_name_prefix,
    )
    pending: dict[Future[ResultT], ItemT] = {}
    all_futures: list[Future[ResultT]] = []
    completed = 0
    exhausted = False

    def _cancelled() -> bool:
        try:
            return bool(is_cancelled and is_cancelled())
        except Exception:
            # A broken cancellation source should fail closed: starting more
            # external work is less safe than returning a partial result.
            return True

    def _fill() -> None:
        nonlocal exhausted
        while not exhausted and len(pending) < limit and not _cancelled():
            try:
                item = next(source)
            except StopIteration:
                exhausted = True
                return
            future = executor.submit(worker, item)
            pending[future] = item
            all_futures.append(future)

    cancelled = False
    try:
        _fill()
        while pending:
            if _cancelled():
                cancelled = True
                break
            done, _not_done = wait(
                tuple(pending),
                timeout=max(0.01, poll_seconds),
                return_when=FIRST_COMPLETED,
            )
            if not done:
                continue
            for future in done:
                item = pending.pop(future)
                if _cancelled():
                    cancelled = True
                    break
                try:
                    result = WorkResult(item, future.result(), None)
                except BaseException as error:
                    result = WorkResult(item, None, error)
                on_complete(result)
                completed += 1
            if cancelled:
                break
            _fill()

        if _cancelled():
            cancelled = True
        if cancelled:
            unfinished = cancel_executor(
                executor,
                all_futures,
                grace_seconds=cancel_grace_seconds,
            )
        else:
            executor.shutdown(wait=True, cancel_futures=False)
            unfinished = 0
        return ExecutorRun(cancelled, completed, unfinished)
    except BaseException:
        cancel_executor(executor, all_futures)
        raise
