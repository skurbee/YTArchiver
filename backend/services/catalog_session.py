"""Connection admission and transaction ownership for catalog consumers.

The legacy index remains responsible for schema/connection construction.
Consumers receive this explicit owner instead of combining its private locks,
connections and timeout helpers. Factories are lazy and replaceable, so a
repository can be exercised with in-memory SQLite without loading app state.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any


class LibraryQueryTimeout(TimeoutError):
    """An interactive library operation ended without a complete result."""


class CatalogSession:
    def __init__(self, *, writer_factory: Callable, reader_factory: Callable,
                 independent_factory: Callable, writer_lock: Any, reader_lock: Any,
                 lock_seconds: Callable[[], float], query_seconds: Callable[[], float],
                 logger: logging.Logger | None = None):
        self._writer_factory = writer_factory
        self._reader_factory = reader_factory
        self._independent_factory = independent_factory
        self._writer_lock = writer_lock
        self._reader_lock = reader_lock
        self._lock_seconds = lock_seconds
        self._query_seconds = query_seconds
        self._log = logger or logging.getLogger(__name__)

    def initialize(self) -> bool:
        """Initialize the existing schema without transferring its connection."""
        return self._writer_factory() is not None

    def open_independent(self):
        """Transfer one independent connection to a caller that must close it."""
        return self._independent_factory()

    @contextmanager
    def writer(self):
        """Own shared-writer admission for a legacy maintenance batch.

        This does not commit surrounding work. New standalone writes should
        use transaction(), which owns BEGIN, commit and rollback explicitly.
        """
        with self._writer_lock:
            yield self._writer_factory()

    @contextmanager
    def maintenance_transaction(self, operation: str, *, independent: bool = False):
        """Own a complete background write with no foreground query deadline.

        Independent maintenance still takes writer admission and refuses an
        outstanding shared transaction. Its connection is always closed here.
        """
        with self._writer_lock:
            shared = self._writer_factory()
            if shared is None:
                raise RuntimeError("DB unavailable")
            if shared.in_transaction:
                raise LibraryQueryTimeout(
                    f"{operation} deferred because the shared index connection has an active transaction")
            conn = self._independent_factory() if independent else shared
            if conn is None:
                raise RuntimeError("DB unavailable")
            began = False
            try:
                conn.execute("BEGIN IMMEDIATE")
                began = True
                yield conn
                conn.commit()
            except BaseException:
                if began:
                    conn.rollback()
                raise
            finally:
                if independent:
                    conn.close()

    @contextmanager
    def reader(self, *, writer_fallback: bool = False):
        """Own a background read without imposing a foreground query deadline."""
        conn = self._reader_factory()
        lock = self._reader_lock
        if conn is None and writer_fallback:
            conn = self._writer_factory()
            lock = self._writer_lock
        with lock:
            yield conn

    @contextmanager
    def bounded_sql(self, conn, operation: str, seconds: float, *, check_complete: bool = True):
        deadline = time.monotonic() + seconds
        old_busy = conn.execute("PRAGMA busy_timeout").fetchone()[0]
        conn.execute(f"PRAGMA busy_timeout={max(1, min(500, int(seconds * 1000)))}")
        conn.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        try:
            yield conn
            if check_complete and time.monotonic() >= deadline:
                raise LibraryQueryTimeout(f"{operation} took too long. Please try again with a narrower selection.")
        except sqlite3.OperationalError as exc:
            if getattr(exc, "sqlite_errorcode", None) in (
                    sqlite3.SQLITE_INTERRUPT, sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                self._log.warning("%s stopped during SQL work: %s", operation, exc)
                raise LibraryQueryTimeout(
                    f"{operation} could not finish while the library was busy. Please try again.") from exc
            raise
        finally:
            conn.set_progress_handler(None, 0)
            conn.execute(f"PRAGMA busy_timeout={int(old_busy)}")

    @contextmanager
    def read(self, operation: str):
        conn = self._reader_factory()
        if conn is None:
            raise RuntimeError("The library index is unavailable. Please try again shortly.")
        if not self._reader_lock.acquire(timeout=self._lock_seconds()):
            self._log.warning("%s timed out waiting for the library reader", operation)
            raise LibraryQueryTimeout(f"{operation} is waiting for another library operation. Please try again.")
        try:
            with self.bounded_sql(conn, operation, self._query_seconds()):
                yield conn
        finally:
            self._reader_lock.release()

    @contextmanager
    def transaction(self, operation: str, *, seconds: float = 3.0):
        """Own one bounded standalone write, never another caller's transaction."""
        started = time.monotonic()
        if not self._writer_lock.acquire(timeout=self._lock_seconds()):
            self._log.warning("%s timed out acquiring the library writer", operation)
            raise LibraryQueryTimeout(f"{operation} could not start while the library was busy. Please try again.")
        conn = None
        began = False
        stage = "opening writer"
        try:
            conn = self._writer_factory()
            if conn is None:
                raise RuntimeError("The library index is unavailable. Please try again shortly.")
            if conn.in_transaction:
                raise LibraryQueryTimeout("A library update is still pending. Please try saving again shortly.")
            with self.bounded_sql(conn, operation, seconds, check_complete=False):
                stage = "starting transaction"
                conn.execute("BEGIN IMMEDIATE")
                began = True
                stage = "writing"
                yield conn
                stage = "committing"
                conn.commit()
            self._log.debug("%s committed in %.3fs", operation, time.monotonic() - started)
        except BaseException as exc:
            if began and conn is not None:
                conn.rollback()
            self._log.warning("%s failed at %s after %.3fs: %s", operation, stage,
                              time.monotonic() - started, exc)
            raise
        finally:
            self._writer_lock.release()
