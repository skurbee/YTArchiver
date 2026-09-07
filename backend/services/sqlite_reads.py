"""Explicit SQLite read lifetimes: live WAL-aware transactions or snapshots."""

import sqlite3
from pathlib import Path


def open_readonly(path: str | Path, *, immutable_snapshot: bool = False,
                  timeout: float = 2.0) -> sqlite3.Connection:
    """Caller owns close. Live readers pin one coherent WAL-visible snapshot.

    Use immutable_snapshot only for a detached file that cannot be modified.
    Live transactions may retain WAL pages until closed; callers must bound
    their work and close on cancellation and failure.
    """
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    if immutable_snapshot:
        uri += "&immutable=1"
    connection = sqlite3.connect(uri, uri=True, timeout=timeout)
    try:
        # mode=ro forbids writes to the input database while still permitting
        # connection-local TEMP virtual tables used for FTS token inspection.
        connection.execute("BEGIN")
        # BEGIN is deferred. Fix the snapshot now rather than at an arbitrary
        # later query in the caller's multi-table inspection.
        connection.execute("SELECT count(*) FROM sqlite_master").fetchone()
        return connection
    except BaseException:
        connection.close()
        raise
