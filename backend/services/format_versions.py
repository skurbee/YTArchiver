"""Small, explicit admission checks for persisted format boundaries."""

import sqlite3


class UnsupportedFormatError(sqlite3.DatabaseError):
    """Preserve this resource until a compatible application can read it."""


def require_version(value, supported, resource: str) -> int:
    """Validate an actual format marker before any migration or replay."""
    try:
        version = int(value)
        valid = not isinstance(value, bool) and str(value) == str(version)
    except (TypeError, ValueError, OverflowError):
        valid = False
        version = -1
    if not valid or version not in supported:
        raise UnsupportedFormatError(
            f"{resource} has unsupported format version {value!r}. "
            "Its contents were preserved; use a compatible application version.")
    return version
