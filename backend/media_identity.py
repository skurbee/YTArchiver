"""Logical identity policy shared by catalog projection and SQL readers.

Identity is an explicit YouTube ID, otherwise the SQLite-compatible path,
otherwise the legacy row ID. Titles never establish identity. Filesystem
containment uses a different policy: these keys must not resolve archive paths.
"""

from __future__ import annotations

import re
from typing import Any

_SQLITE_ASCII_LOWER = str.maketrans(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz",
)


def normalize_media_path(path: Any) -> str:
    """Keep SQLite's ASCII lower/space trimming and historical slash policy."""
    return str(path or "").strip(" ").replace("/", "\\").translate(_SQLITE_ASCII_LOWER)


def identity_key_for_row(row: dict[str, Any]) -> tuple[str, str]:
    """Return the normalized catalog's persisted identity and kind.

    The legacy-row prefix is part of the stored projection format. SQL reader
    aliases use row: for the same fallback partition; neither changes on disk.
    """
    video_id = str(row.get("video_id") or "").strip()
    if video_id:
        return f"id:{video_id}", "youtube"
    filepath = normalize_media_path(row.get("filepath"))
    if filepath:
        return f"path:{filepath}", "path"
    return f"legacy-row:{int(row['id'])}", "legacy"


def canonical_sort_key(row: dict[str, Any]) -> tuple[int, int, int]:
    """Prefer available media, then the primary hint, then stable row ID."""
    available = (
        str(row.get("availability") or "available") == "available"
        and bool(str(row.get("filepath") or "").strip())
    )
    return (0 if available else 1,
            0 if row.get("is_duplicate_of") is None else 1, int(row["id"]))


def _alias(alias: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", alias):
        raise ValueError("SQL table alias must be an identifier")
    return alias


def logical_key_sql(alias: str = "v") -> str:
    """Return the legacy-reader partition key using SQLite's native trimming."""
    v = _alias(alias)
    return (
        "CASE "
        f"WHEN trim(COALESCE({v}.video_id, '')) <> '' "
        f"THEN 'id:' || trim({v}.video_id) "
        f"WHEN trim(COALESCE({v}.filepath, '')) <> '' "
        f"THEN 'path:' || lower(replace(trim({v}.filepath), '/', char(92))) "
        f"ELSE 'row:' || CAST({v}.id AS TEXT) END"
    )


def available_copy_sql(alias: str = "v") -> str:
    v = _alias(alias)
    return (f"COALESCE({v}.availability, 'available') = 'available' "
            f"AND trim(COALESCE({v}.filepath, '')) <> ''")


def canonical_order_sql(alias: str = "v", *, available_only: bool = False) -> str:
    """Channel readers filter availability before ranking within the channel."""
    v = _alias(alias)
    preferred = f"CASE WHEN {v}.is_duplicate_of IS NULL THEN 0 ELSE 1 END, {v}.id"
    if available_only:
        return preferred
    return f"CASE WHEN {available_copy_sql(v)} THEN 0 ELSE 1 END, {preferred}"
