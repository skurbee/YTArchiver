"""Preview-only cross-store integrity scanner.

The scanner deliberately does not import YTArchiver's configured paths.  A
caller must supply the archive root, config file, index database, and queue
file it wants inspected.  Companion files may also be supplied explicitly;
otherwise their names are resolved relative to the explicit config/queue
paths.  SQLite is opened with ``mode=ro&immutable=1`` and every other input is
opened for reading only.

Results are proposals, not commands.  This module contains no repair path.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

_MEDIA_EXTENSIONS = frozenset({
    ".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v", ".flv", ".wmv",
})
_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
_BRACKET_ID_RE = re.compile(r"\[([A-Za-z0-9_-]{11})\](?:\.[^.]+)?$")
_TXT_HEADER_RE = re.compile(
    r"^===\((.+?)\),\s*\([^)]*\),\s*\([^)]*\),\s*\([^)]*\)"
    r"(?:,\s*\(youtu\.be/([A-Za-z0-9_-]{11})\))?==="
)
_EXPECTED_DB_USER_VERSION = 5
_EXPECTED_CATALOG_SCHEMA_VERSION = 1
_EXPECTED_QUEUE_SCHEMA_VERSION = 3
_EXPECTED_RESUMING_SCHEMA_VERSION = 2


def _path(value: str | os.PathLike[str] | Path) -> Path:
    return Path(value).expanduser().resolve(strict=False)


def _norm_text(value: Any) -> str:
    return " ".join(str(value or "").casefold().split())


def _norm_path(value: Any) -> str:
    return os.path.normcase(os.path.normpath(str(value or "").strip()))


def _valid_video_id(value: Any) -> str:
    value = str(value or "").strip()
    return value if _VIDEO_ID_RE.fullmatch(value) else ""


def _record_identity(video_id: Any, channel: Any, title: Any) -> str:
    if video_id := _valid_video_id(video_id):
        return f"id:{video_id}"
    return f"title:{_norm_text(channel)}\0{_norm_text(title)}"


def _display_identity(identity: str) -> str:
    if identity.startswith("title:"):
        channel, _, title = identity[6:].partition("\0")
        return f"{channel} / {title}".strip(" /")
    return identity[3:] if identity.startswith("id:") else identity


def _read_json(path: Path) -> tuple[Any, str]:
    if not path.is_file():
        return None, "missing"
    try:
        with path.open("r", encoding="utf-8") as stream:
            return json.load(stream), ""
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return None, str(exc)


def _read_jsonl(path: Path) -> tuple[list[Any], list[str]]:
    rows: list[Any] = []
    errors: list[str] = []
    if not path.is_file():
        return rows, errors
    try:
        with path.open("r", encoding="utf-8") as stream:
            for line_no, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    rows.append(json.loads(line))
                except (TypeError, ValueError) as exc:
                    errors.append(f"line {line_no}: {exc}")
    except (OSError, UnicodeError) as exc:
        errors.append(str(exc))
    return rows, errors


def _open_database_read_only(path: Path) -> sqlite3.Connection:
    """Open one immutable snapshot without journal/WAL side effects."""
    uri = f"{path.as_uri()}?mode=ro&immutable=1"
    return sqlite3.connect(uri, uri=True)


def _table_exists(connection: sqlite3.Connection, name: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?",
        (name,),
    ).fetchone() is not None


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(connection, table):
        return set()
    safe = table.replace('"', '""')
    return {str(row[1]) for row in connection.execute(
        f'PRAGMA table_info("{safe}")')}


def _token_part(term: Any, rowid: Any, offset: Any) -> int:
    payload = json.dumps(
        [str(term), int(rowid), int(offset)], ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return int.from_bytes(hashlib.blake2b(payload, digest_size=16).digest(), "big")


def _fts_token_signatures(
    connection: sqlite3.Connection, source: str, source_column: str, fts: str,
    *, batch_size: int = 5_000,
) -> tuple[tuple[int, int], tuple[int, int]]:
    """Return exact, order-independent token-instance signatures.

    FTS5's official integrity command is expressed as an INSERT and cannot be
    used on an immutable connection.  ``fts5vocab`` is instead attached in
    TEMP (never the source file), and a bounded in-memory FTS table tokenizes
    source rows in batches.  The count plus a 128-bit modular checksum covers
    term, source rowid, and token offset, including the stale-token/rowid-reuse
    case that a document-count comparison cannot see.
    """
    vocab_name = f"integrity_vocab_{fts}"
    connection.execute(
        f'CREATE VIRTUAL TABLE temp."{vocab_name}" '
        f'USING fts5vocab(main, "{fts}", instance)')
    actual_count = 0
    actual_sum = 0
    modulus = 1 << 128
    for term, doc, _column, offset in connection.execute(
            f'SELECT term,doc,col,offset FROM temp."{vocab_name}"'):
        actual_count += 1
        actual_sum = (actual_sum + _token_part(term, doc, offset)) % modulus

    tokenizer = sqlite3.connect(":memory:")
    expected_count = 0
    expected_sum = 0
    try:
        tokenizer.execute("CREATE VIRTUAL TABLE expected USING fts5(value)")
        tokenizer.execute(
            "CREATE VIRTUAL TABLE expected_vocab USING "
            "fts5vocab(expected, instance)")
        cursor = connection.execute(
            f'SELECT rowid,"{source_column}" FROM "{source}" ORDER BY rowid')
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            tokenizer.executemany(
                "INSERT INTO expected(rowid,value) VALUES(?,?)",
                ((int(rowid), str(value or "")) for rowid, value in rows),
            )
            for term, doc, _column, offset in tokenizer.execute(
                    "SELECT term,doc,col,offset FROM expected_vocab"):
                expected_count += 1
                expected_sum = (
                    expected_sum + _token_part(term, doc, offset)) % modulus
            tokenizer.execute("DELETE FROM expected")
    finally:
        tokenizer.close()
    return (actual_count, actual_sum), (expected_count, expected_sum)


def _add_issue(
    issues: list[dict[str, Any]], category: str, code: str, subject: str,
    detail: str, proposed_repair: str, *, severity: str = "warning",
    evidence: dict[str, Any] | None = None,
) -> None:
    row: dict[str, Any] = {
        "category": category,
        "code": code,
        "severity": severity,
        "subject": str(subject),
        "detail": str(detail),
        "proposed_repair": str(proposed_repair),
    }
    if evidence:
        row["evidence"] = evidence
    issues.append(row)


def _walk_archive(
    archive_path: Path, channel_aliases: dict[str, str] | None = None,
) -> tuple[
    set[str], dict[str, list[dict[str, str]]], dict[str, set[str]], list[str]
]:
    media_paths: set[str] = set()
    transcript_sources: dict[str, list[dict[str, str]]] = {
        "txt": [], "jsonl": [],
    }
    ids_by_path: dict[str, set[str]] = defaultdict(set)
    errors: list[str] = []
    if not archive_path.is_dir():
        return media_paths, transcript_sources, ids_by_path, errors

    try:
        iterator = os.walk(archive_path, followlinks=False)
        for dirpath, _dirs, filenames in iterator:
            folder = Path(dirpath)
            folder_channel = folder.relative_to(archive_path).parts[0] \
                if folder != archive_path else ""
            channel = (channel_aliases or {}).get(
                _norm_text(folder_channel), folder_channel)
            for filename in filenames:
                file_path = folder / filename
                suffix = file_path.suffix.casefold()
                if suffix in _MEDIA_EXTENSIONS:
                    normalized = _norm_path(file_path)
                    media_paths.add(normalized)
                    match = _BRACKET_ID_RE.search(filename)
                    if match:
                        ids_by_path[normalized].add(match.group(1))
                    continue
                if filename.endswith("Transcript.txt"):
                    try:
                        with file_path.open("r", encoding="utf-8") as stream:
                            for line_no, line in enumerate(stream, 1):
                                match = _TXT_HEADER_RE.match(line.rstrip("\r\n"))
                                if match:
                                    transcript_sources["txt"].append({
                                        "identity": _record_identity(
                                            match.group(2), channel, match.group(1)),
                                        "video_id": _valid_video_id(match.group(2)),
                                        "channel": channel,
                                        "title": match.group(1).strip(),
                                        "path": str(file_path),
                                        "line": str(line_no),
                                    })
                    except (OSError, UnicodeError) as exc:
                        errors.append(f"{file_path}: {exc}")
                    continue
                if not filename.endswith("Transcript.jsonl"):
                    continue
                seen: set[str] = set()
                try:
                    with file_path.open("r", encoding="utf-8") as stream:
                        for line_no, line in enumerate(stream, 1):
                            if not line.strip():
                                continue
                            try:
                                value = json.loads(line)
                            except (TypeError, ValueError) as exc:
                                errors.append(f"{file_path}:{line_no}: {exc}")
                                continue
                            if not isinstance(value, dict):
                                continue
                            title = str(value.get("title") or "").strip()
                            if not title:
                                continue
                            video_id = _valid_video_id(value.get("video_id"))
                            identity = _record_identity(video_id, channel, title)
                            if identity in seen:
                                continue
                            seen.add(identity)
                            transcript_sources["jsonl"].append({
                                "identity": identity,
                                "video_id": video_id,
                                "channel": channel,
                                "title": title,
                                "path": str(file_path),
                                "line": str(line_no),
                            })
                except (OSError, UnicodeError) as exc:
                    errors.append(f"{file_path}: {exc}")
    except OSError as exc:
        errors.append(f"{archive_path}: {exc}")
    return media_paths, transcript_sources, ids_by_path, errors


def _fts_integrity(
    connection: sqlite3.Connection, issues: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Compare source rowids with persisted FTS document-size rowids.

    Reading an external-content FTS table directly can conceal a stale shadow
    because returned columns come from the source table.  The ``*_docsize``
    shadow is read directly instead.  This detects missing/extra indexed
    documents without issuing FTS5's write-shaped ``integrity-check`` command.
    """
    checks = (
        ("transcript_fts", "segments", "text", "segments_fts"),
        ("title_fts", "videos", "title", "videos_fts"),
        ("title_fts", "logical_videos", "title", "logical_videos_fts"),
    )
    result: dict[str, dict[str, Any]] = {}
    for category, source, source_column, fts in checks:
        if not _table_exists(connection, source):
            continue
        if not _table_exists(connection, fts):
            _add_issue(
                issues, category, "fts_table_missing", fts,
                f"{fts} is missing while {source} exists.",
                f"Create and rebuild {fts} from {source} in a maintenance transaction.",
                severity="error",
            )
            result[fts] = {"ok": False, "source_rows": None, "indexed_rows": None}
            continue
        docsize = f"{fts}_docsize"
        if not _table_exists(connection, docsize):
            _add_issue(
                issues, category, "fts_shadow_unverifiable", fts,
                f"{docsize} is unavailable, so persisted FTS rowids cannot be compared.",
                f"Run the authoritative {fts} integrity check and rebuild if it fails.",
            )
            result[fts] = {"ok": False, "source_rows": None, "indexed_rows": None}
            continue
        source_ids = {int(row[0]) for row in connection.execute(
            f'SELECT rowid FROM "{source}"')}
        indexed_ids = {int(row[0]) for row in connection.execute(
            f'SELECT id FROM "{docsize}"')}
        missing = sorted(source_ids - indexed_ids)
        extra = sorted(indexed_ids - source_ids)
        token_error = ""
        actual_tokens: tuple[int, int] | None = None
        expected_tokens: tuple[int, int] | None = None
        if not missing and not extra:
            try:
                actual_tokens, expected_tokens = _fts_token_signatures(
                    connection, source, source_column, fts)
            except sqlite3.Error as exc:
                token_error = str(exc)
        token_drift = (
            actual_tokens is not None and expected_tokens is not None
            and actual_tokens != expected_tokens
        )
        healthy = not missing and not extra and not token_drift and not token_error
        result[fts] = {
            "ok": healthy,
            "source_rows": len(source_ids),
            "indexed_rows": len(indexed_ids),
            "missing_rowids": missing[:100],
            "extra_rowids": extra[:100],
            "truncated": len(missing) > 100 or len(extra) > 100,
            "actual_token_instances": actual_tokens[0] if actual_tokens else None,
            "expected_token_instances": expected_tokens[0] if expected_tokens else None,
            "token_check_error": token_error,
        }
        if missing or extra:
            _add_issue(
                issues, category, "fts_rowid_drift", fts,
                f"{len(missing)} source row(s) are absent and {len(extra)} stale row(s) remain.",
                f"Atomically rebuild and verify {fts} from {source}.",
                severity="error",
                evidence={"missing_rowids": missing[:20], "extra_rowids": extra[:20]},
            )
        elif token_drift:
            _add_issue(
                issues, category, "fts_token_drift", fts,
                f"The source has {expected_tokens[0]} token instances but the FTS shadow has {actual_tokens[0]}, or their term/rowid/offset signatures differ.",
                f"Atomically rebuild and verify {fts} from {source}.",
                severity="error",
            )
        elif token_error:
            _add_issue(
                issues, category, "fts_token_check_unavailable", fts,
                f"The exact token comparison could not run: {token_error}",
                f"Run the authoritative {fts} integrity check and rebuild if it fails.",
            )
    return result


def _load_database_records(
    connection: sqlite3.Connection,
) -> tuple[list[dict[str, Any]], set[str]]:
    videos: list[dict[str, Any]] = []
    segment_identities: set[str] = set()
    video_columns = _columns(connection, "videos")
    if video_columns:
        wanted = [
            name for name in (
                "id", "title", "channel", "filepath", "video_id",
                "is_duplicate_of", "availability", "downloaded_ts",
            ) if name in video_columns
        ]
        cursor = connection.execute(
            "SELECT " + ",".join(f'"{name}"' for name in wanted) + " FROM videos")
        videos = [dict(zip(wanted, row, strict=True)) for row in cursor]
    segment_columns = _columns(connection, "segments")
    if {"title", "channel"}.issubset(segment_columns):
        id_expr = "video_id" if "video_id" in segment_columns else "''"
        for video_id, channel, title in connection.execute(
            f"SELECT DISTINCT {id_expr}, channel, title FROM segments"):
            segment_identities.add(_record_identity(video_id, channel, title))
    return videos, segment_identities


def _scan_transcript_agreement(
    transcript_sources: dict[str, list[dict[str, str]]],
    db_identities: set[str], issues: list[dict[str, Any]],
) -> dict[str, Any]:
    by_source = {
        "txt": {row["identity"] for row in transcript_sources["txt"]},
        "jsonl": {row["identity"] for row in transcript_sources["jsonl"]},
        "db": set(db_identities),
    }
    all_identities = set().union(*by_source.values())
    mismatches = 0
    for identity in sorted(all_identities):
        present = sorted(name for name, rows in by_source.items() if identity in rows)
        missing = sorted(set(by_source) - set(present))
        if not missing:
            continue
        mismatches += 1
        if identity.startswith("title:"):
            repair = (
                "Resolve this title-only record to one stable video ID; then regenerate only "
                "the missing transcript representation from an authoritative copy."
            )
        else:
            repair = (
                "Regenerate the missing transcript representation(s) from the authoritative "
                "JSONL/media record, then reindex that stable video ID."
            )
        _add_issue(
            issues, "transcript_agreement", "transcript_store_disagreement",
            _display_identity(identity),
            f"Present in {', '.join(present)}; missing from {', '.join(missing)}.",
            repair,
            evidence={"identity": identity, "present_in": present, "missing_from": missing},
        )
    return {
        "txt_records": len(by_source["txt"]),
        "jsonl_records": len(by_source["jsonl"]),
        "db_records": len(by_source["db"]),
        "mismatches": mismatches,
    }


def _media_exists(filepath: Any, archive_media: set[str]) -> bool:
    value = str(filepath or "").strip()
    return bool(value) and _norm_path(value) in archive_media


def _scan_catalog_links(
    videos: list[dict[str, Any]], archive_media: set[str],
    issues: list[dict[str, Any]],
) -> dict[str, int]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in videos:
        if video_id := _valid_video_id(row.get("video_id")):
            groups[video_id].append(row)

    duplicate_groups = 0
    link_issues = 0
    for video_id, rows in sorted(groups.items()):
        if len(rows) < 2:
            if rows[0].get("is_duplicate_of"):
                link_issues += 1
                _add_issue(
                    issues, "canonical_links", "dangling_duplicate_link", video_id,
                    "The only catalog row for this video ID is marked as a duplicate.",
                    "Clear or relink the duplicate marker after verifying the canonical media row.",
                )
            continue
        duplicate_groups += 1

        def rank(row: dict[str, Any]) -> tuple[int, int, int]:
            available = str(row.get("availability") or "available") == "available"
            saved = _media_exists(row.get("filepath"), archive_media)
            primary = not bool(row.get("is_duplicate_of"))
            return (0 if available and saved else 1, 0 if primary else 1,
                    int(row.get("id") or 0))

        canonical = min(rows, key=rank)
        canonical_path = str(canonical.get("filepath") or "")
        primaries = [row for row in rows if not row.get("is_duplicate_of")]
        wrong_links = [
            row for row in rows if row is not canonical
            and _norm_path(row.get("is_duplicate_of")) != _norm_path(canonical_path)
        ]
        if (len(primaries) != 1 or primaries[0] is not canonical or wrong_links):
            link_issues += 1
            _add_issue(
                issues, "canonical_links", "canonical_link_disagreement", video_id,
                f"{len(rows)} physical rows have {len(primaries)} primary marker(s); "
                f"{len(wrong_links)} duplicate link(s) do not target the preferred copy.",
                "Select the available canonical media row and relink every sibling copy to its filepath.",
                evidence={
                    "preferred_row_id": canonical.get("id"),
                    "preferred_filepath": canonical_path,
                    "row_ids": [row.get("id") for row in rows],
                },
            )
    return {"duplicate_groups": duplicate_groups, "link_issues": link_issues}


def _scan_normalized_catalog_links(
    connection: sqlite3.Connection | None, issues: list[dict[str, Any]],
) -> int:
    if connection is None:
        return 0
    media_columns = _columns(connection, "media_files")
    logical_columns = _columns(connection, "logical_videos")
    required_media = {
        "logical_video_id", "legacy_video_row_id", "filepath", "is_primary",
    }
    required_logical = {
        "logical_id", "video_id", "legacy_canonical_row_id",
    }
    if not required_media.issubset(media_columns) \
            or not required_logical.issubset(logical_columns):
        return 0
    rows = connection.execute(
        """SELECT lv.logical_id,lv.video_id,lv.legacy_canonical_row_id,
                  mf.legacy_video_row_id,mf.filepath,mf.is_primary
             FROM logical_videos AS lv
             JOIN media_files AS mf ON mf.logical_video_id=lv.logical_id
             ORDER BY lv.logical_id,mf.legacy_video_row_id"""
    ).fetchall()
    groups: dict[int, list[tuple[Any, ...]]] = defaultdict(list)
    for row in rows:
        groups[int(row[0])].append(row)
    disagreements = 0
    for logical_id, copies in groups.items():
        primaries = [row for row in copies if bool(row[5])]
        canonical_row_id = copies[0][2]
        primary_matches = (
            len(primaries) == 1
            and (canonical_row_id is None or int(primaries[0][3]) == int(canonical_row_id))
        )
        if primary_matches:
            continue
        disagreements += 1
        video_id = _valid_video_id(copies[0][1])
        _add_issue(
            issues, "canonical_links", "normalized_primary_disagreement",
            video_id or f"logical:{logical_id}",
            f"The normalized catalog has {len(primaries)} primary media rows; "
            f"legacy_canonical_row_id={canonical_row_id}.",
            "Reconcile this logical identity from legacy videos and select exactly one available primary media row.",
            severity="error",
            evidence={
                "logical_id": logical_id,
                "media_row_ids": [row[3] for row in copies],
                "primary_row_ids": [row[3] for row in primaries],
            },
        )
    return disagreements


def _scan_same_title_collisions(
    videos: list[dict[str, Any]], issues: list[dict[str, Any]],
) -> int:
    groups: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list))
    for row in videos:
        channel = _norm_text(row.get("channel"))
        title = _norm_text(row.get("title"))
        if not title:
            continue
        video_id = _valid_video_id(row.get("video_id"))
        identity = f"id:{video_id}" if video_id else f"path:{_norm_path(row.get('filepath'))}"
        groups[(channel, title)][identity].append(row)
    collisions = 0
    for (channel, title), identities in sorted(groups.items()):
        meaningful = [identity for identity in identities if identity != "path:"]
        if len(meaningful) < 2:
            continue
        collisions += 1
        _add_issue(
            issues, "same_title_collisions", "same_title_distinct_identity",
            f"{channel} / {title}".strip(" /"),
            f"The title belongs to {len(meaningful)} distinct logical identities.",
            "Keep the records separate and require stable IDs for transcript or duplicate repair.",
            severity="info",
            evidence={"identities": sorted(meaningful)},
        )
    return collisions


def _parse_download_archive(path: Path) -> tuple[set[str], list[str]]:
    video_ids: set[str] = set()
    errors: list[str] = []
    if not path.is_file():
        return video_ids, errors
    try:
        with path.open("r", encoding="utf-8", errors="replace") as stream:
            for line_no, line in enumerate(stream, 1):
                parts = line.strip().split()
                if len(parts) >= 2 and parts[0] == "youtube" and _valid_video_id(parts[1]):
                    video_ids.add(parts[1])
                elif line.strip():
                    errors.append(f"line {line_no}: malformed archive entry")
    except OSError as exc:
        errors.append(str(exc))
    return video_ids, errors


def _scan_saved_media(
    videos: list[dict[str, Any]], archive_media: set[str],
    filename_ids: dict[str, set[str]], download_ids: set[str],
    issues: list[dict[str, Any]],
) -> dict[str, int]:
    saved_ids = set().union(*filename_ids.values()) if filename_ids else set()
    for row in videos:
        if _media_exists(row.get("filepath"), archive_media):
            if video_id := _valid_video_id(row.get("video_id")):
                saved_ids.add(video_id)
    missing_archive_ids = sorted(download_ids - saved_ids)
    for video_id in missing_archive_ids:
        _add_issue(
            issues, "saved_media", "download_archive_id_without_media", video_id,
            "The yt-dlp download archive marks this ID downloaded, but no saved media was found.",
            "Verify the file was not moved; then either relink/rescan it or remove the stale archive ID so it can be downloaded again.",
            severity="error",
        )

    downloaded_without_media = 0
    for row in videos:
        try:
            downloaded = float(row.get("downloaded_ts") or 0) > 0
        except (TypeError, ValueError):
            downloaded = False
        available = str(row.get("availability") or "available") == "available"
        if not (downloaded or available and row.get("filepath")):
            continue
        if _media_exists(row.get("filepath"), archive_media):
            continue
        downloaded_without_media += 1
        _add_issue(
            issues, "saved_media", "catalog_download_without_media",
            _valid_video_id(row.get("video_id")) or f"row:{row.get('id')}",
            f"Catalog row {row.get('id')} points to unavailable media: {row.get('filepath') or '(blank)' }.",
            "Locate and relink the media, or mark the physical catalog row missing before retrying the download.",
            severity="error",
            evidence={"row_id": row.get("id"), "filepath": row.get("filepath")},
        )
    return {
        "download_archive_ids": len(download_ids),
        "archive_ids_without_media": len(missing_archive_ids),
        "catalog_rows_without_media": downloaded_without_media,
    }


def _task_key(task: Any) -> str:
    if not isinstance(task, dict):
        return ""
    if task_id := str(task.get("task_id") or "").strip():
        return f"task:{task_id}"
    kind = str(task.get("kind") or "transcribe").strip().casefold()
    path = _norm_path(task.get("path") or task.get("filepath"))
    return f"job:{kind}\0{path}" if path else ""


def _queue_tasks(value: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    gpu: list[dict[str, Any]] = []
    sync: list[dict[str, Any]] = []
    if not isinstance(value, dict):
        return gpu, sync
    gpu.extend(row for row in value.get("gpu", []) if isinstance(row, dict)) \
        if isinstance(value.get("gpu"), list) else None
    sync.extend(row for row in value.get("sync", []) if isinstance(row, dict)) \
        if isinstance(value.get("sync"), list) else None
    resuming = value.get("resuming")
    if isinstance(resuming, dict):
        if isinstance(resuming.get("gpu"), dict):
            gpu.append(resuming["gpu"])
        if isinstance(resuming.get("sync"), dict):
            sync.append(resuming["sync"])
    return gpu, sync


def _scan_recovery_records(
    queue: Any, resuming: Any, journal: Any, archive_media: set[str],
    issues: list[dict[str, Any]],
) -> dict[str, int]:
    queue_gpu, _queue_sync = _queue_tasks(queue)
    sidecar_gpu, _sidecar_sync = _queue_tasks(resuming)
    queue_gpu.extend(sidecar_gpu)
    journal_jobs = [row for row in journal if isinstance(row, dict)] \
        if isinstance(journal, list) else []

    # De-duplicate the main/resuming views of one permanent task.
    queue_by_key = {_task_key(row): row for row in queue_gpu if _task_key(row)}
    journal_by_key = {_task_key(row): row for row in journal_jobs if _task_key(row)}
    orphan_count = 0
    for source, rows in (("queue", queue_by_key), ("transcription journal", journal_by_key)):
        for key, row in sorted(rows.items()):
            path = str(row.get("path") or row.get("filepath") or "").strip()
            if not path or _norm_path(path) in archive_media or Path(path).is_file():
                continue
            orphan_count += 1
            _add_issue(
                issues, "recovery_records", "recovery_target_missing", key,
                f"The {source} recovery record targets missing media: {path}.",
                "Locate/relink the media or retire this recovery record after confirming it cannot resume.",
                evidence={"source": source, "path": path},
            )

    queue_only = sorted(set(queue_by_key) - set(journal_by_key))
    journal_only = sorted(set(journal_by_key) - set(queue_by_key))
    for key in queue_only:
        row = queue_by_key[key]
        if str(row.get("kind") or "transcribe").casefold() not in {
            "transcribe", "retranscribe", "punctuate", "punctuation", "compress",
        }:
            continue
        _add_issue(
            issues, "recovery_records", "queue_only_processing_recovery", key,
            "A persisted Processing task has no matching transcription recovery journal row.",
            "Reconcile both recovery stores from the queue task before allowing Processing to start.",
        )
    for key in journal_only:
        _add_issue(
            issues, "recovery_records", "journal_only_processing_recovery", key,
            "A transcription recovery journal row has no matching persisted Processing task.",
            "Reconcile both recovery stores from the journal row before allowing Processing to start.",
        )
    return {
        "queue_gpu_records": len(queue_by_key),
        "journal_records": len(journal_by_key),
        "missing_targets": orphan_count,
        "queue_only": len(queue_only),
        "journal_only": len(journal_only),
    }


def _safe_folder_name(value: Any) -> str:
    name = str(value or "").strip().rstrip(". ")
    return re.sub(r'[<>:"/\\|?*]', "_", name)


def _channel_aliases(config: dict[str, Any]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    channels = config.get("channels")
    if not isinstance(channels, list):
        return aliases
    for channel in channels:
        if not isinstance(channel, dict):
            continue
        name = str(channel.get("name") or channel.get("folder") or "").strip()
        folder = _safe_folder_name(
            channel.get("folder_override") or channel.get("folder") or name)
        if folder and name:
            aliases[_norm_text(folder)] = name
    return aliases


def _scan_folder_overrides(
    config: dict[str, Any], archive_path: Path, issues: list[dict[str, Any]],
) -> int:
    count = 0
    channels = config.get("channels")
    if not isinstance(channels, list):
        return count
    for index, channel in enumerate(channels):
        if not isinstance(channel, dict):
            continue
        name = str(channel.get("name") or channel.get("folder") or "").strip()
        override = str(channel.get("folder_override") or "").strip()
        folder_name = _safe_folder_name(override or channel.get("folder") or name)
        target = archive_path / folder_name if folder_name else archive_path
        if override and not target.is_dir():
            count += 1
            _add_issue(
                issues, "folder_overrides", "folder_override_missing",
                name or f"channel:{index}",
                f"folder_override points to a folder that does not exist: {target}.",
                "Locate the channel folder and update the override, or clear the stale override after verification.",
                evidence={"folder_override": override, "expected_path": str(target)},
            )
        elif override and _norm_text(override) == _norm_text(_safe_folder_name(name)):
            count += 1
            _add_issue(
                issues, "folder_overrides", "redundant_folder_override",
                name or f"channel:{index}",
                "folder_override resolves to the channel's normal folder name.",
                "Clear the redundant override after confirming the resolved folder remains unchanged.",
                severity="info",
                evidence={"folder_override": override, "resolved_path": str(target)},
            )
    return count


def _history_entries(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    for value in values:
        if isinstance(value, str) and value:
            result.append(value)
        elif isinstance(value, dict) and isinstance(value.get("entry"), str):
            if value["entry"]:
                result.append(value["entry"])
    return result


def _scan_activity_history(
    config: dict[str, Any], activity_rows: list[Any],
    activity_errors: list[str], issues: list[dict[str, Any]],
) -> dict[str, int]:
    legacy = _history_entries(config.get("autorun_history") or []) \
        if isinstance(config.get("autorun_history"), list) else []
    canonical = _history_entries(activity_rows)
    for error in activity_errors:
        _add_issue(
            issues, "activity_history", "activity_history_parse_error",
            "activity history", error,
            "Preserve the file, recover valid JSONL records, and rewrite it atomically.",
            severity="error",
        )
    if legacy and legacy != canonical:
        _add_issue(
            issues, "activity_history", "activity_history_disagreement",
            "autorun history",
            f"Config contains {len(legacy)} entries while canonical JSONL contains {len(canonical)} different entries.",
            "Merge the legacy config entries into stable-ID JSONL records, verify the merge, then retire the config copy.",
            evidence={
                "config_only": [entry for entry in legacy if entry not in canonical][:20],
                "jsonl_only": [entry for entry in canonical if entry not in legacy][:20],
            },
        )
    return {"config_entries": len(legacy), "jsonl_entries": len(canonical)}


def _scan_migration_state(
    config: dict[str, Any], queue: Any, resuming: Any,
    connection: sqlite3.Connection | None, issues: list[dict[str, Any]],
) -> dict[str, Any]:
    state: dict[str, Any] = {
        "config_pending_tx_migrated": bool(config.get("_migration_v2_pending_tx_ids")),
        "queue_schema_version": queue.get("_schema_version") if isinstance(queue, dict) else None,
        "resuming_schema_version": resuming.get("_schema_version") if isinstance(resuming, dict) else None,
        "database_user_version": None,
        "catalog_phase": "unavailable",
        "catalog_schema_version": None,
        "catalog_legacy_dirty": None,
    }
    if not state["config_pending_tx_migrated"]:
        _add_issue(
            issues, "migration_state", "config_migration_pending", "config",
            "The pending-transcription ID migration marker is absent or false.",
            "Run the normal guarded config migration and persist its completion marker.",
        )
    try:
        queue_schema = int(state["queue_schema_version"] or 1)
    except (TypeError, ValueError, OverflowError):
        queue_schema = 1
    if isinstance(queue, dict) and queue_schema < _EXPECTED_QUEUE_SCHEMA_VERSION:
        _add_issue(
            issues, "migration_state", "queue_migration_pending", "queue",
            f"Queue schema is {queue_schema}; expected {_EXPECTED_QUEUE_SCHEMA_VERSION}.",
            "Normalize stable task IDs and commit the main/resuming queue pair atomically.",
        )
    if isinstance(resuming, dict):
        try:
            resume_schema = int(state["resuming_schema_version"] or 1)
        except (TypeError, ValueError, OverflowError):
            resume_schema = 1
        if resume_schema < _EXPECTED_RESUMING_SCHEMA_VERSION:
            _add_issue(
                issues, "migration_state", "resuming_migration_pending", "queue resuming sidecar",
                f"Resuming schema is {resume_schema}; expected {_EXPECTED_RESUMING_SCHEMA_VERSION}.",
                "Normalize stable task IDs and commit the resuming sidecar with the main queue.",
            )
    if connection is None:
        return state
    state["database_user_version"] = int(
        connection.execute("PRAGMA user_version").fetchone()[0])
    if state["database_user_version"] < _EXPECTED_DB_USER_VERSION:
        _add_issue(
            issues, "migration_state", "database_migration_pending", "transcription database",
            f"Database user_version is {state['database_user_version']}; expected {_EXPECTED_DB_USER_VERSION}.",
            "Back up the legacy catalog, run the additive database migration, and verify every phase before enabling new reads.",
        )
    if not _table_exists(connection, "catalog_state"):
        state["catalog_phase"] = "legacy"
        _add_issue(
            issues, "migration_state", "catalog_migration_pending", "catalog",
            "The normalized logical-video/media catalog is not installed.",
            "Create a verified legacy backup, install the additive catalog schema, reconcile, and compare before switching reads.",
        )
        return state
    row = connection.execute(
        "SELECT phase,schema_version,legacy_dirty,last_error FROM catalog_state WHERE singleton=1"
    ).fetchone()
    if row is None:
        _add_issue(
            issues, "migration_state", "catalog_state_missing", "catalog",
            "catalog_state has no singleton migration row.",
            "Re-run catalog reconciliation from the untouched legacy videos table.",
            severity="error",
        )
        return state
    phase, schema_version, dirty, last_error = row
    state.update({
        "catalog_phase": str(phase),
        "catalog_schema_version": int(schema_version),
        "catalog_legacy_dirty": bool(dirty),
        "catalog_last_error": str(last_error or ""),
    })
    dirty_keys = int(connection.execute(
        "SELECT COUNT(*) FROM catalog_dirty_keys").fetchone()[0]) \
        if _table_exists(connection, "catalog_dirty_keys") else 0
    state["catalog_dirty_keys"] = dirty_keys
    if (str(phase) != "v2_writes" or int(schema_version) != _EXPECTED_CATALOG_SCHEMA_VERSION
            or bool(dirty) or dirty_keys or last_error):
        _add_issue(
            issues, "migration_state", "catalog_not_current", "catalog",
            f"phase={phase}, schema={schema_version}, legacy_dirty={bool(dirty)}, "
            f"dirty_keys={dirty_keys}, last_error={last_error or '(none)' }.",
            "Reconcile dirty legacy identities, compare projections, and enable v2 writes only after equivalence succeeds.",
            severity="error" if last_error else "warning",
        )
    return state


def scan_integrity(
    *,
    archive_path: str | os.PathLike[str] | Path,
    config_path: str | os.PathLike[str] | Path,
    db_path: str | os.PathLike[str] | Path,
    queue_path: str | os.PathLike[str] | Path,
    download_archive_path: str | os.PathLike[str] | Path | None = None,
    transcription_recovery_path: str | os.PathLike[str] | Path | None = None,
    activity_history_path: str | os.PathLike[str] | Path | None = None,
) -> dict[str, Any]:
    """Inspect explicit paths and return proposed repairs without mutations."""
    archive = _path(archive_path)
    config_file = _path(config_path)
    database = _path(db_path)
    queue_file = _path(queue_path)
    download_file = _path(download_archive_path) if download_archive_path else \
        config_file.with_name("ytarchiver_archive.txt")
    recovery_file = _path(transcription_recovery_path) \
        if transcription_recovery_path else \
        queue_file.with_name("ytarchiver_pending_transcribe.json")
    activity_file = _path(activity_history_path) if activity_history_path else \
        config_file.with_name("autorun_history.jsonl")
    resuming_file = queue_file.with_name(
        f"{queue_file.stem}_resuming{queue_file.suffix or '.json'}")

    issues: list[dict[str, Any]] = []
    inputs = {
        "archive_path": str(archive),
        "config_path": str(config_file),
        "db_path": str(database),
        "queue_path": str(queue_file),
        "download_archive_path": str(download_file),
        "transcription_recovery_path": str(recovery_file),
        "activity_history_path": str(activity_file),
        "queue_resuming_path": str(resuming_file),
    }

    config_value, config_error = _read_json(config_file)
    config = config_value if isinstance(config_value, dict) else {}
    if config_error:
        _add_issue(
            issues, "inputs", "config_unreadable", str(config_file), config_error,
            "Restore or repair a verified config copy before applying any other proposal.",
            severity="error",
        )
    queue, queue_error = _read_json(queue_file)
    if queue_error not in ("", "missing"):
        _add_issue(
            issues, "inputs", "queue_unreadable", str(queue_file), queue_error,
            "Preserve the unreadable queue and recover valid tasks into a new atomic queue file.",
            severity="error",
        )
    resuming, resuming_error = _read_json(resuming_file)
    if resuming_error not in ("", "missing"):
        _add_issue(
            issues, "inputs", "resuming_unreadable", str(resuming_file), resuming_error,
            "Preserve the unreadable sidecar and reconcile recoverable current tasks with the main queue.",
            severity="error",
        )
    journal, journal_error = _read_json(recovery_file)
    if journal_error not in ("", "missing"):
        _add_issue(
            issues, "inputs", "transcription_recovery_unreadable", str(recovery_file),
            journal_error,
            "Preserve the journal and recover valid task records before replacing it atomically.",
            severity="error",
        )
    activity_rows, activity_errors = _read_jsonl(activity_file)

    archive_media, transcript_sources, filename_ids, archive_errors = \
        _walk_archive(archive, _channel_aliases(config))
    if not archive.is_dir():
        _add_issue(
            issues, "inputs", "archive_unavailable", str(archive),
            "The explicit archive root is missing or not a directory.",
            "Correct the archive path before evaluating file-based repair proposals.",
            severity="error",
        )
    for error in archive_errors:
        _add_issue(
            issues, "inputs", "archive_read_error", str(archive), error,
            "Retry the preview after archive access is restored.", severity="error",
        )

    connection: sqlite3.Connection | None = None
    database_error = ""
    videos: list[dict[str, Any]] = []
    segment_identities: set[str] = set()
    fts: dict[str, Any] = {}
    if not database.is_file():
        database_error = "missing"
    else:
        try:
            connection = _open_database_read_only(database)
            videos, segment_identities = _load_database_records(connection)
            fts = _fts_integrity(connection, issues)
        except sqlite3.Error as exc:
            database_error = str(exc)
    if database_error:
        _add_issue(
            issues, "inputs", "database_unreadable", str(database), database_error,
            "Restore/read a verified database snapshot before applying catalog or FTS proposals.",
            severity="error",
        )

    try:
        transcript_summary = _scan_transcript_agreement(
            transcript_sources, segment_identities, issues)
        link_summary = _scan_catalog_links(videos, archive_media, issues)
        link_summary["normalized_primary_issues"] = \
            _scan_normalized_catalog_links(connection, issues)
        collision_count = _scan_same_title_collisions(videos, issues)
        download_ids, download_errors = _parse_download_archive(download_file)
        for error in download_errors:
            _add_issue(
                issues, "saved_media", "download_archive_parse_error",
                str(download_file), error,
                "Preserve valid IDs and rewrite malformed archive entries atomically.",
            )
        media_summary = _scan_saved_media(
            videos, archive_media, filename_ids, download_ids, issues)
        recovery_summary = _scan_recovery_records(
            queue, resuming, journal, archive_media, issues)
        folder_override_count = _scan_folder_overrides(config, archive, issues)
        activity_summary = _scan_activity_history(
            config, activity_rows, activity_errors, issues)
        migration_state = _scan_migration_state(
            config, queue, resuming, connection, issues)
    finally:
        if connection is not None:
            connection.close()

    categories: dict[str, int] = defaultdict(int)
    for issue in issues:
        categories[issue["category"]] += 1
    return {
        "ok": not any(issue["category"] == "inputs" for issue in issues),
        "healthy": not issues,
        "preview_only": True,
        "repairs_applied": False,
        "repair_available": False,
        "verified_backup_required_before_repair": True,
        "scanned_at": time.time(),
        "inputs": inputs,
        "summary": {
            "issues": len(issues),
            "proposed_repairs": len(issues),
            "categories": dict(sorted(categories.items())),
            "media_files_seen": len(archive_media),
            "videos_rows_seen": len(videos),
            "same_title_collisions": collision_count,
            "folder_override_issues": folder_override_count,
        },
        "checks": {
            "fts": fts,
            "transcript_agreement": transcript_summary,
            "canonical_links": link_summary,
            "saved_media": media_summary,
            "recovery_records": recovery_summary,
            "activity_history": activity_summary,
            "migration_state": migration_state,
        },
        "issues": issues,
    }


run_integrity_scan = scan_integrity


__all__ = ["run_integrity_scan", "scan_integrity"]
