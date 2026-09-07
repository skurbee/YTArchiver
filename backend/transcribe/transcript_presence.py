"""Read-only evidence that one video has saved transcript content."""
from __future__ import annotations

import re

from ..services.sidecar_store import read_jsonl, read_text
from ..text_utils import normalize_title
from .transcribe_files import _HEADER_RE

_TITLE_ID_RE = re.compile(r"\s*\[([A-Za-z0-9_-]{11})\]\s*$")


def _record_string(record: dict, key: str) -> str:
    value = record.get(key)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"Transcript record {key!r} must be text")
    return value.strip()


def _matches(stored_title: str, stored_id: str,
             title: str, video_id: str) -> bool:
    # A different stable ID defeats a same-title match. Legacy records that
    # have no ID may still match the normalized title, as the writers do.
    if video_id and stored_id:
        return stored_id == video_id
    return bool(stored_title and title
                and normalize_title(stored_title) == normalize_title(title))


def _txt_has_transcript(content: str, title: str, video_id: str) -> bool:
    found = False
    matches_current = None
    for line_number, raw in enumerate(content.splitlines(), 1):
        line = raw.strip()
        if not line:
            continue
        if line.startswith("==="):
            header = _HEADER_RE.fullmatch(line)
            if header is None:
                raise ValueError(
                    f"Malformed transcript header on line {line_number}")
            stored_title = header.group(1).strip()
            stored_id = (header.group(5) or "").strip()
            # Older headers sometimes kept the filename's trailing [ID].
            # The explicit v2 URL ID, when present, remains authoritative.
            bracket_id = _TITLE_ID_RE.search(stored_title)
            if bracket_id:
                if not stored_id:
                    stored_id = bracket_id.group(1)
                stored_title = stored_title[:bracket_id.start()].strip()
            matches_current = _matches(
                stored_title, stored_id, title, video_id)
        elif matches_current is None:
            raise ValueError(
                f"Transcript content has no identity header on line {line_number}")
        elif matches_current:
            found = True
    return found


def has_existing_transcript(txt_path: str, jsonl_path: str,
                            title: str, video_id: str) -> bool:
    """Check only the two resolved output files for this video's words.

    The caller holds ``transcript_output_locks`` across this read and its
    outcome decision. Missing/empty files and unrelated entries return False.
    Read, decode and parse failures raise OSError or ValueError: uncertainty
    must not turn a possibly existing transcript into a no-speech result.
    Both files are validated even when one already contains matching words.
    No database connection, archive scan, or file write is performed.
    """
    wanted_title = (title or "").strip()
    wanted_id = (video_id or "").strip()
    jsonl = read_jsonl(jsonl_path, invalid="raise")
    found_jsonl = False
    for record in jsonl.records:
        stored_title = _record_string(record, "title")
        stored_id = _record_string(record, "video_id")
        text = _record_string(record, "text")
        if text and _matches(stored_title, stored_id, wanted_title, wanted_id):
            found_jsonl = True
    txt = read_text(txt_path).text
    found_txt = _txt_has_transcript(txt, wanted_title, wanted_id)
    return found_jsonl or found_txt
