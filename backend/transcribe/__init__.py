"""
backend.transcribe package — Whisper transcription pipeline entry point.

Originally a single `backend/transcribe.py` (~3,200 lines). Split into:

    transcribe/core.py             — TranscribeManager + PunctuationManager
    transcribe/transcribe_files.py — write/replace .jsonl + .txt sidecars
    transcribe/transcribe_vtt.py   — YT auto-captions fast-path

This `__init__.py` re-exports every previously-public symbol so external
callers (api_mixins, sync, repair_captions, punct_restore, main) keep
using `from backend.transcribe import TranscribeManager` unchanged.
"""
from __future__ import annotations

from .core import *  # noqa: F401,F403

# Explicit underscore-name re-exports — external callers reach in.
from .core import (  # noqa: F401
    _extract_video_id,
    _norm_title,
    _parse_vtt,
    _replace_jsonl_entry,
    _scan_existing_transcript_titles,
)
