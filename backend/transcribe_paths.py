"""
Transcript path + formatting helpers.

extracted from `backend/transcribe.py` (3,283 lines)
as the first step of decomposing that megafile. These are pure
filename / format / hide-attr utilities — no Whisper, no ffmpeg, no
subprocess state.

Public API (also re-exported by `backend.transcribe`):
    _hide_file_win(path)
    _get_transcript_filename(...) -> (txt_path, subfolder)
    _get_jsonl_sidecar(txt_path) -> str
    _format_upload_date(date_str) -> str
    _format_duration_hms(secs) -> str
    _generate_distributed_words(text, start, end) -> list[dict]
"""

from __future__ import annotations

import os

from .log import get_logger
from .utils import (
    MONTH_FOLDERS as _MONTH_NAMES,
)

# Re-exported so transcribe/transcribe_files.py + transcribe/core.py can
# pull it from this single facade module.
from .utils import hide_file_win as _hide_file_win  # noqa: F401

_log = get_logger(__name__)


def _get_transcript_filename(ch_name: str, folder_path: str,
                             split_years: bool, split_months: bool,
                             combined: bool,
                             year: int | None = None,
                             month: int | None = None
                             ) -> tuple[str, str]:
    """Mirror of YTArchiver.py:11771 _get_transcript_filename.
    Returns (txt_path, subfolder)."""
    # Defensive: split_years=True with year=None is a caller bug —
    # without this guard, both the month and year branches below
    # fall through and we'd silently write to the combined-mode
    # filename (audit: transcribe_paths.py:43-58). Surface the bug
    # rather than silently misroute.
    if split_years and year is None and not combined:
        raise ValueError(
            "_get_transcript_filename: split_years=True requires a "
            "non-None year; got year=None")
    if combined or (not split_years):
        return (os.path.join(folder_path, f"{ch_name} Transcript.txt"), folder_path)

    if split_years and split_months and year and month:
        month_num = int(month) if isinstance(month, str) and str(month).isdigit() else month
        month_name_full = _MONTH_NAMES.get(month_num, f"{month_num:02d} Unknown")
        month_name = month_name_full.split(" ", 1)[1]
        yr_short = str(year)[-2:]
        subfolder = os.path.join(folder_path, str(year), month_name_full)
        fname = f"{ch_name} {month_name} {yr_short} Transcript.txt"
        return (os.path.join(subfolder, fname), subfolder)

    if split_years and year:
        subfolder = os.path.join(folder_path, str(year))
        fname = f"{ch_name} {year} Transcript.txt"
        return (os.path.join(subfolder, fname), subfolder)

    return (os.path.join(folder_path, f"{ch_name} Transcript.txt"), folder_path)


def _get_jsonl_sidecar(txt_path: str) -> str:
    """Hidden JSONL sidecar next to a transcript .txt file.
    Returns .../.{ch_name} ... Transcript.jsonl — matches YTArchiver.py:8490."""
    dirname = os.path.dirname(txt_path)
    basename = os.path.basename(txt_path)
    root_name, _ = os.path.splitext(basename)
    return os.path.join(dirname, "." + root_name + ".jsonl")


def _format_upload_date(date_str: str) -> str:
    """YYYYMMDD -> (MM.DD.YYYY). Matches YTArchiver.py:11757."""
    if len(date_str) == 8 and date_str.isdigit():
        return f"({date_str[4:6]}.{date_str[6:8]}.{date_str[:4]})"
    return f"({date_str})" if date_str else "(Unknown date)"


def _format_duration_hms(secs: float) -> str:
    """Duration in H:MM:SS (or M:SS). Matches YTArchiver.py's _format_duration_hms."""
    try:
        total = int(round(float(secs)))
    except Exception:
        return ""
    if total <= 0:
        return ""
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _generate_distributed_words(text: str, start: float, end: float
                                ) -> list[dict]:
    """Evenly distribute word-level timestamps across a segment.
    Used when the upstream source didn't provide real word-level timings.
    Matches YTArchiver.py:8478 _generate_distributed_words."""
    words = (text or "").split()
    if not words:
        return []
    dur = max(end - start, 0.01)
    step = dur / len(words)
    return [{"w": w,
             "s": round(start + i * step, 3),
             "e": round(start + (i + 1) * step, 3)}
            for i, w in enumerate(words)]
