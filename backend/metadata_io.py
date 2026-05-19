"""
Metadata I/O helpers — JSONL read/write, file-hiding, path resolution.

extracted from `backend/metadata.py` (4,330 lines) as
the first step of decomposing that megafile. These helpers are pure
I/O — no business logic, no yt-dlp, no executor state — so they can
live in their own module without dragging in heavy dependencies.

Public API (also re-exported by `backend.metadata` for backward
compatibility — existing `from backend.metadata import _read_metadata_jsonl`
callers keep working unchanged):
    _hide_file_win(path)
    _unhide_file_win(path)
    _folder_for_channel(ch) -> Path | None
    _get_metadata_jsonl_path(...) -> (jsonl_path, subfolder)
    _read_metadata_jsonl(path) -> {video_id: entry}
    _write_metadata_jsonl(path, entries_dict)
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .log import get_logger
from .utils import (
    MONTH_FOLDERS as _MONTH_NAMES,
)
from .utils import (
    hide_file_win as _hide_file_win,
)
from .utils import (
    unhide_file_win as _unhide_file_win,
)
from .ytarchiver_config import load_config

_log = get_logger(__name__)


def _folder_for_channel(ch: dict[str, Any]) -> Path | None:
    """Resolve the on-disk channel folder. Returns None for the
    `_unnamed/` graveyard or any channel without a usable name."""
    # Late import to avoid circular dep (sync.py imports metadata.py).
    from .sync import sanitize_folder
    cfg = load_config()
    base = (cfg.get("output_dir") or "").strip()
    if not base:
        return None
    folder_name = sanitize_folder((ch.get("folder_override") or "").strip()
                                  or ch.get("name", ""))
    if not folder_name or folder_name == "_unnamed":
        return None
    return Path(base) / folder_name


def _get_metadata_jsonl_path(ch_name: str, folder_path: str,
                             split_years: bool, split_months: bool,
                             year: int | None = None,
                             month: int | None = None
                             ) -> tuple[str, str]:
    """Mirror of YTArchiver.py:26539. Returns (jsonl_path, subfolder)."""
    if not split_years:
        fname = f".{ch_name} Metadata.jsonl"
        return (os.path.join(folder_path, fname), folder_path)
    if split_years and split_months and year and month:
        month_num = int(month) if isinstance(month, str) and str(month).isdigit() else month
        month_full = _MONTH_NAMES.get(month_num, f"{month_num:02d} Unknown")
        month_name = month_full.split(" ", 1)[1]
        yr_short = str(year)[-2:]
        subfolder = os.path.join(folder_path, str(year), month_full)
        fname = f".{ch_name} {month_name} {yr_short} Metadata.jsonl"
        return (os.path.join(subfolder, fname), subfolder)
    if split_years and year:
        subfolder = os.path.join(folder_path, str(year))
        fname = f".{ch_name} {year} Metadata.jsonl"
        return (os.path.join(subfolder, fname), subfolder)
    fname = f".{ch_name} Metadata.jsonl"
    return (os.path.join(folder_path, fname), folder_path)


def _read_metadata_jsonl(jsonl_path: str) -> dict[str, dict[str, Any]]:
    """Load aggregated metadata JSONL into {video_id: entry}.
    Matches YTArchiver.py:26560.

    corrupt-line warning routed through logger
    (was a print()) — captured by PyInstaller --noconsole builds.
    """
    existing: dict[str, dict[str, Any]] = {}
    if not os.path.isfile(jsonl_path):
        return existing
    _bad_lines = 0
    _total_lines = 0
    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                _total_lines += 1
                try:
                    entry = json.loads(line)
                    vid = entry.get("video_id", "")
                    if vid:
                        existing[vid] = entry
                except json.JSONDecodeError:
                    _bad_lines += 1
                    continue
    except Exception as e:
        _log.debug("swallowed: %s", e)
    if _bad_lines > 0:
        try:
            _log.warning(
                "%s: %d/%d JSONL lines were corrupt and skipped. "
                "Metadata for those videos will appear missing.",
                jsonl_path, _bad_lines, _total_lines)
        except Exception as e:
            _log.debug("swallowed: %s", e)
    return existing


def _write_metadata_jsonl(jsonl_path: str,
                          entries_dict: dict[str, dict[str, Any]]) -> None:
    """Write all entries to the aggregated JSONL, hiding on Windows.
    Matches YTArchiver.py:26583.

    Atomic via .tmp + fsync + os.replace so a crash mid-write doesn't
    truncate the entire channel's metadata file.
    """
    os.makedirs(os.path.dirname(jsonl_path) or ".", exist_ok=True)
    if os.name == "nt" and os.path.isfile(jsonl_path):
        _unhide_file_win(jsonl_path)
    tmp_path = jsonl_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        for _vid, data in entries_dict.items():
            f.write(json.dumps(data, ensure_ascii=False) + "\n")
        try:
            f.flush()
            os.fsync(f.fileno())
        except OSError:
            pass
    os.replace(tmp_path, jsonl_path)
    _hide_file_win(jsonl_path)
