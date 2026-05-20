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
import threading
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
    `_unnamed/` graveyard or any channel without a usable name.

    Cached output_dir lookup — bulk metadata ops called this per-video,
    re-reading the entire config file from disk each time (audit:
    metadata_io.py:42-55). Caching by mtime fingerprint catches the
    common case (config unchanged during a bulk pass) while still
    seeing edits made mid-pass.
    """
    # Late import to avoid circular dep (sync.py imports metadata.py).
    from .sync import sanitize_folder
    base = _cached_output_dir()
    if not base:
        return None
    folder_name = sanitize_folder((ch.get("folder_override") or "").strip()
                                  or ch.get("name", ""))
    if not folder_name or folder_name == "_unnamed":
        return None
    return Path(base) / folder_name


_OUTPUT_DIR_CACHE: dict[str, Any] = {"mtime": 0.0, "value": ""}


def _cached_output_dir() -> str:
    """Return cfg["output_dir"], cached by config-file mtime so bulk
    callers (1000+ per-video lookups) only re-read the file when it
    actually changed."""
    try:
        from .ytarchiver_config import CONFIG_FILE as _CF
        try:
            _mt = os.path.getmtime(str(_CF))
        except OSError:
            _mt = 0.0
        if _mt and _mt == _OUTPUT_DIR_CACHE["mtime"]:
            return _OUTPUT_DIR_CACHE["value"]
        cfg = load_config() or {}
        _val = (cfg.get("output_dir") or "").strip()
        _OUTPUT_DIR_CACHE["mtime"] = _mt
        _OUTPUT_DIR_CACHE["value"] = _val
        return _val
    except Exception:
        try:
            cfg = load_config() or {}
            return (cfg.get("output_dir") or "").strip()
        except Exception:
            return ""


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
        # utf-8-sig so a UTF-8 BOM at the top of an externally-edited
        # jsonl doesn't strip the first entry's first byte (audit:
        # metadata_io.py:96).
        with open(jsonl_path, "r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                _total_lines += 1
                try:
                    entry = json.loads(line)
                    vid = entry.get("video_id", "")
                    if vid:
                        # Duplicate-vid resolution: prefer the entry
                        # with the more-recent `fetched_at`. Without
                        # this, a crash mid-rewrite that left two
                        # entries for the same video_id silently
                        # picked LAST line wins regardless of which
                        # was newer (audit: metadata_io.py:83-120).
                        _prev = existing.get(vid)
                        if _prev is None:
                            existing[vid] = entry
                        else:
                            _new_fa = str(entry.get("fetched_at") or "")
                            _prev_fa = str(_prev.get("fetched_at") or "")
                            # ISO-8601 timestamps sort lexicographically.
                            if _new_fa >= _prev_fa:
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


# Per-jsonl-path serializer. Two threads (sync writer + bulk metadata
# refresh) writing the same jsonl_path used to race on os.replace
# — one writer's full entries-dict would land, the other's would
# disappear (audit: metadata_io.py:123-144).
_write_locks: dict[str, threading.Lock] = {}
_write_locks_global = threading.Lock()


def _lock_for(path: str) -> threading.Lock:
    key = os.path.normcase(os.path.abspath(path))
    with _write_locks_global:
        lk = _write_locks.get(key)
        if lk is None:
            lk = threading.Lock()
            _write_locks[key] = lk
        return lk


def _write_metadata_jsonl(jsonl_path: str,
                          entries_dict: dict[str, dict[str, Any]]) -> None:
    """Write all entries to the aggregated JSONL, hiding on Windows.
    Matches YTArchiver.py:26583.

    Atomic via .tmp + fsync + os.replace so a crash mid-write doesn't
    truncate the entire channel's metadata file. Serialized per
    jsonl_path so two concurrent writers can't race on os.replace
    and lose one writer's entries.
    """
    os.makedirs(os.path.dirname(jsonl_path) or ".", exist_ok=True)
    with _lock_for(jsonl_path):
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
