"""
Smoke test: every importable module in `backend/` loads cleanly, and
every name that external callers reach for through the split packages
(backend.sync, backend.transcribe, backend.metadata) is actually exposed.

Run:  python -m pytest tests/

This catches the bug class that bit hard during the package decomposition
refactor:
  - A `from .core import X as _impl` proxy where the helper was deleted
  - A `from typing import Tuple` that ruff stripped because Tuple was
    only used inside a string-evaluated annotation
  - An __init__.py star-import that no longer surfaces a symbol callers
    expect because the symbol moved to a sibling module

Each of those would crash YTArchiver at runtime as soon as the
relevant code path fired, but were invisible to a successful import of
`backend.sync` alone — only the actual symbol access fails. These
tests exercise both shapes.
"""
from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest


# Make `backend` importable regardless of where pytest is invoked from.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# Worker scripts run as subprocesses with their own __main__ block; importing
# them in-process spins up an interactive worker loop that hangs the test.
SUBPROCESS_ENTRYPOINTS = {
    "backend.whisper_worker",
    "backend.punct_worker",
}


def _all_backend_modules() -> list[str]:
    """Walk backend/ and yield every importable module path."""
    out: list[str] = []
    for root, dirs, files in os.walk(REPO_ROOT / "backend"):
        if "__pycache__" in root:
            continue
        for fn in files:
            if not fn.endswith(".py"):
                continue
            rel = Path(root, fn).relative_to(REPO_ROOT).as_posix()
            if rel.endswith("/__init__.py"):
                mod = rel[: -len("/__init__.py")].replace("/", ".")
            else:
                mod = rel[: -len(".py")].replace("/", ".")
            if mod in SUBPROCESS_ENTRYPOINTS:
                continue
            out.append(mod)
    return sorted(out)


@pytest.mark.parametrize("modname", _all_backend_modules())
def test_module_imports(modname: str) -> None:
    """Every backend module should import without raising."""
    importlib.import_module(modname)


# Names external callers actually reach for through each split package.
# These are the contracts the package's __init__.py must keep honoring.
EXPECTED_SYNC_EXPORTS = [
    # Public entry points
    "sync_channel",
    "sync_all",
    "build_format_string",
    "channel_folder_name",
    "sanitize_folder",
    "find_yt_dlp",
    "prefetch_channel_total",
    "quick_check_new_uploads",
    "clear_sync_progress",
    "set_recent_changed_hook",
    "set_metadata_changed_hook",
    "set_sync_active",
    "clear_sync_active",
    "is_sync_active",
    "is_any_sync_active",
    "emit_consolidated_auto_row",
    "emit_metadata_activity_row",
    # Private re-exports (api_mixins reaches for these directly)
    "_find_cookie_source",
    "_startupinfo",
    "_sync_row_emit",
    "_short_summary",
    "_new_pass_id",
    "_fmt_duration",
    "_ROW_EMIT_PASS_ID",
    "_record_recent_download",
    "_persist_row_history",
    "_hide_sidecar_win",
    "_count_cell",
    "_ensure_videos_tab",
]

EXPECTED_TRANSCRIBE_EXPORTS = [
    "TranscribeManager",
    "PunctuationManager",
    "find_python311",
    "_parse_vtt",
    "_replace_jsonl_entry",
    "_norm_title",
    "_extract_video_id",
    "_scan_existing_transcript_titles",
]

EXPECTED_METADATA_EXPORTS = [
    "bulk_refresh_views_likes",
    "count_thumbnail_status_bulk",
    "count_video_id_status_bulk",
    "realign_misplaced_thumbnails",
    "sweep_missing_thumbnails",
    "fetch_single_video_metadata",
    "fetch_metadata_for_videos",
    "backfill_video_ids",
    "_read_metadata_jsonl",
]


@pytest.mark.parametrize("name", EXPECTED_SYNC_EXPORTS)
def test_sync_package_exports(name: str) -> None:
    mod = importlib.import_module("backend.sync")
    assert hasattr(mod, name), (
        f"backend.sync.{name} is missing — likely a re-export gap in "
        "backend/sync/__init__.py. External callers (api_mixins, etc.) "
        "reach for this name directly."
    )


@pytest.mark.parametrize("name", EXPECTED_TRANSCRIBE_EXPORTS)
def test_transcribe_package_exports(name: str) -> None:
    mod = importlib.import_module("backend.transcribe")
    assert hasattr(mod, name), (
        f"backend.transcribe.{name} is missing — likely a re-export "
        "gap in backend/transcribe/__init__.py."
    )


@pytest.mark.parametrize("name", EXPECTED_METADATA_EXPORTS)
def test_metadata_package_exports(name: str) -> None:
    mod = importlib.import_module("backend.metadata")
    assert hasattr(mod, name), (
        f"backend.metadata.{name} is missing — likely a re-export gap "
        "in backend/metadata/__init__.py."
    )


def test_version_module() -> None:
    """APP_VERSION must follow single-decimal vX.Y format (rollover rule)."""
    from backend import version as v
    assert hasattr(v, "APP_VERSION")
    assert hasattr(v, "APP_VERSION_DATE")
    s = v.APP_VERSION
    assert s.startswith("v"), f"APP_VERSION must start with 'v', got {s!r}"
    major_minor = s[1:].split(".")
    assert len(major_minor) == 2, f"APP_VERSION must be vMAJOR.MINOR, got {s!r}"
    assert all(p.isdigit() for p in major_minor), \
        f"APP_VERSION parts must be digits, got {s!r}"
    minor = int(major_minor[1])
    assert 0 <= minor <= 9, (
        f"APP_VERSION minor must be 0-9 (single-decimal rollover rule), "
        f"got {s!r}"
    )
