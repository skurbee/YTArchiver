"""
Shared module-level names for every api_mixins/*.py file.

The mixin classes were extracted from main.py's Api class in an
earlier refactor. Their method bodies reference module-level names
(stdlib aliases, backend module imports, the `_log` logger, helper
functions) that used to be visible from main.py's globals but aren't
visible inside the mixin files.

This module gathers all those names in one place. Each mixin file
does:
    from ._shared import *  # noqa: F401,F403

and gets the union of imports + helper names available.

CAUTION: do NOT import anything from `main` here — that would create
a circular import (main.py imports the mixin package). All names are
imported from backend.* modules or stdlib.

For a tour of the mixin pattern, the implicit `self.<attr>` contracts,
and how to add a new method, see `backend/api_mixins/README.md`.
"""
# stdlib — used widely in moved methods
import ctypes  # noqa: F401
import json  # noqa: F401
import os  # noqa: F401
import re  # noqa: F401

# Optional stdlib that some methods import inside their bodies; pre-
# importing here is harmless but cheaper.
import shutil  # noqa: F401
import subprocess  # noqa: F401
import sys  # noqa: F401
import threading  # noqa: F401
import time  # noqa: F401
import urllib.request  # noqa: F401
from datetime import datetime, timedelta  # noqa: F401
from pathlib import Path  # noqa: F401
from typing import Any, Dict, List, Optional, Tuple  # noqa: F401,UP035

# Third-party
try:
    import webview  # noqa: F401
except ImportError:
    webview = None  # type: ignore

# Backend modules (same aliases main.py uses, so method bodies that
# refer to `sync_backend.xxx`, `metadata_backend.yyy`, etc., resolve).
from backend import archive_scan  # noqa: F401
from backend import autorun as autorun_backend  # noqa: F401
from backend import compress as compress_backend  # noqa: F401
from backend import index as index_backend  # noqa: F401
from backend import metadata as metadata_backend  # noqa: F401
from backend import net as net_backend  # noqa: F401
from backend import reorg as reorg_backend  # noqa: F401
from backend import subs as subs_backend  # noqa: F401
from backend import sync as sync_backend  # noqa: F401
from backend import window_state as winstate  # noqa: F401

# The logger that Patch 3 wired up. Method bodies emit `_log.debug(...)`
# to surface previously-silent exception swallows.
from backend.log import get_logger as _get_logger, swallow  # noqa: F401
from backend.log_stream import LogStreamer  # noqa: F401
from backend.queues import QueueState  # noqa: F401
from backend.transcribe import TranscribeManager  # noqa: F401
from backend.tray import TrayController  # noqa: F401

# Version constants — same source as main.py reads from.
from backend.version import APP_VERSION, APP_VERSION_DATE  # noqa: F401
from backend.ytarchiver_config import (  # noqa: F401
    CONFIG_FILE,
    autorun_history_entries_for_ui,
    backup_config_on_start,
    channels_for_subs_ui,
    config_file_exists,
    config_is_writable,
    load_config,
    recent_for_ui,
    save_config,
)

_log = _get_logger("main")  # share the same logger name as main.py

ALLOWED_REDOWNLOAD_RESOLUTIONS = (
    "best", "2160", "1440", "1080", "720", "480", "360", "240", "144"
)


def _api_err(code: str, message: str, *,
             details: dict | None = None,
             retryable: bool = False) -> dict:
    """Build a typed bridge-API error response.

    Returns the standard error envelope understood by the frontend.
    The "error" key mirrors "message" so existing JS call sites that
    check res.error continue to work without changes while new code
    can key on res.code / res.retryable for conditional handling.

    Defined codes (extend as needed):
      BACKUP_WRITE_FAILED   — export to disk failed
      BACKUP_READ_FAILED    — import / restore read failed
      CONFIG_READ_ONLY      — config file cannot be written
      CONFIG_SAVE_FAILED    — save_config raised
      SYNC_ALREADY_RUNNING  — duplicate sync start attempt
      DRIVE_NOT_WRITABLE    — archive root not writable
      MISSING_DEPENDENCY    — yt-dlp / ffmpeg absent
      TRANSCRIBE_FAILED     — whisper/GPU pipeline error
      METADATA_FAILED       — metadata refresh error
      FILE_NOT_FOUND        — path does not exist
      CANCELLED             — operation cancelled by user
      INTERNAL_ERROR        — unexpected exception
    """
    return {
        "ok": False,
        "code": code,
        "message": message,
        "error": message,          # backward-compat alias
        "details": details or {},
        "retryable": retryable,
    }


def normalize_dialog_paths(paths):
    """Return the first selected pywebview dialog path, or None on cancel."""
    if not paths:
        return None
    if isinstance(paths, str):
        return paths or None
    try:
        if len(paths) == 0:
            return None
        first = paths[0]
        if isinstance(first, str) and first:
            return first
    except (TypeError, IndexError):
        return None
    return None


# Star-import friendly: explicit re-export list including underscore
# names that `from ._shared import *` would otherwise skip.
__all__ = [
    # stdlib aliases
    "ctypes", "json", "os", "re", "sys", "threading", "time",
    "urllib", "datetime", "timedelta", "Path",
    "Any", "Dict", "List", "Optional", "Tuple",
    "shutil", "subprocess", "webview",
    # config helpers
    "load_config", "save_config", "config_file_exists",
    "config_is_writable", "CONFIG_FILE",
    "channels_for_subs_ui", "recent_for_ui",
    "autorun_history_entries_for_ui", "backup_config_on_start",
    # backend module aliases
    "subs_backend", "archive_scan", "sync_backend", "metadata_backend",
    "index_backend", "compress_backend", "reorg_backend", "winstate",
    "autorun_backend", "net_backend",
    # shared helpers
    "_api_err", "normalize_dialog_paths", "ALLOWED_REDOWNLOAD_RESOLUTIONS",
    # classes
    "TrayController", "LogStreamer", "TranscribeManager", "QueueState",
    # logger
    "_log", "_get_logger", "swallow",
    # version
    "APP_VERSION", "APP_VERSION_DATE",
]
