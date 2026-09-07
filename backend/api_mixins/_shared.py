"""Small shared bridge helpers; feature modules import their own dependencies.

Keep this module free of application owners and orchestration imports. New
domain behavior belongs in a service with explicit constructor dependencies.
"""
from __future__ import annotations

from typing import Any, Literal, TypedDict

from backend.log import get_logger

_log = get_logger("main")


class BridgeError(TypedDict):
    ok: Literal[False]
    code: str
    message: str
    error: str
    details: dict[str, Any]
    retryable: bool


ALLOWED_REDOWNLOAD_RESOLUTIONS = (
    "best", "2160", "1440", "1080", "720", "480", "360", "240", "144"
)


def _api_err(code: str, message: str, *,
             details: dict | None = None,
             retryable: bool = False) -> BridgeError:
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


__all__ = [
    "ALLOWED_REDOWNLOAD_RESOLUTIONS", "BridgeError", "_api_err", "_log",
    "normalize_dialog_paths",
]
