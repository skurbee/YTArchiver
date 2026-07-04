"""Archive-drive capacity warning helpers."""
from __future__ import annotations

import os
import shutil
from typing import Any

DEFAULT_CAPACITY_WARNING_MODE = "percent"
DEFAULT_CAPACITY_WARNING_PERCENT = 90
DEFAULT_CAPACITY_WARNING_FREE_GB = 100


def _coerce_int(value: Any, default: int, low: int, high: int) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        n = default
    return max(low, min(high, n))


def normalize_archive_capacity_warning(cfg: dict[str, Any] | None) -> dict[str, int | str]:
    """Return validated archive-capacity warning settings."""
    cfg = cfg or {}
    mode = str(cfg.get("archive_capacity_warning_mode")
               or DEFAULT_CAPACITY_WARNING_MODE)
    if mode not in ("percent", "free_gb"):
        mode = DEFAULT_CAPACITY_WARNING_MODE
    return {
        "mode": mode,
        "percent": _coerce_int(
            cfg.get("archive_capacity_warning_percent"),
            DEFAULT_CAPACITY_WARNING_PERCENT,
            1,
            100,
        ),
        "free_gb": _coerce_int(
            cfg.get("archive_capacity_warning_free_gb"),
            DEFAULT_CAPACITY_WARNING_FREE_GB,
            1,
            1_000_000,
        ),
    }


def archive_capacity_status(path: str, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Inspect the archive root and classify capacity as ok/warning/fail."""
    base = (path or "").strip()
    settings = normalize_archive_capacity_warning(cfg)
    if not base:
        return {
            "ok": False,
            "status": "fail",
            "detail": "Not configured (Settings > Archive root)",
            "settings": settings,
        }
    if not os.path.isdir(base):
        return {
            "ok": False,
            "status": "fail",
            "detail": "(missing)",
            "settings": settings,
        }

    total, used, free = shutil.disk_usage(base)
    free_gb = free / (1024 ** 3)
    percent_full = (used / total * 100.0) if total else 0.0
    if settings["mode"] == "free_gb":
        threshold = int(settings["free_gb"])
        warning = free_gb <= threshold
        threshold_text = f"warning at <= {threshold} GB free"
    else:
        threshold = int(settings["percent"])
        warning = percent_full >= threshold
        threshold_text = f"warning at >= {threshold}% full"

    drive = os.path.splitdrive(base)[0]
    display_base = f"{drive}\\..." if drive else base
    detail = (
        f"{display_base} - {free_gb:.0f} GB free "
        f"({percent_full:.0f}% full) - {threshold_text}"
    )
    return {
        "ok": True,
        "status": "warning" if warning else "ok",
        "detail": detail,
        "free_gb": free_gb,
        "percent_full": percent_full,
        "settings": settings,
    }
