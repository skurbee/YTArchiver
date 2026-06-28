"""Human-readable formatting utilities (split from utils.py)."""
from __future__ import annotations

import time


def format_bytes(n: int, dash_if_zero: bool = True) -> str:
    """Pretty-print a byte count. 0 → '—' when dash_if_zero."""
    n = int(n or 0)
    if n <= 0 and dash_if_zero:
        return "—"
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    i = 0
    v = float(n)
    while v >= 1024 and i < len(units) - 1:
        v /= 1024.0
        i += 1
    if i == 0:
        return f"{int(v)} {units[i]}"
    return f"{v:.{1 if i >= 2 else 0}f} {units[i]}"


def format_duration_hms(secs: float) -> str:
    """Format seconds as `H:MM:SS` or `MM:SS` (when under 1 hour)."""
    try:
        s = int(float(secs or 0))
    except (TypeError, ValueError):
        return "0:00"
    if s < 0:
        s = 0
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def format_elapsed(secs: float) -> str:
    """Compact elapsed-time format for in-line log strings.

    Rules (always fold into Xm XXs):
      < 60s      -> "Xs"            ("47s")
      < 1h       -> "Xm YYs"        ("3m 21s", zero-padded seconds)
      >= 1h      -> "Xh Ym YYs"     ("1h 5m 03s")

    Use this anywhere the user-facing log would otherwise show raw
    seconds for a duration (heartbeats, "took N", elapsed counters).
    Never emit bare "201s" — fold via this helper instead.
    """
    try:
        s = int(float(secs or 0))
    except (TypeError, ValueError):
        return "0s"
    if s < 0:
        s = 0
    if s < 60:
        return f"{s}s"
    h, rem = divmod(s, 3600)
    m, ss = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {ss:02d}s"
    return f"{m}m {ss:02d}s"


def format_enc_size(mb: float) -> str:
    """Format a megabyte count for encode progress display (e.g. '1.23 GB')."""
    try:
        mb = float(mb)
    except (TypeError, ValueError):
        return "—"
    if mb >= 1024:
        return f"{mb / 1024:.2f} GB"
    return f"{mb:.1f} MB"


def fmt_time_ago(ts: float) -> str:
    """Human-friendly 'N min ago' style for Recent tab. ts is Unix epoch."""
    try:
        age = max(0.0, time.time() - float(ts))
    except (TypeError, ValueError):
        return ""
    if age < 60: return "just now"
    if age < 3600: return f"{int(age / 60)} min ago"
    if age < 86400: return f"{int(age / 3600)} h ago"
    if age < 86400 * 30: return f"{int(age / 86400)} d ago"
    if age < 86400 * 365: return f"{int(age / 2592000)} mo ago"
    return f"{int(age / 31536000)} yr ago"
