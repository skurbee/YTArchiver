"""
UI formatting helpers — short formatters used by the *_for_ui
functions in `ytarchiver_config.py`.

extracted from `ytarchiver_config.py` (931 lines).
That module had grown to include 500+ lines of view-model rendering
which doesn't belong in "config". This is the first step of pulling
the rendering helpers out.

These formatters have slightly different semantics from
`utils.format_*` (return empty string for missing values, narrower
KB/MB/GB tiers) — kept as-is so existing rendered UI is byte-identical.
A future patch can unify them once the UI side is also updated.

Public API (re-exported from ytarchiver_config so existing callers
keep working):
    _fmt_time_ago(ts) -> str          "10m ago" / "" if missing
    _fmt_size(raw) -> str             "120 MB" / "" if invalid
    _fmt_dur(raw) -> str              "M:SS" or "H:MM:SS" / "" if zero
    _extract_video_id(url) -> str     YouTube v= param
"""

from __future__ import annotations

import re
import time


def _fmt_time_ago(ts) -> str:
    """Mirror YTArchiver.py:32677 _fmt_time_ago.

    Treats None / 0 / empty as missing → empty string out (NOT "54
    years ago"). Returns just-now / Xm ago / Xh ago / Xd ago.
    """
    if not ts:
        return ""
    try:
        diff = time.time() - float(ts)
    except (TypeError, ValueError):
        return ""
    # Treat slightly-future timestamps (clock drift, DST fall-back) as
    # "just now" instead of empty. Old `diff <= 0` returned "" on the
    # 1-hour DST window so a newly-recorded download appeared with no
    # time-ago label (audit: view_format.py:29-49). Cap the
    # generous-clock-drift bucket at 60s past "now".
    if diff < -60:
        return ""
    if diff < 60:
        return "just now"
    if diff < 3600:
        return f"{int(diff // 60)}m ago"
    if diff < 86400:
        return f"{int(diff // 3600)}h ago"
    return f"{int(diff // 86400)}d ago"


def _fmt_size(raw) -> str:
    """Mirror YTArchiver.py:32686 _fmt_size. Accepts int or numeric string."""
    try:
        b = int(raw)
    except (TypeError, ValueError):
        return ""
    if b >= 1_073_741_824:
        return f"{b / 1_073_741_824:.1f} GB"
    if b >= 1_048_576:
        return f"{b / 1_048_576:.0f} MB"
    if b >= 1_024:
        return f"{b / 1_024:.0f} KB"
    return f"{b} B"


def _fmt_dur(raw) -> str:
    """Mirror YTArchiver.py:32697 _fmt_dur. Accepts int seconds or string."""
    try:
        s = int(raw)
    except (TypeError, ValueError):
        return ""
    if s <= 0:
        return ""
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


_VIDEO_ID_RE = re.compile(r"[?&]v=([A-Za-z0-9_-]{11})")
# Additional patterns for short-form YT URLs that don't carry ?v=.
# Without these, recent_downloads entries with manually-pasted
# `youtu.be/<id>` or `/watch/<id>` URLs lost thumbnail resolution
# (audit: view_format.py:80).
_VIDEO_ID_RE_YOUTU_BE = re.compile(r"youtu\.be/([A-Za-z0-9_-]{11})")
_VIDEO_ID_RE_WATCH_PATH = re.compile(r"/watch/([A-Za-z0-9_-]{11})")
_VIDEO_ID_RE_SHORTS = re.compile(r"/shorts/([A-Za-z0-9_-]{11})")


def _extract_video_id(video_url: str) -> str:
    """Parse the YouTube video id from any of the URL forms:
        watch?v=XXXX, youtu.be/XXXX, /watch/XXXX, /shorts/XXXX.
    Returns "" if not found.
    """
    if not video_url:
        return ""
    for _rx in (_VIDEO_ID_RE, _VIDEO_ID_RE_YOUTU_BE,
                _VIDEO_ID_RE_WATCH_PATH, _VIDEO_ID_RE_SHORTS):
        m = _rx.search(video_url)
        if m:
            return m.group(1)
    return ""
