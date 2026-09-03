"""Channel sync option normalization.

This is intentionally small: it extracts the front-of-sync_channel config
parsing and legacy min/max migration so the rest of the yt-dlp pipeline can
be carved apart with tests around stable input values.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PersistDurationMigration = Callable[[str, bool, bool], None]


@dataclass(slots=True)
class ChannelSyncOptions:
    """Normalized per-channel sync settings consumed by sync_channel."""

    name: str
    url: str
    resolution: str
    auto_transcribe: bool
    min_duration: int
    max_duration: int
    mode: str
    from_date: str
    split_years: bool
    split_months: bool
    migrated_min_duration: bool = False
    migrated_max_duration: bool = False


def normalized_date_after(mode: str, from_date: str) -> str:
    """Return yt-dlp's YYYYMMDD value, rejecting an active invalid date."""
    if mode not in ("fromdate", "date"):
        return ""
    canonical = str(from_date or "").strip().replace("/", "-")
    try:
        parsed = dt.date.fromisoformat(canonical)
    except ValueError as exc:
        raise ValueError(
            "From-date sync needs a valid year, month, and day."
        ) from exc
    return parsed.strftime("%Y%m%d")


def build_match_filter(min_duration: int, max_duration: int) -> str:
    """Build yt-dlp's liveness + duration match filter expression."""
    match_parts = ["!is_live", "!is_upcoming"]
    if min_duration > 0:
        match_parts.append(f"duration>?{int(min_duration)}")
    if max_duration > 0:
        match_parts.append(f"duration<?{int(max_duration)}")
    return " & ".join(match_parts)


def build_output_template(channel_dir: str | Path,
                          split_years: bool,
                          split_months: bool) -> str:
    """Return yt-dlp's collision-safe, archive-compatible output template.

    Downloads first land with their video ID attached.  The sync commit path
    removes that suffix when the ordinary title-only destination is free and
    keeps it only for real duplicate-title collisions.  Limiting the title
    field itself preserves the ID through yt-dlp's 200-byte filename cap.
    """
    ch_dir = Path(channel_dir)
    # Keep the filename component below 200 bytes after the 14-byte
    # `` [video-id]`` marker and the longest sidecars we ask yt-dlp to create
    # (for example ``.live_chat.json``).  Do not combine this with yt-dlp's
    # ``--trim-filenames`` option: yt-dlp applies that limit to the expanded
    # absolute path from the left, so a long parent directory can trim the
    # protective ID itself.  Precision on the title field is component-local.
    filename = "%(title).170B [%(id)s].%(ext)s"
    if split_years and split_months:
        return str(ch_dir
                   / "%(upload_date>%Y|Unknown Year)s"
                   / "%(upload_date>%m %B|Unknown Month)s"
                   / filename)
    if split_years:
        return str(ch_dir
                   / "%(upload_date>%Y|Unknown Year)s"
                   / filename)
    return str(ch_dir / filename)


def _duration_seconds(value: Any, label: str) -> int:
    """Return one stored duration without silently disabling bad limits."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return 0
    if isinstance(value, bool):
        raise ValueError(f"{label} length must be a whole number of seconds.")
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{label} length must be a whole number of seconds.")
    try:
        duration = int(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise ValueError(
            f"{label} length must be a whole number of seconds."
        ) from exc
    if duration < 0:
        raise ValueError(f"{label} length cannot be negative.")
    return duration


def normalize_channel_sync_options(
        channel: dict[str, Any],
        *,
        stream=None,
        persist_migration: PersistDurationMigration | None = None,
) -> ChannelSyncOptions:
    """Return the normalized option bundle used by `sync_channel`.

    YTArchiver stores min/max duration as seconds. Values between 1 and 59
    are legacy v1 seconds and are upgraded to 60 so they keep acting like a
    real lower/upper bound instead of collapsing to zero minutes in the UI.
    When migration happens, the supplied callback is responsible for
    persisting the upgraded values to config.
    """
    name = channel.get("name") or channel.get("folder") or "?"
    url = (channel.get("url") or "").strip()
    resolution = str(channel.get("resolution", "720") or "720")
    auto_tx = bool(channel.get("auto_transcribe"))
    min_dur = _duration_seconds(channel.get("min_duration"), "Minimum")
    max_dur = _duration_seconds(channel.get("max_duration"), "Maximum")
    if min_dur > 0 and max_dur > 0 and min_dur > max_dur:
        raise ValueError(
            f"{name}: minimum length cannot be greater than maximum length."
        )

    migrated_min = False
    migrated_max = False
    if 0 < min_dur < 60:
        try:
            if stream is not None:
                stream.emit_dim(
                    f" (legacy min_duration {min_dur}s upgraded to 60s)")
        except Exception:
            pass
        min_dur = 60
        migrated_min = True
    if 0 < max_dur < 60:
        try:
            if stream is not None:
                stream.emit_dim(
                    f" (legacy max_duration {max_dur}s upgraded to 60s)")
        except Exception:
            pass
        max_dur = 60
        migrated_max = True

    if migrated_min:
        channel["min_duration"] = 60
    if migrated_max:
        channel["max_duration"] = 60
    if (migrated_min or migrated_max) and persist_migration is not None:
        persist_migration(url, migrated_min, migrated_max)

    mode = (channel.get("mode") or "new").lower()
    from_date = (channel.get("from_date")
                 or channel.get("date_after") or "").strip()
    # Validate here, before yt-dlp starts. Older or externally-edited config
    # files must not turn an invalid From-date into an unrestricted download.
    if mode in ("fromdate", "date"):
        normalized_date_after(mode, from_date)

    return ChannelSyncOptions(
        name=name,
        url=url,
        resolution=resolution,
        auto_transcribe=auto_tx,
        min_duration=min_dur,
        max_duration=max_dur,
        mode=mode,
        from_date=from_date,
        split_years=bool(channel.get("split_years")),
        split_months=bool(channel.get("split_months")),
        migrated_min_duration=migrated_min,
        migrated_max_duration=migrated_max,
    )
