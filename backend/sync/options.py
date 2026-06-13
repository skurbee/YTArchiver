"""Channel sync option normalization.

This is intentionally small: it extracts the front-of-sync_channel config
parsing and legacy min/max migration so the rest of the yt-dlp pipeline can
be carved apart with tests around stable input values.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PersistDurationMigration = Callable[[str, bool, bool], None]


@dataclass(slots=True)
class ChannelSyncOptions:
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
    """Return yt-dlp YYYYMMDD dateafter value, or empty when inactive."""
    if mode not in ("fromdate", "date") or not from_date:
        return ""
    date_value = from_date.replace("-", "").replace("/", "")
    if len(date_value) >= 8 and date_value[:8].isdigit():
        return date_value[:8]
    return ""


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
    """Return yt-dlp's archive-compatible output template."""
    ch_dir = Path(channel_dir)
    if split_years and split_months:
        return str(ch_dir
                   / "%(upload_date>%Y|Unknown Year)s"
                   / "%(upload_date>%m %B|Unknown Month)s"
                   / "%(title)s.%(ext)s")
    if split_years:
        return str(ch_dir
                   / "%(upload_date>%Y|Unknown Year)s"
                   / "%(title)s.%(ext)s")
    return str(ch_dir / "%(title)s.%(ext)s")


def _duration_seconds(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


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
    min_dur = _duration_seconds(channel.get("min_duration"))
    max_dur = _duration_seconds(channel.get("max_duration"))

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

    return ChannelSyncOptions(
        name=name,
        url=url,
        resolution=resolution,
        auto_transcribe=auto_tx,
        min_duration=min_dur,
        max_duration=max_dur,
        mode=(channel.get("mode") or "new").lower(),
        from_date=(channel.get("from_date") or "").strip(),
        split_years=bool(channel.get("split_years")),
        split_months=bool(channel.get("split_months")),
        migrated_min_duration=migrated_min,
        migrated_max_duration=migrated_max,
    )
