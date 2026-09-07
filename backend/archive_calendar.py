"""Search and Graph calendar policy independent of the computer timezone.

Precise timestamps use UTC. YouTube date-only metadata retains its original
calendar day using the existing noon-UTC storage representation; no missing
original date is inferred from a timestamp or local timezone.
"""

from datetime import UTC, datetime


def upload_date_epoch(value) -> float:
    raw = str(value or "").strip()
    if len(raw) != 8 or not raw.isdigit():
        return 0.0
    try:
        return datetime(int(raw[:4]), int(raw[4:6]), int(raw[6:]),
                        12, tzinfo=UTC).timestamp()
    except (ValueError, OSError):
        return 0.0


def year_start_epoch(year: int) -> int:
    return int(datetime(int(year), 1, 1, tzinfo=UTC).timestamp())


def calendar_bucket(timestamp: float, bucket: str) -> str:
    value = datetime.fromtimestamp(float(timestamp), UTC)
    if bucket == "week":
        iso = value.isocalendar()
        return f"{iso.year:04d}-W{iso.week:02d}"
    return value.strftime({"year": "%Y", "month": "%Y-%m", "day": "%Y-%m-%d"}[bucket])


def calendar_sql(timestamp_expression: str, bucket: str) -> str:
    """SQL adapter for trusted query expressions, never user-provided SQL."""
    fmt = {"year": "%Y", "month": "%Y-%m", "day": "%Y-%m-%d"}[bucket]
    return f"strftime('{fmt}', {timestamp_expression}, 'unixepoch')"
