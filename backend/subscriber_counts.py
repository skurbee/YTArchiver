"""Background subscriber-count recovery for configured channel URLs."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from . import archive_scan
from .log import get_logger
from .process_runner import YtDlpRunner
from .subs import normalize_channel_url
from .sync import _find_cookie_source

_log = get_logger(__name__)

MAX_FAILURES = 3
_CHANNEL_TAB_RE = re.compile(
    r"/(?:videos|shorts|streams|playlists|community|podcasts|channels|"
    r"featured|about|store|releases)/?$",
    re.IGNORECASE,
)
_TRANSIENT_MARKERS = (
    "traffic budget",
    "traffic governor",
    "rate limit",
    "too many requests",
    "http error 429",
    "timeout",
    "timed out",
    "yt-dlp not found",
    "launch failed",
    "unable to download",
    "network is unreachable",
    "temporary failure",
    "connection reset",
    "connection aborted",
    "sign in",
    "cookies",
    "does not have a videos tab",
    "does not have a streams tab",
)


def _channel_url(url: str) -> str:
    """Use the channel root, which also supports streams-only channels."""
    base = normalize_channel_url(url).strip().rstrip("/")
    base = _CHANNEL_TAB_RE.sub("", base).rstrip("/")
    return base


def _excluded_for_missing_tab(record: dict[str, Any]) -> bool:
    error = str(record.get("subscriber_fetch_last_error") or "").lower()
    return "does not have a videos tab" in error or "does not have a streams tab" in error


def _parse_count(stdout: str) -> int | None:
    for line in (stdout or "").splitlines():
        raw = line.strip().replace(",", "")
        if not raw or raw.upper() in {"NA", "NONE", "NULL"}:
            continue
        try:
            count = int(float(raw))
        except (TypeError, ValueError):
            continue
        if count >= 0:
            return count
    return None


def _is_transient(error: str) -> bool:
    low = (error or "").lower()
    return any(marker in low for marker in _TRANSIENT_MARKERS)


def fetch_subscriber_count(url: str, *, runner: YtDlpRunner | None = None,
                           timeout: float = 45.0) -> dict[str, Any]:
    """Fetch one channel's follower count through its configured URL.

    Read the channel-level field from yt-dlp's channel-root result first. Some
    individual videos omit ``channel_follower_count`` even though the channel
    page exposes it, so treating the newest video's ``NA`` as a channel-level
    miss produces false failures. Fall back to the newest video's metadata for
    extractors where the channel-level field is absent. Public probes run
    first; cookies are only used when a public request itself fails.
    """
    target = _channel_url(url)
    if not target:
        return {"ok": False, "transient": False, "error": "missing URL"}
    runner = runner or YtDlpRunner(cookie_provider=_find_cookie_source)
    if not runner.binary():
        return {"ok": False, "transient": True,
                "error": "yt-dlp not found"}

    def _channel_argv(include_cookies: bool) -> list[str]:
        return runner.build_argv(
            "--flat-playlist",
            "--playlist-end", "1",
            "--skip-download",
            "--print", "playlist:%(channel_follower_count)s",
            target,
            include_cookies=include_cookies,
        )

    def _video_argv(include_cookies: bool) -> list[str]:
        return runner.build_argv(
            "--playlist-end", "1",
            "--skip-download",
            "--print", "%(channel_follower_count)s",
            target,
            include_cookies=include_cookies,
        )

    def _probe(argv: list[str]) -> tuple[int, str, str]:
        return runner.run_capture(
            argv, timeout=timeout,
            traffic_kind="subscriber_count_probe")

    errors: list[str] = []
    rc = 0
    for argv_builder in (_channel_argv, _video_argv):
        public_argv = argv_builder(False)
        rc, stdout, stderr = _probe(public_argv)
        count = _parse_count(stdout)
        if rc == 0 and count is not None:
            return {"ok": True, "count": count}
        errors.extend(
            part.strip() for part in (stderr, stdout) if part.strip())

        # Match the rest of YTArchiver's YouTube policy: cosmetic probes do
        # not attach browser cookies unless the public request itself failed.
        if rc != 0:
            cookie_argv = argv_builder(True)
            if cookie_argv != public_argv:
                rc, stdout, stderr = _probe(cookie_argv)
                count = _parse_count(stdout)
                if rc == 0 and count is not None:
                    return {"ok": True, "count": count}
                errors.extend(
                    part.strip() for part in (stderr, stdout)
                    if part.strip())

    error = " | ".join(errors)[-500:] or (
        "subscriber count unavailable" if rc == 0
        else f"yt-dlp exited with code {rc}")
    return {"ok": False, "transient": _is_transient(error),
            "error": error}


def backfill_missing_counts(
        channels: list[dict[str, Any]], *,
        runner: YtDlpRunner | None = None,
        max_failures: int = MAX_FAILURES,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, int]:
    """Probe every eligible missing channel once for this app launch."""
    runner = runner or YtDlpRunner(cookie_provider=_find_cookie_source)
    if not runner.binary():
        return {"eligible": 0, "updated": 0, "failed": 0,
                "excluded": 0, "deferred": 0}

    initial_cache = archive_scan.load_disk_cache()
    eligible = []
    for channel in channels or []:
        url = str(channel.get("url") or "").strip()
        rec = initial_cache.get(url)
        if not url or not isinstance(rec, dict):
            continue
        if rec.get("subscriber_count") is not None:
            continue
        failures = int(rec.get("subscriber_fetch_failures") or 0)
        if ((rec.get("subscriber_fetch_excluded") or failures >= max_failures)
                and not _excluded_for_missing_tab(rec)):
            continue
        eligible.append((url, channel.get("name") or channel.get("folder") or ""))

    result = {"eligible": len(eligible), "updated": 0, "failed": 0,
              "excluded": 0, "deferred": 0}
    for index, (url, name) in enumerate(eligible, 1):
        # Another worker or a freshly downloaded sidecar may have filled the
        # value while this slow, traffic-spaced pass was running.
        current = archive_scan.load_disk_cache().get(url) or {}
        if current.get("subscriber_count") is not None:
            continue
        if ((current.get("subscriber_fetch_excluded")
                or int(current.get("subscriber_fetch_failures") or 0)
                >= max_failures) and not _excluded_for_missing_tab(current)):
            continue

        fetched = fetch_subscriber_count(url, runner=runner)
        if fetched.get("ok"):
            if archive_scan.record_subscriber_fetch_success(
                    url, int(fetched["count"])):
                result["updated"] += 1
        elif fetched.get("transient"):
            # A traffic/network/session failure is not evidence that this
            # specific channel lacks the field. Stop the launch pass so one
            # outage cannot burn all channels' three-strike allowance.
            result["deferred"] += len(eligible) - index + 1
            break
        else:
            state = archive_scan.record_subscriber_fetch_failure(
                url, str(fetched.get("error") or ""), max_failures)
            result["failed"] += 1
            if state.get("excluded"):
                result["excluded"] += 1

        if progress_cb:
            try:
                progress_cb({"index": index, "total": len(eligible),
                             "name": name, **result})
            except Exception as exc:
                _log.debug("subscriber progress callback failed: %s", exc)
    return result
