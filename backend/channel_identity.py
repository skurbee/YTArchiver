"""Permanent YouTube channel identity and verified handle recovery.

YouTube display names and ``@handles`` are mutable.  A channel's ``UC...``
identifier is the durable identity, so automatic URL changes are allowed only
when yt-dlp proves that exact ID.  Healthy syncs learn the ID from a
playlist-scoped marker in their existing yt-dlp process.  A legacy stale
handle uses two previously cached video IDs plus a direct candidate-URL check
before any config is changed.
"""
from __future__ import annotations

import json
import re
import subprocess
import time
from pathlib import Path
from typing import Any

from . import channel_cache, youtube_traffic
from .log import get_logger
from .process_runner import (
    PROCESS_REGISTRY,
    popen_ytdlp,
    supervise_streaming_process,
)
from .subprocess_util import (
    make_startupinfo,
    subprocess_creationflags,
    utf8_env,
)

_log = get_logger(__name__)

_CHANNEL_ID_RE = re.compile(r"UC[A-Za-z0-9_-]{22}")
_VIDEO_ID_RE = re.compile(r"[A-Za-z0-9_-]{11}")
_TAB_SUFFIX_RE = re.compile(
    r"/(videos|shorts|streams|playlists|community|podcasts|channels|"
    r"featured|about)$",
    re.IGNORECASE,
)
_TRACK_PREFIX = "CHTRACK:::"
_VIDEO_PREFIX = "VIDTRACK:::"
_VERIFY_PREFIX = "CHVERIFY:::"
_VERIFY_PRINT = "CHVERIFY:::%(channel_id)s:::%(uploader_url)s"

CHANNEL_TRACK_PRINT = (
    "playlist:CHTRACK:::%(channel_id)s:::%(uploader_id)s:::"
    "%(uploader_url)s:::%(channel)s"
)


def _clean_channel_id(value: object) -> str:
    text = str(value or "").strip()
    return text if _CHANNEL_ID_RE.fullmatch(text) else ""


def _clean_video_id(value: object) -> str:
    text = str(value or "").strip()
    return text if _VIDEO_ID_RE.fullmatch(text) else ""


def is_channel_page_unavailable_error(text: str) -> bool:
    """Identify yt-dlp's channel-page failure disclaimer.

    This message is not an authentication verdict: youtube:tab emits it after
    a failed webpage request such as an obsolete handle returning HTTP 404.
    """
    low = str(text or "").lower()
    generic_disclaimer = (
        "playlists that require authentication" in low
        and "successful webpage download" in low
    )
    tab_page_failure = "[youtube:tab]" in low and any(
        marker in low for marker in (
            "http error 404",
            "this channel does not exist",
            "failed to resolve url",
        )
    )
    return generic_disclaimer or tab_page_failure


def _split_track(text: str, prefix: str, fields: int) -> list[str] | None:
    raw = str(text or "").strip()
    if not raw.startswith(prefix):
        return None
    parts = raw[len(prefix):].split(":::", fields - 1)
    if len(parts) != fields:
        return None
    return [part.strip() for part in parts]


def parse_channel_track_line(text: str) -> dict[str, str] | None:
    """Parse the channel-scoped marker emitted in a normal sync process."""
    parts = _split_track(text, _TRACK_PREFIX, 4)
    if parts is None:
        return None
    channel_id, uploader_id, uploader_url, channel_name = parts
    channel_id = _clean_channel_id(channel_id)
    if not channel_id:
        return None
    return {
        "channel_id": channel_id,
        "uploader_id": "" if uploader_id == "NA" else uploader_id,
        "uploader_url": "" if uploader_url == "NA" else uploader_url,
        "channel_name": "" if channel_name == "NA" else channel_name,
    }


def _parse_video_track_line(text: str) -> dict[str, str] | None:
    parts = _split_track(text, _VIDEO_PREFIX, 5)
    if parts is None:
        return None
    video_id, channel_id, uploader_id, uploader_url, channel_name = parts
    video_id = _clean_video_id(video_id)
    channel_id = _clean_channel_id(channel_id)
    if not video_id or not channel_id:
        return None
    return {
        "video_id": video_id,
        "channel_id": channel_id,
        "uploader_id": "" if uploader_id == "NA" else uploader_id,
        "uploader_url": "" if uploader_url == "NA" else uploader_url,
        "channel_name": "" if channel_name == "NA" else channel_name,
    }


def _normalized_candidate_url(value: object) -> str:
    from . import subs

    raw = str(value or "").strip()
    if not raw or raw == "NA":
        return ""
    normalized = subs.normalize_channel_url(raw).rstrip("/")
    normalized = _TAB_SUFFIX_RE.sub("", normalized)
    ok, _error = subs.validate_channel_url(normalized)
    return normalized if ok else ""


def _known_video_ids(channel: dict[str, Any], limit: int = 5) -> list[str]:
    """Collect only video IDs durably bound to this exact configured URL."""
    url = str(channel.get("url") or "").strip()
    found: list[str] = []
    for value in channel_cache.get_known_ids(url, limit=limit):
        video_id = _clean_video_id(value)
        if video_id and video_id not in found:
            found.append(video_id)
    return found[:limit]


def _archive_video_ids(
        archive_folder: str | Path | None, limit: int = 5) -> list[str]:
    """Return IDs tied to media inside one exact channel folder.

    This is a migration fallback for subscriptions saved before YTArchiver
    started recording permanent channel IDs and per-URL ID caches. The shared
    metadata scanner corroborates filename candidates with the catalog and
    yt-dlp sidecars, while the explicit folder boundary prevents one channel's
    history from being borrowed by another.
    """
    if not archive_folder:
        return []
    try:
        folder = Path(archive_folder)
        if not folder.is_dir():
            return []
        from .metadata.scan import _scan_channel_videos

        rows = _scan_channel_videos(folder)
    except Exception as exc:
        _log.debug("legacy channel identity archive scan failed: %s", exc)
        return []

    found: list[str] = []
    for row in rows:
        video_id = _clean_video_id(row[0] if row else "")
        if video_id and video_id not in found:
            found.append(video_id)
            if len(found) >= limit:
                break
    return found


def _channel_claims_existing_history(channel: dict[str, Any]) -> bool:
    """Return whether a legacy row represents prior archive work."""
    if any(channel.get(key) for key in (
            "initialized", "init_complete", "sync_complete", "last_sync",
            "failed_video_ids")):
        return True
    for key in ("n_vids", "size_bytes", "size_gb", "batch_resume_index"):
        try:
            if float(channel.get(key) or 0) > 0:
                return True
        except (TypeError, ValueError):
            continue
    return False


def _probe_context(cancel_event=None, pause_event=None, stream=None,
                   task_id: str = "") -> dict[str, Any]:
    from . import sync as sync_backend

    yt = sync_backend.find_yt_dlp()
    if not yt:
        return {"ok": False, "error": "yt-dlp not found"}
    try:
        cookies = list(sync_backend._find_cookie_source() or [])
    except Exception:
        cookies = []
    return {
        "ok": True,
        "yt": str(yt),
        "cookies": cookies,
        "cancel_event": cancel_event,
        "pause_event": pause_event,
        "stream": stream,
        "task_id": str(task_id or ""),
    }


class _ProbeStopToken:
    """Event-shaped view that treats Cancel, Pause, or Skip as a stop."""

    def __init__(self, context: dict[str, Any]) -> None:
        self._context = context

    def is_set(self) -> bool:
        return bool(
            (self._context.get("cancel_event") is not None
             and self._context["cancel_event"].is_set())
            or (self._context.get("pause_event") is not None
                and self._context["pause_event"].is_set())
            or (self._context.get("kill_current") is not None
                and self._context["kill_current"].is_set())
        )

    def wait(self, timeout: float | None = None) -> bool:
        """Match ``threading.Event.wait`` while observing all three events."""
        deadline = (
            None if timeout is None
            else time.monotonic() + max(0.0, float(timeout))
        )
        while not self.is_set():
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                time.sleep(min(0.05, remaining))
            else:
                time.sleep(0.05)
        return True


def _probe_stop_result(context: dict[str, Any]) -> dict[str, Any] | None:
    cancelled = bool(
        context.get("cancel_event") is not None
        and context["cancel_event"].is_set()
    )
    paused = bool(
        context.get("pause_event") is not None
        and context["pause_event"].is_set()
    )
    skipped = bool(
        context.get("kill_current") is not None
        and context["kill_current"].is_set()
    )
    if not (cancelled or paused or skipped):
        return None
    return {
        "ok": False,
        "cancelled": cancelled,
        "paused": paused,
        "skipped": skipped,
        "error": "channel identity verification was stopped",
    }


def _run_identity_probe(
        command: list[str], *, context: dict[str, Any], timeout: float,
        traffic_kind: str, units: int = 1) -> dict[str, Any]:
    """Run one registered UTF-8 probe that Pause/Cancel can interrupt."""
    stopped = _probe_stop_result(context)
    if stopped is not None:
        return stopped
    permission = youtube_traffic.acquire(
        traffic_kind,
        units=units,
        cancel_event=_ProbeStopToken(context),
        pause_event=context.get("pause_event"),
        stream=context.get("stream"),
    )
    if not permission.get("ok"):
        stopped = _probe_stop_result(context)
        if stopped is not None:
            return stopped
        return {
            "ok": False,
            "cancelled": bool(permission.get("cancelled")),
            "paused": bool(permission.get("paused")),
            "error": str(permission.get("error") or "probe was not permitted"),
            "stdout": "",
            "stderr": "",
        }
    stopped = _probe_stop_result(context)
    if stopped is not None:
        return stopped

    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    owner = "sync"
    task_id = str(context.get("task_id") or "")
    role = "channel-identity"
    try:
        proc = popen_ytdlp(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            startupinfo=make_startupinfo(),
            creationflags=subprocess_creationflags(),
            env=utf8_env(),
            registry=PROCESS_REGISTRY,
            owner=owner,
            task_id=task_id,
            role=role,
        )
    except OSError as exc:
        return {
            "ok": False,
            "error": f"launch failed: {exc}",
            "stdout": "",
            "stderr": "",
        }
    try:
        result = supervise_streaming_process(
            proc,
            registry=PROCESS_REGISTRY,
            on_stdout_line=stdout_lines.append,
            on_stderr_line=stderr_lines.append,
            cancel_event=_ProbeStopToken(context),
            timeout=timeout,
            owner=owner,
            task_id=task_id,
            role=role,
        )
    except Exception as exc:
        try:
            PROCESS_REGISTRY.terminate_process(proc, timeout=2.0)
        except Exception as cleanup_exc:
            _log.debug("channel identity probe cleanup failed: %s", cleanup_exc)
        return {
            "ok": False,
            "error": f"probe failed: {exc}",
            "stdout": "",
            "stderr": "",
        }

    stdout = "\n".join(stdout_lines)
    stderr = "\n".join(stderr_lines)
    if result.cancelled:
        return _probe_stop_result(context) or {
            "ok": False,
            "cancelled": True,
            "error": "channel identity verification was stopped",
            "stdout": "",
            "stderr": stderr,
        }
    if result.timed_out:
        return {
            "ok": False,
            "timed_out": True,
            "error": "channel identity verification timed out",
            "stdout": "",
            "stderr": stderr,
        }
    try:
        from .youtube_session import handle_youtube_failure_text
        handle_youtube_failure_text(
            "\n".join((stdout, stderr)),
            context="verifying a YouTube channel identity",
        )
    except Exception as exc:
        _log.debug("channel identity session guard failed: %s", exc)
    return {
        "ok": result.returncode == 0,
        "returncode": result.returncode,
        "stdout": stdout,
        "stderr": stderr,
    }


def _probe_video_identities(
        video_ids: list[str], *, context: dict[str, Any]) -> list[dict[str, str]]:
    requested = [video_id for video_id in video_ids if _clean_video_id(video_id)]
    if not requested or not context.get("ok"):
        return []
    template = (
        "VIDTRACK:::%(id)s:::%(channel_id)s:::%(uploader_id)s:::"
        "%(uploader_url)s:::%(channel)s"
    )
    command = [
        context["yt"],
        "--ignore-errors",
        "--skip-download",
        "--no-warnings",
        "--print", template,
        *context.get("cookies", []),
        *(f"https://www.youtube.com/watch?v={video_id}"
          for video_id in requested),
    ]
    result = _run_identity_probe(
        command,
        context=context,
        timeout=max(30, len(requested) * 20),
        traffic_kind="channel_identity_recovery",
        units=len(requested),
    )
    if (result.get("cancelled") or result.get("paused")
            or result.get("skipped")):
        context["probe_stopped"] = result
        return []
    if not result.get("stdout"):
        if result.get("error"):
            _log.debug("channel identity video probe failed: %s", result["error"])
        return []
    requested_set = set(requested)
    rows: list[dict[str, str]] = []
    for line in str(result.get("stdout") or "").splitlines():
        row = _parse_video_track_line(line)
        if row is not None and row["video_id"] in requested_set:
            rows.append(row)
    return rows


def _verify_candidate_url(
        url: str, *, context: dict[str, Any]) -> str:
    if not context.get("ok"):
        return ""
    from .subs import ensure_videos_suffix

    command = [
        context["yt"],
        # Flat + playlist-scoped on purpose. Reading the identity off the
        # channel itself never extracts an individual video, so a newest
        # upload that is members-only, age-gated, geo-blocked, premiering or
        # live cannot fail the whole check. Extracting that one video used to
        # be the gate, which blocked entire channels from ever syncing.
        "--flat-playlist",
        "--playlist-end", "1",
        "--skip-download",
        "--no-warnings",
        "--ignore-errors",
        "--print", "playlist:" + _VERIFY_PRINT,
        # Belt and braces: a flat entry also carries the channel ID, so a
        # channel whose playlist scope comes back empty still verifies.
        "--print", _VERIFY_PRINT,
        *context.get("cookies", []),
        ensure_videos_suffix(url),
    ]
    result = _run_identity_probe(
        command,
        context=context,
        timeout=30,
        traffic_kind="channel_identity_verify",
    )
    if (result.get("cancelled") or result.get("paused")
            or result.get("skipped")):
        context["probe_stopped"] = result
        return ""
    if not result.get("stdout"):
        if result.get("error"):
            _log.debug(
                "channel identity URL verification failed: %s", result["error"])
        return ""
    for line in str(result.get("stdout") or "").splitlines():
        parts = _split_track(line, _VERIFY_PREFIX, 2)
        if parts is None:
            continue
        # yt-dlp prints "NA" for a field it could not resolve. Skip those so a
        # usable line further down (entry scope vs playlist scope) still counts.
        channel_id = _clean_channel_id(parts[0])
        if channel_id:
            return channel_id
    return ""


def _channel_id_from_url(value: object) -> str:
    match = re.search(
        r"/channel/(UC[A-Za-z0-9_-]{22})(?:/|$)",
        str(value or ""),
        flags=re.IGNORECASE,
    )
    return _clean_channel_id(match.group(1) if match else "")


def _stable_id_from_channel(channel: dict[str, Any]) -> str:
    saved = _clean_channel_id(channel.get("channel_id"))
    return saved or _channel_id_from_url(channel.get("url"))


def operational_channel_url(channel: dict[str, Any]) -> str:
    """Return the immutable channel URL when a permanent ID is known."""
    stable_id = _stable_id_from_channel(channel)
    if stable_id:
        return f"https://www.youtube.com/channel/{stable_id}"
    return str(channel.get("url") or "").strip()


def has_stable_identity(channel: dict[str, Any]) -> bool:
    """Return whether a saved row already carries an immutable identity."""
    return bool(_stable_id_from_channel(channel))


def recover_stale_channel(
        channel: dict[str, Any], *, cancel_event=None,
        pause_event=None, kill_current=None, stream=None,
        evidence_video_ids: list[str] | None = None) -> dict[str, Any]:
    """Resolve a stale handle and commit it only after exact-ID proof."""
    from . import subs

    old_url = _normalized_candidate_url(channel.get("url"))
    if not old_url:
        return {"ok": False, "error": "saved channel URL is invalid"}
    expected_id = _stable_id_from_channel(channel)
    video_ids = (
        [_clean_video_id(value) for value in evidence_video_ids]
        if evidence_video_ids is not None
        else _known_video_ids(channel, limit=5)
    )
    video_ids = list(dict.fromkeys(value for value in video_ids if value))[:2]
    # One archived video plus the independent candidate-URL check below
    # establishes the same immutable channel ID. When two IDs are available,
    # require both to resolve and agree for extra corruption resistance without
    # adding more one-time migration traffic than necessary.
    required = 1 if expected_id else max(1, len(video_ids))
    if len(video_ids) < required:
        return {"ok": False, "error": "not enough archived identity evidence"}

    context = _probe_context(
        cancel_event=cancel_event, pause_event=pause_event, stream=stream,
        task_id=str(channel.get("task_id") or ""))
    context["kill_current"] = kill_current
    if not context.get("ok"):
        return context
    rows = _probe_video_identities(video_ids, context=context)
    if context.get("probe_stopped"):
        return dict(context["probe_stopped"])
    unique_videos = {row["video_id"] for row in rows}
    channel_ids = {row["channel_id"] for row in rows}
    if len(unique_videos) < required or len(channel_ids) != 1:
        return {"ok": False, "error": "archived videos did not agree on one channel"}
    resolved_id = next(iter(channel_ids))
    if expected_id and resolved_id != expected_id:
        return {"ok": False, "error": "permanent channel ID mismatch"}

    candidate_urls = {
        candidate
        for candidate in (
            _normalized_candidate_url(row.get("uploader_url")) for row in rows
        )
        if candidate
    }
    if len(candidate_urls) > 1:
        return {"ok": False, "error": "archived videos reported different handles"}
    new_url = (
        next(iter(candidate_urls))
        if candidate_urls
        else f"https://www.youtube.com/channel/{resolved_id}"
    )
    if _verify_candidate_url(new_url, context=context) != resolved_id:
        if context.get("probe_stopped"):
            return dict(context["probe_stopped"])
        return {"ok": False, "error": "replacement URL failed permanent-ID verification"}

    stopped = _probe_stop_result(context)
    if stopped is not None:
        return stopped

    try:
        committed = subs.update_verified_channel_identity(
            {
                "url": old_url,
                "name": str(channel.get("name") or channel.get("folder") or ""),
                "channel_id": expected_id,
            },
            expected_url=old_url,
            channel_id=resolved_id,
            current_url=new_url,
            cancel_event=cancel_event,
            pause_event=pause_event,
            stop_event=kill_current,
        )
    except (OSError, subs.SubsError) as exc:
        return {"ok": False, "error": str(exc)}
    return {**committed, "verified": True}


def preflight_channel_identity(
        channel: dict[str, Any], *, cancel_event=None, pause_event=None,
        kill_current=None, stream=None,
        has_local_history: bool = False,
        archive_folder: str | Path | None = None) -> dict[str, Any]:
    """Fail closed before a legacy mutable URL can target a new owner.

    Existing ``channel_id`` values and permanent ``/channel/UC...`` URLs are
    already safe. An intentional add/manual URL edit carries a one-time rebind
    marker. Other legacy rows must prove that locally archived video evidence
    and the current replacement URL share one exact permanent ID before any
    enumeration or download begins. Two independent videos are used when
    available; a one-video archive can still be proven because YouTube assigns
    that video to exactly one permanent channel ID and the replacement URL is
    checked independently.
    """
    saved_id = _clean_channel_id(channel.get("channel_id"))
    url_id = _channel_id_from_url(channel.get("url"))
    if saved_id and url_id and saved_id != url_id:
        return {
            "ok": False,
            "error": "saved permanent channel IDs disagree",
        }
    if _stable_id_from_channel(channel):
        return {"ok": True, "changed": False, "channel": dict(channel)}
    if channel.get("channel_identity_rebind_pending"):
        return {
            "ok": True,
            "changed": False,
            "first_bind_allowed": True,
            "channel": dict(channel),
        }

    evidence = _known_video_ids(channel, limit=2)
    if len(evidence) < 2 and archive_folder:
        for video_id in _archive_video_ids(archive_folder, limit=2):
            if video_id not in evidence:
                evidence.append(video_id)
            if len(evidence) >= 2:
                break
    if evidence:
        return recover_stale_channel(
            channel,
            cancel_event=cancel_event,
            pause_event=pause_event,
            kill_current=kill_current,
            stream=stream,
            evidence_video_ids=evidence,
        )
    if has_local_history or _channel_claims_existing_history(channel):
        return {
            "ok": False,
            "error": (
                "This existing archive does not have a locally verified video "
                "ID available to prove its permanent YouTube channel identity."
            ),
        }
    return {
        "ok": True,
        "changed": False,
        "first_bind_allowed": True,
        "channel": dict(channel),
    }


def record_observed_identity(
        channel: dict[str, Any], tracks: list[dict[str, str]]) -> dict[str, Any]:
    """Persist identity emitted by a healthy channel sync (zero extra calls)."""
    from . import subs

    valid = [track for track in tracks if _clean_channel_id(
        track.get("channel_id"))]
    channel_ids = {track["channel_id"] for track in valid}
    if len(channel_ids) != 1:
        return {
            "ok": False,
            "identity_mismatch": bool(_stable_id_from_channel(channel)),
            "error": "channel identity markers disagreed",
        }
    observed_id = next(iter(channel_ids))
    expected_id = _stable_id_from_channel(channel)
    if expected_id and observed_id != expected_id:
        return {
            "ok": False,
            "identity_mismatch": True,
            "error": "permanent channel ID changed unexpectedly",
        }
    current_urls = {
        candidate
        for candidate in (
            _normalized_candidate_url(track.get("uploader_url"))
            for track in valid
        )
        if candidate
    }
    if len(current_urls) > 1:
        return {
            "ok": False,
            "identity_mismatch": bool(expected_id),
            "error": "channel markers reported different handles",
        }
    old_url = _normalized_candidate_url(channel.get("url"))
    if not old_url:
        return {"ok": False, "error": "saved channel URL is invalid"}
    current_url = next(iter(current_urls)) if current_urls else old_url
    try:
        return subs.update_verified_channel_identity(
            {
                "url": old_url,
                "name": str(channel.get("name") or channel.get("folder") or ""),
                "channel_id": expected_id,
            },
            expected_url=old_url,
            channel_id=observed_id,
            current_url=current_url,
        )
    except (OSError, subs.SubsError) as exc:
        return {"ok": False, "error": str(exc)}


def emit_url_changed(stream, result: dict[str, Any]) -> None:
    """Send one nonblocking UI notification after the durable save."""
    if stream is None or not result.get("url_changed"):
        return
    channel = result.get("channel") if isinstance(result.get("channel"), dict) else {}
    payload = {
        "kind": "channel_url_changed",
        "channel_name": str(
            channel.get("name") or channel.get("folder") or "Channel"),
        "old_url": str(result.get("old_url") or ""),
        "new_url": str(result.get("new_url") or ""),
        "channel_id": str(result.get("channel_id") or ""),
    }
    try:
        stream.emit([[json.dumps(payload, ensure_ascii=False), "__control__"]])
    except Exception as exc:
        _log.debug("channel URL change notification failed: %s", exc)
