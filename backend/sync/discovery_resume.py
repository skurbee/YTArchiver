"""Reuse complete discovery for interrupted initial full-channel downloads.

Fresh channels retain lazy discovery. A channel with substantial recorded
progress can invest in one complete flat listing on resume. The immutable
listing carries no download cursor: the ordinary archive and filters remain
authoritative when yt-dlp processes its entries again.
"""
from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass

from .. import channel_cache, channel_identity
from ..log import get_logger
from ..process_runner import CancellationSignals, supervise_streaming_process
from ..subprocess_util import make_startupinfo
from ..ytdlp_options import _find_cookie_source
from . import discovery_manifest as store
from .ytdlp_session import popen_ytdlp_process

_log = get_logger(__name__)
_MIN_RECORDED_VIDEOS = 250
_MAX_JSON_BYTES = 64 * 1024 * 1024
_VIDEO_ID = re.compile(r"[A-Za-z0-9_-]{11}")
_TARGET = re.compile(r"https://www\.youtube\.com/channel/(UC[A-Za-z0-9_-]{22})(?:/(?:videos|shorts|streams))?/?")


@dataclass(frozen=True)
class ResumePlan:
    path: str
    key: str
    end_marker: str

    @property
    def print_template(self) -> str:
        # yt-dlp treats a literal-only print argument as a field name. This
        # empty substitution makes it a template while keeping the marker exact.
        return f"playlist:{self.end_marker}%(id&|)s"


def _plan(key: str) -> ResumePlan:
    return ResumePlan(str(store.manifest_path(key)), key,
                      f"DISCOVERY_RESUME_END:::{key}")


def _eligible(channel: dict, target_url: str) -> str:
    if str(channel.get("mode") or "full").lower() != "full":
        return ""
    if channel.get("init_complete") or (
            channel.get("initialized") and channel.get("sync_complete")):
        return ""
    match = _TARGET.fullmatch(target_url)
    if not match or not channel_identity.has_stable_identity(channel):
        return ""
    expected = channel_identity.operational_channel_url(channel).rstrip("/").rsplit("/", 1)[-1]
    return expected if match[1] == expected else ""


def _has_progress(channel: dict) -> bool:
    # This cache is only an activation hint, never proof of complete discovery
    # or permission to skip a download. Do not add a remote count request.
    if not channel.get("last_sync"):
        return False
    ids = channel_cache.get_known_ids(str(channel.get("url") or ""),
                                      limit=_MIN_RECORDED_VIDEOS) or []
    return len({value for value in ids if isinstance(value, str)
                and _VIDEO_ID.fullmatch(value)}) >= _MIN_RECORDED_VIDEOS


def prepare_discovery_resume(channel: dict, target_url: str, output_dir: str,
                             yt: str, stream, *, cancel_event=None,
                             pause_event=None, kill_current=None) -> ResumePlan | None:
    """Return a validated local playlist, or keep ordinary lazy discovery.

The caller must check its stop signals again after this function returns.
Listing failure is an optimization miss; partial output is never published.
"""
    channel_id = _eligible(channel, target_url)
    stop_local = threading.Event()
    stop = CancellationSignals(cancel_event, pause_event, kill_current, stop_local)
    if not channel_id or stop.is_set():
        return None
    try:
        key = store.context_key(channel_id, target_url, output_dir)
        cached = store.load_manifest(key, channel_id, target_url, output_dir)
        if cached is not None:
            stream.emit_text(" Resuming from the saved video list...\n", "simpleline_green")
            return _plan(key)
        if not _has_progress(channel):
            return None
    except (OSError, ValueError, TypeError) as exc:
        _log.debug("Saved discovery is unavailable: %s", exc)
        return None

    stream.emit_text(
        " Saving the channel's video list to make future resumes faster...\n",
        "simpleline_green")
    payloads: list[dict] = []
    invalid_output = False

    def consume(line: str) -> None:
        nonlocal invalid_output
        value = line.strip()
        if value.startswith("{"):
            if payloads or len(value.encode("utf-8")) > _MAX_JSON_BYTES:
                invalid_output = True
                stop_local.set()
                return
            try:
                payload = json.loads(value)
                if not isinstance(payload, dict):
                    raise ValueError("listing is not an object")
                payloads.append(payload)
            except (ValueError, TypeError):
                invalid_output = True
                stop_local.set()
        elif value.startswith(("ERROR:", "WARNING:")):
            # A successful exit alone cannot prove that warnings did not
            # describe a truncated channel response.
            invalid_output = True
            from ..youtube_session import handle_youtube_failure_text
            if handle_youtube_failure_text(value, context="saving a channel video list"):
                stop_local.set()
        elif "[youtube:tab]" in value:
            _log.debug("Discovery list: %s", value)

    cmd = [yt, "--ignore-config", "--no-quiet", "--flat-playlist", "--lazy-playlist",
           "--dump-single-json", "--skip-download", "--abort-on-error",
           "--retries", "3", "--socket-timeout", "15",
           "--sleep-requests", "0.75", *(_find_cookie_source() or []), target_url]
    try:
        proc = popen_ytdlp_process(
            cmd, startupinfo=make_startupinfo(), cancel_event=stop,
            pause_event=pause_event, stream=stream, text=True)
        result = supervise_streaming_process(
            proc, on_stdout_line=consume, on_stderr_line=consume,
            cancel_event=stop, idle_timeout=120, exit_timeout=10,
            owner="sync", task_id=str(channel.get("task_id") or ""),
            role="channel-discovery-cache")
        if (result.returncode == 0 and result.output_complete
                and not result.cancelled and not result.timed_out
                and not invalid_output and not stop.is_set() and len(payloads) == 1):
            saved = store.save_manifest(key, channel_id, target_url, output_dir, payloads[0])
            if saved is not None:
                return _plan(key)
    except (OSError, ValueError, TypeError) as exc:
        _log.debug("Could not save complete discovery: %s", exc)
    if not CancellationSignals(cancel_event, pause_event, kill_current).is_set():
        stream.emit_dim(" (Saved video list unavailable; continuing normal discovery)")
    return None


def finish_discovery_resume(plans) -> None:
    """Drop this optional cache only after the caller establishes completion."""
    for plan in plans:
        store.invalidate(plan.key)
