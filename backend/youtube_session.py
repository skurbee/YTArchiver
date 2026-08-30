"""Process-wide YouTube authentication and rate-limit guard.

Every yt-dlp path uses the same Firefox cookie source, but historically each
caller interpreted failures independently.  That allowed metadata/comment
jobs to keep running after Firefox signed out even though the download path
already knew how to pause and alert.

This module is deliberately dependency-light.  ``main.Api`` configures it
once with the live stream/pause/queue objects; yt-dlp callers then either:

* call :func:`check_cookie_source` before using the cookie args; or
* pass stderr/output through :func:`handle_youtube_failure_text`.

The app log also installs :func:`scan_log_line` as a last-resort safety net,
so a newly-added YouTube operation cannot silently bypass the visible alarm
as long as it surfaces its error.
"""
from __future__ import annotations

import json
import threading
import time
from typing import Any

from . import youtube_traffic
from .log import get_logger

_log = get_logger(__name__)

_lock = threading.RLock()
_stream = None
_pause_event: threading.Event | None = None
_queues = None
_cookie_alert_fired = False
_rate_limit_alert_ts = 0.0
_rate_limit_detected = threading.Event()
_background_rate_limit_scope = False
_background_cancel_event: threading.Event | None = None


def is_cookie_auth_error(text: str) -> bool:
    """Return True for yt-dlp/browser-cookie authentication failures."""
    low = str(text or "").lower()
    return (
        "sign in to confirm" in low
        or "cookies are missing" in low
        or "cookies are invalid" in low
        or "unable to extract cookies" in low
        or "could not get firefox cookies" in low
        or ("failed to extract any player response" in low and "sign in" in low)
        or ("error:" in low and "cookie" in low
            and ("extract" in low or "sign in" in low or "expired" in low))
    )


def is_youtube_rate_limit_error(text: str) -> bool:
    """Return True for YouTube/yt-dlp throttling failures."""
    low = str(text or "").lower()
    return (
        "rate-limited by youtube" in low
        or "rate limited by youtube" in low
        or "current session has been rate-limited" in low
        or "youtube is rate-limiting" in low
        or "http error 429" in low
        or "http 429" in low
        or "too many requests" in low
    )


def configure(stream, pause_event: threading.Event, queues) -> None:
    """Attach the live app objects used to pause and notify."""
    global _stream, _pause_event, _queues
    with _lock:
        _stream = stream
        _pause_event = pause_event
        _queues = queues


def _targets(stream=None, pause_event=None, queues=None):
    with _lock:
        return (
            stream if stream is not None else _stream,
            pause_event if pause_event is not None else _pause_event,
            queues if queues is not None else _queues,
        )


def _pause(pause_event=None, queues=None) -> None:
    _, event, queue_state = _targets(
        pause_event=pause_event, queues=queues)
    if event is not None:
        try:
            event.set()
        except Exception as exc:
            _log.debug("YouTube guard pause_event.set failed: %s", exc)
    if queue_state is not None:
        try:
            queue_state.set_sync_paused(True)
        except Exception as exc:
            _log.debug("YouTube guard queue pause failed: %s", exc)


def _emit_control(stream, kind: str, **extra: Any) -> None:
    if stream is None:
        return
    payload = {"kind": kind}
    payload.update(extra)
    try:
        stream.emit([[json.dumps(payload), "__control__"]])
    except Exception as exc:
        _log.debug("YouTube guard control emit failed: %s", exc)


def trigger_cookie_alert(*, reason: str = "", context: str = "",
                         stream=None, pause_event=None, queues=None) -> bool:
    """Pause and show the Firefox sign-in alarm once per invalid session."""
    global _cookie_alert_fired
    target_stream, event, queue_state = _targets(
        stream=stream, pause_event=pause_event, queues=queues)
    _pause(event, queue_state)
    with _lock:
        if _cookie_alert_fired:
            return False
        _cookie_alert_fired = True

    if target_stream is not None:
        bar = "\u2588" * 65
        where = f" while {context}" if context else ""
        target_stream.emit([["\n" + bar + "\n", "red"]])
        target_stream.emit([
            ["\u2588  ", "red"],
            [f"FIREFOX IS SIGNED OUT OF YOUTUBE{where}.", "red"],
            ["\n", "red"],
        ])
        target_stream.emit([
            ["\u2588  ", "red"],
            ["Sign back in to YouTube in Firefox. YTArchiver has", "red"],
            ["\n", "red"],
        ])
        target_stream.emit([
            ["\u2588  ", "red"],
            ["paused automatically and will not call YouTube again until "
             "you click Resume.", "red"],
            ["\n", "red"],
        ])
        if reason:
            concise = " ".join(str(reason).split())[:500]
            target_stream.emit([
                ["\u2588  Reason: ", "red"],
                [concise, "red"],
                ["\n", "red"],
            ])
        target_stream.emit([[bar + "\n\n", "red"]])
    _emit_control(target_stream, "cookie_alert", context=context)
    return True


def trigger_rate_limit_alert(*, reason: str = "", context: str = "",
                             stream=None, pause_event=None, queues=None,
                             now: float | None = None) -> bool:
    """Pause immediately on a rate limit and notify at most once per hour."""
    global _rate_limit_alert_ts
    target_stream, event, queue_state = _targets(
        stream=stream, pause_event=pause_event, queues=queues)
    _rate_limit_detected.set()
    circuit = youtube_traffic.record_rate_limit(now=now)
    with _lock:
        background_scope = _background_rate_limit_scope
        background_cancel = _background_cancel_event
    # Scheduled syncs terminate and report their next retry after the
    # scheduler has been rearmed.  Suppress the manual "click Resume"
    # instruction here; it is both wrong for an unattended run and would
    # leave a parked queue that resumes at an arbitrary later time.
    if background_scope:
        if background_cancel is not None:
            try:
                background_cancel.set()
            except Exception as exc:
                _log.debug(
                    "YouTube guard background cancel failed: %s", exc)
        return True
    _pause(event, queue_state)
    now = time.time() if now is None else float(now)
    with _lock:
        if _rate_limit_alert_ts and now - _rate_limit_alert_ts < 3600:
            return False
        _rate_limit_alert_ts = now
    if target_stream is not None:
        where = f" during {context}" if context else ""
        cooldown_until = float(circuit.get("cooldown_until") or 0)
        resume = time.strftime(
            "%I:%M%p", time.localtime(cooldown_until)
        ).lstrip("0").lower() if cooldown_until else "after the cooldown"
        target_stream.emit_error(
            "YouTube rate limit detected"
            f"{where}. YTArchiver auto-paused immediately. Do not resume "
            f"before {resume}; the interrupted task will retry first.")
    _emit_control(target_stream, "youtube_rate_limit_alert", context=context)
    return True


def handle_youtube_failure_text(text: str, *, context: str = "",
                                stream=None, pause_event=None,
                                queues=None) -> str:
    """Classify output and enforce the matching pause/alert policy.

    Returns ``"cookie"``, ``"rate_limit"``, or ``""``.
    """
    if is_cookie_auth_error(text):
        trigger_cookie_alert(
            reason=text, context=context, stream=stream,
            pause_event=pause_event, queues=queues)
        return "cookie"
    if is_youtube_rate_limit_error(text):
        trigger_rate_limit_alert(
            reason=text, context=context, stream=stream,
            pause_event=pause_event, queues=queues)
        return "rate_limit"
    return ""


def scan_log_line(text: str) -> None:
    """LogStreamer scanner: final safety net for all YouTube operations.

    Passive log scanning sees user-facing download titles as well as real
    yt-dlp diagnostics. Require an error-shaped line here so a title such as
    "Too Many Requests" cannot trip the persistent rate-limit circuit.
    Direct stderr callers keep the full classifier sensitivity through
    :func:`handle_youtube_failure_text`.
    """
    low = str(text or "").lower()
    if ("error" not in low and "http" not in low
            and "warning" not in low):
        return
    handle_youtube_failure_text(text)


def check_cookie_source(cookie_args: list[str] | tuple[str, ...] | None,
                        *, context: str = "a YouTube operation") -> bool:
    """Offline Firefox-cookie validation used before every yt-dlp operation.

    This intentionally performs no extra YouTube request.  It verifies that
    Firefox still has a non-expired Google/YouTube authentication cookie;
    the actual yt-dlp call remains the authoritative server-side test and its
    output is handled by :func:`handle_youtube_failure_text`.
    """
    args = list(cookie_args or [])
    if "--cookies-from-browser" not in args:
        # A manual cookies.txt cannot be safely interpreted here.  Empty args
        # are allowed for public-only use; actual auth-required responses will
        # still fire the reactive alarm.
        return True
    try:
        idx = args.index("--cookies-from-browser")
        browser = str(args[idx + 1]).split(":", 1)[0].lower()
    except (ValueError, IndexError):
        return True
    if browser != "firefox":
        return True

    try:
        from .deps_installer import firefox_cookie_status
        status = firefox_cookie_status()
    except Exception as exc:
        _log.debug("Firefox cookie validation failed: %s", exc)
        return True

    if status.get("signed_in"):
        return True

    detail = str(status.get("detail") or "Firefox sign-in not detected")
    trigger_cookie_alert(reason=detail, context=context)
    return False


def check_configured_cookie_session(*, context: str) -> bool:
    """Validate cookies locally and against an authenticated YouTube feed.

    Firefox can retain unexpired-looking cookie rows after YouTube has
    invalidated the server-side session.  ``:ytfav`` is yt-dlp's authenticated
    Liked Videos feed; a one-item flat probe verifies the real session without
    downloading media.
    """
    with _lock:
        if _stream is None or _pause_event is None or _queues is None:
            # Narrow unit-test/CLI helpers do not own the desktop app's pause
            # and notification surface; leave their behavior unchanged.
            return True
    try:
        from .sync.ytdlp_proc import _find_cookie_source, find_yt_dlp
        args = _find_cookie_source(_skip_session_check=True)
    except Exception as exc:
        _log.debug("configured cookie source validation failed: %s", exc)
        return True
    if not check_cookie_source(args, context=context):
        return False
    if not args:
        return True
    yt = find_yt_dlp()
    if not yt:
        return True
    permission = youtube_traffic.acquire("session_probe")
    if not permission.get("ok"):
        if permission.get("cooldown"):
            return False
        return True
    try:
        from .proc_utils import utf8_subprocess_env
        from .process_runner import run_ytdlp
        from .subprocess_util import make_startupinfo
        proc = run_ytdlp(
            [
                yt,
                "--flat-playlist",
                "--playlist-end", "1",
                "--print", "id",
                "--no-warnings",
                "--skip-download",
                *args,
                ":ytfav",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=25,
            startupinfo=make_startupinfo(),
            env=utf8_subprocess_env(),
        )
    except Exception as exc:
        # A connectivity/launch failure is not proof of an expired sign-in.
        _log.debug("YouTube authenticated-session probe failed: %s", exc)
        return True

    output = "\n".join((proc.stdout or "", proc.stderr or ""))
    failure = handle_youtube_failure_text(
        output, context=context)
    if failure:
        return False
    if proc.returncode != 0:
        low = output.lower()
        if ("authentication" in low or "login required" in low
                or "playlist is private" in low
                or "this playlist is private" in low):
            trigger_cookie_alert(
                reason=output or "Authenticated YouTube feed unavailable",
                context=context)
            return False
        # Do not lock the user out on an unrelated extractor/network error.
        _log.debug(
            "YouTube session probe inconclusive (rc=%s): %s",
            proc.returncode, " ".join(output.split())[:300])
        return True
    global _cookie_alert_fired
    with _lock:
        _cookie_alert_fired = False
    return True


def reset_rate_limit_alert() -> None:
    """Allow a resumed task to raise a fresh alert if throttling persists."""
    global _rate_limit_alert_ts
    with _lock:
        _rate_limit_alert_ts = 0.0


def begin_sync_scope(*, background: bool = False,
                     cancel_event: threading.Event | None = None) -> None:
    """Reset per-run detection and select manual/background alert wording."""
    global _background_rate_limit_scope, _background_cancel_event
    with _lock:
        _background_rate_limit_scope = bool(background)
        _background_cancel_event = cancel_event if background else None
        _rate_limit_detected.clear()


def end_sync_scope() -> None:
    """Restore the default manual policy after a sync worker exits."""
    global _background_rate_limit_scope, _background_cancel_event
    with _lock:
        _background_rate_limit_scope = False
        _background_cancel_event = None
        _rate_limit_detected.clear()


def rate_limit_detected() -> bool:
    """Return whether the active sync scope observed a YouTube throttle."""
    return _rate_limit_detected.is_set()
