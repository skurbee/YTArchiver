"""Attach the required request guard to app-owned yt-dlp children.

The plugin runs inside yt-dlp, before its extractor opens each YouTube URL.
Only the parent owns the persistent budget. Intentional budget waits belong
to that child and do not consume its network/stall timeout.
"""
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path


def prepare_command(command, env=None):
    """Return guarded argv, a private child environment, and its session."""
    if isinstance(command, (str, bytes)) or not command:
        raise ValueError("yt-dlp requires an explicit argument list")
    argv = [str(part) for part in command]
    offset = 3 if len(argv) > 2 and argv[1:3] == ["-m", "yt_dlp"] else 1
    options = argv[offset:]
    # Version/help/update-only invocations cannot extract a video. Keep the
    # updater independent of a plugin that a replacement executable may need.
    if options and all(part in {"--version", "--help", "-h", "--update", "-U"}
                       for part in options):
        return argv, env, None
    if "--no-plugin-dirs" in options or "--no-plugins" in options:
        raise ValueError("YouTube request protection cannot be disabled")
    plugin_root = Path(__file__).resolve().parent / "yt_dlp_plugins"
    plugin = plugin_root / "ytarchiver/yt_dlp_plugins/postprocessor/ytarchiver_traffic.py"
    if not plugin.is_file():
        raise OSError("YouTube request protection is missing; download was not started")
    from .youtube_request_broker import prepare_launch

    session = prepare_launch()
    child_env = dict(os.environ if env is None else env)
    child_env.update(session.environment())
    # Ignore ambient yt-dlp configuration: its plugin/settings overrides must
    # not be able to silently bypass the application budget.
    guarded = [*argv[:offset], "--ignore-config", "--no-plugin-dirs", "--plugin-dirs", str(plugin_root),
               "--use-postprocessor", "YTArchiverTrafficGuard:when=pre_process",
               *options]
    return guarded, child_env, session


def request_session(proc):
    # vars avoids manufacturing attributes on subprocess test doubles.
    return vars(proc).get("_yta_request_session") if hasattr(proc, "__dict__") else None


def budget_wait_seconds(proc) -> float:
    session = request_session(proc)
    return session.wait_seconds() if session is not None else 0.0


def budget_waiting(proc) -> bool:
    session = request_session(proc)
    return session.is_waiting() if session is not None else False


def set_request_signals(proc, cancel_event=None, pause_event=None) -> None:
    session = request_session(proc)
    if session is not None:
        session.set_signals(cancel_event=cancel_event, pause_event=pause_event)


def attach_session(proc, session) -> None:
    if session is None:
        return
    proc._yta_request_session = session
    original_communicate = getattr(proc, "communicate", None)
    if not callable(original_communicate):
        # Streaming-only process adapters do not expose communicate().
        session.bind(proc)
        return

    def communicate(input=None, timeout=None):
        def capture(value, **options):
            if value is not None:
                options["input"] = value
            return original_communicate(**options)

        if timeout is None:
            return capture(input)
        started = time.monotonic()
        initial_wait = session.wait_seconds()
        pending_input = input
        while True:
            remaining = float(timeout) - (time.monotonic() - started) + (
                session.wait_seconds() - initial_wait)
            if remaining <= 0:
                # Preserve subprocess's final output/exception behavior.
                return capture(pending_input, timeout=0)
            try:
                return capture(pending_input, timeout=min(0.25, remaining))
            except subprocess.TimeoutExpired:
                # Popen retains partial output and the input offset. Resending
                # input after the first communicate would duplicate stdin.
                pending_input = None

    proc.communicate = communicate
    session.bind(proc)
