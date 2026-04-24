"""
Network-down detection — lightweight reachability probe.

YTArchiver uses this to pause sync/gpu work when the internet flaps
instead of letting yt-dlp burn retries against dead connections.

Expose a module-level Event (`net_down`) that workers can check or wait
on. A background thread polls every ~30s; once we stay reachable for 2
consecutive probes, we clear the flag.
"""

from __future__ import annotations

import socket
import threading
import time
from typing import Optional

# Hosts to probe. Must be reliable, low-latency, worldwide.
_PROBE_HOSTS = [
    ("1.1.1.1", 443), # Cloudflare DNS over HTTPS
    ("8.8.8.8", 443), # Google DNS
    ("youtube.com", 443), # The one we actually need
]


net_down = threading.Event() # Set when network is confirmed offline
_monitor_thread: Optional[threading.Thread] = None
_poll_interval_sec = 30.0
_probe_timeout_sec = 5.0


def probe_once(timeout: float = _probe_timeout_sec) -> bool:
    """Return True if at least one probe host answers a TCP handshake.

    Bug [30]: probes run in parallel so a slow/dead first host doesn't
    block reaching the others. Previously the loop tried hosts
    sequentially with a 5s timeout each; if Cloudflare DNS was slow
    AND Google DNS was slow, youtube.com never got tried within a
    reasonable window and the function returned False even though
    YouTube itself was reachable. With parallel probes the worst-case
    latency is the per-host timeout, not its multiple.
    """
    result_event = threading.Event()
    success = [False]
    def _probe(host: str, port: int):
        try:
            with socket.create_connection((host, port), timeout=timeout):
                success[0] = True
                result_event.set()
        except (socket.timeout, socket.gaierror, OSError):
            pass
    threads = []
    for host, port in _PROBE_HOSTS:
        t = threading.Thread(target=_probe, args=(host, port), daemon=True)
        t.start()
        threads.append(t)
    # Wait either until one succeeds or all have finished trying.
    result_event.wait(timeout=timeout + 0.5)
    return success[0]


def start_monitor():
    """Start the background poller. Safe to call repeatedly.

    audit F-50: runs one synchronous probe before starting the
    background thread. Without this initial probe, workers kicking off
    right after launch see net_down=False even if the network was
    already down — they try jobs that immediately fail until the first
    background poll fires ~30s later.
    """
    global _monitor_thread
    if _monitor_thread and _monitor_thread.is_alive():
        return
    try:
        if not probe_once():
            net_down.set()
    except Exception:
        pass
    _monitor_thread = threading.Thread(target=_run_monitor, daemon=True)
    _monitor_thread.start()


def _run_monitor():
    consecutive_ok = 0
    while True:
        ok = probe_once()
        if ok:
            consecutive_ok += 1
            if net_down.is_set() and consecutive_ok >= 2:
                net_down.clear()
        else:
            consecutive_ok = 0
            net_down.set()
        time.sleep(_poll_interval_sec)


def block_if_down(stream=None, check_cancel=None) -> bool:
    """Block the calling thread while the network is marked down.

    Returns True if we eventually proceed, False if cancelled first.
    `check_cancel` is an optional callable returning True to abort.
    """
    if not net_down.is_set():
        return True
    if stream:
        # bug L-8: surface the 2-probe recovery expectation so the user
        # doesn't think the app is hung during the ~60s it takes to
        # confirm the network is really back. Two consecutive OK probes
        # are required before we un-pause; with _poll_interval_sec ~30s
        # this bounds worst-case recovery at ~60s.
        stream.emit_text(
            " \u26a0 Network down \u2014 pausing until connection returns "
            "(~30-60s after it's back, to confirm stability)...", "red")
    waited = 0
    # audit D-39: require 2 consecutive OK probes to clear net_down
    # from inside the inline wait loop, matching the background
    # monitor's stability requirement. Without this, a single blip
    # of connectivity ends the wait — flappy connections then
    # pause/resume repeatedly as yt-dlp hits another dropout a second
    # later and net_down re-sets.
    _inline_ok = 0
    while net_down.is_set():
        if check_cancel and check_cancel():
            return False
        time.sleep(1)
        waited += 1
        # Re-probe immediately every ~5s so we don't have to wait up to 30s
        if waited % 5 == 0:
            if probe_once():
                _inline_ok += 1
                if _inline_ok >= 2:
                    net_down.clear()
                    break
            else:
                _inline_ok = 0
    if stream:
        stream.emit_text(" \u2713 Network back.", "simpleline_green")
    return True
