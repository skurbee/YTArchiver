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
    """Return True if at least one probe host answers a TCP handshake."""
    for host, port in _PROBE_HOSTS:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except (socket.timeout, socket.gaierror, OSError):
            continue
    return False


def start_monitor():
    """Start the background poller. Safe to call repeatedly."""
    global _monitor_thread
    if _monitor_thread and _monitor_thread.is_alive():
        return
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
        stream.emit_text(" \u26a0 Network down \u2014 pausing until connection returns...", "red")
    waited = 0
    while net_down.is_set():
        if check_cancel and check_cancel():
            return False
        time.sleep(1)
        waited += 1
        # Re-probe immediately every ~5s so we don't have to wait up to 30s
        if waited % 5 == 0:
            if probe_once():
                net_down.clear()
                break
    if stream:
        stream.emit_text(" \u2713 Network back.", "simpleline_green")
    return True
