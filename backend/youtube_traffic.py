"""Persistent YouTube traffic governor.

YTArchiver deliberately treats a yt-dlp launch as an *operation*, not as an
exact HTTP-request count.  One extractor launch can issue several
webpage/player requests internally, while media fragments use a different
traffic path.  The governor therefore combines:

* a rolling one-hour operation budget;
* a rolling 24-hour operation budget;
* randomized minimum spacing between launches; and
* a restart-safe event ledger under the app-data directory.

The conservative defaults are intentionally far below yt-dlp's empirically
observed account limits.  This module owns both enforcement and projections so
the Settings recommendation cannot drift away from what the workers enforce.
"""
from __future__ import annotations

import contextlib
import json
import math
import os
import random
import threading
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .log import get_logger
from .ytarchiver_config import APP_DATA_DIR, load_config

_log = get_logger(__name__)

TRAFFIC_FILE = APP_DATA_DIR / "youtube_traffic.jsonl"
CIRCUIT_FILE = APP_DATA_DIR / "youtube_rate_limit_state.json"
HOUR_SECONDS = 3600
DAY_SECONDS = 86400

TRAFFIC_PRESETS: dict[str, dict[str, int]] = {
    "conservative": {
        "daily": 750,
        "hourly": 90,
        "min_gap": 10,
        "max_gap": 20,
    },
    "balanced": {
        "daily": 1500,
        "hourly": 180,
        "min_gap": 8,
        "max_gap": 15,
    },
    # Unlimited removes the numeric ceilings, but deliberately retains
    # yt-dlp's recommended minimum spacing.  The emergency rate-limit circuit
    # breaker in youtube_session remains non-disableable.
    "unlimited": {
        "daily": 0,
        "hourly": 0,
        "min_gap": 5,
        "max_gap": 10,
    },
}

_lock = threading.RLock()
_loaded = False
_events: list[dict[str, Any]] = []
_last_launch_ts = 0.0
_next_gap_seconds = 0.0
_reservations: dict[str, dict[str, Any]] = {}
_scope = threading.local()
_active_waits: dict[int, dict[str, Any]] = {}
_wait_listeners: list[Any] = []
_budget_override_active = False
_override_wakeup = threading.Event()


def _notify_wait_listeners(state: dict[str, Any]) -> None:
    for callback in list(_wait_listeners):
        try:
            callback(dict(state))
        except Exception as exc:
            _log.debug("YouTube traffic wait listener failed: %s", exc)


def add_wait_listener(callback) -> None:
    """Notify ``callback`` whenever rolling-budget waiting starts or ends."""
    if not callable(callback):
        return
    with _lock:
        if callback not in _wait_listeners:
            _wait_listeners.append(callback)


def wait_status() -> dict[str, Any]:
    """Return the process-wide rolling-budget wait shown by the UI."""
    with _lock:
        waits = list(_active_waits.values())
        override = bool(_budget_override_active)
    if not waits:
        return {"active": False, "override_active": override}
    # The latest release time is the one that governs the whole process when
    # more than one YouTube worker reaches a rolling window simultaneously.
    current = max(
        waits, key=lambda item: float(item.get("until") or 0))
    return {
        "active": True,
        "override_active": override,
        **current,
    }


def _set_wait_state(state: dict[str, Any] | None) -> None:
    ident = threading.get_ident()
    changed = False
    with _lock:
        previous = _active_waits.get(ident)
        if state is None:
            changed = _active_waits.pop(ident, None) is not None
        else:
            normalized = dict(state)
            changed = previous != normalized
            _active_waits[ident] = normalized
        snapshot = wait_status()
    if changed:
        _notify_wait_listeners(snapshot)


def override_budget_limits() -> dict[str, Any]:
    """Ignore configured rolling ceilings until the current sync ends.

    The emergency YouTube rate-limit circuit and launch spacing remain active.
    """
    global _budget_override_active
    before = wait_status()
    with _lock:
        _budget_override_active = True
        _override_wakeup.set()
        snapshot = wait_status()
    _notify_wait_listeners(snapshot)
    return {"ok": True, "wait": before, "override_active": True}


def clear_budget_override() -> None:
    global _budget_override_active
    with _lock:
        changed = _budget_override_active
        _budget_override_active = False
        _override_wakeup.clear()
        snapshot = wait_status()
    if changed:
        _notify_wait_listeners(snapshot)


def budget_override_active() -> bool:
    with _lock:
        return bool(_budget_override_active)


def _load_circuit_locked(now: float | None = None) -> dict[str, Any]:
    now = time.time() if now is None else float(now)
    incidents: list[float] = []
    cooldown_until = 0.0
    try:
        if CIRCUIT_FILE.is_file():
            data = json.loads(CIRCUIT_FILE.read_text(encoding="utf-8"))
            incidents = [
                float(ts) for ts in (data.get("incidents") or [])
                if float(ts) > now - 7 * DAY_SECONDS
            ]
            cooldown_until = float(data.get("cooldown_until") or 0)
    except Exception as exc:
        _log.warning("YouTube rate-limit state read failed: %s", exc)
    return {
        "incidents": incidents,
        "cooldown_until": cooldown_until,
        "active": cooldown_until > now,
        "remaining_seconds": max(0, cooldown_until - now),
    }


def _save_circuit_locked(state: dict[str, Any]) -> None:
    try:
        APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
        temp = Path(str(CIRCUIT_FILE) + f".{os.getpid()}.tmp")
        temp.write_text(json.dumps(
            {
                "incidents": state.get("incidents") or [],
                "cooldown_until": float(state.get("cooldown_until") or 0),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ), encoding="utf-8")
        os.replace(temp, CIRCUIT_FILE)
    except Exception as exc:
        _log.warning("YouTube rate-limit state save failed: %s", exc)


def record_rate_limit(now: float | None = None) -> dict[str, Any]:
    """Persist and escalate the non-disableable rate-limit cooldown."""
    now = time.time() if now is None else float(now)
    with _lock:
        state = _load_circuit_locked(now)
        incidents = list(state["incidents"])
        # One yt-dlp failure can surface through stderr, the sync parser, and
        # the log safety net. Treat every signal during an already-active
        # cooldown as the same incident.
        if state["active"]:
            return state
        incidents.append(now)
        count = len(incidents)
        cooldown_hours = 6 if count == 1 else 24 if count == 2 else 72
        state = {
            "incidents": incidents,
            "cooldown_until": now + cooldown_hours * HOUR_SECONDS,
            "active": True,
            "remaining_seconds": cooldown_hours * HOUR_SECONDS,
            "cooldown_hours": cooldown_hours,
        }
        _save_circuit_locked(state)
        return state


def circuit_state(now: float | None = None) -> dict[str, Any]:
    with _lock:
        state = _load_circuit_locked(now)
    count = len(state["incidents"])
    state["incident_count_7d"] = count
    state["cooldown_hours"] = 6 if count <= 1 else 24 if count == 2 else 72
    return state


def normalize_mode(value: Any) -> str:
    mode = str(value or "conservative").strip().lower()
    return mode if mode in ("conservative", "balanced", "custom",
                            "unlimited") else "conservative"


def is_youtube_url(value: Any) -> bool:
    """Return whether a URL targets YouTube rather than another yt-dlp site."""
    text = str(value or "").strip()
    if text.startswith(":yt"):
        return True
    try:
        host = (urlparse(text).hostname or "").lower()
    except Exception:
        return False
    return (
        host in {"youtu.be", "youtube.com", "youtube-nocookie.com"}
        or host.endswith((
            ".youtu.be", ".youtube.com", ".youtube-nocookie.com"))
    )


def effective_settings(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return validated effective traffic settings for a config snapshot."""
    if cfg is None:
        cfg = load_config()
    mode = normalize_mode(cfg.get("youtube_traffic_mode"))
    if mode in TRAFFIC_PRESETS:
        out = dict(TRAFFIC_PRESETS[mode])
    else:
        def _bounded(key: str, default: int, low: int, high: int) -> int:
            try:
                return max(low, min(high, int(cfg.get(key, default))))
            except (TypeError, ValueError):
                return default

        out = {
            "daily": _bounded(
                "youtube_traffic_custom_daily", 750, 1, 100_000),
            "hourly": _bounded(
                "youtube_traffic_custom_hourly", 90, 1, 10_000),
            "min_gap": _bounded(
                "youtube_traffic_custom_min_gap", 10, 0, 3600),
            "max_gap": _bounded(
                "youtube_traffic_custom_max_gap", 20, 0, 3600),
        }
        out["hourly"] = min(out["hourly"], out["daily"])
        out["max_gap"] = max(out["min_gap"], out["max_gap"])
    out["mode"] = mode
    return out


def estimate_channel_units(channel: dict[str, Any]) -> int:
    """Conservative launch estimate for one channel during a full sweep."""
    mode = str(channel.get("mode") or "full").strip().lower()
    failed = channel.get("failed_video_ids") or {}
    if isinstance(failed, dict):
        retry_count = sum(
            1 for count in failed.values()
            if isinstance(count, int) and 0 < count < 3)
    elif isinstance(failed, list):
        retry_count = len(failed)
    else:
        retry_count = 0

    # Initialized entire-channel subscriptions normally use one five-item
    # quick-check. Subscription/date modes run /videos and /streams.
    if mode in ("full", "new", "fromdate") and not retry_count:
        base = 1
    else:
        base = 2
    return base + min(retry_count, 10)


def estimate_sweep(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Estimate operation cost for one complete configured-channel sweep."""
    if cfg is None:
        cfg = load_config()
    channels = [
        ch for ch in (cfg.get("channels") or [])
        if isinstance(ch, dict) and (ch.get("url") or "").strip()
    ]
    base = sum(estimate_channel_units(ch) for ch in channels)

    # Downloads are unknowable before discovery.  Reserve modest feature
    # headroom based on the number of channels that request post-download
    # captions/metadata, then add a small general variance allowance.
    feature_flags = sum(
        int(bool(ch.get("auto_metadata")))
        + int(bool(ch.get("auto_transcribe")))
        for ch in channels
    )
    feature_headroom = math.ceil(feature_flags / 20)
    variance_headroom = math.ceil(base * 0.05) if base else 0
    total = base + feature_headroom + variance_headroom
    return {
        "channels": len(channels),
        "base_units": base,
        "feature_headroom": feature_headroom,
        "variance_headroom": variance_headroom,
        "units": total,
    }


def projection(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return UI-facing budget and recommended-autosync calculations."""
    if cfg is None:
        cfg = load_config()
    settings = effective_settings(cfg)
    sweep = estimate_sweep(cfg)
    units = int(sweep["units"])
    daily = int(settings["daily"])
    interval = int(cfg.get("autorun_interval", 0) or 0)
    configured_per_day = (
        1440 / interval if interval > 0 else 0.0
    )
    projected_daily = math.ceil(units * configured_per_day)

    if daily <= 0:
        fit = True
        raw_hours = 0.0
        recommendation = (
            "Unlimited mode has no numeric ceiling. Keep at least the "
            "configured launch spacing between operations.")
        low = high = 0
        sweeps_per_day = None
    elif units <= 0:
        fit = True
        raw_hours = 0.0
        recommendation = "Add channels to calculate a recommended interval."
        low = high = 0
        sweeps_per_day = 0
    elif units > daily:
        fit = False
        raw_hours = float("inf")
        recommendation = (
            f"One complete sweep needs about {units} operations, which cannot "
            f"fit inside the {daily}-operation daily budget.")
        low = high = 0
        sweeps_per_day = 0
    else:
        fit = True
        # Keep 15% of the daily budget free for manual downloads, metadata,
        # startup/session checks, and ordinary variability.
        usable_daily = max(units, daily * 0.85)
        raw_hours = max(1.0, 24.0 * units / usable_daily)
        low = max(1, int(math.floor(raw_hours)))
        high = max(low, int(math.ceil(raw_hours)))
        sweeps_per_day = max(1, int(math.floor(24 / raw_hours)))
        if low == high:
            recommendation = (
                f"Based on {sweep['channels']} channels and their enabled "
                f"features, we recommend auto-sync about every {low} hours.")
        else:
            recommendation = (
                f"Based on {sweep['channels']} channels and their enabled "
                f"features, we recommend auto-sync every {low}-{high} hours.")

    return {
        "settings": settings,
        "sweep": sweep,
        "configured_interval_mins": interval,
        "projected_daily_units": projected_daily,
        "fits_complete_sweep": fit,
        "recommended_hours": raw_hours,
        "recommended_hours_low": low,
        "recommended_hours_high": high,
        "recommended_sweeps_per_day": sweeps_per_day,
        "recommendation": recommendation,
    }


def _read_events_locked() -> None:
    global _loaded, _events, _last_launch_ts
    if _loaded:
        return
    rows: list[dict[str, Any]] = []
    try:
        if TRAFFIC_FILE.is_file():
            for line in TRAFFIC_FILE.read_text(
                    encoding="utf-8", errors="replace").splitlines():
                try:
                    row = json.loads(line)
                    ts = float(row.get("ts") or 0)
                    legacy_units = int(row.get("units") or 0)
                    daily_units = int(
                        row.get("daily_units", legacy_units) or 0)
                    hourly_units = int(
                        row.get("hourly_units", legacy_units) or 0)
                    if ts > 0 and (daily_units or hourly_units):
                        rows.append({
                            "ts": ts,
                            "daily_units": daily_units,
                            "hourly_units": hourly_units,
                            "kind": str(row.get("kind") or "youtube"),
                            "reservation_id": str(
                                row.get("reservation_id") or ""),
                        })
                except Exception:
                    continue
    except Exception as exc:
        _log.warning("YouTube traffic ledger read failed: %s", exc)

    # v82.5 originally refunded unused autosync capacity by appending a
    # negative event at sweep completion.  Because rolling windows expire
    # events by their own timestamp, the positive reservation could expire
    # before its later negative refund.  During that gap the orphan refund
    # masked unrelated YouTube operations and understated daily usage.
    #
    # Fold legacy refunds back into their original reservation timestamp.
    # If an older build already pruned the matching reservation, discard the
    # orphan negative row; it must never offset unrelated positive traffic.
    legacy_refunds = False
    reservations_by_id = {
        str(row.get("reservation_id") or ""): row
        for row in rows
        if row.get("kind") == "autosync_sweep_reservation"
        and row.get("reservation_id")
    }
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        if row.get("kind") != "autosync_sweep_refund":
            normalized_rows.append(row)
            continue
        legacy_refunds = True
        reservation = reservations_by_id.get(
            str(row.get("reservation_id") or ""))
        refund = max(0, -int(row.get("daily_units") or 0))
        if reservation is not None and refund:
            reservation["daily_units"] = max(
                0, int(reservation.get("daily_units") or 0) - refund)
    rows = normalized_rows

    _events = sorted(rows, key=lambda item: item["ts"])
    positive = [
        row["ts"] for row in _events
        if row["hourly_units"] > 0
        and row.get("kind") != "autosync_sweep_reservation"
    ]
    _last_launch_ts = max(positive, default=0.0)
    _loaded = True
    _prune_locked(time.time())
    if legacy_refunds:
        _rewrite_locked()


def _rewrite_locked() -> bool:
    try:
        APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
        temp = Path(str(TRAFFIC_FILE) + f".{os.getpid()}.tmp")
        payload = "".join(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
            for row in _events
        )
        temp.write_text(payload, encoding="utf-8")
        os.replace(temp, TRAFFIC_FILE)
        return True
    except Exception as exc:
        _log.warning("YouTube traffic ledger rewrite failed: %s", exc)
        return False


def _append_locked(row: dict[str, Any]) -> bool:
    """Persist and then register one traffic event.

    Returning False lets callers fail closed before a YouTube launch.  The
    in-memory ledger is updated only after the persistent append succeeds, so a
    refused launch does not consume a phantom budget unit for this process.
    """
    try:
        APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
        with TRAFFIC_FILE.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(
                row, ensure_ascii=False, separators=(",", ":")) + "\n")
    except Exception as exc:
        _log.warning("YouTube traffic ledger append failed: %s", exc)
        return False
    _events.append(row)
    return True


def _prune_locked(now: float) -> None:
    global _events
    cutoff = now - DAY_SECONDS
    old_len = len(_events)
    _events = [row for row in _events if row["ts"] > cutoff]
    if old_len != len(_events):
        _rewrite_locked()


def _window_units_locked(now: float, seconds: int, field: str) -> int:
    cutoff = now - seconds
    return max(0, sum(
        int(row[field]) for row in _events if row["ts"] > cutoff))


def _expiry_for_units_locked(now: float, seconds: int, limit: int,
                             requested: int, field: str) -> float:
    """Earliest time a rolling window can accept ``requested`` units."""
    if limit <= 0:
        return now
    relevant = [
        row for row in _events
        if row["ts"] > now - seconds and int(row[field]) > 0
    ]
    current = _window_units_locked(now, seconds, field)
    if current + requested <= limit:
        return now
    running = current
    for row in relevant:
        running -= int(row[field])
        if running + requested <= limit:
            return float(row["ts"]) + seconds + 0.01
    return now + seconds


def eligibility(units: int = 1, cfg: dict[str, Any] | None = None,
                *, include_gap: bool = True,
                ignore_limits: bool = False,
                now: float | None = None) -> dict[str, Any]:
    """Return whether units can launch now and the earliest eligible time."""
    if cfg is None:
        cfg = load_config()
    settings = effective_settings(cfg)
    requested = max(1, int(units or 1))
    now = float(now if now is not None else time.time())
    with _lock:
        _read_events_locked()
        _prune_locked(now)
        day_ts = now if ignore_limits else _expiry_for_units_locked(
            now, DAY_SECONDS, int(settings["daily"]), requested,
            "daily_units")
        hour_ts = now if ignore_limits else _expiry_for_units_locked(
            now, HOUR_SECONDS, int(settings["hourly"]), requested,
            "hourly_units")
        gap_ts = now
        if include_gap and _last_launch_ts > 0:
            gap_ts = _last_launch_ts + max(
                float(settings["min_gap"]), _next_gap_seconds)
        next_ts = max(now, day_ts, hour_ts, gap_ts)
        daily_used = _window_units_locked(
            now, DAY_SECONDS, "daily_units")
        hourly_used = _window_units_locked(
            now, HOUR_SECONDS, "hourly_units")
        circuit = _load_circuit_locked(now)
        if circuit["active"]:
            next_ts = max(next_ts, float(circuit["cooldown_until"]))
    wait_candidates = [
        ("daily_limit", day_ts),
        ("hourly_limit", hour_ts),
        ("spacing", gap_ts),
    ]
    if circuit["active"]:
        wait_candidates.append(
            ("cooldown", float(circuit["cooldown_until"])))
    wait_reason = None
    if next_ts > now + 0.05:
        wait_reason = max(wait_candidates, key=lambda item: item[1])[0]
    impossible = not ignore_limits and bool(
        (settings["daily"] > 0 and requested > settings["daily"])
        or (settings["hourly"] > 0 and requested > settings["hourly"])
    )
    return {
        "allowed": not impossible and next_ts <= now + 0.05,
        "impossible": impossible,
        "next_ts": None if impossible else next_ts,
        "wait_seconds": None if impossible else max(0, next_ts - now),
        "wait_reason": wait_reason,
        "requested": requested,
        "daily_used": daily_used,
        "hourly_used": hourly_used,
        "settings": settings,
        "circuit": circuit,
    }


def sweep_eligibility(cfg: dict[str, Any] | None = None,
                      *, now: float | None = None) -> dict[str, Any]:
    """Return when a complete sweep can reserve its rolling daily capacity.

    A sweep reservation intentionally does not pre-charge the hourly window.
    Each launch within the reserved sweep still consumes hourly capacity and
    observes randomized spacing as it runs.
    """
    if cfg is None:
        cfg = load_config()
    settings = effective_settings(cfg)
    estimate = estimate_sweep(cfg)
    estimated_units = int(estimate["units"] or 0)
    requested = max(1, estimated_units)
    now = float(now if now is not None else time.time())
    with _lock:
        _read_events_locked()
        _prune_locked(now)
        budget_ts = _expiry_for_units_locked(
            now, DAY_SECONDS, int(settings["daily"]), requested,
            "daily_units")
        launched_reservations = {
            str(row.get("reservation_id") or "")
            for row in _events
            if row.get("reservation_id")
            and row.get("kind") != "autosync_sweep_reservation"
            and int(row.get("hourly_units") or 0) > 0
        }
        previous_sweeps = [
            float(row["ts"]) for row in _events
            if row.get("kind") == "autosync_sweep_reservation"
            and str(row.get("reservation_id") or "")
            in launched_reservations
        ]
        last_sweep_ts = max(previous_sweeps, default=0.0)
        daily_used = _window_units_locked(
            now, DAY_SECONDS, "daily_units")
        circuit = _load_circuit_locked(now)
    proj = projection(cfg)
    recommended_hours = float(proj.get("recommended_hours") or 0)
    cadence_ts = (
        last_sweep_ts + recommended_hours * HOUR_SECONDS
        if last_sweep_ts and math.isfinite(recommended_hours)
        else now
    )
    next_ts = max(
        budget_ts,
        cadence_ts,
        float(circuit["cooldown_until"]) if circuit["active"] else now,
    )
    impossible = bool(
        estimated_units <= 0
        or (settings["daily"] > 0 and requested > settings["daily"]))
    return {
        "allowed": not impossible and next_ts <= now + 0.05,
        "impossible": impossible,
        "next_ts": None if impossible else next_ts,
        "wait_seconds": None if impossible else max(0, next_ts - now),
        "requested": requested,
        "daily_used": daily_used,
        "last_sweep_ts": last_sweep_ts or None,
        "cadence_ts": cadence_ts,
        "settings": settings,
        "circuit": circuit,
        "estimate": estimate,
    }


def _current_reservation() -> str:
    return str(getattr(_scope, "reservation_id", "") or "")


def acquire(kind: str, *, units: int = 1, cancel_event=None,
            pause_event=None, stream=None) -> dict[str, Any]:
    """Wait for and consume permission for one YouTube operation."""
    global _last_launch_ts, _next_gap_seconds
    requested = max(1, int(units or 1))
    reservation_id = _current_reservation()
    announced = False
    try:
        while True:
            if cancel_event is not None and cancel_event.is_set():
                return {"ok": False, "cancelled": True}
            if pause_event is not None and pause_event.is_set():
                return {"ok": False, "paused": True}
            now = time.time()
            cfg = load_config()
            override = budget_override_active()
            circuit = circuit_state(now)
            if circuit["active"]:
                resume = time.strftime(
                    "%I:%M%p", time.localtime(circuit["cooldown_until"])
                ).lstrip("0").lower()
                return {
                    "ok": False,
                    "cooldown": True,
                    "cooldown_until": circuit["cooldown_until"],
                    "error": (
                        "YouTube traffic is in an emergency rate-limit "
                        f"cooldown until {resume}."),
                }
            check = eligibility(
                requested, cfg, include_gap=True, ignore_limits=override,
                now=now)
            if check["impossible"]:
                return {
                    "ok": False,
                    "budget_blocked": True,
                    "error": (
                        f"{requested} operations cannot fit inside the "
                        "configured YouTube traffic budget."),
                }

            with _lock:
                _read_events_locked()
                reservation = _reservations.get(reservation_id)
                reserved_available = (
                    reservation is not None
                    and int(reservation.get("remaining") or 0) >= requested
                )
                # A reservation already consumed daily capacity up front.
                # Each launch still consumes the hourly window plus spacing.
                if reserved_available:
                    settings = effective_settings(cfg)
                    hour_at = now if override else _expiry_for_units_locked(
                        now, HOUR_SECONDS, int(settings["hourly"]), requested,
                        "hourly_units")
                    gap_at = (
                        _last_launch_ts
                        + max(float(settings["min_gap"]), _next_gap_seconds)
                        if _last_launch_ts else now)
                    next_at = max(hour_at, gap_at)
                    allowed = next_at <= now + 0.05
                    wait_seconds = max(0.0, next_at - now)
                    wait_reason = (
                        "hourly_limit"
                        if hour_at > now + 0.05 and hour_at >= gap_at
                        else "spacing"
                    )
                else:
                    # Re-check while holding the append lock so concurrent
                    # workers cannot oversubscribe a window or the gap.
                    check = eligibility(
                        requested, cfg, include_gap=True,
                        ignore_limits=override, now=now)
                    allowed = bool(check["allowed"])
                    wait_seconds = float(check["wait_seconds"] or 0)
                    wait_reason = check.get("wait_reason")

                if allowed:
                    settings = effective_settings(cfg)
                    if reserved_available:
                        persisted = _append_locked({
                            "ts": now,
                            "daily_units": 0,
                            "hourly_units": requested,
                            "kind": str(kind or "youtube"),
                            "reservation_id": reservation_id,
                        })
                    else:
                        persisted = _append_locked({
                            "ts": now,
                            "daily_units": requested,
                            "hourly_units": requested,
                            "kind": str(kind or "youtube"),
                            "reservation_id": "",
                        })
                    if not persisted:
                        return {
                            "ok": False,
                            "ledger_error": True,
                            "error": (
                                "YouTube traffic usage could not be saved, "
                                "so the remote operation was not launched."),
                        }
                    if reserved_available:
                        reservation["remaining"] = (
                            int(reservation["remaining"]) - requested)
                    _last_launch_ts = now
                    _next_gap_seconds = random.uniform(
                        float(settings["min_gap"]),
                        float(settings["max_gap"]),
                    )
                    return {
                        "ok": True,
                        "units": requested,
                        "kind": kind,
                        "reserved": reserved_available,
                        "daily_used": check["daily_used"],
                        "hourly_used": check["hourly_used"],
                        "override": override,
                    }

            if wait_reason in ("hourly_limit", "daily_limit"):
                _set_wait_state({
                    "reason": wait_reason,
                    "until": now + wait_seconds,
                    "hourly_used": int(check["hourly_used"]),
                    "hourly_limit": int(check["settings"]["hourly"]),
                    "daily_used": int(check["daily_used"]),
                    "daily_limit": int(check["settings"]["daily"]),
                })
            else:
                _set_wait_state(None)

            # A rolling-limit stop is meaningful even when the next event
            # expires in only a few seconds: without this line the UI appears
            # to stutter between requests with no explanation.  Keep the
            # five-second noise filter only for ordinary spacing waits.
            should_announce = (
                wait_reason in ("hourly_limit", "daily_limit")
                or wait_seconds >= 5
            )
            if not announced and stream is not None and should_announce:
                announced = True
                try:
                    resume = time.strftime(
                        "%I:%M%p", time.localtime(now + wait_seconds)
                    ).lstrip("0").lower()
                    settings = check["settings"]
                    if wait_reason == "hourly_limit":
                        stream.emit([
                            ["\u23f3 YouTube traffic safety: ",
                             ["simpleline", "traffic_wait"]],
                            [
                                "hourly limit reached "
                                f"({check['hourly_used']}/"
                                f"{settings['hourly']}). ",
                                ["dim", "traffic_wait"],
                            ],
                            [
                                f"Waiting for the next rolling slot at "
                                f"{resume}; sync will continue "
                                "automatically.\n",
                                ["dim", "traffic_wait"],
                            ],
                        ])
                    elif wait_reason == "daily_limit":
                        stream.emit([
                            ["\u23f3 YouTube traffic safety: ",
                             ["simpleline", "traffic_wait"]],
                            [
                                "24-hour limit reached "
                                f"({check['daily_used']}/"
                                f"{settings['daily']}). ",
                                ["dim", "traffic_wait"],
                            ],
                            [
                                f"Waiting for the next rolling slot at "
                                f"{resume}; sync will continue "
                                "automatically.\n",
                                ["dim", "traffic_wait"],
                            ],
                        ])
                    else:
                        stream.emit_dim(
                            f" YouTube traffic governor: waiting until "
                            f"{resume} before the next remote operation.")
                except Exception:
                    pass
            sleep_for = max(0.05, min(30.0, wait_seconds or 0.25))
            # Poll fast while a pause can arrive so the global control never
            # sits in "pause pending" for an hour-long budget wait.
            if pause_event is not None:
                sleep_for = min(sleep_for, 0.25)
            if not override and _override_wakeup.wait(timeout=0):
                continue
            if cancel_event is not None:
                if cancel_event.wait(timeout=sleep_for):
                    return {"ok": False, "cancelled": True}
            elif pause_event is not None:
                if pause_event.wait(timeout=sleep_for):
                    return {"ok": False, "paused": True}
            elif not override:
                _override_wakeup.wait(timeout=sleep_for)
            else:
                time.sleep(sleep_for)
    finally:
        _set_wait_state(None)
        if announced and stream is not None and not wait_status()["active"]:
            try:
                stream.emit([[json.dumps({
                    "kind": "clear_line", "marker": "traffic_wait",
                }), "__control__"]])
            except Exception:
                pass


def reserve_sweep(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Atomically reserve a complete estimated sweep for budget autosync."""
    if cfg is None:
        cfg = load_config()
    check = sweep_eligibility(cfg)
    estimate = check["estimate"]
    units = check["requested"]
    if not check["allowed"]:
        return {
            "ok": False,
            "budget_blocked": True,
            "impossible": check["impossible"],
            "next_ts": check["next_ts"],
            "wait_seconds": check["wait_seconds"],
            "units": units,
            "estimate": estimate,
        }
    reservation_id = uuid.uuid4().hex
    now = time.time()
    with _lock:
        _read_events_locked()
        # Re-check under the same lock before charging the reservation.
        recheck = sweep_eligibility(cfg, now=now)
        if not recheck["allowed"]:
            return {
                "ok": False,
                "budget_blocked": True,
                "impossible": recheck["impossible"],
                "next_ts": recheck["next_ts"],
                "wait_seconds": recheck["wait_seconds"],
                "units": units,
                "estimate": estimate,
            }
        persisted = _append_locked({
            "ts": now,
            "daily_units": units,
            "hourly_units": 0,
            "kind": "autosync_sweep_reservation",
            "reservation_id": reservation_id,
        })
        if not persisted:
            return {
                "ok": False,
                "ledger_error": True,
                "budget_blocked": True,
                "impossible": False,
                "next_ts": None,
                "wait_seconds": None,
                "units": units,
                "estimate": estimate,
                "error": (
                    "YouTube traffic usage could not be saved, so the "
                    "scheduled sync was not started."),
            }
        _reservations[reservation_id] = {
            "reserved": units,
            "remaining": units,
            "ts": now,
        }
    return {
        "ok": True,
        "reservation_id": reservation_id,
        "units": units,
        "estimate": estimate,
    }


@contextlib.contextmanager
def reservation_scope(reservation_id: str | None):
    previous = _current_reservation()
    _scope.reservation_id = str(reservation_id or "")
    try:
        yield
    finally:
        _scope.reservation_id = previous


def finish_reservation(reservation_id: str | None) -> dict[str, Any]:
    """Reduce a sweep's original reservation to its actual usage.

    The adjustment stays at the reservation timestamp.  Appending a later
    negative refund would let the positive row expire first and briefly mask
    unrelated traffic near the rolling 24-hour boundary.
    """
    rid = str(reservation_id or "")
    if not rid:
        return {"ok": True, "refunded": 0}
    with _lock:
        reservation = _reservations.pop(rid, None)
        if not reservation:
            # A process restart intentionally leaves the persisted reservation
            # charged for the remainder of its rolling window.
            return {"ok": True, "refunded": 0}
        unused = max(0, int(reservation.get("remaining") or 0))
        if not unused:
            return {"ok": True, "refunded": 0}
        reservation_row = next((
            row for row in _events
            if row.get("kind") == "autosync_sweep_reservation"
            and str(row.get("reservation_id") or "") == rid
        ), None)
        if reservation_row is None:
            # A sweep lasting beyond the rolling window needs no refund: its
            # original charge has already expired.  Never create an orphan
            # negative event that could offset other operations.
            return {"ok": True, "refunded": 0}
        before = max(0, int(reservation_row.get("daily_units") or 0))
        refunded = min(before, unused)
        reservation_row["daily_units"] = before - refunded
        persisted = _rewrite_locked()
        return {
            "ok": True,
            "refunded": refunded,
            "persisted": persisted,
        }


def status(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return live rolling usage plus projection for Settings/Onboarding."""
    if cfg is None:
        cfg = load_config()
    now = time.time()
    proj = projection(cfg)
    with _lock:
        _read_events_locked()
        _prune_locked(now)
        daily_used = _window_units_locked(
            now, DAY_SECONDS, "daily_units")
        hourly_used = _window_units_locked(
            now, HOUR_SECONDS, "hourly_units")
    settings = proj["settings"]
    circuit = circuit_state(now)
    return {
        "ok": True,
        "mode": settings["mode"],
        "daily_limit": settings["daily"],
        "hourly_limit": settings["hourly"],
        "min_gap": settings["min_gap"],
        "max_gap": settings["max_gap"],
        "daily_used": daily_used,
        "hourly_used": hourly_used,
        "daily_remaining": (
            None if settings["daily"] <= 0
            else max(0, settings["daily"] - daily_used)),
        "hourly_remaining": (
            None if settings["hourly"] <= 0
            else max(0, settings["hourly"] - hourly_used)),
        "projection": proj,
        "circuit": circuit,
    }


def _reset_for_tests(path: Path | None = None,
                     circuit_path: Path | None = None) -> None:
    """Reset process-local state; intentionally private test helper."""
    global _loaded, _events, _last_launch_ts, _next_gap_seconds
    global TRAFFIC_FILE, CIRCUIT_FILE, _budget_override_active
    with _lock:
        if path is not None:
            TRAFFIC_FILE = path
        if circuit_path is not None:
            CIRCUIT_FILE = circuit_path
        _loaded = False
        _events = []
        _last_launch_ts = 0.0
        _next_gap_seconds = 0.0
        _reservations.clear()
        _active_waits.clear()
        _wait_listeners.clear()
        _budget_override_active = False
        _override_wakeup.clear()
        _scope.reservation_id = ""
