"""
Window state persistence — save & restore window position/size/splitter
between launches. State lives in ytarchiver_config.json under
`window_state` so it roams with the user's other settings.

Schema:
    {
      "window_state": {
        "x": <int>, "y": <int>, "width": <int>, "height": <int>,
        "maximized": false,
        "splitter_top_px": 44,
        "col_widths": {"subs": {...}, "recent": {...}}
      }
    }
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .ytarchiver_config import load_config, save_config


DEFAULT_STATE: Dict[str, Any] = {
    "x": None,
    "y": None,
    "width": 1100,
    "height": 780,
    "maximized": False,
    "splitter_top_px": 44,
    "col_widths": {},
    "always_on_top": False,
}


def _sanitize_geometry(state: Dict[str, Any]) -> Dict[str, Any]:
    """Reject obviously-invalid saved geometry and fall back to defaults.

    Windows parks minimized windows at (-32000, -32000). If pywebview's
    window-state capture ever reads that position (observed 2026-04-21:
    the window was in a "normal but off-screen" state — IsIconic False,
    coords -32000/-32000), the value lands in config and the NEXT launch
    faithfully restores off every display. Alt-Tab shows the window but
    nothing clicks-through to it.

    Applied on both save and load so stale bad values get scrubbed out
    the moment either path touches them.

    Rules:
    - x/y outside +/- 10000 → reset to None (pywebview centers on primary)
    - width/height < 400/300 or > 20000 → reset to DEFAULT_STATE values
    """
    out = dict(state)
    x, y = out.get("x"), out.get("y")
    try:
        xi = int(x) if x is not None else None
    except (TypeError, ValueError):
        xi = None
    try:
        yi = int(y) if y is not None else None
    except (TypeError, ValueError):
        yi = None
    # Position sanity — negative coords are legal (multi-monitor left
    # side), but Windows' -32000 parking value and anything wildly
    # offscreen gets nuked.
    if xi is not None and (xi < -10000 or xi > 30000):
        xi = None
    if yi is not None and (yi < -10000 or yi > 30000):
        yi = None
    out["x"] = xi
    out["y"] = yi
    # Size sanity — force non-degenerate dimensions.
    try:
        w = int(out.get("width") or 0)
    except (TypeError, ValueError):
        w = 0
    try:
        h = int(out.get("height") or 0)
    except (TypeError, ValueError):
        h = 0
    if w < 400 or w > 20000:
        out["width"] = DEFAULT_STATE["width"]
    if h < 300 or h > 20000:
        out["height"] = DEFAULT_STATE["height"]
    return out


def load_window_state() -> Dict[str, Any]:
    cfg = load_config()
    state = dict(DEFAULT_STATE)
    saved = cfg.get("window_state") or {}
    if isinstance(saved, dict):
        state.update({k: v for k, v in saved.items() if k in DEFAULT_STATE})
    return _sanitize_geometry(state)


def save_window_state(partial: Dict[str, Any]) -> bool:
    """Merge `partial` into the saved state and persist (gated).

    Sanitizes geometry before persisting — a transient (-32000, -32000)
    read from the window would otherwise poison the config and trap
    every future launch off-screen.
    """
    cfg = load_config()
    current = cfg.get("window_state") or {}
    if not isinstance(current, dict):
        current = {}
    # Only accept known keys
    for k in DEFAULT_STATE.keys():
        if k in partial:
            current[k] = partial[k]
    current = _sanitize_geometry(current)
    cfg["window_state"] = current
    return bool(save_config(cfg))
