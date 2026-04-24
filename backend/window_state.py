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

import os
from typing import Any, Dict, List, Optional, Tuple

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
    # audit E-43: if monitor config has changed since the save (dock
    # unplug, presentation mode turned off a secondary display,
    # DPI re-scale), clamp the saved x/y to the union of currently-
    # connected monitor work areas so the window never restores off-
    # screen. Windows-only (ctypes EnumDisplayMonitors); falls back
    # to the saved coords on any failure.
    if os.name == "nt" and out.get("x") is not None and out.get("y") is not None:
        try:
            import ctypes as _ct
            from ctypes import wintypes as _wt
            _rects: List[Tuple[int, int, int, int]] = []
            _MON_CB = _ct.WINFUNCTYPE(
                _ct.c_int, _ct.c_void_p, _ct.c_void_p,
                _ct.POINTER(_wt.RECT), _ct.c_double)

            def _cb(_hmon, _hdc, lprc, _lparam):
                r = lprc.contents
                _rects.append((int(r.left), int(r.top),
                               int(r.right), int(r.bottom)))
                return 1
            _ct.windll.user32.EnumDisplayMonitors(
                None, None, _MON_CB(_cb), 0)
            if _rects:
                _x = int(out["x"]); _y = int(out["y"])
                _w = int(out.get("width") or DEFAULT_STATE["width"])
                _h = int(out.get("height") or DEFAULT_STATE["height"])
                # Bug [31]: require a more substantial visible area
                # (was 100x100 px). 100px is small enough that the user
                # might still struggle to grab the window — saw this on
                # a multi-monitor setup where the window restored as a
                # 110px sliver on the right edge of the laptop screen.
                # 250x150 ensures the title bar AND a usable chunk are
                # actually reachable.
                _MIN_VIS_W, _MIN_VIS_H = 250, 150
                _visible = any(
                    (_x + _w > L and _x < R and _y + _h > T and _y < B)
                    and min(_x + _w, R) - max(_x, L) >= _MIN_VIS_W
                    and min(_y + _h, B) - max(_y, T) >= _MIN_VIS_H
                    for L, T, R, B in _rects)
                if not _visible:
                    L, T, R, B = _rects[0]
                    out["x"] = max(L, min(L + 40, R - _w))
                    out["y"] = max(T, min(T + 40, B - _h))
        except Exception:
            pass
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
