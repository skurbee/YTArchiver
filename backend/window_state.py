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


def load_window_state() -> Dict[str, Any]:
    cfg = load_config()
    state = dict(DEFAULT_STATE)
    saved = cfg.get("window_state") or {}
    if isinstance(saved, dict):
        state.update({k: v for k, v in saved.items() if k in DEFAULT_STATE})
    return state


def save_window_state(partial: Dict[str, Any]) -> bool:
    """Merge `partial` into the saved state and persist (gated)."""
    cfg = load_config()
    current = cfg.get("window_state") or {}
    if not isinstance(current, dict):
        current = {}
    # Only accept known keys
    for k in DEFAULT_STATE.keys():
        if k in partial:
            current[k] = partial[k]
    cfg["window_state"] = current
    return bool(save_config(cfg))
