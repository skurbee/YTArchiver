"""
System-tray icon — pystray integration.

Lazy-loads pystray so it's only a requirement when tray is actually used.
If pystray/PIL aren't installed, the app just runs without a tray icon.

Usage:
    tray = TrayController(on_show=..., on_quit=...)
    tray.start()   # background thread
    ...
    tray.stop()
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Callable, Optional


ICON_CANDIDATES = [
    Path(__file__).resolve().parent.parent / "icon.ico",
    Path.cwd() / "icon.ico",
]


def _find_icon() -> Optional[Path]:
    for p in ICON_CANDIDATES:
        if p.exists():
            return p
    return None


# pystray's right-click context menu follows the Win32 popup-menu theme,
# which defaults to LIGHT even when the app's main window is dark. To
# force the menu dark we opt the process into dark-mode-aware popup
# menus via `uxtheme.dll`'s undocumented ordinals 135 + 136:
#
#   SetPreferredAppMode(ForceDark)  — ordinal 135, Win10 1903+
#   FlushMenuThemes()               — ordinal 136, reapplies to existing
#
# These are undocumented Microsoft APIs used by Explorer, Notepad,
# VS Code, Chrome, etc. Ordinal-based calls are brittle across OS
# builds, so this is strictly best-effort: any failure silently
# degrades to the default light-theme menu (matches classic).
def _apply_dark_menu_theme() -> bool:
    """Call SetPreferredAppMode(ForceDark) via uxtheme.dll ordinal 135.

    Returns True if both ordinals were resolved + called without error.
    Silent on failure — older Windows builds or non-Windows platforms
    just don't get themed menus.
    """
    if os.name != "nt":
        return False
    try:
        import ctypes
        from ctypes import wintypes
        # Load uxtheme.dll
        uxtheme = ctypes.WinDLL("uxtheme", use_last_error=True)
        # Resolve exports by ORDINAL. GetProcAddress treats an integer
        # less than 65536 as MAKEINTRESOURCE(ordinal), so passing it via
        # c_void_p works without manually building a MAKEINTRESOURCE.
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.GetProcAddress.restype  = ctypes.c_void_p
        kernel32.GetProcAddress.argtypes = [wintypes.HMODULE, ctypes.c_void_p]
        hmod = ctypes.c_void_p(uxtheme._handle)
        set_preferred_addr = kernel32.GetProcAddress(hmod, 135)
        flush_menu_addr    = kernel32.GetProcAddress(hmod, 136)
        if not set_preferred_addr or not flush_menu_addr:
            return False
        # SetPreferredAppMode(int mode) — 2 == ForceDark
        SetPreferredAppMode = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_int)(set_preferred_addr)
        FlushMenuThemes     = ctypes.WINFUNCTYPE(None)(flush_menu_addr)
        SetPreferredAppMode(2)  # ForceDark
        FlushMenuThemes()
        return True
    except Exception:
        return False


class TrayController:
    def __init__(self,
                 on_show: Optional[Callable[[], None]] = None,
                 on_hide: Optional[Callable[[], None]] = None,
                 on_sync: Optional[Callable[[], None]] = None,
                 on_quit: Optional[Callable[[], None]] = None,
                 tooltip: str = "YT Archiver"):
        self._on_show = on_show
        self._on_hide = on_hide
        self._on_sync = on_sync
        self._on_quit = on_quit
        self._on_top_toggle = None
        self._always_on_top = False
        # Auto-Sync submenu — None until wired via set_autorun_menu(...)
        self._autorun_labels = None       # list[str] of interval labels
        self._autorun_get_label = None    # callable -> current label
        self._autorun_set_label = None    # callable(label) -> None
        self._tooltip = tooltip
        self._icon = None
        self._thread: Optional[threading.Thread] = None
        self._started = False
        # Spin animation state
        self._base_img = None
        self._Image = None
        self._ImageDraw = None
        self._spin_thread: Optional[threading.Thread] = None
        self._spin_stop = threading.Event()
        self._spin_color = (80, 160, 240, 255)
        self._spin_interval = 0.18
        # Badge overlay — downloaded-this-session count. 0 = no badge.
        # Mirrors OLD's _tray_set_badge (YTArchiver.py:3457).
        self._badge_count: int = 0

    def _build_menu(self):
        try:
            import pystray
        except ImportError:
            return None
        items = []
        if self._on_show:
            items.append(pystray.MenuItem("Show YT Archiver", lambda: self._on_show(), default=True))
        if self._on_hide:
            items.append(pystray.MenuItem("Hide", lambda: self._on_hide()))
        items.append(pystray.Menu.SEPARATOR)
        if self._on_sync:
            items.append(pystray.MenuItem("Sync Subbed", lambda: self._on_sync()))
        # Auto-Sync submenu — radio-selectable intervals, mirrors YTArchiver:3662.
        # pystray only accepts 0/1/2-arg callbacks (pystray/_base.py:561 raises
        # ValueError if co_argcount > 2). Use closure factories so each
        # MenuItem gets a fresh 2-arg action fn + 1-arg checked fn — no
        # default-arg inflation of __code__.co_argcount.
        if self._autorun_labels and self._autorun_set_label:
            def _make_cb(lbl):
                def _cb(icon, item):
                    try: self._autorun_set_label(lbl)
                    except Exception: pass
                return _cb
            def _make_checked(lbl):
                def _checked(item):
                    try:
                        return (self._autorun_get_label() == lbl) if self._autorun_get_label else False
                    except Exception:
                        return False
                return _checked
            auto_items = []
            for label in self._autorun_labels:
                auto_items.append(pystray.MenuItem(
                    label, _make_cb(label),
                    checked=_make_checked(label), radio=True))
            items.append(pystray.MenuItem("Auto-Sync", pystray.Menu(*auto_items)))
        if self._on_top_toggle:
            items.append(pystray.MenuItem(
                "Always on top",
                lambda: self._on_top_toggle(),
                checked=lambda item: self._always_on_top,
            ))
        items.append(pystray.Menu.SEPARATOR)
        if self._on_quit:
            items.append(pystray.MenuItem("Quit", lambda: self._quit_clicked()))
        return pystray.Menu(*items)

    def set_autorun_menu(self, labels, get_label, set_label):
        """Wire the Auto-Sync submenu. Must be set BEFORE start().

        labels     : list[str] — e.g. ["Off", "30 min", "1 hr", ...]
        get_label  : () -> str  — returns currently active label
        set_label  : (str) -> None — fired when user clicks an interval
        """
        self._autorun_labels = list(labels or [])
        self._autorun_get_label = get_label
        self._autorun_set_label = set_label

    def set_on_top_toggle(self, fn, initial: bool = False):
        """Wire an 'Always on top' toggle. Must be set BEFORE start()."""
        self._on_top_toggle = fn
        self._always_on_top = bool(initial)

    def _quit_clicked(self):
        try:
            if self._icon:
                self._icon.stop()
        except Exception:
            pass
        if self._on_quit:
            try:
                self._on_quit()
            except Exception:
                pass

    def start(self) -> bool:
        if self._started:
            return True
        try:
            import pystray
            from PIL import Image, ImageDraw, ImageFont
        except ImportError:
            print("[tray] pystray / Pillow not installed; tray disabled.")
            return False
        # Attach ImageFont to ImageDraw so _compose_badge can use
        # `self._ImageDraw.ImageFont.truetype(...)` without a fresh import.
        try: setattr(ImageDraw, "ImageFont", ImageFont)
        except Exception: pass

        icon_path = _find_icon()
        if icon_path is None:
            print("[tray] icon.ico not found; tray disabled.")
            return False

        try:
            self._base_img = Image.open(icon_path).convert("RGBA")
        except Exception as e:
            print(f"[tray] could not load icon: {e}")
            return False

        self._Image = Image
        self._ImageDraw = ImageDraw

        # Opt this process into dark-themed Win32 popup menus BEFORE the
        # tray icon (and its context menu) spawns. Best-effort — if it
        # fails (non-Windows, older builds, ordinal drift) we fall back
        # to the default light menu.
        _apply_dark_menu_theme()

        self._icon = pystray.Icon("YTArchiver", self._base_img, self._tooltip,
                                  menu=self._build_menu())

        def _run():
            try:
                self._icon.run()
            except Exception as e:
                print(f"[tray] icon.run crashed: {e}")

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        self._started = True
        return True

    # ── Spin animation (matches YTArchiver's tray busy indicator) ──

    def start_spin(self, color: str = "blue"):
        """Start a rotating-dot overlay on the tray icon.
        `color` = "blue" (sync) or "red" (gpu)."""
        if not self._started or not self._icon:
            return
        self._spin_color = (80, 160, 240, 255) if color == "blue" else (230, 80, 80, 255)
        # OLD uses 0.18s/frame for blue (sync), 0.12s/frame for red (GPU) — red
        # spins faster to signal GPU work is active. Mirrors YTArchiver.py:3481.
        self._spin_interval = 0.12 if color == "red" else 0.18
        if self._spin_thread is not None:
            return  # already spinning
        self._spin_stop.clear()
        self._spin_thread = threading.Thread(target=self._spin_loop, daemon=True)
        self._spin_thread.start()

    def stop_spin(self):
        self._spin_stop.set()
        self._spin_thread = None
        # Restore base icon (possibly with badge if a count is set)
        if self._icon and self._base_img:
            try:
                self._icon.icon = self._compose_badge(self._base_img.copy()) \
                    if self._badge_count else self._base_img
            except Exception:
                pass

    def set_badge(self, count: int):
        """Overlay a small count badge on the tray icon (downloaded-this-session).
        Matches YTArchiver.py:3457 _tray_set_badge. 0 clears the badge."""
        try:
            n = int(count or 0)
        except Exception:
            n = 0
        if n == self._badge_count:
            return
        self._badge_count = max(0, n)
        # If not spinning, re-render the base icon with the new badge
        if self._spin_thread is None and self._icon and self._base_img:
            try:
                self._icon.icon = (self._compose_badge(self._base_img.copy())
                                   if self._badge_count else self._base_img)
            except Exception:
                pass

    def _compose_badge(self, img):
        """Draw `self._badge_count` into the bottom-right of an icon copy."""
        try:
            draw = self._ImageDraw.Draw(img)
            w, h = img.size
            # Red circle in the bottom-right
            r = max(6, min(w, h) // 3)
            cx, cy = w - r - 1, h - r - 1
            draw.ellipse([cx - r, cy - r, cx + r, cy + r],
                         fill=(220, 40, 40, 255))
            # Text: the count (or 9+ if large)
            txt = str(self._badge_count) if self._badge_count < 10 else "9+"
            # Best-effort font — pystray's PIL dep always bundles default
            try:
                font = self._ImageDraw.ImageFont.truetype("arial.ttf", max(9, r))
            except Exception:
                font = None
            try:
                if font:
                    bbox = draw.textbbox((0, 0), txt, font=font)
                    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
                    draw.text((cx - tw // 2, cy - th // 2 - 1), txt,
                              fill=(255, 255, 255, 255), font=font)
                else:
                    draw.text((cx - 3, cy - 5), txt, fill=(255, 255, 255, 255))
            except Exception:
                pass
        except Exception:
            pass
        return img

    def _spin_loop(self):
        import math
        frame = 0
        while not self._spin_stop.is_set():
            try:
                img = self._base_img.copy()
                draw = self._ImageDraw.Draw(img)
                # Draw a small dot rotating around the bottom-right
                w, h = img.size
                cx, cy = w - 6, h - 6
                r = 4
                angle = (frame * 45) % 360
                x = cx + r * math.cos(math.radians(angle))
                y = cy + r * math.sin(math.radians(angle))
                draw.ellipse([x - 2, y - 2, x + 2, y + 2], fill=self._spin_color)
                # Overlay the badge on top of the spin (if set)
                if self._badge_count:
                    img = self._compose_badge(img)
                if self._icon:
                    self._icon.icon = img
            except Exception:
                pass
            frame += 1
            self._spin_stop.wait(self._spin_interval)

    def stop(self):
        if self._icon:
            try:
                self._icon.stop()
            except Exception:
                pass
        self._started = False

    def set_tooltip(self, text: str):
        self._tooltip = text
        if self._icon:
            try:
                self._icon.title = text
            except Exception:
                pass
