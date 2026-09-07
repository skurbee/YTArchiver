"""Native desktop startup. Importing this module does not launch the app."""

from __future__ import annotations

import ctypes
import os
import sys
from pathlib import Path

from backend.services.instance_lease import InstanceLease


def _open_instance_mutex():
    from ctypes import wintypes
    kernel = ctypes.windll.kernel32
    kernel.CreateMutexW.restype = wintypes.HANDLE
    kernel.CreateMutexW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR]
    handle = kernel.CreateMutexW(None, False, "Local\\YTArchiver_SingleInstance")
    return handle, kernel.GetLastError() == 183


def _close_instance_mutex(handle):
    from ctypes import wintypes
    kernel = ctypes.windll.kernel32
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    return bool(kernel.CloseHandle(handle))


INSTANCE_LEASE = InstanceLease(_open_instance_mutex, _close_instance_mutex)


def configure_browser_environment() -> None:
    """Disable WebView2's video overlay plane before the control is created."""
    if os.name != "nt":
        return
    key = "WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS"
    switch = "--disable-direct-composition-video-overlays"
    current = os.environ.get(key, "").strip()
    if switch in current:
        return
    os.environ[key] = f"{current} {switch}".strip()


def ensure_single_instance() -> None:
    """Acquire the process lease, or focus the existing app and exit."""
    if os.name == "nt":
        if not INSTANCE_LEASE.acquire():
            # Another instance is running — focus its window and exit.
            import ctypes.wintypes as _wt

            _WNDENUMPROC = ctypes.WINFUNCTYPE(ctypes.c_bool, _wt.HWND, _wt.LPARAM)

            def _window_belongs_to_this_exe(hwnd) -> bool:
                pid = _wt.DWORD()
                try:
                    ctypes.windll.user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
                    if not pid.value:
                        return False
                    _k32 = ctypes.windll.kernel32
                    _k32.OpenProcess.restype = _wt.HANDLE
                    h_proc = _k32.OpenProcess(
                        0x1000, False, pid.value
                    )  # PROCESS_QUERY_LIMITED_INFORMATION
                    if not h_proc:
                        return False
                    try:
                        size = _wt.DWORD(32768)
                        buf = ctypes.create_unicode_buffer(size.value)
                        ok = _k32.QueryFullProcessImageNameW(h_proc, 0, buf, ctypes.byref(size))
                        if not ok:
                            return False
                        return Path(buf.value).name.lower() == Path(sys.executable).name.lower()
                    finally:
                        _k32.CloseHandle(h_proc)
                except Exception:
                    return False

            def _find_and_focus(hwnd, _):
                _n = ctypes.windll.user32.GetWindowTextLengthW(hwnd)
                if _n > 0:
                    _buf = ctypes.create_unicode_buffer(_n + 1)
                    ctypes.windll.user32.GetWindowTextW(hwnd, _buf, _n + 1)
                    # Accept both historical title spellings when focusing an
                    # existing instance.
                    _tv = _buf.value
                    if _tv in {"YTArchiver", "YT Archiver"} and _window_belongs_to_this_exe(hwnd):
                        ctypes.windll.user32.ShowWindow(hwnd, 9)  # SW_RESTORE
                        ctypes.windll.user32.SetForegroundWindow(hwnd)
                        return False
                return True

            _cb = _WNDENUMPROC(_find_and_focus)
            ctypes.windll.user32.EnumWindows(_cb, 0)
            print("[YTArchiver] Another instance is already running.")
            sys.exit(0)


def prepare_desktop(web_root: Path):
    """Prepare native prerequisites only for an explicit application launch."""
    configure_browser_environment()
    ensure_single_instance()
    try:
        import webview
    except ImportError:
        try:
            import ctypes

            ctypes.windll.user32.MessageBoxW(
                None,
                "YTArchiver requires pywebview.\n\n"
                "Install with Python 3.13:\n"
                " Python313\\python.exe -m pip install pywebview",
                "YTArchiver",
                0x10,
            )
        except Exception as e:
            # Logging is not installed during native preparation. Report the
            # secondary dialog failure without masking the import failure.
            print(f"[YTArchiver] pywebview ImportError MessageBox failed: {e}")
        sys.exit(1)
    from backend.html_assembler import assemble_index_html

    try:
        assemble_index_html(web_root)
    except Exception as exc:
        print(f"[html_assembler] could not (re)build index.html: {exc}")
    return webview
