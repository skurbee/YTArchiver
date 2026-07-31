"""Windows taskbar overlay icon support.

The system-tray icon and the taskbar button are separate Windows surfaces.
pystray animates the notification-area icon, while ITaskbarList3 owns the
small overlay drawn on the app's taskbar button.  This module is deliberately
best-effort: unsupported Windows builds or COM failures simply return False.
"""
from __future__ import annotations

import ctypes
import os
import uuid
from ctypes import wintypes

from .log import get_logger

_log = get_logger(__name__)

_CLSID_TASKBAR_LIST = uuid.UUID("56FDF344-FD6D-11D0-958A-006097C9A090")
_IID_ITASKBAR_LIST3 = uuid.UUID("EA1AFB91-9E28-4B86-90E9-9E9F8A5EEFAF")
_CLSCTX_INPROC_SERVER = 0x1
_COINIT_APARTMENTTHREADED = 0x2
_RPC_E_CHANGED_MODE = -2147417850
_SET_OVERLAY_ICON_INDEX = 18


class _GUID(ctypes.Structure):
    _fields_ = [
        ("Data1", ctypes.c_uint32),
        ("Data2", ctypes.c_uint16),
        ("Data3", ctypes.c_uint16),
        ("Data4", ctypes.c_ubyte * 8),
    ]

    @classmethod
    def from_uuid(cls, value: uuid.UUID) -> "_GUID":
        raw = value.bytes_le
        return cls(
            int.from_bytes(raw[0:4], "little"),
            int.from_bytes(raw[4:6], "little"),
            int.from_bytes(raw[6:8], "little"),
            (ctypes.c_ubyte * 8)(*raw[8:]),
        )


class WindowsTaskbarOverlay:
    """Thread-affine ITaskbarList3 wrapper.

    Create, use, and close an instance on the same thread.  The spinner loop
    follows that rule; one-shot badge updates create their own short-lived
    instance on the calling thread.
    """

    def __init__(self, hwnd: int):
        self._hwnd = int(hwnd or 0)
        self._iface = ctypes.c_void_p()
        self._ole32 = None
        self._com_owned = False
        self.available = False
        if os.name != "nt" or not self._hwnd:
            return
        try:
            self._ole32 = ctypes.OleDLL("ole32")
            self._ole32.CoInitializeEx.argtypes = [
                ctypes.c_void_p, ctypes.c_uint32]
            self._ole32.CoInitializeEx.restype = ctypes.c_long
            init_hr = int(self._ole32.CoInitializeEx(
                None, _COINIT_APARTMENTTHREADED))
            if init_hr in (0, 1):
                self._com_owned = True
            elif init_hr != _RPC_E_CHANGED_MODE:
                return

            self._ole32.CoCreateInstance.argtypes = [
                ctypes.POINTER(_GUID),
                ctypes.c_void_p,
                ctypes.c_uint32,
                ctypes.POINTER(_GUID),
                ctypes.POINTER(ctypes.c_void_p),
            ]
            self._ole32.CoCreateInstance.restype = ctypes.c_long
            clsid = _GUID.from_uuid(_CLSID_TASKBAR_LIST)
            iid = _GUID.from_uuid(_IID_ITASKBAR_LIST3)
            hr = int(self._ole32.CoCreateInstance(
                ctypes.byref(clsid),
                None,
                _CLSCTX_INPROC_SERVER,
                ctypes.byref(iid),
                ctypes.byref(self._iface),
            ))
            if hr < 0 or not self._iface:
                self.close()
                return
            hr_init = self._method(3, ctypes.c_long)(self._iface)
            if int(hr_init) < 0:
                self.close()
                return
            self.available = True
        except Exception as exc:
            _log.debug("taskbar overlay initialization failed: %s", exc)
            self.close()

    def _method(self, index: int, restype, *argtypes):
        table = ctypes.cast(
            self._iface,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_void_p)),
        ).contents
        address = table[index]
        prototype = ctypes.WINFUNCTYPE(
            restype, ctypes.c_void_p, *argtypes)
        return prototype(address)

    def set_icon_handle(self, hicon, description: str = "") -> bool:
        if not self.available:
            return False
        try:
            call = self._method(
                _SET_OVERLAY_ICON_INDEX,
                ctypes.c_long,
                wintypes.HWND,
                wintypes.HICON,
                wintypes.LPCWSTR,
            )
            hr = int(call(
                self._iface,
                wintypes.HWND(self._hwnd),
                hicon,
                str(description or ""),
            ))
            return hr >= 0
        except Exception as exc:
            _log.debug("taskbar overlay update failed: %s", exc)
            return False

    def set_pil_image(self, image, description: str = "") -> bool:
        """Convert one Pillow image to HICON and apply it.

        SetOverlayIcon copies the handle, so it is safe to destroy our HICON
        immediately after the call.
        """
        if not self.available or image is None:
            return False
        try:
            from pystray._util import serialized_image
            from pystray._util import win32

            with serialized_image(image, "ICO") as icon_path:
                hicon = win32.LoadImage(
                    None,
                    icon_path,
                    win32.IMAGE_ICON,
                    0,
                    0,
                    win32.LR_DEFAULTSIZE | win32.LR_LOADFROMFILE,
                )
                try:
                    return self.set_icon_handle(hicon, description)
                finally:
                    win32.DestroyIcon(hicon)
        except Exception as exc:
            _log.debug("taskbar overlay image conversion failed: %s", exc)
            return False

    def clear(self) -> bool:
        return self.set_icon_handle(wintypes.HICON(), "")

    def close(self) -> None:
        if self._iface:
            try:
                release = self._method(2, ctypes.c_ulong)
                release(self._iface)
            except Exception:
                pass
            self._iface = ctypes.c_void_p()
        self.available = False
        if self._com_owned and self._ole32 is not None:
            try:
                self._ole32.CoUninitialize()
            except Exception:
                pass
        self._com_owned = False

    def __enter__(self) -> "WindowsTaskbarOverlay":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.close()


__all__ = ["WindowsTaskbarOverlay"]
