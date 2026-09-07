"""An explicit owner for the native process-instance lease."""
from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any


class InstanceLease:
    def __init__(self, acquire: Callable[[], tuple[Any, bool]],
                 release: Callable[[Any], bool]):
        self._acquire = acquire
        self._release = release
        self._lock = threading.Lock()
        self._handle = None

    def acquire(self) -> bool:
        """Return False when another process owns the lease."""
        with self._lock:
            if self._handle is not None:
                return True
            handle, already_exists = self._acquire()
            if handle is None:
                raise OSError("Could not acquire the application instance lease")
            if already_exists:
                if not self._release(handle):
                    raise OSError("Could not close the duplicate instance lease")
                return False
            self._handle = handle
            return True

    def release(self) -> None:
        """Release once; retain ownership and raise if the native close fails."""
        with self._lock:
            if self._handle is None:
                return
            if not self._release(self._handle):
                raise OSError("Could not release the application instance lease")
            self._handle = None
