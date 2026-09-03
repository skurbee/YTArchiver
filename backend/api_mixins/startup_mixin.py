"""
StartupMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

import threading
import uuid

from backend.services.job_supervisor import WorkAdmissionClosed
from backend.services.managed_work import start_managed_task


class StartupMixin:

    _startup_lock = threading.Lock()

    def startup_ready(self):
        """Called by JS on DOMContentLoaded. Kicks off the startup log sequence."""
        # Atomic check-and-set under a class-level lock so a rare
        # reload race (DOMContentLoaded firing twice during a hot
        # reload) doesn't spawn the startup sequence thread twice
        # and double up the boot log lines (audit: startup_mixin.py:
        # 18-22).
        with StartupMixin._startup_lock:
            if getattr(self, "_startup_fired", False):
                return {"ok": True, "already": True}
            self._startup_fired = True
        cancel = threading.Event()
        task_id = f"startup-indexing-{uuid.uuid4().hex}"
        self._startup_cancel = cancel

        def _run():
            try:
                self._run_startup_sequence(cancel)
            finally:
                if getattr(self, "_startup_cancel", None) is cancel:
                    self._startup_cancel = None

        try:
            self._startup_thread = start_managed_task(
                self,
                owner="startup-indexing",
                label="Startup archive checks and indexing",
                task_id=task_id,
                cancel=cancel,
                target=_run,
                name="startup-indexing",
                thread_factory=threading.Thread,
            )
        except WorkAdmissionClosed as exc:
            with StartupMixin._startup_lock:
                self._startup_fired = False
            self._startup_cancel = None
            return {"ok": False, "started": False, "error": str(exc)}
        except Exception as exc:
            with StartupMixin._startup_lock:
                self._startup_fired = False
            self._startup_cancel = None
            return {"ok": False, "started": False, "error": str(exc)}
        return {"ok": True}
