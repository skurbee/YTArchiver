"""
StartupMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class StartupMixin:

    def startup_ready(self):
        """Called by JS on DOMContentLoaded. Kicks off the startup log sequence."""
        if getattr(self, "_startup_fired", False):
            return {"ok": True, "already": True}
        self._startup_fired = True
        threading.Thread(target=self._run_startup_sequence, daemon=True).start()
        return {"ok": True}
