"""
LivestreamsMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class LivestreamsMixin:

    # ─── Deferred livestreams ──────────────────────────────────────────

    def livestreams_list(self):
        """JS bridge: return the current Deferred Livestreams drawer
        contents. Each entry is a livestream / premiere URL that yt-dlp
        couldn't grab yet (e.g. the stream is upcoming or in progress)
        and is being held for a later retry."""
        try:
            from backend import livestreams as _ls
            return {"ok": True, "items": _ls.list_deferred()}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def livestreams_drop(self, video_id):
        """JS bridge: remove a single deferred livestream entry from
        the drawer. Called when a deferred stream successfully
        downloads on a later sync (so we stop showing it as pending),
        or when the user clicks the row's `×` to dismiss it manually."""
        try:
            from backend import livestreams as _ls
            return {"ok": _ls.drop(video_id)}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def livestreams_ignore(self, video_id):
        """Permanently skip this deferred livestream/premiere. Adds
        the video_id to the ignore set so future sync passes never
        re-defer it. Mirrors a "don't show this again" action."""
        try:
            from backend import livestreams as _ls
            return {"ok": _ls.ignore(video_id)}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def livestreams_snooze(self, seconds):
        """Hide the deferred-livestreams drawer for `seconds` from now.
        UI's "Retry in 24hrs / 1 week" dropdown uses this to suppress
        the drawer without forgetting the entries.
        """
        # Explicit validation up front — old `float(seconds or 0)`
        # raised ValueError on garbage strings, then the outer try/
        # except surfaced the bare Python error verbatim (audit:
        # livestreams_mixin.py:60).
        try:
            _sec = float(seconds or 0)
        except (TypeError, ValueError):
            return {"ok": False,
                    "error": f"Snooze duration must be a number (got "
                             f"{seconds!r})."}
        if _sec < 0:
            return {"ok": False,
                    "error": "Snooze duration must be a non-negative number."}
        try:
            from backend import livestreams as _ls
            return {"ok": _ls.snooze_drawer(_sec)}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def livestreams_drawer_state(self):
        """Return {snooze_until_ts, now_ts, visible} so the UI can
        decide whether to render the drawer at all."""
        try:
            from backend import livestreams as _ls
            return {"ok": True, **_ls.drawer_state()}
        except Exception as e:
            return {"ok": False, "error": str(e)}
