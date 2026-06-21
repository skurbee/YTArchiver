"""
Version constants - moved here by Patch 7 so both main.py and the
api_mixins package can read the same authoritative value without a
circular import.

DEPLOY NOTE: bump APP_VERSION here (not main.py) on every git push.
The +0.1 / single-decimal rollover rule still applies. Both main.py
and `backend/api_mixins/_shared.py` import these names.
"""
from __future__ import annotations

APP_VERSION      = "v78.8"
APP_VERSION_DATE = "6.21.26 10:56AM"