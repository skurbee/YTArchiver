"""Authoritative application version shared by the UI and API modules.

DEPLOY NOTE: bump APP_VERSION here (not main.py) on every git push.
The +0.1 / single-decimal rollover rule still applies. Both main.py
and `backend/api_mixins/_shared.py` import these names.
"""
from __future__ import annotations

APP_VERSION      = "v83.5"
APP_VERSION_DATE = "9.3.26 12:36AM"
