"""Shared channel-level state for metadata refresh operations."""
from __future__ import annotations

import time
from typing import Any

from ..log import get_logger
from ..ytarchiver_config import ConfigUnchanged

_log = get_logger(__name__)


def stamp_channel_refresh(channel: dict[str, Any], field: str) -> bool:
    """Persist a successful channel-level refresh/check timestamp."""
    name = channel.get("name") or channel.get("folder") or "?"
    try:
        from .. import ytarchiver_config as config_backend

        with config_backend.config_transaction() as cfg:
            channel_url = (channel.get("url") or "").rstrip("/")
            for saved_channel in cfg.get("channels", []):
                saved_url = (saved_channel.get("url") or "").rstrip("/")
                if saved_url == channel_url:
                    saved_channel[field] = time.time()
                    return True
            raise ConfigUnchanged()
    except ConfigUnchanged:
        return False
    except Exception as exc:
        _log.warning("%s stamp failed for %r: %s", field, name, exc)
        return False
