"""Configuration snapshot selection for API adapters during service migration."""
from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from typing import Any


def config_snapshot(owner: Any, fallback_loader: Callable[[], dict]) -> dict[str, Any]:
    """Use the repository when composed; isolate legacy adapter snapshots.

    An existing services container is authoritative, including read errors.
    Cached adapter state is only supported for old service-less callers, never
    as an error fallback that could silently hide a failed repository read.
    """
    services = getattr(owner, "services", None)
    if services is not None:
        value = services.fresh_config()
    else:
        value = getattr(owner, "_config", None)
        if value is None:
            value = fallback_loader()
    if not isinstance(value, dict):
        raise TypeError("configuration snapshot must be an object")
    return deepcopy(value)
