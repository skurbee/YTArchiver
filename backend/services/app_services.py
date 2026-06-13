"""Application service container.

This is the first small step away from `main.Api` as the implicit owner of
every backend dependency. Existing mixins can keep using `self._queues`,
`self._log_stream`, etc. while new work moves toward explicit
`self.services.<dependency>` access.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.log_stream import LogStreamer
    from backend.queues import QueueState
    from backend.services.event_bus import BridgeEventBus
    from backend.transcribe import TranscribeManager


ConfigLoader = Callable[[], dict[str, Any]]
ConfigSaver = Callable[[dict[str, Any]], bool]


@dataclass(slots=True)
class AppServices:
    """Long-lived dependencies shared across Api mixins.

    Keep this as a thin dependency holder. Domain behavior should live in
    named services added beside this file, not grow here.
    """

    load_config: ConfigLoader
    save_config: ConfigSaver
    queues: QueueState
    log_stream: LogStreamer
    transcribe: TranscribeManager
    event_bus: BridgeEventBus

    def fresh_config(self) -> dict[str, Any]:
        """Read the latest config from disk through the injected loader."""
        return self.load_config() or {}
