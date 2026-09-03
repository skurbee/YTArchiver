"""Application service container.

This is the first small step away from `main.Api` as the implicit owner of
every backend dependency. Existing mixins can keep using `self._queues`,
`self._log_stream`, etc. while new work moves toward explicit
`self.services.<dependency>` access.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from backend.services.config_repository import (
    ConfigLoader,
    ConfigRepository,
    ConfigSaver,
    ConfigUpdater,
)

if TYPE_CHECKING:
    from backend.log_stream import LogStreamer
    from backend.queues import QueueState
    from backend.services.event_bus import BridgeEventBus
    from backend.transcribe import TranscribeManager


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
    update_config: ConfigUpdater | None = None
    config_repository: ConfigRepository | None = None

    def __post_init__(self) -> None:
        if self.config_repository is None:
            self.config_repository = ConfigRepository(
                self.load_config,
                self.save_config,
                self.update_config,
            )

    def fresh_config(self) -> dict[str, Any]:
        """Read the latest config from disk through the injected loader."""
        assert self.config_repository is not None
        return self.config_repository.load()

    def mutate_config(self, mutator):
        """Apply one serialized read-modify-write config transaction."""
        assert self.config_repository is not None
        return self.config_repository.mutate(mutator)
