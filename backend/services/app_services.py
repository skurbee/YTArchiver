"""Application service container.

The composition root injects repositories and feature services. Existing
adapters still expose legacy attributes, but extracted workflows receive
their collaborators explicitly rather than discovering them on Api.
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
    from backend.services.application_information import ApplicationInformation
    from backend.services.event_bus import BridgeEventBus
    from backend.services.startup_sequence import StartupSequence
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
    startup: StartupSequence | None = None
    information: ApplicationInformation | None = None

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
