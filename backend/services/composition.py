"""Construct bridge services without starting workers or a desktop window."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from backend.services.app_services import AppServices
from backend.services.application_information import ApplicationInformation
from backend.services.config_repository import ConfigRepository

if TYPE_CHECKING:
    from backend.log_stream import LogStreamer
    from backend.queues import QueueState
    from backend.services.event_bus import BridgeEventBus
    from backend.transcribe import TranscribeManager


def compose_application_services(
    *,
    config: ConfigRepository,
    config_path: str,
    can_write: Callable[[], bool],
    queues: QueueState,
    log_stream: LogStreamer,
    transcribe: TranscribeManager,
    event_bus: BridgeEventBus,
) -> AppServices:
    """Wire explicit feature owners; runtime admission remains with Api."""
    return AppServices(
        load_config=config.load,
        save_config=config.saver,
        update_config=config.updater,
        config_repository=config,
        queues=queues,
        log_stream=log_stream,
        transcribe=transcribe,
        event_bus=event_bus,
        information=ApplicationInformation(config, config_path, log_stream, can_write),
    )
