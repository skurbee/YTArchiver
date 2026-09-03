"""Explicit repository boundary for application configuration state."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

ConfigLoader = Callable[[], dict[str, Any]]
ConfigSaver = Callable[[dict[str, Any]], bool]
ConfigUpdater = Callable[[Callable[[dict[str, Any]], Any]], tuple[Any, dict[str, Any]]]


@dataclass(slots=True)
class ConfigRepository:
    """Own config reads and serialized commit operations."""

    loader: ConfigLoader
    saver: ConfigSaver
    updater: ConfigUpdater | None = None

    def load(self) -> dict[str, Any]:
        value = self.loader() or {}
        if not isinstance(value, dict):
            raise TypeError("config loader must return an object")
        return value

    def replace(self, value: dict[str, Any]) -> dict[str, Any]:
        if not self.saver(value):
            raise OSError("config save failed")
        return value

    def mutate(
        self,
        mutator: Callable[[dict[str, Any]], Any],
    ) -> tuple[Any, dict[str, Any]]:
        if self.updater is not None:
            return self.updater(mutator)
        config = self.load()
        result = mutator(config)
        self.replace(config)
        return result, config


__all__ = [
    "ConfigLoader",
    "ConfigRepository",
    "ConfigSaver",
    "ConfigUpdater",
]
