"""One ordered command for the saved and running processing default."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .config_repository import CONFIG_COMMAND_LOCK

WHISPER_MODELS = frozenset({"tiny", "small", "medium", "large-v3"})


def validate_model(model: str) -> str:
    if model not in WHISPER_MODELS:
        raise ValueError("Unsupported model")
    return model


@dataclass(frozen=True)
class ModelChangeResult:
    model: str
    persisted: bool = False
    runtime_applied: bool = False
    snapshot: Any = None
    error: str = ""

    def as_dict(self):
        return {"ok": self.persisted or self.runtime_applied,
                "model": self.model, "persisted": self.persisted,
                "runtime_applied": self.runtime_applied,
                "deferred": self.persisted and not self.runtime_applied,
                **({"error": self.error} if self.error else {})}


def change_default_model(
    model: str, *, apply_model: Callable[[str], bool],
    persist: Callable[[], Any] | None = None,
) -> ModelChangeResult:
    """Validate, commit, then apply under one lock shared by both API routes.

    A failed commit never changes the manager. A rejected runtime transition
    after a successful save is explicitly deferred until restart. The manager
    still owns its in-flight-job policy and per-job model snapshots.
    """
    with CONFIG_COMMAND_LOCK:
        try:
            validate_model(model)
        except (TypeError, ValueError) as exc:
            return ModelChangeResult(str(model or ""), error=str(exc))
        snapshot = None
        if persist is not None:
            try:
                snapshot = persist()
            except Exception as exc:
                return ModelChangeResult(model, error=f"Save failed: {exc}")
        try:
            applied = bool(apply_model(model))
            error = "" if applied else "Runtime model change was not accepted."
        except Exception as exc:
            applied = False
            error = f"Runtime model change failed: {exc}"
        if not applied and persist is not None:
            error += " Saved default will apply after restart."
        return ModelChangeResult(model, persist is not None, applied, snapshot, error)
