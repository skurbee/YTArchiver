"""Result and control policy for one transcription/compression job."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class WorkerOutcome(StrEnum):
    """Explicit result contract between a processing task and its owner."""

    SUCCESS = "success"
    NO_SPEECH = "no_speech"
    RETRY = "retry"
    CANCELLED = "cancelled"
    FAILED = "failed"
    CLEANUP_FAILED = "cleanup_failed"


class OutputCancelled(RuntimeError):
    """A writer stopped at its cancellation checkpoint before committing."""


class EmptyTranscript(RuntimeError):
    """An output adapter has definitively found no speech to persist."""


@dataclass(frozen=True, slots=True)
class ExecutionDecision:
    outcome: WorkerOutcome
    terminal: bool
    pause_for_retry: bool


class TranscriptionJobExecutor:
    """Run one operation and guarantee an explicit processing outcome."""

    def run(
        self,
        operation: Callable[[], Any],
        *,
        on_invalid: Callable[[Any], None],
        on_error: Callable[[BaseException], None],
    ) -> WorkerOutcome:
        try:
            returned = operation()
        except OutputCancelled:
            return WorkerOutcome.CANCELLED
        except EmptyTranscript:
            return WorkerOutcome.NO_SPEECH
        except Exception as exc:
            on_error(exc)
            return WorkerOutcome.FAILED
        if isinstance(returned, WorkerOutcome):
            return returned
        on_invalid(returned)
        return WorkerOutcome.FAILED


def apply_control_signals(
    outcome: WorkerOutcome,
    *,
    shutdown_requested: bool,
    cancel_drop_requested: bool,
    defer_requested: bool,
    cancel_requested: bool,
    output_complete: bool,
    write_intent: bool,
) -> WorkerOutcome:
    """Resolve owner signals after the operation has stopped changing files."""
    if shutdown_requested and not cancel_drop_requested and not output_complete:
        return WorkerOutcome.RETRY
    if defer_requested:
        return WorkerOutcome.RETRY
    if cancel_drop_requested:
        return WorkerOutcome.CANCELLED
    if outcome is WorkerOutcome.FAILED and cancel_requested and not write_intent:
        return WorkerOutcome.CANCELLED
    return outcome


def execution_decision(outcome: WorkerOutcome) -> ExecutionDecision:
    terminal = outcome in {
        WorkerOutcome.SUCCESS,
        WorkerOutcome.NO_SPEECH,
        WorkerOutcome.CANCELLED,
    }
    return ExecutionDecision(
        outcome=outcome,
        terminal=terminal,
        pause_for_retry=outcome in {
            WorkerOutcome.FAILED,
            WorkerOutcome.CLEANUP_FAILED,
        },
    )


__all__ = [
    "EmptyTranscript",
    "ExecutionDecision",
    "OutputCancelled",
    "TranscriptionJobExecutor",
    "WorkerOutcome",
    "apply_control_signals",
    "execution_decision",
]
