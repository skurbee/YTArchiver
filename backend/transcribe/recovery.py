"""Version-compatible processing records and explicit checkpoint transitions.

Events and callbacks belong to the runtime owner. Neither durable store may
serialize them. This codec owns the shared field names for the processing
journal and visible queue, including recovery of older records.
"""

from collections.abc import Mapping, MutableMapping
from dataclasses import asdict, dataclass, field, replace
from typing import Any


@dataclass(frozen=True, slots=True)
class RecoveryState:
    retry_required: bool = False
    retry_as_replace: bool = False
    write_intent: bool = False
    caption_recovery: bool = False
    skip_pending_counter: bool = False
    cleanup_only: bool = False
    no_speech_pending: bool = False
    stats_tallied: bool = False
    output_complete: bool = False
    callback_done: bool = False
    followup_pending: bool = False
    followup_enqueued: bool = False
    completed_outcome: str = ""
    defer_requested: bool = False

    @classmethod
    def decode(cls, source: Mapping[str, Any], *, runtime: bool = False,
               interrupted: bool = False) -> "RecoveryState":
        prefix = "_" if runtime else ""
        values = {
            name: (str(source.get(prefix + name) or "")
                   if name == "completed_outcome" else bool(source.get(prefix + name)))
            for name in cls.__dataclass_fields__
        }
        state = cls(**values)
        if interrupted:
            state = replace(
                state,
                retry_required=bool(state.retry_required or state.write_intent
                                    or state.cleanup_only or state.no_speech_pending),
                retry_as_replace=bool(state.retry_as_replace or (
                    state.write_intent and not state.caption_recovery)),
            )
        return state

    def apply(self, runtime: MutableMapping[str, Any], *, include_defaults: bool = False) -> None:
        """Apply live transitions without resurrecting cleared checkpoint keys.

        Durable records always encode every field. Runtime completion helpers
        deliberately remove finished markers; only construction of a restored
        runtime record should populate all false defaults again.
        """
        for name, value in asdict(self).items():
            key = "_" + name
            if value or include_defaults:
                runtime[key] = value
            elif key in runtime:
                if runtime[key]:
                    runtime.pop(key)
                else:
                    runtime[key] = value

    def output_finished(self, outcome: str, *, has_followup: bool) -> "RecoveryState":
        return replace(self, output_complete=True, completed_outcome=outcome,
                       followup_pending=bool(has_followup and not self.followup_enqueued))

    def followup_reserved(self) -> "RecoveryState":
        if not self.output_complete:
            raise ValueError("A follow-up requires completed output")
        return replace(self, followup_pending=False, followup_enqueued=True)

    def followup_failed(self) -> "RecoveryState":
        return replace(self, followup_pending=True, followup_enqueued=False)

    def operation_failed(self) -> "RecoveryState":
        return replace(self, retry_required=True, retry_as_replace=bool(
            self.retry_as_replace or (self.write_intent and not self.caption_recovery
                                     and not self.no_speech_pending and not self.cleanup_only)))

    def cleanup_failed(self) -> "RecoveryState":
        return replace(self, cleanup_only=True)


@dataclass(frozen=True, slots=True)
class ProcessingRecord:
    task_id: str = ""
    path: str = ""
    title: str = ""
    channel: str = ""
    video_id: str = ""
    retranscribe: bool = False
    combined_override: bool | None = None
    bulk_id: str = ""
    bulk_total: int = 0
    bulk_index: int = 0
    kind: str = "transcribe"
    from_download: bool = False
    quality: str = "Average"
    output_res: str = "720"
    compress_after: dict[str, Any] = field(default_factory=dict)
    requested_model: str = ""
    actual_model: str = ""
    recovery: RecoveryState = field(default_factory=RecoveryState)

    @classmethod
    def decode(cls, source: Mapping[str, Any], *, runtime: bool = False,
               interrupted: bool = False, default_model: str = "") -> "ProcessingRecord":
        return cls(
            task_id=str(source.get("task_id") or ""), path=str(source.get("path") or ""),
            title=str(source.get("title") or ""), channel=str(source.get("channel") or ""),
            video_id=str(source.get("video_id") or ""), retranscribe=bool(source.get("retranscribe")),
            combined_override=source.get("combined_override"), bulk_id=str(source.get("bulk_id") or ""),
            bulk_total=int(source.get("bulk_total", 0) or 0),
            bulk_index=int(source.get("bulk_index", 0) or 0),
            kind=str(source.get("kind") or "transcribe").lower(),
            from_download=bool(source.get("from_download")), quality=source.get("quality", "Average"),
            output_res=str(source.get("output_res", "720")),
            compress_after=dict(source.get("compress_after") or {}),
            requested_model=str(source.get("requested_model") or default_model),
            actual_model=str(source.get("actual_model") or ""),
            recovery=RecoveryState.decode(source, runtime=runtime, interrupted=interrupted),
        )

    def journal_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.update(payload.pop("recovery"))
        return payload

    def queue_payload(self) -> dict[str, Any]:
        payload = self.journal_payload()
        payload.pop("defer_requested")  # A pending row is the durable defer reservation.
        if self.kind == "compress":
            for name in ("combined_override", "retranscribe", "video_id", "compress_after",
                         "requested_model", "actual_model"):
                payload.pop(name)
        else:
            payload.pop("quality")
            payload.pop("output_res")
            payload["video_id"] = self.video_id.strip()
        return payload

    def runtime_payload(self, *, cancel_event: Any, consume_defer: bool = True) -> dict[str, Any]:
        """Restore data; the owner supplies a fresh event and reattaches callbacks."""
        payload = self.journal_payload()
        for name in self.recovery.__dataclass_fields__:
            payload.pop(name)
        self.recovery.apply(payload, include_defaults=True)
        if consume_defer:
            # Reconciliation already restored the durable tail order. Replaying
            # the old signal would defer this same task a second time.
            payload["_defer_requested"] = False
        payload["cb"] = None
        payload["cancel"] = cancel_event
        payload["video_id"] = self.video_id.strip()
        return payload
