"""Completed-download phases after tool output has been bound to exact media.

Path discovery/promotion stays in the download session. This boundary receives
an immutable observation, registers it, accounts for it once, and dispatches
follow-ups through explicit ports. Presentation tags are isolated in the log
adapter so processing policy does not need to know the frontend row protocol.
"""

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ..log import swallow
from .download_commit import DownloadCommitResult


@dataclass(frozen=True, slots=True)
class DownloadObservation:
    title: str
    uploader: str
    upload_date: str
    size: str
    duration: str
    video_id: str

    @classmethod
    def parse(cls, line: str) -> "DownloadObservation | None":
        parts = line.split(":::")
        if len(parts) < 7 or parts[0] != "DLTRACK":
            return None
        uploader, uploaded, size, duration, video_id = parts[-5:]
        return cls(":::".join(parts[1:-5]).strip(), uploader, uploaded,
                   size, duration, video_id.strip())


@dataclass(frozen=True, slots=True)
class CompletedMedia:
    path: str
    channel: str
    title: str
    video_id: str
    upload_date: str
    duration: Any
    size_bytes: int | None = None
    filename_id_is_provenance: bool = False


@dataclass(frozen=True, slots=True)
class CompletionDecision:
    result: DownloadCommitResult
    count_download: bool
    count_error: bool


@dataclass
class CompletionLedger:
    committed_ids: list[str]
    counted_ids: set[str] = field(default_factory=set)
    failed_ids: set[str] = field(default_factory=set)

    def has_completed(self, video_id: str) -> bool:
        """Only a successful commit suppresses replay within this pass."""
        return video_id in self.counted_ids

    def failed_once(self, video_id: str) -> bool:
        if video_id in self.failed_ids:
            return False
        self.failed_ids.add(video_id)
        return True

    def register(self, media: CompletedMedia, *, auto_transcribe: bool,
                 commit: Callable[..., DownloadCommitResult]) -> CompletionDecision:
        result = commit(media.path, media.channel, media.title,
                        video_id=media.video_id, auto_transcribe=auto_transcribe,
                        duration=media.duration, upload_date=media.upload_date,
                        filename_id_is_provenance=media.filename_id_is_provenance)
        if not result.ok:
            return CompletionDecision(result, False, self.failed_once(media.video_id))
        first = media.video_id not in self.counted_ids
        if first:
            self.counted_ids.add(media.video_id)
            if media.video_id not in self.committed_ids:
                self.committed_ids.append(media.video_id)
        return CompletionDecision(result, first, False)


@dataclass(frozen=True)
class CompletionLog:
    stream: Any
    compress_placeholder: Callable[[Any, str], None]
    clear_compress_placeholder: Callable[[Any, str], None]

    def metadata_queued(self, video_id: str) -> None:
        marker = f"meta_done_{video_id}"
        self.stream.emit([["      — ⏳ ", ["meta_bracket", marker]],
                          ["Metadata queued…\n", ["simpleline", marker]]])

    def transcription(self, video_id: str, state: str) -> None:
        marker = f"tx_done_{video_id}"
        if state == "duplicate":
            self.stream.emit([["      — ", ["dim", marker]],
                              ["Transcription already queued.\n", ["dim", marker]]])
            return
        message = ("Checking YouTube captions…" if state == "captions"
                   else "Transcription queued in Processing…")
        self.stream.emit([["      — ⏳ ", ["whisper_bracket", marker]],
                          [message + "\n", ["simpleline", marker]]])

    def compression(self, path: str, *, clear: bool = False) -> None:
        action = self.clear_compress_placeholder if clear else self.compress_placeholder
        action(self.stream, path)


@dataclass
class DownloadFollowups:
    log: CompletionLog
    record_recent: Callable[..., None]
    submit_metadata: Callable[[str, str, str], None]
    append_pending_transcription: Callable[[str, str], None]
    drop_livestream: Callable[[str], None]
    processing: Any

    def dispatch(self, media: CompletedMedia, *, duration_seconds: float | None,
                 metadata_enabled: bool, auto_transcribe: bool,
                 compression: dict[str, Any] | None) -> None:
        """Run independent post-download effects; preserve existing failure policy."""
        stream = self.log.stream
        try:
            self.record_recent(media.path, media.channel, media.title, media.video_id,
                               upload_date=media.upload_date.strip(), size_bytes=media.size_bytes,
                               duration_secs=duration_seconds)
        except Exception as exc:
            stream.emit_dim(f" (recent downloads write failed: {exc})")
        try:
            self.drop_livestream(media.video_id)
        except Exception as exc:
            # Reminder cleanup is best effort and does not invalidate the media.
            swallow("deferred-livestream drop", exc)
        if metadata_enabled and media.video_id:
            self.log.metadata_queued(media.video_id)
        self.submit_metadata(media.video_id, media.title, media.path)
        if not auto_transcribe:
            try:
                self.append_pending_transcription(media.channel, media.video_id)
            except Exception as exc:
                stream.emit_dim(f" (pending-transcribe list write failed: {exc})")
        if auto_transcribe and self.processing is not None:
            self.log.transcription(media.video_id, "captions")
            if compression:
                self.log.compression(media.path)
            route = self.processing.route_download_transcription(
                media.path, media.title or os.path.splitext(os.path.basename(media.path))[0],
                channel=media.channel, video_id=media.video_id, compress_after=compression,
                on_processing_queued=lambda: self.log.transcription(media.video_id, "queued"))
            if route == "duplicate":
                self.log.transcription(media.video_id, "duplicate")
        elif compression:
            if self.processing is None:
                stream.emit_error("Video compression was not queued because the processing "
                                  "owner is unavailable. The downloaded original was left unchanged.")
                return
            self.log.compression(media.path)
            try:
                queued = self.processing.compress_enqueue(
                    media.path, title=os.path.splitext(os.path.basename(media.path))[0],
                    channel=media.channel, quality=compression["quality"],
                    output_res=compression["output_res"], from_download=True)
            except Exception as exc:
                queued = False
                stream.emit_error(f"Couldn't queue video compression: {exc}")
            if not queued:
                self.log.compression(media.path, clear=True)
