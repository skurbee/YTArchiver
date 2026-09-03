"""Focused contracts for Patch 5's extracted backend services."""

from __future__ import annotations

import pytest

from backend.services.config_repository import ConfigRepository
from backend.sync.download_commit import commit_download
from backend.transcribe.job_execution import (
    TranscriptionJobExecutor,
    WorkerOutcome,
    apply_control_signals,
    execution_decision,
)


def test_config_repository_uses_serialized_updater_as_commit_point():
    calls: list[str] = []
    live = {"theme": "dark"}

    def update(mutator):
        calls.append("update")
        result = mutator(live)
        return result, dict(live)

    repository = ConfigRepository(
        loader=lambda: {"stale": True},
        saver=lambda _value: False,
        updater=update,
    )

    result, snapshot = repository.mutate(
        lambda config: (config.update({"theme": "light"}) or "changed")
    )

    assert result == "changed"
    assert snapshot == {"theme": "light"}
    assert calls == ["update"]


def test_config_repository_reports_failed_fallback_commit():
    repository = ConfigRepository(
        loader=lambda: {"theme": "dark"},
        saver=lambda _value: False,
    )

    with pytest.raises(OSError, match="config save failed"):
        repository.mutate(lambda config: config.update({"theme": "light"}))


def test_download_commit_requires_durable_media_before_registration(tmp_path):
    partial = tmp_path / "video.mp4.part"
    partial.write_bytes(b"incomplete")
    calls = []

    result = commit_download(
        str(partial),
        "Example Channel",
        "Example Video",
        video_id="example0001",
        auto_transcribe=True,
        registrar=lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    assert not result.ok
    assert not result.durable_media
    assert calls == []


def test_download_commit_has_one_verified_registration_result(tmp_path):
    media = tmp_path / "video.mp4"
    media.write_bytes(b"complete media")
    calls = []

    def register(*args, **kwargs):
        calls.append((args, kwargs))
        return True

    result = commit_download(
        str(media),
        "Example Channel",
        "Example Video",
        video_id="example0001",
        auto_transcribe=False,
        duration="12.5",
        upload_date="20260831",
        registrar=register,
    )

    assert result.ok and result.durable_media and result.registered
    assert result.duration_seconds == 12.5
    assert len(calls) == 1
    assert calls[0][1] == {
        "tx_status": "no_captions",
        "video_id": "example0001",
        "duration_secs": 12.5,
        "upload_date": "20260831",
    }


def test_transcription_executor_never_treats_missing_result_as_success():
    invalid = []
    errors = []

    outcome = TranscriptionJobExecutor().run(
        lambda: None,
        on_invalid=invalid.append,
        on_error=errors.append,
    )

    assert outcome is WorkerOutcome.FAILED
    assert invalid == [None]
    assert errors == []
    assert execution_decision(outcome).pause_for_retry


def test_transcription_executor_maps_legacy_terminal_sentinels():
    def no_speech():
        raise RuntimeError("empty transcript")

    outcome = TranscriptionJobExecutor().run(
        no_speech,
        on_invalid=lambda _value: None,
        on_error=lambda _error: pytest.fail("terminal sentinel was reported"),
    )

    assert outcome is WorkerOutcome.NO_SPEECH
    assert execution_decision(outcome).terminal


def test_transcription_control_policy_keeps_shutdown_work_retryable():
    outcome = apply_control_signals(
        WorkerOutcome.CANCELLED,
        shutdown_requested=True,
        cancel_drop_requested=False,
        defer_requested=False,
        cancel_requested=True,
        output_complete=False,
        write_intent=False,
    )

    assert outcome is WorkerOutcome.RETRY
