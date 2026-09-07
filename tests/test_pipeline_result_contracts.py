"""Explicit pipeline outcomes keep errors out of persisted metadata."""

import io
import threading
from unittest import mock

import pytest

from backend.metadata import fetcher
from backend.metadata.results import FetchStatus, MetadataFetchResult
from backend.transcribe.acceptance import EnqueueStatus
from backend.transcribe.core import TranscribeManager
from backend.transcribe.job_execution import TranscriptionJobExecutor, WorkerOutcome


@pytest.mark.parametrize("status", list(FetchStatus))
def test_metadata_outcomes_preserve_bridge_flags_without_payload(status):
    if status is FetchStatus.SUCCESS:
        result = MetadataFetchResult.success({"title": "_timeout is ordinary text"})
        assert result.legacy_value() == result.metadata
        with pytest.raises(ValueError):
            result.api_failure()
    else:
        result = MetadataFetchResult(status, detail="specific diagnostic")
        response = result.api_failure()
        assert response["ok"] is False
        assert response["code"] == status.value
        assert response["error"] == "specific diagnostic"
        assert result.metadata is None
        assert bool(response.get("cancelled")) == (status is FetchStatus.CANCELLED)
        assert bool(response.get("transient")) == (status is FetchStatus.TIMEOUT)


def test_metadata_contract_refuses_failure_payload():
    with pytest.raises(ValueError):
        MetadataFetchResult(FetchStatus.FAILED, metadata={"_timeout": True})


def test_exception_wording_cannot_turn_a_failure_into_success():
    error = RuntimeError("database failed while saving empty transcript")
    errors = []
    def operation():
        raise error
    outcome = TranscriptionJobExecutor().run(
        operation, on_invalid=lambda _value: None, on_error=errors.append)
    assert outcome is WorkerOutcome.FAILED
    assert errors == [error]


def test_enqueue_rejections_preserve_reason_and_legacy_bool(tmp_path):
    manager = TranscribeManager(mock.Mock())
    missing = manager.enqueue_result(str(tmp_path / "absent.mp4"))
    assert missing.status is EnqueueStatus.MISSING_FILE
    assert missing.api_payload()["error"]
    assert manager.enqueue(str(tmp_path / "absent.mp4")) is False
    manager._shutdown_requested.set()
    assert manager.enqueue_result("unused").status is EnqueueStatus.SHUTDOWN


def test_enqueue_duplicate_and_journal_failure_have_distinct_contracts(tmp_path):
    video = tmp_path / "video.mp4"
    video.write_bytes(b"fixture")
    manager = TranscribeManager(mock.Mock())
    with mock.patch.object(manager, "_persist_pending", return_value=False):
        rejected = manager.enqueue_result(str(video))
    assert rejected.status is EnqueueStatus.SAVE_FAILED
    assert rejected.api_payload()["retryable"]
    assert manager._jobs == []
    manager._jobs = [{"kind": "transcribe", "path": str(video)}]
    assert manager.enqueue_result(str(video)).status is EnqueueStatus.DUPLICATE


def test_typed_fetch_preserves_process_error_and_legacy_adapter():
    def process(*_args, **_kwargs):
        proc = mock.Mock(returncode=1, pid=None)
        proc.poll.return_value = 1
        proc.stdout = io.StringIO("")
        proc.stderr = io.StringIO("warning\nERROR: private video")
        return proc
    with (
        mock.patch.object(fetcher, "popen_ytdlp", side_effect=process),
        mock.patch.object(fetcher, "_find_cookie_source", return_value=[]),
        mock.patch("backend.youtube_traffic.acquire", return_value={"ok": True}),
        mock.patch("backend.youtube_session.handle_youtube_failure_text", return_value=""),
    ):
        result = fetcher._fetch_video_metadata_result("fixture", "abc123def45")
        errors = []
        legacy = fetcher._fetch_video_metadata("fixture", "abc123def45", error_out=errors)
    assert result.status is FetchStatus.FAILED
    assert result.detail == "private video"
    assert legacy is None
    assert errors == ["private video"]


def test_cancelled_fetch_does_not_launch_process():
    cancelled = threading.Event()
    cancelled.set()
    with mock.patch.object(fetcher, "popen_ytdlp") as launch:
        result = fetcher._fetch_video_metadata_result("fixture", "abc123def45", cancel_event=cancelled)
    assert result.status is FetchStatus.CANCELLED
    launch.assert_not_called()


@pytest.mark.parametrize("status", [FetchStatus.CANCELLED, FetchStatus.TIMEOUT,
                                   FetchStatus.COOKIE, FetchStatus.RATE_LIMIT, FetchStatus.FAILED])
def test_single_fetch_failures_never_write_metadata(tmp_path, status):
    with (
        mock.patch.object(fetcher, "find_yt_dlp", return_value="fixture"),
        mock.patch.object(fetcher, "_get_metadata_jsonl_path", return_value=(str(tmp_path / "m.jsonl"), str(tmp_path))),
        mock.patch.object(fetcher, "_read_metadata_jsonl", return_value={}),
        mock.patch.object(fetcher, "_fetch_video_metadata_result", return_value=MetadataFetchResult(status, detail="diagnostic")),
        mock.patch.object(fetcher, "_write_metadata_jsonl") as write,
        mock.patch.object(fetcher, "_download_thumbnail") as thumbnail,
    ):
        result = fetcher.fetch_single_video_metadata(
            {"name": "fixture"}, "abc123def45", str(tmp_path / "v.mp4"),
            "video", mock.Mock(), emit_inline_log=False, dest_folder=str(tmp_path))
    assert result["code"] == status.value
    write.assert_not_called()
    thumbnail.assert_not_called()
