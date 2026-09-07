"""Completed observations keep exact identity through accounting and callbacks."""

from unittest import mock

from backend.sync.completion import (
    CompletedMedia,
    CompletionLedger,
    CompletionLog,
    DownloadFollowups,
    DownloadObservation,
)
from backend.sync.download_commit import DownloadCommitResult


def test_observation_anchors_trailing_fields_when_title_contains_delimiter():
    parsed = DownloadObservation.parse("DLTRACK:::A:::B:::Uploader:::20260905:::2048:::10:::abc123def45")
    assert parsed.title == "A:::B"
    assert parsed.video_id == "abc123def45"
    assert parsed.duration == "10"
    assert DownloadObservation.parse("DLTRACK:::truncated") is None


def test_registration_failure_retry_and_duplicate_are_accounted_once():
    media = CompletedMedia("video.mp4", "Channel", "Title", "abc123def45", "20260905", 10)
    failed = DownloadCommitResult(False, media.path, media.video_id, 10, True, False, "busy")
    success = DownloadCommitResult(True, media.path, media.video_id, 10, True, True)
    commit = mock.Mock(side_effect=[failed, failed, success, success])
    committed = []
    ledger = CompletionLedger(committed)
    decisions = [ledger.register(media, auto_transcribe=False, commit=commit) for _ in range(4)]
    assert [d.count_error for d in decisions] == [True, False, False, False]
    assert [d.count_download for d in decisions] == [False, False, True, False]
    assert committed == [media.video_id]


def test_late_processing_callback_retains_its_own_video_row():
    stream = mock.Mock()
    processing = mock.Mock()
    log = CompletionLog(stream, mock.Mock(), mock.Mock())
    ports = DownloadFollowups(log, mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock(), processing)
    first = CompletedMedia("first.mp4", "C", "First", "first123456", "", 10)
    second = CompletedMedia("second.mp4", "C", "Second", "second12345", "", 10)
    ports.dispatch(first, duration_seconds=10, metadata_enabled=True, auto_transcribe=True, compression=None)
    callback = processing.route_download_transcription.call_args.kwargs["on_processing_queued"]
    ports.dispatch(second, duration_seconds=10, metadata_enabled=True, auto_transcribe=True, compression=None)
    callback()
    assert "tx_done_first123456" in str(stream.emit.call_args)
    assert "tx_done_second12345" not in str(stream.emit.call_args)


def test_rejected_compression_clears_its_placeholder_and_keeps_pending_transcription():
    processing = mock.Mock()
    processing.compress_enqueue.return_value = False
    log = CompletionLog(mock.Mock(), mock.Mock(), mock.Mock())
    pending = mock.Mock()
    ports = DownloadFollowups(log, mock.Mock(), mock.Mock(), pending, mock.Mock(), processing)
    media = CompletedMedia("f.mp4", "C", "F", "abc123def45", "", 10)
    ports.dispatch(media, duration_seconds=10, metadata_enabled=False, auto_transcribe=False,
                   compression={"quality": "Average", "output_res": "720"})
    log.clear_compress_placeholder.assert_called_once_with(log.stream, "f.mp4")
    pending.assert_called_once_with("C", "abc123def45")
