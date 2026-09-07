"""Metadata consumers preserve shared supervisor policy and completeness."""

from unittest import mock

import pytest

from backend.metadata import catalog, fetcher
from backend.metadata.results import FetchStatus
from backend.process_runner import StreamingRunResult


@pytest.mark.parametrize("complete", [True, False])
def test_catalog_absence_conclusions_require_complete_output(complete):
    proc = mock.Mock(returncode=0, pid=None)
    def supervised(_proc, **kwargs):
        kwargs["on_stdout_line"]("abcdefghijk\t100\t5\t2\tTitle\t20260905\t10")
        assert kwargs["idle_timeout"] == 60.0
        assert "pause_event" in kwargs
        return StreamingRunResult(0, [], output_complete=complete)
    with (
        mock.patch.object(catalog, "popen_ytdlp", return_value=proc),
        mock.patch.object(catalog, "supervise_streaming_process", side_effect=supervised),
        mock.patch.object(catalog, "_find_cookie_source", return_value=[]),
        mock.patch.object(catalog.youtube_traffic, "acquire", return_value={"ok": True}),
    ):
        result = catalog._flat_playlist_bulk_stats("fixture", "https://example.invalid/channel", mock.Mock())
    assert result["abcdefghijk"]["view_count"] == 100
    assert result.complete is complete


def test_title_match_rejects_partial_catalog_even_if_observed_title_is_unique():
    def supervised(_proc, **kwargs):
        kwargs["on_stdout_line"]("abcdefghijk\tTitle")
        return StreamingRunResult(0, [], output_complete=False)
    with (
        mock.patch.object(catalog, "popen_ytdlp"),
        mock.patch.object(catalog, "supervise_streaming_process", side_effect=supervised),
        mock.patch.object(catalog, "_find_cookie_source", return_value=[]),
        mock.patch.object(catalog.youtube_traffic, "acquire", return_value={"ok": True}),
        mock.patch("backend.youtube_session.handle_youtube_failure_text", return_value=""),
    ):
        result = catalog._resolve_ids_by_title("fixture", "https://example.invalid/channel",
                                               ["Title.mp4"], mock.Mock())
    assert result == {}


def test_capture_retries_timeouts_but_not_cancelled_processes():
    proc = mock.Mock(pid=None)
    with (
        mock.patch.object(fetcher, "popen_ytdlp", return_value=proc),
        mock.patch.object(fetcher, "_find_cookie_source", return_value=[]),
        mock.patch("backend.youtube_traffic.acquire", return_value={"ok": True}),
        mock.patch.object(fetcher.time, "sleep"),
        mock.patch.object(fetcher, "supervise_streaming_process", side_effect=[
            StreamingRunResult(-1, [], timed_out=True, output_complete=False),
            StreamingRunResult(-1, [], cancelled=True, output_complete=False),
        ]) as supervise,
    ):
        result = fetcher._fetch_video_metadata_result("fixture", "abcdefghijk")
    assert result.status is FetchStatus.CANCELLED
    assert supervise.call_count == 2
    assert [call.kwargs["timeout"] for call in supervise.call_args_list] == [60, 60]
    assert proc not in fetcher._inflight_procs
