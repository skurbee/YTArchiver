"""Replay download completion events with isolated storage and no network."""

import threading
from concurrent.futures import Future
from types import SimpleNamespace
from unittest import mock

import pytest

from backend import metadata
from backend.metadata import fetcher
from backend.metadata.results import FetchStatus, MetadataFetchResult
from backend.sync import core
from backend.sync.completion import CompletedMedia, CompletionLedger
from backend.sync.download_commit import CollisionSafePathResult, DownloadCommitResult


def _text(segments):
    return "".join(segment[0] for segment in segments)


def _metadata_lines(stream):
    return [call.args[0] for call in stream.emit.call_args_list
            if any(str(tag).startswith("meta_done_")
                   for segment in call.args[0]
                   for tag in (segment[1] if isinstance(segment[1], list) else [segment[1]]))]


@pytest.fixture
def run_sync(monkeypatch, tmp_path):
    channel = {"name": "Fixture", "url": "https://www.youtube.com/@fixture",
               "auto_metadata": True, "auto_transcribe": True, "mode": "new"}
    folder = tmp_path / "Fixture"
    folder.mkdir()
    cfg = {"output_dir": str(tmp_path), "channels": [channel]}
    for name, value in {
        "find_yt_dlp": "fixture-ytdlp",
        "load_config": cfg,
        "_find_cookie_source": [],
        "config_is_writable": False,
    }.items():
        monkeypatch.setattr(core, name, mock.Mock(return_value=value))
    for name in ("write_sync_progress", "_record_recent_download", "_hide_sidecar_win",
                 "_bg_channel_maintenance", "finish_ytdlp_process"):
        monkeypatch.setattr(core, name, mock.Mock())
    monkeypatch.setattr(core, "popen_ytdlp_process",
                        mock.Mock(return_value=mock.Mock(returncode=0, pid=None)))
    watchdog = SimpleNamespace(last_output=[0], stop=mock.Mock(), stop_event=threading.Event(),
                                stalled={}, output_complete=True)
    monkeypatch.setattr(core, "start_download_watchdog", mock.Mock(return_value=watchdog))
    monkeypatch.setattr(core, "finalize_collision_safe_bundle",
                        lambda path, _vid: CollisionSafePathResult(True, path, False, False))
    monkeypatch.setattr("backend.subs.streams_url", lambda _url: None)
    monkeypatch.setattr("backend.utils.check_directory_writable", lambda _path: True)
    monkeypatch.setattr("backend.utils.check_disk_space", lambda *_: True)
    monkeypatch.setattr("backend.archive_scan.update_disk_cache_for_channel", mock.Mock())
    monkeypatch.setattr("backend.channel_art.fetch_channel_art", mock.Mock())
    monkeypatch.setattr("backend.channel_cache.append_ids", mock.Mock())
    monkeypatch.setattr("backend.livestreams.drop", mock.Mock())
    monkeypatch.setattr("backend.sync.recent_track.fire_recent_changed_hook", mock.Mock())
    # Any accidentally unstubbed tool launch is a test failure, never traffic.
    monkeypatch.setattr("subprocess.Popen", mock.Mock(side_effect=AssertionError("unexpected process")))

    def run(*, videos=1, repeats=2, failures=0, metadata_result=None, cancel=None,
            cancel_after_output=False):
        lines = []
        for index in range(videos):
            video_id = f"fixture{index:04d}"
            path = folder / f"Video {index}.mp4"
            path.write_bytes(b"fixture media")
            lines.append(f"[download] Destination: {path}\n")
            lines.extend([f"DLTRACK:::Video {index}:::Fixture:::20260901:::13:::60:::{video_id}\n"]
                         * repeats)
        def output(*_):
            yield from (line.encode() for line in lines)
            if cancel_after_output:
                cancel.set()
        monkeypatch.setattr(core, "iter_download_output", output)
        commit_calls = []

        def commit(path, _channel, _title, *, video_id, **_):
            commit_calls.append(video_id)
            success = len(commit_calls) > failures
            return DownloadCommitResult(success, path, video_id, 60, True, success,
                                        "fixture commit failed" if not success else "")

        monkeypatch.setattr(core, "commit_download", commit)
        processing = mock.Mock()
        processed = []
        caption_finished = threading.Event()

        def caption(_path, _title, **kwargs):
            processed.append(kwargs["video_id"])
            caption_finished.set()
            return "inline"

        processing.route_download_transcription.side_effect = caption
        processing.get_channel_batch_stats.side_effect = lambda _: {"done": len(processed), "err": 0}
        processing.has_pending_transcription.return_value = False
        metadata_calls = []

        def fetch(_channel, video_id, *_args, **kwargs):
            assert kwargs["emit_terminal_log"] is False
            # Captions must run while the metadata job is pending.
            assert caption_finished.wait(1), "metadata was serialized ahead of captions"
            metadata_calls.append(video_id)
            if isinstance(metadata_result, Exception):
                raise metadata_result
            return metadata_result if metadata_result is not None else {"ok": True, "fetched": True}

        monkeypatch.setattr(metadata, "fetch_single_video_metadata", fetch)
        stream = mock.Mock(simple_mode=True)
        result = core._sync_channel_impl(channel, stream, transcribe_mgr=processing,
                                         cancel_event=cancel)
        return SimpleNamespace(result=result, stream=stream, processed=processed,
                               metadata_calls=metadata_calls, commit_calls=commit_calls)

    return run


def test_repeated_completion_does_not_repeat_followups_or_reset_metadata(run_sync):
    completed = run_sync(videos=11)
    assert completed.result["downloaded"] == 11
    assert completed.result["errors"] == 0
    assert len(completed.commit_calls) == len(completed.processed) == len(completed.metadata_calls) == 11
    activity = completed.stream.emit_activity.call_args.args[0]
    assert activity["primary"] == "11 downloaded"
    assert activity["secondary"] == "11 transcribed"
    assert activity["tertiary"] == "11 metadata"
    rows = _metadata_lines(completed.stream)
    assert sum("queued" in _text(row) for row in rows) == 11
    assert sum("downloaded" in _text(row) for row in rows) == 11


def test_failed_registration_can_retry_but_does_not_dispatch_premature_followups(run_sync):
    completed = run_sync(repeats=4, failures=2)
    assert completed.result["downloaded"] == 1
    assert len(completed.commit_calls) == 3
    assert completed.processed == completed.metadata_calls == ["fixture0000"]
    assert completed.result["errors"] == 1


@pytest.mark.parametrize("outcome,expected,errors", [
    ({"ok": True, "skipped": True}, "already saved", 0),
    ({"ok": False, "cancelled": True}, "cancelled", 0),
    ({"ok": False, "rate_limited": True}, "deferred by YouTube cooldown", 1),
    ({"ok": False, "error": "fixture error"}, "failed; retry needed", 1),
    (ValueError("fixture failure"), "failed; retry needed", 1),
])
def test_worker_terminal_outcomes_replace_queued_and_failures_reach_summary(run_sync, outcome, expected, errors):
    completed = run_sync(metadata_result=outcome)
    rows = _metadata_lines(completed.stream)
    assert len(rows) == 2
    assert "queued" in _text(rows[0])
    assert expected in _text(rows[1])
    assert all("meta_done_fixture0000" in segment[1] for segment in rows[1])
    assert completed.result["errors"] == errors
    assert completed.stream.emit_activity.call_args.args[0]["errors"] == f"{errors} error" + ("s" if errors != 1 else "")


def test_commit_history_only_blocks_successful_video_ids():
    media = CompletedMedia("fixture.mp4", "Fixture", "Title", "fixture0000", "20260901", 60)
    ledger = CompletionLedger([])
    failed = DownloadCommitResult(False, media.path, media.video_id, 60, True, False)
    succeeded = DownloadCommitResult(True, media.path, media.video_id, 60, True, True)
    assert not ledger.has_completed(media.video_id)
    ledger.register(media, auto_transcribe=True, commit=lambda *_, **__: failed)
    assert not ledger.has_completed(media.video_id)
    ledger.register(media, auto_transcribe=True, commit=lambda *_, **__: succeeded)
    assert ledger.has_completed(media.video_id)
    assert not ledger.has_completed("fixture0001")


def test_cancelling_unstarted_metadata_future_replaces_queued_placeholder(monkeypatch, run_sync):
    pending = Future()
    executor = mock.Mock()
    executor.submit.return_value = pending
    monkeypatch.setattr("concurrent.futures.ThreadPoolExecutor", lambda **_: executor)
    cancel = threading.Event()
    completed = run_sync(cancel=cancel, cancel_after_output=True)
    assert pending.cancelled()
    assert completed.metadata_calls == []
    rows = _metadata_lines(completed.stream)
    assert len(rows) == 2
    assert "queued" in _text(rows[0])
    assert "cancelled" in _text(rows[1])
    assert all("meta_done_fixture0000" in segment[1] for segment in rows[1])


@pytest.fixture
def metadata_fetch(monkeypatch, tmp_path):
    monkeypatch.setattr(fetcher, "find_yt_dlp", lambda: "fixture-ytdlp")
    monkeypatch.setattr(fetcher, "_get_metadata_jsonl_path",
                        lambda *_: (str(tmp_path / "metadata.jsonl"), str(tmp_path)))
    monkeypatch.setattr(fetcher, "_download_thumbnail", mock.Mock(return_value=False))
    monkeypatch.setattr("backend.index.update_video_stats", mock.Mock())
    stored = {}
    monkeypatch.setattr(fetcher, "_read_metadata_jsonl", lambda *_, **__: dict(stored))
    monkeypatch.setattr(fetcher, "_write_metadata_jsonl", lambda _path, rows: stored.update(rows))
    network = mock.Mock(return_value=MetadataFetchResult.success({"video_id": "fixture0000"}))
    monkeypatch.setattr(fetcher, "_fetch_video_metadata_result", network)

    def run(**kwargs):
        stream = mock.Mock()
        result = fetcher.fetch_single_video_metadata(
            {"name": "Fixture"}, "fixture0000", str(tmp_path / "Video.mp4"),
            "Video", stream, dest_folder=str(tmp_path), **kwargs)
        return result, stream

    return SimpleNamespace(run=run, stored=stored, network=network)


def test_saved_metadata_closes_placeholder_without_another_network_fetch(metadata_fetch):
    metadata_fetch.stored["fixture0000"] = {"video_id": "fixture0000"}
    result, stream = metadata_fetch.run()
    assert result == {"ok": True, "skipped": True}
    metadata_fetch.network.assert_not_called()
    assert "Metadata already saved" in _text(_metadata_lines(stream)[0])


@pytest.mark.parametrize("status,expected", [
    (FetchStatus.FAILED, "failed; retry needed"),
    (FetchStatus.CANCELLED, "cancelled"),
    (FetchStatus.RATE_LIMIT, "deferred by YouTube cooldown"),
    (FetchStatus.COOKIE, "needs YouTube sign-in"),
    (FetchStatus.TIMEOUT, "timed out; retry needed"),
])
def test_metadata_fetch_failure_has_a_terminal_marker(metadata_fetch, status, expected):
    metadata_fetch.network.return_value = MetadataFetchResult(status)
    result, stream = metadata_fetch.run()
    assert not result["ok"]
    rows = _metadata_lines(stream)
    assert len(rows) == 1 and expected in _text(rows[0])
    assert all("meta_done_fixture0000" in segment[1] for segment in rows[0])


def test_metadata_write_failure_never_emits_downloaded_success(monkeypatch, metadata_fetch):
    monkeypatch.setattr(fetcher, "_write_metadata_jsonl", mock.Mock(side_effect=OSError("disk full")))
    result, stream = metadata_fetch.run()
    assert not result["ok"]
    assert "failed; retry needed" in _text(_metadata_lines(stream)[0])
    assert all("Metadata downloaded" not in _text(call.args[0]) for call in stream.emit.call_args_list)


def test_metadata_cancellation_before_fetch_is_visible_and_spends_no_request(metadata_fetch):
    cancel = threading.Event()
    cancel.set()
    result, stream = metadata_fetch.run(cancel_event=cancel)
    assert result["cancelled"]
    metadata_fetch.network.assert_not_called()
    assert "cancelled" in _text(_metadata_lines(stream)[0])
