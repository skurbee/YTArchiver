"""Offline manual-download transcription handoff and destination safety."""
from __future__ import annotations

import importlib
import json
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-manual-transcribe-")
os.environ["APPDATA"] = _PROFILE.name
os.environ["LOCALAPPDATA"] = _PROFILE.name
Path(_PROFILE.name, "YTArchiver").mkdir()

from backend import index, livestreams, utils  # noqa: E402
from backend.api_mixins import archive_mixin, transcribe_mixin  # noqa: E402
from backend.process_runner import StreamingRunResult  # noqa: E402
from backend.queues import QueueState  # noqa: E402
from backend.services.channel_leases import (  # noqa: E402
    ChannelLeaseManager,
    LeaseOwner,
    channel_aliases,
    path_alias,
)
from backend.transcribe import core, helpers, punct_manager  # noqa: E402
from backend.transcribe.acceptance import EnqueueResult, EnqueueStatus  # noqa: E402

VID = "abcDEF12345"


class ManualApi(archive_mixin.ArchiveMixin, transcribe_mixin.TranscribeMixin):
    pass


@pytest.fixture
def download(tmp_path, monkeypatch):
    api = ManualApi()
    api._log_stream = mock.Mock()
    api._window = None
    api._push_url_history = mock.Mock()
    api._push_recent_refresh = mock.Mock()
    api._transcribe = mock.Mock()
    api._transcribe.enqueue_result.return_value = EnqueueResult(
        EnqueueStatus.ACCEPTED, task_id="gpu-transcript")
    folder = tmp_path / "custom destination"
    folder.mkdir()
    config = {"video_out_dir": str(folder), "output_dir": str(tmp_path / "archive")}
    controls = SimpleNamespace(
        returncode=0, output_complete=True, cancelled=False,
        cancel_at_commit=False, registered=True, produce_file=True)
    commands = []
    registrar = mock.Mock()

    def register(*args, **kwargs):
        if controls.cancel_at_commit:
            for event in api._archive_single_cancel_events.values():
                event.set()
        return controls.registered

    registrar.side_effect = register
    monkeypatch.setattr(index, "register_video", registrar)
    monkeypatch.setattr(livestreams, "drop", mock.Mock())
    monkeypatch.setattr(utils, "hide_stray_sidecars", mock.Mock())
    monkeypatch.setattr(archive_mixin, "load_config", lambda: config)
    monkeypatch.setattr(archive_mixin.sync_backend, "find_yt_dlp", lambda: "yt-dlp")
    monkeypatch.setattr(archive_mixin.sync_backend, "build_format_string", lambda _r: "best")
    monkeypatch.setattr(archive_mixin.sync_backend, "_find_cookie_source", list)
    monkeypatch.setattr(archive_mixin.sync_backend, "_record_recent_download", mock.Mock(return_value=True))
    monkeypatch.setattr(archive_mixin.youtube_traffic, "acquire", lambda *a, **k: {"ok": True})
    monkeypatch.setattr(archive_mixin, "popen_ytdlp", lambda cmd, **k: commands.append(cmd) or mock.Mock())
    leases = ChannelLeaseManager()
    monkeypatch.setattr(importlib.import_module("backend.services.channel_leases"), "channel_leases", leases)

    def supervise(_proc, **kwargs):
        if controls.produce_file:
            media = folder / f"Manual [{VID}].mp4"
            media.write_bytes(b"video")
            if "--write-info-json" in commands[-1]:
                media.with_suffix(".info.json").write_text(json.dumps({"id": VID}))
            kwargs["on_stdout_line"](f"[download] Destination: {media}")
            kwargs["on_stdout_line"](
                f"DLTRACK:::Manual:::Subscribed Creator:::20200102:::5:::60:::{VID}")
        return StreamingRunResult(
            controls.returncode, [], cancelled=controls.cancelled,
            output_complete=controls.output_complete)

    monkeypatch.setattr(archive_mixin, "supervise_streaming_process", supervise)
    monkeypatch.setattr(archive_mixin, "start_managed_task", lambda _api, **k: k["target"]())
    return SimpleNamespace(api=api, folder=folder, config=config, controls=controls,
                           commands=commands, registrar=registrar, leases=leases)


@pytest.mark.parametrize("requested", [None, False, True])
@pytest.mark.parametrize("metadata", [False, True])
def test_manual_transcription_is_opt_in_and_metadata_independent(download, requested, metadata):
    options = {"grab_metadata": metadata}
    if requested is not None:
        options["transcribe"] = requested
    result = download.api.archive_single_video(f"https://youtu.be/{VID}", options)
    assert result["ok"]
    assert download.api.archive_single_status()["tasks"] == []
    assert ("--write-thumbnail" in download.commands[0]) is metadata
    assert ("--write-info-json" in download.commands[0]) is metadata
    assert download.registrar.call_args.kwargs["tx_status"] == (
        "pending" if requested else "no_captions")
    manager = download.api._transcribe
    if requested:
        manager.enqueue_result.assert_called_once_with(
            str(download.folder / "Manual.mp4"), "Manual",
            channel="Subscribed Creator", video_id=VID, from_download=True)
    else:
        manager.enqueue_result.assert_not_called()


@pytest.mark.parametrize("failure", [
    "returncode", "output_complete", "cancelled", "cancel_at_commit",
    "registered", "produce_file",
])
def test_manual_transcription_requires_successful_uncancelled_commit(download, failure):
    setattr(download.controls, failure,
            1 if failure == "returncode" else failure in {"cancelled", "cancel_at_commit"})
    result = download.api.archive_single_video(f"https://youtu.be/{VID}", {"transcribe": True})
    assert result["ok"]  # The bridge response acknowledges worker startup.
    download.api._transcribe.enqueue_result.assert_not_called()
    assert download.api.archive_single_status()["tasks"] == []


def test_collision_handoff_uses_actual_retained_filename(download):
    original = download.folder / "Manual.mp4"
    original.write_bytes(b"existing other video")
    download.api.archive_single_video(f"https://youtu.be/{VID}", {"transcribe": True})
    expected = download.folder / f"Manual [{VID}].mp4"
    assert expected.read_bytes() == b"video"
    assert original.read_bytes() == b"existing other video"
    assert download.api._transcribe.enqueue_result.call_args.args[0] == str(expected)
    assert download.api._transcribe.enqueue_result.call_args.kwargs["video_id"] == VID


@pytest.mark.parametrize("rejection", ["save_failed", "shutdown", "exception", "duplicate"])
def test_rejected_handoff_keeps_media_and_explains_retry(download, rejection):
    manager = download.api._transcribe
    if rejection == "exception":
        manager.enqueue_result.side_effect = RuntimeError("Processing unavailable")
    else:
        manager.enqueue_result.return_value = EnqueueResult(
            EnqueueStatus(rejection), error="Processing admission refused")
    download.api.archive_single_video(f"https://youtu.be/{VID}", {"transcribe": True})
    assert (download.folder / "Manual.mp4").read_bytes() == b"video"
    assert download.registrar.call_args.kwargs["tx_status"] == "pending"
    assert download.api.archive_single_status()["tasks"] == []
    errors = " ".join(str(call) for call in download.api._log_stream.emit_error.call_args_list)
    if rejection == "duplicate":
        assert not errors  # An existing queue task already owns this exact file.
    else:
        assert "Download saved" in errors
        assert "Transcribe now in Browse" in errors


@pytest.mark.parametrize("subscribed", [False, True])
def test_loose_download_keeps_transcript_beside_media(download, monkeypatch, subscribed):
    media = download.folder / "Manual.mp4"
    media.write_bytes(b"video")
    channel = {"name": "Subscribed Creator", "folder": "Subscribed Creator"}
    monkeypatch.setattr(helpers, "_lookup_channel", lambda _name: channel if subscribed else None)
    monkeypatch.setattr(helpers, "ytarchiver_config_output_dir", lambda: download.config["output_dir"])
    txt, jsonl, *_ = helpers._resolve_transcript_paths(str(media), "Manual", "Subscribed Creator")
    assert Path(txt).parent == download.folder
    assert Path(jsonl).parent == download.folder
    assert Path(txt).name == "Manual Transcript.txt"


def test_manual_handoff_persists_identity_and_preserves_processing_pause(download, tmp_path, monkeypatch):
    monkeypatch.setattr(core, "find_python311", lambda: None)
    monkeypatch.setattr(punct_manager, "get_shared_punct_manager", lambda _stream: mock.Mock())
    monkeypatch.setattr(core, "_bump_transcription_pending", mock.Mock())
    journal = tmp_path / "pending-transcripts.json"
    monkeypatch.setattr(core, "_pending_journal_path", lambda: journal)
    manager = core.TranscribeManager(download.api._log_stream, model="small")
    monkeypatch.setattr(manager, "_ensure_worker", mock.Mock())
    queues = QueueState()
    queues.gpu_paused = True
    queues.gpu_pause_restored = False
    manager._paused.set()
    config = {**download.config, "autorun_gpu": False, "channels": [
        {"name": "Subscribed Creator", "folder": "Subscribed Creator"}]}
    manager.attach_queues(queues, cfg_loader=lambda: config)
    download.api._transcribe = manager
    try:
        download.api.archive_single_video(f"https://youtu.be/{VID}", {"transcribe": True})
        assert manager._paused.is_set()
        assert queues.gpu_paused
        assert not manager._manual_drain.is_set()
        assert not manager._auto_enabled()
        job = manager._jobs[0]
        assert job["path"] == str(download.folder / "Manual.mp4")
        assert job["video_id"] == VID
        assert job["channel"] == "Subscribed Creator"
        assert job["requested_model"] == "small"
        assert queues.gpu_snapshot()[0]["task_id"] == job["task_id"]
        saved = json.loads(journal.read_text(encoding="utf-8"))
        assert saved[0]["video_id"] == VID
        assert saved[0]["path"] == job["path"]
        aliases = manager._channel_aliases_for_job(job)
        assert path_alias(download.folder) in aliases
        held = download.leases.try_acquire(
            channel_aliases(paths=[download.folder]),
            LeaseOwner("manual-download", "other", kind="download"))
        assert held.ok
        try:
            processing = download.leases.try_acquire(
                aliases, LeaseOwner("processing", job["task_id"], kind="transcribe"))
            assert not processing.ok
        finally:
            held.lease.release()
    finally:
        queues.mark_orphan()


def test_channel_download_still_uses_channel_transcript(download, monkeypatch):
    channel = {"name": "Subscribed Creator", "folder": "Subscribed Creator"}
    channel_root = Path(download.config["output_dir"]) / channel["folder"]
    channel_root.mkdir(parents=True)
    media = channel_root / "Manual.mp4"
    media.write_bytes(b"video")
    monkeypatch.setattr(helpers, "_lookup_channel", lambda _name: channel)
    monkeypatch.setattr(helpers, "ytarchiver_config_output_dir", lambda: download.config["output_dir"])
    txt, jsonl, *_ = helpers._resolve_transcript_paths(str(media), "Manual", channel["name"])
    assert Path(txt).parent == channel_root
    assert Path(jsonl).parent == channel_root
    assert Path(txt).name == "Subscribed Creator Transcript.txt"
