"""Offline regressions for download controls and processing boundaries."""
from __future__ import annotations

import ast
import importlib
import io
import json
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-download-controls-")
os.environ["APPDATA"] = _PROFILE.name
os.environ["LOCALAPPDATA"] = _PROFILE.name
Path(_PROFILE.name, "YTArchiver").mkdir()

from backend import compress  # noqa: E402
from backend.api_mixins import archive_mixin, sync_mixin  # noqa: E402
from backend.process_runner import (  # noqa: E402
    ProcessRegistry,
    StreamingRunResult,
    subprocess_creationflags,
    supervise_streaming_process,
)
from backend.queues import QueueState  # noqa: E402
from backend.sync.sync_all import _SyncTaskCancel  # noqa: E402
from backend.transcribe import core, transcribe_vtt  # noqa: E402
from backend.transcribe.punct_manager import PunctuationManager  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


def test_streaming_deadline_tracks_inactivity_not_total_runtime():
    proc = subprocess.Popen(
        [sys.executable, "-u", "-c",
         "import time; [(print(i,flush=True),time.sleep(.02)) for i in range(25)]"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        creationflags=subprocess_creationflags())
    result = supervise_streaming_process(
        proc, idle_timeout=0.3, registry=ProcessRegistry())
    assert result.returncode == 0
    assert not result.timed_out


def test_silent_download_still_times_out():
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        creationflags=subprocess_creationflags())
    result = supervise_streaming_process(
        proc, idle_timeout=0.1, registry=ProcessRegistry())
    assert result.timed_out
    assert proc.poll() is not None


@pytest.mark.parametrize("url", [
    "youtu.be/abcDEF12345", "www.youtube.com/watch?v=abcDEF12345",
    "https://www.youtube.com/watch?v=abcDEF12345&feature=share",
])
@pytest.mark.parametrize("date_file", [True, False])
def test_manual_download_normalization_date_and_idle_timeout(tmp_path, monkeypatch, url, date_file):
    api = archive_mixin.ArchiveMixin()
    api._log_stream = mock.Mock()
    api._window = None
    api._push_url_history = mock.Mock()
    api._push_recent_refresh = mock.Mock()
    commands = []
    seen_status = []
    monkeypatch.setattr(archive_mixin, "load_config", lambda: {"video_out_dir": str(tmp_path)})
    monkeypatch.setattr(archive_mixin, "_probe_output_folder_writable", lambda _p: None)
    monkeypatch.setattr(archive_mixin.sync_backend, "find_yt_dlp", lambda: "yt-dlp")
    monkeypatch.setattr(archive_mixin.sync_backend, "build_format_string", lambda _r: "best")
    monkeypatch.setattr(archive_mixin.sync_backend, "_find_cookie_source", list)
    monkeypatch.setattr(archive_mixin.sync_backend, "_record_recent_download", lambda *a, **k: True)
    monkeypatch.setattr(archive_mixin.youtube_traffic, "acquire", lambda *a, **k: {"ok": True})
    monkeypatch.setattr(archive_mixin, "commit_download", lambda *a, **k: SimpleNamespace(ok=True))
    monkeypatch.setattr(archive_mixin, "popen_ytdlp", lambda cmd, **k: commands.append(cmd) or mock.Mock())

    def supervise(_proc, **kwargs):
        assert kwargs.get("timeout") is None
        assert kwargs["idle_timeout"] == 900
        seen_status.extend(api.archive_single_status()["tasks"])
        media = tmp_path / "Manual [abcDEF12345].mp4"
        media.write_bytes(b"video")
        media.with_suffix(".info.json").write_text('{"id":"abcDEF12345"}')
        os.utime(media, (2_000_000_000, 2_000_000_000))
        for line in [f"[download] Destination: {media}",
                     "DLTRACK:::Manual:::Channel:::20200102:::5:::60:::abcDEF12345"]:
            kwargs["on_stdout_line"](line)
        return StreamingRunResult(0, [])

    monkeypatch.setattr(archive_mixin, "supervise_streaming_process", supervise)
    monkeypatch.setattr(archive_mixin, "start_managed_task", lambda _api, **k: k["target"]())
    result = api.archive_single_video(url, {"date_file": date_file})
    assert result["ok"]
    assert commands[0][-1].startswith("https://")
    assert "feature=" not in commands[0][-1]
    assert seen_status[0]["task_id"] == result["task_id"]
    assert api.archive_single_status()["tasks"] == []
    final_path = tmp_path / "Manual.mp4"
    if date_file:
        assert time.strftime("%Y%m%d", time.localtime(final_path.stat().st_mtime)) == "20200102"
    else:
        assert final_path.stat().st_mtime == 2_000_000_000


def test_manual_cancel_reports_cancelling_until_worker_removes_task():
    api = archive_mixin.ArchiveMixin()
    api._ensure_archive_single_tracking()
    api._archive_single_cancel_events["one"] = threading.Event()
    api._archive_single_jobs["one"] = {"task_id": "one", "title": "Video", "url": "https://example.invalid"}
    assert api.archive_single_cancel("missing")["cancelled"] == 0
    assert api.archive_single_cancel("one")["cancelled"] == 1
    assert api.archive_single_status()["tasks"][0]["state"] == "cancelling"


def test_unexpected_manual_setup_failure_releases_status_and_url_guard(monkeypatch):
    api = archive_mixin.ArchiveMixin()
    monkeypatch.setattr(archive_mixin.sync_backend, "find_yt_dlp", lambda: "yt-dlp")
    monkeypatch.setattr(archive_mixin, "load_config", mock.Mock(side_effect=RuntimeError("configuration unavailable")))
    result = api.archive_single_video("https://youtu.be/abcDEF12345", {})
    assert not result["ok"]
    assert "configuration unavailable" in result["error"]
    assert api.archive_single_status()["tasks"] == []
    assert not api.archive_single_is_running()


def test_sync_skip_keeps_durable_ownership_and_signals_redownload():
    api = sync_mixin.SyncMixin()
    api._queues = QueueState()
    api._sync_skip = threading.Event()
    api._redwnl_cancel = threading.Event()
    api._log_stream = mock.Mock()
    task = {"name": "Example", "url": "https://example.invalid", "kind": "redownload", "task_id": "job-one"}
    assert api._queues.replace_current_task_durable("sync", task)
    assert api.sync_skip_current("job-one")["ok"]
    assert api._queues.current_sync["task_id"] == "job-one"
    assert api._queues.current_sync["cancel_requested"]
    assert api._redwnl_cancel.is_set()
    assert api._sync_skip.is_set()
    assert api._queues.replace_current_task_durable("sync", None, expected_task_id="job-one")
    api._queues.mark_orphan()


def test_task_cancel_remains_latched_when_next_job_clears_skip():
    parent, skip = threading.Event(), threading.Event()
    first = _SyncTaskCancel(parent, skip)
    skip.set()
    assert first.is_set()
    skip.clear()
    assert first.is_set()
    assert not _SyncTaskCancel(parent, skip).is_set()


def test_completed_task_token_does_not_observe_the_next_tasks_skip():
    parent, skip = threading.Event(), threading.Event()
    completed = _SyncTaskCancel(parent, skip)
    completed.finish()
    skip.set()
    assert not completed.is_set()
    parent.set()
    assert completed.is_set(), "whole-pass cancellation still reaches detached work"


def test_current_task_decoration_preserves_cancel_intent():
    state = QueueState()
    task = {"task_id": "decorate", "name": "Example", "kind": "redownload", "cancel_requested": True}
    assert state.replace_current_task_durable("sync", task)
    decorated = {"task_id": "decorate", "name": "Redownload Example", "kind": "redownload"}
    assert state.replace_current_task_durable("sync", decorated, expected_task_id="decorate")
    assert state.current_sync["cancel_requested"]
    decorated["_pass_start_ts"] = 123.0
    state.set_current_sync(decorated)
    assert state.current_sync["cancel_requested"]
    restored = QueueState()
    restored.load()
    assert not restored.get_loaded_resuming().get("sync")
    restored.mark_orphan()
    state.mark_orphan()


def test_auto_off_sync_subbed_adds_every_missing_channel_and_offers_start():
    api = sync_mixin.SyncMixin()
    api._queues = QueueState()
    api._on_queue_changed = mock.Mock()
    api._config = {"autorun_sync": False, "channels": [
        {"name": name, "url": f"https://www.youtube.com/@{name}"}
        for name in ("First", "Second")]}
    assert api._queues.sync_enqueue(api._config["channels"][0])
    result = api._sync_start_all_inner()
    assert result == {"ok": True, "started": False, "queued": 1, "total_queued": 2, "can_start": True}
    assert [task["name"] for task in api._queues.sync_snapshot()] == ["First", "Second"]
    assert api._sync_start_all_inner()["queued"] == 0
    api._queues.mark_orphan()


def test_sync_failed_cancel_save_does_not_signal_or_remove_current(monkeypatch):
    api = sync_mixin.SyncMixin()
    api._queues = QueueState()
    api._sync_skip = threading.Event()
    api._log_stream = mock.Mock()
    original = {"name": "Example", "url": "https://example.invalid", "kind": "download", "task_id": "save-failure"}
    assert api._queues.replace_current_task_durable("sync", original)
    monkeypatch.setattr(api._queues, "_write_resuming_payload", lambda _p: False)
    assert not api.sync_skip_current("save-failure")["ok"]
    assert not api._sync_skip.is_set()
    assert not api._queues.current_sync.get("cancel_requested")
    api._queues.mark_orphan()


def test_defer_rolls_back_pending_copy_if_cancellation_journal_is_rejected(monkeypatch):
    api = sync_mixin.SyncMixin()
    api._queues = QueueState()
    api._sync_skip = threading.Event()
    api._log_stream = mock.Mock()
    task = {"task_id": "defer-failed", "name": "Example", "url": "https://example.invalid", "kind": "download"}
    assert api._queues.replace_current_task_durable("sync", task)
    write = api._queues._write_resuming_payload

    def reject_cancel(payload):
        if ((payload.get("resuming") or {}).get("sync") or {}).get("cancel_requested"):
            return False
        return write(payload)

    monkeypatch.setattr(api._queues, "_write_resuming_payload", reject_cancel)
    assert not api.sync_defer_current(task["task_id"])["ok"]
    assert not api._sync_skip.is_set()
    assert api._queues.current_sync["task_id"] == task["task_id"]
    assert not api._queues.current_sync.get("cancel_requested")
    assert api._queues.sync_snapshot() == []
    restored = QueueState()
    restored.load()
    assert restored.get_loaded_resuming()["sync"]["task_id"] == task["task_id"]
    assert restored.sync_snapshot() == []
    api._queues.mark_orphan()
    restored.mark_orphan()


def test_cancelled_running_task_does_not_restart_after_loading_queue():
    state = QueueState()
    task = {"name": "Example", "url": "https://example.invalid", "kind": "download", "task_id": "cancelled-restart", "cancel_requested": True}
    assert state.replace_current_task_durable("sync", task)
    restored = QueueState()
    restored.load()
    assert not restored.get_loaded_resuming().get("sync")
    assert not restored.sync_snapshot()
    state.mark_orphan()
    restored.mark_orphan()


@pytest.mark.parametrize("kind", ["download", "metadata"])
@pytest.mark.parametrize("defer", [False, True])
def test_sync_cancel_and_defer_advance_only_after_worker_acknowledges(tmp_path, monkeypatch, kind, defer):
    module = importlib.import_module("backend.sync.sync_all")
    cfg = {"channels": [{"name": name, "url": f"https://www.youtube.com/@{name}", "kind": kind}
                        for name in ("First", "Second")]}
    state = QueueState()
    for ch in cfg["channels"]:
        assert state.sync_enqueue(ch)
    stream = mock.Mock()
    api = sync_mixin.SyncMixin()
    api._queues = state
    api._sync_skip = threading.Event()
    api._log_stream = stream
    visited = []

    def worker(ch, _stream, token, **kwargs):
        visited.append(ch["name"])
        if len(visited) == 1:
            task_id = state.current_sync["task_id"]
            result = (api.sync_defer_current(task_id) if defer else api.sync_skip_current(task_id))
            assert result["ok"]
            assert state.current_sync["task_id"] == task_id
            assert state.current_sync["cancel_requested"]
            assert token.is_set()
        else:
            assert not token.is_set()
        return {"downloaded": 0, "errors": 0, "cancelled": token.is_set()}

    @contextmanager
    def transaction():
        yield cfg

    overrides = {
        "load_config": lambda: cfg,
        "ARCHIVE_FILE": str(tmp_path / "absent-archive.txt"),
        "_resolve_sync_task_target": lambda ch: (cfg, ch, frozenset({"isolated"})),
        "_channel_folder_path": lambda *a: "",
        "_check_batch_cooldown": lambda ch: (True, ""),
        "channel_leases": SimpleNamespace(try_acquire=lambda *a: SimpleNamespace(ok=True, lease=SimpleNamespace(release=lambda: None))),
        "channel_identity": SimpleNamespace(preflight_channel_identity=lambda *a, **k: {"ok": True},
                                            has_stable_identity=lambda ch: True,
                                            operational_channel_url=lambda ch: ch["url"]),
        "sync_channel": worker,
        "fire_channel_synced_hook": lambda: None,
        "fire_metadata_changed_hook": lambda: None,
        "_should_batch_limit": lambda *a: False,
        "clear_sync_progress": lambda: None,
        "config_transaction": transaction,
    }
    for name, value in overrides.items():
        monkeypatch.setattr(module, name, value)
    from backend import metadata
    monkeypatch.setattr(metadata, "fetch_channel_metadata", worker)
    result = module._sync_all_impl(stream, threading.Event(), queues=state,
        skip_event=api._sync_skip, add_downloads_from_config=False)
    assert result["ok"], result
    assert visited == (["First", "Second", "First"] if defer else ["First", "Second"])
    assert not state.current_sync
    assert not state.sync_snapshot()
    state.mark_orphan()


def test_punctuation_import_works_in_standalone_bundle_without_loading_models(tmp_path):
    worker = ROOT / "backend" / "punct_worker.py"
    preamble = worker.read_text(encoding="utf-8").split("_out = sys.stdout", 1)[0]
    (tmp_path / "punct_worker.py").write_text(preamble + "\nprint('helper imported')\n", encoding="utf-8")
    shutil.copyfile(ROOT / "backend" / "punct_alignment.py", tmp_path / "punct_alignment.py")
    result = subprocess.run(
        [sys.executable, "-E", "-s", str(tmp_path / "punct_worker.py")],
        capture_output=True, text=True, creationflags=subprocess_creationflags(), check=False)
    assert result.returncode == 0, result.stderr
    assert "helper imported" in result.stdout


def test_failed_punctuation_cannot_be_reported_as_unchanged_success(monkeypatch):
    manager = PunctuationManager(mock.Mock())
    monkeypatch.setattr(manager, "_start", lambda: False)
    with pytest.raises(RuntimeError, match="failed to start"):
        manager.punctuate_checked("three words here")


def test_failed_punctuation_restoration_preserves_outputs_and_retry_checkpoint(tmp_path, monkeypatch):
    from backend import punct_restore

    class FailedManager:
        def is_available(self):
            return True
        def punctuate_checked(self, *args, **kwargs):
            raise RuntimeError("Worker helper could not import")

    monkeypatch.setattr(punct_restore, "_load_checkpoint", lambda _url: [("transcript.jsonl", "abcDEF12345", "Video", "YT CAPTIONS")])
    monkeypatch.setattr(punct_restore, "_load_progress", lambda _url: set())
    monkeypatch.setattr(punct_restore, "_segments_for_video", lambda *a: [{"text": "three words here", "start": 0, "end": 3}])
    monkeypatch.setattr(punct_restore, "TRANSCRIPTION_DB", str(tmp_path / "isolated.sqlite"))
    write = mock.Mock()
    progress = mock.Mock()
    clear = mock.Mock()
    monkeypatch.setattr(punct_restore, "_replace_jsonl_entry", write)
    monkeypatch.setattr(punct_restore, "_append_progress", progress)
    monkeypatch.setattr(punct_restore, "_clear_checkpoint", clear)
    result = punct_restore.restore_punctuation_archive(
        output_dir=str(tmp_path), log_stream=mock.Mock(), scope_url="test-scope",
        shared_punct_mgr=FailedManager())
    assert result["failed"] == 1
    assert result["succeeded"] == 0
    write.assert_not_called()
    progress.assert_not_called()
    clear.assert_not_called()


def _caption_fixture(tmp_path, monkeypatch):
    media = tmp_path / "Video.mp4"
    media.write_bytes(b"video")
    media.with_suffix(".en.vtt").write_text("WEBVTT\n\n00:00:00.000 --> 00:00:03.000\nthree words here\n")
    paths = mock.Mock(return_value=(str(tmp_path / "Combined.txt"), str(tmp_path / "Combined.jsonl"), 2020, 1, "20200102"))
    monkeypatch.setattr(transcribe_vtt, "_resolve_transcript_paths", paths)
    monkeypatch.setattr(transcribe_vtt, "_extract_video_id", lambda *a, **k: "abcDEF12345")
    monkeypatch.setattr(transcribe_vtt, "_write_transcript_entry", mock.Mock(return_value=True))
    monkeypatch.setattr(transcribe_vtt, "_write_jsonl_entry", mock.Mock(return_value=True))
    monkeypatch.setattr(transcribe_vtt, "begin_reconciliation", lambda *a, **k: mock.Mock())
    from backend import index
    monkeypatch.setattr(index, "ingest_jsonl", lambda *a, **k: 1)
    return media, paths


def test_caption_layout_choice_reaches_path_resolver(tmp_path, monkeypatch):
    media, paths = _caption_fixture(tmp_path, monkeypatch)
    result = transcribe_vtt._try_auto_captions(
        str(media), "Video", "Example", mock.Mock(), combined_override=True,
        update_pending=False)
    assert result is transcribe_vtt._CaptionOutcome.SUCCESS
    assert paths.call_args.kwargs["combined_override"] is True


def test_cancel_during_caption_punctuation_writes_nothing(tmp_path, monkeypatch):
    media, _paths = _caption_fixture(tmp_path, monkeypatch)
    cancel = threading.Event()
    def punctuate(text):
        cancel.set()
        return text.capitalize() + "."
    result = transcribe_vtt._try_auto_captions(
        str(media), "Video", "Example", mock.Mock(),
        punct_mgr=SimpleNamespace(punctuate=punctuate), cancel_event=cancel,
        update_pending=False)
    assert result is transcribe_vtt._CaptionOutcome.CANCELLED
    transcribe_vtt._write_transcript_entry.assert_not_called()
    transcribe_vtt._write_jsonl_entry.assert_not_called()


def test_queued_caption_dispatch_does_not_prepare_whisper_first():
    source = ast.parse((ROOT / "backend/transcribe/core.py").read_text(encoding="utf-8"))
    node = next(n for n in ast.walk(source) if isinstance(n, ast.FunctionDef) and n.name == "_execute_job")
    owner = SimpleNamespace(_transcribe_one=mock.Mock(return_value=core._WorkerOutcome.SUCCESS),
                            _prepare_job_model=mock.Mock(side_effect=AssertionError("unneeded Whisper")))
    namespace = {"self": owner, "job": {}, "_job_kind": "transcribe", "_WorkerOutcome": core._WorkerOutcome}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "queued-dispatch", "exec"), namespace)
    assert namespace["_execute_job"]() is core._WorkerOutcome.SUCCESS
    owner._prepare_job_model.assert_not_called()


def test_pause_does_not_claim_running_whisper_child_is_idle():
    commands = io.StringIO()
    pipe = queue.Queue()
    pipe.put(json.dumps({"status": "ok", "model": "small", "text": "Done"}))
    proc = SimpleNamespace(poll=lambda: None, stdin=commands)
    paused = threading.Event()
    paused.set()
    manager = SimpleNamespace(_snapshot_worker_io=lambda: (proc, pipe),
        _cancel_all=threading.Event(), _paused=paused, _queues=mock.Mock(),
        _accept_worker_model_report=lambda *a, **k: True)
    with mock.patch.object(core, "_ffprobe_duration", return_value=10):
        result, _payload = core.TranscribeManager._transcribe_single_file(
            manager, "unused.wav", {"cancel": threading.Event()})
    assert result is core._WorkerOutcome.SUCCESS
    manager._queues.set_gpu_paused_active.assert_not_called()
    assert paused.is_set()


@pytest.mark.parametrize("retranscribe", [False, True])
@pytest.mark.parametrize("cancel_stage", ["response", "paths", "journal"])
def test_late_whisper_success_cannot_commit_after_cancel(tmp_path, monkeypatch, retranscribe, cancel_stage):
    cancel = threading.Event()
    if cancel_stage == "response":
        cancel.set()

    def resolve(*args, **kwargs):
        if cancel_stage == "paths":
            cancel.set()
        return str(tmp_path / "Transcript.txt"), str(tmp_path / "Transcript.jsonl"), 2020, 1, "20200102"

    def arm(_job):
        if cancel_stage == "journal":
            cancel.set()
        return True

    manager = SimpleNamespace(_cancel_all=threading.Event(), _loaded_model="small",
                              _stream=mock.Mock(), _arm_output_write_intent=arm)
    monkeypatch.setattr(core, "_resolve_transcript_paths", resolve)
    monkeypatch.setattr(core, "_extract_video_id", lambda *a, **k: "abcDEF12345")
    writes = []
    for name in ("_write_transcript_entry", "_write_jsonl_entry", "_replace_txt_entry", "_replace_jsonl_entry"):
        write = mock.Mock()
        monkeypatch.setattr(core, name, write)
        writes.append(write)
    result = core.TranscribeManager._write_outputs(
        manager, str(tmp_path / "Video.mp4"),
        {"text": "finished speech", "segments": [{"s": 0, "e": 3, "t": "finished speech"}]},
        title="Video", channel="Example", retranscribe=retranscribe, job={"cancel": cancel})
    assert result is core._WorkerOutcome.CANCELLED
    for write in writes:
        write.assert_not_called()


def test_cancel_during_compression_validation_preserves_original(tmp_path, monkeypatch):
    original = tmp_path / "Video.mp4"
    original.write_bytes(b"original" * 250)
    cancel = threading.Event()

    class Encoder:
        def __init__(self, command, **kwargs):
            Path(command[-1]).write_bytes(b"new" * 200)
            self.stderr = []
            self.returncode = 0
        def poll(self):
            return 0
        def wait(self, **kwargs):
            return 0

    def duration(path, _ffmpeg):
        if "_TEMP_COMPRESS_" in path:
            cancel.set()
        return 100

    monkeypatch.setattr(compress, "find_ffmpeg", lambda: "unused-ffmpeg")
    monkeypatch.setattr(compress, "get_bitrate", lambda *a: 475)
    monkeypatch.setattr(compress, "get_video_duration", duration)
    monkeypatch.setattr(compress, "get_video_codec", lambda *a: "av1")
    monkeypatch.setattr(compress.subprocess, "Popen", Encoder)
    result = compress.compress_video(str(original), mock.Mock(), cancel_event=cancel)
    assert result["reason"] == "cancelled"
    assert original.read_bytes() == b"original" * 250
