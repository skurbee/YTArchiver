"""Real queue/lease handoff with disposable media and a stub recognition step."""

import json
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    from backend.queues import QueueState
    from backend.services.channel_leases import LeaseOwner, channel_leases
    from backend.transcribe import core

    channels = [{"name": name, "url": f"https://youtube.com/@fixture{name}"}
                for name in ("Alpha", "Beta")]
    config = {"output_dir": str(tmp_path), "channels": channels, "autorun_gpu": True}
    stream = Mock()
    manager = core.TranscribeManager(stream, model="small")
    queues = QueueState()
    manager.attach_queues(queues, cfg_loader=lambda: config)
    monkeypatch.setattr(manager, "_ensure_worker", lambda: None)
    monkeypatch.setattr(manager, "_flush_batch_stats", lambda: None)
    monkeypatch.setattr(manager, "_auto_enabled", lambda: True)
    monkeypatch.setattr(manager, "_finish_terminal_pending", lambda _job: True)
    monkeypatch.setattr(core, "_bump_transcription_pending", lambda *_a: None)
    monkeypatch.setattr(core, "_try_auto_captions", lambda *_a, **_k: core._CaptionOutcome.UNAVAILABLE)
    journal = tmp_path / "pending.json"
    monkeypatch.setattr(core, "_pending_journal_path", lambda: journal)
    paths = {}
    for name in ("Alpha", "Beta"):
        folder = tmp_path / name
        folder.mkdir()
        paths[name] = folder / "Video.mp4"
        paths[name].write_bytes(b"fixture media, never decoded")
    holders = []

    def hold(name, owner="sync", kind="download", job_id="sync-alpha"):
        aliases = manager._channel_aliases_for_job({"path": str(paths[name]), "channel": name})
        result = channel_leases.try_acquire(aliases, LeaseOwner(
            owner, job_id, label=f"{kind} for {name}", kind=kind))
        assert result.ok
        holders.append(result.lease)
        return result.lease

    yield SimpleNamespace(core=core, manager=manager, queues=queues, stream=stream,
                          paths=paths, journal=journal, hold=hold, leases=channel_leases)
    for holder in holders:
        holder.release()
    assert channel_leases.active_snapshot() == ()


def test_completed_download_runs_before_its_originating_sync_finishes(pipeline):
    p = pipeline
    parent = p.hold("Alpha")
    assert p.manager.route_download_transcription(
        str(p.paths["Alpha"]), "Completed video", channel="Alpha", video_id="fixture0001") == "processing"
    job = p.manager._jobs[0]
    assert job["_download_sync_job_id"] == "sync-alpha"
    snapshot = p.manager._snapshot_pending_job(job)
    assert "_download_sync_job_id" not in snapshot
    assert "sync-alpha" not in p.journal.read_text(encoding="utf-8")
    observed = []

    def recognize(current):
        assert not parent.released
        assert {row.owner for row in p.leases.active_snapshot()} == {"sync", "processing"}
        assert p.queues.current_gpu["task_id"] == current["task_id"]
        observed.append(current["task_id"])
        return p.core._WorkerOutcome.SUCCESS

    p.manager._transcribe_one_unleased = recognize
    p.manager._worker_loop()
    assert observed == [job["task_id"]]
    assert not p.manager._jobs
    assert p.queues.current_gpu is None
    assert not p.queues.gpu_snapshot()
    assert not parent.released


def test_blocked_channel_stays_pending_while_ready_channel_runs(pipeline):
    p = pipeline
    blocker = p.hold("Alpha", owner="reorg", kind="reorg", job_id="organize-alpha")
    for name, vid in (("Alpha", "fixture0001"), ("Beta", "fixture0002")):
        assert p.manager.enqueue(str(p.paths[name]), name, channel=name, video_id=vid)
    alpha, beta = p.manager._jobs
    observed = []

    def recognize(job):
        observed.append(job["channel"])
        if job is beta:
            assert [row["task_id"] for row in p.queues.gpu_snapshot()] == [alpha["task_id"]]
            assert p.queues.current_gpu["task_id"] == beta["task_id"]
            assert alpha["task_id"] in p.journal.read_text(encoding="utf-8")
            blocker.release()
        return p.core._WorkerOutcome.SUCCESS

    p.manager._transcribe_one_unleased = recognize
    p.manager._worker_loop()
    assert observed == ["Beta", "Alpha"]
    assert not p.manager._jobs
    assert not p.queues.gpu_snapshot()
    assert p.queues.current_gpu is None
    waits = [call.args[0] for call in p.stream.emit.call_args_list
             if "waiting for" in str(call.args[0])]
    assert len(waits) == 1
    assert "tx_done_fixture0001" in waits[0][0][1]
    assert "reorg for Alpha" in waits[0][0][0]


def test_all_blocked_remain_pending_and_shutdown_releases_worker(pipeline):
    p = pipeline
    p.hold("Alpha", owner="reorg", kind="reorg")
    assert p.manager.enqueue(str(p.paths["Alpha"]), "Video", channel="Alpha", video_id="fixture0001")
    original_id = p.manager._jobs[0]["task_id"]
    waiting = threading.Event()
    original_emit = p.manager._emit_processing_wait
    def emit_wait(job, blockers):
        original_emit(job, blockers)
        waiting.set()
    p.manager._emit_processing_wait = emit_wait
    recognize = Mock(side_effect=AssertionError("Blocked job executed"))
    p.manager._transcribe_one_unleased = recognize
    thread = threading.Thread(target=p.manager._worker_loop, daemon=True)
    thread.start()
    try:
        assert waiting.wait(2)
        assert p.queues.current_gpu is None
        assert [row["task_id"] for row in p.queues.gpu_snapshot()] == [original_id]
        assert original_id in p.journal.read_text(encoding="utf-8")
    finally:
        p.manager._shutdown_requested.set()
        thread.join(2)
    assert not thread.is_alive()
    recognize.assert_not_called()
    assert p.manager._jobs[0]["task_id"] == original_id


def test_failed_promotion_releases_lease_and_preserves_queued_identity(pipeline, monkeypatch):
    p = pipeline
    assert p.manager.enqueue(str(p.paths["Alpha"]), "Video", channel="Alpha")
    job = p.manager._jobs[0]
    before = p.journal.read_bytes()
    monkeypatch.setattr(p.manager, "_persist_pending", lambda: False)
    p.manager._transcribe_one_unleased = Mock(side_effect=AssertionError("Undurable job ran"))
    p.manager._worker_loop()
    assert p.leases.active_snapshot() == ()
    assert p.manager._jobs == [job]
    assert p.queues.current_gpu is None
    assert p.queues.gpu_snapshot()[0]["task_id"] == job["task_id"]
    assert p.journal.read_bytes() == before
    assert p.manager._paused.is_set()


@pytest.mark.parametrize("retained", [False, True])
@pytest.mark.parametrize("chunked", [False, True])
def test_empty_replacement_reports_truthful_terminal_status(pipeline, monkeypatch, retained, chunked):
    p = pipeline
    media = p.paths["Alpha"]
    txt = media.parent / "Alpha Transcript.txt"
    jsonl = media.parent / ".Alpha Transcript.jsonl"
    if retained:
        jsonl.write_text(json.dumps({"video_id": "fixture0001", "title": "Video",
                                     "text": "Existing words", "start": 0, "end": 1}) + "\n",
                         encoding="utf-8")
    monkeypatch.setattr(p.core, "_resolve_transcript_paths", lambda *_a, **_k:
                        (str(txt), str(jsonl), 2026, 1, "20260101"))
    monkeypatch.setattr(p.core, "_ffprobe_duration", lambda _p: 10)
    if chunked:
        monkeypatch.setattr(p.core, "_CHUNK_MIN_DURATION", 1)
        monkeypatch.setattr(p.core, "extract_audio_chunk", lambda *_a, **_k:
                            SimpleNamespace(outcome=p.core._WorkerOutcome.SUCCESS))
    monkeypatch.setattr(p.manager, "is_available", lambda: True)
    monkeypatch.setattr(p.manager, "_prepare_job_model", lambda _j: True)
    monkeypatch.setattr(p.manager, "_snapshot_worker_io", lambda: (SimpleNamespace(poll=lambda: None), None))
    monkeypatch.setattr(p.manager, "_punctuate_result", lambda _r: None)
    monkeypatch.setattr(p.manager, "_transcribe_single_file", lambda *_a, **_k:
                        (p.core._WorkerOutcome.SUCCESS, {"text": "", "segments": []}))
    mark = Mock(return_value=True)
    monkeypatch.setattr(p.manager, "_mark_no_speech_durable", mark)
    finish = Mock(return_value=True)
    monkeypatch.setattr(p.manager, "_finish_successful_job", finish)
    job = {"task_id": "recognize-empty", "kind": "transcribe", "path": str(media),
           "title": "Video", "channel": "Alpha", "video_id": "fixture0001",
           "from_download": True, "_retry_as_replace": True, "cancel": threading.Event()}
    outcome = p.manager._transcribe_one(job)
    assert outcome is (p.core._WorkerOutcome.SUCCESS if retained else p.core._WorkerOutcome.NO_SPEECH)
    emitted = [call.args[0] for call in p.stream.emit.call_args_list]
    text = "".join(str(segment[0]) for line in emitted for segment in line)
    assert "✓ Transcription" not in text
    assert ("existing transcript kept" in text) is retained
    for line in emitted:
        assert all("tx_done_fixture0001" in segment[1] for segment in line)
    if retained:
        mark.assert_not_called()
        terminal = emitted[-1]
        assert "tx_done_fixture0001" in terminal[0][1]
        assert "yellow" in terminal[0][1]
        assert finish.call_args.args[1]["_existing_transcript_kept"] is True
        assert json.loads(jsonl.read_text())["text"] == "Existing words"
    else:
        mark.assert_called_once_with(str(media))
        assert finish.call_args.args[1] == {"no_speech": True}
        assert not txt.exists() and not jsonl.exists()
