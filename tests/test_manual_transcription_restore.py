"""Restore manual transcription by video identity, using disposable stores."""

import json
import sqlite3
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    from backend import index, queues
    from backend.transcribe import core
    from backend.transcribe.transcribe_files import _write_transcript_entry

    monkeypatch.setattr(queues, "QUEUE_FILE", tmp_path / "queue.json")
    monkeypatch.setattr(queues, "config_is_writable", lambda: True)
    monkeypatch.setattr(core, "_pending_journal_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(core, "_bump_transcription_pending", lambda *_args: None)
    monkeypatch.setattr(core.TranscribeManager, "_ensure_worker", lambda _self: None)
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE videos (filepath TEXT, video_id TEXT, title TEXT, "
                 "channel TEXT, tx_status TEXT)")
    monkeypatch.setattr(index, "_reader_open", lambda: conn)
    monkeypatch.setattr(index, "_open", lambda: conn)
    monkeypatch.setattr(index, "_reader_lock", threading.RLock())
    txt = tmp_path / "Same title Transcript.txt"
    jsonl = tmp_path / ".Same title Transcript.jsonl"
    monkeypatch.setattr(core, "_resolve_transcript_paths", lambda *_a, **_k:
                        (str(txt), str(jsonl), 2026, 9, "20260912"))
    state = queues.QueueState()
    manager = core.TranscribeManager(Mock())
    manager.attach_queues(state)
    video = tmp_path / "Same title [fixture0002].mp4"
    video.write_bytes(b"fixture media; never decoded")

    def enqueue(video_id="fixture0002"):
        accepted = manager.enqueue_result(
            str(video), "Same title", channel="Uploader", video_id=video_id,
            from_download=True)
        assert accepted.accepted
        return accepted.task_id

    def transcript(video_id="fixture0001"):
        assert _write_transcript_entry(
            str(txt), "Same title", "20260912", 60, "WHISPER", "Saved words", video_id)
        jsonl.write_text(json.dumps({
            "video_id": video_id, "title": "Same title", "text": "Saved words",
            "start": 0, "end": 1,
        }) + "\n", encoding="utf-8")

    def restore():
        restored = core.TranscribeManager(Mock())
        restored.attach_queues(state)
        count = restored.load_pending()
        saved = json.loads((tmp_path / "pending.json").read_text(encoding="utf-8"))
        return count, restored, saved

    yield SimpleNamespace(enqueue=enqueue, transcript=transcript, restore=restore,
                          state=state, conn=conn, txt=txt, jsonl=jsonl, video=video)
    state.mark_orphan()
    conn.close()


def test_same_title_other_id_does_not_discard_pending_manual_download(recovery):
    task_id = recovery.enqueue()
    recovery.transcript("fixture0001")
    original_txt = recovery.txt.read_bytes()
    original_jsonl = recovery.jsonl.read_bytes()

    count, restored, saved = recovery.restore()

    assert count == 1
    assert [job["task_id"] for job in restored._jobs] == [task_id]
    assert [job["task_id"] for job in saved] == [task_id]
    assert [job["task_id"] for job in recovery.state.gpu_snapshot()] == [task_id]
    assert recovery.txt.read_bytes() == original_txt
    assert recovery.jsonl.read_bytes() == original_jsonl


@pytest.mark.parametrize("insert_pending", [False, True])
def test_other_id_no_speech_status_does_not_discard_pending_job(recovery, insert_pending):
    task_id = recovery.enqueue()
    recovery.conn.execute("INSERT INTO videos VALUES (?, ?, ?, ?, ?)", (
        "other.mp4", "fixture0001", "Same title", "Uploader", "no_speech"))
    if insert_pending:
        recovery.conn.execute("INSERT INTO videos VALUES (?, ?, ?, ?, ?)", (
            str(recovery.video), "fixture0002", "Same title", "Uploader", "pending"))

    count, restored, saved = recovery.restore()

    assert count == 1
    assert [job["task_id"] for job in restored._jobs] == [task_id]
    assert [job["task_id"] for job in saved] == [task_id]


@pytest.mark.parametrize("evidence", ["transcript", "no_speech"])
def test_own_id_completed_work_is_still_removed_from_recovery(recovery, evidence):
    recovery.enqueue()
    if evidence == "transcript":
        recovery.transcript("fixture0002")
    else:
        recovery.conn.execute("INSERT INTO videos VALUES (?, ?, ?, ?, ?)", (
            str(recovery.video), "fixture0002", "Same title", "Uploader", "no_speech"))

    count, restored, saved = recovery.restore()

    assert count == 0
    assert restored._jobs == []
    assert saved == []
    assert recovery.state.gpu_snapshot() == []


def test_malformed_sidecar_keeps_job_and_existing_bytes_for_retry(recovery):
    task_id = recovery.enqueue()
    recovery.transcript("fixture0002")
    recovery.jsonl.write_text("{broken\n", encoding="utf-8")
    before = recovery.txt.read_bytes(), recovery.jsonl.read_bytes()

    count, restored, saved = recovery.restore()

    assert count == 1
    assert [job["task_id"] for job in restored._jobs] == [task_id]
    assert [job["task_id"] for job in saved] == [task_id]
    assert (recovery.txt.read_bytes(), recovery.jsonl.read_bytes()) == before


def test_legacy_job_without_id_keeps_existing_title_based_restore_behavior(recovery):
    recovery.enqueue(video_id="")
    recovery.transcript(video_id="")

    count, restored, saved = recovery.restore()

    assert count == 0
    assert restored._jobs == []
    assert saved == []
