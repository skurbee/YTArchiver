"""Paired transcript commits serialize only the files they actually share."""

import json
import threading
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


def _start_thread(callback, errors):
    def run():
        try:
            callback()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def _finish_threads(threads, errors):
    for thread in threads:
        thread.join(3)
    assert all(not thread.is_alive() for thread in threads), "commit deadlocked"
    assert not errors, errors


@pytest.fixture
def caption_state(tmp_path, monkeypatch):
    # Root conftest isolates all application paths before these imports.
    from backend import index
    from backend import ytarchiver_config as config
    from backend.transcribe import core, transcribe_files, transcribe_vtt

    video = tmp_path / "Captioned.mp4"
    video.write_bytes(b"fixture video; no media process is started")
    video.with_suffix(".en.vtt").write_text(
        "WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nA new caption.\n\n",
        encoding="utf-8")
    txt = tmp_path / "Channel Transcript.txt"
    jsonl = tmp_path / ".Channel Transcript.jsonl"
    paths = str(txt), str(jsonl), 2026, 1, "20260102"
    monkeypatch.setattr(transcribe_vtt, "APP_DATA_DIR", tmp_path / "profile")
    monkeypatch.setattr(transcribe_vtt, "_resolve_transcript_paths", lambda *_a, **_k: paths)
    monkeypatch.setattr(core, "_resolve_transcript_paths", lambda *_a, **_k: paths)
    monkeypatch.setattr(index, "ingest_jsonl", lambda *_a, **_k: 1)
    monkeypatch.setattr(config, "remove_pending_tx_id", lambda _vid: True)

    def run_caption(cancel=None):
        return transcribe_vtt._try_auto_captions(
            str(video), "Captioned", "Channel", Mock(),
            video_id_hint="caption0001", from_download=True,
            allow_fetch=False, update_pending=False, cancel_event=cancel)

    return SimpleNamespace(
        core=core, files=transcribe_files, vtt=transcribe_vtt, index=index,
        video=video, txt=txt, jsonl=jsonl, run_caption=run_caption)


def test_reversed_pairs_and_individual_writer_locks_serialize(tmp_path):
    from backend.transcribe.transcribe_files import (
        transcript_output_locks,
        txt_lock_for,
    )

    txt = str(tmp_path / "Transcript.txt")
    jsonl = str(tmp_path / ".Transcript.jsonl")
    first_entered = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()
    second_entered = threading.Event()
    errors = []

    def first():
        with transcript_output_locks(txt, jsonl):
            with txt_lock_for(txt), txt_lock_for(jsonl):
                first_entered.set()
                assert release_first.wait(2)

    def second():
        second_started.set()
        with transcript_output_locks(jsonl, txt):
            second_entered.set()

    threads = [_start_thread(first, errors)]
    try:
        assert first_entered.wait(2)
        threads.append(_start_thread(second, errors))
        assert second_started.wait(2)
        assert not second_entered.wait(0.05)
    finally:
        release_first.set()
        _finish_threads(threads, errors)
    assert second_entered.is_set()


def test_exception_releases_identical_and_canonicalized_paths(tmp_path):
    from backend.transcribe.transcribe_files import transcript_output_locks

    path = str(tmp_path / "Transcript.txt")
    equivalent = str(tmp_path / "child" / ".." / "Transcript.txt")
    with pytest.raises(RuntimeError, match="commit failed"):
        with transcript_output_locks(path, equivalent):
            raise RuntimeError("commit failed")

    acquired = threading.Event()
    errors = []

    def acquire_after_exception():
        with transcript_output_locks(path, path):
            acquired.set()

    thread = _start_thread(acquire_after_exception, errors)
    _finish_threads([thread], errors)
    assert acquired.is_set()


def test_unrelated_transcript_pair_does_not_wait(tmp_path):
    from backend.transcribe.transcribe_files import transcript_output_locks

    errors = []
    acquired = threading.Event()

    def unrelated():
        with transcript_output_locks(str(tmp_path / "other.txt"), str(tmp_path / "other.jsonl")):
            acquired.set()

    with transcript_output_locks(str(tmp_path / "first.txt"), str(tmp_path / "first.jsonl")):
        thread = _start_thread(unrelated, errors)
        try:
            assert acquired.wait(2), "unrelated channel commit was blocked"
        finally:
            _finish_threads([thread], errors)


def test_failed_whisper_replacement_cannot_erase_concurrent_caption(
        caption_state, monkeypatch):
    state = caption_state
    original_id = "original001"
    original_segment = {"s": 0.0, "e": 1.0, "t": "Original words.", "w": []}
    assert state.files._write_transcript_entry(
        str(state.txt), "Original", "20260102", 1.0, "YT CAPTIONS",
        "Original words.", video_id=original_id)
    assert state.files._write_jsonl_entry(
        str(state.jsonl), original_id, "Original", [original_segment])

    manager = state.core.TranscribeManager.__new__(state.core.TranscribeManager)
    manager._cancel_all = threading.Event()
    manager._loaded_model = "small"
    manager._stream = Mock()
    manager._arm_output_write_intent = lambda _job: True

    replacement_written = threading.Event()
    allow_rollback = threading.Event()
    caption_attempted = threading.Event()
    caption_finished = threading.Event()
    errors = []
    outcomes = {}
    pair_locks = state.vtt.transcript_output_locks

    @contextmanager
    def observed_caption_locks(*paths):
        caption_attempted.set()
        with pair_locks(*paths):
            yield

    def fail_txt(*_args, **_kwargs):
        replacement_written.set()
        assert allow_rollback.wait(2)
        raise OSError("injected TXT replacement failure")

    monkeypatch.setattr(state.vtt, "transcript_output_locks", observed_caption_locks)
    monkeypatch.setattr(state.core, "_replace_txt_entry", fail_txt)

    def replace():
        outcomes["whisper"] = manager._write_outputs(
            str(state.video), {
                "text": "Replacement words.", "model": "small",
                "segments": [{"s": 0.0, "e": 1.0, "t": "Replacement words.", "w": []}],
            }, title="Original", channel="Channel", retranscribe=True,
            video_id_hint=original_id)

    def append_caption():
        outcomes["caption"] = state.run_caption()
        caption_finished.set()

    threads = [_start_thread(replace, errors)]
    try:
        assert replacement_written.wait(2)
        threads.append(_start_thread(append_caption, errors))
        assert caption_attempted.wait(2)
        assert not caption_finished.wait(0.05), "caption crossed a pending rollback"
    finally:
        allow_rollback.set()
        _finish_threads(threads, errors)

    assert outcomes["whisper"] is state.core._WorkerOutcome.FAILED
    assert outcomes["caption"] is state.vtt._CaptionOutcome.SUCCESS
    records = [json.loads(line) for line in state.jsonl.read_text(encoding="utf-8").splitlines()]
    assert {record["video_id"] for record in records} == {original_id, "caption0001"}
    text = state.txt.read_text(encoding="utf-8")
    assert "Original words." in text and "A new caption." in text
    assert "Replacement words." not in text
    assert next(record for record in records if record["video_id"] == original_id)["text"] == "Original words."


def test_caption_holds_pair_through_index_finalization(caption_state, monkeypatch):
    state = caption_state
    indexing = threading.Event()
    release_index = threading.Event()
    next_started = threading.Event()
    next_entered = threading.Event()
    errors = []

    def blocked_ingest(*_args, **_kwargs):
        indexing.set()
        assert release_index.wait(2)
        return 1

    def next_commit():
        next_started.set()
        with state.files.transcript_output_locks(str(state.jsonl), str(state.txt)):
            next_entered.set()

    monkeypatch.setattr(state.index, "ingest_jsonl", blocked_ingest)
    threads = [_start_thread(state.run_caption, errors)]
    try:
        assert indexing.wait(2)
        threads.append(_start_thread(next_commit, errors))
        assert next_started.wait(2)
        assert not next_entered.wait(0.05)
    finally:
        release_index.set()
        _finish_threads(threads, errors)
    assert next_entered.is_set()


def test_caption_cancelled_while_waiting_does_not_commit(caption_state, monkeypatch):
    state = caption_state
    cancel = threading.Event()
    attempted = threading.Event()
    errors = []
    outcomes = []
    pair_locks = state.vtt.transcript_output_locks

    @contextmanager
    def observed_locks(*paths):
        attempted.set()
        with pair_locks(*paths):
            yield

    monkeypatch.setattr(state.vtt, "transcript_output_locks", observed_locks)
    with state.files.transcript_output_locks(str(state.txt), str(state.jsonl)):
        thread = _start_thread(lambda: outcomes.append(state.run_caption(cancel)), errors)
        try:
            assert attempted.wait(2)
            cancel.set()
        finally:
            cancel.set()
    _finish_threads([thread], errors)
    assert outcomes == [state.vtt._CaptionOutcome.CANCELLED]
    assert not state.txt.exists() and not state.jsonl.exists()
