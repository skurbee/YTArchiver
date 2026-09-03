from __future__ import annotations

import atexit
import builtins
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

# Standalone runs must never inherit the user's configured app-data/database
# paths.  This assignment happens before any backend import establishes its
# module-level paths.
_TEST_APPDATA = tempfile.TemporaryDirectory(
    prefix="ytarchiver-patch2-transcribe-")
atexit.register(_TEST_APPDATA.cleanup)
os.environ["APPDATA"] = _TEST_APPDATA.name

from backend.transcribe import (
    core as transcribe_core,
)
from backend.transcribe import transcribe_files, transcribe_vtt


class Patch2WorkerOutcomeTests(unittest.TestCase):
    @staticmethod
    def _job(video: Path, callback=None) -> dict:
        return {
            "kind": "transcribe",
            "path": str(video),
            "title": "Recover Me",
            "channel": "Channel",
            "video_id": "abc123def45",
            "combined_override": None,
            "retranscribe": False,
            "bulk_id": "",
            "bulk_total": 0,
            "bulk_index": 0,
            "from_download": False,
            "compress_after": {"quality": "High", "output_res": "1080"},
            "cb": callback,
            "cancel": threading.Event(),
        }

    def test_normal_return_failure_stays_queued_and_in_journal(self) -> None:
        stream = mock.Mock()
        callback = mock.Mock()
        manager = transcribe_core.TranscribeManager(stream)
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            video = root / "Recover Me.mp4"
            video.write_bytes(b"video")
            journal = root / "pending.json"
            job = self._job(video, callback)
            manager._jobs = [job]

            with (
                mock.patch.object(manager, "is_available", return_value=True),
                mock.patch.object(
                    manager, "_transcribe_one",
                    return_value=transcribe_core._WorkerOutcome.FAILED,
                ),
                mock.patch.object(
                    manager, "_prepare_job_model", return_value=True,
                ),
                mock.patch.object(manager, "_flush_batch_stats"),
                mock.patch.object(
                    transcribe_core, "_pending_journal_path",
                    return_value=journal,
                ),
                mock.patch.object(
                    transcribe_core, "_bump_transcription_pending",
                ) as bump_pending,
                mock.patch(
                    "backend.ytarchiver_config.remove_pending_tx_id",
                ) as remove_pending_id,
            ):
                manager._worker_loop()

            saved = json.loads(journal.read_text(encoding="utf-8"))

        self.assertEqual(manager._jobs, [job])
        self.assertIsNone(manager._current_job)
        self.assertTrue(manager._paused.is_set())
        self.assertTrue(job["_retry_required"])
        self.assertEqual(saved[0]["path"], str(video))
        self.assertTrue(saved[0]["retry_required"])
        self.assertEqual(manager._batch_stats["Channel"]["done"], 0)
        self.assertEqual(manager._batch_stats["Channel"]["err"], 1)
        bump_pending.assert_not_called()
        remove_pending_id.assert_not_called()
        callback.assert_not_called()

    def test_bare_none_return_fails_closed_instead_of_counting_done(self) -> None:
        stream = mock.Mock()
        manager = transcribe_core.TranscribeManager(stream)
        with tempfile.TemporaryDirectory() as tmp_dir:
            video = Path(tmp_dir) / "Legacy Failure.mp4"
            video.write_bytes(b"video")
            job = self._job(video)
            manager._jobs = [job]

            with (
                mock.patch.object(manager, "is_available", return_value=True),
                mock.patch.object(manager, "_transcribe_one", return_value=None),
                mock.patch.object(
                    manager, "_prepare_job_model", return_value=True,
                ),
                mock.patch.object(manager, "_persist_pending"),
                mock.patch.object(manager, "_flush_batch_stats"),
                mock.patch.object(
                    transcribe_core, "_bump_transcription_pending",
                ) as bump_pending,
            ):
                manager._worker_loop()

        self.assertEqual(manager._jobs, [job])
        self.assertEqual(manager._batch_stats["Channel"]["done"], 0)
        self.assertEqual(manager._batch_stats["Channel"]["err"], 1)
        self.assertTrue(manager._paused.is_set())
        bump_pending.assert_not_called()
        stream.emit_error.assert_any_call(
            "Transcribe ended without an explicit outcome; "
            "task left queued for retry."
        )

    def test_failed_index_ingest_is_not_reported_as_output_success(self) -> None:
        stream = mock.Mock()
        manager = transcribe_core.TranscribeManager(stream)
        result = {
            "text": "hello world",
            "segments": [
                {"s": 0.0, "e": 1.0, "t": "hello world", "w": []},
            ],
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            video = root / "Video.mp4"
            video.write_bytes(b"video")
            txt_path = root / "Channel Transcript.txt"
            jsonl_path = root / ".Channel Transcript.jsonl"
            job = self._job(video)

            with (
                mock.patch.object(
                    transcribe_core, "_resolve_transcript_paths",
                    return_value=(
                        str(txt_path), str(jsonl_path), 2026, 8, "08.31.2026",
                    ),
                ),
                mock.patch.object(
                    transcribe_core, "_write_transcript_entry",
                    return_value=True,
                ),
                mock.patch.object(
                    transcribe_core, "_write_jsonl_entry",
                    return_value=True,
                ),
                mock.patch.object(
                    transcribe_core,
                    "_hide_per_video_transcript_txt_if_needed",
                ),
                mock.patch.object(manager, "_persist_pending") as persist,
                mock.patch(
                    "backend.index._open_independent", return_value=None,
                ),
            ):
                outcome = manager._write_outputs(
                    str(video), result, title="Video", channel="Channel",
                    video_id_hint="abc123def45", job=job,
                )

        self.assertIs(outcome, transcribe_core._WorkerOutcome.FAILED)
        self.assertTrue(job["_retry_required"])
        self.assertTrue(job["_retry_as_replace"])
        persist.assert_called_once_with()

    def test_compress_false_result_does_not_call_completion_callback(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        callback = mock.Mock()
        job = {
            "path": "Video.mp4",
            "quality": "Average",
            "output_res": "720",
            "cancel": threading.Event(),
            "cb": callback,
        }
        with mock.patch(
            "backend.compress.compress_video",
            return_value={"ok": False, "error": "replace denied"},
        ):
            outcome = manager._compress_one(job)

        self.assertIs(outcome, transcribe_core._WorkerOutcome.FAILED)
        callback.assert_not_called()


class Patch2AggregateWriteTests(unittest.TestCase):
    @staticmethod
    def _original_bytes() -> bytes:
        return (json.dumps({
            "video_id": "old00000001",
            "title": "Existing",
            "start": 0.0,
            "end": 1.0,
            "text": "existing text",
            "words": [],
        }) + "\n").encode()

    @staticmethod
    def _segments() -> list[dict]:
        return [{"s": 1.0, "e": 2.0, "t": "new text", "w": []}]

    @staticmethod
    def _deny_open(target: Path, denied_mode: str):
        real_open = builtins.open
        target_key = os.path.normcase(os.path.abspath(target))

        def _open(file, mode="r", *args, **kwargs):
            file_key = os.path.normcase(os.path.abspath(os.fspath(file)))
            if file_key == target_key and mode == denied_mode:
                raise PermissionError(f"denied test open: {target}")
            return real_open(file, mode, *args, **kwargs)

        return _open

    def test_read_denial_preserves_existing_aggregate_byte_for_byte(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / ".Channel Transcript.jsonl"
            original = self._original_bytes()
            target.write_bytes(original)

            with (
                mock.patch("builtins.open", self._deny_open(target, "rb")),
                mock.patch.object(transcribe_files, "_unhide_file_win"),
                mock.patch.object(transcribe_files, "_hide_file_win"),
                mock.patch.object(transcribe_files.os, "replace") as replace,
            ):
                ok = transcribe_files._write_jsonl_entry_unlocked(
                    str(target), "new00000001", "New", self._segments(),
                )

            self.assertFalse(ok)
            self.assertEqual(target.read_bytes(), original)
            self.assertFalse(Path(str(target) + ".tmp").exists())
            replace.assert_not_called()

    def test_unreadable_target_never_promotes_a_stale_temp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / ".Channel Transcript.jsonl"
            stale = Path(str(target) + ".tmp")
            original = self._original_bytes()
            pending = original + (
                json.dumps({"video_id": "pending0001", "title": "Pending"})
                + "\n"
            ).encode()
            target.write_bytes(original)
            stale.write_bytes(pending)

            with (
                mock.patch("builtins.open", self._deny_open(target, "rb")),
                mock.patch.object(transcribe_files, "_unhide_file_win"),
                mock.patch.object(transcribe_files, "_hide_file_win"),
                mock.patch.object(transcribe_files.os, "replace") as replace,
            ):
                ok = transcribe_files._write_jsonl_entry_unlocked(
                    str(target), "new00000001", "New", self._segments(),
                )

            self.assertFalse(ok)
            self.assertEqual(target.read_bytes(), original)
            self.assertEqual(stale.read_bytes(), pending)
            replace.assert_not_called()

    def test_temp_write_denial_preserves_existing_aggregate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / ".Channel Transcript.jsonl"
            temp_path = Path(str(target) + ".tmp")
            original = self._original_bytes()
            target.write_bytes(original)

            with (
                mock.patch(
                    "builtins.open", self._deny_open(temp_path, "wb"),
                ),
                mock.patch.object(transcribe_files, "_unhide_file_win"),
                mock.patch.object(transcribe_files, "_hide_file_win"),
            ):
                ok = transcribe_files._write_jsonl_entry_unlocked(
                    str(target), "new00000001", "New", self._segments(),
                )

            self.assertFalse(ok)
            self.assertEqual(target.read_bytes(), original)
            self.assertFalse(temp_path.exists())


class Patch2RecoveryContractTests(unittest.TestCase):
    @staticmethod
    def _job(video: Path, callback=None) -> dict:
        return {
            "kind": "transcribe",
            "path": str(video),
            "title": "Recover Me",
            "channel": "Channel",
            "video_id": "abc123def45",
            "combined_override": None,
            "retranscribe": False,
            "bulk_id": "",
            "bulk_total": 0,
            "bulk_index": 0,
            "from_download": False,
            "compress_after": {"quality": "High", "output_res": "1080"},
            "cb": callback,
            "cancel": threading.Event(),
        }

    @staticmethod
    def _result() -> dict:
        return {
            "text": "hello world",
            "segments": [
                {"s": 0.0, "e": 1.0, "t": "hello world", "w": []},
            ],
        }

    def test_write_intent_failure_prevents_first_sidecar_mutation(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            video = root / "Recover Me.mp4"
            video.write_bytes(b"video")
            job = self._job(video)
            manager._current_job = job

            with (
                mock.patch.object(
                    transcribe_core,
                    "_resolve_transcript_paths",
                    return_value=(
                        str(root / "Transcript.txt"),
                        str(root / ".Transcript.jsonl"),
                        2026,
                        8,
                        "08.31.2026",
                    ),
                ),
                mock.patch.object(
                    manager, "_persist_pending", return_value=False,
                ),
                mock.patch.object(
                    transcribe_core, "_write_transcript_entry",
                ) as write_txt,
                mock.patch.object(
                    transcribe_core, "_write_jsonl_entry",
                ) as write_jsonl,
            ):
                outcome = manager._write_outputs(
                    str(video), self._result(), title="Recover Me",
                    channel="Channel", video_id_hint="abc123def45", job=job,
                )

        self.assertIs(outcome, transcribe_core._WorkerOutcome.FAILED)
        self.assertNotIn("_write_intent", job)
        write_txt.assert_not_called()
        write_jsonl.assert_not_called()

    def test_pending_journal_fsync_failure_preserves_old_journal(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            journal = Path(tmp_dir) / "pending.json"
            journal.write_text('[{"path": "old.mp4"}]', encoding="utf-8")
            with (
                mock.patch.object(
                    transcribe_core, "_pending_journal_path",
                    return_value=journal,
                ),
                mock.patch.object(
                    transcribe_core.os, "fsync",
                    side_effect=OSError("fsync denied"),
                ),
                mock.patch.object(transcribe_core.os, "replace") as replace,
            ):
                saved = manager._persist_pending()

            self.assertFalse(saved)
            self.assertEqual(
                journal.read_text(encoding="utf-8"),
                '[{"path": "old.mp4"}]',
            )
            self.assertEqual(
                list(journal.parent.glob(f".{journal.name}.*.tmp")), [],
            )
            replace.assert_not_called()

    def test_load_write_intent_forces_replace_without_counter_increment(
            self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            video = root / "Recover Me.mp4"
            video.write_bytes(b"video")
            journal = root / "pending.json"
            journal.write_text(json.dumps([{
                "path": str(video),
                "title": "Recover Me",
                "channel": "Channel",
                "video_id": "abc123def45",
                "write_intent": True,
            }]), encoding="utf-8")

            with (
                mock.patch.object(
                    transcribe_core, "_pending_journal_path",
                    return_value=journal,
                ),
                mock.patch.object(manager, "_ensure_worker"),
                mock.patch.object(
                    transcribe_core, "_bump_transcription_pending",
                ) as bump_pending,
            ):
                recovered = manager.load_pending()

        self.assertEqual(recovered, 1)
        self.assertEqual(len(manager._jobs), 1)
        self.assertTrue(manager._jobs[0]["_write_intent"])
        self.assertTrue(manager._jobs[0]["_retry_required"])
        self.assertTrue(manager._jobs[0]["_retry_as_replace"])
        bump_pending.assert_not_called()

    def test_cleanup_failure_is_verified_and_retained(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        job = {
            "kind": "transcribe",
            "path": "Video.mp4",
            "title": "Video",
            "channel": "Channel",
            "video_id": "abc123def45",
        }
        with (
            mock.patch(
                "backend.ytarchiver_config.remove_pending_tx_id",
                return_value=False,
            ),
            mock.patch.object(
                manager, "_pending_id_present", return_value=True,
            ) as verify,
            mock.patch.object(
                transcribe_core, "_bump_transcription_pending",
            ) as bump_pending,
        ):
            finished = manager._finish_terminal_pending(job)

        self.assertFalse(finished)
        self.assertFalse(job.get("_pending_decremented"))
        verify.assert_called_once_with("abc123def45")
        bump_pending.assert_not_called()

    def test_no_speech_recovery_runs_callback_and_compress_without_whisper(
            self) -> None:
        stream = mock.Mock()
        callback = mock.Mock()
        manager = transcribe_core.TranscribeManager(stream)
        with tempfile.TemporaryDirectory() as tmp_dir:
            video = Path(tmp_dir) / "Silent.mp4"
            video.write_bytes(b"video")
            job = self._job(video, callback)
            job["_no_speech_pending"] = True
            manager._jobs = [job]

            with (
                mock.patch.object(manager, "is_available", return_value=False),
                mock.patch.object(
                    manager, "_mark_no_speech_durable", return_value=True,
                ),
                mock.patch.object(
                    manager, "_finish_terminal_pending", return_value=True,
                ),
                mock.patch.object(
                    manager, "compress_enqueue", return_value=True,
                ) as compress_enqueue,
                mock.patch.object(manager, "_transcribe_one") as transcribe,
                mock.patch.object(manager, "_persist_pending", return_value=True),
                mock.patch.object(manager, "_flush_batch_stats"),
            ):
                manager._worker_loop()

        callback.assert_called_once_with({"no_speech": True})
        compress_enqueue.assert_called_once()
        transcribe.assert_not_called()
        self.assertNotIn("_no_speech_pending", job)
        self.assertEqual(manager._batch_stats["Channel"]["done"], 1)

    def test_failed_compress_followup_retries_without_replaying_completion(
            self) -> None:
        stream = mock.Mock()
        callback = mock.Mock()
        manager = transcribe_core.TranscribeManager(stream)
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            video = root / "Video.mp4"
            journal = root / "pending.json"
            video.write_bytes(b"video")
            job = self._job(video, callback)
            manager._jobs = [job]
            result = {"text": "durable transcript"}

            def finish_transcription(_job):
                self.assertIs(_job, job)
                return (
                    transcribe_core._WorkerOutcome.SUCCESS
                    if manager._finish_successful_job(job, result)
                    else transcribe_core._WorkerOutcome.CLEANUP_FAILED
                )

            with (
                mock.patch.object(
                    transcribe_core, "_pending_journal_path",
                    return_value=journal,
                ),
                mock.patch.object(
                    manager, "_transcribe_one",
                    side_effect=finish_transcription,
                ) as transcribe,
                mock.patch.object(
                    manager, "_prepare_job_model", return_value=True,
                ),
                mock.patch.object(
                    manager, "compress_enqueue",
                    side_effect=[False, True],
                ) as compress_enqueue,
                mock.patch.object(
                    manager, "_finish_terminal_pending", return_value=True,
                ) as finish_terminal,
                mock.patch.object(manager, "_flush_batch_stats"),
            ):
                manager._worker_loop()
                saved_after_failure = json.loads(
                    journal.read_text(encoding="utf-8"),
                )

                self.assertEqual(manager._jobs, [job])
                self.assertTrue(manager._paused.is_set())
                self.assertTrue(saved_after_failure[0]["output_complete"])
                self.assertTrue(saved_after_failure[0]["callback_done"])
                self.assertTrue(saved_after_failure[0]["followup_pending"])
                self.assertFalse(saved_after_failure[0]["followup_enqueued"])
                self.assertTrue(saved_after_failure[0]["cleanup_only"])
                transcribe.assert_called_once_with(job)
                callback.assert_called_once_with(result)
                self.assertEqual(compress_enqueue.call_count, 1)
                finish_terminal.assert_not_called()

                manager._paused.clear()
                manager._worker_loop()
                final_journal = json.loads(
                    journal.read_text(encoding="utf-8"),
                )

        self.assertEqual(final_journal, [])
        transcribe.assert_called_once_with(job)
        callback.assert_called_once_with(result)
        self.assertEqual(compress_enqueue.call_count, 2)
        finish_terminal.assert_called_once_with(job)
        self.assertEqual(manager._jobs, [])
        self.assertEqual(manager._batch_stats["Channel"]["done"], 1)

    def test_partial_captions_never_fall_through_to_whisper(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            video = Path(tmp_dir) / "Recover Me.mp4"
            video.write_bytes(b"video")
            job = self._job(video)
            manager._current_job = job

            with (
                mock.patch.object(
                    transcribe_core, "_try_auto_captions",
                    return_value=transcribe_vtt._CaptionOutcome.PARTIAL,
                ),
                mock.patch.object(manager, "_persist_pending", return_value=True),
                mock.patch.object(manager, "start_subprocess") as start,
            ):
                outcome = manager._transcribe_one(job)

        self.assertIs(outcome, transcribe_core._WorkerOutcome.FAILED)
        self.assertTrue(job["_retry_required"])
        self.assertTrue(job["_retry_as_replace"])
        start.assert_not_called()

    def test_recovery_replace_job_cannot_reenter_caption_append_path(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            video = Path(tmp_dir) / "Recover Me.mp4"
            video.write_bytes(b"video")
            job = self._job(video)
            job["_retry_as_replace"] = True

            with (
                mock.patch.object(
                    transcribe_core, "_try_auto_captions",
                ) as captions,
                mock.patch.object(manager, "is_available", return_value=False),
            ):
                outcome = manager._transcribe_one(job)

        self.assertIs(outcome, transcribe_core._WorkerOutcome.FAILED)
        captions.assert_not_called()

    def test_cpu_retry_marker_resets_for_a_new_gpu_attempt(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            video = Path(tmp_dir) / "Recover Me.mp4"
            video.write_bytes(b"video")
            job = self._job(video)
            job["compress_after"] = {}
            job["_retried_cpu"] = True

            with mock.patch.object(
                    transcribe_core, "_try_auto_captions", return_value=True):
                outcome = manager._transcribe_one(job)

        self.assertIs(outcome, transcribe_core._WorkerOutcome.SUCCESS)
        self.assertNotIn("_retried_cpu", job)

    def test_new_jsonl_is_deleted_when_paired_txt_replace_fails(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            video = root / "Recover Me.mp4"
            video.write_bytes(b"video")
            txt_path = root / "Transcript.txt"
            jsonl_path = root / ".Transcript.jsonl"
            job = self._job(video)

            def _create_jsonl(*_args, **_kwargs):
                jsonl_path.write_text('{}\n', encoding="utf-8")
                return set()

            with (
                mock.patch.object(
                    transcribe_core,
                    "_resolve_transcript_paths",
                    return_value=(
                        str(txt_path), str(jsonl_path), 2026, 8, "08.31.2026",
                    ),
                ),
                mock.patch.object(
                    manager, "_arm_output_write_intent", return_value=True,
                ),
                mock.patch.object(
                    transcribe_core, "_replace_jsonl_entry",
                    side_effect=_create_jsonl,
                ),
                mock.patch.object(
                    transcribe_core, "_replace_txt_entry",
                    side_effect=PermissionError("TXT locked"),
                ),
                mock.patch(
                    "backend.utils.unhide_file_win",
                ),
            ):
                outcome = manager._write_outputs(
                    str(video), self._result(), title="Recover Me",
                    channel="Channel", retranscribe=True,
                    video_id_hint="abc123def45", job=job,
                )

            self.assertIs(outcome, transcribe_core._WorkerOutcome.FAILED)
            self.assertFalse(jsonl_path.exists())


class Patch2InlineCaptionRecoveryTests(unittest.TestCase):
    class SimulatedCrashError(BaseException):
        pass

    @staticmethod
    def _write_vtt(path: Path, text: str) -> None:
        path.write_text(
            "WEBVTT\n\n00:00:00.000 --> 00:00:01.000\n"
            f"{text}\n",
            encoding="utf-8",
        )

    def _assert_interruption_recovery(self, *, after_jsonl: bool) -> None:
        stream = mock.Mock()
        manager = transcribe_core.TranscribeManager(stream)
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            video = root / "Video.mp4"
            vtt = root / "Video.en.vtt"
            txt_path = root / "Channel Transcript.txt"
            jsonl_path = root / ".Channel Transcript.jsonl"
            journal = root / "pending.json"
            video.write_bytes(b"video")
            self._write_vtt(vtt, "Original caption.")
            resolved = (
                str(txt_path), str(jsonl_path), 2026, 8, "08.31.2026",
            )
            real_write_jsonl = transcribe_vtt._write_jsonl_entry

            def interrupt_jsonl(*args, **kwargs):
                if after_jsonl:
                    self.assertTrue(real_write_jsonl(*args, **kwargs))
                raise self.SimulatedCrashError("simulated process loss")

            with (
                mock.patch.object(
                    transcribe_core, "_pending_journal_path",
                    return_value=journal,
                ),
                mock.patch.object(
                    transcribe_vtt, "_resolve_transcript_paths",
                    return_value=resolved,
                ),
                mock.patch.object(
                    transcribe_vtt, "_write_jsonl_entry",
                    side_effect=interrupt_jsonl,
                ),
                self.assertRaises(self.SimulatedCrashError),
            ):
                manager.route_download_transcription(
                    str(video), "Video", channel="Channel",
                    video_id="abc123def45",
                )

            saved = json.loads(journal.read_text(encoding="utf-8"))
            self.assertEqual(len(saved), 1)
            self.assertTrue(saved[0]["caption_recovery"])
            self.assertTrue(saved[0]["write_intent"])
            self.assertTrue(saved[0]["retry_required"])
            self.assertTrue(saved[0]["skip_pending_counter"])
            self.assertTrue(txt_path.exists())
            self.assertEqual(jsonl_path.exists(), after_jsonl)

            # A fresh process must restore the exact native-caption recovery
            # path, then idempotently replace any subset already written.
            self._write_vtt(vtt, "Recovered caption.")
            recovered_manager = transcribe_core.TranscribeManager(mock.Mock())
            with mock.patch.object(
                transcribe_core, "_pending_journal_path",
                return_value=journal,
            ):
                recovered = recovered_manager.load_pending()

            self.assertEqual(recovered, 1)
            recovered_job = recovered_manager._jobs[0]
            self.assertTrue(recovered_job["_caption_recovery"])
            self.assertTrue(recovered_job["_write_intent"])
            self.assertFalse(recovered_job["_retry_as_replace"])

            with (
                mock.patch.object(
                    transcribe_core, "_pending_journal_path",
                    return_value=journal,
                ),
                mock.patch.object(
                    transcribe_vtt, "_resolve_transcript_paths",
                    return_value=resolved,
                ),
                mock.patch(
                    "backend.index.ingest_jsonl", return_value=True,
                ),
                mock.patch(
                    "backend.ytarchiver_config.remove_pending_tx_id",
                    return_value=True,
                ),
                mock.patch.object(
                    transcribe_core, "_bump_transcription_pending",
                ) as bump_pending,
                mock.patch.object(
                    recovered_manager, "_flush_batch_stats",
                ),
                mock.patch.object(
                    recovered_manager,
                    "_prepare_job_model",
                    return_value=True,
                ),
            ):
                recovered_manager._worker_loop()

            final_journal = json.loads(journal.read_text(encoding="utf-8"))
            txt = txt_path.read_text(encoding="utf-8")
            rows = [
                json.loads(line)
                for line in jsonl_path.read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual(final_journal, [])
        self.assertEqual(txt.count("===(Video)"), 1)
        self.assertNotIn("Original caption.", txt)
        self.assertIn("Recovered caption.", txt)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["text"], "Recovered caption.")
        bump_pending.assert_not_called()

    def test_txt_only_interruption_restores_idempotently(self) -> None:
        self._assert_interruption_recovery(after_jsonl=False)

    def test_txt_and_jsonl_interruption_restores_idempotently(self) -> None:
        self._assert_interruption_recovery(after_jsonl=True)

    def test_marker_persist_failure_prevents_first_caption_write(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            video = Path(tmp_dir) / "Video.mp4"
            video.write_bytes(b"video")
            with (
                mock.patch.object(
                    manager, "_persist_pending", return_value=False,
                ),
                mock.patch.object(
                    transcribe_core, "_try_auto_captions",
                ) as captions,
            ):
                outcome = manager.route_download_transcription(
                    str(video), "Video", channel="Channel",
                    video_id="abc123def45",
                )

        self.assertEqual(outcome, "failed")
        self.assertEqual(manager._inline_caption_jobs, [])
        self.assertEqual(manager._jobs, [])
        captions.assert_not_called()

    def test_success_is_not_green_when_marker_clear_cannot_commit(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            video = Path(tmp_dir) / "Video.mp4"
            video.write_bytes(b"video")
            with (
                mock.patch.object(
                    manager, "_persist_pending",
                    side_effect=[True, False],
                ),
                mock.patch.object(
                    transcribe_core, "_try_auto_captions",
                    return_value=transcribe_core._CaptionOutcome.SUCCESS,
                ),
                mock.patch.object(
                    manager, "_promote_inline_caption_recovery",
                    return_value=True,
                ) as promote,
                mock.patch.object(
                    manager, "record_inline_transcription",
                ) as record_inline,
                mock.patch.object(
                    manager, "_finish_successful_job",
                ) as finish,
            ):
                outcome = manager.route_download_transcription(
                    str(video), "Video", channel="Channel",
                    video_id="abc123def45",
                )

        self.assertEqual(outcome, "processing")
        self.assertEqual(len(manager._inline_caption_jobs), 1)
        promote.assert_called_once_with(manager._inline_caption_jobs[0])
        record_inline.assert_not_called()
        finish.assert_called_once_with(
            manager._inline_caption_jobs[0], {"auto_captions": True},
        )


class Patch2JournalTransactionTests(unittest.TestCase):
    @staticmethod
    def _job(video: Path, title: str = "Video") -> dict:
        return {
            "kind": "transcribe",
            "path": str(video),
            "title": title,
            "channel": "Channel",
            "video_id": "abc123def45",
            "cb": None,
            "cancel": threading.Event(),
        }

    def test_stale_snapshot_cannot_overwrite_newer_concurrent_commit(
            self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            journal = root / "pending.json"
            first_video = root / "First.mp4"
            second_video = root / "Second.mp4"
            first_video.write_bytes(b"video")
            second_video.write_bytes(b"video")
            manager._jobs = [self._job(first_video, "First")]

            first_at_replace = threading.Event()
            allow_first_replace = threading.Event()
            second_at_replace = threading.Event()
            replace_sources: list[str] = []
            replace_lock = threading.Lock()
            real_replace = os.replace

            def controlled_replace(source, target):
                with replace_lock:
                    call_index = len(replace_sources)
                    replace_sources.append(str(source))
                if call_index == 0:
                    first_at_replace.set()
                    if not allow_first_replace.wait(5):
                        raise RuntimeError("timed out waiting for test release")
                else:
                    second_at_replace.set()
                real_replace(source, target)

            results: list[bool] = []
            with (
                mock.patch.object(
                    transcribe_core, "_pending_journal_path",
                    return_value=journal,
                ),
                mock.patch.object(
                    transcribe_core.os, "replace",
                    side_effect=controlled_replace,
                ),
            ):
                first = threading.Thread(
                    target=lambda: results.append(manager._persist_pending()),
                )
                first.start()
                self.assertTrue(first_at_replace.wait(2))

                # Simulate a newer state becoming visible while the old
                # snapshot is paused immediately before replace.
                manager._jobs = [self._job(second_video, "Second")]
                second = threading.Thread(
                    target=lambda: results.append(manager._persist_pending()),
                )
                second.start()
                self.assertFalse(second_at_replace.wait(0.1))

                allow_first_replace.set()
                first.join(5)
                second.join(5)

            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
            self.assertEqual(results, [True, True])
            saved = json.loads(journal.read_text(encoding="utf-8"))
            self.assertEqual([item["title"] for item in saved], ["Second"])
            self.assertEqual(len(set(replace_sources)), 2)
            self.assertEqual(
                list(root.glob(f".{journal.name}.*.tmp")), [],
            )

    def test_clear_waits_for_inflight_snapshot_then_wins(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            journal = root / "pending.json"
            video = root / "Video.mp4"
            video.write_bytes(b"video")
            manager._jobs = [self._job(video)]

            first_at_replace = threading.Event()
            allow_first_replace = threading.Event()
            clear_finished = threading.Event()
            real_replace = os.replace
            call_count = 0
            count_lock = threading.Lock()

            def controlled_replace(source, target):
                nonlocal call_count
                with count_lock:
                    call_index = call_count
                    call_count += 1
                if call_index == 0:
                    first_at_replace.set()
                    if not allow_first_replace.wait(5):
                        raise RuntimeError("timed out waiting for test release")
                real_replace(source, target)

            results: list[bool] = []

            def clear_journal() -> None:
                results.append(manager.clear_pending_journal())
                clear_finished.set()

            with (
                mock.patch.object(
                    transcribe_core, "_pending_journal_path",
                    return_value=journal,
                ),
                mock.patch.object(
                    transcribe_core.os, "replace",
                    side_effect=controlled_replace,
                ),
            ):
                persist = threading.Thread(
                    target=lambda: results.append(manager._persist_pending()),
                )
                persist.start()
                self.assertTrue(first_at_replace.wait(2))
                clear = threading.Thread(target=clear_journal)
                clear.start()
                self.assertFalse(clear_finished.wait(0.1))
                allow_first_replace.set()
                persist.join(5)
                clear.join(5)

            self.assertFalse(persist.is_alive())
            self.assertFalse(clear.is_alive())
            self.assertEqual(results, [True, True])
            self.assertEqual(
                json.loads(journal.read_text(encoding="utf-8")), [],
            )

    def test_enqueue_failure_has_no_visible_or_running_side_effects(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        manager._queues = mock.Mock()
        with tempfile.TemporaryDirectory() as tmp_dir:
            video = Path(tmp_dir) / "Video.mp4"
            video.write_bytes(b"video")
            with (
                mock.patch.object(
                    manager, "_persist_pending", return_value=False,
                ),
                mock.patch.object(manager, "_ensure_worker") as start,
                mock.patch.object(
                    transcribe_core, "_bump_transcription_pending",
                ) as bump,
            ):
                queued = manager.enqueue(
                    str(video), "Video", channel="Channel",
                )

        self.assertFalse(queued)
        self.assertEqual(manager._jobs, [])
        manager._queues.gpu_enqueue.assert_not_called()
        bump.assert_not_called()
        start.assert_not_called()

    def test_worker_does_not_pop_or_run_without_durable_transition(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            video = Path(tmp_dir) / "Video.mp4"
            video.write_bytes(b"video")
            job = self._job(video)
            manager._jobs = [job]
            with (
                mock.patch.object(
                    manager, "_persist_pending", return_value=False,
                ),
                mock.patch.object(manager, "_transcribe_one") as transcribe,
                mock.patch.object(manager, "_compress_one") as compress_job,
                mock.patch.object(manager, "_flush_batch_stats"),
            ):
                manager._worker_loop()

        self.assertEqual(manager._jobs, [job])
        self.assertIsNone(manager._current_job)
        self.assertTrue(manager._paused.is_set())
        transcribe.assert_not_called()
        compress_job.assert_not_called()

    def test_final_snapshot_failure_retains_cleanup_only_job(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            video = Path(tmp_dir) / "Video.mp4"
            video.write_bytes(b"video")
            job = self._job(video)
            manager._jobs = [job]

            def successful_transcription(current_job):
                # `_finish_successful_job` checkpoints this before the real
                # worker returns SUCCESS. Keep the failure injection focused
                # on the later final journal clear.
                current_job["_output_complete"] = True
                return transcribe_core._WorkerOutcome.SUCCESS

            with (
                mock.patch.object(
                    manager, "_persist_pending",
                    side_effect=[True, False],
                ) as persist,
                mock.patch.object(
                    manager, "_transcribe_one",
                    side_effect=successful_transcription,
                ),
                mock.patch.object(
                    manager, "_prepare_job_model", return_value=True,
                ),
                mock.patch.object(
                    manager, "_finish_terminal_pending", return_value=True,
                ),
                mock.patch.object(manager, "_flush_batch_stats"),
            ):
                manager._worker_loop()

        self.assertEqual(persist.call_count, 2)
        self.assertEqual(manager._jobs, [job])
        self.assertTrue(job["_cleanup_only"])
        self.assertIsNone(manager._current_job)
        self.assertTrue(manager._paused.is_set())

    def test_load_failure_preserves_entire_original_journal(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            journal = root / "pending.json"
            saved_jobs = []
            for index in range(3):
                video = root / f"Video {index}.mp4"
                video.write_bytes(b"video")
                saved_jobs.append({
                    "path": str(video),
                    "title": f"Video {index}",
                    "channel": "Channel",
                    "retry_required": True,
                })
            journal.write_text(json.dumps(saved_jobs), encoding="utf-8")
            original = journal.read_bytes()

            with (
                mock.patch.object(
                    transcribe_core, "_pending_journal_path",
                    return_value=journal,
                ),
                mock.patch.object(
                    transcribe_core.os, "replace",
                    side_effect=PermissionError("replace denied"),
                ),
            ):
                recovered = manager.load_pending()

            self.assertEqual(recovered, 0)
            self.assertEqual(manager._jobs, [])
            self.assertEqual(journal.read_bytes(), original)
            self.assertEqual(
                list(root.glob(f".{journal.name}.*.tmp")), [],
            )

    def test_request_drain_does_not_start_unpersisted_restored_job(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock())
        queue_state = mock.Mock()
        manager.attach_queues(queue_state)
        with tempfile.TemporaryDirectory() as tmp_dir:
            video = Path(tmp_dir) / "Video.mp4"
            video.write_bytes(b"video")
            queue_state.gpu_snapshot.return_value = [{
                "kind": "transcribe",
                "path": str(video),
                "title": "Video",
            }]
            with (
                mock.patch.object(
                    manager, "_persist_pending", return_value=False,
                ),
                mock.patch.object(manager, "_ensure_worker") as start,
            ):
                started = manager.request_drain()

        self.assertFalse(started)
        self.assertEqual(manager._jobs, [])
        self.assertFalse(manager._manual_drain.is_set())
        start.assert_not_called()


class Patch2CaptionOutcomeTests(unittest.TestCase):
    @staticmethod
    def _caption_context(root: Path) -> tuple[Path, tuple]:
        video = root / "Captioned.mp4"
        video.write_bytes(b"video")
        (root / "Captioned.en.vtt").write_text("WEBVTT\n", encoding="utf-8")
        paths = (
            str(root / "Transcript.txt"),
            str(root / ".Transcript.jsonl"),
            2026,
            8,
            "08.31.2026",
        )
        return video, paths

    def test_index_failure_is_explicit_partial_caption_outcome(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            video, paths = self._caption_context(root)
            with (
                mock.patch.object(
                    transcribe_vtt, "_parse_vtt",
                    return_value=[{"s": 0.0, "e": 1.0, "t": "hello"}],
                ),
                mock.patch.object(
                    transcribe_vtt, "_resolve_transcript_paths",
                    return_value=paths,
                ),
                mock.patch.object(
                    transcribe_vtt, "_extract_video_id",
                    return_value="caption-video-id",
                ),
                mock.patch.object(
                    transcribe_vtt, "_write_transcript_entry",
                    return_value=True,
                ),
                mock.patch.object(
                    transcribe_vtt, "_write_jsonl_entry", return_value=True,
                ),
                mock.patch(
                    "backend.index.ingest_jsonl", return_value=0,
                ),
            ):
                outcome = transcribe_vtt._try_auto_captions(
                    str(video), "Captioned", "Channel", mock.Mock(),
                    allow_fetch=False, update_pending=False,
                )

        self.assertIs(outcome, transcribe_vtt._CaptionOutcome.PARTIAL)
        self.assertFalse(outcome)

    def test_missing_captions_are_explicitly_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            video = Path(tmp_dir) / "No Captions.mp4"
            video.write_bytes(b"video")
            outcome = transcribe_vtt._try_auto_captions(
                str(video), "No Captions", "Channel", mock.Mock(),
                allow_fetch=False, update_pending=False,
            )

        self.assertIs(outcome, transcribe_vtt._CaptionOutcome.UNAVAILABLE)
        self.assertFalse(outcome)


class Patch2WriterDurabilityTests(unittest.TestCase):
    @staticmethod
    def _original_bytes() -> bytes:
        return (json.dumps({
            "video_id": "old00000001",
            "title": "Existing",
            "start": 0.0,
            "end": 1.0,
            "text": "existing text",
            "words": [],
        }) + "\n").encode()

    @staticmethod
    def _segments() -> list[dict]:
        return [{"s": 1.0, "e": 2.0, "t": "new text", "w": []}]

    def test_txt_append_fsync_failure_preserves_original(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "Transcript.txt"
            target.write_text("original\n", encoding="utf-8")
            with mock.patch.object(
                    transcribe_files.os, "fsync",
                    side_effect=OSError("fsync denied")):
                saved = transcribe_files._write_transcript_entry_unlocked(
                    str(target), "Title", "08.31.2026", 1,
                    "WHISPER", "new text", video_id="abc123def45",
                )

            self.assertFalse(saved)
            self.assertEqual(target.read_text(encoding="utf-8"), "original\n")

    def test_jsonl_replace_fsync_failure_preserves_original(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / ".Transcript.jsonl"
            original = json.dumps({
                "video_id": "abc123def45",
                "title": "Title",
                "text": "old",
            }) + "\n"
            target.write_text(original, encoding="utf-8")
            with (
                mock.patch.object(transcribe_files, "_unhide_file_win"),
                mock.patch.object(transcribe_files, "_hide_file_win"),
                mock.patch.object(
                    transcribe_files.os, "fsync",
                    side_effect=OSError("fsync denied"),
                ),
            ):
                with self.assertRaises(OSError):
                    transcribe_files._replace_jsonl_entry_unlocked(
                        str(target), "Title", "abc123def45",
                        [{"s": 0.0, "e": 1.0, "t": "new"}],
                    )

            self.assertEqual(target.read_text(encoding="utf-8"), original)

    def test_txt_replace_fsync_failure_preserves_original(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "Transcript.txt"
            original = (
                "===(Title), (08.31.2026), (0:00:01), (WHISPER), "
                "(youtu.be/abc123def45)===\nold\n\n\n"
            )
            target.write_text(original, encoding="utf-8")
            with mock.patch.object(
                    transcribe_files.os, "fsync",
                    side_effect=OSError("fsync denied")):
                with self.assertRaises(OSError):
                    transcribe_files._replace_txt_entry_unlocked(
                        str(target), "Title", "new", "WHISPER",
                        video_id="abc123def45",
                    )

            self.assertEqual(target.read_text(encoding="utf-8"), original)

    def test_same_title_txt_replace_uses_header_video_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "Transcript.txt"
            target.write_text(
                "===(Same), (08.31.2026), (0:00:01), (WHISPER), "
                "(youtu.be/old00000001)===\nkeep me\n\n\n"
                "===(Same), (08.31.2026), (0:00:01), (WHISPER), "
                "(youtu.be/new00000001)===\nreplace me\n\n\n",
                encoding="utf-8",
            )

            transcribe_files._replace_txt_entry_unlocked(
                str(target), "Same", "replacement", "WHISPER",
                video_id="new00000001",
            )
            content = target.read_text(encoding="utf-8")

        self.assertIn("keep me", content)
        self.assertNotIn("replace me", content)
        self.assertIn("replacement", content)
        self.assertEqual(content.count("===(Same)"), 2)

    def test_append_retry_replaces_existing_video_id_in_both_sidecars(
            self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            txt_path = root / "Transcript.txt"
            jsonl_path = root / ".Transcript.jsonl"
            video_id = "abc123def45"

            self.assertTrue(transcribe_files._write_transcript_entry_unlocked(
                str(txt_path), "Title", "08.31.2026", 1,
                "YT CAPTIONS", "first", video_id=video_id,
            ))
            self.assertTrue(transcribe_files._write_jsonl_entry_unlocked(
                str(jsonl_path), video_id, "Title",
                [{"s": 0.0, "e": 1.0, "t": "first"}],
            ))
            self.assertTrue(transcribe_files._write_transcript_entry_unlocked(
                str(txt_path), "Title", "08.31.2026", 1,
                "YT CAPTIONS", "second", video_id=video_id,
            ))
            self.assertTrue(transcribe_files._write_jsonl_entry_unlocked(
                str(jsonl_path), video_id, "Title",
                [{"s": 0.0, "e": 1.0, "t": "second"}],
            ))

            txt = txt_path.read_text(encoding="utf-8")
            rows = [
                json.loads(line)
                for line in jsonl_path.read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual(txt.count("===(Title)"), 1)
        self.assertNotIn("first", txt)
        self.assertIn("second", txt)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["text"], "second")

    def test_fsync_denial_preserves_existing_aggregate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / ".Channel Transcript.jsonl"
            temp_path = Path(str(target) + ".tmp")
            original = self._original_bytes()
            target.write_bytes(original)

            with (
                mock.patch.object(transcribe_files, "_unhide_file_win"),
                mock.patch.object(transcribe_files, "_hide_file_win"),
                mock.patch.object(
                    transcribe_files.os, "fsync",
                    side_effect=OSError("fsync denied"),
                ),
                mock.patch.object(transcribe_files.os, "replace") as replace,
            ):
                ok = transcribe_files._write_jsonl_entry_unlocked(
                    str(target), "new00000001", "New", self._segments(),
                )

            self.assertFalse(ok)
            self.assertEqual(target.read_bytes(), original)
            self.assertFalse(temp_path.exists())
            replace.assert_not_called()

    def test_replace_denial_is_recoverable_without_duplicate_append(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / ".Channel Transcript.jsonl"
            original = self._original_bytes()
            target.write_bytes(original)

            with (
                mock.patch.object(transcribe_files, "_unhide_file_win"),
                mock.patch.object(transcribe_files, "_hide_file_win"),
                mock.patch.object(
                    transcribe_files.os, "replace",
                    side_effect=PermissionError("replace denied"),
                ),
            ):
                ok = transcribe_files._write_jsonl_entry_unlocked(
                    str(target), "new00000001", "New", self._segments(),
                )

            # The original is untouched and the complete temp is retained.
            temp_path = Path(str(target) + ".tmp")
            self.assertFalse(ok)
            self.assertEqual(target.read_bytes(), original)
            self.assertTrue(temp_path.exists())

            # Retrying promotes the complete temp and recognizes that this
            # exact append is already present; it must not append it twice.
            with (
                mock.patch.object(transcribe_files, "_unhide_file_win"),
                mock.patch.object(transcribe_files, "_hide_file_win"),
            ):
                retry_ok = transcribe_files._write_jsonl_entry_unlocked(
                    str(target), "new00000001", "New", self._segments(),
                )

            rows = [json.loads(line) for line in target.read_text().splitlines()]
            self.assertTrue(retry_ok)
            self.assertEqual([row["title"] for row in rows], ["Existing", "New"])
            self.assertFalse(temp_path.exists())


if __name__ == "__main__":
    unittest.main()
