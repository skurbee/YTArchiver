from __future__ import annotations

import atexit
import json
import os
import queue
import tempfile
import threading
import unittest
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(
    prefix="ytarchiver-transcribe-progress-")
atexit.register(_TEST_APPDATA.cleanup)
os.environ["APPDATA"] = _TEST_APPDATA.name

from backend.api_mixins.transcribe_mixin import TranscribeMixin
from backend.transcribe import core as transcribe_core


class TranscribeProgressStateTests(unittest.TestCase):
    def test_worker_ok_emits_machine_readable_finalizing_before_output_work(
            self) -> None:
        stream = mock.Mock()
        manager = transcribe_core.TranscribeManager(stream, model="small")
        manager._punctuate_enabled = False
        manager._manual_drain.set()

        process = mock.Mock()
        process.poll.return_value = None
        process.stdin.closed = False
        responses = queue.Queue()
        responses.put(json.dumps({"status": "progress", "pct": 99}))
        responses.put(json.dumps({
            "status": "ok",
            "model": "small",
            "text": "Fixture transcript.",
            "segments": [{
                "s": 0.0,
                "e": 1.0,
                "t": "Fixture transcript.",
                "w": [],
            }],
        }))
        manager._proc = process
        manager._loaded_model = "small"
        manager._line_queue = responses

        job = {
            "kind": "transcribe",
            "path": "Fixture finalization.mp4",
            "title": "Fixture finalization",
            "channel": "Fixture Channel",
            "video_id": "fixture-finalizing-video",
            "requested_model": "small",
            "retranscribe": True,
            "from_download": False,
            "cancel": threading.Event(),
        }
        output_started = threading.Event()
        release_output = threading.Event()
        outcome: list[transcribe_core._WorkerOutcome] = []

        def blocked_output(*_args, **_kwargs):
            output_started.set()
            release_output.wait(timeout=2.0)
            return transcribe_core._WorkerOutcome.SUCCESS

        def run_transcription() -> None:
            outcome.append(manager._transcribe_one_unleased(job))

        with (
            mock.patch.object(manager, "is_available", return_value=True),
            mock.patch.object(
                manager, "_accept_worker_model_report", return_value=True,
            ),
            mock.patch.object(
                manager, "_write_outputs", side_effect=blocked_output,
            ),
            mock.patch.object(
                manager, "_finish_successful_job", return_value=True,
            ),
            mock.patch.object(
                transcribe_core, "_ffprobe_duration", return_value=60.0,
            ),
        ):
            thread = threading.Thread(target=run_transcription, daemon=True)
            thread.start()
            try:
                self.assertTrue(
                    output_started.wait(timeout=1.0),
                    "output finalization did not begin",
                )
                self.assertTrue(
                    thread.is_alive(),
                    "test must observe the UI event while output work is active",
                )

                emitted = [call.args[0] for call in stream.emit.call_args_list]
                rendered = [
                    "".join(str(segment[0]) for segment in line)
                    for line in emitted
                ]
                progress_index = next(
                    index for index, text in enumerate(rendered)
                    if "99%" in text
                )
                finalizing_index = next(
                    index for index, text in enumerate(rendered)
                    if "Finalizing transcript" in text
                )
                self.assertLess(progress_index, finalizing_index)

                finalizing = emitted[finalizing_index]
                tags = {
                    tag
                    for _text, segment_tags in finalizing
                    for tag in (
                        segment_tags
                        if isinstance(segment_tags, list)
                        else [segment_tags]
                    )
                }
                self.assertIn("whisper_finalizing", tags)
                self.assertIn("tx_done_fixture-finalizing-video", tags)
                self.assertTrue(any(
                    tag.startswith("whisper_job_") for tag in tags
                ))
                self.assertNotIn("whisper_pct", tags)
            finally:
                release_output.set()
                thread.join(timeout=2.0)

        self.assertFalse(thread.is_alive())
        self.assertEqual(outcome, [transcribe_core._WorkerOutcome.SUCCESS])

    def test_chunked_worker_emits_tagged_finalizing_before_punctuation_and_output(
            self) -> None:
        stream = mock.Mock()
        manager = transcribe_core.TranscribeManager(stream, model="small")
        manager._punctuate_enabled = True
        punct_started = threading.Event()
        release_punct = threading.Event()
        outcomes: list[transcribe_core._WorkerOutcome] = []
        job = {
            "kind": "transcribe",
            "path": "Chunked fixture.mp4",
            "title": "Chunked fixture",
            "channel": "Fixture Channel",
            "video_id": "chunked00001",
            "job_tag": "whisper_job_chunked_fixture",
            "requested_model": "small",
            "retranscribe": True,
            "from_download": False,
            "cancel": threading.Event(),
        }
        worker_result = {
            "status": "ok",
            "model": "small",
            "text": "Chunked fixture transcript.",
            "segments": [{
                "s": 0.0,
                "e": 1.0,
                "t": "Chunked fixture transcript.",
                "w": [],
            }],
        }

        def blocked_punctuation(text: str) -> str:
            punct_started.set()
            release_punct.wait(timeout=2.0)
            return text

        def run_chunked() -> None:
            outcomes.append(manager._transcribe_chunked(job, 60.0))

        with (
            mock.patch.object(transcribe_core.subprocess, "run"),
            mock.patch.object(
                manager, "_transcribe_single_file",
                return_value=(transcribe_core._WorkerOutcome.SUCCESS,
                              worker_result),
            ),
            mock.patch.object(
                manager._punct, "punctuate", side_effect=blocked_punctuation,
            ),
            mock.patch.object(
                manager, "_write_outputs",
                return_value=transcribe_core._WorkerOutcome.SUCCESS,
            ) as write_outputs,
            mock.patch.object(
                manager, "_finish_successful_job", return_value=True,
            ),
        ):
            thread = threading.Thread(target=run_chunked, daemon=True)
            thread.start()
            try:
                self.assertTrue(
                    punct_started.wait(timeout=1.0),
                    "chunked punctuation did not begin",
                )
                self.assertTrue(thread.is_alive())
                write_outputs.assert_not_called()
                emitted = [call.args[0] for call in stream.emit.call_args_list]
                rendered = [
                    "".join(str(segment[0]) for segment in line)
                    for line in emitted
                ]
                finalizing_index = next(
                    index for index, text in enumerate(rendered)
                    if "Finalizing transcript" in text
                )
                finalizing = emitted[finalizing_index]
                tags = {
                    tag
                    for _text, segment_tags in finalizing
                    for tag in (
                        segment_tags
                        if isinstance(segment_tags, list)
                        else [segment_tags]
                    )
                }
                self.assertIn("whisper_finalizing", tags)
                self.assertIn("tx_done_chunked00001", tags)
                self.assertIn("whisper_job_chunked_fixture", tags)
                self.assertNotIn("whisper_pct", tags)
            finally:
                release_punct.set()
                thread.join(timeout=2.0)

        self.assertFalse(thread.is_alive())
        self.assertEqual(outcomes, [transcribe_core._WorkerOutcome.SUCCESS])


class TranscribeRuntimeStateCallbackTests(unittest.TestCase):
    @staticmethod
    def _job(state_cb: mock.Mock, success_cb: mock.Mock) -> dict:
        return {
            "task_id": "gpu-runtime-state-fixture",
            "kind": "transcribe",
            "path": "C:/Fixture/Runtime state.mp4",
            "title": "Runtime state",
            "channel": "Fixture Channel",
            "video_id": "runtime00001",
            "requested_model": "small",
            "actual_model": "small",
            "from_download": False,
            "retranscribe": True,
            "cancel": threading.Event(),
            "cb": success_cb,
            "state_cb": state_cb,
        }

    def _run_worker(self, outcomes: list[transcribe_core._WorkerOutcome]):
        stream = mock.Mock()
        manager = transcribe_core.TranscribeManager(stream, model="small")
        state_cb = mock.Mock()
        success_cb = mock.Mock()
        job = self._job(state_cb, success_cb)
        manager._jobs.append(job)
        manager._manual_drain.set()
        with (
            mock.patch.object(manager, "_persist_pending", return_value=True),
            mock.patch.object(manager, "_prepare_job_model", return_value=True),
            mock.patch.object(manager, "_transcribe_one",
                              side_effect=outcomes),
            mock.patch.object(
                manager, "_finish_terminal_pending", return_value=True,
            ),
            mock.patch.object(manager, "_flush_batch_stats"),
        ):
            manager._worker_loop()
        return manager, job, state_cb, success_cb

    def test_failed_job_reports_needs_attention_without_success_callback(
            self) -> None:
        manager, job, state_cb, success_cb = self._run_worker([
            transcribe_core._WorkerOutcome.FAILED,
        ])

        success_cb.assert_not_called()
        state_cb.assert_called_once()
        payload = state_cb.call_args.args[0]
        self.assertEqual(payload["state"], "needs_attention")
        self.assertEqual(payload["video_id"], "runtime00001")
        self.assertEqual(payload["filepath"], os.path.normpath(job["path"]))
        self.assertIn("Processing", payload["message"])
        self.assertTrue(manager._paused.is_set())
        self.assertIn(job, manager._jobs)

    def test_retry_reports_queued_then_cancel_reports_cancelled(self) -> None:
        _manager, _job, state_cb, success_cb = self._run_worker([
            transcribe_core._WorkerOutcome.RETRY,
            transcribe_core._WorkerOutcome.CANCELLED,
        ])

        success_cb.assert_not_called()
        states = [call.args[0]["state"] for call in state_cb.call_args_list]
        self.assertEqual(states, ["queued", "cancelled"])

    def test_pause_and_resume_report_truthful_runtime_states(self) -> None:
        manager = transcribe_core.TranscribeManager(mock.Mock(), model="small")
        state_cb = mock.Mock()
        success_cb = mock.Mock()
        job = self._job(state_cb, success_cb)
        manager._current_job = job

        manager.pause()
        manager.resume()

        success_cb.assert_not_called()
        payloads = [call.args[0] for call in state_cb.call_args_list]
        self.assertEqual([payload["state"] for payload in payloads],
                         ["paused", "resuming"])
        self.assertIn("Processing", payloads[0]["message"])
        self.assertIn("Resuming", payloads[1]["message"])

    def test_output_checkpoint_failure_reports_attention_not_success(self) -> None:
        """Durable output alone is not a completed Watch retranscription."""
        stream = mock.Mock()
        manager = transcribe_core.TranscribeManager(stream, model="small")
        state_cb = mock.Mock()
        success_cb = mock.Mock()
        job = self._job(state_cb, success_cb)
        manager._jobs.append(job)
        manager._manual_drain.set()

        def output_then_checkpoint(current_job: dict):
            saved = manager._finish_successful_job(
                current_job, {"text": "durable output"})
            return (transcribe_core._WorkerOutcome.SUCCESS if saved
                    else transcribe_core._WorkerOutcome.CLEANUP_FAILED)

        # queued->current saves, first completion checkpoint fails, then the
        # restored cleanup task is saved. The callback must remain untouched.
        with (
            mock.patch.object(
                manager, "_persist_pending",
                side_effect=[True, False, True],
            ) as persist,
            mock.patch.object(manager, "_prepare_job_model", return_value=True),
            mock.patch.object(
                manager, "_transcribe_one", side_effect=output_then_checkpoint,
            ),
            mock.patch.object(manager, "_flush_batch_stats"),
        ):
            manager._worker_loop()

        self.assertEqual(persist.call_count, 3)
        self.assertTrue(job.get("_output_complete"))
        success_cb.assert_not_called()
        state_cb.assert_called_once()
        payload = state_cb.call_args.args[0]
        self.assertEqual(payload["state"], "needs_attention")
        self.assertIn("Processing", payload["message"])
        self.assertTrue(manager._paused.is_set())
        self.assertIn(job, manager._jobs)

    def test_retry_job_reports_active_state_when_promoted_to_current(self) -> None:
        stream = mock.Mock()
        manager = transcribe_core.TranscribeManager(stream, model="small")
        state_cb = mock.Mock()
        success_cb = mock.Mock()
        job = self._job(state_cb, success_cb)
        job["_retry_required"] = True
        job["_retry_as_replace"] = True
        manager._jobs.append(job)
        manager._manual_drain.set()
        states_seen_before_work: list[str] = []

        def observe_then_cancel(_job: dict):
            states_seen_before_work.extend(
                call.args[0]["state"] for call in state_cb.call_args_list)
            return transcribe_core._WorkerOutcome.CANCELLED

        with (
            mock.patch.object(manager, "_persist_pending", return_value=True),
            mock.patch.object(manager, "_prepare_job_model", return_value=True),
            mock.patch.object(
                manager, "_transcribe_one", side_effect=observe_then_cancel,
            ),
            mock.patch.object(
                manager, "_finish_terminal_pending", return_value=True,
            ),
            mock.patch.object(manager, "_flush_batch_stats"),
        ):
            manager._worker_loop()

        self.assertTrue(
            any(state in {"resuming", "transcribing"}
                for state in states_seen_before_work),
            f"retry began while Watch still had no active state: "
            f"{states_seen_before_work}",
        )
        self.assertEqual(
            state_cb.call_args_list[-1].args[0]["state"], "cancelled")
        success_cb.assert_not_called()

    def test_completed_cleanup_retry_does_not_recreate_watch_state(self) -> None:
        manager = transcribe_core.TranscribeManager(
            mock.Mock(), model="small")
        state_cb = mock.Mock()
        success_cb = mock.Mock()
        job = self._job(state_cb, success_cb)
        job.update({
            "_cleanup_only": True,
            "_output_complete": True,
            "_callback_done": True,
            "_retry_required": True,
            "_completed_outcome": "success",
        })
        manager._jobs.append(job)
        manager._manual_drain.set()

        with (
            mock.patch.object(manager, "_persist_pending",
                              return_value=True),
            mock.patch.object(manager, "_transcribe_one") as transcribe,
            mock.patch.object(
                manager, "_finish_terminal_pending", return_value=True,
            ),
            mock.patch.object(manager, "_flush_batch_stats"),
        ):
            manager._worker_loop()

        transcribe.assert_not_called()
        success_cb.assert_not_called()
        state_cb.assert_not_called()

    def test_output_complete_callback_retry_reports_finalizing(self) -> None:
        manager = transcribe_core.TranscribeManager(
            mock.Mock(), model="small")
        state_cb = mock.Mock()
        success_cb = mock.Mock()
        job = self._job(state_cb, success_cb)
        job.update({
            "_cleanup_only": True,
            "_output_complete": True,
            "_callback_done": False,
            "_retry_required": True,
            "_completed_outcome": "success",
        })
        manager._jobs.append(job)
        manager._manual_drain.set()

        with (
            mock.patch.object(manager, "_persist_pending",
                              return_value=True),
            mock.patch.object(manager, "_transcribe_one") as transcribe,
            mock.patch.object(
                manager, "_finish_terminal_pending", return_value=True,
            ),
            mock.patch.object(manager, "_flush_batch_stats"),
        ):
            manager._worker_loop()

        transcribe.assert_not_called()
        success_cb.assert_called_once_with(None)
        states = [call.args[0]["state"] for call in state_cb.call_args_list]
        self.assertEqual(states, ["finalizing"])

    def test_cancelled_cleanup_retry_clears_watch_without_resuming(self) -> None:
        manager = transcribe_core.TranscribeManager(
            mock.Mock(), model="small")
        state_cb = mock.Mock()
        success_cb = mock.Mock()
        job = self._job(state_cb, success_cb)
        job.update({
            "_cleanup_only": True,
            "_output_complete": False,
            "_callback_done": False,
            "_retry_required": True,
        })
        manager._jobs.append(job)
        manager._manual_drain.set()

        with (
            mock.patch.object(manager, "_persist_pending",
                              return_value=True),
            mock.patch.object(manager, "_transcribe_one") as transcribe,
            mock.patch.object(
                manager, "_finish_terminal_pending", return_value=True,
            ),
            mock.patch.object(manager, "_flush_batch_stats"),
        ):
            manager._worker_loop()

        transcribe.assert_not_called()
        success_cb.assert_not_called()
        states = [call.args[0]["state"] for call in state_cb.call_args_list]
        self.assertEqual(states, ["cancelled"])


class PendingRetranscribeCancellationStateTests(unittest.TestCase):
    @staticmethod
    def _job(task_id: str, state_cb: mock.Mock,
             success_cb: mock.Mock) -> dict:
        return {
            "task_id": task_id,
            "kind": "transcribe",
            "path": f"C:/Fixture/{task_id}.mp4",
            "title": task_id,
            "channel": "Fixture Channel",
            "video_id": f"video-{task_id}",
            "retranscribe": True,
            "cancel": threading.Event(),
            "cb": success_cb,
            "state_cb": state_cb,
        }

    def _manager_with_jobs(self, count: int = 2):
        manager = transcribe_core.TranscribeManager(mock.Mock(), model="small")
        records = []
        for index in range(count):
            state_cb = mock.Mock()
            success_cb = mock.Mock()
            job = self._job(f"queued-{index + 1}", state_cb, success_cb)
            manager._jobs.append(job)
            records.append((job, state_cb, success_cb))
        return manager, records

    @staticmethod
    def _assert_cancelled_once(record) -> None:
        _job, state_cb, success_cb = record
        success_cb.assert_not_called()
        state_cb.assert_called_once()
        payload = state_cb.call_args.args[0]
        assert payload["state"] == "cancelled", payload

    def test_coordinated_removal_notifies_only_after_both_stores_commit(
            self) -> None:
        manager, records = self._manager_with_jobs()
        target, sibling = records
        mirror_committed = False
        journal_committed = False

        def mirror_remove() -> bool:
            nonlocal mirror_committed
            mirror_committed = True
            return True

        def persist() -> bool:
            nonlocal journal_committed
            journal_committed = True
            return True

        def observe_cancel(payload: dict) -> None:
            self.assertTrue(mirror_committed)
            self.assertTrue(journal_committed)
            self.assertEqual(payload["state"], "cancelled")

        target[1].side_effect = observe_cancel
        with mock.patch.object(manager, "_persist_pending", side_effect=persist):
            removed = manager.remove_pending_task_ids_coordinated(
                {target[0]["task_id"]}, mirror_remove, mock.Mock())

        self.assertTrue(removed)
        self._assert_cancelled_once(target)
        sibling[1].assert_not_called()
        sibling[2].assert_not_called()
        self.assertNotIn(target[0], manager._jobs)
        self.assertIn(sibling[0], manager._jobs)

    def test_failed_coordinated_removal_keeps_job_and_sends_no_cancel(
            self) -> None:
        manager, records = self._manager_with_jobs(1)
        target = records[0]
        mirror_restore = mock.Mock(return_value=True)
        with mock.patch.object(manager, "_persist_pending", return_value=False):
            removed = manager.remove_pending_task_ids_coordinated(
                {target[0]["task_id"]}, mock.Mock(return_value=True),
                mirror_restore)

        self.assertFalse(removed)
        self.assertIn(target[0], manager._jobs)
        target[1].assert_not_called()
        target[2].assert_not_called()
        mirror_restore.assert_called_once_with()

    def test_predicate_removal_notifies_after_journal_commit(self) -> None:
        manager, records = self._manager_with_jobs()
        target, sibling = records
        journal_committed = False

        def persist() -> bool:
            nonlocal journal_committed
            journal_committed = True
            return True

        def observe_cancel(payload: dict) -> None:
            self.assertTrue(journal_committed)
            self.assertEqual(payload["state"], "cancelled")

        target[1].side_effect = observe_cancel
        with mock.patch.object(manager, "_persist_pending", side_effect=persist):
            removed = manager.remove_pending_jobs(
                lambda job: job["task_id"] == target[0]["task_id"])

        self.assertEqual(removed, 1)
        self._assert_cancelled_once(target)
        sibling[1].assert_not_called()
        sibling[2].assert_not_called()

    def test_failed_predicate_removal_sends_no_cancel(self) -> None:
        manager, records = self._manager_with_jobs(1)
        target = records[0]
        with mock.patch.object(manager, "_persist_pending", return_value=False):
            removed = manager.remove_pending_jobs(lambda _job: True)

        self.assertEqual(removed, 0)
        self.assertIn(target[0], manager._jobs)
        target[1].assert_not_called()
        target[2].assert_not_called()

    def test_cancel_all_notifies_each_durably_removed_queued_job_once(
            self) -> None:
        manager, records = self._manager_with_jobs(2)
        durable_clear_finished = False

        def clear_journal() -> bool:
            nonlocal durable_clear_finished
            durable_clear_finished = True
            return True

        for _job, state_cb, _success_cb in records:
            state_cb.side_effect = lambda payload: (
                self.assertTrue(durable_clear_finished),
                self.assertEqual(payload["state"], "cancelled"),
            )
        with mock.patch.object(
                manager, "clear_pending_journal", side_effect=clear_journal):
            cancelled = manager.cancel_all(clear_visible=False)

        self.assertTrue(cancelled)
        self.assertEqual(manager._jobs, [])
        for record in records:
            self._assert_cancelled_once(record)

    def test_failed_cancel_all_keeps_jobs_and_sends_no_cancel(self) -> None:
        manager, records = self._manager_with_jobs(2)
        with mock.patch.object(
                manager, "clear_pending_journal", return_value=False):
            cancelled = manager.cancel_all(clear_visible=False)

        self.assertFalse(cancelled)
        self.assertEqual(manager._jobs, [record[0] for record in records])
        for _job, state_cb, success_cb in records:
            state_cb.assert_not_called()
            success_cb.assert_not_called()


class RetranscribeApiStateCallbackTests(unittest.TestCase):
    class Api(TranscribeMixin):
        pass

    def _api(self, manager: mock.Mock):
        api = self.Api()
        api.services = None
        api._transcribe = manager
        api._log_stream = mock.Mock()
        api._window = mock.Mock()
        return api

    def test_enqueue_rejection_never_invokes_success_completion_hook(
            self) -> None:
        manager = mock.Mock()
        manager.enqueue.return_value = False
        api = self._api(manager)
        extra_success = mock.Mock()
        with tempfile.TemporaryDirectory() as tmp:
            video = os.path.join(tmp, "Rejected [reject00001].mp4")
            with open(video, "wb") as handle:
                handle.write(b"fixture")
            with mock.patch("backend.index._reader_open", return_value=None):
                result = api.transcribe_retranscribe(
                    video, "Rejected", "reject00001",
                    _on_complete_extra=extra_success,
                )

        self.assertFalse(result["ok"])
        # The internal Python cleanup hook still runs so a temporary model
        # override can be restored. Only the browser's success-only refresh
        # hook must be withheld for a job that never entered the queue.
        extra_success.assert_called_once()
        cleanup_payload = extra_success.call_args.args[0]
        self.assertFalse(cleanup_payload["ok"])
        self.assertEqual(cleanup_payload["state"], "rejected")
        self.assertTrue(cleanup_payload["error"])
        scripts = [call.args[0] for call in api._window.evaluate_js.call_args_list]
        self.assertFalse(any("_onRetranscribeComplete" in script
                             for script in scripts), scripts)
        rejected = [script for script in scripts
                    if "_onRetranscribeState" in script]
        self.assertEqual(len(rejected), 1, scripts)
        self.assertIn("rejected", rejected[0])

    def test_runtime_state_and_success_use_separate_browser_hooks(self) -> None:
        manager = mock.Mock()
        manager.enqueue.return_value = True
        api = self._api(manager)
        with tempfile.TemporaryDirectory() as tmp:
            video = os.path.join(tmp, "State [states00001].mp4")
            with open(video, "wb") as handle:
                handle.write(b"fixture")
            with mock.patch("backend.index._reader_open", return_value=None):
                result = api.transcribe_retranscribe(
                    video, "State", "states00001")

            self.assertTrue(result["ok"])
            enqueue_kwargs = manager.enqueue.call_args.kwargs
            self.assertIn("on_state", enqueue_kwargs)
            state_cb = enqueue_kwargs["on_state"]
            complete_cb = enqueue_kwargs["on_complete"]
            state_cb({
                "state": "paused",
                "video_id": "states00001",
                "filepath": os.path.normpath(video),
                "message": "Paused — resume from Processing",
            })
            scripts_after_state = [
                call.args[0] for call in api._window.evaluate_js.call_args_list
            ]
            self.assertTrue(any("_onRetranscribeState" in script
                                for script in scripts_after_state))
            self.assertFalse(any("_onRetranscribeComplete" in script
                                 for script in scripts_after_state))

            complete_cb({"ok": True})

        scripts = [call.args[0] for call in api._window.evaluate_js.call_args_list]
        self.assertEqual(sum("_onRetranscribeState" in script
                             for script in scripts), 1)
        self.assertEqual(sum("_onRetranscribeComplete" in script
                             for script in scripts), 1)

    def test_completion_hook_distinguishes_kept_existing_transcript(
            self) -> None:
        def completion_payload(result):
            manager = mock.Mock()
            manager.enqueue.return_value = True
            api = self._api(manager)
            with tempfile.TemporaryDirectory() as tmp:
                video = os.path.join(tmp, "Flag [flags000001].mp4")
                with open(video, "wb") as handle:
                    handle.write(b"fixture")
                with mock.patch("backend.index._reader_open",
                                return_value=None):
                    queued = api.transcribe_retranscribe(
                        video, "Flag", "flags000001")
                self.assertTrue(queued["ok"])
                manager.enqueue.call_args.kwargs["on_complete"](result)

            scripts = [
                call.args[0] for call in api._window.evaluate_js.call_args_list
                if "_onRetranscribeComplete" in call.args[0]
            ]
            self.assertEqual(len(scripts), 1, scripts)
            marker = "window._onRetranscribeComplete("
            encoded = scripts[0].partition(marker)[2].rsplit(");", 1)[0]
            return json.loads(encoded)

        kept = completion_payload({
            "ok": True,
            "_existing_transcript_kept": True,
        })
        ordinary = completion_payload({"ok": True})

        self.assertIs(kept["existing_transcript_kept"], True)
        self.assertFalse(ordinary.get("existing_transcript_kept", False))


if __name__ == "__main__":
    unittest.main()
