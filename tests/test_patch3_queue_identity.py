from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-queue-id-tests-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import queues
from backend.api_mixins.channel_mixin import ChannelMixin
from backend.api_mixins.queue_mixin import QueueMixin
from backend.api_mixins.redownload_mixin import RedownloadMixin
from backend.api_mixins.sync_mixin import SyncMixin
from backend.transcribe import core as transcribe_core
from backend.transcribe.core import TranscribeManager


class QueueIdentityTests(unittest.TestCase):
    def _quiet(self, state: queues.QueueState):
        return mock.patch.object(state, "save_debounced")

    def test_duplicate_sync_target_mutates_only_clicked_task_id(self):
        state = queues.QueueState()
        with self._quiet(state):
            self.assertTrue(state.sync_enqueue({
                "kind": "download", "name": "Same", "url": "u"}))
            self.assertTrue(state.sync_enqueue({
                "kind": "metadata", "name": "Same", "url": "u"}))
            first, second = state.sync_snapshot()
            self.assertNotEqual(first["task_id"], second["task_id"])

            self.assertTrue(state.sync_reorder(second["task_id"], 0))
            self.assertEqual(
                [item["kind"] for item in state.sync_snapshot()],
                ["metadata", "download"],
            )
            self.assertTrue(state.sync_remove_task(second["task_id"]))

        remaining = state.sync_snapshot()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["task_id"], first["task_id"])

    def test_duplicate_gpu_path_is_distinct_by_kind_and_exact_id(self):
        state = queues.QueueState()
        with self._quiet(state):
            self.assertTrue(state.gpu_enqueue({
                "kind": "transcribe", "title": "Same", "path": "v.mp4"}))
            self.assertTrue(state.gpu_enqueue({
                "kind": "compress", "title": "Same", "path": "v.mp4"}))
            transcribe, compress = state.gpu_snapshot()
            self.assertTrue(state.gpu_remove(compress["task_id"]))

        self.assertEqual(
            [item["task_id"] for item in state.gpu_snapshot()],
            [transcribe["task_id"]],
        )

    def test_legacy_file_migrates_ids_and_redownload_kind_durably(self):
        with tempfile.TemporaryDirectory() as td:
            queue_file = Path(td) / "queue.json"
            queue_file.write_text(json.dumps({
                "sync": [{
                    "name": "Restore",
                    "url": "u",
                    "redownload_res": "720",
                }],
                "gpu": [{"kind": "compress", "path": "v.mp4"}],
                "resuming": {
                    "sync": {
                        "name": "Current",
                        "url": "r",
                        "kind": "redownload",
                        "redownload_res": "480",
                    }
                },
                "_schema_version": 2,
            }), encoding="utf-8")

            with mock.patch.object(queues, "QUEUE_FILE", queue_file), \
                    mock.patch.object(queues, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(queues.QueueState, "save_debounced"):
                state = queues.QueueState()
                self.assertTrue(state.load())
                pending_id = state.sync[0]["task_id"]
                resuming = state.get_loaded_resuming()["sync"]
                self.assertEqual(state.sync[0]["kind"], "redownload")
                self.assertEqual(resuming["kind"], "redownload")
                self.assertTrue(resuming["task_id"])
                self.assertTrue(state.save_now())

                reloaded = queues.QueueState()
                self.assertTrue(reloaded.load())
                self.assertEqual(reloaded.sync[0]["task_id"], pending_id)
                saved = json.loads(queue_file.read_text(encoding="utf-8"))
                self.assertEqual(saved["_schema_version"], 3)
                self.assertEqual(
                    saved["resuming"]["sync"]["task_id"],
                    resuming["task_id"],
                )

    def test_collision_is_repaired_across_pending_and_resuming(self):
        with tempfile.TemporaryDirectory() as td:
            queue_file = Path(td) / "queue.json"
            queue_file.write_text(json.dumps({
                "_schema_version": 2,
                "sync": [{
                    "task_id": "duplicate", "kind": "download",
                    "name": "Pending", "url": "pending",
                }],
                "gpu": [],
                "resuming": {"sync": {
                    "task_id": "duplicate", "kind": "redownload",
                    "name": "Current", "url": "current",
                    "redownload_res": "720",
                }},
            }), encoding="utf-8")
            with mock.patch.object(queues, "QUEUE_FILE", queue_file), \
                    mock.patch.object(queues, "config_is_writable",
                                      return_value=True):
                state = queues.QueueState()
                self.assertTrue(state.load())
            self.assertEqual(state.sync[0]["task_id"], "duplicate")
            self.assertNotEqual(
                state.get_loaded_resuming()["sync"]["task_id"], "duplicate")

    def test_failed_migration_save_is_exposed_as_not_durable(self):
        with tempfile.TemporaryDirectory() as td:
            queue_file = Path(td) / "queue.json"
            queue_file.write_text(json.dumps({
                "sync": [{"name": "Legacy", "url": "u"}], "gpu": [],
            }), encoding="utf-8")
            with mock.patch.object(queues, "QUEUE_FILE", queue_file), \
                    mock.patch.object(queues, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(queues.QueueState,
                                      "_write_save_payload",
                                      return_value=False), \
                    mock.patch.object(queues.QueueState, "save_debounced"):
                state = queues.QueueState()
                self.assertTrue(state.load())
                payload = state.to_ui_payload()
            self.assertFalse(payload["identity_ids_durable"])
            self.assertTrue(payload["sync"][0]["task_id"])

    def test_ui_payload_maps_group_to_exact_pending_ids(self):
        state = queues.QueueState()
        with self._quiet(state):
            for index in range(3):
                state.gpu_enqueue({
                    "kind": "transcribe",
                    "path": f"{index}.mp4",
                    "title": f"Video {index}",
                    "channel": "Same",
                    "bulk_id": "bulk",
                })
        payload = state.to_ui_payload()["gpu"]
        self.assertEqual(len(payload), 1)
        row = payload[0]
        self.assertFalse(row["draggable"])
        self.assertEqual(row["pending_indices"], [0, 1, 2])
        self.assertEqual(
            row["represented_task_ids"],
            [item["task_id"] for item in state.gpu_snapshot()],
        )

    def test_bridge_rejects_url_and_accepts_exact_task_id(self):
        state = queues.QueueState()
        with self._quiet(state):
            state.sync_enqueue({"kind": "download", "name": "Same", "url": "u"})
            state.sync_enqueue({"kind": "metadata", "name": "Same", "url": "u"})
        wanted = state.sync_snapshot()[1]["task_id"]

        class Api(QueueMixin):
            def __init__(self):
                self._queues = state
                self.services = None
                self._on_queue_changed = mock.Mock()

        api = Api()
        self.assertEqual(api.queues_sync_remove("u"), {"ok": False})
        self.assertEqual(api.queues_sync_remove(wanted), {"ok": True})
        self.assertEqual(
            [item["kind"] for item in state.sync_snapshot()], ["download"])

    def test_restored_redownload_routes_by_kind_and_keeps_task_id(self):
        state = queues.QueueState()
        with self._quiet(state):
            state.sync_enqueue({
                "kind": "download", "name": "Same", "url": "u"})
            state.sync_enqueue({
                "kind": "redownload", "name": "Same", "url": "u",
                "redownload_res": "720"})
        redownload_id = state.sync_snapshot()[1]["task_id"]
        calls = []

        class Api(RedownloadMixin):
            def __init__(self):
                self._queues = state
                self.services = None

            def chan_redownload(self, identity, resolution, **kwargs):
                calls.append((identity, resolution, kwargs))
                return {"ok": True}

        result = Api().resume_pending_redownloads()
        self.assertEqual(result, {
            "ok": True, "resumed": 1, "skipped": 0,
            "regular_pending": 1,
        })
        self.assertEqual([item["kind"] for item in state.sync_snapshot()],
                         ["download", "redownload"])
        self.assertEqual(calls[0][1], "720")
        self.assertEqual(calls[0][2]["task_id"], redownload_id)
        self.assertTrue(calls[0][2]["_queue_only"])

    def test_processing_journal_and_ui_payload_share_task_id(self):
        job = {
            "task_id": "gpu-permanent", "kind": "compress",
            "path": "v.mp4", "title": "Video", "quality": "Average",
            "output_res": "720",
        }
        journal = TranscribeManager._snapshot_pending_job(job)
        queue_payload = TranscribeManager._queue_payload_for_job(job)
        self.assertEqual(journal["task_id"], "gpu-permanent")
        self.assertEqual(queue_payload["task_id"], "gpu-permanent")
        self.assertEqual(journal["kind"], "compress")
        self.assertEqual(queue_payload["kind"], "compress")

    def test_processing_defer_retries_same_id_at_tail(self):
        state = queues.QueueState()
        with self._quiet(state):
            state.gpu_enqueue({
                "task_id": "next", "kind": "compress",
                "path": "next.mp4", "title": "Next",
            })
        manager = TranscribeManager(mock.Mock())
        manager.attach_queues(state)
        deferred = {
            "task_id": "deferred", "kind": "compress",
            "path": "same.mp4", "title": "Same",
            "cancel": threading.Event(),
            "_defer_requested": True,
        }
        with self._quiet(state):
            manager._restore_job_after_outcome(
                deferred, transcribe_core._WorkerOutcome.RETRY)
        self.assertEqual(
            [item["task_id"] for item in state.gpu_snapshot()],
            ["next", "deferred"],
        )
        self.assertEqual(manager._jobs[0]["task_id"], "deferred")

    def test_exact_sync_remove_rolls_back_when_queue_file_cannot_commit(self):
        with tempfile.TemporaryDirectory() as td:
            queue_file = Path(td) / "queue.json"
            with mock.patch.object(queues, "QUEUE_FILE", queue_file), \
                    mock.patch.object(queues, "config_is_writable",
                                      return_value=True):
                state = queues.QueueState()
                task_id = state.sync_enqueue_with_id({
                    "task_id": "sync-durable",
                    "kind": "download", "name": "A", "url": "u",
                }, durable=True)

                class Api(QueueMixin):
                    def __init__(self):
                        self._queues = state
                        self.services = None
                        self._on_queue_changed = mock.Mock()

                with mock.patch.object(
                        state, "_write_save_payload", return_value=False):
                    result = Api().queues_sync_remove(task_id)

                self.assertEqual(result, {"ok": False})
                self.assertEqual(
                    [item["task_id"] for item in state.sync_snapshot()],
                    ["sync-durable"],
                )
                self.assertEqual(
                    json.loads(queue_file.read_text(encoding="utf-8"))
                    ["sync"][0]["task_id"],
                    "sync-durable",
                )
                self.assertFalse(
                    state.to_ui_payload()["identity_ids_durable"])
                state.mark_orphan()

    def test_gpu_bridge_rolls_back_remove_and_reorder_on_journal_failure(self):
        state = queues.QueueState()
        manager = TranscribeManager(mock.Mock())
        manager.attach_queues(state)
        manager._jobs = [
            {"task_id": "gpu-a", "kind": "transcribe", "path": "a.mp4",
             "channel": "", "cancel": threading.Event()},
            {"task_id": "gpu-b", "kind": "transcribe", "path": "b.mp4",
             "channel": "", "cancel": threading.Event()},
        ]
        with self._quiet(state):
            for job in manager._jobs:
                state.gpu_enqueue({
                    "task_id": job["task_id"], "kind": job["kind"],
                    "path": job["path"],
                })

        class Api(QueueMixin):
            def __init__(self):
                self._queues = state
                self._transcribe = manager
                self.services = None
                self._on_queue_changed = mock.Mock()

        with mock.patch.object(state, "save_now", return_value=True), \
                mock.patch.object(manager, "_persist_pending",
                                  return_value=False):
            removed = Api().queues_gpu_remove("gpu-a")
            reordered = Api().queues_gpu_reorder("gpu-b", 0)
            grouped = Api().queues_gpu_remove_many(["gpu-a", "gpu-b"])

        self.assertFalse(removed["ok"])
        self.assertFalse(reordered["ok"])
        self.assertFalse(grouped["ok"])
        self.assertEqual(
            [item["task_id"] for item in state.gpu_snapshot()],
            ["gpu-a", "gpu-b"],
        )
        self.assertEqual(
            [job["task_id"] for job in manager._jobs],
            ["gpu-a", "gpu-b"],
        )

    def test_enqueue_adopts_orphan_visible_id_and_rejects_failed_reservation(self):
        with tempfile.TemporaryDirectory() as td:
            video = Path(td) / "Video.mp4"
            video.write_bytes(b"video")
            state = queues.QueueState()
            with self._quiet(state):
                state.gpu_enqueue({
                    "task_id": "gpu-orphan", "kind": "transcribe",
                    "path": str(video), "title": "Old",
                })
            manager = TranscribeManager(mock.Mock())
            manager.attach_queues(state)
            with mock.patch.object(state, "save_now", return_value=True), \
                    mock.patch.object(manager, "_persist_pending",
                                      return_value=True), \
                    mock.patch.object(manager, "_ensure_worker"), \
                    mock.patch.object(transcribe_core,
                                      "_bump_transcription_pending"):
                self.assertTrue(manager.enqueue(str(video), "New"))

            self.assertEqual(manager._jobs[0]["task_id"], "gpu-orphan")
            self.assertEqual(
                [item["task_id"] for item in state.gpu_snapshot()],
                ["gpu-orphan"],
            )

            other = Path(td) / "Other.mp4"
            other.write_bytes(b"video")
            blocked = TranscribeManager(mock.Mock())
            blocked.attach_queues(queues.QueueState())
            with mock.patch.object(
                    blocked._queues, "save_now", return_value=False), \
                    mock.patch.object(blocked, "_persist_pending") as persist, \
                    mock.patch.object(blocked, "_ensure_worker") as start, \
                    mock.patch.object(transcribe_core,
                                      "_bump_transcription_pending"):
                self.assertFalse(blocked.enqueue(str(other), "Other"))
            self.assertEqual(blocked._jobs, [])
            self.assertEqual(blocked._queues.gpu_snapshot(), [])
            persist.assert_not_called()
            start.assert_not_called()

    def test_duplicate_redownload_never_creates_hidden_idless_chain_item(self):
        class AliveThread:
            @staticmethod
            def is_alive():
                return True

        class Api(ChannelMixin):
            def __init__(self, state):
                self._queues = state
                self._sync_thread = AliveThread()
                self._redwnl_lock = threading.Lock()
                self._redwnl_pending = []
                self._on_queue_changed = mock.Mock()

        with tempfile.TemporaryDirectory() as td:
            (Path(td) / "Channel").mkdir()
            state = queues.QueueState()
            state.current_sync = {
                "task_id": "sync-running", "kind": "redownload",
                "name": "Other", "url": "other",
            }
            api = Api(state)
            with mock.patch.object(state, "save_now", return_value=True), \
                    mock.patch(
                        "backend.api_mixins.channel_mixin.subs_backend.get_channel",
                        return_value={"name": "Channel", "url": "u"}), \
                    mock.patch(
                        "backend.api_mixins.channel_mixin.load_config",
                        return_value={"output_dir": td}), \
                    mock.patch(
                        "backend.sync.channel_folder_name",
                        return_value="Channel"):
                first = api.chan_redownload("Channel", "720")
                second = api.chan_redownload("Channel", "480")

        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(len(state.sync_snapshot()), 1)
        self.assertEqual(len(api._redwnl_pending), 1)
        self.assertEqual(
            api._redwnl_pending[0]["rd_task"]["task_id"],
            state.sync_snapshot()[0]["task_id"],
        )

    def test_redownload_remove_and_reorder_update_runtime_chain_atomically(self):
        state = queues.QueueState()
        with self._quiet(state):
            for task_id, name in (("sync-a", "A"), ("sync-b", "B")):
                state.sync_enqueue({
                    "task_id": task_id, "kind": "redownload",
                    "name": name, "url": name.lower(),
                    "redownload_res": "720",
                })

        class Api(QueueMixin):
            def __init__(self):
                self._queues = state
                self.services = None
                self._redwnl_lock = threading.Lock()
                self._redwnl_pending = [
                    {"rd_task": {"task_id": "sync-a"}},
                    {"rd_task": {"task_id": "sync-b"}},
                ]
                self._on_queue_changed = mock.Mock()

        api = Api()
        with mock.patch.object(state, "save_now", return_value=True):
            self.assertEqual(api.queues_sync_reorder("sync-b", 0),
                             {"ok": True})
            self.assertEqual(api.queues_sync_remove("sync-b"),
                             {"ok": True})
        self.assertEqual(
            [item["task_id"] for item in state.sync_snapshot()], ["sync-a"])
        self.assertEqual(
            [item["rd_task"]["task_id"] for item in api._redwnl_pending],
            ["sync-a"],
        )

        with mock.patch.object(state, "save_now", return_value=False):
            self.assertEqual(api.queues_sync_remove("sync-a"), {"ok": False})
        self.assertEqual(
            [item["task_id"] for item in state.sync_snapshot()], ["sync-a"])
        self.assertEqual(
            [item["rd_task"]["task_id"] for item in api._redwnl_pending],
            ["sync-a"],
        )

    def test_sync_defer_commits_same_id_before_skip_and_rolls_back_failure(self):
        class Api(SyncMixin):
            def __init__(self, state):
                self._queues = state
                self._log_stream = mock.Mock()
                self.sync_skip_current = mock.Mock(return_value={"ok": True})

        current = {
            "task_id": "sync-current", "kind": "download",
            "name": "Current", "url": "u", "_pass_start_ts": 123,
        }
        failed_state = queues.QueueState()
        failed_state.current_sync = dict(current)
        failed_api = Api(failed_state)
        with mock.patch.object(failed_state, "save_now", return_value=False):
            failed = failed_api.sync_defer_current("sync-current")
        self.assertFalse(failed["ok"])
        self.assertEqual(failed_state.sync_snapshot(), [])
        failed_api.sync_skip_current.assert_not_called()

        state = queues.QueueState()
        state.current_sync = dict(current)
        api = Api(state)
        with mock.patch.object(state, "save_now", return_value=True):
            result = api.sync_defer_current("sync-current")
        self.assertEqual(result, {"ok": True})
        self.assertEqual(state.sync_snapshot()[0]["task_id"], "sync-current")
        self.assertNotIn("_pass_start_ts", state.sync_snapshot()[0])
        api.sync_skip_current.assert_called_once_with("sync-current")

    def test_gpu_defer_requires_both_queue_and_journal_before_cancel(self):
        def manager_with_current():
            state = queues.QueueState()
            manager = TranscribeManager(mock.Mock())
            manager.attach_queues(state)
            job = {
                "task_id": "gpu-current", "kind": "transcribe",
                "path": "video.mp4", "title": "Video",
                "cancel": threading.Event(),
            }
            manager._current_job = job
            state.current_gpu = TranscribeManager._queue_payload_for_job(job)
            return state, manager, job

        state, manager, job = manager_with_current()
        with mock.patch.object(state, "save_now", return_value=True), \
                mock.patch.object(manager, "_persist_pending",
                                  return_value=False), \
                mock.patch.object(manager, "_send_cancel_command") as cancel:
            self.assertFalse(manager.defer_current("gpu-current"))
        self.assertFalse(job["cancel"].is_set())
        self.assertNotIn("_defer_requested", job)
        self.assertEqual(state.gpu_snapshot(), [])
        cancel.assert_not_called()

        state, manager, job = manager_with_current()
        with mock.patch.object(state, "save_now", return_value=True), \
                mock.patch.object(manager, "_persist_pending",
                                  return_value=True), \
                mock.patch.object(manager, "_send_cancel_command") as cancel:
            self.assertTrue(manager.defer_current("gpu-current"))
        self.assertTrue(job["cancel"].is_set())
        self.assertTrue(job["_defer_requested"])
        self.assertEqual(state.gpu_snapshot()[0]["task_id"], "gpu-current")
        cancel.assert_called_once_with()

    def test_exact_queue_commit_fails_closed_when_fsync_is_denied(self):
        with tempfile.TemporaryDirectory() as td:
            queue_file = Path(td) / "queue.json"
            with mock.patch.object(queues, "QUEUE_FILE", queue_file), \
                    mock.patch.object(queues, "config_is_writable",
                                      return_value=True), \
                    mock.patch.object(queues.os, "fsync",
                                      side_effect=PermissionError("denied")):
                state = queues.QueueState()
                task_id = state.sync_enqueue_with_id({
                    "kind": "download", "name": "A", "url": "u",
                }, durable=True)
                self.assertIsNone(task_id)
                self.assertEqual(state.sync_snapshot(), [])
                self.assertFalse(
                    state.to_ui_payload()["identity_ids_durable"])
                state.mark_orphan()

    def test_defer_crash_snapshot_keeps_pending_copy_only_once(self):
        with tempfile.TemporaryDirectory() as td:
            queue_file = Path(td) / "queue.json"
            task = {
                "task_id": "sync-deferred", "kind": "download",
                "name": "A", "url": "u",
            }
            queue_file.write_text(json.dumps({
                "_schema_version": 3,
                "sync": [task],
                "gpu": [],
                "resuming": {"sync": task},
            }), encoding="utf-8")
            with mock.patch.object(queues, "QUEUE_FILE", queue_file), \
                    mock.patch.object(queues, "config_is_writable",
                                      return_value=False):
                state = queues.QueueState()
                self.assertTrue(state.load())
                self.assertEqual(len(state.sync_snapshot()), 1)
                self.assertNotIn("sync", state.get_loaded_resuming())
                state.mark_orphan()

    def test_channel_cancel_keeps_runtime_and_progress_when_save_fails(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            folder = root / "Channel"
            folder.mkdir()
            progress = folder / "_redownload_progress.json"
            progress.write_text("{}", encoding="utf-8")
            queue_file = root / "queue.json"
            task = {
                "task_id": "sync-redownload", "kind": "redownload",
                "name": "Channel", "url": "channel-url",
                "redownload_res": "720",
            }

            with mock.patch.object(queues, "QUEUE_FILE", queue_file), \
                    mock.patch.object(queues, "config_is_writable",
                                      return_value=True):
                state = queues.QueueState()
                self.assertTrue(state.sync_enqueue_with_id(task, durable=True))

                class Api(ChannelMixin):
                    def __init__(self):
                        self._queues = state
                        self._config = {"output_dir": str(root)}
                        self._redwnl_lock = threading.Lock()
                        self._redwnl_pending = [{
                            "rd_task": dict(task), "ch": dict(task),
                            "new_res": "720",
                        }]
                        self._redwnl_cancel = threading.Event()
                        self._on_queue_changed = mock.Mock()
                        self._window = None

                api = Api()
                with mock.patch(
                        "backend.api_mixins.channel_mixin.subs_backend.get_channel",
                        return_value={"name": "Channel", "url": "channel-url"}), \
                        mock.patch("backend.sync.channel_folder_name",
                                   return_value="Channel"), \
                        mock.patch.object(state, "_write_save_payload",
                                          return_value=False):
                    result = api.chan_cancel_redownload("Channel")

                self.assertFalse(result["ok"])
                self.assertEqual(
                    [item["task_id"] for item in state.sync_snapshot()],
                    ["sync-redownload"],
                )
                self.assertEqual(len(api._redwnl_pending), 1)
                self.assertTrue(progress.exists())
                self.assertFalse(api._redwnl_cancel.is_set())
                state.mark_orphan()

    def test_failed_post_sync_redownload_drain_keeps_execution_item(self):
        state = queues.QueueState()
        item = {
            "ch": {"name": "Channel", "url": "u"},
            "new_res": "720", "scope": None, "only_video": None,
            "rd_task": {
                "task_id": "sync-redownload", "kind": "redownload",
                "name": "Channel", "url": "u",
            },
        }

        class Api(SyncMixin):
            def __init__(self):
                self._queues = state
                self._redwnl_lock = threading.Lock()
                self._redwnl_pending = [item]
                self.chan_redownload = mock.Mock(return_value={
                    "ok": False, "error": "folder unavailable",
                })

        api = Api()
        api._drain_pending_redownload_after_sync()
        self.assertEqual(api._redwnl_pending, [item])
        api.chan_redownload.assert_called_once()
        state.mark_orphan()

    def test_gpu_start_and_resume_fail_closed_when_journal_restore_fails(self):
        state = queues.QueueState()
        manager = mock.Mock()
        manager.request_drain.return_value = False

        class Api(QueueMixin):
            def __init__(self):
                self._queues = state
                self._transcribe = manager
                self.services = None
                self._sync_pause = threading.Event()
                self._on_queue_changed = mock.Mock()
                self._log_stream = mock.Mock()

        api = Api()
        with self._quiet(state):
            started = api.gpu_start()
            resumed = api.queue_resume("gpu")
        self.assertFalse(started["ok"])
        self.assertFalse(resumed["ok"])
        self.assertTrue(state.gpu_paused)
        self.assertEqual(manager.request_drain.call_count, 2)
        manager.pause.assert_called()
        state.mark_orphan()


if __name__ == "__main__":
    unittest.main()
