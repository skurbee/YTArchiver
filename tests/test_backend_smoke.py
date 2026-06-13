from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from backend import (
    index,
    index_graph,
    local_fileserver,
    metadata_io,
    queues,
    utils,
    ytarchiver_config,
)
from backend.services import AppServices, BridgeEventBus, file_ops
from backend.sync import ytdlp_session
from backend.sync.options import (
    build_match_filter,
    build_output_template,
    normalize_channel_sync_options,
    normalized_date_after,
)
from backend.sync.ytdlp_events import (
    extract_video_id_from_line,
    is_verbose_chatter_line,
)
from backend.transcribe import transcribe_files


class LocalFileServerAllowlistTests(unittest.TestCase):
    def tearDown(self) -> None:
        local_fileserver.set_allowed_roots([])

    def test_allowlist_fails_closed_when_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "thumb.jpg")
            Path(path).write_bytes(b"image")

            local_fileserver.set_allowed_roots([])

            self.assertFalse(local_fileserver._is_under_allowed_root(path))

    def test_sibling_prefix_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = os.path.join(td, "Archive")
            sibling = os.path.join(td, "ArchiveBad")
            os.makedirs(root)
            os.makedirs(sibling)
            path = os.path.join(sibling, "leak.jpg")
            Path(path).write_bytes(b"image")

            local_fileserver.set_allowed_roots([root])

            self.assertFalse(local_fileserver._is_under_allowed_root(path))

    def test_file_under_allowed_root_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = os.path.join(td, "Archive")
            os.makedirs(root)
            path = os.path.join(root, "thumb.jpg")
            Path(path).write_bytes(b"image")

            local_fileserver.set_allowed_roots([root])

            self.assertTrue(local_fileserver._is_under_allowed_root(path))


class ManagedRootsTests(unittest.TestCase):
    def test_managed_roots_fail_closed_without_configured_roots(self) -> None:
        with mock.patch("backend.ytarchiver_config.load_config",
                        return_value={"output_dir": "", "tp_archive_roots": []}):
            self.assertFalse(utils.is_within_managed_roots(__file__))

    def test_managed_roots_reject_sibling_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = os.path.join(td, "Archive")
            sibling = os.path.join(td, "ArchiveBad")
            os.makedirs(root)
            os.makedirs(sibling)
            path = os.path.join(sibling, "video.mp4")
            Path(path).write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": root,
                                          "tp_archive_roots": []}):
                self.assertFalse(utils.is_within_managed_roots(path))

    def test_managed_roots_accept_child_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = os.path.join(td, "Archive")
            os.makedirs(root)
            path = os.path.join(root, "video.mp4")
            Path(path).write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": root,
                                          "tp_archive_roots": []}):
                self.assertTrue(utils.is_within_managed_roots(path))


class FileOpsTests(unittest.TestCase):
    def test_safe_remove_file_rejects_outside_archive_roots(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            outside = Path(td) / "Elsewhere" / "video.mp4"
            root.mkdir()
            outside.parent.mkdir()
            outside.write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}):
                result = file_ops.safe_remove_file(str(outside))

            self.assertFalse(result["ok"])
            self.assertTrue(outside.exists())

    def test_safe_remove_file_deletes_inside_archive_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            root.mkdir()
            video = root / "video.mp4"
            video.write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                result = file_ops.safe_remove_file(str(video))

            self.assertTrue(result["ok"])
            self.assertFalse(video.exists())

    def test_safe_remove_file_refuses_when_config_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            root.mkdir()
            video = root / "video.mp4"
            video.write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=False):
                result = file_ops.safe_remove_file(str(video))

            self.assertFalse(result["ok"])
            self.assertTrue(video.exists())

    def test_safe_remove_file_can_unhide_before_delete(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            root.mkdir()
            transcript = root / ".video.jsonl"
            transcript.write_text("{}", encoding="utf-8")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "unhide_file_win") as unhide:
                result = file_ops.safe_remove_file(
                    str(transcript),
                    require_config_writable=False,
                    reason="test",
                    unhide_first=True,
                )

            self.assertTrue(result["ok"])
            unhide.assert_called_once_with(str(transcript))
            self.assertFalse(transcript.exists())

    def test_safe_rmtree_channel_folder_deletes_inside_archive_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            folder = root / "Channel"
            folder.mkdir(parents=True)
            (folder / "video.mp4").write_bytes(b"video")

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                result = file_ops.safe_rmtree_channel_folder(str(folder))

            self.assertTrue(result["ok"])
            self.assertTrue(result["deleted_folder"])
            self.assertFalse(folder.exists())

    def test_safe_rmtree_channel_folder_rejects_outside_archive_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            folder = Path(td) / "Elsewhere" / "Channel"
            root.mkdir()
            folder.mkdir(parents=True)

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=True):
                result = file_ops.safe_rmtree_channel_folder(str(folder))

            self.assertFalse(result["ok"])
            self.assertTrue(folder.exists())

    def test_safe_rmtree_channel_folder_refuses_when_config_read_only(
            self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Archive"
            folder = root / "Channel"
            folder.mkdir(parents=True)

            with mock.patch("backend.ytarchiver_config.load_config",
                            return_value={"output_dir": str(root),
                                          "tp_archive_roots": []}), \
                    mock.patch.object(file_ops, "config_is_writable",
                                      return_value=False):
                result = file_ops.safe_rmtree_channel_folder(str(folder))

            self.assertFalse(result["ok"])
            self.assertTrue(folder.exists())


class MetadataJsonlTests(unittest.TestCase):
    def test_metadata_jsonl_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, ".Channel Metadata.jsonl")
            entries = {
                "a1": {"video_id": "a1", "title": "First"},
                "b2": {"video_id": "b2", "title": "Second"},
            }

            with mock.patch.object(metadata_io, "_hide_file_win"), \
                    mock.patch.object(metadata_io, "_unhide_file_win"):
                metadata_io._write_metadata_jsonl(path, entries)

            self.assertEqual(metadata_io._read_metadata_jsonl(path), entries)

    def test_metadata_jsonl_prefers_newer_duplicate_entry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, ".Channel Metadata.jsonl")
            rows = [
                {"video_id": "dup", "title": "Old",
                 "fetched_at": "2026-01-01T00:00:00Z"},
                {"video_id": "dup", "title": "New",
                 "fetched_at": "2026-02-01T00:00:00Z"},
            ]
            Path(path).write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8")

            loaded = metadata_io._read_metadata_jsonl(path)

            self.assertEqual(loaded["dup"]["title"], "New")


class SyncOptionsTests(unittest.TestCase):
    def test_sync_options_normalize_defaults(self) -> None:
        opts = normalize_channel_sync_options({
            "name": "Channel",
            "url": " https://example.test/channel ",
        })

        self.assertEqual(opts.name, "Channel")
        self.assertEqual(opts.url, "https://example.test/channel")
        self.assertEqual(opts.resolution, "720")
        self.assertFalse(opts.auto_transcribe)
        self.assertEqual(opts.mode, "new")
        self.assertFalse(opts.split_years)

    def test_sync_options_upgrade_legacy_durations(self) -> None:
        stream = mock.Mock()
        persisted = []
        channel = {
            "name": "Channel",
            "url": "https://example.test/channel",
            "min_duration": 30,
            "max_duration": 45,
            "mode": "FromDate",
            "from_date": "2026-01-01",
            "split_years": True,
            "split_months": True,
            "auto_transcribe": True,
        }

        opts = normalize_channel_sync_options(
            channel,
            stream=stream,
            persist_migration=lambda url, mn, mx: persisted.append(
                (url, mn, mx)),
        )

        self.assertEqual(opts.min_duration, 60)
        self.assertEqual(opts.max_duration, 60)
        self.assertTrue(opts.migrated_min_duration)
        self.assertTrue(opts.migrated_max_duration)
        self.assertEqual(channel["min_duration"], 60)
        self.assertEqual(channel["max_duration"], 60)
        self.assertEqual(persisted,
                         [("https://example.test/channel", True, True)])
        self.assertEqual(stream.emit_dim.call_count, 2)
        self.assertEqual(opts.mode, "fromdate")
        self.assertEqual(opts.from_date, "2026-01-01")
        self.assertTrue(opts.split_years)
        self.assertTrue(opts.split_months)
        self.assertTrue(opts.auto_transcribe)

    def test_sync_options_date_after_normalization(self) -> None:
        self.assertEqual(
            normalized_date_after("fromdate", "2026-06-12"),
            "20260612",
        )
        self.assertEqual(
            normalized_date_after("date", "2026/06/12 extra"),
            "20260612",
        )
        self.assertEqual(normalized_date_after("new", "2026-06-12"), "")
        self.assertEqual(normalized_date_after("fromdate", "bad-date"), "")

    def test_sync_options_match_filter(self) -> None:
        self.assertEqual(
            build_match_filter(0, 0),
            "!is_live & !is_upcoming",
        )
        self.assertEqual(
            build_match_filter(60, 3600),
            "!is_live & !is_upcoming & duration>?60 & duration<?3600",
        )

    def test_sync_options_output_template(self) -> None:
        root = Path("C:/Archive/Channel")
        self.assertEqual(
            build_output_template(root, False, False),
            str(root / "%(title)s.%(ext)s"),
        )
        self.assertEqual(
            build_output_template(root, True, False),
            str(root
                / "%(upload_date>%Y|Unknown Year)s"
                / "%(title)s.%(ext)s"),
        )
        self.assertEqual(
            build_output_template(root, True, True),
            str(root
                / "%(upload_date>%Y|Unknown Year)s"
                / "%(upload_date>%m %B|Unknown Month)s"
                / "%(title)s.%(ext)s"),
        )


class YtDlpEventsTests(unittest.TestCase):
    def test_extract_video_id_from_line_rejects_all_alpha_false_hit(self) -> None:
        self.assertEqual(
            extract_video_id_from_line("[download] Destination: file.mp4"),
            "",
        )

    def test_extract_video_id_from_line_accepts_realistic_id(self) -> None:
        self.assertEqual(
            extract_video_id_from_line("[youtube] Abc123_def4: Downloading"),
            "Abc123_def4",
        )

    def test_verbose_chatter_line_detection(self) -> None:
        self.assertTrue(is_verbose_chatter_line("  [Merger] Merging formats"))
        self.assertTrue(is_verbose_chatter_line("[info] Writing video metadata"))
        self.assertTrue(is_verbose_chatter_line("Deleting original file x"))
        self.assertFalse(is_verbose_chatter_line("[download] 42.0%"))


class YtDlpSessionTests(unittest.TestCase):
    def test_popen_ytdlp_process_registers_process(self) -> None:
        proc = mock.Mock()

        with mock.patch.object(ytdlp_session.subprocess, "Popen",
                               return_value=proc) as popen, \
                mock.patch.object(ytdlp_session.PROCESS_REGISTRY,
                                  "register") as register:
            result = ytdlp_session.popen_ytdlp_process(
                ["yt-dlp", "url"], startupinfo="startup")

        self.assertIs(result, proc)
        register.assert_called_once_with(proc)
        kwargs = popen.call_args.kwargs
        self.assertEqual(kwargs["stdin"], ytdlp_session.subprocess.DEVNULL)
        self.assertEqual(kwargs["stdout"], ytdlp_session.subprocess.PIPE)
        self.assertEqual(kwargs["stderr"], ytdlp_session.subprocess.STDOUT)
        self.assertEqual(kwargs["bufsize"], 0)
        self.assertEqual(kwargs["startupinfo"], "startup")

    def test_launch_reports_cancel_before_start(self) -> None:
        cancel_event = mock.Mock()
        cancel_event.is_set.return_value = True
        stream = mock.Mock()

        with mock.patch.object(ytdlp_session.subprocess, "Popen") as popen:
            result = ytdlp_session.launch_ytdlp_process(
                ["yt-dlp", "url"], stream, cancel_event=cancel_event)

        self.assertTrue(result.cancelled)
        popen.assert_not_called()

    def test_download_watchdog_kills_stalled_process(self) -> None:
        proc = mock.Mock()
        proc.poll.return_value = None
        stream = mock.Mock()

        handle = ytdlp_session.start_download_watchdog(
            proc, stream, kill_sec=-1, poll_interval=0.01)
        try:
            for _ in range(50):
                if proc.kill.called:
                    break
                ytdlp_session.time.sleep(0.01)
            self.assertTrue(proc.kill.called)
            self.assertTrue(handle.stalled["hit"])
            stream.emit.assert_called()
            stream.flush.assert_called()
        finally:
            handle.stop_event.set()

    def test_finish_ytdlp_process_closes_and_unregisters(self) -> None:
        proc = mock.Mock()
        proc.returncode = 0

        with mock.patch.object(ytdlp_session.PROCESS_REGISTRY,
                               "unregister") as unregister:
            rc = ytdlp_session.finish_ytdlp_process(proc)

        self.assertEqual(rc, 0)
        proc.wait.assert_called_once_with(timeout=10.0)
        proc.stdout.close.assert_called_once()
        unregister.assert_called_once_with(proc)

    def test_finish_ytdlp_process_escalates_after_timeout(self) -> None:
        proc = mock.Mock()
        proc.returncode = None
        proc.wait.side_effect = [
            ytdlp_session.subprocess.TimeoutExpired("yt-dlp", 10),
            ytdlp_session.subprocess.TimeoutExpired("yt-dlp", 5),
            None,
        ]

        ytdlp_session.finish_ytdlp_process(proc)

        proc.terminate.assert_called_once()
        proc.kill.assert_called_once()
        self.assertEqual(proc.wait.call_args_list[-1],
                         mock.call(timeout=2.0))


class ConfigTests(unittest.TestCase):
    def test_load_config_returns_defaults_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_file = Path(td) / "ytarchiver_config.json"

            with mock.patch.object(ytarchiver_config, "APP_DATA_DIR", Path(td)), \
                    mock.patch.object(ytarchiver_config, "CONFIG_FILE", cfg_file):
                cfg = ytarchiver_config.load_config()

            self.assertEqual(cfg["channels"], [])
            self.assertEqual(cfg["output_dir"], "")

    def test_config_transaction_persists_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_file = Path(td) / "ytarchiver_config.json"
            cfg_file.write_text(json.dumps({
                "_migration_v2_pending_tx_ids": True,
                "channels": [],
                "output_dir": "",
            }), encoding="utf-8")

            with mock.patch.object(ytarchiver_config, "APP_DATA_DIR", Path(td)), \
                    mock.patch.object(ytarchiver_config, "CONFIG_FILE", cfg_file):
                with ytarchiver_config.config_transaction() as cfg:
                    cfg["channels"].append({
                        "name": "Channel",
                        "url": "https://example.test/channel",
                    })

                loaded = ytarchiver_config.load_config()

            self.assertEqual(loaded["channels"][0]["name"], "Channel")


class AppServicesTests(unittest.TestCase):
    def test_fresh_config_uses_injected_loader(self) -> None:
        services = AppServices(
            load_config=lambda: {"output_dir": "X:/Archive"},
            save_config=lambda cfg: True,
            queues=mock.Mock(),
            log_stream=mock.Mock(),
            transcribe=mock.Mock(),
            event_bus=mock.Mock(),
        )

        self.assertEqual(services.fresh_config()["output_dir"], "X:/Archive")


class BridgeEventBusTests(unittest.TestCase):
    def test_js_value_escapes_script_close_sequence(self) -> None:
        self.assertEqual(
            BridgeEventBus.js_value({"html": "</script>"}),
            '{"html": "<\\/script>"}',
        )

    def test_update_queues_emits_render_and_state_calls(self) -> None:
        window = mock.Mock()
        bus = BridgeEventBus(lambda: window)

        self.assertTrue(bus.update_queues(
            {"sync": [], "gpu": []},
            {"sync": {"running": False}, "gpu": {"running": True}},
        ))

        script = window.evaluate_js.call_args.args[0]
        self.assertIn("window.renderQueues", script)
        self.assertIn("window.setQueueState", script)
        self.assertIn('"running": true', script)

    def test_show_toast_can_emit_object_payload(self) -> None:
        window = mock.Mock()
        bus = BridgeEventBus(lambda: window)

        self.assertTrue(bus.show_toast("Missing deps", "error", ttl_ms=12000))

        script = window.evaluate_js.call_args.args[0]
        self.assertIn("window._showToast", script)
        self.assertIn('"ttlMs": 12000', script)

    def test_show_toast_and_refresh_subs_emits_both_calls(self) -> None:
        window = mock.Mock()
        bus = BridgeEventBus(lambda: window)

        self.assertTrue(bus.show_toast_and_refresh_subs("Queued", "ok"))

        script = window.evaluate_js.call_args.args[0]
        self.assertIn("window._showToast", script)
        self.assertIn("window.refreshSubsTable", script)


class QueueStateTests(unittest.TestCase):
    def test_queue_save_load_and_resuming_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            queue_file = Path(td) / "queue.json"
            q1 = queues.QueueState()
            q2 = queues.QueueState()
            try:
                with mock.patch.object(queues, "QUEUE_FILE", queue_file), \
                        mock.patch.object(queues, "config_is_writable",
                                          return_value=True), \
                        mock.patch.object(queues.QueueState, "save_debounced",
                                          return_value=None):
                    self.assertTrue(q1.sync_enqueue(
                        {"name": "Channel", "url": "https://example.test/c"}))
                    self.assertTrue(q1.gpu_enqueue(
                        {"task_id": "gpu-1", "path": "video.mp4"}))
                    q1.current_sync = {"name": "Current",
                                       "url": "https://example.test/current"}
                    q1.current_gpu = {"task_id": "gpu-current",
                                      "path": "current.mp4"}

                    self.assertTrue(q1.save_now())
                    self.assertTrue(q2.load())

                self.assertEqual(q2.sync[0]["name"], "Channel")
                self.assertEqual(q2.gpu[0]["task_id"], "gpu-1")
                self.assertEqual(q2.get_loaded_resuming()["sync"]["name"],
                                 "Current")
                self.assertEqual(q2.get_loaded_resuming()["gpu"]["task_id"],
                                 "gpu-current")
            finally:
                q1.mark_orphan()
                q2.mark_orphan()

    def test_corrupt_queue_file_is_sidelined(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            queue_file = Path(td) / "queue.json"
            queue_file.write_text("{not json", encoding="utf-8")
            q = queues.QueueState()
            try:
                with mock.patch.object(queues, "QUEUE_FILE", queue_file):
                    self.assertFalse(q.load())
                    self.assertFalse(queue_file.exists())
                    self.assertTrue(Path(str(queue_file) + ".bak").exists())
            finally:
                q.mark_orphan()


class IndexGraphShapeTests(unittest.TestCase):
    def test_graph_word_frequency_multi_merges_labels_stably(self) -> None:
        fake = {
            "alpha": {"labels": ["2026-01", "2026-03"], "values": [2, 5]},
            "beta": {"labels": ["2026-02", "2026-03"], "values": [7, 11]},
        }

        def _fake_graph(word: str, channel=None, bucket: str = "month"):
            return fake[word]

        with mock.patch.object(index_graph, "graph_word_frequency",
                               side_effect=_fake_graph):
            out = index_graph.graph_word_frequency_multi(
                ["alpha", "beta"], channel="Any", bucket="month")

        self.assertEqual(out["labels"], ["2026-01", "2026-02", "2026-03"])
        self.assertEqual(out["series"][0],
                         {"word": "alpha", "values": [2, 0, 5]})
        self.assertEqual(out["series"][1],
                         {"word": "beta", "values": [0, 7, 11]})


class IndexIngestTests(unittest.TestCase):
    def tearDown(self) -> None:
        self._reset_index_module()

    @staticmethod
    def _reset_index_module() -> None:
        try:
            index._shutdown_index()
        finally:
            index._conn = None
            index._reader_conn = None
            index._schema_inited = False
            index._ingest_locks.clear()

    def test_ingest_reingest_and_delete_keep_fts_in_sync(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "transcription_index.db"
            video_dir = Path(td) / "Channel" / "2026" / "01 January"
            video_dir.mkdir(parents=True)
            video_path = video_dir / "Video [abc123_def4].mp4"
            jsonl_path = video_dir / "Video [abc123_def4].jsonl"
            video_path.write_bytes(b"video")
            jsonl_path.write_text(
                json.dumps({"video_id": "abc123_def4", "title": "Video",
                            "start": 0, "end": 1,
                            "text": "hello archive"}) + "\n" +
                json.dumps({"video_id": "abc123_def4", "title": "Video",
                            "start": 1, "end": 2,
                            "text": ""}) + "\n",
                encoding="utf-8")

            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                self._reset_index_module()
                self.assertTrue(index.register_video(
                    str(video_path), "Channel", "Video",
                    video_id="abc123_def4"))

                inserted = index.ingest_jsonl(
                    str(video_path), str(jsonl_path), "Video", "Channel")

                conn = index._open()
                self.assertIsNotNone(conn)
                assert conn is not None
                self.assertEqual(inserted, 1)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments").fetchone()[0], 1)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments_fts "
                    "WHERE segments_fts MATCH 'hello'").fetchone()[0], 1)

                jsonl_path.write_text(
                    json.dumps({"video_id": "abc123_def4", "title": "Video",
                                "start": 2, "end": 3,
                                "text": "fresh transcript"}) + "\n",
                    encoding="utf-8")
                self.assertEqual(index.ingest_jsonl(
                    str(video_path), str(jsonl_path), "Video", "Channel"), 1)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments_fts "
                    "WHERE segments_fts MATCH 'hello'").fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments_fts "
                    "WHERE segments_fts MATCH 'fresh'").fetchone()[0], 1)

                self.assertEqual(index.delete_segments_for_video(str(video_path)), 1)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments").fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments_fts "
                    "WHERE segments_fts MATCH 'fresh'").fetchone()[0], 0)
                self._reset_index_module()


class NoSpeechStatusTests(unittest.TestCase):
    """Q2: tx_status='no_speech' persistence + per-video lookup + counts.
    Q1: archive-wide transcribed/total %% (no_speech NOT counted)."""

    def tearDown(self) -> None:
        IndexIngestTests._reset_index_module()

    def test_no_speech_persist_lookup_counts_and_archive_pct(self) -> None:
        from backend import archive_scan
        # ignore_cleanup_errors: on Windows the index's reader connection can
        # still hold the .db open at tempdir teardown; the connection is
        # closed by _reset_index_module, but GC timing makes rmtree flaky.
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
            db_path = Path(td) / "transcription_index.db"
            chan_dir = Path(td) / "Chan"
            chan_dir.mkdir(parents=True)
            # Real YouTube ids are 11 chars; the id extractor rejects
            # shorter/alpha-only hints, so use valid 11-char ids (also
            # embedded in the filename as a belt-and-suspenders).
            ids = {"trans": "vTrans00001", "silent": "vSilent0001",
                   "pending": "vPend000001"}
            paths = {}
            for key, vid in ids.items():
                p = chan_dir / f"{key} [{vid}].mp4"
                p.write_bytes(b"v")
                paths[key] = str(p)

            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path), \
                    mock.patch.object(ytarchiver_config, "TRANSCRIPTION_DB",
                                      db_path):
                IndexIngestTests._reset_index_module()
                for key, vid in ids.items():
                    self.assertTrue(index.register_video(
                        paths[key], "Chan", key, video_id=vid))

                # One transcribed, one no_speech; 'pending' stays pending.
                self.assertTrue(index.mark_video_transcribed(paths["trans"]))
                self.assertTrue(index.mark_video_no_speech(paths["silent"]))

                # Per-video lookup distinguishes the states.
                self.assertEqual(
                    index.video_tx_status(video_id=ids["silent"]), "no_speech")
                self.assertEqual(
                    index.video_tx_status(video_id=ids["trans"]), "transcribed")
                self.assertIn(
                    index.video_tx_status(video_id=ids["pending"]),
                    ("pending", ""))

                # channel_transcription_stats: no_speech is its own bucket —
                # NOT counted as transcribed, NOT as pending.
                st = index.channel_transcription_stats("Chan")
                self.assertEqual(st["total"], 3)
                self.assertEqual(st["transcribed"], 1)
                self.assertEqual(st["no_speech"], 1)
                self.assertEqual(st["pending"], 1)

                # Q1: archive-wide coverage = transcribed / total. The silent
                # video is in the denominator but NOT the numerator, so a
                # fully-checked archive with a silent video reads below 100%.
                stats = archive_scan.index_db_stats()
                self.assertEqual(stats["total_videos"], 3)
                self.assertEqual(stats["transcribed_videos"], 1)
                IndexIngestTests._reset_index_module()


class DeleteChannelFromIndexTests(unittest.TestCase):
    """Deleting a channel must purge its videos + segments from the index
    (FTS-safe) so Browse/Search/Videos don't show ghost cards that 404."""

    def tearDown(self) -> None:
        IndexIngestTests._reset_index_module()

    def test_delete_channel_purges_only_that_channel(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
            db_path = Path(td) / "transcription_index.db"

            def _mk(chan: str, vid: str) -> tuple[str, str]:
                d = Path(td) / chan / "2026"
                d.mkdir(parents=True, exist_ok=True)
                mp4 = d / f"V [{vid}].mp4"
                mp4.write_bytes(b"v")
                jl = d / f"V [{vid}].jsonl"
                jl.write_text(json.dumps({
                    "video_id": vid, "title": "V " + vid,
                    "start": 0, "end": 1, "text": "hello " + chan}) + "\n",
                    encoding="utf-8")
                return str(mp4), str(jl)

            with mock.patch.object(index, "TRANSCRIPTION_DB", db_path):
                IndexIngestTests._reset_index_module()
                mp4a, jla = _mk("Doomed", "vDoomed0001")
                mp4b, jlb = _mk("Keepme", "vKeep000001")
                self.assertTrue(index.register_video(
                    mp4a, "Doomed", "V", video_id="vDoomed0001"))
                self.assertTrue(index.register_video(
                    mp4b, "Keepme", "V", video_id="vKeep000001"))
                index.ingest_jsonl(mp4a, jla, "V", "Doomed")
                index.ingest_jsonl(mp4b, jlb, "V", "Keepme")

                conn = index._open()
                assert conn is not None
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM videos").fetchone()[0], 2)

                res = index.delete_channel_from_index("Doomed")
                self.assertGreaterEqual(res["videos"], 1)

                # Doomed gone; Keepme untouched.
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM videos WHERE channel='Doomed'"
                ).fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM videos WHERE channel='Keepme'"
                ).fetchone()[0], 1)
                # Segments + FTS shadow purged for Doomed, intact for Keepme.
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments "
                    "WHERE video_id='vDoomed0001'").fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments_fts "
                    "WHERE segments_fts MATCH 'Doomed'").fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM segments_fts "
                    "WHERE segments_fts MATCH 'Keepme'").fetchone()[0], 1)
                IndexIngestTests._reset_index_module()


class TranscriptFileTests(unittest.TestCase):
    def test_replace_jsonl_entry_preserves_same_title_different_video_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, ".Channel Transcript.jsonl")
            original_rows = [
                {"video_id": "keep-id", "title": "Q&A",
                 "start": 0, "end": 1, "text": "keep me"},
                {"video_id": "replace-id", "title": "Q&A",
                 "start": 1, "end": 2, "text": "replace me"},
            ]
            Path(path).write_text(
                "".join(json.dumps(row) + "\n" for row in original_rows),
                encoding="utf-8")

            with mock.patch.object(transcribe_files, "_hide_file_win"), \
                    mock.patch.object(transcribe_files, "_unhide_file_win"):
                removed = transcribe_files._replace_jsonl_entry(
                    path,
                    title="Q&A",
                    video_id="replace-id",
                    new_segments=[{"start": 2, "end": 3,
                                   "text": "new text", "words": []}],
                )

            rows = [
                json.loads(line)
                for line in Path(path).read_text(encoding="utf-8").splitlines()
            ]

            self.assertEqual(removed, {"Q&A"})
            self.assertEqual([row["video_id"] for row in rows],
                             ["keep-id", "replace-id"])
            self.assertEqual(rows[0]["text"], "keep me")
            self.assertEqual(rows[1]["text"], "new text")

    def test_replace_txt_entry_preserves_surrounding_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "Channel Transcript.txt")
            Path(path).write_text(
                "===(First), (01.01.2026), (0:00:01), (OLD)===\n"
                "first body\n\n\n"
                "===(Target), (01.02.2026), (0:00:02), (OLD)===\n"
                "old target\n\n\n"
                "===(Last), (01.03.2026), (0:00:03), (OLD)===\n"
                "last body\n\n\n",
                encoding="utf-8")

            ok = transcribe_files._replace_txt_entry(
                path, "Target", "new target", "WHISPER:test")

            content = Path(path).read_text(encoding="utf-8")
            self.assertTrue(ok)
            self.assertIn("first body", content)
            self.assertIn("last body", content)
            self.assertIn("new target", content)
            self.assertNotIn("old target", content)
            self.assertIn("===(Target), (01.02.2026), (0:00:02), "
                          "(WHISPER:test)===", content)


if __name__ == "__main__":
    unittest.main()
