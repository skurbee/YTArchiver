"""Focused regression coverage for the user-facing Trash manager."""

from __future__ import annotations

import contextlib
import datetime
import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend import subs  # noqa: E402
from backend.api_mixins.trash_mixin import TrashMixin  # noqa: E402
from backend.services import file_ops  # noqa: E402
from backend.services.channel_leases import (  # noqa: E402
    LeaseOwner,
    channel_leases,
    global_archive_aliases,
)
from backend.services.job_supervisor import JobSupervisor  # noqa: E402
from backend.trash_manager import TrashManager, purge_expired  # noqa: E402


class TrashManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="yta-trash-manager-")
        self.root = Path(self.temp.name) / "Archive"
        self.root.mkdir()
        self.cfg = {
            "output_dir": str(self.root),
            "video_out_dir": "",
            "tp_archive_roots": [],
            "channels": [],
            "trash_retention_days": 30,
        }
        self.manager = TrashManager()
        self.patches = [
            mock.patch("backend.ytarchiver_config.load_config",
                       side_effect=lambda: self.cfg),
            mock.patch("backend.trash_manager.load_config",
                       side_effect=lambda: self.cfg),
            mock.patch.object(file_ops, "config_is_writable", return_value=True),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _channel_entry(self):
        snapshot = {
            "name": "Test Channel",
            "folder": "Test Channel",
            "folder_override": "",
            "url": "https://www.youtube.com/@testchannel",
            "resolution": "1080",
            "min_duration": 180,
            "future_setting": {"preserve": True},
        }
        folder = self.root / "Test Channel"
        folder.mkdir()
        (folder / "Video [ABCDEFGHIJK].mp4").write_bytes(b"video")
        trashed = file_ops.safe_rmtree_channel_folder(
            str(folder), channel_snapshot=snapshot)
        self.assertTrue(trashed["ok"], trashed.get("error"))
        listing = self.manager.list_entries(self.cfg)
        return snapshot, folder, listing["entries"][0], Path(
            trashed["trashed_folder_path"])

    def test_v2_channel_manifest_has_stable_identity_snapshot_and_hidden_root(self):
        with mock.patch.object(file_ops, "hide_file_win") as hide:
            snapshot, _folder, entry, trash_path = self._channel_entry()
        manifest = json.loads((trash_path / ".ytarchiver-trash.json").read_text(
            encoding="utf-8"))
        self.assertEqual(manifest["version"], 2)
        self.assertRegex(manifest["entry_id"], r"^[0-9a-f]{32}$")
        self.assertGreater(manifest["epoch"], 0)
        self.assertEqual(manifest["channel_snapshot"], snapshot)
        self.assertEqual(entry["entry_id"], manifest["entry_id"])
        self.assertEqual(entry["restore_scope"], "full")
        hide.assert_any_call(str(self.root / ".YTArchiver Trash"))

    def test_list_includes_summary_size_and_untracked_counts(self):
        _snapshot, _folder, entry, _trash_path = self._channel_entry()
        listing = self.manager.list_entries(self.cfg)
        self.assertEqual(listing["item_count"], 1)
        self.assertEqual(listing["file_count"], 1)
        self.assertEqual(listing["untracked_count"], 0)
        self.assertEqual(listing["retention_days"], 30)
        self.assertEqual(entry["size_bytes"], len(b"video"))
        self.assertNotIn("_entry_path", entry)

    def test_sidebar_summary_does_not_walk_large_trash_trees(self):
        self._channel_entry()
        with mock.patch(
            "backend.trash_manager.os.walk",
            side_effect=AssertionError("summary must stay shallow"),
        ):
            summary = self.manager.summary(self.cfg)
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["item_count"], 1)
        self.assertEqual(summary["retention_days"], 30)

    def test_full_channel_restore_readds_subscription_and_target_reindexes(self):
        snapshot, folder, entry, trash_path = self._channel_entry()
        self.cfg["channels"] = [snapshot]
        with mock.patch(
            "backend.trash_manager.subs.restore_channel_snapshot",
            return_value={"ok": True, "added": True, "channel": snapshot},
        ) as restore_subscription, mock.patch(
            "backend.trash_manager.index_maintenance.restore_channel_catalog",
            return_value={"ok": True, "registered": 1, "ingested": 0},
        ) as reindex:
            result = self.manager.restore(entry["entry_id"])
        self.assertTrue(result["ok"], result.get("error"))
        self.assertTrue(result["subscription_restored"])
        self.assertTrue(result["catalog_restored"])
        self.assertTrue(folder.is_dir())
        self.assertFalse(trash_path.exists())
        restore_subscription.assert_called_once_with(snapshot)
        reindex.assert_called_once()
        self.assertEqual(Path(reindex.call_args.args[1]), folder)

    def test_channel_restore_rolls_back_added_subscription_when_move_fails(self):
        snapshot, _folder, entry, _trash_path = self._channel_entry()
        with mock.patch(
            "backend.trash_manager.subs.restore_channel_snapshot",
            return_value={"ok": True, "added": True, "channel": snapshot},
        ), mock.patch(
            "backend.trash_manager.file_ops.restore_trash_entry",
            return_value={"ok": False, "error": "destination blocked"},
        ), mock.patch(
            "backend.trash_manager.subs.rollback_restored_channel_snapshot",
            return_value={"ok": True, "removed": True},
        ) as rollback:
            result = self.manager.restore(entry["entry_id"])
        self.assertFalse(result["ok"])
        rollback.assert_called_once_with(snapshot)

    def test_video_restore_uses_saved_catalog_context(self):
        video = self.root / "Video [ABCDEFGHIJK].mp4"
        video.write_bytes(b"video")
        context = {
            "filepath": str(video),
            "video_id": "ABCDEFGHIJK",
            "channel": "Test Channel",
            "title": "Video",
        }
        file_ops.safe_trash_video_file(str(video), catalog_context=context)
        listing = self.manager.list_entries(self.cfg)
        entry = listing["entries"][0]
        with mock.patch(
            "backend.trash_manager.index_backend.register_video",
            return_value=True,
        ) as register:
            result = self.manager.restore(entry["entry_id"])
        self.assertTrue(result["ok"], result.get("error"))
        self.assertTrue(result["catalog_restored"])
        register.assert_called_once_with(
            str(video), "Test Channel", "Video", video_id="ABCDEFGHIJK")

    def test_untracked_video_entry_cannot_be_purged(self):
        video = self.root / "Video [ABCDEFGHIJK].mp4"
        video.write_bytes(b"video")
        trashed = file_ops.safe_trash_video_file(str(video))
        trash_path = Path(trashed["trashed_folder_path"])
        (trash_path / "unexpected.bin").write_bytes(b"keep")
        entry = self.manager.list_entries(self.cfg)["entries"][0]
        self.assertFalse(entry["can_purge"])
        self.assertEqual(entry["untracked_count"], 1)
        result = self.manager.purge(entry["entry_id"])
        self.assertFalse(result["ok"])
        self.assertTrue(trash_path.exists())

    def test_cancelled_purge_never_stages_or_deletes_the_entry(self):
        video = self.root / "Video [ABCDEFGHIJK].mp4"
        video.write_bytes(b"video")
        trashed = file_ops.safe_trash_video_file(str(video))
        trash_path = Path(trashed["trashed_folder_path"])
        entry = self.manager.list_entries(self.cfg)["entries"][0]
        cancel = threading.Event()
        real_write = file_ops._write_json_atomic

        def cancel_after_marker(path, value):
            result = real_write(path, value)
            cancel.set()
            return result

        with mock.patch.object(
            file_ops, "_write_json_atomic", side_effect=cancel_after_marker,
        ):
            result = self.manager.purge(
                entry["entry_id"], cancel_event=cancel)

        self.assertFalse(result["ok"])
        self.assertTrue(result["cancelled"])
        self.assertTrue(trash_path.is_dir())
        self.assertEqual(self.manager.list_entries(self.cfg)["item_count"], 1)

    def test_auto_retention_requires_explicit_complete_state(self):
        video = self.root / "Video [ABCDEFGHIJK].mp4"
        video.write_bytes(b"video")
        trashed = file_ops.safe_trash_video_file(str(video))
        trash_path = Path(trashed["trashed_folder_path"])
        manifest_path = trash_path / ".ytarchiver-trash.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["epoch"] = 1
        manifest.pop("state")
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        result = purge_expired(
            config=self.cfg,
            retention_days=1,
            grace_until_ts=0,
            cancel_event=threading.Event(),
            now=200000,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["purged"], 0)
        self.assertTrue(trash_path.exists())

    def test_trash_root_link_or_junction_is_rejected(self):
        trash_root = self.root / ".YTArchiver Trash"
        trash_root.mkdir()
        with mock.patch("backend.services.file_ops.os.path.isjunction",
                        return_value=True):
            with self.assertRaises(OSError):
                file_ops.ensure_trash_root(str(self.root))
            self.assertFalse(file_ops._is_within_trash_root(
                str(trash_root / "entry"), str(self.root)))

    def test_manager_leaves_a_junctioned_trash_root_untouched(self):
        _snapshot, _folder, entry, trash_path = self._channel_entry()
        trash_root = self.root / ".YTArchiver Trash"

        def is_junction(path):
            return Path(path) == trash_root

        with mock.patch(
            "backend.trash_manager.os.path.isjunction",
            side_effect=is_junction,
        ), mock.patch("backend.trash_manager.os.rename") as rename, mock.patch(
            "backend.trash_manager.file_ops._write_json_atomic",
        ) as write_marker:
            listing = self.manager.list_entries(self.cfg)
            result = self.manager.purge(entry["entry_id"])

        self.assertEqual(listing["item_count"], 0)
        self.assertIn("junction", listing["roots"][0]["error"])
        self.assertFalse(result["ok"])
        self.assertTrue(trash_path.exists())
        rename.assert_not_called()
        write_marker.assert_not_called()

    def test_purge_recovery_junction_is_never_written(self):
        video = self.root / "Video [ABCDEFGHIJK].mp4"
        video.write_bytes(b"video")
        trashed = file_ops.safe_trash_video_file(str(video))
        trash_path = Path(trashed["trashed_folder_path"])
        recovery_dir = self.root / ".YTArchiver Trash" / (
            ".ytarchiver-purge-recovery")
        recovery_dir.mkdir()
        entry = self.manager.list_entries(self.cfg)["entries"][0]

        def is_junction(path):
            return Path(path) == recovery_dir

        with mock.patch(
            "backend.trash_manager.os.path.isjunction",
            side_effect=is_junction,
        ), mock.patch("backend.trash_manager.os.rename") as rename, mock.patch(
            "backend.trash_manager.file_ops._write_json_atomic",
        ) as write_marker:
            result = self.manager.purge(entry["entry_id"])

        self.assertFalse(result["ok"])
        self.assertIn("recovery", result["error"].lower())
        self.assertTrue(trash_path.exists())
        rename.assert_not_called()
        write_marker.assert_not_called()

    def test_restore_recovery_junction_is_never_written(self):
        video = self.root / "Video [ABCDEFGHIJK].mp4"
        video.write_bytes(b"video")
        trashed = file_ops.safe_trash_video_file(str(video))
        trash_path = Path(trashed["trashed_folder_path"])
        recovery_dir = self.root / ".YTArchiver Trash" / (
            file_ops._RESTORE_RECOVERY_DIR)
        recovery_dir.mkdir()

        def is_junction(path):
            return Path(path) == recovery_dir

        with mock.patch(
            "backend.services.file_ops.os.path.isjunction",
            side_effect=is_junction,
        ):
            result = file_ops.restore_trash_entry(
                str(trash_path), archive_root=str(self.root))

        self.assertTrue(result["ok"])
        self.assertIn("not safely contained", result["cleanup_warning"])
        self.assertTrue(video.exists())
        self.assertTrue(trash_path.exists())
        self.assertFalse(any(recovery_dir.iterdir()))

    def test_unreadable_channel_journal_blocks_permanent_delete(self):
        _snapshot, _folder, entry, trash_path = self._channel_entry()
        with mock.patch(
            "backend.services.channel_transactions.load_channel_transaction",
            side_effect=RuntimeError("journal unreadable"),
        ):
            result = self.manager.purge(entry["entry_id"])
        self.assertFalse(result["ok"])
        self.assertIn("could not be verified", result["error"])
        self.assertTrue(trash_path.exists())

    def test_partial_staged_purge_remains_retryable_without_inside_manifest(self):
        video = self.root / "Video [ABCDEFGHIJK].mp4"
        video.write_bytes(b"video")
        file_ops.safe_trash_video_file(str(video))
        entry = self.manager.list_entries(self.cfg)["entries"][0]

        def partial_failure(staged, **_kwargs):
            Path(staged, ".ytarchiver-trash.json").unlink()
            return {"ok": False, "error": "locked media file"}

        with mock.patch(
                "backend.trash_manager.file_ops.purge_trash_entry",
                side_effect=partial_failure):
            failed = self.manager.purge(entry["entry_id"])
        self.assertFalse(failed["ok"])
        self.assertTrue(failed["retryable"])

        listing = self.manager.list_entries(self.cfg)
        self.assertEqual(listing["item_count"], 1)
        retry = listing["entries"][0]
        self.assertEqual(retry["entry_id"], entry["entry_id"])
        self.assertTrue(retry["can_purge"])
        completed = self.manager.purge(retry["entry_id"])
        self.assertTrue(completed["ok"], completed.get("error"))

    def test_interrupted_video_restore_can_resume(self):
        video = self.root / "Video [ABCDEFGHIJK].mp4"
        video.write_bytes(b"video")
        trashed = file_ops.safe_trash_video_file(str(video))
        trash_path = Path(trashed["trashed_folder_path"])
        manifest_path = trash_path / ".ytarchiver-trash.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["state"] = "restoring"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        source = Path(manifest["files"][0]["trashed_path"])
        source.rename(video)

        entry = self.manager.list_entries(self.cfg)["entries"][0]
        self.assertEqual(entry["state"], "restoring")
        self.assertTrue(entry["can_restore"])
        result = self.manager.restore(entry["entry_id"])
        self.assertTrue(result["ok"], result.get("error"))
        self.assertTrue(video.exists())
        self.assertFalse(trash_path.exists())

    def test_retention_stops_when_policy_changes_mid_batch(self):
        old = datetime.datetime(2025, 1, 1, 12, 0, 0)
        for suffix in ("A", "B"):
            video = self.root / f"Video {suffix} [ABCDEFGHIJ{suffix}].mp4"
            video.write_bytes(suffix.encode("ascii"))
            trashed = file_ops.safe_trash_video_file(str(video))
            manifest_path = Path(
                trashed["trashed_folder_path"]) / ".ytarchiver-trash.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["trashed_at"] = old.isoformat(timespec="seconds")
            manifest["epoch"] = int(old.timestamp())
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        active = dict(self.cfg)
        active["trash_retention_grace_until_ts"] = 0.0
        disabled = dict(active)
        disabled["trash_retention_days"] = 0
        reads = iter((active, disabled))
        with mock.patch("backend.trash_manager.load_config",
                        side_effect=lambda: next(reads)):
            result = self.manager.purge_expired(
                active,
                now=datetime.datetime(2026, 1, 1, 12, 0, 0).timestamp(),
                grace_until=0,
                cancel_event=threading.Event(),
                retention_days=30,
            )
        self.assertTrue(result["ok"])
        self.assertFalse(result["enabled"])
        self.assertEqual(result["purged"], 1)
        self.assertEqual(self.manager.list_entries(self.cfg)["item_count"], 1)

    def test_retention_rechecks_policy_at_the_staged_rename_boundary(self):
        old = datetime.datetime(2025, 1, 1, 12, 0, 0)
        video = self.root / "Video [ABCDEFGHIJK].mp4"
        video.write_bytes(b"video")
        trashed = file_ops.safe_trash_video_file(str(video))
        trash_path = Path(trashed["trashed_folder_path"])
        manifest_path = trash_path / ".ytarchiver-trash.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["trashed_at"] = old.isoformat(timespec="seconds")
        manifest["epoch"] = int(old.timestamp())
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        active = dict(self.cfg, trash_retention_grace_until_ts=0.0)
        disabled = dict(active, trash_retention_days=0)

        @contextlib.contextmanager
        def disabled_at_commit():
            yield disabled

        with mock.patch(
            "backend.trash_manager.locked_config_snapshot",
            disabled_at_commit,
        ):
            result = self.manager.purge_expired(
                active,
                now=datetime.datetime(2026, 1, 1, 12, 0, 0).timestamp(),
                grace_until=0,
                cancel_event=threading.Event(),
                retention_days=30,
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["policy_changed"])
        self.assertEqual(result["purged"], 0)
        self.assertTrue(trash_path.exists())

    def test_retention_skips_mismatched_or_invalid_timestamps(self):
        video = self.root / "Video [ABCDEFGHIJK].mp4"
        video.write_bytes(b"video")
        trashed = file_ops.safe_trash_video_file(str(video))
        trash_path = Path(trashed["trashed_folder_path"])
        manifest_path = trash_path / ".ytarchiver-trash.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["trashed_at"] = "nan"
        manifest["epoch"] = 1
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        result = self.manager.purge_expired(
            self.cfg, now=2_000_000_000, grace_until=0,
            cancel_event=threading.Event(), retention_days=1)
        self.assertTrue(result["ok"])
        self.assertEqual(result["purged"], 0)
        self.assertTrue(trash_path.exists())

    def test_untracked_expired_entry_is_a_daily_warning_not_retry_storm(self):
        old = datetime.datetime(2025, 1, 1, 12, 0, 0)
        video = self.root / "Video [ABCDEFGHIJK].mp4"
        video.write_bytes(b"video")
        trashed = file_ops.safe_trash_video_file(str(video))
        trash_path = Path(trashed["trashed_folder_path"])
        (trash_path / "unexpected.bin").write_bytes(b"keep")
        manifest_path = trash_path / ".ytarchiver-trash.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["trashed_at"] = old.isoformat(timespec="seconds")
        manifest["epoch"] = int(old.timestamp())
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = self.manager.purge_expired(
            dict(self.cfg, trash_retention_grace_until_ts=0.0),
            now=datetime.datetime(2026, 1, 1, 12, 0, 0).timestamp(),
            grace_until=0,
            cancel_event=threading.Event(),
            retention_days=30,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["purged"], 0)
        self.assertEqual(len(result["warnings"]), 1)
        self.assertTrue(trash_path.exists())


class SubscriptionSnapshotTests(unittest.TestCase):
    def test_restore_preserves_unknown_fields_without_ui_conversion(self):
        cfg = {"channels": []}

        @contextlib.contextmanager
        def transaction():
            yield cfg

        snapshot = {
            "name": "Exact",
            "folder": "Exact",
            "url": "https://www.youtube.com/@exact",
            "min_duration": 181,
            "future_setting": ["unchanged"],
        }
        with mock.patch.object(subs, "config_transaction", transaction):
            result = subs.restore_channel_snapshot(snapshot)
        self.assertTrue(result["ok"])
        self.assertEqual(cfg["channels"][0], snapshot)


class TrashMixinContractTests(unittest.TestCase):
    def test_restore_accepts_opaque_id_without_client_path_or_epoch(self):
        api = TrashMixin()
        api._sync_mutation_lock = threading.RLock()
        with mock.patch(
            "backend.api_mixins.trash_mixin.trash_manager.restore",
            return_value={"ok": True, "entry_type": "video"},
        ) as restore, mock.patch(
            "backend.api_mixins.trash_mixin.trash_manager.summary",
            return_value={"ok": True, "item_count": 0},
        ):
            result = api.trash_restore({"id": "opaque-id"})
        self.assertTrue(result["ok"])
        restore.assert_called_once_with(
            "opaque-id", None, cancel_event=mock.ANY)
        self.assertIsInstance(
            restore.call_args.kwargs["cancel_event"], threading.Event)

    def test_mutations_honor_the_bridge_admission_check(self):
        api = TrashMixin()
        blocked = {
            "ok": False,
            "started": False,
            "error": "Cannot start work: shutdown is in progress",
        }
        api._work_admission_error = mock.Mock(return_value=blocked)

        with mock.patch(
            "backend.api_mixins.trash_mixin.trash_manager.restore",
        ) as restore:
            self.assertIs(api.trash_restore({"id": "opaque-id"}), blocked)
            restore.assert_not_called()
        with mock.patch(
            "backend.api_mixins.trash_mixin.trash_manager.purge",
        ) as purge:
            self.assertIs(api.trash_purge({"id": "opaque-id"}), blocked)
            purge.assert_not_called()
        with mock.patch(
            "backend.api_mixins.trash_mixin.trash_manager.empty",
        ) as empty_trash:
            self.assertIs(api.trash_empty(), blocked)
            empty_trash.assert_not_called()

        self.assertEqual(api._work_admission_error.call_count, 3)

    def test_restore_refuses_to_race_active_archive_work(self):
        api = TrashMixin()
        held = channel_leases.try_acquire(
            global_archive_aliases(),
            LeaseOwner("sync", "active-sync", label="Active sync"),
        )
        self.assertTrue(held.ok)
        self.assertIsNotNone(held.lease)
        try:
            with mock.patch(
                "backend.api_mixins.trash_mixin.trash_manager.restore",
            ) as restore:
                result = api.trash_restore({"id": "opaque-id"})
            self.assertFalse(result["ok"])
            self.assertTrue(result["busy"])
            restore.assert_not_called()
        finally:
            held.lease.release()

    def test_atomic_admission_scope_blocks_a_shutdown_race(self):
        api = TrashMixin()
        api._job_supervisor = JobSupervisor()
        api._job_supervisor.close_admission("application shutdown")
        with mock.patch(
            "backend.api_mixins.trash_mixin.trash_manager.purge",
        ) as purge:
            result = api.trash_purge({"id": "opaque-id"})
        self.assertFalse(result["ok"])
        self.assertFalse(result["started"])
        self.assertIn("shutdown", result["error"])
        purge.assert_not_called()

    def test_purge_and_empty_are_registered_and_receive_cancellation(self):
        api = TrashMixin()
        api._job_supervisor = JobSupervisor()
        seen_cancels = []

        def purge_side_effect(_entry_id, _epoch, *, cancel_event):
            seen_cancels.append(cancel_event)
            owners = api._job_supervisor.snapshot()["owners"]
            self.assertTrue(any(
                row.get("dynamic") and row.get("owner") == "trash"
                for row in owners
            ))
            return {"ok": False, "error": "test stop"}

        def empty_side_effect(_root_id, *, cancel_event):
            seen_cancels.append(cancel_event)
            owners = api._job_supervisor.snapshot()["owners"]
            self.assertTrue(any(
                row.get("dynamic") and row.get("owner") == "trash"
                for row in owners
            ))
            return {"ok": False, "error": "test stop"}

        with mock.patch(
            "backend.api_mixins.trash_mixin.trash_manager.purge",
            side_effect=purge_side_effect,
        ):
            api.trash_purge({"id": "opaque-id"})
        with mock.patch(
            "backend.api_mixins.trash_mixin.trash_manager.empty",
            side_effect=empty_side_effect,
        ), mock.patch(
            "backend.api_mixins.trash_mixin.trash_manager.summary",
            return_value={"ok": True, "item_count": 0},
        ):
            api.trash_empty({"root_id": "root-id"})

        self.assertEqual(len(seen_cancels), 2)
        self.assertTrue(all(
            isinstance(cancel, threading.Event) for cancel in seen_cancels
        ))
        self.assertFalse(any(
            row.get("dynamic") for row in api._job_supervisor.snapshot()["owners"]
        ))


if __name__ == "__main__":
    unittest.main()
