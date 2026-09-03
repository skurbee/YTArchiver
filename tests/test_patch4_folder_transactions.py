"""Focused tests for Patch 4 channel folder/config transactions."""

from __future__ import annotations

import atexit
import copy
import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

# Import backend modules only after redirecting all application state. These
# tests must never inspect or modify the signed-in user's real AppData archive.
_MODULE_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch4-appdata-")
atexit.register(_MODULE_APPDATA.cleanup)
os.environ["APPDATA"] = _MODULE_APPDATA.name
(Path(_MODULE_APPDATA.name) / "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import subs  # noqa: E402
from backend.services import channel_transactions as transactions  # noqa: E402
from backend.services import file_ops  # noqa: E402
from backend.services.channel_leases import (  # noqa: E402
    LeaseOwner,
    channel_aliases,
    channel_leases,
)


def _config_transaction(store: dict, *, fail_save: bool = False):
    """Return a transaction double with real copy-on-write behavior."""

    @contextmanager
    def transaction():
        working = copy.deepcopy(store)
        try:
            yield working
        except Exception:
            raise
        else:
            if fail_save:
                raise OSError("simulated config save failure")
            store.clear()
            store.update(copy.deepcopy(working))

    return transaction


class FolderTransactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="ytarchiver-patch4-")
        self.root = Path(self.temporary.name)
        self.archive = self.root / "archive"
        self.archive.mkdir()
        self.journal = self.root / "channel_folder_transaction.json"
        self.journal_patch = mock.patch.object(
            transactions,
            "CHANNEL_TRANSACTION_FILE",
            self.journal,
        )
        self.journal_patch.start()
        self.assertEqual(channel_leases.active_snapshot(), ())

    def tearDown(self) -> None:
        self.assertEqual(channel_leases.active_snapshot(), ())
        self.journal_patch.stop()
        self.temporary.cleanup()

    @staticmethod
    def _old_channel() -> dict:
        return {
            "name": "Old Name",
            "folder": "Old Name",
            "url": "https://www.youtube.com/@SameHandle",
            "resolution": "720",
            "last_sync": "old-sync-value",
        }

    def test_config_patch_preserves_unrelated_concurrent_fields(self) -> None:
        old_channel = self._old_channel()
        new_channel = {**old_channel, "name": "New Name", "folder": "New Name"}
        patch = transactions.build_channel_config_patch(old_channel, new_channel)
        live_channel = {
            **old_channel,
            "last_sync": "newer-worker-value",
            "worker_only_field": 17,
        }

        updated = transactions.apply_channel_config_patch(live_channel, patch)

        self.assertEqual(updated["name"], "New Name")
        self.assertEqual(updated["folder"], "New Name")
        self.assertEqual(updated["last_sync"], "newer-worker-value")
        self.assertEqual(updated["worker_only_field"], 17)

    def test_config_patch_rejects_a_third_value_for_an_edited_field(self) -> None:
        old_channel = self._old_channel()
        new_channel = {**old_channel, "name": "New Name", "folder": "New Name"}
        patch = transactions.build_channel_config_patch(old_channel, new_channel)
        conflicting = {**old_channel, "name": "Someone Else"}

        with self.assertRaises(transactions.ChannelTransactionConflict):
            transactions.apply_channel_config_patch(conflicting, patch)

    def test_journal_persists_prepared_and_folder_moved_checkpoints(self) -> None:
        old_channel = self._old_channel()
        new_channel = {**old_channel, "name": "New Name", "folder": "New Name"}
        record = transactions.make_rename_transaction(
            identity={"url": old_channel["url"]},
            old_channel=old_channel,
            new_channel=new_channel,
            old_path=str(self.archive / "Old Name"),
            new_path=str(self.archive / "New Name"),
        )

        self.assertTrue(transactions.write_channel_transaction(record))
        self.assertEqual(transactions.load_channel_transaction(strict=True)["state"], "prepared")
        self.assertTrue(
            transactions.checkpoint_channel_transaction(record, "folder_moved")
        )
        loaded = transactions.load_channel_transaction(strict=True)
        self.assertEqual(loaded["state"], "folder_moved")
        self.assertEqual(loaded["tx_id"], record["tx_id"])

    def test_recovery_uses_patch_not_unchanged_url_to_roll_back(self) -> None:
        old_channel = self._old_channel()
        new_channel = {**old_channel, "name": "New Name", "folder": "New Name"}
        old_path = self.archive / "Old Name"
        new_path = self.archive / "New Name"
        new_path.mkdir()
        record = transactions.make_rename_transaction(
            identity={"url": old_channel["url"]},
            old_channel=old_channel,
            new_channel=new_channel,
            old_path=str(old_path),
            new_path=str(new_path),
        )
        self.assertTrue(transactions.write_channel_transaction(record))
        self.assertTrue(
            transactions.checkpoint_channel_transaction(record, "folder_moved")
        )

        with mock.patch.object(
            transactions,
            "load_config",
            return_value={"channels": [old_channel]},
        ):
            recovered = transactions.recover_channel_transaction()

        self.assertTrue(recovered["ok"])
        self.assertEqual(recovered["action"], "rename-rolled-back")
        self.assertTrue(old_path.is_dir())
        self.assertFalse(new_path.exists())
        self.assertFalse(self.journal.exists())

    def test_recovery_keeps_new_folder_when_config_has_new_fields(self) -> None:
        old_channel = self._old_channel()
        new_channel = {**old_channel, "name": "New Name", "folder": "New Name"}
        old_path = self.archive / "Old Name"
        new_path = self.archive / "New Name"
        new_path.mkdir()
        record = transactions.make_rename_transaction(
            identity={"url": old_channel["url"]},
            old_channel=old_channel,
            new_channel=new_channel,
            old_path=str(old_path),
            new_path=str(new_path),
        )
        self.assertTrue(transactions.write_channel_transaction(record))
        self.assertTrue(
            transactions.checkpoint_channel_transaction(record, "folder_moved")
        )

        with mock.patch.object(
            transactions,
            "load_config",
            return_value={"channels": [new_channel]},
        ):
            recovered = transactions.recover_channel_transaction()

        self.assertTrue(recovered["ok"])
        self.assertEqual(recovered["action"], "rename-kept")
        self.assertFalse(old_path.exists())
        self.assertTrue(new_path.is_dir())
        self.assertFalse(self.journal.exists())

    def test_update_moves_folder_and_preserves_concurrent_config_changes(self) -> None:
        old_channel = self._old_channel()
        old_path = self.archive / "Old Name"
        new_path = self.archive / "New Name"
        old_path.mkdir()
        (old_path / "video.info.json").write_text("{}", encoding="utf-8")
        stale = {
            "output_dir": str(self.archive),
            "theme": "light",
            "channels": [copy.deepcopy(old_channel)],
        }
        persisted = {
            "output_dir": str(self.archive),
            "theme": "dark",
            "channels": [
                {
                    **old_channel,
                    "last_sync": "concurrent-sync",
                    "worker_only_field": True,
                }
            ],
        }

        with mock.patch.object(subs, "load_config", return_value=stale), mock.patch.object(
            subs,
            "config_transaction",
            _config_transaction(persisted),
        ):
            result = subs.update_channel(
                {"url": old_channel["url"]},
                {"name": "New Name"},
            )

        self.assertEqual(result["name"], "New Name")
        self.assertEqual(result["last_sync"], "concurrent-sync")
        self.assertTrue(result["worker_only_field"])
        self.assertEqual(persisted["theme"], "dark")
        self.assertEqual(persisted["channels"][0]["name"], "New Name")
        self.assertTrue((new_path / "video.info.json").is_file())
        self.assertFalse(old_path.exists())
        self.assertFalse(self.journal.exists())

    def test_update_config_save_failure_rolls_folder_back(self) -> None:
        old_channel = self._old_channel()
        old_path = self.archive / "Old Name"
        new_path = self.archive / "New Name"
        old_path.mkdir()
        persisted = {
            "output_dir": str(self.archive),
            "channels": [copy.deepcopy(old_channel)],
        }

        with mock.patch.object(
            subs,
            "load_config",
            return_value=copy.deepcopy(persisted),
        ), mock.patch.object(
            subs,
            "config_transaction",
            _config_transaction(persisted, fail_save=True),
        ):
            result = subs.update_channel(
                {"url": old_channel["url"]},
                {"name": "New Name"},
            )

        self.assertTrue(result["_write_blocked"])
        self.assertTrue(old_path.is_dir())
        self.assertFalse(new_path.exists())
        self.assertEqual(persisted["channels"][0]["name"], "Old Name")
        self.assertFalse(self.journal.exists())

    def test_update_rollback_failure_leaves_truthful_recovery_record(self) -> None:
        old_channel = self._old_channel()
        old_path = self.archive / "Old Name"
        new_path = self.archive / "New Name"
        old_path.mkdir()
        persisted = {
            "output_dir": str(self.archive),
            "channels": [copy.deepcopy(old_channel)],
        }
        real_rename = os.rename
        calls = 0

        def fail_second_rename(source, destination):
            nonlocal calls
            calls += 1
            if calls == 1:
                return real_rename(source, destination)
            raise OSError("simulated rollback failure")

        with mock.patch.object(
            subs,
            "load_config",
            return_value=copy.deepcopy(persisted),
        ), mock.patch.object(
            subs,
            "config_transaction",
            _config_transaction(persisted, fail_save=True),
        ), mock.patch.object(subs.os, "rename", side_effect=fail_second_rename):
            result = subs.update_channel(
                {"url": old_channel["url"]},
                {"name": "New Name"},
            )

        self.assertTrue(result["_recovery_required"])
        self.assertIn("rollback failed", result["_rollback_error"])
        self.assertFalse(old_path.exists())
        self.assertTrue(new_path.is_dir())
        record = transactions.load_channel_transaction(strict=True)
        self.assertEqual(record["state"], "recovery_required")
        self.assertEqual(record["failure_phase"], "rename_rollback")
        # Windows may expose the temporary root through an 8.3 alias while
        # ``Path.resolve()`` expands it to the long spelling.  The journal
        # deliberately preserves the path used by the file operation, so
        # compare the folders' resolved identities rather than their text.
        self.assertEqual(Path(record["old_path"]).resolve(), old_path.resolve())
        self.assertEqual(Path(record["new_path"]).resolve(), new_path.resolve())

    def test_update_returns_clear_busy_error_without_moving_folder(self) -> None:
        old_channel = self._old_channel()
        old_path = self.archive / "Old Name"
        new_path = self.archive / "New Name"
        old_path.mkdir()
        cfg = {"output_dir": str(self.archive), "channels": [old_channel]}
        owner = LeaseOwner("test-worker", "sync-1", label="Active sync")
        held = channel_leases.try_acquire(
            channel_aliases(old_channel, paths=(old_path, new_path)),
            owner,
        )
        self.assertTrue(held.ok)
        try:
            with mock.patch.object(subs, "load_config", return_value=cfg):
                with self.assertRaises(subs.SubsError) as caught:
                    subs.update_channel(
                        {"url": old_channel["url"]},
                        {"name": "New Name"},
                    )
            self.assertIn("Active sync", str(caught.exception))
            self.assertTrue(old_path.is_dir())
            self.assertFalse(new_path.exists())
        finally:
            held.lease.release()

    def test_remove_config_save_failure_restores_quarantined_folder(self) -> None:
        old_channel = self._old_channel()
        old_path = self.archive / "Old Name"
        trash_path = self.archive / ".YTArchiver Trash" / "Old Name-test"
        old_path.mkdir()
        persisted = {
            "output_dir": str(self.archive),
            "channels": [copy.deepcopy(old_channel)],
        }
        real_rename = os.rename

        def quarantine(folder_path, **_kwargs):
            trash_path.parent.mkdir(parents=True, exist_ok=True)
            real_rename(folder_path, trash_path)
            return {
                "ok": True,
                "deleted_folder": True,
                "folder_path": str(old_path),
                "trashed_folder_path": str(trash_path),
            }

        def restore(trashed_folder_path, **kwargs):
            self.assertEqual(trashed_folder_path, str(trash_path))
            self.assertTrue(kwargs.get("expected_transaction_id"))
            real_rename(trash_path, old_path)
            return {"ok": True}

        with mock.patch.object(
            subs,
            "load_config",
            return_value=copy.deepcopy(persisted),
        ), mock.patch.object(
            subs,
            "config_transaction",
            _config_transaction(persisted, fail_save=True),
        ), mock.patch(
            "backend.services.file_ops.safe_rmtree_channel_folder",
            side_effect=quarantine,
        ), mock.patch(
            "backend.services.file_ops.restore_trash_entry",
            side_effect=restore,
        ):
            result = subs.remove_channel(
                {"url": old_channel["url"]},
                delete_files=True,
            )

        self.assertFalse(result["ok"])
        self.assertIn("simulated config save failure", result["error"])
        self.assertTrue(old_path.is_dir())
        self.assertFalse(trash_path.exists())
        self.assertEqual(len(persisted["channels"]), 1)
        self.assertFalse(self.journal.exists())

    def test_remove_crash_after_move_before_checkpoint_recovers_exact_folder(
        self,
    ) -> None:
        """The prepared journal closes the move/checkpoint crash window."""

        class SimulatedProcessLoss(BaseException):
            pass

        old_channel = self._old_channel()
        old_path = self.archive / "Old Name"
        old_path.mkdir()
        (old_path / "video.info.json").write_text("{}", encoding="utf-8")
        persisted = {
            "output_dir": str(self.archive),
            "channels": [copy.deepcopy(old_channel)],
        }
        real_move = file_ops._move_no_replace

        def move_then_lose_process(source, destination):
            real_move(source, destination)
            raise SimulatedProcessLoss("after move, before checkpoint")

        with mock.patch.object(
            subs,
            "load_config",
            return_value=copy.deepcopy(persisted),
        ), mock.patch.object(
            subs,
            "config_transaction",
            _config_transaction(persisted),
        ), mock.patch.object(
            file_ops,
            "assert_within_managed_roots",
            return_value={"ok": True},
        ), mock.patch.object(
            file_ops,
            "_managed_root_for",
            return_value=str(self.archive),
        ), mock.patch.object(
            file_ops,
            "config_is_writable",
            return_value=True,
        ), mock.patch.object(
            file_ops,
            "_move_no_replace",
            side_effect=move_then_lose_process,
        ):
            with self.assertRaises(SimulatedProcessLoss):
                subs.remove_channel(
                    {"url": old_channel["url"]},
                    delete_files=True,
                )

        record = transactions.load_channel_transaction(strict=True)
        self.assertEqual(record["state"], "prepared")
        self.assertTrue(record["tx_id"])
        reserved_path = Path(record["trashed_folder_path"])
        self.assertTrue(reserved_path.is_dir())
        self.assertFalse(old_path.exists())
        manifest = json.loads(
            (reserved_path / ".ytarchiver-trash.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(manifest["transaction_id"], record["tx_id"])

        with mock.patch.object(
            transactions,
            "load_config",
            return_value=copy.deepcopy(persisted),
        ), mock.patch.object(
            file_ops,
            "_managed_root_for",
            return_value=str(self.archive),
        ), mock.patch.object(
            file_ops,
            "config_is_writable",
            return_value=True,
        ):
            recovered = transactions.recover_channel_transaction()

        self.assertTrue(recovered["ok"])
        self.assertEqual(recovered["action"], "remove-rolled-back")
        self.assertTrue((old_path / "video.info.json").is_file())
        self.assertFalse(reserved_path.exists())
        self.assertFalse(self.journal.exists())

    def test_corrupt_existing_journal_fails_startup_recovery_closed(self) -> None:
        self.journal.write_text("{not-json", encoding="utf-8")

        result = transactions.recover_channel_transaction()

        self.assertFalse(result["ok"])
        self.assertTrue(result["recovery_required"])
        self.assertTrue(self.journal.exists())


if __name__ == "__main__":
    unittest.main()
