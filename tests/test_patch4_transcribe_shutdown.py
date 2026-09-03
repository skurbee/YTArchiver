import json
import os
import tempfile
import threading
import time
from pathlib import Path
from unittest import mock

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch4-shutdown-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend.queues import QueueState  # noqa: E402
from backend.transcribe.core import (  # noqa: E402
    TranscribeManager,
    _pending_journal_path,
    _WorkerOutcome,
)
from backend.ytarchiver_config import QUEUE_FILE  # noqa: E402


def test_shutdown_defers_active_gpu_task_without_clearing_recovery(tmp_path):
    stream = mock.Mock()
    queues = QueueState()
    manager = TranscribeManager(stream, model="small")
    manager.attach_queues(queues)
    manager._flush_batch_stats = mock.Mock()

    media = tmp_path / "active.mp4"
    media.write_bytes(b"media")
    entered = threading.Event()

    def interrupted_compression(job):
        entered.set()
        assert job["cancel"].wait(2.0)
        return _WorkerOutcome.CANCELLED

    manager._compress_one = interrupted_compression
    with mock.patch("backend.utils.is_within_managed_roots", return_value=True):
        assert manager.compress_enqueue(
            str(media), "Active", channel="Channel")
    assert entered.wait(1.0)
    current_id = str((queues.current_gpu or {}).get("task_id") or "")
    assert current_id

    assert manager.begin_shutdown()
    assert manager.join_shutdown(2.0)

    pending_ids = {
        str(item.get("task_id") or "") for item in queues.gpu_snapshot()
    }
    assert current_id in pending_ids
    persisted = json.loads(QUEUE_FILE.read_text(encoding="utf-8"))
    assert current_id in {
        str(item.get("task_id") or "") for item in persisted["gpu"]
    }
    journal = json.loads(_pending_journal_path().read_text(encoding="utf-8"))
    assert current_id in {
        str(item.get("task_id") or "") for item in journal
    }
    assert not manager._cancel_all.is_set()
    assert manager.shutdown_snapshot()["accepting"] is False

    with mock.patch("backend.utils.is_within_managed_roots", return_value=True):
        assert not manager.compress_enqueue(str(media), "Rejected")
    queues.mark_orphan()


def test_forced_shutdown_remains_owner_scoped_and_bounded():
    manager = TranscribeManager(mock.Mock(), model="small")
    manager._shutdown_requested.set()
    manager._current_job = {
        "task_id": "gpu-active",
        "cancel": threading.Event(),
    }
    manager._proc = mock.Mock()
    manager._proc.poll.return_value = None
    manager._proc.stdin = None
    manager._punct = mock.Mock()
    manager._punct._proc = None

    started = time.monotonic()
    assert manager.force_shutdown()
    elapsed = time.monotonic() - started

    assert elapsed < 3.0
    assert manager._current_job["cancel"].is_set()
    assert manager._current_job["_shutdown_retry"] is True
