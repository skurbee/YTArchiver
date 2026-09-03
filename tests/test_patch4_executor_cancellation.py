from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

from backend.executor_utils import drain_executor, run_bounded


class _SilentStream:
    def emit(self, _segments) -> None:
        return None


def test_bounded_pool_cancel_is_prompt_and_queued_work_never_starts() -> None:
    entered = threading.Event()
    release = threading.Event()
    worker_done = threading.Event()
    cancel = threading.Event()
    started: list[int] = []
    delivered: list[int] = []
    outcome = []

    def worker(item: int) -> int:
        started.append(item)
        entered.set()
        release.wait(5)
        worker_done.set()
        return item

    def run() -> None:
        outcome.append(run_bounded(
            range(20),
            worker,
            lambda result: delivered.append(result.item),
            max_workers=1,
            max_in_flight=8,
            thread_name_prefix="test-cancel-pool",
            is_cancelled=cancel.is_set,
        ))

    caller = threading.Thread(target=run)
    caller.start()
    assert entered.wait(1)

    started_at = time.monotonic()
    cancel.set()
    caller.join(0.75)
    elapsed = time.monotonic() - started_at

    assert not caller.is_alive()
    assert elapsed < 0.75
    assert started == [0]
    assert delivered == []
    assert outcome[0].cancelled is True
    assert outcome[0].unfinished == 1

    # A running call may finish cooperatively after the caller returns, but its
    # result must never cross the post-cancel durable-write callback boundary.
    release.set()
    assert worker_done.wait(1)
    time.sleep(0.05)
    assert started == [0]
    assert delivered == []


def test_duration_probe_cancel_does_not_wait_for_running_ffprobe(
        monkeypatch) -> None:
    from backend import index
    from backend.metadata import core

    entered = threading.Event()
    release = threading.Event()
    worker_done = threading.Event()
    cancel = threading.Event()
    started: list[str] = []
    returned: list[dict[str, float | None]] = []

    def slow_probe(filepath: str) -> float:
        started.append(filepath)
        entered.set()
        release.wait(5)
        worker_done.set()
        return 12.0

    monkeypatch.setattr(core, "_probe_file_duration", slow_probe)
    monkeypatch.setattr(index, "_open", lambda: None)

    caller = threading.Thread(
        target=lambda: returned.append(core._probe_durations_bulk(
            ["one.mp4", "two.mp4", "three.mp4"],
            _SilentStream(),
            cancel_event=cancel,
            max_workers=1,
        )),
    )
    caller.start()
    assert entered.wait(1)

    cancel.set()
    caller.join(0.75)
    assert not caller.is_alive()
    assert returned == [{}]
    assert started == ["one.mp4"]

    release.set()
    assert worker_done.wait(1)
    time.sleep(0.05)
    assert returned == [{}]
    assert started == ["one.mp4"]


def test_cooperative_drain_can_switch_from_success_wait_to_cancel() -> None:
    entered = threading.Event()
    release = threading.Event()
    cancel = threading.Event()
    started: list[int] = []
    outcome = []
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="test-drain")

    def worker(item: int) -> int:
        started.append(item)
        entered.set()
        release.wait(5)
        return item

    futures = [pool.submit(worker, item) for item in range(6)]
    assert entered.wait(1)
    caller = threading.Thread(target=lambda: outcome.append(drain_executor(
        pool,
        futures,
        is_cancelled=cancel.is_set,
    )))
    caller.start()
    cancel.set()
    caller.join(0.75)

    assert not caller.is_alive()
    assert outcome[0].cancelled is True
    assert outcome[0].unfinished == 1
    assert started == [0]
    assert all(future.cancelled() for future in futures[1:])

    release.set()
    assert futures[0].result(timeout=1) == 0


def test_cancelled_inline_metadata_rechecks_inside_jsonl_lock(
        monkeypatch, tmp_path) -> None:
    from backend.metadata import fetcher

    jsonl = tmp_path / ".Channel Metadata.jsonl"
    cancel = threading.Event()
    fetched = threading.Event()
    wrote = mock.Mock()
    thumbnail = mock.Mock(return_value=True)

    monkeypatch.setattr(fetcher, "find_yt_dlp", lambda: "yt-dlp")
    monkeypatch.setattr(
        fetcher, "_get_metadata_jsonl_path",
        lambda *_args, **_kwargs: (str(jsonl), str(tmp_path)),
    )
    monkeypatch.setattr(fetcher, "_read_metadata_jsonl", lambda *_a, **_k: {})

    def _fetched(*_args, **_kwargs):
        fetched.set()
        return {
            "video_id": "abcdefghijk",
            "title": "Title",
            "thumbnail_url": "https://example.invalid/thumb.jpg",
        }

    monkeypatch.setattr(fetcher, "_fetch_video_metadata", _fetched)
    monkeypatch.setattr(fetcher, "_write_metadata_jsonl", wrote)
    monkeypatch.setattr(fetcher, "_download_thumbnail", thumbnail)

    path_lock = fetcher._lock_for(str(jsonl))
    path_lock.acquire()
    result = []
    worker = threading.Thread(target=lambda: result.append(
        fetcher.fetch_single_video_metadata(
            {"name": "Channel"},
            "abcdefghijk",
            str(tmp_path / "video.mp4"),
            "Title",
            mock.Mock(),
            cancel_event=cancel,
            dest_folder=str(tmp_path),
        )
    ))
    try:
        worker.start()
        assert fetched.wait(1)
        time.sleep(0.03)
        cancel.set()
    finally:
        path_lock.release()
    worker.join(1)

    assert result == [{"ok": False, "cancelled": True}]
    wrote.assert_not_called()
    thumbnail.assert_not_called()
    assert not jsonl.exists()


def test_inline_metadata_worker_remains_owned_until_it_really_exits() -> None:
    from backend.executor_utils import LinkedCancelEvent
    from backend.sync import core

    release = threading.Event()
    pool = ThreadPoolExecutor(max_workers=1)
    future = pool.submit(release.wait, 2)
    token = LinkedCancelEvent()
    core._track_inline_metadata_worker(future, token, "sync-task-1")
    try:
        assert core.inline_metadata_workers_active()
        assert core.inline_metadata_workers_snapshot() == {
            "workers": 1,
            "task_ids": ["sync-task-1"],
        }
        assert core.inline_metadata_workers_cancel()
        assert token.is_set()
        assert not core.inline_metadata_workers_join(0.02)
        release.set()
        assert core.inline_metadata_workers_join(1)
        assert not core.inline_metadata_workers_active()
    finally:
        release.set()
        pool.shutdown(wait=True, cancel_futures=True)
