"""Focused persistence-contract tests for the extracted queue repository."""

from __future__ import annotations

import json

from backend.services.queue_repository import QueueRepository


def test_repository_commits_and_loads_both_queue_generations(tmp_path):
    repository = QueueRepository(tmp_path / "queue.json")

    assert repository.commit_main({"sync": [{"task_id": "sync-one"}]}).ok
    assert repository.commit_resuming({
        "_seq": 1,
        "resuming": {"sync": {"task_id": "sync-running"}},
    }).ok

    assert repository.load_main().data["sync"][0]["task_id"] == "sync-one"
    assert repository.load_resuming().data["sync"]["task_id"] == "sync-running"
    assert not list(tmp_path.glob("*.tmp"))


def test_repository_preserves_corrupt_state_before_reporting_failure(tmp_path):
    queue_file = tmp_path / "queue.json"
    queue_file.write_text("{broken", encoding="utf-8")
    repository = QueueRepository(queue_file)

    result = repository.load_main()

    assert result.state == "sidelined"
    assert not queue_file.exists()
    assert (tmp_path / "queue.json.bak").read_text(encoding="utf-8") == "{broken"


def test_failed_commit_keeps_last_good_queue_and_removes_stage(
    tmp_path, monkeypatch,
):
    queue_file = tmp_path / "queue.json"
    repository = QueueRepository(queue_file)
    assert repository.commit_main({"sync": [{"task_id": "old"}]}).ok
    original = queue_file.read_bytes()

    def fail_replace(_source, _target):
        raise OSError("injected replace failure")

    monkeypatch.setattr(
        "backend.services.queue_repository.os.replace", fail_replace,
    )
    result = repository.commit_main({"sync": [{"task_id": "new"}]})

    assert not result.ok
    assert queue_file.read_bytes() == original
    assert json.loads(queue_file.read_text(encoding="utf-8"))["sync"][0][
        "task_id"
    ] == "old"
    assert not list(tmp_path.glob("*.tmp"))
