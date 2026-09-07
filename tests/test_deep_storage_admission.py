"""Durable admission and interrupted restore through real storage owners."""

import json
from pathlib import Path

import pytest

from backend import fs_safety, queues
from backend.services import file_ops
from backend.services.queue_repository import QueueRepository
from backend.trash_manager import TrashManager


@pytest.mark.parametrize("generation", ["main", "resuming"])
def test_unreadable_queue_blocks_all_saves_until_successful_reload(
        tmp_path, monkeypatch, generation):
    repository = QueueRepository(tmp_path / "queue.json")
    repository.commit_main({"_schema_version": 3, "sync": [
        {"task_id": "saved-work", "url": "https://example.com/channel"}]})
    repository.commit_resuming({"resuming": {}})
    protected = (repository.main_path if generation == "main"
                 else repository.resuming_path)
    before = {p: p.read_bytes() for p in
              (repository.main_path, repository.resuming_path)}
    original_read = Path.read_text

    def read(path, *args, **kwargs):
        if path == protected:
            raise PermissionError("temporarily unavailable")
        return original_read(path, *args, **kwargs)

    monkeypatch.setattr(queues, "config_is_writable", lambda: True)
    owner = queues.QueueState(repository)
    monkeypatch.setattr(Path, "read_text", read)
    assert owner.load() is False
    assert owner.hydration_status()["state"] == "blocked"
    monkeypatch.setattr(Path, "read_text", original_read)
    assert not owner.save_now()
    assert not owner._write_resuming_payload({"resuming": {}})
    owner._atexit_flush()
    assert {p: p.read_bytes() for p in before} == before
    assert not list(tmp_path.glob("*.bak"))
    assert owner.load()
    assert owner.hydration_status()["writable"]
    assert owner.sync[0]["task_id"] == "saved-work"
    assert owner.save_now()
    owner.mark_orphan()


def test_missing_queue_is_a_safe_first_run_baseline(tmp_path, monkeypatch):
    monkeypatch.setattr(queues, "config_is_writable", lambda: True)
    repository = QueueRepository(tmp_path / "queue.json")
    owner = queues.QueueState(repository)
    assert owner.load() is False
    assert owner.hydration_status()["writable"]
    assert owner.save_now()
    assert repository.load_main().state == "ok"
    owner.mark_orphan()


def test_queue_quarantine_retains_previous_evidence(tmp_path):
    repository = QueueRepository(tmp_path / "queue.json")
    for data in ("{first", "{second"):
        repository.main_path.write_text(data, encoding="utf-8")
        assert repository.load_main().state == "sidelined"
    assert {p.read_text() for p in tmp_path.glob("*.bak")} == {"{first", "{second"}


def test_invalid_text_is_quarantined_but_future_queue_is_preserved(tmp_path):
    repository = QueueRepository(tmp_path / "queue.json")
    repository.main_path.write_bytes(b"\xff\xfe invalid UTF-8")
    assert repository.load_main().state == "sidelined"
    future = b'{"_schema_version":99,"sync":[{"task_id":"future-work"}]}'
    repository.main_path.write_bytes(future)
    assert repository.load_main().state == "blocked"
    assert repository.main_path.read_bytes() == future


@pytest.mark.parametrize("failure", ["flush", "fsync", "close", "replace"])
def test_atomic_publication_rejects_staged_io_failure(tmp_path, monkeypatch, failure):
    path = tmp_path / "state.json"
    path.write_bytes(b"old authoritative bytes")
    original_fdopen = fs_safety.os.fdopen

    class FaultyStream:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.wrapped.close()
            if failure == "close":
                raise OSError("close failed")

        def flush(self):
            if failure == "flush":
                raise OSError("disk full")
            self.wrapped.flush()

    monkeypatch.setattr(fs_safety.os, "fdopen",
                        lambda *a, **kw: FaultyStream(original_fdopen(*a, **kw)))
    if failure in {"fsync", "replace"}:
        def fail(*_args):
            raise OSError(f"{failure} failed")
        monkeypatch.setattr(fs_safety.os, failure, fail)
    with pytest.raises(OSError):
        with fs_safety.atomic_write(path) as stream:
            stream.write("new bytes")
    assert path.read_bytes() == b"old authoritative bytes"
    assert not list(tmp_path.glob("*.tmp"))


def test_failed_cache_sync_preserves_disk_and_in_memory_ids(tmp_path, monkeypatch):
    from backend import channel_cache

    path = tmp_path / "channel-cache.json"
    saved = {"channel": {"last_refreshed": 1, "ids": ["saved-id"]}}
    path.write_text(json.dumps(saved), encoding="utf-8")
    before = path.read_bytes()
    monkeypatch.setattr(channel_cache, "CHANNEL_ID_CACHE", path)
    monkeypatch.setattr(channel_cache, "config_is_writable", lambda: True)
    monkeypatch.setattr(channel_cache, "_loaded", False)
    monkeypatch.setattr(channel_cache, "_cache", {})

    def fail(_fd):
        raise OSError("disk synchronization failed")

    monkeypatch.setattr(fs_safety.os, "fsync", fail)
    channel_cache.append_ids("channel", ["new-id"])
    assert channel_cache.get_known_ids("channel") == ["saved-id"]
    assert path.read_bytes() == before


def test_drift_repair_sync_failure_preserves_transcript(tmp_path, monkeypatch):
    from backend import drift_scan

    path = tmp_path / "Transcript.txt"
    path.write_bytes(b"Existing transcript\n")

    def fail(_fd):
        raise OSError("disk synchronization failed")

    monkeypatch.setattr(drift_scan.os, "fsync", fail)
    assert not drift_scan._write_transcript_entry_plain(
        str(path), "Recovered video", "01.01.2020", "1:00", "YT", "Recovered text")
    assert path.read_bytes() == b"Existing transcript\n"
    assert not Path(str(path) + ".tmp").exists()


@pytest.fixture
def trash_entry(tmp_path, monkeypatch):
    root = tmp_path / "Archive"
    root.mkdir()
    cfg = {"output_dir": str(root), "tp_archive_roots": [], "channels": []}
    monkeypatch.setattr("backend.ytarchiver_config.load_config", lambda: cfg)
    monkeypatch.setattr("backend.trash_manager.load_config", lambda: cfg)
    monkeypatch.setattr(file_ops, "config_is_writable", lambda: True)
    media = root / "Example.mp4"
    media.write_bytes(b"original media")
    result = file_ops.safe_trash_video_file(str(media), catalog_context={
        "title": "Original title", "video_id": "ABCDEFGHIJK"})
    assert result["ok"], result
    folder = Path(result["trashed_folder_path"])
    manifest_path = folder / ".ytarchiver-trash.json"
    yield media, folder, manifest_path, Path(result["trashed_file_path"])


def test_complete_missing_trash_rejects_replacement_without_phase_change(trash_entry):
    media, folder, manifest_path, source = trash_entry
    source.unlink()
    media.write_bytes(b"unrelated replacement")
    before = manifest_path.read_bytes()
    manager = TrashManager()
    entry = manager.list_entries()["entries"][0]
    assert not entry["can_restore"]
    assert not manager.restore(entry["entry_id"], entry["epoch"])["ok"]
    assert not file_ops.restore_trash_entry(str(folder))["ok"]
    assert manifest_path.read_bytes() == before
    assert media.read_bytes() == b"unrelated replacement"


@pytest.mark.parametrize("identity", ["exact", "replacement", "legacy"])
def test_interrupted_restore_requires_matching_receipt(trash_entry, identity):
    media, folder, manifest_path, source = trash_entry
    manifest = json.loads(manifest_path.read_text())
    manifest["state"] = "restoring"
    if identity == "legacy":
        manifest["files"][0].pop("restore_identity")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    source.rename(media)
    if identity == "replacement":
        media.unlink()
        media.write_bytes(b"wrong file")
    result = file_ops.restore_trash_entry(str(folder))
    assert result["ok"] is (identity == "exact"), result
    assert folder.exists() is (identity != "exact")


def test_restore_recovers_after_move_finishes_before_error(trash_entry, monkeypatch):
    media, folder, manifest_path, source = trash_entry
    move = file_ops._move_no_replace

    def interrupted_move(src, dest):
        move(src, dest)
        raise OSError("interrupted after durable move")

    monkeypatch.setattr(file_ops, "_move_no_replace", interrupted_move)
    first = file_ops.restore_trash_entry(str(folder))
    assert not first["ok"] and first["resumable"]
    assert not source.exists() and media.read_bytes() == b"original media"
    assert json.loads(manifest_path.read_text())["state"] == "restoring"
    monkeypatch.setattr(file_ops, "_move_no_replace", move)
    assert file_ops.restore_trash_entry(str(folder))["ok"]
    assert media.read_bytes() == b"original media" and not folder.exists()
