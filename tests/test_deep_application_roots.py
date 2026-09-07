"""Additional-root removal owns config and catalog effects, preserving media."""
from __future__ import annotations

import copy
import threading
from types import SimpleNamespace

import pytest

from backend import index
from backend import ytarchiver_config as config
from backend.api_mixins.index_mixin import IndexMixin
from backend.services.archive_roots import ArchiveRootCommands
from backend.services.config_repository import ConfigRepository
from backend.services.job_supervisor import JobSupervisor


@pytest.fixture
def roots(tmp_path):
    primary, extra, other = (tmp_path / name for name in ("primary", "extra", "other"))
    for path in (primary, extra, other):
        path.mkdir()
    seed = copy.deepcopy(config.DEFAULT_CONFIG)
    seed.update(output_dir=str(primary), tp_archive_roots=[str(extra), str(other)], marker="retain")
    assert config.save_config(seed)
    repository = ConfigRepository(config.load_config, config.save_config, config.update_config)
    return repository, primary, extra, other


def test_public_command_removes_only_selected_catalog_root_and_preserves_media(roots):
    repository, primary, extra, other = roots
    paths = [path / "video.mp4" for path in (primary, extra, other)]
    connection = index._open()
    assert connection is not None
    for number, path in enumerate(paths):
        path.write_bytes(b"original media")
        connection.execute("INSERT INTO videos(video_id, filepath, title, channel) VALUES (?,?,?,?)",
                           (str(number) * 11, str(path), "Example", str(number)))
    connection.commit()

    class Api(IndexMixin):
        services = SimpleNamespace(config_repository=repository)
        _job_supervisor = JobSupervisor()

        def _reload_config(self):
            self.refreshed = True

    api = Api()
    result = api.archive_root_remove(str(extra))
    assert result["ok"] and result["removed"] and result["videos"] == 1
    assert api.refreshed
    assert config.load_config()["tp_archive_roots"] == [str(other)]
    assert {row[0] for row in connection.execute("SELECT filepath FROM videos")} == {
        str(paths[0]), str(paths[2])}
    assert all(path.read_bytes() == b"original media" for path in paths)
    repeated = api.archive_root_remove(str(extra))
    assert repeated["ok"] and repeated["already_removed"]
    assert not repeated["removed"] and repeated["videos"] == 0


@pytest.mark.parametrize("which", ["primary", "child", "parent"])
def test_primary_overlap_never_reaches_catalog(roots, which):
    repository, primary, _extra, _other = roots
    selected = {"primary": primary, "child": primary / "child", "parent": primary.parent}[which]
    command = ArchiveRootCommands(repository, lambda _path: pytest.fail("must not delete"))
    assert "primary" in command.remove(str(selected))["error"]


def test_cleanup_failure_keeps_root_visible_and_configuration_unchanged(roots):
    repository, _primary, extra, _other = roots
    before = config.CONFIG_FILE.read_bytes()
    result = ArchiveRootCommands(repository, lambda _path: {
        "ok": False, "error": "catalog unavailable"}).remove(str(extra))
    assert not result["ok"] and result["retryable"] and result["config_restored"]
    assert config.CONFIG_FILE.read_bytes() == before


def test_config_save_failure_reports_partial_cleanup_and_allows_safe_retry(roots, monkeypatch):
    repository, _primary, extra, other = roots
    calls = []
    command = ArchiveRootCommands(repository, lambda path: calls.append(path) or {
        "ok": True, "videos": 2 if len(calls) == 1 else 0})
    with monkeypatch.context() as patch:
        patch.setattr(config, "save_config", lambda _value: False)
        result = command.remove(str(extra))
    assert not result["ok"] and result["catalog_cleaned"]
    assert result["retryable"] and result["config_restored"]
    assert config.load_config()["tp_archive_roots"] == [str(extra), str(other)]
    assert command.remove(str(extra))["removed"]
    assert config.load_config()["tp_archive_roots"] == [str(other)]


def test_final_patch_preserves_unrelated_changes_made_during_catalog_cleanup(roots):
    repository, _primary, extra, other = roots

    def cleanup(_path):
        assert str(extra) in config.load_config()["tp_archive_roots"]
        config.update_config(lambda live: live.update(marker="newer", tp_archive_roots=[
            str(extra), str(other), str(other / "new")]))
        return {"ok": True}

    assert ArchiveRootCommands(repository, cleanup).remove(str(extra))["removed"]
    final = config.load_config()
    assert final["marker"] == "newer"
    assert final["tp_archive_roots"] == [str(other), str(other / "new")]


def test_cancel_after_cleanup_does_not_hide_retryable_root(roots):
    repository, _primary, extra, _other = roots
    cancel = threading.Event()
    command = ArchiveRootCommands(repository, lambda _path: cancel.set() or {"ok": True})
    result = command.remove(str(extra), cancelled=cancel.is_set)
    assert result["cancelled"] and result["catalog_cleaned"]
    assert str(extra) in config.load_config()["tp_archive_roots"]


def test_restore_admission_rejects_public_command_before_any_effect(roots, monkeypatch):
    repository, _primary, extra, _other = roots
    api = IndexMixin()
    api.services = SimpleNamespace(config_repository=repository)
    api._job_supervisor = JobSupervisor()
    api._job_supervisor.close_admission("restore")
    monkeypatch.setattr(index, "delete_catalog_under_root", lambda _root: pytest.fail("must not delete"))
    result = api.archive_root_remove(str(extra))
    assert not result["ok"] and "restore" in result["error"]
    assert str(extra) in config.load_config()["tp_archive_roots"]
