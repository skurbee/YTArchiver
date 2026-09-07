"""Adding a subscription must leave its immediate Sync now action runnable."""

from __future__ import annotations

import contextlib
import copy
import importlib
from unittest import mock

import pytest

from backend import channel_art, subs
from backend.api_mixins import channel_mixin
from backend.api_mixins.channel_mixin import ChannelMixin
from backend.api_mixins.subs_mixin import SubsMixin
from backend.queues import QueueState
from backend.services.channel_leases import channel_leases

sync_all = importlib.import_module("backend.sync.sync_all")


class _Api(SubsMixin, ChannelMixin):
    def __init__(self, store):
        self.store = store
        self._log_stream = mock.Mock()

    def _reload_config(self):
        self._config = copy.deepcopy(self.store)

    def _channel_folder_for_name(self, name):
        channel = next(ch for ch in self.store["channels"] if ch["name"] == name)
        return channel, str(self.store["output_dir"] + "/" + channel["folder"])


@pytest.fixture
def add_flow(tmp_path, monkeypatch):
    store = {"channels": [], "output_dir": str(tmp_path / "Archive")}

    @contextlib.contextmanager
    def transaction():
        working = copy.deepcopy(store)
        yield working
        store.clear()
        store.update(working)

    monkeypatch.setattr(subs, "load_config", lambda: copy.deepcopy(store))
    monkeypatch.setattr(subs, "config_transaction", transaction)
    scheduled_art = []
    # Hold any artwork worker after its real lease acquisition. This makes
    # the former add -> artwork -> busy sync race deterministic and offline.
    monkeypatch.setattr(
        channel_mixin, "start_managed_task",
        lambda _api, **kwargs: scheduled_art.append(kwargs["target"]),
    )
    fetch_art = mock.Mock()
    monkeypatch.setattr(channel_art, "fetch_channel_art", fetch_art)
    api = _Api(store)
    yield api, store, scheduled_art, fetch_art
    # A failing regression must release any held artwork lease too.
    for worker in scheduled_art:
        worker()


def test_add_then_sync_now_dispatches_without_an_artwork_conflict(
        add_flow, tmp_path, monkeypatch):
    api, store, scheduled_art, fetch_art = add_flow
    added = api.subs_add_channel({
        "name": "Fixture Channel",
        "url": "https://www.youtube.com/@SyncNowFixture",
    })
    assert added["ok"] is True
    assert added["write_blocked"] is False
    assert len(store["channels"]) == 1

    monkeypatch.setattr(sync_all, "load_config", lambda: copy.deepcopy(store))
    monkeypatch.setattr(sync_all, "ARCHIVE_FILE", str(tmp_path / "archive.txt"))
    monkeypatch.setattr(sync_all, "clear_sync_progress", mock.Mock())
    monkeypatch.setattr(sync_all, "fire_channel_synced_hook", mock.Mock())
    monkeypatch.setattr(
        sync_all.channel_identity, "preflight_channel_identity",
        lambda channel, **_kwargs: {"ok": True, "channel": channel},
    )
    observed = []

    def download(channel, *_args, **_kwargs):
        observed.append((channel, channel_leases.active_snapshot()))
        return {"ok": True, "downloaded": 1, "errors": 0, "total": 1}

    monkeypatch.setattr(sync_all, "sync_channel", download)
    queues = QueueState()
    monkeypatch.setattr(queues, "save_now", lambda: True)
    monkeypatch.setattr(queues, "save_debounced", lambda: None)
    monkeypatch.setattr(queues, "_write_resuming_payload", lambda *_a, **_k: True)
    stream = mock.Mock()
    try:
        assert queues.sync_enqueue(added["channel"])
        result = sync_all.sync_all(
            stream, queues=queues, add_downloads_from_config=False,
        )
        assert result["ok"] is True, result
        assert result["downloaded"] == 1
        assert result["busy"] is None
        assert len(observed) == 1
        channel, held_leases = observed[0]
        assert channel["url"] == added["channel"]["url"]
        assert len(held_leases) == 1
        assert held_leases[0].owner == "sync"
        assert queues.current_sync is None
        assert queues.sync_snapshot() == []
        assert channel_leases.active_snapshot() == ()
        assert scheduled_art == []
        fetch_art.assert_not_called()
        stream.emit_error.assert_not_called()
    finally:
        queues.mark_orphan()


def test_add_with_failed_config_commit_reports_write_blocked(add_flow, monkeypatch):
    api, store, scheduled_art, fetch_art = add_flow

    @contextlib.contextmanager
    def failed_transaction():
        yield copy.deepcopy(store)
        raise OSError("Fixture config commit failed")

    monkeypatch.setattr(subs, "config_transaction", failed_transaction)
    result = api.subs_add_channel({
        "name": "Fixture Channel",
        "url": "https://www.youtube.com/@SyncNowFixture",
    })

    assert result["ok"] is False
    assert result["write_blocked"] is True
    assert "could not be saved" in result["error"]
    assert store["channels"] == []
    assert scheduled_art == []
    fetch_art.assert_not_called()
    assert channel_leases.active_snapshot() == ()
