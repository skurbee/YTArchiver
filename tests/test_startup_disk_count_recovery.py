"""Execute the startup components through their normal dependency boundaries."""
import atexit
import os
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-startup-counts-")
atexit.register(_PROFILE.cleanup)
os.environ["APPDATA"] = str(Path(_PROFILE.name) / "roaming")
os.environ["LOCALAPPDATA"] = str(Path(_PROFILE.name) / "local")

from backend import archive_scan
from backend.services.config_repository import ConfigRepository
from backend.services.event_bus import BridgeEventBus
from backend.services.startup_scan import StartupDiskScan
from backend.services.startup_stages import run_startup_stages

ROOT = Path(__file__).resolve().parents[1]


def _record(count=3):
    return {"num_vids": count, "physical_copies": count, "size_bytes": 100,
            "last_updated": 100, "count_semantics_version": 2}


@pytest.fixture
def stage(monkeypatch):
    config = {"channels": [{"url": "one"}, {"url": "two"}],
              "last_disk_scan_ts": 9900, "disk_scan_staleness_hours": 24}
    cache = {"one": _record()}
    scanned = {"one": _record(4), "two": _record(5)}
    monkeypatch.setattr(archive_scan, "load_config", lambda: config)
    monkeypatch.setattr(archive_scan, "load_disk_cache", lambda: cache)
    heal = Mock(return_value=0)
    scan = Mock(return_value=scanned)
    publish = Mock(side_effect=lambda fresh: cache.update(fresh) or cache)
    monkeypatch.setattr(archive_scan, "heal_malformed_cache_entries", heal)
    monkeypatch.setattr(archive_scan, "scan_all_channels", scan)
    monkeypatch.setattr(archive_scan, "publish_scan_stats", publish)
    monkeypatch.setattr(archive_scan, "save_disk_cache",
                        Mock(side_effect=AssertionError("Must merge scan results")))
    update = Mock(side_effect=lambda mutation: (mutation(config), config))
    owner = SimpleNamespace(_config=config, _window=Mock())
    cancel = threading.Event()
    stream = Mock()
    busy = Mock(return_value=False)
    scanner = StartupDiskScan(
        ConfigRepository(lambda: config, lambda _cfg: True, update),
        stream, BridgeEventBus(lambda: owner._window),
        lambda snapshot: setattr(owner, "_config", snapshot),
        {"sweep": {}}, clock=lambda: 10000,
    )
    return SimpleNamespace(run=lambda: scanner.run(cancel, config, busy), config=config,
                           cache=cache, scanned=scanned, scan=scan, publish=publish,
                           update=update, cancel=cancel, stream=stream, heal=heal,
                           owner=owner, busy=busy)


def test_recent_partial_cache_is_repaired_and_health_refreshed(stage):
    stage.run()
    stage.scan.assert_called_once()
    stage.publish.assert_called_once_with(stage.scanned)
    stage.update.assert_called_once()
    assert stage.config["last_disk_scan_ts"] == 10000
    assert archive_scan.cache_coverage(stage.config["channels"])["complete"]
    script = stage.owner._window.evaluate_js.call_args.args[0]
    assert "refreshSubsTable" in script
    assert "_refreshIndexStats" in script
    assert "_refreshHealthOverview" in script
    stage.stream.emit_error.assert_not_called()


def test_startup_scan_uses_the_injected_config_for_scan_and_summary(stage, monkeypatch):
    monkeypatch.setattr(archive_scan, "load_config",
                        Mock(side_effect=AssertionError("use injected configuration")))
    stage.run()
    assert stage.scan.call_args.kwargs["cfg"] is stage.config
    stage.stream.emit_error.assert_not_called()


def test_recent_complete_cache_skips_unnecessary_walk(stage):
    stage.cache.update(stage.scanned)
    stage.run()
    stage.scan.assert_not_called()
    stage.publish.assert_not_called()
    stage.update.assert_not_called()


@pytest.mark.parametrize("reason", ["stale", "healed", "never_scanned"])
def test_complete_cache_still_obeys_existing_rescan_triggers(stage, reason):
    stage.cache.update(stage.scanned)
    if reason == "stale":
        stage.config["disk_scan_staleness_hours"] = 0
    elif reason == "healed":
        stage.heal.return_value = 1
    else:
        stage.config["last_disk_scan_ts"] = 0
    stage.run()
    stage.scan.assert_called_once()
    stage.update.assert_called_once()


@pytest.mark.parametrize("shutdown", [False, True])
def test_interrupted_scan_preserves_cache_and_timestamp(stage, shutdown):
    def interrupt(**kwargs):
        if shutdown:
            stage.cancel.set()
        else:
            stage.busy.return_value = True
        assert kwargs["stop_if"]()
        return None
    stage.scan.side_effect = interrupt
    stage.run()
    stage.publish.assert_not_called()
    stage.update.assert_not_called()
    assert stage.config["last_disk_scan_ts"] == 9900
    assert set(stage.cache) == {"one"}


def test_failed_publish_cannot_mark_scan_fresh(stage):
    stage.publish.side_effect = OSError("Simulated write failure")
    stage.run()
    stage.update.assert_not_called()
    assert stage.config["last_disk_scan_ts"] == 9900
    stage.stream.emit_error.assert_called_once()
    assert "Simulated write failure" in stage.stream.emit_error.call_args.args[0]


def test_subscription_added_during_scan_leaves_coverage_incomplete(stage):
    def scan(**_kwargs):
        stage.config["channels"].append({"url": "three"})
        return stage.scanned
    stage.scan.side_effect = scan
    stage.run()
    stage.publish.assert_called_once()
    stage.update.assert_not_called()
    assert any("incomplete" in str(call) for call in stage.stream.emit_dim.call_args_list)


@pytest.mark.parametrize("deferred,cancel_after_sweep,expected_calls", [
    (False, False, 1), (True, False, 2), (True, True, 1),
])
def test_startup_retries_a_deferred_count_once_after_sweep(
        deferred, cancel_after_sweep, expected_calls):
    cancel = threading.Event()
    finished = threading.Event()
    calls = []
    def disk_scan():
        assert not finished.is_set(), "Counting must retain its progress indicator"
        calls.append("scan")
        return False if deferred else None
    def sweep():
        calls.append("sweep")
        if cancel_after_sweep:
            cancel.set()
    owner = SimpleNamespace(_autorun=Mock(), _trash_retention=Mock())
    backfill = Mock()
    clear_loading = Mock()
    clear_indicator = Mock()
    run_startup_stages(
        cancel_event=cancel, finished=finished, disk_scan=disk_scan, sweep=sweep,
        backfill=backfill, clear_loading=clear_loading, clear_indicator=clear_indicator,
        ready_callbacks=(owner._autorun.notify_startup_ready,
                         owner._trash_retention.notify_startup_ready),
    )
    assert calls == ["scan", "sweep"] + (["scan"] if expected_calls == 2 else [])
    assert finished.is_set()
    clear_loading.assert_called_once()
    clear_indicator.assert_called_once()
    if cancel_after_sweep:
        backfill.assert_not_called()
        owner._autorun.notify_startup_ready.assert_not_called()
    else:
        backfill.assert_called_once()
        owner._autorun.notify_startup_ready.assert_called_once_with("indexing")
