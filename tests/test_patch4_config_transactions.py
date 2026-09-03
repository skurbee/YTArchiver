import copy
import os
import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-patch4-config-")
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)


def test_stale_settings_snapshot_cannot_erase_new_window_state(monkeypatch):
    from backend import window_state
    from backend import ytarchiver_config as config
    from backend.api_mixins.settings_mixin import SettingsMixin

    seed = copy.deepcopy(config.DEFAULT_CONFIG)
    seed["log_mode"] = "Simple"
    seed["window_state"] = {"width": 900, "height": 700}
    assert config.save_config(seed)

    stale_original = config.load_config()
    stale_candidate = copy.deepcopy(stale_original)
    stale_candidate["log_mode"] = "Verbose"

    assert window_state.save_window_state({"width": 1234})
    mixin = SettingsMixin()
    ok, _committed = mixin._settings_commit_candidate(
        stale_original, stale_candidate)
    assert ok

    final = config.load_config()
    assert final["log_mode"] == "Verbose"
    assert final["window_state"]["width"] == 1234


def test_failed_config_transaction_leaves_original_bytes(monkeypatch):
    from backend import ytarchiver_config as config

    seed = copy.deepcopy(config.DEFAULT_CONFIG)
    seed["log_mode"] = "Simple"
    assert config.save_config(seed)
    before = config.CONFIG_FILE.read_bytes()

    def _fail(_cfg):
        _cfg["log_mode"] = "Verbose"
        raise RuntimeError("stop")

    try:
        config.update_config(_fail)
    except RuntimeError:
        pass
    else:
        raise AssertionError("transaction failure did not propagate")

    assert config.CONFIG_FILE.read_bytes() == before


def test_api_config_mutators_serialize_disjoint_concurrent_changes():
    from backend import ytarchiver_config as config
    from backend.api_mixins.info_mixin import InfoMixin
    from backend.api_mixins.onboarding_mixin import OnboardingMixin
    from backend.api_mixins.queue_mixin import QueueMixin
    from backend.services.app_services import AppServices

    seed = copy.deepcopy(config.DEFAULT_CONFIG)
    seed["url_history"] = ["existing-url"]
    seed["unrelated_marker"] = "preserve-me"
    assert config.save_config(seed)

    first_inside_transaction = threading.Event()
    release_first = threading.Event()
    second_attempting = threading.Event()
    second_done = threading.Event()

    def controlled_update(mutator):
        def _wrapped(cfg):
            result = mutator(cfg)
            if threading.current_thread().name == "queue-config-writer":
                first_inside_transaction.set()
                assert release_first.wait(2)
            return result

        return config.update_config(_wrapped)

    null_dependency = SimpleNamespace(
        _ensure_worker=lambda: None,
        emit_text=lambda *_args, **_kwargs: None,
        emit_dim=lambda *_args, **_kwargs: None,
    )

    class Api(QueueMixin, OnboardingMixin, InfoMixin):
        def __init__(self):
            self._config = None
            self._window = None
            self._reload_config = lambda: None
            self._on_queue_changed = lambda: None
            self.services = AppServices(
                load_config=config.load_config,
                save_config=config.save_config,
                update_config=controlled_update,
                queues=SimpleNamespace(),
                log_stream=null_dependency,
                transcribe=null_dependency,
                event_bus=SimpleNamespace(),
            )

    api = Api()
    results = {}

    def _queue_writer():
        results["queue"] = api.queue_auto_set("gpu", True)

    def _other_writers():
        second_attempting.set()
        results["traffic"] = api.onboarding_set_traffic(
            "custom", {"daily": 321, "hourly": 45})
        api._push_url_history("new-url")
        second_done.set()

    first = threading.Thread(
        target=_queue_writer, name="queue-config-writer")
    second = threading.Thread(
        target=_other_writers, name="other-config-writers")
    first.start()
    assert first_inside_transaction.wait(2)
    second.start()
    assert second_attempting.wait(2)
    assert not second_done.wait(0.05)

    release_first.set()
    first.join(2)
    second.join(2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert results["queue"]["ok"]
    assert results["traffic"]["ok"]
    final = config.load_config()
    assert final["autorun_gpu"] is True
    assert final["youtube_traffic_mode"] == "custom"
    assert final["youtube_traffic_custom_daily"] == 321
    assert final["youtube_traffic_custom_hourly"] == 45
    assert final["url_history"][:2] == ["new-url", "existing-url"]
    assert final["unrelated_marker"] == "preserve-me"


def test_video_stale_recent_cleanup_preserves_unrelated_config(monkeypatch):
    from backend import index as index_backend
    from backend import ytarchiver_config as config
    from backend.api_mixins.video_mixin import VideoMixin
    from backend.services.app_services import AppServices

    missing = r"X:\Archive\Missing Video.mp4"
    seed = copy.deepcopy(config.DEFAULT_CONFIG)
    seed["recent_downloads"] = [
        {"title": "Missing", "filepath": missing},
        {"title": "Keep", "filepath": r"X:\Archive\Keep.mp4"},
    ]
    seed["unrelated_marker"] = "preserve-me"
    assert config.save_config(seed)

    class Api(VideoMixin):
        def __init__(self):
            null = SimpleNamespace()
            self.services = AppServices(
                load_config=config.load_config,
                save_config=config.save_config,
                update_config=config.update_config,
                queues=null,
                log_stream=null,
                transcribe=null,
                event_bus=null,
            )

    monkeypatch.setattr(
        index_backend,
        "prepare_media_copy_deletion",
        lambda _path: {"ok": True},
    )
    monkeypatch.setattr(
        index_backend,
        "delete_media_copy",
        lambda _path: {"ok": True, "found": True},
    )
    monkeypatch.setattr(
        index_backend,
        "finalize_copy_deletion_preparation",
        lambda _prepared: {"ok": True},
    )
    monkeypatch.setattr(
        "backend.api_mixins.video_mixin.os.path.isfile",
        lambda _path: False,
    )

    result = Api().video_delete_file(missing)

    assert result["ok"]
    final = config.load_config()
    assert [row["title"] for row in final["recent_downloads"]] == ["Keep"]
    assert final["unrelated_marker"] == "preserve-me"


def test_restore_freeze_waits_for_an_admitted_config_writer(monkeypatch):
    from backend import ytarchiver_config as config

    seed = copy.deepcopy(config.DEFAULT_CONFIG)
    seed["log_mode"] = "Simple"
    assert config.save_config(seed)

    entered = threading.Event()
    release = threading.Event()
    frozen = threading.Event()
    original_write = config._save_config_locked

    def _blocked_write(candidate):
        entered.set()
        assert release.wait(2)
        return original_write(candidate)

    monkeypatch.setattr(config, "_save_config_locked", _blocked_write)
    candidate = copy.deepcopy(seed)
    candidate["log_mode"] = "Verbose"
    writer = threading.Thread(target=config.save_config, args=(candidate,))
    writer.start()
    assert entered.wait(2)

    freezer = threading.Thread(
        target=lambda: (
            config.suspend_config_writes("test restore"), frozen.set()
        )
    )
    freezer.start()
    time.sleep(0.05)
    assert not frozen.is_set()

    release.set()
    writer.join(2)
    freezer.join(2)
    assert frozen.is_set()
    committed = config.CONFIG_FILE.read_bytes()
    assert not config.save_config(seed)
    assert config.CONFIG_FILE.read_bytes() == committed
