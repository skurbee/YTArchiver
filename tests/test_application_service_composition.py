"""Application components can be exercised without creating a desktop runtime."""

from __future__ import annotations

import copy
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from backend.api_mixins.info_mixin import InfoMixin
from backend.services.app_services import AppServices
from backend.services.application_information import ApplicationInformation
from backend.services.composition import compose_application_services
from backend.services.config_repository import ConfigRepository

ROOT = Path(__file__).resolve().parents[1]


def test_importing_desktop_entry_does_not_acquire_focus_or_rewrite_html(tmp_path):
    script = """
import os
from pathlib import Path
from unittest.mock import patch
before = os.environ.get("WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS")
html = Path("web/index.html").read_bytes()
with patch("ctypes.windll.kernel32.CreateMutexW", side_effect=AssertionError("mutex")), \\
     patch("ctypes.windll.user32.SetForegroundWindow", side_effect=AssertionError("focus")), \\
     patch("ctypes.windll.user32.MessageBoxW", side_effect=AssertionError("dialog")), \\
     patch("backend.html_assembler.assemble_index_html", side_effect=AssertionError("HTML write")):
    import main
    assert callable(main.Api)
assert os.environ.get("WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS") == before
assert Path("web/index.html").read_bytes() == html
"""
    profile = tmp_path / "profile"
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env={
            **os.environ,
            "APPDATA": str(profile / "roaming"),
            "LOCALAPPDATA": str(profile / "local"),
            "YTARCHIVER_BOOT_TRACE": "1",
        },
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert not list(profile.rglob("boot_trace.log"))


def test_native_launch_prepares_prerequisites_only_when_called(monkeypatch, tmp_path):
    from backend import desktop_startup, html_assembler

    calls = []
    fake_webview = SimpleNamespace()
    monkeypatch.setitem(sys.modules, "webview", fake_webview)
    monkeypatch.setattr(
        desktop_startup, "configure_browser_environment", lambda: calls.append("browser")
    )
    monkeypatch.setattr(desktop_startup, "ensure_single_instance", lambda: calls.append("instance"))
    monkeypatch.setattr(
        html_assembler, "assemble_index_html", lambda root: calls.append(("html", root))
    )
    assert desktop_startup.prepare_desktop(tmp_path) is fake_webview
    assert calls == ["browser", "instance", ("html", tmp_path)]


def test_information_service_commits_through_injected_repository():
    state = {"url_history": ["second", "first"], "unrelated": 12}

    def save(value):
        state.update(copy.deepcopy(value))
        return True

    repository = ConfigRepository(lambda: copy.deepcopy(state), save)
    info = ApplicationInformation(repository, "fixture-config", Mock(), lambda: True)
    info.push_url("first")
    assert state == {"url_history": ["first", "second"], "unrelated": 12}
    state["output_dir"] = "new-root"
    assert info.about(lambda: {"ok": True, "version": "fixture"})["output_dir"] == "new-root"


def test_real_composition_reaches_information_through_public_api():
    from main import Api

    state = {"output_dir": "fixture-archive", "channels": [], "url_history": []}
    repository = ConfigRepository(
        lambda: copy.deepcopy(state), lambda cfg: state.update(cfg) or True
    )
    # Use the production composition root and bridge class without native
    # lifecycle admission. Only information's collaborators are needed here.
    api = Api.__new__(Api)
    api.services = compose_application_services(
        config=repository,
        config_path="fixture-config",
        can_write=lambda: True,
        queues=Mock(),
        log_stream=Mock(),
        transcribe=Mock(),
        event_bus=Mock(),
    )
    api._config = copy.deepcopy(state)
    api.ytdlp_version = lambda: {"ok": True, "version": "fixture"}
    assert api.get_runtime_info()["onboarded"] is True
    api._push_url_history("fixture-url")
    assert api.url_history() == ["fixture-url"]
    assert api.about_info()["output_dir"] == "fixture-archive"


def test_information_service_retains_failed_write_and_reports_it():
    log = Mock()
    original = {"url_history": ["old"]}
    repository = ConfigRepository(lambda: copy.deepcopy(original), lambda _cfg: False)
    info = ApplicationInformation(repository, "fixture-config", log, lambda: True)
    info.push_url("new")
    assert original == {"url_history": ["old"]}
    log.emit_dim.assert_called_once()


def test_existing_profile_is_not_treated_as_onboarding_without_subscriptions():
    info = ApplicationInformation(
        ConfigRepository(dict, lambda _cfg: True), "fixture-config", Mock(), lambda: True
    )
    snapshot = {"output_dir": "archive", "channels": []}
    result = info.runtime_info(snapshot)
    assert result["onboarded"] is True
    assert result["has_config_file"] is True
    assert info.runtime_info(None)["has_config_file"] is False


def test_information_bridge_uses_the_composed_service_without_fallback_loading():
    info = Mock(spec=ApplicationInformation)
    services = AppServices(
        load_config=Mock(side_effect=AssertionError("unexpected config read")),
        save_config=Mock(),
        queues=Mock(),
        log_stream=Mock(),
        transcribe=Mock(),
        event_bus=Mock(),
        information=info,
    )

    class Adapter(InfoMixin):
        def ytdlp_version(self):
            return {"ok": True, "version": "fixture"}

    adapter = Adapter()
    adapter.services = services
    adapter._config = {"output_dir": "archive"}
    adapter.get_runtime_info()
    adapter.about_info()
    adapter.url_history()
    adapter._push_url_history("url")
    adapter.get_last_sync_label()
    info.runtime_info.assert_called_once_with(adapter._config)
    info.about.assert_called_once_with(adapter.ytdlp_version)
    info.url_history.assert_called_once()
    info.push_url.assert_called_once_with("url")
    info.last_sync_label.assert_called_once()


def test_failed_activity_clear_keeps_the_visible_history(monkeypatch):
    from backend import autorun

    monkeypatch.setattr(autorun, "clear_history", lambda: {"ok": False, "error": "busy"})

    class Adapter(InfoMixin):
        _window = Mock()
        _reload_config = Mock()

    api = Adapter()
    assert not api.autorun_history_clear()["ok"]
    api._window.evaluate_js.assert_not_called()
