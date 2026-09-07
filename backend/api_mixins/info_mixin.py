"""Stable information endpoints delegating to an explicitly composed service."""
from __future__ import annotations

from backend.services.application_information import (
    ApplicationInformation,
)
from backend.version import APP_VERSION, APP_VERSION_DATE

from ._shared import _log


class InfoMixin:
    def _information(self) -> ApplicationInformation:
        information = self.services.information
        if information is None:
            raise RuntimeError("Application information service was not composed")
        return information

    def get_runtime_info(self):
        return self._information().runtime_info(self._config)

    def ping(self):
        return "pong"

    def get_header_version(self):
        return {"version": APP_VERSION, "date": APP_VERSION_DATE}

    def get_activity_log_history(self):
        if self._config is not None:
            from backend.autorun import history_entries_for_ui
            return history_entries_for_ui(self._config)
        return []

    def autorun_history_clear(self):
        # The activity repository owns its JSONL/config retirement transaction.
        from backend.autorun import clear_history
        result = clear_history()
        self._reload_config()
        if result.get("ok"):
            try:
                if self._window is not None:
                    self._window.evaluate_js(
                        "if (window.renderActivityLog) window.renderActivityLog([]);"
                        "if (window._syncActivityLogVisibility) "
                        "window._syncActivityLogVisibility();"
                        "if (window._syncClearButtonVisibility) "
                        "window._syncClearButtonVisibility();")
            except Exception as exc:
                _log.debug("Activity history refresh failed: %s", exc)
        return result

    def get_initial_main_log(self):
        return []

    def about_info(self):
        return self._information().about(self.ytdlp_version)

    def url_history(self):
        return self._information().url_history()

    def _push_url_history(self, url):
        self._information().push_url(url)

    def get_last_sync_label(self):
        return self._information().last_sync_label()
