"""Small pywebview bridge event helper.

The JS bridge is a trust boundary and string interpolation gets repetitive
fast. This helper centralizes JSON serialization and the `</script>` escape
that keeps payloads safe to embed in evaluated JavaScript snippets.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any


class BridgeEventBus:
    """Best-effort event emitter for the pywebview window."""

    def __init__(self, window_getter: Callable[[], Any]):
        self._window_getter = window_getter

    @staticmethod
    def js_value(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False).replace("</", "<\\/")

    def evaluate(self, script: str) -> bool:
        window = self._window_getter()
        if window is None:
            return False
        window.evaluate_js(script)
        return True

    def call(self, function_name: str, *args: Any) -> bool:
        argv = ", ".join(self.js_value(arg) for arg in args)
        return self.evaluate(
            f"if (window.{function_name}) window.{function_name}({argv});")

    def show_toast(self, message: str, kind: str = "ok",
                   ttl_ms: int | None = None) -> bool:
        if ttl_ms is None:
            return self.call("_showToast", message, kind)
        return self.call("_showToast", {
            "msg": message,
            "kind": kind,
            "ttlMs": int(ttl_ms),
        })

    def onboarding_progress(self, payload: dict[str, Any]) -> bool:
        return self.call("_onboardingProgress", payload)

    def refresh_subs(self) -> bool:
        return self.evaluate(
            "if (window.refreshSubsTable) window.refreshSubsTable();")

    def show_toast_and_refresh_subs(self, message: str,
                                    kind: str = "ok") -> bool:
        return self.evaluate(
            "if (window._showToast) "
            f"window._showToast({self.js_value(message)}, {self.js_value(kind)});"
            "if (window.refreshSubsTable) window.refreshSubsTable();")

    def update_queues(self, payload: dict[str, Any],
                      state: dict[str, Any]) -> bool:
        return self.evaluate(
            "if (window.renderQueues) "
            f"window.renderQueues({self.js_value(payload)});"
            "if (window.setQueueState) "
            f"window.setQueueState({self.js_value(state)});")
