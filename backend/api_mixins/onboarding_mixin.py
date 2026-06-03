"""
OnboardingMixin — first-run setup wizard backend.

Bridges the wizard UI (web/onboarding.js) to backend.deps_installer, which
restores the dependency-install onboarding lost in the tkinter -> pywebview
migration.

Long installs run on a daemon thread so the js_api call returns immediately;
progress streams to the wizard via window._onboardingProgress({...}). Each
install ends with a {"status": "done", ...} push carrying a fresh probe so
the wizard can re-render final state.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403

from backend import deps_installer as _deps


class OnboardingMixin:

    def onboarding_state(self):
        """Snapshot for the wizard: whether onboarding is complete, the
        current archive root, and a dependency probe."""
        cfg = self._config or load_config()
        try:
            deps = _deps.probe(check_whisper_import=False)
        except Exception as e:
            _log.warning("onboarding probe failed: %s", e)
            deps = {"error": str(e)}
        return {
            "onboarded": bool(cfg.get("onboarded")),
            "output_dir": (cfg.get("output_dir") or "").strip(),
            "version": APP_VERSION,
            "deps": deps,
        }

    def onboarding_probe(self, check_whisper=False):
        """Re-probe dependency state on demand (e.g. after the user installs
        something manually and clicks Re-check)."""
        try:
            return {"ok": True, "deps": _deps.probe(check_whisper_import=bool(check_whisper))}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _push_onboarding(self, payload: dict):
        """Push a progress / state dict to the wizard JS. Best-effort —
        a closed window or detached bridge must never break the installer."""
        try:
            if self._window is None:
                return
            self._window.evaluate_js(
                "window._onboardingProgress && "
                f"window._onboardingProgress({json.dumps(payload)});")
        except Exception as e:
            _log.debug("onboarding push failed: %s", e)

    def _run_install(self, kind: str):
        """Background worker: run the requested installer, stream progress,
        emit a final 'done' with a fresh probe."""
        def prog(d):
            self._push_onboarding(d)
        res: dict = {}
        try:
            if kind == "core":
                res = _deps.install_core(progress=prog)
            elif kind == "whisper":
                res = _deps.install_whisper_stack(progress=prog)
            else:
                res = {"ok": False, "error": f"unknown installer '{kind}'"}
        except Exception as e:
            _log.warning("onboarding install %s crashed: %s", kind, e)
            res = {"ok": False, "error": str(e)}
        try:
            state = _deps.probe(check_whisper_import=(kind == "whisper"))
        except Exception:
            state = {}
        self._push_onboarding({
            "phase": kind,
            "status": "done",
            "ok": bool(res.get("ok")),
            "error": res.get("error", ""),
            "state": state,
        })

    def onboarding_install_core(self):
        """Download yt-dlp + ffmpeg into the managed bin dir (background)."""
        threading.Thread(target=self._run_install, args=("core",),
                         daemon=True).start()
        return {"ok": True, "started": True}

    def onboarding_install_whisper(self):
        """Install Python 3.11 + faster-whisper + torch (+CUDA) (background).
        This is large (multi-GB on a CUDA machine) — the wizard shows live
        progress and the rest of the app works without it."""
        threading.Thread(target=self._run_install, args=("whisper",),
                         daemon=True).start()
        return {"ok": True, "started": True}

    def onboarding_finish(self):
        """Mark onboarding complete so the wizard won't auto-show again.
        Persisted in config (`onboarded`)."""
        try:
            cfg = load_config()
            cfg["onboarded"] = True
            ok = save_config(cfg)
            self._reload_config()
            return {"ok": bool(ok)}
        except Exception as e:
            _log.warning("onboarding_finish failed: %s", e)
            return {"ok": False, "error": str(e)}
