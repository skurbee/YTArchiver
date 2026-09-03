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

import threading
import uuid

from backend import deps_installer as _deps
from backend import youtube_traffic
from backend.services.job_supervisor import WorkAdmissionClosed
from backend.services.managed_work import start_managed_task
from backend.version import APP_VERSION
from backend.ytarchiver_config import load_config, save_config, update_config

from ._shared import _log


class OnboardingMixin:
    _onboarding_install_init_lock = threading.Lock()

    def _ensure_onboarding_install_state(self):
        if (hasattr(self, "_onboarding_install_lock")
                and hasattr(self, "_onboarding_install_running")):
            return
        with self._onboarding_install_init_lock:
            if not hasattr(self, "_onboarding_install_lock"):
                self._onboarding_install_lock = threading.Lock()
            if not hasattr(self, "_onboarding_install_running"):
                self._onboarding_install_running = {
                    "core": False,
                    "whisper": False,
                }

    def _onboarding_install_snapshot(self):
        self._ensure_onboarding_install_state()
        with self._onboarding_install_lock:
            return dict(self._onboarding_install_running)

    def _onboarding_services(self):
        return getattr(self, "services", None)

    def _onboarding_config(self):
        services = self._onboarding_services()
        if services is not None:
            return services.fresh_config()
        cfg = getattr(self, "_config", None)
        if cfg is not None:
            return cfg
        return load_config()

    def _onboarding_save_config(self, cfg):
        services = self._onboarding_services()
        if services is not None:
            return services.save_config(cfg)
        return save_config(cfg)

    def _onboarding_update_config(self, mutator):
        """Commit one wizard field patch against the latest config."""
        services = self._onboarding_services()
        mutate = (getattr(services, "mutate_config", None)
                  if services is not None else None)
        if callable(mutate):
            return mutate(mutator)
        return update_config(mutator)

    def onboarding_state(self):
        """Snapshot for the wizard: whether onboarding is complete, the
        current archive root, and a dependency probe."""
        cfg = self._onboarding_config()
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
            "youtube_traffic": youtube_traffic.status(cfg),
            "installing": self._onboarding_install_snapshot(),
        }

    def onboarding_set_traffic(self, mode, custom=None):
        """Persist the first-run YouTube traffic-safety choice."""
        mode = youtube_traffic.normalize_mode(mode)
        budget_autosync_disabled = False
        try:
            def _mutate(cfg):
                nonlocal budget_autosync_disabled
                cfg["youtube_traffic_mode"] = mode
                if (mode == "unlimited"
                        and int(cfg.get("autorun_interval", 0) or 0) == -1):
                    cfg["autorun_interval"] = 0
                    budget_autosync_disabled = True
                if mode == "custom" and isinstance(custom, dict):
                    bounds = {
                        "daily": (
                            "youtube_traffic_custom_daily", 1, 100_000),
                        "hourly": (
                            "youtube_traffic_custom_hourly", 1, 10_000),
                        "min_gap": (
                            "youtube_traffic_custom_min_gap", 0, 3600),
                        "max_gap": (
                            "youtube_traffic_custom_max_gap", 0, 3600),
                    }
                    for source, (target, low, high) in bounds.items():
                        if source in custom:
                            cfg[target] = max(
                                low, min(high, int(custom[source])))
                    cfg["youtube_traffic_custom_max_gap"] = max(
                        int(cfg.get("youtube_traffic_custom_min_gap", 10)),
                        int(cfg.get("youtube_traffic_custom_max_gap", 20)),
                    )

            _result, cfg = self._onboarding_update_config(_mutate)
            self._reload_config()
            return {
                "ok": True,
                "youtube_traffic": youtube_traffic.status(cfg),
                "budget_autosync_disabled": budget_autosync_disabled,
            }
        except Exception as e:
            _log.warning("onboarding traffic save failed: %s", e)
            return {"ok": False, "error": str(e)}

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
            self.services.event_bus.onboarding_progress(payload)
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
        return self._start_onboarding_install("core")

    def onboarding_install_whisper(self):
        """Install Python 3.11 + faster-whisper + torch (+CUDA) (background).
        This is large (multi-GB on a CUDA machine) — the wizard shows live
        progress and the rest of the app works without it."""
        return self._start_onboarding_install("whisper")

    def _start_onboarding_install(self, kind: str):
        if kind not in {"core", "whisper"}:
            return {"ok": False, "started": False,
                    "error": "Unknown dependency installer."}
        self._ensure_onboarding_install_state()
        with self._onboarding_install_lock:
            if self._onboarding_install_running.get(kind):
                return {"ok": True, "started": False, "running": True,
                        "kind": kind}
            self._onboarding_install_running[kind] = True
        task_id = f"onboarding-{kind}-{uuid.uuid4().hex}"
        cancel = threading.Event()

        def _run():
            try:
                if not cancel.is_set():
                    self._run_install(kind)
            finally:
                with self._onboarding_install_lock:
                    self._onboarding_install_running[kind] = False

        try:
            start_managed_task(
                self,
                owner="dependency-installer",
                label=f"Install onboarding {kind} dependencies",
                task_id=task_id,
                cancel=cancel,
                target=_run,
                name=f"onboarding-install-{kind}",
                thread_factory=threading.Thread,
            )
        except WorkAdmissionClosed as exc:
            with self._onboarding_install_lock:
                self._onboarding_install_running[kind] = False
            return {"ok": False, "started": False, "error": str(exc)}
        except Exception as exc:
            with self._onboarding_install_lock:
                self._onboarding_install_running[kind] = False
            return {"ok": False, "started": False, "error": str(exc)}
        return {"ok": True, "started": True}

    def onboarding_finish(self):
        """Mark onboarding complete so the wizard won't auto-show again.
        Persisted in config (`onboarded`)."""
        try:
            self._onboarding_update_config(
                lambda cfg: cfg.__setitem__("onboarded", True))
            self._reload_config()
            return {"ok": True}
        except Exception as e:
            _log.warning("onboarding_finish failed: %s", e)
            return {"ok": False, "error": str(e)}
