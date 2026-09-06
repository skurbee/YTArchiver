"""
DiagnosticsMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They prefer AppServices when present for config and
log dependencies, with legacy private Api attributes kept as fallback
state.
"""
from __future__ import annotations

import os
import threading
import time
import uuid

from backend.archive_capacity import archive_capacity_status
from backend.services.managed_work import start_managed_task
from backend.version import APP_VERSION
from backend.ytarchiver_config import CONFIG_FILE, load_config

from ._shared import _log

_integrity_init_lock = threading.Lock()


class DiagnosticsMixin:
    def _diagnostics_services(self):
        return getattr(self, "services", None)

    def _diagnostics_config(self):
        services = self._diagnostics_services()
        if services is not None:
            return services.fresh_config()
        return self._config or load_config()

    def _diagnostics_log_stream(self):
        services = self._diagnostics_services()
        stream = (getattr(services, "log_stream", None)
                  if services is not None else None)
        return stream if stream is not None else self._log_stream


    def check_dependencies(self):
        """Probe for optional deps + subprocess runners, log anything missing.

        Runs on startup. Mirrors YTArchiver.py:33620 check_dependencies but
        also surfaces Python 3.11 + ffmpeg which YTArchiver actually needs.
        Returns the probe result list so callers (like the startup log) can
        render it.
        """
        rows = []
        # Python bits
        try:
            import pystray  # noqa: F401
            rows.append({"name": "pystray", "ok": True, "detail": ""})
        except ImportError:
            rows.append({"name": "pystray", "ok": False, "detail": "pip install pystray"})
        try:
            from PIL import Image  # noqa: F401
            rows.append({"name": "Pillow", "ok": True, "detail": ""})
        except ImportError:
            rows.append({"name": "Pillow", "ok": False, "detail": "pip install Pillow"})
        try:
            import webview  # noqa: F401
            rows.append({"name": "pywebview", "ok": True, "detail": ""})
        except ImportError:
            rows.append({"name": "pywebview", "ok": False, "detail": "pip install pywebview"})
        # Executables. Use the same managed-bin-aware probe that the
        # onboarding installer uses so app-installed binaries do not
        # trigger false "missing from PATH" warnings.
        try:
            from backend import deps_installer as _deps_installer
            _dep_probe = _deps_installer.probe(check_whisper_import=False)
        except Exception:
            _dep_probe = {}
        for exe, key, hint in (
                ("yt-dlp", "ytdlp",
                 "Install yt-dlp from Settings -> Dependencies"),
                ("ffmpeg", "ffmpeg",
                 "Install ffmpeg from Settings -> Dependencies"),
                ("ffprobe", "ffprobe", "Comes with ffmpeg")):
            info = (_dep_probe.get(key) or {}) if _dep_probe else {}
            path = info.get("path") or ""
            rows.append({"name": exe, "ok": bool(path),
                         "detail": path or hint})
        # Python 3.11 (for whisper)
        try:
            mgr = getattr(self, "_transcribe", None)
            py311 = getattr(mgr, "_python311", None) if mgr else None
            rows.append({
                "name": "AI transcription tools",
                # Transcription is optional. Keep it visible without making
                # the startup summary claim downloads are broken.
                "ok": True,
                "status": "ok" if py311 else "warning",
                "detail": py311 or (
                    "Run setup again from Settings > App behavior"),
            })
        except Exception:
            rows.append({"name": "AI transcription tools", "ok": True,
                         "status": "warning",
                         "detail": "Status unavailable"})
        # Log a one-line summary for the startup log
        missing = [r for r in rows if not r["ok"]]
        if missing:
            log_stream = self._diagnostics_log_stream()
            log_stream.emit([
                ["[Deps] ", "sync_bracket"],
                [f"{len(missing)} missing: ", "red"],
                [", ".join(r["name"] for r in missing) + "\n", "dim"],
            ])
            log_stream.flush()
            # Critical missing tools (yt-dlp + ffmpeg) prevent sync from
            # working at all. Surface a high-visibility warning to the
            # log AND push a toast so the user doesn't just see a generic
            # "sync failed" error later. The deps line above is easy to
            # miss in a long startup log.
            CRITICAL = {"yt-dlp", "ffmpeg"}
            missing_critical = [r["name"] for r in missing
                                if r["name"] in CRITICAL]
            if missing_critical:
                names = " + ".join(missing_critical)
                log_stream.emit([
                    ["[Deps] ", "sync_bracket"],
                    [f"⚠ {names} not found — ",
                     "red"],
                    ["downloads will fail until installed.\n",
                     "dim"],
                ])
                log_stream.flush()
                # Best-effort toast (pywebview window may not be live yet
                # at first launch — fire after a short delay).
                try:
                    _toast_cancel = threading.Event()

                    def _delayed_toast():
                        try:
                            if _toast_cancel.wait(3.0):
                                return
                            if self._window is None:
                                return
                            msg = (f"Missing: {names}. "
                                   "Install from Settings -> Dependencies "
                                   "for downloads to work.")
                            services = self._diagnostics_services()
                            if services is None:
                                return
                            services.event_bus.show_toast(
                                msg, "error", ttl_ms=12000)
                        except Exception:
                            pass
                    start_managed_task(
                        self,
                        owner="ui-notice",
                        label="Missing dependency notice",
                        target=_delayed_toast,
                        cancel=_toast_cancel,
                        name="missing-dependency-notice",
                        thread_factory=threading.Thread,
                    )
                except Exception:
                    pass
        return {"ok": True, "rows": rows, "missing": len(missing)}


    def check_channel_folders(self):
        """Verify each subscribed channel's on-disk folder exists.

        Returns the list of channels whose folders are missing (only for
        channels marked `initialized=True`). The UI can then prompt the
        user to remove / locate / skip each one. Never modifies config.
        """
        cfg = self._diagnostics_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False,
                    "error": "No archive folder is configured.", "missing": []}
        from backend.sync import channel_folder_name as _cfn
        missing = []
        for ch in cfg.get("channels", []):
            if not ch.get("initialized", False):
                continue
            expected = os.path.join(base, _cfn(ch))
            if not os.path.isdir(expected):
                missing.append({
                    "name": ch.get("name") or ch.get("folder") or "",
                    "url": ch.get("url", ""),
                    "expected": expected,
                })
        if missing:
            log_stream = self._diagnostics_log_stream()
            log_stream.emit([
                ["[Subs] ", "sync_bracket"],
                [f"{len(missing)} channel folder(s) missing \u2014 ", "red"],
                ["see Subs tab for reconcile\n", "dim"],
            ])
            log_stream.flush()
        return {"ok": True, "missing": missing}


    def check_app_update(self):
        """Poll GitHub releases for a newer YTArchiver tag.

        Non-blocking — runs on a background thread. Silent on network failure
        or rate-limit. When a newer version exists, emits a banner into the
        main log with the download URL.

        Mirrors YTArchiver.py:33738 _check_app_update.
        """
        def _ver_tuple(s):
            try:
                return tuple(int(x) for x in str(s).lstrip("v")
                             .replace("-alpha", "").replace("-beta", "").split("."))
            except Exception:
                return (0,)

        cancel_event = threading.Event()

        def _run():
            try:
                if cancel_event.is_set():
                    return
                import json as _json
                import urllib.request as _ur
                req = _ur.Request(
                    "https://api.github.com/repos/skurbee/YTArchiver/releases/latest",
                    headers={"User-Agent": "YTArchiver"},
                )
                with _ur.urlopen(req, timeout=8) as resp:
                    # Size-cap the read so a malformed or malicious
                    # response can't OOM the app (audit:
                    # diagnostics_mixin.py:155-191). 1MB is plenty
                    # for a GitHub release-metadata JSON.
                    data = _json.loads(resp.read(1_000_000))
                if cancel_event.is_set():
                    return
                latest = (data.get("tag_name") or "").strip()
                rel_url = data.get("html_url") or \
                    "https://github.com/skurbee/YTArchiver/releases/latest"
                current = APP_VERSION
                if latest and _ver_tuple(latest) > _ver_tuple(current):
                    sep = "=" * 54
                    log_stream = self._diagnostics_log_stream()
                    log_stream.emit([[f"\n{sep}\n", "update_sep"]])
                    log_stream.emit([
                        [f" \u2b06 Update available: {latest} ", "update_head"],
                        [f"(you have {current})\n", "update_head"],
                    ])
                    log_stream.emit([[f" Download: {rel_url}\n", "update_head"]])
                    log_stream.emit([[f"{sep}\n\n", "update_sep"]])
                    log_stream.flush()
            except Exception as _e:
                # surface the failure as a dim log line
                # so the user has evidence the check ran (and why it
                # failed). Old code silently swallowed, leaving no
                # trace of whether the update probe ever fired.
                try:
                    self._diagnostics_log_stream().emit_dim(
                        f"[Update] check skipped: {_e}")
                except Exception as e:
                    _log.debug("swallowed: %s", e)

        try:
            start_managed_task(
                self,
                owner="update-check",
                label="Application update check",
                target=_run,
                cancel=cancel_event,
                name="application-update-check",
                thread_factory=threading.Thread,
            )
            return {"ok": True, "started": True}
        except Exception as exc:
            return {"ok": False, "started": False, "error": str(exc)}


    def diagnostics_run(self):
        """Self-check: yt-dlp, Python 3.11, FTS DB, GPU, disk space, paths.

        Returns a list of {name, ok, detail} rows. Never raises — every probe
        is wrapped so the dialog always has something to show.
        """
        rows = []
        cfg = self._diagnostics_config()

        def _row(name, ok, detail, status=None):
            row_status = status or ("ok" if ok else "fail")
            rows.append({
                "name": name,
                "ok": bool(ok),
                "status": row_status,
                "detail": str(detail),
            })

        # 1. yt-dlp
        try:
            r = self.ytdlp_version()
            if r.get("ok"):
                _row("yt-dlp", True, r.get("version", "unknown"))
            else:
                _row("yt-dlp", False, r.get("error", "not found"))
        except Exception as e:
            _row("yt-dlp", False, str(e))

        # 2. ffmpeg + ffprobe.  These are core media tools, not implied by
        # the yt-dlp check above, so report them independently.
        try:
            from backend.deps_installer import probe as probe_dependencies

            media_tools = probe_dependencies()
            for key, label in (("ffmpeg", "ffmpeg"),
                               ("ffprobe", "ffprobe")):
                tool = media_tools.get(key) or {}
                _row(
                    label,
                    bool(tool.get("ok")),
                    "Ready" if tool.get("ok") else "Not installed",
                )
        except Exception as e:
            _row("ffmpeg", False, str(e))
            _row("ffprobe", False, str(e))

        # 3. Python 3.11 (for whisper + punct workers)
        try:
            mgr = getattr(self, "_transcribe", None)
            py311 = getattr(mgr, "_python311", None) if mgr else None
            if py311 and os.path.isfile(py311):
                _row("AI transcription tools", True, "Ready")
            else:
                _row("AI transcription tools", True,
                     "Not installed; video downloads still work", "warning")
        except Exception as e:
            _row("AI transcription tools", True,
                 f"Could not check; video downloads still work ({e})", "warning")

        # 4. FTS transcription DB
        try:
            from backend.ytarchiver_config import TRANSCRIPTION_DB
            if TRANSCRIPTION_DB.exists():
                sz = TRANSCRIPTION_DB.stat().st_size
                gb = sz / (1024 ** 3)
                _row("Transcript search", True, f"Ready ({gb:.2f} GB)")
            else:
                _row("Transcript search", True,
                     "Will be created when first needed")
        except Exception as e:
            _row("Transcript search", False, str(e))

        # 5. GPU (nvidia-smi probe). Bound the JS-bridge freeze window:
        # 5s was long enough for a driver-glitch hang to make the
        # Diagnostics dialog feel frozen (audit: diagnostics_mixin.py:
        # 243-255). nvidia-smi returns in <100ms on a healthy install,
        # so 2s is safe and still tolerates a slow first call after
        # driver reload. creationflags suppresses the console-flash
        # on Windows.
        try:
            import subprocess
            r = subprocess.run(
                ["nvidia-smi", "--query-gpu=name,memory.total",
                 "--format=csv,noheader"],
                capture_output=True, text=True, timeout=2,
                creationflags=(0x08000000 if os.name == "nt" else 0),
            )
            if r.returncode == 0 and r.stdout.strip():
                _row("GPU", True, r.stdout.strip().splitlines()[0])
            else:
                _row("GPU", True,
                     "GPU acceleration is unavailable; transcription will use the CPU",
                     "warning")
        except subprocess.TimeoutExpired:
            _row("GPU", True,
                 "The GPU check took too long; transcription will use the CPU",
                 "warning")
        except Exception as e:
            _row("GPU", True,
                 f"GPU acceleration is unavailable; transcription will use the CPU ({e})",
                 "warning")

        # 6. Archive root + free space
        try:
            base = (cfg.get("output_dir") or "").strip()
            status = archive_capacity_status(base, cfg)
            _row("Archive root", status.get("ok", False),
                 status.get("detail", ""), status.get("status"))
        except Exception as e:
            _row("Archive root", False, str(e))

        # 7. Config file
        try:
            if CONFIG_FILE.exists():
                _row("Settings file", True, "Ready")
            else:
                _row("Settings file", False, "Not found")
        except Exception as e:
            _row("Settings file", False, str(e))

        # 8. Write-gate state
        try:
            from backend.ytarchiver_config import config_is_writable
            writable = config_is_writable()
            _row("Settings access", writable,
                 "Ready" if writable else "Read-only")
        except Exception as e:
            _row("Settings access", False, str(e))

        # 9. Cookies source.  Signing in is optional; public videos continue
        # to work, so absence belongs in warnings rather than problems.
        try:
            from backend.sync import _find_cookie_source
            src = _find_cookie_source() or []
            if src and len(src) >= 2:
                # Mask the actual browser-profile filename or
                # cookies.txt path so the diagnostics dump only shows
                # "browser cookies" / "cookies file" without leaking
                # the user's profile name / path. Concrete values are
                # only useful for forensic debugging which a support
                # request can ask for separately.
                if src[0] == "--cookies-from-browser":
                    _row("YouTube sign-in", True, "Browser cookies found")
                else:
                    _row("YouTube sign-in", True, "Cookie file found")
            else:
                _row("YouTube sign-in", True,
                     "Not found; only public videos are available", "warning")
        except Exception as e:
            _row("YouTube sign-in", True,
                 f"Could not check; public videos are still available ({e})",
                 "warning")

        return {"ok": True, "rows": rows}


    def integrity_scan_preview(self):
        return self._run_integrity_scan()

    def _run_integrity_scan(self, *, cancel_event=None, progress=None):
        """Run Patch 5's archive reconciliation audit without changing data.

        Every path is resolved here from the application's active config and
        passed explicitly to the scanner.  The scanner has no repair entry
        point and opens SQLite through an immutable read-only connection.
        """
        cfg = self._diagnostics_config()
        archive_path = str(cfg.get("output_dir") or "").strip()
        if not archive_path:
            return {
                "ok": False,
                "preview_only": True,
                "repairs_applied": False,
                "repair_available": False,
                "error": "Choose an archive folder before running the integrity preview.",
            }
        try:
            from backend.activity_history import ACTIVITY_HISTORY_FILE
            from backend.integrity_scan import scan_integrity
            from backend.ytarchiver_config import (
                APP_DATA_DIR,
                ARCHIVE_FILE,
                QUEUE_FILE,
                TRANSCRIPTION_DB,
            )

            optional = {}
            if cancel_event is not None:
                optional.update(cancel_event=cancel_event, progress=progress)
            result = scan_integrity(
                archive_path=archive_path,
                config_path=CONFIG_FILE,
                db_path=TRANSCRIPTION_DB,
                queue_path=QUEUE_FILE,
                download_archive_path=ARCHIVE_FILE,
                transcription_recovery_path=(
                    APP_DATA_DIR / "ytarchiver_pending_transcribe.json"
                ),
                activity_history_path=ACTIVITY_HISTORY_FILE,
                **optional,
            )
            result["backup_notice"] = (
                "This was a read-only preview. Export and verify a full backup "
                "before applying any proposed repair outside YTArchiver."
            )
            return result
        except Exception as exc:
            _log.exception("integrity preview failed")
            return {
                "ok": False,
                "preview_only": True,
                "repairs_applied": False,
                "repair_available": False,
                "error": f"Integrity preview could not finish: {exc}",
            }

    def _integrity_job_state(self):
        # First calls can arrive on separate bridge threads.
        with _integrity_init_lock:
            if not hasattr(self, "_integrity_job_lock"):
                self._integrity_job_lock = threading.Lock()
                self._integrity_job = None
        return self._integrity_job_lock

    def integrity_scan_start(self):
        """Start one supervised read-only scan without occupying a bridge call."""
        lock = self._integrity_job_state()
        with lock:
            if self._integrity_job and self._integrity_job["running"]:
                return {"ok": True, "started": False, "job_id": self._integrity_job["job_id"]}
            job = {"job_id": uuid.uuid4().hex, "running": True,
                   "cancel_requested": False, "cancel": threading.Event(),
                   "started": time.monotonic(), "phase": "Starting deep archive check",
                   "completed": 0, "unit": "items checked", "result": None}
            self._integrity_job = job

        def progress(update):
            with lock:
                job.update(update)

        def run():
            try:
                result = self._run_integrity_scan(cancel_event=job["cancel"], progress=progress)
            except Exception as exc:
                _log.exception("background integrity preview failed")
                result = {"ok": False, "preview_only": True, "repairs_applied": False,
                          "error": f"Deep archive check could not finish: {exc}"}
            with lock:
                job.update(running=False, result=result,
                           elapsed_seconds=time.monotonic() - job["started"])

        try:
            start_managed_task(self, owner="integrity-scan", label="Deep archive check",
                               task_id=job["job_id"], target=run, cancel=job["cancel"],
                               name="integrity-scan")
        except Exception as exc:
            with lock:
                job.update(running=False, result={"ok": False, "error": str(exc)})
            return {"ok": False, "error": f"Could not start deep archive check: {exc}"}
        return {"ok": True, "started": True, "job_id": job["job_id"]}

    def integrity_scan_state(self):
        lock = self._integrity_job_state()
        with lock:
            job = self._integrity_job
            if not job:
                return {"ok": True, "running": False, "job_id": None}
            state = {key: value for key, value in job.items() if key not in ("cancel", "started")}
            state["ok"] = True
            if job["running"]:
                state["elapsed_seconds"] = time.monotonic() - job["started"]
            return state

    def integrity_scan_cancel(self, job_id=None):
        lock = self._integrity_job_state()
        with lock:
            job = self._integrity_job
            if not job or not job["running"] or (job_id and job_id != job["job_id"]):
                return {"ok": False, "error": "That deep archive check is no longer running."}
            job["cancel_requested"] = True
            job["cancel"].set()
            return {"ok": True, "cancel_requested": True}
