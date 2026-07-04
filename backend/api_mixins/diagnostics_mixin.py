"""
DiagnosticsMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They prefer AppServices when present for config and
log dependencies, with legacy private Api attributes kept as fallback
state.
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
import urllib.request

from ._shared import _log, webview
from backend.archive_capacity import archive_capacity_status
from backend.ytarchiver_config import CONFIG_FILE, config_is_writable, load_config
from backend.version import APP_VERSION


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
            rows.append({"name": "Python 3.11 (whisper)", "ok": bool(py311),
                         "detail": py311 or "Install Python 3.11 + faster-whisper"})
        except Exception:
            rows.append({"name": "Python 3.11 (whisper)", "ok": False, "detail": "unknown"})
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
                    import threading as _th
                    def _delayed_toast():
                        try:
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
                    _th.Timer(3.0, _delayed_toast).start()
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
            return {"ok": False, "error": "output_dir not set", "missing": []}
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

        def _run():
            try:
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

        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}


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

        # 2. Python 3.11 (for whisper + punct workers)
        try:
            mgr = getattr(self, "_transcribe", None)
            py311 = getattr(mgr, "_python311", None) if mgr else None
            if py311 and os.path.isfile(py311):
                _row("Python 3.11 (whisper)", True, py311)
            else:
                _row("Python 3.11 (whisper)", False,
                     "Not found — whisper + punctuation won't run")
        except Exception as e:
            _row("Python 3.11 (whisper)", False, str(e))

        # 3. FTS transcription DB
        try:
            from backend.ytarchiver_config import TRANSCRIPTION_DB
            if TRANSCRIPTION_DB.exists():
                sz = TRANSCRIPTION_DB.stat().st_size
                gb = sz / (1024 ** 3)
                _row("Transcript DB", True,
                     f"{TRANSCRIPTION_DB} ({gb:.2f} GB)")
            else:
                _row("Transcript DB", True,
                     f"{TRANSCRIPTION_DB} (will be created on first use)")
        except Exception as e:
            _row("Transcript DB", False, str(e))

        # 4. GPU (nvidia-smi probe). Bound the JS-bridge freeze window:
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
                _row("GPU", False, "nvidia-smi not available (CPU whisper only)")
        except subprocess.TimeoutExpired:
            _row("GPU", False, "nvidia-smi probe timed out (>2s)")
        except Exception as e:
            _row("GPU", False, f"nvidia-smi not runnable: {e}")

        # 5. Archive root + free space
        try:
            base = (cfg.get("output_dir") or "").strip()
            status = archive_capacity_status(base, cfg)
            _row("Archive root", status.get("ok", False),
                 status.get("detail", ""), status.get("status"))
        except Exception as e:
            _row("Archive root", False, str(e))

        # 6. Config file
        try:
            if CONFIG_FILE.exists():
                _row("Config file", True, str(CONFIG_FILE))
            else:
                _row("Config file", False, "Not found")
        except Exception as e:
            _row("Config file", False, str(e))

        # 7. Write-gate state
        try:
            from backend.ytarchiver_config import config_is_writable
            writable = config_is_writable()
            _row("Write-gate", writable,
                 "Enabled" if writable else "OFF")
        except Exception as e:
            _row("Write-gate", False, str(e))

        # 8. Cookies source
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
                    _row("Cookies source", True, "browser cookies (profile masked)")
                else:
                    _row("Cookies source", True, "cookies.txt file (path masked)")
            else:
                _row("Cookies source", False,
                     "No browser profile or cookies.txt found (public-only mode)")
        except Exception as e:
            _row("Cookies source", False, str(e))

        return {"ok": True, "rows": rows}
