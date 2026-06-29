"""
WindowMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They prefer AppServices when present for config, queue,
and log dependencies, with legacy private Api attributes kept as
fallback state.
"""
from __future__ import annotations

import ctypes
import os
import subprocess
import sys
import threading
import time

from ._shared import _log, webview, normalize_dialog_paths
from backend.ytarchiver_config import load_config, save_config
from backend import window_state as winstate


class WindowMixin:
    def _window_services(self):
        return getattr(self, "services", None)

    def _window_config(self):
        services = self._window_services()
        if services is not None:
            return services.fresh_config()
        cfg = getattr(self, "_config", None)
        if cfg is not None:
            return cfg
        return load_config()

    def _window_save_config(self, cfg):
        services = self._window_services()
        if services is not None:
            return services.save_config(cfg)
        return save_config(cfg)

    def _window_log_stream(self):
        services = self._window_services()
        stream = (getattr(services, "log_stream", None)
                  if services is not None else None)
        return stream if stream is not None else self._log_stream

    def _window_queues(self):
        services = self._window_services()
        queues = (getattr(services, "queues", None)
                  if services is not None else None)
        return queues if queues is not None else self._queues


    def confirm_close(self, choice, remember=False):
        """Frontend hook for the close-to-tray dialog. `choice` is
        either "quit" (destroy window + run shutdown) or "tray" (hide).
        `remember` persists the choice as close_behavior so the dialog
        stops appearing.

        BUG FIX (2026-05-13): destroying the window from inside this
        js_api method deadlocks pywebview. The JS thread is waiting
        for this Python method to return, but `window.destroy()` needs
        the main GUI thread to be free to actually shut the window —
        and the GUI thread is the same thread servicing the JS bridge
        call. Result: full freeze, task-manager kill required.
        Fix: defer the destroy/hide to a background thread so this
        method returns immediately. The JS bridge response delivers,
        the GUI thread becomes free, and the deferred action runs
        cleanly.
        """
        choice = (choice or "").lower()
        if choice not in ("quit", "tray"):
            # Bad choice still releases the reentrant-X guard so the
            # next click can show a fresh modal (audit: main.py H24).
            try: self._close_dialog_pending = False
            except Exception: pass
            return {"ok": False, "error": "Invalid choice"}
        # Release the X-click reentrant guard either way — if the user
        # picked "tray" we'll get more X-clicks later; if "quit" the
        # process is about to exit anyway. Doing it here keeps the flag
        # lifetime tied to the modal lifetime (audit: main.py H24).
        try: self._close_dialog_pending = False
        except Exception: pass
        if remember:
            try:
                cfg = self._window_config()
                cfg["close_behavior"] = choice
                if self._window_save_config(cfg):
                    self._reload_config()
                else:
                    try:
                        self._window_log_stream().emit_dim(
                            " (close behavior preference not saved)")
                    except Exception:
                        pass
            except Exception as e:
                try:
                    self._window_log_stream().emit_dim(
                        f" (close behavior preference not saved: {e})")
                except Exception:
                    pass
                _log.warning("close behavior preference save failed: %s", e)

        # Quit runs on a background thread so the JS bridge can return,
        # then performs the full shutdown cleanup before destroying the
        # webview. main.py performs the final process exit after
        # webview.start() returns.

        def _kill_via_thread():
            import time as _t
            _t.sleep(0.10)  # let the API response return to JS
            # Last-resort fallback only. Normal cleanup gets a long grace
            # period so queue/index/log writes are not cut off mid-flush.
            def _watchdog():
                _t.sleep(30.0)
                import os as _os2
                _os2._exit(0)
            try:
                threading.Thread(target=_watchdog, daemon=True).start()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            # Hide the window first so Quit FEELS instant even while
            # cleanup runs (hide-from-bg-thread is the proven tray
            # pattern).
            try:
                if self._window:
                    self._window.hide()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            # Run the FULL shutdown sequence (queue persist, child-
            # process kill, server stop, SQLite checkpoint). The old
            # path skipped it entirely: TerminateProcess does NOT kill
            # children on Windows, so in-flight yt-dlp/ffmpeg kept
            # writing to the archive headless and the whisper worker
            # lingered holding VRAM — and the queue save was capped at
            # a 200ms join that a waking DrivePool routinely blew
            # through, abandoning the write mid-flight.
            _cb = getattr(self, "_shutdown_cleanup_fn", None)
            if callable(_cb):
                try:
                    _cb()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            else:
                # Wiring missing (unusual init order) — at least give
                # the queue save a real chance.
                try:
                    save_t = threading.Thread(
                        target=lambda: self._window_queues().save_now(),
                        daemon=True)
                    save_t.start()
                    save_t.join(timeout=2.0)
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            # After cleanup, close the webview and let main.py's
            # webview.start() teardown perform the final process exit.
            try:
                state = getattr(self, "_close_state", None)
                if isinstance(state, dict):
                    state["flag"] = True
            except Exception as e:
                _log.debug("swallowed: %s", e)
            try:
                if self._window:
                    self._window.destroy()
                    return
            except Exception as e:
                _log.debug("swallowed: %s", e)
            import os as _os
            _os._exit(0)

        if choice == "tray":
            # Tray path stays soft — we want the window hidden but
            # the process alive. Dispatch on a bg thread to avoid
            # the JS-bridge deadlock pattern.
            def _hide():
                import time as _t
                _t.sleep(0.05)
                try: self._window.hide()
                except Exception as e: _log.debug("swallowed: %s", e)
            try:
                threading.Thread(target=_hide, daemon=True).start()
            except Exception as e:
                _log.debug("swallowed: %s", e)
        else:
            # Quit path: schedule a SINGLE killer thread. The
            # previous double-thread pattern (originally intended as
            # belt-and-suspenders) caused two concurrent
            # self._queues.save_now() calls to race on the same
            # .tmp / os.replace — the queue file could be left
            # half-written or truncated on Quit. Single thread is
            # enough since the killer's join timeout is 200ms and the
            # TerminateProcess fallback is its own guarantee.
            try:
                threading.Thread(target=_kill_via_thread, daemon=True).start()
            except Exception as e:
                _log.debug("swallowed: %s", e)

        return {"ok": True, "action": choice}


    # ─── Native file/folder dialogs ─────────────────────────────────────

    def _normalize_dialog_paths(self, paths):
        """pywebview returns one of: None, "" (cancel), a single string
        (some platforms wrap one path in a bare string), a tuple/list
        of strings (most common), or an empty tuple/list (cancel).
        Bare `if paths: paths[0]` failed both cases: a string is
        truthy but `paths[0]` is the first char; an empty tuple was
        truthy on some pywebview versions even when no selection
        landed (audit: window_mixin H9 — same fix already in
        backup_mixin.py).
        """
        return normalize_dialog_paths(paths)

    def pick_folder(self, title="Choose a folder", initial=None):
        """Open a native folder picker (pywebview's FOLDER_DIALOG)."""
        try:
            import webview as _webview
            if self._window is None:
                return {"ok": False, "error": "No window"}
            paths = self._window.create_file_dialog(
                _webview.FOLDER_DIALOG,
                directory=str(initial) if initial else "",
                allow_multiple=False,
            )
            path = self._normalize_dialog_paths(paths)
            if path:
                return {"ok": True, "path": path}
            return {"ok": False, "cancelled": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def pick_file(self, title="Choose a file", initial=None, ext_filter=None):
        """Open a native file picker (for Manual Transcribe dialog, etc.)."""
        try:
            import webview as _webview
            if self._window is None:
                return {"ok": False, "error": "No window"}
            file_types = tuple(ext_filter) if ext_filter else ()
            paths = self._window.create_file_dialog(
                _webview.OPEN_DIALOG,
                directory=str(initial) if initial else "",
                allow_multiple=False,
                file_types=file_types,
            )
            path = self._normalize_dialog_paths(paths)
            if path:
                return {"ok": True, "path": path}
            return {"ok": False, "cancelled": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    # ─── Window state persistence ───────────────────────────────────────

    def window_state_load(self):
        return winstate.load_window_state()


    def window_state_save(self, partial):
        return {"ok": winstate.save_window_state(partial or {})}


    def window_toggle_fullscreen(self):
        """Toggle fullscreen on the pywebview window (F11)."""
        try:
            if self._window is None:
                return {"ok": False, "error": "No window"}
            self._window.toggle_fullscreen()
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def window_quit(self):
        """Clean-shutdown trigger from JS (Ctrl+Q).

        Routes through confirm_close("quit") — the deferred path with
        the watchdog + full cleanup. The old direct
        self._window.destroy() ran on the JS bridge thread, which is
        the EXACT documented deadlock confirm_close was rewritten to
        avoid in 2026-05-13 (full freeze, task-manager kill), and it
        also skipped queue save + child-process cleanup entirely.
        """
        return self.confirm_close("quit", False)


    def app_restart(self):
        """Re-launch the exe/script, then destroy the current window.

        Used after backup-restore so the freshly-loaded config takes effect.
        """
        try:
            import subprocess
            # Cleanup happens ONCE in `_die` (post-launch) — the
            # previous pre-launch + post-launch double-call meant
            # `_queues.save_now()` ran twice. On a slow disk the
            # second save could land AFTER the new instance had
            # already written its own queue file, clobbering it
            # (audit: window_mixin H8). Also: pre-launch cleanup
            # would race the new instance's port-bind (~2.5s kill
            # vs new instance startup), so dropping it here also
            # eliminates the brief two-instance window.
            # Release the single-instance mutex BEFORE launching the
            # child. The old instance held it through its multi-second
            # teardown, so the child's import-time CreateMutexW saw
            # ERROR_ALREADY_EXISTS and exited — "restart" silently
            # became "quit" (deterministically under `python main.py`,
            # a coin flip for the frozen exe). Accessed via
            # sys.modules["__main__"]: `import main` here would
            # RE-EXECUTE main.py's module level — including the mutex
            # check — inside this very process. (If the Popen below
            # fails, we keep running without the mutex; acceptable —
            # the user just retries the restart.)
            try:
                import ctypes as _ct
                _main_mod = sys.modules.get("__main__")
                _mx = getattr(_main_mod, "_INSTANCE_MUTEX", None)
                if _mx:
                    _ct.windll.kernel32.CloseHandle(_mx)
                    try:
                        _main_mod._INSTANCE_MUTEX = None
                    except Exception:
                        pass
            except Exception as e:
                _log.debug("swallowed: %s", e)
            if getattr(sys, "frozen", False):
                # Running as PyInstaller exe — relaunch the .exe itself.
                subprocess.Popen([sys.executable],
                                 close_fds=True,
                                 creationflags=0x00000008) # DETACHED_PROCESS
            else:
                subprocess.Popen([sys.executable, *sys.argv],
                                 close_fds=True,
                                 creationflags=0x00000008)
            # Small delay so the new process initializes before we tear down.
            # call _shutdown_cleanup() BEFORE os._exit so
            # queue state gets saved, window geometry persisted, and
            # child subprocesses (yt-dlp / whisper / ffmpeg) killed
            # rather than carried over into the restarted process
            # where they'd race with the new instance's fresh jobs.
            def _die():
                time.sleep(0.6)
                try:
                    # Run the full close sequence (queue persist,
                    # subprocess kills, port release) before exiting.
                    # `_shutdown_cleanup` is defined inside main() and
                    # bound to this Api instance via `_register_shutdown`
                    # at startup. If that wiring is somehow missing
                    # (unusual init order), we still os._exit so the
                    # restart doesn't hang.
                    try:
                        _cb = getattr(self, "_shutdown_cleanup_fn", None)
                        if callable(_cb):
                            _cb()
                    except Exception as _ce:
                        print(f"[app_restart] shutdown_cleanup: {_ce}")
                    if self._window:
                        self._window.destroy()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                os._exit(0)
            threading.Thread(target=_die, daemon=True).start()
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def save_text_to_file(self, suggested_name, text):
        """Save log text via pywebview's save dialog."""
        try:
            import webview as _wv
            if self._window is None:
                return {"ok": False, "error": "No window"}
            paths = self._window.create_file_dialog(
                _wv.SAVE_DIALOG,
                save_filename=suggested_name,
            )
            path = normalize_dialog_paths(paths)
            if not path:
                return {"ok": False, "cancelled": True}
            # Atomic tmp+replace so a mid-write disk-full doesn't
            # truncate a pre-existing file the user picked to
            # overwrite (audit: window_mixin.py:244-261).
            tmp = path + ".tmp"
            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    f.write(text or "")
                    try:
                        f.flush()
                        os.fsync(f.fileno())
                    except OSError:
                        pass
                os.replace(tmp, path)
            except Exception:
                try: os.remove(tmp)
                except OSError: pass
                raise
            return {"ok": True, "path": path}
        except Exception as e:
            return {"ok": False, "error": str(e)}
