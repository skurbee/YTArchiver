"""
WindowMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class WindowMixin:

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
            return {"ok": False, "error": "Invalid choice"}
        if remember:
            try:
                from backend.ytarchiver_config import save_config as _sc
                cfg = self._config or load_config()
                cfg["close_behavior"] = choice
                _sc(cfg)
                self._reload_config()
            except Exception as e:
                _log.debug("swallowed: %s", e)

        # NUCLEAR OPTION (2026-05-13): repeated soft fixes haven't
        # held. The user wants Quit to actually quit, period. This
        # path now does the minimum possible amount of work before
        # invoking Win32 TerminateProcess on ourselves — which is
        # the strongest possible "kill this process" call short of
        # pulling power. TerminateProcess does NOT need the target
        # thread to cooperate; it can be invoked from any thread
        # and the OS kills the process unconditionally.
        # The price: in-flight downloads / writes may leave temp
        # files behind. But the user is clicking Quit. They expect
        # the app to exit. Cleanup hygiene is secondary to that.
        # Subsequent launches will rotate temp folders via the
        # startup cleanup_temps sweep anyway.
        import ctypes as _ctypes

        # Fire TWO independent kill paths. If one is somehow blocked
        # by GIL contention or scheduler weirdness, the other should
        # still fire.

        def _kill_via_thread():
            import time as _t
            _t.sleep(0.10)  # let the API response return to JS
            try:
                # Best-effort: persist queue state so we don't lose
                # in-flight enqueue work. Bounded by a 200ms join.
                save_t = threading.Thread(
                    target=lambda: self._queues.save_now(),
                    daemon=True)
                save_t.start()
                save_t.join(timeout=0.2)
            except Exception as e:
                _log.debug("swallowed: %s", e)
            # Kill the process. TerminateProcess with current
            # process handle (-1 == GetCurrentProcess()) is
            # equivalent to TerminateProcess(GetCurrentProcess(), 0)
            # but doesn't need an extra call.
            try:
                _ctypes.windll.kernel32.TerminateProcess(
                    _ctypes.c_void_p(-1), 0)
            except Exception as e:
                _log.debug("swallowed: %s", e)
            # Belt-and-suspenders if TerminateProcess somehow
            # didn't kill us.
            try:
                _ctypes.windll.kernel32.ExitProcess(0)
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
            if paths:
                return {"ok": True, "path": paths[0]}
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
            if paths:
                return {"ok": True, "path": paths[0]}
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
        """Clean-shutdown trigger from JS (Ctrl+Q). Destroys the window."""
        try:
            if self._window:
                self._window.destroy()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return {"ok": True}


    def app_restart(self):
        """Re-launch the exe/script, then destroy the current window.

        Used after backup-restore so the freshly-loaded config takes effect.
        """
        try:
            import subprocess
            # Run shutdown cleanup FIRST so child subprocesses are
            # killed, queue state saved, ports released BEFORE the
            # new instance launches. The previous order spawned the
            # new process and then waited 0.6s before tearing down —
            # during that window two YTArchiver instances briefly ran
            # concurrently against the same SQLite WAL DB and config,
            # observed to cause WAL/locked-db errors and the stale
            # instance's queue save clobbering the freshly-restored
            # config.
            try:
                _cb = getattr(self, "_shutdown_cleanup_fn", None)
                if callable(_cb):
                    _cb()
            except Exception as _ce:
                print(f"[app_restart] shutdown_cleanup (pre-launch): {_ce}")
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
            if not paths:
                return {"ok": False, "cancelled": True}
            if isinstance(paths, str):
                path = paths
            else:
                try:
                    if not len(paths):
                        return {"ok": False, "cancelled": True}
                    path = paths[0]
                except (TypeError, IndexError):
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
