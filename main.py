"""
YTArchiver — pywebview shell.

Run with Python 3.13 (pywebview ships; PATH's 3.11 doesn't).
"""

import ctypes
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path


# ── Version header — last updated 4.20.26 5:27pm ───────────────────────
# Surfaced in the window title, /cmd/ping, and the HTML header bar.
# Every rebuild increments by 0.1 (v45.0 -> v45.1 -> ...),
# carrying the ten at v45.9 -> v46.0.
APP_VERSION      = "v55.3"
APP_VERSION_DATE = "4.24.26 1:31am"


# ── Single-instance mutex (matches YTArchiver.py:109) ──────────────────
# Use a DIFFERENT mutex name than the OLD tkinter YTArchiver so both CAN
# coexist on the user's machine if the classic copy is still around.
_INSTANCE_MUTEX = None
if os.name == "nt":
    _INSTANCE_MUTEX = ctypes.windll.kernel32.CreateMutexW(
        None, False, "Local\\YTArchiver_SingleInstance")
    if ctypes.windll.kernel32.GetLastError() == 183: # ERROR_ALREADY_EXISTS
        # Another instance is running — focus its window and exit.
        import ctypes.wintypes as _wt
        _WNDENUMPROC = ctypes.WINFUNCTYPE(ctypes.c_bool, _wt.HWND, _wt.LPARAM)
        def _find_and_focus(hwnd, _):
            _n = ctypes.windll.user32.GetWindowTextLengthW(hwnd)
            if _n > 0:
                _buf = ctypes.create_unicode_buffer(_n + 1)
                ctypes.windll.user32.GetWindowTextW(hwnd, _buf, _n + 1)
                if "YT Archiver" in _buf.value:
                    ctypes.windll.user32.ShowWindow(hwnd, 9) # SW_RESTORE
                    ctypes.windll.user32.SetForegroundWindow(hwnd)
                    return False
            return True
        _cb = _WNDENUMPROC(_find_and_focus)
        ctypes.windll.user32.EnumWindows(_cb, 0)
        print("[YTArchiver] Another instance is already running.")
        sys.exit(0)

try:
    import webview
except ImportError:
    try:
        import ctypes
        ctypes.windll.user32.MessageBoxW(
            None,
            "YTArchiver requires pywebview.\n\n"
            "Install with Python 3.13:\n"
            " Python313\\python.exe -m pip install pywebview",
            "YTArchiver", 0x10,
        )
    except Exception:
        pass
    sys.exit(1)

ROOT = Path(__file__).resolve().parent
WEB = ROOT / "web"
INDEX = WEB / "index.html"

# Import sample log generator (synthesized test data, Phase 0 only) + real config loader
sys.path.insert(0, str(ROOT))
from backend.sample_logs import (
    generate_activity_log_history,
    stream_main_log_sample,
    generate_subs_channels,
    generate_recent_downloads,
    generate_queues,
)
from backend.ytarchiver_config import (
    load_config,
    save_config,
    config_file_exists,
    config_is_writable,
    CONFIG_FILE,
    channels_for_subs_ui,
    recent_for_ui,
    autorun_history_entries_for_ui,
    backup_config_on_start,
)
from backend import subs as subs_backend
from backend import archive_scan
from backend import sync as sync_backend
from backend import metadata as metadata_backend
from backend import index as index_backend
from backend import compress as compress_backend
from backend import reorg as reorg_backend
from backend import window_state as winstate
from backend import autorun as autorun_backend
from backend import net as net_backend
from backend.tray import TrayController
from backend.log_stream import LogStreamer
from backend.transcribe import TranscribeManager
from backend.queues import QueueState


class Api:
    """
    Exposed to JS as window.pywebview.api.*

    Phase 0: only enough to seed logs with test data.
    Later phases: add every YTArchiver action here.
    """

    def __init__(self):
        self._window = None
        self._stream_thread = None
        self._stream_stop = threading.Event()
        self._config = None
        self._log_stream = LogStreamer(None)
        self._sync_thread = None
        self._sync_cancel = threading.Event()
        self._sync_pause = threading.Event() # set == paused; worker blocks
        self._sync_skip = threading.Event() # set == skip current item
        # Pending redownload chain — right-clicking "Continue Redownload"
        # on multiple channels queues each request here, and a single
        # worker thread drains the queue sequentially. Without this
        # list, the second "Continue Redownload" click hit the
        # sync_is_running() gate and silently rejected (user sees
        # "nothing happens"). Items: {ch, folder, new_res, scope_label,
        # rd_task}. Lock protects append/pop race between JS callers
        # and the draining worker.
        self._redwnl_pending: list = []
        self._redwnl_lock = threading.Lock()
        # Pull whisper model from config so Settings changes actually take
        # effect on next launch. Without this the TranscribeManager defaults
        # to "large-v3" regardless of what the user picked in Settings.
        _init_model = (load_config() or {}).get("whisper_model") or "small"
        self._transcribe = TranscribeManager(self._log_stream, model=_init_model)
        # Disk-error watchdog — pauses all tasks on write failure, resumes
        # audit E-60: construct _queues BEFORE DiskErrorMonitor so the
        # monitor's on_pause/on_resume lambdas don't fire on a not-yet-
        # existent self._queues attribute (which would AttributeError
        # inside the watchdog callback and silently fail to pause).
        # audit E-59: wrap queue load in try/except so a corrupt queue
        # file doesn't brick the entire app — log + start empty, and
        # back up the corrupt file for debugging.
        self._queues = QueueState()
        try:
            self._queues.load()
        except Exception as _qe:
            try:
                import shutil as _sh
                from backend.ytarchiver_config import QUEUE_FILE as _QF
                _bak = str(_QF) + ".corrupt"
                _sh.copy2(str(_QF), _bak)
                print(f"[queues] corrupt queue file backed up to {_bak}: {_qe}")
            except Exception:
                print(f"[queues] queue load failed: {_qe}")
            # Reset to a fresh empty state so the app can still launch.
            self._queues = QueueState()

        from backend.disk_watch import DiskErrorMonitor
        self._disk_mon = DiskErrorMonitor(
            self._log_stream,
            on_pause=lambda: (self._sync_pause.set(),
                              self._queues.set_sync_paused(True),
                              self._queues.set_gpu_paused(True),
                              self._transcribe.pause()),
            on_resume=lambda: (self._sync_pause.clear(),
                               self._queues.set_sync_paused(False),
                               self._queues.set_gpu_paused(False),
                               self._transcribe.resume()),
            get_output_dir=lambda: (load_config().get("output_dir") or "").strip(),
        )
        # Hook the monitor into the log stream so it sees every line.
        self._log_stream.add_line_scanner(self._disk_mon.scan_line)
        # Session download counter → tray badge overlay. Bumps on each
        # "Downloading <title>" line emitted by sync.py; resets when sync
        # idles. Mirrors OLD YTArchiver.py:3457 _tray_set_badge.
        self._session_dl_count = 0
        def _dl_scan(text: str) -> None:
            try:
                # Match only the exact sync-emitted "Downloading <title>" prefix;
                # don't false-positive on yt-dlp chatter or other flows.
                if text.lstrip().startswith("Downloading ") and "yt-dlp" not in text:
                    self._session_dl_count += 1
                    tray = getattr(self, "_tray", None)
                    if tray is not None:
                        try: tray.set_badge(self._session_dl_count)
                        except Exception: pass
            except Exception:
                pass
        self._log_stream.add_line_scanner(_dl_scan)
        # Note: self._queues was already constructed + loaded above
        # (moved earlier for audit E-60 — DiskErrorMonitor references it).
        # Connect the transcribe manager to the shared GPU queue so
        # enqueued jobs show up in the Tasks popover + the Auto
        # checkbox actually gates firing. Without this the manager's
        # internal `_jobs` list is invisible to the UI (a bug:
        # auto-transcribe on a channel, no task appeared in GPU Tasks).
        self._transcribe.attach_queues(self._queues, cfg_loader=load_config)
        # Project rule: launching with items already in the queue must never
        # auto-start on its own — the user explicitly hits Resume. Mirrors
        # YTArchiver.py:34190-34200 (_sync_pipeline_restored logic).
        # Also honors a sync_paused=True flag persisted from a prior explicit
        # pause. Forcing the Event ensures the autorun scheduler or any stray
        # trigger can't kick the worker while pre-existing work is pending.
        _had_sync_items = self._queues.has_sync_pipeline_items()
        _had_gpu_items = self._queues.has_gpu_items()
        # Only keep the paused flag if there's actual work to be paused.
        # A stale flag with no queued items is leftover bookkeeping that
        # would otherwise make the global Pause button show "Resume"
        # forever with nothing to resume.
        if _had_sync_items:
            self._sync_pause.set()
            self._queues.set_sync_paused(True)
        else:
            self._queues.set_sync_paused(False)
        if _had_gpu_items:
            try: self._transcribe.pause()
            except Exception: pass
            self._queues.set_gpu_paused(True)
        else:
            self._queues.set_gpu_paused(False)
        if _had_sync_items or _had_gpu_items:
            # Deferred until log pipeline is up — emit via a micro-delay so
            # the notice lands after any startup banner. Matches OLD's
            # "⏸ Sync queue restored — PAUSED" ui_queue post.
            def _emit_restore_notice():
                try:
                    self._log_stream.emit([
                        ["\u23f8 Queue restored \u2014 ", "pauselog"],
                        ["PAUSED (click Resume to continue)\n", "pauselog"],
                    ])
                    self._log_stream.flush()
                except Exception:
                    pass
            threading.Timer(0.5, _emit_restore_notice).start()
        self._queues.add_listener(self._on_queue_changed)
        # Recent-tab live refresh — every download triggers this via
        # sync._record_recent_download so the Recent grid/list updates
        # without needing an app restart.
        try:
            sync_backend.set_recent_changed_hook(self._push_recent_refresh)
        except Exception: pass
        # Autorun scheduler — trigger kicks sync_start_all on the scheduled thread.
        # Passing `sync_busy_fn` so the scheduler can (a) postpone a fire
        # when sync is already running and (b) hold the countdown visible
        # at "Syncing..." until the current sync completes, matching
        # classic's _run_autorun + _schedule_autorun behavior.
        self._autorun = autorun_backend.AutorunScheduler(
            sync_trigger=lambda: self.sync_start_all(),
            stream=self._log_stream,
            sync_busy_fn=lambda: self.sync_is_running(),
        )
        self._reload_config()
        # Blank-name scan: surface any channel whose folder would resolve to
        # `_unnamed/`. The add/update guards stop new blanks from being
        # saved, but a blank channel imported from an older OLD config or
        # legacy migration would sneak past those — without this warning,
        # a subsequent sync would silently route its downloads to the
        # shared `_unnamed/` graveyard folder (that's how 28 videos got
        # orphaned during a prior sync pass). Fail loud, not silent.
        try:
            _blanks = []
            for _ch in (self._config or {}).get("channels", []):
                _nm = (_ch.get("folder_override") or _ch.get("name") or "").strip()
                if not _nm:
                    _blanks.append(_ch.get("url", "<no url>"))
            if _blanks:
                def _emit_blank_warn():
                    try:
                        self._log_stream.emit([
                            ["\u26a0 ", "red"],
                            [f"{len(_blanks)} channel(s) have a blank name. "
                             "Syncs will be refused until you rename them in "
                             "the Subs tab:\n", "red"],
                        ])
                        for _u in _blanks[:10]:
                            self._log_stream.emit([[" ", None], [f"{_u}\n", "dim"]])
                        if len(_blanks) > 10:
                            self._log_stream.emit(
                                [[f" ... and {len(_blanks) - 10} more\n", "dim"]])
                        self._log_stream.flush()
                    except Exception:
                        pass
                threading.Timer(0.8, _emit_blank_warn).start()
        except Exception:
            pass

    def set_window(self, w):
        self._window = w
        self._log_stream.set_window(w)

    def _on_queue_changed(self):
        """Push updated queue state to the UI whenever anything changes."""
        if self._window is None:
            return
        try:
            import json as _json
            payload = self._queues.to_ui_payload()
            # Tray + tooltip uses "actively-working" semantics: is a channel
            # or GPU job currently being processed right now?
            sync_working = bool(payload['sync']) and payload['sync'][0]['status'] == 'running'
            gpu_working = bool(payload['gpu']) and payload['gpu'][0]['status'] == 'running'
            # UI blink state uses "thread-alive" semantics: is the worker
            # thread running (including parked in _wait_if_paused between
            # channels or paused between chunks)? Without this, the icon
            # button briefly flickers to idle-grey between channels and,
            # more importantly, after the user clicks Resume the button
            # disables for up to 250ms until the next channel's current_sync
            # gets set — which made it look like Resume didn't work.
            _sync_t = getattr(self, "_sync_thread", None)
            sync_alive = bool(_sync_t and _sync_t.is_alive())
            try:
                gpu_alive = bool(self._transcribe.is_active())
            except Exception:
                gpu_alive = gpu_working
            # Blink gate: "alive" state (worker thread running / job
            # queued-and-pending) only counts when that queue's Auto
            # is ON. Otherwise queued-but-parked items would make the
            # button blink even though nothing's actually working.
            # `_working` (= status=="running" on the head item) is
            # NEVER gated — if a job IS running, we blink regardless
            # of auto state (e.g., user clicked Start manually on a
            # parked queue).
            try:
                _cfg = self._config or load_config()
            except Exception:
                _cfg = self._config or {}
            _sync_auto = bool(_cfg.get("autorun_sync", False))
            _gpu_auto = bool(_cfg.get("autorun_gpu", False))
            sync_running = sync_working or (_sync_auto and sync_alive)
            gpu_running = gpu_working or (_gpu_auto and gpu_alive)
            js = (
                f"if (window.renderQueues) window.renderQueues({_json.dumps(payload)});"
                f"if (window.setQueueState) window.setQueueState("
                f" {{sync: {{running: {str(sync_running).lower()}, "
                f"paused: {str(payload['sync_paused']).lower()}}},"
                f" gpu: {{running: {str(gpu_running).lower()}, "
                f"paused: {str(payload['gpu_paused']).lower()}}}}});"
            )
            self._window.evaluate_js(js)
            # Drive tray icon spin + tooltip with current task name.
            # Uses the narrower *_working semantics so the tray shows idle
            # (not spinning) while a pass is paused between channels.
            tray = getattr(self, "_tray", None)
            if tray is not None:
                if gpu_working:
                    job = payload['gpu'][0] if payload['gpu'] else {}
                    label = (job.get("kind") or "").title() or "GPU"
                    target = job.get("name") or job.get("title") or ""
                    tip = f"YT Archiver \u2014 {label}"
                    if target:
                        if len(target) > 35:
                            target = target[:32] + "\u2026"
                        tip += f": {target}"
                    tray.start_spin("red")
                    tray.set_tooltip(tip)
                elif sync_working:
                    job = payload['sync'][0] if payload['sync'] else {}
                    target = job.get("name") or job.get("title") or ""
                    tip = "YT Archiver \u2014 Syncing"
                    if target:
                        if len(target) > 35:
                            target = target[:32] + "\u2026"
                        tip += f": {target}"
                    tray.start_spin("blue")
                    tray.set_tooltip(tip)
                else:
                    tray.stop_spin()
                    tray.set_tooltip("YT Archiver \u2014 Idle")
        except Exception:
            pass

    def attach_tray(self, tray):
        self._tray = tray

    def _reload_config(self):
        """Load real YTArchiver config from %APPDATA% if available."""
        if config_file_exists():
            self._config = load_config()
            print(f"[config] loaded real config from {CONFIG_FILE}")
            print(f" channels: {len(self._config.get('channels', []))}")
            print(f" recent: {len(self._config.get('recent_downloads', []))}")
            print(f" log_mode: {self._config.get('log_mode', 'Simple')}")
            print(f" autorun_history: {len(self._config.get('autorun_history', []))}")
            # Push saved autorun interval into the scheduler
            try:
                self._autorun.set_interval_mins(int(self._config.get("autorun_interval", 0) or 0))
            except Exception:
                pass
            # Apply saved log_mode to the LogStreamer so Verbose-mode users
            # don't launch into Simple and wonder where their dim/debug
            # lines went. set_log_mode() only fires when the user flips
            # the dropdown; startup needs its own apply step.
            try:
                self._log_stream.simple_mode = (
                    self._config.get("log_mode", "Simple") == "Simple")
            except Exception:
                pass
        else:
            print(f"[config] real config not found at {CONFIG_FILE}; using synthetic data")
            self._config = None

    # ─── Environment / capabilities ──────────────────────────────────────
    def get_runtime_info(self):
        cfg = self._config or {}
        return {
            "has_real_config": self._config is not None,
            "config_path": str(CONFIG_FILE),
            "log_mode": cfg.get("log_mode", "Simple"),
            "autorun_interval": cfg.get("autorun_interval", 0),
            "last_sync": cfg.get("last_sync", ""),
            "output_dir": (cfg.get("output_dir") or "").strip(),
            "first_run": not bool((cfg.get("output_dir") or "").strip()
                                  and cfg.get("channels")),
            # Subs-table column visibility toggle — piggybacked on runtime
            # info so the JS can apply the class BEFORE the first
            # renderSubsTable call and avoid a flash of the hidden column.
            "show_avg_size": bool(cfg.get("show_avg_size", False)),
            # Recent tab view mode — "list" (legacy table) or "grid"
            # (thumbnail cards). Piggybacked here so the JS can set the
            # initial visibility of either view before the first render
            # and avoid a flash of the wrong view.
            "recent_view_mode": (cfg.get("recent_view_mode") or "grid"),
        }

    # ─── Phase 0: log seeding ────────────────────────────────────────────

    def ping(self):
        return "pong"

    def get_header_version(self):
        """Live version string for the HTML header strip. JS calls this
        on DOMContentLoaded and overwrites #header-version so the label
        can never drift from APP_VERSION — the index.html hardcoded
        placeholder is cosmetic fallback only."""
        return {"version": APP_VERSION, "date": APP_VERSION_DATE}

    def get_activity_log_history(self):
        """
        Return activity-log entries. When the real config is loaded, the
        authoritative source is `config['autorun_history']` — an empty
        list here means the user intentionally cleared the log, so we
        return [] (NOT the fictional sample data). Sample data is only
        used when there's no config at all (preview / demo mode).
        """
        if self._config is not None:
            return autorun_history_entries_for_ui(self._config)
        return generate_activity_log_history()

    def autorun_history_clear(self):
        """Empty config['autorun_history'] and persist. Called by the
        Activity-log Clear button. After this, a relaunch will show an
        empty activity log instead of re-loading the old entries.
        Matches OLD YTArchiver.py:22243 `_clear_autorun_history`.
        """
        from backend.autorun import clear_history as _ch
        res = _ch()
        self._reload_config()
        # bug M-3: push the cleared state to the frontend so the visible
        # activity log clears immediately instead of waiting for the
        # next unrelated push or tab switch. The renderer accepts an
        # empty array; no dedicated `clearActivityLog` shim is needed.
        try:
            if self._window is not None:
                self._window.evaluate_js(
                    "if (window.renderActivityLog) window.renderActivityLog([]);")
        except Exception:
            pass
        return res

    def get_initial_main_log(self):
        """Return a big batch of main-log lines for initial render.

        audit D-36: gated on a `YTARCHIVER_DEMO_MODE=1` env var so the
        fictional sample log lines (FilmFan / GameReviews etc.) never
        leak into a real user's session. In production the caller gets
        an empty list; JS renders a clean blank log until real content
        arrives via the push pipeline.
        """
        if os.environ.get("YTARCHIVER_DEMO_MODE") == "1":
            return stream_main_log_sample(initial=True)
        return []

    def get_subs_channels(self):
        """Return (rows, total_label) for the Subs table. Real data if avail.

        Enriches each row with n_vids / size_gb / size from the disk cache
        (ytarchiver_disk_cache.json) so counts match what YTArchiver shows.
        """
        if self._config is not None and self._config.get("channels"):
            # Enrich a copy so we don't mutate the in-memory config
            import copy as _copy
            cfg_copy = _copy.deepcopy(self._config)
            archive_scan.enrich_channels_with_stats(cfg_copy.get("channels", []))
            return channels_for_subs_ui(cfg_copy)
        # audit D-36: only serve fake channel data in DEMO mode. A
        # user whose real config has no channels yet gets an empty
        # list, not fictional "FilmFan / GameReviews" rows.
        if os.environ.get("YTARCHIVER_DEMO_MODE") == "1":
            rows, total = generate_subs_channels()
            return rows, total
        return [], "0 channels · 0 videos · 0 GB"

    def get_index_summary(self):
        """Return Index tab data: cards + per-channel breakdown."""
        if self._config is None:
            return None
        return archive_scan.index_summary()

    def get_recent_downloads(self):
        """Return real recent-downloads from config. Empty list when none.

        Earlier builds fell back to a synthetic sample set which populated the
        Recent tab with fake videos the user couldn't delete. Removed.
        """
        cfg = self._config if self._config is not None else load_config()
        return recent_for_ui(cfg)

    def _push_recent_refresh(self):
        """Re-fetch recent_downloads and push to the UI's Recent grid/list.

        Called from backend.sync._record_recent_download every time a new
        video lands, so the Recent tab updates live ("does the
        Recents tab not auto update/refresh when a download happens?").
        Safe no-op when the window isn't ready yet.
        """
        if self._window is None:
            return
        try:
            import json as _json
            # Reload config fresh since _record_recent_download just wrote
            # to disk; self._config may be stale.
            try: self._reload_config()
            except Exception: pass
            rows = self.get_recent_downloads() or []
            js = f"window.renderRecentTable && window.renderRecentTable({_json.dumps(rows)});"
            self._window.evaluate_js(js)
        except Exception as e:
            # Best-effort — never let a UI push crash the download pipeline.
            try: self._log_stream.emit_dim(f"(recent refresh push failed: {e})")
            except Exception: pass

    def get_queues(self):
        """Return the real live queue state — empty list when nothing's queued.

        Earlier builds returned synthetic sample rows for "preview feel" when
        both queues were empty; that was a Phase-0 placeholder that made the
        user see unclearable fake items. Removed — if it's empty, show empty.
        """
        return self._queues.to_ui_payload()

    def queue_auto_get(self):
        """Return current state of the Sync + GPU queue "Auto" checkboxes.
        When Auto is on, adding an item to an empty queue auto-starts it.
        Mirrors YTArchiver.py config keys autorun_gpu + autorun_sync.
        """
        cfg = self._config or load_config()
        return {
            "sync": bool(cfg.get("autorun_sync", False)),
            "gpu": bool(cfg.get("autorun_gpu", False)),
        }

    def queue_auto_set(self, kind, enabled):
        """Persist the Auto checkbox state for sync/gpu queue.
        `kind` must be "sync" or "gpu".
        For GPU, also wake the transcribe worker when toggled ON so
        any queued-but-parked jobs actually fire (the worker was
        sleeping on the `_auto_enabled()` gate — it needs a nudge to
        re-check). Matches rule: unchecking Auto keeps incoming
        tasks parked; re-checking releases them.
        """
        if kind not in ("sync", "gpu"):
            return {"ok": False, "error": "kind must be sync or gpu"}
        key = "autorun_gpu" if kind == "gpu" else "autorun_sync"
        try:
            from backend.ytarchiver_config import save_config as _sc
            cfg = load_config()
            cfg[key] = bool(enabled)
            _sc(cfg)
            if self._config is not None:
                self._config[key] = bool(enabled)
            if kind == "gpu" and enabled:
                # Kick the worker in case it was parked on the Auto
                # gate AND there are jobs sitting in the internal list.
                try: self._transcribe._ensure_worker()
                except Exception: pass
                # bug M-1: push the updated queue state to the UI so the
                # Start/Pause button flips to the correct rendered state
                # immediately. Sync path does this via sync_start_all
                # (→ _on_queue_changed); GPU path was missing the push.
                try: self._on_queue_changed()
                except Exception: pass
            elif kind == "sync" and enabled:
                # Symmetric with GPU: if the user toggles Auto ON and
                # the sync queue has items (e.g., they clicked Sync
                # Subbed with Auto off, then changed their mind),
                # spin up the worker so the queue actually drains.
                # Without this, the enqueued tasks would sit idle
                # until the user clicked Start in the popover.
                try:
                    has_items = bool(self._queues.sync)
                    if has_items and not self.sync_is_running():
                        self.sync_start_all(add_downloads_from_config=False)
                except Exception:
                    pass
            return {"ok": True, "enabled": bool(enabled)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def set_log_mode(self, mode):
        """UI toggled log mode. Pushes filter state into LogStreamer + persists (gated)."""
        if mode not in ("Simple", "Verbose"):
            return False
        # Persist to disk FIRST, then mutate in-memory state on success.
        # If the save fails (permission, write-gate off, disk full),
        # leaving self._config unchanged keeps the in-memory state
        # consistent with what the next reload will read.
        persisted = False
        try:
            from backend.ytarchiver_config import save_config as _sc
            cfg = load_config()
            cfg["log_mode"] = mode
            persisted = bool(_sc(cfg))
        except Exception:
            persisted = False
        if persisted:
            if self._config is not None:
                self._config["log_mode"] = mode
            # LogStreamer respects `simple_mode` when filtering dim/verbose lines
            self._log_stream.simple_mode = (mode == "Simple")
        return persisted

    # ─── Autorun scheduler ─────────────────────────────────────────────

    def autorun_set(self, label_or_mins):
        """Accept a label like '30 min' / '1 hr' / 'Off' OR an integer minutes."""
        if isinstance(label_or_mins, str):
            return self._autorun.set_interval_label(label_or_mins)
        try:
            return self._autorun.set_interval_mins(int(label_or_mins))
        except Exception:
            return {"ok": False, "error": "bad value"}

    def autorun_state(self):
        return self._autorun.get_state()

    # ─── Queue mutations (right-click menu) ────────────────────────────

    def queues_sync_remove(self, identifier):
        """Remove a pending sync item by URL (primary) or channel name.
        `sync_remove` matches on `url` field; if the identifier doesn't
        look like a URL, fall through to a second pass that matches on
        `name` / `folder` to handle payloads where the URL was empty."""
        ident = str(identifier or "").strip()
        ok = self._queues.sync_remove(ident)
        if not ok and ident:
            # Try name / folder fallback — loop through and match by
            # the channel's visible label instead of URL.
            with self._queues._lock:
                before = len(self._queues.sync)
                self._queues.sync = [
                    c for c in self._queues.sync
                    if (c.get("name") or c.get("folder") or "") != ident
                ]
                ok = before != len(self._queues.sync)
            if ok:
                self._queues._notify()
                self._queues.save_debounced()
        self._on_queue_changed()
        return {"ok": ok}

    def queues_gpu_remove(self, identifier):
        """Remove a pending GPU job by path (preferred) or bulk_id.
        If the identifier starts with a recognizable bulk-id prefix
        (hex tokens from `chan_transcribe_pending`), fall through to
        `gpu_remove_bulk` to drop every sibling in that bulk at once —
        otherwise the coalesced "Transcribe {ch} (N videos)" row only
        removes a single underlying job per click."""
        ident = str(identifier or "").strip()
        if not ident:
            return {"ok": False}
        ok = self._queues.gpu_remove(ident)
        if not ok:
            # Fallback: treat as bulk_id.
            dropped = self._queues.gpu_remove_bulk(ident)
            ok = dropped > 0
        self._on_queue_changed()
        return {"ok": ok}

    def queues_gpu_remove_bulk(self, bulk_id):
        """Drop every GPU job with a matching `bulk_id` (coalesced row
        removal). Called from the queue-popover context menu when the
        user removes a "Transcribe {ch} (N videos)" row."""
        dropped = self._queues.gpu_remove_bulk(str(bulk_id or ""))
        self._on_queue_changed()
        return {"ok": dropped > 0, "dropped": dropped}

    def queues_sync_reorder(self, identifier, new_index):
        ok = self._queues.sync_reorder(str(identifier or ""), int(new_index or 0))
        self._on_queue_changed()
        return {"ok": ok}

    def queues_gpu_reorder(self, identifier, new_index):
        ok = self._queues.gpu_reorder(str(identifier or ""), int(new_index or 0))
        self._on_queue_changed()
        return {"ok": ok}

    # ─── Subs CRUD (writes go to real %APPDATA%/YTArchiver/ytarchiver_config.json) ───

    def subs_is_writable(self):
        """Whether YTArchiver can write to the config file right now."""
        return config_is_writable()

    def subs_check_duplicate(self, url, folder):
        """Return {dup_url: existing_name|None, dup_folder: existing_name|None}
        so the Add dialog can warn before actually trying to add.
        """
        try:
            cfg = load_config()
            channels = cfg.get("channels", []) or []
            url_norm = (url or "").strip().lower().rstrip("/")
            folder_norm = (folder or "").strip().lower()
            dup_url = None
            dup_folder = None
            for ch in channels:
                u = (ch.get("url") or "").strip().lower().rstrip("/")
                n = (ch.get("name") or "").strip().lower()
                f = (ch.get("folder") or "").strip().lower()
                if url_norm and u == url_norm:
                    dup_url = ch.get("name") or ch.get("folder") or ch.get("url")
                if folder_norm and (n == folder_norm or f == folder_norm):
                    dup_folder = ch.get("name") or ch.get("folder")
            return {"ok": True, "dup_url": dup_url, "dup_folder": dup_folder}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def subs_preview_folder_name(self, url):
        """Probe yt-dlp for a channel URL's metadata so the user can see
        what folder name they'd get BEFORE committing. Mirrors
        YTArchiver.py:17162 do_preview_folder → _probe.

        Runs on a background thread; returns {ok, channel, folder} when
        done via the persisted `_pending_preview` slot, polled via
        `subs_preview_folder_poll`.
        """
        url = (url or "").strip()
        if not url:
            return {"ok": False, "error": "No URL"}
        yt = sync_backend.find_yt_dlp()
        if not yt:
            return {"ok": False, "error": "yt-dlp not found"}
        token = str(id(url)) + "-" + str(int(time.time() * 1000))
        self._pending_previews = getattr(self, "_pending_previews", {})
        self._pending_previews[token] = {"ok": False, "pending": True}
        def _run():
            import subprocess as _sp
            try:
                cmd = [
                    yt, "--flat-playlist", "--print", "channel",
                    "--print", "uploader",
                    *sync_backend._find_cookie_source(),
                    "--playlist-end", "1", url,
                ]
                r = _sp.run(cmd, capture_output=True, text=True, timeout=25,
                            startupinfo=sync_backend._startupinfo,
                            creationflags=(0x08000000 if os.name == "nt" else 0))
                out = (r.stdout or "").strip().splitlines()
                name = (out[0] if out else "").strip() or (out[1] if len(out) > 1 else "").strip()
                if not name:
                    self._pending_previews[token] = {
                        "ok": False, "error": "yt-dlp returned nothing"}
                    return
                folder = sync_backend.sanitize_folder(name)
                self._pending_previews[token] = {
                    "ok": True, "channel": name, "folder": folder}
            except Exception as e:
                self._pending_previews[token] = {"ok": False, "error": str(e)}
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "token": token}

    def subs_preview_folder_poll(self, token):
        """Poll a pending folder-preview result by token. Returns
        {ok, pending} while running, or the final {ok, channel, folder}
        once `_run` sets it.
        """
        pend = getattr(self, "_pending_previews", {})
        res = pend.get(token)
        if res is None:
            return {"ok": False, "error": "unknown token"}
        if res.get("pending"):
            return {"ok": True, "pending": True}
        # One-shot: pop the result
        try: del pend[token]
        except KeyError: pass
        return res

    def subs_add_channel(self, payload):
        """Add a new channel. Returns {ok, channel?, error?}.

        Also kicks off a one-time channel-art fetch in the background so the
        Browse grid shows the avatar/banner immediately — matches OLD
        YTArchiver behavior where adding a channel triggers
        `_fetch_channel_art`.
        """
        try:
            ch = subs_backend.add_channel(payload or {})
            self._reload_config()
            # Fire-and-forget channel-art fetch
            try:
                name = ch.get("name") or ch.get("folder", "")
                if name:
                    self.chan_fetch_art(name, False)
            except Exception:
                pass
            return {"ok": True, "channel": ch,
                    "write_blocked": ch.get("_write_blocked", False)}
        except subs_backend.SubsError as e:
            return {"ok": False, "error": str(e)}
        except Exception as e:
            return {"ok": False, "error": f"Internal error: {e}"}

    def subs_update_channel(self, identity, payload):
        """Update an existing channel matched by identity (url/name/folder)."""
        try:
            ch = subs_backend.update_channel(identity or {}, payload or {})
            self._reload_config()
            # bug S-4: surface folder-rename failures so the user
            # knows the on-disk folder didn't move. Config was kept at
            # the old name (subs.py rollback).
            resp = {"ok": True, "channel": ch,
                    "write_blocked": ch.get("_write_blocked", False)}
            if ch.get("_folder_rename_error"):
                resp["folder_rename_error"] = ch["_folder_rename_error"]
            return resp
        except subs_backend.SubsError as e:
            return {"ok": False, "error": str(e)}
        except Exception as e:
            return {"ok": False, "error": f"Internal error: {e}"}

    def subs_remove_channel(self, identity, delete_files=False):
        """Remove a channel by identity. Pushes the removed dict onto the
        `_removed_channels_stack` so future subs_undo_remove calls can
        unwind in reverse-remove order (newest undo first).

        Previously stored a single slot — removing two channels in
        succession, then undoing once, left the second one unrecoverable.

        If `delete_files=True`, the channel's on-disk folder (videos +
        transcripts + metadata + thumbnails) is recursively deleted. Undo
        only restores the subscription, not the files.
        """
        try:
            # Snapshot before removal for undo
            ch_snap = subs_backend.get_channel(identity or {})
            # bug S-5: refuse delete_files=True while sync is actively
            # processing this channel — shutil.rmtree racing yt-dlp's
            # active writes can crash sync, partially-delete files, or
            # leave orphan temp dirs. Sub is not removed either since
            # that side effect would also surprise a live sync.
            if delete_files and ch_snap:
                _target_url = (ch_snap.get("url") or "").strip()
                try:
                    active_sync = (self._current_sync_channel
                                   if hasattr(self, "_current_sync_channel")
                                   else "")
                    if active_sync and _target_url and active_sync == _target_url:
                        return {
                            "ok": False,
                            "error": ("Sync is currently running on this "
                                      "channel. Cancel or pause the sync "
                                      "first, then retry the delete."),
                        }
                except Exception:
                    pass
            result = subs_backend.remove_channel(
                identity or {}, delete_files=bool(delete_files))
            ok = bool(result.get("ok"))
            if ok and ch_snap and not delete_files:
                if not hasattr(self, "_removed_channels_stack"):
                    self._removed_channels_stack = []
                self._removed_channels_stack.append(ch_snap)
                # Bound the stack so we don't grow unbounded across
                # a long session of repeated removes.
                if len(self._removed_channels_stack) > 50:
                    self._removed_channels_stack = (
                        self._removed_channels_stack[-50:])
            self._reload_config()
            return {
                "ok": ok,
                "write_blocked": not ok,
                "can_undo": bool(ch_snap and ok and not delete_files),
                "deleted_folder": bool(result.get("deleted_folder")),
                "folder_path": result.get("folder_path"),
                "delete_error": result.get("delete_error"),
            }
        except subs_backend.SubsError as e:
            return {"ok": False, "error": str(e)}
        except Exception as e:
            return {"ok": False, "error": f"Internal error: {e}"}

    def subs_undo_remove(self):
        """Restore the most recently removed channel. Pops from a stack
        so multiple consecutive removes can be undone one-at-a-time
        in LIFO order.
        """
        stack = getattr(self, "_removed_channels_stack", None)
        if not stack:
            # Back-compat: check the pre-stack single-slot attr in case
            # a remove happened before this method was upgraded. Shouldn't
            # hit in normal use, but harmless belt-and-suspenders.
            legacy = getattr(self, "_last_removed_channel", None)
            if legacy:
                self._last_removed_channel = None
                ch = legacy
            else:
                return {"ok": False, "error": "Nothing to undo"}
        else:
            ch = stack.pop()
        try:
            payload = dict(ch)
            # add_channel expects 'folder' / 'name'; strip anything that might confuse it
            payload["folder"] = ch.get("name") or ch.get("folder")
            result = subs_backend.add_channel(payload)
            self._reload_config()
            # bug M-2: pop the disk-cache entry for the restored channel
            # so the next Subs-table render triggers a fresh rescan
            # instead of showing "—" or stale counts. invalidate_channel
            # spawns a background rescan that repopulates num_vids/
            # size_bytes.
            try:
                from backend import archive_scan as _as
                _url = (result.get("url") or ch.get("url") or "").strip()
                if _url:
                    _as.invalidate_channel(_url)
            except Exception:
                pass
            return {
                "ok": True,
                "channel": result,
                "more_undo_available": bool(
                    getattr(self, "_removed_channels_stack", None)),
            }
        except subs_backend.SubsError as e:
            # Restore the item to the stack so the user can retry.
            if stack is not None:
                stack.append(ch)
            return {"ok": False, "error": str(e)}
        except Exception as e:
            if stack is not None:
                stack.append(ch)
            return {"ok": False, "error": str(e)}

    def subs_get_channel(self, identity):
        """Fetch a single channel's full record (for populating the edit panel).

        Min/max durations are converted to minutes here to match the UI unit.
        On save, `_payload_to_channel` converts back to seconds for storage.
        """
        try:
            ch = subs_backend.get_channel_for_ui(identity or {})
            return {"ok": True, "channel": ch} if ch else {"ok": False, "error": "Not found"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def subs_test_url(self, url):
        """Probe a channel URL via yt-dlp, return the canonical name + video count."""
        url = (url or "").strip()
        if not url:
            return {"ok": False, "error": "Empty URL"}
        yt = sync_backend.find_yt_dlp()
        if not yt:
            return {"ok": False, "error": "yt-dlp not found"}
        import subprocess as _sp
        from backend.subs import normalize_channel_url
        try:
            normalized = normalize_channel_url(url)
            cookies = sync_backend._find_cookie_source()
            # Get channel name (from first video)
            r1 = _sp.run([yt, "--flat-playlist", "--playlist-end", "1",
                         "--print", "channel", "--no-warnings", "--quiet",
                         *cookies, normalized],
                        capture_output=True, text=True, timeout=15,
                        startupinfo=sync_backend._startupinfo)
            name = (r1.stdout or "").strip().split("\n")[0] or ""
            # Get total count (best-effort)
            r2 = _sp.run([yt, "--flat-playlist", "--print", "%(playlist_count)s",
                         "--playlist-end", "1", "--no-warnings", "--quiet",
                         *cookies, normalized],
                        capture_output=True, text=True, timeout=15,
                        startupinfo=sync_backend._startupinfo)
            count_raw = (r2.stdout or "").strip().split("\n")[0]
            total = int(count_raw) if count_raw.isdigit() else None
            return {"ok": bool(name), "name": name, "total": total, "url": normalized}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def subs_get_defaults(self):
        """Return the user's default channel settings for the Restore-defaults button.

        min_duration / max_duration are returned in MINUTES to match the
        unit the edit-panel input accepts + displays.
        """
        cfg = self._config or load_config()
        # cfg["min_duration"] is SECONDS (180 = 3 min) per YTArchiver's schema
        raw_min_secs = int(cfg.get("min_duration", 180) or 0)
        # audit E-57: read user-configured defaults from config if set,
        # fall back to conservative defaults. Previously these were
        # hardcoded constants so the "Restore defaults" button in the
        # edit panel always clobbered user preference with the constant
        # values — if user set Settings>General auto_transcribe=true
        # and then clicked Restore on a channel, the channel flipped
        # to false regardless of their stated preference.
        return {
            "resolution": cfg.get("default_resolution", "720"),
            "min_duration": max(0, raw_min_secs // 60),
            "max_duration": 0,
            "auto_metadata": bool(cfg.get("default_auto_metadata", True)),
            "auto_transcribe": bool(cfg.get("default_auto_transcribe", False)),
            "compress_enabled": bool(cfg.get("default_compress_enabled", False)),
            "mode": (cfg.get("default_mode") or "new"),
            "folder_org": (cfg.get("default_folder_org") or "years"),
        }

    # ─── Sync ───────────────────────────────────────────────────────────

    # ─── Startup sequence ──────────────────────────────────────────────

    def startup_ready(self):
        """Called by JS on DOMContentLoaded. Kicks off the startup log sequence."""
        if getattr(self, "_startup_fired", False):
            return {"ok": True, "already": True}
        self._startup_fired = True
        threading.Thread(target=self._run_startup_sequence, daemon=True).start()
        return {"ok": True}

    def _run_startup_sequence(self):
        """Three-stage startup log matching YTArchiver's OLD timing:

            Stage 1 (< 2s) --- Startup checks complete, ready to download ---
                              → Sync Subbed + related buttons enable here
            Stage 2 (20-40s) --- Disk scan complete (N ch \u00b7 M vids \u00b7 X TB) ---
                              → staleness-gated: if cache is newer than
                                `disk_scan_staleness_hours`, skip the walk
                                and just report from the cache (instant)
            Stage 3 (mins) --- Browse tab preload complete (N \u00b7 M cached) ---
                              → sweep + per-channel video-list cache warm

        Each stage runs on its own thread and emits its milestone the
        moment it finishes. The "Loading\u00b7" tick line animates in
        verbose mode; simple mode sees only the three green milestones
        as each one lands (the tick is VERBOSE_ONLY).
        """
        import time as _time
        s = self._log_stream

        def _flush_now():
            try: s.flush()
            except Exception: pass

        def _loading(msg):
            # In-place status line (replace-in-place via `startup_loading`).
            # Filtered from simple mode — user sees only the green milestones.
            try:
                s.emit([[f" {msg}\n", "startup_loading"]])
                _flush_now()
            except Exception: pass

        _loading("Loading\u00b7 ")

        # Pending-transcribe journal restore (fast, runs before any milestone).
        try:
            n = self._transcribe.load_pending()
            if n > 0:
                s.emit_text(
                    f" \u2014 Restored {n} pending transcription job(s) from last session.",
                    "simpleline_blue")
                _flush_now()
        except Exception as _pe:
            s.emit_dim(f" (pending-journal restore skipped: {_pe})")
            _flush_now()

        cfg = self._config or load_config()
        # Per-slot startup status — two slots run in parallel during
        # Stage 3 (sweep + preload). "sweep" slot is top of the UI
        # stack (disk scan / indexing new files). "preload" slot is
        # bottom (browse tab preload). Each has {phase, detail}.
        # Animator iterates both and emits whichever are populated.
        dots_state = {
            "i": 0,
            "sweep": {"phase": "Starting up", "detail": ""},
            "preload": {"phase": "", "detail": ""},
        }
        stage3_done = threading.Event()

        def _push_indicator(slot, text):
            """Push text to the given UI slot ("sweep" or "preload"),
            or `None`/`""` to hide it. Visible in Simple + Verbose."""
            try:
                w = self._window
                if w is None:
                    return
                if text:
                    safe = (text.replace("\\", "\\\\").replace("'", "\\'"))
                    w.evaluate_js(
                        f"window._setIndicator && "
                        f"window._setIndicator('{slot}', '{safe}')")
                else:
                    w.evaluate_js(
                        f"window._setIndicator && "
                        f"window._setIndicator('{slot}', null)")
            except Exception:
                pass

        def _animate_dots():
            """Cycle dots on each active slot (sweep / preload). When a
            slot's `phase` is empty, its UI indicator is hidden; when
            populated, we emit `{phase}{dots} {detail}`. Both can be
            active simultaneously (parallel Stage 3)."""
            while not stage3_done.is_set():
                dots_state["i"] = (dots_state["i"] + 1) % 3
                d = ["\u00b7 ", "\u00b7\u00b7 ", "\u00b7\u00b7\u00b7"][dots_state["i"]]
                log_parts = []
                for slot in ("sweep", "preload"):
                    state = dots_state[slot]
                    phase = state.get("phase") or ""
                    detail = state.get("detail") or ""
                    if phase:
                        line = (f"{phase}{d} {detail}" if detail
                                else f"{phase}{d}")
                        _push_indicator(slot, line.strip())
                        log_parts.append(line.strip())
                    else:
                        _push_indicator(slot, None)
                # Log mirror — join active slots with a separator so
                # the verbose log still has a single representative
                # "Loading" line even with two concurrent phases.
                if log_parts:
                    _loading(" \u00b7 ".join(log_parts))
                _time.sleep(0.4)
            # Hide both slots once startup is fully done.
            _push_indicator("sweep", None)
            _push_indicator("preload", None)
        threading.Thread(target=_animate_dots, daemon=True).start()

        def _clear_loading():
            """Remove the in-place Loading line from the DOM."""
            try:
                w = self._window
                if w is not None:
                    w.evaluate_js("window.clearStartupLine && window.clearStartupLine()")
            except Exception:
                pass

        def _fire_ready_js():
            """Tell the UI to un-gray the Sync Subbed / Sync Tasks buttons."""
            try:
                w = self._window
                if w is not None:
                    w.evaluate_js("window._setReady && window._setReady(true)")
            except Exception:
                pass

        # ── Stage 1: Startup checks (immediate — < 2s) ─────────────────
        # No heavy I/O at this stage. Emit the green milestone right away
        # so the user sees the app responded, and flip the Sync buttons
        # active so they can kick off a sync without waiting for the
        # multi-minute preload.
        try:
            s.emit_text("--- Startup checks complete, ready to download ---",
                        "simpleline_green")
            _flush_now()
        except Exception: pass
        _fire_ready_js()

        # ── Stage 2: Disk walk (staleness-gated) ───────────────────────
        def _stage2_disk_walk():
            try:
                from backend.archive_scan import (
                    load_disk_cache, save_disk_cache, archive_totals,
                    scan_all_channels, heal_malformed_cache_entries,
                )
                # issue #134: drop any cache entries that only contain a
                # `sweep_fingerprint` (no num_vids/size_bytes). Those can
                # be left over from older code paths; if present, they
                # show as "—" in Subs table + Index summary. Force a
                # walk when any are found so the next pass fills them in.
                dropped = heal_malformed_cache_entries()
                stale_hours = int(cfg.get("disk_scan_staleness_hours", 24) or 0)
                last_ts = float(cfg.get("last_disk_scan_ts", 0) or 0)
                age_hours = (_time.time() - last_ts) / 3600.0 if last_ts > 0 else 1e9
                do_walk = ((stale_hours <= 0) or (age_hours >= stale_hours)
                           or (last_ts == 0) or (dropped > 0))

                if do_walk:
                    dots_state["sweep"]["phase"] = "Scanning disk"
                    dots_state["sweep"]["detail"] = ""
                    def _on_walk(ch_name, idx, total):
                        clean = (ch_name or "")[:32]
                        dots_state["sweep"]["phase"] = "Scanning disk"
                        dots_state["sweep"]["detail"] = f"{idx+1}/{total} \u2014 {clean}"
                    walked = scan_all_channels(progress_cb=_on_walk)
                    if walked:
                        save_disk_cache(walked)
                        # Persist the timestamp so next boot can decide
                        # staleness. Previously the exception handler
                        # silently swallowed failures — reported
                        # disk scan running every launch, which means
                        # this save wasn't sticking. Now we surface
                        # the outcome so a silent failure (write-gate
                        # off, disk full, permissions, etc.) is visible
                        # in the log instead of mysteriously rescanning
                        # forever.
                        try:
                            c2 = load_config()
                            c2["last_disk_scan_ts"] = _time.time()
                            ok = save_config(c2)
                            if ok:
                                self._config = c2
                            else:
                                s.emit_error(
                                    "Disk scan timestamp save failed \u2014 "
                                    "next launch will re-run the disk scan. "
                                    "Is the config writable?")
                                _flush_now()
                        except Exception as _se:
                            s.emit_error(
                                f"Disk scan timestamp save raised: {_se}")
                            _flush_now()
                else:
                    # Explicit dim log line when we SKIP the scan so
                    # the user can tell it's honoring the staleness
                    # setting. Verbose-only.
                    age_str = (f"{age_hours:.1f}h" if age_hours < 72
                               else f"{age_hours/24:.1f}d")
                    s.emit_dim(
                        f" Disk scan skipped \u2014 last run was {age_str} ago, "
                        f"staleness threshold is {stale_hours}h.")
                    _flush_now()
                # Emit the milestone from the freshly-walked (or still-cached) totals.
                cache = load_disk_cache()
                t = archive_totals(cache)
                if t["videos"] > 0:
                    s.emit_text(
                        f"--- Disk scan complete ({t['channels']} channels \u00b7 "
                        f"{t['videos']:,} videos \u00b7 "
                        f"{t['size_gb']/1024:.1f} TB) ---",
                        "simpleline_green")
                else:
                    s.emit_text("--- Disk scan complete ---", "simpleline_green")
                _flush_now()
                # issue #134: Subs table was rendered at boot using
                # whatever was in the cache at that moment — which for
                # healed/invalidated channels was an empty record that
                # maps to "—". Now that Stage 2 has just written fresh
                # stats, ask the UI to re-fetch. Without this push the
                # user has to click Subs → some other tab → Subs to see
                # the numbers fill in.
                try:
                    if self._window is not None:
                        self._window.evaluate_js(
                            "if (window.refreshSubsTable) "
                            "window.refreshSubsTable();")
                except Exception:
                    pass
            except Exception as e:
                s.emit_error(f"Disk scan error: {e}")
                _flush_now()

        # ── Stage 3: Sweep + preload run IN PARALLEL ───────────────────
        # Previously ran sequentially — user reported several-minute
        # wait before Browse preload even started, because sweep had
        # to walk every channel folder first. These two jobs are
        # independent: sweep walks disk + writes new rows; preload
        # only reads existing rows. SQLite WAL handles concurrent
        # reader+writer. Worst-case race is a cache entry written
        # before a new file's register_video invalidates it — the
        # next click just does a fresh DB query. Safe.
        def _stage3_sweep_and_preload():
            output_dir = (cfg.get("output_dir") or "").strip()
            sweep_result = {"registered": 0, "ingested": 0}

            def _run_sweep():
                if not output_dir:
                    return
                dots_state["sweep"]["phase"] = "Indexing new files"
                dots_state["sweep"]["detail"] = ""
                def _on_sweep(idx, total, name):
                    clean = (name or "")[:32]
                    dots_state["sweep"]["phase"] = "Indexing new files"
                    dots_state["sweep"]["detail"] = f"{idx}/{total} \u2014 {clean}"
                try:
                    r = index_backend.sweep_new_videos(
                        output_dir, cfg.get("channels", []),
                        progress_cb=_on_sweep)
                    sweep_result["registered"] = int(r.get("registered") or 0)
                    sweep_result["ingested"] = int(r.get("ingested") or 0)
                    sweep_result["skipped_unchanged"] = int(
                        r.get("skipped_unchanged") or 0)
                    sweep_result["walked"] = int(r.get("walked") or 0)
                except Exception as _se:
                    s.emit_error(f"Sweep failed: {_se}")
                    _flush_now()
                finally:
                    # Clear the sweep slot so only preload's slot
                    # remains visible if preload's still running.
                    dots_state["sweep"]["phase"] = ""
                    dots_state["sweep"]["detail"] = ""

            def _run_preload():
                if not cfg.get("browse_preload_all", False):
                    return
                ch_names = [(c.get("name") or c.get("folder") or "")
                            for c in cfg.get("channels", [])]
                ch_names = [n for n in ch_names if n]
                dots_state["preload"]["phase"] = "Preloading Browse tab"
                dots_state["preload"]["detail"] = ""
                def _on_preload(idx, total, name):
                    clean = (name or "")[:32]
                    dots_state["preload"]["phase"] = "Preloading Browse tab"
                    dots_state["preload"]["detail"] = f"{idx}/{total} \u2014 {clean}"
                try:
                    index_backend.preload_all_channels(
                        ch_names, progress_cb=_on_preload,
                        limit=100_000)
                except Exception as _pe:
                    s.emit_error(f"Preload failed: {_pe}")
                    _flush_now()
                finally:
                    dots_state["preload"]["phase"] = ""
                    dots_state["preload"]["detail"] = ""

            # Spawn both on their own threads and wait for both.
            t_sweep = threading.Thread(target=_run_sweep, daemon=True)
            t_preload = threading.Thread(target=_run_preload, daemon=True)
            t_sweep.start()
            t_preload.start()
            t_sweep.join()
            t_preload.join()

            # Emit the stage-3 milestone now that both have drained.
            try:
                from backend import archive_scan as _as
                idx = _as.index_summary()
                n_ch = idx["cards"].get("channels", 0) if idx else 0
                n_vids = idx["cards"].get("videos", 0) if idx else 0
                s.emit_text(
                    f"--- Browse tab preload complete ({n_ch} channels \u00b7 "
                    f"{n_vids:,} videos cached) ---",
                    "simpleline_green")
                _flush_now()
            except Exception as e:
                s.emit_error(f"Browse preload error: {e}")
                _flush_now()

            sweep_reg = sweep_result["registered"]
            sweep_ing = sweep_result["ingested"]
            sweep_skip = sweep_result.get("skipped_unchanged", 0)
            sweep_walked = sweep_result.get("walked", 0)
            if sweep_reg > 0 or sweep_ing > 0:
                s.emit_text(
                    f" \u2014 Background sweep: +{sweep_reg} new videos registered, "
                    f"+{sweep_ing} jsonl ingested.",
                    "simpleline_blue")
                _flush_now()
            if sweep_skip:
                s.emit_dim(
                    f" Sweep: {sweep_skip} channel(s) skipped (folder "
                    f"unchanged since last sweep), {sweep_walked} walked.")
                _flush_now()

            # Storage-pressure warning stays at the tail.
            try:
                import shutil as _sh
                cfg2 = self._config or load_config()
                od = (cfg2.get("output_dir") or "").strip()
                if od:
                    du = _sh.disk_usage(od if os.path.isdir(od) else os.path.dirname(od) or ".")
                    pct = du.used / du.total * 100.0 if du.total else 0
                    free_gb = du.free / (1024 ** 3)
                    if pct >= 90:
                        s.emit([
                            ["\u26a0 ", "red"],
                            [f"Archive drive is {pct:.1f}% full \u2014 only {free_gb:.1f} GB free. "
                             f"New syncs may fail.\n", "red"]])
                        _flush_now()
                    elif pct >= 80:
                        s.emit([
                            ["\u26a0 ", "simpleline_compress"],
                            [f"Archive drive at {pct:.1f}% capacity ({free_gb:.1f} GB free).\n", "dim"]])
                        _flush_now()
            except Exception:
                pass

            stage3_done.set()
            _time.sleep(0.05) # let the animator notice
            _clear_loading()

        # Sequential stages on one background thread — each milestone
        # fires the moment its stage finishes.
        def _run_stages():
            _stage2_disk_walk()
            _stage3_sweep_and_preload()
        threading.Thread(target=_run_stages, daemon=True).start()

    def sync_is_running(self):
        return bool(self._sync_thread and self._sync_thread.is_alive())

    def sync_start_all(self, add_downloads_from_config=True):
        """Kick off the sync worker thread.

        `add_downloads_from_config=True` (default, for Sync Subbed):
        enqueue a `kind=download` task for every subscribed channel
        before the worker starts processing.

        `add_downloads_from_config=False`: spawn the worker but don't
        add anything to the queue. Used by metadata/compress auto-fire
        paths that just need to drain whatever's already queued.
        a bug: metadata_queue_all was calling sync_start_all
        (which always added 103 downloads) instead of just starting
        the worker \u2014 so "Queued metadata for 103 channels" turned
        into "Sync pass starting (206 channels)."
        """
        if self.sync_is_running():
            return {"ok": False, "error": "Sync already running"}
        if not sync_backend.find_yt_dlp():
            return {"ok": False, "error": "yt-dlp not found. Install yt-dlp or place yt-dlp.exe next to the app."}
        # Auto-off + fresh "Sync Subbed" click: enqueue every channel
        # but DON'T spawn the worker. User must manually click Start in
        # the Sync Tasks popover (or toggle Auto on). Matches classic
        # behavior where Auto-off means the queue is a shopping list,
        # not a spin-up. The internal metadata/compress path uses
        # add_downloads_from_config=False — those paths already have
        # items queued and just need the worker drained, so they
        # bypass this gate.
        if add_downloads_from_config:
            try:
                cfg = self._config or load_config()
                if not bool(cfg.get("autorun_sync", False)):
                    # Don't double-queue if a prior Sync Subbed already
                    # staged all the download tasks.
                    existing_dl = any(
                        (c.get("kind") or "download").lower() == "download"
                        for c in self._queues.sync)
                    queued = 0
                    if not existing_dl:
                        for ch in cfg.get("channels", []):
                            if self._queues.sync_enqueue(ch):
                                queued += 1
                    self._on_queue_changed()
                    # bug M-7: return both `queued` (new items just
                    # added this call) AND `total_queued` (items sitting
                    # in the queue, including already-queued ones).
                    # Callers that only care about "is anything queued"
                    # can use `total_queued` without guessing.
                    try:
                        total_queued = len(self._queues.sync)
                    except Exception:
                        total_queued = queued
                    return {"ok": True, "started": False,
                            "queued": queued,
                            "total_queued": total_queued}
            except Exception:
                # If anything goes wrong here, fall through to the
                # old behavior (start the worker). Better to over-fire
                # than to silently drop the user's action.
                pass
        # Clear every event that could have been left set by a previous pass:
        # cancel — fired by "Clear Queue" or the Cancel button
        # skip — fired by "Skip current"
        # pause — fired by the Pause dialog, and NEVER auto-cleared before
        # this fix. Without this clear, starting a new pass after
        # a paused-and-cancelled pass would immediately re-enter
        # the "\u23F8 Sync paused at ..." wait loop with no way
        # to resume via the UI because the dialog-Pause button
        # is meant for mid-pass pausing, not from a cold start.
        self._sync_cancel.clear()
        self._sync_skip.clear()
        self._sync_pause.clear()
        # Mirror the pause-clear onto the QueueState flag too. `queue_pause`
        # sets both the threading.Event AND `QueueState.sync_paused`, but
        # only the Event was cleared here — so a new pass saw `sync_paused`
        # still True, the Pause button flipped to "Resume", and clicking
        # it fired `queue_resume` with no effect. Clear both.
        try: self._queues.set_sync_paused(False)
        except Exception: pass
        # Starting sync implies "resume all work" — clear the GPU pause
        # flag too so transcribe jobs dispatched from this pass actually
        # process instead of piling up behind a stale paused flag left
        # over from a prior session.
        try:
            self._queues.set_gpu_paused(False)
            self._transcribe.resume()
        except Exception: pass
        # Start tray icon spin animation so the user can see sync is live
        # even when the window is minimized. Matches YTArchiver.py:3526
        # _tray_start_spin(red=False).
        try:
            if getattr(self, "_tray", None):
                self._tray.start_spin("blue")
                self._tray.set_tooltip("YT Archiver \u2014 Syncing...")
        except Exception:
            pass
        def _run():
            try:
                sync_backend.sync_all(self._log_stream, self._sync_cancel,
                                      queues=self._queues,
                                      transcribe_mgr=self._transcribe,
                                      pause_event=self._sync_pause,
                                      skip_event=self._sync_skip,
                                      add_downloads_from_config=bool(
                                          add_downloads_from_config))
            except Exception as e:
                self._log_stream.emit_error(f"Sync crashed: {e}")
            finally:
                # Stop the tray spin + restore idle tooltip.
                try:
                    if getattr(self, "_tray", None):
                        self._tray.stop_spin()
                        self._tray.set_tooltip("YT Archiver \u2014 Idle")
                        # Clear session download badge — fresh pass next time
                        try: self._tray.set_badge(0)
                        except Exception: pass
                except Exception:
                    pass
                self._session_dl_count = 0
                # TuneShine: clear stale sync-progress so the display leaves
                # the Sync source and returns to weather. Mirrors OLD's
                # _clear_sync_progress() call at the end of every sync path
                # (YTArchiver.py:6972, :7052, :7128, :19671, :23364).
                try: sync_backend.clear_sync_progress()
                except Exception: pass
                # Tell the autorun scheduler this sync completed — it was
                # holding its countdown at "Syncing..." and now resumes
                # counting down from a full interval. Matches classic's
                # `_schedule_autorun(iv)` inside the sync finally
                # (YTArchiver.py:23380).
                try: self._autorun.notify_sync_done()
                except Exception: pass
                self._log_stream.flush()
                self._on_queue_changed()
                # Scheduled second push AFTER this thread's finally
                # actually returns. Without this, _on_queue_changed
                # runs while we're still inside _run, so
                # `self._sync_thread.is_alive()` reads True and the
                # Sync Tasks icon keeps blinking after the queue
                # finishes. this was reported The Timer fires
                # 500ms later when the thread has definitely exited.
                try: threading.Timer(0.5, self._on_queue_changed).start()
                except Exception: pass
        self._sync_thread = threading.Thread(target=_run, daemon=True)
        self._sync_thread.start()
        self._on_queue_changed()
        return {"ok": True, "started": True}

    def sync_cancel(self):
        self._sync_cancel.set()
        return {"ok": True}

    def sync_clear_queue(self):
        """Drop every queued sync task AND fire cancel so the current pass
        stops at the next channel boundary. Distinct from `sync_cancel` in
        that it ALSO empties `QueueState.sync` so the Sync Tasks popover
        goes empty; cancel alone just aborts the in-flight pass while
        leaving queued items in place. UI exposes this as `Clear Queue`.
        """
        removed = 0
        try:
            removed = self._queues.sync_clear()
        except Exception:
            pass
        self._sync_cancel.set()
        self._on_queue_changed()
        return {"ok": True, "removed": removed}

    def gpu_clear_queue(self):
        """Drop every queued GPU task. Current job (if any) is also
        cancelled via `transcribe_cancel_all`."""
        removed = 0
        try:
            removed = self._queues.gpu_clear()
        except Exception:
            pass
        try:
            self._transcribe.cancel_all()
        except Exception:
            pass
        self._on_queue_changed()
        return {"ok": True, "removed": removed}

    def sync_prefetch_channel(self, identity):
        """Probe a channel for total video + live counts before sync starts.
        Best-effort — returns {ok, total, lives, upcoming}.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        def _run():
            try:
                r = sync_backend.prefetch_channel_total(ch.get("url", ""))
                if r.get("ok"):
                    self._log_stream.emit([
                        ["[Prefetch] ", "sync_bracket"],
                        [f"{ch.get('name', '?')}: ", "simpleline_blue"],
                        [f"{r.get('total', 0)} total, "
                         f"{r.get('lives', 0)} live, "
                         f"{r.get('upcoming', 0)} upcoming\n",
                         "simpleline"],
                    ])
                    self._log_stream.flush()
            except Exception:
                pass
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}

    def sync_quick_check(self, identity):
        """Check the first 5 videos of a channel against our archive to see
        if there's anything new. Returns {ok, has_new, checked, fresh_ids}.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        try:
            from backend.channel_cache import get_cached_ids as _cc_load
            cached = _cc_load(ch.get("url", "")) or []
        except Exception:
            cached = []
        return sync_backend.quick_check_new_uploads(
            ch.get("url", ""), cached)

    # ─── Global pause / resume / skip (both queues) ────────────────────

    def queue_pause(self, which="both"):
        """Pause the sync queue, GPU queue, or both (`which` in:
        'sync' | 'gpu' | 'both'). Persisted to queue state."""
        if which in ("sync", "both"):
            self._sync_pause.set()
            self._queues.set_sync_paused(True)
            self._transcribe.pause() # covers mixed queues via TranscribeManager
        if which in ("gpu", "both"):
            self._queues.set_gpu_paused(True)
            self._transcribe.pause()
        self._on_queue_changed()
        return {"ok": True, "paused": which}

    def queue_resume(self, which="both"):
        """Resume a paused queue."""
        if which in ("sync", "both"):
            self._sync_pause.clear()
            self._queues.set_sync_paused(False)
            self._transcribe.resume()
        if which in ("gpu", "both"):
            self._queues.set_gpu_paused(False)
            self._transcribe.resume()
        self._on_queue_changed()
        return {"ok": True, "paused": False}

    def queue_is_paused(self):
        """Return current paused state for each queue."""
        return {
            "sync": bool(self._queues.sync_paused),
            "gpu": bool(self._queues.gpu_paused),
        }

    def sync_skip_current(self):
        """Skip the currently-running sync item and advance to the next.

        Sets a skip flag that the sync loop polls on each channel iteration,
        and also sets the cancel event so the in-flight yt-dlp subprocess for
        the current channel terminates promptly. The sync worker then clears
        the cancel event and moves on to the next channel.
        """
        try:
            self._sync_skip.set()
            # Kill the current yt-dlp process cleanly — the sync loop sees
            # the skip flag and clears the cancel event before the next one.
            self._sync_cancel.set()
            self._log_stream.emit([
                ["[Sync] ", "sync_bracket"],
                ["Skip current channel \u2014 moving on\n", "simpleline"],
            ])
            self._log_stream.flush()
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def gpu_skip_current(self):
        """Skip the currently-running GPU (transcribe / compress / metadata)
        job and advance to the next one.
        """
        try:
            self._transcribe.skip_current()
            self._log_stream.emit([
                ["[GPU] ", "trans_bracket"],
                ["Skip current GPU job \u2014 moving on\n", "simpleline"],
            ])
            self._log_stream.flush()
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ─── Transcribe ─────────────────────────────────────────────────────

    def transcribe_enqueue(self, path, title=""):
        """Queue a video for transcription."""
        ok = self._transcribe.enqueue(path, title)
        return {"ok": ok}

    def transcribe_folder(self):
        """Prompt for a folder, recursively queue every untranscribed video.

        Mirrors YTArchiver.py:16505 _run_manual_transcription_folder. Skips
        files that already have a .jsonl sidecar. Runs the folder walk in a
        background thread so the UI stays responsive.
        """
        try:
            import webview as _wv
            if self._window is None:
                return {"ok": False, "error": "No window"}
            paths = self._window.create_file_dialog(_wv.FOLDER_DIALOG)
            if not paths:
                return {"ok": False, "cancelled": True}
            folder = paths if isinstance(paths, str) else paths[0]
        except Exception as e:
            return {"ok": False, "error": str(e)}

        def _run():
            queued = 0
            skipped = 0
            for dp, _dns, fns in os.walk(folder):
                for fn in fns:
                    if not fn.lower().endswith((".mp4", ".mkv", ".webm", ".m4a", ".mov")):
                        continue
                    video = os.path.join(dp, fn)
                    base = os.path.splitext(video)[0]
                    if os.path.isfile(base + ".jsonl"):
                        skipped += 1
                        continue
                    title = os.path.splitext(fn)[0]
                    self._transcribe.enqueue(video, title)
                    queued += 1
            self._log_stream.emit([
                ["[GPU] ", "trans_bracket"],
                [f"Transcribe folder \u2014 {os.path.basename(folder)}: ", "simpleline_blue"],
                [f"{queued} queued, {skipped} already done\n", "simpleline"],
            ])
            self._log_stream.flush()
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True, "folder": folder}

    def transcribe_retranscribe(self, path, title="", video_id=""):
        """Queue a re-transcription of a video with the current Whisper model.
        Mirrors YTArchiver.py:16369 `_run_retranscribe_job`.

        Transcripts live in AGGREGATED per-folder files (one `.txt` and
        one hidden `.jsonl` per channel / year / month folder, containing
        entries for every video in that folder). So "re-transcribe" is
        NOT a delete-and-rebuild — it's a surgical swap:

          1. Run Whisper on the video file.
          2. In the aggregated `.jsonl`: remove the old line for this
             video_id + title, append the new segments.
          3. In the aggregated `.txt`: remove the old `===…===\\n<body>\\n\\n`
             block, append the new one (preserving date + duration from
             the old header so provenance survives the swap).
          4. Re-ingest the `.jsonl` so FTS reflects the new segments.

        All four steps happen inside the transcribe worker once the
        Whisper pass finishes (see `_write_outputs(retranscribe=True)`
        in transcribe.py). This Api just queues the job.
        """
        if not path or not os.path.isfile(path):
            return {"ok": False, "error": "File not found"}
        # Best-effort derive the video_id if the caller didn't supply one.
        # The replace helpers use it to catch title-drifted stale entries
        # that a title-only match would miss. Lookup order mirrors
        # `_write_outputs`:
        # hint → `[videoId]` suffix on filename → FTS videos table.
        # Also look up the channel name from the index DB so the
        # [Trnscr] activity-log row shows the channel instead of
        # em-dash. "no channel name?"
        vid_id = (video_id or "").strip()
        channel_name = ""
        if not vid_id:
            import re as _re
            stem = os.path.splitext(os.path.basename(path))[0]
            m = _re.search(r"\[([A-Za-z0-9_-]{11})\]\s*$", stem)
            if m:
                vid_id = m.group(1)
        try:
            from backend.index import _open, _db_lock
            conn = _open()
            if conn is not None:
                with _db_lock:
                    row = conn.execute(
                        "SELECT video_id, channel FROM videos WHERE filepath=? "
                        "COLLATE NOCASE LIMIT 1",
                        (os.path.normpath(path),)).fetchone()
                if row:
                    if not vid_id and row[0]:
                        vid_id = row[0]
                    if row[1]:
                        channel_name = row[1]
        except Exception:
            pass
        # Completion hook: push a JS event when the job finishes so the
        # Watch view can refetch the transcript + re-render its source
        # banner (replacing the "approximate" warning with the new
        # Whisper banner). Mirrors ArchivePlayer's `_ytStartProgressPoll`
        # transition-detection pattern but reactive instead of polled.
        _self = self
        _vid = vid_id
        _path = os.path.normpath(path)
        def _on_done(_result):
            try:
                if _self._window is None:
                    return
                import json as _json
                payload = _json.dumps({"video_id": _vid, "filepath": _path})
                _self._window.evaluate_js(
                    f"if (window._onRetranscribeComplete) "
                    f"window._onRetranscribeComplete({payload});")
            except Exception:
                pass
        ok = self._transcribe.enqueue(
            path,
            title or os.path.basename(os.path.splitext(path)[0]),
            channel=channel_name,
            retranscribe=True,
            video_id=vid_id,
            on_complete=_on_done,
        )
        return {"ok": ok, "video_id": vid_id}

    def transcribe_queue_size(self):
        return {"size": self._transcribe.queue_size()}

    def transcribe_cancel_all(self):
        self._transcribe.cancel_all()
        return {"ok": True}

    def transcribe_available(self):
        """Check whether YTArchiver can run whisper (needs Python 3.11)."""
        return {
            "ok": self._transcribe.is_available(),
            "python311": self._transcribe._python311,
            "worker_script_exists": self._transcribe._worker_script.exists(),
        }

    def transcribe_swap_model(self, new_model, persist=True):
        """Swap the whisper model mid-queue. Current job finishes; next job
        picks up the new model.

        `persist=True` (default, used by the GPU popover's "set default"
        dropdown): also saves the new model as `whisper_model` in config
        so future launches use it by default.

        `persist=False` (used by the one-off re-transcribe model picker
        modal): only swaps the runtime model — doesn't touch the
        Settings default. "manual retranscriptions have nothing
        to do with that [settings default] and should have no influence
        on that setting."
        """
        if not new_model or new_model not in ("tiny", "small", "medium", "large-v3"):
            return {"ok": False, "error": "Unsupported model"}
        ok = self._transcribe.swap_model(new_model)
        if ok and persist:
            if self._config is not None:
                self._config["whisper_model"] = new_model
            try:
                from backend.ytarchiver_config import save_config as _sc
                cfg = load_config()
                cfg["whisper_model"] = new_model
                _sc(cfg)
            except Exception:
                pass
        return {"ok": ok, "model": new_model, "persisted": bool(ok and persist)}

    def transcribe_current_model(self):
        """Return the model the transcribe manager will use for the next job."""
        return {"model": self._transcribe.current_model()}

    # ─── Browse tab (reads from transcription_index.db) ────────────────

    def browse_list_channels(self):
        """Return a list of channels with video counts + avatar/banner URLs.

        Avatar + banner paths are filled in from
        `<channel>/.ChannelArt/{avatar,banner}.jpg` when the files exist
        (dropped there by `chan_fetch_art` / metadata sweep). The frontend
        renders the avatar in the channel-grid card background.
        """
        cfg = load_config()
        channels = cfg.get("channels", [])
        cache = archive_scan.load_disk_cache()
        base = (cfg.get("output_dir") or "").strip()
        from backend.sync import channel_folder_name as _cfn
        from backend.channel_art import (
            avatar_path_for, banner_path_for,
            ensure_avatar_thumb, ensure_banner_thumb,
        )
        from backend.index import _file_url
        out = []
        for ch in channels:
            st = archive_scan.stats_for_channel(ch, cache)
            name = ch.get("name") or ch.get("folder", "")
            # Resolve the channel folder and prefer the cached small
            # thumbs (ensure_* creates them lazily if missing). The
            # full-resolution banners are 2+ MP / ~350 KB each; decoding
            # 100 of them on the grid would stall scroll rendering.
            # Thumbs are ~30 KB, decode in ~1 ms, render at the card's
            # real display size. Falls back to the full-res original if
            # Pillow isn't available or the thumbnail write fails.
            avatar_url = None
            banner_url = None
            if base:
                folder = os.path.join(base, _cfn(ch))
                bp = ensure_banner_thumb(folder) or banner_path_for(folder)
                ap = ensure_avatar_thumb(folder) or avatar_path_for(folder)
                if ap: avatar_url = _file_url(ap)
                if bp: banner_url = _file_url(bp)
            out.append({
                "name": name,
                "folder": name,
                "url": ch.get("url", ""),
                "n_vids": st["n_vids"],
                "size_bytes": st["size_bytes"],
                "size_gb": st["size_gb"],
                "size": archive_scan._fmt_size(st["size_bytes"]),
                "avatar_url": avatar_url,
                "banner_url": banner_url,
                # Pending counters for live-count context-menu labels.
                # Mirrors OLD YTArchiver.py:26322 folder-menu labels.
                "transcription_pending": int(ch.get("transcription_pending") or 0),
                "metadata_pending": int(ch.get("metadata_pending") or 0),
            })
        out.sort(key=lambda c: (c["name"] or "").lower())
        return out

    def browse_week_summary(self, days=7):
        """Return {new_videos, new_channels, total_channels} for the summary bar.

        - new_videos: count of videos with added_ts within N days
        - new_channels: count of distinct channels containing those new videos
        - total_channels: len(config['channels'])
        """
        try:
            cfg = load_config()
            total_channels = len(cfg.get("channels", []))
            recent = index_backend.new_videos_in_last_n_days(int(days or 7))
            return {
                "ok": True,
                "new_videos": recent.get("videos", 0),
                "new_channels": recent.get("channels", 0),
                "total_channels": total_channels,
                "channel_list": recent.get("channel_list", []),
            }
        except Exception as e:
            return {"ok": False, "error": str(e),
                    "new_videos": 0, "new_channels": 0, "total_channels": 0}

    def browse_list_videos(self, channel, sort="newest", limit=500):
        """List videos in a channel from the index DB."""
        return index_backend.list_videos_for_channel(channel, sort=sort, limit=limit)

    def browse_get_transcript(self, payload):
        """Fetch transcript segments + source classification for a video.

        Returns a dict (not a list) so the Watch view can pair the segments
        with a source banner ("Whisper" vs "YT auto-captions — approximate").
          { ok: True, segments: [...], source: {source, raw} }
        Where `source` is one of: whisper | yt_captions_punct | yt_captions_raw | unknown
        and `raw` is the original "(WHISPER:small)" / "(YT CAPTIONS)" /
        "(YT+PUNCTUATION)" tag from the Transcript.txt header.
        """
        segs = index_backend.get_segments(
            video_id=payload.get("video_id"),
            jsonl_path=payload.get("jsonl_path"),
            title=payload.get("title"),
        )
        try:
            src_info = self._classify_transcript_source(
                payload.get("title") or "",
                payload.get("jsonl_path") or "",
                payload.get("video_id") or "")
        except Exception:
            src_info = {"source": "unknown", "raw": ""}
        return {"ok": True, "segments": segs, "source": src_info}

    def _classify_transcript_source(self, title, jsonl_path, video_id):
        """Read the aggregated Transcript.txt that covers `title` and pull
        the source tag out of its `===(title), ..., (SOURCE)===` header line.
        Mirrors ArchivePlayer's `_yt_parse_source_tags_in_dir` + classifier.
        Cheap — only reads until we find the matching header block."""
        if not title and not jsonl_path and not video_id:
            return {"source": "unknown", "raw": ""}
        # Resolve jsonl_path from the DB if the caller only gave us a
        # video_id. Watch-view's frontend doesn't know the jsonl_path —
        # it only has video_id + title — so without this lookup the
        # classifier had no search_dirs and returned unknown, which made
        # the source banner silently disappear. Fall back further to the
        # videos.filepath column so we can still find the Transcript.txt
        # by walking up the video file's directory tree.
        if not jsonl_path and video_id:
            try:
                from backend import index as _idx
                conn = _idx._open()
                if conn is not None:
                    with _idx._db_lock:
                        row = conn.execute(
                            "SELECT s.jsonl_path, v.filepath "
                            "FROM videos v LEFT JOIN segments s "
                            " ON s.video_id = v.video_id "
                            "WHERE v.video_id=? LIMIT 1",
                            (video_id,)).fetchone()
                    if row:
                        jsonl_path = row[0] or ""
                        if not jsonl_path and row[1]:
                            # No segments row yet — seed the search dir
                            # from the video file itself.
                            jsonl_path = row[1]
            except Exception:
                pass
        # Find the candidate .txt — same folder as the jsonl_path, or walk
        # up from there. Name pattern: "<channel> [<year>] [<month>] Transcript.txt".
        search_dirs = []
        if jsonl_path:
            cur = os.path.dirname(jsonl_path)
            for _ in range(3):
                if cur and cur not in search_dirs:
                    search_dirs.append(cur)
                parent = os.path.dirname(cur) if cur else ""
                if parent == cur or not parent:
                    break
                cur = parent
        raw_tag = ""
        norm_title = (title or "").strip().lower()
        for d in search_dirs:
            try:
                fns = [f for f in os.listdir(d)
                       if f.endswith("Transcript.txt")]
            except OSError:
                continue
            for fn in fns:
                fp = os.path.join(d, fn)
                try:
                    with open(fp, "r", encoding="utf-8", errors="replace") as fh:
                        # Scan header lines: ===(title), ..., (SOURCE)===
                        for line in fh:
                            if not line.startswith("==="):
                                continue
                            # Extract last bracketed chunk before the closing ===
                            # Format: "===({title}), {date}, {duration}, ({SOURCE})===\n"
                            body = line.strip().rstrip("=").lstrip("=").strip()
                            parts = [p.strip() for p in body.split(",")]
                            if not parts:
                                continue
                            head_title = parts[0].strip()
                            if head_title.startswith("("):
                                head_title = head_title[1:]
                            if head_title.endswith(")"):
                                head_title = head_title[:-1]
                            if head_title.strip().lower() != norm_title:
                                continue
                            # Got a matching header — last (...) is the source.
                            if len(parts) >= 2:
                                tail = parts[-1].strip()
                                if tail.startswith("(") and tail.endswith(")"):
                                    raw_tag = tail[1:-1].strip()
                            break
                except OSError:
                    continue
                if raw_tag:
                    break
            if raw_tag:
                break

        # Classify — mirrors ArchivePlayer _yt_classify_source exactly.
        up = raw_tag.upper()
        if "WHISPER" in up:
            kind = "whisper"
        elif "YT+PUNCT" in up or "YT+PUNCTUATION" in up:
            kind = "yt_captions_punct"
        elif "YT CAPTIONS" in up or up == "YT":
            kind = "yt_captions_raw"
        else:
            kind = "unknown"
        # bug M-10: stop guessing the model name. Transcripts written by
        # v46.0-v46.5 stored bare "(WHISPER)" with no model. Previously we
        # filled in the user's CURRENT default_model — but that misleads
        # users who've since changed their default: a transcript made
        # with large-v3 would show as "Whisper:small" if the user is now
        # on small. Better to surface an honest "model unknown" suffix;
        # user can Re-transcribe to get a real tag baked in.
        if kind == "whisper" and raw_tag.strip().upper() == "WHISPER":
            raw_tag = "WHISPER:unknown"
        return {"source": kind, "raw": raw_tag}

    def browse_search_context(self, payload):
        """Return context around a search hit — used by the Search viewer pane.

        payload = { segment_id, before?, after? }
        Returns { ok, title, channel, segments:[{s,e,t,is_hit}], before_more, after_more }
        where `is_hit` marks the segment that was originally matched + any
        other segments in the same video that match the query.

        Matches YTArchiver.py:29598 viewer pane data flow — grab N segments
        before + hit + N segments after, let the user pull in more with
        Up/Down "Load more" buttons.
        """
        try:
            seg_id = int((payload or {}).get("segment_id") or 0)
            before = int((payload or {}).get("before") or 30)
            after = int((payload or {}).get("after") or 30)
            query = (payload or {}).get("query") or ""
            return index_backend.get_segment_context(seg_id, before, after, query)
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def browse_get_video_metadata(self, payload):
        """Return the aggregated metadata entry for a single video.

        Walks up from the video's filepath to the channel folder, finds the
        appropriate `.{ch_name} ... Metadata.jsonl`, parses it, and returns
        the entry keyed by `video_id`. Includes description + top 50
        comments + view/like counts — feeds the Watch view metadata drawer
        (YTArchiver.py:31164 _player_drawer_frame).
        """
        try:
            filepath = (payload or {}).get("filepath") or ""
            video_id = (payload or {}).get("video_id") or ""
            title = (payload or {}).get("title") or ""
            channel = (payload or {}).get("channel") or ""
            if not video_id:
                import re as _re
                m = _re.search(r"\[([A-Za-z0-9_-]{11})\]",
                               os.path.basename(filepath))
                if m:
                    video_id = m.group(1)
            if not video_id:
                return {"ok": False, "error": "No video_id"}

            # Find the aggregated metadata JSONL — look in the video's own
            # folder first, then walk up to 3 levels (year / year-month).
            from backend.metadata import _read_metadata_jsonl
            if filepath:
                cur = os.path.dirname(filepath)
                for _ in range(4):
                    if not cur:
                        break
                    try:
                        for fn in os.listdir(cur):
                            if fn.startswith(".") and fn.endswith("Metadata.jsonl"):
                                entries = _read_metadata_jsonl(os.path.join(cur, fn))
                                if video_id in entries:
                                    return {"ok": True, "meta": entries[video_id],
                                            "source": os.path.join(cur, fn)}
                    except OSError:
                        pass
                    parent = os.path.dirname(cur)
                    if parent == cur:
                        break
                    cur = parent

            # Fall back: scan the channel folder the hard way
            cfg = self._config or load_config()
            base = (cfg.get("output_dir") or "").strip()
            if base and channel:
                from backend.sync import channel_folder_name as _cfn
                # Look up channel record for folder_override etc.
                ch_dict = None
                for ch in cfg.get("channels", []):
                    if (ch.get("name") or "") == channel:
                        ch_dict = ch; break
                folder = os.path.join(base, _cfn(ch_dict) if ch_dict else channel)
                if os.path.isdir(folder):
                    for dp, _dns, fns in os.walk(folder):
                        for fn in fns:
                            if fn.startswith(".") and fn.endswith("Metadata.jsonl"):
                                entries = _read_metadata_jsonl(os.path.join(dp, fn))
                                if video_id in entries:
                                    return {"ok": True, "meta": entries[video_id],
                                            "source": os.path.join(dp, fn)}
            return {"ok": False, "error": "Metadata not found"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def browse_search(self, query, channel=None, limit=200):
        """FTS5 search across all segments."""
        return index_backend.search_fts(query, channel=channel, limit=limit)

    def browse_graph(self, word, channel=None, bucket="month", normalize=False):
        """Word frequency over time for the Graph sub-mode.

        `word` may be a single term or a comma-separated list for multi-line charts.
        `channel` may be a single channel name, or a list for per-channel overlay.
        `normalize` True divides each bucket by total segments in that bucket
        (then × 1000) so channels of different sizes become comparable —
        matches YTArchiver.py:30427 normalize checkbox.
        """
        # Multi-channel overlay takes priority if both given
        if isinstance(channel, list) and channel and isinstance(word, str):
            w = word.split(",")[0].strip()
            res = index_backend.graph_channel_overlay(w, channel, bucket=bucket)
        elif isinstance(word, str) and "," in word:
            words = [w.strip() for w in word.split(",") if w.strip()]
            if len(words) > 1:
                res = index_backend.graph_multi(words, channel=channel, bucket=bucket)
            else:
                res = index_backend.graph_word_frequency(word, channel=channel, bucket=bucket)
        else:
            res = index_backend.graph_word_frequency(word, channel=channel, bucket=bucket)

        if normalize and isinstance(res, dict) and res.get("labels"):
            # Pull per-bucket total segments for the same channel/bucket so
            # we can divide. Backend helper returns {label: total_segs}.
            try:
                totals = index_backend.bucket_totals(bucket=bucket, channel=channel)
            except Exception:
                totals = {}
            def _norm(labels, values):
                out = []
                for lbl, v in zip(labels, values):
                    tot = totals.get(str(lbl), 0)
                    if tot > 0:
                        out.append(round((v * 1000.0) / tot, 2))
                    else:
                        out.append(0)
                return out
            if "values" in res:
                res["values"] = _norm(res["labels"], res["values"])
                res["normalized"] = True
            elif "series" in res:
                for s in res["series"]:
                    s["values"] = _norm(res["labels"], s["values"])
                res["normalized"] = True
        return res

    def browse_word_cloud(self, channel=None, top_n=120):
        """Return the top-N most-spoken words for the Graph Word Cloud.

        Skips common English stop-words so the cloud surfaces actually-
        distinctive vocabulary. Matches YTArchiver.py Graph sub-mode's
        matplotlib word cloud conceptually.
        """
        try:
            res = index_backend.top_words(channel=channel, top_n=int(top_n or 120))
            return {"ok": True, "words": res}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ─── Deferred livestreams ──────────────────────────────────────────

    def livestreams_list(self):
        try:
            from backend import livestreams as _ls
            return {"ok": True, "items": _ls.list_deferred()}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def livestreams_drop(self, video_id):
        try:
            from backend import livestreams as _ls
            return {"ok": _ls.drop(video_id)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def livestreams_ignore(self, video_id):
        """Permanently skip this deferred livestream/premiere. Adds
        the video_id to the ignore set so future sync passes never
        re-defer it. Mirrors a "don't show this again" action."""
        try:
            from backend import livestreams as _ls
            return {"ok": _ls.ignore(video_id)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def livestreams_snooze(self, seconds):
        """Hide the deferred-livestreams drawer for `seconds` from now.
        UI's "Retry in 24hrs / 1 week" dropdown uses this to suppress
        the drawer without forgetting the entries.
        """
        try:
            from backend import livestreams as _ls
            return {"ok": _ls.snooze_drawer(float(seconds or 0))}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def livestreams_drawer_state(self):
        """Return {snooze_until_ts, now_ts, visible} so the UI can
        decide whether to render the drawer at all."""
        try:
            from backend import livestreams as _ls
            return {"ok": True, **_ls.drawer_state()}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def browse_video_url(self, filepath):
        """Return a file:/// URL the webview can load into a <video> element."""
        try:
            from backend.index import _file_url
            fp = os.path.normpath(filepath or "")
            if not fp or not os.path.isfile(fp):
                return {"ok": False, "error": "File not found"}
            return {"ok": True, "url": _file_url(fp)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def browse_resolve_segment(self, jsonl_path, video_id=None, title=None):
        """Given a transcript segment's .jsonl_path (from FTS search results),
        resolve the actual video file sitting next to it so the Watch view
        can be opened with a proper filepath.

        Returns {ok, filepath, title, channel, video_id}. If the .jsonl's
        sibling video isn't found, also tries the `videos` table by video_id.
        """
        try:
            out = {"ok": False}
            jp = os.path.normpath(jsonl_path or "")
            if jp and os.path.isfile(jp):
                base = os.path.splitext(jp)[0]
                for ext in (".mp4", ".mkv", ".webm", ".m4a", ".mov"):
                    cand = base + ext
                    if os.path.isfile(cand):
                        return {
                            "ok": True,
                            "filepath": cand,
                            "title": title or os.path.basename(base),
                            "video_id": video_id or "",
                        }
            # Fallback — search the videos table by video_id.
            if video_id:
                from backend.index import _open, _db_lock
                conn = _open()
                if conn is not None:
                    with _db_lock:
                        row = conn.execute(
                            "SELECT filepath, title, channel FROM videos WHERE video_id=? LIMIT 1",
                            (video_id,),
                        ).fetchone()
                    if row and row[0] and os.path.isfile(row[0]):
                        return {
                            "ok": True,
                            "filepath": row[0],
                            "title": row[1] or title or "",
                            "channel": row[2] or "",
                            "video_id": video_id,
                        }
            out["error"] = "Video file not found"
            return out
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def browse_open_video(self, filepath):
        """Launch the video in the system default player (VLC if associated)."""
        try:
            fp = os.path.normpath(filepath)
            if not os.path.isfile(fp):
                return {"ok": False, "error": f"File not found: {fp}"}
            if os.name == "nt":
                os.startfile(fp)
            elif sys.platform == "darwin":
                import subprocess
                subprocess.Popen(["open", fp])
            else:
                import subprocess
                subprocess.Popen(["xdg-open", fp])
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def browse_show_in_explorer(self, filepath):
        """Reveal the file in Explorer/Finder."""
        try:
            fp = os.path.normpath(filepath)
            if not os.path.exists(fp):
                return {"ok": False, "error": f"Not found: {fp}"}
            if os.name == "nt":
                import subprocess
                subprocess.Popen(["explorer", "/select,", fp])
            elif sys.platform == "darwin":
                import subprocess
                subprocess.Popen(["open", "-R", fp])
            else:
                import subprocess
                subprocess.Popen(["xdg-open", os.path.dirname(fp)])
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ─── Bookmarks ──────────────────────────────────────────────────────

    def bookmark_list(self):
        # Consistent {ok, rows} shape matching the other bookmark_*
        # methods. Previously returned the raw list, which diverged
        # from the {ok: bool} shape of bookmark_add/remove/update_note
        # and would crash a JS caller that tried to read `.ok` on the
        # array. Legacy callers that iterated directly would stop
        # working — the one known caller has been updated.
        try:
            rows = index_backend.bookmark_list() or []
            return {"ok": True, "rows": rows}
        except Exception as e:
            return {"ok": False, "rows": [], "error": str(e)}

    def bookmark_add(self, payload):
        bid = index_backend.bookmark_add(
            payload.get("video_id", ""), payload.get("title", ""),
            payload.get("channel", ""), float(payload.get("start_time", 0)),
            payload.get("text", ""), payload.get("note", ""),
        )
        return {"ok": bid is not None, "id": bid}

    def bookmark_remove(self, bm_id):
        return {"ok": index_backend.bookmark_remove(int(bm_id))}

    def bookmark_update_note(self, bm_id, note):
        return {"ok": index_backend.bookmark_update_note(int(bm_id), note or "")}

    def index_summary(self):
        """Segments / videos / channels / bookmarks counts from the index DB."""
        return index_backend.summary()

    def index_count_transcripts(self, folder=None):
        """Count transcript + hidden JSONL files under `folder` (default:
        config.output_dir). Used by the "Delete All Transcriptions" 2-step
        confirm on the Index tab. Mirrors YTArchiver.py:31946 _count_files.
        """
        try:
            if not folder:
                cfg = self._config or load_config()
                folder = (cfg.get("output_dir") or "").strip()
            if not folder or not os.path.isdir(folder):
                return {"ok": False, "error": "Folder not found"}
            txt_count = jsonl_count = 0
            total_bytes = 0
            for dp, _dns, fns in os.walk(folder):
                for fn in fns:
                    fl = fn.lower()
                    fp = os.path.join(dp, fn)
                    if ((fl.endswith("transcript.txt") or
                         fl.endswith("transcription.txt"))
                            and not fn.startswith(".")):
                        txt_count += 1
                        try: total_bytes += os.path.getsize(fp)
                        except OSError: pass
                    elif fl.endswith(".jsonl") and fn.startswith("."):
                        jsonl_count += 1
                        try: total_bytes += os.path.getsize(fp)
                        except OSError: pass
            return {"ok": True,
                    "folder": folder,
                    "txt_count": txt_count,
                    "jsonl_count": jsonl_count,
                    "total": txt_count + jsonl_count,
                    "total_bytes": total_bytes}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def index_delete_all_transcripts(self, folder=None, confirm_token=""):
        """PERMANENTLY delete all transcript + hidden JSONL files under
        `folder`. Requires `confirm_token == "YES-DELETE-ALL"` so the JS
        side has to explicitly pass it after the 2-step dialog.

        Mirrors YTArchiver.py:31985 _delete_worker. Runs on a background
        thread; emits per-100-files progress to the log.
        """
        if confirm_token != "YES-DELETE-ALL":
            return {"ok": False, "error": "Missing confirm token"}
        if not folder:
            cfg = self._config or load_config()
            folder = (cfg.get("output_dir") or "").strip()
        if not folder or not os.path.isdir(folder):
            return {"ok": False, "error": "Folder not found"}
        def _run():
            import ctypes as _ctypes
            self._log_stream.emit_text(
                f"\u26A0 Deleting all transcripts under {folder}\u2026",
                "red")
            self._log_stream.flush()
            deleted = 0
            errors = 0
            for dp, _dns, fns in os.walk(folder):
                for fn in fns:
                    fl = fn.lower()
                    fp = os.path.join(dp, fn)
                    hit = False
                    if (fl.endswith("transcript.txt") or fl.endswith("transcription.txt")) \
                            and not fn.startswith("."):
                        hit = True
                    elif fl.endswith(".jsonl") and fn.startswith("."):
                        hit = True
                    if not hit:
                        continue
                    try:
                        # Un-hide so Python can remove it on Windows
                        if os.name == "nt":
                            try:
                                _ctypes.windll.kernel32.SetFileAttributesW(fp, 0x80)
                            except Exception:
                                pass
                        os.remove(fp)
                        deleted += 1
                        if deleted % 100 == 0:
                            self._log_stream.emit_dim(f" deleted {deleted}\u2026")
                            self._log_stream.flush()
                    except Exception:
                        errors += 1
            # Also clear the FTS index — no point keeping ingested data that
            # points to files we just deleted.
            try:
                conn = index_backend._open()
                if conn is not None:
                    with index_backend._db_lock:
                        conn.execute("DELETE FROM segments")
                        conn.execute("DELETE FROM segments_fts")
                        conn.execute("DELETE FROM indexed_files")
                        conn.execute("UPDATE videos SET tx_status='pending'")
                        conn.commit()
            except Exception:
                pass
            self._log_stream.emit_text(
                f"\u2014 Deleted {deleted} transcript file(s), {errors} errors. "
                "FTS index cleared.",
                "simpleline_red")
            self._log_stream.flush()
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}

    def index_unindexed_count(self):
        """Count transcripts on disk that haven't been ingested into FTS yet.

        Walks the output_dir looking for `.{ch_name} ... Transcript.jsonl`
        files whose path isn't in the indexed_files table. Returns the
        count so the Search/Graph views can show an amber warning banner
        (YTArchiver.py:24756 _update_index_warning).
        """
        try:
            cfg = self._config or load_config()
            output_dir = (cfg.get("output_dir") or "").strip()
            if not output_dir or not os.path.isdir(output_dir):
                return {"ok": True, "unindexed": 0}
            # Collect every aggregated JSONL on disk
            on_disk = set()
            for dp, _dns, fns in os.walk(output_dir):
                for fn in fns:
                    if fn.startswith(".") and fn.endswith("Transcript.jsonl"):
                        on_disk.add(os.path.normpath(os.path.join(dp, fn)))
            # Pull the indexed set from the DB
            indexed = set()
            try:
                import sqlite3 as _s
                conn = index_backend._open()
                if conn is not None:
                    with index_backend._db_lock:
                        for (path,) in conn.execute("SELECT path FROM indexed_files").fetchall():
                            if path:
                                indexed.add(os.path.normpath(path))
            except Exception:
                pass
            unindexed = len(on_disk - indexed)
            return {"ok": True, "unindexed": unindexed, "on_disk": len(on_disk),
                    "indexed": len(indexed)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def index_rebuild_fts(self):
        """Drop + rebuild the FTS5 virtual table from scratch. Runs on a
        background thread and emits progress to the log. Returns immediately.
        """
        def _run():
            try:
                self._log_stream.emit_text(
                    "Rebuilding FTS search index from scratch\u2026", "simpleline_blue")
                self._log_stream.flush()
                res = index_backend.rebuild_fts_index()
                if res.get("ok"):
                    self._log_stream.emit_text(
                        f"\u2014 FTS rebuild complete: {res.get('rows_indexed', 0):,} rows indexed.",
                        "simpleline_green")
                else:
                    self._log_stream.emit_error(
                        f"FTS rebuild failed: {res.get('error', 'unknown')}")
            except Exception as e:
                self._log_stream.emit_error(f"FTS rebuild crashed: {e}")
            finally:
                self._log_stream.flush()
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}

    def archive_rescan(self):
        """Run the startup disk-sweep on demand — picks up files added
        manually or while the app was offline. Also prunes DB entries
        whose files no longer exist (fixes stale `(1)` duplicates and
        any yt-dlp intermediate rows that got indexed before the
        `.fNNN-X` filter landed).
        """
        def _run():
            try:
                cfg = self._config or load_config()
                output_dir = (cfg.get("output_dir") or "").strip()
                if not output_dir:
                    self._log_stream.emit_error("No output_dir configured.")
                    return
                # Step 1: prune DB entries for files no longer on disk
                # / 0-byte phantoms / duplicate-id rows. Emit before and
                # after so the user sees it's doing something — # "I click Rescan, nothing happens, then 5 min later
                # nothing changed."
                self._log_stream.emit_text(
                    "Rescan: pruning stale DB entries...",
                    "simpleline_blue")
                self._log_stream.flush()
                pruned = index_backend.prune_missing_videos()
                if (pruned.get("videos_removed") or pruned.get("duplicate_id")
                        or pruned.get("fake_id_cleared")):
                    _parts = []
                    if pruned.get("missing"):
                        _parts.append(f"{pruned['missing']} missing file(s)")
                    if pruned.get("zero_byte"):
                        _parts.append(f"{pruned['zero_byte']} 0-byte phantom(s)")
                    if pruned.get("duplicate_id"):
                        _parts.append(
                            f"{pruned['duplicate_id']} duplicate(s) flagged")
                    if pruned.get("fake_id_cleared"):
                        _parts.append(
                            f"{pruned['fake_id_cleared']} fake video_id(s) cleared")
                    self._log_stream.emit_text(
                        " \u2014 Pruned: " + ", ".join(_parts) + ".",
                        "simpleline_green")
                else:
                    self._log_stream.emit_text(
                        " \u2014 No stale entries to prune.", "dim")
                self._log_stream.flush()
                # Step 2: sweep for new files.
                channels = cfg.get("channels", [])
                self._log_stream.emit_text(
                    f"Rescan: scanning {len(channels)} channel folder(s) "
                    f"for new files...", "simpleline_blue")
                self._log_stream.flush()
                sweep = index_backend.sweep_new_videos(output_dir, channels)
                self._log_stream.emit_text(
                    f"\u2014 Rescan complete: "
                    f"+{sweep.get('registered', 0)} videos, "
                    f"+{sweep.get('ingested', 0)} transcripts ingested.",
                    "simpleline_green")
                # Push a refresh signal to the frontend so the Browse
                # grid re-queries — the backend-side cache is already
                # invalidated but the currently-rendered grid is still
                # HTML from the last fetch. "the videos are
                # still there after rescan."
                if self._window is not None:
                    try:
                        self._window.evaluate_js(
                            "if (window._onArchiveRescanComplete) "
                            "window._onArchiveRescanComplete();")
                    except Exception:
                        pass
            except Exception as e:
                self._log_stream.emit_error(f"Rescan failed: {e}")
            finally:
                self._log_stream.flush()
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}

    # ─── Metadata (manual "Recheck" from context menu) ──────────────────

    def metadata_recheck_channel(self, identity):
        """Slow-path playlist walk + fetch missing metadata.

        Enqueues a `kind: "metadata"` item on the sync queue so the
        Tasks popover shows it and the Sync pause/cancel buttons can
        pause or clear it. The sync worker loop dispatches metadata
        items to `fetch_channel_metadata`. Matches rule:
        "every channel's metadata check should show as its own sync
        task." If the sync thread isn't already running and Sync Auto
        is on, sync_start_all kicks it off; otherwise the item sits
        queued until the user resumes.

        When pre-existing metadata is detected on disk, pops the 3-button
        dialog OLD YTArchiver uses (Check for New / Refresh Counts / Cancel)
        so the user can pick between fast-skip-existing and slow-refresh-all.
        Matches YTArchiver.py:26669 _metadata_choice_dialog.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}

        # Count pre-existing metadata entries under this channel's folder.
        cfg = self._config or load_config()
        base = (cfg.get("output_dir") or "").strip()
        existing_count = 0
        if base:
            try:
                from backend.sync import channel_folder_name as _cfn
                from backend.metadata import _read_metadata_jsonl as _rmj
                cfolder = os.path.join(base, _cfn(ch))
                for dp, _dns, fns in os.walk(cfolder):
                    for fn in fns:
                        if fn.startswith(".") and fn.endswith("Metadata.jsonl"):
                            existing_count += len(_rmj(os.path.join(dp, fn)))
            except Exception:
                existing_count = 0

        ch_name = ch.get("name") or ch.get("folder", "")

        def _enqueue_task(refresh_mode):
            """Drop a `kind: "metadata"` item on the sync queue and
            fire the sync worker if needed."""
            task = dict(ch)
            task["kind"] = "metadata"
            task["refresh"] = bool(refresh_mode)
            try:
                self._queues.sync_enqueue(task)
            except Exception:
                pass
            self._on_queue_changed()
            # Kick the sync worker to drain the queue item we just
            # added. Pass `add_downloads_from_config=False` so the
            # worker does NOT enqueue 103 download tasks on top of our
            # metadata task \u2014 rule: "everything should be in
            # the task list; don't add things the user didn't ask for."
            try:
                cfg = load_config() or {}
                if cfg.get("autorun_sync", False) and not self.sync_is_running():
                    self.sync_start_all(add_downloads_from_config=False)
            except Exception:
                pass

        # If there's existing metadata, prompt the user. Otherwise, just
        # enqueue a normal fetch-new-only pass.
        if existing_count > 0:
            def _prompt_then_enqueue():
                choice = (self.prompt_metadata_already_downloaded(
                    ch_name, existing_count) or {}).get("choice", "skip")
                if choice in ("skip", "cancel"):
                    self._log_stream.emit_text(
                        f" \u2014 Metadata for {ch_name}: cancelled.",
                        "simpleline_pink")
                    self._log_stream.flush()
                    return
                # "append" = Check for New (fast, skip-existing)
                # "overwrite" = Refresh Counts (re-hit every video)
                _enqueue_task(choice == "overwrite")
            threading.Thread(target=_prompt_then_enqueue, daemon=True).start()
        else:
            _enqueue_task(False)
        return {"ok": True, "queued": True}

    # ─── Feature H-14: year-scoped metadata from grid year-head ctx ─────

    def metadata_queue_channel_year(self, identity, year, refresh=False):
        """Queue a year-scoped metadata task for one channel.

        Called from the Browse video-grid year-head right-click menu
        (app.js:4624, parallel to chan_redownload's year scope). Drops
        a `kind: "metadata"` item on the sync queue with
        `scope: {"year": N}` so `fetch_channel_metadata` filters
        on-disk videos to that year before processing. Everything else
        (skip-previously-failed, refresh-counts behavior, Sync Tasks
        popover visibility, pause/cancel) works identically to the
        whole-channel metadata flow — this is just a scope refinement.

        `refresh=False`: fetch metadata for on-disk videos in YEAR that
        don't yet have it (fast, skip-existing).
        `refresh=True`:  re-hit every on-disk video in YEAR to refresh
        views/likes (slow, re-fetch-all).
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        try:
            year_int = int(year)
        except (TypeError, ValueError):
            return {"ok": False, "error": f"Invalid year: {year!r}"}
        task = dict(ch)
        task["kind"] = "metadata"
        task["refresh"] = bool(refresh)
        task["scope"] = {"year": year_int}
        try:
            self._queues.sync_enqueue(task)
        except Exception as e:
            return {"ok": False, "error": str(e)}
        self._on_queue_changed()
        label = "refresh" if refresh else "download"
        ch_name = ch.get("name") or ch.get("folder", "")
        self._log_stream.emit_text(
            f" \u2014 Queued metadata {label} for {ch_name} ({year_int}) "
            f"on Sync Tasks.", "simpleline_pink")
        self._log_stream.flush()
        # Mirror metadata_queue_all's H-7 behavior: always auto-kick
        # the worker when the user explicitly clicked the menu item
        # (don't gate on autorun_sync — they asked for it by clicking).
        try:
            if not self.sync_is_running():
                self.sync_start_all(add_downloads_from_config=False)
        except Exception:
            pass
        return {"ok": True, "queued": True, "year": year_int,
                "refresh": bool(refresh)}

    # ─── Settings > Metadata tab: per-channel refresh status ─────────────

    def get_channel_metadata_status(self):
        """Return per-channel metadata refresh status for Settings > Metadata.

        Powers the table in settings-view-metadata (index.html:753). Each
        row shows the last time views/likes and comments were refreshed
        for that channel, so stale channels float to the top when sorted
        oldest-first.

        Pulls straight from `self._config["channels"]` — the timestamps
        (`last_views_refresh_ts`, `last_comments_refresh_ts`) get stamped
        by `bulk_refresh_views_likes` and `refresh_channel_comments` in
        backend/metadata.py when those paths finish successfully.

        Returns list[dict] with keys: name, folder, url, video_count,
        last_views_refresh_ts, last_comments_refresh_ts.
        """
        cfg = self._config if self._config is not None else load_config()
        channels = list(cfg.get("channels", []) or [])
        if not channels:
            return []
        # Enrich a copy with n_vids so we don't mutate the live config.
        import copy as _copy
        ch_copy = _copy.deepcopy(channels)
        try:
            archive_scan.enrich_channels_with_stats(ch_copy)
        except Exception:
            pass
        # Pull video-id DB counts so the Metadata tab can show a
        # per-channel status indicator (green if every on-disk file
        # has a resolvable video_id, warn if some missing, red if
        # none). Cheap — DB-only count per channel, no disk walk.
        try:
            from backend.metadata import count_video_id_status as _cvids
        except Exception:
            _cvids = None
        rows = []
        for ch in ch_copy:
            _idstats = _cvids(ch) if _cvids else {
                "total": 0, "with_id": 0, "missing": 0}
            rows.append({
                "name": ch.get("name") or ch.get("folder") or "",
                "folder": ch.get("folder") or "",
                "url": ch.get("url") or "",
                "video_count": int(ch.get("n_vids") or 0),
                "last_views_refresh_ts": ch.get("last_views_refresh_ts"),
                "last_comments_refresh_ts": ch.get("last_comments_refresh_ts"),
                "id_total": _idstats.get("total", 0),
                "id_with_id": _idstats.get("with_id", 0),
                "id_missing": _idstats.get("missing", 0),
            })
        # Sort oldest-refresh-first by default so stale channels float up.
        # A missing timestamp (never refreshed) sorts as oldest (ts=0).
        rows.sort(key=lambda r: (r.get("last_views_refresh_ts") or 0,
                                 (r.get("name") or "").lower()))
        return rows

    def metadata_refresh_comments_all(self, only_recent_days=30):
        """Queue a comments-only refresh for every saved channel.

        Bulk version of metadata_refresh_comments_channel (Settings >
        Metadata > "Refresh comments — all channels" button). Defaults
        to a 30-day scope because comments are the slow per-video path
        and most of the value sits in recently-uploaded videos (users
        often catch videos within 30 minutes of upload, before
        comments exist).
        """
        cfg = self._config or load_config()
        channels = sorted(cfg.get("channels", []),
                          key=lambda c: (c.get("name") or "").lower())
        if not channels:
            return {"ok": False, "error": "No channels configured"}
        try:
            _d = int(only_recent_days) if only_recent_days is not None else None
            if _d is not None and _d <= 0:
                _d = None
        except (TypeError, ValueError):
            _d = None
        queued = 0
        for ch in channels:
            try:
                task = dict(ch)
                task["kind"] = "metadata_comments"
                if _d is not None:
                    task["only_recent_days"] = _d
                if self._queues.sync_enqueue(task):
                    queued += 1
            except Exception as e:
                self._log_stream.emit_error(
                    f"Comments enqueue failed for {ch.get('name')}: {e}")
        self._on_queue_changed()
        scope_str = f" (last {_d}d)" if _d else ""
        self._log_stream.emit_text(
            f" \u2014 Queued comments refresh{scope_str} for {queued} "
            f"channel(s) on Sync Tasks.", "simpleline_pink")
        self._log_stream.flush()
        # Auto-kick the worker (H-7 pattern, same as metadata_queue_all).
        try:
            if not self.sync_is_running() and queued > 0:
                self.sync_start_all(add_downloads_from_config=False)
        except Exception:
            pass
        return {"ok": True, "queued": queued, "channels": len(channels),
                "only_recent_days": _d}

    def metadata_backfill_ids_channel(self, identity):
        """Queue a one-shot video_id backfill for a single channel.

        Powers Settings > Metadata's per-row "Fix IDs" button. Lands
        on the sync queue as `kind: "videoid_backfill"` so the user
        sees it in Sync Tasks and can pause / cancel like any other
        sync item. Backend dispatch routes to
        backend.metadata.backfill_video_ids.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        task = dict(ch)
        task["kind"] = "videoid_backfill"
        try:
            self._queues.sync_enqueue(task)
        except Exception as e:
            return {"ok": False, "error": str(e)}
        self._on_queue_changed()
        try:
            if not self.sync_is_running():
                self.sync_start_all(add_downloads_from_config=False)
        except Exception:
            pass
        return {"ok": True, "queued": True}

    def metadata_backfill_ids_all(self, only_missing=True):
        """Queue a video_id backfill for every saved channel. Default
        `only_missing=True` skips channels whose DB already reports
        zero missing IDs — no point paying yt-dlp time for channels
        that don't need it. Pass False to force-queue everything.

        Important for users migrating from the tkinter-era YTArchiver:
        filenames never carried `[id]` brackets and no .info.json
        sidecars got archived, so the index DB's video_id column is
        NULL for thousands of rows. Without the backfill, the bulk
        views/likes refresh path can't match any on-disk file to its
        YouTube row.
        """
        cfg = self._config or load_config()
        channels = sorted(cfg.get("channels", []),
                          key=lambda c: (c.get("name") or "").lower())
        if not channels:
            return {"ok": False, "error": "No channels configured"}
        try:
            from backend.metadata import count_video_id_status as _cvids
        except Exception:
            _cvids = None
        queued = 0
        skipped = 0
        for ch in channels:
            if only_missing and _cvids:
                try:
                    st = _cvids(ch)
                    if st.get("total", 0) > 0 and st.get("missing", 0) == 0:
                        skipped += 1
                        continue
                except Exception:
                    pass
            try:
                task = dict(ch)
                task["kind"] = "videoid_backfill"
                if self._queues.sync_enqueue(task):
                    queued += 1
            except Exception as e:
                self._log_stream.emit_error(
                    f"Backfill enqueue failed for {ch.get('name')}: {e}")
        self._on_queue_changed()
        try:
            if not self.sync_is_running() and queued > 0:
                self.sync_start_all(add_downloads_from_config=False)
        except Exception:
            pass
        return {"ok": True, "queued": queued,
                "skipped_up_to_date": skipped,
                "channels": len(channels)}

    def metadata_refresh_views_channel(self, identity):
        """Per-channel views/likes refresh — no prompt, straight enqueue.

        metadata_recheck_channel prompts the user when existing metadata
        is found (Check for New / Refresh Counts / Cancel). The Settings
        > Metadata table's per-row "Refresh views" action is always
        "refresh counts", so this method skips the prompt and enqueues
        a refresh=True metadata task directly. Uses the fast bulk path
        (bulk_refresh_views_likes) via fetch_channel_metadata's
        refresh=True delegate.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        task = dict(ch)
        task["kind"] = "metadata"
        task["refresh"] = True
        try:
            self._queues.sync_enqueue(task)
        except Exception as e:
            return {"ok": False, "error": str(e)}
        self._on_queue_changed()
        # No "Queued ..." log line — the pass-starting banner
        # emitted by sync.py already states the action, so an
        # earlier "Queued views/likes refresh for X on Sync Tasks."
        # just duplicated info one line above.
        try:
            if not self.sync_is_running():
                self.sync_start_all(add_downloads_from_config=False)
        except Exception:
            pass
        return {"ok": True, "queued": True}

    # ─── Refresh comments (separate per-channel action) ────────────────

    def metadata_refresh_comments_channel(self, identity,
                                           only_recent_days=None):
        """Per-channel comments-only refresh.

        Separate from views/likes refresh because comments require
        per-video yt-dlp calls (no bulk mode exists), so it's always
        the slow path — worth it when pulling community updates for
        videos caught within minutes of upload (no comments at
        download time, decent comments a week later).

        `only_recent_days` optionally scopes to videos uploaded in
        the last N days. None = all videos for the channel.

        Enqueues on the sync queue as a `kind: "metadata_comments"`
        task (dispatched by sync.py:2693's kind-router) so the user
        sees it in the Sync Tasks popover with pause / cancel
        controls like any other sync task.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        task = dict(ch)
        task["kind"] = "metadata_comments"
        if only_recent_days is not None:
            try:
                _d = int(only_recent_days)
                if _d > 0:
                    task["only_recent_days"] = _d
            except (TypeError, ValueError):
                pass
        try:
            self._queues.sync_enqueue(task)
        except Exception as e:
            return {"ok": False, "error": str(e)}
        self._on_queue_changed()
        ch_name = ch.get("name") or ch.get("folder", "")
        scope_str = (f" (last {task['only_recent_days']}d)"
                     if task.get("only_recent_days") else "")
        self._log_stream.emit_text(
            f" \u2014 Queued comments refresh for {ch_name}{scope_str} "
            f"on Sync Tasks.", "simpleline_pink")
        self._log_stream.flush()
        # Auto-kick the worker (same H-7 pattern as metadata_queue_all).
        try:
            if not self.sync_is_running():
                self.sync_start_all(add_downloads_from_config=False)
        except Exception:
            pass
        return {"ok": True, "queued": True,
                "only_recent_days": task.get("only_recent_days")}

    def metadata_queue_all(self, refresh=False):
        """Enqueue every saved channel as a `kind: "metadata"` sync
        task — each one becomes its own row in the Sync Tasks popover
        so the user can see, pause, and cancel. Matches rule
        that background work must always be represented in a task
        list. `refresh=True` triggers the refresh variant (re-hits
        every video) instead of skip-existing.

        Mirrors YTArchiver.py:28296 _secret_download_all_metadata (new metadata)
        and :28326 _secret_refresh_all_metadata (views/likes refresh only).
        """
        cfg = self._config or load_config()
        channels = sorted(cfg.get("channels", []),
                          key=lambda c: (c.get("name") or "").lower())
        if not channels:
            return {"ok": False, "error": "No channels configured"}
        queued = 0
        for ch in channels:
            try:
                task = dict(ch)
                task["kind"] = "metadata"
                task["refresh"] = bool(refresh)
                if self._queues.sync_enqueue(task):
                    queued += 1
            except Exception as e:
                self._log_stream.emit_error(
                    f"Metadata enqueue failed for {ch.get('name')}: {e}")
        self._on_queue_changed()
        label = "refresh" if refresh else "download"
        self._log_stream.emit_text(
            f" \u2014 Queued metadata {label} for {queued} channel(s) "
            f"on Sync Tasks.", "simpleline_pink")
        self._log_stream.flush()
        # bug H-7: always auto-fire the worker when the user explicitly
        # clicked "Queue all metadata" / "Refresh views/likes" — the
        # old code gated this on `autorun_sync=True`, so users with
        # autorun off saw "Queued for N channels" and then nothing
        # happened because the worker never started. Passing
        # add_downloads_from_config=False means we only drain the
        # already-queued metadata items, not trigger a full sync pass.
        try:
            if not self.sync_is_running() and queued > 0:
                self.sync_start_all(add_downloads_from_config=False)
        except Exception:
            pass
        return {"ok": True, "queued": queued, "channels": len(channels)}

    # ─── Transcript drift scan (feature H-2) ──────────────────────────

    def drift_scan_channel(self, identity):
        """Scan one channel's transcript files for drift between the
        aggregated .txt, hidden .jsonl, and FTS index.

        Cross-references three sources:
          - `{Ch} Transcript.txt` (header-delimited entries)
          - hidden `.{Ch} Transcript.jsonl` (one line per segment)
          - segments_fts (FTS5 external-content table)

        Reports three drift categories:
          A. TXT-without-JSONL — entry in .txt but no matching .jsonl
          B. JSONL-without-TXT — segments in .jsonl but no .txt entry
          C. FTS phantoms — global count of orphan FTS rowids (C-9)

        Pure read, no mutations. Apply side is drift_apply_channel."""
        from backend import drift_scan as _ds
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        cfg = self._config or load_config()
        output_dir = (cfg.get("output_dir") or "").strip()
        if not output_dir:
            return {"ok": False, "error": "output_dir is not configured"}
        return _ds.scan_channel(ch, output_dir)

    def drift_apply_channel(self, identity):
        """Apply the three drift fixes for one channel:
          A. Queue Whisper retranscribe for each TXT-without-JSONL entry
             whose video file can be located in the FTS videos table.
          B. Reconstruct TXT entries from .jsonl segments for each
             JSONL-without-TXT entry (body = concat of segment text,
             date = .jsonl mtime, src_tag = "RECOVERED-FROM-JSONL").
          C. Rebuild FTS if phantom count > 0.

        Runs a fresh scan internally so the apply always acts on current
        state."""
        from backend import drift_scan as _ds
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        cfg = self._config or load_config()
        output_dir = (cfg.get("output_dir") or "").strip()
        if not output_dir:
            return {"ok": False, "error": "output_dir is not configured"}

        # Hook: queue a Whisper retranscribe. Wraps self.transcribe_retranscribe
        # so the drift_scan module stays decoupled from the Api class.
        def _enqueue_retranscribe(filepath, title, video_id):
            self.transcribe_retranscribe(filepath, title, video_id)

        result = _ds.apply_channel(
            ch, output_dir,
            enqueue_retranscribe_fn=_enqueue_retranscribe,
            rebuild_fts_fn=_ds.rebuild_fts_index)

        # Surface what happened in the main log so the user has a
        # record (same pattern as other Tools actions).
        if result.get("ok"):
            a = result.get("actions", {})
            parts = []
            if a.get("txt_reconstructed"):
                parts.append(f"{a['txt_reconstructed']} .txt rebuilt")
            if a.get("retranscribe_queued"):
                parts.append(f"{a['retranscribe_queued']} queued for Whisper")
            if a.get("retranscribe_skipped"):
                parts.append(f"{a['retranscribe_skipped']} skipped (video file missing)")
            if a.get("fts_rebuilt"):
                parts.append("FTS rebuilt")
            ch_name = ch.get("name") or ch.get("folder", "")
            if parts:
                self._log_stream.emit_text(
                    f" \u2014 Drift fix for {ch_name}: "
                    f"{' \u00b7 '.join(parts)}.", "simpleline_pink")
            else:
                self._log_stream.emit_text(
                    f" \u2014 Drift fix for {ch_name}: no actions taken.",
                    "dim")
            self._log_stream.flush()
            # Kick the sync worker so retranscribe jobs drain (matches
            # the H-7 pattern used in metadata_queue_*).
            try:
                if (a.get("retranscribe_queued", 0) > 0
                        and not self.sync_is_running()):
                    self.sync_start_all(add_downloads_from_config=False)
            except Exception:
                pass
        return result

    # ─── Compress dry-run (feature F8) ─────────────────────────────────

    def compress_dry_run(self, output_res="720"):
        """Project how much disk space compression WOULD save if enabled
        globally at the given output_res. Walks the index DB (no
        ffprobe), aggregating each channel's total video count +
        cumulative duration, then computes projected post-compress
        size for each of the three quality tiers.

        Returns {
          ok, output_res,
          channels: [{name, videos, hours, current_gb, generous_gb,
                       average_gb, below_gb}],
          total: {videos, hours, current_gb, generous_gb, average_gb, below_gb}
        }
        Purely read-only; does not modify anything.
        """
        try:
            from backend import index as _idx
            from backend import compress as _cpx
            conn = _idx._open()
            if conn is None:
                return {"ok": False, "error": "Index DB unavailable"}
            presets = _cpx._COMPRESS_PRESETS.get(str(output_res))
            if not presets:
                return {"ok": False,
                        "error": f"No compress preset for output_res={output_res!r}"}
            # Aggregate per-channel: videos + duration + size. Duration
            # may be NULL for older rows — treat those as 0 hours so
            # they don't inflate projected savings (worst-case the real
            # savings are larger than reported).
            with _idx._db_lock:
                rows = conn.execute(
                    "SELECT channel, COUNT(*), "
                    "       COALESCE(SUM(duration_s), 0), "
                    "       COALESCE(SUM(size_bytes), 0) "
                    "FROM videos "
                    "WHERE is_duplicate_of IS NULL "
                    "GROUP BY channel "
                    "ORDER BY SUM(size_bytes) DESC"
                ).fetchall()
            # Per-channel projection.
            out_channels = []
            tot_videos = 0
            tot_hours = 0.0
            tot_current = 0.0
            tot_gen = 0.0
            tot_avg = 0.0
            tot_below = 0.0
            for name, n, dur_s, bytes_ in rows:
                hours = float(dur_s) / 3600.0 if dur_s else 0.0
                current_gb = float(bytes_) / (1024 ** 3) if bytes_ else 0.0
                # MB/hr → GB for the whole channel at each tier
                gen_gb = (presets["Generous"] * hours) / 1024
                avg_gb = (presets["Average"] * hours) / 1024
                below_gb = (presets["Below Average"] * hours) / 1024
                out_channels.append({
                    "name": name or "(unknown)",
                    "videos": int(n),
                    "hours": round(hours, 1),
                    "current_gb": round(current_gb, 1),
                    "generous_gb": round(gen_gb, 1),
                    "average_gb": round(avg_gb, 1),
                    "below_gb": round(below_gb, 1),
                })
                tot_videos += int(n)
                tot_hours += hours
                tot_current += current_gb
                tot_gen += gen_gb
                tot_avg += avg_gb
                tot_below += below_gb
            return {
                "ok": True,
                "output_res": str(output_res),
                "channels": out_channels,
                "total": {
                    "videos": tot_videos,
                    "hours": round(tot_hours, 1),
                    "current_gb": round(tot_current, 1),
                    "generous_gb": round(tot_gen, 1),
                    "average_gb": round(tot_avg, 1),
                    "below_gb": round(tot_below, 1),
                },
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ─── Bulk channel operations (feature F7) ──────────────────────────

    def subs_bulk_update(self, names, changes):
        """Apply a small set of whitelisted changes to N channels at once.

        `names` — list of channel folder / display names.
        `changes` — dict with keys from the whitelist below:
          resolution, auto_transcribe, auto_metadata,
          compress_enabled, compress_level, compress_output_res,
          compress_batch_size.
        Returns {ok, updated, failed}. Guarded to the whitelist so the
        UI can't accidentally wipe urls / folder names / anything
        load-bearing.
        """
        if not isinstance(names, list) or not names:
            return {"ok": False, "error": "No channels selected"}
        if not isinstance(changes, dict) or not changes:
            return {"ok": False, "error": "No changes specified"}
        _ALLOWED = {"resolution", "auto_transcribe", "auto_metadata",
                    "compress_enabled", "compress_level",
                    "compress_output_res", "compress_batch_size"}
        clean = {k: v for k, v in changes.items() if k in _ALLOWED}
        if not clean:
            return {"ok": False, "error": "No allowed fields in changes"}
        updated = 0
        failed = []
        for n in names:
            try:
                ch = subs_backend.get_channel({"name": n}) \
                     or subs_backend.get_channel({"folder": n})
                if not ch:
                    failed.append({"name": n, "reason": "not found"})
                    continue
                payload = dict(ch)
                payload.update(clean)
                # Preserve url so update_channel's identity match works
                subs_backend.update_channel(
                    {"url": ch.get("url", ""), "name": ch.get("name", "")},
                    payload)
                updated += 1
            except Exception as e:
                failed.append({"name": n, "reason": str(e)})
        self._reload_config()
        return {"ok": True, "updated": updated, "failed": failed}

    def subs_bulk_delete(self, names, delete_files=False):
        """Delete N channels at once. `delete_files=True` also removes
        the on-disk folders. Returns {ok, deleted, failed}.
        """
        if not isinstance(names, list) or not names:
            return {"ok": False, "error": "No channels selected"}
        deleted = 0
        failed = []
        # Collect URLs so undo can push them all onto the stack.
        if not hasattr(self, "_removed_channels_stack"):
            self._removed_channels_stack = []
        for n in names:
            try:
                ch = subs_backend.get_channel({"name": n}) \
                     or subs_backend.get_channel({"folder": n})
                if not ch:
                    failed.append({"name": n, "reason": "not found"})
                    continue
                # Reuse the single-delete guard for delete_files + active sync
                res = self.subs_remove_channel(
                    {"url": ch.get("url", "")},
                    delete_files=bool(delete_files))
                if res.get("ok"):
                    deleted += 1
                else:
                    failed.append({"name": n,
                                   "reason": res.get("error", "unknown")})
            except Exception as e:
                failed.append({"name": n, "reason": str(e)})
        return {"ok": True, "deleted": deleted, "failed": failed}

    def subs_bulk_queue_metadata(self, names, refresh=False):
        """Queue a metadata fetch (or refresh) for N channels at once.
        Thin wrapper around the per-channel enqueue path that
        `metadata_queue_all` uses.
        """
        if not isinstance(names, list) or not names:
            return {"ok": False, "error": "No channels selected"}
        queued = 0
        failed = []
        for n in names:
            try:
                ch = subs_backend.get_channel({"name": n}) \
                     or subs_backend.get_channel({"folder": n})
                if not ch:
                    failed.append({"name": n, "reason": "not found"})
                    continue
                task = dict(ch)
                task["kind"] = "metadata"
                task["refresh"] = bool(refresh)
                if self._queues.sync_enqueue(task):
                    queued += 1
            except Exception as e:
                failed.append({"name": n, "reason": str(e)})
        self._on_queue_changed()
        # Auto-fire the worker (H-7 pattern — don't gate on autorun)
        try:
            if not self.sync_is_running() and queued > 0:
                self.sync_start_all(add_downloads_from_config=False)
        except Exception:
            pass
        return {"ok": True, "queued": queued, "failed": failed}

    # ─── Channel context actions ───────────────────────────────────────

    def chan_open_folder(self, folder_or_name):
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        # Accept a raw folder name (string) or an identity dict
        name = folder_or_name if isinstance(folder_or_name, str) else (
            folder_or_name.get("name") or folder_or_name.get("folder", ""))
        from backend.sync import sanitize_folder
        path = os.path.join(base, sanitize_folder(name))
        # audit D-38: if the folder doesn't exist yet, don't silently
        # CREATE it. Right-clicking "Open folder" on a channel that
        # has never synced (URL-only subscription) used to materialize
        # an empty directory on the archive drive — polluting the
        # filesystem with empty folders for every channel the user
        # only clicked "Open folder" on.
        if not os.path.isdir(path):
            return {"ok": False,
                    "error": f"Folder not created yet (no sync has run): {path}"}
        try:
            if os.name == "nt":
                os.startfile(path)
            else:
                import subprocess
                subprocess.Popen(["xdg-open" if sys.platform != "darwin" else "open", path])
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def chan_open_url(self, folder_or_name):
        import webbrowser
        name = folder_or_name if isinstance(folder_or_name, str) else (
            folder_or_name.get("name") or folder_or_name.get("folder", ""))
        ch = subs_backend.get_channel({"name": name})
        if not ch or not ch.get("url"):
            return {"ok": False, "error": "URL not found"}
        webbrowser.open(ch["url"])
        return {"ok": True}

    def channel_transcription_stats(self, folder_or_name):
        """Return {total, transcribed, pending, failed} counts for a channel
        from the FTS DB. Used by the edit panel to show coverage at a glance.
        """
        name = folder_or_name if isinstance(folder_or_name, str) else (
            folder_or_name.get("name") or folder_or_name.get("folder", ""))
        try:
            from backend import index as _idx
            stats = _idx.channel_transcription_stats(name)
            return {"ok": True, **stats}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def chan_fetch_art(self, folder_or_name, force=False):
        """Download channel avatar + banner for one channel.

        Writes <channel_folder>/.ChannelArt/{avatar,banner}.jpg. Best-effort —
        runs in a background thread so the UI doesn't block.
        """
        name = folder_or_name if isinstance(folder_or_name, str) else (
            folder_or_name.get("name") or folder_or_name.get("folder", ""))
        ch = subs_backend.get_channel({"name": name})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        from backend.sync import channel_folder_name as _cfn
        folder = os.path.join(base, _cfn(ch))

        def _run():
            from backend import channel_art as _ca
            _ca.fetch_channel_art(ch.get("url", ""), folder, force=bool(force))

        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}

    def chan_art_paths(self, folder_or_name):
        """Return local avatar/banner paths for a channel, if they exist."""
        name = folder_or_name if isinstance(folder_or_name, str) else (
            folder_or_name.get("name") or folder_or_name.get("folder", ""))
        ch = subs_backend.get_channel({"name": name})
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not ch or not base:
            return {"ok": False}
        from backend.sync import channel_folder_name as _cfn
        from backend.channel_art import avatar_path_for, banner_path_for
        from backend.index import _file_url
        folder = os.path.join(base, _cfn(ch))
        ap = avatar_path_for(folder)
        bp = banner_path_for(folder)
        return {
            "ok": True,
            "avatar_url": _file_url(ap) if ap else None,
            "banner_url": _file_url(bp) if bp else None,
        }

    def subs_queue_pending(self):
        """Left-click of the Subs header "↺ Queue Pending" button.

        Walks every subscribed channel; for any with `transcription_pending > 0`
        (or that have new videos without `.jsonl` sidecars), queues a bulk
        transcribe. `chan_transcribe_pending` is real-state aware — it
        scans aggregate transcripts + DB, skips channels already fully
        transcribed, and resets stale counters so the badge self-heals.

        Matches YTArchiver.py:5808 _queue_pending_transcriptions.
        """
        cfg = self._config or load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        from backend.sync import channel_folder_name as _cfn
        tx_added = 0
        mt_added = 0
        for ch in cfg.get("channels", []):
            ch_name = ch.get("name") or ch.get("folder") or ""
            if not ch_name:
                continue
            folder = os.path.join(base, _cfn(ch))
            # Pending transcribe list is authoritative (see sync hook at
            # `append_pending_tx_id` and transcribe-complete hook at
            # `remove_pending_tx_id`). Iterate only channels with a
            # non-empty ID list — stale integer counters don't matter.
            pending_ids = ch.get("pending_tx_ids") or []
            if isinstance(pending_ids, list) and len(pending_ids) > 0:
                r = self.chan_transcribe_pending(ch_name)
                if r and r.get("ok") and r.get("queued", 0) > 0:
                    tx_added += 1
            if int(ch.get("metadata_pending") or 0) > 0:
                # Enqueue directly as a sync-queue metadata task rather
                # than calling `metadata_recheck_channel` (which pops a
                # per-channel confirm dialog when the folder already has
                # metadata — N dialogs for N channels is a terrible
                # bulk-queue UX). The sync worker's metadata branch calls
                # `fetch_channel_metadata(refresh=False)` which
                # skip-appends rather than overwriting, matching the
                # "Check for New" default behavior.
                try:
                    task = dict(ch)
                    task["kind"] = "metadata"
                    task["refresh"] = False
                    if self._queues.sync_enqueue(task):
                        mt_added += 1
                except Exception:
                    pass
        # Fire the sync worker if autorun_sync is on and we just queued
        # metadata work. Pass `add_downloads_from_config=False` so we
        # drain only the metadata items just queued; don't sneak in
        # a full Sync Subbed pass.
        if mt_added or tx_added:
            try:
                self._on_queue_changed()
                cfg2 = load_config() or {}
                if (cfg2.get("autorun_sync", False) and
                        not self.sync_is_running() and mt_added > 0):
                    self.sync_start_all(add_downloads_from_config=False)
            except Exception:
                pass
        parts = []
        if tx_added: parts.append(f"{tx_added} for transcription")
        if mt_added: parts.append(f"{mt_added} for metadata")
        if parts:
            self._log_stream.emit([["[Subs] ", "sync_bracket"],
                                    [f"\u21ba Queued {', '.join(parts)}.\n",
                                     "simpleline_green"]])
        else:
            self._log_stream.emit([["[Subs] ", "sync_bracket"],
                                    ["No channels with pending transcriptions or metadata.\n",
                                     "dim"]])
        self._log_stream.flush()
        return {"ok": True, "transcribe_queued": tx_added,
                "metadata_queued": mt_added}

    def subs_queue_all(self):
        """Right-click of the "↺ Queue Pending" button — queues ALL channels
        for transcribe. Matches YTArchiver.py:5844 _queue_all_transcriptions.
        """
        cfg = self._config or load_config()
        channels = cfg.get("channels", []) or []
        queued = 0
        for ch in channels:
            name = ch.get("name") or ch.get("folder") or ""
            if not name:
                continue
            r = self.chan_transcribe_all(name)
            if r and r.get("ok") and r.get("queued", 0) > 0:
                queued += 1
        self._log_stream.emit([["[Subs] ", "sync_bracket"],
                                [f"\u21ba Queued all: {queued} channels\n",
                                 "simpleline_green"]])
        self._log_stream.flush()
        return {"ok": True, "queued": queued}

    def chan_transcribe_pending(self, folder_or_name):
        """Queue every video in this channel's `pending_tx_ids` list.

        Authoritative source: `channel.pending_tx_ids` — populated by
        sync.py when a video downloads onto a channel whose
        auto_transcribe flag was off at that moment. No folder scan, no
        title matching, no heuristics. Every ID in that list corresponds
        to a real, concrete file the sync pipeline knows about; we
        resolve filepath via the FTS index and enqueue.

        Matches the v47.7 design spec: "Keep a log of the video IDs
        that are skipped, and have the queue pending button DIRECTLY
        snipe the info we need."
        """
        name = folder_or_name if isinstance(folder_or_name, str) else (
            folder_or_name.get("name") or folder_or_name.get("folder", ""))
        ch = subs_backend.get_channel({"name": name})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        pending_ids = list(ch.get("pending_tx_ids") or [])
        if not pending_ids:
            self._log_stream.emit([
                ["[GPU] ", "trans_bracket"],
                [f"Queue pending for {name}: ", "simpleline_blue"],
                ["nothing pending.\n", "simpleline"],
            ])
            self._log_stream.flush()
            return {"ok": True, "queued": 0, "skipped": 0}

        # Skip IDs whose file is already queued / running so rapid
        # double-clicks don't stack duplicates.
        queued_paths = set()
        try:
            with self._transcribe._jobs_lock:
                for j in self._transcribe._jobs:
                    p = j.get("path") or ""
                    if p:
                        queued_paths.add(os.path.normpath(p).lower())
            cj = self._transcribe._current_job
            if cj:
                p = cj.get("path") or ""
                if p:
                    queued_paths.add(os.path.normpath(p).lower())
        except Exception:
            pass

        # Resolve each ID → filepath via the FTS index, in one shot.
        id_to_path: dict = {}
        unresolved: list = []
        try:
            from backend.index import _open as _idx_open
            conn = _idx_open()
            if conn is not None:
                placeholders = ",".join(["?"] * len(pending_ids))
                rows = conn.execute(
                    f"SELECT video_id, filepath, title FROM videos "
                    f"WHERE video_id IN ({placeholders})",
                    pending_ids,
                ).fetchall()
                for r in rows:
                    vid, fp, title = r[0], (r[1] or ""), (r[2] or "")
                    if vid and fp:
                        id_to_path[vid] = (fp, title)
        except Exception:
            pass

        queued = 0
        skipped = 0
        bulk = []  # (video_path, title)
        for vid in pending_ids:
            info = id_to_path.get(vid)
            if not info:
                unresolved.append(vid)
                continue
            fp, title = info
            if not fp or not os.path.isfile(fp):
                unresolved.append(vid)
                continue
            if os.path.normpath(fp).lower() in queued_paths:
                skipped += 1
                continue
            bulk.append((fp, title or os.path.splitext(os.path.basename(fp))[0]))

        if bulk:
            import uuid as _uuid
            bulk_id = _uuid.uuid4().hex[:12]
            bulk_total = len(bulk)
            for idx, (video, title) in enumerate(bulk):
                self._transcribe.enqueue(video, title, channel=name,
                                         bulk_id=bulk_id, bulk_total=bulk_total,
                                         bulk_index=idx)
                queued += 1

        self._log_stream.emit([
            ["[GPU] ", "trans_bracket"],
            [f"Queue pending for {name}: ", "simpleline_blue"],
            [f"{queued} queued"
             + (f", {skipped} already in queue" if skipped else "")
             + (f", {len(unresolved)} unresolved" if unresolved else "")
             + "\n", "simpleline"],
        ])
        # Log unresolved IDs so the user can see what's dangling — a
        # deleted-since-download video, or an FTS index gap. Dropping
        # those from the list keeps the counter honest.
        if unresolved:
            for u in unresolved:
                self._log_stream.emit([
                    ["[GPU] ", "trans_bracket"],
                    [f"  \u2014 dropping unresolved id: {u}\n", "dim"],
                ])
            try:
                cfg2 = load_config()
                for _ch in cfg2.get("channels", []):
                    if (_ch.get("name") or "") != name:
                        continue
                    ids = _ch.get("pending_tx_ids") or []
                    ids = [x for x in ids if x not in unresolved]
                    _ch["pending_tx_ids"] = ids
                    _ch["transcription_pending"] = len(ids)
                    if not ids:
                        _ch["transcription_complete"] = True
                    break
                from backend.ytarchiver_config import save_config as _sc
                # bug M-9: check the save result. Without this, a
                # write-gate-off / disk-full save would silently leave
                # the stale unresolved IDs on disk; next call reloads
                # them and the same "unresolved" list comes back.
                if not _sc(cfg2):
                    self._log_stream.emit_dim(
                        " (unresolved-id cleanup not persisted — config write-gate off?)")
                else:
                    # Refresh in-memory config so the next caller reads
                    # the pruned list instead of the pre-prune state.
                    self._config = cfg2
            except Exception:
                pass
        self._log_stream.flush()
        return {"ok": True, "queued": queued, "skipped": skipped,
                "unresolved": len(unresolved)}

    def chan_transcribe_all(self, folder_or_name, combined=None):
        """Walk the channel folder for videos without .jsonl sidecars and queue each for whisper.

        `combined` controls per-year output:
          - None : decide from existing transcripts (first-time → may need UI choice)
          - True : write one combined `{ch} Transcript.txt` at the channel root
          - False : follow organization (per-year files)

        If this is the first-time transcribing an organized channel (split_years=True)
        AND `combined` is unspecified, returns `{ok: True, needs_choice: True,
        org_label: "Year" | "Year/Month"}` so the UI can show the OLD-style
        "Follow organization / Combined" radio dialog (YTArchiver.py:5919).
        The UI should then re-call with combined=True or False.
        """
        name = folder_or_name if isinstance(folder_or_name, str) else (
            folder_or_name.get("name") or folder_or_name.get("folder", ""))
        ch = subs_backend.get_channel({"name": name})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        from backend.sync import channel_folder_name
        folder = os.path.join(base, channel_folder_name(ch))
        split_years = bool(ch.get("split_years"))
        split_months = bool(ch.get("split_months"))

        # First-time-choice logic: for organized channels with no existing
        # transcripts, ask the user whether to follow org or combine.
        # Match OLD's dialog at YTArchiver.py:5918-5952.
        if combined is None and split_years:
            has_existing = False
            if os.path.isdir(folder):
                for dp, _dns, fns in os.walk(folder):
                    if any(fn.endswith("Transcript.txt") or
                           fn.endswith("Transcript.jsonl") for fn in fns):
                        has_existing = True
                        break
            if not has_existing:
                org_label = "Year/Month" if split_months else "Year"
                return {"ok": True, "needs_choice": True,
                        "channel": name, "org_label": org_label}
            # Has existing transcripts → follow whatever org they picked last time
            combined = False
        elif combined is None:
            combined = True # unorganized channels always combine

        # Build a dict of already-transcribed titles (normalized) + stored
        # video IDs from every aggregate Transcript.txt under this folder.
        # The scan is now permissive (any *Transcript.txt, unicode-normalized
        # keys, dual plain/with-id variants) so minor string differences
        # between filename and stored title stop producing false
        # "needs transcribing" hits.
        from backend.transcribe import (_scan_existing_transcript_titles,
                                         _norm_title)
        already = _scan_existing_transcript_titles(folder, name)
        done_vids = {vid for (_raw, vid) in already.values() if vid}

        skipped = 0
        bulk = []  # (video_path, plain_title)
        import re as _re
        for dp, _dns, fns in os.walk(folder):
            for fn in fns:
                if not fn.lower().endswith((".mp4", ".mkv", ".webm", ".m4a")):
                    continue
                video = os.path.join(dp, fn)
                base_path = os.path.splitext(video)[0]
                title = os.path.splitext(fn)[0]
                plain_title = _re.sub(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$",
                                      "", title) or title
                vid_m = _re.search(r"\[([A-Za-z0-9_-]{11})\]", title)
                vid_id = vid_m.group(1) if vid_m else ""
                # Per-video legacy .jsonl sidecar (OLD format)
                if os.path.isfile(base_path + ".jsonl"):
                    skipped += 1
                    continue
                # Aggregate title match (normalized, either with or
                # without `[videoId]` tail)
                if (_norm_title(plain_title) in already
                        or _norm_title(title) in already):
                    skipped += 1
                    continue
                # Video-ID match against any aggregate-stored ID
                if vid_id and vid_id in done_vids:
                    skipped += 1
                    continue
                bulk.append((video, plain_title))

        queued = 0
        if bulk:
            import uuid as _uuid
            bulk_id = _uuid.uuid4().hex[:12]
            bulk_total = len(bulk)
            for idx, (video, plain_title) in enumerate(bulk):
                # Pass combined flag through so the transcribe worker writes
                # to the right aggregated file. Respects the user's choice
                # even when it conflicts with the channel's split_years flag.
                # bulk_id coalesces the popover display.
                self._transcribe.enqueue(video, plain_title, channel=name,
                                         combined=bool(combined),
                                         bulk_id=bulk_id, bulk_total=bulk_total,
                                         bulk_index=idx)
                queued += 1
        self._log_stream.emit([
            ["[GPU] ", "trans_bracket"],
            [f"Transcribe all for {name}: ", "simpleline_blue"],
            [f"{queued} queued, {skipped} already transcribed"
             + (" (combined)" if combined and split_years else ""),
             "simpleline"],
            ["\n", None],
        ])
        self._log_stream.flush()
        return {"ok": True, "queued": queued, "skipped": skipped,
                "combined": bool(combined)}

    def chan_redownload_progress_peek(self, folder_or_name):
        """Check whether a channel has a saved redownload-in-progress file.
        Returns {ok, pending: bool, resolution, done_ids_count} so the UI
        can offer a "Continue redownload" button in the edit panel.
        Matches YTArchiver.py:5473 _has_pending_redownload."""
        try:
            name = folder_or_name if isinstance(folder_or_name, str) else (
                folder_or_name.get("name") or folder_or_name.get("folder", ""))
            if not name:
                return {"ok": False, "error": "channel name required"}
            ch = subs_backend.get_channel({"name": name})
            if not ch:
                return {"ok": False, "error": "Channel not found"}
            cfg = self._config or load_config()
            base = (cfg.get("output_dir") or "").strip()
            if not base:
                return {"ok": True, "pending": False}
            from backend.sync import channel_folder_name as _cfn
            folder = os.path.join(base, _cfn(ch))
            pp = os.path.join(folder, "_redownload_progress.json")
            if not os.path.isfile(pp):
                return {"ok": True, "pending": False}
            try:
                import json as _j
                with open(pp, "r", encoding="utf-8") as f:
                    data = _j.load(f)
                done_n = len(data.get("done_ids") or [])
                res = data.get("resolution") or ""
                return {"ok": True, "pending": True,
                        "resolution": res, "done": done_n}
            except Exception:
                return {"ok": True, "pending": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def chan_cancel_redownload(self, folder_or_name):
        """Cancel a pending or running redownload for this channel.

        Four cleanup paths, any of which may apply:
        1. If this channel's redownload is currently running, fire
           `_sync_cancel` so the pipeline exits at its next chunk
           boundary (same mechanism the Sync Tasks popover Cancel
           button uses).
        2. Remove any queued entries for this channel from the
           internal `_redwnl_pending` list — the chain worker won't
           start them.
        3. Remove from `queues.sync` so the UI popover drops the
           row and the task count decrements.
        4. Delete `_redownload_progress.json` from the channel folder
           so the Subs-table chartreuse dot + right-click "Continue
           Redownload" option both disappear on next render.

        Returns `{ok, was_running, was_queued, progress_removed}`.
        """
        try:
            name = folder_or_name if isinstance(folder_or_name, str) else (
                folder_or_name.get("name") or folder_or_name.get("folder", ""))
            if not name:
                return {"ok": False, "error": "channel name required"}
            ch = subs_backend.get_channel({"name": name})
            if not ch:
                return {"ok": False, "error": "Channel not found"}
            ch_url = (ch.get("url") or "").strip()
            cfg = self._config or load_config()
            base = (cfg.get("output_dir") or "").strip()
            from backend.sync import channel_folder_name as _cfn
            folder = os.path.join(base, _cfn(ch)) if base else ""

            was_running = False
            was_queued = False
            progress_removed = False

            # 1. Currently running? Check the active sync task.
            try:
                cur = self._queues.current_sync or {}
                if ((cur.get("kind") or "").lower() == "redownload"
                        and (cur.get("url") or "").strip() == ch_url):
                    was_running = True
                    self._sync_cancel.set()
            except Exception:
                pass

            # 2. Drop matching items from the internal pending chain.
            try:
                with self._redwnl_lock:
                    before = len(self._redwnl_pending)
                    self._redwnl_pending = [
                        it for it in self._redwnl_pending
                        if (it.get("rd_task", {}).get("url") or "").strip()
                        != ch_url
                    ]
                    if len(self._redwnl_pending) < before:
                        was_queued = True
            except Exception:
                pass

            # 3. Remove from the UI queue (may be there without being
            # in _redwnl_pending if the worker already popped it).
            try:
                if ch_url:
                    removed = self._queues.sync_remove(ch_url)
                    was_queued = was_queued or bool(removed)
            except Exception:
                pass

            # 4. Delete the progress file so the pending state clears.
            try:
                if folder:
                    pp = os.path.join(folder, "_redownload_progress.json")
                    if os.path.isfile(pp):
                        os.remove(pp)
                        progress_removed = True
            except OSError:
                pass

            # Invalidate the archive-scan cache so the Subs row
            # re-reads `_pending_redownload` as False on next render.
            try:
                from backend import archive_scan as _as
                _as.invalidate_channel(ch_url)
            except Exception:
                pass

            self._on_queue_changed()
            # bug H-5: push a Subs refresh so the chartreuse "Continue
            # Redownload" dot disappears immediately instead of waiting
            # for a tab switch. Mirrors the redownload-finished path.
            try:
                if self._window is not None:
                    self._window.evaluate_js(
                        "if (window.refreshSubsTable) "
                        "window.refreshSubsTable();")
            except Exception:
                pass
            return {"ok": True,
                    "was_running": was_running,
                    "was_queued": was_queued,
                    "progress_removed": progress_removed}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def chan_scan_resolution_mismatch(self, folder_or_name, target_res):
        """ffprobe every video in the channel's folder and count how many
        are below `target_res` (height). Returns {ok, mismatch, total}.

        Used by the edit panel's "Recheck resolution" button before offering
        to queue a bulk redownload. Matches YTArchiver.py:5155 res_check_btn.
        Fast path: "best" always reports 0 mismatches since we can't know
        what "best" actually is without a fresh catalog probe.
        """
        try:
            import subprocess as _sp
            name = folder_or_name if isinstance(folder_or_name, str) else (
                folder_or_name.get("name") or folder_or_name.get("folder", ""))
            if not name:
                return {"ok": False, "error": "channel name required"}
            target = str(target_res or "720").strip().lower()
            if target == "best":
                return {"ok": True, "mismatch": 0, "total": 0,
                        "note": "Best mode can't be scanned ahead of time."}
            try:
                target_h = int(target)
            except ValueError:
                return {"ok": False, "error": f"bad target: {target}"}
            ch = subs_backend.get_channel({"name": name})
            if not ch:
                return {"ok": False, "error": "Channel not found"}
            cfg = self._config or load_config()
            base = (cfg.get("output_dir") or "").strip()
            if not base:
                return {"ok": False, "error": "output_dir not set"}
            from backend.sync import channel_folder_name as _cfn
            folder = os.path.join(base, _cfn(ch))
            if not os.path.isdir(folder):
                return {"ok": False, "error": f"Folder missing: {folder}"}

            total = 0
            mismatch = 0
            scanned = 0
            _exts = (".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v")
            for dp, _dns, fns in os.walk(folder):
                for fn in fns:
                    if not fn.lower().endswith(_exts):
                        continue
                    total += 1
                    fp = os.path.join(dp, fn)
                    try:
                        r = _sp.run(
                            ["ffprobe", "-v", "error",
                             "-select_streams", "v:0",
                             "-show_entries", "stream=height",
                             "-of", "default=noprint_wrappers=1:nokey=1", fp],
                            capture_output=True, text=True, timeout=6,
                            creationflags=(0x08000000 if os.name == "nt" else 0))
                        height = int((r.stdout or "0").strip() or 0)
                        scanned += 1
                        if height > 0 and height < (target_h - 8):
                            mismatch += 1
                    except Exception:
                        continue
            return {"ok": True, "mismatch": mismatch, "total": total,
                    "scanned": scanned, "target": target_h}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def chan_redownload(self, folder_or_name, new_resolution=None,
                        scope=None):
        """Queue a channel's videos for redownload at a new resolution.

        Runs the full pipeline in `backend/redownload.py` — scans local files,
        fetches the YouTube catalog, matches by ID, downloads each at the new
        resolution, replaces the originals, and persists progress so a
        cancelled run can resume. Respects pause + cancel events.

        `scope` (optional dict):
          None - whole channel (default, matches OLD's tree-view root right-click)
          {year: 2024} - only that year subfolder (split_years channels)
          {year: 2024, month: 5} - only that year+month subfolder
        Mirrors OLD's per-year / per-month tree-view right-click
        (YTArchiver.py:26498 _browse_redownload_folder).
        """
        name = folder_or_name if isinstance(folder_or_name, str) else (
            folder_or_name.get("name") or folder_or_name.get("folder", ""))
        if not name:
            return {"ok": False, "error": "channel name required"}
        new_res = str(new_resolution or "").strip().lower()
        if not new_res:
            return {"ok": False, "error": "new_resolution required"}
        if new_res not in ("best", "2160", "1440", "1080", "720", "480", "360", "240", "144"):
            return {"ok": False, "error": f"Unsupported resolution: {new_res}"}
        ch = subs_backend.get_channel({"name": name})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        from backend.sync import channel_folder_name as _cfn
        _folder_name = _cfn(ch)
        # Defensive: if the channel ended up with a blank name somehow,
        # `channel_folder_name` returns "_unnamed" and we'd redownload into
        # the graveyard folder. Mirrors the guard in sync_channel.
        if _folder_name == "_unnamed":
            return {"ok": False,
                    "error": "Channel name is blank \u2014 edit the channel "
                             "in Subs and set a name before redownloading."}
        folder = os.path.join(base, _folder_name)
        if not os.path.isdir(folder):
            return {"ok": False, "error": f"Channel folder missing: {folder}"}
        # Gate behavior:
        #   - If a regular (non-redownload) sync is running, refuse.
        #   - If a redownload is running, QUEUE this request so the
        #     worker picks it up after the current one finishes.
        #   - If nothing is running, start a worker that drains the
        #     queue sequentially.
        # Reported: right-clicking "Continue Redownload" on channel 2
        # while channel 1 was still redownloading silently failed.
        _sync_alive = bool(self._sync_thread and self._sync_thread.is_alive())
        if _sync_alive:
            _cur = self._queues.current_sync or {}
            if (_cur.get("kind") or "").lower() != "redownload":
                return {"ok": False, "error": "Sync pipeline already running"}
            # Fall through into the enqueue path below.

        # Narrow to a year / month subfolder when requested. The
        # `_scan_local_files` walker already handles any folder path, so
        # pointing it at a subfolder just narrows the redownload set.
        scope_label = ""
        if isinstance(scope, dict) and scope.get("year"):
            y = str(scope["year"])
            sub = os.path.join(folder, y)
            if scope.get("month"):
                try:
                    m = int(scope["month"])
                    from backend.reorg import _MONTH_FOLDERS
                    mf = _MONTH_FOLDERS.get(m)
                    if mf:
                        sub = os.path.join(sub, mf)
                        scope_label = f"{y} / {mf}"
                    else:
                        scope_label = f"{y}"
                except Exception:
                    scope_label = f"{y}"
            else:
                scope_label = f"{y}"
            if not os.path.isdir(sub):
                return {"ok": False,
                        "error": f"Scope folder missing: {sub}"}
            folder = sub

        # Build a queue item + the UI-visible task dict.
        _rd_task = dict(ch)
        _rd_task["kind"] = "redownload"
        _rd_task["redownload_res"] = new_res
        _pending_item = {
            "ch": dict(ch),
            "folder": folder,
            "new_res": new_res,
            "scope_label": scope_label,
            "rd_task": _rd_task,
        }

        with self._redwnl_lock:
            # Always enqueue to the internal chain.
            self._redwnl_pending.append(_pending_item)
            # Mirror to the sync-queue UI so the Sync Tasks popover
            # shows queued redownloads alongside the one running.
            try: self._queues.sync_enqueue(_rd_task)
            except Exception: pass

            # If a worker is already draining the chain, we're done —
            # our item will get picked up when the current one
            # finishes. Reported: second "Continue Redownload" click
            # used to silently error with "Sync pipeline already
            # running"; it now queues and fires in turn.
            if _sync_alive:
                self._on_queue_changed()
                return {"ok": True, "queued": True, "resolution": new_res}

            # Nothing running: reset cancel/pause and spawn the worker.
            # Clear BOTH the threading.Event (pipeline-gate flag) AND
            # the QueueState flags (UI source-of-truth for the blink
            # icon + Pause/Resume button labels). `sync_start_all`
            # does this too — without it, the global pause/resume
            # button and the Sync Tasks popover's "Pause" button
            # stick in paused state even though this fresh redownload
            # is actively running. Reported: user saw the popover
            # showing a green ▶ "Resume" button while "Redownloading
            # ChannelName (480p)" was the active task.
            self._sync_cancel.clear()
            self._sync_pause.clear()
            self._sync_skip.clear()
            try: self._queues.set_sync_paused(False)
            except Exception: pass
            try:
                self._queues.set_gpu_paused(False)
                self._transcribe.resume()
            except Exception: pass

            def _worker():
                while True:
                    with self._redwnl_lock:
                        if not self._redwnl_pending:
                            break
                        item = self._redwnl_pending.pop(0)
                    # Remove the about-to-run item from the sync queue
                    # UI (moves it from "queued" to "running").
                    try:
                        self._queues.sync_remove(
                            item["rd_task"].get("url", ""))
                    except Exception:
                        pass
                    try:
                        self._run_redownload_one(
                            item["ch"], item["folder"],
                            item["new_res"], item["scope_label"])
                    except Exception as _re:
                        try: self._log_stream.emit_error(
                            f"Redownload crashed: {_re}")
                        except Exception: pass
                # Chain drained — final bookkeeping.
                self._on_queue_changed()
                try: self._autorun.notify_sync_done()
                except Exception: pass
                try: threading.Timer(0.5, self._on_queue_changed).start()
                except Exception: pass

            self._sync_thread = threading.Thread(target=_worker, daemon=True)
            self._sync_thread.start()
            self._on_queue_changed()
            return {"ok": True, "started": True, "resolution": new_res}

    def _run_redownload_one(self, ch, folder, new_res, scope_label):
        """Run ONE redownload to completion. Called from the chain
        worker. Previously inlined as `_run` inside `chan_redownload`;
        extracted so the worker can drain multiple queued items
        sequentially without re-spawning threads per item.
        """
        from backend import redownload as _rd
        _scope_text = f" [{scope_label}]" if scope_label else ""
        _rd_task = dict(ch)
        _rd_task["kind"] = "redownload"
        _rd_task["redownload_res"] = new_res
        try:
            self._queues.set_current_sync(_rd_task)
        except Exception:
            pass
        try:
            self._log_stream.emit([
                ["[Sync] ", "sync_bracket"],
                [f"Redownload {ch.get('name','?')}{_scope_text} \u2192 ",
                 "simpleline_green"],
                [("Best\n" if new_res == "best" else f"{new_res}p\n"),
                 "simpleline_green"],
            ])
            self._log_stream.flush()

            def _confirm(avg_pct, direction, res_label, sample_n):
                ev = threading.Event()
                self._redwnl_sample = {
                    "avg_pct": float(avg_pct),
                    "direction": str(direction),
                    "res_label": str(res_label),
                    "sample_n": int(sample_n),
                    "event": ev,
                    "choice": "continue",
                }
                try:
                    import json as _json
                    _payload = _json.dumps({
                        "kind": "redownload_sample",
                        "avg_pct": float(avg_pct),
                        "direction": str(direction),
                        "res_label": str(res_label),
                        "sample_n": int(sample_n),
                    })
                    self._log_stream.emit([
                        [_payload, "__control__"],
                    ])
                    self._log_stream.flush()
                except Exception:
                    pass
                ev.wait(timeout=300)
                return self._redwnl_sample.get("choice", "continue")

            _rd.redownload_channel(
                ch.get("name", ""), ch.get("url", ""), folder, new_res,
                stream=self._log_stream,
                cancel_ev=self._sync_cancel,
                pause_ev=self._sync_pause,
                confirm_cb=_confirm,
            )
        except Exception as e:
            self._log_stream.emit_error(f"Redownload crashed: {e}")
        finally:
            try: self._queues.set_current_sync(None)
            except Exception: pass
            self._log_stream.flush()
            try:
                from backend import archive_scan as _as
                _as.invalidate_channel(ch.get("url", ""))
            except Exception:
                pass
            self._on_queue_changed()
            # Tell the frontend to re-fetch the Subs table so the
            # chartreuse `_pending_redownload` dot clears now that
            # `_redownload_progress.json` has been deleted. Without
            # this push, the Subs table stays cached with the stale
            # dot until the user manually switches tabs or triggers
            # another refresh.
            try:
                if self._window is not None:
                    self._window.evaluate_js(
                        "if (window.refreshSubsTable) "
                        "window.refreshSubsTable();")
            except Exception:
                pass

    def redownload_sample_confirm(self, choice):
        """UI → Python bridge for the "check 10 then re-ask" popup.

        Called from app.js when the user clicks Continue / Cancel / picks
        a new resolution in the sample-confirm modal. Releases the
        worker thread that's parked on `_redwnl_sample["event"]`.

        `choice`:
          - "continue" → keep going at the current resolution
          - "cancel"   → stop the redownload
          - "best" / "2160" / "1440" / "1080" / "720" / "480" / "360"
            / "240" / "144" → switch to that resolution and resample
        """
        pending = getattr(self, "_redwnl_sample", None)
        if not pending:
            return {"ok": False, "error": "no pending sample-confirm"}
        c = str(choice or "continue").strip().lower()
        if c not in ("continue", "cancel",
                     "best", "2160", "1440", "1080", "720",
                     "480", "360", "240", "144"):
            return {"ok": False, "error": f"invalid choice: {c}"}
        pending["choice"] = c
        ev = pending.get("event")
        if ev is not None:
            try: ev.set()
            except Exception: pass
        return {"ok": True, "choice": c}

    def queue_pending_check(self):
        """Count channels that likely have new videos by comparing archive
        file cursor vs disk cache. Cheap sanity estimate — not exact."""
        cfg = load_config()
        channels = cfg.get("channels", [])
        cache = archive_scan.load_disk_cache()
        # Simple heuristic: every channel that auto-metadata=True and has
        # not been synced in the past 2 hours gets flagged as "pending"
        import time as _t
        threshold = _t.time() - 2 * 3600
        n_pending = 0
        for ch in channels:
            rec = cache.get(ch.get("url", ""))
            if not rec or rec.get("last_updated", 0) < threshold:
                n_pending += 1
        return {"ok": True, "count": n_pending, "total": len(channels)}

    # ─── Recent tab actions ────────────────────────────────────────────

    def _recent_lookup_path(self, title, channel):
        """Find the on-disk filepath for a Recent row by title + channel.

        Resolution order:
          1. Config `recent_downloads` explicit path (if file still exists)
          2. Index DB `videos.filepath` by title + channel
          3. Walk the channel folder by title / [videoId] / fuzzy ASCII
             match via `utils.try_find_by_title` — recovers files the user
             manually moved between year/month split layouts.
        """
        cfg = load_config()
        video_id_hint = ""
        stored_path = ""
        for r in cfg.get("recent_downloads", []):
            if r.get("title") == title and r.get("channel") == channel:
                stored_path = r.get("filepath", "") or ""
                video_id_hint = r.get("video_id", "") or video_id_hint
                if stored_path and os.path.isfile(stored_path):
                    return stored_path
        # DB fallback
        try:
            vids = index_backend.list_recent_videos(limit=500, channel=channel)
            for v in vids:
                if v.get("title") == title:
                    if not video_id_hint:
                        video_id_hint = v.get("video_id", "") or ""
                    if not stored_path:
                        stored_path = v.get("filepath", "") or stored_path
                    if v.get("filepath") and os.path.isfile(v["filepath"]):
                        return v["filepath"]
        except Exception:
            pass
        # Moved-file recovery — walk the channel folder by title / videoId
        try:
            from backend.utils import try_locate_moved_file
            base = (cfg.get("output_dir") or "").strip()
            if base and channel:
                from backend.sync import channel_folder_name as _cfn
                # Find the channel record to derive the folder name
                ch = None
                for c in cfg.get("channels", []):
                    if (c.get("name") == channel or c.get("folder") == channel):
                        ch = c
                        break
                ch_folder = os.path.join(base, _cfn(ch) if ch else channel)
                found = try_locate_moved_file(stored_path, title, ch_folder,
                                               video_id_hint)
                if found and os.path.isfile(found):
                    return found
        except Exception:
            pass
        return None

    def recent_play(self, title, channel):
        fp = self._recent_lookup_path(title, channel)
        if not fp:
            return {"ok": False, "error": "File not found"}
        return self.browse_open_video(fp)

    def recent_requeue(self, title, channel):
        """Re-download the YouTube URL stored for this Recent entry.
        Mirrors OLD YTArchiver.py Recent right-click "Re-queue download".

        Returns {ok, queued} or {ok:False, error}.
        """
        try:
            cfg = self._config or load_config()
            for r in cfg.get("recent_downloads", []):
                if r.get("title") == title and r.get("channel") == channel:
                    url = (r.get("video_url") or "").strip()
                    if not url:
                        vid = (r.get("video_id") or "").strip()
                        if vid:
                            url = f"https://www.youtube.com/watch?v={vid}"
                    if not url:
                        return {"ok": False,
                                "error": "No URL saved for this recent entry."}
                    # Delegate to single-video download. Uses the user's
                    # saved video_out_dir + resolution defaults.
                    return self.archive_single_video(url, options={})
            return {"ok": False, "error": "Recent entry not found."}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def recent_resolve(self, title, channel):
        """Return {ok, filepath, video_id} for a Recent row, using the same
        three-step lookup as recent_play (config → DB → fuzzy walk). Used by
        the right-click "Play video" action to hand off to the Browse Watch
        view instead of spawning VLC."""
        fp = self._recent_lookup_path(title, channel)
        if not fp:
            return {"ok": False, "error": "File not found"}
        # Best-effort video_id lookup — check config.recent_downloads first,
        # fall back to the FTS DB row.
        vid = ""
        try:
            cfg = self._config or load_config()
            for r in cfg.get("recent_downloads", []):
                if r.get("title") == title and r.get("channel") == channel:
                    vid = (r.get("video_id") or "").strip()
                    if not vid:
                        # parse from video_url if present
                        import re as _re
                        m = _re.search(r"[?&]v=([A-Za-z0-9_-]{11})",
                                       r.get("video_url") or "")
                        if m: vid = m.group(1)
                    break
        except Exception:
            pass
        if not vid:
            try:
                from backend import index as _idx
                conn = _idx._open()
                if conn is not None:
                    with _idx._db_lock:
                        row = conn.execute(
                            "SELECT video_id FROM videos WHERE title=? AND channel=? "
                            "ORDER BY added_ts DESC LIMIT 1",
                            (title, channel)).fetchone()
                    if row and row[0]:
                        vid = row[0]
            except Exception:
                pass
        return {"ok": True, "filepath": fp, "video_id": vid}

    def recent_show_in_explorer(self, title, channel):
        fp = self._recent_lookup_path(title, channel)
        if not fp:
            return {"ok": False, "error": "File not found"}
        return self.browse_show_in_explorer(fp)

    def recent_open_youtube(self, title, channel):
        """Open the YouTube page for this recent video (if we have video_id)."""
        import webbrowser, re as _re
        fp = self._recent_lookup_path(title, channel)
        if fp:
            m = _re.search(r"\[([A-Za-z0-9_-]{11})\]", os.path.basename(fp))
            if m:
                webbrowser.open(f"https://www.youtube.com/watch?v={m.group(1)}")
                return {"ok": True}
        return {"ok": False, "error": "No video ID available"}

    def recent_delete_file(self, title, channel):
        """Delete the file from disk + remove from recent_downloads list."""
        fp = self._recent_lookup_path(title, channel)
        if not fp:
            return {"ok": False, "error": "File not found"}
        try:
            os.remove(fp)
        except OSError as e:
            return {"ok": False, "error": str(e)}
        # Also drop sidecars.
        # audit F-24: broadened sidecar list — yt-dlp can leave
        # .description, .live_chat.json, .en.srt, .en-orig.vtt,
        # .en-US.vtt and other variants. The narrow list missed
        # these and leaked them on every Recent delete.
        base = os.path.splitext(fp)[0]
        _basic_exts = (".txt", ".jsonl", ".info.json", ".description",
                       ".live_chat.json", ".srt",
                       ".jpg", ".jpeg", ".webp", ".png")
        for ext in _basic_exts:
            try:
                sc = base + ext
                if os.path.isfile(sc):
                    os.remove(sc)
            except OSError:
                pass
        # Language-coded caption sidecars (en, en-orig, en-US, es, etc.)
        # We use a glob rather than enumerating every language code.
        try:
            import glob as _glob
            for pat in (base + ".*.vtt", base + ".*.srt",
                        base + ".*.ttml"):
                for _hit in _glob.glob(pat):
                    try: os.remove(_hit)
                    except OSError: pass
        except Exception:
            pass
        # Remove from recent_downloads (if writable)
        if config_is_writable():
            cfg = load_config()
            cfg["recent_downloads"] = [r for r in cfg.get("recent_downloads", [])
                                        if not (r.get("title") == title and r.get("channel") == channel)]
            from backend.ytarchiver_config import save_config as _sc
            _sc(cfg)
        return {"ok": True}

    # ─── Settings dialog: load / save all tunables ─────────────────────

    def settings_load(self):
        cfg = self._config or load_config()
        return {
            "output_dir": cfg.get("output_dir", ""),
            "video_out_dir": cfg.get("video_out_dir", ""),
            "whisper_model": cfg.get("whisper_model", "small"),
            "default_resolution": cfg.get("default_resolution", "720"),
            "log_mode": cfg.get("log_mode", "Simple"),
            # Index tab surfaces these directly — must round-trip.
            "tp_archive_roots": list(cfg.get("tp_archive_roots") or []),
            "auto_index_enabled": bool(cfg.get("auto_index_enabled", False)),
            "auto_index_threshold": int(cfg.get("auto_index_threshold", 10) or 10),
            # Startup knobs (Settings > General surfaces these too).
            "disk_scan_staleness_hours": int(cfg.get("disk_scan_staleness_hours", 24) or 0),
            "browse_preload_limit": int(cfg.get("browse_preload_limit", 150) or 150),
            "browse_preload_all": bool(cfg.get("browse_preload_all", False)),
            "last_disk_scan_ts": float(cfg.get("last_disk_scan_ts", 0) or 0),
            # Subs table column visibility toggles. Default False for
            # new users — the column is optional polish, not core info.
            "show_avg_size": bool(cfg.get("show_avg_size", False)),
            # Recent tab view mode — "list" (legacy) or "grid" (thumbnail
            # cards). Default "grid" for new users — the thumbnail view
            # reads more naturally at a glance.
            "recent_view_mode": (cfg.get("recent_view_mode") or "grid"),
        }

    def settings_save(self, data):
        if not config_is_writable():
            return {"ok": False, "error": "Write-gate off"}
        cfg = load_config()
        if data.get("output_dir"): cfg["output_dir"] = os.path.normpath(data["output_dir"])
        if data.get("video_out_dir"): cfg["video_out_dir"] = os.path.normpath(data["video_out_dir"])
        if data.get("whisper_model"): cfg["whisper_model"] = data["whisper_model"]
        if data.get("default_resolution"): cfg["default_resolution"] = data["default_resolution"]
        if data.get("log_mode") in ("Simple", "Verbose"):
            cfg["log_mode"] = data["log_mode"]
        # Index-tab persistence: archive roots + auto-index toggle + threshold.
        if isinstance(data.get("tp_archive_roots"), list):
            cfg["tp_archive_roots"] = [str(r) for r in data["tp_archive_roots"] if r]
        if "auto_index_enabled" in data:
            cfg["auto_index_enabled"] = bool(data["auto_index_enabled"])
        if "auto_index_threshold" in data:
            try:
                cfg["auto_index_threshold"] = max(1, min(9999, int(data["auto_index_threshold"])))
            except Exception: pass
        # Startup knobs — all three round-trip here.
        if "disk_scan_staleness_hours" in data:
            try:
                cfg["disk_scan_staleness_hours"] = max(0, min(10_000,
                    int(data["disk_scan_staleness_hours"])))
            except Exception: pass
        if "browse_preload_limit" in data:
            try:
                cfg["browse_preload_limit"] = max(1, min(100_000,
                    int(data["browse_preload_limit"])))
            except Exception: pass
        if "browse_preload_all" in data:
            cfg["browse_preload_all"] = bool(data["browse_preload_all"])
        # Subs table column visibility
        if "show_avg_size" in data:
            cfg["show_avg_size"] = bool(data["show_avg_size"])
        # Recent tab view mode — only accept known values.
        if data.get("recent_view_mode") in ("list", "grid"):
            cfg["recent_view_mode"] = data["recent_view_mode"]
        from backend.ytarchiver_config import save_config as _sc
        if not _sc(cfg):
            return {"ok": False, "error": "Save failed"}
        self._reload_config()
        # Push log mode into LogStreamer
        self._log_stream.simple_mode = (cfg["log_mode"] == "Simple")
        return {"ok": True}

    # ─── yt-dlp version / update ───────────────────────────────────────

    def ytdlp_version(self):
        """Return current yt-dlp version string."""
        yt = sync_backend.find_yt_dlp()
        if not yt:
            return {"ok": False, "error": "yt-dlp not found"}
        try:
            import subprocess as _sp
            r = _sp.run([yt, "--version"], capture_output=True, text=True,
                        timeout=10, startupinfo=sync_backend._startupinfo)
            ver = (r.stdout or "").strip().split("\n")[0] or "unknown"
            return {"ok": True, "version": ver, "path": yt}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def ytdlp_update(self):
        """Run yt-dlp -U in a background thread; stream output to the log."""
        yt = sync_backend.find_yt_dlp()
        if not yt:
            return {"ok": False, "error": "yt-dlp not found"}
        def _run():
            import subprocess as _sp
            self._log_stream.emit([
                ["[Update] ", "update_head"],
                ["Updating yt-dlp...\n", "update_sep"],
            ])
            try:
                proc = _sp.Popen([yt, "-U"],
                                  stdout=_sp.PIPE, stderr=_sp.STDOUT,
                                  encoding="utf-8", errors="replace", bufsize=1,
                                  startupinfo=sync_backend._startupinfo)
                for line in proc.stdout:
                    self._log_stream.emit_dim(" " + line.rstrip())
                proc.wait()
                # audit D-1: check proc.returncode before declaring
                # success. Old code always emitted "update complete"
                # even on non-zero exit (most common cause: the yt-dlp
                # exe is locked by a running sync, so the self-update
                # fails but the banner still claimed it worked).
                if proc.returncode == 0:
                    self._log_stream.emit([["[Update] ", "update_head"],
                                            ["yt-dlp update complete.\n", "update_sep"]])
                else:
                    self._log_stream.emit_error(
                        f"yt-dlp update failed (exit code {proc.returncode}). "
                        "If a sync is running, stop it and try again — the "
                        ".exe can't be replaced while it's open.")
            except Exception as e:
                self._log_stream.emit_error(f"yt-dlp update failed: {e}")
            finally:
                self._log_stream.flush()
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}

    # ─── Channel list export / import ──────────────────────────────────

    def channels_export(self):
        try:
            import webview as _wv, json as _json
            cfg = load_config()
            if self._window is None:
                return {"ok": False, "error": "No window"}
            paths = self._window.create_file_dialog(
                _wv.SAVE_DIALOG, save_filename="ytarchiver_channels.json",
                file_types=("JSON (*.json)",),
            )
            if not paths:
                return {"ok": False, "cancelled": True}
            path = paths if isinstance(paths, str) else paths[0]
            with open(path, "w", encoding="utf-8") as f:
                _json.dump({
                    "exported_from": "YTArchiver",
                    "channels": cfg.get("channels", []),
                }, f, indent=2)
            return {"ok": True, "path": path, "count": len(cfg.get("channels", []))}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def channels_import(self):
        try:
            import webview as _wv, json as _json
            if self._window is None:
                return {"ok": False, "error": "No window"}
            paths = self._window.create_file_dialog(
                _wv.OPEN_DIALOG, allow_multiple=False,
                file_types=("JSON (*.json)", "All files (*.*)"),
            )
            if not paths:
                return {"ok": False, "cancelled": True}
            path = paths if isinstance(paths, str) else paths[0]
            with open(path, "r", encoding="utf-8") as f:
                data = _json.load(f)
            imported = data.get("channels", []) if isinstance(data, dict) else data
            if not isinstance(imported, list):
                return {"ok": False, "error": "Not a channel list"}
            if not config_is_writable():
                return {"ok": False, "error": "Write-gate off"}
            cfg = load_config()
            existing_urls = {c.get("url") for c in cfg.get("channels", [])}
            added = 0
            # bug W-10: track WHY each entry was skipped so the UI can
            # tell the user (previously just reported a raw count with
            # no way to debug a partial import).
            skipped_reasons: List[Dict[str, str]] = []
            for ch in imported:
                if not isinstance(ch, dict):
                    skipped_reasons.append({
                        "name": "(unknown)",
                        "reason": "not a valid channel object",
                    })
                    continue
                if not ch.get("url"):
                    skipped_reasons.append({
                        "name": ch.get("name") or "(no name)",
                        "reason": "missing URL",
                    })
                    continue
                if ch["url"] in existing_urls:
                    skipped_reasons.append({
                        "name": ch.get("name") or ch["url"],
                        "reason": "already subscribed",
                    })
                    continue
                # audit E-44: validate URL shape before adding. Old
                # code accepted any non-empty ch["url"] so a corrupted
                # import file with "not-a-url" values landed channels
                # that failed at sync time with cryptic yt-dlp errors.
                # Checking here surfaces the problem at import time
                # when the user can act on it.
                _u = str(ch.get("url") or "").strip().lower()
                if not (("youtube.com/" in _u) or ("youtu.be/" in _u)):
                    skipped_reasons.append({
                        "name": ch.get("name") or ch["url"],
                        "reason": "URL doesn't look like a YouTube link",
                    })
                    continue
                cfg.setdefault("channels", []).append(ch)
                existing_urls.add(ch["url"])
                added += 1
            cfg["channels"].sort(key=lambda c: (c.get("name") or "").lower())
            from backend.ytarchiver_config import save_config as _sc
            if not _sc(cfg):
                return {"ok": False, "error": "Save failed"}
            self._reload_config()
            return {"ok": True, "added": added,
                    "skipped": len(skipped_reasons),
                    "skipped_reasons": skipped_reasons}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ─── About info ────────────────────────────────────────────────────

    def about_info(self):
        cfg = self._config or load_config()
        yt_ver = "unknown"
        try:
            r = self.ytdlp_version()
            if r.get("ok"):
                yt_ver = r["version"]
        except Exception:
            pass
        return {
            "app_name": "YTArchiver",
            "app_version": APP_VERSION,
            "channels": len(cfg.get("channels", [])),
            "config_path": str(CONFIG_FILE),
            "output_dir": cfg.get("output_dir", ""),
            "ytdlp_version": yt_ver,
            "python_version": sys.version.split()[0],
        }

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
            import pystray # noqa: F401
            rows.append({"name": "pystray", "ok": True, "detail": ""})
        except ImportError:
            rows.append({"name": "pystray", "ok": False, "detail": "pip install pystray"})
        try:
            from PIL import Image # noqa: F401
            rows.append({"name": "Pillow", "ok": True, "detail": ""})
        except ImportError:
            rows.append({"name": "Pillow", "ok": False, "detail": "pip install Pillow"})
        try:
            import webview # noqa: F401
            rows.append({"name": "pywebview", "ok": True, "detail": ""})
        except ImportError:
            rows.append({"name": "pywebview", "ok": False, "detail": "pip install pywebview"})
        # Executables
        import shutil as _sh
        for exe, hint in (("yt-dlp", "pip install yt-dlp (or drop yt-dlp.exe next to this app)"),
                          ("ffmpeg", "Install ffmpeg + put on PATH"),
                          ("ffprobe", "Comes with ffmpeg")):
            path = _sh.which(exe)
            rows.append({"name": exe, "ok": bool(path), "detail": path or hint})
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
            self._log_stream.emit([
                ["[Deps] ", "sync_bracket"],
                [f"{len(missing)} missing: ", "red"],
                [", ".join(r["name"] for r in missing) + "\n", "dim"],
            ])
            self._log_stream.flush()
        return {"ok": True, "rows": rows, "missing": len(missing)}

    def subs_relocate_channel(self, identity, new_folder_name):
        """Update a channel's folder_override to point at a different on-disk
        folder (used when the original folder is gone but the user has it
        elsewhere). `new_folder_name` must be a subfolder of output_dir.

        Mirrors YTArchiver.py:33700 "locate" branch of the missing-folder
        dialog. Never moves files — just updates the config pointer.
        """
        if not identity or not new_folder_name:
            return {"ok": False, "error": "identity + new_folder_name required"}
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        target = os.path.normpath(os.path.join(base, new_folder_name))
        if not os.path.isdir(target):
            return {"ok": False, "error": f"Folder not found: {target}"}
        # Guard: must live inside output_dir (prevent folder_override escapes)
        if os.path.dirname(target) != os.path.normpath(base):
            return {"ok": False,
                    "error": "Target folder must live directly under output_dir"}
        try:
            for ch in cfg.get("channels", []):
                if (ch.get("url") == identity.get("url")
                        or ch.get("name") == identity.get("name")):
                    ch["folder_override"] = os.path.basename(target)
                    break
            from backend.ytarchiver_config import save_config as _sc
            if not _sc(cfg):
                return {"ok": False, "error": "Save blocked (write-gate off)"}
            self._reload_config()
            return {"ok": True, "folder_override": os.path.basename(target)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def subs_browse_for_channel_folder(self, channel_name):
        """Open native folder picker; returns the selected folder's basename
        (must be inside output_dir) or an error."""
        try:
            import webview as _wv
            if self._window is None:
                return {"ok": False, "error": "No window"}
            cfg = load_config()
            base = (cfg.get("output_dir") or "").strip()
            paths = self._window.create_file_dialog(
                _wv.FOLDER_DIALOG, directory=base,
            )
            if not paths:
                return {"ok": False, "cancelled": True}
            picked = paths if isinstance(paths, str) else paths[0]
            picked = os.path.normpath(picked)
            if os.path.dirname(picked) != os.path.normpath(base):
                return {"ok": False,
                        "error": f"Pick a subfolder of:\n {base}"}
            return {"ok": True,
                    "folder_name": os.path.basename(picked),
                    "full_path": picked}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def check_channel_folders(self):
        """Verify each subscribed channel's on-disk folder exists.

        Returns the list of channels whose folders are missing (only for
        channels marked `initialized=True`). The UI can then prompt the
        user to remove / locate / skip each one. Never modifies config.
        """
        cfg = self._config or load_config()
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
            self._log_stream.emit([
                ["[Subs] ", "sync_bracket"],
                [f"{len(missing)} channel folder(s) missing \u2014 ", "red"],
                ["see Subs tab for reconcile\n", "dim"],
            ])
            self._log_stream.flush()
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
                import urllib.request as _ur
                import json as _json
                req = _ur.Request(
                    "https://api.github.com/repos/skurbee/YTArchiver/releases/latest",
                    headers={"User-Agent": "YTArchiver"},
                )
                with _ur.urlopen(req, timeout=8) as resp:
                    data = _json.loads(resp.read())
                latest = (data.get("tag_name") or "").strip()
                rel_url = data.get("html_url") or \
                    "https://github.com/skurbee/YTArchiver/releases/latest"
                current = APP_VERSION
                if latest and _ver_tuple(latest) > _ver_tuple(current):
                    sep = "=" * 54
                    self._log_stream.emit([[f"\n{sep}\n", "update_sep"]])
                    self._log_stream.emit([
                        [f" \u2b06 Update available: {latest} ", "update_head"],
                        [f"(you have {current})\n", "update_head"],
                    ])
                    self._log_stream.emit([[f" Download: {rel_url}\n", "update_head"]])
                    self._log_stream.emit([[f"{sep}\n\n", "update_sep"]])
                    self._log_stream.flush()
            except Exception as _e:
                # audit F-23: surface the failure as a dim log line
                # so the user has evidence the check ran (and why it
                # failed). Old code silently swallowed, leaving no
                # trace of whether the update probe ever fired.
                try:
                    self._log_stream.emit_dim(
                        f"[Update] check skipped: {_e}")
                except Exception:
                    pass

        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}

    def diagnostics_run(self):
        """Self-check: yt-dlp, Python 3.11, FTS DB, GPU, disk space, paths.

        Returns a list of {name, ok, detail} rows. Never raises — every probe
        is wrapped so the dialog always has something to show.
        """
        rows = []
        cfg = self._config or load_config()

        def _row(name, ok, detail):
            rows.append({"name": name, "ok": bool(ok), "detail": str(detail)})

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
            from backend.transcribe import TranscribeManager as _tm
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

        # 4. GPU (nvidia-smi probe)
        try:
            import subprocess
            r = subprocess.run(
                ["nvidia-smi", "--query-gpu=name,memory.total",
                 "--format=csv,noheader"],
                capture_output=True, text=True, timeout=5,
            )
            if r.returncode == 0 and r.stdout.strip():
                _row("GPU", True, r.stdout.strip().splitlines()[0])
            else:
                _row("GPU", False, "nvidia-smi not available (CPU whisper only)")
        except Exception as e:
            _row("GPU", False, f"nvidia-smi not runnable: {e}")

        # 5. Archive root + free space
        try:
            base = (cfg.get("output_dir") or "").strip()
            if not base:
                _row("Archive root", False, "Not configured (Settings > Archive root)")
            elif not os.path.isdir(base):
                _row("Archive root", False, f"{base} (missing)")
            else:
                import shutil
                total, used, free = shutil.disk_usage(base)
                free_gb = free / (1024 ** 3)
                pct = (used / total * 100) if total else 0.0
                _row("Archive root", True,
                     f"{base} \u2014 {free_gb:.0f} GB free ({pct:.0f}% full)")
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
                label = src[1] if src[0] == "--cookies-from-browser" else f"file: {src[1]}"
                _row("Cookies source", True, label)
            else:
                _row("Cookies source", False,
                     "No browser profile or cookies.txt found (public-only mode)")
        except Exception as e:
            _row("Cookies source", False, str(e))

        return {"ok": True, "rows": rows}

    def bookmark_export_csv(self):
        """Prompt for a save path and write bookmarks to CSV."""
        try:
            import csv, io, webview as _wv
            bms = index_backend.bookmark_list()
            if not bms:
                return {"ok": False, "error": "No bookmarks to export"}
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["created", "channel", "title", "start_time", "text", "note", "video_id"])
            for b in bms:
                w.writerow([b.get("created"), b.get("channel"), b.get("title"),
                            b.get("start_time"), b.get("text"), b.get("note"),
                            b.get("video_id")])
            if self._window is None:
                return {"ok": False, "error": "No window"}
            paths = self._window.create_file_dialog(
                _wv.SAVE_DIALOG,
                save_filename="ytarchiver_bookmarks.csv",
            )
            if not paths:
                return {"ok": False, "cancelled": True}
            path = paths if isinstance(paths, str) else paths[0]
            with open(path, "w", encoding="utf-8", newline="") as f:
                f.write(buf.getvalue())
            return {"ok": True, "path": path, "count": len(bms)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ─── Compress / Reorg ──────────────────────────────────────────────

    def compress_video_file(self, filepath, quality="Average", output_res="720"):
        """Queue an AV1 NVENC compression task onto the shared GPU
        queue. rule: the GPU task list is the user's "permission
        to bog down my computer" — so standalone compress must NOT
        fire immediately off a bare thread. It enqueues, then the
        TranscribeManager worker picks it up when Auto is on (or when
        the user manually kicks the queue).
        """
        if not filepath:
            return {"ok": False, "error": "filepath required"}
        try:
            title = os.path.splitext(os.path.basename(filepath))[0]
        except Exception:
            title = filepath
        # Try to derive the channel from the filepath's parent folder
        # for nicer queue labels.
        try:
            channel = os.path.basename(os.path.dirname(filepath))
        except Exception:
            channel = ""
        ok = self._transcribe.compress_enqueue(
            filepath, title=title, channel=channel,
            quality=quality, output_res=output_res)
        return {"ok": bool(ok), "queued": bool(ok)}

    def compress_videos_batch(self, paths, quality="Average", output_res="720",
                              redo_on_larger=True):
        """Queue a list of videos onto the shared GPU queue — one
        compress task per path. Serializes through the same worker as
        transcribe so the GPU isn't slammed with parallel NVENC
        sessions. `redo_on_larger` isn't meaningful at enqueue time
        (it's a per-job retry flag handled inside compress_video), so
        we attach it as a job hint for future use but don't branch on
        it here.
        """
        paths = paths or []
        queued = 0
        for p in paths:
            try:
                title = os.path.splitext(os.path.basename(p))[0]
                channel = os.path.basename(os.path.dirname(p))
            except Exception:
                title, channel = p, ""
            if self._transcribe.compress_enqueue(
                    p, title=title, channel=channel,
                    quality=quality, output_res=output_res):
                queued += 1
        return {"ok": True, "queued": queued, "count": len(paths)}

    def reorg_channel_folder(self, identity, split_years=True, split_months=False,
                             recheck_dates=False):
        """Reorg a channel's folder into year/month subfolders.

        `recheck_dates=True` re-reads .info.json sidecars and fixes file mtimes
        before grouping (matches YTArchiver's Re-check Dates option).
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        from backend.sync import sanitize_folder, channel_folder_name
        folder = os.path.join(base, channel_folder_name(ch))
        def _run():
            try:
                reorg_backend.reorg_channel(folder,
                                            split_years=bool(split_years),
                                            split_months=bool(split_months),
                                            stream=self._log_stream,
                                            cancel_event=self._sync_cancel,
                                            recheck_dates=bool(recheck_dates))
            finally:
                self._log_stream.flush()
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}

    def chan_fix_file_dates(self, identity):
        """Fix file mtimes for a channel's videos using .info.json upload dates.

        Lighter-weight than reorg — doesn't move files, only fixes dates so
        Recent / Browse sorts reflect YouTube upload order. Runs in a thread.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        from backend.sync import channel_folder_name as _cfn
        folder = os.path.join(base, _cfn(ch))

        def _run():
            try:
                reorg_backend.fix_file_dates(folder, self._log_stream,
                                             cancel_event=self._sync_cancel)
            finally:
                self._log_stream.flush()

        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}

    def prompt_metadata_already_downloaded(self, channel_name, count):
        """Ask user via JS dialog: Skip / Overwrite / Append. Returns choice string.

        Intended to be called BY the backend sync worker when it detects
        existing metadata. Bridges back into JS askChoice via evaluate_js.
        """
        if self._window is None:
            return {"choice": "skip"}
        import json as _json
        result = {"val": None, "event": threading.Event()}
        try:
            # Create a one-shot global callback the JS side writes into
            js = (
                "(async () => {"
                f" const c = await window.askMetadataAlreadyDownloaded({_json.dumps(channel_name)}, {int(count)});"
                f" window.pywebview.api._metadata_choice_resolve({_json.dumps(id(result))}, c);"
                "})()"
            )
            # Register a one-shot resolver
            setattr(self, "_pending_metadata_choice", result)
            self._window.evaluate_js(js)
            result["event"].wait(timeout=120)
            return {"choice": result["val"] or "skip"}
        except Exception:
            return {"choice": "skip"}

    def _metadata_choice_resolve(self, _token, val):
        """Internal: JS calls this when the user picks a choice."""
        pending = getattr(self, "_pending_metadata_choice", None)
        if pending:
            pending["val"] = val
            pending["event"].set()
        return {"ok": True}

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
        except Exception:
            pass
        return {"ok": True}

    def app_restart(self):
        """Re-launch the exe/script, then destroy the current window.

        Used after backup-restore so the freshly-loaded config takes effect.
        """
        try:
            import subprocess
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
            # audit D-58: call _shutdown_cleanup() BEFORE os._exit so
            # queue state gets saved, window geometry persisted, and
            # child subprocesses (yt-dlp / whisper / ffmpeg) killed
            # rather than carried over into the restarted process
            # where they'd race with the new instance's fresh jobs.
            def _die():
                time.sleep(0.6)
                try:
                    # Best-effort: if the function is in scope it runs
                    # the full close sequence. If not (import order,
                    # shutdown teardown, etc.) we still os._exit so
                    # the user's restart request doesn't stall.
                    try:
                        _shutdown_cleanup()  # type: ignore[name-defined]
                    except NameError:
                        pass
                    except Exception as _ce:
                        print(f"[app_restart] shutdown_cleanup: {_ce}")
                    if self._window:
                        self._window.destroy()
                except Exception:
                    pass
                os._exit(0)
            threading.Thread(target=_die, daemon=True).start()
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ─── URL history (for autocomplete on the URL field) ──────────────

    def url_history(self):
        """Return recently-typed YouTube URLs (latest first, max 20)."""
        cfg = load_config()
        return cfg.get("url_history", [])[:20]

    def _push_url_history(self, url):
        if not config_is_writable():
            return
        cfg = load_config()
        hist = [u for u in cfg.get("url_history", []) if u != url]
        hist.insert(0, url)
        del hist[20:]
        cfg["url_history"] = hist
        from backend.ytarchiver_config import save_config as _sc
        _sc(cfg)

    # ─── Single-URL archive (Enter on URL field) ───────────────────────

    def archive_single_video(self, url, options=None):
        """Download a single YouTube URL immediately (no channel walk).

        Output layout mirrors YTArchiver.py:17313 build_video_cmd exactly so
        single-video downloads are indistinguishable from OLD's:
          - Target dir: `cfg['video_out_dir']` (NOT `output_dir`/channels),
            no `%(uploader)s` subfolder, no `[id]` suffix.
          - Filename: `{title}.mp4` or `{title} (MM.DD.YY).mp4` when add_date.
          - Custom name sanitizer: `[<>:"/\\|?*\x00-\x1f]` → `_`.
          - `--mtime` when date_file is True so mtime = YT upload date.

        Concurrency guard: a per-URL lock prevents the user from
        spawning parallel yt-dlp processes for the same URL by
        double-clicking. Different URLs are still allowed in parallel.
        """
        import re as _re
        url = (url or "").strip()
        if not url:
            return {"ok": False, "error": "Empty URL"}
        # Concurrency guard — track in-flight URLs so a rapid
        # double-click doesn't launch two yt-dlp processes fighting
        # over the same filename.
        if not hasattr(self, "_archive_single_inflight"):
            self._archive_single_inflight = set()
            self._archive_single_lock = threading.Lock()
        with self._archive_single_lock:
            if url in self._archive_single_inflight:
                return {"ok": False,
                        "error": "Already downloading this URL"}
            self._archive_single_inflight.add(url)
        # Remember this URL for autocomplete next time
        try:
            self._push_url_history(url)
        except Exception:
            pass
        if not sync_backend.find_yt_dlp():
            return {"ok": False, "error": "yt-dlp not found"}
        cfg = load_config()
        opts = options if isinstance(options, dict) else {}
        # Target folder: custom save_to, then video_out_dir (dedicated single-
        # video destination), then fall back to output_dir (the channel root)
        # as a last resort. OLD uses `video_out_dir` exclusively.
        base = (opts.get("save_to") or cfg.get("video_out_dir")
                or cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "No output_dir configured"}
        import subprocess as _sp
        yt = sync_backend.find_yt_dlp()
        res = str(opts.get("resolution") or "1080").strip()
        fmt = sync_backend.build_format_string(res)
        # Filename: OLD-compat template. The date suffix uses TODAY's date
        # (download date), NOT the upload date — matches YTArchiver.py:17316.
        dl_date = datetime.now().strftime("%m.%d.%y")
        use_yt_title = opts.get("use_yt_title", True)
        add_date = bool(opts.get("add_date", False))
        custom_name = (opts.get("custom_name") or "").strip()
        if not use_yt_title and custom_name:
            safe = _re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", custom_name).strip().rstrip(".")
            if add_date:
                fname = f"{safe} ({dl_date}).%(ext)s"
            else:
                fname = f"{safe}.%(ext)s"
        else:
            fname = f"%(title)s ({dl_date}).%(ext)s" if add_date else "%(title)s.%(ext)s"
        out_tpl = os.path.join(base, fname)

        # Option: date_file — apply YT upload date as file mtime (default: True)
        date_file = opts.get("date_file", True)

        def _run():
            self._log_stream.emit([
                ["[Archive] ", "simpleline_green"],
                [f"{url}\n", "simpleline"],
            ])
            # Mirror YTArchiver.py:17327 build_video_cmd exactly — skip the
            # mp4 merge args when downloading audio-only.
            cmd = [yt, "--newline", "--no-quiet"]
            if date_file:
                cmd.append("--mtime")
            cmd += ["--trim-filenames", "200", "--format", fmt]
            if res != "audio":
                cmd += ["--merge-output-format", "mp4", "--ppa", "Merger:-c copy"]
            cmd += [
                "--output", out_tpl,
                "--print",
                "after_video:DLTRACK:::%(title)s:::%(uploader)s:::%(upload_date)s:::%(filesize,filesize_approx)s:::%(duration)s:::%(id)s",
                *sync_backend._find_cookie_source(),
                url,
            ]
            try:
                proc = _sp.Popen(cmd, stdin=_sp.DEVNULL,
                                 stdout=_sp.PIPE, stderr=_sp.STDOUT,
                                 encoding="utf-8", errors="replace",
                                 bufsize=1, startupinfo=sync_backend._startupinfo)
            except OSError as e:
                self._log_stream.emit_error(f"Launch failed: {e}")
                # Release the in-flight lock on launch failure so a
                # retry isn't blocked forever.
                try:
                    with self._archive_single_lock:
                        self._archive_single_inflight.discard(url)
                except Exception:
                    pass
                return
            # bug W-1: parse the DLTRACK line so single-video downloads
            # land in the videos index, the Recent tab, and the FTS
            # index — same as channel-sync downloads. Previously this
            # loop just echoed stdout to the log and the file ended up
            # on disk invisible to the Browse grid / Recent / Search.
            _dltrack = None
            _stderr_errors = []
            try:
                for line in proc.stdout:
                    _line = line.rstrip()
                    self._log_stream.emit_dim(" " + _line)
                    if _line.startswith("DLTRACK:::"):
                        _dltrack = _line
                    # bug W-7: capture known-failure yt-dlp error
                    # signatures so the post-run branch can surface a
                    # toast instead of leaving the user staring at dim
                    # stdout with no actionable info.
                    _ll = _line.lower()
                    if "error" in _ll:
                        if "members-only" in _ll or "join this channel" in _ll:
                            _stderr_errors.append("members-only content")
                        elif "private video" in _ll:
                            _stderr_errors.append("private video")
                        elif "video unavailable" in _ll or "this video is unavailable" in _ll:
                            _stderr_errors.append("video unavailable (deleted or region-locked)")
                        elif "cookies are missing" in _ll or "sign in to confirm" in _ll:
                            _stderr_errors.append("YouTube wants a sign-in (Firefox/Chrome cookies not found or expired)")
                proc.wait()
                # Post-download bookkeeping — emulate the channel-sync
                # path's register_video + _record_recent_download hooks.
                if _dltrack:
                    try:
                        parts = _dltrack.split(":::")
                        # Format: "DLTRACK" + title, uploader, upload_date,
                        # filesize, duration, id
                        _title = parts[1] if len(parts) > 1 else ""
                        _uploader = parts[2] if len(parts) > 2 else ""
                        _vid = parts[6] if len(parts) > 6 else ""
                        # Resolve the final filepath on disk. Template
                        # was `<title> (MM.DD.YY).ext` or `<title>.ext`
                        # under `base`. Scan `base` for something
                        # matching the video ID / title.
                        # audit E-61: when multiple candidate files
                        # match, prefer the NEWEST by mtime (this is
                        # the fresh download). Old code picked the
                        # first glob hit, which for users with two
                        # copies pointed at the stale/duplicate file.
                        # audit F-49: title-prefix fallback now requires
                        # a full-title match (not just first 50 chars)
                        # so series with similar long-title prefixes
                        # ("Video 1:...", "Video 2:...") don't collide.
                        final_path = ""
                        try:
                            import glob as _glob
                            _vid_candidates = []
                            if _vid:
                                for _g in _glob.glob(os.path.join(base, "*")):
                                    if _vid in os.path.basename(_g):
                                        _vid_candidates.append(_g)
                            if _vid_candidates:
                                _vid_candidates.sort(
                                    key=lambda p: os.path.getmtime(p)
                                    if os.path.isfile(p) else 0,
                                    reverse=True)
                                final_path = _vid_candidates[0]
                            if not final_path and _title:
                                # Fallback: match by full sanitized title.
                                _title_sane = _re.sub(
                                    r'[<>:"/\\|?*\x00-\x1f]', "_", _title)
                                _title_candidates = []
                                for _g in _glob.glob(os.path.join(base, "*")):
                                    _stem = os.path.splitext(
                                        os.path.basename(_g))[0]
                                    # Match if the stem EQUALS the full
                                    # sanitized title (possibly with a
                                    # " (MM.DD.YY)" date suffix stripped).
                                    _stem_no_date = _re.sub(
                                        r"\s*\(\d{2}\.\d{2}\.\d{2}\)\s*$",
                                        "", _stem)
                                    if _stem == _title_sane or _stem_no_date == _title_sane:
                                        _title_candidates.append(_g)
                                if _title_candidates:
                                    _title_candidates.sort(
                                        key=lambda p: os.path.getmtime(p)
                                        if os.path.isfile(p) else 0,
                                        reverse=True)
                                    final_path = _title_candidates[0]
                        except Exception:
                            pass
                        if final_path and os.path.isfile(final_path):
                            _channel_name = _uploader or "Single Videos"
                            try:
                                from backend import index as _idx
                                _idx.register_video(
                                    final_path, _channel_name, _title,
                                    tx_status="no_captions",
                                    video_id=_vid)
                            except Exception as _re:
                                self._log_stream.emit_dim(
                                    f" (index register failed: {_re})")
                            try:
                                sync_backend._record_recent_download(
                                    final_path, _channel_name, _title, _vid)
                            except Exception as _re:
                                self._log_stream.emit_dim(
                                    f" (recent downloads write failed: {_re})")
                            # Drop from deferred livestream journal if
                            # this was a previously-deferred premiere
                            # that's now finished (matches bug C-3).
                            if _vid:
                                try:
                                    from backend import livestreams as _ls
                                    _ls.drop(_vid)
                                except Exception:
                                    pass
                    except Exception as _pe:
                        self._log_stream.emit_dim(
                            f" (DLTRACK post-processing failed: {_pe})")
                # bug W-7: if a DLTRACK line never arrived AND yt-dlp
                # logged a known-failure pattern, report that to the
                # user via a visible error line + toast so they know
                # why their download vanished.
                if not _dltrack and _stderr_errors:
                    _reason = _stderr_errors[0]
                    self._log_stream.emit([
                        ["[Archive] ", "red"],
                        [f"Download failed \u2014 {_reason}.\n", "red"],
                    ])
                    try:
                        if self._window is not None:
                            import json as _json
                            self._window.evaluate_js(
                                "window._showToast && window._showToast("
                                f"{_json.dumps(f'Download failed: {_reason}')},"
                                " 'error');")
                    except Exception:
                        pass
                else:
                    self._log_stream.emit([["[Archive] ", "simpleline_green"],
                                            ["done.\n", "simpleline"]])
                self._log_stream.flush()
                # Push a Recent-tab refresh so the new video appears
                # immediately instead of waiting for the next tab
                # switch. Matches the channel-sync push hook.
                try:
                    self._push_recent_refresh()
                except Exception:
                    pass
            finally:
                # Always release the URL guard, even on exception, so
                # the user can retry without restarting the app.
                try:
                    with self._archive_single_lock:
                        self._archive_single_inflight.discard(url)
                except Exception:
                    pass

        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}

    def export_full_backup(self):
        """ZIP the user's config + queue state + cached ID list + seen-filters
        + disk cache + livestream journal into a user-picked file.

        audit C-14: also include the FTS transcript index DB when it's
        small enough to fit (< 2GB). Previously the DB was
        unconditionally skipped, which meant "full backup" restore
        returned a usable archive browser that then had EVERY
        transcript search return empty until the user kicked off a
        full re-transcribe. Now the authoritative search index rides
        along in the ZIP too — the backup is actually full.

        The 2GB cap is a pragmatic stop: ZIP deflate slows dramatically
        past that size and the ZIP64 format has its own constraints.
        For archives where the DB exceeds the cap, the UI surfaces a
        size warning so users can decide to export manually.
        """
        try:
            import io as _io, zipfile as _zf, webview as _wv
            from backend.ytarchiver_config import (
                CONFIG_FILE, QUEUE_FILE, DISK_CACHE_FILE,
                SEEN_FILTER_TITLES, CHANNEL_ID_CACHE, APP_DATA_DIR,
                TRANSCRIPTION_DB,
            )
            candidates = [
                CONFIG_FILE,
                QUEUE_FILE,
                DISK_CACHE_FILE,
                SEEN_FILTER_TITLES,
                CHANNEL_ID_CACHE,
                APP_DATA_DIR / "ytarchiver_livestream_defer.json",
                APP_DATA_DIR / "ytarchiver_pending_transcribe.json",
            ]
            # audit C-14: opt-in include of the FTS DB if it fits.
            _fts_skipped_reason = ""
            try:
                if TRANSCRIPTION_DB.exists():
                    _fts_sz = TRANSCRIPTION_DB.stat().st_size
                    if _fts_sz < 2 * 1024 * 1024 * 1024:
                        candidates.append(TRANSCRIPTION_DB)
                    else:
                        _fts_skipped_reason = (
                            f"FTS DB skipped — too large "
                            f"({_fts_sz / (1024**3):.1f} GB > 2 GB). "
                            f"Back up manually if needed.")
            except OSError:
                pass
            if self._window is None:
                return {"ok": False, "error": "No window"}
            import datetime as _dt
            ts = _dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
            paths = self._window.create_file_dialog(
                _wv.SAVE_DIALOG,
                save_filename=f"ytarchiver_backup_{ts}.zip",
            )
            if not paths:
                return {"ok": False, "cancelled": True}
            out_path = paths if isinstance(paths, str) else paths[0]
            n = 0
            with _zf.ZipFile(out_path, "w", _zf.ZIP_DEFLATED) as zf:
                for p in candidates:
                    if p.exists():
                        zf.write(str(p), arcname=p.name)
                        n += 1
                # Include latest dated snapshot as well
                backup_dir = APP_DATA_DIR / "backups"
                if backup_dir.is_dir():
                    snaps = sorted(backup_dir.glob("config_*.json"),
                                   key=lambda pp: pp.stat().st_mtime, reverse=True)
                    if snaps:
                        zf.write(str(snaps[0]), arcname=f"backups/{snaps[0].name}")
                        n += 1
            _resp = {"ok": True, "path": out_path, "files": n}
            if _fts_skipped_reason:
                _resp["fts_skipped"] = _fts_skipped_reason
            return _resp
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def import_full_backup(self):
        """Restore a previously-exported backup ZIP into %APPDATA%\\YTArchiver.

        Before overwriting any existing files, the current config is snapshotted
        to backups/config_pre_restore_YYYY-MM-DD_HHMMSS.json so the user can roll
        back. Gated by config_is_writable() — a read-only probe still
        lists the ZIP's contents so the frontend can confirm before committing.
        """
        try:
            import zipfile as _zf
            import datetime as _dt
            import shutil as _sh
            import webview as _wv
            from backend.ytarchiver_config import (
                CONFIG_FILE, APP_DATA_DIR, config_is_writable,
            )
            if self._window is None:
                return {"ok": False, "error": "No window"}
            paths = self._window.create_file_dialog(
                _wv.OPEN_DIALOG,
                allow_multiple=False,
                file_types=("Backup ZIP (*.zip)", "All files (*.*)"),
            )
            if not paths:
                return {"ok": False, "cancelled": True}
            zip_path = paths if isinstance(paths, str) else paths[0]

            # First pass: list contents (read-only; safe even if gated off).
            try:
                with _zf.ZipFile(zip_path, "r") as zf:
                    names = [n for n in zf.namelist() if not n.endswith("/")]
            except Exception as e:
                return {"ok": False, "error": f"Not a valid ZIP: {e}"}
            if not names:
                return {"ok": False, "error": "Backup is empty"}

            # Whitelist — only restore files we recognise from export.
            # audit C-14: also allow the FTS index DB so backups that
            # include it can restore cleanly.
            allowed_top = {
                "ytarchiver_config.json",
                "ytarchiver_queue.json",
                "ytarchiver_disk_cache.json",
                "ytarchiver_seen_filters.txt",
                "ytarchiver_channel_id_cache.json",
                "ytarchiver_livestream_defer.json",
                "ytarchiver_pending_transcribe.json",
                "transcription_index.db",
            }

            if not config_is_writable():
                return {
                    "ok": False,
                    "write_blocked": True,
                    "zip_path": zip_path,
                    "names": names,
                    "error": "Write-gate off",
                }

            # Snapshot current config BEFORE touching anything.
            backup_dir = APP_DATA_DIR / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            ts = _dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
            snap_path = backup_dir / f"config_pre_restore_{ts}.json"
            if CONFIG_FILE.exists():
                try:
                    _sh.copy2(str(CONFIG_FILE), str(snap_path))
                except Exception as e:
                    return {"ok": False, "error": f"Pre-restore snapshot failed: {e}"}

            # Extract whitelisted files.
            restored = []
            skipped = []
            # audit F-25: resolve APP_DATA_DIR once for path-containment
            # checks. Any target that doesn't resolve under it after
            # path-join gets rejected. This blocks a crafted ZIP with
            # "backups/../../../../Windows/File.json"-style names from
            # writing outside APP_DATA_DIR even though each individual
            # component looks innocuous.
            _app_data_resolved = Path(str(APP_DATA_DIR)).resolve()
            with _zf.ZipFile(zip_path, "r") as zf:
                for name in names:
                    # audit F-25: reject entries containing `..` OR a
                    # drive letter / absolute path. ZipFile preserves
                    # the raw name, which on Windows can contain
                    # drive-qualified paths that write anywhere.
                    _bad_chars = (".." in name.split("/")
                                  or name.startswith("/")
                                  or name.startswith("\\")
                                  or (len(name) > 1 and name[1] == ":"))
                    if _bad_chars:
                        skipped.append(f"{name} (rejected — suspicious path)")
                        continue
                    # Strip any directory prefix for top-level files; keep
                    # backups/config_*.json in its folder.
                    if name.startswith("backups/") and name.endswith(".json"):
                        target = APP_DATA_DIR / name
                    else:
                        base = os.path.basename(name)
                        if base not in allowed_top:
                            skipped.append(name)
                            continue
                        target = APP_DATA_DIR / base
                    # audit F-25: final containment check — resolve the
                    # target and require it to sit under APP_DATA_DIR.
                    try:
                        _t_resolved = Path(str(target)).resolve()
                        if not str(_t_resolved).startswith(
                                str(_app_data_resolved)):
                            skipped.append(f"{name} (rejected — escapes APP_DATA_DIR)")
                            continue
                    except Exception:
                        skipped.append(f"{name} (rejected — path resolve failed)")
                        continue
                    try:
                        target.parent.mkdir(parents=True, exist_ok=True)
                        with zf.open(name, "r") as src, open(target, "wb") as dst:
                            dst.write(src.read())
                        restored.append(target.name)
                    except Exception as e:
                        skipped.append(f"{name} ({e})")

            # Force a reload of in-memory config.
            self._reload_config()
            return {
                "ok": True,
                "files_restored": len(restored),
                "restored": restored,
                "skipped": skipped,
                "pre_restore_snapshot": str(snap_path) if CONFIG_FILE.exists() else None,
                "zip_path": zip_path,
                "needs_restart": True,
            }
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
            path = paths if isinstance(paths, str) else paths[0]
            with open(path, "w", encoding="utf-8") as f:
                f.write(text or "")
            return {"ok": True, "path": path}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ─── Last Full Sync live label ──────────────────────────────────────

    def get_last_sync_label(self):
        """Return a formatted 'Last Full Sync: HH:MMam/pm, Mon D (X ago)' string."""
        cfg = self._config or load_config()
        ts = cfg.get("last_sync", "") or ""
        return {"label": _format_last_sync_label(ts)}

    def set_parent_folder(self, path):
        """Update config['output_dir'] (gated by write env var)."""
        if not path:
            return {"ok": False, "error": "path required"}
        path = os.path.normpath(path)
        # bug W-4: verify the directory is accessible + writable before
        # we commit it. Previously any path was saved blindly, so a
        # read-only / unplugged / permission-denied path would be
        # accepted; later sync attempts would fail with cryptic
        # "write-gate blocked" errors. Probe with a real tmp file +
        # rmdir so we catch permission issues that os.access (advisory
        # on Windows) misses.
        if not os.path.isdir(path):
            return {"ok": False,
                    "error": f"Folder doesn't exist or isn't accessible: {path}"}
        _test_dir = os.path.join(path, ".ytarch-write-test")
        try:
            os.makedirs(_test_dir, exist_ok=True)
            try:
                os.rmdir(_test_dir)
            except OSError:
                pass
        except OSError as _pe:
            return {"ok": False,
                    "error": f"Folder isn't writable: {_pe}"}
        cfg = load_config()
        cfg["output_dir"] = path
        from backend.ytarchiver_config import save_config as _sc
        ok = _sc(cfg)
        if ok:
            self._reload_config()
            return {"ok": True, "path": path}
        return {"ok": False, "write_blocked": True, "path": path,
                "error": "Write-gate off"}

    def sync_one_channel(self, identity):
        """Sync just one channel (used by context-menu 'Sync now')."""
        if self.sync_is_running():
            return {"ok": False, "error": "Sync already running"}
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        if not sync_backend.find_yt_dlp():
            return {"ok": False, "error": "yt-dlp not found"}
        self._sync_cancel.clear()
        self._sync_pause.clear()
        ch_name = ch.get("name") or ch.get("folder", "")
        try:
            if getattr(self, "_tray", None):
                self._tray.start_spin("blue")
                self._tray.set_tooltip(f"YT Archiver \u2014 Syncing {ch_name}")
        except Exception:
            pass
        def _run():
            # Mirror sync_start_all's visual framing for single-channel
            # syncs: start-of-pass header, [1/1] live row, sync_channel
            # call, [1/1] done row, end-of-pass footer. Without this the
            # manual "Sync now" flow silently ran and the user never saw
            # the usual "[1/1] Name — no new videos" line they expected.
            import time as _t
            from backend.sync import (_sync_row_emit, _short_summary,
                                       _fmt_duration, _new_pass_id,
                                       _ROW_EMIT_PASS_ID)
            # Unique pass id so this channel's [1/1] row doesn't replace
            # a prior pass's [1/1] row in the scrollback (same bug class
            # as the autorun sync_all collision).
            _ROW_EMIT_PASS_ID.id = _new_pass_id()
            t0 = _t.time()
            try:
                self._log_stream.emit([
                    ["=== Sync pass starting ", "header"],
                    ["(1 channel) ===\n", "header"],
                ])
                _sync_row_emit(self._log_stream, 1, 1, ch_name)
                res = sync_backend.sync_channel(
                    ch, self._log_stream, self._sync_cancel,
                    queues=self._queues,
                    transcribe_mgr=self._transcribe,
                    pause_event=self._sync_pause,
                    pass_idx=1, pass_total=1,
                ) or {}
                _dl = int(res.get("downloaded", 0) or 0)
                _err = int(res.get("errors", 0) or 0)
                _sync_row_emit(
                    self._log_stream, 1, 1, ch_name,
                    summary=_short_summary(_dl, _err),
                    name_tag="simpleline_green" if _dl > 0 else "simpleline",
                    summary_tag="simpleline_green" if _dl > 0 else "dim",
                )
                self._log_stream.emit([
                    ["\n=== Pass complete: ", "header"],
                    [f"{_dl} downloaded \u00b7 {_err} errors \u00b7 took "
                     f"{_fmt_duration(_t.time() - t0)} ===\n", "header"],
                ])
            except Exception as e:
                self._log_stream.emit_error(f"Sync crashed: {e}")
            finally:
                try: _ROW_EMIT_PASS_ID.id = ""
                except Exception: pass
                try:
                    if getattr(self, "_tray", None):
                        self._tray.stop_spin()
                        self._tray.set_tooltip("YT Archiver \u2014 Idle")
                except Exception:
                    pass
                # TuneShine: clear stale sync-progress. Single-channel sync
                # was the bug path — sync_channel writes progress but never
                # cleared on its own, leaving TuneShine stuck on the Sync
                # screen forever. (OLD's _clear_sync_progress; YTArchiver.py:19671)
                try: sync_backend.clear_sync_progress()
                except Exception: pass
                self._log_stream.flush()
                self._on_queue_changed()
                # Reset autorun countdown so it doesn't keep showing
                # "Syncing..." now that this single-channel sync finished.
                try: self._autorun.notify_sync_done()
                except Exception: pass
                # Delayed second push so the Sync Tasks icon stops
                # blinking after a single-channel sync finishes
                # (same rationale as sync_start_all's fix).
                try: threading.Timer(0.5, self._on_queue_changed).start()
                except Exception: pass
        self._sync_thread = threading.Thread(target=_run, daemon=True)
        self._sync_thread.start()
        self._on_queue_changed()
        return {"ok": True, "started": True}

    def start_main_log_stream(self):
        """Kick off a background thread that pushes live log lines."""
        if self._stream_thread and self._stream_thread.is_alive():
            return False
        self._stream_stop.clear()
        self._stream_thread = threading.Thread(
            target=self._stream_worker, daemon=True
        )
        self._stream_thread.start()
        return True

    def stop_main_log_stream(self):
        self._stream_stop.set()
        return True

    def _stream_worker(self):
        """Push simulated live log lines to JS every ~400ms."""
        for line in stream_main_log_sample(initial=False):
            if self._stream_stop.is_set():
                break
            self._push_main_log_line(line)
            time.sleep(0.4)

    def _push_main_log_line(self, segments):
        """Call JS window.appendMainLog(segments) from Python."""
        if not self._window:
            return
        import json as _json
        payload = _json.dumps(segments)
        try:
            self._window.evaluate_js(f"window.appendMainLog({payload})")
        except Exception:
            pass


def _format_last_sync_label(ts_str):
    """Format stored last_sync timestamp (YYYY-MM-DD HH:MM) like YTArchiver.py:22157."""
    if not ts_str:
        return "Last Full Sync: Not yet synced"
    try:
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M")
    except Exception:
        return f"Last Full Sync: {ts_str}"
    now = datetime.now()
    diff_mins = max(0, int((now - dt).total_seconds() // 60))
    time_part = dt.strftime("%I:%M%p").lstrip("0").lower()
    # Use non-padded day where possible
    try:
        date_part = dt.strftime("%b %-d") if os.name != "nt" else dt.strftime("%b ") + str(dt.day)
    except Exception:
        date_part = dt.strftime("%b %d")
    if diff_mins < 1:
        ago = "just now"
    elif diff_mins < 60:
        ago = f"{diff_mins} min{'s' if diff_mins != 1 else ''} ago"
    else:
        hrs = diff_mins // 60
        if hrs < 24:
            ago = f"{hrs} hr{'s' if hrs != 1 else ''} ago"
        else:
            days = hrs // 24
            ago = f"{days} day{'s' if days != 1 else ''} ago"
    return f"Last Full Sync: {time_part}, {date_part} ({ago})"


def main():
    # Start the network-down monitor in the background
    try:
        net_backend.start_monitor()
    except Exception:
        pass

    # Dated config snapshot — cheap insurance against corruption
    try:
        snap = backup_config_on_start()
        if snap:
            print(f"[config] dated snapshot: {snap}")
    except Exception:
        pass

    # Start the localhost file server BEFORE creating the Api (so any early
    # URL generation has a port to bake in). Bound to 127.0.0.1 only.
    try:
        from backend import local_fileserver as _fs
        _port = _fs.start_server()
        print(f"[fileserver] serving local assets on 127.0.0.1:{_port}")
    except Exception as _fe:
        print(f"[fileserver] failed to start: {_fe}")

    api = Api()

    # HTTP command server on 127.0.0.1:9855 — lets the ArchivePlayer companion
    # and ArchiveBrowserWithYTTest viewers trigger retranscribe / ping
    # the app. Matches YTArchiver.py:34400 _start_cmd_server.
    try:
        from backend import cmd_server as _cmd
        def _cmd_log(msg): print(msg)
        def _handle_ping(_body):
            depth = 0
            try:
                q = api._queues.to_ui_payload() or {}
                depth = len(q.get("gpu", []))
            except Exception:
                pass
            return {"version": APP_VERSION, "gpu_depth": depth}
        def _handle_gpu_status(_body):
            try:
                q = api._queues.to_ui_payload() or {}
                return {"version": APP_VERSION,
                        "sync": len(q.get("sync", [])),
                        "gpu": len(q.get("gpu", [])),
                        "paused": q.get("gpu_paused", False)}
            except Exception as e:
                return {"error": str(e)}
        def _handle_retranscribe(body):
            """Queue a re-transcribe request from the ArchivePlayer
            companion or ArchiveBrowserWithYTTest. Mirrors OLD
            YTArchiver.py:34592 `_cmd_retranscribe`:
              - accepts {channel, video_id, [model]} (ArchivePlayer's
                actual request shape) OR {filepath, title, channel}
                (legacy/alternative)
              - validates the optional `model` against the supported set
              - disambiguates the DB lookup by channel when both
                channel + video_id are provided (two channels could
                in theory hold the same 11-char id)
              - routes through the same retranscribe path the UI uses
                (retranscribe=True), so the aggregated Transcript.txt /
                .jsonl entries get SURGICALLY SWAPPED, not duplicated.
                Without this, every ArchivePlayer re-transcribe would
                append a second entry to the aggregated files instead
                of replacing the stale one — bug was flagged earlier.
            """
            b = body or {}
            filepath = b.get("filepath") or ""
            video_id = (b.get("video_id") or "").strip()
            title = b.get("title") or ""
            channel = b.get("channel") or ""
            model = (b.get("model") or "").strip()
            _valid_models = {"tiny", "small", "medium", "large-v3"}
            if model and model not in _valid_models:
                return {"ok": False, "error": f"unknown model: {model}"}
            # Resolve filepath when only {channel, video_id} came in.
            if not filepath and video_id:
                try:
                    from backend import index as _idx
                    conn = _idx._open()
                    if conn is not None:
                        with _idx._db_lock:
                            if channel:
                                row = conn.execute(
                                    "SELECT filepath, title, channel FROM videos "
                                    "WHERE video_id=? AND channel=? LIMIT 1",
                                    (video_id, channel)).fetchone()
                            else:
                                row = conn.execute(
                                    "SELECT filepath, title, channel FROM videos "
                                    "WHERE video_id=? LIMIT 1",
                                    (video_id,)).fetchone()
                        if row:
                            filepath = filepath or row[0] or ""
                            title = title or row[1] or ""
                            channel = channel or row[2] or ""
                except Exception:
                    pass
            if not filepath or not os.path.isfile(filepath):
                return {"ok": False,
                        "error": f"Video not found on disk (id={video_id}, ch={channel})"}
            # Model swap BEFORE enqueue so this job runs under the
            # requested model. Matches OLD's per-job model attachment —
            # we use a global model so this is the simplest correct mapping.
            if model:
                try:
                    api._transcribe.swap_model(model)
                except Exception:
                    pass
            # Route through the same path the Watch-view re-transcribe
            # uses so retranscribe=True + video_id are forwarded, and the
            # completion callback push-updates any open Watch view.
            try:
                res = api.transcribe_retranscribe(filepath, title, video_id)
                if res and res.get("ok"):
                    return {"ok": True, "queued": True,
                            "video_id": video_id,
                            "model": model or None}
                return {"ok": False,
                        "error": (res or {}).get("error") or "enqueue failed"}
            except Exception as e:
                return {"ok": False, "error": str(e)}
        _cmd.register_handler("get", "/cmd/ping", _handle_ping)
        _cmd.register_handler("get", "/cmd/gpu-status", _handle_gpu_status)
        _cmd.register_handler("post", "/cmd/retranscribe", _handle_retranscribe)
        _cmd.register_handler("post", "/cmd/ping", _handle_ping)
        _cmd.start_server(APP_VERSION, on_log=_cmd_log)
    except Exception as _ce:
        print(f"[cmd] failed to start: {_ce}")

    # Load saved window state (position / size / maximized)
    ws = winstate.load_window_state()
    kwargs = dict(
        title="YTArchiver",
        url=str(INDEX),
        js_api=api,
        width=int(ws.get("width") or 1100),
        height=int(ws.get("height") or 780),
        min_size=(640, 480),
        background_color="#0f1012",
        resizable=True,
    )
    if ws.get("x") is not None and ws.get("y") is not None:
        kwargs["x"] = int(ws["x"])
        kwargs["y"] = int(ws["y"])
    window = webview.create_window(**kwargs)
    api.set_window(window)

    # Dark title bar via DWM (Windows 10 19041+ / Windows 11). Matches
    # OLD YTArchiver's `_apply_dark_title_bar` so the window chrome doesn't
    # flash white against the dark body during repaint. Best-effort — any
    # failure is silent.
    def _apply_dark_titlebar_when_ready():
        try:
            import ctypes as _ct
            # Wait for the native window handle to exist, up to ~2s
            for _ in range(40):
                try:
                    hwnd = _ct.windll.user32.FindWindowW(None, kwargs["title"])
                    if hwnd:
                        break
                except Exception:
                    hwnd = 0
                time.sleep(0.05)
            else:
                return
            # DWMWA_USE_IMMERSIVE_DARK_MODE = 20 on Win 10/11 (19041+);
            # fall back to attribute 19 for older builds.
            for attr in (20, 19):
                try:
                    val = _ct.c_int(1)
                    rc = _ct.windll.dwmapi.DwmSetWindowAttribute(
                        hwnd, attr, _ct.byref(val), _ct.sizeof(val))
                    if rc == 0:
                        break
                except Exception:
                    pass
        except Exception:
            pass
    threading.Thread(target=_apply_dark_titlebar_when_ready, daemon=True).start()

    # Register window-event handlers to save state on resize/move/close.
    def _on_resized(w, h):
        try:
            winstate.save_window_state({"width": int(w), "height": int(h)})
        except Exception:
            pass
    def _on_moved(x, y):
        try:
            winstate.save_window_state({"x": int(x), "y": int(y)})
        except Exception:
            pass
    def _on_maximized():
        try:
            winstate.save_window_state({"maximized": True})
        except Exception:
            pass
    def _on_restored():
        try:
            winstate.save_window_state({"maximized": False})
        except Exception:
            pass
    # X-button = real quit. Matches YTArchiver.py:34224 on_closing behavior —
    # clicking the window's X unconditionally shuts down (saves state, releases
    # mutex, kills subprocesses). Earlier builds tried to hide-to-tray here,
    # but that left orphan processes ("ghosts") whenever the Windows tray icon
    # wasn't pinned, with no way for the user to reopen or quit them.
    # Users who WANT to minimise to tray use the tray's "Hide" menu item, or
    # the system minimise button — not X.
    _truly_quit = {"flag": False}
    def _on_closing():
        try:
            winstate.save_window_state({})
            _truly_quit["flag"] = True
            _shutdown_cleanup()
        except Exception:
            pass
        return True # always allow close

    def _shutdown_cleanup():
        """Flush disk writes + kill child processes on real shutdown.

        Mirrors YTArchiver.py:34224 on_closing. Runs when the user closes
        the window (X / Ctrl+Q / tray Quit). Order matters — save before kill.
        """
        try:
            # 1. Persist queue state NOW (defeat the debounce timer).
            try: api._queues.save_now()
            except Exception: pass
            # 2. Save window geometry one last time.
            try: winstate.save_window_state({})
            except Exception: pass
            # 3. Stop the tray thread — otherwise pystray keeps the process
            # alive after the window destroys. This was the primary ghost
            # cause prior to 2026-04-18.
            try:
                if tray and getattr(tray, "_started", False):
                    tray.stop()
            except Exception: pass
            # 4. Terminate whisper + punctuation subprocesses cleanly.
            from backend.utils import kill_process
            try: api._transcribe._stop_subprocess(force=True)
            except Exception: pass
            try:
                punct = getattr(api._transcribe, "_punct", None)
                if punct is not None and getattr(punct, "_proc", None):
                    kill_process(punct._proc)
            except Exception: pass
            # 5. Cancel any in-flight sync (best effort).
            try: api._sync_cancel.set()
            except Exception: pass
            # audit D-57: also kill child yt-dlp / ffmpeg subprocesses
            # that the sync cancel event might not reach in time.
            # Without this, quitting during a sync leaves orphan
            # yt-dlp.exe / ffmpeg.exe finishing their current file
            # after the parent window is already gone → partial .part
            # files on disk. psutil is already a dependency.
            try:
                import psutil as _ps
                _me = _ps.Process(os.getpid())
                for _ch in _me.children(recursive=True):
                    _name = ""
                    try: _name = (_ch.name() or "").lower()
                    except Exception: pass
                    if any(k in _name for k in
                           ("yt-dlp", "yt_dlp", "ffmpeg", "ffprobe")):
                        try: _ch.terminate()
                        except Exception: pass
                # Give them a moment, then force-kill stragglers.
                try:
                    _gone, _alive = _ps.wait_procs(_me.children(recursive=True),
                                                    timeout=1.5)
                    for _ch in _alive:
                        try: _ch.kill()
                        except Exception: pass
                except Exception:
                    pass
            except Exception:
                pass
            # audit D-56: stop the HTTP servers so port 9855 (cmd)
            # and the local fileserver port are released cleanly. If
            # webview.start() ever returns through a path that doesn't
            # immediately os._exit, these kept the process alive AND
            # held the ports until reboot — re-launch then failed to
            # bind 9855.
            try:
                from backend import cmd_server as _cs
                _cs.stop_server()
            except Exception:
                pass
            try:
                from backend import local_fileserver as _lfs
                _stop = getattr(_lfs, "stop_server", None)
                if callable(_stop):
                    _stop()
            except Exception:
                pass
            # 6. Clear TuneShine sync-progress so it doesn't show stale
            # "running" state after the app closes. Matches OLD's
            # YTArchiver.py:34306 _clear_sync_progress() shutdown call.
            try: sync_backend.clear_sync_progress()
            except Exception: pass
        except Exception:
            pass

    try:
        window.events.resized += _on_resized
        window.events.moved += _on_moved
        window.events.maximized += _on_maximized
        window.events.restored += _on_restored
        window.events.closing += _on_closing
    except Exception:
        # pywebview may not expose all of these on every platform/version
        pass

    # Start tray icon (optional — silently no-ops if pystray is missing)
    def _tray_show():
        try:
            window.show()
            window.restore()
        except Exception:
            pass
        # bug W-5: show()+restore() are no-ops when the window is
        # already visible-but-behind-other-apps. Force it to the front
        # via Win32 SetForegroundWindow since pywebview 5.x doesn't
        # expose a reliable set_focus on Windows. Silently no-op on
        # other platforms / when ctypes/win32 are unavailable.
        if os.name == "nt":
            try:
                import ctypes as _ct
                _u32 = _ct.windll.user32
                hwnd = _u32.FindWindowW(None, "YTArchiver")
                if hwnd:
                    _u32.ShowWindow(hwnd, 9)  # SW_RESTORE
                    _u32.SetForegroundWindow(hwnd)
            except Exception:
                pass
    def _tray_hide():
        try:
            window.hide()
        except Exception:
            pass
    def _tray_sync():
        try:
            api.sync_start_all()
        except Exception:
            pass
    def _tray_quit():
        _truly_quit["flag"] = True
        try:
            window.destroy()
        except Exception:
            pass

    tray = TrayController(on_show=_tray_show, on_hide=_tray_hide,
                          on_sync=_tray_sync, on_quit=_tray_quit,
                          tooltip="YT Archiver \u2014 Idle")
    # Always-on-top toggle (restore saved pref, defaults to off)
    _on_top_state = {"on": bool(ws.get("always_on_top", False))}
    def _toggle_on_top():
        _on_top_state["on"] = not _on_top_state["on"]
        try:
            window.on_top = _on_top_state["on"]
        except Exception:
            pass
        tray._always_on_top = _on_top_state["on"]
        try:
            winstate.save_window_state({"always_on_top": _on_top_state["on"]})
        except Exception:
            pass
    tray.set_on_top_toggle(_toggle_on_top, initial=_on_top_state["on"])
    # Auto-Sync submenu — radio interval items ("Off", "30 min", ... "24 hr").
    # Matches YTArchiver.py:3671 pystray.MenuItem("Auto-Sync", ...).
    try:
        from backend.autorun import AUTORUN_LABELS as _AR_LABELS
        def _tray_get_autorun_label():
            try: return api.autorun_state().get("label", "Off")
            except Exception: return "Off"
        def _tray_set_autorun_label(lbl):
            try: api.autorun_set(lbl)
            except Exception: pass
        tray.set_autorun_menu(_AR_LABELS, _tray_get_autorun_label, _tray_set_autorun_label)
    except Exception:
        pass
    # Apply the saved always-on-top state to the window itself
    if _on_top_state["on"]:
        try: window.on_top = True
        except Exception: pass
    tray.start()
    api.attach_tray(tray)

    # Start the network-down monitor so sync workers can pause on outage.
    # Cheap (single background thread, 30s TCP probe); no-op on full uptime.
    try:
        from backend import net as _net
        _net.start_monitor()
    except Exception:
        pass

    # Startup sanity checks — dependency probe, missing folder scan, update
    # ping, leftover temp/partial file sweep. All run in background threads
    # so they never block window.show.
    def _startup_checks():
        try: api.check_dependencies()
        except Exception: pass
        try: api.check_channel_folders()
        except Exception: pass
        try: api.check_app_update()
        except Exception: pass
        try:
            from backend.temp_cleanup import startup_cleanup_temps
            startup_cleanup_temps(api._log_stream)
        except Exception: pass
        # Upload-timestamp backfill — needed for the Graph tab's Week
        # bucket. Populates videos.upload_ts from each file's mtime
        # (which yt-dlp set to the YouTube upload date via --mtime).
        # Runs once per launch; idempotent (only fills NULL rows).
        # Background thread so a large archive doesn't slow boot.
        try:
            from backend.index import backfill_upload_ts as _backfill
            _backfill()
        except Exception: pass
    threading.Thread(target=_startup_checks, daemon=True).start()

    webview.start(debug=False)
    # After webview returns, stop the tray icon so pystray's mainloop exits
    try:
        tray.stop()
    except Exception:
        pass
    # Hard-exit to reap any lingering non-daemon threads (log stream writer,
    # autorun scheduler, etc.). Without this, pywebview + pystray together
    # can pin the process alive even after the window destroys. os._exit
    # skips atexit handlers — but all persistent state was already flushed
    # by _shutdown_cleanup above.
    os._exit(0)


if __name__ == "__main__":
    main()
