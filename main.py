"""
YTArchiver — pywebview shell.

Run with Python 3.13 (pywebview ships; PATH's 3.11 doesn't).
"""

import ctypes
import os
import sys
import threading
import time
from pathlib import Path

_BOOT_T0 = time.perf_counter()
_BOOT_TRACE_ENABLED = os.environ.get("YTARCHIVER_BOOT_TRACE") == "1"


def _boot_trace(label: str) -> None:
    """Append a tiny boot timing mark for packaged startup diagnosis."""
    if not _BOOT_TRACE_ENABLED:
        return
    try:
        base = (os.environ.get("APPDATA")
                or os.environ.get("LOCALAPPDATA")
                or str(Path.home()))
        trace_dir = Path(base) / "YTArchiver"
        trace_dir.mkdir(parents=True, exist_ok=True)
        elapsed = time.perf_counter() - _BOOT_T0
        wall = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        exe_name = Path(sys.executable).name
        with (trace_dir / "boot_trace.log").open("a", encoding="utf-8") as f:
            f.write(f"{wall}\tpid={os.getpid()}\t+{elapsed:.3f}s\t"
                    f"{label}\texe={exe_name}\n")
    except Exception:
        pass


_boot_trace("module start")


def _ensure_webview2_browser_args() -> None:
    """Disable WebView2's video overlay plane before the control is created."""
    if os.name != "nt":
        return
    key = "WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS"
    switch = "--disable-direct-composition-video-overlays"
    current = os.environ.get(key, "").strip()
    if switch in current:
        return
    os.environ[key] = f"{current} {switch}".strip()


_ensure_webview2_browser_args()

# APP_VERSION + APP_VERSION_DATE live in backend/version.py — bump THERE
# (+0.1, single-decimal rollover) on every push. Surfaced in the window
# title, /cmd/ping, and the header bar.
from backend.version import APP_VERSION

# ── Single-instance mutex ──────────────────────────────────────────────
_INSTANCE_MUTEX = None
if os.name == "nt":
    _INSTANCE_MUTEX = ctypes.windll.kernel32.CreateMutexW(
        None, False, "Local\\YTArchiver_SingleInstance")
    if ctypes.windll.kernel32.GetLastError() == 183: # ERROR_ALREADY_EXISTS
        # Another instance is running — focus its window and exit.
        import ctypes.wintypes as _wt
        _WNDENUMPROC = ctypes.WINFUNCTYPE(ctypes.c_bool, _wt.HWND, _wt.LPARAM)

        def _window_belongs_to_this_exe(hwnd) -> bool:
            pid = _wt.DWORD()
            try:
                ctypes.windll.user32.GetWindowThreadProcessId(
                    hwnd, ctypes.byref(pid))
                if not pid.value:
                    return False
                _k32 = ctypes.windll.kernel32
                _k32.OpenProcess.restype = _wt.HANDLE
                h_proc = _k32.OpenProcess(
                    0x1000, False, pid.value)  # PROCESS_QUERY_LIMITED_INFORMATION
                if not h_proc:
                    return False
                try:
                    size = _wt.DWORD(32768)
                    buf = ctypes.create_unicode_buffer(size.value)
                    ok = _k32.QueryFullProcessImageNameW(
                        h_proc, 0, buf, ctypes.byref(size))
                    if not ok:
                        return False
                    return (Path(buf.value).name.lower()
                            == Path(sys.executable).name.lower())
                finally:
                    _k32.CloseHandle(h_proc)
            except Exception:
                return False

        def _find_and_focus(hwnd, _):
            _n = ctypes.windll.user32.GetWindowTextLengthW(hwnd)
            if _n > 0:
                _buf = ctypes.create_unicode_buffer(_n + 1)
                ctypes.windll.user32.GetWindowTextW(hwnd, _buf, _n + 1)
                # Accept both historical title spellings when focusing an
                # existing instance.
                _tv = _buf.value
                if _tv in {"YTArchiver", "YT Archiver"} \
                        and _window_belongs_to_this_exe(hwnd):
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
    _boot_trace("webview imported")
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
    except Exception as e:
        # _log isn't defined yet at this stage (set below) — use print
        # so a secondary ctypes failure doesn't mask the original
        # ImportError with NameError.
        print(f"[YTArchiver] pywebview ImportError MessageBox failed: {e}")
    sys.exit(1)

ROOT = Path(__file__).resolve().parent
WEB = ROOT / "web"
INDEX = WEB / "index.html"
_boot_trace("paths resolved")

# Phase 0 demo-data shim (backend/sample_logs.py + web/sample.json) was
# removed once real backends were wired up. No-config / DEMO_MODE paths
# now just return empty results — the UI handles those cleanly.
sys.path.insert(0, str(ROOT))

# web/index.html is built from a template and partials. Assemble the output
# file before pywebview opens the window so the UI sees a complete HTML
# page. Idempotent: skipped if index.html is already up to date.
try:
    from backend.html_assembler import assemble_index_html
    assemble_index_html(WEB)
    _boot_trace("html assembled")
except Exception as _e:  # pragma: no cover - boot-time best effort
    print(f"[html_assembler] could not (re)build index.html: {_e}")
    _boot_trace("html assemble failed")

from backend import auto_backup as auto_backup_backend
from backend import autorun as autorun_backend
from backend import index as index_backend
from backend import net as net_backend
from backend import sync as sync_backend
from backend import trash_retention as trash_retention_backend
from backend import window_state as winstate
from backend.archive_capacity import archive_capacity_status
from backend.log import get_logger as _get_logger
from backend.log import install as _install_log_bridge
from backend.log_stream import LogStreamer
from backend.queues import QueueState
from backend.services import AppServices, BridgeEventBus
from backend.services.job_supervisor import JobSupervisor, OwnerAdapter
from backend.services.managed_work import (
    start_managed_task,
    try_global_archive_lease,
)
from backend.transcribe import TranscribeManager
from backend.tray import TrayController, activity_spin_color
from backend.ytarchiver_config import (
    CONFIG_FILE,
    backup_config_on_start,
    config_file_exists,
    load_config,
    save_config,
    update_config,
)

_boot_trace("backend imports complete")

_log = _get_logger("main")


from backend.api_mixins import (
    ArchiveMixin,
    BackupMixin,
    BookmarkMixin,
    BrowseMixin,
    ChannelMixin,
    DiagnosticsMixin,
    IndexMixin,
    InfoMixin,
    LivestreamsMixin,
    MediaOpsMixin,
    MetadataMixin,
    OnboardingMixin,
    QueueMixin,
    RecentMixin,
    RedownloadMixin,
    SettingsMixin,
    StartupMixin,
    SubsMixin,
    SyncMixin,
    ThumbnailMixin,
    TranscribeMixin,
    TrashMixin,
    VideoMixin,
    WindowMixin,
)

_boot_trace("api mixins imported")


class Api(ArchiveMixin, BackupMixin, BookmarkMixin, BrowseMixin, ChannelMixin, DiagnosticsMixin, IndexMixin, InfoMixin, LivestreamsMixin, MediaOpsMixin, MetadataMixin, OnboardingMixin, QueueMixin, RecentMixin, RedownloadMixin, SettingsMixin, StartupMixin, SubsMixin, SyncMixin, ThumbnailMixin, TrashMixin, TranscribeMixin, VideoMixin, WindowMixin):
    """
    Exposed to JS as window.pywebview.api.*

    Phase 0: only enough to seed logs with test data.
    Later phases: add every YTArchiver action here.
    """

    def __init__(self):
        self._window = None
        self._config = None
        self._job_supervisor = JobSupervisor()
        self._restore_quiesced = False
        self._restore_state_committed = False
        self._log_stream = LogStreamer(None)
        _install_log_bridge(self._log_stream)
        self._sync_thread = None
        self._sync_cancel = threading.Event()
        # Redownloads get their OWN cancel event. They used to borrow
        # _sync_cancel, which every sync-stop leaves SET until the next
        # sync starts — so redownloads begun in that window ghost-
        # cancelled instantly, and one per-channel redownload cancel
        # killed the whole queued chain. Global stop paths set BOTH.
        self._redwnl_cancel = threading.Event()
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
        # Pre-init per-mixin pending dicts + their locks so two
        # concurrent first-callers from JS can't both lazy-init,
        # silently clobbering each other's Lock object and dropping
        # mutex semantics on subsequent calls (audit: HIGH H4 — same
        # pattern in channel_mixin, index_mixin, subs_mixin,
        # media_ops_mixin). Created up-front so the JS bridge never
        # races the first hasattr() check.
        self._pending_res_scans: dict = {}
        self._pending_res_scans_lock = threading.Lock()
        self._delete_transcripts_lock = threading.Lock()
        # Pair each lock with its running-flag. index_mixin only created
        # these flags inside `if not hasattr(self, "_..._lock")`, but since
        # the lock is pre-init'd here that block is skipped — leaving the
        # flag undefined and the maintenance buttons raising AttributeError
        # on first read (audit: index_mixin H/Codex). Init them here too.
        self._delete_transcripts_running = False
        self._fts_rebuild_lock = threading.Lock()
        self._fts_rebuild_running = False
        self._pending_previews: dict = {}
        self._pending_previews_lock = threading.Lock()
        self._drift_scan_results: dict = {}
        self._drift_apply_results: dict = {}
        self._drift_scan_lock = threading.Lock()
        self._drift_apply_lock = threading.Lock()
        # Audit H5: the remaining mixin shared state that was still
        # lazy-init'd via `if not hasattr(...)`. Pre-init up-front so two
        # concurrent JS-bridge first-callers can't both create-and-clobber
        # the lock/map (sync_mixin could otherwise double-spawn a sync).
        # The mixins' hasattr branches stay as harmless fallbacks (they
        # also cover the rare reset-to-None paths).
        self._sync_start_lock = threading.Lock()
        self._sync_mutation_lock = threading.RLock()
        self._chan_art_inflight: set = set()
        self._chan_art_lock = threading.Lock()
        self._pending_metadata_choices: dict = {}
        self._pending_metadata_choices_lock = threading.Lock()
        self._redwnl_samples: dict = {}
        # Lock for the session-download counter + tray badge update
        # below — read-modify-write under contention from multiple log
        # scanner threads (audit: HIGH H14).
        self._session_dl_count_lock = threading.Lock()
        # Native error indicator mirrors the bottom status bar's session-error
        # count exactly. The frontend owns that list and explicitly clears it.
        self._session_error_count = 0
        # Archive-rescan single-flight + live progress state.  The rescan
        # mixin also lazy-initializes these for narrow test doubles, but the
        # real Api owns them up front so simultaneous first bridge calls can
        # never create competing locks.
        self._archive_rescan_lock = threading.Lock()
        self._archive_rescan_running = False
        self._archive_rescan_progress = {
            "running": False,
            "phase": "idle",
            "current": 0,
            "total": 0,
            "phase_current": 0,
            "phase_total": 0,
            "percent": 0,
            "message": "",
            "started_at": None,
            "error": "",
        }
        # Reentrant close-dialog guard — _on_closing's ask-path used
        # to spawn an unbounded thread per X click; flag here so
        # additional clicks while a dialog is pending no-op (audit:
        # HIGH H24).
        self._close_dialog_pending = False
        # Construct QueueState BEFORE TranscribeManager (audit:
        # main.py:140-201). The transcribe manager's __init__ can
        # eventually fire callbacks that touch self._queues; building
        # the queue object first ensures `self._queues` exists if any
        # of those callbacks race init.
        # wrap queue load in try/except so a corrupt queue file
        # doesn't brick the entire app — log + start empty, and back
        # up the corrupt file for debugging.
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
            # Disable atexit on the discarded instance so its atexit
            # handler can't fire at process exit and clobber the
            # corrupt-but-recoverable on-disk queue file with this
            # orphan's empty state. The fresh replacement instance
            # below registers its own atexit hook.
            try:
                self._queues.mark_orphan()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            # Reset to a fresh empty state so the app can still launch.
            self._queues = QueueState()
        # Pull whisper model from config so Settings changes actually take
        # effect on next launch. Without this the TranscribeManager defaults
        # to "large-v3" regardless of what the user picked in Settings.
        _init_model = (load_config() or {}).get("whisper_model") or "small"
        self._transcribe = TranscribeManager(self._log_stream, model=_init_model)
        self._event_bus = BridgeEventBus(lambda: self._window)
        self.services = AppServices(
            load_config=load_config,
            save_config=save_config,
            queues=self._queues,
            log_stream=self._log_stream,
            transcribe=self._transcribe,
            event_bus=self._event_bus,
            update_config=update_config,
        )

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
        # One process-wide YouTube guard covers every yt-dlp feature.  Direct
        # callers proactively validate Firefox cookies through the shared
        # cookie-source helper; this scanner is the final safety net for any
        # YouTube error that reaches the log.
        try:
            from backend import youtube_session as _yt_session
            _yt_session.configure(
                self._log_stream, self._sync_pause, self._queues)
            self._log_stream.add_line_scanner(_yt_session.scan_log_line)
        except Exception as e:
            _log.debug("YouTube session guard setup failed: %s", e)
        # Unseen-download counter → tray + taskbar badge overlay. Downloads
        # completed while the window is focused are already visible to the
        # user, so only count activity after the app loses focus. Focusing the
        # app again clears the badge through app_window_focus_changed().
        self._session_dl_count = 0
        self._window_focused = True
        def _dl_scan(_text: str) -> None:
            try:
                # The semantic marker fires only after DLTRACK confirms a
                # merged file landed. This cannot count a started-but-failed
                # download or a metadata/transcription checkmark.
                with self._session_dl_count_lock:
                    if self._window_focused:
                        return
                    self._session_dl_count += 1
                    _badge_val = self._session_dl_count
                tray = getattr(self, "_tray", None)
                if tray is not None:
                    try: tray.set_badge(_badge_val)
                    except Exception as e: _log.debug("tray badge set failed: %s", e)
            except Exception as e:
                _log.debug("_dl_scan failed: %s", e)
        self._log_stream.add_tag_scanner("download_complete", _dl_scan)
        # Note: self._queues was already constructed + loaded above
        # (moved earlier for audit E-60 — DiskErrorMonitor references it).
        # Connect the transcribe manager to the shared GPU queue so
        # enqueued jobs show up in the Tasks popover + the Auto
        # checkbox actually gates firing. Without this the manager's
        # internal `_jobs` list is invisible to the UI (a bug:
        # auto-transcribe on a channel, no task appeared in GPU Tasks).
        self._transcribe.attach_queues(self._queues, cfg_loader=load_config)
        # Requeue any in-flight items persisted at last shutdown/crash.
        # save_now writes current_sync/current_gpu into a `resuming`
        # dict and load() stashes them via get_loaded_resuming() — but
        # nothing ever consumed the stash, so the mid-channel sync and
        # the mid-flight GPU job silently vanished on every relaunch.
        # Restore them BEFORE the force-pause check below so the
        # never-auto-start rule applies to restored items too.
        try:
            _resuming = self._queues.get_loaded_resuming()
            _consumed_resuming = []
            _rs = _resuming.get("sync")
            if isinstance(_rs, dict):
                _rs.pop("_in_flight", None)
                self._queues.sync_requeue_front(_rs)
                _consumed_resuming.append("sync")
            _rg = _resuming.get("gpu")
            if isinstance(_rg, dict):
                _rg.pop("_in_flight", None)
                self._queues.gpu_enqueue(_rg)
                _consumed_resuming.append("gpu")
            if _consumed_resuming:
                self._queues.clear_resuming_slots(*_consumed_resuming)
            if isinstance(_rs, dict) or isinstance(_rg, dict):
                _log.info("restored in-flight queue item(s) from last session")
        except Exception as e:
            _log.debug("resuming-restore failed: %s", e)
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
            except Exception as e: _log.debug("transcribe pause-on-restore failed: %s", e)
            # restored=True keeps the "prior-session pause" marker alive so a
            # fresh auto-sync download (Auto on) can auto-release this backlog
            # via sync_start / enqueue instead of stranding it behind the
            # launch pause until the user clicks Resume by hand.
            self._queues.set_gpu_paused(True, restored=True)
        else:
            self._queues.set_gpu_paused(False)
        if _had_sync_items or _had_gpu_items:
            # Deferred until log pipeline is up — emit via a micro-delay so
            # the notice lands after any startup banner. Matches OLD's
            # "⏸ Sync queue restored — PAUSED" ui_queue post.
            _restore_notice_cancel = threading.Event()

            def _emit_restore_notice():
                try:
                    if _restore_notice_cancel.wait(0.5):
                        return
                    self._log_stream.emit([
                        ["\u23f8 Queue restored \u2014 ", "pauselog"],
                        ["PAUSED (click Resume to continue)\n", "pauselog"],
                    ])
                    self._log_stream.flush()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            try:
                start_managed_task(
                    self,
                    owner="ui-notice",
                    label="Restored queue notice",
                    target=_emit_restore_notice,
                    cancel=_restore_notice_cancel,
                    name="restored-queue-notice",
                )
            except Exception as e:
                _log.debug("restored queue notice not started: %s", e)
        self._queues.add_listener(self._on_queue_changed)
        # Rolling-budget waits are process-wide state, not QueueState
        # mutations. Push them through the same UI state channel so the
        # global Pause button and footer update immediately when a worker
        # parks for (or leaves) an hourly/24-hour slot.
        try:
            from backend import youtube_traffic as _yt_traffic
            _yt_traffic.add_wait_listener(
                lambda _state: self._on_queue_changed())
        except Exception as e:
            _log.debug("YouTube traffic wait listener setup failed: %s", e)
        # Recent-tab live refresh — every download triggers this via
        # sync._record_recent_download so the Recent grid/list updates
        # without needing an app restart.
        try:
            sync_backend.set_recent_changed_hook(self._push_recent_refresh)
        except Exception as e: _log.debug("set_recent_changed_hook failed: %s", e)
        # Metadata-tab live refresh — fires after every metadata /
        # metadata_comments / videoid_backfill task so the XXm-ago
        # timestamps and Video IDs status update in place without
        # requiring the user to click Reload.
        try:
            sync_backend.set_metadata_changed_hook(self._push_metadata_refresh)
        except Exception as e: _log.debug("set_metadata_changed_hook failed: %s", e)
        # Subs-tab live refresh — fires after each channel finishes in
        # a sync pass so the "Last Sync" column updates in place as the
        # pass advances. Without this the column stayed frozen at the
        # boot-time values until the user clicked away and back.
        try:
            sync_backend.set_channel_synced_hook(self._push_subs_table_refresh)
        except Exception as e: _log.debug("set_channel_synced_hook failed: %s", e)
        # Autorun scheduler — trigger kicks sync_start_all on the scheduled thread.
        # Passing `sync_busy_fn` so the scheduler can (a) postpone a fire
        # when sync is already running and (b) hold the countdown visible
        # at "Syncing..." until the current sync completes, matching
        # classic's _run_autorun + _schedule_autorun behavior.
        def _autorun_busy_reason():
            # Background autorun never competes with user-visible work or a
            # catalog writer.  A rescan remains busy even while it temporarily
            # releases _db_lock between channels.
            if self.sync_is_running() or self.archive_single_is_running():
                return "a download or sync"
            if self.archive_rescan_is_running():
                return "an archive rescan"
            if index_backend.is_db_writer_busy():
                return "database maintenance"
            return ""

        self._autorun = autorun_backend.AutorunScheduler(
            sync_trigger=lambda reservation_id=None: self.sync_start_all(
                scheduled=True,
                traffic_reservation_id=reservation_id,
            ),
            stream=self._log_stream,
            sync_busy_fn=_autorun_busy_reason,
            startup_grace_seconds=(
                autorun_backend.BUDGET_STARTUP_GRACE_SECONDS),
            startup_required_signals=("checks", "indexing"),
        )
        # v80 scheduled backups — daemon thread, no-op while the
        # Settings > Auto-backup cadence is "off". First check fires
        # ~2 min after boot so it never competes with startup work.
        self._auto_backup = auto_backup_backend.AutoBackupScheduler(
            stream=self._log_stream, queue_state=self._queues)
        self._auto_backup.start()

        def _trash_retention_busy_reason():
            # Permanent cleanup is lowest-priority housekeeping. It can wait
            # until every user-visible writer is idle rather than competing
            # with downloads, processing, or catalog maintenance.
            if self.sync_is_running() or self.archive_single_is_running():
                return "a download or sync"
            if self.archive_rescan_is_running():
                return "an archive rescan"
            if getattr(self._queues, "current_gpu", None):
                return "the processing queue"
            if index_backend.is_db_writer_busy():
                return "database maintenance"
            return ""

        def _trash_retention_cleanup(**kwargs):
            from backend.trash_manager import purge_expired, trash_manager

            lease_result = try_global_archive_lease(
                owner="scheduler-trash-retention",
                label="Automatic Trash cleanup",
                task_id="daily-cleanup",
                cancel=kwargs.get("cancel_event"),
            )
            if not lease_result.ok or lease_result.lease is None:
                return {
                    "ok": True,
                    "deferred": True,
                    "reason": "archive_busy",
                    "purged": 0,
                }
            with lease_result.lease:
                result = purge_expired(**kwargs)
            if isinstance(result, dict) and int(result.get("purged") or 0) > 0:
                try:
                    self._trash_event(
                        "_onTrashChanged", trash_manager.summary())
                except Exception as exc:
                    _log.debug("Trash retention UI refresh failed: %s", exc)
            return result

        self._trash_retention = (
            trash_retention_backend.TrashRetentionScheduler(
                cleanup_fn=_trash_retention_cleanup,
                busy_fn=_trash_retention_busy_reason,
                startup_required_signals=("checks", "indexing"),
            )
        )
        self._register_background_owners()
        self._reload_config()
        self._trash_retention.start()
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
                _blank_warn_cancel = threading.Event()

                def _emit_blank_warn():
                    try:
                        if _blank_warn_cancel.wait(0.8):
                            return
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
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                start_managed_task(
                    self,
                    owner="ui-notice",
                    label="Blank channel warning",
                    target=_emit_blank_warn,
                    cancel=_blank_warn_cancel,
                    name="blank-channel-warning",
                )
        except Exception as e:
            _log.debug("swallowed: %s", e)

    @staticmethod
    def _thread_is_alive(thread_obj) -> bool:
        try:
            return bool(thread_obj is not None and thread_obj.is_alive())
        except Exception:
            # A failed status probe cannot prove that the owner is stopped.
            return True

    @classmethod
    def _join_threads(cls, threads, timeout: float) -> bool:
        """Join a snapshot of threads within one shared monotonic deadline."""
        deadline = time.monotonic() + max(0.0, float(timeout))
        unique = []
        seen = set()
        for thread_obj in threads:
            if thread_obj is None or id(thread_obj) in seen:
                continue
            seen.add(id(thread_obj))
            unique.append(thread_obj)
        for thread_obj in unique:
            if not cls._thread_is_alive(thread_obj):
                continue
            remaining = max(0.0, deadline - time.monotonic())
            try:
                if thread_obj is not threading.current_thread():
                    thread_obj.join(timeout=remaining)
            except Exception:
                pass
        return not any(cls._thread_is_alive(row) for row in unique)

    def _archive_download_threads(self):
        """Return tracked manual-download workers (old test doubles are safe)."""
        lock = getattr(self, "_archive_single_lock", None)
        threads = getattr(self, "_archive_single_threads", {})
        try:
            if lock is not None:
                with lock:
                    return list(threads.values()) if isinstance(threads, dict) else []
        except Exception:
            pass
        return list(threads.values()) if isinstance(threads, dict) else []

    @staticmethod
    def _terminate_process_owners(*owners: str) -> int:
        """Terminate only explicitly registered process owners."""
        from backend.process_runner import PROCESS_REGISTRY

        return PROCESS_REGISTRY.terminate_owners(owners, timeout=2.0)

    @staticmethod
    def _registered_processes_active() -> bool:
        try:
            from backend.process_runner import PROCESS_REGISTRY
            return bool(PROCESS_REGISTRY.snapshot())
        except Exception:
            return True

    @staticmethod
    def _join_registered_processes(timeout: float) -> bool:
        """Wait for registered children to finish without guessing names."""
        from backend.process_runner import PROCESS_REGISTRY

        deadline = time.monotonic() + max(0.0, float(timeout))
        while PROCESS_REGISTRY.snapshot() and time.monotonic() < deadline:
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        return not PROCESS_REGISTRY.snapshot()

    @staticmethod
    def _force_registered_processes() -> int:
        """Stop each exact remaining registered root and its descendants."""
        from backend.process_runner import PROCESS_REGISTRY

        processes = [record.proc for record in PROCESS_REGISTRY.snapshot()]
        return PROCESS_REGISTRY.terminate_processes(processes, timeout=2.0)

    def _prepare_sync_owner(self) -> bool:
        self._sync_cancel.set()
        self._redwnl_cancel.set()
        return bool(self._queues.save_now())

    def _prepare_reorg_owner(self) -> bool:
        event = getattr(self, "_reorg_cancel", None)
        if event is not None:
            event.set()
        return True

    def _prepare_ytdlp_monitor_owner(self) -> bool:
        self.stop_ytdlp_update_monitor(timeout=0)
        return True

    def _ytdlp_monitor_active(self) -> bool:
        monitor = getattr(self, "_ytdlp_monitor_thread", None)
        update = getattr(self, "_ytdlp_update_thread", None)
        running = bool(getattr(self, "_ytdlp_update_running", False))
        return (self._thread_is_alive(monitor)
                or self._thread_is_alive(update) or running)

    def _join_ytdlp_monitor(self, timeout: float) -> bool:
        return self._join_threads(
            [getattr(self, "_ytdlp_monitor_thread", None),
             getattr(self, "_ytdlp_update_thread", None)], timeout)

    def _register_background_owners(self) -> None:
        """Register every long-lived owner needed by restore and shutdown."""
        register = self._job_supervisor.register_owner

        register(OwnerAdapter(
            owner="scheduler-autorun", label="Automatic sync scheduler",
            active=self._autorun.is_alive,
            prepare=lambda: (self._autorun.cancel() or True),
            join=self._autorun.join, force=lambda: 0,
        ))
        register(OwnerAdapter(
            owner="scheduler-backup", label="Automatic backup scheduler",
            active=self._auto_backup.is_alive,
            prepare=lambda: (self._auto_backup._stop.set() or True),
            join=self._auto_backup.stop, force=lambda: 0,
        ))
        register(OwnerAdapter(
            owner="scheduler-trash-retention",
            label="Automatic Trash cleanup",
            active=self._trash_retention.is_alive,
            prepare=self._trash_retention.request_stop,
            join=self._trash_retention.join,
            force=lambda: 0,
            details=self._trash_retention.snapshot,
        ))
        register(OwnerAdapter(
            owner="monitor-disk", label="Disk error monitor",
            active=self._disk_mon.is_alive,
            prepare=lambda: (self._disk_mon.stop(0) or True),
            join=self._disk_mon.stop, force=lambda: 0,
        ))
        register(OwnerAdapter(
            owner="monitor-ytdlp", label="yt-dlp update monitor",
            active=self._ytdlp_monitor_active,
            prepare=self._prepare_ytdlp_monitor_owner,
            join=self._join_ytdlp_monitor,
            force=lambda: self._terminate_process_owners("ytdlp-updater"),
        ))
        register(OwnerAdapter(
            owner="sync", label="Sync and redownload worker",
            active=lambda: self._thread_is_alive(self._sync_thread),
            prepare=self._prepare_sync_owner,
            join=lambda timeout: self._join_threads([self._sync_thread], timeout),
            force=lambda: self._terminate_process_owners("sync", "redownload"),
            details=lambda: {
                "current_task_id": str(
                    (getattr(self._queues, "current_sync", None) or {}).get(
                        "task_id") or "")
            },
        ))
        register(OwnerAdapter(
            owner="sync-metadata", label="Inline sync metadata workers",
            active=sync_backend.inline_metadata_workers_active,
            prepare=sync_backend.inline_metadata_workers_cancel,
            join=sync_backend.inline_metadata_workers_join,
            force=lambda: self._terminate_process_owners("sync"),
            details=sync_backend.inline_metadata_workers_snapshot,
        ))
        register(OwnerAdapter(
            owner="sync-maintenance", label="Deferred post-sync maintenance",
            active=sync_backend.post_sync_maintenance_active,
            prepare=sync_backend.post_sync_maintenance_cancel,
            join=sync_backend.post_sync_maintenance_join,
            force=sync_backend.post_sync_maintenance_cancel,
            details=sync_backend.post_sync_maintenance_snapshot,
        ))
        register(OwnerAdapter(
            owner="manual-download", label="Manual downloads",
            active=self.archive_single_is_running,
            prepare=lambda: bool(self.archive_single_cancel().get("ok")),
            join=lambda timeout: self._join_threads(
                self._archive_download_threads(), timeout),
            force=lambda: self._terminate_process_owners("manual-download"),
        ))
        register(OwnerAdapter(
            owner="processing", label="GPU processing queue",
            active=lambda: bool(
                self._transcribe.shutdown_snapshot().get("worker_alive")),
            prepare=self._transcribe.begin_shutdown,
            join=self._transcribe.join_shutdown,
            force=self._transcribe.force_shutdown,
            task_id=lambda: str(
                self._transcribe.shutdown_snapshot().get(
                    "current_task_id") or ""),
            details=self._transcribe.shutdown_snapshot,
        ))
        register(OwnerAdapter(
            owner="reorganization", label="Channel reorganization",
            active=lambda: bool(getattr(self, "_reorg_running", False)),
            prepare=self._prepare_reorg_owner,
            join=lambda timeout: self._join_threads(
                [getattr(self, "_reorg_thread", None)], timeout),
            force=lambda: 0,
        ))
        register(OwnerAdapter(
            owner="archive-rescan", label="Archive rescan",
            active=self.archive_rescan_is_running,
            prepare=lambda: (
                getattr(self, "_archive_rescan_cancel", None).set()
                if getattr(self, "_archive_rescan_cancel", None) is not None
                else True
            ) is not False,
            join=lambda timeout: self._join_threads(
                [getattr(self, "_archive_rescan_thread", None)], timeout),
            force=lambda: 0,
        ))
        # This final catch-all still uses exact registered process roots. It
        # covers dependency installers and any legacy caller that has not yet
        # gained a dedicated top-level adapter, without a process-name scan.
        register(OwnerAdapter(
            owner="zz-registered-processes", label="Registered child processes",
            active=self._registered_processes_active,
            prepare=lambda: True,
            join=self._join_registered_processes,
            force=self._force_registered_processes,
        ))

    def background_owner_snapshot(self):
        """Expose a serializable owner/process inventory for diagnostics."""
        return self._job_supervisor.snapshot()

    def _work_admission_error(self, operation: str):
        """Return a bridge-friendly error when shutdown/restore closed work."""
        try:
            self._job_supervisor.require_admission(operation)
            return None
        except Exception as exc:
            return {"ok": False, "started": False, "error": str(exc)}

    def set_window(self, w):
        """Attach the pywebview window and route backend log output to it."""
        self._window = w
        self._log_stream.set_window(w)
        # NOTE: the "YTArchiver <ver> started" banner is emitted at the TOP
        # of _run_startup_sequence (not here) so it always precedes the
        # "Startup checks complete" milestone. Emitting it here logged it
        # AFTER the buffered startup output flushed, so "checks complete"
        # rendered above "started" — making the checks look fake.

    def _on_queue_changed(self):
        """Push updated queue state to the UI whenever anything changes."""
        if self._window is None:
            return
        try:
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
            # Forward `*_paused_active` (default to *_paused for older
            # payloads) so the UI can blink the Resume button between
            # "user clicked pause" and "worker actually parked".
            _sync_pa = bool(payload.get('sync_paused_active',
                                        payload.get('sync_paused')))
            _gpu_pa = bool(payload.get('gpu_paused_active',
                                       payload.get('gpu_paused')))
            try:
                from backend import youtube_traffic as _yt_traffic
                _traffic_wait = _yt_traffic.wait_status()
            except Exception:
                _traffic_wait = {"active": False}
            try:
                from backend import youtube_session as _yt_session
                _session_limited = bool(_yt_session.rate_limit_detected())
            except Exception:
                _session_limited = False
            self.services.event_bus.update_queues(payload, {
                "sync": {
                    "running": sync_running,
                    "paused": bool(payload["sync_paused"]),
                    "pausedActive": _sync_pa,
                    "trafficWaiting": bool(_traffic_wait.get("active")),
                    "trafficWait": _traffic_wait,
                    # YouTube's emergency "current session has been
                    # rate-limited" hold is separate from the configured
                    # hourly/24-hour rolling-budget wait above. Forward it
                    # explicitly so Sync Tasks parks while its worker waits.
                    "sessionLimited": _session_limited,
                },
                "gpu": {
                    "running": gpu_running,
                    "paused": bool(payload["gpu_paused"]),
                    "pausedActive": _gpu_pa,
                },
            })
            # Drive tray icon spin + tooltip with current task name.
            # Uses the narrower *_working semantics so the tray shows idle
            # (not spinning) while a pass is paused between channels.
            tray = getattr(self, "_tray", None)
            if tray is not None:
                _traffic_waiting = bool(_traffic_wait.get("active"))
                _spin_color = activity_spin_color(
                    sync_working=sync_working,
                    gpu_working=gpu_working,
                    traffic_waiting=_traffic_waiting,
                )
                if _spin_color == "red":
                    job = payload['gpu'][0] if payload['gpu'] else {}
                    label = (job.get("kind") or "").title() or "Processing"
                    target = job.get("name") or job.get("title") or ""
                    tip = f"YTArchiver \u2014 {label}"
                    if target:
                        if len(target) > 35:
                            target = target[:32] + "\u2026"
                        tip += f": {target}"
                    tray.start_spin("red")
                    tray.set_tooltip(tip)
                elif _spin_color == "blue":
                    job = payload['sync'][0] if payload['sync'] else {}
                    target = job.get("name") or job.get("title") or ""
                    tip = "YTArchiver \u2014 Syncing"
                    if target:
                        if len(target) > 35:
                            target = target[:32] + "\u2026"
                        tip += f": {target}"
                    tray.start_spin("blue")
                    tray.set_tooltip(tip)
                else:
                    tray.stop_spin()
                    if getattr(tray, "error_active", False) is True:
                        tray.set_tooltip(
                            "YTArchiver — Errors need attention — open for details")
                    elif _traffic_waiting:
                        _wait_label = (
                            "24-hour" if _traffic_wait.get("reason")
                            == "daily_limit" else "hourly"
                        )
                        tray.set_tooltip(
                            "YTArchiver \u2014 Waiting for YouTube "
                            f"{_wait_label} slot")
                    else:
                        tray.set_tooltip("YTArchiver \u2014 Idle")
        except Exception as e:
            _log.debug("swallowed: %s", e)

    def attach_tray(self, tray):
        """Attach the optional TrayController after pywebview is created."""
        self._tray = tray
        try:
            tray.set_error(self._session_error_count)
        except Exception as exc:
            _log.debug("session error indicator attach failed: %s", exc)

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
                self._autorun.restore_interval_mins(
                    int(self._config.get("autorun_interval", 0) or 0))
            except Exception as e:
                _log.debug("swallowed: %s", e)
            # Apply saved log_mode to the LogStreamer so Verbose-mode users
            # don't launch into Simple and wonder where their dim/debug
            # lines went. set_log_mode() only fires when the user flips
            # the dropdown; startup needs its own apply step.
            try:
                self._log_stream.simple_mode = (
                    self._config.get("log_mode", "Simple") == "Simple")
            except Exception as e:
                _log.debug("swallowed: %s", e)
        else:
            print(f"[config] real config not found at {CONFIG_FILE}; using synthetic data")
            self._config = None

    def _run_startup_sequence(self, cancel_event=None):
        """Three-stage startup log matching YTArchiver's OLD timing:

            Stage 1 (< 2s) --- Startup checks complete, ready to download ---
                              → Sync Subbed + related buttons enable here
            Stage 2 (20-40s) --- Disk scan complete (N ch \u00b7 M vids \u00b7 X TB) ---
                              → staleness-gated: if cache is newer than
                                `disk_scan_staleness_hours`, skip the walk
                                and just report from the cache (instant)
            Stage 3 (background) --- newly-added files swept into the index

        The stages run sequentially under one supervised startup owner and
        emit each milestone as it finishes. A small joined indicator helper
        animates the "Loading\u00b7" line in verbose mode; simple mode sees
        only the three green milestones (the tick is VERBOSE_ONLY).
        """
        import time as _time
        cancel_event = cancel_event or threading.Event()
        s = self._log_stream

        # App-started banner FIRST — must precede the "Startup checks
        # complete" milestone below. (Moved here from set_window, whose log
        # fired after this buffered output flushed, so "checks complete"
        # appeared above "started" and made the checks look instantaneous.)
        try:
            s.emit_text(f"YTArchiver {APP_VERSION} started", None)
        except Exception as e:
            _log.debug("swallowed: %s", e)

        def _flush_now():
            try: s.flush()
            except Exception as e: _log.debug("swallowed: %s", e)

        def _loading(msg):
            # In-place status line (replace-in-place via `startup_loading`).
            # Filtered from simple mode — user sees only the green milestones.
            try:
                s.emit([[f" {msg}\n", "startup_loading"]])
                _flush_now()
            except Exception as e: _log.debug("swallowed: %s", e)

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
        # Startup status for low-priority background indexing.
        dots_state = {
            "i": 0,
            "sweep": {"phase": "Starting up", "detail": ""},
        }
        stage3_done = threading.Event()

        def _push_indicator(slot, text):
            """Push startup status text, or `None`/`""` to hide it.
            Visible in Simple + Verbose.

            Use json.dumps to encode the text argument. A manual replace-chain
            escaped
            `\\` and `'` only — a literal newline / carriage return
            inside a channel folder name would produce broken JS
            (unescaped newline inside a quoted string is a
            SyntaxError) which evaluate_js then silently swallowed
            into the outer except below, leaving the indicator
            stuck on its last value for the affected tick.
            """
            import json as _json
            try:
                w = self._window
                if w is None:
                    return
                if text:
                    safe = _json.dumps(text)  # returns a fully-quoted JS string literal
                    w.evaluate_js(
                        f"window._setIndicator && "
                        f"window._setIndicator({_json.dumps(slot)}, {safe})")
                else:
                    w.evaluate_js(
                        f"window._setIndicator && "
                        f"window._setIndicator({_json.dumps(slot)}, null)")
            except Exception as e:
                _log.debug("swallowed: %s", e)

        def _animate_dots():
            """Cycle dots on each active status slot. When a
            slot's `phase` is empty, its UI indicator is hidden; when
            populated, we emit `{phase}{dots} {detail}`."""
            while not stage3_done.is_set() and not cancel_event.is_set():
                dots_state["i"] = (dots_state["i"] + 1) % 3
                d = ["\u00b7 ", "\u00b7\u00b7 ", "\u00b7\u00b7\u00b7"][dots_state["i"]]
                log_parts = []
                for slot in ("sweep",):
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
                # Log mirror for the verbose startup "Loading" line.
                if log_parts:
                    _loading(" \u00b7 ".join(log_parts))
                cancel_event.wait(0.4)
            # NOTE: post-stage-3 indicator state is handled by the
            # caller after stage3_done.set(). The animator deliberately
            # doesn't touch the slot on exit so it can't race-overwrite
            # cleanup.
        animator = threading.Thread(
            target=_animate_dots, daemon=True, name="startup-indicator")
        animator.start()

        def _clear_loading():
            """Remove the in-place Loading line from the DOM."""
            try:
                w = self._window
                if w is not None:
                    w.evaluate_js("window.clearStartupLine && window.clearStartupLine()")
            except Exception as e:
                _log.debug("swallowed: %s", e)

        def _fire_ready_js():
            """Tell the UI to un-gray the Sync Subbed / Sync Tasks buttons."""
            try:
                w = self._window
                if w is not None:
                    w.evaluate_js("window._setReady && window._setReady(true)")
            except Exception as e:
                _log.debug("swallowed: %s", e)

        # ── Stage 1: Startup checks (immediate — < 2s) ─────────────────
        # No heavy I/O at this stage. Emit the green milestone right away
        # so the user sees the app responded, and flip the Sync buttons
        # active so they can kick off a sync without waiting for background
        # indexing.
        try:
            s.emit_text("--- Startup checks complete, ready to download ---",
                        "simpleline_green")
            _flush_now()
        except Exception as e: _log.debug("swallowed: %s", e)
        _fire_ready_js()
        # Paint the restored queue NOW. Items restored from last session
        # in Api.__init__ were loaded before the window existed, so their
        # listener pushes were silently dropped (self._window is None in
        # _on_queue_changed). Without this explicit repaint, restored
        # Sync/GPU tasks stayed invisible until the next incidental queue
        # mutation — after a cold reboot that could be the end of the
        # disk scan, ~45s later, which read as "my queue is gone."
        try:
            self._on_queue_changed()
        except Exception as e:
            _log.debug("swallowed: %s", e)

        # Every startup archive walk is lowest-priority work. This predicate
        # stays true for an entire sync worker/pass, allowing a scan that
        # began before the user pressed Sync/Resume to stop cooperatively
        # instead of monopolizing pooled-storage metadata I/O underneath it.
        def _startup_low_priority_busy():
            try:
                if index_backend.is_foreground_browse_busy():
                    return True
            except Exception:
                pass
            try:
                if self.sync_is_running():
                    return True
            except Exception:
                pass
            try:
                if self.archive_single_is_running():
                    return True
            except Exception:
                pass
            try:
                mgr = self._transcribe
                if mgr._current_job is not None:
                    return True
            except Exception:
                pass
            try:
                from backend.sync.active_state import is_sync_work_active
                return bool(is_sync_work_active())
            except Exception:
                return False

        # ── Stage 2: Disk walk (staleness-gated) ───────────────────────
        def _stage2_disk_walk():
            """Refresh disk-scan cache after Stage 1 makes the UI usable."""
            if cancel_event.is_set():
                return
            try:
                from backend.archive_scan import (
                    archive_totals,
                    heal_malformed_cache_entries,
                    load_disk_cache,
                    save_disk_cache,
                    scan_all_channels,
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
                    walked = scan_all_channels(
                        progress_cb=_on_walk,
                        stop_if=lambda: (
                            cancel_event.is_set()
                            or _startup_low_priority_busy()
                        ))
                    if cancel_event.is_set():
                        return
                    if walked is None:
                        dots_state["sweep"]["phase"] = ""
                        dots_state["sweep"]["detail"] = ""
                        s.emit_dim(
                            " Disk scan deferred — sync or foreground work "
                            "started; existing cache preserved.")
                        _flush_now()
                        return
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
                            _unused, c2 = update_config(
                                lambda live: live.__setitem__(
                                    "last_disk_scan_ts", _time.time()))
                            self._config = c2
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
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            except Exception as e:
                s.emit_error(f"Disk scan error: {e}")
                _flush_now()

        def _start_subscriber_backfill():
            """Recover missing card counts without delaying app readiness."""
            def _run():
                if cancel_event.is_set():
                    return
                try:
                    from backend.subscriber_counts import (
                        backfill_missing_counts,
                    )
                    current_cfg = self._config or load_config()
                    result = backfill_missing_counts(
                        list(current_cfg.get("channels", []) or []))
                    updated = int(result.get("updated") or 0)
                    failed = int(result.get("failed") or 0)
                    excluded = int(result.get("excluded") or 0)
                    deferred = int(result.get("deferred") or 0)
                    if updated and self._window is not None:
                        # refreshSubsTable fans into _primeBrowse, so visible
                        # channel cards pick up the new counts immediately.
                        self._window.evaluate_js(
                            "window.refreshSubsTable && "
                            "window.refreshSubsTable();")
                    if updated or failed or excluded or deferred:
                        summary = f" Subscriber counts: {updated} recovered"
                        if failed:
                            summary += f", {failed} still unavailable"
                        if excluded:
                            summary += f", {excluded} excluded after 3 attempts"
                        if deferred:
                            summary += f", {deferred} deferred"
                        summary += "."
                        s.emit_dim(summary)
                        _flush_now()
                except Exception as exc:
                    _log.debug("subscriber-count launch backfill failed: %s",
                               exc)

            _run()

        # Stage 3: low-priority background sweep.
        def _stage3_sweep():
            """Run the archive sweep after disk state is known."""
            if cancel_event.is_set():
                return
            output_dir = (cfg.get("output_dir") or "").strip()
            sweep_result = {"registered": 0, "ingested": 0}

            def _run_sweep():
                if not output_dir or cancel_event.is_set():
                    return
                sweep_progress = {"detail": ""}
                def _on_sweep(idx, total, name):
                    clean = (name or "")[:32]
                    sweep_progress["detail"] = f"{idx}/{total} \u2014 {clean}"
                    dots_state["sweep"]["phase"] = "Indexing new files"
                    dots_state["sweep"]["detail"] = sweep_progress["detail"]

                def _sweep_busy():
                    busy = _startup_low_priority_busy()
                    if busy:
                        # The old label said "Indexing new files" for up to an
                        # hour while the sweep was intentionally yielding to a
                        # sync/GPU/Browse task. Tell the truth about the wait.
                        dots_state["sweep"]["phase"] = (
                            "Index scan waiting for active work")
                        dots_state["sweep"]["detail"] = ""
                    elif dots_state["sweep"].get("phase") == (
                            "Index scan waiting for active work"):
                        dots_state["sweep"]["phase"] = "Indexing new files"
                        dots_state["sweep"]["detail"] = (
                            sweep_progress["detail"])
                    return busy

                if not _sweep_busy():
                    dots_state["sweep"]["phase"] = "Indexing new files"
                    dots_state["sweep"]["detail"] = ""
                try:
                    # Pass the low-priority gate to sweep so it yields
                    # between channels while sync/GPU work is active.
                    r = index_backend.sweep_new_videos(
                        output_dir, cfg.get("channels", []),
                        progress_cb=_on_sweep,
                        gpu_busy_fn=_sweep_busy,
                        extra_roots=list(cfg.get("tp_archive_roots") or []))
                    if cancel_event.is_set():
                        return
                    sweep_result["registered"] = int(r.get("registered") or 0)
                    sweep_result["ingested"] = int(r.get("ingested") or 0)
                    sweep_result["skipped_unchanged"] = int(
                        r.get("skipped_unchanged") or 0)
                    sweep_result["walked"] = int(r.get("walked") or 0)
                except Exception as _se:
                    s.emit_error(f"Sweep failed: {_se}")
                    _flush_now()
                finally:
                    # Clear the sweep slot when indexing is done.
                    dots_state["sweep"]["phase"] = ""
                    dots_state["sweep"]["detail"] = ""

            # This stage already runs on the supervised startup worker. Keep
            # the sweep inline so shutdown/restore owns the real writer rather
            # than a nested daemon that could outlive its parent.
            _run_sweep()
            if cancel_event.is_set():
                return

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
                cfg2 = self._config or load_config()
                od = (cfg2.get("output_dir") or "").strip()
                if od:
                    probe = od if os.path.isdir(od) else os.path.dirname(od) or "."
                    cap = archive_capacity_status(probe, cfg2)
                    if cap.get("status") == "warning":
                        detail = cap.get("detail") or "Archive drive is over its warning threshold"
                        s.emit([
                            ["\u26a0 ", "red"],
                            [f"Archive drive warning: {detail}. New syncs may fail.\n", "red"]])
                        _flush_now()
            except Exception as e:
                _log.debug("swallowed: %s", e)

            stage3_done.set()
            # Wait > the animator's 0.4s sleep cycle so its loop
            # definitely sees stage3_done and exits before cleanup.
            cancel_event.wait(0.5)
            _clear_loading()
            # Clear the live indexing indicator.
            try:
                _push_indicator("sweep", None)
            except Exception as e:
                _log.debug("swallowed: %s", e)

        # Sequential stages on one background thread — each milestone
        # fires the moment its stage finishes.
        def _run_stages():
            """Run slow startup stages in order on the boot worker thread."""
            try:
                if not cancel_event.is_set():
                    _stage2_disk_walk()
                if not cancel_event.is_set():
                    _stage3_sweep()
                # Start only after the local sweep finishes: it guarantees
                # normal cache rows exist and avoids racing the sweep's final
                # disk-cache merge. This remains independent background work,
                # so traffic spacing never delays app readiness.
                if not cancel_event.is_set():
                    _start_subscriber_backfill()
            finally:
                stage3_done.set()
                # Budget-based auto-sync is restored from config before the
                # window exists. Release its boot gate only after local
                # startup work is finished, then let the scheduler apply its
                # three-minute remote-sync grace period.
                if not cancel_event.is_set():
                    try:
                        self._autorun.notify_startup_ready("indexing")
                    except Exception as e:
                        _log.debug(
                            "autorun startup-ready notification failed: %s", e)
                    try:
                        self._trash_retention.notify_startup_ready("indexing")
                    except Exception as e:
                        _log.debug(
                            "Trash retention startup-ready notification "
                            "failed: %s", e)
        _run_stages()
        try:
            if animator is not threading.current_thread():
                animator.join(timeout=1.0)
        except (RuntimeError, TypeError):
            pass


def _configured_whisper_model_for_restore(valid_models):
    try:
        configured = ((load_config() or {}).get("whisper_model") or "").strip()
    except Exception as e:
        _log.debug("swallowed: %s", e)
        configured = ""
    return configured if configured in valid_models else "small"


def main():
    _boot_trace("main start")
    _start_minimized = "--start-minimized" in sys.argv

    # Put the app-managed bin dir (%APPDATA%/YTArchiver/bin) on PATH FIRST,
    # before any shutil.which() / dependency probe runs. This is where the
    # first-run onboarding installer drops yt-dlp.exe + ffmpeg/ffprobe, so
    # they resolve everywhere (sync, compress, redownload, diagnostics) with
    # no other changes. Appended (not prepended) so a user's own system
    # tools keep winning — see deps_installer.ensure_bin_on_path.
    try:
        from backend import deps_installer as _deps_boot
        _deps_boot.ensure_bin_on_path()
    except Exception as e:
        _log.debug("ensure_bin_on_path failed (non-fatal): %s", e)
    _boot_trace("bin path ready")

    # Finish or roll back interrupted multi-resource transactions before any
    # scheduler, queue owner, database connection, or config backup starts.
    # Continuing with ambiguous folder/config or restore state would let the
    # new process compound a half-committed generation, so recovery failures
    # deliberately fail closed and leave the durable journal for diagnosis.
    from backend.services.channel_transactions import (
        recover_channel_transaction,
    )
    from backend.services.restore_coordinator import (
        recover_interrupted_restore,
    )
    for _recovery_name, _recover in (
        ("application restore", recover_interrupted_restore),
        ("channel folder", recover_channel_transaction),
    ):
        _recovery = _recover()
        if not _recovery.get("ok"):
            raise RuntimeError(
                f"Interrupted {_recovery_name} requires recovery before "
                f"YTArchiver can start: {_recovery.get('error') or 'unknown error'}"
            )
        if _recovery.get("recovered"):
            _log.warning(
                "Recovered interrupted %s transaction: %s",
                _recovery_name,
                _recovery.get("action") or "completed",
            )
    _boot_trace("transaction recovery checked")

    # Start the network-down monitor in the background
    try:
        net_backend.start_monitor()
    except Exception as e:
        _log.debug("swallowed: %s", e)
    _boot_trace("net monitor started")

    # Dated config snapshot — cheap insurance against corruption
    try:
        snap = backup_config_on_start()
        if snap:
            print(f"[config] dated snapshot: {snap}")
    except Exception as e:
        _log.debug("swallowed: %s", e)
    _boot_trace("config backup checked")

    # T087: boot-failure safety net. The heavy, port-binding construction
    # below (local fileserver, cmd-server on 127.0.0.1:9855, Api(), tray)
    # all happens BEFORE webview.start(). If any of it raises, the process
    # used to exit with the ports still bound, so the NEXT launch silently
    # lost companion-tool integration (cmd_server.start_server returns False
    # on OSError) until reboot. Register an idempotent emergency teardown
    # that releases the ports and flushes queues. The normal shutdown path
    # ends in os._exit(0) (which skips atexit) AND sets shutdown_ran below,
    # so this hook only ever fires when boot blew up before reaching the
    # window — never as a double-teardown on the happy path.
    _boot_state = {
        "api": None,
        "shutdown_ran": False,
        "shutdown_report": None,
    }

    def _boot_emergency_cleanup():
        if _boot_state["shutdown_ran"]:
            return
        _boot_state["shutdown_ran"] = True
        try:
            from backend import cmd_server as _cs
            _cs.stop_server()
        except Exception as e:
            _log.debug("boot-cleanup cmd_server stop: %s", e)
        try:
            from backend import local_fileserver as _lfs
            _stop = getattr(_lfs, "stop_server", None)
            if callable(_stop):
                _stop()
        except Exception as e:
            _log.debug("boot-cleanup fileserver stop: %s", e)
        _api = _boot_state.get("api")
        if _api is not None:
            try:
                _api._queues.save_now()
            except Exception as e:
                _log.debug("boot-cleanup queue save: %s", e)

    import atexit as _atexit
    _atexit.register(_boot_emergency_cleanup)

    # Start the localhost file server BEFORE creating the Api (so any early
    # URL generation has a port to bake in). Bound to 127.0.0.1 only.
    try:
        from backend import local_fileserver as _fs
        # populate the archive-root allowlist before
        # starting the server so the _resolve_path gate is live on
        # the very first request. Roots = every channel output_dir
        # + the global output_dir + known thumbs/cache dirs.
        try:
            _cfg_boot = load_config() or {}
            _roots: list[str] = []
            _g_out = _cfg_boot.get("output_dir") or ""
            if _g_out:
                _roots.append(_g_out)
            _v_out = _cfg_boot.get("video_out_dir") or ""
            if _v_out:
                _roots.append(_v_out)
            for _ch in (_cfg_boot.get("channels") or []):
                _co = _ch.get("output_dir") or ""
                if _co:
                    _roots.append(_co)
            # Serve only explicit app asset dirs, not the whole app root.
            _roots.append(str(WEB))
            _fs.set_allowed_roots(_roots)
        except Exception as _re:
            print(f"[fileserver] could not set allowed roots: {_re}")
        _port = _fs.start_server()
        print(f"[fileserver] serving local assets on 127.0.0.1:{_port}")
    except Exception as _fe:
        print(f"[fileserver] failed to start: {_fe}")
    _boot_trace("local fileserver handled")

    _boot_trace("api construct begin")
    api = Api()
    _boot_trace("api construct done")
    # Hand the live Api to the boot safety net so an emergency teardown can
    # still flush queues if a later boot step (cmd-server, tray) raises.
    _boot_state["api"] = api

    # HTTP command server on 127.0.0.1:9855 lets compatible companion
    # viewers trigger retranscribe / ping
    # the app. Matches YTArchiver.py:34400 _start_cmd_server.
    try:
        from backend import cmd_server as _cmd
        def _cmd_log(msg): print(msg)
        def _handle_ping(_body):
            return {"alive": True}
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
            """Queue a re-transcribe request from a companion viewer.
            `_cmd_retranscribe`:
              - accepts {channel, video_id, [model]}
                actual request shape) OR {filepath, title, channel}
                (legacy/alternative)
              - validates the optional `model` against the supported set
              - disambiguates the DB lookup by channel when both
                channel + video_id are provided (two channels could
                in theory hold the same 11-char id)
              - routes through the same retranscribe path the UI uses
                (retranscribe=True), so the aggregated Transcript.txt /
                .jsonl entries get SURGICALLY SWAPPED, not duplicated.
                Without this, every companion-viewer re-transcribe would
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
                except Exception as e:
                    _log.debug("swallowed: %s", e)
            if not filepath or not os.path.isfile(filepath):
                return {"ok": False,
                        "error": f"Video not found on disk (id={video_id}, ch={channel})"}
            filepath = os.path.normpath(filepath)
            try:
                from backend.utils import is_within_managed_roots
                if not is_within_managed_roots(filepath):
                    return {"ok": False,
                            "error": "Refusing to retranscribe a file outside the archive."}
            except Exception as e:
                _log.warning("cmd retranscribe containment check failed: %s", e)
                return {"ok": False,
                        "error": "Could not verify archive containment for video path."}
            # Route through the same path the Watch-view re-transcribe
            # uses so retranscribe=True + video_id are forwarded, and the
            # completion callback push-updates any open Watch view. The model
            # is immutable job data; no global swap/restore race is needed.
            try:
                res = api.transcribe_retranscribe(
                    filepath, title, video_id, model=model)
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
        _cmd.start_server(APP_VERSION, on_log=_cmd_log)
    except Exception as _ce:
        print(f"[cmd] failed to start: {_ce}")
    _boot_trace("cmd server handled")

    # Load saved window state (position / size / maximized)
    ws = winstate.load_window_state()
    _boot_trace("window state loaded")
    kwargs = {
        "title": "YTArchiver",
        "url": str(INDEX),
        "js_api": api,
        "width": int(ws.get("width") or 1100),
        "height": int(ws.get("height") or 780),
        "min_size": (640, 480),
        "background_color": "#0f1012",
        "resizable": True,
        # Create the window already hidden when launched via the boot
        # Registry entry's `--start-minimized` flag. Doing it at create
        # time is reliable; the post-load `window.hide()` in
        # _startup_checks alone let the WebView2 window flash/stay visible
        # on boot because the window is shown before that callback runs.
        "hidden": _start_minimized,
    }
    if ws.get("x") is not None and ws.get("y") is not None:
        kwargs["x"] = int(ws["x"])
        kwargs["y"] = int(ws["y"])
    _boot_trace("create_window begin")
    window = webview.create_window(**kwargs)
    _boot_trace("create_window done")
    api.set_window(window)

    def _find_native_window_handle(timeout: float = 2.0) -> int:
        """Return the pywebview HWND once Windows has actually created it."""
        if os.name != "nt":
            return 0
        try:
            import ctypes as _ct
            from ctypes import wintypes as _wintypes
            find_window = _ct.windll.user32.FindWindowW
            find_window.argtypes = [
                _wintypes.LPCWSTR, _wintypes.LPCWSTR]
            find_window.restype = _wintypes.HWND
            deadline = time.monotonic() + max(0.0, float(timeout))
            while True:
                hwnd = find_window(None, kwargs["title"])
                if hwnd:
                    return int(hwnd)
                if time.monotonic() >= deadline:
                    return 0
                time.sleep(0.05)
        except Exception as exc:
            _log.debug("native window handle lookup failed: %s", exc)
            return 0

    # Dark title bar via DWM (Windows 10 19041+ / Windows 11). Matches
    # OLD YTArchiver's `_apply_dark_title_bar` so the window chrome doesn't
    # flash white against the dark body during repaint. Best-effort — any
    # failure is silent.
    def _apply_dark_titlebar_when_ready():
        try:
            import ctypes as _ct
            hwnd = _find_native_window_handle()
            if not hwnd:
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
                except Exception as e:
                    _log.debug("swallowed: %s", e)
        except Exception as e:
            _log.debug("swallowed: %s", e)
    def _start_dark_titlebar_thread():
        try:
            threading.Thread(
                target=_apply_dark_titlebar_when_ready,
                daemon=True,
                name="dark-titlebar",
            ).start()
            _boot_trace("dark titlebar thread started")
        except Exception as e:
            _log.debug("dark titlebar thread failed: %s", e)
    _boot_trace("dark titlebar deferred")

    # Debounced window-state save. pywebview emits resize/move at ~60Hz
    # during a drag — without debounce that's 60 config-file writes per
    # second, each loading/mutating/saving the same JSON file (audit:
    # main.py:1057-1076 + window_state.py:146-163). We coalesce all
    # in-flight {width,height,x,y,maximized} updates into one save 250ms
    # after the last event arrives.
    _ws_lock = threading.Lock()
    _ws_pending: dict = {}
    _ws_timer: list = [None]  # holds the current Timer, if any
    _state_writes_enabled = {"value": True}

    def _stop_window_state_writes() -> None:
        """Cancel stale config timers before a restore swaps live state."""
        with _ws_lock:
            _state_writes_enabled["value"] = False
            _ws_pending.clear()
            timer = _ws_timer[0]
            if timer is not None:
                try: timer.cancel()
                except Exception: pass

    def _ws_flush(expected_timer=None):
        try:
            with _ws_lock:
                # A cancelled older timer may already be inside its callback
                # while a newer debounce generation is scheduled. It must not
                # consume the newer generation's pending geometry.
                if (expected_timer is not None
                        and _ws_timer[0] is not expected_timer):
                    return
                snap = _ws_pending.copy()
                _ws_pending.clear()
                _ws_timer[0] = None
                enabled = _state_writes_enabled["value"]
            if snap and enabled:
                winstate.save_window_state(snap)
        except Exception as e:
            _log.debug("swallowed: %s", e)
    def _ws_schedule(updates: dict):
        with _ws_lock:
            if not _state_writes_enabled["value"]:
                return
            _ws_pending.update(updates)
            t = _ws_timer[0]
            if t is not None:
                try: t.cancel()
                except Exception: pass
            timer = None

            def _flush_this_generation():
                _ws_flush(timer)

            timer = threading.Timer(0.25, _flush_this_generation)
            timer.daemon = True
            _ws_timer[0] = timer
            timer.start()

    def _ws_owner_active() -> bool:
        with _ws_lock:
            timer = _ws_timer[0]
        return bool(timer is not None and timer.is_alive())

    def _ws_owner_join(timeout: float) -> bool:
        with _ws_lock:
            timer = _ws_timer[0]
        if timer is not None and timer is not threading.current_thread():
            try:
                timer.join(timeout=max(0.0, float(timeout)))
            except (RuntimeError, TypeError):
                pass
        return not _ws_owner_active()

    # The debouncer is intentionally a Timer because pywebview can emit
    # dozens of resize/move events per second.  It is nevertheless a named,
    # joinable lifecycle owner: restore/shutdown cancels it before the config
    # swap and the supervisor proves that no delayed writer remains.
    try:
        api._job_supervisor.register_owner(OwnerAdapter(
            owner="window-state",
            label="Window-state debouncer",
            active=_ws_owner_active,
            prepare=_stop_window_state_writes,
            join=_ws_owner_join,
            force=_stop_window_state_writes,
        ))
    except ValueError:
        pass
    # Register window-event handlers to save state on resize/move/close.
    def _on_resized(w, h):
        """Persist the latest pywebview window size after resize events."""
        _ws_schedule({"width": int(w), "height": int(h)})
    def _on_moved(x, y):
        """Persist the latest pywebview window position after move events."""
        _ws_schedule({"x": int(x), "y": int(y)})
    def _on_maximized():
        """Persist that the window entered maximized state."""
        _ws_schedule({"maximized": True})
    def _on_restored():
        """Persist that the window left maximized state."""
        _ws_schedule({"maximized": False})
    # X-button behavior is config-driven (.txt close-to-tray request):
    #   "quit"  — shut down immediately (legacy behavior)
    #   "tray"  — minimize to tray, keep app running
    #   "ask"   — prompt with a Quit / Close-to-tray dialog (default)
    # When in "ask" mode the closing event is blocked; the modal in the
    # frontend then calls api.confirm_close(choice, remember) which
    # either hides the window or destroys it (re-entering this handler
    # with _truly_quit flag set so it actually exits).
    _truly_quit = {"flag": False}
    # Expose to api so the JS-side modal can drive the decision back.
    api._close_state = _truly_quit
    def _on_closing():
        """Apply close behavior: quit, hide to tray, or ask via JS modal."""
        # _truly_quit flag set by the tray "Quit" menu or by the in-app
        # confirm dialog — let it through unconditionally.
        if _truly_quit.get("flag"):
            try:
                if not getattr(api, "_restore_quiesced", False):
                    winstate.save_window_state({})
                _shutdown_cleanup()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            return True
        try:
            cfg = api._config or load_config()
        except Exception:
            cfg = {}
        behavior = (cfg.get("close_behavior") or "ask").lower()
        if behavior == "quit":
            try:
                if not getattr(api, "_restore_quiesced", False):
                    winstate.save_window_state({})
                _truly_quit["flag"] = True
                _shutdown_cleanup()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            return True
        if behavior == "tray":
            try: window.hide()
            except Exception as e: _log.debug("swallowed: %s", e)
            return False
        # behavior == "ask" — show the modal and block the close
        # while we wait for the user to pick. Dispatch evaluate_js
        # from a background thread so it can never block the GUI
        # thread that's currently servicing this closing event.
        # Reentrant-X-click guard: rapid X clicks would otherwise
        # spawn a new modal-showing thread per click and wedge the
        # JS-side modal (audit: main.py H24). Flag clears in
        # confirm_close (window_mixin).
        if getattr(api, "_close_dialog_pending", False):
            return False
        try:
            api._close_dialog_pending = True
        except Exception:
            pass
        def _show_modal():
            try:
                _shown = window.evaluate_js(
                    "(function(){ if (window._showCloseDialog) {"
                    " window._showCloseDialog(); return true; }"
                    " return false; })()")
                if not _shown:
                    # appDialogs.js not loaded yet (X-click during the
                    # boot window) — no modal will ever appear, so
                    # release the reentrant guard or the X button
                    # stays dead for the whole session.
                    try: api._close_dialog_pending = False
                    except Exception: pass
            except Exception as e:
                _log.debug("swallowed: %s", e)
                try: api._close_dialog_pending = False
                except Exception: pass
        try:
            threading.Thread(target=_show_modal, daemon=True).start()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return False

    # Pre-declare tray = None so the closure inside _shutdown_cleanup
    # has a defined name to read even if the user closes the window
    # before TrayController is instantiated (~150 lines below). Without
    # this, an X-click during Stage 2/3 startup raised NameError inside
    # the closing handler and skipped subprocess kill + queue persist
    # + port release.
    tray = None

    def _shutdown_cleanup():
        """Checkpoint owners, stop exact children, then persist final state."""
        # confirm_close performs cleanup before destroy(), and destroy then
        # re-enters _on_closing. Without this guard the complete multi-second
        # teardown ran twice in sequence. Mark it at entry so every real-close
        # route shares one durable cleanup pass.
        if _boot_state["shutdown_ran"]:
            return _boot_state.get("shutdown_report") or {
                "ok": False,
                "error": "Shutdown cleanup did not publish a result",
            }
        # Neutralize the T087 boot safety net — the full teardown is running
        # now, so the atexit fallback must not also fire (it would be a no-op
        # under os._exit anyway, but make the intent explicit and defensive).
        _boot_state["shutdown_ran"] = True

        def _flush_queues_now() -> None:
            try:
                api._queues.save_now()
            except Exception as e:
                _log.debug("swallowed: %s", e)

        owners_stopped = False
        report = None
        cleanup_error = ""
        try:
            # One coordinator owns the order: close admission, stop schedulers,
            # checkpoint/cancel jobs, share one deadline, then force only exact
            # registered owners that remain. No process-name scan is involved.
            _stop_window_state_writes()
            report = api._job_supervisor.quiesce(
                reason="application shutdown", timeout=10.0)
            owners_stopped = bool(report.get("ok"))
            if not owners_stopped:
                _log.warning(
                    "shutdown coordinator finished with active owners: %s",
                    ", ".join(report.get("remaining") or []) or
                    report.get("error") or "unknown")

            restored = bool(getattr(api, "_restore_state_committed", False))
            frozen = bool(getattr(api, "_restore_quiesced", False))
            # The authoritative final save occurs only after every queue owner
            # has stopped changing state. A successful restore deliberately
            # skips all stale in-memory writes.
            if owners_stopped and not restored and not frozen:
                _flush_queues_now()
                try: winstate.save_window_state({})
                except Exception as e: _log.debug("swallowed: %s", e)
            elif not restored and not frozen:
                _log.warning(
                    "final queue/window save skipped because a state owner "
                    "did not stop before the shutdown deadline")
            try: net_backend.stop_monitor(timeout=1.0)
            except Exception as e: _log.debug("swallowed: %s", e)
            # Stop the tray thread — otherwise pystray keeps the process
            # alive after the window destroys. This was the primary ghost
            # cause prior to 2026-04-18.
            try:
                if tray and getattr(tray, "_started", False):
                    tray.stop()
            except Exception as e: _log.debug("swallowed: %s", e)
            # stop the HTTP servers so port 9855 (cmd)
            # and the local fileserver port are released cleanly. If
            # webview.start() ever returns through a path that doesn't
            # immediately os._exit, these kept the process alive AND
            # held the ports until reboot — re-launch then failed to
            # bind 9855.
            try:
                from backend import cmd_server as _cs
                _cs.stop_server()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            try:
                from backend import local_fileserver as _lfs
                _stop = getattr(_lfs, "stop_server", None)
                if callable(_stop):
                    _stop()
            except Exception as e:
                _log.debug("swallowed: %s", e)
            if owners_stopped and not restored and not frozen:
                # These live files may have been replaced by backup restore.
                # Never let the old process clear/checkpoint over that state.
                try: sync_backend.clear_sync_progress()
                except Exception as e: _log.debug("swallowed: %s", e)
                try:
                    from backend import index as _idx
                    _idx._shutdown_index()
                except Exception as e: _log.debug("swallowed: %s", e)
        except Exception as e:
            cleanup_error = str(e)
            _log.debug("swallowed: %s", e)
        result = {
            "ok": bool(owners_stopped and not cleanup_error),
            "owners_stopped": bool(owners_stopped),
            "remaining": list((report or {}).get("remaining") or []),
            "error": cleanup_error or str((report or {}).get("error") or ""),
        }
        _boot_state["shutdown_report"] = result
        return result

    def _prepare_restore_commit():
        """Make every old in-memory state owner incapable of writing again."""
        if getattr(api, "_restore_quiesced", False):
            return {
                "ok": False,
                "needs_restart": True,
                "error": "This process is already frozen for restore; restart it.",
            }
        # Stop new debounce timers before closing general work admission. A
        # timer already inside save_config is serialized by the config write
        # gate; suspend_config_writes below waits for it before restore swaps.
        _stop_window_state_writes()
        report = api._job_supervisor.quiesce(
            reason="backup restore", timeout=12.0)
        if not report.get("ok"):
            report["needs_restart"] = True
            report["error"] = (
                report.get("error") or
                "Background work could not be stopped safely. Restart the app."
            )
            return report
        if not api._queues.save_now():
            return {
                "ok": False,
                "needs_restart": True,
                "error": "The current queue could not be checkpointed safely.",
            }

        # From this point forward the process is intentionally one-way: it may
        # finish the restore or quit, but it can never write stale state again.
        try:
            from backend.ytarchiver_config import suspend_config_writes
            suspend_config_writes("backup restore")
        except Exception as exc:
            return {"ok": False, "needs_restart": True, "error": str(exc)}
        try:
            api._queues.mark_orphan()
        except Exception as exc:
            api._restore_quiesced = True
            return {
                "ok": False,
                "needs_restart": True,
                "error": f"The old queue owner could not be frozen: {exc}",
            }
        api._restore_quiesced = True
        try:
            from backend import index as _idx
            _idx._shutdown_index()
        except Exception as exc:
            return {
                "ok": False,
                "needs_restart": True,
                "error": f"The search database could not be closed: {exc}",
            }
        return {
            "ok": True,
            "owners": report.get("after", {}).get("owners", []),
        }

    # Bind the cleanup function to the Api instance so Api.app_restart
    # can invoke the full close sequence (queue persist, subprocess
    # kills, port release). Before this wiring, app_restart referenced
    # `_shutdown_cleanup` from the wrong scope; the resulting NameError
    # was silently swallowed and the restart skipped every cleanup
    # step. See backend audit #1 (2026-05-13).
    try:
        api._shutdown_cleanup_fn = _shutdown_cleanup
        api._prepare_restore_commit_fn = _prepare_restore_commit
    except Exception as e:
        _log.debug("swallowed: %s", e)
    _boot_trace("shutdown cleanup wired")

    _boot_trace("window events bind begin")
    try:
        window.events.resized += _on_resized
        window.events.moved += _on_moved
        window.events.maximized += _on_maximized
        window.events.restored += _on_restored
        window.events.closing += _on_closing
    except Exception:
        # pywebview may not expose all of these on every platform/version
        pass
    _boot_trace("window events bind done")

    # Start tray icon (optional — silently no-ops if pystray is missing)
    def _tray_show():
        """Restore and foreground the pywebview window from the tray menu."""
        try:
            window.show()
            window.restore()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        # show()+restore() are no-ops when the window is
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
            except Exception as e:
                _log.debug("swallowed: %s", e)
    def _tray_hide():
        """Hide the pywebview window from the tray menu."""
        try:
            window.hide()
        except Exception as e:
            _log.debug("swallowed: %s", e)
    def _tray_sync():
        """Start a full sync pass from the tray menu."""
        try:
            api.sync_start_all()
        except Exception as e:
            _log.debug("swallowed: %s", e)
    def _tray_quit():
        """Request a real app shutdown from the tray menu."""
        # Use the same path as the in-app Quit button. It returns to pystray
        # immediately, hides the window first, then performs queue/index/
        # subprocess cleanup in a background thread. The previous path began
        # by destroying WebView2, leaving a visible, hazed "Not responding"
        # window while its closing event did synchronous cleanup.
        try:
            result = api.confirm_close("quit", False)
            if not result or not result.get("ok"):
                raise RuntimeError((result or {}).get("error") or
                                   "quit request was rejected")
        except Exception as e:
            _log.warning("tray quit dispatch failed: %s", e)
            # Last-resort exit is delayed briefly so this callback returns and
            # any already-scheduled queue save gets a chance to finish.
            def _fallback_exit():
                try: time.sleep(0.25)
                except Exception: pass
                os._exit(0)
            threading.Thread(target=_fallback_exit, daemon=True,
                             name="tray-quit-fallback").start()
    _boot_trace("tray callbacks defined")

    # Tray is optional (pystray import or icon load can fail); wrap
    # construction so a tray failure doesn't kill the whole app launch.
    # All later tray.* references are guarded by `if tray is not None`
    # (audit: main.py H11).
    tray = None
    _boot_trace("tray construct begin")
    try:
        tray = TrayController(on_show=_tray_show, on_hide=_tray_hide,
                              on_sync=_tray_sync, on_quit=_tray_quit,
                              tooltip="YTArchiver \u2014 Idle")
    except Exception as _trayE:
        _log.debug("tray construction failed (continuing window-only): %s", _trayE)
    _boot_trace("tray constructed")
    # Always-on-top toggle (restore saved pref, defaults to off)
    _on_top_state = {"on": bool(ws.get("always_on_top", False))}
    def _toggle_on_top():
        _on_top_state["on"] = not _on_top_state["on"]
        try:
            window.on_top = _on_top_state["on"]
        except Exception as e:
            _log.debug("swallowed: %s", e)
        if tray is not None:
            tray._always_on_top = _on_top_state["on"]
        try:
            winstate.save_window_state({"always_on_top": _on_top_state["on"]})
        except Exception as e:
            _log.debug("swallowed: %s", e)
    if tray is not None:
        tray.set_on_top_toggle(_toggle_on_top, initial=_on_top_state["on"])
    # Auto-Sync submenu — radio interval items ("Off", "30 min", ... "24 hr").
    # Matches YTArchiver.py:3671 pystray.MenuItem("Auto-Sync", ...).
    if tray is not None:
        try:
            from backend.autorun import AUTORUN_LABELS as _AR_LABELS
            def _tray_get_autorun_label():
                try: return api.autorun_state().get("label", "Off")
                except Exception: return "Off"
            def _tray_set_autorun_label(lbl):
                try: api.autorun_set(lbl)
                except Exception as e: _log.debug("swallowed: %s", e)
            tray.set_autorun_menu(_AR_LABELS, _tray_get_autorun_label, _tray_set_autorun_label)
        except Exception as e:
            _log.debug("swallowed: %s", e)
    # Apply the saved always-on-top state to the window itself
    if _on_top_state["on"]:
        try: window.on_top = True
        except Exception as e: _log.debug("swallowed: %s", e)
    if tray is not None:
        try:
            tray.start()
            api.attach_tray(tray)
        except Exception as e:
            _log.debug("tray start failed (continuing window-only): %s", e)
    _boot_trace("tray started")

    # Startup sanity checks — dependency probe, missing folder scan, update
    # ping, leftover temp/partial file sweep. All run in background threads
    # so they never block window.show.
    def _startup_checks_impl(startup_cancel):
        _boot_trace("startup callback begin")
        try: api._log_stream.mark_ready()
        except Exception as e: _log.debug("log stream ready failed: %s", e)
        # create_window() only records pywebview's window description; the
        # real Win32 HWND appears after webview.start().  Attach it here so
        # ITaskbarList3 has a valid target.  TrayController also handles this
        # arriving after a restored queue has already started its spinner.
        if tray is not None and os.name == "nt":
            try:
                _tray_hwnd = _find_native_window_handle()
                if _tray_hwnd:
                    tray.set_window_handle(_tray_hwnd)
                else:
                    _log.debug("taskbar overlay HWND was not available")
            except Exception as e:
                _log.debug("taskbar overlay HWND setup failed: %s", e)
        _start_dark_titlebar_thread()
        try: api.check_dependencies()
        except Exception as e: _log.debug("swallowed: %s", e)
        if startup_cancel.is_set():
            return
        # Start one persistent monitor: it checks immediately when overdue,
        # then keeps honoring the configured elapsed-day interval for apps
        # that remain open for days or weeks.
        try: api.start_ytdlp_update_monitor()
        except Exception as e: _log.debug("swallowed: %s", e)
        # Do not touch YouTube merely because the app opened.  Session health
        # is checked lazily by operations that actually need the network, so
        # Browse remains a local-only archive viewer.
        try: api.check_channel_folders()
        except Exception as e: _log.debug("swallowed: %s", e)
        if startup_cancel.is_set():
            return
        try: api.check_app_update()
        except Exception as e: _log.debug("swallowed: %s", e)
        if startup_cancel.is_set():
            return
        try:
            from backend.temp_cleanup import startup_cleanup_temps
            def _startup_cleanup_busy():
                try:
                    if api.sync_is_running() or api.archive_single_is_running():
                        return True
                except Exception:
                    pass
                try:
                    from backend.sync.active_state import is_sync_work_active
                    return bool(is_sync_work_active())
                except Exception:
                    return False
            startup_cleanup_temps(
                api._log_stream, busy_fn=_startup_cleanup_busy)
        except Exception as e: _log.debug("swallowed: %s", e)
        if startup_cancel.is_set():
            return
        # Legacy upload-timestamp fallback — needed for the Graph tab's Week
        # bucket. Populates only NULL rows from file mtime. New downloads and
        # metadata refreshes use yt-dlp's authoritative upload_date instead;
        # `--mtime` is an HTTP Last-Modified value and can predate publication.
        # Runs once per launch; idempotent (only fills NULL rows).
        # The pywebview startup callback already runs off the UI thread.
        try:
            from backend.index import backfill_upload_ts as _backfill
            _backfill()
        except Exception as e: _log.debug("swallowed: %s", e)
        if startup_cancel.is_set():
            return
        # Seed the catalog's real completed-download timestamp from the legacy
        # recent_downloads history. `added_ts` is only first discovery time;
        # keeping the concepts separate prevents rescans of old archives from
        # corrupting Browse > Recently Downloaded ordering.
        try:
            from backend.index import backfill_downloaded_ts_from_recent as _backfill_downloads
            from backend.ytarchiver_config import load_config as _load_dl_cfg
            _dl_result = _backfill_downloads(
                _load_dl_cfg().get("recent_downloads", []))
            if _dl_result.get("updated"):
                _log.info("download timestamp backfill: %s", _dl_result)
        except Exception as e:
            _log.debug("download timestamp backfill failed: %s", e)
        if startup_cancel.is_set():
            return
        # View/like backfill for the global Videos view — materializes
        # view_count/like_count from the per-channel Metadata.jsonl sidecars
        # into the index DB so the whole archive can be sorted by views/
        # likes off an indexed column. Idempotent no-op once populated; it
        # stays under the startup-check owner so restore can wait for it.
        try:
            from backend.index import backfill_video_stats_if_needed as _bvs
            _bvs()
        except Exception as e: _log.debug("swallowed: %s", e)
        if startup_cancel.is_set():
            return
        # Duration backfill for the Index panel's "Hours of video" stat —
        # fills videos.duration_s from each video's longest segment for
        # older imports that lack it. Makes that panel's hours read an
        # instant SUM instead of a multi-minute per-segment GROUP BY that
        # used to hang it. One-time and idempotent.
        try:
            from backend.index import backfill_video_durations_if_needed as _bvd
            _bvd()
        except Exception as e: _log.debug("swallowed: %s", e)
        if startup_cancel.is_set():
            return
        # One-time video_id recovery for legacy NULL-id imports (channels
        # whose only transcript is an aggregated `.{Name} Transcript.jsonl`,
        # so the sweep's .info.json id-backfill couldn't recover them). Fills
        # video_id by matching (channel, title) to the ingested segments,
        # then flips now-linked pending videos to transcribed. Gated by a
        # config flag so the one-time GROUP-BY scan doesn't run every boot;
        # new downloads always capture their id, so once is enough.
        try:
            from backend.ytarchiver_config import load_config as _lc_vid
            if not _lc_vid().get("video_id_seg_backfill_done"):
                def _vid_seg_backfill():
                    try:
                        from backend.index import backfill_video_ids_from_segments as _bvi
                        r = _bvi()
                        if r.get("ok"):
                            from backend.ytarchiver_config import config_transaction as _ctx
                            with _ctx() as _cfg:
                                _cfg["video_id_seg_backfill_done"] = True
                            _log.debug(
                                "video_id seg-backfill: filled %s, "
                                "reconciled %s",
                                r.get("filled"), r.get("reconciled"))
                    except Exception as _e:
                        _log.debug("video_id seg-backfill thread: %s", _e)
                _vid_seg_backfill()
        except Exception as e: _log.debug("swallowed: %s", e)
        # If launched with --start-minimized (e.g. from Windows boot Registry
        # entry), hide the window immediately so the app is tray-only on boot.
        if _start_minimized:
            try:
                window.hide()
            except Exception as e:
                _log.debug("start-minimized hide failed: %s", e)
        if not startup_cancel.is_set():
            try:
                api._autorun.notify_startup_ready("checks")
            except Exception as e:
                _log.debug(
                    "autorun startup-check notification failed: %s", e)
            try:
                api._trash_retention.notify_startup_ready("checks")
            except Exception as e:
                _log.debug(
                    "Trash retention startup-check notification failed: %s", e)
        _boot_trace("startup callback end")

    def _startup_checks():
        """Run pywebview's startup callback as one quiesce-visible owner."""
        import uuid as _uuid

        cancel = threading.Event()
        task_id = f"startup-checks-{_uuid.uuid4().hex}"
        api._startup_checks_cancel = cancel
        try:
            with api._job_supervisor.operation_scope(
                owner="startup-checks",
                label="Startup dependency and catalog checks",
                task_id=task_id,
                cancel=cancel,
            ) as admitted_cancel:
                _startup_checks_impl(admitted_cancel)
        except Exception as exc:
            # Admission normally closes only during Quit/Restart/Restore. At
            # that point the callback must retire silently instead of starting
            # a late index/config writer against the next state generation.
            _log.debug("startup checks not run: %s", exc)
        finally:
            if getattr(api, "_startup_checks_cancel", None) is cancel:
                api._startup_checks_cancel = None
    # Defer startup checks until pywebview has actually rendered the
    # window. Previously this thread started BEFORE webview.start(), so
    # api.check_dependencies / check_channel_folders / check_app_update
    # would call evaluate_js while window.html was still loading — the
    # warnings either silently dropped or landed in a queue the
    # frontend never drained. webview.start's `func=` argument runs the
    # callable on its own thread AFTER the DOM is ready.
    _boot_trace("webview.start begin")
    webview.start(func=_startup_checks, debug=False)
    _boot_trace("webview.start returned")
    # After webview returns, stop the tray icon so pystray's mainloop exits
    try:
        tray.stop()
    except Exception as e:
        _log.debug("swallowed: %s", e)
    # Hard-exit to reap any lingering non-daemon threads (log stream writer,
    # autorun scheduler, etc.). Without this, pywebview + pystray together
    # can pin the process alive even after the window destroys. os._exit
    # skips atexit handlers — but all persistent state was already flushed
    # by _shutdown_cleanup above.
    os._exit(0)


if __name__ == "__main__":
    main()
