import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox
import threading
import subprocess
import json
import os
import re
import time
import copy
import shutil
import sys
import ctypes
import signal
import collections
from datetime import datetime
import unicodedata
import difflib
import hashlib
import urllib.request

# System tray icon (optional — gracefully disabled if not installed)
try:
    import pystray
    from PIL import Image, ImageDraw, ImageFont
    HAS_TRAY = True
except ImportError:
    HAS_TRAY = False

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
    RESOURCE_PATH = sys._MEIPASS
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    RESOURCE_PATH = BASE_DIR

try:
    myappid = 'ytarchiver.app.v1'
    ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
except Exception:
    pass

if os.name == 'nt':
    APP_DATA_DIR = os.path.join(os.environ.get('APPDATA', BASE_DIR), "YTArchiver")
else:
    APP_DATA_DIR = os.path.join(os.path.expanduser("~"), ".config", "YTArchiver")

os.makedirs(APP_DATA_DIR, exist_ok=True)

CONFIG_FILE = os.path.join(APP_DATA_DIR, "ytarchiver_config.json")
ARCHIVE_FILE = os.path.join(APP_DATA_DIR, "ytarchiver_archive.txt")
QUEUE_FILE = os.path.join(APP_DATA_DIR, "ytarchiver_queue.json")

startupinfo = None
if os.name == 'nt':
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0

os.environ["PATH"] = RESOURCE_PATH + os.pathsep + BASE_DIR + os.pathsep + os.environ.get("PATH", "")

DEFAULT_CONFIG = {
    "output_dir": os.path.join(BASE_DIR, "Channel Archives"),
    "video_out_dir": os.path.join(BASE_DIR, "Video Downloads"),
    "vid_date_file": True,
    "vid_add_date": False,
    "min_duration": 0,
    "channels": [],
    "recent_downloads": [],
    "autorun_interval": 0,
    "autorun_history": [],
    "log_mode": "Simple",
    "autorun_gpu": False
}

GPU_BATCH_LIMIT = 5  # max unprocessed encode batches per channel before sync skips it

RECENT_MAX = 1000
CHANNEL_DEFAULTS = {"resolution": "720", "mode": "full", "min_duration": 0, "max_duration": 0,
                    "split_years": False, "split_months": False, "auto_transcribe": False,
                    "compress_enabled": False, "compress_level": "", "compress_output_res": ""}

# Compression presets: resolution → quality tier → MB per hour of video
_COMPRESS_PRESETS = {
    "1080": {"Generous": 1200, "Average": 700,  "Below Average": 300},
    "720":  {"Generous": 800,  "Average": 475,  "Below Average": 200},
    "480":  {"Generous": 500,  "Average": 300,  "Below Average": 130},
    "360":  {"Generous": 375,  "Average": 225,  "Below Average": 100},
    "240":  {"Generous": 250,  "Average": 150,  "Below Average": 65},
    "144":  {"Generous": 150,  "Average": 90,   "Below Average": 40},
}
_QUALITY_OPTIONS = ["Generous", "Average", "Below Average"]
_LEVEL_MIGRATION = {"Low": "Generous", "High": "Average", "Extreme": "Below Average"}

def _get_compress_bitrate(quality, output_res):
    """Get MB/hr for a quality level at a given output resolution."""
    res_key = output_res if output_res in _COMPRESS_PRESETS else "1080"
    return _COMPRESS_PRESETS.get(res_key, _COMPRESS_PRESETS["1080"]).get(quality, 700)

RESOLUTION_OPTIONS = ["144", "240", "360", "480", "720", "1080", "1440", "2160", "best"]
active_processes = []
proc_lock = threading.Lock()
config_lock = threading.RLock()
io_lock = threading.Lock()

session_totals = {"dl": 0, "skip": 0, "err": 0, "dur": 0}
new_download_count = 0
cancel_event = threading.Event()
pause_event = threading.Event()
_autorun_active = False
_sync_running = False
_last_sync_job = {"id": None}
_sync_queue = []  # list of channel dicts queued for sync
_sync_queue_lock = threading.Lock()
_video_dl_queue = []  # list of (cmd, is_single_video) tuples queued during sync
_video_dl_queue_lock = threading.Lock()
_reorg_running = False
_tray_icon = None  # pystray.Icon instance
_tray_base_img = None  # PIL Image of the base icon
_tray_spin_frames = []  # pre-generated spinning animation frames (blue/default)
_tray_spin_frames_red = []  # pre-generated spinning animation frames (red/transcription)
_tray_spin_active = False
_tray_spin_idx = 0
_reorg_queue = []  # list of (channel_name, folder_path, target_years, target_months, ch_url, recheck_dates) tuples
_reorg_queue_lock = threading.Lock()
_current_job = {"label": None, "url": None}  # currently processing job label + channel URL for queue display
_current_sync_ch = None  # full channel dict of the currently syncing channel (for persistence on close)
_last_run_counts = {"dl": 0, "skip": 0, "dur": 0, "err": 0}  # per-run counts from last internal_run_cmd_blocking
_queue_items_removed = False  # tracks if user removed items from sync queue during a manual sync
_transcribe_running = False
_transcribe_sync_controlled = False  # True when current transcription responds to pause_event/cancel_event (not GPU-driven)
_transcribe_queue = []  # list of (ch_name, ch_url, folder, split_years, split_months, combined) tuples
_transcribe_queue_lock = threading.Lock()
_mt_queue = []  # list of file paths (str) for manual transcription
_mt_queue_lock = threading.Lock()

# GPU Tasks queue — independent from the main job queue
_gpu_queue = []          # list of dicts: {"type": "mt"|"transcribe"|"encode", ...details}
_gpu_queue_lock = threading.Lock()
_gpu_running = False
_gpu_cancel = threading.Event()
_gpu_pause = threading.Event()
_gpu_popup = {"win": None}
_gpu_actively_encoding = False  # True while ffmpeg/whisper is processing a file (for blink vs solid)
_gpu_truly_paused = False      # True only while the GPU worker is in its pause-wait loop (encode/transcription completed, waiting for resume)
_gpu_current = {"label": None, "ch_url": None}  # currently processing GPU task
_gpu_current_item = None  # full dict of the item currently being processed (for persistence)
_whisper_model = None  # unused legacy, kept for compat
_whisper_model_lock = threading.Lock()  # unused legacy, kept for compat
_punct_proc = None  # persistent punctuation subprocess (model stays loaded on GPU)
_job_generation = 0  # incremented each time a new sync/job starts; stale workers check before cleanup
_skip_current = threading.Event()  # set when user wants to skip the current job and move to next in queue
_skip_current_gpu = threading.Event()  # same, for GPU Tasks

# Unified queue ordering — tracks insertion order across all queue types.
# Each entry is ("sync"|"reorg"|"transcribe"|"mt"|"video", url_or_key).
# _get_queue_items() and _process_next_queued() use this for display and execution order.
_queue_order = []
_queue_order_lock = threading.Lock()


class _ToolTip:
    """Simple tooltip that appears on hover after a short delay."""
    def __init__(self, widget, text, delay=400):
        self.widget = widget
        self.text = text
        self.delay = delay
        self._tip = None
        self._job = None
        widget.bind("<Enter>", self._schedule, add="+")
        widget.bind("<Leave>", self._cancel, add="+")
        widget.bind("<ButtonPress>", self._cancel, add="+")
        widget.bind("<Unmap>", self._cancel, add="+")

    def _schedule(self, event=None):
        self._cancel()
        self._job = self.widget.after(self.delay, self._show)

    def _cancel(self, event=None):
        if self._job:
            self.widget.after_cancel(self._job)
            self._job = None
        if self._tip:
            self._tip.destroy()
            self._tip = None

    def _show(self):
        if self._tip:
            return
        x = self.widget.winfo_rootx() + self.widget.winfo_width() // 2
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 4
        self._tip = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        tw.attributes("-topmost", True)
        lbl = tk.Label(tw, text=self.text, bg="#333333", fg="#dddddd",
                       font=("Segoe UI", 8), relief="solid", bd=1,
                       padx=6, pady=3, wraplength=260, justify="left")
        lbl.pack()


# Thread-safe log queue: worker threads append callbacks here instead of
# calling root.after() directly (which hits the Tcl interpreter lock and
# can deadlock with the main thread's event processing on Windows).
_ui_queue = collections.deque()

# Plain-Python flag: True while root window is alive.  Worker threads
# check this instead of root.winfo_exists() (which is a Tcl call that
# acquires the interpreter lock and can cause crashes when racing with
# the main thread).  Set to False in on_closing() before root.destroy().
_root_alive = True

# Thread-safe cache of log_mode_var.get() == "Simple".
# Worker threads read this instead of calling log_mode_var.get(), which
# would acquire the Tcl interpreter lock on every call (hundreds/sec
# during downloads).  Updated on the main thread via trace callback.
_is_simple_mode = False

# Thread-safe cache of the autorun interval label (e.g. "Off", "30 min").
# Read by pystray menu callbacks instead of autorun_interval_var.get().
_cached_autorun_label = "Off"

# Persistent auto-scroll flag — survives yview fluctuations during batch UI queue processing
_log_at_bottom = True
_log_user_scrolled = False  # set True by mouse/scrollbar interaction, cleared when user reaches bottom

# Whisper progress dot animation state
_whisper_dots = {"base_before": "", "pct_str": "", "active": False, "idx": 0, "job": None}
# Encode progress dot animation state
_encode_dots = {"active": False, "idx": 0, "job": None}

# Whisper transcription progress counter (for simple mode prefix)
_whisper_counter = {"idx": 0, "total": 0}


def _flush_ui_queue():
    """Process pending UI callbacks on the main thread with a time budget."""
    try:
        # Drop oldest entries if queue grows too large (prevents unbounded memory growth)
        while len(_ui_queue) > 5000:
            _ui_queue.popleft()
        deadline = time.monotonic() + 0.012  # 12ms budget per tick
        while time.monotonic() < deadline:
            try:
                fn = _ui_queue.popleft()
            except IndexError:
                break
            try:
                fn()
            except Exception:
                pass  # isolate per-callback — one failure must not break others
    except Exception:
        pass
    try:
        if root.winfo_exists():
            root.after(50, _flush_ui_queue)
    except Exception:
        pass


def _sync_mini_logs_timer():
    """Sync mini logs on a slow timer instead of per-log-line."""
    try:
        if '_sync_mini_logs_from_main' in globals():
            _sync_mini_logs_from_main()
    except Exception:
        pass
    try:
        if root.winfo_exists():
            root.after(250, _sync_mini_logs_timer)
    except Exception:
        pass


def log(text, tag=None):
    # Pre-strip leading newlines from "===" header lines in simple mode to avoid blank
    # spacers.  This MUST be done here (before _write is defined) rather than inside
    # _write().  Assigning to any closure-captured name inside a nested function makes
    # Python treat that name as a local throughout the entire nested function, so every
    # subsequent read of `text` (e.g. log_box.insert(..., text, ...)) would raise
    # UnboundLocalError — silently swallowed by the try/except — suppressing all output.
    if _is_simple_mode and tag == "header" and "===" in text and text.startswith("\n"):
        text = text.lstrip("\n")

    def _write():
        global _log_at_bottom, _log_scroll_freeze, _log_user_scrolled
        try:
            if 'log_box' in globals() and log_box.winfo_exists():
                # Update persistent auto-scroll flag
                # If the user physically scrolled (mouse wheel / scrollbar drag),
                # only re-enable auto-scroll when they reach the very bottom.
                try:
                    yview = log_box.yview()
                    if yview[1] >= 0.99:
                        _log_at_bottom = True
                        _log_user_scrolled = False
                    elif _log_user_scrolled:
                        # User actively scrolled away — stay off until they hit bottom
                        _log_at_bottom = False
                    elif yview[1] < 0.90:
                        _log_at_bottom = False
                    # Between 0.90-0.99 without user scroll: keep previous value
                except Exception:
                    pass
                at_bottom = _log_at_bottom

                # Freeze scrollbar layout during batch delete/insert to prevent visual jitter
                _log_scroll_freeze = True
                log_box.config(state="normal")

                use_tag = tag
                if not use_tag:
                    if "already been recorded in the archive" in text or "[youtube:tab]" in text:
                        use_tag = "dim"
                    elif any(x in text for x in
                             ["Channel already initialized", "[download] Destination:", "100%", "✓"]):
                        use_tag = "green"
                    elif any(x in text for x in ["ERROR:", "Error:", "failed"]):
                        use_tag = "red"
                    elif "SYNCING:" in text or "---" in text:
                        use_tag = "header"
                    elif "SUMMARY:" in text or "TOTAL SESSION" in text:
                        use_tag = "summary"

                # Whisper progress: always replace in-place (both simple + verbose)
                # Split percentage out and render it green, with animated trailing dots
                if use_tag == "whisper_progress":
                    # Delete ALL whisper_progress/pct/dots ranges
                    for _wp_tag in ("whisper_dots", "whisper_pct", "whisper_progress"):
                        while True:
                            _wr = log_box.tag_ranges(_wp_tag)
                            if not _wr:
                                break
                            log_box.delete(_wr[0], _wr[1])
                    # Split text to colorize the percentage green
                    import re as _re_wp
                    _wp_match = _re_wp.search(r'(\d+%)', text)
                    if _wp_match:
                        _before = text[:_wp_match.start()]
                        _pct_str = _wp_match.group(1)
                        # Strip trailing dots/newline — we animate those separately
                        _after_raw = text[_wp_match.end():]
                        _suffix = _after_raw.rstrip(".\n ").rstrip()
                        log_box.insert(tk.END, _before, "whisper_progress")
                        log_box.insert(tk.END, _pct_str, "whisper_pct")
                        if _suffix:
                            log_box.insert(tk.END, _suffix, "whisper_progress")
                        # Animated dots
                        _dot_chars = [".", "..", "..."]
                        _whisper_dots["base_before"] = _before
                        _whisper_dots["pct_str"] = _pct_str
                        _whisper_dots["suffix"] = _suffix
                        _d = _dot_chars[_whisper_dots["idx"] % 3]
                        log_box.insert(tk.END, _d + "\n", "whisper_dots")
                        # Start dot animation timer if not already running
                        if not _whisper_dots["active"]:
                            _whisper_dots["active"] = True
                            _whisper_dots["idx"] = 0
                            if _root_alive:
                                _whisper_dots["job"] = root.after(350, _whisper_dot_tick)
                    else:
                        log_box.insert(tk.END, text, "whisper_progress")
                    _log_scroll_freeze = False
                    _auto_scrollbar(log_scroll, *log_box.yview())
                    if at_bottom:
                        log_box.see(tk.END)
                    log_box.config(state="disabled")
                    return

                if _is_simple_mode:
                    if use_tag == "header" and "---" in text:
                        _log_scroll_freeze = False
                        log_box.config(state="disabled")
                        return

                    if use_tag not in ("red", "simpleline", "simpleline_green", "simpleline_blue",
                                       "summary", "header",
                                       "simpledownload", "pauselog", "pausestatus", "livestream", "filterskip",
                                       "transcribe_using"):
                        if "SUMMARY:" not in text and "TOTAL SESSION" not in text and "===" not in text:
                            _log_scroll_freeze = False
                            log_box.config(state="disabled")
                            return

                    _ss_insert_pos = None  # position to insert in-place (anti-jitter)
                    if use_tag in ("simpleline", "simpleline_green", "simpleline_blue", "transcribe_using", "filterskip"):
                        ranges = log_box.tag_ranges("simplestatus")
                        if ranges:
                            _ss_insert_pos = log_box.index(ranges[0])
                            log_box.delete(ranges[0], ranges[1])
                        else:
                            # Fallback anchor: insert before pausestatus when simplestatus is absent
                            _ps_r = log_box.tag_ranges("pausestatus")
                            if _ps_r:
                                _ss_insert_pos = log_box.index(_ps_r[0])

                    # Purge "using/fetching" lines when a done line arrives
                    if use_tag in ("simpleline_green", "simpleline_blue"):
                        for _tu_tag in ("transcribe_using",):
                            while True:
                                _tu_r = log_box.tag_ranges(_tu_tag)
                                if not _tu_r:
                                    break
                                log_box.delete(_tu_r[0], _tu_r[1])

                    if use_tag in ("simpledownload", "simpleline", "simpleline_green", "simpleline_blue", "red", "summary", "transcribe_using"):
                        dl_ranges = log_box.tag_ranges("dlprogress")
                        if dl_ranges:
                            log_box.delete(dl_ranges[0], dl_ranges[1])
                else:
                    dl_ranges = log_box.tag_ranges("dlprogress")
                    if dl_ranges:
                        log_box.delete(dl_ranges[0], dl_ranges[1])

                # Save whisper progress content before clearing (to re-insert after new content)
                # Collect all ranges across tags, sorted by position, to preserve interleaved order
                _saved_wp_parts = []  # list of (text, tag) to re-insert
                _wp_ranges = []
                for _wp_tag in ("whisper_progress", "whisper_pct", "whisper_dots"):
                    _wr = log_box.tag_ranges(_wp_tag)
                    for _ri in range(0, len(_wr), 2):
                        _wp_ranges.append((_wr[_ri], _wr[_ri + 1], _wp_tag))
                _wp_ranges.sort(key=lambda r: log_box.index(r[0]))
                for _start, _end, _wp_tag in _wp_ranges:
                    _saved_wp_parts.append((log_box.get(_start, _end), _wp_tag))
                # Delete in reverse order to preserve positions
                for _start, _end, _ in reversed(_wp_ranges):
                    log_box.delete(_start, _end)
                _had_whisper = bool(_saved_wp_parts)
                if _had_whisper:
                    _stop_whisper_dot_anim()

                if (_is_simple_mode
                        and use_tag in ("simpledownload", "red", "summary", "header")):
                    ss_ranges = log_box.tag_ranges("simplestatus")
                    if ss_ranges:
                        log_box.insert(ss_ranges[0], text, use_tag)
                    else:
                        # No simplestatus — insert before pausestatus anchor if present
                        _ps_r2 = log_box.tag_ranges("pausestatus")
                        if _ps_r2:
                            log_box.insert(_ps_r2[0], text, use_tag)
                        else:
                            log_box.insert(tk.END, text, use_tag)
                elif _is_simple_mode and _ss_insert_pos is not None:
                    # Insert at the old simplestatus/pausestatus position to avoid jitter
                    log_box.insert(_ss_insert_pos, text, use_tag)
                elif _is_simple_mode and use_tag == "pausestatus":
                    # pausestatus always goes at the very bottom
                    log_box.insert(tk.END, text, use_tag)
                else:
                    log_box.insert(tk.END, text, use_tag)

                # Re-insert whisper progress at the bottom so it stays visible
                if _had_whisper and _saved_wp_parts:
                    # Re-insert whisper before pausestatus so it stays above the pause anchor
                    _ps_for_w = log_box.tag_ranges("pausestatus")
                    _w_ins_pt = log_box.index(_ps_for_w[0]) if _ps_for_w else tk.END
                    for _wp_text, _wp_tag in _saved_wp_parts:
                        log_box.insert(_w_ins_pt, _wp_text, _wp_tag)
                        if _w_ins_pt != tk.END:
                            _w_ins_pt = log_box.index(f"{_w_ins_pt} + {len(_wp_text)}c")
                    # Restart dot animation
                    if not _whisper_dots["active"]:
                        _whisper_dots["active"] = True
                        _whisper_dots["idx"] = 0
                        if _root_alive:
                            _whisper_dots["job"] = root.after(350, _whisper_dot_tick)

                if _autorun_active and _is_simple_mode:
                    try:
                        line_count = int(log_box.index("end-1c").split(".")[0])
                        if line_count > 20:
                            log_box.delete("1.0", f"{line_count - 20}.0")
                    except Exception:
                        pass
                else:
                    # Cap verbose log at 20000 lines to prevent unbounded memory growth;
                    # when exceeded, trim to 15000 — enough headroom for large single syncs
                    try:
                        line_count = int(log_box.index("end-1c").split(".")[0])
                        if line_count > 20000:
                            log_box.delete("1.0", f"{line_count - 15000}.0")
                    except Exception:
                        pass

                # Unfreeze scrollbar layout now that batch ops are done
                _log_scroll_freeze = False
                _auto_scrollbar(log_scroll, *log_box.yview())

                # Skip auto-scroll for transient/passive messages that shouldn't
                # disrupt the user's current scroll position
                _skip_scroll = (
                    (use_tag == "transcribe_using" and "Adding punctuation" in text) or
                    (use_tag == "header" and "Added to GPU Tasks:" in text)
                )
                if at_bottom and not _skip_scroll:
                    log_box.see(tk.END)
                log_box.config(state="disabled")

                # Show Clear log button when log has content
                if '_show_clear_log_if_needed' in globals():
                    _show_clear_log_if_needed()

            elif sys.stdout:
                sys.stdout.write(text)
                sys.stdout.flush()
        except Exception:
            # Ensure log_box is always re-disabled after an error mid-write
            _log_scroll_freeze = False
            try:
                if 'log_box' in globals() and log_box.winfo_exists():
                    log_box.config(state="disabled")
            except Exception:
                pass

    try:
        if _root_alive:
            _ui_queue.append(_write)
        elif sys.stdout:
            sys.stdout.write(text)
            sys.stdout.flush()
    except Exception:
        pass


def _whisper_dot_tick():
    """Animate the trailing dots on the Whisper progress line."""
    if not _whisper_dots["active"]:
        return
    _whisper_dots["idx"] = (_whisper_dots["idx"] + 1) % 3
    _dot_chars = [".", "..", "..."]
    try:
        if 'log_box' in globals() and log_box.winfo_exists():
            log_box.config(state="normal")
            try:
                # Only replace the dots portion
                _dr = log_box.tag_ranges("whisper_dots")
                if _dr:
                    log_box.delete(_dr[0], _dr[1])
                    log_box.insert(_dr[0], _dot_chars[_whisper_dots["idx"]] + "\n", "whisper_dots")
            finally:
                log_box.config(state="disabled")
    except Exception:
        pass
    if _whisper_dots["active"] and 'root' in globals():
        try:
            _whisper_dots["job"] = root.after(350, _whisper_dot_tick)
        except Exception:
            pass


def _stop_whisper_dot_anim():
    """Stop the whisper dot animation."""
    _whisper_dots["active"] = False
    _old_job = _whisper_dots.get("job")
    _whisper_dots["job"] = None
    if _old_job:
        def _do_cancel():
            try:
                root.after_cancel(_old_job)
            except Exception:
                pass
        _ui_queue.append(_do_cancel)


def _clear_whisper_progress():
    """Clear the whisper progress line and stop its dot animation (used on cancel)."""
    _stop_whisper_dot_anim()
    def _write():
        try:
            if 'log_box' in globals() and log_box.winfo_exists():
                log_box.config(state="normal")
                for _tag in ("whisper_dots", "whisper_pct", "whisper_progress"):
                    while True:
                        _r = log_box.tag_ranges(_tag)
                        if not _r:
                            break
                        log_box.delete(_r[0], _r[1])
                log_box.config(state="disabled")
        except Exception:
            pass
    try:
        if _root_alive:
            _ui_queue.append(_write)
    except Exception:
        pass


def log_progress_bar(current, total):
    def _write():
        try:
            if 'log_box' in globals() and log_box.winfo_exists():
                if _is_simple_mode:
                    return
                try:
                    at_bottom = log_box.yview()[1] >= 0.99
                except Exception:
                    at_bottom = True

                log_box.config(state="normal")
                ranges = log_box.tag_ranges("scanline")
                if ranges:
                    log_box.delete(ranges[0], ranges[1])
                if total > 0:
                    pct = current / total
                    filled = int(28 * pct)
                    bar = "█" * filled + "░" * (28 - filled)
                    msg = f"  {bar}  {current:,} / {total:,}\n"
                else:
                    msg = f"  [scanning...  {current:,} checked]\n"
                log_box.insert(tk.END, msg, "scanline")

                if at_bottom:
                    log_box.see(tk.END)
                log_box.config(state="disabled")
        except Exception:
            pass

    try:
        if _root_alive:
            _ui_queue.append(_write)
    except Exception:
        pass


def log_dl_progress(msg):
    # Pre-parse the percentage and filled bar positions so we can color them green
    _pct_match = re.search(r'\d+\.?\d*%', msg)
    _pct_start = _pct_match.start() if _pct_match else -1
    _pct_end = _pct_match.end() if _pct_match else -1

    _bar_match = re.search(r'█+', msg)
    _bar_start = _bar_match.start() if _bar_match else -1
    _bar_end = _bar_match.end() if _bar_match else -1

    def _apply_pct_tag(insert_pos):
        """Apply green tag to the filled bar and percentage portions starting at insert_pos."""
        if _bar_start >= 0:
            try:
                log_box.tag_add("dlprogress_pct",
                                f"{insert_pos}+{_bar_start}c",
                                f"{insert_pos}+{_bar_end}c")
            except Exception:
                pass
        if _pct_start >= 0:
            try:
                log_box.tag_add("dlprogress_pct",
                                f"{insert_pos}+{_pct_start}c",
                                f"{insert_pos}+{_pct_end}c")
            except Exception:
                pass

    def _write():
        try:
            if 'log_box' in globals() and log_box.winfo_exists():
                log_box.config(state="normal")
                ranges = log_box.tag_ranges("dlprogress")
                _dl_pos = None
                if ranges:
                    _dl_pos = log_box.index(ranges[0])
                    log_box.delete(ranges[0], ranges[1])
                if _dl_pos:
                    log_box.insert(_dl_pos, msg, "dlprogress")
                    _apply_pct_tag(_dl_pos)
                elif _is_simple_mode:
                    ss_ranges = log_box.tag_ranges("simplestatus")
                    if ss_ranges:
                        _insert_pos = log_box.index(ss_ranges[0])
                        log_box.insert(_insert_pos, msg, "dlprogress")
                        _apply_pct_tag(_insert_pos)
                    else:
                        _insert_pos = log_box.index(tk.END)
                        log_box.insert(tk.END, msg, "dlprogress")
                        _apply_pct_tag(_insert_pos)
                else:
                    _insert_pos = log_box.index(tk.END)
                    log_box.insert(tk.END, msg, "dlprogress")
                    _apply_pct_tag(_insert_pos)

                # Never auto-scroll for download progress — it replaces in-place
                # and shouldn't snap the user back to the bottom
                log_box.config(state="disabled")
        except Exception:
            pass

    try:
        if _root_alive:
            _ui_queue.append(_write)
    except Exception:
        pass


def clear_transient_lines():
    def _write():
        try:
            if 'log_box' in globals() and log_box.winfo_exists():
                log_box.config(state="normal")
                for tag in ["scanline", "dlprogress", "simplestatus"]:
                    ranges = log_box.tag_ranges(tag)
                    if ranges:
                        log_box.delete(ranges[0], ranges[1])
                # Remove any orphaned colour-only tags that outlive their base text tag
                for orphan_tag in ("simplestatus_green", "dlprogress_pct"):
                    log_box.tag_remove(orphan_tag, "1.0", tk.END)
                log_box.config(state="disabled")
        except Exception:
            pass

    try:
        if _root_alive:
            _ui_queue.append(_write)
    except Exception:
        pass


def log_simple_status(text=None, extra_tag=None, segments=None):
    def _write():
        try:
            if 'log_box' in globals() and log_box.winfo_exists():
                try:
                    at_bottom = log_box.yview()[1] >= 0.99
                except Exception:
                    at_bottom = True

                # Compute the full text - either from segments or the text parameter
                _full_text = "".join(s for s, _ in segments) if segments is not None else (text or "")

                log_box.config(state="normal")

                # Save encode/whisper progress so they stay above the sync line
                # Collect all ranges sorted by position to preserve interleaved order
                _saved_parts = []
                _all_ranges = []
                for _tag in ("encode_progress", "encode_pct", "encode_dots", "encode_suffix",
                             "whisper_progress", "whisper_pct", "whisper_dots"):
                    _r = log_box.tag_ranges(_tag)
                    for _ri in range(0, len(_r), 2):
                        _all_ranges.append((_r[_ri], _r[_ri + 1], _tag))
                _all_ranges.sort(key=lambda r: log_box.index(r[0]))
                for _start, _end, _tag in _all_ranges:
                    _saved_parts.append((log_box.get(_start, _end), _tag))
                for _start, _end, _ in reversed(_all_ranges):
                    log_box.delete(_start, _end)

                # Save pausestatus so it stays at the very bottom (after simplestatus)
                _saved_ps = []
                _ps_ranges = log_box.tag_ranges("pausestatus")
                if _ps_ranges:
                    for _ri in range(0, len(_ps_ranges), 2):
                        _saved_ps.append(log_box.get(_ps_ranges[_ri], _ps_ranges[_ri + 1]))
                    for _ri in range(len(_ps_ranges) - 2, -1, -2):
                        log_box.delete(_ps_ranges[_ri], _ps_ranges[_ri + 1])

                ranges = log_box.tag_ranges("simplestatus")
                if ranges:
                    _pos = log_box.index(ranges[0])
                    log_box.delete(ranges[0], ranges[1])
                    log_box.insert(_pos, _full_text, "simplestatus")
                    # Apply extra_tag via tag_add after insert so it covers exactly the
                    # inserted text range — avoids Tkinter's boundary tag-inheritance
                    # ambiguity (inserting at a tag boundary with a tuple can pull adjacent
                    # non-simplestatus lines into the range and cause them to be purged).
                    if segments is not None:
                        _seg_p = _pos
                        for _st, _stag in segments:
                            if _stag and _st:
                                _se = log_box.index(f"{_seg_p}+{len(_st)}c")
                                log_box.tag_add(_stag, _seg_p, _se)
                            if _st:
                                _seg_p = log_box.index(f"{_seg_p}+{len(_st)}c")
                    elif extra_tag and _full_text:
                        _new_end = log_box.index(f"{_pos}+{len(_full_text)}c")
                        log_box.tag_add(extra_tag, _pos, _new_end)
                else:
                    _ins_start = log_box.index(tk.END)
                    log_box.insert(tk.END, _full_text, "simplestatus")
                    if segments is not None:
                        _seg_p = _ins_start
                        for _st, _stag in segments:
                            if _stag and _st:
                                _se = log_box.index(f"{_seg_p}+{len(_st)}c")
                                log_box.tag_add(_stag, _seg_p, _se)
                            if _st:
                                _seg_p = log_box.index(f"{_seg_p}+{len(_st)}c")
                    elif extra_tag and _full_text:
                        _new_end = log_box.index(f"{_ins_start}+{len(_full_text)}c")
                        log_box.tag_add(extra_tag, _ins_start, _new_end)

                # Re-insert encode/whisper progress AFTER simplestatus (before pausestatus)
                if _saved_parts:
                    _ps_r_ep = log_box.tag_ranges("pausestatus")
                    _ins_pt = log_box.index(_ps_r_ep[0]) if _ps_r_ep else tk.END
                    for _s_text, _s_tag in _saved_parts:
                        log_box.insert(_ins_pt, _s_text, _s_tag)
                        if _ins_pt != tk.END:
                            _ins_pt = log_box.index(f"{_ins_pt} + {len(_s_text)}c")
                    # Re-apply encode_prefix/encode_title overlays lost during save/restore
                    _ep_r = log_box.tag_ranges("encode_progress")
                    if _ep_r:
                        _ep_start = _ep_r[-2]
                        _ep_text = log_box.get(_ep_r[-2], _ep_r[-1])
                        _enc_i = _ep_text.find("ENCODING")
                        if _enc_i >= 0:
                            # Match ": title (duration) - " or ": title - " after "ENCODING"
                            _pfx_m = re.match(
                                r'^(: .+?)(\s+\([^)]+\)\s*-\s*|\s*-\s*)(.*)$',
                                _ep_text[_enc_i + 8:]
                            )
                            if _pfx_m:
                                _pfx_end = log_box.index(f"{_ep_start}+{_enc_i}c")
                                log_box.tag_add("encode_prefix", _ep_start, _pfx_end)
                                _ttl_start = log_box.index(f"{_ep_start}+{_enc_i + 8}c")
                                _ttl_end = log_box.index(
                                    f"{_ttl_start}+{len(_pfx_m.group(1))}c")
                                log_box.tag_add("encode_title", _ttl_start, _ttl_end)

                # Re-insert pausestatus at the very bottom (after simplestatus)
                for _ps_text in _saved_ps:
                    log_box.insert(tk.END, _ps_text, "pausestatus")

                # Only auto-scroll when inserting new content (not replacing in-place)
                if at_bottom and not ranges:
                    log_box.see(tk.END)
                log_box.config(state="disabled")
        except Exception:
            pass

    try:
        if _root_alive:
            _ui_queue.append(_write)
    except Exception:
        pass


def clear_simple_status():
    def _write():
        try:
            if 'log_box' in globals() and log_box.winfo_exists():
                log_box.config(state="normal")
                ranges = log_box.tag_ranges("simplestatus")
                if ranges:
                    log_box.delete(ranges[0], ranges[1])
                log_box.config(state="disabled")
        except Exception:
            pass

    try:
        if _root_alive:
            _ui_queue.append(_write)
    except Exception:
        pass


def clear_pause_status():
    """Remove the active pause-status anchor line from the log (call before logging resume)."""
    def _write():
        try:
            if 'log_box' in globals() and log_box.winfo_exists():
                log_box.config(state="normal")
                _ps_r = log_box.tag_ranges("pausestatus")
                while _ps_r:
                    log_box.delete(_ps_r[0], _ps_r[1])
                    _ps_r = log_box.tag_ranges("pausestatus")
                log_box.config(state="disabled")
        except Exception:
            pass

    try:
        if _root_alive:
            _ui_queue.append(_write)
    except Exception:
        pass


def _update_encode_progress(text):
    """Update the in-place encoding progress line (replaces previous progress).

    Renders the percentage in green bold (like the whisper progress line) with
    animated trailing dots.
    """
    def _write():
        try:
            if 'log_box' in globals() and log_box.winfo_exists():
                log_box.config(state="normal")

                # Save whisper progress so it stays at the very bottom
                # Collect all ranges sorted by position to preserve interleaved order
                _saved_wp = []
                _wp_ranges = []
                for _tag in ("whisper_progress", "whisper_pct", "whisper_dots"):
                    _r = log_box.tag_ranges(_tag)
                    for _ri in range(0, len(_r), 2):
                        _wp_ranges.append((_r[_ri], _r[_ri + 1], _tag))
                _wp_ranges.sort(key=lambda r: log_box.index(r[0]))
                for _start, _end, _tag in _wp_ranges:
                    _saved_wp.append((log_box.get(_start, _end), _tag))
                for _start, _end, _ in reversed(_wp_ranges):
                    log_box.delete(_start, _end)

                # Delete existing encode progress/pct/dots/suffix ranges
                for _etag in ("encode_dots", "encode_pct", "encode_progress", "encode_suffix", "encode_prefix", "encode_title"):
                    while True:
                        _er = log_box.tag_ranges(_etag)
                        if not _er:
                            break
                        log_box.delete(_er[0], _er[1])

                # Determine insert position — after simplestatus, before pausestatus, then END
                _ps_r = log_box.tag_ranges("pausestatus")
                _enc_pos = log_box.index(_ps_r[0]) if _ps_r else tk.END

                # Split percentage out and render it green+bold
                import re as _re_ep
                _ep_match = _re_ep.search(r'(\d+%)', text)
                if _ep_match:
                    _before = text[:_ep_match.start()]
                    _pct_str = _ep_match.group(1)
                    _after_raw = text[_ep_match.end():]
                    # Strip trailing dots/newline — we animate those separately
                    _suffix = _after_raw.rstrip(".\n ").rstrip()
                    _dot_chars = [".", "..", "..."]
                    _d = _dot_chars[_encode_dots["idx"] % 3]
                    # Insert the whole _before as ONE encode_progress range (blue),
                    # then overlay white tags for "[N/M] " (prefix) and ": title" (title).
                    _enc_i = _before.find("ENCODING")
                    _pfx_m = _re_ep.match(
                        r'^(: .+?)(\s+\([^)]+\)\s*-\s*|\s*-\s*)$', _before[_enc_i + 8:]
                    ) if _enc_i >= 0 else None
                    _ins = _enc_pos
                    log_box.insert(_ins, _before, "encode_progress")
                    if _pfx_m and _enc_i >= 0:
                        # Use tag_ranges to locate the just-inserted range (works for both
                        # specific positions and tk.END insertion points).
                        # tag_ranges returns a flat list of (start, end) pairs; len >= 2 means
                        # at least one range exists.
                        _ep_r = log_box.tag_ranges("encode_progress")
                        if len(_ep_r) >= 2:
                            _ep_start = _ep_r[-2]  # start of the last encode_progress range
                            _pfx_end = log_box.index(f"{_ep_start} + {_enc_i}c")
                            log_box.tag_add("encode_prefix", _ep_start, _pfx_end)
                            _ttl_start = log_box.index(f"{_ep_start} + {_enc_i + 8}c")
                            _ttl_end = log_box.index(f"{_ttl_start} + {len(_pfx_m.group(1))}c")
                            log_box.tag_add("encode_title", _ttl_start, _ttl_end)
                    if _ins != tk.END:
                        _ins = log_box.index(f"{_ins} + {len(_before)}c")
                    log_box.insert(_ins, _pct_str, "encode_pct")
                    if _ins != tk.END:
                        _ins = log_box.index(f"{_ins} + {len(_pct_str)}c")
                    if _suffix:
                        log_box.insert(_ins, _d, "encode_dots")
                        if _ins != tk.END:
                            _ins = log_box.index(f"{_ins} + {len(_d)}c")
                        log_box.insert(_ins, _suffix + "\n", "encode_suffix")
                    else:
                        log_box.insert(_ins, _d + "\n", "encode_dots")
                    if not _encode_dots["active"]:
                        _encode_dots["active"] = True
                        _encode_dots["idx"] = 0
                        if _root_alive:
                            _encode_dots["job"] = root.after(350, _encode_dot_tick)
                else:
                    # No percentage (unknown duration or loading state) — show with animation.
                    # Insert as ONE encode_progress range; overlay white tags for prefix and title.
                    _stripped = text.rstrip(".\n ").rstrip()
                    _dot_chars = [".", "..", "..."]
                    _d = _dot_chars[_encode_dots["idx"] % 3]
                    _ins = _enc_pos
                    _enc_i2 = _stripped.find("ENCODING")
                    _sfx_m = _re_ep.match(
                        r'^(: .+?)(\s+\([^)]+\)\s*-\s*|\s*-\s*)(.*)$', _stripped[_enc_i2 + 8:]
                    ) if _enc_i2 >= 0 else None
                    log_box.insert(_ins, _stripped, "encode_progress")
                    if _sfx_m and _enc_i2 >= 0:
                        _ep_r2 = log_box.tag_ranges("encode_progress")
                        if len(_ep_r2) >= 2:
                            _ep_start2 = _ep_r2[-2]
                            _pfx_end2 = log_box.index(f"{_ep_start2} + {_enc_i2}c")
                            log_box.tag_add("encode_prefix", _ep_start2, _pfx_end2)
                            _ttl_start2 = log_box.index(f"{_ep_start2} + {_enc_i2 + 8}c")
                            _ttl_end2 = log_box.index(f"{_ttl_start2} + {len(_sfx_m.group(1))}c")
                            log_box.tag_add("encode_title", _ttl_start2, _ttl_end2)
                    if _ins != tk.END:
                        _ins = log_box.index(f"{_ins} + {len(_stripped)}c")
                    log_box.insert(_ins, _d + "\n", "encode_dots")
                    if not _encode_dots["active"]:
                        _encode_dots["active"] = True
                        _encode_dots["idx"] = 0
                        if _root_alive:
                            _encode_dots["job"] = root.after(350, _encode_dot_tick)

                # Re-insert whisper progress before pausestatus so it stays above the pause anchor
                if _saved_wp:
                    _ps_for_enc_w = log_box.tag_ranges("pausestatus")
                    _ew_ins_pt = log_box.index(_ps_for_enc_w[0]) if _ps_for_enc_w else tk.END
                    for _s_text, _s_tag in _saved_wp:
                        log_box.insert(_ew_ins_pt, _s_text, _s_tag)
                        if _ew_ins_pt != tk.END:
                            _ew_ins_pt = log_box.index(f"{_ew_ins_pt} + {len(_s_text)}c")

                if _log_at_bottom:
                    log_box.see(tk.END)
                log_box.config(state="disabled")
        except Exception:
            pass
    try:
        if _root_alive:
            _ui_queue.append(_write)
    except Exception:
        pass


def _encode_dot_tick():
    """Animate the trailing dots on the encode progress line."""
    if not _encode_dots["active"]:
        return
    _encode_dots["idx"] = (_encode_dots["idx"] + 1) % 3
    _dot_chars = [".", "..", "..."]
    def _tick():
        try:
            if 'log_box' in globals() and log_box.winfo_exists():
                log_box.config(state="normal")
                try:
                    _dr = log_box.tag_ranges("encode_dots")
                    if _dr:
                        log_box.delete(_dr[0], _dr[1])
                        # If an encode_suffix follows (pause text after dots), omit the
                        # newline from the dots so the suffix stays on the same line
                        _has_suffix = bool(log_box.tag_ranges("encode_suffix"))
                        _new_dots = _dot_chars[_encode_dots["idx"]]
                        if not _has_suffix:
                            _new_dots += "\n"
                        log_box.insert(_dr[0], _new_dots, "encode_dots")
                finally:
                    log_box.config(state="disabled")
        except Exception:
            pass
    _ui_queue.append(_tick)
    if _encode_dots["active"] and 'root' in globals():
        try:
            _encode_dots["job"] = root.after(350, _encode_dot_tick)
        except Exception:
            pass


def _clear_encode_progress():
    """Clear the encoding progress line and stop dot animation."""
    _encode_dots["active"] = False
    _old_job = _encode_dots.get("job")
    _encode_dots["job"] = None
    if _old_job:
        def _do_cancel():
            try:
                root.after_cancel(_old_job)
            except Exception:
                pass
        _ui_queue.append(_do_cancel)
    def _write():
        try:
            if 'log_box' in globals() and log_box.winfo_exists():
                log_box.config(state="normal")
                for _etag in ("encode_dots", "encode_pct", "encode_progress", "encode_suffix", "encode_prefix", "encode_title"):
                    while True:
                        _er = log_box.tag_ranges(_etag)
                        if not _er:
                            break
                        log_box.delete(_er[0], _er[1])
                log_box.config(state="disabled")
        except Exception:
            pass
    try:
        if _root_alive:
            _ui_queue.append(_write)
    except Exception:
        pass


def _clear_simpledownload_lines():
    def _write():
        try:
            if 'log_box' in globals() and log_box.winfo_exists():
                log_box.config(state="normal")

                ranges = list(log_box.tag_ranges("simpledownload"))
                for i in range(len(ranges) - 1, -1, -2):
                    log_box.delete(ranges[i - 1], ranges[i])
                log_box.config(state="disabled")
        except Exception:
            pass

    try:
        if _root_alive:
            _ui_queue.append(_write)
    except Exception:
        pass


_simple_anim_state = {"active": False, "channel": "", "idx": 0, "total": 0, "dots": 0, "job": None,
                      "dl_current": 0, "ch_total": 0, "page_num": 0, "enum_page": 0, "enum_count": 0}
_DOTS = ["·  ", "·· ", "···"]

# Startup loading animation (simple mode only)
_startup_loading = {"active": False, "dots": 0, "job": None}


def _startup_loading_tick():
    try:
        if not _startup_loading["active"] or not root.winfo_exists():
            return
        d = _DOTS[_startup_loading["dots"] % 3]
        _startup_loading["dots"] += 1
        log_simple_status(f"  Loading{d}\n")
    except Exception:
        pass
    finally:
        if _startup_loading["active"] and root.winfo_exists():
            try:
                _startup_loading["job"] = root.after(500, _startup_loading_tick)
            except Exception:
                pass


def _start_startup_loading():
    if not _is_simple_mode:
        return
    _startup_loading["active"] = True
    _startup_loading["dots"] = 0
    if _root_alive:
        try:
            _startup_loading["job"] = root.after(0, _startup_loading_tick)
        except Exception:
            pass


def _stop_startup_loading():
    _startup_loading["active"] = False
    _old_job = _startup_loading.get("job")
    _startup_loading["job"] = None
    if _old_job:
        try:
            root.after_cancel(_old_job)
        except Exception:
            pass
    clear_simple_status()


def _simple_anim_tick():
    try:
        if not _simple_anim_state["active"] or not root.winfo_exists():
            return

        if pause_event.is_set():
            # Show static paused state instead of animated dots
            i = _simple_anim_state["idx"]
            n = _simple_anim_state["total"]
            ch = _simple_anim_state["channel"]
            log_simple_status(f"[{i}/{n}] PAUSED: {ch}\n")
            return  # finally block handles rescheduling
        d = _DOTS[_simple_anim_state["dots"] % 3]
        _simple_anim_state["dots"] += 1
        i = _simple_anim_state["idx"]
        n = _simple_anim_state["total"]
        ch = _simple_anim_state["channel"]
        dl_cur = _simple_anim_state["dl_current"]
        ch_tot = _simple_anim_state["ch_total"]

        # Dynamically update total if items were added to the sync queue
        with _sync_queue_lock:
            dynamic_total = i + len(_sync_queue)
        if dynamic_total > n:
            n = dynamic_total
            _simple_anim_state["total"] = n

        _bs = _simple_anim_state.get("batch_size", 0)
        if dl_cur > 0 and _bs > 0:
            _b_num = (dl_cur - 1) // _bs + 1
            status_text = f"  Downloading video #{dl_cur} (Batch {_b_num})"
        elif dl_cur > 0 and ch_tot > 0:
            status_text = f"  Downloading video #{dl_cur} of {ch_tot}"
        elif dl_cur > 0:
            status_text = "  Downloading"
        else:
            status_text = "  "

        enum_page = _simple_anim_state.get("enum_page", 0)
        enum_count = _simple_anim_state.get("enum_count", 0)
        page = _simple_anim_state["page_num"]
        if enum_page > 0:
            log_simple_status(segments=[
                (f"[{i}/{n}]", None),
                (" SYNCING:", "simplestatus_green"),
                (f" {ch}  ", None),
                (d, "simplestatus_green"),
                ("\n", None),
                (f"Enumerating video IDs, {enum_count:,} found (first run only)", None),
                (d, "simplestatus_green"),
                ("\n", None),
            ])
        elif page > 0:
            log_simple_status(segments=[
                (f"[{i}/{n}]", None),
                (" SYNCING:", "simplestatus_green"),
                (f" {ch}", None),
                (status_text, None),
                (d, "simplestatus_green"),
                ("\n", None),
                (f"Downloading channel info, Page {page}", None),
                (d, "simplestatus_green"),
                ("\n", None),
            ])
        else:
            log_simple_status(segments=[
                (f"[{i}/{n}]", None),
                (" SYNCING:", "simplestatus_green"),
                (f" {ch}", None),
                (status_text, None),
                (d, "simplestatus_green"),
                ("\n", None),
            ])
    except Exception:
        pass
    finally:
        if _simple_anim_state["active"] and root.winfo_exists():
            _simple_anim_state["job"] = root.after(500, _simple_anim_tick)


def _start_simple_anim(channel, idx, total):
    _old_job = _simple_anim_state.get("job")
    _simple_anim_state.update({"active": True, "channel": channel,
                               "idx": idx, "total": total, "dots": 0,
                               "dl_current": 0, "ch_total": 0, "page_num": 0,
                               "enum_page": 0, "enum_count": 0, "batch_size": 0,
                               "job": None})
    def _do_start():
        if _old_job:
            try:
                root.after_cancel(_old_job)
            except Exception:
                pass
        if _simple_anim_state["active"] and root.winfo_exists():
            _simple_anim_state["job"] = root.after(500, _simple_anim_tick)
    _ui_queue.append(_do_start)


def _stop_simple_anim():
    _simple_anim_state["active"] = False
    _old_job = _simple_anim_state.get("job")
    _simple_anim_state["job"] = None
    if _old_job:
        def _do_cancel():
            try:
                root.after_cancel(_old_job)
            except Exception:
                pass
        _ui_queue.append(_do_cancel)


def _update_simple_dl(dl_current, ch_total, batch_size=0):
    _simple_anim_state["dl_current"] = dl_current
    _simple_anim_state["ch_total"] = ch_total
    _simple_anim_state["batch_size"] = batch_size


def _cleanup_partial_files(directory):
    """Remove .part, .temp, .ytdl and orphaned intermediate format files (.fNNN.ext) left behind by cancelled downloads."""
    if not directory or not os.path.isdir(directory):
        return
    # Longer delay to let Windows release file locks after process kill
    time.sleep(1.5)

    def _is_partial(name):
        if name.endswith('.part') or name.endswith('.temp') or name.endswith('.ytdl'):
            return True
        # Catch .mp4.part, .webm.part etc (double extension partials)
        if '.part' in name:
            return True
        # Catch .temp.mp4, .temp.webm etc (yt-dlp mid-download temp files)
        if '.temp.' in name.lower():
            return True
        # Intermediate format files like .f136.mp4, .f251.webm
        if re.search(r'\.f\d+(\.\w+)?$', name):
            return True
        # Catch muxed temp files like .mp4.temp or leftover .webm fragments
        base, ext = os.path.splitext(name)
        if ext.lower() in ('.webm', '.m4a', '.mp4'):
            # Check if this is an intermediate fragment (has .fNNN before extension)
            if re.search(r'\.f\d+$', base):
                return True
        return False

    cleaned = 0
    failed = []
    try:
        for entry in os.scandir(directory):
            if not entry.is_file():
                if entry.is_dir():
                    # Recurse into subdirectories (year/month folders)
                    _cleanup_partial_files(entry.path)
                continue
            if _is_partial(entry.name):
                try:
                    os.remove(entry.path)
                    cleaned += 1
                except OSError:
                    failed.append(entry.path)
    except OSError:
        pass

    # Retry any files that were still locked on first attempt
    if failed:
        time.sleep(2.0)
        for fp in failed:
            try:
                os.remove(fp)
                cleaned += 1
            except OSError:
                pass

    if cleaned:
        log(f"  🧹 Cleaned up {cleaned} partial file(s).\n", "dim")


def _startup_cleanup_temps():
    """Scan all channel folders on startup and remove leftover partial/temp files."""
    try:
        out_dir = config.get("output_dir", "").strip() or BASE_DIR
        if not os.path.isdir(out_dir):
            return
        channels = config.get("channels", [])
        if not channels:
            return
        total_cleaned = 0
        for ch in channels:
            folder_name = sanitize_folder(ch.get("folder_override", "") or ch.get("name", ""))
            ch_folder = os.path.join(out_dir, folder_name)
            if not os.path.isdir(ch_folder):
                continue
            # Inline scan without the sleep delay that _cleanup_partial_files uses
            for dirpath, _, files in os.walk(ch_folder):
                for f in files:
                    name = f
                    is_partial = (
                        name.endswith('.part') or name.endswith('.temp') or name.endswith('.ytdl')
                        or '.part' in name or '.temp.' in name.lower()
                        or bool(re.search(r'\.f\d+(\.\w+)?$', name))
                    )
                    if not is_partial:
                        base, ext = os.path.splitext(name)
                        if ext.lower() in ('.webm', '.m4a', '.mp4') and re.search(r'\.f\d+$', base):
                            is_partial = True
                    if is_partial:
                        try:
                            os.remove(os.path.join(dirpath, f))
                            total_cleaned += 1
                        except OSError:
                            pass
        if total_cleaned:
            log(f"  🧹 Cleaned up {total_cleaned} leftover temp file(s) from previous sessions.\n", "dim")
    except Exception:
        pass


def _fetch_video_title(vid_id):
    """Fetch video title from YouTube oEmbed API. Returns vid_id on failure."""
    try:
        url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={vid_id}&format=json"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read())
            return data.get("title", vid_id)
    except Exception:
        return vid_id


def spawn_yt_dlp(cmd):
    try:
        # Always use utf-8; yt-dlp outputs utf-8 and mbcs mangles apostrophes/special chars into '?'
        # Force unbuffered stdout so --flat-playlist --print id output isn't held in yt-dlp's buffer
        _env = os.environ.copy()
        _env["PYTHONUNBUFFERED"] = "1"
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            startupinfo=startupinfo,
            env=_env
        )
        with proc_lock:
            active_processes.append(proc)
        return proc
    except FileNotFoundError:
        log("ERROR: yt-dlp or ffmpeg not found. Check Settings or system PATH.\n", "red")
        return None


def cleanup_process(proc):
    if proc:
        with proc_lock:
            try:
                active_processes.remove(proc)
            except ValueError:
                pass


if os.name == 'nt':
    # Set proper ctypes types for 64-bit Windows (HANDLE is pointer-sized)
    try:
        ctypes.windll.kernel32.OpenProcess.restype = ctypes.c_void_p
        ctypes.windll.kernel32.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
        ctypes.windll.kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        ctypes.windll.ntdll.NtSuspendProcess.argtypes = [ctypes.c_void_p]
        ctypes.windll.ntdll.NtResumeProcess.argtypes = [ctypes.c_void_p]
    except Exception:
        pass


def _suspend_proc(proc):
    """Suspend a process at the OS level. Currently unused — pause relies on
    pipe backpressure instead (worker stops reading stdout, pipe buffer fills,
    yt-dlp naturally blocks on its next write). Kept for potential future use."""
    if proc is None or proc.poll() is not None:
        return
    try:
        if os.name == 'nt':
            PROCESS_SUSPEND_RESUME = 0x0800
            handle = ctypes.windll.kernel32.OpenProcess(PROCESS_SUSPEND_RESUME, False, proc.pid)
            if handle:
                ctypes.windll.ntdll.NtSuspendProcess(handle)
                ctypes.windll.kernel32.CloseHandle(handle)
        else:
            os.kill(proc.pid, signal.SIGSTOP)
    except Exception:
        pass


def _resume_proc(proc):
    """Resume a previously suspended process. Currently unused — see _suspend_proc."""
    if proc is None or proc.poll() is not None:
        return
    try:
        if os.name == 'nt':
            PROCESS_SUSPEND_RESUME = 0x0800
            handle = ctypes.windll.kernel32.OpenProcess(PROCESS_SUSPEND_RESUME, False, proc.pid)
            if handle:
                ctypes.windll.ntdll.NtResumeProcess(handle)
                ctypes.windll.kernel32.CloseHandle(handle)
        else:
            os.kill(proc.pid, signal.SIGCONT)
    except Exception:
        pass


def show_notification(title, message):
    if os.name == 'nt':
        try:
            safe_title = title.replace("'", "''")
            safe_message = message.replace("'", "''")
            # Windows Runtime toast shows "YT Archiver" as app name;
            # fallback to NotifyIcon if WinRT types unavailable
            ps = (
                "try{"
                "[void][Windows.UI.Notifications.ToastNotificationManager,"
                "Windows.UI.Notifications,ContentType=WindowsRuntime];"
                "[void][Windows.Data.Xml.Dom.XmlDocument,"
                "Windows.Data.Xml.Dom.XmlDocument,ContentType=WindowsRuntime];"
                "$t=[Windows.UI.Notifications.ToastNotificationManager]::"
                "GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02);"
                "$n=$t.GetElementsByTagName('text');"
                f"[void]$n.Item(0).AppendChild($t.CreateTextNode('{safe_title}'));"
                f"[void]$n.Item(1).AppendChild($t.CreateTextNode('{safe_message}'));"
                "$toast=[Windows.UI.Notifications.ToastNotification]::new($t);"
                "[Windows.UI.Notifications.ToastNotificationManager]::"
                "CreateToastNotifier('YT Archiver').Show($toast)"
                "}catch{"
                "Add-Type -AssemblyName System.Windows.Forms;"
                "$i=New-Object System.Windows.Forms.NotifyIcon;"
                "$i.Icon=[System.Drawing.SystemIcons]::Information;"
                "$i.Visible=$true;"
                f"$i.ShowBalloonTip(6000,'{safe_title}','{safe_message}',"
                "[System.Windows.Forms.ToolTipIcon]::None);"
                "Start-Sleep -Seconds 7;$i.Dispose()}"
            )
            subprocess.Popen(
                ['powershell', '-WindowStyle', 'Hidden', '-Command', ps],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                startupinfo=startupinfo
            )
        except Exception:
            pass


def check_directory_writable(path):
    try:
        os.makedirs(path, exist_ok=True)
        test_file = os.path.join(path, '.write_test')
        try:
            with open(test_file, 'w') as f:
                f.write('test')
        finally:
            if os.path.exists(test_file):
                os.remove(test_file)
        return True
    except (OSError, PermissionError):
        return False


def _remove_ids_from_archive(ids_to_remove):
    """Remove specific video IDs from the download archive file."""
    if not ids_to_remove:
        return
    remove_set = set(ids_to_remove)
    with io_lock:
        if not os.path.exists(ARCHIVE_FILE):
            return
        try:
            with open(ARCHIVE_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
            new_lines = []
            for line in lines:
                parts = line.strip().split()
                if len(parts) >= 2 and parts[-1] in remove_set:
                    continue  # Skip this line (remove it)
                new_lines.append(line)
            with open(ARCHIVE_FILE, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
        except (OSError, PermissionError) as e:
            log(f"  ⚠ Could not update archive file: {e}\n", "red")


def detect_url_type(url):
    url = url.strip()
    yt = r'(?:(?:www|m)\.)?youtube\.com'
    if re.search(rf'(?:{yt}/watch\?v=|youtu\.be/[\w-]+|{yt}/(?:shorts|live)/[\w-]+)', url):
        return "video"
    if re.search(rf'{yt}/playlist\?list=', url) or re.search(rf'{yt}/.*[?&]list=', url):
        return "channel"
    if re.search(rf'{yt}/(?:@[\w.-]+|channel/[\w-]+|c/[\w-]+|user/[\w-]+)', url):
        return "channel"
    return "unknown"


def build_format_string(resolution):
    h = f"[height<={resolution}]" if resolution != "best" else ""

    # Prefer H.264 video + AAC audio for maximum Windows/MP4 compatibility.
    # These combos merge instantly with -c copy into MP4 (no re-encoding).
    # First choice: H.264 + AAC (native MP4 codecs, instant merge)
    # Second choice: H.264 + any audio (ffmpeg handles muxing)
    # Third choice: any non-AV1 + AAC audio (fast merge into MP4)
    # Fourth choice: any non-AV1 + any audio
    # Last resort: anything available at requested res, then try adjacent, then any
    base = (
        f"(bestvideo{h}[vcodec^=avc]+bestaudio[acodec^=mp4a])"
        f"/(bestvideo{h}[vcodec^=avc]+bestaudio)"
        f"/(bestvideo{h}[vcodec!^=av01]+bestaudio[acodec^=mp4a])"
        f"/(bestvideo{h}[vcodec!^=av01]+bestaudio)"
        f"/(bestvideo{h}+bestaudio)"
        f"/best{h}"
    )

    if resolution == "best":
        return base

    # Fallback: try one step up, then one step down, then any resolution
    res_int = int(resolution)
    res_above = None
    res_below = None
    for i, r in enumerate(RESOLUTION_OPTIONS[:-1]):  # exclude "best"
        if int(r) == res_int:
            if i + 1 < len(RESOLUTION_OPTIONS) - 1:
                res_above = RESOLUTION_OPTIONS[i + 1]
            if i > 0:
                res_below = RESOLUTION_OPTIONS[i - 1]
            break

    fallbacks = ""
    if res_above:
        ha = f"[height<={res_above}]"
        fallbacks += f"/(bestvideo{ha}+bestaudio)/best{ha}"
    if res_below:
        hb = f"[height<={res_below}]"
        fallbacks += f"/(bestvideo{hb}+bestaudio)/best{hb}"
    # Absolute last resort: any quality at all
    fallbacks += "/(bestvideo+bestaudio)/best"

    return base + fallbacks


def sanitize_folder(name):
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', name).strip().rstrip('.')


def load_config():
    with config_lock:
        cfg = copy.deepcopy(DEFAULT_CONFIG)
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    if isinstance(loaded, dict):
                        cfg.update(loaded)
            except (json.JSONDecodeError, OSError, PermissionError):
                pass
        if not isinstance(cfg.get("recent_downloads"), list):
            cfg["recent_downloads"] = []
        if not isinstance(cfg.get("channels"), list):
            cfg["channels"] = []

        # Data migration: min_duration used to be stored in raw seconds (entered directly
        # by the user with a "Min (s)" label).  It is now stored as whole-minutes * 60.
        # Values that are already multiples of 60 are unambiguous and need no change.
        # Values that are positive but less than 60 were sub-minute second-based entries
        # (e.g. 30 s) — round them up to the nearest minute so nothing is silently lost.
        for ch in cfg.get("channels", []):
            _md = ch.get("min_duration", 0)
            if isinstance(_md, (int, float)) and 0 < _md < 60:
                ch["min_duration"] = 60  # promote to 1 minute rather than losing to 0

        return cfg


_save_config_queue = []
_save_config_lock = threading.Lock()
_save_config_thread_running = False


def save_config(cfg):
    """Queue a config save to run on a background thread so disk I/O never blocks the UI."""
    global _save_config_thread_running

    def _do_save():
        global _save_config_thread_running
        try:
            with config_lock:
                try:
                    temp_file = CONFIG_FILE + ".tmp"
                    with open(temp_file, "w", encoding="utf-8") as f:
                        json.dump(cfg, f, indent=2)
                    os.replace(temp_file, CONFIG_FILE)
                except (PermissionError, OSError) as e:
                    # Retry once after a brief pause (disk may be momentarily busy)
                    try:
                        time.sleep(0.5)
                        temp_file = CONFIG_FILE + ".tmp"
                        with open(temp_file, "w", encoding="utf-8") as f:
                            json.dump(cfg, f, indent=2)
                        os.replace(temp_file, CONFIG_FILE)
                    except (PermissionError, OSError) as e2:
                        try:
                            if sys.stdout:
                                sys.stdout.write(f"ERROR: Could not save config: {e2}\n")
                        except Exception:
                            pass
        finally:
            _save_config_thread_running = False

    with _save_config_lock:
        if not _save_config_thread_running:
            _save_config_thread_running = True
            threading.Thread(target=_do_save, daemon=True).start()


config = load_config()

# Migrate old compress level names → new quality names
_config_migrated = False
for _ch_mig in config.get("channels", []):
    _old_lv = _ch_mig.get("compress_level", "")
    if _old_lv in _LEVEL_MIGRATION:
        _ch_mig["compress_level"] = _LEVEL_MIGRATION[_old_lv]
        _config_migrated = True
if _config_migrated:
    save_config(config)

try:
    from ctypes import windll

    windll.shcore.SetProcessDpiAwareness(1)
except Exception:
    pass

# Force dark mode for context menus (system tray, etc.) on Windows 10 1903+
try:
    _uxtheme = ctypes.windll.uxtheme
    _uxtheme[135](2)   # SetPreferredAppMode(ForceDark=2)
    _uxtheme[136]()     # FlushMenuThemes
except Exception:
    pass

root = tk.Tk()
root.title("YT Archiver")

# Start the UI queue flush timer — processes log callbacks from worker threads
root.after(50, _flush_ui_queue)
# Start the mini-log sync timer — mirrors main log to Subs/Recent tab mini-logs
root.after(250, _sync_mini_logs_timer)

# Set window/taskbar icon
icon_path = os.path.join(RESOURCE_PATH, "icon.ico")
if not os.path.exists(icon_path) and getattr(sys, 'frozen', False):
    # When frozen, --icon only sets the .exe icon, not a bundled file.
    # Extract the icon from the running .exe itself via Win32 API.
    try:
        import tempfile
        from ctypes import windll, c_int, byref
        exe_path = sys.executable
        hicon_small = c_int()
        hicon_large = c_int()
        windll.shell32.ExtractIconExW(exe_path, 0, byref(hicon_large), byref(hicon_small), 1)
        if hicon_large.value:
            hwnd = windll.user32.GetParent(root.winfo_id())
            WM_SETICON = 0x0080
            windll.user32.SendMessageW(hwnd, WM_SETICON, 1, hicon_large.value)  # ICON_BIG
            windll.user32.SendMessageW(hwnd, WM_SETICON, 0, hicon_small.value or hicon_large.value)  # ICON_SMALL
    except Exception:
        pass
elif os.path.exists(icon_path):
    root.iconbitmap(icon_path)
    root.iconbitmap(default=icon_path)

# ─── System tray icon ────────────────────────────────────────────────
def _generate_spin_frames(base_img, num_frames=12, color=(100, 180, 255, 220)):
    """Pre-generate spinning arc overlay frames composited onto the base icon."""
    frames = []
    sz = base_img.size[0]
    for i in range(num_frames):
        frame = base_img.copy()
        overlay = Image.new("RGBA", frame.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        angle_start = i * (360 // num_frames)
        # Draw a partial arc (120 degrees) as the spinner
        pad = max(2, sz // 8)
        bbox = [pad, pad, sz - pad, sz - pad]
        draw.arc(bbox, angle_start, angle_start + 120, fill=color, width=max(2, sz // 12))
        frame = Image.alpha_composite(frame.convert("RGBA"), overlay)
        frames.append(frame)
    return frames


def _generate_flash_frames(base_img, num_frames=8):
    """Pre-generate pulsing red flash frames for transcription — highly visible.
    Alternates between a bright red-tinted icon and the normal icon with a
    large red dot, creating an unmistakable flashing effect.
    """
    frames = []
    sz = base_img.size[0]
    for i in range(num_frames):
        # Sine wave for smooth pulse: 0.0 → 1.0 → 0.0
        import math
        t = (math.sin(i / num_frames * 2 * math.pi - math.pi / 2) + 1) / 2  # 0..1
        alpha = int(60 + t * 180)  # range 60..240
        frame = base_img.copy().convert("RGBA")
        overlay = Image.new("RGBA", frame.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        # Large red circle covering most of the icon
        pad = max(2, sz // 6)
        draw.ellipse([pad, pad, sz - pad, sz - pad], fill=(220, 40, 40, alpha))
        frame = Image.alpha_composite(frame, overlay)
        frames.append(frame)
    return frames


def _make_badge_icon(base_img, count):
    """Composite a bright green notification dot onto the icon."""
    img = base_img.copy().convert("RGBA")
    sz = img.size[0]
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    badge_r = max(5, sz // 4)
    cx = sz - badge_r - 1
    cy = badge_r + 1
    # White outline for contrast
    draw.ellipse([cx - badge_r - 1, cy - badge_r - 1, cx + badge_r + 1, cy + badge_r + 1],
                 fill=(255, 255, 255, 255))
    # Bright green dot
    draw.ellipse([cx - badge_r, cy - badge_r, cx + badge_r, cy + badge_r],
                 fill=(50, 205, 50, 255))
    return Image.alpha_composite(img, overlay)


_tray_spin_use_red = False  # which frame set is active
_tray_spin_thread = None    # persistent spin animation thread
_tray_spin_stop_ev = threading.Event()  # signal spin thread to exit

def _tray_spin_loop():
    """Persistent thread for tray spinning animation (avoids Timer churn)."""
    global _tray_spin_idx
    while not _tray_spin_stop_ev.is_set():
        _frames = _tray_spin_frames_red if _tray_spin_use_red else _tray_spin_frames
        if not _tray_spin_active or _tray_icon is None or not _frames:
            break
        _tray_spin_idx = (_tray_spin_idx + 1) % len(_frames)
        try:
            _tray_icon.icon = _frames[_tray_spin_idx]
        except Exception:
            pass
        # Red flash pulses slower (0.12s × 8 frames = ~1s cycle), blue spins faster
        _interval = 0.12 if _tray_spin_use_red else 0.18
        _tray_spin_stop_ev.wait(_interval)


def _tray_start_spin(red=False):
    """Start the spinning animation on the tray icon.
    red=True uses a bold red spinner (for transcription / heavy GPU work).
    Red always takes priority over blue — if GPU tasks are running, a sync
    call with red=False will not downgrade to blue.
    """
    global _tray_spin_active, _tray_spin_idx, _tray_spin_use_red, _tray_spin_thread
    if not HAS_TRAY or _tray_icon is None:
        return
    # Red (GPU) always takes priority — don't downgrade to blue if GPU is active
    if not red and _gpu_running and _tray_spin_use_red:
        return
    _tray_spin_use_red = red
    _tray_spin_active = True
    _tray_spin_idx = 0
    # Start persistent spin thread if not already running
    if _tray_spin_thread is None or not _tray_spin_thread.is_alive():
        _tray_spin_stop_ev.clear()
        _tray_spin_thread = threading.Thread(target=_tray_spin_loop, daemon=True)
        _tray_spin_thread.start()


def _tray_stop_spin(force=False):
    """Stop spinning and restore the base or badge icon.
    If GPU tasks just finished but sync is still running, fall back to blue spin
    (unless force=True, which is used for pause).
    """
    global _tray_spin_active
    # If sync is still running, fall back to blue spin instead of stopping
    if not force and _sync_running:
        _tray_start_spin(red=False)
        return
    _tray_spin_active = False
    _tray_spin_stop_ev.set()  # signal spin thread to exit
    _update_tray_badge()


def _update_tray_badge():
    """Update the tray icon to show a badge with the current download count, or restore base."""
    if not HAS_TRAY or _tray_icon is None or _tray_base_img is None:
        return
    if _tray_spin_active:
        return  # don't overwrite spin frames
    try:
        if new_download_count > 0:
            _tray_icon.icon = _make_badge_icon(_tray_base_img, new_download_count)
        else:
            _tray_icon.icon = _tray_base_img
    except Exception:
        pass


def _update_tray_tooltip(text):
    """Update the tray icon tooltip text."""
    if not HAS_TRAY or _tray_icon is None:
        return
    try:
        _tray_icon.title = text
    except Exception:
        pass


def _tray_sync_subbed(icon, item):
    """Tray menu action: Sync Subbed."""
    if _root_alive:
        _ui_queue.append(start_sync_all)


def _tray_quit(icon, item):
    """Tray menu action: Quit."""
    if _root_alive:
        _ui_queue.append(lambda: on_closing())


def _tray_show_window(icon, item):
    """Tray menu action: Show/focus the main window."""
    if _root_alive:
        def _show():
            root.deiconify()
            root.lift()
            root.focus_force()
        _ui_queue.append(_show)


def _get_autorun_checked(label):
    """Return a function that checks if the current autorun label matches."""
    def _check(item):
        return _cached_autorun_label == label
    return _check


def _set_autorun(label):
    """Return a callback that sets the autorun interval from the tray menu."""
    def _cb(icon, item):
        if _root_alive:
            def _apply():
                autorun_interval_var.set(label)
                _on_autorun_change()
            _ui_queue.append(_apply)
    return _cb


def _build_tray_menu():
    """Build the pystray context menu."""
    autorun_items = []
    for label in AUTORUN_LABELS:
        autorun_items.append(
            pystray.MenuItem(label, _set_autorun(label), checked=_get_autorun_checked(label), radio=True)
        )
    return pystray.Menu(
        pystray.MenuItem("Show Window", _tray_show_window, default=True),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("Sync Subbed", _tray_sync_subbed),
        pystray.MenuItem("Auto-Sync", pystray.Menu(*autorun_items)),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("Quit", _tray_quit),
    )


def _setup_tray_icon():
    """Initialize and start the system tray icon."""
    global _tray_icon, _tray_base_img, _tray_spin_frames, _tray_spin_frames_red
    if not HAS_TRAY:
        return
    try:
        # Load the base icon
        _ico_path = os.path.join(RESOURCE_PATH, "icon.ico")
        if os.path.exists(_ico_path):
            _tray_base_img = Image.open(_ico_path)
            # Ensure a reasonable size for tray (typically 64x64 works well)
            _tray_base_img = _tray_base_img.resize((64, 64), Image.LANCZOS if hasattr(Image, 'LANCZOS') else Image.BICUBIC)
        else:
            # Fallback: create a simple colored icon
            _tray_base_img = Image.new("RGBA", (64, 64), (30, 30, 35, 255))
            d = ImageDraw.Draw(_tray_base_img)
            d.rounded_rectangle([8, 8, 56, 56], radius=8, fill=(100, 180, 255, 255))

        # Pre-generate spin frames (blue for sync, red for transcription)
        _tray_spin_frames = _generate_spin_frames(_tray_base_img)
        _tray_spin_frames_red = _generate_flash_frames(_tray_base_img)

        _tray_icon = pystray.Icon(
            "YT Archiver",
            _tray_base_img,
            "YT Archiver — Idle",
            menu=_build_tray_menu()
        )

        # Run pystray in a background daemon thread
        _tray_thread = threading.Thread(target=_tray_icon.run, daemon=True)
        _tray_thread.start()
    except Exception:
        pass


# Defer tray icon setup until after mainloop starts (needs AUTORUN_LABELS which are defined later)
def _deferred_tray_setup():
    if HAS_TRAY:
        threading.Thread(target=_setup_tray_icon, daemon=True).start()

# ─── End system tray icon ────────────────────────────────────────────

window_width = config.get("window_w", 900)
window_height = config.get("window_h", 800)
root.update_idletasks()
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()
_saved_x = config.get("window_x")
_saved_y = config.get("window_y")
if _saved_x is not None and _saved_y is not None:
    # Clamp to visible area (in case monitor layout changed)
    x = max(0, min(_saved_x, screen_width - 100))
    y = max(0, min(_saved_y, screen_height - 100))
else:
    x = (screen_width // 2) - (window_width // 2)
    y = (screen_height // 2) - (window_height // 2)
root.geometry(f"{window_width}x{window_height}+{x}+{y}")

C_BG = "#0f1012"
C_SURFACE = "#161719"
C_RAISED = "#1c1e21"
C_INPUT = "#101214"
C_BORDER = "#2a2c30"
C_BORDER_LT = "#38393d"

C_TEXT = "#dde1e8"
C_DIM = "#4a4f5a"
C_ACCENT = "#a0aabb"

C_BTN = "#252729"
C_BTN_HVR = "#2e3035"
C_PRIMARY = "#3a3d42"
C_PRIMARY_H = "#454850"
C_SYNC = "#145c40"
C_SYNC_H = "#187a54"
C_CANCEL = "#6b1a1a"
C_WARN = "#6b3a10"

C_LOG_BG = "#0a0b0d"
C_LOG_TXT = "#7a8494"
C_LOG_GREEN = "#3dd68c"
C_LOG_BLUE = "#6cb4ee"
C_LOG_DIM = "#272a2f"
C_LOG_RED = "#ff6b6b"
C_LOG_HEAD = "#a0aabb"
C_LOG_SUM = "#f5a623"

root.configure(bg=C_BG)

def _apply_dark_title_bar(win):
    """Apply dark title bar to a Tk or Toplevel window on Windows."""
    if os.name != "nt":
        return
    try:
        from ctypes import windll, byref, c_int
        hwnd = windll.user32.GetParent(win.winfo_id())
        windll.dwmapi.DwmSetWindowAttribute(hwnd, 20, byref(c_int(1)), 4)
        windll.dwmapi.DwmSetWindowAttribute(hwnd, 35, byref(c_int(0x00120f0f)), 4)
    except Exception:
        pass

_apply_dark_title_bar(root)


def _dark_askquestion(title, message, yes_text="Yes", no_text="No"):
    """Show a dark-mode yes/no dialog on the main thread.

    Returns True if Yes was clicked, False otherwise.
    Must be called from the main (Tk) thread; uses root.wait_window() so the
    Tk event loop keeps running while the dialog is open.
    """
    _result = [None]

    _dlg = tk.Toplevel(root)
    _dlg.title(title)
    _dlg.configure(bg=C_BG)
    _dlg.resizable(False, False)
    _dlg.transient(root)
    _dlg.grab_set()
    _dlg.update_idletasks()
    _apply_dark_title_bar(_dlg)

    def _dismiss(val):
        _result[0] = val
        try:
            _dlg.destroy()
        except Exception:
            pass

    _dlg.protocol("WM_DELETE_WINDOW", lambda: _dismiss(False))

    tk.Label(_dlg, text=message, bg=C_BG, fg=C_TEXT,
             font=("Segoe UI", 10), justify="left",
             wraplength=420).pack(fill="x", padx=20, pady=(14, 4))

    btn_row = tk.Frame(_dlg, bg=C_BG)
    btn_row.pack(padx=20, pady=(6, 14))
    tk.Button(btn_row, text=yes_text, bg="#3a6a3a", fg="#cccccc",
              relief="flat", font=("Segoe UI", 9, "bold"),
              cursor="hand2", command=lambda: _dismiss(True),
              width=10).pack(side="left", padx=(0, 8))
    tk.Button(btn_row, text=no_text, bg=C_BTN, fg=C_TEXT,
              relief="flat", font=("Segoe UI", 9),
              cursor="hand2", command=lambda: _dismiss(False),
              width=10).pack(side="left")

    _dlg.update_idletasks()
    _rx = root.winfo_rootx() + root.winfo_width() // 2
    _ry = root.winfo_rooty() + root.winfo_height() // 2
    _dlg.geometry(f"+{_rx - _dlg.winfo_width() // 2}+{_ry - _dlg.winfo_height() // 2}")

    root.wait_window(_dlg)
    return bool(_result[0])


def _entry(parent, maxlen=500, **kw):
    kw.setdefault("bg", C_INPUT)
    kw.setdefault("fg", C_TEXT)
    kw.setdefault("insertbackground", C_TEXT)
    kw.setdefault("relief", "flat")
    kw.setdefault("bd", 0)
    kw.setdefault("highlightthickness", 1)
    kw.setdefault("highlightbackground", C_BORDER_LT)
    kw.setdefault("highlightcolor", C_ACCENT)
    kw.setdefault("font", ("Segoe UI", 9))
    e = tk.Entry(parent, **kw)
    # Only add maxlen validation if caller didn't provide their own validator
    if maxlen and maxlen > 0 and 'validate' not in kw and 'validatecommand' not in kw:
        _vcmd = (e.register(lambda new_val, _ml=maxlen: len(new_val) <= _ml), '%P')
        e.config(validate="key", validatecommand=_vcmd)
    return e


def _placeholder(entry, text):
    _active = [True]

    def _show():
        if not entry.get():
            _active[0] = True
            entry.config(fg=C_DIM)
            entry.insert(0, text)

    entry.after(10, _show)

    def _on_focus_in(e):
        if _active[0]:
            _active[0] = False
            entry.delete(0, "end")
            entry.config(fg=C_TEXT)

    def _on_focus_out(e):
        content = entry.get()
        if not content:
            _active[0] = True
            entry.config(fg=C_DIM)
            entry.insert(0, text)
        elif _active[0] and content == text:
            # Placeholder is still showing — keep it dimmed
            entry.config(fg=C_DIM)
        else:
            _active[0] = False
            entry.config(fg=C_TEXT)

    entry._ph_text = text
    entry._ph_active = _active

    entry.bind("<FocusIn>", _on_focus_in)
    entry.bind("<FocusOut>", _on_focus_out)


def _real_get(entry):
    if hasattr(entry, "_ph_active") and entry._ph_active[0]:
        if entry.get() == entry._ph_text:
            return ""
        else:
            entry._ph_active[0] = False
            entry.config(fg=C_TEXT)
    return entry.get()


def _digits_only(P):
    """Validate command: only allow empty string or digits."""
    return P == "" or P.isdigit()


def _parse_duration(raw):
    """Strip non-digit chars and return int. Returns 0 for empty/invalid."""
    if not raw:
        return 0
    digits = re.sub(r'\D', '', str(raw).strip())
    return int(digits) if digits else 0


def _combo(parent, **kw):
    try:
        root.option_add("*TCombobox*Listbox.background", C_INPUT, "interactive")
        root.option_add("*TCombobox*Listbox.foreground", C_TEXT, "interactive")
        root.option_add("*TCombobox*Listbox.selectBackground", C_BTN_HVR, "interactive")
        root.option_add("*TCombobox*Listbox.selectForeground", C_TEXT, "interactive")
    except Exception:
        pass
    return ttk.Combobox(parent, **kw)


style = ttk.Style()
style.theme_use("clam")

style.configure("TNotebook", background=C_BG, borderwidth=0)
style.configure("TNotebook.Tab",
                background=C_BG, foreground=C_ACCENT,
                padding=[16, 7], font=("Segoe UI", 9))
style.map("TNotebook.Tab",
          background=[("selected", C_SURFACE)],
          foreground=[("selected", C_TEXT)],
          font=[("selected", ("Segoe UI", 9, "bold"))])

style.configure("TFrame", background=C_SURFACE)
style.configure("Raised.TFrame", background=C_RAISED)
style.configure("TLabel", background=C_SURFACE, foreground=C_TEXT, font=("Segoe UI", 9))
style.configure("Dim.TLabel", background=C_SURFACE, foreground=C_DIM, font=("Segoe UI", 9))
style.configure("Green.TLabel", background=C_SURFACE, foreground=C_LOG_GREEN, font=("Segoe UI", 9, "bold"))
style.configure("Type.TLabel", background=C_SURFACE, foreground=C_ACCENT, font=("Segoe UI", 9, "italic"))
style.configure("Error.TLabel", background=C_RAISED, foreground=C_LOG_RED, font=("Segoe UI", 8, "italic"))
style.configure("Header.TLabel", background=C_BG, foreground=C_TEXT,
                font=("Segoe UI Semibold", 11))

style.configure("TLabelframe", background=C_RAISED, relief="groove",
                bordercolor=C_BORDER_LT, borderwidth=1,
                lightcolor=C_BORDER_LT, darkcolor=C_BORDER)
style.configure("TLabelframe.Label", background=C_RAISED,
                foreground=C_ACCENT, font=("Segoe UI", 8, "bold"))

_bp = [10, 5]
style.configure("TButton", background=C_BTN, foreground=C_TEXT, padding=_bp, relief="flat", font=("Segoe UI", 9))
style.map("TButton", background=[("active", C_BTN_HVR), ("disabled", C_BORDER)])
style.configure("Accent.TButton", background=C_PRIMARY, foreground=C_TEXT, padding=_bp, relief="flat",
                font=("Segoe UI", 9, "bold"))
style.map("Accent.TButton", background=[("active", C_PRIMARY_H), ("disabled", C_BORDER)],
          foreground=[("disabled", C_DIM)])
style.configure("Sync.TButton", background=C_SYNC, foreground="#ffffff", padding=_bp, relief="flat",
                font=("Segoe UI Emoji", 9, "bold"))
style.map("Sync.TButton", background=[("active", C_SYNC_H), ("disabled", C_BORDER)])
style.configure("Cancel.TButton", background=C_CANCEL, foreground="#ffffff", padding=_bp, relief="flat",
                font=("Segoe UI Emoji", 9))
style.map("Cancel.TButton", background=[("active", "#7a2020"), ("disabled", C_BORDER)])
style.configure("Pause.TButton", background="#2a4a6b", foreground="#ffffff", padding=_bp, relief="flat",
                font=("Segoe UI Emoji", 9))
style.map("Pause.TButton", background=[("active", "#3a5e84"), ("disabled", C_BORDER)])
style.configure("Warn.TButton", background=C_WARN, foreground="#ffffff", padding=_bp, relief="flat",
                font=("Segoe UI", 9))
style.map("Warn.TButton", background=[("active", "#7a4412"), ("disabled", C_BORDER)])

# Emoji-only icon buttons — use Segoe UI Emoji for better glyph rendering on Windows
style.configure("Emoji.TButton", background=C_BTN, foreground=C_TEXT, padding=[2, 2], relief="flat",
                font=("Segoe UI Emoji", 11))
style.map("Emoji.TButton", background=[("active", C_BTN_HVR), ("disabled", C_BORDER)])

style.configure("TEntry",
                fieldbackground=C_INPUT, foreground=C_TEXT, insertcolor=C_TEXT,
                bordercolor=C_BORDER_LT, lightcolor=C_BORDER_LT, darkcolor=C_INPUT,
                relief="flat", padding=[4, 3])
style.map("TEntry",
          bordercolor=[("focus", C_ACCENT)],
          lightcolor=[("focus", C_ACCENT)])

style.configure("TCombobox",
                fieldbackground=C_INPUT, foreground=C_TEXT,
                selectbackground=C_INPUT, selectforeground=C_TEXT,
                bordercolor=C_BORDER_LT, lightcolor=C_INPUT, darkcolor=C_INPUT,
                focuscolor=C_INPUT,
                relief="flat", padding=[4, 3], arrowcolor=C_ACCENT)
style.map("TCombobox",
          fieldbackground=[("readonly", C_INPUT), ("focus", C_INPUT)],
          foreground=[("readonly", C_TEXT), ("focus", C_TEXT)],
          selectbackground=[("readonly", C_INPUT), ("focus", C_INPUT)],
          selectforeground=[("readonly", C_TEXT), ("focus", C_TEXT)],
          bordercolor=[("focus", C_ACCENT)],
          lightcolor=[("focus", C_ACCENT), ("!focus", C_BORDER_LT)],
          darkcolor=[("focus", C_INPUT), ("!focus", C_INPUT)],
          focuscolor=[("readonly", C_INPUT), ("focus", C_INPUT)])

style.layout("TCombobox", [
    ('Combobox.field', {'sticky': 'nswe', 'children': [
        ('Combobox.downarrow', {'side': 'right', 'sticky': 'ns'}),
        ('Combobox.padding', {'expand': '1', 'sticky': 'nswe', 'children': [
            ('Combobox.textarea', {'sticky': 'nswe'})
        ]})
    ]})
])

style.configure("TRadiobutton", background=C_RAISED, foreground=C_TEXT, font=("Segoe UI", 9))
style.map("TRadiobutton", background=[("active", C_RAISED)])
style.configure("TCheckbutton", background=C_RAISED, foreground=C_TEXT, font=("Segoe UI", 9))
style.map("TCheckbutton", background=[("active", C_RAISED)], foreground=[("active", C_TEXT)])
style.configure("TSeparator", background=C_BORDER)
style.configure("TPanedwindow", background=C_SURFACE)
style.configure("TPanedwindow.Sash", sashthickness=6, gripcount=0)
style.map("TPanedwindow.Sash", background=[("active", C_BORDER_LT), ("!active", C_BORDER)])
style.configure("TScrollbar", background=C_BTN, troughcolor=C_INPUT,
                bordercolor=C_INPUT, arrowcolor=C_DIM)
style.map("TScrollbar", background=[("active", C_BTN_HVR)])

style.configure("Recent.Treeview",
                background=C_LOG_BG, foreground=C_TEXT, fieldbackground=C_LOG_BG,
                borderwidth=0, rowheight=20, font=("Consolas", 9))
style.configure("Recent.Treeview.Heading",
                background=C_RAISED, foreground=C_ACCENT, relief="flat",
                font=("Segoe UI", 8, "bold"), padding=[6, 4])
style.map("Recent.Treeview",
          background=[("selected", C_BTN_HVR)],
          foreground=[("selected", C_TEXT)])
style.map("Recent.Treeview.Heading",
          background=[("active", C_BTN)])

header_strip = tk.Frame(root, bg=C_BG, height=42)
header_strip.pack(fill="x", side="top")
header_strip.pack_propagate(False)
tk.Label(header_strip, text="YT ARCHIVER", bg=C_BG, fg=C_TEXT,
         font=("Segoe UI Semibold", 13), anchor="w").pack(side="left", padx=16, pady=10)
tk.Label(header_strip, text="v16.9 - 03.14.26 2:55pm", bg=C_BG, fg=C_DIM,
         font=("Segoe UI", 8), anchor="w").pack(side="left", pady=14)
tk.Frame(root, bg=C_BORDER_LT, height=1).pack(fill="x", side="top")

notebook = ttk.Notebook(root)
notebook.pack(fill="both", expand=True, padx=0, pady=0)

tab_download = ttk.Frame(notebook)
notebook.add(tab_download, text="  Download  ")
tab_download.columnconfigure(0, weight=1)
tab_download.rowconfigure(7, weight=1)

url_input_frame = ttk.Frame(tab_download)
url_input_frame.grid(row=0, column=0, sticky="ew", padx=14, pady=(14, 4))
url_input_frame.columnconfigure(1, weight=1)

ttk.Label(url_input_frame, text="Video URL:").grid(row=0, column=0, sticky="w", padx=(0, 8))
url_var = tk.StringVar()
url_entry = _entry(url_input_frame, textvariable=url_var, width=65)
url_entry.grid(row=0, column=1, sticky="ew")
_placeholder(url_entry, "https://www.youtube.com/watch?v=...")

url_type_label = ttk.Label(url_input_frame, text="", style="Type.TLabel")
url_type_label.grid(row=0, column=2, sticky="w", padx=(10, 0))

# M.T. button removed — manual transcription is now accessed via the GPU Tasks (💻) popup

channel_panel = ttk.Frame(tab_download)
channel_panel.columnconfigure(0, weight=1)

chan_dd_frame = ttk.Frame(channel_panel)
chan_dd_frame.grid(row=0, column=0, sticky="ew", padx=0, pady=(4, 6))
chan_dd_frame.columnconfigure(1, weight=1)

ttk.Label(chan_dd_frame, text="Saved channels:").grid(row=0, column=0, sticky="w", padx=(0, 8))
chan_var = tk.StringVar()
chan_dropdown = _combo(chan_dd_frame, textvariable=chan_var, state="readonly", width=44)
chan_dropdown.grid(row=0, column=1, sticky="w")

chan_opts = ttk.LabelFrame(channel_panel, text="Channel options")
chan_opts.grid(row=1, column=0, sticky="ew", pady=(0, 4))
chan_opts.columnconfigure(3, weight=1)

ttk.Label(chan_opts, text="Resolution:").grid(row=0, column=0, sticky="w", padx=(8, 4), pady=6)
ch_res_var = tk.StringVar(value="720")
_combo(chan_opts, textvariable=ch_res_var, values=RESOLUTION_OPTIONS, state="readonly", width=8).grid(
    row=0, column=1, sticky="w", padx=(0, 20))

ttk.Label(chan_opts, text="Duration Limit:", style="Dim.TLabel").grid(row=0, column=4, columnspan=4, sticky="s",
                                                                      pady=(4, 0))
ttk.Label(chan_opts, text="Min (m)").grid(row=1, column=4, sticky="e", padx=(4, 2))
_vcmd_digits = (root.register(_digits_only), '%P')

ch_dur_var = tk.StringVar(value="")
_entry(chan_opts, textvariable=ch_dur_var, width=6,
       validate="key", validatecommand=_vcmd_digits).grid(row=1, column=5, sticky="w", padx=(0, 4))

ttk.Label(chan_opts, text="Max (m)").grid(row=1, column=6, sticky="e", padx=(4, 2))
ch_maxdur_var = tk.StringVar(value="")
_entry(chan_opts, textvariable=ch_maxdur_var, width=6,
       validate="key", validatecommand=_vcmd_digits).grid(row=1, column=7, sticky="w", padx=(0, 8))
ttk.Label(chan_opts, text="(Blank = Off)", style="Dim.TLabel").grid(row=2, column=4, columnspan=4, sticky="n",
                                                                    pady=(0, 4))

mode_var = tk.StringVar(value="full")
ch_all_var = tk.BooleanVar(value=False)
ch_fromdate_var = tk.BooleanVar(value=False)

mode_cb_frame = ttk.Frame(chan_opts, style="Raised.TFrame")
mode_cb_frame.grid(row=1, column=0, columnspan=4, sticky="w", padx=(4, 8), pady=(0, 4))


def _ch_all_toggled():
    if ch_all_var.get():
        ch_fromdate_var.set(False)
        mode_var.set("full")
    else:
        mode_var.set("sub")


def _ch_fromdate_toggled():
    if ch_fromdate_var.get():
        ch_all_var.set(False)
        mode_var.set("date")
    else:
        mode_var.set("sub")


ttk.Checkbutton(mode_cb_frame, text="All  —  download entire channel",
                variable=ch_all_var, command=_ch_all_toggled).pack(anchor="w")
ttk.Checkbutton(mode_cb_frame, text="From date  —  download everything after a date",
                variable=ch_fromdate_var, command=_ch_fromdate_toggled).pack(anchor="w")
ttk.Label(mode_cb_frame, text="Neither checked = subscribe only (new uploads)",
          style="Dim.TLabel").pack(anchor="w", padx=(20, 0), pady=(2, 0))

folder_row = ttk.Frame(chan_opts, style="Raised.TFrame")
folder_row.grid(row=3, column=0, columnspan=9, sticky="ew", padx=(4, 8), pady=(2, 4))
folder_row.columnconfigure(3, weight=1)

preview_btn = ttk.Button(folder_row, text="🔍 Preview folder name")
preview_btn.grid(row=0, column=0, padx=(0, 8))

folder_preview_var = tk.StringVar(value="")
ttk.Label(folder_row, textvariable=folder_preview_var, style="Dim.TLabel").grid(row=0, column=1, sticky="w",
                                                                                padx=(0, 12))

ttk.Label(folder_row, text="Override:").grid(row=0, column=2, sticky="e", padx=(0, 4))
folder_override_var = tk.StringVar(value="")
folder_override_entry = _entry(folder_row, textvariable=folder_override_var)
folder_override_entry.grid(row=0, column=3, sticky="ew")

video_panel = ttk.Frame(tab_download)
video_panel.columnconfigure(1, weight=1)

vid_opts = ttk.LabelFrame(video_panel, text="Video options")
vid_opts.grid(row=0, column=0, columnspan=3, sticky="ew", pady=(4, 4))
vid_opts.columnconfigure(1, weight=1)

ttk.Label(vid_opts, text="Save to:").grid(row=0, column=0, sticky="w", padx=(8, 4), pady=6)
videodir_var = tk.StringVar(value=config.get("video_out_dir", DEFAULT_CONFIG["video_out_dir"]))
_entry(vid_opts, textvariable=videodir_var, width=48).grid(row=0, column=1, sticky="ew", padx=(0, 4))


def browse_vid_out():
    current_dir = videodir_var.get().strip()
    init_dir = current_dir if os.path.exists(current_dir) else BASE_DIR
    d = filedialog.askdirectory(initialdir=init_dir)
    if d:
        videodir_var.set(d)
        with config_lock:
            config["video_out_dir"] = d
        save_config(config)


ttk.Button(vid_opts, text="Browse", command=browse_vid_out).grid(row=0, column=2, padx=(0, 8))

ttk.Label(vid_opts, text="Resolution:").grid(row=1, column=0, sticky="w", padx=(8, 4), pady=(0, 6))
vid_res_var = tk.StringVar(value="1080")
_combo(vid_opts, textvariable=vid_res_var, values=RESOLUTION_OPTIONS, state="readonly", width=8).grid(
    row=1, column=1, sticky="w", padx=(0, 20))

vid_date_file_var = tk.BooleanVar(value=config.get("vid_date_file", True))


def _save_vid_date_file():
    with config_lock:
        config["vid_date_file"] = vid_date_file_var.get()
    save_config(config)


ttk.Checkbutton(vid_opts, text="Date file to YT upload date", variable=vid_date_file_var,
                command=_save_vid_date_file).grid(
    row=2, column=0, columnspan=4, sticky="w", padx=(8, 4), pady=(0, 2))

vid_add_date_var = tk.BooleanVar(value=config.get("vid_add_date", False))


def _save_vid_add_date():
    with config_lock:
        config["vid_add_date"] = vid_add_date_var.get()
    save_config(config)


ttk.Checkbutton(vid_opts, text="Add date to filename", variable=vid_add_date_var, command=_save_vid_add_date).grid(
    row=3, column=0, columnspan=4, sticky="w", padx=(8, 4), pady=(0, 4))

vid_use_yt_title_var = tk.BooleanVar(value=True)
vid_custom_name_var = tk.StringVar(value="")

vid_name_row = ttk.Frame(vid_opts)
vid_name_row.grid(row=4, column=0, columnspan=3, sticky="ew", padx=(8, 8), pady=(0, 6))
vid_name_row.columnconfigure(1, weight=1)

vid_use_yt_title_cb = ttk.Checkbutton(vid_name_row, text="Use YT title as filename",
                                      variable=vid_use_yt_title_var)
vid_use_yt_title_cb.grid(row=0, column=0, sticky="w", padx=(0, 12))

vid_custom_name_entry = _entry(vid_name_row, textvariable=vid_custom_name_var, width=36,
                               disabledbackground=C_INPUT, disabledforeground=C_DIM)
vid_custom_name_entry.grid(row=0, column=1, sticky="ew")
_placeholder(vid_custom_name_entry, "File Title")


def _custom_name_focus_in(e):
    if vid_custom_name_entry._ph_active[0]:
        vid_custom_name_entry.config(fg=C_DIM)
        vid_custom_name_entry.after(0, lambda: vid_custom_name_entry.icursor(0))


def _custom_name_click(e):
    if vid_custom_name_entry._ph_active[0]:
        vid_custom_name_entry.after(0, lambda: vid_custom_name_entry.icursor(0))
        return "break"


def _custom_name_key(e):
    if vid_custom_name_entry._ph_active[0]:
        vid_custom_name_entry._ph_active[0] = False
        vid_custom_name_entry.delete(0, "end")
        vid_custom_name_entry.config(fg=C_TEXT)


def _custom_name_key_release(e):
    if not vid_custom_name_entry._ph_active[0] and not vid_custom_name_entry.get():
        vid_custom_name_entry._ph_active[0] = True
        vid_custom_name_entry.config(fg=C_DIM)
        vid_custom_name_entry.insert(0, "File Title")
        vid_custom_name_entry.icursor(0)


def _custom_name_focus_out(e):
    if not vid_custom_name_entry._ph_active[0] and not vid_custom_name_entry.get():
        vid_custom_name_entry._ph_active[0] = True
        vid_custom_name_entry.config(fg=C_DIM)
        vid_custom_name_entry.insert(0, "File Title")


vid_custom_name_entry.bind("<FocusIn>", _custom_name_focus_in)
vid_custom_name_entry.bind("<FocusOut>", _custom_name_focus_out)
vid_custom_name_entry.bind("<Button-1>", _custom_name_click)
vid_custom_name_entry.bind("<ButtonRelease-1>", _custom_name_click)
vid_custom_name_entry.bind("<Key>", _custom_name_key)
vid_custom_name_entry.bind("<KeyRelease>", _custom_name_key_release)


def _toggle_custom_name(*_):
    if vid_use_yt_title_var.get():
        vid_custom_name_entry.config(state="disabled")
        if hasattr(vid_custom_name_entry, "_ph_active") and vid_custom_name_entry._ph_active[0]:
            vid_custom_name_entry.delete(0, "end")
            vid_custom_name_entry._ph_active[0] = False
        vid_custom_name_var.set("")
    else:
        vid_custom_name_entry.config(state="normal")
        if not _real_get(vid_custom_name_entry):
            vid_custom_name_entry.config(fg=C_DIM)
            vid_custom_name_entry.delete(0, "end")
            vid_custom_name_entry.insert(0, "File Title")
            vid_custom_name_entry._ph_active[0] = True
        vid_custom_name_entry.focus_set()


vid_use_yt_title_var.trace_add("write", _toggle_custom_name)
_toggle_custom_name()

unknown_panel = ttk.Frame(tab_download)

channel_nudge_panel = ttk.Frame(tab_download)
ttk.Label(channel_nudge_panel, text="📺  Channel URL detected.", style="Type.TLabel").pack(side="left", padx=(4, 8))


def _go_to_add_channel():
    new_url_var.set(url_var.get().strip())
    url_var.set("")
    notebook.select(tab_settings)
    _new_url_entry.event_generate("<FocusOut>")
    root.after(50, new_name_entry.focus_set)


ttk.Button(channel_nudge_panel, text="→ Add to saved channels",
           command=_go_to_add_channel).pack(side="left")

log_paned = ttk.PanedWindow(tab_download, orient=tk.VERTICAL)
log_paned.grid(row=7, column=0, sticky="nsew", padx=14, pady=(0, 14))
tab_download.rowconfigure(7, weight=1)

# Autorun history added FIRST (top) — weight=0 so it stays compact
autorun_history_frame = ttk.Frame(log_paned)
log_paned.add(autorun_history_frame, weight=0)
autorun_history_frame.columnconfigure(0, weight=1)
autorun_history_frame.rowconfigure(0, weight=1)

hist_scroll = ttk.Scrollbar(autorun_history_frame, orient="vertical")
hist_scroll.grid(row=0, column=1, sticky="ns")

autorun_history_listbox = tk.Listbox(
    autorun_history_frame, bg=C_INPUT, fg=C_DIM,
    selectbackground=C_INPUT, selectforeground=C_DIM,
    font=("Consolas", 9), height=2, relief="flat", bd=0, highlightthickness=0,
    activestyle="none", yscrollcommand=hist_scroll.set)
autorun_history_listbox.grid(row=0, column=0, sticky="nsew")
hist_scroll.config(command=autorun_history_listbox.yview)

# Prevent selection (visual-only list)
def _deselect_history(event=None):
    autorun_history_listbox.selection_clear(0, tk.END)

autorun_history_listbox.bind("<<ListboxSelect>>", _deselect_history)
autorun_history_listbox.bind("<Button-1>", lambda e: "break")

# Auto-hide scrollbar helper: only show scrollbar when content overflows
_log_scroll_freeze = False  # suppress scrollbar grid toggling during batch log ops

def _auto_scrollbar(scrollbar, first, last):
    if _log_scroll_freeze:
        scrollbar.set(first, last)
        return
    if float(first) <= 0.0 and float(last) >= 1.0:
        scrollbar.grid_remove()
    else:
        scrollbar.grid()
    scrollbar.set(first, last)

# Main log frame added SECOND (bottom) — weight=1 keeps it compact initially
log_frame = ttk.Frame(log_paned)
log_paned.add(log_frame, weight=1)
log_frame.columnconfigure(0, weight=1)
log_frame.rowconfigure(0, weight=1)

log_scroll = ttk.Scrollbar(log_frame, orient="vertical")
log_scroll.grid(row=0, column=1, sticky="ns")
log_scroll.grid_remove()  # hidden initially

log_box = tk.Text(log_frame, state="disabled",
                  bg=C_LOG_BG, fg=C_LOG_TXT, font=("Consolas", 9),
                  relief="flat", bd=0, highlightthickness=0, padx=8, pady=6,
                  selectbackground=C_BTN, selectforeground=C_TEXT, wrap="none",
                  yscrollcommand=lambda f, l: _auto_scrollbar(log_scroll, f, l))
log_box.grid(row=0, column=0, sticky="nsew")
log_scroll.config(command=log_box.yview)

# --- Detect user scroll to suppress auto-scroll-to-bottom ---
def _on_log_user_scroll(event=None):
    global _log_user_scrolled
    _log_user_scrolled = True

log_box.bind("<MouseWheel>", _on_log_user_scroll, add="+")       # Windows scroll
log_box.bind("<Button-4>", _on_log_user_scroll, add="+")         # Linux scroll up
log_box.bind("<Button-5>", _on_log_user_scroll, add="+")         # Linux scroll down
log_box.bind("<ButtonPress-1>", _on_log_user_scroll, add="+")    # click in log
log_scroll.bind("<ButtonPress-1>", _on_log_user_scroll, add="+") # scrollbar drag

log_box.tag_configure("green", foreground=C_LOG_GREEN)
log_box.tag_configure("dim", foreground=C_LOG_DIM)
log_box.tag_configure("red", foreground=C_LOG_RED)
log_box.tag_configure("header", foreground=C_LOG_HEAD, font=("Consolas", 9, "bold"))
log_box.tag_configure("summary", foreground=C_LOG_SUM, font=("Consolas", 9, "italic"))
log_box.tag_configure("livestream", foreground="#f5a023", font=("Consolas", 9, "bold"))
log_box.tag_configure("filterskip", foreground=C_LOG_SUM)
log_box.tag_configure("scanline", foreground=C_TEXT)
log_box.tag_configure("dlprogress", foreground=C_TEXT)
log_box.tag_configure("dlprogress_pct", foreground=C_LOG_GREEN)
log_box.tag_configure("simplestatus", foreground=C_LOG_HEAD, font=("Consolas", 9, "bold"))
log_box.tag_configure("simplestatus_green", foreground=C_LOG_GREEN)
log_box.tag_configure("simpleline", foreground=C_TEXT, font=("Consolas", 9))
log_box.tag_configure("simpleline_green", foreground=C_LOG_GREEN, font=("Consolas", 9))
log_box.tag_configure("simpleline_blue", foreground=C_LOG_BLUE, font=("Consolas", 9))
log_box.tag_configure("transcribe_using", foreground=C_LOG_BLUE, font=("Consolas", 9))
log_box.tag_configure("simpledownload", foreground=C_LOG_GREEN)
log_box.tag_configure("pauselog", foreground=C_LOG_HEAD)
log_box.tag_configure("pausestatus", foreground=C_LOG_HEAD)
log_box.tag_configure("whisper_progress", foreground=C_LOG_BLUE, font=("Consolas", 9))
log_box.tag_configure("whisper_pct", foreground=C_LOG_GREEN, font=("Consolas", 9, "bold"))
log_box.tag_configure("whisper_dots", foreground=C_LOG_BLUE, font=("Consolas", 9))
log_box.tag_configure("encode_progress", foreground=C_LOG_BLUE, font=("Consolas", 9))
log_box.tag_configure("encode_prefix", foreground=C_TEXT, font=("Consolas", 9))
log_box.tag_configure("encode_title", foreground=C_TEXT, font=("Consolas", 9))
log_box.tag_configure("encode_pct", foreground=C_LOG_GREEN, font=("Consolas", 9, "bold"))
log_box.tag_configure("encode_dots", foreground=C_LOG_BLUE, font=("Consolas", 9))
log_box.tag_configure("encode_suffix", foreground=C_LOG_BLUE, font=("Consolas", 9))
# encode_prefix and encode_title must override encode_progress (white over blue)
log_box.tag_raise("encode_prefix", "encode_progress")
log_box.tag_raise("encode_title", "encode_progress")
# simplestatus_green must override simplestatus so SYNCING label stays green
log_box.tag_raise("simplestatus_green", "simplestatus")


# Set initial sash position so the log panel starts with ~3 lines of space
def _set_initial_sash():
    try:
        # Place sash so the autorun history area gets minimal space
        # and the log area starts with roughly 3 lines (~50px)
        h = log_paned.winfo_height()
        if h > 100:
            log_paned.sashpos(0, max(0, h - 55))
    except Exception:
        pass


root.after(150, _set_initial_sash)

log_mode_var = tk.StringVar(value=config.get("log_mode", "Simple"))
_is_simple_mode = log_mode_var.get() == "Simple"

tab_settings = ttk.Frame(notebook)
notebook.add(tab_settings, text="  Subs  ")
tab_settings.columnconfigure(0, weight=1)
tab_settings.columnconfigure(1, weight=1)
tab_settings.rowconfigure(3, weight=1)

ttk.Label(tab_settings, text="Parent Folder:").grid(row=0, column=0, sticky="w", padx=12, pady=(16, 4))
outdir_var = tk.StringVar(value=config.get("output_dir", ""))


def browse_outdir():
    current_dir = outdir_var.get().strip()
    init_dir = current_dir if os.path.exists(current_dir) else BASE_DIR
    d = filedialog.askdirectory(initialdir=init_dir)
    if d:
        outdir_var.set(d)
        with config_lock:
            config["output_dir"] = d
        save_config(config)


def _save_outdir_on_focusout(event=None):
    with config_lock:
        config["output_dir"] = outdir_var.get().strip()
    save_config(config)


_outdir_entry = _entry(tab_settings, textvariable=outdir_var, width=52)
_outdir_entry.grid(row=0, column=1, sticky="ew", padx=(0, 4), pady=(16, 4))
_outdir_entry.bind("<FocusOut>", _save_outdir_on_focusout)

ttk.Button(tab_settings, text="Browse", command=browse_outdir).grid(row=0, column=2, padx=(0, 12), pady=(16, 4))

ttk.Separator(tab_settings, orient="horizontal").grid(row=1, column=0, columnspan=3, sticky="ew", padx=12, pady=10)

ttk.Label(tab_settings, text="Subbed Channels:").grid(row=2, column=0, columnspan=3, sticky="sw", padx=12, pady=(4, 0))
chan_list_frame = ttk.Frame(tab_settings)
chan_list_frame.grid(row=3, column=0, columnspan=3, sticky="nsew", padx=12, pady=(2, 4))
chan_list_frame.columnconfigure(0, weight=1)
chan_list_frame.rowconfigure(0, weight=1)

chan_scrollbar = ttk.Scrollbar(chan_list_frame, orient="vertical")
chan_scrollbar.grid(row=0, column=1, sticky="ns")

settings_chan_tree = ttk.Treeview(chan_list_frame, style="Recent.Treeview",
                                  columns=("folder", "res", "min", "max", "compress", "transcribed", "last_sync", "url"),
                                  show="headings", selectmode="browse",
                                  yscrollcommand=chan_scrollbar.set)
settings_chan_tree.grid(row=0, column=0, sticky="nsew")
chan_scrollbar.config(command=settings_chan_tree.yview)

_CHAN_COL_LABELS = {
    "folder": "Folder",
    "res": "Res",
    "min": "Min",
    "max": "Max",
    "transcribed": "Transcribed",
    "last_sync": "Last Sync",
    "url": "URL"
}
_chan_sort_state = {"col": None, "reverse": False}


def _sort_chan_tree(col, reverse):
    if col == "url": return
    _chan_sort_state["col"] = col
    _chan_sort_state["reverse"] = reverse
    l = [(settings_chan_tree.set(k, col), k) for k in settings_chan_tree.get_children('')]

    if col in ("min", "max"):
        def parse_val(s):
            if not s or s == "—": return 0
            if s.endswith("s"): return int(s[:-1])
            if s.endswith("m"): return int(s[:-1]) * 60
            return 0

        l.sort(key=lambda t: parse_val(t[0]), reverse=reverse)
    elif col == "res":
        def parse_res(s):
            if not s: return 0
            if s.endswith("p"): return int(s[:-1])
            if s == "best": return 9999
            try:
                return int(s)
            except:
                return 0

        l.sort(key=lambda t: parse_res(t[0]), reverse=reverse)
    elif col == "last_sync":
        def get_ts(iid):
            url = settings_chan_tree.set(iid, "url")
            with config_lock:
                for c in config.get("channels", []):
                    if c["url"] == url:
                        ls = c.get("last_sync", "")
                        if not ls: return 0
                        try:
                            return time.mktime(datetime.strptime(ls, "%Y-%m-%d %H:%M").timetuple())
                        except Exception:
                            return 0
            return 0

        l.sort(key=lambda t: get_ts(t[1]), reverse=reverse)
    else:
        l.sort(key=lambda t: t[0].lower(), reverse=reverse)

    for index, (val, k) in enumerate(l):
        settings_chan_tree.move(k, '', index)
        settings_chan_tree.item(k, tags=("odd" if index % 2 else "even",))

    settings_chan_tree.heading(col, command=lambda: _sort_chan_tree(col, not reverse))

    arrow = " ▲" if not reverse else " ▼"
    for c, base_label in _CHAN_COL_LABELS.items():
        if c == "url": continue
        anchor = "w"
        if c == col:
            settings_chan_tree.heading(c, text=base_label + arrow, anchor=anchor)
        else:
            settings_chan_tree.heading(c, text=base_label, anchor=anchor)


settings_chan_tree.heading("folder", text="Folder", anchor="w", command=lambda: _sort_chan_tree("folder", False))
settings_chan_tree.heading("res", text="Res", anchor="w", command=lambda: _sort_chan_tree("res", False))
settings_chan_tree.heading("min", text="Min", anchor="w", command=lambda: _sort_chan_tree("min", False))
settings_chan_tree.heading("max", text="Max", anchor="w", command=lambda: _sort_chan_tree("max", False))
settings_chan_tree.heading("compress", text="Compress", anchor="center")
settings_chan_tree.heading("transcribed", text="Transcribed", anchor="w",
                           command=lambda: _sort_chan_tree("transcribed", False))
settings_chan_tree.heading("last_sync", text="Last Sync", anchor="w",
                           command=lambda: _sort_chan_tree("last_sync", False))
settings_chan_tree.heading("url", text="URL", anchor="w")

settings_chan_tree.column("folder", stretch=False, width=170, anchor="w")
settings_chan_tree.column("res", stretch=False, width=45, anchor="w")
settings_chan_tree.column("min", stretch=False, width=45, anchor="w")
settings_chan_tree.column("max", stretch=False, width=45, anchor="w")
settings_chan_tree.column("compress", stretch=False, width=65, anchor="center")
settings_chan_tree.column("transcribed", stretch=False, width=85, anchor="w")
settings_chan_tree.column("last_sync", stretch=False, width=100, anchor="w")
settings_chan_tree.column("url", stretch=True, minwidth=100, anchor="w")
settings_chan_tree.tag_configure("odd", background="#0c0f14")
settings_chan_tree.tag_configure("even", background=C_LOG_BG)

add_outer = ttk.LabelFrame(tab_settings, text="Add channel")
add_outer.grid(row=4, column=0, columnspan=3, sticky="ew", padx=12, pady=(8, 4))
add_outer.columnconfigure(1, weight=1)
add_outer.columnconfigure(3, weight=2)

ttk.Label(add_outer, text="Folder Name:").grid(row=0, column=0, padx=(8, 4), pady=(6, 2))
new_name_var = tk.StringVar()
new_name_entry = _entry(add_outer, textvariable=new_name_var, width=22)
new_name_entry.grid(row=0, column=1, sticky="ew", padx=(0, 12), pady=(6, 2))
_placeholder(new_name_entry, "Target folder name")
ttk.Label(add_outer, text="Channel URL:").grid(row=0, column=2, padx=(0, 4), pady=(6, 2))
new_url_var = tk.StringVar()
_new_url_entry = _entry(add_outer, textvariable=new_url_var, width=38)
_new_url_entry.grid(row=0, column=3, sticky="ew", padx=(0, 8), pady=(6, 2))
_placeholder(_new_url_entry, "https://www.youtube.com/@handle")

url_error_var = tk.StringVar(value="")
url_error_label = ttk.Label(add_outer, textvariable=url_error_var, style="Error.TLabel")
url_error_label.grid(row=1, column=3, sticky="nw", padx=(0, 8), pady=(0, 4))

ttk.Label(add_outer, text="Resolution:").grid(row=2, column=0, sticky="w", padx=(8, 4), pady=(0, 4))
new_res_var = tk.StringVar(value="720")
_combo(add_outer, textvariable=new_res_var, values=RESOLUTION_OPTIONS, state="readonly", width=8).grid(
    row=2, column=1, sticky="w", padx=(0, 12))

ttk.Label(add_outer, text="Duration Limit:", style="Dim.TLabel").grid(row=1, column=4, columnspan=4, sticky="s",
                                                                      pady=(4, 0))
ttk.Label(add_outer, text="Min (m)").grid(row=2, column=4, sticky="e", padx=(4, 2))
new_dur_var = tk.StringVar(value="")
_entry(add_outer, textvariable=new_dur_var, width=6,
       validate="key", validatecommand=_vcmd_digits).grid(row=2, column=5, sticky="w", padx=(0, 4))

ttk.Label(add_outer, text="Max (m)").grid(row=2, column=6, sticky="e", padx=(4, 2))
new_maxdur_var = tk.StringVar(value="")
_entry(add_outer, textvariable=new_maxdur_var, width=6,
       validate="key", validatecommand=_vcmd_digits).grid(row=2, column=7, sticky="w", padx=(0, 8))
ttk.Label(add_outer, text="(Blank = Off)", style="Dim.TLabel").grid(row=3, column=4, columnspan=4, sticky="n",
                                                                    pady=(0, 4))

ttk.Label(add_outer, text="Range:").grid(row=3, column=0, sticky="nw", padx=(8, 4), pady=(4, 4))
new_mode_var = tk.StringVar(value="sub")
new_all_var = tk.BooleanVar(value=False)
new_fromdate_var = tk.BooleanVar(value=False)
date_year_var = tk.StringVar(value="")
date_month_var = tk.StringVar(value="")
date_day_var = tk.StringVar(value="")
mode_add_frame = ttk.Frame(add_outer, style="Raised.TFrame")
mode_add_frame.grid(row=3, column=1, columnspan=3, sticky="w", pady=(0, 4))


def _new_all_toggled():
    if new_all_var.get():
        new_fromdate_var.set(False)
        new_mode_var.set("full")
    else:
        new_mode_var.set("sub")
    _toggle_date_entry()


def _new_fromdate_toggled():
    if new_fromdate_var.get():
        new_all_var.set(False)
        new_mode_var.set("date")
    else:
        new_mode_var.set("sub")
    _toggle_date_entry()


ttk.Checkbutton(mode_add_frame, text="All  —  download entire channel",
                variable=new_all_var, command=_new_all_toggled).pack(anchor="w")
ttk.Checkbutton(mode_add_frame, text="From date  —  download everything after a date",
                variable=new_fromdate_var, command=_new_fromdate_toggled).pack(anchor="w")
ttk.Label(mode_add_frame, text="Neither checked = subscribe only (new uploads)",
          style="Dim.TLabel").pack(anchor="w", padx=(20, 0), pady=(2, 0))

_date_row = ttk.Frame(mode_add_frame, style="Raised.TFrame")
ttk.Label(_date_row, text="Start date:", style="Dim.TLabel").pack(side="left", padx=(18, 6))
ttk.Label(_date_row, text="Year", style="Dim.TLabel").pack(side="left", padx=(0, 3))
_date_year_entry = _entry(_date_row, textvariable=date_year_var, width=6)
_date_year_entry.pack(side="left")
_placeholder(_date_year_entry, "YYYY")
ttk.Label(_date_row, text="Month", style="Dim.TLabel").pack(side="left", padx=(10, 3))
_date_month_entry = _entry(_date_row, textvariable=date_month_var, width=4)
_date_month_entry.pack(side="left")
_placeholder(_date_month_entry, "MM")
ttk.Label(_date_row, text="Day", style="Dim.TLabel").pack(side="left", padx=(10, 3))
_date_day_entry = _entry(_date_row, textvariable=date_day_var, width=4)
_date_day_entry.pack(side="left")
_placeholder(_date_day_entry, "DD")
ttk.Label(_date_row, text="Month/Day optional (defaults to Jan 1)",
          style="Dim.TLabel").pack(side="left", padx=(12, 0))


def _date_auto_advance(entry, next_entry, max_chars):
    """Auto-advance focus when the user types the expected number of digits."""
    def _on_key(event):
        val = entry.get()
        # Only advance on digit keys, not backspace/tab/etc
        if event.char.isdigit() and len(val) >= max_chars:
            next_entry.focus_set()
    entry.bind("<KeyRelease>", _on_key)

_date_auto_advance(_date_year_entry, _date_month_entry, 4)
_date_auto_advance(_date_month_entry, _date_day_entry, 2)


def _toggle_date_entry():
    if new_fromdate_var.get():
        _date_row.pack(anchor="w", pady=(2, 0))
    else:
        _date_row.pack_forget()
        date_year_var.set("")
        date_month_var.set("")
        date_day_var.set("")
        _date_year_entry.event_generate("<FocusOut>")
        _date_month_entry.event_generate("<FocusOut>")
        _date_day_entry.event_generate("<FocusOut>")


def refresh_channel_dropdowns():
    with config_lock:
        sorted_channels = sorted(config.get("channels", []), key=lambda c: c["name"].lower())
        names = [c["name"] for c in sorted_channels]
        chan_dropdown["values"] = names

        settings_chan_tree.delete(*settings_chan_tree.get_children())
        for i, c in enumerate(sorted_channels):
            res = c.get("resolution", CHANNEL_DEFAULTS["resolution"])
            display_res = f"{res}p" if res.isdigit() else res
            dur = c.get("min_duration", 0)
            dur_str = f"{dur // 60}m" if dur else "—"
            maxdur = c.get("max_duration", 0)
            maxdur_m = maxdur // 60 if maxdur else 0
            maxdur_str = f"{maxdur_m}m" if maxdur_m else "—"
            name_col = c['name']

            ls_raw = c.get("last_sync", "")
            ls_str = "Never"
            if ls_raw:
                try:
                    dt = datetime.strptime(ls_raw, "%Y-%m-%d %H:%M")
                    diff_mins = int((datetime.now() - dt).total_seconds() // 60)
                    if diff_mins < 1:
                        ls_str = "just now"
                    elif diff_mins < 60:
                        ls_str = f"{diff_mins}m ago"
                    elif diff_mins < 1440:
                        ls_str = f"{diff_mins // 60}hr ago"
                    elif diff_mins < 10080:
                        ls_str = f"{diff_mins // 1440}d ago"
                    else:
                        ls_str = f"{diff_mins // 10080}wk ago"
                except Exception:
                    ls_str = ls_raw[:12]

            # Compress column — checkmark if any compression settings enabled
            compress_str = "✓" if c.get("compress_enabled", False) and c.get("compress_level", "") in _QUALITY_OPTIONS else "—"

            # Transcribed status column — only show Running/Queued for transcription tasks, not encode
            auto_t = c.get("auto_transcribe", False)
            ch_url_t = c.get("url", "")
            t_complete = c.get("transcription_complete", False)
            _is_queued_t = False
            _is_running_t = False
            # Check GPU Tasks queue
            with _gpu_queue_lock:
                _is_queued_t = any(q.get("ch_url") == ch_url_t and q["type"] in ("transcribe", "mt") for q in _gpu_queue)
            if _gpu_running and _gpu_current.get("ch_url") == ch_url_t:
                _cur_item = _gpu_current_item
                if _cur_item and _cur_item.get("type") in ("transcribe", "mt"):
                    _is_running_t = True
            # Check standalone transcription (non-GPU path) — skip during sync/reorg
            # since _current_job["url"] is shared and may belong to the sync, not transcription
            if not _is_running_t and _transcribe_running and not _sync_running and not _reorg_running and _current_job.get("url") == ch_url_t:
                _is_running_t = True
            if not _is_queued_t:
                with _transcribe_queue_lock:
                    _is_queued_t = any(q[1] == ch_url_t for q in _transcribe_queue)
            t_pending = c.get("transcription_pending", 0)
            if _is_running_t:
                trans_str = "Running"
            elif _is_queued_t:
                trans_str = "Queued"
            elif t_complete and t_pending > 0:
                trans_str = f"✓ -{t_pending}"
            elif t_complete:
                trans_str = "✓ Done" if auto_t else "✓"
            elif auto_t:
                trans_str = "✓ Auto"
            else:
                trans_str = "—"

            tag = "odd" if i % 2 else "even"
            settings_chan_tree.insert("", tk.END, values=(name_col, display_res, dur_str, maxdur_str, compress_str, trans_str, ls_str, c['url']),
                                      tags=(tag,))

    if _chan_sort_state["col"]:
        _sort_chan_tree(_chan_sort_state["col"], _chan_sort_state["reverse"])


_editing_channel = {"name": None}


def _set_edit_mode(ch):
    _editing_channel["name"] = ch["name"]
    new_name_var.set(ch.get("folder_override", ch["name"]))
    new_url_var.set(ch["url"])
    new_res_var.set(ch.get("resolution", "720"))
    _min = ch.get("min_duration", 0)
    new_dur_var.set(str(_min // 60) if _min else "")
    _mx_secs = ch.get("max_duration", 0)
    new_maxdur_var.set(str(_mx_secs // 60) if _mx_secs else "")
    _m = ch.get("mode", "full")
    new_mode_var.set(_m)
    new_all_var.set(_m == "full")
    new_fromdate_var.set(_m == "date")
    stored_date = ch.get("date_after", "")
    if stored_date and len(stored_date) == 8:
        date_year_var.set(stored_date[:4])
        date_month_var.set(stored_date[4:6])
        date_day_var.set(stored_date[6:])
    else:
        date_year_var.set("")
        date_month_var.set("")
        date_day_var.set("")
    _toggle_date_entry()
    _sy = ch.get("split_years", False)
    _sm = ch.get("split_months", False)
    if _sy and _sm:
        new_folder_org_var.set("Years/Months")
    elif _sy:
        new_folder_org_var.set("Years")
    else:
        new_folder_org_var.set("None")
    new_auto_transcribe_var.set(ch.get("auto_transcribe", False))
    new_compress_var.set(ch.get("compress_enabled", False))
    _cr = ch.get("compress_output_res", "")
    new_compress_res_var.set(f"{_cr}p" if _cr else "Original")  # set Res first → triggers quality combo enable
    _raw_level = ch.get("compress_level", "")
    new_compress_level_var.set(_LEVEL_MIGRATION.get(_raw_level, _raw_level))
    new_compress_batch_var.set(str(ch.get("compress_batch_size", 20)))
    add_channel_btn.config(text="💾 Update channel", state="normal", style="Warn.TButton")
    add_outer.config(text="Edit channel")
    _set_add_details_visible(True)
    cancel_edit_btn.pack(side="left", padx=(0, 8))
    reorg_done_label.pack_forget()

    new_name_entry.event_generate("<FocusOut>")
    _new_url_entry.event_generate("<FocusOut>")
    if ch.get("mode") != "date":
        _date_year_entry.event_generate("<FocusOut>")
        _date_month_entry.event_generate("<FocusOut>")
        _date_day_entry.event_generate("<FocusOut>")

    new_name_entry.focus_set()


def _clear_edit_mode():
    _editing_channel["name"] = None
    new_name_var.set("")
    new_url_var.set("")
    new_res_var.set("720")
    new_dur_var.set("")
    new_maxdur_var.set("")
    new_mode_var.set("sub")
    new_all_var.set(False)
    new_fromdate_var.set(False)
    date_year_var.set("")
    date_month_var.set("")
    date_day_var.set("")
    _toggle_date_entry()
    new_folder_org_var.set("None")
    new_auto_transcribe_var.set(False)
    new_compress_var.set(False)
    new_compress_level_var.set("")
    new_compress_res_var.set("")
    new_compress_batch_var.set("20")
    add_channel_btn.config(text="Add channel", style="TButton", state="disabled")
    add_outer.config(text="Add channel")
    _set_add_details_visible(False)
    cancel_edit_btn.pack_forget()
    try:
        reorg_done_label.pack_forget()
    except Exception:
        pass

    new_name_entry.event_generate("<FocusOut>")
    _new_url_entry.event_generate("<FocusOut>")
    _date_year_entry.event_generate("<FocusOut>")
    _date_month_entry.event_generate("<FocusOut>")
    _date_day_entry.event_generate("<FocusOut>")


def on_channel_double_click(event):
    sel = settings_chan_tree.selection()
    if not sel: return
    item = settings_chan_tree.item(sel[0])
    target_url = item['values'][7]
    with config_lock:
        channels = config.get("channels", [])
        for ch in channels:
            if ch["url"] == target_url:
                _set_edit_mode(copy.deepcopy(ch))
                break


settings_chan_tree.bind("<Double-Button-1>", on_channel_double_click)


def on_chan_list_select(event):
    if settings_chan_tree.selection():
        sync_single_btn.config(state="normal")
        remove_channel_btn.pack(side="left", padx=(0, 8))
        remove_channel_btn.config(state="normal")
    else:
        sync_single_btn.config(state="disabled")
        remove_channel_btn.pack_forget()


settings_chan_tree.bind("<<TreeviewSelect>>", on_chan_list_select)

_chan_ctx_menu = tk.Menu(root, tearoff=0, bg=C_RAISED, fg=C_TEXT,
                         activebackground=C_BTN_HVR, activeforeground=C_TEXT,
                         disabledforeground=C_DIM, bd=0, relief="flat")

_ctx_channel = {"ch": None}


def _chan_ctx_open_url():
    ch = _ctx_channel["ch"]
    if not ch:
        return
    try:
        import webbrowser
        webbrowser.open(ch["url"])
    except Exception as e:
        log(f"ERROR: Could not open URL: {e}\n", "red")


def _chan_ctx_open_folder():
    ch = _ctx_channel["ch"]
    if not ch:
        return
    with config_lock:
        base = config.get("output_dir", "").strip() or BASE_DIR
    folder_name = sanitize_folder(ch.get("folder_override", "").strip() or ch["name"])
    path = os.path.join(base, folder_name)

    if not os.path.exists(path):
        try:
            os.makedirs(path, exist_ok=True)
        except OSError:
            path = base
    if not os.path.exists(path):
        log(f"  \u26a0 Output folder not found: {path}\n  Check your output directory in Settings.\n", "red")
        return
    try:
        norm_path = os.path.normpath(path)
        if os.name == "nt":
            os.startfile(norm_path)
        elif sys.platform == "darwin":
            subprocess.Popen(["open", norm_path])
        else:
            subprocess.Popen(["xdg-open", norm_path])
    except Exception as e:
        log(f"ERROR: Could not open folder: {e}\n", "red")


def _chan_ctx_edit_settings():
    ch = _ctx_channel["ch"]
    if not ch:
        return
    _set_edit_mode(ch)
    notebook.select(tab_settings)


def _chan_ctx_sync_now():
    ch = _ctx_channel["ch"]
    if not ch:
        return
    if _sync_running or _reorg_running:
        # Queue the channel for sync after the current operation finishes
        with _sync_queue_lock:
            # Don't queue duplicates
            if not any(q["url"] == ch["url"] for q in _sync_queue):
                _sync_queue.append(copy.deepcopy(ch))
                with _queue_order_lock:
                    _queue_order.append(("sync", ch["url"]))
                log(f"\n=== Added {ch['name']} to sync queue ===\n", "header")
            else:
                log(f"  ⚠ {ch['name']} is already in the sync queue.\n", "simpleline")
        _update_queue_btn()
        return
    # Match the channel in the treeview to visually select it before syncing
    for item in settings_chan_tree.get_children():
        if settings_chan_tree.item(item)['values'][7] == ch["url"]:
            settings_chan_tree.selection_set(item)
            on_chan_list_select(None)
            break
    sync_single_channel()


_chan_ctx_menu.add_command(label="Sync now", command=_chan_ctx_sync_now)
_chan_ctx_menu.add_command(label="Edit settings", command=_chan_ctx_edit_settings)
_chan_ctx_menu.add_command(label="Open folder in Explorer", command=_chan_ctx_open_folder)
_chan_ctx_menu.add_command(label="Open URL in browser", command=_chan_ctx_open_url)

_chan_ctx_menu.add_separator()


def _chan_ctx_get_folder():
    """Get the folder path for the right-clicked channel."""
    ch = _ctx_channel["ch"]
    if not ch:
        return None
    with config_lock:
        base = config.get("output_dir", "").strip() or BASE_DIR
    folder_name = sanitize_folder(ch.get("folder_override", "").strip() or ch["name"])
    return os.path.join(base, folder_name)


def _chan_ctx_update_split(ch_url, split_years, split_months):
    """Update a channel's split settings in config and save."""
    with config_lock:
        for cfg_ch in config.get("channels", []):
            if cfg_ch["url"] == ch_url:
                cfg_ch["split_years"] = split_years
                cfg_ch["split_months"] = split_months
                break
    save_config(config)
    if _root_alive:
        _ui_queue.append(refresh_channel_dropdowns)


def _chan_ctx_org_by_year():
    ch = _ctx_channel["ch"]
    if not ch:
        return
    folder = _chan_ctx_get_folder()
    if not folder or not os.path.isdir(folder):
        log(f"  ⚠ Folder not found: {folder}\n", "red")
        return
    if not messagebox.askyesno("Organize Folder",
                               f"Reorganize \"{ch['name']}\" folder by Year?\n\n"
                               f"This will move all video files into year subfolders."):
        return
    recheck = messagebox.askyesno("Re-check Dates",
                                  "Would you like to also re-check file dates?\n\n"
                                  "This fetches exact upload dates from YouTube for each video.\n"
                                  "It ensures files are dated correctly before organizing.\n\n"
                                  "⚠ WARNING: THIS CAN TAKE MULTIPLE HOURS ON LARGE CHANNELS")
    _run_reorganize_auto(ch["name"], folder, target_years=True, target_months=False,
                         ch_url=ch["url"], recheck_dates=recheck)


def _chan_ctx_org_by_year_month():
    ch = _ctx_channel["ch"]
    if not ch:
        return
    folder = _chan_ctx_get_folder()
    if not folder or not os.path.isdir(folder):
        log(f"  ⚠ Folder not found: {folder}\n", "red")
        return
    if not messagebox.askyesno("Organize Folder",
                               f"Reorganize \"{ch['name']}\" folder by Year/Month?\n\n"
                               f"This will move all video files into year and month subfolders."):
        return
    recheck = messagebox.askyesno("Re-check Dates",
                                  "Would you like to also re-check file dates?\n\n"
                                  "This fetches exact upload dates from YouTube for each video.\n"
                                  "It ensures files are dated correctly before organizing.\n\n"
                                  "⚠ WARNING: THIS CAN TAKE MULTIPLE HOURS ON LARGE CHANNELS")
    _run_reorganize_auto(ch["name"], folder, target_years=True, target_months=True,
                         ch_url=ch["url"], recheck_dates=recheck)


def _chan_ctx_unorganize():
    ch = _ctx_channel["ch"]
    if not ch:
        return
    folder = _chan_ctx_get_folder()
    if not folder or not os.path.isdir(folder):
        log(f"  ⚠ Folder not found: {folder}\n", "red")
        return
    if not messagebox.askyesno("Un-Organize Folder",
                               f"Un-organize \"{ch['name']}\" folder?\n\n"
                               f"This will move all video files back to the root folder."):
        return
    _run_reorganize_auto(ch["name"], folder, target_years=False, target_months=False,
                         ch_url=ch["url"])


_chan_ctx_menu.add_command(label="Org. Folder by Year", command=_chan_ctx_org_by_year)
_chan_ctx_menu.add_command(label="Org. Folder by Year/Month", command=_chan_ctx_org_by_year_month)
_chan_ctx_menu.add_command(label="Un-Organize Folder", command=_chan_ctx_unorganize)

_chan_ctx_menu.add_separator()


def _chan_ctx_reapply_org():
    ch = _ctx_channel["ch"]
    if not ch:
        return
    folder = _chan_ctx_get_folder()
    if not folder or not os.path.isdir(folder):
        log(f"  ⚠ Folder not found: {folder}\n", "red")
        return
    sy = ch.get("split_years", False)
    sm = ch.get("split_months", False)
    if sy and sm:
        desc = "Year/Month"
    elif sy:
        desc = "Year"
    else:
        desc = "Flat (no subfolders)"
    if not messagebox.askyesno("Re-apply Organization",
                               f"Re-apply \"{desc}\" organization to \"{ch['name']}\"?\n\n"
                               f"This will move any misplaced files to match the current setting."):
        return
    recheck = messagebox.askyesno("Re-check Dates",
                                  "Would you like to also re-check file dates?\n\n"
                                  "This fetches exact upload dates from YouTube for each video.\n"
                                  "It ensures files are dated correctly before organizing.\n\n"
                                  "⚠ WARNING: THIS CAN TAKE MULTIPLE HOURS ON LARGE CHANNELS")
    _run_reorganize_auto(ch["name"], folder, target_years=sy, target_months=sm,
                         ch_url=ch["url"], recheck_dates=recheck)


_chan_ctx_menu.add_command(label="Re-apply Organization", command=_chan_ctx_reapply_org)

_chan_ctx_menu.add_separator()


def _chan_ctx_transcribe():
    """Right-click → Transcribe channel. Shows dialog for org mode, then starts transcription."""
    ch = _ctx_channel["ch"]
    if not ch:
        return
    folder = _chan_ctx_get_folder()
    if not folder:
        return
    ch_name = ch["name"]
    ch_url = ch["url"]
    sy = ch.get("split_years", False)
    sm = ch.get("split_months", False)

    if not sy:
        # Unorganized — no dialog needed, just one big file
        _add_to_gpu_queue({"type": "transcribe", "ch_name": ch_name, "ch_url": ch_url,
                           "folder": folder, "split_years": False, "split_months": False, "combined": True})
        return

    # Show dialog: Combined vs Follow organization
    dlg = tk.Toplevel(root)
    dlg.title("Transcribe Channel")
    dlg.configure(bg=C_BG)
    dlg.resizable(False, False)
    dlg.transient(root)
    dlg.grab_set()
    dlg.update_idletasks()
    _apply_dark_title_bar(dlg)

    # Center on parent
    dlg.update_idletasks()
    pw, ph = root.winfo_width(), root.winfo_height()
    px, py = root.winfo_x(), root.winfo_y()
    dw, dh = 380, 200
    dlg.geometry(f"{dw}x{dh}+{px + (pw - dw) // 2}+{py + (ph - dh) // 2}")

    tk.Label(dlg, text=f"Transcribe: {ch_name}", bg=C_BG, fg=C_TEXT,
             font=("Segoe UI Semibold", 11)).pack(pady=(16, 4))
    tk.Label(dlg, text="Where should transcript files be placed?", bg=C_BG, fg=C_DIM,
             font=("Segoe UI", 9)).pack(pady=(0, 12))

    choice_var = tk.StringVar(value="follow")
    org_desc = "Year" if sy and not sm else "Year/Month"

    rb_frame = tk.Frame(dlg, bg=C_BG)
    rb_frame.pack(padx=20, anchor="w")
    tk.Radiobutton(rb_frame, text=f"Follow organization ({org_desc} folders)",
                   variable=choice_var, value="follow",
                   bg=C_BG, fg=C_TEXT, selectcolor=C_RAISED, activebackground=C_BG,
                   activeforeground=C_TEXT, font=("Segoe UI", 10)).pack(anchor="w", pady=2)
    tk.Radiobutton(rb_frame, text="Combined (one file for entire channel)",
                   variable=choice_var, value="combined",
                   bg=C_BG, fg=C_TEXT, selectcolor=C_RAISED, activebackground=C_BG,
                   activeforeground=C_TEXT, font=("Segoe UI", 10)).pack(anchor="w", pady=2)

    btn_frame = tk.Frame(dlg, bg=C_BG)
    btn_frame.pack(pady=(16, 0))

    def _on_ok():
        combined = choice_var.get() == "combined"
        dlg.destroy()
        _add_to_gpu_queue({"type": "transcribe", "ch_name": ch_name, "ch_url": ch_url,
                           "folder": folder, "split_years": sy, "split_months": sm, "combined": combined})

    ttk.Button(btn_frame, text="Start", command=_on_ok, style="Sync.TButton").pack(side="left", padx=4)
    ttk.Button(btn_frame, text="Cancel", command=dlg.destroy).pack(side="left", padx=4)

    dlg.bind("<Return>", lambda e: _on_ok())
    dlg.bind("<Escape>", lambda e: dlg.destroy())


_chan_ctx_menu.add_command(label="Transcribe channel", command=_chan_ctx_transcribe)

_chan_ctx_menu.add_separator()


def _chan_ctx_remove():
    ch = _ctx_channel["ch"]
    if not ch:
        return
    if not messagebox.askyesno("Remove Channel",
                               f"Are you sure you want to remove \"{ch['name']}\" from your subscription list?"):
        return
    # Select the channel in the tree so remove_channel() can find it
    for item in settings_chan_tree.get_children():
        if settings_chan_tree.item(item)['values'][7] == ch["url"]:
            settings_chan_tree.selection_set(item)
            on_chan_list_select(None)
            break
    remove_channel()


_chan_ctx_menu.add_command(label="Remove this channel", command=_chan_ctx_remove)


def _chan_ctx_show(event):
    row = settings_chan_tree.identify_row(event.y)
    if row:
        settings_chan_tree.selection_set(row)
        on_chan_list_select(None)
        target_url = settings_chan_tree.item(row)['values'][7]
        with config_lock:
            for c in config.get("channels", []):
                if c["url"] == target_url:
                    _ctx_channel["ch"] = copy.deepcopy(c)
                    break
    else:
        _ctx_channel["ch"] = None

    if _ctx_channel["ch"]:
        ch = _ctx_channel["ch"]
        sy = ch.get("split_years", False)
        sm = ch.get("split_months", False)

        # Menu indices: 0=Sync, 1=Edit, 2=Open folder, 3=Open URL, 4=separator,
        #               5=Org Year, 6=Org Year/Month, 7=Un-Organize, 8=separator, 9=Re-apply,
        #               10=separator, 11=Transcribe, 12=separator, 13=Remove
        # Dynamic label: show "Add to Sync List" when a sync/reorg is running
        if _sync_running or _reorg_running:
            # Check if this channel is already queued or actively syncing
            _ch_url = ch.get("url", "")
            with _sync_queue_lock:
                _already_queued = any(q["url"] == _ch_url for q in _sync_queue)
            _already_syncing = _current_job.get("url") == _ch_url
            if _already_queued or _already_syncing:
                _chan_ctx_menu.entryconfig(0, label="Already in Sync List",
                                          state="normal", foreground=C_DIM,
                                          command=lambda: None)
            else:
                _chan_ctx_menu.entryconfig(0, label="Add to Sync List",
                                          state="normal", foreground=C_TEXT,
                                          command=_chan_ctx_sync_now)
        else:
            _chan_ctx_menu.entryconfig(0, label="Sync now",
                                      state="normal", foreground=C_TEXT,
                                      command=_chan_ctx_sync_now)

        # Dim the option matching the channel's current organization mode;
        # all others stay enabled (they will queue if a sync/reorg is running)
        for idx, is_current in ((5, sy and not sm), (6, sy and sm), (7, not sy and not sm)):
            if is_current:
                _chan_ctx_menu.entryconfig(idx, foreground=C_DIM, state="normal")
            else:
                _chan_ctx_menu.entryconfig(idx, foreground=C_TEXT, state="normal")
        _chan_ctx_menu.entryconfig(9, foreground=C_TEXT, state="normal")

        # Transcribe menu item (index 11) — now routes to GPU Tasks
        _ch_url_t = ch.get("url", "")
        # Check if this channel has any downloaded videos
        _ch_folder = _chan_ctx_get_folder()
        _has_videos = False
        if _ch_folder and os.path.isdir(_ch_folder):
            _VIDEO_EXTS_CHECK = (".mp4", ".mkv", ".webm", ".avi", ".wav", ".mp3", ".m4a", ".flac")
            for _root_d, _dirs_d, _files_d in os.walk(_ch_folder):
                if any(f.lower().endswith(_VIDEO_EXTS_CHECK) for f in _files_d):
                    _has_videos = True
                    break
        # Check if this channel is already being transcribed via GPU Tasks
        _is_active_gpu = _gpu_running and _gpu_current.get("label") and _ch_url_t and _ch_url_t in (_gpu_current.get("label") or "")
        with _gpu_queue_lock:
            _already_in_gpu = any(q.get("ch_url") == _ch_url_t for q in _gpu_queue)
            _gpu_has_items = bool(_gpu_queue)
        if not _has_videos:
            _chan_ctx_menu.entryconfig(11, label="Transcribe Channel  (sync first)",
                                      state="normal", foreground=C_DIM,
                                      command=lambda: None)
            try:
                _chan_ctx_menu.entryconfig(11, activeforeground=C_DIM)
            except Exception:
                pass
        elif _is_active_gpu:
            _chan_ctx_menu.entryconfig(11, label="Transcription in progress",
                                      state="normal", foreground=C_DIM,
                                      command=lambda: None)
        elif _already_in_gpu:
            _chan_ctx_menu.entryconfig(11, label="Already in GPU Tasks",
                                      state="normal", foreground=C_DIM,
                                      command=lambda: None)
        else:
            _t_label = "Add Transc. to GPU Tasks" if (_gpu_has_items or _gpu_running) else "Transcribe Channel"
            _chan_ctx_menu.entryconfig(11, label=_t_label,
                                      state="normal", foreground=C_TEXT,
                                      command=_chan_ctx_transcribe)

        try:
            _chan_ctx_menu.tk_popup(event.x_root, event.y_root)
        finally:
            _chan_ctx_menu.grab_release()


settings_chan_tree.bind("<Button-3>", _chan_ctx_show)


def global_click_handler(event):
    try:
        if str(notebook.select()) == str(tab_settings):
            if event.widget not in (settings_chan_tree, chan_scrollbar, remove_channel_btn, sync_single_btn,
                                    add_channel_btn, action_btn_frame, cancel_edit_btn):
                settings_chan_tree.selection_set([])
                on_chan_list_select(None)
    except Exception:
        pass


root.bind_all("<Button-1>", global_click_handler, add="+")


def _parse_date_input(raw):
    raw = raw.strip()
    if re.fullmatch(r'\d{8}', raw):
        return raw
    m = re.fullmatch(r'(\d{4})-(\d{2})-(\d{2})', raw)
    if m:
        return m.group(1) + m.group(2) + m.group(3)
    return None


def add_channel():
    name = sanitize_folder(_real_get(new_name_entry).strip())
    url = _real_get(_new_url_entry).strip()
    if not name or not url: return

    dur_val = _parse_duration(new_dur_var.get()) * 60
    maxdur_val = _parse_duration(new_maxdur_var.get()) * 60

    mode = new_mode_var.get()
    date_after = ""
    if mode == "date":
        y = _real_get(_date_year_entry).strip()
        m = _real_get(_date_month_entry).strip()
        d = _real_get(_date_day_entry).strip()
        if not y or not re.fullmatch(r'\d{4}', y):
            url_error_var.set("Invalid date — Year is required (YYYY)")
            return
        m = m.zfill(2) if m else "01"
        d = d.zfill(2) if d else "01"
        if not re.fullmatch(r'\d{2}', m) or not re.fullmatch(r'\d{2}', d):
            url_error_var.set("Invalid date format")
            return
        date_after = y + m + d
        url_error_var.set("")

    editing = _editing_channel["name"]

    # Capture old/new split settings before saving (for auto-reorganize on edit)
    old_split_years = False
    old_split_months = False
    new_years = new_split_years_var.get()
    new_months = new_split_months_var.get()

    with config_lock:
        channels = config.setdefault("channels", [])

        if editing:
            for ch in channels:
                if ch["name"] == editing:
                    if name != editing and any(c["name"] == name for c in channels):
                        log(f"ERROR: A channel named '{name}' already exists.\n", "red")
                        return

                    old_split_years = ch.get("split_years", False)
                    old_split_months = ch.get("split_months", False)

                    old_mode = ch.get("mode")
                    old_date_after = ch.get("date_after", "")
                    if old_mode != mode or old_date_after != date_after:
                        ch["initialized"] = False
                        ch["sync_complete"] = False
                        ch.pop("init_batch_after", None)
                        ch.pop("init_complete", None)

                        # If date filter was removed/changed, un-archive the previously
                        # filtered IDs so they can be downloaded on next sync
                        if old_mode == "date" and ch.get("date_archived_ids"):
                            _remove_ids_from_archive(ch["date_archived_ids"])
                            removed_count = len(ch["date_archived_ids"])
                            ch["date_archived_ids"] = []
                            log(f"  ✓ Removed {removed_count:,} date-filtered IDs from archive "
                                f"— they will be re-evaluated on next sync.\n", "green")

                    ch["folder_override"] = sanitize_folder(name)
                    ch["name"] = name
                    ch["url"] = url
                    ch["resolution"] = new_res_var.get()
                    ch["min_duration"] = dur_val
                    ch["max_duration"] = maxdur_val
                    ch["mode"] = mode
                    ch["date_after"] = date_after
                    ch["split_years"] = new_years
                    ch["split_months"] = new_months
                    ch["auto_transcribe"] = new_auto_transcribe_var.get()
                    _old_compress = ch.get("compress_enabled", False)
                    _old_c_level = ch.get("compress_level", "")
                    _old_c_res = ch.get("compress_output_res", "")
                    _new_compress = new_compress_var.get()
                    _new_c_level = new_compress_level_var.get()
                    _new_c_res_raw = new_compress_res_var.get()
                    _new_c_res = _new_c_res_raw.replace("p", "") if _new_c_res_raw != "Original" else ""
                    ch["compress_enabled"] = _new_compress
                    ch["compress_level"] = _new_c_level
                    ch["compress_output_res"] = _new_c_res
                    try:
                        ch["compress_batch_size"] = int(new_compress_batch_var.get())
                    except (ValueError, TypeError):
                        ch["compress_batch_size"] = 20
                    break
        else:
            if any(c["name"] == name for c in channels):
                log(f"ERROR: A channel named '{name}' already exists.\n", "red")
                return
            channels.append({
                "name": name, "url": url, "resolution": new_res_var.get(),
                "min_duration": dur_val,
                "max_duration": maxdur_val,
                "mode": mode, "date_after": date_after, "initialized": False,
                "folder_override": sanitize_folder(name),
                "split_years": new_years,
                "split_months": new_months,
                "auto_transcribe": new_auto_transcribe_var.get(),
                "compress_enabled": new_compress_var.get(),
                "compress_level": new_compress_level_var.get(),
                "compress_output_res": (lambda v: v.replace("p", "") if v != "Original" else "")(new_compress_res_var.get()),
                "compress_batch_size": int(new_compress_batch_var.get() or "20"),
            })

    _clear_edit_mode()
    refresh_channel_dropdowns()
    save_config(config)

    # Don't auto-add newly subscribed channels to the sync queue —
    # initialization should only happen on manual sync or full sync-subbed run.

    # Auto-reorganize existing downloads if split settings changed during edit
    if editing and (old_split_years != new_years or old_split_months != new_months):
        with config_lock:
            base = config.get("output_dir", "").strip() or BASE_DIR
        folder_path = os.path.join(base, sanitize_folder(name))
        _run_reorganize_auto(name, folder_path, new_years, new_months)

    # Offer to re-download & compress existing videos if compress settings changed
    if editing:
        _compress_changed = (
            _new_compress and _new_c_level in _QUALITY_OPTIONS and
            (not _old_compress or _old_c_level != _new_c_level or _old_c_res != _new_c_res)
        )
        if _compress_changed:
            with config_lock:
                _bl_base = config.get("output_dir", "").strip() or BASE_DIR
            _bl_folder = os.path.join(_bl_base, sanitize_folder(name))
            if os.path.isdir(_bl_folder):
                _vid_exts = {".mp4", ".mkv", ".webm", ".avi", ".mov", ".flv", ".m4v"}
                _bl_count = 0
                for _r, _d, _f in os.walk(_bl_folder):
                    _d[:] = [d for d in _d if d not in ("_TEMP_COMPRESS", "_BACKLOG_TEMP")]
                    _bl_count += sum(1 for fn in _f if os.path.splitext(fn)[1].lower() in _vid_exts)
                if _bl_count > 0:
                    _bl_res = ch.get("resolution", "720")
                    _bl_res_label = "Best" if _bl_res == "best" else f"{_bl_res}p"
                    _bl_out_label = f" → {_new_c_res}p output" if _new_c_res else ""
                    _bl_bitrate = _get_compress_bitrate(_new_c_level, _new_c_res)
                    _bl_ask = _dark_askquestion(
                        "Apply to Existing Videos",
                        f"Apply compression to {_bl_count:,} existing video(s)?\n\n"
                        f"Re-download at {_bl_res_label}{_bl_out_label}, "
                        f"quality: {_new_c_level} (~{_bl_bitrate} MB/hr)."
                    )
                    if _bl_ask:
                        _add_to_gpu_queue({
                            "type": "backlog_encode",
                            "ch_name": name,
                            "ch_url": url,
                            "folder": _bl_folder,
                            "resolution": _bl_res,
                            "bitrate_mbhr": _bl_bitrate,
                            "output_res": _new_c_res,
                            "split_years": new_years,
                            "split_months": new_months,
                            "batch_size": int(new_compress_batch_var.get() or "20"),
                        })


def sync_single_channel():
    global _sync_running
    sel = settings_chan_tree.selection()
    if not sel:
        return
    item = settings_chan_tree.item(sel[0])
    target_url = item['values'][7]

    with config_lock:
        ch = None
        for c in config.get("channels", []):
            if c["url"] == target_url:
                ch = copy.deepcopy(c)
                break
        if not ch: return

    # If something is already running, queue instead of starting immediately
    if _sync_running or _reorg_running:
        with _sync_queue_lock:
            if not any(q["url"] == ch["url"] for q in _sync_queue):
                _sync_queue.append(copy.deepcopy(ch))
                with _queue_order_lock:
                    _queue_order.append(("sync", ch["url"]))
                log(f"\n=== Added {ch['name']} to sync queue ===\n", "header")
            else:
                log(f"  ⚠ {ch['name']} is already in the sync queue.\n", "simpleline")
        _update_queue_btn()
        return

    for key in session_totals:
        session_totals[key] = 0
    cancel_event.clear()

    _schedule_autorun(0)

    sync_single_btn.config(state="disabled", text="⏳ Syncing...")
    _update_queue_btn()

    global _job_generation
    _job_generation += 1
    _my_gen = _job_generation

    _sync_running = True
    _current_job["label"] = f"Sync {ch['name']}"
    _current_job["url"] = ch["url"]
    global _current_sync_ch
    _current_sync_ch = copy.deepcopy(ch)
    _tray_start_spin()
    _update_tray_tooltip(f"YT Archiver — Syncing {ch['name']}")
    _captured_outdir = outdir_var.get().strip() or BASE_DIR

    def _single_worker():
        global _sync_running, _current_sync_ch
        try:
            out_dir = _captured_outdir
            if not check_directory_writable(out_dir):
                log(f"ERROR: Cannot write to '{out_dir}'.\n", "red")
                return

            log(f"\n--- [1/1] SYNCING: {ch['name']} ---\n", "header")
            log(f"  Checking channel...\n", "dim")

            deferred_streams = []
            live_ids = []
            max_dur_ch = ch.get("max_duration", 0)
            if not cancel_event.is_set():
                live_videos = _prefetch_livestreams(ch["url"])
                if live_videos:
                    live_ids = [vid[0] for vid in live_videos]
                    if max_dur_ch:
                        log(f"  ⏭ {len(live_videos)} livestream(s) skipped (max-dur set).\n", "dim")
                    else:
                        for _lid, _lurl in live_videos:
                            deferred_streams.append((ch, _lurl, _lid))
                        _lnames = ", ".join(vid[0] for vid in live_videos)
                        log(f"\n", "livestream")
                        log(f"  ⚠  LIVESTREAM DETECTED — WILL DOWNLOAD AFTER SYNC  ⚠\n", "livestream")
                        log(f"  {len(live_videos)} stream(s) queued: {_lnames}\n", "livestream")
                        log(f"\n", "livestream")

            mode = ch.get("mode", "full")
            url = ch["url"]
            res = ch.get("resolution", "720")
            min_dur = ch.get("min_duration", 0)
            folder_ovr = ch.get("folder_override", "")
            is_init = ch.get("initialized", False)
            sync_complete = ch.get("sync_complete", True)

            # --- Batch safety: check cooldown for large full-mode channels ---
            batch_limited = False
            if mode == "full" and not ch.get("init_complete", False):
                can_proceed, cooldown_str = _check_batch_cooldown(ch)
                if not can_proceed:
                    log(f"Skipping {ch['name']} — next batch after {cooldown_str}\n", "dim")
                    if _is_simple_mode:
                        _stop_simple_anim()
                        _cn = ch['name'] if len(ch['name']) <= 34 else ch['name'][:31] + "..."
                        log(f"[{1}/{1}] {_cn:<34} —  Downloaded: None, hit daily limit. Resets at {cooldown_str}\n", "simpleline")
                    return

            if mode == "sub" and not is_init:
                log("First sync: Archiving existing backlog...\n", "green")
                success = internal_run_subscribe_blocking(url)
                if cancel_event.is_set():
                    return
                if success:
                    with config_lock:
                        for cfg_ch in config.get("channels", []):
                            if cfg_ch["url"] == url: cfg_ch["initialized"] = True
                    save_config(config)
                    log("Initialization complete.\n", "green")
                else:
                    log(f"ERROR: Backlog archive failed for {ch['name']}.\n", "red")
                    return
            elif mode == "date" and not is_init:
                date_after = ch.get("date_after", "")
                if not date_after:
                    log(f"ERROR: date_after missing for {ch['name']}.\n", "red")
                    return
                log(f"First sync: Archiving videos before {date_after[:4]}-{date_after[4:6]}-{date_after[6:]}...\n",
                    "green")
                success = internal_run_subscribe_before_date(url, date_after)
                if cancel_event.is_set():
                    return
                if success:
                    with config_lock:
                        for cfg_ch in config.get("channels", []):
                            if cfg_ch["url"] == url: cfg_ch["initialized"] = True
                    save_config(config)
                else:
                    log(f"ERROR: Date-archive failed for {ch['name']}.\n", "red")
                    return

            with config_lock:
                for cfg_ch in config.get("channels", []):
                    if cfg_ch["url"] == url:
                        cfg_ch["sync_complete"] = False
            save_config(config)

            cmd = build_channel_cmd(url, out_dir, min_dur, res, folder_ovr,
                                    break_on_existing=is_init and sync_complete,
                                    max_dur=ch.get("max_duration", 0),
                                    split_years=ch.get("split_years", False),
                                    split_months=ch.get("split_months", False))
            if not cancel_event.is_set():
                # Always update anim state so switching to Simple mid-sync shows correct channel
                _simple_anim_state.update({"channel": ch['name'], "idx": 1, "total": 1,
                                           "dl_current": 0, "ch_total": 0})
                if _is_simple_mode:
                    _start_simple_anim(ch['name'], 1, 1)

                # Skip prefetch for uninitialized full-mode channels — it always returns 0
                # and fires 2 yt-dlp calls that may trigger YouTube rate-limiting before enumeration
                _skip_prefetch = (ch.get("mode", "full") == "full"
                                  and not ch.get("init_complete", False)
                                  and not ch.get("initialized", False))
                if _skip_prefetch:
                    ch_total = 0
                    log("  Skipping video count prefetch (uninitialized channel, going straight to enumeration).\n", "dim")
                else:
                    log("  Fetching video count...\n", "dim")
                    ch_total = _prefetch_total(url)
                if ch_total:
                    log(f"  Video count: {ch_total:,}\n", "dim")

                # --- Batch safety: limit large channel downloads ---
                _batch_pstart = 0
                _batch_cache_ids = None
                _batch_start_idx = 0
                _batch_end_idx = 0
                _all_cached_done = False
                if _should_batch_limit(ch, ch_total):
                    batch_limited = True

                    # Try cached batch flow
                    _batch_cache_ids, _cache_created = _load_or_create_batch_cache(url)
                    if cancel_event.is_set():
                        return

                    if _batch_cache_ids:
                        _batch_start_idx = ch.get("batch_resume_index", 0)

                        # On subsequent runs, check for new uploads
                        if not _cache_created and _batch_start_idx > 0:
                            log("  Checking for new uploads...\n", "dim")
                            _new_ids = _check_new_videos(url, _batch_cache_ids)
                            if _new_ids:
                                log(f"  Found {len(_new_ids)} new video(s), updating cache.\n", "green")
                                _new_set = set(_new_ids)
                                _batch_cache_ids = _new_ids + [x for x in _batch_cache_ids if x not in _new_set]
                                _batch_start_idx += len(_new_ids)
                                try:
                                    with open(_get_batch_cache_path(url), "w", encoding="utf-8") as _cf:
                                        _cf.write("\n".join(_batch_cache_ids) + "\n")
                                except Exception:
                                    pass

                        # Pre-filter: skip already-archived IDs so yt-dlp doesn't waste
                        # time checking/skipping thousands of already-downloaded videos.
                        # Auto-advance through fully-archived batches.
                        _archived_set = _load_archived_ids()
                        _filtered_slice = None

                        while _batch_cache_ids and not cancel_event.is_set():
                            _batch_end_idx = min(_batch_start_idx + BATCH_LIMIT, len(_batch_cache_ids))
                            _batch_slice = _batch_cache_ids[_batch_start_idx:_batch_end_idx]
                            _filtered_slice = [vid for vid in _batch_slice if vid not in _archived_set]
                            _skipped_pre = len(_batch_slice) - len(_filtered_slice)

                            if _filtered_slice:
                                if _skipped_pre:
                                    log(f"  Skipped {_skipped_pre:,} already-downloaded IDs in batch.\n", "dim")
                                break  # Found videos to download

                            # Entire batch already archived — advance
                            log(f"  Batch {_batch_start_idx:,}-{_batch_end_idx:,} fully archived, advancing...\n", "dim")
                            _batch_start_idx = _batch_end_idx

                            if _batch_start_idx >= len(_batch_cache_ids):
                                # ALL videos in cache are already downloaded
                                _clear_batch_state(url, mark_complete=True)
                                log(f"  All {len(_batch_cache_ids):,} cached videos already downloaded. Initialization complete!\n", "green")
                                _all_cached_done = True
                                _batch_cache_ids = None
                                break

                        if _batch_cache_ids and _filtered_slice:
                            _bf_path = _build_batch_file(_filtered_slice)
                        else:
                            _bf_path = None

                        if _bf_path:
                            cmd = build_channel_cmd(url, out_dir, min_dur, res, folder_ovr,
                                                    break_on_existing=False,
                                                    max_dur=ch.get("max_duration", 0),
                                                    split_years=ch.get("split_years", False),
                                                    split_months=ch.get("split_months", False),
                                                    max_downloads=BATCH_LIMIT,
                                                    batch_file=_bf_path)
                            _remaining = len(_batch_cache_ids) - _batch_start_idx
                            log(f"  Large channel ({len(_batch_cache_ids):,} videos). Downloading {len(_filtered_slice):,} new videos (batch {_batch_start_idx:,}-{_batch_end_idx:,}, {_remaining:,} remaining)...\n", "green")
                        elif not _all_cached_done:
                            _batch_cache_ids = None  # fall through to legacy

                    if not _batch_cache_ids and not _all_cached_done:
                        # Legacy fallback
                        _batch_pstart = _get_batch_playlist_start(ch)
                        cmd = build_channel_cmd(url, out_dir, min_dur, res, folder_ovr,
                                                break_on_existing=False,
                                                max_dur=ch.get("max_duration", 0),
                                                split_years=ch.get("split_years", False),
                                                split_months=ch.get("split_months", False),
                                                max_downloads=BATCH_LIMIT,
                                                playlist_start=_batch_pstart)
                        if _batch_pstart > 1:
                            log(f"  Large channel ({ch_total:,} videos). Resuming from index {_batch_pstart}, batch of {BATCH_LIMIT:,}...\n", "green")
                        else:
                            log(f"  Large channel detected ({ch_total:,} videos). Downloading batch of {BATCH_LIMIT:,}...\n", "green")

            # Build incremental compress callback if compress is enabled
            _sc_level = ch.get("compress_level", "")
            _sc_batch_cb = None
            _sc_bsize = ch.get("compress_batch_size", 20)
            if ch.get("compress_enabled", False) and _sc_level in _QUALITY_OPTIONS:
                _sc_folder = os.path.join(out_dir, sanitize_folder(ch.get("folder_override", "") or ch["name"]))
                _sc_prompt_shown = [False]
                def _sc_batch_cb(count, batch_paths, _ch=ch, _u=url, _f=_sc_folder, _lv=_sc_level, _bs=_sc_bsize):
                    _b = _get_next_compress_batch(_u)
                    _add_to_gpu_queue({
                        "type": "encode", "ch_name": _ch["name"], "ch_url": _ch["url"],
                        "folder": _f, "bitrate_mbhr": _get_compress_bitrate(_lv, _ch.get("compress_output_res", "")),
                        "output_res": _ch.get("compress_output_res", ""),
                        "split_years": _ch.get("split_years", False),
                        "split_months": _ch.get("split_months", False),
                        "batch_num": _b, "batch_size": _bs,
                        "target_paths": batch_paths,
                    }, _quiet=True)
                    if count >= 100 and not _sc_prompt_shown[0] and not _gpu_running:
                        _sc_prompt_shown[0] = True
                        if _ask_start_gpu_tasks(count):
                            _ui_queue.append(_gpu_start)

            if not cancel_event.is_set() and not _all_cached_done:
                c_dl = internal_run_cmd_blocking(cmd, channel_total=ch_total, live_ids=live_ids,
                                                 on_batch_ready=_sc_batch_cb,
                                                 compress_batch_size=_sc_bsize)

                # Also check /streams tab for past livestreams
                _streams_url = _get_streams_url(url)
                if _streams_url and not cancel_event.is_set():
                    _streams_cmd = build_channel_cmd(_streams_url, out_dir, min_dur, res, folder_ovr,
                                                     break_on_existing=is_init and sync_complete,
                                                     max_dur=ch.get("max_duration", 0),
                                                     split_years=ch.get("split_years", False),
                                                     split_months=ch.get("split_months", False))
                    _s_dl = internal_run_cmd_blocking(_streams_cmd, on_batch_ready=_sc_batch_cb,
                                                      compress_batch_size=_sc_bsize)
                    if _s_dl:
                        c_dl = (c_dl or 0) + _s_dl

                if _is_simple_mode:
                    _stop_simple_anim()
                    if not cancel_event.is_set():
                        _v = "no new videos" if not c_dl else f"{c_dl} video{'s' if c_dl != 1 else ''}"
                        _tag = "simpleline_green" if c_dl else "simpleline"
                        _cn = ch['name'] if len(ch['name']) <= 34 else ch['name'][:31] + "..."
                        log(f"[{1}/{1}] {_cn:<34} —  Downloaded: {_v}\n", _tag)

            if deferred_streams and not cancel_event.is_set():
                log(f"\n\n" + "█" * 55 + "\n", "livestream")
                log(f"  ⚠  DOWNLOADING {len(deferred_streams)} DEFERRED LIVESTREAM(S)  ⚠\n", "livestream")
                log(f"█" * 55 + "\n\n", "livestream")
                for _ds_ch, _ds_url, _ds_id in deferred_streams:
                    if cancel_event.is_set(): break
                    _ds_name = _ds_ch.get("name", _ds_id)
                    log(f"--- LIVESTREAM: {_ds_name} ---\n", "header")
                    _ds_out = os.path.join(out_dir, sanitize_folder(_ds_ch.get("folder_override", "") or _ds_name))
                    _ds_cmd = build_video_cmd(_ds_url, _ds_out, _ds_ch.get("resolution", "720"))
                    internal_run_cmd_blocking(_ds_cmd)

            _cleanup_batch_file()

            if cancel_event.is_set():
                _stop_simple_anim()
                clear_transient_lines()
                if _skip_current.is_set():
                    pass  # "Skipping" message already logged by _skip_current_job()
                else:
                    log("\nSyncs cancelled by user.\n", "red")
                _cleanup_partial_files(out_dir)
                # Save batch resume if large channel and enough progress was made (but not on skip)
                if not _skip_current.is_set() and (ch_total > 200 or batch_limited):
                    _cancel_total = _last_run_counts["dl"] + _last_run_counts["skip"] + _last_run_counts["dur"] + _last_run_counts["err"]
                    if _cancel_total > 50:
                        if _batch_cache_ids:
                            # Stay at _batch_start_idx — pre-filter will skip archived on resume
                            _save_batch_resume(url, _batch_pstart, _last_run_counts,
                                               cache_resume_index=_batch_start_idx)
                        else:
                            _save_batch_resume(url, _batch_pstart, _last_run_counts)
                        log(f"  Batch progress saved — will resume from here next sync.\n", "dim")
            else:
                # --- Batch safety: handle batch completion ---
                _batch_more_remaining = False
                if batch_limited:
                    _total_processed = _last_run_counts["dl"] + _last_run_counts["skip"] + _last_run_counts["dur"] + _last_run_counts["err"]
                    if _batch_cache_ids:
                        # Advance to end of batch range — pre-filter handled archived IDs
                        _cache_pos = _batch_end_idx
                        _batch_all_done = (_cache_pos >= len(_batch_cache_ids))
                    else:
                        _batch_all_done = (_total_processed < BATCH_LIMIT)

                    if not _batch_all_done:
                        _batch_more_remaining = True
                        if _batch_cache_ids:
                            _save_batch_resume(url, _batch_pstart, _last_run_counts,
                                               cache_resume_index=_cache_pos)
                        else:
                            _save_batch_resume(url, _batch_pstart, _last_run_counts)
                        cooldown_dt = _set_batch_cooldown(url)
                        time_str = cooldown_dt.strftime("%I:%M%p").lstrip("0").lower()
                        date_str = cooldown_dt.strftime("%b %d")
                        log(f"\n  Batch complete — downloaded {c_dl:,} of ~{ch_total:,} videos.\n", "green")
                        log(f"  Next batch available after {time_str}, {date_str}\n", "green")
                        log(f"  Sync this channel again after cooldown to continue.\n", "dim")
                    else:
                        _clear_batch_state(url, mark_complete=True)
                        log(f"  Channel initialization complete — all videos downloaded.\n", "green")

                _ts = datetime.now().strftime("%Y-%m-%d %H:%M")
                with config_lock:
                    for cfg_ch in config.get("channels", []):
                        if cfg_ch["url"] == url:
                            if not _batch_more_remaining:
                                cfg_ch["sync_complete"] = True
                                cfg_ch["initialized"] = True
                            cfg_ch["last_sync"] = _ts
                    # Don't update global "Last Full Sync" for single channel syncs —
                    # that should only be set when a full sync-subbed run completes.
                save_config(config)
                _ui_queue.append(refresh_channel_dropdowns)

                # New videos downloaded — track pending transcription count
                if c_dl > 0:
                    ch["transcription_pending"] = ch.get("transcription_pending", 0) + c_dl

                # Auto-compress: if channel has compress enabled and new videos were downloaded
                # (handled via incremental _sc_batch_cb during download; nothing extra needed here)

                # Auto-transcribe: if channel has auto_transcribe enabled and new videos were downloaded,
                # add it to the GPU Tasks so the user can process when ready
                if c_dl > 0 and ch.get("auto_transcribe", False):
                    _at_folder = os.path.join(out_dir, sanitize_folder(ch.get("folder_override", "") or ch["name"]))
                    _at_sy = ch.get("split_years", False)
                    _at_sm = ch.get("split_months", False)
                    _add_to_gpu_queue({
                        "type": "transcribe", "ch_name": ch["name"], "ch_url": url,
                        "folder": _at_folder, "split_years": _at_sy, "split_months": _at_sm,
                        "combined": not _at_sy
                    })

                _dl = session_totals["dl"]
                _plural = "s" if _dl != 1 else ""
                _notif_msg = f"Downloaded {_dl} video{_plural}. Errors: {session_totals['err']}"
                if _batch_more_remaining:
                    _notif_msg += " (batch — more remaining)"
                show_notification("YT Archiver — Sync complete", _notif_msg)

                _skipped_list = _last_run_counts.get("skipped_titles", [])
                _skip_n = len(_skipped_list)
                log("\n" + "=" * 45 + "\n", "summary")
                log(f"CHANNEL SUMMARY:\n", "summary")
                log(f"Downloaded: {session_totals['dl']}, Skipped: {_skip_n}\n", "summary")
                if session_totals['err'] > 0:
                    log(f"Errors: {session_totals['err']}\n", "summary")
                if _skipped_list:
                    log(f"Skipped videos:\n", "summary")
                    for _i, (_title, _reason) in enumerate(_skipped_list, 1):
                        log(f"  {_i}. {_title}  — {_reason}\n", "filterskip")
                log("=" * 45 + "\n", "summary")
                log("\n=== CHANNEL SYNC COMPLETE ===\n", "header")
        finally:
            elapsed_single = (datetime.now() - t_start_single).total_seconds()
            _record_sync(session_totals["dl"], session_totals["err"], elapsed_single,
                         kind="Manual", channel_name=ch.get("name", ""),
                         skipped=session_totals["dur"])

            # If a newer job has taken over (user cancelled + started new sync),
            # don't touch any shared state — the new job owns it now.
            if _job_generation == _my_gen:
                # Check for queued syncs, reorg, or transcription jobs before fully finishing.
                # Keep _sync_running=True until we confirm nothing else is queued,
                # to prevent a race where another sync starts in the gap.
                _queue_started = False
                if _skip_current.is_set():
                    _skip_current.clear()
                    cancel_event.clear()
                    _queue_started = _process_next_queued()
                elif not cancel_event.is_set():
                    _queue_started = _process_next_queued()

                if not _queue_started:
                    _sync_running = False
                    _current_sync_ch = None
                    if _root_alive:
                        def _on_single_sync_done():
                            _validate_download_btn()
                            sync_btn.config(state="normal")
                            sync_single_btn.config(state="normal", text="▶ Sync this channel")
                            _sync_task_finished()
                            _tray_stop_spin()
                            _dl = session_totals["dl"]
                            if _dl > 0:
                                _update_tray_tooltip(f"YT Archiver — {_dl} new video{'s' if _dl != 1 else ''} downloaded")
                            else:
                                _update_tray_tooltip("YT Archiver — Idle")

                            _iv = AUTORUN_OPTIONS.get(_cached_autorun_label, 0)
                            if _iv:
                                _schedule_autorun(_iv)

                        _ui_queue.append(_on_single_sync_done)

                # Process any videos queued during the sync (only if no queued sync took over)
                if not cancel_event.is_set() and not _queue_started:
                    _process_video_dl_queue()

    # Don't switch to Download tab — mini-logs on every tab show progress,
    # and the user may want to stay on the Subs tab to queue more channels.
    t_start_single = datetime.now()
    threading.Thread(target=_single_worker, daemon=True).start()


def _process_sync_queue():
    """Process next queued sync if any. Returns True if a sync was started."""
    with _sync_queue_lock:
        if not _sync_queue:
            return False
        next_ch = _sync_queue.pop(0)
        remaining = len(_sync_queue)
    with _queue_order_lock:
        try:
            _queue_order.remove(("sync", next_ch["url"]))
        except ValueError:
            pass

    if next_ch.get("initialized", False):
        _current_job["label"] = f"Sync {next_ch['name']}"
    else:
        _current_job["label"] = f"Initialize {next_ch['name']}"
    _update_queue_btn()
    log(f"\n=== Processing queued sync: {next_ch['name']}", "header")
    if remaining:
        log(f" ({remaining} more in queue)", "header")
    log(f" ===\n", "header")

    # Find and select the channel in the tree, then sync it
    def _start_queued():
        global _sync_running
        try:
            _found = False
            for item in settings_chan_tree.get_children():
                if settings_chan_tree.item(item)['values'][7] == next_ch["url"]:
                    settings_chan_tree.selection_set(item)
                    on_chan_list_select(None)
                    _found = True
                    break
            # Must clear _sync_running before calling sync_single_channel(),
            # otherwise it sees True and re-queues instead of starting.
            _sync_running = False
            if _found:
                sync_single_channel()
            else:
                log(f"  ⚠ Could not find {next_ch.get('name', '?')} in channel list — skipping.\n", "red")
                # Try next queued item instead of getting stuck
                _process_next_queued()
        except Exception as e:
            _sync_running = False
            _current_sync_ch = None
            _current_job["label"] = None
            _current_job["url"] = None
            _tray_stop_spin()
            _update_tray_tooltip("YT Archiver — Idle")
            _sync_task_finished()
            _update_queue_btn()
            log(f"  ⚠ Error starting queued sync: {e}\n", "red")

    if _root_alive:
        _ui_queue.append(_start_queued)
    return True


def _process_next_queued():
    """Process the next queued item in insertion order. Returns True if something was started."""
    with _queue_order_lock:
        order_copy = list(_queue_order)
    for source, key in order_copy:
        if source == "sync":
            with _sync_queue_lock:
                has_items = bool(_sync_queue)
            if has_items:
                return _process_sync_queue()
        elif source == "reorg":
            with _reorg_queue_lock:
                has_items = bool(_reorg_queue)
            if has_items:
                return _process_reorg_queue()
        elif source == "transcribe":
            with _transcribe_queue_lock:
                has_items = bool(_transcribe_queue)
            if has_items:
                return _process_transcribe_queue()
        elif source == "mt":
            with _mt_queue_lock:
                has_items = bool(_mt_queue)
            if has_items:
                return _process_mt_queue()
    # Fallback: try each queue in case _queue_order is out of sync
    return _process_sync_queue() or _process_reorg_queue() or _process_transcribe_queue() or _process_mt_queue()


def _process_video_dl_queue():
    """Process queued single-video downloads sequentially after sync finishes.

    Must be called from a worker thread (not the main thread) since it blocks
    on each download. All UI updates are dispatched to the main thread via
    root.after().
    """
    with _video_dl_queue_lock:
        queued = list(_video_dl_queue)
        _video_dl_queue.clear()
    if not queued:
        return
    _update_queue_btn()
    log(f"\n--- Processing {len(queued)} queued download{'s' if len(queued) != 1 else ''} ---\n", "header")

    # Update UI from main thread
    def _show_dl_ui():
        sync_btn.config(state="disabled")
        sync_single_btn.config(state="disabled")
        _update_queue_btn()

    _ui_queue.append(_show_dl_ui)

    for cmd, is_single in queued:
        if cancel_event.is_set():
            break
        cancel_event.clear()
        internal_run_cmd_blocking(cmd)
        if cancel_event.is_set():
            try:
                out_idx = cmd.index("--output")
                out_path = cmd[out_idx + 1]
                _cleanup_partial_files(os.path.dirname(out_path))
            except (ValueError, IndexError):
                pass
            break
        if is_single:
            def _clear_url():
                url_var.set("")
                url_entry.event_generate("<FocusOut>")
                vid_custom_name_var.set("")
            _ui_queue.append(_clear_url)

    # Reset UI from main thread
    def _done_dl_ui():
        _validate_download_btn()
        if not _sync_running:
            sync_btn.config(state="normal")
        sync_single_btn.config(state="normal", text="▶ Sync this channel")
        _sync_task_finished()
        on_chan_list_select(None)

    _ui_queue.append(_done_dl_ui)


def remove_channel():
    sel = settings_chan_tree.selection()
    if not sel: return
    item = settings_chan_tree.item(sel[0])
    target_url = item['values'][7]

    with config_lock:
        channels = config.setdefault("channels", [])
        ch_to_remove = next((c for c in channels if c["url"] == target_url), None)
        if not ch_to_remove: return
        removed_name = ch_to_remove["name"]
        removed_url = ch_to_remove.get("url", "")

    delete_ids = messagebox.askyesno(
        "Remove channel",
        f"Remove \"{removed_name}\" from your subscription list.\n\n"
        "Delete this channel's video IDs from the blocklist?\n\n"
        "• Yes — IDs are removed, so they could be re-downloaded if re-added\n"
        "• No  — videos already archived stay skipped on future syncs",
        icon="question"
    )

    if delete_ids and removed_url:
        def _purge_ids():
            log(f"  Fetching IDs for \"{removed_name}\" to remove from blocklist...\n", "dim")
            proc = None
            try:
                probe_cmd = [
                    "yt-dlp", "--flat-playlist", "--no-warnings",
                    "--print", "%(id)s",
                    "--cookies-from-browser", "firefox",
                    removed_url
                ]
                proc = spawn_yt_dlp(probe_cmd)
                if not proc:
                    return
                channel_ids = set()
                for line in proc.stdout:
                    line = line.strip()
                    if re.fullmatch(r'[\w-]{11}', line):
                        channel_ids.add(line)
                proc.wait()

                if not channel_ids:
                    log(f"  ⚠ No IDs found for \"{removed_name}\" — blocklist unchanged.\n", "red")
                    return

                with io_lock:
                    if os.path.exists(ARCHIVE_FILE):
                        with open(ARCHIVE_FILE, encoding="utf-8") as f_:
                            lines = f_.readlines()
                        kept = []
                        for l in lines:
                            parts = l.strip().split()
                            if parts and parts[-1] in channel_ids:
                                continue
                            kept.append(l)
                        removed_count = len(lines) - len(kept)
                        with open(ARCHIVE_FILE, "w", encoding="utf-8") as f_:
                            f_.writelines(kept)
                        log(f"  ✓ Removed {removed_count:,} IDs for \"{removed_name}\" from blocklist.\n", "green")
                    else:
                        log("  ⚠ Archive file not found — nothing to purge.\n", "red")
            except Exception as e:
                log(f"ERROR purging IDs for \"{removed_name}\": {e}\n", "red")
            finally:
                cleanup_process(proc)

        threading.Thread(target=_purge_ids, daemon=True).start()

    # Determine channel folder path before removing from config
    with config_lock:
        _folder_ovr = ch_to_remove.get("folder_override", "").strip()
        _ch_folder_name = sanitize_folder(_folder_ovr or removed_name)
        _base = config.get("output_dir", "").strip() or BASE_DIR
    _ch_folder_path = os.path.join(_base, _ch_folder_name)

    with config_lock:
        config["channels"] = [c for c in config.get("channels", []) if c["name"] != removed_name]
        if chan_var.get() == removed_name:
            chan_var.set("")
            url_var.set("")
            save_prefs_btn.config(state="disabled")
        if _editing_channel["name"] == removed_name:
            _clear_edit_mode()

    refresh_channel_dropdowns()
    save_config(config)
    remove_channel_btn.pack_forget()

    # Remove any queued GPU tasks for this channel
    with _gpu_queue_lock:
        _before = len(_gpu_queue)
        _gpu_queue[:] = [q for q in _gpu_queue if q.get("ch_url") != removed_url]
        _removed_gpu = _before - len(_gpu_queue)
    if _removed_gpu:
        log(f"  Removed {_removed_gpu} GPU task(s) for \"{removed_name}\" from queue.\n", "dim")
        _update_gpu_btn()
        _save_queue_state()

    # Offer to delete the channel's folder if it exists
    if os.path.isdir(_ch_folder_path):
        try:
            _total_bytes = 0
            for _dp, _dns, _fns in os.walk(_ch_folder_path):
                for _fn in _fns:
                    try:
                        _total_bytes += os.path.getsize(os.path.join(_dp, _fn))
                    except OSError:
                        pass
            _size_disp = _fmt_size(str(_total_bytes)) if _total_bytes else "empty"
        except OSError:
            _size_disp = "unknown size"

        if messagebox.askyesno(
            "Delete channel folder?",
            f"Would you like to delete this channel's folder as well?\n\n"
            f"{_ch_folder_path}\n"
            f"({_size_disp})",
            icon="warning"
        ):
            try:
                shutil.rmtree(_ch_folder_path)
                log(f"  ✓ Deleted folder: {_ch_folder_path}\n", "green")
            except Exception as e:
                log(f"ERROR: Could not delete folder: {e}\n", "red")


split_row = ttk.Frame(add_outer, style="Raised.TFrame")
split_row.grid(row=4, column=0, columnspan=9, sticky="w", padx=(4, 8), pady=(2, 4))

new_folder_org_var = tk.StringVar(value="None")
new_auto_transcribe_var = tk.BooleanVar(value=False)

# Computed BooleanVars kept in sync with the dropdown so all existing code that
# reads new_split_years_var / new_split_months_var continues to work unchanged.
new_split_years_var = tk.BooleanVar(value=False)
new_split_months_var = tk.BooleanVar(value=False)

def _on_folder_org_changed(*_):
    val = new_folder_org_var.get()
    new_split_years_var.set(val in ("Years", "Years/Months"))
    new_split_months_var.set(val == "Years/Months")

new_folder_org_var.trace_add("write", _on_folder_org_changed)

ttk.Label(split_row, text="Folder Org:").pack(side="left", padx=(4, 4))
folder_org_combo = _combo(split_row, textvariable=new_folder_org_var,
                          values=["None", "Years", "Years/Months"],
                          state="readonly", width=13)
folder_org_combo.pack(side="left", padx=(0, 16))

auto_transcribe_cb = ttk.Checkbutton(split_row, text="Auto-transcribe new videos",
                                      variable=new_auto_transcribe_var)
auto_transcribe_cb.pack(side="right", padx=(16, 4))

reorg_done_label = ttk.Label(split_row, text="Done!", style="Green.TLabel")
_reorg_done_job = {"id": None}


MONTH_NAMES = {
    1: "01 January", 2: "02 February", 3: "03 March", 4: "04 April",
    5: "05 May", 6: "06 June", 7: "07 July", 8: "08 August",
    9: "09 September", 10: "10 October", 11: "11 November", 12: "12 December"
}

# Reverse lookup: match existing month folders to month numbers
_MONTH_FOLDER_LOOKUP = {}
for _mn, _mname in MONTH_NAMES.items():
    _MONTH_FOLDER_LOOKUP[_mname.lower()] = _mn                     # "01 january"
    _MONTH_FOLDER_LOOKUP[_mname.split(" ", 1)[1].lower()] = _mn    # "january"
    _MONTH_FOLDER_LOOKUP[str(_mn).zfill(2)] = _mn                  # "01"
    _MONTH_FOLDER_LOOKUP[str(_mn)] = _mn                           # "1"

# Also handle old format like "1 January", "2 February", etc.
for _mn in range(1, 13):
    _old_fmt = f"{_mn} {MONTH_NAMES[_mn].split(' ', 1)[1]}"
    _MONTH_FOLDER_LOOKUP[_old_fmt.lower()] = _mn


def _is_year_folder(name):
    return bool(re.fullmatch(r'\d{4}', name))


def _is_month_folder(name):
    return name.lower() in _MONTH_FOLDER_LOOKUP


VIDEO_EXTS = {'.mp4', '.mkv', '.webm', '.avi', '.mov', '.flv', '.wmv', '.m4v'}


def _reorganize_channel_folder(channel_name, folder_path, target_years, target_months,
                               trust_mtime=False):
    """Reorganize a channel's download folder based on split settings.

    target_years=True, target_months=True  → year/month subfolders
    target_years=True, target_months=False → year subfolders only
    target_years=False                      → flat (everything in root)

    trust_mtime: if True, always use file mtime for date sorting (use after
                 dates have been verified/fixed). If False, prefer existing
                 folder names over mtime.
    """
    if not os.path.isdir(folder_path):
        log(f"  ⚠ Folder not found: {folder_path}\n", "red")
        return

    cancel_event.clear()

    log(f"\n--- Reorganizing: {channel_name} ---\n", "header")

    if target_years:
        if target_months:
            log(f"  Target structure: Year → Month subfolders\n", "dim")
        else:
            log(f"  Target structure: Year subfolders\n", "dim")
    else:
        log(f"  Target structure: Flat (all files in one folder)\n", "dim")

    moved_count = 0
    error_count = 0
    skipped_count = 0

    # Phase 1: Collect all video files from ALL locations (root, year folders, month folders)
    # Each entry: (current_path, mtime_datetime, folder_year, folder_month)
    # folder_year/folder_month come from the existing folder structure and are trusted
    # over mtime (which may reflect HTTP Last-Modified rather than actual upload date).
    all_files = []

    # Scan root folder — no folder date info, fall back to mtime
    try:
        for entry in os.scandir(folder_path):
            if entry.is_file() and os.path.splitext(entry.name)[1].lower() in VIDEO_EXTS:
                try:
                    mtime = datetime.fromtimestamp(entry.stat().st_mtime)
                    all_files.append((entry.path, mtime, None, None))
                except OSError:
                    all_files.append((entry.path, None, None, None))
    except OSError as e:
        log(f"  ERROR scanning root: {e}\n", "red")
        return

    # Scan year subfolders — trust year from folder name
    try:
        for entry in os.scandir(folder_path):
            if entry.is_dir() and _is_year_folder(entry.name):
                f_year = int(entry.name)
                # Scan files directly in year folder
                try:
                    for vid in os.scandir(entry.path):
                        if vid.is_file() and os.path.splitext(vid.name)[1].lower() in VIDEO_EXTS:
                            try:
                                mtime = datetime.fromtimestamp(vid.stat().st_mtime)
                                all_files.append((vid.path, mtime, f_year, None))
                            except OSError:
                                all_files.append((vid.path, None, f_year, None))
                except OSError:
                    pass

                # Scan month subfolders within year folders — trust both year and month
                try:
                    for month_entry in os.scandir(entry.path):
                        if month_entry.is_dir() and _is_month_folder(month_entry.name):
                            f_month = _MONTH_FOLDER_LOOKUP[month_entry.name.lower()]
                            try:
                                for vid in os.scandir(month_entry.path):
                                    if vid.is_file() and os.path.splitext(vid.name)[1].lower() in VIDEO_EXTS:
                                        try:
                                            mtime = datetime.fromtimestamp(vid.stat().st_mtime)
                                            all_files.append((vid.path, mtime, f_year, f_month))
                                        except OSError:
                                            all_files.append((vid.path, None, f_year, f_month))
                            except OSError:
                                pass
                except OSError:
                    pass
    except OSError:
        pass

    total = len(all_files)
    if total == 0:
        log(f"  No video files found in {folder_path}\n", "dim")
        log(f"--- Reorganization complete ---\n", "header")
        return

    log(f"  Found {total:,} video file(s) to process...\n", "dim")

    # Phase 2: Determine target path for each file and move if needed
    is_simple = _is_simple_mode
    _reorg_dots = [0]
    _reorg_anim_job = [None]
    _reorg_anim_active = [False]

    def _reorg_anim_tick():
        try:
            if not _reorg_anim_active[0] or not root.winfo_exists():
                return
            d = _DOTS[_reorg_dots[0] % 3]
            _reorg_dots[0] += 1
            log_simple_status(f"  → Moving {total:,} files{d}\n")
        except Exception:
            pass
        finally:
            if _reorg_anim_active[0] and root.winfo_exists():
                _reorg_anim_job[0] = root.after(500, _reorg_anim_tick)

    if is_simple:
        _reorg_anim_active[0] = True
        def _start_reorg_anim():
            if root.winfo_exists():
                _reorg_anim_job[0] = root.after(500, _reorg_anim_tick)
        _ui_queue.append(_start_reorg_anim)

    for i, (filepath, mtime, folder_year, folder_month) in enumerate(all_files):
        if cancel_event.is_set():
            log(f"  ⚠ Reorganization cancelled.\n", "red")
            # Stop the animation
            _reorg_anim_active[0] = False
            _old_rj = _reorg_anim_job[0]
            _reorg_anim_job[0] = None
            if _old_rj:
                _ui_queue.append(lambda j=_old_rj: root.after_cancel(j) if root.winfo_exists() else None)
            if is_simple:
                log_simple_status("")
            return False

        filename = os.path.basename(filepath)

        # Determine target directory
        # When trust_mtime is True (dates were just re-checked from YouTube), always
        # use mtime. Otherwise prefer folder names over mtime (mtime may reflect
        # HTTP Last-Modified rather than actual upload date) — BUT if the file's
        # mtime year disagrees with the folder year, trust mtime instead (the file
        # was likely placed in the wrong folder and its mtime has since been corrected).
        use_folder = not trust_mtime
        if use_folder and folder_year and mtime and mtime.year != folder_year:
            use_folder = False

        if not target_years:
            # Flat: everything goes to root
            target_dir = folder_path
        elif target_years and not target_months:
            # Year only
            if use_folder and folder_year:
                year_str = str(folder_year)
            elif mtime:
                year_str = str(mtime.year)
            else:
                year_str = "Unknown Year"
            target_dir = os.path.join(folder_path, year_str)
        else:
            # Year + month
            if use_folder and folder_year and folder_month:
                year_str = str(folder_year)
                month_str = MONTH_NAMES.get(folder_month, f"{folder_month:02d} Unknown")
            elif use_folder and folder_year:
                year_str = str(folder_year)
                # Have year from folder but no month — use mtime month as best guess
                if mtime:
                    month_str = MONTH_NAMES.get(mtime.month, f"{mtime.month:02d} Unknown")
                else:
                    month_str = "Unknown Month"
            elif mtime:
                year_str = str(mtime.year)
                month_str = MONTH_NAMES.get(mtime.month, f"{mtime.month:02d} Unknown")
            else:
                year_str = "Unknown Year"
                month_str = "Unknown Month"
            target_dir = os.path.join(folder_path, year_str, month_str)

        target_path = os.path.join(target_dir, filename)

        # Skip if already in the right place
        if os.path.normpath(filepath) == os.path.normpath(target_path):
            skipped_count += 1
            continue

        # Handle filename collision at target
        if os.path.exists(target_path):
            base, ext = os.path.splitext(filename)
            counter = 1
            while os.path.exists(target_path):
                target_path = os.path.join(target_dir, f"{base} ({counter}){ext}")
                counter += 1

        try:
            os.makedirs(target_dir, exist_ok=True)
            shutil.move(filepath, target_path)
            moved_count += 1
            if not is_simple:
                rel_target = os.path.relpath(target_dir, folder_path)
                log(f"  → Moved: {filename}  →  {rel_target}\n", "green")
        except (OSError, shutil.Error) as e:
            log(f"  ⚠ Failed to move: {filename} — {e}\n", "red")
            error_count += 1

    # Stop the animation
    _reorg_anim_active[0] = False
    _old_rj = _reorg_anim_job[0]
    _reorg_anim_job[0] = None
    if _old_rj:
        _ui_queue.append(lambda j=_old_rj: root.after_cancel(j) if root.winfo_exists() else None)
    # Clear the animated status line
    if is_simple:
        log_simple_status("")

    # Phase 3: Clean up empty directories
    cleaned = 0
    for dirpath, dirnames, filenames in os.walk(folder_path, topdown=False):
        if dirpath == folder_path:
            continue
        rel = os.path.relpath(dirpath, folder_path)
        parts = rel.replace("\\", "/").split("/")
        # Only clean up year/month folders we manage
        if len(parts) <= 2 and (_is_year_folder(parts[0]) or parts[0] in ("Unknown Year",)):
            try:
                if not os.listdir(dirpath):
                    os.rmdir(dirpath)
                    cleaned += 1
            except OSError:
                pass

    summary_parts = []
    if moved_count: summary_parts.append(f"{moved_count:,} moved")
    if skipped_count: summary_parts.append(f"{skipped_count:,} already in place")
    if error_count: summary_parts.append(f"{error_count:,} errors")
    if cleaned: summary_parts.append(f"{cleaned:,} empty folders removed")

    log(f"  ✓ Done: {', '.join(summary_parts)}\n", "simpleline_green")
    log(f"--- Reorganization complete ---\n", "summary")
    return True


def _norm_ascii(text):
    """Normalize text to ASCII-only lowercase alphanumeric for fuzzy matching."""
    # NFKD decomposition turns é->e, ñ->n, etc.
    decomposed = unicodedata.normalize('NFKD', text.lower())
    # Keep only ASCII alphanumeric
    return re.sub(r'[^a-z0-9]', '', decomposed)


def _fix_file_dates(channel_url, folder_path):
    """Fetch upload dates from YouTube and fix file mtimes for accurate date sorting."""
    proc = None
    try:
        log(f"\n--- Fetching exact upload dates from YouTube (this may take a while)... ---\n", "header")

        # Use --skip-download (no --flat-playlist) to fetch exact upload dates
        # from each video's page. Slower than approximate_date but accurate.
        proc = spawn_yt_dlp([
            "yt-dlp", "--skip-download", "--no-warnings", "--ignore-errors",
            "--ignore-no-formats-error",
            "--sleep-requests", "1.0",
            "--print", "%(upload_date)s|||%(title)s",
            "--cookies-from-browser", "firefox",
            channel_url
        ])
        if not proc:
            log(f"  ⚠ Could not start yt-dlp to fetch dates.\n", "red")
            return

        # Build lookup: normalized title -> upload_date
        date_lookup = {}
        date_words = []  # [(upload_date, word_set), ...] for word-overlap fallback
        count = 0
        _last_progress_log = time.monotonic()

        # Animated dots for Simple mode (runs on main thread via root.after)
        _fetch_anim = {"active": True, "count": 0, "dot_i": 0, "job": None}
        _dot_cycle = [".  ", ".. ", "..."]

        def _animate_fetch_dots():
            if not _fetch_anim["active"] or not root.winfo_exists():
                return
            try:
                if _is_simple_mode and _fetch_anim["count"] > 0:
                    _fetch_anim["dot_i"] = (_fetch_anim["dot_i"] + 1) % 3
                    log_simple_status(f"  Fetched dates for {_fetch_anim['count']:,} videos so far{_dot_cycle[_fetch_anim['dot_i']]}\n")
                _fetch_anim["job"] = root.after(500, _animate_fetch_dots)
            except Exception:
                pass

        if root.winfo_exists():
            _ui_queue.append(lambda: _animate_fetch_dots())

        for line in proc.stdout:
            if cancel_event.is_set():
                break
            parts = line.strip().split("|||", 1)
            if len(parts) == 2:
                upload_date = parts[0].strip()
                title = parts[1].strip()
                if len(upload_date) == 8 and upload_date.isdigit():
                    # NFC-normalize to handle precomposed vs decomposed Unicode
                    title_nfc = unicodedata.normalize('NFC', title)
                    norm = re.sub(r'[^\w]', '', title_nfc.lower())
                    if norm:
                        date_lookup[norm] = upload_date
                    # Also store with pipe replaced (--replace-in-metadata swaps | to -)
                    norm_pipe = re.sub(r'[^\w]', '', title_nfc.replace('|', '-').lower())
                    if norm_pipe and norm_pipe != norm:
                        date_lookup[norm_pipe] = upload_date
                    # Also store ASCII-only normalized version (handles é, ñ, superscripts, etc.)
                    norm_ascii = _norm_ascii(title_nfc)
                    if norm_ascii and norm_ascii != norm:
                        date_lookup[norm_ascii] = upload_date
                    # Also store with yt-dlp filename sanitization applied (: → ：, etc.)
                    sanitized = title_nfc.replace(':', '：').replace('|', '｜').replace('"', '"').replace('?', '？').replace('*', '＊')
                    norm_sanitized = re.sub(r'[^\w]', '', sanitized.lower())
                    if norm_sanitized and norm_sanitized != norm:
                        date_lookup[norm_sanitized] = upload_date
                    # Build word set for word-overlap fallback (extract words from original title)
                    _words = set()
                    for _w in re.findall(r'\S+', title_nfc.lower()):
                        _aw = _norm_ascii(_w)
                        if len(_aw) >= 2:
                            _words.add(_aw)
                    if len(_words) >= 2:
                        date_words.append((upload_date, _words, norm_ascii or norm))
                    count += 1
                    _fetch_anim["count"] = count
                    is_simple = _is_simple_mode
                    if not is_simple:
                        # Verbose mode: log each video individually (use explicit "dim" tag
                        # to prevent auto-tag detection from coloring titles containing
                        # keywords like "failed" or "100%")
                        date_str = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"
                        log(f"  [{count:,}] {date_str}  {title_nfc}\n", "dim")
                    else:
                        # Simple mode: periodic summary (handled by dot animation)
                        pass

        # Stop the dot animation
        _fetch_anim["active"] = False
        _old_fetch_job = _fetch_anim.get("job")
        _fetch_anim["job"] = None
        if _old_fetch_job:
            _ui_queue.append(lambda j=_old_fetch_job: root.after_cancel(j) if root.winfo_exists() else None)

        proc.wait()
        cleanup_process(proc)
        proc = None

        clear_transient_lines()
        if cancel_event.is_set():
            log("⛔ Date check cancelled.\n", "red")
            return
        if not date_lookup:
            log(f"  ⚠ Could not fetch any dates from YouTube.\n", "red")
            return

        log(f"  Fetched dates for {count:,} videos. Matching to files...\n", "dim")

        # Walk all video files and fix mtimes
        fixed = 0
        unmatched = 0
        already_correct = 0
        unmatched_files = []

        for dirpath, dirnames, filenames in os.walk(folder_path):
            for fname in filenames:
                if cancel_event.is_set():
                    return
                base, ext = os.path.splitext(fname)
                if ext.lower() not in VIDEO_EXTS:
                    continue

                filepath = os.path.join(dirpath, fname)
                # NFC-normalize filename to match NFC-normalized titles
                base_nfc = unicodedata.normalize('NFC', base)
                norm_fname = re.sub(r'[^\w]', '', base_nfc.lower())
                norm_fname_ascii = _norm_ascii(base_nfc)

                # Try exact normalized match
                upload_date = date_lookup.get(norm_fname)

                # Try ASCII-only normalized match (handles special chars stripped by filesystem)
                if not upload_date and norm_fname_ascii != norm_fname:
                    upload_date = date_lookup.get(norm_fname_ascii)

                # Try prefix match (--trim-filenames truncates long titles)
                if not upload_date and len(norm_fname) >= 15:
                    for norm_title, date_val in date_lookup.items():
                        if norm_title.startswith(norm_fname) or norm_fname.startswith(norm_title[:len(norm_fname)]):
                            upload_date = date_val
                            break

                # Try ASCII prefix match for special character titles
                if not upload_date and len(norm_fname_ascii) >= 10:
                    for norm_title, date_val in date_lookup.items():
                        if norm_title.startswith(norm_fname_ascii) or norm_fname_ascii.startswith(norm_title[:len(norm_fname_ascii)]):
                            upload_date = date_val
                            break

                # Word-overlap fallback: extract real words from original strings
                if not upload_date and date_words:
                    fname_words = set()
                    for _w in re.findall(r'\S+', base_nfc.lower()):
                        _aw = _norm_ascii(_w)
                        if len(_aw) >= 2:
                            fname_words.add(_aw)
                    if len(fname_words) >= 2:
                        best_overlap = 0
                        best_date = None
                        for date_val, title_words, _ in date_words:
                            overlap = len(fname_words & title_words) / max(len(fname_words), len(title_words))
                            if overlap > best_overlap and overlap >= 0.65:
                                best_overlap = overlap
                                best_date = date_val
                        if best_date:
                            upload_date = best_date

                # SequenceMatcher fallback: catch near-matches from encoding/sanitization diffs
                if not upload_date and norm_fname_ascii and len(norm_fname_ascii) >= 10:
                    best_ratio = 0
                    best_date = None
                    for date_val, _, t_ascii in date_words:
                        if not t_ascii:
                            continue
                        ratio = difflib.SequenceMatcher(None, norm_fname_ascii, t_ascii).ratio()
                        if ratio > best_ratio and ratio >= 0.80:
                            best_ratio = ratio
                            best_date = date_val
                    if best_date:
                        upload_date = best_date

                if upload_date:
                    try:
                        ud = datetime.strptime(upload_date, "%Y%m%d")
                        ud_ts = ud.replace(hour=12).timestamp()
                        current_mtime = os.path.getmtime(filepath)
                        current_date = datetime.fromtimestamp(current_mtime).strftime("%Y%m%d")
                        if current_date != upload_date:
                            os.utime(filepath, (ud_ts, ud_ts))
                            fixed += 1
                        else:
                            already_correct += 1
                    except (ValueError, OSError):
                        unmatched += 1
                else:
                    unmatched += 1
                    unmatched_files.append(fname)

        # Log unmatched files for debugging
        if unmatched_files and not (_is_simple_mode):
            log(f"  Unmatched files ({len(unmatched_files)}):\n", "dim")
            for uf in unmatched_files[:20]:
                log(f"    - {uf}\n", "dim")
            if len(unmatched_files) > 20:
                log(f"    ... and {len(unmatched_files) - 20} more\n", "dim")

        parts = []
        if fixed:
            parts.append(f"{fixed:,} fixed")
        if already_correct:
            parts.append(f"{already_correct:,} already correct")
        if unmatched:
            parts.append(f"{unmatched:,} unmatched")
        log(f"  ✓ Date check complete: {', '.join(parts)}.\n", "simpleline_green")

    except Exception as e:
        log(f"  ⚠ Error fixing dates: {e}\n", "red")
    finally:
        if proc:
            cleanup_process(proc)


def _process_reorg_queue():
    """Process next queued reorganize if any. Returns True if one was started."""
    with _reorg_queue_lock:
        if not _reorg_queue:
            return False
        args = _reorg_queue.pop(0)
        remaining = len(_reorg_queue)

    ch_name, folder, t_years, t_months, ch_url, recheck = args
    with _queue_order_lock:
        try:
            _queue_order.remove(("reorg", ch_url or ch_name))
        except ValueError:
            pass
    if recheck:
        _current_job["label"] = f"Re-date & Organize {ch_name}"
    elif t_years:
        _current_job["label"] = f"Re-Organize {ch_name}"
    else:
        _current_job["label"] = f"Un-Organize {ch_name}"
    _update_queue_btn()
    log(f"\n=== Processing queued reorganize: {ch_name}", "header")
    if remaining:
        log(f" ({remaining} more in queue)", "header")
    log(f" ===\n", "header")
    _run_reorganize_auto(ch_name, folder, t_years, t_months, ch_url=ch_url, recheck_dates=recheck)
    return True


def _run_reorganize_auto(channel_name, folder_path, target_years, target_months, ch_url=None,
                         recheck_dates=False):
    """Trigger reorganization automatically after updating channel split settings."""
    global _reorg_running

    # If a reorg or sync is already running, queue this request
    if _reorg_running or _sync_running:
        with _reorg_queue_lock:
            if not any(q[0] == channel_name for q in _reorg_queue):
                _reorg_queue.append((channel_name, folder_path, target_years, target_months, ch_url, recheck_dates))
                with _queue_order_lock:
                    _queue_order.append(("reorg", ch_url or channel_name))
                log(f"\n=== Added {channel_name} reorganize to Sync List ===\n", "header")
            else:
                log(f"  ⚠ {channel_name} is already in the reorganize queue.\n", "simpleline")
        _update_queue_btn()
        return

    def _worker():
        global _reorg_running
        _reorg_running = True
        try:
            cancel_event.clear()
            _ui_queue.append(lambda: _update_queue_btn())
            if recheck_dates and ch_url:
                _fix_file_dates(ch_url, folder_path)
                if cancel_event.is_set():
                    return
            success = _reorganize_channel_folder(channel_name, folder_path, target_years, target_months,
                                                   trust_mtime=recheck_dates)
            if success and ch_url:
                _chan_ctx_update_split(ch_url, target_years, target_months)
        finally:
            _reorg_running = False
            def _reorg_done():
                _sync_task_finished()
                if _reorg_done_job["id"]:
                    try:
                        root.after_cancel(_reorg_done_job["id"])
                    except Exception:
                        pass
                reorg_done_label.pack(side="left", padx=(4, 0))
                _reorg_done_job["id"] = root.after(5000, lambda: reorg_done_label.pack_forget())
            _ui_queue.append(_reorg_done)
            # Process any queued jobs in insertion order
            if _skip_current.is_set():
                _skip_current.clear()
                cancel_event.clear()
                _process_next_queued()
            elif not cancel_event.is_set():
                _process_next_queued()

    threading.Thread(target=_worker, daemon=True).start()


# ─── Transcription Feature ──────────────────────────────────────────────────────

def _parse_vtt_to_text(vtt_path):
    """Parse a .vtt subtitle file into clean plain text."""
    import html as _html_mod
    try:
        with open(vtt_path, "r", encoding="utf-8") as f:
            raw = f.read()
    except Exception:
        return ""

    lines = raw.split("\n")
    clean = []
    prev = ""
    for line in lines:
        line = line.strip()
        # Skip WEBVTT header, NOTE lines, blank lines
        if not line or line.startswith("WEBVTT") or line.startswith("NOTE") or line.startswith("Kind:") or line.startswith("Language:"):
            continue
        # Skip timestamp lines (00:00:01.234 --> 00:00:04.567)
        if re.match(r'\d{2}:\d{2}', line) and '-->' in line:
            continue
        # Skip cue position markers
        if re.match(r'^\d+$', line):  # numeric cue IDs
            continue
        if 'align:' in line or 'position:' in line:
            continue
        # Strip HTML tags
        line = re.sub(r'<[^>]+>', '', line)
        # Decode HTML entities (&nbsp; &amp; &lt; etc.) to plain text
        line = _html_mod.unescape(line)
        # Normalize whitespace (non-breaking spaces, multiple spaces, etc.)
        line = re.sub(r'\s+', ' ', line).strip()
        if not line:
            continue
        # Deduplicate consecutive identical lines (YouTube auto-subs repeat)
        if line != prev:
            clean.append(line)
            prev = line

    return " ".join(clean)


def _parse_vtt_to_segments(vtt_path):
    """Parse a .vtt subtitle file into merged, deduplicated timestamped segments.

    YouTube VTT uses rolling captions where each cue repeats previous text and
    adds a few words.  This parser merges overlapping cues so the output has one
    clean segment per actual phrase with no duplication.

    Returns list of {"start": float_secs, "end": float_secs, "text": str}.
    """
    import html as _html_mod
    try:
        with open(vtt_path, "r", encoding="utf-8") as f:
            raw = f.read()
    except Exception:
        return []

    def _ts_to_secs(ts_str):
        """Convert 'HH:MM:SS.mmm' or 'MM:SS.mmm' to float seconds."""
        parts = ts_str.strip().split(":")
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
        return 0.0

    # Step 1: Parse raw cues from VTT
    raw_cues = []  # list of (start, end, text)
    lines = raw.split("\n")
    current_start = None
    current_end = None
    current_text = []

    for line in lines:
        line = line.strip()
        ts_match = re.match(r'(\d[\d:.]+)\s*-->\s*(\d[\d:.]+)', line)
        if ts_match:
            # Flush previous cue
            if current_text and current_start is not None:
                joined = " ".join(current_text)
                raw_cues.append((current_start, current_end, joined))
            current_start = _ts_to_secs(ts_match.group(1))
            current_end = _ts_to_secs(ts_match.group(2))
            current_text = []
            continue
        if not line or line.startswith("WEBVTT") or line.startswith("NOTE") or \
                line.startswith("Kind:") or line.startswith("Language:"):
            continue
        if re.match(r'^\d+$', line):
            continue
        if 'align:' in line or 'position:' in line:
            continue
        cleaned = re.sub(r'<[^>]+>', '', line)
        cleaned = _html_mod.unescape(cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        if cleaned:
            current_text.append(cleaned)

    # Flush last cue
    if current_text and current_start is not None:
        joined = " ".join(current_text)
        raw_cues.append((current_start, current_end, joined))

    if not raw_cues:
        return []

    # Step 2: Merge overlapping rolling cues.
    # YouTube auto-subs emit cues where each new cue's text starts with the
    # tail of the previous cue's text plus new words.  We detect overlap by
    # checking if the new cue's text starts with a suffix of the current
    # accumulated text, and if so, only keep the new words.
    segments = []
    seg_start = raw_cues[0][0]
    seg_end = raw_cues[0][1]
    seg_text = raw_cues[0][2]

    for i in range(1, len(raw_cues)):
        _s, _e, _t = raw_cues[i]

        # Check if this cue is a rolling continuation of the current segment.
        # A rolling cue's text typically starts with a suffix of seg_text.
        _is_overlap = False
        if seg_text and _t:
            # Check if the new cue text starts with the end of the accumulated text
            # Try matching the last N words of seg_text against the start of _t
            seg_words = seg_text.split()
            new_words = _t.split()
            # Try overlap lengths from large to small
            max_overlap = min(len(seg_words), len(new_words) - 1)
            for ol in range(max_overlap, 0, -1):
                if seg_words[-ol:] == new_words[:ol]:
                    # Found overlap — append only the new words
                    extra = " ".join(new_words[ol:])
                    if extra:
                        seg_text += " " + extra
                    seg_end = _e
                    _is_overlap = True
                    break

            # Also catch near-zero-duration "echo" cues (e.g., 805.91→805.92)
            # that just repeat the tail without adding new words
            if not _is_overlap and (_e - _s) < 0.1:
                _is_overlap = True  # skip it
                seg_end = max(seg_end, _e)

        if not _is_overlap:
            # Flush current segment and start a new one
            if seg_text.strip():
                segments.append({"start": seg_start, "end": seg_end, "text": seg_text.strip()})
            seg_start = _s
            seg_end = _e
            seg_text = _t

    # Flush final segment
    if seg_text.strip():
        segments.append({"start": seg_start, "end": seg_end, "text": seg_text.strip()})

    return segments


def _get_jsonl_path(txt_path):
    """Get the hidden JSONL file path corresponding to a transcript .txt path."""
    dirname = os.path.dirname(txt_path)
    basename = os.path.basename(txt_path)
    jsonl_name = "." + basename.replace("Transcript.txt", "Transcript.jsonl")
    return os.path.join(dirname, jsonl_name)


def _hide_file_win(path):
    """Set the hidden attribute on a file (Windows only)."""
    if os.name == "nt":
        try:
            import ctypes
            ctypes.windll.kernel32.SetFileAttributesW(path, 0x02)  # FILE_ATTRIBUTE_HIDDEN
        except Exception:
            pass


def _write_jsonl_entry(jsonl_path, video_id, title, segments):
    """Append JSONL lines for one video's timestamped segments."""
    import json as _json
    try:
        os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)
        with open(jsonl_path, "a", encoding="utf-8") as f:
            for seg in segments:
                line = _json.dumps({
                    "video_id": video_id or "",
                    "title": title,
                    "start": round(seg["start"], 2),
                    "end": round(seg["end"], 2),
                    "text": seg["text"]
                }, ensure_ascii=False)
                f.write(line + "\n")
        _hide_file_win(jsonl_path)
    except Exception:
        pass


def _scan_existing_jsonl(folder_path, ch_name):
    """Scan JSONL transcript files under folder_path. Return set of video titles already in JSONL."""
    existing = set()
    import json as _json
    for dirpath, _dirs, files in os.walk(folder_path):
        for f in files:
            if f.startswith(".") and ch_name in f and f.endswith("Transcript.jsonl"):
                try:
                    with open(os.path.join(dirpath, f), "r", encoding="utf-8") as fh:
                        for line in fh:
                            line = line.strip()
                            if line:
                                entry = _json.loads(line)
                                existing.add(entry.get("title", ""))
                except Exception:
                    pass
    return existing


# Path to Python 3.11 with CUDA PyTorch + Whisper installed
_WHISPER_PYTHON = r"C:\Users\Scott\AppData\Local\Programs\Python\Python311\python.exe"
_whisper_proc = None  # persistent Whisper subprocess (model stays loaded)
_whisper_model_choice = "large-v3"  # selected model — set via dialog before transcription
_ffmpeg_proc = None  # ffmpeg encode subprocess (for compression feature)

# Whisper helper script — runs under Python 3.11 with CUDA, stays alive accepting JSON requests
# Uses faster-whisper (CTranslate2 backend) for ~4x speedup + built-in VAD to prevent hallucination loops
_WHISPER_SCRIPT = r'''
import sys, json, os, io

# Save real stdout for our JSON protocol, redirect stdout/stderr to suppress
# any prints from huggingface_hub downloads, tqdm bars, or import warnings
_out = sys.stdout
sys.stdout = io.StringIO()
sys.stderr = io.StringIO()

# faster-whisper uses CTranslate2 — no PyTorch needed for inference
from faster_whisper import WhisperModel

_model_name = os.environ.get("WHISPER_MODEL", "large-v3")
_device = os.environ.get("WHISPER_DEVICE", "cuda")
_compute = os.environ.get("WHISPER_COMPUTE", "float16")

try:
    model = WhisperModel(_model_name, device=_device, compute_type=_compute)
except Exception:
    _device = "cpu"
    _compute = "default"
    model = WhisperModel(_model_name, device=_device, compute_type=_compute)

# Restore stderr for real errors during transcription, keep stdout suppressed
sys.stderr = sys.__stderr__

_out.write(json.dumps({"status": "ready", "device": _device}) + "\n")
_out.flush()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
        path = req.get("path", "")
        duration = req.get("duration", 0)
    except json.JSONDecodeError:
        path = line
        duration = 0
    if not path:
        continue
    try:
        segments_gen, info = model.transcribe(
            path,
            language="en",
            beam_size=5,
            vad_filter=True,
            vad_parameters=dict(min_silence_duration_ms=500),
            condition_on_previous_text=True,
            no_speech_threshold=0.6,
        )
        # info.duration is the total audio length in seconds
        total_dur = info.duration if info.duration and info.duration > 0 else duration
        all_segments = []
        last_pct = -1
        for seg in segments_gen:
            all_segments.append(seg)
            if total_dur > 0:
                pct = min(99, int(seg.end / total_dur * 100))
                if pct > last_pct:
                    last_pct = pct
                    _out.write(json.dumps({"status": "progress", "pct": pct}) + "\n")
                    _out.flush()

        text = " ".join(seg.text.strip() for seg in all_segments if seg.text.strip())
        seg_data = [{"s": round(seg.start, 2), "e": round(seg.end, 2), "t": seg.text.strip()}
                     for seg in all_segments if seg.text.strip()]
        _out.write(json.dumps({"status": "ok", "text": text, "segments": seg_data}) + "\n")
        _out.flush()
    except Exception as e:
        _out.write(json.dumps({"status": "error", "text": str(e)}) + "\n")
        _out.flush()
'''


# Punctuation helper script — runs under Python 3.11 with CUDA, stays alive accepting JSON requests
_PUNCT_SCRIPT = r'''
import sys, json, io, re, os, logging

_out = sys.stdout
sys.stdout = io.StringIO()
sys.stderr = io.StringIO()

os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"
os.environ["TRANSFORMERS_NO_ADVISORY_WARNINGS"] = "1"
os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
os.environ["TOKENIZERS_PARALLELISM"] = "false"

for _name in ("transformers", "huggingface_hub", "safetensors", "transformers.modeling_utils"):
    logging.getLogger(_name).setLevel(logging.ERROR)

try:
    from transformers import pipeline as tf_pipeline
    import torch

    device_str = "cuda" if torch.cuda.is_available() else "cpu"
    pipe = tf_pipeline("ner", "oliverguhr/fullstop-punctuation-multilang-large",
                       aggregation_strategy="none",
                       device=0 if device_str == "cuda" else -1)

    _out.write(json.dumps({"status": "ready", "device": device_str}) + "\n")
    _out.flush()
except Exception as e:
    _out.write(json.dumps({"status": "error", "text": str(e)}) + "\n")
    _out.flush()
    sys.exit(1)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
        text = req.get("text", "")
    except json.JSONDecodeError:
        continue
    if not text:
        _out.write(json.dumps({"status": "ok", "text": ""}) + "\n")
        _out.flush()
        continue

    try:
        cleaned = re.sub(r"(?<!\d)[.,;:!?](?!\d)", "", text)
        words = cleaned.split()
        if not words:
            _out.write(json.dumps({"status": "ok", "text": text}) + "\n")
            _out.flush()
            continue

        chunk_size = 230
        overlap = 5 if len(words) > chunk_size else 0

        def _chunk(lst, n, stride):
            for i in range(0, len(lst), n - stride):
                yield lst[i:i + n]

        batches = list(_chunk(words, chunk_size, overlap))
        if len(batches) > 1 and len(batches[-1]) <= overlap:
            batches.pop()

        tagged = []
        for batch in batches:
            ov = 0 if batch is batches[-1] else overlap
            text_chunk = " ".join(batch)
            result = pipe(text_chunk)
            char_index = 0
            result_index = 0
            for word in batch[:len(batch) - ov]:
                char_index += len(word) + 1
                label = "0"
                while result_index < len(result) and char_index > result[result_index]["end"]:
                    label = result[result_index]["entity"]
                    result_index += 1
                tagged.append((word, label))

        out = ""
        for word, label in tagged:
            out += word
            if label == "0":
                out += " "
            elif label in ".,?-:":
                out += label + " "
        out = out.strip()

        out = re.sub(r"([.!?]\s+)(\w)", lambda m: m.group(1) + m.group(2).upper(), out)
        if out:
            out = out[0].upper() + out[1:]

        _out.write(json.dumps({"status": "ok", "text": out}) + "\n")
        _out.flush()
    except Exception as e:
        _out.write(json.dumps({"status": "error", "text": str(e)}) + "\n")
        _out.flush()
'''


def _check_punct_installed():
    """Check if transformers is installed under Python 3.11."""
    if not os.path.exists(_WHISPER_PYTHON):
        return False
    try:
        result = subprocess.run(
            [_WHISPER_PYTHON, "-c", "from transformers import pipeline; print('ok')"],
            capture_output=True, text=True, timeout=30, startupinfo=startupinfo
        )
        return result.returncode == 0 and "ok" in result.stdout
    except Exception:
        return False


def _install_punct_blocking():
    """Install transformers under Python 3.11. Returns True on success."""
    if not os.path.exists(_WHISPER_PYTHON):
        log(f"  ⚠ Python 3.11 not found at {_WHISPER_PYTHON}\n", "red")
        return False
    try:
        log("  Installing transformers library for punctuation...\n", "simpleline")
        result = subprocess.run(
            [_WHISPER_PYTHON, "-m", "pip", "install", "transformers"],
            capture_output=True, text=True, timeout=600, startupinfo=startupinfo
        )
        if result.returncode == 0:
            log("  ✓ Punctuation library installed.\n", "simpleline_green")
            return True
        else:
            log(f"  ⚠ Punctuation library install failed: {result.stderr[:300]}\n", "red")
            return False
    except Exception as e:
        log(f"  ⚠ Punctuation library install error: {e}\n", "red")
        return False


def _start_punct_process():
    """Start the persistent punctuation subprocess under Python 3.11. Returns True if ready."""
    global _punct_proc
    if _punct_proc is not None and _punct_proc.poll() is None:
        return True  # Already running

    try:
        log("  Loading punctuation model...\n", "simpleline")
        _punct_proc = subprocess.Popen(
            [_WHISPER_PYTHON, "-c", _PUNCT_SCRIPT],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1, startupinfo=startupinfo
        )
        ready_line = _punct_proc.stdout.readline().strip()
        if ready_line:
            info = json.loads(ready_line)
            if info.get("status") == "ready":
                log(f"  ✓ Punctuation model loaded ({info.get('device', '?').upper()}).\n", "simpleline_green")
                return True
            elif info.get("status") == "error":
                log(f"  ⚠ Punctuation model failed: {info.get('text', 'unknown')}\n", "red")
                _stop_punct_process()
                return False
        log("  ⚠ Punctuation process did not start correctly.\n", "red")
        _stop_punct_process()
        return False
    except Exception as e:
        log(f"  ⚠ Failed to start punctuation process: {e}\n", "red")
        _stop_punct_process()
        return False


def _stop_punct_process():
    """Stop the persistent punctuation subprocess."""
    global _punct_proc
    if _punct_proc is not None:
        try:
            _punct_proc.stdin.close()
            _punct_proc.terminate()
            _punct_proc.wait(timeout=10)
        except Exception:
            try:
                _punct_proc.kill()
            except Exception:
                pass
        _punct_proc = None


def _check_whisper_installed():
    """Check if faster-whisper + CUDA CTranslate2 is installed under Python 3.11."""
    if not os.path.exists(_WHISPER_PYTHON):
        return False
    try:
        result = subprocess.run(
            [_WHISPER_PYTHON, "-c",
             "from faster_whisper import WhisperModel; import ctranslate2; print(ctranslate2.get_cuda_device_count() > 0)"],
            capture_output=True, text=True, timeout=30, startupinfo=startupinfo
        )
        return result.returncode == 0 and "True" in result.stdout
    except Exception:
        return False


def _install_whisper_blocking():
    """Install faster-whisper + CUDA PyTorch (for punctuation) under Python 3.11. Returns True on success."""
    if not os.path.exists(_WHISPER_PYTHON):
        log(f"  ⚠ Python 3.11 not found at {_WHISPER_PYTHON}\n", "red")
        return False
    try:
        # Step 0: Remove old openai-whisper if installed (cleanup, saves disk space)
        subprocess.run(
            [_WHISPER_PYTHON, "-m", "pip", "uninstall", "-y", "openai-whisper"],
            capture_output=True, text=True, timeout=120, startupinfo=startupinfo
        )

        # Step 1: Install CUDA PyTorch (still needed for punctuation subprocess)
        log("  Installing CUDA PyTorch under Python 3.11...\n", "simpleline")
        torch_result = subprocess.run(
            [_WHISPER_PYTHON, "-m", "pip", "install",
             "torch", "torchvision", "torchaudio",
             "--index-url", "https://download.pytorch.org/whl/cu121"],
            capture_output=True, text=True, timeout=900, startupinfo=startupinfo
        )
        if torch_result.returncode == 0:
            log("  ✓ CUDA PyTorch installed.\n", "simpleline_green")
        else:
            log(f"  ⚠ CUDA PyTorch install failed: {torch_result.stderr[:300]}\n", "red")
            return False

        # Step 2: Install faster-whisper (CTranslate2 backend — ~4x faster than openai-whisper)
        log("  Installing faster-whisper...\n", "simpleline")
        result = subprocess.run(
            [_WHISPER_PYTHON, "-m", "pip", "install", "faster-whisper"],
            capture_output=True, text=True, timeout=600, startupinfo=startupinfo
        )
        if result.returncode == 0:
            log("  ✓ faster-whisper installed successfully.\n", "simpleline_green")
            return True
        else:
            log(f"  ⚠ faster-whisper install failed: {result.stderr[:500]}\n", "red")
            return False
    except Exception as e:
        log(f"  ⚠ Whisper install error: {e}\n", "red")
        return False


def _start_whisper_process():
    """Start the persistent Whisper subprocess under Python 3.11. Returns True if ready."""
    global _whisper_proc
    if _whisper_proc is not None and _whisper_proc.poll() is None:
        return True  # Already running

    try:
        _model = _whisper_model_choice
        _hf_cache = os.path.join(os.environ.get("HF_HOME", os.path.join(os.path.expanduser("~"), ".cache", "huggingface")), "hub")
        _model_cached = os.path.isdir(os.path.join(_hf_cache, f"models--Systran--faster-whisper-{_model}"))
        _dl_hint = "" if _model_cached else " (first run downloads model)"
        log(f"  Transcribing - Loading Whisper model ({_model}) on GPU...{_dl_hint}\n", "transcribe_using")
        _env = os.environ.copy()
        _env["WHISPER_MODEL"] = _model
        _env["WHISPER_DEVICE"] = "cuda"
        _env["WHISPER_COMPUTE"] = "float16"
        _whisper_proc = subprocess.Popen(
            [_WHISPER_PYTHON, "-c", _WHISPER_SCRIPT],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1, startupinfo=startupinfo, env=_env
        )
        # Wait for "ready" message (model loading)
        ready_line = _whisper_proc.stdout.readline().strip()
        if ready_line:
            import json as _json
            info = _json.loads(ready_line)
            if info.get("status") == "ready":
                log(f"  ✓ Whisper model loaded ({_model}, {info.get('device', '?').upper()}).\n", "simpleline_green")
                return True
        log("  ⚠ Whisper process did not start correctly.\n", "red")
        return False
    except Exception as e:
        log(f"  ⚠ Failed to start Whisper process: {e}\n", "red")
        return False


def _stop_whisper_process():
    """Stop the persistent Whisper subprocess."""
    global _whisper_proc
    if _whisper_proc is not None:
        try:
            _whisper_proc.stdin.close()
            _whisper_proc.terminate()
            _whisper_proc.wait(timeout=10)
        except Exception:
            try:
                _whisper_proc.kill()
            except Exception:
                pass
        _whisper_proc = None


def _stop_ffmpeg_process():
    """Stop the ffmpeg encode subprocess if running."""
    global _ffmpeg_proc
    if _ffmpeg_proc is not None:
        try:
            _ffmpeg_proc.terminate()
            _ffmpeg_proc.wait(timeout=10)
        except Exception:
            try:
                _ffmpeg_proc.kill()
            except Exception:
                pass
        _ffmpeg_proc = None


def _ffprobe_duration(file_path):
    """Get video duration in seconds via ffprobe. Returns float or 0 on error."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", file_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            encoding="utf-8", errors="replace", startupinfo=startupinfo, timeout=30
        )
        return float(result.stdout.strip())
    except Exception:
        return 0


def _fmt_enc_size(mb):
    """Format a compressed file size in MB, showing GB (2 decimal places) if ≥ 1024 MB."""
    if mb is None or mb < 0:
        return "?"
    if mb >= 1024:
        return f"{mb / 1024:.2f} GB"
    return f"{mb:.1f} MB"


def _ffprobe_is_compressed(file_path):
    """Check if a file has the ytarchiver_compressed metadata marker."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format_tags=comment",
             "-of", "csv=p=0", file_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            encoding="utf-8", errors="replace", startupinfo=startupinfo, timeout=30
        )
        return "ytarchiver_compressed=1" in result.stdout
    except Exception:
        return False


def _compress_channel(ch_name, ch_url, folder, bitrate_mbhr, split_years, split_months,
                      output_res="", cancel_ev=None, pause_ev=None, _sync_mode=False,
                      target_paths=None, batch_num=None):
    """Compress all un-compressed video files in a channel folder using ffmpeg AV1 NVENC.

    bitrate_mbhr: target file size in MB per hour of video.
    output_res: optional target height (e.g. "360", "720"). Empty = keep original.
    cancel_ev/pause_ev: optional threading.Events.
    _sync_mode: if True, run directly (blocking) instead of spawning a thread.
    """
    global _ffmpeg_proc, _gpu_actively_encoding

    _VIDEO_EXTS_COMPRESS = (".mp4", ".mkv", ".webm", ".avi", ".mov", ".flv", ".wmv", ".m4v")

    def _worker():
        global _ffmpeg_proc
        _ce = cancel_ev or cancel_event
        _pe = pause_ev or pause_event

        if not _is_simple_mode:
            log(f"\n=== Compressing: {ch_name} ===\n", "header")
            _res_label = f"{output_res}p" if output_res else "Original"
            log(f"  Target: {bitrate_mbhr} MB/hr  |  Output: {_res_label}\n", "simpleline")

        # ── Step 1: Scan for video files ──
        if target_paths:
            local_files = {}
            for tp in target_paths:
                if os.path.exists(tp) and any(tp.lower().endswith(ext) for ext in _VIDEO_EXTS_COMPRESS):
                    if "_TEMP_COMPRESS" not in tp and ".temp." not in tp.lower() and ".part" not in tp.lower():
                        local_files[os.path.basename(tp)] = tp
            if not local_files:
                log("  No target video files found for this batch.\n", "simpleline")
                return
        else:
            local_files = {}
            for dirpath, _, files in os.walk(folder):
                for f in files:
                    if f.lower().endswith(_VIDEO_EXTS_COMPRESS):
                        # Skip temp files from previous interrupted runs or partial downloads
                        if "_TEMP_COMPRESS" in f or ".temp." in f.lower() or ".part" in f.lower():
                            continue
                        fpath = os.path.join(dirpath, f)
                        local_files[f] = fpath

            if not local_files:
                log("  No video files found to compress.\n", "simpleline")
                return

        if not _is_simple_mode:
            log(f"  Found {len(local_files)} video file(s).\n", "simpleline")

        # ── Step 2: Filter out already-compressed files ──
        files_to_compress = {}
        skipped = 0
        for fname, fpath in local_files.items():
            if _ce.is_set():
                log(f"\n  ⛔ Compression cancelled.\n", "red")
                return
            if _ffprobe_is_compressed(fpath):
                skipped += 1
            else:
                files_to_compress[fname] = fpath

        if not _is_simple_mode:
            if skipped:
                log(f"  {skipped} file(s) already compressed — skipping.\n", "simpleline")
        if not files_to_compress:
            log(f"  ✓ All videos already compressed!\n", "simpleline_green")
            return

        if _is_simple_mode:
            log(f"\n=== Compressing: {ch_name}, {len(files_to_compress)} files ===\n", "header")
        else:
            log(f"  {len(files_to_compress)} file(s) to compress.\n", "simpleline")

        # ── Step 3: Calculate target bitrate ──
        # Convert MB/hr to kbps: MB/hr * 1024 * 8 / 3600
        target_total_kbps = (bitrate_mbhr * 1024 * 8) / 3600
        audio_kbps = 128  # fixed AAC audio bitrate
        video_kbps = max(int(target_total_kbps - audio_kbps), 50)  # minimum 50 kbps video

        if not _is_simple_mode:
            log(f"  Video bitrate: ~{video_kbps} kbps (VBR), Audio: {audio_kbps} kbps\n", "simpleline")

        # ── Step 4: Process each file ──
        done_count = 0
        err_count = 0
        total = len(files_to_compress)
        t_start = time.time()

        for idx, (fname, fpath) in enumerate(files_to_compress.items(), 1):
            # Pause check
            if _pe.is_set() and not _ce.is_set():
                log(f"\n  ⏸ Compression paused.\n", "pauselog")
                while _pe.is_set() and not _ce.is_set():
                    time.sleep(0.25)
                if not _ce.is_set():
                    log(f"  ▶ Compression resuming...\n", "pauselog")

            if _ce.is_set():
                log(f"\n  ⛔ Compression cancelled.\n", "red")
                break

            # Get duration for progress reporting
            duration = _ffprobe_duration(fpath)
            if duration:
                _d_tot_min = int(duration // 60)
                if _d_tot_min >= 60:
                    dur_str = f"{_d_tot_min // 60}h{_d_tot_min % 60:02d}m"
                else:
                    dur_str = f"{_d_tot_min}m{int(duration % 60):02d}s"
            else:
                dur_str = "?"

            fname_short = fname if len(fname) <= 50 else fname[:47] + "..."
            if not _is_simple_mode:
                log(f"\n  [{idx}/{total}] {fname_short} ({dur_str})\n", "simpleline")

            # Build temp output path
            base, ext = os.path.splitext(fpath)
            temp_path = base + "_TEMP_COMPRESS.mp4"

            # Build ffmpeg command
            cmd = [
                "ffmpeg", "-y", "-i", fpath,
            ]
            if output_res:
                cmd += ["-vf", f"scale=-2:{output_res}"]
            cmd += [
                "-c:v", "av1_nvenc",
                "-rc", "vbr",
                "-cq", "32",
                "-b:v", f"{video_kbps}k",
                "-maxrate", f"{int(video_kbps * 1.5)}k",
                "-preset", "p6",
                "-multipass", "2",
                "-c:a", "aac", "-b:a", f"{audio_kbps}k",
                "-movflags", "+faststart",
                "-metadata", "comment=ytarchiver_compressed=1",
                temp_path
            ]

            try:
                proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    startupinfo=startupinfo, encoding="utf-8", errors="replace"
                )
                _ffmpeg_proc = proc
                _gpu_actively_encoding = True
                _encode_t0 = time.time()

                # Show a loading state while ffmpeg initializes (can take several minutes)
                if _is_simple_mode:
                    _load_info = f"[{idx}/{total}] ENCODING: {fname_short} ({dur_str}) - " if duration > 0 else f"[{idx}/{total}] ENCODING: {fname_short} - "
                    _update_encode_progress(f"{_load_info}loading\n")

                # Parse progress from stderr
                _time_re = re.compile(r'time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})')
                last_pct = -1
                _dot_idx = 0
                _EDOTS = [".", "..", "..."]
                for line in proc.stderr:
                    if _ce.is_set():
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except Exception:
                            proc.kill()
                        break

                    m = _time_re.search(line)
                    if m and duration > 0:
                        h, mi, s, cs = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
                        elapsed = h * 3600 + mi * 60 + s + cs / 100
                        pct = min(int(elapsed / duration * 100), 100)
                        if pct != last_pct:
                            last_pct = pct
                            _dot_idx += 1
                            _d = _EDOTS[_dot_idx % 3]
                            _vid_info = f"[{idx}/{total}] ENCODING: {fname_short} ({dur_str}) - " if _is_simple_mode else ""
                            if _pe.is_set():
                                _update_encode_progress(f"{_vid_info}{pct}% — will pause after this video{_d}\n")
                            else:
                                _update_encode_progress(f"{_vid_info}{pct}%{_d}\n")
                    elif duration <= 0:
                        _dot_idx += 1
                        _d = _EDOTS[_dot_idx % 3]
                        _vid_info = f"[{idx}/{total}] ENCODING: {fname_short} - " if _is_simple_mode else ""
                        if _pe.is_set():
                            _update_encode_progress(f"{_vid_info}— will pause after this video{_d}\n")
                        else:
                            _update_encode_progress(f"{_vid_info}{_d}\n")

                try:
                    proc.wait(timeout=300)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=10)
                _ffmpeg_proc = None
                _gpu_actively_encoding = False
                _clear_encode_progress()
                _encode_elapsed = time.time() - _encode_t0

                if _ce.is_set():
                    # Clean up temp file on cancel
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    except Exception:
                        pass
                    break

                # Format elapsed time
                _em = int(_encode_elapsed) // 60
                _es = int(_encode_elapsed) % 60
                _elapsed_str = f"{_em}m{_es:02d}s" if _em > 0 else f"{_es}s"

                if proc.returncode == 0 and os.path.exists(temp_path):
                    # Get sizes for logging
                    orig_size = os.path.getsize(fpath)
                    new_size = os.path.getsize(temp_path)
                    ratio = (1 - new_size / orig_size) * 100 if orig_size > 0 else 0

                    # Atomic replace; preserve original upload-date mtime (ffmpeg creates a
                    # new file with today's timestamp, which would lose the YT upload date)
                    try:
                        orig_stat = os.stat(fpath)
                        os.replace(temp_path, fpath)
                        try:
                            os.utime(fpath, (orig_stat.st_atime, orig_stat.st_mtime))
                        except OSError:
                            pass
                        done_count += 1
                        orig_mb = orig_size / (1024 * 1024)
                        new_mb = new_size / (1024 * 1024)
                        _rt_str = f", {duration / _encode_elapsed:.1f}x realtime" if duration > 0 and _encode_elapsed > 0 else ""
                        if _is_simple_mode:
                            log(f"    [{idx}/{total}] {fname_short} ({dur_str})\n", "simpleline_blue")
                        log(f"    ✓ {_fmt_enc_size(orig_mb)} → {_fmt_enc_size(new_mb)} ({ratio:.0f}% smaller, took {_elapsed_str}{_rt_str})\n", "simpleline_blue")
                    except Exception as e:
                        err_count += 1
                        log(f"    ⚠ Replace failed: {e}\n", "red")
                        try:
                            os.remove(temp_path)
                        except Exception:
                            pass
                else:
                    err_count += 1
                    log(f"    ⚠ ffmpeg failed (exit code {proc.returncode}, took {_elapsed_str})\n", "red")
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    except Exception:
                        pass

            except FileNotFoundError:
                err_count += 1
                log(f"    ⚠ ffmpeg not found — cannot compress.\n", "red")
                break
            except Exception as e:
                err_count += 1
                _ffmpeg_proc = None
                log(f"    ⚠ Compression error: {e}\n", "red")
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except Exception:
                    pass

        # ── Summary ──
        elapsed = time.time() - t_start
        if not _ce.is_set():
            _e_hrs, _e_rem = divmod(int(elapsed), 3600)
            _e_mins, _e_secs = divmod(_e_rem, 60)
            if _e_hrs:
                _t_str = f"{_e_hrs}h {_e_mins}m {_e_secs}s"
            elif _e_mins:
                _t_str = f"{_e_mins}m {_e_secs}s"
            else:
                _t_str = f"{_e_secs}s"
            log(f"\n  ✓ Compression complete: {done_count} done", "simpleline_green")
            if err_count:
                log(f", {err_count} errors", "simpleline_green")
            log(f" ({_t_str})\n", "simpleline_green")
            _record_compression(ch_name, done_count, err_count, elapsed, batch_num)

    if _sync_mode:
        _worker()
    else:
        threading.Thread(target=_worker, daemon=True).start()


def _backlog_compress_channel(ch_name, ch_url, folder, resolution, bitrate_mbhr, output_res,
                              split_years, split_months, batch_size=20,
                              cancel_ev=None, pause_ev=None, _sync_mode=False):
    """Re-download and compress existing videos in a channel folder.

    Fetches the YouTube video list, matches local files by title→video ID,
    then processes in batches: download at new quality → ffmpeg AV1 NVENC → replace.
    Shows batch size stats and prompts user to confirm after first batch.
    """
    global _ffmpeg_proc, _gpu_actively_encoding

    _VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".avi", ".mov", ".flv", ".wmv", ".m4v")

    def _worker():
        global _ffmpeg_proc
        _ce = cancel_ev or cancel_event
        _pe = pause_ev or pause_event
        _current_bitrate = bitrate_mbhr
        _current_output_res = output_res

        log(f"\n=== Backlog Compress: {ch_name} ===\n", "header")
        _res_key = _current_output_res if _current_output_res in _COMPRESS_PRESETS else "1080"
        _presets_for_res = _COMPRESS_PRESETS.get(_res_key, _COMPRESS_PRESETS["1080"])
        _lvl_name = next((k for k, v in _presets_for_res.items() if v == _current_bitrate), f"{_current_bitrate} MB/hr")
        _res_label = f"{_current_output_res}p" if _current_output_res else "Original"
        log(f"  Quality: {_lvl_name} ({_current_bitrate} MB/hr)  |  Output: {_res_label}  |  Download: {resolution}\n", "simpleline")

        # ── Step 1: Scan local video files ──
        local_files = {}
        for dirpath, dirnames, files in os.walk(folder):
            dirnames[:] = [d for d in dirnames if d != "_BACKLOG_TEMP"]
            for f in files:
                if f.lower().endswith(_VIDEO_EXTS):
                    if "_TEMP_COMPRESS" in f or "_BACKLOG_TEMP" in f or ".temp." in f.lower() or ".part" in f.lower():
                        continue
                    fpath = os.path.join(dirpath, f)
                    local_files[f] = fpath

        if not local_files:
            log("  No video files found.\n", "simpleline")
            return

        log(f"  Found {len(local_files)} local video file(s).\n", "simpleline")

        # ── Step 2: Filter already-compressed ──
        files_to_process = {}
        skipped_compressed = 0
        for fname, fpath in local_files.items():
            if _ce.is_set():
                return
            if _ffprobe_is_compressed(fpath):
                skipped_compressed += 1
            else:
                files_to_process[fname] = fpath

        if skipped_compressed:
            log(f"  {skipped_compressed} file(s) already compressed — skipping.\n", "simpleline")
        if not files_to_process:
            log(f"  ✓ All videos already compressed!\n", "simpleline_green")
            return

        log(f"  {len(files_to_process)} file(s) to re-download and compress.\n", "simpleline")

        if _ce.is_set():
            return

        # ── Step 3: Fetch YouTube video list for ID matching ──
        log("  Fetching YouTube video list for ID matching...\n", "simpleline")
        yt_title_to_id = {}
        try:
            enum_cmd = [
                "yt-dlp", "--flat-playlist",
                "--print", "%(id)s|||%(title)s",
                "--no-warnings",
                "--cookies-from-browser", "firefox",
                ch_url
            ]
            enum_proc = subprocess.run(enum_cmd, capture_output=True, text=True, timeout=600,
                                       startupinfo=startupinfo)
            for line in enum_proc.stdout.strip().split("\n"):
                if "|||" in line:
                    vid_id, yt_title = line.strip().split("|||", 1)
                    yt_title_to_id[yt_title.strip()] = vid_id.strip()
            log(f"  Found {len(yt_title_to_id)} video(s) on YouTube.\n", "simpleline")
        except Exception as e:
            log(f"  ⚠ Could not fetch YouTube list: {e}\n", "red")
            return

        if _ce.is_set():
            return

        # Build normalized lookup: norm_title -> (video_id, yt_title)
        norm_lookup = {}
        for yt_title, vid_id in yt_title_to_id.items():
            norm = re.sub(r'[^\w]', '', unicodedata.normalize('NFC', yt_title.lower()))
            norm_lookup[norm] = (vid_id, yt_title)
            norm_a = _norm_ascii(yt_title)
            if norm_a != norm and norm_a not in norm_lookup:
                norm_lookup[norm_a] = (vid_id, yt_title)

        # ── Step 4: Match local files to video IDs ──
        work_list = []  # list of (video_id, original_path, original_size)
        skipped_no_match = 0
        for fname, fpath in files_to_process.items():
            base = os.path.splitext(fname)[0]
            base_nfc = unicodedata.normalize('NFC', base)
            norm_fname = re.sub(r'[^\w]', '', base_nfc.lower())
            norm_fname_ascii = _norm_ascii(base_nfc)

            match = norm_lookup.get(norm_fname)
            if not match and norm_fname_ascii != norm_fname:
                match = norm_lookup.get(norm_fname_ascii)
            # Prefix match for truncated filenames (--trim-filenames)
            if not match and len(norm_fname) >= 15:
                for norm_title, val in norm_lookup.items():
                    if norm_title.startswith(norm_fname) or norm_fname.startswith(norm_title[:len(norm_fname)]):
                        match = val
                        break
            if not match and len(norm_fname_ascii) >= 10:
                for norm_title, val in norm_lookup.items():
                    if norm_title.startswith(norm_fname_ascii) or norm_fname_ascii.startswith(norm_title[:len(norm_fname_ascii)]):
                        match = val
                        break

            if match:
                orig_size = os.path.getsize(fpath)
                work_list.append((match[0], fpath, orig_size))
            else:
                skipped_no_match += 1

        if skipped_no_match:
            log(f"  ⚠ {skipped_no_match} file(s) could not be matched to YouTube videos.\n", "red")
        if not work_list:
            log(f"  No files to process after matching.\n", "simpleline")
            return

        log(f"  Matched {len(work_list)} file(s) for backlog processing.\n", "simpleline")

        # ── Step 5: Process in batches ──
        temp_dir = os.path.join(folder, "_BACKLOG_TEMP")
        total_batches = (len(work_list) + batch_size - 1) // batch_size
        total_orig_bytes = 0
        total_new_bytes = 0
        total_done = 0
        total_errors = 0
        fmt = build_format_string(resolution)
        first_batch = True
        t_start = time.time()

        for batch_num in range(total_batches):
            if _ce.is_set():
                log(f"\n  ⛔ Backlog cancelled.\n", "red")
                break

            # Pause check
            if _pe.is_set() and not _ce.is_set():
                log(f"\n  ⏸ Backlog paused.\n", "pauselog")
                while _pe.is_set() and not _ce.is_set():
                    time.sleep(0.25)
                if not _ce.is_set():
                    log(f"  ▶ Backlog resuming...\n", "pauselog")

            batch_start = batch_num * batch_size
            batch_end = min(batch_start + batch_size, len(work_list))
            batch = work_list[batch_start:batch_end]

            _process_batch_result = [None]  # [True=continue, False=redo, None=pending]

            def _process_batch(_bitrate, _out_res):
                """Download, compress, replace one batch. Returns (batch_orig, batch_new, errors)."""
                os.makedirs(temp_dir, exist_ok=True)
                batch_orig = sum(item[2] for item in batch)
                batch_new = 0
                batch_errors = 0

                # Calculate target bitrate
                target_total_kbps = (_bitrate * 1024 * 8) / 3600
                audio_kbps = 128
                video_kbps = max(int(target_total_kbps - audio_kbps), 50)

                for idx, (vid_id, orig_path, orig_size) in enumerate(batch):
                    if _ce.is_set():
                        break

                    if _pe.is_set() and not _ce.is_set():
                        log(f"\n  ⏸ Backlog paused.\n", "pauselog")
                        while _pe.is_set() and not _ce.is_set():
                            time.sleep(0.25)
                        if not _ce.is_set():
                            log(f"  ▶ Backlog resuming...\n", "pauselog")

                    orig_fname = os.path.basename(orig_path)
                    fname_short = orig_fname if len(orig_fname) <= 50 else orig_fname[:47] + "..."
                    log(f"\n  [{batch_start + idx + 1}/{len(work_list)}] {fname_short}\n", "simpleline")

                    # Download at new quality
                    dl_path = os.path.join(temp_dir, f"{vid_id}.mp4")
                    vid_url = f"https://www.youtube.com/watch?v={vid_id}"
                    dl_cmd = [
                        "yt-dlp", "--newline", "--no-quiet",
                        "--trim-filenames", "200",
                        "--format", fmt, "--merge-output-format", "mp4",
                        "--ppa", "Merger:-c copy",
                        "--output", dl_path,
                        "--cookies-from-browser", "firefox",
                        "--no-download-archive",
                        vid_url
                    ]

                    log(f"    Downloading ({resolution})...\n", "simpleline")
                    try:
                        dl_proc = spawn_yt_dlp(dl_cmd)
                        with proc_lock:
                            active_processes.append(dl_proc)
                        for line in dl_proc.stdout:
                            if _ce.is_set():
                                dl_proc.terminate()
                                break
                        dl_proc.wait()
                        with proc_lock:
                            if dl_proc in active_processes:
                                active_processes.remove(dl_proc)
                        if _ce.is_set():
                            break
                        if dl_proc.returncode != 0 or not os.path.exists(dl_path):
                            # yt-dlp may produce a file with different extension after merge
                            found_dl = None
                            for f_temp in os.listdir(temp_dir):
                                if f_temp.startswith(vid_id) and not f_temp.endswith("_compressed.mp4"):
                                    found_dl = os.path.join(temp_dir, f_temp)
                                    break
                            if found_dl:
                                dl_path = found_dl
                            else:
                                log(f"    ⚠ Download failed.\n", "red")
                                batch_errors += 1
                                continue
                    except Exception as e:
                        log(f"    ⚠ Download error: {e}\n", "red")
                        batch_errors += 1
                        continue

                    # Compress with ffmpeg
                    compressed_path = os.path.join(temp_dir, f"{vid_id}_compressed.mp4")

                    # Get duration for progress reporting
                    _bl_duration = _ffprobe_duration(dl_path)

                    ffmpeg_cmd = ["ffmpeg", "-y", "-i", dl_path]
                    if _out_res:
                        ffmpeg_cmd += ["-vf", f"scale=-2:{_out_res}"]
                    ffmpeg_cmd += [
                        "-c:v", "av1_nvenc",
                        "-rc", "vbr",
                        "-cq", "32",
                        "-b:v", f"{video_kbps}k",
                        "-maxrate", f"{int(video_kbps * 1.5)}k",
                        "-preset", "p6",
                        "-multipass", "2",
                        "-c:a", "aac", "-b:a", f"{audio_kbps}k",
                        "-movflags", "+faststart",
                        "-metadata", "comment=ytarchiver_compressed=1",
                        compressed_path
                    ]

                    try:
                        proc = subprocess.Popen(
                            ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            startupinfo=startupinfo, encoding="utf-8", errors="replace"
                        )
                        _ffmpeg_proc = proc
                        _gpu_actively_encoding = True
                        _bl_encode_t0 = time.time()

                        # Compute vid info strings for progress display and loading state
                        if _is_simple_mode:
                            _bl_total = len(work_list)
                            _bl_idx = batch_start + idx + 1
                            _bl_dur_short = f"{int(_bl_duration // 60)}m{int(_bl_duration % 60):02d}s" if _bl_duration > 0 else "?"
                            _bl_vid_info_dur = f"[{_bl_idx}/{_bl_total}] ENCODING: {fname_short} ({_bl_dur_short}) - "
                            _bl_vid_info_nodur = f"[{_bl_idx}/{_bl_total}] ENCODING: {fname_short} - "
                            # Show loading state while ffmpeg initializes (issue #50)
                            _bl_load_info = _bl_vid_info_dur if _bl_duration > 0 else _bl_vid_info_nodur
                            _update_encode_progress(f"{_bl_load_info}loading\n")
                        else:
                            _bl_vid_info_dur = ""
                            _bl_vid_info_nodur = ""

                        _bl_time_re = re.compile(r'time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})')
                        _bl_last_pct = -1
                        _bl_dot_idx = 0
                        _EDOTS = [".", "..", "..."]
                        for line in proc.stderr:
                            if _ce.is_set():
                                proc.terminate()
                                try:
                                    proc.wait(timeout=5)
                                except Exception:
                                    proc.kill()
                                break

                            _bl_m = _bl_time_re.search(line)
                            if _bl_m and _bl_duration > 0:
                                _bl_h, _bl_mi, _bl_s, _bl_cs = int(_bl_m.group(1)), int(_bl_m.group(2)), int(_bl_m.group(3)), int(_bl_m.group(4))
                                _bl_elapsed = _bl_h * 3600 + _bl_mi * 60 + _bl_s + _bl_cs / 100
                                _bl_pct = min(int(_bl_elapsed / _bl_duration * 100), 100)
                                if _bl_pct != _bl_last_pct:
                                    _bl_last_pct = _bl_pct
                                    _bl_dot_idx += 1
                                    _d = _EDOTS[_bl_dot_idx % 3]
                                    if _pe.is_set():
                                        _update_encode_progress(f"{_bl_vid_info_dur}{_bl_pct}% — will pause after this video{_d}\n")
                                    else:
                                        _update_encode_progress(f"{_bl_vid_info_dur}{_bl_pct}%{_d}\n")
                            elif _bl_duration <= 0:
                                _bl_dot_idx += 1
                                _d = _EDOTS[_bl_dot_idx % 3]
                                if _pe.is_set():
                                    _update_encode_progress(f"{_bl_vid_info_nodur}— will pause after this video{_d}\n")
                                else:
                                    _update_encode_progress(f"{_bl_vid_info_nodur}{_d}\n")

                        if not _ce.is_set():
                            try:
                                proc.wait(timeout=300)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                proc.wait(timeout=10)
                        _ffmpeg_proc = None
                        _gpu_actively_encoding = False
                        _clear_encode_progress()
                        _bl_encode_elapsed = time.time() - _bl_encode_t0

                        if _ce.is_set():
                            break

                        # Format elapsed time
                        _bl_em = int(_bl_encode_elapsed) // 60
                        _bl_es = int(_bl_encode_elapsed) % 60
                        _bl_elapsed_str = f"{_bl_em}m{_bl_es:02d}s" if _bl_em > 0 else f"{_bl_es}s"

                        if proc.returncode == 0 and os.path.exists(compressed_path):
                            new_size = os.path.getsize(compressed_path)
                            # Replace original file
                            try:
                                os.replace(compressed_path, orig_path)
                                batch_new += new_size
                                o_mb = orig_size / (1024 * 1024)
                                n_mb = new_size / (1024 * 1024)
                                _bl_ratio = (1 - new_size / orig_size) * 100 if orig_size > 0 else 0
                                _bl_dur_str = f"{int(_bl_duration // 60)}m{int(_bl_duration % 60):02d}s" if _bl_duration > 0 else "?"
                                _bl_rt_str = f", {_bl_duration / _bl_encode_elapsed:.1f}x realtime" if _bl_duration > 0 and _bl_encode_elapsed > 0 else ""
                                if _is_simple_mode:
                                    log(f"    [{batch_start + idx + 1}/{len(work_list)}] {fname_short} ({_bl_dur_str})\n", "simpleline_blue")
                                log(f"    ✓ {_fmt_enc_size(o_mb)} → {_fmt_enc_size(n_mb)} ({_bl_ratio:.0f}% smaller, took {_bl_elapsed_str}{_bl_rt_str})\n", "simpleline_blue")
                            except Exception as e:
                                log(f"    ⚠ Replace failed: {e}\n", "red")
                                batch_errors += 1
                        else:
                            log(f"    ⚠ Encode failed (took {_bl_elapsed_str}).\n", "red")
                            batch_errors += 1
                    except FileNotFoundError:
                        log(f"    ⚠ ffmpeg not found.\n", "red")
                        batch_errors += 1
                        break
                    except Exception as e:
                        _ffmpeg_proc = None
                        log(f"    ⚠ Encode error: {e}\n", "red")
                        batch_errors += 1
                    finally:
                        # Clean up downloaded temp file
                        try:
                            if os.path.exists(dl_path):
                                os.remove(dl_path)
                        except Exception:
                            pass
                        try:
                            if os.path.exists(compressed_path):
                                os.remove(compressed_path)
                        except Exception:
                            pass

                return batch_orig, batch_new, batch_errors

            # Process the batch (may loop if user wants to redo with new settings)
            while True:
                if _ce.is_set():
                    break

                batch_orig, batch_new, batch_errors = _process_batch(_current_bitrate, _current_output_res)

                if _ce.is_set():
                    break

                # Log batch stats
                batch_done = len(batch) - batch_errors
                total_done += batch_done
                total_errors += batch_errors
                total_orig_bytes += batch_orig
                total_new_bytes += batch_new

                if batch_orig > 0 and batch_new > 0:
                    orig_mb = batch_orig / (1024 * 1024)
                    new_mb = batch_new / (1024 * 1024)
                    reduction = ((batch_orig - batch_new) / batch_orig) * 100 if batch_orig > 0 else 0
                    log(f"\n  Batch {batch_num + 1}/{total_batches}: {orig_mb:.0f} MB → {new_mb:.0f} MB ({reduction:.0f}% reduction)\n", "header")

                # After first batch, ask user to confirm
                if first_batch and total_batches > 1 and not _ce.is_set():
                    first_batch = False
                    _confirm_result = [None]  # None=pending, True=continue, "redo"=redo

                    def _ask_confirm():
                        if batch_orig > 0 and batch_new > 0:
                            o_mb = batch_orig / (1024 * 1024)
                            n_mb = batch_new / (1024 * 1024)
                            pct = ((batch_orig - batch_new) / batch_orig) * 100
                            msg = (f"Batch 1 complete: {o_mb:.0f} MB → {n_mb:.0f} MB "
                                   f"({pct:.0f}% reduction)\n\n"
                                   f"Continue with these settings for the remaining "
                                   f"{len(work_list) - batch_size} videos?")
                        else:
                            msg = (f"Batch 1 complete with {batch_errors} error(s).\n\n"
                                   f"Continue for the remaining {len(work_list) - batch_size} videos?")
                        result = _dark_askquestion("Backlog Compression", msg,
                                                      yes_text="Continue", no_text="Adjust Settings")
                        _confirm_result[0] = True if result else "redo"

                    _ui_queue.append(_ask_confirm)
                    while _confirm_result[0] is None and not _ce.is_set():
                        time.sleep(0.25)

                    if _ce.is_set():
                        break

                    if _confirm_result[0] == "redo":
                        # Ask for new settings
                        _new_settings = [None]  # (bitrate, output_res) or None

                        def _ask_new_settings():
                            dlg = tk.Toplevel(root)
                            dlg.title("Adjust Compression Settings")
                            dlg.configure(bg=C_BG)
                            dlg.resizable(False, False)
                            dlg.grab_set()
                            dlg.update_idletasks()
                            _apply_dark_title_bar(dlg)

                            tk.Label(dlg, text="Adjust settings and try batch 1 again:",
                                     bg=C_BG, fg=C_TEXT, font=("Segoe UI", 10)).pack(padx=16, pady=(12, 8))

                            row = tk.Frame(dlg, bg=C_BG)
                            row.pack(padx=16, pady=4)
                            # Res dropdown (first)
                            tk.Label(row, text="Res:", bg=C_BG, fg=C_DIM, font=("Segoe UI", 9)).pack(side="left", padx=(0, 4))
                            _rv = tk.StringVar(value=f"{_current_output_res}p" if _current_output_res else "Original")
                            _rv_combo = _combo(row, textvariable=_rv, values=["Original", "144p", "240p", "360p", "480p", "720p", "1080p"],
                                               state="readonly", width=10)
                            _rv_combo.pack(side="left", padx=(0, 12))
                            # Quality dropdown (second, depends on Res)
                            tk.Label(row, text="Quality:", bg=C_BG, fg=C_DIM, font=("Segoe UI", 9)).pack(side="left", padx=(0, 4))
                            _dlg_res_key = _current_output_res if _current_output_res in _COMPRESS_PRESETS else "1080"
                            _dlg_presets = _COMPRESS_PRESETS.get(_dlg_res_key, _COMPRESS_PRESETS["1080"])
                            _init_quality = next((k for k, v in _dlg_presets.items() if v == _current_bitrate), "Average")
                            _lv = tk.StringVar(value=_init_quality)
                            _lv_combo = _combo(row, textvariable=_lv, values=_QUALITY_OPTIONS, state="readonly", width=14)
                            _lv_combo.pack(side="left")

                            def _on_dlg_res_change(*_):
                                rv_raw = _rv.get()
                                rk = rv_raw.replace("p", "") if rv_raw != "Original" else "1080"
                                _lv_combo.config(values=_QUALITY_OPTIONS)
                                cur_q = _lv.get()
                                if cur_q not in _COMPRESS_PRESETS.get(rk, _COMPRESS_PRESETS["1080"]):
                                    _lv.set("Average")
                            _rv.trace_add("write", _on_dlg_res_change)

                            def _ok():
                                lv = _lv.get()
                                rv = _rv.get()
                                new_or = rv.replace("p", "") if rv != "Original" else ""
                                new_br = _get_compress_bitrate(lv, new_or)
                                _new_settings[0] = (new_br, new_or)
                                dlg.destroy()

                            def _cancel():
                                _new_settings[0] = "cancel"
                                dlg.destroy()

                            btn_row = tk.Frame(dlg, bg=C_BG)
                            btn_row.pack(padx=16, pady=(8, 12))
                            tk.Button(btn_row, text="Retry Batch 1", bg="#3a6a3a", fg="#cccccc",
                                      relief="flat", font=("Segoe UI", 9, "bold"),
                                      cursor="hand2", command=_ok).pack(side="left", padx=(0, 8))
                            tk.Button(btn_row, text="Cancel Backlog", bg="#8b1a1a", fg="#ffffff",
                                      relief="flat", font=("Segoe UI", 9, "bold"),
                                      cursor="hand2", command=_cancel).pack(side="left")

                            dlg.update_idletasks()
                            dlg.geometry(f"+{root.winfo_x() + 200}+{root.winfo_y() + 200}")

                        _ui_queue.append(_ask_new_settings)
                        while _new_settings[0] is None and not _ce.is_set():
                            time.sleep(0.25)

                        if _ce.is_set() or _new_settings[0] == "cancel":
                            log(f"\n  ⛔ Backlog cancelled by user.\n", "red")
                            break

                        # Apply new settings and redo batch 1
                        _current_bitrate, _current_output_res = _new_settings[0]
                        # Undo batch 1 stats
                        total_done -= batch_done
                        total_errors -= batch_errors
                        total_orig_bytes -= batch_orig
                        total_new_bytes -= batch_new
                        _res_key = _current_output_res if _current_output_res in _COMPRESS_PRESETS else "1080"
                        _presets_for_res = _COMPRESS_PRESETS.get(_res_key, _COMPRESS_PRESETS["1080"])
                        _lvl_name = next((k for k, v in _presets_for_res.items() if v == _current_bitrate), f"{_current_bitrate} MB/hr")
                        _res_label = f"{_current_output_res}p" if _current_output_res else "Original"
                        log(f"\n  Retrying batch 1 with: {_lvl_name} ({_current_bitrate} MB/hr), {_res_label}\n", "header")
                        continue  # Redo the while loop with new settings
                    # User confirmed — break inner loop, proceed to next batch
                    break
                else:
                    first_batch = False
                    break  # Not first batch or only one batch, proceed

        # Clean up temp dir
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass

        # ── Summary ──
        if not _ce.is_set() and total_done > 0:
            elapsed = time.time() - t_start
            _e_hrs, _e_rem = divmod(int(elapsed), 3600)
            _e_mins, _e_secs = divmod(_e_rem, 60)
            if _e_hrs:
                _t_str = f"{_e_hrs}h {_e_mins}m {_e_secs}s"
            elif _e_mins:
                _t_str = f"{_e_mins}m {_e_secs}s"
            else:
                _t_str = f"{_e_secs}s"
            orig_gb = total_orig_bytes / (1024 ** 3)
            new_gb = total_new_bytes / (1024 ** 3)
            pct = ((total_orig_bytes - total_new_bytes) / total_orig_bytes * 100) if total_orig_bytes > 0 else 0
            log(f"\n=== Backlog Complete: {ch_name} ===\n", "header")
            log(f"  Total: {total_done} video(s) processed ({_t_str})\n", "simpleline_green")
            log(f"  Before: {orig_gb:.1f} GB → After: {new_gb:.1f} GB ({pct:.0f}% reduction)\n", "simpleline_green")
            if skipped_compressed:
                log(f"  Skipped: {skipped_compressed} (already compressed)", "simpleline")
            if skipped_no_match:
                log(f"  Unmatched: {skipped_no_match} (no YouTube match)", "simpleline")
            if total_errors:
                log(f"  Errors: {total_errors}\n", "red")
            log("\n", "simpleline")

    if _sync_mode:
        _worker()
    else:
        threading.Thread(target=_worker, daemon=True).start()


def _whisper_transcribe(audio_path, duration=0, title="", cancel_ev=None, pause_ev=None):
    """Transcribe a file using the persistent Whisper subprocess.

    Returns (text, segments) where text is str or None, and segments is a list of
    {"start": float, "end": float, "text": str} dicts (empty list on failure).
    If duration (seconds) is provided, progress percentage is logged in real time.
    title is shown in the progress line so the user knows which video is being processed.
    cancel_ev/pause_ev default to the global cancel_event/pause_event if not provided.
    """
    global _gpu_actively_encoding
    _ce = cancel_ev or cancel_event
    _pe = pause_ev or pause_event
    global _whisper_proc
    if _whisper_proc is None or _whisper_proc.poll() is not None:
        if not _start_whisper_process():
            return None
    try:
        import json as _json
        _title_disp = title
        if _title_disp and len(_title_disp) > 40:
            _title_disp = _title_disp[:37] + "..."
        _title_part = f' "{_title_disp}"' if _title_disp else ""
        # In simple mode, prefix with [idx/total] instead of spaces
        _wp = f"  [{_whisper_counter['idx']}/{_whisper_counter['total']}] " if _is_simple_mode and _whisper_counter['total'] else "    "
        _gpu_actively_encoding = True
        log(f"{_wp}Transcribing{_title_part}, 0%...\n", "whisper_progress")
        request = _json.dumps({"path": audio_path, "duration": duration})
        _whisper_proc.stdin.write(request + "\n")
        _whisper_proc.stdin.flush()

        # Read responses — may be progress updates before the final result
        while True:
            response_line = _whisper_proc.stdout.readline().strip()
            if not response_line:
                _gpu_actively_encoding = False
                return None, []
            result = _json.loads(response_line)
            if result.get("status") == "progress":
                pct = result.get("pct", 0)
                if _ce.is_set():
                    log(f"{_wp}Transcribing{_title_part}, {pct}% — cancelling after this file...\n", "whisper_progress")
                elif _pe.is_set():
                    log(f"{_wp}Transcribing{_title_part}, {pct}% — will pause after this file...\n", "whisper_progress")
                else:
                    log(f"{_wp}Transcribing{_title_part}, {pct}%...\n", "whisper_progress")
                continue
            elif result.get("status") == "ok":
                log(f"{_wp}Transcribing{_title_part}, 100%...\n", "whisper_progress")
                _gpu_actively_encoding = False
                _text = result.get("text") or None
                _raw_segs = result.get("segments", [])
                _segments = [{"start": s["s"], "end": s["e"], "text": s["t"]} for s in _raw_segs]
                return _text, _segments
            else:
                log(f"  ⚠ Whisper error: {result.get('text', 'unknown')}\n", "red")
                _gpu_actively_encoding = False
                return None, []
    except Exception as e:
        log(f"  ⚠ Whisper communication error: {e}\n", "red")
        _gpu_actively_encoding = False
        _stop_whisper_process()  # Kill broken process, will restart on next call
        return None, []


# ─── Punctuation restoration (for YouTube auto-captions) ─────────────────────
def _load_punctuation_model():
    """Start the punctuation restoration subprocess on GPU. Returns True if ready."""
    global _punct_proc
    if _punct_proc is not None and _punct_proc.poll() is None:
        return True

    if not os.path.exists(_WHISPER_PYTHON):
        log("  ⚠ Punctuation model unavailable (Python 3.11 not found).\n", "red")
        return False

    # Check / install transformers in the CUDA Python environment
    if not _check_punct_installed():
        if not _install_punct_blocking():
            return False

    return _start_punct_process()


def _punctuate_text(text):
    """Restore punctuation and capitalization via the GPU subprocess.

    Returns original text if subprocess is unavailable or errors.
    """
    global _punct_proc
    if _punct_proc is None or _punct_proc.poll() is not None:
        return text
    try:
        request = json.dumps({"text": text})
        _punct_proc.stdin.write(request + "\n")
        _punct_proc.stdin.flush()

        response_line = _punct_proc.stdout.readline().strip()
        if not response_line:
            return text
        result = json.loads(response_line)
        if result.get("status") == "ok":
            return result.get("text", text)
        else:
            log(f"    ⚠ Punctuation error: {result.get('text', 'unknown')}\n", "red")
            return text
    except Exception:
        _stop_punct_process()  # Kill broken process
        return text


def _whisper_punct_fixup(text):
    """Run punctuation model on Whisper output only if it lacks punctuation."""
    if not text:
        return text
    # Require meaningful punctuation density, not just a single stray comma.
    # At least 1 punctuation mark per ~200 chars (and at least 3 total).
    _p_count = sum(1 for ch in text if ch in ',.!?;')
    if _p_count >= max(3, len(text) // 200):
        return text
    if _punct_proc is None or _punct_proc.poll() is not None:
        return text  # Punctuation model not loaded — skip
    try:
        return _punctuate_text(text)
    except Exception:
        return text


def _check_internet(timeout=5):
    """Quick connectivity check. Returns True if internet is reachable."""
    for host in ("https://www.google.com", "https://dns.google"):
        try:
            urllib.request.urlopen(host, timeout=timeout)
            return True
        except Exception:
            continue
    return False


def _wait_for_internet(cancel_ev, pause_ev, log_fn=None):
    """Block until internet is restored or cancel is set. Returns True if restored, False if cancelled."""
    if log_fn:
        log_fn(f"  ⚠ Internet connection lost — pausing until restored...\n", "red")
    while not cancel_ev.is_set():
        if _check_internet(timeout=5):
            if log_fn:
                log_fn(f"  ✓ Internet restored — resuming.\n", "simpleline_green")
            return True
        time.sleep(3)
    return False


def _fetch_auto_captions(video_id, temp_dir):
    """Fetch YouTube captions (manual or auto-generated) for a video.

    Returns (text, segments) where text is str or None, and segments is a list of
    {"start": float, "end": float, "text": str} dicts (empty list on failure).
    """
    temp_base = os.path.join(temp_dir, f"_transcript_{video_id}")
    cmd = [
        "yt-dlp", "--skip-download",
        "--write-sub", "--write-auto-sub", "--sub-lang", "en", "--sub-format", "vtt",
        "-o", temp_base + ".%(ext)s",
        "--no-playlist",
        f"https://www.youtube.com/watch?v={video_id}"
    ]

    try:
        subprocess.run(cmd, capture_output=True, text=True, timeout=120, startupinfo=startupinfo)
    except Exception as e:
        log(f"    ⚠ yt-dlp caption fetch error: {e}\n", "red")
        return None, []

    # Find any VTT file created for this video
    import glob
    vtt_files = glob.glob(os.path.join(temp_dir, f"_transcript_{video_id}*.vtt"))
    if not vtt_files:
        return None, []
    vtt_path = vtt_files[0]

    text = _parse_vtt_to_text(vtt_path)
    segments = _parse_vtt_to_segments(vtt_path)
    try:
        for vf in vtt_files:
            os.remove(vf)
    except Exception:
        pass
    return (text if text else None), segments


def _format_upload_date(date_str):
    """Convert YYYYMMDD to (MM.DD.YYYY) display format."""
    if len(date_str) == 8 and date_str.isdigit():
        return f"({date_str[4:6]}.{date_str[6:8]}.{date_str[:4]})"
    return f"({date_str})" if date_str else "(Unknown date)"


def _format_duration(dur_str):
    """Ensure duration is wrapped in parens."""
    if not dur_str:
        return "(Unknown length)"
    return f"({dur_str})"


def _get_transcript_filename(ch_name, folder_path, split_years, split_months, combined,
                              year=None, month=None):
    """Build the transcript .txt file path based on organization settings.

    Returns (file_path, subfolder_path) where subfolder_path is the target directory.
    """
    if combined or (not split_years):
        # Single file in channel root
        return os.path.join(folder_path, f"{ch_name} Transcript.txt"), folder_path

    if split_years and split_months and year and month:
        # Year/Month: e.g. "ChannelName January 24 Transcript.txt" in year/month folder
        month_num = int(month) if isinstance(month, str) and month.isdigit() else month
        month_name = MONTH_NAMES.get(month_num, f"{month_num:02d} Unknown").split(" ", 1)[1]  # "January"
        yr_short = str(year)[-2:]  # "24"
        subfolder = os.path.join(folder_path, str(year), MONTH_NAMES.get(month_num, f"{month_num:02d} Unknown"))
        fname = f"{ch_name} {month_name} {yr_short} Transcript.txt"
        return os.path.join(subfolder, fname), subfolder

    if split_years and year:
        # Year only: e.g. "ChannelName 2024 Transcript.txt" in year folder
        subfolder = os.path.join(folder_path, str(year))
        fname = f"{ch_name} {year} Transcript.txt"
        return os.path.join(subfolder, fname), subfolder

    # Fallback
    return os.path.join(folder_path, f"{ch_name} Transcript.txt"), folder_path


def _scan_existing_transcripts(folder_path, ch_name):
    """Scan all transcript .txt files under folder_path. Return set of video titles already transcribed."""
    existing = set()
    pattern = re.compile(r'^===\((.+?)\),\s*\(')
    for dirpath, _dirs, files in os.walk(folder_path):
        for f in files:
            if f.startswith(ch_name) and f.endswith("Transcript.txt"):
                try:
                    with open(os.path.join(dirpath, f), "r", encoding="utf-8") as fh:
                        for line in fh:
                            m = pattern.match(line.strip())
                            if m:
                                existing.add(m.group(1))
                except Exception:
                    pass
    return existing


def _sort_transcript_entries(txt_paths):
    """Sort transcript entries in each .txt file chronologically by date.

    Entry format: ===({title}), ({MM.DD.YYYY}), ({duration}), ({source})===
    Entries are separated by triple-newline.
    """
    _entry_re = re.compile(r'^===\((.+?)\),\s*\((\d{2})\.(\d{2})\.(\d{4})\)')
    for txt_path in txt_paths:
        try:
            with open(txt_path, "r", encoding="utf-8") as f:
                content = f.read()
            if not content.strip():
                continue

            # Split into entries (each starts with ===)
            raw_entries = re.split(r'(?=^===\()', content, flags=re.MULTILINE)
            raw_entries = [e for e in raw_entries if e.strip()]
            if len(raw_entries) <= 1:
                continue  # nothing to sort

            # Parse date from each entry for sorting
            dated_entries = []
            for entry in raw_entries:
                m = _entry_re.match(entry.strip())
                if m:
                    mm, dd, yyyy = m.group(2), m.group(3), m.group(4)
                    sort_key = f"{yyyy}{mm}{dd}"
                else:
                    sort_key = "00000000"  # unknown date → put first
                dated_entries.append((sort_key, entry))

            # Sort newest first (reverse chronological)
            dated_entries.sort(key=lambda x: x[0], reverse=True)

            # Rewrite file
            sorted_content = ""
            for _, entry in dated_entries:
                # Ensure each entry ends with exactly triple-newline
                entry = entry.rstrip("\n") + "\n\n\n"
                sorted_content += entry

            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(sorted_content)
        except Exception:
            pass  # don't break transcription over a sort error


def _ask_whisper_model_dialog(prompt_text="Which Whisper model to use?",
                              subtitle_text="Manual file transcription"):
    """Show model selection dialog for manual transcription.
    Returns (model_string, timed_out) tuple, or (None, False) if cancelled.
    Blocks the calling thread until the user picks.
    """
    _model_result = [None]
    _model_timed_out = [False]

    def _ask():
        _dlg = tk.Toplevel(root)
        _dlg.title("Whisper Model Selection")
        _dlg.configure(bg=C_BG)
        _dlg.resizable(False, False)
        _dlg.transient(root)
        _dlg.grab_set()
        _dlg.update_idletasks()
        _apply_dark_title_bar(_dlg)
        _rx = root.winfo_rootx() + root.winfo_width() // 2
        _ry = root.winfo_rooty() + root.winfo_height() // 2
        _dlg.geometry(f"+{_rx - 160}+{_ry - 140}")

        tk.Label(_dlg, text=prompt_text,
                 bg=C_BG, fg=C_TEXT, font=("Segoe UI", 10, "bold"),
                 pady=10, padx=20).pack(fill="x")
        tk.Label(_dlg, text=subtitle_text,
                 bg=C_BG, fg=C_DIM, font=("Segoe UI", 9),
                 padx=20).pack(fill="x")

        _btn_frame = tk.Frame(_dlg, bg=C_BG, pady=10)
        _btn_frame.pack(fill="x", padx=20)

        _models = [
            ("tiny",     "Fastest  (~30-50\u00d7 realtime)",  "tiny"),
            ("small",    "Fast  (~15-20\u00d7 realtime)",     "small"),
            ("medium",   "Balanced  (~7-10\u00d7 realtime)",  "medium"),
            ("large-v3", "Best quality  (~3-5\u00d7 realtime)", "large-v3"),
        ]
        _DEFAULT_MODEL = "small"
        _countdown = {"secs": 60, "job": None}

        def _pick(m, dlg=_dlg):
            if _countdown["job"]:
                try:
                    _dlg.after_cancel(_countdown["job"])
                except Exception:
                    pass
            _model_result[0] = m
            try:
                dlg.destroy()
            except Exception:
                pass

        _timer_lbl = tk.Label(_dlg, text=f"Auto-selecting {_DEFAULT_MODEL} in 60s...",
                              bg=C_BG, fg=C_DIM, font=("Segoe UI", 8),
                              padx=20)
        _timer_lbl.pack(fill="x", pady=(0, 4))

        def _tick():
            _countdown["secs"] -= 1
            if _countdown["secs"] <= 0:
                _model_timed_out[0] = True
                _pick(_DEFAULT_MODEL)
                return
            try:
                _timer_lbl.config(text=f"Auto-selecting {_DEFAULT_MODEL} in {_countdown['secs']}s...")
                _countdown["job"] = _dlg.after(1000, _tick)
            except Exception:
                pass

        _countdown["job"] = _dlg.after(1000, _tick)

        for label, desc, model_id in _models:
            _row = tk.Frame(_btn_frame, bg=C_BG)
            _row.pack(fill="x", pady=2)
            _b = tk.Button(_row, text=label, width=10,
                           bg="#3a3a3a", fg=C_TEXT, activebackground="#555555",
                           activeforeground=C_TEXT, relief="flat", bd=0,
                           font=("Segoe UI", 9, "bold"), cursor="hand2",
                           command=lambda m=model_id: _pick(m))
            _b.pack(side="left", padx=(0, 8))
            tk.Label(_row, text=desc, bg=C_BG, fg=C_DIM,
                     font=("Segoe UI", 9)).pack(side="left")
            if model_id == _DEFAULT_MODEL:
                tk.Label(_row, text="(default)", bg=C_BG, fg="#5a8a5a",
                         font=("Segoe UI", 8)).pack(side="left", padx=(4, 0))

        _dlg.protocol("WM_DELETE_WINDOW", lambda: _pick(_DEFAULT_MODEL))

    if _root_alive:
        _ui_queue.append(_ask)
        while _model_result[0] is None and not cancel_event.is_set():
            time.sleep(0.1)

    return _model_result[0], _model_timed_out[0]


def _ask_start_gpu_tasks(count, cancel_ev=None, timeout=180):
    """Show a timed dialog asking the user to start GPU Tasks. Returns True if user clicks Yes.
    Auto-dismisses after timeout seconds (default 3 min), returning False.
    Thread-safe: creates dialog on main thread, waits from worker thread.
    """
    _result = [None]  # None=pending, True=yes, False=no/timeout

    def _show():
        dlg = tk.Toplevel(root)
        dlg.title("Start GPU Tasks?")
        dlg.configure(bg=C_BG)
        dlg.resizable(False, False)
        dlg.grab_set()
        dlg.update_idletasks()
        _apply_dark_title_bar(dlg)
        dlg.protocol("WM_DELETE_WINDOW", lambda: _dismiss(False))

        tk.Label(dlg, text=f"Downloaded {count} videos so far.",
                 bg=C_BG, fg=C_TEXT, font=("Segoe UI", 10, "bold")).pack(padx=20, pady=(14, 4))
        tk.Label(dlg, text="It is recommended to start GPU Tasks now\nto save storage space.",
                 bg=C_BG, fg=C_DIM, font=("Segoe UI", 9)).pack(padx=20, pady=(0, 8))

        _secs = [timeout]
        _timer_lbl = tk.Label(dlg, text=f"Auto-dismissing in {_secs[0]}s...",
                              bg=C_BG, fg=C_DIM, font=("Segoe UI", 8))
        _timer_lbl.pack(padx=20, pady=(0, 4))
        _timer_job = [None]

        def _tick():
            _secs[0] -= 1
            if _secs[0] <= 0:
                _dismiss(False)
                return
            try:
                _timer_lbl.config(text=f"Auto-dismissing in {_secs[0]}s...")
                _timer_job[0] = dlg.after(1000, _tick)
            except Exception:
                pass

        def _dismiss(val):
            _result[0] = val
            if _timer_job[0]:
                try:
                    dlg.after_cancel(_timer_job[0])
                except Exception:
                    pass
            try:
                dlg.destroy()
            except Exception:
                pass

        btn_row = tk.Frame(dlg, bg=C_BG)
        btn_row.pack(padx=20, pady=(4, 14))
        tk.Button(btn_row, text="Start GPU Tasks", bg="#3a6a3a", fg="#cccccc",
                  relief="flat", font=("Segoe UI", 9, "bold"), cursor="hand2",
                  command=lambda: _dismiss(True)).pack(side="left", padx=(0, 10))
        tk.Button(btn_row, text="Not Now", bg=C_SURFACE, fg=C_DIM,
                  relief="flat", font=("Segoe UI", 9), cursor="hand2",
                  command=lambda: _dismiss(False)).pack(side="left")

        _timer_job[0] = dlg.after(1000, _tick)
        dlg.update_idletasks()
        dlg.geometry(f"+{root.winfo_x() + 200}+{root.winfo_y() + 200}")

    _ui_queue.append(_show)
    _ce = cancel_ev or cancel_event
    while _result[0] is None and not _ce.is_set():
        time.sleep(0.25)
    return _result[0] is True


def _count_gpu_encode_batches(ch_url):
    """Count unprocessed encode/backlog_encode batches for a channel in the GPU queue."""
    count = 0
    with _gpu_queue_lock:
        for q in _gpu_queue:
            if q.get("ch_url") == ch_url and q["type"] in ("encode", "backlog_encode"):
                count += 1
    # Also count the currently-running item if it's an encode for this channel
    if _gpu_running and _gpu_current_item:
        _ci = _gpu_current_item
        if _ci.get("ch_url") == ch_url and _ci.get("type") in ("encode", "backlog_encode"):
            count += 1
    return count


def _get_max_encode_batch(ch_url):
    """Return the highest batch_num already queued (or running) for a channel's encode tasks, or 0."""
    with _gpu_queue_lock:
        nums = [q.get("batch_num", 0) for q in _gpu_queue
                if q.get("ch_url") == ch_url and q.get("type") == "encode" and q.get("batch_num") is not None]
        _cur = _gpu_current_item
        if _cur and _cur.get("ch_url") == ch_url and _cur.get("type") == "encode" and _cur.get("batch_num") is not None:
            nums.append(_cur["batch_num"])
    return max(nums) if nums else 0


def _get_next_compress_batch(ch_url):
    """Increment and return the compression batch sequence number for a channel.

    Persisted in channel config so batch numbers survive app restarts.
    """
    seq = 1
    with config_lock:
        for ch in config.get("channels", []):
            if ch.get("url") == ch_url:
                seq = ch.get("compress_batch_seq", 0) + 1
                ch["compress_batch_seq"] = seq
                break
    save_config(config)
    return seq


def _add_to_gpu_queue(item, _quiet=False):
    """Add a task to the GPU Tasks queue and show the GPU button.
    _quiet: if True, suppress duplicate warning logs (used by incremental compress callback).
    """
    with _gpu_queue_lock:
        # Also check the currently-running item for dedup
        _cur = _gpu_current_item
        # Duplicate check
        if item["type"] == "transcribe":
            if any(q.get("ch_url") == item["ch_url"] and q["type"] == "transcribe" for q in _gpu_queue) or \
               (_cur and _cur.get("ch_url") == item.get("ch_url") and _cur.get("type") == "transcribe"):
                if not _quiet:
                    log(f"  ⚠ {item['ch_name']} is already in the GPU Tasks queue.\n", "simpleline")
                return
            label = f"Transcribe {item['ch_name']}"
        elif item["type"] == "encode":
            _bn = item.get("batch_num")
            if _bn is not None:
                # Batch-numbered: dedup by channel + batch number
                if any(q.get("ch_url") == item["ch_url"] and q["type"] == "encode" and q.get("batch_num") == _bn for q in _gpu_queue) or \
                   (_cur and _cur.get("ch_url") == item.get("ch_url") and _cur.get("type") == "encode" and _cur.get("batch_num") == _bn):
                    if not _quiet:
                        log(f"  ⚠ {item['ch_name']} Batch {_bn} compression is already in the GPU Tasks queue.\n", "simpleline")
                    return
                label = f"Compress {item['ch_name']}, Batch {_bn}"
            else:
                # Non-batch: dedup by channel URL (original behavior)
                if any(q.get("ch_url") == item["ch_url"] and q["type"] == "encode" for q in _gpu_queue) or \
                   (_cur and _cur.get("ch_url") == item.get("ch_url") and _cur.get("type") == "encode"):
                    if not _quiet:
                        log(f"  ⚠ {item['ch_name']} compression is already in the GPU Tasks queue.\n", "simpleline")
                    return
                label = f"Compress {item['ch_name']}"
        elif item["type"] == "backlog_encode":
            if any(q.get("ch_url") == item["ch_url"] and q["type"] == "backlog_encode" for q in _gpu_queue) or \
               (_cur and _cur.get("ch_url") == item.get("ch_url") and _cur.get("type") == "backlog_encode"):
                if not _quiet:
                    log(f"  ⚠ {item['ch_name']} backlog is already in the GPU Tasks queue.\n", "simpleline")
                return
            label = f"Backlog {item['ch_name']}"
        elif item["type"] == "mt":
            if item.get("folder_path"):
                # Folder-based manual transcription
                if any(q.get("folder_path") == item["folder_path"] and q["type"] == "mt" for q in _gpu_queue) or \
                   (_cur and _cur.get("folder_path") == item.get("folder_path") and _cur.get("type") == "mt"):
                    if not _quiet:
                        log(f"  Folder already in GPU Tasks queue.\n", "simpleline")
                    return
                label = f"M.T. {item['folder_name']} ({item['vid_count']} files)"
            else:
                # Single-file manual transcription
                if any(q.get("file_path") == item.get("file_path") and q["type"] == "mt" for q in _gpu_queue) or \
                   (_cur and _cur.get("file_path") == item.get("file_path") and _cur.get("type") == "mt"):
                    if not _quiet:
                        log(f"  File already in GPU Tasks queue.\n", "simpleline")
                    return
                fname = os.path.splitext(os.path.basename(item["file_path"]))[0]
                label = f"M.T. {fname}"
        else:
            label = "GPU Task"
        _gpu_queue.append(item)
    log(f"=== Added to GPU Tasks: {label} ===\n", "header")
    _update_gpu_btn()
    _save_queue_state()
    # Refresh channel list so Transcribed column shows "Queued" immediately
    if item.get("type") in ("transcribe", "mt"):
        _ui_queue.append(refresh_channel_dropdowns)
    # Autorun: auto-start GPU processing if enabled and not already running
    if not _gpu_running and config.get("autorun_gpu", False):
        _ui_queue.append(_gpu_start)


def _process_transcribe_queue():
    """Process next queued transcription if any. Returns True if one was started."""
    with _transcribe_queue_lock:
        if not _transcribe_queue:
            return False
        args = _transcribe_queue.pop(0)
        remaining = len(_transcribe_queue)

    ch_name, ch_url, folder, sy, sm, combined = args
    with _queue_order_lock:
        try:
            _queue_order.remove(("transcribe", ch_url))
        except ValueError:
            pass
    _current_job["label"] = f"Transcribe {ch_name}"
    _update_queue_btn()
    log(f"\n=== Processing queued transcription: {ch_name}", "header")
    if remaining:
        log(f" ({remaining} more in queue)", "header")
    log(f" ===\n", "header")
    _start_transcription(ch_name, ch_url, folder, sy, sm, combined)
    return True


def _start_transcription(ch_name, ch_url, folder, split_years, split_months, combined,
                         cancel_ev=None, pause_ev=None, skip_model_dialog=False, _sync_mode=False):
    """Start transcribing a channel. Queues if something is already running.

    cancel_ev/pause_ev: optional threading.Events (default to global cancel_event/pause_event).
    skip_model_dialog: if True, skip model selection and use current _whisper_model_choice.
    _sync_mode: if True, run the worker body directly (blocking) instead of spawning a thread.
    """
    global _transcribe_running

    if not _sync_mode and (_sync_running or _reorg_running or _transcribe_running):
        with _transcribe_queue_lock:
            if not any(q[1] == ch_url for q in _transcribe_queue):
                _transcribe_queue.append((ch_name, ch_url, folder, split_years, split_months, combined))
                with _queue_order_lock:
                    _queue_order.append(("transcribe", ch_url))
                log(f"\n=== Added {ch_name} transcription to Sync List ===\n", "header")
            else:
                log(f"  ⚠ {ch_name} is already in the transcription queue.\n", "simpleline")
        _update_queue_btn()
        return

    def _worker():
        global _transcribe_running, _transcribe_sync_controlled, _whisper_model_choice
        _ce = cancel_ev or cancel_event
        _pe = pause_ev or pause_event
        _transcribe_running = True
        _transcribe_sync_controlled = (cancel_ev is None)  # True when sync pipeline, False when GPU-driven
        if not _sync_mode:
            _current_job["label"] = f"Transcribe {ch_name}"
            _current_job["url"] = ch_url
            _update_queue_btn()
        _tray_start_spin(red=True)
        _update_tray_tooltip(f"YT Archiver — Transcribing {ch_name}")
        _ui_queue.append(refresh_channel_dropdowns)
        _whisper_available = None  # None = not checked yet

        try:
            _ce.clear()
            if not _sync_mode:
                _ui_queue.append(_update_queue_btn)

            log(f"\n{'='*60}\n", "header")
            log(f"  TRANSCRIBING: {ch_name}\n", "header")
            log(f"{'='*60}\n\n", "header")

            # ── Step 1: Scan local video files ──────────────────────────
            log("  Scanning local video files...\n", "simpleline")
            _VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".avi", ".wav", ".mp3", ".m4a", ".flac")
            local_files = {}  # filename_no_ext -> full_path
            for dirpath, _, files in os.walk(folder):
                for f in files:
                    if f.lower().endswith(_VIDEO_EXTS):
                        # Skip partial/temp files from interrupted downloads
                        if ".temp." in f.lower() or ".part" in f.lower():
                            continue
                        fname_no_ext = os.path.splitext(f)[0]
                        local_files[fname_no_ext] = os.path.join(dirpath, f)

            if not local_files:
                log("  ⚠ No video files found in channel folder.\n", "red")
                return

            log(f"  Found {len(local_files)} video file(s) on disk.\n", "simpleline")

            if _ce.is_set():
                log(f"\n  ⛔ Transcription cancelled.\n", "red")
                return

            # ── Step 2: Scan existing transcripts to skip already-done ──
            already_done = _scan_existing_transcripts(folder, ch_name)
            files_to_process = {}
            for fname, fpath in local_files.items():
                if fname not in already_done:
                    files_to_process[fname] = fpath

            if already_done:
                log(f"  {len(already_done)} video(s) already transcribed — skipping.\n", "simpleline")

            # One-time migration: delete old JSONL files so they get regenerated
            # with the improved merged-segment parser (v15.4 fix).
            _migration_key = f"jsonl_v154_migrated_{ch_name}"
            if already_done and not config.get(_migration_key):
                for _dp, _dns, _fns in os.walk(folder):
                    for _fn in _fns:
                        if _fn.startswith(".") and ch_name in _fn and _fn.endswith("Transcript.jsonl"):
                            try:
                                os.remove(os.path.join(_dp, _fn))
                            except Exception:
                                pass
                with config_lock:
                    config[_migration_key] = True
                    save_config(config)
            _jsonl_existing = _scan_existing_jsonl(folder, ch_name) if already_done else set()
            _jsonl_needed = already_done - _jsonl_existing if already_done else set()

            if not files_to_process and not _jsonl_needed:
                log(f"  ✓ All videos already transcribed!\n", "simpleline_green")
                # Mark channel as fully transcribed
                with config_lock:
                    for _cfg_ch in config.get("channels", []):
                        if _cfg_ch.get("url") == ch_url:
                            _cfg_ch["transcription_complete"] = True
                            _cfg_ch["transcription_pending"] = 0
                            break
                    save_config(config)
                return

            if _ce.is_set():
                log(f"\n  ⛔ Transcription cancelled.\n", "red")
                return

            # ── Step 3: Fetch YT playlist for title→ID matching ─────────
            log("  Fetching YouTube video list for caption matching...\n", "simpleline")
            yt_title_to_id = {}  # yt_title -> video_id
            try:
                enum_cmd = [
                    "yt-dlp", "--flat-playlist",
                    "--print", "%(id)s|||%(title)s",
                    "--no-warnings",
                    ch_url
                ]
                enum_proc = subprocess.run(enum_cmd, capture_output=True, text=True, timeout=300, startupinfo=startupinfo)
                for line in enum_proc.stdout.strip().split("\n"):
                    if "|||" in line:
                        vid_id, yt_title = line.strip().split("|||", 1)
                        yt_title_to_id[yt_title.strip()] = vid_id.strip()
                log(f"  Found {len(yt_title_to_id)} video(s) on YouTube.\n", "simpleline")
            except Exception as e:
                log(f"  ⚠ Could not fetch YouTube list: {e}. All files will use Whisper.\n", "red")

            if _ce.is_set():
                log(f"\n  ⛔ Transcription cancelled.\n", "red")
                return

            # ── Step 3.5: JSONL backfill + punctuation sweep for already-done ─
            def _normalize_title(title):
                """Normalize a title for matching: NFKC (fullwidth→ASCII), strip unsafe chars, lowercase."""
                import unicodedata
                s = unicodedata.normalize('NFKC', title)
                s = re.sub(r'[\\/:*?"<>|]', '', s)
                s = re.sub(r'\s+', ' ', s).strip()
                return s.lower()

            if _jsonl_needed and yt_title_to_id and not _ce.is_set():
                # Build normalized YT title → video_id lookup
                _yt_norm_backfill = {}
                for yt_title, vid_id in yt_title_to_id.items():
                    _norm = _normalize_title(yt_title)
                    if _norm not in _yt_norm_backfill:
                        _yt_norm_backfill[_norm] = vid_id

                # Match needed titles to video IDs
                _backfill_list = []
                for title in _jsonl_needed:
                    vid_id = yt_title_to_id.get(title)
                    if not vid_id:
                        _norm_t = _normalize_title(title)
                        vid_id = _yt_norm_backfill.get(_norm_t)
                    if vid_id:
                        _backfill_list.append((title, vid_id))

                if _backfill_list:
                    _bf_total = len(_backfill_list)
                    log(f"  Generating searchable .jsonl for {_bf_total} video(s)...\n", "simpleline")
                    _bf_temp = os.path.join(folder, "_transcribe_temp")
                    os.makedirs(_bf_temp, exist_ok=True)
                    _bf_done = 0
                    _bf_idx = 0
                    for _bf_title, _bf_vid in _backfill_list:
                        if _ce.is_set():
                            break
                        _bf_idx += 1
                        try:
                            _, _bf_segs = _fetch_auto_captions(_bf_vid, _bf_temp)
                            if _bf_segs:
                                # Determine which txt file this video belongs to
                                _bf_fpath = local_files.get(_bf_title)
                                if _bf_fpath:
                                    _bf_mtime = datetime.fromtimestamp(os.path.getmtime(_bf_fpath))
                                    _bf_txt, _ = _get_transcript_filename(
                                        ch_name, folder, split_years, split_months, combined,
                                        year=_bf_mtime.year, month=_bf_mtime.month)
                                else:
                                    _bf_txt, _ = _get_transcript_filename(
                                        ch_name, folder, split_years, split_months, combined)
                                _bf_jsonl = _get_jsonl_path(_bf_txt)
                                _write_jsonl_entry(_bf_jsonl, _bf_vid, _bf_title, _bf_segs)
                                _bf_done += 1
                        except Exception:
                            pass
                    if _bf_done:
                        log(f"  ✓ {_bf_done} searchable .jsonl entry/entries generated.\n", "simpleline_green")

            # ── Punctuation sweep for already-transcribed entries ──
            if already_done and not _ce.is_set():
                _punct_sweep_needed = False
                # Scan existing transcript files for entries lacking punctuation
                _entry_pattern = re.compile(r'^===\((.+?)\),\s*\((\d{2}\.\d{2}\.\d{4})\),\s*\(([^)]*)\),\s*(\([^)]+\))===')
                _txt_files_to_sweep = []
                for dirpath, _dirs, files in os.walk(folder):
                    for f in files:
                        if f.startswith(ch_name) and f.endswith("Transcript.txt"):
                            _txt_files_to_sweep.append(os.path.join(dirpath, f))

                _punct_fixes = 0
                for _sw_path in _txt_files_to_sweep:
                    try:
                        with open(_sw_path, "r", encoding="utf-8") as _sf:
                            _sw_content = _sf.read()
                    except Exception:
                        continue

                    _sw_entries = _sw_content.split("\n\n\n")
                    _sw_modified = False
                    _new_entries = []
                    for _sw_entry in _sw_entries:
                        _sw_entry_stripped = _sw_entry.strip()
                        if not _sw_entry_stripped:
                            _new_entries.append(_sw_entry)
                            continue
                        # Parse header line and body
                        _sw_lines = _sw_entry_stripped.split("\n", 1)
                        if len(_sw_lines) < 2 or not _sw_lines[0].startswith("==="):
                            _new_entries.append(_sw_entry)
                            continue
                        _sw_header = _sw_lines[0]
                        _sw_body = _sw_lines[1]
                        # Skip exclusion entries
                        if "NO AUDIO DATA" in _sw_header:
                            _new_entries.append(_sw_entry)
                            continue
                        # Check punctuation density
                        _sw_p_count = sum(1 for ch in _sw_body if ch in ',.!?;')
                        if _sw_p_count >= max(3, len(_sw_body) // 200):
                            _new_entries.append(_sw_entry)
                            continue
                        if len(_sw_body) < 20:
                            _new_entries.append(_sw_entry)
                            continue
                        # Needs punctuation — load model if not already loaded
                        if not _punct_sweep_needed:
                            _punct_sweep_needed = True
                            if not _load_punctuation_model():
                                break  # Can't load model, skip sweep
                            log(f"  Running punctuation sweep on previously transcribed entries...\n", "simpleline")
                        try:
                            _fixed_body = _punctuate_text(_sw_body)
                            # Update source tag to note punctuation was added
                            _new_header = _sw_header.replace("(YT CAPTIONS)", "(YT+PUNCTUATION)")
                            _new_entries.append(f"{_new_header}\n{_fixed_body}")
                            _sw_modified = True
                            _punct_fixes += 1
                        except Exception:
                            _new_entries.append(_sw_entry)
                    if _sw_modified:
                        try:
                            _new_content = "\n\n\n".join(_new_entries)
                            with open(_sw_path, "w", encoding="utf-8") as _sf:
                                _sf.write(_new_content)
                        except Exception:
                            pass
                if _punct_fixes:
                    log(f"  ✓ Punctuation added to {_punct_fixes} existing transcript(s).\n", "simpleline_green")

            # If all files were already transcribed, we're done (backfill/sweep was the only work)
            if not files_to_process:
                log(f"  ✓ All videos already transcribed!\n", "simpleline_green")
                with config_lock:
                    for _cfg_ch in config.get("channels", []):
                        if _cfg_ch.get("url") == ch_url:
                            _cfg_ch["transcription_complete"] = True
                            _cfg_ch["transcription_pending"] = 0
                            break
                    save_config(config)
                return

            if _ce.is_set():
                log(f"\n  ⛔ Transcription cancelled.\n", "red")
                return

            # ── Step 4: Split into matched (captions) vs unmatched (Whisper) ─
            matched = []    # (filename, filepath, video_id)  — can try auto-captions
            unmatched = []  # (filename, filepath)            — must use Whisper

            _normalize_for_match = _normalize_title  # alias for step 4

            # Build normalized lookup: normalized_yt_title → video_id
            _yt_normalized = {}
            for yt_title, vid_id in yt_title_to_id.items():
                _norm = _normalize_for_match(yt_title)
                if _norm not in _yt_normalized:  # first match wins (avoid rare collisions)
                    _yt_normalized[_norm] = vid_id

            for fname, fpath in files_to_process.items():
                # Try exact match first, then normalized match
                if fname in yt_title_to_id:
                    matched.append((fname, fpath, yt_title_to_id[fname]))
                else:
                    _norm_fname = _normalize_for_match(fname)
                    if _norm_fname in _yt_normalized:
                        matched.append((fname, fpath, _yt_normalized[_norm_fname]))
                    else:
                        unmatched.append((fname, fpath))

            # Sort by file modification time, newest first
            matched.sort(key=lambda x: os.path.getmtime(x[1]), reverse=True)
            unmatched.sort(key=lambda x: os.path.getmtime(x[1]), reverse=True)

            log(f"  {len(matched)} file(s) matched to YouTube titles (will try auto-captions).\n", "simpleline")
            if unmatched:
                log(f"  {len(unmatched)} file(s) unmatched (will use Whisper).\n", "simpleline")
            log("\n", "simpleline")

            total = len(matched) + len(unmatched)
            temp_dir = os.path.join(folder, "_transcribe_temp")
            os.makedirs(temp_dir, exist_ok=True)

            # Load punctuation model if we have any matched files (for YT captions)
            _punct_loaded = False
            if matched:
                _punct_loaded = _load_punctuation_model()

            if _ce.is_set():
                log(f"\n  ⛔ Transcription cancelled.\n", "red")
                return

            _prior_done = len(already_done)  # count of videos transcribed before this session
            done_count = 0
            err_count = 0
            idx = 0
            _modified_txt_files = set()  # track which .txt files we wrote to, for post-sort
            _transcription_log = []  # [(fname, source, elapsed_secs, error_str_or_None)]
            _t_total_start = time.time()

            # ── Phase A: Process matched files (auto-captions first) ────
            for fname, fpath, vid_id in matched:
                idx += 1

                # Pause check
                _pl = "GPU Tasks" if _sync_mode else "Sync"
                if _pe.is_set() and not _ce.is_set():
                    log(f"  ⏸ {_pl} paused at {_fmt_time()} — click Resume.\n", "pausestatus")
                    while _pe.is_set() and not _ce.is_set():
                        time.sleep(0.25)
                    if not _ce.is_set():
                        clear_pause_status()
                        log(f"  ▶ {_pl} resumed at {_fmt_time()}...\n", "pauselog")
                if _ce.is_set():
                    log(f"\n  ⛔ Transcription cancelled ({done_count}/{total} completed).\n", "red")
                    break

                log(f"  [{idx}/{total}] {fname} — fetching captions...\n" if not _is_simple_mode else f"    Transcribing [{idx}/{total}] {fname} - fetching captions...\n", "transcribe_using")
                _t_vid_start = time.time()

                text, _vtt_segments = _fetch_auto_captions(vid_id, temp_dir)
                source = "auto-captions"

                if not text:
                    # Check if failure is due to internet outage before queuing for Whisper
                    if not _check_internet(timeout=5):
                        if _wait_for_internet(_ce, _pe, log_fn=log):
                            # Internet restored — retry this file
                            text, _vtt_segments = _fetch_auto_captions(vid_id, temp_dir)
                        else:
                            break  # cancelled
                    if not text:
                        # Auto-captions genuinely unavailable — Whisper this file instead
                        log(f"  [{idx}/{total}] {fname} — no captions, queuing for Whisper.\n", "transcribe_using")
                        unmatched.append((fname, fpath))
                        idx -= 1   # give back the slot — this file will be counted in Phase B
                        continue

                # Restore punctuation to YouTube captions
                if _punct_loaded:
                    log(f"    Adding punctuation...\n" if not _is_simple_mode else f"    Transcribing [{idx}/{total}] {fname} - Adding punctuation...\n", "transcribe_using")
                    text = _punctuate_text(text)

                # Get date/duration from local file mtime
                mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
                year_num, month_num = mtime.year, mtime.month
                upload_date = mtime.strftime("%Y%m%d")

                dur_str = ""
                try:
                    probe_cmd = ["ffprobe", "-v", "quiet", "-show_entries",
                                 "format=duration", "-of", "csv=p=0", fpath]
                    probe_result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=30, startupinfo=startupinfo)
                    secs = float(probe_result.stdout.strip())
                    hrs, remainder = divmod(int(secs), 3600)
                    mins, sec = divmod(remainder, 60)
                    dur_str = f"{hrs}:{mins:02d}:{sec:02d}" if hrs else f"{mins}:{sec:02d}"
                except Exception:
                    pass

                # Write transcript entry
                txt_path, subfolder = _get_transcript_filename(
                    ch_name, folder, split_years, split_months, combined,
                    year=year_num, month=month_num)
                os.makedirs(subfolder, exist_ok=True)

                date_fmt = _format_upload_date(upload_date)
                dur_fmt = _format_duration(dur_str)
                src_tag = "(YT+PUNCTUATION)" if _punct_loaded else "(YT CAPTIONS)"
                entry = f"===({fname}), {date_fmt}, {dur_fmt}, {src_tag}===\n{text}\n\n\n"

                try:
                    with open(txt_path, "a", encoding="utf-8") as f:
                        f.write(entry)
                    _modified_txt_files.add(txt_path)
                except Exception as e:
                    log(f"  ⚠ Error writing transcript: {e}\n", "red")
                    _transcription_log.append((fname, source, time.time() - _t_vid_start, str(e)))
                    err_count += 1
                    continue

                # Write hidden JSONL with timestamps for searchability
                if _vtt_segments:
                    _jsonl_path = _get_jsonl_path(txt_path)
                    _write_jsonl_entry(_jsonl_path, vid_id, fname, _vtt_segments)

                _vid_elapsed = time.time() - _t_vid_start
                _transcription_log.append((fname, source, _vid_elapsed, None))
                done_count += 1
                _ve_m, _ve_s = divmod(int(_vid_elapsed), 60)
                _ve_str = f"took {_ve_m}min {_ve_s:02d}sec" if _ve_m else f"took {_ve_s}sec"
                _src_part = f"{source},"
                if _is_simple_mode:
                    _prefix = f"  [{idx}/{total}] "
                    _suffix = f"done ({_src_part} {_ve_str})"
                    _body_width = 58 - len(_prefix)
                    _name_dash = f"{fname} — "
                    if len(_name_dash) > _body_width:
                        _name_dash = fname[:_body_width - 6] + "... — "
                    else:
                        _name_dash = _name_dash.ljust(_body_width)
                    log(f"{_prefix}{_name_dash}{_suffix}\n", "simpleline_blue")
                else:
                    log(f"  [{idx}/{total}] {fname} — done ({_src_part} {_ve_str})\n", "simpleline_blue")

            # ── Phase B: Process unmatched files (Whisper) ──────────────
            if unmatched and not _ce.is_set():
                # Re-sort unmatched by mtime newest first (Phase A may have added failed items)
                unmatched.sort(key=lambda x: os.path.getmtime(x[1]), reverse=True)

                # ── Model selection ──
                log(f"\n  {len(unmatched)} video(s) need Whisper AI transcription.\n", "simpleline")

                if skip_model_dialog:
                    # GPU Tasks mode — model already chosen, skip dialog
                    _stop_whisper_process()
                    log(f"  Using Whisper model: {_whisper_model_choice}\n", "simpleline")
                    _model_result = [_whisper_model_choice]
                    _model_timed_out = [False]
                else:
                    _model_result = [None]
                    _model_timed_out = [False]

                    def _ask_model():
                        _dlg = tk.Toplevel(root)
                        _dlg.title("Whisper Model Selection")
                        _dlg.configure(bg=C_BG)
                        _dlg.resizable(False, False)
                        _dlg.transient(root)
                        _dlg.grab_set()
                        _dlg.update_idletasks()
                        _apply_dark_title_bar(_dlg)

                        # Center on root window
                        _dlg.update_idletasks()
                        _rx = root.winfo_rootx() + root.winfo_width() // 2
                        _ry = root.winfo_rooty() + root.winfo_height() // 2
                        _dlg.geometry(f"+{_rx - 160}+{_ry - 140}")

                        tk.Label(_dlg, text="Which model to use for Whisper?",
                                 bg=C_BG, fg=C_TEXT, font=("Segoe UI", 10, "bold"),
                                 pady=10, padx=20).pack(fill="x")
                        tk.Label(_dlg, text=f"{len(unmatched)} video(s) without YouTube captions",
                                 bg=C_BG, fg=C_DIM, font=("Segoe UI", 9),
                                 padx=20).pack(fill="x")

                        _btn_frame = tk.Frame(_dlg, bg=C_BG, pady=10)
                        _btn_frame.pack(fill="x", padx=20)

                        _models = [
                            ("tiny",     "Fastest  (~120-200× realtime)",  "tiny"),
                            ("small",    "Fast  (~60-80× realtime)",     "small"),
                            ("medium",   "Balanced  (~28-40× realtime)",  "medium"),
                            ("large-v3", "Best quality  (~12-20× realtime)", "large-v3"),
                        ]

                        _DEFAULT_MODEL = "small"
                        _countdown = {"secs": 60, "job": None}

                        def _pick(m, dlg=_dlg):
                            if _countdown["job"]:
                                try:
                                    _dlg.after_cancel(_countdown["job"])
                                except Exception:
                                    pass
                            _model_result[0] = m
                            try:
                                dlg.destroy()
                            except Exception:
                                pass

                        # Countdown label
                        _timer_lbl = tk.Label(_dlg, text=f"Auto-selecting {_DEFAULT_MODEL} in 60s...",
                                              bg=C_BG, fg=C_DIM, font=("Segoe UI", 8),
                                              padx=20)
                        _timer_lbl.pack(fill="x", pady=(0, 4))

                        def _tick():
                            _countdown["secs"] -= 1
                            if _countdown["secs"] <= 0:
                                _model_timed_out[0] = True
                                _pick(_DEFAULT_MODEL)
                                return
                            try:
                                _timer_lbl.config(text=f"Auto-selecting {_DEFAULT_MODEL} in {_countdown['secs']}s...")
                                _countdown["job"] = _dlg.after(1000, _tick)
                            except Exception:
                                pass

                        _countdown["job"] = _dlg.after(1000, _tick)

                        for label, desc, model_id in _models:
                            _row = tk.Frame(_btn_frame, bg=C_BG)
                            _row.pack(fill="x", pady=2)
                            _b = tk.Button(_row, text=label, width=10,
                                           bg="#3a3a3a", fg=C_TEXT, activebackground="#555555",
                                           activeforeground=C_TEXT, relief="flat", bd=0,
                                           font=("Segoe UI", 9, "bold"), cursor="hand2",
                                           command=lambda m=model_id: _pick(m))
                            _b.pack(side="left", padx=(0, 8))
                            tk.Label(_row, text=desc, bg=C_BG, fg=C_DIM,
                                     font=("Segoe UI", 9)).pack(side="left")
                            if model_id == _DEFAULT_MODEL:
                                tk.Label(_row, text="(default)", bg=C_BG, fg="#5a8a5a",
                                         font=("Segoe UI", 8)).pack(side="left", padx=(4, 0))

                        # Skip button
                        tk.Frame(_dlg, bg=C_BG, height=5).pack()
                        tk.Button(_dlg, text="Skip Whisper (use only YT captions)",
                                  bg="#3a3a3a", fg=C_DIM, activebackground="#555555",
                                  activeforeground=C_TEXT, relief="flat", bd=0,
                                  font=("Segoe UI", 8), cursor="hand2",
                                  command=lambda: _pick("skip")).pack(pady=(0, 12))

                        _dlg.protocol("WM_DELETE_WINDOW", lambda: _pick(_DEFAULT_MODEL))

                    if _root_alive:
                        _ui_queue.append(_ask_model)
                        while _model_result[0] is None and not _ce.is_set():
                            time.sleep(0.1)

                if _ce.is_set():
                    log(f"\n  ⛔ Transcription cancelled.\n", "red")
                    # fall through — _whisper_available stays None, nothing will run
                elif _model_result[0] == "skip":
                    log(f"  Skipping {len(unmatched)} video(s) without YouTube captions.\n", "simpleline")
                    _whisper_available = False
                else:
                    _whisper_model_choice = _model_result[0]
                    # If model changed, stop old process so it relaunches with new model
                    _stop_whisper_process()
                    if _model_timed_out[0]:
                        log(f"  No user input, defaulting to {_whisper_model_choice} model\n", "simpleline")
                    else:
                        log(f"  Selected Whisper model: {_whisper_model_choice}\n", "simpleline")

                # Check if Whisper is available (only once)
                if not _ce.is_set() and _model_result[0] != "skip" and _whisper_available is None:
                    # First check if CUDA GPU is even present on this system
                    _has_cuda = False
                    try:
                        _cuda_check = subprocess.run(
                            [_WHISPER_PYTHON, "-c", "import torch; print(torch.cuda.is_available())"],
                            capture_output=True, text=True, timeout=30, startupinfo=startupinfo
                        ) if os.path.exists(_WHISPER_PYTHON) else None
                        _has_cuda = _cuda_check is not None and "True" in _cuda_check.stdout
                    except Exception:
                        pass

                    if not _has_cuda:
                        _whisper_available = False
                        log(f"  ⚠ No CUDA GPU detected — Whisper unavailable.\n", "simpleline")
                        log(f"  Skipping {len(unmatched)} video(s) without YouTube captions.\n", "simpleline")
                    else:
                        _whisper_available = _check_whisper_installed()
                        if not _whisper_available:
                            log("  Whisper AI is not installed (needed for files without captions).\n", "simpleline")
                            _install_result = [None]

                            def _ask_install():
                                _install_result[0] = messagebox.askyesno(
                                    "Install Whisper AI",
                                    f"{len(unmatched)} video(s) need Whisper AI for transcription.\n\n"
                                    "Whisper requires ~2.5 GB of downloads (plus model download on first use).\n\n"
                                    "Install now? (These videos will be skipped if you decline)")
                            if _root_alive:
                                _ui_queue.append(_ask_install)
                                while _install_result[0] is None and not _ce.is_set():
                                    time.sleep(0.1)
                                if _install_result[0]:
                                    _whisper_available = _install_whisper_blocking()
                                else:
                                    _whisper_available = False
                                    log(f"  Skipping {len(unmatched)} video(s) without captions.\n", "simpleline")

                if _whisper_available:
                    for fname, fpath in unmatched:
                        idx += 1

                        # Pause check
                        _pl = "GPU Tasks" if _sync_mode else "Sync"
                        if _pe.is_set() and not _ce.is_set():
                            log(f"  ⏸ {_pl} paused at {_fmt_time()} — click Resume.\n", "pausestatus")
                            while _pe.is_set() and not _ce.is_set():
                                time.sleep(0.25)
                            if not _ce.is_set():
                                clear_pause_status()
                                log(f"  ▶ {_pl} resumed at {_fmt_time()}...\n", "pauselog")
                        if _ce.is_set():
                            log(f"\n  ⛔ Transcription cancelled ({done_count}/{total} completed).\n", "red")
                            break

                        # Get duration BEFORE whisper (needed for progress %)
                        _dur_secs = 0
                        dur_str = ""
                        try:
                            probe_cmd = ["ffprobe", "-v", "quiet", "-show_entries",
                                         "format=duration", "-of", "csv=p=0", fpath]
                            probe_result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=30, startupinfo=startupinfo)
                            _dur_secs = float(probe_result.stdout.strip())
                            hrs, remainder = divmod(int(_dur_secs), 3600)
                            mins, sec = divmod(remainder, 60)
                            dur_str = f"{hrs}:{mins:02d}:{sec:02d}" if hrs else f"{mins}:{sec:02d}"
                        except Exception:
                            pass

                        if not _is_simple_mode:
                            log(f"  [{idx}/{total}] {fname} — using Whisper...\n", "transcribe_using")
                        _t_vid_start = time.time()
                        # Set counter for simple mode progress prefix
                        _whisper_counter["idx"] = idx
                        _whisper_counter["total"] = total
                        # Check if pause was requested — Whisper can't be interrupted mid-file,
                        # so inform the user it will pause after the current file finishes.
                        if _pe.is_set() and not _ce.is_set():
                            log(f"  ⏸ Pause requested — waiting for current transcription to finish...\n", "pauselog")
                        text, _vtt_segments = _whisper_transcribe(fpath, duration=_dur_secs, title=fname, cancel_ev=_ce, pause_ev=_pe)
                        _clear_whisper_progress()  # Remove progress line now that file is done
                        if text:
                            text = _whisper_punct_fixup(text)
                        source = "Whisper"

                        if not text:
                            # Write exclusion entry so this video is skipped on future transcribes
                            log(f"  [{idx}/{total}] {fname} — no speech detected, excluding.\n", "simpleline")
                            _transcription_log.append((fname, source, time.time() - _t_vid_start, "excluded — no speech"))
                            try:
                                _excl_mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
                                _excl_year, _excl_month = _excl_mtime.year, _excl_mtime.month
                                _excl_date = _excl_mtime.strftime("%Y%m%d")
                                _excl_txt, _excl_sub = _get_transcript_filename(
                                    ch_name, folder, split_years, split_months, combined,
                                    year=_excl_year, month=_excl_month)
                                os.makedirs(_excl_sub, exist_ok=True)
                                _excl_date_fmt = _format_upload_date(_excl_date)
                                _excl_dur_fmt = _format_duration(dur_str)
                                _excl_entry = f"===({fname}), {_excl_date_fmt}, {_excl_dur_fmt}, (NO AUDIO DATA — EXCLUDED)===\n[No speech detected]\n\n\n"
                                with open(_excl_txt, "a", encoding="utf-8") as _ef:
                                    _ef.write(_excl_entry)
                                _modified_txt_files.add(_excl_txt)
                                done_count += 1
                            except Exception as _excl_e:
                                log(f"    ⚠ Could not write exclusion: {_excl_e}\n", "red")
                                err_count += 1
                            continue

                        # Get date from file mtime
                        mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
                        year_num, month_num = mtime.year, mtime.month
                        upload_date = mtime.strftime("%Y%m%d")

                        txt_path, subfolder = _get_transcript_filename(
                            ch_name, folder, split_years, split_months, combined,
                            year=year_num, month=month_num)
                        os.makedirs(subfolder, exist_ok=True)

                        date_fmt = _format_upload_date(upload_date)
                        dur_fmt = _format_duration(dur_str)
                        _w_model_tag = f"WHISPER {_whisper_model_choice.upper()}"
                        entry = f"===({fname}), {date_fmt}, {dur_fmt}, ({_w_model_tag})===\n{text}\n\n\n"

                        try:
                            with open(txt_path, "a", encoding="utf-8") as f:
                                f.write(entry)
                            _modified_txt_files.add(txt_path)
                        except Exception as e:
                            log(f"  ⚠ Error writing transcript: {e}\n", "red")
                            _transcription_log.append((fname, source, time.time() - _t_vid_start, str(e)))
                            err_count += 1
                            continue

                        # Write hidden JSONL with timestamps for searchability
                        if _vtt_segments:
                            # Whisper videos don't have a YouTube video_id — use empty string
                            _jsonl_path = _get_jsonl_path(txt_path)
                            _write_jsonl_entry(_jsonl_path, "", fname, _vtt_segments)

                        _vid_elapsed = time.time() - _t_vid_start
                        _transcription_log.append((fname, source, _vid_elapsed, None))
                        done_count += 1
                        _ve_m, _ve_s = divmod(int(_vid_elapsed), 60)
                        _ve_str = f"took {_ve_m}min {_ve_s:02d}sec" if _ve_m else f"took {_ve_s}sec"
                        _src_part = f"{source},"
                        if _is_simple_mode:
                            _prefix = f"  [{idx}/{total}] "
                            _suffix = f"done ({_src_part} {_ve_str})"
                            _body_width = 58 - len(_prefix)
                            _name_dash = f"{fname} — "
                            if len(_name_dash) > _body_width:
                                _name_dash = fname[:_body_width - 6] + "... — "
                            else:
                                _name_dash = _name_dash.ljust(_body_width)
                            log(f"{_prefix}{_name_dash}{_suffix}\n", "simpleline_blue")
                        else:
                            log(f"  [{idx}/{total}] {fname} — done ({_src_part} {_ve_str})\n", "simpleline_blue")
                else:
                    err_count += len(unmatched)

            # ── Post-process: sort entries chronologically in each .txt file ──
            if _modified_txt_files and done_count > 0:
                _sort_transcript_entries(_modified_txt_files)

            # Cleanup temp dir
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass

            if not _ce.is_set():
                log(f"\n  ✓ Transcription complete: {done_count} done", "simpleline_green")
                if err_count:
                    log(f", {err_count} skipped", "simpleline_green")
                log(".\n", "simpleline_green")
                # Mark channel as fully transcribed (only if not cancelled)
                if err_count == 0:
                    with config_lock:
                        for _cfg_ch in config.get("channels", []):
                            if _cfg_ch.get("url") == ch_url:
                                _cfg_ch["transcription_complete"] = True
                                _cfg_ch["transcription_pending"] = 0
                                break
                        save_config(config)

            # ── Transcription Summary ──
            if _transcription_log:
                _t_total_elapsed = time.time() - _t_total_start
                _t_hrs, _t_rem = divmod(int(_t_total_elapsed), 3600)
                _t_mins, _t_secs = divmod(_t_rem, 60)
                if _t_hrs:
                    _total_str = f"{_t_hrs}h {_t_mins}m {_t_secs}s"
                elif _t_mins:
                    _total_str = f"{_t_mins}m {_t_secs}s"
                else:
                    _total_str = f"{_t_secs}s"

                log("\n" + "=" * 50 + "\n", "summary")
                log(f"TRANSCRIPTION SUMMARY: {ch_name}\n", "summary")
                log(f"Total time: {_total_str}\n", "summary")
                log(f"Completed: {done_count}  |  Errors: {err_count}\n", "summary")
                log("-" * 50 + "\n", "summary")
                for _tl_name, _tl_source, _tl_secs, _tl_err in _transcription_log:
                    _tl_m, _tl_s = divmod(int(_tl_secs), 60)
                    _tl_time = f"{_tl_m}m {_tl_s:02d}s" if _tl_m else f"{_tl_s}s"
                    if _tl_err:
                        log(f"  ✗ {_tl_name}  [{_tl_source}]  {_tl_time}  ERROR: {_tl_err}\n", "red")
                    else:
                        log(f"  ✓ {_tl_name}  [{_tl_source}]  {_tl_time}\n", "dim")
                log("=" * 50 + "\n", "summary")

            # Record in autorun history
            if done_count > 0 or err_count > 0:
                _t_rec_elapsed = time.time() - _t_total_start
                _record_transcription(done_count + _prior_done, err_count, _t_rec_elapsed,
                                      channel_name=ch_name, skipped=0)

        except Exception as e:
            log(f"\n  ⚠ Transcription error: {e}\n", "red")
        finally:
            _transcribe_running = False
            _transcribe_sync_controlled = False
            _clear_whisper_progress()  # Remove whisper progress line after transcription completes
            if not _sync_mode:
                _stop_whisper_process()  # Free GPU memory (skip in sync_mode — GPU worker manages this)
                _stop_punct_process()   # Free GPU memory
                if not _sync_running:
                    _tray_stop_spin()
                    _update_tray_tooltip("YT Archiver — Idle")
                    _current_job["label"] = None
                    _current_job["url"] = None
                _update_queue_btn()
            _ui_queue.append(refresh_channel_dropdowns)
            if not _sync_mode:
                _ui_queue.append(_sync_task_finished)
            # Process any queued jobs in insertion order (only when not in sync_mode)
            if not _sync_mode:
                if _skip_current.is_set():
                    _skip_current.clear()
                    cancel_event.clear()
                    _process_next_queued()
                elif not _ce.is_set():
                    _process_next_queued()

    if _sync_mode:
        _worker()  # Run synchronously — caller is the GPU worker thread
    else:
        threading.Thread(target=_worker, daemon=True).start()


def _process_mt_queue():
    """Process next queued manual transcription. Returns True if one was started."""
    with _mt_queue_lock:
        if not _mt_queue:
            return False
        file_path = _mt_queue.pop(0)
        remaining = len(_mt_queue)
    with _queue_order_lock:
        try:
            _queue_order.remove(("mt", file_path))
        except ValueError:
            pass
    fname = os.path.splitext(os.path.basename(file_path))[0]
    log(f"\n=== Processing queued manual transcription: {fname}", "header")
    if remaining:
        log(f" ({remaining} more in queue)", "header")
    log(f" ===\n", "header")
    _run_manual_transcription(file_path)
    return True


def _start_manual_transcription():
    """Prompt user to select a local video file or folder, then add to GPU Tasks queue."""
    _choice = [None]  # "file", "folder", or None (cancelled)

    if root.winfo_exists():
        _dlg = tk.Toplevel(root)
        _dlg.title("Manual Transcription")
        _dlg.configure(bg=C_BG)
        _dlg.resizable(False, False)
        _dlg.transient(root)
        _dlg.grab_set()
        _dlg.update_idletasks()
        _apply_dark_title_bar(_dlg)
        _rx = root.winfo_rootx() + root.winfo_width() // 2
        _ry = root.winfo_rooty() + root.winfo_height() // 2
        _dlg.geometry(f"+{_rx - 140}+{_ry - 60}")

        tk.Label(_dlg, text="Select a single file or an entire folder?",
                 bg=C_BG, fg=C_TEXT, font=("Segoe UI", 10, "bold"),
                 pady=12, padx=20).pack(fill="x")

        _btn_row = tk.Frame(_dlg, bg=C_BG)
        _btn_row.pack(fill="x", padx=20, pady=(0, 14))

        def _pick(val):
            _choice[0] = val
            try:
                _dlg.destroy()
            except Exception:
                pass

        for lbl, val in [("\U0001F4C4  File", "file"), ("\U0001F4C1  Folder", "folder")]:
            b = tk.Button(_btn_row, text=lbl, bg=C_BTN, fg=C_TEXT,
                          activebackground=C_BTN_HVR, activeforeground=C_TEXT,
                          font=("Segoe UI", 10), relief="flat", bd=0,
                          cursor="hand2", padx=16, pady=6,
                          command=lambda v=val: _pick(v))
            b.pack(side="left", expand=True, fill="x", padx=4)

        _cancel_btn = tk.Button(_btn_row, text="Cancel", bg=C_BTN, fg=C_DIM,
                                activebackground=C_BTN_HVR, activeforeground=C_TEXT,
                                font=("Segoe UI", 9), relief="flat", bd=0,
                                cursor="hand2", padx=10, pady=6,
                                command=lambda: _pick(None))
        _cancel_btn.pack(side="left", padx=(8, 4))

        _dlg.protocol("WM_DELETE_WINDOW", lambda: _pick(None))
        root.wait_window(_dlg)

    if _choice[0] is None:
        return  # cancelled

    if _choice[0] == "file":
        file_path = filedialog.askopenfilename(
            title="Select video file to transcribe",
            filetypes=[
                ("Video files", "*.mp4 *.mkv *.webm *.avi *.mov *.flv *.wmv *.m4v"),
                ("Audio files", "*.wav *.mp3 *.m4a *.flac *.ogg *.aac"),
                ("All files", "*.*"),
            ]
        )
        if not file_path:
            return
        file_path = os.path.normpath(file_path)

        with _gpu_queue_lock:
            _cur_item = _gpu_current_item
            if any(item.get("file_path") == file_path and item.get("type") == "mt" for item in _gpu_queue) or \
               (_cur_item and _cur_item.get("file_path") == file_path and _cur_item.get("type") == "mt"):
                log(f"  File already in GPU Tasks queue.\n", "simpleline")
                return

        _add_to_gpu_queue({"type": "mt", "file_path": file_path})
    else:  # folder
        folder_path = filedialog.askdirectory(title="Select folder to transcribe")
        if not folder_path:
            return
        folder_path = os.path.normpath(folder_path)

        # Count video files
        _vid_exts = {".mp4", ".mkv", ".webm", ".avi", ".mov", ".flv", ".wmv", ".m4v",
                     ".wav", ".mp3", ".m4a", ".flac", ".ogg", ".aac"}
        vid_count = sum(1 for f in os.listdir(folder_path)
                        if os.path.isfile(os.path.join(folder_path, f))
                        and os.path.splitext(f)[1].lower() in _vid_exts)
        if vid_count == 0:
            log(f"  No video/audio files found in folder.\n", "red")
            return

        with _gpu_queue_lock:
            _cur_item = _gpu_current_item
            if any(item.get("folder_path") == folder_path and item["type"] == "mt" for item in _gpu_queue) or \
               (_cur_item and _cur_item.get("folder_path") == folder_path and _cur_item.get("type") == "mt"):
                log(f"  Folder already in GPU Tasks queue.\n", "simpleline")
                return

        folder_name = os.path.basename(folder_path)
        _add_to_gpu_queue({"type": "mt", "folder_path": folder_path, "folder_name": folder_name,
                           "vid_count": vid_count})


def _run_manual_transcription(file_path, cancel_ev=None, pause_ev=None,
                              skip_model_dialog=False, _sync_mode=False):
    """Run manual transcription of a single local file in a background thread.

    cancel_ev/pause_ev: optional threading.Events (default to global cancel_event/pause_event).
    skip_model_dialog: if True, skip model selection and use current _whisper_model_choice.
    _sync_mode: if True, run the worker body directly (blocking) instead of spawning a thread.
    """
    global _transcribe_running, _whisper_model_choice

    fname = os.path.splitext(os.path.basename(file_path))[0]

    def _worker():
        global _transcribe_running, _transcribe_sync_controlled, _whisper_model_choice
        _ce = cancel_ev or cancel_event
        _pe = pause_ev or pause_event
        _transcribe_running = True
        _transcribe_sync_controlled = (cancel_ev is None)
        if not _sync_mode:
            _current_job["label"] = f"M.T. {fname}"
            _current_job["url"] = None
            _update_queue_btn()
        _tray_start_spin(red=True)
        _update_tray_tooltip(f"YT Archiver \u2014 Transcribing {fname}")

        try:
            _ce.clear()
            if not _sync_mode:
                _ui_queue.append(_update_queue_btn)

            log(f"\n{'='*60}\n", "header")
            log(f"  MANUAL TRANSCRIPTION: {fname}\n", "header")
            log(f"{'='*60}\n\n", "header")

            if not os.path.isfile(file_path):
                log(f"  File not found: {file_path}\n", "red")
                return

            out_dir = os.path.dirname(file_path)
            out_path = os.path.join(out_dir, fname + ".txt")
            if os.path.isfile(out_path):
                log(f"  Transcript already exists: {os.path.basename(out_path)}\n", "simpleline")
                log(f"  Overwriting...\n", "simpleline")

            if skip_model_dialog:
                # GPU Tasks mode — model already chosen
                _stop_whisper_process()
                log(f"  Using Whisper model: {_whisper_model_choice}\n", "simpleline")
            else:
                # Model selection dialog
                model_choice, timed_out = _ask_whisper_model_dialog(
                    prompt_text="Which Whisper model to use?",
                    subtitle_text=f"Transcribing: {fname}"
                )
                if _ce.is_set() or model_choice is None:
                    log(f"\n  Cancelled.\n", "red")
                    return

                _whisper_model_choice = model_choice
                _stop_whisper_process()

                if timed_out:
                    log(f"  No user input, defaulting to {_whisper_model_choice} model\n", "simpleline")
                else:
                    log(f"  Selected Whisper model: {_whisper_model_choice}\n", "simpleline")

            # Check CUDA availability
            _has_cuda = False
            try:
                _cuda_check = subprocess.run(
                    [_WHISPER_PYTHON, "-c", "import torch; print(torch.cuda.is_available())"],
                    capture_output=True, text=True, timeout=30, startupinfo=startupinfo
                ) if os.path.exists(_WHISPER_PYTHON) else None
                _has_cuda = _cuda_check is not None and "True" in _cuda_check.stdout
            except Exception:
                pass

            if not _has_cuda:
                log(f"  \u26a0 No CUDA GPU detected \u2014 Whisper unavailable.\n", "red")
                return

            # Check Whisper installation
            if not _check_whisper_installed():
                log("  Whisper AI is not installed.\n", "red")
                _install_result = [None]

                def _ask_install():
                    _install_result[0] = messagebox.askyesno(
                        "Install Whisper AI",
                        "Whisper AI is required for transcription.\n\n"
                        "This requires ~2.5 GB of downloads (plus model download on first use).\n\n"
                        "Install now?")

                _ui_queue.append(_ask_install)
                while _install_result[0] is None and not _ce.is_set():
                    time.sleep(0.1)
                if not _install_result[0]:
                    log(f"  Transcription cancelled \u2014 Whisper not installed.\n", "red")
                    return
                if not _install_whisper_blocking():
                    log(f"  Failed to install Whisper.\n", "red")
                    return

            # Get duration for progress reporting
            _dur_secs = 0
            try:
                probe_cmd = ["ffprobe", "-v", "quiet", "-show_entries",
                             "format=duration", "-of", "csv=p=0", file_path]
                probe_result = subprocess.run(probe_cmd, capture_output=True,
                                              text=True, timeout=30, startupinfo=startupinfo)
                _dur_secs = float(probe_result.stdout.strip())
            except Exception:
                pass

            if _ce.is_set():
                log(f"\n  Cancelled.\n", "red")
                return

            _whisper_counter["idx"] = 1
            _whisper_counter["total"] = 1
            _t_start = time.time()
            log(f"  Transcribing with Whisper ({_whisper_model_choice})...\n", "simpleline")

            text, _ = _whisper_transcribe(file_path, duration=_dur_secs, title=fname,
                                          cancel_ev=_ce, pause_ev=_pe)
            _clear_whisper_progress()  # Remove progress line now that file is done

            if not text:
                log(f"  Whisper returned empty result.\n", "red")
                return

            text = _whisper_punct_fixup(text)

            try:
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(text)
            except Exception as e:
                log(f"  Error writing transcript: {e}\n", "red")
                return

            elapsed = time.time() - _t_start
            _m, _s = divmod(int(elapsed), 60)
            _time_str = f"{_m}min {_s:02d}sec" if _m else f"{_s}sec"
            log(f"\n  Transcript saved: {os.path.basename(out_path)} ({_time_str})\n", "simpleline_blue")

        except Exception as e:
            log(f"\n  Manual transcription error: {e}\n", "red")
        finally:
            _transcribe_running = False
            _transcribe_sync_controlled = False
            _clear_whisper_progress()  # Remove whisper progress line after transcription completes
            if not _sync_mode:
                _stop_whisper_process()
                if not _sync_running:
                    _tray_stop_spin()
                    _update_tray_tooltip("YT Archiver \u2014 Idle")
                    _current_job["label"] = None
                    _current_job["url"] = None
                _update_queue_btn()
            if not _sync_mode:
                _ui_queue.append(_sync_task_finished)
            if not _sync_mode:
                if _skip_current.is_set():
                    _skip_current.clear()
                    cancel_event.clear()
                    _process_next_queued()
                elif not _ce.is_set():
                    _process_next_queued()

    if _sync_mode:
        _worker()  # Run synchronously — caller is the GPU worker thread
    else:
        threading.Thread(target=_worker, daemon=True).start()


def _run_manual_transcription_folder(folder_path, folder_name, cancel_ev=None, pause_ev=None,
                                      _sync_mode=False):
    """Run manual transcription of all video/audio files in a folder.

    All files are transcribed and written to a single (foldername) Transcript.txt
    using the same entry format as channel transcription.
    Always runs via GPU Tasks (_sync_mode=True from GPU worker).
    """
    global _transcribe_running, _whisper_model_choice

    _VID_EXTS = {".mp4", ".mkv", ".webm", ".avi", ".mov", ".flv", ".wmv", ".m4v",
                 ".wav", ".mp3", ".m4a", ".flac", ".ogg", ".aac"}

    def _worker():
        global _transcribe_running, _transcribe_sync_controlled, _whisper_model_choice
        _ce = cancel_ev or cancel_event
        _pe = pause_ev or pause_event
        _transcribe_running = True
        _transcribe_sync_controlled = (cancel_ev is None)
        _tray_start_spin(red=True)
        _update_tray_tooltip(f"YT Archiver — Transcribing {folder_name}")

        try:
            _ce.clear()

            log(f"\n{'='*60}\n", "header")
            log(f"  MANUAL TRANSCRIPTION (FOLDER): {folder_name}\n", "header")
            log(f"{'='*60}\n\n", "header")

            if not os.path.isdir(folder_path):
                log(f"  Folder not found: {folder_path}\n", "red")
                return

            # Gather video/audio files, sorted chronologically by file modification date
            all_files = [f for f in os.listdir(folder_path)
                         if os.path.isfile(os.path.join(folder_path, f))
                         and os.path.splitext(f)[1].lower() in _VID_EXTS]
            files = sorted(all_files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))

            if not files:
                log(f"  No video/audio files found in folder.\n", "red")
                return

            out_path = os.path.join(folder_path, f"{folder_name} Transcript.txt")

            # Check existing transcript for already-transcribed files (skip on re-run)
            _existing_titles = set()
            if os.path.isfile(out_path):
                try:
                    with open(out_path, "r", encoding="utf-8") as _ef:
                        for _line in _ef:
                            _m = re.match(r'^===\((.+?)\),', _line)
                            if _m:
                                _existing_titles.add(_m.group(1))
                except Exception:
                    pass
            if _existing_titles:
                _before_count = len(files)
                files = [f for f in files if os.path.splitext(f)[0] not in _existing_titles]
                _skipped_existing = _before_count - len(files)
                if _skipped_existing:
                    log(f"  Skipping {_skipped_existing} already-transcribed file(s)\n", "simpleline")
                if not files:
                    log(f"  All files already transcribed.\n", "simpleline_green")
                    return

            log(f"  Found {len(files)} files to transcribe\n", "simpleline")
            log(f"  Output: {folder_name} Transcript.txt\n\n", "simpleline")

            # Check CUDA availability
            _has_cuda = False
            try:
                _cuda_check = subprocess.run(
                    [_WHISPER_PYTHON, "-c", "import torch; print(torch.cuda.is_available())"],
                    capture_output=True, text=True, timeout=30, startupinfo=startupinfo
                ) if os.path.exists(_WHISPER_PYTHON) else None
                _has_cuda = _cuda_check is not None and "True" in _cuda_check.stdout
            except Exception:
                pass

            if not _has_cuda:
                log(f"  ⚠ No CUDA GPU detected — Whisper unavailable.\n", "red")
                return

            # Check Whisper installation
            if not _check_whisper_installed():
                log("  Whisper AI is not installed.\n", "red")
                _install_result = [None]

                def _ask_install():
                    _install_result[0] = messagebox.askyesno(
                        "Install Whisper AI",
                        "Whisper AI is required for transcription.\n\n"
                        "This requires ~2.5 GB of downloads (plus model download on first use).\n\n"
                        "Install now?")

                _ui_queue.append(_ask_install)
                while _install_result[0] is None and not _ce.is_set():
                    time.sleep(0.1)
                if not _install_result[0]:
                    log(f"  Transcription cancelled — Whisper not installed.\n", "red")
                    return
                if not _install_whisper_blocking():
                    log(f"  Failed to install Whisper.\n", "red")
                    return

            _stop_whisper_process()
            log(f"  Using Whisper model: {_whisper_model_choice}\n\n", "simpleline")

            _t_start = time.time()
            _whisper_counter["idx"] = 0
            _whisper_counter["total"] = len(files)
            _transcribed = 0
            _skipped = 0

            for i, filename in enumerate(files):
                if _ce.is_set():
                    log(f"\n  Cancelled.\n", "red")
                    return

                # Handle pause
                while _pe.is_set() and not _ce.is_set():
                    time.sleep(0.5)
                if _ce.is_set():
                    log(f"\n  Cancelled.\n", "red")
                    return

                fpath = os.path.join(folder_path, filename)
                fname = os.path.splitext(filename)[0]
                _whisper_counter["idx"] = i + 1

                log(f"  [{i+1}/{len(files)}] {fname}\n", "simpleline")

                # Get duration
                dur_str = ""
                _dur_secs = 0
                try:
                    probe_cmd = ["ffprobe", "-v", "quiet", "-show_entries",
                                 "format=duration", "-of", "csv=p=0", fpath]
                    probe_result = subprocess.run(probe_cmd, capture_output=True,
                                                  text=True, timeout=30, startupinfo=startupinfo)
                    _dur_secs = float(probe_result.stdout.strip())
                    hrs, remainder = divmod(int(_dur_secs), 3600)
                    mins, sec = divmod(remainder, 60)
                    dur_str = f"{hrs}:{mins:02d}:{sec:02d}" if hrs else f"{mins}:{sec:02d}"
                except Exception:
                    pass

                # Get date from file mtime
                try:
                    mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
                    upload_date = mtime.strftime("%Y%m%d")
                except Exception:
                    upload_date = ""

                # Transcribe
                text, _ = _whisper_transcribe(fpath, duration=_dur_secs, title=fname,
                                              cancel_ev=_ce, pause_ev=_pe)
                _clear_whisper_progress()  # Remove progress line now that file is done

                if _ce.is_set():
                    log(f"\n  Cancelled.\n", "red")
                    return

                if text:
                    text = _whisper_punct_fixup(text)

                if not text:
                    # Write exclusion entry so this video is skipped on future transcribes
                    log(f"    no speech detected, excluding.\n", "simpleline")
                    try:
                        _excl_date_fmt = _format_upload_date(upload_date)
                        _excl_dur_fmt = _format_duration(dur_str)
                        _excl_entry = f"===({fname}), {_excl_date_fmt}, {_excl_dur_fmt}, (NO AUDIO DATA — EXCLUDED)===\n[No speech detected]\n\n\n"
                        with open(out_path, "a", encoding="utf-8") as _ef:
                            _ef.write(_excl_entry)
                        _transcribed += 1
                    except Exception:
                        _skipped += 1
                    continue

                # Build entry in channel transcription format
                date_fmt = _format_upload_date(upload_date)
                dur_fmt = _format_duration(dur_str)
                _w_model_tag = f"WHISPER {_whisper_model_choice.upper()}"
                entry = f"===({fname}), {date_fmt}, {dur_fmt}, ({_w_model_tag})===\n{text}\n\n\n"

                try:
                    with open(out_path, "a", encoding="utf-8") as f:
                        f.write(entry)
                    _transcribed += 1
                except Exception as e:
                    log(f"    ⚠ Error writing transcript: {e}\n", "red")
                    _skipped += 1

            # Sort the transcript file by date (newest first)
            if _transcribed > 0 and os.path.isfile(out_path):
                try:
                    _sort_transcript_entries(out_path)
                except Exception:
                    pass

            elapsed = time.time() - _t_start
            _m, _s = divmod(int(elapsed), 60)
            _h, _m = divmod(_m, 60)
            if _h:
                _time_str = f"{_h}hr {_m:02d}min {_s:02d}sec"
            elif _m:
                _time_str = f"{_m}min {_s:02d}sec"
            else:
                _time_str = f"{_s}sec"

            log(f"\n  Folder transcription complete: {_transcribed} transcribed", "simpleline_green")
            if _skipped:
                log(f", {_skipped} skipped", "simpleline_green")
            log(f" ({_time_str})\n", "simpleline_green")
            log(f"  Saved: {folder_name} Transcript.txt\n", "simpleline_green")

        except Exception as e:
            log(f"\n  Manual folder transcription error: {e}\n", "red")
        finally:
            _transcribe_running = False
            _transcribe_sync_controlled = False
            _clear_whisper_progress()  # Remove whisper progress line after transcription completes
            if not _sync_mode:
                _stop_whisper_process()
                if not _sync_running:
                    _tray_stop_spin()
                    _update_tray_tooltip("YT Archiver — Idle")

    if _sync_mode:
        _worker()
    else:
        threading.Thread(target=_worker, daemon=True).start()


# --- Compress row (Row 5) ---
compress_row = ttk.Frame(add_outer, style="Raised.TFrame")
compress_row.grid(row=5, column=0, columnspan=9, sticky="w", padx=(4, 8), pady=(2, 4))

new_compress_var = tk.BooleanVar(value=False)
new_compress_level_var = tk.StringVar(value="")
new_compress_res_var = tk.StringVar(value="")
new_compress_batch_var = tk.StringVar(value="20")

compress_cb = ttk.Checkbutton(compress_row, text="Compress after download",
                               variable=new_compress_var)
compress_cb.pack(side="left", padx=(4, 8))

_COMPRESS_RES_OPTIONS = ["Original", "144p", "240p", "360p", "480p", "720p", "1080p"]
_compress_res_label = tk.Label(compress_row, text="Res:", bg=C_SURFACE, fg=C_DIM,
                                font=("Segoe UI", 9))
_compress_res_combo = _combo(compress_row, textvariable=new_compress_res_var,
                              values=_COMPRESS_RES_OPTIONS, state="readonly", width=8)
new_compress_res_var.set("")
_ToolTip(_compress_res_combo, "Output resolution (Original = keep source resolution)")

_compress_quality_label = tk.Label(compress_row, text="Quality:", bg=C_SURFACE, fg=C_DIM,
                                    font=("Segoe UI", 9))
_compress_quality_combo = _combo(compress_row, textvariable=new_compress_level_var,
                                  values=[], state="disabled", width=14)
_quality_tooltip = _ToolTip(_compress_quality_combo,
    "Select a resolution first to see quality options")

_compress_batch_label = tk.Label(compress_row, text="Batch:", bg=C_SURFACE, fg=C_DIM,
                                  font=("Segoe UI", 9))
_compress_batch_combo = _combo(compress_row, textvariable=new_compress_batch_var,
                                values=["1", "5", "10", "20", "50"], state="readonly", width=4)
new_compress_batch_var.set("20")
_ToolTip(_compress_batch_combo, "Compress every N downloads during bulk syncs.\nSmaller = more frequent GPU tasks, larger = fewer.\nSync will skip a channel if it has 5+ unprocessed\nbatches in GPU Tasks to avoid filling storage.")


def _on_compress_res_change(*_):
    """When resolution changes, enable Quality combo and update its tooltip."""
    res_raw = new_compress_res_var.get()
    if not res_raw:
        _compress_quality_combo.config(state="disabled")
        _compress_quality_combo["values"] = []
        new_compress_level_var.set("")
        _quality_tooltip.text = "Select a resolution first to see quality options"
        return
    res_key = res_raw.replace("p", "") if res_raw != "Original" else "1080"
    presets = _COMPRESS_PRESETS.get(res_key, _COMPRESS_PRESETS["1080"])
    _compress_quality_combo.config(state="readonly")
    _compress_quality_combo["values"] = _QUALITY_OPTIONS
    current_q = new_compress_level_var.get()
    if current_q not in presets:
        new_compress_level_var.set("Average")
    tip_lines = " · ".join(f"{q} = ~{presets[q]} MB/hr" for q in _QUALITY_OPTIONS)
    _quality_tooltip.text = f"{tip_lines}\nAV1 NVENC, preset p6, two-pass, 128k AAC audio"

new_compress_res_var.trace_add("write", _on_compress_res_change)


def _toggle_compress_entry(*_):
    if new_compress_var.get():
        _compress_res_label.pack(side="left", padx=(0, 4))
        _compress_res_combo.pack(side="left", padx=(0, 8))
        _compress_quality_label.pack(side="left", padx=(0, 4))
        _compress_quality_combo.pack(side="left", padx=(0, 8))
        _compress_batch_label.pack(side="left", padx=(0, 4))
        _compress_batch_combo.pack(side="left", padx=(0, 8))
    else:
        _compress_res_label.pack_forget()
        _compress_res_combo.pack_forget()
        _compress_quality_label.pack_forget()
        _compress_quality_combo.pack_forget()
        _compress_batch_label.pack_forget()
        _compress_batch_combo.pack_forget()
        new_compress_level_var.set("")
        new_compress_res_var.set("")
        new_compress_batch_var.set("20")


new_compress_var.trace_add("write", _toggle_compress_entry)

action_btn_frame = ttk.Frame(add_outer, style="Raised.TFrame")
action_btn_frame.grid(row=6, column=0, columnspan=9, sticky="w", padx=8, pady=(4, 8))

add_channel_btn = ttk.Button(action_btn_frame, text="Add channel", command=add_channel, state="disabled")
add_channel_btn.pack(side="left", padx=(0, 8))

sync_single_btn = ttk.Button(action_btn_frame, text="▶ Sync this channel", command=sync_single_channel,
                             style="Sync.TButton", state="disabled")
# sync_single_btn intentionally not packed — user can right-click channel to sync

remove_channel_btn = ttk.Button(action_btn_frame, text="⛔Remove selected", command=remove_channel, style="Cancel.TButton",
                                state="disabled")

cancel_edit_btn = ttk.Button(action_btn_frame, text="Cancel edit", command=_clear_edit_mode)
cancel_edit_btn.pack(side="left", padx=(0, 8))
cancel_edit_btn.pack_forget()

# ─── Show/hide add-channel detail options ─────────────────
_add_details_visible = True
_add_detail_widgets = []  # widgets to toggle, populated on first hide

def _set_add_details_visible(show):
    """Show or hide the channel detail options (resolution, mode, duration, etc.)."""
    global _add_details_visible
    if show == _add_details_visible:
        return
    _add_details_visible = show
    if show:
        # Restore all previously hidden widgets
        for child in _add_detail_widgets:
            try:
                child.grid()
            except Exception:
                pass
    else:
        # Collect and hide widgets in rows >= 2 (plus Duration Limit header in row 1)
        _add_detail_widgets.clear()
        for child in add_outer.winfo_children():
            try:
                info = child.grid_info()
                if not info:
                    continue
                row = int(info.get("row", 0))
                if row >= 2 or (row == 1 and child != url_error_label):
                    _add_detail_widgets.append(child)
                    child.grid_remove()
            except Exception:
                pass

def _check_add_details_visibility(*_):
    """Show detail options when user types in URL/name fields or is editing a channel."""
    try:
        n = _real_get(new_name_entry).strip()
        u = _real_get(_new_url_entry).strip()
        editing = _editing_channel.get("name")
        _set_add_details_visible(bool(n or u or editing))
    except Exception:
        pass

new_name_var.trace_add("write", _check_add_details_visibility)
new_url_var.trace_add("write", _check_add_details_visibility)

# Start collapsed
_set_add_details_visible(False)
# ─── End show/hide add-channel detail options ─────────────

ttk.Separator(tab_settings, orient="horizontal").grid(row=5, column=0, columnspan=3, sticky="ew", padx=12, pady=10)

# Mini log for Subs tab
_subs_mini_log_frame = ttk.Frame(tab_settings)
_subs_mini_log_frame.grid(row=6, column=0, columnspan=3, sticky="ew", padx=12, pady=(0, 8))
_subs_mini_log_frame.columnconfigure(0, weight=1)

subs_mini_log = tk.Text(_subs_mini_log_frame, state="disabled", height=4,
                         bg=C_LOG_BG, fg=C_TEXT, font=("Consolas", 9),
                         relief="flat", bd=0, highlightthickness=1,
                         highlightbackground=C_BORDER, highlightcolor=C_BORDER,
                         padx=8, pady=4, wrap="none")
subs_mini_log.grid(row=0, column=0, sticky="ew")

for _tag_name, _tag_cfg in [("green", {"foreground": C_LOG_GREEN}),
                             ("red", {"foreground": C_LOG_RED}),
                             ("header", {"foreground": C_LOG_HEAD, "font": ("Consolas", 9, "bold")}),
                             ("summary", {"foreground": C_LOG_SUM, "font": ("Consolas", 9, "italic")}),
                             ("simpleline", {"foreground": C_TEXT}),
                             ("simpleline_green", {"foreground": C_LOG_GREEN}),
                             ("simpleline_blue", {"foreground": C_LOG_BLUE}),
                             ("simpledownload", {"foreground": C_LOG_GREEN}),
                             ("simplestatus", {"foreground": C_LOG_HEAD, "font": ("Consolas", 9, "bold")}),
                             ("simplestatus_green", {"foreground": C_LOG_GREEN, "font": ("Consolas", 9, "bold")}),
                             ("dlprogress_pct", {"foreground": C_LOG_GREEN}),
                             ("pauselog", {"foreground": C_LOG_HEAD}),
                             ("pausestatus", {"foreground": C_LOG_HEAD}),
                             ("livestream", {"foreground": "#f5a023", "font": ("Consolas", 9, "bold")}),
                             ("filterskip", {"foreground": C_LOG_SUM}),
                             ("whisper_progress", {"foreground": C_LOG_BLUE}),
                             ("whisper_pct", {"foreground": C_LOG_GREEN, "font": ("Consolas", 9, "bold")}),
                             ("whisper_dots", {"foreground": C_LOG_BLUE}),
                             ("encode_progress", {"foreground": C_LOG_BLUE}),
                             ("encode_pct", {"foreground": C_LOG_GREEN, "font": ("Consolas", 9, "bold")}),
                             ("encode_dots", {"foreground": C_LOG_BLUE}),
                             ("encode_suffix", {"foreground": C_LOG_BLUE}),
                             ("transcribe_using", {"foreground": C_LOG_BLUE})]:
    subs_mini_log.tag_configure(_tag_name, **_tag_cfg)


def get_channel_by_name(name):
    with config_lock:
        for c in config.get("channels", []):
            if c["name"] == name:
                return copy.deepcopy(c)
    return None


def on_channel_select(event):
    ch = get_channel_by_name(chan_var.get())
    if ch:
        url_var.set(ch["url"])
        ch_res_var.set(ch.get("resolution", CHANNEL_DEFAULTS["resolution"]))
        _min = ch.get("min_duration", 0)
        ch_dur_var.set(str(_min // 60) if _min else "")
        _mx_secs = ch.get("max_duration", 0)
        ch_maxdur_var.set(str(_mx_secs // 60) if _mx_secs else "")
        _m = ch.get("mode", CHANNEL_DEFAULTS["mode"])
        mode_var.set(_m)
        ch_all_var.set(_m == "full")
        ch_fromdate_var.set(_m == "date")
        folder_override_var.set(ch.get("folder_override", ""))
        save_prefs_btn.config(state="normal")
        save_prefs_label_var.set(f"saving to: {ch['name']}")


chan_dropdown.bind("<<ComboboxSelected>>", on_channel_select)

current_url_type = {"t": "unknown"}
debounce_id = None


def on_url_change(*_):
    global debounce_id
    if debounce_id:
        root.after_cancel(debounce_id)
    debounce_id = root.after(350, process_url_update)


def process_url_update():
    url = _real_get(url_entry).strip()
    kind = detect_url_type(url) if url else "unknown"
    ch = get_channel_by_name(chan_var.get())

    if ch and ch["url"] != url:
        chan_var.set("")
        save_prefs_btn.config(state="disabled")
        save_prefs_label_var.set("")
        folder_override_var.set("")

    if kind == current_url_type["t"]: return
    current_url_type["t"] = kind

    channel_panel.grid_remove()
    channel_nudge_panel.grid_remove()
    video_panel.grid_remove()
    unknown_panel.grid_remove()

    if kind == "channel":
        url_type_label.config(text="")
        channel_nudge_panel.grid(row=1, column=0, sticky="ew", padx=12, pady=8)
    elif kind == "video":
        url_type_label.config(text="🎬  Video detected")
        video_panel.grid(row=1, column=0, sticky="ew", padx=12, pady=2)
    else:
        url_type_label.config(text="")
        unknown_panel.grid(row=1, column=0, sticky="ew", padx=12, pady=2)


unknown_panel.grid(row=1, column=0, sticky="ew", padx=12, pady=2)


def _validate_add_btn():
    try:
        if 'add_channel_btn' not in globals() or not add_channel_btn.winfo_exists():
            return

        n = _real_get(new_name_entry).strip()
        u = _real_get(_new_url_entry).strip()

        is_changed = True
        is_valid_url = True

        if u:
            if detect_url_type(u) != "channel":
                is_valid_url = False
                url_error_var.set("Invalid URL (must be a youtube channel/playlist)")
            else:
                url_error_var.set("")
        else:
            url_error_var.set("")

        editing = _editing_channel.get("name")

        is_valid_date = True
        if new_mode_var.get() == "date":
            y = _real_get(_date_year_entry).strip()
            if not y or not re.fullmatch(r'\d{4}', y):
                is_valid_date = False

        if editing:
            with config_lock:
                for ch in config.get("channels", []):
                    if ch["name"] == editing:
                        y = _real_get(_date_year_entry).strip()
                        mo = _real_get(_date_month_entry).strip().zfill(2)
                        dy = _real_get(_date_day_entry).strip().zfill(2)
                        cur_date = (y + mo + dy) if (y or mo or dy) else ""
                        parsed = _parse_date_input(cur_date) or ""

                        dur_val = _parse_duration(new_dur_var.get()) * 60

                        mx_val = _parse_duration(new_maxdur_var.get())
                        mx_str = str(mx_val) if mx_val else ""
                        ch_mx_val = str(ch.get("max_duration", 0) // 60) if ch.get("max_duration", 0) else ""

                        _cur_c_res = new_compress_res_var.get()
                        _cur_c_res_val = _cur_c_res.replace("p", "") if _cur_c_res != "Original" else ""
                        if ch["name"] == n and ch["url"] == u and \
                                ch.get("min_duration", 0) == dur_val and \
                                ch_mx_val == mx_str and \
                                ch.get("resolution", "720") == new_res_var.get() and \
                                ch.get("mode", "full") == new_mode_var.get() and \
                                ch.get("date_after", "") == parsed and \
                                ch.get("split_years", False) == new_split_years_var.get() and \
                                ch.get("split_months", False) == new_split_months_var.get() and \
                                ch.get("compress_enabled", False) == new_compress_var.get() and \
                                ch.get("compress_level", "") == new_compress_level_var.get() and \
                                ch.get("compress_output_res", "") == _cur_c_res_val and \
                                ch.get("compress_batch_size", 20) == int(new_compress_batch_var.get() or "20"):
                            is_changed = False
                        break

        if n and u and is_changed and is_valid_url and is_valid_date:
            add_channel_btn.config(state="normal", style="Warn.TButton")
        else:
            add_channel_btn.config(state="disabled", style="TButton")
    except Exception:
        pass


def _validate_download_btn():
    try:
        if 'download_btn' not in globals() or not download_btn.winfo_exists():
            return
        url = _real_get(url_entry).strip()
        kind = detect_url_type(url) if url else "unknown"
        if kind == "video":
            if not download_btn.winfo_ismapped():
                download_btn.pack(side="left", padx=(0, 6), before=sync_btn)
        else:
            if download_btn.winfo_ismapped():
                download_btn.pack_forget()
    except Exception:
        pass


def _trigger_validation(*_):
    if _root_alive:
        _validate_add_btn()
        _validate_download_btn()


url_var.trace_add("write", on_url_change)
for var in (url_var, new_name_var, new_url_var, new_dur_var, new_maxdur_var, new_res_var, new_mode_var, date_year_var,
            date_month_var, date_day_var, new_folder_org_var,
            new_compress_var, new_compress_level_var, new_compress_res_var, new_compress_batch_var):
    var.trace_add("write", _trigger_validation)

for entry_widget in (url_entry, new_name_entry, _new_url_entry, _date_year_entry, _date_month_entry, _date_day_entry):
    entry_widget.bind("<FocusIn>", lambda e: root.after(50, _trigger_validation), add="+")
    entry_widget.bind("<FocusOut>", lambda e: root.after(50, _trigger_validation), add="+")


def do_preview_folder():
    url = url_var.get().strip()
    if not url: folder_preview_var.set("paste a URL first"); return
    folder_preview_var.set("probing…")
    preview_btn.config(state="disabled")

    def _probe():
        proc = None
        try:
            proc = spawn_yt_dlp(["yt-dlp", "--print", "%(uploader)s", "--playlist-items", "1", url])
            if not proc: return
            try:
                out, _ = proc.communicate(timeout=30)
                lines = out.strip().splitlines()
                name = lines[-1].strip() if lines else ""

                def _update_ui(detected_name):
                    if not root.winfo_exists(): return
                    folder_preview_var.set(f"yt-dlp says: \"{detected_name}\"")
                    if not folder_override_var.get().strip(): folder_override_var.set(detected_name)

                if name and proc.returncode == 0:
                    _ui_queue.append(lambda: _update_ui(name))
                else:
                    _ui_queue.append(lambda: folder_preview_var.set("couldn't detect — check URL"))
            except subprocess.TimeoutExpired:
                if proc: proc.kill()
                _ui_queue.append(lambda: folder_preview_var.set("Timeout"))
        except Exception as e:
            _ui_queue.append(lambda err=e: folder_preview_var.set(f"error: {err}"))
        finally:
            cleanup_process(proc)
            _ui_queue.append(lambda: preview_btn.config(state="normal") if preview_btn.winfo_exists() else None)

    threading.Thread(target=_probe, daemon=True).start()


preview_btn.config(command=do_preview_folder)

save_prefs_row = ttk.Frame(chan_opts)
save_prefs_row.grid(row=4, column=0, columnspan=9, sticky="w", padx=4, pady=(0, 6))
save_prefs_label_var = tk.StringVar(value="")
save_prefs_btn = ttk.Button(save_prefs_row, text="💾 Save as channel default", state="disabled")
save_prefs_btn.grid(row=0, column=0, padx=(0, 10))
ttk.Label(save_prefs_row, textvariable=save_prefs_label_var, style="Dim.TLabel").grid(row=0, column=1)


def save_channel_prefs():
    ch_name = chan_var.get()
    with config_lock:
        for ch in config.get("channels", []):
            if ch["name"] == ch_name:
                ch["resolution"] = ch_res_var.get()
                ch["min_duration"] = _parse_duration(ch_dur_var.get()) * 60
                ch["max_duration"] = _parse_duration(ch_maxdur_var.get()) * 60
                ch["mode"] = mode_var.get()
                ch["folder_override"] = sanitize_folder(folder_override_var.get().strip())
                break
    save_config(config)
    refresh_channel_dropdowns()
    log(f"Saved prefs for {ch_name}\n", "green")


save_prefs_btn.config(command=save_channel_prefs)

btn_frame = ttk.Frame(tab_download)
btn_frame.grid(row=3, column=0, sticky="ew", padx=12, pady=(4, 6))


def build_channel_cmd(url, out_dir, min_dur, resolution, folder_override="", break_on_existing=False, max_dur=0,
                      split_years=False, split_months=False, max_downloads=0, playlist_start=0, batch_file=None):
    fmt = build_format_string(resolution)
    folder = sanitize_folder(folder_override.strip()) or "%(uploader)s"

    filters = ["!is_live", "!is_upcoming"]
    if min_dur and min_dur > 0:
        filters.append(f"duration>?{min_dur}")
    if max_dur and max_dur > 0:
        filters.append(f"duration<?{max_dur}")

    _dur_filter = " & ".join(filters)

    # Build output path with optional year/month subdirectories
    # Use yt-dlp's date formatting with fallback defaults for videos missing upload_date
    if split_years and split_months:
        out_template = os.path.join(out_dir, folder,
                                    "%(upload_date>%Y|Unknown Year)s",
                                    "%(upload_date>%m %B|Unknown Month)s",
                                    "%(title)s.%(ext)s")
    elif split_years:
        out_template = os.path.join(out_dir, folder,
                                    "%(upload_date>%Y|Unknown Year)s",
                                    "%(title)s.%(ext)s")
    else:
        out_template = os.path.join(out_dir, folder, "%(title)s.%(ext)s")

    cmd = [
        "yt-dlp", "--newline", "--no-quiet", "--mtime", "--ignore-errors",
        "--trim-filenames", "200", "--format", fmt, "--merge-output-format", "mp4",
        "--ppa", "Merger:-c copy",
        "--sleep-requests", "0.25",
        "--match-filter", _dur_filter,
        "--output", out_template,
        "--download-archive", ARCHIVE_FILE,
        "--print",
        "after_video:DLTRACK:::%(title)s:::%(uploader)s:::%(upload_date)s:::%(filesize,filesize_approx)s:::%(duration)s:::%(id)s",
        "--cookies-from-browser", "firefox"
    ]
    # Sanitize pipe characters from titles before template evaluation to prevent
    # incorrect year/month folder placement on Windows (pipe interferes with
    # the fallback syntax in %(upload_date>%Y|Unknown Year)s)
    if split_years:
        cmd += ["--replace-in-metadata", "title", "\\|", "-"]
    if break_on_existing:
        cmd.append("--break-on-existing")
    if max_downloads and max_downloads > 0:
        cmd += ["--max-downloads", str(max_downloads)]
    if batch_file:
        cmd += ["--batch-file", batch_file]
    else:
        if playlist_start and playlist_start > 1:
            cmd += ["--playlist-start", str(playlist_start)]
        cmd.append(url)
    return cmd


def _get_streams_url(url):
    """Return the /streams tab URL for a channel, or None if not a channel URL."""
    _u = url.rstrip("/")
    if "/@" in _u or "/channel/" in _u or "/c/" in _u or "/user/" in _u:
        # Strip existing tab suffix if present (e.g. /videos, /shorts)
        _u = re.sub(r'/(videos|shorts|streams|playlists|community|podcasts|channels)$', '', _u)
        return _u + "/streams"
    return None


def build_video_cmd(url, out_dir, resolution, add_date=False, custom_name=None, date_file=True):
    fmt = build_format_string(resolution)

    dl_date = datetime.now().strftime("%m.%d.%y")

    if custom_name:
        safe_name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', custom_name).strip().rstrip('.')
        if add_date:
            filename_template = f"{safe_name} ({dl_date}).%(ext)s"
        else:
            filename_template = f"{safe_name}.%(ext)s"
    else:
        filename_template = f"%(title)s ({dl_date}).%(ext)s" if add_date else "%(title)s.%(ext)s"

    cmd = ["yt-dlp", "--newline", "--no-quiet"]
    if date_file:
        cmd.append("--mtime")
    cmd += [
        "--trim-filenames", "200",
        "--format", fmt, "--merge-output-format", "mp4",
        "--ppa", "Merger:-c copy",
        "--output", os.path.join(out_dir, filename_template),
        "--print",
        "after_video:DLTRACK:::%(title)s:::%(uploader)s:::%(upload_date)s:::%(filesize,filesize_approx)s:::%(duration)s:::%(id)s",
        "--cookies-from-browser", "firefox",
        url
    ]
    return cmd


def _log_scan_status(checked, matched, date_disp, title):
    def _write():
        try:
            if 'log_box' in globals() and log_box.winfo_exists():
                try:
                    at_bottom = log_box.yview()[1] >= 0.99
                except Exception:
                    at_bottom = True

                log_box.config(state="normal")
                ranges = log_box.tag_ranges("scanline")
                if ranges:
                    log_box.delete(ranges[0], ranges[1])
                msg = f"  [{checked:,} scanned · {matched:,} matched]  {date_disp}  {title}\n"
                log_box.insert(tk.END, msg, "scanline")

                if at_bottom:
                    log_box.see(tk.END)
                log_box.config(state="disabled")
        except Exception:
            pass

    try:
        if _root_alive:
            _ui_queue.append(_write)
    except Exception:
        pass


def internal_run_subscribe_before_date(url, date_str):
    proc = None
    try:
        log(f"  Scanning channel for videos before {date_str[:4]}-{date_str[4:6]}-{date_str[6:]} (this may take a few minutes)...\n",
            "dim")

        probe_cmd = [
            "yt-dlp", "--flat-playlist", "--no-warnings",
            "--extractor-args", "youtubetab:approximate_date",
            "--print", "%(id)s|||%(upload_date)s|||%(title)s",
            "--cookies-from-browser", "firefox",
            url
        ]
        proc = spawn_yt_dlp(probe_cmd)
        if not proc:
            return False

        ids = []
        checked = 0
        first_result = threading.Event()

        def _heartbeat():
            start = time.time()
            while not first_result.is_set() and not cancel_event.is_set():
                elapsed = int(time.time() - start)

                def _write(s=elapsed):
                    try:
                        if 'log_box' in globals() and log_box.winfo_exists():
                            try:
                                at_bottom = log_box.yview()[1] >= 0.99
                            except Exception:
                                at_bottom = True

                            log_box.config(state="normal")
                            ranges = log_box.tag_ranges("scanline")
                            if ranges:
                                log_box.delete(ranges[0], ranges[1])
                            log_box.insert(tk.END, f"  Loading playlist... ({s}s elapsed)\n", "scanline")

                            if at_bottom:
                                log_box.see(tk.END)
                            log_box.config(state="disabled")
                    except Exception:
                        pass

                try:
                    if _root_alive:
                        _ui_queue.append(_write)
                except Exception:
                    pass
                time.sleep(1)

        threading.Thread(target=_heartbeat, daemon=True).start()

        for line in proc.stdout:
            if cancel_event.is_set():
                break

            line_lower = line.lower()
            if "error:" in line_lower and "cookie" in line_lower and (
                    "extract" in line_lower or "sign in" in line_lower):
                log("\n" + "█" * 65 + "\n", "red")
                log("█  PLEASE INSTALL FIREFOX, SIGN IN TO YOUTUBE, AND TRY AGAIN.\n", "red")
                log("█" * 65 + "\n\n", "red")
                cancel_event.set()
                break

            line = line.strip()
            parts = line.split("|||", 2)
            if len(parts) == 3 and re.fullmatch(r'[\w-]{11}', parts[0]):
                vid_id, upload_date, title = parts
                first_result.set()
                checked += 1
                date_disp = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}" if len(
                    upload_date) == 8 else upload_date
                title_trunc = title[:55] + "…" if len(title) > 55 else title

                if upload_date and upload_date <= date_str:
                    ids.append(vid_id)
                _log_scan_status(checked, len(ids), date_disp, title_trunc)
        first_result.set()
        clear_transient_lines()
        proc.wait()

        if cancel_event.is_set():
            return False
        if proc.returncode != 0:
            log(f"ERROR: yt-dlp exited {proc.returncode} while fetching pre-date IDs.\n", "red")
            return False

        log(f"  Found {len(ids):,} videos before that date — archiving them...\n", "dim")
        with io_lock:
            existing = set()
            if os.path.exists(ARCHIVE_FILE):
                with open(ARCHIVE_FILE, encoding="utf-8") as f_:
                    for l in f_:
                        p = l.strip().split()
                        if p: existing.add(p[-1])
            new_ids = [i for i in ids if i not in existing]
            if new_ids:
                with open(ARCHIVE_FILE, "a", encoding="utf-8") as f_:
                    for vid_id in new_ids:
                        f_.write(f"youtube {vid_id}\n")

        # Store the date-filtered IDs in channel config so they can be
        # removed from the archive if the date filter is later changed
        with config_lock:
            for cfg_ch in config.get("channels", []):
                if cfg_ch["url"] == url:
                    cfg_ch["date_archived_ids"] = ids
                    break
        save_config(config)

        log(f"  ✓ Archived {len(new_ids):,} IDs ({len(ids) - len(new_ids):,} already present).\n", "green")
        return True

    except Exception as e:
        log(f"Error in date-subscribe: {e}\n", "red")
        return False
    finally:
        cleanup_process(proc)


BATCH_LIMIT = 100000
BATCH_COOLDOWN_HOURS = 72


def _check_batch_cooldown(ch):
    """Check if a channel is in batch cooldown.
    Returns (can_proceed, cooldown_str). can_proceed=False means still in cooldown."""
    batch_after = ch.get("init_batch_after")
    if not batch_after:
        return True, ""
    try:
        cooldown_dt = datetime.fromisoformat(batch_after)
        if datetime.now() >= cooldown_dt:
            return True, ""
        time_str = cooldown_dt.strftime("%I:%M%p").lstrip("0").lower()
        date_str = cooldown_dt.strftime("%b %d")
        return False, f"{time_str}, {date_str}"
    except (ValueError, TypeError):
        return True, ""


def _should_batch_limit(ch, ch_total):
    """Return True if batch limiting should apply: full mode, not init_complete, >1000 videos.
    If video count is unavailable (0), assume large for uninitialized channels."""
    if ch.get("mode", "full") != "full":
        return False
    if ch.get("init_complete", False):
        return False
    if ch_total > 0:
        return ch_total > BATCH_LIMIT
    # Count unavailable — batch limit if channel isn't initialized yet (safe default)
    return not ch.get("initialized", False)


def _set_batch_cooldown(url):
    """Set a 24-hour cooldown on the channel. Returns the cooldown datetime."""
    from datetime import timedelta
    cooldown_dt = datetime.now() + timedelta(hours=BATCH_COOLDOWN_HOURS)
    with config_lock:
        for cfg_ch in config.get("channels", []):
            if cfg_ch["url"] == url:
                cfg_ch["init_batch_after"] = cooldown_dt.isoformat()
    save_config(config)
    return cooldown_dt


BATCH_PAGE_SIZE = 50  # approximate videos per page; used to back up ~1 page on resume


def _get_batch_playlist_start(ch):
    """Get the playlist start index for a batch-limited channel, backing up ~1 page for safety."""
    resume = ch.get("batch_resume_index", 0)
    if resume <= 0:
        return 0  # no resume — start from beginning
    return max(1, resume - BATCH_PAGE_SIZE + 1)


def _save_batch_resume(url, playlist_start, run_counts, cache_resume_index=None):
    """Save the resume index after a batch run.
    If cache_resume_index is provided (cached mode), use it directly.
    Otherwise calculate from playlist_start + processed (legacy mode)."""
    if cache_resume_index is not None:
        new_resume = cache_resume_index
    else:
        total_processed = run_counts["dl"] + run_counts["skip"] + run_counts["dur"] + run_counts["err"]
        start = max(1, playlist_start) if playlist_start else 1
        new_resume = start + total_processed
    with config_lock:
        for cfg_ch in config.get("channels", []):
            if cfg_ch["url"] == url:
                cfg_ch["batch_resume_index"] = new_resume
    save_config(config)
    return new_resume


def _clear_batch_state(url, mark_complete=False):
    """Clear batch cooldown and resume index. If mark_complete=True, mark channel init as done."""
    with config_lock:
        for cfg_ch in config.get("channels", []):
            if cfg_ch["url"] == url:
                cfg_ch.pop("init_batch_after", None)
                cfg_ch.pop("batch_resume_index", None)
                if mark_complete:
                    cfg_ch["init_complete"] = True
    save_config(config)
    _delete_batch_cache(url)


def _prefetch_total(url):
    proc = None
    try:
        # Target /videos tab specifically to avoid getting tab count (3) instead of video count
        _url = url.rstrip("/")
        if ("/@" in _url or "/channel/" in _url or "/c/" in _url) and not _url.endswith("/videos"):
            _url = _url + "/videos"

        # Use DEVNULL stderr so stdout only has the playlist_count value
        cmd = [
            "yt-dlp", "--flat-playlist", "--no-warnings", "--playlist-end", "1",
            "--print", "%(playlist_count)s", "--cookies-from-browser", "firefox", _url
        ]
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            startupinfo=startupinfo
        )
        with proc_lock:
            active_processes.append(proc)

        count = 0
        for line in proc.stdout:
            if cancel_event.is_set():
                break
            line = line.strip()
            if not line or line == "NA":
                continue
            if line.isdigit():
                count = int(line)
                break
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except Exception:
                pass

        if count > 0:
            return count

        # Fallback: try extracting from JSON metadata if --print didn't work
        if not cancel_event.is_set():
            with proc_lock:
                try:
                    active_processes.remove(proc)
                except ValueError:
                    pass
            proc = subprocess.Popen(
                ["yt-dlp", "--flat-playlist", "--no-warnings", "--playlist-end", "1",
                 "--dump-single-json", "--no-download",
                 "--cookies-from-browser", "firefox", _url],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                encoding="utf-8",
                errors="replace",
                startupinfo=startupinfo
            )
            with proc_lock:
                active_processes.append(proc)
            import json as _json
            raw = proc.stdout.read()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
            try:
                data = _json.loads(raw)
                count = data.get("playlist_count") or data.get("n_entries") or 0
                if isinstance(count, int) and count > 0:
                    return count
            except Exception:
                pass

        return 0
    except Exception:
        return 0
    finally:
        if proc:
            with proc_lock:
                try:
                    active_processes.remove(proc)
                except ValueError:
                    pass


# ─── Batch ID cache helpers ──────────────────────────────────────────
def _get_batch_cache_path(url):
    """Return the path for a channel's batch video ID cache file."""
    url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()[:12]
    return os.path.join(APP_DATA_DIR, f"batch_cache_{url_hash}.txt")


def _delete_batch_cache(url):
    """Delete the batch ID cache file for a channel."""
    try:
        p = _get_batch_cache_path(url)
        if os.path.exists(p):
            os.remove(p)
    except Exception:
        pass


def _enumerate_all_video_ids(url):
    """Run yt-dlp --flat-playlist --print id to enumerate ALL video IDs for a channel.
    Uses spawn_yt_dlp (stderr merged into stdout, which is proven to work).
    Uses readline() instead of for-loop to avoid TextIOWrapper read buffer stalls.
    11-char ID lines = video IDs, everything else = yt-dlp progress messages."""
    proc = None
    ids = []
    try:
        log("  Enumerating all video IDs (first run only, this may take a while)...\n", "green")
        # Target /videos tab to avoid multi-tab confusion (channel URL returns 3 tabs)
        _enum_url = url.rstrip("/")
        if ("/@" in _enum_url or "/channel/" in _enum_url or "/c/" in _enum_url) and not _enum_url.endswith("/videos"):
            _enum_url = _enum_url + "/videos"

        proc = spawn_yt_dlp([
            "yt-dlp", "--flat-playlist", "--lazy-playlist", "--print", "id",
            "--cookies-from-browser", "firefox",
            _enum_url
        ])
        if not proc:
            return []

        is_simple = _is_simple_mode
        _enum_count = 0
        _got_any_output = False
        _start_time = time.time()
        _last_verbose_log = 0  # ID count at last verbose progress log

        # Use readline() loop — NOT 'for line in proc.stdout' — to avoid
        # TextIOWrapper's internal read buffer stalling on sparse output.
        while True:
            if cancel_event.is_set():
                break
            line = proc.stdout.readline()
            if not line:
                break  # EOF — process closed stdout
            _got_any_output = True
            line = line.strip()
            if not line:
                continue
            # 11-char alphanumeric+dash = video ID
            if re.fullmatch(r'[\w-]{11}', line):
                ids.append(line)
                _enum_count += 1
                # Update simple mode animation with actual count
                _simple_anim_state["enum_count"] = _enum_count
                _simple_anim_state["enum_page"] = max(1, _enum_count // 30)
                # Verbose mode: log progress every 500 IDs
                if not is_simple and _enum_count - _last_verbose_log >= 500:
                    _elapsed = int(time.time() - _start_time)
                    log(f"  ...{_enum_count:,} IDs enumerated ({_elapsed}s elapsed)\n", "dim")
                    _last_verbose_log = _enum_count
                continue
            # Everything else is a yt-dlp status line (stderr merged in) — log it
            if not is_simple:
                log(f"  {line}\n", "dim")

        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except Exception:
                pass
        if ids:
            _elapsed = int(time.time() - _start_time)
            log(f"  Enumerated {len(ids):,} video IDs in {_elapsed}s.\n", "green")
        else:
            if _got_any_output:
                log("  Warning: yt-dlp produced output but no video IDs were found.\n", "red")
            else:
                log("  Warning: yt-dlp produced no output at all. Check cookies/network.\n", "red")
        return ids
    except Exception as e:
        log(f"  Error enumerating video IDs: {e}\n", "red")
        return []
    finally:
        _simple_anim_state["enum_page"] = 0
        _simple_anim_state["enum_count"] = 0
        cleanup_process(proc)


def _load_or_create_batch_cache(url):
    """Load cached video IDs or enumerate and create the cache.
    Returns (ids_list, was_created_now). Returns ([], False) on failure."""
    cache_path = _get_batch_cache_path(url)

    # Try loading existing cache
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                ids = [line.strip() for line in f if line.strip()]
            if ids:
                log(f"  Loaded {len(ids):,} cached video IDs.\n", "dim")
                return ids, False
        except Exception as e:
            log(f"  Error reading cache, will re-enumerate: {e}\n", "red")

    # Cache doesn't exist or was corrupt — enumerate from scratch
    ids = _enumerate_all_video_ids(url)
    if cancel_event.is_set():
        return [], False
    if not ids:
        log("  Could not enumerate video IDs. Falling back to standard batch mode.\n", "red")
        return [], False

    # Save cache
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write("\n".join(ids) + "\n")
        log(f"  Cached {len(ids):,} video IDs for future batches.\n", "green")
    except Exception as e:
        log(f"  Warning: Could not save ID cache: {e}\n", "red")

    return ids, True


def _check_new_videos(url, cached_ids, check_count=100):
    """Quick-check the first check_count video IDs from the channel.
    Returns list of new IDs not in cached_ids that should be prepended.
    Uses spawn_yt_dlp (stderr merged into stdout, proven to work)."""
    proc = None
    new_ids = []
    try:
        _check_url = url.rstrip("/")
        if ("/@" in _check_url or "/channel/" in _check_url or "/c/" in _check_url) and not _check_url.endswith("/videos"):
            _check_url = _check_url + "/videos"

        proc = spawn_yt_dlp([
            "yt-dlp", "--flat-playlist", "--lazy-playlist",
            "--playlist-end", str(check_count),
            "--print", "id",
            "--cookies-from-browser", "firefox",
            _check_url
        ])
        if not proc:
            return []

        cached_set = set(cached_ids)  # Full set for accurate membership check

        # Use readline() to avoid TextIOWrapper read buffer stalls
        while True:
            if cancel_event.is_set():
                break
            line = proc.stdout.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            # Only process 11-char video IDs, skip yt-dlp progress lines
            if re.fullmatch(r'[\w-]{11}', line):
                if line in cached_set:
                    break  # Hit a known ID — everything after is already cached
                new_ids.append(line)
        # Kill process before waiting (may still be running after early break)
        if proc.poll() is None:
            proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        return new_ids
    except Exception:
        return []
    finally:
        cleanup_process(proc)


def _load_archived_ids():
    """Load already-downloaded video IDs from the download archive file.
    Returns a set of video ID strings for fast lookup."""
    archived = set()
    try:
        if os.path.exists(ARCHIVE_FILE):
            with open(ARCHIVE_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        archived.add(parts[-1])
    except Exception:
        pass
    return archived


def _build_batch_file(video_ids):
    """Write video IDs as full YouTube URLs to a temp file for --batch-file.
    Returns file path, or None on error."""
    batch_path = os.path.join(APP_DATA_DIR, "batch_urls_temp.txt")
    try:
        with open(batch_path, "w", encoding="utf-8") as f:
            for vid_id in video_ids:
                f.write(f"https://www.youtube.com/watch?v={vid_id}\n")
        return batch_path
    except Exception as e:
        log(f"  Error creating batch file: {e}\n", "red")
        return None


def _cleanup_batch_file():
    """Remove the temporary batch URL file if it exists."""
    try:
        p = os.path.join(APP_DATA_DIR, "batch_urls_temp.txt")
        if os.path.exists(p):
            os.remove(p)
    except Exception:
        pass


def _prefetch_livestreams(url):
    proc = None
    try:
        proc = spawn_yt_dlp([
            "yt-dlp", "--flat-playlist", "--no-warnings",
            "--playlist-end", "30",
            "--match-filter", "is_live | is_upcoming",
            "--print", "%(id)s\t%(webpage_url)s",
            "--cookies-from-browser", "firefox",
            url
        ])
        if not proc:
            return []
        results = []
        for line in proc.stdout:
            if cancel_event.is_set():
                break
            parts = line.strip().split("\t")
            if len(parts) == 2 and parts[0] and parts[1]:
                results.append((parts[0], parts[1]))
        proc.wait()
        return results
    except Exception:
        return []
    finally:
        cleanup_process(proc)


def internal_run_subscribe_blocking(url):
    proc = None
    try:
        probe_cmd = ["yt-dlp", "--flat-playlist", "--no-warnings", "--print", "%(id)s", "--cookies-from-browser",
                     "firefox", url]
        proc = spawn_yt_dlp(probe_cmd)
        if not proc:
            return False

        ids = []
        for line in proc.stdout:
            if cancel_event.is_set():
                break

            line_lower = line.lower()
            if "error:" in line_lower and "cookie" in line_lower and (
                    "extract" in line_lower or "sign in" in line_lower):
                log("\n" + "█" * 65 + "\n", "red")
                log("█  PLEASE INSTALL FIREFOX, SIGN IN TO YOUTUBE, AND TRY AGAIN.\n", "red")
                log("█" * 65 + "\n\n", "red")
                cancel_event.set()
                break

            line = line.strip()
            if re.fullmatch(r'[\w-]{11}', line):
                ids.append(line)
        proc.wait()

        if cancel_event.is_set():
            return False

        if proc.returncode != 0:
            log(f"ERROR: yt-dlp exited with code {proc.returncode} while fetching video IDs.\n", "red")
            return False

        if not ids:
            log("ERROR: No video IDs retrieved from channel. Subscribe aborted.\n", "red")
            return False

        with io_lock:
            existing = set()
            if os.path.exists(ARCHIVE_FILE):
                with open(ARCHIVE_FILE, encoding="utf-8") as f:
                    for l in f:
                        p = l.strip().split()
                        if p: existing.add(p[-1])

            new_ids = [i for i in ids if i not in existing]
            if new_ids:
                with open(ARCHIVE_FILE, "a", encoding="utf-8") as f:
                    for vid_id in new_ids: f.write(f"youtube {vid_id}\n")
                log(f"✓ Wrote {len(new_ids)} new IDs to archive.\n", "green")
            else:
                log(f"✓ All {len(ids)} IDs already in archive.\n", "green")

        return True

    except Exception as e:
        log(f"Error subscribing: {e}\n", "red")
        return False
    finally:
        cleanup_process(proc)


def run_cmd(cmd, is_single_video=False):
    cancel_event.clear()
    download_btn.pack_forget()
    sync_btn.config(state="disabled")
    sync_single_btn.config(state="disabled")
    _update_queue_btn()

    def _run():
        internal_run_cmd_blocking(cmd)
        if cancel_event.is_set():
            # Extract output directory from the command's --output arg
            try:
                out_idx = cmd.index("--output")
                out_path = cmd[out_idx + 1]
                _cleanup_partial_files(os.path.dirname(out_path))
            except (ValueError, IndexError):
                pass
        if _root_alive:
            def _done():
                _validate_download_btn()
                _sync_task_finished()
                if not _sync_running:
                    sync_btn.config(state="normal")
                on_chan_list_select(None)

            _ui_queue.append(_done)
            if is_single_video:
                def _clear_url():
                    url_var.set("")
                    url_entry.event_generate("<FocusOut>")
                    vid_custom_name_var.set("")

                _ui_queue.append(_clear_url)

    threading.Thread(target=_run, daemon=True).start()


def _parse_ytdlp_size(s):
    if not s: return ""
    m = re.match(r'([\d.]+)\s*(GiB|MiB|KiB|GB|MB|KB|B)', s.strip(), re.IGNORECASE)
    if not m: return ""
    val = float(m.group(1))
    unit = m.group(2).upper()
    mult = {'GIB': 1 << 30, 'MIB': 1 << 20, 'KIB': 1 << 10,
            'GB': 10 ** 9, 'MB': 10 ** 6, 'KB': 10 ** 3, 'B': 1}
    return str(int(val * mult.get(unit, 1)))


def internal_run_cmd_blocking(cmd, channel_total=0, live_ids=None, on_batch_ready=None, compress_batch_size=20):
    if live_ids is None:
        live_ids = []
    proc = None
    videos_processed = set()
    dl_count = 0
    skip_count = 0
    err_count = 0
    dur_count = 0
    _skipped_dur_titles = []  # titles of videos skipped by duration/filter
    current_vid_id = None
    current_merge_dest = ""
    current_dl_size_bytes = ""
    current_dl_dest = ""
    _prog_last_ts = 0.0
    _prog_last_pct = -1.0
    _speed_samples = []
    _tracked_paths = []  # file paths captured at DLTRACK time for per-batch compression
    try:
        proc = spawn_yt_dlp(cmd)
        if not proc: return 0

        # Threaded stdout reader — prevents blocking when yt-dlp hangs,
        # so pause/cancel checks still run every 2 seconds.
        import queue as _queue_mod
        _line_q = _queue_mod.Queue()

        def _stdout_reader():
            try:
                for _rl in proc.stdout:
                    _line_q.put(_rl)
                    # When paused, stop reading to create back-pressure on yt-dlp
                    while pause_event.is_set() and not cancel_event.is_set():
                        time.sleep(0.5)
            except Exception:
                pass
            _line_q.put(None)  # sentinel: EOF

        threading.Thread(target=_stdout_reader, daemon=True).start()

        while True:
            if cancel_event.is_set():
                break

            # Pause check runs even when yt-dlp produces no output
            if pause_event.is_set() and not cancel_event.is_set():
                clear_transient_lines()
                log(f"  ⏸ Sync paused at {_fmt_time()} — click Resume.\n", "pausestatus")
                while pause_event.is_set() and not cancel_event.is_set():
                    time.sleep(0.25)
                if not cancel_event.is_set():
                    clear_pause_status()
                    log(f"  ▶ Sync resumed at {_fmt_time()}...\n", "pauselog")
            if cancel_event.is_set():
                break

            try:
                line = _line_q.get(timeout=2.0)
            except _queue_mod.Empty:
                continue  # no output — loop back to check pause/cancel
            if line is None:
                break  # EOF — yt-dlp exited

            is_simple_mode = _is_simple_mode

            # Strip Unicode replacement characters from yt-dlp output
            line = line.replace('\ufffd', '')

            line_lower = line.lower()
            if "error:" in line_lower and "cookie" in line_lower and (
                    "extract" in line_lower or "sign in" in line_lower):
                log("\n" + "█" * 65 + "\n", "red")
                log("█  PLEASE INSTALL FIREFOX, SIGN IN TO YOUTUBE, AND TRY AGAIN.\n", "red")
                log("█" * 65 + "\n\n", "red")
                cancel_event.set()
                break

            # Suppress noisy yt-dlp pip/update nag messages
            if "installed yt-dlp with pip" in line_lower or "use that to update" in line_lower:
                continue
            # Suppress max-downloads reached message (handled by batch logic)
            if "maximum number of downloads" in line_lower:
                continue

            # Track [youtube:tab] page enumeration for simple mode status
            if "[youtube:tab]" in line:
                _page_m = re.search(r'page\s+(\d+)', line)
                if _page_m:
                    _simple_anim_state["page_num"] = int(_page_m.group(1))
                if is_simple_mode:
                    continue
            elif _simple_anim_state.get("page_num", 0) > 0:
                # Enumeration phase ended, clear page indicator
                _simple_anim_state["page_num"] = 0

            if "[download] Destination:" not in line:
                m = re.search(r'\[(?:youtube|download|info)\]\s+([a-zA-Z0-9_-]{11}):', line)
                if m:
                    current_vid_id = m.group(1)
            if "Extracting URL:" in line:
                m = re.search(r'v=([a-zA-Z0-9_-]{11})', line)
                if m:
                    current_vid_id = m.group(1)

            if "DLTRACK:::" in line:
                try:
                    parts = line.strip().split(":::")
                    if len(parts) >= 4:
                        # Strip Unicode replacement characters from title
                        parts[1] = parts[1].replace('\ufffd', '')
                        size_bytes = parts[4].strip() if len(parts) >= 5 else ""
                        duration_s = parts[5].strip() if len(parts) >= 6 else ""

                        if current_merge_dest and os.path.exists(current_merge_dest):
                            try:
                                size_bytes = str(os.path.getsize(current_merge_dest))
                            except OSError:
                                pass
                        if not size_bytes or size_bytes in ("NA", "None", "none"):
                            size_bytes = current_dl_size_bytes

                        channel_name = parts[2].strip() if len(parts) > 2 and parts[2].strip() else "NA"
                        if channel_name in ("NA", "None", "none", ""):
                            if current_dl_dest:
                                channel_name = os.path.basename(os.path.dirname(current_dl_dest))
                            else:
                                channel_name = "Unknown"

                        # Determine final filepath for the output file
                        # Priority: merge dest (from [Merger] log) > constructed path > directory scan
                        filepath = ""
                        if current_merge_dest and os.path.exists(current_merge_dest):
                            filepath = current_merge_dest
                        elif current_dl_dest:
                            # yt-dlp merges video.f123.mp4 → video.mp4; construct the expected path
                            base_path = re.sub(r'\.f\d+\.\w+$', '', current_dl_dest)
                            mp4_path = os.path.splitext(base_path)[0] + '.mp4'
                            mkv_path = os.path.splitext(base_path)[0] + '.mkv'
                            if os.path.exists(mp4_path):
                                filepath = mp4_path
                            elif os.path.exists(mkv_path):
                                filepath = mkv_path
                            elif os.path.exists(current_dl_dest):
                                filepath = current_dl_dest

                        # Fallback: scan directory for most recent video file
                        # Use creation time (st_ctime) on Windows because --mtime sets
                        # mtime to the upload date which can be years ago, causing
                        # the recency check to always fail
                        if not filepath or not os.path.exists(filepath):
                            scan_dir = os.path.dirname(current_merge_dest or current_dl_dest or "")
                            if scan_dir and os.path.isdir(scan_dir):
                                try:
                                    candidates = [
                                        e for e in os.scandir(scan_dir)
                                        if (e.name.endswith('.mp4') or e.name.endswith('.mkv'))
                                           and not e.name.endswith('.part')
                                    ]
                                    if candidates:
                                        _tkey = (lambda e: e.stat().st_ctime) if os.name == 'nt' else (lambda e: e.stat().st_mtime)
                                        best = max(candidates, key=_tkey)
                                        if (time.time() - _tkey(best)) < 300:
                                            filepath = best.path
                                except OSError:
                                    pass

                        # Last resort: match by normalized title in the output directory tree
                        # Handles files with special Unicode characters where path detection failed
                        if not filepath or not os.path.exists(filepath):
                            _title_raw = parts[1].strip()
                            _norm_title = re.sub(r'[^\w]', '', _title_raw.lower())
                            if _norm_title and len(_norm_title) >= 5:
                                _out_base = ""
                                try:
                                    _oi = cmd.index("--output")
                                    # Walk up from template to find the base channel folder
                                    _out_tmpl = cmd[_oi + 1]
                                    _out_base = os.path.dirname(_out_tmpl)
                                    # Strip any remaining yt-dlp template tokens from the path
                                    while '%(' in _out_base:
                                        _out_base = os.path.dirname(_out_base)
                                except (ValueError, IndexError):
                                    pass
                                if _out_base and os.path.isdir(_out_base):
                                    try:
                                        for _dp, _dns, _fns in os.walk(_out_base):
                                            for _fn in _fns:
                                                _fb, _fe = os.path.splitext(_fn)
                                                if _fe.lower() not in ('.mp4', '.mkv', '.webm'):
                                                    continue
                                                _norm_fn = re.sub(r'[^\w]', '', _fb.lower())
                                                if _norm_fn == _norm_title or (
                                                    len(_norm_fn) >= 15 and (
                                                        _norm_title.startswith(_norm_fn) or
                                                        _norm_fn.startswith(_norm_title[:len(_norm_fn)])
                                                    )
                                                ):
                                                    _cand = os.path.join(_dp, _fn)
                                                    # Use creation time on Windows (--mtime sets mtime to upload date)
                                                    _cand_time = os.path.getctime(_cand) if os.name == 'nt' else os.path.getmtime(_cand)
                                                    if (time.time() - _cand_time) < 600:
                                                        filepath = _cand
                                                        break
                                            if filepath:
                                                break
                                    except OSError:
                                        pass

                        # Get accurate file size from disk (DLTRACK fires after video is fully merged)
                        if filepath and os.path.exists(filepath):
                            try:
                                _disk_size = os.path.getsize(filepath)
                                if _disk_size > 0:
                                    size_bytes = str(_disk_size)
                            except OSError:
                                pass

                        vid_id_str = parts[6].strip() if len(parts) >= 7 else ""
                        video_url = f"https://www.youtube.com/watch?v={vid_id_str}" if vid_id_str else ""
                        record_download(parts[1], channel_name, parts[3], size_bytes, duration_s, filepath, video_url)

                        # Log ✓ line with standardized column widths for Simple mode
                        _size_str = f"({_fmt_size(size_bytes)})" if size_bytes and size_bytes not in ("NA", "None", "none", "") else ""
                        _date_str_raw = parts[3].strip() if len(parts) >= 4 else ""
                        if len(_date_str_raw) == 8 and _date_str_raw.isdigit():
                            _disp_date = f"{_date_str_raw[4:6]}.{_date_str_raw[6:]}.{_date_str_raw[2:4]}"
                        else:
                            _disp_date = ""
                        if is_simple_mode:
                            _title_max = 52
                            _chan_max = 18
                            _raw_title = parts[1].strip()
                            if len(_raw_title) > _title_max:
                                _disp_title = _raw_title[:_title_max - 3] + "..."
                            else:
                                _disp_title = _raw_title.ljust(_title_max)
                            _disp_chan = channel_name[:_chan_max].ljust(_chan_max)
                            _date_col = f" {_disp_date}  " if _disp_date else "  "
                            _disp_size = _size_str.rjust(10) if _size_str else ""
                            log(f"  ✓ {_disp_title}  {_disp_chan}{_date_col}{_disp_size}\n", "simpledownload")
                        else:
                            _size_str2 = f"  ({_fmt_size(size_bytes)})" if size_bytes and size_bytes not in ("NA", "None", "none", "") else ""
                            log(f"  ✓ {parts[1]}  —  {channel_name}{_size_str2}\n", "simpledownload")

                        # Set file mtime to upload date for accurate month-folder sorting
                        # --mtime uses HTTP Last-Modified which can differ from actual upload date
                        upload_date_str = parts[3].strip()
                        if filepath and os.path.exists(filepath):
                            if len(upload_date_str) == 8 and upload_date_str.isdigit():
                                try:
                                    ud = datetime.strptime(upload_date_str, "%Y%m%d")
                                    ud_ts = ud.replace(hour=12).timestamp()
                                    os.utime(filepath, (ud_ts, ud_ts))
                                except (ValueError, OSError):
                                    pass

                                # Fix folder placement: yt-dlp's template may use approximate_date
                                # which can put files in the wrong year/month folder. Check and move
                                # to the correct folder based on the exact upload_date from DLTRACK.
                                try:
                                    _out_idx = cmd.index("--output")
                                    _out_tmpl = cmd[_out_idx + 1]
                                    # Only fix if split_years is active (template has year folder)
                                    if "upload_date" in _out_tmpl and os.sep in _out_tmpl:
                                        _file_dir = os.path.dirname(filepath)
                                        _file_name = os.path.basename(filepath)
                                        _year = upload_date_str[:4]
                                        _month_num = int(upload_date_str[4:6])
                                        _month_name = MONTH_NAMES.get(_month_num, f"{_month_num:02d} Unknown")

                                        # Determine what the correct folder should be
                                        # Walk up from template to find the channel base folder
                                        _base_dir = os.path.dirname(_out_tmpl)
                                        while '%(' in _base_dir:
                                            _base_dir = os.path.dirname(_base_dir)

                                        if os.path.isdir(_base_dir):
                                            # Check if template uses year+month or just year
                                            _has_month = ("%m" in _out_tmpl or "%B" in _out_tmpl)
                                            if _has_month:
                                                _correct_dir = os.path.join(_base_dir, _year, _month_name)
                                            else:
                                                _correct_dir = os.path.join(_base_dir, _year)

                                            _correct_path = os.path.join(_correct_dir, _file_name)
                                            if os.path.normpath(filepath) != os.path.normpath(_correct_path):
                                                os.makedirs(_correct_dir, exist_ok=True)
                                                if not os.path.exists(_correct_path):
                                                    shutil.move(filepath, _correct_path)
                                                    filepath = _correct_path
                                                    # Update the record with the corrected path
                                                    with config_lock:
                                                        recent = config.get("recent_downloads", [])
                                                        if recent and recent[0].get("title") == parts[1]:
                                                            recent[0]["filepath"] = filepath
                                                    if not is_simple_mode:
                                                        _rel = os.path.relpath(_correct_dir, _base_dir)
                                                        log(f"  → Moved to correct folder: {_rel}\n", "dim")
                                except (ValueError, IndexError, OSError, shutil.Error):
                                    pass
                            else:
                                # upload_date was NA/invalid — file keeps --mtime date (HTTP Last-Modified)
                                log(f"  ⚠ No upload date for '{parts[1][:50]}' — file date may be inaccurate.\n", "dim")

                        current_merge_dest = ""
                        current_dl_size_bytes = ""
                        current_dl_dest = ""
                        _prog_last_ts = 0.0
                        _prog_last_pct = -1.0
                        _speed_samples.clear()

                        # Track filepath for per-batch compression (fires after merge, so path is final)
                        if filepath and os.path.exists(filepath) and on_batch_ready and compress_batch_size > 0:
                            _tracked_paths.append(filepath)
                            if len(_tracked_paths) % compress_batch_size == 0:
                                try:
                                    _batch_start = len(_tracked_paths) - compress_batch_size
                                    on_batch_ready(dl_count, list(_tracked_paths[_batch_start:]))
                                except Exception:
                                    pass

                        # Stop merge "Finishing..." animation if running
                        try:
                            if '_merge_anim' in dir() and _merge_anim.get("active"):
                                _merge_anim["active"] = False
                                _old_merge_job = _merge_anim.get("job")
                                _merge_anim["job"] = None
                                if _old_merge_job:
                                    _ui_queue.append(lambda j=_old_merge_job: root.after_cancel(j) if root.winfo_exists() else None)
                                clear_simple_status()
                        except Exception:
                            pass

                    if not is_simple_mode:
                        pass  # Don't log the raw DLTRACK line even in verbose
                    continue
                except Exception:
                    pass

            if "[download]" in line and "%" in line and "ETA" in line:
                m = re.search(r'\[download\]\s+([\d\.]+)%\s+of\s+(.*?)\s+at\s+(.*?)\s+ETA\s+(.*)', line)
                if m:
                    try:
                        pct_val = float(m.group(1))

                        if pct_val >= 99.9:
                            parsed = _parse_ytdlp_size(m.group(2))
                            if parsed:
                                current_dl_size_bytes = parsed

                        _now = time.monotonic()
                        if _now - _prog_last_ts < 0.15 and pct_val != 100.0:
                            continue
                        _prog_last_ts = _now
                        _prog_last_pct = pct_val

                        raw_speed = m.group(3).strip()
                        _speed_samples.append(raw_speed)
                        if len(_speed_samples) > 6:
                            _speed_samples.pop(0)

                        smooth_speed = sorted(_speed_samples)[len(_speed_samples) // 2]
                        filled = int(28 * (pct_val / 100.0))
                        bar = "█" * filled + "░" * (28 - filled)
                        msg = f"  {bar}  {m.group(1)}%  |  {m.group(2).strip()}  |  {smooth_speed}  |  ETA {m.group(4).strip()}\n"
                        log_dl_progress(msg)
                        continue
                    except ValueError:
                        pass

            mm = re.search(r'\[(?:Merger|ffmpeg|FixupM3u8)\] (?:Merging|Remuxing|Converting)[^"]*"(.+?)"', line)
            if mm:
                current_merge_dest = mm.group(1).strip()
                # Show "Finishing..." in simple mode for large files (>500MB)
                if is_simple_mode:
                    _merge_size = 0
                    try:
                        _merge_size = int(current_dl_size_bytes) if current_dl_size_bytes else 0
                    except (ValueError, TypeError):
                        pass
                    if _merge_size > 500_000_000:
                        _merge_start_ts = time.monotonic()
                        _merge_anim = {"active": True, "dots": 0, "job": None}

                        def _merge_anim_tick():
                            try:
                                if not _merge_anim["active"] or not root.winfo_exists():
                                    return
                                if pause_event.is_set():
                                    return  # just return; finally block handles rescheduling
                                _merge_anim["dots"] = (_merge_anim["dots"] + 1) % 3
                                d = _DOTS[_merge_anim["dots"]]
                                elapsed = time.monotonic() - _merge_start_ts
                                if elapsed > 90:
                                    log_simple_status(f"  Still finalizing file, your disk may be busy{d}\n")
                                else:
                                    log_simple_status(f"  Finishing{d}\n")
                            except Exception:
                                pass
                            finally:
                                if _merge_anim["active"] and root.winfo_exists():
                                    _merge_anim["job"] = root.after(500, _merge_anim_tick)

                        def _start_merge_anim():
                            if root.winfo_exists():
                                _merge_anim["job"] = root.after(500, _merge_anim_tick)
                        _ui_queue.append(_start_merge_anim)

            if "recorded in the archive" in line:
                skip_count += 1
                session_totals["skip"] += 1

                checked = skip_count + dl_count + dur_count
                if checked == 1 or checked % 25 == 0:
                    log_progress_bar(checked, channel_total)
                if is_simple_mode:
                    continue

            elif "does not pass filter" in line or "not supported between instances of 'int' and 'str'" in line:
                dur_count += 1
                session_totals["dur"] += 1

                # Determine filter reason by checking the whole yt-dlp line
                # (works regardless of whether yt-dlp wraps the expression in parens)
                _has_min = "duration>?" in line or "duration >" in line
                _has_max = "duration<?" in line or "duration <" in line
                if _has_min and not _has_max:
                    _filter_reason = "Filtered: too short."
                elif _has_max and not _has_min:
                    _filter_reason = "Filtered: too long."
                elif _has_min and _has_max:
                    _filter_reason = "Filtered: outside duration range."
                else:
                    _filter_reason = "Filtered: outside duration range."

                # Extract title for summary enumeration (both modes)
                _m_title = re.search(r'\[download\]\s+(.+?)\s+does not pass filter', line)
                _skip_title_raw = _m_title.group(1).strip() if _m_title else "Unknown"
                _skipped_dur_titles.append((_skip_title_raw, _filter_reason))

                if is_simple_mode:
                    _skip_title = _skip_title_raw
                    _skip_tmax = 49
                    if len(_skip_title) > _skip_tmax:
                        _skip_title = _skip_title[:_skip_tmax - 3] + "..."
                    short = f"[SKIP] {_skip_title.ljust(_skip_tmax)}  -{_filter_reason}\n"
                    log(short, "filterskip")
                else:
                    log(line, "filterskip")

                if current_vid_id and current_vid_id not in live_ids:
                    with io_lock:
                        with open(ARCHIVE_FILE, "a", encoding="utf-8") as f:
                            f.write(f"youtube {current_vid_id}\n")
                    if not is_simple_mode:
                        log(f"  [Auto-Archived] Added {current_vid_id} to archive so it won't be checked again.\n", "dim")
                continue

            elif any(x in line for x in ["ERROR:", "Error:"]):
                # Members-only videos are not real errors — treat as skips
                if "Join this channel to get access to members-only content" in line:
                    dur_count += 1
                    session_totals["dur"] += 1
                    _vid = current_vid_id or "video"
                    _skip_title = _fetch_video_title(_vid) if _vid != "video" else "video"
                    _skipped_dur_titles.append((_skip_title, "Members-only content."))
                    if is_simple_mode:
                        _skip_tmax = 49
                        if len(_skip_title) > _skip_tmax:
                            _skip_title = _skip_title[:_skip_tmax - 3] + "..."
                        short = f"[SKIP] {_skip_title:<{_skip_tmax}}  -Members-only content.\n"
                        log(short, "filterskip")
                    else:
                        log(line, "filterskip")
                    if current_vid_id and current_vid_id not in live_ids:
                        with io_lock:
                            with open(ARCHIVE_FILE, "a", encoding="utf-8") as f:
                                f.write(f"youtube {current_vid_id}\n")
                        if not is_simple_mode:
                            log(f"  [Auto-Archived] Added {current_vid_id} to archive so it won't be checked again.\n", "dim")
                    continue
                # Private/unavailable videos — auto-archive so they never reappear
                if current_vid_id and ("Video unavailable" in line or "This video is private" in line
                                       or "video is no longer available" in line.lower()):
                    dur_count += 1
                    session_totals["dur"] += 1
                    _vid = current_vid_id
                    _skip_title = _fetch_video_title(_vid) if _vid != "video" else "video"
                    _skipped_dur_titles.append((_skip_title, "Private/unavailable."))
                    if is_simple_mode:
                        _skip_tmax = 49
                        if len(_skip_title) > _skip_tmax:
                            _skip_title = _skip_title[:_skip_tmax - 3] + "..."
                        short = f"[SKIP] {_skip_title:<{_skip_tmax}}  -Private/unavailable.\n"
                        log(short, "filterskip")
                    else:
                        log(line, "filterskip")
                    with io_lock:
                        with open(ARCHIVE_FILE, "a", encoding="utf-8") as f:
                            f.write(f"youtube {current_vid_id}\n")
                    if not is_simple_mode:
                        log(f"  [Auto-Archived] Added {current_vid_id} to archive so it won't be checked again.\n", "dim")
                    continue
                err_count += 1
                session_totals["err"] += 1
                if "Requested format is not available" in line:
                    if not is_simple_mode:
                        log(line, "red")
                        log("  ↳ All format fallbacks exhausted. This may be due to YouTube JS challenge issues.\n", "dim")
                        log("  ↳ Try updating yt-dlp:  yt-dlp -U\n", "dim")
                    # In simple mode, silently count the error (shown in summary)
                else:
                    log(line, "red")
                continue
            elif "WARNING:" in line:
                log(line, "dim")
                if is_simple_mode:
                    continue
            elif "[download] Destination:" in line:
                v_name = line.split("Destination: ")[-1].strip()
                current_dl_dest = v_name

                dedup_key = current_vid_id or re.sub(r'\.f\d+\.\w+$', '', v_name)
                if dedup_key not in videos_processed:
                    videos_processed.add(dedup_key)
                    dl_count += 1
                    session_totals["dl"] += 1

                    if is_simple_mode:
                        _update_simple_dl(dl_count, 0, batch_size=compress_batch_size if on_batch_ready else 0)

            if not is_simple_mode:
                log(line)

        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except Exception:
                pass

        # Fire final-batch callback for any remaining tracked paths that didn't complete a full batch
        if on_batch_ready and compress_batch_size > 0 and _tracked_paths:
            _remaining_count = len(_tracked_paths) % compress_batch_size
            if _remaining_count > 0:
                try:
                    on_batch_ready(dl_count, list(_tracked_paths[-_remaining_count:]))
                except Exception:
                    pass

        if not cancel_event.is_set():
            clear_transient_lines()
            _simple = _is_simple_mode
            if not _simple:
                if dl_count == 0:
                    if err_count == 0:
                        log(f"SUMMARY: Downloaded: 0, no new videos\n", "summary")
                    else:
                        log(f"SUMMARY: Downloaded: 0, no new videos | Errors: {err_count}\n", "summary")
                else:
                    log(f"SUMMARY: Downloaded: {dl_count} | Skipped: {skip_count} | Errors: {err_count}\n", "summary")

    except Exception as e:
        clear_transient_lines()
        log(f"Error: {e}\n", "red")
    finally:
        cleanup_process(proc)

    _last_run_counts.update({"dl": dl_count, "skip": skip_count, "dur": dur_count, "err": err_count,
                             "skipped_titles": list(_skipped_dur_titles)})
    return dl_count


_URL_PLACEHOLDER = "https://www.youtube.com/watch?v=..."


def start_download():
    url = _real_get(url_entry).strip()
    kind = detect_url_type(url)
    if not url: log("ERROR: No URL.\n", "red"); return
    if kind == "channel":
        log("ERROR: Channel URLs should be added via Settings. Paste the URL there to subscribe.\n", "red")
        return
    if kind == "unknown": log("ERROR: Unknown URL type. Paste a YouTube video URL (/watch?v=, /shorts/).\n",
                              "red"); return

    out_dir = videodir_var.get().strip() or BASE_DIR
    if not check_directory_writable(out_dir): log(f"ERROR: Cannot write to '{out_dir}'.\n", "red"); return

    custom_name = None
    if not vid_use_yt_title_var.get():
        custom_name = _real_get(vid_custom_name_entry).strip() or None

    cmd = build_video_cmd(url, out_dir, vid_res_var.get(), add_date=vid_add_date_var.get(),
                          custom_name=custom_name, date_file=vid_date_file_var.get())

    # Clear URL box and video panel immediately so user can queue more
    url_var.set("")
    url_entry.event_generate("<FocusOut>")
    vid_custom_name_var.set("")

    if _sync_running or _reorg_running:
        # Queue the download for after the current operation finishes
        with _video_dl_queue_lock:
            _video_dl_queue.append((cmd, True))
        with _queue_order_lock:
            if not any(t == "video" for t, _ in _queue_order):
                _queue_order.append(("video", "batch"))
        log(f"Video added to download queue.\n", "simpleline_green")
        _update_queue_btn()
        return

    run_cmd(cmd, is_single_video=False)


def start_sync_all():
    global _sync_running, _queue_items_removed
    _queue_items_removed = False
    with config_lock:
        channels = sorted(config.get("channels", []), key=lambda c: c.get("name", "").lower())
    if not channels:
        log("No saved channels found to sync.\n", "red")
        return

    # If a sync is already running, queue all channels for later
    if _sync_running:
        with _sync_queue_lock:
            already_queued = len([q for q in _sync_queue if q["url"] in [c["url"] for c in channels]]) >= len(channels)
            if already_queued:
                log("Full Sub Sync already queued.\n", "simpleline")
                return
            added = 0
            for ch in channels:
                if not any(q["url"] == ch["url"] for q in _sync_queue):
                    _sync_queue.append(copy.deepcopy(ch))
                    with _queue_order_lock:
                        _queue_order.append(("sync", ch["url"]))
                    added += 1
        log(f"\n=== {added} channel{'s' if added != 1 else ''} added to sync queue ===\n", "header")
        _update_queue_btn()
        return

    # If a reorg is running, queue the sync for later
    if _reorg_running:
        with _sync_queue_lock:
            # Check if a full sync is already queued
            already_queued = len([q for q in _sync_queue if q["url"] in [c["url"] for c in channels]]) >= len(channels)
            if already_queued:
                log("Full Sub Sync already queued. Cancel and Re-Sync to restart.\n", "simpleline")
                return
            for ch in channels:
                if not any(q["url"] == ch["url"] for q in _sync_queue):
                    _sync_queue.append(copy.deepcopy(ch))
                    with _queue_order_lock:
                        _queue_order.append(("sync", ch["url"]))
        _what = "transcription" if _transcribe_running else "reorganize"
        log(f"\n=== Sync added to Sync List ({_what} in progress) ===\n", "header")
        sync_btn.config(state="disabled")
        _update_queue_btn()
        return

    _schedule_autorun(0)

    for key in session_totals: session_totals[key] = 0
    cancel_event.clear()

    sync_btn.config(state="disabled", text="⏳ Syncing.")
    _update_queue_btn()

    _dot_cycle = [". ", ".. ", "..."]
    _dot_state = {"i": 0, "job": None}

    def _animate_dots():
        if not root.winfo_exists(): return
        try:
            cur = sync_btn.cget("text")
            if cur.startswith("⏳ Syncing") or cur == "⏸ Paused":
                if pause_event.is_set():
                    sync_btn.config(text="⏸ Paused")
                else:
                    _dot_state["i"] = (_dot_state["i"] + 1) % len(_dot_cycle)
                    sync_btn.config(text="⏳ Syncing" + _dot_cycle[_dot_state["i"]])
                _dot_state["job"] = root.after(500, _animate_dots)
        except Exception:
            pass

    _dot_state["job"] = root.after(500, _animate_dots)

    global _job_generation
    _job_generation += 1
    _my_gen = _job_generation

    _sync_running = True
    _current_job["label"] = "Full Sub Sync"
    _tray_start_spin()
    _update_tray_tooltip("YT Archiver — Syncing...")
    _captured_outdir = outdir_var.get().strip() or BASE_DIR

    def _sync_worker():
        global _sync_running, _current_sync_ch
        try:
            out_dir = _captured_outdir
            if not check_directory_writable(out_dir):
                log(f"ERROR: Cannot write to '{out_dir}'. Sync aborted.\n", "red")
                return

            deferred_streams = []

            ch_dl_map = {}

            # Populate sync queue so channels are visible and removable
            with _sync_queue_lock:
                for ch in channels:
                    if not any(q["url"] == ch["url"] for q in _sync_queue):
                        _sync_queue.append(copy.deepcopy(ch))
                        with _queue_order_lock:
                            _queue_order.append(("sync", ch["url"]))
            _update_queue_btn()

            processed = 0
            _gpu_skip_count = 0  # consecutive GPU batch limit skips
            while True:
                # Check cancel — but if it's a skip, clear and continue to next channel
                if cancel_event.is_set():
                    if _skip_current.is_set():
                        _skip_current.clear()
                        cancel_event.clear()
                    else:
                        break
                with _sync_queue_lock:
                    if not _sync_queue:
                        break
                    # All remaining channels are being skipped due to GPU batch limit
                    if _gpu_skip_count > 0 and _gpu_skip_count >= len(_sync_queue):
                        log(f"\n  ⏭ All remaining channels skipped — GPU batch limit reached. Start GPU Tasks to continue.\n", "dim")
                        break
                    ch = _sync_queue.pop(0)
                    current_total = processed + 1 + len(_sync_queue)
                with _queue_order_lock:
                    try:
                        _queue_order.remove(("sync", ch["url"]))
                    except ValueError:
                        pass
                _update_queue_btn()

                # GPU batch limit: skip channel if too many unprocessed encode batches
                _c_level_bl = ch.get("compress_level", "")
                if ch.get("compress_enabled", False) and _c_level_bl in _QUALITY_OPTIONS:
                    _pending_batches = _count_gpu_encode_batches(ch.get("url", ""))
                    if _pending_batches >= GPU_BATCH_LIMIT:
                        _skip_name = ch.get("name", "?")
                        log(f"\n  ⏭ Skipping {_skip_name} — {_pending_batches} unprocessed GPU batches (limit: {GPU_BATCH_LIMIT})\n", "dim")
                        with _sync_queue_lock:
                            _sync_queue.append(ch)
                        with _queue_order_lock:
                            _queue_order.append(("sync", ch["url"]))
                        _update_queue_btn()
                        _gpu_skip_count += 1
                        continue

                _gpu_skip_count = 0  # reset on successful processing
                processed += 1
                i = processed

                if pause_event.is_set():
                    clear_transient_lines()
                    log(f"  ⏸ Sync paused at {_fmt_time()} before channel {i}/{current_total} — click Resume.\n", "pausestatus")
                    while pause_event.is_set() and not cancel_event.is_set():
                        time.sleep(0.25)
                    if cancel_event.is_set() and not _skip_current.is_set():
                        break
                    if _skip_current.is_set():
                        _skip_current.clear()
                        cancel_event.clear()
                        continue
                    clear_pause_status()
                    log(f"  ▶ Sync resumed at {_fmt_time()}...\n", "pauselog")

                ch_name = ch["name"]
                ch_dl_map[ch_name] = 0
                _current_job["label"] = f"Initializing {ch_name}" if not ch.get("initialized", False) else f"Sync {ch_name}"
                _current_job["url"] = ch.get("url")
                _current_sync_ch = copy.deepcopy(ch)

                log(f"\n--- [{i}/{current_total}] SYNCING: {ch_name} ---\n", "header")
                log(f"  Checking channel...\n", "dim")
                _update_tray_tooltip(f"YT Archiver — [{i}/{current_total}] {ch_name}")

                max_dur_ch = ch.get("max_duration", 0)
                live_ids = []
                if not cancel_event.is_set():
                    live_videos = _prefetch_livestreams(ch["url"])
                    if live_videos:
                        live_ids = [vid[0] for vid in live_videos]
                        if max_dur_ch:
                            log(f"  ⏭ {len(live_videos)} livestream(s) skipped (max-dur set).\n", "dim")
                        else:
                            for _lid, _lurl in live_videos:
                                deferred_streams.append((ch, _lurl, _lid))
                            _lnames = ", ".join(vid[0] for vid in live_videos)
                            log(f"\n", "livestream")
                            log(f"  ⚠  LIVESTREAM DETECTED — WILL DOWNLOAD AFTER SYNC  ⚠\n", "livestream")
                            log(f"  {len(live_videos)} stream(s) queued: {_lnames}\n", "livestream")
                            log(f"\n", "livestream")

                mode = ch.get("mode", "full")
                url = ch["url"]
                res = ch.get("resolution", "720")
                min_dur = ch.get("min_duration", 0)
                folder_ovr = ch.get("folder_override", "")
                is_initialized = ch.get("initialized", False)
                sync_complete = ch.get("sync_complete", True)

                # --- Batch safety: check cooldown for large full-mode channels ---
                batch_limited = False
                if mode == "full" and not ch.get("init_complete", False):
                    can_proceed, cooldown_str = _check_batch_cooldown(ch)
                    if not can_proceed:
                        log(f"  Skipping — next batch after {cooldown_str}\n", "dim")
                        # Show in simple mode summary so user sees the channel wasn't forgotten
                        if _is_simple_mode:
                            _stop_simple_anim()
                            _pad = 34 + len(str(current_total)) - len(str(i))
                            _cn = ch_name if len(ch_name) <= _pad else ch_name[:_pad - 3] + "..."
                            log(f"[{i}/{current_total}] {_cn:<{_pad}} —  Downloaded: None, hit daily limit. Resets at {cooldown_str}\n", "simpleline")
                        continue

                if mode == "sub" and not is_initialized:
                    log(f"First sync: Archiving existing backlog...\n", "green")
                    success = internal_run_subscribe_blocking(url)

                    if cancel_event.is_set():
                        if _skip_current.is_set():
                            _skip_current.clear()
                            cancel_event.clear()
                            continue
                        break

                    if success:
                        with config_lock:
                            for cfg_ch in config.get("channels", []):
                                if cfg_ch["url"] == url: cfg_ch["initialized"] = True
                        save_config(config)
                        log(f"Initialization complete.\n", "green")
                    else:
                        log(f"ERROR: Backlog archive failed for {ch_name}. Skipping download for this channel.\n",
                            "red")
                        continue
                elif mode == "sub":
                    log(f"Channel already initialized. Checking for new uploads only...\n", "green")

                elif mode == "date" and not is_initialized:
                    date_after = ch.get("date_after", "")
                    if not date_after:
                        log(f"ERROR: date_after missing for {ch_name}. Skipping.\n", "red")
                        continue
                    log(f"First sync: Archiving videos before {date_after[:4]}-{date_after[4:6]}-{date_after[6:]}...\n",
                        "green")
                    success = internal_run_subscribe_before_date(url, date_after)

                    if cancel_event.is_set():
                        if _skip_current.is_set():
                            _skip_current.clear()
                            cancel_event.clear()
                            continue
                        break

                    if success:
                        with config_lock:
                            for cfg_ch in config.get("channels", []):
                                if cfg_ch["url"] == url: cfg_ch["initialized"] = True
                        save_config(config)
                        log(f"Initialization complete. Downloading from {date_after[:4]}-{date_after[4:6]}-{date_after[6:]} onward...\n",
                            "green")
                    else:
                        log(f"ERROR: Date-archive failed for {ch_name}. Skipping.\n", "red")
                        continue
                elif mode == "date":
                    date_after = ch.get("date_after", "")
                    log(f"Checking for new uploads (subscribed from {date_after[:4]}-{date_after[4:6]}-{date_after[6:]})...\n",
                        "green")

                with config_lock:
                    for cfg_ch in config.get("channels", []):
                        if cfg_ch["url"] == url:
                            cfg_ch["sync_complete"] = False
                save_config(config)

                cmd = build_channel_cmd(url, out_dir, min_dur, res, folder_ovr,
                                        break_on_existing=is_initialized and sync_complete,
                                        max_dur=ch.get("max_duration", 0),
                                        split_years=ch.get("split_years", False),
                                        split_months=ch.get("split_months", False))

                if not cancel_event.is_set():
                    # Always update anim state so switching to Simple mid-sync shows correct channel
                    _simple_anim_state.update({"channel": ch_name, "idx": i, "total": current_total,
                                               "dl_current": 0, "ch_total": 0})
                    if _is_simple_mode:
                        _start_simple_anim(ch_name, i, current_total)

                    # Skip prefetch for uninitialized full-mode channels
                    _skip_prefetch = (mode == "full"
                                      and not ch.get("init_complete", False)
                                      and not is_initialized)
                    if _skip_prefetch:
                        ch_total = 0
                    else:
                        ch_total = _prefetch_total(url)
                    if cancel_event.is_set():
                        break
                    if ch_total and not (is_initialized and sync_complete):
                        log(f"  {ch_total:,} videos found. Scanning archive...\n", "dim")
                    elif not ch_total and not (is_initialized and sync_complete):
                        log(f"  (Could not fetch total — progress bar unavailable)\n", "dim")

                    # --- Batch safety: limit large channel downloads ---
                    _batch_pstart = 0
                    _batch_cache_ids = None
                    _batch_start_idx = 0
                    _batch_end_idx = 0
                    _all_cached_done = False
                    if _should_batch_limit(ch, ch_total):
                        batch_limited = True

                        # Try cached batch flow
                        _batch_cache_ids, _cache_created = _load_or_create_batch_cache(url)
                        if cancel_event.is_set():
                            break

                        if _batch_cache_ids:
                            _batch_start_idx = ch.get("batch_resume_index", 0)

                            # On subsequent runs, check for new uploads
                            if not _cache_created and _batch_start_idx > 0:
                                log("  Checking for new uploads...\n", "dim")
                                _new_ids = _check_new_videos(url, _batch_cache_ids)
                                if _new_ids:
                                    log(f"  Found {len(_new_ids)} new video(s), updating cache.\n", "green")
                                    _new_set = set(_new_ids)
                                    _batch_cache_ids = _new_ids + [x for x in _batch_cache_ids if x not in _new_set]
                                    _batch_start_idx += len(_new_ids)
                                    try:
                                        with open(_get_batch_cache_path(url), "w", encoding="utf-8") as _cf:
                                            _cf.write("\n".join(_batch_cache_ids) + "\n")
                                    except Exception:
                                        pass

                            # Pre-filter: skip already-archived IDs
                            _archived_set = _load_archived_ids()
                            _filtered_slice = None

                            while _batch_cache_ids and not cancel_event.is_set():
                                _batch_end_idx = min(_batch_start_idx + BATCH_LIMIT, len(_batch_cache_ids))
                                _batch_slice = _batch_cache_ids[_batch_start_idx:_batch_end_idx]
                                _filtered_slice = [vid for vid in _batch_slice if vid not in _archived_set]
                                _skipped_pre = len(_batch_slice) - len(_filtered_slice)

                                if _filtered_slice:
                                    if _skipped_pre:
                                        log(f"  Skipped {_skipped_pre:,} already-downloaded IDs in batch.\n", "dim")
                                    break

                                log(f"  Batch {_batch_start_idx:,}-{_batch_end_idx:,} fully archived, advancing...\n", "dim")
                                _batch_start_idx = _batch_end_idx

                                if _batch_start_idx >= len(_batch_cache_ids):
                                    _clear_batch_state(url, mark_complete=True)
                                    log(f"  All {len(_batch_cache_ids):,} cached videos already downloaded. Initialization complete!\n", "green")
                                    _all_cached_done = True
                                    _batch_cache_ids = None
                                    break

                            if _batch_cache_ids and _filtered_slice:
                                _bf_path = _build_batch_file(_filtered_slice)
                            else:
                                _bf_path = None

                            if _bf_path:
                                cmd = build_channel_cmd(url, out_dir, min_dur, res, folder_ovr,
                                                        break_on_existing=False,
                                                        max_dur=ch.get("max_duration", 0),
                                                        split_years=ch.get("split_years", False),
                                                        split_months=ch.get("split_months", False),
                                                        max_downloads=BATCH_LIMIT,
                                                        batch_file=_bf_path)
                                _remaining = len(_batch_cache_ids) - _batch_start_idx
                                log(f"  Large channel ({len(_batch_cache_ids):,} videos). Downloading {len(_filtered_slice):,} new videos (batch {_batch_start_idx:,}-{_batch_end_idx:,}, {_remaining:,} remaining)...\n", "green")
                            elif not _all_cached_done:
                                _batch_cache_ids = None  # fall through to legacy

                        if not _batch_cache_ids and not _all_cached_done:
                            # Legacy fallback
                            _batch_pstart = _get_batch_playlist_start(ch)
                            cmd = build_channel_cmd(url, out_dir, min_dur, res, folder_ovr,
                                                    break_on_existing=False,
                                                    max_dur=ch.get("max_duration", 0),
                                                    split_years=ch.get("split_years", False),
                                                    split_months=ch.get("split_months", False),
                                                    max_downloads=BATCH_LIMIT,
                                                    playlist_start=_batch_pstart)
                            if _batch_pstart > 1:
                                log(f"  Large channel ({ch_total:,} videos). Resuming from index {_batch_pstart}, batch of {BATCH_LIMIT:,}...\n", "green")
                            else:
                                log(f"  Large channel detected ({ch_total:,} videos). Downloading batch of {BATCH_LIMIT:,}...\n", "green")

                # Build incremental compress callback if compress is enabled
                _sc_level = ch.get("compress_level", "")
                _sc_batch_cb = None
                _sc_bsize = ch.get("compress_batch_size", 20)
                if ch.get("compress_enabled", False) and _sc_level in _QUALITY_OPTIONS:
                    _sc_folder = os.path.join(out_dir, sanitize_folder(ch.get("folder_override", "") or ch_name))
                    _sc_prompt_shown = [False]
                    def _sc_batch_cb(count, batch_paths, _ch=ch, _cn=ch_name, _u=url, _f=_sc_folder, _lv=_sc_level, _bs=_sc_bsize):
                        _b = _get_next_compress_batch(_u)
                        _add_to_gpu_queue({
                            "type": "encode", "ch_name": _cn, "ch_url": _u,
                            "folder": _f, "bitrate_mbhr": _get_compress_bitrate(_lv, _ch.get("compress_output_res", "")),
                            "output_res": _ch.get("compress_output_res", ""),
                            "split_years": _ch.get("split_years", False),
                            "split_months": _ch.get("split_months", False),
                            "batch_num": _b, "batch_size": _bs,
                            "target_paths": batch_paths,
                        }, _quiet=True)
                        if count >= 100 and not _sc_prompt_shown[0] and not _gpu_running:
                            _sc_prompt_shown[0] = True
                            if _ask_start_gpu_tasks(count):
                                _ui_queue.append(_gpu_start)

                if not cancel_event.is_set() and not _all_cached_done:
                    c_dl = internal_run_cmd_blocking(cmd, channel_total=ch_total if not cancel_event.is_set() else 0,
                                                     live_ids=live_ids,
                                                     on_batch_ready=_sc_batch_cb,
                                                     compress_batch_size=_sc_bsize)

                    # Also check /streams tab for past livestreams
                    _streams_url = _get_streams_url(url)
                    if _streams_url and not cancel_event.is_set():
                        _streams_cmd = build_channel_cmd(_streams_url, out_dir, min_dur, res, folder_ovr,
                                                         break_on_existing=is_initialized and sync_complete,
                                                         max_dur=ch.get("max_duration", 0),
                                                         split_years=ch.get("split_years", False),
                                                         split_months=ch.get("split_months", False))
                        _s_dl = internal_run_cmd_blocking(_streams_cmd, on_batch_ready=_sc_batch_cb,
                                                          compress_batch_size=_sc_bsize)
                        if _s_dl:
                            c_dl = (c_dl or 0) + _s_dl

                if c_dl:
                    ch_dl_map[ch_name] += c_dl

                _cleanup_batch_file()

                if cancel_event.is_set():
                    _stop_simple_anim()
                    break

                if _is_simple_mode:
                    _stop_simple_anim()
                    _v = "no new videos" if not c_dl else f"{c_dl} video{'s' if c_dl != 1 else ''}"
                    _tag = "simpleline_green" if c_dl else "simpleline"
                    _pad = 34 + len(str(current_total)) - len(str(i))
                    _cn = ch_name if len(ch_name) <= _pad else ch_name[:_pad - 3] + "..."
                    log(f"[{i}/{current_total}] {_cn:<{_pad}} —  Downloaded: {_v}\n", _tag)

                # --- Batch safety: handle batch completion ---
                _batch_more_remaining = False
                if batch_limited:
                    _total_processed = _last_run_counts["dl"] + _last_run_counts["skip"] + _last_run_counts["dur"] + _last_run_counts["err"]
                    if _batch_cache_ids:
                        _cache_pos = _batch_end_idx
                        _batch_all_done = (_cache_pos >= len(_batch_cache_ids))
                    else:
                        _batch_all_done = (_total_processed < BATCH_LIMIT)

                    if not _batch_all_done:
                        _batch_more_remaining = True
                        if _batch_cache_ids:
                            _save_batch_resume(url, _batch_pstart, _last_run_counts,
                                               cache_resume_index=_cache_pos)
                        else:
                            _save_batch_resume(url, _batch_pstart, _last_run_counts)
                        cooldown_dt = _set_batch_cooldown(url)
                        time_str = cooldown_dt.strftime("%I:%M%p").lstrip("0").lower()
                        date_str = cooldown_dt.strftime("%b %d")
                        log(f"\n  Batch complete — downloaded {c_dl:,} of ~{ch_total:,} videos.\n", "green")
                        log(f"  Next batch available after {time_str}, {date_str}\n", "green")
                    else:
                        _clear_batch_state(url, mark_complete=True)
                        log(f"  Channel initialization complete — all videos downloaded.\n", "green")

                _ts = datetime.now().strftime("%Y-%m-%d %H:%M")
                with config_lock:
                    for cfg_ch in config.get("channels", []):
                        if cfg_ch["url"] == url:
                            if not _batch_more_remaining:
                                cfg_ch["sync_complete"] = True
                                cfg_ch["initialized"] = True
                            cfg_ch["last_sync"] = _ts
                save_config(config)
                if _root_alive:
                    _ui_queue.append(refresh_channel_dropdowns)

                # New videos downloaded — track pending transcription count
                if c_dl > 0:
                    ch["transcription_pending"] = ch.get("transcription_pending", 0) + c_dl
                    with config_lock:
                        for _cfg_ch in config.get("channels", []):
                            if _cfg_ch.get("url") == url:
                                _cfg_ch["transcription_pending"] = _cfg_ch.get("transcription_pending", 0) + c_dl
                                break

                # Auto-compress: if channel has compress enabled and new videos were downloaded
                # (handled via incremental _sc_batch_cb during download; nothing extra needed here)

                # Auto-transcribe: if channel has auto_transcribe enabled and new videos were downloaded
                if c_dl > 0 and ch.get("auto_transcribe", False):
                    _at_folder = os.path.join(out_dir, sanitize_folder(ch.get("folder_override", "") or ch_name))
                    _at_sy = ch.get("split_years", False)
                    _at_sm = ch.get("split_months", False)
                    _add_to_gpu_queue({
                        "type": "transcribe", "ch_name": ch_name, "ch_url": url,
                        "folder": _at_folder, "split_years": _at_sy, "split_months": _at_sm,
                        "combined": not _at_sy
                    })

            if deferred_streams and not cancel_event.is_set():
                log(f"\n\n" + "█" * 55 + "\n", "livestream")
                log(f"  ⚠  DOWNLOADING {len(deferred_streams)} DEFERRED LIVESTREAM(S)  ⚠\n", "livestream")
                log(f"█" * 55 + "\n\n", "livestream")
                for _ds_ch, _ds_url, _ds_id in deferred_streams:
                    if cancel_event.is_set():
                        break
                    _ds_name = _ds_ch.get("name", _ds_id)
                    log(f"--- LIVESTREAM: {_ds_name} ---\n", "header")
                    _ds_out = os.path.join(out_dir, sanitize_folder(_ds_ch.get("folder_override", "") or _ds_name))
                    _ds_cmd = build_video_cmd(
                        _ds_url, _ds_out,
                        _ds_ch.get("resolution", "720")
                    )
                    c_dl = internal_run_cmd_blocking(_ds_cmd)
                    if c_dl:
                        ch_name = _ds_ch.get("name", "")
                        ch_dl_map[ch_name] = ch_dl_map.get(ch_name, 0) + c_dl

            if cancel_event.is_set():
                _stop_simple_anim()
                clear_transient_lines()
                log("\nSync cancelled.\n", "red")
                _cleanup_partial_files(out_dir)
                # Save batch resume if large channel and enough progress was made
                if ch_total > 200:
                    _cancel_total = _last_run_counts["dl"] + _last_run_counts["skip"] + _last_run_counts["dur"] + _last_run_counts["err"]
                    if _cancel_total > 50:
                        if _batch_cache_ids:
                            _save_batch_resume(url, _batch_pstart, _last_run_counts,
                                               cache_resume_index=_batch_start_idx)
                        else:
                            _save_batch_resume(url, _batch_pstart, _last_run_counts)
                        log(f"  Batch progress saved — will resume from here next sync.\n", "dim")
            else:
                zero_dl = sum(1 for count in ch_dl_map.values() if count == 0)

                log("\n" + "=" * 45 + "\n", "summary")
                log(f"TOTAL SYNC SUMMARY:\n", "summary")
                log(f"Downloaded: {session_totals['dl']}, Channels without new videos: {zero_dl}\n", "summary")
                if session_totals['err'] > 0:
                    log(f"Errors: {session_totals['err']}\n", "summary")
                log("=" * 45 + "\n", "summary")
                log("\n=== ALL CHANNELS SYNCED ===\n", "header")

                _ts = datetime.now().strftime("%Y-%m-%d %H:%M")
                # Only update the "Last full sync" timer if no items were removed
                # from the queue (i.e. this was a true complete sync)
                if not _queue_items_removed:
                    with config_lock:
                        config["last_sync"] = _ts
                    save_config(config)
                _dl = session_totals["dl"]
                if _root_alive:
                    _ui_queue.append(refresh_channel_dropdowns)
                    if not _queue_items_removed:
                        _ui_queue.append(lambda ts=_ts: _update_last_sync_display(ts))
                _plural = "s" if _dl != 1 else ""
                show_notification(
                    "YT Archiver — Sync complete",
                    f"Downloaded {_dl} video{_plural}. Errors: {session_totals['err']}"
                )
        finally:
            elapsed_manual = (datetime.now() - t_start_manual).total_seconds()
            _record_sync(session_totals["dl"], session_totals["err"], elapsed_manual, kind="Manual",
                         skipped=session_totals["dur"])

            # If a newer job has taken over, don't touch shared state
            if _job_generation == _my_gen:
                _sync_running = False
                _current_sync_ch = None
                _current_job["label"] = None
                _current_job["url"] = None
                _update_queue_btn()

                # Check for queued jobs in insertion order before fully finishing
                _queue_started = False
                if _skip_current.is_set():
                    _skip_current.clear()
                    cancel_event.clear()
                    _queue_started = _process_next_queued()
                elif not cancel_event.is_set():
                    _queue_started = _process_next_queued()

                if not _queue_started and _root_alive:
                    def _on_manual_sync_done():
                        _validate_download_btn()
                        sync_btn.config(state="normal", text="🔄 Sync Subbed")
                        _sync_task_finished()
                        _tray_stop_spin()
                        _dl = session_totals["dl"]
                        if _dl > 0:
                            _update_tray_tooltip(f"YT Archiver — {_dl} new video{'s' if _dl != 1 else ''} downloaded")
                        else:
                            _update_tray_tooltip("YT Archiver — Idle")

                        _iv = AUTORUN_OPTIONS.get(_cached_autorun_label, 0)
                        if _iv:
                            _schedule_autorun(_iv)

                    _ui_queue.append(_on_manual_sync_done)

                # Process any videos queued during the sync (only if no queued sync took over)
                if not cancel_event.is_set() and not _queue_started:
                    _process_video_dl_queue()

    t_start_manual = datetime.now()
    threading.Thread(target=_sync_worker, daemon=True).start()


download_btn = ttk.Button(btn_frame, text="▶  Download", command=start_download, style="Accent.TButton")
_ToolTip(download_btn, "Download the video from the URL above")
# download_btn starts hidden — shown/hidden by _validate_download_btn

sync_btn = ttk.Button(btn_frame, text="🔄 Sync Subbed", command=start_sync_all, style="Sync.TButton",
                      state="disabled")
sync_btn.pack(side="left", padx=(0, 6))
_ToolTip(sync_btn, "Sync and download every channel in your Sub list")

def _clear_all_logs():
    global _log_at_bottom, _log_user_scrolled
    log_box.config(state="normal")
    log_box.delete("1.0", tk.END)
    log_box.config(state="disabled")
    _log_at_bottom = True
    _log_user_scrolled = False
    if 'clear_log_btn' in globals():
        clear_log_btn.pack_forget()
    if 'subs_mini_log' in globals():
        for ml in (subs_mini_log, recent_mini_log):
            try:
                ml.config(state="normal")
                ml.delete("1.0", tk.END)
                ml.config(state="disabled")
            except Exception:
                pass


def _show_clear_log_if_needed():
    """Show Clear log button only when the log has content."""
    try:
        if 'clear_log_btn' not in globals():
            return
        content = log_box.get("1.0", "end-1c").strip()
        if content and not clear_log_btn.winfo_ismapped():
            clear_log_btn.pack(side="left", padx=(6, 0), after=sync_btn)
        elif not content and clear_log_btn.winfo_ismapped():
            clear_log_btn.pack_forget()
    except Exception:
        pass


# clear_log_btn created later after btn_frame/sync_btn exist (packed next to sync button)


def stop_downloads():
    global _sync_running, _reorg_running, _transcribe_running, _current_sync_ch
    pause_event.clear()

    # Clear any queued syncs, video downloads, reorg, and transcription jobs
    # (GPU Tasks is independent — cancelled only via its own menu)
    with _sync_queue_lock:
        _sync_queue.clear()
    with _video_dl_queue_lock:
        _video_dl_queue.clear()
    with _reorg_queue_lock:
        _reorg_queue.clear()
    with _transcribe_queue_lock:
        _transcribe_queue.clear()
    with _mt_queue_lock:
        _mt_queue.clear()
    with _queue_order_lock:
        _queue_order.clear()

    # Reset flags immediately so context-menu actions work right after cancel
    _sync_running = False
    _reorg_running = False
    _transcribe_running = False
    _transcribe_sync_controlled = False
    _current_sync_ch = None
    if not _gpu_running:
        _stop_whisper_process()  # Kill Whisper subprocess on cancel (only if GPU isn't using it)
        _stop_punct_process()   # Kill punctuation subprocess on cancel
        _clear_whisper_progress()  # Remove stale whisper progress line from log
    _current_job["label"] = None
    _current_job["url"] = None
    _tray_stop_spin()
    _update_tray_tooltip("YT Archiver — Idle")

    _update_queue_btn()
    _update_gpu_btn()

    cancel_event.set()

    _validate_download_btn()
    sync_btn.config(state="normal", text="🔄 Sync Subbed")
    sync_single_btn.config(state="normal", text="▶ Sync this channel")

    with proc_lock:
        procs = list(active_processes)
    for p in procs:
        if p.poll() is None:
            if os.name == "nt":
                subprocess.Popen(['taskkill', '/F', '/T', '/PID', str(p.pid)],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                 startupinfo=startupinfo)
            else:
                p.kill()
    log("\n⛔ Cancelling Syncs...\n", "red")


def _skip_current_job():
    """Cancel only the currently running job and proceed to the next queued item."""
    _skip_current.set()
    cancel_event.set()
    pause_event.clear()

    # Kill active download processes (but don't clear queues)
    with proc_lock:
        procs = list(active_processes)
    for p in procs:
        if p.poll() is None:
            if os.name == "nt":
                subprocess.Popen(['taskkill', '/F', '/T', '/PID', str(p.pid)],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                 startupinfo=startupinfo)
            else:
                p.kill()
    log("\n⏭ Skipping current job...\n", "red")


def _skip_current_gpu_job():
    """Cancel only the currently running GPU task and proceed to the next queued item."""
    _skip_current_gpu.set()
    _gpu_cancel.set()
    _gpu_pause.clear()

    # Kill active Whisper/ffmpeg subprocesses (but don't clear the GPU queue)
    _stop_whisper_process()
    _stop_ffmpeg_process()
    log("\n⏭ Skipping current GPU task...\n", "red")


def _fmt_time():
    return datetime.now().strftime("%I:%M%p").lstrip("0").lower()


def toggle_pause():
    if pause_event.is_set():
        pause_event.clear()
        # No OS-level process resume needed — the worker thread will
        # exit its pause loop and resume reading stdout, which unblocks
        # yt-dlp automatically via pipe backpressure.
    else:
        pause_event.set()
        # No OS-level process suspend needed — the worker thread will
        # stop reading stdout at the next pause check, the pipe buffer
        # fills up, and yt-dlp naturally blocks on its next write.


def _sync_task_finished():
    """Called when sync/reorg finishes — resets job state and updates sync tasks button."""
    if not _sync_running and not _reorg_running:
        pause_event.clear()
        _current_job["label"] = None
        _current_job["url"] = None
        _update_queue_btn()


# --- Sync Tasks Button ---
_queue_popup = {"win": None}  # Track the popup window

queue_btn = ttk.Button(btn_frame, text="📋", width=3, style="SyncQ.TButton", takefocus=False)
# Packed later after _last_sync_spacer is created

# --- Sync badge (green circle with count) ---
_sync_badge = tk.Label(queue_btn, text="", bg="#3a5a3a", fg="#ffffff",
                       font=("Segoe UI", 7, "bold"), padx=2, pady=0,
                       borderwidth=0, highlightthickness=0)
_sync_badge_count = {"n": -1}  # track last displayed count to avoid redundant updates


def _update_sync_badge():
    """Update the green badge count on the Sync Tasks button."""
    items = _get_queue_items()
    n = len(items)
    if n == _sync_badge_count["n"]:
        return  # no change
    _sync_badge_count["n"] = n
    if n > 0:
        _sync_badge.config(text=str(n) if n <= 99 else "99+")
        _sync_badge.place(relx=1.0, rely=0.0, anchor="ne", x=-1, y=1)
        _sync_badge.lift()
    else:
        _sync_badge.place_forget()


def _active_label(label, with_dots=False):
    """Convert task label to active/running form: 'Sync X' -> 'Syncing X', etc.
    If with_dots=True, append animated dots based on current time."""
    _verb_map = {
        "Sync ": "Syncing ",
        "Transcribe ": "Transcribing ",
        "Initialize ": "Initializing ",
        "Compress ": "Compressing ",
        "Re-Organize ": "Re-Organizing ",
        "Re-date & Organize ": "Re-dating & Organizing ",
        "Un-Organize ": "Un-Organizing ",
        "Download ": "Downloading ",
        "Backlog ": "Processing Backlog ",
        "M.T. ": "M.T. ",
    }
    result = label
    for prefix, replacement in _verb_map.items():
        if label.startswith(prefix):
            result = replacement + label[len(prefix):]
            break
    if with_dots:
        _dot_phase = int(time.time() * 2) % 3  # cycles every ~0.5s
        result += "." * (_dot_phase + 1)
    return result


def _get_queue_items():
    """Return a list of (label, queue_source, index) for all queued items.
    queue_source is 'sync', 'reorg', 'video', or 'current' — used for removal.
    index is the position within that specific queue.
    Items are returned in _queue_order (insertion order), not grouped by type.
    """
    items = []
    # Show currently processing item first
    if _current_job["label"]:
        if (_sync_running or _reorg_running or _transcribe_running) and pause_event.is_set():
            _active_lbl = _active_label(_current_job["label"]) + " (Paused)"
        elif _sync_running or _reorg_running or _transcribe_running:
            _active_lbl = _active_label(_current_job["label"], with_dots=True)
        else:
            _active_lbl = _current_job["label"]
        items.append((f"▶ {_active_lbl}", "current", -1))

    # Build lookup maps from type-specific queues, keyed by URL
    sync_map = {}
    with _sync_queue_lock:
        for i, ch in enumerate(_sync_queue):
            name = ch.get("name", "?")
            mode = ch.get("mode", "full")
            if not ch.get("initialized", False):
                lbl = f"Initialize {name}"
            else:
                lbl = f"Sync {name}"
            sync_map[ch["url"]] = (lbl, "sync", i)
    reorg_map = {}
    with _reorg_queue_lock:
        for i, args in enumerate(_reorg_queue):
            ch_name, _, t_years, t_months, ch_url, recheck = args
            key = ch_url or ch_name
            if recheck:
                lbl = f"Re-date & Organize {ch_name}"
            elif t_years:
                lbl = f"Re-Organize {ch_name}"
            else:
                lbl = f"Un-Organize {ch_name}"
            reorg_map[key] = (lbl, "reorg", i)
    transcribe_map = {}
    with _transcribe_queue_lock:
        for i, args in enumerate(_transcribe_queue):
            transcribe_map[args[1]] = (f"Transcribe {args[0]}", "transcribe", i)
    mt_map = {}
    with _mt_queue_lock:
        for i, fpath in enumerate(_mt_queue):
            mt_fname = os.path.splitext(os.path.basename(fpath))[0]
            mt_map[fpath] = (f"M.T. {mt_fname}", "mt", i)
    video_item = None
    with _video_dl_queue_lock:
        n_vids = len(_video_dl_queue)
        if n_vids == 1:
            video_item = ("Download 1 video", "video", 0)
        elif n_vids > 1:
            video_item = (f"Download {n_vids} videos", "video", 0)

    # Emit items in _queue_order (insertion order)
    seen = set()
    with _queue_order_lock:
        for source, key in _queue_order:
            if (source, key) in seen:
                continue
            seen.add((source, key))
            if source == "sync" and key in sync_map:
                items.append(sync_map.pop(key))
            elif source == "reorg" and key in reorg_map:
                items.append(reorg_map.pop(key))
            elif source == "transcribe" and key in transcribe_map:
                items.append(transcribe_map.pop(key))
            elif source == "mt" and key in mt_map:
                items.append(mt_map.pop(key))
            elif source == "video" and video_item:
                items.append(video_item)
                video_item = None

    # Append any orphaned items (added without _queue_order tracking, e.g. legacy/startup)
    for entry in sync_map.values():
        items.append(entry)
    for entry in reorg_map.values():
        items.append(entry)
    for entry in transcribe_map.values():
        items.append(entry)
    for entry in mt_map.values():
        items.append(entry)
    if video_item:
        items.append(video_item)

    return items


def _remove_queue_item(source, idx):
    """Remove an item from a specific queue by source and index."""
    global _queue_items_removed
    removed = None
    removed_key = None
    if source == "sync":
        with _sync_queue_lock:
            if 0 <= idx < len(_sync_queue):
                popped = _sync_queue.pop(idx)
                removed = popped.get("name", "?")
                removed_key = popped.get("url")
                _queue_items_removed = True
    elif source == "reorg":
        with _reorg_queue_lock:
            if 0 <= idx < len(_reorg_queue):
                popped = _reorg_queue.pop(idx)
                removed = popped[0]
                removed_key = popped[4] or popped[0]  # ch_url or ch_name
    elif source == "transcribe":
        with _transcribe_queue_lock:
            if 0 <= idx < len(_transcribe_queue):
                popped = _transcribe_queue.pop(idx)
                removed = popped[0]
                removed_key = popped[1]  # ch_url
    elif source == "mt":
        with _mt_queue_lock:
            if 0 <= idx < len(_mt_queue):
                popped = _mt_queue.pop(idx)
                removed = os.path.splitext(os.path.basename(popped))[0]
                removed_key = popped
    elif source == "video":
        with _video_dl_queue_lock:
            if _video_dl_queue:
                _video_dl_queue.clear()
                removed = "queued videos"
                removed_key = "batch"
    if removed_key:
        with _queue_order_lock:
            try:
                _queue_order.remove((source, removed_key))
            except ValueError:
                pass
    if removed:
        log(f"  Removed {removed} from queue.\n", "dim")
    _update_queue_btn()


def _confirm_remove(source, idx, label):
    if messagebox.askyesno("Remove from Queue", f"Remove \"{label}\" from the Sync List?"):
        _remove_queue_item(source, idx)


def _show_queue_menu(event=None):
    # Close existing popup if open
    if _queue_popup["win"] and _queue_popup["win"].winfo_exists():
        _queue_popup["win"].destroy()
        _queue_popup["win"] = None
        return

    popup = tk.Toplevel(root)
    popup.withdraw()  # Hide until positioned to avoid flash on open
    popup.overrideredirect(True)
    popup.configure(bg="#2d2d2d", highlightbackground="#555555", highlightthickness=1)
    _queue_popup["win"] = popup

    # Shared state for drag-to-reorder and live refresh
    _drag = {"active": False, "src_sync_idx": -1, "src_widget": None}
    _state = {"refresh_job": None, "last_snapshot": None, "wrapper": None, "widgets": [], "mw_bind_id": None, "active_lbl": None, "pause_resume_btn": None}

    def _build_content():
        """Build or rebuild queue popup content. Skips if nothing changed."""
        items = _get_queue_items()
        # Strip animated dots from snapshot so dot cycling doesn't trigger full rebuilds
        # Strip animated dots AND "(Paused)" from snapshot so neither triggers a full rebuild —
        # the active label widget is updated in-place in the no-structural-change path below.
        def _snap_label(lbl):
            s = lbl.rstrip(".")
            if s.endswith(" (Paused)"):
                s = s[:-9]
            return s
        snapshot = [(_snap_label(lbl), src, idx) for lbl, src, idx in items]
        if _state["last_snapshot"] == snapshot:
            # Just update the dot animation on the active item's label widget
            _albl = _state.get("active_lbl")
            if _albl and _current_job["label"] and (_sync_running or _reorg_running or _transcribe_running):
                try:
                    if pause_event.is_set():
                        _fresh = f"▶ {_current_job['label']} (Paused)"
                    else:
                        _fresh = f"▶ {_active_label(_current_job['label'], with_dots=True)}"
                    _albl.config(text=f"  1. {_fresh}")
                except Exception:
                    pass
            # Update pause/resume button text in-place to avoid full rebuild flicker
            _sync_pr_btn = _state.get("pause_resume_btn")
            if _sync_pr_btn:
                try:
                    if pause_event.is_set():
                        _sync_pr_btn.config(text="\u25B6 Resume", bg="#3a6a3a", activebackground="#4a8a4a")
                    else:
                        _sync_pr_btn.config(text="\u23F8 Pause", bg="#2a4a6b", activebackground="#3a5e84")
                except Exception:
                    pass
            return  # No structural change
        _state["last_snapshot"] = snapshot

        # Unbind old mousewheel before rebuilding
        if _state["mw_bind_id"]:
            try:
                popup.unbind("<MouseWheel>", _state["mw_bind_id"])
            except Exception:
                pass
            _state["mw_bind_id"] = None

        # Build new content into a fresh wrapper (double-buffer to avoid flash)
        wrapper = tk.Frame(popup, bg="#2d2d2d")
        _widgets = []

        # Header row: "  Sync Tasks (N)"
        hdr = tk.Frame(wrapper, bg="#2d2d2d")
        hdr.pack(fill="x", padx=4, pady=(6, 2))
        hdr_text = f"  Sync Tasks ({len(items)})" if items else "  Sync Tasks (empty)"
        tk.Label(hdr, text=hdr_text, bg="#2d2d2d", fg="#888888",
                 font=("Segoe UI", 9, "italic"), anchor="w").pack(side="left")

        if items:
            max_visible = 10
            show_count = min(len(items), max_visible)
            item_height = 26
            list_height = show_count * item_height

            canvas_frame = tk.Frame(wrapper, bg="#2d2d2d")
            canvas_frame.pack(fill="both", expand=True, padx=2)

            canvas = tk.Canvas(canvas_frame, bg="#2d2d2d", highlightthickness=0,
                               height=list_height, width=310)

            if len(items) > max_visible:
                scrollbar = ttk.Scrollbar(canvas_frame, orient="vertical", command=canvas.yview)
                scrollbar.pack(side="right", fill="y")
                canvas.configure(yscrollcommand=scrollbar.set)

            canvas.pack(side="left", fill="both", expand=True)

            inner = tk.Frame(canvas, bg="#2d2d2d")
            canvas.create_window((0, 0), window=inner, anchor="nw")

            for i, (label, source, idx) in enumerate(items):
                row = tk.Frame(inner, bg="#2d2d2d")
                row.pack(fill="x")

                if source == "current":
                    lbl = tk.Label(row, text=f"  {i + 1}. {label}", bg="#2d2d2d", fg="#6abf6a",
                                   font=("Segoe UI", 9, "bold"), anchor="w", padx=4, pady=2)
                    lbl.pack(side="left", fill="x", expand=True)
                    _widgets.append({"row": row, "lbl": lbl, "source": source, "idx": idx})
                    _state["active_lbl"] = lbl  # track for dot-only updates

                    # Right-click on current (running) item to skip it and move to next
                    def _skip_current_click(e):
                        popup.destroy()
                        if messagebox.askyesno("Skip Current", "Cancel the current job and move to the next one in queue?"):
                            _skip_current_job()
                    for w in [row, lbl]:
                        w.bind("<Button-3>", _skip_current_click)
                else:
                    handle = tk.Label(row, text=" ≡", bg="#2d2d2d", fg="#666666",
                                      font=("Segoe UI", 10), cursor="fleur", pady=2)
                    handle.pack(side="left")

                    lbl = tk.Label(row, text=f" {i + 1}. {label}", bg="#2d2d2d", fg="#cccccc",
                                   font=("Segoe UI", 9), anchor="w", padx=4, pady=2)
                    lbl.pack(side="left", fill="x", expand=True)

                    info = {"row": row, "lbl": lbl, "handle": handle, "source": source, "idx": idx}
                    _widgets.append(info)

                    # Hover highlight
                    def _enter(e, r=row, l=lbl, h=handle):
                        if not _drag["active"]:
                            r.config(bg="#444444")
                            l.config(bg="#444444")
                            if h: h.config(bg="#444444")
                    def _leave(e, r=row, l=lbl, h=handle):
                        if not _drag["active"]:
                            r.config(bg="#2d2d2d")
                            l.config(bg="#2d2d2d")
                            if h: h.config(bg="#2d2d2d")
                    for w in ([row, lbl] + ([handle] if handle else [])):
                        w.bind("<Enter>", _enter)
                        w.bind("<Leave>", _leave)

                    # Right-click to remove any non-current item
                    def _remove(e, s=source, qi=idx, lb=label):
                        popup.destroy()
                        _confirm_remove(s, qi, lb)
                    for w in ([row, lbl] + ([handle] if handle else [])):
                        w.bind("<Button-3>", _remove)

                    # Drag-to-reorder for all non-current items
                    def _make_drag_bindings(r, l, h, s=source):
                        def _find_my_widget_index():
                            """Look up this row's position in _state['widgets']."""
                            for wi_idx, wi in enumerate(_state["widgets"]):
                                if wi["row"] is r:
                                    return wi_idx
                            return -1

                        def _on_press(e):
                            _drag["active"] = True
                            _drag["src_widget_idx"] = _find_my_widget_index()
                            _drag["src_widget"] = r
                            r.config(bg="#555555")
                            l.config(bg="#555555")
                            if h: h.config(bg="#555555")

                        def _on_motion(e):
                            if not _drag["active"]:
                                return
                            y = e.y_root
                            for wi in _state["widgets"]:
                                if wi["source"] == "current" or wi["row"] == _drag["src_widget"]:
                                    continue
                                try:
                                    wy = wi["row"].winfo_rooty()
                                    wh2 = wi["row"].winfo_height()
                                    if wy <= y <= wy + wh2:
                                        wi["row"].config(bg="#3a5a3a")
                                        wi["lbl"].config(bg="#3a5a3a")
                                        if wi.get("handle"): wi["handle"].config(bg="#3a5a3a")
                                    else:
                                        wi["row"].config(bg="#2d2d2d")
                                        wi["lbl"].config(bg="#2d2d2d")
                                        if wi.get("handle"): wi["handle"].config(bg="#2d2d2d")
                                except Exception:
                                    pass

                        def _on_release(e):
                            if not _drag["active"]:
                                return
                            _drag["active"] = False
                            # Reset all backgrounds
                            for wi in _state["widgets"]:
                                try:
                                    wi["row"].config(bg="#2d2d2d")
                                    wi["lbl"].config(bg="#2d2d2d")
                                    if wi.get("handle"): wi["handle"].config(bg="#2d2d2d")
                                except Exception:
                                    pass
                            # Find drop target (any non-current row)
                            y = e.y_root
                            target_widget_idx = None
                            for wi_idx, wi in enumerate(_state["widgets"]):
                                if wi["source"] == "current":
                                    continue
                                try:
                                    wy = wi["row"].winfo_rooty()
                                    wh2 = wi["row"].winfo_height()
                                    if wy <= y <= wy + wh2:
                                        target_widget_idx = wi_idx
                                        break
                                except Exception:
                                    pass
                            src_wi = _drag["src_widget_idx"]
                            if target_widget_idx is not None and target_widget_idx != src_wi:
                                # Reorder _queue_order based on widget positions
                                # Widget list mirrors _queue_order but offset by 1 if "current" is at [0]
                                has_current = (_state["widgets"][0]["source"] == "current") if _state["widgets"] else False
                                src_qo = (src_wi - 1) if has_current else src_wi
                                dst_qo = (target_widget_idx - 1) if has_current else target_widget_idx
                                with _queue_order_lock:
                                    if 0 <= src_qo < len(_queue_order) and 0 <= dst_qo < len(_queue_order):
                                        moved = _queue_order.pop(src_qo)
                                        _queue_order.insert(dst_qo, moved)
                                # Update labels in-place
                                new_items = _get_queue_items()
                                _state["last_snapshot"] = [(lb, s2, ix) for lb, s2, ix in new_items]
                                for j, (new_label, new_source, new_idx) in enumerate(new_items):
                                    if j < len(_state["widgets"]):
                                        wi = _state["widgets"][j]
                                        prefix = "  " if new_source == "current" else " "
                                        wi["lbl"].config(text=f"{prefix}{j + 1}. {new_label}")
                                        wi["source"] = new_source
                                        wi["idx"] = new_idx

                        for w in [h, l, r]:
                            w.bind("<ButtonPress-1>", _on_press)
                            w.bind("<B1-Motion>", _on_motion)
                            w.bind("<ButtonRelease-1>", _on_release)

                    _make_drag_bindings(row, lbl, handle)

            inner.update_idletasks()
            canvas.configure(scrollregion=canvas.bbox("all"))

            # Mouse wheel scrolling — popup-level binding to avoid global event interference
            def _on_mousewheel(e):
                try:
                    canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
                except Exception:
                    pass
            _state["mw_bind_id"] = popup.bind("<MouseWheel>", _on_mousewheel, add="+")

            # Footer hint
            has_queued = any(s != "current" for _, s, _ in items)
            hint_text = "  Drag to reorder · Right-click to remove" if has_queued else ""
            hint = tk.Label(wrapper, text=hint_text, bg="#2d2d2d", fg="#4a4f5a",
                            font=("Segoe UI", 8), anchor="w")
            hint.pack(fill="x", padx=4, pady=(2, 2))

            # Footer buttons — show when running (Pause/Cancel) or queued (Start/Cancel)
            # Only count transcription as "sync-running" if it's sync-controlled (not GPU-driven)
            _sync_pipeline_active = _sync_running or _reorg_running or (_transcribe_running and _transcribe_sync_controlled)
            _show_sync_btns = False
            if _sync_pipeline_active:
                _show_sync_btns = True
            else:
                _has_queued = bool(_sync_queue) or bool(_reorg_queue) or bool(_transcribe_queue) or bool(_mt_queue)
                if _has_queued:
                    _show_sync_btns = True

            if _show_sync_btns:
                btn_row = tk.Frame(wrapper, bg="#2d2d2d")
                btn_row.pack(fill="x", padx=4, pady=(4, 6))

                if not _sync_pipeline_active:
                    # Not running but has queued items — show Start
                    def _start_sync_queue():
                        popup.destroy()
                        cancel_event.clear()
                        _process_next_queued()
                    tk.Button(btn_row, text="\u25B6 Start", bg="#3a6a3a", fg="#cccccc",
                              activebackground="#4a8a4a", activeforeground="#cccccc",
                              relief="flat", bd=0, font=("Segoe UI Emoji", 9, "bold"),
                              cursor="hand2", command=_start_sync_queue).pack(side="left", padx=(4, 4))
                    _state["pause_resume_btn"] = None
                else:
                    _pr_btn_ref = [None]
                    def _sync_toggle_pause():
                        toggle_pause()
                        # Update button text immediately (no 300ms refresh delay)
                        try:
                            _b = _pr_btn_ref[0]
                            if _b and _b.winfo_exists():
                                if pause_event.is_set():
                                    _b.config(text="\u25B6 Resume", bg="#3a6a3a",
                                              activebackground="#4a8a4a")
                                else:
                                    _b.config(text="\u23F8 Pause", bg="#2a4a6b",
                                              activebackground="#3a5e84")
                        except Exception:
                            pass
                    if pause_event.is_set():
                        _sync_pr_btn = tk.Button(btn_row, text="\u25B6 Resume", bg="#3a6a3a", fg="#cccccc",
                                  activebackground="#4a8a4a", activeforeground="#cccccc",
                                  relief="flat", bd=0, font=("Segoe UI Emoji", 9, "bold"),
                                  cursor="hand2", command=_sync_toggle_pause)
                    else:
                        _sync_pr_btn = tk.Button(btn_row, text="\u23F8 Pause", bg="#2a4a6b", fg="#cccccc",
                                  activebackground="#3a5e84", activeforeground="#cccccc",
                                  relief="flat", bd=0, font=("Segoe UI Emoji", 9, "bold"),
                                  cursor="hand2", command=_sync_toggle_pause)
                    _pr_btn_ref[0] = _sync_pr_btn
                    _sync_pr_btn.pack(side="left", padx=(4, 4))
                    _state["pause_resume_btn"] = _sync_pr_btn
                def _confirm_cancel_sync():
                    # Count total items (current + queued)
                    _total = 0
                    if _sync_pipeline_active:
                        _total += 1  # currently running job
                    with _sync_queue_lock:
                        _total += len(_sync_queue)
                    with _reorg_queue_lock:
                        _total += len(_reorg_queue)
                    with _transcribe_queue_lock:
                        _total += len(_transcribe_queue)
                    with _mt_queue_lock:
                        _total += len(_mt_queue)
                    if _total >= 2:
                        _choice = [None]
                        _cdlg = tk.Toplevel(root)
                        _cdlg.title("Cancel Queue")
                        _cdlg.configure(bg=C_BG)
                        _cdlg.resizable(False, False)
                        _cdlg.grab_set()
                        _cdlg.transient(root)
                        _cdlg.update_idletasks()
                        _apply_dark_title_bar(_cdlg)
                        tk.Label(_cdlg, text="Cancelling clears the queue. Are you sure?",
                                 bg=C_BG, fg=C_TEXT, font=("Segoe UI", 10),
                                 padx=20, pady=16).pack()
                        _cb_row = tk.Frame(_cdlg, bg=C_BG)
                        _cb_row.pack(pady=(0, 14))
                        def _do_pause():
                            _choice[0] = "pause"
                            _cdlg.destroy()
                        def _do_yes():
                            _choice[0] = "yes"
                            _cdlg.destroy()
                        def _do_no():
                            _cdlg.destroy()
                        if _sync_pipeline_active:
                            tk.Button(_cb_row, text="Yes", bg="#8b1a1a", fg="#ffffff",
                                      activebackground="#a52a2a", activeforeground="#ffffff",
                                      relief="flat", bd=0, font=("Segoe UI", 9, "bold"),
                                      padx=14, pady=4, cursor="hand2",
                                      command=_do_yes).pack(side="left", padx=(0, 8))
                            tk.Button(_cb_row, text="Pause", bg="#2a4a6b", fg="#cccccc",
                                      activebackground="#3a5e84", activeforeground="#cccccc",
                                      relief="flat", bd=0, font=("Segoe UI", 9, "bold"),
                                      padx=14, pady=4, cursor="hand2",
                                      command=_do_pause).pack(side="left")
                        else:
                            tk.Button(_cb_row, text="Yes", bg="#8b1a1a", fg="#ffffff",
                                      activebackground="#a52a2a", activeforeground="#ffffff",
                                      relief="flat", bd=0, font=("Segoe UI", 9, "bold"),
                                      padx=14, pady=4, cursor="hand2",
                                      command=_do_yes).pack(side="left", padx=(0, 8))
                            tk.Button(_cb_row, text="No", bg="#2a4a6b", fg="#cccccc",
                                      activebackground="#3a5e84", activeforeground="#cccccc",
                                      relief="flat", bd=0, font=("Segoe UI", 9, "bold"),
                                      padx=14, pady=4, cursor="hand2",
                                      command=_do_no).pack(side="left")
                        _cdlg.protocol("WM_DELETE_WINDOW", _cdlg.destroy)
                        # Center on root window
                        _cdlg.update_idletasks()
                        _rx = root.winfo_rootx() + root.winfo_width() // 2
                        _ry = root.winfo_rooty() + root.winfo_height() // 2
                        _cdlg.geometry(f"+{_rx - _cdlg.winfo_width() // 2}+{_ry - _cdlg.winfo_height() // 2}")
                        _cdlg.wait_window()
                        if _choice[0] == "pause":
                            toggle_pause()
                            return
                        elif _choice[0] != "yes":
                            return  # closed without choosing
                    stop_downloads()
                tk.Button(btn_row, text="\u26D4 Cancel", bg="#8b1a1a", fg="#ffffff",
                          activebackground="#a52a2a", activeforeground="#ffffff",
                          relief="flat", bd=0, font=("Segoe UI Emoji", 9, "bold"),
                          cursor="hand2", command=_confirm_cancel_sync).pack(side="left")
        else:
            empty = tk.Label(wrapper, text="  (empty)", bg="#2d2d2d", fg="#666666",
                             font=("Segoe UI", 9), anchor="w")
            empty.pack(fill="x", padx=4, pady=(0, 6))

        # Swap: pack new wrapper, then destroy old (double-buffer avoids flash)
        wrapper.pack(fill="both", expand=True)
        old_wrapper = _state["wrapper"]
        if old_wrapper:
            try:
                old_wrapper.destroy()
            except Exception:
                pass
        _state["wrapper"] = wrapper
        _state["widgets"] = _widgets

    _build_content()

    # Position below the button
    def _reposition_queue_popup(*_args):
        try:
            if popup.winfo_exists():
                x = queue_btn.winfo_rootx()
                y = queue_btn.winfo_rooty() + queue_btn.winfo_height()
                popup.geometry(f"+{x}+{y}")
        except Exception:
            pass

    _reposition_queue_popup()
    popup.deiconify()  # Show now that it's positioned

    # Follow main window when it moves
    _state["configure_bind_id"] = root.bind("<Configure>", _reposition_queue_popup, add="+")

    # --- Live refresh (updates queue while open) ---
    def _refresh():
        try:
            if popup.winfo_exists() and not _drag["active"]:
                _build_content()
            if popup.winfo_exists():
                _reposition_queue_popup()
                _state["refresh_job"] = popup.after(300, _refresh)
        except Exception:
            pass
    _state["refresh_job"] = popup.after(300, _refresh)

    # --- Close popup: toggle via queue button, or press Escape ---
    popup.bind("<Escape>", lambda e: popup.destroy())
    # Also bind Escape on root so it works even without popup focus
    def _esc_close(e):
        try:
            if popup.winfo_exists():
                popup.destroy()
        except Exception:
            pass
    _state["esc_bind_id"] = root.bind("<Escape>", _esc_close, add="+")

    # Close when clicking outside the popup
    _state["popup_alive"] = True

    def _on_click_outside_q(e):
        try:
            if not _state.get("popup_alive"):
                return
            if not popup.winfo_exists():
                return
            # Check if the click is on the queue_btn itself (toggle behavior)
            try:
                w = e.widget
                if w == queue_btn:
                    return  # Let the button's own command handle toggle
            except Exception:
                pass
            # Check if click is inside the popup
            px, py = popup.winfo_rootx(), popup.winfo_rooty()
            pw, ph = popup.winfo_width(), popup.winfo_height()
            if px <= e.x_root <= px + pw and py <= e.y_root <= py + ph:
                return  # Click inside popup — ignore
            popup.destroy()
        except Exception:
            pass
    # Delay binding by one event cycle so the button-press that opened
    # the popup does not immediately fire the "click outside" handler.
    def _bind_click_outside_q():
        if popup.winfo_exists():
            _state["click_outside_id"] = root.bind("<Button-1>", _on_click_outside_q, add="+")
    root.after(10, _bind_click_outside_q)

    def _on_popup_destroy(e):
        if e.widget != popup:
            return
        try:
            if _state["refresh_job"]:
                popup.after_cancel(_state["refresh_job"])
        except Exception:
            pass
        try:
            if _state.get("mw_bind_id"):
                popup.unbind("<MouseWheel>", _state["mw_bind_id"])
        except Exception:
            pass
        try:
            if _state.get("esc_bind_id"):
                root.unbind("<Escape>", _state["esc_bind_id"])
        except Exception:
            pass
        try:
            if _state.get("configure_bind_id"):
                root.unbind("<Configure>", _state["configure_bind_id"])
        except Exception:
            pass
        _state["popup_alive"] = False
        try:
            if _state.get("click_outside_id"):
                root.unbind("<Button-1>", _state["click_outside_id"])
                _state["click_outside_id"] = None
        except Exception:
            pass
        _queue_popup["win"] = None
    popup.bind("<Destroy>", _on_popup_destroy)


def _queue_btn_click(e=None):
    """Toggle the queue popup — bound to <Button-1> to suppress the pressed-state flicker."""
    _show_queue_menu()
    return "break"

queue_btn.bind("<Button-1>", _queue_btn_click, add="+")
_ToolTip(queue_btn, "Sync Tasks")


def _update_queue_btn():
    """Update sync tasks button state (badge, blink, popup refresh). Button is always visible."""
    def _do():
        try:
            items = _get_queue_items()
            # Close the popup when queue becomes empty (e.g. after cancel)
            if not items:
                if _queue_popup["win"] and _queue_popup["win"].winfo_exists():
                    _queue_popup["win"].destroy()
                    _queue_popup["win"] = None

            # Update badge count
            _update_sync_badge()

            # Manage sync blink animation
            _is_running = _sync_running or _reorg_running
            if _is_running and not _sync_blink["active"]:
                _sync_blink_start()
            elif not _is_running and _sync_blink["active"]:
                _sync_blink_stop()
        except Exception:
            pass
    try:
        _ui_queue.append(_do)
    except Exception:
        pass


# --- GPU Tasks Button ---
gpu_btn = ttk.Button(btn_frame, text="💻", width=3, style="Gpu.TButton", takefocus=False)
# Packed later after _last_sync_spacer is created

# --- GPU badge (red circle with count) ---
_gpu_badge = tk.Label(gpu_btn, text="", bg="#6b1a1a", fg="#ffffff",
                      font=("Segoe UI", 7, "bold"), padx=2, pady=0,
                      borderwidth=0, highlightthickness=0)
_gpu_badge_count = {"n": -1}  # track last displayed count to avoid redundant updates


def _update_gpu_badge():
    """Update the red badge count on the GPU button."""
    with _gpu_queue_lock:
        n = len(_gpu_queue)
    if _gpu_running and _gpu_current.get("label"):
        n += 1
    if n == _gpu_badge_count["n"]:
        return  # no change
    _gpu_badge_count["n"] = n
    if n > 0:
        _gpu_badge.config(text=str(n) if n <= 99 else "99+")
        _gpu_badge.place(relx=1.0, rely=0.0, anchor="ne", x=-1, y=1)
        _gpu_badge.lift()
    else:
        _gpu_badge.place_forget()


# --- Sync/GPU button blink animation (unified timer — always in sync) ---
# Blink = running, solid color = paused, default bg = idle
_sync_blink = {"active": False}
_gpu_blink = {"active": False}
_blink_clock = {"on": False, "job": None}

# Dedicated style for Sync Tasks button — same as Emoji.TButton but we toggle its background
style.configure("SyncQ.TButton", background=C_BTN, foreground=C_TEXT, padding=[2, 2], relief="flat",
                font=("Segoe UI Emoji", 11))
style.map("SyncQ.TButton", background=[("pressed", C_BTN), ("active", C_BTN_HVR), ("disabled", C_BORDER)])

# Dedicated style for GPU button — same as Emoji.TButton but we toggle its background
style.configure("Gpu.TButton", background=C_BTN, foreground=C_TEXT, padding=[2, 2], relief="flat",
                font=("Segoe UI Emoji", 11))
style.map("Gpu.TButton", background=[("pressed", C_BTN), ("active", C_BTN_HVR), ("disabled", C_BORDER)])


def _blink_tick():
    """Unified blink clock — toggles both Sync and GPU buttons in phase.
    Running = blink, Paused = solid color, Idle = default bg."""
    try:
        if not root.winfo_exists():
            _blink_clock["job"] = None
            return
        _blink_clock["on"] = not _blink_clock["on"]
        is_on = _blink_clock["on"]

        # Sync Tasks button
        if _sync_blink["active"]:
            if pause_event.is_set():
                # Paused → solid color
                style.configure("SyncQ.TButton", background="#3a5a3a")
                style.map("SyncQ.TButton", background=[("pressed", "#3a5a3a"), ("active", "#4a6a4a"), ("disabled", C_BORDER)])
            elif is_on:
                style.configure("SyncQ.TButton", background="#3a5a3a")
                style.map("SyncQ.TButton", background=[("pressed", "#3a5a3a"), ("active", "#4a6a4a"), ("disabled", C_BORDER)])
            else:
                style.configure("SyncQ.TButton", background=C_BTN)
                style.map("SyncQ.TButton", background=[("pressed", C_BTN), ("active", C_BTN_HVR), ("disabled", C_BORDER)])

        # GPU Tasks button — blink while actively encoding; go solid when paused between items
        # (i.e. pause is set AND nothing is actively encoding/transcribing).
        if _gpu_blink["active"] and gpu_btn.winfo_ismapped():
            if _gpu_truly_paused or (_gpu_pause.is_set() and not _gpu_actively_encoding):
                # Worker is waiting in the pause loop — hold solid red
                style.configure("Gpu.TButton", background="#6b1a1a")
                style.map("Gpu.TButton", background=[("pressed", "#6b1a1a"), ("active", "#8a2a2a"), ("disabled", C_BORDER)])
            elif is_on:
                style.configure("Gpu.TButton", background="#6b1a1a")
                style.map("Gpu.TButton", background=[("pressed", "#6b1a1a"), ("active", "#8a2a2a"), ("disabled", C_BORDER)])
            else:
                style.configure("Gpu.TButton", background=C_BTN)
                style.map("Gpu.TButton", background=[("pressed", C_BTN), ("active", C_BTN_HVR), ("disabled", C_BORDER)])

        if _sync_blink["active"] or _gpu_blink["active"]:
            _blink_clock["job"] = root.after(700, _blink_tick)
        else:
            _blink_clock["on"] = False
            _blink_clock["job"] = None
    except Exception:
        _blink_clock["job"] = None


def _ensure_blink_running():
    """Start the unified blink clock if it's not already ticking."""
    if _blink_clock["job"] is None and root.winfo_exists():
        _blink_clock["on"] = False
        _blink_clock["job"] = root.after(700, _blink_tick)


def _sync_blink_start():
    """Start the Sync Tasks button blink animation."""
    if _sync_blink["active"]:
        return
    _sync_blink["active"] = True
    _ensure_blink_running()


def _sync_blink_stop():
    """Stop the Sync Tasks button blink animation."""
    _sync_blink["active"] = False
    try:
        style.configure("SyncQ.TButton", background=C_BTN)
        style.map("SyncQ.TButton", background=[("pressed", C_BTN), ("active", C_BTN_HVR), ("disabled", C_BORDER)])
    except Exception:
        pass


def _gpu_blink_start():
    """Start the GPU button blink animation."""
    if _gpu_blink["active"]:
        return
    _gpu_blink["active"] = True
    _ensure_blink_running()


def _gpu_blink_stop():
    """Stop the GPU button blink animation."""
    _gpu_blink["active"] = False
    try:
        style.configure("Gpu.TButton", background=C_BTN)
        style.map("Gpu.TButton", background=[("pressed", C_BTN), ("active", C_BTN_HVR), ("disabled", C_BORDER)])
    except Exception:
        pass


def _update_gpu_btn():
    """Update GPU Tasks button state (badge, blink). Button is always visible."""
    def _do():
        try:
            # Update badge count
            _update_gpu_badge()

            # Manage blink animation based on GPU running state
            if _gpu_running and not _gpu_blink["active"]:
                _gpu_blink_start()
            elif not _gpu_running and _gpu_blink["active"]:
                _gpu_blink_stop()
        except Exception:
            pass
    try:
        _ui_queue.append(_do)
    except Exception:
        pass


def _get_gpu_queue_items():
    """Return list of (label, index) for display in the GPU Tasks popup."""
    items = []
    if _gpu_running and _gpu_current["label"]:
        items.append((f"▶ {_active_label(_gpu_current['label'], with_dots=True)}", -1))
    with _gpu_queue_lock:
        for i, item in enumerate(_gpu_queue):
            if item["type"] == "transcribe":
                items.append((f"Transcribe {item['ch_name']}", i))
            elif item["type"] == "encode":
                _bn = item.get("batch_num")
                _bl = f"Compress {item['ch_name']}, Batch {_bn}" if _bn is not None else f"Compress {item['ch_name']}"
                items.append((_bl, i))
            elif item["type"] == "backlog_encode":
                items.append((f"Backlog {item['ch_name']}", i))
            elif item["type"] == "mt":
                if item.get("folder_path"):
                    items.append((f"M.T. {item['folder_name']} ({item['vid_count']} files)", i))
                else:
                    fname = os.path.splitext(os.path.basename(item["file_path"]))[0]
                    items.append((f"M.T. {fname}", i))
    return items


def _remove_gpu_queue_item(idx):
    """Remove an item from the GPU queue by index."""
    with _gpu_queue_lock:
        if 0 <= idx < len(_gpu_queue):
            removed = _gpu_queue.pop(idx)
            if removed["type"] == "transcribe":
                label = removed["ch_name"]
            else:
                label = os.path.splitext(os.path.basename(removed.get("file_path", "")))[0]
            log(f"  Removed {label} from GPU Tasks.\n", "dim")
    _update_gpu_btn()
    _save_queue_state()


def _show_gpu_menu(event=None):
    """Show the GPU Tasks popup menu — styled to match the Sync Tasks popup."""
    if _gpu_popup["win"] and _gpu_popup["win"].winfo_exists():
        _gpu_popup["win"].destroy()
        _gpu_popup["win"] = None
        return

    popup = tk.Toplevel(root)
    popup.withdraw()  # Hide until positioned to avoid flash on open
    popup.overrideredirect(True)
    popup.configure(bg="#2d2d2d", highlightbackground="#555555", highlightthickness=1)
    _gpu_popup["win"] = popup

    _state = {"refresh_job": None, "last_snapshot": None, "wrapper": None, "mw_bind_id": None, "active_lbl": None, "widgets": [], "pause_resume_btn": None}
    _drag = {"active": False, "src_widget_idx": -1, "src_widget": None}

    def _mt_from_popup():
        popup.destroy()
        _gpu_popup["win"] = None
        _start_manual_transcription()

    def _confirm_gpu_remove(idx, label):
        with _gpu_queue_lock:
            _item = _gpu_queue[idx] if idx < len(_gpu_queue) else None
        if _item and _item.get("type") == "encode":
            msg = "Are you sure you want to remove unprocessed batches? There is no way to re-add them."
            title = "Remove Batch"
        else:
            msg = f"Remove '{label}' from the GPU Tasks?"
            title = "Remove from GPU Tasks"
        if messagebox.askyesno(title, msg, parent=popup):
            _remove_gpu_queue_item(idx)

    def _do_gpu_start():
        """Start GPU processing — closes popup first, then shows model dialog."""
        try:
            if popup.winfo_exists():
                popup.destroy()
                _gpu_popup["win"] = None
            _gpu_start()
        except Exception:
            pass

    def _build_content():
        """Build or rebuild GPU popup content. Skips if nothing changed."""
        items = _get_gpu_queue_items()
        # Strip animated dots from snapshot so dot cycling doesn't trigger full rebuilds.
        # Exclude button state (pause/resume) from snapshot — update button in-place instead.
        snapshot = [(lbl.rstrip("."), idx) for lbl, idx in items]
        if _state["last_snapshot"] == snapshot:
            # Just update the dot animation on the active item's label widget
            _albl = _state.get("active_lbl")
            if _albl and _gpu_running and _gpu_current.get("label"):
                try:
                    _fresh = f"▶ {_active_label(_gpu_current['label'], with_dots=True)}"
                    _albl.config(text=f"  1. {_fresh}")
                except Exception:
                    pass
            # Update pause/resume button text in-place to avoid full rebuild flicker
            _pr_btn = _state.get("pause_resume_btn")
            if _pr_btn and _gpu_running:
                try:
                    if _gpu_pause.is_set():
                        _pr_btn.config(text="\u25B6 Resume", bg="#3a6a3a", activebackground="#4a8a4a")
                    else:
                        _pr_btn.config(text="\u23F8 Pause", bg="#2a4a6b", activebackground="#3a5e84")
                except Exception:
                    pass
            return  # No structural change
        _state["last_snapshot"] = snapshot

        # Unbind old mousewheel before rebuilding
        if _state["mw_bind_id"]:
            try:
                popup.unbind("<MouseWheel>", _state["mw_bind_id"])
            except Exception:
                pass
            _state["mw_bind_id"] = None

        wrapper = tk.Frame(popup, bg="#2d2d2d")

        # Header row: "  GPU Tasks (N)  [📁]"
        hdr = tk.Frame(wrapper, bg="#2d2d2d")
        hdr.pack(fill="x", padx=4, pady=(6, 2))
        with _gpu_queue_lock:
            n = len(_gpu_queue)
        count_n = n + (1 if _gpu_running and _gpu_current["label"] else 0)
        hdr_text = f"  GPU Tasks ({count_n})" if count_n else "  GPU Tasks (empty)"
        tk.Label(hdr, text=hdr_text, bg="#2d2d2d", fg="#888888",
                 font=("Segoe UI", 9, "italic"), anchor="w").pack(side="left")
        # Small folder icon button for Manual Transcription
        _folder_btn = tk.Button(hdr, text="\U0001F4C1", bg="#2d2d2d", fg="#888888",
                  activebackground="#444444", activeforeground="#cccccc",
                  relief="flat", bd=0, font=("Segoe UI Emoji", 9),
                  cursor="hand2", command=_mt_from_popup, padx=4)
        _folder_btn.pack(side="right")
        _ToolTip(_folder_btn, "Manual Transcription")
        # Autorun checkbox — auto-start GPU Tasks when items are added
        _autorun_var = tk.BooleanVar(value=config.get("autorun_gpu", False))
        def _toggle_autorun_gpu():
            val = _autorun_var.get()
            config["autorun_gpu"] = val
            save_config(config)
        _autorun_cb = tk.Checkbutton(hdr, text="Auto", bg="#2d2d2d", fg="#888888",
                                      selectcolor="#2d2d2d", activebackground="#2d2d2d",
                                      activeforeground="#cccccc", font=("Segoe UI", 8),
                                      variable=_autorun_var, command=_toggle_autorun_gpu,
                                      bd=0, highlightthickness=0, pady=0, padx=2)
        _autorun_cb.pack(side="right")
        _ToolTip(_autorun_cb, "Auto-start GPU Tasks when new items are added")

        if items:
            max_visible = 10
            show_count = min(len(items), max_visible)
            item_height = 26
            list_height = show_count * item_height

            canvas_frame = tk.Frame(wrapper, bg="#2d2d2d")
            canvas_frame.pack(fill="both", expand=True, padx=2)

            canvas = tk.Canvas(canvas_frame, bg="#2d2d2d", highlightthickness=0,
                               height=list_height, width=310)

            if len(items) > max_visible:
                scrollbar = ttk.Scrollbar(canvas_frame, orient="vertical", command=canvas.yview)
                scrollbar.pack(side="right", fill="y")
                canvas.configure(yscrollcommand=scrollbar.set)

            canvas.pack(side="left", fill="both", expand=True)

            inner = tk.Frame(canvas, bg="#2d2d2d")
            canvas.create_window((0, 0), window=inner, anchor="nw")

            _widgets = []
            for i, (label, idx) in enumerate(items):
                row = tk.Frame(inner, bg="#2d2d2d")
                row.pack(fill="x")

                if idx == -1:
                    # Currently processing — green, bold
                    lbl = tk.Label(row, text=f"  {i + 1}. {label}", bg="#2d2d2d", fg="#6abf6a",
                                   font=("Segoe UI", 9, "bold"), anchor="w", padx=4, pady=2)
                    lbl.pack(side="left", fill="x", expand=True)
                    _state["active_lbl"] = lbl  # track for dot-only updates
                    _widgets.append({"row": row, "lbl": lbl, "handle": None, "source": "current", "idx": idx})

                    # Right-click on current (running) GPU item to skip it and move to next
                    def _skip_gpu_click(e):
                        popup.destroy()
                        _gpu_popup["win"] = None
                        if messagebox.askyesno("Skip Current", "Cancel the current GPU task and move to the next one?"):
                            _skip_current_gpu_job()
                    for w in [row, lbl]:
                        w.bind("<Button-3>", _skip_gpu_click)
                else:
                    handle = tk.Label(row, text=" ≡", bg="#2d2d2d", fg="#666666",
                                      font=("Segoe UI", 10), cursor="fleur", pady=2)
                    handle.pack(side="left")

                    lbl = tk.Label(row, text=f" {i + 1}. {label}", bg="#2d2d2d", fg="#cccccc",
                                   font=("Segoe UI", 9), anchor="w", padx=4, pady=2)
                    lbl.pack(side="left", fill="x", expand=True)

                    _widgets.append({"row": row, "lbl": lbl, "handle": handle, "source": "gpu", "idx": idx})

                    # Hover highlight
                    def _enter(e, r=row, l=lbl, h=handle):
                        if not _drag["active"]:
                            r.config(bg="#444444"); l.config(bg="#444444")
                            if h: h.config(bg="#444444")
                    def _leave(e, r=row, l=lbl, h=handle):
                        if not _drag["active"]:
                            r.config(bg="#2d2d2d"); l.config(bg="#2d2d2d")
                            if h: h.config(bg="#2d2d2d")
                    for w in ([row, lbl] + ([handle] if handle else [])):
                        w.bind("<Enter>", _enter)
                        w.bind("<Leave>", _leave)

                    # Right-click to remove
                    def _remove(e, qi=idx, lb=label):
                        _confirm_gpu_remove(qi, lb)
                    for w in ([row, lbl] + ([handle] if handle else [])):
                        w.bind("<Button-3>", _remove)

                    # Drag-to-reorder
                    def _make_drag_bindings(r, l, h):
                        def _find_my_widget_index():
                            for wi_idx, wi in enumerate(_state["widgets"]):
                                if wi["row"] is r:
                                    return wi_idx
                            return -1

                        def _on_press(e):
                            _drag["active"] = True
                            _drag["src_widget_idx"] = _find_my_widget_index()
                            _drag["src_widget"] = r
                            r.config(bg="#555555"); l.config(bg="#555555")
                            if h: h.config(bg="#555555")

                        def _on_motion(e):
                            if not _drag["active"]:
                                return
                            y = e.y_root
                            for wi in _state["widgets"]:
                                if wi["source"] == "current" or wi["row"] == _drag["src_widget"]:
                                    continue
                                try:
                                    wy = wi["row"].winfo_rooty()
                                    wh2 = wi["row"].winfo_height()
                                    if wy <= y <= wy + wh2:
                                        wi["row"].config(bg="#3a5a3a"); wi["lbl"].config(bg="#3a5a3a")
                                        if wi.get("handle"): wi["handle"].config(bg="#3a5a3a")
                                    else:
                                        wi["row"].config(bg="#2d2d2d"); wi["lbl"].config(bg="#2d2d2d")
                                        if wi.get("handle"): wi["handle"].config(bg="#2d2d2d")
                                except Exception:
                                    pass

                        def _on_release(e):
                            if not _drag["active"]:
                                return
                            _drag["active"] = False
                            for wi in _state["widgets"]:
                                try:
                                    wi["row"].config(bg="#2d2d2d"); wi["lbl"].config(bg="#2d2d2d")
                                    if wi.get("handle"): wi["handle"].config(bg="#2d2d2d")
                                except Exception:
                                    pass
                            y = e.y_root
                            target_widget_idx = None
                            for wi_idx, wi in enumerate(_state["widgets"]):
                                if wi["source"] == "current":
                                    continue
                                try:
                                    wy = wi["row"].winfo_rooty()
                                    wh2 = wi["row"].winfo_height()
                                    if wy <= y <= wy + wh2:
                                        target_widget_idx = wi_idx
                                        break
                                except Exception:
                                    pass
                            src_wi = _drag["src_widget_idx"]
                            if target_widget_idx is not None and target_widget_idx != src_wi:
                                has_current = (_state["widgets"][0]["source"] == "current") if _state["widgets"] else False
                                src_qo = (src_wi - 1) if has_current else src_wi
                                dst_qo = (target_widget_idx - 1) if has_current else target_widget_idx
                                with _gpu_queue_lock:
                                    if 0 <= src_qo < len(_gpu_queue) and 0 <= dst_qo < len(_gpu_queue):
                                        moved = _gpu_queue.pop(src_qo)
                                        _gpu_queue.insert(dst_qo, moved)
                                # Update labels in-place
                                new_items = _get_gpu_queue_items()
                                _state["last_snapshot"] = [(lb.rstrip("."), ix) for lb, ix in new_items] + [("__btn__", "__unchanged__")]
                                for j, (new_label, new_idx) in enumerate(new_items):
                                    if j < len(_state["widgets"]):
                                        wi = _state["widgets"][j]
                                        prefix = "  " if new_idx == -1 else " "
                                        wi["lbl"].config(text=f"{prefix}{j + 1}. {new_label}")
                                        wi["idx"] = new_idx
                                _save_queue_state()

                        for w in [h, l, r]:
                            w.bind("<ButtonPress-1>", _on_press)
                            w.bind("<B1-Motion>", _on_motion)
                            w.bind("<ButtonRelease-1>", _on_release)

                    _make_drag_bindings(row, lbl, handle)

            _state["widgets"] = _widgets

            inner.update_idletasks()
            canvas.configure(scrollregion=canvas.bbox("all"))

            # Use popup-level binding instead of bind_all to avoid global event interference
            def _on_mousewheel(e):
                try:
                    canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
                except Exception:
                    pass
            _state["mw_bind_id"] = popup.bind("<MouseWheel>", _on_mousewheel, add="+")

            # Footer hint
            has_queued = any(idx != -1 for _, idx in items)
            hint_text = "  Drag to reorder · Right-click to remove" if has_queued else ""
            hint = tk.Label(wrapper, text=hint_text, bg="#2d2d2d", fg="#4a4f5a",
                            font=("Segoe UI", 8), anchor="w")
            hint.pack(fill="x", padx=4, pady=(2, 2))

            # Footer buttons — only shown when there's something to act on
            _show_gpu_btns = False
            if not _gpu_running:
                with _gpu_queue_lock:
                    _show_gpu_btns = bool(_gpu_queue)
            else:
                _show_gpu_btns = True

            if _show_gpu_btns:
                btn_row = tk.Frame(wrapper, bg="#2d2d2d")
                btn_row.pack(fill="x", padx=4, pady=(4, 6))

                if not _gpu_running:
                    tk.Button(btn_row, text="\u25B6 Start", bg="#3a6a3a", fg="#cccccc",
                              activebackground="#4a8a4a", activeforeground="#cccccc",
                              relief="flat", bd=0, font=("Segoe UI Emoji", 9, "bold"),
                              cursor="hand2", command=_do_gpu_start).pack(side="left", padx=(4, 4))
                    _state["pause_resume_btn"] = None
                else:
                    _gpu_pr_btn_ref = [None]
                    def _gpu_toggle_pause():
                        _gpu_pause_handler()
                        # Update button text immediately (no 1000ms refresh delay)
                        try:
                            _b = _gpu_pr_btn_ref[0]
                            if _b and _b.winfo_exists():
                                if _gpu_pause.is_set():
                                    _b.config(text="\u25B6 Resume", bg="#3a6a3a",
                                              activebackground="#4a8a4a")
                                else:
                                    _b.config(text="\u23F8 Pause", bg="#2a4a6b",
                                              activebackground="#3a5e84")
                        except Exception:
                            pass
                    if _gpu_pause.is_set():
                        _pr_btn = tk.Button(btn_row, text="\u25B6 Resume", bg="#3a6a3a", fg="#cccccc",
                                  activebackground="#4a8a4a", activeforeground="#cccccc",
                                  relief="flat", bd=0, font=("Segoe UI Emoji", 9, "bold"),
                                  cursor="hand2", command=_gpu_toggle_pause)
                    else:
                        _pr_btn = tk.Button(btn_row, text="\u23F8 Pause", bg="#2a4a6b", fg="#cccccc",
                                  activebackground="#3a5e84", activeforeground="#cccccc",
                                  relief="flat", bd=0, font=("Segoe UI Emoji", 9, "bold"),
                                  cursor="hand2", command=_gpu_toggle_pause)
                    _gpu_pr_btn_ref[0] = _pr_btn
                    _pr_btn.pack(side="left", padx=(4, 4))
                    _state["pause_resume_btn"] = _pr_btn
                def _confirm_cancel_gpu():
                    _total = 0
                    if _gpu_running:
                        _total += 1  # currently running GPU task
                    with _gpu_queue_lock:
                        _total += len(_gpu_queue)
                    if _total >= 2:
                        _choice = [None]
                        _cdlg = tk.Toplevel(root)
                        _cdlg.title("Cancel GPU Tasks")
                        _cdlg.configure(bg=C_BG)
                        _cdlg.resizable(False, False)
                        _cdlg.grab_set()
                        _cdlg.transient(root)
                        _cdlg.update_idletasks()
                        _apply_dark_title_bar(_cdlg)
                        tk.Label(_cdlg, text="Cancelling clears the queue. Are you sure?",
                                 bg=C_BG, fg=C_TEXT, font=("Segoe UI", 10),
                                 padx=20, pady=16).pack()
                        _cb_row = tk.Frame(_cdlg, bg=C_BG)
                        _cb_row.pack(pady=(0, 14))
                        def _do_pause():
                            _choice[0] = "pause"
                            _cdlg.destroy()
                        def _do_yes():
                            _choice[0] = "yes"
                            _cdlg.destroy()
                        def _do_no():
                            _cdlg.destroy()
                        if _gpu_running:
                            tk.Button(_cb_row, text="Yes", bg="#8b1a1a", fg="#ffffff",
                                      activebackground="#a52a2a", activeforeground="#ffffff",
                                      relief="flat", bd=0, font=("Segoe UI", 9, "bold"),
                                      padx=14, pady=4, cursor="hand2",
                                      command=_do_yes).pack(side="left", padx=(0, 8))
                            tk.Button(_cb_row, text="Pause", bg="#2a4a6b", fg="#cccccc",
                                      activebackground="#3a5e84", activeforeground="#cccccc",
                                      relief="flat", bd=0, font=("Segoe UI", 9, "bold"),
                                      padx=14, pady=4, cursor="hand2",
                                      command=_do_pause).pack(side="left")
                        else:
                            tk.Button(_cb_row, text="Yes", bg="#8b1a1a", fg="#ffffff",
                                      activebackground="#a52a2a", activeforeground="#ffffff",
                                      relief="flat", bd=0, font=("Segoe UI", 9, "bold"),
                                      padx=14, pady=4, cursor="hand2",
                                      command=_do_yes).pack(side="left", padx=(0, 8))
                            tk.Button(_cb_row, text="No", bg="#2a4a6b", fg="#cccccc",
                                      activebackground="#3a5e84", activeforeground="#cccccc",
                                      relief="flat", bd=0, font=("Segoe UI", 9, "bold"),
                                      padx=14, pady=4, cursor="hand2",
                                      command=_do_no).pack(side="left")
                        _cdlg.protocol("WM_DELETE_WINDOW", _cdlg.destroy)
                        # Center on root window
                        _cdlg.update_idletasks()
                        _rx = root.winfo_rootx() + root.winfo_width() // 2
                        _ry = root.winfo_rooty() + root.winfo_height() // 2
                        _cdlg.geometry(f"+{_rx - _cdlg.winfo_width() // 2}+{_ry - _cdlg.winfo_height() // 2}")
                        _cdlg.wait_window()
                        if _choice[0] == "pause":
                            _gpu_pause_handler()
                            return
                        elif _choice[0] != "yes":
                            return  # closed without choosing
                    _gpu_cancel_handler()
                tk.Button(btn_row, text="\u26D4 Cancel", bg="#8b1a1a", fg="#ffffff",
                          activebackground="#a52a2a", activeforeground="#ffffff",
                          relief="flat", bd=0, font=("Segoe UI Emoji", 9, "bold"),
                          cursor="hand2", command=_confirm_cancel_gpu).pack(side="left")
        else:
            empty = tk.Label(wrapper, text="  (empty)", bg="#2d2d2d", fg="#666666",
                             font=("Segoe UI", 9), anchor="w")
            empty.pack(fill="x", padx=4, pady=(0, 6))

        # Swap: pack new wrapper, then destroy old (double-buffer avoids flash)
        wrapper.pack(fill="both", expand=True)
        old_wrapper = _state["wrapper"]
        if old_wrapper:
            try:
                old_wrapper.destroy()
            except Exception:
                pass
        _state["wrapper"] = wrapper

    _build_content()

    # Position below the button
    def _reposition_gpu_popup(*_args):
        try:
            if popup.winfo_exists():
                x = gpu_btn.winfo_rootx()
                y = gpu_btn.winfo_rooty() + gpu_btn.winfo_height()
                popup.geometry(f"+{x}+{y}")
        except Exception:
            pass

    _reposition_gpu_popup()
    popup.deiconify()  # Show now that it's positioned

    # Follow main window when it moves
    _state["configure_bind_id"] = root.bind("<Configure>", _reposition_gpu_popup, add="+")

    # --- Live refresh (double-buffered — no flicker) ---
    def _refresh():
        try:
            if popup.winfo_exists() and not _drag["active"]:
                _build_content()
            if popup.winfo_exists():
                _reposition_gpu_popup()
                _state["refresh_job"] = popup.after(300, _refresh)
        except Exception:
            pass
    _state["refresh_job"] = popup.after(1000, _refresh)

    # Close on Escape
    popup.bind("<Escape>", lambda e: popup.destroy())
    _state["esc_bind_id"] = root.bind("<Escape>", lambda e: (popup.destroy() if popup.winfo_exists() else None), add="+")

    # Close when clicking outside the popup
    _state["popup_alive"] = True

    def _on_click_outside(e):
        try:
            if not _state.get("popup_alive"):
                return
            if not popup.winfo_exists():
                return
            # Check if the click is on the gpu_btn itself (toggle behavior)
            try:
                w = e.widget
                if w == gpu_btn:
                    return  # Let the button's own command handle toggle
            except Exception:
                pass
            # Check if click is inside the popup
            px, py = popup.winfo_rootx(), popup.winfo_rooty()
            pw, ph = popup.winfo_width(), popup.winfo_height()
            if px <= e.x_root <= px + pw and py <= e.y_root <= py + ph:
                return  # Click inside popup — ignore
            popup.destroy()
        except Exception:
            pass
    # Delay binding by one event cycle so the button-press that opened
    # the popup does not immediately fire the "click outside" handler.
    def _bind_click_outside():
        if popup.winfo_exists():
            _state["click_outside_id"] = root.bind("<Button-1>", _on_click_outside, add="+")
    root.after(10, _bind_click_outside)

    def _on_popup_destroy(e):
        if e.widget != popup:
            return
        try:
            if _state["refresh_job"]:
                popup.after_cancel(_state["refresh_job"])
        except Exception:
            pass
        try:
            if _state.get("mw_bind_id"):
                popup.unbind("<MouseWheel>", _state["mw_bind_id"])
        except Exception:
            pass
        try:
            if _state.get("esc_bind_id"):
                root.unbind("<Escape>", _state["esc_bind_id"])
        except Exception:
            pass
        try:
            if _state.get("configure_bind_id"):
                root.unbind("<Configure>", _state["configure_bind_id"])
        except Exception:
            pass
        _state["popup_alive"] = False
        try:
            if _state.get("click_outside_id"):
                root.unbind("<Button-1>", _state["click_outside_id"])
                _state["click_outside_id"] = None
        except Exception:
            pass
        _gpu_popup["win"] = None
    popup.bind("<Destroy>", _on_popup_destroy)


def _gpu_btn_click(e=None):
    """Toggle the GPU popup — bound to <Button-1> to suppress the pressed-state flicker."""
    _show_gpu_menu()
    return "break"

gpu_btn.bind("<Button-1>", _gpu_btn_click, add="+")
_ToolTip(gpu_btn, "GPU Tasks")


def _gpu_start():
    """Start processing the GPU Tasks queue."""
    global _gpu_running, _whisper_model_choice

    with _gpu_queue_lock:
        if not _gpu_queue:
            return

    # Close popup before model dialog (if still open)
    if _gpu_popup["win"] and _gpu_popup["win"].winfo_exists():
        _gpu_popup["win"].destroy()
        _gpu_popup["win"] = None

    # Only show Whisper model dialog if queue contains transcription tasks
    _has_transcribe_tasks = False
    with _gpu_queue_lock:
        _has_transcribe_tasks = any(q["type"] in ("transcribe", "mt") for q in _gpu_queue)

    if not _has_transcribe_tasks:
        # No transcription tasks — skip model dialog and start immediately
        _gpu_running = True
        _gpu_cancel.clear()
        _gpu_pause.clear()
        _update_gpu_btn()

        def _gpu_worker():
            global _gpu_running, _gpu_current_item, _gpu_truly_paused
            _tray_start_spin(red=True)  # red indicator for any GPU task (encode/transcribe)
            try:
                while True:
                    if _gpu_pause.is_set() and not _gpu_cancel.is_set():
                        _tray_stop_spin(force=True)  # stop blinking while paused
                        log(f"  ⏸ GPU Tasks paused at {_fmt_time()} — click Resume.\n", "pausestatus")
                        _gpu_truly_paused = True
                        while _gpu_pause.is_set() and not _gpu_cancel.is_set():
                            time.sleep(0.25)
                        _gpu_truly_paused = False
                        if not _gpu_cancel.is_set():
                            _tray_start_spin(red=True)  # resume blinking
                            clear_pause_status()
                            log(f"  ▶ GPU Tasks resumed at {_fmt_time()}...\n", "pauselog")

                    if _gpu_cancel.is_set():
                        if _skip_current_gpu.is_set():
                            # Skip current task — clear cancel and continue to next item
                            _skip_current_gpu.clear()
                            _gpu_cancel.clear()
                        else:
                            log(f"\n  ⛔ GPU Tasks cancelled by user.\n", "red")
                            break

                    with _gpu_queue_lock:
                        if not _gpu_queue:
                            break
                        item = _gpu_queue.pop(0)
                    _gpu_current_item = item
                    _save_queue_state()

                    if item["type"] == "encode":
                        _ebn = item.get("batch_num")
                        _gpu_current["label"] = f"Compress {item['ch_name']}, Batch {_ebn}" if _ebn is not None else f"Compress {item['ch_name']}"
                        _gpu_current["ch_url"] = item.get("ch_url")
                        _update_gpu_btn()
                        if _is_simple_mode:
                            _update_encode_progress("Encode loading\n")
                        _compress_channel(
                            item["ch_name"], item["ch_url"], item["folder"],
                            item["bitrate_mbhr"],
                            item["split_years"], item["split_months"],
                            output_res=item.get("output_res", ""),
                            cancel_ev=_gpu_cancel, pause_ev=_gpu_pause,
                            _sync_mode=True,
                            target_paths=item.get("target_paths"),
                            batch_num=item.get("batch_num"),
                        )
                    elif item["type"] == "backlog_encode":
                        _gpu_current["label"] = f"Backlog {item['ch_name']}"
                        _gpu_current["ch_url"] = item.get("ch_url")
                        _update_gpu_btn()
                        if _is_simple_mode:
                            _update_encode_progress("Encode loading\n")
                        _backlog_compress_channel(
                            item["ch_name"], item["ch_url"], item["folder"],
                            item["resolution"], item["bitrate_mbhr"],
                            item.get("output_res", ""),
                            item["split_years"], item["split_months"],
                            batch_size=item.get("batch_size", 20),
                            cancel_ev=_gpu_cancel, pause_ev=_gpu_pause,
                            _sync_mode=True
                        )
                    elif item["type"] == "mt":
                        if item.get("folder_path"):
                            _gpu_current["label"] = f"M.T. {item['folder_name']} ({item['vid_count']} files)"
                            _gpu_current["ch_url"] = None
                            _update_gpu_btn()
                            _run_manual_transcription_folder(
                                item["folder_path"], item["folder_name"],
                                cancel_ev=_gpu_cancel, pause_ev=_gpu_pause,
                                _sync_mode=True
                            )
                        else:
                            fname = os.path.splitext(os.path.basename(item["file_path"]))[0]
                            _gpu_current["label"] = f"M.T. {fname}"
                            _gpu_current["ch_url"] = None
                            _update_gpu_btn()
                            _run_manual_transcription(
                                item["file_path"],
                                cancel_ev=_gpu_cancel, pause_ev=_gpu_pause,
                                skip_model_dialog=True, _sync_mode=True
                            )
                    elif item["type"] == "transcribe":
                        _gpu_current["label"] = f"Transcribe {item['ch_name']}"
                        _gpu_current["ch_url"] = item.get("ch_url")
                        _update_gpu_btn()
                        _ui_queue.append(refresh_channel_dropdowns)
                        _start_transcription(
                            item["ch_name"], item["ch_url"], item["folder"],
                            item["split_years"], item["split_months"], item["combined"],
                            cancel_ev=_gpu_cancel, pause_ev=_gpu_pause,
                            skip_model_dialog=True, _sync_mode=True
                        )

                    _gpu_current["label"] = None
                    _gpu_current["ch_url"] = None
                    _gpu_current_item = None
                    _update_gpu_btn()

                if not _gpu_cancel.is_set():
                    log(f"\n  ✓ All GPU Tasks completed.\n", "simpleline_green")
            except Exception as e:
                log(f"\n  ⚠ GPU Tasks error: {e}\n", "red")
            finally:
                _gpu_running = False
                _gpu_current["label"] = None
                _gpu_current["ch_url"] = None
                _gpu_current_item = None
                try:
                    _stop_whisper_process()
                except Exception:
                    pass
                try:
                    _stop_punct_process()
                except Exception:
                    pass
                try:
                    _stop_ffmpeg_process()
                except Exception:
                    pass
                _tray_stop_spin()
                _update_tray_tooltip("YT Archiver — Idle")
                _update_gpu_btn()
                _save_queue_state()
                try:
                    _ui_queue.append(_sync_task_finished)
                except Exception:
                    pass

        threading.Thread(target=_gpu_worker, daemon=True).start()
        return

    # Show model dialog directly on the main thread (using wait_window to avoid deadlock)
    _model_result = [None]
    _model_timed_out = [False]

    _dlg = tk.Toplevel(root)
    _dlg.title("Whisper Model Selection")
    _dlg.configure(bg=C_BG)
    _dlg.resizable(False, False)
    _dlg.transient(root)
    _dlg.grab_set()
    _dlg.update_idletasks()
    _apply_dark_title_bar(_dlg)
    _rx = root.winfo_rootx() + root.winfo_width() // 2
    _ry = root.winfo_rooty() + root.winfo_height() // 2
    _dlg.geometry(f"+{_rx - 160}+{_ry - 140}")

    tk.Label(_dlg, text="Which Whisper model for GPU Tasks?",
             bg=C_BG, fg=C_TEXT, font=("Segoe UI", 10, "bold"),
             pady=10, padx=20).pack(fill="x")
    tk.Label(_dlg, text="This model will be used for all queued tasks",
             bg=C_BG, fg=C_DIM, font=("Segoe UI", 9),
             padx=20).pack(fill="x")

    _btn_frame_dlg = tk.Frame(_dlg, bg=C_BG, pady=10)
    _btn_frame_dlg.pack(fill="x", padx=20)

    _models = [
        ("tiny",     "Fastest  (~30-50\u00d7 realtime)",  "tiny"),
        ("small",    "Fast  (~15-20\u00d7 realtime)",     "small"),
        ("medium",   "Balanced  (~7-10\u00d7 realtime)",  "medium"),
        ("large-v3", "Best quality  (~3-5\u00d7 realtime)", "large-v3"),
    ]
    _DEFAULT_MODEL = "small"
    _countdown = {"secs": 60, "job": None}

    def _pick(m):
        if _countdown["job"]:
            try:
                _dlg.after_cancel(_countdown["job"])
            except Exception:
                pass
        _model_result[0] = m
        try:
            _dlg.destroy()
        except Exception:
            pass

    _timer_lbl = tk.Label(_dlg, text=f"Auto-selecting {_DEFAULT_MODEL} in 60s...",
                          bg=C_BG, fg=C_DIM, font=("Segoe UI", 8),
                          padx=20)
    _timer_lbl.pack(fill="x", pady=(0, 4))

    def _tick():
        _countdown["secs"] -= 1
        if _countdown["secs"] <= 0:
            _model_timed_out[0] = True
            _pick(_DEFAULT_MODEL)
            return
        try:
            _timer_lbl.config(text=f"Auto-selecting {_DEFAULT_MODEL} in {_countdown['secs']}s...")
            _countdown["job"] = _dlg.after(1000, _tick)
        except Exception:
            pass

    _countdown["job"] = _dlg.after(1000, _tick)

    for label, desc, model_id in _models:
        _row = tk.Frame(_btn_frame_dlg, bg=C_BG)
        _row.pack(fill="x", pady=2)
        _b = tk.Button(_row, text=label, width=10,
                       bg="#3a3a3a", fg=C_TEXT, activebackground="#555555",
                       activeforeground=C_TEXT, relief="flat", bd=0,
                       font=("Segoe UI", 9, "bold"), cursor="hand2",
                       command=lambda m=model_id: _pick(m))
        _b.pack(side="left", padx=(0, 8))
        tk.Label(_row, text=desc, bg=C_BG, fg=C_DIM,
                 font=("Segoe UI", 9)).pack(side="left")
        if model_id == _DEFAULT_MODEL:
            tk.Label(_row, text="(default)", bg=C_BG, fg="#5a8a5a",
                     font=("Segoe UI", 8)).pack(side="left", padx=(4, 0))

    _dlg.protocol("WM_DELETE_WINDOW", lambda: _pick(_DEFAULT_MODEL))

    # Block until dialog closes (non-deadlocking — lets mainloop process events)
    root.wait_window(_dlg)

    model_choice = _model_result[0]
    timed_out = _model_timed_out[0]
    if model_choice is None:
        return
    _whisper_model_choice = model_choice
    _stop_whisper_process()

    if timed_out:
        log(f"\n  GPU Tasks: auto-selected {_whisper_model_choice} model\n", "simpleline")
    else:
        log(f"\n  GPU Tasks: using {_whisper_model_choice} model\n", "simpleline")

    _gpu_running = True
    _gpu_cancel.clear()
    _gpu_pause.clear()
    _update_gpu_btn()

    def _gpu_worker():
        global _gpu_running, _gpu_current_item, _gpu_truly_paused
        _tray_start_spin(red=True)  # red indicator for any GPU task (encode/transcribe)
        try:
            while True:
                # Pause check
                if _gpu_pause.is_set() and not _gpu_cancel.is_set():
                    _tray_stop_spin(force=True)  # stop blinking while paused
                    log(f"  ⏸ GPU Tasks paused at {_fmt_time()} — click Resume.\n", "pausestatus")
                    _gpu_truly_paused = True
                    while _gpu_pause.is_set() and not _gpu_cancel.is_set():
                        time.sleep(0.25)
                    _gpu_truly_paused = False
                    if not _gpu_cancel.is_set():
                        _tray_start_spin(red=True)  # resume blinking
                        clear_pause_status()
                        log(f"  ▶ GPU Tasks resumed at {_fmt_time()}...\n", "pauselog")

                if _gpu_cancel.is_set():
                    if _skip_current_gpu.is_set():
                        _skip_current_gpu.clear()
                        _gpu_cancel.clear()
                    else:
                        log(f"\n  ⛔ GPU Tasks cancelled by user.\n", "red")
                        break

                # Pop next item
                with _gpu_queue_lock:
                    if not _gpu_queue:
                        break
                    item = _gpu_queue.pop(0)
                _gpu_current_item = item
                _save_queue_state()

                if item["type"] == "mt":
                    if item.get("folder_path"):
                        _gpu_current["label"] = f"M.T. {item['folder_name']} ({item['vid_count']} files)"
                        _gpu_current["ch_url"] = None
                        _update_gpu_btn()
                        _run_manual_transcription_folder(
                            item["folder_path"], item["folder_name"],
                            cancel_ev=_gpu_cancel, pause_ev=_gpu_pause,
                            _sync_mode=True
                        )
                    else:
                        fname = os.path.splitext(os.path.basename(item["file_path"]))[0]
                        _gpu_current["label"] = f"M.T. {fname}"
                        _gpu_current["ch_url"] = None
                        _update_gpu_btn()
                        _run_manual_transcription(
                            item["file_path"],
                            cancel_ev=_gpu_cancel, pause_ev=_gpu_pause,
                            skip_model_dialog=True, _sync_mode=True
                        )
                elif item["type"] == "transcribe":
                    _gpu_current["label"] = f"Transcribe {item['ch_name']}"
                    _gpu_current["ch_url"] = item.get("ch_url")
                    _update_gpu_btn()
                    _start_transcription(
                        item["ch_name"], item["ch_url"], item["folder"],
                        item["split_years"], item["split_months"], item["combined"],
                        cancel_ev=_gpu_cancel, pause_ev=_gpu_pause,
                        skip_model_dialog=True, _sync_mode=True
                    )
                elif item["type"] == "encode":
                    _ebn = item.get("batch_num")
                    _gpu_current["label"] = f"Compress {item['ch_name']}, Batch {_ebn}" if _ebn is not None else f"Compress {item['ch_name']}"
                    _gpu_current["ch_url"] = item.get("ch_url")
                    _update_gpu_btn()
                    _compress_channel(
                        item["ch_name"], item["ch_url"], item["folder"],
                        item["bitrate_mbhr"],
                        item["split_years"], item["split_months"],
                        output_res=item.get("output_res", ""),
                        cancel_ev=_gpu_cancel, pause_ev=_gpu_pause,
                        _sync_mode=True,
                        target_paths=item.get("target_paths"),
                        batch_num=item.get("batch_num"),
                    )
                elif item["type"] == "backlog_encode":
                    _gpu_current["label"] = f"Backlog {item['ch_name']}"
                    _gpu_current["ch_url"] = item.get("ch_url")
                    _update_gpu_btn()
                    _backlog_compress_channel(
                        item["ch_name"], item["ch_url"], item["folder"],
                        item["resolution"], item["bitrate_mbhr"],
                        item.get("output_res", ""),
                        item["split_years"], item["split_months"],
                        batch_size=item.get("batch_size", 20),
                        cancel_ev=_gpu_cancel, pause_ev=_gpu_pause,
                        _sync_mode=True
                    )

                _gpu_current["label"] = None
                _gpu_current["ch_url"] = None
                _gpu_current_item = None
                _update_gpu_btn()

            if not _gpu_cancel.is_set():
                log(f"\n  ✓ All GPU Tasks completed.\n", "simpleline_green")
        except Exception as e:
            log(f"\n  ⚠ GPU Tasks error: {e}\n", "red")
        finally:
            _gpu_running = False
            _gpu_current["label"] = None
            _gpu_current["ch_url"] = None
            _gpu_current_item = None
            _stop_whisper_process()
            _stop_punct_process()
            _stop_ffmpeg_process()
            _tray_stop_spin()
            _update_tray_tooltip("YT Archiver — Idle")
            _update_gpu_btn()
            _save_queue_state()
            # Safety: ensure main cancel/pause buttons are hidden after GPU work
            _ui_queue.append(_sync_task_finished)

    threading.Thread(target=_gpu_worker, daemon=True).start()


def _gpu_cancel_handler():
    """Cancel all GPU Tasks."""
    _gpu_cancel.set()
    with _gpu_queue_lock:
        _gpu_queue.clear()
    _stop_whisper_process()
    _clear_whisper_progress()  # Remove stale whisper progress line from log
    log("\n⛔ Cancelling GPU Tasks...\n", "red")
    _update_gpu_btn()


def _gpu_pause_handler():
    """Toggle pause on GPU Tasks."""
    if _gpu_pause.is_set():
        _gpu_pause.clear()
    else:
        _gpu_pause.set()


def _format_last_sync(ts_str):
    if not ts_str:
        return "Not yet synced"
    try:
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M")
        now = datetime.now()
        diff_mins = int((now - dt).total_seconds() // 60)
        time_part = dt.strftime("%I:%M%p").lstrip("0").lower()
        date_part = dt.strftime("%b %-d") if os.name != "nt" else dt.strftime("%b {d}").replace("{d}", str(dt.day))
        if diff_mins < 1:
            ago = "just now"
        elif diff_mins < 60:
            ago = f"{diff_mins} min{'s' if diff_mins != 1 else ''} ago"
        else:
            diff_hrs = diff_mins // 60
            if diff_hrs < 24:
                ago = f"{diff_hrs} hr{'s' if diff_hrs != 1 else ''} ago"
            else:
                diff_days = diff_hrs // 24
                ago = f"{diff_days} day{'s' if diff_days != 1 else ''} ago"
        return f"{time_part}, {date_part}  ({ago})"
    except Exception:
        return ts_str


def _update_last_sync_display(ts_str=None):
    if _last_sync_job["id"]:
        try:
            root.after_cancel(_last_sync_job["id"])
        except Exception:
            pass

    if not ('last_sync_var' in globals() and root.winfo_exists()):
        return

    stored = ts_str if ts_str is not None else config.get("last_sync", "")
    last_sync_var.set(_format_last_sync(stored))
    _last_sync_job["id"] = root.after(60_000, lambda: _update_last_sync_display())


_last_sync_str = config.get("last_sync", "")
last_sync_var = tk.StringVar(value=_format_last_sync(_last_sync_str))
_last_sync_spacer = ttk.Frame(btn_frame)
_last_sync_spacer.pack(side="left", fill="x", expand=True)
# Pack queue buttons after the spacer — fixed position near Last Full Sync, unaffected by left-side buttons
queue_btn.pack(side="left", padx=(0, 0))
gpu_btn.pack(side="left", padx=(4, 0))
ttk.Label(btn_frame, text="Last Full Sync:", style="Dim.TLabel").pack(side="left", padx=(8, 4))
ttk.Label(btn_frame, textvariable=last_sync_var, style="Dim.TLabel").pack(side="left", padx=(0, 4))
root.after(100, lambda: _update_last_sync_display())

ttk.Separator(tab_download, orient="horizontal").grid(row=4, column=0, sticky="ew", padx=12, pady=(0, 4))

AUTORUN_OPTIONS = {"Off": 0, "30 min": 30, "1 hr": 60, "2 hr": 120, "4 hr": 240, "6 hr": 360, "12 hr": 720,
                   "24 hr": 1440}
AUTORUN_LABELS = list(AUTORUN_OPTIONS.keys())
AUTORUN_MINUTES = list(AUTORUN_OPTIONS.values())

autorun_frame = ttk.Frame(tab_download)
autorun_frame.grid(row=5, column=0, sticky="ew", padx=14, pady=(0, 2))
autorun_frame.columnconfigure(4, weight=1)

ttk.Label(autorun_frame, text="Auto-sync:", style="Dim.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 8))

_saved_interval = config.get("autorun_interval", 0)
_saved_label = next((k for k, v in AUTORUN_OPTIONS.items() if v == _saved_interval), "Off")
autorun_interval_var = tk.StringVar(value=_saved_label)
_cached_autorun_label = _saved_label  # initialise thread-safe cache
def _sync_autorun_cache(*_):
    global _cached_autorun_label
    _cached_autorun_label = autorun_interval_var.get()
autorun_interval_var.trace_add("write", _sync_autorun_cache)
autorun_combo = _combo(autorun_frame, textvariable=autorun_interval_var,
                       values=AUTORUN_LABELS, state="readonly", width=8)
autorun_combo.grid(row=0, column=1, padx=(0, 16))

autorun_countdown_var = tk.StringVar(value="")
ttk.Label(autorun_frame, textvariable=autorun_countdown_var, style="Dim.TLabel", width=22).grid(
    row=0, column=3, sticky="w", padx=(0, 4))

clear_log_btn = ttk.Button(btn_frame, text="Clear log", command=_clear_all_logs)
_ToolTip(clear_log_btn, "Clear the log output")
# Starts hidden — shown when log has content via _show_clear_log_if_needed()
# Packed into btn_frame (next to sync_btn) rather than autorun_frame


def _clear_autorun_history():
    with config_lock:
        config["autorun_history"] = []
    save_config(config)
    _refresh_autorun_history()


autorun_clear_btn = ttk.Button(autorun_frame, text="Clear", command=_clear_autorun_history,
                               style="TButton", padding=[4, 1])

ttk.Label(autorun_frame, text="Log:", style="Dim.TLabel").grid(row=0, column=5, sticky="e", padx=(0, 6))
_log_mode_frame = ttk.Frame(autorun_frame)
_log_mode_frame.grid(row=0, column=6, sticky="w")
ttk.Radiobutton(_log_mode_frame, text="Verbose", variable=log_mode_var, value="Verbose").pack(side="left", padx=(0, 8))
ttk.Radiobutton(_log_mode_frame, text="Simple", variable=log_mode_var, value="Simple").pack(side="left")


def _save_log_mode(*_):
    global _is_simple_mode
    _is_simple_mode = log_mode_var.get() == "Simple"
    with config_lock:
        config["log_mode"] = log_mode_var.get()
    save_config(config)

    # Handle log mode transitions during an active sync
    if _sync_running:
        if log_mode_var.get() == "Simple":
            # Switching to Simple — restart animation if we have channel info
            if _simple_anim_state.get("channel") and not _simple_anim_state["active"]:
                _simple_anim_state["active"] = True
                _simple_anim_state["dots"] = 0
                if root.winfo_exists():
                    _simple_anim_state["job"] = root.after(500, _simple_anim_tick)
        else:
            # Switching to Verbose — stop animation and clear status line
            if _simple_anim_state["active"]:
                _stop_simple_anim()
            clear_simple_status()


log_mode_var.trace_add("write", _save_log_mode)

_autorun_job = {"id": None}
_autorun_next = {"ts": None}

AUTORUN_HISTORY_MAX = 100


def _refresh_autorun_history():
    autorun_history_listbox.delete(0, tk.END)
    with config_lock:
        history = list(config.get("autorun_history", []))
    for entry in history:
        autorun_history_listbox.insert(tk.END, f"  {entry}")
    for i in range(0, autorun_history_listbox.size(), 2):
        autorun_history_listbox.itemconfig(i, bg="#0c0f14")
    # Scroll to bottom so newest entry is visible
    autorun_history_listbox.see(tk.END)

    # Get the currently visible panes
    current_panes = [str(p) for p in log_paned.panes()]

    if history:
        autorun_clear_btn.grid(row=0, column=2, sticky="w", padx=(8, 0))
        # Bring the frame back if it's not currently shown
        if str(autorun_history_frame) not in current_panes:
            log_paned.insert(0, autorun_history_frame, weight=0)
    else:
        autorun_clear_btn.grid_remove()
        # Hide the frame if it's currently visible
        if str(autorun_history_frame) in current_panes:
            log_paned.forget(autorun_history_frame)


def _record_sync(dl, err, elapsed_secs, kind="Auto", channel_name="", skipped=0):
    ts = datetime.now().strftime("%-I:%M%p").lower().lstrip("0") if os.name != "nt" else datetime.now().strftime(
        "%I:%M%p").lower().lstrip("0")
    date = datetime.now().strftime("%b {d}").replace("{d}", str(datetime.now().day))
    mins = int(elapsed_secs // 60)
    secs = int(elapsed_secs % 60)
    if mins >= 60:
        hrs = mins // 60
        rem_mins = mins % 60
        dur = f"took {hrs}h {rem_mins:02d}m"
    elif mins:
        dur = f"took {mins}m {secs:02d}s"
    else:
        dur = f"took {secs}s"
    ch_part = f"  {channel_name}  —" if channel_name else " "
    line = f"[{kind}] {ts}, {date}  —{ch_part}  {dl} downloaded · {skipped} skipped · {err} errors · {dur}"
    with config_lock:
        hist = config.setdefault("autorun_history", [])
        hist.append(line)
        if len(hist) > AUTORUN_HISTORY_MAX:
            config["autorun_history"] = hist[-AUTORUN_HISTORY_MAX:]
    save_config(config)
    if _root_alive:
        _ui_queue.append(_refresh_autorun_history)


_record_autorun = _record_sync


def _record_transcription(done_count, err_count, elapsed_secs, channel_name="", skipped=0):
    ts = datetime.now().strftime("%-I:%M%p").lower().lstrip("0") if os.name != "nt" else datetime.now().strftime(
        "%I:%M%p").lower().lstrip("0")
    date = datetime.now().strftime("%b {d}").replace("{d}", str(datetime.now().day))
    mins = int(elapsed_secs // 60)
    secs = int(elapsed_secs % 60)
    if mins >= 60:
        hrs = mins // 60
        rem_mins = mins % 60
        dur = f"took {hrs}h {rem_mins:02d}m"
    elif mins:
        dur = f"took {mins}m {secs:02d}s"
    else:
        dur = f"took {secs}s"
    ch_part = f"  {channel_name}  —" if channel_name else " "
    line = f"[Transcr.] {ts}, {date}  —{ch_part}  {done_count} transcribed · {skipped} skipped · {err_count} errors · {dur}"
    with config_lock:
        hist = config.setdefault("autorun_history", [])
        hist.append(line)
        if len(hist) > AUTORUN_HISTORY_MAX:
            config["autorun_history"] = hist[-AUTORUN_HISTORY_MAX:]
    save_config(config)
    if _root_alive:
        _ui_queue.append(_refresh_autorun_history)


def _record_compression(ch_name, done_count, err_count, elapsed_secs, batch_num=None):
    ts = datetime.now().strftime("%-I:%M%p").lower().lstrip("0") if os.name != "nt" else datetime.now().strftime(
        "%I:%M%p").lower().lstrip("0")
    date = datetime.now().strftime("%b {d}").replace("{d}", str(datetime.now().day))
    mins = int(elapsed_secs // 60)
    secs = int(elapsed_secs % 60)
    if mins >= 60:
        hrs = mins // 60
        rem_mins = mins % 60
        dur = f"took {hrs}h {rem_mins:02d}m"
    elif mins:
        dur = f"took {mins}m {secs:02d}s"
    else:
        dur = f"took {secs}s"
    batch_part = f" Batch {batch_num} —" if batch_num is not None else ""
    line = f"[Cmprss] {ts}, {date}  —  {ch_name}{batch_part}  {done_count} compressed · {err_count} errors · {dur}"
    with config_lock:
        hist = config.setdefault("autorun_history", [])
        hist.append(line)
        if len(hist) > AUTORUN_HISTORY_MAX:
            config["autorun_history"] = hist[-AUTORUN_HISTORY_MAX:]
    save_config(config)
    if _root_alive:
        _ui_queue.append(_refresh_autorun_history)


def _tick_countdown():
    try:
        if not root.winfo_exists():
            return
        nxt = _autorun_next.get("ts")
        if nxt:
            diff = int((nxt - datetime.now()).total_seconds())
            if diff > 0:
                h, rem = divmod(diff, 3600)
                m, s = divmod(rem, 60)
                if h:
                    _cd_text = f"{h}h {m:02d}m"
                    autorun_countdown_var.set(f"Next sync in: {_cd_text}")
                elif m:
                    _cd_text = f"{m}m {s:02d}s"
                    autorun_countdown_var.set(f"Next sync in: {_cd_text}")
                else:
                    _cd_text = f"{s}s"
                    autorun_countdown_var.set(f"Next sync in: {_cd_text}")
                # Update tray tooltip with countdown (only if not currently syncing)
                if not _sync_running:
                    _update_tray_tooltip(f"YT Archiver — Next sync in {_cd_text}")
            else:
                autorun_countdown_var.set("Syncing now...")
        root.after(1_000, _tick_countdown)
    except Exception:
        # Reschedule even on error to prevent countdown from dying
        try:
            root.after(2_000, _tick_countdown)
        except Exception:
            pass


def _run_autorun():
    global _sync_running
    if not root.winfo_exists():
        return

    if _sync_running:
        _autorun_job["id"] = root.after(60_000, _run_autorun)
        return
    interval_mins = AUTORUN_OPTIONS.get(autorun_interval_var.get(), 0)
    if not interval_mins:
        return

    with config_lock:
        channels = sorted(config.get("channels", []), key=lambda c: c.get("name", "").lower())
    if not channels:
        return

    if root.winfo_exists():
        log_box.config(state="normal")
        log_box.delete("1.0", tk.END)
        log_box.config(state="disabled")

    t_start = datetime.now()

    _autorun_next["ts"] = None
    autorun_countdown_var.set("Syncing now...")

    for key in session_totals:
        session_totals[key] = 0
    cancel_event.clear()

    if root.winfo_exists():
        _ui_queue.append(lambda: (
            sync_btn.config(state="disabled", text="⏳ Auto-syncing..."),
            _update_queue_btn(),
        ))

    global _job_generation
    _job_generation += 1
    _my_gen = _job_generation

    _sync_running = True
    _tray_start_spin()
    _update_tray_tooltip("YT Archiver — Auto-syncing...")
    _captured_outdir = outdir_var.get().strip() or BASE_DIR

    def _auto_worker():
        global _autorun_active, _sync_running, _current_sync_ch
        _autorun_active = True
        ch_dl_map = {}
        try:
            out_dir = _captured_outdir
            if not check_directory_writable(out_dir):
                log(f"ERROR: Cannot write to autorun target '{out_dir}'.\n", "red")
                return

            deferred_streams = []

            for i, ch in enumerate(channels, 1):
                if cancel_event.is_set():
                    break

                if pause_event.is_set():
                    clear_transient_lines()
                    log(f"  ⏸ Sync paused at {_fmt_time()} before channel {i}/{len(channels)} — click Resume.\n", "pausestatus")
                    while pause_event.is_set() and not cancel_event.is_set():
                        time.sleep(0.25)
                    if cancel_event.is_set():
                        break
                    clear_pause_status()
                    log(f"  ▶ Sync resumed at {_fmt_time()}...\n", "pauselog")

                ch_name = ch['name']
                ch_dl_map[ch_name] = 0
                _current_job["url"] = ch.get("url")
                _current_sync_ch = copy.deepcopy(ch)

                # GPU batch limit: skip channel if too many unprocessed encode batches
                _c_level_bl = ch.get("compress_level", "")
                if ch.get("compress_enabled", False) and _c_level_bl in _QUALITY_OPTIONS:
                    _pending_batches = _count_gpu_encode_batches(ch.get("url", ""))
                    if _pending_batches >= GPU_BATCH_LIMIT:
                        log(f"\n  ⏭ Skipping {ch_name} — {_pending_batches} unprocessed GPU batches (limit: {GPU_BATCH_LIMIT})\n", "dim")
                        continue

                log(f"\n--- [{i}/{len(channels)}] SYNCING: {ch_name} ---\n", "header")
                _update_tray_tooltip(f"YT Archiver — [{i}/{len(channels)}] {ch_name}")

                max_dur_ch = ch.get("max_duration", 0)
                live_ids = []
                if not cancel_event.is_set():
                    live_videos = _prefetch_livestreams(ch["url"])
                    if live_videos:
                        live_ids = [vid[0] for vid in live_videos]
                        if max_dur_ch:
                            log(f"  ⏭ {len(live_videos)} livestream(s) skipped (max-dur set).\n", "dim")
                        else:
                            for _lid, _lurl in live_videos:
                                deferred_streams.append((ch, _lurl, _lid))
                            _lnames = ", ".join(vid[0] for vid in live_videos)
                            log(f"\n", "livestream")
                            log(f"  ⚠  LIVESTREAM DETECTED — WILL DOWNLOAD AFTER SYNC  ⚠\n", "livestream")
                            log(f"  {len(live_videos)} stream(s) queued: {_lnames}\n", "livestream")
                            log(f"\n", "livestream")

                mode = ch.get("mode", "full")
                url = ch["url"]
                res = ch.get("resolution", "720")
                min_dur = ch.get("min_duration", 0)
                folder_ovr = ch.get("folder_override", "")
                is_init = ch.get("initialized", False)
                sync_complete = ch.get("sync_complete", True)

                # --- Batch safety: check cooldown for large full-mode channels ---
                batch_limited = False
                if mode == "full" and not ch.get("init_complete", False):
                    can_proceed, cooldown_str = _check_batch_cooldown(ch)
                    if not can_proceed:
                        log(f"  Skipping — next batch after {cooldown_str}\n", "dim")
                        if _is_simple_mode:
                            _stop_simple_anim()
                            _tot = len(channels)
                            _pad = 34 + len(str(_tot)) - len(str(i))
                            _cn = ch['name'] if len(ch['name']) <= _pad else ch['name'][:_pad - 3] + "..."
                            log(f"[{i}/{_tot}] {_cn:<{_pad}} —  Downloaded: None, hit daily limit. Resets at {cooldown_str}\n", "simpleline")
                        continue

                if mode == "sub" and not is_init:
                    success = internal_run_subscribe_blocking(url)
                    if cancel_event.is_set(): break
                    if success:
                        with config_lock:
                            for c in config.get("channels", []):
                                if c["url"] == url: c["initialized"] = True
                        save_config(config)
                    else:
                        continue
                elif mode == "date" and not is_init:
                    da = ch.get("date_after", "")
                    if not da: continue
                    success = internal_run_subscribe_before_date(url, da)
                    if cancel_event.is_set(): break
                    if success:
                        with config_lock:
                            for c in config.get("channels", []):
                                if c["url"] == url: c["initialized"] = True
                        save_config(config)
                    else:
                        continue

                with config_lock:
                    for cfg_ch in config.get("channels", []):
                        if cfg_ch["url"] == url:
                            cfg_ch["sync_complete"] = False
                save_config(config)

                cmd = build_channel_cmd(url, out_dir, min_dur, res, folder_ovr,
                                        break_on_existing=is_init and sync_complete,
                                        max_dur=ch.get("max_duration", 0),
                                        split_years=ch.get("split_years", False),
                                        split_months=ch.get("split_months", False))

                if not cancel_event.is_set():
                    # Always update anim state so switching to Simple mid-sync shows correct channel
                    _simple_anim_state.update({"channel": ch['name'], "idx": i, "total": len(channels),
                                               "dl_current": 0, "ch_total": 0})
                    if _is_simple_mode:
                        _start_simple_anim(ch['name'], i, len(channels))

                    # Skip prefetch for uninitialized full-mode channels
                    _skip_prefetch = (mode == "full"
                                      and not ch.get("init_complete", False)
                                      and not is_init)
                    if _skip_prefetch:
                        ch_total = 0
                    else:
                        ch_total = _prefetch_total(url)

                    # --- Batch safety: limit large channel downloads ---
                    _batch_pstart = 0
                    _batch_cache_ids = None
                    _batch_start_idx = 0
                    _batch_end_idx = 0
                    _all_cached_done = False
                    if _should_batch_limit(ch, ch_total):
                        batch_limited = True

                        # Try cached batch flow
                        _batch_cache_ids, _cache_created = _load_or_create_batch_cache(url)
                        if cancel_event.is_set():
                            break

                        if _batch_cache_ids:
                            _batch_start_idx = ch.get("batch_resume_index", 0)

                            # On subsequent runs, check for new uploads
                            if not _cache_created and _batch_start_idx > 0:
                                log("  Checking for new uploads...\n", "dim")
                                _new_ids = _check_new_videos(url, _batch_cache_ids)
                                if _new_ids:
                                    log(f"  Found {len(_new_ids)} new video(s), updating cache.\n", "green")
                                    _new_set = set(_new_ids)
                                    _batch_cache_ids = _new_ids + [x for x in _batch_cache_ids if x not in _new_set]
                                    _batch_start_idx += len(_new_ids)
                                    try:
                                        with open(_get_batch_cache_path(url), "w", encoding="utf-8") as _cf:
                                            _cf.write("\n".join(_batch_cache_ids) + "\n")
                                    except Exception:
                                        pass

                            # Pre-filter: skip already-archived IDs
                            _archived_set = _load_archived_ids()
                            _filtered_slice = None

                            while _batch_cache_ids and not cancel_event.is_set():
                                _batch_end_idx = min(_batch_start_idx + BATCH_LIMIT, len(_batch_cache_ids))
                                _batch_slice = _batch_cache_ids[_batch_start_idx:_batch_end_idx]
                                _filtered_slice = [vid for vid in _batch_slice if vid not in _archived_set]
                                _skipped_pre = len(_batch_slice) - len(_filtered_slice)

                                if _filtered_slice:
                                    if _skipped_pre:
                                        log(f"  Skipped {_skipped_pre:,} already-downloaded IDs in batch.\n", "dim")
                                    break

                                log(f"  Batch {_batch_start_idx:,}-{_batch_end_idx:,} fully archived, advancing...\n", "dim")
                                _batch_start_idx = _batch_end_idx

                                if _batch_start_idx >= len(_batch_cache_ids):
                                    _clear_batch_state(url, mark_complete=True)
                                    log(f"  All {len(_batch_cache_ids):,} cached videos already downloaded. Initialization complete!\n", "green")
                                    _all_cached_done = True
                                    _batch_cache_ids = None
                                    break

                            if _batch_cache_ids and _filtered_slice:
                                _bf_path = _build_batch_file(_filtered_slice)
                            else:
                                _bf_path = None

                            if _bf_path:
                                cmd = build_channel_cmd(url, out_dir, min_dur, res, folder_ovr,
                                                        break_on_existing=False,
                                                        max_dur=ch.get("max_duration", 0),
                                                        split_years=ch.get("split_years", False),
                                                        split_months=ch.get("split_months", False),
                                                        max_downloads=BATCH_LIMIT,
                                                        batch_file=_bf_path)
                                _remaining = len(_batch_cache_ids) - _batch_start_idx
                                log(f"  Large channel ({len(_batch_cache_ids):,} videos). Downloading {len(_filtered_slice):,} new videos (batch {_batch_start_idx:,}-{_batch_end_idx:,}, {_remaining:,} remaining)...\n", "green")
                            elif not _all_cached_done:
                                _batch_cache_ids = None  # fall through to legacy

                        if not _batch_cache_ids and not _all_cached_done:
                            # Legacy fallback
                            _batch_pstart = _get_batch_playlist_start(ch)
                            cmd = build_channel_cmd(url, out_dir, min_dur, res, folder_ovr,
                                                    break_on_existing=False,
                                                    max_dur=ch.get("max_duration", 0),
                                                    split_years=ch.get("split_years", False),
                                                    split_months=ch.get("split_months", False),
                                                    max_downloads=BATCH_LIMIT,
                                                    playlist_start=_batch_pstart)
                            if _batch_pstart > 1:
                                log(f"  Large channel ({ch_total:,} videos). Resuming from index {_batch_pstart}, batch of {BATCH_LIMIT:,}...\n", "green")
                            else:
                                log(f"  Large channel detected ({ch_total:,} videos). Downloading batch of {BATCH_LIMIT:,}...\n", "green")

                # Build incremental compress callback if compress is enabled
                _sc_level = ch.get("compress_level", "")
                _sc_batch_cb = None
                _sc_bsize = ch.get("compress_batch_size", 20)
                if ch.get("compress_enabled", False) and _sc_level in _QUALITY_OPTIONS:
                    _sc_folder = os.path.join(out_dir, sanitize_folder(ch.get("folder_override", "") or ch_name))
                    _sc_prompt_shown = [False]
                    def _sc_batch_cb(count, batch_paths, _ch=ch, _cn=ch_name, _u=url, _f=_sc_folder, _lv=_sc_level, _bs=_sc_bsize):
                        _b = _get_next_compress_batch(_u)
                        _add_to_gpu_queue({
                            "type": "encode", "ch_name": _cn, "ch_url": _u,
                            "folder": _f, "bitrate_mbhr": _get_compress_bitrate(_lv, _ch.get("compress_output_res", "")),
                            "output_res": _ch.get("compress_output_res", ""),
                            "split_years": _ch.get("split_years", False),
                            "split_months": _ch.get("split_months", False),
                            "batch_num": _b, "batch_size": _bs,
                            "target_paths": batch_paths,
                        }, _quiet=True)
                        if count >= 100 and not _sc_prompt_shown[0] and not _gpu_running:
                            _sc_prompt_shown[0] = True
                            if _ask_start_gpu_tasks(count):
                                _ui_queue.append(_gpu_start)

                if not cancel_event.is_set() and not _all_cached_done:
                    c_dl = internal_run_cmd_blocking(cmd, channel_total=ch_total if not cancel_event.is_set() else 0,
                                                     live_ids=live_ids,
                                                     on_batch_ready=_sc_batch_cb,
                                                     compress_batch_size=_sc_bsize)

                    # Also check /streams tab for past livestreams
                    _streams_url = _get_streams_url(url)
                    if _streams_url and not cancel_event.is_set():
                        _streams_cmd = build_channel_cmd(_streams_url, out_dir, min_dur, res, folder_ovr,
                                                         break_on_existing=is_init and sync_complete,
                                                         max_dur=ch.get("max_duration", 0),
                                                         split_years=ch.get("split_years", False),
                                                         split_months=ch.get("split_months", False))
                        _s_dl = internal_run_cmd_blocking(_streams_cmd, on_batch_ready=_sc_batch_cb,
                                                          compress_batch_size=_sc_bsize)
                        if _s_dl:
                            c_dl = (c_dl or 0) + _s_dl

                if c_dl:
                    ch_dl_map[ch_name] += c_dl

                _cleanup_batch_file()

                if cancel_event.is_set():
                    _stop_simple_anim()
                    break

                if _is_simple_mode:
                    _stop_simple_anim()
                    _v = "no new videos" if not c_dl else f"{c_dl} video{'s' if c_dl != 1 else ''}"
                    _tag = "simpleline_green" if c_dl else "simpleline"
                    _tot = len(channels)
                    _pad = 34 + len(str(_tot)) - len(str(i))
                    _cn = ch['name'] if len(ch['name']) <= _pad else ch['name'][:_pad - 3] + "..."
                    log(f"[{i}/{_tot}] {_cn:<{_pad}} —  Downloaded: {_v}\n", _tag)

                # --- Batch safety: handle batch completion ---
                _batch_more_remaining = False
                if batch_limited:
                    _total_processed = _last_run_counts["dl"] + _last_run_counts["skip"] + _last_run_counts["dur"] + _last_run_counts["err"]
                    if _batch_cache_ids:
                        _cache_pos = _batch_end_idx
                        _batch_all_done = (_cache_pos >= len(_batch_cache_ids))
                    else:
                        _batch_all_done = (_total_processed < BATCH_LIMIT)

                    if not _batch_all_done:
                        _batch_more_remaining = True
                        if _batch_cache_ids:
                            _save_batch_resume(url, _batch_pstart, _last_run_counts,
                                               cache_resume_index=_cache_pos)
                        else:
                            _save_batch_resume(url, _batch_pstart, _last_run_counts)
                        cooldown_dt = _set_batch_cooldown(url)
                        time_str = cooldown_dt.strftime("%I:%M%p").lstrip("0").lower()
                        date_str = cooldown_dt.strftime("%b %d")
                        log(f"\n  Batch complete — downloaded {c_dl:,} of ~{ch_total:,} videos.\n", "green")
                        log(f"  Next batch available after {time_str}, {date_str}\n", "green")
                    else:
                        _clear_batch_state(url, mark_complete=True)
                        log(f"  Channel initialization complete — all videos downloaded.\n", "green")

                _ts = datetime.now().strftime("%Y-%m-%d %H:%M")
                with config_lock:
                    for cfg_ch in config.get("channels", []):
                        if cfg_ch["url"] == url:
                            if not _batch_more_remaining:
                                cfg_ch["sync_complete"] = True
                                cfg_ch["initialized"] = True
                            cfg_ch["last_sync"] = _ts
                save_config(config)
                if _root_alive:
                    _ui_queue.append(refresh_channel_dropdowns)

                # New videos downloaded — track pending transcription count
                if c_dl > 0:
                    with config_lock:
                        for _cfg_ch in config.get("channels", []):
                            if _cfg_ch.get("url") == url:
                                _cfg_ch["transcription_pending"] = _cfg_ch.get("transcription_pending", 0) + c_dl
                                break

                # Auto-compress: if channel has compress enabled and new videos were downloaded
                # (handled via incremental _sc_batch_cb during download; nothing extra needed here)

                # Auto-transcribe: if channel has auto_transcribe enabled and new videos were downloaded
                if c_dl > 0 and ch.get("auto_transcribe", False):
                    ch_name_at = ch.get("name", "")
                    _at_folder = os.path.join(out_dir, sanitize_folder(ch.get("folder_override", "") or ch_name_at))
                    _at_sy = ch.get("split_years", False)
                    _at_sm = ch.get("split_months", False)
                    _add_to_gpu_queue({
                        "type": "transcribe", "ch_name": ch_name_at, "ch_url": url,
                        "folder": _at_folder, "split_years": _at_sy, "split_months": _at_sm,
                        "combined": not _at_sy
                    })

            if deferred_streams and not cancel_event.is_set():
                log(f"\n\n" + "█" * 55 + "\n", "livestream")
                log(f"  ⚠  DOWNLOADING {len(deferred_streams)} DEFERRED LIVESTREAM(S)  ⚠\n", "livestream")
                log(f"█" * 55 + "\n\n", "livestream")
                for _ds_ch, _ds_url, _ds_id in deferred_streams:
                    if cancel_event.is_set():
                        break
                    _ds_name = _ds_ch.get("name", _ds_id)
                    log(f"--- LIVESTREAM: {_ds_name} ---\n", "header")
                    _ds_out = os.path.join(out_dir, sanitize_folder(_ds_ch.get("folder_override", "") or _ds_name))
                    _ds_cmd = build_video_cmd(
                        _ds_url, _ds_out,
                        _ds_ch.get("resolution", "720")
                    )
                    c_dl = internal_run_cmd_blocking(_ds_cmd)
                    if c_dl:
                        ch_name = _ds_ch.get("name", "")
                        ch_dl_map[ch_name] = ch_dl_map.get(ch_name, 0) + c_dl

            if cancel_event.is_set():
                _stop_simple_anim()
                clear_transient_lines()
                log("\nSync cancelled.\n", "red")
                _cleanup_partial_files(out_dir)
                # Save batch resume if large channel and enough progress was made
                if ch_total > 200:
                    _cancel_total = _last_run_counts["dl"] + _last_run_counts["skip"] + _last_run_counts["dur"] + _last_run_counts["err"]
                    if _cancel_total > 50:
                        if _batch_cache_ids:
                            _save_batch_resume(url, _batch_pstart, _last_run_counts,
                                               cache_resume_index=_batch_start_idx)
                        else:
                            _save_batch_resume(url, _batch_pstart, _last_run_counts)
                        log(f"  Batch progress saved — will resume from here next sync.\n", "dim")
            else:
                zero_dl = sum(1 for count in ch_dl_map.values() if count == 0)

                log("\n" + "=" * 45 + "\n", "summary")
                log(f"TOTAL SESSION SUMMARY:\n", "summary")
                log(f"Downloaded: {session_totals['dl']}, Channels without new videos: {zero_dl}\n", "summary")
                if session_totals['err'] > 0:
                    log(f"Errors: {session_totals['err']}\n", "summary")
                log("=" * 45 + "\n", "summary")
                log("\n=== AUTO-SYNC COMPLETE ===\n", "header")

        finally:
            _autorun_active = False
            elapsed = (datetime.now() - t_start).total_seconds()
            _record_sync(session_totals["dl"], session_totals["err"], elapsed, kind="Auto",
                         skipped=session_totals["dur"])
            _ts = datetime.now().strftime("%Y-%m-%d %H:%M")
            with config_lock:
                config["last_sync"] = _ts
            save_config(config)

            # Always update display timestamps
            if _root_alive:
                _ui_queue.append(refresh_channel_dropdowns)
                _ui_queue.append(lambda ts=_ts: _update_last_sync_display(ts))

            # If a newer job has taken over, don't touch shared state
            if _job_generation == _my_gen:
                _sync_running = False
                _current_sync_ch = None

                # Check for queued jobs in insertion order before fully finishing
                _queue_started = False
                if _skip_current.is_set():
                    _skip_current.clear()
                    cancel_event.clear()
                    _queue_started = _process_next_queued()
                elif not cancel_event.is_set():
                    _queue_started = _process_next_queued()

                if _root_alive and not _queue_started:
                    _iv = interval_mins
                    _dl_count = session_totals["dl"]
                    def _on_auto_done(iv=_iv, dl=_dl_count):
                        sync_btn.config(state="normal", text="🔄 Sync Subbed")
                        _validate_download_btn()
                        _sync_task_finished()
                        _schedule_autorun(iv)
                        _tray_stop_spin()
                        if dl > 0:
                            _update_tray_tooltip(f"YT Archiver — {dl} new video{'s' if dl != 1 else ''} downloaded")
                        else:
                            _update_tray_tooltip("YT Archiver — Idle")
                    _ui_queue.append(_on_auto_done)

                # Process any videos queued during the sync (only if no queued sync took over)
                if not cancel_event.is_set() and not _queue_started:
                    _process_video_dl_queue()

    threading.Thread(target=_auto_worker, daemon=True).start()


def _schedule_autorun(interval_mins):
    if _autorun_job["id"]:
        try:
            root.after_cancel(_autorun_job["id"])
        except Exception:
            pass
    if interval_mins <= 0:
        _autorun_next["ts"] = None
        autorun_countdown_var.set("")
        return
    ms = interval_mins * 60 * 1000
    _autorun_job["id"] = root.after(ms, _run_autorun)
    from datetime import timedelta as _td

    if not _sync_running:
        _autorun_next["ts"] = datetime.now() + _td(minutes=interval_mins)


def _on_autorun_change(*_):
    label = autorun_interval_var.get()
    minutes = AUTORUN_OPTIONS.get(label, 0)
    with config_lock:
        config["autorun_interval"] = minutes
    save_config(config)
    _schedule_autorun(minutes)
    if minutes == 0:
        autorun_countdown_var.set("")


autorun_combo.bind("<<ComboboxSelected>>", _on_autorun_change)

_boot_interval = config.get("autorun_interval", 0)

_refresh_autorun_history()
if _boot_interval > 0:
    _schedule_autorun(_boot_interval)
root.after(1000, _tick_countdown)

# Now that AUTORUN_LABELS and _on_autorun_change exist, set up the tray icon
root.after(500, _deferred_tray_setup)

tab_recent = ttk.Frame(notebook)
notebook.add(tab_recent, text="  Recent  ")
tab_recent.columnconfigure(0, weight=1)
tab_recent.rowconfigure(1, weight=1)


def on_tab_changed(event):
    global new_download_count
    selected = notebook.select()

    # When leaving the Recent tab, clear any selection so the user doesn't
    # have to manually deselect before scrolling when they come back.
    if selected != str(tab_recent):
        try:
            if recent_tree.selection():
                recent_tree.selection_remove(*recent_tree.selection())
                delete_files_btn.config(state="disabled")
        except Exception:
            pass

    if selected == str(tab_download):
        # Re-show cancel/pause buttons if a sync or reorg is still active
        # (they can lose pack state when switching tabs during long operations)
        if _sync_running or _reorg_running:
            _update_queue_btn()
    elif selected == str(tab_recent):
        if new_download_count > 0:
            new_download_count = 0
            notebook.tab(tab_recent, text="  Recent  ")
            _update_tray_badge()
            # Clear the "X new videos downloaded" tooltip since user has seen them
            if not _sync_running:
                nxt = _autorun_next.get("ts")
                if nxt and (nxt - datetime.now()).total_seconds() > 0:
                    pass  # _tick_countdown will update tooltip
                else:
                    _update_tray_tooltip("YT Archiver — Idle")
    elif selected == str(tab_settings):
        root.after(10, lambda: notebook.focus_set())


notebook.bind("<<NotebookTabChanged>>", on_tab_changed)

recent_header = ttk.Frame(tab_recent)
recent_header.grid(row=0, column=0, sticky="ew", padx=12, pady=(10, 4))
recent_header.columnconfigure(0, weight=1)
ttk.Label(recent_header, text="Recently downloaded videos", style="Dim.TLabel").grid(row=0, column=0, sticky="w")

delete_files_btn = ttk.Button(recent_header, text="Delete File", style="Cancel.TButton", state="disabled")
# Starts hidden — shown when a file is selected AND list has items


def clear_recent():
    with config_lock:
        config["recent_downloads"] = []
    save_config(config)
    refresh_recent_list()
    _update_recent_buttons()


clear_list_btn = ttk.Button(recent_header, text="Clear list", command=clear_recent, style="Warn.TButton")
# Starts hidden — shown when recent list has items


def _update_recent_buttons():
    """Show/hide Clear list and Delete File buttons based on recent list state."""
    has_items = bool(recent_tree.get_children())
    if has_items:
        if not clear_list_btn.winfo_ismapped():
            clear_list_btn.grid(row=0, column=3, sticky="e")
    else:
        clear_list_btn.grid_forget()
        delete_files_btn.grid_forget()
        delete_files_btn.config(state="disabled", text="Delete File")

recent_frame = ttk.Frame(tab_recent)
recent_frame.grid(row=1, column=0, sticky="nsew", padx=12, pady=(0, 12))
recent_frame.columnconfigure(0, weight=1)
recent_frame.rowconfigure(0, weight=1)
recent_scrollbar = ttk.Scrollbar(recent_frame, orient="vertical")
recent_scrollbar.grid(row=0, column=1, sticky="ns")
recent_scrollbar.grid_remove()  # hidden until needed

recent_tree = ttk.Treeview(recent_frame, style="Recent.Treeview",
                           columns=("title", "channel", "time", "duration", "size", "orig_idx"),
                           show="headings", selectmode="extended",
                           yscrollcommand=lambda f, l: _auto_scrollbar(recent_scrollbar, f, l))
recent_tree.config(displaycolumns=("title", "channel", "time", "duration", "size"))

_RECENT_COL_LABELS = {
    "title": "Title",
    "channel": "Channel",
    "time": "Downloaded",
    "duration": "Length",
    "size": "Size",
}

_recent_sort_state = {"col": None, "reverse": False}


def _sort_recent_tree(col, reverse):
    _recent_sort_state["col"] = col
    _recent_sort_state["reverse"] = reverse
    l = [(recent_tree.set(k, col), k) for k in recent_tree.get_children('')]

    if col == "size":
        def parse_sz(s):
            if not s: return 0
            s = s.strip()
            if s.endswith("GB"): return float(s[:-2]) * 1073741824
            if s.endswith("MB"): return float(s[:-2]) * 1048576
            if s.endswith("KB"): return float(s[:-2]) * 1024
            if s.endswith("B"): return float(s[:-1])
            return 0

        l.sort(key=lambda t: parse_sz(t[0]), reverse=reverse)
    elif col == "duration":
        def parse_dur(s):
            if not s: return 0
            parts = s.split(':')
            if len(parts) == 3: return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            if len(parts) == 2: return int(parts[0]) * 60 + int(parts[1])
            return 0

        l.sort(key=lambda t: parse_dur(t[0]), reverse=reverse)
    elif col == "time":
        l.sort(key=lambda t: int(recent_tree.set(t[1], "orig_idx")), reverse=not reverse)
    else:
        l.sort(key=lambda t: t[0].lower(), reverse=reverse)

    for index, (val, k) in enumerate(l):
        recent_tree.move(k, '', index)

    recent_tree.heading(col, command=lambda: _sort_recent_tree(col, not reverse))

    arrow = " ▲" if not reverse else " ▼"
    for c, base_label in _RECENT_COL_LABELS.items():
        anchor = "e" if c in ("duration", "size") else "w"
        if c == col:
            recent_tree.heading(c, text=base_label + arrow, anchor=anchor)
        else:
            recent_tree.heading(c, text=base_label, anchor=anchor)

    for i, item in enumerate(recent_tree.get_children('')):
        recent_tree.item(item, tags=("odd" if i % 2 else "even",))


recent_tree.heading("title", text="Title", anchor="w", command=lambda: _sort_recent_tree("title", False))
recent_tree.heading("channel", text="Channel", anchor="w", command=lambda: _sort_recent_tree("channel", False))
recent_tree.heading("time", text="Downloaded", anchor="w", command=lambda: _sort_recent_tree("time", False))
recent_tree.heading("duration", text="Length", anchor="e", command=lambda: _sort_recent_tree("duration", False))
recent_tree.heading("size", text="Size", anchor="e", command=lambda: _sort_recent_tree("size", False))

recent_tree.column("title", stretch=False, minwidth=120, width=400, anchor="w")
recent_tree.column("channel", stretch=False, minwidth=60, width=140, anchor="w")
recent_tree.column("time", stretch=False, minwidth=50, width=100, anchor="w")
recent_tree.column("duration", stretch=False, minwidth=40, width=62, anchor="e")
recent_tree.column("size", stretch=False, minwidth=40, width=72, anchor="e")

recent_tree.tag_configure("odd", background="#0c0f14")
recent_tree.tag_configure("even", background=C_LOG_BG)
recent_tree.grid(row=0, column=0, sticky="nsew")
recent_scrollbar.config(command=recent_tree.yview)

# Auto-fill title column to use remaining space when window is resized
_recent_resize_job = {"id": None}


def _on_recent_tree_configure(event=None):
    if _recent_resize_job["id"]:
        try:
            root.after_cancel(_recent_resize_job["id"])
        except Exception:
            pass
    _recent_resize_job["id"] = root.after(50, _do_recent_resize)


def _do_recent_resize():
    _recent_resize_job["id"] = None
    try:
        total = recent_tree.winfo_width()
        if total < 50:
            return
        other = sum(recent_tree.column(c, "width") for c in ("channel", "time", "duration", "size"))
        title_w = max(120, total - other - 22)
        recent_tree.column("title", width=title_w)
    except Exception:
        pass


recent_tree.bind("<Configure>", _on_recent_tree_configure)

# Mini log for Recent tab
_recent_mini_log_frame = ttk.Frame(tab_recent)
_recent_mini_log_frame.grid(row=2, column=0, sticky="ew", padx=12, pady=(0, 8))
_recent_mini_log_frame.columnconfigure(0, weight=1)

recent_mini_log = tk.Text(_recent_mini_log_frame, state="disabled", height=4,
                           bg=C_LOG_BG, fg=C_TEXT, font=("Consolas", 9),
                           relief="flat", bd=0, highlightthickness=1,
                           highlightbackground=C_BORDER, highlightcolor=C_BORDER,
                           padx=8, pady=4, wrap="none")
recent_mini_log.grid(row=0, column=0, sticky="ew")

for _tag_name, _tag_cfg in [("green", {"foreground": C_LOG_GREEN}),
                             ("red", {"foreground": C_LOG_RED}),
                             ("header", {"foreground": C_LOG_HEAD, "font": ("Consolas", 9, "bold")}),
                             ("summary", {"foreground": C_LOG_SUM, "font": ("Consolas", 9, "italic")}),
                             ("simpleline", {"foreground": C_TEXT}),
                             ("simpleline_green", {"foreground": C_LOG_GREEN}),
                             ("simpleline_blue", {"foreground": C_LOG_BLUE}),
                             ("simpledownload", {"foreground": C_LOG_GREEN}),
                             ("simplestatus", {"foreground": C_LOG_HEAD, "font": ("Consolas", 9, "bold")}),
                             ("simplestatus_green", {"foreground": C_LOG_GREEN, "font": ("Consolas", 9, "bold")}),
                             ("dlprogress_pct", {"foreground": C_LOG_GREEN}),
                             ("pauselog", {"foreground": C_LOG_HEAD}),
                             ("pausestatus", {"foreground": C_LOG_HEAD}),
                             ("livestream", {"foreground": "#f5a023", "font": ("Consolas", 9, "bold")}),
                             ("filterskip", {"foreground": C_LOG_SUM}),
                             ("whisper_progress", {"foreground": C_LOG_BLUE}),
                             ("whisper_pct", {"foreground": C_LOG_GREEN, "font": ("Consolas", 9, "bold")}),
                             ("whisper_dots", {"foreground": C_LOG_BLUE}),
                             ("encode_progress", {"foreground": C_LOG_BLUE}),
                             ("encode_pct", {"foreground": C_LOG_GREEN, "font": ("Consolas", 9, "bold")}),
                             ("encode_dots", {"foreground": C_LOG_BLUE}),
                             ("encode_suffix", {"foreground": C_LOG_BLUE}),
                             ("transcribe_using", {"foreground": C_LOG_BLUE})]:
    recent_mini_log.tag_configure(_tag_name, **_tag_cfg)

# All known log tags for mini-log mirroring (priority order for detection)
# simplestatus_green must precede simplestatus so the SYNCING status renders green;
# dlprogress_pct must precede dlprogress so the progress bar percentage is green.
_ALL_LOG_TAGS = ("green", "red", "header", "summary", "simpleline", "simpleline_green",
                 "simpleline_blue", "simpledownload",
                 "simplestatus_green", "simplestatus",
                 "dlprogress_pct", "dlprogress", "scanline",
                 "pauselog", "pausestatus", "livestream", "filterskip", "dim", "whisper_progress",
                 "whisper_pct", "whisper_dots", "encode_progress", "encode_pct", "encode_dots",
                 "encode_suffix", "transcribe_using")


def _sync_mini_logs_from_main():
    """Mirror the last 4 lines from the main log_box to both mini logs.

    Uses dump() to preserve multi-tag lines (e.g. whisper progress where
    the percentage has a different color tag than the surrounding text).
    """
    try:
        if 'log_box' not in globals() or not log_box.winfo_exists():
            return
        if 'subs_mini_log' not in globals():
            return

        # Get total line count in main log
        end_idx = log_box.index("end-1c")
        total_lines = int(end_idx.split(".")[0])
        # Empty log check
        if total_lines <= 1 and not log_box.get("1.0", "end-1c").strip():
            for ml in (subs_mini_log, recent_mini_log):
                try:
                    ml.config(state="normal")
                    ml.delete("1.0", tk.END)
                    ml.config(state="disabled")
                except Exception:
                    pass
            return

        start_line = max(1, total_lines - 3)  # last 4 lines

        # Use dump() to extract text with tag transitions preserved
        _allowed = set(_ALL_LOG_TAGS)
        segments = []  # list of (text, tag_or_None)
        # Seed active_tags with tags already active at the start of the dump range
        active_tags = set(t for t in _ALL_LOG_TAGS if t in log_box.tag_names(f"{start_line}.0"))
        try:
            for item in log_box.dump(f"{start_line}.0", "end-1c", tag=True, text=True):
                kind = item[0]
                if kind == "tagon" and item[1] in _allowed:
                    active_tags.add(item[1])
                elif kind == "tagoff" and item[1] in _allowed:
                    active_tags.discard(item[1])
                elif kind == "text":
                    # Pick the highest-priority active tag
                    tag = None
                    for t in _ALL_LOG_TAGS:
                        if t in active_tags:
                            tag = t
                            break
                    segments.append((item[1], tag))
        except Exception:
            # Fallback: simple line-by-line copy without multi-tag support
            segments = []
            for line_num in range(start_line, total_lines + 1):
                line_text = log_box.get(f"{line_num}.0", f"{line_num}.end")
                if not line_text and line_num == total_lines:
                    continue
                if line_num < total_lines:
                    line_text += "\n"
                tags_at = log_box.tag_names(f"{line_num}.0")
                line_tag = None
                for t in _ALL_LOG_TAGS:
                    if t in tags_at:
                        line_tag = t
                        break
                segments.append((line_text, line_tag))

        # Write to both mini logs
        for ml in (subs_mini_log, recent_mini_log):
            try:
                if not ml.winfo_exists():
                    continue
                ml.config(state="normal")
                ml.delete("1.0", tk.END)
                for text, tag in segments:
                    if tag:
                        ml.insert(tk.END, text, tag)
                    else:
                        ml.insert(tk.END, text)
                ml.see(tk.END)
                ml.config(state="disabled")
            except Exception:
                pass
    except Exception:
        pass


def _fmt_time_ago(ts):
    if not ts: return ""
    diff = time.time() - ts
    if diff < 60: return "just now"
    if diff < 3600: return f"{int(diff // 60)}m ago"
    if diff < 86400: return f"{int(diff // 3600)}h ago"
    return f"{int(diff // 86400)}d ago"


def _fmt_size(raw):
    try:
        b = int(raw)
        if b >= 1_073_741_824: return f"{b / 1_073_741_824:.1f} GB"
        if b >= 1_048_576:     return f"{b / 1_048_576:.0f} MB"
        if b >= 1_024:         return f"{b / 1_024:.0f} KB"
        return f"{b} B"
    except Exception:
        return ""


def _fmt_dur(raw):
    try:
        s = int(raw)
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"
    except Exception:
        return ""


def refresh_recent_list():
    sel_fingerprints = set()
    try:
        # Capture fingerprints directly from the tree's displayed values
        # (not from config, which may already have new items inserted)
        for iid in recent_tree.selection():
            title = recent_tree.set(iid, "title")
            channel = recent_tree.set(iid, "channel")
            sel_fingerprints.add((title, channel))
    except Exception:
        pass

    try:
        yview_top = recent_tree.yview()[0]
    except Exception:
        yview_top = 0.0

    # Save user-adjusted column widths before rebuild (skip title — it auto-fills)
    _saved_col_widths = {}
    for col in ("channel", "time", "duration", "size"):
        try:
            _saved_col_widths[col] = recent_tree.column(col, "width")
        except Exception:
            pass

    recent_tree.delete(*recent_tree.get_children())
    with config_lock:
        recent_data = list(config.get("recent_downloads", []))

    new_sel = []
    for i, e in enumerate(recent_data):
        raw_title = e.get('title', '?')
        title = str(raw_title).replace('\n', ' ').strip()

        # Strip Unicode replacement characters (mangled encoding artifacts)
        title = title.replace('\ufffd', '')

        channel = str(e.get('channel', '?')).replace('\n', ' ').strip()
        dl_ts = e.get("download_ts", 0)
        time_ago = _fmt_time_ago(dl_ts) if dl_ts else ""
        size = _fmt_size(e.get("size", ""))
        duration = _fmt_dur(e.get("duration", ""))
        tag = "odd" if i % 2 else "even"

        item = recent_tree.insert("", tk.END, values=(title, channel, time_ago, duration, size, i), tags=(tag,))

        # Reselect based on fingerprint (title + channel from the tree's displayed values)
        if (title, channel) in sel_fingerprints:
            new_sel.append(item)

    if new_sel:
        recent_tree.selection_set(new_sel)
        # Scroll to keep the selected item visible
        try:
            recent_tree.see(new_sel[0])
        except Exception:
            pass
    else:
        try:
            if yview_top > 0.0:
                recent_tree.yview_moveto(yview_top)
        except Exception:
            pass

    if _recent_sort_state["col"]:
        _sort_recent_tree(_recent_sort_state["col"], _recent_sort_state["reverse"])

    # Restore user-adjusted column widths after rebuild (deferred for correct geometry)
    def _restore_col_widths():
        for col, w in _saved_col_widths.items():
            try:
                recent_tree.column(col, width=w)
            except Exception:
                pass
    root.after_idle(_restore_col_widths)

    # Show/hide buttons based on list state
    if '_update_recent_buttons' in globals():
        _update_recent_buttons()


def _on_recent_select(event=None):
    sel = recent_tree.selection()
    if not sel:
        delete_files_btn.grid_forget()
        delete_files_btn.config(state="disabled", text="Delete File")
    elif len(sel) == 1:
        if not delete_files_btn.winfo_ismapped():
            delete_files_btn.grid(row=0, column=1, sticky="e", padx=(0, 8))
        delete_files_btn.config(state="normal", text="Delete File")
    else:
        if not delete_files_btn.winfo_ismapped():
            delete_files_btn.grid(row=0, column=1, sticky="e", padx=(0, 8))
        delete_files_btn.config(state="normal", text=f"Delete {len(sel)} Files")


def _get_recent_orig_idx(iid):
    return int(recent_tree.set(iid, "orig_idx"))


def _delete_selected_files():
    sel = recent_tree.selection()
    if not sel:
        return
    n = len(sel)
    noun = f"{n} file" if n == 1 else f"{n} files"
    if not messagebox.askyesno(
            "Confirm Delete",
            f"Permanently delete {noun} from disk?\n\nThis cannot be undone.",
            icon="warning"):
        return

    indices = sorted([_get_recent_orig_idx(iid) for iid in sel], reverse=True)
    with config_lock:
        recent = config.get("recent_downloads", [])
        for idx in indices:
            if 0 <= idx < len(recent):
                fp = recent[idx].get("filepath", "")
                if not fp:
                    log(f"  ⚠ No filepath stored for '{recent[idx].get('title', '?')}' — remove from list only.\n",
                        "red")
                elif not os.path.exists(fp):
                    log(f"  ⚠ File not found on disk, removing from list:\n    {fp}\n", "red")
                else:
                    try:
                        fsize = _fmt_size(str(os.path.getsize(fp)))
                        size_str = f"  ({fsize})" if fsize else ""
                        os.remove(fp)
                        log(f"  🗑 Deleted: {fp}{size_str}\n", "green")
                    except OSError as e:
                        log(f"ERROR: Could not delete {fp}: {e}\n", "red")
                recent.pop(idx)
        config["recent_downloads"] = recent
    save_config(config)
    refresh_recent_list()
    delete_files_btn.config(state="disabled", text="Delete File")


def _try_find_by_title(entry):
    """Try to locate a video file by matching its title in the channel's output directory."""
    title = entry.get("title", "")
    channel_name = entry.get("channel", "")
    if not title or not channel_name:
        return None

    ch = None
    with config_lock:
        for c in config.get("channels", []):
            if c["name"] == channel_name or c.get("folder_override", "") == channel_name:
                ch = c
                break
    if not ch:
        return None

    with config_lock:
        base = config.get("output_dir", "").strip() or BASE_DIR
    folder_name = sanitize_folder(ch.get("folder_override", "").strip() or ch["name"])
    channel_dir = os.path.join(base, folder_name)
    if not os.path.isdir(channel_dir):
        return None

    norm_title = re.sub(r'[^\w]', '', title.lower())
    if len(norm_title) < 5:
        return None

    try:
        for dp, dns, fns in os.walk(channel_dir):
            for fn in fns:
                fb, fe = os.path.splitext(fn)
                if fe.lower() not in ('.mp4', '.mkv', '.webm'):
                    continue
                norm_fn = re.sub(r'[^\w]', '', fb.lower())
                if norm_fn == norm_title or (
                    len(norm_fn) >= 10 and (
                        norm_title.startswith(norm_fn) or
                        norm_fn.startswith(norm_title)
                    )
                ):
                    return os.path.join(dp, fn)
    except OSError:
        pass
    return None


def _get_channel_dir(entry):
    """Get the output directory for a channel based on the entry's channel name."""
    channel_name = entry.get("channel", "")
    if not channel_name:
        return None
    ch = None
    with config_lock:
        for c in config.get("channels", []):
            if c["name"] == channel_name or c.get("folder_override", "") == channel_name:
                ch = c
                break
    if not ch:
        return None
    with config_lock:
        base = config.get("output_dir", "").strip() or BASE_DIR
    folder_name = sanitize_folder(ch.get("folder_override", "").strip() or ch["name"])
    channel_dir = os.path.join(base, folder_name)
    return channel_dir if os.path.isdir(channel_dir) else None


def _try_locate_moved_file(entry, original_fp):
    """Try to find a video that was moved by reorganization using current channel settings."""
    channel_name = entry.get("channel", "")
    upload_date = entry.get("date", "")
    filename = os.path.basename(original_fp)
    if not channel_name or not filename:
        return None

    # Find the channel config
    ch = None
    with config_lock:
        for c in config.get("channels", []):
            if c["name"] == channel_name or c.get("folder_override", "") == channel_name:
                ch = c
                break
    if not ch:
        return None

    with config_lock:
        base = config.get("output_dir", "").strip() or BASE_DIR
    folder_name = sanitize_folder(ch.get("folder_override", "").strip() or ch["name"])
    channel_dir = os.path.join(base, folder_name)

    # Build candidate paths based on current split settings
    candidates = []
    if ch.get("split_years") and len(upload_date) >= 4:
        year = upload_date[:4]
        if ch.get("split_months") and len(upload_date) >= 6:
            month_num = upload_date[4:6]
            try:
                month_name = datetime.strptime(month_num, "%m").strftime("%B")
            except ValueError:
                month_name = "Unknown Month"
            candidates.append(os.path.join(channel_dir, year, f"{month_num} {month_name}", filename))
        candidates.append(os.path.join(channel_dir, year, filename))

    # Also try flat (no split) in case they removed splits
    candidates.append(os.path.join(channel_dir, filename))

    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate

    # Fuzzy match: try same directories but match by stem (handles slight name differences)
    base_stem = os.path.splitext(filename)[0]
    norm_stem = re.sub(r'\W+', '', base_stem.lower())
    for candidate in candidates:
        try:
            scan_dir = os.path.dirname(candidate)
            if os.path.isdir(scan_dir):
                for e in os.scandir(scan_dir):
                    if e.is_file() and re.sub(r'\W+', '', os.path.splitext(e.name)[0].lower()) == norm_stem:
                        return e.path
        except OSError:
            pass

    return None


def _play_video():
    sel = recent_tree.selection()
    if len(sel) != 1: return
    idx = _get_recent_orig_idx(sel[0])
    with config_lock:
        recent = config.get("recent_downloads", [])
        entry = recent[idx] if idx < len(recent) else {}
        fp = entry.get("filepath", "")

    if not fp or not os.path.exists(fp):
        # Try to find the file by title if filepath is missing or invalid
        found = None
        if fp:
            found = _try_locate_moved_file(entry, fp)
        if not found:
            found = _try_find_by_title(entry)
        if found:
            fp = found
            with config_lock:
                recent = config.get("recent_downloads", [])
                if idx < len(recent):
                    recent[idx]["filepath"] = fp
            save_config(config)
        else:
            log("  ⚠ Could not locate this video file.\n", "red")
            return

    try:
        if os.name == "nt":
            os.startfile(fp)
        elif sys.platform == "darwin":
            subprocess.Popen(["open", fp])
        else:
            subprocess.Popen(["xdg-open", fp])
    except Exception as e:
        log(f"ERROR: Could not play video: {e}\n", "red")


def _show_in_explorer():
    sel = recent_tree.selection()
    if len(sel) != 1: return
    idx = _get_recent_orig_idx(sel[0])
    with config_lock:
        recent = config.get("recent_downloads", [])
        entry = recent[idx] if idx < len(recent) else {}
        fp = entry.get("filepath", "")

    if not fp or not os.path.exists(fp):
        found = None
        if fp:
            # Try original directory with fuzzy name match first
            parent_dir = os.path.dirname(fp)
            base_stem = os.path.splitext(os.path.basename(fp))[0]
            norm_stem = re.sub(r'\W+', '', base_stem.lower())
            try:
                matches = [
                    e for e in os.scandir(parent_dir)
                    if e.is_file() and re.sub(r'\W+', '', os.path.splitext(e.name)[0].lower()) == norm_stem
                ]
                if matches:
                    found = matches[0].path
            except OSError:
                pass
            # Try reconstructing path based on current channel split settings
            if not found:
                found = _try_locate_moved_file(entry, fp)
        # Try finding by title in channel directory
        if not found:
            found = _try_find_by_title(entry)
        if found:
            fp = found
            with config_lock:
                recent = config.get("recent_downloads", [])
                if idx < len(recent):
                    recent[idx]["filepath"] = fp
            save_config(config)
        else:
            # Last resort: open the channel's folder
            ch_dir = _get_channel_dir(entry)
            if ch_dir:
                try:
                    if os.name == "nt":
                        subprocess.Popen(["explorer", ch_dir])
                    elif sys.platform == "darwin":
                        subprocess.Popen(["open", ch_dir])
                    else:
                        subprocess.Popen(["xdg-open", ch_dir])
                    log(f"  ⚠ Could not locate exact file — opened channel folder instead.\n", "red")
                except Exception:
                    log("  ⚠ Could not locate this video file.\n", "red")
            else:
                log("  ⚠ Could not locate this video file.\n", "red")
            return

    try:
        if os.name == "nt":
            subprocess.Popen(f'explorer /select,"{os.path.normpath(fp)}"')
        elif sys.platform == "darwin":
            subprocess.Popen(["open", "-R", fp])
        else:
            subprocess.Popen(["xdg-open", os.path.dirname(fp)])
    except Exception as e:
        log(f"ERROR: Could not open explorer: {e}\n", "red")


def _open_video_on_yt():
    sel = recent_tree.selection()
    if len(sel) != 1: return
    idx = _get_recent_orig_idx(sel[0])
    with config_lock:
        recent = config.get("recent_downloads", [])
        video_url = recent[idx].get("video_url", "") if idx < len(recent) else ""

    if not video_url:
        log("  ⚠ No YouTube URL stored for this entry.\n", "red")
        return
    try:
        import webbrowser
        webbrowser.open(video_url)
    except Exception as e:
        log(f"ERROR: Could not open browser: {e}\n", "red")


_recent_ctx_menu = tk.Menu(root, tearoff=0, bg=C_RAISED, fg=C_TEXT,
                           activebackground=C_BTN_HVR, activeforeground=C_TEXT,
                           bd=0, relief="flat")
_recent_ctx_menu.add_command(label="Play video", command=_play_video)
_recent_ctx_menu.add_command(label="Show in Explorer", command=_show_in_explorer)
_recent_ctx_menu.add_command(label="Open video on YouTube", command=_open_video_on_yt)
_recent_ctx_menu.add_separator()
_recent_ctx_menu.add_command(label="Delete file", command=_delete_selected_files)


def _recent_ctx_show(event):
    row = recent_tree.identify_row(event.y)
    if row:
        if row not in recent_tree.selection():
            recent_tree.selection_set(row)
        sel = recent_tree.selection()
        single = len(sel) == 1
        _recent_ctx_menu.entryconfig("Play video", state="normal" if single else "disabled")
        _recent_ctx_menu.entryconfig("Show in Explorer", state="normal" if single else "disabled")
        _recent_ctx_menu.entryconfig("Open video on YouTube", state="normal" if single else "disabled")
        try:
            _recent_ctx_menu.tk_popup(event.x_root, event.y_root)
        finally:
            _recent_ctx_menu.grab_release()


recent_tree.bind("<Button-3>", _recent_ctx_show)
recent_tree.bind("<<TreeviewSelect>>", _on_recent_select)


def _on_recent_click(event):
    if not recent_tree.identify_row(event.y):
        recent_tree.selection_set([])
        delete_files_btn.config(state="disabled", text="Delete File")


recent_tree.bind("<Button-1>", _on_recent_click, add="+")


def _deselect_recent(event=None):
    recent_tree.selection_set([])
    delete_files_btn.config(state="disabled", text="Delete File")


for _w in [recent_header] + [w for w in recent_header.winfo_children()
                             if w not in (delete_files_btn,)]:
    _w.bind("<Button-1>", _deselect_recent, add="+")
delete_files_btn.config(command=_delete_selected_files)


def record_download(title, channel, date, size_bytes="", duration_s="", filepath="", video_url=""):
    def _delayed_record():
        final_size = size_bytes
        # Give ffmpeg a brief moment to finish writing the file to disk to fetch the exact final byte count
        for _ in range(6):
            if filepath and os.path.exists(filepath):
                try:
                    disk_size = os.path.getsize(filepath)
                    if disk_size > 0:
                        final_size = str(disk_size)
                        break
                except OSError:
                    pass
            time.sleep(0.5)

        def _sync():
            global new_download_count
            if not ('recent_tree' in globals() and recent_tree.winfo_exists()): return
            with config_lock:
                recent = config.setdefault("recent_downloads", [])
                recent.insert(0, {"title": title, "channel": channel, "date": date,
                                  "size": final_size, "duration": duration_s, "filepath": filepath,
                                  "video_url": video_url, "download_ts": time.time()})
                if len(recent) > RECENT_MAX: config["recent_downloads"] = recent[:RECENT_MAX]
            save_config(config)
            refresh_recent_list()

            if notebook.select() != str(tab_recent):
                new_download_count += 1
                notebook.tab(tab_recent, text=f"  Recent ({new_download_count})  ")
                _update_tray_badge()

        if _root_alive: _ui_queue.append(_sync)

    threading.Thread(target=_delayed_record, daemon=True).start()


refresh_channel_dropdowns()
refresh_recent_list()


def check_dependencies():
    global HAS_TRAY
    missing = []
    yt_bin = "yt-dlp.exe" if os.name == 'nt' else "yt-dlp"
    ff_bin = "ffmpeg.exe" if os.name == 'nt' else "ffmpeg"
    if not (shutil.which("yt-dlp") or os.path.exists(os.path.join(BASE_DIR, yt_bin))): missing.append("yt-dlp")
    if not (shutil.which("ffmpeg") or os.path.exists(os.path.join(BASE_DIR, ff_bin))): missing.append("ffmpeg")

    # Auto-install tray icon dependencies if missing
    if not HAS_TRAY:
        tray_pkgs = []
        try:
            import pystray
        except ImportError:
            tray_pkgs.append("pystray")
        try:
            from PIL import Image
        except ImportError:
            tray_pkgs.append("Pillow")
        if tray_pkgs:
            def _install_tray():
                global HAS_TRAY
                for pkg in tray_pkgs:
                    try:
                        subprocess.run(
                            [sys.executable, "-m", "pip", "install", pkg],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            encoding="utf-8", errors="replace",
                            startupinfo=startupinfo
                        )
                    except Exception:
                        pass
                try:
                    import pystray as _pt
                    from PIL import Image as _img, ImageDraw as _drw
                    globals()["pystray"] = _pt
                    globals()["Image"] = _img
                    globals()["ImageDraw"] = _drw
                    HAS_TRAY = True
                    if _root_alive:
                        _ui_queue.append(_setup_tray_icon)
                except ImportError:
                    pass
            threading.Thread(target=_install_tray, daemon=True).start()

    if missing:
        names = ' and '.join(missing)
        if messagebox.askyesno("Missing Dependencies",
                               f"{names} not found.\n\n"
                               f"Would you like to try installing {'them' if len(missing) > 1 else 'it'} automatically via pip?\n\n"
                               f"(You can also install manually and restart)"):
            def _install():
                for pkg in missing:
                    log(f"--- Installing {pkg}... ---\n", "header")
                    try:
                        proc = subprocess.run(
                            [sys.executable, "-m", "pip", "install", pkg],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            encoding="utf-8", errors="replace",
                            startupinfo=startupinfo
                        )
                        if proc.returncode == 0:
                            log(f"  ✓ {pkg} installed successfully.\n", "green")
                        else:
                            log(f"  ⚠ Failed to install {pkg}. Install it manually.\n", "red")
                    except Exception as e:
                        log(f"  ⚠ Error installing {pkg}: {e}\n", "red")
                log("--- Dependency install complete. Restart the app if needed. ---\n", "simpleline_green")
            threading.Thread(target=_install, daemon=True).start()
        else:
            messagebox.showwarning("Missing", f"{names} not found. The app cannot download videos without {'them' if len(missing) > 1 else 'it'}.")


root.after(100, check_dependencies)


def _check_channel_folders():
    """Check that each subscribed channel's directory still exists on disk.
    If a folder is missing, prompt the user to remove the channel or locate the folder."""
    with config_lock:
        base = config.get("output_dir", "").strip() or BASE_DIR
        channels = list(config.get("channels", []))
    if not channels:
        return

    missing = []
    for ch in channels:
        folder_name = sanitize_folder(ch.get("folder_override", "").strip() or ch["name"])
        folder_path = os.path.join(base, folder_name)
        if not os.path.isdir(folder_path):
            # Only flag channels that have been initialized (new channels won't have folders yet)
            if ch.get("initialized", False):
                missing.append((ch["name"], ch["url"], folder_path))

    if not missing:
        return

    import queue as _q
    result_q = _q.Queue()

    def _prompt_missing():
        for ch_name, ch_url, expected_path in missing:
            answer = messagebox.askyesnocancel(
                "Missing Channel Folder",
                f"Cannot locate folder for \"{ch_name}\".\n\n"
                f"Expected: {expected_path}\n\n"
                "• Yes — Remove from Sub list\n"
                "• No — Browse for the folder\n"
                "• Cancel — Ignore for now",
                icon="warning"
            )
            if answer is True:
                # Remove from sub list
                with config_lock:
                    config["channels"] = [c for c in config.get("channels", []) if c["url"] != ch_url]
                save_config(config)
                log(f"  Removed \"{ch_name}\" from sub list (folder missing).\n", "dim")
            elif answer is False:
                # Browse for folder
                new_path = filedialog.askdirectory(
                    title=f"Locate folder for \"{ch_name}\"",
                    initialdir=base
                )
                if new_path and os.path.isdir(new_path):
                    # Validate the folder is inside the parent output directory
                    norm_new = os.path.normpath(new_path)
                    norm_base = os.path.normpath(base)
                    if os.path.dirname(norm_new) == norm_base:
                        new_folder_name = os.path.basename(norm_new)
                        with config_lock:
                            for cfg_ch in config.get("channels", []):
                                if cfg_ch["url"] == ch_url:
                                    cfg_ch["folder_override"] = new_folder_name
                                    break
                        save_config(config)
                        log(f"  Updated folder for \"{ch_name}\" → {new_folder_name}\n", "green")
                    else:
                        messagebox.showwarning(
                            "Invalid Location",
                            f"The selected folder must be inside your output directory:\n{base}"
                        )
            # else: Cancel — do nothing
        result_q.put(True)
        if root.winfo_exists():
            root.after(0, refresh_channel_dropdowns)

    if _root_alive:
        _ui_queue.append(_prompt_missing)
        # Wait for the dialogs to complete before continuing startup
        try:
            result_q.get(timeout=300)
        except _q.Empty:
            pass


def run_startup_updates():
    def _download_yt_dlp_binary(target_path):
        """Download the latest yt-dlp binary from GitHub to target_path."""
        yt_name = "yt-dlp.exe" if os.name == 'nt' else "yt-dlp"
        dl_url = f"https://github.com/yt-dlp/yt-dlp/releases/latest/download/{yt_name}"
        import urllib.request
        urllib.request.urlretrieve(dl_url, target_path)

    def _get_yt_dlp_version():
        """Get the current yt-dlp version string."""
        try:
            r = subprocess.run(
                ["yt-dlp", "--version"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                encoding="utf-8", errors="replace",
                startupinfo=startupinfo
            )
            return r.stdout.strip() if r.stdout else "unknown"
        except Exception:
            return "unknown"

    def _update():
        log("--- Checking for yt-dlp updates... ---\n", "header")
        try:
            current_ver = _get_yt_dlp_version()
            log(f"Current version: {current_ver}\n", "dim")

            # Try yt-dlp -U for self-update and parse latest version
            proc = subprocess.Popen(
                ["yt-dlp", "-U"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                encoding="utf-8", errors="replace",
                startupinfo=startupinfo
            )
            update_output = []
            latest_ver = None
            for line in proc.stdout:
                line = line.rstrip("\n") + "\n"
                update_output.append(line)
                # Parse latest version from lines like "Latest version: stable@2026.03.03 from ..."
                m = re.search(r'(?:Latest|Newest)\s+version:\s*\S*@?(\d{4}\.\d{2}\.\d{2})', line, re.IGNORECASE)
                if m:
                    latest_ver = m.group(1)
                ll = line.lower()
                if "installed yt-dlp with pip" in ll or "use that to update" in ll:
                    continue
                if any(x in line for x in ["ERROR:", "Error:"]):
                    log(line, "red")
                elif any(x in line for x in ["up to date", "Updated", "yt-dlp"]):
                    log(line, "green")
                else:
                    log(line, "dim")
            proc.wait()

            full_output = "".join(update_output).lower()

            # If already up to date, skip everything
            if "up to date" in full_output:
                pass
            elif proc.returncode != 0 or "error" in full_output or "can't update" in full_output:
                # yt-dlp -U failed — try alternative update methods
                if "pip" in full_output or "pypi" in full_output:
                    log("  Updating yt-dlp via pip...\n", "dim")
                    try:
                        pip_proc = subprocess.run(
                            [sys.executable, "-m", "pip", "install", "-U", "yt-dlp"],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            encoding="utf-8", errors="replace",
                            startupinfo=startupinfo
                        )
                        if pip_proc.returncode == 0:
                            log("  ✓ yt-dlp updated via pip.\n", "green")
                        else:
                            log(f"  ⚠ pip update failed (exit {pip_proc.returncode}).\n", "red")
                    except Exception as e:
                        log(f"  ⚠ Could not update via pip: {e}\n", "red")
                elif getattr(sys, 'frozen', False):
                    log("  Bundled yt-dlp can't self-update, downloading latest...\n", "dim")
                    try:
                        yt_name = "yt-dlp.exe" if os.name == 'nt' else "yt-dlp"
                        target = os.path.join(BASE_DIR, yt_name)
                        _download_yt_dlp_binary(target)
                        log(f"  ✓ Updated yt-dlp binary in {BASE_DIR}\n", "green")
                    except Exception as e:
                        log(f"  ⚠ Could not download yt-dlp update: {e}\n", "red")

            # Verify update actually took effect
            if latest_ver and current_ver != "unknown":
                new_ver = _get_yt_dlp_version()
                if new_ver == current_ver and new_ver != latest_ver:
                    # Update didn't take effect — a stale binary is shadowing the pip install
                    yt_name = "yt-dlp.exe" if os.name == 'nt' else "yt-dlp"
                    stale_path = shutil.which("yt-dlp")
                    # Also check common local locations
                    local_bin = os.path.join(BASE_DIR, yt_name)
                    resource_bin = os.path.join(RESOURCE_PATH, yt_name) if RESOURCE_PATH != BASE_DIR else None
                    target = stale_path or local_bin
                    log(f"  ⚠ Version still {new_ver} after update — stale binary detected.\n", "dim")
                    log(f"  Downloading latest yt-dlp to {target}...\n", "dim")
                    try:
                        _download_yt_dlp_binary(target)
                        # Also update resource path copy if separate
                        if resource_bin and os.path.exists(resource_bin) and resource_bin != target:
                            _download_yt_dlp_binary(resource_bin)
                        final_ver = _get_yt_dlp_version()
                        if final_ver != current_ver:
                            log(f"  ✓ yt-dlp updated: {current_ver} → {final_ver}\n", "green")
                        else:
                            log(f"  ⚠ Could not update yt-dlp. Try manually replacing yt-dlp.exe.\n", "red")
                    except Exception as e:
                        log(f"  ⚠ Could not download yt-dlp binary: {e}\n", "red")

        except FileNotFoundError:
            log("yt-dlp not found — skipping update check.\n", "red")

        log("--- Checking ffmpeg is installed... ---\n", "header")
        try:
            result = subprocess.run(
                ["ffmpeg", "-version"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                encoding="utf-8", errors="replace",
                startupinfo=startupinfo
            )
            first_line = result.stdout.splitlines()[0] if result.stdout else "unknown"
            log(f"ffmpeg: {first_line}\n", "green")
        except FileNotFoundError:
            log("ffmpeg not found — skipping version check.\n", "red")

        # Check that each subscribed channel's folder still exists
        _check_channel_folders()

        # Clean up any leftover partial/temp files from interrupted downloads
        _startup_cleanup_temps()

        log("--- Startup checks complete, ready to download ---\n", "simpleline_green")
        _stop_startup_loading()

        # Enable sync button now that startup is complete
        if _root_alive:
            _ui_queue.append(lambda: sync_btn.config(state="normal"))

        # Restore preserved queue from previous session if any
        _load_queue_state()

    threading.Thread(target=_update, daemon=True).start()


root.after(200, run_startup_updates)
root.after(50, _start_startup_loading)


def _save_queue_state():
    """Save current queue state to disk for restoration on next launch."""
    queue_data = {"sync": [], "reorg": [], "video": [], "transcribe": [], "order": [], "gpu": []}
    # Include the currently-running sync channel so it survives crashes
    if _current_sync_ch is not None and _sync_running:
        with _sync_queue_lock:
            if not any(q["url"] == _current_sync_ch["url"] for q in _sync_queue):
                queue_data["sync"].append(copy.deepcopy(_current_sync_ch))
    with _sync_queue_lock:
        for ch in _sync_queue:
            queue_data["sync"].append(copy.deepcopy(ch))
    with _reorg_queue_lock:
        for args in _reorg_queue:
            queue_data["reorg"].append(list(args))
    with _transcribe_queue_lock:
        for args in _transcribe_queue:
            queue_data["transcribe"].append(list(args))
    with _video_dl_queue_lock:
        # Video download commands contain subprocess args — skip them (not easily restorable)
        pass
    # Include the currently-running GPU item so it survives crashes
    if _gpu_current_item is not None:
        with _gpu_queue_lock:
            queue_data["gpu"].append(copy.deepcopy(_gpu_current_item))
    with _gpu_queue_lock:
        for item in _gpu_queue:
            queue_data["gpu"].append(copy.deepcopy(item))
    queue_data["gpu_paused"] = _gpu_pause.is_set()
    with _queue_order_lock:
        saved_order = list(_queue_order)
    # Prepend the currently-running item's order entry (it was removed from
    # _queue_order when it started running, so it would otherwise become an
    # orphan on load and appear AFTER queued items instead of before them)
    if _current_sync_ch is not None and _sync_running:
        saved_order.insert(0, ("sync", _current_sync_ch["url"]))
    queue_data["order"] = saved_order
    try:
        with open(QUEUE_FILE, "w", encoding="utf-8") as f:
            json.dump(queue_data, f, indent=2)
    except Exception:
        pass


def _load_queue_state():
    """Load preserved queue state from disk and populate queues."""
    try:
        if not os.path.exists(QUEUE_FILE):
            return False
        with open(QUEUE_FILE, "r", encoding="utf-8") as f:
            queue_data = json.load(f)
        os.remove(QUEUE_FILE)  # Consume the file

        restored = 0
        sync_items = queue_data.get("sync", [])
        if sync_items:
            with _sync_queue_lock:
                for ch in sync_items:
                    if not any(q["url"] == ch["url"] for q in _sync_queue):
                        _sync_queue.append(ch)
                        restored += 1
        reorg_items = queue_data.get("reorg", [])
        if reorg_items:
            with _reorg_queue_lock:
                for args in reorg_items:
                    _reorg_queue.append(tuple(args))
                    restored += 1
        transcribe_items = queue_data.get("transcribe", [])
        if transcribe_items:
            with _transcribe_queue_lock:
                for args in transcribe_items:
                    _transcribe_queue.append(tuple(args))
                    restored += 1
        gpu_items = queue_data.get("gpu", [])
        gpu_restored = 0
        if gpu_items:
            with _gpu_queue_lock:
                for item in gpu_items:
                    _gpu_queue.append(item)
                    restored += 1
                    gpu_restored += 1
        # Restore GPU pause state
        if queue_data.get("gpu_paused", False) and gpu_restored > 0:
            _gpu_pause.set()
        # Restore unified queue ordering
        saved_order = queue_data.get("order", [])
        if saved_order:
            with _queue_order_lock:
                for entry in saved_order:
                    if isinstance(entry, (list, tuple)) and len(entry) == 2:
                        _queue_order.append(tuple(entry))
        if restored:
            log(f"Restored {restored} job(s) from previous session.\n", "simpleline_green")
            if gpu_restored:
                _pause_str = " (paused)" if _gpu_pause.is_set() else ""
                log(f"  💻 {gpu_restored} GPU task(s) restored{_pause_str}.\n", "simpleline_green")
            _update_queue_btn()
            _update_gpu_btn()
            return True
    except Exception:
        pass
    return False


def on_closing():
    global _root_alive
    # Save window position and size before closing
    try:
        geo = root.geometry()  # e.g. "900x800+100+200"
        _wh, _pos = geo.split("+", 1)
        _w, _h = _wh.split("x")
        _px, _py = _pos.split("+")
        config["window_w"] = int(_w)
        config["window_h"] = int(_h)
        config["window_x"] = int(_px)
        config["window_y"] = int(_py)
        save_config(config)
    except Exception:
        pass
    _root_alive = False  # Tell worker threads to stop touching Tkinter immediately

    # Check if there are queued jobs
    has_queue = False
    with _sync_queue_lock:
        if _sync_queue:
            has_queue = True
    if not has_queue:
        with _reorg_queue_lock:
            if _reorg_queue:
                has_queue = True
    if not has_queue:
        with _transcribe_queue_lock:
            if _transcribe_queue:
                has_queue = True
    if not has_queue:
        with _gpu_queue_lock:
            if _gpu_queue:
                has_queue = True
    if not has_queue and _gpu_running:
        has_queue = True

    # Include currently-running items in has_queue check
    if _current_sync_ch is not None and _sync_running:
        has_queue = True
    if _gpu_current_item is not None:
        has_queue = True

    if has_queue:
        _save_queue_state()

    # Stop the system tray icon
    global _tray_spin_active
    _tray_spin_active = False
    _tray_spin_stop_ev.set()  # signal spin thread to exit
    if _tray_icon is not None:
        try:
            _tray_icon.stop()
        except Exception:
            pass

    # Stop Whisper/punctuation/ffmpeg subprocesses if running
    _stop_whisper_process()
    _stop_punct_process()
    _stop_ffmpeg_process()

    # Clear pause so worker threads can exit cleanly
    pause_event.clear()
    cancel_event.set()

    with proc_lock:
        procs = list(active_processes)
    for p in procs:
        if p.poll() is None:
            if os.name == 'nt':
                subprocess.Popen(['taskkill', '/F', '/T', '/PID', str(p.pid)],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                 startupinfo=startupinfo)
            else:
                p.kill()

    # Give worker threads a moment to notice cancel_event and save state
    # (they're daemon threads — root.destroy() will kill them)
    time.sleep(1.0)
    root.destroy()


root.protocol("WM_DELETE_WINDOW", on_closing)


def _defocus_on_click(e):
    widget = e.widget

    if isinstance(widget, str):
        return
    # Don't steal focus from popup/toplevel windows (e.g., job queue)
    try:
        if widget.winfo_toplevel() != root:
            return
    except Exception:
        pass
    _skip_classes = {
        "Entry", "TEntry", "Text", "TCombobox",
        "Listbox", "Treeview",
        "Button", "TButton", "TCheckbutton",
    }
    if widget.winfo_class() not in _skip_classes:
        root.focus_set()


root.bind_all("<Button-1>", _defocus_on_click, add="+")

root.mainloop()
