"""
auto_backup.py — scheduled full-state backups into the archive root,
plus the self-describing "YTArchiver Info" folder (v80).

The point: the archive on disk should outlive the program AND the
program's PC. This module maintains `<archive root>\\YTArchiver Info\\`:

  ABOUT THIS ARCHIVE.txt        — human-readable documentation of every
                                  file convention in the archive, so a
                                  future reader can parse it without the
                                  app or its source code.
  YTArchiver.exe                — a copy of the running app (frozen
                                  builds only), so the tool that reads
                                  the archive lives WITH the archive.
  ytarchiver_backup_<ts>.zip    — the same full app-state export the
                                  Health tab offers (config, subs,
                                  queue, downloaded-IDs list, filters,
                                  channel-ID cache), written on a
                                  schedule. Newest KEEP_BACKUPS stay;
                                  older ones move to the archive's
                                  hidden trash — nothing is deleted.

`build_backup_zip` is the single zip-writing core — the Health tab's
manual "Export" (api_mixins/backup_mixin.py) calls it too, so the two
paths can't drift.

Scheduling: `AutoBackupScheduler` is a daemon thread started from
main.py. Every 15 minutes it checks whether a backup is due per the
`auto_backup_interval` config key ("off" | "daily" | "weekly" |
"monthly") and runs one when it is. An unreachable archive drive just
postpones to the next tick.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import threading
import time
from datetime import datetime

from .log import get_logger, swallow
from .utils import hide_file_win
from .ytarchiver_config import (
    APP_DATA_DIR,
    ARCHIVE_FILE,
    CHANNEL_ID_CACHE,
    CONFIG_FILE,
    DISK_CACHE_FILE,
    QUEUE_FILE,
    SEEN_FILTER_TITLES,
    TRANSCRIPTION_DB,
    config_transaction,
    load_config,
)

_log = get_logger(__name__)

INFO_DIR_NAME = "YTArchiver Info"
ABOUT_NAME = "ABOUT THIS ARCHIVE.txt"
EXE_COPY_NAME = "YTArchiver.exe"
BACKUP_PREFIX = "ytarchiver_backup_"
KEEP_BACKUPS = 4

BACKUP_MANIFEST_NAME = "ytarchiver_backup_manifest.json"

# FTS index DB rides along only when small enough for ZIP deflate to
# stay reasonable (see backup_mixin's original rationale).
_FTS_ZIP_CAP = 2 * 1024 * 1024 * 1024

_INTERVAL_SECS = {
    "daily": 24 * 3600,
    "weekly": 7 * 24 * 3600,
    "monthly": 30 * 24 * 3600,
}


def backup_file_entries():
    """(arcname, Path) pairs for every app-state file worth backing up.

    v80 adds the yt-dlp download archive (the downloaded-video-IDs
    list) and the livestream drawer/ignore journals — previously a
    "full backup" restore forgot which videos were already downloaded.
    """
    return (
        (CONFIG_FILE.name, CONFIG_FILE),
        (QUEUE_FILE.name, QUEUE_FILE),
        (DISK_CACHE_FILE.name, DISK_CACHE_FILE),
        (SEEN_FILTER_TITLES.name, SEEN_FILTER_TITLES),
        (CHANNEL_ID_CACHE.name, CHANNEL_ID_CACHE),
        (ARCHIVE_FILE.name, ARCHIVE_FILE),
        ("ytarchiver_livestream_defer.json",
         APP_DATA_DIR / "ytarchiver_livestream_defer.json"),
        ("ytarchiver_livestream_drawer.json",
         APP_DATA_DIR / "ytarchiver_livestream_drawer.json"),
        ("ytarchiver_livestream_ignore.json",
         APP_DATA_DIR / "ytarchiver_livestream_ignore.json"),
        ("ytarchiver_pending_transcribe.json",
         APP_DATA_DIR / "ytarchiver_pending_transcribe.json"),
    )


def build_backup_zip(out_path: str) -> dict:
    """Write the full app-state backup ZIP to `out_path` (atomic via
    .tmp + os.replace). Returns {"files", "fts_included", "fts_size",
    "fts_skipped_reason"}. Raises on failure (tmp cleaned up).

    Lifted from backup_mixin.export_full_backup so the manual export
    and the scheduled auto-backup share one implementation.
    """
    import zipfile as _zf

    fts_skipped_reason = ""
    fts_size = 0
    include_fts = False
    try:
        if TRANSCRIPTION_DB.exists():
            fts_size = int(TRANSCRIPTION_DB.stat().st_size)
            if fts_size < _FTS_ZIP_CAP:
                include_fts = True
            else:
                fts_skipped_reason = (
                    f"FTS DB skipped — too large "
                    f"({fts_size / (1024**3):.1f} GB > 2 GB). "
                    f"Back up manually if needed.")
    except OSError:
        pass

    n = 0
    tmp_path = out_path + ".tmp"
    try:
        with _zf.ZipFile(tmp_path, "w", _zf.ZIP_DEFLATED) as zf:
            for arcname, p in backup_file_entries():
                if p.exists():
                    zf.write(str(p), arcname=arcname)
                    n += 1
            if include_fts:
                # Consistent point-in-time snapshot of the live
                # WAL-mode DB via sqlite3's backup API — a raw file
                # copy can tear mid-checkpoint.
                import sqlite3 as _sq3
                import tempfile as _tf
                _fd, _snap = _tf.mkstemp(suffix=".db")
                os.close(_fd)
                try:
                    _src = _sq3.connect(
                        f"file:{TRANSCRIPTION_DB}?mode=ro",
                        uri=True, timeout=60)
                    try:
                        _dst = _sq3.connect(_snap)
                        try:
                            _src.backup(_dst)
                        finally:
                            _dst.close()
                    finally:
                        _src.close()
                    zf.write(_snap, arcname=TRANSCRIPTION_DB.name)
                    n += 1
                finally:
                    try: os.remove(_snap)
                    except OSError: pass
            zf.writestr(BACKUP_MANIFEST_NAME, json.dumps({
                "app": "YTArchiver",
                "backup_type": "app-state",
                "fts_db_included": bool(include_fts),
                "fts_db_size": fts_size,
                "fts_skipped_reason": fts_skipped_reason,
            }, indent=2, sort_keys=True))
            n += 1
            backup_dir = APP_DATA_DIR / "backups"
            if backup_dir.is_dir():
                snaps = sorted(backup_dir.glob("config_*.json"),
                               key=lambda pp: pp.stat().st_mtime,
                               reverse=True)
                if snaps:
                    zf.write(str(snaps[0]),
                             arcname=f"backups/{snaps[0].name}")
                    n += 1
        os.replace(tmp_path, out_path)
    except Exception:
        try: os.remove(tmp_path)
        except OSError: pass
        raise
    return {"files": n, "fts_included": include_fts, "fts_size": fts_size,
            "fts_skipped_reason": fts_skipped_reason}


# ─── ABOUT THIS ARCHIVE.txt ────────────────────────────────────────────

def _about_text() -> str:
    try:
        from .version import APP_VERSION
    except Exception:
        APP_VERSION = "?"
    today = datetime.now().strftime("%Y-%m-%d")
    return f"""ABOUT THIS ARCHIVE
==================

This folder tree is a YouTube channel archive created by YTArchiver.
Everything here is designed to be readable WITHOUT the program — this
file documents every convention so the archive stays fully usable even
if the app is long gone.

WHAT'S IN EACH CHANNEL FOLDER
-----------------------------
Visible files (what you see in Explorer):
  *.mp4                          The videos. Filenames are the video
                                 titles (sanitized for Windows).
  <Channel> Transcript.txt       All transcripts for the channel (or
                                 for that year/month subfolder), one
                                 block per video. See TRANSCRIPTS below.

Folder layout varies per channel: flat (all videos in the channel
folder), yearly (\\2024\\...), or monthly (\\2024\\03 March\\...).
Transcript + metadata sidecars follow the same split.

FILE DATES MATTER
-----------------
Each video file's "Date modified" IS its original YouTube upload date.
When copying or backing up this archive, use tools that preserve file
modification times. (Since v80, the upload date is ALSO embedded inside
each MP4's metadata tags — see EMBEDDED TAGS below — so a lost mtime is
recoverable.)

HIDDEN SIDECARS (enable "Show hidden files" in Explorer to see them)
--------------------------------------------------------------------
  .<Channel> Metadata.jsonl      One JSON record per line per video:
                                 video_id, title, description,
                                 view_count, like_count, comment_count,
                                 upload_date (YYYYMMDD), duration
                                 (seconds), thumbnail_url, comments
                                 (list), fetched_at (when the counts
                                 were snapshotted).
  .<Channel> Transcript.jsonl    Machine-readable transcript: one JSON
                                 record per spoken segment — video_id,
                                 title, start, end (seconds), text, and
                                 words = [{{w, s, e}}] per-word timings.
  .Thumbnails\\                   <Title> [<videoID>].jpg — one
                                 thumbnail per video. The [bracketed]
                                 part is the YouTube video ID.
  .ChannelArt\\                   avatar.jpg / banner.jpg for the
                                 channel page.
  .YTArchiver Trash\\             (archive root) Files removed via the
                                 app are quarantined here, never
                                 hard-deleted.

TRANSCRIPTS
-----------
Each video's block in <Channel> Transcript.txt starts with a header:

  ===(Title), (MM.DD.YYYY), (H:MM:SS), (SOURCE), (youtu.be/<videoID>)===

followed by the full transcript text. SOURCE is how the transcript was
made: YT / YT+PUNCTUATION (YouTube captions, optionally punctuation-
restored) or WHISPER:<model> (local speech-to-text). Older entries may
lack the final (youtu.be/...) field — the video ID for those can be
found in the .jsonl sidecars or the .Thumbnails filenames.

EMBEDDED TAGS (v80+)
--------------------
Every MP4 also carries its identity INSIDE the file container (visible
in Explorer's Properties > Details, VLC, or ffprobe):
  Title    = real video title      Artist  = channel name
  Date     = upload date (ISO)     Comment = original YouTube URL
So even a renamed, moved, or date-stripped file can always be traced
back to its source video.

THIS FOLDER ("{INFO_DIR_NAME}")
-------------------------------
  {ABOUT_NAME}   — this file (regenerated automatically).
  {EXE_COPY_NAME}            — a copy of the app itself. Run it on any
                             Windows PC to browse/search this archive.
  {BACKUP_PREFIX}*.zip — snapshots of the app's state:
                             settings, channel subscriptions, the
                             downloaded-video-ID list, filters, queue.
                             To restore: run the app, Health tab >
                             Backup and Migration > Import. The newest
                             {KEEP_BACKUPS} are kept; older ones move to the
                             archive trash.

Generated by YTArchiver v{APP_VERSION} on {today}. Format: v2.
"""


def refresh_info_folder(output_dir: str) -> str:
    """Create/refresh `<output_dir>\\YTArchiver Info\\` (ABOUT file +
    exe copy). Returns the folder path. Raises OSError only if the
    folder itself can't be created — the exe copy is best-effort."""
    info_dir = os.path.join(output_dir, INFO_DIR_NAME)
    os.makedirs(info_dir, exist_ok=True)

    # ABOUT file — atomic rewrite only when content changed.
    about_path = os.path.join(info_dir, ABOUT_NAME)
    text = _about_text()
    try:
        with open(about_path, "r", encoding="utf-8") as f:
            unchanged = f.read() == text
    except OSError:
        unchanged = False
    if not unchanged:
        tmp = about_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(text)
            os.replace(tmp, about_path)
        except OSError as e:
            try: os.remove(tmp)
            except OSError: pass
            swallow("auto_backup about write", e)

    # Exe copy — frozen builds only (a dev run's python.exe is useless
    # to a future reader). Copied when size or mtime differs.
    try:
        if getattr(sys, "frozen", False):
            src = sys.executable
            dst = os.path.join(info_dir, EXE_COPY_NAME)
            s_st = os.stat(src)
            try:
                d_st = os.stat(dst)
                differs = (d_st.st_size != s_st.st_size
                           or int(d_st.st_mtime) != int(s_st.st_mtime))
            except OSError:
                differs = True
            if differs:
                shutil.copy2(src, dst)
    except OSError as e:
        swallow("auto_backup exe copy", e)
    return info_dir


def _rotate_backups(info_dir: str, output_dir: str) -> int:
    """Keep the newest KEEP_BACKUPS zips; MOVE older ones into the
    archive's hidden trash (never delete). Returns how many moved."""
    try:
        zips = sorted(
            (e for e in os.scandir(info_dir)
             if e.is_file() and e.name.startswith(BACKUP_PREFIX)
             and e.name.endswith(".zip")),
            key=lambda e: e.stat().st_mtime, reverse=True)
    except OSError:
        return 0
    moved = 0
    for e in zips[KEEP_BACKUPS:]:
        trash_dir = os.path.join(output_dir, ".YTArchiver Trash",
                                 INFO_DIR_NAME)
        try:
            os.makedirs(trash_dir, exist_ok=True)
            try:
                hide_file_win(os.path.join(output_dir, ".YTArchiver Trash"))
            except Exception:
                pass
            dest = os.path.join(trash_dir, e.name)
            if os.path.exists(dest):
                stem, ext = os.path.splitext(e.name)
                dest = os.path.join(trash_dir,
                                    f"{stem}.{int(time.time())}{ext}")
            shutil.move(e.path, dest)
            moved += 1
        except OSError as ex:
            swallow("auto_backup rotate", ex)
    return moved


def run_backup(output_dir: str, *, mark_auto: bool = True) -> dict:
    """Refresh the info folder and write one backup zip into it.
    Returns {"ok", "path", "files", "fts_skipped_reason"} or
    {"ok": False, "error"}. Updates last_backup_ts (+
    last_auto_backup_ts when mark_auto) on success."""
    output_dir = os.path.normpath((output_dir or "").strip())
    if not output_dir or not os.path.isdir(output_dir):
        return {"ok": False, "error": f"archive root unavailable: "
                                      f"{output_dir!r}"}
    try:
        info_dir = refresh_info_folder(output_dir)
    except OSError as e:
        return {"ok": False, "error": f"cannot create info folder: {e}"}
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    out_path = os.path.join(info_dir, f"{BACKUP_PREFIX}{ts}.zip")
    try:
        stats = build_backup_zip(out_path)
    except Exception as e:
        return {"ok": False, "error": f"backup zip failed: {e}"}
    _rotate_backups(info_dir, output_dir)
    now = time.time()
    try:
        with config_transaction() as cfg:
            cfg["last_backup_ts"] = now
            if mark_auto:
                cfg["last_auto_backup_ts"] = now
    except Exception as e:
        swallow("auto_backup ts save", e)
    return {"ok": True, "path": out_path, "files": stats["files"],
            "fts_skipped_reason": stats["fts_skipped_reason"]}


# ─── Scheduler ─────────────────────────────────────────────────────────

class AutoBackupScheduler:
    """Daemon thread: run a backup when `auto_backup_interval` says one
    is due. Ticks every 15 min; first check ~2 min after boot so it
    never competes with startup work."""

    _FIRST_TICK_S = 120
    _TICK_S = 900
    _FAIL_EMIT_COOLDOWN_S = 6 * 3600

    def __init__(self, stream=None):
        self._stream = stream
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_fail_emit = 0.0

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop, name="auto-backup", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    # internals ---------------------------------------------------------

    def _emit(self, parts):
        if self._stream is None:
            return
        try:
            self._stream.emit(parts)
        except Exception as e:
            swallow("auto_backup emit", e)

    def _loop(self):
        if self._stop.wait(self._FIRST_TICK_S):
            return
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception as e:
                _log.error("auto-backup tick failed: %s", e)
            if self._stop.wait(self._TICK_S):
                return

    def _tick(self):
        cfg = load_config()
        mode = (cfg.get("auto_backup_interval") or "off").strip().lower()
        secs = _INTERVAL_SECS.get(mode)
        if not secs:
            return
        last = float(cfg.get("last_auto_backup_ts", 0) or 0)
        # 2% slack so a daily backup doesn't creep later every day by
        # one tick interval.
        if time.time() - last < secs * 0.98:
            return
        output_dir = (cfg.get("output_dir") or "").strip()
        if not output_dir or not os.path.isdir(output_dir):
            # Archive drive offline/unmounted — quietly retry next tick.
            _log.debug("auto-backup due but archive root unavailable")
            return
        res = run_backup(output_dir)
        if res.get("ok"):
            _log.info("auto-backup written: %s", res.get("path"))
            self._emit([
                ["\U0001f4be ", "dim"],
                [f"Auto-backup saved ({res.get('files', 0)} files) → ",
                 "dim"],
                [f"{res.get('path')}\n", "dim"],
            ])
        else:
            _log.error("auto-backup failed: %s", res.get("error"))
            now = time.time()
            if now - self._last_fail_emit >= self._FAIL_EMIT_COOLDOWN_S:
                self._last_fail_emit = now
                self._emit([
                    ["⚠ ", "red"],
                    [f"Auto-backup failed: {res.get('error')}\n", "red"],
                ])
