"""Local duration probing and backfill, independent of metadata refresh."""

from __future__ import annotations

import subprocess
import threading
import time

from ..executor_utils import WorkResult, run_bounded
from ..log import get_logger
from ..log_stream import LogStreamer
from ..subprocess_util import make_startupinfo
from ..utils import utf8_subprocess_env as _utf8_env

_log = get_logger(__name__)
_startupinfo = make_startupinfo()

def _probe_file_duration(filepath: str) -> float | None:
    """Single-file ffprobe call returning duration in seconds. None on
    any error. Used by _probe_durations_bulk to fill `videos.duration_s`
    for files that came from the tkinter-era importer (which never
    probed duration, leaving NULL across the board).
    """
    try:
        proc = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries",
             "format=duration", "-of",
             "default=noprint_wrappers=1:nokey=1", filepath],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            startupinfo=_startupinfo, env=_utf8_env(),
            timeout=10, encoding="utf-8", errors="replace")
    except Exception:
        return None
    raw = (proc.stdout or "").strip()
    try:
        v = float(raw)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None

def _probe_durations_bulk(filepaths: list[str], stream: LogStreamer,
                          cancel_event: threading.Event | None = None,
                          pause_event: threading.Event | None = None,
                          max_workers: int = 6,
                          ) -> dict[str, float | None]:
    """Probe duration for a batch of files in parallel.

    Reason this exists: backfill_video_ids' duration-match strategy
    needs `local_dur` to disambiguate same-day same-title YT
    candidates. The tkinter-era importer never populated
    `videos.duration_s`, so on migrated archives every duration is
    NULL — strategies that compare against duration get zero data
    and fail silently. This helper fills the gap with one ffprobe
    call per file, ~70ms each, parallelized 6-wide → ~12s for 1000
    files. Results write back to `videos.duration_s` so subsequent
    runs skip the probe entirely (the SELECT in the caller pulls
    them out of the DB).
    """
    out: dict[str, float | None] = {}
    if not filepaths:
        return out
    _t0 = time.time()
    _total = len(filepaths)
    try:
        stream.emit([[f"  — Probing duration for {_total:,} file(s)"
                     f" via ffprobe…\n",
                     ["simpleline", "backfill_progress"]]])
    except Exception as e:
        _log.debug("swallowed: %s", e)
    _last_tick = time.time()
    _done = 0
    def _cancelled() -> bool:
        return bool(cancel_event is not None and cancel_event.is_set())

    def _probe_one(fp: str) -> float | None:
        while pause_event is not None and pause_event.is_set():
            if _cancelled():
                return None
            time.sleep(0.05)
        if _cancelled():
            return None
        return _probe_file_duration(fp)

    def _record_probe(result: WorkResult[str, float | None]) -> None:
        nonlocal _done, _last_tick
        out[result.item] = None if result.error is not None else result.value
        _done += 1
        _now = time.time()
        if (_now - _last_tick) >= 1.5 and _done < _total:
            try:
                stream.emit([[f"  — Probing duration "
                             f"[{_done:,}/{_total:,}]…\n",
                             ["simpleline", "backfill_progress"]]])
            except Exception as e:
                _log.debug("swallowed: %s", e)
            _last_tick = _now

    # Submit at most one item per worker.  On cancel there is no enormous
    # executor queue left to drain, and already-running ffprobe calls are
    # allowed to finish behind a strict late-write boundary: only this caller
    # persists results that arrived before cancellation.
    run_bounded(
        filepaths,
        _probe_one,
        _record_probe,
        max_workers=max_workers,
        thread_name_prefix="dur-probe",
        is_cancelled=_cancelled,
    )
    # Persist probed durations to the DB so the next pass doesn't
    # re-probe the same files. One transaction, write only the
    # successful probes (None values stay NULL — re-probing them is
    # cheap and might succeed if the file was being written during
    # the first attempt).
    try:
        from .. import index as _idx
        _conn = _idx._open()
        if _conn is not None:
            with _idx._db_lock:
                for _fp, _d in out.items():
                    if _d is None or _d <= 0:
                        continue
                    try:
                        _conn.execute(
                            "UPDATE videos SET duration_s=? "
                            "WHERE filepath=? COLLATE NOCASE "
                            "AND (duration_s IS NULL OR duration_s<=0)",
                            (_d, _fp))
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                _conn.commit()
    except Exception as e:
        _log.debug("swallowed: %s", e)
    try:
        _resolved_n = sum(1 for v in out.values() if v and v > 0)
        stream.emit([[f"  — Probed {_resolved_n:,}/{_total:,} duration(s)"
                     f" in {time.time() - _t0:.1f}s\n",
                     ["simpleline", "backfill_progress"]]])
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return out

def count_missing_durations() -> int:
    """How many available archived videos have no stored duration_s.

    Missing and partial catalog rows intentionally remain available to the
    integrity/repair tools, but there is no media file for ffprobe to read.
    Counting them here makes the Video lengths tool offer work that can never
    finish.
    """
    try:
        from .. import index as _idx
        conn = _idx._reader_open()
        if conn is None:
            return 0
        with _idx._reader_lock:
            return int(conn.execute(
                "SELECT COUNT(*) FROM videos "
                "WHERE (duration_s IS NULL OR duration_s<=0) "
                "AND COALESCE(availability, 'available')='available'"
            ).fetchone()[0])
    except Exception as e:
        _log.debug("count_missing_durations failed: %s", e)
        return 0

def backfill_missing_durations(stream: LogStreamer,
                               cancel_event: threading.Event | None = None,
                               pause_event: threading.Event | None = None,
                               ) -> dict:
    """Fill videos.duration_s for every available file that's missing it by
    ffprobing the file locally (no YouTube). The on-disk file is the only
    accurate source — the disk-sweep/import paths register rows without a
    duration. Idempotent: only touches rows still NULL/0, so a cancelled run
    resumes cleanly on the next start. Progress + the actual duration_s
    writes are handled by _probe_durations_bulk. Returns
    {ok, total, resolved, cancelled}."""
    from .. import index as _idx
    conn = _idx._reader_open()
    if conn is None:
        return {"ok": False, "error": "index unavailable",
                "total": 0, "resolved": 0}
    with _idx._reader_lock:
        rows = conn.execute(
            "SELECT filepath FROM videos "
            "WHERE (duration_s IS NULL OR duration_s<=0) "
            "AND COALESCE(availability, 'available')='available' "
            "AND filepath IS NOT NULL AND filepath!='' "
            "ORDER BY rowid").fetchall()
    filepaths = [r[0] for r in rows if r and r[0]]
    total = len(filepaths)
    if not total:
        try:
            stream.emit([["  — Every available video already has a length. "
                          "Nothing to do.\n", "simpleline"]])
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return {"ok": True, "total": 0, "resolved": 0,
                "failed": 0, "unchecked": 0, "cancelled": False}
    try:
        stream.emit([[f" Checking video lengths — {total:,} missing. "
                      f"Reading each file with ffprobe…\n", "header"]])
    except Exception as e:
        _log.debug("swallowed: %s", e)
    probed = _probe_durations_bulk(filepaths, stream, cancel_event, pause_event)
    resolved = sum(1 for v in probed.values() if v and v > 0)
    cancelled = bool(cancel_event and cancel_event.is_set())
    attempted = len(probed)
    # Workers that observe cancellation return without probing. Do not label
    # those files as unreadable; they are simply unfinished and can resume.
    failed = 0 if cancelled else max(0, attempted - resolved)
    unchecked = (max(0, total - resolved) if cancelled
                 else max(0, total - attempted))
    try:
        if cancelled:
            detail = (
                f" Stopped — filled {resolved:,} of {total:,} video "
                f"length(s). {unchecked:,} still unchecked"
            )
            detail += ". Re-run later to continue.\n"
            tag = "summary"
        elif failed:
            detail = (
                f" Finished — filled {resolved:,} of {total:,} video "
                f"length(s). {failed:,} available file(s) could not be read "
                "by ffprobe and were left unchanged.\n"
            )
            tag = "summary"
        else:
            detail = (
                f" Done — filled {resolved:,} of {total:,} video length(s).\n"
            )
            tag = "simpleline_green"
        stream.emit([[detail, tag]])
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return {"ok": True, "total": total, "resolved": resolved,
            "failed": failed, "unchecked": unchecked,
            "cancelled": cancelled}
