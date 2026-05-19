"""
Compress — ffmpeg AV1 NVENC encoder wrapper.

Mirrors YTArchiver.py:9488 compress command. Encodes a video at a target
bitrate + quality preset, replaces the original in-place (with safety
checks + rollback on unexpectedly-larger output).

Presets (from YTArchiver.py:179 _COMPRESS_PRESETS):
    resolution × tier → MB per hour of video

    1080: Generous=1200 Average=700 Below Average=300
    720: Generous=800 Average=475 Below Average=200
    480: Generous=500 Average=300 Below Average=130
    360: Generous=375 Average=225 Below Average=100
    240: Generous=250 Average=150 Below Average=65
    144: Generous=150 Average=90 Below Average=40
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .log import get_logger
from .log_stream import LogStreamer

_log = get_logger(__name__)


_COMPRESS_PRESETS = {
    # 2160 (4K) and 1440 (1440p) used to be missing, causing
    # silent fallback to 1080p bitrates — 4K output at Generous would
    # get ~1200 MB/hr when it needs ~2600+. Padding with AV1-appropriate
    # bitrates (AV1 NVENC on RTX 40-series, roughly 2x efficient vs HEVC).
    "2160": {"Generous": 2800, "Average": 1600, "Below Average": 800},
    "1440": {"Generous": 1800, "Average": 1000, "Below Average": 500},
    "1080": {"Generous": 1200, "Average": 700, "Below Average": 300},
    "720": {"Generous": 800, "Average": 475, "Below Average": 200},
    "480": {"Generous": 500, "Average": 300, "Below Average": 130},
    "360": {"Generous": 375, "Average": 225, "Below Average": 100},
    "240": {"Generous": 250, "Average": 150, "Below Average": 65},
    "144": {"Generous": 150, "Average": 90, "Below Average": 40},
}

_FFMPEG_TIME_RE = re.compile(r"time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})")
_FFPROBE_DURATION_RE = re.compile(r"Duration:\s*(\d{2}):(\d{2}):(\d{2})")

# startupinfo + creationflags now come from
# subprocess_util — same source of truth as sync.py and transcribe.py.
from .subprocess_util import make_startupinfo as _make_startupinfo

_startupinfo = _make_startupinfo()


def get_bitrate(quality: str, output_res: str) -> int:
    """Return MB/hr for a given quality tier + resolution."""
    res_key = output_res if output_res in _COMPRESS_PRESETS else "1080"
    return _COMPRESS_PRESETS.get(res_key, _COMPRESS_PRESETS["1080"]) \
                            .get(quality, 700)


def find_ffmpeg() -> str | None:
    p = shutil.which("ffmpeg") or shutil.which("ffmpeg.exe")
    if p:
        return p
    for c in [
        Path.cwd() / "ffmpeg.exe",
        Path(__file__).resolve().parent.parent / "ffmpeg.exe",
    ]:
        if c.exists():
            return str(c)
    return None


# Patch D: cache `ffmpeg -i` output by (filepath, mtime). The compress
# pipeline calls get_video_duration() then get_video_codec() back-to-
# back on the same file (lines ~181, 188 plus post-compress at 359,
# 384). Each call previously spawned its own ffmpeg subprocess (~300-
# 500ms each). With the cache, second/third/fourth probe of the same
# file is instant. Cache key includes mtime so a file modified between
# probes invalidates correctly.
_probe_cache: dict[tuple[str, float], tuple[float, str]] = {}
_probe_cache_lock = threading.Lock()


def _probe_video_info(filepath: str, ffmpeg: str) -> tuple[float, str]:
    """Run `ffmpeg -i filepath` once and parse stderr for duration +
    video codec. Returns (duration_seconds, codec_lowercase). Returns
    (0.0, "") on probe failure.

    Cached by (filepath, mtime) — see _probe_cache above.
    """
    try:
        _mt = os.path.getmtime(filepath)
    except OSError:
        _mt = 0.0
    _key = (filepath, _mt)
    with _probe_cache_lock:
        _cached = _probe_cache.get(_key)
        if _cached is not None:
            return _cached
    try:
        proc = subprocess.run(
            [ffmpeg, "-i", filepath],
            capture_output=True, text=True, timeout=15,
            startupinfo=_startupinfo,
        )
    except (subprocess.TimeoutExpired, OSError):
        result = (0.0, "")
        with _probe_cache_lock:
            _probe_cache[_key] = result
        return result
    stderr = proc.stderr or ""
    dur = 0.0
    m = _FFPROBE_DURATION_RE.search(stderr)
    if m:
        dur = float(int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3)))
    codec = ""
    import re as _re
    m2 = _re.search(r"Video:\s*([a-z0-9]+)", stderr, _re.IGNORECASE)
    if m2:
        codec = m2.group(1).lower()
    result = (dur, codec)
    with _probe_cache_lock:
        _probe_cache[_key] = result
    return result


def get_video_duration(filepath: str, ffmpeg: str) -> float:
    """Probe duration in seconds via ffmpeg -i.
    Patch D: delegates to the shared _probe_video_info cache."""
    return _probe_video_info(filepath, ffmpeg)[0]


def get_video_codec(filepath: str, ffmpeg: str) -> str:
    """Probe video-stream codec name via ffmpeg -i stderr. Returns
    lowercase codec string ("av1", "hevc", "h264", ...) or "" on
    probe failure. Used post-compress to verify NVENC actually
    produced AV1 and didn't silently fall back to another codec on
    driver hiccup.
    Patch D: delegates to the shared _probe_video_info cache."""
    return _probe_video_info(filepath, ffmpeg)[1]


def compress_video(input_path: str, stream: LogStreamer,
                   quality: str = "Average",
                   output_res: str = "720",
                   cancel_event: threading.Event | None = None,
                   replace_original: bool = True,
                   progress_cb: Callable[[int], None] | None = None,
                   dry_run: bool = False,
                   ) -> dict[str, Any]:
    """
    Encode one video with av1_nvenc at quality+res preset.
    Returns {ok, orig_bytes, new_bytes, saved_pct, took}.

    `dry_run=True` logs the intended action without touching any files
    (no ffmpeg spawn, no `_TEMP_COMPRESS` temp, no `os.replace`). Useful
    for previewing a batch run before committing to it.
    """
    ffmpeg = find_ffmpeg()
    if not ffmpeg:
        stream.emit_error("Video compression isn't available — the compression tool (ffmpeg) is missing.")
        return {"ok": False, "error": "ffmpeg missing"}
    if not os.path.isfile(input_path):
        return {"ok": False, "error": "input not found"}
    if dry_run:
        orig_size = os.path.getsize(input_path)
        mb_per_hr = get_bitrate(quality, output_res)
        stream.emit([
            ["[dry-run] ", ["dim"]],
            [f"would compress {os.path.basename(input_path)} ", None],
            [f"(quality={quality}, res={output_res}, "
             f"target ~{mb_per_hr} MB/hr, "
             f"orig {orig_size/1024/1024:.1f} MB) ", ["dim"]],
            [f"→ replace_original={replace_original}\n", ["dim"]],
        ])
        return {"ok": True, "dry_run": True,
                "input": input_path, "orig_bytes": orig_size,
                "quality": quality, "output_res": output_res}

    base, ext = os.path.splitext(input_path)
    temp_path = base + "_TEMP_COMPRESS" + ext
    # write a .lock sidecar next to the temp file so a
    # concurrent startup_cleanup_temps pass (e.g. second-instance launch)
    # doesn't nuke this in-flight encode. _LockGuard removes it via
    # __del__ when the function returns (CPython refcount semantics —
    # same pattern as sync.py's _ExecGuard). Inert if directory isn't
    # writable.
    lock_path = temp_path + ".lock"
    try:
        with open(lock_path, "w", encoding="utf-8") as _lk:
            _lk.write(str(int(time.time())))
    except OSError:
        lock_path = ""

    class _LockGuard:
        __slots__ = ("_path",)
        def __init__(self, p: str): self._path = p
        def __del__(self):
            try:
                if self._path:
                    os.remove(self._path)
            except OSError:
                pass
    _lock_guard = _LockGuard(lock_path)  # noqa: F841 — held for cleanup

    # per-file in-place marker so the "Encoding ...", every
    # progress-bar update, and the final ✓ done line all REPLACE one
    # another at the same scroll position. Without this the progress
    # bar persisted next to the per-video block while the done line
    # landed minutes later at log bottom, under unrelated channels.
    # Hash the basename so any non-ascii / spaces / quotes are safe in
    # the tag string (must match \w+ rules in logs.js _inplaceKind).
    import hashlib as _hashlib
    _marker_tag = "compress_done_" + _hashlib.md5(
        os.path.basename(input_path).encode("utf-8", "replace")
    ).hexdigest()[:12]

    # Determine bitrates
    mb_per_hr = get_bitrate(quality, output_res)
    target_total_kbps = (mb_per_hr * 1024 * 8) / 3600
    audio_kbps = 128
    video_kbps = max(int(target_total_kbps - audio_kbps), 50)

    # Probe duration
    dur = get_video_duration(input_path, ffmpeg)

    orig_size = os.path.getsize(input_path)
    # Probe the source codec too so the user can see why we're
    # re-encoding (e.g. h264 \u2192 av1) and notice if we're wasting time
    # re-encoding an already-av1 file. Verbose-only.
    try:
        src_codec = get_video_codec(input_path, ffmpeg)
    except Exception:
        src_codec = ""

    # Build ffmpeg command (matches YTArchiver.py:9488)
    cmd = [ffmpeg, "-y", "-i", input_path]
    if output_res and str(output_res).isdigit():
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
        temp_path,
    ]

    name = os.path.basename(input_path)
    display = name if len(name) <= 60 else name[:57] + "..."
    # Em-dash prefix (compress-color) + white body, matching classic
    # simpleline_compress painter output (brackets/em-dashes colored,
    # body text default).
    stream.emit([
        ["  \u2014 ", ["simpleline_compress", _marker_tag]],
        ["Encoding ", ["simpleline", _marker_tag]],
        [f'"{display}"', ["encode_title", _marker_tag]],
        [f" \u2014 {quality} / {output_res}p\n", ["dim", _marker_tag]],
    ])

    # VERBOSE-ONLY diagnostics. All `dim`-tagged so Simple mode drops
    # the whole line via `_line_is_verbose_only`. Design intent:
    # verbose mode should be densely informative — surface every
    # input/decision/command so users
    # debugging a bad encode can see exactly what ffmpeg was told.
    _dur_str = (f"{int(dur//60):02d}:{int(dur%60):02d}"
                if dur > 0 else "unknown")
    _src_codec_str = src_codec or "unknown"
    _full_path = input_path
    stream.emit([
        ["    \u2014 ", ["dim"]],
        [f"source: {_full_path}\n", ["dim"]],
    ])
    stream.emit([
        ["    \u2014 ", ["dim"]],
        [f"probed: {_dur_str} duration, "
         f"{orig_size:,} bytes ({orig_size/1024/1024:.1f} MB), "
         f"codec={_src_codec_str}\n", ["dim"]],
    ])
    stream.emit([
        ["    \u2014 ", ["dim"]],
        [f"target: {mb_per_hr} MB/hr "
         f"({target_total_kbps:.0f} kbps total = "
         f"{video_kbps} kbps video + {audio_kbps} kbps audio)\n",
         ["dim"]],
    ])
    stream.emit([
        ["    \u2014 ", ["dim"]],
        [f"encoder: av1_nvenc \u00b7 preset p6 \u00b7 multipass 2 \u00b7 "
         f"rc vbr \u00b7 cq 32 \u00b7 maxrate {int(video_kbps * 1.5)}k\n",
         ["dim"]],
    ])
    # Full ffmpeg command \u2014 single line, easy to copy-paste into a
    # terminal for manual reproduction. Truncate at 600 chars in case
    # the input path is absurdly long.
    _cmd_str = " ".join(repr(c) if " " in c or '"' in c else c for c in cmd)
    if len(_cmd_str) > 600:
        _cmd_str = _cmd_str[:600] + "\u2026"
    stream.emit([
        ["    \u2014 ", ["dim"]],
        [f"ffmpeg cmd: {_cmd_str}\n", ["dim"]],
    ])

    t0 = time.time()
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            startupinfo=_startupinfo, encoding="utf-8", errors="replace", bufsize=1,
        )
        # register ffmpeg with PROCESS_REGISTRY so
        # shutdown's kill_all() reaps it even if the encode is mid-flight.
        try:
            from .process_runner import PROCESS_REGISTRY
            PROCESS_REGISTRY.register(proc)
        except Exception:
            pass
    except OSError as e:
        stream.emit_error(f"Couldn't start video compression: {e}")
        return {"ok": False, "error": str(e)}

    last_pct = -1
    first_progress_emitted = False
    for line in proc.stderr:
        if cancel_event is not None and cancel_event.is_set():
            try:
                proc.terminate()
                proc.wait(timeout=3)
            except Exception:
                proc.kill()
            try:
                os.remove(temp_path)
            except OSError:
                pass
            stream.emit_text(" \u26d4 Encode cancelled.", "red")
            return {"ok": False, "reason": "cancelled"}

        m = _FFMPEG_TIME_RE.search(line)
        if m and dur > 0:
            sec = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
            pct = min(99, int(sec / dur * 100))
            # Bug [26]: emit at the FIRST progress sample regardless of
            # whether it lands on a 5% boundary. A fast encode that goes
            # straight from 0% to 6% to 12% would otherwise never trigger
            # an emit (none of those % 5 == 0), leaving the UI stuck at
            # the initial state until the encode completes.
            should_emit = (pct != last_pct
                           and (pct % 5 == 0 or not first_progress_emitted))
            if should_emit:
                last_pct = pct
                first_progress_emitted = True
                stream.emit([
                    [" ", [_marker_tag]],
                    ["\u2588" * (pct // 5), ["encode_progress", _marker_tag]],
                    ["\u2591" * (20 - pct // 5), ["dim", _marker_tag]],
                    [f" {pct}%", ["encode_pct", _marker_tag]],
                    ["\n", [_marker_tag]],
                ])
                if progress_cb:
                    try: progress_cb(pct)
                    # previously silently eaten; surface so
                    # a broken UI hook shows up in logs instead of
                    # mysteriously going silent mid-encode.
                    except Exception as _cb_e:
                        stream.emit_dim(f" (progress_cb failed: {_cb_e})")

    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.terminate()
    # unregister from PROCESS_REGISTRY now that the
    # encode has exited.
    try:
        from .process_runner import PROCESS_REGISTRY
        PROCESS_REGISTRY.unregister(proc)
    except Exception:
        pass

    # check ffmpeg returncode BEFORE accepting the output.
    # A mid-encode crash (NVENC driver reset, OOM, etc.) leaves a short
    # temp file that was smaller than the original — with no returncode
    # check, the "smaller than orig" safety below would PROMOTE the
    # truncated stub over the pristine source. Not recoverable.
    if proc.returncode is not None and proc.returncode != 0:
        stream.emit_error(
            f"ffmpeg exited with code {proc.returncode}; leaving original intact.")
        try: os.remove(temp_path)
        except OSError: pass
        return {"ok": False, "error": f"ffmpeg rc={proc.returncode}",
                "reason": "ffmpeg_error"}

    if not os.path.isfile(temp_path):
        stream.emit_error("Video compression didn't produce an output file.")
        return {"ok": False, "error": "no output"}
    new_size = os.path.getsize(temp_path)

    # Verbose-only: announce that the subprocess returned ok and we're
    # entering the safety-check phase. Helps the user follow the flow.
    stream.emit([
        ["    — ", ["dim"]],
        [f"ffmpeg returncode=0, output written: "
         f"{new_size:,} bytes ({new_size/1024/1024:.1f} MB)\n",
         ["dim"]],
    ])

    # ffprobe the temp file's duration and require it within
    # ~2% of the original. A partial/truncated encode that SOMEHOW
    # passes the returncode check (Windows hard-kill, ffmpeg oddity)
    # could still be a 2-minute stub of a 60-minute video. Without this
    # guard the "smaller than orig" check below promotes it, silently
    # destroying the tail.
    if dur > 0:
        new_dur = get_video_duration(temp_path, ffmpeg)
        stream.emit([
            ["    — ", ["dim"]],
            [f"duration check: orig {dur:.1f}s vs new {new_dur:.1f}s "
             f"(tolerance ±{max(3.0, dur * 0.02):.1f}s)\n",
             ["dim"]],
        ])
        # 2% tolerance, minimum 3 seconds absolute slack for very short clips.
        max_loss = max(3.0, dur * 0.02)
        if new_dur <= 0 or (dur - new_dur) > max_loss:
            stream.emit_error(
                f"Compressed duration mismatch "
                f"(orig {dur:.0f}s, new {new_dur:.0f}s) — leaving original intact.")
            try: os.remove(temp_path)
            except OSError: pass
            return {"ok": False, "error": "duration_mismatch",
                    "reason": "duration_mismatch",
                    "orig_dur": dur, "new_dur": new_dur}

    # verify the output codec is actually AV1. If NVENC
    # silently falls back to HEVC/H.264 (driver crash, session
    # collision, unsupported input colorspace), ffmpeg can return
    # rc=0 + correct duration but wrong codec. Promoting a non-AV1
    # "compressed" file wastes future re-compress runs and may
    # produce a larger-than-expected archive.
    new_codec = get_video_codec(temp_path, ffmpeg)
    stream.emit([
        ["    — ", ["dim"]],
        [f"codec check: output codec = {new_codec or 'unknown'} "
         f"(expected av1)\n", ["dim"]],
    ])
    if new_codec and new_codec != "av1":
        stream.emit_error(
            f"Compressed output is {new_codec}, not av1 "
            f"(NVENC silent fallback?) — leaving original intact.")
        try: os.remove(temp_path)
        except OSError: pass
        return {"ok": False, "error": "codec_mismatch",
                "reason": "codec_mismatch",
                "expected": "av1", "actual": new_codec}

    # Safety: if output is larger than input, skip the replace
    if new_size >= orig_size:
        stream.emit([
            [" ", None],
            ["\u26a0 ", "red"],
            [f"Output larger than original ({orig_size:,} \u2192 {new_size:,}), skipping replace.\n",
             "simpleline"],
        ])
        try:
            os.remove(temp_path)
        except OSError:
            pass
        return {"ok": False, "reason": "grew",
                "orig_bytes": orig_size, "new_bytes": new_size}

    took = time.time() - t0
    saved_pct = 100.0 * (1 - new_size / orig_size)

    if replace_original:
        # retry the replace a couple of times on transient
        # Windows file locks (VLC preview, antivirus, Explorer preview-
        # pane, Thumbnail cache). AV1 encodes cost minutes of GPU time;
        # throwing away the output because another process held the
        # target for 2 seconds is bad economics.
        _replace_err: Exception | None = None
        # Patch C: add randomized jitter to the retry sleep so multiple
        # processes (VLC preview, antivirus, Explorer thumbnail cache)
        # don't collide on every retry by ticking on the same wall-
        # clock interval. Jitter range is small (0..1s) so total retry
        # window is still bounded.
        import random as _random
        for _try in range(3):
            try:
                os.replace(temp_path, input_path)
                _replace_err = None
                break
            except OSError as e:
                _replace_err = e
                if _try < 2:
                    time.sleep(2.0 + _random.uniform(0, 1.0))
        if _replace_err is not None:
            stream.emit_error(
                f"Could not replace original after 3 attempts: {_replace_err}")
            # on replace-failure (file locked by VLC preview /
            # antivirus / cross-drive), the temp file used to sit in
            # _TEMP_COMPRESS/ forever — `temp_cleanup.py` treats non-
            # empty temp dirs as "in use" and skips them. Clean up now
            # so we don't leak GB of AV1 encodes on repeated failures.
            try:
                os.remove(temp_path)
            except OSError:
                pass
            return {"ok": False, "error": str(_replace_err)}

    # Per-file done line. Same _marker_tag as the progress bars so it
    # REPLACES the bar in place (issue #146) instead of appending at
    # log bottom after unrelated channels have scrolled past.
    stream.emit([
        [" \u2014 \u2713 ", ["simpleline_green", _marker_tag]],
        ["Compressed ", ["simpleline", _marker_tag]],
        [f"\u2014 {orig_size/1024/1024:.0f}MB \u2192 {new_size/1024/1024:.0f}MB ",
         ["dim", _marker_tag]],
        [f"(\u2212{saved_pct:.0f}%)", ["simpleline_compress", _marker_tag]],
        [f" in {took:.0f}s\n", ["dim", _marker_tag]],
    ])
    return {"ok": True, "orig_bytes": orig_size, "new_bytes": new_size,
            "saved_pct": saved_pct, "took": took}


# ── Batch compress + redo-on-larger fallback ───────────────────────────

_QUALITY_LADDER = ["Generous", "Average", "Below Average"]


def compress_videos_batch(paths, stream: LogStreamer,
                          quality: str = "Average",
                          output_res: str = "720",
                          cancel_event: threading.Event | None = None,
                          redo_on_larger: bool = True,
                          batch_size: int = 0,
                          batch_num: int = 1,
                          batch_total: int = 1,
                          channel_name: str = "",
                          dry_run: bool = False,
                          ) -> dict[str, Any]:
    """Compress a list of videos sequentially.

    If `redo_on_larger` is True and the encoded output is ≥ the original
    size, retry that video one step down the quality ladder
    (Generous → Average → Below Average). Matches YTArchiver's "redo"
    pattern from the compress flow.

    `batch_size` (>0) splits `paths` into chunks of that size and runs each
    as a numbered batch with its own header. Mirrors YTArchiver's
    _count_gpu_encode_batches / _get_max_encode_batch / _get_next_compress_batch
    so large jobs don't keep ffmpeg hot for hours and can survive between
    pauses.
    """
    # Auto-split when batch_size provided and we have more paths than that.
    if batch_size and len(paths) > batch_size:
        n_splits = (len(paths) + batch_size - 1) // batch_size
        agg = {"done": 0, "grew": 0, "errors": 0,
               "sum_orig": 0, "sum_new": 0, "cancelled": False}
        for i in range(n_splits):
            if cancel_event is not None and cancel_event.is_set():
                agg["cancelled"] = True
                break
            chunk = paths[i * batch_size:(i + 1) * batch_size]
            r = compress_videos_batch(
                chunk, stream, quality=quality, output_res=output_res,
                cancel_event=cancel_event, redo_on_larger=redo_on_larger,
                batch_size=0, batch_num=i + 1, batch_total=n_splits,
                dry_run=dry_run,
            )
            agg["done"] += r.get("done", 0)
            agg["grew"] += r.get("grew", 0)
            agg["errors"] += r.get("errors", 0)
            agg["sum_orig"] += r.get("sum_orig", 0)
            agg["sum_new"] += r.get("sum_new", 0)
            if r.get("cancelled"):
                agg["cancelled"] = True
                break
        agg["ok"] = True
        return agg
    # Header when we're one of N batches
    if batch_total > 1:
        stream.emit([["=== Compress batch ", "header"],
                     [f"{batch_num}/{batch_total} ", "header"],
                     [f"({len(paths)} videos) ===\n", "header"]])
    t_batch_start = time.time()
    n_done = 0
    n_grew = 0
    n_err = 0
    sum_orig = 0
    sum_new = 0

    # Sticky active status line pinned at the bottom during the batch
    # compress pass — mirrors classic's `mode="compress"` anim
    # (YTArchiver.py:1976 _ANIM_MODES). `clear_line` control drops the
    # old line so each update lands at the current DOM bottom.
    import json as _json
    _batch_label = f"Compressing batch {batch_num}/{batch_total}" if batch_total > 1 else "Compressing"
    def _emit_active(_i: int, _n: int, _fname: str):
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "compress_active"}),
             "__control__"],
        ])
        # Color discipline: only [ / ] + "<label>:" render in the
        # compress color; numbers + filename stay white.
        stream.emit([
            ["[", ["compress_bracket", "compress_active"]],
            [str(_i), ["simpleline", "compress_active"]],
            ["/", ["compress_bracket", "compress_active"]],
            [str(_n), ["simpleline", "compress_active"]],
            ["] ", ["compress_bracket", "compress_active"]],
            [f"{_batch_label}: ", ["compress_bracket", "compress_active"]],
            [f"{_fname}\u2026\n", ["simpleline", "compress_active"]],
        ])

    def _clear_active():
        stream.emit([
            [_json.dumps({"kind": "clear_line",
                          "marker": "compress_active"}),
             "__control__"],
        ])

    for i, path in enumerate(paths, 1):
        if cancel_event is not None and cancel_event.is_set():
            stream.emit_text(" \u26d4 Batch cancelled.", "red")
            break
        stream.emit([
            [f"[{i}/{len(paths)}] ", "compress_bracket"],
            [f"{os.path.basename(path)}\n", "simpleline"],
        ])
        _emit_active(i, len(paths), os.path.basename(path))
        res = compress_video(path, stream, quality=quality,
                              output_res=output_res,
                              cancel_event=cancel_event,
                              dry_run=dry_run)
        if res.get("ok"):
            n_done += 1
            sum_orig += res.get("orig_bytes", 0)
            sum_new += res.get("new_bytes", 0)
        elif res.get("reason") == "grew" and redo_on_larger:
            # Step down one tier and retry
            n_grew += 1
            try:
                idx = _QUALITY_LADDER.index(quality)
            except ValueError:
                # Bug [105]: was idx=1 (Average) which silently demoted
                # an unrecognized "Generous"-equivalent quality to a
                # LOWER tier than the user picked. Default to 0 (most
                # generous) so a typo / schema drift errs on the safe
                # side, and log so the user sees the fallback.
                idx = 0
                stream.emit_dim(
                    f" (unrecognized quality {quality!r}; defaulting to "
                    f"{_QUALITY_LADDER[0]} tier)")
            if idx + 1 < len(_QUALITY_LADDER):
                retry_q = _QUALITY_LADDER[idx + 1]
                stream.emit([
                    [" ", None], ["\u21A9 ", "simpleline_compress"],
                    [f"Retrying at {retry_q}\n", "dim"],
                ])
                res2 = compress_video(path, stream, quality=retry_q,
                                       output_res=output_res,
                                       cancel_event=cancel_event,
                                       dry_run=dry_run)
                if res2.get("ok"):
                    n_done += 1
                    sum_orig += res2.get("orig_bytes", 0)
                    sum_new += res2.get("new_bytes", 0)
                else:
                    n_err += 1
            else:
                # Already at lowest tier; give up
                n_err += 1
        else:
            n_err += 1

    # Drop the sticky active-status line before the done-summary so
    # the "Batch done" footer doesn't sit below a phantom
    # "Compressing..." line that's no longer accurate.
    _clear_active()

    saved = (1 - sum_new / sum_orig) * 100.0 if sum_orig else 0.0
    # Done summary — green checkmark + white body (body was
    # fully-compress-colored before; matches classic painter rule now).
    stream.emit([
        [" \u2713 ", "simpleline_green"],
        ["Batch done: ", "simpleline"],
        [f"{n_done}/{len(paths)} compressed \u00b7 {n_grew} redone \u00b7 "
         f"{n_err} errors \u00b7 saved {saved:.1f}%\n", "simpleline"],
    ])

    # autorun_history [Cmprss] row — matches YTArchiver.py:22602
    # _record_compression. Only emit when something actually happened
    # (matches OLD's behavior + the "only log real work" rule).
    if (n_done > 0 or n_err > 0) and batch_num == batch_total:
        try:
            elapsed = time.time() - t_batch_start
            from . import autorun as _ar
            primary = f"{n_done} compressed"
            _ar.append_history_entry(
                _ar.format_history_entry("Cmprss", channel_name or "",
                                         primary, secondary="",
                                         errors=n_err, took_sec=elapsed))
            # Live activity-log row
            from datetime import datetime as _dt
            now = _dt.now()
            time_str = now.strftime("%I:%M%p").lstrip("0").lower()
            date_str = now.strftime("%b %d").replace(" 0", " ")
            took = (f"took {int(elapsed)}s" if elapsed < 60
                    else f"took {int(elapsed)//60}m {int(elapsed)%60}s")
            stream.emit_activity({
                "kind": "Cmprss",
                "time_date": f"{time_str}, {date_str}",
                "channel": channel_name or "",
                "primary": primary,
                "secondary": "",
                "errors": f"{n_err} errors",
                "took": took,
                "row_tag": "hist_compress" if n_done > 0 else "",
            })
        except Exception as e:
            _log.debug("swallowed: %s", e)
    return {"ok": True, "done": n_done, "grew": n_grew, "errors": n_err,
            "saved_pct": saved}
