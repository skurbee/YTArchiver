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
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .log_stream import LogStreamer


_COMPRESS_PRESETS = {
    "1080": {"Generous": 1200, "Average": 700, "Below Average": 300},
    "720": {"Generous": 800, "Average": 475, "Below Average": 200},
    "480": {"Generous": 500, "Average": 300, "Below Average": 130},
    "360": {"Generous": 375, "Average": 225, "Below Average": 100},
    "240": {"Generous": 250, "Average": 150, "Below Average": 65},
    "144": {"Generous": 150, "Average": 90, "Below Average": 40},
}

_FFMPEG_TIME_RE = re.compile(r"time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})")
_FFPROBE_DURATION_RE = re.compile(r"Duration:\s*(\d{2}):(\d{2}):(\d{2})")

_startupinfo = None
if os.name == "nt":
    _startupinfo = subprocess.STARTUPINFO()
    _startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    _startupinfo.wShowWindow = 0


def get_bitrate(quality: str, output_res: str) -> int:
    """Return MB/hr for a given quality tier + resolution."""
    res_key = output_res if output_res in _COMPRESS_PRESETS else "1080"
    return _COMPRESS_PRESETS.get(res_key, _COMPRESS_PRESETS["1080"]) \
                            .get(quality, 700)


def find_ffmpeg() -> Optional[str]:
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


def get_video_duration(filepath: str, ffmpeg: str) -> float:
    """Probe duration in seconds via ffmpeg -i."""
    try:
        proc = subprocess.run(
            [ffmpeg, "-i", filepath],
            capture_output=True, text=True, timeout=15,
            startupinfo=_startupinfo,
        )
    except (subprocess.TimeoutExpired, OSError):
        return 0.0
    m = _FFPROBE_DURATION_RE.search(proc.stderr)
    if not m:
        return 0.0
    return (int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3)))


def compress_video(input_path: str, stream: LogStreamer,
                   quality: str = "Average",
                   output_res: str = "720",
                   cancel_event: Optional[threading.Event] = None,
                   replace_original: bool = True,
                   progress_cb: Optional[Callable[[int], None]] = None
                   ) -> Dict[str, Any]:
    """
    Encode one video with av1_nvenc at quality+res preset.
    Returns {ok, orig_bytes, new_bytes, saved_pct, took}.
    """
    ffmpeg = find_ffmpeg()
    if not ffmpeg:
        stream.emit_error("Compress: ffmpeg not found.")
        return {"ok": False, "error": "ffmpeg missing"}
    if not os.path.isfile(input_path):
        return {"ok": False, "error": "input not found"}

    base, ext = os.path.splitext(input_path)
    temp_path = base + "_TEMP_COMPRESS" + ext

    # Determine bitrates
    mb_per_hr = get_bitrate(quality, output_res)
    target_total_kbps = (mb_per_hr * 1024 * 8) / 3600
    audio_kbps = 128
    video_kbps = max(int(target_total_kbps - audio_kbps), 50)

    # Probe duration
    dur = get_video_duration(input_path, ffmpeg)

    orig_size = os.path.getsize(input_path)

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
        ["  \u2014 ", "simpleline_compress"],
        ["Encoding ", "simpleline"],
        [f'"{display}"', "encode_title"],
        [f" \u2014 {quality} / {output_res}p\n", "dim"],
    ])

    t0 = time.time()
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            startupinfo=_startupinfo, encoding="utf-8", errors="replace", bufsize=1,
        )
    except OSError as e:
        stream.emit_error(f"ffmpeg launch failed: {e}")
        return {"ok": False, "error": str(e)}

    last_pct = -1
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
            if pct != last_pct and pct % 5 == 0:
                last_pct = pct
                stream.emit([
                    [" ", None],
                    ["\u2588" * (pct // 5), "encode_progress"],
                    ["\u2591" * (20 - pct // 5), "dim"],
                    [f" {pct}%", "encode_pct"], ["\n", None],
                ])
                if progress_cb:
                    try: progress_cb(pct)
                    except Exception: pass

    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.terminate()

    if not os.path.isfile(temp_path):
        stream.emit_error("Compress: output file was not created.")
        return {"ok": False, "error": "no output"}
    new_size = os.path.getsize(temp_path)

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
        try:
            os.replace(temp_path, input_path)
        except OSError as e:
            stream.emit_error(f"Could not replace original: {e}")
            return {"ok": False, "error": str(e)}

    # Per-file done line. Green checkmark + white filename + dim size
    # delta, with the saved-percent in compress color to match the
    # classic painter emphasis (only the highlight bits colored).
    stream.emit([
        [" \u2713 ", "simpleline_green"],
        [f"{display} ", "simpleline"],
        [f"\u2014 {orig_size/1024/1024:.0f}MB \u2192 {new_size/1024/1024:.0f}MB ", "dim"],
        [f"(\u2212{saved_pct:.0f}%)", "simpleline_compress"],
        [f" in {took:.0f}s\n", "dim"],
    ])
    return {"ok": True, "orig_bytes": orig_size, "new_bytes": new_size,
            "saved_pct": saved_pct, "took": took}


# ── Batch compress + redo-on-larger fallback ───────────────────────────

_QUALITY_LADDER = ["Generous", "Average", "Below Average"]


def compress_videos_batch(paths, stream: LogStreamer,
                          quality: str = "Average",
                          output_res: str = "720",
                          cancel_event: Optional[threading.Event] = None,
                          redo_on_larger: bool = True,
                          batch_size: int = 0,
                          batch_num: int = 1,
                          batch_total: int = 1,
                          channel_name: str = "") -> Dict[str, Any]:
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
        stream.emit([
            [f"[{_i}/{_n}] ", ["compress_bracket", "compress_active"]],
            [f"{_batch_label}: {_fname}\u2026\n",
             ["compress_bracket", "compress_active"]],
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
                              cancel_event=cancel_event)
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
                idx = 1
            if idx + 1 < len(_QUALITY_LADDER):
                retry_q = _QUALITY_LADDER[idx + 1]
                stream.emit([
                    [" ", None], ["\u21A9 ", "simpleline_compress"],
                    [f"Retrying at {retry_q}\n", "dim"],
                ])
                res2 = compress_video(path, stream, quality=retry_q,
                                       output_res=output_res,
                                       cancel_event=cancel_event)
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
        except Exception:
            pass
    return {"ok": True, "done": n_done, "grew": n_grew, "errors": n_err,
            "saved_pct": saved}
