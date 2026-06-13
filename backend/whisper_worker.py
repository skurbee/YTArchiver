"""
Persistent faster-whisper worker process.

Spawned by transcribe.py under Python 3.11 (where faster-whisper + CUDA
CTranslate2 are installed). Loads the model once, then accepts JSON
requests on stdin and emits JSON status/results on stdout.

Verbatim port of YTArchiver.py:8699 _WHISPER_SCRIPT. DO NOT modify
without testing end-to-end — this file talks a specific JSON protocol
that transcribe.py depends on.

Environment inputs:
    WHISPER_MODEL (default "large-v3")
    WHISPER_DEVICE (default "cuda")
    WHISPER_COMPUTE (default "float16")

Protocol:
    → stdin: { "path": "/full/path.mp4", "duration": 123.4 }
    ← stdout: { "status": "starting" }
              { "status": "progress", "pct": 42 }
              { "status": "ok", "text": "full text...", "segments": [...] }
              or { "status": "error", "text": "reason" }
"""

import io
import json
import os
import sys

# Save real stdout for our JSON protocol, redirect stdout/stderr to suppress
# prints from huggingface_hub downloads, tqdm bars, or import warnings.
# keep a handle to the REAL stderr so crashes during model
# load or transcription land somewhere the parent can read. Previously
# the io.StringIO capture was restored to sys.__stderr__ on line 49, but
# the parent spawned this subprocess with stderr=DEVNULL anyway, so
# nothing ever surfaced. The parent side must pass stderr=subprocess.PIPE
# (see transcribe.py start_subprocess) for the captured stderr to be
# readable on abnormal exit.
_out = sys.stdout
_real_err = sys.stderr
sys.stdout = io.StringIO()
sys.stderr = io.StringIO()

from faster_whisper import WhisperModel

_model_name = os.environ.get("WHISPER_MODEL", "large-v3")
_device = os.environ.get("WHISPER_DEVICE", "cuda")
_compute = os.environ.get("WHISPER_COMPUTE", "float16")

_cuda_fallback_reason = None
try:
    model = WhisperModel(_model_name, device=_device, compute_type=_compute)
except Exception as _cuda_err:
    _cuda_fallback_reason = str(_cuda_err)
    _device = "cpu"
    _compute = "default"
    try:
        model = WhisperModel(_model_name, device=_device, compute_type=_compute)
    except Exception as _cpu_err:
        # Both CUDA AND CPU failed — surface the error JSON BEFORE
        # exiting so the parent's "didn't respond" generic message
        # is replaced with the real diagnostic. Previously this
        # raise propagated through and the parent saw only a dead
        # subprocess + the captured-StringIO stderr that never
        # flushed (sys.stderr restoration happens AFTER model load,
        # so worker stderr during model load goes to the buffer).
        try:
            sys.stderr = sys.__stderr__
            _out.write(json.dumps({
                "status": "error",
                "text": (f"Whisper model load failed on BOTH cuda "
                         f"and cpu. cuda: {_cuda_fallback_reason!r}; "
                         f"cpu: {_cpu_err!r}"),
                "fatal": True,
            }) + "\n")
            _out.flush()
        except Exception:
            pass
        sys.exit(1)

# Restore stderr for real errors during transcription
sys.stderr = sys.__stderr__

_ready_msg = {"status": "ready", "device": _device}
if _cuda_fallback_reason:
    _ready_msg["cuda_fallback_reason"] = _cuda_fallback_reason
_out.write(json.dumps(_ready_msg) + "\n")
_out.flush()

# Shared cancel flag — set by a separate stdin-reader thread when the
# parent sends `{"command": "cancel"}`. The transcription segments
# loop polls this and bails out cleanly, freeing GPU resources without
# the parent having to TerminateProcess (which leaves CUDA context
# fragmented on Windows and degrades GPU memory availability over
# repeated skip_current actions).
import threading as _threading

_cancel_flag = _threading.Event()
_request_queue: list = []  # FIFO of pending {path, duration, language}
_request_lock = _threading.Lock()
_stdin_done = _threading.Event()

def _stdin_reader():
    """Continuously read stdin lines; route cancel commands to the
    cancel flag, route everything else as new requests."""
    while True:
        line = sys.stdin.readline()
        if not line:
            _stdin_done.set()
            return
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            # A torn/partial write from the parent or a UTF-8 BOM at
            # pipe start would land here. Previously the bytes were
            # interpreted as a filepath and enqueued as a transcribe
            # job — risk of treating garbage as a path and stalling
            # the request/response stream (audit: whisper_worker H49).
            # Emit an error response and drop the line.
            try:
                sys.stdout.write(json.dumps({
                    "status": "error",
                    "text": "malformed request (not valid JSON)",
                }) + "\n")
                sys.stdout.flush()
            except Exception:
                pass
            continue
        if obj.get("command") == "cancel":
            _cancel_flag.set()
            continue
        with _request_lock:
            _request_queue.append(obj)

_threading.Thread(target=_stdin_reader, daemon=True,
                  name="whisper-stdin-reader").start()

# Main loop: pull requests from the queue. Sleeps briefly when empty
# so we don't burn CPU between jobs.
import time as _time

while True:
    line = None
    with _request_lock:
        if _request_queue:
            req = _request_queue.pop(0)
            line = "_consumed"
    if line is None:
        if _stdin_done.is_set():
            break
        _time.sleep(0.05)
        continue
    # Convert the dict back to the path/duration/language vars the
    # legacy body expects.
    path = req.get("path", "")
    duration = req.get("duration", 0)
    # Parent's ffprobe-derived fallback when info.duration comes back
    # 0 or None (rare with vad_filter rejecting everything on silent-
    # intro videos). Without this, progress emits got skipped and the
    # UI looked hung (audit: transcribe/core.py:1303 / 1729).
    duration_fallback = req.get("duration_fallback", 0) or 0
    language = req.get("language", "en")
    # Clear cancel for the new job — cancel only applies to the
    # currently-running job.
    _cancel_flag.clear()
    if not path:
        continue

    try:
        _out.write(json.dumps({"status": "starting"}) + "\n")
        _out.flush()

        segments_gen, info = model.transcribe(
            path,
            language=language,
            beam_size=5,
            vad_filter=True,
            vad_parameters={"min_silence_duration_ms": 500},
            condition_on_previous_text=False,
            no_speech_threshold=0.6,
            word_timestamps=True,
        )
        # Pick the best non-zero duration source: faster-whisper's
        # detected info.duration first, then the JSON-supplied
        # duration field, then the parent's ffprobe fallback.
        total_dur = (info.duration
                     if info.duration and info.duration > 0
                     else (duration or duration_fallback or 0))
        all_segments = []
        last_pct = -1
        _cancelled = False
        for seg in segments_gen:
            # Check the cancel flag on each segment. Parent sends
            # `{"command": "cancel"}` on stdin; the stdin-reader
            # thread sets _cancel_flag. We break early, freeing the
            # GPU without the parent having to TerminateProcess
            # (which fragments CUDA context on Windows).
            if _cancel_flag.is_set():
                _cancelled = True
                break
            all_segments.append(seg)
            if total_dur > 0:
                pct = min(99, int(seg.end / total_dur * 100))
                if pct > last_pct:
                    last_pct = pct
                    _out.write(json.dumps({"status": "progress", "pct": pct}) + "\n")
                    _out.flush()
        if _cancelled:
            _out.write(json.dumps({"status": "cancelled"}) + "\n")
            _out.flush()
            continue

        text = " ".join(seg.text.strip() for seg in all_segments if seg.text.strip())

        # ── Re-segment long Whisper outputs to a 30-second cap ─────────
        # Why this whole block exists:
        # Whisper occasionally emits one huge segment for a long run of
        # uninterrupted speech (a monologue, a news anchor, etc). The
        # transcript viewer renders each segment as one karaoke "line"
        # and highlights word-by-word during playback. A 90-second
        # block would scroll off-screen long before its highlight
        # finishes, making the karaoke unusable — so we split anything
        # longer than 30s at word boundaries into multiple shorter
        # segments. 30s matches the cap documented in the
        # yt-archiver-timestamps.md memory note.
        # Three branches below:
        #   1. seg already ≤ 30s   → emit unchanged, keep per-word
        #                            timestamps from Whisper
        #   2. seg > 30s WITH word timestamps   → preferred path: chunk
        #      by counting words, use the actual word start/end times
        #      so the karaoke stays sample-accurate
        #   3. seg > 30s with NO word timestamps   → fallback: chunk
        #      by even time slices, lose the word-level karaoke for
        #      that range. Rare — only when the model was run without
        #      word_timestamps=True (shouldn't happen in this app).
        # Variables in the chunking math:
        #   _MAX_SEG  = 30.0 second segment cap
        #   dur       = this segment's duration (end - start)
        #   n_chunks  = ceil(dur / 30), minimum 2 (we already know
        #               we're in the long-segment branch)
        #   wpc       = words-per-chunk = len(raw_words) // n_chunks
        #   ci        = chunk index (0..n_chunks-1)
        #   wi0..wi1  = slice indices into the word list for this chunk
        _MAX_SEG = 30.0
        seg_data = []
        for seg in all_segments:
            t = seg.text.strip()
            if not t:
                continue
            dur = seg.end - seg.start
            raw_words = [w for w in (seg.words or []) if w.word.strip()]
            # Branch 1: short enough, no chunking needed.
            if dur <= _MAX_SEG:
                w_data = [{"w": w.word.strip(), "s": round(w.start, 3), "e": round(w.end, 3)}
                          for w in raw_words]
                seg_data.append({"s": round(seg.start, 2), "e": round(seg.end, 2),
                                 "t": t, "w": w_data})
            # Branch 2: too long, but we have word-level timestamps —
            # split the word list into roughly-equal slices and use the
            # first/last word's timestamps as the new segment bounds.
            elif raw_words:
                n_chunks = max(2, int(dur / _MAX_SEG) + (1 if dur % _MAX_SEG > 0 else 0))
                wpc = max(1, len(raw_words) // n_chunks)
                for ci in range(n_chunks):
                    wi0 = ci * wpc
                    wi1 = wi0 + wpc if ci < n_chunks - 1 else len(raw_words)
                    chunk_ws = raw_words[wi0:wi1]
                    if not chunk_ws:
                        continue
                    chunk_text = " ".join(w.word.strip() for w in chunk_ws)
                    w_data = [{"w": w.word.strip(), "s": round(w.start, 3), "e": round(w.end, 3)}
                              for w in chunk_ws]
                    # Clamp end >= start + 0.01 so a rare faster-whisper
                    # edge case where last-word.end comes back below
                    # first-word.start doesn't emit a malformed segment
                    # with e < s (audit: whisper_worker.py:170-174).
                    _s_round = round(chunk_ws[0].start, 2)
                    _e_round = round(chunk_ws[-1].end, 2)
                    if _e_round < _s_round + 0.01:
                        _e_round = round(_s_round + 0.01, 2)
                    seg_data.append({"s": _s_round,
                                     "e": _e_round,
                                     "t": chunk_text, "w": w_data})
            # Branch 3: too long AND no word timestamps. Fall back to
            # splitting the text by word count and interpolating segment
            # start/end times linearly across the duration. The chunks
            # carry no word-level data (`"w": []`), so the UI will
            # render them as plain text without per-word karaoke.
            else:
                words = t.split()
                n_chunks = max(2, int(dur / _MAX_SEG) + (1 if dur % _MAX_SEG > 0 else 0))
                chunk_dur = dur / n_chunks
                # Distribute remainder across first chunks so the last
                # chunk isn't lopsidedly bigger than the rest (audit:
                # whisper_worker H63). Previously `wpc = len(words)//n`
                # left a possibly-huge tail in the final chunk.
                _base = len(words) // n_chunks
                _extra = len(words) % n_chunks
                wi0 = 0
                for ci in range(n_chunks):
                    _take = _base + (1 if ci < _extra else 0)
                    if _take == 0:
                        continue
                    wi1 = wi0 + _take
                    chunk_text = " ".join(words[wi0:wi1])
                    wi0 = wi1
                    if not chunk_text:
                        continue
                    cs = round(seg.start + ci * chunk_dur, 2)
                    ce = round(min(seg.end, seg.start + (ci + 1) * chunk_dur), 2)
                    seg_data.append({"s": cs, "e": ce, "t": chunk_text, "w": []})

        _out.write(json.dumps({"status": "ok", "text": text, "segments": seg_data}) + "\n")
        _out.flush()
    except Exception as e:
        _out.write(json.dumps({"status": "error", "text": str(e)}) + "\n")
        _out.flush()
