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

import sys, json, os, io

# Save real stdout for our JSON protocol, redirect stdout/stderr to suppress
# prints from huggingface_hub downloads, tqdm bars, or import warnings.
# audit D-54: keep a handle to the REAL stderr so crashes during model
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
    model = WhisperModel(_model_name, device=_device, compute_type=_compute)

# Restore stderr for real errors during transcription
sys.stderr = sys.__stderr__

_ready_msg = {"status": "ready", "device": _device}
if _cuda_fallback_reason:
    _ready_msg["cuda_fallback_reason"] = _cuda_fallback_reason
_out.write(json.dumps(_ready_msg) + "\n")
_out.flush()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
        path = req.get("path", "")
        duration = req.get("duration", 0)
        # Optional per-job language override; defaults to English.
        # Pass `null` (or omit) to enable Whisper's auto-detect.
        language = req.get("language", "en")
    except json.JSONDecodeError:
        path = line
        duration = 0
        language = "en"
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
            vad_parameters=dict(min_silence_duration_ms=500),
            condition_on_previous_text=False,
            no_speech_threshold=0.6,
            word_timestamps=True,
        )
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

        # Split any segment longer than 30s at word boundaries (30s cap
        # matches yt-archiver-timestamps.md memory)
        _MAX_SEG = 30.0
        seg_data = []
        for seg in all_segments:
            t = seg.text.strip()
            if not t:
                continue
            dur = seg.end - seg.start
            raw_words = [w for w in (seg.words or []) if w.word.strip()]
            if dur <= _MAX_SEG:
                w_data = [{"w": w.word.strip(), "s": round(w.start, 3), "e": round(w.end, 3)}
                          for w in raw_words]
                seg_data.append({"s": round(seg.start, 2), "e": round(seg.end, 2),
                                 "t": t, "w": w_data})
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
                    seg_data.append({"s": round(chunk_ws[0].start, 2),
                                     "e": round(chunk_ws[-1].end, 2),
                                     "t": chunk_text, "w": w_data})
            else:
                words = t.split()
                n_chunks = max(2, int(dur / _MAX_SEG) + (1 if dur % _MAX_SEG > 0 else 0))
                chunk_dur = dur / n_chunks
                wpc = max(1, len(words) // n_chunks)
                for ci in range(n_chunks):
                    wi0 = ci * wpc
                    wi1 = wi0 + wpc if ci < n_chunks - 1 else len(words)
                    chunk_text = " ".join(words[wi0:wi1])
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
