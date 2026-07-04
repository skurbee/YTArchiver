"""
transcribe.transcribe_vtt — YT auto-caption fetch + VTT parse.

Patch 19 phase T2 (v68.9): extracted from transcribe/legacy.py.

Functions:
    _fetch_captions_via_ytdlp(video_path, stream, fetched_paths_out)
        Probe yt-dlp for captions; write a temp .vtt next to the video.

    _try_auto_captions(video_path, title, channel, stream, punct_mgr,
                       job_tag, video_id_hint)
        Wire the .vtt-parse fast path into the aggregated Transcript.txt
        + hidden JSONL sidecar. Returns True if captions were parsed and
        Whisper can be skipped.

    _ts_to_sec(ts), _parse_vtt(path)
        Pure parsers — the second one is a full-fidelity port of the
        original tkinter app's VTT-to-segments routine.

Already imported standalone by `repair_captions.py` (for _parse_vtt)
and reused by transcribe's auto-caption fast path here.
"""
from __future__ import annotations

import bisect
import glob
import os
import re
import shutil
import subprocess
import time

from ..log import get_logger
from ..log_stream import LogStreamer
from ..subprocess_util import make_startupinfo as _make_startupinfo
from .paths import (
    _generate_distributed_words,
    _hide_per_video_transcript_txt_if_needed,
)
from .transcribe_files import (
    _write_jsonl_entry,
    _write_transcript_entry,
)

_log = get_logger(__name__)

# Hide the yt-dlp subprocess console window on Windows. Matches the
# `_startupinfo` constant in transcribe/legacy.py — when this file was
# split out, the constant didn't come with it and yt-dlp captions
# fetches crashed with `name '_startupinfo' is not defined`.
_startupinfo = _make_startupinfo()


def _resolve_transcript_paths(*args, **kwargs):
    """Lazy proxy — `_resolve_transcript_paths` lives in transcribe/core.py
    and importing it eagerly here would create a cycle (core imports
    transcribe_vtt at the top, and this function calls into core).
    """
    from .core import _resolve_transcript_paths as _real
    return _real(*args, **kwargs)


def _bump_transcription_pending(*args, **kwargs):
    """Lazy proxy — `_bump_transcription_pending` lives in
    transcribe/core.py and adjusts the Subs-row "-N" pending count.
    Eager import would cycle (core imports transcribe_vtt at top).
    """
    from .core import _bump_transcription_pending as _real
    return _real(*args, **kwargs)


def _norm_title(s: str) -> str:
    """Thin alias for text_utils.normalize_title."""
    from ..text_utils import normalize_title
    return normalize_title(s)


def _extract_video_id(video_path: str, hint: str = "") -> str:
    """Resolve a YT video id for the given file via the consolidated helper."""
    from ..text_utils import extract_video_id as _canon
    quick = _canon(video_path, hint=hint)
    if quick:
        return quick
    # Read-only DB lookup — use the reader path so auto-caption /
    # transcribe ID resolution doesn't queue behind a sweep / ingest.
    try:
        from .. import index as _idx
        conn = _idx._reader_open()
        if conn is not None:
            with _idx._reader_lock:
                return _canon(video_path, hint=hint, conn=conn)
    except Exception as e:
        _log.debug("swallowed: %s", e)
    return ""


def _fetch_captions_via_ytdlp(video_path: str, stream: LogStreamer,
                              fetched_paths_out: list[str]) -> str | None:
    """Probe yt-dlp for captions and write a .vtt next to the video.

    Mirrors YTArchiver.py:11641 `_fetch_auto_captions`: tries without cookies
    first (fast — skips Firefox DB read), falls back to with-cookies on 403 /
    empty result. Adds any written file to `fetched_paths_out` so the caller
    can clean up after parsing.

    Returns the path to the written .vtt (auto-caption preferred over manual
    subs so we get <c>-tag word timing), or None if no captions exist or
    yt-dlp is unavailable.
    """
    yt = shutil.which("yt-dlp") or shutil.which("yt-dlp.exe")
    if not yt:
        return None
    # filename regex + videos-table fallback consolidated.
    vid_id = _extract_video_id(video_path)
    if not vid_id:
        return None # can't probe without a video ID

    base = os.path.splitext(video_path)[0]
    temp_base = base + ".__cap_probe"
    video_url = f"https://www.youtube.com/watch?v={vid_id}"

    # glob.escape: titles with [brackets] are normal on YouTube and
    # square brackets are glob character classes — unescaped, the probe
    # finds nothing and cleanup deletes nothing (orphan .vtt litter).
    def _glob_vtts() -> list[str]:
        return glob.glob(glob.escape(temp_base) + "*.vtt")

    def _cleanup():
        for _p in glob.glob(glob.escape(temp_base) + "*"):
            try: os.remove(_p)
            except OSError: pass

    def _run(use_cookies: bool) -> bool:
        cmd = [
            yt, "--skip-download",
            "--write-sub", "--write-auto-sub",
            "--sub-lang", "en", "--sub-format", "vtt",
            "-o", temp_base + ".%(ext)s",
            "--no-playlist",
            "--force-overwrites",
        ]
        if use_cookies:
            try:
                from ..sync import _find_cookie_source
                cmd += list(_find_cookie_source())
            except Exception:
                cmd += ["--cookies-from-browser", "firefox"]
        cmd.append(video_url)
        try:
            # Capture stderr instead of /dev/nulling it so yt-dlp's
            # explanation for a failed caption fetch (cookies expired,
            # member-only, region-lock, etc.) surfaces in the log
            # instead of silently vanishing. User's view goes from
            # "why did Whisper run on this video?" to a clear dim line
            # pointing at the root cause. Only emit when the stderr
            # looks like an auth/cookie issue — generic "no captions
            # available" is noise.
            r = subprocess.run(cmd, stdin=subprocess.DEVNULL,
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE,
                               timeout=120, startupinfo=_startupinfo)
            try:
                err_text = (r.stderr or b"").decode(
                    "utf-8", errors="replace")
            except Exception:
                err_text = ""
            if err_text and use_cookies:
                _lower = err_text.lower()
                if any(p in _lower for p in (
                        "sign in to confirm",
                        "cookies are missing",
                        "cookies are invalid",
                        "failed to extract any player response",
                        "this video is private",
                        "this video is members",
                )):
                    try:
                        # First matching error line, trimmed. Guard
                        # the splitlines()[0] fallback against an
                        # empty err_text \u2014 IndexError previously got
                        # swallowed and the user got no diagnostic
                        # at all (audit: transcribe_vtt H53).
                        _lines = err_text.splitlines() if err_text else []
                        first_err = next(
                            (ln.strip() for ln in _lines
                             if ln.strip().lower().startswith(("error", "warning"))),
                            (_lines[0].strip() if _lines else "")
                        )[:160]
                        if first_err:
                            stream.emit([
                                [" \u26A0 Caption fetch blocked: ", "dim"],
                                [f"{first_err}\n", "dim"],
                            ])
                        else:
                            _log.debug("caption fetch returned no stderr")
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
            return True
        except Exception:
            return False

    # Pass 1: cookieless
    _run(False)
    vtts = _glob_vtts()
    if not vtts:
        _cleanup()
        # Pass 2: with cookies (some channels require auth for captions)
        _run(True)
        vtts = _glob_vtts()
    if not vtts:
        _cleanup()
        return None

    # Prefer auto-generated VTT — it has <c> tags with per-word timestamps.
    def _caption_pref(path: str) -> tuple[int, str]:
        name = os.path.basename(path).lower()
        if name.endswith(".en.vtt"):
            rank = 0
        elif name.endswith(".en-us.vtt"):
            rank = 1
        elif name.endswith(".en-gb.vtt"):
            rank = 2
        else:
            rank = 3
        return (rank, name)

    vtts = sorted(vtts, key=_caption_pref)
    pick = vtts[0]
    if len(vtts) > 1:
        for vf in vtts:
            try:
                with open(vf, "r", encoding="utf-8") as fh:
                    sample = fh.read(2000)
                if "<c>" in sample or "<c " in sample:
                    pick = vf
                    break
            except Exception as e:
                _log.debug("swallowed: %s", e)
    # Track every fetched file (including unpicked alternates) for cleanup.
    fetched_paths_out.extend(vtts)
    return pick


def _try_auto_captions(video_path: str, title: str, channel: str,
                        stream: LogStreamer,
                        punct_mgr=None,
                        job_tag: str = "",
                        video_id_hint: str = "",
                        from_download: bool = False) -> bool:
    """If yt-dlp wrote a .en.vtt (or similar) next to the video, parse it
    into the aggregated channel Transcript.txt + hidden JSONL sidecar,
    then ingest into FTS — skip Whisper entirely.

    `job_tag` is the per-job unique inplace kind (e.g. `whisper_job_7`)
    that stamps every emit from this transcription so progress/done
    lines replace EACH OTHER within the job but never stomp another
    job's lines. reported a high-video-count channel's 2-video transcription
    lines disappearing entirely — root cause was all emits sharing
    the generic `whisper_*` kind, so video 2's "Loading punctuation…"
    replaced video 1's "— ✓ Transcription" done line.

    `punct_mgr` is an optional PunctuationManager. When provided AND
    the model loads successfully, the parsed caption text gets run
    through the punctuation-restoration pass (matches OLD YTArchiver.py:
    15437-15439 `_punctuate_text(text)` call) and the stored source
    tag becomes `(YT+PUNCTUATION)` instead of `(YT CAPTIONS)` so the
    Watch-view source banner shows "punctuation restored". Segments
    in the .jsonl also get punctuated per-segment (matches NEW's
    Whisper punct pass for consistent .jsonl quality).

    Output matches YTArchiver.py:15449-15478 exactly. Returns True on
    success; False if no usable auto-sub file exists."""
    base = os.path.splitext(video_path)[0]
    candidates = [
        f"{base}.en.vtt", f"{base}.en-US.vtt", f"{base}.en-GB.vtt",
        f"{base}.en-us.vtt", f"{base}.en-gb.vtt", f"{base}.vtt",
        f"{base}.en.ttml", f"{base}.en.srt",
    ]
    vtt = next((p for p in candidates if os.path.isfile(p)), None)

    # Fallback: if sync didn't get a .vtt (e.g. auto-transcribe was off at
    # sync time, or yt-dlp's caption fetch failed transiently), try yt-dlp
    # directly here — matches OLD's `_fetch_auto_captions` (YTArchiver.py:11641)
    # which runs a cookieless probe first, then retries with cookies on 403.
    _fetched_temp: list[str] = []
    if not vtt:
        vtt = _fetch_captions_via_ytdlp(video_path, stream, _fetched_temp)
        if vtt:
            candidates.append(vtt)

    if not vtt:
        return False

    t0 = time.time()
    try:
        segs = _parse_vtt(vtt)
    except Exception as _ve:
        # surface the parse failure before bailing. Old code
        # silently returned False, causing the caller to emit "No
        # auto-captions available — using Whisper..." even though the
        # captions WERE present, just unparseable. A dim warning here
        # makes the distinction visible without derailing the fallback.
        try:
            stream.emit_dim(
                f" (auto-captions parse failed: {_ve} \u2014 "
                f"falling back to Whisper)")
        except Exception as e:
            _log.debug("swallowed: %s", e)
        # Clean up any temp files we fetched before bailing. Retry
        # on Windows file-lock cases (yt-dlp's fd may not be fully
        # closed yet) — without this, the orphan .__cap_probe*.vtt
        # accumulates on every subsequent attempt because the same
        # path keeps coming back from the cookieless re-fetch.
        for _p in _fetched_temp:
            _removed = False
            for _attempt in range(3):
                try:
                    os.remove(_p)
                    _removed = True
                    break
                except OSError:
                    time.sleep(0.1)
            if not _removed:
                _log.debug("temp .vtt cleanup failed (will retry next pass): %s", _p)
        return False
    if not segs:
        # Clean ONLY the .vtt files we just fetched into the temp
        # location. Sweeping `candidates` would delete pre-existing
        # user-supplied .vtt sidecars next to videos that happen to
        # parse to zero segments (audit: transcribe_vtt H72) —
        # permanently losing the user's caption work.
        for _p in list(_fetched_temp):
            for _attempt in range(3):
                try:
                    os.remove(_p)
                    break
                except OSError:
                    time.sleep(0.1)
        return False

    # Resolve aggregated transcript paths for this channel (matches OLD layout).
    paths = _resolve_transcript_paths(video_path, title, channel)
    if paths is None:
        return False
    txt_path, jsonl_path, _year, _month, upload_date = paths
    try:
        os.makedirs(os.path.dirname(txt_path), exist_ok=True)
    except OSError:
        pass

    # Extract video id — caller-supplied hint wins (sync passes the
    # DLTRACK-derived id explicitly so it always matches the sync.py
    # tx_done_<vid> placeholder; see issue #148). Then try the filename
    # suffix, then the FTS DB lookup. consolidated into helper.
    vid_id = _extract_video_id(video_path, hint=video_id_hint or "")

    # Append formatted entry to the aggregated .txt + hidden .jsonl.
    full_text = " ".join(s["t"] for s in segs if s.get("t")).strip()
    duration = segs[-1]["e"] if segs else 0

    # Punctuation restoration — mirrors.
    # YT auto-captions arrive as a run-on stream of lowercase words;
    # running them through the punct model restores casing + commas
    # + periods so the .txt reads like a real transcript. Source tag
    # flips to `YT+PUNCTUATION` so ArchivePlayer / Watch banner can
    # detect the upgraded quality. Silently falls back to raw captions
    # if the punct model isn't available or the call fails.
    # Skip the pass entirely when the source VTT already arrived with
    # sentence punctuation — modern YT auto-cap is delivered punctuated
    # natively, and re-running the model on already-good text is wasted
    # work. The banner that fronts the watch view is content-driven
    # (see logs.js _buildSourceBanner) so the user still sees the
    # "(punctuated)" qualifier; the source tag stays YT CAPTIONS which
    # now consistently means "no punct pass ran during ingest".
    src_tag = "YT CAPTIONS"
    _vtt_punct_re = re.compile(r"[.,!?;:]")
    # Sample three windows (start + middle + end) instead of just the
    # first 800 chars so a long video with mixed caption sources (e.g.
    # punctuated manual-subs intro + bare-words auto-cap body) doesn't
    # falsely classify as "already punctuated" and skip the per-segment
    # punct pass. Each sample is 400 chars.
    _already_punct = False
    if full_text:
        _W = 400
        _samples = [full_text[:_W]]
        if len(full_text) > _W * 2:
            _mid = max(0, (len(full_text) - _W) // 2)
            _samples.append(full_text[_mid:_mid + _W])
        if len(full_text) > _W * 3:
            _samples.append(full_text[-_W:])
        _punct_hits = sum(
            1 for s in _samples if _vtt_punct_re.search(s))
        # Majority vote (was: unanimous). Unanimous-vote with only 2
        # samples flagged punctuated captions as unpunctuated whenever
        # the second sample's specific 400-char window happened to lack
        # terminal punctuation, triggering a wasteful + sometimes
        # double-punctuating re-pass (audit: transcribe_vtt H47).
        # Floor-half ceiling so 1/1, 1/2, 2/3, 2/4 = punctuated.
        import math as _math
        _already_punct = _punct_hits >= _math.ceil(len(_samples) / 2)
    if punct_mgr is not None and full_text and not _already_punct:
        try:
            # `job_tag` (e.g. `whisper_job_7`) makes this line
            # replace ONLY this video's prior "Loading punctuation..."
            # or similar, and get replaced ONLY by this video's final
            # "— ✓ Transcription" done line. Without it, any other
            # video's whisper_* emit would stomp this line.
            _tag_list = ["transcribe_using"]
            if job_tag:
                _tag_list.append(job_tag)
            else:
                _tag_list.append("whisper_progress")
            stream.emit([[" Adding punctuation...\n", _tag_list]])
            punct_text = punct_mgr.punctuate(full_text)
            if punct_text and punct_text != full_text:
                full_text = punct_text
                # Per-segment punct so the JSONL (and therefore FTS /
                # Watch-view karaoke text) is punctuated consistently.
                # Matches NEW's Whisper flow's per-segment pass.
                for seg in segs:
                    t = (seg.get("t") or "").strip()
                    if t:
                        try:
                            pt = punct_mgr.punctuate(t)
                            if pt:
                                seg["t"] = pt
                        except Exception as e:
                            _log.debug("swallowed: %s", e)
                src_tag = "YT+PUNCTUATION"
        except Exception as _pe:
            stream.emit_dim(f" (punctuation skipped: {_pe})")

    if not _write_transcript_entry(txt_path, title, upload_date, duration,
                                   src_tag, full_text):
        try:
            stream.emit_error(f"Could not write transcript to {txt_path}")
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return False
    _hide_per_video_transcript_txt_if_needed(video_path, txt_path)
    if not _write_jsonl_entry(jsonl_path, vid_id, title, segs):
        try:
            stream.emit_error(
                f"Could not write transcript JSONL to {jsonl_path} "
                f"— not marking {os.path.basename(video_path)} transcribed")
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return False

    # Clean up only .vtt sidecars fetched by this function. Pre-existing
    # user-supplied caption sidecars must survive a successful parse.
    for _p in list(_fetched_temp):
        if os.path.isfile(_p):
            try: os.remove(_p)
            except OSError: pass

    # FTS ingest — use the new aggregated .jsonl path
    try:
        from .. import index as _idx
        _idx.ingest_jsonl(video_path, jsonl_path, title, channel)
        _idx.mark_video_transcribed(video_path)
    except Exception as e:
        _log.debug("swallowed: %s", e)
    # Decrement transcription_pending / set transcription_complete on 0.
    _bump_transcription_pending(channel, -1)
    # Drop this video's ID from the authoritative pending list so the
    # Subs "-X" indicator shrinks. `vid_id` was extracted earlier in
    # this function for jsonl + FTS writes.
    if vid_id:
        try:
            from .. import ytarchiver_config as _cfg
            _cfg.remove_pending_tx_id(vid_id)
        except Exception as e:
            _log.debug("swallowed: %s", e)

    took = time.time() - t0
    realtime = f"{duration/took:.1f}x" if took > 0 and duration > 0 else ""
    # Per-video done line. Stamp every segment with the per-job
    # `job_tag` (unique per video) so this line REPLACES this
    # video's in-progress lines AND gets left alone by other
    # videos' transcriptions. ALSO stamp with `tx_done_<vid>` so the
    # done line lands at the placeholder sync.py reserved under the
    # channel's block rather than at the bottom of the log.
    # `_inplaceKind` prioritizes `tx_done_` over `whisper_job_`.
    # audit SR-2 (user screenshot): on multi-video channels,
    # the "Transcription queued…" placeholder persisted next to the
    # auto-captions ✓ done line because `_tx_tag` was LAST in each
    # tag list here — `_inplaceKind` resolved the emit to
    # `whisper_job_<N>` (the first match) and the tx_done_<vid>
    # marker got missed, so the placeholder didn't replace.
    # The Whisper-done path at _transcribe_one already put _tx_tag
    # FIRST (see comment at line 2529) but the auto-captions path
    # kept the old order. Mirror it so both paths correctly replace
    # the sync.py placeholder.
    _tx_tag = f"tx_done_{vid_id}" if vid_id else ""
    _em_tag = [t for t in (_tx_tag, "whisper_bracket", job_tag) if t]
    _dim_tag = [t for t in (_tx_tag, "dim", job_tag) if t]
    # Parens detail uses `tx_detail` (a brighter shade than `dim`) so
    # "(auto-captions, took 2s, 133.0x realtime)" is actually readable.
    # `.t-dim` is so close to the log background that the detail blended
    # into the noise \u2014 this lifts it into the readable range without
    # competing with the main "\u2713 Transcription" label.
    _detail_tag = [t for t in (_tx_tag, "tx_detail", job_tag) if t]
    _lbl_tag = [t for t in (_tx_tag, "simpleline_blue", job_tag) if t]
    # Match the Whisper done line in core.py: indent under the parent
    # " \u2014 \u2713 Title (size)" video row when this transcription is part of
    # a sync download flow. Standalone retranscribes keep the 1-space
    # indent so they line up with their own header.
    _lead = "      " if from_download else " "
    stream.emit([
        [_lead, _dim_tag],
        ["\u2014 \u2713 ", _em_tag],
        ["Transcription", _lbl_tag],
        [f" (auto-captions, took {took:.0f}s, {realtime} realtime)\n", _detail_tag],
    ])
    return True


def _ts_to_sec(ts: str) -> float:
    """Convert 'HH:MM:SS.mmm' or 'MM:SS.mmm' to float seconds.
    Mirrors YTArchiver.py:8245-8252."""
    ts = ts.replace(",", ".").strip()
    parts = ts.split(":")
    if len(parts) == 3:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
    if len(parts) == 2:
        return int(parts[0]) * 60 + float(parts[1])
    return 0.0


def _attach_words_to_segments(segments: list, all_words: list) -> list:
    out = []
    if all_words:
        word_starts = [w["s"] for w in all_words]
        for seg in segments:
            seg_words = []
            # Strict partitioning: each word belongs to the segment whose
            # [start, end) range contains its timestamp. bisect finds the
            # first word at/after the segment start directly, avoiding the
            # old bounded back-scan that could stop too early.
            widx = bisect.bisect_left(word_starts, seg["start"])
            scan = widx
            while scan < len(all_words) and all_words[scan]["s"] < seg["end"]:
                seg_words.append(all_words[scan])
                scan += 1
            if not seg_words:
                seg_words = _generate_distributed_words(
                    seg["text"], seg["start"], seg["end"])
            out.append({
                "s": round(seg["start"], 2),
                "e": round(seg["end"], 2),
                "t": seg["text"],
                "w": seg_words,
            })
    else:
        for seg in segments:
            out.append({
                "s": round(seg["start"], 2),
                "e": round(seg["end"], 2),
                "t": seg["text"],
                "w": _generate_distributed_words(
                    seg["text"], seg["start"], seg["end"]),
            })
    return out


def _parse_vtt(path: str) -> list:
    """Full-fidelity port of YTArchiver.py:8223 `_parse_vtt_to_segments`.

    Three steps:
      1. Parse raw cues from the VTT and extract per-word <c>-tag timestamps
         (the YouTube auto-caption markup: `word<00:00:00.480><c> next</c>`).
      2. Merge rolling-caption overlap where each new cue repeats the tail of
         the previous cue plus a few new words — flush at 30s cap.
      3. Attach per-word timestamps back onto the merged segments; fall back
         to distributed timings when no <c> tags were present (manual subs).

    Returns a list of short-key segments: [{s, e, t, w: [{w, s, e}, ...]}, ...]
    — compatible with `_write_jsonl_entry` and `_try_auto_captions`.
    """
    import html as _html_mod
    import re as _re
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            raw = f.read()
    except OSError:
        return []

    # ── Step 1: Parse raw cues (text + original raw lines for <c> extraction) ──
    raw_cues = [] # (start, end, joined_text, [raw_content_lines])
    lines = raw.split("\n")
    current_start = None
    current_end = None
    current_text = []
    current_raw = []
    ts_line_re = _re.compile(r'(\d[\d:.]+)\s*-->\s*(\d[\d:.]+)')
    for line in lines:
        line = line.strip()
        m = ts_line_re.match(line)
        if m:
            if current_text and current_start is not None:
                raw_cues.append((current_start, current_end,
                                 " ".join(current_text), list(current_raw)))
            current_start = _ts_to_sec(m.group(1))
            current_end = _ts_to_sec(m.group(2))
            current_text = []
            current_raw = []
            continue
        if not line or line.startswith(("WEBVTT", "NOTE", "Kind:", "Language:")):
            continue
        if _re.match(r'^\d+$', line):
            continue
        if 'align:' in line or 'position:' in line:
            continue
        current_raw.append(line)
        cleaned = _re.sub(r'<[^>]+>', '', line)
        cleaned = _html_mod.unescape(cleaned)
        cleaned = _re.sub(r'\s+', ' ', cleaned).strip()
        if cleaned:
            current_text.append(cleaned)
    if current_text and current_start is not None:
        raw_cues.append((current_start, current_end,
                         " ".join(current_text), list(current_raw)))
    if not raw_cues:
        return []

    # ── Step 1b: Extract per-word timestamps from <c> tags ──
    ctag_re = _re.compile(r'<(\d[\d:.]+)><c[^>]*>(.*?)</c>')
    all_words = []
    has_ctags = False
    for cue_idx, (cue_s, cue_e, _, raw_lines) in enumerate(raw_cues):
        for raw_line in raw_lines:
            tags = list(ctag_re.finditer(raw_line))
            if tags:
                has_ctags = True
                # Every cue with <c> tags has an untagged prefix before the
                # first tag — for cue 0 it's the leading words, for every
                # continuation cue it's the newly-rolled-in word(s) for the
                # rolling caption. Both must be captured at cue_s, otherwise
                # the new word in each continuation cue is silently dropped.
                first_tag_pos = raw_line.find('<')
                if first_tag_pos > 0:
                    prefix = _html_mod.unescape(raw_line[:first_tag_pos]).strip()
                    for pw in prefix.split():
                        if pw.strip():
                            all_words.append({"w": pw.strip(), "s": cue_s})
                for mm in tags:
                    ts = _ts_to_sec(mm.group(1))
                    word_text = _html_mod.unescape(mm.group(2)).strip()
                    for w in word_text.split():
                        if w.strip():
                            all_words.append({"w": w.strip(), "s": ts})
    # No <c> tags (manual subs): distribute word starts across cue duration.
    if not has_ctags:
        for cue_s, cue_e, text, _ in raw_cues:
            dur = cue_e - cue_s
            words_in_cue = text.strip().split()
            n = len(words_in_cue)
            for wi, w in enumerate(words_in_cue):
                all_words.append({
                    "w": w,
                    "s": round(cue_s + dur * wi / max(n, 1), 3),
                })
    # Compute end times (each word ends when the next begins).
    all_words.sort(key=lambda w: w["s"])
    for i in range(len(all_words) - 1):
        all_words[i]["e"] = round(all_words[i + 1]["s"], 3)
    if all_words:
        # set the last word's end to the end of the cue
        # that actually contains its start, not the globally-last cue
        # end. Otherwise the final word's highlight bar stretches all
        # the way to video end (sometimes seconds longer than the word
        # really spans) in the Watch-view karaoke.
        _last_start = all_words[-1]["s"]
        _last_end = round(raw_cues[-1][1], 3)  # safe fallback
        for _cs, _ce, _txt, _raw in raw_cues:
            if _cs <= _last_start <= _ce:
                _last_end = round(_ce, 3)
        # Cap the last word's end at start + median word duration so a
        # cue with trailing silence doesn't stretch the karaoke
        # highlight on the final word for seconds past the actual
        # utterance (audit: transcribe_vtt.py:540-554). Median over
        # previously-computed (e-s) values; falls back to 0.5s if
        # there's only one word.
        if len(all_words) >= 2:
            _durs = sorted(
                max(0.0, all_words[_i].get("e", 0.0) - all_words[_i]["s"])
                for _i in range(len(all_words) - 1)
            )
            _med = _durs[len(_durs) // 2] if _durs else 0.5
            _cap = round(_last_start + max(0.2, min(2.0, _med)), 3)
            _last_end = min(_last_end, _cap)
        all_words[-1]["e"] = _last_end
        for w in all_words:
            w["s"] = round(w["s"], 3)

    # ── Step 2: Merge overlapping rolling cues, flush at 30s cap ──
    MAX_SEG_SECS = 30.0
    segments = []
    seg_start = raw_cues[0][0]
    seg_end = raw_cues[0][1]
    seg_text = raw_cues[0][2]
    for i in range(1, len(raw_cues)):
        _s, _e, _t, _ = raw_cues[i]
        # Skip empty/formatting-only cues — they used to slip past the
        # echo guard below because `_t` was falsy, but the duration-only
        # branch (<0.1s) then mis-flagged the next non-empty cue as an
        # overlap and merged it (audit: transcribe_vtt.py:564-603).
        if not _t.strip():
            continue
        is_overlap = False
        if seg_text and _t:
            seg_words_list = seg_text.split()
            new_words_list = _t.split()
            max_overlap = min(len(seg_words_list),
                              len(new_words_list) - 1, 20)
            for ol in range(max_overlap, 0, -1):
                if seg_words_list[-ol:] == new_words_list[:ol]:
                    extra = " ".join(new_words_list[ol:])
                    is_overlap = True
                    if (_e - seg_start) > MAX_SEG_SECS:
                        if seg_text.strip():
                            segments.append({
                                "start": seg_start, "end": seg_end,
                                "text": seg_text.strip()})
                        seg_start = _s
                        seg_end = _e
                        seg_text = _t
                    else:
                        if extra:
                            seg_text += " " + extra
                        seg_end = _e
                    break
            # Catch near-zero-duration "echo" cues that just repeat the tail.
            if not is_overlap and (_e - _s) < 0.1:
                is_overlap = True
                seg_end = max(seg_end, _e)
        if not is_overlap:
            if seg_text.strip():
                segments.append({
                    "start": seg_start, "end": seg_end,
                    "text": seg_text.strip()})
            seg_start = _s
            seg_end = _e
            seg_text = _t
    if seg_text.strip():
        segments.append({"start": seg_start, "end": seg_end,
                         "text": seg_text.strip()})

    # ── Step 2b: Split any segment that still exceeds the cap ──
    capped = []
    for seg in segments:
        dur = seg["end"] - seg["start"]
        if dur <= MAX_SEG_SECS:
            capped.append(seg)
            continue
        words = seg["text"].split()
        n = max(2, int(dur / MAX_SEG_SECS) + (1 if dur % MAX_SEG_SECS > 0 else 0))
        cdur = dur / n
        # Distribute words EVENLY across chunks instead of dumping the
        # remainder into the last chunk. Old wper = len//n then "last
        # chunk grabs everything left" made the last chunk much larger
        # than wper on a 600-word seg split into 3 chunks — its words
        # didn't line up with its time slice and karaoke drifted on
        # the tail (audit: transcribe_vtt.py:611-625). Spread the
        # remainder across the first `rem` chunks so chunk sizes
        # differ by at most 1 word.
        _nw = len(words)
        _base = _nw // n
        _rem = _nw % n
        _cursor = 0
        for ci in range(n):
            _len_ci = _base + (1 if ci < _rem else 0)
            w0 = _cursor
            w1 = _cursor + _len_ci
            _cursor = w1
            ct = " ".join(words[w0:w1])
            if not ct:
                continue
            cs = round(seg["start"] + ci * cdur, 2)
            ce = round(min(seg["end"], seg["start"] + (ci + 1) * cdur), 2)
            capped.append({"start": cs, "end": ce, "text": ct})
    segments = capped
    return _attach_words_to_segments(segments, all_words)

    # ── Step 3: Attach per-word timestamps back onto the merged segments ──
    out = []
    if all_words:
        word_starts = [w["s"] for w in all_words]
        for seg in segments:
            seg_words = []
            # Strict partitioning: each word belongs to the segment whose
            # [start, end) range contains its timestamp. Previously used a
            # ±0.5s buffer which pulled the next segment's first 1-3 words
            # into the prior segment, producing visible "heading to heading
            # to" duplications at segment boundaries.
            widx = bisect.bisect_left(word_starts, seg["start"])
            scan = widx
            while scan < len(all_words) and all_words[scan]["s"] < seg["end"]:
                seg_words.append(all_words[scan])
                scan += 1
            if not seg_words:
                seg_words = _generate_distributed_words(
                    seg["text"], seg["start"], seg["end"])
            out.append({
                "s": round(seg["start"], 2),
                "e": round(seg["end"], 2),
                "t": seg["text"],
                "w": seg_words,
            })
    else:
        for seg in segments:
            out.append({
                "s": round(seg["start"], 2),
                "e": round(seg["end"], 2),
                "t": seg["text"],
                "w": _generate_distributed_words(
                    seg["text"], seg["start"], seg["end"]),
            })
    return out


