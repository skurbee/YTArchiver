"""
provenance.py — "Embed File Tags" backfill tool.

Retrofits the archive's existing files with the same provenance the
download pipeline now writes for new videos (v80):

  Phase A (txt): upgrade legacy v1 aggregated-Transcript.txt headers
    ===(title), (date), (dur), (src)===
  to the v2 form carrying the video id
    ===(title), (date), (dur), (src), (youtu.be/<id>)===
  The id comes from the sibling hidden `.<name> Transcript.jsonl` —
  the two files are written together by the transcribe pipeline, so an
  exact raw-title match there is the highest-fidelity source. Titles
  that match zero or 2+ distinct ids are skipped and counted (never
  guessed).

  Phase B (mp4): remux each known-id video (ffmpeg -map 0 -c copy,
  no re-encode) to embed title / artist(channel) / date(ISO, from the
  file's mtime — the app's canonical upload-date store) / comment
  (watch URL) tags, plus -movflags +faststart as a side benefit.
  Writes to a same-directory .prov.tmp.mp4, verifies, restores the
  original mtime, then atomically os.replace()s onto the original.
  A ledger in APPDATA makes the pass resumable/idempotent across runs.

Runs as a sync-queue task (kind "provenance") — modeled on
punct_restore / repair_captions — so it inherits the queue's
pause / resume / cancel controls and logs to the main activity log.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import time

from .log import get_logger, swallow
from .pause_helpers import wait_while_paused
from .utils import sqlite_like_escape
from .ytarchiver_config import APP_DATA_DIR, TRANSCRIPTION_DB

_log = get_logger(__name__)

LEDGER_FILE = APP_DATA_DIR / "provenance_ledger.jsonl"

_TMP_SUFFIX = ".prov.tmp.mp4"

# How far a -c copy remux may deviate from the source size before we
# take a closer look. Streams are byte-copied so the delta is *usually*
# just container overhead (moov move + tags) — but some sources (notably
# certain AV1 mp4s) carry megabytes of free/interleave padding that the
# remux legitimately reclaims, shrinking the file well past 5% with ZERO
# content loss. So a size miss is no longer a hard fail: it falls back to
# a duration check (see _embed_one), which is the true "did we lose data"
# signal. The band just decides the fast path vs. the closer look.
_SIZE_MIN_RATIO = 0.95
_SIZE_MAX_RATIO = 1.10

def _startupinfo():
    if os.name != "nt":
        return None
    si = subprocess.STARTUPINFO()
    si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    si.wShowWindow = subprocess.SW_HIDE
    return si


# ─── Ledger ────────────────────────────────────────────────────────────

def _load_ledger() -> dict[str, tuple[int, int]]:
    """{normcase(path): (size, int(mtime))} for files already tagged."""
    done: dict[str, tuple[int, int]] = {}
    try:
        with open(LEDGER_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    done[os.path.normcase(rec["path"])] = (
                        int(rec.get("size", -1)), int(rec.get("mtime", -1)))
                except Exception:
                    continue
    except FileNotFoundError:
        pass
    except OSError as e:
        swallow("provenance ledger read", e)
    return done


def _ledger_append(fh, path: str, size: int, mtime: float) -> None:
    try:
        fh.write(json.dumps({"path": path, "size": int(size),
                             "mtime": int(mtime),
                             "ts": round(time.time(), 1)},
                            ensure_ascii=False) + "\n")
        fh.flush()
    except OSError as e:
        swallow("provenance ledger append", e)


# ─── Phase A — Transcript.txt header upgrade ───────────────────────────

def _jsonl_for_txt(txt_path: str) -> str:
    """`{dir}/Foo Transcript.txt` -> `{dir}/.Foo Transcript.jsonl`."""
    d, name = os.path.split(txt_path)
    return os.path.join(d, "." + name[: -len(".txt")] + ".jsonl")


def _title_id_map(jsonl_path: str) -> dict[str, set[str]]:
    """{raw title: {distinct video ids}} from a Transcript.jsonl."""
    out: dict[str, set[str]] = {}
    try:
        with open(jsonl_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                title = (obj.get("title") or "").strip()
                vid = (obj.get("video_id") or "").strip()
                if title and vid:
                    out.setdefault(title, set()).add(vid)
    except (FileNotFoundError, OSError):
        pass
    return out


def _upgrade_txt_file(txt_path: str, dry_run: bool) -> dict:
    """Upgrade one aggregated .txt's v1 headers in place (atomic).
    Returns {upgraded, ambiguous, unknown, changed}."""
    from .transcribe.transcribe_files import (_HEADER_RE, _VIDEO_ID_RE,
                                              txt_lock_for)
    from .utils import _file_has_hidden_attribute
    stats = {"upgraded": 0, "ambiguous": 0, "unknown": 0, "changed": False}
    ids_by_title = _title_id_map(_jsonl_for_txt(txt_path))
    # Per-video transcripts (manual downloads) carry the Windows hidden
    # attribute — tmp+replace would strip it, so remember and restore.
    try:
        was_hidden = _file_has_hidden_attribute(txt_path)
    except Exception:
        was_hidden = False
    with txt_lock_for(txt_path):
        try:
            with open(txt_path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
        except OSError:
            return stats
        for i, line in enumerate(lines):
            ls = line.rstrip("\r\n")
            if not ls.startswith("===("):
                continue
            m = _HEADER_RE.match(ls)
            if not m or m.group(5):
                continue  # not a header, or already v2
            ids = ids_by_title.get(m.group(1).strip())
            if not ids:
                stats["unknown"] += 1
                continue
            if len(ids) > 1:
                stats["ambiguous"] += 1
                continue
            vid = next(iter(ids))
            if not _VIDEO_ID_RE.match(vid):
                stats["unknown"] += 1
                continue
            eol = line[len(ls):]
            rebuilt = (f"===({m.group(1)}), {m.group(2)}, {m.group(3)}, "
                       f"{m.group(4)}, (youtu.be/{vid})===")
            lines[i] = rebuilt + ls[m.end():] + eol
            stats["upgraded"] += 1
            stats["changed"] = True
        if stats["changed"] and not dry_run:
            tmp = txt_path + ".tmp"
            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    f.writelines(lines)
                    try:
                        f.flush()
                        os.fsync(f.fileno())
                    except OSError as e:
                        swallow("provenance txt fsync", e)
                os.replace(tmp, txt_path)
                if was_hidden:
                    # tmp+replace strips the hidden attribute; restore
                    # it so hidden per-video transcripts stay hidden.
                    from .utils import hide_file_win as _hide
                    try:
                        _hide(txt_path)
                    except Exception as e:
                        swallow("provenance txt re-hide", e)
            except OSError:
                try: os.remove(tmp)
                except OSError: pass
                raise
    return stats


def _channel_dirs(output_dir: str, channel_folder: str | None) -> list[str]:
    if channel_folder:
        p = os.path.join(output_dir, channel_folder)
        return [p] if os.path.isdir(p) else []
    out = []
    try:
        for e in os.scandir(output_dir):
            if (e.is_dir() and not e.name.startswith(".")
                    and e.name != "YTArchiver Info"):
                out.append(e.path)
    except OSError as e:
        swallow("provenance scandir", e)
    return sorted(out, key=str.lower)


# ─── Phase B — MP4 tag remux ───────────────────────────────────────────

def _mp4_worklist(output_dir: str, channel_folder: str | None,
                  db_path=None) -> list[tuple[str, str, str, str]]:
    """[(filepath, video_id, title, channel)] for known-id videos whose
    file lives under output_dir (index-only tp_archive_roots and manual
    one-offs outside the archive root are deliberately excluded)."""
    db = str(db_path or TRANSCRIPTION_DB)
    scope_root = (os.path.join(output_dir, channel_folder)
                  if channel_folder else output_dir)
    pattern = sqlite_like_escape(os.path.normpath(scope_root) + os.sep) + "%"
    rows: list[tuple[str, str, str, str]] = []
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=60)
        try:
            cur = conn.execute(
                "SELECT filepath, video_id, title, channel FROM videos "
                "WHERE video_id IS NOT NULL AND video_id != '' "
                "AND filepath IS NOT NULL "
                "AND filepath LIKE ? ESCAPE '\\'",
                (pattern,))
            for fp, vid, title, channel in cur:
                if fp and fp.lower().endswith(".mp4"):
                    rows.append((fp, vid, title or "", channel or ""))
        finally:
            conn.close()
    except sqlite3.Error as e:
        _log.error("provenance worklist query failed: %s", e)
    rows.sort(key=lambda r: r[0].lower())
    return rows


def _find_ffprobe(ffmpeg: str) -> str | None:
    """Locate ffprobe next to ffmpeg (same build ships both), else on PATH."""
    try:
        d = os.path.dirname(ffmpeg or "")
        if d:
            for name in ("ffprobe.exe", "ffprobe"):
                cand = os.path.join(d, name)
                if os.path.isfile(cand):
                    return cand
    except Exception:
        pass
    import shutil as _sh
    return _sh.which("ffprobe") or _sh.which("ffprobe.exe")


def _count_video_packets(ffprobe: str, path: str) -> int | None:
    """Count ACTUAL demuxed video packets (not the moov-declared frame
    count, which a truncated file still reports at full value). This is the
    ground truth for "did the remux keep all the content." Returns None if
    it can't be determined.
    """
    try:
        proc = subprocess.run(
            [ffprobe, "-v", "error", "-select_streams", "v:0",
             "-count_packets", "-show_entries", "stream=nb_read_packets",
             "-of", "csv=p=0", path],
            capture_output=True, encoding="utf-8", errors="replace",
            timeout=600, startupinfo=_startupinfo())
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    raw = (proc.stdout or "").strip().splitlines()
    if not raw:
        return None
    try:
        return int(raw[0].strip())
    except (ValueError, TypeError):
        return None


def _embed_one(ffmpeg: str, path: str, video_id: str, title: str,
               channel: str) -> tuple[bool, str]:
    """Remux one file with provenance tags. Returns (ok, error)."""
    try:
        st = os.stat(path)
    except OSError as e:
        return False, f"stat failed: {e}"
    date_iso = time.strftime("%Y-%m-%d", time.localtime(st.st_mtime))
    url = f"https://www.youtube.com/watch?v={video_id}"
    tmp = path + _TMP_SUFFIX
    cmd = [ffmpeg, "-v", "error", "-y",
           "-i", path,
           "-map", "0", "-c", "copy",
           "-movflags", "+faststart",
           "-metadata", f"title={title}" if title else "title=",
           "-metadata", f"artist={channel}" if channel else "artist=",
           "-metadata", f"date={date_iso}",
           "-metadata", f"comment={url}",
           tmp]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, encoding="utf-8", errors="replace",
            timeout=1800, startupinfo=_startupinfo())
    except (OSError, subprocess.TimeoutExpired) as e:
        try: os.remove(tmp)
        except OSError: pass
        return False, f"ffmpeg spawn/timeout: {e}"
    if proc.returncode != 0:
        try: os.remove(tmp)
        except OSError: pass
        err = (proc.stderr or "").strip().splitlines()
        return False, f"ffmpeg rc={proc.returncode}: {err[-1] if err else '?'}"
    try:
        new_size = os.path.getsize(tmp)
        lo = _SIZE_MIN_RATIO * st.st_size
        hi = _SIZE_MAX_RATIO * st.st_size + 4_000_000
        if not (lo <= new_size <= hi):
            # Outside the fast-path band. Don't fail on size alone — a -c copy
            # can shrink well past 5% by reclaiming container padding with no
            # data loss (seen on AV1 mp4s: identical streams, MBs of free/
            # interleave overhead removed). Verify with the ground truth:
            # the count of ACTUAL demuxed video packets. A stream copy keeps
            # every packet, so src and output counts match to the frame; a
            # real truncation drops thousands. (Declared frame count / moov
            # duration is NOT usable here — a truncated file still reports
            # its full declared values.) The reprieve is shrink-side only; a
            # copy that GREW past the upper bound stays suspect regardless.
            ffprobe = _find_ffprobe(ffmpeg)
            src_pkts = _count_video_packets(ffprobe, path) if ffprobe else None
            out_pkts = _count_video_packets(ffprobe, tmp) if ffprobe else None
            pkts_ok = (src_pkts is not None and out_pkts is not None
                       and abs(src_pkts - out_pkts) <= max(1, src_pkts // 1000))
            if not (pkts_ok and new_size < lo):
                try: os.remove(tmp)
                except OSError: pass
                if ffprobe is None:
                    return False, (f"size sanity failed, ffprobe unavailable "
                                   f"to verify ({st.st_size} -> {new_size} "
                                   f"bytes)")
                if src_pkts is None or out_pkts is None:
                    return False, (f"size sanity failed, packet count "
                                   f"unverifiable ({st.st_size} -> "
                                   f"{new_size} bytes)")
                return False, (f"size+content sanity failed ({st.st_size} -> "
                               f"{new_size} bytes, {src_pkts} -> {out_pkts} "
                               f"video packets)")
            _log.info("provenance: %s shrank %d -> %d bytes but all %d video "
                      "packets preserved — reclaimed container padding, "
                      "accepting", os.path.basename(path),
                      st.st_size, new_size, out_pkts)
        # Original mtime IS the upload date — must survive the swap.
        os.utime(tmp, (st.st_atime, st.st_mtime))
        os.replace(tmp, path)
    except OSError as e:
        try: os.remove(tmp)
        except OSError: pass
        return False, f"finalize failed: {e}"
    return True, ""


def _sweep_stale_tmp(ch_dir: str) -> int:
    """Remove orphaned *.prov.tmp.mp4 from a crashed/cancelled prior run."""
    n = 0
    for dirpath, _dns, fns in os.walk(ch_dir):
        for fn in fns:
            if fn.endswith(_TMP_SUFFIX):
                try:
                    os.remove(os.path.join(dirpath, fn))
                    n += 1
                except OSError as e:
                    swallow("provenance tmp sweep", e)
    return n


# ─── Entry point (sync-queue task body) ────────────────────────────────

def embed_provenance_archive(output_dir: str,
                             channel_folder: str | None = None,
                             do_txt: bool = True,
                             do_mp4: bool = True,
                             dry_run: bool = False,
                             log_stream=None,
                             cancel_event=None,
                             pause_event=None,
                             queues=None,
                             scope_url: str | None = None,
                             db_path=None) -> dict:
    """Run the backfill. Returns counters for the sync-row summary:
    {succeeded, skipped, failed, cancelled, txt_upgraded, txt_ambiguous,
     txt_unknown, missing}."""
    stream = log_stream
    res = {"succeeded": 0, "skipped": 0, "failed": 0, "cancelled": False,
           "txt_upgraded": 0, "txt_ambiguous": 0, "txt_unknown": 0,
           "missing": 0}

    def _emit(parts):
        if stream is None:
            return
        try:
            stream.emit(parts)
        except Exception as e:
            swallow("provenance emit", e)

    def _progress(text):
        # Single in-place counter line — `provenance_progress` is
        # registered in logs.js _inplaceKind so each emit REPLACES the
        # previous one instead of appending. `simpleline` (not `dim`)
        # so it's visible in Simple log mode.
        _emit([[text + "\n", ["simpleline", "provenance_progress"]]])

    def _clear_progress():
        # Remove the sticky counter before a summary/cancel so the
        # stale "[N/M] ..." line doesn't linger after the phase ends.
        if stream is None:
            return
        try:
            import json as _json
            stream.emit([[_json.dumps({"kind": "clear_line",
                                       "marker": "provenance_progress"}),
                          "__control__"]])
        except Exception as e:
            swallow("provenance clear_line", e)

    def _cancelled() -> bool:
        if cancel_event is not None and cancel_event.is_set():
            res["cancelled"] = True
            _clear_progress()
            return True
        if wait_while_paused(pause_event, cancel_event, stream=stream,
                             label="Embed file tags", queues=queues,
                             tick=0.5):
            res["cancelled"] = True
            _clear_progress()
            return True
        return False

    output_dir = os.path.normpath((output_dir or "").strip())
    if not output_dir or not os.path.isdir(output_dir):
        raise RuntimeError(f"archive root not found: {output_dir!r}")

    # ── Phase A — txt headers ──────────────────────────────────────
    if do_txt:
        ch_dirs = _channel_dirs(output_dir, channel_folder)
        _emit([["  — ", ["simpleline_pink"]],
               [f"Adding video IDs to Transcript.txt headers "
                f"({len(ch_dirs)} channel folder"
                f"{'s' if len(ch_dirs) != 1 else ''})"
                f"{' [dry-run]' if dry_run else ''}\n", ["simpleline"]]])
        for ci, ch_dir in enumerate(ch_dirs):
            if _cancelled():
                return res
            if queues is not None:
                try: queues.set_sync_pass_progress(ci, len(ch_dirs))
                except Exception as e: swallow("provenance progress", e)
            _progress(f"    [{ci + 1}/{len(ch_dirs)}] "
                      f"{os.path.basename(ch_dir)} — "
                      f"{res['txt_upgraded']:,} IDs added so far")
            for dirpath, dns, fns in os.walk(ch_dir):
                dns[:] = [d for d in dns if not d.startswith(".")]
                for fn in fns:
                    if not fn.endswith("Transcript.txt"):
                        continue
                    if _cancelled():
                        return res
                    try:
                        st = _upgrade_txt_file(
                            os.path.join(dirpath, fn), dry_run)
                        res["txt_upgraded"] += st["upgraded"]
                        res["txt_ambiguous"] += st["ambiguous"]
                        res["txt_unknown"] += st["unknown"]
                    except Exception as e:
                        res["failed"] += 1
                        _emit([["  — ", ["dim"]],
                               [f"header upgrade failed for {fn}: {e}\n",
                                ["red"]]])
        _clear_progress()
        _emit([["  — ", ["simpleline_pink"]],
               [f"Headers: {res['txt_upgraded']:,} upgraded · "
                f"{res['txt_ambiguous']:,} ambiguous · "
                f"{res['txt_unknown']:,} no known ID"
                f"{' [dry-run]' if dry_run else ''}\n",
                ["simpleline"]]])

    # ── Phase B — mp4 tags ─────────────────────────────────────────
    if do_mp4:
        from .compress import find_ffmpeg
        ffmpeg = find_ffmpeg()
        if not ffmpeg:
            raise RuntimeError("ffmpeg not found — cannot embed MP4 tags")
        for ch_dir in _channel_dirs(output_dir, channel_folder):
            _sweep_stale_tmp(ch_dir)
        work = _mp4_worklist(output_dir, channel_folder, db_path=db_path)
        ledger = _load_ledger()
        _emit([["  — ", ["simpleline_pink"]],
               [f"Embedding tags in {len(work):,} known-ID "
                f"videos{' [dry-run]' if dry_run else ''}\n",
                ["simpleline"]]])
        ledger_fh = None
        if not dry_run:
            try:
                LEDGER_FILE.parent.mkdir(parents=True, exist_ok=True)
                ledger_fh = open(LEDGER_FILE, "a", encoding="utf-8")
            except OSError as e:
                swallow("provenance ledger open", e)
        t_last_emit = time.time()
        try:
            for wi, (fp, vid, title, channel) in enumerate(work):
                if _cancelled():
                    return res
                if queues is not None:
                    try: queues.set_sync_pass_progress(wi, len(work))
                    except Exception as e: swallow("provenance progress", e)
                try:
                    st = os.stat(fp)
                except OSError:
                    res["missing"] += 1
                    continue
                led = ledger.get(os.path.normcase(fp))
                if led and led == (int(st.st_size), int(st.st_mtime)):
                    res["skipped"] += 1
                    continue
                if dry_run:
                    res["succeeded"] += 1
                    continue
                ok, err = _embed_one(ffmpeg, fp, vid, title, channel)
                if ok:
                    res["succeeded"] += 1
                    try:
                        st2 = os.stat(fp)
                        if ledger_fh is not None:
                            _ledger_append(ledger_fh, fp, st2.st_size,
                                           st2.st_mtime)
                    except OSError as e:
                        swallow("provenance ledger stat", e)
                else:
                    res["failed"] += 1
                    _log.error("provenance embed failed for %s: %s", fp, err)
                    if res["failed"] <= 20:
                        _emit([["  — ", ["dim"]],
                               [f"{os.path.basename(fp)}: {err}\n", ["red"]]])
                # In-place counter — every 10 files or 2s, whichever
                # comes first (LogStreamer batches at 60ms, so this is
                # cheap; the line replaces itself via the
                # provenance_progress marker).
                now = time.time()
                if ((wi + 1) % 10 == 0 or (now - t_last_emit) >= 2
                        or wi + 1 == len(work)):
                    t_last_emit = now
                    _progress(f"    [{wi + 1:,}/{len(work):,}] "
                              f"Embedding tags — "
                              f"{res['succeeded']:,} tagged · "
                              f"{res['skipped']:,} already done · "
                              f"{res['failed']} failed")
        finally:
            if ledger_fh is not None:
                try: ledger_fh.close()
                except OSError: pass
        _clear_progress()
        _emit([["  — ", ["simpleline_pink"]],
               [f"Tags: {res['succeeded']:,} tagged · "
                f"{res['skipped']:,} already done · "
                f"{res['missing']:,} files missing · "
                f"{res['failed']} failed"
                f"{' [dry-run]' if dry_run else ''}\n",
                ["simpleline"]]])

    if queues is not None:
        try: queues.set_sync_pass_progress(0, 0)
        except Exception as e: swallow("provenance progress reset", e)
    return res
