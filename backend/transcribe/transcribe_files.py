"""
transcribe.transcribe_files — aggregated .txt / .jsonl writers.

Patch 19 phase T1 (v68.9): extracted from transcribe/legacy.py.

Functions write the per-entry blocks for the aggregated
`<channel> Transcript.txt` and the hidden `.<channel> Transcript.jsonl`
sidecars. These are the OLD-YTArchiver-compatible file formats — bytes
on disk must match the legacy format exactly so existing archives
remain readable.

Public surface (re-imported into legacy.py for back-compat):
    _write_jsonl_entry      append long-form JSONL entries
    _write_transcript_entry append a formatted .txt block
    _replace_jsonl_entry    surgically swap one video's entries
    _replace_txt_entry      surgically swap one video's .txt block
    _HEADER_RE              regex matching the .txt entry header
"""
from __future__ import annotations

import json
import os
import re
import threading as _threading

from ..log import get_logger
from ..transcribe_paths import (
    _format_duration_hms,
    _format_upload_date,
    _generate_distributed_words,
    _hide_file_win,
)
from ..utils import unhide_file_win as _unhide_file_win

# Per-path locks for the aggregated Transcript.txt writers. drift_scan's
# reconstruction does a read→append→os.replace of the SAME files from a
# different thread; without shared serialization, its snapshot-replace
# silently erased any entry a transcribe worker appended in between.
_TXT_LOCKS: dict[str, _threading.RLock] = {}
_TXT_LOCKS_GUARD = _threading.Lock()


def txt_lock_for(path: str) -> _threading.RLock:
    """Process-wide lock for one aggregated .txt path (normcase'd)."""
    key = os.path.normcase(os.path.normpath(os.path.abspath(path or "")))
    with _TXT_LOCKS_GUARD:
        lk = _TXT_LOCKS.get(key)
        if lk is None:
            lk = _threading.RLock()
            _TXT_LOCKS[key] = lk
        return lk

_log = get_logger(__name__)


def _norm_title(s: str) -> str:
    """Thin alias for text_utils.normalize_title — used by the
    title-keyed match logic in _replace_jsonl_entry / _replace_txt_entry.
    """
    from ..text_utils import normalize_title
    return normalize_title(s)


def _seg_to_jsonl_line(video_id: str, title: str, seg: dict) -> str:
    """Serialize ONE transcript segment to a single canonical .jsonl line
    (trailing newline included). Single source of truth for the on-disk
    format so the append path (_write_jsonl_entry) and the rewrite path
    (_replace_jsonl_entry) can't drift byte-for-byte (audit: transcribe_files
    duplicated serializer). Accepts short-form (s/e/t/w) or long-form
    (start/end/text/words) segment dicts.
    """
    s = seg.get("start") if "start" in seg else seg.get("s", 0.0)
    e = seg.get("end") if "end" in seg else seg.get("e", 0.0)
    t = seg.get("text") if "text" in seg else seg.get("t", "")
    raw_words = seg.get("words") if "words" in seg else seg.get("w")
    entry = {
        "video_id": video_id or "",
        "title": title,
        "start": round(float(s or 0), 2),
        "end": round(float(e or 0), 2),
        "text": t or "",
    }
    # Distinguish None (key absent — generate words) from [] (key
    # explicit-empty — respect Whisper Branch 3 intent)
    # (audit: transcribe_files.py:67-89).
    if raw_words is None:
        entry["words"] = _generate_distributed_words(
            entry["text"], entry["start"], entry["end"])
    elif raw_words:
        # word records stay short-form ("w"/"s"/"e"), same as on-disk.
        entry["words"] = [
            {"w": w.get("w") if isinstance(w, dict) else str(w),
             "s": round(float((w.get("s") if isinstance(w, dict) else 0) or 0), 3),
             "e": round(float((w.get("e") if isinstance(w, dict) else 0) or 0), 3)}
            for w in raw_words
        ]
    else:
        entry["words"] = _generate_distributed_words(
            entry["text"], entry["start"], entry["end"])
    return json.dumps(entry, ensure_ascii=False) + "\n"


def _write_jsonl_entry(jsonl_path: str, video_id: str, title: str,
                       segments: list[dict]) -> None:
    """Append long-form JSONL entries for one video. Matches YTArchiver.py:8508.

    Each line:
      {"video_id":..., "title":..., "start":..., "end":...,
       "text":..., "words":[{"w","s","e"}, ...]}

    Note: segments from NEW's internal format use short keys {s,e,t,w}. This
    helper accepts EITHER short-form or long-form keys and always writes
    long-form to disk.
    """
    try:
        _jsonl_dir = os.path.dirname(jsonl_path)
        if _jsonl_dir:
            os.makedirs(_jsonl_dir, exist_ok=True)

        # Build lines in memory so a disk failure mid-write doesn't leave
        # half-a-line on disk.
        new_lines = [_seg_to_jsonl_line(video_id, title, seg)
                     for seg in segments]

        # atomic write via .tmp + os.replace. Previously
        # the function opened in append mode ("a") and ALSO ran a torn-
        # last-line repair on every call because append wasn't atomic.
        # Now: read existing content (if any), build full new content
        # in memory, write to .tmp, fsync, atomic replace. No torn-write
        # repair needed because every replace lands a complete file.
        existing = b""
        if os.path.isfile(jsonl_path):
            try:
                with open(jsonl_path, "rb") as _f:
                    existing = _f.read()
                # Clear Windows hidden so we can write (re-hidden below).
                _unhide_file_win(os.path.normpath(jsonl_path))
            except OSError as e:
                _log.debug("swallowed: %s", e)
                existing = b""

        # If the existing file's last line is missing a trailing newline
        # (legacy torn write from before this fix), prepend one before
        # appending the new lines so the result is still line-valid.
        if existing and not existing.endswith(b"\n"):
            existing = existing + b"\n"

        new_bytes = existing + "".join(new_lines).encode("utf-8")
        tmp = jsonl_path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(new_bytes)
            try:
                f.flush()
                os.fsync(f.fileno())
            except OSError as e:
                _log.debug("swallowed: %s", e)
        # Hide tmp BEFORE replace so the file is never briefly visible
        # in Explorer between the replace and the re-hide (audit:
        # transcribe_files H58).
        try: _hide_file_win(tmp)
        except Exception: pass
        os.replace(tmp, jsonl_path)
        # Defensive re-hide after replace in case the hidden attribute
        # was lost during the rename (rare on same-volume, but happens
        # cross-volume on Windows).
        try: _hide_file_win(jsonl_path)
        except Exception: pass
    except Exception as _jse:
        # surface to module-level log so .txt/.jsonl desync
        # is diagnosable. Was a print() — routes via
        # logger so PyInstaller --noconsole builds also capture it.
        try:
            _log.error("_write_jsonl_entry failed for %s: %s",
                       os.path.basename(jsonl_path), _jse)
        except Exception:
            print(f"[transcribe] _write_jsonl_entry failed for "
                  f"{os.path.basename(jsonl_path)}: {_jse}")


def _write_transcript_entry(txt_path, *args, **kwargs):
    """Lock-serialized facade over _write_transcript_entry_unlocked —
    shares per-path locks with _replace_txt_entry and drift_scan's
    reconstruction writer."""
    with txt_lock_for(txt_path):
        return _write_transcript_entry_unlocked(txt_path, *args, **kwargs)


def _write_transcript_entry_unlocked(txt_path: str, title: str,
                            upload_date: str, duration_secs: float,
                            source_tag: str, text: str) -> bool:
    """Append one formatted block to the aggregated Transcript.txt.
    Format (YTArchiver.py:15458):
      ===(title), (MM.DD.YYYY), (H:MM:SS), (SOURCE)===
      {text}
      [triple newline]

    Atomic write: read existing content, append the new entry in memory,
    write to <path>.tmp with fsync, then os.replace onto the final path.
    The previous open(path, "a") pattern could leave a partially-flushed
    final entry on crash mid-write — the torn header at EOF wouldn't
    parse cleanly on the next read.
    """
    try:
        os.makedirs(os.path.dirname(txt_path), exist_ok=True)
        date_fmt = _format_upload_date(upload_date or "")
        dur_raw = _format_duration_hms(duration_secs or 0) or ""
        dur_fmt = f"({dur_raw})" if dur_raw else "(Unknown length)"
        src_fmt = source_tag if source_tag.startswith("(") else f"({source_tag})"
        entry = f"===({title}), {date_fmt}, {dur_fmt}, {src_fmt}===\n{text}\n\n\n"
        # Read existing content (file may not exist yet on first transcribe).
        # `errors="replace"` so a partially corrupt UTF-8 byte sequence
        # doesn't UnicodeDecodeError and leave the file broken for
        # subsequent appends — matches _replace_txt_entry's read mode
        # (audit: transcribe_files H43).
        try:
            with open(txt_path, "r", encoding="utf-8", errors="replace") as f:
                existing = f.read()
        except FileNotFoundError:
            existing = ""
        new_content = existing + entry
        tmp = txt_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(new_content)
                try:
                    f.flush()
                    os.fsync(f.fileno())
                except OSError as e:
                    _log.debug("swallowed: %s", e)
            os.replace(tmp, txt_path)
        except OSError:
            try: os.remove(tmp)
            except OSError: pass
            raise
        return True
    except Exception:
        return False


# Header pattern for the per-entry "===(title), (date), (duration), (source)==="
# line in the aggregated Transcript.txt. Captures title (group 1), date
# (group 2), duration (group 3), source tag (group 4). Matches OLD
# YTArchiver.py:28997 `_HEADER_RE`.
# Title uses `.+?` (non-greedy) so YT titles containing `)` (e.g.
# "How I made $1M (in one year)") are not truncated at the first paren.
# The trailing `,\s*\(` anchor guarantees correct termination at the
# date field. Date/dur/src groups stay restrictive because they're
# emitted by our own writers and won't contain `)`.
_HEADER_RE = re.compile(
    r'^===\((.+?)\),\s*(\([^)]*\)),\s*(\([^)]*\)),\s*(\([^)]*\))===',
    re.MULTILINE)


def _replace_jsonl_entry(jsonl_path: str, title: str, video_id: str,
                         new_segments: list[dict]) -> set:
    """Surgically swap this video's entries in the aggregated .jsonl.

    `_replace_jsonl_entry` — used by the
    retranscribe flow to replace the old auto-captions / older-Whisper
    entries with the newly-transcribed ones WITHOUT blowing away the
    other videos that share the aggregated file.

    Matches on BOTH title AND video_id — catches the case where a title
    drifted between transcriptions (e.g. YouTube normalized "huge
    change.." → "huge change..." after the first auto-caption pass).
    Returns the set of distinct titles that were removed so the caller
    can feed them into `_replace_txt_entry` for the same cleanup on the
    .txt side.
    """
    # Clear Windows hidden/readonly so we can write. The re-hide is
    # in a try/finally below so the sidecar can never get stranded
    # visible — even if any step between unhide and the final hide
    # raises (read failure, build error, disk full, AV-locked rename).
    # Violating the "hidden sidecars" invariant would expose internals
    # to the user's archive view permanently.
    # Skip chmod if the file doesn't exist yet — a first-time
    # retranscribe targets a path the writer is about to create, so
    # chmoding raises FileNotFoundError. Skip cleanly instead of
    # swallowing a confusing OSError (audit: transcribe_files.py:
    # 188-194). The hide/unhide pair only matters when the file
    # already exists.
    _jsonl_abs = os.path.normpath(jsonl_path)
    if os.path.exists(_jsonl_abs):
        _unhide_file_win(_jsonl_abs)
        if os.name == "nt":
            try:
                import stat
                os.chmod(jsonl_path, stat.S_IWRITE | stat.S_IREAD)
            except FileNotFoundError:
                # Raced with another writer that deleted the file —
                # rare. Carry on; the writer below will recreate it.
                pass
            except Exception as e:
                _log.debug("swallowed: %s", e)

    try:
        old_lines: list[str] = []
        try:
            with open(jsonl_path, "r", encoding="utf-8") as f:
                old_lines = f.readlines()
        except FileNotFoundError:
            pass

        kept: list[str] = []
        removed_titles: set = set()
        vid_norm = (video_id or "").strip()
        tit_key = _norm_title(title)
        for line in old_lines:
            ls = line.strip()
            if not ls:
                continue
            try:
                obj = json.loads(ls)
                seg_title = (obj.get("title") or "").strip()
                seg_vid = (obj.get("video_id") or "").strip()
                # Match by video_id, or by normalized title ONLY when id
                # disambiguation is impossible (the line carries no id,
                # or we don't know our own). The old title-OR-id match
                # purged segments of a DIFFERENT video that legitimately
                # shared the title ('Q&A', 'LIVE', weekly shows) —
                # silent transcript loss that drift_scan can't detect
                # because both sidecars stayed mutually consistent.
                # (The .txt side still purges by title alone — its
                # headers carry no ids — but with the .jsonl preserved,
                # a lost .txt block is recoverable via Drift Scan's
                # rebuild instead of being gone forever.)
                _title_hit = bool(seg_title) and _norm_title(seg_title) == tit_key
                _id_hit = bool(vid_norm) and seg_vid == vid_norm
                if _id_hit or (_title_hit and (not seg_vid or not vid_norm)):
                    if seg_title:
                        removed_titles.add(seg_title)
                    continue # drop this line
            except Exception as e:
                _log.debug("swallowed: %s", e)
            kept.append(line if line.endswith("\n") else line + "\n")

        # build the new segments inline and write the
        # filtered-kept lines + new lines in ONE atomic operation. Previously
        # this function wrote kept lines, then called _write_jsonl_entry which
        # re-read the file from disk and rewrote it — two reads + two writes
        # for an operation that only needs one of each.
        new_lines = [_seg_to_jsonl_line(video_id, title, seg)
                     for seg in new_segments]

        # If kept's last entry is missing a trailing newline, fix before append.
        if kept and not kept[-1].endswith("\n"):
            kept[-1] = kept[-1] + "\n"

        final_bytes = ("".join(kept) + "".join(new_lines)).encode("utf-8")
        tmp = jsonl_path + ".tmp"
        try:
            with open(tmp, "wb") as f:
                f.write(final_bytes)
                try:
                    f.flush()
                    os.fsync(f.fileno())
                except OSError as e:
                    _log.debug("swallowed: %s", e)
            # Hide tmp BEFORE the replace so the jsonl is never
            # briefly visible in Explorer (audit: transcribe_files H58).
            try: _hide_file_win(tmp)
            except Exception: pass
            os.replace(tmp, jsonl_path)
        except OSError as _oe:
            # previously returned early WITHOUT re-appending the
            # new segments, silently leaving the OLD entry on disk while the
            # caller thought retranscribe succeeded. Now we re-raise so the
            # caller's emit_error in _write_outputs surfaces the failure and
            # the user can see that their retranscribe didn't land.
            try: os.remove(tmp)
            except OSError: pass
            _log.error("_replace_jsonl_entry atomic replace failed: %s", _oe)
            raise
    finally:
        # Always restore the hidden attribute, even on failure. If the
        # file was deleted by an earlier step or never existed, this is
        # a no-op. Best-effort — never let a re-hide failure mask the
        # real exception above.
        try:
            _hide_file_win(jsonl_path)
        except Exception as e:
            _log.debug("swallowed: %s", e)

    return removed_titles


def _replace_txt_entry(txt_path, *args, **kwargs):
    """Lock-serialized facade over _replace_txt_entry_unlocked — see
    txt_lock_for."""
    with txt_lock_for(txt_path):
        return _replace_txt_entry_unlocked(txt_path, *args, **kwargs)


def _replace_txt_entry_unlocked(txt_path: str, title: str, new_text: str,
                       source_tag: str,
                       extra_titles_to_remove=None) -> bool:
    """Surgically swap this video's `===(…)===\\n<body>\\n\\n\\n` block in
    the aggregated Transcript.txt. `_replace_txt_entry`.

    `extra_titles_to_remove` is the set returned by `_replace_jsonl_entry`
    — additional titles discovered via video_id match. Passing them here
    lets the .txt pass remove stale title-drifted entries consistently.

    `source_tag` can be "(WHISPER:small)" or the bare model name; stored
    verbatim as the 4th bracketed field on the header line so the
    ArchivePlayer / Browse source banner can detect it.

    Returns True on success. On failure, raises — the caller in
    transcribe.core._write_outputs catches the exception and runs the
    .jsonl roll-back to keep the two sidecars in sync. The previous
    bare `except Exception: return False` here silently swallowed the
    failure so the caller's roll-back branch never fired, leaving the
    user with a new .jsonl + an old .txt (split state).
    """
    try:
        with open(txt_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except FileNotFoundError:
        content = ""

    # Build purge set as NORMALIZED keys (NFC + lowercase +
    # whitespace-collapsed + trailing-punct stripped). Without this
    # the check below misses "Title." vs "Title" variants that the
    # retranscribe flow legitimately needs to swap out — which is
    # what caused the triple-block duplication in v47.6 and older.
    purge = {_norm_title(t) for t in (extra_titles_to_remove or ())}
    purge.add(_norm_title(title))
    purge.discard("")

    # Remove each matching entry (header line through the next header
    # or EOF). Iterate from the end so earlier match positions stay
    # valid as we slice. Capture date+duration from the FIRST removed
    # entry so the new block inherits the provenance.
    matches = list(_HEADER_RE.finditer(content))
    new_content = content
    date_fmt = "(Unknown date)"
    dur_fmt = "(Unknown length)"
    captured = False
    for i in range(len(matches) - 1, -1, -1):
        m = matches[i]
        entry_key = _norm_title(m.group(1))
        if entry_key not in purge:
            continue
        end = matches[i + 1].start() if i + 1 < len(matches) else len(new_content)
        if not captured:
            # Matches group indices of _HEADER_RE: (title, date, dur, src)
            date_fmt = m.group(2)
            dur_fmt = m.group(3)
            captured = True
        new_content = new_content[:m.start()] + new_content[end:]

    src_fmt = source_tag if source_tag.startswith("(") else f"({source_tag})"
    new_entry = f"===({title}), {date_fmt}, {dur_fmt}, {src_fmt}===\n{new_text}\n\n\n"

    new_content = new_content.rstrip("\n") + "\n\n\n" if new_content.strip() else ""
    new_content += new_entry

    # Assert absolute path so a misrouted bare filename doesn't silently
    # write into cwd (audit: transcribe_files.py:351). Background worker
    # threads inherit cwd from main(), which on a shortcut-launched
    # frozen exe isn't where the user expects.
    if not os.path.isabs(txt_path):
        raise ValueError(
            f"_replace_txt_entry refusing non-absolute txt_path: {txt_path}")
    os.makedirs(os.path.dirname(txt_path) or ".", exist_ok=True)
    tmp = txt_path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(new_content)
        os.replace(tmp, txt_path)
    except OSError:
        # Clean up partial tmp so the next attempt doesn't see stale data.
        try: os.remove(tmp)
        except OSError: pass
        raise
    return True
