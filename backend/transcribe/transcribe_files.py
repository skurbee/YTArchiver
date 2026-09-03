"""
transcribe.transcribe_files — aggregated .txt / .jsonl writers.

Functions write the per-entry blocks for the aggregated
`<channel> Transcript.txt` and the hidden `.<channel> Transcript.jsonl`
sidecars. These are established on-disk formats; bytes must remain stable so
existing archives
remain readable.

Public surface (re-exported through the transcribe package):
    _write_jsonl_entry      append long-form JSONL entries
    _write_transcript_entry append a formatted .txt block
    _replace_jsonl_entry    surgically swap one video's entries
    _replace_txt_entry      surgically swap one video's .txt block
    _HEADER_RE              regex matching the .txt entry header
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading as _threading

from ..log import get_logger
from ..services.sidecar_store import (
    SidecarError,
    atomic_write_bytes,
    atomic_write_text,
    fsync_directory,
    read_bytes,
    read_text,
    validate_jsonl_bytes,
)
from ..utils import unhide_file_win as _unhide_file_win
from .paths import (
    _format_duration_hms,
    _format_upload_date,
    _generate_distributed_words,
    _hide_file_win,
)

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


def _seg_to_jsonl_record(video_id: str, title: str,
                         seg: dict) -> dict[str, object]:
    """Return the canonical on-disk record for one transcript segment.

    The same object is serialized to the aggregate JSONL and handed to the
    guarded per-video index updater. Keeping the normalization here prevents
    the database from retaining raw Whisper timestamps/words that differ from
    the durable sidecar representation.
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
    return entry


def _seg_to_jsonl_line(video_id: str, title: str, seg: dict) -> str:
    """Serialize ONE transcript segment to a canonical JSONL line.

    Accepts short-form (s/e/t/w) or long-form (start/end/text/words) segment
    dictionaries. Append and replacement writers both route through this
    helper so the established sidecar bytes remain stable.
    """
    entry = _seg_to_jsonl_record(video_id, title, seg)
    return json.dumps(entry, ensure_ascii=False) + "\n"


def _jsonl_generation(path: str, *, exists: bool | None = None) -> dict:
    """Capture the file identity fields used by an incremental-index receipt."""
    if exists is False:
        return {"exists": False, "mtime": 0.0, "mtime_ns": 0, "size": 0}
    try:
        info = os.stat(path)
    except FileNotFoundError:
        return {"exists": False, "mtime": 0.0, "mtime_ns": 0, "size": 0}
    return {
        "exists": True,
        "mtime": float(info.st_mtime),
        "mtime_ns": int(info.st_mtime_ns),
        "size": int(info.st_size),
    }


def _searchable_jsonl_record(record: dict) -> bool:
    """Match index.ingest_jsonl's rule that blank transcript text is skipped."""
    value = record.get("text") if "text" in record else record.get("t", "")
    return isinstance(value, str) and bool(value.strip())


def _valid_jsonl_bytes(payload: bytes) -> bool:
    """Return True when *payload* is a complete UTF-8 JSONL document."""
    try:
        validate_jsonl_bytes(payload)
    except SidecarError:
        return False
    return True


def _next_jsonl_recovery_path(tmp: str) -> str:
    """Return a non-existent path that preserves a conflicting temp file."""
    candidate = tmp + ".recovery"
    suffix = 2
    while os.path.exists(candidate):
        candidate = f"{tmp}.recovery.{suffix}"
        suffix += 1
    return candidate


def _recover_stale_jsonl_tmp(jsonl_path: str) -> bool:
    """Recover a complete stale atomic-write temp before the next update.

    A failed ``os.replace`` can leave ``<jsonl>.tmp`` behind after it has
    already been marked hidden. Reopening that fixed temp name with ``wb``
    then fails with ``PermissionError`` on Windows. A valid temp that extends
    the current JSONL is safe to promote. Any other temp is moved aside under
    a hidden recovery name so it is preserved for inspection instead of being
    truncated or deleted.
    """
    tmp = jsonl_path + ".tmp"
    if not os.path.isfile(tmp):
        return False

    _unhide_file_win(os.path.normpath(tmp))
    _unhide_file_win(os.path.normpath(jsonl_path))
    current_snapshot = read_bytes(jsonl_path)
    current = current_snapshot.data
    pending_snapshot = read_bytes(tmp)
    if not pending_snapshot.exists:
        return False
    pending = pending_snapshot.data

    # A stale stage may only replace an existing aggregate after both the old
    # generation and proposed generation validate completely.  A corrupt old
    # file is never treated as an empty/prefix document.
    if current_snapshot.exists:
        validate_jsonl_bytes(current, require_trailing_newline=False)

    if (len(pending) >= len(current)
            and pending.startswith(current)
            and _valid_jsonl_bytes(pending)):
        os.replace(tmp, jsonl_path)
        fsync_directory(os.path.dirname(jsonl_path) or ".")
        _hide_file_win(jsonl_path)
        _log.warning("Recovered complete stale JSONL temp for %s",
                     os.path.basename(jsonl_path))
        return True

    recovery = _next_jsonl_recovery_path(tmp)
    os.replace(tmp, recovery)
    fsync_directory(os.path.dirname(recovery) or ".")
    _hide_file_win(recovery)
    _log.error("Preserved conflicting stale JSONL temp as %s",
               os.path.basename(recovery))
    return False


def _write_jsonl_entry(jsonl_path: str, video_id: str, title: str,
                       segments: list[dict]) -> bool:
    """Lock-serialized facade for the aggregated JSONL append writer."""
    with txt_lock_for(jsonl_path):
        return _write_jsonl_entry_unlocked(
            jsonl_path, video_id, title, segments)


def _write_jsonl_entry_unlocked(jsonl_path: str, video_id: str, title: str,
                                segments: list[dict]) -> bool:
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

        # Promote a complete append left behind by an earlier failed replace
        # before reading the current document. On Windows, a stale hidden temp
        # otherwise makes open(..., "wb") fail with EACCES.
        _recover_stale_jsonl_tmp(jsonl_path)

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
        snapshot = read_bytes(jsonl_path)
        existing = snapshot.data
        if snapshot.exists:
            # Validate every old line before deriving a replacement. This is
            # intentionally stricter than the read-only viewers: a malformed
            # aggregate must be repaired, not silently normalized by append.
            validate_jsonl_bytes(existing, require_trailing_newline=False)
            _unhide_file_win(os.path.normpath(jsonl_path))

        # Appending is the normal first-write path, but it must also be safe to
        # repeat after a process dies between the paired TXT/JSONL commits. A
        # stable video id makes an existing entry authoritative: replace every
        # segment for that id instead of appending a second copy. This also
        # covers retries whose punctuation/text changed slightly, where a mere
        # payload-suffix check would not recognize the prior commit.
        vid_norm = (video_id or "").strip()
        if vid_norm:
            for raw_line in existing.splitlines():
                try:
                    obj = json.loads(raw_line)
                except (TypeError, ValueError):
                    continue
                if (isinstance(obj, dict)
                        and (obj.get("video_id") or "").strip() == vid_norm):
                    _replace_jsonl_entry_unlocked(
                        jsonl_path, title, video_id, segments)
                    return True

        # If the existing file's last line is missing a trailing newline
        # (legacy torn write from before this fix), prepend one before
        # appending the new lines so the result is still line-valid.
        if existing and not existing.endswith(b"\n"):
            existing = existing + b"\n"

        new_payload = "".join(new_lines).encode("utf-8")
        # If the prior call wrote a complete temp but failed only at
        # os.replace, stale-temp recovery above has just promoted exactly this
        # payload. Treat the retry as complete instead of appending it twice.
        if new_payload and existing.endswith(new_payload):
            return True

        new_bytes = existing + new_payload
        atomic_write_bytes(
            jsonl_path,
            new_bytes,
            validator=validate_jsonl_bytes,
            before_replace=_hide_file_win,
            after_replace=_hide_file_win,
            stage_path=jsonl_path + ".tmp",
            preserve_stage_on_replace_error=True,
        )
        return True
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
        return False
    finally:
        # Reading an existing sidecar requires clearing HIDDEN on Windows.
        # Restore the archive invariant even when recovery or replace fails.
        try:
            _hide_file_win(jsonl_path)
        except Exception as e:
            _log.debug("swallowed: %s", e)


def _write_transcript_entry(txt_path, *args, **kwargs):
    """Lock-serialized facade over _write_transcript_entry_unlocked —
    shares per-path locks with _replace_txt_entry and drift_scan's
    reconstruction writer."""
    with txt_lock_for(txt_path):
        return _write_transcript_entry_unlocked(txt_path, *args, **kwargs)


def _write_transcript_entry_unlocked(txt_path: str, title: str,
                            upload_date: str, duration_secs: float,
                            source_tag: str, text: str,
                            video_id: str = "") -> bool:
    """Append one formatted block to the aggregated Transcript.txt.
    Format (YTArchiver.py:15458, +v80 url field):
      ===(title), (MM.DD.YYYY), (H:MM:SS), (SOURCE), (youtu.be/<id>)===
      {text}
      [triple newline]
    The url field is omitted when `video_id` is empty/malformed, which
    keeps the legacy 4-field shape for unknown-id videos.

    Atomic write: read existing content, append the new entry in memory,
    write to a same-directory stage with fsync, then os.replace onto the final path.
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
        entry = (f"===({title}), {date_fmt}, {dur_fmt}, {src_fmt}"
                 f"{_header_url_field(video_id)}===\n{text}\n\n\n")
        # Read existing content (file may not exist yet on first transcribe).
        # Missing is a safe first write. Any other read/decode failure stops
        # the operation so old transcript bytes cannot be replaced with a
        # lossy ``errors='replace'`` decode.
        existing = read_text(txt_path).text

        # A retry after the TXT commit but before JSONL/index completion must
        # not append another block. Prefer the v2 header's stable video id; for
        # legacy/no-id output, recognizing an identical trailing block still
        # makes an exact crash retry idempotent.
        vid_norm = (video_id or "").strip()
        if vid_norm and any(
                (m.group(5) or "").strip() == vid_norm
                for m in _HEADER_RE.finditer(existing)):
            return _replace_txt_entry_unlocked(
                txt_path, title, text, source_tag,
                video_id=vid_norm,
                upload_date=upload_date,
                duration_secs=duration_secs)
        if entry and existing.endswith(entry):
            return True
        new_content = existing + entry
        atomic_write_text(txt_path, new_content)
        return True
    except Exception:
        return False


# Header pattern for the per-entry header line in the aggregated
# Transcript.txt. Two on-disk generations exist:
#   v1: ===(title), (date), (duration), (source)===
#   v2: ===(title), (date), (duration), (source), (youtu.be/<id>)===
# Captures title (group 1), date (group 2), duration (group 3), source
# tag (group 4), bare video id (group 5, None on v1 headers).
# Title uses `.*` greedily so YT titles containing the field-looking
# delimiter `), (` are not truncated at the first apparent boundary.
# The (?!youtu\.be/) lookahead on the source group is what makes the
# optional 5th field unambiguous: without it, greedy-title backtracking
# parses a v2 line as a v1 line with `title), (date` glued into the
# title and the URL misread as the source tag. Date/dur/src groups stay
# restrictive because they're emitted by our own writers and won't
# contain `)`.
_HEADER_RE = re.compile(
    r'^===\((.*)\),\s*(\([^)]*\)),\s*(\([^)]*\)),\s*'
    r'(\((?!youtu\.be/)[^)]*\))'
    r'(?:,\s*\(youtu\.be/([A-Za-z0-9_-]{11})\))?===',
    re.MULTILINE)

_VIDEO_ID_RE = re.compile(r'^[A-Za-z0-9_-]{11}$')


def _header_url_field(video_id: str) -> str:
    """Return the `, (youtu.be/<id>)` header suffix, or '' when the id
    is absent/malformed. Central so the append and replace writers emit
    byte-identical fields."""
    vid = (video_id or "").strip()
    return f", (youtu.be/{vid})" if _VIDEO_ID_RE.match(vid) else ""
_BODY_WS_RE = re.compile(r"\s+")


def _body_key(text: str) -> str:
    return _BODY_WS_RE.sub(" ", (text or "").strip())


def _jsonl_text_candidates_from_bytes(data: bytes | None, title: str,
                                      video_id: str) -> set[str]:
    """Return old transcript-body candidates for one video from JSONL."""
    if not data:
        return set()
    vid_norm = (video_id or "").strip()
    title_key = _norm_title(title)
    grouped: dict[str, list[str]] = {}
    for raw in data.splitlines():
        if not raw.strip():
            continue
        try:
            obj = json.loads(raw.decode("utf-8"))
        except Exception as e:
            _log.debug("swallowed: %s", e)
            continue
        if not isinstance(obj, dict):
            continue
        seg_vid = (obj.get("video_id") or "").strip()
        seg_title = (obj.get("title") or "").strip()
        title_hit = bool(seg_title) and _norm_title(seg_title) == title_key
        id_hit = bool(vid_norm) and seg_vid == vid_norm
        if not (id_hit or (title_hit and (not seg_vid or not vid_norm))):
            continue
        key = seg_vid or seg_title or title
        grouped.setdefault(key, []).append(obj.get("text") or "")
    candidates: set[str] = set()
    for parts in grouped.values():
        joined = " ".join(p.strip() for p in parts if p.strip()).strip()
        if joined:
            candidates.add(joined)
    return candidates


def parse_transcript_header(line: str) -> tuple[str, str, str, str] | None:
    """Parse a Transcript.txt header into unwrapped fields.

    The title field is user-controlled and can contain the literal
    delimiter text `), (`. Parse from the right-hand metadata fields so
    sibling repair/punctuation code shares one boundary rule.
    """
    m = _HEADER_RE.match((line or "").strip())
    if not m:
        return None
    return (
        m.group(1).strip(),
        m.group(2).strip("()"),
        m.group(3).strip("()"),
        m.group(4).strip("()"),
    )


def _replace_jsonl_entry(jsonl_path: str, title: str, video_id: str,
                         new_segments: list[dict], *,
                         receipt_out: dict | None = None) -> set:
    """Lock-serialized facade for the aggregated JSONL replacement writer.

    ``receipt_out`` is optional and preserves the established set return API.
    On success it receives a proof of the exact base/final file generations
    and canonical replacement records. The search index uses that proof for
    its guarded fast path; ordinary callers can continue to omit it.
    """
    if receipt_out is not None:
        receipt_out.clear()
    with txt_lock_for(jsonl_path):
        try:
            return _replace_jsonl_entry_unlocked(
                jsonl_path, title, video_id, new_segments,
                receipt_out=receipt_out)
        finally:
            # Also covers recovery/unhide failures that occur before the
            # unlocked writer reaches its own try/finally block.
            try:
                _hide_file_win(jsonl_path)
            except Exception as e:
                _log.debug("swallowed: %s", e)


def _replace_jsonl_entry_unlocked(jsonl_path: str, title: str, video_id: str,
                                  new_segments: list[dict], *,
                                  receipt_out: dict | None = None) -> set:
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
    # Recover a complete append that may have been stranded by an earlier
    # failed atomic replace before this operation snapshots the JSONL.
    _recover_stale_jsonl_tmp(jsonl_path)

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
        snapshot = read_bytes(jsonl_path)
        base_generation = _jsonl_generation(
            jsonl_path, exists=snapshot.exists)
        if snapshot.exists:
            # A replacement is destructive by definition. Validate the full
            # old generation before selecting rows so malformed/non-object
            # records can never disappear as an accidental side effect.
            validate_jsonl_bytes(
                snapshot.data,
                require_trailing_newline=False,
            )
            old_text = snapshot.data.decode("utf-8-sig")
            old_lines = old_text.splitlines(keepends=True)
        else:
            old_lines = []

        kept: list[str] = []
        removed_titles: set = set()
        removed_searchable_count = 0
        removed_blank_video_id = False
        base_searchable_count = 0
        kept_searchable_count = 0
        requires_full_reingest = False
        vid_norm = (video_id or "").strip()
        tit_key = _norm_title(title)
        for line in old_lines:
            ls = line.strip()
            if not ls:
                continue
            obj = json.loads(ls)
            raw_title = obj.get("title")
            raw_video_id = obj.get("video_id")
            if raw_title is not None and not isinstance(raw_title, str):
                requires_full_reingest = True
            if raw_video_id is not None and not isinstance(raw_video_id, str):
                requires_full_reingest = True
            seg_title = (raw_title or "").strip() if isinstance(
                raw_title, str) or raw_title is None else ""
            seg_vid = (raw_video_id or "").strip() if isinstance(
                raw_video_id, str) or raw_video_id is None else ""
            searchable = _searchable_jsonl_record(obj)
            raw_text = obj.get("text") if "text" in obj else obj.get("t", "")
            if not isinstance(raw_text, str):
                requires_full_reingest = True
            if searchable:
                base_searchable_count += 1
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
                if searchable:
                    removed_searchable_count += 1
                if not seg_vid:
                    removed_blank_video_id = True
                continue # drop this line
            kept.append(line if line.endswith("\n") else line + "\n")
            if searchable:
                kept_searchable_count += 1

        # build the new segments inline and write the
        # filtered-kept lines + new lines in ONE atomic operation. Previously
        # this function wrote kept lines, then called _write_jsonl_entry which
        # re-read the file from disk and rewrote it — two reads + two writes
        # for an operation that only needs one of each.
        canonical_records = [
            _seg_to_jsonl_record(video_id, title, seg)
            for seg in new_segments
        ]
        new_lines = [
            json.dumps(record, ensure_ascii=False) + "\n"
            for record in canonical_records
        ]

        # If kept's last entry is missing a trailing newline, fix before append.
        if kept and not kept[-1].endswith("\n"):
            kept[-1] = kept[-1] + "\n"

        final_bytes = ("".join(kept) + "".join(new_lines)).encode("utf-8")
        try:
            atomic_write_bytes(
                jsonl_path,
                final_bytes,
                validator=validate_jsonl_bytes,
                before_replace=_hide_file_win,
                after_replace=_hide_file_win,
                stage_path=jsonl_path + ".tmp",
                preserve_stage_on_replace_error=True,
            )
        except SidecarError as _oe:
            # previously returned early WITHOUT re-appending the
            # new segments, silently leaving the OLD entry on disk while the
            # caller thought retranscribe succeeded. Now we re-raise so the
            # caller's emit_error in _write_outputs surfaces the failure and
            # the user can see that their retranscribe didn't land.
            _log.error("_replace_jsonl_entry atomic write failed: %s", _oe)
            raise
        final_generation = _jsonl_generation(jsonl_path)
        if receipt_out is not None:
            receipt_out.update({
                "version": 1,
                "jsonl_path": os.path.normpath(jsonl_path),
                "video_id": vid_norm,
                "title": title,
                "base_generation": base_generation,
                "final_generation": final_generation,
                "final_sha256": hashlib.sha256(final_bytes).hexdigest(),
                "base_searchable_count": base_searchable_count,
                "final_searchable_count": (
                    kept_searchable_count
                    + sum(_searchable_jsonl_record(record)
                          for record in canonical_records)
                ),
                "removed_searchable_count": removed_searchable_count,
                "removed_blank_video_id": removed_blank_video_id,
                "requires_full_reingest": requires_full_reingest,
                "canonical_records": canonical_records,
            })
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
                       extra_titles_to_remove=None,
                       old_text_candidates=None,
                       video_id: str = "",
                       upload_date: str = "",
                       duration_secs: float = 0) -> bool:
    """Surgically swap this video's `===(…)===\\n<body>\\n\\n\\n` block in
    the aggregated Transcript.txt. `_replace_txt_entry`.

    `extra_titles_to_remove` is the set returned by `_replace_jsonl_entry`
    — additional titles discovered via video_id match. Passing them here
    lets the .txt pass remove stale title-drifted entries consistently.

    `source_tag` can be "(WHISPER:small)" or the bare model name; stored
    verbatim as the 4th bracketed field on the header line so the
    The companion viewer / Browse source banner can detect it.

    Returns True on success. On failure, raises — the caller in
    transcribe.core._write_outputs catches the exception and runs the
    .jsonl roll-back to keep the two sidecars in sync. The previous
    bare `except Exception: return False` here silently swallowed the
    failure so the caller's roll-back branch never fired, leaving the
    user with a new .jsonl + an old .txt (split state).
    """
    content = read_text(txt_path).text

    # Build purge set as NORMALIZED keys (NFC + lowercase +
    # whitespace-collapsed + trailing-punct stripped). Without this
    # the check below misses "Title." vs "Title" variants that the
    # retranscribe flow legitimately needs to swap out — which is
    # what caused the triple-block duplication in v47.6 and older.
    purge = {_norm_title(t) for t in (extra_titles_to_remove or ())}
    purge.add(_norm_title(title))
    purge.discard("")

    matches = list(_HEADER_RE.finditer(content))
    old_body_keys = {
        _body_key(t) for t in (old_text_candidates or ()) if _body_key(t)
    }
    vid = (video_id or "").strip()
    id_matches: list[tuple[int, int, re.Match[str]]] = []
    title_matches: list[tuple[int, int, re.Match[str]]] = []
    body_matches: list[tuple[int, int, re.Match[str]]] = []
    for i, m in enumerate(matches):
        entry_key = _norm_title(m.group(1))
        if entry_key not in purge:
            continue
        end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
        candidate = (m.start(), end, m)
        header_vid = (m.group(5) or "").strip()
        if vid and header_vid == vid:
            # v2 headers carry a stable identity. Prefer every block for the
            # target id (including stale-title duplicates) over title/body
            # heuristics, which are only needed for legacy v1 headers.
            id_matches.append(candidate)
            continue
        if vid and header_vid:
            # A v2 header for another video is authoritative evidence that a
            # same-title block is not ours. Never feed it to legacy fallbacks.
            continue
        title_matches.append(candidate)
        if old_body_keys:
            body_start = content.find("\n", m.end(), end)
            body = content[(body_start + 1 if body_start >= 0 else m.end()):end]
            if _body_key(body) in old_body_keys:
                body_matches.append(candidate)

    if id_matches:
        removals = id_matches
    elif old_body_keys:
        removals = body_matches
        if not removals and len(title_matches) == 1:
            # Legacy/edited TXT body may not match JSONL exactly, but a single
            # matching header is still unambiguous.
            removals = title_matches
        elif not removals and title_matches:
            raise ValueError(
                f"Refusing ambiguous same-title TXT replacement for {title!r}")
    else:
        if len(title_matches) > 1:
            raise ValueError(
                f"Refusing ambiguous same-title TXT replacement for {title!r}")
        removals = title_matches

    # Remove each matching entry (header line through the next header
    # or EOF). Iterate from the end so earlier match positions stay
    # valid as we slice. Capture date+duration (and, when the caller
    # didn't supply one, the video id) from the first removed entry so
    # the new block inherits the provenance.
    new_content = content
    # A recovery replacement can legitimately have no prior TXT block to
    # inherit from. Preserve the caller's source metadata in that case while
    # keeping the historical Unknown defaults for existing callers.
    date_fmt = _format_upload_date(upload_date or "")
    dur_raw = _format_duration_hms(duration_secs or 0) or ""
    dur_fmt = f"({dur_raw})" if dur_raw else "(Unknown length)"
    captured = False
    for start, end, m in sorted(removals, key=lambda x: x[0], reverse=True):
        if not captured:
            # Matches group indices of _HEADER_RE:
            # (title, date, dur, src, video_id?)
            date_fmt = m.group(2)
            dur_fmt = m.group(3)
            if not vid:
                vid = (m.group(5) or "").strip()
            captured = True
        new_content = new_content[:start] + new_content[end:]

    src_fmt = source_tag if source_tag.startswith("(") else f"({source_tag})"
    new_entry = (f"===({title}), {date_fmt}, {dur_fmt}, {src_fmt}"
                 f"{_header_url_field(vid)}===\n{new_text}\n\n\n")

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
    atomic_write_text(txt_path, new_content)
    return True
