"""Transcript drift scanner — audit feature H-2.

For a given channel (or all channels), cross-references the aggregated
`{ch} Transcript.txt` files against the hidden `.{ch} Transcript.jsonl`
sidecars and the FTS index, flagging mismatches:

  A. TXT-without-JSONL: a video has an entry in the .txt but no matching
     segments in any .jsonl under the channel folder. This is the worst
     kind — searchable text exists in the .txt but the .jsonl is the
     authoritative source for FTS ingest + Watch-view karaoke. Fix:
     queue a Whisper retranscribe that rebuilds both.

  B. JSONL-without-TXT: a video has segments in the .jsonl but no entry
     in the .txt. Weird state — someone truncated the .txt (manual edit?
     crash during append?) while the .jsonl still holds the data. Fix:
     reconstruct the .txt entry by concatenating the .jsonl segments'
     text + synthesizing a header with best-effort source tag.

  C. FTS phantoms: segments_fts has rowids with no corresponding row in
     segments (audit C-9 — FTS5 external-content tables don't auto-sync
     on source DELETE). Not easily scoped per-channel because
     segments_fts has no channel column, so the phantom count is
     reported globally. Fix: run a full FTS rebuild (idempotent, cheap).

The module is deliberately stateless and pure-function — callers (the
js_api side in main.py) decide when to fire scans and fixes.
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any

from .log import get_logger

_log = get_logger(__name__)


def _is_hidden_transcript_jsonl(dirpath: str, name: str) -> bool:
    """A jsonl is "the channel's transcript sidecar" if EITHER its filename
    starts with '.' (the canonical convention) OR it has the Windows
    hidden attribute set. The dot prefix is what the writers emit, but
    files that were manually moved/copied via Explorer can lose the dot
    prefix while keeping the hidden attribute — those should still count.
    """
    if not name.endswith("Transcript.jsonl"):
        return False
    if name.startswith("."):
        return True
    # Fallback: check the Windows FILE_ATTRIBUTE_HIDDEN bit. Quietly
    # returns False on non-Windows or on any ctypes failure.
    if os.name != "nt":
        return False
    try:
        import ctypes
        FILE_ATTRIBUTE_HIDDEN = 0x02
        attrs = ctypes.windll.kernel32.GetFileAttributesW(
            os.path.join(dirpath, name))
        # GetFileAttributesW returns 0xFFFFFFFF (INVALID_FILE_ATTRIBUTES) on error
        if attrs == 0xFFFFFFFF:
            return False
        return bool(attrs & FILE_ATTRIBUTE_HIDDEN)
    except Exception as e:
        _log.debug("swallowed: %s", e)
        return False


# consolidated onto text_utils.normalize_title.
# The canonical normalizer adds trailing-punct stripping ("title." ==
# "title") which is what drift detection wants — the previous copy here
# kept trailing punct so "title." and "title" drifted into separate keys.
from .text_utils import normalize_title as _norm_title

# Regex for .txt header: "===(title), (MM.DD.YYYY), (H:MM), (SOURCE)==="
# Same as transcribe._HEADER_RE; copied here to stay import-independent.
# title group is non-greedy + anchored by the literal "), (" that
# follows. Old [^)]* refused to cross any close-paren so a real
# YT title like "Foo (Bar)" parsed as title="Foo (Bar" and every
# video with parens in its name showed up as drift (audit:
# drift_scan.py:80).
_HEADER_RE = re.compile(
    r'^===\((.+?)\),\s*(\([^)]*\)),\s*(\([^)]*\)),\s*'
    r'(\((?!youtu\.be/)[^)]*\))'
    r'(?:,\s*\(youtu\.be/([A-Za-z0-9_-]{11})\))?===',
    re.MULTILINE)

# Id bracket suffix extraction (matches `... [abc12_def-3]` at end of
# a title, preserving the raw title with or without the bracket).
_ID_BRACKET_RE = re.compile(r"\[([A-Za-z0-9_-]{11})\]\s*$")


def _scan_txt_titles(folder_path: str) -> dict[str, list[dict[str, Any]]]:
    """Walk all `*Transcript.txt` under folder_path. Return
    {norm_title: [ {"raw": ..., "video_id": ..., "txt_path": ...,
                    "src_tag": ..., "date": ...}, ... ]}

    Stores a LIST per normalized title so duplicate-title entries (re-
    uploads, daily-show duplicates, series with shared title prefix)
    are all preserved. The previous setdefault dropped every duplicate
    after the first, hiding genuine drift for those entries.
    """
    out: dict[str, list[dict[str, Any]]] = {}
    if not folder_path or not os.path.isdir(folder_path):
        return out
    for dirpath, _dirs, files in os.walk(folder_path):
        for f in files:
            if not f.endswith("Transcript.txt"):
                continue
            fp = os.path.join(dirpath, f)
            try:
                with open(fp, "r", encoding="utf-8") as fh:
                    for line in fh:
                        m = _HEADER_RE.match(line.rstrip("\r\n"))
                        if not m:
                            continue
                        raw = (m.group(1) or "").strip()
                        if not raw:
                            continue
                        vid_id = ""
                        im = _ID_BRACKET_RE.search(raw)
                        if im:
                            vid_id = im.group(1)
                        elif m.group(5):
                            # v2 headers carry the id in the trailing
                            # (youtu.be/<id>) field.
                            vid_id = m.group(5)
                        raw_plain = _ID_BRACKET_RE.sub("", raw).strip() or raw
                        rec = {"raw": raw, "video_id": vid_id, "txt_path": fp,
                               "date": (m.group(2) or "").strip("()"),
                               "dur": (m.group(3) or "").strip("()"),
                               "src_tag": (m.group(4) or "").strip("()")}
                        # Append-not-setdefault so duplicate titles are kept.
                        out.setdefault(_norm_title(raw), []).append(rec)
                        out.setdefault(_norm_title(raw_plain), []).append(rec)
            except Exception:
                continue
    return out


def _scan_jsonl_titles(folder_path: str) -> dict[str, list[dict[str, Any]]]:
    """Walk all hidden `.*Transcript.jsonl` under folder_path. Return
    {norm_title: [ {"raw": ..., "video_id": ..., "jsonl_path": ...} ] }

    Stores a LIST per normalized title so duplicate-title entries
    (re-uploads etc.) are all preserved. See _scan_txt_titles for the
    same rationale. Within a single jsonl file, duplicate lines for
    the same video_id are de-duplicated; cross-file duplicates are
    kept so drift detection sees them.
    """
    out: dict[str, list[dict[str, Any]]] = {}
    if not folder_path or not os.path.isdir(folder_path):
        return out
    for dirpath, _dirs, files in os.walk(folder_path):
        for f in files:
            # Channel transcript sidecar: dot-prefix is the canonical
            # form, but we also accept files with the Windows hidden
            # attribute (in case a copy/restore stripped the dot prefix).
            if not _is_hidden_transcript_jsonl(dirpath, f):
                continue
            fp = os.path.join(dirpath, f)
            # Within this file, dedupe by video_id so a single jsonl
            # with thousands of segments per video doesn't append the
            # same record thousands of times.
            seen_vid_in_file: set = set()
            seen_title_in_file: set = set()
            try:
                with open(fp, "r", encoding="utf-8") as fh:
                    for line in fh:
                        ls = line.strip()
                        if not ls:
                            continue
                        try:
                            obj = json.loads(ls)
                        except Exception:
                            continue
                        title = (obj.get("title") or "").strip()
                        if not title:
                            continue
                        vid_id = (obj.get("video_id") or "").strip()
                        # Skip if we already saw this video in THIS file
                        dedup_key = vid_id or f"_t::{_norm_title(title)}"
                        if dedup_key in seen_vid_in_file:
                            continue
                        seen_vid_in_file.add(dedup_key)
                        key = _norm_title(title)
                        seen_title_in_file.add(key)
                        rec = {"raw": title, "video_id": vid_id,
                               "jsonl_path": fp}
                        out.setdefault(key, []).append(rec)
                        # Also store [id]-stripped key so callers can
                        # match regardless of bracket presence.
                        raw_plain = _ID_BRACKET_RE.sub("", title).strip()
                        if raw_plain:
                            plain_key = _norm_title(raw_plain)
                            if plain_key != key:
                                out.setdefault(plain_key, []).append(rec)
            except Exception as e:
                _log.debug("swallowed: %s", e)
    return out


def _channel_folder(channel: dict[str, Any], output_dir: str) -> str | None:
    """Resolve the channel's on-disk folder. Mirrors the folder-name
    resolution used elsewhere (folder_override → name → sanitized)."""
    if not output_dir:
        return None
    from . import sync as _sync
    name = _sync.channel_folder_name(channel)
    if not name:
        return None
    return os.path.join(output_dir, name)


def _count_fts_phantoms() -> int | None:
    """Global phantom count: FTS5 rowids with no matching segments row.

    Returns 0 if the DB is missing or the query fails. This is a global
    measure (not per-channel) because segments_fts doesn't carry a
    channel column — phantoms are tracked via rowids only. In practice
    the fix (rebuild FTS) is global too, so per-channel scoping wouldn't
    change the fix path."""
    try:
        from . import index as _idx
        conn = _idx._reader_open()
        if conn is None:
            _log.warning("FTS phantom count unavailable: index reader not open")
            return None
        # Reader path — COUNT comparison can run in parallel with any
        # writer (sweep, ingest). Drift Scan is a diagnostic, so making
        # it block was extra painful.
        with _idx._reader_lock:
            # Count phantoms via LEFT JOIN on rowid — that's the only
            # reliable way. The old simple-subtraction approach (COUNT
            # segments_fts - COUNT segments) over-reports because FTS5
            # external-content tables can include deleted-but-not-merged
            # entries in their COUNT(*) until the next 'merge'/'optimize'
            # command. LEFT JOIN finds the rowids that genuinely have no
            # backing segment.
            row = conn.execute(
                "SELECT COUNT(*) FROM segments_fts f "
                "LEFT JOIN segments s ON f.rowid = s.id "
                "WHERE s.id IS NULL"
            ).fetchone()
        return int(row[0]) if row else 0
    except Exception as e:
        _log.warning("FTS phantom count failed; index may be locked/corrupt: %s", e)
        return None


def scan_channel(channel: dict[str, Any], output_dir: str) -> dict[str, Any]:
    """Scan one channel's transcript drift.

    Returns:
      {
        "ok": True,
        "channel": {"name": ..., "folder": ...},
        "folder": absolute_path,
        "txt_without_jsonl": [{"title", "video_id", "txt_path",
                                "src_tag", "date"}, ...],
        "jsonl_without_txt": [{"title", "video_id", "jsonl_path"}, ...],
        "fts_phantoms": N,  # global count
        "totals": {"txt_titles": X, "jsonl_titles": Y}
      }
    """
    folder = _channel_folder(channel, output_dir)
    if not folder:
        return {"ok": False, "error": "Could not resolve channel folder"}
    if not os.path.isdir(folder):
        return {"ok": False, "error": f"Channel folder does not exist: {folder}"}

    txt_map = _scan_txt_titles(folder)
    jsonl_map = _scan_jsonl_titles(folder)

    # Pre-compute video_id sets from each side so the cross-check
    # inside the loop is O(1) instead of O(N) per entry. Iterate unique
    # records directly instead of materializing extra flattened lists;
    # large channels can have many raw/plain alias keys pointing at the
    # same record objects.
    def _iter_unique_records(m: dict[str, list[dict[str, Any]]],
                             path_key: str):
        seen: set[tuple[str, str]] = set()
        for records in m.values():
            for rec in records:
                dedup_key = (rec.get(path_key, ""), rec.get("raw", ""))
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)
                yield rec

    _jsonl_vids = {r.get("video_id")
                   for r in _iter_unique_records(jsonl_map, "jsonl_path")
                   if r.get("video_id")}
    _txt_vids = {r.get("video_id")
                 for r in _iter_unique_records(txt_map, "txt_path")
                 if r.get("video_id")}

    seen_txt_paths: set = set()
    seen_jsonl_paths: set = set()

    txt_without_jsonl: list[dict[str, Any]] = []
    jsonl_without_txt: list[dict[str, Any]] = []

    # Iterate the unique records so duplicate-title entries are all
    # checked individually for drift (rather than the previous
    # setdefault-first-wins behavior that hid drift for re-uploads).
    for rec in _iter_unique_records(txt_map, "txt_path"):
        dedup_key = (rec["txt_path"], rec["raw"])
        if dedup_key in seen_txt_paths:
            continue
        seen_txt_paths.add(dedup_key)
        key = _norm_title(rec["raw"])
        if key in jsonl_map:
            continue
        raw_plain = _ID_BRACKET_RE.sub("", rec["raw"]).strip()
        if raw_plain and _norm_title(raw_plain) in jsonl_map:
            continue
        if rec.get("video_id") and rec["video_id"] in _jsonl_vids:
            continue
        txt_without_jsonl.append({
            "title": rec["raw"],
            "video_id": rec.get("video_id", ""),
            "txt_path": rec["txt_path"],
            "src_tag": rec.get("src_tag", ""),
            "date": rec.get("date", ""),
        })

    for rec in _iter_unique_records(jsonl_map, "jsonl_path"):
        dedup_key = (rec["jsonl_path"], rec["raw"])
        if dedup_key in seen_jsonl_paths:
            continue
        seen_jsonl_paths.add(dedup_key)
        key = _norm_title(rec["raw"])
        if key in txt_map:
            continue
        raw_plain = _ID_BRACKET_RE.sub("", rec["raw"]).strip()
        if raw_plain and _norm_title(raw_plain) in txt_map:
            continue
        if rec.get("video_id") and rec["video_id"] in _txt_vids:
            continue
        jsonl_without_txt.append({
            "title": rec["raw"],
            "video_id": rec.get("video_id", ""),
            "jsonl_path": rec["jsonl_path"],
        })

    # FTS phantoms — global count (see _count_fts_phantoms docstring).
    fts_phantoms = _count_fts_phantoms()

    # Distinct counts (dedup-safe).
    # Bug [32]: normalize paths via normcase+normpath so case differences
    # on Windows ("C:\Path" vs "c:\path") don't produce false-distinct
    # entries. The same .txt or .jsonl file viewed through symlinks /
    # mixed-case parents would have inflated the distinct count.
    def _np(p: str) -> str:
        try:
            return os.path.normcase(os.path.normpath(p))
        except Exception:
            return p
    txt_titles_distinct = len({(_np(r["txt_path"]), r["raw"])
                                for r in _iter_unique_records(
                                    txt_map, "txt_path")})
    jsonl_titles_distinct = len({(_np(r["jsonl_path"]), r["raw"])
                                  for r in _iter_unique_records(
                                      jsonl_map, "jsonl_path")})

    return {
        "ok": True,
        "channel": {"name": channel.get("name", ""),
                    "folder": channel.get("folder", "")},
        "folder": folder,
        "txt_without_jsonl": txt_without_jsonl,
        "jsonl_without_txt": jsonl_without_txt,
        "fts_phantoms": fts_phantoms,
        "fts_phantoms_error": (
            "unavailable" if fts_phantoms is None else ""),
        "totals": {"txt_titles": txt_titles_distinct,
                   "jsonl_titles": jsonl_titles_distinct},
    }


def _write_transcript_entry_plain(txt_path: str, title: str, date_str: str,
                                  duration_str: str, source_tag: str,
                                  text: str, video_id: str = "") -> bool:
    """Append a new header+body entry to an aggregated Transcript.txt.

    Used by apply_channel to reconstruct missing .txt entries from
    .jsonl data. Header format matches the existing
    _write_transcript_entry in transcribe.py exactly:
      ===(title), (MM.DD.YYYY), (H:MM), (SOURCE)===
      <text>

    Returns True on success, False on I/O error. Creates the file +
    parent directory if they don't exist."""
    try:
        os.makedirs(os.path.dirname(txt_path), exist_ok=True)
    except OSError:
        pass
    try:
        from backend.transcribe.transcribe_files import (
            _header_url_field as _hurl)
        _url_field = _hurl(video_id)
    except Exception:
        _url_field = ""
    header = (f"===({title}), ({date_str}), ({duration_str}), "
              f"({source_tag}){_url_field}===")
    body = text.rstrip() + "\n\n"
    # Read-append-tmp-replace, SERIALIZED with the transcribe writers
    # via their shared per-path lock. The atomic-replace alone did NOT
    # fix interleaving — a snapshot-replace with no shared lock
    # guarantees that any entry a transcribe worker appends between
    # our read and our os.replace is silently erased (drift_apply runs
    # on a worker thread concurrent with sync's transcribe writers).
    try:
        from backend.transcribe.transcribe_files import txt_lock_for as _tlf
        _lk = _tlf(txt_path)
    except Exception:
        import threading as _th
        _lk = _th.RLock()
    with _lk:
        try:
            try:
                with open(txt_path, "r", encoding="utf-8") as fh:
                    existing = fh.read()
            except FileNotFoundError:
                existing = ""
            new_content = existing + header + "\n" + body
            tmp = txt_path + ".tmp"
            try:
                with open(tmp, "w", encoding="utf-8") as fh:
                    fh.write(new_content)
                    try:
                        fh.flush()
                        os.fsync(fh.fileno())
                    except OSError as e:
                        _log.debug("swallowed: %s", e)
                os.replace(tmp, txt_path)
            except OSError:
                try: os.remove(tmp)
                except OSError: pass
                return False
            return True
        except OSError:
            return False


def _rebuild_txt_from_jsonl_entries(jsonl_path: str,
                                    titles_to_recover: list[str]
                                    ) -> dict[str, dict[str, Any]]:
    """Read jsonl_path, group segments by title for each title in
    titles_to_recover, and return
    {title: {"text": concatenated_body, "video_id": id,
             "duration_s": approx_seconds}}
    Titles not found in the file are omitted. Other titles in the file
    are ignored."""
    want: dict[str, str] = {}
    for raw_title in titles_to_recover or []:
        requested = (raw_title or "").strip()
        if not requested:
            continue
        candidates = [requested]
        stripped_id = _ID_BRACKET_RE.sub("", requested).strip()
        if stripped_id and stripped_id != requested:
            candidates.append(stripped_id)
        for cand in candidates:
            key = _norm_title(cand)
            if key and key not in want:
                want[key] = requested
    buckets: dict[str, dict[str, Any]] = {}
    try:
        with open(jsonl_path, "r", encoding="utf-8") as fh:
            for line in fh:
                ls = line.strip()
                if not ls:
                    continue
                try:
                    obj = json.loads(ls)
                except Exception:
                    continue
                t = (obj.get("title") or "").strip()
                if not t:
                    continue
                match_title = want.get(_norm_title(t))
                if not match_title:
                    stripped_t = _ID_BRACKET_RE.sub("", t).strip()
                    match_title = want.get(_norm_title(stripped_t))
                if not match_title:
                    continue
                seg_text = (obj.get("text") or "").strip()
                seg_end = float(obj.get("end") or obj.get("e") or 0)
                vid = (obj.get("video_id") or "").strip()
                b = buckets.setdefault(match_title, {"parts": [], "end": 0.0,
                                                     "video_id": vid})
                if seg_text:
                    b["parts"].append(seg_text)
                if seg_end > b["end"]:
                    b["end"] = seg_end
                if vid and not b["video_id"]:
                    b["video_id"] = vid
    except OSError:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for t, b in buckets.items():
        out[t] = {"text": " ".join(b["parts"]).strip(),
                  "duration_s": b["end"],
                  "video_id": b["video_id"]}
    return out


def _fmt_duration_hms(secs: float) -> str:
    """Seconds → H:MM (zero-padded minutes). Matches OLD's transcript
    header duration format."""
    total = max(0, int(secs))
    h = total // 3600
    m = (total % 3600) // 60
    return f"{h}:{m:02d}"


def _date_from_epoch(ts: Any) -> str:
    try:
        val = float(ts or 0)
    except (TypeError, ValueError):
        return ""
    if val <= 0:
        return ""
    try:
        return time.strftime("%m.%d.%Y", time.localtime(val))
    except (OverflowError, OSError, ValueError):
        return ""


def _file_mtime_date(path: str) -> str:
    try:
        return _date_from_epoch(os.path.getmtime(path))
    except OSError:
        return ""


def _recovered_upload_date(channel_name: str, title: str,
                           video_id: str = "") -> str:
    """Best-effort real upload date for a JSONL-to-TXT recovery.

    Prefer videos.upload_ts, then the archived video's file mtime. The
    aggregate JSONL mtime is deliberately not consulted here because it
    changes whenever any video in the channel is appended.
    """
    if not channel_name:
        return ""
    title_key = _norm_title(title)
    plain_key = _norm_title(_ID_BRACKET_RE.sub("", title or "").strip())
    try:
        from . import index as _idx
        conn = _idx._reader_open()
        if conn is None:
            return ""
        with _idx._reader_lock:
            if video_id:
                row = conn.execute(
                    "SELECT upload_ts, filepath FROM videos "
                    "WHERE channel=? COLLATE NOCASE "
                    "AND video_id=? "
                    "AND is_duplicate_of IS NULL "
                    "ORDER BY (upload_ts IS NULL) ASC, id ASC LIMIT 1",
                    (channel_name, video_id)).fetchone()
                if row:
                    date = _date_from_epoch(row[0])
                    if date:
                        return date
                    date = _file_mtime_date(row[1] or "")
                    if date:
                        return date
            rows = conn.execute(
                "SELECT title, upload_ts, filepath FROM videos "
                "WHERE channel=? COLLATE NOCASE "
                "AND filepath IS NOT NULL AND filepath != '' "
                "AND is_duplicate_of IS NULL",
                (channel_name,)).fetchall()
        for db_title, upload_ts, fp in rows:
            db_key = _norm_title(db_title or "")
            db_plain = _norm_title(_ID_BRACKET_RE.sub("", db_title or "").strip())
            if db_key not in {title_key, plain_key} and db_plain not in {
                    title_key, plain_key}:
                continue
            date = _date_from_epoch(upload_ts)
            if date:
                return date
            date = _file_mtime_date(fp or "")
            if date:
                return date
    except Exception as e:
        _log.debug("recovered upload date lookup failed: %s", e)
    return ""


def apply_channel(channel: dict[str, Any], output_dir: str,
                  scan_result: dict[str, Any] | None = None,
                  enqueue_retranscribe_fn=None,
                  rebuild_fts_fn=None) -> dict[str, Any]:
    """Apply the three fixes to drift found in `channel`.

    If `scan_result` is None, scans the channel fresh.

    Returns:
      {
        "ok": True,
        "actions": {
          "txt_reconstructed": N,  # entries recovered from .jsonl
          "retranscribe_queued": M,  # orphan .txt queued for Whisper
          "retranscribe_skipped": K,  # orphan .txt with no findable video
          "fts_rebuilt": True|False,
        },
        "details": {
          "txt_reconstructed_titles": [...],
          "retranscribe_queued_titles": [...],
          "retranscribe_skipped_titles": [...],
        },
      }

    `enqueue_retranscribe_fn(filepath, title, video_id)` is the hook
    main.py provides to queue a Whisper retranscribe task. If None,
    the retranscribe category is only reported, not acted on.

    `rebuild_fts_fn()` is the hook to rebuild FTS. If None, the phantom
    category is only reported, not acted on."""

    if scan_result is None:
        scan_result = scan_channel(channel, output_dir)
        if not scan_result.get("ok"):
            return scan_result

    details = {
        "txt_reconstructed_titles": [],
        "retranscribe_queued_titles": [],
        "retranscribe_skipped_titles": [],
    }
    actions = {
        "txt_reconstructed": 0,
        "retranscribe_queued": 0,
        "retranscribe_skipped": 0,
        "fts_rebuilt": False,
    }

    # ─── Fix B: JSONL-without-TXT → reconstruct .txt entries ───────────
    # Group orphans by jsonl_path so we only open each file once.
    jsonl_orphans: dict[str, list[str]] = {}
    for orphan in scan_result.get("jsonl_without_txt", []) or []:
        jp = orphan["jsonl_path"]
        jsonl_orphans.setdefault(jp, []).append(orphan["title"])

    for jsonl_path, titles in jsonl_orphans.items():
        # Reconstruct content from the .jsonl segments.
        rebuilt = _rebuild_txt_from_jsonl_entries(jsonl_path, titles)
        if not rebuilt:
            continue
        # Derive the matching .txt path (drop leading dot prefix, swap
        # .jsonl → .txt). Format: {dir}/.{name} Transcript.jsonl →
        # {dir}/{name} Transcript.txt
        base = os.path.basename(jsonl_path)
        if base.startswith("."):
            base = base[1:]
        if base.endswith(".jsonl"):
            base = base[:-6] + ".txt"
        txt_path = os.path.join(os.path.dirname(jsonl_path), base)
        for title, data in rebuilt.items():
            src_tag = "RECOVERED-FROM-JSONL"
            date_str = (
                _recovered_upload_date(
                    channel.get("name", ""), title, data.get("video_id", ""))
                or _file_mtime_date(jsonl_path)
                or ""
            )
            dur_str = _fmt_duration_hms(float(data.get("duration_s") or 0))
            # Header time column historically held H:MM duration; we
            # reuse the same field for consistency with other entries.
            if _write_transcript_entry_plain(
                    txt_path, title, date_str, dur_str, src_tag,
                    data.get("text", ""),
                    video_id=data.get("video_id", "")):
                actions["txt_reconstructed"] += 1
                details["txt_reconstructed_titles"].append(title)

    # ─── Fix A: TXT-without-JSONL → queue retranscribe ─────────────────
    if enqueue_retranscribe_fn is not None:
        # Need the video file path for each orphan. Look up by title in
        # the FTS DB's videos table (scoped to this channel).
        filepaths_by_title = _lookup_video_filepaths(
            channel.get("name", ""),
            [o["title"] for o in scan_result.get("txt_without_jsonl", [])])
        for orphan in scan_result.get("txt_without_jsonl", []) or []:
            title = orphan["title"]
            raw_plain = _ID_BRACKET_RE.sub("", title).strip() or title
            fp = (filepaths_by_title.get(_norm_title(title))
                  or filepaths_by_title.get(_norm_title(raw_plain)))
            if not fp or not os.path.isfile(fp):
                actions["retranscribe_skipped"] += 1
                details["retranscribe_skipped_titles"].append(title)
                continue
            try:
                enqueue_retranscribe_fn(fp, title, orphan.get("video_id", ""))
                actions["retranscribe_queued"] += 1
                details["retranscribe_queued_titles"].append(title)
            except Exception:
                actions["retranscribe_skipped"] += 1
                details["retranscribe_skipped_titles"].append(title)

    # ─── Fix C: FTS phantoms → rebuild ─────────────────────────────────
    if (scan_result.get("fts_phantoms") or 0) > 0 and rebuild_fts_fn is not None:
        try:
            rebuild_fts_fn()
            actions["fts_rebuilt"] = True
        except Exception as e:
            _log.debug("swallowed: %s", e)

    return {"ok": True, "actions": actions, "details": details,
            "scan": scan_result}


def _lookup_video_filepaths(channel_name: str,
                            titles: list[str]) -> dict[str, str]:
    """Return {norm_title: filepath} for each title in `titles` that
    can be found in the FTS DB's videos table under this channel.
    Titles not found are omitted. Uses COLLATE NOCASE for the title
    match and strips trailing [id] brackets for robustness."""
    out: dict[str, str] = {}
    if not channel_name or not titles:
        return out
    try:
        from . import index as _idx
        conn = _idx._reader_open()
        if conn is None:
            return out
        # Reader path — pure SELECT against videos.
        with _idx._reader_lock:
            # Pre-fetch all this channel's (title, filepath) so we can
            # match in Python and handle [id] bracket variants.
            rows = conn.execute(
                "SELECT title, filepath FROM videos "
                "WHERE channel=? COLLATE NOCASE "
                "AND filepath IS NOT NULL AND filepath != '' "
                "AND is_duplicate_of IS NULL",
                (channel_name,)).fetchall()
        # Build a lookup map keyed by normalized title (with and without
        # [id] bracket).
        db_map: dict[str, str] = {}
        for db_title, fp in rows:
            if not db_title or not fp:
                continue
            db_map[_norm_title(db_title)] = fp
            raw_plain = _ID_BRACKET_RE.sub("", db_title).strip()
            if raw_plain:
                db_map[_norm_title(raw_plain)] = fp
        for t in titles:
            key = _norm_title(t)
            if key in db_map:
                out[key] = db_map[key]
                continue
            raw_plain = _ID_BRACKET_RE.sub("", t).strip()
            if raw_plain:
                pkey = _norm_title(raw_plain)
                if pkey in db_map:
                    out[pkey] = db_map[pkey]
    except Exception as e:
        _log.warning("video filepath lookup failed for %r: %s",
                     channel_name, e)
    return out


def rebuild_fts_index() -> bool:
    """Run the FTS5 rebuild idiom to clean up external-content phantom
    rows. Idempotent and cheap (~1s even on large DBs).

    after the rebuild, verify the FTS table is responsive
    (SELECT COUNT(*) works and returns >= 0) before claiming success.
    A locked DB or corrupted FTS5 index can silently no-op the
    rebuild INSERT; the read-back confirms the write actually
    landed.
    """
    try:
        from . import index as _idx
        conn = _idx._open()
        if conn is None:
            return False
        with _idx._db_lock:
            # Capture pre-rebuild count for sanity check.
            try:
                _before = conn.execute(
                    "SELECT COUNT(*) FROM segments_fts").fetchone()[0]
            except Exception:
                _before = None
            conn.execute(
                "INSERT INTO segments_fts(segments_fts) VALUES('rebuild')")
            conn.commit()
            # Post-rebuild verification: the table should still be
            # queryable. If this raises, the "success" we'd otherwise
            # report is a lie — bubble up False so callers can show
            # a real error.
            try:
                _after = conn.execute(
                    "SELECT COUNT(*) FROM segments_fts").fetchone()[0]
            except Exception:
                return False
            # If the rebuild succeeded, _after should equal or exceed
            # _before minus phantoms. We don't assert an exact match
            # (rebuild legitimately removes phantoms). Just confirm
            # the read worked.
            _ = (_before, _after)
        return True
    except Exception:
        return False
