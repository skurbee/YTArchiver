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
from typing import Any, Dict, List, Optional, Tuple


def _norm_title(s: str) -> str:
    """Mirrors transcribe._norm_title — strip/lower/collapse-whitespace.
    Kept as a local copy so drift_scan has no import-time dependency on
    transcribe (which pulls in Whisper/ffmpeg modules at import)."""
    s = (s or "").strip().lower()
    # Unicode NFKC normalization handles combining-mark + width
    # differences that commonly drift between yt-dlp's VTT path and
    # the Whisper path.
    try:
        import unicodedata
        s = unicodedata.normalize("NFKC", s)
    except Exception:
        pass
    # Collapse internal whitespace runs
    s = re.sub(r"\s+", " ", s)
    return s


# Regex for .txt header: "===(title), (MM.DD.YYYY), (H:MM), (SOURCE)==="
# Same as transcribe._HEADER_RE; copied here to stay import-independent.
_HEADER_RE = re.compile(
    r'^===\(([^)]*)\),\s*(\([^)]*\)),\s*(\([^)]*\)),\s*(\([^)]*\))===',
    re.MULTILINE)

# Id bracket suffix extraction (matches `... [abc12_def-3]` at end of
# a title, preserving the raw title with or without the bracket).
_ID_BRACKET_RE = re.compile(r"\[([A-Za-z0-9_-]{11})\]\s*$")


def _scan_txt_titles(folder_path: str) -> Dict[str, Dict[str, Any]]:
    """Walk all `*Transcript.txt` under folder_path. Return
    {norm_title: {"raw": raw_title, "video_id": id_or_empty,
                  "txt_path": abs_path, "src_tag": "(WHISPER:large-v3)",
                  "date": "(MM.DD.YYYY)"}}
    One entry per distinct title. If the same title appears in multiple
    .txt files (shouldn't happen but possible with split-years drift),
    the first one wins."""
    out: Dict[str, Dict[str, Any]] = {}
    if not folder_path or not os.path.isdir(folder_path):
        return out
    for dirpath, _dirs, files in os.walk(folder_path):
        for f in files:
            if not f.endswith("Transcript.txt"):
                continue
            fp = os.path.join(dirpath, f)
            try:
                with open(fp, "r", encoding="utf-8") as fh:
                    content = fh.read()
            except Exception:
                continue
            for m in _HEADER_RE.finditer(content):
                raw = (m.group(1) or "").strip()
                if not raw:
                    continue
                vid_id = ""
                im = _ID_BRACKET_RE.search(raw)
                if im:
                    vid_id = im.group(1)
                # Store two normalized keys so lookups succeed regardless
                # of whether the jsonl side has the [id] suffix or not.
                raw_plain = _ID_BRACKET_RE.sub("", raw).strip() or raw
                rec = {"raw": raw, "video_id": vid_id, "txt_path": fp,
                       "date": (m.group(2) or "").strip("()"),
                       "dur": (m.group(3) or "").strip("()"),
                       "src_tag": (m.group(4) or "").strip("()")}
                out.setdefault(_norm_title(raw), rec)
                out.setdefault(_norm_title(raw_plain), rec)
    return out


def _scan_jsonl_titles(folder_path: str) -> Dict[str, Dict[str, Any]]:
    """Walk all hidden `.*Transcript.jsonl` under folder_path. Return
    {norm_title: {"raw": raw_title, "video_id": id_or_empty,
                  "jsonl_path": abs_path}}
    One entry per distinct title across all .jsonl files."""
    out: Dict[str, Dict[str, Any]] = {}
    if not folder_path or not os.path.isdir(folder_path):
        return out
    for dirpath, _dirs, files in os.walk(folder_path):
        for f in files:
            # Hidden .*Transcript.jsonl (dot prefix = Windows hidden).
            if not (f.startswith(".") and f.endswith("Transcript.jsonl")):
                continue
            fp = os.path.join(dirpath, f)
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
                        key = _norm_title(title)
                        if key not in out:
                            out[key] = {"raw": title, "video_id": vid_id,
                                        "jsonl_path": fp}
                        # Also store [id]-stripped key so callers can
                        # match regardless of bracket presence.
                        raw_plain = _ID_BRACKET_RE.sub("", title).strip()
                        if raw_plain:
                            plain_key = _norm_title(raw_plain)
                            if plain_key not in out:
                                out[plain_key] = {"raw": title,
                                                  "video_id": vid_id,
                                                  "jsonl_path": fp}
            except Exception:
                pass
    return out


def _channel_folder(channel: Dict[str, Any], output_dir: str) -> Optional[str]:
    """Resolve the channel's on-disk folder. Mirrors the folder-name
    resolution used elsewhere (folder_override → name → sanitized)."""
    if not output_dir:
        return None
    name = (channel.get("folder")
            or channel.get("folder_override")
            or channel.get("name")
            or "").strip()
    if not name:
        return None
    return os.path.join(output_dir, name)


def _count_fts_phantoms() -> int:
    """Global phantom count: FTS5 rowids with no matching segments row.

    Returns 0 if the DB is missing or the query fails. This is a global
    measure (not per-channel) because segments_fts doesn't carry a
    channel column — phantoms are tracked via rowids only. In practice
    the fix (rebuild FTS) is global too, so per-channel scoping wouldn't
    change the fix path."""
    try:
        from . import index as _idx
        conn = _idx._open()
        if conn is None:
            return 0
        with _idx._db_lock:
            # Simple total comparison. If FTS > segments, the delta is
            # the phantom count. FTS rowids that point at nothing still
            # get counted in segments_fts, so `COUNT(*)` captures them.
            # Note: FTS5 contentless queries against a virtual table
            # require a MATCH expression, but segments_fts is
            # external-content so COUNT(*) works directly.
            n_fts = conn.execute(
                "SELECT COUNT(*) FROM segments_fts").fetchone()[0]
            n_seg = conn.execute(
                "SELECT COUNT(*) FROM segments").fetchone()[0]
        return max(0, int(n_fts) - int(n_seg))
    except Exception:
        return 0


def scan_channel(channel: Dict[str, Any], output_dir: str) -> Dict[str, Any]:
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

    # Collect distinct entries — dedup since _scan_* store each entry
    # under two keys (with and without [id] suffix). We use txt_path or
    # jsonl_path as the dedup ID since each entry has exactly one.
    seen_txt_paths: set = set()
    seen_jsonl_paths: set = set()

    txt_without_jsonl: List[Dict[str, Any]] = []
    jsonl_without_txt: List[Dict[str, Any]] = []

    for key, rec in txt_map.items():
        # Dedup by (txt_path, raw title)
        dedup_key = (rec["txt_path"], rec["raw"])
        if dedup_key in seen_txt_paths:
            continue
        seen_txt_paths.add(dedup_key)
        # Does a matching .jsonl entry exist?
        if key in jsonl_map:
            continue
        # Also try the [id]-stripped key
        raw_plain = _ID_BRACKET_RE.sub("", rec["raw"]).strip()
        if raw_plain and _norm_title(raw_plain) in jsonl_map:
            continue
        # Also try by video_id if we have one
        if rec.get("video_id"):
            if any(m.get("video_id") == rec["video_id"]
                   for m in jsonl_map.values()):
                continue
        txt_without_jsonl.append({
            "title": rec["raw"],
            "video_id": rec.get("video_id", ""),
            "txt_path": rec["txt_path"],
            "src_tag": rec.get("src_tag", ""),
            "date": rec.get("date", ""),
        })

    for key, rec in jsonl_map.items():
        dedup_key = (rec["jsonl_path"], rec["raw"])
        if dedup_key in seen_jsonl_paths:
            continue
        seen_jsonl_paths.add(dedup_key)
        if key in txt_map:
            continue
        raw_plain = _ID_BRACKET_RE.sub("", rec["raw"]).strip()
        if raw_plain and _norm_title(raw_plain) in txt_map:
            continue
        if rec.get("video_id"):
            if any(m.get("video_id") == rec["video_id"]
                   for m in txt_map.values()):
                continue
        jsonl_without_txt.append({
            "title": rec["raw"],
            "video_id": rec.get("video_id", ""),
            "jsonl_path": rec["jsonl_path"],
        })

    # FTS phantoms — global count (see _count_fts_phantoms docstring).
    fts_phantoms = _count_fts_phantoms()

    # Distinct counts (dedup-safe).
    txt_titles_distinct = len({(r["txt_path"], r["raw"])
                                for r in txt_map.values()})
    jsonl_titles_distinct = len({(r["jsonl_path"], r["raw"])
                                  for r in jsonl_map.values()})

    return {
        "ok": True,
        "channel": {"name": channel.get("name", ""),
                    "folder": channel.get("folder", "")},
        "folder": folder,
        "txt_without_jsonl": txt_without_jsonl,
        "jsonl_without_txt": jsonl_without_txt,
        "fts_phantoms": fts_phantoms,
        "totals": {"txt_titles": txt_titles_distinct,
                   "jsonl_titles": jsonl_titles_distinct},
    }


def _write_transcript_entry_plain(txt_path: str, title: str, date_str: str,
                                  duration_str: str, source_tag: str,
                                  text: str) -> bool:
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
    header = f"===({title}), ({date_str}), ({duration_str}), ({source_tag})==="
    body = text.rstrip() + "\n\n"
    try:
        with open(txt_path, "a", encoding="utf-8") as fh:
            fh.write(header + "\n")
            fh.write(body)
        return True
    except OSError:
        return False


def _rebuild_txt_from_jsonl_entries(jsonl_path: str,
                                    titles_to_recover: List[str]
                                    ) -> Dict[str, Dict[str, Any]]:
    """Read jsonl_path, group segments by title for each title in
    titles_to_recover, and return
    {title: {"text": concatenated_body, "video_id": id,
             "duration_s": approx_seconds}}
    Titles not found in the file are omitted. Other titles in the file
    are ignored."""
    want = {t: True for t in (titles_to_recover or [])}
    buckets: Dict[str, Dict[str, Any]] = {}
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
                if not t or t not in want:
                    continue
                seg_text = (obj.get("text") or "").strip()
                seg_end = float(obj.get("end") or obj.get("e") or 0)
                vid = (obj.get("video_id") or "").strip()
                b = buckets.setdefault(t, {"parts": [], "end": 0.0,
                                           "video_id": vid})
                if seg_text:
                    b["parts"].append(seg_text)
                if seg_end > b["end"]:
                    b["end"] = seg_end
                if vid and not b["video_id"]:
                    b["video_id"] = vid
    except OSError:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
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


def apply_channel(channel: Dict[str, Any], output_dir: str,
                  scan_result: Optional[Dict[str, Any]] = None,
                  enqueue_retranscribe_fn=None,
                  rebuild_fts_fn=None) -> Dict[str, Any]:
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
    jsonl_orphans: Dict[str, List[str]] = {}
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
        # Use the .jsonl file's mtime as best-effort date approximation.
        try:
            mt = os.path.getmtime(jsonl_path)
            ts_struct = time.localtime(mt)
            date_str = time.strftime("%m.%d.%Y", ts_struct)
            time_str = time.strftime("%-I:%M", ts_struct) if os.name != "nt" \
                       else time.strftime("%#I:%M", ts_struct)
        except Exception:
            date_str = "00.00.0000"
            time_str = "0:00"
        for title, data in rebuilt.items():
            src_tag = "RECOVERED-FROM-JSONL"
            dur_str = _fmt_duration_hms(float(data.get("duration_s") or 0))
            # Header time column historically held H:MM duration; we
            # reuse the same field for consistency with other entries.
            if _write_transcript_entry_plain(
                    txt_path, title, date_str, dur_str, src_tag,
                    data.get("text", "")):
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
    if scan_result.get("fts_phantoms", 0) > 0 and rebuild_fts_fn is not None:
        try:
            rebuild_fts_fn()
            actions["fts_rebuilt"] = True
        except Exception:
            pass

    return {"ok": True, "actions": actions, "details": details,
            "scan": scan_result}


def _lookup_video_filepaths(channel_name: str,
                            titles: List[str]) -> Dict[str, str]:
    """Return {norm_title: filepath} for each title in `titles` that
    can be found in the FTS DB's videos table under this channel.
    Titles not found are omitted. Uses COLLATE NOCASE for the title
    match and strips trailing [id] brackets for robustness."""
    out: Dict[str, str] = {}
    if not channel_name or not titles:
        return out
    try:
        from . import index as _idx
        conn = _idx._open()
        if conn is None:
            return out
        with _idx._db_lock:
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
        db_map: Dict[str, str] = {}
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
    except Exception:
        pass
    return out


def rebuild_fts_index() -> bool:
    """Run the FTS5 rebuild idiom to clean up external-content phantom
    rows. Idempotent and cheap (~1s even on large DBs)."""
    try:
        from . import index as _idx
        conn = _idx._open()
        if conn is None:
            return False
        with _idx._db_lock:
            conn.execute(
                "INSERT INTO segments_fts(segments_fts) VALUES('rebuild')")
            conn.commit()
        return True
    except Exception:
        return False
