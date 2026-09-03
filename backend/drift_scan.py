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

  C. FTS integrity: the raw token index for segments or video titles no
     longer agrees with its external-content source table. This is global,
     not channel-scoped. Fix: atomically rebuild and verify both FTS shadows.

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
    record_no = 0
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
                        if m.group(5):
                            # A v2 header's explicit watch URL is authoritative.
                            # A real title can itself end in an 11-character
                            # bracketed label, so that suffix must never outrank
                            # the separately written youtu.be identity field.
                            vid_id = m.group(5)
                        elif im:
                            vid_id = im.group(1)
                        raw_plain = _ID_BRACKET_RE.sub("", raw).strip() or raw
                        record_no += 1
                        rec = {"raw": raw, "video_id": vid_id, "txt_path": fp,
                               "record_no": record_no,
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
    """Compatibility count of unhealthy FTS indexes.

    External-content FTS tables can return the current content row for a stale
    token attached to a recycled rowid, so a normal ``LEFT JOIN`` cannot
    diagnose this safely. Delegate to the one authoritative FTS5 integrity
    check and return the number of unhealthy shadows. ``fts_phantoms`` remains
    the public result key for compatibility with the existing Drift dialog.
    """
    try:
        from . import index_maintenance as _maintenance
        health = _maintenance.fts_health_check()
    except Exception as exc:
        _log.warning("FTS health check failed: %s", exc)
        return None
    if health.get("ok"):
        return 0
    indexes = health.get("indexes") or {}
    unhealthy = sum(
        1 for result in indexes.values() if not result.get("ok"))
    if unhealthy:
        return unhealthy
    _log.warning("FTS health check unavailable: %s",
                 health.get("error") or "unknown error")
    return None


def _record_title_keys(rec: dict[str, Any]) -> set[str]:
    raw = (rec.get("raw") or rec.get("title") or "").strip()
    keys = {_norm_title(raw)} if raw else set()
    plain = _ID_BRACKET_RE.sub("", raw).strip()
    if plain:
        keys.add(_norm_title(plain))
    keys.discard("")
    return keys


def _canonical_title_identities(
        channel_name: str,
        ) -> dict[str, dict[str, str]] | None:
    """Return title-keyed logical DB identities for one channel.

    The inner mapping is ``logical_video_key -> video_id`` so two physical
    copies of one video remain one candidate. ``None`` means the catalog could
    not be consulted; callers must fail closed rather than write an ID-less
    transcript entry.
    """
    if not channel_name:
        return {}
    try:
        from . import index as _idx
        conn = _idx._reader_open()
        if conn is None:
            return None
        canonical_ctes = _idx.canonical_videos_cte_sql()
        with _idx._reader_lock:
            rows = conn.execute(
                f"WITH {canonical_ctes} "
                "SELECT title, logical_video_key, video_id "
                "FROM canonical_videos "
                "WHERE channel=? COLLATE NOCASE",
                (channel_name,),
            ).fetchall()
    except Exception as exc:
        _log.warning("canonical drift identity lookup failed for %r: %s",
                     channel_name, exc)
        return None

    result: dict[str, dict[str, str]] = {}
    for title, logical_key, video_id in rows:
        if not logical_key:
            continue
        for key in _record_title_keys({"raw": title or ""}):
            result.setdefault(key, {})[str(logical_key)] = (
                str(video_id or "").strip())
    return result


def _unique_canonical_video_id(
        rec: dict[str, Any],
        candidates_by_title: dict[str, dict[str, str]] | None,
        ) -> str:
    """Resolve one usable ID only when title identifies one logical row."""
    if candidates_by_title is None:
        return ""
    logical: dict[str, str] = {}
    for key in _record_title_keys(rec):
        logical.update(candidates_by_title.get(key, {}))
    if len(logical) != 1:
        return ""
    video_id = next(iter(logical.values()), "")
    return video_id if re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id) else ""


def _unique_records(records_by_title: dict[str, list[dict[str, Any]]],
                    path_key: str) -> list[dict[str, Any]]:
    """Flatten alias-keyed scan records without collapsing distinct IDs."""
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, int]] = set()
    for records in records_by_title.values():
        for rec in records:
            key = (rec.get(path_key, ""), rec.get("raw", ""),
                   rec.get("video_id", ""), int(rec.get("record_no") or 0))
            if key in seen:
                continue
            seen.add(key)
            out.append(rec)
    return out


def _match_transcript_records(
        txt_records: list[dict[str, Any]],
        jsonl_records: list[dict[str, Any]],
        ) -> tuple[set[int], set[int], set[int], set[int]]:
    """Pair ID-first, then by title only when the pairing is mutual-unique.

    Returns matched TXT/JSONL indices plus ambiguous unmatched indices. Known
    but different IDs are never title-matched. Any fallback involving a
    missing ID must identify exactly one candidate in both directions.
    """
    matched_txt: set[int] = set()
    matched_jsonl: set[int] = set()

    txt_by_id: dict[str, list[int]] = {}
    jsonl_by_id: dict[str, list[int]] = {}
    for idx, rec in enumerate(txt_records):
        if vid := (rec.get("video_id") or "").strip():
            txt_by_id.setdefault(vid, []).append(idx)
    for idx, rec in enumerate(jsonl_records):
        if vid := (rec.get("video_id") or "").strip():
            jsonl_by_id.setdefault(vid, []).append(idx)
    for vid in txt_by_id.keys() & jsonl_by_id.keys():
        for txt_idx, jsonl_idx in zip(
                txt_by_id[vid], jsonl_by_id[vid], strict=False):
            matched_txt.add(txt_idx)
            matched_jsonl.add(jsonl_idx)

    def _candidates(rec, other_records, unmatched):
        rec_id = (rec.get("video_id") or "").strip()
        rec_keys = _record_title_keys(rec)
        result = []
        for idx in unmatched:
            other = other_records[idx]
            other_id = (other.get("video_id") or "").strip()
            if rec_id and other_id and rec_id != other_id:
                continue
            if rec_keys & _record_title_keys(other):
                result.append(idx)
        return result

    # Mutual uniqueness prevents two same-title/no-ID records from greedily
    # consuming one another in iteration order.
    changed = True
    while changed:
        changed = False
        unmatched_txt = set(range(len(txt_records))) - matched_txt
        unmatched_jsonl = set(range(len(jsonl_records))) - matched_jsonl
        for txt_idx in sorted(unmatched_txt):
            candidates = _candidates(
                txt_records[txt_idx], jsonl_records, unmatched_jsonl)
            if len(candidates) != 1:
                continue
            jsonl_idx = candidates[0]
            reverse = _candidates(
                jsonl_records[jsonl_idx], txt_records, unmatched_txt)
            if reverse != [txt_idx]:
                continue
            matched_txt.add(txt_idx)
            matched_jsonl.add(jsonl_idx)
            changed = True

    unmatched_txt = set(range(len(txt_records))) - matched_txt
    unmatched_jsonl = set(range(len(jsonl_records))) - matched_jsonl

    def _ambiguous_indices(own_records, own_unmatched,
                           other_records, other_unmatched):
        ambiguous: set[int] = set()
        for idx in own_unmatched:
            rec = own_records[idx]
            rec_id = (rec.get("video_id") or "").strip()
            keys = _record_title_keys(rec)
            same_side = [
                peer_idx for peer_idx, peer in enumerate(own_records)
                if peer_idx != idx and keys & _record_title_keys(peer)
            ]
            candidates = _candidates(rec, other_records, other_unmatched)
            if not rec_id and same_side:
                ambiguous.add(idx)
                continue
            for other_idx in candidates:
                other = other_records[other_idx]
                other_id = (other.get("video_id") or "").strip()
                reverse = _candidates(other, own_records, own_unmatched)
                if (not rec_id or not other_id) and (
                        len(candidates) != 1 or reverse != [idx]):
                    ambiguous.add(idx)
                    break
        return ambiguous

    ambiguous_txt = _ambiguous_indices(
        txt_records, unmatched_txt, jsonl_records, unmatched_jsonl)
    ambiguous_jsonl = _ambiguous_indices(
        jsonl_records, unmatched_jsonl, txt_records, unmatched_txt)
    return matched_txt, matched_jsonl, ambiguous_txt, ambiguous_jsonl


def scan_channel(channel: dict[str, Any], output_dir: str) -> dict[str, Any]:
    """Scan one channel's transcript drift.

    Returns:
      {
        "ok": True|False,
        "partial": True|False,
        "errors": [...],  # includes an unavailable FTS health check
        "channel": {"name": ..., "folder": ...},
        "folder": absolute_path,
        "txt_without_jsonl": [{"title", "video_id", "txt_path",
                                "src_tag", "date"}, ...],
        "jsonl_without_txt": [{"title", "video_id", "jsonl_path"}, ...],
        "fts_health_issues": N,  # unhealthy global FTS shadows
        "fts_phantoms": N,  # compatibility alias
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

    txt_records = _unique_records(txt_map, "txt_path")
    jsonl_records = _unique_records(jsonl_map, "jsonl_path")
    matched_txt, matched_jsonl, ambiguous_txt, ambiguous_jsonl = (
        _match_transcript_records(txt_records, jsonl_records))

    txt_without_jsonl: list[dict[str, Any]] = []
    jsonl_without_txt: list[dict[str, Any]] = []

    for idx, rec in enumerate(txt_records):
        if idx in matched_txt:
            continue
        txt_without_jsonl.append({
            "title": rec["raw"],
            "video_id": rec.get("video_id", ""),
            "txt_path": rec["txt_path"],
            "src_tag": rec.get("src_tag", ""),
            "date": rec.get("date", ""),
            "auto_repair": idx not in ambiguous_txt,
            "identity_warning": (
                "ambiguous same-title identity" if idx in ambiguous_txt else ""),
        })

    canonical_title_ids: dict[str, dict[str, str]] | None = None
    canonical_title_ids_loaded = False
    for idx, rec in enumerate(jsonl_records):
        if idx in matched_jsonl:
            continue
        video_id = (rec.get("video_id") or "").strip()
        auto_repair = idx not in ambiguous_jsonl
        identity_warning = (
            "ambiguous same-title identity" if idx in ambiguous_jsonl else "")
        if auto_repair and not video_id:
            if not canonical_title_ids_loaded:
                canonical_title_ids = _canonical_title_identities(
                    channel.get("name", ""))
                canonical_title_ids_loaded = True
            video_id = _unique_canonical_video_id(rec, canonical_title_ids)
            if not video_id:
                auto_repair = False
                identity_warning = "no unique canonical video ID"
        jsonl_without_txt.append({
            "title": rec["raw"],
            "video_id": video_id,
            "jsonl_path": rec["jsonl_path"],
            "auto_repair": auto_repair,
            "identity_warning": identity_warning,
        })

    # FTS integrity is global. The compatibility helper now delegates to the
    # authoritative raw-token integrity check for both external-content
    # shadows; its value is an unhealthy-index count, not a row count.
    fts_health_issues = _count_fts_phantoms()

    # The alias-keyed scan maps were already flattened by _unique_records.
    # Count identities here, not distinct title strings: two no-ID headers
    # with the same title must remain visible as two ambiguous records.
    txt_titles_distinct = len(txt_records)
    jsonl_titles_distinct = len(jsonl_records)

    health_errors: list[str] = []
    if fts_health_issues is None:
        health_errors.append(
            "FTS health could not be verified; index consistency is unknown.")

    return {
        "ok": not health_errors,
        "partial": bool(health_errors),
        "error": health_errors[0] if health_errors else "",
        "errors": health_errors,
        "channel": {"name": channel.get("name", ""),
                    "folder": channel.get("folder", "")},
        "folder": folder,
        "txt_without_jsonl": txt_without_jsonl,
        "jsonl_without_txt": jsonl_without_txt,
        "fts_health_issues": fts_health_issues,
        "fts_phantoms": fts_health_issues,
        "fts_phantoms_error": (
            "unavailable" if fts_health_issues is None else ""),
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
        from backend.transcribe.transcribe_files import _header_url_field as _hurl
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
                                    titles_to_recover: list[Any]
                                    ) -> dict[str, dict[str, Any]]:
    """Read jsonl_path, group segments by title for each title in
    titles_to_recover, and return
    {title: {"text": concatenated_body, "video_id": id,
             "duration_s": approx_seconds}}
    Titles not found in the file are omitted. Other titles in the file
    are ignored."""
    requests: list[dict[str, str]] = []
    for item in titles_to_recover or []:
        if isinstance(item, dict):
            requested = (item.get("title") or item.get("raw") or "").strip()
            requested_id = (item.get("video_id") or "").strip()
        else:
            requested = (item or "").strip()
            match = _ID_BRACKET_RE.search(requested)
            requested_id = match.group(1) if match else ""
        if not requested:
            continue
        keys = {_norm_title(requested)}
        stripped_id = _ID_BRACKET_RE.sub("", requested).strip()
        if stripped_id and stripped_id != requested:
            keys.add(_norm_title(stripped_id))
        requests.append({"title": requested, "video_id": requested_id,
                         "keys": keys})
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
                vid = (obj.get("video_id") or "").strip()
                row_keys = {_norm_title(t)}
                stripped_t = _ID_BRACKET_RE.sub("", t).strip()
                if stripped_t:
                    row_keys.add(_norm_title(stripped_t))
                id_matches = [r for r in requests
                              if vid and r["video_id"] == vid]
                if len(id_matches) == 1:
                    request = id_matches[0]
                else:
                    title_matches = [
                        r for r in requests
                        if r["keys"] & row_keys
                        and not (vid and r["video_id"]
                                 and vid != r["video_id"])
                    ]
                    if len(title_matches) != 1:
                        continue
                    request = title_matches[0]
                match_title = request["title"]
                if not match_title:
                    continue
                seg_text = (obj.get("text") or "").strip()
                seg_end = float(obj.get("end") or obj.get("e") or 0)
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
        canonical_ctes = _idx.canonical_videos_cte_sql()
        with _idx._reader_lock:
            if video_id:
                row = conn.execute(
                    f"WITH {canonical_ctes} "
                    "SELECT logical_upload_ts, filepath FROM canonical_videos "
                    "WHERE channel=? COLLATE NOCASE "
                    "AND video_id=? "
                    "AND is_available_copy=1 LIMIT 1",
                    (channel_name, video_id)).fetchone()
                if row:
                    date = _date_from_epoch(row[0])
                    if date:
                        return date
                    date = _file_mtime_date(row[1] or "")
                    if date:
                        return date
            rows = conn.execute(
                f"WITH {canonical_ctes} "
                "SELECT title, logical_upload_ts, filepath FROM canonical_videos "
                "WHERE channel=? COLLATE NOCASE "
                "AND filepath IS NOT NULL AND filepath != '' "
                "AND is_available_copy=1",
                (channel_name,)).fetchall()
        matching_rows = []
        for db_title, upload_ts, fp in rows:
            db_key = _norm_title(db_title or "")
            db_plain = _norm_title(_ID_BRACKET_RE.sub("", db_title or "").strip())
            if db_key not in {title_key, plain_key} and db_plain not in {
                    title_key, plain_key}:
                continue
            matching_rows.append((upload_ts, fp))
        # A no-ID title fallback is safe only when exactly one logical
        # canonical candidate exists. Same-title videos get no guessed date.
        if len(matching_rows) == 1:
            upload_ts, fp = matching_rows[0]
            return (_date_from_epoch(upload_ts)
                    or _file_mtime_date(fp or ""))
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
        "ok": True|False,
        "partial": True|False,
        "errors": [...],
        "actions": {
          "txt_reconstructed": N,  # entries recovered from .jsonl
          "retranscribe_queued": M,  # orphan .txt queued for Whisper
          "retranscribe_skipped": K,  # orphan .txt with no findable video
          "retranscribe_failed": F,  # queue explicitly rejected/errored
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

    `rebuild_fts_fn()` is the hook to rebuild FTS. If None, an unhealthy
    FTS result is only reported, not acted on."""

    if scan_result is None:
        scan_result = scan_channel(channel, output_dir)
    if not scan_result.get("ok"):
        # A supplied scan is subject to the same fail-closed contract as a
        # freshly generated one. In particular, never apply a payload whose
        # FTS health phase was unavailable/partial.
        return scan_result

    details = {
        "txt_reconstructed_titles": [],
        "retranscribe_queued_titles": [],
        "retranscribe_skipped_titles": [],
        "retranscribe_failed_titles": [],
        "ambiguous_skipped_titles": [],
    }
    actions = {
        "txt_reconstructed": 0,
        "retranscribe_queued": 0,
        "retranscribe_skipped": 0,
        "retranscribe_failed": 0,
        "ambiguous_skipped": 0,
        "fts_rebuilt": False,
    }
    errors: list[str] = []

    # ─── Fix B: JSONL-without-TXT → reconstruct .txt entries ───────────
    # Group orphans by jsonl_path so we only open each file once.
    jsonl_orphans: dict[str, list[dict[str, Any]]] = {}
    for orphan in scan_result.get("jsonl_without_txt", []) or []:
        if orphan.get("auto_repair") is False:
            actions["ambiguous_skipped"] += 1
            details["ambiguous_skipped_titles"].append(orphan["title"])
            continue
        jp = orphan["jsonl_path"]
        jsonl_orphans.setdefault(jp, []).append(orphan)

    for jsonl_path, orphans in jsonl_orphans.items():
        # Derive the matching .txt path (drop leading dot prefix, swap
        # .jsonl → .txt). Format: {dir}/.{name} Transcript.jsonl →
        # {dir}/{name} Transcript.txt
        base = os.path.basename(jsonl_path)
        if base.startswith("."):
            base = base[1:]
        if base.endswith(".jsonl"):
            base = base[:-6] + ".txt"
        txt_path = os.path.join(os.path.dirname(jsonl_path), base)
        for orphan in orphans:
            # Resolve each identity independently. This preserves two known
            # IDs that happen to share a title instead of merging their text.
            rebuilt = _rebuild_txt_from_jsonl_entries(jsonl_path, [orphan])
            title = orphan["title"]
            data = rebuilt.get(title)
            if not data:
                continue
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
        for orphan in scan_result.get("txt_without_jsonl", []) or []:
            title = orphan["title"]
            if orphan.get("auto_repair") is False:
                actions["ambiguous_skipped"] += 1
                details["ambiguous_skipped_titles"].append(title)
                continue
            fp = _resolve_video_filepath(
                channel.get("name", ""), title,
                orphan.get("video_id", ""))
            if not fp or not os.path.isfile(fp):
                actions["retranscribe_skipped"] += 1
                details["retranscribe_skipped_titles"].append(title)
                continue
            try:
                enqueue_result = enqueue_retranscribe_fn(
                    fp, title, orphan.get("video_id", ""))
                enqueue_ok = (
                    enqueue_result.get("ok") is True
                    if isinstance(enqueue_result, dict)
                    else enqueue_result is True
                )
                if enqueue_ok:
                    actions["retranscribe_queued"] += 1
                    details["retranscribe_queued_titles"].append(title)
                    continue
                enqueue_error = (
                    str(enqueue_result.get("error") or "queue rejected the job")
                    if isinstance(enqueue_result, dict)
                    else "queue did not confirm the job"
                )
                actions["retranscribe_failed"] += 1
                details["retranscribe_failed_titles"].append(title)
                errors.append(
                    f"Could not queue re-transcription for {title}: "
                    f"{enqueue_error}")
            except Exception as exc:
                actions["retranscribe_failed"] += 1
                details["retranscribe_failed_titles"].append(title)
                errors.append(
                    f"Could not queue re-transcription for {title}: {exc}")

    # ─── Fix C: unhealthy FTS shadow(s) → atomic dual rebuild ───────────
    fts_health_issues = scan_result.get(
        "fts_health_issues", scan_result.get("fts_phantoms")) or 0
    if fts_health_issues > 0 and rebuild_fts_fn is not None:
        try:
            rebuild_result = rebuild_fts_fn()
            actions["fts_rebuilt"] = (
                bool(rebuild_result.get("ok"))
                if isinstance(rebuild_result, dict)
                else bool(rebuild_result)
            )
            if not actions["fts_rebuilt"]:
                rebuild_error = (
                    str(rebuild_result.get("error") or "rebuild was rejected")
                    if isinstance(rebuild_result, dict)
                    else "rebuild was rejected"
                )
                errors.append(f"Search index rebuild failed: {rebuild_error}")
        except Exception as exc:
            errors.append(f"Search index rebuild failed: {exc}")

    return {
        "ok": not errors,
        "partial": bool(errors),
        "error": errors[0] if errors else "",
        "errors": errors,
        "actions": actions,
        "details": details,
        "scan": scan_result,
    }


def _resolve_video_filepath(channel_name: str, title: str,
                            video_id: str = "") -> str:
    """Resolve one repair target by ID, or by one unique logical title."""
    if not channel_name or not title:
        return ""
    try:
        from . import index as _idx
        conn = _idx._reader_open()
        if conn is None:
            return ""
        canonical_ctes = _idx.canonical_videos_cte_sql()
        with _idx._reader_lock:
            rows = conn.execute(
                f"WITH {canonical_ctes} "
                "SELECT title, filepath, video_id FROM canonical_videos "
                "WHERE channel=? COLLATE NOCASE "
                "AND filepath IS NOT NULL AND filepath != '' "
                "AND is_available_copy=1",
                (channel_name,),
            ).fetchall()
        requested_id = (video_id or "").strip()
        if requested_id:
            candidates = [row for row in rows
                          if (row[2] or "").strip() == requested_id]
        else:
            requested_keys = _record_title_keys({"raw": title})
            title_rows = [row for row in rows
                          if requested_keys & _record_title_keys(
                              {"raw": row[0] or ""})]
            if len(title_rows) != 1:
                return ""
            candidates = title_rows
        if not candidates:
            return ""
        return candidates[0][1] or ""
    except Exception as e:
        _log.warning("video filepath resolution failed for %r: %s",
                     channel_name, e)
        return ""


def _lookup_video_filepaths(channel_name: str,
                            titles: list[str]) -> dict[str, str]:
    """Return {norm_title: filepath} for each title in `titles` that
    can be found in the FTS DB's videos table under this channel.
    Titles not found are omitted. Uses COLLATE NOCASE for the title
    match and strips trailing [id] brackets for robustness."""
    out: dict[str, str] = {}
    if not channel_name or not titles:
        return out
    for title in titles:
        fp = _resolve_video_filepath(channel_name, title)
        if fp:
            out[_norm_title(title)] = fp
    return out


def rebuild_fts_index() -> bool:
    """Compatibility shim for the authoritative atomic dual-FTS rebuild."""
    try:
        from . import index_maintenance as _maintenance
        return bool(_maintenance.rebuild_fts_index().get("ok"))
    except Exception as exc:
        _log.warning("FTS rebuild failed: %s", exc)
        return False
