"""UI projections of configuration snapshots.

Formatting and optional archive/thumbnail enrichment live here; this module
never loads, migrates, or saves configuration. Callers supply one snapshot.
"""
from __future__ import annotations

import os
import time
from typing import Any

from .log import swallow

# ── Helpers the UI actually needs ───────────────────────────────────────

def _last_sync_epoch(value: Any) -> float | None:
    """Best-effort parser for legacy channel last_sync values."""
    if isinstance(value, (int, float)) and value > 0:
        return float(value)
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    try:
        import datetime as _dt
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                return _dt.datetime.strptime(raw, fmt).timestamp()
            except ValueError:
                pass
        for fmt in ("%I:%M%p, %b %d", "%I:%M %p, %b %d"):
            try:
                now = _dt.datetime.now()
                parsed = _dt.datetime.strptime(
                    f"{raw} {now.year}", f"{fmt} %Y")
            except ValueError:
                continue
            dt = parsed.replace(year=now.year)
            if dt.timestamp() - time.time() > 86400:
                dt = dt.replace(year=now.year - 1)
            return dt.timestamp()
    except Exception:
        return None
    return None


def channels_for_subs_ui(cfg: dict[str, Any]):
    """
    Transform config['channels'] into the row dict format the Subs table
    renderer expects. Returns (rows, total_label).
    """
    channels = sorted(
        cfg.get("channels", []),
        key=lambda c: c.get("name", "").lower(),
    )
    # Pre-derive the archive root once — we need it to check for
    # `_redownload_progress.json` in each channel's folder so the
    # Subs table can flag channels with unfinished redownloads.
    _base_dir = (cfg.get("output_dir") or "").strip()
    try:
        from .sync import channel_folder_name as _cfn_for_redwnl
    except Exception:
        _cfn_for_redwnl = None
    rows = []
    total_gb = 0.0
    for ch in channels:
        folder = ch.get("name", "") or ch.get("folder", "")
        res = (ch.get("resolution", "") or "").strip() or "—"
        # YTArchiver stores min/max as SECONDS on disk (180 = 3 minutes).
        # Display in minutes to match the original UI + user expectation.
        # sub-minute durations used to floor to "0m" which the
        # renderer then dashed to "—", hiding a real filter. Now:
        # values between 1-59 seconds show as "<1m" so the user knows
        # the filter is set (just non-zero rather than invisible).
        min_d = int(ch.get("min_duration", 0) or 0)
        max_d = int(ch.get("max_duration", 0) or 0)
        min_mins = max(0, min_d // 60)
        max_mins = max(0, max_d // 60)
        # Expose sub-minute sentinels so the UI can render specially.
        if 0 < min_d < 60 and min_mins == 0:
            min_mins = -1  # signals "<1m"
        if 0 < max_d < 60 and max_mins == 0:
            max_mins = -1
        # Last-sync shown as relative ("10hr ago") to match YTArchiver.py:5307.
        # Epoch values are preferred; known legacy strings are parsed through
        # local-time timestamps. Unknown strings render as "unknown" instead
        # of leaking a truncated raw timestamp fragment into the UI.
        ls_raw_val = ch.get("last_sync")
        ls_str = "Never"
        _last_epoch = _last_sync_epoch(ls_raw_val)
        _diff_secs: float | None = (
            max(0.0, time.time() - _last_epoch)
            if _last_epoch is not None else None)
        if (_diff_secs is None and isinstance(ls_raw_val, str)
                and ls_raw_val.strip()):
            ls_str = "unknown"
        if _diff_secs is not None:
            diff_mins = int(_diff_secs // 60)
            if diff_mins < 1:
                ls_str = "just now"
            elif diff_mins < 60:
                ls_str = f"{diff_mins}m ago"
            elif diff_mins < 1440:
                ls_str = f"{diff_mins // 60}hr ago"
            elif diff_mins < 43200:
                ls_str = f"{diff_mins // 1440}d ago"
            else:
                ls_str = f"{diff_mins // 43200}mo ago"
        # Original YTArchiver shows "A ✓" when the channel has auto-<X>=true
        # and the <X>-enabled flag is also on (e.g. `auto_transcribe=True` AND
        # the channel's been transcribed at least once). Match that here by
        # prefixing "A " to the checkmark when the auto_* flag is set.
        # Pending deltas for "A ✓ -X" display: when a channel with
        # auto_transcribe=True has new videos that escaped the auto
        # path (sync downloaded them with the flag momentarily off,
        # or a pipeline hiccup), the Subs cell shows how far behind
        # we are.
        _pending_tx_list = ch.get("pending_tx_ids") or []
        _pending_tx_n = len(_pending_tx_list) if isinstance(_pending_tx_list, list) else 0

        def _mark(auto_key: str, enabled: bool, behind: int = 0,
                  _ch=ch) -> str:
            is_auto = bool(_ch.get(auto_key))
            delta = f" -{behind}" if behind > 0 else ""
            if enabled and is_auto: return f"A \u2713{delta}"
            if enabled: return f"\u2713{delta}"
            if behind > 0: return f"\u2014 -{behind}"
            return "\u2014"
        # Average video size = total size ÷ number of videos. the user wants a
        # quick way to eyeball which channels are shipping big files vs
        # tons of small ones. Displayed in MB (most channels are in that
        # range; GB shown when average is over a gig).
        n_v = int(ch.get("n_vids", 0) or 0)
        size_bytes = int(ch.get("size_bytes", 0) or 0)
        sz_gb = float(ch.get("size_gb", 0) or 0)
        if n_v > 0 and sz_gb > 0:
            avg_mb = (sz_gb * 1024.0) / n_v
            if avg_mb >= 1024:
                avg_str = f"{avg_mb/1024:.1f} GB"
            elif avg_mb >= 100:
                avg_str = f"{int(avg_mb)} MB"
            else:
                avg_str = f"{avg_mb:.0f} MB"
        else:
            avg_str = "\u2014"

        # Pending-redownload probe: the redownload pipeline persists a
        # `_redownload_progress.json` next to the channel's videos while
        # a pass is in flight. When that file is present the Subs table
        # paints a small chartreuse dot next to the channel name so the
        # user sees at a glance which channels still have unfinished
        # resolution upgrades. We ALSO parse the file to extract the
        # target resolution so the right-click menu can show "Continue
        # Redownload at 480p" instead of the generic "Redownload at..."
        # submenu when this channel has pending work.
        _pending_redwnl = False
        _pending_redwnl_res = ""
        if _base_dir and _cfn_for_redwnl is not None:
            try:
                import os as _os
                _ch_folder = _os.path.join(_base_dir, _cfn_for_redwnl(ch))
                _pp = _os.path.join(_ch_folder,
                                     "_redownload_progress.json")
                if _os.path.isfile(_pp):
                    _pending_redwnl = True
                    try:
                        import json as _j
                        with open(_pp, "r", encoding="utf-8") as _f:
                            _data = _j.load(_f)
                        _pending_redwnl_res = (
                            _data.get("resolution") or "").strip()
                    except Exception as e:
                        swallow("pending-redownload resolution read", e)
            except Exception:
                _pending_redwnl = False

        rows.append({
            "folder": folder,
            "res": res + ("p" if res.isdigit() else ""),
            "min": ("<1m" if min_mins == -1
                    else f"{min_mins}m" if min_mins else "—"),
            "max": ("<1m" if max_mins == -1
                    else f"{max_mins}m" if max_mins else "—"),
            "compress": "\u2713" if ch.get("compress_enabled") else "\u2014",
            # Transcribe / Metadata treat the auto flag itself as "enabled".
            # `_pending_tx_n` is the length of pending_tx_ids: videos that
            # downloaded while auto_transcribe was off (the -X indicator).
            "transcribe": _mark("auto_transcribe",
                                 bool(ch.get("auto_transcribe")),
                                 behind=_pending_tx_n),
            "metadata": _mark("auto_metadata", bool(ch.get("auto_metadata"))),
            "last_sync": ls_str,
            "n_vids": f"{ch.get('n_vids', 0):,}" if ch.get("n_vids") else "—",
            # Small channels used to round down to the misleading "0.0 GB".
            # Use the shared size formatter so MB-sized archives stay useful.
            "size": _fmt_size(size_bytes) if size_bytes else "—",
            "size_bytes": size_bytes,
            "size_gb": sz_gb,
            "avg_size": avg_str,
            # Queue-Pending badge derives from the authoritative ID lists.
            # `transcription_pending` is kept as a back-compat mirror but
            # is no longer the source of truth — the IDs are.
            "_pending_tx": _pending_tx_n,
            "_pending_meta": int(ch.get("metadata_pending", 0) or 0),
            # Chartreuse dot indicator in the Subs folder cell — True
            # when `_redownload_progress.json` exists for this channel.
            "_pending_redownload": _pending_redwnl,
            # Saved target resolution so the right-click menu can show
            # "Continue Redownload at 480p" instead of the generic
            # "Redownload at..." submenu when there's pending work.
            "_redownload_res": _pending_redwnl_res,
        })
        total_gb += ch.get("size_gb", 0) or 0

    if total_gb >= 1024:
        total_label = f"Total: {total_gb/1024:.1f} TB"
    elif total_gb > 0:
        total_label = f"Total: {total_gb:.1f} GB"
    else:
        total_label = f"Total: \u2014 ({len(rows)} channels)"
    return rows, total_label


# formatters moved to backend/view_format.py.
# Re-imported here so existing callers (recent_for_ui, etc.) and any
# external `from .ytarchiver_config import _fmt_size` callers still work.
# _extract_video_id moved to backend/view_format.py.
from .view_format import (
    _extract_video_id,  # noqa: F401
    _fmt_dur,
    _fmt_size,
    _fmt_time_ago,
)


def recent_for_ui(cfg: dict[str, Any]):
    """Transform config['recent_downloads'] into UI-ready rows.

    Real entries stored by YTArchiver contain `size` in raw bytes (as str),
    `duration` in raw seconds (as str), `date` as YYYYMMDD, and
    `download_ts` as Unix epoch. We format those into the display strings
    the Recent tab expects.

    Also resolves each row's thumbnail sidecar (via
    `backend.index.find_thumbnail_channelwide`, the same channel-wide lookup
    the Browse grid uses) and exposes it as `thumbnail_url` so the grid-card
    Recent view can render real thumbnails. The legacy table view ignores
    these extras.
    """
    # Pull thumbnail resolver lazily — avoids import-cycle risk since
    # backend.index already imports this module for some helpers.
    try:
        from .index import _file_url as _thumb_url

        # Channel-wide resolver (not the narrow find_thumbnail): catches
        # thumbnails that live in a sibling year/month .Thumbnails/ folder,
        # which the Recent tab otherwise rendered as a gradient placeholder
        # even though the Browse grid showed them fine.
        from .index import find_thumbnail_channelwide as _find_thumb
    except Exception:
        _find_thumb = None
        _thumb_url = None

    out = []
    # Sort newest-first by download_ts BEFORE slicing. Without the
    # explicit sort, users with >200 lifetime downloads could hide
    # fresh entries: any past code path that appended to the END of
    # `recent_downloads` instead of the front leaves new entries in
    # positions 201+, silently truncated by the [:200] slice. Sort
    # guarantees the newest 200 are always shown regardless of
    # insertion order.
    _all_recent = cfg.get("recent_downloads", []) or []
    # .txt issue: if a channel was deleted from Subs (files removed too),
    # its videos kept showing up in Browse > Recent and yielded "file
    # not found" on click. Filter out entries whose file is missing on
    # disk so the Recent list reflects what actually exists.
    def _file_exists_for(entry):
        fp = entry.get("filepath", "") or ""
        if not fp:
            return True  # legacy entries without filepath — keep
        try:
            return os.path.isfile(fp)
        except Exception:
            return True
    _recent_candidates = sorted(
        _all_recent,
        key=lambda r: (r.get("download_ts") or 0),
        reverse=True,
    )[:300]
    _sorted_recent = [
        r for r in _recent_candidates if _file_exists_for(r)
    ][:200]
    # Set of tracked-subscription channel identifiers (name + folder),
    # case-folded. Used to flag each row as `tracked` so the Recent grid
    # menu can hide the channel-only actions (Refresh metadata, Redownload)
    # for loose manual single-video downloads, whose uploader isn't a
    # subscription. Those two actions hard-fail with "Channel not in
    # subscriptions" on untracked rows.
    _tracked_channels = set()
    for _c in (cfg.get("channels", []) or []):
        for _k in ("name", "folder"):
            _v = (_c.get(_k) or "").strip().lower()
            if _v:
                _tracked_channels.add(_v)
    for r in _sorted_recent:
        # Prefer download_ts for the "time ago" column; fall back to any
        # already-formatted `time` field an older config might carry.
        t = _fmt_time_ago(r.get("download_ts")) or r.get("time", "") or ""
        # Size / duration are raw — format them like the original did.
        size_raw = r.get("size", "")
        size_disp = _fmt_size(size_raw)
        dur_disp = _fmt_dur(r.get("duration", ""))
        fp = r.get("filepath", "")
        vid = r.get("video_id") or _extract_video_id(r.get("video_url", ""))

        # Thumbnail resolution for the grid-card view. Best-effort — if the
        # sidecar isn't on disk the grid falls back to its gradient
        # placeholder. Channel-wide so a thumbnail foldered under a different
        # year/month than the mp4 (upload-month vs download-month split)
        # still resolves, matching the Browse grid.
        thumbnail_url = ""
        if fp and _find_thumb and _thumb_url:
            try:
                tp = _find_thumb(fp, vid)
                if tp:
                    thumbnail_url = _thumb_url(tp)
            except Exception as e:
                swallow("thumbnail URL lookup", e)

        # size_bytes — raw int for the grid meta line (also used by the JS
        # `_fmtBytes` helper if it wants to re-format).
        try: size_bytes = int(size_raw) if size_raw not in ("", None) else 0
        except Exception: size_bytes = 0

        # uploaded — prefer explicit `date` (YYYYMMDD) on the entry, fall
        # back to `download_ts` so the grid card still shows something.
        # validate YYYYMMDD parses as a real calendar date
        # before accepting it — otherwise "99999999" stored on a
        # corrupted entry renders as "9999-99-99" and confuses the UI.
        uploaded_disp = ""
        date_str = str(r.get("date") or "")
        _date_ok = False
        if len(date_str) == 8 and date_str.isdigit():
            try:
                import datetime as _dt_v
                _dt_v.datetime.strptime(date_str, "%Y%m%d")
                _date_ok = True
            except ValueError:
                _date_ok = False
        if _date_ok:
            uploaded_disp = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        elif r.get("download_ts"):
            try:
                import datetime as _dt
                uploaded_disp = _dt.datetime.fromtimestamp(
                    float(r["download_ts"])).strftime("%Y-%m-%d")
            except Exception:
                uploaded_disp = ""

        out.append({
            "title": r.get("title", ""),
            "channel": r.get("channel", ""),
            "time": t,
            "duration": dur_disp,
            "size": size_disp,
            # Pass through identifiers so the UI can double-click to open
            # in Watch view with the real video file.
            "filepath": fp,
            "video_id": vid,
            # True when this row's channel is a tracked subscription.
            # The Recent grid menu hides Refresh metadata + Redownload
            # for untracked (manual single-video) downloads since both
            # require a subscription channel and otherwise hard-fail.
            "tracked": (r.get("channel", "") or "").strip().lower()
                        in _tracked_channels,
            # Grid-card extras (ignored by the list view).
            "thumbnail_url": thumbnail_url,
            "size_bytes": size_bytes,
            "uploaded": uploaded_disp,
            # Missing download_ts used to default to 0 (Unix epoch), which
            # the time-ago formatter rendered as "54 years ago" and pushed
            # rows to the top under descending-time sort. None lets the
            # display layer show "—" or similar.
            "download_ts": r.get("download_ts") or None,
        })
    return out


def autorun_history_entries_for_ui(cfg: dict[str, Any]):
    """
    Parse config['autorun_history'] into structured cells for the grid-aligned
    activity-log renderer.

    Real YTArchiver stores each entry as one string like:
        "[Metdta] 3:16pm, Apr 10 \u2014 ExampleChannel \u2014 5 fetched \u00b7 2800 skipped \u00b7 0 errors \u00b7 took 36s"

    We split it by em-dashes into (kind, time/date, channel, body), then split
    the body on bullet-dots into primary/secondary/errors/took.
    """
    import re
    # Was [-100:]; bumped to match the on-disk cap so the UI can show
    # the full scroll-back instead of a tiny one-day window.
    entries = cfg.get("autorun_history", [])[-10000:]
    out = []
    alt = False
    for entry in entries:
        if not isinstance(entry, str):
            continue
        m = re.match(r"^\s*\[\s*(\w+)\s*\]\s*(.*)$", entry)
        if not m:
            out.append({
                "cells": {"kind": "", "time_date": entry,
                          "channel": "", "primary": "", "secondary": "",
                          "errors": "", "took": "", "row_tag": ""},
                "alt": alt,
            })
            alt = not alt
            continue
        kind = m.group(1).strip()
        rest = m.group(2)
        # Split by em-dash surrounded by whitespace
        parts = [p.strip() for p in re.split(r"\s+\u2014\s+", rest)]
        time_date = parts[0] if len(parts) > 0 else ""
        channel = parts[1] if len(parts) > 1 else ""
        body = parts[2] if len(parts) > 2 else ""

        # Split body on middle-dot "·"
        bparts = [p.strip() for p in body.split("\u00b7")]
        primary = bparts[0] if len(bparts) > 0 else ""
        secondary = ""
        tertiary = ""
        errors = ""
        took = ""
        if len(bparts) >= 5:
            # Consolidated [Dwnld] shape ( merged row):
            # primary · transcribed · metadata · errors · took
            # Each count gets its own grid cell in the UI so a wider
            # window uses its horizontal space cleanly (vs. cramming
            # two counts into one cell with internal ellipsis).
            secondary = bparts[1]
            tertiary = bparts[2]
            errors, took = bparts[3], bparts[4]
        elif len(bparts) == 4:
            if kind == "ReDwnl":
                # ReDwnl body: replaced · skipped · errors · took
                # Pack all 3 counts into the first 3 num columns
                # (primary · secondary · tertiary) and leave the
                # errors cell empty. Otherwise the middle tertiary
                # column (reserved for [Dwnld]'s metadata count)
                # renders empty and produces a huge gap between
                # "skipped" and "errors" in the grid. The errors
                # count's "N errors" regex still gets its red
                # highlight from _HIST_HILITE regardless of which
                # cell it sits in.
                secondary = bparts[1]
                tertiary = bparts[2]
                took = bparts[3]
            else:
                # Metdta shape: primary, skipped/refreshed, errors, took
                secondary, errors, took = bparts[1], bparts[2], bparts[3]
        elif len(bparts) == 3:
            # Simpler shape: primary, errors, took
            errors, took = bparts[1], bparts[2]
        elif len(bparts) == 2:
            took = bparts[1]

        tag = _hist_tag_for_kind(kind, body) or ""
        out.append({
            "cells": {
                "kind": kind,
                "time_date": time_date,
                "channel": channel,
                "primary": primary,
                "secondary": secondary,
                "tertiary": tertiary,
                "errors": errors,
                "took": took,
                "row_tag": tag,
            },
            "alt": alt,
        })
        alt = not alt
    return out


def _hist_tag_for_kind(kind: str, rest: str):
    """Pick the row_tag color for a kind. Each match family accepts
    either a non-zero integer OR a single \u2713 checkmark — the latter
    represents "exactly 1 of this" per single-video polish.
    """
    import re
    # Either "\u2713 foo" or "N [optional word] foo" (N >= 1) counts as
    # "work happened". The optional-word slot catches phrases like
    # "N IDs backfilled" or "N comments refreshed" where there's a
    # noun between the count and the verb. Without it, the regex
    # required the digit to be immediately before the verb — so
    # "8235 IDs backfilled" reloaded from autorun_history came back
    # uncolored even though the live emit correctly tagged it pink.
    def _done(pattern: str) -> bool:
        check = re.search(r"\u2713\s+(?:\w+\s+)?(?:" + pattern + r")\b", rest)
        if check:
            return True
        m = re.search(r"\b(\d+)\s+(?:\w+\s+)?(?:" + pattern + r")\b", rest)
        return bool(m and int(m.group(1)) > 0)

    if kind == "Trnscr":
        if _done("transcribed"):
            return "hist_blue"
    elif kind == "Metdta":
        # "fetched" / "refreshed" cover the original bulk-views +
        # per-video metadata paths. "backfilled" covers the ID
        # backfill pass (body: "N IDs backfilled"). Without the
        # third verb, backfill rows reloaded from autorun_history
        # came back in the default (white) color instead of the
        # pink all metadata-kind rows should render in.
        if _done("fetched") or _done("refreshed") or _done("backfilled"):
            return "hist_pink"
    elif kind in ("Manual", "Auto", "Dwnld"):
        if _done("downloaded"):
            return "hist_green"
    elif kind == "ReDwnl":
        if _done("replaced") or "running..." in rest:
            return "hist_redwnl"
    elif kind == "Cmprss":
        if _done("compressed"):
            return "hist_compress"
    elif kind == "Reorg":
        if _done("moved|reorganized"):
            return "hist_reorg"
    return None
