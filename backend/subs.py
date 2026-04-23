"""
Subs CRUD — add / edit / remove channels in the real YTArchiver config.

This is the first real backend module. Writes are gated by
config_is_writable() so we don't stomp on an externally-locked config.

Schema matches YTArchiver.py's CHANNEL_DEFAULTS (line 173):
    {
        "name": "ExampleChannel",
        "url": "https://www.youtube.com/@ExampleChannel",
        "resolution": "720" | "1080" | "best" | "audio" | ...,
        "mode": "full" | "new" | "fromdate", # range radio
        "min_duration": 3, # minutes
        "max_duration": 0, # 0 = no cap
        "split_years": False,
        "split_months": False,
        "auto_transcribe": False,
        "auto_metadata": True,
        "compress_enabled": False,
        "compress_level": "Generous" | "Average" | "Below Average",
        "compress_output_res": "1080" | "720" | ...,
        "last_sync": "",
        "from_date": "YYYY-MM-DD" | "",
    }
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from .ytarchiver_config import (
    load_config, save_config, CHANNEL_DEFAULTS_ALL,
)


def normalize_channel_url(url: str) -> str:
    """Normalize a YouTube channel URL to canonical form.

    Accepts:
        @handle
        https://www.youtube.com/@handle
        youtube.com/@handle
        /@handle
        channel/UC...
        c/customname
        user/username

    Returns a canonical form (adds scheme + www if missing). Does NOT append
    `/videos` — use `ensure_videos_suffix()` for that when the caller needs
    the all-videos playlist rather than the channel home.
    """
    if not url:
        return ""
    url = url.strip()
    # bare @handle → full URL
    if url.startswith("@"):
        return f"https://www.youtube.com/{url}"
    # /@handle
    if url.startswith("/@"):
        return f"https://www.youtube.com{url}"
    # no scheme → add https
    if not url.startswith(("http://", "https://")):
        if url.startswith("youtube.com") or url.startswith("www.youtube.com"):
            return "https://" + url.lstrip("/")
        if url.startswith("/"):
            return "https://www.youtube.com" + url
        # assume bare handle without @
        return f"https://www.youtube.com/@{url}"
    return url


def streams_url(url: str) -> Optional[str]:
    """Return the `/streams` tab URL for a channel, or None for non-channel URLs.

    Mirrors YTArchiver.py:17303 `_get_streams_url`. Used by sync to do a
    second pass after the main /videos enumeration so past livestreams that
    YouTube filed under /streams (not /videos) are also caught.
    """
    if not url:
        return None
    u = url.rstrip("/")
    lower = u.lower()
    ch_markers = ("/@", "/channel/", "/c/", "/user/")
    if not any(m in lower for m in ch_markers):
        return None
    # Strip any existing tab suffix
    import re as _re
    u = _re.sub(
        r"/(videos|shorts|streams|playlists|community|podcasts|channels|featured|about)$",
        "", u, flags=_re.IGNORECASE)
    return u + "/streams"


def ensure_videos_suffix(url: str) -> str:
    """Append `/videos` to a channel URL so yt-dlp walks the full video list.

    Mirrors YTArchiver.py:2594 _ensure_videos_tab. No-op for URLs that
    already end in /videos, /shorts, /streams, /playlists, /community,
    /featured, or that aren't channel-type URLs (e.g. /watch, /playlist).
    """
    if not url:
        return url
    base = url.rstrip("/")
    lower = base.lower()
    skip_tails = ("/videos", "/shorts", "/streams", "/playlists",
                  "/community", "/featured", "/about")
    for t in skip_tails:
        if lower.endswith(t):
            return base
    # Don't touch single-video or playlist URLs
    if "/watch" in lower or "/playlist" in lower:
        return base
    # Channel-type endings we want to extend: /@handle, /channel/UC...,
    # /c/name, /user/name
    ch_markers = ("/@", "/channel/", "/c/", "/user/")
    if any(m in lower for m in ch_markers):
        return base + "/videos"
    return base


def validate_channel_url(url: str) -> Tuple[bool, str]:
    """Return (ok, error_msg)."""
    url = url.strip()
    if not url:
        return False, "URL is required."
    norm = normalize_channel_url(url)
    parsed = urlparse(norm)
    if parsed.scheme not in ("http", "https"):
        return False, "URL must start with http:// or https://."
    if "youtube.com" not in parsed.netloc and "youtu.be" not in parsed.netloc:
        return False, "URL must be a youtube.com link."
    path = parsed.path.strip("/")
    if not path:
        return False, "URL must include a channel path (/@handle, /channel/UC..., /c/name, /user/name)."
    return True, ""


def _find_channel(channels: List[Dict[str, Any]], match: Dict[str, str]) -> Optional[int]:
    """Find the index of a channel matching by url, name, or folder."""
    match_url = normalize_channel_url(match.get("url", "")) if match.get("url") else ""
    match_name = (match.get("name") or match.get("folder") or "").strip().lower()
    for i, ch in enumerate(channels):
        ch_url = normalize_channel_url(ch.get("url", ""))
        if match_url and ch_url == match_url:
            return i
        ch_name = (ch.get("name") or ch.get("folder") or "").strip().lower()
        if match_name and ch_name == match_name:
            return i
    return None


def _apply_defaults(ch: Dict[str, Any]) -> Dict[str, Any]:
    """Merge in defaults for any missing fields."""
    out = dict(CHANNEL_DEFAULTS_ALL)
    out.update(ch)
    # Normalize URL
    if out.get("url"):
        out["url"] = normalize_channel_url(out["url"])
    # Ensure name == folder (tkinter app uses name; UI uses folder)
    if out.get("folder") and not out.get("name"):
        out["name"] = out["folder"]
    elif out.get("name") and not out.get("folder"):
        out["folder"] = out["name"]
    return out


def _payload_to_channel(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Map UI payload shape to YTArchiver's channel shape.

    min_duration / max_duration: the UI sends MINUTES (to match how the
    tkinter app's Min / Max fields display). We store SECONDS on disk
    because YTArchiver's own load_config migration promotes raw-seconds
    legacy data to minutes*60 — meaning every live value on disk is in
    seconds. Converting here keeps the drop-in replacement lossless.
    """
    def _mins_to_secs(v):
        try: return max(0, int(v)) * 60
        except Exception: return 0
    ch = {
        "name": (payload.get("folder") or payload.get("name") or "").strip(),
        "folder": (payload.get("folder") or payload.get("name") or "").strip(),
        "url": normalize_channel_url(payload.get("url", "")),
        "resolution": str(payload.get("resolution", "720")),
        "min_duration": _mins_to_secs(payload.get("min_duration")),
        "max_duration": _mins_to_secs(payload.get("max_duration")),
        "auto_transcribe": bool(payload.get("auto_transcribe")),
        "auto_metadata": bool(payload.get("auto_metadata", True)),
        "compress_enabled": bool(payload.get("compress_enabled")),
    }
    # Range mapping: subscribe (default, new uploads only) / all / fromdate
    # bug M-6: keep `date_after` in sync with `from_date` so legacy config
    # readers that look at the older field name don't see a blank value
    # after a UI save. Sync itself reads from_date (sync.py:399) — date_after
    # is legacy from Classic. Writing both keeps migration lossless.
    range_val = payload.get("range", "subscribe")
    if range_val == "all":
        ch["mode"] = "full"
        ch["from_date"] = ""
        ch["date_after"] = ""
    elif range_val == "fromdate":
        ch["mode"] = "fromdate"
        _fd = payload.get("from_date", "").strip()
        ch["from_date"] = _fd
        ch["date_after"] = _fd
    else:
        ch["mode"] = "new"
        ch["from_date"] = ""
        ch["date_after"] = ""
    # Folder org mapping: flat / years / months
    org = payload.get("folder_org", "years")
    ch["split_years"] = (org in ("years", "months"))
    ch["split_months"] = (org == "months")
    # Compress details
    if ch["compress_enabled"]:
        ch["compress_level"] = payload.get("compress_level", "Generous")
        ch["compress_output_res"] = str(payload.get("compress_output_res", "720"))
    else:
        ch["compress_level"] = ""
        ch["compress_output_res"] = ""
    return _apply_defaults(ch)


# ── Public API ─────────────────────────────────────────────────────────

class SubsError(Exception):
    pass


def list_channels() -> List[Dict[str, Any]]:
    cfg = load_config()
    return list(cfg.get("channels", []))


def fetch_channel_display_name(url: str, timeout_sec: int = 15) -> Optional[str]:
    """Best-effort: use yt-dlp to resolve a URL to its canonical channel name.
    Returns None on failure (so UI can fall back to user-supplied name).

    yt-dlp's `--print channel` on a flat-playlist probe frequently returns
    "NA" because channel pages don't carry per-video channel metadata in
    the flat listing. Fall back to `uploader` when that happens, and as a
    last resort drop the flat-playlist flag and fetch the first video's
    full metadata (slower but reliable).
    """
    try:
        import subprocess as _sp
        from . import sync as _sync
        yt = _sync.find_yt_dlp()
        if not yt:
            return None
        # Pass 1: flat-playlist with channel+uploader fields
        cmd = [
            yt, "--flat-playlist", "--playlist-end", "1",
            "--print", "%(channel,uploader,playlist_title)s",
            "--no-warnings", "--quiet",
            *_sync._find_cookie_source(),
            normalize_channel_url(url),
        ]
        r = _sp.run(cmd, capture_output=True, text=True,
                    timeout=timeout_sec, startupinfo=_sync._startupinfo)
        raw_out = (r.stdout or "").strip()
        name = raw_out.split("\n")[0].strip() if raw_out else ""
        # yt-dlp sentinel for "not available" is the literal string "NA"
        if name in ("", "NA"):
            # Pass 2: resolve one video fully (no --flat-playlist) so
            # yt-dlp returns the real metadata including channel name.
            cmd2 = [
                yt, "--playlist-end", "1", "--skip-download",
                "--print", "%(channel,uploader,playlist_title)s",
                "--no-warnings", "--quiet",
                *_sync._find_cookie_source(),
                normalize_channel_url(url),
            ]
            r2 = _sp.run(cmd2, capture_output=True, text=True,
                         timeout=timeout_sec + 15,
                         startupinfo=_sync._startupinfo)
            raw_out = (r2.stdout or "").strip()
            name = raw_out.split("\n")[0].strip() if raw_out else ""
        if name == "NA":
            name = ""
        # yt-dlp sometimes returns the channel's "Videos" tab title instead
        # of the bare channel name — e.g. "Deep Dive Documentaries - Videos".
        # Strip common tab suffixes. Same treatment as OLD YTArchiver.
        for suffix in (" - Videos", " - Playlists", " - Shorts",
                       " - Streams", " - Home"):
            if name.endswith(suffix):
                name = name[:-len(suffix)].strip()
                break
        return name or None
    except Exception:
        return None


def add_channel(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Append a new channel. Raises SubsError on invalid input or duplicate.

    Unspecified fields fall back to the user's configured defaults (resolution,
    min_duration, auto_metadata) so adding a bare "@handle" gets sensible values.
    """
    ok, err = validate_channel_url(payload.get("url", ""))
    if not ok:
        raise SubsError(err)
    # Apply user defaults for any unspecified optional fields.
    # Note: payload comes from the UI, so min/max_duration are MINUTES.
    # cfg["min_duration"] is stored in SECONDS, so convert back to minutes
    # when using it as a default.
    try:
        cfg_defaults = load_config()
        payload = dict(payload)
        if "resolution" not in payload or payload["resolution"] in (None, ""):
            payload["resolution"] = cfg_defaults.get("default_resolution", "720")
        if "min_duration" not in payload or payload["min_duration"] in (None, "", 0):
            default_secs = int(cfg_defaults.get("min_duration", 180) or 180)
            payload["min_duration"] = max(0, default_secs // 60) # minutes
        if "auto_metadata" not in payload:
            payload["auto_metadata"] = True
    except Exception:
        pass
    # Strip-check (not just truthy) — whitespace-only values like " " are
    # effectively blank after _payload_to_channel's .strip(). Without this,
    # " " passes the truthy guard, skips the auto-fetch, then gets stored
    # as "" downstream → sync_channel computes `sanitize_folder("") ==
    # "_unnamed"` and every download for this channel lands in the shared
    # `_unnamed/` graveyard folder. That's exactly how 28 files got
    # orphaned in the user's archive; this guard closes the door.
    if not (str(payload.get("folder") or "").strip()
            or str(payload.get("name") or "").strip()):
        # Auto-fetch the canonical channel name via yt-dlp
        fetched = fetch_channel_display_name(payload.get("url", ""))
        if fetched and fetched.strip():
            payload["folder"] = fetched.strip()
            payload["name"] = fetched.strip()
        else:
            raise SubsError("Folder name is required (and auto-fetch from URL failed).")
    new_ch = _payload_to_channel(payload)
    # Final sanity: after all mapping+stripping, name must be non-blank.
    # Belt-and-suspenders guard so we never persist a channel that would
    # route downloads to _unnamed/ at sync time.
    if not (new_ch.get("name") or "").strip():
        raise SubsError(
            "Channel folder name could not be determined from the URL. "
            "Provide a folder name explicitly.")
    cfg = load_config()
    channels = cfg.setdefault("channels", [])
    # Check dup by URL or folder name
    if _find_channel(channels, {"url": new_ch["url"]}) is not None:
        raise SubsError("A channel with that URL already exists.")
    if _find_channel(channels, {"name": new_ch["name"]}) is not None:
        raise SubsError("A channel with that folder name already exists.")
    channels.append(new_ch)
    # Sort alphabetically by name (matches YTArchiver's usual ordering)
    channels.sort(key=lambda c: (c.get("name") or "").lower())
    if not save_config(cfg):
        # Gated — return the proposed channel anyway so the UI can show it
        return {**new_ch, "_write_blocked": True}
    return new_ch


def update_channel(identity: Dict[str, str], payload: Dict[str, Any]) -> Dict[str, Any]:
    """Update an existing channel matched by identity (url or name/folder).

    If the folder name changed, rename the on-disk folder too so the user's
    archive stays in sync with the config (safer than leaving orphaned dirs).
    """
    cfg = load_config()
    channels = cfg.setdefault("channels", [])
    idx = _find_channel(channels, identity)
    if idx is None:
        raise SubsError(f"Channel not found: {identity}")
    existing = channels[idx]
    # Partial-update safety: if the caller passed a sparse payload (e.g.
    # only {"name": "X"} to rename), we must not let `_payload_to_channel`
    # rebuild the whole dict from DEFAULTS — that would silently wipe the
    # URL / auto_transcribe / mode / etc. Detect sparse payloads and
    # merge on top of the existing channel so unmentioned fields survive.
    sparse_payload = not payload.get("url") and "url" not in payload
    if sparse_payload:
        # Merge: start from existing, overlay payload keys directly
        merged = dict(existing)
        # Handle known UI-shape fields — these need conversion
        for k, v in payload.items():
            if k == "min_duration" and isinstance(v, (int, str)):
                # Accept either seconds or minutes; heuristic: if less than
                # 1000, assume minutes and convert.
                try:
                    n = int(v)
                    merged[k] = n * 60 if n < 1000 else n
                except Exception:
                    pass
            elif k == "max_duration" and isinstance(v, (int, str)):
                try:
                    n = int(v)
                    merged[k] = n * 60 if n < 1000 else n
                except Exception:
                    pass
            elif k == "folder_org":
                merged["split_years"] = (v in ("years", "months"))
                merged["split_months"] = (v == "months")
            elif k == "range":
                if v == "all": merged["mode"] = "full"
                elif v == "fromdate": merged["mode"] = "fromdate"
                else: merged["mode"] = "new"
            else:
                merged[k] = v
        # When name changed, keep folder in sync if not explicitly set
        if "name" in payload and "folder" not in payload:
            merged["folder"] = merged["name"]
        # Reverse case: when ONLY folder changed, keep name in sync.
        # Without this, editing a channel's folder from "Valve News"
        # to "Tyler McVicker" would leave `name` stale as "Valve
        # News" — sync would correctly write to the new folder, but
        # the Subs table's display-name column, the Browse channel
        # grid, and the tray tooltip would all still show the old
        # name until a fresh full-payload edit landed.
        if "folder" in payload and "name" not in payload:
            merged["name"] = merged["folder"]
        updated = merged
    else:
        updated = _payload_to_channel(payload)
        # Preserve every field we don't edit in the UI — last_sync, disk
        # stats, bootstrap/batch state, transcription/metadata counters,
        # etc. Without this merge, editing a channel's resolution would
        # silently wipe its init_batch_after cooldown.
        _preserve = (
            "last_sync", "n_vids", "size_gb", "size_bytes",
            "initialized", "init_complete", "init_batch_after",
            "batch_resume_index",
            "transcription_complete", "transcription_pending",
            "metadata_pending",
            "folder_override",
        )
        for key in _preserve:
            if key in existing and key not in payload:
                updated[key] = existing[key]

    # Detect folder rename → move the on-disk folder
    import os as _os
    from . import sync as _sync
    old_name = (existing.get("name") or existing.get("folder") or "").strip()
    new_name = (updated.get("name") or updated.get("folder") or "").strip()
    if old_name and new_name and old_name != new_name:
        base = (cfg.get("output_dir") or "").strip()
        if base:
            old_path = _os.path.join(base, _sync.sanitize_folder(old_name))
            new_path = _os.path.join(base, _sync.sanitize_folder(new_name))
            if _os.path.isdir(old_path) and not _os.path.exists(new_path):
                try:
                    _os.rename(old_path, new_path)
                    updated["_folder_renamed"] = {"from": old_path, "to": new_path}
                except OSError as e:
                    # bug S-4: old code just captured the error and
                    # saved the new name anyway, creating a
                    # disk-vs-config mismatch (config says "NewName",
                    # folder is still "OldName", next sync creates an
                    # empty "NewName" folder alongside). Preserve the
                    # old name and surface the error so the UI can
                    # toast. The caller (main.py subs_update_channel)
                    # checks `_folder_rename_error` in the response.
                    updated["name"] = old_name
                    updated["folder"] = old_name
                    if "folder_override" in updated:
                        updated["folder_override"] = old_name
                    updated["_folder_rename_error"] = str(e)

    # Sanity: refuse to save a channel with a blanked-out name. Matches the
    # add_channel guard — prevents sync from routing to _unnamed/ on the
    # next run if the user accidentally cleared the name field in the UI.
    if not (updated.get("name") or "").strip():
        raise SubsError(
            "Channel name cannot be blank \u2014 syncs would route to the "
            "shared `_unnamed/` graveyard folder. Provide a name.")
    channels[idx] = updated
    channels.sort(key=lambda c: (c.get("name") or "").lower())
    if not save_config(cfg):
        return {**updated, "_write_blocked": True}
    return updated


def remove_channel(identity: Dict[str, str],
                   delete_files: bool = False) -> Dict[str, Any]:
    """Remove a channel from the subs list.

    If `delete_files=True`, also recursively delete the channel's on-disk
    folder (videos, transcripts, metadata, thumbnails, .ChannelArt — the
    whole folder). Returns {ok, deleted_folder, delete_error?}. The delete
    is best-effort; if it partially fails the subscription is still removed
    so the user isn't stuck with a broken record.
    """
    import shutil
    cfg = load_config()
    channels = cfg.setdefault("channels", [])
    idx = _find_channel(channels, identity)
    if idx is None:
        raise SubsError(f"Channel not found: {identity}")
    ch = dict(channels[idx])
    channels.pop(idx)
    saved = save_config(cfg)

    result: Dict[str, Any] = {"ok": bool(saved), "deleted_folder": False}
    if saved and delete_files:
        base = (cfg.get("output_dir") or "").strip()
        if base:
            try:
                from .sync import channel_folder_name as _cfn
                folder_name = _cfn(ch)
                folder_path = os.path.join(base, folder_name)
                if os.path.isdir(folder_path):
                    shutil.rmtree(folder_path, ignore_errors=False)
                    result["deleted_folder"] = True
                    result["folder_path"] = folder_path
            except Exception as e:
                result["delete_error"] = str(e)
    return result


def get_channel(identity: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Return a channel dict by url/name/folder. The raw on-disk schema
    stores min/max_duration as seconds; this is the unchanged disk record.
    Use `get_channel_for_ui()` to receive the UI-formatted dict (minutes).
    """
    cfg = load_config()
    idx = _find_channel(cfg.get("channels", []), identity)
    if idx is None:
        return None
    return dict(cfg["channels"][idx])


def get_channel_for_ui(identity: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Return a channel dict with min/max_duration converted to minutes
    (the unit the UI displays + edits in). Used by the Edit-channel panel.
    """
    ch = get_channel(identity)
    if ch is None:
        return None
    ch = dict(ch)
    try: ch["min_duration"] = max(0, int(ch.get("min_duration") or 0) // 60)
    except Exception: ch["min_duration"] = 0
    try: ch["max_duration"] = max(0, int(ch.get("max_duration") or 0) // 60)
    except Exception: ch["max_duration"] = 0
    return ch
