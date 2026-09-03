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

import copy
import datetime as dt
import os
import re
import uuid
from typing import Any
from urllib.parse import urlparse

from .log import get_logger
from .services.channel_leases import (
    LeaseOwner,
    channel_aliases,
    channel_leases,
    global_archive_aliases,
)
from .services.channel_transactions import (
    ChannelTransactionConflict,
    ChannelTransactionJournalError,
    apply_channel_config_patch,
    build_channel_config_patch,
    channel_transaction_aliases,
    checkpoint_channel_transaction,
    clear_channel_transaction,
    load_channel_transaction,
    make_remove_transaction,
    make_rename_transaction,
    mark_channel_recovery_required,
    write_channel_transaction,
)
from .ytarchiver_config import (
    CHANNEL_DEFAULTS_ALL,
    config_transaction,
    load_config,
    save_config,  # noqa: F401 - retained for test/extension monkeypatch compatibility
)

_log = get_logger(__name__)


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
        if url.startswith(("youtube.com", "www.youtube.com")):
            return "https://" + url.lstrip("/")
        if url.startswith("/"):
            return "https://www.youtube.com" + url
        # assume bare handle without @
        if re.fullmatch(r"[A-Za-z0-9_-]{2,30}", url):
            return f"https://www.youtube.com/@{url}"
        return url
    return url


def streams_url(url: str) -> str | None:
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


def validate_channel_url(url: str) -> tuple[bool, str]:
    """Return (ok, error_msg)."""
    url = url.strip()
    if not url:
        return False, "URL is required."
    norm = normalize_channel_url(url)
    parsed = urlparse(norm)
    if parsed.scheme not in ("http", "https"):
        return False, "URL must start with http:// or https://."
    # reject youtu.be — that's the short video-URL form and
    # never hosts channels. Accepting it let users paste a video URL,
    # pass validation, then sync tried to walk "a channel" built from
    # one video's URL → garbage results silently.
    host = (parsed.hostname or "").lower()
    if host not in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
        return False, "URL must be a youtube.com channel link (not youtu.be)."
    path = parsed.path.strip("/")
    if not path:
        return False, "URL must include a channel path (/@handle, /channel/UC..., /c/name, /user/name)."
    # verify the path LOOKS like a channel path, not a
    # watch/playlist URL. Accepting /watch?v=... as "a channel" meant
    # sync enumerated a single-video URL and produced silent
    # wrong-result output.
    _valid_prefixes = ("@", "channel/UC", "c/", "user/")
    if not any(path.startswith(p) for p in _valid_prefixes):
        return False, ("URL doesn't look like a channel path. "
                       "Expected one of: /@handle, /channel/UC..., /c/name, /user/name. "
                       "Watch (/watch?v=...) and playlist URLs are not channels.")
    return True, ""


def _find_channel(channels: list[dict[str, Any]], match: dict[str, str]) -> int | None:
    """Find the index of a channel matching by url, name, or folder."""
    match_url = (normalize_channel_url(match.get("url", "")).rstrip("/")
                 if match.get("url") else "")
    match_name = (match.get("name") or match.get("folder") or "").strip().lower()
    for i, ch in enumerate(channels):
        ch_url = normalize_channel_url(ch.get("url", "")).rstrip("/")
        if match_url and ch_url == match_url:
            return i
        ch_name = (ch.get("name") or ch.get("folder") or "").strip().lower()
        if match_name and ch_name == match_name:
            return i
    return None


def _apply_defaults(ch: dict[str, Any]) -> dict[str, Any]:
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


def _payload_to_channel(payload: dict[str, Any]) -> dict[str, Any]:
    """Map UI payload shape to YTArchiver's channel shape.

    min_duration / max_duration: the UI sends MINUTES (to match how the
    tkinter app's Min / Max fields display). We store SECONDS on disk
    because YTArchiver's own load_config migration promotes raw-seconds
    legacy data to minutes*60 — meaning every live value on disk is in
    seconds. Converting here keeps the drop-in replacement lossless.
    """
    def _mins_to_secs(v):
        if v is None or (isinstance(v, str) and not v.strip()):
            return 0
        if isinstance(v, bool):
            raise SubsError("Minimum and maximum length must be whole numbers.")
        text = str(v).strip()
        if not re.fullmatch(r"-?\d+", text):
            raise SubsError("Minimum and maximum length must be whole numbers.")
        return int(text) * 60
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
    # keep `date_after` in sync with `from_date` so legacy config
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


def _validate_channel_constraints(channel: dict[str, Any]) -> None:
    """Reject settings that would make a sync silently do the wrong work."""
    try:
        minimum = int(channel.get("min_duration") or 0)
        maximum = int(channel.get("max_duration") or 0)
    except (OverflowError, TypeError, ValueError) as exc:
        raise SubsError("Minimum and maximum length must be whole numbers.") \
            from exc
    if minimum < 0 or maximum < 0:
        raise SubsError("Minimum and maximum length cannot be negative.")
    if minimum and maximum and minimum > maximum:
        raise SubsError(
            "Minimum length cannot be greater than maximum length."
        )

    mode = str(channel.get("mode") or "new").strip().lower()
    if mode not in {"fromdate", "date"}:
        return
    from_date = str(
        channel.get("from_date") or channel.get("date_after") or ""
    ).strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", from_date):
        raise SubsError("From date must include a valid year, month, and day.")
    try:
        dt.date.fromisoformat(from_date)
    except ValueError as exc:
        raise SubsError("From date is not a valid calendar date.") from exc


# ── Public API ─────────────────────────────────────────────────────────

class SubsError(Exception):
    """Raised for user-correctable subscription add/update failures."""



def list_channels() -> list[dict[str, Any]]:
    cfg = load_config()
    return list(cfg.get("channels", []))


def fetch_channel_display_name(url: str, timeout_sec: int = 15) -> str | None:
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
        from . import youtube_traffic
        from .process_runner import PROCESS_REGISTRY, popen_ytdlp
        yt = _sync.find_yt_dlp()
        if not yt:
            return None
        # Helper that registers the spawned proc with the global
        # registry so a shutdown mid-call kills the yt-dlp probe child
        # cleanly. Old subprocess.run path bypassed the registry, so
        # shutting down while adding a channel left yt-dlp.exe running
        # for up to 30s (audit: subs.py:280-296).
        def _run_registered(argv, timeout):
            permission = youtube_traffic.acquire("channel_name_probe")
            if not permission.get("ok"):
                return ""
            proc = popen_ytdlp(
                argv, stdout=_sp.PIPE, stderr=_sp.PIPE,
                text=True, encoding="utf-8", errors="replace",
                startupinfo=_sync._startupinfo,
                registry=PROCESS_REGISTRY)
            try:
                try:
                    out, _err = proc.communicate(timeout=timeout)
                    try:
                        from .youtube_session import handle_youtube_failure_text
                        if handle_youtube_failure_text(
                                _err or "",
                                context="resolving a channel name"):
                            return ""
                    except Exception as _guard_error:
                        _log.debug(
                            "channel-name YouTube guard failed: %s",
                            _guard_error)
                except _sp.TimeoutExpired:
                    try: proc.kill()
                    except Exception: pass
                    try: proc.communicate(timeout=5)
                    except Exception: pass
                    return ""
            finally:
                try: PROCESS_REGISTRY.unregister(proc)
                except Exception: pass
            return out or ""
        def _probe_name(cookie_args: list[str]) -> str:
            # Pass 1: flat-playlist with channel+uploader fields.
            cmd = [
                yt, "--flat-playlist", "--playlist-end", "1",
                "--print", "%(channel,uploader,playlist_title)s",
                "--no-warnings", "--quiet",
                *cookie_args,
                normalize_channel_url(url),
            ]
            raw_out = _run_registered(cmd, timeout_sec).strip()
            name = raw_out.split("\n")[0].strip() if raw_out else ""
            # yt-dlp sentinel for "not available" is the literal string "NA".
            if name not in ("", "NA"):
                return name
            # Pass 2: resolve one video fully (no --flat-playlist) so
            # yt-dlp returns the real metadata including channel name.
            cmd2 = [
                yt, "--playlist-end", "1", "--skip-download",
                "--print", "%(channel,uploader,playlist_title)s",
                "--no-warnings", "--quiet",
                *cookie_args,
                normalize_channel_url(url),
            ]
            raw_out = _run_registered(cmd2, timeout_sec + 15).strip()
            return raw_out.split("\n")[0].strip() if raw_out else ""

        name = _probe_name([])
        if name in ("", "NA"):
            # Cosmetic channel-name probes should not exercise the user's
            # YouTube cookies unless the public probe failed.
            cookie_args = _sync._find_cookie_source() or []
            if cookie_args:
                name = _probe_name(cookie_args)
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


def add_channel(payload: dict[str, Any]) -> dict[str, Any]:
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
        min_value = payload.get("min_duration")
        min_unspecified = (
            "min_duration" not in payload
            or min_value is None
            or (isinstance(min_value, str) and not min_value.strip())
        )
        if min_unspecified:
            raw_default = cfg_defaults.get("min_duration", 180)
            default_secs = int(
                180 if raw_default in (None, "") else raw_default)
            payload["min_duration"] = max(0, default_secs // 60) # minutes
        if "auto_metadata" not in payload:
            payload["auto_metadata"] = True
    except Exception as e:
        _log.debug("swallowed: %s", e)
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
    _validate_channel_constraints(new_ch)
    # T123: run the dup-check + append + save as one atomic transaction so
    # a concurrent worker save (e.g. a sync's last_sync write) can't load a
    # stale snapshot mid-add and clobber the new channel (or vice versa).
    try:
        with config_transaction() as cfg:
            channels = cfg.setdefault("channels", [])
            # Check dup by URL or folder name
            if _find_channel(channels, {"url": new_ch["url"]}) is not None:
                raise SubsError("A channel with that URL already exists.")
            if _find_channel(channels, {"name": new_ch["name"]}) is not None:
                raise SubsError(
                    "A channel with that folder name already exists.")
            from .sync import channel_folder_name
            wanted_folder = channel_folder_name(new_ch).casefold()
            if wanted_folder and any(
                    channel_folder_name(channel).casefold() == wanted_folder
                    for channel in channels if isinstance(channel, dict)):
                raise SubsError(
                    "Another channel already uses that archive folder.")
            channels.append(new_ch)
            # Sort alphabetically by name (matches YTArchiver's ordering)
            channels.sort(key=lambda c: (c.get("name") or "").lower())
    except SubsError:
        raise
    except OSError:
        # config_transaction raises OSError when the atomic save fails
        # (disk full, AV lock, write-gate). The transaction's cfg is a
        # private deep copy that's now discarded, so there is no ghost
        # channel to roll back — just report the write-block to the UI.
        return {**new_ch, "_write_blocked": True}
    return new_ch


class _SnapshotRestoreAbort(RuntimeError):
    """Exit a config transaction without saving an unchanged document."""

    def __init__(self, result: dict[str, Any]) -> None:
        super().__init__(str(result.get("error") or "unchanged"))
        self.result = result


def restore_channel_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Re-add one exact channel record saved in a v2 Trash manifest.

    This intentionally does *not* call :func:`add_channel`: that public Add
    path converts duration units, fills current UI defaults, and may perform a
    network name lookup.  A Trash restore must reproduce the record that was
    removed, including fields introduced by newer versions of the app.
    """
    if not isinstance(snapshot, dict):
        return {"ok": False, "added": False,
                "error": "Trash entry has no valid channel snapshot."}
    candidate = copy.deepcopy(snapshot)
    name = str(candidate.get("name") or candidate.get("folder") or "").strip()
    url = normalize_channel_url(str(candidate.get("url") or ""))
    if not name or not url:
        return {"ok": False, "added": False,
                "error": "Saved channel details are incomplete."}
    candidate["url"] = url
    if not str(candidate.get("name") or "").strip():
        candidate["name"] = name
    if not str(candidate.get("folder") or "").strip():
        candidate["folder"] = name

    from .sync import channel_folder_name

    wanted_folder = channel_folder_name(candidate).casefold()
    stable_keys = ("channel_id", "id", "stable_key")
    try:
        with config_transaction() as cfg:
            channels = cfg.setdefault("channels", [])
            for current in channels:
                if not isinstance(current, dict):
                    continue
                if current == candidate:
                    raise _SnapshotRestoreAbort({
                        "ok": True,
                        "added": False,
                        "already_present": True,
                        "channel": copy.deepcopy(current),
                    })
                current_url = normalize_channel_url(
                    str(current.get("url") or ""))
                if current_url and current_url == url:
                    raise _SnapshotRestoreAbort({
                        "ok": False, "added": False,
                        "error": "A channel with that URL already exists.",
                    })
                current_name = str(
                    current.get("name") or current.get("folder") or ""
                ).strip().casefold()
                if current_name and current_name == name.casefold():
                    raise _SnapshotRestoreAbort({
                        "ok": False, "added": False,
                        "error": "A channel with that name already exists.",
                    })
                if wanted_folder and channel_folder_name(current).casefold() == wanted_folder:
                    raise _SnapshotRestoreAbort({
                        "ok": False, "added": False,
                        "error": "Another channel already uses that archive folder.",
                    })
                for key in stable_keys:
                    wanted = str(candidate.get(key) or "").strip().casefold()
                    existing = str(current.get(key) or "").strip().casefold()
                    if wanted and existing and wanted == existing:
                        raise _SnapshotRestoreAbort({
                            "ok": False, "added": False,
                            "error": "That saved channel is already configured.",
                        })
            channels.append(candidate)
            channels.sort(key=lambda channel: str(
                channel.get("name") or "").casefold())
    except _SnapshotRestoreAbort as exc:
        return exc.result
    except (OSError, TypeError, ValueError) as exc:
        return {"ok": False, "added": False, "error": str(exc)}
    return {"ok": True, "added": True, "already_present": False,
            "channel": copy.deepcopy(candidate)}


def rollback_restored_channel_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Remove only the unchanged record inserted by a failed Trash restore."""
    if not isinstance(snapshot, dict):
        return {"ok": False, "removed": False,
                "error": "Saved channel details are invalid."}
    wanted_url = normalize_channel_url(str(snapshot.get("url") or ""))
    try:
        with config_transaction() as cfg:
            channels = cfg.setdefault("channels", [])
            matches = [
                (index, channel)
                for index, channel in enumerate(channels)
                if isinstance(channel, dict)
                and normalize_channel_url(str(channel.get("url") or "")) == wanted_url
            ]
            if len(matches) != 1:
                raise _SnapshotRestoreAbort({
                    "ok": False, "removed": False,
                    "error": "Could not uniquely identify the restored channel.",
                })
            index, current = matches[0]
            # Config loading may add newly introduced defaults.  Every value
            # that was actually saved in the manifest must still match before
            # rollback is allowed to remove the record.
            if any(current.get(key) != value for key, value in snapshot.items()):
                raise _SnapshotRestoreAbort({
                    "ok": False, "removed": False,
                    "error": "The restored channel changed; rollback was refused.",
                })
            channels.pop(index)
    except _SnapshotRestoreAbort as exc:
        return exc.result
    except (OSError, TypeError, ValueError) as exc:
        return {"ok": False, "removed": False, "error": str(exc)}
    return {"ok": True, "removed": True}


class _FolderTransactionAbort(RuntimeError):
    """Stop a config transaction and return a prepared user-facing result."""

    def __init__(self, result: dict[str, Any]) -> None:
        message = result.get("error") or result.get("delete_error") or ""
        super().__init__(str(message))
        self.result = result


def _same_path(first: str, second: str) -> bool:
    return os.path.normcase(os.path.abspath(first)) == os.path.normcase(
        os.path.abspath(second)
    )


def _same_filesystem(source: str, destination: str) -> bool:
    try:
        if os.name == "nt":
            source_drive = os.path.splitdrive(os.path.abspath(source))[0]
            destination_drive = os.path.splitdrive(os.path.abspath(destination))[0]
            return source_drive.casefold() == destination_drive.casefold()
        return os.stat(source).st_dev == os.stat(os.path.dirname(destination)).st_dev
    except OSError:
        return False


def _mutation_lease(
    channel: dict[str, Any],
    *,
    updated_channel: dict[str, Any] | None = None,
    paths: tuple[str, ...] = (),
    needs_journal: bool = False,
    label: str,
):
    alias_sets = []
    current_aliases = channel_aliases(channel, paths=paths)
    if current_aliases:
        alias_sets.append(current_aliases)
    if updated_channel is not None:
        updated_aliases = channel_aliases(updated_channel)
        if updated_aliases:
            alias_sets.append(updated_aliases)
    if needs_journal:
        alias_sets.append(channel_transaction_aliases())
    if not alias_sets:
        alias_sets.append(global_archive_aliases())
    owner = LeaseOwner(
        owner="subscriptions",
        job_id=uuid.uuid4().hex,
        label=label,
        kind="channel-config",
    )
    return channel_leases.try_acquire_many(alias_sets, owner)


def _require_empty_channel_journal() -> None:
    try:
        pending = load_channel_transaction(strict=True)
    except ChannelTransactionJournalError as exc:
        raise SubsError(str(exc)) from exc
    if pending is not None:
        raise SubsError(
            "An earlier channel-folder change still needs recovery. "
            "Restart YTArchiver before changing another channel folder."
        )


def _rollback_folder_rename(
    record: dict[str, Any],
    old_path: str,
    new_path: str,
    cause: object,
) -> tuple[bool, str]:
    try:
        old_exists = os.path.exists(old_path)
        new_exists = os.path.exists(new_path)
        if new_exists and not old_exists:
            os.rename(new_path, old_path)
        elif not (old_exists and not new_exists):
            raise OSError(
                "Cannot determine which folder is authoritative during rollback."
            )
    except OSError as exc:
        detail = f"{cause}; folder rollback failed: {exc}"
        mark_channel_recovery_required(record, phase="rename_rollback", error=detail)
        return False, detail
    if not clear_channel_transaction():
        detail = "Folder rollback completed, but its recovery journal remains."
        mark_channel_recovery_required(
            record,
            phase="journal_clear_after_rollback",
            error=detail,
        )
        return False, detail
    return True, ""


def update_channel(
        identity: dict[str, str], payload: dict[str, Any], *,
        pending_path_reconciler=None) -> dict[str, Any]:
    """Update an existing channel matched by identity (url or name/folder).

    If the folder name changed, rename the on-disk folder too so the user's
    archive stays in sync with the config (safer than leaving orphaned dirs).
    """
    cfg = load_config()
    channels = cfg.setdefault("channels", [])
    idx = _find_channel(channels, identity)
    if idx is None:
        raise SubsError(f"Channel not found: {identity}")
    existing = dict(channels[idx])
    # Partial-update safety: if the caller passed a sparse payload (e.g.
    # only {"name": "X"} to rename), we must not let `_payload_to_channel`
    # rebuild the whole dict from DEFAULTS — that would silently wipe the
    # URL / auto_transcribe / mode / etc. Detect sparse payloads and
    # merge on top of the existing channel so unmentioned fields survive.
    # URL hygiene — add_channel validates, edits previously didn't:
    # a present-but-blank url flipped the payload onto the full rebuild
    # path AND persisted url='' (a channel that can never sync again);
    # a typo'd watch/playlist URL was persisted unchecked.
    if "url" in payload:
        _u = (payload.get("url") or "").strip()
        if not _u:
            payload = dict(payload)
            payload.pop("url", None)  # keep the existing URL
        else:
            _ok_u, _why_u = validate_channel_url(_u)
            if not _ok_u:
                raise SubsError(f"Invalid channel URL: {_why_u}")
    sparse_payload = not payload.get("url") and "url" not in payload
    if sparse_payload:
        # Merge: start from existing, overlay payload keys directly
        merged = dict(existing)
        # Handle known UI-shape fields — these need conversion
        for k, v in payload.items():
            if k == "min_duration":
                # Accept either seconds or minutes; heuristic: if less than
                # 1000, assume minutes and convert.
                if isinstance(v, bool) or not re.fullmatch(
                        r"-?\d+", str(v).strip()):
                    raise SubsError(
                        "Minimum length must be a whole number.")
                n = int(v)
                merged[k] = n * 60 if n < 1000 else n
            elif k == "max_duration":
                if isinstance(v, bool) or not re.fullmatch(
                        r"-?\d+", str(v).strip()):
                    raise SubsError(
                        "Maximum length must be a whole number.")
                n = int(v)
                merged[k] = n * 60 if n < 1000 else n
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
            # Blank duration fields are omitted by the editor. Preserve the
            # saved limits instead of letting the full payload mapper turn an
            # omitted value into zero.
            "min_duration", "max_duration",
            "last_sync", "n_vids", "size_gb", "size_bytes",
            "initialized", "init_complete", "init_batch_after",
            "batch_resume_index",
            "transcription_complete", "transcription_pending",
            "metadata_pending",
            "folder_override",
            # Legacy builds stored this value. The current Processing queue
            # is per-file, but an ordinary edit must not erase the field.
            "compress_batch_size",
        )
        for key in _preserve:
            if key in existing and key not in payload:
                updated[key] = existing[key]

    if not str(updated.get("name") or "").strip():
        raise SubsError(
            "Channel name cannot be blank — syncs would route to the shared "
            "`_unnamed/` graveyard folder. Provide a name."
        )
    _validate_channel_constraints(updated)

    from . import sync as sync_backend

    old_name = str(existing.get("name") or existing.get("folder") or "").strip()
    new_name = str(updated.get("name") or updated.get("folder") or "").strip()
    old_folder = sync_backend.channel_folder_name(existing)
    new_folder = sync_backend.channel_folder_name(updated)
    folder_changed = bool(old_folder and new_folder and old_folder != new_folder)
    base = str(cfg.get("output_dir") or "").strip()
    old_path = os.path.join(base, old_folder) if folder_changed and base else ""
    new_path = os.path.join(base, new_folder) if folder_changed and base else ""
    physical_change = bool(old_path and new_path and not _same_path(old_path, new_path))

    lease_result = _mutation_lease(
        existing,
        updated_channel=updated,
        paths=tuple(path for path in (old_path, new_path) if path),
        needs_journal=physical_change,
        label=f"Edit channel {old_name or new_name}",
    )
    if not lease_result.ok or lease_result.lease is None:
        raise SubsError(lease_result.explanation)

    patch = build_channel_config_patch(existing, updated)
    journal: dict[str, Any] | None = None
    journal_written = False
    renamed: dict[str, str] | None = None
    queue_remapped = False
    queue_reconcile_result: dict[str, Any] = {"ok": True, "changed": 0}
    committed = dict(updated)
    with lease_result.lease:
        try:
            with config_transaction() as live_cfg:
                live_channels = live_cfg.setdefault("channels", [])
                live_idx = _find_channel(live_channels, identity)
                if live_idx is None:
                    raise SubsError(
                        "Channel changed while the edit was open. Reload and retry."
                    )
                live_channel = dict(live_channels[live_idx])
                try:
                    live_channel = apply_channel_config_patch(live_channel, patch)
                except ChannelTransactionConflict as exc:
                    raise SubsError(str(exc)) from exc

                wanted_name = str(
                    live_channel.get("name") or live_channel.get("folder") or ""
                ).strip().casefold()
                wanted_folder = sync_backend.channel_folder_name(
                    live_channel).casefold()
                for position, other in enumerate(live_channels):
                    if position == live_idx:
                        continue
                    if not isinstance(other, dict):
                        continue
                    other_name = str(
                        other.get("name") or other.get("folder") or ""
                    ).strip().casefold()
                    if wanted_name and other_name == wanted_name:
                        raise SubsError(
                            f"A channel named {live_channel.get('name')!r} "
                            "already exists."
                        )
                    if (wanted_folder
                            and sync_backend.channel_folder_name(
                                other).casefold() == wanted_folder):
                        raise SubsError(
                            "Another channel already uses that archive folder."
                        )

                live_base = str(live_cfg.get("output_dir") or "").strip()
                if physical_change and not _same_path(live_base, base):
                    raise SubsError(
                        "The archive location changed while this edit was open. "
                        "Reload and retry."
                    )
                if physical_change:
                    old_exists = os.path.isdir(old_path)
                    new_exists = os.path.exists(new_path)
                    if old_exists and new_exists:
                        raise SubsError(
                            f"Cannot rename {old_name!r}: destination folder "
                            f"{new_path!r} already exists."
                        )
                    if not old_exists and new_exists:
                        raise SubsError(
                            "The old channel folder is missing and the destination "
                            "already exists. Nothing was changed."
                        )
                    if old_exists:
                        if not _same_filesystem(old_path, new_path):
                            raise SubsError(
                                f"Cannot rename folder from {old_name!r} to "
                                f"{new_name!r}: source and destination are on "
                                "different volumes."
                            )
                        _require_empty_channel_journal()
                        journal = make_rename_transaction(
                            identity=dict(identity),
                            old_channel=existing,
                            new_channel=updated,
                            old_path=old_path,
                            new_path=new_path,
                        )
                        if not write_channel_transaction(journal):
                            raise SubsError(
                                "Could not save folder-rename recovery state. "
                                "Nothing was changed."
                            )
                        journal_written = True
                        if callable(pending_path_reconciler):
                            queue_reconcile_result = pending_path_reconciler(
                                old_path, new_path, old_name, new_name)
                            if not queue_reconcile_result.get("ok"):
                                raise SubsError(
                                    queue_reconcile_result.get("error")
                                    or "Queued Processing tasks could not be updated."
                                )
                            queue_remapped = bool(
                                queue_reconcile_result.get("changed"))
                        try:
                            os.rename(old_path, new_path)
                            renamed = {"from": old_path, "to": new_path}
                        except OSError as exc:
                            if os.path.isdir(new_path) and not os.path.exists(old_path):
                                renamed = {"from": old_path, "to": new_path}
                            raise SubsError(
                                f"Could not rename folder from {old_name!r} to "
                                f"{new_name!r}: {exc}. No settings saved."
                            ) from exc
                        if not checkpoint_channel_transaction(journal, "folder_moved"):
                            raise SubsError(
                                "Folder moved, but its durable recovery checkpoint "
                                "could not be saved."
                            )

                live_channels[live_idx] = live_channel
                live_channels.sort(
                    key=lambda channel: str(channel.get("name") or "").lower()
                )
                committed = dict(live_channel)
        except (OSError, SubsError) as exc:
            recovery_error = ""
            rollback_processing_queue = True
            if renamed is not None and journal is not None:
                rolled_back, recovery_error = _rollback_folder_rename(
                    journal,
                    renamed["from"],
                    renamed["to"],
                    exc,
                )
                if rolled_back:
                    renamed = None
                else:
                    # The folder is still at (or ambiguously near) the new
                    # path and the durable recovery journal owns the decision.
                    # Keep queued paths aligned with that new-side recovery
                    # state instead of blindly pointing them back at a missing
                    # old folder.
                    rollback_processing_queue = False
            elif journal_written and journal is not None:
                if not clear_channel_transaction():
                    recovery_error = (
                        "No folder change was committed, but its recovery journal "
                        "could not be cleared."
                    )
                    mark_channel_recovery_required(
                        journal,
                        phase="journal_clear_after_failure",
                        error=recovery_error,
                    )
            if (queue_remapped and rollback_processing_queue
                    and callable(pending_path_reconciler)):
                queue_rollback = pending_path_reconciler(
                    new_path, old_path, new_name, old_name)
                if not queue_rollback.get("ok"):
                    queue_error = str(
                        queue_rollback.get("error")
                        or "Processing queue rollback failed."
                    )
                    recovery_error = "; ".join(
                        part for part in (recovery_error, queue_error) if part)
            if recovery_error:
                return {
                    **updated,
                    "_write_blocked": True,
                    "_recovery_required": True,
                    "_rollback_error": recovery_error,
                }
            if isinstance(exc, SubsError):
                raise
            return {**updated, "_write_blocked": True, "_error": str(exc)}

        if renamed is not None and journal is not None:
            committed["_folder_renamed"] = renamed
            committed["_processing_queue_result"] = queue_reconcile_result
            if not clear_channel_transaction():
                detail = (
                    "Channel and folder were updated, but the recovery journal "
                    "could not be cleared."
                )
                mark_channel_recovery_required(
                    journal,
                    phase="journal_clear_after_commit",
                    error=detail,
                )
                committed["_recovery_required"] = True
                committed["_recovery_error"] = detail
    return committed


def remove_channel(
    identity: dict[str, str],
    delete_files: bool = False,
) -> dict[str, Any]:
    """Remove one subscription, quarantining its folder as one transaction.

    When folder quarantine fails, the subscription remains in config so the
    user can retry. A config-save failure restores the quarantined folder.
    """
    cfg = load_config()
    channels = cfg.setdefault("channels", [])
    idx = _find_channel(channels, identity)
    if idx is None:
        raise SubsError(f"Channel not found: {identity}")
    channel = dict(channels[idx])

    from .sync import channel_folder_name

    base = str(cfg.get("output_dir") or "").strip()
    folder_name = channel_folder_name(channel)
    folder_path = os.path.join(base, folder_name) if base and folder_name else ""
    lease_result = _mutation_lease(
        channel,
        paths=(folder_path,) if folder_path else (),
        needs_journal=bool(delete_files and folder_path),
        label=f"Remove channel {channel.get('name') or folder_name}",
    )
    if not lease_result.ok or lease_result.lease is None:
        return {
            "ok": False,
            "busy": lease_result.status in {"busy", "timeout"},
            "deleted_folder": False,
            "error": lease_result.explanation,
        }

    result: dict[str, Any] = {"ok": False, "deleted_folder": False}
    journal: dict[str, Any] | None = None
    journal_written = False
    quarantined_path = ""
    saved = False
    with lease_result.lease:
        try:
            with config_transaction() as live_cfg:
                live_channels = live_cfg.setdefault("channels", [])
                live_idx = _find_channel(live_channels, identity)
                if live_idx is None:
                    raise SubsError(f"Channel not found: {identity}")
                live_channel = dict(live_channels[live_idx])

                if delete_files and folder_path:
                    live_base = str(live_cfg.get("output_dir") or "").strip()
                    live_folder = channel_folder_name(live_channel)
                    live_path = (
                        os.path.join(live_base, live_folder)
                        if live_base and live_folder
                        else ""
                    )
                    if not live_path or not _same_path(live_path, folder_path):
                        raise SubsError(
                            "The channel folder changed while removal was open. "
                            "Reload and retry."
                        )
                    if os.path.isdir(folder_path):
                        from backend.services.file_ops import (
                            restore_trash_entry,
                            safe_rmtree_channel_folder,
                        )

                        _require_empty_channel_journal()
                        journal = make_remove_transaction(
                            identity=dict(identity),
                            old_channel=live_channel,
                            old_path=folder_path,
                            archive_root=live_base,
                        )
                        if not write_channel_transaction(journal):
                            raise _FolderTransactionAbort(
                                {
                                    **result,
                                    "delete_error": (
                                        "Could not save folder-removal recovery "
                                        "state. Nothing was changed."
                                    ),
                                }
                            )
                        journal_written = True
                        deleted = safe_rmtree_channel_folder(
                            folder_path,
                            require_config_writable=True,
                            reason="subs_remove_channel",
                            reserved_trash_path=str(
                                journal["trashed_folder_path"]),
                            transaction_id=str(journal["tx_id"]),
                            channel_snapshot=live_channel,
                        )
                        result.update(
                            {
                                key: value
                                for key, value in deleted.items()
                                if key not in {"ok", "reason", "error"}
                            }
                        )
                        if not deleted.get("ok"):
                            detail = str(
                                deleted.get("error") or "Folder quarantine failed."
                            )
                            rollback_error = str(deleted.get("rollback_error") or "")
                            if rollback_error:
                                mark_channel_recovery_required(
                                    journal,
                                    phase="quarantine_rollback",
                                    error=f"{detail}; {rollback_error}",
                                )
                                result.update(
                                    {
                                        "_recovery_required": True,
                                        "_rollback_error": rollback_error,
                                    }
                                )
                            elif not clear_channel_transaction():
                                mark_channel_recovery_required(
                                    journal,
                                    phase="journal_clear_after_failure",
                                    error=detail,
                                )
                                result["_recovery_required"] = True
                            result["delete_error"] = detail
                            raise _FolderTransactionAbort(result)

                        quarantined_path = str(
                            deleted.get("trashed_folder_path") or ""
                        )
                        if deleted.get("deleted_folder") and not quarantined_path:
                            detail = "Folder moved, but its trash location is unknown."
                            mark_channel_recovery_required(
                                journal,
                                phase="folder_moved",
                                error=detail,
                            )
                            result.update(
                                {
                                    "delete_error": detail,
                                    "_recovery_required": True,
                                }
                            )
                            raise _FolderTransactionAbort(result)
                        if quarantined_path and not checkpoint_channel_transaction(
                            journal,
                            "folder_moved",
                        ):
                            restored = restore_trash_entry(
                                quarantined_path,
                                expected_transaction_id=str(journal["tx_id"]),
                            )
                            if restored.get("ok"):
                                result["deleted_folder"] = False
                                if not clear_channel_transaction():
                                    mark_channel_recovery_required(
                                        journal,
                                        phase="journal_clear_after_rollback",
                                        error="Removal checkpoint was rolled back.",
                                    )
                                    result["_recovery_required"] = True
                                result["delete_error"] = (
                                    "Folder-removal checkpoint failed; the folder "
                                    "was restored."
                                )
                            else:
                                rollback_error = str(
                                    restored.get("error") or "Trash restore failed."
                                )
                                mark_channel_recovery_required(
                                    journal,
                                    phase="remove_rollback",
                                    error=rollback_error,
                                )
                                result.update(
                                    {
                                        "delete_error": (
                                            "Folder-removal checkpoint and rollback "
                                            "both failed."
                                        ),
                                        "_recovery_required": True,
                                        "_rollback_error": rollback_error,
                                    }
                                )
                            raise _FolderTransactionAbort(result)

                live_channels.pop(live_idx)
            saved = True
        except _FolderTransactionAbort as exc:
            return exc.result
        except (OSError, SubsError) as exc:
            result["error"] = str(exc) or "Config save failed"
            if quarantined_path and journal is not None:
                from backend.services.file_ops import restore_trash_entry

                restored = restore_trash_entry(
                    quarantined_path,
                    expected_transaction_id=str(journal["tx_id"]),
                )
                if restored.get("ok"):
                    result["deleted_folder"] = False
                    if not clear_channel_transaction():
                        detail = (
                            "Folder was restored, but its recovery journal remains."
                        )
                        mark_channel_recovery_required(
                            journal,
                            phase="journal_clear_after_rollback",
                            error=detail,
                        )
                        result.update(
                            {
                                "_recovery_required": True,
                                "_rollback_error": detail,
                            }
                        )
                else:
                    rollback_error = str(
                        restored.get("error") or "Trash restore failed."
                    )
                    mark_channel_recovery_required(
                        journal,
                        phase="remove_rollback",
                        error=f"{exc}; {rollback_error}",
                    )
                    result.update(
                        {
                            "_recovery_required": True,
                            "_rollback_error": rollback_error,
                        }
                    )
            elif journal_written and journal is not None:
                if not clear_channel_transaction():
                    mark_channel_recovery_required(
                        journal,
                        phase="journal_clear_after_failure",
                        error=exc,
                    )
                    result["_recovery_required"] = True
            return result

        if saved and journal_written and journal is not None:
            if not clear_channel_transaction():
                detail = (
                    "Subscription and folder removal completed, but the recovery "
                    "journal could not be cleared."
                )
                mark_channel_recovery_required(
                    journal,
                    phase="journal_clear_after_commit",
                    error=detail,
                )
                result["_recovery_required"] = True
                result["recovery_error"] = detail

        channel_url = str(channel.get("url") or "").strip()
        if channel_url:
            try:
                from . import archive_scan

                archive_scan.invalidate_channel(channel_url)
            except Exception as exc:
                _log.debug("archive scan cache invalidation failed: %s", exc)
            try:
                from . import channel_cache

                channel_cache.clear(channel_url)
            except Exception as exc:
                _log.debug("channel id cache invalidation failed: %s", exc)
        result["ok"] = True
    return result


def get_channel(identity: dict[str, str]) -> dict[str, Any] | None:
    """Return a channel dict by url/name/folder. The raw on-disk schema
    stores min/max_duration as seconds; this is the unchanged disk record.
    Use `get_channel_for_ui()` to receive the UI-formatted dict (minutes).
    """
    cfg = load_config()
    idx = _find_channel(cfg.get("channels", []), identity)
    if idx is None:
        return None
    return dict(cfg["channels"][idx])


def get_channel_for_ui(identity: dict[str, str]) -> dict[str, Any] | None:
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
