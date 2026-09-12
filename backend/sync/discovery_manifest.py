"""Disposable, complete channel listings for resuming an unfinished download.

The archive remains the authority for completed videos. A manifest records no
download cursor and no credentials, expiring media URLs, or archive paths. Its
caller must establish that discovery finished successfully before saving it.
Only validated synthetic playlists may be handed to yt-dlp's load-info-json.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit

from .. import ytarchiver_config
from ..log import get_logger
from ..services.sidecar_store import atomic_write_bytes, sidecar_lock

_log = get_logger(__name__)
SCHEMA = 1
MAX_BYTES = 64 * 1024 * 1024
MAX_ENTRIES = 200_000
MAX_DEPTH = 6
_CHANNEL_ID = re.compile(r"UC[A-Za-z0-9_-]{22}\Z")
_VIDEO_ID = re.compile(r"[A-Za-z0-9_-]{11}\Z")
_KEY = re.compile(r"[a-f0-9]{64}\Z")
_LIVE_STATUSES = {"not_live", "is_live", "is_upcoming", "was_live", "post_live"}


def context_key(channel_id: str, target_url: str, output_dir: str) -> str:
    """Hash permanent identity, canonical channel target, and destination.

    Invalid or ambiguous context raises ValueError. Relative destinations are
    rejected so a working-directory change cannot silently rebind a manifest.
    """
    if not isinstance(channel_id, str) or not _CHANNEL_ID.fullmatch(channel_id):
        raise ValueError("discovery requires a permanent channel ID")
    if not isinstance(target_url, str):
        raise ValueError("invalid discovery target")
    url = urlsplit(target_url.strip())
    if (url.scheme.lower() not in {"http", "https"}
            or url.hostname not in {"youtube.com", "www.youtube.com", "m.youtube.com"}
            or url.username or url.password or url.port):
        raise ValueError("discovery target is not a public YouTube channel URL")
    parts = url.path.rstrip("/").split("/")[1:]
    if not parts:
        raise ValueError("missing discovery channel")
    if parts[0] == "channel":
        if len(parts) < 2 or parts[1] != channel_id:
            raise ValueError("discovery target identifies a different channel")
        suffix = parts[2:]
    elif parts[0] in {"c", "user"} and len(parts) >= 2 and parts[1]:
        suffix = parts[2:]
    elif parts[0].startswith("@") and len(parts[0]) > 1:
        suffix = parts[1:]
    else:
        raise ValueError("discovery target is not a channel")
    if suffix not in ([], ["videos"], ["streams"], ["shorts"]):
        raise ValueError("unsupported discovery channel tab")
    query = urlencode(sorted(parse_qsl(url.query, keep_blank_values=True)))
    canonical = "https://www.youtube.com/" + "/".join(parts)
    if query:
        canonical += "?" + query
    if not isinstance(output_dir, str) or not output_dir or not os.path.isabs(output_dir):
        raise ValueError("discovery destination must be absolute")
    destination = os.path.normcase(os.path.normpath(os.path.abspath(output_dir)))
    context = json.dumps([channel_id, canonical, destination], ensure_ascii=False)
    return hashlib.sha256(context.encode("utf-8")).hexdigest()


def manifest_path(key: str) -> Path:
    """Return the sole cache file owned by a validated context hash."""
    if not isinstance(key, str) or not _KEY.fullmatch(key):
        raise ValueError("invalid discovery cache key")
    return Path(ytarchiver_config.APP_DATA_DIR) / "sync_discovery" / f"{key}.json"


def _validate_identity(node: dict, expected: str, *, playlist: bool) -> None:
    supplied = node.get("channel_id")
    if supplied not in (None, "") and supplied != expected:
        raise ValueError("discovery channel identity mismatch")
    node_id = node.get("id")
    if playlist:
        if isinstance(node_id, str) and _CHANNEL_ID.fullmatch(node_id) and node_id != expected:
            raise ValueError("discovery playlist identity mismatch")
        if supplied != expected and node_id != expected:
            raise ValueError("discovery playlist does not prove channel identity")


def _normalize_playlist(raw: dict, channel_id: str, key: str) -> dict:
    entries = []
    seen = set()
    examined = 0

    def visit(node: object, depth: int) -> None:
        nonlocal examined
        if depth > MAX_DEPTH or not isinstance(node, dict):
            raise ValueError("malformed discovery playlist")
        if node.get("_type") == "playlist":
            _validate_identity(node, channel_id, playlist=True)
            children = node.get("entries")
            if not isinstance(children, list):
                raise ValueError("discovery playlist was not fully materialized")
            for child in children:
                visit(child, depth + 1)
            return
        examined += 1
        if examined > MAX_ENTRIES:
            raise ValueError("discovery listing is too large")
        _validate_identity(node, channel_id, playlist=False)
        video_id = node.get("id")
        if (node.get("_type") not in {"url", "url_transparent"}
                or node.get("ie_key") != "Youtube"
                or not isinstance(video_id, str) or not _VIDEO_ID.fullmatch(video_id)):
            raise ValueError("discovery entry is not a flat YouTube video")
        raw_url = node.get("url")
        if not isinstance(raw_url, str):
            raise ValueError("missing discovery video URL")
        url = urlsplit(raw_url)
        watch_match = (url.path == "/watch"
                       and [value for name, value in parse_qsl(url.query) if name == "v"] == [video_id])
        shorts_match = url.path == f"/shorts/{video_id}"
        if (url.scheme not in {"https", "http"}
                or url.hostname not in {"youtube.com", "www.youtube.com", "m.youtube.com"}
                or url.username or url.password or url.port
                or not (watch_match or shorts_match)):
            raise ValueError("discovery video URL does not match its ID")
        title = node.get("title")
        if title is not None and (not isinstance(title, str) or len(title) > 4096):
            raise ValueError("invalid discovery title")
        duration = node.get("duration")
        if duration is not None and (isinstance(duration, bool)
                or not isinstance(duration, (int, float))
                or (isinstance(duration, float) and not math.isfinite(duration)) or duration < 0):
            raise ValueError("invalid discovery duration")
        live_status = node.get("live_status")
        if live_status is not None and (not isinstance(live_status, str)
                or live_status not in _LIVE_STATUSES):
            raise ValueError("invalid discovery live status")
        dynamic = live_status in {"is_live", "is_upcoming", "post_live"}
        for flag in ("is_live", "is_upcoming"):
            if flag in node and node[flag] is not None:
                if not isinstance(node[flag], bool):
                    raise ValueError("invalid discovery live flag")
                dynamic = dynamic or node[flag]
        # A saved broadcast may have finished before replay. Let fresh video
        # extraction decide live status and its eventual duration; preserving
        # these transient values could filter it out indefinitely.
        entry = {
            "_type": "url", "ie_key": "Youtube", "id": video_id,
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "title": title, "duration": None if dynamic else duration,
        }
        if video_id not in seen:
            seen.add(video_id)
            entries.append(entry)

    if not isinstance(raw, dict) or raw.get("_type") != "playlist":
        raise ValueError("discovery root is not a playlist")
    visit(raw, 0)
    if not entries:
        raise ValueError("empty discovery listing cannot establish reusable progress")
    return {
        "_type": "playlist", "id": channel_id, "channel_id": channel_id,
        "extractor": "youtube:tab", "extractor_key": "YoutubeTab",
        "title": "Saved channel videos", "entries": entries,
        "_ytarchiver_discovery": {"schema": SCHEMA, "context_key": key},
    }


def _check_context(key: str, channel_id: str, target_url: str, output_dir: str) -> Path:
    path = manifest_path(key)
    if key != context_key(channel_id, target_url, output_dir):
        raise ValueError("discovery context mismatch")
    return path


def load_manifest(key: str, channel_id: str, target_url: str, output_dir: str) -> dict | None:
    """Read a validated, complete synthetic playlist, or fall back on a miss.

    Exact normalized equality also rejects injected root URLs, executable
    extractor choices, or unexpected metadata before load-info-json sees them.
    """
    try:
        path = _check_context(key, channel_id, target_url, output_dir)
        with path.open("rb") as handle:
            payload = handle.read(MAX_BYTES + 1)
        if len(payload) > MAX_BYTES:
            raise ValueError("discovery manifest is too large")
        raw = json.loads(payload)
        metadata = raw.get("_ytarchiver_discovery") if isinstance(raw, dict) else None
        if (not isinstance(metadata, dict) or type(metadata.get("schema")) is not int
                or metadata.get("schema") != SCHEMA):
            raise ValueError("unsupported discovery manifest schema")
        normalized = _normalize_playlist(raw, channel_id, key)
        if raw != normalized:
            raise ValueError("discovery manifest schema or contents changed")
        return normalized
    except FileNotFoundError:
        return None
    except (OSError, ValueError, TypeError, RecursionError) as exc:
        _log.debug("Saved channel listing unavailable: %s", exc)
        return None


def save_manifest(key: str, channel_id: str, target_url: str, output_dir: str,
                  raw_playlist: dict) -> dict | None:
    """Atomically save a proven-complete listing; failure only disables reuse."""
    try:
        if not ytarchiver_config.config_is_writable():
            return None
        path = _check_context(key, channel_id, target_url, output_dir)
        normalized = _normalize_playlist(raw_playlist, channel_id, key)
        payload = json.dumps(normalized, ensure_ascii=False, allow_nan=False,
                             separators=(",", ":")).encode("utf-8")
        if len(payload) > MAX_BYTES:
            raise ValueError("discovery manifest is too large")

        def writable(_stage: str) -> None:
            if not ytarchiver_config.config_is_writable():
                raise OSError("discovery cache writes suspended")

        atomic_write_bytes(path, payload, before_replace=writable)
        return normalized
    except (OSError, ValueError, TypeError, RecursionError) as exc:
        _log.debug("Could not save channel listing for reuse: %s", exc)
        return None


def invalidate(key: str) -> None:
    """Remove only this disposable local cache entry, never archive data."""
    try:
        path = manifest_path(key)
        with sidecar_lock(path):
            if ytarchiver_config.config_is_writable():
                path.unlink(missing_ok=True)
    except (OSError, ValueError, TypeError) as exc:
        _log.debug("Could not discard saved channel listing: %s", exc)
