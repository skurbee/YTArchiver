"""Admit YouTube request starts through the parent application's shared budget.

yt-dlp constructs configured postprocessors before extraction. Installing the
instance wrapper here therefore covers playlist, player, metadata and caption
requests as well as retries that re-enter YoutubeDL.urlopen. Media byte transfers
are excluded; redirects and retries internal to a transport are not separate
urlopen calls. Broker messages never include cookies or request URLs.
"""

import json
import os
import re
import socket
from datetime import UTC, datetime
from urllib.parse import urlsplit

from yt_dlp.postprocessor.common import PostProcessor

_MAX_MESSAGE = 4096
_YOUTUBE_DOMAINS = (
    "youtube.com", "youtu.be", "youtube-nocookie.com",
    "ytimg.com", "ggpht.com", "googleusercontent.com",
)
_YOUTUBE_HOSTS = frozenset({
    "youtubei.googleapis.com", "youtube.googleapis.com", "accounts.google.com",
})


class _TrafficSafetyError(RuntimeError):
    """A request must not be sent without the parent governor's permission."""


def _is_domain(host, domain):
    return host == domain or host.endswith("." + domain)


def _request_url(request):
    if isinstance(request, str):
        return request
    try:
        urls = [getattr(request, name, None) for name in ("full_url", "url")]
    except Exception:
        raise _TrafficSafetyError("Unsupported network request; traffic safety cannot inspect it.") from None
    urls = [value for value in urls if value is not None]
    if not urls or any(not isinstance(value, str) for value in urls) or len(set(urls)) != 1:
        raise _TrafficSafetyError("Unsupported network request; traffic safety cannot inspect it.")
    return urls[0]


def _request_kind(request):
    """Return a safe category without retaining a signed URL or its parameters."""
    url = _request_url(request)
    try:
        if not url or any(ord(char) < 32 or ord(char) == 127 for char in url):
            raise ValueError
        parts = urlsplit(url)
        host = (parts.hostname or "").lower().rstrip(".")
        if (parts.scheme.lower() not in {"http", "https"} or not host
                or any(char in parts.netloc for char in "\\%")
                or any(char.isspace() for char in parts.netloc)):
            raise ValueError
        # Accessing port validates malformed or out-of-range port strings.
        _ = parts.port
        host = host.encode("idna").decode("ascii").lower().rstrip(".")
    except (ValueError, UnicodeError):
        raise _TrafficSafetyError("Unsupported network URL; traffic safety cannot inspect it.") from None

    if _is_domain(host, "googlevideo.com"):
        if parts.path == "/videoplayback" or parts.path.startswith("/videoplayback/"):
            return "youtube_media"
        return "youtube_media_manifest"
    if host not in _YOUTUBE_HOSTS and not any(_is_domain(host, domain) for domain in _YOUTUBE_DOMAINS):
        return None
    if parts.path.rstrip("/").endswith("/timedtext"):
        return "youtube_caption"
    if "/manifest/" in parts.path or parts.path.endswith((".m3u8", ".mpd")):
        return "youtube_media_manifest"
    return "youtube_http"


def _http_status(value):
    for candidate in (value, getattr(value, "response", None)):
        if candidate is not None:
            # A valid status is authoritative even when it is not 429. Probing
            # yt-dlp's deprecated Response.code alias prints a false CLI error.
            for field in ("status", "status_code", "code"):
                status = getattr(candidate, field, None)
                if isinstance(status, int) and 100 <= status <= 599:
                    return status
    return None


def _caption_file_has_text(path):
    """Reject empty caption responses before trying an alternate track."""
    import html

    try:
        with open(path, encoding="utf-8-sig", errors="replace") as caption:
            in_cue = False
            for line in caption:
                if re.match(r"\s*\d[\d:.]+\s*-->\s*\d[\d:.]+", line):
                    in_cue = True
                    continue
                if not line.strip():
                    in_cue = False
                elif in_cue and any(char.isalnum() for char in
                                     html.unescape(re.sub(r"<[^>]*>", "", line))):
                    return True
    except OSError:
        pass
    return False


class YTArchiverTrafficGuardPP(PostProcessor):
    """Guard requests, select captions, and attest completed metadata extraction."""

    def __init__(self, downloader=None):
        port = os.environ.get("YTARCHIVER_TRAFFIC_PORT", "")
        token = os.environ.get("YTARCHIVER_TRAFFIC_TOKEN", "")
        if (not re.fullmatch(r"[0-9]{1,5}", port) or not 1 <= int(port) <= 65535
                or not re.fullmatch(r"[A-Za-z0-9_-]{32,256}", token)):
            raise _TrafficSafetyError("YouTube traffic safety configuration is missing or invalid.")
        if downloader is None or not callable(getattr(downloader, "urlopen", None)):
            raise _TrafficSafetyError("YouTube traffic safety could not install its request guard.")
        super().__init__(downloader)
        self._port = int(port)
        self._token = token
        self._failed = False
        if getattr(downloader, "_ytarchiver_traffic_guard_installed", False):
            return
        original = downloader.urlopen

        def guarded_urlopen(request):
            self._check_active()
            try:
                kind = _request_kind(request)
            except _TrafficSafetyError:
                self._failed = True
                raise
            if kind and kind != "youtube_media":
                self._rpc("acquire", kind)
            self._check_active()
            try:
                response = original(request)
            except Exception as exc:
                if kind and _http_status(exc) == 429:
                    try:
                        self._rpc("rate_limit", kind)
                    except _TrafficSafetyError:
                        # Preserve yt-dlp's original error. The failed RPC has
                        # latched this wrapper closed, including future retries.
                        pass
                    finally:
                        # Media-byte requests do not acquire budget. Latch
                        # this worker closed as well as recording the shared
                        # circuit so their retries cannot continue after 429.
                        self._failed = True
                raise
            if kind and _http_status(response) == 429:
                try:
                    self._rpc("rate_limit", kind)
                finally:
                    self._failed = True
                    close = getattr(response, "close", None)
                    if callable(close):
                        close()
                raise _TrafficSafetyError("YouTube returned HTTP 429; further requests are deferred.")
            return response

        downloader.urlopen = guarded_urlopen
        downloader._ytarchiver_traffic_guard_installed = True
        self._install_caption_selection(downloader)

    def _install_caption_selection(self, downloader):
        original_select = getattr(downloader, "process_subtitles", None)
        original_write = getattr(downloader, "_write_subtitles", None)
        if not callable(original_select) or not callable(original_write):
            return
        from yt_dlp.utils import DownloadError

        pending = {}

        def select_one(video_id, candidate):
            language, formats, manual = candidate
            previous = downloader.params.get("subtitleslangs")
            downloader.params["subtitleslangs"] = [language]
            try:
                return original_select(video_id, {language: formats} if manual else {},
                                       {} if manual else {language: formats})
            finally:
                downloader.params["subtitleslangs"] = previous

        def select_subtitles(video_id, normal, automatic):
            requested = downloader.params.get("subtitleslangs") or []
            # Other callers can request arbitrary languages. Restrict this
            # optimization to the application's explicit English choices.
            if (not requested or downloader.params.get("allsubtitles")
                    or any(not re.fullmatch(r"en(?:-[A-Za-z]+)?", lang) for lang in requested)):
                return original_select(video_id, normal, automatic)
            candidates = []
            for manual, tracks, enabled in (
                (True, normal or {}, downloader.params.get("writesubtitles")),
                (False, automatic or {}, downloader.params.get("writeautomaticsub")),
            ):
                order = ("en", "en-US", "en-GB", "en-orig") if manual else (
                    "en-orig", "en", "en-US", "en-GB")
                languages = sorted((lang for lang in tracks if lang in requested),
                                   key=lambda lang: (order.index(lang) if lang in order else len(order), lang))
                if enabled:
                    candidates.extend((lang, tracks[lang], manual) for lang in languages if tracks[lang])
            if not candidates:
                return original_select(video_id, normal, automatic)
            # yt-dlp handles one video at a time; filtered videos must not
            # leave a growing cache while walking a large channel.
            pending.clear()
            pending[video_id] = candidates
            return select_one(video_id, candidates[0])

        def write_subtitles(information, filename):
            candidates = pending.pop(information.get("id"), None)
            if not candidates:
                return original_write(information, filename)
            last_error = None
            for index, candidate in enumerate(candidates):
                self._check_active()
                if index:
                    information["requested_subtitles"] = select_one(information["id"], candidate)
                try:
                    written = original_write(information, filename)
                except DownloadError as error:
                    self._check_active()
                    last_error = error
                    continue
                self._check_active()
                if written is None:
                    return None  # yt-dlp's local-write failure abort sentinel
                if written and any(_caption_file_has_text(path) for path, _final in written):
                    return written
            information["requested_subtitles"] = {}
            if last_error is not None:
                raise last_error
            return []

        downloader.process_subtitles = select_subtitles
        downloader._write_subtitles = write_subtitles

    def _check_active(self):
        if self._failed:
            raise _TrafficSafetyError("YouTube traffic safety stopped this worker after a failed permission check.")

    def _rpc(self, operation, kind):
        self._check_active()
        payload = json.dumps({"token": self._token, "op": operation, "kind": kind}).encode("ascii") + b"\n"
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
                connection.settimeout(1.0)
                connection.connect(("127.0.0.1", self._port))
                connection.sendall(payload)
                data = bytearray()
                while b"\n" not in data:
                    try:
                        chunk = connection.recv(_MAX_MESSAGE + 1 - len(data))
                    except TimeoutError:
                        # A shared budget can wait longer than a single read.
                        # Parent exit/disconnection closes the socket instead.
                        continue
                    if not chunk:
                        raise _TrafficSafetyError("YouTube traffic safety connection closed before permission arrived.")
                    data.extend(chunk)
                    if len(data) > _MAX_MESSAGE:
                        raise _TrafficSafetyError("YouTube traffic safety returned an invalid response.")
                if data[-1:] != b"\n" or data.count(b"\n") != 1:
                    raise _TrafficSafetyError("YouTube traffic safety returned an invalid response.")
                result = json.loads(data[:-1])
                if not isinstance(result, dict) or type(result.get("ok")) is not bool:
                    raise _TrafficSafetyError("YouTube traffic safety returned an invalid response.")
                if result["ok"] is not True:
                    raise _TrafficSafetyError("YouTube request was deferred by shared traffic safety.")
        except _TrafficSafetyError:
            self._failed = True
            raise
        except (OSError, ValueError, UnicodeError, RecursionError):
            self._failed = True
            # Transport/decoder errors may contain input or environment data;
            # only this fixed explanation may reach yt-dlp's output.
            raise _TrafficSafetyError("YouTube traffic safety permission could not be verified.") from None

    def run(self, information):
        post_extractor = information.get("__post_extractor")
        video_id = information.get("id")
        if (getattr(self._downloader, "params", {}).get("getcomments")
                and information.get("_type", "video") == "video"
                and isinstance(video_id, str) and video_id
                and callable(post_extractor)
                and getattr(post_extractor, "_ytarchiver_snapshot_wrapped", False) is not True):
            information.pop("ytarchiver_metadata_snapshot", None)

            def finish_metadata():
                result = post_extractor()
                if isinstance(result, dict):
                    comments, count = result.get("comments"), result.get("comment_count")
                    disabled = ("comments" in result and "comment_count" in result
                                and comments is None and count is None)
                    complete = (isinstance(comments, list) and type(count) is int
                                and count == len(comments))
                    if disabled or complete:
                        result = {**result, "ytarchiver_metadata_snapshot": {
                            "version": 1, "video_id": video_id, "comments_complete": True,
                            "comments_disabled": disabled,
                            "fetched_at": datetime.now(UTC).isoformat(),
                        }}
                return result

            finish_metadata._ytarchiver_snapshot_wrapped = True
            information["__post_extractor"] = finish_metadata
        return [], information
