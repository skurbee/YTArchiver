"""
Tiny local HTTP server for serving channel art + thumbnails to the
pywebview window.

Problem: pywebview loads index.html from a `file://` origin. Modern webviews
(WebView2 on Windows) block cross-origin `file://` image requests for
security — so `background-image: url("file:///Z:/.../thumb.jpg")` silently
fails to load even though the file exists.

Solution: spin up an `http.server` on 127.0.0.1 with a random free port at
app launch. Any path the UI needs (thumbnails, avatars, banners) is served
over `http://127.0.0.1:<port>/<abs-path>`. The webview treats HTTP as a
regular origin so the images load without fuss.

Security: bound to 127.0.0.1 only (never reachable from LAN), reads files
only — no writes, no directory listings, no path escaping. GET-only.
"""

from __future__ import annotations

import mimetypes
import os
import secrets
import socket
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .log import get_logger

_log = get_logger(__name__)


_server_port: int = 0
_server_thread: threading.Thread | None = None
_httpd = None  # module-level handle so stop_server() can
               # shut the server down cleanly (server_close releases
               # the port socket; shutdown stops the serve_forever loop).
_lock = threading.Lock()

# allowlist of archive roots the fileserver may serve.
# Populated at start_server() time from the live config's channel
# output_dirs + Z: archive root + thumbs cache. Requests outside
# these roots are rejected with 403. Before this, the path check
# was only "..-in-segments + isabs + exists", which let any
# absolute path on disk be read through http://127.0.0.1:PORT/file/.
_allowed_roots: list[str] = []
_request_token: str = ""


def _normalize_root(p: str) -> str:
    """Return an absolute, lowercased, no-trailing-sep form of p.
    Used to compare incoming request paths against the allowlist
    without case or trailing-slash false-mismatches on Windows.
    """
    try:
        return os.path.normcase(os.path.abspath(p)).rstrip("/\\")
    except Exception:
        return ""


def set_allowed_roots(roots: list[str]) -> None:
    """Register the set of directory roots the fileserver may serve.
    Called from main.py after config load. Roots are normalized to
    absolute case-insensitive form.
    """
    global _allowed_roots
    _allowed_roots = [r for r in (_normalize_root(x) for x in (roots or [])) if r]


def _is_under_allowed_root(path: str) -> bool:
    """True if `path` resolves to something under one of the registered
    allowed roots.

    empty allowlist now FAILS CLOSED (returns False).
    Previously returned True as a "backward-compatible fallback" — but
    there's a real startup race where the server binds before
    set_allowed_roots() runs, and any local process could read any file
    on disk through /file/... during that window. Fail-closed eliminates
    the window. main.py must call set_allowed_roots() before relying on
    the fileserver.
    """
    if not _allowed_roots:
        try:
            _log.warning("local_fileserver: request before allowlist set — "
                         "rejecting %r", path)
        except Exception:
            pass
        return False
    try:
        p = _normalize_root(path)
    except Exception:
        return False
    if not p:
        return False
    # Also resolve symlinks so a malicious symlink under an allowed
    # root can't tunnel to a path outside it (audit: local_fileserver
    # H103). Use realpath on the ORIGINAL (un-normcased) path then
    # re-normalize for the prefix check.
    try:
        _real = os.path.normcase(os.path.realpath(path)).rstrip("/\\")
    except Exception:
        _real = p
    for root in _allowed_roots:
        # os.path.normcase ensures case-insensitive prefix match on
        # Windows. The + os.sep guard prevents "/ArchiveBad" from
        # matching an allowed root "/Archive".
        root_prefixes = (root + os.sep, root + "/")
        if p == root or p.startswith(root_prefixes):
            # Belt + suspenders: also require the realpath under root.
            if _real == root or _real.startswith(root_prefixes):
                return True
            try:
                _log.warning(
                    "local_fileserver: symlink escape blocked: %r → %r",
                    path, _real)
            except Exception:
                pass
            return False
    return False


def _authorized_request(handler: BaseHTTPRequestHandler,
                        parsed: urllib.parse.SplitResult) -> bool:
    expected = _request_token
    if not expected:
        return False
    supplied = handler.headers.get("X-YTArchiver-Token", "")
    if not supplied:
        try:
            supplied = urllib.parse.parse_qs(
                parsed.query, keep_blank_values=True).get("t", [""])[0]
        except Exception:
            supplied = ""
    return secrets.compare_digest(str(supplied), expected)


class _FileRequestHandler(BaseHTTPRequestHandler):
    # Advertise HTTP/1.1 so `Accept-Ranges: bytes` + 206 Partial Content
    # responses are recognized by WebView2 / Chromium video elements.
    # Without this, the <video> tag couldn't seek — browser would request
    # a byte range, the server would ignore it, and the playhead would
    # snap back to currentTime. this was reported
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args): # noqa: A002 — stdlib signature
        # Silence access log chatter; pywebview traffic is a constant stream
        pass

    def _resolve_path(self):
        """Shared GET/HEAD path resolver. Returns the absolute path or None
        (None means an error response was already sent)."""
        if self.path in ("/", "/ping"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", "2")
            self.end_headers()
            try: self.wfile.write(b"ok")
            except (OSError, ConnectionError): pass
            return None
        parsed = urllib.parse.urlsplit(self.path)
        if not parsed.path.startswith("/file/"):
            self.send_error(404); return None
        if not _authorized_request(self, parsed):
            self.send_error(403); return None
        raw = parsed.path[len("/file/"):]
        try:
            path = urllib.parse.unquote(raw)
        except Exception:
            self.send_error(400); return None
        if not path or ".." in path.replace("\\", "/").split("/"):
            self.send_error(400); return None
        if not os.path.isabs(path):
            self.send_error(400); return None
        # reject requests outside the archive root
        # allowlist (set_allowed_roots). Previously a request for
        # /file/C:/Users/*/Documents/*.pdf passed all the other
        # checks. Allowlist empty = fallback allow (backward-compat
        # for dev/demo).
        if not _is_under_allowed_root(path):
            self.send_error(403); return None
        if not os.path.isfile(path):
            self.send_error(404); return None
        return path

    def _parse_range(self, size: int):
        """Parse the incoming `Range: bytes=START-END` header.

        Returns (start, end, is_range). If the header is absent, returns
        (0, size-1, False). If malformed or out of bounds, sends a 416
        response and returns None so the caller knows to bail.
        """
        rng = self.headers.get("Range", "")
        if not rng or not rng.startswith("bytes="):
            return 0, size - 1, False
        try:
            spec = rng[len("bytes="):].split(",")[0].strip()
            a, b = spec.split("-", 1)
            start = int(a) if a else 0
            end = int(b) if b else (size - 1)
            # Suffix-range "bytes=-500" = last 500 bytes
            if not a and b:
                start = max(0, size - int(b))
                end = size - 1
            if start < 0 or end >= size or start > end:
                raise ValueError
        except Exception:
            self.send_response(416)
            self.send_header("Content-Range", f"bytes */{size}")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return None
        return start, end, True

    def do_HEAD(self):
        path = self._resolve_path()
        if path is None:
            return
        try: size = os.path.getsize(path)
        except OSError: self.send_error(500); return
        ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(size))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Cache-Control", "public, max-age=86400")
        self.end_headers()

    def do_GET(self):
        path = self._resolve_path()
        if path is None:
            return # ping reply or error was already sent
        ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
        try: size = os.path.getsize(path)
        except OSError: self.send_error(500); return

        parsed = self._parse_range(size)
        if parsed is None:
            return # 416 already sent
        start, end, is_range = parsed
        content_length = end - start + 1

        if is_range:
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        else:
            self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(content_length))
        # Accept-Ranges: bytes tells the browser this resource is seekable.
        # Without it, <video> won't even try range requests and falls back
        # to sequential-only playback, which breaks scrubbing.
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Cache-Control", "public, max-age=86400")
        self.end_headers()
        try:
            with open(path, "rb") as f:
                f.seek(start)
                remaining = content_length
                while remaining > 0:
                    chunk = f.read(min(64 * 1024, remaining))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    remaining -= len(chunk)
        except (OSError, ConnectionError):
            # Client closed the connection (common on seek — browser
            # aborts the in-flight range to issue a new one). Silent.
            pass


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_server() -> int:
    """Start the local file server (once). Returns the port number."""
    global _server_port, _server_thread, _httpd, _request_token
    with _lock:
        if _server_port and _server_thread and _server_thread.is_alive():
            return _server_port
        # TOCTOU retry: _pick_free_port releases the probe socket
        # before bind, so another process can grab the port in the
        # gap. Retry a few times on EADDRINUSE before failing
        # (audit: local_fileserver L58).
        _last_err = None
        for _try in range(5):
            port = _pick_free_port()
            try:
                _httpd = ThreadingHTTPServer(
                    ("127.0.0.1", port), _FileRequestHandler)
                break
            except OSError as _be:
                _last_err = _be
                if _try == 4:
                    raise
                continue
        _server_thread = threading.Thread(
            target=_httpd.serve_forever, name="YTA-FileServer", daemon=True)
        _server_thread.start()
        _server_port = port
        _request_token = secrets.token_urlsafe(32)
        return port


def stop_server() -> None:
    """Shut the server down cleanly. Idempotent.

    BUG FIX (2026-05-13): `httpd.shutdown()` blocks until ALL active
    handler threads return. The WebView keeps long-lived streaming
    requests open (video files, thumbnails), so shutdown() would
    wait indefinitely — Quit froze the whole app. Fix: just close
    the listening socket so no NEW requests come in. The handler
    threads are daemons; they die when the Python process exits.
    Run the close on a 1s-timeout background thread so even
    server_close() can't deadlock the caller.
    """
    global _server_port, _server_thread, _httpd, _request_token
    with _lock:
        httpd = _httpd
        _httpd = None
        _server_port = 0
        _server_thread = None
        _request_token = ""
    if httpd is None:
        return
    def _close():
        try: httpd.server_close()
        except Exception as e: _log.debug("swallowed: %s", e)
    t = threading.Thread(target=_close, daemon=True)
    t.start()
    t.join(timeout=1.0)


def get_port() -> int:
    """Return the active port (0 if server hasn't started yet)."""
    return _server_port


def url_for(path: str) -> str:
    """Return an HTTP URL the webview can use to fetch `path`.

    Caller is responsible for starting the server first via `start_server()`.
    """
    if not _server_port or not path or not _request_token:
        return ""
    # quote the whole abs path so spaces/brackets/apostrophes are encoded.
    # safe="" means EVERYTHING gets percent-encoded except alphanumerics —
    # the path separators come back through because they're inside the
    # `quote` call's default safe chars.
    norm = os.path.abspath(path).replace("\\", "/")
    encoded = urllib.parse.quote(norm, safe="")
    token = urllib.parse.quote(_request_token, safe="")
    return f"http://127.0.0.1:{_server_port}/file/{encoded}?t={token}"
