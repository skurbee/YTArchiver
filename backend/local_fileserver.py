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
import socket
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


_server_port: int = 0
_server_thread: threading.Thread | None = None
_httpd = None  # audit D-56: module-level handle so stop_server() can
               # shut the server down cleanly (server_close releases
               # the port socket; shutdown stops the serve_forever loop).
_lock = threading.Lock()


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
        if not self.path.startswith("/file/"):
            self.send_error(404); return None
        raw = self.path[len("/file/"):]
        q = raw.find("?")
        if q >= 0:
            raw = raw[:q]
        try:
            path = urllib.parse.unquote(raw)
        except Exception:
            self.send_error(400); return None
        if not path or ".." in path.replace("\\", "/").split("/"):
            self.send_error(400); return None
        if not os.path.isabs(path):
            self.send_error(400); return None
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
    global _server_port, _server_thread, _httpd
    with _lock:
        if _server_port and _server_thread and _server_thread.is_alive():
            return _server_port
        port = _pick_free_port()
        _httpd = ThreadingHTTPServer(("127.0.0.1", port), _FileRequestHandler)
        _server_thread = threading.Thread(
            target=_httpd.serve_forever, name="YTA-FileServer", daemon=True)
        _server_thread.start()
        _server_port = port
        return port


def stop_server() -> None:
    """Shut the server down cleanly. Idempotent — safe to call from
    shutdown paths even if the server never started.

    audit D-56: without this, the listening socket was held until the
    Python process exited. On clean webview shutdown paths that
    returned from webview.start() without hitting os._exit (rare but
    possible under dev-reload), port 9855's neighbor (this file-
    server's picked port) would linger, and a re-launch would fail to
    bind. Calling shutdown() stops the serve_forever loop; server_close
    frees the socket immediately.
    """
    global _server_port, _server_thread, _httpd
    with _lock:
        httpd = _httpd
        _httpd = None
        _server_port = 0
        _server_thread = None
    if httpd is not None:
        try: httpd.shutdown()
        except Exception: pass
        try: httpd.server_close()
        except Exception: pass


def get_port() -> int:
    """Return the active port (0 if server hasn't started yet)."""
    return _server_port


def url_for(path: str) -> str:
    """Return an HTTP URL the webview can use to fetch `path`.

    Caller is responsible for starting the server first via `start_server()`.
    """
    if not _server_port or not path:
        return ""
    # quote the whole abs path so spaces/brackets/apostrophes are encoded.
    # safe="" means EVERYTHING gets percent-encoded except alphanumerics —
    # the path separators come back through because they're inside the
    # `quote` call's default safe chars.
    norm = os.path.abspath(path).replace("\\", "/")
    encoded = urllib.parse.quote(norm, safe="")
    return f"http://127.0.0.1:{_server_port}/file/{encoded}"
