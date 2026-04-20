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
_lock = threading.Lock()


class _FileRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002 — stdlib signature
        # Silence access log chatter; pywebview traffic is a constant stream
        pass

    def do_GET(self):
        if self.path in ("/", "/ping"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
            return
        # /file/<url-encoded-abspath>
        if not self.path.startswith("/file/"):
            self.send_error(404)
            return
        raw = self.path[len("/file/"):]
        # Strip query string if any
        q = raw.find("?")
        if q >= 0:
            raw = raw[:q]
        try:
            path = urllib.parse.unquote(raw)
        except Exception:
            self.send_error(400)
            return
        # Normalize + basic safety: require absolute path, must exist,
        # must be a file (not a dir), no funky traversal.
        if not path or ".." in path.replace("\\", "/").split("/"):
            self.send_error(400); return
        if not os.path.isabs(path):
            self.send_error(400); return
        if not os.path.isfile(path):
            self.send_error(404); return
        ctype, _ = mimetypes.guess_type(path)
        if not ctype:
            ctype = "application/octet-stream"
        try:
            size = os.path.getsize(path)
        except OSError:
            self.send_error(500); return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(size))
        self.send_header("Cache-Control", "public, max-age=86400")
        self.end_headers()
        try:
            with open(path, "rb") as f:
                # Stream in chunks for big video thumbnails etc.
                while True:
                    chunk = f.read(64 * 1024)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
        except (OSError, ConnectionError):
            pass


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_server() -> int:
    """Start the local file server (once). Returns the port number."""
    global _server_port, _server_thread
    with _lock:
        if _server_port and _server_thread and _server_thread.is_alive():
            return _server_port
        port = _pick_free_port()
        httpd = ThreadingHTTPServer(("127.0.0.1", port), _FileRequestHandler)
        _server_thread = threading.Thread(
            target=httpd.serve_forever, name="YTA-FileServer", daemon=True)
        _server_thread.start()
        _server_port = port
        return port


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
