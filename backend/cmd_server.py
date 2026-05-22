"""
HTTP command server on 127.0.0.1:9855 — the API the ArchivePlayer companion +
ArchiveBrowserWithYTTest viewers talk to.

Mirrors YTArchiver.py:34400-34960 _start_cmd_server + _CmdHandler. Scope:

  GET /cmd/ping → liveness probe; returns app version + gpu depth
  GET /cmd/gpu-status → queue counts
  POST /cmd/retranscribe → queue a re-transcribe (body: {video_id | filepath, title, channel})

Additional endpoints the live app has (repair-orphans, repair-duplicates,
repair-mismatches) are NOT wired here — they'd need the whole playlist-diff
machinery, and neither viewer calls them in day-to-day use yet.

Bind defaults to 127.0.0.1 (loopback only). ArchivePlayer's host-discovery
probes 127.0.0.1 first (archive_player.py:3017) so same-machine integration
works out of the box. For cross-machine LAN integration — where ArchivePlayer
runs on a different PC and relies on the /24 subnet scan path — set env
`YTARCHIVER_CMD_BIND=0.0.0.0` to re-enable LAN binding.

Previous default was 0.0.0.0 which exposed an unauthenticated control plane
to every host on the LAN. Switched to loopback 2026-04-23 (audit D-40).
"""

from __future__ import annotations

import http.server as _http_server
import json
import os
import secrets
import socket
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .log import get_logger

_log = get_logger(__name__)


# generate a per-install auth token. Same-machine
# trust boundary was previously implicit — any local process could POST
# to /cmd/retranscribe. Now requests must echo X-Auth-Token (or include
# ?token=... in the URL). Token persisted to %APPDATA%\YTArchiver\
# cmd_token so external tools (ArchivePlayer) can read it. GET endpoints
# (/cmd/ping, /cmd/gpu-status) remain unauthenticated since they're
# pure read-only liveness/status. POST endpoints require the token.
_TOKEN_FILE_NAME = "cmd_token"
_AUTH_TOKEN: str = ""


def _appdata_dir() -> Path:
    """Best-effort YTArchiver appdata dir; falls back to cwd."""
    base = os.environ.get("APPDATA") or os.environ.get("LOCALAPPDATA")
    if base:
        p = Path(base) / "YTArchiver"
    else:
        p = Path.cwd() / ".ytarchiver"
    try:
        p.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    return p


def _load_or_create_token() -> str:
    """Read the cmd auth token, generating one on first launch."""
    global _AUTH_TOKEN
    if _AUTH_TOKEN:
        return _AUTH_TOKEN
    tok_path = _appdata_dir() / _TOKEN_FILE_NAME
    try:
        if tok_path.is_file():
            t = tok_path.read_text(encoding="utf-8").strip()
            if t and len(t) >= 16:
                _AUTH_TOKEN = t
                return _AUTH_TOKEN
    except OSError as e:
        _log.debug("swallowed: %s", e)
    # Generate fresh + atomic write so two concurrent launches don't
    # leave one process running with an in-memory token that doesn't
    # match the on-disk file the other process wrote (audit:
    # cmd_server L59).
    _AUTH_TOKEN = secrets.token_urlsafe(32)
    try:
        _tmp_tok = str(tok_path) + ".tmp"
        with open(_tmp_tok, "w", encoding="utf-8") as _f:
            _f.write(_AUTH_TOKEN)
            try:
                _f.flush()
                os.fsync(_f.fileno())
            except OSError:
                pass
        os.replace(_tmp_tok, str(tok_path))
        # If a concurrent launch raced us, prefer their token over
        # ours so both processes converge to the same value.
        try:
            _disk = tok_path.read_text(encoding="utf-8").strip()
            if _disk and _disk != _AUTH_TOKEN and len(_disk) >= 16:
                _AUTH_TOKEN = _disk
        except OSError:
            pass
    except OSError as e:
        _log.debug("swallowed: %s", e)
    return _AUTH_TOKEN


def get_auth_token() -> str:
    """Return the active cmd auth token (creates it if needed).
    Callers in-process can pass this to legitimate POSTs."""
    return _load_or_create_token()


def get_token_path() -> str:
    """Return the absolute path to the token file (for external
    tools that need to read it)."""
    return str(_appdata_dir() / _TOKEN_FILE_NAME)


_CMD_PORT = 9855
# the previous default of `os.environ.get(...)` meant a
# legacy `YTARCHIVER_CMD_BIND=0.0.0.0` in the user's env would
# silently re-open the LAN binding even after the 2026-04-23 fix.
# Require both the env-var AND an explicit opt-in flag
# `YTARCHIVER_CMD_ALLOW_LAN=1`. Without the second flag, non-
# loopback bind addresses are ignored and we fall back to
# loopback. Set both for cross-host integration on a LAN
# (companion tool running on another PC).
_env_bind = os.environ.get("YTARCHIVER_CMD_BIND", "127.0.0.1")
_allow_lan = os.environ.get("YTARCHIVER_CMD_ALLOW_LAN", "") == "1"
_is_loopback = _env_bind in ("127.0.0.1", "localhost", "::1")
_CMD_BIND = _env_bind if (_is_loopback or _allow_lan) else "127.0.0.1"
# If the user asked for LAN binding without the explicit opt-in,
# log a one-line warning so they know why the effective bind is loopback.
# print → logger (PyInstaller --noconsole drops stdout).
if _env_bind != "127.0.0.1" and not _allow_lan:
    _log.warning("ignoring YTARCHIVER_CMD_BIND=%s — set "
                 "YTARCHIVER_CMD_ALLOW_LAN=1 to confirm LAN exposure.",
                 _env_bind)

# Companion-tool integration note: if a companion tool ever moves
# to a different host it will need to either (a) run on the same
# machine (loopback) or (b) set both env vars AND add its own auth
# header / token.


_HANDLERS: dict[str, dict[str, Callable]] = {"get": {}, "post": {}}
_STATE = {"started": False, "app_version": "", "srv": None, "thread": None}


def register_handler(method: str, path: str, fn: Callable):
    """Register a handler fn(body) -> dict for a given method + path.
    method: "get" or "post". Body is parsed JSON for POSTs, empty for GETs."""
    _HANDLERS[method.lower()][path] = fn


def _read_body(request) -> dict[str, Any]:
    try:
        length = int(request.headers.get("Content-Length") or 0)
    except (TypeError, ValueError):
        length = 0
    if length <= 0:
        return {}
    try:
        return json.loads(request.rfile.read(length).decode("utf-8"))
    except Exception:
        return {}


class _CmdHandler(_http_server.BaseHTTPRequestHandler):
    server_version = "YTArchiver-CMD"

    def _cors_origin(self) -> str:
        """audit C-5: return the Access-Control-Allow-Origin value.
        Wildcard "*" was the prior behavior and let any webpage in
        any browser tab issue cross-origin POSTs to /cmd/retranscribe.
        Restrict to null (file://), and localhost origins — the only
        callers that should reach this server are (a) the pywebview
        shell from a file:// URL and (b) ArchivePlayer from loopback
        Python (no CORS gate on non-browser clients). If the
        incoming Origin doesn't match, omit the CORS header so the
        browser blocks the response — functionality for the real
        callers is preserved, CSRF surface eliminated.
        """
        origin = self.headers.get("Origin", "")
        if not origin:
            # Non-browser (ArchivePlayer / curl) — no CORS needed.
            return ""
        if origin in ("null",):
            return origin
        if origin.startswith("http://127.0.0.1") or \
                origin.startswith("http://localhost") or \
                origin.startswith("https://127.0.0.1") or \
                origin.startswith("https://localhost"):
            return origin
        # Unknown origin — don't advertise CORS. Browser will block.
        return ""

    def _json(self, code, body):
        payload = json.dumps(body).encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            _origin = self._cors_origin()
            if _origin:
                self.send_header("Access-Control-Allow-Origin", _origin)
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()
            self.wfile.write(payload)
        except Exception as e:
            _log.debug("swallowed: %s", e)

    def do_OPTIONS(self):
        try:
            self.send_response(204)
            _origin = self._cors_origin()
            if _origin:
                self.send_header("Access-Control-Allow-Origin", _origin)
                self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()
        except Exception as e:
            _log.debug("swallowed: %s", e)

    def do_GET(self):
        fn = _HANDLERS["get"].get(self.path)
        if fn is None:
            self._json(404, {"ok": False, "error": "unknown_command",
                             "path": self.path})
            return
        try:
            resp = fn({}) or {}
            if not isinstance(resp, dict):
                resp = {"ok": True, "value": resp}
            resp.setdefault("ok", True)
            self._json(200, resp)
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    def do_POST(self):
        body = _read_body(self)
        # POST endpoints require the X-Auth-Token
        # header (or ?token=... query param). Generated per-install
        # token at %APPDATA%\YTArchiver\cmd_token. Without this any
        # local process could trigger retranscribe jobs.
        expected = _load_or_create_token()
        supplied = self.headers.get("X-Auth-Token", "") or ""
        if not supplied and "?" in (self.path or ""):
            try:
                from urllib.parse import parse_qs, urlparse
                qs = parse_qs(urlparse(self.path).query)
                supplied = (qs.get("token") or [""])[0]
            except Exception:
                supplied = ""
        if not supplied or not secrets.compare_digest(supplied, expected):
            self._json(401, {"ok": False, "error": "unauthorized"})
            return
        fn = _HANDLERS["post"].get(self.path.split("?", 1)[0])
        if fn is None:
            self._json(404, {"ok": False, "error": "unknown_command",
                             "path": self.path})
            return
        try:
            resp = fn(body) or {}
            if not isinstance(resp, dict):
                resp = {"ok": True, "value": resp}
            resp.setdefault("ok", True)
            self._json(200, resp)
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    # Silence the default access-log chatter
    def log_message(self, fmt, *args):
        pass


def _port_busy(bind: str, port: int) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.settimeout(0.2)
        return s.connect_ex((bind if bind != "0.0.0.0" else "127.0.0.1", port)) == 0
    except Exception:
        return False
    finally:
        try: s.close()
        except Exception as e: _log.debug("swallowed: %s", e)


def start_server(app_version: str = "",
                 on_log: Callable[[str], None] | None = None) -> bool:
    """Start the HTTP command server. Idempotent; returns True on a fresh
    start, False if already running or the port is busy."""
    if _STATE.get("started"):
        return False
    _STATE["app_version"] = app_version
    # ensure auth token exists before first request.
    _load_or_create_token()
    # Register built-in handlers (ping + gpu-status). Callers can add more.
    register_handler("get", "/cmd/ping", lambda _b: {
        "version": _STATE.get("app_version", ""),
        "gpu_depth": 0, # overridden if main.py registers a richer handler
    })
    try:
        srv = _http_server.ThreadingHTTPServer(
            (_CMD_BIND, _CMD_PORT), _CmdHandler)
    except OSError as e:
        if on_log:
            try: on_log(f"[cmd] Port {_CMD_PORT} busy \u2014 viewer integration disabled ({e})")
            except Exception as e: _log.debug("swallowed: %s", e)
        return False
    _STATE["srv"] = srv
    def _serve():
        try: srv.serve_forever()
        except Exception as e: _log.debug("swallowed: %s", e)
    t = threading.Thread(target=_serve, name="ytarchiver-cmd",
                         daemon=True)
    t.start()
    _STATE["thread"] = t
    _STATE["started"] = True
    if on_log:
        try:
            on_log(f"[cmd] Viewer-integration receiver listening on "
                   f"{_CMD_BIND}:{_CMD_PORT}"
                   f"{' (LAN-accessible)' if _CMD_BIND == '0.0.0.0' else ' (localhost only)'}")
        except Exception as e:
            _log.debug("swallowed: %s", e)
    return True


def stop_server():
    """Stop the cmd HTTP server. BUG FIX (2026-05-13): `srv.shutdown()`
    blocks until handler threads return — if a ping request is in
    flight (frequent during normal use), shutdown waits indefinitely
    and the Quit path freezes. Close the socket on a timeout-bounded
    thread; daemon handler threads die with the process.
    """
    srv = _STATE.get("srv")
    _STATE["srv"] = None
    _STATE["thread"] = None
    _STATE["started"] = False
    if srv is None:
        return
    def _close():
        try: srv.server_close()
        except Exception as e: _log.debug("swallowed: %s", e)
    import threading as _th
    t = _th.Thread(target=_close, daemon=True)
    t.start()
    t.join(timeout=1.0)
