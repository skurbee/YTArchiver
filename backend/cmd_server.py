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

Bind defaults to 0.0.0.0 so LAN viewers can reach it, matching OLD. Use
env `YTARCHIVER_CMD_BIND=127.0.0.1` to lock to loopback only.
"""

from __future__ import annotations

import http.server as _http_server
import json
import os
import socket
import threading
from typing import Any, Callable, Dict, Optional


_CMD_PORT = 9855
_CMD_BIND = os.environ.get("YTARCHIVER_CMD_BIND", "0.0.0.0")


_HANDLERS: Dict[str, Dict[str, Callable]] = {"get": {}, "post": {}}
_STATE = {"started": False, "app_version": "", "srv": None, "thread": None}


def register_handler(method: str, path: str, fn: Callable):
    """Register a handler fn(body) -> dict for a given method + path.
    method: "get" or "post". Body is parsed JSON for POSTs, empty for GETs."""
    _HANDLERS[method.lower()][path] = fn


def _read_body(request) -> Dict[str, Any]:
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

    def _json(self, code, body):
        payload = json.dumps(body).encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()
            self.wfile.write(payload)
        except Exception:
            pass

    def do_OPTIONS(self):
        try:
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()
        except Exception:
            pass

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
        fn = _HANDLERS["post"].get(self.path)
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
        except Exception: pass


def start_server(app_version: str = "",
                 on_log: Optional[Callable[[str], None]] = None) -> bool:
    """Start the HTTP command server. Idempotent; returns True on a fresh
    start, False if already running or the port is busy."""
    if _STATE.get("started"):
        return False
    _STATE["app_version"] = app_version
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
            except Exception: pass
        return False
    _STATE["srv"] = srv
    def _serve():
        try: srv.serve_forever()
        except Exception: pass
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
        except Exception:
            pass
    return True


def stop_server():
    srv = _STATE.get("srv")
    if srv is not None:
        try: srv.shutdown()
        except Exception: pass
    _STATE["srv"] = None
    _STATE["thread"] = None
    _STATE["started"] = False
