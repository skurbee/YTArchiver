"""Grant individual yt-dlp requests through the parent traffic governor.

The child receives only an ephemeral loopback port and a per-launch secret.
Requests contain a fixed operation category, never URLs, cookies or headers.
There is no independent child ledger: reservations, pacing and the emergency
circuit remain owned by ``youtube_traffic`` in this process.
"""

from __future__ import annotations

import atexit
import contextlib
import json
import secrets
import socketserver
import threading
import time
from collections.abc import Iterator
from typing import Any

from . import youtube_traffic

_MAX_MESSAGE = 4096
_READ_TIMEOUT = 3.0
_POLL_INTERVAL = 0.1
_UNBOUND_LIFETIME = 60.0
_ALLOWED_KINDS = frozenset({
    "youtube_http", "youtube_page", "youtube_api", "youtube_caption",
    "youtube_thumbnail", "youtube_manifest", "youtube_media_manifest", "youtube_media",
})
_BROKER_LOCK = threading.Lock()
_broker: _RequestBroker | None = None


class _CancelSignal:
    def __init__(self, session: RequestSession):
        self._session = session

    def is_set(self) -> bool:
        return self._session._cancelled()

    def wait(self, timeout: float | None = None) -> bool:
        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        while not self.is_set():
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                return False
            self._session._closed.wait(
                _POLL_INTERVAL if remaining is None else min(_POLL_INTERVAL, remaining))
        return True


class _PauseSignal:
    def __init__(self, session: RequestSession):
        self._session = session

    def is_set(self) -> bool:
        with self._session._lock:
            event = self._session._pause_event
        return event is not None and event.is_set()


class RequestSession:
    """One launch's authorization and intentional-wait accounting."""

    def __init__(self, broker: _RequestBroker, reservation_id: str):
        self._broker = broker
        self._token = secrets.token_urlsafe(32)
        self._reservation_id = reservation_id
        self._created = time.monotonic()
        self._closed = threading.Event()
        self._lock = threading.RLock()
        self._request_lock = threading.Lock()
        self._proc: Any = None
        self._cancel_event: Any = None
        self._pause_event: Any = None
        self._waiting_count = 0
        self._waiting_since = 0.0
        self._wait_total = 0.0
        self._cancel = _CancelSignal(self)
        self._pause = _PauseSignal(self)

    def environment(self) -> dict[str, str]:
        return {
            "YTARCHIVER_TRAFFIC_PORT": str(self._broker.port),
            "YTARCHIVER_TRAFFIC_TOKEN": self._token,
        }

    def bind(self, proc: Any) -> None:
        """Associate the child after Popen, including early-request races."""
        with self._lock:
            if self._proc is not None and self._proc is not proc:
                raise ValueError("A traffic session belongs to one child process")
            self._proc = proc
        if self._cancelled():
            self.close()

    def set_signals(self, cancel_event=None, pause_event=None) -> None:
        """Attach worker controls; omitted signals preserve prior controls."""
        with self._lock:
            if cancel_event is not None:
                self._cancel_event = cancel_event
            if pause_event is not None:
                self._pause_event = pause_event

    def close(self) -> None:
        self._closed.set()
        self._broker.discard(self)

    def _cancelled(self) -> bool:
        if self._closed.is_set():
            return True
        with self._lock:
            proc = self._proc
            cancel_event = self._cancel_event
        if cancel_event is not None and cancel_event.is_set():
            return True
        if proc is not None:
            try:
                return proc.poll() is not None
            except Exception:
                # A child whose lifecycle cannot be verified gets no grant.
                return True
        return time.monotonic() - self._created >= _UNBOUND_LIFETIME

    def is_waiting(self) -> bool:
        with self._lock:
            return self._waiting_count > 0

    def wait_seconds(self) -> float:
        """Return elapsed intentional waiting, including the active interval."""
        with self._lock:
            active = time.monotonic() - self._waiting_since if self._waiting_count else 0.0
            return self._wait_total + max(0.0, active)

    @contextlib.contextmanager
    def _waiting(self) -> Iterator[None]:
        with self._lock:
            if not self._waiting_count:
                self._waiting_since = time.monotonic()
            self._waiting_count += 1
        try:
            yield
        finally:
            with self._lock:
                self._waiting_count -= 1
                if not self._waiting_count:
                    self._wait_total += max(0.0, time.monotonic() - self._waiting_since)

    def _wait_unpaused(self) -> bool:
        while self._pause.is_set():
            if self._cancel.wait(_POLL_INTERVAL):
                return False
        return not self._cancel.is_set()

    def acquire(self, kind: str) -> dict[str, Any]:
        # Serialize one child's concurrent requests and count their waiting
        # intervals as a union, not N times the same wall-clock interval.
        with self._waiting(), self._request_lock:
            with youtube_traffic.reservation_scope(self._reservation_id):
                while self._wait_unpaused():
                    result = youtube_traffic.acquire(
                        kind, cancel_event=self._cancel, pause_event=self._pause)
                    if result.get("paused"):
                        continue
                    if self._cancel.is_set():
                        break
                    if result.get("ok"):
                        # A pause may arrive just after the ledger append.
                        # Hold that grant until resume without charging twice.
                        if self._wait_unpaused():
                            return {"ok": True}
                        break
                    return {
                        "ok": False,
                        "error": str(result.get("error") or "YouTube request permission denied"),
                    }
        return {"ok": False, "cancelled": True, "error": "YouTube request cancelled"}


class _Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        self.connection.settimeout(_READ_TIMEOUT)
        try:
            data = self.rfile.readline(_MAX_MESSAGE + 1)
            if len(data) > _MAX_MESSAGE or not data.endswith(b"\n"):
                response = {"ok": False, "error": "Invalid traffic request"}
            else:
                payload = json.loads(data)
                response = self.server.broker.dispatch(payload)
            self.wfile.write(json.dumps(response, separators=(",", ":")).encode() + b"\n")
        except (OSError, UnicodeError, ValueError, TypeError):
            # Broken connections and malformed payloads can never authorize
            # the child. Do not log payloads or authentication material.
            return


class _Server(socketserver.ThreadingTCPServer):
    daemon_threads = True
    block_on_close = False
    allow_reuse_address = False
    request_queue_size = 32

    def __init__(self, broker: _RequestBroker):
        self.broker = broker
        self._slots = threading.BoundedSemaphore(64)
        super().__init__(("127.0.0.1", 0), _Handler)

    def process_request(self, request, client_address) -> None:
        if not self._slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._slots.release()
            raise

    def process_request_thread(self, request, client_address) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._slots.release()

    def handle_error(self, request, client_address) -> None:
        # The protocol fails closed; traceback logs must not leak payloads.
        return


class _RequestBroker:
    def __init__(self):
        self._lock = threading.Lock()
        self._sessions: dict[str, RequestSession] = {}
        self._closed = threading.Event()
        self._server = _Server(self)
        self.port = int(self._server.server_address[1])
        self._thread = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": _POLL_INTERVAL},
            name="youtube-request-broker", daemon=True)
        self._watcher = threading.Thread(
            target=self._watch_children, name="youtube-request-lifecycle", daemon=True)
        self._thread.start()
        self._watcher.start()

    def prepare(self, reservation_id: str) -> RequestSession:
        session = RequestSession(self, reservation_id)
        with self._lock:
            if self._closed.is_set():
                raise OSError("YouTube request broker is closed")
            self._sessions[session._token] = session
        return session

    def discard(self, session: RequestSession) -> None:
        with self._lock:
            self._sessions.pop(session._token, None)

    def dispatch(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {"ok": False, "error": "Invalid traffic request"}
        token = payload.get("token")
        if not isinstance(token, str) or not token or len(token) > 128:
            return {"ok": False, "error": "Traffic session unavailable"}
        with self._lock:
            session = self._sessions.get(token)
        if session is None or not secrets.compare_digest(token, session._token):
            return {"ok": False, "error": "Traffic session unavailable"}
        if session._cancelled():
            session.close()
            return {"ok": False, "error": "Traffic session closed"}
        if payload.get("op") == "rate_limit":
            # The child observed HTTP 429. Activate the shared circuit before
            # any worker can issue the next request; stderr handling may lag.
            try:
                youtube_traffic.record_rate_limit()
            except Exception:
                session.close()
                return {"ok": False, "error": "YouTube cooldown could not be recorded"}
            return {"ok": True}
        kind = payload.get("kind")
        if payload.get("op") != "acquire" or not isinstance(kind, str) or kind not in _ALLOWED_KINDS:
            return {"ok": False, "error": "Unsupported traffic request"}
        try:
            return session.acquire(kind)
        except Exception:
            session.close()
            return {"ok": False, "error": "YouTube request governor unavailable"}

    def _watch_children(self) -> None:
        while not self._closed.wait(_POLL_INTERVAL):
            with self._lock:
                sessions = list(self._sessions.values())
            for session in sessions:
                if session._cancelled():
                    session.close()

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        with self._lock:
            sessions = list(self._sessions.values())
        for session in sessions:
            session.close()
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=1)
        self._watcher.join(timeout=1)


def prepare_launch() -> RequestSession:
    """Create request authorization before launching an app-owned yt-dlp."""
    global _broker
    with _BROKER_LOCK:
        if _broker is None:
            _broker = _RequestBroker()
        broker = _broker
    return broker.prepare(youtube_traffic._current_reservation())


def _shutdown() -> None:
    global _broker
    with _BROKER_LOCK:
        broker, _broker = _broker, None
    if broker is not None:
        broker.close()


atexit.register(_shutdown)
