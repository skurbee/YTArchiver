"""Validated JSON-line transport shared by standalone Python 3.11 workers.

This module must remain dependency-free: workers import it beside their source
script under an external interpreter, without importing the application.
"""
from __future__ import annotations

import json
import math
import threading
from collections.abc import Iterator
from typing import Any, TextIO


class ProtocolError(ValueError):
    """A complete message does not satisfy the worker transport contract."""


def _object(line: str) -> dict[str, Any]:
    try:
        value = json.loads(line)
    except (ValueError, TypeError) as exc:
        raise ProtocolError("Malformed request: not valid JSON") from exc
    if not isinstance(value, dict):
        raise ProtocolError("Worker message must be a JSON object")
    return value


def decode_request(line: str, kind: str) -> dict[str, Any]:
    request = _object(line)
    if kind == "whisper":
        if "command" in request:
            if request["command"] != "cancel":
                raise ProtocolError("Unknown worker command")
            return {"command": "cancel"}
        if not isinstance(request.get("path"), str) or not request["path"].strip():
            raise ProtocolError("Whisper request requires a nonempty path")
        for field in ("duration", "duration_fallback"):
            value = request.get(field, 0)
            try:
                valid = (not isinstance(value, bool) and isinstance(value, (int, float))
                         and math.isfinite(value) and value >= 0)
            except OverflowError:
                valid = False
            if not valid:
                raise ProtocolError(f"Invalid {field}")
        language = request.get("language", "en")
        if language is not None and (not isinstance(language, str) or not language.strip()):
            raise ProtocolError("Invalid language")
    elif kind == "punctuation":
        if not isinstance(request.get("text"), str):
            raise ProtocolError("Punctuation request requires text")
    else:
        raise ProtocolError("Unknown worker request type")
    return request


def decode_response(line: str) -> dict[str, Any]:
    message = _object(line)
    status = message.get("status")
    if not isinstance(status, str) or status not in {"ready", "starting", "progress", "ok", "error", "cancelled"}:
        raise ProtocolError("Unknown worker response status")
    if status == "progress":
        value = message.get("pct")
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not 0 <= value <= 100 or not math.isfinite(value)):
            raise ProtocolError("Invalid worker progress")
    if status in {"ok", "error"} and not isinstance(message.get("text", ""), str):
        raise ProtocolError("Invalid worker result text")
    if status == "ok" and not isinstance(message.get("segments", []), list):
        raise ProtocolError("Invalid worker result segments")
    return message


class ProtocolWriter:
    """The sole response writer, including errors from the stdin thread."""

    def __init__(self, stream: TextIO, *, model: str | None = None):
        self._stream = stream
        self._model = model
        self._lock = threading.Lock()

    def send(self, message: dict[str, Any]) -> None:
        payload = dict(message)
        if self._model is not None:
            payload.setdefault("model", self._model)
        line = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        decode_response(line)
        with self._lock:
            self._stream.write(line + "\n")
            self._stream.flush()


def iter_requests(stream: TextIO, kind: str, writer: ProtocolWriter) -> Iterator[dict[str, Any]]:
    """Reject one bad message with one response, then keep serving the pipe."""
    for line in stream:
        if not line.strip():
            continue
        try:
            request = decode_request(line, kind)
        except ProtocolError as exc:
            writer.send({"status": "error", "text": str(exc), "error": str(exc)})
            continue
        yield request
