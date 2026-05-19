"""
backend/log.py — bridge Python's `logging` module into the UI LogStreamer.

Stands up a configured `logging.Logger` whose records flow through the
existing LogStreamer.emit_text() so they appear in the UI log panel.
Lets us replace silent `except Exception: pass` blocks with
`log.debug(...)` calls without spamming the Simple-mode user.

Level → tag mapping (uses existing LogStreamer tag scheme):
    DEBUG    → "dim"  (already in VERBOSE_ONLY_TAGS → hidden in Simple mode)
    INFO     → None   (default style, visible in both modes)
    WARNING  → None   (no dedicated yellow tag yet; Patch 1 stays minimal)
    ERROR    → "red"
    CRITICAL → "red"

Usage from any module:
    from backend.log import get_logger
    log = get_logger(__name__)

    try:
        do_something()
    except Exception as e:
        log.debug("do_something failed in xyz path: %s", e)

Wire-up (once, in main.py):
    from backend.log import install as install_log_bridge
    install_log_bridge(self._log_stream)
"""

from __future__ import annotations

import logging

_LEVEL_TO_TAG = {
    logging.DEBUG:    "dim",
    logging.INFO:     None,
    logging.WARNING:  None,
    logging.ERROR:    "red",
    logging.CRITICAL: "red",
}


_ROOT_LOGGER_NAME = "ytarchiver"
_bridge_attached = False


class LogStreamerHandler(logging.Handler):
    """Forwards LogRecords to a LogStreamer as one tagged line per record."""

    def __init__(self, stream, level: int = logging.DEBUG):
        super().__init__(level=level)
        self._stream = stream

    def emit(self, record: logging.LogRecord) -> None:
        try:
            text = self.format(record)
            tag = _LEVEL_TO_TAG.get(record.levelno, None)
            self._stream.emit_text(text, tag)
        except Exception:
            # Never let the logger crash the caller; can't log this
            # without recursing, so defer to logging's own error path.
            self.handleError(record)


def install(stream) -> None:
    """Attach the LogStreamer bridge to the `ytarchiver` root logger.

    Idempotent — safe to call more than once. Should be called exactly
    once from main.py shortly after the LogStreamer is instantiated.
    """
    global _bridge_attached
    root = logging.getLogger(_ROOT_LOGGER_NAME)
    root.setLevel(logging.DEBUG)
    if _bridge_attached:
        return
    handler = LogStreamerHandler(stream)
    # Prefix DEBUG/WARNING/ERROR messages with the logger name (e.g.
    # `[ytarchiver.backend.sync]`) so silenced-but-logged swallows are
    # locatable. INFO messages are user-facing announcements (e.g. the
    # startup line) and skip the prefix — Patch 3 only emits DEBUG/ERROR
    # from internal code paths.
    class _NamedFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            base = super().format(record)
            if record.levelno == logging.INFO:
                return base
            return f"[{record.name}] {base}"
    handler.setFormatter(_NamedFormatter("%(message)s"))
    root.addHandler(handler)
    # Don't propagate to Python's stderr root logger — we don't want
    # duplicate output in the packaged exe console (when one is attached).
    root.propagate = False
    _bridge_attached = True


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a logger that flows through the LogStreamer bridge.

    Names like "backend.sync" become children of the `ytarchiver` root
    logger and inherit its handler. Calling with no name returns the
    root itself.
    """
    if not name or name == _ROOT_LOGGER_NAME:
        return logging.getLogger(_ROOT_LOGGER_NAME)
    if name.startswith(_ROOT_LOGGER_NAME + "."):
        return logging.getLogger(name)
    return logging.getLogger(f"{_ROOT_LOGGER_NAME}.{name}")


# ── standardized swallow helper ─────────────────────
# The codebase had 500+ instances of `try: ... except Exception as e:
# _log.debug("swallowed: %s", e)` — indistinguishable from each other
# when scrolling logs. The helper below requires a `reason` argument
# so each swallow carries WHY it's safe. Use `level="warning"` for
# unexpected-but-recoverable failures (network blip, file briefly
# locked); the default DEBUG stays out of Simple-mode logs.

def swallow(reason: str, exc: BaseException, *,
            level: str = "debug", logger_name: str | None = None
            ) -> None:
    """Log a swallowed exception with an explicit reason.

    Args:
        reason: short tag describing why this swallow is OK (e.g.
            "probe cleanup", "shutdown race", "transient stat").
            Always provide one — that's the whole point of this
            helper vs. the old `swallowed: %s` pattern.
        exc: the exception that was caught.
        level: "debug" (default, hidden in Simple mode) or "warning"
            (visible — use for unexpected failures).
        logger_name: optional module name; defaults to the caller's
            logger via `get_logger(None)`.
    """
    lg = get_logger(logger_name) if logger_name else get_logger(None)
    fn = getattr(lg, level, lg.debug)
    try:
        fn("swallowed (%s): %s", reason, exc)
    except Exception:
        pass
