"""
ThumbnailMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They prefer AppServices when present, with legacy
private Api attributes kept as fallback state.
"""
from __future__ import annotations

import threading
import time

from ._shared import _log
from backend.ytarchiver_config import load_config
from backend import subs as subs_backend


class ThumbnailMixin:

    _realign_init_lock = threading.Lock()

    def _thumbnail_services(self):
        return getattr(self, "services", None)

    def _thumbnail_config(self):
        services = self._thumbnail_services()
        if services is not None:
            return services.fresh_config()
        cfg = getattr(self, "_config", None)
        if cfg is not None:
            return cfg
        return load_config()

    def _thumbnail_log_stream(self):
        services = self._thumbnail_services()
        stream = (getattr(services, "log_stream", None)
                  if services is not None else None)
        return stream if stream is not None else self._log_stream

    def _ensure_realign_jobs(self):
        if (hasattr(self, "_realign_jobs")
                and hasattr(self, "_realign_jobs_lock")):
            return
        with self._realign_init_lock:
            if not hasattr(self, "_realign_jobs"):
                self._realign_jobs = {}
            if not hasattr(self, "_realign_jobs_lock"):
                self._realign_jobs_lock = threading.Lock()


    def thumbnail_status_bulk(self, force=False):
        """Issue #154: return {channel: {total, with_thumb, missing}}
        for every subscribed channel. Drives the thumbnail % column
        in Settings > Metadata.

        Cached: subsequent calls return instantly when each channel's
        folder fingerprint matches the persisted cache. Pass
        `force=True` to ignore the cache and re-walk every channel
        (wired to the "Force recheck thumbnails" button).
        """
        try:
            from backend.metadata import count_thumbnail_status_bulk
            cfg = self._thumbnail_config()
            channels = cfg.get("channels", []) or []
            return {"ok": True,
                    "rows": count_thumbnail_status_bulk(
                        channels, force=bool(force))}
        except Exception as e:
            return {"ok": False, "error": str(e), "rows": {}}


    def refetch_thumbnails(self, folder_or_name):
        """Issue #154: spawn a background sweep that downloads any
        missing thumbnails for one channel. Returns immediately; the
        sweep result goes to the log.
        """
        name = folder_or_name if isinstance(folder_or_name, str) else (
            folder_or_name.get("name") or folder_or_name.get("folder", ""))
        ch = subs_backend.get_channel({"name": name})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        def _run():
            log_stream = self._thumbnail_log_stream()
            try:
                from backend import metadata as _md
                log_stream.emit_text(
                    f" - Thumbnail refetch starting for {name}...",
                    "simpleline")
                res = _md.sweep_missing_thumbnails(ch, stream=log_stream)
                log_stream.emit_text(
                    f" - Thumbnail refetch for {name}: "
                    f"{res.get('fetched', 0)} fetched, "
                    f"{res.get('missing', 0)} still missing, "
                    f"{res.get('checked', 0)} checked",
                    "simpleline_green")
            except Exception as _e:
                log_stream.emit_error(
                    f"Thumbnail refetch failed for {name}: {_e}")
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}


    def realign_start(self, dry_run=True):
        """Start a thumbnail realign pass (survey when dry_run=True, move
        when False) on a worker thread and return a token. The pass
        streams per-channel progress to the main log and can be stopped
        via realign_cancel(token); poll realign_poll(token) for the
        result. Background-threaded so the long os.walk over a multi-TB
        archive doesn't block the bridge (the old synchronous call ran
        for minutes with the UI showing only a frozen 'Scanning…')."""
        try:
            import uuid as _uuid
            self._ensure_realign_jobs()
            token = _uuid.uuid4().hex
            cancel_ev = threading.Event()
            now = time.time()
            with self._realign_jobs_lock:
                # Sweep abandoned entries (>15 min) so the dict can't grow.
                stale = [k for k, v in self._realign_jobs.items()
                         if isinstance(v, dict)
                         and (now - (v.get("_ts") or now)) > 900]
                for k in stale:
                    self._realign_jobs.pop(k, None)
                self._realign_jobs[token] = {
                    "done": False, "cancel": cancel_ev, "_ts": now}

            def _worker():
                log_stream = self._thumbnail_log_stream()
                try:
                    from backend import metadata as _md
                    res = _md.realign_misplaced_thumbnails(
                        channels=(self._thumbnail_config()
                                  or {}).get("channels", []),
                        dry_run=bool(dry_run),
                        stream=log_stream,
                        cancel_event=cancel_ev)
                except Exception as _e:
                    res = {"ok": False, "error": str(_e)}
                with self._realign_jobs_lock:
                    self._realign_jobs[token] = {
                        "done": True, "result": res, "cancel": cancel_ev,
                        "_ts": time.time()}

            threading.Thread(target=_worker, daemon=True,
                             name="thumb-realign").start()
            return {"ok": True, "started": True, "token": token}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def realign_poll(self, token):
        """Poll a realign_start token. Returns {pending: True} while
        running, or the final result payload (with ok/scanned/misaligned/
        moved/per_channel/cancelled) when done. The token is forgotten
        once the final payload is returned."""
        self._ensure_realign_jobs()
        with self._realign_jobs_lock:
            entry = self._realign_jobs.get(token)
            if entry is None:
                return {"ok": False, "error": "unknown token"}
            if not entry.get("done"):
                return {"ok": True, "pending": True}
            self._realign_jobs.pop(token, None)
            return entry.get("result") or {"ok": False, "error": "no result"}


    def realign_cancel(self, token):
        """Signal a running realign pass (by token) to stop at the next
        channel / directory boundary. Idempotent; safe on an unknown or
        already-finished token."""
        self._ensure_realign_jobs()
        with self._realign_jobs_lock:
            entry = self._realign_jobs.get(token)
            ev = entry.get("cancel") if isinstance(entry, dict) else None
        if ev is not None:
            try:
                ev.set()
            except Exception as e:
                _log.debug("swallowed: %s", e)
        return {"ok": True}


    def refetch_thumbnails_all(self):
        """Run sweep_missing_thumbnails sequentially across every saved
        channel. Single background thread (not the sync queue) so this
        doesn't block scheduled syncs; thumbnails are a side-channel
        cosmetic fetch, not a sync operation. Logs per-channel progress
        + a final tally.

        Returns immediately with `{ok, started, channels}` — the actual
        work runs async; the user watches the log.
        """
        cfg = self._thumbnail_config()
        channels = sorted(cfg.get("channels", []),
                          key=lambda c: (c.get("name") or "").lower())
        if not channels:
            return {"ok": False, "error": "No channels configured"}
        def _run():
            log_stream = self._thumbnail_log_stream()
            try:
                from backend import metadata as _md
                total_fetched = total_missing = total_checked = 0
                log_stream.emit_text(
                    f" — Thumbnail refetch starting for "
                    f"{len(channels)} channel(s)…",
                    "simpleline_pink")
                log_stream.flush()
                for i, ch in enumerate(channels, 1):
                    nm = ch.get("name") or ch.get("folder") or "?"
                    try:
                        log_stream.emit_text(
                            f"  - [{i}/{len(channels)}] {nm}…",
                            "simpleline")
                        res = _md.sweep_missing_thumbnails(
                            ch, stream=log_stream)
                        total_fetched += int(res.get("fetched", 0) or 0)
                        total_missing += int(res.get("missing", 0) or 0)
                        total_checked += int(res.get("checked", 0) or 0)
                        if res.get("fetched", 0) > 0 or res.get("missing", 0) > 0:
                            log_stream.emit_text(
                                f"    {res.get('fetched', 0)} fetched, "
                                f"{res.get('missing', 0)} still missing, "
                                f"{res.get('checked', 0)} checked",
                                "simpleline_green")
                    except Exception as _per_ch_e:
                        log_stream.emit_error(
                            f"Thumbnail refetch failed for {nm}: "
                            f"{_per_ch_e}")
                log_stream.emit_text(
                    f" — Thumbnail refetch complete: "
                    f"{total_fetched} fetched, "
                    f"{total_missing} still missing, "
                    f"{total_checked} checked across "
                    f"{len(channels)} channel(s).",
                    "simpleline_pink")
                log_stream.flush()
            except Exception as _e:
                log_stream.emit_error(
                    f"Bulk thumbnail refetch failed: {_e}")
        threading.Thread(target=_run, daemon=True,
                         name="thumb-refetch-all").start()
        return {"ok": True, "started": True,
                "channels": len(channels)}
