"""
ThumbnailMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class ThumbnailMixin:

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
            cfg = self._config or load_config()
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
            try:
                from backend import metadata as _md
                self._log_stream.emit_text(
                    f" - Thumbnail refetch starting for {name}...",
                    "simpleline")
                res = _md.sweep_missing_thumbnails(ch, stream=self._log_stream)
                self._log_stream.emit_text(
                    f" - Thumbnail refetch for {name}: "
                    f"{res.get('fetched', 0)} fetched, "
                    f"{res.get('missing', 0)} still missing, "
                    f"{res.get('checked', 0)} checked",
                    "simpleline_green")
            except Exception as _e:
                self._log_stream.emit_error(
                    f"Thumbnail refetch failed for {name}: {_e}")
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}


    def realign_misplaced_thumbnails(self, dry_run=True):
        """Survey (and optionally move) thumbnails that ended up in the
        wrong year/month folder. Same-volume os.replace so no copy/
        delete cycle. Returns survey counts. `dry_run=True` (default)
        just reports; pass False to actually move.
        """
        try:
            from backend import metadata as _md
            res = _md.realign_misplaced_thumbnails(
                channels=(self._config or load_config() or {}).get("channels", []),
                dry_run=bool(dry_run),
                stream=self._log_stream if not dry_run else None)
            return res
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def refetch_thumbnails_all(self):
        """Run sweep_missing_thumbnails sequentially across every saved
        channel. Single background thread (not the sync queue) so this
        doesn't block scheduled syncs; thumbnails are a side-channel
        cosmetic fetch, not a sync operation. Logs per-channel progress
        + a final tally.

        Returns immediately with `{ok, started, channels}` — the actual
        work runs async; the user watches the log.
        """
        cfg = self._config or load_config()
        channels = sorted(cfg.get("channels", []),
                          key=lambda c: (c.get("name") or "").lower())
        if not channels:
            return {"ok": False, "error": "No channels configured"}
        def _run():
            try:
                from backend import metadata as _md
                total_fetched = total_missing = total_checked = 0
                self._log_stream.emit_text(
                    f" — Thumbnail refetch starting for "
                    f"{len(channels)} channel(s)…",
                    "simpleline_pink")
                self._log_stream.flush()
                for i, ch in enumerate(channels, 1):
                    nm = ch.get("name") or ch.get("folder") or "?"
                    try:
                        self._log_stream.emit_text(
                            f"  - [{i}/{len(channels)}] {nm}…",
                            "simpleline")
                        res = _md.sweep_missing_thumbnails(
                            ch, stream=self._log_stream)
                        total_fetched += int(res.get("fetched", 0) or 0)
                        total_missing += int(res.get("missing", 0) or 0)
                        total_checked += int(res.get("checked", 0) or 0)
                        if res.get("fetched", 0) > 0 or res.get("missing", 0) > 0:
                            self._log_stream.emit_text(
                                f"    {res.get('fetched', 0)} fetched, "
                                f"{res.get('missing', 0)} still missing, "
                                f"{res.get('checked', 0)} checked",
                                "simpleline_green")
                    except Exception as _per_ch_e:
                        self._log_stream.emit_error(
                            f"Thumbnail refetch failed for {nm}: "
                            f"{_per_ch_e}")
                self._log_stream.emit_text(
                    f" — Thumbnail refetch complete: "
                    f"{total_fetched} fetched, "
                    f"{total_missing} still missing, "
                    f"{total_checked} checked across "
                    f"{len(channels)} channel(s).",
                    "simpleline_pink")
                self._log_stream.flush()
            except Exception as _e:
                self._log_stream.emit_error(
                    f"Bulk thumbnail refetch failed: {_e}")
        threading.Thread(target=_run, daemon=True,
                         name="thumb-refetch-all").start()
        return {"ok": True, "started": True,
                "channels": len(channels)}
