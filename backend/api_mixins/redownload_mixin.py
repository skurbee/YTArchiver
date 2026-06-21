"""
RedownloadMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class RedownloadMixin:

    def resume_pending_redownloads(self):
        """Issue #162: scan queues.sync for redownload-kind tasks (left
        there by a previous run that exited before draining them) and
        re-route each through chan_redownload so the chain worker
        spawns and resumes from `_redownload_progress.json`. Without
        this, clicking the popover Resume button after a restart fired
        `sync_start_all` and started a regular Sync Subbed pass —
        the redownload state was never picked up.

        Returns {ok, resumed: N, skipped: M}.
        """
        resumed = 0
        skipped = 0
        try:
            # Snapshot the queue so we can iterate without mutation races.
            tasks_snapshot = list(self._queues.sync)
        except Exception:
            tasks_snapshot = []
        # Remove the redownload items from the live queue FIRST. The
        # chan_redownload path will re-enqueue them via sync_enqueue +
        # _redwnl_pending. Skipping this would leave stale rows in the
        # popover for the lifetime of the chain worker.
        for t in tasks_snapshot:
            kind = (t.get("kind") or "").lower()
            if kind != "redownload":
                continue
            res = (t.get("redownload_res") or "").strip().lower() or "best"
            name = t.get("name") or t.get("folder", "")
            url = t.get("url", "")
            if not name:
                skipped += 1
                continue
            try:
                self._queues.sync_remove(url or name)
            except Exception as e:
                _log.debug("swallowed: %s", e)
            try:
                identity = {"url": url} if url else {"name": name}
                r = self.chan_redownload(identity, res, scope=t.get("scope"))
                if isinstance(r, dict) and r.get("ok"):
                    resumed += 1
                else:
                    skipped += 1
            except Exception:
                skipped += 1
        return {"ok": True, "resumed": resumed, "skipped": skipped}


    def _run_redownload_one(self, ch, folder, new_res, scope_label):
        """Run ONE redownload to completion. Called from the chain
        worker. Previously inlined as `_run` inside `chan_redownload`;
        extracted so the worker can drain multiple queued items
        sequentially without re-spawning threads per item.
        """
        from backend import redownload as _rd
        _scope_text = f" [{scope_label}]" if scope_label else ""
        _rd_task = dict(ch)
        _rd_task["kind"] = "redownload"
        _rd_task["redownload_res"] = new_res
        try:
            self._queues.set_current_sync(_rd_task)
        except Exception as e:
            _log.debug("swallowed: %s", e)
        try:
            self._log_stream.emit([
                ["[Sync] ", "sync_bracket"],
                [f"Redownload {ch.get('name','?')}{_scope_text} \u2192 ",
                 "simpleline_green"],
                [("Best\n" if new_res == "best" else f"{new_res}p\n"),
                 "simpleline_green"],
            ])
            self._log_stream.flush()

            def _confirm(avg_pct, direction, res_label, sample_n):
                ev = threading.Event()
                # Per-job key so a concurrent sample-confirm step can't
                # overwrite this one's pending dict. Each call captures
                # its own `pending` local and reads choice from THAT —
                # never from `self._redwnl_sample` — so a second job
                # writing the single-slot attribute can't mis-resolve
                # this one (audit: redownload_mixin.py C4).
                _job_key = (ch.get("url") or ch.get("name") or "")
                pending = {
                    "avg_pct": float(avg_pct),
                    "direction": str(direction),
                    "res_label": str(res_label),
                    "sample_n": int(sample_n),
                    "event": ev,
                    # Default is now `cancel` on timeout. Old default
                    # was `continue`, so a user who walked away for
                    # 5+ minutes had the redownload silently proceed
                    # without their consent.
                    "choice": "cancel",
                    "_job_key": _job_key,
                    "_timed_out": False,
                }
                if not hasattr(self, "_redwnl_samples") or \
                        self._redwnl_samples is None:
                    self._redwnl_samples = {}
                self._redwnl_samples[_job_key] = pending
                # Legacy single-slot kept for the resolver fast-path and
                # any external introspection; resolver also walks the
                # keyed dict so multiple-pending overlaps still resolve.
                self._redwnl_sample = pending
                try:
                    import json as _json
                    _payload = _json.dumps({
                        "kind": "redownload_sample",
                        "avg_pct": float(avg_pct),
                        "direction": str(direction),
                        "res_label": str(res_label),
                        "sample_n": int(sample_n),
                    })
                    self._log_stream.emit([
                        [_payload, "__control__"],
                    ])
                    self._log_stream.flush()
                except Exception as e:
                    _log.debug("swallowed: %s", e)
                _signaled = ev.wait(timeout=300)
                if not _signaled:
                    # User never answered. Mark as timeout and treat
                    # as cancel — surface a log line so they know
                    # later that the redownload stopped, not silently
                    # progressed.
                    try:
                        pending["_timed_out"] = True
                        self._log_stream.emit_dim(
                            "[Sync] Redownload sample-confirm timed out "
                            "(5 min) — cancelling rather than proceeding.")
                        self._log_stream.flush()
                    except Exception:
                        pass
                try:
                    self._redwnl_samples.pop(_job_key, None)
                except Exception:
                    pass
                return pending.get("choice", "cancel")

            _rd.redownload_channel(
                ch.get("name", ""), ch.get("url", ""), folder, new_res,
                stream=self._log_stream,
                cancel_ev=self._redwnl_cancel,
                pause_ev=self._sync_pause,
                confirm_cb=_confirm,
                queues=self._queues,
            )
        except Exception as e:
            self._log_stream.emit_error(f"Redownload crashed: {e}")
        finally:
            try: self._queues.set_current_sync(None)
            except Exception as e: _log.debug("swallowed: %s", e)
            self._log_stream.flush()
            try:
                from backend import archive_scan as _as
                _as.invalidate_channel(ch.get("url", ""))
            except Exception as e:
                _log.debug("swallowed: %s", e)
            self._on_queue_changed()
            # Tell the frontend to re-fetch the Subs table so the
            # chartreuse `_pending_redownload` dot clears now that
            # `_redownload_progress.json` has been deleted. Without
            # this push, the Subs table stays cached with the stale
            # dot until the user manually switches tabs or triggers
            # another refresh.
            try:
                if self._window is not None:
                    self._window.evaluate_js(
                        "if (window.refreshSubsTable) "
                        "window.refreshSubsTable();")
            except Exception as e:
                _log.debug("swallowed: %s", e)


    def redownload_sample_confirm(self, choice):
        """UI → Python bridge for the "check 10 then re-ask" popup.

        Called from app.js when the user clicks Continue / Cancel / picks
        a new resolution in the sample-confirm modal. Releases the
        worker thread that's parked on `_redwnl_sample["event"]`.

        `choice`:
          - "continue" → keep going at the current resolution
          - "cancel"   → stop the redownload
          - "best" / "2160" / "1440" / "1080" / "720" / "480" / "360"
            / "240" / "144" → switch to that resolution and resample
        """
        samples = getattr(self, "_redwnl_samples", None) or {}
        pending_list = list(samples.values())
        if not pending_list:
            # Fall back to legacy single-slot in case nothing was keyed
            # (paths that haven't been migrated yet).
            legacy = getattr(self, "_redwnl_sample", None)
            if legacy:
                pending_list = [legacy]
        if not pending_list:
            return {"ok": False, "error": "no pending sample-confirm"}
        c = str(choice or "continue").strip().lower()
        if c not in ("continue", "cancel", *ALLOWED_REDOWNLOAD_RESOLUTIONS):
            return {"ok": False, "error": f"invalid choice: {c}"}
        # In normal (serial) operation there's exactly one pending. If
        # multiple ever overlap, apply the user's choice to all rather
        # than dropping any — leaving one stranded would hang the worker
        # for the full 5-minute timeout.
        for pending in pending_list:
            pending["choice"] = c
            ev = pending.get("event")
            if ev is not None:
                try: ev.set()
                except Exception as e: _log.debug("swallowed: %s", e)
        return {"ok": True, "choice": c, "resolved": len(pending_list)}


    def queue_pending_check(self):
        """Count channels that likely have new videos by comparing archive
        file cursor vs disk cache. Cheap sanity estimate — not exact."""
        cfg = load_config()
        channels = cfg.get("channels", [])
        cache = archive_scan.load_disk_cache()
        # scale the "pending" threshold to the user's
        # autorun interval. If autorun runs every 30 min and the
        # threshold is a hardcoded 2h, the badge always shows
        # non-zero even when every channel was synced recently.
        # Rule: threshold = max(autorun_interval, 2h) so the badge
        # never flags a channel as pending until at least one
        # scheduled autorun cycle has passed without it being
        # touched. Falls back to 2h if no interval is configured.
        import time as _t
        _autorun_min = 0
        try:
            _autorun_min = int(cfg.get("autorun_interval_mins") or 0)
        except (TypeError, ValueError):
            _autorun_min = 0
        _interval_secs = max(_autorun_min * 60, 2 * 3600)
        threshold = _t.time() - _interval_secs
        n_pending = 0
        for ch in channels:
            rec = cache.get(ch.get("url", ""))
            if not rec or rec.get("last_updated", 0) < threshold:
                n_pending += 1
        return {"ok": True, "count": n_pending, "total": len(channels)}
