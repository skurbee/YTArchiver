"""
RedownloadMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They prefer AppServices when present, with legacy
private Api attributes kept as fallback state.
"""
from __future__ import annotations

import json
import threading
import time
import uuid

from backend.ytarchiver_config import load_config

from ._shared import ALLOWED_REDOWNLOAD_RESOLUTIONS, _log

_SAMPLE_CONFIRM_TIMEOUT_SEC = 300


class RedownloadMixin:
    def _redownload_services(self):
        return getattr(self, "services", None)

    def _redownload_queues(self):
        services = self._redownload_services()
        q = getattr(services, "queues", None) if services is not None else None
        return q if q is not None else self._queues

    def _redownload_log_stream(self):
        services = self._redownload_services()
        stream = (getattr(services, "log_stream", None)
                  if services is not None else None)
        return stream if stream is not None else self._log_stream

    def _redownload_config(self):
        services = self._redownload_services()
        if services is not None:
            return services.fresh_config()
        return load_config()

    def resume_pending_redownloads(self):
        """Issue #162: scan queues.sync for redownload-kind tasks (left
        there by a previous run that exited before draining them) and
        re-route each through chan_redownload so the chain worker
        spawns and resumes from `_redownload_progress.json`. Without
        this, clicking the popover Resume button after a restart fired
        `sync_start_all` and started a regular Sync Subbed pass —
        the redownload state was never picked up.

        Returns counts for restored redownload and ordinary pending tasks.
        """
        resumed = 0
        skipped = 0
        queues = self._redownload_queues()
        try:
            # Snapshot the queue so we can iterate without mutation races.
            tasks_snapshot = queues.sync_snapshot()
            if not isinstance(tasks_snapshot, list):
                raise TypeError("sync snapshot is not a list")
        except Exception:
            legacy_tasks = getattr(queues, "sync", [])
            tasks_snapshot = (list(legacy_tasks)
                              if isinstance(legacy_tasks, list) else [])
        regular_pending = sum(
            1 for task in tasks_snapshot
            if str(task.get("kind") or "download").lower() != "redownload"
        )
        redownload_total = len(tasks_snapshot) - regular_pending

        # Keep every durable row in place while chan_redownload establishes
        # the exact in-memory chain reservation.  The worker promotes that row
        # to the crash-resumable current slot immediately before execution.
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
                video_id = str(t.get("only_video_id") or "").strip()
                if video_id:
                    channel_url = str(t.get("channel_url") or "").strip()
                    channel_name = str(t.get("channel_name") or "").strip()
                    identity = ({"url": channel_url} if channel_url
                                else {"name": channel_name})
                    r = self.chan_redownload(
                        identity, res, scope=t.get("scope"),
                        task_id=t.get("task_id", ""),
                        only_video={
                            "video_id": video_id,
                            "filepath": t.get("only_filepath") or "",
                            "title": t.get("only_title") or name,
                        },
                        _queue_only=bool(regular_pending))
                else:
                    identity = {"url": url} if url else {"name": name}
                    r = self.chan_redownload(
                        identity, res, scope=t.get("scope"),
                        task_id=t.get("task_id", ""),
                        _queue_only=bool(regular_pending))
                if isinstance(r, dict) and r.get("ok"):
                    resumed += 1
                else:
                    skipped += 1
            except Exception:
                skipped += 1
        result = {"ok": bool(resumed or not redownload_total),
                  "resumed": resumed, "skipped": skipped,
                  "regular_pending": regular_pending}
        if redownload_total and not resumed:
            result["error"] = "Saved redownload tasks could not be resumed"
        return result


    def _run_redownload_one(self, ch, folder, new_res, scope_label,
                            only_video=None, rd_task=None):
        """Run ONE redownload to completion. Called from the chain
        worker. Previously inlined as `_run` inside `chan_redownload`;
        extracted so the worker can drain multiple queued items
        sequentially without re-spawning threads per item.
        """
        from backend import redownload as _rd
        queues = self._redownload_queues()
        log_stream = self._redownload_log_stream()
        cancel_event = self._redwnl_cancel
        _scope_text = f" [{scope_label}]" if scope_label else ""
        _rd_task = dict(rd_task) if isinstance(rd_task, dict) else dict(ch)
        _rd_task["kind"] = "redownload"
        _rd_task["redownload_res"] = new_res
        only_video = only_video if isinstance(only_video, dict) else {}
        if only_video:
            video_id = str(only_video.get("video_id") or "").strip()
            video_title = (str(only_video.get("title") or "").strip()
                           or video_id)
            _rd_task.update({
                "name": video_title,
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "channel_name": ch.get("name") or ch.get("folder") or "",
                "channel_url": ch.get("url") or "",
                "only_video_id": video_id,
                "only_filepath": only_video.get("filepath") or "",
                "only_title": video_title,
            })
        try:
            _rd_task_id = str(_rd_task.get("task_id") or "").strip()
            durable_replace = getattr(
                type(queues), "replace_current_task_durable", None)
            published = bool(
                not cancel_event.is_set() and _rd_task_id and callable(durable_replace)
                and queues.replace_current_task_durable(
                    "sync", _rd_task, expected_task_id=_rd_task_id)
            )
            if not published:
                # Promotion already created the running slot. A failed CAS
                # means Clear/Skip won or persistence failed; never resurrect
                # that exact task with the non-transactional setter.
                _log.debug(
                    "redownload current-task decoration was not published "
                    "for %s", _rd_task_id or "missing task id")
        except Exception as e:
            _log.warning(
                "redownload: current-task decoration failed safely: %s", e)
        try:
            if cancel_event.is_set():
                return
            log_stream.emit([
                ["[Sync] ", "sync_bracket"],
                [f"Redownload {_rd_task.get('name','?')}{_scope_text} \u2192 ",
                 "simpleline_green"],
                [("Best\n" if new_res == "best" else f"{new_res}p\n"),
                 "simpleline_green"],
            ])
            log_stream.flush()

            def _confirm(avg_pct, direction, res_label, sample_n):
                return self._wait_redownload_sample(
                    avg_pct, direction, res_label, sample_n,
                    cancel_event, log_stream)

            _rd.redownload_channel(
                ch.get("name", ""), ch.get("url", ""), folder, new_res,
                stream=log_stream,
                cancel_ev=cancel_event,
                pause_ev=self._sync_pause,
                confirm_cb=_confirm,
                queues=queues,
                only_video_id=_rd_task.get("only_video_id", ""),
                only_filepath=_rd_task.get("only_filepath", ""),
                only_title=_rd_task.get("only_title", ""),
                scope=_rd_task.get("scope"),
            )
        except Exception as e:
            log_stream.emit_error(f"Redownload crashed: {e}")
        finally:
            cleared = False
            try:
                current = queues.current_sync
                if current is None:
                    # An exact per-channel cancel durably clears the recovery
                    # slot before signalling this worker.
                    cleared = True
                else:
                    cleared = queues.replace_current_task_durable(
                        "sync", None,
                        expected_task_id=str(
                            _rd_task.get("task_id") or "").strip(),
                    )
                if not cleared:
                    _log.warning(
                        "redownload completion could not durably clear its "
                        "recovery slot; task remains recoverable")
            except Exception as e:
                _log.warning(
                    "redownload finally: durable current clear failed: %s", e)
            if cleared and cancel_event.is_set():
                # Defer persists the same task at the durable queue's tail.
                # Reattach its execution companion only after this worker has
                # stopped and acknowledged its current slot. A normal Cancel
                # has no matching pending row, and Stop must not restart work.
                try:
                    with self._redwnl_lock:
                        stopped = getattr(self, "_sync_cancel", None)
                        task_id = str(_rd_task.get("task_id") or "").strip()
                        deferred = None
                        if task_id and not (stopped and stopped.is_set()):
                            deferred = next((dict(task) for task in queues.sync_snapshot()
                                             if str(task.get("task_id") or "").strip() == task_id
                                             and task.get("kind") == "redownload"), None)
                        if deferred is not None and not any(
                                str((item.get("rd_task") or {}).get("task_id") or "").strip() == task_id
                                for item in self._redwnl_pending):
                            deferred.pop("cancel_requested", None)
                            self._redwnl_pending.append({
                                "ch": dict(ch), "folder": folder,
                                "new_res": deferred.get("redownload_res") or new_res,
                                "scope_label": scope_label, "scope": deferred.get("scope"),
                                "only_video": dict(only_video), "rd_task": deferred,
                            })
                except Exception as exc:
                    _log.warning("deferred redownload runtime restoration failed: %s", exc)
            log_stream.flush()
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


    def _sample_state(self):
        # The redownload lane is serial; this also serializes bridge answers
        # with the worker's deadline and cleanup.
        if not hasattr(self, "_redwnl_samples_lock"):
            self._redwnl_samples_lock = threading.Lock()
        if not isinstance(getattr(self, "_redwnl_samples", None), dict):
            self._redwnl_samples = {}
        return self._redwnl_samples_lock, self._redwnl_samples

    def _wait_redownload_sample(self, avg_pct, direction, res_label, sample_n,
                               cancel_event, stream):
        sample_id = uuid.uuid4().hex
        pending = {"event": threading.Event(), "choice": "cancel",
                   "answered": False, "sample_id": sample_id,
                   "deadline_ts": time.time() + _SAMPLE_CONFIRM_TIMEOUT_SEC}
        lock, samples = self._sample_state()
        with lock:
            samples[sample_id] = pending
            self._redwnl_sample = pending

        def emit(kind, **fields):
            stream.emit([[json.dumps({"kind": kind, "sample_id": sample_id,
                                     **fields}), "__control__"]])
            stream.flush()

        reason = "timeout"
        try:
            emit("redownload_sample", avg_pct=float(avg_pct),
                 direction=str(direction), res_label=str(res_label),
                 sample_n=int(sample_n), deadline_ts=pending["deadline_ts"])
            while True:
                with lock:
                    if cancel_event.is_set():
                        pending["choice"] = "cancel"
                        reason = "cancelled"
                        break
                    if pending["answered"]:
                        reason = "answered"
                        break
                    remaining = pending["deadline_ts"] - time.time()
                    if remaining <= 0:
                        break
                pending["event"].wait(min(remaining, 0.2))
        except Exception as exc:
            _log.warning("redownload sample dialog failed: %s", exc)
            reason = "cancelled"
            pending["choice"] = "cancel"
        finally:
            with lock:
                samples.pop(sample_id, None)
                if getattr(self, "_redwnl_sample", None) is pending:
                    self._redwnl_sample = None
            try:
                emit("redownload_sample_closed", reason=reason)
                if reason == "timeout":
                    stream.emit_dim("[Sync] Sample confirmation timed out — redownload cancelled.")
                    stream.flush()
            except Exception as exc:
                _log.debug("redownload sample close notification failed: %s", exc)
        return pending["choice"]

    def redownload_sample_confirm(self, choice, sample_id=None):
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
        c = str(choice or "").strip().lower()
        if c not in ("continue", "cancel", *ALLOWED_REDOWNLOAD_RESOLUTIONS):
            return {"ok": False, "error": f"invalid choice: {c}"}
        lock, samples = self._sample_state()
        with lock:
            pending = samples.get(str(sample_id or ""))
            if not sample_id and len(samples) == 1:
                pending = next(iter(samples.values()))
            if not pending or pending["deadline_ts"] <= time.time():
                return {"ok": False, "expired": True,
                        "error": "This sample confirmation has expired or closed."}
            if pending["answered"]:
                return {"ok": False, "error": "This sample already has an answer."}
            pending["choice"] = c
            pending["answered"] = True
            pending["event"].set()
            return {"ok": True, "choice": c, "resolved": 1,
                    "sample_id": pending["sample_id"]}
