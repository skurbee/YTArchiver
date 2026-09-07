"""Startup workflow assembled from explicit application dependencies."""

from __future__ import annotations

import os
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from backend.archive_capacity import archive_capacity_status
from backend.log import get_logger
from backend.services.config_repository import ConfigRepository
from backend.services.event_bus import BridgeEventBus
from backend.services.ports import LogSink
from backend.services.startup_scan import StartupDiskScan
from backend.services.startup_stages import run_startup_stages
from backend.version import APP_VERSION

_log = get_logger(__name__)


class StartupCatalog(Protocol):
    def is_foreground_browse_busy(self) -> bool: ...
    def sweep_new_videos(self, output_dir: str, channels: list, **kwargs) -> dict: ...


@dataclass(frozen=True)
class StartupDependencies:
    config: ConfigRepository
    log_stream: LogSink
    events: BridgeEventBus
    catalog: StartupCatalog
    window: Callable[[], Any]
    restore_pending: Callable[[], int]
    publish_config: Callable[[dict], None]
    queue_changed: Callable[[], None]
    sync_running: Callable[[], bool]
    manual_running: Callable[[], bool]
    processing_busy: Callable[[], bool]
    autorun_ready: Callable[[str], None]
    trash_ready: Callable[[str], None]


@dataclass
class StartupSequence:
    dependencies: StartupDependencies

    def run(self, cancel_event=None):
        """Three-stage startup log matching YTArchiver's OLD timing:

            Stage 1 (< 2s) --- Startup checks complete, ready to download ---
                              → Sync Subbed + related buttons enable here
            Stage 2 (20-40s) --- Disk scan complete (N ch \u00b7 M vids \u00b7 X TB) ---
                              → staleness-gated: if cache is newer than
                                `disk_scan_staleness_hours`, skip the walk
                                and just report from the cache (instant)
            Stage 3 (background) --- newly-added files swept into the index

        The stages run sequentially under one supervised startup owner and
        emit each milestone as it finishes. A small joined indicator helper
        animates the "Loading\u00b7" line in verbose mode; simple mode sees
        only the three green milestones (the tick is VERBOSE_ONLY).
        """
        cancel_event = cancel_event or threading.Event()
        s = self.dependencies.log_stream

        # App-started banner FIRST — must precede the "Startup checks
        # complete" milestone below. (Moved here from set_window, whose log
        # fired after this buffered output flushed, so "checks complete"
        # appeared above "started" and made the checks look instantaneous.)
        try:
            s.emit_text(f"YTArchiver {APP_VERSION} started", None)
        except Exception as e:
            _log.debug("swallowed: %s", e)

        def _flush_now():
            try:
                s.flush()
            except Exception as e:
                _log.debug("swallowed: %s", e)

        def _loading(msg):
            # In-place status line (replace-in-place via `startup_loading`).
            # Filtered from simple mode — user sees only the green milestones.
            try:
                s.emit([[f" {msg}\n", "startup_loading"]])
                _flush_now()
            except Exception as e:
                _log.debug("swallowed: %s", e)

        _loading("Loading\u00b7 ")

        # Pending-transcribe journal restore (fast, runs before any milestone).
        try:
            n = self.dependencies.restore_pending()
            if n > 0:
                s.emit_text(
                    f" \u2014 Restored {n} pending transcription job(s) from last session.",
                    "simpleline_blue",
                )
                _flush_now()
        except Exception as _pe:
            s.emit_dim(f" (pending-journal restore skipped: {_pe})")
            _flush_now()

        cfg = self.dependencies.config.load()
        # Startup status for low-priority background indexing.
        dots_state = {
            "i": 0,
            "sweep": {"phase": "Starting up", "detail": ""},
        }
        stage3_done = threading.Event()

        def _push_indicator(slot, text):
            """Push startup status text, or `None`/`""` to hide it.
            Visible in Simple + Verbose.

            Use json.dumps to encode the text argument. A manual replace-chain
            escaped
            `\\` and `'` only — a literal newline / carriage return
            inside a channel folder name would produce broken JS
            (unescaped newline inside a quoted string is a
            SyntaxError) which evaluate_js then silently swallowed
            into the outer except below, leaving the indicator
            stuck on its last value for the affected tick.
            """
            import json as _json

            try:
                w = self.dependencies.window()
                if w is None:
                    return
                if text:
                    safe = _json.dumps(text)  # returns a fully-quoted JS string literal
                    w.evaluate_js(
                        f"window._setIndicator && window._setIndicator({_json.dumps(slot)}, {safe})"
                    )
                else:
                    w.evaluate_js(
                        f"window._setIndicator && window._setIndicator({_json.dumps(slot)}, null)"
                    )
            except Exception as e:
                _log.debug("swallowed: %s", e)

        def _animate_dots():
            """Cycle dots on each active status slot. When a
            slot's `phase` is empty, its UI indicator is hidden; when
            populated, we emit `{phase}{dots} {detail}`."""
            while not stage3_done.is_set() and not cancel_event.is_set():
                dots_state["i"] = (dots_state["i"] + 1) % 3
                d = ["\u00b7 ", "\u00b7\u00b7 ", "\u00b7\u00b7\u00b7"][dots_state["i"]]
                log_parts = []
                for slot in ("sweep",):
                    state = dots_state[slot]
                    phase = state.get("phase") or ""
                    detail = state.get("detail") or ""
                    if phase:
                        line = f"{phase}{d} {detail}" if detail else f"{phase}{d}"
                        _push_indicator(slot, line.strip())
                        log_parts.append(line.strip())
                    else:
                        _push_indicator(slot, None)
                # Log mirror for the verbose startup "Loading" line.
                if log_parts:
                    _loading(" \u00b7 ".join(log_parts))
                cancel_event.wait(0.4)
            # NOTE: post-stage-3 indicator state is handled by the
            # caller after stage3_done.set(). The animator deliberately
            # doesn't touch the slot on exit so it can't race-overwrite
            # cleanup.

        animator = threading.Thread(target=_animate_dots, daemon=True, name="startup-indicator")
        animator.start()

        def _clear_loading():
            """Remove the in-place Loading line from the DOM."""
            try:
                w = self.dependencies.window()
                if w is not None:
                    w.evaluate_js("window.clearStartupLine && window.clearStartupLine()")
            except Exception as e:
                _log.debug("swallowed: %s", e)

        def _fire_ready_js():
            """Tell the UI to un-gray the Sync Subbed / Sync Tasks buttons."""
            try:
                w = self.dependencies.window()
                if w is not None:
                    w.evaluate_js("window._setReady && window._setReady(true)")
            except Exception as e:
                _log.debug("swallowed: %s", e)

        # ── Stage 1: Startup checks (immediate — < 2s) ─────────────────
        # No heavy I/O at this stage. Emit the green milestone right away
        # so the user sees the app responded, and flip the Sync buttons
        # active so they can kick off a sync without waiting for background
        # indexing.
        try:
            s.emit_text("--- Startup checks complete, ready to download ---", "simpleline_green")
            _flush_now()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        _fire_ready_js()
        # Paint the restored queue NOW. Items restored from last session
        # in Api.__init__ were loaded before the window existed, so their
        # listener pushes were silently dropped (self.dependencies.window() is None in
        # _on_queue_changed). Without this explicit repaint, restored
        # Sync/GPU tasks stayed invisible until the next incidental queue
        # mutation — after a cold reboot that could be the end of the
        # disk scan, ~45s later, which read as "my queue is gone."
        try:
            self.dependencies.queue_changed()
        except Exception as e:
            _log.debug("swallowed: %s", e)

        # Every startup archive walk is lowest-priority work. This predicate
        # stays true for an entire sync worker/pass, allowing a scan that
        # began before the user pressed Sync/Resume to stop cooperatively
        # instead of monopolizing pooled-storage metadata I/O underneath it.
        def _startup_low_priority_busy():
            try:
                if self.dependencies.catalog.is_foreground_browse_busy():
                    return True
            except Exception:
                pass
            try:
                if self.dependencies.sync_running():
                    return True
            except Exception:
                pass
            try:
                if self.dependencies.manual_running():
                    return True
            except Exception:
                pass
            try:
                if self.dependencies.processing_busy():
                    return True
            except Exception:
                pass
            try:
                from backend.sync.active_state import is_sync_work_active

                return bool(is_sync_work_active())
            except Exception:
                return False

        # ── Stage 2: Disk walk (staleness-gated) ───────────────────────
        disk_scan = StartupDiskScan(
            self.dependencies.config,
            s,
            self.dependencies.events,
            self.dependencies.publish_config,
            dots_state,
        )

        def _start_subscriber_backfill():
            """Recover missing card counts without delaying app readiness."""

            def _run():
                if cancel_event.is_set():
                    return
                try:
                    from backend.subscriber_counts import (
                        backfill_missing_counts,
                    )

                    current_cfg = self.dependencies.config.load()
                    result = backfill_missing_counts(list(current_cfg.get("channels", []) or []))
                    updated = int(result.get("updated") or 0)
                    failed = int(result.get("failed") or 0)
                    excluded = int(result.get("excluded") or 0)
                    deferred = int(result.get("deferred") or 0)
                    if updated and self.dependencies.window() is not None:
                        # refreshSubsTable fans into _primeBrowse, so visible
                        # channel cards pick up the new counts immediately.
                        self.dependencies.window().evaluate_js(
                            "window.refreshSubsTable && window.refreshSubsTable();"
                        )
                    if updated or failed or excluded or deferred:
                        summary = f" Subscriber counts: {updated} recovered"
                        if failed:
                            summary += f", {failed} still unavailable"
                        if excluded:
                            summary += f", {excluded} excluded after 3 attempts"
                        if deferred:
                            summary += f", {deferred} deferred"
                        summary += "."
                        s.emit_dim(summary)
                        _flush_now()
                except Exception as exc:
                    _log.debug("subscriber-count launch backfill failed: %s", exc)

            _run()

        # Stage 3: low-priority background sweep.
        def _stage3_sweep():
            """Run the archive sweep after disk state is known."""
            if cancel_event.is_set():
                return
            output_dir = (cfg.get("output_dir") or "").strip()
            sweep_result = {"registered": 0, "ingested": 0}

            def _run_sweep():
                if not output_dir or cancel_event.is_set():
                    return
                sweep_progress = {"detail": ""}

                def _on_sweep(idx, total, name):
                    clean = (name or "")[:32]
                    sweep_progress["detail"] = f"{idx}/{total} \u2014 {clean}"
                    dots_state["sweep"]["phase"] = "Indexing new files"
                    dots_state["sweep"]["detail"] = sweep_progress["detail"]

                def _sweep_busy():
                    busy = _startup_low_priority_busy()
                    if busy:
                        # The old label said "Indexing new files" for up to an
                        # hour while the sweep was intentionally yielding to a
                        # sync/GPU/Browse task. Tell the truth about the wait.
                        dots_state["sweep"]["phase"] = "Index scan waiting for active work"
                        dots_state["sweep"]["detail"] = ""
                    elif dots_state["sweep"].get("phase") == ("Index scan waiting for active work"):
                        dots_state["sweep"]["phase"] = "Indexing new files"
                        dots_state["sweep"]["detail"] = sweep_progress["detail"]
                    return busy

                if not _sweep_busy():
                    dots_state["sweep"]["phase"] = "Indexing new files"
                    dots_state["sweep"]["detail"] = ""
                try:
                    # Pass the low-priority gate to sweep so it yields
                    # between channels while sync/GPU work is active.
                    r = self.dependencies.catalog.sweep_new_videos(
                        output_dir,
                        cfg.get("channels", []),
                        progress_cb=_on_sweep,
                        gpu_busy_fn=_sweep_busy,
                        extra_roots=list(cfg.get("tp_archive_roots") or []),
                    )
                    if cancel_event.is_set():
                        return
                    sweep_result["registered"] = int(r.get("registered") or 0)
                    sweep_result["ingested"] = int(r.get("ingested") or 0)
                    sweep_result["skipped_unchanged"] = int(r.get("skipped_unchanged") or 0)
                    sweep_result["walked"] = int(r.get("walked") or 0)
                except Exception as _se:
                    s.emit_error(f"Sweep failed: {_se}")
                    _flush_now()
                finally:
                    # Clear the sweep slot when indexing is done.
                    dots_state["sweep"]["phase"] = ""
                    dots_state["sweep"]["detail"] = ""

            # This stage already runs on the supervised startup worker. Keep
            # the sweep inline so shutdown/restore owns the real writer rather
            # than a nested daemon that could outlive its parent.
            _run_sweep()
            if cancel_event.is_set():
                return

            sweep_reg = sweep_result["registered"]
            sweep_ing = sweep_result["ingested"]
            sweep_skip = sweep_result.get("skipped_unchanged", 0)
            sweep_walked = sweep_result.get("walked", 0)
            if sweep_reg > 0 or sweep_ing > 0:
                s.emit_text(
                    f" \u2014 Background sweep: +{sweep_reg} new videos registered, "
                    f"+{sweep_ing} jsonl ingested.",
                    "simpleline_blue",
                )
                _flush_now()
            if sweep_skip:
                s.emit_dim(
                    f" Sweep: {sweep_skip} channel(s) skipped (folder "
                    f"unchanged since last sweep), {sweep_walked} walked."
                )
                _flush_now()

            # Storage-pressure warning stays at the tail.
            try:
                cfg2 = self.dependencies.config.load()
                od = (cfg2.get("output_dir") or "").strip()
                if od:
                    probe = od if os.path.isdir(od) else os.path.dirname(od) or "."
                    cap = archive_capacity_status(probe, cfg2)
                    if cap.get("status") == "warning":
                        detail = cap.get("detail") or "Archive drive is over its warning threshold"
                        s.emit(
                            [
                                ["\u26a0 ", "red"],
                                [f"Archive drive warning: {detail}. New syncs may fail.\n", "red"],
                            ]
                        )
                        _flush_now()
            except Exception as e:
                _log.debug("swallowed: %s", e)

        # Sequential stages on one background thread — each milestone
        # fires the moment its stage finishes.
        run_startup_stages(
            cancel_event=cancel_event,
            finished=stage3_done,
            disk_scan=lambda: disk_scan.run(cancel_event, cfg, _startup_low_priority_busy),
            sweep=_stage3_sweep,
            backfill=_start_subscriber_backfill,
            clear_loading=_clear_loading,
            clear_indicator=lambda: _push_indicator("sweep", None),
            ready_callbacks=(self.dependencies.autorun_ready, self.dependencies.trash_ready),
        )
        try:
            if animator is not threading.current_thread():
                animator.join(timeout=1.0)
        except (RuntimeError, TypeError):
            pass
