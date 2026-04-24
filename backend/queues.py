"""
Queue state — persistent multi-queue manager.

Matches YTArchiver's ytarchiver_queue.json schema (YTArchiver.py:34016):
    {
      "sync": [channel_dict, ...],
      "reorg": [[args], ...],
      "video": [],
      "transcribe": [[args], ...],
      "redownload": [dict, ...],
      "metadata": [dict, ...],
      "gpu": [dict, ...],
      "order": [["kind", "id"], ...],
      "gpu_paused": false,
      "sync_paused": false
    }

Single source of truth. Persists debounced 2s after changes.
Gated by config_is_writable() (same gate as config writes).
"""

from __future__ import annotations

import copy
import json
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from .ytarchiver_config import QUEUE_FILE, config_is_writable


class QueueState:
    """Central queue manager. Thread-safe."""

    def __init__(self):
        self._lock = threading.RLock()
        self.sync: List[Dict[str, Any]] = []
        self.reorg: List[list] = []
        self.video: List[list] = []
        self.transcribe: List[list] = []
        self.redownload: List[Dict[str, Any]] = []
        self.metadata: List[Dict[str, Any]] = []
        self.gpu: List[Dict[str, Any]] = []
        self.order: List[list] = [] # [[kind, id], ...]
        self.gpu_paused: bool = False
        self.sync_paused: bool = False

        # Current in-flight items (not yet re-queued, but shown in popover)
        self.current_sync: Optional[Dict[str, Any]] = None
        self.current_gpu: Optional[Dict[str, Any]] = None
        self.current_redownload: Optional[Dict[str, Any]] = None
        self.current_metadata: Optional[Dict[str, Any]] = None

        # Sync-pass progress: when "Sync Subbed" runs, we don't enqueue 103
        # individual channel items into `self.sync` — we iterate them
        # inline in `sync_start_all`. But the popover shouldn't look like
        # a single-item queue; the user should see "Downloading ChannelName
        # (17/103)" so they know how far along the pass is. These two
        # fields are set / cleared by sync_start_all.
        self.sync_pass_index: int = 0
        self.sync_pass_total: int = 0

        # Debounced save scheduler
        self._save_timer: Optional[threading.Timer] = None
        self._save_interval_sec = 2.0

        # Listeners notified on any state change (UI push)
        self._listeners: List[Callable[[], None]] = []

        # audit D-35: register atexit hook so a crash/kill within the
        # 2s debounce window still flushes. Idempotent — atexit only
        # fires once per process, and _atexit_flush is a no-op when
        # nothing is pending.
        try:
            import atexit as _atx
            _atx.register(self._atexit_flush)
        except Exception:
            pass

    # ── listener registration ───────────────────────────────────────

    def add_listener(self, fn: Callable[[], None]):
        self._listeners.append(fn)

    def _notify(self):
        for fn in list(self._listeners):
            try:
                fn()
            except Exception:
                pass

    # ── load/save ────────────────────────────────────────────────────

    def load(self) -> bool:
        """Load queue state from ytarchiver_queue.json. Returns True on success.

        Mirrors OLD YTArchiver.py:34103 _load_queue_state: if the JSON is
        corrupt, rename the file to .bak so next launch starts fresh instead
        of soft-locking on the same parse error every time.
        """
        if not QUEUE_FILE.exists():
            return False
        try:
            with QUEUE_FILE.open("r", encoding="utf-8") as f:
                raw = f.read()
        except OSError:
            return False
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            # Corrupt queue file — rename to .bak so recovery is possible
            # and next launch isn't stuck on the same error forever.
            try:
                bak = str(QUEUE_FILE) + ".bak"
                os.replace(str(QUEUE_FILE), bak)
            except OSError:
                try: os.remove(str(QUEUE_FILE))
                except OSError: pass
            return False
        with self._lock:
            self.sync = list(data.get("sync", []))
            self.reorg = list(data.get("reorg", []))
            self.video = list(data.get("video", []))
            self.transcribe = list(data.get("transcribe", []))
            self.redownload = list(data.get("redownload", []))
            self.metadata = list(data.get("metadata", []))
            self.gpu = list(data.get("gpu", []))
            self.order = list(data.get("order", []))
            self.gpu_paused = bool(data.get("gpu_paused", False))
            self.sync_paused = bool(data.get("sync_paused", False))
        self._notify()
        return True

    def save_now(self) -> bool:
        """Serialize + atomically replace QUEUE_FILE. Gated by env var."""
        if not config_is_writable():
            return False
        with self._lock:
            payload = {
                "sync": copy.deepcopy(self.sync),
                "reorg": copy.deepcopy(self.reorg),
                "video": copy.deepcopy(self.video),
                "transcribe": copy.deepcopy(self.transcribe),
                "redownload": copy.deepcopy(self.redownload),
                "metadata": copy.deepcopy(self.metadata),
                "gpu": copy.deepcopy(self.gpu),
                "order": copy.deepcopy(self.order),
                "gpu_paused": self.gpu_paused,
                "sync_paused": self.sync_paused,
            }
            # audit C-11: in-flight items now persist in a separate
            # `resuming` dict instead of being inserted at the front of
            # the regular queue lists. Old behavior re-popped the same
            # item on next boot and treated it as a normal queued job,
            # which silently re-processed it (partial downloads got
            # re-run, retranscribes silently reverted to regular
            # transcribes). Putting them in `resuming` means load()
            # can emit a visible "restart notice" and requeue them in
            # a controlled way rather than letting them race the fresh
            # boot state.
            resuming: Dict[str, Any] = {}
            if self.current_sync is not None:
                resuming["sync"] = copy.deepcopy(self.current_sync)
            if self.current_redownload is not None:
                resuming["redownload"] = copy.deepcopy(self.current_redownload)
            if self.current_metadata is not None:
                resuming["metadata"] = copy.deepcopy(self.current_metadata)
            if self.current_gpu is not None:
                resuming["gpu"] = copy.deepcopy(self.current_gpu)
            if resuming:
                payload["resuming"] = resuming
            # Also keep the legacy-style front-insertion so existing
            # load() code paths (and any third-party tools reading the
            # file) see in-flight items in the queue. The new
            # `resuming` key is additive; load() uses it when present.
            if self.current_sync is not None:
                payload["sync"].insert(0, copy.deepcopy(self.current_sync))
            if self.current_redownload is not None:
                payload["redownload"].insert(0, copy.deepcopy(self.current_redownload))
            if self.current_metadata is not None:
                payload["metadata"].insert(0, copy.deepcopy(self.current_metadata))
            if self.current_gpu is not None:
                payload["gpu"].insert(0, copy.deepcopy(self.current_gpu))
        try:
            tmp = str(QUEUE_FILE) + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            os.replace(tmp, QUEUE_FILE)
            return True
        except OSError:
            return False

    def save_debounced(self):
        """Schedule a save for _save_interval_sec from now (coalesces bursts).

        audit D-35: an atexit hook is registered at construction
        (see __init__) so that if the app is killed or crashes during
        the 2-second debounce window, pending changes still flush to
        disk. Without this, close-during-queue-edit silently lost the
        latest enqueue/remove.
        """
        with self._lock:
            if self._save_timer is not None:
                return
            t = threading.Timer(self._save_interval_sec, self._do_debounced_save)
            t.daemon = True
            t.start()
            self._save_timer = t

    def _do_debounced_save(self):
        with self._lock:
            self._save_timer = None
        self.save_now()

    def _atexit_flush(self):
        """atexit hook — cancel any pending debounce timer and force a
        synchronous save. No-op if nothing is pending. Called once per
        process at interpreter shutdown."""
        try:
            with self._lock:
                t = self._save_timer
                self._save_timer = None
            if t is not None:
                try: t.cancel()
                except Exception: pass
            self.save_now()
        except Exception:
            pass

    # ── sync queue ──────────────────────────────────────────────────

    def sync_enqueue(self, channel: Dict[str, Any]) -> bool:
        """Add a channel to the sync queue if not already present.
        Dedupe is keyed on (kind, url) so a "Download X" and a
        separate "Metadata check X" can coexist — they're different
        units of work even though they target the same channel.
        """
        url = channel.get("url", "")
        kind = (channel.get("kind") or "download").lower()
        with self._lock:
            for c in self.sync:
                if (c.get("url") == url and
                        (c.get("kind") or "download").lower() == kind):
                    return False
            self.sync.append(copy.deepcopy(channel))
            self.order.append(["sync", url])
        self._notify()
        self.save_debounced()
        return True

    def sync_pop(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self.sync:
                return None
            ch = self.sync.pop(0)
            # audit E-15: remove only the FIRST matching order entry
            # (not all of them). Same URL can legitimately have multiple
            # sync jobs queued (e.g. a Download task and a separate
            # Metadata-recheck task — `sync_enqueue` dedupes on
            # (kind, url), not url alone). Wiping all order entries for
            # that URL dropped the bookkeeping for the OTHER pending
            # job, which then dispatched out of insertion order.
            _u = ch.get("url")
            for _i, _o in enumerate(self.order):
                if _o and _o[0] == "sync" and _o[1] == _u:
                    self.order.pop(_i)
                    break
        self._notify()
        self.save_debounced()
        return ch

    def sync_remove(self, url: str) -> bool:
        with self._lock:
            before = len(self.sync)
            self.sync = [c for c in self.sync if c.get("url") != url]
            self.order = [o for o in self.order
                          if not (o and o[0] == "sync" and o[1] == url)]
            changed = len(self.sync) != before
        if changed:
            self._notify()
            self.save_debounced()
        return changed

    def sync_clear(self) -> int:
        """Remove every queued sync task; keep the currently-running one.
        Returns the number of queued items removed."""
        with self._lock:
            removed = len(self.sync)
            self.sync = []
            self.order = [o for o in self.order if not (o and o[0] == "sync")]
        if removed:
            self._notify()
            self.save_debounced()
        return removed

    def gpu_clear(self) -> int:
        """Remove every queued GPU task; keep the currently-running one."""
        with self._lock:
            removed = len(self.gpu)
            self.gpu = []
            self.order = [o for o in self.order if not (o and o[0] == "gpu")]
        if removed:
            self._notify()
            self.save_debounced()
        return removed

    def sync_reorder(self, url: str, new_index: int) -> bool:
        with self._lock:
            idx = next((i for i, c in enumerate(self.sync) if c.get("url") == url), -1)
            if idx < 0 or new_index < 0 or new_index >= len(self.sync):
                return False
            item = self.sync.pop(idx)
            self.sync.insert(new_index, item)
        self._notify()
        self.save_debounced()
        return True

    # ── gpu queue ───────────────────────────────────────────────────

    def gpu_enqueue(self, item: Dict[str, Any]) -> bool:
        """Queue a transcription/encode job for the GPU lane. Dedupes
        by `path` to prevent double-entries on startup when both
        QueueState.load() (which restores gpu from disk) and the
        transcribe pending-journal recovery might try to add the same
        item. Returns True if the item was added, False if a duplicate
        was already present."""
        path = (item.get("path") or "").strip()
        with self._lock:
            if path:
                for existing in self.gpu:
                    if (existing.get("path") or "").strip() == path:
                        return False
            self.gpu.append(copy.deepcopy(item))
        self._notify()
        self.save_debounced()
        return True

    def gpu_pop(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self.gpu:
                return None
            it = self.gpu.pop(0)
        self._notify()
        self.save_debounced()
        return it

    def gpu_remove(self, task_id: str) -> bool:
        with self._lock:
            before = len(self.gpu)
            self.gpu = [i for i in self.gpu
                        if (i.get("id") or i.get("path")) != task_id]
            changed = len(self.gpu) != before
        if changed:
            self._notify()
            self.save_debounced()
        return changed

    def gpu_remove_bulk(self, bulk_id: str) -> int:
        """Remove every GPU queue item sharing a `bulk_id`. Returns the
        number dropped. Used when the coalesced "Transcribe {ch} (N
        videos)" row is removed from the context menu — one click should
        drop all N videos, not just the top one."""
        if not bulk_id:
            return 0
        with self._lock:
            before = len(self.gpu)
            self.gpu = [i for i in self.gpu
                        if str(i.get("bulk_id") or "") != bulk_id]
            dropped = before - len(self.gpu)
        if dropped:
            self._notify()
            self.save_debounced()
        return dropped

    def gpu_reorder(self, task_id: str, new_index: int) -> bool:
        with self._lock:
            idx = next((i for i, t in enumerate(self.gpu)
                        if (t.get("id") or t.get("path")) == task_id), -1)
            if idx < 0 or new_index < 0 or new_index >= len(self.gpu):
                return False
            item = self.gpu.pop(idx)
            self.gpu.insert(new_index, item)
        self._notify()
        self.save_debounced()
        return True

    # ── current-task tracking ───────────────────────────────────────

    def set_current_sync(self, ch: Optional[Dict[str, Any]]):
        with self._lock:
            self.current_sync = copy.deepcopy(ch) if ch else None
        self._notify()

    def set_sync_pass_progress(self, index: int, total: int) -> None:
        """Record `(index, total)` so the popover label reads
        'Downloading {name} ({index}/{total})'. Called by sync_start_all
        at the top of each channel iteration. `index=0, total=0` clears
        the pass state (no pass active)."""
        with self._lock:
            self.sync_pass_index = max(0, int(index))
            self.sync_pass_total = max(0, int(total))
        self._notify()

    def set_current_gpu(self, item: Optional[Dict[str, Any]]):
        with self._lock:
            self.current_gpu = copy.deepcopy(item) if item else None
        self._notify()

    # ── UI payload ──────────────────────────────────────────────────

    def to_ui_payload(self) -> Dict[str, Any]:
        """Return the shape the queue popovers expect (see web/logs.js renderQueues)."""
        with self._lock:
            sync_list = []
            if self.current_sync:
                # When a Sync-Subbed pass is running, decorate the active
                # channel label with "(N/total)" so the popover shows
                # progress through the pass. Outside of a pass, we just
                # render the channel name plain.
                label = self._task_label_sync(self.current_sync, running=True)
                if self.sync_pass_total > 0 and self.sync_pass_index > 0:
                    label = f"{label} ({self.sync_pass_index}/{self.sync_pass_total})"
                sync_list.append({
                    "name": label,
                    "status": "running",
                    # Identifiers used by the right-click "Remove from
                    # queue" context menu → api.queues_sync_remove.
                    # Without these the JS fell back to the display name
                    # which didn't match the backend's URL-keyed removal.
                    "url": (self.current_sync.get("url") or "").strip(),
                    "channel_name": (self.current_sync.get("name")
                                      or self.current_sync.get("folder")
                                      or "").strip(),
                })
            for ch in self.sync:
                sync_list.append({
                    "name": self._task_label_sync(ch, running=False),
                    "status": "queued",
                    "url": (ch.get("url") or "").strip(),
                    "channel_name": (ch.get("name")
                                      or ch.get("folder") or "").strip(),
                })

            gpu_list = []
            # Track which bulk_ids are being represented by the running
            # item so the still-queued remainder from that bulk collapses
            # into a single "Transcribe {ch} (N more)" row.
            running_bulk_id = ""
            if self.current_gpu:
                running_bulk_id = str(self.current_gpu.get("bulk_id") or "")
                gpu_list.append({
                    "name": self._task_label_gpu(self.current_gpu, running=True,
                                                 bulk_context=None),
                    "status": "running",
                    "path": (self.current_gpu.get("path") or "").strip(),
                    "bulk_id": running_bulk_id,
                })
            # Coalesce queued items by bulk_id. First pass: count items per
            # bulk_id. Second pass: emit one row per bulk (or per-item if
            # no bulk_id).
            bulk_counts: Dict[str, int] = {}
            bulk_channels: Dict[str, str] = {}
            for t in self.gpu:
                bid = str(t.get("bulk_id") or "")
                if bid:
                    bulk_counts[bid] = bulk_counts.get(bid, 0) + 1
                    if bid not in bulk_channels:
                        bulk_channels[bid] = (t.get("channel") or "").strip()
            seen_bulks: set = set()
            for t in self.gpu:
                bid = str(t.get("bulk_id") or "")
                if bid and bid in seen_bulks:
                    continue
                if bid and bulk_counts.get(bid, 0) > 1:
                    # Emit one condensed row for the whole bulk.
                    ch_name = bulk_channels.get(bid) or (t.get("channel") or "?")
                    remaining = bulk_counts[bid]
                    # If part of this bulk is already the "running" slot,
                    # the queued remainder is one short of bulk_total.
                    if bid == running_bulk_id:
                        label = f"Transcribe {ch_name} ({remaining} more)"
                    else:
                        label = f"Transcribe {ch_name} ({remaining} videos)"
                    gpu_list.append({
                        "name": label,
                        "status": "queued",
                        "bulk_id": bid,
                        "bulk_count": remaining,
                    })
                    seen_bulks.add(bid)
                else:
                    gpu_list.append({
                        "name": self._task_label_gpu(t, running=False,
                                                     bulk_context=None),
                        "status": "queued",
                        "path": (t.get("path") or "").strip(),
                        "bulk_id": str(t.get("bulk_id") or ""),
                    })
            return {
                "sync": sync_list,
                "gpu": gpu_list,
                "gpu_paused": self.gpu_paused,
                "sync_paused": self.sync_paused,
            }

    @staticmethod
    def _task_label_sync(ch: Dict[str, Any], running: bool) -> str:
        """Pos 1 (running) uses present-continuous, other slots use the plain verb.
        Branches on `kind` so the popover shows meaningful labels for
        non-download sync-queue items (metadata recheck, etc.).
        Label must START with a verb that `colorizeTaskName` recognizes
        so the popover rows get color-coded — "Metadata" → pink,
        "Download" → green, etc.
        """
        name = ch.get("name") or ch.get("folder") or "?"
        kind = (ch.get("kind") or "download").lower()
        if kind == "metadata":
            # Keep "Metadata" as the leading word so `colorizeTaskName`
            # in logs.js picks the pink `qv-meta` class. "the
            # check metadata part of these tasks in queue are supposed
            # to be colored pink LIKE THEY WERE IN PREVIOUS VERSION."
            return f"Metadata check \u2014 {name}"
        if kind == "videoid_backfill":
            # Fix IDs task — share the Metadata color family (pink)
            # since it's a metadata-kind repair, not a download. Label
            # starts with "Metadata" so colorizeTaskName picks up the
            # pink `qv-meta` class like the other metadata rows.
            return f"Metadata ID fix \u2014 {name}"
        if kind == "redownload":
            # Classic showed active redownload as "Redownload
            # ChannelName (480p)" with a Pause/Resume state.
            # Leading word must be recognized by colorizeTaskName
            # so the row picks up the redownload (chartreuse) color.
            res = str(ch.get("redownload_res") or "").strip()
            res_label = ""
            if res:
                res_label = f" ({'Best' if res == 'best' else res + 'p'})"
            verb = "Redownloading" if running else "Redownload"
            return f"{verb} {name}{res_label}"
        verb = "Downloading" if running else "Download"
        return f"{verb} {name}"

    @staticmethod
    def _task_label_gpu(t: Dict[str, Any], running: bool,
                        bulk_context: Optional[Dict[str, Any]] = None) -> str:
        # `bulk_context` is reserved for future coalesce-label overrides
        # from to_ui_payload (per-video label remains the same for now).
        title = t.get("title") or os.path.basename(t.get("path", "?")).rsplit(".", 1)[0]
        raw_kind = (t.get("kind") or "transcribe").lower()
        if raw_kind == "transcribe":
            verb = "Transcribing" if running else "Transcribe"
        elif raw_kind == "encode":
            verb = "Encoding" if running else "Encode"
        elif raw_kind == "compress":
            verb = "Compressing" if running else "Compress"
        else:
            verb = raw_kind.capitalize()
        # When the job is part of a bulk and is currently running, decorate
        # it with "(X/total)" so the user can see progress through the batch.
        if running:
            bi = int(t.get("bulk_index") or 0)
            bt = int(t.get("bulk_total") or 0)
            if bt > 1:
                return f"{verb} {title} ({bi + 1}/{bt})"
        return f"{verb} {title}"

    # ── pause state ─────────────────────────────────────────────────

    def set_gpu_paused(self, paused: bool):
        with self._lock:
            self.gpu_paused = bool(paused)
        self._notify()
        self.save_debounced()

    def set_sync_paused(self, paused: bool):
        with self._lock:
            self.sync_paused = bool(paused)
        self._notify()
        self.save_debounced()

    # ── stats ───────────────────────────────────────────────────────

    def counts(self) -> Dict[str, int]:
        # audit E-16: include transcribe + video counts so the UI
        # badge totals don't silently undercount. Old counts() only
        # returned sync/gpu/redownload/metadata/reorg, so items on
        # transcribe/video lists were invisible to any caller using
        # this dict for summaries.
        with self._lock:
            return {
                "sync": len(self.sync) + (1 if self.current_sync else 0),
                "gpu": len(self.gpu) + (1 if self.current_gpu else 0),
                "redownload": len(self.redownload),
                "metadata": len(self.metadata),
                "reorg": len(self.reorg),
                "transcribe": len(self.transcribe),
                "video": len(self.video),
            }

    # ── restore-on-launch helpers ───────────────────────────────────
    def has_sync_pipeline_items(self) -> bool:
        """True if sync/reorg/transcribe/redownload/metadata/video has items.
        Used after load() to decide whether to force-pause (Project rule: launching with items in queue must never auto-start)."""
        with self._lock:
            return bool(self.sync or self.reorg or self.transcribe
                        or self.redownload or self.metadata or self.video)

    def has_gpu_items(self) -> bool:
        with self._lock:
            return bool(self.gpu)
