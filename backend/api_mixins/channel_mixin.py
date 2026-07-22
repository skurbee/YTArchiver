"""
ChannelMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time

from ._shared import _log, ALLOWED_REDOWNLOAD_RESOLUTIONS
from backend.ytarchiver_config import load_config, save_config
from backend import archive_scan
from backend import subs as subs_backend
from backend import reorg as reorg_backend
from backend.queues import QueueState


class ChannelMixin:

    # ─── Channel context actions ───────────────────────────────────────

    @staticmethod
    def _coerce_channel_name(folder_or_name) -> str:
        if isinstance(folder_or_name, str):
            return folder_or_name.strip()
        if isinstance(folder_or_name, dict):
            return str(folder_or_name.get("name")
                       or folder_or_name.get("folder") or "").strip()
        return ""

    def _channel_folder_for_name(self, name, *, use_cached_config=False):
        """Resolve (channel_dict, absolute folder path) for an already-
        coerced channel name.

        Returns a ``(ch, folder)`` tuple on success, or an
        ``{"ok": False, "error": ...}`` dict on failure ("Channel not found"
        / "output_dir not set"). Collapses the get_channel -> output_dir ->
        channel_folder_name preamble that was copy-pasted across ChannelMixin
        methods (T347). `use_cached_config` mirrors the in-place handlers that
        read `self._config or load_config()` instead of a fresh load.
        """
        ch = subs_backend.get_channel({"name": name})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        cfg = ((self._config or load_config())
               if use_cached_config else load_config())
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        from backend.sync import channel_folder_name
        return ch, os.path.join(base, channel_folder_name(ch))

    def chan_open_folder(self, folder_or_name):
        # Validate arg type up front. JS callers can accidentally pass
        # None or a number; without this guard, .get(...) below raises
        # AttributeError which surfaces in the JS bridge as an opaque
        # "TypeError: NoneType has no attribute 'get'" instead of a
        # friendly toast.
        if folder_or_name is None or not isinstance(folder_or_name, (str, dict)):
            return {"ok": False,
                    "error": "Invalid argument (expected str or dict)"}
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        # Accept a raw folder name (string) or an identity dict
        name = self._coerce_channel_name(folder_or_name)
        if not name:
            return {"ok": False, "error": "Invalid channel argument"}
        ch = None
        try:
            ch = subs_backend.get_channel(folder_or_name if isinstance(
                folder_or_name, dict) else {"name": name})
            if not ch:
                ch = subs_backend.get_channel({"folder": name})
        except Exception:
            ch = None
        from backend.sync import channel_folder_name, sanitize_folder
        folder_name = channel_folder_name(ch) if ch else sanitize_folder(name)
        path = os.path.join(base, folder_name)
        # if the folder doesn't exist yet, don't silently
        # CREATE it. Right-clicking "Open folder" on a channel that
        # has never synced (URL-only subscription) used to materialize
        # an empty directory on the archive drive — polluting the
        # filesystem with empty folders for every channel the user
        # only clicked "Open folder" on.
        if not os.path.isdir(path):
            return {"ok": False,
                    "error": f"Folder not created yet (no sync has run): {path}"}
        try:
            if os.name == "nt":
                os.startfile(path)
            else:
                import subprocess
                subprocess.Popen(["xdg-open" if sys.platform != "darwin" else "open", path])
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def chan_open_url(self, folder_or_name):
        import webbrowser
        try:
            name = self._coerce_channel_name(folder_or_name)
            if not name:
                return {"ok": False, "error": "Invalid channel argument"}
            ch = subs_backend.get_channel({"name": name})
            if not ch or not ch.get("url"):
                return {"ok": False, "error": "URL not found"}
            webbrowser.open(ch["url"])
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def channel_transcription_stats(self, folder_or_name):
        """Return {total, transcribed, pending, failed} counts for a channel
        from the FTS DB. Used by the edit panel to show coverage at a glance.
        """
        name = self._coerce_channel_name(folder_or_name)
        try:
            from backend import index as _idx
            stats = _idx.channel_transcription_stats(name)
            return {"ok": True, **stats}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def transcribe_retranscribe_channel(self, folder_or_name, model=""):
        """Queue every video in a channel for Whisper retranscribe.

        right-click a channel → re-transcribe entire
        channel with model selection. Swaps the running Whisper model
        first (one-off, NOT persisted to Settings), then enqueues every
        video in the channel that has a transcript file.
        """
        name = self._coerce_channel_name(folder_or_name)
        if not name:
            return {"ok": False, "error": "Channel name required"}
        try:
            from backend import index as _idx
            rows = _idx.list_videos_for_channel(name, sort="newest",
                                                  limit=10000) or []
        except Exception as e:
            return {"ok": False, "error": f"Could not list videos: {e}"}
        if not rows:
            return {"ok": False, "error": "No videos found for channel"}
        # Swap Whisper model just for this batch when one was supplied.
        if model:
            try:
                self.transcribe_swap_model(model, persist=False)
            except Exception as e:
                _log.warning("model swap before retranscribe failed (batch will use current model): %s", e)
        queued = 0
        skipped = 0
        for row in rows:
            fp = row.get("filepath") or ""
            t = row.get("title") or ""
            vid = row.get("video_id") or ""
            if not fp or not os.path.isfile(fp):
                skipped += 1
                continue
            res = self.transcribe_retranscribe(fp, t, vid,
                                               _log_queued=False)
            if isinstance(res, dict) and res.get("ok"):
                queued += 1
            else:
                skipped += 1
        try:
            video_word = "Video" if queued == 1 else "Videos"
            self._transcribe_log_stream().emit_text(
                f" — Queued re-transcribe: {name}, {queued:,} {video_word}",
                "simpleline_blue")
        except Exception:
            pass
        return {"ok": True, "queued": queued, "skipped": skipped,
                "total": len(rows), "channel": name,
                "model": model or "default"}


    def chan_fetch_art(self, folder_or_name, force=False):
        """Download channel avatar + banner for one channel.

        Writes <channel_folder>/.ChannelArt/{avatar,banner}.jpg. Best-effort —
        runs in a background thread so the UI doesn't block.
        """
        name = self._coerce_channel_name(folder_or_name)
        resolved = self._channel_folder_for_name(name)  # T347
        if isinstance(resolved, dict):
            return resolved
        ch, folder = resolved

        # Per-channel in-flight dedupe so rapid clicks don't spawn N
        # concurrent yt-dlp processes hitting the same channel URL and
        # racing each other on the same .ChannelArt/*.jpg writes
        # (audit: channel_mixin H23). Pattern matches
        # archive_mixin's _archive_single_inflight.
        _key = (ch.get("url") or name or folder).strip().lower()
        if not hasattr(self, "_chan_art_inflight") or \
                self._chan_art_inflight is None:
            self._chan_art_inflight = set()
            self._chan_art_lock = threading.Lock()
        with self._chan_art_lock:
            if _key in self._chan_art_inflight:
                return {"ok": False,
                        "error": "Already fetching art for this channel"}
            self._chan_art_inflight.add(_key)

        def _run():
            # Surface failures explicitly. Old code let any exception
            # escape the thread silently — the user clicked "Fetch
            # art", saw no error, and assumed it worked even when the
            # network was down or yt-dlp failed (audit:
            # channel_mixin.py:118).
            try:
                from backend import channel_art as _ca
                _ca.fetch_channel_art(ch.get("url", ""), folder,
                                       force=bool(force))
            except Exception as e:
                try:
                    self._log_stream.emit_error(
                        f"Channel-art fetch failed for {name}: {e}")
                    self._log_stream.flush()
                except Exception:
                    pass
                _log.debug("chan_fetch_art swallowed: %s", e)
            finally:
                try:
                    with self._chan_art_lock:
                        self._chan_art_inflight.discard(_key)
                except Exception:
                    pass

        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}


    def chan_art_paths(self, folder_or_name):
        """Return local avatar/banner paths for a channel, if they exist."""
        name = self._coerce_channel_name(folder_or_name)
        ch = subs_backend.get_channel({"name": name})
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not ch or not base:
            return {"ok": False}
        from backend.channel_art import avatar_path_for, banner_path_for
        from backend.index import _file_url
        from backend.sync import channel_folder_name as _cfn
        folder = os.path.join(base, _cfn(ch))
        ap = avatar_path_for(folder)
        bp = banner_path_for(folder)
        return {
            "ok": True,
            "avatar_url": _file_url(ap) if ap else None,
            "banner_url": _file_url(bp) if bp else None,
        }


    def chan_transcribe_pending(self, folder_or_name):
        """Queue every video in this channel's `pending_tx_ids` list.

        Authoritative source: `channel.pending_tx_ids` — populated by
        sync.py when a video downloads onto a channel whose
        auto_transcribe flag was off at that moment. No folder scan, no
        title matching, no heuristics. Every ID in that list corresponds
        to a real, concrete file the sync pipeline knows about; we
        resolve filepath via the FTS index and enqueue.

        Matches the v47.7 design spec: "Keep a log of the video IDs
        that are skipped, and have the queue pending button DIRECTLY
        snipe the info we need."
        """
        name = self._coerce_channel_name(folder_or_name)
        ch = subs_backend.get_channel({"name": name})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        pending_ids = list(ch.get("pending_tx_ids") or [])
        if not pending_ids:
            self._log_stream.emit([
                ["[GPU] ", "trans_bracket"],
                [f"Queue pending for {name}: ", "simpleline_blue"],
                ["nothing pending.\n", "simpleline"],
            ])
            self._log_stream.flush()
            return {"ok": True, "queued": 0, "skipped": 0}

        # Skip IDs whose file is already queued / running so rapid
        # double-clicks don't stack duplicates.
        queued_paths = set()
        try:
            # Read _current_job INSIDE the same _jobs_lock that protects
            # the queue. Old code released the lock first, so the
            # transcribe worker could reassign _current_job between the
            # release and the read — letting a duplicate slip past the
            # dedupe set or, in the other direction, marking an
            # already-finished job as still queued (audit:
            # channel_mixin.py:204).
            with self._transcribe._jobs_lock:
                for j in self._transcribe._jobs:
                    p = j.get("path") or ""
                    if p:
                        queued_paths.add(os.path.normpath(p).lower())
                cj = self._transcribe._current_job
                if cj:
                    p = cj.get("path") or ""
                    if p:
                        queued_paths.add(os.path.normpath(p).lower())
        except Exception as e:
            _log.warning("could not read transcribe job queue for dedup (duplicates may be enqueued): %s", e)

        # Resolve each ID → filepath via the FTS index, in one shot.
        id_to_path: dict = {}
        unresolved: list = []
        try:
            from backend.index import _open as _idx_open
            conn = _idx_open()
            if conn is not None:
                placeholders = ",".join(["?"] * len(pending_ids))
                rows = conn.execute(
                    f"SELECT video_id, filepath, title, tx_status FROM videos "
                    f"WHERE video_id IN ({placeholders})",
                    pending_ids,
                ).fetchall()
                for r in rows:
                    vid, fp, title = r[0], (r[1] or ""), (r[2] or "")
                    txs = (r[3] or "") if len(r) > 3 else ""
                    if vid and fp:
                        id_to_path[vid] = (fp, title, txs)
        except Exception as e:
            _log.warning("FTS index unavailable for Queue Pending resolve; all IDs treated as unresolved: %s", e)

        queued = 0
        skipped = 0
        bulk = []  # (video_path, title, video_id)
        for vid in pending_ids:
            info = id_to_path.get(vid)
            if not info:
                unresolved.append(vid)
                continue
            fp, title, txs = info
            # Skip genuinely-silent videos: already checked, Whisper found no
            # speech. Re-running just yields nothing again. (Explicit
            # right-click "Re-transcribe" still allows it; this is the
            # auto/catch-up "Queue Pending" path.)
            if txs == "no_speech":
                skipped += 1
                continue
            if not fp or not os.path.isfile(fp):
                unresolved.append(vid)
                continue
            if os.path.normpath(fp).lower() in queued_paths:
                skipped += 1
                continue
            bulk.append((
                fp,
                title or os.path.splitext(os.path.basename(fp))[0],
                vid,
            ))

        if bulk:
            import uuid as _uuid
            bulk_id = _uuid.uuid4().hex[:12]
            bulk_total = len(bulk)
            for idx, (video, title, vid) in enumerate(bulk):
                if self._transcribe.enqueue(video, title, channel=name,
                                            bulk_id=bulk_id,
                                            bulk_total=bulk_total,
                                            bulk_index=idx,
                                            video_id=vid):
                    queued += 1
                else:
                    skipped += 1

        self._log_stream.emit([
            ["[GPU] ", "trans_bracket"],
            [f"Queue pending for {name}: ", "simpleline_blue"],
            [f"{queued} queued"
             + (f", {skipped} already in queue" if skipped else "")
             + (f", {len(unresolved)} unresolved" if unresolved else "")
             + "\n", "simpleline"],
        ])
        # Log unresolved IDs so the user can see what's dangling — a
        # deleted-since-download video, or an FTS index gap. Dropping
        # those from the list keeps the counter honest.
        if unresolved:
            for u in unresolved:
                self._log_stream.emit([
                    ["[GPU] ", "trans_bracket"],
                    [f"  \u2014 dropping unresolved id: {u}\n", "dim"],
                ])
            try:
                cfg2 = load_config()
                for _ch in cfg2.get("channels", []):
                    if (_ch.get("name") or "") != name:
                        continue
                    ids = _ch.get("pending_tx_ids") or []
                    ids = [x for x in ids if x not in unresolved]
                    _ch["pending_tx_ids"] = ids
                    _ch["transcription_pending"] = len(ids)
                    if not ids:
                        _ch["transcription_complete"] = True
                    break
                from backend.ytarchiver_config import save_config as _sc
                # check the save result. Without this, a
                # write-gate-off / disk-full save would silently leave
                # the stale unresolved IDs on disk; next call reloads
                # them and the same "unresolved" list comes back.
                if not _sc(cfg2):
                    self._log_stream.emit_dim(
                        " (unresolved-id cleanup not persisted — config write-gate off?)")
                else:
                    # Refresh in-memory config so the next caller reads
                    # the pruned list instead of the pre-prune state.
                    self._config = cfg2
            except Exception as e:
                _log.warning("unresolved-id prune save failed; stale IDs will persist until next launch: %s", e)
        self._log_stream.flush()
        return {"ok": True, "queued": queued, "skipped": skipped,
                "unresolved": len(unresolved)}


    def chan_transcribe_all(self, folder_or_name, combined=None):
        """Walk the channel folder for videos without .jsonl sidecars and queue each for whisper.

        `combined` controls per-year output:
          - None : decide from existing transcripts (first-time → may need UI choice)
          - True : write one combined `{ch} Transcript.txt` at the channel root
          - False : follow organization (per-year files)

        If this is the first-time transcribing an organized channel (split_years=True)
        AND `combined` is unspecified, returns `{ok: True, needs_choice: True,
        org_label: "Year" | "Year/Month"}` so the UI can show the OLD-style
        "Follow organization / Combined" radio dialog (YTArchiver.py:5919).
        The UI should then re-call with combined=True or False.
        """
        name = self._coerce_channel_name(folder_or_name)
        resolved = self._channel_folder_for_name(name)  # T347
        if isinstance(resolved, dict):
            return resolved
        ch, folder = resolved
        split_years = bool(ch.get("split_years"))
        split_months = bool(ch.get("split_months"))

        # First-time-choice logic: for organized channels with no existing
        # transcripts, ask the user whether to follow org or combine.
        # Match OLD's dialog at YTArchiver.py:5918-5952.
        if combined is None and split_years:
            has_existing = False
            if os.path.isdir(folder):
                for dp, _dns, fns in os.walk(folder):
                    if any(fn.endswith(("Transcript.txt",
                                        "Transcript.jsonl")) for fn in fns):
                        has_existing = True
                        break
            if not has_existing:
                org_label = "Year/Month" if split_months else "Year"
                return {"ok": True, "needs_choice": True,
                        "channel": name, "org_label": org_label}
            # Has existing transcripts → follow whatever org they picked last time
            combined = False
        elif combined is None:
            combined = True # unorganized channels always combine

        # Build a dict of already-transcribed titles (normalized) + stored
        # video IDs from every aggregate Transcript.txt under this folder.
        # The scan is now permissive (any *Transcript.txt, unicode-normalized
        # keys, dual plain/with-id variants) so minor string differences
        # between filename and stored title stop producing false
        # "needs transcribing" hits.
        from backend.transcribe import _norm_title, _scan_existing_transcript_titles
        already = _scan_existing_transcript_titles(folder, name)
        done_vids = {vid for (_raw, vid) in already.values() if vid}
        no_speech_vids: set[str] = set()
        no_speech_paths: set[str] = set()
        no_speech_titles: set[str] = set()
        try:
            from backend import index as _idx
            conn = _idx._reader_open() or _idx._open()
            if conn is not None:
                with _idx._reader_lock:
                    rows = conn.execute(
                        "SELECT video_id, filepath, title FROM videos "
                        "WHERE channel=? COLLATE NOCASE "
                        "AND tx_status='no_speech'",
                        (name,),
                    ).fetchall()
                for r in rows:
                    if r[0]:
                        no_speech_vids.add(str(r[0]).strip())
                    if r[1]:
                        no_speech_paths.add(os.path.normcase(
                            os.path.normpath(os.path.abspath(str(r[1])))))
                    if r[2]:
                        no_speech_titles.add(_norm_title(str(r[2])))
        except Exception as e:
            _log.debug("no_speech skip lookup failed for %s: %s", name, e)

        skipped = 0
        no_speech_skipped = 0
        bulk = []  # (video_path, plain_title, video_id)
        import re as _re
        for dp, _dns, fns in os.walk(folder):
            for fn in fns:
                if not fn.lower().endswith((".mp4", ".mkv", ".webm", ".m4a")):
                    continue
                video = os.path.join(dp, fn)
                base_path = os.path.splitext(video)[0]
                title = os.path.splitext(fn)[0]
                plain_title = _re.sub(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$",
                                      "", title) or title
                vid_m = _re.search(r"\[([A-Za-z0-9_-]{11})\]", title)
                vid_id = vid_m.group(1) if vid_m else ""
                video_key = os.path.normcase(
                    os.path.normpath(os.path.abspath(video)))
                if ((vid_id and vid_id in no_speech_vids)
                        or video_key in no_speech_paths
                        or _norm_title(plain_title) in no_speech_titles
                        or _norm_title(title) in no_speech_titles):
                    skipped += 1
                    no_speech_skipped += 1
                    continue
                # Per-video legacy .jsonl sidecar (OLD format)
                if os.path.isfile(base_path + ".jsonl"):
                    skipped += 1
                    continue
                # Aggregate title match (normalized, either with or
                # without `[videoId]` tail)
                if (_norm_title(plain_title) in already
                        or _norm_title(title) in already):
                    skipped += 1
                    continue
                # Video-ID match against any aggregate-stored ID
                if vid_id and vid_id in done_vids:
                    skipped += 1
                    continue
                bulk.append((video, plain_title, vid_id))

        queued = 0
        if bulk:
            import uuid as _uuid
            bulk_id = _uuid.uuid4().hex[:12]
            bulk_total = len(bulk)
            for idx, (video, plain_title, vid_id) in enumerate(bulk):
                # Pass combined flag through so the transcribe worker writes
                # to the right aggregated file. Respects the user's choice
                # even when it conflicts with the channel's split_years flag.
                # bulk_id coalesces the popover display.
                self._transcribe.enqueue(video, plain_title, channel=name,
                                         combined=bool(combined),
                                         bulk_id=bulk_id, bulk_total=bulk_total,
                                         bulk_index=idx,
                                         video_id=vid_id)
                queued += 1
        self._log_stream.emit([
            ["[GPU] ", "trans_bracket"],
            [f"Transcribe all for {name}: ", "simpleline_blue"],
            [f"{queued} queued, {skipped} already handled"
             + (f" ({no_speech_skipped} no-speech)"
                if no_speech_skipped else "")
             + (" (combined)" if combined and split_years else ""),
             "simpleline"],
            ["\n", None],
        ])
        self._log_stream.flush()
        # push a queue-changed notification so the GPU
        # Tasks popover reflects the freshly-enqueued items without
        # waiting for the next periodic poll (~500ms).
        try:
            self._on_queue_changed()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        return {"ok": True, "queued": queued, "skipped": skipped,
                "combined": bool(combined)}


    def chan_redownload_progress_peek(self, folder_or_name):
        """Check whether a channel has a saved redownload-in-progress file.
        Returns {ok, pending: bool, resolution, done_ids_count} so the UI
        can offer a "Continue redownload" button in the edit panel.
        Matches YTArchiver.py:5473 _has_pending_redownload."""
        try:
            name = self._coerce_channel_name(folder_or_name)
            if not name:
                return {"ok": False, "error": "channel name required"}
            ch = subs_backend.get_channel({"name": name})
            if not ch:
                return {"ok": False, "error": "Channel not found"}
            cfg = self._config or load_config()
            base = (cfg.get("output_dir") or "").strip()
            if not base:
                return {"ok": True, "pending": False}
            from backend.sync import channel_folder_name as _cfn
            folder = os.path.join(base, _cfn(ch))
            pp = os.path.join(folder, "_redownload_progress.json")
            if not os.path.isfile(pp):
                return {"ok": True, "pending": False}
            try:
                import json as _j
                with open(pp, "r", encoding="utf-8") as f:
                    data = _j.load(f)
                done_n = len(data.get("done_ids") or [])
                res = data.get("resolution") or ""
                return {"ok": True, "pending": True,
                        "resolution": res, "done": done_n}
            except Exception:
                return {"ok": True, "pending": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def chan_cancel_redownload(self, folder_or_name):
        """Cancel a pending or running redownload for this channel.

        Four cleanup paths, any of which may apply:
        1. If this channel's redownload is currently running, fire
           `_sync_cancel` so the pipeline exits at its next chunk
           boundary (same mechanism the Sync Tasks popover Cancel
           button uses).
        2. Remove any queued entries for this channel from the
           internal `_redwnl_pending` list — the chain worker won't
           start them.
        3. Remove from `queues.sync` so the UI popover drops the
           row and the task count decrements.
        4. Delete `_redownload_progress.json` and any legacy broken-count
           sidecar from the channel folder so the Subs-table chartreuse
           dot + right-click "Continue Redownload" option both disappear
           on next render.

        Returns `{ok, was_running, was_queued, progress_removed}`.
        """
        try:
            name = self._coerce_channel_name(folder_or_name)
            if not name:
                return {"ok": False, "error": "channel name required"}
            ch = subs_backend.get_channel({"name": name})
            if not ch:
                return {"ok": False, "error": "Channel not found"}
            ch_url = (ch.get("url") or "").strip()
            cfg = self._config or load_config()
            base = (cfg.get("output_dir") or "").strip()
            from backend.sync import channel_folder_name as _cfn
            folder = os.path.join(base, _cfn(ch)) if base else ""

            was_running = False
            was_queued = False
            progress_removed = False

            # 1. Currently running? Check the active sync task.
            try:
                cur = self._queues.current_sync or {}
                if ((cur.get("kind") or "").lower() == "redownload"
                        and (cur.get("url") or "").strip() == ch_url):
                    was_running = True
                    # Per-channel cancel sets the REDOWNLOAD event only
                    # — setting the shared _sync_cancel here killed
                    # every other queued chain item (the worker passes
                    # the still-set event into each subsequent run) and
                    # left later Reorg/Fix-dates ghost-cancelled.
                    self._redwnl_cancel.set()
            except Exception as e:
                _log.debug("swallowed: %s", e)

            # 2. Drop matching items from the internal pending chain.
            try:
                with self._redwnl_lock:
                    before = len(self._redwnl_pending)
                    self._redwnl_pending = [
                        it for it in self._redwnl_pending
                        if (it.get("rd_task", {}).get("url") or "").strip()
                        != ch_url
                    ]
                    if len(self._redwnl_pending) < before:
                        was_queued = True
            except Exception as e:
                _log.debug("swallowed: %s", e)

            # 3. Remove from the UI queue (may be there without being
            # in _redwnl_pending if the worker already popped it).
            try:
                if ch_url:
                    removed = self._queues.sync_remove(ch_url)
                    was_queued = was_queued or bool(removed)
            except Exception as e:
                _log.debug("swallowed: %s", e)

            # 4. Delete the progress file so the pending state clears.
            try:
                if folder:
                    for pp in (
                            os.path.join(folder, "_redownload_progress.json"),
                            os.path.join(folder,
                                         "_redownload_broken_counts.json")):
                        if os.path.isfile(pp):
                            os.remove(pp)
                            progress_removed = True
            except OSError:
                pass

            # Invalidate the archive-scan cache so the Subs row
            # re-reads `_pending_redownload` as False on next render.
            try:
                from backend import archive_scan as _as
                _as.invalidate_channel(ch_url)
            except Exception as e:
                _log.debug("swallowed: %s", e)

            self._on_queue_changed()
            # push a Subs refresh so the chartreuse "Continue
            # Redownload" dot disappears immediately instead of waiting
            # for a tab switch. Mirrors the redownload-finished path.
            try:
                if self._window is not None:
                    self._window.evaluate_js(
                        "if (window.refreshSubsTable) "
                        "window.refreshSubsTable();")
            except Exception as e:
                _log.debug("swallowed: %s", e)
            return {"ok": True,
                    "was_running": was_running,
                    "was_queued": was_queued,
                    "progress_removed": progress_removed}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def chan_scan_resolution_mismatch(self, folder_or_name, target_res):
        """ffprobe every video in the channel's folder and count how many
        are below `target_res` (height). Returns {ok, started, token}.

        Used by the edit panel's "Recheck resolution" button before offering
        to queue a bulk redownload. Matches YTArchiver.py:5155 res_check_btn.
        Fast path: "best" always reports 0 mismatches since we can't know
        what "best" actually is without a fresh catalog probe.

        Bridge-thread fast return — the os.walk + per-file ffprobe loop
        can take minutes on large channels; the work runs on a worker
        thread. JS polls via chan_scan_resolution_mismatch_poll(token).
        """
        try:
            import subprocess as _sp
            import uuid as _uuid
            name = self._coerce_channel_name(folder_or_name)
            if not name:
                return {"ok": False, "error": "channel name required"}
            target = str(target_res or "720").strip().lower()
            if target not in ALLOWED_REDOWNLOAD_RESOLUTIONS:
                return {"ok": False, "error": f"Unsupported resolution: {target}"}
            if target == "best":
                return {"ok": True, "mismatch": 0, "total": 0,
                        "note": "Best mode can't be scanned ahead of time."}
            target_h = int(target)
            ch = subs_backend.get_channel({"name": name})
            if not ch:
                return {"ok": False, "error": "Channel not found"}
            cfg = self._config or load_config()
            base = (cfg.get("output_dir") or "").strip()
            if not base:
                return {"ok": False, "error": "output_dir not set"}
            from backend.sync import channel_folder_name as _cfn
            folder = os.path.join(base, _cfn(ch))
            if not os.path.isdir(folder):
                return {"ok": False, "error": f"Folder missing: {folder}"}

            token = _uuid.uuid4().hex
            if not hasattr(self, "_pending_res_scans"):
                self._pending_res_scans = {}
                self._pending_res_scans_lock = threading.Lock()

            def _scan_worker():
                total = 0
                mismatch = 0
                scanned = 0
                _exts = (".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v")
                for dp, _dns, fns in os.walk(folder):
                    for fn in fns:
                        if not fn.lower().endswith(_exts):
                            continue
                        total += 1
                        fp_ = os.path.join(dp, fn)
                        try:
                            r = _sp.run(
                                ["ffprobe", "-v", "error",
                                 "-select_streams", "v:0",
                                 "-show_entries", "stream=height",
                                 "-of", "default=noprint_wrappers=1:nokey=1", fp_],
                                capture_output=True, text=True, timeout=6,
                                creationflags=(0x08000000 if os.name == "nt" else 0))
                            height = int((r.stdout or "0").strip() or 0)
                            scanned += 1
                            # Redownload supports intentional downsizing, so a
                            # 720p file is also a mismatch for a selected 360p
                            # target. The old one-sided `< target` check only
                            # detected upgrades and reported the whole archive
                            # as "already at 360p or higher."
                            if height > 0 and abs(height - target_h) > 8:
                                mismatch += 1
                        except Exception:
                            continue
                import time as _t_mod
                with self._pending_res_scans_lock:
                    self._pending_res_scans[token] = {
                        "done": True,
                        "_ts": _t_mod.time(),
                        "result": {"ok": True, "mismatch": mismatch,
                                   "total": total, "scanned": scanned,
                                   "target": target_h},
                    }

            # Sweep abandoned entries (>10 min old) on every new submit
            # so the dict can't grow unbounded if the user navigates
            # away mid-scan (audit: channel_mixin H10).
            import time as _t_mod
            _now_ts = _t_mod.time()
            with self._pending_res_scans_lock:
                _stale = [k for k, v in self._pending_res_scans.items()
                          if isinstance(v, dict)
                          and (_now_ts - (v.get("_ts") or _now_ts)) > 600]
                for k in _stale:
                    self._pending_res_scans.pop(k, None)
                self._pending_res_scans[token] = {"done": False, "_ts": _now_ts}
            threading.Thread(target=_scan_worker, daemon=True,
                             name="chan_scan_resolution").start()
            return {"ok": True, "started": True, "token": token}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def chan_scan_resolution_mismatch_poll(self, token):
        """Poll a token returned by chan_scan_resolution_mismatch. Returns
        {pending: True} while running, or the final {ok, mismatch, total,
        ...} payload when done. Once returned, the token is forgotten."""
        if not hasattr(self, "_pending_res_scans"):
            return {"ok": False, "error": "unknown token"}
        with self._pending_res_scans_lock:
            entry = self._pending_res_scans.get(token)
            if entry is None:
                return {"ok": False, "error": "unknown token"}
            if not entry.get("done"):
                return {"pending": True}
            del self._pending_res_scans[token]
        return entry.get("result") or {"ok": False, "error": "no result"}



    def chan_redownload(self, folder_or_name, new_resolution=None,
                        scope=None):
        """Queue a channel's videos for redownload at a new resolution.

        Runs the full pipeline in `backend/redownload.py` — scans local files,
        fetches the YouTube catalog, matches by ID, downloads each at the new
        resolution, replaces the originals, and persists progress so a
        cancelled run can resume. Respects pause + cancel events.

        `scope` (optional dict):
          None - whole channel (default, matches OLD's tree-view root right-click)
          {year: 2024} - only that year subfolder (split_years channels)
          {year: 2024, month: 5} - only that year+month subfolder
        Mirrors OLD's per-year / per-month tree-view right-click
        (YTArchiver.py:26498 _browse_redownload_folder).
        """
        name = self._coerce_channel_name(folder_or_name)
        if not name:
            return {"ok": False, "error": "channel name required"}
        new_res = str(new_resolution or "").strip().lower()
        if not new_res:
            return {"ok": False, "error": "new_resolution required"}
        if new_res not in ALLOWED_REDOWNLOAD_RESOLUTIONS:
            return {"ok": False, "error": f"Unsupported resolution: {new_res}"}
        ch = subs_backend.get_channel({"name": name})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        from backend.sync import channel_folder_name as _cfn
        _folder_name = _cfn(ch)
        # Defensive: if the channel ended up with a blank name somehow,
        # `channel_folder_name` returns "_unnamed" and we'd redownload into
        # the graveyard folder. Mirrors the guard in sync_channel.
        if _folder_name == "_unnamed":
            return {"ok": False,
                    "error": "Channel name is blank \u2014 edit the channel "
                             "in Subs and set a name before redownloading."}
        folder = os.path.join(base, _folder_name)
        if not os.path.isdir(folder):
            return {"ok": False, "error": f"Channel folder missing: {folder}"}
        # Narrow to a year / month subfolder when requested — BEFORE the gate
        # below. Previously this ran AFTER the gate, so a scoped redownload
        # queued while a regular sync was running got stored with the whole-
        # channel folder + an empty scope_label and later drained as a WHOLE-
        # channel redownload (audit r2). The `_scan_local_files` walker handles
        # any folder path, so pointing it at a subfolder just narrows the set.
        scope_label = ""
        if isinstance(scope, dict) and scope.get("year"):
            y = str(scope["year"])
            sub = os.path.join(folder, y)
            if scope.get("month"):
                try:
                    m = int(scope["month"])
                    from backend.reorg import _MONTH_FOLDERS
                    mf = _MONTH_FOLDERS.get(m)
                    if mf:
                        sub = os.path.join(sub, mf)
                        scope_label = f"{y} / {mf}"
                    else:
                        scope_label = f"{y}"
                except Exception:
                    scope_label = f"{y}"
            else:
                scope_label = f"{y}"
            if not os.path.isdir(sub):
                return {"ok": False,
                        "error": f"Scope folder missing: {sub}"}
            folder = sub
        # Gate behavior:
        #   - If a regular (non-redownload) sync is running, refuse.
        #   - If a redownload is running, QUEUE this request so the
        #     worker picks it up after the current one finishes.
        #   - If nothing is running, start a worker that drains the
        #     queue sequentially.
        # Reported: right-clicking "Continue Redownload" on channel 2
        # while channel 1 was still redownloading silently failed.
        _sync_alive = bool(self._sync_thread and self._sync_thread.is_alive())
        if _sync_alive:
            _cur = self._queues.current_sync or {}
            # previously a regular-sync-in-flight refused
            # the redownload outright. Now we surface a clearer error
            # AND enqueue it on the redownload chain so a follow-up
            # "Start" click can drain it after the current sync ends.
            # (A fully-automatic hand-off would require sync_start_all
            # to drain _redwnl_pending on completion — left for a
            # follow-up since it crosses two worker abstractions.)
            if (_cur.get("kind") or "").lower() != "redownload":
                # Pre-check for duplicate enqueue against the same
                # channel URL so a user clicking multiple times doesn't
                # pile up the same redownload in the pending list.
                _ch_url = (ch.get("url") or "").strip()
                with self._redwnl_lock:
                    _already = any(
                        (p.get("ch") or {}).get("url") == _ch_url
                        for p in self._redwnl_pending) if _ch_url else False
                    if not _already:
                        self._redwnl_pending.append({
                            "ch": dict(ch),
                            "folder": folder,
                            "new_res": new_res,
                        "scope_label": scope_label,
                        "scope": scope,
                        "rd_task": dict(ch, kind="redownload",
                                         redownload_res=new_res,
                                         scope=scope),
                    })
                try: self._on_queue_changed()
                except Exception as e: _log.debug("swallowed: %s", e)
                # Branch on `_already` so the second click on the same
                # channel doesn't get the same "queued" message that
                # the first click got — the second click did NOT
                # actually enqueue, and the user deserves to know
                # (audit: channel_mixin H3).
                if _already:
                    return {"ok": False,
                            "error": "Redownload already queued for this channel."}
                return {"ok": False,
                        "error": "Sync pipeline running — redownload queued. "
                                 "It will start when the current sync ends."}
            # Fall through into the enqueue path below.

        # Build a queue item + the UI-visible task dict. (folder + scope_label
        # were already narrowed above, before the gate; `scope` is carried so
        # the sync-side drain can re-narrow correctly — audit r2.)
        _rd_task = dict(ch)
        _rd_task["kind"] = "redownload"
        _rd_task["redownload_res"] = new_res
        _rd_task["scope"] = scope
        _pending_item = {
            "ch": dict(ch),
            "folder": folder,
            "new_res": new_res,
            "scope_label": scope_label,
            "scope": scope,
            "rd_task": _rd_task,
        }

        with self._redwnl_lock:
            # Always enqueue to the internal chain.
            self._redwnl_pending.append(_pending_item)
            # Mirror to the sync-queue UI so the Sync Tasks popover
            # shows queued redownloads alongside the one running.
            try: self._queues.sync_enqueue(_rd_task)
            except Exception as e: _log.warning("redownload sync_enqueue failed; task won't appear in Tasks popover: %s", e)

            # If a worker is already draining the chain, we're done —
            # our item will get picked up when the current one
            # finishes. Reported: second "Continue Redownload" click
            # used to silently error with "Sync pipeline already
            # running"; it now queues and fires in turn.
            if _sync_alive:
                self._on_queue_changed()
                return {"ok": True, "queued": True, "resolution": new_res}

            # Nothing running: reset cancel/pause and spawn the worker.
            # Clear BOTH the threading.Event (pipeline-gate flag) AND
            # the QueueState flags (UI source-of-truth for the blink
            # icon + Pause/Resume button labels). `sync_start_all`
            # does this too — without it, the global pause/resume
            # button and the Sync Tasks popover's "Pause" button
            # stick in paused state even though this fresh redownload
            # is actively running. Reported: user saw the popover
            # showing a green ▶ "Resume" button while "Redownloading
            # ChannelName (480p)" was the active task.
            self._sync_cancel.clear()
            self._redwnl_cancel.clear()
            self._sync_pause.clear()
            self._sync_skip.clear()
            try: self._queues.set_sync_paused(False)
            except Exception as e: _log.debug("swallowed: %s", e)
            try:
                self._queues.set_gpu_paused(False)
                self._transcribe.resume()
            except Exception as e: _log.debug("swallowed: %s", e)

            def _worker():
                while True:
                    with self._redwnl_lock:
                        if not self._redwnl_pending:
                            break
                        item = self._redwnl_pending.pop(0)
                        # Fresh cancel state for EACH chain item — a
                        # per-channel cancel of item N must not abort
                        # items N+1.. at their first check. Done under
                        # _redwnl_lock so a global stop (which drains
                        # the list and sets the event under the same
                        # lock) can't interleave between pop and clear.
                        self._redwnl_cancel.clear()
                    # Remove the about-to-run item from the sync queue
                    # UI (moves it from "queued" to "running").
                    try:
                        self._queues.sync_remove(
                            item["rd_task"].get("url", ""))
                    except Exception as e:
                        _log.debug("swallowed: %s", e)
                    try:
                        self._run_redownload_one(
                            item["ch"], item["folder"],
                            item["new_res"], item["scope_label"])
                    except Exception as _re:
                        try: self._log_stream.emit_error(
                            f"Redownload crashed: {_re}")
                        except Exception as e: _log.debug("swallowed: %s", e)
                # Chain drained — final bookkeeping.
                self._on_queue_changed()
                try: self._autorun.notify_sync_done()
                except Exception as e: _log.warning("autorun notify_sync_done failed after redownload: %s", e)
                try: threading.Timer(0.5, self._on_queue_changed).start()
                except Exception as e: _log.debug("swallowed: %s", e)

            if not self._start_sync_thread_locked(_worker):
                # A regular sync or another redownload worker started
                # after our first idle check. Keep the pending item queued
                # and let the live worker / completion drain pick it up.
                self._on_queue_changed()
                return {"ok": True, "queued": True,
                        "resolution": new_res}
            self._on_queue_changed()
            return {"ok": True, "started": True, "resolution": new_res}


    def chan_fix_file_dates(self, identity):
        """Fix file mtimes for a channel's videos using .info.json upload dates.

        Lighter-weight than reorg — doesn't move files, only fixes dates so
        Recent / Browse sorts reflect YouTube upload order. Runs in a thread.
        """
        ch = subs_backend.get_channel(identity or {})
        if not ch:
            return {"ok": False, "error": "Channel not found"}
        cfg = load_config()
        base = (cfg.get("output_dir") or "").strip()
        if not base:
            return {"ok": False, "error": "output_dir not set"}
        from backend.sync import channel_folder_name as _cfn
        folder = os.path.join(base, _cfn(ch))

        # Refuse to stamp mtimes while sync / GPU / reorg is actively
        # writing into this channel folder. Without this guard, a
        # yt-dlp finalize (which sets mtime=now) racing with our
        # fix_file_dates pass left timestamps in an inconsistent
        # state (audit: channel_mixin.py:826). Match the same channel
        # checks used by reorg_channel_folder.
        ch_name = ch.get("name") or ch.get("folder", "")
        try:
            # QueueState exposes current_sync / current_gpu as dict ATTRIBUTES,
            # not methods. The old calls current_sync_channel() /
            # current_gpu_channel() don't exist — they raised, were swallowed
            # below, and the guard never fired (audit #3). Read the real channel
            # identity: a sync carries "name"/"folder", a GPU task carries
            # "channel".
            _cs = self._queues.current_sync or {}
            _cg = self._queues.current_gpu or {}
            cur_sync = (_cs.get("name") or _cs.get("folder") or "")
            cur_gpu  = (_cg.get("channel") or "")
        except Exception:
            cur_sync = cur_gpu = ""
        if ch_name and (cur_sync == ch_name or cur_gpu == ch_name):
            return {"ok": False,
                    "error": f"\"{ch_name}\" is currently being synced or "
                             f"transcribed; try again when that finishes."}

        # Single-slot guard — two concurrent date-fix passes stamping
        # mtimes (possibly on the same channel) would race each other.
        if getattr(self, "_fixdates_running", False):
            return {"ok": False,
                    "error": "A file-date fix is already running. Wait for "
                             "it to finish (or cancel it) first."}

        # Per-run event — _sync_cancel stays set after any stopped sync
        # until the next sync starts, which made Fix-file-dates abort
        # instantly in that window (same ghost-cancel Reorg had).
        # Stored on self so chan_fix_dates_cancel can actually set it;
        # before this the event was function-local and NOTHING could
        # ever cancel a running date fix (audit S4).
        _fixdates_cancel = threading.Event()
        self._fixdates_cancel = _fixdates_cancel
        self._fixdates_running = True
        def _run():
            try:
                reorg_backend.fix_file_dates(folder, self._log_stream,
                                             cancel_event=_fixdates_cancel)
            finally:
                try:
                    self._log_stream.flush()
                finally:
                    # Identity-compared clear (mirrors reorg_channel_folder)
                    # so a hypothetical newer run's event can't be wiped
                    # by this older run's teardown.
                    if getattr(self, "_fixdates_cancel", None) is _fixdates_cancel:
                        self._fixdates_cancel = None
                    self._fixdates_running = False

        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "started": True}


    def chan_fix_dates_cancel(self):
        """Stop an in-progress Fix-file-dates pass at its next file.
        Mirrors reorg_cancel — returns {ok, running} where `running`
        reflects whether a pass was actually active when cancel fired,
        so the UI can toast "cancelling" vs "nothing to cancel"."""
        was_running = bool(getattr(self, "_fixdates_running", False))
        ev = getattr(self, "_fixdates_cancel", None)
        if ev is not None:
            try:
                ev.set()
            except Exception as e:
                _log.debug("fix-dates cancel set failed: %s", e)
        return {"ok": True, "running": was_running}
