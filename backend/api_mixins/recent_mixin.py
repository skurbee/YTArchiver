"""
RecentMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They prefer AppServices when present, with legacy
private Api attributes kept as fallback state.
"""
from __future__ import annotations

import json
import os
import re
import time

from ._shared import _log
from backend.log import swallow
from backend.ytarchiver_config import config_is_writable, load_config, recent_for_ui, save_config
from backend import index as index_backend


class RecentMixin:
    def _recent_services(self):
        return getattr(self, "services", None)

    def _recent_config(self):
        services = self._recent_services()
        if services is not None:
            return services.fresh_config()
        cfg = getattr(self, "_config", None)
        if cfg is not None:
            return cfg
        return load_config()

    def _recent_save_config(self, cfg):
        services = self._recent_services()
        if services is not None:
            return services.save_config(cfg)
        return save_config(cfg)

    def _recent_log_stream(self):
        services = self._recent_services()
        stream = (getattr(services, "log_stream", None)
                  if services is not None else None)
        return stream if stream is not None else self._log_stream


    def get_recent_downloads(self):
        """Return real recent-downloads from config. Empty list when none.

        Earlier builds fell back to a synthetic sample set which populated the
        Recent tab with fake videos the user couldn't delete. Removed.
        """
        return recent_for_ui(self._recent_config())


    def list_all_videos(self, sort="recent", limit=60, offset=0, query=""):
        """Paginated global video list for the Videos view — every video in
        the archive, sorted by recent/newest/oldest/title/channel/views/likes/
        largest. `query` filters by title/channel substring. Returns
        {rows, has_more, offset}."""
        try:
            _limit = max(1, min(int(limit or 60), 1000))
        except (TypeError, ValueError):
            _limit = 60
        try:
            _offset = max(0, int(offset or 0))
        except (TypeError, ValueError):
            _offset = 0
        try:
            # Guarantee the very first Browse request sees the legacy download
            # history even if the background startup backfill has not run yet.
            # Idempotent; subsequent calls update zero rows.
            if str(sort or "recent").lower() == "recent":
                index_backend.backfill_downloaded_ts_from_recent(
                    self._recent_config().get("recent_downloads", []))
            # Mark this as a foreground Browse query so the startup sweep
            # yields the Z: pool to it — a cold Videos open
            # does up to 60 channel-wide thumbnail walks and must not run
            # head-to-head with the background walkers on the same disk.
            with index_backend.foreground_browse():
                return index_backend.list_all_videos(
                    sort=str(sort or "recent"),
                    limit=_limit, offset=_offset,
                    query=str(query or ""))
        except Exception as e:
            return {"rows": [], "has_more": False, "offset": _offset,
                    "error": str(e)}


    def clear_recent_downloads(self):
        """Empty the recent_downloads list. Files on disk are untouched.

        Wired to the Recent tab's "Clear list" button. The previous .txt
        report noted the button did nothing — the API had been missing
        since the pywebview port; the Tkinter version had its own
        equivalent. Returns {ok: bool, error?: str}.
        """
        try:
            cfg = self._recent_config()
            cfg["recent_downloads"] = []
            ok = self._recent_save_config(cfg)
            if not ok:
                return {"ok": False, "error": "Config write failed."}
            try: self._reload_config()
            except Exception as e: swallow("config reload after clear", e)
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def _push_recent_refresh(self, channel=None):
        """Re-fetch recent_downloads and push to the UI's Browse grids.

        Called from backend.sync._record_recent_download every time a new
        video lands, so Browse updates live ("does the Recents tab not auto
        update/refresh when a download happens?"). `channel` (when known)
        lets the UI target the matching channel grid. Safe no-op when the
        window isn't ready yet.
        """
        if self._window is None:
            return
        try:
            import json as _json
            # Reload config fresh since _record_recent_download just wrote
            # to disk; self._config may be stale.
            try: self._reload_config()
            except Exception as e: swallow("config reload on recent push", e)
            # A new download just landed (and was registered in the index).
            # Fan out to every loaded Browse grid (all-Videos, the current
            # channel grid, and the Manual view) so the new video appears
            # live AND stays preloaded even if Browse isn't the active tab —
            # no "pop-in" when the user switches back. Each grid is a no-op
            # if it isn't showing the new video.
            _ch_arg = _json.dumps(channel or "")
            self._window.evaluate_js(
                "window._onBrowseDownloadLanded && "
                f"window._onBrowseDownloadLanded({_ch_arg});")
        except Exception as e:
            # Best-effort — never let a UI push crash the download pipeline.
            try:
                self._recent_log_stream().emit_dim(
                    f"(recent refresh push failed: {e})")
            except Exception as e: swallow("recent-push dim emit", e)


    # ─── Recent tab actions ────────────────────────────────────────────

    def _recent_identity(self, title_or_payload, channel=None):
        if isinstance(title_or_payload, dict):
            p = title_or_payload
            return {
                "title": str(p.get("title") or "").strip(),
                "channel": str(p.get("channel") or "").strip(),
                "filepath": str(p.get("filepath") or "").strip(),
                "video_id": str(p.get("video_id")
                                or p.get("videoId") or "").strip(),
            }
        return {
            "title": str(title_or_payload or "").strip(),
            "channel": str(channel or "").strip(),
            "filepath": "",
            "video_id": "",
        }

    @staticmethod
    def _norm_recent_path(path: str) -> str:
        try:
            return os.path.normcase(os.path.normpath(path or ""))
        except Exception:
            return path or ""

    def _recent_find_entry(self, ident: dict):
        cfg = self._recent_config()
        rows = cfg.get("recent_downloads", []) or []
        target_fp = self._norm_recent_path(ident.get("filepath", ""))
        target_vid = ident.get("video_id", "")
        if target_fp:
            for r in rows:
                if self._norm_recent_path(r.get("filepath", "")) == target_fp:
                    return r, cfg
        if target_vid:
            for r in rows:
                if (r.get("video_id") or "").strip() == target_vid:
                    return r, cfg
        title = ident.get("title", "")
        channel = ident.get("channel", "")
        if title or channel:
            matches = [
                r for r in rows
                if r.get("title") == title and r.get("channel") == channel
            ]
            if len(matches) == 1:
                return matches[0], cfg
            if len(matches) > 1:
                return {"_ambiguous": True, "matches": len(matches)}, cfg
        return None, cfg

    def _recent_lookup_path_from_identity(self, title_or_payload,
                                          channel=None):
        ident = self._recent_identity(title_or_payload, channel)
        if ident.get("filepath"):
            fp = ident["filepath"]
            if os.path.isfile(fp):
                return fp
        entry, _cfg = self._recent_find_entry(ident)
        if isinstance(entry, dict) and entry.get("_ambiguous"):
            return None
        if entry:
            fp = entry.get("filepath", "") or ""
            if fp and os.path.isfile(fp):
                return fp
        return self._recent_lookup_path(ident.get("title", ""),
                                        ident.get("channel", ""))

    def _recent_is_ambiguous_legacy(self, ident: dict) -> bool:
        if ident.get("filepath") or ident.get("video_id"):
            return False
        entry, _cfg = self._recent_find_entry(ident)
        return isinstance(entry, dict) and entry.get("_ambiguous")

    def _recent_lookup_path(self, title, channel):
        """Find the on-disk filepath for a Recent row by title + channel.

        Resolution order:
          1. Config `recent_downloads` explicit path (if file still exists)
          2. Index DB `videos.filepath` by title + channel
          3. Walk the channel folder by title / [videoId] / fuzzy ASCII
             match via `utils.try_find_by_title` — recovers files the user
             manually moved between year/month split layouts.
        """
        cfg = self._recent_config()
        video_id_hint = ""
        # Iterate ALL matching entries — old code returned at the
        # first stored_path that existed, but with duplicates from
        # re-download cycles the FIRST match wasn't always the
        # newest (audit: recent_mixin.py:88-93). Pick the entry
        # whose file exists with the most recent mtime.
        _candidates = []
        for r in cfg.get("recent_downloads", []):
            if r.get("title") == title and r.get("channel") == channel:
                video_id_hint = r.get("video_id", "") or video_id_hint
                _sp = r.get("filepath", "") or ""
                if _sp:
                    _candidates.append(_sp)
        _best = None
        _best_mt = -1.0
        for _sp in _candidates:
            try:
                if os.path.isfile(_sp):
                    _mt = os.path.getmtime(_sp)
                    if _mt > _best_mt:
                        _best_mt = _mt
                        _best = _sp
            except OSError:
                continue
        if _best:
            return _best
        stored_path = _candidates[0] if _candidates else ""
        # DB fallback
        try:
            vids = index_backend.list_recent_videos(limit=500, channel=channel)
            for v in vids:
                if v.get("title") == title:
                    if not video_id_hint:
                        video_id_hint = v.get("video_id", "") or ""
                    if not stored_path:
                        stored_path = v.get("filepath", "") or stored_path
                    if v.get("filepath") and os.path.isfile(v["filepath"]):
                        return v["filepath"]
        except Exception as e:
            swallow("recent-path lookup", e)
        # Moved-file recovery — walk the channel folder by title / videoId
        try:
            from backend.utils import try_locate_moved_file
            base = (cfg.get("output_dir") or "").strip()
            if base and channel:
                from backend.sync import channel_folder_name as _cfn
                # Find the channel record to derive the folder name
                ch = None
                for c in cfg.get("channels", []):
                    if (c.get("name") == channel or c.get("folder") == channel):
                        ch = c
                        break
                ch_folder = os.path.join(base, _cfn(ch) if ch else channel)
                found = try_locate_moved_file(stored_path, title, ch_folder,
                                               video_id_hint)
                if found and os.path.isfile(found):
                    return found
        except Exception as e:
            swallow("moved-file locate", e)
        return None


    def recent_play(self, title, channel=None):
        fp = self._recent_lookup_path_from_identity(title, channel)
        if not fp:
            return {"ok": False, "error": "File not found"}
        return self.browse_open_video(fp)


    def recent_requeue(self, title, channel=None):
        """Re-download the YouTube URL stored for this Recent entry.
        Mirrors OLD YTArchiver.py Recent right-click "Re-queue download".

        Returns {ok, queued} or {ok:False, error}.
        """
        try:
            ident = self._recent_identity(title, channel)
            r, _cfg = self._recent_find_entry(ident)
            if isinstance(r, dict) and r.get("_ambiguous"):
                return {"ok": False,
                        "error": "Recent entry is ambiguous; select a row with filepath/video_id."}
            if r:
                url = (r.get("video_url") or "").strip()
                if not url:
                    vid = (r.get("video_id")
                           or ident.get("video_id") or "").strip()
                    if vid:
                        url = f"https://www.youtube.com/watch?v={vid}"
                if not url:
                    return {"ok": False,
                            "error": "No URL saved for this recent entry."}
                # Delegate to single-video download. Uses the user's
                # saved video_out_dir + resolution defaults.
                return self.archive_single_video(url, options={})
            return {"ok": False, "error": "Recent entry not found."}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def recent_resolve(self, title, channel=None):
        """Return {ok, filepath, video_id} for a Recent row, using the same
        three-step lookup as recent_play (config → DB → fuzzy walk). Used by
        the right-click "Play video" action to hand off to the Browse Watch
        view instead of spawning VLC."""
        ident = self._recent_identity(title, channel)
        fp = self._recent_lookup_path_from_identity(ident)
        if not fp:
            return {"ok": False, "error": "File not found"}
        # Best-effort video_id lookup — check config.recent_downloads first,
        # fall back to the FTS DB row.
        vid = ""
        try:
            cfg = self._recent_config()
            for r in cfg.get("recent_downloads", []):
                if (ident.get("filepath")
                        and self._norm_recent_path(r.get("filepath", ""))
                        != self._norm_recent_path(ident["filepath"])):
                    continue
                if (ident.get("video_id")
                        and (r.get("video_id") or "").strip()
                        != ident["video_id"]):
                    continue
                if (ident.get("filepath") or ident.get("video_id")
                        or (r.get("title") == ident.get("title")
                            and r.get("channel") == ident.get("channel"))):
                    vid = (r.get("video_id") or "").strip()
                    if not vid:
                        # parse from video_url if present
                        import re as _re
                        m = _re.search(r"[?&]v=([A-Za-z0-9_-]{11})",
                                       r.get("video_url") or "")
                        if m: vid = m.group(1)
                    break
        except Exception as e:
            swallow("recent video-id lookup", e)
        if not vid:
            # Reader connection so this fallback doesn't queue behind
            # writers during startup sweep / ingest.
            try:
                from backend import index as _idx
                rconn = _idx._reader_open()
                if rconn is not None:
                    with _idx._reader_lock:
                        row = rconn.execute(
                            "SELECT video_id FROM videos WHERE title=? AND channel=? "
                            "ORDER BY added_ts DESC LIMIT 1",
                            (ident.get("title", ""),
                             ident.get("channel", ""))).fetchone()
                    if row and row[0]:
                        vid = row[0]
            except Exception as e:
                swallow("index video-id lookup", e)
        return {"ok": True, "filepath": fp, "video_id": vid}


    def recent_show_in_explorer(self, title, channel=None):
        fp = self._recent_lookup_path_from_identity(title, channel)
        if not fp:
            return {"ok": False, "error": "File not found"}
        return self.browse_show_in_explorer(fp)


    def recent_open_youtube(self, title, channel=None):
        """Open the YouTube page for this recent video (if we have video_id)."""
        import re as _re
        import webbrowser
        # Prefer the video_id stashed on the recent_downloads entry —
        # single-video downloads via archive_single_video write files
        # without a `[VIDEOID]` suffix in the filename, so the regex
        # path used to always fail for those rows even though the ID
        # was sitting right there in config (audit: recent_mixin.py:
        # 213-223).
        try:
            ident = self._recent_identity(title, channel)
            vid = ident.get("video_id", "")
            if vid and _re.fullmatch(r"[A-Za-z0-9_-]{11}", vid):
                webbrowser.open(f"https://www.youtube.com/watch?v={vid}")
                return {"ok": True}
            r, _cfg = self._recent_find_entry(ident)
            if isinstance(r, dict) and r.get("_ambiguous"):
                return {"ok": False,
                        "error": "Recent entry is ambiguous; select a row with video_id."}
            if r:
                vid = (r.get("video_id") or "").strip()
                if vid and _re.fullmatch(r"[A-Za-z0-9_-]{11}", vid):
                    webbrowser.open(f"https://www.youtube.com/watch?v={vid}")
                    return {"ok": True}
        except Exception as _e:
            swallow("open-youtube url", _e)
        # Filename-suffix fallback for older entries that pre-date the
        # video_id field.
        fp = self._recent_lookup_path_from_identity(title, channel)
        if fp:
            m = _re.search(r"\[([A-Za-z0-9_-]{11})\]", os.path.basename(fp))
            if m:
                webbrowser.open(f"https://www.youtube.com/watch?v={m.group(1)}")
                return {"ok": True}
        return {"ok": False, "error": "No video ID available"}


    def recent_delete_file(self, title, channel=None):
        """Move the file to app trash + remove from recent_downloads list."""
        ident = self._recent_identity(title, channel)
        if self._recent_is_ambiguous_legacy(ident):
            return {"ok": False,
                    "error": "Recent entry is ambiguous; select a row with filepath/video_id."}
        fp = self._recent_lookup_path_from_identity(ident)
        if not fp:
            return {"ok": False, "error": "File not found"}
        # Defense-in-depth: refuse to os.remove a path resolving OUTSIDE the
        # archive roots this app manages (audit: recent_mixin containment).
        from backend.services.file_ops import safe_trash_video_file
        trashed = safe_trash_video_file(
            fp, require_config_writable=True, reason="recent_delete_file")
        if not trashed.get("ok"):
            return trashed
        # Refuse the destructive os.remove if config writes are blocked
        # — otherwise we delete the file but can't update the
        # recent_downloads list, leaving the user with a stale entry
        # pointing at a missing file with no way to clean it up
        # (audit: recent_mixin H22).
        # Drop sidecars. audit F-24 list lives in utils.delete_video_sidecars.
        # Mirror video_mixin.video_delete_file's index cleanup so the
        # FTS / videos rows tied to this filepath are dropped too.
        # Without this, Browse + Search kept returning "file not
        # found" hits for the trashed file (audit: recent_mixin.py:
        # 226-246).
        index_warning = ""
        try:
            from backend import index as _idx
            # FTS-safe, video_id-keyed segment removal (works in the
            # aggregated layout; the old per-video jsonl_path DELETE
            # matched zero rows there and skipped the FTS5 'delete'
            # sync on legacy rows). Must run BEFORE the videos row is
            # dropped — the helper resolves video_id from it.
            _idx.delete_segments_for_video(fp)
            _conn = _idx._open()
            if _conn is not None:
                with _idx._db_lock:
                    _conn.execute(
                        "DELETE FROM videos WHERE filepath = ? COLLATE NOCASE",
                        (fp,))
                    _conn.commit()
        except Exception as _e:
            index_warning = (
                "File moved to trash but index cleanup failed; run Rescan "
                f"to remove stale Browse/Search entries. ({_e})"
            )
            _log.debug("recent_delete_file index cleanup failed: %s", _e)
        # Remove from recent_downloads (if writable)
        if config_is_writable():
            try:
                target_fp = self._norm_recent_path(ident.get("filepath") or fp)
                target_vid = ident.get("video_id", "")
                from backend.ytarchiver_config import config_transaction as _ctx
                with _ctx() as cfg:
                    cfg["recent_downloads"] = [
                        r for r in cfg.get("recent_downloads", [])
                        if not (
                            (target_fp and self._norm_recent_path(
                                r.get("filepath", "")) == target_fp)
                            or (target_vid and (r.get("video_id") or "").strip()
                                == target_vid)
                            or (not target_fp and not target_vid
                                and r.get("title") == ident.get("title")
                                and r.get("channel") == ident.get("channel"))
                        )
                    ]
            except Exception:
                return {"ok": False, "file_trashed": True,
                        "error": "File moved to trash but config write failed; recent_downloads may show stale entry",
                        "trashed_file_path": trashed.get("trashed_file_path"),
                        "trashed_folder_path": trashed.get("trashed_folder_path")}
        if index_warning:
            return {"ok": False, "file_trashed": True,
                    "cleanup_failed": True, "error": index_warning,
                    "warning": index_warning,
                    "trashed_file_path": trashed.get("trashed_file_path"),
                    "trashed_folder_path": trashed.get("trashed_folder_path")}
        return {"ok": True,
                "trashed_file_path": trashed.get("trashed_file_path"),
                "trashed_folder_path": trashed.get("trashed_folder_path")}
