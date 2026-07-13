"""
BrowseMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They prefer AppServices when present for config and
log dependencies, with legacy private Api attributes kept as fallback
state.
"""
from __future__ import annotations

from collections import OrderedDict

from backend.services import file_ops

import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, List

from ._shared import _log, webview
from backend.log import swallow
from backend.ytarchiver_config import load_config
from backend import archive_scan
from backend import index as index_backend


_METADATA_DRAWER_CACHE_LOCK = threading.Lock()
_METADATA_DRAWER_CACHE: OrderedDict[tuple[str, str], dict[str, Any]] = OrderedDict()
_METADATA_DRAWER_CACHE_MAX = 128
_TRANSCRIPT_SOURCE_SCAN_BYTES = 5 * 1024 * 1024
_MANUAL_THUMB_BACKFILL_LOCK = threading.Lock()
_MANUAL_THUMB_BACKFILL_INFLIGHT: set[str] = set()
_MANUAL_DURATION_BACKFILL_LOCK = threading.Lock()
_MANUAL_DURATION_BACKFILL_INFLIGHT: set[str] = set()


def _iter_transcript_header_lines(path: str):
    """Yield header lines from a bounded tail scan of a Transcript.txt file."""
    try:
        size = os.path.getsize(path)
    except OSError:
        size = 0
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        if size > _TRANSCRIPT_SOURCE_SCAN_BYTES:
            fh.seek(max(0, size - _TRANSCRIPT_SOURCE_SCAN_BYTES))
            if fh.tell() > 0:
                fh.readline()
        for line in fh:
            if line.startswith("==="):
                yield line


def _metadata_file_signature(path: str) -> tuple[int, int] | None:
    try:
        st = os.stat(path)
        return st.st_mtime_ns, st.st_size
    except OSError:
        return None


def _metadata_drawer_cache_get(video_id: str, channel: str) -> dict | None:
    key = (video_id, channel or "")
    with _METADATA_DRAWER_CACHE_LOCK:
        cached = _METADATA_DRAWER_CACHE.get(key)
        if not cached:
            return None
        source = str(cached.get("source") or "")
        sig = _metadata_file_signature(source)
        if sig is not None and sig == cached.get("sig"):
            _METADATA_DRAWER_CACHE.move_to_end(key)
            return {
                "ok": True,
                "meta": cached.get("meta") or {},
                "source": source,
            }
        _METADATA_DRAWER_CACHE.pop(key, None)
    return None


def _metadata_drawer_cache_put(video_id: str, channel: str,
                               source: str, meta: dict) -> None:
    sig = _metadata_file_signature(source)
    if sig is None:
        return
    key = (video_id, channel or "")
    with _METADATA_DRAWER_CACHE_LOCK:
        _METADATA_DRAWER_CACHE[key] = {
            "source": source,
            "sig": sig,
            "meta": meta,
        }
        _METADATA_DRAWER_CACHE.move_to_end(key)
        while len(_METADATA_DRAWER_CACHE) > _METADATA_DRAWER_CACHE_MAX:
            _METADATA_DRAWER_CACHE.popitem(last=False)


def _real_norm(path: str) -> str:
    try:
        return os.path.normcase(os.path.realpath(os.path.normpath(path))).rstrip("/\\")
    except (OSError, ValueError, TypeError):
        return ""


def _same_or_under(path: str, root: str) -> bool:
    p = _real_norm(path)
    r = _real_norm(root)
    if not p or not r:
        return False
    return p == r or p.startswith(r + os.sep) or p.startswith(r + "/")


def _is_system_temp_path(path: str) -> bool:
    candidates = {
        tempfile.gettempdir(),
        os.environ.get("TEMP", ""),
        os.environ.get("TMP", ""),
        os.path.join(os.environ.get("LOCALAPPDATA", ""), "Temp"),
    }
    return any(_same_or_under(path, c) for c in candidates if c)


def _fmt_manual_duration(seconds: float | int | str | None) -> str:
    try:
        total = int(float(seconds or 0))
    except (TypeError, ValueError):
        return ""
    if total <= 0:
        return ""
    h, m, s = total // 3600, (total % 3600) // 60, total % 60
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _probe_manual_duration_seconds(filepath: str) -> float | None:
    if not filepath or not os.path.isfile(filepath):
        return None
    try:
        from backend.process_runner import find_ffprobe
        ffprobe = find_ffprobe()
    except Exception:
        ffprobe = None
    if not ffprobe:
        return None
    try:
        from backend.subprocess_util import (
            make_startupinfo,
            subprocess_creationflags,
        )
        proc = subprocess.run(
            [ffprobe, "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", filepath],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
            encoding="utf-8",
            errors="replace",
            startupinfo=make_startupinfo(),
            creationflags=subprocess_creationflags(),
        )
    except Exception as e:
        _log.debug("manual duration probe failed for %r: %s", filepath, e)
        return None
    try:
        dur = float((proc.stdout or "").strip())
    except (TypeError, ValueError):
        return None
    return dur if dur > 0 else None


def _path_is_registered_video(filepath: str) -> bool:
    """True if `filepath` exactly matches a registered video in the index.

    A single/manual download can be saved anywhere the user chose (custom
    "Save to" folder), outside the configured archive roots. Those files are
    still app-managed — every download is registered in the `videos` table
    with its full path — so we treat an exact index match as authorization to
    play/open it. Exact (case-insensitive) match only: a crafted JS path that
    isn't a known download stays rejected.
    """
    try:
        from backend.index import _reader_lock, _reader_open
        rconn = _reader_open()
        if rconn is None:
            return False
        with _reader_lock:
            row = rconn.execute(
                "SELECT 1 FROM videos WHERE filepath=? COLLATE NOCASE LIMIT 1",
                (filepath,),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _guard_browse_launch_path(filepath: str, *, require_file: bool) -> dict:
    fp = os.path.normpath(filepath or "")
    if not fp:
        return {"ok": False, "error": "No path provided"}
    exists = os.path.isfile(fp) if require_file else os.path.exists(fp)
    if not exists:
        return {"ok": False, "error": f"Not found: {fp}"}
    guard = file_ops.assert_within_managed_roots(fp)
    if guard.get("ok"):
        return {"ok": True, "path": fp}
    # Not under a configured root — but manual downloads can be saved to a
    # custom location. They're still ours if the index has them, so allow an
    # exact registered-video match (covers downloads saved anywhere on disk,
    # existing or future) rather than rejecting them as "outside the archive".
    if _path_is_registered_video(fp):
        return {"ok": True, "path": fp}
    return guard


class BrowseMixin:
    def _browse_services(self):
        return getattr(self, "services", None)

    def _browse_config(self):
        services = self._browse_services()
        if services is not None:
            return services.fresh_config()
        cfg = getattr(self, "_config", None)
        if cfg is not None:
            return cfg
        return load_config()

    def _browse_fresh_config(self):
        services = self._browse_services()
        if services is not None:
            return services.fresh_config()
        return load_config()

    def _browse_log_stream(self):
        services = self._browse_services()
        stream = (getattr(services, "log_stream", None)
                  if services is not None else None)
        return stream if stream is not None else self._log_stream

    def _queue_manual_local_thumbnail_backfill(
            self, candidates: list[dict[str, Any]]) -> None:
        win = getattr(self, "_window", None)
        if win is None or not candidates:
            return
        todo: list[dict[str, str]] = []
        with _MANUAL_THUMB_BACKFILL_LOCK:
            for r in candidates:
                fp = r.get("filepath") or ""
                if not fp:
                    continue
                key = os.path.normcase(os.path.normpath(fp))
                if key in _MANUAL_THUMB_BACKFILL_INFLIGHT:
                    continue
                _MANUAL_THUMB_BACKFILL_INFLIGHT.add(key)
                todo.append({
                    "key": key,
                    "filepath": fp,
                    "title": r.get("title") or "",
                    "video_id": r.get("video_id") or "",
                })
        if not todo:
            return

        try:
            stream = self._browse_log_stream()
        except Exception:
            stream = None

        def _run():
            try:
                from backend import index as _idx
                from backend.thumbnails import (
                    _ensure_thumbnails_dir,
                    _generate_local_thumbnail,
                )

                def _push_ready(item, tp, source=""):
                    ready = {
                        "filepath": item["filepath"],
                        "thumbnail_url": _idx._file_url(tp),
                        "thumbnail_source": source,
                    }
                    try:
                        import json as _json
                        payload = _json.dumps([ready])
                        win.evaluate_js(
                            "window._manualThumbsReady && "
                            f"window._manualThumbsReady({payload});")
                    except Exception as e:
                        _log.debug("manual thumbnail push failed: %s", e)

                # Pass 1 is lookup-only. Do not let one expensive local
                # generation block later cards whose sidecars already exist.
                missing = []
                for item in todo:
                    fp = item["filepath"]
                    if not os.path.isfile(fp):
                        continue
                    video_id = item["video_id"]
                    tp = _idx.find_thumbnail(fp, video_id)
                    if tp:
                        source = ("local" if ".local." in
                                  os.path.basename(tp).lower() else "")
                        _push_ready(item, tp, source)
                    else:
                        missing.append(item)

                # Pass 2 handles true misses. Generation can be slow, but all
                # ready sidecars above are already visible by this point.
                for item in missing:
                    fp = item["filepath"]
                    thumb_dir = _ensure_thumbnails_dir(os.path.dirname(fp))
                    tp = _generate_local_thumbnail(
                        fp, thumb_dir, item["title"], item["video_id"],
                        stream=stream)
                    if tp:
                        _push_ready(item, tp, "local")
            finally:
                with _MANUAL_THUMB_BACKFILL_LOCK:
                    for item in todo:
                        _MANUAL_THUMB_BACKFILL_INFLIGHT.discard(item["key"])

        threading.Thread(target=_run, name="manual-local-thumbs",
                         daemon=True).start()

    def _queue_manual_duration_backfill(
            self, candidates: list[dict[str, Any]]) -> None:
        win = getattr(self, "_window", None)
        if win is None or not candidates:
            return
        todo: list[dict[str, Any]] = []
        with _MANUAL_DURATION_BACKFILL_LOCK:
            for r in candidates:
                fp = r.get("filepath") or ""
                if not fp:
                    continue
                key = os.path.normcase(os.path.normpath(fp))
                if key in _MANUAL_DURATION_BACKFILL_INFLIGHT:
                    continue
                _MANUAL_DURATION_BACKFILL_INFLIGHT.add(key)
                todo.append({
                    "key": key,
                    "filepath": fp,
                    "title": r.get("title") or "",
                    "channel": r.get("channel") or "",
                    "video_id": r.get("video_id") or "",
                    "register_if_missing": bool(
                        r.get("register_if_missing")),
                })
        if not todo:
            return

        def _run():
            try:
                from concurrent.futures import ThreadPoolExecutor, as_completed
                from backend import index as _idx

                # ffprobe mostly reads container headers. A small bounded pool
                # repairs legacy/manual rows far faster than the old serial
                # 10-second-per-file loop without flooding pooled storage.
                def _probe(item):
                    fp = item["filepath"]
                    dur = _probe_manual_duration_seconds(fp)
                    return item, dur

                workers = min(4, len(todo))
                with ThreadPoolExecutor(
                        max_workers=workers,
                        thread_name_prefix="manual-duration-probe") as pool:
                    futures = [pool.submit(_probe, item) for item in todo]
                    completed = as_completed(futures)
                    for future in completed:
                        try:
                            item, dur = future.result()
                        except Exception as e:
                            _log.debug("manual duration worker failed: %s", e)
                            continue
                        fp = item["filepath"]
                        label = _fmt_manual_duration(dur)
                        if not label:
                            continue
                        persisted = False
                        try:
                            persisted = _idx.set_video_duration(fp, dur)
                        except Exception as e:
                            _log.debug(
                                "manual duration persist failed for %r: %s",
                                fp, e)
                        # Root-folder discovery is the safety net for loose
                        # manual videos absent from the catalog. Merely probing
                        # those files left nowhere to retain duration_s, so
                        # every launch forgot it and showed blanks again.
                        if item.get("register_if_missing") and not persisted:
                            try:
                                _idx.register_video(
                                    fp,
                                    item.get("channel") or "Single Videos",
                                    item.get("title") or os.path.splitext(
                                        os.path.basename(fp))[0],
                                    tx_status="pending",
                                    video_id=item.get("video_id") or None,
                                    duration_secs=dur,
                                )
                            except Exception as e:
                                _log.debug(
                                    "manual fallback registration failed for "
                                    "%r: %s", fp, e)
                        # Publish each result as soon as it is known instead of
                        # waiting for the slowest ffprobe in the whole page.
                        try:
                            import json as _json
                            payload = _json.dumps([
                                {"filepath": fp, "duration": label}
                            ])
                            win.evaluate_js(
                                "window._manualDurationsReady && "
                                f"window._manualDurationsReady({payload});")
                        except Exception as e:
                            _log.debug("manual duration push failed: %s", e)
            finally:
                with _MANUAL_DURATION_BACKFILL_LOCK:
                    for item in todo:
                        _MANUAL_DURATION_BACKFILL_INFLIGHT.discard(item["key"])

        threading.Thread(target=_run, name="manual-local-durations",
                         daemon=True).start()


    # ─── Browse tab (reads from transcription_index.db) ────────────────

    def browse_list_channels(self):
        """Return a list of channels with video counts + avatar/banner URLs.

        Avatar + banner paths are filled in from
        `<channel>/.ChannelArt/{avatar,banner}.jpg` when the files exist
        (dropped there by `chan_fetch_art` / metadata sweep). The frontend
        renders the avatar in the channel-grid card background.
        """
        # Foreground Browse query (the Channels grid) — make the startup
        # sweep yield the Z: pool while this loads, and keep this
        # bridge call short so the window can close promptly if the user
        # quits mid-load. Without the guard this raced the sweep's thumbnail
        # backfill on the same disk and took minutes.
        with index_backend.foreground_browse():
            return self._browse_list_channels_impl()

    def _browse_list_channels_impl(self):
        cfg = self._browse_fresh_config()
        channels = cfg.get("channels", [])
        cache = archive_scan.load_disk_cache()
        base = (cfg.get("output_dir") or "").strip()
        from backend.channel_art import (
            avatar_path_for,
            banner_path_for,
            ensure_avatar_thumb,
            ensure_banner_thumb,
        )
        from backend.index import _file_url
        from backend.sync import channel_folder_name as _cfn
        # Same first-use guarantee as the global Videos view: channel sort by
        # Recently downloaded must not race the startup history migration.
        try:
            index_backend.backfill_downloaded_ts_from_recent(
                cfg.get("recent_downloads", []))
        except Exception as e:
            _log.debug("download timestamp seed failed: %s", e)
        # Per-channel most-recent *completed download* timestamp. `added_ts`
        # is discovery/index time and can jump today when a rescan finds an
        # old file, which made 15-year-old channels sort as freshly downloaded.
        last_added: dict[str, float] = {}
        try:
            from backend import index as _idx
            _conn = _idx._reader_open()
            if _conn is not None:
                with _idx._reader_lock:
                    for _cn, _mx in _conn.execute(
                            "SELECT channel, MAX(downloaded_ts) FROM videos "
                            "WHERE is_duplicate_of IS NULL AND "
                            "COALESCE(availability, 'available')='available' "
                            "GROUP BY channel"):
                        if _cn:
                            last_added[_cn.strip().lower()] = float(_mx or 0)
        except Exception as e:
            _log.debug("last_added query failed: %s", e)
        out = []
        for ch in channels:
            st = archive_scan.stats_for_channel(ch, cache)
            name = ch.get("name") or ch.get("folder", "")
            # Resolve the channel folder and prefer the cached small
            # thumbs (ensure_* creates them lazily if missing). The
            # full-resolution banners are 2+ MP / ~350 KB each; decoding
            # 100 of them on the grid would stall scroll rendering.
            # Thumbs are ~30 KB, decode in ~1 ms, render at the card's
            # real display size. Falls back to the full-res original if
            # Pillow isn't available or the thumbnail write fails.
            avatar_url = None
            banner_url = None
            if base:
                folder = os.path.join(base, _cfn(ch))
                # ensure_* can raise on a transient I/O
                # error (Pillow missing, .ChannelArt locked, etc).
                # If we don't wrap them, the fallback to *_path_for
                # is unreachable and the grid shows a blank card
                # even when the full-res original exists on disk.
                try:
                    bp = ensure_banner_thumb(folder)
                except Exception:
                    bp = None
                if not bp:
                    bp = banner_path_for(folder)
                try:
                    ap = ensure_avatar_thumb(folder)
                except Exception:
                    ap = None
                if not ap:
                    ap = avatar_path_for(folder)
                if ap: avatar_url = _file_url(ap)
                if bp: banner_url = _file_url(bp)
            out.append({
                "name": name,
                "folder": name,
                "url": ch.get("url", ""),
                "n_vids": st["n_vids"],
                "size_bytes": st["size_bytes"],
                "size_gb": st["size_gb"],
                "size": archive_scan._fmt_size(st["size_bytes"]),
                "last_added_ts": last_added.get((name or "").strip().lower(), 0),
                "avatar_url": avatar_url,
                "banner_url": banner_url,
                # Pending counters for live-count context-menu labels.
                # folder-menu labels.
                "transcription_pending": int(ch.get("transcription_pending") or 0),
                "metadata_pending": int(ch.get("metadata_pending") or 0),
            })
        out.sort(key=lambda c: (c["name"] or "").lower())
        return out


    def browse_week_summary(self, days=7):
        """Return {new_videos, new_channels, total_channels} for the summary bar.

        - new_videos: count of videos with added_ts within N days
        - new_channels: count of distinct channels containing those new videos
        - total_channels: len(config['channels'])
        """
        try:
            cfg = self._browse_fresh_config()
            total_channels = len(cfg.get("channels", []))
            with index_backend.foreground_browse():
                recent = index_backend.new_videos_in_last_n_days(int(days or 7))
            return {
                "ok": True,
                "new_videos": recent.get("videos", 0),
                "new_channels": recent.get("channels", 0),
                "total_channels": total_channels,
                "channel_list": recent.get("channel_list", []),
            }
        except Exception as e:
            return {"ok": False, "error": str(e),
                    "new_videos": 0, "new_channels": 0, "total_channels": 0}


    def browse_list_videos(self, channel, sort="newest", limit=500):
        """List videos in a channel from the index DB."""
        # Foreground Browse query — make the startup sweep yield
        # the Z: pool while this user-initiated channel grid loads.
        with index_backend.foreground_browse():
            return index_backend.list_videos_for_channel(channel, sort=sort, limit=limit)

    def browse_list_videos_page(self, channel, sort="newest", limit=120,
                                offset=0, query=""):
        """Paginated channel videos for the default ungrouped Browse grid."""
        try:
            _limit = max(1, min(int(limit or 120), 500))
        except (TypeError, ValueError):
            _limit = 120
        try:
            _offset = max(0, int(offset or 0))
        except (TypeError, ValueError):
            _offset = 0
        try:
            with index_backend.foreground_browse():
                return index_backend.list_videos_for_channel_page(
                    str(channel or ""),
                    sort=str(sort or "newest"),
                    limit=_limit,
                    offset=_offset,
                    query=str(query or ""))
        except Exception as e:
            return {"rows": [], "has_more": False, "offset": _offset,
                    "next_offset": _offset, "error": str(e)}


    def browse_get_transcript(self, payload):
        """Fetch transcript segments + source classification for a video.

        Returns a dict (not a list) so the Watch view can pair the segments
        with a source banner ("Whisper" vs "YT auto-captions — approximate").
          { ok: True, segments: [...], source: {source, raw} }
        Where `source` is one of: whisper | yt_captions_punct | yt_captions_raw | unknown
        and `raw` is the original "(WHISPER:small)" / "(YT CAPTIONS)" /
        "(YT+PUNCTUATION)" tag from the Transcript.txt header.
        """
        segs = index_backend.get_segments(
            video_id=payload.get("video_id"),
            jsonl_path=payload.get("jsonl_path"),
            title=payload.get("title"),
            channel=payload.get("channel"),
            filepath=payload.get("filepath"),
            strict_identity=True,
        )
        try:
            src_info = self._classify_transcript_source(
                payload.get("title") or "",
                payload.get("jsonl_path") or "",
                payload.get("video_id") or "")
        except Exception:
            src_info = {"source": "unknown", "raw": ""}
        # When there are NO segments, look up the video's tx_status so the
        # Watch view can distinguish a genuinely-silent video ('no_speech',
        # Whisper ran and found nothing) from one that simply hasn't been
        # transcribed yet — both render zero segments otherwise. Only on the
        # empty path so the common (has-transcript) case stays a single query.
        tx_status = ""
        if not segs:
            try:
                tx_status = index_backend.video_tx_status(
                    video_id=payload.get("video_id") or "",
                    title=payload.get("title") or "")
            except Exception:
                tx_status = ""
        return {"ok": True, "segments": segs, "source": src_info,
                "tx_status": tx_status}


    def _classify_transcript_source(self, title, jsonl_path, video_id):
        """Read the aggregated Transcript.txt that covers `title` and pull
        the source tag out of its `===(title), ..., (SOURCE)===` header line.
        Mirrors ArchivePlayer's `_yt_parse_source_tags_in_dir` + classifier.
        Cheap — only reads until we find the matching header block."""
        if not title and not jsonl_path and not video_id:
            return {"source": "unknown", "raw": ""}
        # Resolve jsonl_path from the DB if the caller only gave us a
        # video_id. Watch-view's frontend doesn't know the jsonl_path —
        # it only has video_id + title — so without this lookup the
        # classifier had no search_dirs and returned unknown, which made
        # the source banner silently disappear. Fall back further to the
        # videos.filepath column so we can still find the Transcript.txt
        # by walking up the video file's directory tree.
        if not jsonl_path and video_id:
            try:
                from backend import index as _idx
                # Reader path (no _db_lock) so this lookup doesn't queue
                # behind the startup sweep's FTS-ingest writes — same
                # reason get_segments was migrated.
                # ORDER BY s.id DESC picks the MOST-RECENT-INGEST's
                # jsonl_path so the classifier resolves to the same
                # Transcript.txt directory that get_segments will pull
                # transcript text from. Without this match (plain
                # LIMIT 1 picks any matching segment row arbitrarily)
                # the classifier could land on a stale ingest's
                # directory, miss the active Transcript.txt header,
                # and return source=unknown — losing the
                # Whisper/YT-captions banner above the transcript.
                conn = _idx._reader_open() or _idx._open()
                if conn is not None:
                    row = conn.execute(
                        "SELECT s.jsonl_path, v.filepath "
                        "FROM videos v LEFT JOIN segments s "
                        " ON s.video_id = v.video_id "
                        "WHERE v.video_id=? "
                        "ORDER BY s.id DESC LIMIT 1",
                        (video_id,)).fetchone()
                    if row:
                        jsonl_path = row[0] or ""
                        if not jsonl_path and row[1]:
                            # No segments row yet — seed the search dir
                            # from the video file itself.
                            jsonl_path = row[1]
            except Exception as e:
                swallow("jsonl-path db query", e)
        # Find the candidate .txt — same folder as the jsonl_path, or walk
        # up from there. Name pattern: "<channel> [<year>] [<month>] Transcript.txt".
        search_dirs = []
        if jsonl_path:
            cur = os.path.dirname(jsonl_path)
            for _ in range(3):
                if cur and cur not in search_dirs:
                    search_dirs.append(cur)
                parent = os.path.dirname(cur) if cur else ""
                if parent == cur or not parent:
                    break
                cur = parent
        # scan the WHOLE file and take the LAST matching
        # header, not the first. Retranscribe appends a new header at
        # the end of the file and removes the old one — but if title
        # normalization differs between retranscribe (NFC + punct strip)
        # and this classifier (lower only), the old header can survive
        # and the classifier would return the stale "(YT_CAPTIONS_PUNCT)"
        # source. Taking the LAST matching header in scan order is the
        # safe play: the newest write always wins.
        # Also use punctuation-insensitive title compare so the classifier
        # matches even when titles drift in trailing "." between
        # YouTube auto-caption metadata and Whisper retranscribe input.
        import re as _re_cl
        def _classify_norm(s):
            v = (s or "").strip().lower()
            return _re_cl.sub(r"[\.\?\!…]+$", "", v).strip()

        # v62.4 bug fix: header format is
        #   ===(title), (DATE), (TIME), (SOURCE)===
        # The original parser did `body.split(",")` and assumed
        # parts[0] is the title — which silently broke for titles
        # that contain a comma (e.g. voidzilla's "i tried getting
        # scammed, instead i got the t1 phone"). The title would
        # come out as "i tried getting scammed" — no match against
        # the requested title — so the classifier returned unknown
        # and the Watch view dropped its source banner entirely.
        # Anchor the parse on the END of the line where DATE / TIME
        # / SOURCE never contain commas, and let the title absorb
        # whatever comes before — title can contain commas or even
        # parens without breaking the match.
        # v2 headers append an optional 5th `(youtu.be/<id>)` field. The
        # (?!youtu\.be/) lookahead on the SOURCE group stops greedy-title
        # backtracking from absorbing a field and misreading the URL as
        # the source tag on v2 lines.
        _hdr_re = _re_cl.compile(
            r"^\((.*)\)\s*,\s*\(([^()]+)\)\s*,\s*\(([^()]+)\)\s*,"
            r"\s*\(((?!youtu\.be/)[^()]+)\)"
            r"(?:\s*,\s*\(youtu\.be/[A-Za-z0-9_-]{11}\))?\s*$"
        )
        raw_tag = ""
        norm_title = _classify_norm(title)
        for d in search_dirs:
            try:
                fns = [f for f in os.listdir(d)
                       if f.endswith("Transcript.txt")]
            except OSError:
                continue
            for fn in fns:
                fp = os.path.join(d, fn)
                _last_tag = ""
                try:
                    # Scan a bounded tail of large aggregate transcripts and
                    # keep the last matching header so newest writes win.
                    for line in _iter_transcript_header_lines(fp):
                        body = line.strip().rstrip("=").lstrip("=").strip()
                        m = _hdr_re.match(body)
                        if m:
                            head_title = m.group(1).strip()
                            tag = m.group(4).strip()
                            if _classify_norm(head_title) == norm_title:
                                _last_tag = tag
                            continue
                        # Fallback: pre-regex split-by-comma path
                        # for any legacy/odd header that doesn't
                        # match the canonical 4-field shape.
                        parts = [p.strip() for p in body.split(",")]
                        if not parts:
                            continue
                        head_title = parts[0].strip()
                        if head_title.startswith("("):
                            head_title = head_title[1:]
                        if head_title.endswith(")"):
                            head_title = head_title[:-1]
                        if _classify_norm(head_title) != norm_title:
                            continue
                        if len(parts) >= 2:
                            tail = parts[-1].strip()
                            # v2 headers end with the (youtu.be/<id>)
                            # url field — the source tag is one field
                            # earlier.
                            if (tail.lstrip("(").startswith("youtu.be/")
                                    and len(parts) >= 3):
                                tail = parts[-2].strip()
                            if tail.startswith("(") and tail.endswith(")"):
                                _last_tag = tail[1:-1].strip()
                except OSError:
                    continue
                if _last_tag:
                    raw_tag = _last_tag
                    break
            if raw_tag:
                break

        # Classify — mirrors ArchivePlayer _yt_classify_source exactly.
        up = raw_tag.upper()
        if "WHISPER" in up:
            kind = "whisper"
        elif "YT+PUNCT" in up or "YT+PUNCTUATION" in up:
            kind = "yt_captions_punct"
        elif "YT CAPTIONS" in up or up == "YT":
            kind = "yt_captions_raw"
        else:
            kind = "unknown"
        # stop guessing the model name. Transcripts written by
        # v46.0-v46.5 stored bare "(WHISPER)" with no model. Previously we
        # filled in the user's CURRENT default_model — but that misleads
        # users who've since changed their default: a transcript made
        # with large-v3 would show as "Whisper:small" if the user is now
        # on small. Better to surface an honest "model unknown" suffix;
        # user can Re-transcribe to get a real tag baked in.
        if kind == "whisper" and raw_tag.strip().upper() == "WHISPER":
            raw_tag = "WHISPER:unknown"
        return {"source": kind, "raw": raw_tag}


    def browse_search_context(self, payload):
        """Return context around a search hit — used by the Search viewer pane.

        payload = { segment_id, before?, after? }
        Returns { ok, title, channel, segments:[{s,e,t,is_hit}], before_more, after_more }
        where `is_hit` marks the segment that was originally matched + any
        other segments in the same video that match the query.
        before/after are clamped to 0..500 to keep malformed callers from
        asking the bridge to marshal huge context windows.

        Matches YTArchiver.py:29598 viewer pane data flow — grab N segments
        before + hit + N segments after, let the user pull in more with
        Up/Down "Load more" buttons.
        """
        try:
            seg_id = int((payload or {}).get("segment_id") or 0)
            before = max(0, min(int((payload or {}).get("before") or 30), 500))
            after = max(0, min(int((payload or {}).get("after") or 30), 500))
            query = (payload or {}).get("query") or ""
            return index_backend.get_segment_context(seg_id, before, after, query)
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def browse_get_video_metadata(self, payload):
        """Return the aggregated metadata entry for a single video.

        Walks up from the video's filepath to the channel folder, finds the
        appropriate `.{ch_name} ... Metadata.jsonl`, parses it, and returns
        the entry keyed by `video_id`. Includes description + top 50
        comments + view/like counts — feeds the Watch view metadata drawer
        (YTArchiver.py:31164 _player_drawer_frame).
        """
        try:
            filepath = (payload or {}).get("filepath") or ""
            video_id = (payload or {}).get("video_id") or ""
            channel = (payload or {}).get("channel") or ""
            if not video_id:
                import re as _re
                m = _re.search(r"\[([A-Za-z0-9_-]{11})\]",
                               os.path.basename(filepath))
                if m:
                    video_id = m.group(1)
            if not video_id:
                return {"ok": False, "error": "No video_id"}
            cached = _metadata_drawer_cache_get(video_id, channel)
            if cached:
                return cached

            # Find the aggregated metadata JSONL — look in the video's own
            # folder first, then walk up to 3 levels (year / year-month).
            from backend.metadata import _read_metadata_jsonl
            if filepath:
                cur = os.path.dirname(filepath)
                for _ in range(4):
                    if not cur:
                        break
                    try:
                        for fn in os.listdir(cur):
                            if fn.startswith(".") and fn.endswith("Metadata.jsonl"):
                                source = os.path.join(cur, fn)
                                entries = _read_metadata_jsonl(source)
                                if video_id in entries:
                                    meta = entries[video_id]
                                    _metadata_drawer_cache_put(
                                        video_id, channel, source, meta)
                                    return {"ok": True, "meta": meta,
                                            "source": source}
                    except OSError:
                        pass
                    parent = os.path.dirname(cur)
                    if parent == cur:
                        break
                    cur = parent

            # Fall back: scan the channel folder. Depth-cap at 3 levels
            # below the channel root (channel → year → month → ...).
            # Channel folder organization never goes deeper than that,
            # but the unbounded walk used to freeze the UI for several
            # seconds when a metadata jsonl lived in an unexpected
            # location (audit: browse_mixin.py:329).
            cfg = self._browse_config()
            base = (cfg.get("output_dir") or "").strip()
            if base and channel:
                from backend.sync import channel_folder_name as _cfn
                # Look up channel record for folder_override etc.
                ch_dict = None
                for ch in cfg.get("channels", []):
                    if (ch.get("name") or "") == channel:
                        ch_dict = ch; break
                folder = os.path.join(base, _cfn(ch_dict) if ch_dict else channel)
                if os.path.isdir(folder):
                    _root_depth = folder.rstrip(os.sep).count(os.sep)
                    _MAX_DEPTH = 3
                    for dp, dns, fns in os.walk(folder):
                        if dp.rstrip(os.sep).count(os.sep) - _root_depth >= _MAX_DEPTH:
                            dns[:] = []  # don't recurse further
                        for fn in fns:
                            if fn.startswith(".") and fn.endswith("Metadata.jsonl"):
                                source = os.path.join(dp, fn)
                                entries = _read_metadata_jsonl(source)
                                if video_id in entries:
                                    meta = entries[video_id]
                                    _metadata_drawer_cache_put(
                                        video_id, channel, source, meta)
                                    return {"ok": True, "meta": meta,
                                            "source": source}
            return {"ok": False, "error": "Metadata not found"}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def browse_refresh_video_metadata(self, payload):
        """Refresh views/likes/description/comments for a single video.

        Drives the Watch view's "Refresh metadata" button — synchronous
        per-video fetch via yt-dlp, write straight back to the channel's
        aggregated Metadata.jsonl, and return the new entry so the
        drawer can re-render without a Back-and-reopen round-trip.

        payload: {filepath, video_id?, title?, channel?}
        Returns: {ok, meta?, error?}
        """
        try:
            filepath = (payload or {}).get("filepath") or ""
            video_id = (payload or {}).get("video_id") or ""
            title    = (payload or {}).get("title") or ""
            channel  = (payload or {}).get("channel") or ""
            if not video_id and filepath:
                import re as _re
                m = _re.search(r"\[([A-Za-z0-9_-]{11})\]",
                               os.path.basename(filepath))
                if m:
                    video_id = m.group(1)
            if not video_id:
                return {"ok": False, "error": "No video_id"}
            if not filepath or not os.path.isfile(filepath):
                return {"ok": False, "error": "Video file not found"}
            cfg = self._browse_config()
            ch_dict = None
            for ch in cfg.get("channels", []):
                if (ch.get("name") or "") == channel:
                    ch_dict = ch
                    break
            if ch_dict is None:
                return {"ok": False, "error": f"Channel '{channel}' not in config"}
            from backend.metadata import fetch_single_video_metadata
            res = fetch_single_video_metadata(
                ch_dict, video_id, filepath, title,
                self._browse_log_stream(), emit_inline_log=False, refresh=True)
            if not res.get("ok"):
                return {"ok": False,
                        "error": res.get("error") or "fetch failed",
                        "transient": bool(res.get("transient"))}
            # Re-read the entry we just wrote so the caller gets the
            # canonical on-disk shape (matches browse_get_video_metadata).
            ret = self.browse_get_video_metadata({
                "filepath": filepath, "video_id": video_id,
                "title": title, "channel": channel,
            })
            # Re-render the Recent grid so the freshly downloaded
            # thumbnail shows up without a relaunch. The .jpg sidecar
            # gets written inside fetch_single_video_metadata above,
            # but the Recent grid was rendered BEFORE that write and
            # has a stale thumbnail_url="" on the card.
            try: self._push_recent_refresh()
            except Exception as e: swallow("recent-refresh push", e)
            return ret if ret.get("ok") else {"ok": True, "meta": res.get("entry")}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def _manual_refresh_metadata_one(self, payload, *, push_refresh: bool):
        """Refresh metadata for a single MANUAL download (not part of any
        subscription). Fetches views/likes/description/comments by video_id and
        writes the entry to a `.…Metadata.jsonl` NEXT TO the video file, where
        browse_get_video_metadata's filepath-walk finds it — so loose downloads
        outside the channel tree get metadata too.

        payload: {filepath, video_id?, title?, channel?}
        Returns: {ok, meta?, error?, transient?}
        """
        try:
            filepath = (payload or {}).get("filepath") or ""
            video_id = (payload or {}).get("video_id") or ""
            title    = (payload or {}).get("title") or ""
            channel  = (payload or {}).get("channel") or ""
            if not video_id and filepath:
                import re as _re
                m = _re.search(r"\[([A-Za-z0-9_-]{11})\]",
                               os.path.basename(filepath))
                if m:
                    video_id = m.group(1)
            if not video_id:
                return {"ok": False,
                        "error": "No video ID for this download — can't fetch "
                                 "metadata. (ID backfill is a separate step.)"}
            if not filepath or not os.path.isfile(filepath):
                return {"ok": False, "error": "Video file not found"}
            dest = os.path.dirname(filepath)
            # Synthetic channel: `name` only drives the JSONL filename; no
            # year/month split for a loose single video. dest_folder steers
            # the write next to the file.
            syn = {"name": channel or "Manual",
                   "split_years": False, "split_months": False}
            from backend.metadata import fetch_single_video_metadata
            res = fetch_single_video_metadata(
                syn, video_id, filepath, title, self._browse_log_stream(),
                emit_inline_log=False, refresh=True, dest_folder=dest)
            if not res.get("ok"):
                return {"ok": False,
                        "error": res.get("error") or "fetch failed",
                        "transient": bool(res.get("transient"))}
            # Hide the freshly-written metadata/thumbnail sidecars so the
            # manual-downloads folder still shows only the video file.
            try:
                from backend import utils as _u
                _u.hide_stray_sidecars(
                    dest, recursive=False, hide_per_video_transcripts=True)
            except Exception as e:
                swallow("manual meta hide sidecars", e)
            if push_refresh:
                try: self._push_recent_refresh()
                except Exception as e: swallow("manual metadata refresh push", e)
            return {"ok": True, "meta": res.get("entry")}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def manual_refresh_metadata(self, payload):
        return self._manual_refresh_metadata_one(payload, push_refresh=True)

    def _manual_bulk_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        offset = 0
        while True:
            res = self.list_manual_videos("title", 500, offset)
            page = (res or {}).get("rows") or []
            if not page:
                break
            for r in page:
                fp = r.get("filepath") or ""
                key = os.path.normcase(os.path.normpath(fp)) if fp else ""
                if not key or key in seen:
                    continue
                seen.add(key)
                rows.append(r)
            if not (res or {}).get("has_more"):
                break
            offset += len(page)
            if offset > 10000:
                break
        return rows

    def manual_bulk_action_summary(self):
        """Preflight counts for Manual-tab bulk actions.

        The frontend uses this to ask for confirmation before starting costly
        work, and the workers use the same underlying row source so the counts
        match what will actually be processed.
        """
        try:
            rows = self._manual_bulk_rows()
            total = len(rows)
            with_id = 0
            missing_id = 0
            excluded_id = 0
            tried_missing_id = 0
            transcribed = 0
            no_speech = 0
            missing_files = 0
            failed_attempts = 0

            for r in rows:
                fp = r.get("filepath") or ""
                if not fp or not os.path.isfile(fp):
                    missing_files += 1
                if r.get("video_id"):
                    with_id += 1
                else:
                    missing_id += 1
                    if r.get("id_backfill_excluded_ts"):
                        excluded_id += 1
                    elif r.get("id_backfill_tried_ts"):
                        tried_missing_id += 1
                    try:
                        failed_attempts += int(r.get("id_backfill_fail_count") or 0)
                    except (TypeError, ValueError):
                        pass

                tx = (r.get("tx_status") or "").lower()
                if tx not in {"transcribed", "done", "no_speech"}:
                    if self._manual_has_local_transcript(r):
                        tx = "transcribed"
                if tx in {"transcribed", "done"}:
                    transcribed += 1
                elif tx == "no_speech":
                    no_speech += 1

            recover_eligible = max(0, missing_id - excluded_id)
            transcribe_done = transcribed + no_speech
            return {
                "ok": True,
                "total": total,
                "with_id": with_id,
                "missing_id": missing_id,
                "recover_eligible": recover_eligible,
                "recover_excluded": excluded_id,
                "recover_tried": tried_missing_id,
                "recover_failed_attempts": failed_attempts,
                "metadata_eligible": with_id,
                "metadata_skipped_no_id": missing_id,
                "transcribed": transcribed,
                "no_speech": no_speech,
                "transcribe_eligible": max(0, total - transcribe_done),
                "transcribe_skipped": transcribe_done,
                "missing_files": missing_files,
                "percent_with_id": round((with_id / total) * 100, 1)
                                   if total else 0.0,
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _manual_emit_text(self, text: str, tag: str = "dim") -> None:
        try:
            self._browse_log_stream().emit_text(text, tag)
        except Exception as e:
            _log.debug("manual bulk log emit failed: %s", e)

    def _manual_emit_js(self, callback_name: str, payload: dict[str, Any]) -> None:
        try:
            win = getattr(self, "_window", None)
            if win is None:
                return
            import json as _json
            win.evaluate_js(
                f"window.{callback_name} && "
                f"window.{callback_name}({_json.dumps(payload)});")
        except Exception as e:
            _log.debug("manual bulk js emit failed: %s", e)

    def manual_refresh_all_metadata(self):
        existing = getattr(self, "_manual_refresh_all_thread", None)
        if existing is not None and existing.is_alive():
            return {"ok": False, "error": "Manual metadata refresh is already running"}

        def _run():
            summary = {
                "ok": True,
                "total": 0,
                "refreshed": 0,
                "skipped_no_id": 0,
                "failed": 0,
            }
            try:
                rows = self._manual_bulk_rows()
                summary["total"] = len(rows)
                self._manual_emit_text(
                    f" - Manual metadata refresh: {len(rows):,} video(s) scanned.\n")
                self._manual_emit_js("_manualRefreshAllProgress", {
                    **summary,
                    "current": 0,
                    "phase": "scanned",
                })
                for i, r in enumerate(rows, 1):
                    fp = r.get("filepath") or ""
                    title = r.get("title") or os.path.basename(fp)
                    self._manual_emit_js("_manualRefreshAllProgress", {
                        **summary,
                        "current": i,
                        "phase": "fetching",
                        "title": title,
                    })
                    if not fp or not os.path.isfile(fp):
                        summary["failed"] += 1
                        self._manual_emit_js("_manualRefreshAllProgress", {
                            **summary,
                            "current": i,
                            "phase": "failed",
                            "title": title,
                        })
                        continue
                    payload = {
                        "filepath": fp,
                        "video_id": r.get("video_id") or "",
                        "title": title,
                        "channel": r.get("channel") or "",
                    }
                    res = self._manual_refresh_metadata_one(
                        payload, push_refresh=False)
                    if res.get("ok"):
                        summary["refreshed"] += 1
                        phase = "done"
                    elif "video ID" in str(res.get("error") or ""):
                        summary["skipped_no_id"] += 1
                        phase = "skipping"
                    else:
                        summary["failed"] += 1
                        phase = "failed"
                    self._manual_emit_js("_manualRefreshAllProgress", {
                        **summary,
                        "current": i,
                        "phase": phase,
                        "title": title,
                    })
                    if i % 10 == 0 or i == len(rows):
                        self._manual_emit_text(
                            f" - Manual metadata [{i:,}/{len(rows):,}]: "
                            f"{summary['refreshed']:,} refreshed, "
                            f"{summary['skipped_no_id']:,} no ID, "
                            f"{summary['failed']:,} failed.\n")
                try:
                    self._push_recent_refresh()
                except Exception as e:
                    swallow("manual metadata bulk refresh push", e)
            except Exception as e:
                summary = {"ok": False, "error": str(e)}
            finally:
                self._manual_emit_js("_manualRefreshAllDone", summary)

        t = threading.Thread(target=_run, name="manual-metadata-all",
                             daemon=True)
        self._manual_refresh_all_thread = t
        t.start()
        return {"ok": True, "started": True}

    def manual_transcribe_all(self, model=""):
        existing = getattr(self, "_manual_transcribe_all_thread", None)
        if existing is not None and existing.is_alive():
            return {"ok": False, "error": "Manual transcription queueing is already running"}

        apply_model = getattr(self, "_apply_runtime_whisper_model", None)
        if callable(apply_model):
            model_result = apply_model(model or "")
            if not model_result.get("ok"):
                return model_result

        def _run():
            summary = {
                "ok": True,
                "total": 0,
                "queued": 0,
                "skipped": 0,
                "failed": 0,
            }
            try:
                rows = self._manual_bulk_rows()
                summary["total"] = len(rows)
                self._manual_emit_text(
                    f" - Manual transcribe: {len(rows):,} video(s) scanned.\n")
                mgr_getter = getattr(self, "_transcribe_manager", None)
                if not callable(mgr_getter):
                    raise RuntimeError("Transcription manager is unavailable")
                mgr = mgr_getter()
                bulk_id = f"manual-{int(time.time())}"
                candidates = [
                    r for r in rows
                    if (r.get("tx_status") or "").lower()
                    not in {"transcribed", "done", "no_speech"}
                ]
                total = len(candidates)
                summary["skipped"] = len(rows) - total
                summary["candidate_total"] = total
                self._manual_emit_js("_manualTranscribeAllProgress", {
                    **summary,
                    "current": 0,
                    "phase": "scanned",
                })
                for i, r in enumerate(candidates, 1):
                    fp = r.get("filepath") or ""
                    title = r.get("title") or os.path.basename(fp)
                    self._manual_emit_js("_manualTranscribeAllProgress", {
                        **summary,
                        "current": i,
                        "phase": "queueing",
                        "title": title,
                    })
                    if not fp or not os.path.isfile(fp):
                        summary["failed"] += 1
                        self._manual_emit_js("_manualTranscribeAllProgress", {
                            **summary,
                            "current": i,
                            "phase": "failed",
                            "title": title,
                        })
                        continue
                    try:
                        ok = mgr.enqueue(
                            fp,
                            title,
                            channel=r.get("channel") or "",
                            video_id=r.get("video_id") or "",
                            bulk_id=bulk_id,
                            bulk_total=total,
                            bulk_index=i,
                        )
                    except Exception:
                        ok = False
                    if ok:
                        summary["queued"] += 1
                    else:
                        summary["failed"] += 1
                    self._manual_emit_js("_manualTranscribeAllProgress", {
                        **summary,
                        "current": i,
                        "phase": "queued" if ok else "failed",
                        "title": title,
                    })
                    if i % 10 == 0 or i == total:
                        self._manual_emit_text(
                            f" - Manual transcribe queue [{i:,}/{total:,}]: "
                            f"{summary['queued']:,} queued, "
                            f"{summary['failed']:,} failed.\n")
                try:
                    self._on_queue_changed()
                except Exception as e:
                    _log.debug("manual transcribe queue refresh failed: %s", e)
            except Exception as e:
                summary = {"ok": False, "error": str(e)}
            finally:
                self._manual_emit_js("_manualTranscribeAllQueued", summary)

        t = threading.Thread(target=_run, name="manual-transcribe-all",
                             daemon=True)
        self._manual_transcribe_all_thread = t
        t.start()
        return {"ok": True, "started": True}


    def manual_backfill_ids(self, dry_run=False, limit=None):
        """Start recovering video IDs for no-ID manual downloads, in the
        background. Searches YouTube by each file's embedded/fallback title and
        matches on duration; writes confident matches, queues ambiguous ones
        for review. Streams progress to the activity log; returns immediately.
        """
        import threading as _thr
        existing = getattr(self, "_manual_backfill_thread", None)
        if existing is not None and existing.is_alive():
            return {"ok": False, "error": "Recover IDs is already running"}
        self._manual_backfill_cancel = _thr.Event()
        stream = self._browse_log_stream()
        cancel = self._manual_backfill_cancel
        _lim = None
        try:
            _lim = int(limit) if limit else None
        except (TypeError, ValueError):
            _lim = None

        def _run():
            summary = {"ok": False}
            try:
                from backend.metadata import manual_backfill as _mb
                summary = _mb.backfill_manual_video_ids(
                    stream, cancel_event=cancel,
                    dry_run=bool(dry_run), limit=_lim)
            except Exception as _e:
                summary = {"ok": False, "error": str(_e)}
                try: stream.emit_error(f"Recover IDs failed: {_e}")
                except Exception as _ee: swallow("recover-ids err emit", _ee)
            finally:
                # Refresh the Manual grid so freshly-resolved rows pick up
                # their new id (and thus the "Refresh metadata" action).
                try: self._push_recent_refresh()
                except Exception as _pe: swallow("recover-ids refresh", _pe)
                # Reset the start/stop button in the UI.
                try:
                    if getattr(self, "_window", None) is not None:
                        import json as _json
                        _summary = _json.dumps(summary)
                        self._window.evaluate_js(
                            "window._manualBackfillDone && "
                            f"window._manualBackfillDone({_summary});")
                except Exception as _we: swallow("recover-ids done reset", _we)

        t = _thr.Thread(target=_run, name="manual-id-backfill", daemon=True)
        self._manual_backfill_thread = t
        t.start()
        return {"ok": True, "started": True, "dry_run": bool(dry_run)}

    def manual_backfill_ids_cancel(self):
        """Request the in-progress Recover IDs run to stop."""
        ev = getattr(self, "_manual_backfill_cancel", None)
        if ev is not None:
            ev.set()
        return {"ok": True}

    def manual_backfill_review_list(self):
        """Ambiguous matches saved for review by the last Recover IDs run.
        Each: {filepath, title, duration, candidates:[{id,title,duration,
        channel,title_sim,dur_delta}]}."""
        try:
            from backend.metadata.manual_backfill import REVIEW_FILE
            if not REVIEW_FILE.exists():
                return {"ok": True, "items": []}
            import json as _json
            items = []
            for ln in REVIEW_FILE.read_text(
                    encoding="utf-8", errors="replace").splitlines():
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    items.append(_json.loads(ln))
                except Exception:
                    pass
            return {"ok": True, "items": items}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _manual_review_remove(self, filepath):
        """Rewrite the review file without `filepath` (resolved or skipped)."""
        try:
            from backend.metadata.manual_backfill import REVIEW_FILE
            if not REVIEW_FILE.exists():
                return
            import json as _json
            key = os.path.normcase(os.path.normpath(filepath or ""))
            kept = []
            for ln in REVIEW_FILE.read_text(
                    encoding="utf-8", errors="replace").splitlines():
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    obj = _json.loads(ln)
                    if os.path.normcase(os.path.normpath(
                            obj.get("filepath") or "")) == key:
                        continue
                except Exception:
                    pass
                kept.append(ln)
            REVIEW_FILE.write_text(
                ("\n".join(kept) + "\n") if kept else "", encoding="utf-8")
        except Exception as e:
            swallow("review remove", e)

    def manual_backfill_apply_pick(self, filepath, video_id, channel="", title=""):
        """User picked a candidate for an ambiguous manual download: register
        it with that id, queue its metadata refresh, and drop it from the
        review list. The slow network metadata fetch runs in the background so
        the picker can advance immediately."""
        try:
            if not filepath or not video_id:
                return {"ok": False, "error": "missing filepath or video_id"}
            if not os.path.isfile(filepath):
                return {"ok": False, "error": "file not found"}
            from backend import index as _idx
            url = f"https://www.youtube.com/watch?v={video_id}"
            wrote = _idx.set_manual_video_id(
                filepath, video_id, url, channel=channel or "")
            if not wrote:
                _idx.register_video(
                    filepath, channel or "Single Videos",
                    title or os.path.splitext(os.path.basename(filepath))[0],
                    video_id=video_id)
            self._manual_review_remove(filepath)
            try: self._push_recent_refresh()
            except Exception as e: swallow("review-pick refresh", e)

            def _fetch_after_pick():
                try:
                    from backend.metadata import fetch_single_video_metadata
                    fetch_single_video_metadata(
                        {"name": channel or "Single Videos",
                         "split_years": False, "split_months": False},
                        video_id, filepath, title or "", self._browse_log_stream(),
                        emit_inline_log=False, refresh=True,
                        dest_folder=os.path.dirname(filepath))
                    try:
                        from backend import utils as _u
                        _u.hide_stray_sidecars(
                            os.path.dirname(filepath), recursive=False,
                            hide_per_video_transcripts=True)
                    except Exception as e:
                        swallow("review-pick hide sidecars", e)
                except Exception as e:
                    swallow("review-pick metadata fetch", e)
                finally:
                    try: self._push_recent_refresh()
                    except Exception as e: swallow("review-pick metadata push", e)

            try:
                threading.Thread(
                    target=_fetch_after_pick,
                    name="manual-review-metadata",
                    daemon=True,
                ).start()
            except Exception as e:
                swallow("review-pick metadata thread", e)
            return {"ok": True, "metadata_started": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def manual_backfill_review_skip(self, filepath):
        """Drop an item from the review list without resolving it."""
        try:
            self._manual_review_remove(filepath)
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def browse_search(self, query, channel=None, limit=200, sort="relevance",
                      year_from=None, year_to=None):
        """FTS5 search across transcript segments.

        `channel` accepts either a single channel-folder string, a list
        of folders (multi-channel scope), or None / empty list ("all
        channels"). Passing a list lets the new search UI scope to a
        subset of channels without forcing the user to run separate
        searches.

        `sort` controls result ordering — "relevance" (default, FTS5
        bm25 rank), "newest", "oldest", "channel", or "title". The UI
        sort dropdown passes the user's pick through.

        `year_from` / `year_to` (inclusive) constrain results to segments
        in that upload-year window; either may be None for an open bound.
        """
        return index_backend.search_fts(query, channel=channel,
                                         limit=limit, sort=sort,
                                         year_from=year_from, year_to=year_to)


    def browse_search_titles(self, query, channel=None, limit=200, sort="newest",
                             year_from=None, year_to=None):
        """Global video search by title across every channel (or a
        subset). `channel` follows the same accept-string-or-list rule
        as `browse_search` so the new search UI can apply its channel
        multi-select to titles too.

        `sort` controls result ordering — "newest" (default), "oldest",
        "channel", or "title". Title-only search defaults to newest
        because there's no FTS5 relevance score for LIKE-based matches.

        `year_from` / `year_to` (inclusive) constrain results to that
        upload-year window; either may be None for an open bound.
        """
        return index_backend.search_video_titles(
            query, channel=channel, limit=limit, sort=sort,
            year_from=year_from, year_to=year_to)


    def browse_graph(self, word, channel=None, bucket="month", normalize=False):
        """Word frequency over time for the Graph sub-mode.

        `word` may be a single term or a comma-separated list for multi-line charts.
        `channel` may be a single channel name, or a list for per-channel overlay.
        `normalize` True divides each bucket by total segments in that bucket
        (then × 1000) so channels of different sizes become comparable —
        matches YTArchiver.py:30427 normalize checkbox.
        """
        # Coerce/validate `word` — JS callers occasionally pass null which
        # would trip the .split AttributeError below and surface as an
        # opaque TypeError. Return a clear error instead.
        word = (word or "")
        if not isinstance(word, str):
            try: word = str(word)
            except Exception: word = ""
        if not word.strip():
            return {"ok": False, "error": "word required"}
        # Multi-channel overlay takes priority if both given
        if isinstance(channel, list) and channel and isinstance(word, str):
            w = word.split(",")[0].strip()
            res = index_backend.graph_channel_overlay(w, channel, bucket=bucket)
        elif isinstance(word, str) and "," in word:
            words = [w.strip() for w in word.split(",") if w.strip()]
            if len(words) > 1:
                res = index_backend.graph_multi(words, channel=channel, bucket=bucket)
            else:
                res = index_backend.graph_word_frequency(word, channel=channel, bucket=bucket)
        else:
            res = index_backend.graph_word_frequency(word, channel=channel, bucket=bucket)

        if normalize and isinstance(res, dict) and res.get("labels"):
            # Pull per-bucket total segments for the same channel/bucket so
            # we can divide. Backend helper returns {label: total_segs}.
            try:
                totals = index_backend.bucket_totals(bucket=bucket, channel=channel)
            except Exception:
                totals = {}
            def _norm(labels, values):
                out = []
                for lbl, v in zip(labels, values, strict=False):
                    tot = totals.get(str(lbl), 0)
                    if tot > 0:
                        out.append(round((v * 1000.0) / tot, 2))
                    else:
                        out.append(0)
                return out
            if "values" in res:
                res["values"] = _norm(res["labels"], res["values"])
                res["normalized"] = True
            elif "series" in res:
                for s in res["series"]:
                    s["values"] = _norm(res["labels"], s["values"])
                res["normalized"] = True
        return res


    def browse_word_cloud(self, channel=None, top_n=120):
        """Return the top-N most-spoken words for the Graph Word Cloud.

        Skips common English stop-words so the cloud surfaces actually-
        distinctive vocabulary. Matches YTArchiver.py Graph sub-mode's
        matplotlib word cloud conceptually.
        """
        try:
            res = index_backend.top_words(channel=channel, top_n=int(top_n or 120))
            return {"ok": True, "words": res}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def browse_video_url(self, filepath):
        """Return a file:/// URL the webview can load into a <video> element."""
        try:
            from backend.index import _file_url
            guard = _guard_browse_launch_path(filepath, require_file=True)
            if not guard.get("ok"):
                return guard
            fp = guard["path"]
            return {"ok": True, "url": _file_url(fp)}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def browse_resolve_segment(self, jsonl_path, video_id=None, title=None):
        """Given a transcript segment's .jsonl_path (from FTS search results),
        resolve the actual video file sitting next to it so the Watch view
        can be opened with a proper filepath.

        Returns {ok, filepath, title, channel, video_id}. If the .jsonl's
        sibling video isn't found, also tries the `videos` table by video_id.
        """
        try:
            out = {"ok": False}
            jp = os.path.normpath(jsonl_path or "")
            if jp and os.path.isfile(jp):
                base = os.path.splitext(jp)[0]
                for ext in (".mp4", ".mkv", ".webm", ".m4a", ".mov"):
                    cand = base + ext
                    if os.path.isfile(cand):
                        guard = file_ops.assert_within_managed_roots(cand)
                        if not guard.get("ok"):
                            return guard
                        # Derive channel from path so the Watch view
                        # has the context it needs for downstream
                        # browse_get_video_metadata + thumbnails
                        # (audit: browse_mixin H2). Standard layout
                        # is <output_dir>/<channel>/<file>, so the
                        # parent folder name is the channel.
                        _ch_guess = ""
                        try:
                            _ch_guess = os.path.basename(
                                os.path.dirname(cand)) or ""
                        except Exception:
                            pass
                        return {
                            "ok": True,
                            "filepath": cand,
                            "title": title or os.path.basename(base),
                            "channel": _ch_guess,
                            "video_id": video_id or "",
                        }
            # Fallback — search the videos table by video_id.
            # Use the reader connection so this lookup doesn't queue
            # behind sweep / ingest_jsonl writers holding `_db_lock`.
            if video_id:
                from backend.index import _reader_lock, _reader_open
                rconn = _reader_open()
                if rconn is not None:
                    with _reader_lock:
                        row = rconn.execute(
                            "SELECT filepath, title, channel FROM videos WHERE video_id=? LIMIT 1",
                            (video_id,),
                        ).fetchone()
                    if row and row[0] and os.path.isfile(row[0]):
                        guard = file_ops.assert_within_managed_roots(row[0])
                        if not guard.get("ok"):
                            return guard
                        return {
                            "ok": True,
                            "filepath": row[0],
                            "title": row[1] or title or "",
                            "channel": row[2] or "",
                            "video_id": video_id,
                        }
            out["error"] = "Video file not found"
            return out
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def browse_open_video(self, filepath):
        """Launch the video in the system default player (VLC if associated)."""
        try:
            guard = _guard_browse_launch_path(filepath, require_file=True)
            if not guard.get("ok"):
                return guard
            fp = guard["path"]
            if os.name == "nt":
                os.startfile(fp)
            elif sys.platform == "darwin":
                import subprocess
                subprocess.Popen(["open", fp])
            else:
                import subprocess
                subprocess.Popen(["xdg-open", fp])
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    def browse_show_in_explorer(self, filepath):
        """Reveal the file in Explorer/Finder."""
        try:
            guard = _guard_browse_launch_path(filepath, require_file=False)
            if not guard.get("ok"):
                return guard
            fp = guard["path"]
            if os.name == "nt":
                import subprocess
                # On Windows, `explorer /select,<path>` must keep the
                # `/select,` switch UNQUOTED with only the path quoted:
                #   explorer /select,"C:\dir with spaces\file.mp4"
                # Passing ["explorer", "/select,<path>"] as a list made
                # Python quote the whole `/select,<path>` element when the
                # path had spaces, so Explorer stopped recognizing the
                # switch and just opened a default window without selecting
                # the file. Pass one raw command string instead so the
                # switch stays bare. (Windows filenames can't contain `"`,
                # so wrapping the path in quotes is always safe.)
                subprocess.Popen(f'explorer /select,"{fp}"', close_fds=True)
            elif sys.platform == "darwin":
                import subprocess
                subprocess.Popen(["open", "-R", fp])
            else:
                import subprocess
                subprocess.Popen(["xdg-open", os.path.dirname(fp)])
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}


    # ─── Manual Downloads view ─────────────────────────────────────────

    _MANUAL_VIDEO_EXTS = frozenset(
        {".mp4", ".webm", ".mkv", ".avi", ".mov", ".m4v"}
    )

    def _iter_manual_folder_videos(self, folder: str):
        """Yield video files directly in video_out_dir.

        The configured manual folder is also the parent of `Whole Channels`
        in this archive layout. Recursing it pulls channel-sync libraries and
        user collection subfolders into Manual, so the disk fallback stays
        root-only. Custom Save-to downloads outside this root still appear via
        the index path above.
        """
        with os.scandir(folder) as it:
            for entry in it:
                if not entry.is_file(follow_symlinks=False):
                    continue
                ext = os.path.splitext(entry.name)[1].lower()
                if ext not in self._MANUAL_VIDEO_EXTS:
                    continue
                yield entry.path

    def _manual_has_local_transcript(self, row: dict[str, Any]) -> bool:
        fp = row.get("filepath") or ""
        if not fp:
            return False
        title = row.get("title") or os.path.splitext(os.path.basename(fp))[0]
        channel = row.get("channel") or ""
        candidates: list[str] = []
        try:
            from backend.transcribe.helpers import _resolve_transcript_paths
            paths = _resolve_transcript_paths(fp, title, channel)
            if paths:
                candidates.extend([paths[0], paths[1]])
        except Exception as e:
            _log.debug("manual transcript path resolve failed: %s", e)

        base, _ext = os.path.splitext(fp)
        folder = os.path.dirname(fp)
        stem = os.path.splitext(os.path.basename(fp))[0]
        clean_stem = re.sub(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$", "", stem) or stem
        for name in dict.fromkeys([stem, clean_stem]):
            candidates.extend([
                os.path.join(folder, f"{name} Transcript.txt"),
                os.path.join(folder, f".{name} Transcript.jsonl"),
            ])
        candidates.extend([base + ".jsonl", base + ".txt"])

        for path in dict.fromkeys(candidates):
            if not path:
                continue
            try:
                if os.path.isfile(path) and os.path.getsize(path) > 0:
                    return True
            except OSError:
                continue
        return False

    def list_manual_videos(self, sort="newest", limit=60, offset=0):
        """List single/manual downloads for the Manual Downloads view.

        Single downloads can be saved anywhere — the dedicated video_out_dir
        OR a custom 'Save to' folder — so we list them from the INDEX (every
        download is registered there) rather than just one folder. Two sources,
        merged + de-duplicated by path:
          1. The index — every single download, any location (rich rows:
             channel, video_id, thumbnail, upload date).
          2. A direct walk of video_out_dir — a safety net for any video file
             dropped there that isn't in the index yet.
        Returns paginated rows + the video_out_dir label.
        """
        cfg = self._browse_config()
        folder = (cfg.get("video_out_dir") or cfg.get("output_dir") or "").strip()
        try:
            lim = max(1, min(500, int(limit)))
        except (TypeError, ValueError):
            lim = 60
        try:
            off = max(0, int(offset))
        except (TypeError, ValueError):
            off = 0

        rows: list[dict] = []
        seen: set[str] = set()
        duplicate_index_paths: set[str] = set()
        folder_fallback_paths: set[str] = set()

        # 1. Index-registered single downloads (any location on disk).
        #    include_thumbs=False is CRITICAL: the per-row channel-wide
        #    thumbnail walk resolves a loose file's "channel root" to a huge
        #    folder on pooled storage and hung the view for minutes. We do a
        #    cheap per-page thumbnail lookup after pagination instead.
        try:
            from backend import index as _idx
            try:
                duplicate_index_paths = {
                    os.path.normcase(os.path.normpath(p))
                    for p in _idx.list_manual_duplicate_filepaths()
                    if p
                }
            except Exception:
                duplicate_index_paths = set()
            for r in _idx.list_manual_videos(include_thumbs=False):
                fp = r.get("filepath") or ""
                if not fp:
                    continue
                if _is_system_temp_path(fp) or not os.path.isfile(fp):
                    continue
                key = os.path.normcase(os.path.normpath(fp))
                if key in seen:
                    continue
                seen.add(key)
                rows.append(r)
        except Exception as e:
            _log.debug("list_manual_videos index query failed: %s", e)

        # 2. Folder-walk video_out_dir for any not-yet-indexed files.
        #    Root-only by design; subfolders under this parent include channel
        #    sync archives and separate user collections.
        if folder and os.path.isdir(folder):
            try:
                for fp in self._iter_manual_folder_videos(folder):
                    key = os.path.normcase(os.path.normpath(fp))
                    if key in seen or key in duplicate_index_paths:
                        continue
                    try:
                        st = os.stat(fp)
                    except OSError:
                        continue
                    seen.add(key)
                    folder_fallback_paths.add(key)
                    rows.append({
                        "filepath": fp,
                        "title": os.path.splitext(os.path.basename(fp))[0],
                        "size_bytes": st.st_size,
                        "mtime": st.st_mtime,
                        "channel": "", "video_id": "", "thumbnail_url": "",
                        "upload_ts": None, "added_ts": None,
                        "show_channel": True,
                    })
            except OSError as e:
                _log.debug("list_manual_videos scandir failed: %s", e)

        # Sort (newest/oldest by YT upload date → added → file mtime, all secs).
        def _tkey(r):
            return (r.get("upload_ts") or r.get("added_ts")
                    or r.get("mtime") or 0)
        sort = str(sort or "newest")
        if sort == "newest":
            rows.sort(key=_tkey, reverse=True)
        elif sort == "oldest":
            rows.sort(key=_tkey)
        elif sort == "largest":
            rows.sort(key=lambda r: r.get("size_bytes") or 0, reverse=True)
        elif sort == "title":
            rows.sort(key=lambda r: (r.get("title") or "").lower())

        page = rows[off: off + lim + 1]
        has_more = len(page) > lim
        page = page[:lim]

        # Never run ffprobe or generate thumbnails in this bridge request.
        # Those operations can take tens of seconds per file on pooled storage.
        # Looking up an ALREADY-EXISTING sidecar is cheap, though, and returning
        # it with the page is much more reliable than racing one evaluate_js
        # callback per card against the frontend render. Resolve ready sidecars
        # synchronously; only true misses go to the background generator below.
        try:
            from backend import index as _idx
            for r in page:
                if r.get("thumbnail_url"):
                    continue
                fp = r.get("filepath") or ""
                if not fp:
                    continue
                tp = _idx.find_thumbnail(fp, r.get("video_id") or "")
                if not tp:
                    continue
                r["thumbnail_url"] = _idx._file_url(tp)
                if ".local." in os.path.basename(tp).lower():
                    r["thumbnail_source"] = "local"
        except Exception as e:
            _log.debug("manual ready-thumbnail lookup failed: %s", e)

        # Indexed values and ready sidecars render now; only missing durations
        # and thumbnails patch into the cards later.
        duration_candidates = []
        for r in page:
            if r.get("duration"):
                continue
            candidate = dict(r)
            key = os.path.normcase(os.path.normpath(r.get("filepath") or ""))
            candidate["register_if_missing"] = key in folder_fallback_paths
            duration_candidates.append(candidate)
        self._queue_manual_duration_backfill(duration_candidates)

        self._queue_manual_local_thumbnail_backfill(
            [r for r in page if not r.get("thumbnail_url")])

        for r in page:
            if (r.get("tx_status") or "").lower() not in (
                    "transcribed", "done", "no_speech"):
                if self._manual_has_local_transcript(r):
                    r["tx_status"] = "transcribed"
            badges = []
            if not r.get("video_id"):
                if r.get("id_backfill_excluded_ts"):
                    badges.append({"label": "ID excluded", "kind": "bad"})
                else:
                    badges.append({"label": "No ID", "kind": "warn"})
            elif r.get("id_backfill_tried_ts"):
                badges.append({"label": "Recovered ID", "kind": "ok"})
            tx = (r.get("tx_status") or "").lower()
            if r.get("thumbnail_source") == "local":
                badges.append({"label": "Local thumb", "kind": "ok"})
            if r.get("video_id") and not (
                    r.get("views") or r.get("view_count") is not None):
                badges.append({"label": "Metadata missing", "kind": "warn"})
            if tx in ("failed", "error"):
                badges.append({"label": "Transcript failed", "kind": "bad"})
            elif tx == "no_speech":
                badges.append({"label": "No transcript", "kind": "neutral"})
            elif tx not in ("transcribed", "done"):
                badges.append({"label": "No transcript", "kind": "bad"})
            if badges:
                r["manual_badges"] = badges

        return {"rows": page, "has_more": has_more, "folder": folder,
                "total": len(rows)}
