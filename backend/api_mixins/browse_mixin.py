"""
BrowseMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class BrowseMixin:

    # ─── Browse tab (reads from transcription_index.db) ────────────────

    def browse_list_channels(self):
        """Return a list of channels with video counts + avatar/banner URLs.

        Avatar + banner paths are filled in from
        `<channel>/.ChannelArt/{avatar,banner}.jpg` when the files exist
        (dropped there by `chan_fetch_art` / metadata sweep). The frontend
        renders the avatar in the channel-grid card background.
        """
        cfg = load_config()
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
            cfg = load_config()
            total_channels = len(cfg.get("channels", []))
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
        return index_backend.list_videos_for_channel(channel, sort=sort, limit=limit)


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
        )
        try:
            src_info = self._classify_transcript_source(
                payload.get("title") or "",
                payload.get("jsonl_path") or "",
                payload.get("video_id") or "")
        except Exception:
            src_info = {"source": "unknown", "raw": ""}
        return {"ok": True, "segments": segs, "source": src_info}


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
                _log.debug("swallowed: %s", e)
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
        _hdr_re = _re_cl.compile(
            r"^\((.*)\)\s*,\s*\(([^()]+)\)\s*,\s*\(([^()]+)\)\s*,"
            r"\s*\(([^()]+)\)\s*$"
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
                    with open(fp, "r", encoding="utf-8", errors="replace") as fh:
                        # Scan ALL header lines, keep the last matching one.
                        for line in fh:
                            if not line.startswith("==="):
                                continue
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

        Matches YTArchiver.py:29598 viewer pane data flow — grab N segments
        before + hit + N segments after, let the user pull in more with
        Up/Down "Load more" buttons.
        """
        try:
            seg_id = int((payload or {}).get("segment_id") or 0)
            before = int((payload or {}).get("before") or 30)
            after = int((payload or {}).get("after") or 30)
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
            title = (payload or {}).get("title") or ""
            channel = (payload or {}).get("channel") or ""
            if not video_id:
                import re as _re
                m = _re.search(r"\[([A-Za-z0-9_-]{11})\]",
                               os.path.basename(filepath))
                if m:
                    video_id = m.group(1)
            if not video_id:
                return {"ok": False, "error": "No video_id"}

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
                                entries = _read_metadata_jsonl(os.path.join(cur, fn))
                                if video_id in entries:
                                    return {"ok": True, "meta": entries[video_id],
                                            "source": os.path.join(cur, fn)}
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
            cfg = self._config or load_config()
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
                                entries = _read_metadata_jsonl(os.path.join(dp, fn))
                                if video_id in entries:
                                    return {"ok": True, "meta": entries[video_id],
                                            "source": os.path.join(dp, fn)}
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
            cfg = self._config or load_config()
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
                self._log_stream, emit_inline_log=False, refresh=True)
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
            except Exception as e: _log.debug("swallowed: %s", e)
            return ret if ret.get("ok") else {"ok": True, "meta": res.get("entry")}
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
                for lbl, v in zip(labels, values):
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
            fp = os.path.normpath(filepath or "")
            if not fp or not os.path.isfile(fp):
                return {"ok": False, "error": "File not found"}
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
            fp = os.path.normpath(filepath)
            if not os.path.isfile(fp):
                return {"ok": False, "error": f"File not found: {fp}"}
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
            fp = os.path.normpath(filepath)
            if not os.path.exists(fp):
                return {"ok": False, "error": f"Not found: {fp}"}
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
