"""
BookmarkMixin — extracted from the main Api class for browsability.

Methods in this mixin are mixed into the Api class via multiple
inheritance. They reference `self.<state>` which still resolves
to the Api instance at runtime — no body changes were made
when moving them out of main.py.
"""
from __future__ import annotations

from ._shared import *  # noqa: F401,F403


class BookmarkMixin:

    # ─── Bookmarks ──────────────────────────────────────────────────────

    def bookmark_list(self):
        # Consistent {ok, rows} shape matching the other bookmark_*
        # methods. Previously returned the raw list, which diverged
        # from the {ok: bool} shape of bookmark_add/remove/update_note
        # and would crash a JS caller that tried to read `.ok` on the
        # array. Legacy callers that iterated directly would stop
        # working — the one known caller has been updated.
        try:
            rows = index_backend.bookmark_list() or []
            return {"ok": True, "rows": rows}
        except Exception as e:
            return {"ok": False, "rows": [], "error": str(e)}


    def bookmark_add(self, payload):
        """JS bridge: persist a new transcript bookmark. `payload` is the
        dict the Watch-view bookmark button sends — video_id, title,
        channel, start_time (seconds into the video), the snippet text
        the user highlighted, and an optional note. Returns the new row
        id so the UI can refer to this bookmark later (for edit/remove)."""
        payload = payload or {}
        try:
            start_time = float(payload.get("start_time", 0) or 0)
        except (TypeError, ValueError):
            start_time = 0.0
        bid = index_backend.bookmark_add(
            payload.get("video_id", ""), payload.get("title", ""),
            payload.get("channel", ""), start_time,
            payload.get("text", ""), payload.get("note", ""),
        )
        return {"ok": bid is not None, "id": bid}


    def bookmark_remove(self, bm_id):
        """JS bridge: delete a transcript bookmark by row id. Called by
        the bookmark list's `×` button."""
        try:
            rid = int(bm_id)
        except (TypeError, ValueError):
            return {"ok": False, "error": "Invalid bookmark id"}
        return {"ok": index_backend.bookmark_remove(rid)}


    def bookmark_update_note(self, bm_id, note):
        """JS bridge: replace the free-text note on an existing bookmark.
        Used by the inline note-edit textbox in the bookmark list."""
        try:
            rid = int(bm_id)
        except (TypeError, ValueError):
            return {"ok": False, "error": "Invalid bookmark id"}
        return {"ok": index_backend.bookmark_update_note(rid, note or "")}


    def bookmark_export_csv(self):
        """Prompt for a save path and write bookmarks to CSV."""
        try:
            import csv
            import io

            import webview as _wv
            bms = index_backend.bookmark_list()
            if not bms:
                return {"ok": False, "error": "No bookmarks to export"}
            import datetime as _dt

            def _fmt_created(v):
                """Render the bookmark's `created` value as a readable
                'YYYY-MM-DD HH:MM:SS'. Accepts a unix-epoch number (or a
                numeric string). Legacy rows may hold the bare placeholder
                string "%s" (an old DEFAULT bug) — those, and anything else
                non-numeric, render as blank rather than leaking junk."""
                try:
                    ts = float(v)
                except (TypeError, ValueError):
                    return ""
                if ts <= 0:
                    return ""
                try:
                    return _dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                except (OverflowError, OSError, ValueError):
                    return ""

            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["created", "channel", "title", "start_time", "text", "note", "video_id"])
            for b in bms:
                w.writerow([_fmt_created(b.get("created")), b.get("channel"), b.get("title"),
                            b.get("start_time"), b.get("text"), b.get("note"),
                            b.get("video_id")])
            if self._window is None:
                return {"ok": False, "error": "No window"}
            paths = self._window.create_file_dialog(
                _wv.SAVE_DIALOG,
                save_filename="ytarchiver_bookmarks.csv",
            )
            path = normalize_dialog_paths(paths)
            if not path:
                return {"ok": False, "cancelled": True}
            # Atomic tmp+replace so a mid-write disk-full doesn't
            # clobber a pre-existing CSV the user picked to overwrite
            # (audit: bookmark_mixin.py:58-88).
            tmp = path + ".tmp"
            try:
                with open(tmp, "w", encoding="utf-8", newline="") as f:
                    f.write(buf.getvalue())
                    try:
                        f.flush()
                        os.fsync(f.fileno())
                    except OSError:
                        pass
                os.replace(tmp, path)
            except Exception:
                try: os.remove(tmp)
                except OSError: pass
                raise
            return {"ok": True, "path": path, "count": len(bms)}
        except Exception as e:
            return {"ok": False, "error": str(e)}
