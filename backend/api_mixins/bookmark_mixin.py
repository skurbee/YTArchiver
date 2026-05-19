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
        bid = index_backend.bookmark_add(
            payload.get("video_id", ""), payload.get("title", ""),
            payload.get("channel", ""), float(payload.get("start_time", 0)),
            payload.get("text", ""), payload.get("note", ""),
        )
        return {"ok": bid is not None, "id": bid}


    def bookmark_remove(self, bm_id):
        """JS bridge: delete a transcript bookmark by row id. Called by
        the bookmark list's `×` button."""
        return {"ok": index_backend.bookmark_remove(int(bm_id))}


    def bookmark_update_note(self, bm_id, note):
        """JS bridge: replace the free-text note on an existing bookmark.
        Used by the inline note-edit textbox in the bookmark list."""
        return {"ok": index_backend.bookmark_update_note(int(bm_id), note or "")}


    def bookmark_export_csv(self):
        """Prompt for a save path and write bookmarks to CSV."""
        try:
            import csv
            import io

            import webview as _wv
            bms = index_backend.bookmark_list()
            if not bms:
                return {"ok": False, "error": "No bookmarks to export"}
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["created", "channel", "title", "start_time", "text", "note", "video_id"])
            for b in bms:
                w.writerow([b.get("created"), b.get("channel"), b.get("title"),
                            b.get("start_time"), b.get("text"), b.get("note"),
                            b.get("video_id")])
            if self._window is None:
                return {"ok": False, "error": "No window"}
            paths = self._window.create_file_dialog(
                _wv.SAVE_DIALOG,
                save_filename="ytarchiver_bookmarks.csv",
            )
            if not paths:
                return {"ok": False, "cancelled": True}
            path = paths if isinstance(paths, str) else paths[0]
            with open(path, "w", encoding="utf-8", newline="") as f:
                f.write(buf.getvalue())
            return {"ok": True, "path": path, "count": len(bms)}
        except Exception as e:
            return {"ok": False, "error": str(e)}
