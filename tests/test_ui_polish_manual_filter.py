"""Manual filters include matches beyond the first page and preserve identities."""
import atexit
import os
import tempfile
from pathlib import Path

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-manual-filter-")
atexit.register(_PROFILE.cleanup)
os.environ["APPDATA"] = str(Path(_PROFILE.name) / "roaming")
os.environ["LOCALAPPDATA"] = str(Path(_PROFILE.name) / "local")

from backend import index
from backend.api_mixins.browse_mixin import BrowseMixin


def test_filter_precedes_pagination_and_treats_wildcards_literally(tmp_path, monkeypatch):
    rows = []
    for number in range(125):
        path = tmp_path / f"video-{number:03}.mp4"
        path.write_bytes(b"fixture")
        rows.append({"title": f"Video {number:03}", "channel": "Example",
                     "filepath": str(path), "duration": "1:00", "tx_status": "transcribed",
                     "thumbnail_url": "fixture", "upload_ts": number + 1})
    rows[0]["title"] = "Rare 100%_match"
    rows[1]["channel"] = "UNIQUE creator"
    api = BrowseMixin()
    monkeypatch.setattr(api, "_browse_config", lambda: {"video_out_dir": str(tmp_path)})
    monkeypatch.setattr(api, "_iter_manual_folder_videos", lambda _folder: [])
    monkeypatch.setattr(api, "_queue_manual_duration_backfill", lambda _rows: None)
    monkeypatch.setattr(api, "_queue_manual_local_thumbnail_backfill", lambda _rows: None)
    monkeypatch.setattr(index, "list_manual_duplicate_filepaths", list)
    monkeypatch.setattr(index, "list_manual_videos", lambda **_kw: [dict(row) for row in rows])
    monkeypatch.setattr("backend.api_mixins.browse_mixin._is_system_temp_path", lambda _path: False)

    initial = api.list_manual_videos(limit=60)
    assert len(initial["rows"]) == 60
    assert all(row["title"] != "Rare 100%_match" for row in initial["rows"])
    matched = api.list_manual_videos(limit=60, query="RARE 100%_MATCH")
    assert [row["filepath"] for row in matched["rows"]] == [rows[0]["filepath"]]
    assert matched["total"] == 1
    assert matched["unfiltered_total"] == 125
    assert not matched["has_more"]
    assert api.list_manual_videos(query="unique")["rows"][0]["filepath"] == rows[1]["filepath"]
    assert api.list_manual_videos(query="no match")["rows"] == []
    assert api.list_manual_videos(query="%_")["total"] == 1
