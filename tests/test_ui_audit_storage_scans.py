"""Fresh metadata counts and multi-root integrity in disposable archives."""
import atexit
import json
import os
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-storage-scans-")
atexit.register(_PROFILE.cleanup)
os.environ["APPDATA"] = str(Path(_PROFILE.name) / "roaming")
os.environ["LOCALAPPDATA"] = str(Path(_PROFILE.name) / "local")

from backend import archive_scan, integrity_scan, metadata
from backend.api_mixins.metadata_mixin import MetadataMixin


def test_force_metadata_scan_publishes_actual_count(tmp_path, monkeypatch):
    base = tmp_path / "archive"
    channel = base / "Fixture"
    channel.mkdir(parents=True)
    (channel / "new video.mp4").write_bytes(b"fresh media")
    url = "https://www.youtube.com/@Fixture"
    config = {"output_dir": str(base), "channels": [
        {"name": "Fixture", "folder": "Fixture", "url": url}]}
    monkeypatch.setattr(archive_scan, "DISK_CACHE_FILE", tmp_path / "disk-cache.json")
    monkeypatch.setattr(archive_scan, "load_config", lambda: config)
    monkeypatch.setattr(metadata, "count_video_id_status_bulk", lambda *args, **kwargs: {})
    archive_scan.save_disk_cache({url: {"num_vids": 0, "size_bytes": 0,
        "count_semantics_version": 2, "last_updated": 1, "subscriber_count": 12}})

    class Api(MetadataMixin):
        def _metadata_config(self):
            return config

    rows = Api().get_channel_metadata_status(force=True)
    assert rows[0]["video_count"] == 1
    saved = archive_scan.load_disk_cache()[url]
    assert saved["num_vids"] == 1
    assert saved["size_bytes"] == len(b"fresh media")
    assert saved["subscriber_count"] == 12


def test_scan_publish_preserves_newer_counts_and_subscriber_metadata(tmp_path, monkeypatch):
    monkeypatch.setattr(archive_scan, "DISK_CACHE_FILE", tmp_path / "cache.json")
    archive_scan.save_disk_cache({"channel": {"num_vids": 10, "last_updated": 20,
                                            "subscriber_count": 99}})
    merged = archive_scan.publish_scan_stats({"channel": {
        "num_vids": 1, "last_updated": 10, "subscriber_count": 3}})
    assert merged["channel"]["num_vids"] == 10
    merged = archive_scan.publish_scan_stats({"channel": {
        "num_vids": 11, "last_updated": 30, "subscriber_count": 3}})
    assert merged["channel"]["num_vids"] == 11
    assert merged["channel"]["subscriber_count"] == 99


def test_force_metadata_scan_reports_publish_failure(tmp_path, monkeypatch):
    class Api(MetadataMixin):
        def _metadata_config(self):
            return {"channels": [{"name": "Fixture", "url": "channel"}]}

    monkeypatch.setattr(archive_scan, "scan_all_channels", lambda: {"channel": {}})
    monkeypatch.setattr(archive_scan, "save_disk_cache", lambda _cache: False)
    monkeypatch.setattr(archive_scan, "DISK_CACHE_FILE", tmp_path / "cache.json")
    result = Api().get_channel_metadata_status(force=True)
    assert result["ok"] is False
    assert "Could not save" in result["error"]


def integrity_fixture(tmp_path, *, missing_root=False):
    primary, extra, manual, custom = [tmp_path / name for name in ("primary", "extra", "manual", "custom")]
    for root in (primary, manual, custom):
        root.mkdir()
    if not missing_root:
        extra.mkdir()
    files = [(extra / "Extra.mp4", "aaaaaaaaaa1"),
             (manual / "Manual.mp4", "bbbbbbbbbb2"),
             (custom / "Custom.mp4", "cccccccccc3")]
    for path, _identity in files:
        if path.parent.is_dir():
            path.write_bytes(b"intact")
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"output_dir": str(primary), "channels": [],
                                 "tp_archive_roots": [str(extra)], "video_out_dir": str(manual)}))
    database = tmp_path / "index.db"
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("CREATE TABLE videos(id INTEGER PRIMARY KEY, title TEXT, channel TEXT, "
                           "filepath TEXT, video_id TEXT, availability TEXT, downloaded_ts REAL)")
        for path, identity in files:
            connection.execute("INSERT INTO videos(title,channel,filepath,video_id,availability,downloaded_ts) "
                               "VALUES(?,?,?,?,?,?)", (path.stem, "Fixture", str(path), identity, "available", 1))
        connection.commit()
    archive = tmp_path / "download-archive.txt"
    archive.write_text("\n".join("youtube " + identity for _path, identity in files))
    return integrity_scan.scan_integrity(archive_path=primary, config_path=config,
        db_path=database, queue_path=tmp_path / "queue.json", download_archive_path=archive)


def test_integrity_recognizes_additional_manual_and_custom_roots(tmp_path):
    result = integrity_fixture(tmp_path)
    assert result["summary"]["media_files_seen"] == 3
    assert result["checks"]["saved_media"]["catalog_rows_without_media"] == 0
    assert result["checks"]["saved_media"]["archive_ids_without_media"] == 0
    assert len(result["inputs"]["archive_roots"]) == 3


def test_integrity_defers_missing_media_proposals_for_offline_root(tmp_path):
    result = integrity_fixture(tmp_path, missing_root=True)
    assert result["ok"] is False
    assert result["checks"]["saved_media"] == {"deferred": True}
    assert result["checks"]["canonical_links"]["deferred"] is True
    assert not any(issue["code"] in {"catalog_download_without_media", "download_archive_without_media"}
                   for issue in result["issues"])
