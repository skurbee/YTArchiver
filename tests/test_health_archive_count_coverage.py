"""Partial saved scans cannot masquerade as complete library totals."""
import atexit
import json
import os
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path

import pytest

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-health-counts-")
atexit.register(_PROFILE.cleanup)
os.environ["APPDATA"] = str(Path(_PROFILE.name) / "roaming")
os.environ["LOCALAPPDATA"] = str(Path(_PROFILE.name) / "local")

from backend import archive_scan, index, ytarchiver_config


def _record(videos=3, **updates):
    return {"num_vids": videos, "physical_copies": videos,
            "size_bytes": videos * 100, "last_updated": 123,
            "count_semantics_version": 2, **updates}


def _channels():
    return [{"name": "First", "url": "first"},
            {"name": "Second", "url": "second"},
            {"name": "Empty", "url": "empty"}]


def test_partial_cache_exposes_absent_channels_and_counts_real_zero():
    cache = {"first": _record(4), "empty": _record(0), "removed": _record(999)}
    assert archive_scan.cache_coverage(_channels(), cache) == {
        "complete": False, "cached_channels": 2, "total_channels": 3,
        "missing_channels": ["Second"],
    }
    cache["second"] = _record(6)
    assert archive_scan.cache_coverage(_channels(), cache) == {
        "complete": True, "cached_channels": 3, "total_channels": 3,
        "missing_channels": [],
    }


@pytest.mark.parametrize("bad", [
    None, {}, {"sweep_fingerprint": "only"},
    _record(count_semantics_version=1),
    _record(count_semantics_version="invalid"),
    _record(num_vids=-1), _record(num_vids="invalid"),
    _record(size_bytes=None), _record(physical_copies="invalid"),
    _record(last_updated="invalid"), _record(last_updated=float("nan")),
])
def test_malformed_records_are_incomplete_and_match_healer(monkeypatch, bad):
    cache = {"first": bad, "empty": _record(0)}
    coverage = archive_scan.cache_coverage(_channels(), cache)
    assert coverage["missing_channels"] == ["First", "Second"]
    assert coverage["cached_channels"] == 1
    monkeypatch.setattr(archive_scan, "migrate_legacy_cache_counts", lambda: 0)
    monkeypatch.setattr(archive_scan, "load_disk_cache", lambda: cache)
    saved = []
    monkeypatch.setattr(archive_scan, "save_disk_cache", lambda value: saved.append(value.copy()) or True)
    assert archive_scan.heal_malformed_cache_entries() == 1
    assert saved == [{"empty": _record(0)}]


def test_duplicate_urls_do_not_inflate_coverage_and_missing_urls_stay_incomplete():
    channels = [{"name": "First", "url": " first "},
                {"name": "Same subscription", "url": "first"},
                {"folder": "Needs a URL"}, {}]
    assert archive_scan.cache_coverage(channels, {"first": _record()}) == {
        "complete": False, "cached_channels": 1, "total_channels": 3,
        "missing_channels": ["Needs a URL", "Unnamed channel"],
    }
    assert archive_scan.cache_coverage([], {"orphan": _record()}) == {
        "complete": True, "cached_channels": 0, "total_channels": 0,
        "missing_channels": [],
    }


def test_summary_excludes_orphan_cache_and_reports_partial_scope_without_scan(monkeypatch):
    channels = _channels()
    channels.append({"name": "Duplicate config row", "url": "first"})
    cache = {"first": _record(4, physical_copies=5), "empty": _record(0),
             "second": _record(90, count_semantics_version=1),
             "removed": _record(999)}
    monkeypatch.setattr(archive_scan, "load_config", lambda: {"channels": channels})
    monkeypatch.setattr(archive_scan, "load_disk_cache", lambda: cache)

    def unexpected(*_args, **_kwargs):
        pytest.fail("A summary read must not scan folders, read the catalog, or write state")

    for method in ("scan_all_channels", "scan_channel_folder", "_indexed_channel_stats",
                   "migrate_legacy_cache_counts", "save_disk_cache"):
        monkeypatch.setattr(archive_scan, method, unexpected)
    summary = archive_scan.index_summary()
    assert summary["cards"] == {
        "channels": 4, "scan_complete": False, "scanned_channels": 2,
        "total_channels": 3, "videos": 4, "physical_copies": 5,
        "size_gb": 400 / (1024 ** 3), "size_label": archive_scan._fmt_size(400),
        "transcribed_channels": 0, "transcribed_pct_channels": 0.0,
    }
    by_name = {row["folder"]: row for row in summary["per_channel"]}
    assert by_name["First"]["n_vids"] == 4
    assert by_name["Second"]["n_vids"] == 0
    assert "removed" not in by_name
    assert cache["second"]["num_vids"] == 90


def test_successful_publication_repairs_coverage_without_losing_newer_counts(monkeypatch):
    cache = {"first": _record(7, last_updated=500, subscriber_count=12)}
    monkeypatch.setattr(archive_scan, "load_disk_cache", lambda: cache)
    saved = []
    monkeypatch.setattr(archive_scan, "save_disk_cache", lambda value: saved.append(value.copy()) or True)
    result = archive_scan.publish_scan_stats({
        "first": _record(4, last_updated=100), "second": _record(8), "empty": _record(0),
    })
    assert archive_scan.cache_coverage(_channels(), result)["complete"]
    assert result["first"]["num_vids"] == 7
    assert result["first"]["subscriber_count"] == 12
    assert len(saved) == 1


def test_failed_publication_does_not_claim_success_or_change_saved_file(tmp_path, monkeypatch):
    path = tmp_path / "disk-cache.json"
    before = json.dumps({"first": _record(4)}).encode()
    path.write_bytes(before)
    monkeypatch.setattr(archive_scan, "DISK_CACHE_FILE", path)
    monkeypatch.setattr(archive_scan, "save_disk_cache", lambda _value: False)
    with pytest.raises(OSError, match="Could not save refreshed archive counts"):
        archive_scan.publish_scan_stats({"second": _record(8), "empty": _record(0)})
    assert path.read_bytes() == before
    assert not archive_scan.cache_coverage(_channels())["complete"]


@pytest.mark.parametrize("failure", ["initialization", "connection", "query"])
def test_catalog_statistics_read_failure_is_explicit(tmp_path, monkeypatch, failure):
    path = tmp_path / "catalog.db"
    # A real readable SQLite file with a missing segments table exercises the
    # SQL-error route without ever opening the configured application database.
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE fixture (value INTEGER)")
    monkeypatch.setattr(ytarchiver_config, "TRANSCRIPTION_DB", path)
    monkeypatch.setattr(index, "_schema_inited", failure != "initialization")
    monkeypatch.setattr(index, "_open", lambda: None)
    if failure == "connection":
        def fail_connect(*_args, **_kwargs):
            raise sqlite3.OperationalError("Simulated unavailable catalog")
        monkeypatch.setattr(sqlite3, "connect", fail_connect)
    result = archive_scan.index_db_stats()
    assert result["ok"] is False
    assert "could not be read" in result["error"]
    assert result["total_videos"] == 0
    assert result["transcribed_videos"] == 0
    assert result["hours"] == 0


def test_genuine_empty_catalog_is_distinguished_from_failed_read(tmp_path, monkeypatch):
    path = tmp_path / "empty-catalog.db"
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE segments (id INTEGER)")
    monkeypatch.setattr(ytarchiver_config, "TRANSCRIPTION_DB", path)
    monkeypatch.setattr(index, "_schema_inited", True)
    monkeypatch.setattr(index, "canonical_videos_cte_sql", lambda: (
        "canonical_videos AS (SELECT 1 AS is_available_copy, "
        "0 AS logical_duration_s, 'pending' AS tx_status WHERE 0)"))
    result = archive_scan.index_db_stats()
    assert result["ok"] is True
    assert "error" not in result
    assert result["segments"] == 0
    assert result["total_videos"] == 0
