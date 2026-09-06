"""Identity, root scope and file comparison regressions in isolated state."""
import atexit
import os
import tempfile
from pathlib import Path

import pytest

_PROFILE = tempfile.TemporaryDirectory(prefix="ytarchiver-storage-catalog-")
atexit.register(_PROFILE.cleanup)
os.environ["APPDATA"] = str(Path(_PROFILE.name) / "roaming")
os.environ["LOCALAPPDATA"] = str(Path(_PROFILE.name) / "local")

from backend import index, index_bookmarks
from backend.api_mixins.backup_mixin import _clean_import_channel
from backend.fs_safety import files_equal, sampled_files_equal


@pytest.fixture
def database(tmp_path, monkeypatch):
    index._shutdown_index()
    index._schema_inited = False
    monkeypatch.setattr(index, "TRANSCRIPTION_DB", tmp_path / "index.db")
    monkeypatch.setattr(index, "find_thumbnail", lambda *_args: None)
    connection = index._open()
    assert connection is not None
    yield connection
    index._shutdown_index()
    index._schema_inited = False


def video(connection, path, identity, title="Shared title", channel="Fixture", **extra):
    connection.execute(
        "INSERT INTO videos(filepath,video_id,title,channel,tx_status,availability,is_duplicate_of,added_ts) "
        "VALUES(?,?,?,?,?,?,?,?)", (str(path), identity, title, channel, "transcribed",
                                 extra.get("availability", "available"), extra.get("duplicate"),
                                 extra.get("added", 1)))


def segment(connection, path, identity, text="spoken words", title="Shared title", channel="Fixture"):
    connection.execute(
        "INSERT INTO segments(video_id,title,channel,start_time,end_time,text,jsonl_path) VALUES(?,?,?,?,?,?,?)",
        (identity, title, channel, 0, 1, text, str(path)))
    connection.execute("INSERT OR IGNORE INTO indexed_files VALUES(?,?,?)", (str(path), 1, 1))
    connection.commit()


def test_bookmark_known_id_never_uses_another_video(database, tmp_path):
    video(database, tmp_path / "other.mp4", "bbbbbbbbbb2")
    item = {"video_id": "aaaaaaaaaa1", "title": "Shared title", "channel": "Fixture"}
    index_bookmarks._enrich_video_fields(database, item)
    assert not item.get("filepath")
    item["channel"] = "Different channel"
    index_bookmarks._enrich_video_fields(database, item)
    assert not item.get("filepath")


def test_bookmark_prefers_available_primary_of_same_id(database, tmp_path):
    primary = tmp_path / "primary.mp4"
    video(database, primary, "aaaaaaaaaa1")
    video(database, tmp_path / "duplicate.mp4", "aaaaaaaaaa1", duplicate=str(primary), added=99)
    video(database, tmp_path / "missing.mp4", "aaaaaaaaaa1", availability="missing", added=100)
    item = {"video_id": "aaaaaaaaaa1", "title": "Shared title", "channel": "Fixture"}
    index_bookmarks._enrich_video_fields(database, item)
    assert item["filepath"] == str(primary)


def test_idless_bookmark_requires_unambiguous_channel_title(database, tmp_path):
    video(database, tmp_path / "first.mp4", "aaaaaaaaaa1")
    video(database, tmp_path / "second.mp4", "bbbbbbbbbb2")
    item = {"video_id": "", "title": "Shared title", "channel": "Fixture"}
    index_bookmarks._enrich_video_fields(database, item)
    assert not item.get("filepath")


def test_watch_rejects_different_id_with_identical_title_and_channel(database, tmp_path):
    segment(database, tmp_path / "wrong.jsonl", "bbbbbbbbbb2", "other video's words")
    assert index.get_segments(video_id="aaaaaaaaaa1", title="Shared title",
                              channel="Fixture", strict_identity=True) == []


def test_watch_can_still_resolve_legacy_blank_id(database, tmp_path):
    segment(database, tmp_path / "legacy.jsonl", "", "legacy words")
    rows = index.get_segments(video_id="aaaaaaaaaa1", title="Shared title",
                              channel="Fixture", strict_identity=True)
    assert rows and rows[0]["t"] == "legacy words"


def test_watch_legacy_fallback_rejects_ambiguous_channel_or_known_catalog_id(database, tmp_path):
    segment(database, tmp_path / "one.jsonl", "", channel="One")
    segment(database, tmp_path / "two.jsonl", "", channel="Two")
    assert index.get_segments(video_id="aaaaaaaaaa1", title="Shared title") == []
    video(database, tmp_path / "other.mp4", "bbbbbbbbbb2", channel="One")
    database.commit()
    assert index.get_segments(video_id="aaaaaaaaaa1", title="Shared title", channel="One") == []


@pytest.mark.parametrize("source_path", [None, ""])
def test_watch_filepath_id_keeps_exact_transcript_without_source_path(database, tmp_path, source_path):
    path = tmp_path / "Shared title [good1234567].mp4"
    path.write_bytes(b"fixture")
    video(database, path, "bad12345678")
    segment(database, tmp_path / "older.jsonl", "good1234567", "older copy")
    database.execute(
        "INSERT INTO segments(video_id,title,channel,start_time,end_time,text,jsonl_path) VALUES(?,?,?,?,?,?,?)",
        ("good1234567", "Shared title", "Fixture", 0, 1, "correct transcript", source_path))
    database.execute(
        "INSERT INTO segments(video_id,title,channel,start_time,end_time,text,jsonl_path) VALUES(?,?,?,?,?,?,?)",
        ("bad12345678", "Other title", "Other channel", 0, 1, "wrong transcript", source_path))
    database.commit()
    rows = index.get_segments(video_id="bad12345678", title="Shared title", channel="Fixture",
                              filepath=str(path), strict_identity=True)
    assert [row["t"] for row in rows] == ["correct transcript"]


def test_watch_valid_database_identity_wins_over_misleading_bracket(database, tmp_path):
    path = tmp_path / "Shared title [good1234567].mp4"
    path.write_bytes(b"fixture")
    video(database, path, "bad12345678")
    segment(database, tmp_path / "valid.jsonl", "bad12345678", "database identity words")
    segment(database, tmp_path / "misleading.jsonl", "good1234567", "other video's words")
    rows = index.get_segments(video_id="bad12345678", title="Shared title", channel="Fixture",
                              filepath=str(path), strict_identity=True)
    assert [row["t"] for row in rows] == ["database identity words"]


def test_watch_stale_identity_does_not_choose_among_reused_titles(database, tmp_path):
    path = tmp_path / "Shared title [good1234567].mp4"
    path.write_bytes(b"fixture")
    video(database, path, "bad12345678")
    segment(database, tmp_path / "candidate.jsonl", "good1234567", "candidate words")
    segment(database, tmp_path / "conflict.jsonl", "cccccccccc3", "conflicting words")
    assert index.get_segments(video_id="bad12345678", title="Shared title", channel="Fixture",
                              filepath=str(path), strict_identity=True) == []


def test_deleted_transcript_cleanup_preserves_other_roots_and_failed_deletions(database, tmp_path):
    selected = tmp_path / "archive"
    sibling = tmp_path / "archive-other"
    selected.mkdir()
    sibling.mkdir()
    missing = selected / ".Deleted Transcript.jsonl"
    retained = selected / ".Retained Transcript.jsonl"
    outside = sibling / ".Other Transcript.jsonl"
    retained.write_text("still present")
    outside.write_text("still present")
    for path, identity, text in [(missing, "aaaaaaaaaa1", "deletedword"),
                                 (retained, "bbbbbbbbbb2", "retainedword"),
                                 (outside, "cccccccccc3", "outsideword")]:
        video(database, path.with_suffix(".mp4"), identity)
        segment(database, path, identity, text)
    result = index.clear_missing_transcripts_under_root(str(selected))
    assert result["ok"], result
    assert result["segments"] == 1
    assert database.execute("SELECT text FROM segments ORDER BY text").fetchall() == [
        ("outsideword",), ("retainedword",)]
    statuses = dict(database.execute("SELECT video_id,tx_status FROM videos"))
    assert statuses == {"aaaaaaaaaa1": "pending", "bbbbbbbbbb2": "transcribed",
                        "cccccccccc3": "transcribed"}
    assert database.execute("SELECT COUNT(*) FROM segments_fts WHERE segments_fts MATCH 'outsideword'").fetchone()[0] == 1


@pytest.mark.parametrize("size", [1, 1048577, 1572864, 2097152, 3145728])
def test_small_duplicate_checks_compare_the_final_byte(tmp_path, size):
    left, right = tmp_path / "left.mp4", tmp_path / "right.mp4"
    left.write_bytes(b"a" * size)
    right.write_bytes(b"a" * (size - 1) + b"b")
    assert not sampled_files_equal(str(left), str(right))
    assert not files_equal(str(left), str(right))
    right.write_bytes(left.read_bytes())
    assert sampled_files_equal(str(left), str(right))
    assert files_equal(str(left), str(right))


def test_exact_comparison_reads_unsampled_regions(tmp_path):
    left, right = tmp_path / "left.mp4", tmp_path / "right.mp4"
    size = 8 << 20
    left.write_bytes(b"a" * size)
    right.write_bytes(b"a" * size)
    with right.open("r+b") as stream:
        stream.seek(2 << 20)
        stream.write(b"b")
    assert not files_equal(str(left), str(right))


def test_channel_import_preserves_verified_identity_without_rebind_permission():
    clean, error = _clean_import_channel({
        "name": "Fixture", "url": "https://www.youtube.com/@fixture",
        "channel_id": "UC" + "a" * 22, "channel_identity_rebind_pending": True})
    assert not error
    assert clean["channel_id"] == "UC" + "a" * 22
    assert "channel_identity_rebind_pending" not in clean
    clean, error = _clean_import_channel({"name": "Fixture", "url": "https://www.youtube.com/@fixture",
                                           "channel_id": "invalid"})
    assert clean is None and "channel ID" in error
