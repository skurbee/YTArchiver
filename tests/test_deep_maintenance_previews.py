"""Maintenance previews stay read-only; provenance checkpoints scale linearly."""

import json
import os
from unittest.mock import Mock

import pytest

from backend import provenance, reorg
from backend.services import provenance_ledger
from backend.services.provenance_ledger import ProvenanceLedger
from backend.services.sidecar_store import SidecarValidationError, SidecarWriteError


def _tree(root):
    return {str(path.relative_to(root)): (path.read_bytes(), path.stat().st_mtime_ns)
            for path in root.rglob("*") if path.is_file()}


@pytest.mark.parametrize("recheck", [False, True])
def test_reorg_preview_preserves_tree_bytes_and_mtimes(tmp_path, monkeypatch, recheck):
    channel = tmp_path / "Channel"
    channel.mkdir()
    video = channel / "Video.mp4"
    video.write_bytes(b"video")
    video.with_suffix(".info.json").write_text(json.dumps({"upload_date": "20240102"}))
    os.utime(video, (100, 100))
    monkeypatch.setattr("backend.ytarchiver_config.load_config", lambda: {
        "output_dir": str(tmp_path), "channels": []})
    before = _tree(tmp_path)
    result = reorg.reorg_channel(str(channel), True, False, Mock(),
                                 recheck_dates=recheck, dry_run=True)
    assert result["ok"] and result["moved"] == 1
    assert _tree(tmp_path) == before
    assert sorted(path.name for path in channel.iterdir()) == ["Video.info.json", "Video.mp4"]


def test_reorg_execution_still_applies_authoritative_date(tmp_path, monkeypatch):
    channel = tmp_path / "Channel"
    channel.mkdir()
    video = channel / "Video.mp4"
    video.write_bytes(b"video")
    video.with_suffix(".info.json").write_text(json.dumps({"upload_date": "20240102"}))
    os.utime(video, (100, 100))
    monkeypatch.setattr("backend.ytarchiver_config.load_config", lambda: {
        "output_dir": str(tmp_path), "channels": []})
    monkeypatch.setattr("backend.index.update_video_path", lambda *_args: True)
    expected = reorg._date_from_info_json(video).timestamp()
    result = reorg.reorg_channel(str(channel), True, False, Mock(),
                                 recheck_dates=True, dry_run=False)
    moved = channel / "2024" / "Video.mp4"
    assert result["ok"] and moved.is_file()
    assert moved.stat().st_mtime == expected


def test_provenance_preview_preserves_temporary_media_and_ledger(tmp_path, monkeypatch):
    channel = tmp_path / "Channel"
    channel.mkdir()
    video = channel / "Video.mp4"
    video.write_bytes(b"video")
    stage = channel / "Video.prov.tmp.mp4"
    stage.write_bytes(b"interrupted")
    ledger = tmp_path / "provenance.jsonl"
    ledger.write_bytes(b'{"path":')
    monkeypatch.setattr(provenance, "LEDGER_FILE", ledger)
    monkeypatch.setattr(provenance, "_mp4_worklist", lambda *_a, **_kw: [
        (str(video), "ABCDEFGHIJK", "Video", "Channel")])
    monkeypatch.setattr("backend.compress.find_ffmpeg", lambda: "unused-ffmpeg")
    embed = Mock()
    monkeypatch.setattr(provenance, "_embed_one", embed)
    before = _tree(tmp_path)
    result = provenance.embed_provenance_archive(str(tmp_path), do_txt=False, dry_run=True)
    assert result["succeeded"] == 1
    assert _tree(tmp_path) == before
    embed.assert_not_called()


def _record(number, path=None):
    return {"path": path or f"video-{number}.mp4", "size": number + 1,
            "mtime": 100 + number, "ts": number}


def test_ledger_parses_history_once_and_appends_one_record_each_time(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    reads = Mock(wraps=provenance_ledger.read_bytes)
    monkeypatch.setattr(provenance_ledger, "read_bytes", reads)
    ledger = ProvenanceLedger(path)
    ledger.load(recover=True)
    previous_size = 0
    bytes_added = 0
    for number in range(100):
        ledger.append(_record(number))
        size = path.stat().st_size
        appended = path.read_bytes()[previous_size:]
        assert len(appended.splitlines()) == 1
        assert json.loads(appended)["path"] == f"video-{number}.mp4"
        bytes_added += len(appended)
        previous_size = size
    assert reads.call_count == 1
    assert bytes_added == path.stat().st_size
    assert len(ProvenanceLedger(path).load()) == 100


def test_torn_tail_recovery_preserves_evidence_and_valid_prefix(tmp_path):
    path = tmp_path / "ledger.jsonl"
    good = json.dumps(_record(0)).encode() + b"\n"
    original = good + b'{"path":"unfinished'
    path.write_bytes(original)
    ledger = ProvenanceLedger(path)
    assert len(ledger.load(recover=True)) == 1
    assert path.read_bytes() == good
    assert next(tmp_path.glob("*.interrupted")).read_bytes() == original
    ledger.append(_record(1))
    assert len(ProvenanceLedger(path).load()) == 2


def test_malformed_committed_history_is_never_rewritten(tmp_path):
    path = tmp_path / "ledger.jsonl"
    original = b"{broken}\n" + json.dumps(_record(0)).encode() + b"\n"
    path.write_bytes(original)
    with pytest.raises(SidecarValidationError):
        ProvenanceLedger(path).append(_record(1))
    assert path.read_bytes() == original


def test_failed_append_does_not_advance_memory_or_committed_history(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    ledger = ProvenanceLedger(path)
    ledger.append(_record(0))
    before = path.read_bytes()
    monkeypatch.setattr(provenance_ledger.os, "fsync", Mock(side_effect=OSError("disk full")))
    with pytest.raises(SidecarWriteError):
        ledger.append(_record(1))
    assert path.read_bytes() == before
    assert len(ledger.records) == 1


def test_writable_open_compacts_superseded_records(tmp_path):
    path = tmp_path / "ledger.jsonl"
    with path.open("w", encoding="utf-8") as stream:
        for number in range(1100):
            stream.write(json.dumps(_record(number, "one.mp4")) + "\n")
    ledger = ProvenanceLedger(path)
    assert ledger.load(recover=True)["one.mp4"] == (1100, 1199)
    assert len(path.read_text().splitlines()) == 1


def test_valid_legacy_last_line_without_newline_is_preserved_on_append(tmp_path):
    path = tmp_path / "ledger.jsonl"
    path.write_text(json.dumps(_record(0)), encoding="utf-8")
    ledger = ProvenanceLedger(path)
    assert len(ledger.load()) == 1
    ledger.append(_record(1))
    assert len(ProvenanceLedger(path).load()) == 2
