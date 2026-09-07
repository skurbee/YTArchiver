"""Empty replacement decisions require real, video-specific saved words."""
from __future__ import annotations

import atexit
import json
import os
import tempfile
from pathlib import Path
from unittest import mock

import pytest

_TEST_APPDATA = tempfile.TemporaryDirectory(prefix="ytarchiver-presence-")
atexit.register(_TEST_APPDATA.cleanup)
os.environ["APPDATA"] = _TEST_APPDATA.name
os.environ["LOCALAPPDATA"] = _TEST_APPDATA.name

from backend.transcribe import transcript_presence as presence  # noqa: E402

VIDEO_A = "AAAAABBBBB1"
VIDEO_B = "AAAAABBBBB2"
TITLE = "Target video"


def _paths(root):
    return root / "Channel Transcript.txt", root / ".Channel Transcript.jsonl"


def _header(title=TITLE, video_id=VIDEO_A):
    suffix = f", (youtu.be/{video_id})" if video_id else ""
    return f"===({title}), (01.01.2026), (00:30), (YT CAPTIONS){suffix}==="


def _jsonl(path, records):
    path.write_text("".join(json.dumps(record) + "\n" for record in records),
                    encoding="utf-8")


def _check(root, title=TITLE, video_id=VIDEO_A):
    txt, jsonl = _paths(root)
    return presence.has_existing_transcript(str(txt), str(jsonl), title, video_id)


@pytest.mark.parametrize("contents", [None, b"", b"\n \n"])
def test_missing_or_empty_files_do_not_establish_prior_transcript(tmp_path, contents):
    if contents is not None:
        for path in _paths(tmp_path):
            path.write_bytes(contents)
    assert not _check(tmp_path)


@pytest.mark.parametrize("record,expected", [
    ({"title": "Old title", "video_id": VIDEO_A, "text": "Saved words"}, True),
    ({"title": TITLE, "video_id": VIDEO_B, "text": "Other video's words"}, False),
    ({"title": TITLE, "video_id": VIDEO_A, "text": " \n "}, False),
    ({"title": TITLE, "video_id": VIDEO_A}, False),
    ({"title": " Target  video! ", "text": "Legacy words"}, True),
    ({"title": "Another title", "text": "Unrelated words"}, False),
])
def test_jsonl_evidence_uses_stable_identity_and_nonempty_text(tmp_path, record, expected):
    _jsonl(_paths(tmp_path)[1], [record])
    assert _check(tmp_path) is expected


def test_other_video_in_both_aggregate_files_is_not_evidence(tmp_path):
    txt, jsonl = _paths(tmp_path)
    txt.write_text(_header(video_id=VIDEO_B) + "\nOther words\n", encoding="utf-8")
    _jsonl(jsonl, [{"title": TITLE, "video_id": VIDEO_B, "text": "Other words"}])
    assert not _check(tmp_path)


@pytest.mark.parametrize("header,body,expected", [
    (_header(title="Old title"), "Saved words", True),
    (_header(video_id=""), "Legacy words", True),
    (_header(title=" Target  video! ", video_id=""), "Legacy words", True),
    (_header(video_id=VIDEO_B), "Other video's words", False),
    (_header(title=f"{TITLE} [{VIDEO_A}]", video_id=""), "Legacy ID words", True),
    (_header(title=f"{TITLE} [{VIDEO_B}]", video_id=""), "Other video's words", False),
    (_header(title="Unrelated title", video_id=""), "Other words", False),
    (_header(), " \n\n ", False),
])
def test_txt_fallback_requires_target_header_and_nonblank_body(tmp_path, header, body, expected):
    _paths(tmp_path)[0].write_text(header + "\n" + body + "\n", encoding="utf-8")
    assert _check(tmp_path) is expected


def test_empty_target_block_does_not_borrow_next_videos_body(tmp_path):
    txt, _ = _paths(tmp_path)
    txt.write_text(_header() + "\n\n" + _header(video_id=VIDEO_B)
                   + "\nOnly the second video has words\n", encoding="utf-8")
    assert not _check(tmp_path)


def test_title_containing_header_delimiters_keeps_its_identity(tmp_path):
    title = "Episode 3 (cont), (2026 edition)"
    _paths(tmp_path)[0].write_text(
        _header(title=title, video_id="") + "\nLegacy words\n", encoding="utf-8")
    assert _check(tmp_path, title=title)


@pytest.mark.parametrize("payload", [b'{"text":', b"[]\n", b"\xff\n",
    b'{"title": ["bad"], "text": "words"}\n',
    b'{"video_id": 5, "text": "words"}\n',
    b'{"text": {"bad": true}}\n'])
def test_corrupt_jsonl_is_uncertain_even_when_txt_has_target_words(tmp_path, payload):
    txt, jsonl = _paths(tmp_path)
    txt.write_text(_header() + "\nSaved words\n", encoding="utf-8")
    jsonl.write_bytes(payload)
    with pytest.raises((OSError, ValueError)):
        _check(tmp_path)


@pytest.mark.parametrize("payload", [b"\xff", b"===(broken header)===\nWords\n",
    b"Unattributed words without an identity header\n"])
def test_corrupt_txt_is_uncertain_even_when_jsonl_has_target_words(tmp_path, payload):
    txt, jsonl = _paths(tmp_path)
    txt.write_bytes(payload)
    _jsonl(jsonl, [{"title": TITLE, "video_id": VIDEO_A, "text": "Saved words"}])
    with pytest.raises((OSError, ValueError)):
        _check(tmp_path)


def test_malformed_later_record_is_not_hidden_by_earlier_target_match(tmp_path):
    _, jsonl = _paths(tmp_path)
    _jsonl(jsonl, [{"title": TITLE, "video_id": VIDEO_A, "text": "Saved words"}])
    jsonl.write_bytes(jsonl.read_bytes() + b'{"unfinished":\n')
    with pytest.raises((OSError, ValueError)):
        _check(tmp_path)


@pytest.mark.parametrize("unreadable", ["txt", "jsonl"])
def test_read_failure_propagates_instead_of_claiming_no_transcript(tmp_path, unreadable):
    target = "read_text" if unreadable == "txt" else "read_jsonl"
    with mock.patch.object(presence, target, side_effect=PermissionError("Fixture denied")):
        with pytest.raises(OSError, match="Fixture denied"):
            _check(tmp_path)


def test_presence_check_reads_only_resolved_files_and_preserves_all_bytes(tmp_path):
    txt, jsonl = _paths(tmp_path)
    txt.write_text(_header() + "\nSaved words\n", encoding="utf-8")
    _jsonl(jsonl, [{"title": TITLE, "video_id": VIDEO_A, "text": "Saved words"}])
    before = {path: path.read_bytes() for path in (txt, jsonl)}
    opened = []
    original_open = open

    def checked_open(path, mode="r", *args, **kwargs):
        assert mode == "rb", "Presence checks must not modify files"
        opened.append(Path(path))
        return original_open(path, mode, *args, **kwargs)

    with mock.patch("builtins.open", side_effect=checked_open):
        assert _check(tmp_path)
    assert opened == [jsonl, txt]
    assert {path: path.read_bytes() for path in (txt, jsonl)} == before
