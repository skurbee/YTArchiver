"""Validation checks every JSONL record without retaining the complete document."""

from __future__ import annotations

import json
import weakref
from types import SimpleNamespace

import pytest

from backend.services import sidecar_store


@pytest.mark.parametrize("payload", [
    pytest.param(b"", id="empty"),
    pytest.param(b" \t\r\n\n", id="blank-lines"),
    pytest.param(b"{}\n", id="empty-object"),
    pytest.param(b'\xef\xbb\xbf{"id":1}\r\n', id="utf8-bom-crlf"),
    pytest.param('{"text":"café ☃","words":[{"w":"hello","s":0}]}\n'.encode(),
                 id="unicode-nested-record"),
    pytest.param(b'{"id":1}\r\n \t\r\n{"id":2}\n', id="multiple-records"),
    # The shared sidecar validator checks object shape, not transcript timestamp
    # semantics. Keep the decoder's existing numeric acceptance unchanged.
    pytest.param(b'{"value":NaN,"other":Infinity}\n', id="decoder-number-compatibility"),
])
def test_validation_accepts_existing_payload_formats(payload):
    assert sidecar_store.validate_jsonl_bytes(payload) is None


@pytest.mark.parametrize("payload", [
    pytest.param(b"[]\n", id="array"),
    pytest.param(b"null\n", id="null"),
    pytest.param(b"true\n", id="boolean"),
    pytest.param(b"12\n", id="number"),
    pytest.param(b'"text"\n', id="string"),
    pytest.param(b'{"bad":}\n', id="invalid-json"),
    pytest.param(b'{"text":"\xff"}\n', id="invalid-utf8"),
    pytest.param(b'{}\n\xef\xbb\xbf{}\n', id="bom-after-first-line"),
])
def test_validation_still_rejects_invalid_records_and_encoding(payload):
    with pytest.raises(sidecar_store.SidecarValidationError):
        sidecar_store.validate_jsonl_bytes(payload)


@pytest.mark.parametrize("payload", [b"{}", b'{"id":1}\n{"id":2}', b"{}\r"])
def test_trailing_newline_requirement_remains_explicit(payload):
    with pytest.raises(sidecar_store.SidecarValidationError, match="incomplete final line"):
        sidecar_store.validate_jsonl_bytes(payload)
    assert sidecar_store.validate_jsonl_bytes(
        payload, require_trailing_newline=False) is None


@pytest.mark.parametrize("last_line", [b'{"broken":}\n', b'["not-an-object"]\n'])
def test_malformed_final_record_rejects_the_entire_staged_replacement(tmp_path, last_line):
    target = tmp_path / "transcript.jsonl"
    original = b'{"original":"must survive"}\n'
    target.write_bytes(original)
    payload = b'{"valid":true}\n' * 256 + last_line

    with pytest.raises(sidecar_store.SidecarValidationError, match=r"<memory>:257:"):
        sidecar_store.atomic_write_bytes(
            target, payload, validator=sidecar_store.validate_jsonl_bytes)

    assert target.read_bytes() == original


def _track_decoded_objects(monkeypatch):
    class TrackedDict(dict):
        __slots__ = ("__weakref__",)

    references = []
    counts = SimpleNamespace(peak=0)

    def decode(payload):
        value = json.loads(payload)
        if isinstance(value, dict):
            value = TrackedDict(value)
            references.append(weakref.ref(value))
            counts.peak = max(counts.peak, sum(ref() is not None for ref in references))
        return value

    # Replace only the target module's decoder reference. Other application
    # modules and pytest retain their ordinary json module.
    monkeypatch.setattr(sidecar_store, "json", SimpleNamespace(
        loads=decode, JSONDecodeError=json.JSONDecodeError))
    return references, counts


@pytest.mark.parametrize("record_count", [8, 128, 512])
def test_validation_releases_decoded_nested_records_as_it_progresses(
        monkeypatch, record_count):
    references, counts = _track_decoded_objects(monkeypatch)
    payload = b'{"text":"some words","words":[{"w":"some","s":0,"e":1}]}\n' * record_count

    sidecar_store.validate_jsonl_bytes(payload)

    # Every record was checked. During assignment the next decode may coexist
    # with the previous value, but memory must not grow with all parsed records.
    assert len(references) == record_count
    assert counts.peak <= 2
    assert all(ref() is None for ref in references)


def test_read_jsonl_still_returns_all_complete_nested_records(tmp_path, monkeypatch):
    expected = [{"id": i, "words": [{"w": f"word {i}", "s": i, "e": i + 1}]}
                for i in range(128)]
    path = tmp_path / "transcript.jsonl"
    path.write_text("\n".join(json.dumps(record) for record in expected), encoding="utf-8-sig")
    references, counts = _track_decoded_objects(monkeypatch)

    result = sidecar_store.read_jsonl(path)

    assert result.exists
    assert result.path == path
    assert result.records == tuple(expected)
    assert result.invalid_lines == ()
    assert len(references) == len(expected)
    assert counts.peak == len(expected)
    assert all(ref() is not None for ref in references)


def test_permissive_read_retains_valid_records_and_original_bad_line_numbers(tmp_path):
    path = tmp_path / "transcript.jsonl"
    path.write_bytes(b'{"id":1}\n\n["invalid"]\n{"bad":}\n{"id":2}')

    result = sidecar_store.read_jsonl(path, invalid="skip")

    assert result.records == ({"id": 1}, {"id": 2})
    assert result.invalid_lines == (3, 4)
    with pytest.raises(sidecar_store.SidecarValidationError, match=r"<memory>:3:"):
        sidecar_store.validate_jsonl_bytes(path.read_bytes(), require_trailing_newline=False)
