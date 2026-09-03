"""Focused duration-default contracts for channel creation and editing."""

from __future__ import annotations

import json

import pytest

from backend import subs, ytarchiver_config

_MISSING = object()


@pytest.mark.parametrize(
    ("supplied", "expected_seconds"),
    (
        pytest.param(0, 0, id="explicit-number-zero"),
        pytest.param("0", 0, id="explicit-string-zero"),
        pytest.param(_MISSING, 180, id="absent-uses-configured-default"),
        pytest.param(None, 180, id="none-uses-configured-default"),
        pytest.param("", 180, id="empty-uses-configured-default"),
    ),
)
def test_add_channel_distinguishes_zero_from_unspecified_minimum(
    tmp_path,
    monkeypatch,
    supplied,
    expected_seconds,
) -> None:
    config_file = tmp_path / "ytarchiver_config.json"
    config_file.write_text(
        json.dumps({
            "channels": [],
            "output_dir": str(tmp_path / "Archive"),
            "min_duration": 180,
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(ytarchiver_config, "APP_DATA_DIR", tmp_path)
    monkeypatch.setattr(ytarchiver_config, "CONFIG_FILE", config_file)

    payload = {
        "url": "https://www.youtube.com/@duration_fixture",
        "name": "Duration Fixture",
    }
    if supplied is not _MISSING:
        payload["min_duration"] = supplied

    channel = subs.add_channel(payload)

    assert channel["min_duration"] == expected_seconds


def test_full_edit_preserves_omitted_duration_limits(tmp_path, monkeypatch) -> None:
    """A blank editor field is omitted even though the URL is still present."""
    config_file = tmp_path / "ytarchiver_config.json"
    url = "https://www.youtube.com/@duration_fixture"
    config_file.write_text(
        json.dumps({
            "channels": [{
                "name": "Duration Fixture",
                "folder": "Duration Fixture",
                "url": url,
                "resolution": "720",
                "min_duration": 180,
                "max_duration": 900,
            }],
            "output_dir": str(tmp_path / "Archive"),
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(ytarchiver_config, "APP_DATA_DIR", tmp_path)
    monkeypatch.setattr(ytarchiver_config, "CONFIG_FILE", config_file)

    updated = subs.update_channel(
        {"url": url},
        {
            "folder": "Duration Fixture",
            "url": url,
            "resolution": "1080",
            "auto_transcribe": True,
            "auto_metadata": True,
            # min_duration / max_duration intentionally omitted, matching
            # pywebview serialization of blank editor fields.
        },
    )

    assert updated["min_duration"] == 180
    assert updated["max_duration"] == 900
    assert updated["resolution"] == "1080"
