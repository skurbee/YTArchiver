"""Channel cards retain a readable original when their cached preview fails."""

import builtins
import io
import os
from pathlib import Path
from urllib.request import urlopen

import pytest
from PIL import Image

from backend import channel_art, local_fileserver
from backend.api_mixins import browse_mixin


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    folder = tmp_path / "Saved Folder"
    art = folder / ".ChannelArt"
    art.mkdir(parents=True)
    config = {"output_dir": str(tmp_path), "channels": [{
        "name": "Display Name", "folder_override": folder.name,
        "url": "https://www.youtube.com/@fixture",
    }]}
    monkeypatch.setattr(browse_mixin.archive_scan, "load_disk_cache", lambda: {
        config["channels"][0]["url"]: {"subscriber_count_checked_at": 1},
    })
    monkeypatch.setattr(browse_mixin.archive_scan, "stats_for_channel", lambda *_: {
        "n_vids": 3, "size_bytes": 1024, "size_gb": 0,
    })
    monkeypatch.setattr(browse_mixin.index_backend, "_reader_open", lambda: None)

    class Api(browse_mixin.BrowseMixin):
        def _browse_fresh_config(self):
            return config

    local_fileserver.stop_server()
    local_fileserver.set_allowed_roots([str(tmp_path)])
    local_fileserver.start_server()
    yield Api(), art
    # Production shutdown is deliberately asynchronous for the UI. Wait for
    # this fixture's listener before the next test creates another server.
    local_fileserver._httpd.shutdown()
    local_fileserver.stop_server()


def write_image(path, size):
    Image.new("RGB", size, "#386291").save(path, "JPEG")


@pytest.mark.parametrize("kind", ["avatar", "banner"])
def test_unreadable_cached_preview_exposes_readable_original(catalog, monkeypatch, kind):
    api, art = catalog
    original = art / f"{kind}.jpg"
    preview = art / f"{kind}_small.jpg"
    write_image(original, (320, 180))
    write_image(preview, (80, 45))
    os.utime(original, (10, 10))
    os.utime(preview, (20, 20))
    original_bytes = original.read_bytes()
    open_file = builtins.open

    def fail_preview(path, mode="r", *args, **kwargs):
        if isinstance(path, (str, os.PathLike)) and Path(path) == preview and "r" in mode:
            raise OSError(22, "Fixture file data unavailable")
        return open_file(path, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fail_preview)
    with pytest.raises(OSError):
        builtins.open(preview, "rb")
    row = api._browse_list_channels_impl()[0]
    assert row["name"] == "Display Name"
    assert row["n_vids"] == 3
    assert row[f"{kind}_fallback_url"]
    assert row[f"{kind}_url"] != row[f"{kind}_fallback_url"]
    with urlopen(row[f"{kind}_fallback_url"], timeout=2) as response:
        body = response.read()
        assert response.status == 200
        assert body == original_bytes
        with Image.open(io.BytesIO(body)) as image:
            image.load()
            assert image.size == (320, 180)


@pytest.mark.parametrize("kind", ["avatar", "banner"])
def test_original_primary_does_not_offer_same_file_as_fallback(catalog, monkeypatch, kind):
    api, art = catalog
    write_image(art / f"{kind}.jpg", (320, 180))
    monkeypatch.setattr(channel_art, "_make_thumb", lambda *_: False)
    row = api._browse_list_channels_impl()[0]
    assert row[f"{kind}_url"]
    assert row[f"{kind}_fallback_url"] is None


def test_unreadable_original_keeps_healthy_cached_preview(catalog, monkeypatch):
    api, art = catalog
    original = art / "banner.jpg"
    preview = art / "banner_small.jpg"
    write_image(original, (320, 180))
    write_image(preview, (80, 45))
    os.utime(original, (10, 10))
    os.utime(preview, (20, 20))
    preview_bytes = preview.read_bytes()
    open_file = builtins.open

    def fail_original(path, mode="r", *args, **kwargs):
        if isinstance(path, (str, os.PathLike)) and Path(path) == original and "r" in mode:
            raise OSError(22, "Fixture original unavailable")
        return open_file(path, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fail_original)
    row = api._browse_list_channels_impl()[0]
    with urlopen(row["banner_url"], timeout=2) as response:
        assert response.status == 200
        assert response.read() == preview_bytes


def test_missing_art_has_no_fake_fallback(catalog):
    api, _art = catalog
    row = api._browse_list_channels_impl()[0]
    for key in ("avatar_url", "banner_url", "avatar_fallback_url", "banner_fallback_url"):
        assert row[key] is None
