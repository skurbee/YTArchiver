"""The external downloader's Python baseline is older than the desktop host."""

import ast
import datetime
import importlib.util
import sys
import types
from pathlib import Path
from unittest import mock

import yt_dlp

PLUGIN = (Path(__file__).resolve().parents[1] / "backend" / "yt_dlp_plugins"
          / "ytarchiver" / "yt_dlp_plugins" / "postprocessor" / "ytarchiver_traffic.py")


def test_external_plugin_uses_python310_syntax():
    ast.parse(PLUGIN.read_text(encoding="utf-8"), filename=str(PLUGIN), feature_version=(3, 10))


def test_plugin_import_and_metadata_timestamp_work_without_datetime_utc(monkeypatch):
    # Model the exact standard-library difference that made the frozen
    # downloader reject the plugin before it could construct the guard.
    older_datetime = types.ModuleType("datetime")
    older_datetime.__dict__.update(vars(datetime))
    older_datetime.__dict__.pop("UTC", None)
    monkeypatch.setitem(sys.modules, "datetime", older_datetime)
    spec = importlib.util.spec_from_file_location("python310_traffic_guard", PLUGIN)
    plugin = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(plugin)
    monkeypatch.setenv("YTARCHIVER_TRAFFIC_PORT", "12345")
    monkeypatch.setenv("YTARCHIVER_TRAFFIC_TOKEN", "fixture_token_" + "a" * 32)
    with yt_dlp.YoutubeDL({"getcomments": True, "quiet": True}, auto_init=False) as downloader:
        monkeypatch.setattr(downloader, "urlopen", mock.Mock(
            side_effect=AssertionError("unexpected network request")))
        monkeypatch.setattr(plugin.YTArchiverTrafficGuardPP, "_rpc", mock.Mock(
            side_effect=AssertionError("unexpected broker request")))
        pp = plugin.YTArchiverTrafficGuardPP(downloader)
        info = {"id": "fixture0001", "__post_extractor": lambda: {
            "comments": [], "comment_count": 0,
        }}
        pp.run(info)
        downloader.post_extract(info)
    marker = info["ytarchiver_metadata_snapshot"]
    assert marker["comments_complete"] is True
    assert marker["video_id"] == "fixture0001"
    assert datetime.datetime.fromisoformat(marker["fetched_at"]).utcoffset().total_seconds() == 0
