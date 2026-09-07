"""Check the guard against real HTTP response types without making requests."""

import importlib.util
import io
import warnings
from pathlib import Path
from urllib.error import HTTPError as UrllibHTTPError

import pytest
from yt_dlp.globals import IN_CLI
from yt_dlp.networking import Response
from yt_dlp.networking.exceptions import HTTPError
from yt_dlp.utils import deprecation_warning

PLUGIN = (Path(__file__).resolve().parents[1] / "backend" / "yt_dlp_plugins"
          / "ytarchiver" / "yt_dlp_plugins" / "postprocessor" / "ytarchiver_traffic.py")
URL = "https://www.youtube.com/watch?v=fixture0001"


@pytest.fixture
def guard(monkeypatch):
    spec = importlib.util.spec_from_file_location("runtime_request_guard", PLUGIN)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setenv("YTARCHIVER_TRAFFIC_PORT", "12345")
    monkeypatch.setenv("YTARCHIVER_TRAFFIC_TOKEN", "fixture_token_" + "a" * 32)
    monkeypatch.setattr(IN_CLI, "value", False)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        yield module


def install(guard, monkeypatch, outcome):
    events = []

    class Downloader:
        params = {}

        def urlopen(self, request):
            events.append(("network", request))
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    monkeypatch.setattr(guard.YTArchiverTrafficGuardPP, "_rpc",
                        lambda self, operation, kind: events.append((operation, kind)))
    downloader = Downloader()
    guard.YTArchiverTrafficGuardPP(downloader)
    return downloader, events


@pytest.mark.parametrize("status", [200, 206, 302, 404])
def test_real_response_uses_supported_status_without_deprecation(guard, monkeypatch, status):
    with Response(io.BytesIO(b"response body"), URL, {}, status=status) as response:
        downloader, events = install(guard, monkeypatch, response)
        assert downloader.urlopen(URL) is response
        assert response.read() == b"response body"
        assert events == [("acquire", "youtube_http"), ("network", URL)]


def test_real_response_in_cli_mode_does_not_print_a_false_error(guard, monkeypatch, capsys):
    monkeypatch.setattr(IN_CLI, "value", True)
    monkeypatch.setattr(deprecation_warning, "_cache", set())
    with Response(io.BytesIO(b"response body"), URL, {}, status=200) as response:
        downloader, _ = install(guard, monkeypatch, response)
        assert downloader.urlopen(URL) is response
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


@pytest.mark.parametrize("status", [403, 429])
@pytest.mark.parametrize("error_type", [HTTPError, UrllibHTTPError])
def test_real_http_error_is_preserved_and_only_429_latches(guard, monkeypatch, status, error_type):
    response = Response(io.BytesIO(b"error body"), URL, {}, status=status)
    error = (HTTPError(response) if error_type is HTTPError else
             UrllibHTTPError(URL, status, "HTTP failure", {}, response))
    try:
        downloader, events = install(guard, monkeypatch, error)
        with pytest.raises(error_type) as raised:
            downloader.urlopen(URL)
        assert raised.value is error
        expected = [("acquire", "youtube_http"), ("network", URL)]
        if status == 429:
            expected.append(("rate_limit", "youtube_http"))
            with pytest.raises(guard._TrafficSafetyError):
                downloader.urlopen(URL)
            assert events == expected
        else:
            with pytest.raises(error_type) as retried:
                downloader.urlopen(URL)
            assert retried.value is error
            assert events == expected * 2
    finally:
        error.close()


def test_real_returned_429_closes_response_and_blocks_subsequent_requests(guard, monkeypatch):
    response = Response(io.BytesIO(b"rate limited"), URL, {}, status=429)
    downloader, events = install(guard, monkeypatch, response)
    with pytest.raises(guard._TrafficSafetyError, match="HTTP 429"):
        downloader.urlopen(URL)
    assert response.closed and response.fp.closed
    with pytest.raises(guard._TrafficSafetyError):
        downloader.urlopen(URL)
    assert events == [("acquire", "youtube_http"), ("network", URL),
                      ("rate_limit", "youtube_http")]
