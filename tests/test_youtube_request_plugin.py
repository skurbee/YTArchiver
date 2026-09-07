"""Exercise the yt-dlp request guard without importing the app or contacting YouTube."""

import importlib.util
import json
import socket
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from urllib.request import Request

import pytest

PLUGIN = (Path(__file__).resolve().parents[1] / "backend" / "yt_dlp_plugins"
          / "ytarchiver" / "yt_dlp_plugins" / "postprocessor" / "ytarchiver_traffic.py")
TOKEN = "fixture_token_" + "a" * 32
APPROVED = b'{"ok":true}\n'


@pytest.fixture
def plugin(monkeypatch):
    class PostProcessor:
        def __init__(self, downloader=None):
            self._downloader = downloader

    for name in ("yt_dlp", "yt_dlp.postprocessor", "yt_dlp.postprocessor.common"):
        module = ModuleType(name)
        if name.endswith("common"):
            module.PostProcessor = PostProcessor
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location("request_guard_under_test", PLUGIN)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setenv("YTARCHIVER_TRAFFIC_PORT", "12345")
    monkeypatch.setenv("YTARCHIVER_TRAFFIC_TOKEN", TOKEN)
    return module


class BrokerSocket:
    def __init__(self, replies, events):
        self.replies = iter(replies)
        self.events = events
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def settimeout(self, timeout):
        assert timeout == 1.0

    def connect(self, address):
        assert address == ("127.0.0.1", 12345)

    def sendall(self, message):
        assert len(message) <= 4096 and message.endswith(b"\n")
        payload = json.loads(message)
        assert set(payload) == {"token", "op", "kind"}
        assert payload["token"] == TOKEN
        self.events.append((payload["op"], payload["kind"]))

    def recv(self, size):
        assert 0 < size <= 4097
        reply = next(self.replies)
        if isinstance(reply, Exception):
            raise reply
        return reply


@pytest.fixture
def broker(monkeypatch, plugin):
    class Broker:
        def __init__(self):
            self.pending = []
            self.connections = []
            self.events = []

        def reply(self, *chunks):
            self.pending.append(chunks)

        def socket(self, family, socket_type):
            assert (family, socket_type) == (socket.AF_INET, socket.SOCK_STREAM)
            assert self.pending, "Unexpected broker call"
            connection = BrokerSocket(self.pending.pop(0), self.events)
            self.connections.append(connection)
            return connection

    instance = Broker()
    monkeypatch.setattr(plugin.socket, "socket", instance.socket)
    return instance


class Downloader:
    def __init__(self, events=None, outcome=None):
        self.events = events if events is not None else []
        self.outcome = outcome
        self.requests = []

    def urlopen(self, request):
        self.events.append(("network", None))
        self.requests.append(request)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


@pytest.mark.parametrize("variable,value", [
    ("YTARCHIVER_TRAFFIC_PORT", None),
    ("YTARCHIVER_TRAFFIC_PORT", ""),
    ("YTARCHIVER_TRAFFIC_PORT", "0"),
    ("YTARCHIVER_TRAFFIC_PORT", "65536"),
    ("YTARCHIVER_TRAFFIC_PORT", "1.5"),
    ("YTARCHIVER_TRAFFIC_PORT", "１２３４５"),
    ("YTARCHIVER_TRAFFIC_PORT", "12345\n"),
    ("YTARCHIVER_TRAFFIC_TOKEN", None),
    ("YTARCHIVER_TRAFFIC_TOKEN", "short"),
    ("YTARCHIVER_TRAFFIC_TOKEN", "x" * 257),
    ("YTARCHIVER_TRAFFIC_TOKEN", "x" * 32 + "\n"),
])
def test_invalid_environment_prevents_startup(monkeypatch, plugin, variable, value):
    if value is None:
        monkeypatch.delenv(variable)
    else:
        monkeypatch.setenv(variable, value)
    downloader = Downloader()
    original = downloader.urlopen
    with pytest.raises(plugin._TrafficSafetyError, match="configuration"):
        plugin.YTArchiverTrafficGuardPP(downloader)
    assert downloader.urlopen == original
    assert not downloader.requests


def test_constructor_guards_before_postprocessing_and_keeps_run_noop(plugin, broker):
    broker.reply(APPROVED)
    result = object()
    downloader = Downloader(broker.events, outcome=result)
    postprocessor = plugin.YTArchiverTrafficGuardPP(downloader)
    assert downloader.urlopen("https://www.youtube.com/watch?v=fixture") is result
    assert broker.events == [("acquire", "youtube_http"), ("network", None)]
    information = {"id": "fixture"}
    assert postprocessor.run(information) == ([], information)
    assert postprocessor.run(information)[1] is information
    assert broker.connections[0].closed


@pytest.mark.parametrize("network_request", [
    "https://www.youtube.com/watch?v=fixture",
    Request("https://www.youtube.com/watch?v=fixture"),
    SimpleNamespace(url="https://www.youtube.com/watch?v=fixture"),
])
def test_supported_request_shapes_are_admitted_without_replacing_input(plugin, broker, network_request):
    broker.reply(APPROVED)
    downloader = Downloader(broker.events)
    plugin.YTArchiverTrafficGuardPP(downloader)
    downloader.urlopen(network_request)
    assert downloader.requests == [network_request]
    assert broker.events[0] == ("acquire", "youtube_http")


@pytest.mark.parametrize("url,kind", [
    ("https://youtube.com/watch?v=fixture", "youtube_http"),
    ("https://m.youtube.com/watch?v=fixture", "youtube_http"),
    ("https://www.YouTube.com./watch?v=fixture", "youtube_http"),
    ("https://youtube.com。/watch?v=fixture", "youtube_http"),
    ("https://youtu.be/fixture", "youtube_http"),
    ("https://www.youtube-nocookie.com/embed/fixture", "youtube_http"),
    ("https://youtubei.googleapis.com/youtubei/v1/player", "youtube_http"),
    ("https://youtube.googleapis.com/youtube/v3/videos", "youtube_http"),
    ("https://i.ytimg.com/vi/fixture/default.jpg", "youtube_http"),
    ("https://yt3.ggpht.com/fixture", "youtube_http"),
    ("https://lh3.googleusercontent.com/fixture", "youtube_http"),
    ("https://accounts.google.com/ServiceLogin", "youtube_http"),
    ("https://www.youtube.com/api/timedtext?v=fixture&signature=private", "youtube_caption"),
    ("https://manifest.googlevideo.com/api/manifest/dash/fixture", "youtube_media_manifest"),
    ("https://r1.googlevideo.com/control", "youtube_media_manifest"),
    ("https://www.youtube.com/api/manifest/hls_variant/fixture", "youtube_media_manifest"),
    ("https://www.youtube.com/fixture.mpd", "youtube_media_manifest"),
])
def test_youtube_control_endpoints_are_counted(plugin, broker, url, kind):
    broker.reply(APPROVED)
    downloader = Downloader(broker.events)
    plugin.YTArchiverTrafficGuardPP(downloader)
    downloader.urlopen(url)
    assert broker.events == [("acquire", kind), ("network", None)]


@pytest.mark.parametrize("url", [
    "https://r1.googlevideo.com/videoplayback?id=fixture&signature=private",
    "https://r1.googlevideo.com/videoplayback/fixture",
    "https://example.invalid/",
    "https://notyoutube.com/",
    "https://youtube.com.example.invalid/",
    "https://youtubei.googleapis.com.example.invalid/",
    "https://www.youtube.com@example.invalid/",
    "https://googlevideo.com.example.invalid/videoplayback",
])
def test_media_bytes_and_unrelated_hosts_do_not_consume_budget(plugin, broker, url):
    downloader = Downloader(broker.events)
    plugin.YTArchiverTrafficGuardPP(downloader)
    downloader.urlopen(url)
    assert broker.events == [("network", None)]
    assert not broker.connections


@pytest.mark.parametrize("network_request", [
    None, b"https://youtube.com/", object(), {},
    SimpleNamespace(url=123),
    SimpleNamespace(full_url="https://example.invalid/", url="https://youtube.com/"),
    "", "//youtube.com/", "https:////youtube.com/", "https://youtube.com:bad/",
    "https://youtube.com:65536/", "https://you\ntube.com/", "https://youtube%2ecom/",
    "https://example.invalid\\@youtube.com/",
])
def test_uninspectable_request_fails_closed_and_latches(plugin, broker, network_request):
    downloader = Downloader(broker.events)
    plugin.YTArchiverTrafficGuardPP(downloader)
    with pytest.raises(plugin._TrafficSafetyError):
        downloader.urlopen(network_request)
    with pytest.raises(plugin._TrafficSafetyError):
        downloader.urlopen("https://example.invalid/")
    assert not broker.events


@pytest.mark.parametrize("response", [
    b"", b"invalid\n", b"[]\n", b'{}\n', b'{"ok":1}\n', b'{"ok":"true"}\n',
    b'{"ok":false,"error":"private_signed_url"}\n',
    b'{"ok":true}\nextra', b'{"ok":true}\n\n', b"x" * 4097,
    b'{"ok":\xff}\n', b"[" * 2000 + b"]" * 2000 + b"\n",
    OSError("private_token_in_os_error"),
])
def test_rpc_failure_never_sends_request_or_reacquires(plugin, broker, response):
    broker.reply(response)
    downloader = Downloader(broker.events)
    plugin.YTArchiverTrafficGuardPP(downloader)
    with pytest.raises(plugin._TrafficSafetyError) as failure:
        downloader.urlopen("https://youtube.com/watch?signature=secret")
    assert "private" not in str(failure.value) and "secret" not in str(failure.value)
    with pytest.raises(plugin._TrafficSafetyError):
        downloader.urlopen("https://youtube.com/watch?signature=secret")
    assert not downloader.requests
    assert len(broker.connections) == 1
    assert broker.connections[0].closed


def test_budget_wait_keeps_one_permission_request_across_socket_timeouts(plugin, broker):
    broker.reply(TimeoutError(), b'{"ok":', TimeoutError(), b'true}\n')
    downloader = Downloader(broker.events)
    plugin.YTArchiverTrafficGuardPP(downloader)
    downloader.urlopen("https://youtube.com/")
    assert broker.events == [("acquire", "youtube_http"), ("network", None)]
    assert len(broker.connections) == 1


@pytest.mark.parametrize("stage", ["connect", "sendall"])
def test_connection_or_send_failure_never_retries_ambiguous_permission(monkeypatch, plugin, broker, stage):
    broker.reply(APPROVED)

    def fail(*_):
        raise OSError("private_endpoint_details")

    monkeypatch.setattr(BrokerSocket, stage, fail)
    downloader = Downloader(broker.events)
    plugin.YTArchiverTrafficGuardPP(downloader)
    with pytest.raises(plugin._TrafficSafetyError) as failure:
        downloader.urlopen("https://youtube.com/")
    assert "private" not in str(failure.value)
    with pytest.raises(plugin._TrafficSafetyError):
        downloader.urlopen("https://youtube.com/")
    assert len(broker.connections) == 1
    assert broker.connections[0].closed
    assert not downloader.requests


def test_every_network_retry_reentering_urlopen_consumes_permission(plugin, broker):
    downloader = Downloader(broker.events, outcome=OSError("temporary failure"))
    plugin.YTArchiverTrafficGuardPP(downloader)
    for _ in range(3):
        broker.reply(APPROVED)
        with pytest.raises(OSError, match="temporary failure"):
            downloader.urlopen("https://youtube.com/")
    assert broker.events == [("acquire", "youtube_http"), ("network", None)] * 3


@pytest.mark.parametrize("field", ["status", "status_code", "code", "response"])
def test_http429_notifies_parent_before_original_error_escapes(plugin, broker, field):
    broker.reply(APPROVED)
    broker.reply(APPROVED)
    error = RuntimeError("original HTTP failure")
    setattr(error, field, SimpleNamespace(status=429) if field == "response" else 429)
    downloader = Downloader(broker.events, outcome=error)
    plugin.YTArchiverTrafficGuardPP(downloader)
    with pytest.raises(RuntimeError) as raised:
        downloader.urlopen("https://youtube.com/")
    assert raised.value is error
    assert broker.events == [("acquire", "youtube_http"), ("network", None),
                             ("rate_limit", "youtube_http")]


def test_failed_rate_limit_notification_preserves_error_but_blocks_future_network(plugin, broker):
    broker.reply(APPROVED)
    broker.reply(b"")
    error = RuntimeError("original HTTP failure")
    error.status = 429
    downloader = Downloader(broker.events, outcome=error)
    plugin.YTArchiverTrafficGuardPP(downloader)
    with pytest.raises(RuntimeError) as raised:
        downloader.urlopen("https://youtube.com/")
    assert raised.value is error
    with pytest.raises(plugin._TrafficSafetyError):
        downloader.urlopen("https://r1.googlevideo.com/videoplayback?id=fixture")
    assert len(downloader.requests) == 1
    assert len(broker.connections) == 2


def test_media_http429_also_activates_global_cooldown(plugin, broker):
    broker.reply(APPROVED)
    error = RuntimeError("original HTTP failure")
    error.status = 429
    downloader = Downloader(broker.events, outcome=error)
    plugin.YTArchiverTrafficGuardPP(downloader)
    with pytest.raises(RuntimeError) as raised:
        downloader.urlopen("https://r1.googlevideo.com/videoplayback?id=fixture")
    assert raised.value is error
    assert broker.events == [("network", None), ("rate_limit", "youtube_media")]
    with pytest.raises(plugin._TrafficSafetyError):
        downloader.urlopen("https://r1.googlevideo.com/videoplayback?id=fixture")
    assert len(downloader.requests) == 1
    assert len(broker.connections) == 1


def test_returned_http429_response_is_closed_and_never_treated_as_success(plugin, broker):
    broker.reply(APPROVED)
    broker.reply(APPROVED)
    response = SimpleNamespace(status=429, close=lambda: broker.events.append(("close", None)))
    downloader = Downloader(broker.events, outcome=response)
    plugin.YTArchiverTrafficGuardPP(downloader)
    with pytest.raises(plugin._TrafficSafetyError, match="HTTP 429"):
        downloader.urlopen("https://youtube.com/")
    assert broker.events == [("acquire", "youtube_http"), ("network", None),
                             ("rate_limit", "youtube_http"), ("close", None)]


def test_duplicate_postprocessor_does_not_double_count(plugin, broker):
    broker.reply(APPROVED)
    downloader = Downloader(broker.events)
    plugin.YTArchiverTrafficGuardPP(downloader)
    plugin.YTArchiverTrafficGuardPP(downloader)
    downloader.urlopen("https://youtube.com/")
    assert broker.events == [("acquire", "youtube_http"), ("network", None)]
