"""Storage failures must finish HTTP responses instead of parking image loads."""

import base64
import builtins
import errno
import http.client
import threading
import urllib.parse
from http.server import ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from backend import local_fileserver as fileserver


@pytest.fixture
def server(tmp_path, monkeypatch):
    image = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")
    healthy = tmp_path / "healthy.png"
    healthy.write_bytes(image)
    damaged = tmp_path / "unreadable.jpg"
    damaged.write_bytes(b"x" * (64 * 1024 + 200))
    empty = tmp_path / "empty.jpg"
    empty.write_bytes(b"")
    monkeypatch.setattr(fileserver, "_request_token", "test-token")
    monkeypatch.setattr(fileserver, "_allowed_roots", [fileserver._canonical_path(tmp_path)])
    monkeypatch.setattr(fileserver, "_allowed_files", set())
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), fileserver._FileRequestHandler)
    worker = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    worker.start()

    def url(path, token="test-token"):
        return "/file/" + urllib.parse.quote(str(path), safe="") + "?t=" + token

    def connect():
        return http.client.HTTPConnection("127.0.0.1", httpd.server_port, timeout=1)

    def fail(mode):
        class FaultyFile:
            def __init__(self, actual):
                self.actual = actual
                self.reads = 0
            def __enter__(self):
                return self
            def __exit__(self, *_):
                self.actual.close()
            def seek(self, offset):
                if mode == "seek":
                    raise OSError(errno.EINVAL, "simulated device failure")
                return self.actual.seek(offset)
            def read(self, size):
                self.reads += 1
                if mode == "first" or (mode == "midstream" and self.reads > 1):
                    raise OSError(errno.EINVAL, "simulated device failure")
                if mode == "truncated" and self.reads > 1:
                    return b""
                if mode == "empty-first":
                    return b""
                return self.actual.read(size)

        def open_file(path, mode_arg="r", *args, **kwargs):
            if str(path) == str(damaged) and mode_arg == "rb":
                if mode == "open":
                    raise OSError(errno.EINVAL, "simulated device failure")
                return FaultyFile(builtins.open(path, mode_arg, *args, **kwargs))
            return builtins.open(path, mode_arg, *args, **kwargs)

        monkeypatch.setattr(fileserver, "open", open_file, raising=False)

    yield SimpleNamespace(healthy=healthy, damaged=damaged, empty=empty, image=image,
                          url=url, connect=connect, fail=fail)
    httpd.shutdown()
    httpd.server_close()
    worker.join(2)
    assert not worker.is_alive()


@pytest.mark.parametrize("failure", ["open", "seek", "first", "empty-first"])
def test_unreadable_file_returns_complete_error_before_success_headers(server, failure):
    server.fail(failure)
    connection = server.connect()
    try:
        connection.request("GET", server.url(server.damaged))
        response = connection.getresponse()
        assert response.status == 500
        body = response.read()
        assert len(body) == int(response.getheader("Content-Length"))
        assert response.will_close
        connection.request("GET", server.url(server.healthy))
        response = connection.getresponse()
        assert response.status == 200 and response.read() == server.image
    finally:
        connection.close()


@pytest.mark.parametrize("failure", ["midstream", "truncated"])
@pytest.mark.parametrize("ranged", [False, True])
def test_midstream_failure_closes_connection_instead_of_hanging_body(server, failure, ranged):
    server.fail(failure)
    connection = server.connect()
    try:
        headers = {"Range": "bytes=10-65599"} if ranged else {}
        connection.request("GET", server.url(server.damaged), headers=headers)
        response = connection.getresponse()
        assert response.status == (206 if ranged else 200)
        with pytest.raises(http.client.IncompleteRead) as error:
            response.read()
        assert len(error.value.partial) == 64 * 1024
    finally:
        connection.close()
    connection = server.connect()
    try:
        connection.request("GET", server.url(server.healthy))
        assert connection.getresponse().read() == server.image
    finally:
        connection.close()


def test_successful_range_and_head_keep_existing_protocol_semantics(server):
    connection = server.connect()
    try:
        connection.request("GET", server.url(server.healthy), headers={"Range": "bytes=2-9"})
        response = connection.getresponse()
        assert response.status == 206
        assert response.getheader("Content-Range") == f"bytes 2-9/{len(server.image)}"
        assert response.getheader("Content-Type") == "image/png"
        assert response.getheader("Accept-Ranges") == "bytes"
        assert response.getheader("Cache-Control") == "public, max-age=86400"
        assert response.read() == server.image[2:10]
        connection.request("HEAD", server.url(server.healthy))
        response = connection.getresponse()
        assert response.status == 200
        assert int(response.getheader("Content-Length")) == len(server.image)
        assert response.read() == b""
        connection.request("GET", server.url(server.empty))
        response = connection.getresponse()
        assert response.status == 200 and response.read() == b""
    finally:
        connection.close()


def test_initial_range_read_failure_is_an_error_not_an_incomplete_206(server):
    server.fail("first")
    connection = server.connect()
    try:
        connection.request("GET", server.url(server.damaged), headers={"Range": "bytes=10-20"})
        response = connection.getresponse()
        assert response.status == 500
        assert response.getheader("Content-Range") is None
        assert response.read()
    finally:
        connection.close()


def test_authentication_and_allowlist_still_precede_file_open(server, tmp_path, monkeypatch):
    denied = tmp_path.parent / "outside.jpg"
    opened = []
    monkeypatch.setattr(fileserver, "open", lambda *args: opened.append(args), raising=False)
    for url in (server.url(server.healthy, "wrong-token"), server.url(denied)):
        connection = server.connect()
        try:
            connection.request("GET", url)
            response = connection.getresponse()
            assert response.status == 403
            assert response.read()
        finally:
            connection.close()
    assert not opened


def test_client_can_abort_a_video_response_without_affecting_other_images(server):
    connection = server.connect()
    response = None
    try:
        connection.request("GET", server.url(server.damaged), headers={"Range": "bytes=0-65599"})
        response = connection.getresponse()
        assert response.status == 206
        assert response.read(16) == b"x" * 16
    finally:
        if response is not None:
            response.close()
        connection.close()
    healthy = server.connect()
    try:
        healthy.request("GET", server.url(server.healthy))
        response = healthy.getresponse()
        assert response.status == 200 and response.read() == server.image
    finally:
        healthy.close()
