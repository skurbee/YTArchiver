"""Exercise complete standalone worker scripts using fake model dependencies."""
import ast
import io
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from backend.worker_protocol import (
    ProtocolError,
    ProtocolWriter,
    decode_request,
    decode_response,
    iter_requests,
)

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("kind,payload", [
    ("whisper", "{bad"), ("whisper", "[]"), ("whisper", "null"),
    ("whisper", '{"path":12}'), ("whisper", '{"path":"ok","duration":-1}'),
    ("whisper", '{"path":"ok","duration":1e500}'),
    ("whisper", '{"command":"unknown"}'), ("punctuation", '{"text":[]}'),
])
def test_invalid_request_gets_one_visible_error_and_stream_continues(kind, payload):
    output = io.StringIO()
    valid = {"path": "fixture.mp4"} if kind == "whisper" else {"text": "fixture"}
    requests = list(iter_requests(io.StringIO(payload + "\n" + json.dumps(valid) + "\n"),
                                  kind, ProtocolWriter(output, model="small")))
    assert requests == [valid]
    error = json.loads(output.getvalue())
    assert error["status"] == "error" and error["model"] == "small"


@pytest.mark.parametrize("payload", ['[]', '{"status":[]}', '{"status":"progress","pct":"bad"}',
                                     '{"status":"ok","text":4}', '{"status":"ok","segments":{}}'])
def test_response_decoder_rejects_invalid_shape(payload):
    with pytest.raises(ProtocolError):
        decode_response(payload)


def test_cancel_and_empty_punctuation_requests_remain_valid():
    assert decode_request('{"command":"cancel"}', "whisper") == {"command": "cancel"}
    assert decode_request('{"text":""}', "punctuation") == {"text": ""}


@pytest.mark.parametrize("worker", ["whisper_worker.py", "punct_worker.py"])
def test_complete_standalone_worker_handles_invalid_requests_then_a_valid_job(tmp_path, worker):
    for filename in (worker, "worker_protocol.py", "punct_alignment.py"):
        source = ROOT / "backend" / filename
        ast.parse(source.read_text(encoding="utf-8"), feature_version=(3, 11))
        shutil.copyfile(source, tmp_path / filename)
    (tmp_path / "faster_whisper.py").write_text(
        "from types import SimpleNamespace\n"
        "class WhisperModel:\n"
        " def __init__(self,*a,**k): pass\n"
        " def transcribe(self,*a,**k):\n"
        "  return iter(()), SimpleNamespace(duration=1)\n", encoding="utf-8")
    (tmp_path / "torch.py").write_text(
        "from types import SimpleNamespace\ncuda=SimpleNamespace(is_available=lambda:False)\n", encoding="utf-8")
    (tmp_path / "transformers.py").write_text("def pipeline(*a,**k): return lambda text: []\n", encoding="utf-8")
    valid = {"path": "synthetic.mp4", "duration": 1} if worker.startswith("whisper") else {"text": "hello world"}
    env = dict(os.environ, APPDATA=str(tmp_path / "roaming"), LOCALAPPDATA=str(tmp_path / "local"),
               PYTHONDONTWRITEBYTECODE="1", WHISPER_MODEL="small")
    result = subprocess.run([sys.executable, "-B", "-s", str(tmp_path / worker)],
                            input="{bad\n[]\n" + json.dumps(valid) + "\n",
                            capture_output=True, text=True, encoding="utf-8", env=env,
                            timeout=5, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert result.returncode == 0, result.stderr
    messages = [json.loads(line) for line in result.stdout.splitlines()]
    assert [message["status"] for message in messages[:3]] == ["ready", "error", "error"]
    assert messages[-1]["status"] == "ok"
    if worker.startswith("whisper"):
        assert all(message["model"] == "small" for message in messages)
    else:
        assert messages[-1]["text"] == "Hello world"
