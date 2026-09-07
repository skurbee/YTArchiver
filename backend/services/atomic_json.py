"""Durable JSON publication primitive for recovery journals and manifests."""
from __future__ import annotations

import json
import os
import uuid
from typing import Any


def publish_json(path: str, value: dict[str, Any]) -> str:
    """Flush a unique adjacent stage, then atomically replace its destination."""
    tmp_path = f"{path}.tmp-{uuid.uuid4().hex}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
    return path
