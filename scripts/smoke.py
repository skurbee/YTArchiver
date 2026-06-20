"""Cheap local smoke checks for YTArchiver.

Runs the backend unittest smoke suite, verifies web/index.html is already in
sync with its template partials, and syntax-checks frontend JavaScript with
Node when Node is available on PATH.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _run(cmd: list[str]) -> None:
    print("+", " ".join(cmd))
    subprocess.run(cmd, cwd=ROOT, check=True)


def _check_index_fresh() -> None:
    from backend.html_assembler import assemble_index_html

    index_path = ROOT / "web" / "index.html"
    before = index_path.read_bytes() if index_path.exists() else b""
    assemble_index_html(ROOT / "web")
    after = index_path.read_bytes() if index_path.exists() else b""
    if before != after:
        raise SystemExit(
            "web/index.html was stale and has been regenerated; review the diff "
            "and rerun scripts/smoke.py."
        )


def _check_js_syntax() -> None:
    node = shutil.which("node")
    if not node:
        print("node not found; skipping JS syntax checks")
        return
    for path in sorted((ROOT / "web").glob("*.js")):
        _run([node, "--check", str(path)])


def main() -> int:
    _run([sys.executable, "-m", "unittest", "tests.test_backend_smoke"])
    _check_index_fresh()
    _check_js_syntax()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
