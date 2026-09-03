"""Fail when the checked-in HTML does not match its template/partials."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.html_assembler import render_index_html


def main() -> int:
    output = ROOT / "web" / "index.html"
    try:
        expected = render_index_html(ROOT / "web")
        actual = output.read_bytes()
    except OSError as exc:
        print(f"Generated HTML check failed: {type(exc).__name__}: {exc}")
        return 1
    if actual != expected:
        print(
            "web/index.html is stale. Run the application HTML assembler, "
            "review the generated diff, and rerun the gate."
        )
        return 1
    print("Generated HTML matches index.template.html and all partials.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
