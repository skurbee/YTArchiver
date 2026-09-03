"""Statically verify literal frontend bridge calls have Python handlers."""
from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "web"

_BRIDGE_CALL_RE = re.compile(
    r"\b_?bridgeCall\s*\(\s*([\"'])(?P<name>[A-Za-z][A-Za-z0-9_]*)\1"
)
_API_PROPERTY_RE = re.compile(
    r"\b(?:YT\.api|window\.pywebview\.api)\.(?P<name>[A-Za-z][A-Za-z0-9_]*)\b"
)


def _without_js_comments(source: str) -> str:
    """Remove JS comments while preserving quoted string contents."""
    out: list[str] = []
    index = 0
    quote = ""
    while index < len(source):
        char = source[index]
        nxt = source[index + 1] if index + 1 < len(source) else ""
        if quote:
            out.append(char)
            if char == "\\" and index + 1 < len(source):
                index += 1
                out.append(source[index])
            elif char == quote:
                quote = ""
            index += 1
            continue
        if char in ("\"", "'", "`"):
            quote = char
            out.append(char)
            index += 1
            continue
        if char == "/" and nxt == "/":
            index += 2
            while index < len(source) and source[index] not in "\r\n":
                index += 1
            out.append("\n")
            continue
        if char == "/" and nxt == "*":
            index += 2
            while index + 1 < len(source) and source[index:index + 2] != "*/":
                out.append("\n" if source[index] == "\n" else " ")
                index += 1
            index = min(len(source), index + 2)
            continue
        out.append(char)
        index += 1
    return "".join(out)


def frontend_methods(web_dir: Path = WEB) -> set[str]:
    methods: set[str] = set()
    for path in sorted(web_dir.glob("*.js")):
        source = _without_js_comments(path.read_text(encoding="utf-8"))
        methods.update(match.group("name") for match in _BRIDGE_CALL_RE.finditer(source))
        methods.update(match.group("name") for match in _API_PROPERTY_RE.finditer(source))
    return methods


def backend_methods(root: Path = ROOT) -> set[str]:
    methods: set[str] = set()
    paths = [root / "main.py", *sorted((root / "backend" / "api_mixins").glob("*.py"))]
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            if node.name != "Api" and not node.name.endswith("Mixin"):
                continue
            for member in node.body:
                if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if not member.name.startswith("_"):
                        methods.add(member.name)
    return methods


def missing_bridge_methods(root: Path = ROOT) -> list[str]:
    return sorted(frontend_methods(root / "web") - backend_methods(root))


def main() -> int:
    used = frontend_methods()
    missing = missing_bridge_methods()
    if missing:
        print("Frontend bridge contract failed. Missing Python handlers:")
        for name in missing:
            print(f"  {name}")
        return 1
    print(f"Bridge contract passed ({len(used)} literal frontend calls checked).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
