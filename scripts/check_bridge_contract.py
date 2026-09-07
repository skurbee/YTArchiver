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
    """Resolve Api's real source inheritance without importing application code."""
    modules: dict[str, tuple[dict, dict]] = {}
    classes: dict[tuple[str, str], ast.ClassDef] = {}
    resolving: set[tuple[str, str]] = set()
    linearizations: dict[tuple[str, str], list[tuple[str, str]]] = {}

    def module_symbols(module: str) -> tuple[dict, dict]:
        if module in modules:
            return modules[module]
        path = root.joinpath(*module.split("."))
        source = path.with_suffix(".py")
        package = module.rpartition(".")[0]
        if not source.is_file():
            source = path / "__init__.py"
            package = module
        if not source.is_file():
            raise ValueError(f"Cannot resolve bridge base module: {module}")
        names: dict[str, ast.ClassDef] = {}
        aliases: dict[str, str] = {}
        for node in ast.parse(source.read_text(encoding="utf-8-sig"), filename=str(source)).body:
            if isinstance(node, ast.ClassDef):
                names[node.name] = node
            elif isinstance(node, ast.ImportFrom):
                base = node.module or ""
                if node.level:
                    parts = package.split(".") if package else []
                    if node.level > len(parts):
                        raise ValueError(f"Invalid relative import in {module}")
                    base = ".".join(parts[:len(parts)-node.level+1] + ([base] if base else []))
                for alias in node.names:
                    if alias.name != "*":
                        aliases[alias.asname or alias.name] = f"{base}.{alias.name}"
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    aliases[alias.asname or alias.name.split(".")[0]] = (
                        alias.name if alias.asname else alias.name.split(".")[0])
            elif isinstance(node, ast.Assign) and isinstance(node.value, ast.Name):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        aliases[target.id] = f"{module}.{node.value.id}"
        modules[module] = names, aliases
        return names, aliases

    def resolve(qualified: str, seen: frozenset[str] = frozenset()) -> tuple[str, str]:
        if qualified in seen:
            raise ValueError(f"Cyclic bridge base alias: {qualified}")
        module, _, name = qualified.rpartition(".")
        names, aliases = module_symbols(module)
        if name in names:
            key = module, name
            classes[key] = names[name]
            return key
        if name in aliases:
            return resolve(aliases[name], seen | {qualified})
        raise ValueError(f"Cannot resolve bridge base class: {qualified}")

    def base_class(module: str, expr: ast.expr) -> tuple[str, str] | None:
        if isinstance(expr, ast.Name) and expr.id == "object":
            return None
        parts = []
        while isinstance(expr, ast.Attribute):
            parts.insert(0, expr.attr)
            expr = expr.value
        if not isinstance(expr, ast.Name):
            raise ValueError(f"Unsupported dynamic Api base in {module}")
        _, aliases = module_symbols(module)
        head = aliases.get(expr.id, f"{module}.{expr.id}")
        return resolve(".".join([head, *parts]))

    def mro(key: tuple[str, str]) -> list[tuple[str, str]]:
        if key in linearizations:
            return linearizations[key]
        if key in resolving:
            raise ValueError(f"Cyclic bridge inheritance: {key}")
        resolving.add(key)
        bases = [base for expr in classes[key].bases
                 if (base := base_class(key[0], expr)) is not None]
        sequences = [list(mro(base)) for base in bases] + [list(bases)]
        result = [key]
        while any(sequences):
            sequences = [sequence for sequence in sequences if sequence]
            candidate = next((sequence[0] for sequence in sequences
                              if not any(sequence[0] in other[1:] for other in sequences)), None)
            if candidate is None:
                raise ValueError(f"Inconsistent bridge inheritance: {key}")
            result.append(candidate)
            for sequence in sequences:
                if sequence[0] == candidate:
                    sequence.pop(0)
        resolving.remove(key)
        linearizations[key] = result
        return result

    methods: set[str] = set()
    for key in reversed(mro(resolve("main.Api"))):
        for member in classes[key].body:
            if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if not member.name.startswith("_"):
                    methods.add(member.name)
            elif isinstance(member, (ast.Assign, ast.AnnAssign)):
                targets = member.targets if isinstance(member, ast.Assign) else [member.target]
                for target in targets:
                    if isinstance(target, ast.Name):
                        methods.discard(target.id)
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
