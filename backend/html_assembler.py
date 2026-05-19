"""
html_assembler — build web/index.html from web/index.template.html.

The template uses simple `<!-- @include partials/foo.html -->` markers.
Each marker is replaced with the verbatim contents of the named partial
file (path is relative to web/). No conditionals, no variables, no
nested includes — just flat inclusion.

Called once at the start of `main.py` boot. If the assembled
`web/index.html` is already newer than every input file (template +
all partials), assembly is skipped — keeps PyInstaller-bundled launches
fast (no rewrite on every startup).

Why a build step at all?
    Patch 19 (v72.1) split the previously 1,953-line `web/index.html`
    into a small shell template + per-tab / per-dialog partials, so
    contributors can read one section at a time. There's no Node /
    bundler in the project, so this Python helper takes the place of
    a build step.
"""
from __future__ import annotations

import os
import re
from pathlib import Path


_INCLUDE_RE = re.compile(
    r"^(\s*)<!--\s*@include\s+(?P<path>[A-Za-z0-9_./-]+)\s*-->\s*$",
    re.MULTILINE,
)


def assemble_index_html(web_dir: Path | str) -> Path:
    """Assemble `index.template.html` + partials into `index.html`.

    Returns the absolute path of the assembled file. Idempotent: if
    `index.html` is already newer than every input, this is a no-op.
    """
    web = Path(web_dir)
    template = web / "index.template.html"
    output = web / "index.html"

    if not template.is_file():
        # No template = leave index.html alone (some deployments may
        # ship a pre-assembled index.html without the template).
        return output

    # Discover every partial referenced in the template so we can
    # check mtimes for an up-to-date short-circuit.
    src = template.read_text(encoding="utf-8")
    partial_paths: list[Path] = []
    for m in _INCLUDE_RE.finditer(src):
        partial_paths.append(web / m.group("path"))

    # Up-to-date check: skip rewrite if every input mtime is <= output mtime.
    try:
        out_mt = output.stat().st_mtime
        inputs = [template, *partial_paths]
        if all(p.is_file() and p.stat().st_mtime <= out_mt for p in inputs):
            return output
    except OSError:
        pass  # output doesn't exist or unreadable — fall through and assemble

    # Expand each @include marker. Preserves the indentation of the
    # marker so the included content lines up under whatever block
    # it's nested in.
    def _replace(match: re.Match[str]) -> str:
        indent = match.group(1) or ""
        rel = match.group("path")
        partial = web / rel
        try:
            body = partial.read_text(encoding="utf-8")
        except OSError as exc:
            return (
                f"{indent}<!-- @include failed: {rel}: {exc} -->\n"
                f"{indent}<!-- Re-running assembly may resolve this. -->"
            )
        # If the partial's lines aren't already indented, add the marker's
        # indent to each non-blank line so the output reads cleanly.
        # (Detection: if the very first non-blank line has no leading
        # whitespace, treat the partial as un-indented and pad it.)
        first_real = next((ln for ln in body.splitlines() if ln.strip()), "")
        if first_real and not first_real.startswith((" ", "\t")) and indent:
            body = "\n".join(
                (indent + ln) if ln.strip() else ln
                for ln in body.splitlines()
            )
        return body

    assembled = _INCLUDE_RE.sub(_replace, src)
    # Normalize line endings to LF — the original index.html is LF,
    # and CRLF can confuse diff tools / git blame. Browsers don't care.
    assembled = assembled.replace("\r\n", "\n").replace("\r", "\n")
    output.write_bytes(assembled.encode("utf-8"))
    # Bump mtime forward by 1s to make the "all inputs ≤ output" check
    # robust against same-second writes on filesystems with 1s resolution.
    try:
        now = os.path.getmtime(output) + 1
        os.utime(output, (now, now))
    except OSError:
        pass
    return output


__all__ = ["assemble_index_html"]
