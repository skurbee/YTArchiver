"""High-confidence secret and publication-privacy scan.

Findings intentionally contain only a rule ID, relative filename, and line
number.  The matching text is never retained or printed.
"""
from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True, slots=True)
class Rule:
    rule_id: str
    pattern: re.Pattern[str]


@dataclass(frozen=True, slots=True)
class Finding:
    rule_id: str
    relative_path: str
    line_number: int


RULES = (
    Rule("SECRET_PRIVATE_KEY", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    Rule("SECRET_GITHUB_TOKEN", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b")),
    Rule("SECRET_GITHUB_FINE_GRAINED", re.compile(r"\bgithub_pat_[A-Za-z0-9_]{40,}\b")),
    Rule("SECRET_OPENAI_KEY", re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{24,}\b")),
    Rule("SECRET_AWS_ACCESS_KEY", re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")),
    Rule("SECRET_SLACK_TOKEN", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b")),
    Rule("SECRET_DISCORD_WEBHOOK", re.compile(r"https://(?:canary\.|ptb\.)?discord(?:app)?\.com/api/webhooks/\d+/[A-Za-z0-9_-]+")),
    Rule("SECRET_GOOGLE_API_KEY", re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b")),
    Rule("PRIVATE_WINDOWS_PROFILE", re.compile(r"(?i)\b[A-Z]:\\Users\\(?!Public\\|Default\\|USERNAME\\|Example\\)[^\\\r\n]+")),
    Rule("PRIVATE_ARCHIVE_DRIVE", re.compile(r"(?i)\bZ:\\(?:Archive|Video Archive)\\")),
    Rule(
        "PRIVATE_PRODUCTION_EXAMPLE",
        re.compile("|".join(("neo" + "explains", "Cold" + "Fusion")), re.IGNORECASE),
    ),
)

_TEXT_SUFFIXES = {
    ".css", ".html", ".ini", ".js", ".json", ".md", ".ps1", ".py",
    ".toml", ".txt", ".xml", ".yaml", ".yml",
}


def tracked_files(root: Path = ROOT) -> list[Path]:
    result = subprocess.run(
        [
            "git",
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ],
        cwd=root,
        check=True,
        capture_output=True,
    )
    return [root / raw.decode("utf-8") for raw in result.stdout.split(b"\0") if raw]


def scan_paths(paths: list[Path], *, root: Path = ROOT, rules: tuple[Rule, ...] = RULES) -> list[Finding]:
    findings: list[Finding] = []
    for path in paths:
        if path.suffix.lower() not in _TEXT_SUFFIXES or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            continue
        relative = path.resolve().relative_to(root.resolve()).as_posix()
        for line_number, line in enumerate(text.splitlines(), start=1):
            for rule in rules:
                if rule.pattern.search(line):
                    findings.append(Finding(rule.rule_id, relative, line_number))
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="*", type=Path)
    args = parser.parse_args(argv)
    paths = [path.resolve() for path in args.paths] if args.paths else tracked_files()
    findings = scan_paths(paths)
    if findings:
        print("Repository publication scan failed (matching text is redacted):")
        for finding in findings:
            print(f"  {finding.relative_path}:{finding.line_number} [{finding.rule_id}]")
        return 1
    print(f"Repository publication scan passed ({len(paths)} tracked paths checked).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
