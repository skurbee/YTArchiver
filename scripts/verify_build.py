"""Verify the Windows executable without launching it."""
from __future__ import annotations

import argparse
import re
import struct
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
REQUIRED_ARCHIVE_ENTRIES = (
    "backend\\punct_worker.py",
    "backend\\whisper_worker.py",
    "licenses\\README.md",
    "requirements\\worker-cpu.lock",
    "requirements\\worker-cuda.lock",
    "THIRD_PARTY_NOTICES.md",
    "web\\bridge.js",
    "web\\index.html",
    "web\\punctRestoreDialog.js",
)


def read_pe_machine(path: Path) -> int:
    with path.open("rb") as handle:
        if handle.read(2) != b"MZ":
            raise ValueError("missing MZ header")
        handle.seek(0x3C)
        raw_offset = handle.read(4)
        if len(raw_offset) != 4:
            raise ValueError("truncated DOS header")
        pe_offset = struct.unpack("<I", raw_offset)[0]
        handle.seek(pe_offset)
        if handle.read(4) != b"PE\0\0":
            raise ValueError("missing PE signature")
        raw_machine = handle.read(2)
        if len(raw_machine) != 2:
            raise ValueError("truncated COFF header")
        return struct.unpack("<H", raw_machine)[0]


def archive_listing(path: Path) -> str:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "PyInstaller.utils.cliutils.archive_viewer",
            "-l",
            str(path),
        ],
        cwd=ROOT,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise ValueError("PyInstaller archive could not be inspected")
    # archive_viewer prints each member name using Python repr, so Windows
    # separators appear doubled in stdout.
    return result.stdout.replace("/", "\\").replace("\\\\", "\\")


def _string_resources(path: Path) -> dict[str, str]:
    try:
        import pefile
    except ImportError as exc:  # pragma: no cover - build lock always provides it
        raise ValueError("pefile is required for resource verification") from exc

    pe = pefile.PE(str(path), fast_load=False)
    values: dict[str, str] = {}
    for file_info in getattr(pe, "FileInfo", []) or []:
        for block in file_info:
            if getattr(block, "Key", b"") != b"StringFileInfo":
                continue
            for table in block.StringTable:
                for key, value in table.entries.items():
                    decoded_key = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)
                    decoded_value = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
                    values[decoded_key] = decoded_value
    return values


def expected_version() -> tuple[str, str]:
    from backend.version import APP_VERSION, APP_VERSION_DATE

    return APP_VERSION.lstrip("v"), APP_VERSION_DATE


def verify(path: Path) -> list[str]:
    errors: list[str] = []
    if not path.is_file():
        return ["executable does not exist"]
    if path.stat().st_size < 1_000_000:
        errors.append("executable is unexpectedly small")
    try:
        machine = read_pe_machine(path)
        if machine != 0x8664:
            errors.append(f"wrong PE machine 0x{machine:04x}; expected x64")
    except ValueError as exc:
        errors.append(str(exc))

    try:
        listing = archive_listing(path)
        for entry in REQUIRED_ARCHIVE_ENTRIES:
            if entry not in listing:
                errors.append(f"missing packaged resource: {entry}")
    except ValueError as exc:
        errors.append(str(exc))

    try:
        strings = _string_resources(path)
        version, version_date = expected_version()
        if strings.get("ProductVersion") != version:
            errors.append("ProductVersion does not match backend/version.py")
        if strings.get("FileVersion") != version:
            errors.append("FileVersion does not match backend/version.py")
        comments = strings.get("Comments", "")
        if version_date and not re.search(re.escape(version_date), comments):
            errors.append("version date is missing from the Windows resource")
    except ValueError as exc:
        errors.append(str(exc))
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("exe", nargs="?", type=Path, default=ROOT / "dist" / "YTArchiver.exe")
    args = parser.parse_args(argv)
    errors = verify(args.exe.resolve())
    if errors:
        print("Build verification failed:")
        for error in errors:
            print(f"  {error}")
        return 1
    print(f"Build verification passed: {args.exe.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
