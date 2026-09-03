"""Import the backend in a disposable application-data environment.

``main.py`` is compiled but deliberately not imported: importing it acquires
the Windows single-instance mutex, which is an application action rather than
an import-graph check.
"""
from __future__ import annotations

import compileall
import importlib
import os
import pkgutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP = {
    "backend.punct_worker",
    "backend.whisper_worker",
}
REQUIRED_RUNTIME_IMPORTS = ("PIL", "psutil", "pystray", "webview")


def import_backend() -> list[str]:
    """Return sanitized import failures; never use the user's real APPDATA."""
    failures: list[str] = []
    with tempfile.TemporaryDirectory(prefix="ytarchiver-import-check-") as raw:
        sandbox = Path(raw)
        old_env = {
            name: os.environ.get(name)
            for name in ("APPDATA", "LOCALAPPDATA", "YTARCHIVER_BOOT_TRACE")
        }
        os.environ["APPDATA"] = str(sandbox / "Roaming")
        os.environ["LOCALAPPDATA"] = str(sandbox / "Local")
        os.environ.pop("YTARCHIVER_BOOT_TRACE", None)
        try:
            if str(ROOT) not in sys.path:
                sys.path.insert(0, str(ROOT))
            for name in REQUIRED_RUNTIME_IMPORTS:
                try:
                    importlib.import_module(name)
                except Exception as exc:  # noqa: BLE001 - import boundary
                    failures.append(f"{name}: {type(exc).__name__}")

            import backend

            for module in pkgutil.walk_packages(backend.__path__, "backend."):
                if module.name in SKIP:
                    continue
                try:
                    importlib.import_module(module.name)
                except Exception as exc:  # noqa: BLE001 - import boundary
                    failures.append(f"{module.name}: {type(exc).__name__}")
        finally:
            for name, value in old_env.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
    return failures


def main() -> int:
    if not compileall.compile_dir(
        ROOT / "backend", quiet=1, force=True, legacy=False
    ):
        print("Python compilation failed under backend/.")
        return 1
    if not compileall.compile_file(ROOT / "main.py", quiet=1, force=True):
        print("Python compilation failed for main.py.")
        return 1

    failures = import_backend()
    if failures:
        print("Import check failed:")
        for failure in failures:
            print(f"  {failure}")
        return 1
    print("Import check passed (backend modules + runtime dependencies).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
