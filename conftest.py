"""Reject shared-process collection and isolate paths before application imports.

The supported suite runner is scripts/check.ps1. A focused pytest invocation
may select tests from exactly one file; it receives a fresh profile even when
started by an IDE without the runner's environment setup.
"""

from __future__ import annotations

import atexit
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest


def validate_test_selection(root: Path, arguments: list[str], cwd: Path) -> Path:
    """Validate selection without importing or collecting any test module."""
    tests = (root / "tests").resolve()
    selected: set[Path] = set()
    for argument in arguments:
        path = Path(argument.split("::", 1)[0])
        path = (path if path.is_absolute() else cwd / path).resolve()
        if not path.is_file() or path.suffix != ".py" or not path.is_relative_to(tests):
            raise pytest.UsageError(
                "Select exactly one Python test file under tests/. "
                "Use scripts/check.ps1 for the full isolated suite."
            )
        selected.add(path)
    if len(selected) != 1:
        raise pytest.UsageError(
            "Test files require separate Python processes. "
            "Use scripts/check.ps1 for the full isolated suite."
        )
    return selected.pop()


def _remove_profile(profile: Path, temporary_root: Path) -> None:
    # Registered before application atexit writers, so they finish first.
    resolved = profile.resolve()
    if resolved.parent == temporary_root and resolved.name.startswith("ytarchiver-pytest-"):
        shutil.rmtree(resolved, ignore_errors=True)


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    """Run before collection can import modules that capture profile paths."""
    root = Path(__file__).resolve().parent
    validate_test_selection(root, config.args, Path(config.invocation_params.dir))
    if config.option.pyargs:
        raise pytest.UsageError("Package collection is unsupported; select one test file.")
    if any(name == "backend" or name.startswith("backend.") for name in sys.modules):
        raise pytest.UsageError(
            "Application modules were imported before test isolation. "
            "Start a fresh Python process without application-importing plugins."
        )
    temporary_root = Path(tempfile.gettempdir()).resolve()
    profile = Path(tempfile.mkdtemp(prefix="ytarchiver-pytest-", dir=temporary_root))
    atexit.register(_remove_profile, profile, temporary_root)
    for variable, folder in (("APPDATA", "Roaming"), ("LOCALAPPDATA", "Local")):
        target = profile / folder
        target.mkdir()
        os.environ[variable] = str(target)
    os.environ["YTARCHIVER_TEST_PROFILE"] = str(profile)
