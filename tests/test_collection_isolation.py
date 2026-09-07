"""The collection guard must reject unsafe selections before fixture imports."""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("collection_guard", ROOT / "conftest.py")
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


def test_selection_accepts_multiple_nodes_from_one_file():
    selected = guard.validate_test_selection(
        ROOT,
        [
            "tests/test_collection_isolation.py::test_a",
            "tests/test_collection_isolation.py::test_b",
        ],
        ROOT,
    )
    assert selected == Path(__file__).resolve()


@pytest.mark.parametrize(
    "arguments",
    [[], ["tests"], ["."], ["tests/test_collection_isolation.py", "tests/test_backend_smoke.py"]],
)
def test_selection_rejects_collection_across_files(arguments):
    with pytest.raises(pytest.UsageError):
        guard.validate_test_selection(ROOT, arguments, ROOT)


def test_focused_run_has_profile_before_test_imports():
    profile = Path(os.environ["YTARCHIVER_TEST_PROFILE"])
    assert Path(os.environ["APPDATA"]) == profile / "Roaming"
    assert Path(os.environ["LOCALAPPDATA"]) == profile / "Local"
    assert profile.name.startswith("ytarchiver-pytest-")


def test_rejected_collection_never_imports_synthetic_canary(tmp_path):
    # This is a synthetic project with one canary, never the application's
    # aggregate suite. No backend module is imported by the child process.
    project = tmp_path / "synthetic"
    tests = project / "tests"
    tests.mkdir(parents=True)
    shutil.copyfile(ROOT / "conftest.py", project / "conftest.py")
    marker = project / "collected"
    (tests / "test_canary.py").write_text(
        "from pathlib import Path\nPath('collected').write_text('unsafe collection')\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", str(tests)],
        cwd=project,
        capture_output=True,
        text=True,
        timeout=30,
        env={**os.environ, "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"},
    )
    assert result.returncode == 4, result.stdout + result.stderr
    assert "Select exactly one" in result.stderr
    assert not marker.exists()
