from __future__ import annotations

import re
import struct
import subprocess
from pathlib import Path

import pytest

from backend import deps_installer
from backend.html_assembler import render_index_html
from scripts import check_bridge_contract, repository_scan, verify_build

ROOT = Path(__file__).resolve().parents[2]


def test_html_render_is_pure_and_missing_partial_fails_closed(tmp_path: Path) -> None:
    web = tmp_path / "web"
    partials = web / "partials"
    partials.mkdir(parents=True)
    (web / "index.template.html").write_text(
        "<main>\n  <!-- @include partials/body.html -->\n</main>\n",
        encoding="utf-8",
    )
    (partials / "body.html").write_text("<p>Safe</p>\n", encoding="utf-8")
    output = web / "index.html"
    output.write_text("leave me alone", encoding="utf-8")

    assert render_index_html(web) == b"<main>\n  <p>Safe</p>\n</main>\n"
    assert output.read_text(encoding="utf-8") == "leave me alone"

    (partials / "body.html").unlink()
    with pytest.raises(FileNotFoundError, match="partial could not be read"):
        render_index_html(web)


def test_bridge_contract_reports_only_real_source_calls(tmp_path: Path) -> None:
    (tmp_path / "web").mkdir()
    mixins = tmp_path / "backend" / "api_mixins"
    mixins.mkdir(parents=True)
    (tmp_path / "main.py").write_text("class Api:\n    pass\n", encoding="utf-8")
    (mixins / "demo_mixin.py").write_text(
        "class DemoMixin:\n    def present(self):\n        pass\n",
        encoding="utf-8",
    )
    (tmp_path / "web" / "calls.js").write_text(
        "// bridgeCall('comment_only')\n"
        "bridgeCall('present');\n"
        "YT.api.missing();\n",
        encoding="utf-8",
    )

    assert check_bridge_contract.missing_bridge_methods(tmp_path) == ["missing"]


def test_repository_scan_never_retains_matching_secret(tmp_path: Path) -> None:
    secret = "ghp_" + "A" * 32
    fine_grained = "github_pat_" + "B" * 48
    path = tmp_path / "sample.txt"
    path.write_text(
        f"token={secret}\nfine_grained={fine_grained}\n", encoding="utf-8"
    )
    findings = repository_scan.scan_paths([path], root=tmp_path)

    assert findings == [
        repository_scan.Finding("SECRET_GITHUB_TOKEN", "sample.txt", 1),
        repository_scan.Finding(
            "SECRET_GITHUB_FINE_GRAINED", "sample.txt", 2
        ),
    ]
    assert secret not in repr(findings)
    assert fine_grained not in repr(findings)


def test_repository_scan_includes_untracked_but_not_ignored_files(
    tmp_path: Path,
) -> None:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / ".gitignore").write_text("ignored/\n", encoding="utf-8")
    (tmp_path / "tracked.txt").write_text("tracked\n", encoding="utf-8")
    (tmp_path / "new.txt").write_text("new\n", encoding="utf-8")
    ignored = tmp_path / "ignored"
    ignored.mkdir()
    (ignored / "artifact.txt").write_text("ignored\n", encoding="utf-8")
    subprocess.run(
        ["git", "add", ".gitignore", "tracked.txt"], cwd=tmp_path, check=True
    )

    relative = {
        path.relative_to(tmp_path).as_posix()
        for path in repository_scan.tracked_files(tmp_path)
    }

    assert relative == {".gitignore", "new.txt", "tracked.txt"}


def test_all_dependency_locks_are_exact_and_hashed() -> None:
    locks = sorted((ROOT / "requirements").glob("*.lock"))
    assert {path.name for path in locks} == {
        "build.lock",
        "dev.lock",
        "runtime.lock",
        "worker-cpu.lock",
        "worker-cuda.lock",
    }
    package_pattern = re.compile(
        r"^[A-Za-z0-9_.-]+==\S+ --hash=sha256:[0-9a-f]{64}$"
    )
    for lock in locks:
        text = lock.read_text(encoding="utf-8")
        assert "--require-hashes" in text
        packages = [line for line in text.splitlines() if "==" in line]
        assert packages
        assert all(package_pattern.fullmatch(line) for line in packages)

    cpu = (ROOT / "requirements" / "worker-cpu.lock").read_text(encoding="utf-8")
    cuda = (ROOT / "requirements" / "worker-cuda.lock").read_text(encoding="utf-8")
    assert "torch==2.5.1 --hash=sha256:" in cpu
    assert "torch==2.5.1+cu121 --hash=sha256:" in cuda
    assert "transformers==4.57.1 --hash=sha256:" in cpu
    assert "transformers==4.57.1 --hash=sha256:" in cuda
    assert "download.pytorch.org/whl/cu121" in cuda


def test_worker_installer_uses_one_hash_verified_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    monkeypatch.setattr(
        deps_installer,
        "install_python311",
        lambda _progress=None: {"ok": True, "path": "python311.exe"},
    )
    monkeypatch.setattr(
        deps_installer,
        "detect_gpu",
        lambda: {"ok": False, "name": ""},
    )
    monkeypatch.setattr(
        deps_installer,
        "_worker_lock_path",
        lambda *, cuda: Path("worker-cpu.lock"),
    )
    monkeypatch.setattr(deps_installer, "_whisper_ready", lambda _python: True)

    def fake_run(cmd, *_args, **_kwargs):
        commands.append(list(cmd))
        return 0, ""

    monkeypatch.setattr(deps_installer, "_run_streaming", fake_run)
    result = deps_installer._install_whisper_stack_unlocked()

    assert result["ok"] is True
    assert commands[0][0:4] == ["python311.exe", "-m", "pip", "install"]
    assert "--require-hashes" in commands[0]
    assert commands[0][-2:] == ["-r", "worker-cpu.lock"]
    assert commands[1] == ["python311.exe", "-m", "pip", "check"]
    assert not any(">=" in item or "<" in item for item in commands[0])


def test_worker_readiness_checks_the_punctuation_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []

    class Completed:
        returncode = 0

    def fake_run(command, **_kwargs):
        seen.extend(command)
        return Completed()

    monkeypatch.setattr(deps_installer.os.path, "isfile", lambda _path: True)
    monkeypatch.setattr(deps_installer, "_no_window", lambda: (None, 0))
    monkeypatch.setattr(deps_installer.subprocess, "run", fake_run)

    assert deps_installer._whisper_ready("python311.exe") is True
    assert "transformers" in seen[-1]
    assert "pipeline" in seen[-1]


def test_backend_version_is_the_only_python_version_assignment() -> None:
    assignments: list[Path] = []
    for path in [ROOT / "main.py", *(ROOT / "backend").rglob("*.py")]:
        source = path.read_text(encoding="utf-8")
        if re.search(r"(?m)^APP_VERSION\s*=", source):
            assignments.append(path.relative_to(ROOT))
    assert assignments == [Path("backend/version.py")]
    project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'dynamic = ["version"]' in project
    assert not re.search(r"(?m)^version\s*=\s*[\"']\d", project)


def test_windows_gate_and_ci_cover_required_stages() -> None:
    gate = (ROOT / "scripts" / "check.ps1").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "quality.yml").read_text(
        encoding="utf-8"
    )
    for required in (
        "-W', 'error'",
        "coverage",
        "--parallel-mode",
        "ruff",
        "import_check.py",
        "--check",
        "npm.cmd' @('ci'",
        "test:browser",
        "check_generated_html.py",
        "check_bridge_contract.py",
        "repository_scan.py",
        "PyInstaller",
        "verify_build.py",
        "Get-TreeFingerprint",
    ):
        assert required in gate
    assert "runs-on: windows-latest" in workflow
    assert "-Bootstrap -RequireCleanTree" in workflow


def test_pe_header_reader_accepts_only_x64_pe(tmp_path: Path) -> None:
    image = bytearray(256)
    image[0:2] = b"MZ"
    image[0x3C:0x40] = struct.pack("<I", 0x80)
    image[0x80:0x84] = b"PE\0\0"
    image[0x84:0x86] = struct.pack("<H", 0x8664)
    path = tmp_path / "sample.exe"
    path.write_bytes(image)
    assert verify_build.read_pe_machine(path) == 0x8664


def test_notices_and_packaged_resources_are_declared() -> None:
    notices = (ROOT / "THIRD_PARTY_NOTICES.md").read_text(encoding="utf-8")
    spec = (ROOT / "YTArchiver.spec").read_text(encoding="utf-8")
    assert "pywebview" in notices and "Chart.js" in notices and "FFmpeg" in notices
    assert "backend/version.py is authoritative" in spec
    assert "requirements'), 'requirements'" in spec
    assert "licenses'), 'licenses'" in spec
    assert "THIRD_PARTY_NOTICES.md" in spec
