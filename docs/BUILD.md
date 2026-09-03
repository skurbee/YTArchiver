# Building YTArchiver

## Supported build path

YTArchiver is built and verified on Windows x64. The toolchain versions are
pinned in `.python-version` and `.nvmrc`; do not substitute a different Python
or Node version for a release build.

The authoritative local command is:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/check.ps1 -Bootstrap
```

`-Bootstrap` creates a disposable virtual environment under the system temp
directory and installs the exact, SHA-256-verified locks from `requirements/`.
It does not reuse an unverified global PyInstaller installation. The gate uses
disposable `APPDATA` and `LOCALAPPDATA` directories for tests and removes its
temporary environment when it finishes.

Output: `dist/YTArchiver.exe`.

## What the gate verifies

`scripts/check.ps1` runs these stages before it accepts the executable:

1. validate exact/hash-locked Python dependency files;
2. run Ruff, compile every Python file, and import every backend module;
3. run each Python test module in a fresh interpreter with
   warnings-as-errors and collect branch coverage;
4. check every JavaScript file's syntax and run Node regression tests;
5. install pinned Playwright dependencies and run browser behavior tests;
6. prove that `web/index.html` matches its template and partials;
7. verify every frontend `pywebview.api` call has a Python API method;
8. scan tracked publication content for configured secret/privacy patterns;
9. clean `build/` and `dist/`, then build with `YTArchiver.spec`; and
10. verify the artifact is a Windows x64 PE with the expected version
    resources and required packaged files.

The gate fingerprints the working tree at the start and end. If a formatter,
generator, test, or build step changes source, the gate fails instead of
quietly shipping unreviewed output.

Use `-RequireCleanTree` for CI or a release checkout. `-SkipBuild` is useful
for a fast local code-only pass. `-SkipBrowserInstall` may be used only when
the required browser is already installed.

## Generated frontend rule

`web/index.html` is a committed build artifact assembled from
`web/index.template.html` and `web/partials/*.html`. Edit the source template
or partial, regenerate, and verify it before running the full gate:

```powershell
py -3.13 -c "from backend.html_assembler import assemble_index_html; assemble_index_html('web')"
py -3.13 scripts/check_generated_html.py
```

## Dependency locks

The lock files have separate responsibilities:

- `requirements/runtime.lock` — Python 3.13 desktop runtime
- `requirements/build.lock` — PyInstaller and packaging tools
- `requirements/dev.lock` — Ruff, pytest, coverage, and test helpers
- `requirements/worker-cpu.lock` — Python 3.11 CPU transcription worker
- `requirements/worker-cuda.lock` — Python 3.11 CUDA transcription worker

Refreshing locks is an intentional maintenance action, not part of a normal
build:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/lock_dependencies.ps1
```

Review every version and hash change before accepting it. The normal quality
gate calls the script with `-ValidateOnly`.

## PyInstaller specification and notices

Always build through `YTArchiver.spec`. It declares the pywebview hidden
imports and packages the frontend, icon, worker scripts, third-party notices,
and license texts. A bare `pyinstaller --onefile main.py` command does not
represent the supported artifact.

The build itself is non-interactive and does not launch YTArchiver. After the
automated gate passes, a maintainer may perform a separate manual acceptance
run of `dist/YTArchiver.exe` against disposable or explicitly approved state.

## CI

`.github/workflows/quality.yml` runs the same gate on `windows-latest` for
pushes, pull requests, and manual dispatches:

```powershell
scripts/check.ps1 -Bootstrap -RequireCleanTree
```

CI uploads `dist/YTArchiver.exe` only after the complete gate and artifact
verification pass. CI does not publish a GitHub release; release publication
remains a separate, explicitly authorized action.
