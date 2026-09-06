# Building YTArchiver

## Supported build path

YTArchiver is built and verified on Windows x64. The toolchain versions are
pinned in `.python-version` and `.nvmrc`; the gate checks the exact versions,
including the Python 3.13 patch version. The broader Node `engines` range in
`package.json` does not override the gate's pin. Run commands from the repository
root in PowerShell.

The authoritative local command is:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/check.ps1 -Bootstrap
```

`-Bootstrap` creates a disposable virtual environment under the system temp
directory and installs the exact, SHA-256-verified locks from `requirements/`.
It does not reuse an unverified global PyInstaller installation. The gate uses
disposable `APPDATA` and `LOCALAPPDATA` directories for tests and removes its
temporary environment when it finishes.

Output after a successful build: `dist/YTArchiver.exe`. The gate does not launch
the application, replace an installed copy, bump the version, commit, push, or
publish a release. Those are separately authorized actions.

For the full automated checks without building an executable:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/check.ps1 -Bootstrap -SkipBuild
```

`-SkipBuild` does not create or verify a new executable. An existing `dist` file
may be from an earlier build.

## What the gate verifies

`scripts/check.ps1` runs these stages before it accepts the executable:

1. validate exact/hash-locked Python dependency files;
2. run Ruff, compile `backend/` and `main.py`, and check the backend import graph
   plus desktop runtime dependencies in disposable application data;
3. run each Python test module in a fresh interpreter with
   warnings-as-errors and collect branch coverage;
4. syntax-check JavaScript in `web/` and `tests/` plus the Playwright
   configuration, and run Node regression tests;
5. install the locked npm dependencies, optionally install Playwright Chromium,
   and run headless browser behavior tests;
6. prove that `web/index.html` matches its template and partials;
7. statically check literal frontend bridge calls for matching Python handlers;
8. scan repository publication content for configured secret/privacy patterns;
9. clean `build/` and `dist/`, then build with `YTArchiver.spec`; and
10. verify the artifact is a Windows x64 PE with the expected version
    resources and required packaged files.

The import check compiles but does not import `main.py`, because importing it
acquires the application's single-instance mutex. The separate Whisper and
punctuation worker entry points are excluded from desktop import checks; this
gate does not execute transcription models or validate GPU performance.

The gate compares working-tree fingerprints at the start and end using tracked
diffs and the untracked-file list. Detected changes fail the gate. Review source
before running it, and avoid concurrent edits or regeneration during the run.

Use `-RequireCleanTree` for CI or a release checkout. Without it, reviewed local
changes are allowed, but the gate still checks for changes during its run.

### Browser selection

`playwright.config.js` uses installed **Chrome** by default. To use installed
Edge, set the channel before running the gate or browser command:

```powershell
$env:YTARCHIVER_BROWSER_CHANNEL = 'msedge'
```

The gate's `npx playwright install chromium` step downloads Playwright's Chromium;
it does not change the configured Chrome/Edge channel. Ensure the selected
browser is installed. `-SkipBrowserInstall` skips that download only: it still
runs `npm ci --ignore-scripts` and the browser tests. Neither the default browser
tests nor the gate drive an existing application window.

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

`docs/requirements.txt` is a range-based convenience list. Reproducible installs
use the lock files, including their hashes and transitive dependencies. Worker
packages stay in their separate Python 3.11 environment; do not mix them into the
Python 3.13 desktop/build environment. Dependency diagnostics and **Run setup
again** are under **Settings → About & troubleshooting**. Health → Overview
shows the download tool's status and links to its update settings.

Refreshing locks is an intentional maintenance action, not part of a normal
build:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/lock_dependencies.ps1
```

Review every version and hash change before accepting it. The normal quality
gate calls the script with `-ValidateOnly`.

## Focused development checks

The bootstrapped gate deletes its temporary environment at exit. For repeated
focused work, create a separate environment with the same lock profiles. First
verify that the Python launcher selects the version pinned by this checkout:

```powershell
$ExpectedPython = (Get-Content -Raw .python-version).Trim()
$ActualPython = (& py -3.13 -c "import platform; print(platform.python_version())").Trim()
if ($LASTEXITCODE -ne 0 -or $ActualPython -ne $ExpectedPython) {
    throw "Install and select the Python version pinned in .python-version."
}
py -3.13 -m venv .venv
.venv\Scripts\python.exe -m pip install --require-hashes -r requirements/build.lock
.venv\Scripts\python.exe -m pip install --require-hashes --no-build-isolation -r requirements/runtime.lock
.venv\Scripts\python.exe -m pip install --require-hashes -r requirements/dev.lock
.venv\Scripts\python.exe -m pip check
```

Stop if any setup command fails. Install `build.lock` before `runtime.lock`:
the runtime profile includes a hash-locked source distribution that uses the
locked build tools with `--no-build-isolation`. Release builds should still use
the disposable `-Bootstrap` gate.

### One Python test file per process

Never run aggregate `pytest`, including `pytest tests/`. Test modules can change
process-wide environment variables and application singleton state.
Every test file needs a fresh Python interpreter and new disposable `APPDATA`
and `LOCALAPPDATA` directories, set **before any backend import**.

Use `scripts/check.ps1` directly for the complete gate. `scripts/check.sh` is a
Git Bash compatibility wrapper that forwards its arguments to that PowerShell
script; it does not implement a separate aggregate test runner and still requires
Windows PowerShell.

For example, this runs only the backend smoke test file:

```powershell
$TestRoot = Join-Path ([IO.Path]::GetTempPath()) ('ytarchiver-test-' + [Guid]::NewGuid().ToString('N'))
$PreviousAppData = $env:APPDATA
$PreviousLocalAppData = $env:LOCALAPPDATA
try {
    $env:APPDATA = Join-Path $TestRoot 'Roaming'
    $env:LOCALAPPDATA = Join-Path $TestRoot 'Local'
    New-Item -ItemType Directory -Force -Path $env:APPDATA,$env:LOCALAPPDATA | Out-Null
    .venv\Scripts\python.exe -m pytest -W error -q tests/test_backend_smoke.py
    if ($LASTEXITCODE -ne 0) { throw 'The focused test failed.' }
}
finally {
    $env:APPDATA = $PreviousAppData
    $env:LOCALAPPDATA = $PreviousLocalAppData
}
```

Replace the test path with one other file to check it separately; do not append
multiple files to that Python invocation. The example leaves its disposable
folder in the system temporary directory for inspection. It does not touch the
normal application profile. A manual source or executable check likewise needs
its profile selected before launch and disposable or explicitly approved archive
folders.

`scripts/smoke.py` is an older subset rather than the full gate. It does not
provide its own application-data isolation, can rewrite stale generated HTML,
and skips JavaScript syntax checks when Node is absent.

### Node and browser regressions

With the Node version from `.nvmrc`, run the Node-only regressions using the same
file selection as the Windows gate:

```powershell
$FrontendTests = @(Get-ChildItem -LiteralPath tests -File -Filter 'test_frontend*.js' | Select-Object -ExpandProperty FullName)
node --test @FrontendTests
```

Browser tests load the real frontend and a fixture bridge from
`tests/frontend/browser`; they do not import the live Python application:

```powershell
npm ci --ignore-scripts
npm run test:browser
```

Install the selected Chrome/Edge channel as described above. The Playwright
configuration runs headlessly, with one worker and failure traces/screenshots
under the system temporary directory. `npm run test:browser:headed` deliberately
opens visible browser windows and is only for an intended interactive check.

## PyInstaller specification and notices

Always build through `YTArchiver.spec`. It declares the pywebview hidden
imports and packages the frontend, icon, worker scripts, third-party notices,
and license texts. A bare `pyinstaller --onefile main.py` command does not
represent the supported artifact.

The spec also includes the worker dependency locks and punctuation alignment
helper. yt-dlp, ffmpeg, and ffprobe are bundled only when their executables are
present at the repository root; otherwise runtime tool discovery and setup are
separate from the Python packaging step.

The build itself is non-interactive and does not launch YTArchiver. After the
automated gate passes, a maintainer may perform a separate manual acceptance
run of `dist/YTArchiver.exe` against disposable or explicitly approved state.
Replacing an installed executable requires a separate local deployment step;
do not stop a running instance or copy over it without approval.

For a maintainer push, coordinate the `backend/version.py` version/date update
and `docs/CHANGELOG.md` entry first. A successful local build authorizes neither
a git push nor tags or release publication.

## CI

`.github/workflows/quality.yml` runs the same gate on `windows-latest` for
pushes to `main`, pull requests, and manual dispatches:

```powershell
scripts/check.ps1 -Bootstrap -RequireCleanTree
```

CI uploads `dist/YTArchiver.exe` only after the complete gate and artifact
verification pass. CI does not publish a GitHub release; release publication
remains a separate, explicitly authorized action.
