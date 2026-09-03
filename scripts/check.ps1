[CmdletBinding()]
param(
    [switch]$Bootstrap,
    [switch]$RequireCleanTree,
    [switch]$SkipBuild,
    [switch]$SkipBrowserInstall
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location -LiteralPath $Root

function Invoke-Native([string]$File, [string[]]$Arguments) {
    Write-Host ('+ ' + $File + ' ' + ($Arguments -join ' '))
    & $File @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $($LASTEXITCODE): $File"
    }
}

function Invoke-Step([string]$Name, [scriptblock]$Action) {
    Write-Host ""
    Write-Host "== $Name ==" -ForegroundColor Cyan
    & $Action
}

function Get-TreeFingerprint {
    # Use the checkout's normal Git attributes and line-ending rules. Forcing
    # core.autocrlf off here makes an unchanged Windows checkout look dirty as
    # soon as Git has to re-read a CRLF file instead of trusting its stat cache.
    $diff = (& git diff --binary --no-ext-diff -- . | Out-String)
    if ($LASTEXITCODE -ne 0) { throw 'git diff failed.' }
    $cached = (& git diff --cached --binary --no-ext-diff -- . | Out-String)
    if ($LASTEXITCODE -ne 0) { throw 'git diff --cached failed.' }
    $untracked = (& git ls-files --others --exclude-standard | Sort-Object | Out-String)
    if ($LASTEXITCODE -ne 0) { throw 'git untracked-file query failed.' }
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [Text.Encoding]::UTF8.GetBytes($diff + "`0" + $cached + "`0" + $untracked)
        return ([BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '')
    }
    finally {
        $sha.Dispose()
    }
}

$InitialFingerprint = Get-TreeFingerprint
if ($RequireCleanTree) {
    $porcelain = @(& git status --porcelain=v1 --untracked-files=all)
    if ($LASTEXITCODE -ne 0) { throw 'git status failed.' }
    if ($porcelain.Count -ne 0) {
        throw 'Quality gate requires a clean checkout; commit or stash changes first.'
    }
}

$ExpectedPython = (Get-Content -Raw -LiteralPath (Join-Path $Root '.python-version')).Trim()
$BasePython = $null
$PathPython = Get-Command 'python.exe' -ErrorAction SilentlyContinue
if ($PathPython) {
    $PathPythonVersion = (& $PathPython.Source -c 'import platform; print(platform.python_version())' | Select-Object -Last 1)
    if ($LASTEXITCODE -eq 0 -and $PathPythonVersion -eq $ExpectedPython) {
        $BasePython = $PathPython.Source
    }
}
if (-not $BasePython) {
    $LauncherPython = (& py -3.13 -c 'import sys; print(sys.executable)' | Select-Object -Last 1)
    if ($LASTEXITCODE -eq 0 -and $LauncherPython) {
        $BasePython = $LauncherPython
    }
}
if (-not $BasePython) {
    throw "The Python $ExpectedPython interpreter pinned by .python-version is unavailable."
}
$BasePython = (Resolve-Path -LiteralPath $BasePython).Path
$ActualPython = (& $BasePython -c 'import platform; print(platform.python_version())' | Select-Object -Last 1)
if ($ActualPython -ne $ExpectedPython) {
    throw "Python $ExpectedPython is required; found $ActualPython."
}

$TempBase = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$CheckRoot = Join-Path $TempBase ("ytarchiver-quality-" + [Guid]::NewGuid().ToString('N'))
$CheckRoot = [IO.Path]::GetFullPath($CheckRoot)
if (-not $CheckRoot.StartsWith($TempBase, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Refusing to create quality-gate workspace outside the system temp directory.'
}
[IO.Directory]::CreateDirectory($CheckRoot) | Out-Null

$OldAppData = $env:APPDATA
$OldLocalAppData = $env:LOCALAPPDATA
$GateAppData = Join-Path $CheckRoot 'AppData\Roaming'
$GateLocalAppData = Join-Path $CheckRoot 'AppData\Local'
$env:APPDATA = $GateAppData
$env:LOCALAPPDATA = $GateLocalAppData
[IO.Directory]::CreateDirectory($env:APPDATA) | Out-Null
[IO.Directory]::CreateDirectory($env:LOCALAPPDATA) | Out-Null
$env:PYTHONUTF8 = '1'
$env:PYTHONDONTWRITEBYTECODE = '1'

try {
    $Python = $BasePython
    if ($Bootstrap) {
        Invoke-Step 'Create locked Python environment' {
            $Venv = Join-Path $CheckRoot 'venv'
            Invoke-Native $BasePython @('-m', 'venv', $Venv)
            $script:Python = Join-Path $Venv 'Scripts\python.exe'
            Invoke-Native $script:Python @(
                '-m', 'pip', 'install', '--disable-pip-version-check',
                '--require-hashes', '-r', (Join-Path $Root 'requirements\build.lock')
            )
            Invoke-Native $script:Python @(
                '-m', 'pip', 'install', '--disable-pip-version-check',
                '--require-hashes', '--no-build-isolation',
                '-r', (Join-Path $Root 'requirements\runtime.lock')
            )
            Invoke-Native $script:Python @(
                '-m', 'pip', 'install', '--disable-pip-version-check',
                '--require-hashes', '-r', (Join-Path $Root 'requirements\dev.lock')
            )
            Invoke-Native $script:Python @('-m', 'pip', 'check')
        }
    }

    Invoke-Step 'Dependency lock validation' {
        Invoke-Native 'powershell.exe' @(
            '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File',
            (Join-Path $Root 'scripts\lock_dependencies.ps1'), '-ValidateOnly'
        )
    }
    Invoke-Step 'Python lint' {
        Invoke-Native $Python @('-m', 'ruff', 'check', '.')
    }
    Invoke-Step 'Python compile and import graph' {
        Invoke-Native $Python @((Join-Path $Root 'scripts\import_check.py'))
    }
    Invoke-Step 'Python tests, warnings, and coverage' {
        # Test modules intentionally set process-wide APPDATA and several
        # production owners have process-lifetime shutdown/write gates. Run
        # each file in a fresh interpreter and profile so collection order
        # cannot leak those globals into an unrelated test file.
        $env:COVERAGE_FILE = Join-Path $CheckRoot '.coverage'
        $PythonTests = @(
            Get-ChildItem -LiteralPath (Join-Path $Root 'tests') -Recurse -File -Filter 'test_*.py'
        ) | Sort-Object FullName
        if ($PythonTests.Count -eq 0) { throw 'No Python tests were found.' }
        foreach ($test in $PythonTests) {
            $testKey = $test.FullName.Substring($Root.Length + 1) -replace '[^A-Za-z0-9_.-]', '_'
            $env:APPDATA = Join-Path $CheckRoot ("test-state\$testKey\Roaming")
            $env:LOCALAPPDATA = Join-Path $CheckRoot ("test-state\$testKey\Local")
            [IO.Directory]::CreateDirectory($env:APPDATA) | Out-Null
            [IO.Directory]::CreateDirectory($env:LOCALAPPDATA) | Out-Null
            Invoke-Native $Python @(
                '-m', 'coverage', 'run', '--parallel-mode', '--branch', '--source=backend',
                '-m', 'pytest', '-W', 'error', '-q', $test.FullName
            )
        }
        $env:APPDATA = $GateAppData
        $env:LOCALAPPDATA = $GateLocalAppData
        Invoke-Native $Python @('-m', 'coverage', 'combine', $CheckRoot)
        Invoke-Native $Python @('-m', 'coverage', 'report', '--show-missing')
        Invoke-Native $Python @(
            '-m', 'coverage', 'xml', '-o', (Join-Path $CheckRoot 'coverage.xml')
        )
    }

    $NodeVersion = (Invoke-Native 'node.exe' @('--version') | Out-String).Trim()
    $ExpectedNode = (Get-Content -Raw -LiteralPath (Join-Path $Root '.nvmrc')).Trim()
    if ($NodeVersion.TrimStart('v') -ne $ExpectedNode) {
        throw "Node $ExpectedNode is required; found $NodeVersion."
    }
    Invoke-Step 'JavaScript syntax' {
        $JavaScript = @(
            Get-ChildItem -LiteralPath (Join-Path $Root 'web') -Recurse -File -Filter '*.js'
            Get-ChildItem -LiteralPath (Join-Path $Root 'tests') -Recurse -File -Filter '*.js'
            Get-Item -LiteralPath (Join-Path $Root 'playwright.config.js')
        ) | Sort-Object FullName -Unique
        foreach ($path in $JavaScript) {
            Invoke-Native 'node.exe' @('--check', $path.FullName)
        }
    }
    Invoke-Step 'Frontend unit regressions' {
        $UnitTests = @(Get-ChildItem -LiteralPath (Join-Path $Root 'tests') -File -Filter 'test_frontend*.js')
        if ($UnitTests.Count -eq 0) { throw 'No frontend unit tests were found.' }
        Invoke-Native 'node.exe' (@('--test') + @($UnitTests.FullName))
    }
    Invoke-Step 'Browser behavior tests' {
        Invoke-Native 'npm.cmd' @('ci', '--ignore-scripts')
        if (-not $SkipBrowserInstall) {
            Invoke-Native 'npx.cmd' @('playwright', 'install', 'chromium')
        }
        Invoke-Native 'npm.cmd' @('run', 'test:browser')
    }
    Invoke-Step 'Generated HTML' {
        Invoke-Native $Python @((Join-Path $Root 'scripts\check_generated_html.py'))
    }
    Invoke-Step 'Frontend/backend bridge contract' {
        Invoke-Native $Python @((Join-Path $Root 'scripts\check_bridge_contract.py'))
    }
    Invoke-Step 'Secret and publication privacy scan' {
        Invoke-Native $Python @((Join-Path $Root 'scripts\repository_scan.py'))
    }

    if (-not $SkipBuild) {
        Invoke-Step 'Clean PyInstaller build' {
            foreach ($name in @('build', 'dist')) {
                $target = [IO.Path]::GetFullPath((Join-Path $Root $name))
                if (-not $target.StartsWith($Root + [IO.Path]::DirectorySeparatorChar,
                        [StringComparison]::OrdinalIgnoreCase)) {
                    throw "Refusing to clean unexpected path: $target"
                }
                if (Test-Path -LiteralPath $target) {
                    Remove-Item -LiteralPath $target -Recurse -Force
                }
            }
            Invoke-Native $Python @(
                '-m', 'PyInstaller', '--clean', '--noconfirm',
                (Join-Path $Root 'YTArchiver.spec')
            )
            Invoke-Native $Python @(
                (Join-Path $Root 'scripts\verify_build.py'),
                (Join-Path $Root 'dist\YTArchiver.exe')
            )
        }
    }

    $FinalFingerprint = Get-TreeFingerprint
    if ($FinalFingerprint -ne $InitialFingerprint) {
        Write-Host ''
        Write-Host 'Source-tree changes detected:' -ForegroundColor Yellow
        $FinalStatus = @(& git status --short --untracked-files=all)
        if ($LASTEXITCODE -ne 0) { throw 'git status failed.' }
        if ($FinalStatus.Count -eq 0) {
            Write-Host '  Git reports a clean tree; the fingerprint changed unexpectedly.'
        }
        else {
            $FinalStatus | ForEach-Object { Write-Host "  $_" }
        }
        throw 'A quality check changed the source tree; review the generated changes.'
    }
    Write-Host ""
    Write-Host 'QUALITY GATE PASSED' -ForegroundColor Green
}
finally {
    $env:APPDATA = $OldAppData
    $env:LOCALAPPDATA = $OldLocalAppData
    Remove-Item Env:COVERAGE_FILE -ErrorAction SilentlyContinue
    if ($CheckRoot.StartsWith($TempBase, [StringComparison]::OrdinalIgnoreCase) -and
        (Test-Path -LiteralPath $CheckRoot)) {
        Remove-Item -LiteralPath $CheckRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
}
