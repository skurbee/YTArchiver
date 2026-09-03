[CmdletBinding()]
param(
    [switch]$ValidateOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$Requirements = Join-Path $Root 'requirements'

function Resolve-Python([string]$Version) {
    $result = & py "-$Version" -c 'import sys; print(sys.executable)'
    if ($LASTEXITCODE -ne 0 -or -not $result) {
        throw "Python $Version is required to lock this profile."
    }
    $python = (Resolve-Path ($result | Select-Object -Last 1)).Path
    $actual = (& $python -c 'import platform; print(platform.python_version())' |
        Select-Object -Last 1)
    $expected = if ($Version -eq '3.13') {
        (Get-Content -Raw -LiteralPath (Join-Path $Root '.python-version')).Trim()
    }
    elseif ($Version -eq '3.11') {
        # Must match the verified CPython installer selected by deps_installer.
        $installerSource = [IO.File]::ReadAllText(
            (Join-Path $Root 'backend\deps_installer.py'))
        $versionMatch = [regex]::Match(
            $installerSource,
            '(?m)^_PY311_VERSION\s*=\s*["''](?<version>\d+\.\d+\.\d+)["'']\s*$'
        )
        if (-not $versionMatch.Success) {
            throw 'Could not read the worker Python version from deps_installer.py.'
        }
        $versionMatch.Groups['version'].Value
    }
    else {
        throw "No exact interpreter policy exists for Python $Version."
    }
    if ($actual -ne $expected) {
        throw "Python $expected is required to lock this profile; found $actual."
    }
    return $python
}

function New-LockProfile(
    [string]$Name,
    [string]$PythonVersion,
    [string[]]$Packages,
    [string[]]$PipOptions = @(),
    [bool]$OnlyBinary = $true
) {
    return [PSCustomObject]@{
        Name = $Name
        PythonVersion = $PythonVersion
        Packages = $Packages
        PipOptions = $PipOptions
        OnlyBinary = $OnlyBinary
    }
}

$Runtime = @(
    'bottle==0.13.4', 'cffi==2.0.0', 'clr_loader==0.2.10',
    'Pillow==12.2.0', 'proxy_tools==0.1.0', 'psutil==7.0.0',
    'pycparser==3.0', 'pystray==0.19.5', 'pythonnet==3.0.5',
    'pywebview==6.2.1', 'six==1.17.0', 'typing_extensions==4.13.0'
)
$Build = @(
    'altgraph==0.17.5', 'packaging==25.0', 'pefile==2024.8.26',
    'pip==25.2', 'pyinstaller==6.19.0',
    'pyinstaller-hooks-contrib==2026.4', 'pywin32-ctypes==0.2.3',
    'setuptools==80.9.0'
)
$Dev = @(
    'colorama==0.4.6', 'coverage==7.16.0', 'iniconfig==2.3.0',
    'packaging==25.0', 'pluggy==1.6.0', 'Pygments==2.19.1',
    'pytest==9.0.3', 'ruff==0.15.13'
)
# Keep Transformers on the application's previously supported 4.x contract;
# 5.x changed the worker-facing pipeline stack even though a bare import can
# still succeed. This exact set is smoke-installed by the release procedure.
$WorkerCommon = @(
    'av==16.1.0', 'certifi==2026.1.4',
    'charset-normalizer==3.4.4', 'colorama==0.4.6',
    'ctranslate2==4.7.1', 'faster-whisper==1.2.1', 'filelock==3.20.3',
    'flatbuffers==25.12.19', 'fsspec==2026.2.0',
    'hf-xet==1.3.2', 'huggingface_hub==0.36.0', 'idna==3.11',
    'Jinja2==3.1.6', 'MarkupSafe==3.0.3',
    'mpmath==1.3.0', 'networkx==3.6.1', 'numpy==2.3.5',
    'onnxruntime==1.24.3', 'packaging==26.0', 'protobuf==7.34.0',
    'PyYAML==6.0.3', 'regex==2026.1.15', 'requests==2.32.5',
    'safetensors==0.7.0', 'setuptools==65.5.0', 'sympy==1.13.1',
    'tokenizers==0.22.2', 'tqdm==4.67.3', 'transformers==4.57.1',
    'typing_extensions==4.15.0', 'urllib3==2.6.3'
)

$Profiles = @(
    # proxy_tools 0.1.0 is published only as an sdist. Its exact artifact is
    # still hash-locked; callers install build.lock first and use
    # --no-build-isolation for this one profile.
    (New-LockProfile 'runtime' '3.13' $Runtime @() $false),
    (New-LockProfile 'build' '3.13' $Build),
    (New-LockProfile 'dev' '3.13' $Dev),
    (New-LockProfile 'worker-cpu' '3.11' ($WorkerCommon + @('torch==2.5.1'))),
    (New-LockProfile 'worker-cuda' '3.11' ($WorkerCommon + @('torch==2.5.1+cu121')) @(
        '--extra-index-url', 'https://download.pytorch.org/whl/cu121'
    ))
)

function Assert-LockShape([string]$Path) {
    $text = [IO.File]::ReadAllText($Path)
    if ($text -notmatch '(?m)^--require-hashes\s*$') {
        throw "$Path is missing mandatory hash enforcement."
    }
    $packageLines = @($text -split "`r?`n" | Where-Object {
        $_ -match '^[A-Za-z0-9_.-]+=='
    })
    if (-not $packageLines) {
        throw "$Path contains no locked packages."
    }
    foreach ($line in $packageLines) {
        if ($line -notmatch '^[A-Za-z0-9_.-]+==[^ ]+ --hash=sha256:[0-9a-f]{64}$') {
            throw "$Path has a package without one exact SHA-256 artifact hash."
        }
    }
}

function Assert-LockProfile([string]$Path, [object]$Profile) {
    Assert-LockShape $Path
    $text = [IO.File]::ReadAllText($Path)
    $expected = @($Profile.Packages | ForEach-Object {
        ([string]$_).ToLowerInvariant().Replace('_', '-')
    } | Sort-Object)
    $actual = @($text -split "`r?`n" | ForEach-Object {
        if ($_ -match '^(?<requirement>[A-Za-z0-9_.-]+==[^ ]+) --hash=sha256:') {
            $Matches['requirement'].ToLowerInvariant().Replace('_', '-')
        }
    } | Sort-Object)
    $difference = @(Compare-Object -ReferenceObject $expected -DifferenceObject $actual)
    if ($difference.Count -ne 0) {
        throw "$Path does not match the exact package profile declared by this script."
    }
    $hasBinaryOnly = $text -match '(?m)^--only-binary=:all:\s*$'
    if ($hasBinaryOnly -ne [bool]$Profile.OnlyBinary) {
        throw "$Path has the wrong binary-artifact policy."
    }
    $hasCudaIndex = $text -match '(?m)^--extra-index-url https://download\.pytorch\.org/whl/cu121\s*$'
    if ($hasCudaIndex -ne ($Profile.Name -eq 'worker-cuda')) {
        throw "$Path has the wrong package-index policy."
    }
}

function Write-Lock([object]$Profile) {
    $python = Resolve-Python $Profile.PythonVersion
    $report = Join-Path ([IO.Path]::GetTempPath()) (
        "ytarchiver-$($Profile.Name)-$([Guid]::NewGuid().ToString('N')).json"
    )
    try {
        $arguments = @(
            '-m', 'pip', 'install', '--disable-pip-version-check',
            '--dry-run', '--ignore-installed', '--no-cache-dir',
            '--report', $report
        )
        if ($Profile.OnlyBinary) {
            $arguments += '--only-binary=:all:'
        }
        else {
            $arguments += '--no-build-isolation'
        }
        $arguments += $Profile.PipOptions + $Profile.Packages
        & $python @arguments
        if ($LASTEXITCODE -ne 0) {
            throw "pip could not resolve the $($Profile.Name) lock."
        }
        $resolved = (Get-Content -Raw -LiteralPath $report | ConvertFrom-Json).install
        $lines = [Collections.Generic.List[string]]::new()
        $lines.Add('# Generated by scripts/lock_dependencies.ps1; do not hand edit.')
        $lines.Add("# Windows x64 / CPython $($Profile.PythonVersion) artifact lock.")
        $lines.Add('--require-hashes')
        if ($Profile.OnlyBinary) {
            $lines.Add('--only-binary=:all:')
        }
        $lines.Add('--index-url https://pypi.org/simple')
        if ($Profile.Name -eq 'worker-cuda') {
            $lines.Add('--extra-index-url https://download.pytorch.org/whl/cu121')
        }
        $lines.Add('')
        foreach ($item in ($resolved | Sort-Object { $_.metadata.name.ToLowerInvariant() })) {
            $hash = [string]$item.download_info.archive_info.hash
            if ($hash -notmatch '^sha256=([0-9a-f]{64})$') {
                throw "Resolver returned an unverifiable artifact for $($item.metadata.name)."
            }
            $name = ([string]$item.metadata.name).ToLowerInvariant().Replace('_', '-')
            $lines.Add("$name==$($item.metadata.version) --hash=sha256:$($Matches[1])")
        }
        $target = Join-Path $Requirements "$($Profile.Name).lock"
        [IO.Directory]::CreateDirectory($Requirements) | Out-Null
        [IO.File]::WriteAllText($target, ($lines -join "`n") + "`n", [Text.UTF8Encoding]::new($false))
        Assert-LockShape $target
        Write-Host "Locked $($Profile.Name): $target"
    }
    finally {
        Remove-Item -LiteralPath $report -Force -ErrorAction SilentlyContinue
    }
}

function Write-CudaLock {
    # PyTorch's CUDA wheel does not publish PEP 658 sidecar metadata, so pip's
    # dry-run downloads the entire multi-gigabyte wheel merely to write a
    # resolver report. The official simple index already publishes the exact
    # artifact SHA-256 in the link fragment; derive the CUDA lock from the
    # otherwise-identical, fully-resolved CPU lock and that signed HTTPS index.
    $cpuPath = Join-Path $Requirements 'worker-cpu.lock'
    if (-not (Test-Path -LiteralPath $cpuPath -PathType Leaf)) {
        throw 'worker-cpu.lock must be generated before worker-cuda.lock.'
    }
    $indexUrl = 'https://download.pytorch.org/whl/cu121/torch/'
    $html = (Invoke-WebRequest -UseBasicParsing -Uri $indexUrl).Content
    $pattern = 'href="[^\"]*torch-2\.5\.1%2Bcu121-cp311-cp311-win_amd64\.whl#sha256=(?<hash>[0-9a-f]{64})"'
    $matches = [regex]::Matches($html, $pattern)
    if ($matches.Count -ne 1) {
        throw 'Official PyTorch index did not return exactly one pinned CUDA artifact.'
    }
    $hash = $matches[0].Groups['hash'].Value
    $lines = [Collections.Generic.List[string]]::new()
    foreach ($line in ([IO.File]::ReadAllLines($cpuPath))) {
        if ($line -eq '# Windows x64 / CPython 3.11 artifact lock.') {
            $lines.Add('# Windows x64 / CPython 3.11 CUDA 12.1 artifact lock.')
        }
        elseif ($line -eq '--index-url https://pypi.org/simple') {
            $lines.Add($line)
            $lines.Add('--extra-index-url https://download.pytorch.org/whl/cu121')
        }
        elseif ($line -match '^torch==2\.5\.1 --hash=sha256:') {
            $lines.Add("torch==2.5.1+cu121 --hash=sha256:$hash")
        }
        else {
            $lines.Add($line)
        }
    }
    $target = Join-Path $Requirements 'worker-cuda.lock'
    [IO.File]::WriteAllText($target, ($lines -join "`n") + "`n", [Text.UTF8Encoding]::new($false))
    Assert-LockShape $target
    Write-Host "Locked worker-cuda: $target"
}

if ($ValidateOnly) {
    foreach ($profile in $Profiles) {
        $target = Join-Path $Requirements "$($profile.Name).lock"
        if (-not (Test-Path -LiteralPath $target -PathType Leaf)) {
            throw "Missing lock: $target"
        }
        Assert-LockProfile $target $profile
    }
    Write-Host 'Dependency lock structure and exact profiles passed.'
    exit 0
}

foreach ($profile in $Profiles) {
    if ($profile.Name -eq 'worker-cuda') {
        Write-CudaLock
    }
    else {
        Write-Lock $profile
    }
}
