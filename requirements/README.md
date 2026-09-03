# Reproducible dependency environments

These locks target Windows x64 and the interpreter versions in
`.python-version` / `backend/deps_installer.py`.

- `runtime.lock` is the Python 3.13 desktop runtime.
- `build.lock` contains PyInstaller and packaging tools.
- `dev.lock` contains the test, coverage, and lint tools.
- `worker-cpu.lock` and `worker-cuda.lock` are the Python 3.11 transcription
  worker alternatives. The app chooses one from detected GPU hardware.

Every artifact is exact-versioned and SHA-256 locked. Regenerate all locks on
Windows with:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/lock_dependencies.ps1
```

For a clean desktop environment, install `build.lock` first, then install
`runtime.lock` with `--no-build-isolation`, and finally `dev.lock`. The only
source distribution is `proxy-tools`; disabling build isolation makes it use
the already locked build toolchain instead of downloading an untracked one.

The CUDA lock is derived from the fully resolved CPU lock plus the SHA-256
published by PyTorch's official CUDA 12.1 simple index. This avoids downloading
the multi-gigabyte wheel merely to refresh its metadata.
