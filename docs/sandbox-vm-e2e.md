# Sandbox VM E2E Harness

Use `scripts/run-sandbox-vm-e2e.sh` to run sandbox-focused integration tests that boot real VMs.

## What It Does

- Builds selected VM E2E test binaries.
- Ad-hoc signs host/test binaries required for Virtualization.framework.
- Runs ignored VM tests with deterministic defaults.
- Writes per-suite logs and run metadata to a reproducible artifact directory.

## Prerequisites

- macOS on Apple Silicon (`arm64`).
- Linux VM artifacts installed under `~/.vz/linux/`.
- `codesign` available.
- Network access for first-time image pulls.

## Default Command

```bash
./scripts/run-sandbox-vm-e2e.sh
```

Default suite is `sandbox`, which expands to:

- `runtime` (`vz-oci-macos/tests/runtime_e2e.rs`)
- `stack` (`vz-stack/tests/stack_e2e.rs`)

## Suite Selection

```bash
# Only runtime sandbox behavior
./scripts/run-sandbox-vm-e2e.sh --suite runtime

# Runtime + stack + buildkit
./scripts/run-sandbox-vm-e2e.sh --suite all

# Multiple flags or comma-separated tokens both work
./scripts/run-sandbox-vm-e2e.sh --suite runtime --suite buildkit
./scripts/run-sandbox-vm-e2e.sh --suite runtime,buildkit
```

Supported suite tokens:

- `runtime`
- `stack`
- `buildkit`
- `sandbox` (`runtime + stack`)
- `all` (`runtime + stack + buildkit`)

## Reproducible Debug Runs

```bash
# Keep running all suites even if one fails
./scripts/run-sandbox-vm-e2e.sh --suite all --keep-going

# Use release profile
./scripts/run-sandbox-vm-e2e.sh --profile release

# Override rust test args (replaces default args)
./scripts/run-sandbox-vm-e2e.sh --suite runtime -- --ignored --nocapture --exact smoke_pull_and_run_alpine
```

Default rust test args are:

```text
--ignored --nocapture --test-threads=1
```

## Output Artifacts

By default, artifacts are written under:

```text
.artifacts/sandbox-vm-e2e/
```

Each run creates a timestamped directory containing:

- `run-info.txt` (host/profile/suites/args)
- `<suite>.log` files
- `summary.txt`

A `latest` symlink points to the most recent run.

## Signing Behavior

The harness signs:

- `crates/target/<profile>/vz` (with virtualization entitlement)
- `crates/target/<profile>/vz-guest-agent`
- each selected VM E2E test binary (with virtualization entitlement)

## CI

Self-hosted VM E2E automation is in:

- `.github/workflows/vm-e2e.yml`

The workflow calls the same script so local and CI behavior stay aligned.
