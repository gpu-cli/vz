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

## Use-Case Scenarios

Use `--scenario` to run deterministic sandbox workflows by name.

Capability matrix:

- `runtime-smoke` → `smoke_pull_and_run_alpine`
- `runtime-lifecycle` → `lifecycle_create_exec_stop_remove`
- `runtime-port-forwarding` → `port_forwarding_tcp`
- `runtime-shared-vm-net` → `shared_vm_inter_service_connectivity`
- `stack-real-services` → `real_services_postgres_and_redis`
- `stack-control-socket` → `exec_via_control_socket`
- `stack-port-forwarding` → `stack_port_forwarding`
- `stack-snapshot-restore` → `complex_stack_snapshot_restore_rewinds_shared_vm_state`
- `stack-user-journey-checkpoint` → `complex_stack_user_journey_with_named_volume_checkpoint`
- `buildkit-roundtrip` → `buildkit_builds_dockerfile_and_run_uses_built_image`

Scenario groups:

- `sandbox-usecases` → runtime + stack use-cases (no buildkit)
- `all-usecases` → runtime + stack + buildkit use-cases

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

# Run sandbox use-case matrix (runtime + stack)
./scripts/run-sandbox-vm-e2e.sh --scenario sandbox-usecases

# Run only snapshot/restore use-case scenario
./scripts/run-sandbox-vm-e2e.sh --scenario stack-snapshot-restore
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
- `<suite>.log` files or `<scenario>.log` files (scenario mode)
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

The scheduled workflow now runs:

- `vm-e2e-smoke` (`sandbox` suite)
- `vm-e2e-nightly-full` (`all` suite, depends on smoke)

Artifacts are published as:

- `sandbox-vm-e2e-smoke-artifacts`
- `sandbox-vm-e2e-nightly-full-artifacts`
