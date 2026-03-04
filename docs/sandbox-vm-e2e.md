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
- `stack-user-journey-checkpoint` → `complex_stack_snapshot_restore_rewinds_shared_vm_state`
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

## Spaces Release Gate

For Spaces R1 btrfs checkpoint release-gating, run the signed VM snapshot scenario in
`release` profile and require a green summary with non-zero executed tests.

Command:

```bash
./scripts/run-sandbox-vm-e2e.sh --profile release --scenario stack-snapshot-restore
```

Mandatory pass criteria:

- `summary.txt` contains `passed=stack-snapshot-restore`
- `summary.txt` contains `failed=none`
- `stack-snapshot-restore.log` contains `running 1 test`
- artifacts are archived from `.artifacts/sandbox-vm-e2e/<timestamp>/`

Recommended pre-release evidence bundle:

- `<timestamp>/run-info.txt`
- `<timestamp>/summary.txt`
- `<timestamp>/stack-snapshot-restore.log`

## Linux btrfs Portability Gate

For Linux-native btrfs portability (checkpoint + shared-cache send/receive), run the dedicated
harness on a Linux host/VM with a real btrfs workspace path:

```bash
VZ_TEST_BTRFS_WORKSPACE=/mnt/vz-btrfs ./scripts/run-linux-btrfs-e2e.sh
```

If your Linux VM does not already have a btrfs workspace, provision one once:

```bash
sudo ./scripts/provision-linux-btrfs-workspace.sh --workspace /mnt/vz-btrfs
```

For dedicated remote `vz` Linux environments (SSH-accessible), use the remote wrapper:

```bash
# one-time setup
cp config/vz-linux-btrfs-e2e.env.example .config/vz-linux-btrfs-e2e.env
$EDITOR .config/vz-linux-btrfs-e2e.env

# then run (no flags)
./scripts/run-linux-btrfs-e2e-remote.sh

# or explicit flags
./scripts/run-linux-btrfs-e2e-remote.sh \
  --host user@vz-linux-host \
  --workspace /mnt/vz-btrfs \
  --remote-repo ~/workspace/vz \
  --profile release
```

Policy:

- Use real `vz` Linux VM hosts only for this portability gate.
- Localhost/Docker-backed targets are intentionally rejected by the remote wrapper.

The remote wrapper runs the same gate script on the remote host and copies the resulting artifact
directory back under:

```text
.artifacts/linux-btrfs-e2e-remote/<timestamp>/
```

Mandatory pass criteria:

- `summary.txt` contains:
  - `passed=spaces_btrfs_checkpoint_restore_and_fork_use_real_subvolumes`
  - `checkpoint_export_import_round_trip_preserves_workspace_snapshot`
  - `space_cache_export_import_round_trip_preserves_payload`
- `summary.txt` contains `failed=none`
- Each corresponding log file contains `running 1 test`

Artifacts are written under:

```text
.artifacts/linux-btrfs-e2e/<timestamp>/
```

## High-Level `vz` on Linux VM Gate (No SSH)

Use this to validate high-level `vz` CLI/API behavior against real daemon-owned
Linux runtime orchestration inside the local `vz` Linux VM environment.

Run from inside the Linux VM:

```bash
./scripts/run-vz-linux-vm-e2e.sh --workspace /mnt/vz-btrfs --profile release
```

Or run from macOS host into a local `vz` VM (no SSH) using VM control socket:

```bash
./scripts/run-vz-linux-vm-e2e-local.sh \
  --vm-name vz-linux-test \
  --guest-repo /workspace/vz \
  --auto-start \
  --vm-image ~/.vz/images/<mac-vm-image>.img \
  --mount repo:/Users/$USER/workspace/jl/vz \
  --workspace /mnt/vz-btrfs \
  --profile release
```

Notes:

- `--mount` is forwarded to `vz vm mac run` during auto-start.
- ensure `--guest-repo` matches the in-guest mount path for your VM image.
- wrapper can provision btrfs workspace in-guest automatically before running harness.

What this flow validates:

- `vz-runtimed` starts and owns runtime state.
- `vz-api` routes to daemon over UDS.
- high-level `vz` CLI (`create`, `ls`, `inspect`) works via `api-http` transport.
- `vz vm linux` daemon lifecycle flows (`list`, `inspect`, streamed `exec`, `stop`, `rm`) work via daemon gRPC transport.
- streamed exec output and non-zero exit code propagation are validated (`exit 7` test case).
- final sandbox state is `terminated`.

Artifacts are written under:

```text
.artifacts/vz-linux-vm-e2e/<timestamp>/
```

## Release-Gate One-Liner

Run from repo root:

```bash
./scripts/run-linux-daemon-release-gate.sh \
  --workspace /mnt/vz-btrfs \
  --profile release
```

On macOS this delegates to the local VM wrapper (`run-vz-linux-vm-e2e-local.sh`).
On Linux it runs the harness directly (`run-vz-linux-vm-e2e.sh`).

Deterministic artifact root:

```text
.artifacts/release-gates/linux-daemon/
```

Gate checklist:

- Latest run summary exists at:
  `.artifacts/release-gates/linux-daemon/latest/summary.txt`
- `summary.txt` contains:
  - `passed=vz_cli_api_daemon_linux_happy_path,vz_vm_linux_daemon_lifecycle`
  - `failed=none`
- Artifacts include:
  - `vm-linux-list.json`
  - `vm-linux-inspect.json`
  - `vm-linux-exec-success.log`
  - `vm-linux-exec-fail.log`

## Signing Behavior

The harness signs:

- `crates/target/<profile>/vz` (with virtualization entitlement)
- `crates/target/<profile>/vz-guest-agent`
- each selected VM E2E test binary (with virtualization entitlement)

For BuildKit suites/scenarios, the harness also sets `VZ_BUILDKIT_DIR` to a
per-run artifact directory so stale host cache state does not bleed across runs.

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
