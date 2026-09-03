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
- `jq`, `shasum`, and the tools required by
  `scripts/build-runtime-free-buildkit.sh` when a BuildKit lane is selected.
- Network access for first-time image pulls.

## Default Command

```bash
./scripts/run-sandbox-vm-e2e.sh --profile release
```

Default suite is `sandbox`, which expands to:

- `runtime` (`vz-oci-macos/tests/runtime_e2e.rs`)
- `stack` (`vz-stack/tests/stack_e2e.rs`)

The script's profile option defaults to `debug` for focused development
scenarios. The complete `runtime`, `sandbox`, and `all` lanes include strict
exec-supervision release evidence and therefore fail preflight unless
`--profile release` is explicit.

## Use-Case Scenarios

Use `--scenario` to run deterministic sandbox workflows by name.

Capability matrix:

- `runtime-smoke` → `smoke_pull_and_run_alpine`
- `runtime-lifecycle` → `lifecycle_create_exec_stop_remove`
- `runtime-container-id-ownership` → `container_id_lifecycle_serialization_and_generation_ownership`
- `runtime-exec-supervision` → `runtime_exec_supervision`
- `runtime-port-forwarding` → `port_forwarding_tcp`
- `runtime-shared-vm-net` → `shared_vm_inter_service_connectivity`
- `stack-real-services` → `real_services_postgres_and_redis`
- `stack-control-socket` → `exec_via_control_socket`
- `stack-port-forwarding` → `stack_port_forwarding`
- `stack-container-ownership` → `stack_container_generation_ownership`
- `stack-snapshot-restore` → `complex_stack_snapshot_restore_rewinds_shared_vm_state`
- `stack-user-journey-checkpoint` → `complex_stack_snapshot_restore_rewinds_shared_vm_state`
- `buildkit-roundtrip` → `buildkit_builds_dockerfile_and_run_uses_built_image`

Before any VM starts, the harness cross-builds the current Linux guest agent
with `cargo zigbuild`, rebuilds both Developer and Hardened/container
initramfs bundles, and records their SHA-256 digests in `run-info.txt`. Each
test lane is pinned to those workspace bundles. This prevents a freshly built
macOS host binary from being tested against a stale guest agent embedded in an
older initramfs. Set `VZ_E2E_GUEST_AGENT_BUILD_TOOL` only when deliberately
using another compatible cross-build tool.

The complete `runtime` suite also includes
`undeclared_host_import_does_not_inject_host_vz_internal`. This real-VM
regression creates a stack-network-namespace container with no declared import
and requires `host.vz.internal` to be absent from `/etc/hosts`. It is not a
positive host-import test: the authenticated relay is tracked separately and
must not be replaced by arbitrary gateway reachability.

The complete `runtime` suite also runs the strict exec-supervision matrix. It
exercises the synchronous/unary-shaped host adapter, streaming, and PTY
container exec against `SIGTERM`, `SIGINT`, `SIGKILL`, and deadline
cancellation (12 exact cells). The synchronous adapter uses the same supervised
stream internally; the raw legacy `OciService.Exec` path is retired and is not
release evidence. Every cell
retains the target's raw container/guest PID, `/proc` start time, process-group
ID, exact cgroup path and membership, then requires synchronous process/session
reaping, exact baseline cgroup restoration, marker cleanup, and a post-case
container-health probe. Signal exits must be exactly 143, 130, and 137. The
timeout is fixed at two seconds and must complete within the schema's bounded
window. PTY cells also prove resize control. There are no optional cells,
required skips, or scenario retries. Additional retained-child cases prove the
explicit cancellation receipt and the normal-leader-exit path both kill and
reap the remaining process group before returning. Further required cases
cancel at the deterministic registered/pre-guest-RPC boundary, drop a live host
execution future after normal readiness, and kill the exact outer trampoline by
PID/start-time identity. Two additional caller-abort cases pause after the guest
has returned its post-execve readiness proof but before the outer task can take
ownership: one uses a named streaming execution and the other the anonymous
unary adapter. While that boundary is held, both must expose a live leader and
retained child in the exact container cgroup. Aborting the caller must then
remove both exact PID/start-time identities, restore baseline cgroup membership,
remove any named host session and marker, and leave the container healthy. Each
case must retain cleanup authority and publish no invalid readiness.

Schema v4 adds three exact-once regressions. An authenticated, container-targeted
request with an invalid environment key must return a definite pre-spawn error,
leave both the host session count and exact cgroup membership unchanged, release
the lifecycle writer, and pass a post-case exec. A slow but live streaming
consumer pauses for six seconds while the guest emits 5 MiB (more than the
64 × 65,536-byte bounded channel capacity); the gate requires the exact byte
count and SHA-256, one readiness event, one exit event, and `Exit(0)` last. The
third case injects a single authenticated response loss after dispatch but
before the host observes readiness. Its prepared request ID must take the
reconciliation path and return the stable caller-visible
`reconciliation=TERMINAL_REAPED` outcome, with exact process/cgroup/session
cleanup, released lifecycle admission, and a healthy post-case exec. The fault
selector is supplied only by the strict harness and targets that unique command,
so a parallel full runtime suite cannot consume it in another exec.

Scenario groups:

- `sandbox-usecases` → runtime + stack use-cases (no buildkit and no
  release-only exec-supervision evidence)
- `all-usecases` → runtime + stack + buildkit use-cases, including
  exec-supervision; select `--profile release`

## Suite Selection

```bash
# Only runtime sandbox behavior
./scripts/run-sandbox-vm-e2e.sh --profile release --suite runtime

# Runtime + stack + buildkit
./scripts/run-sandbox-vm-e2e.sh --profile release --suite all

# Multiple flags or comma-separated tokens both work
./scripts/run-sandbox-vm-e2e.sh --profile release --suite runtime --suite buildkit
./scripts/run-sandbox-vm-e2e.sh --profile release --suite runtime,buildkit
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
./scripts/run-sandbox-vm-e2e.sh --profile release --suite all --keep-going

# Use release profile
./scripts/run-sandbox-vm-e2e.sh --profile release

# Override rust test args (replaces default args)
./scripts/run-sandbox-vm-e2e.sh --suite runtime -- --ignored --nocapture --exact smoke_pull_and_run_alpine

# Run sandbox use-case matrix (runtime + stack)
./scripts/run-sandbox-vm-e2e.sh --scenario sandbox-usecases

# Run only snapshot/restore use-case scenario
./scripts/run-sandbox-vm-e2e.sh --scenario stack-snapshot-restore

# Run the release-built Mac→vz Linux exec-supervision gate
./scripts/run-sandbox-vm-e2e.sh --profile release --scenario runtime-exec-supervision
```

The focused `runtime-exec-supervision` scenario is release evidence, as is any
complete suite lane containing `runtime`. The harness rejects every such lane
unless `--profile release` is selected. A complete release `runtime`, `sandbox`,
or `all` run includes the scenario and validates its evidence. The evidence
records and the validator independently bind the `release` profile, the signed
test-binary SHA-256, and the rebuilt Developer initramfs SHA-256 used by that
run.

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
- `container-id-ownership.json` when the runtime container-ID ownership test runs
- `container-id-ownership.json.sha256`, verified by the harness before success
- `runtime-exec-supervision.json` when the focused exec-supervision scenario or
  complete runtime lane runs; its strict schema version 4 binds the release
  profile plus signed test-binary and rebuilt Developer-initramfs SHA-256
  identities, and contains all 12 adapter × termination cells, pre-ready
  cancellation, authenticated pre-spawn rejection, slow-consumer backpressure,
  request-ID reconciliation after pre-ready response loss, post-ready caller
  loss, named and anonymous caller abort at the guest-ready-before-owner
  boundary, exact outer-death, normal-exit descendant cleanup, and final
  zero-leak evidence
- `runtime-exec-supervision.json.sha256`, verified by the harness before success
- `stack-port-forwarding-teardown.json` when the focused port-forwarding
  scenario or complete stack lane runs
- `stack-port-forwarding-teardown.json.sha256`, verified by the harness before
  stack success
- `stack-container-ownership.json` when the focused container-ownership
  scenario or complete stack lane runs
- `stack-container-ownership.json.sha256`, verified by the harness before
  stack success
- `buildkit-artifact/` when a BuildKit lane is selected, containing the
  immutable archive, checksum sidecar, validated manifest and inventory,
  validation report, candidate build provenance when available, and a
  checksum file binding the retained evidence set

A `latest` symlink points to the most recent run.

Run directories are created exclusively. If two runs begin during the same UTC
second, the later run gets a collision-safe suffix rather than sharing or
overwriting the first run's evidence.

Stack lanes use a run-scoped OCI data directory under the timestamped artifact
directory. Stack-namespaced generated IDs and explicit container names
therefore cannot collide with a user's normal `~/.vz/oci` state or with
metadata left by an interrupted earlier gate; HOME remains unchanged for
kernel and registry-credential discovery.

### Stack teardown ownership gate

`stack-port-forwarding` uses a dynamically allocated loopback port and connects
exactly once after stack convergence. Its evidence records exact container IDs,
durable generations, container-to-stack routes, VM handles, shared-stack VMs,
and stack port-forward registries before startup, while active, after service
down, and after shared-VM shutdown. The active and service-down inventories
must prove the selected port is owned; the final inventory must prove the exact
port can be rebound and that containers, routes, handles, listeners, overlays,
and generation reservations are gone.

Both the focused scenario and complete stack lane fail if service down or VM
shutdown returns an error, if the test log cannot be captured, if the evidence
or checksum is missing or malformed, or if the host test log contains a
code-owned `VZ_STACK_TEARDOWN_VIOLATION:<stable_code>` sentinel for a teardown
failure, forbidden fallback, or actual test retry. Sentinel scan errors fail
closed and preserve a diagnostic artifact. A successful test process alone is
therefore insufficient.

Run the focused release gate with:

```bash
./scripts/run-sandbox-vm-e2e.sh \
  --profile release \
  --scenario stack-port-forwarding
```

### Stack container-generation ownership gate

`stack-container-ownership` runs two stacks with the same `db` service against
one runtime. Both creates must reach the runtime admission barrier before
either is released, then converge with distinct stable generated IDs and exact
container-to-stack routes.

The scenario next injects one lost control-plane acknowledgement after the real
runtime has published a running generation. Reconciliation must retain the
runtime-issued `{container_id, generation, stack_id}` proof, remove precisely
that generation, prove its metadata, route, rootfs, overlay, youki state, and
cgroup are absent before recreation, and publish a higher replacement
generation. Finally, two stacks request the same explicit `container_name`.
The contender must return `state_conflict` without cleanup authority while the
owner's route, generation, and raw guest process identity remain unchanged.

The retained `stack-container-ownership.json` records raw lifecycle generation
inventories, route pairs, guest boot/process/cgroup/namespace/root identities,
the ordered generation-cleanup operation, and final leak inventory. The harness
validates its exact schema and invariants with `jq`, verifies its SHA-256
sidecar, and refuses stack success when either artifact is absent or malformed.
The expected reconciliation recovery is executed exactly once; there are no
test retries or fallback cleanup by container ID.

Run the focused release gate with:

```bash
./scripts/run-sandbox-vm-e2e.sh \
  --profile release \
  --scenario stack-container-ownership
```

### Container-ID lifecycle ownership gate

Run the focused release-built Apple-silicon VM scenario with:

```bash
./scripts/run-sandbox-vm-e2e.sh \
  --profile release \
  --scenario runtime-container-id-ownership
```

The scenario exercises caller-selected IDs through standalone and shared-stack
VM paths. It requires both in-flight and already-active duplicates to fail
closed, setup failure cleanup to prove its guest resources, reservation, and
host lifecycle maps are clean before a later explicit recreate, setup commits
to publish atomically, and stop/remove/recreate to preserve one generation owner. The retained
`container-id-ownership.json` contains the raw guest boot ID, init start time,
cgroup, namespace identities, and container-root identity for each generation,
plus final host/guest leak inventories. The harness validates this evidence and
verifies its SHA-256 sidecar before recording both paths in `summary.txt` as
`container_id_ownership=...` and `container_id_ownership_sha256=...`.

The fixed exec/recreate schedule waits for the guest-originated container-ready
acknowledgement, which is emitted only after the exact generation is pinned and
the requested command crosses `execve`. Host lifecycle admission is retained
through that acknowledgement. The test then stops, removes, and recreates the
same ID and requires the acknowledged lifecycle generation and raw guest
identity to differ from the replacement; ordinary process-spawn readiness is
not accepted as evidence.

After the focused scenario, run the complete release nonregression gate:

```bash
./scripts/run-sandbox-vm-e2e.sh --profile release --suite all --keep-going
```

The full gate must pass without required skips or retries. It also retains the
ownership evidence and runs the stack-layer
`stack_service_config_change_triggers_recreate` regression, which proves normal
service configuration drift still performs stop, remove, and recreate.

Archive at least:

- `run-info.txt`
- `summary.txt`
- `runtime-container-id-ownership.log` from the focused run
- `container-id-ownership.json`
- `container-id-ownership.json.sha256`
- `runtime.log`, `stack.log`, and `buildkit.log` from the full run
- `runtime-test-artifacts.jsonl`, `stack-test-artifacts.jsonl`, and the VM serial logs
- the complete `buildkit-artifact/` directory, including
  `buildkit-artifact-evidence.sha256`

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
- local wrapper fails fast when guest OS is not Linux (for example, macOS base images).
- set `VZ_BIN=/path/to/vz` to force a specific host `vz` binary; otherwise it auto-detects PATH or repo-built binaries.

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

## Host-Boot Linux Bootstrap Runner

For direct host-boot Linux guest command execution (no pre-existing VM image), use:

```bash
VZ_BIN=/tmp/vz-target-e2e/debug/vz \
./scripts/run-vz-linux-hostboot-command.sh \
  --name bootstrap-smoke \
  --output-dir .artifacts/vm-linux-hostboot-smoke \
  --command 'echo guest_ok; /bin/busybox uname -s'
```

This bootstrap path:

- initializes descriptor + persistent disk (`vz vm linux init`)
- boots Linux guest (`vz vm linux run`)
- executes command in guest with streamed output and propagated exit code
- stops VM automatically after command completion

## Signing Behavior

The harness signs:

- `crates/target/<profile>/vz` (with virtualization entitlement)
- `crates/target/<profile>/vz-guest-agent`
- each selected VM E2E test binary (with virtualization entitlement)

For BuildKit suites/scenarios, the harness also sets `VZ_BUILDKIT_DIR` to a
per-run artifact directory so stale host cache state does not bleed across runs.
The artifact input is also run-scoped; the test process never consumes an
inherited path directly.

Before `v0.3.21` is published, the normal local BuildKit gate leaves both
artifact override variables unset:

```bash
env -u VZ_BUILDKIT_ARTIFACT_ARCHIVE \
    -u VZ_BUILDKIT_ARTIFACT_SHA256 \
    ./scripts/run-sandbox-vm-e2e.sh --profile release --suite all
```

The harness invokes `scripts/build-runtime-free-buildkit.sh` exactly once,
validates the pinned candidate through
`scripts/validate-runtime-free-buildkit.sh`, copies it into the exclusive run
directory, makes the retained input read-only, and passes only that copy and
its verified digest to the BuildKit test. There is no fallback to a published
asset, an upstream all-binaries archive, a system Docker installation, or a
different OCI runtime. `run-info.txt` and `summary.txt` record
`buildkit_artifact_source_mode=candidate-build`,
`buildkit_builder_invocations=1`, and every retained evidence path.
`run-info.txt` records `buildkit_release_gate_qualified=pending` before VM work;
the final green summary records `buildkit_release_gate_qualified=true` only
after all selected suites and required evidence pass. The builder's source,
toolchain, and build logs remain in `buildkit-candidate-output/`; its
`buildkit-candidate-output.sha256` binds every retained builder output.

An operator can instead test a specific unpublished archive by setting the
pair together:

```bash
VZ_BUILDKIT_ARTIFACT_ARCHIVE=/absolute/path/vz-buildkit-v0.19.0-linux-arm64.tar \
VZ_BUILDKIT_ARTIFACT_SHA256=<exact-64-hex-sha256> \
./scripts/run-sandbox-vm-e2e.sh --profile debug --suite buildkit
```

The pair is checked before any guest rebuild, Cargo build, or VM start. Blank,
singleton, non-file, symlink, malformed-digest, checksum-mismatch, or invalid
archive inputs fail closed. A valid override is copied before validation to
prevent the operator-owned file changing beneath the run. This mode records
`operator-override` and zero builder invocations. Its validation evidence is
retained, but build provenance is reported as unavailable unless it came from
the pinned candidate builder; an operator override is therefore diagnostic and
does not replace candidate-build provenance in a release gate. Operator
overrides are accepted only with the debug profile. A release-profile BuildKit
run rejects them before guest/Cargo/VM work and records
`buildkit_release_gate_qualified=true` only for a validated candidate build
with one builder invocation and complete provenance.

The complete `buildkit` suite additionally retains the guest's OCI runtime
inventory at `buildkit-runtime-inventory.txt` in the timestamped artifact
directory. A missing or empty inventory is a suite failure, and `summary.txt`
records the retained evidence path. Before each BuildKit invocation the harness
removes any prior evidence, then validates the JSON schema and its youki-only
runtime, observed create/run, empty forbidden-path, daemon, and cgroup values.

After `v0.3.21` publishes the immutable BuildKit asset, add and run a separate,
explicit published-source clean-install lane with both override variables
absent and the local candidate cache removed. That lane must prove the default
downloader consumes the published asset. It is deliberately separate from the
pre-release candidate gate so neither source can silently substitute for the
other; publication must never cause this harness to switch sources implicitly.

## Automation

The release workflow in `.github/workflows/release.yml` uses the same pinned
BuildKit builder and validator as this local harness. Real Virtualization.framework
E2E release evidence is collected by running this host harness on Apple-silicon
macOS; there is currently no checked-in `.github/workflows/vm-e2e.yml`.
