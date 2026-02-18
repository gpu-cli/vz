# 01 - Compliance Target

## Objective

Define "true OCI compliant layer" in precise terms so implementation and testing are unambiguous.

## Compliance Contract

We treat compliance as three layers:

1. OCI distribution/image compliance on host.
2. OCI runtime-spec compliance in guest through `youki`.
3. Compose compatibility through an adapter over a typed stack model.

## Layer 1: OCI Distribution + Image (Host)

Required behavior:

- Resolve references (`registry/name:tag` and `@digest`).
- Pull manifests/config/layers with auth and digest validation.
- Select `linux/arm64` variants from image indexes.
- Assemble rootfs deterministically with whiteout semantics.

Current status:

- Mostly implemented in `vz-oci` image store/puller.
- Keep this as the host responsibility.

## Layer 2: OCI Runtime Spec (Guest via youki)

Required behavior:

- Build an OCI bundle (`config.json` + `rootfs`) per container.
- Execute lifecycle through `youki`:
  - `create`
  - `start`
  - `state`
  - `exec`
  - `kill`
  - `delete`
- Report container state based on runtime state, not host PID guessing.
- Preserve OCI semantics for args/env/cwd/user/mounts.

Current gap:

- `ExecutionMode::OciRuntime` currently routes to guest direct exec instead of runtime lifecycle.

## Layer 3: Compose Compatibility (Adapter)

Required behavior:

- Source of truth is typed `StackSpec` in Rust.
- Compose YAML is parsed into `StackSpec`, then reconciled.
- Support initial compose subset:
  - services image, command/entrypoint, environment, working_dir, user
  - ports
  - volumes (bind + named + ephemeral)
  - depends_on + health/readiness ordering
- Deterministic apply/reconcile behavior with durable state.

Non-goal for first release:

- Full Docker Engine parity.

## Compose Feature Contract (v1)

Accepted keys:

- Top-level: `services`, `volumes`
- Service: `image`, `command`, `entrypoint`, `environment`, `working_dir`, `user`, `ports`, `volumes`, `depends_on`, `healthcheck`, `restart`
- Volume: `driver` (only `local`), `driver_opts` (restricted, host-safe subset)

Rejected in v1 (hard error with actionable message):

- `build`
- `networks` (custom network definitions beyond default stack network)
- `configs`
- `secrets` (Compose-native object form)
- `deploy`
- `profiles`
- `extends`
- `devices`
- `extra_hosts`
- `ipc`, `pid`, `cgroup`, `runtime`

Behavior contract:

- Unsupported keys fail validation before reconciliation starts.
- Parser emits a stable feature error code for each rejected key.
- `B22` in `04-beads.md` implements this contract.

## Acceptance Criteria

### OCI runtime compliance bar

- `Runtime::run(..., execution_mode = OciRuntime)` no longer uses guest direct exec path.
- Bundle is materialized and validated before start.
- `stop/remove/ps/state/exec` map to runtime lifecycle and preserve state transitions.

### Conformance tests

- Runtime-spec lifecycle tests for create/start/state/exec/kill/delete.
- Regression tests for env/cwd/user/mount behavior.
- Guest runtime errors are surfaced with stable error mapping.

### Compose readiness bar

- Multi-service stack with dependency ordering starts deterministically.
- Published port conflicts are detected before apply.
- Named volumes survive service restart.

### Common image validation bar

- Tier 1 PR smoke matrix passes for representative base/language/service images.
- Tier 2 nightly matrix passes for full common-image cohort.
- Validation includes entrypoint/cmd, env/cwd/user, lifecycle, mounts, and networking semantics.

## Scope Boundaries

In scope:

- Linux guest containers through `youki`.
- VM-per-service isolation.
- SDK-first reconciler.

Out of scope (initial):

- Running `youki` on host macOS.
- Full OCI hooks matrix support in v1 if guest/kernel constraints block specific hook types.
- Full Docker API emulation.
