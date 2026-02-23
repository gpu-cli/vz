# 04 — Testing, Rollout, and Risks

## Test Strategy

## CI Matrix

1. `linux-x86_64`:
   - unit tests (all crates)
   - backend integration tests (mock + privileged where available)
2. `linux-arm64`:
   - unit tests
   - OCI lifecycle smoke tests
3. `macos-arm64`:
   - existing VM-backed integration tests
   - regression suite for runtime facade behavior

## Test Layers

### Layer 1 — Unit

- backend selection logic
- config parsing and defaults
- error mapping
- bundle generation

### Layer 2 — Integration (host)

- pull/run/exec/stop/remove
- cgroup CPU controls (`cpu.max`)
- mount + working directory behavior
- port publish lifecycle

### Layer 3 — End-to-End

- `vz oci run` smoke scenarios
- `vz stack up/down` multi-service scenarios
- validation cohort run on Linux host
- `planning/linux-native-support/run-linux-stack-config-matrix.sh` for automated config-matrix validation (60+ pass/fail variants)
- manual matrix execution from `05-manual-linux-stack-harness.md` for networking/dependency/logging edge cases

## Rollout Plan

1. Hidden backend flag (`VZ_BACKEND=linux-native`) while stabilizing.
2. Beta enablement on Linux by default with override back to legacy/test backend.
3. GA once compatibility matrix and failure telemetry are acceptable.

## Compatibility Matrix

Track support by distro/kernel:

- Ubuntu LTS (primary)
- Debian stable
- Fedora (latest)
- Arch (best effort)

Also track required capabilities:

- cgroup v2 enabled
- unprivileged user namespaces (for rootless)
- required runtime binary availability

## Operational Observability

- structured lifecycle logs with container ID + backend
- explicit reason codes for unsupported host/kernel features
- startup diagnostics command (`vz oci doctor` future)

## Risks and Mitigations

1. Kernel feature variability across distros.
   - Mitigation: capability probes + clear unsupported errors.
2. Rootless runtime differences.
   - Mitigation: explicit compatibility tiers; rootful fallback mode.
3. Networking complexity and conflict with host setup.
   - Mitigation: backend abstraction + incremental networking feature set.
4. Regression risk on macOS path during refactor.
   - Mitigation: adapter-first migration and parity tests.

## Done When

- Linux CI lane is green for runtime and stack smoke tests.
- macOS regression suite remains green.
- Known limitations are documented with deterministic failure behavior.
- Linux-native backend can be enabled by default without major blockers.
