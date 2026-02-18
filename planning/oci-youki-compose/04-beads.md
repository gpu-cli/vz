# 04 - Bead Breakdown

## Execution Strategy

Order beads by dependency so each delivers a testable increment.

Legend:

- `Blockers`: prerequisite bead IDs.
- `Done when`: objective acceptance signal.

## Milestone A: OCI Runtime Core Path

### B01 - Runtime mode contract

- Scope: replace placeholder `OciRuntime` fallback with explicit `NotImplemented` error until real path lands.
- Blockers: none
- Done when: no silent fallback from `OciRuntime` to guest-exec.

### B02 - Bundle generator + runtime-spec type model

- Scope: add OCI bundle writer and adopt `oci-spec` crate for runtime `config.json` generation.
- Blockers: B01
- Done when: unit tests verify generated config for cmd/env/cwd/user/mount mappings.

### B03 - Guest OCI protocol types

- Scope: add typed request/response protocol for OCI lifecycle messages.
- Blockers: B01
- Done when: protocol round-trip tests cover create/start/state/exec/kill/delete.

### B04 - youki provisioning pipeline

- Scope: provision pinned `youki` (`linux/arm64`) into Linux guest artifacts with checksum/version validation.
- Blockers: B03
- Done when: booted Linux VM can execute `youki --version` deterministically.

### B05 - Linux guest agent OCI dispatch

- Scope: add OCI dispatch handlers to Linux agent path (`crates/vz-linux/src/agent.rs`), explicitly not `crates/vz-guest-agent`.
- Blockers: B03
- Done when: protocol-driven OCI lifecycle requests reach Linux agent handlers.

### B06 - Linux guest `youki` invocation + error mapping

- Scope: implement command invocation and stable error/result mapping from Linux agent to host protocol.
- Blockers: B04, B05
- Done when: integration test executes create/start/state/delete via `youki`.

### B07 - Host lifecycle wiring

- Scope: map `vz-oci` lifecycle ops to typed OCI protocol calls.
- Blockers: B02, B06
- Done when: `OciRuntime` executes command path through `youki exec`.

### B08 - Stop/remove semantics replacement

- Scope: replace host PID signal logic with runtime lifecycle semantics (`kill`, `delete`, `state`).
- Blockers: B07
- Done when: graceful and forced stop state transitions are runtime-driven and correct.

## Milestone B: Service-Grade Runtime

### B09 - RuntimeConfig and RunConfig OCI extensions

- Scope: add `guest_oci_runtime`, `guest_oci_runtime_path`, `guest_state_dir`, and run-level OCI fields.
- Blockers: B07
- Done when: config values are plumbed end-to-end and covered by unit tests.

### B10 - Guest overlay and mount setup

- Scope: implement read-only lower rootfs + writable overlay + deterministic bind/named mount ordering in guest.
- Blockers: B02, B07
- Done when: writable overlay semantics and mount order are validated in integration tests.

### B11 - Mount support in public run surface

- Scope: add mount model to `RunConfig` and CLI `-v/--volume`; emit into bundle/runtime path.
- Blockers: B02, B07, B10
- Done when: declared mounts appear and behave correctly in running OCI lifecycle containers.

### B12 - Long-lived container handles

- Scope: split create/start/exec/stop from current run-then-stop single-shot path.
- Blockers: B07
- Done when: container remains running after start and supports later exec/stop/remove.

### B13 - Runtime state store v2

- Scope: persist desired/observed OCI lifecycle state and runtime metadata.
- Blockers: B08, B12
- Done when: runtime restart can rehydrate and reattach state without orphaning containers.

### B14 - Crash recovery conformance

- Scope: recovery tests for host process crash/restart and re-sync from runtime source of truth.
- Blockers: B13
- Done when: crash recovery scenarios pass in CI integration suite.

### B15 - Lifecycle conformance harness

- Scope: add create/start/state/exec/kill/delete conformance suite including exec-while-running assertions.
- Blockers: B08, B12
- Done when: conformance suite is green in CI for Linux guest path.

## Milestone C: Stack Runtime Core

### B16 - `vz-stack` crate skeleton + sqlite state store

- Scope: add crate, typed `StackSpec`, `apply()` entrypoint, and sqlite `state_store.rs`.
- Blockers: B13
- Done when: no-op reconcile persists desired/observed records.

### B17 - Stack event pipeline

- Scope: implement structured event emission/storage API (`ServiceCreating`, `ServiceReady`, `PortConflict`, etc.).
- Blockers: B16
- Done when: reconciler emits persisted events and API consumers can stream them.

### B18 - Reconciler core

- Scope: implement diff planner + ordered executor for service actions.
- Blockers: B16
- Done when: deterministic behavior is proven:
  same input state produces same ordered actions;
  repeated apply with unchanged spec is no-op;
  service ordering is stable across runs.

### B19 - Volume planner

- Scope: bind/named/ephemeral planning with recreate-on-mount-change behavior.
- Blockers: B16, B18
- Done when: named volume data survives recreate and ephemeral volume GC works.

### B20 - Network backend abstraction (gvproxy first)

- Scope: implement `NetworkBackend` with `gvproxy` as first shipping backend and explicit binary provisioning.
- Blockers: B16, B18
- Done when: per-stack isolation and published port reconciliation work end-to-end with `gvproxy`.

### B21 - Health/dependency gating

- Scope: readiness/health and depends_on orchestration.
- Blockers: B18
- Done when: dependent services wait on readiness deterministically.

## Milestone D: Compose Adapter

### B22 - Compose subset importer (strict feature contract)

- Scope: parse Compose subset to `StackSpec`; reject unsupported keys with stable errors.
- Blockers: B16, B18, B19, B20
- Done when: accepted/rejected key behavior matches `01-compliance-target.md`.

### B23 - `vz stack` CLI surface

- Scope: `vz stack up/down/ps/events` commands backed by stack APIs.
- Blockers: B17, B18
- Done when: CLI drives stack lifecycle and consumes event pipeline.

### B24 - Compose compatibility fixtures

- Scope: canonical `web+redis` and `web+postgres+redis` fixture tests.
- Blockers: B22, B23
- Done when: fixtures run without manual networking/volume workarounds.

## Milestone E: Common Image Validation

### B25 - Validation harness foundation

- Scope: add shared validation harness for lifecycle/mount/network scenario execution.
- Blockers: B15
- Done when: scenarios run against at least one image with structured pass/fail output.

### B26 - Common image cohort manifest

- Scope: define pinned common-image cohorts and expected behavior profiles.
- Blockers: B25
- Done when: cohort manifest is versioned and consumed by harness.

### B27 - Validation CI infrastructure

- Scope: add runners/jobs/cache strategy and artifact retention for Tier 1/2/3 suites.
- Blockers: B26
- Done when: CI can execute matrix suites reproducibly with published artifacts.

### B28 - Tier 1 PR smoke gate

- Scope: enforce smoke suite on `alpine`, `python`, `nginx` with core lifecycle checks.
- Blockers: B27
- Done when: PR CI fails on lifecycle/env/cwd/user/port regressions for smoke cohort.

### B29 - Tier 2 nightly conformance

- Scope: run full cohort matrix nightly with lifecycle/mount/network/compose scenarios.
- Blockers: B27, B24
- Done when: nightly report is published with per-image scenario outcomes.

### B30 - Tier 3 weekly stress

- Scope: long-run stress loops and scale/concurrency scenarios on representative cohorts.
- Blockers: B29
- Done when: weekly report tracks flake rate and hard failures.

## Suggested First Execution Slice

Start with: `B01 -> B02 -> B03 -> B04 -> B05 -> B06 -> B07 -> B08 -> B12 -> B15`.

This is the smallest path to a true OCI runtime lifecycle backed by `youki` plus conformance proof.
