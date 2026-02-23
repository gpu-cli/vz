# Linux Native Host Support — Planning Overview

## Vision

Enable `vz` to run sandbox/container workloads on **Linux hosts** while keeping the macOS Virtualization.framework path intact. The primary principle is:

- keep backend-specific primitives isolated,
- keep runtime contract shared,
- keep caller-facing API stable.

This allows Linux support now without blocking a later cleanup/refactor.

## Problem Statement

Current architecture supports:

- macOS host via Virtualization.framework (`vz`, `vz-sandbox`, `vz-linux`, `vz-oci`)
- Linux guest containers inside macOS VMs

It does **not** support Linux host execution because key crates are gated to `target_os = "macos"` and the runtime path assumes VZ-backed VMs.

We need a Linux-native backend that reuses existing lifecycle/protocol concepts and minimizes churn in:

- `vz-oci`
- `vz-stack`
- `vz-validation`
- CLI commands and caller integrations

## Goals

1. Run OCI workloads on Linux hosts with the same high-level runtime API.
2. Reuse existing request/response shapes, run config, and lifecycle semantics.
3. Keep macOS behavior unchanged.
4. Introduce backend abstraction now so later refactors are incremental.

## Non-Goals (Phase 1)

- Replacing macOS VZ implementation.
- Full Docker API compatibility.
- Perfect feature parity across every Linux distro/kernel variant on day one.

## Proposed Architecture

```
Caller (vz-cli / vz-stack / libraries)
            |
            v
      Runtime Facade (shared contract)
        /                    \
       v                      v
MacOS Virtualization     Linux Native Backend
(existing path)          (new primitives crate)
```

### New Components

- `vz-runtime-contract` (new crate)
  - backend traits + backend-neutral runtime types
- `vz-linux-native` (new crate)
  - Linux host primitives and lifecycle executor
- `vz-macos-backend` (new crate or module split)
  - adapter around existing macOS runtime behavior

### Existing Components Reused

- OCI image pull/store/caching logic from current `vz-oci`
- run/exec/stop/remove lifecycle semantics
- stack-level orchestration (`vz-stack`)
- validation scenario model (`vz-validation`)

## Implementation Phases

### Phase 1 — Shared Runtime Contract

Define backend interface and move shared runtime-facing types out of backend-specific code.

### Phase 2 — Linux Native Primitives Crate

Implement Linux host backend as a dedicated crate with OCI lifecycle primitives.

### Phase 3 — Runtime Integration

Refactor `vz-oci` to route through backend trait and select backend by host OS/config.

### Phase 4 — CLI/Stack/Validation Integration

Compile/run on Linux host with backend-appropriate command availability.

### Phase 5 — Hardening + Rollout

CI matrix, distro compatibility, feature flags, staged release.

## Dependency Graph

```
vz-cli / vz-stack / vz-validation
            |
            v
          vz-oci
            |
            v
   vz-runtime-contract
      /            \
     v              v
vz-macos-backend  vz-linux-native
```

## Done When

- Linux host can run `vz oci run ubuntu:24.04 -- echo ok` successfully.
- `vz-stack up` works on Linux for supported Compose subset.
- Existing macOS tests continue to pass with no functional regression.
- Backend selection is explicit and test-covered.

## Documents

- `01-shared-runtime-contract.md`
- `02-linux-native-backend.md`
- `03-runtime-selection-and-integration.md`
- `04-testing-rollout-and-risks.md`
- `05-manual-linux-stack-harness.md`
- `run-linux-stack-config-matrix.sh`
