# OCI Compliance + Compose Runtime (youki)

Date: 2026-02-18

## Goal

Move `vz-oci` from "OCI-compatible run path" to a true OCI runtime-spec implementation by running `youki` inside Linux guests, then build a Compose-class stack control plane on top (`vz-stack`).

This track is aligned with private stack-runtime notes, and the required design constraints are restated in this folder so execution does not depend on external files.

## Why This Track Exists

Current code in `crates/vz-oci` has an `ExecutionMode::OciRuntime` flag, but the runtime path is still a stub and falls back to guest-agent direct exec.

That means we currently:

- pull OCI images and assemble rootfs correctly
- do not run a runtime-spec lifecycle (`create/start/state/exec/kill/delete`) via an OCI runtime
- do not expose stack reconciliation semantics needed for real Compose-class workflows

## Design Decision

Use a split model:

- Host side (`vz-oci`) owns image distribution, rootfs assembly, VM lifecycle, and orchestration.
- Guest side owns OCI runtime-spec execution through `youki`.

This preserves the VM-per-service isolation model while making container semantics OCI-compliant.

Linux OCI lifecycle dispatch is owned by the Linux guest-agent path (`crates/vz-linux/src/agent.rs`), not the macOS `crates/vz-guest-agent` crate.

## Architecture

```text
Host macOS
  vz-cli / SDK
      |
      v
  vz-stack (spec/reconciler/state/events)
      |
      v
  vz-oci (image pull, bundle build, VM lifecycle)
      |
      v   vsock protocol
Linux guest VM
  vz-linux guest agent
      |
      v
  youki (OCI runtime-spec lifecycle)
      |
      v
  container process tree
```

## Standards Target

- OCI Distribution Spec: image pull/auth/digest behavior (host).
- OCI Image Spec: manifest/config/layers + deterministic rootfs assembly (host).
- OCI Runtime Spec: bundle + lifecycle semantics via `youki` (guest).
- Compose Spec: adapter layer on top of typed `StackSpec` (not the source of truth).

Details are in `01-compliance-target.md`.

## Delivery Phases

1. Compliance contract + gap closure in `vz-oci` interfaces.
2. `youki` lifecycle integration in guest path (real `OciRuntime` mode).
3. Long-lived container/service lifecycle (not run-then-stop only).
4. `vz-stack` reconciler and durable state store.
5. Compose adapter and compatibility tests.
6. Common-image validation matrix and CI gates (PR/nightly/weekly).

## Document Index

- `01-compliance-target.md`: compliance scope, pass/fail contract, and non-goals.
- `02-youki-integration.md`: host/guest boundary and concrete `youki` lifecycle mapping.
- `03-stack-control-plane.md`: stack runtime design for networking/volumes/reconciliation.
- `04-beads.md`: dependency-ordered bead plan with acceptance criteria.
- `05-image-validation.md`: extensive validation matrix for common images and CI gating.
