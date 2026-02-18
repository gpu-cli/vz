# 03 - Stack Control Plane (Compose Replacement Path)

## Objective

Define the orchestration layer that sits above `vz-oci` and turns single-container runtime primitives into deterministic multi-service stack behavior.

## Principle

`StackSpec` is the source of truth.

Compose YAML is an input adapter, not the core model.

## Crate Boundary

Add `crates/vz-stack`:

```text
vz-stack
  spec.rs           # typed desired state
  reconcile.rs      # diff + action planner
  state_store.rs    # sqlite desired/observed/events
  service/          # service lifecycle adapters
  network/          # backend trait + implementations
  volume/           # mount planner + volume manager
  events.rs         # structured progress stream
```

`vz-oci` remains workload runtime; `vz-stack` owns orchestration.

## Typed Spec (Minimal v1)

Core objects:

- `StackSpec`
- `ServiceSpec`
- `NetworkSpec`
- `VolumeSpec`

Key service fields:

- image
- command/args
- env
- resources (cpu/memory)
- mounts
- published ports
- depends_on
- healthcheck
- policy (network/egress)

## Reconciler Contract

`apply(stack_spec)` must be:

- idempotent
- convergent
- restart-safe

Deterministic means:

- same desired spec + same observed state => same ordered action plan
- repeated `apply()` without spec change => no-op
- dependency ordering is stable and explicit (topological + tie-break by service name)
- failures are replay-safe from persisted checkpoints

Loop:

1. Load desired + observed state.
2. Compute action graph.
3. Execute in dependency order.
4. Persist progress/event log.
5. Retry transient failures with backoff.

## Networking Model

Need:

- per-stack isolation boundary
- service discovery names
- east-west connectivity
- host port publish reconciliation

Approach:

- `NetworkBackend` trait.
- Ship `gvproxy` backend first in v1.
- Keep fallback host-proxy backend for bootstrap/dev.

Provisioning requirement:

- `gvproxy` binary/version provisioning and discovery is part of the runtime deliverable, not an implicit host prerequisite.

## Volume Model

Volume types:

- `bind`
- `named`
- `ephemeral`
- `secret`

Constraints:

- VirtioFS mounts are static at VM creation.
- Any mount topology change implies service recreate.

Design:

- mount planner resolves all mounts at apply-time
- planner generates stable mount tags and VM mount set
- named volumes stored in runtime data dir with metadata

## Compose Adapter Strategy

Phases:

1. Parse Compose subset -> `StackSpec`.
2. Validate unsupported fields early and fail with actionable error.
3. Run same reconciler path as typed SDK usage.

This keeps behavior consistent between SDK and YAML flows.

## Observability and Debuggability

Must have:

- structured event stream (`ServiceCreating`, `ServiceReady`, `PortConflict`, etc.)
- durable state transitions in sqlite
- `vz stack ps` / `vz stack events` surfaces

Implementation mapping:

- event production pipeline is part of stack runtime core (not CLI-only)
- CLI surfaces are consumers of the event/state API

## Initial Exit Criteria

- Multi-service stack (`web + db + cache`) comes up deterministically.
- Restarting host process recovers observed state without orphaning resources.
- `down` tears down services/networks/ephemerals predictably.
