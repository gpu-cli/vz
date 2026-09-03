# First-class Environment topology, Machine identity, and durable lifecycle

Depends on: Product contract and terminology

## Purpose

Replace inferred `vz.run.*` behavior with a versioned aggregate contract for
Projects, Environment instances, and target-native Machines, including durable
ownership and correct stop/delete semantics.

## Step 1: Add the aggregate schema

- Add versioned `ProjectDefinition`, `WorkspaceBinding`, `EnvironmentInstance`,
  `MachineSpec`, `MachineInstance`, `MachineIncarnation`, `Network`, `Endpoint`,
  and ownership records.
- Require an explicit `Developer` or `Hardened` Machine profile in new 0.4
  definitions. Developer is capability-rich; Hardened is restricted and valid
  only for supported Linux targets. Migration assigns the profile from legacy
  provenance rather than defaulting ambiguous records.
- Put immutable `TargetSpec` (`os`, `arch`, image/version/channel), requested
  requirements, backend, and negotiated capabilities on each Machine.
- Persist and return project/environment/machine IDs through manager, gRPC,
  APIs, status JSON, events, errors, receipts, and inspection.
- Migrate a legacy project record deterministically to one Environment with one
  Machine without confusing Hardened sandboxes with Developer Machines.

## Step 2: Make profile selection explicit

- Select a Machine backend from `(host OS, Machine target OS, architecture)` and
  fail explicitly for unsupported tuples.
- Select the declared Developer/Hardened kernel profile for Linux Machines at
  the backend boundary; reject invalid target/profile combinations.
- Select pinned native image metadata and TargetAdapter for macOS/Windows Machines.
- Retain Container as a deprecated internal alias for Hardened during migration.
- Fail closed on artifact/profile mismatches.

## Step 3: Reconcile aggregate and child lifecycle

- Reconcile Machines, networks, endpoints, volumes, and dependencies as one
  idempotent Environment operation with streamed progress and a terminal receipt.
- Add resumable aggregate and Machine states with explicit degraded/failure
  behavior and replaceable Machine incarnations.
- Implement stop/start/restart without deleting persistent disks or
  Machine-specific managed contexts.
- Keep delete explicit, destructive, ownership-checked, and recoverable where practical.
- Ensure daemon restart reconstructs authoritative Environment/Machine/topology
  state and cleans stale live resources.

## Step 4: Define resource ownership

Persist the Environment ownership graph, including every
Machine/VM/container/disk/socket/context/network/endpoint mapping, in
daemon-owned records. Use bounded collision-checked keys containing Environment
and Machine identity where applicable.

Define `EnvironmentSupervisor`, `HostBackend`, `TargetAdapter`, `CapabilitySet`,
`StorageBackend`, `WorkspaceBackend`, and `NetworkBackend` contracts. Unsupported
capabilities are explicit rather than silently substituting another Machine.

## Validation

- Contract serialization/migration tests across old and new aggregate records.
- State-machine and idempotency tests for aggregate and Machine lifecycle.
- Real local VM tests proving multiple Environments per project/worktree,
  multiple Machines per Environment, and identity-preserving stop/up.
- Failure during reconciliation recovers without duplicate or cross-owned
  resources; ambiguous selection fails with candidates.
- Hardened artifact selection remains unchanged and rejects Developer metadata.
- Contract fixtures and JSON/protobuf round trips are identical on macOS, Linux, and Windows builds.
