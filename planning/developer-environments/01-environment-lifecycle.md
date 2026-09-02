# First-class Developer Environment identity and lifecycle

Depends on: Product contract and terminology

## Purpose

Replace inferred `vz.run.*` behavior with a versioned runtime contract for Developer Environments and correct stop/delete semantics.

## Step 1: Add explicit environment class

- Extend runtime-contract and wire types with an environment class, immutable `TargetSpec` (`os`, `arch`, image/version/channel), requested requirements, and negotiated capabilities.
- Persist and return the class through manager, gRPC, API, CLI JSON, receipts, and inspection.
- Migrate legacy project records deterministically without confusing Hardened sandboxes with Developer environments.

## Step 2: Make profile selection explicit

- Select a backend from `(host OS, target OS, architecture)` and fail explicitly for unsupported tuples.
- Select the Developer kernel/profile for Linux Developer environments at the backend boundary.
- Select pinned native image metadata and TargetAdapter for macOS/Windows environments.
- Retain Container as a deprecated internal alias for Hardened during migration.
- Fail closed on artifact/profile mismatches.

## Step 3: Separate stop from delete

- Add a resumable stopped state and valid transitions.
- Implement stop/start/restart without deleting persistent disks or managed contexts.
- Keep delete explicit, destructive, ownership-checked, and recoverable where practical.
- Ensure daemon restart reconstructs authoritative environment state and cleans stale live resources.

## Step 4: Define resource ownership

Persist the environment-to-VM/container/disk/socket/context/network mapping in one daemon-owned record. Use bounded collision-checked keys for filesystem and socket resources.

Define `HostBackend`, `TargetAdapter`, and `CapabilitySet` contracts for isolation, process/console semantics, filesystem sharing, networking, service supervision, capability endpoints, suspend/resume, and checkpoints. Unsupported capabilities are explicit status fields and errors rather than silent target substitution.

## Validation

- Contract serialization/migration tests across old and new records.
- State-machine and idempotency tests for create/start/stop/restart/delete/failure.
- Real local VM test proving stopped state resumes with the same environment identity and disk.
- Hardened artifact selection remains unchanged and rejects Developer metadata.
- Contract fixtures and JSON/protobuf round trips are identical on macOS, Linux, and Windows builds.
