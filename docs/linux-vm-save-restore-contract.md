# Linux VM Save/Restore Contract

This document defines how `vz vm linux` save/restore semantics map to daemon checkpoint primitives.

## Scope

- Applies to daemon-owned Linux spaces (`vz vm linux ...`).
- Does not change `vz vm mac save|restore` behavior (Virtualization.framework state files).

## UX Contract

Planned Linux-facing commands:

- `vz vm linux save <sandbox-id> [--class fs_quick|vm_full] [--tag <tag>] [--state-db <path>]`
- `vz vm linux restore <sandbox-id> <checkpoint-id> [--state-db <path>]`

Equivalent existing control-plane operations:

- Save maps to `create_checkpoint`.
- Restore maps to `restore_checkpoint`.

The canonical path remains daemon gRPC first, with optional HTTP API transport parity.

## Checkpoint Class Mapping

- `fs_quick` (default):
  - Linux btrfs-backed filesystem snapshot semantics.
  - Fast checkpoint/restore loops.
  - Expected default for frequent interactive save points.
- `vm_full`:
  - Full machine-state replay semantics when backend supports it.
  - Higher overhead and stricter compatibility coupling.
  - Must fail closed on unsupported backend capability.

`vz vm linux save` defaults to `fs_quick` to match Spaces fast-iteration behavior.

## Retention and Lineage

- `--tag` on save maps to checkpoint retention tag.
- Tagged checkpoints are protected from default GC policy.
- Untagged checkpoints remain subject to daemon retention policy.
- Restore does not mutate checkpoint lineage metadata; it mutates sandbox state to checkpoint guarantees.

## Error Model

- Unsupported class/backend capability returns deterministic validation/backend error (no silent downgrade).
- Restore compatibility mismatch fails before apply using checkpoint fingerprint checks.
- Missing checkpoint returns not-found.

## Implementation Follow-up

Implementation bead: `vz-g4ea.4.3.1` (CLI wiring for `vz vm linux save|restore` aliases).
