# On-Demand Privilege Elevation for Provisioning

## Context

`vz` can already create macOS images from Apple-sourced restore media, and it can provision users/agents/SPICE into those images. The current friction is privilege handling:

- Some offline image operations require host admin privileges.
- Users can run most of the flow unprivileged, then fail deep in provisioning.
- Documentation currently requires users to remember a separate `sudo vz provision ...` step.

Goal: keep a one-command experience while only requesting elevation when required.

## Problem Statement

A fully rootless offline provisioning path is not reliable on macOS for current implementation details:

- `diskutil enableOwnership` requires root.
- Writing/modifying launchd-owned paths and dslocal state in mounted guest volume may require root semantics.
- Prior sudo runs can leave host-side mountpoints/ownership that make later rootless runs fail in confusing ways.

We need deterministic privilege behavior with explicit UX and bounded scope.

## Goals

1. One-command setup flow (`vz init`) for end users.
2. Request elevation only at the point privileged operations begin.
3. Keep privileged execution scope minimal and auditable.
4. Preserve non-interactive automation behavior (clear failure + exact rerun command).
5. Maintain legal-safe distribution model (Apple bits downloaded locally by user).

## Non-Goals

1. Eliminating all privileged operations in offline provisioning.
2. Building a permanent privileged background daemon in the first iteration.
3. Changing image legal/distribution policy in this plan.

## UX Design

### Interactive TTY flow

1. User runs `vz init`.
2. `vz` performs all unprivileged steps first (IPSW resolution, disk creation/install).
3. Before privileged provisioning stage, `vz` checks privilege requirement.
4. If elevation is needed and current user is not root:
   - print concise explanation of why,
   - request elevation once,
   - re-exec the same stage under `sudo`.
5. Continue pipeline and return to normal user-facing output.

### Non-interactive flow (CI/scripts)

If no TTY and elevation is needed:

- fail fast with a structured message,
- include exact rerun command (for example: `sudo vz init ...` or `sudo vz provision ...`),
- non-zero exit code that callers can detect.

## Technical Approach

### Phase 1 (recommended): Stage re-exec via `sudo`

Implement a small elevation module in `vz-cli`:

- detect `is_root` + `isatty`,
- build an allowlisted re-exec command for a specific stage,
- execute `sudo` with explicit argv (no shell string concatenation),
- preserve only required environment.

Target stages:

1. Offline provisioning/mount mutation stage.
2. Any root-required ownership fix path.

### Phase 2 (optional): Native privileged helper

If needed later, move from `sudo` re-exec to a signed helper tool pattern. Keep this out of initial scope unless there is strong UX/security pressure.

## Command/Stage Model

Introduce explicit stage boundaries in `init`:

1. `resolve_ipsw`
2. `install_macos`
3. `offline_provision` (privileged)
4. `post_provision_finalize`

Only `offline_provision` should be elevation-gated in v1.

## Security Constraints

1. No shelling with interpolated strings for elevated execution.
2. Strict allowlist of which commands/subcommands can be elevated.
3. Minimize elevated lifetime (single stage, then return).
4. Log stage transitions and privilege transitions.
5. Redact secrets from logs.

## Error Handling

Explicit user-facing errors:

1. Elevation denied/canceled.
2. `sudo` unavailable.
3. No TTY available.
4. Elevated stage failed (include stage name and actionable next step).

## Testing Plan

1. Unit tests for argv reconstruction and allowlist enforcement.
2. Unit tests for interactive/non-interactive branching.
3. Integration test that verifies clear message + rerun command when elevation is required in non-interactive mode.
4. Regression test for mixed root/non-root runs to avoid mountpoint ownership conflicts.

## Rollout Plan

1. Implement stage-gated elevation in `vz provision` path.
2. Wire `vz init` to call provisioning in-process and trigger elevation automatically.
3. Update CLI/docs to present one-command flow as default.
4. Keep explicit `sudo vz provision` as documented fallback for power users.

## Open Questions

1. Should `vz init` always auto-run provisioning, or be configurable with `--no-provision`?
2. Should we add a `--yes` flag to auto-accept elevation prompt in interactive contexts?
3. Do we want JSON-structured error codes for orchestration wrappers immediately, or after v1?
