# Linux VM Base/Validate/Patch Parity Matrix

This document maps `vz vm mac` base/validate/patch surfaces to Linux support status and required follow-up work.

## Scope

- Compare `vz vm mac` subcommands:
  - `vm mac base ...`
  - `vm mac validate ...`
  - `vm mac patch ...`
- Define Linux parity strategy under daemon-owned runtime flow.

## Support Matrix

| Surface | Linux Status | Short-Term Behavior | Notes |
|---|---|---|---|
| `vm linux base` | Initial support shipped | Daemon-owned list/inspect/upsert/delete | Definitions are persisted by `vz-runtimed` and consumed by CLI over gRPC. |
| `vm linux validate` | Initial support shipped | Daemon-owned descriptor/artifact/backend validation stream | Validation logic executes in daemon, CLI only renders stream output. |
| `vm linux patch` | Not yet supported | Fail with actionable guidance | No Linux patch bundle workflow implemented yet. |

## Required UX for Unsupported Operations

When invoked, unsupported commands should fail with deterministic guidance:

- `vm linux patch`: indicate feature not available and link planned bead.

Failure output requirements:

1. explicit unsupported operation name.
2. suggested replacement command.
3. docs path for details.

## Planned Supported Operations

1. `vm linux patch`
- candidate initial scope:
  - declarative artifact delta apply to descriptor/disks
  - compatibility validation + rollback receipt

2. Extend `vm linux base`
- candidate follow-up scope:
  - policy hooks + receipts for base mutations
  - base/channel promotion semantics analogous to `vm mac base`

## Follow-up Implementation Beads

- `vz-g4ea.4.2.1`: add explicit `vm linux base|validate|patch` unsupported command stubs with actionable guidance.
- `vz-g4ea.4.2.2`: implement first Linux `validate` surface (descriptor+artifact+agent readiness checks) via daemon.
- `vz-g4ea.4.2.3`: design and implement Linux base definition management APIs.
- `vz-g4ea.4.2.4`: design and implement Linux patch workflow and receipts.
