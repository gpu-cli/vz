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
| `vm linux patch` | Initial support shipped | Daemon-owned apply/rollback stream with receipt linkage | Patch bundles update base definitions and persist rollback snapshots in daemon runtime data. |

## Required UX for Unsupported Operations

Current unsupported gap:

- Advanced patch planning/authoring ergonomics (`create`, `verify`, signed bundles) from `vm mac patch`.

## Planned Supported Operations

1. Extend `vm linux patch`
- candidate follow-up scope:
  - signed patch bundle verification and trust policy hooks
  - richer incompatibility diagnostics and dry-run mode

2. Extend `vm linux base`
- candidate follow-up scope:
  - policy hooks + receipts for base mutations
  - base/channel promotion semantics analogous to `vm mac base`

## Follow-up Implementation Beads

- `vz-g4ea.4.2.1`: add explicit `vm linux base|validate|patch` unsupported command stubs with actionable guidance.
- `vz-g4ea.4.2.2`: implement first Linux `validate` surface (descriptor+artifact+agent readiness checks) via daemon.
- `vz-g4ea.4.2.3`: design and implement Linux base definition management APIs.
- `vz-g4ea.4.2.4`: design and implement Linux patch workflow and receipts.
