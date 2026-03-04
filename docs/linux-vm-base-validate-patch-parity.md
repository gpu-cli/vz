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
| `vm linux base` | Not yet supported | Fail with actionable guidance | Linux uses `vm linux init` descriptor+artifact flow today. |
| `vm linux validate` | Not yet supported | Fail with actionable guidance | Validation currently covered by host-boot and e2e harnesses. |
| `vm linux patch` | Not yet supported | Fail with actionable guidance | No Linux patch bundle workflow implemented yet. |

## Required UX for Unsupported Operations

When invoked, commands should fail with deterministic guidance:

- `vm linux base`: suggest `vz vm linux init --name ...` and link docs.
- `vm linux validate`: suggest `vz vm linux test e2e ...` and staging validation runbook.
- `vm linux patch`: indicate feature not available and link planned bead.

Failure output requirements:

1. explicit unsupported operation name.
2. suggested replacement command.
3. docs path for details.

## Planned Supported Operations

1. `vm linux validate`
- candidate initial scope:
  - artifact checksum validation
  - descriptor consistency checks
  - guest-agent readiness smoke check

2. `vm linux patch`
- candidate initial scope:
  - declarative artifact delta apply to descriptor/disks
  - compatibility validation + rollback receipt

3. `vm linux base`
- candidate initial scope:
  - managed Linux base definitions from `~/.vz/linux` sources
  - list/inspect/update/delete operations via daemon APIs

## Follow-up Implementation Beads

- `vz-g4ea.4.2.1`: add explicit `vm linux base|validate|patch` unsupported command stubs with actionable guidance.
- `vz-g4ea.4.2.2`: implement first Linux `validate` surface (descriptor+artifact+agent readiness checks) via daemon.
- `vz-g4ea.4.2.3`: design and implement Linux base definition management APIs.
- `vz-g4ea.4.2.4`: design and implement Linux patch workflow and receipts.
