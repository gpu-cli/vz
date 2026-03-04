# Incident Playbook

## Roles and Ownership

- Incident Commander (IC): runtime on-call engineer.
- Communications Lead: API/runtime owner delegate.
- Scribe: records timeline, decisions, and follow-ups.

## Severity Model

- Sev-1: broad outage or data-loss risk; immediate paging.
- Sev-2: major degradation with user impact; urgent triage.
- Sev-3: localized/non-critical degradation; scheduled remediation.

## Response Flow

1. Declare incident in team channel with severity and owner.
2. Assign IC, Comms Lead, and Scribe.
3. Start timeline:
  - first alert timestamp
  - impact scope
  - hypotheses and actions
4. Execute matching runbook:
  - `runbook-daemon-outage.md`
  - `runbook-checkpoint-portability-failures.md`
5. Communicate updates every 15 minutes (Sev-1/2).
6. Confirm recovery exit criteria.
7. Close incident with summary and follow-up actions.

## Rollback Guidance

- Control-plane rollback:
  - restart daemon/API with last-known-good config and binary.
- Feature rollback:
  - disable newly introduced surfaces via CLI/API routing gate where applicable.
- Data-path rollback:
  - pause mutation traffic before DB/layout rollback.

## Post-Incident Requirements

1. Publish timeline and root-cause summary.
2. File beads for all remediations with owners and due dates.
3. Track closure of follow-ups in weekly reliability review.
