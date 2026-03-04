# Runtime Operations Package

This folder contains production operations guidance for runtime daemon/API incidents.

## Contents

- `runbook-daemon-outage.md`: daemon/API outage triage and recovery.
- `runbook-checkpoint-portability-failures.md`: checkpoint/cache portability failure handling.
- `incident-playbook.md`: ownership, communications, severity handling, rollback path.
- `game-day-log-2026-03-04.md`: executed game-day drills and follow-up tracking.
- `../disaster-recovery-drills.md`: automated RTO/RPO drill harness and gate policy.

## Scope

- Applies to daemon-owned runtime control plane (`vz-runtimed`, `vz-api`, `vz-cli`).
- Focuses on sandbox lifecycle, checkpoint portability, and control-plane reliability.
