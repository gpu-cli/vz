# Disaster Recovery Drills (RTO/RPO Gate)

This document defines automated backup/restore drills and RTO/RPO validation.

## Harness

Use:

```bash
scripts/run-disaster-recovery-drill.sh \
  --backup-cmd "<backup command>" \
  --restore-cmd "<restore command>" \
  --verify-cmd "<health check command>" \
  --rto-target-secs 300 \
  --rpo-target-secs 60 \
  --report .artifacts/dr-drill/latest.json
```

## Example (State DB Snapshot Drill)

```bash
scripts/run-disaster-recovery-drill.sh \
  --backup-cmd "mkdir -p .artifacts/dr-backup && cp .vz-runtime/stack-state.db .artifacts/dr-backup/stack-state.db.bak" \
  --restore-cmd "cp .artifacts/dr-backup/stack-state.db.bak .vz-runtime/stack-state.db" \
  --verify-cmd "test -f .vz-runtime/stack-state.db" \
  --rto-target-secs 120 \
  --rpo-target-secs 60 \
  --report .artifacts/dr-drill/state-db.json
```

## Metrics and Gate Rules

- RTO = `verify_end_unix - restore_start_unix`
- RPO = `restore_start_unix - backup_end_unix`

Gate fails when:

1. verify command does not succeed before timeout.
2. RTO exceeds `--rto-target-secs`.
3. RPO exceeds `--rpo-target-secs`.

## Evidence Retention

- Persist reports under `.artifacts/dr-drill/`.
- Include report JSON in release readiness review.
- Regressions block readiness sign-off until remediated.
