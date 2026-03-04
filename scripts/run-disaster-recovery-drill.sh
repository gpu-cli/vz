#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run-disaster-recovery-drill.sh --backup-cmd "<cmd>" --restore-cmd "<cmd>" --verify-cmd "<cmd>" [options]

Required:
  --backup-cmd "<cmd>"     Command that creates a recoverable backup.
  --restore-cmd "<cmd>"    Command that restores from backup.
  --verify-cmd "<cmd>"     Command that verifies post-restore service readiness.

Options:
  --verify-timeout-secs <n>  Timeout for verify command retries (default: 120)
  --verify-interval-secs <n> Retry interval for verify command (default: 2)
  --rto-target-secs <n>      RTO threshold (default: 300)
  --rpo-target-secs <n>      RPO threshold (default: 60)
  --report <path>            Report JSON path (default: .artifacts/dr-drill/latest.json)

The script exits non-zero when:
  - backup/restore/verify command fails
  - measured RTO exceeds target
  - measured RPO exceeds target
EOF
}

BACKUP_CMD=""
RESTORE_CMD=""
VERIFY_CMD=""
VERIFY_TIMEOUT_SECS=120
VERIFY_INTERVAL_SECS=2
RTO_TARGET_SECS=300
RPO_TARGET_SECS=60
REPORT_PATH=".artifacts/dr-drill/latest.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --backup-cmd) BACKUP_CMD="${2:-}"; shift 2 ;;
    --restore-cmd) RESTORE_CMD="${2:-}"; shift 2 ;;
    --verify-cmd) VERIFY_CMD="${2:-}"; shift 2 ;;
    --verify-timeout-secs) VERIFY_TIMEOUT_SECS="${2:-}"; shift 2 ;;
    --verify-interval-secs) VERIFY_INTERVAL_SECS="${2:-}"; shift 2 ;;
    --rto-target-secs) RTO_TARGET_SECS="${2:-}"; shift 2 ;;
    --rpo-target-secs) RPO_TARGET_SECS="${2:-}"; shift 2 ;;
    --report) REPORT_PATH="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$BACKUP_CMD" || -z "$RESTORE_CMD" || -z "$VERIFY_CMD" ]]; then
  usage
  exit 2
fi

BACKUP_START_UNIX="$(date +%s)"
bash -lc "$BACKUP_CMD"
BACKUP_END_UNIX="$(date +%s)"

RESTORE_START_UNIX="$(date +%s)"
bash -lc "$RESTORE_CMD"

VERIFY_DEADLINE=$(( RESTORE_START_UNIX + VERIFY_TIMEOUT_SECS ))
VERIFY_OK=0
VERIFY_END_UNIX=0
while (( "$(date +%s)" <= VERIFY_DEADLINE )); do
  if bash -lc "$VERIFY_CMD" >/dev/null 2>&1; then
    VERIFY_OK=1
    VERIFY_END_UNIX="$(date +%s)"
    break
  fi
  sleep "$VERIFY_INTERVAL_SECS"
done

if (( VERIFY_OK == 0 )); then
  VERIFY_END_UNIX="$(date +%s)"
fi

RTO_SECS=$(( VERIFY_END_UNIX - RESTORE_START_UNIX ))
RPO_SECS=$(( RESTORE_START_UNIX - BACKUP_END_UNIX ))

PASSED=true
FAIL_REASON=""

if (( VERIFY_OK == 0 )); then
  PASSED=false
  FAIL_REASON="verify_timeout"
elif (( RTO_SECS > RTO_TARGET_SECS )); then
  PASSED=false
  FAIL_REASON="rto_exceeded"
elif (( RPO_SECS > RPO_TARGET_SECS )); then
  PASSED=false
  FAIL_REASON="rpo_exceeded"
fi

mkdir -p "$(dirname "$REPORT_PATH")"
cat >"$REPORT_PATH" <<EOF
{
  "backup_start_unix": $BACKUP_START_UNIX,
  "backup_end_unix": $BACKUP_END_UNIX,
  "restore_start_unix": $RESTORE_START_UNIX,
  "verify_end_unix": $VERIFY_END_UNIX,
  "rto_secs": $RTO_SECS,
  "rpo_secs": $RPO_SECS,
  "rto_target_secs": $RTO_TARGET_SECS,
  "rpo_target_secs": $RPO_TARGET_SECS,
  "verify_timeout_secs": $VERIFY_TIMEOUT_SECS,
  "verify_ok": $( [[ $VERIFY_OK -eq 1 ]] && echo "true" || echo "false" ),
  "passed": $PASSED,
  "fail_reason": $(printf '%s' "$FAIL_REASON" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')
}
EOF

if [[ "$PASSED" != "true" ]]; then
  echo "drill failed: $FAIL_REASON (rto=${RTO_SECS}s, rpo=${RPO_SECS}s, report=$REPORT_PATH)" >&2
  exit 1
fi

echo "drill passed (rto=${RTO_SECS}s, rpo=${RPO_SECS}s, report=$REPORT_PATH)"
