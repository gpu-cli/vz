#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: check-security-assessment-gate.sh --assessment-dir <dir> [--allow-open-high]

Required files in assessment dir:
  scope.md
  report.md
  findings.json
  regression-tests.md

findings.json format:
{
  "findings": [
    {"id":"SEC-1","severity":"critical|high|...","status":"open|mitigated|accepted","owner":"...","evidence":"..."}
  ]
}
EOF
}

ASSESSMENT_DIR=""
ALLOW_OPEN_HIGH=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --assessment-dir) ASSESSMENT_DIR="${2:-}"; shift 2 ;;
    --allow-open-high) ALLOW_OPEN_HIGH=1; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$ASSESSMENT_DIR" ]]; then
  usage
  exit 2
fi

for f in scope.md report.md findings.json regression-tests.md; do
  if [[ ! -f "$ASSESSMENT_DIR/$f" ]]; then
    echo "gate failed: missing $ASSESSMENT_DIR/$f" >&2
    exit 1
  fi
done

python3 - <<'PY' "$ASSESSMENT_DIR/findings.json" "$ALLOW_OPEN_HIGH"
import json
import sys
from pathlib import Path

findings_path = Path(sys.argv[1])
allow_open_high = bool(int(sys.argv[2]))

try:
    doc = json.loads(findings_path.read_text())
except Exception as exc:
    print(f"gate failed: invalid findings.json: {exc}", file=sys.stderr)
    sys.exit(1)

items = doc.get("findings", [])
if not isinstance(items, list):
    print("gate failed: findings must be an array", file=sys.stderr)
    sys.exit(1)

open_critical = 0
open_high = 0
for item in items:
    severity = str(item.get("severity", "")).lower()
    status = str(item.get("status", "")).lower()
    if status != "open":
        continue
    if severity == "critical":
        open_critical += 1
    elif severity == "high":
        open_high += 1

if open_critical > 0:
    print(f"gate failed: open critical findings={open_critical}", file=sys.stderr)
    sys.exit(1)

if open_high > 0 and not allow_open_high:
    print(f"gate failed: open high findings={open_high}", file=sys.stderr)
    sys.exit(1)

print(
    f"security assessment gate passed: open_critical={open_critical}, open_high={open_high}",
    file=sys.stdout,
)
PY
