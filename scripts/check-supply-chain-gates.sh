#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: check-supply-chain-gates.sh --dist-dir <dir> --artifact <name> [options]

Required:
  --dist-dir <dir>      Distribution directory containing release artifacts.
  --artifact <name>     Artifact basename to validate (for example: vz).

Options:
  --max-high <n>        Maximum allowed HIGH vulnerabilities (default: 0).
  --allow-critical      Allow CRITICAL vulnerabilities (default: false).

Expected files:
  <dist>/<artifact>
  <dist>/<artifact>.sbom.json
  <dist>/<artifact>.sig
  <dist>/<artifact>.intoto.jsonl
  <dist>/<artifact>.vulns.json
EOF
}

DIST_DIR=""
ARTIFACT=""
MAX_HIGH=0
ALLOW_CRITICAL=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dist-dir) DIST_DIR="${2:-}"; shift 2 ;;
    --artifact) ARTIFACT="${2:-}"; shift 2 ;;
    --max-high) MAX_HIGH="${2:-}"; shift 2 ;;
    --allow-critical) ALLOW_CRITICAL=1; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$DIST_DIR" || -z "$ARTIFACT" ]]; then
  usage
  exit 2
fi

BASE="$DIST_DIR/$ARTIFACT"
SBOM="$BASE.sbom.json"
SIG="$BASE.sig"
PROV="$BASE.intoto.jsonl"
VULNS="$BASE.vulns.json"

for path in "$BASE" "$SBOM" "$SIG" "$PROV" "$VULNS"; do
  if [[ ! -f "$path" ]]; then
    echo "gate failed: missing required artifact $path" >&2
    exit 1
  fi
done

python3 - <<'PY' "$SBOM" "$PROV" "$VULNS" "$MAX_HIGH" "$ALLOW_CRITICAL"
import json
import sys
from pathlib import Path

sbom_path = Path(sys.argv[1])
prov_path = Path(sys.argv[2])
vuln_path = Path(sys.argv[3])
max_high = int(sys.argv[4])
allow_critical = bool(int(sys.argv[5]))

def load_json(path: Path):
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        print(f"gate failed: invalid json in {path}: {exc}", file=sys.stderr)
        sys.exit(1)

load_json(sbom_path)

try:
    prov_path.read_text()
except Exception as exc:
    print(f"gate failed: unable to read provenance {prov_path}: {exc}", file=sys.stderr)
    sys.exit(1)

vulns = load_json(vuln_path)
items = vulns.get("vulnerabilities", [])
if not isinstance(items, list):
    print("gate failed: vulnerabilities field must be a list", file=sys.stderr)
    sys.exit(1)

critical = 0
high = 0
for item in items:
    sev = str(item.get("severity", "")).lower()
    if sev == "critical":
        critical += 1
    elif sev == "high":
        high += 1

if critical > 0 and not allow_critical:
    print(f"gate failed: critical vulnerabilities={critical}", file=sys.stderr)
    sys.exit(1)
if high > max_high:
    print(f"gate failed: high vulnerabilities={high} exceeds threshold={max_high}", file=sys.stderr)
    sys.exit(1)

print(
    f"supply-chain gates passed: critical={critical}, high={high}, max_high={max_high}",
    file=sys.stdout,
)
PY
