#!/bin/bash
# Read-only validator for an aggregate evidence root; reproduces the gate's
# verdict independently (GOAL-0.4.0.md "Evidence and mechanical verdict").
#
#   scripts/validate-vz-0.4-evidence.sh .artifacts/vz-0.4-e2e/<run-id>/manifest.json
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ $# -ne 1 ]]; then
  printf 'usage: %s <aggregate manifest.json>\n' "$0" >&2
  exit 2
fi
manifest="$1"
case "$manifest" in
  /*) ;;
  *) manifest="$PWD/$manifest" ;;
esac
exec uv run --no-project --python /usr/bin/python3 \
  --with-requirements "$script_dir/helpers/gate-requirements.txt" \
  python -B "$script_dir/helpers/vz04_validate.py" "$manifest"
