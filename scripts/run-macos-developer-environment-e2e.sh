#!/bin/bash
# EXPLICIT FAILING STUB for the `native-macos` lane of the vz 0.4 aggregate gate.
# The lane is not implemented. This stub never provisions anything: it writes a
# schema-valid lane-result.json with outcome=failed, failure.reason=not_implemented
# into --evidence-dir and exits 3, so every scenario assigned to the lane is
# reported MISSING by the aggregate validator. There is no skipped state.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec uv run --no-project --python /usr/bin/python3 \
  --with-requirements "$script_dir/helpers/gate-requirements.txt" \
  python -B "$script_dir/helpers/vz04_lanes.py" stub --lane native-macos --entry-point "scripts/run-macos-developer-environment-e2e.sh" "$@"
