#!/bin/bash
# Single aggregate vz 0.4 release-gate entry point (GOAL-0.4.0.md "Release
# candidate and entry point"). Thin wrapper: the orchestrator is
# scripts/helpers/vz04_gate.py, run with hash-pinned isolated Python deps via
# uv (no global/user-site install, system Python untouched).
#
#   scripts/run-vz-0.4-release-gate.sh --suite all --release-dir <dir> --run-id <id> \
#     --docker <path> --compose-plugin <path> --buildx-plugin <path> [--evidence-root <dir>] \
#     [--state-root <dir>] [--linux-docker-context <name>] [--sleep-wake-ack-file <path>]
#
# Only --suite all is accepted. --dry-lanes is DEV ONLY (see docs/vz-0.4-release-gate.md):
# lanes are replaced by their not_implemented results and the verdict can never be PASS.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec uv run --no-project --python /usr/bin/python3 \
  --with-requirements "$script_dir/helpers/gate-requirements.txt" \
  python -B "$script_dir/helpers/vz04_gate.py" "$@"
