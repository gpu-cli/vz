#!/usr/bin/env bash
# Compatibility wrapper for the unified sandbox VM E2E harness.
#
# Equivalent to:
#   ./scripts/run-sandbox-vm-e2e.sh --suite buildkit [args...]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/run-sandbox-vm-e2e.sh" --suite buildkit "$@"
