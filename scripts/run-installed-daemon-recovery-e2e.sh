#!/bin/bash
# Only fresh fixtures; production code, never this harness, reclaims dead sockets.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec /usr/bin/python3 "$script_dir/helpers/installed_daemon_recovery_e2e.py" "$@"
