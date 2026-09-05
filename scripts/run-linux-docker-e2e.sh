#!/bin/bash
# Explicit host-client gate. The full release contract is never a subset alias.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec /usr/bin/python3 "$script_dir/helpers/linux_docker_e2e.py" "$@"
