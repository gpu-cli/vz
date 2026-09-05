#!/bin/bash
# Fresh, narrowly scoped DEV public Delete proof. Never attaches existing VMs.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec /usr/bin/python3 "$script_dir/helpers/installed_delete_e2e.py" "$@"
