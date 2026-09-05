#!/bin/bash
# Fresh scoped DEV proof; never attaches existing VMs or retries missed windows.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec /usr/bin/python3 "$script_dir/helpers/installed_delete_disconnect_e2e.py" "$@"
