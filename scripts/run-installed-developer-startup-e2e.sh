#!/bin/bash
# Separate DEV installed-public-CLI proof; never invokes the older lifecycle test.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec /usr/bin/python3 "$script_dir/helpers/installed_developer_startup.py" "$@"
