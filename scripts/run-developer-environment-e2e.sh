#!/bin/bash
# `topology` lane of the vz 0.4 aggregate gate (skeleton; criteria 21 and 15
# static sub-checks are physical, everything else is an honest not_implemented).
# Thin wrapper: the hash-pinned jsonschema from gate-requirements.txt is needed
# because the lane admits the release directory through vz04_candidate and
# self-validates its lane-result before writing it.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec uv run --no-project --python /usr/bin/python3 \
  --with-requirements "$script_dir/helpers/gate-requirements.txt" \
  python -B "$script_dir/helpers/developer_environment_e2e.py" "$@"
