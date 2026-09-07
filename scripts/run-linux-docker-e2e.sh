#!/bin/bash
# Explicit host-client gate. The full release contract is never a subset alias.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Isolated, hash-pinned dependencies; nothing is installed into system Python.
# Both sets are required because a composed `--suite all` includes the registry
# suite (cryptography, bcrypt) alongside the gate's schema validation.
exec uv run --no-project --python /usr/bin/python3 \
  --with-requirements "$script_dir/helpers/registry-requirements.txt" \
  --with-requirements "$script_dir/helpers/gate-requirements.txt" \
  python -B "$script_dir/helpers/linux_docker_e2e.py" "$@"
