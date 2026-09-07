#!/bin/bash
# Explicit host entry for the DEV registry slice. The isolated Python
# dependencies (cryptography/bcrypt, hash-pinned in registry-requirements.txt)
# are provided by uv for this process only: no global or user-site install and
# no change to system Python. The suite is fixed here; passing --suite is an
# error rather than a silent override, and --suite all remains rejected.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for arg in "$@"; do
  case "$arg" in
    --suite|--suite=*)
      printf 'error: --suite is fixed to registry by this wrapper\n' >&2
      exit 2
      ;;
  esac
done
exec uv run --no-project --python /usr/bin/python3 \
  --with-requirements "$script_dir/helpers/registry-requirements.txt" \
  python -B "$script_dir/helpers/linux_docker_e2e.py" --suite registry "$@"
