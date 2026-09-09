#!/bin/bash
# Explicit host-client gate. The full release contract is never a subset alias.
#
# The suite modules pin their own sources and re-verify them during the run, so
# the tree a run reads must not move under it. It reads a frozen one:
# `frozen_tree.py` copies this checkout into a private git worktree under
# /private/tmp, runs `linux_docker_e2e.py` from there, and removes it at the end.
# `--evidence-dir` and `--state-root` are argv and stay wherever the caller put
# them, outside that tree. The working checkout is free to be merged while a run
# executes; mutating the run's own frozen tree still aborts it.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Isolated, hash-pinned dependencies; nothing is installed into system Python.
# Both sets are required because a composed `--suite all` includes the registry
# suite (cryptography, bcrypt) alongside the gate's schema validation.
exec uv run --no-project --python /usr/bin/python3 \
  --with-requirements "$script_dir/helpers/registry-requirements.txt" \
  --with-requirements "$script_dir/helpers/gate-requirements.txt" \
  python -B "$script_dir/helpers/frozen_tree.py" \
  --entry scripts/helpers/linux_docker_e2e.py -- "$@"
