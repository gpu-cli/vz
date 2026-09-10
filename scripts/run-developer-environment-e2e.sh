#!/bin/bash
# `topology` lane of the vz 0.4 aggregate gate.
#
# The lane reads a FROZEN tree, not the working checkout. `frozen_tree.py`
# copies this checkout into a private git worktree under /private/tmp, runs
# `developer_environment_e2e.py` from there, and removes it at the end.
# `--evidence-dir` and `--state-root` are argv and stay wherever the caller put
# them, outside that tree.
#
# Two things this buys, and the second is the one that matters here. The
# checkout is free to be merged while a 45-minute run executes -- which it will
# be, because the lane's sub-checks are developed in parallel branches. And the
# `source_tree` field of the lane result becomes true: `vz04_lanes.base_result`
# records `frozen_tree.record(repo_root)`, so an unfrozen lane names whatever
# the checkout happened to be when the result was WRITTEN rather than the tree
# the run actually executed against. Evidence that names the wrong tree is worse
# than evidence that names none.
#
# Thin wrapper: the hash-pinned jsonschema from gate-requirements.txt is needed
# because the lane admits the release directory through vz04_candidate and
# self-validates its lane-result before writing it.
#
# Exit codes: 0 passed, 1 failed, 2 input rejected, 3 not_implemented.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec uv run --no-project --python /usr/bin/python3 \
  --with-requirements "$script_dir/helpers/gate-requirements.txt" \
  python -B "$script_dir/helpers/frozen_tree.py" \
  --entry scripts/helpers/developer_environment_e2e.py -- "$@"
