#!/bin/bash
# `native-macos` lane of the vz 0.4 aggregate gate.
#
# Drives the real hardware harness `scripts/run-installed-native-macos-e2e.py
# --require-swift`, which builds and runs the Swift fixture inside a native
# macOS Machine, and translates what it left behind into a lane result for
# `gate.native.target_native_execution` (criterion 4).
#
# This lane never provisions a macOS template: `vz-macos-setup` downloads an
# Apple IPSW and takes an administrator authorisation, which is a maintainer
# step (planning/developer-environments/macos-local-setup.md). With no template
# registered in an installed machine-target catalog the lane reports
# failure.reason=prerequisite and its scenario FAIL. A zero exit from the
# harness is necessary but not sufficient for PASS; see the module docstring.
#
# The lane reads a FROZEN tree, not the working checkout. `frozen_tree.py`
# copies this checkout into a private git worktree under /private/tmp, runs
# `native_macos_lane_result.py` from there, and removes it at the end. The lane
# resolves the gate contract, the Swift fixture and the hardware harness from
# `REPO_ROOT`, so all three come from that one frozen tree and cannot disagree
# with each other or move mid-run. `--evidence-dir` and `--state-root` are argv
# and stay wherever the caller put them, outside it.
#
# It also makes the lane result's `source_tree` true: `vz04_lanes.base_result`
# records `frozen_tree.record(repo_root)`, so an unfrozen lane names whatever
# the checkout happened to be when the result was WRITTEN rather than the tree
# the run executed against.
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
  --entry scripts/helpers/native_macos_lane_result.py -- "$@"
