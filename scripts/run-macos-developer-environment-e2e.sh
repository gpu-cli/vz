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
# Thin wrapper: the hash-pinned jsonschema from gate-requirements.txt is needed
# because the lane admits the release directory through vz04_candidate and
# self-validates its lane-result before writing it.
#
# Exit codes: 0 passed, 1 failed, 2 input rejected, 3 not_implemented.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec uv run --no-project --python /usr/bin/python3 \
  --with-requirements "$script_dir/helpers/gate-requirements.txt" \
  python -B "$script_dir/helpers/native_macos_lane_result.py" "$@"
