#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

failures=0

report_failure() {
  echo "guardrail failure: $1" >&2
  failures=$((failures + 1))
}

check_contains() {
  local pattern="$1"
  local file="$2"
  local description="$3"
  if ! rg -q --no-heading --color=never "$pattern" "$file"; then
    report_failure "$description ($file)"
  fi
}

check_no_match() {
  local pattern="$1"
  local description="$2"
  shift 2

  local output
  if output=$(rg -n --no-heading --color=never "$pattern" "$@" 2>/dev/null); then
    report_failure "$description"$'\n'"$output"
  fi
}

runtime_cli_control_plane_files=(
  "crates/vz-cli/src/commands/build_mgmt.rs"
  "crates/vz-cli/src/commands/checkpoint.rs"
  "crates/vz-cli/src/commands/execution.rs"
  "crates/vz-cli/src/commands/file.rs"
  "crates/vz-cli/src/commands/lease.rs"
  "crates/vz-cli/src/commands/sandbox.rs"
  "crates/vz-cli/src/commands/stack/commands.rs"
)

runtime_cli_files=(
  "${runtime_cli_control_plane_files[@]}"
  "crates/vz-cli/src/commands/image.rs"
)

for file in "${runtime_cli_control_plane_files[@]}"; do
  check_contains "connect_control_plane_for_state_db" "$file" \
    "runtime CLI command must route through daemon client wiring"
done

check_contains "connect_image_daemon" \
  "crates/vz-cli/src/commands/image.rs" \
  "runtime image CLI command must route through daemon client wiring"

check_no_match "StateStore::open\\(|rusqlite::|Connection::open\\(" \
  "runtime CLI command files must not open sqlite directly" \
  "${runtime_cli_files[@]}"

check_no_match "StateStore::open\\(" \
  "vz-api production surfaces must not open sqlite directly" \
  "crates/vz-api/src/lib.rs" \
  "crates/vz-api/src/daemon_bridge.rs"

check_no_match "connect_api_http_for_state_db|falls back to daemon-grpc" \
  "api-http transport daemon fallback shim must not exist" \
  "crates/vz-cli/src/commands/runtime_daemon.rs"

check_contains "api-http transport cannot use direct daemon gRPC connector" \
  "crates/vz-cli/src/commands/runtime_daemon.rs" \
  "api-http transport selector must fail closed for direct daemon connector use"

check_contains "legacy local-runtime path removed in daemon-only mode" \
  "crates/vz-cli/src/commands/oci.rs" \
  "legacy oci mutation commands must remain fail-closed in daemon mode"

check_contains "super::build::run\\(\\*build_args\\)\\.await" \
  "crates/vz-cli/src/commands/image.rs" \
  "vz image build command must dispatch to build command implementation"

coverage_output=""
if ! coverage_output="$("$ROOT/scripts/check-runtime-v2-rpc-test-coverage.sh" 2>&1)"; then
  report_failure "$coverage_output"
fi

if [[ "$failures" -gt 0 ]]; then
  echo "daemon-only guardrails failed ($failures issue(s))" >&2
  exit 1
fi

if [[ -n "$coverage_output" ]]; then
  echo "$coverage_output"
fi
echo "daemon-only guardrails passed"
