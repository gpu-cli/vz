#!/usr/bin/env bash
# Run high-level vz CLI + API E2E against local Linux runtime daemon.
#
# This harness is intended to run on a real Linux VM host. It validates that
# higher-level `vz` UX surfaces operate correctly over daemon-owned runtime
# orchestration by exercising API + CLI lifecycle calls in one flow.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PROFILE="debug"
OUTPUT_ROOT="$REPO_ROOT/.artifacts/vz-linux-vm-e2e"
WORKSPACE="${VZ_TEST_BTRFS_WORKSPACE:-}"
API_BIND="${VZ_LINUX_VM_E2E_API_BIND:-127.0.0.1:18181}"

usage() {
    cat <<'USAGE'
run-vz-linux-vm-e2e.sh

Run high-level vz CLI/API happy-path validation on Linux against daemon runtime.

Options:
  --workspace <path>          Workspace root (default: $VZ_TEST_BTRFS_WORKSPACE)
  --profile <debug|release>   Cargo profile (default: debug)
  --output-dir <path>         Artifact root (default: .artifacts/vz-linux-vm-e2e)
  --api-bind <host:port>      API bind address (default: 127.0.0.1:18181)
  -h, --help                  Show help
USAGE
}

err() {
    echo "error: $*" >&2
    exit 1
}

wait_for_http() {
    local url="$1"
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        if curl -fsS "$url" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workspace)
            WORKSPACE="${2:-}"
            shift 2
            ;;
        --profile)
            PROFILE="${2:-}"
            shift 2
            ;;
        --output-dir)
            OUTPUT_ROOT="${2:-}"
            shift 2
            ;;
        --api-bind)
            API_BIND="${2:-}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            err "unknown argument '$1'"
            ;;
    esac
done

[[ "$(uname -s)" == "Linux" ]] || err "this harness must run on Linux"
[[ "$PROFILE" == "debug" || "$PROFILE" == "release" ]] || err "--profile must be debug|release"
[[ -n "$WORKSPACE" ]] || err "--workspace (or VZ_TEST_BTRFS_WORKSPACE) is required"
[[ -d "$WORKSPACE" ]] || err "workspace does not exist: $WORKSPACE"
command -v curl >/dev/null 2>&1 || err "curl not found in PATH"
command -v cargo >/dev/null 2>&1 || err "cargo not found in PATH"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="$OUTPUT_ROOT/$timestamp"
mkdir -p "$RUN_DIR"
ln -sfn "$timestamp" "$OUTPUT_ROOT/latest"

STATE_ROOT="$WORKSPACE/.vz-e2e/$timestamp"
STATE_DB="$STATE_ROOT/stack-state.db"
RUNTIME_DIR="$STATE_ROOT/.vz-runtime"
SOCKET_PATH="$RUNTIME_DIR/runtimed.sock"
HOME_DIR="$STATE_ROOT/home"
PROJECT_DIR="$STATE_ROOT/project"
mkdir -p "$STATE_ROOT" "$RUNTIME_DIR" "$HOME_DIR/.vz" "$PROJECT_DIR"

cp "$REPO_ROOT/config/vz-space.json.example" "$PROJECT_DIR/vz.json"

BUILD_ARGS=()
if [[ "$PROFILE" == "release" ]]; then
    BUILD_ARGS+=(--release)
fi

{
    echo "timestamp_utc=$timestamp"
    echo "profile=$PROFILE"
    echo "workspace=$WORKSPACE"
    echo "api_bind=$API_BIND"
    echo "state_db=$STATE_DB"
    echo "runtime_dir=$RUNTIME_DIR"
} > "$RUN_DIR/run-info.txt"

echo "==> building binaries"
(
    cd "$REPO_ROOT/crates"
    cargo build "${BUILD_ARGS[@]}" -p vz-runtimed -p vz-api -p vz-cli
) >"$RUN_DIR/build.log" 2>&1

TARGET_DIR="$REPO_ROOT/crates/target/$PROFILE"
BIN_RUNTIMED="$TARGET_DIR/vz-runtimed"
BIN_API="$TARGET_DIR/vz-api"
BIN_VZ="$TARGET_DIR/vz"
[[ -x "$BIN_RUNTIMED" ]] || err "missing binary: $BIN_RUNTIMED"
[[ -x "$BIN_API" ]] || err "missing binary: $BIN_API"
[[ -x "$BIN_VZ" ]] || err "missing binary: $BIN_VZ"

cleanup() {
    set +e
    if [[ -n "${API_PID:-}" ]]; then
        kill "$API_PID" >/dev/null 2>&1 || true
        wait "$API_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "${RUNTIMED_PID:-}" ]]; then
        kill "$RUNTIMED_PID" >/dev/null 2>&1 || true
        wait "$RUNTIMED_PID" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

echo "==> starting vz-runtimed"
"$BIN_RUNTIMED" \
    --state_store_path "$STATE_DB" \
    --runtime_data_dir "$RUNTIME_DIR" \
    --socket_path "$SOCKET_PATH" \
    >"$RUN_DIR/runtimed.log" 2>&1 &
RUNTIMED_PID=$!

echo "==> starting vz-api"
"$BIN_API" \
    --bind "$API_BIND" \
    --state_store_path "$STATE_DB" \
    --daemon_socket_path "$SOCKET_PATH" \
    --daemon_runtime_data_dir "$RUNTIME_DIR" \
    --daemon_auto_spawn false \
    >"$RUN_DIR/api.log" 2>&1 &
API_PID=$!

API_BASE_URL="http://$API_BIND"
wait_for_http "$API_BASE_URL/v1/capabilities" || err "api failed readiness check"

SANDBOX_ID="vz-e2e-${timestamp,,}"
CREATE_PAYLOAD_FILE="$RUN_DIR/create-sandbox.json"
cat > "$CREATE_PAYLOAD_FILE" <<EOF
{
  "project_dir": "$PROJECT_DIR",
  "stack_name": "$SANDBOX_ID",
  "cpus": 2,
  "memory_mb": 1024,
  "labels": {
    "vz.project_dir": "$PROJECT_DIR",
    "source": "vz-linux-vm-e2e"
  }
}
EOF

echo "==> creating sandbox via API"
curl -fsS \
    -H 'content-type: application/json' \
    --data @"$CREATE_PAYLOAD_FILE" \
    "$API_BASE_URL/v1/sandboxes" \
    > "$RUN_DIR/api-create-response.json"

echo "==> validating via vz CLI (api-http transport)"
VZ_CONTROL_PLANE_TRANSPORT=api-http \
VZ_RUNTIME_API_BASE_URL="$API_BASE_URL" \
VZ_RUNTIME_DAEMON_AUTOSTART=0 \
HOME="$HOME_DIR" \
"$BIN_VZ" ls --state-db "$STATE_DB" --json > "$RUN_DIR/vz-ls.json"

grep -q "\"sandbox_id\": \"$SANDBOX_ID\"" "$RUN_DIR/vz-ls.json" || err "vz ls output missing created sandbox"

VZ_CONTROL_PLANE_TRANSPORT=api-http \
VZ_RUNTIME_API_BASE_URL="$API_BASE_URL" \
VZ_RUNTIME_DAEMON_AUTOSTART=0 \
HOME="$HOME_DIR" \
"$BIN_VZ" inspect "$SANDBOX_ID" --state-db "$STATE_DB" > "$RUN_DIR/vz-inspect.json"

grep -q "\"sandbox_id\": \"$SANDBOX_ID\"" "$RUN_DIR/vz-inspect.json" || err "vz inspect output missing expected sandbox_id"

echo "==> terminating via vz CLI"
VZ_CONTROL_PLANE_TRANSPORT=api-http \
VZ_RUNTIME_API_BASE_URL="$API_BASE_URL" \
VZ_RUNTIME_DAEMON_AUTOSTART=0 \
HOME="$HOME_DIR" \
"$BIN_VZ" rm "$SANDBOX_ID" --state-db "$STATE_DB" > "$RUN_DIR/vz-rm.log"

curl -fsS "$API_BASE_URL/v1/sandboxes/$SANDBOX_ID" > "$RUN_DIR/api-sandbox-final.json"
grep -q '"state": "terminated"' "$RUN_DIR/api-sandbox-final.json" || err "final sandbox state is not terminated"

{
    echo "passed=vz_cli_api_daemon_linux_happy_path"
    echo "failed=none"
} > "$RUN_DIR/summary.txt"

echo "==> summary"
cat "$RUN_DIR/summary.txt"
echo "==> artifacts: $RUN_DIR"
