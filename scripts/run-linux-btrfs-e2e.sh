#!/usr/bin/env bash
# Run Linux+btrfs release-gate tests for runtimed portability paths.
#
# This harness executes ignored tests that require:
# - Linux host
# - btrfs-provisioned workspace path
#
# Usage examples:
#   VZ_TEST_BTRFS_WORKSPACE=/mnt/vz-btrfs ./scripts/run-linux-btrfs-e2e.sh
#   ./scripts/run-linux-btrfs-e2e.sh --workspace /mnt/vz-btrfs --profile release
#   ./scripts/run-linux-btrfs-e2e.sh --keep-going

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PROFILE="debug"
KEEP_GOING=false
OUTPUT_ROOT="$REPO_ROOT/.artifacts/linux-btrfs-e2e"
WORKSPACE="${VZ_TEST_BTRFS_WORKSPACE:-}"
RUN_ARGS=("--ignored" "--nocapture" "--test-threads=1")
TESTS=(
    "spaces_btrfs_checkpoint_restore_and_fork_use_real_subvolumes"
    "checkpoint_export_import_round_trip_preserves_workspace_snapshot"
    "space_cache_export_import_round_trip_preserves_payload"
)

usage() {
    cat <<'USAGE'
run-linux-btrfs-e2e.sh

Execute Linux+btrfs ignored runtimed tests and capture artifacts.

Options:
  --workspace <path>          btrfs workspace root (default: $VZ_TEST_BTRFS_WORKSPACE)
  --profile <debug|release>   Cargo profile (default: debug)
  --output-dir <path>         Artifact root (default: .artifacts/linux-btrfs-e2e)
  --keep-going                Continue after failures
  -h, --help                  Show help

Environment:
  VZ_TEST_BTRFS_WORKSPACE     Required if --workspace is not provided.
USAGE
}

err() {
    echo "error: $*" >&2
    exit 1
}

ensure_btrfs_workspace() {
    local path="$1"
    [[ -n "$path" ]] || err "VZ_TEST_BTRFS_WORKSPACE (or --workspace) is required"
    [[ -d "$path" ]] || err "workspace path is not a directory: $path"

    local fs_type=""
    if command -v stat >/dev/null 2>&1; then
        fs_type="$(stat -f -c %T "$path" 2>/dev/null || true)"
    fi
    if [[ "${fs_type,,}" == "unknown" ]]; then
        fs_type=""
    fi
    if [[ -z "$fs_type" ]] && command -v findmnt >/dev/null 2>&1; then
        fs_type="$(findmnt -n -M "$path" -o FSTYPE 2>/dev/null || true)"
    fi
    if [[ -z "$fs_type" ]] && command -v findmnt >/dev/null 2>&1; then
        fs_type="$(findmnt -n -T "$path" -o FSTYPE 2>/dev/null || true)"
    fi

    [[ "$fs_type" == "btrfs" ]] || err "workspace path must be on btrfs (detected: ${fs_type:-unknown})"
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
        --keep-going)
            KEEP_GOING=true
            shift
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

[[ "$PROFILE" == "debug" || "$PROFILE" == "release" ]] || err "--profile must be debug|release"
[[ "$(uname -s)" == "Linux" ]] || err "this harness must run on Linux"
command -v btrfs >/dev/null 2>&1 || err "btrfs command not found in PATH"

ensure_btrfs_workspace "$WORKSPACE"
export VZ_TEST_BTRFS_WORKSPACE="$WORKSPACE"
: "${CARGO_TARGET_DIR:=$WORKSPACE/.cargo-target}"
export CARGO_TARGET_DIR
mkdir -p "$CARGO_TARGET_DIR"
if command -v chattr >/dev/null 2>&1; then
    chattr +C "$CARGO_TARGET_DIR" >/dev/null 2>&1 || true
fi
echo "==> disk usage preflight"
df -h "$WORKSPACE" "$CARGO_TARGET_DIR" 2>/dev/null || true
if command -v btrfs >/dev/null 2>&1; then
    btrfs filesystem usage -T "$WORKSPACE" 2>/dev/null || true
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="$OUTPUT_ROOT/$timestamp"
mkdir -p "$RUN_DIR"
ln -sfn "$timestamp" "$OUTPUT_ROOT/latest"

BUILD_ARGS=()
if [[ "$PROFILE" == "release" ]]; then
    BUILD_ARGS+=(--release)
fi

echo "==> output directory: $RUN_DIR"
{
    echo "timestamp_utc=$timestamp"
    echo "host=$(hostname)"
    echo "profile=$PROFILE"
    echo "workspace=$WORKSPACE"
    echo "tests=${TESTS[*]}"
} > "$RUN_DIR/run-info.txt"

FAILED=()
PASSED=()

for test_name in "${TESTS[@]}"; do
    log_file="$RUN_DIR/${test_name}.log"
    cmd=(cargo test -p vz-runtimed "${BUILD_ARGS[@]}" "$test_name" -- "${RUN_ARGS[@]}")
    echo "running [$test_name]: ${cmd[*]}"

    set +e
    (
        cd "$REPO_ROOT/crates"
        "${cmd[@]}"
    ) 2>&1 | tee "$log_file"
    status=${PIPESTATUS[0]}
    set -e

    if [[ $status -eq 0 ]] && grep -q "^running 0 tests$" "$log_file"; then
        status=86
        echo "error: test filter '$test_name' executed zero tests" >&2
    fi

    if [[ $status -eq 0 ]]; then
        PASSED+=("$test_name")
    else
        FAILED+=("$test_name:$status")
        if [[ "$KEEP_GOING" != "true" ]]; then
            break
        fi
    fi
done

{
    echo "passed=${PASSED[*]:-none}"
    echo "failed=${FAILED[*]:-none}"
} > "$RUN_DIR/summary.txt"

echo "==> summary"
echo "passed: ${PASSED[*]:-none}"
echo "failed: ${FAILED[*]:-none}"

if [[ ${#FAILED[@]} -gt 0 ]]; then
    exit 1
fi
