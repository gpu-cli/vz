#!/usr/bin/env bash
# Run reproducible daemon-side space-cache warm/cold benchmark smoke and capture artifacts.
#
# Usage:
#   ./scripts/run-space-cache-benchmark.sh
#   ./scripts/run-space-cache-benchmark.sh --profile release
#   ./scripts/run-space-cache-benchmark.sh --output-dir .artifacts/space-cache-bench

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PROFILE="debug"
OUTPUT_ROOT="$REPO_ROOT/.artifacts/space-cache-bench"

usage() {
    cat <<'USAGE'
run-space-cache-benchmark.sh

Execute a deterministic warm/cold daemon cache benchmark-smoke test and save logs.

Options:
  --profile <debug|release>   Cargo profile (default: debug)
  --output-dir <path>         Artifact root (default: .artifacts/space-cache-bench)
  -h, --help                  Show help
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile)
            PROFILE="${2:-}"
            shift 2
            ;;
        --output-dir)
            OUTPUT_ROOT="${2:-}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown argument '$1'" >&2
            exit 1
            ;;
    esac
done

[[ "$PROFILE" == "debug" || "$PROFILE" == "release" ]] || {
    echo "error: --profile must be debug|release" >&2
    exit 1
}

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="$OUTPUT_ROOT/$timestamp"
mkdir -p "$RUN_DIR"
ln -sfn "$timestamp" "$OUTPUT_ROOT/latest"

BUILD_ARGS=()
if [[ "$PROFILE" == "release" ]]; then
    BUILD_ARGS+=(--release)
fi

TEST_NAME="grpc::tests::prepare_space_cache_cold_then_warm_hit_benchmark_smoke"
LOG_FILE="$RUN_DIR/prepare_space_cache_cold_then_warm_hit_benchmark_smoke.log"

echo "==> output directory: $RUN_DIR"
{
    echo "timestamp_utc=$timestamp"
    echo "host=$(hostname)"
    echo "profile=$PROFILE"
    echo "test=$TEST_NAME"
} > "$RUN_DIR/run-info.txt"

(
    cd "$REPO_ROOT/crates"
    cargo test -p vz-runtimed "${BUILD_ARGS[@]}" "$TEST_NAME" -- --exact --nocapture
) 2>&1 | tee "$LOG_FILE"

bench_line="$(grep -E '^BENCH space_cache_prepare ' "$LOG_FILE" | tail -n 1 || true)"
if [[ -z "$bench_line" ]]; then
    echo "error: benchmark marker line not found in $LOG_FILE" >&2
    exit 1
fi

echo "$bench_line" > "$RUN_DIR/summary.txt"
echo "==> summary"
cat "$RUN_DIR/summary.txt"
