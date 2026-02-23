#!/usr/bin/env bash
# Build, sign, and run the ignored BuildKit end-to-end test binary.
#
# This signs the integration test executable itself with the
# virtualization entitlement required by Virtualization.framework.
#
# Usage:
#   ./scripts/run-buildkit-e2e.sh
#   ./scripts/run-buildkit-e2e.sh --profile release
#   ./scripts/run-buildkit-e2e.sh -- --ignored --nocapture

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENTITLEMENTS="$REPO_ROOT/entitlements/vz-cli.entitlements.plist"

PROFILE="debug"
EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile)
            PROFILE="${2:-}"
            shift 2
            ;;
        --)
            shift
            EXTRA_ARGS=("$@")
            break
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

if [[ "$PROFILE" != "debug" && "$PROFILE" != "release" ]]; then
    echo "error: --profile must be one of: debug, release" >&2
    exit 2
fi

if [[ ! -f "$ENTITLEMENTS" ]]; then
    echo "error: entitlements plist not found at $ENTITLEMENTS" >&2
    exit 1
fi

TEST_NAME="buildkit_e2e"
TARGET_DIR="$REPO_ROOT/crates/target/$PROFILE"

cd "$REPO_ROOT/crates"

BUILD_ARGS=()
if [[ "$PROFILE" == "release" ]]; then
    BUILD_ARGS+=(--release)
fi

echo "building test binary: $TEST_NAME ($PROFILE)"
cargo test -p vz-oci-macos "${BUILD_ARGS[@]}" --test "$TEST_NAME" --no-run

shopt -s nullglob
TEST_BIN=""
for candidate in "$TARGET_DIR"/deps/"$TEST_NAME"-*; do
    if [[ -f "$candidate" && -x "$candidate" ]]; then
        TEST_BIN="$candidate"
        break
    fi
done
shopt -u nullglob

if [[ -z "$TEST_BIN" ]]; then
    echo "error: unable to locate built test binary at $TARGET_DIR/deps/$TEST_NAME-*" >&2
    exit 1
fi

echo "signing: $TEST_BIN"
codesign --force --sign - --entitlements "$ENTITLEMENTS" "$TEST_BIN"
codesign --verify --verbose "$TEST_BIN"

RUN_ARGS=("--ignored" "--nocapture" "--test-threads=1")
if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
    RUN_ARGS=("${EXTRA_ARGS[@]}")
fi

echo "running: $TEST_BIN ${RUN_ARGS[*]}"
"$TEST_BIN" "${RUN_ARGS[@]}"
