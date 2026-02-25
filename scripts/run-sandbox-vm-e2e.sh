#!/usr/bin/env bash
# Build, sign, and run sandbox-focused VM end-to-end test suites.
#
# This harness executes ignored Rust integration tests that boot real VMs and
# therefore require the virtualization entitlement on the test executable.
#
# Usage examples:
#   ./scripts/run-sandbox-vm-e2e.sh
#   ./scripts/run-sandbox-vm-e2e.sh --suite runtime
#   ./scripts/run-sandbox-vm-e2e.sh --suite sandbox --profile release
#   ./scripts/run-sandbox-vm-e2e.sh --suite all --keep-going
#   ./scripts/run-sandbox-vm-e2e.sh -- --ignored --nocapture --exact smoke_pull_and_run_alpine

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENTITLEMENTS="$REPO_ROOT/entitlements/vz-cli.entitlements.plist"

PROFILE="debug"
OUTPUT_ROOT="$REPO_ROOT/.artifacts/sandbox-vm-e2e"
KEEP_GOING=false
SUITE_TOKENS=()
SCENARIO_TOKENS=()
RUN_ARGS=("--ignored" "--nocapture" "--test-threads=1")

usage() {
    cat <<'USAGE'
run-sandbox-vm-e2e.sh

Build, sign, and execute real-VM sandbox E2E suites.

Options:
  --profile <debug|release>   Cargo profile for builds (default: debug)
  --suite <name>              Suite to run (repeatable, comma-separated allowed)
                              names: runtime, stack, buildkit, sandbox, all
                              default: sandbox (runtime + stack)
  --scenario <name>           Run named use-case scenario(s) (repeatable/comma-separated)
                              names:
                                runtime-smoke, runtime-lifecycle, runtime-port-forwarding,
                                runtime-shared-vm-net, stack-real-services,
                                stack-control-socket, stack-port-forwarding,
                                stack-snapshot-restore, stack-user-journey-checkpoint, buildkit-roundtrip,
                                sandbox-usecases, all-usecases
                              note: when set, suite selection is derived from scenarios
  --output-dir <path>         Artifacts/log root (default: .artifacts/sandbox-vm-e2e)
  --keep-going                Continue running remaining suites after failures
  -h, --help                  Show help
  -- <args...>                Override rust test binary args (default is
                              --ignored --nocapture --test-threads=1)

Environment:
  VZ_SKIP_KERNEL_CHECK=1      Skip ~/.vz/linux preflight check
USAGE
}

err() {
    echo "error: $*" >&2
    exit 1
}

warn() {
    echo "warn: $*" >&2
}

append_unique() {
    local value="$1"
    local existing
    for existing in "${RESOLVED_SUITES[@]}"; do
        if [[ "$existing" == "$value" ]]; then
            return
        fi
    done
    RESOLVED_SUITES+=("$value")
}

append_unique_scenario() {
    local value="$1"
    local existing
    for existing in "${RESOLVED_SCENARIOS[@]}"; do
        if [[ "$existing" == "$value" ]]; then
            return
        fi
    done
    RESOLVED_SCENARIOS+=("$value")
}

expand_suite_token() {
    local token="$1"
    local lowered
    lowered="$(echo "$token" | tr '[:upper:]' '[:lower:]')"

    local part
    IFS=',' read -r -a parts <<< "$lowered"
    for part in "${parts[@]}"; do
        case "$part" in
            "")
                ;;
            runtime)
                append_unique "runtime"
                ;;
            stack)
                append_unique "stack"
                ;;
            buildkit)
                append_unique "buildkit"
                ;;
            sandbox)
                append_unique "runtime"
                append_unique "stack"
                ;;
            all)
                append_unique "runtime"
                append_unique "stack"
                append_unique "buildkit"
                ;;
            *)
                err "unknown suite '$part' (expected runtime|stack|buildkit|sandbox|all)"
                ;;
        esac
    done
}

expand_scenario_token() {
    local token="$1"
    local lowered
    lowered="$(echo "$token" | tr '[:upper:]' '[:lower:]')"

    local part
    IFS=',' read -r -a parts <<< "$lowered"
    for part in "${parts[@]}"; do
        case "$part" in
            "")
                ;;
            runtime-smoke|runtime-lifecycle|runtime-port-forwarding|runtime-shared-vm-net|stack-real-services|stack-control-socket|stack-port-forwarding|stack-snapshot-restore|stack-user-journey-checkpoint|buildkit-roundtrip)
                append_unique_scenario "$part"
                ;;
            sandbox-usecases)
                append_unique_scenario "runtime-smoke"
                append_unique_scenario "runtime-lifecycle"
                append_unique_scenario "runtime-shared-vm-net"
                append_unique_scenario "stack-real-services"
                append_unique_scenario "stack-control-socket"
                append_unique_scenario "stack-port-forwarding"
                append_unique_scenario "stack-snapshot-restore"
                append_unique_scenario "stack-user-journey-checkpoint"
                ;;
            all-usecases)
                append_unique_scenario "runtime-smoke"
                append_unique_scenario "runtime-lifecycle"
                append_unique_scenario "runtime-port-forwarding"
                append_unique_scenario "runtime-shared-vm-net"
                append_unique_scenario "stack-real-services"
                append_unique_scenario "stack-control-socket"
                append_unique_scenario "stack-port-forwarding"
                append_unique_scenario "stack-snapshot-restore"
                append_unique_scenario "stack-user-journey-checkpoint"
                append_unique_scenario "buildkit-roundtrip"
                ;;
            *)
                err "unknown scenario '$part'"
                ;;
        esac
    done
}

scenario_suite() {
    case "$1" in
        runtime-smoke|runtime-lifecycle|runtime-port-forwarding|runtime-shared-vm-net)
            echo "runtime"
            ;;
        stack-real-services|stack-control-socket|stack-port-forwarding|stack-snapshot-restore|stack-user-journey-checkpoint)
            echo "stack"
            ;;
        buildkit-roundtrip)
            echo "buildkit"
            ;;
        *)
            return 1
            ;;
    esac
}

scenario_test_filter() {
    case "$1" in
        runtime-smoke)
            echo "smoke_pull_and_run_alpine"
            ;;
        runtime-lifecycle)
            echo "lifecycle_create_exec_stop_remove"
            ;;
        runtime-port-forwarding)
            echo "port_forwarding_tcp"
            ;;
        runtime-shared-vm-net)
            echo "shared_vm_inter_service_connectivity"
            ;;
        stack-real-services)
            echo "real_services_postgres_and_redis"
            ;;
        stack-control-socket)
            echo "exec_via_control_socket"
            ;;
        stack-port-forwarding)
            echo "stack_port_forwarding"
            ;;
        stack-snapshot-restore)
            echo "complex_stack_snapshot_restore_rewinds_shared_vm_state"
            ;;
        stack-user-journey-checkpoint)
            echo "complex_stack_user_journey_with_named_volume_checkpoint"
            ;;
        buildkit-roundtrip)
            echo "buildkit_builds_dockerfile_and_run_uses_built_image"
            ;;
        *)
            return 1
            ;;
    esac
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile)
            PROFILE="${2:-}"
            shift 2
            ;;
        --suite)
            SUITE_TOKENS+=("${2:-}")
            shift 2
            ;;
        --scenario)
            SCENARIO_TOKENS+=("${2:-}")
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
        --)
            shift
            RUN_ARGS=("$@")
            break
            ;;
        *)
            err "unknown argument '$1'"
            ;;
    esac
done

if [[ "$PROFILE" != "debug" && "$PROFILE" != "release" ]]; then
    err "--profile must be one of: debug, release"
fi

RESOLVED_SUITES=()
RESOLVED_SCENARIOS=()

if [[ ${#SCENARIO_TOKENS[@]} -gt 0 ]]; then
    for token in "${SCENARIO_TOKENS[@]}"; do
        expand_scenario_token "$token"
    done
    if [[ ${#RESOLVED_SCENARIOS[@]} -eq 0 ]]; then
        err "no scenarios selected"
    fi
    if [[ ${#SUITE_TOKENS[@]} -gt 0 ]]; then
        warn "--suite is ignored when --scenario is provided"
    fi
    for scenario in "${RESOLVED_SCENARIOS[@]}"; do
        append_unique "$(scenario_suite "$scenario")"
    done
else
    if [[ ${#SUITE_TOKENS[@]} -eq 0 ]]; then
        SUITE_TOKENS=("sandbox")
    fi
    for token in "${SUITE_TOKENS[@]}"; do
        expand_suite_token "$token"
    done
fi

if [[ ${#RESOLVED_SUITES[@]} -eq 0 ]]; then
    err "no suites selected"
fi

if [[ "$(uname -s)" != "Darwin" ]]; then
    err "VM E2E suites require macOS"
fi

if [[ "$(uname -m)" != "arm64" ]]; then
    err "VM E2E suites require Apple Silicon (arm64)"
fi

if ! command -v codesign >/dev/null 2>&1; then
    err "codesign not found in PATH"
fi

if [[ ! -f "$ENTITLEMENTS" ]]; then
    err "entitlements plist not found at $ENTITLEMENTS"
fi

if [[ "${VZ_SKIP_KERNEL_CHECK:-0}" != "1" ]]; then
    if [[ ! -d "$HOME/.vz/linux" ]]; then
        err "missing Linux VM artifacts directory at $HOME/.vz/linux"
    fi
    if [[ -z "$(find "$HOME/.vz/linux" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
        err "Linux VM artifacts directory is empty: $HOME/.vz/linux"
    fi
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="$OUTPUT_ROOT/$timestamp"
mkdir -p "$RUN_DIR"
ln -sfn "$timestamp" "$OUTPUT_ROOT/latest"

BUILD_ARGS=()
if [[ "$PROFILE" == "release" ]]; then
    BUILD_ARGS+=(--release)
fi
TARGET_DIR="$REPO_ROOT/crates/target/$PROFILE"

sign_binary() {
    local binary="$1"
    local entitlements="${2:-}"
    local args=(--force --sign -)

    if [[ ! -f "$binary" ]]; then
        err "expected binary not found: $binary"
    fi

    if [[ -n "$entitlements" ]]; then
        args+=(--entitlements "$entitlements")
    fi

    echo "signing: $binary"
    codesign "${args[@]}" "$binary"
    codesign --verify --verbose "$binary"
}

find_test_binary() {
    local test_name="$1"
    local best=""

    shopt -s nullglob
    local candidate
    for candidate in "$TARGET_DIR"/deps/"$test_name"-*; do
        if [[ -f "$candidate" && -x "$candidate" ]]; then
            if [[ -z "$best" || "$candidate" -nt "$best" ]]; then
                best="$candidate"
            fi
        fi
    done
    shopt -u nullglob

    if [[ -z "$best" ]]; then
        return 1
    fi

    echo "$best"
}

suite_package() {
    case "$1" in
        runtime)
            echo "vz-oci-macos"
            ;;
        stack)
            echo "vz-stack"
            ;;
        buildkit)
            echo "vz-oci-macos"
            ;;
        *)
            return 1
            ;;
    esac
}

suite_test_name() {
    case "$1" in
        runtime)
            echo "runtime_e2e"
            ;;
        stack)
            echo "stack_e2e"
            ;;
        buildkit)
            echo "buildkit_e2e"
            ;;
        *)
            return 1
            ;;
    esac
}

run_and_log() {
    local suite="$1"
    local label="$2"
    local binary="$3"
    shift 3
    local args=("$@")
    local log_file="$RUN_DIR/${label}.log"

    echo "running [$label/$suite]: $binary ${args[*]}"

    set +e
    "$binary" "${args[@]}" 2>&1 | tee "$log_file"
    local status=${PIPESTATUS[0]}
    set -e

    return $status
}

echo "==> output directory: $RUN_DIR"
{
    echo "timestamp_utc=$timestamp"
    echo "host=$(hostname)"
    echo "profile=$PROFILE"
    echo "suites=${RESOLVED_SUITES[*]}"
    echo "scenarios=${RESOLVED_SCENARIOS[*]:-none}"
    echo "run_args=${RUN_ARGS[*]}"
} > "$RUN_DIR/run-info.txt"

echo "==> building host binaries required for local VM flows"
(
    cd "$REPO_ROOT/crates"
    cargo build "${BUILD_ARGS[@]}" -p vz-cli -p vz-guest-agent
)

if [[ -f "$TARGET_DIR/vz" ]]; then
    sign_binary "$TARGET_DIR/vz" "$ENTITLEMENTS"
fi
if [[ -f "$TARGET_DIR/vz-guest-agent" ]]; then
    sign_binary "$TARGET_DIR/vz-guest-agent"
fi

FAILED=()
PASSED=()
should_stop=false

for suite in "${RESOLVED_SUITES[@]}"; do
    package="$(suite_package "$suite")" || err "unknown suite '$suite'"
    test_name="$(suite_test_name "$suite")" || err "unknown suite '$suite'"

    echo "==> building [$suite] ($package::$test_name)"
    (
        cd "$REPO_ROOT/crates"
        cargo test -p "$package" "${BUILD_ARGS[@]}" --test "$test_name" --no-run
    )

    test_binary="$(find_test_binary "$test_name")" || err "unable to locate test binary for $test_name in $TARGET_DIR/deps"

    sign_binary "$test_binary" "$ENTITLEMENTS"

    if [[ ${#RESOLVED_SCENARIOS[@]} -gt 0 ]]; then
        for scenario in "${RESOLVED_SCENARIOS[@]}"; do
            if [[ "$(scenario_suite "$scenario")" != "$suite" ]]; then
                continue
            fi
            test_filter="$(scenario_test_filter "$scenario")" || err "unknown scenario '$scenario'"
            scenario_args=("${RUN_ARGS[@]}" "--exact" "$test_filter")

            if run_and_log "$suite" "$scenario" "$test_binary" "${scenario_args[@]}"; then
                echo "==> scenario passed: $scenario"
                PASSED+=("$scenario")
            else
                status=$?
                echo "==> scenario failed: $scenario (exit $status)"
                FAILED+=("$scenario:$status")
                if [[ "$KEEP_GOING" != "true" ]]; then
                    should_stop=true
                    break
                fi
            fi
        done
    else
        if run_and_log "$suite" "$suite" "$test_binary" "${RUN_ARGS[@]}"; then
            echo "==> suite passed: $suite"
            PASSED+=("$suite")
        else
            status=$?
            echo "==> suite failed: $suite (exit $status)"
            FAILED+=("$suite:$status")
            if [[ "$KEEP_GOING" != "true" ]]; then
                should_stop=true
            fi
        fi
    fi

    if [[ "$should_stop" == "true" ]]; then
        break
    fi
done

echo "==> summary"
echo "passed: ${PASSED[*]:-none}"
echo "failed: ${FAILED[*]:-none}"

action_summary="$RUN_DIR/summary.txt"
{
    echo "passed=${PASSED[*]:-none}"
    echo "failed=${FAILED[*]:-none}"
} > "$action_summary"

if [[ ${#FAILED[@]} -gt 0 ]]; then
    exit 1
fi

echo "all selected VM E2E suites passed"
