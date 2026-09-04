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
FULL_CLOSURE_GATE_SELECTED=false
SUITE_TOKENS=()
SCENARIO_TOKENS=()
RUN_ARGS=("--ignored" "--nocapture" "--test-threads=1")

usage() {
    cat <<'USAGE'
run-sandbox-vm-e2e.sh

Build, sign, and execute real-VM sandbox E2E suites.

Options:
  --profile <debug|release>   Cargo profile for builds (default: debug)
                              complete runtime-containing lanes require release
  --suite <name>              Suite to run (repeatable, comma-separated allowed)
                              names: runtime, stack, buildkit, sandbox, all
                              default: sandbox (runtime + stack)
  --scenario <name>           Run named use-case scenario(s) (repeatable/comma-separated)
                              names:
                                runtime-smoke, runtime-lifecycle, runtime-container-id-ownership,
                                runtime-exec-semantics, runtime-exec-supervision,
                                runtime-exec-defaults,
                                runtime-port-forwarding, runtime-shared-vm-net,
                                runtime-generation-crash-reopen, stack-real-services,
                                stack-control-socket, stack-port-forwarding,
                                stack-container-ownership,
                                stack-snapshot-restore,
                                environment-lifecycle-journal-linux-vm,
                                buildkit-roundtrip,
                                sandbox-usecases, all-usecases
                              note: when set, suite selection is derived from scenarios
  --output-dir <path>         Artifacts/log root (default: .artifacts/sandbox-vm-e2e)
  --keep-going                Continue running remaining suites after failures
  -h, --help                  Show help
  -- <args...>                Override rust test binary args (default is
                              --ignored --nocapture --test-threads=1)

Environment:
  VZ_SKIP_KERNEL_CHECK=1      Skip ~/.vz/linux preflight check
  VZ_E2E_GUEST_AGENT_BUILD_TOOL=<tool>
                              Linux guest-agent build tool (default: zigbuild)
  VZ_BUILDKIT_ARTIFACT_ARCHIVE=<absolute-path>
  VZ_BUILDKIT_ARTIFACT_SHA256=<64-hex-digest>
                              Optional paired operator override for BuildKit.
                              When neither is set, the pinned candidate is built.
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
                FULL_CLOSURE_GATE_SELECTED=true
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
            runtime-smoke|runtime-lifecycle|runtime-container-id-ownership|runtime-exec-semantics|runtime-exec-supervision|runtime-exec-defaults|runtime-port-forwarding|runtime-shared-vm-net|runtime-generation-crash-reopen|stack-real-services|stack-control-socket|stack-port-forwarding|stack-container-ownership|stack-snapshot-restore|stack-user-journey-checkpoint|environment-lifecycle-journal-linux-vm|buildkit-roundtrip)
                append_unique_scenario "$part"
                ;;
            sandbox-usecases)
                append_unique_scenario "runtime-smoke"
                append_unique_scenario "runtime-lifecycle"
                append_unique_scenario "runtime-container-id-ownership"
                append_unique_scenario "runtime-exec-semantics"
                append_unique_scenario "runtime-exec-defaults"
                append_unique_scenario "runtime-shared-vm-net"
                append_unique_scenario "stack-real-services"
                append_unique_scenario "stack-control-socket"
                append_unique_scenario "stack-port-forwarding"
                append_unique_scenario "stack-container-ownership"
                append_unique_scenario "stack-snapshot-restore"
                append_unique_scenario "environment-lifecycle-journal-linux-vm"
                ;;
            all-usecases)
                append_unique_scenario "runtime-smoke"
                append_unique_scenario "runtime-lifecycle"
                append_unique_scenario "runtime-container-id-ownership"
                append_unique_scenario "runtime-exec-semantics"
                append_unique_scenario "runtime-exec-supervision"
                append_unique_scenario "runtime-exec-defaults"
                append_unique_scenario "runtime-port-forwarding"
                append_unique_scenario "runtime-shared-vm-net"
                append_unique_scenario "runtime-generation-crash-reopen"
                append_unique_scenario "stack-real-services"
                append_unique_scenario "stack-control-socket"
                append_unique_scenario "stack-port-forwarding"
                append_unique_scenario "stack-container-ownership"
                append_unique_scenario "stack-snapshot-restore"
                append_unique_scenario "environment-lifecycle-journal-linux-vm"
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
        runtime-smoke|runtime-lifecycle|runtime-container-id-ownership|runtime-exec-semantics|runtime-exec-supervision|runtime-exec-defaults|runtime-port-forwarding|runtime-shared-vm-net|runtime-generation-crash-reopen)
            echo "runtime"
            ;;
        stack-real-services|stack-control-socket|stack-port-forwarding|stack-container-ownership|stack-snapshot-restore|environment-lifecycle-journal-linux-vm)
            echo "stack"
            ;;
        stack-user-journey-checkpoint)
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
        runtime-container-id-ownership)
            echo "container_id_lifecycle_serialization_and_generation_ownership"
            ;;
        runtime-exec-semantics)
            echo "container_exec_user_environment_semantics"
            ;;
        runtime-exec-supervision)
            echo "runtime_exec_supervision"
            ;;
        runtime-exec-defaults)
            echo "container_exec_inherits_image_process_defaults"
            ;;
        runtime-port-forwarding)
            echo "port_forwarding_tcp"
            ;;
        runtime-shared-vm-net)
            echo "shared_vm_inter_service_connectivity"
            ;;
        runtime-generation-crash-reopen)
            echo "runtime::tests::generation_ownership_sigkill_crash_reopen"
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
        stack-container-ownership)
            echo "stack_container_generation_ownership"
            ;;
        stack-snapshot-restore)
            echo "complex_stack_vm_full_snapshot_fails_closed_without_mutation"
            ;;
        stack-user-journey-checkpoint)
            echo "complex_stack_vm_full_snapshot_fails_closed_without_mutation"
            ;;
        environment-lifecycle-journal-linux-vm)
            echo "environment_lifecycle_journal_linux_vm_stop_up_delete_recovers_without_cross_environment_damage"
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

EXEC_SUPERVISION_LANE_SELECTED=false
if [[ ${#RESOLVED_SCENARIOS[@]} -eq 0 ]]; then
    for selected_suite in "${RESOLVED_SUITES[@]}"; do
        if [[ "$selected_suite" == "runtime" ]]; then
            EXEC_SUPERVISION_LANE_SELECTED=true
            break
        fi
    done
elif [[ " ${RESOLVED_SCENARIOS[*]} " == *" runtime-exec-supervision "* ]]; then
    EXEC_SUPERVISION_LANE_SELECTED=true
fi
if [[ "$EXEC_SUPERVISION_LANE_SELECTED" == "true" && "$PROFILE" != "release" ]]; then
    err "every lane that emits runtime-exec-supervision evidence requires --profile release"
fi

CRASH_REOPEN_LANE_SELECTED=false
if [[ "$FULL_CLOSURE_GATE_SELECTED" == "true" \
    || " ${RESOLVED_SCENARIOS[*]:-} " == *" runtime-generation-crash-reopen "* ]]; then
    CRASH_REOPEN_LANE_SELECTED=true
fi
if [[ "$CRASH_REOPEN_LANE_SELECTED" == "true" && "$PROFILE" != "release" ]]; then
    err "runtime generation crash/reopen evidence requires --profile release"
fi

BUILDKIT_SELECTED=false
for selected_suite in "${RESOLVED_SUITES[@]}"; do
    if [[ "$selected_suite" == "buildkit" ]]; then
        BUILDKIT_SELECTED=true
        break
    fi
done

BUILDKIT_ARTIFACT_SOURCE_MODE="not-selected"
BUILDKIT_BUILDER_INVOCATIONS=0
BUILDKIT_OPERATOR_ARCHIVE=""
BUILDKIT_EXPECTED_SHA256="none"
BUILDKIT_RELEASE_GATE_QUALIFIED="not-applicable"

# Validate the paired operator override before any guest rebuild, Cargo build,
# or VM startup. Runtime/stack-only runs intentionally ignore these variables.
if [[ "$BUILDKIT_SELECTED" == "true" ]]; then
    buildkit_archive_is_set=false
    buildkit_sha_is_set=false
    if [[ "${VZ_BUILDKIT_ARTIFACT_ARCHIVE+x}" == "x" ]]; then
        buildkit_archive_is_set=true
    fi
    if [[ "${VZ_BUILDKIT_ARTIFACT_SHA256+x}" == "x" ]]; then
        buildkit_sha_is_set=true
    fi

    if [[ "$buildkit_archive_is_set" == "false" && "$buildkit_sha_is_set" == "false" ]]; then
        BUILDKIT_ARTIFACT_SOURCE_MODE="candidate-build"
    elif [[ "$buildkit_archive_is_set" == "true" && "$buildkit_sha_is_set" == "true" ]]; then
        if [[ ! "${VZ_BUILDKIT_ARTIFACT_ARCHIVE:-}" =~ [^[:space:]] ]]; then
            err "VZ_BUILDKIT_ARTIFACT_ARCHIVE must not be blank"
        fi
        if [[ "$VZ_BUILDKIT_ARTIFACT_ARCHIVE" != /* ]]; then
            err "VZ_BUILDKIT_ARTIFACT_ARCHIVE must be an absolute path"
        fi
        if [[ ! "${VZ_BUILDKIT_ARTIFACT_SHA256:-}" =~ ^[0-9a-fA-F]{64}$ ]]; then
            err "VZ_BUILDKIT_ARTIFACT_SHA256 must be exactly 64 hexadecimal characters"
        fi
        if [[ -L "$VZ_BUILDKIT_ARTIFACT_ARCHIVE" || ! -f "$VZ_BUILDKIT_ARTIFACT_ARCHIVE" ]]; then
            err "VZ_BUILDKIT_ARTIFACT_ARCHIVE must name a regular, non-symlink file"
        fi

        BUILDKIT_OPERATOR_ARCHIVE="$(
            cd "$(dirname "$VZ_BUILDKIT_ARTIFACT_ARCHIVE")"
            printf '%s/%s\n' "$PWD" "$(basename "$VZ_BUILDKIT_ARTIFACT_ARCHIVE")"
        )"
        BUILDKIT_EXPECTED_SHA256="$(printf '%s' "$VZ_BUILDKIT_ARTIFACT_SHA256" | tr '[:upper:]' '[:lower:]')"
        buildkit_actual_sha256="$(shasum -a 256 "$BUILDKIT_OPERATOR_ARCHIVE" | cut -d' ' -f1)"
        if [[ "$buildkit_actual_sha256" != "$BUILDKIT_EXPECTED_SHA256" ]]; then
            err "BuildKit operator override checksum mismatch: expected $BUILDKIT_EXPECTED_SHA256, got $buildkit_actual_sha256"
        fi
        BUILDKIT_ARTIFACT_SOURCE_MODE="operator-override"
    else
        err "VZ_BUILDKIT_ARTIFACT_ARCHIVE and VZ_BUILDKIT_ARTIFACT_SHA256 must be set together"
    fi

    if [[ "$PROFILE" == "release" ]]; then
        if [[ "$BUILDKIT_ARTIFACT_SOURCE_MODE" != "candidate-build" ]]; then
            err "release-profile BuildKit evidence requires the pinned candidate builder; operator overrides are debug-only"
        fi
        BUILDKIT_RELEASE_GATE_QUALIFIED="pending"
    fi
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

if ! command -v jq >/dev/null 2>&1; then
    err "jq not found in PATH (required to resolve Cargo build artifacts)"
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
mkdir -p "$OUTPUT_ROOT"
OUTPUT_ROOT="$(cd "$OUTPUT_ROOT" && pwd)"
RUN_DIR="$OUTPUT_ROOT/$timestamp"
if ! mkdir "$RUN_DIR" 2>/dev/null; then
    RUN_DIR="$(mktemp -d "$OUTPUT_ROOT/${timestamp}.XXXXXX")"
fi
RUN_NAME="$(basename "$RUN_DIR")"
ln -sfn "$RUN_NAME" "$OUTPUT_ROOT/latest"
BUILDKIT_RUNTIME_INVENTORY_EVIDENCE="$RUN_DIR/buildkit-runtime-inventory.txt"
CONTAINER_ID_OWNERSHIP_EVIDENCE="$RUN_DIR/container-id-ownership.json"
CONTAINER_ID_OWNERSHIP_SHA256="$RUN_DIR/container-id-ownership.json.sha256"
STACK_TEARDOWN_EVIDENCE="$RUN_DIR/stack-port-forwarding-teardown.json"
STACK_TEARDOWN_SHA256="$RUN_DIR/stack-port-forwarding-teardown.json.sha256"
STACK_CONTAINER_OWNERSHIP_EVIDENCE="$RUN_DIR/stack-container-ownership.json"
STACK_CONTAINER_OWNERSHIP_SHA256="$RUN_DIR/stack-container-ownership.json.sha256"
VM_FULL_UNSUPPORTED_EVIDENCE="$RUN_DIR/stack-vm-full-unsupported.json"
VM_FULL_UNSUPPORTED_SHA256="$RUN_DIR/stack-vm-full-unsupported.json.sha256"
ENVIRONMENT_LIFECYCLE_EVIDENCE="$RUN_DIR/environment-lifecycle-journal-linux-vm.json"
ENVIRONMENT_LIFECYCLE_SHA256="$RUN_DIR/environment-lifecycle-journal-linux-vm.json.sha256"
EXEC_SUPERVISION_EVIDENCE="$RUN_DIR/runtime-exec-supervision.json"
EXEC_SUPERVISION_SHA256="$RUN_DIR/runtime-exec-supervision.json.sha256"
RUNTIME_CRASH_REOPEN_EVIDENCE="$RUN_DIR/runtime-generation-crash-reopen.json"
RUNTIME_CRASH_REOPEN_SHA256="$RUN_DIR/runtime-generation-crash-reopen.json.sha256"
STACK_CRASH_REOPEN_EVIDENCE="$RUN_DIR/runtime-generation-state-store-v7.json"
STACK_CRASH_REOPEN_SHA256="$RUN_DIR/runtime-generation-state-store-v7.json.sha256"

BUILDKIT_ARCHIVE_BASENAME="vz-buildkit-v0.19.0-linux-arm64.tar"
BUILDKIT_SHA256_BASENAME="$BUILDKIT_ARCHIVE_BASENAME.sha256"
BUILDKIT_ARTIFACT_EVIDENCE_DIR="$RUN_DIR/buildkit-artifact"
BUILDKIT_ARTIFACT_ARCHIVE_EVIDENCE="none"
BUILDKIT_ARTIFACT_SHA256_EVIDENCE="none"
BUILDKIT_ARTIFACT_MANIFEST_EVIDENCE="none"
BUILDKIT_ARTIFACT_INVENTORY_EVIDENCE="none"
BUILDKIT_ARTIFACT_PROVENANCE_EVIDENCE="none"
BUILDKIT_ARTIFACT_VERIFICATION_EVIDENCE="none"
BUILDKIT_ARTIFACT_EVIDENCE_CHECKSUMS="none"
BUILDKIT_BUILDER_OUTPUT_CHECKSUMS="none"

provision_buildkit_artifact() {
    local builder="$REPO_ROOT/scripts/build-runtime-free-buildkit.sh"
    local validator="$REPO_ROOT/scripts/validate-runtime-free-buildkit.sh"
    local source_dir=""
    local source_archive=""
    local source_sha_file=""
    local validation_dir="$RUN_DIR/buildkit-artifact-validation"
    local sidecar_digest=""
    local sidecar_name=""
    local sidecar_extra=""

    [[ -x "$validator" ]] || err "BuildKit artifact validator is not executable: $validator"

    if [[ "$BUILDKIT_ARTIFACT_SOURCE_MODE" == "candidate-build" ]]; then
        [[ -x "$builder" ]] || err "BuildKit artifact builder is not executable: $builder"
        source_dir="$RUN_DIR/buildkit-candidate-output"
        BUILDKIT_BUILDER_INVOCATIONS=1
        "$builder" --output-dir "$source_dir"
        source_archive="$source_dir/$BUILDKIT_ARCHIVE_BASENAME"
        source_sha_file="$source_dir/$BUILDKIT_SHA256_BASENAME"
        [[ -f "$source_archive" && ! -L "$source_archive" ]] \
            || err "candidate builder did not produce $BUILDKIT_ARCHIVE_BASENAME"
        [[ -f "$source_sha_file" && ! -L "$source_sha_file" ]] \
            || err "candidate builder did not produce $BUILDKIT_SHA256_BASENAME"
        IFS=' ' read -r sidecar_digest sidecar_name sidecar_extra < "$source_sha_file" \
            || err "candidate BuildKit checksum sidecar must contain exactly one line"
        if [[ "$(wc -l < "$source_sha_file" | tr -d '[:space:]')" != "1" \
            || -n "$sidecar_extra" || "$sidecar_name" != "$BUILDKIT_ARCHIVE_BASENAME" ]]; then
            err "candidate BuildKit checksum sidecar must contain only the digest and exact archive basename"
        fi
        BUILDKIT_EXPECTED_SHA256="$sidecar_digest"
        if [[ ! "$BUILDKIT_EXPECTED_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
            err "candidate BuildKit checksum sidecar does not contain a lowercase SHA-256 digest"
        fi
    else
        source_archive="$BUILDKIT_OPERATOR_ARCHIVE"
    fi

    mkdir "$BUILDKIT_ARTIFACT_EVIDENCE_DIR"
    BUILDKIT_ARTIFACT_ARCHIVE_EVIDENCE="$BUILDKIT_ARTIFACT_EVIDENCE_DIR/$BUILDKIT_ARCHIVE_BASENAME"
    BUILDKIT_ARTIFACT_SHA256_EVIDENCE="$BUILDKIT_ARTIFACT_EVIDENCE_DIR/$BUILDKIT_SHA256_BASENAME"
    cp "$source_archive" "$BUILDKIT_ARTIFACT_ARCHIVE_EVIDENCE"
    printf '%s  %s\n' "$BUILDKIT_EXPECTED_SHA256" "$BUILDKIT_ARCHIVE_BASENAME" \
        > "$BUILDKIT_ARTIFACT_SHA256_EVIDENCE"

    # Preserve genuine builder provenance beside the immutable copy so the
    # validator can bind it into candidate-build evidence. Operator overrides
    # intentionally report provenance as unavailable rather than inventing it.
    if [[ "$BUILDKIT_ARTIFACT_SOURCE_MODE" == "candidate-build" ]]; then
        [[ -f "$source_dir/buildkit-artifact-provenance.json" ]] \
            || err "candidate builder did not produce buildkit-artifact-provenance.json"
        cp "$source_dir/buildkit-artifact-provenance.json" \
            "$BUILDKIT_ARTIFACT_EVIDENCE_DIR/buildkit-artifact-provenance.json"
    fi

    chmod 0444 "$BUILDKIT_ARTIFACT_ARCHIVE_EVIDENCE" "$BUILDKIT_ARTIFACT_SHA256_EVIDENCE"
    "$validator" \
        --archive "$BUILDKIT_ARTIFACT_ARCHIVE_EVIDENCE" \
        --expected-sha256 "$BUILDKIT_EXPECTED_SHA256" \
        --output-dir "$validation_dir" \
        --source-mode "$BUILDKIT_ARTIFACT_SOURCE_MODE"

    BUILDKIT_ARTIFACT_MANIFEST_EVIDENCE="$BUILDKIT_ARTIFACT_EVIDENCE_DIR/manifest.json"
    BUILDKIT_ARTIFACT_INVENTORY_EVIDENCE="$BUILDKIT_ARTIFACT_EVIDENCE_DIR/buildkit-artifact-inventory.txt"
    BUILDKIT_ARTIFACT_VERIFICATION_EVIDENCE="$BUILDKIT_ARTIFACT_EVIDENCE_DIR/buildkit-artifact-verification.json"
    for validated_name in manifest.json buildkit-artifact-inventory.txt buildkit-artifact-verification.json; do
        [[ -s "$validation_dir/$validated_name" ]] \
            || err "BuildKit validator did not produce $validated_name"
        cp "$validation_dir/$validated_name" "$BUILDKIT_ARTIFACT_EVIDENCE_DIR/$validated_name"
    done

    if [[ -s "$validation_dir/buildkit-artifact-provenance.json" ]]; then
        BUILDKIT_ARTIFACT_PROVENANCE_EVIDENCE="$BUILDKIT_ARTIFACT_EVIDENCE_DIR/buildkit-artifact-provenance.json"
        if [[ "$BUILDKIT_ARTIFACT_SOURCE_MODE" == "candidate-build" ]] \
            && ! cmp -s "$source_dir/buildkit-artifact-provenance.json" \
                "$validation_dir/buildkit-artifact-provenance.json"; then
            err "BuildKit validator provenance does not match the candidate builder output"
        fi
        cp "$validation_dir/buildkit-artifact-provenance.json" "$BUILDKIT_ARTIFACT_PROVENANCE_EVIDENCE"
    elif [[ "$BUILDKIT_ARTIFACT_SOURCE_MODE" == "candidate-build" ]]; then
        err "BuildKit validator did not retain candidate build provenance"
    fi
    rm -rf "$validation_dir"

    BUILDKIT_ARTIFACT_EVIDENCE_CHECKSUMS="$BUILDKIT_ARTIFACT_EVIDENCE_DIR/buildkit-artifact-evidence.sha256"
    local evidence_files=(
        "$BUILDKIT_ARCHIVE_BASENAME"
        "$BUILDKIT_SHA256_BASENAME"
        "manifest.json"
        "buildkit-artifact-inventory.txt"
        "buildkit-artifact-verification.json"
    )
    if [[ "$BUILDKIT_ARTIFACT_PROVENANCE_EVIDENCE" != "none" ]]; then
        evidence_files+=("buildkit-artifact-provenance.json")
    fi
    (
        cd "$BUILDKIT_ARTIFACT_EVIDENCE_DIR"
        shasum -a 256 "${evidence_files[@]}" > "$(basename "$BUILDKIT_ARTIFACT_EVIDENCE_CHECKSUMS")"
        shasum -a 256 -c "$(basename "$BUILDKIT_ARTIFACT_EVIDENCE_CHECKSUMS")" >/dev/null
    )
    chmod 0444 "$BUILDKIT_ARTIFACT_EVIDENCE_DIR"/*
    chmod 0555 "$BUILDKIT_ARTIFACT_EVIDENCE_DIR"

    if [[ "$BUILDKIT_ARTIFACT_SOURCE_MODE" == "candidate-build" ]]; then
        BUILDKIT_BUILDER_OUTPUT_CHECKSUMS="$source_dir/buildkit-candidate-output.sha256"
        local builder_output_files=()
        local builder_output_path
        for builder_output_path in "$source_dir"/*; do
            [[ -f "$builder_output_path" && ! -L "$builder_output_path" ]] \
                || err "candidate builder left a non-regular output: $builder_output_path"
            builder_output_files+=("$(basename "$builder_output_path")")
        done
        [[ ${#builder_output_files[@]} -gt 0 ]] || err "candidate builder retained no outputs"
        (
            cd "$source_dir"
            shasum -a 256 "${builder_output_files[@]}" \
                > "$(basename "$BUILDKIT_BUILDER_OUTPUT_CHECKSUMS")"
            shasum -a 256 -c "$(basename "$BUILDKIT_BUILDER_OUTPUT_CHECKSUMS")" >/dev/null
        )
        chmod 0444 "$source_dir"/*
        chmod 0555 "$source_dir"
    fi
}

if [[ "$BUILDKIT_SELECTED" == "true" ]]; then
    echo "==> provisioning BuildKit artifact ($BUILDKIT_ARTIFACT_SOURCE_MODE)"
    provision_buildkit_artifact
fi

# The VM executes the Linux guest agent embedded in each profile's initramfs,
# not the macOS host binary built below. Rebuild both bundles on every run so
# source changes cannot be silently tested against a stale guest executable.
GUEST_AGENT_BUILD_TOOL="${VZ_E2E_GUEST_AGENT_BUILD_TOOL:-zigbuild}"
for kernel_profile in developer container; do
    echo "==> rebuilding Linux $kernel_profile guest bundle"
    make -C "$REPO_ROOT/linux" \
        KERNEL_PROFILE="$kernel_profile" \
        TRUST_EXISTING_KERNEL_IMAGE=1 \
        GUEST_AGENT_BUILD_TOOL="$GUEST_AGENT_BUILD_TOOL" \
        initramfs version 2>&1 | tee "$RUN_DIR/linux-$kernel_profile-build.log"
done

DEVELOPER_INITRAMFS_SHA256="$(shasum -a 256 "$REPO_ROOT/linux/out/initramfs.img" | cut -d' ' -f1)"
CONTAINER_INITRAMFS_SHA256="$(shasum -a 256 "$REPO_ROOT/linux/out/container/initramfs.img" | cut -d' ' -f1)"

BUILD_ARGS=()
if [[ "$PROFILE" == "release" ]]; then
    BUILD_ARGS+=(--release)
fi

run_cargo_recording_artifacts() {
    local artifact_log="$1"
    shift

    # Cargo is the source of truth for artifact locations. This matters when
    # build.build-dir separates build outputs from the configured target-dir.
    (
        cd "$REPO_ROOT/crates"
        cargo "$@" --message-format=json-render-diagnostics
    ) | tee "$artifact_log" \
        | jq --unbuffered -r \
            'select(.reason == "compiler-message") | .message.rendered // empty' >&2
}

resolve_cargo_executable() {
    local artifact_log="$1"
    local target_name="$2"
    local target_kind="$3"

    jq -ers \
        --arg target_name "$target_name" \
        --arg target_kind "$target_kind" \
        '
            [
                .[]
                | select(
                    .reason == "compiler-artifact"
                    and .target.name == $target_name
                    and (.target.kind | index($target_kind)) != null
                    and .executable != null
                )
                | .executable
            ]
            | unique
            | if length == 1 then
                .[0]
              elif length == 0 then
                error("Cargo did not report an executable for \($target_kind) target \($target_name)")
              else
                error("Cargo reported multiple executables for \($target_kind) target \($target_name): \(.)")
              end
        ' "$artifact_log"
}

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

validate_buildkit_runtime_inventory_evidence() {
    local evidence_file="$1"

    jq -e '
        (type == "object") and
        ((keys | sort) == ([
            "buildkitd_executable",
            "buildkitd_oci_worker_binary",
            "cgroup_filesystem",
            "forbidden_runtime_paths",
            "observed_oci_subcommands",
            "observed_runtime_paths",
            "oci_runtime_elf_paths",
            "oci_worker_binary",
            "runtime_binary",
            "runtime_version",
            "shim_target"
        ] | sort)) and
        (.oci_worker_binary == "/tmp/vz-buildkit-oci-runtime") and
        (.shim_target == "/usr/bin/vz-guest-agent") and
        (.runtime_binary == "/mnt/linux-bin/youki") and
        (.observed_runtime_paths == ["/mnt/linux-bin/youki"]) and
        ((.observed_oci_subcommands | type) == "array") and
        (all(.observed_oci_subcommands[]; type == "string")) and
        (any(.observed_oci_subcommands[]; . == "create" or . == "run")) and
        (.oci_runtime_elf_paths == ["/mnt/linux-bin/youki"]) and
        (.forbidden_runtime_paths == []) and
        ((.runtime_version | type) == "string") and
        (.runtime_version | test("youki"; "i")) and
        (.buildkitd_executable == "/mnt/buildkit-bin/buildkitd") and
        (.buildkitd_oci_worker_binary == "/tmp/vz-buildkit-oci-runtime") and
        (.cgroup_filesystem == "cgroup2")
    ' "$evidence_file" >/dev/null
}

validate_container_id_ownership_evidence() {
    local evidence_file="$1"

    jq -e '
        (type == "object") and
        (.schema_version == 1) and
        (.scenario == "runtime-container-id-ownership") and
        ((.container_id | type) == "string") and
        (.standalone.in_flight_duplicate_rejected == true) and
        (.standalone.active_duplicate_rejected == true) and
        (.stack.duplicate_rejected_before_release == true) and
        (.stack.loser_setup_absent == true) and
        (.stack.failed_setup_returned_error == true) and
        (.stack.failed_setup_clean == true) and
        (.stack.failed_generation_released == true) and
        (.stack.failed_guest_resources_clean == true) and
        (.stack.failed_host_maps_clean == true) and
        (.stack.failed_setup_commit_absent == true) and
        (.stack.successful_setup_commit_present == true) and
        (.stack.exec_did_not_cross_generation == true) and
        (.stack.ready_a_matches_process_probe == true) and
        (.stack.ready_b_matches_process_probe == true) and
        (.stack.ready_generations_distinct == true) and
        (.standalone.generation_a.owner == "standalone-a") and
        (.standalone.generation_b.owner == "standalone-b") and
        (.stack.generation_a.owner == "stack-a") and
        (.stack.generation_b.owner == "stack-b") and
        (.stack.ready_generation_a.container_id == .container_id) and
        (.stack.ready_generation_b.container_id == .container_id) and
        (.stack.ready_generation_b.lifecycle_generation > .stack.ready_generation_a.lifecycle_generation) and
        (.stack.ready_generation_a.init_pid > 0) and
        (.stack.ready_generation_b.init_pid > 0) and
        (.stack.ready_generation_a.init_start_time > 0) and
        (.stack.ready_generation_b.init_start_time > 0) and
        (.stack.ready_generation_a.cgroup.device > 0) and
        (.stack.ready_generation_a.cgroup.inode > 0) and
        (.stack.ready_generation_b.cgroup.device > 0) and
        (.stack.ready_generation_b.cgroup.inode > 0) and
        (.stack.ready_generation_a.root.device > 0) and
        (.stack.ready_generation_a.root.inode > 0) and
        (.stack.ready_generation_b.root.device > 0) and
        (.stack.ready_generation_b.root.inode > 0) and
        ((.stack.ready_generation_a.init_start_time != .stack.ready_generation_b.init_start_time) or
         (.stack.ready_generation_a.cgroup != .stack.ready_generation_b.cgroup) or
         (.stack.ready_generation_a.namespaces != .stack.ready_generation_b.namespaces) or
         (.stack.ready_generation_a.root != .stack.ready_generation_b.root)) and
        (.standalone.generation_a.boot_id != .standalone.generation_b.boot_id) and
        ((.stack.generation_a.guest_init_pid != .stack.generation_b.guest_init_pid) or
         (.stack.generation_a.start_time != .stack.generation_b.start_time) or
         (.stack.generation_a.cgroup_path != .stack.generation_b.cgroup_path) or
         (.stack.generation_a.cgroup_identity != .stack.generation_b.cgroup_identity) or
         (.stack.generation_a.mnt_identity != .stack.generation_b.mnt_identity) or
         (.stack.generation_a.net_identity != .stack.generation_b.net_identity) or
         (.stack.generation_a.pid_identity != .stack.generation_b.pid_identity) or
         (.stack.generation_a.ipc_identity != .stack.generation_b.ipc_identity) or
         (.stack.generation_a.uts_identity != .stack.generation_b.uts_identity) or
         (.stack.generation_a.root_identity != .stack.generation_b.root_identity)) and
        (.final.metadata_absent == true) and
        (.final.rootfs_absent == true) and
        (.final.shared_vm_absent == true) and
        (.final.guest_resources_clean == true) and
        (.final.stale_exec_rejected == true) and
        (.final.generation_released == true) and
        (.final.host_maps_clean == true) and
        (.final.orphan_setup_tmp == [])
    ' "$evidence_file" >/dev/null
}

validate_exec_supervision_evidence() {
    local evidence_file="$1"
    local expected_profile="$2"
    local expected_test_binary_sha256="$3"
    local expected_developer_initramfs_sha256="$4"

    # The stable `unary` evidence label denotes the synchronous/unary-shaped
    # host adapter. It runs over the supervised stream; raw legacy
    # OciService.Exec is retired and cannot satisfy this release gate.
    jq -e \
        --arg expected_profile "$expected_profile" \
        --arg expected_test_binary_sha256 "$expected_test_binary_sha256" \
        --arg expected_developer_initramfs_sha256 "$expected_developer_initramfs_sha256" '
        def integer: type == "number" and . >= 0 and . == floor;
        def sha256: type == "string" and test("^[0-9a-f]{64}$");
        def build_identity:
            ((keys | sort) == ([
                "developer_initramfs_sha256", "profile", "test_binary_sha256"
            ] | sort)) and
            (.profile == "release") and
            (.profile == $expected_profile) and
            (.test_binary_sha256 | sha256) and
            (.test_binary_sha256 == $expected_test_binary_sha256) and
            (.developer_initramfs_sha256 | sha256) and
            (.developer_initramfs_sha256 == $expected_developer_initramfs_sha256);
        def member:
            ((keys | sort) == (["pgid", "pid", "start_time"] | sort)) and
            (.pid | integer) and (.pid > 0) and
            (.start_time | integer) and (.start_time > 0) and
            (.pgid | integer) and (.pgid > 0);
        def identity:
            ((keys | sort) == ([
                "cgroup_path", "child_container_pgid", "child_container_pid",
                "child_host_pgid", "child_host_pid", "child_start_time",
                "container_pgid", "container_pid", "host_pgid", "host_pid",
                "start_time"
            ] | sort)) and
            ((.cgroup_path | type) == "string") and
            (.cgroup_path | startswith("/")) and
            (.container_pid | integer) and (.container_pid > 0) and
            (.host_pid | integer) and (.host_pid > 0) and
            (.start_time | integer) and (.start_time > 0) and
            (.container_pgid | integer) and (.container_pgid > 0) and
            (.host_pgid | integer) and (.host_pgid > 0) and
            (.child_container_pid | integer) and (.child_container_pid > 0) and
            (.child_host_pid | integer) and (.child_host_pid > 0) and
            (.child_start_time | integer) and (.child_start_time > 0) and
            (.child_container_pgid | integer) and (.child_container_pgid > 0) and
            (.child_host_pgid | integer) and (.child_host_pgid > 0) and
            (.child_container_pgid == .container_pgid) and
            (.child_host_pgid == .host_pgid) and
            (.child_host_pid != .host_pid);
        def cell($cgroup_path):
            ((keys | sort) == ([
                "active_cgroup_members", "adapter", "baseline_cgroup_members",
                "cgroup_restored", "elapsed_ms", "execution_id",
                "expected_exit_code", "identity", "marker_removed",
                "observed_exit_code", "post_case_probe", "process_identity_absent",
                "pty_resized", "session_reaped", "signal", "termination",
                "timed_out", "timeout_requested_ms"
            ] | sort)) and
            (.adapter == "unary" or .adapter == "streaming" or .adapter == "pty") and
            (.termination == "term" or .termination == "int" or
                .termination == "kill" or .termination == "timeout") and
            (.execution_id == ("exec-supervision-" + .adapter + "-" + .termination)) and
            (.identity | identity) and
            (.identity.cgroup_path == $cgroup_path) and
            ((.baseline_cgroup_members | type) == "array") and
            ((.baseline_cgroup_members | length) == 1) and
            (all(.baseline_cgroup_members[]; member)) and
            ((.active_cgroup_members | type) == "array") and
            ((.active_cgroup_members | length) > (.baseline_cgroup_members | length)) and
            (all(.active_cgroup_members[]; member)) and
            (.identity.host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.identity.child_host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.cgroup_restored == true) and
            (.process_identity_absent == true) and
            (.session_reaped == true) and
            (.marker_removed == true) and
            (.post_case_probe == true) and
            (.pty_resized == (.adapter == "pty")) and
            (.elapsed_ms | integer) and (.elapsed_ms > 0) and
            (if .termination == "timeout" then
                .signal == null and .expected_exit_code == null and
                .observed_exit_code == null and .timed_out == true and
                .timeout_requested_ms == 2000 and
                .elapsed_ms >= .timeout_requested_ms and
                .elapsed_ms <= (.timeout_requested_ms + 4000)
             elif .termination == "term" then
                .signal == "SIGTERM" and .expected_exit_code == 143 and
                .observed_exit_code == 143 and .timed_out == false and
                .timeout_requested_ms == null
             elif .termination == "int" then
                .signal == "SIGINT" and .expected_exit_code == 130 and
                .observed_exit_code == 130 and .timed_out == false and
                .timeout_requested_ms == null
             else
                .signal == "SIGKILL" and .expected_exit_code == 137 and
                .observed_exit_code == 137 and .timed_out == false and
                .timeout_requested_ms == null
             end);
        def normal_exit($cgroup_path):
            ((keys | sort) == ([
                "active_cgroup_members", "adapter", "baseline_cgroup_members",
                "cgroup_restored", "child_identity_absent", "execution_id",
                "exit_code", "identity", "leader_identity_absent",
                "markers_removed", "post_case_probe", "session_reaped"
            ] | sort)) and
            (.adapter == "streaming") and
            (.execution_id == "exec-supervision-normal-exit") and
            (.exit_code == 0) and
            (.identity | identity) and
            (.identity.cgroup_path == $cgroup_path) and
            ((.baseline_cgroup_members | type) == "array") and
            ((.baseline_cgroup_members | length) == 1) and
            (all(.baseline_cgroup_members[]; member)) and
            ((.active_cgroup_members | type) == "array") and
            ((.active_cgroup_members | length) > (.baseline_cgroup_members | length)) and
            (all(.active_cgroup_members[]; member)) and
            (.identity.host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.identity.child_host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.cgroup_restored == true) and
            (.leader_identity_absent == true) and
            (.child_identity_absent == true) and
            (.session_reaped == true) and
            (.markers_removed == true) and
            (.post_case_probe == true);
        def cancellation($cgroup_path):
            ((keys | sort) == ([
                "active_cgroup_members", "adapter", "baseline_cgroup_members",
                "cgroup_restored", "child_identity_absent", "execution_id",
                "exit_code", "identity", "leader_identity_absent",
                "marker_removed", "post_case_probe", "session_reaped"
            ] | sort)) and
            (.adapter == "streaming") and
            (.execution_id == "exec-supervision-cancel") and
            (.exit_code == 143) and
            (.identity | identity) and
            (.identity.cgroup_path == $cgroup_path) and
            ((.baseline_cgroup_members | type) == "array") and
            ((.baseline_cgroup_members | length) == 1) and
            (all(.baseline_cgroup_members[]; member)) and
            ((.active_cgroup_members | type) == "array") and
            ((.active_cgroup_members | length) > (.baseline_cgroup_members | length)) and
            (all(.active_cgroup_members[]; member)) and
            (.identity.host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.identity.child_host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.cgroup_restored == true) and
            (.leader_identity_absent == true) and
            (.child_identity_absent == true) and
            (.session_reaped == true) and
            (.marker_removed == true) and
            (.post_case_probe == true);
        def cancel_before_ready:
            ((keys | sort) == ([
                "adapter", "admission", "cgroup_restored",
                "container_ready_events", "execution_id", "marker_absent",
                "post_case_probe", "session_reaped", "terminal_error"
            ] | sort)) and
            (.adapter == "streaming") and
            (.execution_id == "exec-supervision-cancel-before-ready") and
            (.admission == "exec-before-guest-rpc") and
            (.container_ready_events == 0) and
            ((.terminal_error | type) == "string") and
            (.terminal_error | contains("cancelled during startup")) and
            (.marker_absent == true) and
            (.cgroup_restored == true) and
            (.session_reaped == true) and
            (.post_case_probe == true);
        def pre_spawn_rejection:
            ((keys | sort) == ([
                "adapter", "attempts", "authenticated_definite_error",
                "cgroup_members_after", "cgroup_members_before", "execution_id",
                "interactive_events", "invalid_environment_key",
                "lifecycle_writer_available", "post_case_probe",
                "session_count_after", "session_count_before",
                "stale_control_rejected", "target", "terminal_error"
            ] | sort)) and
            (.attempts == 1) and
            (.adapter == "streaming") and
            (.target == "container") and
            (.execution_id == "exec-supervision-pre-spawn-rejection") and
            (.invalid_environment_key == "INVALID=ENVIRONMENT_KEY") and
            (.authenticated_definite_error == true) and
            ((.terminal_error | type) == "string") and
            (.terminal_error | contains("invalid key")) and
            (.terminal_error | contains("lifecycle authority was retained") | not) and
            (.interactive_events == 0) and
            (.session_count_before == 0) and
            (.session_count_after == 0) and
            ((.cgroup_members_before | type) == "array") and
            ((.cgroup_members_before | length) == 1) and
            (all(.cgroup_members_before[]; member)) and
            (.cgroup_members_after == .cgroup_members_before) and
            (.stale_control_rejected == true) and
            (.lifecycle_writer_available == true) and
            (.post_case_probe == true);
        def slow_live_consumer:
            ((keys | sort) == ([
                "adapter", "attempts", "cgroup_restored",
                "channel_capacity_events", "content_exact", "execution_id",
                "exit_code", "exit_events", "exit_last",
                "expected_stdout_bytes", "lifecycle_writer_available",
                "max_chunk_bytes", "observed_stdout_bytes", "pause_ms",
                "post_case_probe", "ready_events", "retired_drain_threshold_ms",
                "session_reaped", "stderr_bytes", "stdout_sha256"
            ] | sort)) and
            (.attempts == 1) and
            (.adapter == "streaming") and
            (.execution_id == "exec-supervision-slow-live-consumer") and
            (.pause_ms == 6000) and
            (.retired_drain_threshold_ms == 5000) and
            (.pause_ms > .retired_drain_threshold_ms) and
            (.channel_capacity_events == 64) and
            (.max_chunk_bytes == 65536) and
            (.expected_stdout_bytes == 5242880) and
            (.expected_stdout_bytes > (.channel_capacity_events * .max_chunk_bytes)) and
            (.observed_stdout_bytes == .expected_stdout_bytes) and
            (.stdout_sha256 == "d07bf203cd25e8dcd5b467d29f1d43ef2a33afa886f9f9d99dd036de8d1d4a45") and
            (.content_exact == true) and
            (.ready_events == 1) and
            (.exit_events == 1) and
            (.stderr_bytes == 0) and
            (.exit_code == 0) and
            (.exit_last == true) and
            (.cgroup_restored == true) and
            (.session_reaped == true) and
            (.lifecycle_writer_available == true) and
            (.post_case_probe == true);
        def response_loss_before_ready($cgroup_path):
            ((keys | sort) == ([
                "active_cgroup_members", "adapter", "attempts",
                "baseline_cgroup_members", "cgroup_restored",
                "child_identity_absent", "execution_id", "fault_dwell_ms",
                "fault_selector", "identity", "injected_error_observed",
                "injection_error_marker", "interactive_events",
                "leader_identity_absent",
                "lifecycle_writer_available", "marker_removed",
                "marker_observed_during_fault_dwell",
                "post_case_probe", "request_id_reconcile_outcome",
                "request_id_reconciled_to_terminal_proof", "session_reaped",
                "stale_control_rejected", "target", "terminal_error"
            ] | sort)) and
            (.attempts == 1) and
            (.adapter == "streaming") and
            (.target == "container") and
            (.execution_id == "exec-supervision-response-loss-before-ready") and
            (.fault_selector == "/vz-exec-response-loss-command/sh") and
            (.fault_dwell_ms == 5000) and
            (.injection_error_marker == "test-injected container exec response loss before readiness") and
            (.injected_error_observed == true) and
            ((.terminal_error | type) == "string") and
            (.injection_error_marker as $marker | .terminal_error | contains($marker)) and
            (.terminal_error | contains("; reconciliation=TERMINAL_REAPED")) and
            (.marker_observed_during_fault_dwell == true) and
            (.interactive_events == 0) and
            (.identity | identity) and
            (.identity.cgroup_path == $cgroup_path) and
            ((.baseline_cgroup_members | type) == "array") and
            ((.baseline_cgroup_members | length) == 1) and
            (all(.baseline_cgroup_members[]; member)) and
            ((.active_cgroup_members | type) == "array") and
            ((.active_cgroup_members | length) > (.baseline_cgroup_members | length)) and
            (all(.active_cgroup_members[]; member)) and
            (.identity.host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.identity.child_host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.request_id_reconcile_outcome == "TERMINAL_REAPED") and
            (.request_id_reconciled_to_terminal_proof == true) and
            (.cgroup_restored == true) and
            (.leader_identity_absent == true) and
            (.child_identity_absent == true) and
            (.session_reaped == true) and
            (.stale_control_rejected == true) and
            (.lifecycle_writer_available == true) and
            (.marker_removed == true) and
            (.post_case_probe == true);
        def dropped_future($cgroup_path):
            ((keys | sort) == ([
                "active_cgroup_members", "adapter", "baseline_cgroup_members",
                "cgroup_restored", "child_identity_absent", "execution_id",
                "identity", "join_cancelled", "leader_identity_absent",
                "marker_removed", "post_case_probe", "session_reaped"
            ] | sort)) and
            (.adapter == "streaming") and
            (.execution_id == "exec-supervision-dropped-future") and
            (.join_cancelled == true) and
            (.identity | identity) and
            (.identity.cgroup_path == $cgroup_path) and
            ((.baseline_cgroup_members | type) == "array") and
            ((.baseline_cgroup_members | length) == 1) and
            (all(.baseline_cgroup_members[]; member)) and
            ((.active_cgroup_members | type) == "array") and
            ((.active_cgroup_members | length) > (.baseline_cgroup_members | length)) and
            (all(.active_cgroup_members[]; member)) and
            (.identity.host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.identity.child_host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.cgroup_restored == true) and
            (.leader_identity_absent == true) and
            (.child_identity_absent == true) and
            (.session_reaped == true) and
            (.marker_removed == true) and
            (.post_case_probe == true);
        def ready_before_owner_abort($cgroup_path):
            ((keys | sort) == ([
                "active_cgroup_members", "adapter", "admission",
                "baseline_cgroup_members", "case", "cgroup_restored",
                "child_identity_absent", "execution_id", "identity",
                "join_cancelled", "leader_identity_absent", "marker_removed",
                "post_case_probe", "session_reaped",
                "session_registered_before_abort", "stale_control_rejected"
            ] | sort)) and
            (.case == "ready-before-owner-named" or
                .case == "ready-before-owner-anonymous") and
            (.admission == "exec-guest-rpc-ready-before-owner") and
            (.join_cancelled == true) and
            (.identity | identity) and
            (.identity.cgroup_path == $cgroup_path) and
            ((.baseline_cgroup_members | type) == "array") and
            ((.baseline_cgroup_members | length) == 1) and
            (all(.baseline_cgroup_members[]; member)) and
            ((.active_cgroup_members | type) == "array") and
            ((.active_cgroup_members | length) > (.baseline_cgroup_members | length)) and
            (all(.active_cgroup_members[]; member)) and
            (.identity.host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.identity.child_host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.cgroup_restored == true) and
            (.leader_identity_absent == true) and
            (.child_identity_absent == true) and
            (.session_reaped == true) and
            (.marker_removed == true) and
            (.post_case_probe == true) and
            (if .case == "ready-before-owner-named" then
                .adapter == "streaming" and
                .execution_id == "exec-supervision-ready-before-owner" and
                .session_registered_before_abort == true and
                .stale_control_rejected == true
             else
                .adapter == "unary" and
                .execution_id == null and
                .session_registered_before_abort == false and
                .stale_control_rejected == null
             end);
        def outer_identity:
            ((keys | sort) == ([
                "cgroup_path", "pgid", "pid", "start_time",
                "target_cgroup_member"
            ] | sort)) and
            (.cgroup_path == "/") and
            (.target_cgroup_member == false) and
            (.pid | integer) and (.pid > 0) and
            (.start_time | integer) and (.start_time > 0) and
            # The outer supervisor remains outside the container PID
            # namespace, so procfs may render its process group as zero from
            # the guest view even though its exact PID/start-time are visible.
            (.pgid | integer);
        def outer_trampoline_kill($cgroup_path):
            ((keys | sort) == ([
                "active_cgroup_members", "adapter", "baseline_cgroup_members",
                "cgroup_restored", "child_identity_absent", "execution_id",
                "exit_code", "identity", "leader_identity_absent",
                "marker_removed", "outer_identity", "outer_identity_absent",
                "post_case_probe", "session_reaped"
            ] | sort)) and
            (.adapter == "streaming") and
            (.execution_id == "exec-supervision-outer-trampoline-kill") and
            (.exit_code == 137) and
            (.identity | identity) and
            (.identity.cgroup_path == $cgroup_path) and
            (.outer_identity | outer_identity) and
            (.outer_identity.pid != .identity.host_pid) and
            (.outer_identity.pid != .identity.child_host_pid) and
            ((.baseline_cgroup_members | type) == "array") and
            ((.baseline_cgroup_members | length) == 1) and
            (all(.baseline_cgroup_members[]; member)) and
            ((.active_cgroup_members | type) == "array") and
            ((.active_cgroup_members | length) > (.baseline_cgroup_members | length)) and
            (all(.active_cgroup_members[]; member)) and
            (.outer_identity.pid as $pid |
                all(.active_cgroup_members[]; .pid != $pid)) and
            (.identity.host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.identity.child_host_pid as $pid |
                any(.active_cgroup_members[]; .pid == $pid)) and
            (.cgroup_restored == true) and
            (.outer_identity_absent == true) and
            (.leader_identity_absent == true) and
            (.child_identity_absent == true) and
            (.session_reaped == true) and
            (.marker_removed == true) and
            (.post_case_probe == true);

        .cgroup_path as $cgroup_path |
        (type == "object") and
        ((keys | sort) == ([
            "build", "cancel_before_ready", "cancellation", "cgroup_path",
            "container_id", "dropped_future", "final", "matrix", "normal_exit",
            "outer_trampoline_kill", "pre_spawn_rejection",
            "ready_before_owner_aborts", "response_loss_before_ready", "scenario",
            "schema_version", "slow_live_consumer"
        ] | sort)) and
        (.schema_version == 4) and
        (.scenario == "runtime-exec-supervision") and
        (.build | build_identity) and
        (.container_id == "exec-supervision-e2e") and
        ((.cgroup_path | type) == "string") and
        (.cgroup_path | startswith("/")) and
        ((.matrix | type) == "array") and
        ((.matrix | length) == 12) and
        ([.matrix[] | (.adapter + "-" + .termination)] | sort) == ([
            "pty-int", "pty-kill", "pty-term", "pty-timeout",
            "streaming-int", "streaming-kill", "streaming-term", "streaming-timeout",
            "unary-int", "unary-kill", "unary-term", "unary-timeout"
        ] | sort) and
        (all(.matrix[]; cell($cgroup_path))) and
        (.normal_exit | normal_exit($cgroup_path)) and
        (.cancellation | cancellation($cgroup_path)) and
        (.cancel_before_ready | cancel_before_ready) and
        (.pre_spawn_rejection | pre_spawn_rejection) and
        (.slow_live_consumer | slow_live_consumer) and
        (.response_loss_before_ready | response_loss_before_ready($cgroup_path)) and
        (.dropped_future | dropped_future($cgroup_path)) and
        ((.ready_before_owner_aborts | type) == "array") and
        ((.ready_before_owner_aborts | length) == 2) and
        ([.ready_before_owner_aborts[].case] | sort) == ([
            "ready-before-owner-anonymous", "ready-before-owner-named"
        ] | sort) and
        (all(.ready_before_owner_aborts[]; ready_before_owner_abort($cgroup_path))) and
        (.outer_trampoline_kill | outer_trampoline_kill($cgroup_path)) and
        ((.final | keys | sort) == ([
            "diagnostics", "metadata_absent", "rootfs_absent",
            "tracked_container_absent", "zero_leaks"
        ] | sort)) and
        (.final.zero_leaks == true) and
        (.final.tracked_container_absent == true) and
        (.final.metadata_absent == true) and
        (.final.rootfs_absent == true) and
        ((.final.diagnostics | type) == "string") and
        ((.final.diagnostics | length) > 0)
    ' "$evidence_file" >/dev/null
}

validate_stack_teardown_evidence() {
    local evidence_file="$1"

    jq -e '
        def nonempty_string: type == "string" and length > 0;
        def generation_scope:
            (type == "object") and
            ((keys | sort) == ([
                "environment_id", "machine_id", "machine_incarnation_id", "project_id",
                "reservation_id", "stack_id"
            ] | sort)) and
            (.reservation_id | type == "string" and
                test("^vzscr2-sha256:[0-9a-f]{64}$")) and
            (.project_id | nonempty_string) and
            (.environment_id | nonempty_string) and
            (.machine_id | nonempty_string) and
            (.machine_incarnation_id | nonempty_string) and
            (.stack_id | nonempty_string);
        .container_ids as $container_ids |
        (type == "object") and
        ((keys | sort) == ([
            "active", "after_service_down", "after_vm_shutdown", "before",
            "container_ids", "host_listener", "operations", "scenario",
            "schema_version", "stack_id"
        ] | sort)) and
        (.schema_version == 2) and
        (.scenario == "stack-port-forwarding-teardown") and
        (.stack_id == "port-fwd") and
        ((.host_listener | keys | sort) == ([
            "address", "free_before_start", "owned_after_service_down",
            "owned_while_active", "port", "rebound_after_vm_shutdown"
        ] | sort)) and
        (.host_listener.address == "127.0.0.1") and
        ((.host_listener.port | type) == "number") and
        (.host_listener.port == (.host_listener.port | floor)) and
        (.host_listener.port > 0 and .host_listener.port <= 65535) and
        (.host_listener.free_before_start == true) and
        (.host_listener.owned_while_active == true) and
        (.host_listener.owned_after_service_down == true) and
        (.host_listener.rebound_after_vm_shutdown == true) and
        ((.operations | keys | sort) == (["down", "shutdown", "up"] | sort)) and
        (all(.operations[];
            ((keys | sort) == (["error", "succeeded"] | sort)) and
            .succeeded == true and .error == null
        )) and
        ((.container_ids | type) == "array") and
        ((.container_ids | length) == 2) and
        ((.container_ids | unique | length) == 2) and
        (all(.container_ids[]; type == "string" and length > 0)) and
        (all([.before, .active, .after_service_down, .after_vm_shutdown][];
            ((keys | sort) == (["lifecycle", "tracked_container_ids"] | sort)) and
            ((.tracked_container_ids | type) == "array") and
            ((.tracked_container_ids | unique | length) == (.tracked_container_ids | length)) and
            (all(.tracked_container_ids[]; type == "string" and length > 0)) and
            ((.lifecycle | keys | sort) == ([
                "active_lifecycles", "container_lock_slots", "container_route_pairs",
                "container_routes", "exec_bindings", "exec_sessions", "generations",
                "overlay_cleanup_pending", "rootfs_directories", "setup_restore_entries",
                "stack_lock_slots", "stack_port_forward_ids", "stack_port_forwards",
                "stack_vm_ids", "stack_vms", "vm_handle_ids", "vm_handles"
            ] | sort)) and
            ([
                .lifecycle.active_lifecycles, .lifecycle.container_lock_slots,
                .lifecycle.container_routes, .lifecycle.exec_bindings,
                .lifecycle.exec_sessions, .lifecycle.overlay_cleanup_pending,
                .lifecycle.rootfs_directories, .lifecycle.setup_restore_entries,
                .lifecycle.stack_lock_slots, .lifecycle.stack_port_forwards,
                .lifecycle.stack_vms, .lifecycle.vm_handles
            ] | all(type == "number" and . >= 0 and . == floor)) and
            ([
                .lifecycle.vm_handle_ids[], .lifecycle.stack_vm_ids[],
                .lifecycle.stack_port_forward_ids[]
            ] | all(type == "string" and length > 0)) and
            ((.lifecycle.vm_handle_ids | unique | length) == (.lifecycle.vm_handle_ids | length)) and
            ((.lifecycle.stack_vm_ids | unique | length) == (.lifecycle.stack_vm_ids | length)) and
            ((.lifecycle.stack_port_forward_ids | unique | length) == (.lifecycle.stack_port_forward_ids | length)) and
            (.lifecycle.vm_handles == (.lifecycle.vm_handle_ids | length)) and
            (.lifecycle.stack_vms == (.lifecycle.stack_vm_ids | length)) and
            (.lifecycle.container_routes == (.lifecycle.container_route_pairs | length)) and
            (.lifecycle.stack_port_forwards == (.lifecycle.stack_port_forward_ids | length)) and
            (all(.lifecycle.generations[];
                ((keys | sort) == ([
                    "container_id", "generation", "owner_alive", "owner_pid", "quarantined",
                    "reserved", "scope"
                ] | sort)) and
                ((.container_id | type) == "string") and
                ((.generation | type) == "number") and
                (.generation > 0 and .generation == (.generation | floor)) and
                ((.reserved | type) == "boolean") and
                ((.owner_pid | type) == "number") and
                (.owner_pid >= 0 and .owner_pid == (.owner_pid | floor)) and
                ((.owner_alive | type) == "boolean") and
                ((.quarantined | type) == "boolean") and
                ((.scope == null) or (.scope | generation_scope)) and
                (.quarantined == (.reserved and (.scope == null)))
            )) and
            ((.lifecycle.generations | map(.container_id) | unique | length) ==
                (.lifecycle.generations | length))
        )) and
        (.before.tracked_container_ids == []) and
        (.before.lifecycle.vm_handles == 0) and
        (.before.lifecycle.vm_handle_ids == []) and
        (.before.lifecycle.stack_vms == 0) and
        (.before.lifecycle.stack_vm_ids == []) and
        (.before.lifecycle.container_routes == 0) and
        (.before.lifecycle.container_route_pairs == []) and
        (.before.lifecycle.stack_port_forwards == 0) and
        (.before.lifecycle.stack_port_forward_ids == []) and
        ((.active.tracked_container_ids | sort) == (.container_ids | sort)) and
        (.active.lifecycle.vm_handles == 2) and
        ((.active.lifecycle.vm_handle_ids | sort) == (.container_ids | sort)) and
        (.active.lifecycle.stack_vms == 1) and
        (.active.lifecycle.stack_vm_ids == ["port-fwd"]) and
        (.active.lifecycle.container_routes == 2) and
        ((.active.lifecycle.container_route_pairs | length) == 2) and
        ((.active.lifecycle.container_route_pairs | map(.[0]) | sort) ==
            ($container_ids | sort)) and
        (all(.active.lifecycle.container_route_pairs[];
            (type == "array") and (length == 2) and
            ((.[0] | type) == "string") and ((.[1] | type) == "string") and
            (.[0] as $container_id |
                .[1] == "port-fwd" and any($container_ids[]; . == $container_id))
        )) and
        (.active.lifecycle.stack_port_forwards == 1) and
        (.active.lifecycle.stack_port_forward_ids == ["port-fwd"]) and
        ([.container_ids[] as $container_id |
            any(.active.lifecycle.generations[];
                .container_id == $container_id and .reserved == true and .generation > 0 and
                .scope.stack_id == "port-fwd" and .quarantined == false)
        ] | all) and
        (([.active.lifecycle.generations[] | select(.reserved) | .container_id] | sort) ==
            ($container_ids | sort)) and
        (.after_service_down.tracked_container_ids == []) and
        (.after_service_down.lifecycle.vm_handles == 0) and
        (.after_service_down.lifecycle.vm_handle_ids == []) and
        (.after_service_down.lifecycle.container_routes == 0) and
        (.after_service_down.lifecycle.container_route_pairs == []) and
        (.after_service_down.lifecycle.exec_bindings == 0) and
        (.after_service_down.lifecycle.active_lifecycles == 0) and
        (.after_service_down.lifecycle.stack_vms == 1) and
        (.after_service_down.lifecycle.stack_vm_ids == ["port-fwd"]) and
        (.after_service_down.lifecycle.stack_port_forwards == 1) and
        (.after_service_down.lifecycle.stack_port_forward_ids == ["port-fwd"]) and
        ([.container_ids[] as $container_id |
            any(.after_service_down.lifecycle.generations[];
                .container_id == $container_id and .reserved == false and .generation > 0 and
                .scope.stack_id == "port-fwd" and .quarantined == false)
        ] | all) and
        (all(.after_service_down.lifecycle.generations[]; .reserved == false)) and
        (.after_vm_shutdown.tracked_container_ids == []) and
        (.after_vm_shutdown.lifecycle.vm_handles == 0) and
        (.after_vm_shutdown.lifecycle.vm_handle_ids == []) and
        (.after_vm_shutdown.lifecycle.stack_vms == 0) and
        (.after_vm_shutdown.lifecycle.stack_vm_ids == []) and
        (.after_vm_shutdown.lifecycle.container_routes == 0) and
        (.after_vm_shutdown.lifecycle.container_route_pairs == []) and
        (.after_vm_shutdown.lifecycle.stack_port_forwards == 0) and
        (.after_vm_shutdown.lifecycle.stack_port_forward_ids == []) and
        (.after_vm_shutdown.lifecycle.exec_bindings == 0) and
        (.after_vm_shutdown.lifecycle.active_lifecycles == 0) and
        (.after_vm_shutdown.lifecycle.exec_sessions == 0) and
        (.after_vm_shutdown.lifecycle.setup_restore_entries == 0) and
        (.after_vm_shutdown.lifecycle.overlay_cleanup_pending == 0) and
        (.after_vm_shutdown.lifecycle.rootfs_directories == 0) and
        (all(.after_vm_shutdown.lifecycle.generations[]; .reserved == false)) and
        ((.after_vm_shutdown.lifecycle.generations |
            map(select(.container_id as $container_id |
                any($container_ids[]; . == $container_id)))) as $selected_generations |
            ($selected_generations | length) == 2 and
            all($selected_generations[];
                .reserved == false and .owner_alive == false and .generation > 0 and
                .scope.stack_id == "port-fwd" and .quarantined == false))
    ' "$evidence_file" >/dev/null
}

validate_vm_full_unsupported_evidence() {
    local evidence_file="$1"
    local expected_reason='vm_full_checkpoint=false: shared VM state depends on external VirtioFS/device state that is not captured atomically'

    jq -e --arg expected_reason "$expected_reason" '
        def exact_keys($expected): (keys | sort) == ($expected | sort);
        def nonnegative_integer:
            type == "number" and . >= 0 and . == floor;
        def positive_integer:
            type == "number" and . > 0 and . == floor;
        def nonempty_string: type == "string" and length > 0;
        def generation_scope:
            (type == "object") and
            exact_keys([
                "environment_id", "machine_id", "machine_incarnation_id", "project_id",
                "reservation_id", "stack_id"
            ]) and
            (.reservation_id | type == "string" and
                test("^vzscr2-sha256:[0-9a-f]{64}$")) and
            (.project_id | nonempty_string) and
            (.environment_id | nonempty_string) and
            (.machine_id | nonempty_string) and
            (.machine_incarnation_id | nonempty_string) and
            (.stack_id | nonempty_string);
        def service_ids:
            (type == "object") and
            exact_keys(["api", "cache", "db"]) and
            (all(.[]; type == "string" and length > 0)) and
            (([.[]] | unique | length) == 3);
        def guest_identity:
            (type == "object") and
            exact_keys([
                "boot_id", "cgroup_identity", "cgroup_path", "guest_init_pid",
                "ipc_identity", "mnt_identity", "net_identity", "owner",
                "pid_identity", "root_identity", "start_time", "uts_identity"
            ]) and
            ((.owner | type) == "string") and
            ((.boot_id | type) == "string" and length > 0) and
            (.guest_init_pid | positive_integer) and
            (all([
                .start_time, .cgroup_path, .cgroup_identity, .mnt_identity,
                .net_identity, .pid_identity, .ipc_identity, .uts_identity,
                .root_identity
            ][]; type == "string" and length > 0));
        def lifecycle:
            (type == "object") and
            exact_keys([
                "active_lifecycles", "container_lock_slots", "container_route_pairs",
                "container_routes", "exec_bindings", "exec_sessions", "generations",
                "overlay_cleanup_pending", "rootfs_directories", "setup_restore_entries",
                "stack_lock_slots", "stack_port_forward_ids", "stack_port_forwards",
                "stack_vm_ids", "stack_vms", "vm_handle_ids", "vm_handles"
            ]) and
            (all([
                .active_lifecycles, .container_lock_slots, .container_routes,
                .exec_bindings, .exec_sessions, .overlay_cleanup_pending,
                .rootfs_directories, .setup_restore_entries, .stack_lock_slots,
                .stack_port_forwards, .stack_vms, .vm_handles
            ][]; nonnegative_integer)) and
            ((.container_route_pairs | type) == "array") and
            ((.stack_port_forward_ids | type) == "array") and
            ((.stack_vm_ids | type) == "array") and
            ((.vm_handle_ids | type) == "array") and
            (.container_routes == (.container_route_pairs | length)) and
            (.stack_port_forwards == (.stack_port_forward_ids | length)) and
            (.stack_vms == (.stack_vm_ids | length)) and
            (.vm_handles == (.vm_handle_ids | length)) and
            (all(.generations[];
                (type == "object") and
                exact_keys([
                    "container_id", "generation", "owner_alive", "owner_pid", "quarantined",
                    "reserved", "scope"
                ]) and
                ((.container_id | type) == "string" and length > 0) and
                (.generation | positive_integer) and
                ((.owner_alive | type) == "boolean") and
                (.owner_pid | nonnegative_integer) and
                ((.reserved | type) == "boolean") and
                ((.quarantined | type) == "boolean") and
                ((.scope == null) or (.scope | generation_scope)) and
                (.quarantined == (.reserved and (.scope == null)))
            ));
        def probe($service; $container_id):
            (type == "object") and
            exact_keys(["command", "container_id", "exit_code", "semantic_output_ok", "stderr", "stdout"]) and
            (.container_id == $container_id) and
            (.exit_code == 0) and
            (.semantic_output_ok == true) and
            (.stderr == "") and
            (if $service == "api" then
                .command == ["/bin/sh", "-c", "printf vz-api-snapshot-probe"] and
                .stdout == "vz-api-snapshot-probe"
             elif $service == "cache" then
                .command == ["redis-cli", "ping"] and .stdout == "PONG\n"
             else
                .command == ["pg_isready", "-U", "app"] and
                (.stdout | contains("accepting connections"))
             end);
        def unsupported_operation($operation):
            (type == "object") and
            exact_keys(["error_variant", "invocations", "operation", "reason"]) and
            (.error_variant == "UnsupportedOperation") and
            (.invocations == 1) and
            (.operation == $operation) and
            (.reason == $expected_reason);

        .scenario.service_container_ids.before as $ids |
        ([ $ids[] ] | sort) as $container_ids |
        (type == "object") and
        exact_keys(["cleanup", "scenario", "snapshot_destination_absent_after_cleanup"]) and
        (.snapshot_destination_absent_after_cleanup == true) and

        (.scenario | exact_keys([
            "guest_generation_identities", "runtime", "scenario", "schema_version",
            "service_container_ids", "service_probes", "snapshot_destination",
            "stack_id", "vm_full_operations"
        ])) and
        (.scenario.schema_version == 2) and
        (.scenario.scenario == "complex_stack_vm_full_snapshot_fails_closed_without_mutation") and
        (.scenario.stack_id == "snapshot-stack") and
        (.scenario.service_container_ids | exact_keys(["after", "before"])) and
        (.scenario.service_container_ids.before | service_ids) and
        (.scenario.service_container_ids.after == $ids) and

        (.scenario.service_probes | exact_keys(["after", "before"])) and
        (all([.scenario.service_probes.before, .scenario.service_probes.after][];
            exact_keys(["api", "cache", "db"]))) and
        (.scenario.service_probes.before.api | probe("api"; $ids.api)) and
        (.scenario.service_probes.before.cache | probe("cache"; $ids.cache)) and
        (.scenario.service_probes.before.db | probe("db"; $ids.db)) and
        (.scenario.service_probes.after.api | probe("api"; $ids.api)) and
        (.scenario.service_probes.after.cache | probe("cache"; $ids.cache)) and
        (.scenario.service_probes.after.db | probe("db"; $ids.db)) and

        (.scenario.guest_generation_identities | exact_keys(["after", "before"])) and
        (all([.scenario.guest_generation_identities.before, .scenario.guest_generation_identities.after][];
            exact_keys(["api", "cache", "db"]) and all(.[]; guest_identity))) and
        (.scenario.guest_generation_identities.after == .scenario.guest_generation_identities.before) and
        ([.scenario.guest_generation_identities.before[].boot_id] | unique | length) == 1 and
        (.scenario.guest_generation_identities.before.api.cgroup_path | contains($ids.api)) and
        (.scenario.guest_generation_identities.before.cache.cgroup_path | contains($ids.cache)) and
        (.scenario.guest_generation_identities.before.db.cgroup_path | contains($ids.db)) and

        (.scenario.runtime | exact_keys([
            "lifecycle_after", "lifecycle_before", "tracked_container_ids_after",
            "tracked_container_ids_before"
        ])) and
        ((.scenario.runtime.tracked_container_ids_before | sort) == $container_ids) and
        (.scenario.runtime.tracked_container_ids_after == .scenario.runtime.tracked_container_ids_before) and
        (.scenario.runtime.lifecycle_before | lifecycle) and
        (.scenario.runtime.lifecycle_after == .scenario.runtime.lifecycle_before) and
        (.scenario.runtime.lifecycle_before.active_lifecycles == 3) and
        (.scenario.runtime.lifecycle_before.container_routes == 3) and
        (.scenario.runtime.lifecycle_before.exec_bindings == 3) and
        (.scenario.runtime.lifecycle_before.exec_sessions == 0) and
        (.scenario.runtime.lifecycle_before.overlay_cleanup_pending == 0) and
        (.scenario.runtime.lifecycle_before.rootfs_directories == 3) and
        (.scenario.runtime.lifecycle_before.setup_restore_entries == 0) and
        (.scenario.runtime.lifecycle_before.stack_port_forwards == 0) and
        (.scenario.runtime.lifecycle_before.stack_vms == 1) and
        (.scenario.runtime.lifecycle_before.stack_vm_ids == ["snapshot-stack"]) and
        (.scenario.runtime.lifecycle_before.vm_handles == 3) and
        ((.scenario.runtime.lifecycle_before.vm_handle_ids | sort) == $container_ids) and
        ((.scenario.runtime.lifecycle_before.generations | map(.container_id) | sort) == $container_ids) and
        (all(.scenario.runtime.lifecycle_before.generations[];
            .reserved == true and .owner_alive == true and
            .scope.stack_id == "snapshot-stack" and .quarantined == false)) and
        (all(.scenario.runtime.lifecycle_before.container_route_pairs[];
            . as $route |
            ($route | type) == "array" and ($route | length) == 2 and
            $route[1] == "snapshot-stack" and
            any($container_ids[]; . == $route[0]))) and

        (.scenario.vm_full_operations | exact_keys(["restore", "save"])) and
        (.scenario.vm_full_operations.save | unsupported_operation("create_checkpoint")) and
        (.scenario.vm_full_operations.restore | unsupported_operation("restore_checkpoint")) and
        (.scenario.snapshot_destination | exact_keys([
            "absent_after_restore", "absent_after_save", "absent_before", "path"
        ])) and
        (.scenario.snapshot_destination.absent_before == true) and
        (.scenario.snapshot_destination.absent_after_save == true) and
        (.scenario.snapshot_destination.absent_after_restore == true) and
        ((.scenario.snapshot_destination.path | type) == "string") and
        (.scenario.snapshot_destination.path | endswith("/snapshot-stack.state")) and

        (.cleanup | exact_keys([
            "baseline_tracked_container_ids", "exact_owned_container_ids",
            "final_lifecycle", "final_tracked_container_ids", "sandbox_active",
            "stack_id", "zero_inventory"
        ])) and
        (.cleanup.stack_id == "snapshot-stack") and
        (.cleanup.baseline_tracked_container_ids == []) and
        ((.cleanup.exact_owned_container_ids | sort) == $container_ids) and
        (.cleanup.final_tracked_container_ids == []) and
        (.cleanup.sandbox_active == false) and
        (.cleanup.zero_inventory == true) and
        (.cleanup.final_lifecycle | lifecycle) and
        ([
            .cleanup.final_lifecycle.active_lifecycles,
            .cleanup.final_lifecycle.container_routes,
            .cleanup.final_lifecycle.exec_bindings,
            .cleanup.final_lifecycle.exec_sessions,
            .cleanup.final_lifecycle.overlay_cleanup_pending,
            .cleanup.final_lifecycle.rootfs_directories,
            .cleanup.final_lifecycle.setup_restore_entries,
            .cleanup.final_lifecycle.stack_port_forwards,
            .cleanup.final_lifecycle.stack_vms,
            .cleanup.final_lifecycle.vm_handles
        ] | all(. == 0)) and
        (.cleanup.final_lifecycle.container_route_pairs == []) and
        (.cleanup.final_lifecycle.stack_port_forward_ids == []) and
        (.cleanup.final_lifecycle.stack_vm_ids == []) and
        (.cleanup.final_lifecycle.vm_handle_ids == []) and
        ((.cleanup.final_lifecycle.generations | map(.container_id) | sort) == $container_ids) and
        (all(.cleanup.final_lifecycle.generations[];
            .reserved == false and .owner_alive == false and
            .scope.stack_id == "snapshot-stack" and .quarantined == false))
    ' "$evidence_file" >/dev/null
}

extract_vm_full_unsupported_evidence() {
    local log_file="$1"
    local evidence_file="$2"
    local marker='VZ_STACK_VM_FULL_UNSUPPORTED_EVIDENCE:'
    local temporary="${evidence_file}.tmp"
    local sentinel_count

    rm -f "$temporary" "$evidence_file"
    sentinel_count="$(grep -F -c "$marker" "$log_file" || true)"
    if [[ "$sentinel_count" != "1" ]]; then
        echo "expected exactly one VM-full unsupported evidence sentinel, found $sentinel_count" >&2
        return 1
    fi
    awk -v marker="$marker" '
        index($0, marker) {
            print substr($0, index($0, marker) + length(marker))
        }
    ' "$log_file" > "$temporary"
    if [[ "$(wc -l < "$temporary" | tr -d '[:space:]')" != "1" ]] \
        || ! validate_vm_full_unsupported_evidence "$temporary"; then
        echo "VM-full unsupported evidence is malformed or violates the strict schema" >&2
        rm -f "$temporary"
        return 1
    fi
    mv "$temporary" "$evidence_file"
}

validate_stack_container_ownership_evidence() {
    local evidence_file="$1"
    local expected_profile="$2"
    local expected_test_binary_sha256="$3"

    jq -e \
        --arg expected_profile "$expected_profile" \
        --arg expected_test_binary_sha256 "$expected_test_binary_sha256" '
        def nonempty_string: type == "string" and length > 0;
        def sha256: type == "string" and test("^[0-9a-f]{64}$");
        def generation_scope:
            (type == "object") and
            ((keys | sort) == ([
                "environment_id", "machine_id", "machine_incarnation_id", "project_id",
                "reservation_id", "stack_id"
            ] | sort)) and
            (.reservation_id | type == "string" and
                test("^vzscr2-sha256:[0-9a-f]{64}$")) and
            (.project_id | nonempty_string) and
            (.environment_id | nonempty_string) and
            (.machine_id | nonempty_string) and
            (.machine_incarnation_id | nonempty_string) and
            (.stack_id | nonempty_string) and
            (.project_id == "prj_stack_fixture") and
            (.environment_id == "env_stack_fixture") and
            (.machine_id == "mch_stack_fixture") and
            (.machine_incarnation_id == "inc_stack_fixture");
        def workload_scope:
            (type == "object") and
            ((keys | sort) == ([
                "environment_id", "machine_id", "machine_incarnation_id", "project_id",
                "schema_version", "stack_id"
            ] | sort)) and
            (.schema_version == 1) and
            (.project_id == "prj_stack_fixture") and
            (.environment_id == "env_stack_fixture") and
            (.machine_id == "mch_stack_fixture") and
            (.machine_incarnation_id == "inc_stack_fixture") and
            (.stack_id | nonempty_string);
        def ownership:
            ((keys | sort) == (["container_id", "generation", "scope", "stack_id"] | sort)) and
            ((.container_id | type) == "string" and (.container_id | length) > 0) and
            ((.generation | type) == "number" and .generation > 0 and
                .generation == (.generation | floor)) and
            ((.stack_id | type) == "string" and (.stack_id | length) > 0) and
            (.scope | generation_scope) and
            (.scope.stack_id == .stack_id);
        def guest:
            ((keys | sort) == ([
                "boot_id", "cgroup_identity", "cgroup_path", "guest_init_pid",
                "ipc_identity", "mnt_identity", "net_identity", "owner",
                "pid_identity", "root_identity", "start_time", "uts_identity"
            ] | sort)) and
            ((.owner | type) == "string" and (.owner | length) > 0) and
            ((.boot_id | type) == "string" and (.boot_id | length) > 0) and
            ((.guest_init_pid | type) == "number" and .guest_init_pid > 0 and
                .guest_init_pid == (.guest_init_pid | floor)) and
            (all([
                .start_time, .cgroup_path, .cgroup_identity, .mnt_identity,
                .net_identity, .pid_identity, .ipc_identity, .uts_identity,
                .root_identity
            ][]; type == "string" and length > 0));
        def lifecycle:
            ((keys | sort) == ([
                "active_lifecycles", "container_lock_slots", "container_route_pairs",
                "container_routes", "exec_bindings", "exec_sessions", "generations",
                "overlay_cleanup_pending", "rootfs_directories", "setup_restore_entries",
                "stack_lock_slots", "stack_port_forward_ids", "stack_port_forwards",
                "stack_vm_ids", "stack_vms", "vm_handle_ids", "vm_handles"
            ] | sort)) and
            (all([
                .active_lifecycles, .container_lock_slots, .container_routes,
                .exec_bindings, .exec_sessions, .overlay_cleanup_pending,
                .rootfs_directories, .setup_restore_entries, .stack_lock_slots,
                .stack_port_forwards, .stack_vms, .vm_handles
            ][]; type == "number" and . >= 0 and . == floor)) and
            (.container_routes == (.container_route_pairs | length)) and
            (.stack_port_forwards == (.stack_port_forward_ids | length)) and
            (.stack_vms == (.stack_vm_ids | length)) and
            (.vm_handles == (.vm_handle_ids | length)) and
            (all(.container_route_pairs[];
                type == "array" and length == 2 and
                all(.[]; type == "string" and length > 0))) and
            (all(.generations[];
                ((keys | sort) == ([
                    "container_id", "generation", "owner_alive", "owner_pid", "quarantined",
                    "reserved", "scope"
                ] | sort)) and
                ((.container_id | type) == "string" and (.container_id | length) > 0) and
                ((.generation | type) == "number" and .generation > 0 and
                    .generation == (.generation | floor)) and
                ((.owner_pid | type) == "number" and .owner_pid >= 0 and
                    .owner_pid == (.owner_pid | floor)) and
                ((.reserved | type) == "boolean") and
                ((.owner_alive | type) == "boolean") and
                ((.quarantined | type) == "boolean") and
                ((.scope == null) or (.scope | generation_scope)) and
                (.quarantined == (.reserved and (.scope == null))))) and
            ((.generations | map(.container_id) | unique | length) == (.generations | length));

        (type == "object") and
        ((keys | sort) == ([
            "build_identity", "concurrent_same_service", "final", "foreign_collision",
            "owned_failure", "scenario", "schema_version", "scope_identity"
        ] | sort)) and
        (.schema_version == 4) and
        (.scenario == "stack-container-ownership") and
        ((.build_identity | keys | sort) == (["profile", "test_binary_sha256"] | sort)) and
        (.build_identity.profile == "release") and
        (.build_identity.profile == $expected_profile) and
        (.build_identity.test_binary_sha256 | sha256) and
        (.build_identity.test_binary_sha256 == $expected_test_binary_sha256) and
        ((.scope_identity | keys | sort) == ([
            "kind", "topology_authoritative", "workloads"
        ] | sort)) and
        (.scope_identity.kind == "machine_workload_scope") and
        (.scope_identity.topology_authoritative == true) and
        ((.scope_identity.workloads | type) == "array") and
        ((.scope_identity.workloads | length) == 5) and
        ((.scope_identity.workloads | map(.stack_id) | unique | length) == 5) and
        (all(.scope_identity.workloads[]; . | workload_scope)) and
        ((.scope_identity.workloads | map(.stack_id) | sort) == [
            "foreign-contender", "foreign-owner", "owned", "same-a", "same-b"
        ]) and

        (.concurrent_same_service as $same |
            (($same | keys | sort) == (["barrier", "lifecycle", "service_name", "stacks"] | sort)) and
            ($same.service_name == "db") and
            (($same.barrier | keys | sort) == (["both_reached_before_release", "container_ids", "kind"] | sort)) and
            ($same.barrier.kind == "create_before_reservation") and
            ($same.barrier.both_reached_before_release == true) and
            (($same.stacks | type) == "array" and ($same.stacks | length) == 2) and
            (($same.stacks | map(.stack_id) | sort) == ["same-a", "same-b"]) and
            (($same.stacks | map(.container_id) | unique | length) == 2) and
            (($same.barrier.container_ids | sort) == ($same.stacks | map(.container_id) | sort)) and
            ($same.lifecycle | lifecycle) and
            (all($same.stacks[];
                ((keys | sort) == (["container_id", "guest", "ownership", "stack_id"] | sort)) and
                (.ownership | ownership) and (.guest | guest) and
                (.container_id == .ownership.container_id) and
                (.stack_id == .ownership.stack_id) and
                (.container_id | startswith("vzs1-")) and
                (.guest.owner == (.stack_id + "-db")) and
                (.container_id as $id | .stack_id as $stack |
                    any($same.lifecycle.container_route_pairs[]; . == [$id, $stack])) and
                (.ownership as $token |
                    any($same.lifecycle.generations[];
                        .container_id == $token.container_id and
                        .generation == $token.generation and
                        .scope == $token.scope and
                        .reserved == true and .quarantined == false))))
        ) and

        (.owned_failure as $owned |
            (($owned | keys | sort) == ([
                "after_remove_before_recreate", "cleanup_operations", "failed_guest",
                "failed_lifecycle", "failure_token", "injected_error_code",
                "injection_point", "journal_token", "replacement_guest",
                "replacement_lifecycle", "replacement_token", "service_name", "stack_id"
            ] | sort)) and
            ($owned.stack_id == "owned") and ($owned.service_name == "worker") and
            ($owned.injection_point == "after_runtime_publication_before_executor_finalize") and
            ($owned.injected_error_code == "injected_post_publication") and
            ($owned.failure_token | ownership) and ($owned.journal_token | ownership) and
            ($owned.failure_token == $owned.journal_token) and
            ($owned.failure_token.stack_id == $owned.stack_id) and
            ($owned.failed_guest | guest) and
            ($owned.failed_guest.owner == "owned-generation-a") and
            ($owned.failed_lifecycle | lifecycle) and
            ($owned.failure_token as $token |
                any($owned.failed_lifecycle.generations[];
                    .container_id == $token.container_id and
                    .generation == $token.generation and
                    .scope == $token.scope and
                    .reserved == false and .quarantined == false)) and
            ($owned.failure_token as $token |
                all($owned.failed_lifecycle.container_route_pairs[];
                    .[0] != $token.container_id)) and
            (($owned.cleanup_operations | length) == 1) and
            ($owned.cleanup_operations[0] as $cleanup |
                (($cleanup | keys | sort) == (["operation", "outcome", "ownership"] | sort)) and
                ($cleanup.operation == "cleanup_container_generation") and
                ($cleanup.outcome == "removed") and
                ($cleanup.ownership == $owned.failure_token)) and
            ($owned.after_remove_before_recreate as $removed |
                (($removed | keys | sort) == ([
                    "guest_cgroup_absent", "guest_overlay_absent", "guest_youki_state_absent",
                    "lifecycle", "metadata_absent", "rootfs_absent"
                ] | sort)) and
                ($removed.metadata_absent == true) and ($removed.rootfs_absent == true) and
                ($removed.guest_overlay_absent == true) and
                ($removed.guest_youki_state_absent == true) and
                ($removed.guest_cgroup_absent == true) and
                ($removed.lifecycle | lifecycle) and
                ($owned.failure_token as $token |
                    (all($removed.lifecycle.container_route_pairs[]; .[0] != $token.container_id)) and
                    any($removed.lifecycle.generations[];
                        .container_id == $token.container_id and
                        .generation == $token.generation and
                        .scope == $token.scope and
                        .reserved == false and .quarantined == false))) and
            ($owned.replacement_token | ownership) and ($owned.replacement_guest | guest) and
            ($owned.replacement_lifecycle | lifecycle) and
            ($owned.replacement_token.container_id == $owned.failure_token.container_id) and
            ($owned.replacement_token.stack_id == $owned.failure_token.stack_id) and
            ($owned.replacement_token.generation > $owned.failure_token.generation) and
            ($owned.replacement_token.scope.project_id == $owned.failure_token.scope.project_id) and
            ($owned.replacement_token.scope.environment_id == $owned.failure_token.scope.environment_id) and
            ($owned.replacement_token.scope.machine_id == $owned.failure_token.scope.machine_id) and
            ($owned.replacement_token.scope.machine_incarnation_id ==
                $owned.failure_token.scope.machine_incarnation_id) and
            ($owned.replacement_token.scope.stack_id == $owned.failure_token.scope.stack_id) and
            ($owned.replacement_token.scope.reservation_id !=
                $owned.failure_token.scope.reservation_id) and
            ($owned.replacement_guest.owner == "owned-generation-b") and
            ($owned.replacement_guest != $owned.failed_guest) and
            ($owned.replacement_token as $token |
                any($owned.replacement_lifecycle.container_route_pairs[];
                    . == [$token.container_id, $token.stack_id]) and
                any($owned.replacement_lifecycle.generations[];
                    .container_id == $token.container_id and
                    .generation == $token.generation and
                    .scope == $token.scope and
                    .reserved == true and .quarantined == false))
        ) and

        (.foreign_collision as $foreign |
            (($foreign | keys | sort) == ([
                "after_lifecycle", "before_lifecycle", "cleanup_operations",
                "collision_cleanup", "collision_error_code", "container_id",
                "contender_observed_cleanup", "contender_stack_id", "owner_after_guest",
                "owner_before_guest", "owner_stack_id", "owner_token"
            ] | sort)) and
            ($foreign.owner_stack_id == "foreign-owner") and
            ($foreign.contender_stack_id == "foreign-contender") and
            ($foreign.container_id == "shared-explicit-id") and
            ($foreign.owner_token | ownership) and
            ($foreign.owner_token.container_id == $foreign.container_id) and
            ($foreign.owner_token.stack_id == $foreign.owner_stack_id) and
            ($foreign.collision_error_code == "state_conflict") and
            ($foreign.collision_cleanup == null) and
            ($foreign.contender_observed_cleanup == null) and
            ($foreign.cleanup_operations == []) and
            ($foreign.owner_before_guest | guest) and ($foreign.owner_after_guest | guest) and
            ($foreign.owner_before_guest == $foreign.owner_after_guest) and
            ($foreign.owner_before_guest.owner == "foreign-owner") and
            ($foreign.before_lifecycle | lifecycle) and ($foreign.after_lifecycle | lifecycle) and
            ($foreign.owner_token as $token |
                any($foreign.before_lifecycle.container_route_pairs[];
                    . == [$token.container_id, $token.stack_id]) and
                any($foreign.before_lifecycle.generations[];
                    .container_id == $token.container_id and
                    .generation == $token.generation and
                    .scope == $token.scope and
                    .reserved == true and .quarantined == false) and
                any($foreign.after_lifecycle.container_route_pairs[];
                    . == [$token.container_id, $token.stack_id]) and
                all($foreign.after_lifecycle.container_route_pairs[];
                    .[0] != $token.container_id or .[1] != $foreign.contender_stack_id) and
                any($foreign.after_lifecycle.generations[];
                    .container_id == $token.container_id and
                    .generation == $token.generation and
                    .scope == $token.scope and
                    .reserved == true and .quarantined == false))
        ) and

        (.final as $final |
            (($final | keys | sort) == (["lifecycle", "tested_container_ids", "tracked_container_ids"] | sort)) and
            ($final.lifecycle | lifecycle) and
            ($final.tracked_container_ids == []) and
            (($final.tested_container_ids | type) == "array") and
            (($final.tested_container_ids | unique | length) == 4) and
            ($final.lifecycle.vm_handles == 0) and ($final.lifecycle.vm_handle_ids == []) and
            ($final.lifecycle.stack_vms == 0) and ($final.lifecycle.stack_vm_ids == []) and
            ($final.lifecycle.container_routes == 0) and ($final.lifecycle.container_route_pairs == []) and
            ($final.lifecycle.stack_port_forwards == 0) and
            ($final.lifecycle.stack_port_forward_ids == []) and
            ($final.lifecycle.exec_bindings == 0) and ($final.lifecycle.active_lifecycles == 0) and
            ($final.lifecycle.exec_sessions == 0) and ($final.lifecycle.setup_restore_entries == 0) and
            ($final.lifecycle.overlay_cleanup_pending == 0) and ($final.lifecycle.rootfs_directories == 0) and
            ([$final.tested_container_ids[] as $id |
                any($final.lifecycle.generations[];
                    .container_id == $id and .reserved == false and
                    .scope != null and .quarantined == false)] | all) and
            ([.concurrent_same_service.stacks[] as $stack |
                any($final.lifecycle.generations[];
                    .container_id == $stack.ownership.container_id and
                    .generation == $stack.ownership.generation and
                    .scope == $stack.ownership.scope and
                    .reserved == false and .quarantined == false)] | all) and
            (.owned_failure.replacement_token as $token |
                any($final.lifecycle.generations[];
                    .container_id == $token.container_id and
                    .generation == $token.generation and
                    .scope == $token.scope and
                    .reserved == false and .quarantined == false)) and
            (.foreign_collision.owner_token as $token |
                any($final.lifecycle.generations[];
                    .container_id == $token.container_id and
                    .generation == $token.generation and
                    .scope == $token.scope and
                    .reserved == false and .quarantined == false))
        )
    ' "$evidence_file" >/dev/null
}

validate_environment_lifecycle_evidence() {
    local evidence_file="$1"

    jq -e '
        def exact_keys($keys): (keys | sort) == ($keys | sort);
        def nonempty: type == "string" and length > 0;
        def sha256: type == "string" and test("^sha256:[0-9a-f]{64}$");
        def hex_sha256: type == "string" and test("^[0-9a-f]{64}$");
        def bounded_key: type == "string" and startswith("vzr1-") and length <= 64;

        (type == "object") and
        (exact_keys([
            "backend_invocations", "boot_ids", "controls", "final", "host_target",
            "ids", "operations", "phases", "reopen", "scenario", "schema_version",
            "sentinels", "sibling_isolation", "stop_replay"
        ])) and
        (.schema_version == 1) and
        (.scenario == "environment-lifecycle-journal-linux-vm") and

        (.host_target | exact_keys([
            "backend", "host_arch", "host_os", "machine_arch", "machine_os", "profile"
        ])) and
        (.host_target == {
            "host_os": "macos", "host_arch": "aarch64", "machine_os": "linux",
            "machine_arch": "aarch64", "profile": "developer",
            "backend": "macos_virtualization_linux"
        }) and

        (.ids | exact_keys(["project_id", "sibling", "target"])) and
        (.ids.project_id == "prj_lifecycle_mac_e2e") and
        (all([.ids.target, .ids.sibling][];
            exact_keys([
                "backend_key", "disk_resource_id", "environment_id", "incarnation_id",
                "machine_id"
            ]) and
            (.environment_id | test("^env_")) and
            (.machine_id | test("^mch_")) and
            (.incarnation_id | test("^inc_")) and
            (.backend_key | bounded_key) and
            (.disk_resource_id | bounded_key)
        )) and
        (.ids.target.environment_id != .ids.sibling.environment_id) and
        (.ids.target.machine_id != .ids.sibling.machine_id) and
        (.ids.target.incarnation_id != .ids.sibling.incarnation_id) and
        (.ids.target.backend_key != .ids.sibling.backend_key) and
        (.ids.target.disk_resource_id != .ids.sibling.disk_resource_id) and

        ((.operations | type) == "array") and
        ((.operations | length) == 6) and
        ((.operations | map(.operation_id) | unique | length) == 6) and
        ((.operations | map(.request_id) | unique | length) == 6) and
        ((.operations | map(.idempotency_key) | unique | length) == 6) and
        (all(.operations[];
            exact_keys([
                "definition_digest", "generation", "idempotency_key", "kind", "label",
                "operation_id", "plan_digest", "request_hash", "request_id", "status"
            ]) and
            (.operation_id | test("^lop_[A-Za-z0-9._:-]+$")) and
            (.generation | type == "number" and . > 0 and . == floor) and
            (.request_id | nonempty) and
            (.idempotency_key | nonempty) and
            (.request_hash | sha256) and
            (.definition_digest | sha256) and
            (.plan_digest | sha256) and
            (.status == "succeeded")
        )) and
        ((.operations | map([.label, .kind, .generation])) == [
            ["target_initial_up", "up", 1],
            ["sibling_initial_up", "up", 1],
            ["target_stop", "stop", 2],
            ["target_up_after_reopen", "up", 3],
            ["target_delete", "delete", 4],
            ["sibling_delete", "delete", 2]
        ]) and

        ((.phases | type) == "array") and
        ((.phases | map(.name)) == [
            "initial_up", "persistent_sentinels", "target_stop", "exact_stop_replay",
            "store_only_reopen", "target_up_after_reopen", "target_delete_reopen",
            "target_delete", "sibling_delete", "final_cleanup"
        ]) and
        (all(.phases[]; exact_keys(["name", "passed"]) and .passed == true)) and

        (.backend_invocations | exact_keys([
            "boot", "disk_already_absent", "disk_remove_attempts", "disk_removed",
            "shutdown", "stop_replay"
        ])) and
        (.backend_invocations == {
            "boot": 3, "shutdown": 3, "disk_remove_attempts": 3,
            "disk_removed": 2, "disk_already_absent": 1, "stop_replay": 0
        }) and

        (.stop_replay | exact_keys([
            "backend_invocations", "pending_steps", "same_generation", "same_operation",
            "same_plan_digest"
        ])) and
        (.stop_replay == {
            "same_operation": true, "same_generation": true, "same_plan_digest": true,
            "pending_steps": 0, "backend_invocations": 0
        }) and

        (.boot_ids | exact_keys([
            "sibling_after_target_delete", "sibling_initial", "target_after_reopen",
            "target_initial"
        ])) and
        (all(.boot_ids[]; nonempty)) and
        (.boot_ids.target_initial != .boot_ids.target_after_reopen) and
        (.boot_ids.sibling_initial == .boot_ids.sibling_after_target_delete) and

        (.sentinels | exact_keys([
            "sibling_persisted", "sibling_sha256", "target_persisted", "target_sha256"
        ])) and
        (.sentinels.target_sha256 | hex_sha256) and
        (.sentinels.sibling_sha256 | hex_sha256) and
        (.sentinels.target_persisted == true) and
        (.sentinels.sibling_persisted == true) and

        (.reopen | exact_keys([
            "delete_operation_byte_equal", "delete_plan_digest_equal", "disk_step_pending",
            "runtime_kept_alive", "runtime_reattachment_claimed", "store_only"
        ])) and
        (.reopen.store_only == true) and
        (.reopen.runtime_kept_alive == true) and
        (.reopen.runtime_reattachment_claimed == false) and
        (.reopen.delete_operation_byte_equal == true) and
        (.reopen.delete_plan_digest_equal == true) and
        (.reopen.disk_step_pending == true) and

        (.sibling_isolation | exact_keys([
            "aggregate_bytes_equal_after_delete", "aggregate_bytes_equal_after_restart",
            "aggregate_bytes_equal_after_stop", "live_after_target_delete",
            "live_after_target_restart", "live_during_target_stop",
            "ownership_bytes_equal_after_delete", "ownership_bytes_equal_after_restart",
            "ownership_bytes_equal_after_stop"
        ])) and
        (all(.sibling_isolation[]; . == true)) and

        (.final | exact_keys([
            "disk_count", "environment_rows", "operation_count", "ownership_rows",
            "runtime_active_lifecycles", "runtime_exec_sessions", "runtime_stack_vms",
            "runtime_vm_handles", "tombstone_count"
        ])) and
        (.final == {
            "tombstone_count": 2, "operation_count": 6, "environment_rows": 0,
            "ownership_rows": 0, "disk_count": 0, "runtime_vm_handles": 0,
            "runtime_stack_vms": 0, "runtime_active_lifecycles": 0,
            "runtime_exec_sessions": 0
        }) and

        (.controls | exact_keys(["fallbacks", "invocations", "retries"])) and
        (.controls == {"invocations": 1, "retries": 0, "fallbacks": 0})
    ' "$evidence_file" >/dev/null
}

validate_runtime_crash_reopen_evidence() {
    local evidence_file="$1"
    local expected_profile="$2"
    local expected_test_binary_sha256="$3"

    if ! jq -e '
        .coverage_classification == "runtime_store_post_commit_lost_ack_only" and
        .state_store_expectation == {
            "schema_version": 7,
            "status": "separate_companion_required",
            "required_boundary": "Action-v3 executor and StateStore atomic crash/reopen companion evidence"
        }
    ' "$evidence_file" >/dev/null; then
        echo "runtime generation crash/reopen evidence is incomplete: missing the required schema-v7 Action-v3 StateStore companion declaration" >&2
        return 1
    fi

    jq -e \
        --arg expected_profile "$expected_profile" \
        --arg expected_test_binary_sha256 "$expected_test_binary_sha256" '
        def exact_keys($keys): (keys | sort) == ($keys | sort);
        def nonempty: type == "string" and length > 0;
        def hex_sha256: type == "string" and test("^[0-9a-f]{64}$");
        def scope:
            exact_keys([
                "environment_id", "machine_id", "machine_incarnation_id",
                "project_id", "reservation_id", "stack_id"
            ]) and
            (all([
                .environment_id, .machine_id, .machine_incarnation_id,
                .project_id, .reservation_id, .stack_id
            ][]; nonempty));
        def raw_state:
            exact_keys(["containers", "generations", "routes"]) and
            ((.containers | type) == "array") and
            ((.generations | type) == "object") and
            ((.routes | type) == "array") and
            (all(.routes[]; type == "array" and length == 2 and all(.[]; nonempty)));
        def boundary:
            exact_keys([
                "boundary", "child", "container_id", "generation", "outcomes",
                "post_raw_state", "pre_raw_state", "prior_generation", "prior_scope", "scope"
            ]) and
            (.boundary | nonempty) and (.container_id | nonempty) and
            (.scope | scope) and
            (.generation | type == "number" and . > 0 and . == floor) and
            (.child == {"expected_exit_code": 137, "signal": "SIGKILL"}) and
            (.pre_raw_state | raw_state) and (.post_raw_state | raw_state) and
            (.outcomes | exact_keys([
                "cleanup_outcome", "exact_inspection", "foreign_inspection",
                "replacement_inspection", "same_reservation_generation"
            ])) and
            (.pre_raw_state.generations[.container_id].generation == .generation) and
            (.pre_raw_state.generations[.container_id].scope == .scope);

        (type == "object") and
        (exact_keys([
            "boundaries", "build_identity", "controls", "coverage_classification", "scenario",
            "schema_version", "state_store_expectation"
        ])) and
        (.schema_version == 1) and
        (.scenario == "runtime-generation-crash-reopen") and
        (.coverage_classification == "runtime_store_post_commit_lost_ack_only") and
        (.build_identity | exact_keys(["profile", "test_binary_sha256"])) and
        (.build_identity.profile == "release") and
        (.build_identity.profile == $expected_profile) and
        (.build_identity.test_binary_sha256 | hex_sha256) and
        (.build_identity.test_binary_sha256 == $expected_test_binary_sha256) and
        (.controls == {
            "harness_invocations": 1,
            "child_processes": 5,
            "retries": 0,
            "fallbacks": 0,
            "skips": 0
        }) and
        (.state_store_expectation == {
            "schema_version": 7,
            "status": "separate_companion_required",
            "required_boundary": "Action-v3 executor and StateStore atomic crash/reopen companion evidence"
        }) and
        ((.boundaries | length) == 5) and
        (all(.boundaries[]; boundary)) and
        ((.boundaries | map(.boundary) | sort) == ([
            "cleanup_completion", "metadata_publication", "replacement_persistence",
            "reservation_persistence", "route_publication"
        ] | sort)) and

        (.boundaries[] | select(.boundary == "reservation_persistence") as $case |
            $case.prior_scope == null and $case.prior_generation == null and
            $case.outcomes.exact_inspection == "reserved_unpublished" and
            $case.outcomes.same_reservation_generation == $case.generation and
            $case.outcomes.foreign_inspection == "foreign" and
            $case.outcomes.replacement_inspection == null and
            $case.outcomes.cleanup_outcome == null and
            $case.pre_raw_state.containers == [] and
            $case.post_raw_state.containers == [] and
            $case.post_raw_state.generations[$case.container_id].generation == $case.generation and
            $case.post_raw_state.generations[$case.container_id].scope == $case.scope and
            $case.post_raw_state.generations[$case.container_id].reserved == true) and

        (.boundaries[] | select(.boundary == "metadata_publication") as $case |
            $case.outcomes.exact_inspection == "published" and
            $case.outcomes.same_reservation_generation == null and
            $case.outcomes.foreign_inspection == null and
            $case.outcomes.replacement_inspection == null and
            $case.outcomes.cleanup_outcome == null and
            ($case.pre_raw_state.containers | length) == 1 and
            $case.pre_raw_state.containers[0].id == $case.container_id and
            $case.pre_raw_state.containers[0].status == "Created" and
            ($case.post_raw_state.containers | length) == 1 and
            $case.post_raw_state.containers[0].id == $case.container_id and
            ($case.post_raw_state.containers[0].status.Stopped.exit_code == -1)) and

        (.boundaries[] | select(.boundary == "route_publication") as $case |
            $case.outcomes.exact_inspection == "reserved_unpublished" and
            $case.outcomes.same_reservation_generation == $case.generation and
            $case.outcomes.foreign_inspection == "foreign" and
            $case.outcomes.replacement_inspection == null and
            $case.outcomes.cleanup_outcome == null and
            any($case.pre_raw_state.routes[]; . == [$case.container_id, $case.scope.stack_id]) and
            any($case.post_raw_state.routes[]; . == [$case.container_id, $case.scope.stack_id])) and

        (.boundaries[] | select(.boundary == "cleanup_completion") as $case |
            $case.outcomes.exact_inspection == "absent" and
            $case.outcomes.same_reservation_generation == null and
            $case.outcomes.foreign_inspection == null and
            $case.outcomes.replacement_inspection == null and
            $case.outcomes.cleanup_outcome == "removed" and
            $case.pre_raw_state.containers == [] and
            $case.post_raw_state.containers == [] and
            $case.pre_raw_state.generations[$case.container_id].reserved == false and
            $case.post_raw_state.generations[$case.container_id].reserved == false and
            $case.post_raw_state.routes == []) and

        (.boundaries[] | select(.boundary == "replacement_persistence") as $case |
            ($case.prior_scope | scope) and
            ($case.prior_generation | type == "number" and . > 0 and . < $case.generation) and
            $case.outcomes.exact_inspection == "reserved_unpublished" and
            $case.outcomes.same_reservation_generation == $case.generation and
            $case.outcomes.foreign_inspection == "foreign" and
            $case.outcomes.replacement_inspection == "replacement" and
            $case.outcomes.cleanup_outcome == null and
            $case.post_raw_state.generations[$case.container_id].generation == $case.generation and
            $case.post_raw_state.generations[$case.container_id].scope == $case.scope and
            $case.post_raw_state.generations[$case.container_id].reserved == true)
    ' "$evidence_file" >/dev/null
}

validate_stack_crash_reopen_evidence() {
    local evidence_file="$1"
    local expected_profile="$2"
    local expected_test_binary_sha256="$3"
    local expected_runtime_companion_sha256="$4"

    jq -e \
        --arg expected_profile "$expected_profile" \
        --arg expected_test_binary_sha256 "$expected_test_binary_sha256" \
        --arg expected_runtime_companion_sha256 "$expected_runtime_companion_sha256" '
        def exact_keys($keys): (keys | sort) == ($keys | sort);
        def nonempty: type == "string" and length > 0;
        def hex_sha256: type == "string" and test("^[0-9a-f]{64}$");
        def scope:
            exact_keys([
                "environment_id", "machine_id", "machine_incarnation_id",
                "project_id", "reservation_id", "stack_id"
            ]) and
            (all([
                .environment_id, .machine_id, .machine_incarnation_id,
                .project_id, .reservation_id, .stack_id
            ][]; nonempty));
        def ownership:
            exact_keys(["container_id", "generation", "scope", "stack_id"]) and
            (.container_id | nonempty) and
            (.generation | type == "number" and . > 0 and . == floor) and
            (.stack_id | nonempty) and
            (.scope | scope) and
            (.scope.stack_id == .stack_id);
        def binding:
            exact_keys(["bound_at", "ownership", "reservation_id", "service_name"]) and
            (.bound_at | type == "number" and . > 0 and . == floor) and
            (.reservation_id | nonempty) and
            (.service_name | nonempty) and
            (.ownership | ownership) and
            (.reservation_id == .ownership.scope.reservation_id);
        def event_counts:
            exact_keys(["creating", "failed", "ready", "stopped", "stopping"]) and
            all(.[]; type == "number" and . >= 0 and . == floor);
        def store_snapshot:
            exact_keys([
                "action_schema_version", "audit_action_hash", "audit_rows", "audit_status",
                "binding", "event_counts", "intent_status", "observed_phase", "ready",
                "schema_version", "session_actions_hash", "session_cursor", "session_status"
            ]) and
            (.schema_version == 7) and
            (.action_schema_version == 3) and
            (.session_actions_hash | type == "string" and
                test("^vzrah2-sha256:[0-9a-f]{64}$")) and
            (.audit_action_hash | type == "string" and
                test("^vzrah2-sha256:[0-9a-f]{64}$")) and
            (.session_actions_hash == .audit_action_hash) and
            (.session_status | nonempty) and
            (.session_cursor | type == "number" and . >= 0 and . == floor) and
            (.audit_rows | type == "number" and . >= 0 and . == floor) and
            (.audit_status | nonempty) and
            (.intent_status | nonempty) and
            (.observed_phase | nonempty) and
            (.ready | type == "boolean") and
            (.binding | binding) and
            (.event_counts | event_counts);
        def runtime_snapshot:
            exact_keys(["counters", "inspection"]) and
            (.inspection | nonempty) and
            (.counters | exact_keys(["activate", "cleanup", "reserve"])) and
            all(.counters[]; type == "number" and . >= 0 and . == floor);
        def runtime_deltas:
            exact_keys(["activate", "cleanup", "reserve"]) and
            all(.[]; type == "number" and . >= 0 and . == floor);
        def boundary:
            exact_keys(["boundary", "child", "ownership", "post_replay", "pre_replay", "replay"]) and
            (.boundary | nonempty) and
            (.child == {"signal": "SIGKILL", "expected_exit_code": 137}) and
            (.ownership | ownership) and
            (.pre_replay | exact_keys(["runtime", "store"])) and
            (.pre_replay.store | store_snapshot) and
            (.pre_replay.runtime | runtime_snapshot) and
            (.post_replay | exact_keys(["runtime", "store"])) and
            (.post_replay.store | store_snapshot) and
            (.post_replay.runtime | runtime_snapshot) and
            (.replay | exact_keys(["failed", "runtime_deltas", "succeeded"])) and
            (.replay.failed | type == "number" and . >= 0 and . == floor) and
            (.replay.succeeded | type == "number" and . >= 0 and . == floor) and
            (.replay.failed + .replay.succeeded == 1) and
            (.replay.runtime_deltas | runtime_deltas) and
            (.pre_replay.store.binding.ownership == .ownership) and
            (.post_replay.store.binding.ownership == .ownership);
        def reserved_creating_pre:
            .store.session_status == "active" and
            .store.session_cursor == 0 and
            .store.audit_rows == 1 and
            .store.audit_status == "started" and
            .store.intent_status == "reserved" and
            .store.observed_phase == "creating" and
            .store.ready == false and
            .store.event_counts == {
                "creating": 1, "failed": 0, "ready": 0, "stopping": 0, "stopped": 0
            };
        def completed_running_post:
            .store.session_status == "completed" and
            .store.session_cursor == 1 and
            .store.audit_rows == 1 and
            .store.audit_status == "completed" and
            .store.intent_status == "running" and
            .store.observed_phase == "running" and
            .store.ready == false and
            .store.event_counts == {
                "creating": 1, "failed": 0, "ready": 0, "stopping": 0, "stopped": 0
            } and
            .runtime.inspection == "published";
        def failed_cleaned_post:
            .store.session_status == "failed" and
            .store.session_cursor == 0 and
            .store.audit_rows == 1 and
            .store.audit_status == "failed" and
            .store.intent_status == "cleaned" and
            .store.observed_phase == "stopped" and
            .store.ready == false and
            .store.event_counts == {
                "creating": 1, "failed": 1, "ready": 0, "stopping": 1, "stopped": 1
            } and
            .runtime.inspection == "absent";

        (type == "object") and
        (exact_keys([
            "action_schema_version", "boundaries", "build_identity", "controls",
            "coverage_classification", "foreign_receipt_zero_write", "runtime_store_companion",
            "scenario", "schema_version"
        ])) and
        (.schema_version == 7) and
        (.scenario == "runtime-generation-state-store-v7") and
        (.coverage_classification == "action_v3_executor_state_store_atomicity") and
        (.action_schema_version == 3) and
        (.build_identity | exact_keys(["profile", "test_binary_sha256"])) and
        (.build_identity.profile == "release") and
        (.build_identity.profile == $expected_profile) and
        (.build_identity.test_binary_sha256 | hex_sha256) and
        (.build_identity.test_binary_sha256 == $expected_test_binary_sha256) and
        (.runtime_store_companion | exact_keys(["scenario", "sha256"])) and
        (.runtime_store_companion.scenario == "runtime-generation-crash-reopen") and
        (.runtime_store_companion.sha256 | hex_sha256) and
        (.runtime_store_companion.sha256 == $expected_runtime_companion_sha256) and
        (.controls == {
            "harness_invocations": 1,
            "child_processes": 4,
            "sigkills": 4,
            "reopen_replays": 4,
            "fallbacks": 0,
            "skips": 0
        }) and
        ((.boundaries | length) == 4) and
        (all(.boundaries[]; boundary)) and
        ((.boundaries | map(.boundary) | sort) == ([
            "observed_upsert_before_intent_cas", "running_committed_before_batch_commit",
            "runtime_published_before_receipt", "successor_bound_before_activation"
        ] | sort)) and

        (.boundaries[] | select(.boundary == "successor_bound_before_activation") as $case |
            ($case.pre_replay | reserved_creating_pre) and
            ($case.pre_replay.runtime.inspection == "reserved_unpublished") and
            ($case.pre_replay.runtime.counters == {
                "reserve": 1, "activate": 0, "cleanup": 0
            }) and
            ($case.replay == {
                "succeeded": 1, "failed": 0,
                "runtime_deltas": {"reserve": 0, "activate": 1, "cleanup": 0}
            }) and
            ($case.post_replay | completed_running_post) and
            ($case.post_replay.runtime.counters == {
                "reserve": 1, "activate": 1, "cleanup": 0
            })) and

        (all(.boundaries[] |
            select(.boundary == "runtime_published_before_receipt" or
                .boundary == "observed_upsert_before_intent_cas");
            (.pre_replay | reserved_creating_pre) and
            (.pre_replay.runtime.inspection == "published") and
            (.pre_replay.runtime.counters == {
                "reserve": 1, "activate": 1, "cleanup": 0
            }) and
            (.replay == {
                "succeeded": 0, "failed": 1,
                "runtime_deltas": {"reserve": 0, "activate": 0, "cleanup": 1}
            }) and
            (.post_replay | failed_cleaned_post) and
            (.post_replay.runtime.counters == {
                "reserve": 1, "activate": 1, "cleanup": 1
            }))) and

        (.boundaries[] | select(.boundary == "running_committed_before_batch_commit") as $case |
            ($case.pre_replay.store.session_status == "active") and
            ($case.pre_replay.store.session_cursor == 0) and
            ($case.pre_replay.store.audit_rows == 1) and
            ($case.pre_replay.store.audit_status == "started") and
            ($case.pre_replay.store.intent_status == "running") and
            ($case.pre_replay.store.observed_phase == "running") and
            ($case.pre_replay.store.ready == false) and
            ($case.pre_replay.store.event_counts == {
                "creating": 1, "failed": 0, "ready": 0, "stopping": 0, "stopped": 0
            }) and
            ($case.pre_replay.runtime.inspection == "published") and
            ($case.pre_replay.runtime.counters == {
                "reserve": 1, "activate": 1, "cleanup": 0
            }) and
            ($case.replay == {
                "succeeded": 1, "failed": 0,
                "runtime_deltas": {"reserve": 0, "activate": 0, "cleanup": 0}
            }) and
            ($case.post_replay | completed_running_post) and
            ($case.post_replay.runtime.counters ==
                $case.pre_replay.runtime.counters) and
            ($case.post_replay.store.event_counts.ready ==
                $case.pre_replay.store.event_counts.ready)) and

        (.foreign_receipt_zero_write |
            exact_keys([
                "logical_sha256_after", "logical_sha256_before", "machine_code",
                "runtime_deltas", "total_changes_delta"
            ]) and
            .machine_code == "state_conflict" and
            .total_changes_delta == 0 and
            (.logical_sha256_before | hex_sha256) and
            (.logical_sha256_after | hex_sha256) and
            .logical_sha256_before == .logical_sha256_after and
            (.runtime_deltas | runtime_deltas) and
            .runtime_deltas == {"reserve": 0, "activate": 0, "cleanup": 0})
    ' "$evidence_file" >/dev/null
}

write_and_validate_container_id_ownership_checksum() {
    local evidence_file="$1"
    local checksum_file="$2"
    local evidence_name
    evidence_name="$(basename "$evidence_file")"
    local digest
    digest="$(shasum -a 256 "$evidence_file" | cut -d' ' -f1)"
    printf '%s  %s\n' "$digest" "$evidence_name" > "$checksum_file"
    (cd "$(dirname "$evidence_file")" && shasum -a 256 -c "$(basename "$checksum_file")") >/dev/null
}

write_and_validate_exec_supervision_checksum() {
    local evidence_file="$1"
    local checksum_file="$2"
    local evidence_name
    evidence_name="$(basename "$evidence_file")"
    local digest
    digest="$(shasum -a 256 "$evidence_file" | cut -d' ' -f1)"
    printf '%s  %s\n' "$digest" "$evidence_name" > "$checksum_file"
    (cd "$(dirname "$evidence_file")" && shasum -a 256 -c "$(basename "$checksum_file")") >/dev/null
}

write_and_validate_stack_teardown_checksum() {
    local evidence_file="$1"
    local checksum_file="$2"
    local evidence_name
    evidence_name="$(basename "$evidence_file")"
    local digest
    digest="$(shasum -a 256 "$evidence_file" | cut -d' ' -f1)"
    printf '%s  %s\n' "$digest" "$evidence_name" > "$checksum_file"
    (cd "$(dirname "$evidence_file")" && shasum -a 256 -c "$(basename "$checksum_file")") >/dev/null
}

write_and_validate_stack_container_ownership_checksum() {
    local evidence_file="$1"
    local checksum_file="$2"
    local evidence_name
    evidence_name="$(basename "$evidence_file")"
    local digest
    digest="$(shasum -a 256 "$evidence_file" | cut -d' ' -f1)"
    printf '%s  %s\n' "$digest" "$evidence_name" > "$checksum_file"
    (cd "$(dirname "$evidence_file")" && shasum -a 256 -c "$(basename "$checksum_file")") >/dev/null
}

write_and_validate_vm_full_unsupported_checksum() {
    local evidence_file="$1"
    local checksum_file="$2"
    local evidence_name
    evidence_name="$(basename "$evidence_file")"
    local digest
    digest="$(shasum -a 256 "$evidence_file" | cut -d' ' -f1)"
    printf '%s  %s\n' "$digest" "$evidence_name" > "$checksum_file"
    (cd "$(dirname "$evidence_file")" && shasum -a 256 -c "$(basename "$checksum_file")") >/dev/null
}

write_and_validate_environment_lifecycle_checksum() {
    local evidence_file="$1"
    local checksum_file="$2"
    local evidence_name
    evidence_name="$(basename "$evidence_file")"
    local digest
    digest="$(shasum -a 256 "$evidence_file" | cut -d' ' -f1)"
    printf '%s  %s\n' "$digest" "$evidence_name" > "$checksum_file"
    (cd "$(dirname "$evidence_file")" && shasum -a 256 -c "$(basename "$checksum_file")") >/dev/null
}

write_and_validate_runtime_crash_reopen_checksum() {
    local evidence_file="$1"
    local checksum_file="$2"
    local evidence_name
    evidence_name="$(basename "$evidence_file")"
    local digest
    digest="$(shasum -a 256 "$evidence_file" | cut -d' ' -f1)"
    printf '%s  %s\n' "$digest" "$evidence_name" > "$checksum_file"
    (cd "$(dirname "$evidence_file")" && shasum -a 256 -c "$(basename "$checksum_file")") >/dev/null
}

write_and_validate_stack_crash_reopen_checksum() {
    local evidence_file="$1"
    local checksum_file="$2"
    local evidence_name
    evidence_name="$(basename "$evidence_file")"
    local digest
    digest="$(shasum -a 256 "$evidence_file" | cut -d' ' -f1)"
    printf '%s  %s\n' "$digest" "$evidence_name" > "$checksum_file"
    (cd "$(dirname "$evidence_file")" && shasum -a 256 -c "$(basename "$checksum_file")") >/dev/null
}

run_and_log() {
    local suite="$1"
    local label="$2"
    local binary="$3"
    shift 3
    local args=("$@")
    local log_file="$RUN_DIR/${label}.log"
    local cmd_env=()
    local exec_supervision_test_binary_sha256=""
    local stack_ownership_test_binary_sha256=""
    local crash_reopen_test_binary_sha256=""
    local stack_crash_reopen_test_binary_sha256=""
    local emits_vm_full_unsupported_evidence=false

    # BuildKit tests are sensitive to stale shared cache state under ~/.vz/buildkit.
    # Pin a per-run directory so CI/local harness executions are deterministic.
    if [[ "$suite" == "buildkit" ]]; then
        local buildkit_dir="$RUN_DIR/buildkit-home"
        mkdir -p "$buildkit_dir"
        rm -f "$BUILDKIT_RUNTIME_INVENTORY_EVIDENCE"
        cmd_env+=("VZ_BUILDKIT_DIR=$buildkit_dir")
        cmd_env+=("VZ_BUILDKIT_RUNTIME_INVENTORY_EVIDENCE=$BUILDKIT_RUNTIME_INVENTORY_EVIDENCE")
        cmd_env+=("VZ_BUILDKIT_ARTIFACT_ARCHIVE=$BUILDKIT_ARTIFACT_ARCHIVE_EVIDENCE")
        cmd_env+=("VZ_BUILDKIT_ARTIFACT_SHA256=$BUILDKIT_EXPECTED_SHA256")
    fi

    if [[ "$suite" == "stack" || "$suite" == "runtime" ]]; then
        local stack_serial_dir="$RUN_DIR/${label}-vm-serial"
        mkdir -p "$stack_serial_dir"
        cmd_env+=("VZ_STACK_SERIAL_LOG_DIR=$stack_serial_dir")
    fi

    # Stack E2Es intentionally use stable service names as container IDs. Give
    # every harness run a private lifecycle/image store so an interrupted
    # earlier run cannot leave durable metadata that poisons a later gate.
    # Keep HOME unchanged so kernel artifacts and registry credentials retain
    # their normal host resolution.
    if [[ "$suite" == "stack" ]]; then
        local stack_oci_data_dir="$RUN_DIR/${label}-oci"
        mkdir -p "$stack_oci_data_dir"
        cmd_env+=("VZ_STACK_E2E_OCI_DATA_DIR=$stack_oci_data_dir")
        if [[ "$label" == "stack" || "$label" == "stack-snapshot-restore" \
            || "$label" == "stack-user-journey-checkpoint" ]]; then
            emits_vm_full_unsupported_evidence=true
            VM_FULL_UNSUPPORTED_EVIDENCE_VALIDATED=false
            rm -f "$VM_FULL_UNSUPPORTED_EVIDENCE" "$VM_FULL_UNSUPPORTED_SHA256"
        fi
        if [[ "$label" == "stack" || "$label" == "stack-port-forwarding" ]]; then
            rm -f "$STACK_TEARDOWN_EVIDENCE"
            rm -f "$STACK_TEARDOWN_SHA256"
            cmd_env+=("VZ_STACK_TEARDOWN_EVIDENCE=$STACK_TEARDOWN_EVIDENCE")
        fi
        if [[ "$label" == "stack" || "$label" == "stack-container-ownership" ]]; then
            if [[ "$PROFILE" != "release" ]]; then
                echo "stack container-ownership evidence cannot run under profile '$PROFILE'" >&2
                return 109
            fi
            stack_ownership_test_binary_sha256="$(shasum -a 256 "$binary" | cut -d' ' -f1)"
            rm -f "$STACK_CONTAINER_OWNERSHIP_EVIDENCE"
            rm -f "$STACK_CONTAINER_OWNERSHIP_SHA256"
            cmd_env+=("VZ_STACK_CONTAINER_OWNERSHIP_EVIDENCE=$STACK_CONTAINER_OWNERSHIP_EVIDENCE")
            cmd_env+=("VZ_STACK_OWNERSHIP_BUILD_PROFILE=$PROFILE")
            cmd_env+=("VZ_STACK_OWNERSHIP_TEST_BINARY_SHA256=$stack_ownership_test_binary_sha256")
        fi
        if [[ "$label" == "stack" || "$label" == "environment-lifecycle-journal-linux-vm" ]]; then
            rm -f "$ENVIRONMENT_LIFECYCLE_EVIDENCE"
            rm -f "$ENVIRONMENT_LIFECYCLE_SHA256"
            cmd_env+=("VZ_ENVIRONMENT_LIFECYCLE_EVIDENCE=$ENVIRONMENT_LIFECYCLE_EVIDENCE")
        fi
    fi

    if [[ "$suite" == "runtime" ]]; then
        if [[ "$label" == "runtime" || "$label" == "runtime-container-id-ownership" ]]; then
            rm -f "$CONTAINER_ID_OWNERSHIP_EVIDENCE"
            rm -f "$CONTAINER_ID_OWNERSHIP_SHA256"
        fi
        cmd_env+=("VZ_CONTAINER_ID_OWNERSHIP_EVIDENCE=$CONTAINER_ID_OWNERSHIP_EVIDENCE")
        if [[ "$label" == "runtime" || "$label" == "runtime-exec-supervision" ]]; then
            if [[ "$PROFILE" != "release" ]]; then
                echo "exec supervision release evidence cannot run under profile '$PROFILE'" >&2
                return 103
            fi
            exec_supervision_test_binary_sha256="$(shasum -a 256 "$binary" | cut -d' ' -f1)"
            rm -f "$EXEC_SUPERVISION_EVIDENCE"
            rm -f "$EXEC_SUPERVISION_SHA256"
            cmd_env+=("VZ_EXEC_SUPERVISION_EVIDENCE=$EXEC_SUPERVISION_EVIDENCE")
            cmd_env+=("VZ_EXEC_SUPERVISION_BUILD_PROFILE=$PROFILE")
            cmd_env+=("VZ_EXEC_SUPERVISION_TEST_BINARY_SHA256=$exec_supervision_test_binary_sha256")
            cmd_env+=("VZ_EXEC_SUPERVISION_DEVELOPER_INITRAMFS_SHA256=$DEVELOPER_INITRAMFS_SHA256")
            cmd_env+=("VZ_TEST_DROP_CONTAINER_EXEC_RESPONSE_BEFORE_READY_COMMAND=/vz-exec-response-loss-command/sh")
            cmd_env+=("VZ_TEST_DROP_CONTAINER_EXEC_RESPONSE_DWELL_MS=5000")
        fi
        if [[ "$label" == "runtime-generation-crash-reopen" ]]; then
            if [[ "$PROFILE" != "release" ]]; then
                echo "runtime generation crash/reopen evidence cannot run under profile '$PROFILE'" >&2
                return 110
            fi
            crash_reopen_test_binary_sha256="$(shasum -a 256 "$binary" | cut -d' ' -f1)"
            rm -f "$RUNTIME_CRASH_REOPEN_EVIDENCE" "$RUNTIME_CRASH_REOPEN_SHA256"
            cmd_env+=("VZ_RUNTIME_CRASH_REOPEN_EVIDENCE=$RUNTIME_CRASH_REOPEN_EVIDENCE")
            cmd_env+=("VZ_RUNTIME_CRASH_BUILD_PROFILE=$PROFILE")
            cmd_env+=("VZ_RUNTIME_CRASH_TEST_BINARY_SHA256=$crash_reopen_test_binary_sha256")
        fi
    fi

    if [[ "$label" == "runtime-generation-state-store-v7" ]]; then
        if [[ "$PROFILE" != "release" ]]; then
            echo "stack runtime-generation StateStore crash/reopen evidence cannot run under profile '$PROFILE'" >&2
            return 110
        fi
        if [[ ! -f "$RUNTIME_CRASH_REOPEN_EVIDENCE" \
            || "$RUNTIME_CRASH_REOPEN_EVIDENCE_VALIDATED" != "true" ]]; then
            echo "stack crash/reopen companion requires validated runtime crash/reopen evidence" >&2
            return 111
        fi
        stack_crash_reopen_test_binary_sha256="$(shasum -a 256 "$binary" | cut -d' ' -f1)"
        local runtime_crash_reopen_sha256_value
        runtime_crash_reopen_sha256_value="$(shasum -a 256 "$RUNTIME_CRASH_REOPEN_EVIDENCE" | cut -d' ' -f1)"
        rm -f "$STACK_CRASH_REOPEN_EVIDENCE" "$STACK_CRASH_REOPEN_SHA256"
        cmd_env+=("VZ_STACK_CRASH_REOPEN_EVIDENCE=$STACK_CRASH_REOPEN_EVIDENCE")
        cmd_env+=("VZ_STACK_CRASH_BUILD_PROFILE=$PROFILE")
        cmd_env+=("VZ_STACK_CRASH_TEST_BINARY_SHA256=$stack_crash_reopen_test_binary_sha256")
        cmd_env+=("VZ_RUNTIME_CRASH_REOPEN_SHA256_VALUE=$runtime_crash_reopen_sha256_value")
    fi

    cmd_env+=("VZ_LINUX_DEVELOPER_BUNDLE_DIR=$REPO_ROOT/linux/out")
    cmd_env+=("VZ_LINUX_CONTAINER_BUNDLE_DIR=$REPO_ROOT/linux/out/container")

    echo "running [$label/$suite]: $binary ${args[*]}"

    set +e
    env "${cmd_env[@]}" "$binary" "${args[@]}" 2>&1 | tee "$log_file"
    local pipeline_status=("${PIPESTATUS[@]}")
    local status="${pipeline_status[0]}"
    local tee_status="${pipeline_status[1]}"
    set -e

    if [[ $tee_status -ne 0 ]]; then
        echo "artifact log capture failed ($label/$suite, tee exit $tee_status)" >&2
        return 94
    fi

    if [[ $status -eq 0 ]] && grep -q "^running 0 tests$" "$log_file"; then
        echo "scenario/suite executed zero tests ($label/$suite); check scenario_test_filter mapping" >&2
        return 86
    fi

    if [[ $status -eq 0 ]] && grep -q "VZ_E2E_REQUIRED_SKIP:" "$log_file"; then
        echo "scenario/suite reported a required skip despite a successful exit ($label/$suite)" >&2
        return 88
    fi

    if [[ $status -eq 0 && "$suite" == "stack" ]]; then
        local violation_artifact="$RUN_DIR/${label}-teardown-violations.txt"
        local violation_marker='VZ_STACK_TEARDOWN_VIOLATION:[A-Z0-9_]+'
        set +e
        grep -n -E "$violation_marker" "$log_file" > "$violation_artifact"
        local violation_scan_status=$?
        set -e
        case "$violation_scan_status" in
            0)
                echo "stack teardown emitted a code-owned failure or retry sentinel ($label/$suite)" >&2
                sed -n '1,120p' "$violation_artifact" >&2
                return 91
                ;;
            1)
                rm -f "$violation_artifact"
                ;;
            *)
                echo "stack teardown sentinel scan failed closed ($label/$suite, grep exit $violation_scan_status)" \
                    > "$violation_artifact"
                echo "stack teardown sentinel scan failed ($label/$suite)" >&2
                return 95
                ;;
        esac
    fi

    if [[ $status -eq 0 && "$PROFILE" == "release" \
        && "$suite" == "buildkit" && "$label" == "buildkit" ]] \
        && ! grep -Fqx \
            "test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished" \
            <(sed -E 's/; finished in .*/; finished/' "$log_file"); then
        echo "complete BuildKit suite did not report exactly 3/3 passing tests with zero ignored or filtered tests" >&2
        return 96
    fi

    if [[ $status -eq 0 && "$PROFILE" == "release" \
        && "$suite" == "stack" && "$label" == "stack" ]] \
        && ! grep -Fqx \
            "test result: ok. 24 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out; finished" \
            <(sed -E 's/; finished in .*/; finished/' "$log_file"); then
        echo "complete stack suite did not report exactly 24/24 real-VM tests with zero ignored failures" >&2
        return 99
    fi

    if [[ $status -eq 0 && "$PROFILE" == "release" \
        && "$suite" == "runtime" && "$label" == "runtime" ]] \
        && ! grep -Fqx \
            "test result: ok. 18 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished" \
            <(sed -E 's/; finished in .*/; finished/' "$log_file"); then
        echo "complete runtime suite did not report exactly 18/18 real-VM tests with zero ignored failures" >&2
        return 100
    fi

    if [[ $status -eq 0 && "$label" != "$suite" ]] \
        && ! grep -Eq \
            '^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished$' \
            <(sed -E 's/; finished in .*/; finished/' "$log_file"); then
        echo "focused scenario did not report exactly one passing physical test with zero ignored tests ($label/$suite)" >&2
        return 106
    fi

    if [[ $status -eq 0 && "$emits_vm_full_unsupported_evidence" == "true" ]]; then
        if ! extract_vm_full_unsupported_evidence \
            "$log_file" "$VM_FULL_UNSUPPORTED_EVIDENCE"; then
            echo "VM-full unsupported evidence is missing, duplicated, or invalid ($label/$suite)" >&2
            return 107
        fi
        if ! write_and_validate_vm_full_unsupported_checksum \
            "$VM_FULL_UNSUPPORTED_EVIDENCE" "$VM_FULL_UNSUPPORTED_SHA256"; then
            echo "VM-full unsupported evidence checksum creation or verification failed" >&2
            return 108
        fi
        VM_FULL_UNSUPPORTED_EVIDENCE_VALIDATED=true
    fi

    if [[ $status -eq 0 ]] && [[ "$suite" == "stack" ]] \
        && [[ "$label" == "stack" || "$label" == "stack-port-forwarding" ]]; then
        if [[ ! -f "$STACK_TEARDOWN_EVIDENCE" ]] \
            || ! validate_stack_teardown_evidence "$STACK_TEARDOWN_EVIDENCE"; then
            echo "stack teardown evidence is missing, malformed, or violates cleanup ownership" >&2
            return 92
        fi
        if ! write_and_validate_stack_teardown_checksum \
            "$STACK_TEARDOWN_EVIDENCE" "$STACK_TEARDOWN_SHA256"; then
            echo "stack teardown evidence checksum creation or verification failed" >&2
            return 93
        fi
        STACK_TEARDOWN_EVIDENCE_VALIDATED=true
    fi

    if [[ $status -eq 0 ]] && [[ "$suite" == "stack" ]] \
        && [[ "$label" == "stack" || "$label" == "stack-container-ownership" ]]; then
        if [[ ! -f "$STACK_CONTAINER_OWNERSHIP_EVIDENCE" ]] \
            || ! validate_stack_container_ownership_evidence \
                "$STACK_CONTAINER_OWNERSHIP_EVIDENCE" \
                "$PROFILE" \
                "$stack_ownership_test_binary_sha256"; then
            echo "stack container-ownership evidence is missing, malformed, or violates generation ownership" >&2
            return 97
        fi
        if ! write_and_validate_stack_container_ownership_checksum \
            "$STACK_CONTAINER_OWNERSHIP_EVIDENCE" "$STACK_CONTAINER_OWNERSHIP_SHA256"; then
            echo "stack container-ownership evidence checksum creation or verification failed" >&2
            return 98
        fi
        STACK_CONTAINER_OWNERSHIP_EVIDENCE_VALIDATED=true
    fi

    if [[ $status -eq 0 ]] && [[ "$suite" == "stack" ]] \
        && [[ "$label" == "stack" || "$label" == "environment-lifecycle-journal-linux-vm" ]]; then
        if [[ ! -f "$ENVIRONMENT_LIFECYCLE_EVIDENCE" ]] \
            || ! validate_environment_lifecycle_evidence "$ENVIRONMENT_LIFECYCLE_EVIDENCE"; then
            echo "Environment lifecycle evidence is missing, malformed, or violates the journal contract" >&2
            return 104
        fi
        if ! write_and_validate_environment_lifecycle_checksum \
            "$ENVIRONMENT_LIFECYCLE_EVIDENCE" "$ENVIRONMENT_LIFECYCLE_SHA256"; then
            echo "Environment lifecycle evidence checksum creation or verification failed" >&2
            return 105
        fi
        ENVIRONMENT_LIFECYCLE_EVIDENCE_VALIDATED=true
    fi

    if [[ $status -eq 0 ]] && [[ "$suite" == "buildkit" ]]; then
        if ! validate_buildkit_runtime_inventory_evidence "$BUILDKIT_RUNTIME_INVENTORY_EVIDENCE"; then
            echo "BuildKit runtime inventory evidence is missing, malformed, or violates the youki-only contract" >&2
            return 87
        fi
        BUILDKIT_EVIDENCE_VALIDATED=true
    fi

    if [[ $status -eq 0 ]] && [[ "$suite" == "runtime" ]]; then
        if [[ -f "$CONTAINER_ID_OWNERSHIP_EVIDENCE" ]]; then
            if ! validate_container_id_ownership_evidence "$CONTAINER_ID_OWNERSHIP_EVIDENCE"; then
                echo "container-ID ownership evidence is malformed or violates the lifecycle contract" >&2
                return 89
            fi
            if ! write_and_validate_container_id_ownership_checksum \
                "$CONTAINER_ID_OWNERSHIP_EVIDENCE" "$CONTAINER_ID_OWNERSHIP_SHA256"; then
                echo "container-ID ownership evidence checksum creation or verification failed" >&2
                return 90
            fi
            RUNTIME_ID_EVIDENCE_VALIDATED=true
        elif [[ "$label" == "runtime" || "$label" == "runtime-container-id-ownership" ]]; then
            echo "container-ID ownership scenario did not retain its required evidence" >&2
            return 89
        fi
        if [[ "$label" == "runtime" || "$label" == "runtime-exec-supervision" ]]; then
            if [[ ! -f "$EXEC_SUPERVISION_EVIDENCE" ]] \
                || ! validate_exec_supervision_evidence \
                    "$EXEC_SUPERVISION_EVIDENCE" \
                    "$PROFILE" \
                    "$exec_supervision_test_binary_sha256" \
                    "$DEVELOPER_INITRAMFS_SHA256"; then
                echo "exec supervision evidence is missing, malformed, or violates the schema-v4 release contract" >&2
                return 101
            fi
            if ! write_and_validate_exec_supervision_checksum \
                "$EXEC_SUPERVISION_EVIDENCE" "$EXEC_SUPERVISION_SHA256"; then
                echo "exec supervision evidence checksum creation or verification failed" >&2
                return 102
            fi
            EXEC_SUPERVISION_EVIDENCE_VALIDATED=true
        fi
        if [[ "$label" == "runtime-generation-crash-reopen" ]]; then
            if [[ ! -f "$RUNTIME_CRASH_REOPEN_EVIDENCE" ]] \
                || ! validate_runtime_crash_reopen_evidence \
                    "$RUNTIME_CRASH_REOPEN_EVIDENCE" \
                    "$PROFILE" \
                    "$crash_reopen_test_binary_sha256"; then
                echo "runtime generation crash/reopen evidence is missing or malformed" >&2
                return 111
            fi
            if ! write_and_validate_runtime_crash_reopen_checksum \
                "$RUNTIME_CRASH_REOPEN_EVIDENCE" "$RUNTIME_CRASH_REOPEN_SHA256"; then
                echo "runtime generation crash/reopen evidence checksum failed" >&2
                return 112
            fi
            RUNTIME_CRASH_REOPEN_EVIDENCE_VALIDATED=true
        fi
    fi

    if [[ $status -eq 0 && "$label" == "runtime-generation-state-store-v7" ]]; then
        local runtime_crash_reopen_sha256_value
        runtime_crash_reopen_sha256_value="$(shasum -a 256 "$RUNTIME_CRASH_REOPEN_EVIDENCE" | cut -d' ' -f1)"
        if [[ ! -f "$STACK_CRASH_REOPEN_EVIDENCE" ]] \
            || ! validate_stack_crash_reopen_evidence \
                "$STACK_CRASH_REOPEN_EVIDENCE" \
                "$PROFILE" \
                "$stack_crash_reopen_test_binary_sha256" \
                "$runtime_crash_reopen_sha256_value"; then
            echo "stack runtime-generation StateStore crash/reopen evidence is missing or malformed" >&2
            return 111
        fi
        if ! write_and_validate_stack_crash_reopen_checksum \
            "$STACK_CRASH_REOPEN_EVIDENCE" "$STACK_CRASH_REOPEN_SHA256"; then
            echo "stack runtime-generation StateStore crash/reopen evidence checksum failed" >&2
            return 112
        fi
        STACK_CRASH_REOPEN_EVIDENCE_VALIDATED=true
    fi

    return "$status"
}

echo "==> output directory: $RUN_DIR"
{
    echo "timestamp_utc=$timestamp"
    echo "host=$(hostname)"
    echo "profile=$PROFILE"
    echo "suites=${RESOLVED_SUITES[*]}"
    echo "scenarios=${RESOLVED_SCENARIOS[*]:-none}"
    echo "run_args=${RUN_ARGS[*]}"
    echo "guest_agent_build_tool=$GUEST_AGENT_BUILD_TOOL"
    echo "developer_initramfs_sha256=$DEVELOPER_INITRAMFS_SHA256"
    echo "container_initramfs_sha256=$CONTAINER_INITRAMFS_SHA256"
    echo "buildkit_artifact_source_mode=$BUILDKIT_ARTIFACT_SOURCE_MODE"
    echo "buildkit_builder_invocations=$BUILDKIT_BUILDER_INVOCATIONS"
    echo "buildkit_release_gate_qualified=$BUILDKIT_RELEASE_GATE_QUALIFIED"
    echo "buildkit_artifact_archive=$BUILDKIT_ARTIFACT_ARCHIVE_EVIDENCE"
    echo "buildkit_artifact_sha256=$BUILDKIT_EXPECTED_SHA256"
    echo "buildkit_artifact_checksum_file=$BUILDKIT_ARTIFACT_SHA256_EVIDENCE"
    echo "buildkit_artifact_manifest=$BUILDKIT_ARTIFACT_MANIFEST_EVIDENCE"
    echo "buildkit_artifact_inventory=$BUILDKIT_ARTIFACT_INVENTORY_EVIDENCE"
    echo "buildkit_artifact_provenance=$BUILDKIT_ARTIFACT_PROVENANCE_EVIDENCE"
    echo "buildkit_artifact_verification=$BUILDKIT_ARTIFACT_VERIFICATION_EVIDENCE"
    echo "buildkit_artifact_evidence_checksums=$BUILDKIT_ARTIFACT_EVIDENCE_CHECKSUMS"
    echo "buildkit_builder_output_checksums=$BUILDKIT_BUILDER_OUTPUT_CHECKSUMS"
    echo "vm_full_unsupported_path=$VM_FULL_UNSUPPORTED_EVIDENCE"
    echo "vm_full_unsupported_checksum=$VM_FULL_UNSUPPORTED_SHA256"
    echo "environment_lifecycle_evidence=$ENVIRONMENT_LIFECYCLE_EVIDENCE"
    echo "environment_lifecycle_checksum=$ENVIRONMENT_LIFECYCLE_SHA256"
    echo "runtime_crash_reopen_evidence=$RUNTIME_CRASH_REOPEN_EVIDENCE"
    echo "runtime_crash_reopen_checksum=$RUNTIME_CRASH_REOPEN_SHA256"
    echo "stack_crash_reopen_evidence=$STACK_CRASH_REOPEN_EVIDENCE"
    echo "stack_crash_reopen_checksum=$STACK_CRASH_REOPEN_SHA256"
} > "$RUN_DIR/run-info.txt"

echo "==> building host binaries required for local VM flows"
host_artifact_log="$RUN_DIR/host-build-artifacts.jsonl"
run_cargo_recording_artifacts "$host_artifact_log" \
    build "${BUILD_ARGS[@]}" -p vz-cli -p vz-guest-agent

vz_binary="$(resolve_cargo_executable "$host_artifact_log" "vz" "bin")" \
    || err "unable to resolve the vz executable from $host_artifact_log"
guest_agent_binary="$(resolve_cargo_executable "$host_artifact_log" "vz-guest-agent" "bin")" \
    || err "unable to resolve the vz-guest-agent executable from $host_artifact_log"

sign_binary "$vz_binary" "$ENTITLEMENTS"
sign_binary "$guest_agent_binary"

FAILED=()
PASSED=()
should_stop=false
BUILDKIT_SUITE_RAN="$BUILDKIT_SELECTED"
BUILDKIT_EVIDENCE_VALIDATED=false
RUNTIME_ID_EVIDENCE_VALIDATED=false
RUNTIME_ID_EVIDENCE_REQUIRED=false
EXEC_SUPERVISION_EVIDENCE_VALIDATED=false
EXEC_SUPERVISION_EVIDENCE_REQUIRED=false
STACK_TEARDOWN_EVIDENCE_VALIDATED=false
STACK_TEARDOWN_EVIDENCE_REQUIRED=false
STACK_CONTAINER_OWNERSHIP_EVIDENCE_VALIDATED=false
STACK_CONTAINER_OWNERSHIP_EVIDENCE_REQUIRED=false
VM_FULL_UNSUPPORTED_EVIDENCE_VALIDATED=false
VM_FULL_UNSUPPORTED_EVIDENCE_REQUIRED=false
ENVIRONMENT_LIFECYCLE_EVIDENCE_VALIDATED=false
ENVIRONMENT_LIFECYCLE_EVIDENCE_REQUIRED=false
RUNTIME_CRASH_REOPEN_EVIDENCE_VALIDATED=false
RUNTIME_CRASH_REOPEN_EVIDENCE_REQUIRED="$CRASH_REOPEN_LANE_SELECTED"
STACK_CRASH_REOPEN_EVIDENCE_VALIDATED=false
STACK_CRASH_REOPEN_EVIDENCE_REQUIRED="$CRASH_REOPEN_LANE_SELECTED"

run_stack_crash_reopen_companion() {
    local binary="$1"
    local companion_args=(
        "${RUN_ARGS[@]}" "--exact"
        "crash_reopen_tests::action_v3_state_store_sigkill_crash_reopen"
    )

    if run_and_log \
        "stack" \
        "runtime-generation-state-store-v7" \
        "$binary" \
        "${companion_args[@]}"; then
        echo "==> scenario passed: runtime-generation-state-store-v7"
        PASSED+=("runtime-generation-state-store-v7")
    else
        local status=$?
        echo "==> scenario failed: runtime-generation-state-store-v7 (exit $status)"
        FAILED+=("runtime-generation-state-store-v7:$status")
        if [[ "$KEEP_GOING" != "true" ]]; then
            should_stop=true
        fi
    fi
}

for suite in "${RESOLVED_SUITES[@]}"; do
    package="$(suite_package "$suite")" || err "unknown suite '$suite'"
    test_name="$(suite_test_name "$suite")" || err "unknown suite '$suite'"
    if [[ "$suite" == "stack" ]] && { [[ ${#RESOLVED_SCENARIOS[@]} -eq 0 ]] \
        || [[ " ${RESOLVED_SCENARIOS[*]} " == *" stack-port-forwarding "* ]]; }; then
        STACK_TEARDOWN_EVIDENCE_REQUIRED=true
    fi
    if [[ "$suite" == "stack" ]] && { [[ ${#RESOLVED_SCENARIOS[@]} -eq 0 ]] \
        || [[ " ${RESOLVED_SCENARIOS[*]} " == *" stack-container-ownership "* ]]; }; then
        STACK_CONTAINER_OWNERSHIP_EVIDENCE_REQUIRED=true
    fi
    if [[ "$suite" == "stack" ]] && { [[ ${#RESOLVED_SCENARIOS[@]} -eq 0 ]] \
        || [[ " ${RESOLVED_SCENARIOS[*]} " == *" stack-snapshot-restore "* ]] \
        || [[ " ${RESOLVED_SCENARIOS[*]} " == *" stack-user-journey-checkpoint "* ]]; }; then
        VM_FULL_UNSUPPORTED_EVIDENCE_REQUIRED=true
    fi
    if [[ "$suite" == "stack" ]] && { [[ ${#RESOLVED_SCENARIOS[@]} -eq 0 ]] \
        || [[ " ${RESOLVED_SCENARIOS[*]} " == *" environment-lifecycle-journal-linux-vm "* ]]; }; then
        ENVIRONMENT_LIFECYCLE_EVIDENCE_REQUIRED=true
    fi

    echo "==> building [$suite] ($package::$test_name)"
    test_artifact_log="$RUN_DIR/${suite}-test-artifacts.jsonl"
    run_cargo_recording_artifacts "$test_artifact_log" \
        test -p "$package" "${BUILD_ARGS[@]}" --test "$test_name" --no-run

    test_binary="$(resolve_cargo_executable "$test_artifact_log" "$test_name" "test")" \
        || err "unable to resolve the $test_name executable from $test_artifact_log"

    sign_binary "$test_binary" "$ENTITLEMENTS"

    crash_reopen_test_binary=""
    stack_crash_reopen_test_binary=""
    if [[ "$suite" == "runtime" ]] \
        && { [[ "$FULL_CLOSURE_GATE_SELECTED" == "true" ]] \
            || [[ " ${RESOLVED_SCENARIOS[*]:-} " == *" runtime-generation-crash-reopen "* ]]; }; then
        crash_reopen_artifact_log="$RUN_DIR/runtime-crash-reopen-test-artifacts.jsonl"
        run_cargo_recording_artifacts "$crash_reopen_artifact_log" \
            test -p "$package" "${BUILD_ARGS[@]}" --lib --no-run
        crash_reopen_test_binary="$(
            resolve_cargo_executable "$crash_reopen_artifact_log" "vz_oci_macos" "lib"
        )" || err "unable to resolve the vz-oci-macos library test executable"
        sign_binary "$crash_reopen_test_binary" "$ENTITLEMENTS"

        stack_crash_reopen_artifact_log="$RUN_DIR/stack-crash-reopen-test-artifacts.jsonl"
        run_cargo_recording_artifacts "$stack_crash_reopen_artifact_log" \
            test -p vz-stack "${BUILD_ARGS[@]}" --lib --no-run
        stack_crash_reopen_test_binary="$(
            resolve_cargo_executable "$stack_crash_reopen_artifact_log" "vz_stack" "lib"
        )" || err "unable to resolve the vz-stack library test executable"
        sign_binary "$stack_crash_reopen_test_binary" "$ENTITLEMENTS"
    fi

    if [[ ${#RESOLVED_SCENARIOS[@]} -gt 0 ]]; then
        for scenario in "${RESOLVED_SCENARIOS[@]}"; do
            if [[ "$(scenario_suite "$scenario")" != "$suite" ]]; then
                continue
            fi
            test_filter="$(scenario_test_filter "$scenario")" || err "unknown scenario '$scenario'"
            scenario_args=("${RUN_ARGS[@]}" "--exact" "$test_filter")
            scenario_binary="$test_binary"
            if [[ "$scenario" == "runtime-generation-crash-reopen" ]]; then
                RUNTIME_CRASH_REOPEN_EVIDENCE_REQUIRED=true
                STACK_CRASH_REOPEN_EVIDENCE_REQUIRED=true
                scenario_binary="$crash_reopen_test_binary"
            fi
            if [[ "$scenario" == "runtime-container-id-ownership" ]]; then
                RUNTIME_ID_EVIDENCE_REQUIRED=true
            fi
            if [[ "$scenario" == "runtime-exec-supervision" ]]; then
                EXEC_SUPERVISION_EVIDENCE_REQUIRED=true
            fi

            scenario_passed=false
            if run_and_log "$suite" "$scenario" "$scenario_binary" "${scenario_args[@]}"; then
                echo "==> scenario passed: $scenario"
                PASSED+=("$scenario")
                scenario_passed=true
            else
                status=$?
                echo "==> scenario failed: $scenario (exit $status)"
                FAILED+=("$scenario:$status")
                if [[ "$KEEP_GOING" != "true" ]]; then
                    should_stop=true
                    break
                fi
            fi
            if [[ "$scenario" == "runtime-generation-crash-reopen" \
                && "$scenario_passed" == "true" ]]; then
                run_stack_crash_reopen_companion "$stack_crash_reopen_test_binary"
                if [[ "$should_stop" == "true" ]]; then
                    break
                fi
            fi
        done
    else
        if [[ "$suite" == "runtime" ]]; then
            RUNTIME_ID_EVIDENCE_REQUIRED=true
            EXEC_SUPERVISION_EVIDENCE_REQUIRED=true
        fi
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
        if [[ "$suite" == "runtime" && "$FULL_CLOSURE_GATE_SELECTED" == "true" ]] \
            && { [[ "$should_stop" == "false" ]] || [[ "$KEEP_GOING" == "true" ]]; }; then
            RUNTIME_CRASH_REOPEN_EVIDENCE_REQUIRED=true
            STACK_CRASH_REOPEN_EVIDENCE_REQUIRED=true
            crash_reopen_args=(
                "${RUN_ARGS[@]}" "--exact"
                "$(scenario_test_filter runtime-generation-crash-reopen)"
            )
            crash_reopen_passed=false
            if run_and_log \
                "$suite" \
                "runtime-generation-crash-reopen" \
                "$crash_reopen_test_binary" \
                "${crash_reopen_args[@]}"; then
                echo "==> scenario passed: runtime-generation-crash-reopen"
                PASSED+=("runtime-generation-crash-reopen")
                crash_reopen_passed=true
            else
                status=$?
                echo "==> scenario failed: runtime-generation-crash-reopen (exit $status)"
                FAILED+=("runtime-generation-crash-reopen:$status")
                if [[ "$KEEP_GOING" != "true" ]]; then
                    should_stop=true
                fi
            fi
            if [[ "$crash_reopen_passed" == "true" ]]; then
                run_stack_crash_reopen_companion "$stack_crash_reopen_test_binary"
            fi
        fi
    fi

    if [[ "$should_stop" == "true" ]]; then
        break
    fi
done

if [[ "$BUILDKIT_SUITE_RAN" == "true" ]]; then
    if [[ "$BUILDKIT_EVIDENCE_VALIDATED" == "true" ]]; then
        echo "==> retained BuildKit runtime inventory: $BUILDKIT_RUNTIME_INVENTORY_EVIDENCE"
    else
        echo "==> BuildKit suite did not retain runtime inventory evidence" >&2
        FAILED+=("buildkit-runtime-inventory:87")
    fi
fi

if [[ "$RUNTIME_ID_EVIDENCE_REQUIRED" == "true" && "$RUNTIME_ID_EVIDENCE_VALIDATED" != "true" ]]; then
    echo "==> required container-ID ownership evidence was not validated" >&2
    FAILED+=("container-id-ownership-evidence:89")
fi

if [[ "$EXEC_SUPERVISION_EVIDENCE_REQUIRED" == "true" \
    && "$EXEC_SUPERVISION_EVIDENCE_VALIDATED" != "true" ]]; then
    echo "==> required exec supervision evidence was not validated" >&2
    FAILED+=("exec-supervision-evidence:101")
fi

if [[ "$STACK_TEARDOWN_EVIDENCE_REQUIRED" == "true" && "$STACK_TEARDOWN_EVIDENCE_VALIDATED" != "true" ]]; then
    echo "==> required stack teardown evidence was not validated" >&2
    FAILED+=("stack-teardown-evidence:92")
fi

if [[ "$STACK_CONTAINER_OWNERSHIP_EVIDENCE_REQUIRED" == "true" \
    && "$STACK_CONTAINER_OWNERSHIP_EVIDENCE_VALIDATED" != "true" ]]; then
    echo "==> required stack container-ownership evidence was not validated" >&2
    FAILED+=("stack-container-ownership-evidence:97")
fi

if [[ "$VM_FULL_UNSUPPORTED_EVIDENCE_REQUIRED" == "true" \
    && "$VM_FULL_UNSUPPORTED_EVIDENCE_VALIDATED" != "true" ]]; then
    echo "==> required VM-full unsupported evidence was not validated" >&2
    FAILED+=("vm-full-unsupported-evidence:107")
fi

if [[ "$ENVIRONMENT_LIFECYCLE_EVIDENCE_REQUIRED" == "true" \
    && "$ENVIRONMENT_LIFECYCLE_EVIDENCE_VALIDATED" != "true" ]]; then
    echo "==> required Environment lifecycle evidence was not validated" >&2
    FAILED+=("environment-lifecycle-evidence:104")
fi

if [[ "$RUNTIME_CRASH_REOPEN_EVIDENCE_REQUIRED" == "true" \
    && "$RUNTIME_CRASH_REOPEN_EVIDENCE_VALIDATED" != "true" ]]; then
    echo "==> required runtime generation crash/reopen evidence was not validated" >&2
    FAILED+=("runtime-generation-crash-reopen-evidence:111")
fi

if [[ "$STACK_CRASH_REOPEN_EVIDENCE_REQUIRED" == "true" \
    && "$STACK_CRASH_REOPEN_EVIDENCE_VALIDATED" != "true" ]]; then
    echo "==> required stack runtime-generation StateStore crash/reopen evidence was not validated" >&2
    FAILED+=("runtime-generation-state-store-v7-evidence:111")
fi

if [[ "$BUILDKIT_RELEASE_GATE_QUALIFIED" == "pending" ]]; then
    if [[ "$BUILDKIT_EVIDENCE_VALIDATED" == "true" && ${#FAILED[@]} -eq 0 ]]; then
        BUILDKIT_RELEASE_GATE_QUALIFIED="true"
    else
        BUILDKIT_RELEASE_GATE_QUALIFIED="false"
    fi
fi

echo "==> summary"
echo "passed: ${PASSED[*]:-none}"
echo "failed: ${FAILED[*]:-none}"

action_summary="$RUN_DIR/summary.txt"
{
    echo "passed=${PASSED[*]:-none}"
    echo "failed=${FAILED[*]:-none}"
    if [[ "$BUILDKIT_EVIDENCE_VALIDATED" == "true" ]]; then
        echo "buildkit_runtime_inventory=$BUILDKIT_RUNTIME_INVENTORY_EVIDENCE"
    else
        echo "buildkit_runtime_inventory=none"
    fi
    echo "buildkit_artifact_source_mode=$BUILDKIT_ARTIFACT_SOURCE_MODE"
    echo "buildkit_builder_invocations=$BUILDKIT_BUILDER_INVOCATIONS"
    echo "buildkit_release_gate_qualified=$BUILDKIT_RELEASE_GATE_QUALIFIED"
    echo "buildkit_artifact_archive=$BUILDKIT_ARTIFACT_ARCHIVE_EVIDENCE"
    echo "buildkit_artifact_sha256=$BUILDKIT_EXPECTED_SHA256"
    echo "buildkit_artifact_checksum_file=$BUILDKIT_ARTIFACT_SHA256_EVIDENCE"
    echo "buildkit_artifact_manifest=$BUILDKIT_ARTIFACT_MANIFEST_EVIDENCE"
    echo "buildkit_artifact_inventory=$BUILDKIT_ARTIFACT_INVENTORY_EVIDENCE"
    echo "buildkit_artifact_provenance=$BUILDKIT_ARTIFACT_PROVENANCE_EVIDENCE"
    echo "buildkit_artifact_verification=$BUILDKIT_ARTIFACT_VERIFICATION_EVIDENCE"
    echo "buildkit_artifact_evidence_checksums=$BUILDKIT_ARTIFACT_EVIDENCE_CHECKSUMS"
    echo "buildkit_builder_output_checksums=$BUILDKIT_BUILDER_OUTPUT_CHECKSUMS"
    echo "vm_full_unsupported_required=$VM_FULL_UNSUPPORTED_EVIDENCE_REQUIRED"
    echo "vm_full_unsupported_validated=$VM_FULL_UNSUPPORTED_EVIDENCE_VALIDATED"
    if [[ "$VM_FULL_UNSUPPORTED_EVIDENCE_VALIDATED" == "true" ]]; then
        echo "vm_full_unsupported_path=$VM_FULL_UNSUPPORTED_EVIDENCE"
        echo "vm_full_unsupported_checksum=$VM_FULL_UNSUPPORTED_SHA256"
    else
        echo "vm_full_unsupported_path=none"
        echo "vm_full_unsupported_checksum=none"
    fi
    if [[ "$RUNTIME_ID_EVIDENCE_VALIDATED" == "true" ]]; then
        echo "container_id_ownership=$CONTAINER_ID_OWNERSHIP_EVIDENCE"
        echo "container_id_ownership_sha256=$CONTAINER_ID_OWNERSHIP_SHA256"
    else
        echo "container_id_ownership=none"
        echo "container_id_ownership_sha256=none"
    fi
    if [[ "$EXEC_SUPERVISION_EVIDENCE_VALIDATED" == "true" ]]; then
        echo "exec_supervision=$EXEC_SUPERVISION_EVIDENCE"
        echo "exec_supervision_sha256=$EXEC_SUPERVISION_SHA256"
    else
        echo "exec_supervision=none"
        echo "exec_supervision_sha256=none"
    fi
    if [[ "$STACK_TEARDOWN_EVIDENCE_VALIDATED" == "true" ]]; then
        echo "stack_teardown=$STACK_TEARDOWN_EVIDENCE"
        echo "stack_teardown_sha256=$STACK_TEARDOWN_SHA256"
    else
        echo "stack_teardown=none"
        echo "stack_teardown_sha256=none"
    fi
    if [[ "$STACK_CONTAINER_OWNERSHIP_EVIDENCE_VALIDATED" == "true" ]]; then
        echo "stack_container_ownership=$STACK_CONTAINER_OWNERSHIP_EVIDENCE"
        echo "stack_container_ownership_sha256=$STACK_CONTAINER_OWNERSHIP_SHA256"
    else
        echo "stack_container_ownership=none"
        echo "stack_container_ownership_sha256=none"
    fi
    if [[ "$ENVIRONMENT_LIFECYCLE_EVIDENCE_VALIDATED" == "true" ]]; then
        echo "environment_lifecycle=$ENVIRONMENT_LIFECYCLE_EVIDENCE"
        echo "environment_lifecycle_sha256=$ENVIRONMENT_LIFECYCLE_SHA256"
    else
        echo "environment_lifecycle=none"
        echo "environment_lifecycle_sha256=none"
    fi
    echo "environment_lifecycle_required=$ENVIRONMENT_LIFECYCLE_EVIDENCE_REQUIRED"
    echo "environment_lifecycle_validated=$ENVIRONMENT_LIFECYCLE_EVIDENCE_VALIDATED"
    if [[ "$RUNTIME_CRASH_REOPEN_EVIDENCE_VALIDATED" == "true" ]]; then
        echo "runtime_crash_reopen=$RUNTIME_CRASH_REOPEN_EVIDENCE"
        echo "runtime_crash_reopen_sha256=$RUNTIME_CRASH_REOPEN_SHA256"
    else
        echo "runtime_crash_reopen=none"
        echo "runtime_crash_reopen_sha256=none"
    fi
    echo "runtime_crash_reopen_required=$RUNTIME_CRASH_REOPEN_EVIDENCE_REQUIRED"
    echo "runtime_crash_reopen_validated=$RUNTIME_CRASH_REOPEN_EVIDENCE_VALIDATED"
    if [[ "$STACK_CRASH_REOPEN_EVIDENCE_VALIDATED" == "true" ]]; then
        echo "stack_crash_reopen=$STACK_CRASH_REOPEN_EVIDENCE"
        echo "stack_crash_reopen_sha256=$STACK_CRASH_REOPEN_SHA256"
    else
        echo "stack_crash_reopen=none"
        echo "stack_crash_reopen_sha256=none"
    fi
    echo "stack_crash_reopen_required=$STACK_CRASH_REOPEN_EVIDENCE_REQUIRED"
    echo "stack_crash_reopen_validated=$STACK_CRASH_REOPEN_EVIDENCE_VALIDATED"
} > "$action_summary"

if [[ ${#FAILED[@]} -gt 0 ]]; then
    exit 1
fi

echo "all selected VM E2E suites passed"
