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
                                runtime-smoke, runtime-lifecycle, runtime-container-id-ownership,
                                runtime-exec-semantics,
                                runtime-exec-defaults,
                                runtime-port-forwarding, runtime-shared-vm-net, stack-real-services,
                                stack-control-socket, stack-port-forwarding,
                                stack-snapshot-restore, buildkit-roundtrip,
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
            runtime-smoke|runtime-lifecycle|runtime-container-id-ownership|runtime-exec-semantics|runtime-exec-defaults|runtime-port-forwarding|runtime-shared-vm-net|stack-real-services|stack-control-socket|stack-port-forwarding|stack-snapshot-restore|stack-user-journey-checkpoint|buildkit-roundtrip)
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
                append_unique_scenario "stack-snapshot-restore"
                ;;
            all-usecases)
                append_unique_scenario "runtime-smoke"
                append_unique_scenario "runtime-lifecycle"
                append_unique_scenario "runtime-container-id-ownership"
                append_unique_scenario "runtime-exec-semantics"
                append_unique_scenario "runtime-exec-defaults"
                append_unique_scenario "runtime-port-forwarding"
                append_unique_scenario "runtime-shared-vm-net"
                append_unique_scenario "stack-real-services"
                append_unique_scenario "stack-control-socket"
                append_unique_scenario "stack-port-forwarding"
                append_unique_scenario "stack-snapshot-restore"
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
        runtime-smoke|runtime-lifecycle|runtime-container-id-ownership|runtime-exec-semantics|runtime-exec-defaults|runtime-port-forwarding|runtime-shared-vm-net)
            echo "runtime"
            ;;
        stack-real-services|stack-control-socket|stack-port-forwarding|stack-snapshot-restore)
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
        runtime-exec-defaults)
            echo "container_exec_inherits_image_process_defaults"
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
            echo "complex_stack_snapshot_restore_rewinds_shared_vm_state"
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

validate_stack_teardown_evidence() {
    local evidence_file="$1"

    jq -e '
        .container_ids as $container_ids |
        (type == "object") and
        ((keys | sort) == ([
            "active", "after_service_down", "after_vm_shutdown", "before",
            "container_ids", "host_listener", "operations", "scenario",
            "schema_version", "stack_id"
        ] | sort)) and
        (.schema_version == 1) and
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
                    "container_id", "generation", "owner_alive", "owner_pid", "reserved"
                ] | sort)) and
                ((.container_id | type) == "string") and
                ((.generation | type) == "number") and
                (.generation > 0 and .generation == (.generation | floor)) and
                ((.reserved | type) == "boolean") and
                ((.owner_pid | type) == "number") and
                (.owner_pid >= 0 and .owner_pid == (.owner_pid | floor)) and
                ((.owner_alive | type) == "boolean")
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
                .container_id == $container_id and .reserved == true and .generation > 0)
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
                .container_id == $container_id and .reserved == false and .generation > 0)
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
                .reserved == false and .owner_alive == false and .generation > 0))
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

run_and_log() {
    local suite="$1"
    local label="$2"
    local binary="$3"
    shift 3
    local args=("$@")
    local log_file="$RUN_DIR/${label}.log"
    local cmd_env=()

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
        if [[ "$label" == "stack" || "$label" == "stack-port-forwarding" ]]; then
            rm -f "$STACK_TEARDOWN_EVIDENCE"
            rm -f "$STACK_TEARDOWN_SHA256"
            cmd_env+=("VZ_STACK_TEARDOWN_EVIDENCE=$STACK_TEARDOWN_EVIDENCE")
        fi
    fi

    if [[ "$suite" == "runtime" ]]; then
        if [[ "$label" == "runtime" || "$label" == "runtime-container-id-ownership" ]]; then
            rm -f "$CONTAINER_ID_OWNERSHIP_EVIDENCE"
            rm -f "$CONTAINER_ID_OWNERSHIP_SHA256"
        fi
        cmd_env+=("VZ_CONTAINER_ID_OWNERSHIP_EVIDENCE=$CONTAINER_ID_OWNERSHIP_EVIDENCE")
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

    if [[ $status -eq 0 && "$PROFILE" == "release" \
        && "$suite" == "buildkit" && "$label" == "buildkit" ]] \
        && ! grep -Fqx \
            "test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished" \
            <(sed -E 's/; finished in .*/; finished/' "$log_file"); then
        echo "complete BuildKit suite did not report exactly 3/3 passing tests with zero ignored or filtered tests" >&2
        return 96
    fi

    if [[ $status -eq 0 ]] && [[ "$suite" == "stack" ]] \
        && [[ "$label" == "stack" || "$label" == "stack-port-forwarding" ]]; then
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
STACK_TEARDOWN_EVIDENCE_VALIDATED=false
STACK_TEARDOWN_EVIDENCE_REQUIRED=false

for suite in "${RESOLVED_SUITES[@]}"; do
    package="$(suite_package "$suite")" || err "unknown suite '$suite'"
    test_name="$(suite_test_name "$suite")" || err "unknown suite '$suite'"
    if [[ "$suite" == "stack" ]] && { [[ ${#RESOLVED_SCENARIOS[@]} -eq 0 ]] \
        || [[ " ${RESOLVED_SCENARIOS[*]} " == *" stack-port-forwarding "* ]]; }; then
        STACK_TEARDOWN_EVIDENCE_REQUIRED=true
    fi

    echo "==> building [$suite] ($package::$test_name)"
    test_artifact_log="$RUN_DIR/${suite}-test-artifacts.jsonl"
    run_cargo_recording_artifacts "$test_artifact_log" \
        test -p "$package" "${BUILD_ARGS[@]}" --test "$test_name" --no-run

    test_binary="$(resolve_cargo_executable "$test_artifact_log" "$test_name" "test")" \
        || err "unable to resolve the $test_name executable from $test_artifact_log"

    sign_binary "$test_binary" "$ENTITLEMENTS"

    if [[ ${#RESOLVED_SCENARIOS[@]} -gt 0 ]]; then
        for scenario in "${RESOLVED_SCENARIOS[@]}"; do
            if [[ "$(scenario_suite "$scenario")" != "$suite" ]]; then
                continue
            fi
            test_filter="$(scenario_test_filter "$scenario")" || err "unknown scenario '$scenario'"
            scenario_args=("${RUN_ARGS[@]}" "--exact" "$test_filter")
            if [[ "$scenario" == "runtime-container-id-ownership" ]]; then
                RUNTIME_ID_EVIDENCE_REQUIRED=true
            fi

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
        if [[ "$suite" == "runtime" ]]; then
            RUNTIME_ID_EVIDENCE_REQUIRED=true
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

if [[ "$STACK_TEARDOWN_EVIDENCE_REQUIRED" == "true" && "$STACK_TEARDOWN_EVIDENCE_VALIDATED" != "true" ]]; then
    echo "==> required stack teardown evidence was not validated" >&2
    FAILED+=("stack-teardown-evidence:92")
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
    if [[ "$RUNTIME_ID_EVIDENCE_VALIDATED" == "true" ]]; then
        echo "container_id_ownership=$CONTAINER_ID_OWNERSHIP_EVIDENCE"
        echo "container_id_ownership_sha256=$CONTAINER_ID_OWNERSHIP_SHA256"
    else
        echo "container_id_ownership=none"
        echo "container_id_ownership_sha256=none"
    fi
    if [[ "$STACK_TEARDOWN_EVIDENCE_VALIDATED" == "true" ]]; then
        echo "stack_teardown=$STACK_TEARDOWN_EVIDENCE"
        echo "stack_teardown_sha256=$STACK_TEARDOWN_SHA256"
    else
        echo "stack_teardown=none"
        echo "stack_teardown_sha256=none"
    fi
} > "$action_summary"

if [[ ${#FAILED[@]} -gt 0 ]]; then
    exit 1
fi

echo "all selected VM E2E suites passed"
