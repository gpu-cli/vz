#!/usr/bin/env bash
# Run release-gate Linux daemon validation with deterministic artifact layout.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PROFILE="release"
WORKSPACE="${VZ_TEST_BTRFS_WORKSPACE:-}"
VM_NAME="${VZ_LINUX_VM_NAME:-}"
GUEST_REPO="${VZ_LINUX_VM_GUEST_REPO:-}"
VM_IMAGE="${VZ_LINUX_VM_IMAGE:-}"
OUTPUT_ROOT="$REPO_ROOT/.artifacts/release-gates/linux-daemon"
API_BIND="${VZ_LINUX_VM_E2E_API_BIND:-127.0.0.1:18181}"

usage() {
    cat <<'USAGE'
run-linux-daemon-release-gate.sh

Run Linux daemon release-gate validation in one command.

On Linux hosts:
  - runs scripts/run-vz-linux-vm-e2e.sh

On macOS hosts:
  - runs scripts/run-vz-linux-vm-e2e-local.sh (no SSH) when --vm-name/--guest-repo/--vm-image
    are provided
  - otherwise falls back to scripts/run-vz-linux-vm-e2e-hostboot.sh to run
    the same Linux harness in a transient host-boot guest.

Options:
  --workspace <path>          Linux workspace path (or VZ_TEST_BTRFS_WORKSPACE)
  --profile <debug|release>   Build profile (default: release)
  --output-dir <path>         Artifact root (default: .artifacts/release-gates/linux-daemon)
  --api-bind <host:port>      API bind for Linux harness (default: 127.0.0.1:18181)
  --vm-name <name>            macOS mode: local VM name (or VZ_LINUX_VM_NAME)
  --guest-repo <path>         macOS mode: in-guest repo path (or VZ_LINUX_VM_GUEST_REPO)
  --vm-image <path>           macOS mode auto-start image (or VZ_LINUX_VM_IMAGE)
  --mount <TAG:HOST_PATH>     macOS mode: forwarded to vm mac run (repeatable)
  --wait-secs <n>             macOS mode: wait for VM readiness after auto-start (default: 120)
  --no-provision-btrfs        macOS mode: skip btrfs provisioning in guest
  -h, --help                  Show help
USAGE
}

err() {
    echo "error: $*" >&2
    exit 1
}

MOUNTS=()
WAIT_SECS="120"
PROVISION_BTRFS=true

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
        --vm-name)
            VM_NAME="${2:-}"
            shift 2
            ;;
        --guest-repo)
            GUEST_REPO="${2:-}"
            shift 2
            ;;
        --vm-image)
            VM_IMAGE="${2:-}"
            shift 2
            ;;
        --mount)
            MOUNTS+=("${2:-}")
            shift 2
            ;;
        --wait-secs)
            WAIT_SECS="${2:-}"
            shift 2
            ;;
        --no-provision-btrfs)
            PROVISION_BTRFS=false
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
[[ -n "$WORKSPACE" ]] || err "--workspace (or VZ_TEST_BTRFS_WORKSPACE) is required"

mkdir -p "$OUTPUT_ROOT"

if [[ "$(uname -s)" == "Linux" ]]; then
    "$SCRIPT_DIR/run-vz-linux-vm-e2e.sh" \
        --workspace "$WORKSPACE" \
        --profile "$PROFILE" \
        --output-dir "$OUTPUT_ROOT" \
        --api-bind "$API_BIND"
    exit 0
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
    if [[ -n "$VM_NAME" && -n "$GUEST_REPO" && -n "$VM_IMAGE" ]]; then
        cmd=(
            "$SCRIPT_DIR/run-vz-linux-vm-e2e-local.sh"
            --vm-name "$VM_NAME"
            --guest-repo "$GUEST_REPO"
            --workspace "$WORKSPACE"
            --profile "$PROFILE"
            --output-dir "$OUTPUT_ROOT"
            --auto-start
            --vm-image "$VM_IMAGE"
            --wait-secs "$WAIT_SECS"
        )
        if [[ "$PROVISION_BTRFS" == "false" ]]; then
            cmd+=(--no-provision-btrfs)
        fi
        for mount in "${MOUNTS[@]}"; do
            cmd+=(--mount "$mount")
        done
        "${cmd[@]}"
        exit 0
    fi

    # Host-boot fallback path (no pre-existing local VM required).
    cmd=(
        "$SCRIPT_DIR/run-vz-linux-vm-e2e-hostboot.sh"
        --output-dir "$OUTPUT_ROOT/hostboot"
        --harness-output-dir "$OUTPUT_ROOT"
        --workspace "$WORKSPACE"
        --profile "$PROFILE"
    )
    if [[ "$PROVISION_BTRFS" == "false" ]]; then
        cmd+=(--no-provision-btrfs)
    fi
    "${cmd[@]}"
    exit 0
fi

err "unsupported host OS: $(uname -s)"
