#!/usr/bin/env bash
# Run Linux VM E2E harness inside a local vz-managed VM (no SSH).
#
# This wrapper executes commands through `vz vm mac exec` against an already
# running local VM. It can also provision a loopback btrfs workspace in-guest
# before running the high-level harness.

set -euo pipefail

VM_NAME=""
GUEST_REPO=""
WORKSPACE="/mnt/vz-btrfs"
PROFILE="debug"
PROVISION_BTRFS=true
BTRFS_IMAGE="/var/lib/vz-btrfs-workspace.img"
BTRFS_SIZE_GB="64"
VM_IMAGE=""
VM_CPUS="4"
VM_MEMORY_GB="8"
AUTO_START=false
WAIT_SECS="90"
MOUNTS=()
RUN_PID=""

usage() {
    cat <<'USAGE'
run-vz-linux-vm-e2e-local.sh

Run Linux E2E harness inside a local vz-managed VM without SSH.

Options:
  --vm-name <name>            VM name registered in `vz vm mac list` (required)
  --guest-repo <path>         Repo path inside VM (required)
  --workspace <path>          In-guest btrfs workspace path (default: /mnt/vz-btrfs)
  --profile <debug|release>   Harness profile (default: debug)
  --vm-image <path>           VM image path for auto-start if VM is not running
  --vm-cpus <n>               CPUs for auto-started VM (default: 4)
  --vm-memory-gb <n>          Memory GB for auto-started VM (default: 8)
  --auto-start                Auto-start VM if not already running (requires --vm-image)
  --wait-secs <n>             Max seconds waiting for VM to accept exec after auto-start (default: 90)
  --mount <TAG:HOST_PATH>     VirtioFS mount passed to `vz vm mac run` (repeatable, auto-start only)
  --provision-btrfs           Ensure btrfs workspace exists in guest (default: on)
  --no-provision-btrfs        Skip btrfs provisioning step
  --btrfs-image <path>        In-guest loopback image path (default: /var/lib/vz-btrfs-workspace.img)
  --btrfs-size-gb <n>         In-guest loopback image size GiB (default: 64)
  -h, --help                  Show help

Notes:
  - Without --auto-start, VM must already be running and reachable by `vz vm mac exec`.
  - This script does not use SSH.
USAGE
}

err() {
    echo "error: $*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --vm-name)
            VM_NAME="${2:-}"
            shift 2
            ;;
        --guest-repo)
            GUEST_REPO="${2:-}"
            shift 2
            ;;
        --workspace)
            WORKSPACE="${2:-}"
            shift 2
            ;;
        --profile)
            PROFILE="${2:-}"
            shift 2
            ;;
        --vm-image)
            VM_IMAGE="${2:-}"
            shift 2
            ;;
        --vm-cpus)
            VM_CPUS="${2:-}"
            shift 2
            ;;
        --vm-memory-gb)
            VM_MEMORY_GB="${2:-}"
            shift 2
            ;;
        --auto-start)
            AUTO_START=true
            shift
            ;;
        --wait-secs)
            WAIT_SECS="${2:-}"
            shift 2
            ;;
        --mount)
            MOUNTS+=("${2:-}")
            shift 2
            ;;
        --provision-btrfs)
            PROVISION_BTRFS=true
            shift
            ;;
        --no-provision-btrfs)
            PROVISION_BTRFS=false
            shift
            ;;
        --btrfs-image)
            BTRFS_IMAGE="${2:-}"
            shift 2
            ;;
        --btrfs-size-gb)
            BTRFS_SIZE_GB="${2:-}"
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

[[ -n "$VM_NAME" ]] || err "--vm-name is required"
[[ -n "$GUEST_REPO" ]] || err "--guest-repo is required"
[[ "$PROFILE" == "debug" || "$PROFILE" == "release" ]] || err "--profile must be debug|release"
if [[ "$AUTO_START" == "true" && -z "$VM_IMAGE" ]]; then
    err "--auto-start requires --vm-image"
fi

command -v vz >/dev/null 2>&1 || err "vz binary not found in PATH"

vm_exec_probe() {
    vz vm mac exec "$VM_NAME" --user dev -- /bin/sh -lc "echo vm_ok" >/dev/null 2>&1
}

cleanup() {
    if [[ -n "$RUN_PID" ]]; then
        # keep VM running by default; caller can stop explicitly
        :
    fi
}
trap cleanup EXIT

if ! vm_exec_probe; then
    if [[ "$AUTO_START" != "true" ]]; then
        err "VM '$VM_NAME' is not reachable; start it first or pass --auto-start --vm-image"
    fi

    echo "==> starting VM: $VM_NAME"
    run_cmd=(vz vm mac run --name "$VM_NAME" --image "$VM_IMAGE" --cpus "$VM_CPUS" --memory "$VM_MEMORY_GB" --headless)
    for mount in "${MOUNTS[@]}"; do
        run_cmd+=(--mount "$mount")
    done
    "${run_cmd[@]}" >/tmp/vz-linux-vm-e2e-local-run.log 2>&1 &
    RUN_PID=$!

    deadline=$((SECONDS + WAIT_SECS))
    until vm_exec_probe; do
        if (( SECONDS >= deadline )); then
            err "VM '$VM_NAME' did not become reachable within ${WAIT_SECS}s"
        fi
        sleep 1
    done
fi

echo "==> VM reachable: $VM_NAME"

if [[ "$PROVISION_BTRFS" == "true" ]]; then
    echo "==> provisioning btrfs workspace in guest: $WORKSPACE"
    vz vm mac exec "$VM_NAME" --user root -- /bin/sh -lc \
        "set -euo pipefail; cd '$GUEST_REPO'; ./scripts/provision-linux-btrfs-workspace.sh --workspace '$WORKSPACE' --image '$BTRFS_IMAGE' --size-gb '$BTRFS_SIZE_GB' --owner dev"
fi

echo "==> running Linux VM E2E harness in guest"
vz vm mac exec "$VM_NAME" --user dev -- /bin/sh -lc \
    "set -euo pipefail; if [ -f \"\$HOME/.cargo/env\" ]; then . \"\$HOME/.cargo/env\"; fi; cd '$GUEST_REPO'; ./scripts/run-vz-linux-vm-e2e.sh --workspace '$WORKSPACE' --profile '$PROFILE'"

echo "==> completed successfully"
