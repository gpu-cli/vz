#!/usr/bin/env bash
# Bootstrap a host-booted Linux guest and execute a guest command.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

NAME="linux-hostboot"
OUTPUT_DIR="$REPO_ROOT/.artifacts/vm-linux-hostboot"
DISK_SIZE_GB="32"
CPUS="4"
MEMORY_MB="4096"
FORCE_INIT=false
GUEST_COMMAND=""
MOUNTS=()
AGENT_TIMEOUT_SECS="60"
COMMAND_TIMEOUT_SECS="1800"
GUEST_USER=""

usage() {
    cat <<'USAGE'
run-vz-linux-hostboot-command.sh

Bootstrap a Linux host-boot descriptor/disk and run one command in-guest.

Options:
  --name <name>                 Logical image name (default: linux-hostboot)
  --output-dir <path>           Descriptor/disk directory (default: .artifacts/vm-linux-hostboot)
  --disk-size-gb <n>            Persistent disk size GiB (default: 32)
  --cpus <n>                    VM CPUs (default: 4)
  --memory-mb <n>               VM memory MB (default: 4096)
  --mount <TAG:HOST_PATH[:ro|rw]>  VirtioFS share passed to `vz vm linux run` (repeatable)
  --command <shell-command>     Guest command for `/bin/sh -lc` (required)
  --guest-user <user>           Optional guest user for command execution
  --agent-timeout-secs <n>      Guest-agent readiness timeout (default: 60)
  --command-timeout-secs <n>    Guest command timeout (default: 1800)
  --force-init                  Replace existing descriptor/disk metadata
  -h, --help                    Show help

Env:
  VZ_BIN                        Explicit host `vz` binary path.
USAGE
}

err() {
    echo "error: $*" >&2
    exit 1
}

resolve_vz_bin() {
    if [[ -n "${VZ_BIN:-}" ]]; then
        [[ -x "$VZ_BIN" ]] || err "VZ_BIN is set but not executable: $VZ_BIN"
        echo "$VZ_BIN"
        return 0
    fi
    if command -v vz >/dev/null 2>&1; then
        command -v vz
        return 0
    fi
    if [[ -x "$REPO_ROOT/crates/target/debug/vz" ]]; then
        echo "$REPO_ROOT/crates/target/debug/vz"
        return 0
    fi
    if [[ -x "$REPO_ROOT/crates/target/release/vz" ]]; then
        echo "$REPO_ROOT/crates/target/release/vz"
        return 0
    fi
    err "vz binary not found (set VZ_BIN, put vz in PATH, or build crates/target/{debug,release}/vz)"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --name)
            NAME="${2:-}"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="${2:-}"
            shift 2
            ;;
        --disk-size-gb)
            DISK_SIZE_GB="${2:-}"
            shift 2
            ;;
        --cpus)
            CPUS="${2:-}"
            shift 2
            ;;
        --memory-mb)
            MEMORY_MB="${2:-}"
            shift 2
            ;;
        --mount)
            MOUNTS+=("${2:-}")
            shift 2
            ;;
        --command)
            GUEST_COMMAND="${2:-}"
            shift 2
            ;;
        --guest-user)
            GUEST_USER="${2:-}"
            shift 2
            ;;
        --agent-timeout-secs)
            AGENT_TIMEOUT_SECS="${2:-}"
            shift 2
            ;;
        --command-timeout-secs)
            COMMAND_TIMEOUT_SECS="${2:-}"
            shift 2
            ;;
        --force-init)
            FORCE_INIT=true
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

[[ "$(uname -s)" == "Darwin" ]] || err "host-boot Linux guest execution requires macOS host"
[[ -n "$GUEST_COMMAND" ]] || err "--command is required"

VZ_BIN="$(resolve_vz_bin)"
echo "==> using vz binary: $VZ_BIN"

mkdir -p "$OUTPUT_DIR"

init_cmd=(
    "$VZ_BIN" vm linux init
    --name "$NAME"
    --output-dir "$OUTPUT_DIR"
    --disk-size-gb "$DISK_SIZE_GB"
)
if [[ "$FORCE_INIT" == "true" ]]; then
    init_cmd+=(--force)
fi

echo "==> initializing linux image descriptor"
"${init_cmd[@]}"

run_cmd=(
    "$VZ_BIN" vm linux run
    --name "$NAME"
    --output-dir "$OUTPUT_DIR"
    --cpus "$CPUS"
    --memory "$MEMORY_MB"
    --agent-timeout-secs "$AGENT_TIMEOUT_SECS"
    --guest-command "$GUEST_COMMAND"
    --guest-command-timeout-secs "$COMMAND_TIMEOUT_SECS"
)
if [[ -n "$GUEST_USER" ]]; then
    run_cmd+=(--guest-command-user "$GUEST_USER")
fi
for mount in "${MOUNTS[@]}"; do
    run_cmd+=(--mount "$mount")
done

echo "==> booting linux guest and running command"
"${run_cmd[@]}"

