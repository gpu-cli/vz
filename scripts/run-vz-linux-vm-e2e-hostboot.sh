#!/usr/bin/env bash
# Run Linux daemon E2E harness in a host-booted Linux distro guest (no SSH).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

NAME="linux-daemon-e2e"
PROFILE="debug"
WORKSPACE_IN_GUEST="/mnt/vz-btrfs"
OUTPUT_DIR="$REPO_ROOT/.artifacts/vm-linux-hostboot-e2e"
ROOTFS_DIR=""
DISK_SIZE_GB="24"
CPUS="4"
MEMORY_MB="8192"
SKIP_PKG_SETUP=false
PROVISION_BTRFS=true
BTRFS_IMAGE="/var/lib/vz-btrfs-workspace.img"
BTRFS_SIZE_GB="64"

usage() {
    cat <<'USAGE'
run-vz-linux-vm-e2e-hostboot.sh

Run scripts/run-vz-linux-vm-e2e.sh inside a host-boot Linux distro guest.
No external SSH hosts are used.

Options:
  --name <name>                Logical host-boot image name (default: linux-daemon-e2e)
  --profile <debug|release>    Harness profile (default: debug)
  --workspace <path>           In-guest btrfs workspace path (default: /mnt/vz-btrfs)
  --output-dir <path>          Host descriptor/disk output dir (default: .artifacts/vm-linux-hostboot-e2e)
  --rootfs-dir <path>          Distro rootfs directory (default: auto via ensure-alpine-rootfs.sh)
  --disk-size-gb <n>           Persistent disk GiB (default: 24)
  --cpus <n>                   VM CPUs (default: 4)
  --memory-mb <n>              VM memory MB (default: 8192)
  --skip-pkg-setup             Skip guest package install bootstrap
  --no-provision-btrfs         Skip in-guest btrfs provisioning
  --btrfs-image <path>         In-guest loopback btrfs image (default: /var/lib/vz-btrfs-workspace.img)
  --btrfs-size-gb <n>          In-guest btrfs image size GiB (default: 64)
  -h, --help                   Show help

Env:
  VZ_BIN                       Explicit host `vz` binary path.
USAGE
}

err() {
    echo "error: $*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --name)
            NAME="${2:-}"
            shift 2
            ;;
        --profile)
            PROFILE="${2:-}"
            shift 2
            ;;
        --workspace)
            WORKSPACE_IN_GUEST="${2:-}"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="${2:-}"
            shift 2
            ;;
        --rootfs-dir)
            ROOTFS_DIR="${2:-}"
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
        --skip-pkg-setup)
            SKIP_PKG_SETUP=true
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

[[ "$PROFILE" == "debug" || "$PROFILE" == "release" ]] || err "--profile must be debug|release"
[[ "$(uname -s)" == "Darwin" ]] || err "host-boot Linux VM E2E requires macOS host"

if [[ -z "$ROOTFS_DIR" ]]; then
    ROOTFS_DIR="$("$SCRIPT_DIR/ensure-alpine-rootfs.sh")"
fi
[[ -d "$ROOTFS_DIR" ]] || err "rootfs dir not found: $ROOTFS_DIR"

pkg_setup=""
skip_pkg_preflight=""
if [[ "$SKIP_PKG_SETUP" != "true" ]]; then
    pkg_setup='
apk_retry() {
  local attempts="${1:-5}";
  shift;
  local n=1;
  while [ "$n" -le "$attempts" ]; do
    if "$@"; then
      return 0;
    fi;
    echo "apk command failed (attempt ${n}/${attempts}), retrying..." >&2;
    sleep $((n * 2));
    n=$((n + 1));
  done;
  return 1;
};
apk_retry 6 apk update;
apk_retry 6 apk add --no-cache bash curl git build-base pkgconf openssl-dev openssl-libs-static protobuf-dev rustup btrfs-progs util-linux iproute2 iptables runc;
if ! command -v youki >/dev/null 2>&1 && command -v runc >/dev/null 2>&1; then
  runc_bin="$(command -v runc)";
  mkdir -p /usr/bin;
  mkdir -p /usr/local/bin;
  ln -sf "$runc_bin" /usr/bin/youki;
  ln -sf "$runc_bin" /usr/local/bin/youki;
fi;
command -v youki >/dev/null 2>&1 || { echo "youki shim unavailable after bootstrap" >&2; exit 1; };
cargo_supports_resolver3() {
  cargo --version 2>/dev/null | awk "{split(\$2,v,\".\"); exit !((v[1] > 1) || (v[1] == 1 && v[2] >= 84))}"
}
if ! cargo_supports_resolver3; then
  rustup-init -y --default-toolchain stable --profile minimal --no-modify-path;
fi;
if [ -f "$HOME/.cargo/env" ]; then . "$HOME/.cargo/env"; fi;
if command -v rustup >/dev/null 2>&1; then rustup toolchain install stable >/dev/null 2>&1 || true; rustup default stable >/dev/null 2>&1 || true; fi;
'
else
    skip_pkg_preflight='
required_tools="bash curl git cargo rustup btrfs youki";
for tool in $required_tools; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "missing required guest tool \"$tool\" while --skip-pkg-setup is set; rerun without --skip-pkg-setup" >&2;
    exit 1;
  fi;
done;
'
fi

provision_btrfs_cmd=""
if [[ "$PROVISION_BTRFS" == "true" ]]; then
    provision_btrfs_cmd="
cd /mnt/repo;
./scripts/provision-linux-btrfs-workspace.sh --workspace '${WORKSPACE_IN_GUEST}' --image '${BTRFS_IMAGE}' --size-gb '${BTRFS_SIZE_GB}' --owner root;
"
fi

guest_cmd="
set -euo pipefail;
export HOME=/root;
mkdir -p \"\$HOME\" \"\$HOME/.docker\";
if [ ! -f \"\$HOME/.docker/config.json\" ]; then
  printf '{}\n' > \"\$HOME/.docker/config.json\";
fi;
export DOCKER_CONFIG=\"\$HOME/.docker\";
mkdir -p /mnt/repo;
mount -t virtiofs repo /mnt/repo;
${skip_pkg_preflight}
${pkg_setup}
${provision_btrfs_cmd}
export CARGO_TARGET_DIR='/tmp/vz-cargo-target';
cd /mnt/repo;
./scripts/run-vz-linux-vm-e2e.sh --workspace '${WORKSPACE_IN_GUEST}' --profile '${PROFILE}'
"

"$SCRIPT_DIR/run-vz-linux-hostboot-command.sh" \
    --name "$NAME" \
    --output-dir "$OUTPUT_DIR" \
    --disk-size-gb "$DISK_SIZE_GB" \
    --cpus "$CPUS" \
    --memory-mb "$MEMORY_MB" \
    --rootfs-dir "$ROOTFS_DIR" \
    --mount "repo:${REPO_ROOT}" \
    --command "$guest_cmd"
