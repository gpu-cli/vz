#!/usr/bin/env bash
# Provision a loopback-backed btrfs workspace on Linux.
#
# Intended for dedicated VM test hosts where we need a real btrfs path for
# spaces/runtime portability gates.

set -euo pipefail

WORKSPACE="/mnt/vz-btrfs"
IMAGE_PATH="/var/lib/vz-btrfs-workspace.img"
IMAGE_SIZE_GB="64"
OWNER_USER="${SUDO_USER:-${USER:-}}"

usage() {
    cat <<'USAGE'
provision-linux-btrfs-workspace.sh

Create and mount a loopback-backed btrfs filesystem for vz tests.

Options:
  --workspace <path>      Mountpoint/workspace path (default: /mnt/vz-btrfs)
  --image <path>          Backing image path (default: /var/lib/vz-btrfs-workspace.img)
  --size-gb <n>           Image size in GiB when creating image (default: 64)
  --owner <user>          User to own workspace root (default: invoking user)
  -h, --help              Show help
USAGE
}

err() {
    echo "error: $*" >&2
    exit 1
}

require_root() {
    if [[ "$(id -u)" -ne 0 ]]; then
        err "must run as root (use sudo)"
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workspace)
            WORKSPACE="${2:-}"
            shift 2
            ;;
        --image)
            IMAGE_PATH="${2:-}"
            shift 2
            ;;
        --size-gb)
            IMAGE_SIZE_GB="${2:-}"
            shift 2
            ;;
        --owner)
            OWNER_USER="${2:-}"
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

require_root
command -v btrfs >/dev/null 2>&1 || err "btrfs command not found"
command -v findmnt >/dev/null 2>&1 || err "findmnt not found"
[[ -n "$OWNER_USER" ]] || err "--owner is required or USER/SUDO_USER must be set"

mkdir -p "$(dirname "$IMAGE_PATH")"
[[ "$IMAGE_SIZE_GB" =~ ^[0-9]+$ ]] || err "--size-gb must be an integer"
desired_size_bytes=$((IMAGE_SIZE_GB * 1024 * 1024 * 1024))
if [[ ! -f "$IMAGE_PATH" ]]; then
    truncate -s "${IMAGE_SIZE_GB}G" "$IMAGE_PATH"
else
    current_size_bytes="$(stat -c %s "$IMAGE_PATH")"
    if (( current_size_bytes < desired_size_bytes )); then
        truncate -s "${IMAGE_SIZE_GB}G" "$IMAGE_PATH"
    fi
fi

if ! blkid -s TYPE -o value "$IMAGE_PATH" 2>/dev/null | grep -qx "btrfs"; then
    mkfs.btrfs -f "$IMAGE_PATH"
fi

mkdir -p "$WORKSPACE"
if ! findmnt -n -M "$WORKSPACE" >/dev/null 2>&1; then
    mount -o loop "$IMAGE_PATH" "$WORKSPACE"
fi

# Ensure an expanded backing file is reflected in mounted filesystem capacity.
btrfs filesystem resize max "$WORKSPACE" >/dev/null 2>&1 || true

FSTYPE="$(findmnt -n -M "$WORKSPACE" -o FSTYPE || true)"
[[ "$FSTYPE" == "btrfs" ]] || err "workspace is not mounted as btrfs (detected: ${FSTYPE:-unknown})"

chown "$OWNER_USER":"$OWNER_USER" "$WORKSPACE"

echo "workspace ready:"
echo "  path: $WORKSPACE"
echo "  fs:   $FSTYPE"
echo "  owner:$OWNER_USER"
