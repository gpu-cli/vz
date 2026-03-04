#!/usr/bin/env bash
# Download/extract Alpine minirootfs for host-boot Linux VM workflows.

set -euo pipefail

ALPINE_VERSION="3.20"
ARCH=""
OUTPUT_DIR="${HOME:-.}/.vz/linux-rootfs"
FORCE=false

usage() {
    cat <<'USAGE'
ensure-alpine-rootfs.sh

Download and extract Alpine minirootfs into a deterministic local directory.

Options:
  --version <major.minor>    Alpine stream version (default: 3.20)
  --arch <arch>              Alpine arch (default: host-derived; arm64->aarch64)
  --output-dir <path>        Rootfs parent directory (default: ~/.vz/linux-rootfs)
  --force                    Re-download and re-extract even if present
  -h, --help                 Show help
USAGE
}

err() {
    echo "error: $*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)
            ALPINE_VERSION="${2:-}"
            shift 2
            ;;
        --arch)
            ARCH="${2:-}"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="${2:-}"
            shift 2
            ;;
        --force)
            FORCE=true
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

if [[ -z "$ARCH" ]]; then
    host_arch="$(uname -m)"
    case "$host_arch" in
        arm64|aarch64)
            ARCH="aarch64"
            ;;
        x86_64|amd64)
            ARCH="x86_64"
            ;;
        *)
            err "unsupported host arch '$host_arch'; pass --arch explicitly"
            ;;
    esac
fi

base_url="https://dl-cdn.alpinelinux.org/alpine/v${ALPINE_VERSION}/releases/${ARCH}"
releases_url="${base_url}/latest-releases.yaml"

mkdir -p "$OUTPUT_DIR"
target_dir="${OUTPUT_DIR}/alpine-v${ALPINE_VERSION}-${ARCH}"
stamp_file="${target_dir}/.alpine-rootfs-stamp"

if [[ -f "$stamp_file" && "$FORCE" != "true" ]]; then
    echo "$target_dir"
    exit 0
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

echo "==> resolving latest Alpine minirootfs (${ALPINE_VERSION}, ${ARCH})"
curl -fsSL "$releases_url" -o "$tmp_dir/latest-releases.yaml"
archive_name="$(awk '/^[[:space:]]*file:[[:space:]]+alpine-minirootfs-.*\.tar\.gz$/ {print $2; exit}' "$tmp_dir/latest-releases.yaml")"
[[ -n "$archive_name" ]] || err "failed to resolve minirootfs archive from $releases_url"

archive_url="${base_url}/${archive_name}"
archive_path="${tmp_dir}/${archive_name}"
echo "==> downloading $archive_name"
curl -fL "$archive_url" -o "$archive_path"

if [[ -d "$target_dir" ]]; then
    rm -rf "$target_dir"
fi
mkdir -p "$target_dir"

echo "==> extracting to $target_dir"
tar -xzf "$archive_path" -C "$target_dir"
echo "$archive_name" > "$stamp_file"

echo "$target_dir"
